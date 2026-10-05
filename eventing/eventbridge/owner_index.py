"""The global `correlationid -> userkey` index. DESIGN_PHASE3.md §2.6, §6.2.

**The one table that is deliberately cross-tenant**, and it has to be: the lookup that
decides *which* tenant owns a correlation must happen before a tenant — and therefore
before a per-user store (§6.1) — can be chosen. Everything else in Phase 3 pushes data
into per-user files precisely so a query cannot reach another tenant's rows; this is the
documented exception, and it holds nothing but a mapping from a correlation id to a
tenancy key.

Two jobs, and the second is the reason the first is affordable:

1. **Owner-scoped reads** (§6.2). `owner_of(corr)` answers "whose is this?" so a handler
   can compare it to the caller and answer `404` when they differ.
2. **The global uniqueness guarantee** (§2.6). `UNIQUE` on `correlationid` makes two
   tenants minting the same id impossible, which is what lets
   `sessionuuid = uuid5(NAMESPACE, correlationid)` stay **unsalted**. Salting it with the
   userkey is the tempting alternative and §2.6 rejects it: it would break every existing
   session (`--resume` is keyed on that uuid, and Phase 1 §16 Gap B's checkpoint/restore
   path is built on it) for a collision this constraint already prevents.

**The constraint is on `correlationid` ALONE**, and this is the part to get right.
`(userkey, correlationid)` is the correct primary key *inside* a tenant's store and is
exactly the wrong constraint here: two tenants minting one `corr` produce two different
tuples, so it would never conflict and the collision would pass through silently — which
is the failure the whole guarantee exists to prevent.

It also replaces the `Minter` seeding loop. Phase 2 seeded a `seen` set from
`all_correlations(limit=10000)` on one store; with N per-user stores that would mean N
SQLite opens before the socket binds. §2.6 is explicit that one indexed `SELECT` per mint
is both cheaper and simpler than seeding from every tenant.

Stdlib only (`sqlite3`).
"""
from __future__ import annotations

import pathlib
import sqlite3
import threading

# `userkey` is nullable on purpose: single-tenant mode and the `shared` tier both record
# ownership as NULL rather than inventing a key. That keeps the uniqueness guarantee on
# `correlationid` in force for every deployment — including the ones that never turn
# multi-tenancy on — so a single-tenant bridge gets the same protection against a
# duplicate mint that a multi-tenant one does.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS correlation_owner (
  correlationid TEXT PRIMARY KEY,   -- UNIQUE, and deliberately NOT (userkey, corr)
  userkey       TEXT,               -- NULL = single-tenant or the `shared` tier
  -- Millisecond precision, matching `ce.now_iso()`. At second granularity a batch
  -- submitted inside one second had arbitrary order among its rows, so
  -- `ORDER BY created_utc DESC LIMIT n` returned an arbitrary subset -- and
  -- `submit_members` publishes a whole 100-member batch in a loop, which is exactly
  -- that case. Ordered reads also tie-break on `rowid`, which is free and total.
  created_utc   TEXT NOT NULL,
  -- A TOMBSTONE marks a correlation whose data is gone but whose id must never be
  -- reissued. See `forget`: reuse is unsafe because `sessionuuid` is unsalted.
  deleted_utc   TEXT
);
CREATE INDEX IF NOT EXISTS idx_owner_userkey ON correlation_owner(userkey);

-- Which stores have already been seeded (`seed_from`). Without this the startup backfill
-- re-opens every tenant store and re-reads `all_correlations` on EVERY boot, which is the
-- per-start N-store seeding §2.6 rejects — reintroduced by the fix for the missing seed.
-- Sound because nothing can add an UNINDEXED id to a store after it is seeded: every mint
-- claims as it mints, so a seeded store cannot later acquire one.
CREATE TABLE IF NOT EXISTS seed_state (
  scope     TEXT PRIMARY KEY,   -- "" for the shared/single store, else the userkey
  done_utc  TEXT NOT NULL
);
"""


class Collision(Exception):
    """A correlation id already claimed by a different tenant.

    Separate from the "already mine" case, which is idempotent and must not raise: the
    submit path can legitimately re-record the same (corr, userkey) pair, and a retried
    request should not fail. Only a genuine cross-tenant clash is an error.
    """


class OwnerIndex:
    """`correlationid -> userkey`, with `correlationid` globally unique.

    One SQLite file, opened once for the process lifetime. Unlike the per-user stores
    this is never evicted — it is small (three short columns per correlation), it is on
    the hot path of every mint and every owner-scoped read, and it is the one table whose
    absence would silently remove the uniqueness guarantee.
    """

    def __init__(self, root: str | pathlib.Path) -> None:
        base = pathlib.Path(root)
        base.mkdir(parents=True, exist_ok=True)
        self._c = sqlite3.connect(base / "owners.sqlite",
                                  check_same_thread=False, isolation_level=None)
        self._c.execute("PRAGMA journal_mode=WAL")
        self._c.execute("PRAGMA synchronous=NORMAL")
        self._c.executescript(_SCHEMA)
        # `CREATE TABLE IF NOT EXISTS` is a no-op on an existing file, so a column added
        # after one was created needs an explicit ALTER — the same idiom `store.py` uses
        # for `prompts.submitter`. Idempotent: SQLite raises when it already exists.
        try:
            self._c.execute("ALTER TABLE correlation_owner ADD COLUMN deleted_utc TEXT")
        except sqlite3.OperationalError:
            pass
        # EventBridge is single-replica (Phase 1 §8.6) but multi-threaded: the HTTP
        # worker pool and the responses consumer both reach this. `isolation_level=None`
        # means autocommit, so the lock is what makes claim's check-then-insert atomic.
        self._lock = threading.Lock()

    # ---- the mint-time guarantee -------------------------------------------

    def claim(self, correlationid: str, userkey: str | None) -> None:
        """Record ownership. Raises `Collision` if another tenant already owns it.

        Idempotent for the same owner, so a retried submit is not an error. The
        comparison treats NULL and a key as different owners, which is correct: a
        correlation minted in single-tenant mode and later claimed by a tenant is a
        genuine ambiguity, not a no-op.

        **The database is the authority, not the lock.** `ON CONFLICT DO NOTHING` plus a
        follow-up read means a conflict presents as `Collision` whether or not two writers
        raced past the in-process lock. An earlier version did `SELECT` then a bare
        `INSERT`: correct while the lock held, but if two writers ever did race the
        `INSERT` raised `sqlite3.IntegrityError` rather than `Collision` — and
        `Minter.mint` catches `Collision` specifically (deliberately, so a broken index is
        not retried 2000 times), so that path would have surfaced a 500 instead of
        retrying. §6.5's deletion path and any future sweeper are exactly the second
        writer that makes this reachable.
        """
        with self._lock:
            cur = self._c.execute(
                "INSERT INTO correlation_owner(correlationid,userkey,created_utc) "
                "VALUES (?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now')) "
                "ON CONFLICT(correlationid) DO NOTHING",
                (correlationid, userkey))
            if cur.rowcount:
                return
            # The row already existed. Same owner is idempotent; anything else is a
            # clash. A TOMBSTONE is never a re-claim, whoever asks: `forget` clears
            # `userkey` to NULL, so for `claim(corr, None)` the `row[0] == userkey` test
            # below would read as "already mine" and succeed — contradicting the
            # guarantee `forget` makes, that the id is reserved forever because a reissue
            # derives the same unsalted `sessionuuid`.
            #
            # Not reachable from `Minter.mint`, which checks `exists()` first and a
            # tombstone is `True`. It becomes reachable the moment a `correlationid`
            # arrives from outside — which §7's triggers and T6/T7 plausibly bring — so it
            # is closed here, where the invariant is written down.
            row = self._c.execute(
                "SELECT userkey, deleted_utc FROM correlation_owner WHERE correlationid=?",
                (correlationid,)).fetchone()
            if row is not None and row[1]:
                raise Collision(
                    f"correlation {correlationid!r} is tombstoned and can never be "
                    f"reissued; a reissue would derive the same sessionuuid")
            if row is not None and row[0] == userkey:
                return
            raise Collision(
                f"correlation {correlationid!r} is already owned by "
                f"{(row[0] if row else None) or '(single-tenant)'}, cannot claim for "
                f"{userkey or '(single-tenant)'}")

    def exists(self, correlationid: str) -> bool:
        """Whether this id is taken by anyone. The `Minter`'s uniqueness check.

        One indexed lookup against a PRIMARY KEY, which is what §2.6 trades the
        N-store seeding loop for.
        """
        return self._c.execute(
            "SELECT 1 FROM correlation_owner WHERE correlationid=? LIMIT 1",
            (correlationid,)).fetchone() is not None

    def needs_seeding(self, userkey: str | None = None) -> bool:
        """Whether this store's ids still have to be backfilled.

        Lets the caller skip the `Store` open and the `all_correlations` query entirely,
        which is the whole cost §2.6 objects to. `seed_from` is `INSERT OR IGNORE` so
        repeating it was harmless to *correctness* — what repeated was two SQLite
        connections and an indexed scan per tenant before the socket binds, on every boot.
        """
        return self._c.execute(
            "SELECT 1 FROM seed_state WHERE scope=? LIMIT 1",
            (userkey or "",)).fetchone() is None

    def mark_seeded(self, userkey: str | None = None) -> None:
        """Record that this store has been backfilled. Idempotent."""
        with self._lock:
            self._c.execute(
                "INSERT OR REPLACE INTO seed_state(scope,done_utc) "
                "VALUES (?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))", (userkey or "",))

    def seed_from(self, correlationids, userkey: str | None = None) -> int:
        """Record ids that already exist outside the index. Returns how many were new.

        **This is not an optimisation; it closes a correctness hole.** §2.6 is right that
        one `SELECT` per mint beats seeding a `seen` set from N stores on every start — but
        it does not follow that the index never needs seeding *once*. On the first start
        after an upgrade, `owners.sqlite` is brand new and empty while `sessions.sqlite`
        still holds every correlation from before, so `exists()` answers "free" for ids
        that are in use. `Minter.mint` consults nothing else, so it can reissue a live id;
        `upsert_session` then overwrites that session and the new prompt appends to
        somebody's existing conversation.

        The id space is 50 adjectives x 50 animals x 10,000 = 25,000,000, so a deployment
        with ~1,000 existing correlations has roughly a 1-in-25,000 chance per mint.
        Phase 2's chance was zero, because its `seen` set was seeded from the store — so
        without this, Phase 3 is a regression in the DEFAULT configuration, which is the
        one thing the phase promises cannot happen.

        Marks the scope seeded on completion, so `needs_seeding` returns `False`
        afterwards and the caller can skip the store open next boot. Callers that bypass
        `needs_seeding` still get a correct (if wasted) re-seed.

        `INSERT OR IGNORE`, so re-seeding is free and a correlation already claimed by a
        tenant keeps its owner. Seeding assigns `userkey=None` for ids recovered from a
        single-tenant store, which is the correct owner for them: they predate tenancy.
        """
        new = 0
        with self._lock:
            for corr in correlationids:
                cur = self._c.execute(
                    "INSERT OR IGNORE INTO correlation_owner"
                    "(correlationid,userkey,created_utc) "
                    "VALUES (?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
                    (corr, userkey))
                new += cur.rowcount or 0
            self._c.execute(
                "INSERT OR REPLACE INTO seed_state(scope,done_utc) "
                "VALUES (?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))", (userkey or "",))
        return new

    # ---- owner-scoped reads (§6.2) -----------------------------------------

    def owner_of(self, correlationid: str) -> tuple[str | None, bool]:
        """`(userkey, known)`. `known=False` means the correlation is not in the index.

        The two-value return distinguishes "owned by nobody in particular" (single-tenant
        or the `shared` tier, a legitimate NULL) from "never seen". A bare `None` would
        conflate them, and §6.2's `404` rule depends on telling them apart: an unknown
        correlation and another tenant's correlation must look identical to the caller,
        but a handler still needs to know which case it is in to decide whether to
        consult a store at all.
        """
        row = self._c.execute(
            "SELECT userkey FROM correlation_owner WHERE correlationid=?",
            (correlationid,)).fetchone()
        if row is None:
            return None, False
        return row[0], True

    def correlations_for(self, userkey: str | None, limit: int = 1000) -> list[str]:
        """Every correlation a tenant owns, newest first. Backs the group-list filter."""
        if userkey is None:
            rows = self._c.execute(
                "SELECT correlationid FROM correlation_owner "
                "WHERE userkey IS NULL AND deleted_utc IS NULL "
                "ORDER BY created_utc DESC, rowid DESC LIMIT ?", (limit,)).fetchall()
        else:
            rows = self._c.execute(
                "SELECT correlationid FROM correlation_owner WHERE userkey=? "
                "ORDER BY created_utc DESC, rowid DESC LIMIT ?",
                (userkey, limit)).fetchall()
        return [r[0] for r in rows]

    def userkeys(self) -> list[str]:
        """Tenants with at least one correlation. Used to enumerate stores to sweep."""
        rows = self._c.execute(
            "SELECT DISTINCT userkey FROM correlation_owner "
            "WHERE userkey IS NOT NULL").fetchall()
        return [r[0] for r in rows]

    def forget(self, correlationid: str) -> None:
        """Tombstone one mapping: clear the owner, keep the id reserved. §6.5.

        **The id is never reissued, and that is the whole point.** An earlier version
        `DELETE`d the row and its docstring claimed "freeing the id for reuse is
        deliberate" — which was both wrong about the code's intent and unsafe. `exists()`
        is the `Minter`'s only uniqueness check, so a deleted row frees the id; and
        because `sessionuuid = uuid5(NAMESPACE, correlationid)` is **unsalted** by design
        (§2.6), a reissued `correlationid` derives *the same session uuid* as the deleted
        one. A `claude` transcript left on a runner's volume, or a checkpoint that
        outlived the delete, is then resumable by the new correlation — one user's
        conversation continuing inside somebody else's agent.

        Tombstoning costs one short row per deleted correlation and removes the whole
        class. The alternative — guaranteeing every transcript keyed on that uuid is
        purged everywhere, including volumes this process does not own — is not a
        guarantee this component can make.

        The owner is cleared so a tombstone leaks nothing about who the correlation
        belonged to, which matters because §6.5 is a deletion path.
        """
        with self._lock:
            self._c.execute(
                "UPDATE correlation_owner "
                "SET userkey=NULL, deleted_utc=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE correlationid=?", (correlationid,))

    def forget_tenant(self, userkey: str) -> int:
        """Tombstone every mapping for one tenant, returning how many. §6.5's `rm -rf`.

        Same reasoning as `forget`: the rows stay as tombstones so none of the tenant's
        ids can be reissued, and the owner is cleared so the tombstones say nothing about
        whose they were.
        """
        with self._lock:
            cur = self._c.execute(
                "UPDATE correlation_owner "
                "SET userkey=NULL, deleted_utc=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE userkey=?", (userkey,))
            return cur.rowcount or 0

    def is_tombstoned(self, correlationid: str) -> bool:
        """Whether this id is reserved by a deleted correlation."""
        row = self._c.execute(
            "SELECT deleted_utc FROM correlation_owner WHERE correlationid=?",
            (correlationid,)).fetchone()
        return bool(row and row[0])

    def count(self) -> int:
        return int(self._c.execute(
            "SELECT COUNT(*) FROM correlation_owner").fetchone()[0])

    def close(self) -> None:
        self._c.close()
