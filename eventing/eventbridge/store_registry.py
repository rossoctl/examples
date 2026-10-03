"""Per-user stores, opened lazily and closed on an LRU. DESIGN_PHASE3.md §6.1.

```text
{tmpdir}/eventbridge/
  users/
    gh-mrsabath-4c1d9e07/
      responses.sqlite        # responses, keyed (correlationid, sequence)
      sessions.sqlite         # sessions, prompts, groups, group_members, transcripts
    gh-aslom-7b2e55a1/
      ...
  shared/                     # tier `shared` (§3.8), and any unattributable event
  responses.sqlite            # SINGLE-TENANT mode lives in the bridge root, not shared/
  sessions.sqlite
  owners.sqlite               # the global correlationid -> userkey index
```

Note where single-tenant mode's files sit: **the bridge root, not `shared/`**. §6.1's
diagram puts them in `shared/`, and the code deliberately differs — a Phase 2 deployment
already has `responses.sqlite` and `sessions.sqlite` directly in `{tmpdir}/eventbridge/`,
so relocating them on upgrade would make every existing session and transcript vanish
from the UI.

**Why separate files rather than one database with a `userkey` column.** The single-DB
version is cheaper — one connection pair, one startup, no file-descriptor arithmetic —
and §6.1 rejects it: `store.py` has 28 methods and every one reads or writes tenant data.
A `WHERE userkey = ?` that must be remembered 28 times, and in every method added later,
is a predicate that will eventually be forgotten, and the symptom is a user reading
someone else's conversation. Separate files make the mistake *structurally unavailable* —
the connection **is** the tenant, and a query cannot reach rows that are not in the file
it executes against. For a phase whose entire subject is isolation, buying that property
with file descriptors is the right trade.

That is also why this class exposes `for_userkey()` returning a whole `Store` rather than
wrapping all 28 methods with a userkey argument: wrapping would reintroduce exactly the
per-call parameter the file split exists to eliminate.

**The cost, as arithmetic.** Two SQLite connections per tenant, each in WAL mode — so the
main DB plus `-wal` and `-shm`. The default soft `RLIMIT_NOFILE` of 1024 bounds this
around 150-200 concurrently-open tenants before anything else the process holds, not 500,
because WAL multiplies the count. Hence the LRU.

Stdlib only.
"""
from __future__ import annotations

import pathlib
import threading

from eventbridge.store import Store
from shared import tenancy

# §6.1 / §8.1 `EB_MAX_OPEN_STORES`. 64 pairs is ~192 file descriptors with WAL, which
# leaves comfortable room under a 1024 soft limit for the Kafka sockets, the HTTP
# listener and the worker pool.
DEFAULT_MAX_OPEN = 64

# The directory single-tenant mode and the `shared` tier both use. Named rather than
# derived so the on-disk layout does not change when tenancy is switched on: a bridge
# upgraded from Phase 2 keeps reading the store it already has.
SHARED = "shared"


class StoreRegistry:
    """`userkey -> Store`, opened on demand, evicted on an LRU.

    **Eviction drops the registry's reference; it does not close the store.** §6.1 says
    the LRU "closes" and that "closing is safe because a Store is stateless above SQLite".
    The first half is no longer what the code does and the second was disproven: a handler
    holds a store across several calls, so a concurrent request for another tenant could
    close it mid-use and the next call failed with `ProgrammingError: Cannot operate on a
    closed database`. See `_evict_if_needed` for the full argument.

    A store with live SSE subscribers is additionally **pinned** — kept in the cache, not
    merely uncollected — so a later reader resolves the SAME object and therefore sees the
    subscriber's notifications. Evicting it would leave an open page watching an object
    nothing writes to any more: updates stop with no error, the Phase 2 §6.1 symptom class.
    """

    def __init__(self, root: str | pathlib.Path, *,
                 max_open: int = DEFAULT_MAX_OPEN,
                 multi: bool = False) -> None:
        self._root = pathlib.Path(root)
        self._max_open = max(1, int(max_open))
        self._multi = multi
        # Insertion-ordered, used as the LRU: a hit moves the key to the end, eviction
        # takes from the front. `dict` has guaranteed insertion order, so this needs no
        # OrderedDict and no linked list.
        self._open: dict[str, Store] = {}
        self._lock = threading.RLock()
        self.evictions = 0
        # §6.1: an event whose `userkey` names nobody goes to `shared/` and bumps this,
        # rather than being dropped or guessed into a tenant's store. Surfaced on
        # /healthz so a misconfigured runner is visible instead of silent.
        self.unattributed = 0

    # ---- paths --------------------------------------------------------------

    def _dir_for(self, userkey: str | None) -> pathlib.Path:
        """Where one tenant's files live.

        In single-tenant mode this is the bridge root itself, NOT `shared/` — a Phase 2
        deployment already has `responses.sqlite` and `sessions.sqlite` sitting directly
        in `{tmpdir}/eventbridge/`, and relocating them on upgrade would make every
        existing session and transcript vanish from the UI. Multi-tenant mode is new, so
        it is free to use the `users/<userkey>/` layout.

        **The key is validated before it reaches the join.** It can arrive from an inbound
        Kafka header, and `Store.__init__` calls `mkdir(parents=True)`, so a key containing
        `..` would create and then write to a directory inside another tenant's store or
        outside the tree entirely. An invalid key is treated exactly like a missing one
        rather than raising, so a forged value lands in `shared/`, bumps `unattributed`
        and is visible — see `for_event`.
        """
        if not self._multi:
            return self._root
        if not tenancy.is_valid_userkey(userkey):
            return self._root / SHARED
        return self._root / "users" / userkey

    # ---- the lookup ---------------------------------------------------------

    def for_userkey(self, userkey: str | None) -> Store:
        """The `Store` for one tenant, opening it if necessary.

        In single-tenant mode every call returns the same store and `userkey` is ignored
        entirely — the same shape `TopicSet` uses, and what keeps this additive.
        """
        key = self._cache_key(userkey)
        with self._lock:
            store = self._open.get(key)
            if store is not None:
                # Mark as recently used.
                del self._open[key]
                self._open[key] = store
                return store
            store = Store(self._dir_for(userkey))
            self._open[key] = store
            # `protect=key`: the store just opened is the one being returned, so it must
            # never be the eviction victim. Without this, a cache whose older entries are
            # all pinned evicts the newest instead — and then hands the caller a store
            # whose connections are closed, which fails later as
            # `ProgrammingError: Cannot operate on a closed database` from somewhere that
            # looks unrelated to caching.
            self._evict_if_needed(protect=key)
            return store

    def _cache_key(self, userkey: str | None) -> str:
        """The LRU key. Must agree with `_dir_for` about where a key maps, or an invalid
        one would get its own cache entry whose Store points at `shared/` — two entries,
        one directory, and SQLite connections to the same files from both."""
        if not self._multi:
            return SHARED
        return userkey if tenancy.is_valid_userkey(userkey) else SHARED

    def for_event(self, event) -> Store:
        """The store an inbound response belongs in, from its `ce_userkey`.

        §6.1: an event with no `userkey` in multi-tenant mode goes to `shared/` and
        increments `unattributed` — **never silently into a user's store, and never
        dropped**. Both of those would be worse: filing it under a guess corrupts
        somebody's history, and dropping it is indistinguishable from an agent that never
        answered.
        """
        from shared import ce
        getter = getattr(event, "get", None)
        userkey = getter(ce.EXT_USERKEY) if getter else None
        # An INVALID key counts as unattributed too, not just a missing one. It is
        # truthy, so a bare `not userkey` check would let a forged `../..` sail through
        # to the path join uncounted — the forgery has to be visible, which is the whole
        # point of the counter.
        if self._multi and not tenancy.is_valid_userkey(userkey):
            with self._lock:
                self.unattributed += 1
        return self.for_userkey(userkey)

    # ---- eviction -----------------------------------------------------------

    def _evict_if_needed(self, protect: str | None = None) -> None:
        """Evict least-recently-used stores until the cache fits. Caller holds the lock.

        **Eviction drops the registry's reference; it does NOT close the store.** That
        distinction is the whole correctness argument, and closing here was a real
        use-after-close bug: a handler resolves a store, and while it is still making
        calls on it a *concurrent request for a different tenant* pushes the cache over
        its ceiling and closes it underneath. The next call fails with
        `sqlite3.ProgrammingError: Cannot operate on a closed database`, from a line that
        looks nothing to do with caching. Every multi-call read path was exposed —
        `get_html` alone makes five calls on one store — so pinning the SSE path only
        would have fixed the case that was easiest to see, not the class.

        Dropping the reference instead means CPython closes the connections when the last
        holder goes away, which is exactly the lifetime that is safe. The file descriptors
        are reclaimed slightly later than an explicit close, and that is the right trade:
        the ceiling is a soft budget, while a closed connection under an in-flight request
        is a 500.

        Two things are never evicted even in this weaker sense: `protect` (the store the
        current caller is about to use) and any store with live SSE subscribers. A
        subscribed store is kept in the cache so a later reader gets the SAME object and
        therefore sees the subscriber's notifications; evicting it would leave the viewer
        watching an object nothing writes to any more — updates stop with no error, which
        is the Phase 2 §6.1 symptom class. If everything left is pinned the cache exceeds
        `max_open` rather than breaking a viewer.
        """
        if len(self._open) <= self._max_open:
            return
        for key in list(self._open):
            if len(self._open) <= self._max_open:
                break
            if key == protect:
                continue
            if _has_subscribers(self._open[key]):
                continue
            # Reference dropped, not closed. See the docstring.
            del self._open[key]
            self.evictions += 1

    # ---- lifecycle ----------------------------------------------------------

    @property
    def open_count(self) -> int:
        with self._lock:
            return len(self._open)

    def known_userkeys(self) -> list[str]:
        """Tenants with a directory on disk, whether or not currently open.

        Read from the filesystem rather than from the open cache, because the cache is an
        LRU and says nothing about what exists — a sweeper that trusted it would skip
        every tenant that happens to be closed.
        """
        base = self._root / "users"
        if not base.is_dir():
            return []
        return sorted(p.name for p in base.iterdir() if p.is_dir())

    def close(self) -> None:
        with self._lock:
            for store in self._open.values():
                try:
                    store.close()
                except Exception:  # noqa: BLE001
                    pass
            self._open.clear()


def _has_subscribers(store: Store) -> bool:
    """Whether a store has live SSE viewers, and therefore must not be evicted.

    Reaches for a private attribute deliberately: adding a public accessor to `Store`
    for this would suggest the subscriber map is part of its API, when the eviction rule
    is the registry's concern. Defensive about the attribute's absence so an older or
    stubbed Store cannot crash eviction — treating "cannot tell" as "pinned" is the safe
    direction, since the cost is a file descriptor rather than a broken page.
    """
    subs = getattr(store, "_subscribers", None)
    if subs is None:
        return True
    try:
        return any(subs.values())
    except Exception:  # noqa: BLE001
        return True
