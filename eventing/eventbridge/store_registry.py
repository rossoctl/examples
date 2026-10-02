"""Per-user stores, opened lazily and closed on an LRU. DESIGN_PHASE3.md §6.1.

```text
{tmpdir}/eventbridge/
  users/
    gh-mrsabath-4c1d9e07/
      responses.sqlite        # responses, keyed (correlationid, sequence)
      sessions.sqlite         # sessions, prompts, groups, group_members, transcripts
    gh-aslom-7b2e55a1/
      ...
  shared/                     # tier `shared` (§3.8), and ALL of single-tenant mode
```

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

# §6.1 / §8.1 `EB_MAX_OPEN_STORES`. 64 pairs is ~192 file descriptors with WAL, which
# leaves comfortable room under a 1024 soft limit for the Kafka sockets, the HTTP
# listener and the worker pool.
DEFAULT_MAX_OPEN = 64

# The directory single-tenant mode and the `shared` tier both use. Named rather than
# derived so the on-disk layout does not change when tenancy is switched on: a bridge
# upgraded from Phase 2 keeps reading the store it already has.
SHARED = "shared"


class StoreRegistry:
    """`userkey -> Store`, opened on demand, closed on an LRU.

    Closing is safe because a `Store` is stateless above SQLite — every method opens a
    transaction and commits, so there is no in-memory state to lose. The ONE thing that
    is not: `Store.subscribe()` holds `threading.Event`s for live SSE viewers, so a store
    with subscribers is **pinned** and exempt from eviction. Evicting it would make an
    open transcript page stop updating with no error anywhere — the Phase 2 §6.1 class of
    bug, where the symptom looks like lost events rather than like a closed file.
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
        """
        if not self._multi:
            return self._root
        if not userkey:
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
        if not self._multi:
            return SHARED
        return userkey or SHARED

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
        if self._multi and not userkey:
            with self._lock:
                self.unattributed += 1
        return self.for_userkey(userkey)

    # ---- eviction -----------------------------------------------------------

    def _evict_if_needed(self, protect: str | None = None) -> None:
        """Close least-recently-used stores until the cache fits. Caller holds the lock.

        Two things are never evicted: `protect` (the store the current caller is about to
        use) and any store with live SSE subscribers. If everything left is pinned the
        cache is allowed to exceed `max_open` rather than breaking a live viewer — going
        over a soft descriptor budget degrades and is visible in `/healthz`, while
        evicting a subscribed store silently stops a page from updating. The first is
        recoverable, the second is not.
        """
        if len(self._open) <= self._max_open:
            return
        for key in list(self._open):
            if len(self._open) <= self._max_open:
                break
            if key == protect:
                continue
            store = self._open[key]
            if _has_subscribers(store):
                continue
            del self._open[key]
            try:
                store.close()
            except Exception:  # noqa: BLE001 - a failed close must not fail a request
                pass
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
