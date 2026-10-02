"""Human-friendly correlation ID generator: <adj>-<animal>-<4 digits>."""
from __future__ import annotations

import pathlib
import random
import re
import threading

from eventbridge.owner_index import Collision

REGEX = re.compile(r"^[a-z]{3,10}-[a-z]{3,12}-\d{4}$")


def _load(name: str) -> list[str]:
    p = pathlib.Path(__file__).resolve().parents[1] / "shared" / "words" / f"{name}.txt"
    return [w.strip() for w in p.read_text().splitlines() if w.strip() and w.strip().isalpha()]


_ADJ = _load("adjectives")
_ANI = _load("animals")


class Minter:
    """Mints unguessable-ish, human-readable correlation ids that are globally unique.

    Phase 3 §2.6: with an `index` (the `OwnerIndex`), uniqueness is enforced by one
    indexed `SELECT` against a `UNIQUE` column rather than by an in-memory `seen` set
    seeded at startup. That matters for two reasons:

    * **Correctness across tenants.** Per-user stores (§6.1) mean the old seeding loop
      would have to read `all_correlations()` from N stores — so N SQLite opens before the
      socket binds, and at 100 tenants with 10,000 correlations each that is a measurable
      startup delay for a guarantee one `SELECT` gives directly.
    * **It is what keeps `sessionuuid` unsalted.** `uuid5(NAMESPACE, correlationid)` is
      derived identically by both services with no mapping table. Salting it with the
      userkey to avoid cross-tenant collisions would break every existing session, and
      §2.6 rejects that because this uniqueness check removes the collision instead.

    Without an `index` it keeps the Phase 2 behaviour exactly — an in-memory set, seeded
    by `remember()` — so existing callers and tests are unaffected.
    """

    def __init__(self, seed: int | None = None, index=None) -> None:
        self._rng = random.Random(seed)
        self._seen: set[str] = set()
        self._lock = threading.Lock()
        self._index = index

    def mint(self, *, userkey: str | None = None) -> str:
        """Mint an id nobody holds, and claim it for `userkey` in the same breath.

        `userkey` is keyword-only, and callers reach this through `mint_for()` rather
        than passing it directly. Both choices exist for the same reason: `Minter` is
        substituted by test fakes and by anything standing in for an id generator, and a
        signature change here breaks every substitute at the *call site* rather than where
        the substitute is defined. `mint_for` keeps `mint()` callable with no arguments, so
        an older substitute keeps working untouched.

        The claim is inside the retry loop rather than after it, so a candidate that
        another thread wins between the check and the insert simply fails and the loop
        tries again. Claiming afterwards would reintroduce the race the index exists to
        close — two HTTP workers can mint concurrently, since EventBridge is
        single-replica but multi-threaded.
        """
        for _ in range(2000):
            candidate = f"{self._rng.choice(_ADJ)}-{self._rng.choice(_ANI)}-{self._rng.randint(0, 9999):04d}"
            if not REGEX.match(candidate):
                continue
            if self._index is None:
                with self._lock:
                    if candidate not in self._seen:
                        self._seen.add(candidate)
                        return candidate
                continue
            # The index is authoritative. `exists` is a fast negative filter; `claim`
            # is the atomic one that actually decides, and its Collision is what makes a
            # concurrent winner retry rather than both callers believing they own the id.
            if self._index.exists(candidate):
                continue
            try:
                self._index.claim(candidate, userkey)
            except Collision:
                # Another worker won this id between `exists` and `claim`. That is a
                # retry, not a failure.
                #
                # Deliberately NOT `except Exception`: a genuinely broken index (disk
                # full, database locked, schema missing) would then be retried 2000 times
                # and surface as `RuntimeError("correlation ID space exhausted")`, which
                # sends the reader to the word lists instead of the database. A real
                # failure belongs to the caller.
                continue
            return candidate
        raise RuntimeError("correlation ID space exhausted")

    def remember(self, corr: str) -> None:
        """Phase 2's seeding hook. A no-op when an index is configured — the index
        already knows every claimed id, so there is nothing to remember."""
        if self._index is not None:
            return
        with self._lock:
            self._seen.add(corr)


def mint_for(minter, userkey: str | None) -> str:
    """Mint from `minter`, passing `userkey` only when it can accept it.

    A free function rather than a method because the object being called may not be a
    `Minter` at all: the handlers and `GroupService` accept any object with `mint()`, and
    several tests supply deterministic fakes that predate tenancy. Probing for support
    keeps those working unchanged while real deployments get the ownership claim.

    The fallback is not silent about what it gives up — a substitute without `userkey`
    support mints an id that is NOT claimed in the owner index, so the uniqueness
    guarantee degrades to whatever that substitute implements. That is acceptable only
    because it is reachable from test doubles, never from `__main__`, which always
    constructs a real `Minter` with an index.
    """
    if _accepts_userkey(minter.mint):
        return minter.mint(userkey=userkey)
    return minter.mint()


def _accepts_userkey(fn) -> bool:
    """Whether `fn` can be called with a `userkey` keyword.

    Inspected rather than discovered by catching `TypeError` around the call: a working
    `mint()` can itself raise `TypeError` from its own body, and swallowing that would
    silently retry the mint without an ownership claim — turning a real bug into a
    missing index entry, which is far harder to notice. Anything unintrospectable is
    assumed to support it, so a `Mock` gets the keyword (and records it) rather than
    being quietly downgraded.
    """
    import inspect
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return True
    params = sig.parameters
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return True
    return "userkey" in params
