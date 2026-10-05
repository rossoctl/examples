"""Per-user stores and the LRU that bounds them. DESIGN_PHASE3.md §6.1.

Two properties carry the weight:

* **Isolation is structural.** A query cannot reach rows that are not in the file it
  executes against, so there is no `WHERE userkey = ?` to forget across `store.py`'s 28
  methods. `test_a_query_cannot_reach_another_tenants_rows` is that claim.
* **A subscribed store is pinned.** Evicting one would make an open SSE page stop
  updating with no error anywhere — the Phase 2 §6.1 class of bug, where the symptom
  looks like lost events rather than a closed file.
"""
from __future__ import annotations

from eventbridge.store_registry import SHARED, StoreRegistry
from shared import ce

A = "gh-alice-a1b2c3d4"
B = "gh-bob-9e8f7a6b"


def _multi(tmp_path, **kw):
    return StoreRegistry(tmp_path, multi=True, **kw)


# ---- single-tenant mode: unchanged ---------------------------------------

def test_single_mode_returns_one_store_for_everyone(tmp_path):
    """Phase 2's behaviour: one store, and a userkey changes nothing."""
    r = StoreRegistry(tmp_path, multi=False)
    assert r.for_userkey(None) is r.for_userkey(A) is r.for_userkey(B)


def test_single_mode_keeps_the_files_where_phase2_left_them(tmp_path):
    """Relocating them on upgrade would make every existing session and transcript
    vanish from the UI."""
    StoreRegistry(tmp_path, multi=False).for_userkey(None)
    assert (tmp_path / "responses.sqlite").is_file()
    assert (tmp_path / "sessions.sqlite").is_file()
    assert not (tmp_path / "users").exists()


def test_single_mode_never_counts_anything_unattributed(tmp_path):
    """There is no tenant to attribute to, so the counter must stay quiet."""
    r = StoreRegistry(tmp_path, multi=False)
    r.for_event(ce.CloudEvent(attrs={"correlationid": "brave-otter-4718"}))
    assert r.unattributed == 0


# ---- multi-tenant layout -------------------------------------------------

def test_each_tenant_gets_its_own_directory(tmp_path):
    r = _multi(tmp_path)
    r.for_userkey(A)
    r.for_userkey(B)
    assert (tmp_path / "users" / A / "sessions.sqlite").is_file()
    assert (tmp_path / "users" / B / "sessions.sqlite").is_file()


def test_the_shared_tier_has_its_own_directory(tmp_path):
    r = _multi(tmp_path)
    r.for_userkey(None)
    assert (tmp_path / SHARED / "sessions.sqlite").is_file()


def test_the_same_tenant_gets_the_same_object(tmp_path):
    r = _multi(tmp_path)
    assert r.for_userkey(A) is r.for_userkey(A)


def test_different_tenants_get_different_objects(tmp_path):
    r = _multi(tmp_path)
    assert r.for_userkey(A) is not r.for_userkey(B)


def test_a_query_cannot_reach_another_tenants_rows(tmp_path):
    """§6.1's whole argument, asserted directly: the connection IS the tenant."""
    r = _multi(tmp_path)
    sa, sb = r.for_userkey(A), r.for_userkey(B)
    sa.upsert_session("brave-otter-0001", "u1", "/w", "alice's prompt")
    sb.upsert_session("brave-otter-0002", "u2", "/w", "bob's prompt")
    assert sa.get_session("brave-otter-0001") is not None
    assert sa.get_session("brave-otter-0002") is None
    assert sb.get_session("brave-otter-0001") is None


def test_known_userkeys_reads_the_filesystem_not_the_cache(tmp_path):
    """The cache is an LRU and says nothing about what exists — a sweeper that trusted
    it would skip every tenant that happens to be closed."""
    r = _multi(tmp_path, max_open=1)
    r.for_userkey(A)
    r.for_userkey(B)          # evicts A from the cache
    assert r.open_count == 1
    assert set(r.known_userkeys()) == {A, B}


def test_known_userkeys_is_empty_before_any_tenant_exists(tmp_path):
    assert _multi(tmp_path).known_userkeys() == []


# ---- the LRU -------------------------------------------------------------

def test_the_lru_closes_the_least_recently_used(tmp_path):
    r = _multi(tmp_path, max_open=2)
    r.for_userkey("gh-a-00000001")
    r.for_userkey("gh-b-00000002")
    r.for_userkey("gh-c-00000003")
    assert r.open_count == 2
    assert r.evictions == 1


def test_a_hit_refreshes_recency(tmp_path):
    r = _multi(tmp_path, max_open=2)
    first = r.for_userkey("gh-a-00000001")
    r.for_userkey("gh-b-00000002")
    r.for_userkey("gh-a-00000001")        # refresh A, so B is now oldest
    r.for_userkey("gh-c-00000003")        # evicts B
    assert r.for_userkey("gh-a-00000001") is first


def test_an_evicted_store_reopens_with_its_data_intact(tmp_path):
    """Closing is safe because a Store is stateless above SQLite."""
    r = _multi(tmp_path, max_open=1)
    r.for_userkey(A).upsert_session("brave-otter-0001", "u1", "/w", "hello")
    r.for_userkey(B)                      # evicts A
    assert r.for_userkey(A).get_session("brave-otter-0001") is not None


def test_a_subscribed_store_is_never_evicted(tmp_path):
    """Evicting it would make an open transcript page stop updating with no error."""
    r = _multi(tmp_path, max_open=1)
    pinned = r.for_userkey(A)
    pinned.subscribe("brave-otter-0001")
    r.for_userkey(B)
    r.for_userkey("gh-c-00000003")
    # The pinned store is still the same object, and still open.
    assert r.for_userkey(A) is pinned
    assert r.evictions >= 1               # something else was evicted instead


def test_a_writer_reaches_a_live_viewer_across_an_eviction_wave(tmp_path):
    """The notification path pinning actually protects, not just object identity.

    Without the pinning check the earlier tests still passed: eviction no longer closes a
    store, so a held reference keeps working. What breaks is subtler — the *writer* calls
    `for_userkey` and gets a NEW Store object, whose `_subscribers` map is empty, so
    `insert_response` notifies nobody and the viewer's page stops updating with no error
    anywhere. That is the Phase 2 §6.1 symptom class, and this is the assertion for it.
    """
    r = _multi(tmp_path, max_open=1)
    viewer_store = r.for_userkey(A)
    ev = viewer_store.subscribe("brave-otter-0001")

    # A burst of other tenants, each of which would evict A if it were not pinned.
    for i in range(5):
        r.for_userkey(f"gh-u{i}-0000000{i}")

    # The writer resolves the store the way the responses consumer does.
    writer_store = r.for_userkey(A)
    assert writer_store is viewer_store, "writer and viewer must share one Store"
    writer_store.insert_response({
        "correlationid": "brave-otter-0001", "sequence": 1, "phase": "result",
        "final": "true", "id": "e1", "time": "2026-10-02T00:00:00Z", "data": {"text": "hi"},
    })
    assert ev.is_set(), "the viewer was never notified"


def test_the_cache_may_exceed_max_open_when_everything_is_pinned(tmp_path):
    """Going over a soft descriptor budget degrades; breaking a live viewer does not
    recover. The first is the right direction to err."""
    r = _multi(tmp_path, max_open=1)
    for uk in ("gh-a-00000001", "gh-b-00000002", "gh-c-00000003"):
        r.for_userkey(uk).subscribe("brave-otter-0001")
    assert r.open_count == 3
    assert r.evictions == 0


def test_unsubscribing_makes_a_store_evictable_again(tmp_path):
    r = _multi(tmp_path, max_open=1)
    store = r.for_userkey(A)
    ev = store.subscribe("brave-otter-0001")
    r.for_userkey(B)
    assert r.open_count == 2
    store.unsubscribe("brave-otter-0001", ev)
    r.for_userkey("gh-c-00000003")
    assert r.open_count <= 2


def test_the_store_being_returned_is_never_the_eviction_victim(tmp_path):
    """Regression: with every older entry pinned, eviction used to take the store it had
    just opened and then hand the caller a CLOSED one — surfacing much later as
    `ProgrammingError: Cannot operate on a closed database` from somewhere that looks
    unrelated to caching."""
    r = _multi(tmp_path, max_open=1)
    r.for_userkey(A).subscribe("brave-otter-0001")      # pin A
    fresh = r.for_userkey(B)
    # The returned store must be usable, which is the property the bug broke.
    fresh.upsert_session("brave-otter-0002", "u2", "/w", "hello")
    assert fresh.get_session("brave-otter-0002") is not None
    assert r.for_userkey(B) is fresh


def test_a_usable_store_is_returned_even_under_relentless_eviction(tmp_path):
    """Every call opens a new tenant with max_open=1, so each one evicts the previous.
    The store handed back must still work."""
    r = _multi(tmp_path, max_open=1)
    for i in range(6):
        uk = f"gh-u{i}-0000000{i}"
        s = r.for_userkey(uk)
        s.upsert_session(f"brave-otter-000{i}", f"u{i}", "/w", "hi")
        assert s.get_session(f"brave-otter-000{i}") is not None


def test_an_evicted_store_stays_usable_while_someone_holds_it(tmp_path):
    """Regression, and the reason eviction drops a reference rather than closing.

    A handler resolves a store and keeps making calls on it; meanwhile a concurrent
    request for a different tenant pushes the cache over its ceiling. If eviction closed
    the store, the holder's next call would fail with `ProgrammingError: Cannot operate
    on a closed database` from a line that looks unrelated to caching — and EVERY
    multi-call read path was exposed (`get_html` alone makes five calls).
    """
    r = _multi(tmp_path, max_open=1)
    held = r.for_userkey(A)
    held.upsert_session("brave-otter-0001", "u1", "/w", "hello")
    r.for_userkey(B)                       # evicts A from the cache
    # The holder must still be able to use it, both for reads and for subscribing.
    assert held.get_session("brave-otter-0001") is not None
    ev = held.subscribe("brave-otter-0001")
    assert held.events_for("brave-otter-0001") == []
    held.unsubscribe("brave-otter-0001", ev)


def test_the_sse_path_pins_its_store_before_reading(tmp_path):
    """`_sse_generator` subscribes before the replay, so the store cannot be evicted
    between resolving it and pinning it."""
    import inspect

    from eventbridge.handlers import Handlers
    src = inspect.getsource(Handlers._sse_generator)
    sub = src.index(".subscribe(")
    replay = src.index("events_for(correlationid, since_seq=since_seq)")
    assert sub < replay, "subscribe must come before the replay read"


def test_max_open_is_floored_at_one(tmp_path):
    """A zero or negative ceiling would evict every store immediately after opening
    it, which is an infinite reopen loop rather than a small cache."""
    for bad in (0, -5):
        r = StoreRegistry(tmp_path / f"m{bad}", multi=True, max_open=bad)
        r.for_userkey(A)
        assert r.open_count == 1


# ---- for_event -----------------------------------------------------------

def test_for_event_routes_by_the_events_userkey(tmp_path):
    r = _multi(tmp_path)
    evt = ce.CloudEvent(attrs={"correlationid": "brave-otter-0001",
                               ce.EXT_USERKEY: A})
    assert r.for_event(evt) is r.for_userkey(A)


def test_an_event_without_a_userkey_goes_to_shared_and_is_counted(tmp_path):
    """§6.1: never guessed into a tenant's store (that corrupts a history) and never
    dropped (that looks like an agent which never answered)."""
    r = _multi(tmp_path)
    evt = ce.CloudEvent(attrs={"correlationid": "brave-otter-0001"})
    assert r.for_event(evt) is r.for_userkey(None)
    assert r.unattributed == 1


def test_the_unattributed_counter_accumulates(tmp_path):
    r = _multi(tmp_path)
    for _ in range(3):
        r.for_event(ce.CloudEvent(attrs={"correlationid": "brave-otter-0001"}))
    assert r.unattributed == 3


def test_an_attributed_event_does_not_bump_the_counter(tmp_path):
    r = _multi(tmp_path)
    r.for_event(ce.CloudEvent(attrs={"correlationid": "x", ce.EXT_USERKEY: A}))
    assert r.unattributed == 0


def test_for_event_tolerates_a_plain_dict(tmp_path):
    """The consumer hands around both CloudEvent objects and envelope dicts."""
    r = _multi(tmp_path)
    assert r.for_event({ce.EXT_USERKEY: A}) is r.for_userkey(A)


# ---- lifecycle ----------------------------------------------------------

def test_close_closes_everything(tmp_path):
    r = _multi(tmp_path)
    r.for_userkey(A)
    r.for_userkey(B)
    r.close()
    assert r.open_count == 0


def test_store_close_wakes_subscribers(tmp_path):
    """A viewer blocked on Event.wait() would otherwise hang until its own timeout
    instead of noticing the process is going away."""
    r = _multi(tmp_path)
    store = r.for_userkey(A)
    ev = store.subscribe("brave-otter-0001")
    assert not ev.is_set()
    store.close()
    assert ev.is_set()


def test_reopening_after_close_works(tmp_path):
    r = _multi(tmp_path)
    r.for_userkey(A).upsert_session("brave-otter-0001", "u1", "/w", "hi")
    r.close()
    assert r.for_userkey(A).get_session("brave-otter-0001") is not None
