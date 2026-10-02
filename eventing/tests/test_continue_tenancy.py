"""`/continue` in multi-tenant mode: resolving the owning tenant, and refusing cleanly.

A `/continue` turn has to reach the tenant that *owns* the correlation, not the caller's
— the route is still unauthenticated (Phase 2 §3.1's capability URL), so there is no
caller identity to use, and publishing to a shared topic would run the turn on another
tenant's runner under that tenant's credential.

T5 replaced the original submitter-derived lookup with the global
`correlationid -> userkey` index (§2.6). These tests cover the index-based behaviour and
keep the refusal guarantees that matter: when the owner cannot be determined,
`TopicSet.requests(None)` raises by design, and an uncaught `ValueError` is a WSGI 500
with a stack trace rather than a status code.
"""
from __future__ import annotations

import io
import json
import pathlib
from unittest.mock import MagicMock

from eventbridge import registry
from eventbridge.config import Cfg
from eventbridge.correlation import Minter
from eventbridge.handlers import Handlers
from eventbridge.owner_index import OwnerIndex
from eventbridge.store import Store
from eventbridge.store_registry import StoreRegistry
from shared import ce, tenancy

OWNER = "mrsabath"


def _reg(*userids):
    return registry.parse(json.dumps({
        "version": 1,
        "users": [{"issuer": "github", "userid": u, "tier": "isolated"}
                  for u in userids],
    }))


def _api(tmp_path: pathlib.Path, *, multi=True, reg=None):
    """A Handlers wired the way `__main__` wires it, so the tests exercise real paths."""
    cfg = Cfg()
    cfg.tmpdir = str(tmp_path)
    cfg.tenancy_mode = tenancy.MULTI if multi else tenancy.SINGLE
    cfg.topic_prefix = "kev1"
    root = tmp_path / "eventbridge"
    owners = OwnerIndex(root) if multi else None
    stores = StoreRegistry(root, multi=multi) if multi else None
    store = stores.for_userkey(None) if stores else Store(root)
    producer = MagicMock()
    producer.publish_request.return_value = "evt-1"
    minter = Minter(index=owners)
    h = Handlers(cfg, store, producer, minter,
                 registry=reg if multi else None, stores=stores, owners=owners)
    return h, producer, owners, stores


class _Start:
    def __init__(self):
        self.status = None

    def __call__(self, status, headers):
        self.status = status


def _environ(prompt="go on"):
    body = json.dumps({"prompt": prompt}).encode()
    return {"wsgi.input": io.BytesIO(body), "CONTENT_LENGTH": str(len(body)),
            "CONTENT_TYPE": "application/json", "REQUEST_METHOD": "POST"}


def _form_environ(prompt="go on"):
    body = f"prompt={prompt}".encode()
    return {"wsgi.input": io.BytesIO(body), "CONTENT_LENGTH": str(len(body)),
            "CONTENT_TYPE": "application/x-www-form-urlencoded",
            "REQUEST_METHOD": "POST"}


def _seed(h, owners, stores, userkey):
    """Create a correlation owned by `userkey`, the way the submit path would."""
    corr = h.minter.mint(userkey=userkey)
    store = stores.for_userkey(userkey) if stores else h.store
    store.upsert_session(corr, ce.session_uuid(corr), "/w", "hello")
    store.insert_prompt(corr, "start", "hello", submitter=OWNER)
    return corr


# ---- the happy path -------------------------------------------------------

def test_continue_publishes_to_the_owning_tenants_topic(tmp_path):
    h, producer, owners, stores = _api(tmp_path, reg=_reg(OWNER))
    uk = tenancy.userkey("github", OWNER)
    corr = _seed(h, owners, stores, uk)
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid=corr)
    assert sr.status == "202 Accepted"
    assert producer.publish_request.call_args[1]["userkey"] == uk


def test_the_owner_comes_from_the_index_not_the_submitter(tmp_path):
    """T5's change: ownership is a recorded fact, not re-derived from a name.

    The submitter here is a login that is NOT in the registry, which the old
    submitter-derived lookup would have failed on. The index knows the owner anyway.
    """
    h, producer, owners, stores = _api(tmp_path, reg=registry.Registry())
    uk = tenancy.userkey("github", OWNER)
    corr = h.minter.mint(userkey=uk)
    store = stores.for_userkey(uk)
    store.upsert_session(corr, ce.session_uuid(corr), "/w", "hello")
    store.insert_prompt(corr, "start", "hello", submitter="someone-unregistered")
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid=corr)
    assert sr.status == "202 Accepted"
    assert producer.publish_request.call_args[1]["userkey"] == uk


def test_a_correlation_with_no_submitter_at_all_still_resolves(tmp_path):
    """A `prompts` row with a NULL submitter used to be unresolvable. The index does
    not consult `submitter`, so a mirror-backfilled correlation works."""
    h, producer, owners, stores = _api(tmp_path, reg=_reg(OWNER))
    uk = tenancy.userkey("github", OWNER)
    corr = h.minter.mint(userkey=uk)
    store = stores.for_userkey(uk)
    store.upsert_session(corr, ce.session_uuid(corr), "/w", "hello")
    store.insert_prompt(corr, "start", "hello", submitter=None)
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid=corr)
    assert sr.status == "202 Accepted"
    assert producer.publish_request.call_args[1]["userkey"] == uk


def test_single_mode_continue_is_unchanged(tmp_path):
    """Phase 2's behaviour: no userkey, no ownership question."""
    h, producer, owners, stores = _api(tmp_path, multi=False)
    corr = h.minter.mint()
    h.store.upsert_session(corr, ce.session_uuid(corr), "/w", "hello")
    h.store.insert_prompt(corr, "start", "hello", submitter=None)
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid=corr)
    assert sr.status == "202 Accepted"
    assert producer.publish_request.call_args[1]["userkey"] is None


# ---- refusals -------------------------------------------------------------

def test_an_unknown_correlation_is_404_not_500(tmp_path):
    """Not in the index at all: the store cannot be chosen, so there is nothing to
    read and nothing to publish."""
    h, producer, _, _ = _api(tmp_path, reg=_reg(OWNER))
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid="brave-otter-0000")
    assert sr.status == "404 Not Found"
    producer.publish_request.assert_not_called()


def test_the_html_form_refuses_rather_than_redirecting(tmp_path):
    """Redirecting silently would look like the turn was accepted and then vanished."""
    h, producer, _, _ = _api(tmp_path, reg=_reg(OWNER))
    sr = _Start()
    out = h.continue_agent_html(_form_environ(), sr, correlationid="brave-otter-0000")
    assert sr.status == "503 Service Unavailable"
    assert b"cannot resume" in b"".join(out)
    producer.publish_request.assert_not_called()


def test_the_html_form_still_redirects_on_success(tmp_path):
    h, producer, owners, stores = _api(tmp_path, reg=_reg(OWNER))
    uk = tenancy.userkey("github", OWNER)
    corr = _seed(h, owners, stores, uk)
    sr = _Start()
    h.continue_agent_html(_form_environ(), sr, correlationid=corr)
    assert sr.status == "303 See Other"
    assert producer.publish_request.call_args[1]["userkey"] == uk


def test_a_refused_continue_writes_nothing(tmp_path):
    """No orphan prompt row claiming a turn that never ran."""
    h, _, owners, stores = _api(tmp_path, reg=_reg(OWNER))
    uk = tenancy.userkey("github", OWNER)
    corr = _seed(h, owners, stores, uk)
    before = len(stores.for_userkey(uk).get_prompts(corr))
    h.continue_agent(_environ(), _Start(), correlationid="brave-otter-0000")
    assert len(stores.for_userkey(uk).get_prompts(corr)) == before


# ---- the helper's contract ------------------------------------------------

def test_owner_userkey_returns_exactly_one_of_key_or_reason(tmp_path):
    """The two-value return distinguishes "no key needed" from "owner unknown"."""
    h1, _, _, _ = _api(tmp_path / "a", multi=False)
    assert h1._owner_userkey("brave-otter-4718") == (None, None)

    h2, _, owners2, stores2 = _api(tmp_path / "b", reg=_reg(OWNER))
    uk = tenancy.userkey("github", OWNER)
    corr = _seed(h2, owners2, stores2, uk)
    key, reason = h2._owner_userkey(corr)
    assert key == uk and reason is None

    key, reason = h2._owner_userkey("brave-otter-0000")
    assert key is None and reason


def test_a_shared_tier_correlation_resolves_to_a_null_owner(tmp_path):
    """A known correlation with no tenant is a real answer, not a failure: those
    events live in `shared/` by design."""
    h, _, owners, stores = _api(tmp_path, reg=_reg(OWNER))
    corr = h.minter.mint(userkey=None)
    key, reason = h._owner_userkey(corr)
    assert key is None and reason is None


def test_store_of_returns_none_for_an_unplaceable_correlation(tmp_path):
    h, _, _, _ = _api(tmp_path, reg=_reg(OWNER))
    assert h._store_of("brave-otter-0000") is None


def test_store_of_is_the_single_store_in_single_mode(tmp_path):
    h, _, _, _ = _api(tmp_path, multi=False)
    assert h._store_of("brave-otter-0000") is h.store


# ---- cross-tenant isolation ----------------------------------------------

def test_two_tenants_get_different_stores(tmp_path):
    """The property §6.1 buys with file descriptors: the connection IS the tenant."""
    h, _, owners, stores = _api(tmp_path, reg=_reg("alice", "bob"))
    a = tenancy.userkey("github", "alice")
    b = tenancy.userkey("github", "bob")
    corr_a = _seed(h, owners, stores, a)
    corr_b = _seed(h, owners, stores, b)

    store_a = stores.for_userkey(a)
    store_b = stores.for_userkey(b)
    assert store_a is not store_b
    # Each tenant's store knows only its own correlation — not because of a WHERE
    # clause, but because the other row is in a different file.
    assert store_a.get_session(corr_a) is not None
    assert store_a.get_session(corr_b) is None
    assert store_b.get_session(corr_b) is not None
    assert store_b.get_session(corr_a) is None


def test_reads_route_to_the_owning_tenants_store(tmp_path):
    """A GET for alice's correlation must read alice's store, whoever asks."""
    h, _, owners, stores = _api(tmp_path, reg=_reg("alice", "bob"))
    a = tenancy.userkey("github", "alice")
    corr_a = _seed(h, owners, stores, a)
    assert h._store_of(corr_a) is stores.for_userkey(a)
    assert h._store_of(corr_a) is not stores.for_userkey(
        tenancy.userkey("github", "bob"))
