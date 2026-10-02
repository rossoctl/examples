"""`/continue` in multi-tenant mode: resolving the owning tenant, and refusing cleanly.

A `/continue` turn has to reach the tenant that *owns* the correlation, not the caller's
— the route is still unauthenticated (Phase 2 §3.1's capability URL), so there is no
caller identity to use, and publishing to a shared topic would run the turn on another
tenant's runner under that tenant's credential.

When the owner cannot be determined, `TopicSet.requests(None)` raises by design. These
tests pin that the handler refuses with a status code first, because an uncaught
`ValueError` is a WSGI 500 with a stack trace, and three of the ways to reach it are
ordinary operation rather than abuse:

* a user removed from the registry;
* a `prompts` row whose `submitter` is NULL (nullable, and pre-dates auth);
* a correlation back-filled from the topic by `RequestsMirror`, which records no
  submitter at all.
"""
from __future__ import annotations

import json
import pathlib
from unittest.mock import MagicMock

import pytest

from eventbridge import registry
from eventbridge.config import Cfg
from eventbridge.correlation import Minter
from eventbridge.handlers import Handlers
from eventbridge.store import Store
from shared import ce, tenancy

CORR = "brave-otter-4718"
OWNER = "mrsabath"


def _reg(*userids):
    return registry.parse(json.dumps({
        "version": 1,
        "users": [{"issuer": "github", "userid": u, "tier": "isolated"}
                  for u in userids],
    }))


def _handlers(tmp_path: pathlib.Path, *, mode=tenancy.MULTI, reg=None, submitter=OWNER):
    cfg = Cfg()
    cfg.tmpdir = str(tmp_path)
    cfg.tenancy_mode = mode
    cfg.topic_prefix = "kev1"
    store = Store(tmp_path)
    store.upsert_session(CORR, ce.session_uuid(CORR), str(tmp_path / "w"), "hello")
    # `submitter=None` is the pre-auth / mirror-backfilled shape: the column is
    # nullable, so this is a real row, not a contrived one.
    store.insert_prompt(CORR, "start", "hello", submitter=submitter)
    producer = MagicMock()
    producer.publish_request.return_value = "evt-1"
    h = Handlers(cfg, store, producer, Minter(),
                 registry=reg if mode == tenancy.MULTI else None)
    return h, producer


class _Start:
    """Captures the WSGI status line."""

    def __init__(self):
        self.status = None

    def __call__(self, status, headers):
        self.status = status


def _environ(prompt="go on"):
    body = json.dumps({"prompt": prompt}).encode()
    import io
    return {"wsgi.input": io.BytesIO(body), "CONTENT_LENGTH": str(len(body)),
            "CONTENT_TYPE": "application/json", "REQUEST_METHOD": "POST"}


def _form_environ(prompt="go on"):
    import io
    body = f"prompt={prompt}".encode()
    return {"wsgi.input": io.BytesIO(body), "CONTENT_LENGTH": str(len(body)),
            "CONTENT_TYPE": "application/x-www-form-urlencoded",
            "REQUEST_METHOD": "POST"}


# ---- the happy path -------------------------------------------------------

def test_continue_publishes_to_the_owning_tenants_topic(tmp_path):
    h, producer = _handlers(tmp_path, reg=_reg(OWNER))
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid=CORR)
    assert sr.status == "202 Accepted"
    assert producer.publish_request.call_args[1]["userkey"] == \
        tenancy.userkey("github", OWNER)


def test_single_mode_continue_is_unchanged(tmp_path):
    """Phase 2's behaviour: no userkey, no ownership question."""
    h, producer = _handlers(tmp_path, mode=tenancy.SINGLE)
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid=CORR)
    assert sr.status == "202 Accepted"
    assert producer.publish_request.call_args[1]["userkey"] is None


def test_single_mode_continue_works_with_no_submitter_recorded(tmp_path):
    """The pre-auth row shape must not become a refusal in the default mode."""
    h, _ = _handlers(tmp_path, mode=tenancy.SINGLE, submitter=None)
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid=CORR)
    assert sr.status == "202 Accepted"


# ---- the three refusals ---------------------------------------------------

def test_a_submitter_no_longer_in_the_registry_is_a_503_not_a_500(tmp_path):
    h, producer = _handlers(tmp_path, reg=_reg("someone-else"))
    sr = _Start()
    out = h.continue_agent(_environ(), sr, correlationid=CORR)
    assert sr.status == "503 Service Unavailable"
    assert OWNER in json.loads(b"".join(out))["error"]
    producer.publish_request.assert_not_called()


def test_a_null_submitter_is_a_503_not_a_500(tmp_path):
    """`prompts.submitter` is nullable and pre-dates auth, so this is a real row."""
    h, producer = _handlers(tmp_path, reg=_reg(OWNER), submitter=None)
    sr = _Start()
    out = h.continue_agent(_environ(), sr, correlationid=CORR)
    assert sr.status == "503 Service Unavailable"
    assert "no submitter recorded" in json.loads(b"".join(out))["error"]
    producer.publish_request.assert_not_called()


def test_an_empty_registry_is_a_503_not_a_500(tmp_path):
    h, producer = _handlers(tmp_path, reg=registry.Registry())
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid=CORR)
    assert sr.status == "503 Service Unavailable"
    producer.publish_request.assert_not_called()


def test_a_refused_continue_writes_nothing(tmp_path):
    """No orphan prompt row claiming a turn that never ran."""
    h, _ = _handlers(tmp_path, reg=_reg("someone-else"))
    before = len(h.store.get_prompts(CORR))
    h.continue_agent(_environ(), _Start(), correlationid=CORR)
    assert len(h.store.get_prompts(CORR)) == before


# ---- the HTML form path ---------------------------------------------------

def test_the_html_form_refuses_with_503_rather_than_redirecting(tmp_path):
    """Redirecting silently would look like the turn was accepted and then vanished,
    which is exactly the symptom class this phase keeps trying to avoid."""
    h, producer = _handlers(tmp_path, reg=_reg("someone-else"))
    sr = _Start()
    out = h.continue_agent_html(_form_environ(), sr, correlationid=CORR)
    assert sr.status == "503 Service Unavailable"
    assert b"cannot resume" in b"".join(out)
    producer.publish_request.assert_not_called()


def test_the_html_form_still_redirects_on_success(tmp_path):
    h, producer = _handlers(tmp_path, reg=_reg(OWNER))
    sr = _Start()
    h.continue_agent_html(_form_environ(), sr, correlationid=CORR)
    assert sr.status == "303 See Other"
    assert producer.publish_request.call_args[1]["userkey"] == \
        tenancy.userkey("github", OWNER)


# ---- the helper's contract ------------------------------------------------

def test_owner_userkey_returns_exactly_one_of_key_or_reason(tmp_path):
    """The two-value return exists because a bare `None` cannot distinguish "single
    mode, no key needed" from "multi mode, owner unknown"."""
    h, _ = _handlers(tmp_path, mode=tenancy.SINGLE)
    assert h._owner_userkey(CORR) == (None, None)

    h2, _ = _handlers(tmp_path / "b", reg=_reg(OWNER))
    key, reason = h2._owner_userkey(CORR)
    assert key and reason is None

    h3, _ = _handlers(tmp_path / "c", reg=_reg("nobody"))
    key, reason = h3._owner_userkey(CORR)
    assert key is None and reason


def test_an_unknown_correlation_in_multi_mode_still_404s(tmp_path):
    """The ownership check must not shadow the existing 404."""
    h, _ = _handlers(tmp_path, reg=_reg(OWNER))
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid="brave-otter-0000")
    assert sr.status == "404 Not Found"


@pytest.mark.parametrize("spelling", ["MrSabath", "MRSABATH"])
def test_owner_resolution_folds_github_case(tmp_path, spelling):
    """A correlation submitted as `MrSabath` must resolve to the same tenant as
    `mrsabath`, or a resume lands in a different tenant than the original turn."""
    h, producer = _handlers(tmp_path, reg=_reg(OWNER), submitter=spelling)
    sr = _Start()
    h.continue_agent(_environ(), sr, correlationid=CORR)
    assert sr.status == "202 Accepted"
    assert producer.publish_request.call_args[1]["userkey"] == \
        tenancy.userkey("github", OWNER)
