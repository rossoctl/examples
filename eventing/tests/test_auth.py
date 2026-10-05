"""Bearer-token auth on the submit path.

Mirrors the harness in test_groups.py (its `_call` helper and fixture shape)
rather than inventing a second one, so header injection uses the same
`environ={...}` seam the Idempotency-Key tests already use.
"""
from __future__ import annotations

import io
import json

import pytest

from eventbridge import auth
from eventbridge.config import Cfg as EbCfg
from eventbridge.group_service import GroupService
from eventbridge.handlers import Handlers
from eventbridge.store import Store

TOKENS = "alice:s3cret-alice,bob:s3cret-bob"


class FakeProducer:
    def __init__(self):
        self.requests = []
        self.group_events = []

    def publish_request(self, **kw):
        self.requests.append(kw)
        return f"evt-{len(self.requests)}"

    def publish_group_event(self, *, type_, groupid, data, subject="group",
                            userkey=None):
        self.group_events.append({"type": type_, "groupid": groupid, "data": data,
                                  "userkey": userkey})
        return f"gevt-{len(self.group_events)}"


class SeqMinter:
    def __init__(self):
        self.n = 0

    def mint(self):
        self.n += 1
        return f"test-agent-{self.n:04d}"

    def remember(self, corr):
        pass


def _call(handler, *, body=None, method="GET", qs="", **kw):
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    raw = json.dumps(body).encode() if body is not None else b""
    environ = {"wsgi.input": io.BytesIO(raw), "CONTENT_LENGTH": str(len(raw)) if raw else "",
               "QUERY_STRING": qs, "REQUEST_METHOD": method}
    environ.update(kw.pop("environ", {}))
    out = handler(environ, start_response, **kw)
    return captured["status"], captured["headers"], b"".join(out)


def _api(tmp_path, tokens: str = ""):
    store = Store(tmp_path / "eb")
    cfg = EbCfg(tmpdir=str(tmp_path), public_base_url="http://eb.test",
                auth_tokens=auth.parse_tokens(tokens) if tokens else {})
    producer = FakeProducer()
    groups = GroupService(cfg, store, producer, SeqMinter())
    return Handlers(cfg, store, producer, SeqMinter(), groups=groups), store, producer


def _bearer(tok: str) -> dict:
    return {"environ": {"HTTP_AUTHORIZATION": f"Bearer {tok}"}}


# ---- parse_tokens -----------------------------------------------------------

def test_parse_tokens_maps_token_to_name():
    assert auth.parse_tokens(TOKENS) == {"s3cret-alice": "alice", "s3cret-bob": "bob"}


@pytest.mark.parametrize("raw", ["", "   ", "noColon", ":tok", "name:", ",,", "name"])
def test_parse_tokens_skips_malformed_entries(raw):
    """A typo must not take the service down, and must not authenticate."""
    assert auth.parse_tokens(raw) == {}


def test_parse_tokens_tolerates_whitespace_and_keeps_good_entries():
    assert auth.parse_tokens(" alice : tok1 , junk , bob:tok2 ") == {
        "tok1": "alice", "tok2": "bob"}


# ---- resolve_identity -------------------------------------------------------

def test_resolve_identity_disabled_when_no_tokens():
    """(None, None) means 'allowed, anonymous' — the default demo path."""
    assert auth.resolve_identity({}, {}) == (None, None)


def test_resolve_identity_accepts_valid_token():
    ident, why = auth.resolve_identity(
        {"HTTP_AUTHORIZATION": "Bearer s3cret-bob"}, auth.parse_tokens(TOKENS))
    assert (ident, why) == ("bob", None)


def test_resolve_identity_scheme_is_case_insensitive():
    ident, why = auth.resolve_identity(
        {"HTTP_AUTHORIZATION": "bearer s3cret-alice"}, auth.parse_tokens(TOKENS))
    assert (ident, why) == ("alice", None)


@pytest.mark.parametrize("header,expect", [
    ("",                      "authentication required"),
    ("Basic dXNlcjpwYXNz",    "unsupported authentication scheme"),
    ("s3cret-alice",          "unsupported authentication scheme"),
    ("Bearer ",               "empty bearer token"),
    ("Bearer wrong",          "invalid bearer token"),
])
def test_resolve_identity_rejects(header, expect):
    ident, why = auth.resolve_identity(
        {"HTTP_AUTHORIZATION": header} if header else {}, auth.parse_tokens(TOKENS))
    assert ident is None
    assert why is not None and expect in why


def test_resolve_identity_never_echoes_the_presented_token():
    """The 401 body is returned to the client; it must not reflect a secret."""
    _, why = auth.resolve_identity(
        {"HTTP_AUTHORIZATION": "Bearer hunter2"}, auth.parse_tokens(TOKENS))
    assert "hunter2" not in why


def test_resolve_identity_handles_non_ascii_token():
    """compare_digest raises TypeError on non-ASCII str; must reject, not 500."""
    ident, why = auth.resolve_identity(
        {"HTTP_AUTHORIZATION": "Bearer paßwort"}, auth.parse_tokens(TOKENS))
    assert ident is None and why is not None


# ---- POST /v0/agents --------------------------------------------------------

def test_start_agent_open_when_auth_disabled(tmp_path):
    h, _, producer = _api(tmp_path)
    status, _, _ = _call(h.start_agent, method="POST", body={"prompt": "hi"})
    assert status.startswith("202")
    assert producer.requests[0].get("submitter") is None


def test_start_agent_401_without_credential(tmp_path):
    h, _, producer = _api(tmp_path, TOKENS)
    status, headers, body = _call(h.start_agent, method="POST", body={"prompt": "hi"})
    assert status.startswith("401")
    assert headers.get("WWW-Authenticate") == auth.CHALLENGE
    assert json.loads(body)["error"]
    assert producer.requests == []            # nothing was enqueued


def test_start_agent_401_with_wrong_credential(tmp_path):
    """Distinguishes real validation from a mere presence check."""
    h, _, producer = _api(tmp_path, TOKENS)
    status, _, _ = _call(h.start_agent, method="POST", body={"prompt": "hi"},
                         **_bearer("not-the-token"))
    assert status.startswith("401")
    assert producer.requests == []


def test_start_agent_202_with_valid_credential_and_records_submitter(tmp_path):
    h, store, producer = _api(tmp_path, TOKENS)
    status, _, body = _call(h.start_agent, method="POST", body={"prompt": "hi"},
                            **_bearer("s3cret-alice"))
    assert status.startswith("202")
    corr = json.loads(body)["correlationid"]
    # on the event...
    assert producer.requests[0]["submitter"] == "alice"
    # ...and in the store, which is what the HTML transcript renders from.
    assert store.get_prompts(corr)[0]["submitter"] == "alice"


def test_each_token_resolves_to_its_own_name(tmp_path):
    h, _, producer = _api(tmp_path, TOKENS)
    _call(h.start_agent, method="POST", body={"prompt": "a"}, **_bearer("s3cret-alice"))
    _call(h.start_agent, method="POST", body={"prompt": "b"}, **_bearer("s3cret-bob"))
    assert [r["submitter"] for r in producer.requests] == ["alice", "bob"]


def test_auth_runs_before_body_validation(tmp_path):
    """An unauthenticated caller learns nothing about which bodies are valid."""
    h, _, _ = _api(tmp_path, TOKENS)
    status, _, _ = _call(h.start_agent, method="POST", body={})   # no prompt -> would be 400
    assert status.startswith("401")


# ---- POST /v0/groups --------------------------------------------------------

def test_create_group_401_without_credential(tmp_path):
    h, _, producer = _api(tmp_path, TOKENS)
    status, headers, _ = _call(h.create_group, method="POST",
                               body={"label": "b", "prompts": ["one", "two"]})
    assert status.startswith("401")
    assert headers.get("WWW-Authenticate") == auth.CHALLENGE
    assert producer.requests == []


def test_create_group_202_stamps_submitter_on_every_member(tmp_path):
    h, _, producer = _api(tmp_path, TOKENS)
    status, _, body = _call(h.create_group, method="POST",
                            body={"label": "b", "prompts": ["one", "two", "three"]},
                            **_bearer("s3cret-bob"))
    assert status.startswith("202")
    assert len(json.loads(body)["members"]) == 3
    assert [r["submitter"] for r in producer.requests] == ["bob"] * 3


def test_create_group_auth_precedes_groups_disabled_check(tmp_path):
    """503 leaks whether group support is on; 401 must win."""
    store = Store(tmp_path / "eb")
    cfg = EbCfg(tmpdir=str(tmp_path), auth_tokens=auth.parse_tokens(TOKENS))
    h = Handlers(cfg, store, FakeProducer(), SeqMinter(), groups=None)
    status, _, _ = _call(h.create_group, method="POST", body={"prompts": ["x"]})
    assert status.startswith("401")


# ---- routes that must stay open ---------------------------------------------

def test_continue_stays_open_for_the_ntfy_action_button(tmp_path):
    """Guarding /continue would need a token inside every phone notification,
    which is worse. Continuing requires an unguessable correlationid instead."""
    h, _, producer = _api(tmp_path, TOKENS)
    _, _, body = _call(h.start_agent, method="POST", body={"prompt": "hi"},
                       **_bearer("s3cret-alice"))
    corr = json.loads(body)["correlationid"]
    status, _, _ = _call(h.continue_agent, method="POST", body={"prompt": "more"},
                         correlationid=corr)
    assert status.startswith("202")


def test_healthz_stays_open(tmp_path):
    h, _, _ = _api(tmp_path, TOKENS)
    status, _, _ = _call(h.healthz)
    assert status.startswith("200")
