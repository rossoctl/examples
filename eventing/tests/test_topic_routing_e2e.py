"""Handler → Producer → **topic string**, with a fake Kafka. DESIGN_PHASE3.md §3.1, §6.1.

The #883 review identified this as the suite's structural blind spot, and it was right:
`create_group` shipping without a `userkey` and `/continue` 500-ing on an unowned
correlation both survived 217 new test functions, because every multi-mode test either
called `GroupService` directly with the argument the handler fails to pass, or replaced
`Producer` with a mock and asserted on its keyword arguments.

So this file fakes **one layer lower** — at `KafkaProducer`, so the real `Producer`, the
real `TopicSet` and the real handler all run — and asserts the thing that actually
matters: *which topic did the bytes go to*. `test_topicset_wiring.py` covers `TopicSet` and
`Producer` in isolation; nothing covered the chain.

The stated hazard in `test_continue_tenancy.py` is that "publishing to a shared topic would
run the turn on another tenant's runner, under that tenant's credential". These are the
assertions for that sentence.
"""
from __future__ import annotations

import io
import json

import pytest

from eventbridge import auth, registry
from eventbridge.config import Cfg
from eventbridge.correlation import Minter
from eventbridge.group_service import GroupService
from eventbridge.handlers import Handlers
from eventbridge.kafka_out import Producer
from eventbridge.owner_index import OwnerIndex
from eventbridge.store_registry import StoreRegistry
from shared import ce, tenancy

ALICE = "alice"
BOB = "bob"
UK_A = tenancy.userkey(auth.ISSUER_STATIC, ALICE)
UK_B = tenancy.userkey(auth.ISSUER_STATIC, BOB)


class FakeKafkaProducer:
    """Records `(topic, headers, value)` for every send. The seam the suite was missing.

    Deliberately NOT a `Producer` stand-in: the point is that `Producer.publish_request`
    and `TopicSet.requests` both execute for real, so a missing `userkey` at the handler
    shows up here as the wrong topic rather than as an absent keyword argument.
    """

    def __init__(self, *a, **kw):
        self.sent: list[dict] = []

    def send(self, topic, *, key=None, value=None, headers=None):
        self.sent.append({"topic": topic, "key": key, "value": value,
                          "headers": dict(headers or [])})

        class _F:
            def get(self, timeout=None):
                return None
        return _F()

    def flush(self, timeout=None):
        pass

    def close(self, timeout=None):
        pass

    # ---- helpers the tests read ----
    @property
    def topics(self) -> list[str]:
        return [s["topic"] for s in self.sent]

    def events(self):
        return [ce.from_kafka_binary(list(s["headers"].items()), s["value"])
                for s in self.sent]


@pytest.fixture
def api(tmp_path, monkeypatch):
    """A real Handlers + real Producer over a fake Kafka, in multi-tenant mode."""
    monkeypatch.setattr("eventbridge.kafka_out.KafkaProducer", FakeKafkaProducer)
    cfg = Cfg()
    cfg.tmpdir = str(tmp_path)
    cfg.tenancy_mode = tenancy.MULTI
    cfg.topic_prefix = "kev1"
    cfg.auth_tokens = {f"tok-{ALICE}": ALICE, f"tok-{BOB}": BOB}
    root = tmp_path / "eventbridge"
    owners = OwnerIndex(root)
    stores = StoreRegistry(root, multi=True)
    store = stores.for_userkey(None)
    producer = Producer("b:9092", "requests", "rossoctl://eventbridge/test",
                        response_topic="responses", topics=cfg.topics)
    minter = Minter(index=owners)
    groups = GroupService(cfg, store, producer, minter, stores=stores)
    reg = registry.parse(json.dumps({
        "version": 1,
        "users": [{"issuer": auth.ISSUER_STATIC, "userid": u, "tier": "isolated"}
                  for u in (ALICE, BOB)]}))
    h = Handlers(cfg, store, producer, minter, groups=groups, registry=reg,
                 stores=stores, owners=owners)
    return h, producer._prod, owners, stores


class _Start:
    def __init__(self):
        self.status = None

    def __call__(self, status, headers):
        self.status = status


def _post(body, token):
    raw = json.dumps(body).encode()
    return {"wsgi.input": io.BytesIO(raw), "CONTENT_LENGTH": str(len(raw)),
            "CONTENT_TYPE": "application/json", "REQUEST_METHOD": "POST",
            "QUERY_STRING": "", "HTTP_AUTHORIZATION": f"Bearer {token}"}


# ---- POST /v0/agents -----------------------------------------------------

def test_start_agent_publishes_to_the_callers_own_requests_topic(api):
    h, kafka, _, _ = api
    sr = _Start()
    h.start_agent(_post({"prompt": "hi"}, f"tok-{ALICE}"), sr)
    assert sr.status.startswith("202"), sr.status
    assert kafka.topics == [f"kev1-{UK_A}-requests"]


def test_two_tenants_never_share_a_requests_topic(api):
    """The property the whole phase is for, asserted on the wire."""
    h, kafka, _, _ = api
    h.start_agent(_post({"prompt": "a"}, f"tok-{ALICE}"), _Start())
    h.start_agent(_post({"prompt": "b"}, f"tok-{BOB}"), _Start())
    assert kafka.topics == [f"kev1-{UK_A}-requests", f"kev1-{UK_B}-requests"]
    assert len(set(kafka.topics)) == 2


def test_the_published_event_carries_the_matching_userkey(api):
    """The topic and the attribute have to agree, or the runner's response comes back
    stamped for a different tenant than the one whose topic served it."""
    h, kafka, _, _ = api
    h.start_agent(_post({"prompt": "hi"}, f"tok-{ALICE}"), _Start())
    evt = kafka.events()[0]
    assert evt.attrs[ce.EXT_USERKEY] == UK_A
    assert kafka.topics[0] == f"kev1-{UK_A}-requests"


def test_no_request_is_ever_published_to_the_single_mode_topic(api):
    """`requests` is the single-tenant name. Seeing it in multi mode means a userkey was
    dropped somewhere between the handler and `TopicSet`."""
    h, kafka, _, _ = api
    h.start_agent(_post({"prompt": "a"}, f"tok-{ALICE}"), _Start())
    h.create_group(_post({"prompts": ["x", "y"]}, f"tok-{BOB}"), _Start())
    assert "requests" not in kafka.topics
    assert "responses" not in kafka.topics


# ---- POST /v0/groups -----------------------------------------------------

def test_create_group_publishes_members_and_the_started_event_to_one_tenant(api):
    """The `create_group` defect, caught at the layer that would have caught it.

    The group lifecycle event goes to the tenant's RESPONSES topic (§21.2: EventRunner
    would try to execute anything on requests), and every member to their requests topic.
    All of them belong to the same tenant.
    """
    h, kafka, owners, stores = api
    sr = _Start()
    out = h.create_group(_post({"prompts": ["p1", "p2"]}, f"tok-{ALICE}"), sr)
    assert sr.status.startswith("202"), sr.status
    gid = json.loads(b"".join(out))["groupid"]

    assert kafka.topics.count(f"kev1-{UK_A}-requests") == 2
    assert f"kev1-{UK_A}-responses" in kafka.topics
    assert all(UK_A in t for t in kafka.topics), kafka.topics
    # And the store side agrees with the wire side.
    assert stores.for_userkey(UK_A).get_group(gid) is not None
    assert owners.owner_of(gid) == (UK_A, True)


def test_a_group_event_is_never_published_to_another_tenants_topic(api):
    h, kafka, _, _ = api
    h.create_group(_post({"prompts": ["p1"]}, f"tok-{ALICE}"), _Start())
    assert not [t for t in kafka.topics if UK_B in t]


# ---- POST /continue ------------------------------------------------------

def test_continue_publishes_to_the_owning_tenants_topic(api):
    h, kafka, _, _ = api
    out = h.start_agent(_post({"prompt": "hi"}, f"tok-{ALICE}"), _Start())
    corr = json.loads(b"".join(out))["correlationid"]
    kafka.sent.clear()

    sr = _Start()
    # Note the credential: BOB's. `/continue` is unauthenticated (Phase 2 §3.1's
    # capability URL), so what decides the topic is the correlation's OWNER, not the
    # caller — which is what stops a resume running under another tenant's credential.
    h.continue_agent(_post({"prompt": "go on"}, f"tok-{BOB}"), sr, correlationid=corr)
    assert sr.status.startswith("202"), sr.status
    assert kafka.topics == [f"kev1-{UK_A}-requests"]


def test_continue_for_an_unowned_correlation_publishes_nothing(api):
    """The `/continue` 500: it used to write a prompt row and then raise. Nothing may
    reach Kafka, and nothing may be written."""
    h, kafka, owners, stores = api
    corr = h.minter.mint(userkey=None)          # shared tier: known, unowned
    stores.for_userkey(None).upsert_session(corr, ce.session_uuid(corr), "/w", "hi")
    kafka.sent.clear()

    sr = _Start()
    h.continue_agent(_post({"prompt": "go on"}, f"tok-{ALICE}"), sr, correlationid=corr)
    assert sr.status.startswith(("404", "503")), sr.status
    assert kafka.sent == []
    assert stores.for_userkey(None).get_prompts(corr) == []


def test_continue_for_an_unknown_correlation_publishes_nothing(api):
    h, kafka, _, _ = api
    sr = _Start()
    h.continue_agent(_post({"prompt": "go on"}, f"tok-{ALICE}"), sr,
                     correlationid="brave-otter-0000")
    assert sr.status.startswith("404"), sr.status
    assert kafka.sent == []


# ---- single-tenant mode --------------------------------------------------

@pytest.fixture
def single(tmp_path, monkeypatch):
    monkeypatch.setattr("eventbridge.kafka_out.KafkaProducer", FakeKafkaProducer)
    cfg = Cfg()
    cfg.tmpdir = str(tmp_path)
    root = tmp_path / "eventbridge"
    stores = StoreRegistry(root, multi=False)
    store = stores.for_userkey(None)
    producer = Producer("b:9092", "requests", "rossoctl://eventbridge/test",
                        response_topic="responses", topics=cfg.topics)
    minter = Minter(index=OwnerIndex(root))
    groups = GroupService(cfg, store, producer, minter)
    return (Handlers(cfg, store, producer, minter, groups=groups),
            producer._prod)


def test_single_mode_publishes_to_the_configured_topics(single):
    """Phase 2's wire behaviour, asserted on the wire rather than inferred."""
    h, kafka = single
    out = h.start_agent(_post({"prompt": "hi"}, "unused"), _Start())
    corr = json.loads(b"".join(out))["correlationid"]
    h.continue_agent(_post({"prompt": "more"}, "unused"), _Start(), correlationid=corr)
    h.create_group(_post({"prompts": ["p1"]}, "unused"), _Start())
    assert set(kafka.topics) == {"requests", "responses"}


def test_single_mode_puts_no_userkey_on_the_wire(single):
    h, kafka = single
    h.start_agent(_post({"prompt": "hi"}, "unused"), _Start())
    assert ce.EXT_USERKEY not in kafka.events()[0].attrs
