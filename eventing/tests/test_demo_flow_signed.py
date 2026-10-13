"""`k8s_demo_flow.py`'s overlay wiring and the forgery it publishes. #896.

The stages themselves need a cluster, but three things that decide whether the demo
proves anything do not — and one of them was silently wrong before this:

  * which overlay runs in mock mode (`self.mock`)
  * which overlay runs the signed beats (`self.signed_beats`)
  * whether the forged event the demo publishes is the event the consumer refuses

The third is the one worth a test rather than a comment. A forgery that is malformed
would be refused for the wrong reason — `from_kafka_binary` would simply fail to decode
it — and the demo would appear to pass while proving nothing about the signature check.
"""
from __future__ import annotations

import binascii
import sys

import k8s_demo_flow as flow

from shared import ce, keyset
from shared import signing as S

SEED_EB = binascii.unhexlify(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
SEED_R1 = binascii.unhexlify(
    "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb")


def _flow(overlay: str, *extra):
    """A Flow with no Kubectl and no Checks — only the constructor's own decisions."""
    args = flow.build_args(["--overlay", overlay, *extra])
    f = flow.Flow.__new__(flow.Flow)
    f.args = args
    f.mock = args.overlay != "demo"
    f.signed_beats = (args.signed_beats if args.signed_beats is not None
                      else args.overlay == "kind-signed")
    return f


# ---- which overlay means what ------------------------------------------------

def test_kind_signed_is_an_accepted_overlay():
    """`k8s_deploy.py` already accepted it and already required --signing-keys for it;
    the demo flow's `--overlay` was the one place the chain broke, so the overlay
    existed and nothing could drive it."""
    assert flow.build_args(["--overlay", "kind-signed"]).overlay == "kind-signed"


def test_only_the_demo_overlay_runs_a_real_agent():
    """The bug this fixes. `self.mock` read `overlay == "test"`, but only the `demo`
    overlay sets `ER_MOCK_CLAUDE=false` — so `kind` and `kind-signed` run mocked while
    the flow asserted a real-agent reply.

    Worse than a clean failure: the mock reply is `MOCK-REPLY[<prompt>]` and the prompt
    contains the text stage 2 looks for, so the assertion could pass by accident.
    """
    assert _flow("test").mock is True
    assert _flow("kind").mock is True
    assert _flow("kind-signed").mock is True
    assert _flow("demo").mock is False


def test_the_signed_beats_default_on_only_for_the_signed_overlay():
    """§8.5.2 — state the default. `kind-signed` exists for nothing else, and the beats
    would fail on an overlay that does not enforce."""
    assert _flow("kind-signed").signed_beats is True
    assert _flow("test").signed_beats is False
    assert _flow("kind").signed_beats is False
    assert _flow("demo").signed_beats is False


def test_the_signed_beats_can_be_forced_either_way():
    """So the forgery can be demonstrated against any enforcing deployment, and skipped
    on `kind-signed` when only the scaling beats are wanted."""
    assert _flow("kind", "--signed-beats").signed_beats is True
    assert _flow("kind-signed", "--no-signed-beats").signed_beats is False


# ---- the forgery is the real thing ------------------------------------------

def _run_forge(topic="kev1-responses", gid="test-group-0001",
               corr="test-agent-0001", bootstrap="broker:9092", seq=9001):
    """Execute FORGE_SRC with a fake KafkaProducer, returning what it published.

    The source is run rather than parsed, because the thing under test is that the
    events it builds decode and get refused — which only executing it can show.
    """
    sent: list[tuple] = []

    class _Producer:
        def __init__(self, **_kw):
            pass

        def send(self, topic, *, key, value, headers):
            sent.append((topic, key, value, headers))

        def flush(self, timeout=None):
            pass

    src = flow.FORGE_SRC % {"topic": topic, "gid": gid, "corr": corr,
                            "bootstrap": bootstrap, "seq": seq}
    fake_kafka = type(sys)("kafka")
    fake_kafka.KafkaProducer = _Producer
    saved = sys.modules.get("kafka")
    sys.modules["kafka"] = fake_kafka
    try:
        exec(compile(src, "<forge>", "exec"), {"__name__": "__forge__"})
    finally:
        if saved is not None:
            sys.modules["kafka"] = saved
        else:
            sys.modules.pop("kafka", None)
    return sent


def test_the_forgery_publishes_both_events_onto_the_response_topic():
    sent = _run_forge()
    assert len(sent) == 2, "a member terminal and a group.completed"
    assert {t for t, _k, _v, _h in sent} == {"kev1-responses"}
    # Keyed so the member lands on its correlation's partition, as a genuine
    # publisher's would — ordering per correlation is the Phase 0 contract.
    assert sent[0][1] == b"test-agent-0001"
    assert sent[1][1] == b"test-group-0001"


def test_the_forged_events_decode_as_well_formed_cloudevents():
    """If they did not, `from_kafka_binary` would fail before any signature check and
    the demo would be refusing a malformed message rather than an unsigned one."""
    for topic, _key, value, headers in _run_forge():
        evt = ce.from_kafka_binary([(k, v) for k, v in headers], value)
        assert evt.get("specversion") == "1.0"
        assert evt.get("source") == "rossoctl://attacker/forge"
        assert evt.get("groupid") == "test-group-0001"
        assert not evt.get("signature"), "an attacker has no approved key to sign with"


def test_the_forged_events_are_refused_by_the_real_decision_function():
    """The assertion that makes the beat worth demoing: these exact bytes are what
    `response_decision` refuses under enforcement — not a near-miss."""
    ks = keyset.KeySet({"eb-01": S.public_key(SEED_EB),
                        "runner-01": S.public_key(SEED_R1)})
    for _topic, _key, value, headers in _run_forge():
        evt = ce.from_kafka_binary([(k, v) for k, v in headers], value)
        ok, why, _verified = S.response_decision(evt, ks, require=True,
                                                 bridge_kid="eb-01")
        assert not ok, f"the forgery must be refused, got accept with {why!r}"
        assert "signature" in why


def test_the_forged_member_terminal_claims_to_be_terminal():
    """It has to be `final=true` to be refused at all: §4.4 passes an unsigned
    NON-terminal frame through by policy, because signing every stdout frame would add
    minutes to a chatty run. A forgery that forgot this would be accepted and the demo
    would assert the opposite of the control."""
    member = _run_forge()[0]
    evt = ce.from_kafka_binary([(k, v) for k, v in member[3]], member[2])
    assert str(evt.get("final")).lower() == "true"
    assert evt.get("phase") == "error"
    assert "Ship the goods" in str(evt.data)


def test_the_forged_terminal_does_not_reuse_the_genuine_runs_first_sequence():
    """Found in review, before the first cluster run: a forgery at `sequence=1` always
    collides, because `Emitter.next_seq` returns 1 for a genuine run's first event.

    #955's store rule then refuses the rejected row rather than storing it — the
    STRONGER outcome — but it leaves no row carrying `data.signature_rejected`, so the
    "the refusal is on record" assertion finds nothing and the beat fails for a reason
    that has nothing to do with signing. The default must therefore not collide.
    """
    member = _run_forge()[0]
    evt = ce.from_kafka_binary([(k, v) for k, v in member[3]], member[2])
    assert int(evt.get("sequence")) > 1, \
        "the default forgery must not land on a sequence the genuine run holds"


def test_the_collision_case_is_still_reachable_on_purpose():
    """It is the more interesting outcome, so it stays available — just not as the
    default, and with the assertion pointed at the surviving genuine row instead."""
    assert _flow("kind-signed").args.forge_collide is False
    assert _flow("kind-signed", "--forge-collide").args.forge_collide is True
