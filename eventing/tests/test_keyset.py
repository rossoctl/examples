"""The approved-key set, and the `kid` that selects from it.

Also covers the sign -> Kafka wire -> verify path, which had no test at all: the
existing signing tests sign and verify the same in-memory object, and the codec
tests never touch signing. So nothing checked that the signature travels as a
`ce_signature` header, that the `kid` survives, or that tampering between producer
and consumer is caught.
"""
from __future__ import annotations

import base64
import binascii
import json

import pytest

from shared import ce, keyset
from shared import signing as S

# RFC 8032 test vector 1 — the same seed the signing tests use.
SEED = binascii.unhexlify(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
PUB = S.public_key(SEED)

SEED2 = binascii.unhexlify(
    "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb")
PUB2 = S.public_key(SEED2)


def _write(tmp_path, obj) -> str:
    p = tmp_path / "agents.json"
    p.write_text(json.dumps(obj))
    return str(p)


def _event(**over):
    attrs = {"type": ce.TYPE_RESPONSE, "source": "rossoctl://eventrunner/test",
             "datacontenttype": "application/json",
             "correlationid": "brave-otter-4718",
             "sessionuuid": "8f1b6d2e-0000-5000-8000-000000000000",
             "sequence": 3, "phase": "result", "final": "true"}
    attrs.update(over)
    return ce.new_event(data={"text": "hello"}, **attrs)


# ---- loading ----------------------------------------------------------------

def test_load_accepts_hex_base64_and_base64url(tmp_path):
    ks = keyset.load(_write(tmp_path, {
        "hex": PUB.hex(),
        "b64": base64.b64encode(PUB).decode(),
        "b64u": base64.urlsafe_b64encode(PUB).decode().rstrip("="),
    }))
    assert len(ks) == 3
    assert ks.select("hex") == ks.select("b64") == ks.select("b64u") == PUB


def test_kids_are_sorted_and_loggable(tmp_path):
    ks = keyset.load(_write(tmp_path, {"z": PUB.hex(), "a": PUB2.hex()}))
    assert ks.kids == ("a", "z")


def test_membership(tmp_path):
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    assert "runner-01" in ks and "runner-99" not in ks


@pytest.mark.parametrize("bad", [
    {"k": "not-hex-at-all"},
    {"k": PUB.hex()[:40]},          # too short
    {"k": (PUB + b"\x00").hex()},   # 33 bytes
    {"k": 1234},                    # not a string
    {"": PUB.hex()},                # empty kid
])
def test_load_rejects_a_malformed_entry_rather_than_partially_loading(tmp_path, bad):
    """An authorization list that quietly dropped an entry would fail closed for a
    legitimate agent, which reads as a broken deploy rather than a control."""
    with pytest.raises(ValueError):
        keyset.load(_write(tmp_path, bad))


def test_load_rejects_a_non_object(tmp_path):
    with pytest.raises(ValueError):
        keyset.load(_write(tmp_path, ["not", "an", "object"]))


def test_load_if_set_returns_none_when_unconfigured():
    assert keyset.load_if_set(None) is None
    assert keyset.load_if_set("") is None


# ---- selection --------------------------------------------------------------

def test_select_returns_the_named_key(tmp_path):
    ks = keyset.load(_write(tmp_path, {"a": PUB.hex(), "b": PUB2.hex()}))
    assert ks.select("a") == PUB
    assert ks.select("b") == PUB2


def test_select_unknown_kid_returns_none(tmp_path):
    ks = keyset.load(_write(tmp_path, {"a": PUB.hex()}))
    assert ks.select("nope") is None


def test_select_none_kid_works_for_a_single_key_set(tmp_path):
    """A one-key deployment should not need every signer to name its key."""
    ks = keyset.load(_write(tmp_path, {"only": PUB.hex()}))
    assert ks.select(None) == PUB


def test_select_none_kid_is_ambiguous_with_several_keys(tmp_path):
    """Guessing would mean accepting a signature from ANY approved agent for an
    event that named none of them."""
    ks = keyset.load(_write(tmp_path, {"a": PUB.hex(), "b": PUB2.hex()}))
    assert ks.select(None) is None


# ---- kid in the token -------------------------------------------------------

def test_sign_event_without_kid_keeps_the_original_header():
    """Backward compatible: an unnamed token is byte-identical to before."""
    token = S.sign_event(_event(), SEED)
    header = json.loads(S._b64u_dec(token.split(".")[0]))
    assert header == {"alg": "EdDSA", "typ": "ce+jws"}
    assert S.token_kid(token) is None


def test_sign_event_with_kid_puts_it_in_the_protected_header():
    token = S.sign_event(_event(), SEED, kid="runner-01")
    header = json.loads(S._b64u_dec(token.split(".")[0]))
    assert header == {"alg": "EdDSA", "kid": "runner-01", "typ": "ce+jws"}
    assert S.token_kid(token) == "runner-01"


def test_a_kid_bearing_token_still_verifies():
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    ok, why = S.verify_signature(e, PUB)
    assert ok, why


def test_the_kid_is_covered_by_the_signature():
    """Swapping the kid to point at another approved key must not verify —
    otherwise an attacker could relabel an event as coming from someone else."""
    e = _event()
    token = S.sign_event(e, SEED, kid="runner-01")
    protected, _, sig = token.split(".")
    forged_header = S._b64u(json.dumps(
        {"alg": "EdDSA", "kid": "runner-02", "typ": "ce+jws"},
        separators=(",", ":"), sort_keys=True).encode())
    e.attrs["signature"] = f"{forged_header}..{sig}"
    ok, _ = S.verify_signature(e, PUB)
    assert not ok


@pytest.mark.parametrize("token", ["", "a.b", "not a token", "...", "a..b..c"])
def test_token_kid_tolerates_garbage(token):
    assert S.token_kid(token) is None


def test_token_kid_ignores_a_non_string_kid():
    header = S._b64u(json.dumps({"alg": "EdDSA", "kid": 7}).encode())
    assert S.token_kid(f"{header}..sig") is None


# ---- the end-to-end check: sign -> Kafka wire -> verify ---------------------

def test_signature_survives_the_kafka_binary_roundtrip():
    """The gap this file exists to close.

    Before this, nothing tested signing across the codec: the signing tests sign
    and verify the same in-memory object, and the codec tests never sign. So the
    signature travelling as a `ce_signature` header, the `kid` surviving the trip,
    and tampering between producer and consumer were all unverified.

    Type stability is the reason it works. `ce.new_event` stringifies attributes on
    construction, so `sequence` is already `"3"` when it is signed and is still
    `"3"` after `from_kafka_binary` — nothing is coerced in between. That is worth
    pinning: signing over values that changed representation on the wire would
    fail for every event, and only an end-to-end test catches it.
    """
    e = _event(sequence=3)
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    headers, value = ce.to_kafka_binary(e)
    back = ce.from_kafka_binary(headers, value)

    assert S.token_kid(back.get("signature")) == "runner-01"
    ok, why = S.verify_signature(back, PUB)
    assert ok, why


def test_roundtrip_verification_fails_with_the_wrong_key():
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    back = ce.from_kafka_binary(*ce.to_kafka_binary(e))
    ok, _ = S.verify_signature(back, PUB2)
    assert not ok


def test_tampering_on_the_wire_is_detected():
    """Rewrite an attribute after signing, as anything with topic write access
    could, and the signature must fail."""
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    headers, value = ce.to_kafka_binary(e)
    headers = [(k, b"hostile-otter-0001" if k == "ce_correlationid" else v)
               for k, v in headers]
    back = ce.from_kafka_binary(headers, value)
    ok, _ = S.verify_signature(back, PUB)
    assert not ok


def test_payload_tampering_on_the_wire_is_detected():
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    headers, _ = ce.to_kafka_binary(e)
    back = ce.from_kafka_binary(
        headers, json.dumps({"text": "Transfer approved."}).encode())
    ok, _ = S.verify_signature(back, PUB)
    assert not ok


def _tampered_on_the_wire(attr, value, **signed_attrs):
    """Sign an event, then rewrite one `ce_` header in flight. Returns (ok, why)."""
    e = _event(**signed_attrs)
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    headers, body = ce.to_kafka_binary(e)
    headers = [(k, value if k == f"ce_{attr}" else v) for k, v in headers]
    return S.verify_signature(ce.from_kafka_binary(headers, body), PUB)


def test_the_submitter_is_covered_by_the_signature():
    """What makes the claim in eventbridge/auth.py true rather than aspirational.

    `submitter` records who EventBridge believes submitted a request. Until it was in
    SIGNED_ATTRS, anything with topic write access could rewrite that name — so the
    audit trail named whoever the last writer chose.
    """
    ok, why = _tampered_on_the_wire("submitter", b"attacker",
                                    submitter="mrsabath", submitteriss="github")
    assert not ok and "does not verify" in why


def test_the_submitter_issuer_is_covered_by_the_signature():
    """Without this, a forged `submitteriss=github` upgrades a name an operator typed
    into an env var to a login GitHub appears to have verified."""
    ok, _ = _tampered_on_the_wire("submitteriss", b"github",
                                  submitter="mrsabath", submitteriss="static")
    assert not ok


def test_the_groupid_is_covered_by_the_signature():
    """DESIGN_PHASE1.md §21.9.9 requires it: a forged `groupid` moves a response into
    another batch and corrupts that batch's fan-in counts."""
    ok, _ = _tampered_on_the_wire("groupid", b"g-victim", groupid="g-mine")
    assert not ok


def test_adding_a_groupid_after_signing_is_detected():
    """The absence of an attribute is signed too, not just its value — otherwise an
    ungrouped response could be adopted into a batch it was never part of."""
    e = _event()                       # no groupid
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    headers, body = ce.to_kafka_binary(e)
    headers.append(("ce_groupid", b"g-victim"))
    ok, _ = S.verify_signature(ce.from_kafka_binary(headers, body), PUB)
    assert not ok


def test_text_plain_payload_roundtrips():
    e = _event(datacontenttype="text/plain")
    e.data = "hello"
    e.attrs["signature"] = S.sign_event(e, SEED)
    back = ce.from_kafka_binary(*ce.to_kafka_binary(e))
    ok, why = S.verify_signature(back, PUB)
    assert ok, why


# ---- the two together: kid selects, then verification decides ---------------

def test_approved_agent_verifies_and_unapproved_does_not(tmp_path):
    """The intended mechanism, composed by hand: the keyset as authorization list.

    Kept hand-wired even though `signing.verify_with_keyset` now does exactly this in
    production, because a test that reaches through the primitives one at a time says
    where a failure is — `select` returned nothing, or the signature did not verify —
    which a single boolean from the composed function cannot.
    """
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))

    approved = _event()
    approved.attrs["signature"] = S.sign_event(approved, SEED, kid="runner-01")
    back = ce.from_kafka_binary(*ce.to_kafka_binary(approved))
    pub = ks.select(S.token_kid(back.get("signature")))
    assert pub is not None
    assert S.verify_signature(back, pub)[0]

    # Same event, signed by a key nobody approved, naming a kid not in the set.
    rogue = _event()
    rogue.attrs["signature"] = S.sign_event(rogue, SEED2, kid="runner-99")
    back2 = ce.from_kafka_binary(*ce.to_kafka_binary(rogue))
    assert ks.select(S.token_kid(back2.get("signature"))) is None


def test_a_rogue_key_claiming_an_approved_kid_is_refused(tmp_path):
    """The nastier case: the attacker knows an approved kid but not its key."""
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED2, kid="runner-01")
    back = ce.from_kafka_binary(*ce.to_kafka_binary(e))
    pub = ks.select(S.token_kid(back.get("signature")))
    assert pub == PUB                      # kid resolves...
    assert not S.verify_signature(back, pub)[0]   # ...but the signature does not


# ---- sign_into: the one place that decides "signing is enabled" --------------

def test_sign_into_is_a_no_op_without_a_seed():
    """The default path, asserted rather than assumed: no seed, no attribute."""
    e = _event()
    assert S.sign_into(e, None) is False
    assert "signature" not in e.attrs


def test_sign_into_signs_and_the_result_verifies():
    e = _event()
    assert S.sign_into(e, SEED, "runner-01") is True
    assert S.verify_signature(e, PUB) == (True, "ok")
    assert S.token_kid(e.attrs["signature"]) == "runner-01"


def test_sign_into_degrades_rather_than_raising_on_a_bad_seed():
    """A key-config mistake must not take out publishing entirely.

    Unsigned is safe because the verifying side refuses unsigned events when
    enforcement is on; a raise here would reject 100% of traffic at the producer.
    """
    e = _event()
    assert S.sign_into(e, b"\x01" * 31) is False   # 31 bytes is not an Ed25519 seed
    assert "signature" not in e.attrs


# ---- verify_with_keyset: the composed authorization check --------------------

def test_verify_with_keyset_accepts_an_approved_signer(tmp_path):
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    e = _event()
    S.sign_into(e, SEED, "runner-01")
    back = ce.from_kafka_binary(*ce.to_kafka_binary(e))
    ok, why = S.verify_with_keyset(back, ks)
    assert ok, why


def test_verify_with_keyset_reports_a_missing_signature(tmp_path):
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    ok, why = S.verify_with_keyset(_event(), ks)
    assert not ok and "no ce_signature" in why


def test_verify_with_keyset_reports_an_unapproved_kid(tmp_path):
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    e = _event()
    S.sign_into(e, SEED2, "runner-99")
    ok, why = S.verify_with_keyset(e, ks)
    assert not ok and "'runner-99' is not in the approved key set" in why


def test_verify_with_keyset_reports_an_ambiguous_unnamed_token(tmp_path):
    """Two approved keys and a token naming neither: guessing would accept a
    signature from *any* approved agent for an event that claimed none of them."""
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex(),
                                       "runner-02": PUB2.hex()}))
    e = _event()
    S.sign_into(e, SEED)                      # no kid
    ok, why = S.verify_with_keyset(e, ks)
    assert not ok and "ambiguous" in why


def test_verify_with_keyset_accepts_an_unnamed_token_against_a_single_key(tmp_path):
    """The friendly path: one approved key means nothing has to name it."""
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    e = _event()
    S.sign_into(e, SEED)                      # no kid
    ok, why = S.verify_with_keyset(e, ks)
    assert ok, why


def test_expect_kid_pins_the_signer_to_one_identity(tmp_path):
    """What stops an approved runner forging an EventBridge group event."""
    ks = keyset.load(_write(tmp_path, {"eventbridge": PUB.hex(),
                                       "runner-01": PUB2.hex()}))
    e = _event(type=ce.TYPE_GROUP_STARTED)
    S.sign_into(e, SEED2, "runner-01")        # a genuinely approved key...
    ok, why = S.verify_with_keyset(e, ks, expect_kid="eventbridge")
    assert not ok and "expected a signature from kid 'eventbridge'" in why

    bridge = _event(type=ce.TYPE_GROUP_STARTED)
    S.sign_into(bridge, SEED, "eventbridge")
    assert S.verify_with_keyset(bridge, ks, expect_kid="eventbridge")[0]


# ---- response_decision: enabled? verified? enforced? -------------------------

def test_response_decision_accepts_everything_when_no_keyset_is_configured():
    """Today's behaviour, which must survive untouched as the default."""
    ok, why, verified = S.response_decision(_event(), None, require=True)
    assert ok and "not enabled" in why


def test_response_decision_accepts_a_verified_response(tmp_path):
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    e = _event()
    S.sign_into(e, SEED, "runner-01")
    assert S.response_decision(e, ks, require=True) == (True, "ok", True)


def test_response_decision_rejects_an_unsigned_response_when_enforcing(tmp_path):
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    ok, why, verified = S.response_decision(_event(), ks, require=True)
    assert not ok and "no ce_signature" in why


def test_response_decision_reports_but_accepts_in_audit_mode(tmp_path):
    """The two-flag rollout in one assertion: a keyset alone verifies and explains,
    without yet rewriting anything a user sees."""
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    ok, why, verified = S.response_decision(_event(), ks, require=False)
    assert ok, "audit mode must not reject"
    assert "no ce_signature" in why, "but it must still say what was wrong"
    assert not verified, \
        "and it must be distinguishable from a clean pass, which is what makes the " \
        "reject rate countable without parsing the reason prose"


def test_response_decision_requires_the_bridge_kid_on_group_events(tmp_path):
    ks = keyset.load(_write(tmp_path, {"eventbridge": PUB.hex(),
                                       "runner-01": PUB2.hex()}))
    forged = _event(type=ce.TYPE_GROUP_COMPLETED)
    S.sign_into(forged, SEED2, "runner-01")
    ok, why, verified = S.response_decision(forged, ks, require=True, bridge_kid="eventbridge")
    assert not ok and "expected a signature from kid 'eventbridge'" in why

    # A plain response from that same runner is still fine — the pin is per-class.
    resp = _event()
    S.sign_into(resp, SEED2, "runner-01")
    assert S.response_decision(resp, ks, require=True, bridge_kid="eventbridge")[0]


def test_an_unsigned_non_terminal_frame_is_accepted(tmp_path):
    """Matching the signing policy, not a loophole.

    `emit()` signs terminal events only, because it runs per stdout frame at
    ~150-200 ms a signature. Verifying all-or-nothing would rewrite every streamed
    frame of every genuine run to phase=error — which is what happened the first time
    this was run against a real broker.
    """
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    frame = _event(sequence=1, phase="stdout", final="false")
    ok, why, verified = S.response_decision(frame, ks, require=True)
    assert ok, why
    assert "non-terminal" in why
    assert verified, \
        "accepted by policy, not in spite of a failure — otherwise an audit-mode " \
        "counter would log every streamed frame of every genuine run"


def test_an_unsigned_terminal_response_is_still_rejected(tmp_path):
    """The line the exemption must not cross: the terminal event is the one the
    transcript presents as the answer, so refusing it unsigned is the whole control."""
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    ok, why, verified = S.response_decision(_event(phase="result", final="true"), ks, require=True)
    assert not ok and "no ce_signature" in why


def test_a_non_terminal_frame_that_claims_a_signature_is_still_verified(tmp_path):
    """The exemption is for events carrying NO signature. One that presents a bad
    signature is lying about something, and gets checked."""
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    frame = _event(sequence=1, phase="stdout", final="false")
    S.sign_into(frame, SEED2, "runner-01")          # approved kid, wrong key
    ok, why, verified = S.response_decision(frame, ks, require=True)
    assert not ok and "does not verify" in why


def test_an_unsigned_group_event_is_rejected_despite_having_no_final_attribute(tmp_path):
    """Group events carry no `final` at all, so testing that alone would classify one
    as an unsigned intermediate frame and wave it through — which is precisely the
    forged `group.completed` the bridge kid exists to catch."""
    ks = keyset.load(_write(tmp_path, {"eventbridge": PUB.hex()}))
    grp = _event(type=ce.TYPE_GROUP_COMPLETED, groupid="g-1")
    grp.attrs.pop("final", None)
    assert "final" not in grp.attrs
    ok, why, verified = S.response_decision(grp, ks, require=True, bridge_kid="eventbridge")
    assert not ok and "no ce_signature" in why


def test_response_decision_does_not_mutate_the_event(tmp_path):
    """What "pure" buys: the caller owns the rewrite, so this is safe to call on the
    hot path of a consumer loop without copying first."""
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    e = _event()
    before_attrs, before_data = dict(e.attrs), dict(e.data)
    S.response_decision(e, ks, require=True)
    assert e.attrs == before_attrs and e.data == before_data
