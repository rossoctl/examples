"""The topic audit: what is signed, and what each result answers. R4, `shared/audit.py`.

`shared.audit` is pure on purpose — it takes decoded CloudEvents and returns verdicts —
so every claim the tool prints is checked here without a broker. That matters more than
usual for this module: it exists to make signature coverage *visible*, so a bug in it
would misreport the very control it was built to display, and it would do so
convincingly.

The verdict names are part of the output a human reads on stage, so they are asserted
literally rather than through a helper.
"""
from __future__ import annotations

import binascii

from shared import audit, ce, keyset
from shared import signing as S

SEED_EB = binascii.unhexlify(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
SEED_R1 = binascii.unhexlify(
    "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb")
SEED_ROGUE = binascii.unhexlify(
    "c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7")


def _ks():
    return keyset.KeySet({"eb-01": S.public_key(SEED_EB),
                          "runner-01": S.public_key(SEED_R1)})


def _wire(evt):
    """Round-trip through the binding, so a test sees the event as the tool will."""
    return ce.from_kafka_binary(*ce.to_kafka_binary(evt))


def _request(corr="brave-otter-4718", *, seed=SEED_EB, kid="eb-01"):
    e = ce.new_event(type=ce.TYPE_REQUEST, source="rossoctl://eventbridge/t",
                     datacontenttype="application/json", correlationid=corr,
                     sessionuuid=ce.session_uuid(corr), mode="start",
                     data={"prompt": "go"})
    S.sign_into(e, seed, kid)
    return _wire(e)


def _response(corr="brave-otter-4718", *, causation="", seq=1, final=True,
              seed=SEED_R1, kid="runner-01", phase=None):
    e = ce.new_event(type=ce.TYPE_RESPONSE, source="rossoctl://eventrunner/t",
                     datacontenttype="application/json", correlationid=corr,
                     sessionuuid=ce.session_uuid(corr), sequence=seq,
                     phase=phase or ("result" if final else "stdout"),
                     final="true" if final else "false", causationid=causation,
                     data={"text": "hi"})
    S.sign_into(e, seed, kid)
    return _wire(e)


def _group_completed(gid="test-group-0001", *, seed=SEED_EB, kid="eb-01"):
    e = ce.new_event(type=ce.TYPE_GROUP_COMPLETED, source="rossoctl://eventbridge/t",
                     datacontenttype="application/json", groupid=gid,
                     data={"reason": "all"})
    S.sign_into(e, seed, kid)
    return _wire(e)


# ---- the five verdicts -------------------------------------------------------

def test_a_signature_from_an_approved_key_reads_signed_ok():
    verdict, _why, kid = audit.classify(_response(), _ks())
    assert verdict == audit.VERDICT_OK
    assert kid == "runner-01", "the kid is reported, so an operator sees WHICH agent"


def test_a_signature_from_an_unapproved_key_reads_signed_bad():
    """The forgery case the demo exists to show. Structurally perfect signature,
    key nobody approved."""
    verdict, why, kid = audit.classify(
        _response(seed=SEED_ROGUE, kid="runner-99"), _ks())
    assert verdict == audit.VERDICT_BAD
    assert kid == "runner-99", "the claimed kid is shown even though it did not verify"
    assert "runner-99" in why


def test_an_unsigned_terminal_reads_unsigned():
    verdict, why, _kid = audit.classify(_response(seed=None, kid=None), _ks())
    assert verdict == audit.VERDICT_UNSIGNED
    assert "terminal" in why


def test_an_unsigned_streamed_frame_reads_unsigned_ok():
    """**The distinction that keeps the tool honest.** `emit()` signs terminals only,
    because a signature costs ~222 ms and signing every stdout frame would add minutes
    to a chatty run. A tool that flagged these would report every genuine run as mostly
    unsigned, and the real findings would be lost in the noise.
    """
    verdict, why, _kid = audit.classify(
        _response(seed=None, kid=None, final=False), _ks())
    assert verdict == audit.VERDICT_EXEMPT
    assert "non-terminal" in why


def test_an_unsigned_group_event_is_not_exempt():
    """A group lifecycle event is never a streamed frame, so the terminal-only policy
    does not cover it: an unsigned `group.completed` ends a batch."""
    verdict, _why, _kid = audit.classify(
        _group_completed(seed=None, kid=None), _ks())
    assert verdict == audit.VERDICT_UNSIGNED


def test_without_a_keyset_nothing_is_verified_and_the_tool_says_so():
    """§8.5.2 — state the default. Listing is not auditing, and a tool that printed
    a reassuring verdict with nothing to check against would be the worst outcome."""
    for evt in (_response(), _response(seed=None, kid=None), _request()):
        verdict, why, _kid = audit.classify(evt, None)
        assert verdict == audit.VERDICT_UNKNOWN
        assert "no keyset" in why


def test_a_group_event_is_pinned_to_the_bridge_kid():
    """Same rule the live consumer applies: an approved runner is still not the bridge,
    so its `group.completed` must read BAD here too."""
    ok, _why, _kid = audit.classify(
        _group_completed(seed=SEED_R1, kid="runner-01"), _ks(), bridge_kid="eb-01")
    assert ok == audit.VERDICT_BAD
    assert audit.classify(_group_completed(), _ks(),
                          bridge_kid="eb-01")[0] == audit.VERDICT_OK


# ---- the chain ---------------------------------------------------------------

_DEFAULT = object()


def _rows(requests, responses, ks=_DEFAULT, bridge_kid=None):
    """`ks=None` means "no keyset configured" and must survive as None.

    Spelled with a sentinel rather than `ks or _ks()`, which silently replaced an
    explicit None with a working keyset — so the no-keyset test passed against a
    verified chain and asserted nothing. Found by that test failing.
    """
    ks = _ks() if ks is _DEFAULT else ks
    rq = [audit.row_for(e, i, ks, bridge_kid=bridge_kid)
          for i, e in enumerate(requests)]
    rs = [audit.row_for(e, i, ks, bridge_kid=bridge_kid)
          for i, e in enumerate(responses)]
    return rq, rs


def test_a_result_chains_back_to_its_signed_wake_event():
    """The abstract's audit claim, and the reason it is worth anything: `causationid` is
    in SIGNED_ATTRS, so the link cannot be rewritten in flight."""
    req = _request()
    rq, rs = _rows([req], [_response(causation=req.get("id"))])
    chain, = audit.chains(rq, rs)
    assert chain.state == "bound"
    assert chain.bound is True
    assert chain.terminal.verdict == audit.VERDICT_OK


def test_a_verified_result_naming_an_unsigned_wake_is_result_only():
    """Both ends or neither. A signed result naming an unsigned request proves who
    finished a run that anyone could have started."""
    req = _request(seed=None, kid=None)
    rq, rs = _rows([req], [_response(causation=req.get("id"))])
    chain, = audit.chains(rq, rs)
    assert chain.state == "result-only"
    assert chain.bound is False


def test_a_result_naming_no_wake_event_is_orphaned():
    """What an injected response looks like: an answer to a question nobody asked.
    Reported rather than dropped, because a drop is invisible."""
    rq, rs = _rows([], [_response(causation="not-on-this-topic")])
    chain, = audit.chains(rq, rs)
    assert chain.state == "orphaned"
    assert len(chain.results) == 1


def test_a_genuine_answer_beside_a_forgery_is_contested_not_bound():
    """`(correlationid, sequence)` means both rows exist, so a reader cannot tell which
    is the answer from the topic alone. Distinguishing this from "no terminal" and from
    "the terminal is unsigned" is the whole reason `state` is not a boolean — the same
    reasoning as recording a timeout as a timeout rather than a refusal.
    """
    req = _request()
    rq, rs = _rows([req], [
        _response(causation=req.get("id"), seq=1),
        _response(causation=req.get("id"), seq=2, seed=SEED_ROGUE, kid="runner-99")])
    chain, = audit.chains(rq, rs)
    assert chain.state == "contested"
    assert chain.bound is False
    assert chain.terminal.verdict == audit.VERDICT_OK, \
        "`terminal` still names the genuine answer, not whichever arrived last"


def test_a_run_with_no_terminal_yet_is_open_not_a_finding():
    """Reading a topic mid-run must not look like tampering."""
    req = _request()
    rq, rs = _rows([req], [_response(causation=req.get("id"), final=False, seed=None,
                                     kid=None)])
    chain, = audit.chains(rq, rs)
    assert chain.state == "open"


def test_an_unverified_terminal_is_distinguished_from_a_missing_one():
    req = _request()
    rq, rs = _rows([req], [_response(causation=req.get("id"), seed=None, kid=None)])
    chain, = audit.chains(rq, rs)
    assert chain.state == "unverified"


def test_with_no_keyset_a_chain_reports_unknown_rather_than_bound():
    req = _request()
    rq, rs = _rows([req], [_response(causation=req.get("id"))], ks=None)
    chain, = audit.chains(rq, rs)
    assert chain.state == "unknown"
    assert chain.bound is False, "nothing was checked, so nothing is proven"


# ---- the summary a slide quotes ---------------------------------------------

def test_the_summary_counts_terminals_separately_from_events():
    """`terminals_verified / terminals` is the number worth a slide: a terminal is the
    event the transcript presents as the answer, so it is the proportion of answers
    whose origin is provable. A bare signed/total ratio would be dominated by the
    streamed frames that are unsigned by policy."""
    req = _request()
    rq, rs = _rows([req], [
        _response(causation=req.get("id"), seq=1, final=False, seed=None, kid=None),
        _response(causation=req.get("id"), seq=2),
        _response(causation=req.get("id"), seq=3, seed=SEED_ROGUE, kid="runner-99")])
    s = audit.summarise(rq + rs)
    assert s["total"] == 4
    assert s["terminals"] == 2, "the streamed frame is not a terminal"
    assert s["terminals_verified"] == 1
    assert s[audit.VERDICT_EXEMPT] == 1
    assert s[audit.VERDICT_BAD] == 1
