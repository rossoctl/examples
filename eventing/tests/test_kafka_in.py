"""EventBridge's responses consumer: §11 verification, and the thread that must not die.

This module had no behavioural test at all before signing was wired — it was only
grepped as source text by `test_groups.py`. That mattered, because `kafka_in.run()`
decodes and writes to SQLite *outside* any `try`: anything that raises between the poll
and the store write ends the `for`, ends the `while`, and the consumer thread is gone
while the process stays up and the pod still reports healthy. A consumer that silently
stopped consuming is the worst failure this system has, so the verification path added
here has to degrade rather than raise — and that property needs a test, not a comment.

Most of what matters is decided by `signing.response_decision`, which is pure; those
tests live in `test_keyset.py`. What is left here is the part only the loop can show:
that a rejected event is stored rather than dropped, and that the loop survives.
"""
from __future__ import annotations

import binascii
import json
import pathlib

from eventbridge import kafka_in
from eventbridge.store import Store
from shared import ce, keyset
from shared import signing as S

# RFC 8032 vectors: the bridge's own key, an approved runner, and a rogue.
SEED_EB = binascii.unhexlify(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
SEED_R1 = binascii.unhexlify(
    "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb")
SEED_ROGUE = binascii.unhexlify(
    "c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7")


def _keyset(tmp_path):
    p = tmp_path / "agents.json"
    p.write_text(json.dumps({"eb-01": S.public_key(SEED_EB).hex(),
                             "runner-01": S.public_key(SEED_R1).hex()}))
    return keyset.load(str(p))


class _Rec:
    def __init__(self, headers, value):
        self.headers, self.value = headers, value


def _response(corr="brave-otter-4718", *, seed=None, kid=None, seq=1,
              phase="result", text="the real answer", **over):
    attrs = dict(type=ce.TYPE_RESPONSE, source="rossoctl://eventrunner/test",
                 datacontenttype="application/json", correlationid=corr,
                 sessionuuid=ce.session_uuid(corr), sequence=seq, phase=phase,
                 final="true")
    attrs.update(over)
    evt = ce.new_event(data={"text": text}, **attrs)
    S.sign_into(evt, seed, kid)
    return _Rec(*ce.to_kafka_binary(evt))


def _group_event(gid="g-123", *, seed=None, kid=None,
                 type_=ce.TYPE_GROUP_COMPLETED):
    evt = ce.new_event(type=type_, source="rossoctl://eventbridge/test",
                       datacontenttype="application/json", groupid=gid,
                       data={"reason": "all done"})
    S.sign_into(evt, seed, kid)
    return _Rec(*ce.to_kafka_binary(evt))


class _FakeKafka:
    """Iterable KafkaConsumer stand-in: `kafka_in.run()` does `for rec in c`.

    Stopping is driven from inside iteration and has to happen at exactly the right
    moment. `run()` checks `stopping` before the `while`, so stopping beforehand skips
    the loop entirely; it also checks at the top of each record, so stopping *before*
    yielding the last one makes `run()` break without processing it. The flag is
    therefore set when iteration is resumed after the final record — by then its body
    has already run, and the outer `while` exits instead of spinning on an exhausted
    iterator forever.
    """

    def __init__(self, records, consumer):
        self._records = list(records)
        self._consumer = consumer
        self.closed = False

    def __iter__(self):
        while self._records:
            yield self._records.pop(0)
        # Every record has been processed by now: end the outer while loop.
        self._consumer.stop()

    def close(self):
        self.closed = True


class _FakeProducer:
    """Records what the group service would publish. Deliberately a local copy of
    `test_groups.py`'s: importing across test modules couples two files that otherwise
    share nothing, and this one only needs the group-event half."""

    def __init__(self):
        self.requests = []
        self.group_events = []

    def publish_request(self, **kw):
        self.requests.append(kw)
        return f"evt-{len(self.requests)}"

    def publish_group_event(self, *, type_, groupid, data, subject="group",  # noqa: ARG002
                            userkey=None):
        self.group_events.append({"type": type_, "groupid": groupid, "data": data,
                                  "userkey": userkey})
        return f"gevt-{len(self.group_events)}"


class _SeqMinter:
    r"""Deterministic ids satisfying correlation.REGEX (^[a-z]{3,10}-[a-z]{3,12}-\d{4}$)."""

    def __init__(self):
        self.n = 0

    def mint(self):
        self.n += 1
        return f"test-agent-{self.n:04d}"

    def remember(self, corr):
        pass


def _group_service(tmp_path):
    """A real GroupService over a real Store, so a test can assert the OUTCOME of a
    verdict rather than the verdict. The store is shared with the consumer so both
    write to one database, which is how they are wired in `__main__.py`."""
    from eventbridge.config import Cfg as EbCfg
    from eventbridge.group_service import GroupService
    store = Store(pathlib.Path(tmp_path) / "eb")
    cfg = EbCfg(tmpdir=str(tmp_path), public_base_url="http://eb.test")
    producer = _FakeProducer()
    return GroupService(cfg, store, producer, _SeqMinter()), store, producer


def _drain(tmp_path, records, monkeypatch, *, store=None, **kw):
    """Run the consumer over `records` to exhaustion, synchronously.

    `run()` is called directly rather than through `start()`: the record list is
    finite, so there is nothing to wait for and a real thread would only add a race.
    """
    store = store or Store(pathlib.Path(tmp_path) / "eb")
    seen: list[dict] = []
    c = kafka_in.Consumer("broker:9092", "responses", store,
                          on_event=seen.append, **kw)
    fake = _FakeKafka(records, c)
    monkeypatch.setattr(kafka_in, "KafkaConsumer", lambda *a, **k: fake)
    c.run()
    return c, store, seen, fake


# ---- the default path: nothing configured, nothing changes -------------------

def test_an_unsigned_response_is_stored_normally_when_verification_is_off(
        tmp_path, monkeypatch):
    """Today's behaviour, which must be exactly preserved as the default."""
    c, store, seen, _ = _drain(tmp_path, [_response()], monkeypatch)
    rows = store.events_for("brave-otter-4718")
    assert len(rows) == 1
    assert rows[0]["phase"] == "result", "an unverified event must not be rewritten"
    assert rows[0]["data"]["text"] == "the real answer"
    assert c.rejected == 0


# ---- verification on ---------------------------------------------------------

def test_a_response_from_an_approved_agent_is_stored_unchanged(tmp_path, monkeypatch):
    c, store, _, _ = _drain(
        tmp_path, [_response(seed=SEED_R1, kid="runner-01")], monkeypatch,
        keyset=_keyset(tmp_path), require_signature=True, bridge_kid="eb-01")
    rows = store.events_for("brave-otter-4718")
    assert rows[0]["phase"] == "result" and c.rejected == 0
    assert rows[0]["data"]["text"] == "the real answer"


def test_a_forged_response_is_stored_as_an_error_not_dropped(tmp_path, monkeypatch):
    """The demo, and the reason rejection is not a silent drop.

    A dropped event is indistinguishable from an agent that never answered. Stored as
    `phase=error` it becomes a red card in the transcript, a priority-5 notification,
    and a retained `raw_json` row — the forgery attempt is evidence rather than absence.
    """
    forged = _response(seed=None, text="Transfer approved. Ship the goods.")
    c, store, seen, _ = _drain(tmp_path, [forged], monkeypatch,
                               keyset=_keyset(tmp_path), require_signature=True,
                               bridge_kid="eb-01")
    rows = store.events_for("brave-otter-4718")
    assert len(rows) == 1, "the event must still be persisted"
    assert rows[0]["phase"] == "error", "and rewritten so it renders as a rejection"
    assert rows[0]["data"]["signature_rejected"] is True
    # ntfy reads data["text"] for the error body; any other key shows up on the
    # phone as "(error, see raw)".
    assert "unverified response rejected" in rows[0]["data"]["text"]
    assert "no ce_signature" in rows[0]["data"]["reason"]
    assert c.rejected == 1
    # The forged text must not be presented AS the answer...
    assert "Ship the goods" not in rows[0]["data"]["text"]
    assert seen and seen[0]["phase"] == "error", "downstream sees the rejection too"


def test_a_rejected_event_is_retained_for_audit(tmp_path, monkeypatch):
    """...but it must still be retained, which is the stated reason for storing a
    rejection rather than dropping it.

    `insert_response` derives BOTH `data_json` and `raw_json` from one dict, so
    mutating the envelope in place destroyed the forensic record while the module
    docstring still promised it — leaving a signature whose covered attributes no
    longer existed and nothing for an operator to review. Raised by @aslom on #879.
    """
    forged = _response(seed=None, seq=4, phase="result",
                       text="Transfer approved. Ship the goods.")
    c, store, _, _ = _drain(tmp_path, [forged], monkeypatch,
                            keyset=_keyset(tmp_path), require_signature=True,
                            bridge_kid="eb-01")
    raw = store.raw_events_for("brave-otter-4718")[0]
    kept = raw["data"]["rejected"]
    assert kept["data"]["text"] == "Transfer approved. Ship the goods.", \
        "the payload that was refused must be reviewable"
    assert kept["phase"] == "result", "including the phase it claimed to be"
    assert kept["attrs"]["sequence"] == "4"
    assert kept["source"] == "rossoctl://eventrunner/test"
    assert raw["phase"] == "error", "while the stored phase still drives the red card"
    assert c.rejected == 1


def test_the_retained_signature_can_still_be_re_checked(tmp_path, monkeypatch):
    """The sharper half of the same point: a retained signature is only worth keeping
    if the attributes it covered are kept with it. Here a validly-signed event is
    rejected for naming an unapproved kid, and the record is complete enough to verify
    offline — which is what makes an incident reviewable rather than just logged."""
    rec = _response(seq=5, seed=SEED_ROGUE, kid="runner-99")
    c, store, _, _ = _drain(tmp_path, [rec], monkeypatch, keyset=_keyset(tmp_path),
                            require_signature=True, bridge_kid="eb-01")
    kept = store.raw_events_for("brave-otter-4718")[0]["data"]["rejected"]
    assert c.rejected == 1
    # Rebuild the event exactly as it arrived and verify it against the rogue key.
    replayed = ce.CloudEvent(attrs=dict(kept["attrs"]), data=kept["data"])
    ok, why = S.verify_signature(replayed, S.public_key(SEED_ROGUE))
    assert ok, f"the retained record must still verify against the key that signed it: {why}"
    assert S.token_kid(kept["signature"]) == "runner-99", \
        "so an operator can see which key id the forgery claimed"


def test_a_response_signed_by_an_unapproved_key_is_rejected(tmp_path, monkeypatch):
    """A structurally perfect signature from a key nobody approved."""
    c, store, _, _ = _drain(
        tmp_path, [_response(seed=SEED_ROGUE, kid="runner-99")], monkeypatch,
        keyset=_keyset(tmp_path), require_signature=True, bridge_kid="eb-01")
    assert store.events_for("brave-otter-4718")[0]["phase"] == "error"
    assert c.rejected == 1


def test_a_tampered_payload_is_rejected(tmp_path, monkeypatch):
    """Signed by an approved agent, then the body was changed in flight."""
    rec = _response(seed=SEED_R1, kid="runner-01")
    rec.value = json.dumps({"text": "Transfer approved. Ship the goods."}).encode()
    c, store, _, _ = _drain(tmp_path, [rec], monkeypatch, keyset=_keyset(tmp_path),
                            require_signature=True, bridge_kid="eb-01")
    assert store.events_for("brave-otter-4718")[0]["phase"] == "error"
    assert c.rejected == 1


def test_audit_mode_reports_without_rewriting(tmp_path, monkeypatch):
    """A keyset alone verifies and logs; only enforcement rewrites what users see.

    That ordering exists so an operator can watch the reject rate on real traffic
    before it starts painting bubbles red and paging a phone.
    """
    c, store, _, _ = _drain(tmp_path, [_response(seed=None)], monkeypatch,
                            keyset=_keyset(tmp_path), require_signature=False,
                            bridge_kid="eb-01")
    rows = store.events_for("brave-otter-4718")
    assert rows[0]["phase"] == "result", "audit mode must not rewrite"
    assert rows[0]["data"]["text"] == "the real answer"
    assert c.rejected == 0


def test_a_real_runs_streamed_frames_survive_enforcement(tmp_path, monkeypatch):
    """What a genuine run actually looks like on the topic, under enforcement.

    `emit()` signs terminal events only, so a run publishes N unsigned `stdout`
    frames and one signed terminal. Verifying all-or-nothing turned every frame of
    every real run red — caught by running this against a live broker, not by a unit
    test, which is why this one exists.
    """
    recs = [
        _response(seq=1, phase="stdout", text="thinking...", final="false"),
        _response(seq=2, phase="stdout", text="still working", final="false"),
        _response(seq=3, phase="result", text="2+2 is 4",
                  seed=SEED_R1, kid="runner-01"),
    ]
    c, store, _, _ = _drain(tmp_path, recs, monkeypatch, keyset=_keyset(tmp_path),
                            require_signature=True, bridge_kid="eb-01")
    rows = store.events_for("brave-otter-4718")
    assert [r["phase"] for r in rows] == ["stdout", "stdout", "result"], \
        "a genuine run must render unchanged"
    assert c.rejected == 0
    assert rows[-1]["data"]["text"] == "2+2 is 4"


def test_a_forged_terminal_is_rejected_among_genuine_frames(tmp_path, monkeypatch):
    """The demo in its realistic shape: the forgery arrives alongside real streaming
    output, and only it is rewritten."""
    recs = [
        _response(seq=1, phase="stdout", text="thinking...", final="false"),
        _response(seq=2, phase="result", text="2+2 is 4",
                  seed=SEED_R1, kid="runner-01"),
        _response(seq=3, phase="result", text="Transfer approved. Ship the goods."),
    ]
    c, store, _, _ = _drain(tmp_path, recs, monkeypatch, keyset=_keyset(tmp_path),
                            require_signature=True, bridge_kid="eb-01")
    rows = store.events_for("brave-otter-4718")
    assert [r["phase"] for r in rows] == ["stdout", "result", "error"]
    assert c.rejected == 1
    assert "Ship the goods" not in rows[2]["data"]["text"], \
        "the forgery must not be presented as the answer"
    assert rows[1]["data"]["text"] == "2+2 is 4", "the genuine answer is untouched"


# ---- group lifecycle events --------------------------------------------------

def test_a_group_event_signed_by_the_bridge_is_accepted(tmp_path, monkeypatch):
    """Group events carry `groupid` and no `correlationid`, so they take the routing
    branch that never reaches insert_response — they must still verify."""
    groups: list[dict] = []
    c, _, _, _ = _drain(tmp_path, [_group_event(seed=SEED_EB, kid="eb-01")],
                        monkeypatch, keyset=_keyset(tmp_path),
                        require_signature=True, bridge_kid="eb-01",
                        on_group_event=groups.append)
    assert c.rejected == 0
    assert groups and groups[0]["groupid"] == "g-123"
    assert groups[0].get("phase") != "error"


def test_an_approved_runner_cannot_forge_a_group_event(tmp_path, monkeypatch):
    """The hole a flat keyset would leave: a forged `group.completed` ends a batch
    early and fires a "finished" notification for work that never ran. runner-01 is
    genuinely approved — it is just not the bridge.

    **This test used to stop at the check.** It asserted that the dict handed to
    `on_group_event` carried `phase="error"`, with the message "rather than settling the
    group" — but it wired a list's `append` as the callback, so nothing in it could tell
    whether the group settled. The verdict was correct and ignored, which is #885, and
    asserting a verdict while claiming an outcome is DESIGN_PHASE2 §8.5.1 exactly: a
    claim about an outcome has to follow the path to that outcome. It now drives a real
    `GroupService` and asserts the batch.
    """
    svc, store, producer = _group_service(tmp_path)
    gid, _ = svc.create(label="real-work", expected=2)
    svc.submit_members(gid, ["a", "b"])
    c, _, _, _ = _drain(tmp_path, [_group_event(gid=gid, seed=SEED_R1, kid="runner-01")],
                        monkeypatch, store=store, keyset=_keyset(tmp_path),
                        require_signature=True, bridge_kid="eb-01",
                        on_group_event=svc.on_group_event)
    assert c.rejected == 1
    assert store.get_group(gid)["completed_utc"] is None, \
        "a forged completion must not settle a batch whose agents never ran"
    assert [e for e in producer.group_events
            if e["type"] == ce.TYPE_GROUP_COMPLETED] == [], \
        "and must not publish a completion, which is what pages the operator's phone"


# ---- the property this file exists for --------------------------------------

def test_the_consumer_survives_a_verifier_that_raises(tmp_path, monkeypatch):
    """The most important test here.

    `from_kafka_binary` and `insert_response` are not inside a try, so an exception
    escaping the verification path would end the consume loop for the life of the pod —
    silently, with the process still healthy. Both records must be processed.
    """
    def boom(*a, **k):
        raise RuntimeError("keyset backend exploded")
    monkeypatch.setattr(S, "response_decision", boom)

    recs = [_response(corr="brave-otter-4718", seed=SEED_R1, kid="runner-01"),
            _response(corr="calm-badger-1234", seed=SEED_R1, kid="runner-01")]
    c, store, seen, fake = _drain(tmp_path, recs, monkeypatch,
                                  keyset=_keyset(tmp_path), require_signature=True,
                                  bridge_kid="eb-01")
    assert len(seen) == 2, "the loop must process both records, not die on the first"
    assert c.rejected == 2, "and fail closed while enforcement is on"
    for corr in ("brave-otter-4718", "calm-badger-1234"):
        assert store.events_for(corr)[0]["phase"] == "error"
    assert fake.closed, "the consumer is still closed cleanly on the way out"


def test_a_verifier_that_raises_fails_open_when_not_enforcing(tmp_path, monkeypatch):
    """Audit mode must not start rejecting because the verifier broke: nothing is
    being enforced, so a broken check has no opinion to act on."""
    def boom(*a, **k):
        raise RuntimeError("keyset backend exploded")
    monkeypatch.setattr(S, "response_decision", boom)

    c, store, seen, _ = _drain(tmp_path, [_response(seed=SEED_R1, kid="runner-01")],
                               monkeypatch, keyset=_keyset(tmp_path),
                               require_signature=False, bridge_kid="eb-01")
    assert len(seen) == 1
    assert c.rejected == 0
    assert store.events_for("brave-otter-4718")[0]["phase"] == "result"
    assert c.audit_failed == 1, \
        "but it is still an unverified acceptance, and audit mode exists to count those"


# ---- audit mode: the reject rate the rollout asks you to watch ---------------

def test_audit_mode_counts_and_logs_what_enforcement_would_reject(
        tmp_path, monkeypatch, capsys):
    """§4.4 promises "a keyset alone verifies and logs while storing events unchanged",
    and that is the step where an operator watches the reject rate before enforcing.

    The verdict was computed and the reason discarded, so nothing surfaced it: no log
    line, no counter, and therefore no reject rate to watch. The rollout the design
    documents could not be performed.
    """
    c, store, seen, _ = _drain(tmp_path, [_response()], monkeypatch,
                               keyset=_keyset(tmp_path), require_signature=False)
    assert c.audit_failed == 1, "the unsigned terminal response must be counted"
    assert c.rejected == 0, "but audit mode rejects nothing"

    # Stored unchanged is the other half of the promise: audit mode observes without
    # rewriting the row a user reads.
    rows = store.events_for("brave-otter-4718")
    assert rows[0]["phase"] == "result"
    assert "signature_rejected" not in (rows[0].get("data") or {})
    assert len(seen) == 1

    out = capsys.readouterr().out
    assert "audit: would reject response on brave-otter-4718" in out
    assert "no ce_signature" in out, "the reason is what makes the line actionable"


def test_audit_mode_stays_quiet_for_responses_that_verify(tmp_path, monkeypatch):
    """A counter that also counts successes is not a reject rate."""
    c, _, _, _ = _drain(tmp_path, [_response(seed=SEED_R1, kid="runner-01")],
                        monkeypatch, keyset=_keyset(tmp_path),
                        require_signature=False)
    assert c.audit_failed == 0 and c.rejected == 0


def test_audit_mode_does_not_count_unsigned_non_terminal_frames(tmp_path, monkeypatch):
    """`emit()` signs terminal events only, so every streamed frame of every genuine
    run is unsigned on purpose. Counting those would bury the signal this exists for."""
    c, _, _, _ = _drain(tmp_path,
                        [_response(seq=1, phase="stdout", final="false", text="thinking")],
                        monkeypatch, keyset=_keyset(tmp_path),
                        require_signature=False)
    assert c.audit_failed == 0, "accepted by the signing policy, not a failed check"


def test_the_audit_counter_stays_zero_once_enforcement_is_on(tmp_path, monkeypatch):
    """The two counters partition the population rather than double-counting it: with
    enforcement on, a forgery lands in `rejected`, which is the observable that already
    existed."""
    c, _, _, _ = _drain(tmp_path, [_response()], monkeypatch,
                        keyset=_keyset(tmp_path), require_signature=True)
    assert c.rejected == 1 and c.audit_failed == 0


def test_no_audit_logging_when_verification_is_not_configured(
        tmp_path, monkeypatch, capsys):
    """The default deployment has no keyset. Nothing was checked, so there is nothing
    to report, and an unconfigured bridge must not log a line per event."""
    c, _, _, _ = _drain(tmp_path, [_response()], monkeypatch)
    assert c.audit_failed == 0
    assert "audit:" not in capsys.readouterr().out


def test_an_undecodable_record_still_ends_the_loop_as_before(tmp_path, monkeypatch):
    """Not a regression this change introduces, but worth pinning what it does NOT fix:
    the decode at the top of the loop is still outside any try. Verification was made
    safe; the pre-existing decode hazard is unchanged and out of scope here."""
    src = pathlib.Path(kafka_in.__file__).read_text()
    assert "evt = ce.from_kafka_binary(rec.headers or [], rec.value)" in src
    assert "enable_auto_commit=True" in src, (
        "the live consumer must keep committing — test_groups.py pins this too")


# ---- a rejected event must not act (#885) -----------------------------------
#
# The rewrite marks a refused event; these pin that it is also inert. The distinction
# matters because the rewrite deliberately preserves `final` and `groupid` so the
# forensic record stays readable — which is exactly what let a rejected event go on
# finishing members and settling batches.

def test_a_rejected_member_response_does_not_finish_its_member(tmp_path, monkeypatch):
    """#885 item 1 at the consumer boundary: the verdict must stop the callback."""
    calls: list[dict] = []
    c, store, _, _ = _drain(
        tmp_path, [_response(groupid="g-1", seq=1)], monkeypatch,
        keyset=_keyset(tmp_path), require_signature=True, bridge_kid="eb-01",
        on_member_event=calls.append)
    assert c.rejected == 1
    assert calls == [], "a rejected event must not reach on_member_event"
    rows = store.events_for("brave-otter-4718")
    assert rows and rows[0]["phase"] == "error", \
        "but it is still stored, because a drop looks like an agent that never answered"
    assert rows[0]["data"]["rejected"]["data"] == {"text": "the real answer"}, \
        "with the refused payload kept for review"


def test_a_rejected_group_event_does_not_reach_the_group_service(tmp_path, monkeypatch):
    """#885 item 2 at the consumer boundary."""
    groups: list[dict] = []
    c, _, _, _ = _drain(tmp_path, [_group_event(seed=SEED_R1, kid="runner-01")],
                        monkeypatch, keyset=_keyset(tmp_path),
                        require_signature=True, bridge_kid="eb-01",
                        on_group_event=groups.append)
    assert c.rejected == 1
    assert groups == [], "a rejected group event must not reach on_group_event"


def test_a_rejected_frame_cannot_overwrite_a_verified_answer(tmp_path, monkeypatch):
    """#885 item 3. `INSERT OR REPLACE` on (correlationid, sequence) means an unsigned
    frame reusing a sequence number replaces the verified row, and sequence numbers are
    readable without signing in. Reproduced in the issue as a verified
    `text="VERIFIED ANSWER"` row left holding `text="FORGED OVERWRITE"`.
    """
    c, store, _, _ = _drain(
        tmp_path,
        [_response(seq=3, text="2+2 is 4", seed=SEED_R1, kid="runner-01"),
         _response(seq=3, text="Transfer approved. Ship the goods.")],
        monkeypatch, keyset=_keyset(tmp_path), require_signature=True,
        bridge_kid="eb-01")
    rows = store.events_for("brave-otter-4718")
    assert len(rows) == 1 and rows[0]["sequence"] == 3
    assert rows[0]["data"]["text"] == "2+2 is 4", \
        "the verified answer must survive a forgery at its own sequence"
    assert "Ship the goods" not in json.dumps(rows), \
        "and the forged text must appear nowhere in the stored row"
    assert c.rejected == 1
    assert c.rejected_not_stored == 1, \
        "the refused insert must be counted, not dropped silently"


def test_a_redelivered_forgery_does_not_accumulate_rows(tmp_path, monkeypatch):
    """The idempotence half: a rejection may replace a rejection, so at-least-once
    redelivery of the same forgery leaves one row rather than failing to land."""
    c, store, _, _ = _drain(
        tmp_path, [_response(seq=2), _response(seq=2)], monkeypatch,
        keyset=_keyset(tmp_path), require_signature=True, bridge_kid="eb-01")
    rows = store.events_for("brave-otter-4718")
    assert len(rows) == 1 and rows[0]["phase"] == "error"
    assert c.rejected == 2
    assert c.rejected_not_stored == 0, "a rejection replacing a rejection is not a refusal"


def test_a_verified_answer_can_still_replace_a_redelivered_duplicate(tmp_path, monkeypatch):
    """The regression guard for the LEGITIMATE `INSERT OR REPLACE`.

    RQ-1 accepts at-least-once delivery and `emit.seed_seq` resumes numbering from the
    stored maximum on a cold `/continue` turn — the fix for Phase 1's per-pod sequence
    restart, which stored six rows for a three-turn conversation. A fix for #885 that
    made the insert non-clobbering in general would re-open that bug under a new name.
    """
    c, store, _, _ = _drain(
        tmp_path,
        [_response(seq=1, text="first delivery", seed=SEED_R1, kid="runner-01"),
         _response(seq=1, text="second delivery", seed=SEED_R1, kid="runner-01")],
        monkeypatch, keyset=_keyset(tmp_path), require_signature=True,
        bridge_kid="eb-01")
    rows = store.events_for("brave-otter-4718")
    assert len(rows) == 1, "a redelivered genuine frame must still collapse onto one row"
    assert rows[0]["data"]["text"] == "second delivery"
    assert c.rejected == 0 and c.rejected_not_stored == 0


def test_a_verified_answer_replaces_an_earlier_rejection_at_the_same_sequence(
        tmp_path, monkeypatch):
    """The asymmetry's other direction: the forgery arrives first, then the genuine
    answer. A rejected row is replaceable, so the real answer must win."""
    c, store, _, _ = _drain(
        tmp_path,
        [_response(seq=4, text="Ship the goods"),
         _response(seq=4, text="2+2 is 4", seed=SEED_R1, kid="runner-01")],
        monkeypatch, keyset=_keyset(tmp_path), require_signature=True,
        bridge_kid="eb-01")
    rows = store.events_for("brave-otter-4718")
    assert len(rows) == 1
    assert rows[0]["phase"] == "result" and rows[0]["data"]["text"] == "2+2 is 4", \
        "a genuine answer must be able to replace a rejection at its sequence"
    assert c.rejected == 1 and c.rejected_not_stored == 0


def test_audit_mode_still_lets_an_unverified_event_act(tmp_path, monkeypatch):
    """The guard reads `ok`, never `verified`. Audit mode exists so an operator can
    watch the reject rate BEFORE enforcing, which needs the events to keep acting."""
    calls: list[dict] = []
    c, store, _, _ = _drain(
        tmp_path, [_response(groupid="g-1")], monkeypatch,
        keyset=_keyset(tmp_path), require_signature=False, bridge_kid="eb-01",
        on_member_event=calls.append)
    assert c.audit_failed == 1 and c.rejected == 0
    assert len(calls) == 1, "audit mode must not make an event inert"
    assert store.events_for("brave-otter-4718")[0]["phase"] == "result", \
        "and must store it unchanged"


def test_unsigned_streamed_frames_still_reach_the_group_service(tmp_path, monkeypatch):
    """`emit()` signs terminal events only, because a signature costs ~222 ms and
    signing every stdout frame would add minutes to a chatty run. So an unsigned
    non-terminal is accepted BY POLICY and reports `verified=True`.

    If a guard keyed on `verified` instead of `ok`, every streamed frame of every
    genuine run would stop driving `mark_member_running`. That is the §4.4 step 2
    mistake, which rewrote every frame of every run to `phase="error"` and was found
    against a live broker rather than by any unit test.
    """
    calls: list[dict] = []
    c, _, _, _ = _drain(
        tmp_path,
        [_response(groupid="g-1", seq=1, phase="stdout", final="false", text="thinking"),
         _response(groupid="g-1", seq=2, phase="result", text="done",
                   seed=SEED_R1, kid="runner-01")],
        monkeypatch, keyset=_keyset(tmp_path), require_signature=True,
        bridge_kid="eb-01", on_member_event=calls.append)
    assert c.rejected == 0, "an unsigned non-terminal frame is not a rejection"
    assert len(calls) == 2, "both the streamed frame and the signed terminal must act"


def test_the_rejection_marker_cannot_arrive_from_the_wire(tmp_path, monkeypatch):
    """The marker is only unforgeable if the wire cannot produce it.

    `from_kafka_binary` derives attribute names from `ce_`-prefixed headers, so this
    would need a header literally named `ce___rejected`. Asserted rather than argued,
    because the whole guard rests on it: a forger who could set the marker could mark
    every genuine answer rejected, turning the control into a denial of service.
    """
    rec = _response(seq=1, text="hi", seed=SEED_R1, kid="runner-01")
    hdrs = list(rec.headers) + [("ce___rejected", b"true"),
                                (f"ce_{ce.KEY_REJECTED}", b"true")]
    c, store, seen, _ = _drain(tmp_path, [_Rec(hdrs, rec.value)], monkeypatch,
                               keyset=_keyset(tmp_path), require_signature=True,
                               bridge_kid="eb-01")
    assert c.rejected == 0, "the wire must not be able to mark an event rejected"
    assert seen and not ce.is_rejected(seen[0])
    assert store.events_for("brave-otter-4718")[0]["phase"] == "result"
