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
    genuinely approved — it is just not the bridge."""
    groups: list[dict] = []
    c, _, _, _ = _drain(tmp_path, [_group_event(seed=SEED_R1, kid="runner-01")],
                        monkeypatch, keyset=_keyset(tmp_path),
                        require_signature=True, bridge_kid="eb-01",
                        on_group_event=groups.append)
    assert c.rejected == 1
    assert groups and groups[0]["phase"] == "error", \
        "the group event is marked rejected rather than settling the group"


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
