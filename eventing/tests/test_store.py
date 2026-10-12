"""SQLite Store: insert, filter by sequence, SSE notify."""
import pathlib
import threading

from eventbridge.store import Store
from shared import ce


def test_insert_and_events_for(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "test-otter-0001"
    for seq in range(1, 4):
        s.insert_response({
            "correlationid": corr, "sequence": seq, "phase": "stdout",
            "id": f"id-{seq}", "time": "2026-09-21T00:00:00Z",
            "data": {"seq": seq}, "final": "false",
        })
    all_evts = s.events_for(corr)
    assert [e["sequence"] for e in all_evts] == [1, 2, 3]

    since1 = s.events_for(corr, since_seq=1)
    assert [e["sequence"] for e in since1] == [2, 3]


def test_final_detected(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "test-otter-0002"
    s.insert_response({"correlationid": corr, "sequence": 1, "phase": "stdout",
                       "id": "a", "time": "t", "data": {}, "final": "false"})
    assert not s.final_seen(corr)
    s.insert_response({"correlationid": corr, "sequence": 2, "phase": "result",
                       "id": "b", "time": "t", "data": {}, "final": "true"})
    assert s.final_seen(corr)


def test_subscribe_notifies_on_insert(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "test-otter-0003"
    ev = s.subscribe(corr)
    def emit():
        s.insert_response({"correlationid": corr, "sequence": 1, "phase": "stdout",
                           "id": "x", "time": "t", "data": {}, "final": "false"})
    threading.Timer(0.05, emit).start()
    assert ev.wait(timeout=1.0), "subscriber was not notified"
    s.unsubscribe(corr, ev)


def test_upsert_session_increments_turns(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "test-otter-0004"
    s.upsert_session(corr, "uuid-x", "/tmp/work", "first")
    s.upsert_session(corr, "uuid-x", "/tmp/work", None)
    s.upsert_session(corr, "uuid-x", "/tmp/work", None)
    session = s.get_session(corr)
    assert session and session["turns"] == 2


# ---- the asymmetric conflict rule (#885 item 3) -----------------------------

def _row(corr, seq, text, *, rejected=False, phase="result"):
    e = {"correlationid": corr, "sequence": seq, "phase": phase, "id": f"id-{seq}",
         "time": "2026-10-09T00:00:00Z", "data": {"text": text}, "final": "true"}
    if rejected:
        e[ce.KEY_REJECTED] = True
    return e


def test_a_duplicate_delivery_still_collapses_onto_one_row(tmp_path: pathlib.Path):
    """The contract the asymmetry must not break, stated so the next person changing
    conflict resolution can see what it protects.

    RQ-1 accepts at-least-once delivery, and `emit.seed_seq` resumes numbering from the
    stored maximum on a cold `/continue` turn — the fix for the Phase 1 bug where a new
    pod restarted `sequence` at 1 and stored six rows for a three-turn conversation.
    """
    s = Store(tmp_path)
    corr = "test-otter-0010"
    assert s.insert_response(_row(corr, 1, "first delivery")) is True
    assert s.insert_response(_row(corr, 1, "second delivery")) is True
    rows = s.events_for(corr)
    assert len(rows) == 1, "a redelivered genuine frame must collapse onto one row"
    assert rows[0]["data"]["text"] == "second delivery"


def test_a_rejected_row_never_replaces_one_that_was_not_rejected(tmp_path: pathlib.Path):
    """#885 item 3 at the store boundary, with no Kafka in the picture."""
    s = Store(tmp_path)
    corr = "test-otter-0011"
    s.insert_response(_row(corr, 1, "2+2 is 4"))
    assert s.insert_response(_row(corr, 1, "Ship the goods", rejected=True)) is False, \
        "the refusal must be reported so the caller can count it"
    rows = s.events_for(corr)
    assert len(rows) == 1 and rows[0]["data"]["text"] == "2+2 is 4", \
        "the verified answer must survive"


def test_a_rejected_row_may_replace_another_rejection(tmp_path: pathlib.Path):
    """So a redelivered forgery stays idempotent rather than accumulating rows."""
    s = Store(tmp_path)
    corr = "test-otter-0012"
    assert s.insert_response(_row(corr, 1, "forgery one", rejected=True)) is True
    assert s.insert_response(_row(corr, 1, "forgery two", rejected=True)) is True
    rows = s.events_for(corr)
    assert len(rows) == 1 and rows[0]["data"]["text"] == "forgery two"


def test_a_genuine_row_may_replace_a_rejection(tmp_path: pathlib.Path):
    """The other direction: the forgery landed first, the real answer must still win."""
    s = Store(tmp_path)
    corr = "test-otter-0013"
    s.insert_response(_row(corr, 1, "Ship the goods", rejected=True))
    assert s.insert_response(_row(corr, 1, "2+2 is 4")) is True
    rows = s.events_for(corr)
    assert len(rows) == 1 and rows[0]["data"]["text"] == "2+2 is 4"


def test_the_rejected_column_is_added_to_an_existing_database(tmp_path: pathlib.Path):
    """The idempotent ALTER, mirroring the `prompts.submitter` precedent: a database
    created before this column existed must keep working, because `CREATE TABLE IF NOT
    EXISTS` is a no-op on an existing file."""
    import sqlite3
    (tmp_path / "x").mkdir()
    conn = sqlite3.connect(tmp_path / "x" / "responses.sqlite", isolation_level=None)
    conn.execute("CREATE TABLE responses (correlationid TEXT NOT NULL, "
                 "sequence INTEGER NOT NULL, phase TEXT NOT NULL, "
                 "event_id TEXT NOT NULL, event_time TEXT NOT NULL, "
                 "data_json TEXT NOT NULL, final INTEGER NOT NULL DEFAULT 0, "
                 "raw_json TEXT NOT NULL, PRIMARY KEY (correlationid, sequence))")
    conn.close()
    s = Store(tmp_path / "x")
    corr = "test-otter-0014"
    assert s.insert_response(_row(corr, 1, "after upgrade")) is True
    assert s.insert_response(_row(corr, 1, "forged", rejected=True)) is False, \
        "the upgraded database must enforce the rule too"
    assert s.events_for(corr)[0]["data"]["text"] == "after upgrade"
    Store(tmp_path / "x")   # reopening must not raise on the already-added column
