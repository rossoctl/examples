"""Agent groups — batch fan-out with a tracked fan-in. DESIGN_PHASE1 §21.

The tests that matter most are the ones about *counting*. §21.5 changed the brief's
decrementing counter to derived counts, because RQ-1 accepts at-least-once delivery:
a redelivered request re-runs the agent and emits a second terminal event, and a
counter would then reach zero while agents were still running — firing "all done"
early, which is the one lie a progress display must never tell.
"""
import datetime as dt
import io
import json
import pathlib

import pytest

from eventbridge.config import Cfg as EbCfg
from eventbridge.group_service import GroupService
from eventbridge.group_view import render
from eventbridge.groups import MIN_FINISHED_FOR_ETA, completion_reason, humanize, progress, summary_line
from eventbridge.handlers import Handlers
from eventbridge.store import Store
from shared import ce

# ---- helpers ----------------------------------------------------------------

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
    r"""Deterministic ids that still satisfy correlation.REGEX
    (^[a-z]{3,10}-[a-z]{3,12}-\d{4}$), because the handlers validate the shape."""

    def __init__(self):
        self.n = 0

    def mint(self):
        self.n += 1
        return f"test-agent-{self.n:04d}"

    def remember(self, corr):
        pass


@pytest.fixture
def svc(tmp_path):
    store = Store(tmp_path / "eb")
    cfg = EbCfg(tmpdir=str(tmp_path), public_base_url="http://eb.test")
    producer = FakeProducer()
    return GroupService(cfg, store, producer, SeqMinter()), store, producer, cfg


def member_event(groupid, corr, *, final=False, phase="stdout", text="hi", seq=1):
    return {"correlationid": corr, "groupid": groupid, "sequence": seq,
            "phase": phase, "final": "true" if final else "false",
            "data": {"text": text, "role": "final" if final else "assistant"}}


# ---- the wire ---------------------------------------------------------------

def test_group_event_types_are_distinguishable():
    assert ce.is_group_event(ce.CloudEvent(attrs={"type": ce.TYPE_GROUP_STARTED}))
    assert ce.is_group_event(ce.CloudEvent(attrs={"type": ce.TYPE_GROUP_COMPLETED}))
    assert not ce.is_group_event(ce.CloudEvent(attrs={"type": ce.TYPE_RESPONSE}))


def test_groupid_survives_the_kafka_binary_binding():
    e = ce.new_event(type=ce.TYPE_RESPONSE, source="s", datacontenttype="application/json",
                     correlationid="c1", groupid="g1", sequence=3, phase="result",
                     final="true", data={"text": "done"})
    back = ce.from_kafka_binary(*ce.to_kafka_binary(e))
    assert back["groupid"] == "g1"
    assert back["correlationid"] == "c1"


def test_a_group_event_carries_no_correlationid():
    """Which is exactly why the responses consumer must route on type: handing this to
    insert_response would violate its (correlationid, sequence) primary key."""
    e = ce.new_event(type=ce.TYPE_GROUP_STARTED, source="s",
                     datacontenttype="application/json", groupid="g1",
                     data={"expected": 3})
    back = ce.from_kafka_binary(*ce.to_kafka_binary(e))
    assert "correlationid" not in back.attrs
    assert back["groupid"] == "g1"


# ---- §21.5 counting: the correction to the brief ---------------------------

def test_a_redelivered_terminal_event_does_not_count_twice(svc):
    s, store, _, _ = svc
    gid, _ = s.create(label="b", expected=3)
    corrs = s.submit_members(gid, ["a", "b", "c"])
    for c in corrs:
        s.on_member_event(member_event(gid, c, final=True, phase="result"))
    assert store.group_counts(gid)["terminal"] == 3
    # The same terminal events arrive again — redelivery, or a topic re-scan.
    for c in corrs:
        s.on_member_event(member_event(gid, c, final=True, phase="result"))
    assert store.group_counts(gid)["terminal"] == 3, \
        "a duplicate terminal event must not be counted again"


def test_completion_does_not_fire_early_under_redelivery(svc):
    """The concrete failure a decrementing counter would produce: two duplicates of
    member 1 would take a 3-agent counter to zero with two agents still running."""
    s, store, producer, _ = svc
    gid, _ = s.create(label="b", expected=3)
    corrs = s.submit_members(gid, ["a", "b", "c"])
    for _ in range(3):
        s.on_member_event(member_event(gid, corrs[0], final=True, phase="result"))
    assert store.get_group(gid)["completed_utc"] is None, "completed with 2 agents left"
    completions = [e for e in producer.group_events
                   if e["type"] == ce.TYPE_GROUP_COMPLETED]
    assert completions == []


def test_the_completion_event_is_published_exactly_once(svc):
    s, store, producer, _ = svc
    gid, _ = s.create(label="b", expected=2)
    corrs = s.submit_members(gid, ["a", "b"])
    for c in corrs:
        s.on_member_event(member_event(gid, c, final=True, phase="result"))
    # Extra triggers from any source must not re-publish.
    s.maybe_complete(gid)
    s.maybe_complete(gid)
    for c in corrs:
        s.on_member_event(member_event(gid, c, final=True, phase="result"))
    completions = [e for e in producer.group_events
                   if e["type"] == ce.TYPE_GROUP_COMPLETED]
    assert len(completions) == 1, f"published {len(completions)} completion events"


def test_the_completion_guard_survives_a_restart(tmp_path):
    """The guard is a row, not a flag, so a fresh GroupService over the same store
    cannot publish a second completion."""
    store = Store(tmp_path / "eb")
    cfg = EbCfg(tmpdir=str(tmp_path))
    p1, p2 = FakeProducer(), FakeProducer()
    s1 = GroupService(cfg, store, p1, SeqMinter())
    gid, _ = s1.create(label="b", expected=1)
    corr = s1.submit_members(gid, ["a"])[0]
    s1.on_member_event(member_event(gid, corr, final=True, phase="result"))
    assert len([e for e in p1.group_events if e["type"] == ce.TYPE_GROUP_COMPLETED]) == 1

    s2 = GroupService(cfg, store, p2, SeqMinter())   # "restarted" EventBridge
    s2.on_member_event(member_event(gid, corr, final=True, phase="result"))
    s2.maybe_complete(gid)
    assert [e for e in p2.group_events if e["type"] == ce.TYPE_GROUP_COMPLETED] == []


def test_a_failed_member_is_terminal_but_not_a_success(svc):
    s, store, _, _ = svc
    gid, _ = s.create(label="b", expected=2)
    a, b = s.submit_members(gid, ["a", "b"])
    s.on_member_event(member_event(gid, a, final=True, phase="result"))
    s.on_member_event(member_event(gid, b, final=True, phase="error"))
    counts = store.group_counts(gid)
    assert counts["finished"] == 1 and counts["failed"] == 1
    assert counts["terminal"] == 2
    assert store.get_group(gid)["completed_utc"] is not None


def test_a_member_event_for_an_unknown_group_is_accepted(svc):
    """A restart re-scans the topic and a fast agent can finish before its group's
    started event is processed, so terminal facts can genuinely arrive first."""
    s, store, _, _ = svc
    s.on_member_event(member_event("ghost-group", "lost-agent-9999", final=True,
                                   phase="result"))
    assert store.group_counts("ghost-group")["terminal"] == 1
    # And the group row can be filled in afterwards without losing the fact.
    store.create_group("ghost-group", label="late", expected=1)
    assert store.group_counts("ghost-group")["finished"] == 1


def test_non_final_events_mark_a_member_running(svc):
    s, store, _, _ = svc
    gid, _ = s.create(label="b", expected=1)
    corr = s.submit_members(gid, ["a"])[0]
    assert store.group_counts(gid)["submitted"] == 1
    s.on_member_event(member_event(gid, corr, final=False))
    assert store.group_counts(gid)["running"] == 1


# ---- idempotent creation ----------------------------------------------------

def test_an_idempotency_key_prevents_a_double_batch(svc):
    s, store, producer, _ = svc
    g1, created1 = s.create(label="b", expected=100, idempotency_key="abc")
    g2, created2 = s.create(label="b", expected=100, idempotency_key="abc")
    assert g1 == g2
    assert created1 is True and created2 is False
    starts = [e for e in producer.group_events if e["type"] == ce.TYPE_GROUP_STARTED]
    assert len(starts) == 1, "a retry must not announce a second group"


def test_fan_out_records_membership_before_publishing(svc):
    """Otherwise a fast agent's terminal event arrives for a member nobody recorded."""
    s, store, producer, _ = svc
    gid, _ = s.create(label="b", expected=3)
    corrs = s.submit_members(gid, ["p1", "p2", "p3"])
    assert len(producer.requests) == 3
    assert all(r["groupid"] == gid for r in producer.requests)
    assert {m["correlationid"] for m in store.group_members(gid)} == set(corrs)


# ---- completion rules -------------------------------------------------------

def test_quorum_completes_early():
    g = {"groupid": "g", "expected": 10, "min_success": 3}
    assert completion_reason(g, {"terminal": 2, "finished": 2, "members": 10}) is None
    assert completion_reason(g, {"terminal": 3, "finished": 3, "members": 10}) == "quorum"


def test_a_deadline_completes_a_short_group():
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5)).isoformat()
    g = {"groupid": "g", "expected": 10, "deadline_utc": past}
    assert completion_reason(g, {"terminal": 4, "finished": 4, "members": 10}) == "deadline"


def test_cancellation_beats_the_count():
    g = {"groupid": "g", "expected": 10, "cancelled_utc": "2026-01-01T00:00:00Z"}
    assert completion_reason(g, {"terminal": 10, "finished": 10, "members": 10}) == "cancelled"


def test_an_over_subscribed_group_can_still_complete():
    g = {"groupid": "g", "expected": 3}
    assert completion_reason(g, {"terminal": 5, "finished": 5, "members": 5}) == "all"


def test_an_open_ended_group_only_completes_once_closed():
    g = {"groupid": "g", "expected": None}
    counts = {"terminal": 4, "finished": 4, "members": 4}
    assert completion_reason(g, counts) is None
    assert completion_reason({**g, "closed_utc": "2026-01-01T00:00:00Z"}, counts) == "all"


def test_the_deadline_sweep_completes_and_notifies(svc):
    s, store, producer, _ = svc
    gid, _ = s.create(label="b", expected=5, deadline_s=-1)   # already past
    s.submit_members(gid, ["a", "b"])
    assert s.sweep_deadlines() == 1
    g = store.get_group(gid)
    assert g["completion_reason"] == "deadline"
    done = [e for e in producer.group_events if e["type"] == ce.TYPE_GROUP_COMPLETED]
    assert len(done) == 1 and done[0]["data"]["reason"] == "deadline"


# ---- progress arithmetic ----------------------------------------------------

def _members(now, finished=0, running=0, queued=0, failed=0, spacing=5):
    def iso(off):
        return (now + dt.timedelta(seconds=off)).isoformat().replace("+00:00", "Z")
    out = []
    i = 0
    for k in range(finished):
        out.append({"correlationid": f"f{k}", "status": "finished",
                    "submitted_utc": iso(-300), "first_event_utc": iso(-290),
                    "finished_utc": iso(-200 + k * spacing), "terminal_phase": "result"})
        i += 1
    for k in range(failed):
        out.append({"correlationid": f"x{k}", "status": "failed",
                    "submitted_utc": iso(-300), "first_event_utc": iso(-290),
                    "finished_utc": iso(-150 + k * spacing), "terminal_phase": "error"})
    for k in range(running):
        out.append({"correlationid": f"r{k}", "status": "running",
                    "submitted_utc": iso(-300), "first_event_utc": iso(-20),
                    "finished_utc": None, "terminal_phase": None})
    for k in range(queued):
        out.append({"correlationid": f"q{k}", "status": "submitted",
                    "submitted_utc": iso(-300), "first_event_utc": None,
                    "finished_utc": None, "terminal_phase": None})
    return out


def test_counts_are_reported_in_user_units():
    now = dt.datetime(2026, 9, 23, 12, tzinfo=dt.timezone.utc)
    g = {"groupid": "g", "expected": 40, "created_utc": now.isoformat()}
    p = progress(g, _members(now, finished=11, failed=1, running=6, queued=22), now=now)
    assert (p["terminal"], p["denominator"]) == (12, 40)
    assert p["finished"] == 11 and p["failed"] == 1
    assert "12/40 done" in summary_line(p)


def test_the_denominator_never_shrinks_when_more_members_appear():
    """A bar that moves backward breaks the one promise the widget makes, so an
    over-subscribed group revises the total upward and says so."""
    now = dt.datetime(2026, 9, 23, 12, tzinfo=dt.timezone.utc)
    g = {"groupid": "g", "expected": 3, "created_utc": now.isoformat()}
    p = progress(g, _members(now, finished=5), now=now)
    assert p["denominator"] == 5
    assert p["denominator_revised"] is True
    assert p["percent"] == 100.0


def test_an_open_ended_group_shows_a_running_count_and_no_bar():
    now = dt.datetime(2026, 9, 23, 12, tzinfo=dt.timezone.utc)
    g = {"groupid": "g", "expected": None, "created_utc": now.isoformat()}
    p = progress(g, _members(now, finished=17, running=2), now=now)
    assert p["finished"] == 17
    assert p["denominator"] == 19        # what we have seen, not a guess
    assert "17" in summary_line(p)


def test_no_eta_until_enough_members_have_finished():
    now = dt.datetime(2026, 9, 23, 12, tzinfo=dt.timezone.utc)
    g = {"groupid": "g", "expected": 20, "created_utc": now.isoformat()}
    below = progress(g, _members(now, finished=MIN_FINISHED_FOR_ETA - 1, queued=17), now=now)
    assert below["eta"] is None, "an estimate from 2 samples is noise"
    above = progress(g, _members(now, finished=MIN_FINISHED_FOR_ETA + 3, queued=13), now=now)
    assert above["eta"] is not None


def test_a_completed_group_shows_no_eta():
    now = dt.datetime(2026, 9, 23, 12, tzinfo=dt.timezone.utc)
    g = {"groupid": "g", "expected": 5, "created_utc": now.isoformat(),
         "completed_utc": now.isoformat(), "completion_reason": "all"}
    p = progress(g, _members(now, finished=5), now=now)
    assert p["eta"] is None and p["state"] == "all"


def test_estimates_are_rounded_not_precise():
    assert humanize(10) == "less than a minute"
    assert humanize(120) == "about 2 minutes"
    assert humanize(4000).endswith("minutes")     # not "66.7 minutes"
    assert ":" not in (humanize(4000) or "")
    assert humanize(None) is None


def test_a_stall_is_stated_in_words():
    now = dt.datetime(2026, 9, 23, 12, tzinfo=dt.timezone.utc)
    g = {"groupid": "g", "expected": 10, "created_utc": now.isoformat()}
    members = _members(now, finished=3, queued=7)
    for m in members:                              # last activity long ago
        if m["finished_utc"]:
            m["finished_utc"] = (now - dt.timedelta(seconds=600)).isoformat()
        if m["first_event_utc"]:
            m["first_event_utc"] = (now - dt.timedelta(seconds=600)).isoformat()
    p = progress(g, members, now=now)
    assert p["stall_note"] and "no activity" in p["stall_note"]


# ---- the page ---------------------------------------------------------------

def _page(**over):
    p = {"groupid": "swift-falcon-3921", "label": "nightly", "state": "running",
         "expected": 40, "denominator": 40, "denominator_revised": False,
         "members_seen": 40, "terminal": 12, "finished": 11, "failed": 1,
         "running": 6, "queued": 22, "percent": 30.0, "elapsed": "3m12s",
         "eta": "about 5 minutes", "throughput_per_min": 3.4, "stall_note": None,
         "completed_utc": None,
         "members": [{"correlationid": "warm-gecko-4156", "status": "finished",
                      "duration": "2.1s", "text": "MOCK-REPLY[hi]"}]}
    p.update(over)
    return p


def test_the_page_leads_with_counts_then_percentage():
    h = render(_page(), base_url="https://eb.test")
    assert "of 40 agents finished" in h
    assert h.index("of 40 agents finished") < h.index("30%")


def test_the_page_links_each_member_to_its_own_agent_page():
    h = render(_page(), base_url="https://eb.test")
    assert 'href="https://eb.test/v0/agents/warm-gecko-4156"' in h


def test_the_bar_is_accessible_and_counts_agents_not_percent():
    h = render(_page(), base_url="")
    assert 'role="progressbar"' in h
    assert 'aria-valuenow="12"' in h and 'aria-valuemax="40"' in h


def test_the_page_states_a_revised_denominator():
    h = render(_page(expected=3, denominator=5, denominator_revised=True), base_url="")
    assert "more agents joined than declared" in h


def test_the_page_shows_a_stall_note_when_stalled():
    h = render(_page(stall_note="no activity for 4m00s"), base_url="")
    assert "no activity for 4m00s" in h and "may be stuck" in h


def test_a_finished_group_does_not_poll():
    h = render(_page(state="all", completed_utc="2026-09-23T12:00:00Z"), base_url="")
    assert '"done": true' in h


# ---- ntfy: announce the edges, suppress the middle -------------------------

class RecordingNtfy:
    """NtfyPublisher with the network replaced, so submit/suppress logic is testable."""

    def __init__(self, store, notify_errors=False):
        from eventbridge.config import NtfyCfg
        from eventbridge.ntfy import NtfyPublisher
        cfg = NtfyCfg(enabled=True, topic="t", group_notify_errors=notify_errors)
        self.pub = NtfyPublisher(cfg, "http://eb.test", store=store)
        self.sent = []
        self.pub._post = lambda payload: self.sent.append(payload)

    def submit_and_drain(self, event):
        self.pub.submit(event)
        while not self.pub._q.empty():
            self.pub._publish(self.pub._q.get())


def test_a_group_start_notifies_and_members_do_not(svc):
    s, store, _, _ = svc
    gid, _ = s.create(label="batch", expected=2)
    corrs = s.submit_members(gid, ["a", "b"])
    n = RecordingNtfy(store)

    n.submit_and_drain({"type": ce.TYPE_GROUP_STARTED, "groupid": gid,
                        "data": {"label": "batch", "expected": 2}})
    assert len(n.sent) == 1 and "started" in n.sent[0]["title"]

    n.submit_and_drain(member_event(gid, corrs[0], final=True, phase="result"))
    assert len(n.sent) == 1, "a member of an open group must not notify"


def test_the_group_completion_notifies_with_totals(svc):
    s, store, _, _ = svc
    gid, _ = s.create(label="batch", expected=1)
    n = RecordingNtfy(store)
    n.submit_and_drain({"type": ce.TYPE_GROUP_COMPLETED, "groupid": gid,
                        "data": {"label": "batch", "reason": "all", "finished": 9,
                                 "failed": 1, "expected": 10, "elapsed": "2m10s",
                                 "slowest_member_s": 7.2}})
    assert len(n.sent) == 1
    body = n.sent[0]["message"]
    assert "9 finished" in body and "1 failed" in body and "2m10s" in body
    assert n.sent[0]["click"].endswith(f"/v0/groups/{gid}")


def test_a_redelivered_member_event_after_completion_still_does_not_notify(svc):
    """The regression that produced ~47 notifications for a 100-agent batch.

    RQ-1's at-least-once delivery means terminal events arrive again AFTER the group
    completes: KEDA scales a pod down, its offset was never committed, Kafka redelivers
    the request and the agent re-runs. Suppression keyed on "group still open" let every
    one of those through. Membership is the correct key.
    """
    s, store, _, _ = svc
    gid, _ = s.create(label="batch", expected=1)
    corr = s.submit_members(gid, ["a"])[0]
    s.on_member_event(member_event(gid, corr, final=True, phase="result"))
    assert store.get_group(gid)["completed_utc"] is not None

    n = RecordingNtfy(store)
    n.submit_and_drain(member_event(gid, corr, final=True, phase="result"))
    n.submit_and_drain(member_event(gid, "late-agent-0009", final=True, phase="result"))
    assert n.sent == [], "a batch is two notifications, not two plus its redeliveries"


def test_a_hundred_members_produce_exactly_two_notifications(svc):
    """The property the user asked for, stated as a test: batch in, two out."""
    s, store, _, _ = svc
    gid, _ = s.create(label="demo-100", expected=100)
    corrs = s.submit_members(gid, [f"t{i}" for i in range(100)])
    n = RecordingNtfy(store)

    n.submit_and_drain({"type": ce.TYPE_GROUP_STARTED, "groupid": gid,
                        "data": {"label": "demo-100", "expected": 100}})
    for i, c in enumerate(corrs):
        n.submit_and_drain(member_event(gid, c, final=True, phase="result"))
        if i % 3 == 0:                                   # redelivery
            n.submit_and_drain(member_event(gid, c, final=True, phase="result"))
    n.submit_and_drain({"type": ce.TYPE_GROUP_COMPLETED, "groupid": gid,
                        "data": {"label": "demo-100", "reason": "all",
                                 "finished": 100, "failed": 0, "expected": 100}})
    assert len(n.sent) == 2, f"expected 2 notifications, got {len(n.sent)}"
    assert "started" in n.sent[0]["title"] and "100 done" in n.sent[1]["title"]


def test_an_ungrouped_agent_still_notifies(svc):
    s, store, _, _ = svc
    n = RecordingNtfy(store)
    n.submit_and_drain({"correlationid": "solo", "phase": "result", "final": "true",
                        "data": {"text": "hi"}})
    assert len(n.sent) == 1


def test_member_errors_can_be_surfaced_by_config(svc):
    """Off by default to match the brief; recommended on, because a batch where 30 of
    40 agents are failing otherwise looks healthy until the very end."""
    s, store, _, _ = svc
    gid, _ = s.create(label="batch", expected=5)
    corr = s.submit_members(gid, ["a"])[0]

    quiet = RecordingNtfy(store, notify_errors=False)
    quiet.submit_and_drain(member_event(gid, corr, final=True, phase="error"))
    assert quiet.sent == []

    loud = RecordingNtfy(store, notify_errors=True)
    loud.submit_and_drain(member_event(gid, corr, final=True, phase="error"))
    assert len(loud.sent) == 1


# ---- HTTP surface -----------------------------------------------------------

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
    return captured["status"], b"".join(out)


@pytest.fixture
def api(svc):
    s, store, producer, cfg = svc
    return Handlers(cfg, store, producer, SeqMinter(), groups=s), s, store, producer


def test_post_groups_creates_and_fans_out(api):
    h, s, store, producer = api
    status, body = _call(h.create_group, method="POST",
                         body={"label": "b", "prompts": ["one", "two", "three"]})
    assert status.startswith("202")
    out = json.loads(body)
    assert len(out["members"]) == 3 and out["expected"] == 3
    assert out["html_url"].endswith(f"/v0/groups/{out['groupid']}")
    assert len(producer.requests) == 3


def test_post_groups_rejects_a_contradictory_expected(api):
    h, *_ = api
    status, body = _call(h.create_group, method="POST",
                         body={"prompts": ["a"], "expected": 5})
    assert status.startswith("400")
    assert "contradicts" in json.loads(body)["error"]


def test_post_groups_needs_prompts_or_expected(api):
    h, *_ = api
    status, _ = _call(h.create_group, method="POST", body={"label": "b"})
    assert status.startswith("400")


def test_the_idempotency_key_header_is_honoured(api):
    h, s, store, producer = api
    env = {"HTTP_IDEMPOTENCY_KEY": "retry-1"}
    st1, b1 = _call(h.create_group, method="POST", body={"prompts": ["a", "b"]},
                    environ=env)
    st2, b2 = _call(h.create_group, method="POST", body={"prompts": ["a", "b"]},
                    environ=env)
    assert json.loads(b1)["groupid"] == json.loads(b2)["groupid"]
    assert json.loads(b2)["created"] is False
    assert len(producer.requests) == 2, "a retry must not submit the batch twice"


def test_group_status_reports_progress(api):
    h, s, store, _ = api
    gid, _ = s.create(label="b", expected=2)
    corrs = s.submit_members(gid, ["a", "b"])
    s.on_member_event(member_event(gid, corrs[0], final=True, phase="result"))
    status, body = _call(h.group_status, groupid=gid)
    assert status.startswith("200")
    p = json.loads(body)
    assert p["finished"] == 1 and p["denominator"] == 2
    assert p["members"][0]["correlationid"] == corrs[0]


def test_group_status_404s_for_an_unknown_group(api):
    h, *_ = api
    status, _ = _call(h.group_status, groupid="nope-missing-0001")
    assert status.startswith("404")


def test_the_group_page_renders(api):
    h, s, store, _ = api
    gid, _ = s.create(label="b", expected=1)
    corr = s.submit_members(gid, ["a"])[0]
    status, body = _call(h.group_html, groupid=gid)
    assert status.startswith("200")
    assert corr.encode() in body


def test_close_and_cancel(api):
    h, s, store, _ = api
    gid, _ = s.create(label="b", expected=None)
    s.submit_members(gid, ["a"])
    status, body = _call(h.cancel_group, method="POST", groupid=gid)
    assert status.startswith("200")
    out = json.loads(body)
    assert out["cancelled"] is True
    # The limitation is stated rather than implied.
    assert "NOT interrupted" in out["note"]


def test_start_agent_can_join_an_existing_group(api):
    h, s, store, producer = api
    gid, _ = s.create(label="b", expected=2)
    status, body = _call(h.start_agent, method="POST",
                         body={"prompt": "hello", "groupid": gid})
    assert status.startswith("202")
    out = json.loads(body)
    assert out["groupid"] == gid
    assert store.group_counts(gid)["members"] == 1
    assert producer.requests[-1]["groupid"] == gid


def test_start_agent_rejects_a_malformed_groupid(api):
    h, *_ = api
    status, _ = _call(h.start_agent, method="POST",
                      body={"prompt": "hi", "groupid": "Bad Group!"})
    assert status.startswith("400")


# ---- end to end through the service ----------------------------------------

def test_a_hundred_member_group_completes_once(svc):
    """The demo shape: 100 members, some failing, duplicates in the stream."""
    s, store, producer, _ = svc
    gid, _ = s.create(label="demo-100", expected=100)
    corrs = s.submit_members(gid, [f"task {i}" for i in range(100)])
    assert len(corrs) == 100

    for i, c in enumerate(corrs):
        s.on_member_event(member_event(gid, c, final=False))
        phase = "error" if i % 25 == 0 else "result"
        s.on_member_event(member_event(gid, c, final=True, phase=phase))
        if i % 10 == 0:                      # redelivery
            s.on_member_event(member_event(gid, c, final=True, phase=phase))

    counts = store.group_counts(gid)
    assert counts["terminal"] == 100
    assert counts["failed"] == 4 and counts["finished"] == 96
    completions = [e for e in producer.group_events
                   if e["type"] == ce.TYPE_GROUP_COMPLETED]
    assert len(completions) == 1
    assert completions[0]["data"]["finished"] == 96
    p = s.snapshot(gid)
    assert p["percent"] == 100.0 and p["state"] == "all"


# ---- back-links, the list page, and last-updated ----------------------------

def test_the_agent_page_links_back_to_its_group(api):
    """Without this a member reached from the group page is a dead end: the operator
    has to hand-edit the URL to get back to the batch they were watching."""
    h, s, store, _ = api
    gid, _ = s.create(label="batch-a", expected=1)
    corr = s.submit_members(gid, ["p"])[0]
    status, body = _call(h.get_html, correlationid=corr)
    assert status.startswith("200")
    page = body.decode()
    assert f'/v0/groups/{gid}' in page and "back to group" in page
    assert "batch-a" in page


def test_an_ungrouped_agent_page_has_no_back_link(api):
    h, s, store, _ = api
    store.upsert_session("solo-agent-0001", "u", "/w", "hi")
    status, body = _call(h.get_html, correlationid="solo-agent-0001")
    assert status.startswith("200")
    assert "back to group" not in body.decode()


def test_turns_exposes_the_group_so_the_cli_can_follow_it(api):
    h, s, store, _ = api
    gid, _ = s.create(label="b", expected=1)
    corr = s.submit_members(gid, ["p"])[0]
    status, body = _call(h.get_turns, correlationid=corr)
    out = json.loads(body)
    assert out["groupid"] == gid
    assert out["group_url"].endswith(f"/v0/groups/{gid}")


def test_the_group_list_carries_counts_and_last_activity(api):
    h, s, store, _ = api
    gid, _ = s.create(label="b", expected=2)
    corrs = s.submit_members(gid, ["a", "b"])
    s.on_member_event(member_event(gid, corrs[0], final=True, phase="result"))
    rows = store.all_groups()
    row = next(r for r in rows if r["groupid"] == gid)
    assert row["members"] == 2 and row["finished"] == 1
    assert row["last_activity_utc"], "the list must say when the batch last moved"


def test_the_group_list_is_json_for_the_cli_and_html_for_a_browser(api):
    h, s, store, _ = api
    s.create(label="b", expected=1)

    status, body = _call(h.list_groups)
    assert status.startswith("200") and json.loads(body)["groups"]

    status, body = _call(h.list_groups, environ={"HTTP_ACCEPT": "text/html"})
    assert status.startswith("200")
    page = body.decode()
    assert "<title>agent groups</title>" in page
    assert "last activity" in page


def test_the_pages_update_in_place_rather_than_reloading(api):
    """A reload would discard scroll position and flicker a 100-row table once a
    second, which is the opposite of leaving a batch page open to watch."""
    h, s, store, _ = api
    gid, _ = s.create(label="b", expected=1)
    s.submit_members(gid, ["a"])
    _, body = _call(h.group_html, groupid=gid)
    page = body.decode()
    assert "setTimeout(tick, 1000)" in page, "must poll every second"
    assert "last updated" in page
    # The only mention of reload is the comment explaining why it is not used.
    assert page.count("location.reload") <= 1

    _, body = _call(h.list_groups, environ={"HTTP_ACCEPT": "text/html"})
    assert "setTimeout(tick, 1000)" in body.decode()


def test_partitions_allow_at_least_ten_pods():
    """§21 demo: a 100-agent batch should spread over >= 10 pods. Kafka gives each
    partition to exactly one consumer in a group, so maxReplicaCount is capped by the
    partition count — the two numbers are one decision."""
    import pathlib
    import re
    root = pathlib.Path(__file__).resolve().parents[1]
    topics = (root / "k8s" / "topics" / "kafkatopics.yaml").read_text()
    parts = [int(m) for m in re.findall(r"(?m)^\s+partitions:\s*(\d+)", topics)]
    assert parts and min(parts) >= 10, f"partitions {parts} cannot host 10 consumers"
    so = (root / "k8s" / "base" / "eventrunner-scaledobject.yaml").read_text()
    mx = int(re.search(r"maxReplicaCount:\s*(\d+)", so).group(1))
    assert mx >= 10, f"maxReplicaCount {mx} < 10"
    assert mx <= min(parts), f"maxReplicaCount {mx} exceeds {min(parts)} partitions"


def test_the_scaledobject_overrides_the_default_hpa_scaleup_policy():
    """maxReplicaCount alone does not give fast fan-out.

    KEDA handles 0 -> 1 itself but delegates 1 -> N to an HPA, and Kubernetes' default
    scaleUp policy adds at most max(100%, 4 pods) per 15s — so from one pod a burst
    needs two cycles (~30s) to reach the cap of 10, and would need three to reach 12. A
    100-agent batch that finishes in ~60s would be well into its second half before the
    capacity existed, which is why the policy is stated explicitly rather than left to
    the default. Asserted as a relation to maxReplicaCount, so changing the cap does not
    silently reintroduce the rate limit.
    """
    import pathlib
    import re
    so = (pathlib.Path(__file__).resolve().parents[1] / "k8s" / "base"
          / "eventrunner-scaledobject.yaml").read_text()
    assert "horizontalPodAutoscalerConfig" in so
    up = so[so.index("scaleUp:"):so.index("scaleDown:")]
    assert "stabilizationWindowSeconds: 0" in up
    value = int(re.search(r"type: Pods\s+value:\s*(\d+)", up).group(1))
    mx = int(re.search(r"maxReplicaCount:\s*(\d+)", so).group(1))
    assert value >= mx, (f"scaleUp allows {value} pods per step but the cap is {mx}; "
                         f"the burst would still be rate-limited")


# ---- replay: group history must survive an EventBridge restart ---------------
#
# §21.2 claimed the group lifecycle is "replayable from Kafka alone". It was not: the
# live responses consumer commits offsets under a fixed group id, so a restarted pod
# (with the test overlay's emptyDir /data) served 404 for every earlier group — while
# its two notifications had already been delivered, so clicking one landed on
# "unknown groupid". These tests pin the fix.

def test_a_replayed_group_is_rebuilt_without_republishing(svc):
    """The core rule: a replay rebuilds state, it does not create new facts."""
    s, store, producer, cfg = svc
    gid, _ = s.create(label="batch", expected=2)
    corrs = s.submit_members(gid, ["a", "b"])
    for c in corrs:
        s.on_member_event(member_event(gid, c, final=True, phase="result"))
    assert store.get_group(gid)["completed_utc"] is not None
    published = list(producer.group_events)
    assert len([e for e in published if e["type"] == ce.TYPE_GROUP_COMPLETED]) == 1

    # A brand-new EventBridge over an EMPTY store — what a restarted pod sees.
    fresh_store = Store(pathlib.Path(cfg.tmpdir) / "eb-restarted")
    fresh_prod = FakeProducer()
    restarted = GroupService(cfg, fresh_store, fresh_prod, SeqMinter())
    assert fresh_store.get_group(gid) is None

    # Replay the topic in the order it was written: started, members, completed.
    restarted.on_group_event({"type": ce.TYPE_GROUP_STARTED, "groupid": gid,
                              "data": published[0]["data"]}, replay=True)
    for c in corrs:
        restarted.on_member_event(member_event(gid, c, final=True, phase="result"),
                                  replay=True)
    done = next(e for e in published if e["type"] == ce.TYPE_GROUP_COMPLETED)
    restarted.on_group_event({"type": ce.TYPE_GROUP_COMPLETED, "groupid": gid,
                              "data": done["data"]}, replay=True)

    g = fresh_store.get_group(gid)
    assert g is not None, "the group must come back after a restart"
    assert g["expected"] == 2 and g["label"] == "batch"
    assert g["completed_utc"] is not None, "and come back already complete"
    assert fresh_store.group_counts(gid)["finished"] == 2
    assert fresh_prod.group_events == [], \
        "a replay must not re-publish — that would re-notify a batch finished hours ago"


def test_members_replayed_before_the_completed_event_do_not_complete_it(svc):
    """Ordering trap: on the topic the members come BEFORE `completed`, so a replay that
    completed on the last member would publish a duplicate before ever reading it."""
    s, store, producer, cfg = svc
    gid, _ = s.create(label="batch", expected=2)
    corrs = s.submit_members(gid, ["a", "b"])

    fresh_store = Store(pathlib.Path(cfg.tmpdir) / "eb-order")
    fresh_prod = FakeProducer()
    restarted = GroupService(cfg, fresh_store, fresh_prod, SeqMinter())
    restarted.on_group_event({"type": ce.TYPE_GROUP_STARTED, "groupid": gid,
                              "data": {"label": "batch", "expected": 2}}, replay=True)
    for c in corrs:
        restarted.on_member_event(member_event(gid, c, final=True, phase="result"),
                                  replay=True)
    assert fresh_prod.group_events == [], "all members done, but nothing may be published"
    assert fresh_store.get_group(gid)["completed_utc"] is None


def test_a_group_left_unfinished_by_a_crash_is_settled_once(svc):
    """If EventBridge died between the last member and the completed event, no completion
    exists on the topic. That IS a new fact, so the mirror settles it — and publishes."""
    s, store, producer, cfg = svc
    gid, _ = s.create(label="batch", expected=2)
    corrs = s.submit_members(gid, ["a", "b"])

    fresh_store = Store(pathlib.Path(cfg.tmpdir) / "eb-crash")
    fresh_prod = FakeProducer()
    restarted = GroupService(cfg, fresh_store, fresh_prod, SeqMinter())
    restarted.on_group_event({"type": ce.TYPE_GROUP_STARTED, "groupid": gid,
                              "data": {"label": "batch", "expected": 2}}, replay=True)
    for c in corrs:
        restarted.on_member_event(member_event(gid, c, final=True, phase="result"),
                                  replay=True)
    # What GroupMirror does after its scan.
    assert restarted.maybe_complete(gid) is True
    assert len([e for e in fresh_prod.group_events
                if e["type"] == ce.TYPE_GROUP_COMPLETED]) == 1
    assert restarted.maybe_complete(gid) is False, "and only once"


def test_the_mirror_reads_from_the_start_and_never_commits():
    """The two properties that make a replay possible at all. The live consumer does the
    opposite on purpose, which is why a second consumer is needed rather than a flag."""
    import inspect

    from eventbridge import kafka_group_mirror, kafka_in
    mirror = inspect.getsource(kafka_group_mirror)
    assert "seek_to_beginning" in mirror
    assert "enable_auto_commit=False" in mirror
    assert "group_id=None" in mirror, (
        "no consumer group: joining one costs a coordinator round trip and a rebalance, "
        "and the first version read 0 records because it mistook the join's empty polls "
        "for the end of the topic"
    )
    assert "end_offsets" in mirror, (
        "completion must be decided by offsets, not by a poll returning nothing"
    )

    live = inspect.getsource(kafka_in)
    assert "enable_auto_commit=True" in live, (
        "if the live consumer stops committing it would re-scan on every start and the "
        "mirror would be redundant — revisit this test, not just the code")


def test_the_mirror_only_touches_group_state(svc):
    """Response rows are left to the committing consumer: re-inserting every response on
    every start is exactly the cost that consumer exists to avoid."""
    import inspect

    from eventbridge import kafka_group_mirror
    src = inspect.getsource(kafka_group_mirror)
    assert "insert_response" not in src
    assert "on_group_event" in src and "on_member_event" in src


# ---- GroupMirror.run(): the scan itself ---------------------------------------


class _FakeRec:
    def __init__(self, headers, value, offset):
        self.headers, self.value, self.offset = headers, value, offset


class _FakeConsumer:
    """Enough of KafkaConsumer to drive run(). `empty_first` mimics the polls that a
    real consumer returns while it is still warming up — the trap the first version of
    this code fell into, where three of them looked like the end of the topic."""

    def __init__(self, records_by_partition, *, empty_first=0, metadata_after=0):
        self._recs = records_by_partition
        self._empty_first = empty_first
        self._metadata_after = metadata_after
        self._pos = {}
        self._assigned = []
        self.closed = False
        self.committed = False
        self.metadata_calls = 0

    # -- construction / assignment
    def partitions_for_topic(self, _topic):
        self.metadata_calls += 1
        if self.metadata_calls <= self._metadata_after:
            return None
        return set(self._recs)

    def assign(self, tps):
        self._assigned = list(tps)

    def end_offsets(self, tps):
        return {tp: len(self._recs.get(tp.partition, [])) for tp in tps}

    def seek_to_beginning(self, *tps):
        for tp in (tps or self._assigned):
            self._pos[tp.partition] = 0

    def position(self, tp):
        return self._pos.get(tp.partition, 0)

    # -- the read loop
    def poll(self, timeout_ms=None):  # noqa: ARG002
        if self._empty_first > 0:
            self._empty_first -= 1
            return {}
        out = {}
        for tp in self._assigned:
            recs = self._recs.get(tp.partition, [])
            at = self._pos.get(tp.partition, 0)
            if at < len(recs):
                out[tp] = recs[at:]
                self._pos[tp.partition] = len(recs)
        return out

    def commit(self, *a, **k):
        self.committed = True

    def close(self):
        self.closed = True


def _record(event_dict, offset):
    """A real wire record, so the test exercises ce decoding too."""
    evt = ce.new_event(**event_dict)
    headers, value = ce.to_kafka_binary(evt)
    return _FakeRec(headers, value, offset)


def _mirror_over(records_by_partition, groups, monkeypatch, **kw):
    from eventbridge import kafka_group_mirror as mod
    fake = _FakeConsumer(records_by_partition, **kw)
    monkeypatch.setattr(mod, "KafkaConsumer", lambda **_: fake)
    m = mod.GroupMirror("broker:9092", "responses", groups)
    m.run()          # synchronous: the scan is one-shot by design
    return m, fake


def test_run_rebuilds_a_group_across_partitions(svc, monkeypatch):
    s, store, producer, cfg = svc
    gid, _ = s.create(label="batch", expected=2)
    corrs = s.submit_members(gid, ["a", "b"])

    fresh_store = Store(pathlib.Path(cfg.tmpdir) / "eb-run")
    fresh_prod = FakeProducer()
    restarted = GroupService(cfg, fresh_store, fresh_prod, SeqMinter())

    started = {"type": ce.TYPE_GROUP_STARTED, "source": "/eb", "groupid": gid,
               "data": {"label": "batch", "expected": 2}}
    members = [{"type": ce.TYPE_RESPONSE, "source": "/er", "groupid": gid,
                "correlationid": c, "sequence": 3, "phase": "result", "final": True,
                "data": {"role": "final", "text": "ok"}} for c in corrs]
    done = {"type": ce.TYPE_GROUP_COMPLETED, "source": "/eb", "groupid": gid,
            "data": {"label": "batch", "reason": "all", "expected": 2, "finished": 2}}

    # Split across partitions, as a 12-partition topic really would, and make the first
    # polls come back empty.
    m, fake = _mirror_over(
        {0: [_record(started, 0), _record(done, 1)],
         5: [_record(members[0], 0)],
         9: [_record(members[1], 0)]},
        restarted, monkeypatch, empty_first=3,
    )

    g = fresh_store.get_group(gid)
    assert g is not None and g["expected"] == 2
    assert g["completed_utc"] is not None
    assert g["completion_reason"] == "all"
    assert fresh_store.group_counts(gid)["finished"] == 2
    assert m.rebuilt_groups == 2 and m.rebuilt_members == 2
    assert m.settled == 0, "the topic already held the completion — nothing to settle"
    assert fresh_prod.group_events == [], "a replay must not re-notify"
    assert fake.closed and not fake.committed


def test_run_waits_for_topic_metadata(svc, monkeypatch):
    """partitions_for_topic returns None until metadata arrives; concluding "empty topic"
    there would silently skip the whole replay."""
    _s, _store, _producer, cfg = svc
    fresh = GroupService(cfg, Store(pathlib.Path(cfg.tmpdir) / "eb-meta"),
                         FakeProducer(), SeqMinter())
    started = {"type": ce.TYPE_GROUP_STARTED, "source": "/eb", "groupid": "cool-ox-1234",
               "data": {"label": "l", "expected": 1}}
    m, fake = _mirror_over({0: [_record(started, 0)]}, fresh, monkeypatch,
                           metadata_after=3)
    assert fake.metadata_calls == 4
    assert m.rebuilt_groups == 1


def test_run_settles_a_group_the_topic_left_unfinished(svc, monkeypatch):
    """No group.completed on the topic: EventBridge died in the window between the last
    member and writing it. The mirror finishes the job, and publishes — once."""
    s, store, producer, cfg = svc
    gid, _ = s.create(label="batch", expected=2)
    corrs = s.submit_members(gid, ["a", "b"])

    fresh_store = Store(pathlib.Path(cfg.tmpdir) / "eb-settle")
    fresh_prod = FakeProducer()
    restarted = GroupService(cfg, fresh_store, fresh_prod, SeqMinter())
    started = {"type": ce.TYPE_GROUP_STARTED, "source": "/eb", "groupid": gid,
               "data": {"label": "batch", "expected": 2}}
    recs = {0: [_record(started, 0)]}
    for i, c in enumerate(corrs):
        recs[i + 1] = [_record({"type": ce.TYPE_RESPONSE, "source": "/er", "groupid": gid,
                                "correlationid": c, "sequence": 3, "phase": "result",
                                "final": True, "data": {"role": "final"}}, 0)]
    m, _fake = _mirror_over(recs, restarted, monkeypatch)

    assert m.settled == 1
    assert fresh_store.get_group(gid)["completed_utc"] is not None
    assert len([e for e in fresh_prod.group_events
                if e["type"] == ce.TYPE_GROUP_COMPLETED]) == 1


def test_run_leaves_an_in_flight_group_open(svc, monkeypatch):
    """A batch still running when the pod restarted must come back OPEN, not completed —
    its remaining members are still coming through the live consumer."""
    s, store, producer, cfg = svc
    gid, _ = s.create(label="batch", expected=3)
    corrs = s.submit_members(gid, ["a", "b", "c"])

    fresh_store = Store(pathlib.Path(cfg.tmpdir) / "eb-inflight")
    fresh_prod = FakeProducer()
    restarted = GroupService(cfg, fresh_store, fresh_prod, SeqMinter())
    started = {"type": ce.TYPE_GROUP_STARTED, "source": "/eb", "groupid": gid,
               "data": {"label": "batch", "expected": 3}}
    only_one_done = _record({"type": ce.TYPE_RESPONSE, "source": "/er", "groupid": gid,
                             "correlationid": corrs[0], "sequence": 3, "phase": "result",
                             "final": True, "data": {"role": "final"}}, 0)
    m, _fake = _mirror_over({0: [_record(started, 0)], 1: [only_one_done]},
                            restarted, monkeypatch)
    g = fresh_store.get_group(gid)
    assert g["completed_utc"] is None
    assert fresh_store.group_counts(gid)["finished"] == 1
    assert m.settled == 0
    assert fresh_prod.group_events == []


def test_run_survives_an_undecodable_record(svc, monkeypatch):
    """One corrupt record must not cost the whole replay."""
    _s, _store, _producer, cfg = svc
    fresh_store = Store(pathlib.Path(cfg.tmpdir) / "eb-corrupt")
    fresh = GroupService(cfg, fresh_store, FakeProducer(), SeqMinter())
    started = {"type": ce.TYPE_GROUP_STARTED, "source": "/eb", "groupid": "cool-ox-1234",
               "data": {"label": "l", "expected": 1}}
    junk = _FakeRec([("ce_id", b"x")], b"not a cloudevent", 0)
    m, _fake = _mirror_over({0: [junk, _record(started, 1)]}, fresh, monkeypatch)
    assert m.rebuilt_groups == 1
    assert fresh_store.get_group("cool-ox-1234") is not None


def test_run_ignores_events_with_no_groupid(svc, monkeypatch):
    """The responses topic is mostly ungrouped traffic; it must cost nothing here."""
    _s, _store, _producer, cfg = svc
    fresh = GroupService(cfg, Store(pathlib.Path(cfg.tmpdir) / "eb-ungrouped"),
                         FakeProducer(), SeqMinter())
    plain = _record({"type": ce.TYPE_RESPONSE, "source": "/er",
                     "correlationid": "lone-yak-1111", "sequence": 1,
                     "phase": "result", "final": True, "data": {"role": "final"}}, 0)
    m, _fake = _mirror_over({0: [plain]}, fresh, monkeypatch)
    assert m.rebuilt_groups == 0 and m.rebuilt_members == 0


def test_run_does_not_hang_when_a_partition_never_delivers(svc, monkeypatch):
    """End offsets say there is more, but nothing arrives (a deleted segment, an aborted
    transaction). The scan must give up, not block EventBridge's startup forever."""
    _s, _store, _producer, cfg = svc
    fresh = GroupService(cfg, Store(pathlib.Path(cfg.tmpdir) / "eb-gap"),
                         FakeProducer(), SeqMinter())
    from eventbridge import kafka_group_mirror as mod

    class _NeverDelivers(_FakeConsumer):
        def poll(self, timeout_ms=None):  # noqa: ARG002
            return {}

    fake = _NeverDelivers({0: [_FakeRec([], b"", 0)]})
    monkeypatch.setattr(mod, "KafkaConsumer", lambda **_: fake)
    m = mod.GroupMirror("broker:9092", "responses", fresh, max_empty_polls=2)
    m.run()
    assert fake.closed


def test_a_broker_that_is_down_does_not_block_startup(svc, monkeypatch):
    """EventBridge must serve HTTP even with no Kafka; the replay is best-effort."""
    _s, _store, _producer, cfg = svc
    fresh = GroupService(cfg, Store(pathlib.Path(cfg.tmpdir) / "eb-nobroker"),
                         FakeProducer(), SeqMinter())
    from eventbridge import kafka_group_mirror as mod

    def _boom(**_):
        raise OSError("no brokers")

    monkeypatch.setattr(mod, "KafkaConsumer", _boom)
    mod.GroupMirror("broker:9092", "responses", fresh).run()   # must not raise
