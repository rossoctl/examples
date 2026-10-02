"""Group lifecycle: create, fan out, attribute member events, complete once. §21.

Sits between the HTTP handlers and the store so the completion rule lives in exactly
one place. Everything that could fire the completion transition — a member's terminal
event, an explicit close, the deadline sweep — goes through `maybe_complete()`.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

from eventbridge.groups import completion_reason, progress
from shared import ce


def _log(msg: str) -> None:
    print(f"[groups] {msg}", flush=True)


class GroupService:
    def __init__(self, cfg, store, producer, minter) -> None:
        self.cfg = cfg
        self.store = store
        self.producer = producer
        self.minter = minter

    # ---- creation and fan-out ---------------------------------------------
    def create(self, *, label: str | None, expected: int | None,
               min_success: int | None = None, deadline_s: float | None = None,
               idempotency_key: str | None = None) -> tuple[str, bool]:
        groupid = self.minter.mint()
        deadline = None
        if deadline_s:
            deadline = (dt.datetime.now(dt.timezone.utc)
                        + dt.timedelta(seconds=deadline_s)).isoformat(
                            timespec="milliseconds").replace("+00:00", "Z")
        out = self.store.create_group(groupid, label=label, expected=expected,
                                      min_success=min_success, deadline_utc=deadline,
                                      idempotency_key=idempotency_key)
        if not out["created"]:
            # An idempotency-key hit: the caller is retrying, not asking for a second
            # batch. Give back the original so a retry cannot double-launch 100 agents.
            return out["groupid"], False
        self._publish_started(out["groupid"], label, expected, min_success, deadline)
        return out["groupid"], True

    def _publish_started(self, groupid, label, expected, min_success, deadline) -> None:
        try:
            self.producer.publish_group_event(
                type_=ce.TYPE_GROUP_STARTED, groupid=groupid, subject="group-started",
                data={"label": label, "expected": expected, "min_success": min_success,
                      "deadline_utc": deadline})
        except Exception as e:  # noqa: BLE001 - the group still exists in the store
            _log(f"could not publish group.started for {groupid}: {e!r}")

    def submit_members(self, groupid: str, prompts: list[str], *,
                       max_turns: int = 3, model: str | None = None,
                       submitter: str | None = None,
                       submitter_iss: str | None = None,
                       userkey: str | None = None,
                       agent: str | None = None) -> list[str]:
        """Publish every member's request, recording membership first.

        Membership is recorded BEFORE the request is published so a fast agent's
        response event can always be attributed; the reverse order leaves a window in
        which a member's terminal event arrives for an unknown member.
        """
        corrs: list[str] = []
        for prompt in prompts:
            corr = self.minter.mint()
            sess = ce.session_uuid(corr)
            workdir = f"{self.cfg.tmpdir}/eventrunner/work/{corr}"
            self.store.upsert_session(corr, sess, workdir, prompt)
            self.store.insert_prompt(corr, "start", prompt, submitter=submitter)
            self.store.add_group_member(groupid, corr)
            self.producer.publish_request(
                prompt=prompt, correlationid=corr, sessionuuid=sess, mode="start",
                model=model, max_turns=max_turns, subject="start", groupid=groupid,
                submitter=submitter, submitter_iss=submitter_iss,
                userkey=userkey, agent=agent)
            corrs.append(corr)
        return corrs

    # ---- member events ----------------------------------------------------
    def on_member_event(self, event: dict[str, Any], *, replay: bool = False) -> None:
        """Attribute one response event to its group and maybe complete the group.

        `replay=True` (the startup mirror) updates the store but never completes the
        group, so re-reading a batch that finished hours ago cannot re-publish its
        completion event or re-send its notification. The mirror settles genuinely
        unfinished groups itself, once, after the scan.
        """
        groupid = event.get("groupid")
        corr = event.get("correlationid")
        if not groupid or not corr:
            return
        # Accepted even if the group row does not exist yet: a restart re-scans the
        # topic and a fast agent can finish before its group's started event lands.
        self.store.add_group_member(groupid, corr)
        final = str(event.get("final", "false")).lower() == "true"
        if not final:
            self.store.mark_member_running(groupid, corr)
            return
        first = self.store.mark_member_finished(groupid, corr,
                                               event.get("phase") or "result")
        if replay:
            return
        if not first:
            # A redelivered terminal event. Counting it again is exactly the bug §21.5
            # exists to prevent, so say so once and stop.
            _log(f"duplicate terminal event for {groupid}/{corr} ignored")
            return
        self.maybe_complete(groupid)

    def on_group_event(self, event: dict[str, Any], *, replay: bool = False) -> None:
        """Reconcile a group lifecycle event read back off the topic.

        The topic is the audit trail, so a `started` event we did not originate — or one
        replayed after a restart — recreates the group row rather than being dropped.
        A replayed `completed` event marks the group complete without re-publishing
        anything, which is what lets the mirror restore a finished batch silently.
        """
        groupid = event.get("groupid")
        if not groupid:
            return
        data = event.get("data") or {}
        if event.get("type") == ce.TYPE_GROUP_STARTED:
            self.store.create_group(
                groupid, label=data.get("label"), expected=data.get("expected"),
                min_success=data.get("min_success"),
                deadline_utc=data.get("deadline_utc"))
        elif event.get("type") == ce.TYPE_GROUP_COMPLETED:
            # Marks completed_utc via the same guarded UPDATE, so a later member replay
            # cannot decide the group still needs completing.
            self.store.complete_group(groupid, data.get("reason") or "all")

    # ---- completion -------------------------------------------------------
    def maybe_complete(self, groupid: str) -> bool:
        group = self.store.get_group(groupid)
        if group is None or group.get("completed_utc"):
            return False
        counts = self.store.group_counts(groupid)
        reason = completion_reason(group, counts)
        if reason is None:
            return False
        # The guard is the UPDATE, not this check: two threads can both get here.
        if not self.store.complete_group(groupid, reason):
            return False
        group = self.store.get_group(groupid)
        members = self.store.group_members(groupid)
        p = progress(group, members)
        _log(f"group {groupid} complete ({reason}): "
             f"{p['finished']} finished, {p['failed']} failed, {p['elapsed']}")
        try:
            self.producer.publish_group_event(
                type_=ce.TYPE_GROUP_COMPLETED, groupid=groupid,
                subject="group-completed",
                data={"label": group.get("label"), "reason": reason,
                      "finished": p["finished"], "failed": p["failed"],
                      "expected": p["expected"], "members": p["members_seen"],
                      "elapsed_s": p["elapsed_s"], "elapsed": p["elapsed"],
                      "slowest_member_s": _slowest(members)})
        except Exception as e:  # noqa: BLE001
            _log(f"could not publish group.completed for {groupid}: {e!r}")
        return True

    def close(self, groupid: str, expected: int | None = None) -> bool:
        ok = self.store.close_group(groupid, expected)
        self.maybe_complete(groupid)
        return ok

    def cancel(self, groupid: str) -> bool:
        """Stop queued members. Running agents are NOT stopped — see §21.9.5.

        Interrupting a live `claude` needs a mechanism EventRunner honours, which
        belongs with the Job-per-request work. Saying so plainly beats implying an
        abort that does not happen.
        """
        ok = self.store.cancel_group(groupid)
        self.maybe_complete(groupid)
        return ok

    def sweep_deadlines(self) -> int:
        """Complete groups whose deadline passed. Without this a group that is short
        one member never completes and therefore never notifies (§21.6)."""
        n = 0
        for groupid in self.store.groups_past_deadline():
            if self.maybe_complete(groupid):
                n += 1
        return n

    # ---- reads ------------------------------------------------------------
    def snapshot(self, groupid: str) -> dict[str, Any] | None:
        group = self.store.get_group(groupid)
        if group is None:
            return None
        members = self.store.group_members(groupid)
        p = progress(group, members)
        p["members"] = members
        return p


def _slowest(members: list[dict[str, Any]]) -> float | None:
    """Straggler latency: the slowest member sets the group's duration (§21.9.7)."""
    import datetime as _dt

    def _p(ts):
        if not ts:
            return None
        try:
            v = _dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return v if v.tzinfo else v.replace(tzinfo=_dt.timezone.utc)
        except (ValueError, TypeError):
            return None

    spans = []
    for m in members:
        a, b = _p(m.get("submitted_utc")), _p(m.get("finished_utc"))
        if a and b:
            spans.append((b - a).total_seconds())
    return round(max(spans), 1) if spans else None
