"""Group lifecycle: create, fan out, attribute member events, complete once. §21.

Sits between the HTTP handlers and the store so the completion rule lives in exactly
one place. Everything that could fire the completion transition — a member's terminal
event, an explicit close, the deadline sweep — goes through `maybe_complete()`.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

from eventbridge.correlation import mint_for
from eventbridge.groups import completion_reason, progress
from shared import ce


def _log(msg: str) -> None:
    print(f"[groups] {msg}", flush=True)


class GroupService:
    """Group lifecycle, between the HTTP handlers and the store.

    Phase 3 §6.1: `stores` is the `StoreRegistry` when per-user stores are in use. The
    single `store` stays the default and is what every single-tenant deployment keeps
    using, so `self.store` remains correct wherever a tenant is not in question —
    `on_member_event` and the deadline sweeper both route by the event's own `userkey`.
    """

    def __init__(self, cfg, store, producer, minter, stores=None) -> None:
        self.cfg = cfg
        self.store = store
        self.producer = producer
        self.minter = minter
        self.stores = stores

    def _store_for(self, userkey: str | None):
        """The store one tenant's group data lives in.

        Falls back to the single store whenever there is no registry, which is both
        single-tenant mode and every existing test — so this method is a no-op there
        rather than a branch each caller has to remember.
        """
        if self.stores is None:
            return self.store
        return self.stores.for_userkey(userkey)

    # ---- creation and fan-out ---------------------------------------------
    def create(self, *, label: str | None, expected: int | None,
               min_success: int | None = None, deadline_s: float | None = None,
               idempotency_key: str | None = None,
               userkey: str | None = None) -> tuple[str, bool]:
        # A group id comes from the same generator as a correlation id, so it is claimed
        # in the same index: the two namespaces are separate in the URL space but share
        # one id space, and letting a group id collide with a correlation id would make
        # `owner_of` ambiguous for whichever arrived second.
        groupid = mint_for(self.minter, userkey)
        deadline = None
        if deadline_s:
            deadline = (dt.datetime.now(dt.timezone.utc)
                        + dt.timedelta(seconds=deadline_s)).isoformat(
                            timespec="milliseconds").replace("+00:00", "Z")
        out = self._store_for(userkey).create_group(
            groupid, label=label, expected=expected,
            min_success=min_success, deadline_utc=deadline,
            idempotency_key=idempotency_key)
        if not out["created"]:
            # An idempotency-key hit: the caller is retrying, not asking for a second
            # batch. Give back the original so a retry cannot double-launch 100 agents.
            return out["groupid"], False
        self._publish_started(out["groupid"], label, expected, min_success, deadline,
                              userkey=userkey)
        return out["groupid"], True

    def _publish_started(self, groupid, label, expected, min_success, deadline,
                         userkey: str | None = None) -> None:
        try:
            self.producer.publish_group_event(
                type_=ce.TYPE_GROUP_STARTED, groupid=groupid, subject="group-started",
                data={"label": label, "expected": expected, "min_success": min_success,
                      "deadline_utc": deadline},
                userkey=userkey)
        except (TypeError, ValueError):
            # NOT swallowed. Both are programming errors rather than a broker being down,
            # and the broad `except` below would turn either into a group that silently
            # never announces itself:
            #
            #  * TypeError  — a signature mismatch between this call and the producer.
            #    Phase 3 hit exactly that while adding `userkey`: every group event
            #    stopped publishing and the only symptom was a batch that never completed.
            #  * ValueError — `TopicSet` refusing to name a topic without a userkey in
            #    multi mode. That means a caller failed to pass one, which is the
            #    `create_group` defect the #883 review found; logging it would hide the
            #    cause and leave the same silent never-completing batch.
            #
            # Loud is correct for both. A genuine broker failure is still handled below.
            raise
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
        # Resolved once outside the loop: every member of a batch belongs to the caller
        # who launched it, so re-resolving per member would be N LRU touches for one
        # answer — and at 100 members that is 100 chances to evict something else.
        store = self._store_for(userkey)
        for prompt in prompts:
            corr = mint_for(self.minter, userkey)
            sess = ce.session_uuid(corr)
            workdir = f"{self.cfg.tmpdir}/eventrunner/work/{corr}"
            store.upsert_session(corr, sess, workdir, prompt)
            store.insert_prompt(corr, "start", prompt, submitter=submitter)
            store.add_group_member(groupid, corr)
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
        # §6.1: the store is chosen by the event's own `userkey`, which is signed (§2.6)
        # and therefore not rewritable in flight. Routing by the event rather than by any
        # ambient state is what keeps one tenant's batch progress out of another's store.
        userkey = event.get(ce.EXT_USERKEY)
        store = self._store_for(userkey)
        # Accepted even if the group row does not exist yet: a restart re-scans the
        # topic and a fast agent can finish before its group's started event lands.
        store.add_group_member(groupid, corr)
        final = str(event.get("final", "false")).lower() == "true"
        if not final:
            store.mark_member_running(groupid, corr)
            return
        first = store.mark_member_finished(groupid, corr,
                                           event.get("phase") or "result")
        if replay:
            return
        if not first:
            # A redelivered terminal event. Counting it again is exactly the bug §21.5
            # exists to prevent, so say so once and stop.
            _log(f"duplicate terminal event for {groupid}/{corr} ignored")
            return
        self.maybe_complete(groupid, userkey=userkey)

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
        store = self._store_for(event.get(ce.EXT_USERKEY))
        if event.get("type") == ce.TYPE_GROUP_STARTED:
            store.create_group(
                groupid, label=data.get("label"), expected=data.get("expected"),
                min_success=data.get("min_success"),
                deadline_utc=data.get("deadline_utc"))
        elif event.get("type") == ce.TYPE_GROUP_COMPLETED:
            # Marks completed_utc via the same guarded UPDATE, so a later member replay
            # cannot decide the group still needs completing.
            store.complete_group(groupid, data.get("reason") or "all")

    # ---- completion -------------------------------------------------------
    def maybe_complete(self, groupid: str, *, userkey: str | None = None) -> bool:
        store = self._store_for(userkey)
        group = store.get_group(groupid)
        if group is None or group.get("completed_utc"):
            return False
        counts = store.group_counts(groupid)
        reason = completion_reason(group, counts)
        if reason is None:
            return False
        # The guard is the UPDATE, not this check: two threads can both get here.
        if not store.complete_group(groupid, reason):
            return False
        group = store.get_group(groupid)
        members = store.group_members(groupid)
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
                      "slowest_member_s": _slowest(members)},
                userkey=userkey)
        except (TypeError, ValueError):
            # See `_publish_started`: a signature mismatch or a missing userkey is a
            # programming error and must not be downgraded into "this batch never
            # announced it finished". The guard is repeated at both call sites rather than
            # factored into a helper, because what it protects is the argument list of
            # *this* call, and a wrapper would move the mismatch one frame away from the
            # arguments that caused it.
            raise
        except Exception as e:  # noqa: BLE001
            _log(f"could not publish group.completed for {groupid}: {e!r}")
        return True

    def close(self, groupid: str, expected: int | None = None, *,
              userkey: str | None = None) -> bool:
        ok = self._store_for(userkey).close_group(groupid, expected)
        self.maybe_complete(groupid, userkey=userkey)
        return ok

    def cancel(self, groupid: str, *, userkey: str | None = None) -> bool:
        """Stop queued members. Running agents are NOT stopped — see §21.9.5.

        Interrupting a live `claude` needs a mechanism EventRunner honours, which
        belongs with the Job-per-request work. Saying so plainly beats implying an
        abort that does not happen.
        """
        ok = self._store_for(userkey).cancel_group(groupid)
        self.maybe_complete(groupid, userkey=userkey)
        return ok

    def sweep_deadlines(self) -> int:
        """Complete groups whose deadline passed. Without this a group that is short
        one member never completes and therefore never notifies (§21.6).

        With per-user stores this has to sweep **every** tenant, not the one store: a
        stalled batch belonging to a tenant nobody has touched recently is exactly the
        case the deadline exists for, and it is also the tenant least likely to be in the
        LRU. Sweeping only the default store would silently stop honouring deadlines for
        everyone else — the failure mode being silence, which §21.6 calls the worst one.
        """
        n = 0
        for userkey in self._tenants_to_sweep():
            for groupid in self._store_for(userkey).groups_past_deadline():
                if self.maybe_complete(groupid, userkey=userkey):
                    n += 1
        return n

    def _tenants_to_sweep(self) -> list[str | None]:
        """Every tenant whose groups the sweeper must look at.

        `None` is always included: it is the single store in single-tenant mode and the
        `shared` tier in multi-tenant mode, and in both cases it can hold groups.
        """
        if self.stores is None:
            return [None]
        return [None, *self.stores.known_userkeys()]

    # ---- reads ------------------------------------------------------------
    def snapshot(self, groupid: str, *, userkey: str | None = None) -> dict[str, Any] | None:
        store = self._store_for(userkey)
        group = store.get_group(groupid)
        if group is None:
            return None
        members = store.group_members(groupid)
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
