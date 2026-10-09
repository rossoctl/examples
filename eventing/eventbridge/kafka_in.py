"""KafkaConsumer thread — polls responses, writes SQLite, fans out to ntfy.

§11: this is where a response is checked against the approved-key set. Anything with
write access to the responses topic otherwise gets its output stored, rendered in the
transcript and pushed to the operator's phone **as a legitimate agent answer**.

A rejected event is stored as `phase="error"` rather than dropped. That is deliberate:
dropping it silently is indistinguishable from an agent that never answered, while
`phase="error"` reuses machinery already wired — a red card in the HTML transcript and
an ntfy priority-5 alert — so the forgery attempt is visible instead of invisible.

**What "reviewable" requires.** `Store.insert_response` derives both `data_json` and
`raw_json` from the single dict it is handed, so rewriting the envelope in place would
overwrite the evidence with the notice about it: the refused payload, the phase it
claimed, and the attributes the signature covered would all be gone, leaving a
signature that can no longer be checked against anything. The original is therefore
preserved under `data["rejected"]` — attrs, payload, claimed source and signature — so
an incident can be verified offline rather than merely logged.

**Stored is not the same as acted on (#885).** Marking a refused event was originally the
whole of enforcement, and it was not enough: the rewrite preserves `final` and `groupid`
so the record stays readable, which meant a rejected event went on finishing its member,
settling its batch, and overwriting the verified row at its sequence. With enforcement
on, a forged terminal could finish a batch as `failed: 1` and the genuine answer that
followed was discarded as a duplicate — so the forgery did not merely add a lie, it
destroyed the truth. A rejected envelope is therefore marked with `ce.KEY_REJECTED` and
reaches no acting path: not `on_group_event`, not `on_member_event`, and not a sequence
a non-rejected row already holds.

**The guard reads `ok`, never `verified`, and that is load-bearing.** An unsigned
non-terminal frame and a deployment with no keyset both report `verified=False` while
being accepted *by policy* — `emit()` signs terminal events only, because a signature
costs ~222 ms and signing every stdout frame would add minutes to a chatty run. Keying
the guard on `verified` would stop every streamed frame of every genuine run from
acting, which is the §4.4 step 2 mistake: it rewrote every frame of every run to
`phase="error"` and was found against a live broker, by no unit test.
"""
from __future__ import annotations

import threading
from typing import Callable

from kafka import KafkaConsumer

from eventbridge.store import Store
from shared import ce, signing, tenancy


class Consumer(threading.Thread):
    def __init__(
        self,
        bootstrap: str,
        response_topic: str,
        store: Store,
        on_event: Callable[[dict], None] | None = None,
        group_id: str = "eventbridge-responses",
        on_group_event: Callable[[dict], None] | None = None,
        on_member_event: Callable[[dict], None] | None = None,
        keyset=None,
        require_signature: bool = False,
        bridge_kid: str | None = None,
        topics: tenancy.TopicSet | None = None,
        stores=None,
    ) -> None:
        super().__init__(daemon=True, name="kafka-responses-consumer")
        self._bootstrap_servers = bootstrap
        # §3.2: the chosen option is an EXPLICIT topic list, re-subscribed when the
        # registry changes — not `subscribe(pattern=...)`. A pattern is picked up by
        # metadata refresh (`metadata.max.age.ms`, 5 minutes by default), so a new
        # user's first response can be published before anyone is subscribed, and with
        # `auto_offset_reset=latest` it is simply never read: the page stays empty, the
        # group counter never advances, and no error appears anywhere.
        #
        # In single mode the list is the one configured topic, which is today's
        # behaviour. The blocking `ensure_subscribed` that makes multi mode safe is T6.
        self._topics = topics or tenancy.TopicSet(
            "", response_topic=response_topic, request_topic="")
        self._topic = self._topics.responses() if not self._topics.multi else response_topic
        self._store = store
        # §6.1 — when set, each response is filed in the store its own `ce_userkey`
        # names. `None` keeps the single-store behaviour, which is single-tenant mode.
        self._stores = stores
        self._on_event = on_event
        self._on_group_event = on_group_event
        self._on_member_event = on_member_event
        self._group = group_id
        # §11. `keyset=None` means verification is off, which is the default and
        # exactly today's behaviour. `bridge_kid` pins group lifecycle events to
        # EventBridge's own key, so an approved runner cannot forge a group.completed.
        self._keyset = keyset
        self._require_sig = require_signature
        self._bridge_kid = bridge_kid
        self._rejected = 0
        self._audit_failed = 0
        self._rejected_not_stored = 0
        self._stopping = threading.Event()

    @property
    def rejected(self) -> int:
        """Responses stored as phase=error because they did not verify."""
        return self._rejected

    @property
    def rejected_not_stored(self) -> int:
        """Rejected responses refused even the `phase=error` row, because a verified
        row already held their (correlationid, sequence). #885 item 3.

        Separate from `rejected` because it is a different operational fact: the event
        was refused AND it collided with a genuine answer, which is what an attempt to
        overwrite a verified result looks like from the store's side.
        """
        return self._rejected_not_stored

    @property
    def audit_failed(self) -> int:
        """Responses accepted unchanged that enforcement *would* have rejected.

        This is the reject rate the two-flag rollout asks an operator to watch: with a
        keyset configured and `EB_REQUIRE_RESPONSE_SIGNATURE=false`, it counts what
        turning enforcement on would start refusing. Stays 0 once enforcement is on,
        because then those events land in `rejected` instead.
        """
        return self._audit_failed

    def stop(self) -> None:
        self._stopping.set()

    def run(self) -> None:
        c = KafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=500,
        )
        try:
            while not self._stopping.is_set():
                for rec in c:
                    if self._stopping.is_set():
                        break
                    evt = ce.from_kafka_binary(rec.headers or [], rec.value)
                    # §11: verify while the CloudEvent is still in hand — the check
                    # needs `.attrs`/`.data`, which envelope_dict has already flattened.
                    #
                    # The try is not belt-and-braces. `from_kafka_binary` above and
                    # `insert_response` below are NOT inside one, so a raise anywhere in
                    # here ends the for, ends the while, and the thread is gone — while
                    # the process stays up and the pod still reports healthy. The
                    # verification path must degrade, never raise.
                    ok, why, verified = True, "not checked", True
                    if self._keyset is not None:
                        try:
                            ok, why, verified = signing.response_decision(
                                evt, self._keyset, require=self._require_sig,
                                bridge_kid=self._bridge_kid)
                        except Exception as e:  # noqa: BLE001
                            # Fail closed only where enforcement is on: if the verifier
                            # itself is broken, an unverifiable event is not evidence of
                            # anything, and silently accepting it defeats the control.
                            ok, why = (not self._require_sig), f"verifier raised: {e!r}"
                            verified = False
                            print(f"[kafka_in] verification error: {e!r}")
                    d = ce.envelope_dict(evt)
                    if ok and not verified:
                        # Audit mode: the event is stored unchanged, and this line plus
                        # the counter are the only trace that enforcement would have
                        # refused it. §4.4's two-flag rollout ("a keyset alone verifies
                        # and logs") is unperformable without them -- the verdict was
                        # computed and the reason discarded, so there was no reject rate
                        # to watch before turning enforcement on.
                        self._audit_failed += 1
                        print(f"[kafka_in] audit: would reject response on "
                              f"{d.get('correlationid') or d.get('groupid')}: {why}")
                    if not ok:
                        self._rejected += 1
                        print(f"[kafka_in] unverified response on "
                              f"{d.get('correlationid') or d.get('groupid')}: {why}")
                        # `text` is load-bearing: ntfy reads data["text"] for the error
                        # body, so anything else shows up on the phone as
                        # "(error, see raw)". str() because insert_response json.dumps
                        # this dict outside any try — a non-serialisable reason would
                        # kill the thread by a second route.
                        #
                        # `rejected` carries the event as it actually arrived.
                        # `insert_response` derives BOTH data_json and raw_json from
                        # this one dict, so overwriting `phase`/`data` in place would
                        # destroy the forensic record while the docstring above still
                        # promised it — leaving a signature whose covered attributes no
                        # longer exist, and nothing for an operator to review.
                        #
                        # `ce.KEY_REJECTED` is the verdict itself, carried at the top
                        # level because that is the one thing every acting path receives.
                        # It is what `insert_response` and the group service read; the
                        # `data` keys below are for a human reading the transcript, and
                        # keying a control on them would be keying it on `data`, which
                        # arrives off the wire intact. See `ce.KEY_REJECTED`.
                        d = dict(d, phase="error", data={
                            "text": f"unverified response rejected: {why}",
                            "signature_rejected": True,
                            "reason": str(why),
                            "rejected": {"phase": evt.get("phase"),
                                         "final": evt.get("final"),
                                         "source": evt.get("source"),
                                         "signature": evt.get("signature"),
                                         "attrs": dict(evt.attrs),
                                         "data": evt.data},
                        }, **{ce.KEY_REJECTED: True})
                    # §21.2: route on type. A group lifecycle event carries `groupid`
                    # but no `correlationid`, so handing it to insert_response would
                    # violate that table's (correlationid, sequence) primary key.
                    if ce.is_group_event(evt):
                        # #885 — `ok`, not `verified`. An unsigned non-terminal frame and
                        # a deployment with no keyset both report `verified=False` while
                        # being accepted BY POLICY, and skipping those would stop every
                        # streamed frame of every genuine run from reaching the group
                        # service. Only `not ok` is the enforcement-refused population.
                        #
                        # The group service refuses a marked envelope too (§885's single
                        # rule), so this is the source-side half of a rule enforced at
                        # both ends rather than the only guard.
                        if self._on_group_event and ok:
                            try: self._on_group_event(d)
                            except Exception as e: print(f"[kafka_in] group event: {e!r}")
                    else:
                        # §6.1: routed by the event's OWN userkey, which is signed
                        # (§2.6) so it cannot be rewritten in flight. An event with no
                        # userkey in multi-tenant mode lands in `shared/` and bumps the
                        # `unattributed` counter — never guessed into a tenant's store
                        # (that corrupts somebody's history) and never dropped (that is
                        # indistinguishable from an agent that never answered).
                        target = (self._stores.for_event(evt) if self._stores
                                  else self._store)
                        if not target.insert_response(d):
                            # #885 item 3 — the row at this sequence was NOT a rejection,
                            # so a rejected frame was refused the overwrite. Counted and
                            # logged rather than dropped silently: §4.4's rule is that a
                            # drop is indistinguishable from an agent that never
                            # answered. The verified row it failed to replace is the
                            # better record of what happened at this sequence.
                            self._rejected_not_stored += 1
                            print(f"[kafka_in] rejected response for "
                                  f"{d.get('correlationid')} seq {d.get('sequence')} "
                                  f"not stored: a verified row holds that sequence")
                        if self._on_member_event and d.get("groupid") and ok:
                            try: self._on_member_event(d)
                            except Exception as e: print(f"[kafka_in] member event: {e!r}")
                    if self._on_event:
                        try: self._on_event(d)
                        except Exception: pass
        finally:
            c.close()
