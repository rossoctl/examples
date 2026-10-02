"""Emit response CloudEvents on the responses topic. Single Producer instance."""
from __future__ import annotations

import threading
from typing import Any

from kafka import KafkaProducer

from shared import ce, signing


class Emitter:
    """§11: signs **terminal events only**, and what that does and does not prove.

    `emit()` runs for every `stdout` frame an agent produces, and the pure-Python
    Ed25519 here costs ~150-200 ms per signature, so signing every frame would add
    minutes to a chatty run. Terminal events are one per run, where the cost is
    invisible against an agent that already took seconds.

    The honest consequence: a signature on the terminal event proves **who finished a
    run**, not **what the run said along the way**. Anything with write access to the
    responses topic can still forge `final=false` frames for a live correlation, and
    they will render in the transcript. Closing that needs either cheap signatures or
    a signed digest chain across frames; neither is in scope here, and claiming
    otherwise while demoing would be wrong.

    The seed lives on the instance, so none of the seven `emit()` call sites in
    `runner.py` know signing exists.
    """

    def __init__(self, bootstrap: str, response_topic: str, source_uri: str,
                 seed: bytes | None = None, kid: str | None = None,
                 userkey: str | None = None) -> None:
        self._prod = KafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=5)
        self._topic = response_topic
        self._source = source_uri
        self._seed = seed
        self._kid = kid
        # Phase 3 §3.3: stamped on every response so EventBridge knows which store to
        # file it in. Held here rather than threaded through `emit`'s callers because
        # it is a property of the *runner* — this pod serves exactly one tenant — and a
        # per-call parameter would be one more thing a call site could forget, with a
        # misfiled response as the symptom. Empty in single-tenant mode, where the
        # attribute is simply absent and behaviour is Phase 2's.
        self._userkey = userkey or None
        self._seq_lock = threading.Lock()
        self._seq_by_corr: dict[str, int] = {}

    def next_seq(self, corr: str, start: int | None = None) -> int:
        with self._seq_lock:
            if corr not in self._seq_by_corr and start is not None:
                self._seq_by_corr[corr] = start - 1
            self._seq_by_corr[corr] = self._seq_by_corr.get(corr, 0) + 1
            return self._seq_by_corr[corr]

    def seed_seq(self, corr: str, last: int) -> None:
        with self._seq_lock:
            self._seq_by_corr[corr] = max(last, self._seq_by_corr.get(corr, 0))

    def emit(self, *, correlationid: str, sessionuuid: str, sequence: int,
             phase: str, final: bool, data: Any,
             causationid: str | None = None, groupid: str | None = None) -> str:
        """Publish one response event.

        `causationid` (DESIGN_PHASE1.md §11) is the `id` of the request event that
        caused this one. Without it the only link is `correlationid`, which
        identifies a *conversation*, not a *turn* — so responses from a
        `/continue` turn could not be attributed to their specific triggering
        request, which is what makes a signed audit trail meaningful.
        """
        attrs: dict[str, Any] = {}
        if causationid:
            attrs["causationid"] = causationid
        if groupid:
            attrs["groupid"] = groupid
        if self._userkey:
            attrs[ce.EXT_USERKEY] = self._userkey
        event = ce.new_event(
            type=ce.TYPE_RESPONSE,
            source=self._source,
            datacontenttype="application/json",
            correlationid=correlationid,
            sessionuuid=sessionuuid,
            sequence=sequence,
            phase=phase,
            final="true" if final else "false",
            data=data,
            **attrs,
        )
        # Terminal events only — see the class docstring for the cost and the limit
        # that buys. `final` is already the parameter, so no call site changes.
        if final:
            signing.sign_into(event, self._seed, self._kid)
        headers, value = ce.to_kafka_binary(event)
        self._prod.send(self._topic,
                        key=correlationid.encode(),
                        value=value,
                        headers=headers).get(timeout=5)
        return event["id"]

    def close(self) -> None:
        try:
            self._prod.flush(timeout=2)
        finally:
            self._prod.close(timeout=2)
