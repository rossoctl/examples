"""KafkaConsumer that mirrors every request CloudEvent's prompt into
`prompts.sqlite`. This is the safety net that back-fills prompts for
correlations created before the prompts table existed, and also decouples
prompt persistence from EventBridge's HTTP path — if someone POSTs to
Kafka directly (or a Phase 1 signed-event producer does), we still see
the prompt.

The mirror reads from the requests topic with a dedicated consumer group
(`eventbridge-requests-mirror`) starting at `earliest` so it can pick up
whatever Kafka still retains. It uses `Store.backfill_prompt_if_missing`
so re-runs are idempotent — the same (correlationid, mode, prompt) is
never inserted twice.
"""
from __future__ import annotations

import json
import os
import threading

from kafka import KafkaConsumer

from eventbridge.store import Store
from shared import ce, tenancy


class RequestsMirror(threading.Thread):
    def __init__(self, bootstrap: str, request_topic: str, store: Store,
                 group_id: str | None = None,
                 topics: tenancy.TopicSet | None = None,
                 stores=None) -> None:
        super().__init__(daemon=True, name="kafka-requests-mirror")
        self._bootstrap_servers = bootstrap
        # §3.2: "RequestsMirror needs the same treatment" as the responses consumer —
        # it is what back-fills prompts for correlations the bridge did not originate,
        # so a topic it is not subscribed to is a correlation with no prompt. The
        # per-user subscription management is T6; this threads the set through.
        self._topics = topics or tenancy.TopicSet(
            "", request_topic=request_topic, response_topic="")
        self._topic = self._topics.requests() if not self._topics.multi else request_topic
        self._store = store
        # §6.1 — route each back-filled prompt into the store its own `ce_userkey` names.
        # Without this the mirror knows the per-tenant TOPIC names and still writes every
        # prompt into the shared store, so a correlation whose request the bridge did not
        # originate shows a BLANK PROMPT on its owner's page — which is precisely the gap
        # this mirror exists to close.
        self._stores = stores
        # Per-process group id so every EB start re-scans the whole topic.
        # `backfill_prompt_if_missing()` makes re-inserts a no-op, so this
        # is idempotent and cheap. A fixed group id would commit offsets
        # once and then never re-see the old events on the next start.
        self._group = group_id or f"eventbridge-requests-mirror-{os.getpid()}"
        self._stopping = threading.Event()

    def stop(self) -> None:
        self._stopping.set()

    def run(self) -> None:
        c = KafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,      # don't persist offsets — we re-scan every start
            consumer_timeout_ms=500,
        )
        try:
            while not self._stopping.is_set():
                for rec in c:
                    if self._stopping.is_set():
                        break
                    try:
                        evt = ce.from_kafka_binary(rec.headers or [], rec.value)
                    except Exception as e:  # noqa: BLE001
                        print(f"[requests-mirror] parse fail at offset={rec.offset}: {e}")
                        continue
                    corr = evt.attrs.get("correlationid")
                    if not corr:
                        continue
                    mode = evt.attrs.get("mode", "start") or "start"
                    data = evt.data
                    if isinstance(data, (bytes, bytearray)):
                        try: data = json.loads(data.decode())
                        except Exception: data = None
                    if isinstance(data, str):
                        try: data = json.loads(data)
                        except Exception: data = None
                    prompt = (data.get("prompt") if isinstance(data, dict) else None)
                    if not prompt:
                        continue
                    submitted = evt.attrs.get("time")
                    target = (self._stores.for_event(evt) if self._stores
                              else self._store)
                    target.backfill_prompt_if_missing(
                        correlationid=corr, mode=mode, prompt=prompt,
                        submitted_utc=submitted,
                    )
        finally:
            c.close()
