"""ntfy fan-out — POST JSON to ntfy root. Stdlib urllib.request only.

The design goal: **the notification body itself carries enough info that a
phone user who cannot reach EventBridge's HTTP URL still sees the useful
result at a glance.** So `_compose_body()` packs the user's last prompt,
the assistant's reply, and a compact stats footer into the ntfy `message`
field — ntfy renders line breaks, so the layout survives.

Click / attach / http-action fields always use `EVENT_BRIDGE_PUBLIC_BASE_URL`
verbatim — no loopback detection. If the operator sets a loopback URL,
that's their choice (Tailscale tunnel, port-forward, testing from the same
machine, etc.); we trust them.
"""
from __future__ import annotations

import json
import queue
import threading
import urllib.request
from typing import Any

from eventbridge.config import NtfyCfg

# --- Message composition (pure functions — easy to unit-test) --------------

def _short(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _stats_line(stats: dict[str, Any] | None) -> str:
    if not isinstance(stats, dict):
        return ""
    bits: list[str] = []
    dm = stats.get("duration_ms")
    if isinstance(dm, (int, float)):
        bits.append(f"⏱ {dm/1000:.1f}s")
    c = stats.get("total_cost_usd")
    if isinstance(c, (int, float)) and c > 0:
        bits.append(f"💸 ${c:.4f}")
    u = stats.get("usage") or {}
    if isinstance(u, dict) and (u.get("input_tokens") or u.get("output_tokens")):
        bits.append(f"🔤 {u.get('input_tokens', 0)}→{u.get('output_tokens', 0)}")
    sr = stats.get("stop_reason")
    if sr and sr != "end_turn":
        bits.append(f"stop={sr}")
    return " · ".join(bits)


def _last_assistant_text(store, corr: str) -> str | None:
    """Look up the most recent assistant reply for this corr — used when the
    final event's own `text` was deduped away."""
    if store is None:
        return None
    try:
        for e in reversed(store.events_for(corr)):
            d = e.get("data") if isinstance(e.get("data"), dict) else {}
            if d.get("role") == "assistant" and d.get("text"):
                return d["text"]
    except Exception:
        return None
    return None


def _last_prompt(store, corr: str) -> str | None:
    if store is None:
        return None
    try:
        rows = store.get_prompts(corr)
        return rows[-1].get("prompt") if rows else None
    except Exception:
        return None


# Message body is capped to comfortably fit ntfy's 4KB message limit while
# leaving room for headers. Assistant text gets most of the budget.
MSG_CAP        = 3500
PROMPT_CHARS   = 240
ASSISTANT_CHARS = 2800


def compose_body(event: dict[str, Any], store=None) -> str:
    """Build the ntfy `message` string for one response event.

    Structure (line-broken so it reads cleanly in the phone notification's
    expanded view):

        ❓ <user prompt, first ~240 chars>

        💬 <assistant reply, first ~2800 chars>

        ⏱ 6.6s · 💸 $0.05 · 🔤 812→24

        📎 <correlationid>
    """
    corr  = event.get("correlationid", "?")
    phase = event.get("phase", "?")
    data  = event.get("data") if isinstance(event.get("data"), dict) else {}

    parts: list[str] = []

    if phase == "error":
        err = data.get("text") or "(error, see raw)"
        parts.append(f"❌ {_short(err, ASSISTANT_CHARS)}")
    else:
        prompt = _last_prompt(store, corr)
        if prompt:
            parts.append(f"❓ {_short(prompt, PROMPT_CHARS)}")

        # Prefer the classified event's own text; fall back to the last
        # assistant reply in the store (dedupe strips text off final events).
        assistant = data.get("text")
        if not assistant:
            assistant = _last_assistant_text(store, corr)
        if assistant:
            parts.append(f"💬 {_short(assistant, ASSISTANT_CHARS)}")

    stats = _stats_line(data.get("stats"))
    if stats:
        parts.append(stats)

    parts.append(f"📎 {corr}")
    body = "\n\n".join(parts)
    return body[:MSG_CAP]


def compose_title(event: dict[str, Any]) -> str:
    """Punchy one-liner for the notification title."""
    corr  = event.get("correlationid", "?")
    phase = event.get("phase", "?")
    data  = event.get("data") if isinstance(event.get("data"), dict) else {}
    if phase == "error":
        return f"⚠ agent {corr} — error"
    stats = data.get("stats") or {}
    dm = stats.get("duration_ms") if isinstance(stats, dict) else None
    c  = stats.get("total_cost_usd") if isinstance(stats, dict) else None
    tail_bits: list[str] = []
    if isinstance(dm, (int, float)): tail_bits.append(f"{dm/1000:.1f}s")
    if isinstance(c, (int, float)) and c > 0: tail_bits.append(f"${c:.4f}")
    if tail_bits:
        return f"agent {corr} · {' · '.join(tail_bits)}"
    return f"agent {corr} · {phase}"


# --- Publisher thread ------------------------------------------------------

class NtfyPublisher(threading.Thread):
    def __init__(self, cfg: NtfyCfg, public_base_url: str, store=None,
                 stores=None) -> None:
        super().__init__(daemon=True, name="ntfy-publisher")
        self._cfg = cfg
        self._base = public_base_url.rstrip("/")
        self._store = store
        # §6.1 — resolve the store per event, from its own `ce_userkey`. Without this,
        # `_last_prompt` and `_last_assistant_text` read only the shared store, so in
        # multi-tenant mode a notification for a tenant's correlation arrives with no
        # prompt and no reply — exactly the content §4.1 says the body carries so a phone
        # that cannot reach EventBridge still sees the result.
        self._stores = stores
        self._q: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._stopping = threading.Event()

    def _store_for(self, event: dict[str, Any]):
        """The store holding this event's prompt and replies."""
        if self._stores is None:
            return self._store
        return self._stores.for_event(event)

    def submit(self, event: dict[str, Any]) -> None:
        if not self._cfg.enabled or not self._cfg.topic:
            return
        from shared import ce
        # §21.6: a group announces its edges. The start and completion events notify
        # regardless of `phase` (they have none); member events are suppressed while
        # their group is still open, so a 100-agent batch is two notifications rather
        # than a hundred.
        if event.get("type") in ce.GROUP_TYPES:
            self._q.put(event)
            return
        if event.get("phase") not in self._cfg.phases:
            return
        if self._suppressed_member(event):
            return
        self._q.put(event)

    def _suppressed_member(self, event: dict[str, Any]) -> bool:
        """True when this event belongs to a known group.

        Membership is the whole test — NOT "the group is still open". An earlier version
        also required `completed_utc IS NULL`, and that leaked badly in practice: a
        100-agent batch produced ~47 per-agent notifications. The cause is RQ-1's
        at-least-once delivery. KEDA scales pods down, their offsets were never
        committed, Kafka redelivers the requests, the agents re-run and emit SECOND
        terminal events — which arrive after the group has completed and so sailed
        through the open-group check.

        The counting was never wrong (duplicates are ignored, completion fires once);
        only the notification policy was. A batch is two notifications, at its edges,
        for the whole life of the group.
        """
        groupid = event.get("groupid")
        store = self._store_for(event)
        if not groupid or store is None:
            return False
        if self._cfg.group_notify_errors and event.get("phase") == "error":
            return False
        try:
            # An unknown groupid is treated as ungrouped: if we have no group row we
            # will never send group notifications either, so suppressing would mean
            # sending nothing at all. Routed by the event's own userkey for the same
            # reason as the body lookup — the group row is in its owner's store.
            return store.get_group(groupid) is not None
        except Exception:  # noqa: BLE001
            return False

    def _publish_group(self, event: dict[str, Any]) -> None:
        """One notification when a batch starts, one when it ends (§21.6).

        These are the two that matter: past about a minute of waiting the operator has
        left, and should have. The notification is what calls them back, so it carries
        enough to decide whether to return — counts in user units, not a percentage.
        """
        from shared import ce
        gid = event.get("groupid", "?")
        d = event.get("data") or {}
        label = d.get("label") or gid
        started = event.get("type") == ce.TYPE_GROUP_STARTED

        if started:
            n = d.get("expected")
            title = f"group {label} started"
            msg = (f"🚀 {n} agent(s) submitted\n\n📎 {gid}"
                   if n else f"🚀 group started\n\n📎 {gid}")
            tags, prio = ["rocket"], 3
        else:
            fin, fail = d.get("finished", 0), d.get("failed", 0)
            reason = d.get("reason", "all")
            title = f"group {label} · {fin} done" + (f", {fail} failed" if fail else "")
            lines = [f"✅ {fin} finished"]
            if fail:
                lines.append(f"❌ {fail} failed")
            if d.get("expected"):
                lines.append(f"🎯 of {d['expected']} expected")
            if d.get("elapsed"):
                lines.append(f"⏱ {d['elapsed']}")
            if d.get("slowest_member_s"):
                lines.append(f"🐢 slowest agent {d['slowest_member_s']}s")
            if reason != "all":
                lines.append(f"⚠️ ended by: {reason}")
            msg = "\n".join(lines) + f"\n\n📎 {gid}"
            tags = ["white_check_mark"] if not fail else ["warning"]
            prio = 4 if (fail or reason != "all") else 3

        payload = {
            "topic": self._cfg.topic, "title": title, "message": msg,
            "priority": prio, "tags": tags,
            "click": f"{self._base}/v0/groups/{gid}",
        }
        self._post(payload)

    def stop(self) -> None:
        self._stopping.set()
        self._q.put(None)

    def run(self) -> None:
        while not self._stopping.is_set():
            item = self._q.get()
            if item is None:
                return
            try:
                self._publish(item)
            except Exception as e:  # noqa: BLE001
                print(f"[ntfy] publish failed: {e}")

    def _publish(self, event: dict[str, Any]) -> None:
        from shared import ce
        if event.get("type") in ce.GROUP_TYPES:
            self._publish_group(event)
            return
        corr  = event.get("correlationid", "?")
        phase = event.get("phase", "?")

        body_msg = compose_body(event, self._store_for(event))
        title    = compose_title(event)
        tags     = ["robot", phase]
        if phase == "error":
            tags = ["warning", "robot", "error"]

        # Trust EVENT_BRIDGE_PUBLIC_BASE_URL verbatim — no loopback detection.
        # If the operator set a loopback URL, that's their setup (Tailscale,
        # SSH port-forward, testing on the same host, etc.). The message body
        # already carries everything the phone needs; these fields are extras.
        payload: dict[str, Any] = {
            "topic":    self._cfg.topic,
            "title":    title,
            "message":  body_msg,
            "priority": 5 if phase == "error" else 3,
            "tags":     tags,
            "click":    f"{self._base}/v0/agents/{corr}",
            "attach":   f"{self._base}/v0/agents/{corr}/events.jsonl",
            "filename": f"{corr}.jsonl",
            "actions": [
                {"action": "view", "label": "Open history",
                 "url": f"{self._base}/v0/agents/{corr}", "clear": True},
                {"action": "view", "label": "Raw CloudEvents",
                 "url": f"{self._base}/v0/agents/{corr}/events.jsonl", "clear": False},
                {"action": "http", "label": "Continue…", "method": "POST",
                 "url": f"{self._base}/v0/agents/{corr}/continue",
                 "headers": {"Content-Type": "application/json"},
                 "body": json.dumps({"prompt": "continue"}), "clear": True},
            ],
        }

        self._post(payload)

    def _post(self, payload: dict[str, Any]) -> None:
        """The single place a notification leaves the process.

        Shared by the per-agent and the group publishers so auth, timeout and
        error handling cannot drift between them.
        """
        headers = {"Content-Type": "application/json"}
        if self._cfg.token:
            headers["Authorization"] = f"Bearer {self._cfg.token}"
        req = urllib.request.Request(
            self._cfg.base_url.rstrip("/"),
            data=json.dumps(payload).encode(),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
