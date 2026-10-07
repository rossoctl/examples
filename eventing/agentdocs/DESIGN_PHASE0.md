# DESIGN — Phase 0: Local Signed-Event Wake Demo (no Kubernetes)

Status: draft
Scope: Phase 0 — everything runs on a single macOS/Linux developer machine. No Kubernetes, no KEDA scaler, no cloud broker. Kafka runs locally from a tarball (see the running instance on `localhost:9092`).

Phase 0 exists to prove the **event-shaped control path** end to end:

```
Claude Code
   │ (calls skill "eventbridge")
   ▼
[eventbridge-agent]  ──HTTP──▶  EventBridge  ──CloudEvent (binary)──▶  Kafka:requests
                                       │                                       │
                                       │                                       ▼
                                       │                                 EventRunner
                                       │                                       │
                                       │                         spawns  claude -p
                                       │                          (--session-id / --resume)
                                       │                                       │
                                       │                                       ▼
                ntfy   ◀── EventBridge  ◀── Kafka:responses (binary CloudEvent)
                                       │
                                       ▼
                            HTML history at /v0/agents/<corr>
                            POST /continue → resume same claude session
```

Once the wire shape is stable, Phase 1 swaps EventRunner's process-fork for a KEDA-scaled Kubernetes Job that consumes the same Kafka topic with the same CloudEvent contract.

---

## 0. AI Agent Skill — `eventbridge`

A Claude Code skill lets you drive the whole thing from a conversation: **"run an agent to summarize threat-model1.md"** invokes the skill, which POSTs to EventBridge, polls for events, and shows the response inline. If you send a follow-up ("now list the mitigations"), the skill resumes the same correlationid so `claude -p --resume` sees the earlier turn.

### 0.1 Layout

```
.claude/skills/eventbridge/
├── SKILL.md            # skill contract (name, description, when-to-invoke)
├── eventbridge-cli.py  # small pure-Python helper: run, watch, continue, show
└── OPENAPI.md          # human-readable copy of EventBridge's OAS
```

### 0.2 `SKILL.md` (contract)

```markdown
---
name: eventbridge
description: |
  Run an autonomous Claude agent through the local EventBridge (Kafka + CloudEvents),
  stream responses, and resume prior conversations by correlationID. Use this whenever
  the user asks to "run an agent", "start a background task", or "continue <corr>".
---

## When to invoke
- "run an agent to <task>"       → skill.run(prompt=<task>)
- "check on <correlationid>"     → skill.watch(correlationid)
- "continue <correlationid>: X"  → skill.cont(correlationid, prompt=X)
- "show history <correlationid>" → skill.show(correlationid, format=html|json)

## Endpoints (see OPENAPI.md for full schema)
POST   /v0/agents                       {prompt, model?, max_turns?, notify?}
GET    /v0/agents/{corr}                (HTML view)
GET    /v0/agents/{corr}/events?since=N (JSON, filterable)
POST   /v0/agents/{corr}/continue       {prompt} — resumes same claude session
```

### 0.3 `eventbridge-cli.py` — pure-Python skill helper

Uses **stdlib only** (`urllib.request`, `json`, `time`, `argparse`) so the skill doesn't drag in wheels:

```python
#!/usr/bin/env python3
"""Skill helper — POST to EventBridge, poll response events, print inline."""
import argparse, json, os, sys, time, urllib.request

BASE = os.environ.get("EVENTBRIDGE_URL", "http://127.0.0.1:8080")

def _req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req  = urllib.request.Request(BASE + path, data=data, method=method,
                                  headers={"Content-Type": "application/json"} if body else {})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())

def cmd_run(args):
    r = _req("POST", "/v0/agents", {"prompt": args.prompt, "max_turns": args.max_turns})
    corr = r["correlationid"]; print(f"correlationid: {corr}")
    if args.watch: _watch(corr)

def _watch(corr):
    since = 0
    while True:
        r = _req("GET", f"/v0/agents/{corr}/events?since={since}")
        for e in r["events"]:
            print(f"[{e['sequence']:02d} {e['phase']:6s}] {json.dumps(e['data'])[:200]}")
            since = e["sequence"]
        if r["final"]: return
        time.sleep(1.0)

def cmd_watch(args): _watch(args.correlationid)
def cmd_cont(args):
    r = _req("POST", f"/v0/agents/{args.correlationid}/continue", {"prompt": args.prompt})
    print(f"resumed: {r['correlationid']} seq={r.get('sequence')}"); _watch(args.correlationid)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="op", required=True)
    r = sub.add_parser("run");    r.add_argument("prompt"); r.add_argument("--max-turns", type=int, default=3); r.add_argument("--watch", action=argparse.BooleanOptionalAction, default=True); r.set_defaults(fn=cmd_run)
    w = sub.add_parser("watch");  w.add_argument("correlationid"); w.set_defaults(fn=cmd_watch)
    c = sub.add_parser("cont");   c.add_argument("correlationid"); c.add_argument("prompt"); c.set_defaults(fn=cmd_cont)
    args = p.parse_args(); args.fn(args)
```

Claude Code invokes the skill; the skill runs `uv run python .claude/skills/eventbridge/eventbridge-cli.py run "…"` (or `cont`, `watch`) and streams stdout back into the chat. No new deps — stdlib is enough here because the skill only talks HTTP to a local service.

---

## 1. Runtime prerequisites


| Component | Version | Notes |
|---|---|---|
| CPython **free-threaded** | 3.14.7t | `python3.14t -VV` must say "free-threading build". `sys._is_gil_enabled()` returns `False`. Install: `brew install python-freethreading` or `uv python install 3.14t`. |
| **uv** | 0.12.15+ | Package + venv manager. Handles PEP 668 externally-managed pythons; drives `uv run` for launching components. |
| `kafka-python` | 3.0.11 | **Pure Python.** Kafka client — no C extension. |
| `cloudevents` | 2.2.0 | **Pure Python.** Kafka binding at `cloudevents.v1.kafka`. |
| Apache Kafka | 4.x (KRaft) | Local broker on `localhost:9092`; topics `requests` + `responses` already created. |
| `claude` CLI | current | Headless via `claude -p`. Session resume via `--session-id <UUID>` / `--resume <UUID>` / `--continue`. |
| ntfy | ntfy.sh (public) or self-hosted | Publish via HTTP `POST` with JSON body (Actions array). |

### 1.1 Pure-Python discipline (hard rule)

**No C extensions allowed** except for stdlib modules (which are pinned to the interpreter and audited by CPython). This has two consequences:

- No `pyyaml` (ships a `_yaml.so` C ext by default). Config files use **TOML** (`tomllib` stdlib) or JSON.
- No `requests` (pulls in `charset_normalizer`, which has an optional C accelerator). HTTP client uses **`urllib.request`** stdlib.
- No `confluent-kafka` (needs `librdkafka` + a source build; no free-threaded wheel).
- `sqlite3` stdlib is allowed — it's audited by CPython and free-threading-compatible on 3.14t.

Verification one-liner (should print all `False`):

```bash
python3.14t -X gil=0 -c '
import kafka, cloudevents, urllib.request, sqlite3, tomllib, wsgiref, http.server, sys
print("gil enabled:", sys._is_gil_enabled())
'
```

### 1.2 Package management — uv

All Python tooling is driven by **uv**. Brew's `python3.14t` is PEP 668 externally-managed; system pip refuses to install into it. uv creates isolated venvs and bypasses that cleanly.

Bootstrap:

```bash
brew install uv
cd ~/sandbox/rossoctl-keda1
uv venv --python 3.14t .venv          # one-time
uv sync                                # installs pyproject deps into .venv
```

`pyproject.toml` (single project, two console-scripts):

```toml
[project]
name = "rossoctl-keda1"
version = "0.1.0"
requires-python = ">=3.14"
dependencies = [
  "kafka-python>=3.0.11",
  "cloudevents>=2.2.0",
]

[project.scripts]
rossoctl-eventbridge = "eventbridge.__main__:main"
rossoctl-eventrunner = "eventrunner.__main__:main"

[tool.uv]
python-preference = "only-managed"
```

Running:

```bash
uv run rossoctl-eventbridge          # HTTP + Kafka producer/consumer
uv run rossoctl-eventrunner          # Kafka consumer + claude subprocess
uv run python -m pytest tests/       # tests
uv run python .claude/skills/eventbridge/eventbridge-cli.py run "…"
```

`uv run` implicitly re-runs `uv sync` when `pyproject.toml` changes.

### 1.3 Filesystem layout under `$TMPDIR`

Never `/tmp`. Everything scratch or per-request goes under `$TMPDIR` (macOS resolves this to a per-user path, Linux typically to `/tmp` with the correct sticky-bit semantics — we still let the env var win).

```
${TMPDIR%/}/rossoctl-keda1/
├── eventbridge/
│   ├── responses.sqlite            # per-correlationid response history (WAL)
│   ├── sessions.sqlite             # correlationid ↔ claude session UUID mapping
│   └── openapi.json                # generated OAS snapshot
└── eventrunner/
    └── work/<correlationid>/       # claude cwd; enables --continue by directory
        ├── prompt.txt              # latest prompt in this correlation
        ├── stdout.jsonl            # raw stream-json capture
        └── stderr.log
```

---

## 2. CloudEvent contract (binary content mode)

Both topics carry **binary-mode** CloudEvents — attributes ride Kafka headers as `ce_*`, body is the raw data payload. This matches `cloudevents.v1.kafka.to_binary` / `from_binary`. Structured mode is not used in Phase 0.

### 2.1 `requests` topic — event type `dev.rossoctl.agent.request.v1`

| Attribute | Kafka header | Example |
|---|---|---|
| `specversion` | `ce_specversion` | `1.0` |
| `type` | `ce_type` | `dev.rossoctl.agent.request.v1` |
| `source` | `ce_source` | `rossoctl://eventbridge/local` |
| `id` | `ce_id` | UUIDv4 |
| `time` | `ce_time` | RFC3339 |
| `datacontenttype` | `ce_datacontenttype` | `application/json` |
| `subject` | `ce_subject` | short human hint (`"summarize threat model"`) |
| **`correlationid`** (ext) | `ce_correlationid` | `wake-brave-otter-4718` — friendly slug |
| **`sessionuuid`** (ext) | `ce_sessionuuid` | UUIDv4 — real claude session id |
| **`mode`** (ext) | `ce_mode` | `start` (first turn) \| `continue` (resume) |

Kafka message **key** = `correlationid` (all events for one correlation land on one partition — trivial ordering even with N partitions later).

Data payload (JSON, `application/json`):
```json
{
  "prompt": "Summarize threat-model1.md in 5 bullets.",
  "model":  "claude-opus-5",
  "max_turns": 3,
  "cwd": null
}
```

### 2.2 `responses` topic — event type `dev.rossoctl.agent.response.v1`

Same envelope plus:

| Attribute | Header | Notes |
|---|---|---|
| `correlationid` | `ce_correlationid` | echoes request |
| `sessionuuid` | `ce_sessionuuid` | echoes request |
| **`sequence`** (ext) | `ce_sequence` | monotonic per correlationid: 1, 2, … |
| **`phase`** (ext) | `ce_phase` | `stdout` \| `stderr` \| `result` \| `error` |
| **`final`** (ext) | `ce_final` | `"true"` on the terminating event, else `"false"` |

Data payload — compacted from claude's `stream-json` frames by `runner.classify()` so consumers read `text` first and don't have to dig through nested JSON. Every response event carries a uniform shape:

```json
{
  "type": "assistant | result | system | user | <original>",
  "role": "assistant | final | system | user | raw",
  "text": "<human-readable text, or null>",
  "stats": { … only present when role=final … },
  "raw":   { … the full original stream-json frame, when ER_INCLUDE_RAW=true (default) … }
}
```

Concretely:

- **`role=assistant`** — `text` = concatenated `content[].text` (with `[tool_use:<name>]` markers for tool_use blocks). Emitted on `phase=stdout`.
- **`role=final`** — claude's own `type=result` frame. `text` = the `result` field. `stats` = `{num_turns, duration_ms, duration_api_ms, total_cost_usd, stop_reason, is_error, usage:{input_tokens,output_tokens,cache_*}}`. **Emitted on `phase=result final=true` — this is the terminal event.** No synthesized wrapper event is added if this arrived cleanly.
- **`role=system`** — `text` = one-line summary (e.g. `"init: model=claude-opus-5 session=abc12345… tools=27"`). `subtype` echoes claude's `init` / `hook_started` / `hook_response` / …. Hook frames are filtered by default (`ER_EMIT_SYSTEM_HOOKS=false`).
- **`role=raw`** — unrecognized JSON frame or a non-JSON line — text is the raw content.
- **`phase=error final=true`** — synthesized only when claude exits non-zero OR exits without ever emitting a `type=result` frame. `text` = `"claude exited with code N"`.

Design notes:

- **The claude CLI's `stream-json` protocol emits the model's last reply twice** — once inside the `assistant` frame's `content[0].text` and again inside the `result` frame's `result:` field. By default EventRunner **strips the duplicate**: when the final frame's `text` equals the prior assistant's text, `text` is set to `None` and `text_echoes_prior_assistant=true` is added so downstream consumers can render an "echo" note. Stats and `raw` are still preserved. Set `ER_DEDUPE_FINAL_TEXT=false` to keep both copies on the wire (audit-preserving mode).
- **`raw` is optional** — set `ER_INCLUDE_RAW=false` when moving to Phase 1 / signed events to shrink the wire and keep the CloudEvent envelope focused. Consumers must not depend on `raw` for correctness.

### 2.3 Correlation ID scheme — human-friendly, UUID-derived session id

Not raw UUIDs — users copy these into `curl` and read them back in logs:

```
<adjective>-<animal>-<4 digits>
   wake-brave-otter-4718
```

- Word lists (~200 × ~200 × 10⁴ ≈ 4 × 10⁸ space).
- Collision-guard set in EventBridge process memory.
- Regex: `^[a-z]{3,10}-[a-z]{3,12}-\d{4}$`.

**Why can't `correlationid` BE the claude session id?** Because `claude --session-id` **strictly requires a UUID** — verified by passing our friendly slug and getting `Error: Invalid session ID. Must be a valid UUID.` (exit 1). See Q&E-2 in §9.

**Solution — deterministic derivation.** `sessionuuid` is a **UUIDv5** derived from the correlationid under a fixed project namespace:

```python
# shared/ce.py
import uuid
NAMESPACE = uuid.UUID("6e5f8a90-0000-5000-a000-rossoctlkeda1")   # project-scoped, arbitrary but fixed

def session_uuid(correlationid: str) -> str:
    return str(uuid.uuid5(NAMESPACE, correlationid))
```

Effect: EventBridge and EventRunner **independently derive** the same UUID from the correlationid alone — no mapping table needed, no `sessions.sqlite` lookup on the hot path. `sessions.sqlite` still exists for auxiliary metadata (workdir, first prompt, turn count), but it is not on the critical path of `--resume`.

---

## 3. EventBridge (Python, threading, OAS, HTML)

### 3.1 Process shape

Single free-threaded Python 3.14t process:

```
main thread            ─── wsgiref ThreadingWSGIServer (HTTP)
worker pool            ─── ThreadPoolExecutor(N=8)
kafka producer thread  ─── flushes to `requests`
kafka consumer thread  ─── polls `responses`, writes SQLite
ntfy publisher thread  ─── drains queue → urllib.request.POST to ntfy
```

Web layer: **stdlib `wsgiref.simple_server` + `socketserver.ThreadingMixIn`** — no framework. Routes hand-dispatched in a small `router.py` (regex → handler). This keeps the deps list at just `kafka-python` + `cloudevents`.

### 3.2 HTTP API (OpenAPI 3.1)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v0/agents` | Start a new agent run. Returns `correlationid` + `sessionuuid`. |
| `POST` | `/v0/agents/{correlationid}/continue` | Send a new prompt to the same correlation → resumes `claude --resume`. |
| `GET`  | `/v0/agents/{correlationid}` | **HTML history view** — turn-grouped chat (server-rendered). |
| `GET`  | `/v0/agents/{correlationid}/turns` | JSON turn-grouped view (one row per turn, user prompt + assistant reply + stats). `?full=1` embeds events. |
| `GET`  | `/v0/agents/{correlationid}/events` | JSON events, optional `?since=<sequence>`. |
| `GET`  | `/v0/agents/{correlationid}/events.sse` | text/event-stream — live push to HTML view. |
| `GET`  | `/v0/agents/{correlationid}/events.jsonl` | Full CloudEvent JSON dump (for ntfy `Attach`). |
| `GET`  | `/openapi.json` / `/docs` | OAS + Swagger UI. |
| `GET`  | `/healthz` | Liveness. |

**Start**:
```
POST /v0/agents
{"prompt":"Summarize threat-model1.md in 5 bullets.","max_turns":3,"notify":true}
→ 202
{"correlationid":"wake-brave-otter-4718",
 "sessionuuid":"5f0f5f0f-0000-4000-8000-000000000001",
 "event_id":"…",
 "topic":"requests",
 "html_url":"http://127.0.0.1:8080/v0/agents/wake-brave-otter-4718"}
```

**Continue** (same correlation, new prompt — resumes claude session):
```
POST /v0/agents/wake-brave-otter-4718/continue
{"prompt":"Now list the mitigations for #2."}
→ 202
{"correlationid":"wake-brave-otter-4718","sequence":<next>,"event_id":"…"}
```

**Poll**:
```
GET /v0/agents/wake-brave-otter-4718/events?since=0
→ 200
{"correlationid":"…","events":[…],"final":<bool>}
```

Filtering is server-side: the consumer loop indexes every response event in `responses.sqlite` keyed by `correlationid`; GETs read from SQLite (`SELECT … WHERE correlationid=? AND sequence>?`).

### 3.3 HTML history view — `/v0/agents/{correlationid}`

Server-rendered HTML (no framework, no JS build). One template rendered inline from a Python string.

**Turn-grouped chat layout.** A single correlationid can carry many turns (one initial `POST /v0/agents` plus N `/continue` posts). The view groups response events by turn and renders each turn as a `<details>` block:

- **Summary line (always visible):** `Turn N · mode · duration · $cost — <first line of assistant reply>`.
- **Body (expanded for the latest turn, collapsed for earlier):**
  - `USER: <prompt>` — from `prompts.sqlite` (§3.5).
  - `ASSISTANT: <assistant.text>` — extracted by `runner.classify()`.
  - Nested `<details>Details (N events)</details>` — the flat event list for this turn, one card per response event (system/init, echoed final with usage stats, tool_use, raw stream-json inside a further nested `<details>`).

**Turn boundary heuristic.** A turn *ends* on any response event whose `final=true`; the next event opens a new turn. This works for real claude (`type=result` frame is promoted to `phase=result final=true`) and the mock runner, and doesn't rely on `system/init` frames (some invocations may skip them).

**Prompts.** Each `POST /v0/agents` and `POST /v0/agents/<corr>/continue` inserts a row into `prompts.sqlite` keyed by `(correlationid, turn_index)`. `group_by_turns()` pairs them positionally with the grouped events at render time. Missing prompts are rendered as `(prompt not recorded)`.

**Header rollup.** Above the turn list: `sessionuuid · N turns · M events · $total-cost · input→output tokens`. Cost and tokens are summed across all `role=final` events' `stats`.

**Continue box** at the bottom: `<textarea>` + `<button>` — submits `POST /v0/agents/<corr>/continue-html`, then reloads. This is how a human resumes a headless agent conversation without touching the CLI.

**Live updates via SSE** — the page subscribes to `GET /v0/agents/{corr}/events.sse` (see Q&E-6). Live cards land in a "↓ live updates" section at the bottom; on `final=true` the page reloads so the fresh events regroup into their turn under the details tree. Falls back gracefully when `EventSource` is not available.

**JSON sibling.** `GET /v0/agents/{corr}/turns` returns the same grouping as JSON — one row per turn with `prompt`, `assistant_text`, `stats`, `event_count`. Add `?full=1` to embed the flat events under each turn. This is what the skill CLI's `chat` subcommand consumes.

The HTML view is what ntfy notifications' `Click` header (and Actions `view` buttons) point at. See §3.6.

### 3.4 OpenAPI 3.1 spec

Hand-written `openapi.py` — five paths, no framework needed. Served at `/openapi.json`; Swagger UI at `/docs` loads from `cdn.jsdelivr.net`. Snapshot persisted to `${TMPDIR%/}/rossoctl-keda1/eventbridge/openapi.json` on startup for external linters.

### 3.5 Producing / consuming Kafka

```python
# eventbridge/kafka_out.py — pure Python
from kafka import KafkaProducer
from cloudevents.v1.http import CloudEvent
from cloudevents.v1.kafka import to_binary
import json, uuid

_producer = KafkaProducer(bootstrap_servers=CFG.kafka_bootstrap)

def publish_request(prompt, correlationid, sessionuuid, mode="start", **attrs):
    event = CloudEvent(
        {
            "type":            "dev.rossoctl.agent.request.v1",
            "source":          CFG.source_uri,
            "id":              str(uuid.uuid4()),
            "subject":         attrs.get("subject", "agent-request"),
            "datacontenttype": "application/json",
            "correlationid":   correlationid,
            "sessionuuid":     sessionuuid,
            "mode":            mode,           # "start" | "continue"
        },
        {"prompt": prompt, "model": attrs.get("model"), "max_turns": attrs.get("max_turns", 3)},
    )
    msg = to_binary(event, data_marshaller=lambda d: json.dumps(d).encode())
    value = msg.value.encode() if isinstance(msg.value, str) else msg.value
    _producer.send(
        "requests",
        key=correlationid.encode(),
        value=value,
        headers=[(k, v) for k, v in msg.headers.items()],
    ).get(timeout=5)
    return event["id"]
```

```python
# eventbridge/kafka_in.py — pure Python
from kafka import KafkaConsumer
from cloudevents.v1.kafka import KafkaMessage, from_binary
import json

consumer = KafkaConsumer(
    "responses",
    bootstrap_servers=CFG.kafka_bootstrap,
    group_id="eventbridge-responses",
    auto_offset_reset="earliest",
    consumer_timeout_ms=1000,
)
for rec in consumer:                        # blocks up to consumer_timeout_ms per empty poll
    kmsg = KafkaMessage(
        headers={k: v for k, v in (rec.headers or [])},
        key=rec.key,
        value=rec.value,
    )
    event = from_binary(kmsg, data_unmarshaller=json.loads)
    store.insert_response(event)
    ntfy_queue.put(event)                   # fan-out to ntfy thread
```

SQLite schemas:

```sql
-- responses.sqlite
CREATE TABLE responses (
  correlationid TEXT NOT NULL,
  sequence      INTEGER NOT NULL,
  phase         TEXT NOT NULL,
  event_id      TEXT NOT NULL,
  event_time    TEXT NOT NULL,
  data_json     TEXT NOT NULL,
  final         INTEGER NOT NULL DEFAULT 0,
  raw_json      TEXT NOT NULL,               -- full CloudEvent (for /events.jsonl)
  PRIMARY KEY (correlationid, sequence)
);
CREATE INDEX responses_by_corr ON responses(correlationid);

-- sessions.sqlite  (auxiliary metadata; NOT on the /continue hot path — sessionuuid is derived)
CREATE TABLE sessions (
  correlationid TEXT PRIMARY KEY,
  sessionuuid   TEXT NOT NULL,          -- redundant with uuid5(NAMESPACE, correlationid); stored for auditing
  workdir       TEXT NOT NULL,
  first_prompt  TEXT,
  created_utc   TEXT NOT NULL,
  updated_utc   TEXT NOT NULL,
  turns         INTEGER NOT NULL DEFAULT 0
);
```

`PRAGMA journal_mode=WAL`. Under free-threading, wrap all writes with `threading.Lock` and open connections with `check_same_thread=False`.

### 3.6 ntfy fan-out — configurable topic, action buttons, CloudEvent link

The "ntfy subscription ID" is the ntfy **topic** — configurable via env or config. It should be an opaque, unguessable string (recommended for ntfy.sh) — a real one is never written down here, because the topic name IS the credential or a project-scoped slug for self-hosted instances.

Config (TOML; loaded via stdlib `tomllib`):

```toml
# eventbridge/config.toml
[kafka]
bootstrap = "localhost:9092"
request_topic  = "requests"
response_topic = "responses"

[http]
addr = "127.0.0.1:8080"
workers = 8

[ntfy]
enabled  = true
base_url = "https://ntfy.sh"
topic    = "<your-ntfy-topic>"                    # subscription ID — subscribe at https://ntfy.sh/<your-ntfy-topic>
token    = ""                                     # optional Bearer for self-hosted
phases   = ["result", "error"]                    # which response phases fan out
public_base_url = "http://127.0.0.1:8080"         # inserted into Click / Actions
```

Env overrides always win:

| Var | Default | Meaning |
|---|---|---|
| `NTFY_ENABLED` | `true` | |
| `NTFY_BASE_URL` | `https://ntfy.sh` | |
| `NTFY_TOPIC` | (required) | an opaque unguessable string; full URL becomes `${NTFY_BASE_URL}/${NTFY_TOPIC}` |
| `NTFY_TOKEN` | (empty) | Bearer, self-hosted only |
| `NTFY_PHASES` | `result,error` | |
| `EVENT_BRIDGE_PUBLIC_BASE_URL` | `http://127.0.0.1:8080` | Externally-reachable URL for the HTML view + ntfy Click/Actions. Used **verbatim** — no loopback detection, so a Tailscale/SSH-forward/localhost setup all just work if the phone can reach that URL. |

**Payload format — JSON, not headers.** The guide fetched during research (bigiron.cc) is explicit: header syntax for Actions "breaks on commas and semicolons inside URLs", so for anything beyond the trivial case, POST JSON to the ntfy root:

```python
# eventbridge/ntfy.py — pure Python, stdlib only
import json, urllib.request

def publish(evt, data):
    corr = evt["correlationid"]
    phase = evt["phase"]
    if phase not in CFG.ntfy.phases: return
    body = {
        "topic":   CFG.ntfy.topic,
        "title":   f"{phase}: {corr}",
        "message": _summary(data)[:400],
        "priority": 5 if phase == "error" else 3,
        "tags":    ["robot", phase],
        "click":   f"{CFG.public_base_url}/v0/agents/{corr}",
        "attach":  f"{CFG.public_base_url}/v0/agents/{corr}/events.jsonl",
        "filename": f"{corr}.jsonl",
        "actions": [
            {"action": "view", "label": "Open history",
             "url": f"{CFG.public_base_url}/v0/agents/{corr}", "clear": True},
            {"action": "view", "label": "Raw CloudEvents",
             "url": f"{CFG.public_base_url}/v0/agents/{corr}/events.jsonl", "clear": False},
            {"action": "http", "label": "Continue…", "method": "POST",
             "url": f"{CFG.public_base_url}/v0/agents/{corr}/continue",
             "headers": {"Content-Type": "application/json"},
             "body": json.dumps({"prompt": "continue"})},
        ],
    }
    headers = {"Content-Type": "application/json"}
    if CFG.ntfy.token:
        headers["Authorization"] = f"Bearer {CFG.ntfy.token}"
    req = urllib.request.Request(
        CFG.ntfy.base_url, data=json.dumps(body).encode(), headers=headers, method="POST",
    )
    urllib.request.urlopen(req, timeout=5).read()
```

Notes from research:

- **Can ntfy carry the full CloudEvent JSON?** Directly in the notification body, no — ntfy renders it as text and phones truncate. The right pattern is `attach` + `filename`: the notification stays short, and tapping "download attachment" fetches the full CloudEvent JSON stream from EventBridge. That gives you the machine-readable payload without stuffing it into the toast.
- **Action buttons — max 3, three types.** `view` opens a URL (used for HTML history + jsonl). `http` fires POST/GET/PUT/DELETE with headers/body (used for "Continue…" one-tap resume). `broadcast` is Android-only Tasker. Never put a real credential in an `http` action's headers — the payload is stored in the phone's local notification history.
- **Click → URL** is single-tap: opens the HTML view. Actions are the additional buttons.

### 3.7 Resume flow — /continue → `claude --resume`

1. `POST /v0/agents/<corr>/continue {"prompt":"…"}` arrives.
2. EventBridge derives `sessionuuid = uuid5(NAMESPACE, correlationid)` — no DB lookup. Refuses (404) only if no prior `start` event for that correlationid exists in `responses.sqlite`.
3. Publishes a **new** request CloudEvent with `mode="continue"`, same `correlationid`, computed `sessionuuid`, `subject="resume"`.
4. EventRunner independently derives the same `sessionuuid` (or reads it from `ce_sessionuuid`) → shells `claude -p --resume <sessionuuid>` in the per-correlationid workdir.
5. Response events flow back on the responses topic tagged with the same `correlationid` + incremented `sequence`.

The claude CLI persists sessions to `~/.claude/projects/<encoded-cwd>/<session-id>.jsonl`, so the `workdir` must be stable across invocations for the same correlationid — that's why §1.3 keeps a per-correlationid working directory.

---

## 4. EventRunner (Python, threading, process-per-request)

### 4.1 Process shape

```
main thread            ─── KafkaConsumer(group=eventrunner), yields records
router                 ─── per-correlationid AgentSlot (§4.5); enqueues + spawns worker
worker thread (per     ─── one thread per BUSY correlationid, drains that slot's FIFO;
correlationid)             for each request, spawns `claude`, pumps stdout/stderr,
                           emits response CloudEvents, waits, moves to next queued turn
BoundedSemaphore(N)    ─── caps DISTINCT correlationids running in parallel (ER_MAX_CONCURRENT)
producer thread        ─── drains emit queue, produces to responses topic
```

Process-per-request, not thread-per-request — matches the Phase 1 Kubernetes Job model. Python threads only orchestrate; `claude` is a child OS process. Ordering guarantees for a single correlationid live in the router (§4.5), not in claude itself: Q&E-8 confirms claude serializes internally but nondeterministically, so we serialize at our layer to make ordering match the request stream.

### 4.2 Consume + dispatch

```python
# eventrunner/consume.py
from kafka import KafkaConsumer
from cloudevents.v1.kafka import KafkaMessage, from_binary
import json

consumer = KafkaConsumer(
    "requests",
    bootstrap_servers=CFG.kafka_bootstrap,
    group_id="eventrunner",
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    consumer_timeout_ms=1000,
)
for rec in consumer:
    kmsg = KafkaMessage(
        headers={k: v for k, v in (rec.headers or [])},
        key=rec.key,
        value=rec.value,
    )
    event = from_binary(kmsg, data_unmarshaller=json.loads)
    router.submit(event)     # per-correlationid FIFO; see §4.5
    consumer.commit()        # after the router owns it — not before
```

### 4.3 Spawn claude (start vs. resume)

```python
# eventrunner/runner.py
import itertools, os, pathlib, subprocess, threading, time, uuid

def run_agent(event):
    corr    = event["correlationid"]
    session = event["sessionuuid"]
    mode    = event.get("mode", "start")
    payload = event.data if isinstance(event.data, dict) else json.loads(event.data)
    prompt  = payload["prompt"]
    workdir = pathlib.Path(os.environ["TMPDIR"], "rossoctl-keda1/eventrunner/work", corr)
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / "prompt.txt").write_text(prompt)

    cmd = [
        CFG.claude_bin, "-p", prompt,
        "--output-format", "stream-json",
        "--verbose",
        "--max-turns", str(payload.get("max_turns", 3)),
        "--permission-mode", "acceptEdits",
        "--no-session-persistence" if False else "",   # keep persistence for resume
        "--bare",
    ]
    cmd = [a for a in cmd if a]
    if mode == "start":
        cmd += ["--session-id", session]
    else:  # continue
        cmd += ["--resume", session]
    if model := payload.get("model"):
        cmd += ["--model", model]

    seq = itertools.count(next_sequence(corr))
    started = time.monotonic()
    with subprocess.Popen(cmd, cwd=workdir,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, bufsize=1) as proc:
        t_out = threading.Thread(target=_pump, args=(proc.stdout, corr, session, "stdout", seq), daemon=True)
        t_err = threading.Thread(target=_pump, args=(proc.stderr, corr, session, "stderr", seq), daemon=True)
        t_out.start(); t_err.start()
        rc = proc.wait(); t_out.join(); t_err.join()

    emit(corr, session, next(seq), phase="result", final=True, data={
        "final": True, "exit_code": rc,
        "duration_ms": int((time.monotonic() - started) * 1000),
    })

def _pump(stream, corr, session, phase, seq):
    for line in stream:
        emit(corr, session, next(seq), phase=phase, final=False, data={"text": line.rstrip()})
```

`stream-json` output means each turn from `claude` becomes its own response CloudEvent — the HTML view and the ntfy result event both get incremental progress.

Each stream-json line is fed through `runner.classify()` before emit (§2.2 for the target shape). Two behaviors matter for the terminal event:

- **Claude's own `type=result` frame is emitted as `phase=result final=true`** — it *is* the terminal event, not a stdout precursor to some synthesized wrapper. `_pump` sets a `saw_result` flag; the outer function then skips its synthesized terminal when the exit code is 0.
- **`system/hook_*` frames are filtered** by default (`ER_EMIT_SYSTEM_HOOKS=false`) — `SessionStart:startup` chatter is not interesting to a downstream KEDA / ntfy consumer.
- If claude exits non-zero, or exits without emitting `type=result`, `_pump` never sets `saw_result`; the outer function emits a `phase=error final=true` event with `text="claude exited with code N"` so the terminal signal is never lost.

**Working directory is per-correlationid** (`workdir` above). Claude's session store is keyed by cwd + session-id, so keeping the workdir stable across `mode=start` and `mode=continue` requests for the same correlation is what lets `--resume` find the transcript.

### 4.4 Emit response CloudEvent

```python
# eventrunner/emit.py
def emit(corr, session, sequence, phase, final, data):
    event = CloudEvent(
        {
            "type":            "dev.rossoctl.agent.response.v1",
            "source":          CFG.source_uri,       # rossoctl://eventrunner/local
            "id":              str(uuid.uuid4()),
            "datacontenttype": "application/json",
            "correlationid":   corr,
            "sessionuuid":     session,
            "sequence":        sequence,
            "phase":           phase,
            "final":           "true" if final else "false",
        },
        data,
    )
    kmsg = to_binary(event, data_marshaller=lambda d: json.dumps(d).encode())
    _producer.send("responses",
                   key=corr.encode(),
                   value=kmsg.value.encode() if isinstance(kmsg.value, str) else kmsg.value,
                   headers=[(k, v) for k, v in kmsg.headers.items()]).get(timeout=5)
```

### 4.5 Per-correlationid routing & internal queue

Q&E-8 established that concurrent `--resume <same-uuid>` calls do not corrupt the transcript, but their ordering is nondeterministic — an unacceptable UX for a "continue my agent conversation" workflow. EventRunner solves this at the dispatcher level: **one live `claude` subprocess per correlationid, at most, with a per-correlationid FIFO for anything that arrives while it is busy.**

```
    KafkaConsumer(requests)         ─── main thread
              │
              ▼
        dispatcher.submit(event)    ─── acquires router.lock briefly
              │
              ▼
   router: correlationid → AgentSlot
              │
   ┌──────────┴────────────┐
   │ slot idle             │ slot busy
   ▼                       ▼
 spawn worker         append event
 for this slot        to slot.queue
```

**Data structure** — a single `Router` object holding a `dict[str, AgentSlot]`:

```python
# eventrunner/router.py — pure Python, threading only
from collections import deque
from dataclasses import dataclass, field
import threading

@dataclass
class AgentSlot:
    correlationid: str
    queue:  deque         = field(default_factory=deque)  # request CloudEvents, FIFO
    lock:   threading.Lock = field(default_factory=threading.Lock)
    busy:   bool          = False
    worker: "threading.Thread | None" = None

class Router:
    def __init__(self, max_concurrent_correlations: int):
        self._slots: dict[str, AgentSlot] = {}
        self._router_lock = threading.Lock()                                    # protects the map
        self._concurrency = threading.BoundedSemaphore(max_concurrent_correlations)
        # ↑ caps DISTINCT correlationids running in parallel; ER_MAX_CONCURRENT

    def submit(self, event) -> None:
        corr = event["correlationid"]
        with self._router_lock:
            slot = self._slots.get(corr)
            if slot is None:
                slot = AgentSlot(correlationid=corr)
                self._slots[corr] = slot
        with slot.lock:
            slot.queue.append(event)
            if not slot.busy:
                slot.busy = True
                slot.worker = threading.Thread(
                    target=self._drain, args=(slot,), daemon=True,
                    name=f"agent-worker/{corr}",
                )
                slot.worker.start()

    def _drain(self, slot: AgentSlot) -> None:
        # Serialize turns for THIS correlationid; parallelism across correlationids
        # is bounded by the semaphore.
        with self._concurrency:
            while True:
                with slot.lock:
                    if not slot.queue:
                        slot.busy = False
                        return
                    event = slot.queue.popleft()
                run_agent(event)     # spawns claude, streams stdout → response events, waits

        # After exiting `with self._concurrency`, another correlationid may resume.
        # (If new events arrived for this slot in the meantime, submit() saw busy=False
        # and started a new worker for them.)
```

**Guarantees:**

- **Ordering.** For a given correlationid, requests are dispatched in the order they were consumed off the `requests` topic. Since Kafka message key = `correlationid` (see §2.1), all events for one correlation land on the same partition, so consumer order matches producer order — the FIFO is honored end to end.
- **Concurrency isolation.** Different correlationids run in parallel, bounded by `ER_MAX_CONCURRENT` (default 4). A single correlationid never has two simultaneous `claude` subprocesses.
- **No stall.** `_drain` returns as soon as the slot's queue is empty; a later burst re-arms a fresh worker. `busy` guards races between `submit` and `_drain` under `slot.lock`.
- **Semaphore fairness.** `BoundedSemaphore` gives FIFO wake-up on most POSIX implementations. If two correlationids are queued behind the concurrency cap and one has more pending turns than the other, they still get fair alternation because each `run_agent` releases and re-acquires per-turn is **not** what we do — we hold the semaphore for the whole drain, so a very active correlation could hog it. This is an acceptable trade-off in Phase 0 (4-slot cap on a laptop); a token-bucket fairness scheme is Phase 1.

**Kafka commit discipline.** The consume loop in §4.2 currently commits before dispatch. With the router, we must commit **after** the router accepts the event (i.e., after `router.submit(event)` returns), so a crash between consume and enqueue does not lose the request:

```python
for rec in consumer:
    event = from_binary(_kmsg(rec), data_unmarshaller=json.loads)
    router.submit(event)     # returns as soon as it's on the slot queue
    consumer.commit()
```

Router queues are in-process (not durable). A process crash loses queued-but-not-yet-run requests; Kafka's committed offsets have already advanced past them. Phase 0 accepts this — the request event is still on the `requests` topic, so a manual reset of the consumer group offset is enough to replay. Phase 1 (KEDA-scaled Jobs) sidesteps the issue: one Job per request, no in-process queue.

**HTML view & SSE.** No change. `sequence` on response events is still monotonic per correlationid — the router just guarantees that requests are executed in the order that produced those sequences.

**Test hook.** `router.submit()` is easy to unit-test in isolation: feed it two events with the same correlationid, block the first `run_agent` behind a `threading.Event`, verify the second one waits and then runs after the first completes. A stub `run_agent` replaces the real subprocess call.

---

## 5. Configuration surface

Both components read three sources in order: env > `config.toml` > baked defaults.

Shared:

| Var | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | |
| `REQUEST_TOPIC` | `requests` | |
| `RESPONSE_TOPIC` | `responses` | |
| `TMPDIR` | (inherited) | scratch/persistent root |

EventBridge:

| Var | Default | Meaning |
|---|---|---|
| `EB_HTTP_ADDR` | `127.0.0.1:8080` | HTTP listener |
| `EB_WORKERS` | `8` | thread pool |
| `EVENT_BRIDGE_PUBLIC_BASE_URL` | `http://127.0.0.1:8080` | inserted verbatim into HTML view + ntfy Click/Actions (no loopback filter) |
| `NTFY_ENABLED` | `true` | |
| `NTFY_BASE_URL` | `https://ntfy.sh` | |
| **`NTFY_TOPIC`** | (required) | an opaque unguessable string → subscribe at `https://ntfy.sh/<your-ntfy-topic>` |
| `NTFY_TOKEN` | (empty) | Bearer, self-hosted |
| `NTFY_PHASES` | `result,error` | |

EventRunner:

| Var | Default | Meaning |
|---|---|---|
| `ER_MAX_CONCURRENT` | `4` | max **distinct** correlationids running in parallel (per-corr FIFO §4.5) |
| `CLAUDE_BIN` | `claude` | path to the CLI |
| `ER_MOCK_CLAUDE` | `false` | swap `subprocess.Popen(claude)` for a deterministic 3-event mock (no API cost) |
| `ER_EMIT_SYSTEM_HOOKS` | `false` | forward `system/hook_started` + `hook_response` frames as response events. Off by default — they're just `SessionStart:startup` chatter |
| `ER_INCLUDE_RAW` | `true` | include the full stream-json frame under `data.raw` on each response event. Set `false` in Phase 1 to shrink signed envelopes |
| `ER_DEDUPE_FINAL_TEXT` | `true` | when claude's `type=result` frame's `result:` text equals the prior assistant text, strip `text` from the final event (keep stats + raw) and set `text_echoes_prior_assistant=true`. Off preserves both copies verbatim |

**Env vars forwarded to the `claude` subprocess** (allowlist, not full inherit — see `eventrunner/runner.py::child_env`): `PATH`, `HOME`, `USER`, `LOGNAME`, `TMPDIR`, `SHELL`, `LANG`, `LC_ALL`, `TERM`, `ANTHROPIC_BASE_URL`, `ANTHROPIC_MODEL`, `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_API_KEY`, `CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS`. Secrets are redacted in the startup log.

---

## 6. Running the demo end to end

```
term 1 │ Kafka                                (already running from earlier session)
       │   export JAVA_HOME=$(/usr/libexec/java_home -v 21)
       │   kafka/bin/kafka-server-start.sh kafka/config/server.properties
term 2 │ EventBridge
       │   export NTFY_TOPIC=<your-ntfy-topic>
       │   uv run rossoctl-eventbridge
term 3 │ EventRunner
       │   uv run rossoctl-eventrunner
term 4 │ ntfy subscription  (see AGENTS.md — Safari added to Dock is best on macOS)
       │   open -a Safari "https://ntfy.sh/$NTFY_TOPIC"
term 5 │ Driver (either curl or the skill)
       │   # via curl
       │   curl -sX POST http://127.0.0.1:8080/v0/agents \
       │     -H 'Content-Type: application/json' \
       │     -d '{"prompt":"Summarize threat-model1.md in 5 bullets.","max_turns":3}'
       │   # → {"correlationid":"wake-brave-otter-4718","sessionuuid":"…",…}
       │
       │   # via the skill (from a Claude Code session in this repo)
       │   /eventbridge run "Summarize threat-model1.md in 5 bullets."
       │   /eventbridge cont wake-brave-otter-4718 "Now list mitigations for #2."
       │   open http://127.0.0.1:8080/v0/agents/wake-brave-otter-4718
```

Expected outcomes:

1. `POST /v0/agents` → 202 with `correlationid` + `sessionuuid` in <100 ms.
2. EventRunner spawns `claude -p --session-id <uuid> …` in `${TMPDIR}rossoctl-keda1/eventrunner/work/<corr>/`.
3. `GET /v0/agents/<corr>` HTML view begins showing `phase=stdout` events within seconds; auto-refreshes.
4. `phase=result final=true` event lands; HTML stops auto-refreshing.
5. An ntfy notification arrives with:
   - Title: `result: wake-brave-otter-4718`
   - Body: first ~200 chars of `data.result`
   - Tap: opens `http://127.0.0.1:8080/v0/agents/wake-brave-otter-4718` (HTML view)
   - "Raw CloudEvents" action button: fetches `.jsonl` — the full CloudEvent JSON stream
   - "Continue…" action: one-tap `POST /continue` (dev convenience only; strip in Phase 1)
6. `POST /continue` with a new prompt → new request event with `mode=continue` → EventRunner runs `claude -p --resume <sessionuuid>` in the same workdir → new response events append with `sequence` continuing where it left off.

---

## 6a. Integration test runner (`rossoctl-e2e-test`)

A third console-script `rossoctl-e2e-test` is a **stdlib-only** Python harness that drives the whole flow end to end and asserts on every observable channel. Console script:

```toml
[project.scripts]
rossoctl-eventbridge = "eventbridge.__main__:main"
rossoctl-eventrunner = "eventrunner.__main__:main"
rossoctl-e2e-test    = "tests_e2e.__main__:main"        # new
```

Run with `uv run rossoctl-e2e-test` (add `--skill` to route the two "starts" through the `eventbridge` Claude Code skill instead of raw HTTP, verifying the skill wrapper too).

### 6a.1 What it does

```
                    e2e test runner
                          |
       POST /v0/agents  ->|  (agent A)
       POST /v0/agents  ->|  (agent B)
                          |
       Kafka `requests`  watcher  (3 events)
       Kafka `responses` watcher  (N events)
       ntfy      /json   stream watcher
                          |
       POST /v0/agents/<B>/continue
                          |
       GET  /v0/agents/<A>/events   (final)
       GET  /v0/agents/<B>/events   (final)
                          |
                    PASS / FAIL summary
```

### 6a.2 Assertions

1. **Kafka `requests` topic** — exactly **3** `dev.rossoctl.agent.request.v1` events observed after test start: two `mode=start` (correlationids `A` and `B`), one `mode=continue` (correlationid `B`, second turn). `ce_sessionuuid` on every event matches `uuid5(NAMESPACE, correlationid)`.
2. **Kafka `responses` topic** — at least **2** events with `ce_phase=result` `ce_final=true` (one per agent's first turn) plus **1** more `phase=result final=true` for agent B's continuation. Every `ce_correlationid` appears in requests too. Sequence numbers per correlationid are contiguous starting from 1.
3. **ntfy stream** — subscribes to `${NTFY_BASE_URL}/${NTFY_TOPIC}/json` before starting the test. Every notification with `event=="message"` observed during the test must correspond to a `phase=result` or `phase=error` response event; body contains the correlationid; `click` field is `${EVENT_BRIDGE_PUBLIC_BASE_URL}/v0/agents/<corr>`.
4. **HTTP** — `GET /v0/agents/<A>/events` and `.../<B>/events` return `final:true` within the test's timeout. Agent B's response events span `sequence` >= 1 across both turns (contiguous, no gap).
5. **Correlation ID scheme** — both correlationids match the regex `^[a-z]{3,10}-[a-z]{3,12}-\d{4}$` and are distinct.

Any failed assertion => non-zero exit, structured diagnostic printed. Pass => one-line PASS + timing summary.

### 6a.3 Layout

```
tests_e2e/
  __main__.py               (entrypoint - argparse, orchestration)
  watchers/
    kafka_watcher.py        (KafkaConsumer group=e2e-<pid>-{requests,responses})
    ntfy_watcher.py         (urllib.request GET <topic>/json, line-by-line)
  agents.py                 (start_agent, continue_agent, poll_until_final)
  assertions.py             (typed asserts with human-readable failure messages)
  skill_bridge.py           (--skill mode: shells `uv run python .claude/skills/eventbridge/eventbridge-cli.py ...`)
```

### 6a.4 Skeleton — `tests_e2e/__main__.py`

```python
"""End-to-end integration test - start 2 agents, continue one, verify every channel."""
import argparse, json, os, threading, time, urllib.request, uuid
from concurrent.futures import ThreadPoolExecutor
from tests_e2e.watchers.kafka_watcher import KafkaWatcher
from tests_e2e.watchers.ntfy_watcher  import NtfyWatcher
from tests_e2e.agents      import start, cont, wait_final
from tests_e2e.assertions  import expect

EB   = os.environ.get("EVENTBRIDGE_URL", "http://127.0.0.1:8080")
NTFY = f"{os.environ['NTFY_BASE_URL']}/{os.environ['NTFY_TOPIC']}"
NAMESPACE = uuid.UUID("6e5f8a90-0000-5000-a000-000000000001")   # match shared/ce.py

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skill", action="store_true",
                    help="route starts through the skill CLI, not raw HTTP")
    ap.add_argument("--timeout", type=float, default=90.0)
    args = ap.parse_args()

    print("* spawning watchers ...")
    now_ms = time.time_ns() // 1_000_000
    req_watch  = KafkaWatcher(topic="requests",  since=now_ms)
    rsp_watch  = KafkaWatcher(topic="responses", since=now_ms)
    ntfy_watch = NtfyWatcher(url=f"{NTFY}/json")
    for w in (req_watch, rsp_watch, ntfy_watch): w.start()

    print("* starting agents A and B ...")
    corrA = start("What is 2 + 2? Reply with just the digit.", skill=args.skill)
    corrB = start("Name one animal starting with the letter O.", skill=args.skill)
    print(f"  A={corrA}  B={corrB}")

    print("* waiting for both to reach final ...")
    with ThreadPoolExecutor(max_workers=2) as ex:
        fA = ex.submit(wait_final, corrA, args.timeout)
        fB = ex.submit(wait_final, corrB, args.timeout)
        evA, evB = fA.result(), fB.result()
    print(f"  A final in {evA['duration_s']:.1f}s  result={evA['events'][-1]['data'].get('result','')!r}")
    print(f"  B final in {evB['duration_s']:.1f}s  result={evB['events'][-1]['data'].get('result','')!r}")

    print("* continuing agent B ...")
    cont(corrB, "Now name one animal starting with P.")
    evB2 = wait_final(corrB, args.timeout)
    print(f"  B(cont) final in {evB2['duration_s']:.1f}s")

    print("* asserting channels ...")
    reqs = req_watch.drain(timeout=2.0)
    rsps = rsp_watch.drain(timeout=2.0)
    ntfs = ntfy_watch.drain(timeout=2.0)
    for w in (req_watch, rsp_watch, ntfy_watch): w.stop()

    starts  = [e for e in reqs if e.get("mode") == "start"    and e["correlationid"] in (corrA, corrB)]
    conts   = [e for e in reqs if e.get("mode") == "continue" and e["correlationid"] == corrB]
    results = [e for e in rsps if e.get("phase") == "result"  and e["correlationid"] in (corrA, corrB)]

    expect(len(starts) == 2, f"expected 2 start events; saw {len(starts)}")
    expect(len(conts)  == 1, f"expected 1 continue event; saw {len(conts)}")
    expect(len(results) >= 3, f"expected >=3 result events; saw {len(results)}")
    for e in reqs:
        expected_uuid = str(uuid.uuid5(NAMESPACE, e["correlationid"]))
        expect(e["sessionuuid"] == expected_uuid,
               f"sessionuuid on {e['correlationid']} is {e['sessionuuid']}, expected uuid5={expected_uuid}")
    corr_notified = {n.get("title","").split(": ")[-1] for n in ntfs if n.get("event") == "message"}
    expect(corrA in corr_notified and corrB in corr_notified,
           f"ntfy stream missed corrA/corrB: saw titles for {corr_notified}")

    print(f"PASS  {len(reqs)} req  {len(rsps)} rsp  {len(ntfs)} ntfy events observed")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
```

### 6a.5 `NtfyWatcher` — pure stdlib

Because `requests` is banned, the ntfy JSON stream (verified format: `<topic>/json`, one JSON object per line, `event` field is `open`/`message`/`keepalive` per `docs.ntfy.sh/subscribe/api/`) is consumed via `urllib.request` reading chunked lines:

```python
# tests_e2e/watchers/ntfy_watcher.py
import json, threading, urllib.request

class NtfyWatcher(threading.Thread):
    def __init__(self, url):
        super().__init__(daemon=True)
        self.url = url
        self._buf, self._lock, self._stop = [], threading.Lock(), threading.Event()

    def run(self):
        req = urllib.request.Request(self.url, headers={"Accept":"application/x-ndjson"})
        with urllib.request.urlopen(req, timeout=None) as r:
            for raw in r:
                if self._stop.is_set(): return
                line = raw.decode().strip()
                if not line: continue
                try:
                    evt = json.loads(line)
                except json.JSONDecodeError:
                    continue
                with self._lock:
                    self._buf.append(evt)

    def stop(self): self._stop.set()

    def drain(self, timeout=1.0):
        import time
        time.sleep(timeout)
        with self._lock:
            out, self._buf = list(self._buf), []
        return out
```

Filter on `event=="message"` (ignore `open` and `keepalive`).

### 6a.6 `KafkaWatcher` — ad-hoc consumer group

Ad-hoc `group_id=e2e-<pid>-<topic>` so the test doesn't disturb EventBridge/EventRunner offsets. `auto_offset_reset="latest"` and a `since` millisecond filter so we ignore anything published before the test started:

```python
# tests_e2e/watchers/kafka_watcher.py
import json, os, threading
from kafka import KafkaConsumer
from cloudevents.v1.kafka import KafkaMessage, from_binary

class KafkaWatcher(threading.Thread):
    def __init__(self, topic, since):
        super().__init__(daemon=True)
        self.topic, self.since = topic, since
        self._buf, self._lock, self._stop = [], threading.Lock(), threading.Event()

    def run(self):
        c = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP","localhost:9092"),
            group_id=f"e2e-{os.getpid()}-{self.topic}",
            auto_offset_reset="latest",
            consumer_timeout_ms=500,
        )
        while not self._stop.is_set():
            for rec in c:
                if rec.timestamp < self.since:
                    continue
                km = KafkaMessage(headers={k:v for k,v in (rec.headers or [])},
                                  key=rec.key, value=rec.value)
                evt = from_binary(km, data_unmarshaller=json.loads)
                flat = {k: evt.get(k) for k in
                        ("type","correlationid","sessionuuid","mode","phase","final","sequence")}
                flat["data"] = evt.data
                with self._lock:
                    self._buf.append(flat)
                if self._stop.is_set(): break
        c.close()

    def stop(self): self._stop.set()

    def drain(self, timeout=1.0):
        import time
        time.sleep(timeout)
        with self._lock:
            out, self._buf = list(self._buf), []
        return out
```

### 6a.7 Skill mode (`--skill`)

`tests_e2e/skill_bridge.py` shells the skill CLI and parses stdout for the `correlationid: ...` line — exercises the whole path a human sees when driving from Claude Code:

```python
import re, subprocess

def start_via_skill(prompt: str) -> str:
    p = subprocess.run(
        ["uv","run","python",".claude/skills/eventbridge/eventbridge-cli.py",
         "run", prompt, "--watch=false"],
        check=True, capture_output=True, text=True, timeout=30,
    )
    m = re.search(r"correlationid: (\S+)", p.stdout)
    if not m:
        raise RuntimeError(f"skill did not print correlationid; stdout={p.stdout!r}")
    return m.group(1)
```

The e2e runner uses `--skill` mode only for the two `start` calls; continue is always over HTTP (the skill's `cont` subcommand does the same POST anyway).

### 6a.8 Running

```bash
# assumes EventBridge + EventRunner + Kafka + ntfy topic already configured
export NTFY_TOPIC=<your-ntfy-topic>
uv run rossoctl-e2e-test                # HTTP path
uv run rossoctl-e2e-test --skill        # exercises the skill wrapper too
```

Expected output:

```
* spawning watchers ...
* starting agents A and B ...
  A=wake-brave-otter-1234  B=cold-lazy-panda-5678
* waiting for both to reach final ...
  A final in 3.4s  result='4'
  B final in 4.1s  result='Otter'
* continuing agent B ...
  B(cont) final in 5.0s
* asserting channels ...
PASS  3 req  24 rsp  3 ntfy events observed
```

## 7. Project layout

```
rossoctl-keda1/
├── DESIGN_PHASE0.md              (this file)
├── AGENTS.md                     (session notes: ntfy on macOS)
├── pyproject.toml                (uv-managed; two console-scripts)
├── uv.lock
├── kafka/                        (Kafka 4.x tarball, KRaft server)
├── .claude/
│   └── skills/
│       └── eventbridge/
│           ├── SKILL.md
│           ├── eventbridge-cli.py     (stdlib-only helper)
│           └── OPENAPI.md
├── eventbridge/
│   ├── __main__.py
│   ├── config.py                 (env + tomllib + defaults)
│   ├── config.toml
│   ├── http_server.py            (wsgiref + ThreadingMixIn)
│   ├── router.py                 (regex routes → handlers)
│   ├── openapi.py                (OAS 3.1 dict + Swagger UI HTML)
│   ├── html_view.py              (server-rendered history template)
│   ├── correlation.py            (word-pair generator + regex)
│   ├── kafka_out.py              (KafkaProducer + to_binary)
│   ├── kafka_in.py               (KafkaConsumer + from_binary + sqlite writer)
│   ├── store.py                  (SQLite gateways for responses + sessions)
│   └── ntfy.py                   (urllib.request POST to ntfy)
├── eventrunner/
│   ├── __main__.py
│   ├── config.py
│   ├── config.toml
│   ├── consume.py                (Kafka consumer loop, hands off to router)
│   ├── router.py                 (per-correlationid AgentSlot + FIFO; see §4.5)
│   ├── runner.py                 (subprocess.Popen(claude…), --session-id/--resume)
│   └── emit.py                   (to_binary + KafkaProducer)
├── shared/
│   ├── ce.py                     (CloudEvent envelope helpers, extension names)
│   └── words/                    (adjectives.txt, animals.txt)
└── tests/
    ├── test_roundtrip_binary.py  (proves §2 wire contract)
    ├── test_correlation.py       (word-pair regex + collision guard)
    └── test_openapi.py           (OAS is valid 3.1)
```

---

## 8. Explicit non-goals (Phase 0)

Deferred to Phase 1+:

- **Kubernetes / KEDA** — no manifests, no scaler config.
- **CloudEvent signing / verification** — no JWS yet. Phase 1 adds detached signatures + EventRunner refusing unsigned events.
- **Causation binding** — no `causationid` cryptographically linking response → request; Phase 1 adds it.
- **Scope blocks** — no per-request capability envelope. Phase 0 uses `--permission-mode acceptEdits` (fine on a laptop, dangerous anywhere else).
- **Auth on EventBridge HTTP** — localhost-only bind. Do not expose to the LAN.
- **Multi-tenant ntfy** — one topic per developer.
- **Persistence beyond one machine** — SQLite is per-host.
- **Structured-mode CloudEvents** — binary mode only.
- **`http`-action credentials on the phone** — the "Continue…" action posts to localhost, which only works from a phone if you're on the same LAN with a tunnel. Not a production pattern.

## 9. Resolved open questions

Each item has been verified with either a documented source or a small experiment reproduced under `${TMPDIR%/}/claude-resume-experiment/` and `${TMPDIR%/}/sse_experiment.py`.

### Q&E-1 — Kafka client compatibility with free-threading ✅

**Resolved.** `kafka-python 3.0.11` (pure Python) keeps GIL disabled on `python3.14t`; `confluent-kafka` needs a source build and re-enables the GIL. Binary CloudEvent round-trip through local Kafka verified end-to-end.

### Q&E-2 — Can `correlationid` be the claude session id? ❌ / mid-turn design change ✅

**Answered.** No — `claude --session-id` **strictly validates a UUID**:

```
$ claude -p "…" --session-id wake-brave-otter-4718 …
Error: Invalid session ID. Must be a valid UUID.
$ echo $?
1
```

**Decision (superseded):** the design previously kept `sessionuuid` as a separate column in `sessions.sqlite`. **New design (§2.3):** derive `sessionuuid` deterministically via `uuid.uuid5(NAMESPACE, correlationid)`. One user-visible identifier, no mapping table on the hot path, both components arrive at the same UUID from the correlationid alone.

### Q&E-3 — Does `claude --resume` preserve context? ✅

**Verified.** Turn 1 with `--session-id <uuid> "prompt1"` → transcript at `~/.claude/projects/<encoded-cwd>/<uuid>.jsonl` (19 lines: user/assistant + latch/attachment/queue metadata). Turn 2 with `--resume <uuid> "prompt2"` → same transcript file grew to include both prompts and both responses. Session file:

```
~/.claude/projects/-Users-aslom-sandbox-rossoctl-keda1--tmp-claude-resume-experiment/f0508fc9-...jsonl
  9 user      "Reply with exactly the two characters OK and the digit 1…"
 10 assistant "OK1"
 15 user      "Reply with exactly the two characters OK and the digit 2…"
 16 assistant "OK2"
```

Note on `num_turns`: the `type=result` event's `num_turns` field counts **turns within the current invocation**, not cumulative session turns. Runner should not rely on it as a monotonic counter — use our own `sequence` extension attribute instead.

### Q&E-4 — Does `--no-session-persistence` break resume? ✅

**Verified.** Start with `--no-session-persistence` → transcript file **not** written. Attempting `--resume <uuid>` immediately after:

```
$ claude -p "…" --resume 37968536-e332-47ec-b78e-265a8d09e419 …
No conversation found with session ID: 37968536-e332-47ec-b78e-265a8d09e419
$ echo $?
1
```

**Design implication:** EventRunner **must not** pass `--no-session-persistence` on `mode=start` turns. Set it only for one-shot workloads that will never be resumed (out of scope for Phase 0).

### Q&E-5 — Can ntfy `http` actions confirm success? ✅ (implicitly)

**Verified in docs.** From `docs.ntfy.sh/publish/`, the `clear` parameter on an `http` action:

> "Clear notification after HTTP request succeeds. If the request fails, the notification is not cleared."

So the success/failure signal already exists — a `clear=true` action that succeeds removes the toast from the phone, a failure leaves it there. **Design implication:** the "Continue…" action in §3.6 gets `clear=true`. EventBridge does **not** need to send a follow-up ntfy notification to confirm success. If the user wants richer confirmation, EventBridge can still emit a separate notification when the resume request lands on `responses` with `phase=result` — that's already covered by the default `NTFY_PHASES=result,error`.

### Q&E-6 — SSE from stdlib wsgiref (no framework) ✅

**Prototyped.** Ran `${TMPDIR%/}/sse_experiment.py` — a 30-line stdlib-only WSGI server with `ThreadingMixIn` streams `text/event-stream` frames to a `urllib.request` client. Timings measured client-side:

```
[t+0.02s] data: tick 0
[t+0.42s] data: tick 1
[t+0.83s] data: tick 2
[t+1.23s] data: [done]
```

400 ms server-side sleeps landed as 400 ms client-side deltas — no framework buffering. **Design update:** replace `<meta http-equiv="refresh" content="2">` in §3.3 with an SSE endpoint at `GET /v0/agents/{correlationid}/events.sse` that the HTML view subscribes to via `new EventSource()`. Headers required: `Cache-Control: no-cache` and `X-Accel-Buffering: no` (the latter matters only behind nginx, but harmless everywhere else).

### Q&E-7 — `stream-json` shape: fresh vs `--resume` ✅

**Verified** via `${TMPDIR%/}/phase0-unknowns/exp1_stream_json_shape.py`. Both invocations emit exactly three top-level JSON events in the same order: `type=system`, `type=assistant`, `type=result`. The `result` event's key set is byte-for-byte identical between fresh and resumed invocations (25 keys including `api_error_status`, `duration_api_ms`, `duration_ms`, `first_content_frame_ms`, `is_error`, `modelUsage`, `num_turns`, `permission_denials`, `queued_turn_count`, `result`, `result_index`, `session_id`, `stop_reason`, `subagent_stats`, `subtype`, `terminal_reason`, `time_to_request_ms`, `total_cost_usd`, `ttft_ms`, `ttft_stream_ms`, `type`, `usage`, `uuid`).

`num_turns` is 1 in both cases — confirming it is per-invocation, not cumulative. **Design implication:** the runner ignores `num_turns` for progress tracking; the response CloudEvent's `sequence` extension attribute is the authoritative monotonic counter per correlationid.

### Q&E-8 — Concurrent `--resume` of the same session ✅

**Verified** via `${TMPDIR%/}/phase0-unknowns/exp2_concurrent_resume.py`. Two `claude -p --resume <same-uuid>` processes were spawned in the same instant, one asking for "OK2" and the other for "OK3":

- Both exit `0`. Both return the correct model reply (A→`OK2`, B→`OK3`). No error, no corruption.
- The on-disk transcript (`~/.claude/projects/<encoded-cwd>/<uuid>.jsonl`) grew from 19 → 32 lines (+13). Both `user` prompts and both `assistant` replies are present.
- **claude CLI serialized internally**: A's assistant reply landed at `T+2.7s`; B's at `T+9.1s` — B waited ~6.4s while A ran. The transcript contains `queue-operation` marker lines around both resume entrypoints, suggesting an in-process file lock or turn queue.
- **Ordering is nondeterministic.** Whichever process wins the internal race applies its turn first, and the loser's model context now includes the winner's turn. For prompts that are context-independent (like this experiment) the result is still "correct"; for a context-dependent second prompt ("what did I just ask?") the answer becomes racy.

**Design implication:** filesystem safety alone is not enough. EventRunner **must queue per-correlationid** so that ordering is deterministic, the user sees turns in the order they submitted them, and the model context evolves predictably. See §4.5.

## 10. Remaining unknowns (deferred)

1. ~~`--resume` output-format `stream-json` first-event shape~~ → resolved as Q&E-7 (§9).
2. ~~Concurrent `--resume` of the same session~~ → resolved as Q&E-8 (§9); routing design in §4.5.
3. **Phase 1 signature over the CloudEvent envelope** — JWS on the canonicalized event, extension attribute `ce_signature`. Explicitly out of scope for Phase 0 (see §8). Design deferred to Phase 1.
