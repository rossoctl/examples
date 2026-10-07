# README — Phase 0 end-to-end demo

Local signed-event wake demo, single machine, no Kubernetes.
Design in [`DESIGN_PHASE0.md`](DESIGN_PHASE0.md); build details in [`IMPLEMENTATION_REPORT0.md`](IMPLEMENTATION_REPORT0.md).

You'll drive an agent through the wire path

```
  HTTP  →  EventBridge  →  Kafka:requests  →  EventRunner  →  claude -p
                                                                   │
                                                                   ▼
   HTML view / SSE  ←  EventBridge  ←  Kafka:responses  ←  stream-json → CloudEvents
```

and watch every event both in a browser tab and (optionally) as ntfy pushes on your phone.

---

## 1. Prerequisites

Verified working on macOS 14+ / Darwin 27; Linux should work identically.

| Component | Install | Notes |
|---|---|---|
| Python 3.14 free-threading | `brew install python-freethreading` | `python3.14t -VV` must say *free-threading build* |
| uv (≥ 0.12) | `brew install uv` | Manages the venv; bypasses PEP 668 |
| Apache Kafka 4.x (KRaft) | already unpacked at `./kafka/` | see `AGENTS.md` §"Kafka on macOS from tarball" for one-time setup |
| Java 21 (only for Kafka's shell scripts) | `brew install openjdk@21` or `openjdk` | Python client speaks Kafka wire natively; no JVM in the hot path |
| `claude` CLI | already installed | `claude --version` should print 2.x |
| ntfy on a phone (optional) | Install ntfy app; subscribe to any random topic slug you choose | Only needed for §5 |

**One-time setup** (already done in this checkout):
```bash
uv venv --python 3.14t .venv        # creates .venv with free-threaded 3.14t
uv sync                              # installs kafka-python + cloudevents
```

Sanity check — GIL is off, deps import, Kafka is up:
```bash
.venv/bin/python -c 'import sys, kafka, cloudevents; print("gil:", sys._is_gil_enabled(), kafka.__version__, cloudevents.__version__)'
# → gil: False 3.0.11 2.2.0

nc -z localhost 9092 && echo kafka-up
```

If Kafka isn't up, start it first:
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk        # or: $(/usr/libexec/java_home -v 21)
export PATH="$JAVA_HOME/bin:$PATH"
kafka/bin/kafka-server-start.sh kafka/config/server.properties
```

---

## 2. Terminal layout

Five terminals are enough. `#4` is optional (ntfy), `#5` is the driver.

```
┌──────────────┬─────────────────────────────────────────────────────────────┐
│ Terminal 1   │ Kafka broker (localhost:9092)                                │
│ Terminal 2   │ EventBridge (HTTP :8080 + Kafka producer/consumer)           │
│ Terminal 3   │ EventRunner (Kafka consumer → claude subprocess or mock)     │
│ Terminal 4   │ ntfy subscription (Safari web app or phone)      [optional]  │
│ Terminal 5   │ Driver (curl, or the eventbridge Claude Code skill)          │
└──────────────┴─────────────────────────────────────────────────────────────┘
```

You can collapse 2+3 into a single tmux window; the split is only for legibility.

---

## 3. Start the services

### Terminal 1 — Kafka

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk
export PATH="$JAVA_HOME/bin:$PATH"
kafka/bin/kafka-server-start.sh kafka/config/server.properties
```

Wait for `[KafkaRaftServer nodeId=1] Kafka Server started`. Topics `requests` and `responses` were created earlier (verify with `kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`).

### Terminal 2 — EventBridge

```bash
uv run rossoctl-eventbridge
# → [eventbridge] pidfile: ${TMPDIR%/}/rossoctl-keda1/eventbridge.pid
# → [eventbridge] bootstrap=localhost:9092 http=127.0.0.1:8080 ntfy=off
# → [eventbridge] listening on http://127.0.0.1:8080  docs=/docs (Ctrl-C to stop)
```

Open <http://127.0.0.1:8080/docs> for Swagger UI, or <http://127.0.0.1:8080/healthz> for a liveness probe.

**Ctrl-C** exits cleanly (drains the Kafka consumer + closes the producer);
a **second Ctrl-C** forces an exit if cleanup gets stuck. If the process
started with a stale/live PID file it will refuse (see §7 troubleshooting).

### Terminal 3 — EventRunner

Two modes — pick one:

**Mock mode (recommended for a first run — spends zero API tokens):**
```bash
ER_MOCK_CLAUDE=true uv run rossoctl-eventrunner
# → [eventrunner] bootstrap=localhost:9092 max_concurrent=4 mock_claude=True
```
Mock mode returns a deterministic 3-event sequence per request (`system → assistant → result`) — same wire shape as real Claude, no subprocess, no API calls.

**Real Claude:**
```bash
uv run rossoctl-eventrunner
# → [eventrunner] pidfile: ${TMPDIR%/}/rossoctl-keda1/eventrunner.pid
# → [eventrunner] bootstrap=localhost:9092 max_concurrent=4 mock_claude=False
```
Each request spawns `claude -p --output-format stream-json ...`. You need `claude` on `PATH` and (implicitly) a valid Anthropic auth in your environment. Each turn costs real tokens.

**Stopping both daemons** — every process writes its PID to
`${TMPDIR%/}/rossoctl-keda1/<name>.pid` on startup and removes it on clean
exit. Fastest way to stop everything is the bundled helper:

```bash
scripts/stop-demo.sh
# → [eventbridge] sending SIGINT to 12345 ... exited
# → [eventrunner] sending SIGINT to 12346 ... exited
```

It escalates SIGINT → SIGTERM → SIGKILL if needed. Manual equivalent:

```bash
kill -INT "$(cat "${TMPDIR%/}/rossoctl-keda1/eventbridge.pid")"
kill -INT "$(cat "${TMPDIR%/}/rossoctl-keda1/eventrunner.pid")"
```

If a daemon crashed and left a stale PID file behind, the next startup will
automatically reclaim it (message: `reclaimed stale pidfile`). If one is
still running and you want to override anyway, `RUN_FORCE=1 uv run
rossoctl-eventbridge`.

Optional tuning:

| Env | Default | Meaning |
|---|---|---|
| `ER_MAX_CONCURRENT` | 4 | Max **distinct** correlationids running in parallel. Within one correlationid the Router serializes turns FIFO — see `DESIGN_PHASE0.md §4.5`. |
| `CLAUDE_BIN` | `claude` | Path to the CLI if not on `PATH` |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | |
| `ER_EMIT_SYSTEM_HOOKS` | `false` | Forward `SessionStart` hook frames as events — off by default (they're noise). |
| `ER_INCLUDE_RAW` | `true` | Include the full stream-json frame under `data.raw` on each response event — set `false` to shrink CloudEvent envelopes. |
| `ER_DEDUPE_FINAL_TEXT` | `true` | When claude's `type=result` frame carries the same `result:` text as the prior assistant message (its stream-json protocol always does this), strip `text` from the final event and mark it with `text_echoes_prior_assistant=true`. Turn off to keep both copies on the wire. |

**Env vars forwarded to the `claude` subprocess.** EventRunner does **not**
inherit its full environment to the child — it hands claude a curated dict.
If any of these are set when you launch EventRunner, they pass through:

| Var | Purpose |
|---|---|
| `ANTHROPIC_BASE_URL` | Route API traffic through a proxy (e.g. LiteLLM, cortex, on-prem gateway) |
| `ANTHROPIC_MODEL` | Override the default model without editing request payloads |
| `ANTHROPIC_AUTH_TOKEN` | Auth token for the base URL above (redacted in logs) |
| `ANTHROPIC_API_KEY` | Alternate auth mechanism (also redacted) |
| `CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS` | Turn off beta features in `claude` |

Everything else (`PATH`, `HOME`, `USER`, `TMPDIR`, `SHELL`, `LANG`, `LC_ALL`,
`TERM`, `LOGNAME`) is forwarded because claude needs it to find its session
store and print correctly; no other host env leaks into the subprocess.

At startup EventRunner logs which routing vars it will forward (secrets show
head + length, never the value):
```
[eventrunner] forwarding to claude subprocess: ANTHROPIC_BASE_URL=https://litellm.internal ANTHROPIC_AUTH_TOKEN=sk-a…(42 chars)
```
or, if nothing routing-related is set:
```
[eventrunner] no ANTHROPIC_*/CLAUDE_CODE_* env vars set — claude uses its own defaults
```

Typical launch through a routing proxy:
```bash
export ANTHROPIC_BASE_URL=https://litellm.internal/anthropic
export ANTHROPIC_AUTH_TOKEN=sk-your-litellm-token
export ANTHROPIC_MODEL=claude-sonnet-5
uv run rossoctl-eventrunner
```

---

## 4. Drive the demo (Terminal 5)

### 4.1 Via `curl` — the raw wire path

Start an agent and capture its correlationid into `$CORR` so every follow-up
snippet Just Works:
```bash
CORR=$(curl -sS -X POST http://127.0.0.1:8080/v0/agents \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Summarize threat-model1.md in 3 bullets.","max_turns":1}' \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["correlationid"])')
echo "started $CORR"
# → started salty-oyster-0639
```

The POST body echoes the full record — `correlationid`, `sessionuuid`
(=`uuid5(NAMESPACE, correlationid)`), `event_id`, `topic`, `html_url` — if
you want to see it, drop the `-c '...'` filter and pipe to
`python3 -m json.tool` instead.

Poll for events (server-side filtering by monotonic sequence):
```bash
curl -sS "http://127.0.0.1:8080/v0/agents/$CORR/events" | python3 -m json.tool
```

Open the HTML history view in a browser — it live-streams via SSE:
```bash
open "http://127.0.0.1:8080/v0/agents/$CORR"       # macOS
# or: xdg-open on Linux
```

Continue the same conversation (resumes the same claude session):
```bash
curl -sS -X POST "http://127.0.0.1:8080/v0/agents/$CORR/continue" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Now list mitigations for #2."}'
```

Watch the router serialize concurrent turns for one correlationid:
```bash
for tag in A B C; do
  curl -sS -X POST "http://127.0.0.1:8080/v0/agents/$CORR/continue" \
    -H 'Content-Type: application/json' \
    -d "{\"prompt\":\"CONT-$tag\"}" &
done; wait
sleep 3
# The events will land in three contiguous per-corr sequence blocks —
# A first, then B, then C — with no interleave, proving DESIGN §4.5.
curl -sS "http://127.0.0.1:8080/v0/agents/$CORR/events?since=3" | python3 -m json.tool
```

Fetch the raw CloudEvents (ndjson) — this is the shape KEDA / Knative /
downstream tooling would see on the responses topic in Phase 1:
```bash
curl -sS "http://127.0.0.1:8080/v0/agents/$CORR/events.jsonl"
```

Live-stream via SSE from the terminal:
```bash
curl -sN "http://127.0.0.1:8080/v0/agents/$CORR/events.sse"
```

Turn-grouped chat view — pairs each user prompt with its assistant reply so
a multi-turn conversation reads top-to-bottom:
```bash
curl -sS "http://127.0.0.1:8080/v0/agents/$CORR/turns" | python3 -m json.tool
# → { "correlationid": "…", "final": true, "turns": [
#     {"turn_index":1, "mode":"start",    "prompt":"…", "assistant_text":"…",
#      "stats":{"duration_ms":…, "total_cost_usd":…}, "event_count":3},
#     {"turn_index":2, "mode":"continue", "prompt":"…", "assistant_text":"…", …},
#     …
#   ]}
```
Add `?full=1` to include the flat event list under each turn.

The HTML view at `/v0/agents/$CORR` renders the same grouping visually:
each turn is a `<details>` block with USER/ASSISTANT paragraphs on top;
system frames, echoed finals, tool_use markers, and raw stream-json all
collapse behind a per-turn "Details (N events)" toggle. The most recent
turn opens by default; earlier ones stay collapsed to reduce scroll.

Extract just the model's reply — every response event carries
`data.text` up front so you never have to walk into `data.raw.message.content[0].text`:
```bash
# All assistant turns, one per line
curl -sS "http://127.0.0.1:8080/v0/agents/$CORR/events" \
  | python3 -c 'import json,sys
for e in json.load(sys.stdin)["events"]:
    d = e.get("data") or {}
    if d.get("role") == "assistant" and d.get("text"): print(d["text"])'

# Just the final result string + stats
curl -sS "http://127.0.0.1:8080/v0/agents/$CORR/events" \
  | python3 -c 'import json,sys
for e in json.load(sys.stdin)["events"]:
    d = e.get("data") or {}
    if d.get("role") == "final":
        print("text :", d.get("text"))
        print("stats:", d.get("stats"))'
```

**Note on repeated text.** Claude's `stream-json` protocol emits the model's
last reply twice by design — once in the `assistant` frame's
`content[0].text`, and again in its own `type=result` frame's `result:`
field (claude's summary). By default (`ER_DEDUPE_FINAL_TEXT=true`)
EventRunner strips `text` from the final event when it matches the
assistant text and marks it with `text_echoes_prior_assistant=true`; the
final card keeps its `stats` (num_turns, duration, cost, usage) and the
`raw` frame. The HTML view labels these cards "↑ echoes the previous
assistant message" and dims them. Set `ER_DEDUPE_FINAL_TEXT=false` to
preserve both copies for downstream audit tooling.

### 4.2 Via the `eventbridge` Claude Code skill

The skill wraps the same HTTP surface behind a Claude-Code-friendly CLI so
you can drive the demo from inside a Claude Code chat — either with the
`/eventbridge …` slash command or with natural-language phrases that Claude
Code will match against the skill's contract.

**Prerequisites:** Terminal 1 (Kafka), 2 (EventBridge), 3 (EventRunner) all
running from §3. The skill just talks HTTP to EventBridge on `:8080`.

**How Claude Code finds the skill.** The files at
`.claude/skills/eventbridge/{SKILL.md,eventbridge-cli.py,OPENAPI.md}` are
auto-discovered when you launch Claude Code inside this repo. Verify with
`/help` — you should see `eventbridge` in the skills list. The mapping from
natural language to skill actions (from `SKILL.md`):

| You say… | Skill does |
|---|---|
| "run an agent to \<task>" | `run` — POST /v0/agents, watch until final |
| "check on \<correlationid>" | `watch` — poll /events until final |
| "continue \<correlationid>: \<X>" | `cont` — POST /continue, watch until final |
| "show history \<correlationid>" | opens `http://…/v0/agents/<corr>` |

#### 4.2.1 End-to-end demo from inside Claude Code

**Start an agent** — either the slash form or plain English:

```
/eventbridge run "Summarize threat-model1.md in 3 bullets."
```
or just:
```
run an agent to summarize threat-model1.md in 3 bullets
```

Either way Claude Code invokes `eventbridge-cli.py run "…"` and streams the
response events inline as they arrive:

```
correlationid: wake-brave-otter-4718
[01 stdout] {"type":"system","session_id":"562d15ef-…"}
[02 stdout] {"type":"assistant","message":{"role":"assistant","content":[{"text":"Bullet 1: …"}]}}
[03 result] {"final":true,"exit_code":0,"duration_ms":8421,"result":"Bullet 1: …\nBullet 2: …\nBullet 3: …"}
```

**Continue the same conversation** (resumes the claude session — same
`sessionuuid`, sequences continue where they left off):

```
/eventbridge cont wake-brave-otter-4718 "Now list mitigations for #2."
```
or:
```
continue wake-brave-otter-4718: now list mitigations for #2
```

**Peek at an in-flight or finished run without restarting it:**

```
/eventbridge watch wake-brave-otter-4718
```
or:
```
check on wake-brave-otter-4718
```

**See the conversation as a chat log** (turn-grouped, user prompt + assistant reply per turn — hides system/init, echoed finals, and raw JSON):

```
/eventbridge chat wake-brave-otter-4718
```
or:
```
chat wake-brave-otter-4718
```

Prints:
```
correlationid: wake-brave-otter-4718  3 turns  final=True

— Turn 1 (start, 3.4s, $0.0900) —
USER:      Summarize threat-model1.md in 3 bullets.
ASSISTANT: 1. …
           2. …
           3. …

— Turn 2 (continue, 4.1s, $0.1100) —
USER:      Now list mitigations for #2.
ASSISTANT: …
```

**See the full history in the browser** — Claude Code will just call `open`
(macOS) / `xdg-open` (Linux) with the HTML view URL:
```
show history wake-brave-otter-4718
```

#### 4.2.2 Same skill CLI, driven from the shell

The CLI is a plain stdlib-only script — you can run it directly outside of
Claude Code (useful in a `Makefile`, CI, or when you want to script the
demo):

```bash
# Fire-and-forget (returns as soon as the correlationid is minted)
.venv/bin/python .claude/skills/eventbridge/eventbridge-cli.py run \
  "Summarize threat-model1.md in 3 bullets." --no-watch
# → correlationid: plain-otter-3272

# Run and stream response events until final
.venv/bin/python .claude/skills/eventbridge/eventbridge-cli.py run \
  "Name one animal starting with O."
# → correlationid: brave-otter-1183
# → [01 stdout] {"type":"system", …}
# → [02 stdout] {"type":"assistant", … "Otter"}
# → [03 result] {"final":true, …, "result":"Otter"}

# Resume that correlationid with a follow-up
.venv/bin/python .claude/skills/eventbridge/eventbridge-cli.py cont \
  brave-otter-1183 "Now one starting with P."

# Watch an existing correlationid to catch up on its events
.venv/bin/python .claude/skills/eventbridge/eventbridge-cli.py watch \
  brave-otter-1183
```

Chain into `$CORR` for shell composability:
```bash
CORR=$(.venv/bin/python .claude/skills/eventbridge/eventbridge-cli.py run \
  "Hello." --no-watch | awk '/^correlationid:/{print $2}')
echo "started $CORR"
```

#### 4.2.3 Point the skill at a different EventBridge

`EVENTBRIDGE_URL` overrides the default `http://127.0.0.1:8080` — useful
when EventBridge is on another host, or when you're running two instances
side-by-side for a Phase 1 comparison:

```bash
EVENTBRIDGE_URL=http://laptop.lan:8080 \
  .venv/bin/python .claude/skills/eventbridge/eventbridge-cli.py run "…"
```

Inside Claude Code, export the var in the shell that launched the session
(`export EVENTBRIDGE_URL=…` then start Claude Code) — the skill inherits
your env.

#### 4.2.4 What the skill does NOT do (yet)

- No streaming SSE mode — the CLI polls `/events?since=<seq>` every second.
  Response events still appear within ~1s of arrival. Use `curl -sN
  …/events.sse` (§4.1) for true push.
- No ntfy integration on the CLI side — notifications come from EventBridge
  itself when `NTFY_TOPIC` is set (§5). The skill just prints events to stdout.
- No signature verification — Phase 1 will add JWS + `ce_signature` checks.

---

## 5. Optional — ntfy push notifications

EventBridge fans out `phase=result` and `phase=error` events to ntfy (configurable). Set a topic (any random slug; treat as an unguessable secret since anyone with it can read notifications):

```bash
export NTFY_TOPIC=$(uuidgen | tr '[:upper:]' '[:lower:]')      # e.g. d4pnvco…
export NTFY_BASE_URL=https://ntfy.sh                            # or self-hosted
export NTFY_ENABLED=true
export EVENT_BRIDGE_PUBLIC_BASE_URL="http://$(ipconfig getifaddr en0):8080"
                                    # phone links open the HTML view;
                                    # used verbatim, so a Tailscale / SSH-forward
                                    # / port-forwarded loopback URL works too
uv run rossoctl-eventbridge                                     # restart with ntfy on
```

Subscribe on your phone (ntfy app → Add subscription → paste the topic slug) or in a browser tab (`https://ntfy.sh/$NTFY_TOPIC` — macOS Safari can be added to Dock so notifications keep working after the tab closes, see `AGENTS.md`).

Each `phase=result` publishes a **self-sufficient** notification — the body
carries everything you need, so a phone that can't reach your Mac still
gets useful information:

```
Title: agent brave-otter-4718 · 6.6s · $0.0470

❓ Say hi to Alek

💬 Hi Alek! What can I help you with?

⏱ 6.6s · 💸 $0.0470 · 🔤 812→24

📎 brave-otter-4718
```

- **Prompt** (`❓`) is looked up from the `prompts` table for this corr.
- **Assistant reply** (`💬`) comes from the classified event's `data.text`,
  falling back to the last stored `assistant` event when dedupe stripped it.
- **Stats** row is per-turn duration, cost, and input→output tokens.
- **Corr** at the bottom for reference.

`EVENT_BRIDGE_PUBLIC_BASE_URL` is used **verbatim** in every click / attach /
http-action field — no loopback filtering. Set it to whatever the phone can
reach: a LAN IP, a public hostname, a Tailscale MagicDNS address, or a
`127.0.0.1:8080` loopback if you're testing from the same machine or have
an SSH/port-forward in place. If you can't reach the URL from the phone the
message body still carries everything useful (prompt + reply + stats + corr),
so the notification is never empty.

`NTFY_PHASES=result,error` by default; add `stdout` if you want a notification per turn.

### 5.1 Making the phone-clickable URL work through Cisco AnyConnect (TUNNELALL) or any restrictive network

When your Mac is on **Cisco AnyConnect / Secure Client in `tunnel-all` mode** (common corporate policy), a phone on the same wifi *cannot* reach the Mac's LAN IP (`192.168.x.y:8080`). Two things happen simultaneously:

1. The Mac's default route is now the VPN tunnel, so replies to inbound LAN packets get routed *out the tunnel* instead of back to the phone.
2. AnyConnect can also install packet filters that drop inbound LAN traffic entirely (server-side "Local LAN Access" policy).

The client-side "Allow local LAN access" checkbox in AnyConnect does **nothing** when the corporate ASA/FTD headend disables it — verified by Cisco's own troubleshooting docs. `localhost` still works only because it never leaves `lo0`, which no VPN can touch.

EventBridge helps diagnose this at startup — you'll see a candidate list like:

```
[eventbridge] EVENT_BRIDGE_PUBLIC_BASE_URL='http://127.0.0.1:8080' — used verbatim in HTML view + ntfy Click/Actions.
[eventbridge] candidate addresses on this host (pick one and re-run with EVENT_BRIDGE_PUBLIC_BASE_URL=<url>):
    http://192.168.68.9:8080          [likely-lan        ] iface=en0
    http://9.111.38.119:8080          [likely-vpn        ] iface=utun4 ← current default route
    http://192.168.64.1:8080          [likely-docker     ] iface=bridge100
    http://127.0.0.1:8080             [loopback          ] iface=lo0
[eventbridge] note: under Cisco AnyConnect TUNNELALL, the phone will
[eventbridge]       fail to reach a LAN address because the Mac's return
[eventbridge]       traffic is routed via the VPN tunnel. Use a mesh (Tailscale)
[eventbridge]       or public tunnel (ngrok, cloudflared) in that case.
```

If the "current default route" is a `utun*` interface — that's the corporate VPN and you'll need a bypass. The rest of this section shows the two easiest ones.

### 5.2 ngrok — one-command public tunnel

**Install** (free tier gives a rotating `<random>.ngrok-free.app` hostname; paid gives a static one):

```bash
brew install ngrok/ngrok/ngrok        # macOS
# or download from https://ngrok.com/download

ngrok config add-authtoken <your-token>   # one-time, from ngrok.com/dashboard
```

**Expose port 8080** in a dedicated terminal (leave running):

```bash
ngrok http 8080
```

You'll see a session banner with the public URL, e.g. `https://a1b2c3.ngrok-free.app`. Anything hitting that URL is forwarded to your Mac's `127.0.0.1:8080` **regardless of VPN** — the tunnel dials outbound from your Mac, so it bypasses the TUNNELALL inbound-block problem entirely.

**Point EventBridge at it** and restart:

```bash
NGROK_URL="https://a1b2c3.ngrok-free.app"                # from the ngrok terminal
export EVENT_BRIDGE_PUBLIC_BASE_URL="$NGROK_URL"

# Restart EB so ntfy fan-outs and the HTML view pick up the new URL
scripts/stop-demo.sh
uv run rossoctl-eventbridge
```

Or automate the URL grab from ngrok's local admin API:

```bash
NGROK_URL=$(curl -s http://127.0.0.1:4040/api/tunnels \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["tunnels"][0]["public_url"])')
export EVENT_BRIDGE_PUBLIC_BASE_URL="$NGROK_URL"
uv run rossoctl-eventbridge
```

Now every ntfy notification's `click` / `attach` / `Continue…` action points at the ngrok URL, and tapping opens the HTML history view on your phone. Free-tier caveat: the hostname changes every time ngrok restarts, so re-export and restart EB whenever you restart ngrok.

**Ngrok paid tier** ($8/mo) gives you a **static domain** you can bake into your shell profile:

```bash
# One-time reservation at ngrok dashboard → Domains
ngrok http --domain=my-agent-demo.ngrok.app 8080

# In your shell profile:
export EVENT_BRIDGE_PUBLIC_BASE_URL=https://my-agent-demo.ngrok.app
```

### 5.3 Cloudflare Tunnel — free, static, no signup

Cloudflare's `cloudflared` gives you a free tunnel with a stable random hostname (`https://<random>.trycloudflare.com`) — no account required for quick tunnels:

```bash
brew install cloudflared              # macOS
cloudflared tunnel --url http://127.0.0.1:8080
```

Copy the `.trycloudflare.com` URL from cloudflared's output and set it:

```bash
export EVENT_BRIDGE_PUBLIC_BASE_URL=https://<random>.trycloudflare.com
scripts/stop-demo.sh && uv run rossoctl-eventbridge
```

Same tradeoff as ngrok free tier: hostname rotates on restart. For a stable hostname, use `cloudflared tunnel create` bound to a domain you own (also free).

### 5.4 Tailscale — mesh VPN alongside Cisco

If you and your phone both install **Tailscale** and log into the same account, your phone gets a `100.x.y.z` MagicDNS address for the Mac that works even while Cisco TUNNELALL is running (Tailscale traffic runs alongside, not through, the corporate tunnel):

```bash
brew install --cask tailscale
tailscale login
tailscale ip -4                       # e.g. 100.71.15.42

export EVENT_BRIDGE_PUBLIC_BASE_URL=http://100.71.15.42:8080
# or use MagicDNS name:
export EVENT_BRIDGE_PUBLIC_BASE_URL=http://laptop.tail1234.ts.net:8080
```

Recommended when the same Mac is used often — no per-restart hostname churn, works offline too (LAN-direct peers when on same wifi).

### 5.5 Self-test — which URL will actually work?

EventBridge runs a self-test on every startup and prints a verdict for every candidate URL:

```
[selftest] EVENT_BRIDGE_PUBLIC_BASE_URL candidates — verdict per URL:
    • http://127.0.0.1:8082          [reachable-same-host-only]
        · only reachable from this Mac; phones cannot use loopback
    ◐ http://192.168.68.9:8082       [reachable-lan-only]
        · ⚠ default route is via utun4 (VPN) — under Cisco TUNNELALL a phone's
          inbound SYN gets a reply routed out the VPN, so the phone times out
        · workaround: ngrok / cloudflared / Tailscale (see README §5.2–5.4)
    ◐ http://9.111.38.119:8082       [reachable-vpn-only]
        · VPN address — only reachable from inside that VPN
    ✗ http://192.168.64.1:8082       [not-listening]
        · check EB_HTTP_ADDR (bind 0.0.0.0 to reach from LAN)
```

You can also hit the test on demand from any shell:

```bash
# from the eventbridge skill CLI
python3 .claude/skills/eventbridge/eventbridge-cli.py selftest
# probe an ngrok URL you just started
python3 .claude/skills/eventbridge/eventbridge-cli.py selftest \
  --url https://a1b2c3.ngrok-free.app

# or raw JSON
curl -s http://127.0.0.1:8080/v0/selftest | jq .
curl -s "http://127.0.0.1:8080/v0/selftest?url=https://a1b2c3.ngrok-free.app" | jq .
```

Verdicts:

| Verdict | Meaning |
|---|---|
| `recommended` | Public tunnel that's live, OR LAN address with a plain LAN default route. Phone should reach this. |
| `reachable-lan-only` | LAN listens, but the default route is a `utun*` VPN — classic TUNNELALL trap; a phone on the same wifi will time out. Use a tunnel. |
| `reachable-vpn-only` | Only reachable from inside that VPN. |
| `reachable-same-host-only` | Loopback — same Mac only. |
| `not-listening` | Nothing answered on TCP connect. Either EB is bound to loopback only (set `EB_HTTP_ADDR=0.0.0.0:8080`), or the public-tunnel service is offline. |

### 5.6 Which tunnel?

| Situation | Recommended |
|---|---|
| One-off demo, no account | Cloudflare Tunnel (`cloudflared`) |
| Recurring dev, willing to sign up | ngrok (free tier for random URL, paid for static) |
| Multiple devices + long-term use | Tailscale |
| Phone is on the same LAN, no VPN | Just use the `likely-lan` candidate address printed at EB startup |

---

## 6. Verify the router guarantees

Run the unit tests any time you want to reprove the DESIGN §4.5 contract without touching Kafka or claude:

```bash
.venv/bin/python -m pytest tests/ -q
# → ..........................................................  58 passed in 7.27s
```

The four tests in `tests/test_router.py` cover FIFO ordering per correlationid, cross-correlation parallelism, the concurrency cap, and slot re-arm after drain.

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `curl: (7) Failed to connect to :8080` | EventBridge not up | Terminal 2 |
| `POST /v0/agents` hangs then 500 | Kafka broker unreachable | Check `nc -z localhost 9092`; restart Kafka in Terminal 1 |
| Events appear on `/events` but stop before `phase=result` | EventRunner not up, or wrong `CLAUDE_BIN` | Terminal 3; use `ER_MOCK_CLAUDE=true` to isolate wire issues from claude issues |
| `TypeError: thread function must be callable` | You wrote `self._bootstrap = …` on a `Thread` subclass. That name is reserved by `threading.Thread` on 3.14t | Rename the field (see `AGENTS.md`) |
| Two concurrent `/continue` land out of order | You bypassed the Router (e.g., wrote directly to Kafka with a non-`correlationid` key) | Always use EventBridge's `/continue` endpoint; router serializes per-corr |
| Legacy events on `requests` cause `KeyError` in EventRunner | Stale messages from prior runs | Consumer already skips malformed events with a log line; to purge, `kafka-consumer-groups --group eventrunner --reset-offsets --to-latest --topic requests --execute` (with the runner stopped) |
| SSE returns `A server error occurred` | You added `Connection: keep-alive` back into `handlers.py` | wsgiref rejects hop-by-hop headers; use only `Cache-Control` + `X-Accel-Buffering` |
| `claude -p --session-id wake-…` fails | claude requires a UUID, not our slug | Runner already derives `uuid5(NAMESPACE, correlationid)`; only pass through EventBridge, never the slug directly |
| Startup: `[eventbridge] already running as pid NNNN` | prior instance is alive | `kill -INT NNNN`, or `scripts/stop-demo.sh`, or `RUN_FORCE=1 uv run …` to overwrite |
| Startup: `[eventbridge] reclaimed stale pidfile` | prior instance crashed | informational — you're the new owner |
| Ctrl-C once doesn't exit | old bug: signal handler called `server.shutdown()` from the same thread `serve_forever()` was on. Fixed 2026-09-21 | update to current source; the fix runs `serve_forever` in a worker thread |
| HTML view keeps auto-reloading, textarea unusable | old bug: SSE replayed history on every subscribe + JS called `location.reload()` on final. Fixed 2026-09-21 | update to current source; SSE takes `?since=<seq>`, `final` auto-regroups in place via DOM patch (no reload) |
| Old correlationid shows `(prompt not recorded)` | correlation predates the `prompts` table | restart EB to run the Kafka `requests`-topic mirror; it re-scans the topic on every start and back-fills any prompt still in the retention window |

---

## 8. What comes next

- **Phase 1**: Swap `subprocess.Popen(claude)` in `eventrunner/runner.py` for a Kubernetes Job consumed by the same Kafka topic + CloudEvent contract; wire a KEDA `ScaledObject` on consumer lag; add JWS signatures over the CloudEvent envelope (`ce_signature` extension); bind response events to their triggering request via `causationid`.
- **Real-claude e2e harness (`rossoctl-e2e-test`)**: full multi-channel integration test described in `DESIGN_PHASE0.md §6a` — deferred until an API budget is agreed.

See `IMPLEMENTATION_REPORT0.md §4` for the current known-issues list.
