# Eventing — signed-event agent wake path

Event-driven plumbing that wakes a Claude Code agent from a Kafka message and
streams every step of its run back out as CloudEvents.

Phase 0 is a **local, single-machine demo** — no Kubernetes, no signatures yet.
It exists to pin down the wire contract (CloudEvent envelope, correlation
identity, per-conversation ordering) before any of it moves onto a cluster.

## The wire path

```text
  HTTP  →  EventBridge  →  Kafka:requests  →  EventRunner  →  claude -p
                                                                  │
                                                                  ▼
   HTML view / SSE  ←  EventBridge  ←  Kafka:responses  ←  stream-json → CloudEvents
```

A request enters over HTTP, becomes a CloudEvent on the `requests` topic, gets
picked up by EventRunner which spawns `claude -p --output-format stream-json`,
and each stream-json frame is republished to `responses`. EventBridge consumes
those back and serves them as JSON, ndjson, SSE, or a live HTML transcript.

## Components

| Path | Role |
|---|---|
| `eventbridge/` | WSGI HTTP front door (`:8080`), Kafka producer + consumer, SQLite event store, HTML/SSE views, ntfy fan-out, startup network self-test |
| `eventrunner/` | Kafka consumer that spawns `claude` per request and converts stream-json into response CloudEvents |
| `shared/` | CloudEvent envelope helpers, binary-mode Kafka binding, PID files, correlation word lists |
| `tests/` | 97 unit tests across 15 files — no Kafka or `claude` needed |
| `scripts/stop-demo.sh` | SIGINT → SIGTERM → SIGKILL both daemons via their PID files |
| `Dockerfile-eventbridge`, `Dockerfile-eventrunner` | One image per service, free-threaded CPython supplied by uv |
| `scripts/build-images.sh` | Multi-arch build (`linux/amd64,linux/arm64`) + push, or `--local` single-arch |
| `scripts/push-images.sh` | Pushes locally-tagged images (companion to `--local`) |
| `scripts/docker-e2e-test.sh` | Builds both images, stands up Kafka, drives one turn over a fresh host port |

Two console entry points, defined in `pyproject.toml`:

```bash
uv run rossoctl-eventbridge    # HTTP + Kafka bridge
uv run rossoctl-eventrunner    # claude executor
```

## Two design decisions worth knowing

**Correlation identity is human-readable, session identity is not.**
A run gets a slug like `brave-otter-4718` for humans, and `claude` gets
`uuid5(NAMESPACE, correlationid)` because it requires a real UUID. The slug
never reaches the CLI directly. Continuing a conversation reuses both, so
`claude` resumes the same session.

**Turns are FIFO per conversation, parallel across conversations.**
EventRunner's router gives each correlationid a slot and serializes its turns
in arrival order, while distinct correlationids run concurrently up to
`ER_MAX_CONCURRENT` (default 4). Three concurrent `/continue` calls on one
correlationid land as three contiguous, non-interleaved blocks. This is the
contract in [`DESIGN_PHASE0.md §4.5`](agentdocs/DESIGN_PHASE0.md), and
`tests/test_router.py` proves it —
FIFO order, cross-correlation parallelism, the concurrency cap, and slot
re-arm after drain.

## Quickstart

Requires Python 3.14 **free-threaded** (`python3.14t`), `uv`, a local Kafka 4.x
(KRaft) on `:9092` with topics `requests` and `responses`, and the `claude` CLI.

```bash
uv run rossoctl-eventbridge    # terminal 1
uv run rossoctl-eventrunner    # terminal 2
```

**Mock mode is the default when no credential is in the environment.** With
neither `ANTHROPIC_AUTH_TOKEN` nor `ANTHROPIC_API_KEY` set, EventRunner returns
a deterministic `system → assistant → result` sequence with the same wire shape
as real Claude — no subprocess, no API tokens. That makes a bare run useful
instead of failing on auth for every request, and it tells wire problems apart
from `claude` problems. The startup line always states the mode and why:

```text
[eventrunner] bootstrap=… mock_claude=True (auto: none of ANTHROPIC_AUTH_TOKEN/ANTHROPIC_API_KEY set)
```

An explicit `ER_MOCK_CLAUDE` always wins over the auto-detection. You need
`ER_MOCK_CLAUDE=false` in one common case: `claude` authenticated through
`claude login` rather than an API key, where there is no env var to detect.

Drive it:

```bash
CORR=$(curl -sS -X POST http://127.0.0.1:8080/v0/agents \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Name one animal starting with O.","max_turns":1}' \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["correlationid"])')

open "http://127.0.0.1:8080/v0/agents/$CORR"     # live HTML transcript
```

Stop everything with `scripts/stop-demo.sh`.

## HTTP surface

| Endpoint | Returns |
|---|---|
| `POST /v0/agents` | Start a run; mints the correlationid |
| `POST /v0/agents/{corr}/continue` | Another turn on the same claude session |
| `GET /v0/agents/{corr}` | Live HTML transcript, turn-grouped, SSE-backed |
| `GET /v0/agents/{corr}/events` | JSON events, `?since=<seq>` for incremental polling |
| `GET /v0/agents/{corr}/events.jsonl` | Raw CloudEvents as ndjson — the Phase 1 downstream shape |
| `GET /v0/agents/{corr}/events.sse` | True server push |
| `GET /v0/agents/{corr}/turns` | Prompt paired with reply per turn, plus cost/duration stats |
| `POST /v0/groups` | Create a batch and fan out its members; `Idempotency-Key` honoured |
| `GET /v0/groups` | Recent groups — HTML by default, JSON with `Accept: application/json` |
| `GET /v0/groups/{gid}` | Live batch progress page: percent, ETA, per-member rows |
| `GET /v0/groups/{gid}/status` | The JSON the CLI polls; `?full=1` embeds every member |
| `POST /v0/groups/{gid}/close` | Stop expecting more members (optionally revise `expected`) |
| `POST /v0/groups/{gid}/cancel` | Give up on the stragglers and complete now |
| `GET /v0/selftest` | Which public base URL a phone can actually reach |
| `GET /docs`, `GET /healthz` | Swagger UI, liveness |

Every response event carries `data.text` at the top level, so extracting a
reply never means walking into `data.raw.message.content[0].text`.

`POST /v0/groups` takes **`prompts`** — a flat list of strings, not member objects:

```bash
curl -X POST http://127.0.0.1:8080/v0/groups -H 'Content-Type: application/json' \
  -d '{"label":"hello-3","prompts":["Say Hello 1","Say Hello 2","Say Hello 3"]}'
```

Optional alongside it: `expected` (for an open group fed later — required when `prompts`
is omitted, and rejected when it contradicts `len(prompts)`), `label`, `min_success`,
`deadline_s`, `max_turns` (default 3), `model`, and an `Idempotency-Key` header so a
retried POST returns the same group rather than launching a second batch. Passing
`members[]` instead fails with `{"error": "give either prompts[] or expected"}`.

A batch sends exactly **two** ntfy notifications — one when it starts, one when it
finishes — no matter how many agents it contains; while a group is open its members'
own notifications are suppressed. Group state is a cache: `/data` is `emptyDir` in the
`test` and `kind` overlays, so on every start EventBridge replays the `responses` topic
to rebuild it (`[group-mirror] rebuilt N group(s)` in the log), which is what keeps a
notification's link working after a pod restart.

## Configuration

| Env | Default | Effect |
|---|---|---|
| `ER_MOCK_CLAUDE` | auto | Deterministic fake responses, zero API cost. Unset means "mock unless a credential is present"; an explicit `true`/`false` overrides that |
| `ER_MAX_CONCURRENT` | `4` | Distinct correlationids running in parallel |
| `ER_INCLUDE_RAW` | `true` | Keep the full stream-json frame under `data.raw` |
| `ER_DEDUPE_FINAL_TEXT` | `true` | Strip the duplicate final text that stream-json always emits twice |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Broker address |
| `CLAUDE_BIN` | `claude` | CLI path if not on `PATH` |
| `NTFY_TOPIC` / `NTFY_ENABLED` | off | Push `result`/`error` phases to a phone |
| `EVENT_BRIDGE_PUBLIC_BASE_URL` | — | Used verbatim in HTML and ntfy links |

Added in Phase 1:

| Env | Default | Effect |
|---|---|---|
| `ER_CONSUMER_GROUP` | `eventrunner` | Kafka consumer group. **Must equal the KEDA trigger's `consumerGroup`** — a mismatch is a silent no-scale failure |
| `ER_COMMIT_AFTER_TERMINAL` | `true` | Commit offsets only after the terminal event, so lag means "work not finished" and KEDA cannot scale a streaming pod away |
| `ER_MAX_REQUEST_AGE_S` | `3600` | Drop replayed requests older than this instead of re-running them. `0` disables |
| `ER_DRAIN_TIMEOUT_S` | `570` | How long SIGTERM waits for in-flight runs. Keep below `terminationGracePeriodSeconds` |
| `ER_HEARTBEAT_PATH` / `ER_HEARTBEAT_MAX_AGE_S` | `$TMPDIR/…/heartbeat`, `90` | Liveness heartbeat; `python -m eventrunner.healthcheck` is the probe |
| `ER_EVENTBRIDGE_URL` | — | Where to checkpoint transcripts and read the last sequence. Unset disables both |
| `ER_TRANSCRIPT_MAX_BYTES` / `EB_TRANSCRIPT_MAX_BYTES` | 32 MiB | Transcript size cap. A trivial one-turn transcript is already ~222 KB |
| `ER_KAFKA_RETRY_INITIAL_S` / `ER_KAFKA_RETRY_MAX_S` | `1` / `30` | Backoff bounds for the consumer's connect retry |
| `ER_REQUIRE_SIGNATURE` | `false` | Refuse unsigned/badly-signed requests |
| `ER_SIGNING_KEY_PATH` / `ER_VERIFY_KEY_PATH` | — | Ed25519 seed (hex, base64 or 32 raw bytes) |
| `EB_AUTH_TOKENS` | — (open) | `name:token` pairs guarding the two routes that create work. **Empty means no authentication**, and no manifest sets it — see below |

`EB_AUTH_TOKENS` deserves its own note because the default is fail-open. With it
unset — which is how every manifest in `k8s/` ships — an unauthenticated
`POST /v0/agents` returns 202 and spawns a `claude` run, and the OpenShift Route
makes that reachable from outside the cluster. Auth on the HTTP surface is
deferred to Phase 2 on purpose; until then, set this whenever the Route is
reachable by anyone you would not hand a shell to. It guards only
`POST /v0/agents` and `POST /v0/groups`; `GET`s, `/healthz`, `/docs`,
`PUT /transcript` and `/continue` stay open by design. Read only from the
environment, never from `config.toml`, which is committed. Deployment recipe and
the full list of what stays open: `agentdocs/README_PHASE1.md` §
"Submit-path authentication".

Two of these are load-bearing in ways that are easy to miss.
`ER_EVENTBRIDGE_URL` is what makes `/continue` survive a scale-to-zero *and* what
keeps event `sequence` numbers unique across pods — without it a second pod
restarts numbering at 1 and its rows overwrite the first turn's.
`ER_CONSUMER_GROUP` must match the ScaledObject exactly or KEDA measures a group
nobody joins.

#### Phase 3 — per-user isolation

Added in Phase 3 (`agentdocs/DESIGN_PHASE3.md`). Only the variables whose code is
actually in the tree — §9's steps 1–4. Every one of them is off or single-tenant by
default, so an existing deployment that upgrades and changes nothing behaves exactly
as it did:

| Env | Default | Effect |
|---|---|---|
| `EB_TENANCY_MODE` | `single` | `single` reproduces Phase 2: one topic pair, one store, no ownership checks. `multi` switches on per-user topics and requires an authenticated, registered caller |
| `EB_TOPIC_PREFIX` | `kev1` | First component of every per-user topic name, `{prefix}-{userkey}-requests`. Unused in `single` mode |
| `EB_USER_REGISTRY_PATH` | — | The approved-user registry (a ConfigMap, §2.5). **Required in `multi` mode, and an empty one denies everyone** — the other reading turns a missing file into an open door |
| `EB_MAX_OPEN_STORES` | `64` | LRU ceiling on open per-user SQLite stores. Two WAL connections per tenant (main + `-wal` + `-shm`), so a 1024 soft descriptor limit bounds this near 150–200 tenants |
| `ER_USERKEY` | — | Which tenant this runner serves. Stamped on every response so EventBridge can file it. **Required once `REQUEST_TOPIC` is not `requests`**; the runner refuses to start otherwise |
| `ER_AGENT_DIR` | `/etc/rossoctl/agents` | Where baked `AgentSpec`s live (§5.2) |
| `ER_AGENT_NAME` | `default` | Fallback agent when neither the request nor the registry names one. `default` with no file on disk is Phase 2's argv exactly |

Three notes on these. **`multi` mode is not yet isolation you should present as
such.**
Per-user stores *are* implemented (T5), so each tenant's sessions, responses and
transcripts live in their own SQLite files and a read routes to the owning tenant's store
— but nothing yet checks that the *caller* is that owner, because owner-scoped reads (T7)
and transcript authentication (T8) are not in. So in `multi` mode the HTTP reads,
`/continue` and `PUT /transcript` are still as open as Phase 2's. And even once they
land, separate topics without Kafka ACLs are *organisation*, not isolation;
`DESIGN_PHASE3.md` §3.6 and §10 are explicit about which claim each configuration earns.

**`multi` also does not work end to end yet.** Until T6 neither consumer subscribes to
the per-user topics, so no response is ever consumed: transcript pages stay empty, group
counters never advance, and nothing is logged. Treat it as something to develop against,
not to run.

**Adding a user is an operator action.** EventBridge has no Kubernetes client and will
not create topics, so a registry entry whose topics do not exist is a refusal, not an
auto-provision.

EventRunner does **not** hand its whole environment to the child process. It
forwards a curated set — `ANTHROPIC_BASE_URL`, `ANTHROPIC_MODEL`,
`ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_API_KEY`,
`CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS`, `CLAUDE_CONFIG_DIR`,
`CLAUDE_CODE_PROJECT_DIR_NAME`, plus the basics `claude` needs to find its session
store — and logs which routing vars it will pass, showing secrets as head + length
only. That makes routing through LiteLLM, cortex, or
an on-prem gateway a matter of exporting two variables.

## Containers

One image per service, built from this directory. `scripts/build-images.sh`
wraps both dockerfiles and produces a multi-arch manifest by default:

```bash
./scripts/build-images.sh                     # linux/amd64 + linux/arm64, pushed to quay.io/aslomnet/
./scripts/build-images.sh --local             # single-arch host build, loaded locally, not pushed
./scripts/build-images.sh --tag v0.1.0
REGISTRY_PREFIX=ghcr.io/aslom/ ./scripts/build-images.sh
```

Default output on the registry:

- `quay.io/aslomnet/rossoctl-eventbridge:dev` — OCI image index over
  `linux/amd64` and `linux/arm64`, with SLSA provenance and SBOM attestations
- `quay.io/aslomnet/rossoctl-eventrunner:dev` — same shape

Multi-arch requires `docker buildx` and an amd64 emulator. On macOS with
Apple Silicon, use **Rosetta** rather than QEMU — the JIT-to-arm64 path is
roughly an order of magnitude faster for CPU-bound steps like the CPython
install. Enable it once in Rancher Desktop under Preferences → Virtual
Machine → Emulation (VZ type + "Rosetta support") or in Docker Desktop
under Settings → General. The script probes on startup and warns if the
active emulator looks like slow QEMU rather than Rosetta.

The old, plain single-shot flow still works:

```bash
docker build -f Dockerfile-eventbridge -t rossoctl/eventbridge:dev .
docker build -f Dockerfile-eventrunner -t rossoctl/eventrunner:dev .
```

Both install a uv-managed free-threaded CPython — there is no `python:3.14t`
tag on Docker Hub to base on — and assert the GIL is off at build time. They
install only `[project.dependencies]` via `uv pip install -r pyproject.toml`
rather than the project itself: the source is copied in and run from `/app`, so a
built wheel buys nothing and skipping the build keeps the layer cache useful.

Image-specific defaults that differ from a local run:

| Setting | Why |
|---|---|
| `EB_HTTP_ADDR=0.0.0.0:8080` | The `127.0.0.1` default is unreachable from outside the container |
| `KAFKA_BOOTSTRAP=kafka:9092` | `localhost` means the container itself; `kafka` is the expected network alias |
| `TMPDIR=/data` (a volume) | Where the SQLite store, pidfiles and per-run work dirs land |

The runner image deliberately does **not** vendor the `claude` CLI — it is
distributed per platform and versioned separately. Mock mode means the image is
still useful on its own; for real runs, install the CLI in a derived image or
bind-mount one and set `CLAUDE_BIN`.

The end-to-end script builds both images, starts a single-node KRaft Kafka,
creates the topics, boots both services, and drives one full turn over a **new
host port** (18080, so it will not collide with a local demo on 8080):

```bash
./scripts/docker-e2e-test.sh              # build, assert, tear down
./scripts/docker-e2e-test.sh --port 19090
./scripts/docker-e2e-test.sh --keep       # leave it running to poke at
```

It asserts the mock-mode decision three ways directly against the image (no
credential → mock, credential → real, explicit override → real), then that
`/healthz` answers on the new port, that the startup log explains the chosen
mode, that a `MOCK-REPLY` event completes the round trip, and that
`events.jsonl` and `/turns` render it. Any credential in your shell is *not*
forwarded unless you pass `--real`, so the run stays free and deterministic.
Logs for every step land in `$TMPDIR/rossoctl-eventing-e2e/`.

## Tests

```bash
.venv/bin/python -m pytest tests/ -q
```

No Kafka, no broker, no `claude`. Coverage spans the router contract,
CloudEvent round-trips in binary mode, the SQLite store, correlation IDs, turn
grouping, graceful shutdown, PID file reclaim, OpenAPI generation, ntfy body
construction, and the network self-test.

## Remote access caveat

If the host is on a full-tunnel corporate VPN (Cisco AnyConnect TUNNELALL), a
phone on the same wifi **cannot** reach the host's LAN address — return traffic
routes out the tunnel. EventBridge detects this at startup and prints a verdict
per candidate address (`recommended`, `reachable-lan-only`,
`reachable-vpn-only`, `reachable-same-host-only`, `not-listening`). The fix is
a tunnel or mesh: `cloudflared` for a one-off, ngrok for recurring dev,
Tailscale for multiple devices long-term.

## Phase 1 — Kubernetes and KEDA

Phase 1 is implemented. The wire contract is unchanged; what changed is **where the
consumer runs and who decides how many of them there are**.

```text
  HTTP ─▶ EventBridge ─▶ Kafka:requests ─▶ EventRunner ─▶ claude -p
        (Deployment,                        (Deployment,
         replicas=1,                         replicas 0..6)
         public URL)                               ▲
                                                   │ scales on consumer lag
                                             KEDA ScaledObject
```

An idle demo consumes no agent capacity; a posted request wakes a pod in a few
seconds. EventBridge stays at one replica — something has to accept the POST and
hold the event store, so the claim is "no *agent* capacity while idle", not
"nothing is running".

What arrived with it:

- **KEDA `ScaledObject`** on consumer-group lag, `minReplicaCount: 0`, capped by the
  requests topic's partition count.
- **Offsets commit after the terminal event**, so lag means "work not finished" and
  KEDA cannot scale a streaming pod away mid-run.
- **A blocking Kafka retry loop and a heartbeat liveness probe.** The old failure was
  a live process with a dead consumer thread: the pod reported healthy and consumed
  nothing.
- **A graceful drain.** SIGTERM waits for in-flight `claude` runs and says on stderr
  what it is waiting for.
- **Transcript checkpointing** through EventBridge, so `/continue` resumes a session
  even after the pod that started it was scaled away. It rests on one verified fact:
  `claude --resume` accepts a *path* to a transcript `.jsonl`, not only a session id
  — see `scripts/verify_resume_by_path.py`.
- **Sequence continuity across pods.** A new pod would otherwise restart event
  `sequence` at 1 and overwrite the previous turn's stored rows.
- **`ce_causationid`** binding every response event to its triggering request, and
  **JWS signing** over the envelope (pure-Python Ed25519, feature-flagged off).
- **Python tooling.** Every script in `scripts/` is Python and stdlib-only; the
  historically documented entry points keep one-line `exec` shims.

Manifests live in `k8s/` (`base/`, `topics/`, `kind/`, `overlays/{test,kind,demo}`);
the design and build records are in [`agentdocs/`](agentdocs/README.md).
**Nothing secret is committed**: the demo overlay references a Secret that
`k8s_deploy.py` creates from your environment at deploy time.

---

### Three environments

| | What it proves | Needs |
|---|---|---|
| [Local](#1-local-no-containers) | the logic: 286 unit tests | Python 3.14t, `uv` |
| [Kind](#2-kind-local-kubernetes) | the whole KEDA story, no shared cluster, no VPN | Docker + `kind` |
| [OpenShift / ykt1](#3-openshift-ykt1) | the same, plus the arbitrary-UID and multi-node paths Kind cannot test | cluster access |

Docker-only (no Kubernetes) still works too — see [Containers](#containers) above for
`./scripts/docker-e2e-test.sh`.

---

### 1. Local (no containers)

**Setup**

```bash
uv venv --python 3.14t .venv
uv pip install --python .venv/bin/python kafka-python cloudevents pytest
.venv/bin/python -c 'import sys; print("GIL:", sys._is_gil_enabled())'   # -> False
```

**Tests**

```bash
PYTHONPATH=$PWD .venv/bin/python -m pytest tests/ -q

# Force mock mode. Do this if you have a real credential exported: with
# ER_MOCK_CLAUDE unset, config.load() auto-selects REAL claude when it sees
# ANTHROPIC_AUTH_TOKEN or ANTHROPIC_API_KEY.
ER_MOCK_CLAUDE=true PYTHONPATH=$PWD .venv/bin/python -m pytest tests/ -q
```

Expect **378 passed, 7 skipped** (the skips want a local Kafka) as of this branch —
measured on macOS 15 / arm64 with CPython 3.14.0rc1 free-threaded. The three
`test_cli_output.py` failures noted earlier no longer reproduce; they read a skill file
that lives outside this repository, so they depend on where the checkout sits.

**The one paid test.** The transcript-checkpoint design rests on `claude --resume`
accepting a path. This proves it for about two cents, and includes the negative
control that makes the result meaningful:

```bash
.venv/bin/python scripts/verify_resume_by_path.py
```

**The broker.** The Quickstart assumes a Kafka 4.x (KRaft) already on `:9092`; there is
no script for it. Without containers, from the Apache tarball and a JDK 17+:

```bash
tar xzf kafka_2.13-4.1.0.tgz && cd kafka_2.13-4.1.0
sed -i '' "s#^log.dirs=.*#log.dirs=$PWD/data#" config/server.properties   # /tmp may be unwritable
bin/kafka-storage.sh format -t "$(bin/kafka-storage.sh random-uuid)" \
  -c config/server.properties --standalone
bin/kafka-server-start.sh config/server.properties &

for t in requests responses; do
  bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \
    --create --topic $t --partitions 12 --replication-factor 1
done
```

12 partitions matches the Phase 1 overlays, so local and cluster runs stay comparable.
In `kafka-consumer-groups.sh --describe`, **lag means unfinished work, not lateness** —
the offset commit is deferred to the terminal event, which is what makes lag a usable
queue gauge during a batch.

**The demo** is unchanged from Phase 0 — see [Quickstart](#quickstart).

---

### 2. Kind (local Kubernetes)

**Setup.** A default `kind create cluster` is **not** sufficient: ingress-nginx
schedules with a `nodeSelector` on `ingress-ready=true` and binds host ports, so
without both in the cluster config nothing is reachable.
`k8s/kind/kind-cluster.yaml` supplies them.

```bash
python3 scripts/kind_setup.py --check      # what is missing, changes nothing
python3 scripts/kind_setup.py --recreate   # build the cluster from that config
python3 scripts/kind_setup.py              # install into an existing one
```

That installs exactly what a bare cluster lacks: ingress-nginx, Strimzi watching
namespace `kafka` plus a single-node Kafka named **`my-cluster`** (same name and
bootstrap DNS as ykt1, which is why `base/configmap.yaml` needs no change), and
KEDA.

**Kind has no CPU/memory knobs.** Each node is a container and the ceiling is the
Docker/Rancher/Colima VM; `cpu:`/`memory:` in a Kind config do nothing. A short VM
shows up as `Pending` pods or an OOMKilled broker, not as a Kind error.

| | CPU | Memory |
|---|---|---|
| Minimum | 4 | 8 GiB |
| Recommended | 6 | 12 GiB |
| Verified on | 8 | 24 GiB |

`kind_setup.py` reports what it found and fails below the minimum.

**Deploy and test**

```bash
KC=../../.kube/config-kind          # every script takes --kubeconfig

python3 scripts/k8s_preflight.py --kubeconfig $KC --overlay kind --yes
python3 scripts/k8s_deploy.py    --kubeconfig $KC --overlay kind --yes
python3 scripts/k8s_e2e_test.py  --kubeconfig $KC --overlay kind
python3 scripts/k8s_teardown.py  --kubeconfig $KC
```

Public URL: **`http://eventbridge.127.0.0.1.nip.io:30080`** (nip.io resolves
`*.127.0.0.1.nip.io` to loopback, so no `/etc/hosts` editing).

On Apple Silicon the cluster is arm64: `eventbridge`/`eventrunner` are multi-arch and
just work, but the derived `claude` image is amd64-only, so for the demo overlay
rebuild it natively with
`python3 scripts/build_images.py --with-claude --claude-platforms linux/arm64`.

---

### 3. OpenShift (ykt1)

**Setup.** Nothing to install — Strimzi, KEDA and the Route API are already there.
Just confirm you are pointed at the right cluster; preflight refuses to act on an
unacknowledged current-context, because deploying into the wrong cluster is the worst
outcome available here.

```bash
python3 scripts/k8s_preflight.py --yes              # 13 checks, each with its remediation
python3 scripts/k8s_preflight.py --context <ctx>    # or pin it explicitly
```

**Deploy and test**

```bash
python3 scripts/k8s_deploy.py   --yes                  # test overlay into kev1
python3 scripts/k8s_deploy.py   --yes --check-only     # is a deploy needed?
python3 scripts/k8s_e2e_test.py                        # 12 assertions, free, mock mode
python3 scripts/k8s_teardown.py                        # back to zero cost, deletes nothing
```

Deploy runs in a fixed order and **gates on the public URL**: the Route hostname is
published as `EVENT_BRIDGE_PUBLIC_BASE_URL` and then proven with a `GET /healthz`
from outside the cluster before anything else counts. The idle state it verifies is
**zero EventRunner replicas** — that is correct, not a failure.

---

### The full slow demo

The four-stage loop — zero → wake → stream → zero — asserted with timings rather
than narrated. Add `--kubeconfig $KC --overlay kind` for Kind.

```bash
# free and deterministic (mock mode), ~6 min
python3 scripts/k8s_demo_flow.py

# the same plus T1.10: 3 concurrent conversations across pods
python3 scripts/k8s_demo_flow.py --concurrency 3

# faster and weaker: skips the cold /continue and the replay guard
python3 scripts/k8s_demo_flow.py --skip-cold-continue --skip-idle-replay
```

| Stage | Asserted |
|---|---|
| 0 | 0 replicas, no lingering pod, `Active=False` before anything is posted |
| 1 | `Active=True` and replicas ≥ 1, **wake latency recorded** |
| 2 | a `phase=stdout` event, then a terminal one; replicas stayed ≥ 1 *for the whole run* |
| 2b | `/continue` on a warm pod; the transcript is checkpointed |
| 2c | `/continue` **after a full scale-to-zero cycle** — the Gap B gate |
| 3 | replicas return to 0; `Active=False` |
| T1.10 | `--concurrency N`: more than one pod, each `/continue` resuming its own session |
| T5.6 | a group whose offsets were deleted does not replay the backlog |

Watch it happen in a second window:

```bash
kubectl -n kev1 get deploy eventrunner -w
```

**The real-agent demo** needs a credential, the derived `claude` image, and real
mode. Nothing secret is committed — the values come from your environment and are
applied via stdin (not `--from-literal`, which would put the token in `ps` output):

```bash
export ANTHROPIC_BASE_URL=https://ete-litellm.ai-models.vpc.res.ibm.com
export ANTHROPIC_AUTH_TOKEN=<your-litellm-virtual-key>
export ANTHROPIC_MODEL=claude-haiku-4-5-20251001        # optional

python3 scripts/build_images.py --with-claude
python3 scripts/k8s_deploy.py    --yes --overlay demo
python3 scripts/k8s_demo_flow.py --overlay demo --concurrency 3
```

Use the **`vpc`** host, not `vpc-int`: `vpc-int` resolves to IBM-internal `9.x`
addresses that the cluster's AWS worker nodes cannot route — it works from a laptop
on the VPN and times out in a pod. The deploy script warns if you pass one.

Mock mode cannot demonstrate *retained context* (it emits a fixed reply), so stage 2c
and `--concurrency` only fully close their gates with a real agent.

---

### Driving it with the `/eventbridge` skill

The skill CLI works against any of the three environments. It defaults to
`http://127.0.0.1:8080`, so **pass `--base-url` when you mean a cluster** —
silently hitting localhost is the confusing failure mode.

```bash
# The normal case: set it once, then no flag on any command. A bare hostname is
# assumed https, so a Route host can be pasted straight in.
export EVENTBRIDGE_URL=eventbridge-kev1.apps.ykt1.hcp.res.ibm.com
python3 skills/eventbridge/eventbridge-cli.py run "Say hi"

# Or per command, when you mean a different target just this once
python3 skills/eventbridge/eventbridge-cli.py run "Say hi" \
  --base-url http://eventbridge.127.0.0.1.nip.io:30080      # kind
```

Precedence is `--base-url` > `$EVENTBRIDGE_URL` > `http://127.0.0.1:8080`, and
**the CLI reports which of the three it used on every run**, including the default:

```text
· endpoint https://eventbridge-kev1.apps.ykt1.hcp.res.ibm.com (from $EVENTBRIDGE_URL)
· endpoint http://127.0.0.1:8080 (from default)
```

That line used to be printed only when the endpoint was *not* the default, which is
backwards: the silent case was the one that needed announcing. A run that quietly went
to localhost reads exactly like "the agent never replied".

**There is no discovery step, by design.** The CLI is pure HTTP — it never invokes
`kubectl`, and neither should an agent driving it (`SKILL.md` says so explicitly, and a
test asserts the CLI cannot start a subprocess at all). If the endpoint is wrong or
down it says so and exits 2 with the fix, so nothing needs to pre-check `/healthz`:

```text
✖ cannot reach EventBridge at http://127.0.0.1:8080 ([Errno 61] Connection refused)
  endpoint came from: default
  No endpoint was given, so this used the laptop default. Point it at the
  right EventBridge — nothing here discovers one for you:
      export EVENTBRIDGE_URL=https://<eventbridge-host>   # whole session
      … --base-url https://<eventbridge-host>             # one command
```

**Every run echoes its own command first**, shell-quoted and pasteable, so a
transcript can be replayed by hand and a wrong flag is visible where it was used
rather than in the results:

```text
$ python3 skills/eventbridge/eventbridge-cli.py group run --template 'Say Hello {n}' --count 100 --label hello-100 --base-url https://eventbridge-kev1.apps.ykt1.hcp.res.ibm.com
```

`EVENTBRIDGE_ECHO=0` silences it; a `user:pass@` in any URL argument becomes `***@`
before printing.

**Submit, then poll — don't block.** A tool call (or a CI step) shows its output when
the command *exits*, so one long watching command reports nothing for minutes and then
everything at once, or gets killed at a timeout and reports nothing at all. Submit with
`--no-watch`, which returns in ~2 s with every correlationid, then check each one in its
own short bounded call:

```bash
$CLI runmany "Say hello." "Say world." --label agent1 --label agent2 --no-watch
$CLI watch sweet-crab-0956 --timeout 30      # seconds, not minutes
$CLI group status <gid>                      # one-shot counts, no waiting
```

Stdout is line-buffered explicitly, so partial progress survives a kill rather than
dying in an 8 KiB pipe buffer. And don't append `2>&1`: it does nothing for latency and
throws away whether a line was progress or `✖ cannot reach EventBridge`.

The skill lives in the repo at **`skills/eventbridge/`** and is exposed to Claude
Code through a symlink from the user's skills directory, so there is exactly one copy
to edit and it is versioned with the code it drives.

**Two agents at once — and why the order matters:**

```bash
python3 skills/eventbridge/eventbridge-cli.py runmany \
  "Say hello. One word." "Say world. One word." \
  --label agent1 --label agent2
```

`runmany` POSTs every prompt *before* it watches any of them, so N requests land on
the topic together, KEDA sees lag N and scales to N pods. Two sequential `run` calls
would each finish before the next was submitted — lag would never exceed 1, one pod
would serve both, and nothing would scale. Observed on ykt1:

```text
· endpoint https://eventbridge-kev1.apps.ykt1.hcp.res.ibm.com (from $EVENTBRIDGE_URL)
▸ agent1 grumpy-beetle-8286 · https://…/v0/agents/grumpy-beetle-8286
▸ agent2 gentle-manatee-0993 · https://…/v0/agents/gentle-manatee-0993
· 2 agent(s) submitted together — KEDA should scale to 2 pod(s)
agent1 ASSISTANT: MOCK-REPLY[Say hello. One word.]
agent2 ASSISTANT: MOCK-REPLY[Say world. One word.]
✔ 2/2 agent(s) finished

spec_replicas=1 pods=0     # KEDA activates
spec_replicas=2 pods=2     # then scales to the lag
```

Both turns produced their own ntfy notification.

**Setup needed before running the skill: just the endpoint.** The CLI is
stdlib-only (no venv), `NTFY_TOPIC` is a property of the *deployment* rather than of
this client, and mock-vs-real is decided by the EventRunner deployment. `kubectl` is
only needed if you want to watch pods.

**Two URLs, easily confused.** `--base-url` / `EVENTBRIDGE_URL` is *client side* —
where the CLI sends requests. `EVENT_BRIDGE_PUBLIC_BASE_URL` is set *on the
deployment* and is what EventBridge advertises in HTML links and ntfy actions.
Changing one does not change the other.

Against the `test` and `kind` overlays every reply is `MOCK-REPLY[...]` — those
overlays mount no credential, so mock mode is auto-selected. That is expected.

### Demo scenario: three agents, then a batch of 100

Two runs, back to back, that show the one behaviour people get wrong when they first
see this: **whether a run is part of a batch changes how many notifications your phone
gets.** Three loose agents send three notifications. A hundred agents in a batch send
two. Run them in this order — the contrast is the demo.

Everything below was executed on ykt1 on 2026-09-23 against the `test` overlay (mock
agents, so it costs no tokens). The numbers in the output blocks are measured, not
illustrative.

```bash
cd examples-aslom/eventing
CLI="python3 skills/eventbridge/eventbridge-cli.py"

# Point the CLI at the cluster once. Everything below then needs no --base-url, and
# the CLI echoes `(from $EVENTBRIDGE_URL)` on every run so the target is never in doubt.
export EVENTBRIDGE_URL=eventbridge-kev1.apps.ykt1.hcp.res.ibm.com

# The baseline to compare against afterwards. An ntfy topic is a capability, so this
# reads your own topic — substitute yours.
curl -s "https://ntfy.sh/$NTFY_TOPIC/json?poll=1&since=all" | wc -l
```

Only the pod-watching step later on needs `kubectl` (and a
`KUBECONFIG=…/config-ykt1`); the CLI itself never touches a cluster API.

#### Part 1 — three separate agents, three notifications

Ask the skill for it in words:

> **on ykt1, run agent1 to say hello, agent2 to say world and agent3 to say again,
> one word each**

which it turns into exactly one command — `runmany`, never three `run` calls:

```bash
$CLI runmany "Say hello. One word." "Say world. One word." "Say again. One word." \
  --label agent1 --label agent2 --label agent3
```

```text
$ python3 skills/eventbridge/eventbridge-cli.py runmany 'Say hello. One word.' 'Say world. One word.' 'Say again. One word.' --label agent1 --label agent2 --label agent3
· endpoint https://eventbridge-kev1.apps.ykt1.hcp.res.ibm.com (from $EVENTBRIDGE_URL)
▸ agent1 patient-whale-1324  · https://…/v0/agents/patient-whale-1324
▸ agent2 bitter-leopard-8584 · https://…/v0/agents/bitter-leopard-8584
▸ agent3 cold-puma-7650      · https://…/v0/agents/cold-puma-7650
· 3 agent(s) submitted together — KEDA should scale to 3 pod(s)
agent3 ASSISTANT: MOCK-REPLY[Say again. One word.]   agent3 ✔ done · 1.4s · 20→32 tokens
agent1 ASSISTANT: MOCK-REPLY[Say hello. One word.]   agent1 ✔ done · 1.4s · 20→32 tokens
agent2 ASSISTANT: MOCK-REPLY[Say world. One word.]   agent2 ✔ done · 3.9s · 20→32 tokens
✔ 3/3 agent(s) finished
```

**What to verify — exactly 3 notifications, each pointing at its own page:**

```text
title : agent cold-puma-7650 · 1.4s
click : https://…/v0/agents/cold-puma-7650
body  : ❓ Say again. One word.  💬 MOCK-REPLY[Say again. One word.]
title : agent patient-whale-1324 · 1.4s      click: …/v0/agents/patient-whale-1324
title : agent bitter-leopard-8584 · 3.9s     click: …/v0/agents/bitter-leopard-8584
```

And **no batch exists** — that is the half of this part that is easy to skip:

```bash
for c in patient-whale-1324 bitter-leopard-8584 cold-puma-7650; do
  curl -s -o /dev/null -w "$c HTTP %{http_code}\n" "https://$EVENTBRIDGE_URL/v0/agents/$c"
  curl -s "https://$EVENTBRIDGE_URL/v0/agents/$c" | grep -c '<div class="groupback">'   # 0
  curl -s "https://$EVENTBRIDGE_URL/v0/agents/$c/turns" | grep -o '"groupid": *[^,]*'   # null
done
curl -s -H 'Accept: application/json' "https://$EVENTBRIDGE_URL/v0/groups"   # count unchanged
```

```text
patient-whale-1324     HTTP 200 · groupback divs: 0 · groupid: None
bitter-leopard-8584    HTTP 200 · groupback divs: 0 · groupid: None
cold-puma-7650         HTTP 200 · groupback divs: 0 · groupid: None
groups: 15  (15 before, so no group was created)
```

Three notifications for three agents is the right behaviour at this scale, and the
reason batches exist: it does not survive being multiplied by 100.

The same watcher as Part 2 (below) shows KEDA following the lag exactly — and catches a
trap worth seeing once, at `t=16s`:

```text
t=6s   replicas=1 running=0     # KEDA activates
t=8s   replicas=3 running=0     # scales to the lag: 3 requests, 3 pods
t=12s  replicas=3 running=1
t=16s  replicas=0 running=2     # ← spec.replicas is 0 while two pods are still running
t=20s  replicas=0 running=0
```

`spec.replicas == 0` does **not** mean no consumer is running: a pod lives until it
exits or `terminationGracePeriodSeconds` elapses, and until then it is still a group
member consuming from the topic. Any "is it idle yet?" assertion has to require zero
replicas *and* no surviving pod.

#### Part 2 — one batch of 100 agents, two notifications

The simplest phrasing that gets there — no mention of groups, templates or counters:

> **on ykt1, run a batch of 100 agents, each saying Hello N**

"batch" (or "as a group", or "100 agents") is what selects `group run`; "each saying
Hello N" is what becomes the template. One command, and it monitors itself:

```bash
$CLI group run --template "Say Hello {n}" --count 100 --label hello-100-demo
```

`{n}` is 1-based (`{i}` is 0-based, `--start` shifts it). Do not pass 100 positional
prompts — the CLI echoes the first and last generated prompt so the pattern is
reviewable before anything runs, which 100 shell-quoted arguments are not. Watching is
the default, so progress lands in the transcript with no second command:

```text
$ python3 skills/eventbridge/eventbridge-cli.py group run --template 'Say Hello {n}' --count 100 --label hello-100-demo
· endpoint https://eventbridge-kev1.apps.ykt1.hcp.res.ibm.com (from $EVENTBRIDGE_URL)
▸ group hello-100-demo bold-hornet-2962 · https://…/v0/groups/bold-hornet-2962
  all groups · https://…/v0/groups
· 100 agents submitted together (from template 'Say Hello {n}')
  first: 'Say Hello 1'   last: 'Say Hello 100'
  0/100 done · 100 queued · 8s elapsed
  4/100 done · 4 running · 92 queued · 24s elapsed · about 2 minutes left
  41/100 done · 16 running · 43 queued · 45s elapsed · less than a minute left
  88/100 done · 5 running · 7 queued · 1m00s elapsed · less than a minute left
  100/100 done · 1m20s elapsed
✔ group complete · 100 finished · 0 failed · 1m20s
```

**What to verify — exactly 2 notifications, both pointing at the group page:**

```text
title : group hello-100-demo started
click : https://…/v0/groups/bold-hornet-2962
body  : 🚀 100 agent(s) submitted · 📎 bold-hornet-2962

title : group hello-100-demo · 100 done
click : https://…/v0/groups/bold-hornet-2962
body  : ✅ 100 finished · 🎯 of 100 expected · ⏱ 1m20s · 🐢 slowest agent 79.6s
```

Not 100, and not 102: while a group is open its members' own notifications are
suppressed, keyed on **membership** rather than on the group still being open — so a
redelivered request whose duplicate terminal event arrives *after* completion stays
quiet too. Each member page gains a link back the other way:

```bash
curl -s "https://$EVENTBRIDGE_URL/v0/groups/bold-hornet-2962/status" | head -c 300
curl -s "https://$EVENTBRIDGE_URL/v0/agents/fresh-mole-0029" | grep -o '<div class="groupback">.*</div>'
```

```text
label hello-100-demo · finished/expected 100/100 · reason: all · members 100
<div class="groupback">← back to group <a href="/v0/groups/bold-hornet-2962">hello-100-demo</a> <code>bold-hornet-2962</code></div>
```

**And the fan-out, in a second terminal while Part 2 runs:**

```bash
while true; do
  printf '%s replicas=%s running=%s\n' "$(date +%T)" \
    "$(kubectl -n kev1 get deploy eventrunner -o jsonpath='{.spec.replicas}')" \
    "$(kubectl -n kev1 get pods -l app.kubernetes.io/name=eventrunner \
        --no-headers 2>/dev/null | grep -c Running)"
  sleep 3
done
```

Note the selector is `app.kubernetes.io/name=eventrunner`; a plain `app=eventrunner`
matches nothing and reports a silent `0` throughout. Measured:

```text
t=6s   replicas=10   # the explicit HPA scaleUp policy jumps straight to the cap
t=12s  replicas=10 running=1
t=30s  replicas=10 running=8
t=33s  replicas=0    # the batch drained
```

Reaching the cap in one step rather than climbing `1 → 5 → 10` over ~30 s is the
`behavior.scaleUp` policy in `k8s/base/eventrunner-scaledobject.yaml` doing its job:
KEDA handles 0 → 1 but delegates 1 → N to an HPA, whose default policy adds at most
`max(100%, 4 pods)` per 15 s.

`maxReplicaCount` is **10**, under the topic's **12** partitions. Partitions are the
real ceiling — Kafka gives each partition to exactly one consumer in a group, and KEDA
enforces the same bound by default (`allowIdleConsumers: false`) — so the cap can be
lowered freely but raising it past 12 would mean raising partitions first, which is a
one-way door. With 10 consumers over 12 partitions, two of them simply own two
partitions each.

#### The summary to read back

| | Part 1 — three loose agents | Part 2 — a batch of 100 |
|---|---|---|
| Command | `runmany "…" "…" "…"` | `group run --template … --count 100` |
| ntfy notifications | **3** — one per agent | **2** — group started, group finished |
| Each notification links to | that agent's own page | the group page |
| Group created | none (`groupid: null`) | one, with 100 member rows |
| Wall clock | ~4 s | ~1m20s |
| Reproduced | twice, delta 3 both times | delta 2 |
| Peak `spec.replicas` | 3 | 10 (the cap) |

If Part 2 sends more than two notifications, the suppression regressed. If a group
page 404s after a pod restart, check the log for `[group-mirror] rebuilt N group(s)` —
`/data` is `emptyDir` in this overlay, so group state is rebuilt from the `responses`
topic on every start.

### Phone notifications (ntfy)

Off by default. An **ntfy topic name is a capability** — anyone who knows it can read
*and* publish your notifications — so it is never committed. Export it and deploy:

```bash
export NTFY_TOPIC=<your-ntfy-topic>
export NTFY_TOKEN=<token>                 # only for a protected topic
python3 scripts/k8s_deploy.py --yes
```

**Running locally, `NTFY_TOPIC` alone is not enough — set `NTFY_ENABLED=true` too.**
`eventbridge/config.toml` ships `enabled = false`, and the env override derives its own
default from whatever the file loaded (`e("NTFY_ENABLED", "true" if cfg.ntfy.enabled
else "false")`), so an unset `NTFY_ENABLED` leaves ntfy off no matter what the topic is.
The startup banner is the check — it must say `ntfy=on`:

```bash
NTFY_ENABLED=true NTFY_TOPIC=$NTFY_TOPIC \
  EVENT_BRIDGE_PUBLIC_BASE_URL=http://<lan-ip>:8080 \
  .venv/bin/python -m eventbridge | grep 'ntfy='
# [eventbridge] bootstrap=… http=… ntfy=on
```

With `ntfy=off` nothing is sent and nothing is logged, which looks exactly like a
delivery failure. Also set `EVENT_BRIDGE_PUBLIC_BASE_URL` to a LAN address: the default
`127.0.0.1` still delivers a fully readable notification, but the click-through and the
`Continue…` action button resolve to loopback and do nothing from a phone.

`k8s_deploy.py` stores it as `Secret/eventbridge-ntfy`, which the base Deployment
references with `optional: true` — listed after the ConfigMap so its
`NTFY_ENABLED=true` overrides the ConfigMap's `"false"`. Confirm with:

```bash
kubectl -n kev1 logs deploy/eventbridge | grep ntfy      # -> ntfy=on
curl -s "https://ntfy.sh/$NTFY_TOPIC/json?poll=1&since=10m"
```

Each notification carries the prompt, the reply, a stats line and a click-through to
the correlation's HTML view at `EVENT_BRIDGE_PUBLIC_BASE_URL`.

Both images install `ca-certificates` **and** symlink `/etc/ssl/cert.pem` to it,
because the bundled standalone CPython looks for the former path while Debian writes
the latter. Without both, every outbound HTTPS call fails with
`CERTIFICATE_VERIFY_FAILED` and ntfy silently delivers nothing.

### Watching the topics during a demo

`scripts/watch_topics.py` is **cluster-only** — it locates a broker by the Strimzi
selector `strimzi.io/name=my-cluster-kafka` and aborts against a local broker with
`[watch] ABORT no Kafka broker pod matching …`. For a local run, point the Kafka CLI at
`127.0.0.1:9092` directly (see [Local](#1-local-no-containers)).

Your `kafka-get-offsets.sh` pipeline is right — `--topic` does accept a regex in Kafka
4.x. But **`watch` is not in the Strimzi broker image** (nor is `clear`), so running it
inside the pod fails with `bash: line 1: watch: command not found`.

Run `watch` **locally** and let `kubectl exec` run only the Kafka tool. No remote shell
means no nested quoting, and `awk` runs on your machine:

```bash
watch -n 2 "kubectl -n kafka exec my-cluster-dual-role-0 -- \
  bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
    --topic '^(kev1-requests|kev1-responses)\$' \
  | awk -F: '{s[\$1]+=\$3} END {for (t in s) print t, s[t]}' | sort"
```

If you would rather stay inside the pod, loop by hand — `printf` replaces the missing
`clear`, and the awk program is double-quoted so `\$1` survives the outer single quotes:

```bash
kubectl -n kafka exec my-cluster-dual-role-0 -- bash -c '
while true; do
  printf "\033[H\033[J"; date -u +%H:%M:%SZ
  bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
    --topic "^(kev1-requests|kev1-responses)$" \
  | awk -F: "{s[\$1]+=\$3} END {for (t in s) print t, s[t]}" | sort
  sleep 2
done'
```

Two caveats on what that number means. It sums **end offsets**, which is records ever
produced, not records still present — subtract `--time -2` (earliest) once retention
starts expiring the head. And it says nothing about how much is *done*.

For the demo, `scripts/watch_topics.py` shows both, redrawing every 2 s in place:

```bash
python3 scripts/watch_topics.py                      # topics + group lag
python3 scripts/watch_topics.py --group <groupid>     # + per-member running/queued
python3 scripts/watch_topics.py --once --no-color     # one snapshot, for logs
```

```text
topics   (end − earliest = records still retained)
  TOPIC               RECORDS  PRODUCED  PARTS   RATE/s
  kev1-requests           435       435     12       +8.0
  kev1-responses         2331      2331     12      +24.0

consumer group kev1-eventrunner (active)
  lag (work not finished)             62   RQ-1 defers the commit to the terminal event,
    ├─ claimed, in flight             62   so lag means unfinished, not merely unread
    └─ not claimed yet                 0
  partitions owned by a member        12 / 12
```

#### Tracking status, not just counts

Kafka stores **committed** offsets and never a live consumer's in-memory position, so
there is no server-side "claimed and half-processed" flag to read. But two facts
together give you the split:

1. `kafka-consumer-groups.sh --describe` prints a **CONSUMER-ID** per partition, or `-`
   when no live member owns it. So lag on an *assigned* partition is claimed work;
   lag on an *unassigned* partition is not claimed by anyone yet.
2. In this system **lag already means "not finished"**, not "not read" — RQ-1 defers the
   offset commit until the agent's terminal event, so a record stays uncommitted for the
   whole run. That is also precisely the number KEDA scales on.

Captured live during an 80-agent batch, the two states are clearly distinguishable:

| moment | lag | claimed | not claimed | partitions owned |
|---|---|---|---|---|
| just after the POST | 80 | 0 | **80** | 0/12 |
| pods joined the group | 62 | **62** | 0 | 12/12 |
| draining | 16 | **16** | 0 | 12/12 |

For the exact **running vs queued** split, EventBridge is authoritative rather than
Kafka: it tracks per-member state instead of inferring it from offsets, which is what
`--group <groupid>` adds to the display. Kafka can tell you *how much* is unfinished and
whether anyone owns it; only the application knows which individual agents are mid-run.

### Measured

| | ykt1 | Kind |
|---|---|---|
| e2e | **49/49** | **37/37** |
| POST → KEDA scales up | 1.7–3.0 s | 2.2–4.3 s |
| POST → pod Ready | ~14 s | — |
| POST → back to zero | **22 s** (cooldown 5) | — |
| Concurrency (2–3 correlations) | peak 2–4 replicas | — |

Where the wake time actually goes, measured from a genuinely idle state:

```text
POST ─1.7s─▶ KEDA decides to scale    (pollingInterval: 5)
     ─14s──▶ pod Ready                (image pull + free-threaded CPython init)
     ─22s──▶ back to zero             (cooldownPeriod: 5, after the run finished)
```

KEDA's *detection* was never the slow part. The `test` and `kind` overlays use
`cooldownPeriod: 5` (down from 30), which took the whole loop from 46.5 s to 22 s —
the 30 s had been an idle pod lingering, not a wake delay. Shortening it is safe
because RQ-1 commits the offset only after the terminal event, so lag stays ≥ 1 for
the whole run and KEDA cannot scale away a pod that is still working.

The remaining ~12 s is pod startup, which no KEDA setting affects. Pre-pulling the
image onto nodes is the lever there.

RQ-4 assumed a cold start under 10 s; the scale-up decision beats that comfortably,
while *pod Ready* does not on a node that has to pull a 245 MB image. The longest run
observed was a real-agent turn stuck in 429 retry backoff for 300 s, which is also the
best evidence for RQ-1: replicas stayed ≥ 1 for all 300 s.

Kind sets `group.initial.rebalance.delay.ms: 0`; ykt1 leaves it at 3000 ms and Phase 1
does not change a broker-wide setting on a shared cluster. So Kind timings are **not**
comparable to cluster timings.

Still deferred to Phase 2: Job-per-request isolation for a per-run capability
envelope, non-idempotent agent support, auth on EventBridge's HTTP surface,
multi-replica EventBridge, and TLS to Kafka.

## Detail docs

The long-form designs, build reports and full runbooks live in
**[`agentdocs/`](agentdocs/)**, which has [its own index](agentdocs/README.md). They
are the authoritative references this file summarizes:

| | |
|---|---|
| [`agentdocs/DESIGN_PHASE0.md`](agentdocs/DESIGN_PHASE0.md) | the wire contract, the `uuid5` session scheme, and the §4.5 per-correlation ordering guarantee |
| [`agentdocs/DESIGN_PHASE1.md`](agentdocs/DESIGN_PHASE1.md) | the KEDA scaling model, the §16 gaps, and §3.2 on the local Kind target |
| [`agentdocs/IMPLEMENTATION_REPORT0.md`](agentdocs/IMPLEMENTATION_REPORT0.md) | what Phase 0 built, and the follow-ups it left |
| [`agentdocs/IMPLEMENTATION_REPORT1.md`](agentdocs/IMPLEMENTATION_REPORT1.md) | what Phase 1 built, measured results on both clusters, 28 findings, and what is still blocked |
| [`agentdocs/README_PHASE0.md`](agentdocs/README_PHASE0.md) | the laptop runbook in full detail |
| [`agentdocs/README_PHASE1.md`](agentdocs/README_PHASE1.md) | the Phase 1 runbook in full detail — local, Docker, Kind, OpenShift |

If something behaves surprisingly, check
[`IMPLEMENTATION_REPORT1.md`](agentdocs/IMPLEMENTATION_REPORT1.md) §4 first — most
surprises are already recorded there with their root cause.
