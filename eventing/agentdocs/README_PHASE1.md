# README — Phase 1: running the KEDA-scaled deployment

How to run the Phase 1 system in each of the three environments, with the tests
and the demo flow for each. Companion to `DESIGN_PHASE1.md` (the design) and
`IMPLEMENTATION_REPORT1.md` (what was built and what the tests actually showed).

Phase 0's `README_PHASE0.md` still describes the laptop demo in more detail; this
document only covers what Phase 1 changed or added.

```text
                          UNCHANGED WIRE CONTRACT
                                    │
  HTTP ─▶ EventBridge ─▶ Kafka:requests ─▶ EventRunner ─▶ claude -p
        (Deployment,                        (Deployment,
         replicas=1,                         replicas 0..6)
         public Route)                             ▲
                                                   │ scales on consumer lag
                                             KEDA ScaledObject
```

---

## 0. Which environment do you want?

| | What it proves | Needs | Time |
|---|---|---|---|
| [1. Local, no containers](#1-local-quick-tests-no-containers) | the logic: 286 unit tests, plus the `--resume` fact the design rests on | Python 3.14t, `uv` | ~60 s |
| [2. Docker on your laptop](#2-docker-on-your-laptop) | the real wire path through a real Kafka, in the real images | Docker | ~3 min (first build longer) |
| [3. A local Kind cluster](#3-a-local-kind-cluster) | the whole KEDA story with no shared cluster and no VPN — e2e **37/37** | Docker + `kind` | ~10 min first time |
| [4. Kubernetes / OpenShift](#4-kubernetes--openshift) | the same, plus the arbitrary-UID and multi-node paths Kind cannot test — e2e **49/49** | a cluster with Strimzi + KEDA | ~8 min |

All paths are relative to `eventing/` in the `examples` repository unless said
otherwise.

Every script is Python and stdlib-only (`DESIGN_PHASE1.md` §9). The two
historically documented entry points keep one-line `exec` shims
(`scripts/build-images.sh`, `scripts/docker-e2e-test.sh`) so old muscle memory
still works.

---

## 1. Local quick tests (no containers)

The fastest useful signal. No Docker, no Kafka, no cluster.

### Set up the interpreter

Phase 0 requires **free-threaded** CPython 3.14 (`3.14t`); the code runs the WSGI
server, Kafka consumer, ntfy publisher and per-correlation agent workers as
concurrent threads.

```bash
cd eventing
uv venv --python 3.14t .venv
uv pip install --python .venv/bin/python kafka-python cloudevents pytest
.venv/bin/python -c 'import sys; print("GIL enabled:", sys._is_gil_enabled())'   # -> False
```

Only pure-Python dependencies are allowed (§1.1): `pyyaml`, `requests` and
`confluent-kafka` are banned — they ship C extensions, and `confluent-kafka` would
re-enable the GIL on import.

### Run the tests

```bash
PYTHONPATH=$PWD .venv/bin/python -m pytest tests/ -q

# Force mock mode explicitly. Worth doing if you have a real credential exported:
# with ER_MOCK_CLAUDE unset, config.load() auto-selects REAL claude when it sees
# ANTHROPIC_AUTH_TOKEN or ANTHROPIC_API_KEY.
ER_MOCK_CLAUDE=true PYTHONPATH=$PWD .venv/bin/python -m pytest tests/ -q
```

What the Phase 1 files cover:

| File | Gates |
|---|---|
| `test_proclib.py` | the harness cannot report PASS for a command that never ran, and a run that asserted nothing exits 1 |
| `test_k8slib.py` | kubectl JSON → dicts; a **missing field raises** instead of returning empty |
| `test_offsets.py` | offsets commit only the contiguous completed prefix (RQ-1) |
| `test_heartbeat.py` | a frozen clock makes a wedged consumer thread detectable (§8.4) |
| `test_consume_phase1.py` | construction retry, `ER_CONSUMER_GROUP`, deferred commits, the stale-replay guard, and **the consumer thread surviving a raising poll** (§8.1) |
| `test_drain.py` | SIGTERM waits for in-flight runs and says so on stderr (§8.3) |
| `test_transcript.py` | the transcript checkpoint endpoints and the cold-pod resume path (§16 Gap B) |
| `test_signing.py` | Ed25519 against the RFC 8032 vectors, and canonicalization (§11) |
| `test_manifests.py` | manifest invariants — needs `kubectl` on PATH; the cluster-gated half skips without one |

Some tests skip without a local Kafka or a cluster; that is expected and the
summary says which.

### Verify the one empirical fact Gap B rests on

The transcript-checkpoint design depends on `claude --resume` accepting a **path**
to a `.jsonl` rather than only a session id. The docs say so; this proves it, and
costs two cheap model calls:

```bash
.venv/bin/python scripts/verify_resume_by_path.py
.venv/bin/python scripts/verify_resume_by_path.py --model claude-sonnet-4-5-20250929
```

It starts a session in directory A, copies the transcript to directory B, resumes
**by path** from there with a fresh `CLAUDE_CONFIG_DIR`, and asserts the reply
still knows a codeword from turn 1. It also runs the negative control — resuming
by *id* on a filesystem that never saw the session — which must fail, because that
is the failure Gap B is about.

### Run the laptop demo

Unchanged from Phase 0:

```bash
# Kafka on localhost:9092 with topics `requests` and `responses` — see README_PHASE0.md
uv run rossoctl-eventbridge      # http://127.0.0.1:8080, /docs
uv run rossoctl-eventrunner      # mock mode unless a credential is in the env
python3 scripts/stop_demo.py     # was stop-demo.sh
```

---

## 2. Docker on your laptop

Builds both images, starts a single-node KRaft Kafka and both services on a
private network, drives turns through the real wire path, and tears everything
down.

```bash
./scripts/docker-e2e-test.sh              # build, test, tear down
./scripts/docker-e2e-test.sh --keep       # leave it up to poke at
./scripts/docker-e2e-test.sh --no-build   # reuse existing images
./scripts/docker-e2e-test.sh --port 19090
```

Published on host port **18080** by default so it cannot collide with a laptop
demo on 8080. Free and deterministic: no credential is forwarded, so EventRunner's
auto-detection selects mock mode. `--real` forwards your credential, but the base
runner image ships no `claude` CLI, so expect `claude binary not found` unless you
supply one.

It asserts, in order: the three mock-mode auto-selection decisions **inside the
built image**; `ER_CONSUMER_GROUP` reaching the config; Kafka up and topics
created; `/healthz` on the new port; the startup log announcing mock mode *and
why*; the consumer connecting; one turn end to end; `ce_causationid` on every
response event; `events.jsonl` and `/turns`; the transcript `PUT`/`GET` round trip
and its 404 for an unknown correlation; a `/continue` turn; and a clean SIGTERM.

### Build and publish images

```bash
python3 scripts/build_images.py --local                 # single-arch, local daemon
python3 scripts/build_images.py                         # multi-arch, builds AND pushes
python3 scripts/build_images.py --with-claude           # + the derived image for the real demo
python3 scripts/push_images.py --dry-run                # print the refs, push nothing
python3 scripts/push_images.py --verify                 # push, then assert anonymous pullability
```

Multi-arch build and push are inseparable — a manifest list cannot live in a local
docker daemon. One-time setup:

```bash
docker buildx create --name rossoctl-multi --driver docker-container --use
```

On Apple Silicon enable **Rosetta** rather than QEMU (Rancher Desktop:
Preferences → Virtual Machine → Emulation → Type VZ + Rosetta support). The build
script times a trivial amd64 container at startup and warns if the emulator looks
like QEMU, because the difference is roughly an order of magnitude.

There is no `python:3.14t` tag on Docker Hub, so both images install a uv-managed
free-threaded CPython and **assert the GIL is off at build time**.

### The derived `claude` image

The base runner image deliberately omits the CLI (it is distributed per-platform
and versioned independently, and mock mode makes a bare `docker run` useful
without it). `Dockerfile-eventrunner-claude` adds it, pinned to the version this
project verified `--resume`-by-path against. The demo overlay uses it; the test
overlay does not.

Two things about it are deliberate and easy to trip over:

- It publishes as **`rossoctl-eventrunner:claude-dev`** — a tag on the existing
  runner repository, not a repository of its own. Quay creates new repositories
  *private*, which makes them fail preflight's anonymous-pull check; reusing the
  public repo avoids a manual step in the registry UI.
- It builds **`linux/amd64` only**, and does not run `claude --version` at build
  time. The CLI bundles a `bun` binary that aborts under QEMU user-mode emulation,
  so a cross-built layer cannot execute it on an arm64 builder. Whether it really
  runs is verified on a real cluster node by preflight check 13.

---

## 3. A local Kind cluster

Kind runs the full Phase 1 story — scale-from-zero, scale-to-zero, the Ingress,
resume across a cold pod — with no shared cluster and no VPN. **The application
manifests are identical**; only the ingress differs, because Kind has no OpenShift
Route API.

### Create the cluster

A default `kind create cluster` is **not** sufficient, and this is the one thing to
get right. The ingress-nginx controller schedules with a `nodeSelector` on
`ingress-ready=true` and binds host ports, so without both of those in the cluster
config nothing is reachable from your laptop and the controller sits `Pending`.
`k8s/kind/kind-cluster.yaml` supplies both:

```bash
python3 scripts/kind_setup.py --check      # what is missing, changes nothing
python3 scripts/kind_setup.py              # install into the existing cluster
python3 scripts/kind_setup.py --recreate   # rebuild the cluster from the config first
python3 scripts/kind_setup.py --name kev1 --kubeconfig .kube/config-kind
```

That closes exactly the gaps a bare Kind cluster has, and nothing else:

1. the cluster, from `k8s/kind/kind-cluster.yaml`;
2. **ingress-nginx** (kind provider), reachable on host port 30080;
3. **Strimzi** watching namespace `kafka`, plus a single-node KRaft Kafka named
   `my-cluster` — the same name and bootstrap DNS as the reference cluster, which is
   why `base/configmap.yaml` needs no change;
4. **KEDA**, including a check that the `external.metrics.k8s.io` APIService is
   Available — a Ready KEDA operator with an unavailable APIService still cannot
   scale.

### CPU and memory

**Kind has no resource knobs.** Each node is a container and the ceiling is the
Docker/Rancher/Colima VM; putting `cpu:` or `memory:` in the Kind config does
nothing. A too-small VM does not produce an error from Kind — it produces pods
stuck `Pending` on insufficient cpu/memory, or an OOMKilled Kafka broker, which
reads like an application bug.

```bash
docker info --format '{{.NCPU}} CPUs / {{.MemTotal}} bytes'
KUBECONFIG=.kube/config-kind kubectl get node -o jsonpath='{.items[0].status.allocatable}'
```

| | CPU | Memory | For |
|---|---|---|---|
| Minimum | 4 | 8 GiB | Strimzi + KEDA + ingress-nginx + both workloads, one broker |
| Recommended | 6 | 12 GiB | room for several EventRunner replicas and real `claude` subprocesses |
| Verified on | 8 | 24 GiB | comfortable |

Raise them in Rancher Desktop (Preferences → Virtual Machine), Docker Desktop
(Settings → Resources), or `colima start --cpu 6 --memory 12`. `kind_setup.py`
reports what it found and fails below the minimum rather than letting you discover
it as a mystery.

A single control-plane node is deliberate: `maxReplicaCount` is bounded by the
topic's partition count, not the node count, and six small pods fit on one node.

### Deploy and test

Every script takes `--kubeconfig`, so nothing depends on which kubeconfig happens
to be exported:

```bash
KC=.kube/config-kind
python3 scripts/k8s_preflight.py  --kubeconfig $KC --yes --overlay kind
python3 scripts/k8s_deploy.py     --kubeconfig $KC --yes --overlay kind
python3 scripts/k8s_e2e_test.py   --kubeconfig $KC       --overlay kind
python3 scripts/k8s_demo_flow.py  --kubeconfig $KC       --overlay kind --concurrency 3
python3 scripts/k8s_teardown.py   --kubeconfig $KC
```

The public URL is **`http://eventbridge.127.0.0.1.nip.io:30080`** — nip.io resolves
any `*.127.0.0.1.nip.io` to loopback, so you get a real hostname-based Ingress with
no `/etc/hosts` editing. Plain HTTP on purpose: the kind ingress-nginx ships a
self-signed certificate, and for a loopback-only demo TLS only adds click-throughs.

```bash
open http://eventbridge.127.0.0.1.nip.io:30080/v0/agents/<corr>
KUBECONFIG=.kube/config-kind kubectl -n kev1 get deploy eventrunner -w
```

### What Kind does and does not prove

Worth knowing so you do not over-trust a green local run:

- **It tests the half of the image work that OpenShift hides.** There is no SCC to
  inject a numeric `runAsUser`, so `runAsNonRoot: true` is enforced against the
  image's own `USER` — the exact configuration that used to fail with
  `image has non-numeric user (app)`. Conversely the arbitrary-UID path (preflight
  check 6) is meaningless on Kind, because nothing assigns a foreign UID.
- **Wake latency is better than the cluster's and is not comparable.** The local
  Kafka sets `group.initial.rebalance.delay.ms: 0`; the shared cluster leaves it at
  3000 ms and Phase 1 deliberately does not change a broker-wide setting there. Do
  not quote a Kind number as if it were a cluster number.
- **One node means no cross-host partition distribution.** Concurrency across pods
  is still exercised; placement across machines is not.
- **On Apple Silicon the cluster is arm64.** `eventbridge` and `eventrunner` are
  multi-arch and just work. The derived `claude` image is amd64-only (the CLI
  bundles a `bun` binary that aborts under QEMU), so for the demo overlay here
  rebuild it natively — which works precisely because it is no longer
  cross-building:

```bash
python3 scripts/build_images.py --with-claude --claude-platforms linux/arm64
```

---

## 4. Kubernetes / OpenShift

### What the cluster must already have

| | |
|---|---|
| Strimzi | operator + a `Kafka` CR, in namespace `kafka`. **Serves only `kafka.strimzi.io/v1`** |
| KEDA | `ScaledObject` CRD and a working metrics adapter |
| Ingress | an OpenShift `Route` API or an `IngressClass`, with wildcard DNS |
| Images | `rossoctl-eventbridge` and `rossoctl-eventrunner` **anonymously pullable** (there is no pull-secret fallback, by design) |

You do not have to check these by hand — preflight does, and fails with the
remediation:

```bash
python3 scripts/k8s_preflight.py --yes
python3 scripts/k8s_preflight.py --yes --skip 6,7     # skip the two slow pod probes
```

The 12 checks cover cluster identity, the CRDs and served versions, **which
namespace Strimzi actually watches**, anonymous image pullability and
architecture, arbitrary-UID writability (a throwaway pod with *no volume
mounted*), KEDA's ability to reach Kafka across namespaces (a temporary
ScaledObject plus a real external-metrics query), ingress capability and wildcard
DNS, topic-name collisions, the namespace and its quota, and the default
StorageClass when the overlay wants a PVC.

`--yes` is required because check 1 refuses to act on the ambient
current-context without acknowledgement — deploying into the wrong cluster is the
worst outcome available here. `--context <name>` pins it instead.

### Deploy

```bash
python3 scripts/k8s_deploy.py --yes                      # test overlay into kev1
python3 scripts/k8s_deploy.py --yes --namespace kev2
python3 scripts/k8s_deploy.py --yes --check-only          # is a deploy needed?
python3 scripts/k8s_deploy.py --yes --dry-run             # server dry-run, mutate nothing
python3 scripts/k8s_deploy.py --yes --force-deploy
```

It runs, in the §13 order:

0. preflight,
1. the namespace,
2. the two `KafkaTopic`s **in namespace `kafka`** (not the target namespace — the
   operator watches only its own, and a CR anywhere else is silently ignored),
3. the overlay,
4. the Route hostname published as `EVENT_BRIDGE_PUBLIC_BASE_URL` and then
   **proven** with a `GET /healthz` from outside the cluster — this is a hard gate,
5. the idle state, which is **zero EventRunner replicas**. That is correct, not a
   failure.

Change detection uses two mechanisms together: a `sha256` of the rendered overlay,
stamped as an annotation on the namespace, as a cheap gate; and `kubectl diff -k`
as the authoritative answer, because it also catches cluster-side drift that a
file hash cannot see. KEDA changing the replica count does **not** register as
drift.

Manual equivalents, for when you are debugging:

```bash
NS=kev1
kubectl apply -f k8s/topics/kafkatopics.yaml
kubectl -n kafka wait --for=condition=Ready kafkatopic/$NS-requests --timeout=120s
kubectl apply -k k8s/overlays/test
kubectl -n $NS rollout status deploy/eventbridge --timeout=180s
HOST=$(kubectl -n $NS get route eventbridge -o jsonpath='{.spec.host}')
kubectl -n $NS set env deploy/eventbridge "EVENT_BRIDGE_PUBLIC_BASE_URL=https://$HOST"
curl -fsS "https://$HOST/healthz"
kubectl -n $NS get scaledobject eventrunner    # READY=True ACTIVE=False
kubectl -n $NS get deploy eventrunner          # READY 0/0  <- correct
```

### Run the e2e test

```bash
./scripts/k8s-e2e-test.sh                 # detect, deploy if needed, test
./scripts/k8s-e2e-test.sh --namespace kev2
./scripts/k8s-e2e-test.sh --force-deploy
./scripts/k8s-e2e-test.sh --no-deploy     # fail if not already deployed
./scripts/k8s-e2e-test.sh --keep          # skip the scale-to-zero wait
```

Twelve assertions; 4, 6, 7 and 11 are the Phase 1 point because they test KEDA
rather than the wire:

| # | Assertion |
|---|---|
| 1 | preflight passes |
| 2 | both `KafkaTopic`s `Ready=True` |
| 3 | eventbridge available; `/healthz` 200 **on the public URL** |
| 4 | idle: `Ready=True`, `Active=False`, eventrunner `0/0` |
| 5 | `POST /v0/agents` → 202 with a correlationid |
| 6 | **KEDA activates** within 60 s, replicas ≥ 1 |
| 7 | a pod starts, and its log says `mock_claude=True` with an `auto:` reason |
| 8 | a `final=true` event within 120 s |
| 9 | the reply contains `MOCK-REPLY` |
| 10 | `events.jsonl` non-empty; `/turns` pairs the prompt with its turn |
| 11 | **scale back to zero** after the cooldown |
| 12 | cold-start latency recorded (RQ-4) |

Mock mode needs no override: the test overlay mounts no credential at all, so
EventRunner's auto-detection chooses mock itself — which also means assertion 7
exercises that detection in-cluster rather than bypassing it.

On any failure the script dumps the §14.3 triage set: the ScaledObject's
conditions (trigger errors live there), the runner pod logs, the KEDA operator
log, and recent namespace events.

### Run the demo flow

```bash
python3 scripts/k8s_demo_flow.py                        # mock mode, free
python3 scripts/k8s_demo_flow.py --skip-cold-continue   # faster, weaker
python3 scripts/k8s_demo_flow.py --overlay demo         # REAL claude, costs tokens
python3 scripts/k8s_demo_flow.py --overlay demo --concurrency 3   # T1.10
```

The four stages, each asserted with its timing printed:

| Stage | What it asserts |
|---|---|
| 0 | replicas `0`, `Active=False` before anything is posted |
| 1 | after the POST: `Active=True`, replicas ≥ 1, **wake latency recorded** |
| 2 | a `phase=stdout` event, then a terminal one; replicas stayed ≥ 1 for the whole run |
| 2b | `/continue` on a warm pod resumes the same session, and the transcript is checkpointed |
| 2c | `/continue` **after a full scale-to-zero cycle** also resumes — the Gap B gate |
| 3 | replicas return to `0`; `Active=False` |
| T1.10 | `--concurrency N`: N correlations run on more than one pod, and each `/continue` resumes **its own** session |
| T5.6 | a group whose offsets were deleted does not replay the backlog |

Stage 2c only *proves* resumed context with a real agent: mock mode emits a fixed
reply and cannot demonstrate memory. Run `--overlay demo` for the real gate. The
same is true of `--concurrency`: in mock mode it verifies that KEDA scales past one
pod, but only a real agent can show that two conversations did not cross wires.

T5.6 temporarily lowers `ER_MAX_REQUEST_AGE_S` (to `--replay-max-age`, default
60 s) and restores it afterwards. Without that, nothing posted in the last hour is
old enough to exercise the guard and the check could only ever pass by waiting an
hour.

Watch it happen in a second window:

```bash
kubectl -n kev1 get deploy eventrunner -w
```

Say the boundary out loud when demoing: the **agent** scales to zero, not the
whole system. EventBridge stays at one replica — something has to accept the POST
and hold the event store. The claim is "no agent capacity is consumed while idle".

### The real demo, with credentials

Nothing secret is committed. The demo overlay *references* a Secret named
`anthropic-credentials` and never contains one; the deploy script creates it from
your environment:

```bash
export ANTHROPIC_BASE_URL=https://ete-litellm.ai-models.vpc.res.ibm.com
export ANTHROPIC_AUTH_TOKEN=<your-litellm-virtual-key>
export ANTHROPIC_MODEL=claude-sonnet-4-5-20250929        # optional

python3 scripts/build_images.py --with-claude
python3 scripts/k8s_deploy.py --yes --overlay demo
python3 scripts/k8s_demo_flow.py --overlay demo
```

Details that matter:

- The values are applied **via stdin**, not
  `kubectl create secret --from-literal=`, which would put the token in the
  process argv where `ps` can read it. No kustomize `secretGenerator` either —
  that would inline the value into `kubectl kustomize` output.
- **Use the `vpc` host, not `vpc-int`.** `vpc-int` resolves to IBM-internal `9.x`
  addresses which this cluster's AWS worker nodes cannot route: it works from a
  laptop on the VPN and times out in a pod. The deploy script warns if you pass a
  `vpc-int` URL.
- `ER_MOCK_CLAUDE=false` is set explicitly in the demo overlay, so real mode is a
  declaration rather than a side effect of whether a Secret happened to mount.
- The demo overlay also moves `HOME` onto the mounted volume (`/data/home`) and
  requests a PVC, so the event store and checkpointed transcripts survive a
  restart.

Drive it exactly as in Phase 0 — the HTTP surface is unchanged, only the hostname
differs:

```bash
HOST=$(kubectl -n kev1 get route eventbridge -o jsonpath='{.spec.host}')
CORR=$(curl -sS -X POST "https://$HOST/v0/agents" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Name one animal starting with O.","max_turns":1}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["correlationid"])')
open "https://$HOST/v0/agents/$CORR"
curl -sS -X POST "https://$HOST/v0/agents/$CORR/continue" \
  -H 'Content-Type: application/json' -d '{"prompt":"And one starting with P?"}'
```

### Agent groups — batch fan-out (§21)

One call submits N agents as a tracked **group**, with its own page and — the point —
**exactly two notifications** instead of N.

```bash
HOST=$(kubectl -n kev1 get route eventbridge -o jsonpath='{.spec.host}')

# 100 agents, tracked live in the terminal
python3 skills/eventbridge/eventbridge-cli.py group run \
  "$(python3 - <<'P'
print('\n'.join(f'Batch item {i+1}: reply ok.' for i in range(100)))
P
)" --label demo-100 --base-url "$HOST"

python3 skills/eventbridge/eventbridge-cli.py group status <groupid> --base-url "$HOST"
python3 skills/eventbridge/eventbridge-cli.py group watch  <groupid> --base-url "$HOST"
python3 skills/eventbridge/eventbridge-cli.py group list                --base-url "$HOST"

# or as part of the asserted demo flow
python3 scripts/k8s_demo_flow.py --group 100
```

What it looks like, measured on ykt1:

```text
▸ group demo-100 quiet-whale-6700 · https://…/v0/groups/quiet-whale-6700
· 100 agents submitted together
   0/100 done · 100 queued · 19s elapsed
  34/100 done · 12 running · 54 queued · 40s elapsed · less than a minute left
  80/100 done · 13 running ·  7 queued · 51s elapsed · less than a minute left
 100/100 done · 57s elapsed
✔ group complete · 100 finished · 0 failed · 57s
```

**Pages.** `/v0/groups` lists batches (HTML in a browser, JSON for the CLI) with a
per-row "last activity" age; `/v0/groups/<id>` is the dashboard — a percent-done bar,
counts in user units, elapsed, an ETA once it can be computed honestly, and a member
table linking to each `/v0/agents/<corr>`. Each member page links **back** to its group.
Both pages update **in place every second**, never reloading, so they can be left open.

**Notifications.** A batch is two: one when it starts, one when it finishes (totals,
duration, slowest member). Per-agent notifications are suppressed for the whole life of
the group — verified as exactly 2 for 100 agents.

If you want to hear about failures as they happen rather than at the end, set
`NTFY_GROUP_NOTIFY_ERRORS=true`; a batch where 30 of 40 agents are failing otherwise
looks identical to a healthy one until it ends.

**Mock agents sleep 1-5 s** (`ER_MOCK_DELAY_MIN_S` / `ER_MOCK_DELAY_MAX_S`) so there is
something to watch. Without it a 100-agent batch finishes faster than the page renders.

**Capacity — two settings, not one.** Ten pods for a 100-agent batch needs *both*:

| Setting | Why |
|---|---|
| `partitions: 12` on `kev1-requests` | Kafka gives each partition to exactly one consumer in a group, so partitions cap useful pods. Increasable in place, **never decreasable** |
| `maxReplicaCount: 10` | must stay ≤ the partition count (12) |
| `advanced.horizontalPodAutoscalerConfig.behavior.scaleUp` | **the non-obvious one.** KEDA does 0→1 itself but delegates 1→N to an HPA, whose default policy adds only `max(100%, 4 pods)` per 15 s — so a burst climbs 1→5→10 over ~30 s and a 60 s batch is well into its second half first. The explicit policy allows the whole cap in one step |

Measured with all three: `replicas=10` — the cap — on the first observation after the POST, peaking
at **10 Running pods**, batch done in 57 s.

### Watching the topics during a demo

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

### Teardown

Returns the demo to zero cost **without destroying anything expensive**:

```bash
python3 scripts/k8s_teardown.py                  # pause + scale to zero
python3 scripts/k8s_teardown.py --resume         # undo it
python3 scripts/k8s_teardown.py --purge          # really delete (asks first)
```

The default pauses the ScaledObject with `paused-replicas=0`, which pins the
target at zero and makes KEDA ignore new lag, then scales both Deployments to
zero. Preserved: the namespace, the ScaledObject, **the KafkaTopics and their
data**, the Secrets, and KEDA itself. `--purge` deletes the app objects and both
topics — including every message in them — and asks you to type the namespace name
first.

Two things that look alarming and are correct: a paused ScaledObject still reports
`Active=True` while messages sit in the topic, and the Route stays resolvable
while pointing at zero endpoints, so it serves a 503 rather than a DNS error.

### Submit-path authentication (off by default — a deployed instance is open)

**Read this before exposing a Route.** EventBridge's HTTP surface authenticates
only when `EB_AUTH_TOKENS` is set, and **no manifest in `k8s/` sets it**. The
default is therefore fail-open: `resolve_identity` returns "allowed, anonymous",
an unauthenticated `POST /v0/agents` returns 202 and spawns a `claude` run, and
`k8s/base/eventbridge-route.yaml` makes that reachable from outside the cluster.

That is the intended Phase 1 posture — auth on the HTTP surface is deferred to
Phase 2 — but it is a property of a deployment, not a detail, so set the tokens
whenever the Route is reachable by anyone you would not hand a shell to:

```bash
kubectl -n kev1 create secret generic eventbridge-auth \
  --from-literal=EB_AUTH_TOKENS="alice:$(openssl rand -hex 24),bob:$(openssl rand -hex 24)"
# then reference it from the EventBridge Deployment's envFrom as a secretRef.
```

`EB_AUTH_TOKENS` is `name:token` pairs, comma separated. It is read **only** from
the environment and deliberately never from `config.toml`, which is committed. The
validated name rides onto the request event as `ce_submitter` and shows up as
"Turn 1 — start — by alice" in the transcript view and on `/turns`.

Two limits worth stating plainly:

- Only the two routes that **create** work are guarded (`POST /v0/agents`,
  `POST /v0/groups`). `GET`s, `/healthz`, `/docs`, `PUT /transcript` and
  `/continue` stay open by design — `/continue` because authenticating it would
  put a long-lived bearer token inside every ntfy notification; it relies on the
  unguessable `correlationid` as a capability URL instead.
- `submitter` is signed (it joined `signing.SIGNED_ATTRS` in Phase 2, DESIGN_PHASE2
  §4.2), and Kafka is still plaintext. The claim is "EventBridge refuses
  unauthenticated submissions and records who it believes submitted this — and on a
  signed request, the signature proves EventBridge asserted that name", not "this
  event proves who typed it."

Setting `EB_AUTH_TOKENS` will 401 the `/eventbridge` CLI, the three e2e scripts
and the README `curl`s until they are taught to send the header.

### Signed events (optional, off by default)

`ER_REQUIRE_SIGNATURE=false` in the ConfigMap. To turn it on, mount an Ed25519
seed and flip the flag:

```bash
python3 - <<'PY' > /tmp/seed.hex
import os; print(os.urandom(32).hex())
PY
kubectl -n kev1 create secret generic event-signing-key --from-file=seed.hex=/tmp/seed.hex
# then set ER_REQUIRE_SIGNATURE=true and ER_VERIFY_KEY_PATH=/keys/seed.hex,
# mount the Secret at /keys, and have the publisher sign with the same seed.
rm /tmp/seed.hex
```

Signing is implemented in pure Python (`cryptography` is a C extension and
banned), verified against the RFC 8032 test vectors. It is off by default partly
because the pure-Python scalar multiplication is real per-event overhead:
IMPLEMENTATION_REPORT1.md §9 measured **222 ms to sign and 227 ms to verify** on
the reference laptop, and later hardware has measured inside a 190–230 ms band —
fine for the terminal event, wrong for every intermediate one.

---

## 5. Troubleshooting

| Symptom | Cause |
|---|---|
| `no matches for kind "KafkaTopic" in version "kafka.strimzi.io/v1beta2"` | Strimzi 1.0.x serves only `v1` |
| `KafkaTopic` created but never `Ready`, no events at all | it is in the wrong namespace — Strimzi watches only its own, silently |
| `CreateContainerConfigError: image has non-numeric user (app)` | the image needs a numeric `USER`; fixed, but check you are not on an old tag |
| `PermissionError: [Errno 13] … '/data/…'` | arbitrary-UID permissions; preflight check 6 reproduces it |
| `No module named eventrunner.healthcheck` in the liveness probe | the deployed image predates Phase 1 — rebuild and push |
| eventrunner never scales up | `ER_CONSUMER_GROUP` does not match the ScaledObject trigger's `consumerGroup` |
| eventrunner never scales **down** | the terminal event was never emitted, so the offset was never committed (RQ-1 defers it deliberately) |
| pod `Running` but nothing is consumed | the §8.1 failure — now caught by the liveness probe; look for a traceback followed by silence |
| `/continue` fails after an idle period | §16 Gap B; check the transcript endpoint returns 200 for that correlationid |
| a turn's events are missing, or `/turns` pairs a prompt with the wrong reply | per-pod `sequence` restart overwriting earlier rows — fixed by seeding from EventBridge; if it reappears, check the runner log for `resuming sequence numbering for <corr> at N` |
| the Route serves 503 | zero endpoints — EventBridge is scaled to zero (probably by teardown) |
| history mysteriously reset | `my-cluster` uses **ephemeral** storage; a broker restart loses all topic data |
| a group that ntfy linked to now returns `unknown groupid` | `/data` is `emptyDir` in the `test` and `kind` overlays, so an EventBridge restart empties the store. `GroupMirror` rebuilds it from the `responses` topic on every start — look for `[group-mirror] rebuilt N group(s)` in the log. If that line is missing the pod predates the fix; if it says `0 group(s)` with a non-empty topic, or `no metadata for 'responses'`, the broker was unreachable during startup |
| a group is stuck at, say, 99/100 forever | the missing member never emitted a terminal event. Restarting EventBridge does **not** paper over it: the replay reconstructs what the topic says and only settles a group whose members are all finished. Use `group close` / `group cancel`, or a deadline |
| (Kind) ingress controller stuck `Pending` | the cluster lacks `ingress-ready=true` and/or the host port mappings — recreate it from `k8s/kind/kind-cluster.yaml` (`kind_setup.py --recreate`) |
| (Kind) `http://eventbridge.127.0.0.1.nip.io:30080` refuses the connection | the cluster was created without `extraPortMappings`, or something else holds host port 30080 |
| (Kind) pods `Pending` on insufficient cpu/memory, or an OOMKilled broker | the Docker/Rancher/Colima VM is too small — Kind itself has no resource knobs |
| (Kind) `exec format error`, or ImagePullBackOff on the claude image | the cluster is arm64 and that image is amd64-only — rebuild with `--claude-platforms linux/arm64` |

Useful one-liners:

```bash
kubectl -n kev1 describe scaledobject eventrunner        # trigger errors live here
kubectl -n kev1 logs deploy/eventbridge | grep group-mirror   # what the last restart rebuilt
kubectl -n kev1 logs deploy/eventrunner --tail=100
kubectl -n keda  logs deploy/keda-operator --tail=50
kubectl -n kev1 get events --sort-by=.lastTimestamp | tail -20
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/kev1/s0-kafka-kev1-requests?labelSelector=scaledobject.keda.sh%2Fname%3Deventrunner"
```
