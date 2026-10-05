# DESIGN — Phase 1: KEDA-scaled Kubernetes deployment

Status: draft (revision 2)
Scope: **delta over `DESIGN_PHASE0.md`.** Read that first — this document only
records what changes when the Phase 0 demo moves from a laptop onto a
Kubernetes cluster with Strimzi-managed Kafka and KEDA-driven scaling.

Phase 0 proved the wire shape. Phase 1 changes *where the consumer runs and
who decides how many of them there are*, and nothing else:

```text
                          UNCHANGED WIRE CONTRACT
                                    │
  HTTP ─▶ EventBridge ─▶ Kafka:requests ─▶ EventRunner ─▶ claude -p
        (Deployment,                        (Deployment,
         replicas=1,                         replicas 0..N)
         public Route)                             ▲
                                                   │ scales on consumer lag
                                             KEDA ScaledObject
```

The single new moving part is KEDA: it watches consumer-group lag on the
requests topic and scales the EventRunner Deployment from **zero** to N.
An idle demo costs nothing; a posted request wakes a pod within seconds.

---

## 1. What does NOT change

Stated explicitly, because the temptation in a "Phase 1" doc is to redesign
things that already work:

- **The CloudEvent contract** (§2 of Phase 0) — same two topics, same binary
  content mode, same `ce_*` headers, same `correlationid` / `sessionuuid` /
  `sequence` / `phase` / `final` extensions, same compacted `data` payload
  with `text` at the top level.
- **Kafka message key = `correlationid`** (§2.1). Load-bearing in Phase 1 for
  a new reason — see §4.
- **The correlation-ID scheme and `uuid5` session derivation** (§2.3).
- **EventBridge's HTTP surface** (§3.2), HTML view, SSE endpoint, `/turns`,
  `events.jsonl`, OpenAPI.
- **EventRunner's per-correlationid FIFO router** (§4.5). It keeps serializing
  turns *within* a pod.
- **The `claude --session-id` / `--resume` mechanics** (§4.3, Q&E-2..4).
- **Pure-Python discipline** (§1.1). No new runtime dependencies — in
  particular the application does **not** gain a Kubernetes client library.
- **Mock mode** and its credential auto-detection. Verified working in-cluster
  (§3.1, Finding 3) and it is what makes the e2e test free and deterministic.

Everything below is additive or a replacement of deployment mechanics.

---

## 2. Goals, in priority order

Ordered deliberately, because the order drives both the pre-deployment checks
(§12) and the deployment sequence (§13).

1. **A reachable public URL serving EventBridge.** Everything a human sees
   hangs off it: the HTML transcript, the SSE stream, ntfy click-through. If
   the URL is not reachable from outside the cluster, the demo has no face —
   so this is proven *before* anything else is deployed, and re-proven against
   the real Route immediately after EventBridge starts. Discovering an ingress
   problem after a full deploy wastes the whole cycle.
2. **Scale from zero on demand.** A posted request wakes an EventRunner pod
   with no human action.
3. **Scale back to zero when idle.** An idle demo consumes no compute. This is
   the half that is easy to forget to test.
4. **The wire contract is untouched.** The same `curl` from Phase 0 works
   against the cluster, only the hostname differs.
5. **A free, deterministic e2e test.** Mock mode, no API spend, repeatable.

---

## 3. Target cluster — verified facts

Written against the `ykt1` cluster. Anything here that
differs on another cluster changes the manifests, so §12 re-checks all of it
at deploy time rather than trusting this table.

| Fact | Value | Consequence |
|---|---|---|
| Context | `default/api-ykt1-hcp-res-ibm-com:6443/aslom@us.ibm.com` | OpenShift, k8s v1.33.13 |
| Node architecture | `amd64` × 6 | the multi-arch images resolve to `linux/amd64` |
| Strimzi | 1.0.1, ns `kafka` | **serves only `kafka.strimzi.io/v1`** — §6 |
| Strimzi watch scope | `STRIMZI_NAMESPACE` is a `fieldRef` to its own namespace | **watches ONLY ns `kafka`** — Finding 1 |
| Kafka cluster | CR `my-cluster`, Kafka 4.2.0, KRaft, 1 dual-role node, **ephemeral storage** | bootstrap `my-cluster-kafka-bootstrap.kafka.svc:9092`; a broker restart loses all topic data |
| KEDA | 2.20.0, ns `keda` | `ScaledObject` + `ScaledJob` CRDs present; `Paused` condition supported (§17) |
| Default StorageClass | `gp3-csi` (`WaitForFirstConsumer`) | PVC available if wanted |
| Target namespace | `kev1` — exists | default; configurable per §5 |
| Namespace UID range | `1000910000/10000` | **arbitrary-UID containers** — Finding 3 |

### 3.1 Three findings that shape the design

**Finding 1 — Strimzi only reconciles its own namespace.** The operator's
`STRIMZI_NAMESPACE` env var is a downward-API `fieldRef` to
`metadata.namespace`, i.e. `kafka`. A `Kafka` or `KafkaTopic` CR created in
`kev1` is **silently ignored** — no reconcile, no event, no error. Therefore
Phase 1 reuses `my-cluster` in ns `kafka`, `KafkaTopic` CRs are created **in ns
`kafka`**, and workloads in `kev1` connect cross-namespace.

**Finding 2 — the images are public, and that must be asserted, not assumed.**
Both `quay.io/aslomnet/rossoctl-eventbridge:dev` and `…-eventrunner:dev` now
serve an unauthenticated manifest request with **HTTP 200** and carry both
`linux/amd64` and `linux/arm64`. Verified end to end: a pod in `kev1` with no
Quay pull secret pulled the runner image in **5.1 s** (245 MB).

Visibility is a Quay setting that a human can flip back, and the failure mode
is a slow, confusing `ImagePullBackOff` deep into a deploy. So the design
**assumes public images and the deployment script hard-fails if they are
not** — see §12, check 4. There is no pull-secret fallback path; if the check
fails the fix is to make the repositories public again.

**Finding 3 — OpenShift runs the containers as an arbitrary UID, and the
images are not ready for it.** A probe pod in `kev1` was assigned
`runAsUser: 1000910000`, `gid=0`, `groups=[0]` — OpenShift's SCC overrides the
image's `USER app` (uid 10001). Because `Dockerfile-eventrunner` does
`chown -R app:app /data`, the running process cannot write its own `TMPDIR`:

```text
PermissionError: [Errno 13] Permission denied: '/data/rossoctl-keda1'
```

Re-running the same probe **with an `emptyDir` mounted at `/data`** succeeded
(`uid=1000910000 gid=0 groups=[0]`, and mock-mode auto-detection reported
`mock=True reason=auto: none of ANTHROPIC_AUTH_TOKEN/ANTHROPIC_API_KEY set`).
So a mounted volume masks the problem — but the image is still wrong, and §8.5
fixes it rather than relying on the mount.

---

### 3.2 A second target: a local Kind cluster

Everything above describes `ykt1`. Phase 1 also targets a **local Kind cluster**,
because the reference cluster is shared, occasionally rate-limited, and requires a
VPN — none of which should be prerequisites for working on the scaling model.

The goal is deliberately narrow: **the application manifests must not change.**
Kind is a second *environment*, not a second deployment. That is achievable because
the only things the workloads depend on are a Kafka bootstrap address, a consumer
group and a URL, so the design keeps all three identical:

| | `ykt1` | Kind |
|---|---|---|
| Kafka | pre-existing `my-cluster` in ns `kafka` | `my-cluster` in ns `kafka`, created by `kind_setup.py` — **same name, same bootstrap DNS** |
| Strimzi | 1.0.1, pre-installed | latest (1.2.0), installed; also serves only `v1`, so §6's manifests are unchanged |
| KEDA | 2.20.0, pre-installed | 2.20.0, installed |
| Ingress | OpenShift `Route` | `Ingress` + ingress-nginx, `overlays/kind` |
| Node arch | amd64 × 6 | **arm64** × 1 on Apple Silicon |
| UID | SCC-assigned arbitrary UID, gid 0 | the image's own `USER 10001` |
| Storage | `gp3-csi` | `standard` (rancher local-path), default |

Four consequences worth stating, because each one is a place the two environments
genuinely differ rather than a place the abstraction leaks:

**1. Kind is the vanilla-Kubernetes case §8.5 exists for.** There is no SCC to
inject a numeric `runAsUser`, so `runAsNonRoot: true` is enforced against the
image's own `USER` — which is exactly the configuration that produced
`CreateContainerConfigError: image has non-numeric user (app)` before §8.5 made
the directive numeric. Kind therefore tests the half of §8.5 that OpenShift masks,
and OpenShift tests the half Kind masks. Both are needed.

**2. Ingress replaces the Route, and that needs a cluster-creation-time decision.**
`overlays/kind` deletes the inherited `Route` (`$patch: delete`) and adds an
`Ingress`. But ingress-nginx's kind variant schedules its controller with a
`nodeSelector` on `ingress-ready=true` and binds host ports, so a cluster created
by a plain `kind create cluster` **cannot serve an Ingress at all** — the
controller stays `Pending` and nothing is reachable. The node label and
`extraPortMappings` must be in the cluster config, which is why
`k8s/kind/kind-cluster.yaml` is a required artefact and not a convenience. The
host is `eventbridge.127.0.0.1.nip.io:30080`: nip.io gives a real hostname-based
Ingress with no `/etc/hosts` editing, and 30080/30443 stay clear of the two ports
this project already uses (8080 for a laptop EventBridge, 18080 for the Docker
e2e).

**3. Resource sizing is a property of the VM, not of Kind.** Kind has no CPU or
memory knobs — each node is a container, and the ceiling is whatever the
Docker/Rancher/Colima VM has. Writing `memory:` into the Kind config does nothing.
A short VM does not produce an error from Kind; it produces pods stuck `Pending`
on insufficient cpu/memory, or an OOMKilled Kafka broker, which reads like an
application fault. So `kind_setup.py` reports the VM's size and asserts a floor:

| | CPU | Memory | For |
|---|---|---|---|
| Minimum | 4 | 8 GiB | Strimzi + KEDA + ingress-nginx + both workloads, one broker |
| Recommended | 6 | 12 GiB | room for several EventRunner replicas and real `claude` subprocesses |
| Verified on | 8 | 24 GiB | comfortable |

A single control-plane node is deliberate: `maxReplicaCount` is bounded by the
topic's partition count, not by node count, and six small pods fit on one node.
Extra workers would cost memory and prove nothing.

**4. One image is not multi-arch, and it is the one the demo needs.** The
`eventbridge` and `eventrunner` images are built `linux/amd64,linux/arm64`, so they
run on Kind unchanged. The derived `claude` image is built **amd64-only**, because
the CLI bundles a `bun` binary that aborts under QEMU user-mode emulation and so
cannot be exercised in a cross-built layer. On an arm64 Kind cluster the demo
overlay therefore needs that image rebuilt natively
(`--claude-platforms linux/arm64`), which works precisely because it is no longer
cross-building. The mock e2e path is unaffected.

**What Kind cannot prove.** It is one node, so it cannot exercise partition-to-pod
distribution across hosts the way a 6-node cluster does; it has no SCC, so the
arbitrary-UID path (§12 check 6) is not tested there; and `group.initial.rebalance.
delay.ms` is set to 0 locally, so it does **not** reproduce §16 Gap A's 3 s wake
floor — local wake latency is better than the cluster's and must not be quoted as
if it were the same measurement.

---

## 4. The scaling model — ScaledObject on a Deployment, not a Job per request

Phase 0's closing line promised "a KEDA-scaled Kubernetes Job". Phase 1
deliberately does **not** do that yet.

**Chosen: `ScaledObject` scaling an EventRunner `Deployment` 0..N.** Each pod
runs the unmodified Phase 0 EventRunner — Kafka consumer, per-corr router,
`subprocess.Popen(claude)` — and KEDA varies the replica count.

Why this is right for Phase 1:

1. **It preserves the §4.5 ordering contract for free.** Phase 0 keys every
   request message by `correlationid`, so all turns for one conversation land
   on one partition, and Kafka assigns each partition to exactly one consumer
   in the group. The per-correlationid FIFO guarantee the in-process Router
   provides across *threads* is provided by Kafka across *pods*, with no
   coordination. Corollary: **`maxReplicaCount` must be ≤ the partition
   count**, which KEDA enforces by default (`allowIdleConsumers: false`).
2. **Zero new application code for scaling.** No Kubernetes client, no RBAC
   for the workload, no Job template to keep in sync with a Deployment.
3. **A Job per request breaks ordering.** Two Jobs for one `correlationid`
   cannot serialize; Q&E-8 showed concurrent resume corrupts turn ordering.
4. **A Job per request needs a different consume model** — a short-lived pod
   claiming exactly one message, which the long-lived consumer loop is not.

Deferred to Phase 2, as a *security* story rather than a scaling one: one pod
per agent run buys a per-request capability envelope, hard per-run resource
limits, and a much smaller blast radius for `--permission-mode acceptEdits`.

---

## 5. Namespace and naming

Everything deploys into one namespace, **`kev1` by default, configurable**.
Configurability is a kustomize `namespace:` directive plus one env var, not a
templating language:

```bash
NS="${NS:-kev1}"     # every script honours this
```

**Topic names are namespace-prefixed.** The broker in ns `kafka` is shared —
it already carries an unrelated `keda-test-topic` — so the generic Phase 0
names would be a collision risk and an ownership puzzle:

| Phase 0 (local) | Phase 1 (cluster) |
|---|---|
| `requests` | `kev1-requests` |
| `responses` | `kev1-responses` |
| consumer group `eventrunner` | `kev1-eventrunner` |

Topics are plain config (`REQUEST_TOPIC` / `RESPONSE_TOPIC`), already in the
Phase 0 configuration surface. The consumer group is currently a hard-coded
default argument and must become configurable — §8.2.

---

## 6. Strimzi — topics as CRs

Two `KafkaTopic` resources, **in ns `kafka`**, with `apiVersion:
kafka.strimzi.io/v1`. Both details are mandatory and both fail confusingly if
wrong: a `v1beta2` manifest fails outright, and a CR in the wrong namespace is
accepted by the API server and then never reconciled.

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: kev1-requests
  namespace: kafka                     # MUST be the operator's namespace
  labels:
    strimzi.io/cluster: my-cluster
spec:
  partitions: 6                        # >= ScaledObject maxReplicaCount
  replicas: 1                          # single-broker cluster
  config:
    retention.ms: "86400000"           # 24h — bounds replay, see §16 Gap C
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: kev1-responses
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  partitions: 6
  replicas: 1
  config:
    retention.ms: "86400000"
```

Both blocks were validated with `kubectl apply --dry-run=server` against the
live CRDs.

Notes:

- **Partitions are the scaling ceiling.** 6 partitions → at most 6 useful
  EventRunner pods.
- `replicas: 1` because the cluster has one broker; higher leaves the topic
  `Ready=False` forever.
- **Ephemeral storage means topic data does not survive a broker restart.**
  Fine for a demo, and worth knowing when history mysteriously resets.

---

## 7. KEDA — the ScaledObject

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: eventrunner
  namespace: kev1
spec:
  scaleTargetRef:
    name: eventrunner                  # the Deployment
  minReplicaCount: 0                   # scale to zero when idle
  maxReplicaCount: 1                   # interim: 1 until §16 Gap B T1.7 lands, then 6
  pollingInterval: 5                   # seconds between lag checks
  cooldownPeriod: 300                  # see "the long-run problem" below
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: my-cluster-kafka-bootstrap.kafka.svc:9092
        consumerGroup: kev1-eventrunner
        topic: kev1-requests
        lagThreshold: "1"              # one pending request wakes a pod
        activationLagThreshold: "0"    # 0 -> 1 on any lag at all
        offsetResetPolicy: earliest    # match the consumer — §7.1
```

### 7.1 Three KEDA details that decide whether this works

**`offsetResetPolicy` should match the application's `auto_offset_reset`.**
`eventrunner/consume.py` uses `earliest`, so the trigger does too. This keeps
KEDA's view of "how much work is pending" consistent with what a starting
consumer will actually read.

*Correction to revision 1 of this document:* it claimed that with `latest`
and no committed offset KEDA computes zero lag and never scales from zero.
A direct measurement contradicts that — a probe ScaledObject with
`offsetResetPolicy: latest` against a 3-partition topic whose consumer group
had never committed reported lag **3** and went `Active=True`. So do not rely
on any particular no-committed-offset behaviour in either direction; set
`earliest` for semantic correctness and let §12 check 7 verify lag is actually
computable on the real topic. Expect a consequence of `earliest`: messages
already retained in the topic count as lag and trigger a scale-up on first
deploy.

**Scale-to-zero depends on committed offsets.** The Phase 0 consumer commits
explicitly (`enable_auto_commit=False` plus `c.commit()`), so lag is computable
with no group members alive. Switching to auto-commit would quietly break
scale-from-zero.

**The long-run problem.** KEDA scales down on lag, and lag drops when a
message is *committed*, not when the agent run *finishes*. Phase 0 commits
before the run completes, so a pod can be selected for termination
mid-stream. §19 RQ-1 resolves this properly by moving the commit; until then
`cooldownPeriod: 300` plus a long `terminationGracePeriodSeconds` makes the
race unlikely, and §8.3's drain makes a terminated pod finish its work.

---

## 8. Application changes required

The smallest set of code changes that makes the Phase 0 binaries deployable.
Every item here is a real gap found by designing against the live cluster.

### 8.1 Kafka consumer retry loop — blocking

`eventrunner/consume.py` builds its `KafkaConsumer` eagerly inside `run()`
with no retry. In Kubernetes, pod start order is not guaranteed: KEDA may
start a pod while the broker is rolling. The consumer thread then raises and
dies **while the main thread keeps running** — the pod stays `Running`, reports
healthy, consumes nothing, and the demo hangs silently.

Required: bounded-backoff retry around construction, surviving
`NoBrokersAvailable` and unknown-topic errors, retrying for the life of the
pod. Flagged as item 6 in `AGENTS.md`; now a hard blocker.

### 8.2 Consumer group must be configurable

`Consumer.__init__` takes `group_id: str = "eventrunner"` and `__main__.py`
never passes it. The group must match the ScaledObject trigger exactly, so add
`ER_CONSUMER_GROUP` (default `eventrunner`, overlay sets `kev1-eventrunner`).
A mismatch is another silent no-scale failure.

### 8.3 Graceful drain, logged to stderr

Resolves RQ-2. On SIGTERM, EventRunner must **wait for in-flight `claude` runs
to finish** rather than abandoning them, and must **say so on stderr** so the
waiting is visible in `kubectl logs`:

```text
[eventrunner] SIGTERM received; draining 2 in-flight run(s), will not accept new work
[eventrunner] still waiting on correlationid=brave-otter-4718 (42s elapsed)
[eventrunner] drain complete, exiting
```

stderr specifically, because it is unbuffered by default and interleaves
correctly with a crash trace; `kubectl logs` shows both streams.
`terminationGracePeriodSeconds` must exceed the longest expected run (600 s is
a reasonable start), otherwise the kubelet SIGKILLs mid-drain and the
politeness is wasted. Verify `router.stop()` actually joins in-flight work —
Phase 0 tested process-level shutdown, not mid-run drain.

### 8.4 Liveness must detect a wedged consumer thread

The §8.1 failure mode — live process, dead consumer thread — is exactly what a
liveness probe exists to catch, and a process check cannot see it. EventRunner
has no HTTP server, so: the consumer loop touches a heartbeat file on each
poll, and the Deployment adds an `exec` probe asserting its mtime is recent.
A few lines, and it converts a silent hang into a pod restart.

### 8.5 Arbitrary-UID portability — fixed and verified

Per Finding 3 the images did not run under OpenShift's SCC. **Both Dockerfiles
are now fixed**, so one image works on a laptop, on OpenShift and on vanilla
Kubernetes. Two separate bugs were involved.

**Bug 1 — writable paths were owned by the wrong group.** `chown -R app:app
/data` made `/data` writable only by uid 10001, and the SCC-assigned UID is not
that user. Red Hat's image guidelines and Docker's OpenShift guide agree on the
fix: OpenShift assigns the arbitrary UID to the **root group (GID 0)** and the
process always runs with `gid=0`, so every writable path must be group-0 owned
and group-writable:

```dockerfile
ENV TMPDIR=/data \
    HOME=/home/app

RUN useradd --uid 10001 --gid 0 --create-home --home-dir /home/app app \
 && mkdir -p /data \
 && chgrp -R 0 /data /app /home/app \
 && chmod -R g=u /data /app /home/app
```

`chmod g=u` copies the owner bits onto the group. Setting the user's **primary
group to 0** matters too: it makes the laptop case exercise the same permission
path as OpenShift instead of a separate owner-only path that hides the bug.

`HOME` is now set explicitly rather than left to a passwd lookup, because an
arbitrary UID has no `/etc/passwd` entry on a plain Kubernetes node. This is
load-bearing beyond tidiness — `claude` keeps session transcripts under
`$HOME/.claude`, so an unwritable `HOME` breaks `--resume` outright.

**Bug 2 — a named `USER` is rejected by vanilla Kubernetes.** With
`runAsNonRoot: true` (required by restricted Pod Security, and by OpenShift's
`restricted-v2`) the kubelet must prove the user is non-root, and it cannot do
that from a name:

```text
CreateContainerConfigError: container has runAsNonRoot and image has
non-numeric user (app), cannot verify user is non-root
```

OpenShift masked this because its SCC injects an explicit numeric `runAsUser`.
Vanilla Kubernetes does not. So the directive is numeric:

```dockerfile
USER 10001
```

**Verification.** `scripts/test_image_uids.py` runs both images against all
three UID schemes — image default, `--user 1000910000:0` (simulating the SCC),
and `--user 10001:0` — asserting each can write `$TMPDIR` and `$HOME` and
import its own config. All 6 combinations pass. Confirmed again on the real
cluster with **no volume mounted**, the exact shape that failed before:

```text
uid=1000910000 gid=0 groups=[0] HOME=/home/app TMPDIR=/data
write TMPDIR /data OK
write HOME /home/app OK
FIX-VERIFIED mock=True
```

Deployment still mounts a volume at `/data` and must **not** set `runAsUser`
(let the SCC assign one), but the image no longer depends on the mount to be
correct.

### 8.6 EventBridge is single-replica, by constraint

The event store is SQLite on local disk. Two replicas means two divergent
databases behind one Service, and the HTML view would show different history
depending on which pod answered. `kafka_requests_mirror.py` also uses a
per-process consumer group with no commits so every start re-scans the topic —
sane for one pod, duplicative for many.

So EventBridge is `replicas: 1`, with **no** HPA and no ScaledObject, and
`strategy: type: Recreate` to avoid two pods sharing a volume during rollout.
This is a constraint to state, not a limitation to fix; sharing the store means
replacing SQLite, which is out of scope.

Storage: `emptyDir` is enough for the e2e test. For a demo you want to revisit,
mount a `gp3-csi` PVC at `/data`.

### 8.7 Public base URL and the startup self-test

`EVENT_BRIDGE_PUBLIC_BASE_URL` is used verbatim in HTML links and ntfy
actions, and in-cluster it must be the Route hostname, which the pod cannot
discover. It is set explicitly after the Route exists (§13, step 4).

`eventbridge/netcands.py` will enumerate pod IPs and report them as
unreachable candidates. Cosmetic log noise, not a fault — the explicit
`EVENT_BRIDGE_PUBLIC_BASE_URL` wins.

### 8.8 Images

Already built and pushed as multi-arch manifests by
`eventing/scripts/build-images.sh`. Two caveats:

- **`:dev` is a mutable tag.** With `imagePullPolicy: Always` a restart can
  pick up a different build than its neighbours. Deploy an immutable tag
  (`--tag v0.1.0`) or a digest for anything you need to reason about.
- **The runner image ships no `claude` CLI.** Mock mode works out of the box;
  the real demo needs a derived image (§15).

### 8.9 Config surface additions

| Var | Phase 1 value | Note |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `my-cluster-kafka-bootstrap.kafka.svc:9092` | cross-namespace DNS |
| `REQUEST_TOPIC` | `kev1-requests` | §5 |
| `RESPONSE_TOPIC` | `kev1-responses` | §5 |
| `ER_CONSUMER_GROUP` | `kev1-eventrunner` | **new**, §8.2 |
| `EB_HTTP_ADDR` | `0.0.0.0:8080` | already the image default |
| `EVENT_BRIDGE_PUBLIC_BASE_URL` | the Route host | §8.7 |
| `TMPDIR` | `/data` | already the image default |
| `ER_MOCK_CLAUDE` | unset (auto) for test; `false` for demo | §15 |
| `ER_INCLUDE_RAW` | `false` in the signed overlay | shrinks signed envelopes |
| `ER_REQUIRE_SIGNATURE` | `false` | **new**, §11 |

---

## 9. Tooling: Python only — no shell scripts

**Decision: every script in `eventing/scripts/` is Python, and the existing
shell scripts are converted.** Phase 0 wrote them in bash; Phase 1 does not add
more, and retires the ones that exist.

The argument is not taste, it is this project's own history:

1. **Bash has already failed at this job three times.** `docker-e2e-test.sh`
   started with inline `python3 -c` assertions and had to have them extracted
   into a separate `_events.py` because nested quotes inside a double-quoted
   shell string are unmaintainable. While writing *this document* the same
   class of bug bit twice more — once parsing a registry manifest
   (`SyntaxError: unexpected character after line continuation character`) and
   once in a UID test matrix, where `set -- $pair` silently clobbered a loop
   variable and produced `docker: invalid reference format` while the harness
   cheerfully reported `RESULT: PASS` because `$?` had captured `sed`'s exit
   code rather than `docker`'s. **A test harness that reports PASS when the
   command never ran is worse than no harness.** That is a bash-shaped bug, and
   it is why `scripts/test_image_uids.py` is Python.
2. **`kubectl -o json` output wants a data structure**, not a `jsonpath` string
   that returns empty and exits 0 when a field moves.
3. **Polling with backoff is most of the logic.** "Wait for X, up to N seconds,
   and say exactly why if it never happens" is a function with parameters in
   Python and a copy-pasted `for i in $(seq …)` in bash. There are a dozen such
   waits across preflight, deploy and e2e.
4. **Exit-code handling in pipelines is a footgun.** `cmd | sed` reports `sed`'s
   status; `PIPESTATUS` fixes it only if you remember, and nothing warns you
   when you forget.
5. **Assertions become unit-testable.** The repo has `pytest` and 100+ tests.
   Condition parsing and grouping logic can be tested without a cluster.

Constraints kept: **stdlib only** — `subprocess`, `json`, `urllib.request`,
`argparse`, `hashlib`, `time`. Scripts shell out to `kubectl`/`docker` with
argument *lists*, never a shell string, which removes the quoting layer
entirely. No Kubernetes client library, honouring §1.1 and avoiding in-cluster
auth handling and client-version skew.

### 9.1 Target layout

```text
eventing/scripts/
├── proclib.py            subprocess helpers, Checks accumulator, PASS/FAIL output
├── k8slib.py             kubectl wrapper (-> dict), wait_for(), condition readers
├── build_images.py       was build-images.sh   (multi-arch + --local)
├── push_images.py        was push-images.sh
├── stop_demo.py          was stop-demo.sh
├── docker_e2e_test.py    was docker-e2e-test.sh (+ absorbs _events.py)
├── test_image_uids.py    the §8.5 three-UID matrix                   [written]
├── k8s_preflight.py      the §12 checks
├── k8s_deploy.py         deploy + verify, in the §13 order
├── k8s_e2e_test.py       the §14 test
├── k8s_demo_flow.py      the §15a four-stage demo-flow verification
└── k8s_teardown.py       §16 scale-to-zero + pause
```

Thin `.sh` shims are kept **only** for the two entry points that are already
documented in `README.md` and muscle memory — `docker-e2e-test.sh` and
`build-images.sh`. Each contains one `exec python3 …` line and no logic, so
there is no second implementation to drift.

`proclib.py` preserves the Phase 0 output contract — `[e2e] PASS` / `FAIL`
lines and a `Checks` object that accumulates failures so exit is 0 only when
everything passed — so converted scripts read identically in CI. Crucially, a
`Checks` object cannot report PASS for a command that failed to run, which is
the specific bug that motivated the conversion.

## 10. Manifest layout

```text
eventing/k8s/
├── base/
│   ├── kustomization.yaml
│   ├── eventbridge-deployment.yaml     replicas:1, Recreate, /healthz probes
│   ├── eventbridge-service.yaml        ClusterIP :8080
│   ├── eventbridge-route.yaml          OpenShift Route (+ Ingress variant)
│   ├── eventrunner-deployment.yaml     NO spec.replicas (§14.1), heartbeat probe
│   ├── eventrunner-scaledobject.yaml   the KEDA trigger (§7)
│   └── configmap.yaml                  the §8.9 table, minus secrets
├── topics/
│   └── kafkatopics.yaml                ns kafka, apiVersion v1 (§6)
├── kind/                               §3.2, local only — not applied to a real cluster
│   ├── kind-cluster.yaml               ingress-ready label + extraPortMappings
│   └── kafka-cluster.yaml              single-node KRaft Kafka named my-cluster
└── overlays/
    ├── test/                           mock mode, cooldownPeriod 30, emptyDir
    ├── kind/                           Route deleted, Ingress added, cooldown 30
    └── demo/                           real claude image, Secret, PVC, cooldown 300
```

`topics/` sits outside `base/` precisely because it targets a *different
namespace* and cannot participate in the overlay's `namespace:` rewrite.
Applying it is its own step and needs write access to ns `kafka`.

The `test` overlay's short `cooldownPeriod` is what makes asserting
scale-back-to-zero feasible within a test's patience (§14).

---

## 11. Signed events and causation binding

The two remaining Phase 0 non-goals. Both **feature-flagged, disabled by
default**, so the e2e path is unaffected and either can be enabled alone.

**JWS over the envelope.** A new extension attribute `ce_signature` carries a
detached JWS over a canonical serialization of the core CloudEvent attributes
plus `correlationid`, `sessionuuid`, `sequence`, and a digest of `data`.
Ed25519 keys in a read-only Secret. `ER_REQUIRE_SIGNATURE=true` makes
EventRunner refuse unsigned or badly-signed requests (log and commit, rather
than retry forever).

Canonicalization is the part that will bite: signer and verifier must agree
byte-for-byte. Sorted `key=value` lines over the signed attribute set plus
`sha256(data-bytes)` is sufficient and avoids a JCS dependency, which would
violate the pure-Python rule. Pair it with `ER_INCLUDE_RAW=false` — the `raw`
frame roughly doubles envelope size for no consumer benefit.

**Causation binding.** Each response event gains `ce_causationid` = the `id` of
the request event that caused it. Today the link is implicit via
`correlationid`, which identifies a *conversation*, not a *turn*. With
`causationid`, a `/continue` turn's responses are attributable to their
specific triggering request, which is what makes a signed audit trail
meaningful. Requires threading the request `id` through `Router.submit` →
`run_agent` → `Emitter`.

---

## 12. Pre-deployment checks

`k8s_preflight.py`, run standalone or as the first phase of deploy and e2e.
Every check fails loudly with the remediation, because each one corresponds to
a failure that is slow or confusing to diagnose later. Goal 1 (§2) is why
checks 8–9 exist before anything is deployed.

| # | Check | Fails when | Why it matters |
|---|---|---|---|
| 1 | Cluster reachable; print the context and require `--context` or explicit confirmation | wrong cluster targeted | deploying into the wrong cluster is the worst outcome here |
| 2 | CRDs present: `kafkatopics.kafka.strimzi.io` serving **v1**, `scaledobjects.keda.sh` | Strimzi/KEDA missing or v1 not served | the `v1beta2` trap (§6) |
| 3 | Strimzi operator's watched namespace == ns holding the topics | operator watches elsewhere | Finding 1 — CRs would be silently ignored |
| 4 | **Both images anonymously pullable**: unauthenticated manifest GET returns 200 **and** contains a manifest for the cluster's node architecture | repo went private, tag missing, arch absent | Finding 2 — hard fail, no pull-secret fallback |
| 5 | Node architecture ∈ image platforms | arm64-only image on amd64 nodes | `exec format error` at runtime |
| 6 | **Arbitrary-UID writability**: throwaway pod with a volume at `/data` writes `$TMPDIR` and loads config | image not OpenShift-ready | Finding 3 — this is the exact probe that caught it |
| 7 | **KEDA → Kafka reachability**: temp ScaledObject on an existing topic reaches `Ready=True` and the external metrics API returns a numeric value; then deleted | cross-namespace DNS or NetworkPolicy blocks the scaler | resolves RQ-3; a broken scaler means silent no-scaling |
| 8 | **Ingress capability**: Route CRD or IngressClass present; cluster apps domain resolvable in DNS | no ingress controller | goal 1 — fail before deploying, not after |
| 9 | **Public URL reachable end-to-end**: after EventBridge is up, `GET https://<route>/healthz` from outside the cluster returns 200 | Route created but not routable | goal 1, proven not assumed |
| 10 | **Topic name collisions**: no pre-existing `KafkaTopic` or broker topic named `<ns>-requests` / `<ns>-responses` that this deploy does not own | another user took the name | resolves RQ-5; prevents consuming someone else's data |
| 11 | Namespace exists or is creatable; quota admits the pods | quota exhausted | otherwise pods sit `Pending` with an unhelpful message |
| 12 | Default StorageClass exists, if the overlay requests a PVC | no provisioner | PVC stays `Pending` forever |
| 13 | **The claude CLI image is anonymously pullable and carries the CLI** (demo overlay only; skipped otherwise) | image missing, or built where the bundled `bun` cannot execute | T2.4's gate. Without it every real run fails with `claude binary not found` — and mock mode is exactly what stops the e2e from catching that. Runs on a cluster node rather than at build time because the CLI's `bun` aborts under QEMU user-mode emulation, so a cross-built image cannot execute it on an arm64 builder |

Check 13 was added with the demo overlay and this table documented 12 for a while, which
`IMPLEMENTATION_REPORT1.md` and `README_PHASE1.md` both reference by number — so the
count is worth keeping right rather than discovering later that the design documents 12
of 13.

Check 9 necessarily runs *after* EventBridge is deployed, so preflight has two
phases: checks 1–8 and 10–13 run before anything is applied, and check 9 runs
as the gate between deployment step 4 and step 5 (§13).

**On a Kind cluster (§3.2)** the same checks apply, with three differences that are
properties of the environment rather than exemptions. Check 6 (arbitrary-UID
writability) passes trivially because there is no SCC to assign a foreign UID —
it proves nothing there, and the OpenShift run is what makes it meaningful. Check 8
finds an `IngressClass` rather than a Route API, which is exactly the branch it was
written to distinguish. And on a bare Kind cluster checks 2, 3 and 8 all fail until
`kind_setup.py` has run; that failure IS the gap report, so the script's `--check`
mode is a thin wrapper over the same assertions rather than a second
implementation.

---

## 13. Deployment steps

`python3 scripts/k8s_deploy.py [--namespace kev1] [--overlay test]` performs
these in order. The manual equivalents are shown because they are what you
will type when debugging.

**0. Preflight** — §12 checks 1–8, 10–13. Abort on any failure.

**1. Namespace.**

```bash
NS="${NS:-kev1}"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
```

**2. Topics — in ns `kafka`, not `$NS`.** Before the app, because §8.1
mitigates the dependency but does not remove it.

```bash
kubectl apply -f eventing/k8s/topics/kafkatopics.yaml
kubectl -n kafka wait --for=condition=Ready kafkatopic/kev1-requests  --timeout=120s
kubectl -n kafka wait --for=condition=Ready kafkatopic/kev1-responses --timeout=120s
```

**3. EventBridge + Service + Route.** EventBridge first, alone, because goal 1
is the public URL and there is no point starting EventRunner until the face of
the demo is proven reachable.

```bash
kubectl apply -k eventing/k8s/overlays/test
kubectl -n "$NS" rollout status deploy/eventbridge --timeout=180s
```

**4. Publish and verify the public URL** — §12 check 9, the gate.

```bash
HOST=$(kubectl -n "$NS" get route eventbridge -o jsonpath='{.spec.host}')
kubectl -n "$NS" set env deploy/eventbridge "EVENT_BRIDGE_PUBLIC_BASE_URL=https://$HOST"
kubectl -n "$NS" rollout status deploy/eventbridge --timeout=120s
curl -fsS "https://$HOST/healthz"          # MUST return 200 before proceeding
```

**5. EventRunner + ScaledObject.** Applied by the same overlay; verify the
idle state, which is **zero replicas** — that is correct, not a failure.

```bash
kubectl -n "$NS" get scaledobject eventrunner   # READY=True  ACTIVE=False
kubectl -n "$NS" get deploy eventrunner         # READY 0/0
```

If a previous teardown (§17) paused the ScaledObject, step 5 must unpause it:

```bash
kubectl -n "$NS" annotate scaledobject eventrunner \
  autoscaling.keda.sh/paused-replicas- autoscaling.keda.sh/paused- 2>/dev/null || true
```

---

## 14. Minimal e2e test

`scripts/k8s_e2e_test.py` (shim: `scripts/k8s-e2e-test.sh`), mirroring
`docker-e2e-test.sh` in structure, flags and output style — same `[e2e] PASS`
lines, same "exit 0 only if every assertion passed" contract. Runs **from a
laptop with `kubectl`**, so it needs no in-cluster image.

```bash
./scripts/k8s-e2e-test.sh                    # detect, deploy if needed, test
./scripts/k8s-e2e-test.sh --namespace kev2
./scripts/k8s-e2e-test.sh --force-deploy     # deploy even if unchanged
./scripts/k8s-e2e-test.sh --no-deploy        # fail if not already deployed
./scripts/k8s-e2e-test.sh --keep             # skip teardown
```

Mock mode is forced (no credential forwarded), so runs are free and
deterministic.

### 14.1 Deciding whether a deploy is needed

The requirement is to redeploy when the YAML changed and skip it otherwise.
Two mechanisms, used together:

**Authoritative: `kubectl diff -k <overlay>` exit code.** It renders the
overlay, sends a server-side dry-run, and compares against live objects.
Exit `0` = no differences, `1` = differences, `>1` = error. This is better than
any local bookkeeping because it also catches **cluster-side drift** — someone
hand-editing a Deployment — which a file hash cannot see.

One prerequisite makes this work: **the EventRunner Deployment must not declare
`spec.replicas`.** KEDA owns that field, so if the manifest pins it, `diff`
reports a difference forever the moment KEDA scales, and "has anything
changed?" answers yes on every run. Omitting `replicas` from a Deployment
managed by a ScaledObject is the recommended practice anyway; here it is also
what makes change detection usable.

**Fast path: a manifest digest.** `kubectl kustomize <overlay>` piped through
`hashlib.sha256`, compared against an annotation stamped at deploy time:

```text
rossoctl.dev/manifest-sha256: <hex>     # on the namespace, written by k8s_deploy.py
```

One API call, no per-object dry-run, and it gives a definite "identical
manifests" answer. Used as a cheap gate: if the digest matches, run `diff` to
confirm no drift; if it differs, deploy without bothering to diff.

Known caveats to handle rather than be surprised by: `kubectl diff` needs the
namespace to exist (so it runs after step 1), it reports nothing useful if the
CRDs are missing (hence §12 check 2 first), and defaulted or webhook-mutated
fields can produce cosmetic diffs — if one shows up, the fix is to stop
declaring that field, not to ignore the tool.

### 14.2 Assertions

Assertions 4, 6, 7 and 11 are the Phase 1 point — they test KEDA rather than
the wire. The rest carry over from Phase 0 and guard against regression.

| # | Assertion |
|---|---|
| 1 | Preflight (§12) passes |
| 2 | Both `KafkaTopic`s reach `Ready=True` |
| 3 | `deploy/eventbridge` available; `/healthz` 200 **on the public URL** |
| 4 | **Idle state**: ScaledObject `Ready=True`, `Active=False`, eventrunner `0/0` |
| 5 | `POST /v0/agents` → 202 with a `correlationid` |
| 6 | **KEDA activates**: within 60 s `Active=True` and replicas ≥ 1 |
| 7 | **A pod starts** and its log announces `mock_claude=True` with an `auto:` reason |
| 8 | A `final=true` event arrives within 120 s |
| 9 | The reply contains `MOCK-REPLY` |
| 10 | `events.jsonl` non-empty; `/turns` pairs the prompt with its turn |
| 11 | **Scale back to zero**: after the overlay's `cooldownPeriod`, replicas return to 0 |
| 12 | Cold-start latency recorded: POST → first `phase=stdout` event (RQ-4) |

Assertion 11 is what proves scale-*to*-zero rather than merely
scale-*from*-zero, and is why the test overlay sets `cooldownPeriod: 30`.
Budget ~90 s for it.

### 14.3 Failure triage

On any failure the script dumps, rather than merely reporting:

- `describe scaledobject eventrunner` — trigger errors live in its conditions.
- `logs deploy/eventrunner --tail=100` — a dead consumer thread (§8.1) shows as
  a traceback followed by silence.
- `logs -n keda deploy/keda-operator --tail=50` — scaler-side errors.
- `get events --sort-by=.lastTimestamp | tail -20` — image pulls, SCC denials,
  quota rejections.

---

## 15. The actual demo (real `claude`)

The e2e test proves plumbing with a mock. The demo runs a real agent and needs
three things the test does not.

**1. An image containing the `claude` CLI.** The runner image omits it
deliberately, so a derived image is required:

```dockerfile
FROM quay.io/aslomnet/rossoctl-eventrunner:dev
USER 0
RUN <install the claude CLI for linux/amd64>
RUN chgrp -R 0 /data /app && chmod -R g=u /data /app   # keep §8.5 valid
USER app
```

Without this, every real run fails with `claude binary not found`. Mock mode is
what stops the e2e test from catching that, so it must be checked here.

**2. A credential, and the matching mode override.**

```bash
kubectl -n "$NS" create secret generic anthropic \
  --from-literal=ANTHROPIC_AUTH_TOKEN='<token>'
```

Consumed via `envFrom.secretRef`. The Phase 0 auto-detection would select real
mode on its own, but set `ER_MOCK_CLAUDE=false` explicitly so the mode is a
declaration rather than a side effect of whether a Secret happened to mount.

**3. The verified public URL** from §13 step 4.

Then drive it exactly as in Phase 0 — the HTTP surface is unchanged:

```bash
HOST=$(kubectl -n "$NS" get route eventbridge -o jsonpath='{.spec.host}')

CORR=$(curl -sS -X POST "https://$HOST/v0/agents" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Name one animal starting with O.","max_turns":1}' \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["correlationid"])')

open "https://$HOST/v0/agents/$CORR"
```

The demo-worthy part is the second window:

```bash
kubectl -n "$NS" get deploy eventrunner -w
```

Zero replicas before the POST, a pod appearing within seconds of it, the
transcript streaming into the HTML view, and the count falling back to zero a
few minutes after the run completes. Then `POST /continue` on the same
correlationid and watch it wake again and resume the same `claude` session.

---

## 16. Demo flow — stage by stage

The demo is a four-stage loop: **zero agent replicas → a task event wakes the
agent → the agent runs and streams events → back to zero once idle.** This
section reviews the design against that flow, names the mechanism and the
observable evidence for each stage, and records three gaps the review found.

`scripts/k8s_demo_flow.py` asserts all four stages in order and prints the
measured timings, so the flow is verified rather than narrated.

### Stage 0 — idle at zero

| | |
|---|---|
| Mechanism | `minReplicaCount: 0` (§7) |
| Evidence | `kubectl -n kev1 get deploy eventrunner` → `0/0`; ScaledObject `Active=False` |
| Watch | `kubectl -n kev1 get deploy eventrunner -w` in a second window |

**Boundary to state out loud when demoing:** the *agent* scales to zero, not the
whole system. EventBridge stays at one replica — something has to accept the
POST and hold the event store (§8.6). The claim is "no agent capacity is
consumed while idle", not "nothing is running".

### Stage 1 — a task event wakes the agent

| | |
|---|---|
| Mechanism | `POST /v0/agents` → request event on `kev1-requests` → lag 1 → KEDA poll (≤ `pollingInterval` 5 s) → `activationLagThreshold: 0` → 0→1 |
| Evidence | ScaledObject flips `Active=True`; a pod goes `Pending`→`Running` |
| Durability | the request is safe with zero consumers — Kafka retains it; the pod reads from the group's committed offset when it joins |

**Gap A — every wake pays a fixed 3 s broker delay.** `my-cluster` leaves
`group.initial.rebalance.delay.ms` at its default, measured on the broker:

```text
group.initial.rebalance.delay.ms=3000  synonyms={DEFAULT_CONFIG:...=3000}
```

Scale-from-zero forms a *new* consumer-group generation every time, and the
broker deliberately waits 3 s before completing that first rebalance so that
co-starting members can join one round. With one pod that wait buys nothing —
it is a third of RQ-4's 10 s budget spent idle, and it looks like "slow
startup" rather than a configured delay.

The lever is `spec.kafka.config` on the Kafka CR, but **`my-cluster` is shared**
with other namespaces and this is a broker-wide setting, so Phase 1 does not
change it. Recorded as a known floor on wake latency instead: budget
≈ 3 s rebalance + 0–5 s image pull + interpreter start.

### Stage 2 — the agent runs and streams events

| | |
|---|---|
| Mechanism | consumer → per-corr router → `Popen(claude)` → stream-json frames → response events on `kev1-responses` → EventBridge → SQLite → HTML/SSE |
| Evidence | the HTML transcript fills live; `GET /events` shows `phase=stdout`; `/turns` pairs prompt with reply |
| Watch | the Route URL in a browser, or `curl -sN .../events.sse` |

**This is where RQ-1 does double duty.** Committing the offset only after the
terminal event means lag stays ≥ 1 for the whole run, so KEDA keeps seeing work
in flight and does not scale the pod away mid-stream. The fix chosen for
"never miss an event" also solves "don't kill a running agent".

**Gap B — `claude` session state is ephemeral, so `/continue` does not survive
a scale-to-zero cycle.** `claude --resume <uuid>` needs the transcript at
`$HOME/.claude/projects/<encoded-cwd>/<uuid>.jsonl`, and `HOME` is on the
container's **ephemeral layer**. So:

1. turn 1 (`mode=start`) runs in pod A and writes the transcript into pod A
2. the demo goes idle and pod A is destroyed
3. turn 2 (`mode=continue`) starts pod B, which runs `claude --resume <uuid>`
   against a filesystem that has never seen that session

Phase 0 never hit this because one long-lived process owned the transcripts.
The §15 demo line — "POST /continue and watch it resume the same session" —
**will fail** if the pod scaled away in between.

#### Researched options

Facts established from the Claude Code docs and from this cluster, with the
CLI at **2.1.278**:

- **`--resume` accepts a transcript path, not just an ID.** Per the headless
  docs: *"In place of the session ID, you can pass `--resume` the absolute path
  to a session's `.jsonl` transcript file, and Claude Code continues the
  conversation stored in that file."* This is the load-bearing fact — it means
  the transcript does **not** have to be reconstructed at the exact
  cwd-derived path.
- **The transcript is self-contained.** A resume needs that one `.jsonl`; there
  is no sibling index or database required.
- **The storage location is relocatable.** `CLAUDE_CONFIG_DIR` moves the whole
  `~/.claude` tree, and `CLAUDE_CODE_PROJECT_DIR_NAME` (needs ≥ 2.1.234; we
  have 2.1.278) pins the `projects/<name>` segment instead of deriving it from
  cwd — removing the dependency on cwd encoding, which is lossy (every
  non-alphanumeric character becomes `-`).
- **The Agent SDK has a `SessionStore` adapter** built for precisely this,
  documented as being for *"serverless functions, autoscaled workers ... don't
  share a filesystem"* and *"local containers are ephemeral"*. It is
  **SDK-only**, not available to a CLI subprocess.
- **This cluster has no RWX storage.** Only `gp2-csi`/`gp3-csi` (EBS,
  ReadWriteOnce); no EFS/NFS/CephFS CSI driver is installed, and all 12 PVCs
  cluster-wide are `ReadWriteOnce`.

| Option | Verdict |
|---|---|
| **1. Checkpoint the transcript through EventBridge, resume by path** | **chosen** — see below |
| 2. Agent SDK `SessionStore` adapter | right long-term shape, but requires replacing the `Popen(claude)` architecture (§4.3) with the Python Agent SDK, and that dependency must first be audited against the §1.1 pure-Python / free-threaded rule. Deferred to Phase 2 |
| 3. RWO PVC + `maxReplicaCount: 1` | works with zero code change; caps the deployment at one replica and AZ-pins the PVC. Kept as the **interim** state until option 1 lands |
| 4. Shared RWX volume for `~/.claude` | **not available** on this cluster, and concurrent multi-host access to one `~/.claude` is undocumented — session files are append-only but record ordering would race. Rejected |
| 5. Stateless context replay from EventBridge's stored turns | most portable, but needs the SDK or raw API for a `messages` array and discards `claude`'s own session state (tool history). Rejected for Phase 1 as a semantic change, not a deployment change |
| 6. StatefulSet + per-pod PVC via `volumeClaimTemplates` | KEDA can target a StatefulSet (`scaleTargetRef` takes `kind`), and per-pod PVCs survive scale-down. But correctness needs partition→ordinal affinity, and then **scale-to-zero breaks the wake**: a task landing on partition 5 goes unserved while only pod-0 exists. Rejected |

#### Chosen: checkpoint the transcript through EventBridge

Because `--resume` takes a path and the transcript is self-contained, the
runner can stay stateless without any shared filesystem:

- **After** a turn reaches its terminal event, EventRunner `PUT`s the
  transcript to EventBridge at `/v0/agents/<corr>/transcript`.
- **Before** a `mode=continue` turn, it `GET`s the transcript into a local
  scratch path and passes **that path** to `--resume`.

Why this fits rather than bolting on infrastructure:

- **EventBridge is already the single-replica stateful component** with a
  volume and a SQLite store (§8.6). It is the natural home for per-correlation
  state, and it needs no new backing service — no S3, no Redis.
- **No concurrent-writer problem.** Requests are keyed by `correlationid`, so
  all turns for one conversation land on one partition and therefore one pod
  (§4), and the per-corr router serializes them within that pod. Exactly one
  writer per session, which is the property options 4 and 6 could not give.
- **It lifts the replica cap.** With state in EventBridge rather than on a pod,
  `maxReplicaCount` returns to being bounded by partitions (6), not storage.
- **It keeps the CLI.** No SDK migration, no new dependency, no change to the
  free-threaded runtime story.

Costs and risks to hold in view: the transcript grows with conversation length,
so it is a blob that wants a size cap and a retention rule; a restored
transcript can contain absolute paths from earlier tool results, which stay
valid only because the per-correlation workdir is derived deterministically
from `corr` under a fixed `TMPDIR=/data`; and the path form of `--resume` is
**documented but not yet verified locally**, so it is the first gate on the
implementation task (T1.7).

Until T1.7 lands, the deployed configuration is option 3 — `maxReplicaCount: 1`
plus a PVC and `CLAUDE_CONFIG_DIR=/data/claude` — so the demo flow works
throughout, and the replica cap is raised as a follow-up rather than being a
prerequisite.

### Stage 3 — back to zero when idle

| | |
|---|---|
| Mechanism | terminal event → offset commit → lag 0 → `cooldownPeriod` elapses → scale to `minReplicaCount: 0` |
| Evidence | replicas return to `0/0`; ScaledObject `Active=False` |
| Timing | `cooldownPeriod` 300 s in the demo overlay, 30 s in the test overlay so the assertion is affordable |

**Gap C — a week of idleness causes a replay storm.** The broker keeps
committed offsets for a limited time, measured:

```text
offsets.retention.minutes=10080          # 7 days
offsets.retention.check.interval.ms=600000
```

A consumer group with **zero members** for longer than 7 days has its committed
offsets deleted. The next wake finds no offset, falls back to
`auto_offset_reset=earliest`, and replays **every request still inside the
topic's retention** — one new task triggering a re-run of the whole backlog.

For a demo that is idle by design, a week between uses is ordinary, so this is
likely rather than theoretical. It is not corruption — RQ-1 already accepts
at-least-once and assumes idempotent agents — but it is a burst of real API
spend, and most of the replayed `mode=continue` requests will fail anyway
because their sessions are long gone (Gap B).

Mitigations, in order of effort:

- **Bound the blast radius**: set the topics' `retention.ms` *below*
  `offsets.retention.minutes` — 24 h rather than the 7 days in §6 — so the
  worst case replays a day, not a week.
- **Commit a starting offset at first start**, so the group always has one.
- Raising `offsets.retention.minutes` is broker-wide on a shared cluster, so it
  is not an option here for the same reason as Gap A.

Phase 1 takes the first two. §6's `retention.ms` is therefore `86400000`, not
`604800000`.

### What the flow verification asserts

| Stage | Assertion in `k8s_demo_flow.py` |
|---|---|
| 0 | replicas `0`, `Active=False` before anything is posted |
| 1 | after POST: `Active=True` and replicas ≥ 1 within 60 s; **wake latency recorded** |
| 2 | a `phase=stdout` event arrives, then `final=true`; reply contains the expected text; replicas stayed ≥ 1 for the whole run |
| 2b | `/continue` on a warm pod resumes the same session |
| 2c | `/continue` after a full scale-to-zero cycle also resumes (guards Gap B) |
| 3 | replicas return to `0` within `cooldownPeriod` + slack; `Active=False` |

---

## 17. Teardown — scale down, delete nothing

Teardown returns the demo to zero cost **without destroying anything that is
expensive or shared to recreate**. Explicitly preserved: the namespace, the
ScaledObject, the KafkaTopics and their data, and KEDA itself.

`python3 scripts/k8s_teardown.py [--namespace kev1]`:

```bash
NS="${NS:-kev1}"

# 1. Disable KEDA autoscaling for this topic, pinned at zero. KEDA keeps
#    watching nothing: it holds the Deployment at 0 and ignores new lag.
kubectl -n "$NS" annotate scaledobject eventrunner \
  autoscaling.keda.sh/paused-replicas=0 --overwrite

# 2. Scale both workloads to zero.
kubectl -n "$NS" scale deploy/eventrunner --replicas=0
kubectl -n "$NS" scale deploy/eventbridge --replicas=0
```

Verified on this cluster: annotating a live ScaledObject with
`paused-replicas=0` moved it to `Paused=True reason=ScaledObjectPaused` and
held the target Deployment at `replicas: 0` **even while `Active=True`** —
i.e. with real lag on the topic, KEDA still did not scale up. That is the
property teardown needs: `paused-replicas` is preferable to the plain
`paused: "true"` annotation, which freezes the count wherever it happens to be
rather than pinning it to zero.

Resume without reapplying anything:

```bash
kubectl -n "$NS" annotate scaledobject eventrunner \
  autoscaling.keda.sh/paused-replicas- --overwrite
kubectl -n "$NS" scale deploy/eventbridge --replicas=1
```

What teardown deliberately does **not** do, and why:

| Not done | Reason |
|---|---|
| `kubectl delete namespace kev1` | destroys the Route hostname, any PVC and its history; the namespace is the stable home for this demo |
| Delete the `KafkaTopic` CRs | **deleting the CR deletes the real topic and all its data.** Recreating is cheap; losing an audit trail mid-investigation is not |
| Delete the ScaledObject | pausing achieves the same idle cost and keeps the tested configuration in place |
| Touch KEDA or Strimzi | cluster-shared operators; other namespaces depend on them |
| Delete Secrets | left in place so resume needs no credential re-entry. Remove manually if the cluster is shared |

Two consequences worth stating: a paused ScaledObject still shows
`Active=True` while messages sit in the topic, which looks alarming but is
correct; and the EventBridge Route stays resolvable while pointing at zero
endpoints, so it will serve a 503 rather than a DNS error.

A genuinely complete removal — for abandoning the demo, not idling it — is a
separate, explicit `--purge` mode that additionally deletes the app objects and
the two `KafkaTopic` CRs in ns `kafka`. It is not the default and prints what
it is about to destroy.

---

## 18. Non-goals (Phase 1)

Carried over from Phase 0 §8 unless resolved above. Still deferred:

- **Job-per-request isolation** and per-request capability envelopes — §4.
- **Non-idempotent agent support** — §19 RQ-1.
- **Auth on EventBridge HTTP.** A Route makes it reachable by anyone who
  learns the hostname. Acceptable on a demo cluster and nowhere else.
- **Multi-replica EventBridge** — needs a shared store, §8.6.
- **A dedicated Kafka cluster per namespace** — needs a cluster-scoped Strimzi
  install, Finding 1.
- **TLS to Kafka.** Plaintext `:9092` in-cluster; `:9093` plus a `KafkaUser`
  is the next step.
- **Resource requests/limits tuned from measurement.** Placeholders until a
  real run produces numbers.
- **In-cluster e2e as a Job**, and CI integration.

---

## 19. Resolved questions

Revision 1 listed these as open. All five are now decided, three of them by
measurement against the live cluster.

### RQ-1 — Offsets commit *after* the terminal event; agents are assumed idempotent

**Decision: prioritize never missing an event.** The commit moves to after the
terminal (`final=true`) event is emitted, which makes consumer lag mean "work
not finished" rather than "message not read". This fixes the §7.1 scale-down
race at its root: KEDA cannot see zero lag while a run is still streaming.

The cost is **at-least-once delivery**. A pod killed mid-run causes the request
to be redelivered and the agent run to repeat. Phase 1 accepts this and
assumes **agent actions are idempotent and safe to repeat** — true for the
read-and-summarize workloads the demo exercises. Consumers must therefore
tolerate duplicate response events; deduplicating on
`(correlationid, sequence)` is sufficient, and the store's existing
per-correlation sequence makes that natural.

**Future phases — non-idempotent agents.** An agent that sends mail, merges a
PR or moves money cannot be re-run blindly. The shape of the fix, for a later
phase:

- **Declare it.** Non-idempotency becomes an explicit property of the request
  rather than an assumption — e.g. a `ce_idempotent: "false"` extension
  attribute, or `data.delivery: "at-most-once"`. Defaulting to *idempotent*
  keeps existing behaviour; an agent that needs stricter handling must say so,
  and the runner can refuse what it cannot guarantee.
- **Claim before running.** For declared non-idempotent requests, the runner
  takes an exclusive, durable claim keyed by the request `id` before spawning
  anything, and a redelivery that finds an existing claim reports a terminal
  `phase=error` instead of re-running. SQLite cannot back this across pods, so
  it needs a shared store or a log-compacted Kafka claims topic.
- **Make the effect transactional where possible.** The strongest version is
  an outbox: the agent's external effect and its claim commit together. That is
  a much larger design and belongs with the Job-per-request work in §4.

### RQ-2 — Drain in-flight runs, and say so on stderr

**Decision: wait.** SIGTERM stops intake and waits for in-flight `claude` runs
to finish, logging progress to **stderr** so `kubectl logs` shows what the pod
is waiting on and for how long. Specified in §8.3, with
`terminationGracePeriodSeconds` sized above the longest expected run. Still to
verify in code: that `router.stop()` joins in-flight work rather than
abandoning it.

### RQ-3 — KEDA reaches the Kafka bootstrap across namespaces ✅ verified

Measured, not assumed. A probe ScaledObject in ns `kev1` with a Kafka trigger
against `my-cluster-kafka-bootstrap.kafka.svc:9092` reported:

```text
Ready=True   reason=ScaledObjectReady
Active=True  reason=ScalerActive
```

and the external metrics API returned a real value:

```text
GET /apis/external.metrics.k8s.io/v1beta1/namespaces/kev1/s0-kafka-keda-test-topic
{"metricName":"s0-kafka-keda-test-topic","value":"3"}
```

A numeric lag value can only come from KEDA having connected to the broker in
ns `kafka` from ns `keda` and queried the topic. Cross-namespace reachability
is confirmed on this cluster, with no NetworkPolicy in the way. Because that
can change, §12 check 7 re-runs this probe at deploy time. It also produced the
correction recorded in §7.1.

### RQ-4 — Cold-start latency is assumed low (< 10 s)

**Decision: assume < 10 s** and treat scale-from-zero as acceptable for
interactive use. One honest caveat from measurement: pulling the 245 MB runner
image on a node that has never seen it took **5.1 s**, before container start,
free-threaded CPython init and Kafka group join. So a *first-ever* pod on a
fresh node will likely exceed 10 s, while subsequent pods on a warm node should
be well inside it. Assertion 12 of the e2e test records the real number every
run, which turns this assumption into data at no extra cost. If the cold case
matters, pre-pulling the image onto nodes is the lever.

### RQ-5 — Topic-name collisions are checked before deploy

**Decision: check, do not assume.** The `kev1-` prefix (§5) makes a collision
unlikely, and §12 check 10 makes it impossible to hit silently: preflight fails
if a `KafkaTopic` CR or broker topic of that name already exists and was not
created by this deployment. This matters because consuming someone else's topic
would look like a working deploy that mysteriously processes foreign events.

---

## 20. Implementation tasks

Ordered, and each gated by a test that must pass before the next task starts.
The sequence ends with the §16 demo-flow verification, so "done" means the
four-stage loop has been *observed*, not that code was written.

### T0 — Convert the tooling to Python (§9). No behaviour change.

| # | Task | Gate |
|---|---|---|
| T0.1 | `proclib.py` — subprocess helpers, `Checks` accumulator, PASS/FAIL output | `pytest tests/test_proclib.py`, including a regression test that `Checks` **cannot** report PASS for a command that never ran (the §9 bug) |
| T0.2 | `k8slib.py` — kubectl wrapper returning dicts, `wait_for()`, condition readers | `pytest tests/test_k8slib.py` against captured kubectl JSON, incl. a missing-field case that must raise rather than return empty |
| T0.3 | `build-images.sh` → `build_images.py` | `python3 scripts/build_images.py --local` builds both images and `test_image_uids.py` still passes against them |
| T0.4 | `push-images.sh` → `push_images.py` | `--dry-run` prints the exact refs; a real push yields a 2-platform manifest |
| T0.5 | `stop-demo.sh` → `stop_demo.py` | `pytest tests/test_pidfile.py` plus new stale-vs-live PID cases |
| T0.6 | `docker-e2e-test.sh` → `docker_e2e_test.py`, absorbing `_events.py` | **the full docker e2e passes with the same assertions it passes today** |
| T0.7 | Retire the shell versions, leaving one-line `exec` shims for the two documented entry points | no `scripts/*.sh` contains logic beyond a single `exec` |

T0.6 is the load-bearing one: the shell version passes end to end **today**, so
there is a known-good baseline to diff against. Convert while that is still
true, before other changes muddy the comparison.

### T1 — Application changes for Kubernetes (§8)

Every gate here is a unit test, deliberately: T1 can be finished and reviewed
before any manifest exists.

| # | Task | Gate |
|---|---|---|
| T1.1 | Kafka consumer retry loop (§8.1) | a consumer whose construction raises `NoBrokersAvailable` 3× still ends up subscribed, and the thread never dies |
| T1.2 | `ER_CONSUMER_GROUP` config (§8.2) | default `eventrunner`, env override honoured, value actually reaches `Consumer` |
| T1.3 | Graceful drain + stderr progress (§8.3) | SIGTERM with a run in flight waits for it, and the drain lines appear on **stderr** |
| T1.4 | Heartbeat file + staleness check (§8.4) | the loop touches the file; a frozen clock makes staleness detectable |
| T1.5 | Offset commit moves after the terminal event (RQ-1) | no commit before the terminal event; duplicate delivery deduped on `(correlationid, sequence)` |
| T1.6 | Commit a starting offset on first start (§16 Gap C) | a fresh consumer group has a committed offset before the first poll returns |
| T1.7 | **Verify `--resume <absolute path to .jsonl>` empirically** (§16 Gap B) — the fact the chosen fix rests on, documented but unproven here | a session started in dir A resumes from a transcript copied to dir B by path, and the reply demonstrates retained context. If this fails, Gap B falls back to option 3 and T1.8–T1.9 are dropped |
| T1.8 | EventBridge transcript endpoints: `PUT`/`GET /v0/agents/<corr>/transcript`, stored per correlation with a size cap | unit tests: round-trip, size-cap rejection, 404 for unknown corr, and idempotent overwrite |
| T1.9 | EventRunner checkpoint/restore around each turn (§16 Gap B) | unit test: a `mode=continue` turn with no local transcript fetches and resumes by path; a terminal event triggers exactly one upload |
| T1.10 | Raise `maxReplicaCount` to 6 once T1.7–T1.9 pass | two concurrent correlations run on two pods, and each `/continue` still resumes the right session |

### T2 — Images (§8.5)

| # | Task | Gate | Status |
|---|---|---|---|
| T2.1 | GID-0 permissions, numeric `USER`, explicit `HOME`, both Dockerfiles | `test_image_uids.py` → 6/6 | **done** |
| T2.2 | Verify on real OpenShift with **no** volume mounted | pod `Succeeded`, writes `$TMPDIR` and `$HOME` | **done** |
| T2.3 | Multi-arch manifest including the cluster's arch | `imagetools inspect` shows amd64 + arm64 | **done** |
| T2.4 | Derived `rossoctl-eventrunner-claude` image carrying the CLI (§15) | `claude --version` answers in-image and §8.5 permissions still hold | todo |

### T3 — Manifests (§10)

| # | Task | Gate |
|---|---|---|
| T3.1 | `base/` Deployments, Service, Route, ConfigMap | `kubectl apply --dry-run=server` clean for every file |
| T3.2 | `eventrunner-scaledobject.yaml` | dry-run clean, **and** a test asserting the Deployment declares no `spec.replicas` (§14.1) |
| T3.3 | `topics/kafkatopics.yaml` — `kafka.strimzi.io/v1`, ns `kafka`, 24 h retention | dry-run clean, then `Ready=True` on the cluster |
| T3.4 | `overlays/test` (mock, cooldown 30, emptyDir) and `overlays/demo` (real CLI, Secret, PVC, `HOME=/data/home`, cooldown 300) | both render; dry-run clean; a test asserts the demo overlay puts `HOME` on the volume (§16 Gap B) |

### T4 — Deployment tooling (§12, §13)

| # | Task | Gate |
|---|---|---|
| T4.1 | `k8s_preflight.py`, checks 1–12 | each check unit-tested with mocked kubectl; the suite runs green against ykt1 |
| T4.2 | `k8s_deploy.py` in the §13 order, public URL gated before EventRunner starts | deploys into `kev1`; `/healthz` 200 on the Route; idle state `0/0` |
| T4.3 | Change detection (§14.1) | a re-run reports "no deploy needed"; touching a manifest reports "deploy needed"; KEDA scaling does **not** register as drift |
| T4.4 | `k8s_teardown.py` (§17) | after teardown: replicas 0, `Paused=True`, and the namespace plus both topics still exist |

### T5 — End-to-end and demo-flow verification

The finish line. Nothing here passes until T0–T4 are done.

| # | Task | Gate |
|---|---|---|
| T5.1 | `k8s_e2e_test.py` assertions 1–12 (§14.2) | all pass against ykt1, exit 0 |
| T5.2 | Wake-latency measurement vs RQ-4 | recorded every run; flagged when > 10 s on a warm node |
| T5.3 | **`k8s_demo_flow.py` — the §16 four-stage loop** | zero → wake → streaming → back to zero, each asserted, with timings printed |
| T5.4 | `/continue` on a warm pod | the second turn resumes the same session |
| T5.5 | **`/continue` across a full scale-to-zero cycle** | resumes successfully — the regression gate for §16 Gap B |
| T5.6 | Idle-replay guard | a group idled past offset retention does not replay a day of tasks (§16 Gap C), exercised by deleting the group's offsets rather than waiting |
| T5.7 | Teardown, then resume | after §17 teardown and un-pause, one more full demo-flow pass succeeds |

T5.5 and T5.6 exist specifically because they are the two gaps this review
found. Without them the demo works on the day it is built and fails a week
later, which is the worst possible failure mode for something whose entire
point is sitting idle at zero.

---

## 21. Agent groups — batch fan-out with a tracked fan-in

A new first-class object: a **group** of related agent runs, submitted together and
tracked to completion as a unit, with its own page, its own rows in the store, and
exactly two notifications — one when it starts and one when it finishes.

This is the fan-out/fan-in pattern, and the literature on it is blunt about the two
things that go wrong: completion accounting under at-least-once delivery, and
stragglers. Both are addressed below, and one of them requires **modifying the
original brief** — see §21.5.

The UX half is unusually well served by existing research, because "AI agents bring
back the batch job" is now a named phenomenon rather than a metaphor. §21.7 cites it.

### 21.1 Why a group is an object, not a query

A group could be faked with a shared label and a filtered list. It is a stored object
instead, because three things must be true that a query cannot provide:

1. **An expected count declared up front.** Without it there is no denominator, and
   therefore no honest progress bar (§21.7).
2. **Exactly one completion transition.** "The last agent just finished" is an event
   that must fire once, and must survive an EventBridge restart. That is a stored
   state machine, not a `SELECT`.
3. **A notification policy scoped to the group** — suppress the members, announce the
   whole. The suppression decision needs to know the group exists and is still open.

### 21.2 The wire: one extension attribute, two event types

**`ce_groupid`** joins `correlationid` / `sessionuuid` / `sequence` as an extension
attribute on both request and response events. Lowercase and unpunctuated, matching
the existing names and the CloudEvents naming rule. An agent run belongs to at most
one group, for the life of the correlation.

Two new event types:

| Type | Meaning |
|---|---|
| `dev.rossoctl.agent.group.started.v1` | a group was created; carries `expected`, `label`, and the member correlationids known at creation |
| `dev.rossoctl.agent.group.completed.v1` | the group reached a terminal state; carries totals, duration, failure count and `reason` |

**Both go on the `responses` topic, not `requests`.** This is not arbitrary.
EventRunner consumes `requests` and would treat anything there as an agent run to
execute; a group event has no prompt and no session. Meanwhile EventBridge already
consumes `responses`, and that consumer already feeds both the store and the ntfy
publisher — so putting group events there gets persistence, notification and
auditability through the existing path with no new plumbing.

Because group events carry `groupid` but no `correlationid`, **EventBridge's responses
consumer must route on `type`**: group types update the groups tables, everything else
goes to `insert_response()` as today. Without that split, `Store.insert_response`
would be handed an event with no primary key.

The group lifecycle is therefore replayable from Kafka alone, which matters because
the topic is the audit trail and the SQLite store is a cache of it.

**But being replayable is not the same as being replayed**, and the first
implementation shipped the gap. EventBridge's live responses consumer uses a *fixed*
group id and *commits* offsets — it must, or every start would re-insert every
response ever written. So a restarted pod resumes at its committed offset and re-reads
nothing. With the `test` and `kind` overlays' `emptyDir` at `/data`, a restart meant an
empty store and HTTP 404 for every group that existed before it — while the two ntfy
notifications for those groups had already been delivered, so following one landed on
`unknown groupid`.

The fix is a **second, read-only consumer** — `GroupMirror`, the same shape as the
`RequestsMirror` that already back-fills prompts:

- it **assigns every partition by hand with no consumer group**, seeks to the
  beginning, and reads to a one-time snapshot of the end offsets. No group means no
  coordinator round trip, no rebalance, and no offsets to commit or collide with;
  completion is decided by comparing position against those end offsets, not by a poll
  coming back empty (a poll returns empty during warm-up, which an earlier version
  mistook for the end of the topic and so replayed nothing);
- it is **one-shot** — it catches up and exits, leaving the topic to the live consumer,
  rather than shadowing it forever;
- it **rebuilds only the group tables**. Response rows stay the committing consumer's
  job, because re-inserting every response on every start is precisely the cost that
  consumer exists to avoid;
- it **never publishes and never notifies**. A replay reconstructs state; it does not
  create facts. Re-publishing `group.completed` would re-notify a batch that finished
  hours ago.

The one exception is a group the topic leaves *unfinished*: if EventBridge died between
the last member's terminal event and the `group.completed` it owed, that completion is
genuinely absent and writing it is a new fact. So after the scan the mirror calls
`maybe_complete()` for every group it touched. That is safe because completion is
once-only at the SQL level (`WHERE completed_utc IS NULL`, §21.5) — a group already
completed on the topic stays silent.

This is also why members are applied with `replay=True` rather than through the normal
path: on the topic the member events come *before* the `group.completed`, so a replay
that completed on the last member would publish a duplicate before ever reading the
real one.

### 21.3 API surface

```text
POST   /v0/groups                     create a group, optionally fanning out its members
GET    /v0/groups                     list recent groups (JSON)
GET    /v0/groups/<groupid>           the group page (HTML)
GET    /v0/groups/<groupid>/status    progress as JSON — what the CLI polls
GET    /v0/groups/<groupid>/events.sse   live updates, honouring ?since=<seq>
POST   /v0/groups/<groupid>/close     close an open-ended group (no `expected`)
POST   /v0/groups/<groupid>/cancel    stop submitting queued members (§21.9.5)
```

`POST /v0/groups` takes either shape:

```json
{ "label": "nightly-review", "prompts": ["…", "…", "…"], "max_turns": 3 }
{ "label": "nightly-review", "expected": 40 }
```

The first **creates the group and submits its members in one call**, which is how the
CLI will use it: it removes a race (a member finishing before the group row exists)
and matches what `runmany` already does for the ungrouped case. The second declares a
group that will be populated by later `POST /v0/agents` calls carrying
`{"groupid": "…"}`.

`expected` may be **omitted** for an open-ended group. Progress then shows a running
count with no denominator and no bar, which is what the research prescribes when the
total is unknowable, and `POST /close` supplies the denominator later.

Creation accepts an **`Idempotency-Key` header**. A retried POST with the same key
returns the original group rather than creating a second one — otherwise a client
retry silently doubles a 40-agent batch.

### 21.4 Store

```sql
CREATE TABLE groups (
  groupid       TEXT PRIMARY KEY,
  label         TEXT,
  expected      INTEGER,            -- NULL for an open-ended group
  min_success   INTEGER,            -- §21.9.3 quorum; NULL = all must finish
  deadline_utc  TEXT,               -- §21.9.2; NULL = no deadline
  created_utc   TEXT NOT NULL,
  closed_utc    TEXT,               -- when `expected` was fixed by POST /close
  completed_utc TEXT,               -- the once-only transition guard
  completion_reason TEXT,           -- all | quorum | deadline | cancelled
  idempotency_key TEXT UNIQUE
);

CREATE TABLE group_members (
  groupid       TEXT NOT NULL,
  correlationid TEXT NOT NULL,
  status        TEXT NOT NULL,      -- submitted | running | finished | failed
  submitted_utc TEXT NOT NULL,
  first_event_utc TEXT,
  finished_utc  TEXT,
  terminal_phase TEXT,              -- result | error
  PRIMARY KEY (groupid, correlationid)
);
CREATE INDEX group_members_by_corr ON group_members(correlationid);
```

The `correlationid` index is what lets the responses consumer answer "does this event
belong to a group?" on the hot path without a scan.

**Member rows must be insertable for a group that does not exist yet.** An
EventBridge restart re-scans the topic, and a fast agent can finish before its group's
`started` event is processed, so terminal facts can genuinely arrive first. The store
therefore accepts the fact and reconciles when the group row appears — the same
shape as `Store.backfill_prompt_if_missing`, which exists for exactly this reason.

### 21.5 Completion accounting — the brief's counter must change

The brief specifies an internal counter: incremented by `expected`, decremented on
each member's final event, firing completion at zero. **That is not safe here, and the
reason is in this document already.**

RQ-1 accepts **at-least-once delivery**: a pod killed mid-run causes its request to be
redelivered and the agent to run again, emitting a *second* `final=true` event for the
same correlationid. A decrementing counter would decrement twice for one member and
reach zero while other agents are still running — firing "all done" early, which is
the one lie a progress display must never tell. EventBridge restarting and re-scanning
the topic produces the same double-count, as does any consumer-group rebalance.

**Instead: store per-member terminal facts and derive the counts.**

```sql
-- idempotent by construction: the second arrival changes nothing
UPDATE group_members SET status='finished', finished_utc=?, terminal_phase=?
 WHERE groupid=? AND correlationid=? AND finished_utc IS NULL;
```

Counts are then `SELECT COUNT(*) … GROUP BY status`, never a mutated number. This is
the "idempotent workers" requirement the fan-in literature states plainly — queues
deliver at-least-once, so aggregation must be idempotent — and it costs nothing here
because the store is already keyed per member.

**The completion event fires exactly once**, guarded by a conditional update rather
than by application logic:

```sql
UPDATE groups SET completed_utc=?, completion_reason=?
 WHERE groupid=? AND completed_utc IS NULL;
-- publish group.completed ONLY if rowcount == 1
```

That guard is in SQLite rather than memory on purpose: it holds across an EventBridge
restart, which an in-process flag would not. It is the same discipline §8.6 already
relies on — EventBridge is single-replica, so a row is a lock.

Note the consequence for consumers: a duplicate *member* response event is expected
and harmless, but the `group.completed` event is emitted once. Anything downstream
should still tolerate seeing it twice, because the topic offers at-least-once, not
exactly-once — the guard prevents *generating* a second one, not receiving one.

### 21.6 Notifications: announce the edges, suppress the middle

| When | ntfy |
|---|---|
| group created | **one** notification: label, expected count, group URL |
| a member finishes | **suppressed** while the group is open |
| group completes | **one** notification: finished/failed totals, duration, URL |

Suppression lives in `NtfyPublisher.submit`, which today filters only on `phase`. It
gains one more condition: an event with a `groupid` whose group is still open is
dropped. The lookup is a primary-key hit on `group_members` plus one on `groups`.

Two gaps in the brief worth fixing, both of which produce *silence* where the user
expects a notification:

- **A group that never completes never notifies.** If a caller declares 40 and submits
  39, or an agent's request is lost, the counter never settles and no notification is
  ever sent. §21.9.2's deadline closes this: the group completes with
  `reason=deadline` and the notification says so.
- **Member failures are invisible.** Suppressing *all* member events means a batch
  where 30 of 40 agents error looks identical to a healthy one until the end. The
  completion notification reports the failure count, but "anxious waits feel longer",
  and a batch that is visibly failing early is one the operator would abort. I
  recommend `NTFY_GROUP_NOTIFY_ERRORS=true`, notifying on member `phase=error` only.
  Default kept at `false` to match the brief as written; the recommendation is to
  flip it.

### 21.7 The group page

`GET /v0/groups/<groupid>` renders the same way the agent page does — server-rendered
HTML, then SSE appends with `?since=<seq>` so a reload replays nothing.

**A percent-done bar is legitimate here, and this is worth stating because it usually
is not.** Nielsen's current guidance on agent progress names the *denominator problem*:
an agent cannot know it is 40% done debugging, so a percentage over a single agent's
work is "progress theater". A group is different — the denominator is the number of
agents, which is declared and countable. So the bar is honest at the group level
precisely where it would be dishonest at the member level. The member rows accordingly
show **state**, never a fabricated per-agent percentage.

Design rules taken from that research, each with the reason it applies here:

| Rule | Applied |
|---|---|
| **Count in user units**, not bare percentages | "12 of 40 agents finished" is the headline; "30%" is secondary |
| **No known total? show the running count anyway** | open-ended groups show "17 finished so far", no bar |
| **Never let the bar move backward**, never let the step list silently grow | if more members appear than `expected`, the denominator is revised **once, visibly** ("expected 40 → 43") and the percentage never decreases; denominator = `max(expected, members_seen)` |
| **Elapsed always; remaining only when honest** | elapsed from the first member; ETA withheld until ≥ 3 members have finished |
| **Round and pad estimates** | "about 4 minutes", never "3:47"; revise downward freely, upward once and visibly |
| **Never stall silently** | if no member has changed state for > 2 min, say "no activity for 4 minutes" in words instead of freezing |
| **Labor illusion — show the work** | each running member shows its latest `text`, which the store already has |
| **Interim artifacts** | each finished member shows a truncated reply inline, so a doomed batch can be abandoned after 4 minutes rather than 4 hours |
| **Ambient, not a transcript** | the page is a glanceable dashboard; per-agent detail stays behind a link. "Transcripts are for audits. Ambient cues are for monitoring." |
| **Survive leave-and-return without resetting** | the existing `?since=` SSE contract already does this |

**ETA from observed throughput, not assumed parallelism.** The tempting formula —
`remaining × mean_duration / concurrency` — needs a concurrency figure, and the real
one is capped by `maxReplicaCount` and the partition count (§4), so a 40-agent group
runs about 6 at a time, not 40. Rather than model that, measure it:

```text
throughput = finished_in_window / window_seconds        (sliding, last ~60s)
eta        = remaining / throughput                     (shown only if throughput > 0)
```

Self-correcting, needs no knowledge of the cluster, and degrades to "no estimate"
instead of to a lie. The page must also **not imply more parallelism than exists**:
showing "6 running" when 34 are queued is honest and explains the wait, which is
itself worth more than the bar ("unexplained waits feel longer than explained waits").

Layout, top to bottom: group label and state chip; the bar with "12 of 40 finished ·
2 failed · 6 running · 20 queued"; elapsed and ETA; a stall notice when applicable;
then the member table — correlationid (linking to `/v0/agents/<corr>`), status,
duration, cost, and the latest or final text.

Accessibility: the bar carries `role="progressbar"` with `aria-valuenow/min/max` and a
text alternative, and status is conveyed by label as well as colour.

### 21.8 CLI and skill

```bash
eventbridge-cli.py group run "prompt one" "prompt two" … --label nightly --base-url …
eventbridge-cli.py group status <groupid>        # one-shot progress
eventbridge-cli.py group watch  <groupid>        # follow to completion
eventbridge-cli.py group list
```

`group run` is `runmany` plus a group: same submit-everything-before-watching
ordering, which §21.9.4 notes is what makes KEDA scale at all. `runmany` stays as the
ungrouped shortcut.

Terminal output follows the same rules as the page — counts in user units, elapsed
always, ETA only when earned:

```text
▸ group nightly-review swift-falcon-3921 · https://…/v0/groups/swift-falcon-3921
· 40 agents submitted together
  12/40 finished · 2 failed · 6 running · 20 queued · 3m12s elapsed · about 5 min left
✔ group complete · 38 finished · 2 failed · 8m41s
```

The skill gains a `group` intent for "run these N prompts as a batch" / "how is group
X doing", and — per §21.9 — must be told that a group of N is **one** `group run`
call, never N `run` calls.

### 21.9 Proposed improvements beyond the brief

Separated from the specification above because these are recommendations, not
requirements. The first is a correctness fix and should be treated as mandatory.

**21.9.1 Derived counts instead of a decrementing counter.** §21.5. Mandatory: the
counter as briefed fires completion early under at-least-once delivery.

**21.9.2 A deadline, and terminal states other than success.** Every fan-in design has
to answer "what about stragglers", and the literature's answer is a deadline with
partial results. `deadline_utc` (default: created + 1 h, configurable) gives the group
`reason=deadline` completion rather than hanging forever — which, per §21.6, is also
the difference between one notification and none.

**21.9.3 K-of-N completion.** "Do you need every response, or can you return once K of
N reply?" `min_success` completes the group as soon as K members finish, for batches
where the operator wants the first K answers and does not care about the tail. Cheap
to add given derived counts; meaningless with a decrementing counter.

**21.9.4 Create-and-fan-out in one call.** Already in §21.3, called out here because it
removes a whole class of ordering bug and because the submit-all-first ordering is
load-bearing for scaling: submitting one prompt at a time lets each finish before the
next arrives, so lag never exceeds 1 and KEDA never scales past one pod. Measured:
two prompts submitted together took the Deployment 0 → 1 → 2.

**21.9.5 Cancel.** "Anything inspectable early lets the user abort a doomed run after 4
minutes instead of 4 hours" — but that only helps if abort exists. `POST /cancel` can
cheaply stop *queued* members (EventBridge stops submitting, and un-submitted members
are simply never published). Stopping *running* members needs a mechanism EventRunner
honours — a cancellation tombstone it checks between turns, or pod-level termination —
which is a larger change and belongs with the Job-per-request work in §4. Ship the
cheap half; document the limit plainly on the page ("6 running agents will finish").

**21.9.6 Distinguish failed from finished.** The brief counts "final events". A member
that ends `phase=error` is final but not successful, and a bar that counts it as
success is lying. `status` separates them and the page shows both.

**21.9.7 Straggler and fan-out observability.** The metrics the literature names map
directly onto this store: fan-out degree (`expected`), **straggler latency** (P99 of
member duration — the slowest member sets the group's duration), partial failure rate,
and aggregation delay. All derivable from `group_members` with no new instrumentation,
and all worth printing in the completion notification and on the page.

**21.9.8 Retention.** `groups` and `group_members` grow without bound on a demo that
is never cleaned. A row cap or an age-based prune, consistent with the topics' 24 h
`retention.ms` (§6), keeps the store bounded and keeps the group list readable.

**21.9.9 Group events participate in signing.** §11's `SIGNED_ATTRS` gains `groupid`,
so a signed audit trail covers batch membership. Otherwise a signed per-agent trail
says nothing about which batch an agent belonged to, which is precisely the
attribution a batch makes interesting.

### 21.10 Risks and open questions

- **Nested groups** are deliberately out of scope: one `groupid` per correlation, no
  hierarchy. Revisit only if a real use case appears.
- **A group spanning a topic-retention boundary** (24 h) cannot be fully replayed from
  Kafka, so its SQLite rows become the only record. Acceptable, and the same tradeoff
  §6 already accepts — but note the store is `emptyDir` in the `test` and `kind`
  overlays, so there "the only record" survives exactly as long as the pod. A group
  older than retention that outlives a restart is genuinely lost; a PVC is the fix if
  that ever matters, not a larger replay.
- **The replay cost grows with the topic, not with the groups.** `GroupMirror` reads
  every record in `responses` to find the grouped ones: 7 280 records took under 25 s
  on ykt1, but a topic two orders of magnitude larger would make startup visibly slow.
  Bounded today by the 24 h retention. If it stops being bounded, give group events
  their own topic rather than making the scan cleverer.
- **`expected` is caller-supplied and unverifiable.** The design handles both
  directions (deadline for under-, visible revision for over-subscription) but cannot
  detect a caller that simply lied.
- **Throughput-based ETA is noisy for small groups.** With 4 members the window rarely
  contains enough completions; suppressing the estimate below 3 finishes is a
  heuristic, not a guarantee. Showing no estimate is the correct failure mode.
- **Does the group page need auth?** No more than the agent page, which §18 already
  lists as a non-goal — but a group page aggregates more in one URL, so it is a
  slightly larger prize for anyone who learns the hostname.

### 21.11 Implementation tasks

| # | Task | Gate |
|---|---|---|
| T6.1 | `groupid` extension attribute; group event types in `shared/ce.py` | round-trip test through the Kafka binary binding |
| T6.2 | `groups` / `group_members` tables + store methods | unit tests: idempotent terminal update (same event twice changes nothing), member-before-group insertion, derived counts |
| T6.3 | Once-only completion transition | a test that calls the transition twice and asserts exactly one publish, plus one across a simulated restart |
| T6.4 | Responses-consumer routing on `type` | a group event does not reach `insert_response`; an agent event still does |
| T6.5 | `POST /v0/groups` incl. fan-out and `Idempotency-Key` | round-trip; a repeated key returns the same groupid and submits nothing extra |
| T6.6 | `GET /status`, `/events.sse`, `GET /v0/groups` | JSON shape; SSE replays nothing with `?since=` |
| T6.7 | ntfy: start and completion notifications, member suppression | three tests: start notifies, member suppressed while open, completion notifies once |
| T6.8 | Deadline sweep → `reason=deadline` completion | a group with a past deadline completes and notifies |
| T6.9 | The group page | counts in user units; denominator revised upward never downward; no ETA below 3 finishes; stall notice appears |
| T6.10 | CLI `group run/status/watch/list` + skill intent | a 3-prompt group reports 3/3 and links each member |
| T6.11 | Cluster verification | a 6-agent group on ykt1: KEDA scales past one pod, exactly two notifications arrive, the page reaches 6/6 |
| T6.12 | `GroupMirror` replay (§21.2) | a scan over a fake multi-partition topic rebuilds a group complete with its members and publishes nothing; the empty polls of consumer warm-up are not mistaken for the end of the topic; metadata arriving late is waited for; a corrupt record, an ungrouped event and a partition that never delivers are each survived |
| T6.13 | Crash-window settle (§21.2) | a topic with every member finished but no `group.completed` is settled exactly once and does publish; an in-flight group comes back open; a group already completed on the topic stays silent |
| T6.14 | Restart on the cluster | restart `deploy/eventbridge` (whose `/data` is `emptyDir`) and assert every earlier group is served again with the same counts and reason, while the ntfy message count is unchanged |
