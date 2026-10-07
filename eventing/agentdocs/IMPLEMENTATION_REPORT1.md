# IMPLEMENTATION REPORT — Phase 1

Scope: implementation of `DESIGN_PHASE1.md` (revision 2) — tasks T0 through T5.
Runbook: `README_PHASE1.md`.

Phase 1 is **implemented and observed working on the `ykt1` cluster**. "Done"
means the §16 four-stage loop was watched happening, not that code was written:
an idle deployment at zero agent replicas, a POST waking a pod in ~5 s, the
transcript streaming into the HTML view, and the count falling back to zero ~31 s
after the run finished.

---

## 1. What was built

| Task | Deliverable | Status |
|---|---|---|
| T0.1 | `scripts/proclib.py` — subprocess + `Checks` accumulator | done, 18 tests |
| T0.2 | `scripts/k8slib.py` — kubectl → dicts, `dig()` that raises | done, 30 tests |
| T0.3 | `build-images.sh` → `build_images.py` | done |
| T0.4 | `push-images.sh` → `push_images.py` | done |
| T0.5 | `stop-demo.sh` → `stop_demo.py` | done |
| T0.6 | `docker-e2e-test.sh` → `docker_e2e_test.py`, absorbing `_events.py` | done, 24 checks pass |
| T0.7 | shell versions retired, one-line `exec` shims for documented paths | done |
| T1.1 | Kafka consumer retry loop, blocking, supervised | done |
| T1.2 | `ER_CONSUMER_GROUP` | done |
| T1.3 | graceful drain + stderr progress | done |
| T1.4 | heartbeat file + `eventrunner.healthcheck` probe | done |
| T1.5 | offsets commit after the terminal event (`OffsetLedger`) | done |
| T1.6 | a starting offset committed on first start | done |
| T1.7 | **`--resume <path>` verified empirically** | **done — it works** |
| T1.8 | EventBridge `PUT`/`GET /v0/agents/<corr>/transcript` | done |
| T1.9 | EventRunner checkpoint/restore around each turn | done |
| T1.10 | `maxReplicaCount` raised to 6 | done (see §5 for the caveat) |
| T2.4 | `Dockerfile-eventrunner-claude` | done |
| T3.1-3.4 | `k8s/` base, topics, `overlays/{test,demo}` | done, 24 tests |
| T4.1 | `k8s_preflight.py`, 13 checks | done, 25 checks green on ykt1 |
| T4.2 | `k8s_deploy.py` in the §13 order, public-URL gate | done |
| T4.3 | change detection (digest + `kubectl diff`) | done, verified both ways |
| T4.4 | `k8s_teardown.py` | done |
| T5.1 | `k8s_e2e_test.py`, assertions 1-12 | **49 checks pass on ykt1** |
| T5.2 | wake-latency measurement vs RQ-4 | 5.0 s (assumption: < 10 s) |
| T5.3 | `k8s_demo_flow.py`, the four-stage loop | done |
| T5.4 | `/continue` on a warm pod | done |
| T5.5 | `/continue` across a full scale-to-zero cycle | done (mock); real-agent gate below |
| T5.6 | idle-replay guard | done |
| T5.7 | teardown, then resume | done |
| §11 | `ce_causationid`; JWS signing, flagged off | done, 24 tests |
| §3.2 | local Kind cluster: config, bootstrap, `overlays/kind` | done, see §5 |
| §21 / T6 | agent groups: wire, store, service, pages, API, CLI | done, 69 tests, see §6 |

New modules: `eventrunner/{offsets,transcript,signing,healthcheck}.py`,
`shared/heartbeat.py`, `scripts/{proclib,k8slib,imagelib,build_images,push_images,
stop_demo,docker_e2e_test,k8s_preflight,k8s_deploy,k8s_e2e_test,k8s_demo_flow,
k8s_teardown,verify_resume_by_path}.py`, 13 files under `k8s/`.

Unit tests went from **100 passing to 286** (8 new files, 155 new tests).
`ruff check` is clean on every file Phase 1 added.

---

## 2. T1.7 — the fact the Gap B fix rests on, now proven

`DESIGN_PHASE1.md` §16 chose "checkpoint the transcript through EventBridge" on
the strength of a documented but unverified claim, and flagged it as the first
gate on the implementation. `scripts/verify_resume_by_path.py` now proves it, and
is re-runnable for about two cents:

```text
[t1.7] ── turn 1 — start, in directory A ──
[t1.7] PASS start turn exited 0
[t1.7]   reply: 'OK'
[t1.7] ── transcript relocation ──
[t1.7] PASS transcript .jsonl written by the start turn  (21 lines, 222448 bytes)
[t1.7] PASS transcript copied to a path under directory B
[t1.7] ── turn 2 — resume by absolute path, in directory B, fresh config dir ──
[t1.7] PASS resume-by-path turn exited 0
[t1.7]   reply: 'PLUM-8821'
[t1.7] PASS resumed reply contains the codeword PLUM-8821 — context was restored
[t1.7] ── negative control — resume by session-id with no local transcript ──
[t1.7]   rc=1  stderr: No conversation found with session ID: 32552a6e-…
[t1.7] VERDICT: path-form resume WORKS — T1.8/T1.9 proceed
```

Both halves matter. The positive case restores context across a **different cwd
and a different `CLAUDE_CONFIG_DIR`**, which is what a different pod is. The
negative control fails exactly as Gap B predicts, which is what makes the
positive result attributable to the path form rather than to something else.

**A number worth carrying forward:** a trivial one-turn transcript is already
**222 KB** — the system prompt and tool definitions dominate. Any size cap has to
be generous or it rejects the very first checkpoint; the implementation uses
32 MiB (`EB_TRANSCRIPT_MAX_BYTES` / `ER_TRANSCRIPT_MAX_BYTES`).

---

## 3. Test results

### Local unit tests

```text
363 passed, 2 skipped                               ~77s
```

Identical under `ER_MOCK_CLAUDE=true` and `ER_MOCK_CLAUDE=false` — worth checking
explicitly, because this machine has a real credential in its environment, so
leaving the variable unset makes `config.load()` auto-select *real* mode.

The 2 skips need a local Kafka or a `.venv` that this sandbox cannot spawn. The 3
`tests/test_cli_output.py` failures reported earlier in this session are gone: they
read `.claude/skills/eventbridge/SKILL.md`, and the skill now lives in the repository
at `skills/eventbridge/` (symlinked from `.claude/skills/`).

Add `KUBECONFIG=../../.kube/config-ykt1` to include the 5 cluster-gated tests. Without
it they **skip rather than fail**, because the default `.kube/config` has no
current-context — a silent pass that is worth knowing about before trusting a green run.

### Docker e2e — T0.6's gate

The load-bearing conversion: the shell version passed end to end before Phase 1
began, so there was a known-good baseline to diff against.

```text
[e2e] ALL 24 CHECKS PASSED
```

Same assertions as the shell version, plus: `ER_CONSUMER_GROUP` reaching the
config in-image, the consumer announcing its connection, `ce_causationid` on every
response event, the transcript `PUT`/`GET` round trip and its 404 for an unknown
correlation, a `/continue` turn, and a clean SIGTERM through `docker stop`.

### Kubernetes e2e — T5.1's gate

```text
[e2e] ALL 49 CHECKS PASSED                       (ykt1, OpenShift)
[e2e] wake latency (POST -> scale-up): 1.0s
[e2e] scale-to-zero: 31s after idle (cooldown 30s)

[e2e] ALL 37 CHECKS PASSED                       (Kind)
[e2e] wake latency (POST -> scale-up): 2.2s
[e2e] scale-to-zero: 30s after idle (cooldown 30s)
```

All twelve §14.2 assertions, including the four that are the Phase 1 point:
idle at `0/0` with `Active=False`; KEDA activating within 60 s; a pod announcing
`mock_claude=True` with an `auto:` reason; and the return to zero.

### Preflight — T4.1's gate

```text
[preflight] ALL 25 CHECKS PASSED
```

Highlights, each re-measuring something the design had asserted:

- Strimzi serves **only `v1`**; the operator's `STRIMZI_NAMESPACE` resolves to
  `kafka`, confirming Finding 1.
- Both images anonymously pullable, `linux/amd64` present.
- The arbitrary-UID probe, **with no volume mounted** — the exact shape that
  failed before §8.5:
  `uid=1000910000 gid=0 groups=[0] HOME=/home/app TMPDIR=/data`, both writes OK.
- KEDA reaching Kafka cross-namespace, with a real value from the external
  metrics API (`s0-kafka-keda-test-topic = 0`) — re-confirming RQ-3.
- Wildcard DNS resolving `eventbridge-kev1.apps.ykt1.hcp.res.ibm.com`.

### Measurements against the design's assumptions

| | Design | Measured |
|---|---|---|
| Wake latency (RQ-4) | assume < 10 s | **1.0–5.0 s** on ykt1, 2.2–4.3 s on Kind |
| Scale-to-zero | cooldown + slack | **30–31 s** with `cooldownPeriod: 30` |
| Concurrency (T1.10) | ≤ partition count | peak **4** replicas for 3 correlations |
| Image pull | 5.1 s cold (245 MB) | 454 ms – 2.45 s warm |
| Rebalance floor (Gap A) | ~3 s | consistent with the 5.0 s total |
| Ed25519 sign / verify | not estimated | **222 ms / 227 ms** — see §9 |

---

## 4. Deltas from the design, and bugs found

Thirty-seven things the design did not say, or said slightly wrong. Grouped by whose
bug it was. The one in §4's "per-pod sequence restart" heading is the one that
would have broken a real demo.

### Bugs in the Phase 0 application code

**1. `claude` blocks on inherited stdin, then fails.** `subprocess.Popen` never
set `stdin`, so the child inherited EventRunner's. The CLI waits 3 s and warns
`no stdin data received in 3s, proceeding without it`. Found while writing the
T1.7 gate; fixed with `stdin=subprocess.DEVNULL`. Latent in Phase 0 — a laptop
run has a terminal on stdin, a pod usually does not.

**2. A tool-using turn consumes the `--max-turns` budget.** With `--max-turns 1`,
a model that decides to write a memory file spends the turn on the tool call and
the run ends `subtype=error_max_turns`, exit 1. EventBridge's default is
`max_turns: 3`, so this is reachable in the demo: a tool-using agent can burn the
budget and surface as a synthesized `phase=error`. Not fixed — it is correct
behaviour — but it is why `verify_resume_by_path.py` asks the model not to use
tools and allows 4 turns.

### Wrong in the design document

**3. `kafka.errors.NoBrokersAvailable` no longer exists.** §8.1 names it; it was
removed in kafka-python 3.x, where an unreachable broker surfaces as
`KafkaConnectionError`. The retry catches bare `Exception` specifically so a class
rename cannot reintroduce the dead-thread bug.

**4. Omitting `spec.replicas` does not make the field absent.** §14.1 implies the
field's absence is what keeps `kubectl diff` stable. Measured: the API server
**defaults `replicas: 1` on create**. What actually makes change detection work is
`kubectl apply`'s merge semantics — a field in neither the manifest nor the
last-applied-configuration is left untouched. Verified directly: with KEDA holding
the Deployment at 0, a server dry-run apply reports **0**, not 1 and not absent.
The conclusion in §14.1 is right; the reasoning needed correcting, and
`tests/test_manifests.py` now pins the real invariant.

**5. `Route` is not a CRD.** §12 check 8 as designed looked for
`routes.route.openshift.io` among CRDs. On OpenShift, Route is served by an
*aggregated* API server, so it never appears in `kubectl get crd` — the check
reported "no Routes" on a cluster where Routes are the only working ingress.
Fixed with an `/apis/<group>/<version>` discovery query.

**6. §14 assertion 7 conflicts with a forced mock mode.** The assertion requires
the pod log to show an `auto:` reason, but an overlay setting `ER_MOCK_CLAUDE=true`
produces `explicit ER_MOCK_CLAUDE=true`. Resolved in the assertion's favour: the
test overlay sets nothing and mounts no credential Secret, so auto-detection picks
mock by itself. Just as deterministic — nothing can inject a credential into those
pods — and it exercises the detection path in-cluster instead of bypassing it.

**7. §13's step ordering is not quite achievable with one `kubectl apply -k`.**
The text wants EventBridge proven reachable *before* EventRunner starts, but the
commands apply the whole overlay at once. This turns out not to matter:
`minReplicaCount: 0` plus an empty topic means the EventRunner Deployment sits at
zero replicas until a request is posted, which cannot happen before the URL works.
Recorded rather than worked around.

### Bugs in the Phase 1 code, found by its own tests

**8. `Router.drain()` never printed its progress lines.** The condition wait used
fixed 1 s slices, so the only thing that woke it was a run completing — at which
point `in_flight` was already empty and the report loop had nothing to print. In
production (15 s interval, long runs) it happened to work; the fix bounds the wait
by the next report deadline.

**9. `on_done` fired after the run left the in-flight set.** A real correctness
bug, surfaced as an intermittent test failure. `drain()` treats `in_flight == 0` as
"everything finished" and the consumer commits offsets right after drain returns —
so a run could stop being counted before its offset-completion callback had run.
Fixed by completing, *then* clearing the slot, *then* waking the drain, with a
regression test asserting `on_done` always observes its own run as still in flight.

**10. A strategic-merge patch produced a Deployment with two volume types.** The
demo overlay's PVC patch merged `persistentVolumeClaim` onto the base's `data`
volume — `volumes` is keyed by `name`, so the `emptyDir` stayed too and the API
server rejected it (`may not specify more than 1 volume type`). Needs a JSON 6902
`replace` of the whole element. Caught by `--dry-run=server`, which is exactly
what T3.1's gate is for.

### The one that mattered most: per-pod sequence restart

**15. A new pod restarted `sequence` at 1 and overwrote the previous turn's stored
events.** Found by running the demo flow on the cluster, not by any unit test, and
invisible in Phase 0 by construction.

`Emitter` keeps its per-correlation sequence counter **in memory**. Phase 0 had one
long-lived EventRunner process, so the counter was naturally monotonic for the life
of a conversation. In Phase 1 every scale-from-zero is a *new process* whose
counter starts at 1 — and EventBridge's `responses` table is
`PRIMARY KEY (correlationid, sequence)` written with `INSERT OR REPLACE`.

A three-turn conversation across a scale-to-zero cycle therefore stored **six rows
instead of nine**, with turn 1's events silently replaced by turn 3's:

```text
 stored rows: 6
 seq 1 stdout | init: mock session=90afdfd2…
 seq 2 stdout | MOCK-REPLY[Once more: what was the codeword? …   <- turn 3's reply
 seq 3 result |
 seq 4 stdout | init: mock session=90afdfd2…
 seq 5 stdout | MOCK-REPLY[What codeword did I give you? …       <- turn 2's reply
 seq 6 result |
```

`/turns` then paired turn 1's prompt with turn 3's reply and showed turn 3 with
zero events. Note what this was *not*: no error anywhere, no failed assertion in
the e2e test (which drives a single turn), and a UI that looked plausible while
being wrong.

It also quietly invalidates a claim in RQ-1 — "deduplicating on
`(correlationid, sequence)` is sufficient" — because that assumes the pair is
unique per event. With per-pod restart it is not, and the "dedupe" destroys data.

**Fix:** before the first emit of a turn, the runner asks EventBridge for the
highest sequence it already holds and seeds the Emitter with it
(`TranscriptStore.seed_emitter` → the existing but previously unused
`Emitter.seed_seq`). The wire contract is unchanged, the channel already existed
for transcript checkpointing, and a failed query degrades with a warning rather
than failing the turn. `last_sequence()` returns `None` rather than `0` when it
cannot tell, because seeding to 0 when the real answer is 6 *is* the bug.

Six tests pin it, including the demonstration that a fresh Emitter really does
restart at 1, that seeding never rewinds, and that two pods' sequence sets do not
intersect. Confirmed on the cluster after the fix — a six-turn conversation across
several scale-to-zero cycles stored a contiguous `1..18` with no collisions, where
before the fix three turns had produced six rows.

**The general lesson:** moving a stateful component from "one long-lived process"
to "0..N ephemeral pods" invalidates every in-memory counter it owned. Phase 1's
design chapter caught the two obvious ones — the `claude` transcript (Gap B) and
committed offsets (Gap C) — and missed this one, which lived in three lines of
`emit.py`.

**17. Deleting a consumer group does NOT make KEDA report lag.** Measured twice on
ykt1: after `kafka-consumer-groups.sh --delete --group kev1-eventrunner`, with
`offsetResetPolicy: earliest` and messages still retained in the topic, KEDA did
**not** scale up within 240 s. So the replay storm in §16 Gap C needs a *new
request* as its trigger — which is exactly how Gap C words it ("one new task
triggering a re-run of the whole backlog"), and it is worth pinning down because
the §7.1 revision note, which found lag 3 for a never-committed group under
`latest`, could be read as implying the opposite. An intermediate version of T5.6
relied on the missing offset alone to wake a pod and failed for that reason; the
check now posts one request, as the design describes.

**18. `kafka-consumer-groups.sh --delete-offsets --all-topics` is not valid.**
T5.6 simulates offset expiry by deleting the group's committed offsets;
`--all-topics` belongs to `--reset-offsets` and `--delete-offsets` demands an
explicit `--topic`, so the first attempt failed with a usage dump. `--delete
--group <g>` removes the group and its offsets together, which is exactly what the
broker does after `offsets.retention.minutes` with no members, and it only works
while the group is empty — which the test already guarantees by waiting for
scale-to-zero first.

**19. Quay creates new repositories private, and preflight correctly refused to
deploy.** Pushing the derived image to a *new* repository
(`rossoctl-eventrunner-claude`) produced an anonymous `HTTP 401`, and check 4
hard-failed the demo deploy — doing exactly what Finding 2 designed it to do,
since the alternative is a slow `ImagePullBackOff` deep into the deploy. The fix
needs no human step in the registry UI and no pull-secret fallback: the derived
image is published as a **tag on the already-public runner repository**,
`rossoctl-eventrunner:claude-dev`. Worth knowing before adding any further image.

**20. My own stage-2 assertion measured the wrong moment.** "Replicas stayed ≥ 1
for the whole run" checked `replicas` *after* observing the terminal event over
HTTP. A mock run lasts milliseconds, so by then the cooldown could legitimately
have elapsed and a correct scale-down read as a mid-run kill (`replicas=0`). The
assertion now samples replicas from a background thread **while** the turn runs and
asserts on the minimum — and when the run is shorter than ~5 s it says plainly
that the window was too short to measure rather than claiming a pass. The RQ-1
property is only observable on a run long enough to observe, which in practice
means a real agent.

### Environment and tooling

**12. The `claude` CLI cannot be exercised in a cross-built image.** It bundles a
`bun` binary that aborts under QEMU user-mode emulation
(`qemu: uncaught target signal 6 (Aborted)`), so `claude --version` in an amd64
layer fails on an arm64 builder — a build failure with nothing to do with the
image being wrong. The derived image now builds `linux/amd64` only and asserts the
binary is installed; **whether it runs is verified on a real cluster node** by new
preflight check 13, which is better evidence than an emulated build-time check.

**13. The first Kubernetes e2e run failed on a stale image.** Four assertions
failed with `No module named eventrunner.healthcheck`: the `:dev` tag in the
registry predated every Phase 1 change. Obvious in hindsight, and exactly the
hazard §8.8 warns about — `:dev` is mutable. Deploy an immutable tag for anything
you need to reason about.

**14. `kubectl set env` does not create drift.** Worth recording because it looks
like it should. After step 4 sets `EVENT_BRIDGE_PUBLIC_BASE_URL` out of band,
`kubectl diff -k` still exits 0, because the ConfigMap deliberately never declares
that key. This is what lets §13 step 4 and §14.1 coexist.

**21. Docker became available on this machine.** `AGENTS.md` records that no
container runtime existed, so all container work was static validation. That is no
longer true: both images were built multi-arch, pushed, and the docker e2e ran
green.

---

## 5. The second target: a local Kind cluster

Added after the fact, per `DESIGN_PHASE1.md` §3.2. The point was to make the
reference cluster optional — it is shared, occasionally rate-limited, and behind a
VPN — **without changing a single application manifest.** That held: `base/` and
`k8s/topics/` are byte-identical between the two environments, and the only
difference is `overlays/kind`, which deletes the inherited `Route` and adds an
`Ingress`.

What made it possible is keeping the Kafka identity the same. `kind_setup.py`
creates a Kafka named `my-cluster` in namespace `kafka`, so the bootstrap address
in `base/configmap.yaml` —
`my-cluster-kafka-bootstrap.kafka.svc:9092` — resolves on both clusters unchanged.

New artefacts:

| | |
|---|---|
| `k8s/kind/kind-cluster.yaml` | the cluster config: `ingress-ready=true` node label + `extraPortMappings` 30080/30443 |
| `k8s/kind/kafka-cluster.yaml` | single-node KRaft `Kafka` + `KafkaNodePool`, 512 MiB heap, `group.initial.rebalance.delay.ms: 0` |
| `k8s/overlays/kind/` | `$patch: delete` on the Route, plus an nginx `Ingress` with SSE-safe annotations |
| `scripts/kind_setup.py` | creates the cluster and installs ingress-nginx, Strimzi + Kafka, and KEDA; `--check` reports the gaps |
| `--kubeconfig` / `--ingress-port` | on every k8s script, so nothing depends on an exported `KUBECONFIG` |

Results on a fresh cluster (arm64, 8 CPU / 23.4 GiB):

```text
[kind]   ALL 14 CHECKS PASSED      kind_setup.py  (Kafka Ready after 121s)
[deploy] ALL 37 CHECKS PASSED      k8s_deploy.py   --overlay kind
[e2e]    ALL 37 CHECKS PASSED      k8s_e2e_test.py --overlay kind  (wake 2.2s)
```

The deploy includes the public-URL gate answering 200 on
`http://eventbridge.127.0.0.1.nip.io:30080/healthz` — nip.io resolves any
`*.127.0.0.1.nip.io` to loopback, giving a real hostname-based Ingress with no
`/etc/hosts` editing.

### Resource sizing: the answer is "size the VM, not the cluster"

Asked whether the cluster had enough CPU and memory: **yes, comfortably.** It
reports 8 CPU / 23.4 GiB allocatable, against a 4 CPU / 8 GiB minimum and a
6 CPU / 12 GiB recommendation.

The important part is that **Kind has no resource knobs at all.** Each node is a
container; the ceiling is the Docker/Rancher/Colima VM. Writing `cpu:` or `memory:`
into a Kind config does nothing. And a too-small VM does not fail as a Kind error —
it fails as pods stuck `Pending` on insufficient cpu/memory, or an OOMKilled Kafka
broker, which reads like an application bug. So `kind_setup.py` reports the VM's
size, asserts the floor, and warns between the floor and the recommendation.

**What did need a new cluster config was not resources.** The pre-existing Kind
cluster had only its API port mapped (`6443 → 50761`) and no `ingress-ready` label.
ingress-nginx's kind variant schedules with a `nodeSelector` on that label and binds
host ports, so that cluster could not serve an Ingress at all — the controller would
sit `Pending` and nothing would be reachable. That is the reason
`k8s/kind/kind-cluster.yaml` is a required artefact rather than a convenience, and
it is the recommendation: recreate from that config. The existing `kind` cluster was
left untouched; the new one is named `kev1` alongside it.

### Findings from the Kind path

**22. Kind tests the half of §8.5 that OpenShift hides, and vice versa.** There is
no SCC to inject a numeric `runAsUser`, so `runAsNonRoot: true` is enforced against
the image's own `USER` — precisely the configuration that produced
`CreateContainerConfigError: image has non-numeric user (app)` before §8.5 made the
directive numeric. Conversely the arbitrary-UID probe (check 6) proves nothing on
Kind, because nothing assigns a foreign UID. Neither environment alone validates the
image.

**23. `kubectl wait --for=condition=Ready pod` fails when no pod exists yet.**
`apply` returns before the Deployment has created one, so the wait died with
`error: no matching resources found` while the controller was in fact coming up
fine. `rollout status` handles the gap; `wait` is only safe once you know the object
exists.

**24. KEDA's release YAML does not name its deployments the way the Helm chart
does.** The metrics deployment is `keda-metrics-apiserver`, not
`keda-operator-metrics-apiserver`; there is also `keda-admission`. Verified against a
real 2.20.0 install on both clusters.

**25. Two preflight checks assumed a cluster that had already been used.** Check 6
creates a probe pod in the target namespace, and check 7 needs an existing Ready
`KafkaTopic` to point a trigger at — both true on the reference cluster, neither true
on a fresh one. Check 6 now creates the namespace (idempotently, as deploy step 1
does anyway) and check 7 creates a throwaway `KafkaTopic` and deletes it. Skipping
check 7 was the wrong alternative: it is the one check that proves KEDA can reach
Kafka, and a broken scaler means silent no-scaling.

**26. A configurable namespace and a literal topic manifest did not fit together.**
§5 says the namespace is configurable, but `k8s/topics/kafkatopics.yaml` names
`kev1-requests` / `kev1-responses` literally — it cannot be templated, because it
targets namespace `kafka` and so cannot take part in an overlay's namespace rewrite.
`--namespace kev2` therefore used to wait 180 s for a topic nothing had created.
Deploy now detects the mismatch up front and says exactly what to change.

**27. `spec.replicas == 0` does not mean no consumer is running — and it cost two
false failures.** The Kind e2e failed "KEDA activates within 60 s" twice, at 61.8 s
and 61.7 s, while every other assertion passed and the reply arrived normally. A
direct measurement explained it: the external metric showed lag **1 immediately**
after the POST and then **0**, with `spec.replicas` at 0 the whole time — something
had consumed the message. That something was the previous run's pod, still alive.

A Deployment's `spec.replicas` drops the instant KEDA scales down, but the pod
survives until it exits or `terminationGracePeriodSeconds` (600 s here) expires, and
until then it is still a consumer-group member still consuming. So the e2e declared
the system idle, posted a request, the dying pod served it, lag never rose, KEDA
correctly never scaled up — and the assertion that KEDA *would* scale up failed while
the system was behaving perfectly. The suspiciously repeatable ~60 s was simply the
assertion's own budget.

Both the e2e's idle gate and the demo flow's `wait_for_zero` now require **zero
replicas AND no surviving pod**. This was invisible on the reference cluster only
because its pods happened to exit fast enough to close the window.

The general shape is worth keeping in mind for anything that asserts on scale-to-zero:
"the controller says zero" and "nothing is running" are different claims, and only
the second one makes a wake test meaningful.

**28. Neither image had a CA bundle, so ntfy was dead in-cluster.** Found while
configuring phone notifications on ykt1: EventBridge could not make *any* outbound
HTTPS request.

```text
CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate
ssl.get_default_verify_paths() -> cafile: None  capath: None
```

`debian:trixie-slim` ships no CA store and neither Dockerfile installed one. The
consequence is silent: ntfy fan-out — a documented Phase 0 feature — simply never
delivered, and `eventbridge/selftest.py`'s public-tunnel probes could never succeed.
It is invisible on a laptop, which has a CA store of its own.

Installing `ca-certificates` turned out to be only half the fix, which a build-time
assertion caught immediately: this interpreter is a **uv-managed standalone CPython
compiled to look for `/etc/ssl/cert.pem`**, while Debian writes
`/etc/ssl/certs/ca-certificates.crt`. So `cafile` was still `None` after the install.
Both images now symlink the two and assert `cafile` is non-empty during the build.

A symlink rather than `SSL_CERT_FILE` on purpose: the `claude` subprocess receives a
curated env allowlist (`eventrunner/runner.py::child_env`), so an env var would not
reach it, whereas OpenSSL's default path always applies. Preflight check 6 now also
asserts the CA bundle from inside a real pod, so the class cannot return silently.

**29. An ntfy topic name is a capability, so it is handled like a credential.**
Anyone who knows the topic can both read every notification and publish to it. It is
therefore never committed: `k8s_deploy.py` creates `Secret/eventbridge-ntfy` from
`NTFY_TOPIC` in the environment, and the base Deployment references that Secret with
`optional: true` so overlays without ntfy are unaffected. The Secret is listed
**after** the ConfigMap in `envFrom`, which is what lets its `NTFY_ENABLED=true`
override the ConfigMap's `"false"` — verified in a running pod, since last-source-wins
for duplicate keys is easy to assume and worth confirming.

**30. The "30 second KEDA lag" was the cooldown, not the wake.** Asked to shorten an
apparent 30 s delay, measurement located it precisely — and not where it was assumed
to be:

| | cooldownPeriod 30 | cooldownPeriod 5 |
|---|---|---|
| POST → KEDA scales up | 1.7 s | 3.0 s |
| POST → pod Ready | 13.7 s | 14.7 s |
| POST → back to zero | 46.5 s | **22.0 s** |

KEDA's detection was already fast, because `pollingInterval` was 5 rather than its
30 s default. The 30 s was an **idle pod lingering after its work finished**, which
reads as sluggishness when demoing the zero → wake → zero loop. The `test` and `kind`
overlays now use 5, halving the loop.

Shortening it is safe specifically because of RQ-1: the offset is committed only
after the terminal event, so lag stays ≥ 1 for the entire run and KEDA cannot select
a working pod for termination. The cooldown governs only how promptly an *idle* pod
goes away. The demo overlay keeps 300 deliberately — a real conversation usually
continues, and a warm pod skips the cold start on every `/continue`.

What is left is ~12 s of **pod startup** (image pull plus free-threaded CPython
init), which no KEDA setting touches; pre-pulling the image is the only lever.

**31. Two timing-dependent tests were wall-clock races.** Both passed in isolation
and flaked under load — the worst failure mode for a test, since it trains you to
re-run rather than read. `drive()` in `test_consume_phase1.py` slept a fixed
`0.05 × polls` and then asserted; nine call sites now pass `until=<predicate>` and
return as soon as the asserted thing happens, while the two tests that assert
something does *not* happen keep the bounded wait deliberately, with a comment saying
so. Verified over 20 consecutive runs.

**32. Local wake latency is better than the cluster's, and is not comparable.** The
Kind broker sets `group.initial.rebalance.delay.ms: 0`, which is the 3 s floor §16
Gap A has to live with on the shared cluster. A Kind number must not be quoted as if
it were a cluster number.

## 6. Agent groups (§21) — implemented

Batch fan-out with a tracked fan-in: one call submits N agents, the group gets its own
page and its own rows, and a batch produces **exactly two notifications** instead of N.

| | |
|---|---|
| Wire | `ce_groupid` extension; `group.started.v1` / `group.completed.v1` on the **responses** topic |
| Store | `groups` + `group_members`; counts derived, never a mutable counter |
| Service | `eventbridge/group_service.py` — create, fan out, attribute, complete once, sweep deadlines |
| Progress | `eventbridge/groups.py` — denominator, ETA from observed throughput, stall detection |
| Pages | `/v0/groups/<id>` (dashboard), `/v0/groups` (list, HTML or JSON by `Accept`) |
| API | `POST /v0/groups`, `/status`, `/close`, `/cancel`; `POST /v0/agents` takes `groupid` |
| CLI | `group run|status|watch|list` |
| Tests | **56** in `tests/test_groups.py` |

### Measured on ykt1: a group of 100

```text
▸ group demo-100 soft-panda-7058 · https://…/v0/groups/soft-panda-7058
· 100 agents submitted together
  0/100 done · 100 queued · 19s elapsed
  3/100 done · 4 running · 93 queued · 22s elapsed · about 1 minute left
 33/100 done · 14 running · 53 queued · 44s elapsed · about 1 minute left
 69/100 done · 15 running · 16 queued · 51s elapsed · less than a minute left
100/100 done · 1m02s elapsed
✔ group complete · 100 finished · 0 failed · 1m02s
```

Mock agents now sleep a uniform 1-5 s (`ER_MOCK_DELAY_MIN_S`/`MAX_S`). Without that a
100-agent batch completes faster than the page can render, so the progress display
never shows progress — the one thing the feature exists to do.

### What the implementation found that the design did not

**33. Notification suppression keyed on "group still open" leaks badly.** The first
version suppressed a member event when its group had not completed. A 100-agent batch
still produced **~47 per-agent notifications**. The cause is RQ-1's at-least-once
delivery, one layer down: KEDA scales pods down, their offsets were never committed,
Kafka redelivers the requests, the agents **re-run**, and their second terminal events
arrive *after* the group has completed — sailing straight through the open-group check.

```text
[groups] duplicate terminal event for soft-panda-7058/tired-leopard-2563 ignored
[groups] duplicate terminal event for soft-panda-7058/wild-hornet-2756 ignored      … ×47
```

Worth being precise about what was and was not broken: the **counting** was correct
throughout — every duplicate was ignored, the totals stayed at 100, and completion
fired exactly once. Only the notification policy was wrong. §21.5 protected the
arithmetic and I had assumed that protected everything downstream of it.

The fix is to key suppression on **membership**, not on group state: if an event belongs
to a known group, the group owns its notifications for the whole life of the group.
Two tests pin it — one for the redelivery-after-completion case, one asserting that 100
members plus a third again in duplicates produce exactly two notifications.

**34. `maxReplicaCount` alone does not give fast fan-out.** Raising the cap to 12 did
not make a burst reach 12. KEDA handles 0 → 1 itself but delegates **1 → N to an HPA**,
and Kubernetes' default `scaleUp` policy adds at most `max(100% of current, 4 pods)` per
15 s. From one pod that is 1 → 5 → 10 → 12: about three cycles, ~45 s. A batch that
finishes in ~60 s is half over before the capacity it asked for exists.

Stating the policy explicitly fixes it:

```yaml
advanced:
  horizontalPodAutoscalerConfig:
    behavior:
      scaleUp:
        stabilizationWindowSeconds: 0
        policies: [{type: Pods, value: 12, periodSeconds: 15}]
```

Measured: `replicas=12` on the first observation after the POST, rather than climbing.
There is no thundering-herd risk because the ceiling is already bounded twice — by
`maxReplicaCount` and by the partition count.

**Lowered 12 → 10 on 2026-09-24**, on request, with the `value:` in both HPA policies
moved with it — a `scaleUp` value below the cap would quietly reinstate the rate limit
this finding is about. Partitions stayed at 12: lowering the cap needs nothing, since 12
partitions over 10 consumers just means two of them own two partitions each, whereas
raising it past 12 would need partitions raised first and that is irreversible. The
guard tests are written as relations (`cap ≥ 10`, `cap ≤ partitions`,
`scaleUp value ≥ cap`) rather than against the literal 12, so they held across the
change instead of needing an edit — which is the point of writing them that way.

Verified on both clusters by running a batch and watching `spec.replicas`: ykt1 (40
agents) `0 → 10` at t=6s, 8 pods Running, done in 44 s; Kind (30 agents) `1 → 10` at
t=12s, **10 pods Running**, done in 21 s. Neither exceeded 10.

**35. Partitions are the real ceiling, and raising them is a one-way door.** Ten pods
needs ten partitions: Kafka gives each partition to exactly one consumer in a group, and
KEDA enforces the same bound by default (`allowIdleConsumers: false`). Partitions were
raised 6 → 12 in place, which Strimzi supports — but the CRD says plainly they can never
be decreased. One caveat to know before doing it again: the key → partition mapping
hashes over the partition *count*, so a conversation whose turns straddle the change can
have them land on different partitions and therefore different pods, which is the single
case where §4's per-correlation ordering guarantee does not hold. Harmless for a new
batch; do it while idle.

**36. A group page that reloads is unusable.** The first version polled `/status` and
called `location.reload()` on completion. For a 100-row table refreshing every second
that discards scroll position and flickers — the opposite of what a page you leave open
is for. It now patches the DOM in place every second, repaints only when the payload
actually changes, and shows a per-row "last activity" age plus a "last updated" clock
that keeps counting between fetches, so a stalled poll is visible as a growing number
rather than as a page that merely looks fine.

**37. Embedding JavaScript in an f-string is a trap.** Every literal `{` in the script
needs doubling, and the first attempt failed with
`ValueError: Invalid format specifier` from a JS object literal. The script now lives in
a `string.Template`, which has no brace semantics at all.

**38. "Replayable from Kafka alone" was true of the format and false of the code.**
§21.2 argued that putting group events on `responses` made the group lifecycle
reconstructible from the topic, and the wire format does support that. Nothing
reconstructed it. The live responses consumer uses a fixed group id and commits offsets
— it must, or every start would re-insert every response ever written — so a restarted
pod resumes at its committed offset and re-reads nothing. Combined with `emptyDir` at
`/data` in the `test` and `kind` overlays, a restart produced an empty store:

```text
gentle-tapir-7180  ->  404          # both had been notified hours earlier,
soft-panda-7058    ->  404          # so following the ntfy link died here
```

The user found it from the other end — "why was no ntfy sent for the test group?" —
and the answer was that two *had* been sent, and the page they pointed at was gone. A
notification that outlives the state it links to is worse than a missing notification.

The fix is `eventbridge/kafka_group_mirror.py`, a second read-only consumer in the shape
of the `RequestsMirror` that already back-fills prompts. It rebuilds only the group
tables, never publishes, and exits once caught up. On ykt1 it replays 7 280 records
across 12 partitions and reconstructs 15 groups in under 25 s.

**39. "Three empty polls means caught up" reads zero records.** That was the first
version's stop condition, and it deployed cleanly and did nothing:

```text
[group-mirror] rebuilt 0 group event(s) and 0 member event(s) across 0 group(s)
```

The empty polls were the consumer joining its group — coordinator discovery and the
first rebalance — not the end of the topic. It is a genuinely appealing heuristic and it
is wrong in the one case that matters, at startup, every time.

The replacement has no consumer group at all: assign every partition by hand, snapshot
the end offsets, `seek_to_beginning`, and read until `position(tp) >= end[tp]` for every
partition. That removes the coordinator round trip and the rebalance along with the
ambiguity, and makes completion a fact rather than an inference. An empty poll now only
arms a give-up counter for the pathological case (a deleted segment, an aborted
transaction) where the end offset is unreachable.

**40. On the topic, members come before the completion — so a naive replay publishes a
duplicate.** Replaying member events through the normal path completes the group on the
last member, publishes `group.completed`, notifies, and only *then* reads the real
completion already sitting on the topic. Hence `replay=True`, which applies the member
and returns before the completion check.

The mirror does still settle one case, and it is the only case where a replay creating a
new fact is correct: a group whose members all finished but whose `group.completed` is
absent, i.e. EventBridge died inside that window. After its scan the mirror calls
`maybe_complete()` for every group it touched, which is safe because §21.5's transition
is once-only in SQL (`WHERE completed_utc IS NULL`) — a group already completed on the
topic stays silent.

Eight tests drive `run()` over a fake multi-partition topic, including the warm-up empty
polls and the late metadata that caused the two failures above. Verified on the cluster
the way the bug was found, from the outside:

```text
ntfy before restart                                   83
[group-mirror] replaying 7269 record(s) across 12/12 non-empty partition(s)
[group-mirror] rebuilt 14 group(s) from 28 group event(s) and 7083 member event(s)
gentle-tapir-7180  ->  200        soft-panda-7058  ->  200   (100/100, reason=all)
ntfy after restart                                    83     delta 0
```

Then a fresh 3-agent group end to end: 2 notifications, restart, still `3/3 · reason=all
· 3 members`, ntfy still unchanged.

**41. `partitions_for_topic` returning `None` does not mean the topic is empty.**
Topic metadata is fetched lazily, so the first call after constructing a consumer can
legitimately return nothing. Treating that as "no partitions" skips the replay silently
— the same failure as #39, reached by a different route. The mirror retries for up to
10 s before concluding the topic is unreadable, and says so in the log if it does.

## 7. Blocked: the real-agent gates

The demo overlay deploys and everything up to the model call works. What does not
work is the model call, for a reason outside this project: **the LiteLLM team
budget is exhausted.**

```text
{"error":{"message":"Budget has been exceeded! Team=b19f95b6-… Current cost:
30001.64591778147, Max budget: 30000.0","type":"budget_exceeded","code":"429"}}
```

The same key and endpoint returned 429 from inside a cluster pod and from the
laptop, so it is an account limit, not a networking or configuration fault. Note
that T1.7 (§2) ran successfully against the same endpoint earlier in the session,
so the budget was crossed somewhere between the two.

**What the demo overlay did prove before hitting the wall**, all of it on the real
cluster:

- Preflight passed 37 checks against the demo overlay, including **check 13**: the
  derived image runs `claude --version` on a real amd64 node and still loads
  EventRunner's config.
- `Secret/anthropic-credentials` was created from the environment via stdin, with
  only a 4-character prefix and a length logged. The token appears in **no file,
  no manifest and no `kubectl kustomize` output** — verified by grepping the repo.
- The PVC bound, `HOME` moved to `/data/home`, and the pod started in real mode:
  `mock_claude=False (explicit ER_MOCK_CLAUDE=false)`.
- The credential reached the `claude` subprocess through the §8.5 allowlist, with
  the token redacted in the startup log:
  `forwarding to claude subprocess: ANTHROPIC_BASE_URL=… ANTHROPIC_MODEL=…
  ANTHROPIC_AUTH_TOKEN=sk-k…(25 chars) CLAUDE_CONFIG_DIR=/data/home/.claude`
- `claude` started, initialised against the real model and emitted its
  `system/init` frame with 25 tools — then sat in retry backoff on 429s.

**What therefore remains unverified** until the budget is raised:

- Stage 2c with a real agent — the actual §16 Gap B gate. Mock mode proves the
  transcript round trip, the cold pod and the wake, but cannot demonstrate
  *retained context*, and T1.7 proves resume-by-path only on a laptop.
- T1.10's second half: two concurrent conversations each resuming their own
  session rather than crossing wires. The scaling half is checked in mock mode.
- Any real-agent timing for RQ-4 (all latency numbers here are mock-mode).

One incidental confirmation: when the real-agent pod was killed mid-run, its
offset had **not** been committed, so the request stayed in the topic as genuine
lag and the next deploy's idle check correctly reported `replicas=1` instead of 0.
That is RQ-1 behaving exactly as designed, observed by accident.

## 8. Not verified, and honest limits

- **The real-agent gates are blocked on budget, not on code** — see §5.
- **`maxReplicaCount` is 6** (justified by T1.7-T1.9 landing, and ≤ the partition
  count), with `--concurrency N` added to the demo flow to assert both halves of
  T1.10's gate. Only its scaling half has been run.
- **`ER_REQUIRE_SIGNATURE=true` has no end-to-end run.** The signing code is unit
  tested against the RFC vectors and the flag is wired through `consume.py`, but no
  publisher signs requests yet, so the verify-and-reject path has not been
  exercised on a cluster. EventBridge does not sign.
  *(Resolved in Phase 2: EventBridge signs requests and group events, EventRunner
  signs terminal responses, and both verify against the approved-key set. The
  reject paths now have tests. See `DESIGN_PHASE2.md` §4.)*
- **Resource requests and limits are still placeholders** (§18). A real run has
  not produced numbers.
- **Single-broker, ephemeral storage.** A broker restart loses all topic data, and
  `Kafka/my-cluster` reports `Warning=True reason=KafkaStorage` permanently.
  Preflight warns rather than fails.
- **No CI integration and no in-cluster e2e Job** (§18, unchanged).

---

## 9. Signing: the cost of the pure-Python rule

§1.1 bans C extensions, which rules out `cryptography`, so Ed25519 is implemented
from RFC 8032 in `shared/signing.py` (~120 lines) and checked against the
RFC's own test vectors — all three pass for key derivation, signing and
verification, plus tamper, wrong-key, malformed-input and `alg` confusion cases.

Measured on this laptop:

| | |
|---|---|
| `canonical()` | 0.005 ms |
| `sign_event()` | **222 ms** |
| `verify_signature()` | **227 ms** |
| detached JWS | 128 chars |

Canonicalization is free; the cost is entirely the pure-Python scalar
multiplication. At ~3-10 response events per turn that is 0.7-2.2 s of pure
signing overhead per turn, and verification lands on the request path. This is the
concrete reason the feature stays off by default, and it is a real argument for
revisiting §1.1 for this one dependency if signing ever becomes mandatory —
`cryptography` does Ed25519 in microseconds.

Canonicalization is sorted `key=value` lines over a fixed attribute set plus
`sha256(data-bytes)`, chosen over JCS to avoid a dependency. §11 called
byte-for-byte agreement "the part that will bite", so the tests pin it from both
directions: order-independence, absent attributes omitted rather than written
empty, and every attribute in `SIGNED_ATTRS` demonstrably affecting the digest.

---

## 10. The four-stage loop, observed

`scripts/k8s_demo_flow.py` asserts each stage with timings, rather than narrating
it.

```text
[flow] ── stage 0 — idle at zero ──
[flow] PASS EventBridge is up — it stays at 1 replica BY DESIGN (§8.6)
[flow] PASS eventrunner at 0 replicas and Active=False before anything is posted
[flow] ── stage 1 — a task event wakes the agent ──
[flow] PASS KEDA flipped Active=True and scaled up within 90s
[flow] PASS an eventrunner pod reached Ready
[flow] ── stage 2 — the agent runs and streams events ──
[flow] PASS a phase=stdout event arrived
[flow] PASS turn 1 reached a terminal event
[flow] PASS replicas stayed >=1 for the whole run (RQ-1: the commit is deferred)
[flow] ── stage 2b — /continue on a WARM pod resumes the session ──
[flow] PASS turn 2 completed on the warm pod
[flow] ── stage 3 — back to zero when idle ──
[flow] PASS eventrunner returned to 0 replicas
[flow] PASS ScaledObject is Active=False again
[flow] ── stage 2c — /continue AFTER a full scale-to-zero cycle (Gap B) ──
[flow] PASS the /continue woke a NEW pod
```

The stage-2 assertion is the one worth pointing at during a demo: **replicas
stayed ≥ 1 for the whole run.** That is RQ-1 doing double duty — deferring the
offset commit until the terminal event means lag never reaches zero mid-stream, so
KEDA cannot select a pod carrying a live `claude` subprocess for termination. The
fix for "never miss an event" is the same fix as "don't kill a running agent".

---

## 11. Next actions

1. **Raise or reset the LiteLLM team budget**, then run
   `python3 scripts/k8s_demo_flow.py --overlay demo --concurrency 3`. That single
   command closes both real-agent gates: stage 2c (§16 Gap B with retained
   context) and T1.10 (two conversations resuming independently). Everything else
   is already in place — see §5 for exactly how far it got.
2. **Rotate the credential used in this session.** It was pasted into a chat
   transcript, so treat it as exposed regardless of the budget state. It is stored
   only in `Secret/anthropic-credentials` in ns `kev1`; teardown deliberately
   preserves Secrets, so delete it by hand if the cluster is shared.
3. **Pin an immutable image tag** for anything demo-facing. `:dev` caused the one
   e2e failure in this session and §8.8 predicted it.
4. **Commit the work.** `eventing/` is on branch `kev1` in the `examples`
   repository and still untracked/staged. This repo requires `git commit -s` (DCO)
   and forbids `Co-Authored-By` — `CLAUDE.md` mandates
   `Assisted-By: Claude (Anthropic AI) <noreply@anthropic.com>`, enforced by a
   `commit-msg` hook.
5. **Move the `eventbridge` skill into the repository.** The phase docs are done —
   they now live alongside this file in `eventing/agentdocs/` — but the skill is
   still at `ked1/.claude/skills/eventbridge`, which is why 3 tests fail.
6. **Measure resources** from a real run and replace the §18 placeholders.
7. **Decide on `pyproject.toml`'s phantom `tests_e2e` package** — it still makes
   the project non-pip-installable, and both Dockerfiles work around it by
   installing dependencies only.
8. **If signing becomes mandatory**, revisit §1.1 for `cryptography` — 222 ms per
   event is not viable at volume.
