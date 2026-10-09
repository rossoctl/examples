# Eventing — executive summary

What the `eventing/` component is, what each phase decided, and what is actually
built as opposed to designed. One page per audience question, with the figures
cited to the document that measured them.

**Audience:** someone deciding whether to fund, demo, or depend on this — not
someone changing the code. For that, read
[`agentdocs/README.md`](../agentdocs/README.md) and follow its reading order.

**Status date:** 2026-10-09. The implementation status in §4 and the KubeCon
figures in §5 expire; §§1–3 do not. Where a number appears here it is quoted
from the document that measured it, named inline.

---

## 1. What it is

Event-driven plumbing that wakes a headless Claude Code agent from a Kafka
message and streams every step of its run back out as CloudEvents.

```text
  HTTP  →  EventBridge  →  Kafka:requests  →  EventRunner  →  claude -p
                                                                  │
                                                                  ▼
   HTML view / SSE  ←  EventBridge  ←  Kafka:responses  ←  stream-json → CloudEvents
```

Two services. **EventBridge** is the HTTP front door, Kafka producer and
consumer, SQLite event store, HTML/SSE transcript view and ntfy fan-out.
**EventRunner** consumes requests, spawns `claude` per request, and converts
each `stream-json` frame into a response CloudEvent.

One constraint runs through every phase: **pure Python, no C extensions**
(`DESIGN_PHASE0.md` §1.1). It is why Ed25519 is implemented from RFC 8032 by
hand, and — see §5 — it is what makes the SPIRE upgrade path expensive.

## 2. The phases at a glance

| Phase | Theme | Status |
|---|---|---|
| **0** | The wire contract: CloudEvents, correlation and session identity, per-conversation ordering | Built; laptop only |
| **1** | Kubernetes and KEDA: scale-to-zero, agent groups | Built; measured on two clusters |
| **2** | Identity on the event path: who submitted, which agent answered | Wired; **13 control claims found false in review** |
| **3** | Per-user isolation, declarative agents, event triggers | **Partial** — rollout steps 1–4 plus T5; the rest is design |
| **4** | Enrollment and human-in-the-loop approval | **Design only**; its core mechanism is measured |

Phases are deltas, not replacements. Where a later phase supersedes an earlier
one it says so rather than editing in place, which is why the Phase 0 documents
remain accurate for the laptop demo they describe.

## 3. What each phase decided

### Phase 0 — the contract that must not change

Binary-mode CloudEvents on two topics. The Kafka message key is the
`correlationid`, so every turn of one conversation lands on one partition.

Two decisions carry the rest of the architecture:

- **Correlation identity is human-readable; session identity is not.** Users get
  `brave-otter-4718`. The `claude` CLI requires a real UUID, so
  `sessionuuid = uuid5(NAMESPACE, correlationid)` — both services derive the same
  UUID from the slug alone, with no mapping table on the hot path.
- **Turns are FIFO per conversation, parallel across conversations.** At most one
  live `claude` subprocess per `correlationid`, with a per-correlation queue for
  anything arriving while it is busy.

Both were forced by experiment rather than chosen. All eight open questions
(Q&E-1..8) were settled by a reproducible test; two were decisive.
**Q&E-2:** `--session-id` rejects a non-UUID outright, which is why the `uuid5`
derivation exists. **Q&E-8:** two concurrent `--resume` calls on one session do
not corrupt the transcript, but their **ordering is nondeterministic** — so
filesystem safety was not enough and the per-correlation router had to exist.

### Phase 1 — KEDA, scale-to-zero, and three gaps

Scaling is a **KEDA `ScaledObject` on a Deployment (0..N)**, deliberately not a
Job per request. Kafka already assigns one partition to one consumer, so
Phase 0's ordering contract holds across pods for free, with no new application
code. Job-per-request is deferred as a *security* story — a per-run capability
envelope — not a scaling one.

The best idea in the component is one inversion: **lag means "work not
finished", not "work not read".** Committing the Kafka offset only after the
terminal event makes consumer lag a correct queue gauge *and* makes it
structurally impossible for KEDA to scale away a pod mid-stream. The fix for
"never miss an event" is also the fix for "don't kill a running agent". Observed
by accident: a real run sat in 429 retry backoff for 300 s and replicas stayed
at or above 1 for all 300 s (`IMPLEMENTATION_REPORT1.md` §7).

Three gaps, each measured on the broker rather than assumed
(`DESIGN_PHASE1.md` §16):

| Gap | What it is | Resolution |
|---|---|---|
| **A — rebalance floor** | `group.initial.rebalance.delay.ms=3000`; every scale-from-zero forms a new consumer generation and waits 3 s | Not fixed. Broker-wide setting on a *shared* cluster, so recorded as a known floor on wake latency |
| **B — ephemeral session state** | `claude --resume` needs its transcript under `$HOME`, which is on the container's ephemeral layer, so `/continue` breaks across a scale-to-zero cycle | Fixed, resting on one proven fact: `--resume` accepts an absolute transcript *path*, so the transcript can be checkpointed and restored |
| **C — idle replay** | Offsets are deleted after 7 days with no group members; the next wake falls back to `earliest` and replays the retained backlog as real API spend | Bounded: topic `retention.ms` set to 24 h, below offset retention, plus a committed starting offset at first start |

**Agent groups** (§21) add batch fan-out with a tracked fan-in: one call submits
N agents, the batch gets its own page, and it produces **exactly two
notifications** instead of N. The brief's design was rejected on analysis: a
decrementing counter double-decrements under at-least-once redelivery and fires
"all done" early, "the one lie a progress display must never tell". Replaced by
per-member terminal facts with derived counts, and a completion guarded by a
conditional SQL update so it survives an EventBridge restart.

**Measured** (`IMPLEMENTATION_REPORT1.md` §3) — state which cluster when quoting
these, as the two are not comparable:

| | Measured |
|---|---|
| Wake latency | **1.0–5.0 s** on OpenShift (ykt1); **2.2–4.3 s** on Kind |
| Scale back to zero | **30–31 s** at `cooldownPeriod: 30` |
| Image pull | 5.1 s cold (245 MB); 454 ms–2.45 s warm |
| Ed25519 sign / verify | **222 ms / 227 ms** (the cost of the pure-Python rule) |
| Group of 100 agents | **1m02s**, 0 failed |

**The bug only a scale-to-zero deployment could expose.** The response emitter
held its per-correlation `sequence` counter in memory. Every scale-from-zero is
a new process starting again at 1, against a store keyed
`PRIMARY KEY (correlationid, sequence)` and written `INSERT OR REPLACE`. A
three-turn conversation across a cycle stored **six rows instead of nine**, with
turn 1's events silently replaced by turn 3's. No error, no failing test, and a
UI that looked plausible while being wrong. It also invalidated a documented
claim that deduplicating on `(correlationid, sequence)` was sufficient. The
general lesson is recorded as such: *moving a stateful component from one
long-lived process to 0..N ephemeral pods invalidates every in-memory counter it
owned.*

### Phase 2 — identity, and the thirteen findings

The constraint that rules out the design most people reach for: **GitHub issues
no verifiable user token.** It is opaque — no signature, no claims, nothing to
check offline. So there is no JWT to verify and no JWT library; EventBridge must
ask GitHub via `GET /user`, which makes the token-to-login cache
**load-bearing rather than an optimisation**. The cache is keyed by
`sha256(token)` so a memory dump or a careless log yields nothing usable, and
failures are not cached so an outage cannot pin a legitimate user to a refusal.
Its honest cost is measured and pinned by a test: a token revoked on GitHub
keeps authenticating for the full 300 s TTL.

Decisions worth carrying to other services:

- **`401` and `403` stay distinct.** `401` is "I do not know you" and carries
  `WWW-Authenticate`, because retrying with a credential is the remedy. `403` is
  "I know exactly who you are and you are not approved", carries no challenge,
  and **names the refused login** — the difference between an actionable error
  and a support ticket.
- **An empty approved-user list denies everyone**, because the other reading
  turns a missing environment variable into an open door.
- **A rejected response is stored as `phase=error`, not dropped**, because "a drop
  is indistinguishable from an agent that never answered". The full event is kept
  under `data["rejected"]` for review.

For agent identity, the `kid` lives in the JWS *protected* header, so it is
covered by the signature and cannot be swapped to relabel an event as another
agent's. The keyset file **is** the authorization list: an unknown `kid` has no
key and the event is refused. Group lifecycle events are pinned to EventBridge's
own `kid`, because a forged `group.completed` ends a batch early and fires a
"finished" notification for work that never ran.

**§8 is the most valuable section in the component, and it is a record of
failure.** Writing the user-facing documentation meant restating every control
claim for a reader who cannot see the code. Six review rounds then checked each
restated claim **by running it**. Thirteen did not hold. The worst:

> `EB_REQUIRE_RESPONSE_SIGNATURE=true` with no keyset **refuses nothing** — the
> decision is never reached. The request side with no key refuses **everything**.
> Both were documented as "refusal is on". One was true.

The diagnosis generalises beyond this component: *each claim was written from the
change that introduced it, and stayed true only for the configuration that
change was exercised in.* And the detail that should worry anyone relying on a
test suite as evidence: **all thirteen coexisted with a passing suite of ~900
tests**, because the tests exercised the configuration each claim was true in.

That produced **five questions to ask of any control claim before it ships**
(§8.5) — the most reusable output of the whole project:

1. **Which code path, by name?** Follow it to the *outcome*, not just to the check.
2. **What is the default?** Most controls here default to off.
3. **What happens when it is half-configured?** A control with two variables has
   four cases, and the three that are not "both set" are the ones a reader hits.
   Highest-yield of the five.
4. **Can an attacker choose the input the check reads?** Including by *omitting* it.
5. **What does the claim become one release from now?**

### Phase 3 — per-user isolation (partially built)

A raw `submitter` cannot be a tenancy key. It fails to name a Kafka topic, a
Kubernetes object and an ntfy topic — and, the one that matters, **any lossy
normalisation can map two identities onto one key**, after which user B reads
user A's events. Replacing illegal characters with `-` maps `a.b@x.com`,
`a_b@x.com` and `a-b@x.com` onto one tenant. Hence the derived `userkey` ends in
a hash: one human with two spellings getting two tenants is **wasteful, not
unsafe**, and the unsafe direction is what the digest makes impossible.

**§3.6 is the model for how to state a boundary.** Per-user topics without a
broker authorizer give you exactly this and no more:

> EventBridge will not serve one user another user's events, and each user's
> agents run in their own pods with their own credentials. Anything with network
> access to the broker can still read any topic.

The document instructs saying the second sentence out loud when demoing it. A
dedicated broker with real ACLs (Tier B) is what upgrades the claim from
"EventBridge will not serve it to you" to "the broker will not". Also recorded
honestly: per-user topics are primarily a **scaling** improvement that looks like
an isolation feature — the aggregate concurrency ceiling rises from 12 pods to
roughly 300.

Three further designs worth knowing: a per-user ntfy topic must **not** contain
the user's name, because the name travels to the phone; a fetched skill bundle
is a **supply-chain artifact** and passes five fail-closed gates (source
allowlist, empty by default because otherwise the runner is an SSRF proxy with
cluster-internal reach; mandatory digest pinning; a signature for anything that
can execute; strip-what-was-not-signed; safe extraction with hard caps); and
event triggers carry **four independent loop controls**, because in this system a
cycle does not hang — *it bills*, in parallel, until someone notices. The hop
counter is a signed attribute, since an unsigned counter can be reset in flight.

### Phase 4 — human-in-the-loop approval (design only)

The whole phase rests on one question, and it was settled by running it: **can a
tool call be paused mid-run in headless `claude -p` until an external system
answers?** Measured against `claude 2.1.270`: a `PreToolUse` hook fires in `-p`
mode, **blocks for 75 s without the tool running**, honours its configured
timeout, and on `deny` the tool never executes while the reason reaches the agent.

Two documented designs were ruled out the same way. **`--permission-prompt-tool`
does not exist** on this CLI; the flag that does requires the Agent SDK driving
the session in-process, which a subprocess-spawning runner cannot do. And
**`plan` mode never attempts the guarded call** — the agent writes a plan and
exits plan mode, so the hook adjudicates nothing.

The control that matters most binds the approval to **a digest of the tool
input**, inside the HMAC. The hook recomputes the digest before returning
`allow`, so a key that approved `echo hi` cannot be replayed against a rewritten
command. Supporting rigour: a timeout is recorded as `timeout`, never as `deny`,
because "nobody was awake" and "a human said no" need different fixes.

The hole that remains is stated plainly: **the hook runs in the pod the agent
runs in.** An agent that can write the settings file or the hook script disables
its own guard, and the mitigation is a filesystem permission, not a
cryptographic control.

## 4. What is built, and what is not

| Capability | State |
|---|---|
| CloudEvent wire contract, correlation/session identity, per-correlation FIFO | Built |
| KEDA scale-to-zero on consumer lag, graceful drain, arbitrary-UID portability | Built, measured on OpenShift and Kind |
| Agent groups: fan-out, tracked fan-in, two notifications | Built, measured at 100 agents |
| GitHub device-flow sign-in, approved-user list, `401`/`403`, `ce_submitter` | Built; **off by default** — a deployed instance with no configuration is open |
| Event signing and verification, `kid` keyset, group-event pinning | Built; all signing variables default off; **never run end to end on a cluster** |
| Per-user tenancy keys, per-user stores, ownership index, declarative `AgentSpec` | **Partial.** `EB_TENANCY_MODE` defaults to `single`, reproducing Phase 2 exactly |
| Owner-scoped reads, transcript auth, per-user ntfy, triggers, fetched skills, Kafka ACLs | Design only |
| Enrollment, human-in-the-loop approval | Design only |

Two statuses deserve emphasis because they are easy to misread.

**Phase 3's `multi` mode routes but does not yet authorize.** It gives each
tenant its own store and topics; it does **not** check that a reader is the
owner. The flag is deliberately environment-only — a committed config file that
could flip it is "a way to get per-user isolation half-enabled by accident". It
should not be enabled or demoed until owner-scoped reads and transcript
authentication land.

**Signing is implemented but unproven in deployment.** The cryptography is
checked against the RFC's own test vectors and the canonicalisation is
injective, but the signed path has never run end to end on a cluster, and the
open issues in §5 mean enforcement is currently advisory.

## 5. The KubeCon NA 2026 commitment

The accepted talk — *The Seams Are the Trap: Agent Fleets from CNCF Parts You
Already Have* — commits to a four-seam taxonomy and five live demo beats.
Assessed against the code (`KUBECON_NA_2026.md`, with the open-issue state
re-checked 2026-10-09):

| Beat | State |
|---|---|
| D1 zero agent replicas | Backed — KEDA `minReplicaCount: 0`, measured |
| D2 signed task event → agent wake | Partial — JWS is real, never run on a cluster |
| D3 blocked egress on a wrong-scope call | **No implementation at all** |
| D4 completion bound to the wake event | Backed — `ce_causationid` |
| D5 scale to zero when idle | Backed — 30–31 s measured |

**The decision that shapes everything else: two of the four seams name SPIRE,
and there is no SPIRE.** Wiring it is not a flag flip — it requires an
issuer-agnostic verifier, which means either adding EC/RSA support to
hand-rolled pure-Python crypto or relaxing the no-C-extensions rule in favour of
`cryptography`. That is the dominant engineering cost of the remaining plan, and
it is where the §1.1 constraint stops being free.

The recommendation on record is to **reframe SPIRE as the named upgrade path**
rather than fake a fourth seam on stage, on the grounds that "overselling the
guarantee in a talk about overselling guarantees is the one failure mode with no
recovery". The MVP that makes the talk honest and demo-backed is roughly two
days of work and buys this line:

> Three seams delivered and demo-backed. One named as the upgrade path, with its
> obstacles.

Per the same document, the demo should be **pre-recorded and narrated live**:
the real-agent path has been blocked on an exhausted model-API budget, a 429 on
stage is unrecoverable, and conference wifi is already a documented design
assumption.

## 6. Assessment

**What is strong.** Decisions trace to measurements and experiments rather than
to reasoning, and the documents record what is *not* verified as carefully as
what is. Deferring the offset commit so lag means "unfinished", deriving group
completion from stored facts instead of a mutable counter, and binding an
approval to a digest of the tool input are each the right answer to a problem
that has a tempting wrong answer. The §8.5 five questions are portable to any
service making control claims.

**Where the risk actually is.** Not in any single open bug, but in the pattern
Phase 2 §8.4 names: **the documentation is more rigorous than the code**, and
~900 passing tests did not catch the two worst findings. Half-configured states
are where this system fails, and the two services fail in *opposite* directions.
Phases 3 and 4 add controls of exactly the same shape, so the five questions need
to be a gate on new work rather than a retrospective on old work.

**Three decisions outstanding.**

1. **SPIRE** — reframe as upgrade path, or wire a minimal version. Blocks the
   talk's framing.
2. **The pure-Python rule** — now load-bearing in a way it was not designed to
   be. The hand-rolled scalar multiplication carries a "not side-channel
   resistant — do not promote this to a trust boundary that assumes it is"
   warning that now has production callers, and the rule is what makes SPIRE
   expensive.
3. **Phase 3 `multi` mode** — do not enable or demo it until owner-scoped reads
   and transcript authentication land.

---

## Where to read more

| For | Read |
|---|---|
| The whole document set and its reading order | [`agentdocs/README.md`](../agentdocs/README.md) |
| The wire contract that must not change | [`agentdocs/DESIGN_PHASE0.md`](../agentdocs/DESIGN_PHASE0.md) |
| Running it, in any of four environments | [`agentdocs/README_PHASE1.md`](../agentdocs/README_PHASE1.md) |
| Identity, and the thirteen findings | [`agentdocs/DESIGN_PHASE2.md`](../agentdocs/DESIGN_PHASE2.md) §8 |
| Writing a claim about a control, in any phase | [`agentdocs/DESIGN_PHASE2.md`](../agentdocs/DESIGN_PHASE2.md) §8.5 |
| The talk: status, plan, slide shape | [`agentdocs/KUBECON_NA_2026.md`](../agentdocs/KUBECON_NA_2026.md) |
