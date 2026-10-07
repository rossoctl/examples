# KubeCon NA — "The Seams Are the Trap"

Status: **knowledge base, living document.** Accepted talk; 30-minute session slot.
Scope: everything needed to deliver the talk — the abstract as submitted, what we
committed to show, the gap between that and the code, the plan to close it, and the
slide/demo shape that fits 30 minutes.

**If you are picking this up to do the work:** §5 is the MVP — four items, ~two days,
and the honest talk they buy. §4 is the full plan it was cut from.

This file is the reference other analyses should cite. It is deliberately specific
about what is **not** done, because the talk's own thesis is that unchecked control
claims are where compositions leak — and `DESIGN_PHASE2.md` §8 is the record of this
repository making thirteen of them.

Read alongside:

| For | Read |
|---|---|
| The wire contract that must not change | [`DESIGN_PHASE0.md`](DESIGN_PHASE0.md) |
| KEDA scaling, scale-to-zero, the §16 gaps | [`DESIGN_PHASE1.md`](DESIGN_PHASE1.md) |
| Identity on the event path, **and §8's thirteen findings** | [`DESIGN_PHASE2.md`](DESIGN_PHASE2.md) |
| Per-user isolation, `AgentSpec`, triggers (design only) | [`DESIGN_PHASE3.md`](DESIGN_PHASE3.md) |
| Measured results, what is blocked | [`IMPLEMENTATION_REPORT1.md`](IMPLEMENTATION_REPORT1.md) |

---

## 1. The abstract as submitted

**Title:** The Seams Are the Trap: Agent Fleets from CNCF Parts You Already Have

> Every vendor is shipping an agent platform. You probably don't need one — what you
> need to know is how to compose the CNCF projects you already are running to get
> headless agents running safely in your clusters.
>
> We identify challenges: cold-start identity gap (the agent egresses before SPIRE
> issues an SVID), trust-scope drift (egress allowlist decided at deploy, stale by
> wake), unverified delivery (the event broker hands off events before signature
> check), and unbound audit (completion not chained back to the signed wake).
>
> We'll walk through one reference pattern: KEDA scaling deployments from queue depth,
> signed CloudEvents (SPIFFE source + JWS) as the wake contract, sidecar policy reading
> SVIDs at runtime, and the event broker log as tamper-evident audit. In live demo we
> show: zero agent replicas → signed task event to agent wake → blocked egress on a
> wrong-scope call → completion bound to the wake event → scale to zero when agents are
> idle.

**Benefits to ecosystem, as submitted** (paraphrased to the four commitments it makes):

1. A four-point **seam taxonomy** — cold-start identity gap, trust-scope drift,
   unverified delivery, unbound audit — each a named integration point between two
   graduated/sandbox projects.
2. A **composition counter-argument** to monolithic agent runtimes, built from KEDA,
   Knative Eventing (or Strimzi-backed Kafka), SPIFFE/SPIRE, CloudEvents + JWS.
3. **Vendor neutrality** — stock CNCF projects; Kagenti/rossoctl referenced as OSS and
   itself a composition, not a platform.
4. A **demo-backed artifact** the community can reference after the talk.

### 1.1 The five demo beats we committed to

The abstract names these in order. Numbering them is useful because the plan below
tracks each one separately.

| # | Beat | Backed by |
|---|---|---|
| D1 | zero agent replicas | ✅ KEDA `minReplicaCount: 0`, measured |
| D2 | signed task event → agent wake | 🟡 JWS real, never run on a cluster |
| D3 | blocked egress on a wrong-scope call | ❌ **no implementation at all** |
| D4 | completion bound to the wake event | ✅ `ce_causationid` |
| D5 | scale to zero when agents are idle | ✅ measured, 30–31 s back to zero with `cooldownPeriod: 30` |

---

## 2. Status against the abstract

Assessed 2026-10-05 against `main`. Method: read all nine `agentdocs/` files and the
full source, then **ran the identity decision functions** rather than trusting
docstrings — which is `DESIGN_PHASE2.md` §8.6's rule, and it changed two conclusions.

Test suite at review time: **904 collected**. On CI, 896 passed and 8 skipped; on a
laptop with the `claude` CLI on `PATH`, 899 passed and 5 skipped — the three extra CI
skips are `test_agentspec.py`'s pinned-CLI check plus `test_graceful_shutdown.py`, which
needs a reachable Kafka. The five constant skips are `test_manifests.py`'s
`no reachable cluster` cases. Quote the collected count and the environment, not a bare
pass figure. (`DESIGN_PHASE2.md` §6 says 490/5; that is Phase 2's own delta measurement,
not a live count — see §5/M4 before touching it.)

### 2.1 The four seams

| Seam | Named CNCF integration | Code status |
|---|---|---|
| Cold-start identity gap | SPIRE → KEDA | ❌ **No SPIRE/SPIFFE/SVID in `eventing/`.** 8 mentions across 4 files, all prose, all aspirational. (`mcp/wiki_memory_tool/` does ship header-simulated SPIFFE identity — a different component, and its own README notes there is no real SVID verification) |
| Trust-scope drift | SPIRE → egress sidecar | ❌ **No NetworkPolicy, no Istio, no AuthorizationPolicy, no sidecar** |
| Unverified delivery | CloudEvents verifier → Knative trigger | 🟡 Verification exists and is real; **no Knative** (Strimzi instead, which the abstract permits) |
| Unbound audit | event bus → work record | 🟡 `ce_causationid` binds response to request; **the replay path verifies nothing** |

### 2.2 What is genuinely done and demo-ready

- **KEDA scale-to-zero, measured on two clusters.** OpenShift (ykt1) and Kind. The only
  sourced figures are `IMPLEMENTATION_REPORT1.md` §3's: wake latency **1.0–5.0 s** on
  ykt1 and **2.2–4.3 s** on Kind; scale back to zero **30–31 s** with
  `cooldownPeriod: 30`; image pull **5.1 s** cold (245 MB), 454 ms–2.45 s warm. Earlier
  drafts of this file quoted 1.7–3.0 s, ~14 s and 22 s, which appear in no other document
  in the repository — do not put them on a slide. Per §4's own sequencing note, ykt1 and
  Kind timings are not comparable, so say which cluster a number is from.
- **Detached JWS over the CloudEvent envelope.** Ed25519 implemented from RFC 8032 in
  pure Python, checked against the RFC's own test vectors.
- **The canonicalization is injective.** Length-prefixed (netstring-style) fields, after
  a plain `key=value` form was found to let a newline in a value synthesize an extra
  attribute — two structurally different events encoding to identical bytes, one
  signature validating both.
- **`ce_causationid`** on every response, binding it to its triggering request.
- **User identity end to end.** GitHub device flow → `GET /user` → approved-list check,
  `401`/`403` deliberately distinct, `ce_submitter` + `ce_submitteriss` on the wire.
  Verified against a real account (§6 of Phase 2).
- **`k8s_demo_flow.py`** asserts the four-stage loop with timings rather than narration.

### 2.3 Two results that are better than the abstract claims

Worth slide space; they are the strongest engineering in the repo.

**Lag means "work not finished," not "not read."** Deferring the Kafka offset commit to
the terminal event makes consumer lag a *correct* queue gauge and makes it structurally
impossible for KEDA to scale away a streaming pod. Proven by accident: a real run sat in
429 retry backoff for 300 s and replicas stayed ≥ 1 for all 300 s.

**`ce_causationid` already closes the "unbound audit" seam.** The abstract presents it as
a challenge; the code has it.

### 2.4 Confirmed by running the code

<!-- VERIFY v0.9.0: this table describes #888 and #885 while they are open. Once
     rossoctl/examples#888 lands, "refuses nothing without a keyset" becomes false and the
     row should say what the fail-closed behaviour is; once #885 lands, drop the
     rewrite-still-applies row. Do not delete the table — it is §5/M3's demo beat. -->

Not from docstrings. These five lines are the honest state of response verification:

```
A) require=True, keyset=None        -> ACCEPTED  ("verification not enabled")   ← #888
B) forged unsigned terminal         -> refused   ✅
C) signed by unapproved kid         -> refused   ✅
D) forged unsigned NON-terminal     -> ACCEPTED  (by design, see below)
E) forged group.completed, enforced -> refused by verifier… then applied anyway ← #885
```

**(D) is a deliberate policy, not a bug.** `emit()` signs terminal events only, because
it runs per `stdout` frame and Ed25519 costs **222 ms to sign, 227 ms to verify** here
(`IMPLEMENTATION_REPORT1.md` §9, measured — not the ~150–200 ms the docstrings
estimate); signing every frame would
add minutes to a chatty run. The verifier matches that policy. The honest limit: this
proves *who finished a run*, not *what it said along the way*. Say that on stage.

**(E) is the one that will bite on stage.** `signing.response_decision` correctly refuses
the forged `group.completed`. But `kafka_in.py:138` rewrites only `phase` and `data` — it
preserves `type` and `groupid`, then routes on `is_group_event(evt)` using the
**original** event. `group_service.on_group_event` reads only those two fields, so it
calls `complete_group()` anyway. A forged event the verifier *correctly rejected* still
ends the batch and fires a "100 finished" notification for work that never ran. The group
mirror also replays group events with **no verification at all** on restart.

So "the event broker log is tamper-evident audit" is currently half-true: the log is
replayable, but the replay trusts it unconditionally.

### 2.5 Open bugs, all still open

<!-- VERIFY v0.9.0: re-check each of rossoctl/examples#885-#889 before the talk. "All still
     open" is the claim with the shortest shelf life in this document, and §5/M1 and M4 are
     the work that invalidates it. Update the row rather than the heading. -->

From the `DESIGN_PHASE2.md` §8 docs review. None fixed as of 2026-10-05.

| Issue | What |
|---|---|
| [#885](https://github.com/rossoctl/examples/issues/885) | Response-signature enforcement is advisory — rejected events still end batches and overwrite verified answers |
| [#886](https://github.com/rossoctl/examples/issues/886) | Audit mode records nothing, so the documented two-flag rollout cannot be performed |
| [#887](https://github.com/rossoctl/examples/issues/887) | Unauthenticated `PUT /transcript` rewrites what an agent resumes from; correlation ids are listable |
| [#888](https://github.com/rossoctl/examples/issues/888) | `EB_REQUIRE_RESPONSE_SIGNATURE` without a keyset refuses nothing and reports nothing |
| [#889](https://github.com/rossoctl/examples/issues/889) | Stale pidfile naming PID 1 crash-loops the pod after an abrupt exit |

#888 is the sharpest asymmetry and the easiest for a reviewer to find: the response side
with enforcement on and no keyset refuses **nothing** while printing
`response verification OFF`; the request side with `ER_REQUIRE_SIGNATURE=true` and no key
refuses **everything**. Both were documented as "refusal is on." Only one was true.

#887 also defeats Phase 2 §3.1's capability-URL argument: `GET /v0/groups` returns the
100 most recent groups without sign-in, and `/v0/groups/{id}/status` lists every member's
correlation id. For any conversation in a group, the capability is published.

### 2.6 The signed path has never run on a cluster

`k8s/overlays/kind-signed/` exists and is carefully written. But:

- No document records a run of it.
- `k8s_demo_flow.py --overlay` accepts only `test`, `kind`, `demo` — it **cannot drive
  `kind-signed`**.
- Every signing result is unit-tested only.

This matters because of this repository's own precedent: `DESIGN_PHASE2.md` §4.4 step 2
records that the signer/verifier policy mismatch (signing terminals only, verifying
all-or-nothing) was found **by running against a live broker, never by a unit test** —
because every test until then used terminal events.

### 2.7 The SPIRE upgrade path does not currently hold

`keyset.py` claims the keyset "is the same shape as a JWKS, so the upgrade path — SPIRE
issuing and rotating the keys — replaces *where the keys come from* without changing a
line of verification logic."

`DESIGN_PHASE2.md` §8.3 already records that this is false, and it is worth repeating
here because it is load-bearing for the talk:

- The verifier accepts **EdDSA only**; SPIRE issues EC or RSA.
- The keyset is a flat JSON map of kid → key; `keyset.load` **rejects a JWKS document
  outright**.
- The kid lookup survives. The algorithm and the file format do not.

Also from §8.3, and under-appreciated: **a flat keyset makes the asymmetric keys
attribution, not restriction.** EventBridge's own key is in `EB_VERIFY_KEYSET_PATH` by
requirement, so its signature verifies on any run's answer — as does any approved
runner's. Phase 2 §4.4 step 5 draws this conclusion for group events and pins them to
`EB_SIGNING_KID`; it does not draw the same conclusion for member answers.

To its credit, `keyset.py` is honest where it counts: *"What it does not do is attest
anything. The set records which keys an operator approved, not which workloads a platform
vouched for. Say that plainly when demoing it."*

---

## 3. The decision that shapes the rest: SPIRE

Two of the four seams name SPIRE. There is no SPIRE. This is the one call to make before
anything else, because it determines both the plan and the slides.

### Option A — reframe the talk (recommended)

Keep the Ed25519 keyset as the *working* control and present SPIRE as the named upgrade
path with the honest gap. Three seams delivered and demo-backed; the fourth scoped
explicitly as future work, with the §8.3 obstacles named.

**Why this is recommended.** You have a strong, measured, honest talk about three seams.
A fourth faked on stage is a bigger risk than one openly scoped as future work — and the
talk's own thesis is that unchecked control claims are the trap. Overselling the guarantee
in a talk *about* overselling guarantees is the one failure mode with no recovery.

There is also a real seam you can speak to without SPIRE: **signing is opt-in, and the
two sides fail in opposite directions** (#888). That *is* a cold-start trust gap — a
different one than the abstract names, but a genuine, measured, reproducible one.

### Option B — wire a minimal SPIRE

Deliverable: SPIRE server + agent on Kind, EventRunner fetches an X.509 or JWT SVID, the
SPIFFE ID becomes the CloudEvent `source`, and the verifier resolves keys from the SPIFFE
trust bundle instead of a ConfigMap.

**Cost, honestly:** this is not a flag flip. It requires an issuer-agnostic verifier
(add EC/RSA to a hand-rolled pure-Python Ed25519 implementation, or relax the §1.1
no-C-extensions rule and adopt `cryptography`), plus JWKS/trust-bundle parsing, plus the
SVID fetch and rotation handling. Call it the dominant engineering cost of the whole
remaining plan.

**The §1.1 tension is the real decision inside Option B.** Phase 2 §4.3 already notes
that if the pure-Python constraint is relaxed, `cryptography`'s Ed25519 is the better
trade than hardening the hand-rolled one. SPIRE forces that question: supporting EC/RSA
by hand is substantially more code than Ed25519 was, and `_scalar_mult` already carries a
documented "not side-channel resistant — do not promote this to a trust boundary that
assumes it is" warning that now has production callers.

### Recommendation

**Option A, with one carve-out.** Reframe SPIRE as the upgrade path, *and* build the
egress block (D3) with NetworkPolicy — which needs no SPIRE at all and restores the most
visual demo beat. See W3 below.

---

## 4. Implementation plan

Ordered by ratio of talk-value to effort. W1–W3 are the ones that change what you can
honestly say on stage; W4–W6 are polish.

**This is the full plan, kept for the reasoning.** The subset actually proposed for the
talk is **§5's M1-M4**, which cuts W6 entirely, defers half of W4, and replaces W3 with a
substitution rather than a build. Read this section for *why* each item matters; read §5
for what to do.

### W1 — Fix #885 and #888 (highest value, smallest change)

These are the difference between a security demo and security theater, and a sharp
audience member can find #888 from the slides alone.

- **#888:** consult `require` even when no keyset is loaded. `kafka_in.py` has
  `ok, why = True, "not checked"` behind `if self._keyset is not None`, so `require` is
  never reached. Fail closed when enforcement is on and verification is unconfigured, and
  make the startup line say so instead of `response verification OFF`.
- **#885:** route on the **rewritten** event, not the original — or clear `groupid` and
  `type` on rejection so `on_group_event` cannot be reached. Then decide what the group
  mirror does on replay: verifying on replay is the correct answer, but note that it
  changes restart behaviour for *existing* unsigned topic data, so it needs a flag.
- Add a test per bug that asserts the **outcome** (the batch does not complete), not the
  verdict. Phase 2 §8.6's rule: `response_decision` returning `False` is not the same as
  the batch not completing.

Exit criteria: forged `group.completed` with enforcement on does **not** complete the
batch and does **not** notify; enforcement with no keyset refuses and says why.

### W2 — Run `kind-signed` end to end, and make it demo-drivable

Without this there is no evidence the signed path survives a real broker, and the
repository's own precedent says unit tests do not catch this class of bug.

- Add `kind-signed` to `k8s_demo_flow.py --overlay` choices (currently
  `("test", "kind", "demo")`).
- Add two demo stages: **a signed wake that verifies** (D2) and **a forged event that is
  refused** — the latter is the money shot and currently has no script.
- Record the result in `IMPLEMENTATION_REPORT1.md` §8, which still says
  `ER_REQUIRE_SIGNATURE=true` has no end-to-end run.
- Confirm the signing cost on-cluster. `IMPLEMENTATION_REPORT1.md` §9 already measured
  **222 ms / 227 ms** on a laptop — re-running it here gave 190/198 ms median, so treat
  ~190–230 ms as the pure-Python band and the hardware as the variable. This is a
  confirmation on cluster hardware, not a first measurement, and the wake-path latency
  number on the slides must include it.

Exit criteria: one command drives zero → signed wake → verified completion → forged
event refused → zero, with assertions and timings.

### W3 — The egress block (D3) — and why the obvious MVP does not work

The only promised demo beat with zero implementation, and the most visceral one.

**The obvious MVP does not work, and the way it fails is on-theme.** The first version of
this section said: add a `NetworkPolicy` to the EventRunner namespace with a DNS + broker
+ model-endpoint egress allowlist, no SPIRE needed. That recommendation was made without
checking the CNI. `k8s/kind/kind-cluster.yaml` creates a default Kind cluster, and Kind's
default CNI is **kindnetd, which does not implement NetworkPolicy**: the object is
accepted by the API server, `kubectl get networkpolicy` shows it, and **nothing is
enforced**. The demo would have shown a policy that appeared to work and blocked nothing
— which is precisely the class of failure this talk is about, reached by skipping §8.5.1
("which code path, by name?") on a piece of infrastructure rather than on code.

So the real options are:

| Option | What it costs | Verdict |
|---|---|---|
| Swap Kind's CNI for Calico or Cilium | `disableDefaultCNI: true`, install a CNI, then re-verify the whole Phase 1 stack on it — Strimzi, KEDA, ingress-nginx, the scale-to-zero timings | ❌ Too much risk this close to the talk, for one demo beat |
| Demo the egress block on **OpenShift** (ykt1), which enforces NetworkPolicy natively | A second cluster in the demo, on VPN, and §"Measured" already warns Kind and cluster timings are not comparable | 🟡 Viable as a **separately recorded clip**, labelled as a different cluster |
| **Cut D3 and substitute the #888 asymmetry** | A few hours, no new infrastructure | ✅ **Recommended — see M3** |

If the beat is built at all, two things still apply:

- **Make the failure legible.** A `NetworkPolicy` denial is a connection timeout, which
  on stage is indistinguishable from a slow network. Either surface the agent's own error
  frame (`phase=error` already renders as a red card in the SSE transcript) or add a
  deliberately short client timeout so it fails in ~2 s rather than 30.
- **Name the limit out loud:** this is a deploy-time allowlist, which is precisely the
  "trust-scope drift" seam the abstract says is the *problem*. Demoing the static version
  and naming the drift is honest and still makes the point. Claiming it is scope-aware at
  wake time would not be.

If Option B (§3) lands later, the same beat upgrades to sidecar-reading-SVID without
changing the demo script.

Exit criteria, if built: a wrong-scope call is blocked, visibly, within a few seconds, in
under 60 s of demo time, **on a CNI that enforces policy** — verified by checking that a
deliberately-denied call actually fails before relying on it.

### W4 — Fix #887 and #889

- **#887** is the one with the widest blast radius for a live demo: an unauthenticated
  `PUT /transcript` changes what the agent *resumes from*, and request signing does not
  cover it. If anyone in the room has the Route hostname, this is reachable. At minimum,
  gate it behind the Phase 2 §3.1 planned HMAC capability key
  (`key = HMAC(server_secret, correlationid + exp)`), which is stdlib-only and stores
  nothing.
- **#889** is a pidfile crash-loop after an abrupt exit. Low glamour, high demo risk —
  this is the bug that bites when you restart a pod between rehearsal and the live run.

### W5 — Honesty pass on the docs the talk points at

The talk will send people to this repository. Three fixes:

- `keyset.py`'s "upgrade path changes no verification logic" claim — correct it per
  `DESIGN_PHASE2.md` §8.3, in place, since the module docstring is what a reader lands on.
- A footnote on `DESIGN_PHASE2.md` §6's test count pointing at §2's current figure.
  **Not** an overwrite: `490/5` is Phase 2's delta measurement and its "adds 40 over
  450/5" arithmetic depends on it. Small, but the talk's thesis is about claims that
  quietly stopped being true, and a historical measurement never stopped being true.
- The `<!-- VERIFY -->` comments `DESIGN_PHASE2.md` §8.5.5 prescribes are now in this
  file (§2.4, §2.5, §5/M1, §5/M3, §6.3) as of M4 — W5 is after the talk, which was too
  late for a document whose §5/M3 depends on #888 staying open. Re-check them here.

### W6 — Stretch, only if W1–W4 land early

- Minimal SPIRE (Option B). Treat as a separate, post-talk workstream unless everything
  else is done with weeks to spare.
- Phase 3's `userkey` / per-user topic isolation is drafted (`DESIGN_PHASE3.md` §2–§3)
  and **not implemented**. Do not promise it. It is good "what's next" slide material and
  nothing more.

### Sequencing

W1 → W2 → W3 are strictly ordered: W2 needs W1's fixes to be worth running, and W3's
beat slots into the W2 demo script. W4 is parallel. W5 is last and cheap. Rehearse on
Kind, not OpenShift — Kind sets `group.initial.rebalance.delay.ms: 0` while the shared
cluster leaves it at 3000 ms, so **Kind timings are not comparable to cluster timings**;
pick one for the recorded numbers and say which.

---

## 5. The MVP cut: M1-M4

§4 is the full plan. This is the subset that makes the talk **honest and demo-backed**,
which is a smaller target than finishing Phase 2. Roughly **two days of focused work.**

The line the MVP buys:

> **Three seams delivered and demo-backed. One named as the upgrade path, with its
> obstacles.**

That is achievable. Four seams delivered is not, and attempting it is what puts the talk
at risk.

| | Item | Effort | Tracked | Blocks |
|---|---|---|---|---|
| **M1** | Fix #888, then #885 | ~0.5 d | [#895](https://github.com/rossoctl/examples/issues/895) | M2 |
| **M2** | Run `kind-signed` end to end | ~1 d | [#896](https://github.com/rossoctl/examples/issues/896) | the recording |
| **M3** | Substitute the egress beat | a decision | — | the demo list |
| **M4** | #889 + the honesty pass | ~2 h | #889 | — |

M2 is the one that must not slip. M3 is a call to make, not work to schedule.

### M1 — Fix #888, then #885  *(~0.5 day)* — [#895](https://github.com/rossoctl/examples/issues/895)

Both call sites were read; both are small and localised. Every `path:line` below is
against **`0667270`**, this document's base — the first draft cited `3264e8d`, the commit
before #883, and the rebase moved `kafka_in.py` by +16 lines and `__main__.py` by +4
without the pointers being re-derived. Re-derive them, do not adjust them, after any
rebase.

**#888** — `kafka_in.py:109`. The decision is unreachable without a keyset:

```python
ok, why = True, "not checked"
if self._keyset is not None:        # <- `require` is never consulted
```

Fix: when `require_signature` is on and `keyset is None`, fail closed. **The startup
banner matters as much as the behaviour** — `__main__.py:80` prints
`response verification OFF` in exactly that configuration, so an operator who asked for
enforcement is told it is off and refuses nothing. The silent wrong state is the bug;
the missing refusal is only half of it.

**#885** — `kafka_in.py:152` routes on `is_group_event(evt)`, the **original** event, so
the rejection rewrite reaches `on_group_event` → `complete_group()`. The rewrite
deliberately preserves `type` and `groupid` for forensics (`insert_response` derives both
`data_json` and `raw_json` from the one dict), so the fix is not to strip them: it is to
not route a **rejected** event to the group handler at all. The forensic record already
lives under `data["rejected"]`, so nothing is lost.

Tests must **assert the outcome, not the verdict** — `DESIGN_PHASE2.md` §8.6's rule.
`response_decision` returning `False` is not the same as the batch not completing, and
that gap *is* #885.

**Out of scope for the MVP:** verifying the group mirror's replay path. It is the right
fix, but it changes restart behaviour for existing unsigned topic data, so it needs a
flag — and it does not appear in the demo.

Exit criteria: a forged `group.completed` with enforcement on does **not** complete the
batch and does **not** notify; enforcement with no keyset refuses and the banner says
why.

### M2 — Run `kind-signed` end to end  *(~1 day, depends on M1)* — [#896](https://github.com/rossoctl/examples/issues/896)

**The item that must not slip.** It is what converts "we wrote a verifier" into "we ran
one", and §2.6 is the argument: there is currently no evidence the signed path survives a
real broker, ~900 passing tests did not catch #885 or #888, and §4.4 step 2 records that
the one comparable bug was found against a live broker and by no unit test.

- Add `kind-signed` to `k8s_demo_flow.py --overlay`, which today accepts only
  `("test", "kind", "demo")` and therefore **cannot drive the signed overlay at all**.
- Two new stages: **a signed wake that verifies**, and **a forged event that is refused**.
  The second is the money shot and has no script today.
- Capture the on-cluster signing cost. The measured laptop figure is 222 ms / 227 ms
  (`IMPLEMENTATION_REPORT1.md` §9), ~190–230 ms across re-runs; whatever the cluster
  gives has to be inside the wake-latency number on the slide, not omitted from it.
- Update `IMPLEMENTATION_REPORT1.md` §8, which still says `ER_REQUIRE_SIGNATURE=true` has
  no end-to-end run.

Exit criteria: one command drives zero → signed wake → verified completion → forged event
refused → zero, with assertions and timings.

### M3 — Substitute the egress beat  *(a decision, not a build)*

<!-- VERIFY v0.9.0: this beat demos rossoctl/examples#888 as a live bug, and M1 fixes #888.
     Run M1 and M3 in that order and the beat becomes "watch the fix land"; fix #888 first
     without reading this and the substituted beat no longer reproduces. If #888 is already
     closed when you read this, reframe the beat as before/after rather than deleting it —
     the asymmetry is the point, not the bug. -->

Per §4/W3, the `NetworkPolicy` MVP does not enforce on Kind's default CNI. Rather than
change CNI or add a second cluster, **cut D3 and substitute the #888 asymmetry**:

> Enforcement on, no keyset → refuses **nothing**, banner says `OFF`.
> The request side, no key → refuses **everything**.
> Both were documented as "refusal is on". One was true.

This is a real, reproducible, measured trust-scope failure, it needs no new
infrastructure, and it fits the talk's thesis better than a static allowlist would have —
a deploy-time allowlist is the seam the abstract calls a *problem*, so demoing it
straight always required an apology. Demo the fix landing.

If D3 is wanted for real, record it on ykt1 as a separate clip and say on-slide that it
is a different cluster.

### M4 — #889 and the honesty pass  *(~2 hours, parallel)*

- **#889** — a stale pidfile naming PID 1 crash-loops the pod after an abrupt exit. Low
  glamour, high demo risk: this is the bug that bites when a pod is restarted between
  rehearsal and the live run.
- **Docs:** correct `keyset.py`'s "the upgrade path changes no verification logic" claim
  (false per §8.3 — EdDSA-only against SPIRE's EC/RSA, and `keyset.load` rejects JWKS).
  **Leave `DESIGN_PHASE2.md` §6's `490/5` alone:** it is Phase 2's own delta measurement
  — "adds 40 over a 450/5 baseline on the same tree" — not a live claim about the suite,
  and overwriting it breaks that arithmetic. The `agentdocs` README says a phase record
  is superseded explicitly rather than edited in place. Add a footnote pointing at §2's
  current count instead.
- Add the `<!-- VERIFY -->` comments `DESIGN_PHASE2.md` §8.5.5 prescribes to any statement
  in this file that depends on #885–#889 staying open — §2.4's table, §2.5, §5/M1 and
  §6.3's slide, and in particular **§5/M3**, whose substituted demo beat stops reproducing
  once M1 lands. Naming the release, the issue and what should change is the rule; this
  document prescribing markers it does not carry is the same inversion §8 is about.

**Deferred: #887.** Widest blast radius in principle — an unauthenticated
`PUT /transcript` changes what the agent resumes from — but the HMAC capability-key fix is
real design work and invisible on stage. Mitigate operationally for the conference
instead: do not expose the Route, or set `EB_AUTH_TOKENS`.

### What the MVP cuts, explicitly

| Cut | Why |
|---|---|
| **SPIRE** (§3 Option B) | Needs an issuer-agnostic verifier — EC/RSA added to hand-rolled pure-Python crypto, or relaxing §1.1 for `cryptography`. Dominant cost of the whole plan |
| **D3 as a live beat** | kindnetd does not enforce NetworkPolicy (§4/W3) |
| **Phase 3 tenancy** | Partly implemented, but owner-scoped reads are not. Good "what's next" material; do not demo it |
| **Group-mirror replay verification** | Correct, but needs a flag and is not on stage |
| **#887 code fix** | Mitigate operationally |

### Sequencing

```
M1 (0.5d) ──▶ M2 (1d) ──▶ record the demo
M4 (2h, parallel)
M3 = a decision, not a build
```

M1 → M2 is strictly ordered: M2 is not worth running until M1's fixes are in. Rehearse on
Kind, and note that Kind sets `group.initial.rebalance.delay.ms: 0` where the shared
cluster leaves it at 3000 ms — so pick one for the recorded numbers and say which.

### The talk the MVP supports

| Seam | After M1-M4 |
|---|---|
| Unbound audit | ✅ closed — `ce_causationid` |
| Unverified delivery | ✅ closed — signed, enforced, **and run on a cluster** |
| Cold-start identity gap | ✅ demoed as the #888 asymmetry, named honestly |
| Trust-scope drift | 🟡 named, with the deploy-time-allowlist limit stated and §8.3's SPIRE obstacles given |

Plus the §8 section (§6.3 below), which is the differentiator and needs no engineering at
all.

---

## 6. The 30-minute shape

30 minutes is roughly **22–24 minutes of content, 4 minutes of demo, 3–4 minutes of
questions.** The demo is the constraint: D1–D5 as written cannot be driven live in four
minutes: wake alone is 1.0–5.0 s on ykt1 and the cycle back to zero is 30–31 s with
`cooldownPeriod: 30` (`IMPLEMENTATION_REPORT1.md` §3), and that is before the agent
does any work.

### 6.1 Demo strategy: pre-record, narrate live

**Record the demo. Do not run it live.** Reasons, in order of how much they would hurt:

1. The full four-stage loop is ~6 minutes in mock mode (`k8s_demo_flow.py`), and the
   real-agent path has been blocked on an exhausted LiteLLM team budget
   (`IMPLEMENTATION_REPORT1.md` §7) — a 429 on stage is unrecoverable.
2. A cold image pull is 5.1 s of the wake (245 MB); warm it is 454 ms–2.45 s
   (`IMPLEMENTATION_REPORT1.md` §3). Pre-pull on the node and the number improves, but
   it is still dead air.
3. Conference wifi. Phase 2 §2.5 already lists offline operation as a design
   requirement for exactly this reason.
4. #889 crash-loops a pod after an abrupt exit.

Record at 1× with a visible clock, cut the dead air with explicit on-screen jumps
("+12 s"), and narrate live over it. Have `watch_topics.py` output on screen for the lag
numbers — it shows the claimed/unclaimed split, which is the detail that makes the KEDA
story land. Keep a terminal ready to run one *short* thing live if the room is engaged.

**Mock mode is the right choice for the recording** and should be stated on the slide.
It costs no tokens, is deterministic, and is auto-selected when no credential is present.
The one thing it cannot show is *retained context* across a scale-to-zero (stage 2c), so
either accept that or record that one beat separately against a funded key.

### 6.2 Running order

| Min | Section | Notes |
|---|---|---|
| 0–2 | The vendor-platform framing | "You probably don't need one." Land the thesis fast |
| 2–5 | The composition: KEDA + Strimzi/Kafka + CloudEvents + JWS | One architecture diagram. Say **Strimzi, not Knative** |
| 5–9 | The seam taxonomy — all four, named | The artifact people take home. One slide, four rows |
| 9–13 | Seam 1 + 4: the wake contract and bound audit | Signed CloudEvents, `ce_causationid`. Lead with the lag insight (§2.3) |
| 13–17 | Seam 2 + 3: scope drift and delivery | Where the egress block and the keyset live. Name what they do **not** prove |
| 17–21 | **Demo** (recorded, narrated) | D1 → D2 → D3 → D4 → D5. Hard-stop at 4 min |
| 21–25 | **§8: thirteen claims that did not hold** | The differentiator. See below |
| 25–27 | What's next, honestly | SPIRE upgrade path + obstacles; Phase 3 tenancy as design |
| 27–30 | Questions | |

### 6.3 The §8 section is the differentiator — give it 3–4 minutes

<!-- VERIFY v0.9.0: "thirteen claims that do not hold in code" is true while #885-#889 are
     open. As they land, the count of claims that do not hold drops while the count of
     claims that *were* found stays thirteen — say "thirteen found" on the slide, not
     "thirteen outstanding", so the number does not need re-deriving before the talk. -->

`DESIGN_PHASE2.md` §8 — thirteen documented claims that do not hold in code, each with a
filed issue, plus the five-question checklist that came out of them — is the most valuable
artifact in this repository, and it is more on-theme than it first appears.

The abstract promises "a named, demo-backed taxonomy of where those compositions leak."
§8.4 names *why* they leak:

> **"each claim was written from the change that introduced it, and stayed true only for
> the configuration that change was exercised in."**

That is a seam taxonomy for security *documentation*, and nobody else at the conference
will be presenting one. The five questions (§8.5) compress to one slide:

1. Which code path, by name?
2. What is the default?
3. What happens when it is half-configured?
4. Can an attacker choose the input the check reads?
5. What does the claim become one release from now?

Plus the two rules from `DESIGN_PHASE2.md` §8.6 that generalize best: **call the real
function** (a
reimplementation of the guard proves nothing about the guard), and **assert the outcome,
not the verdict**.

**This section also inoculates you.** Saying the thirteen out loud turns the open bugs
from something a sharp attendee catches into evidence of rigor. Go first.

### 6.4 Slide-by-slide sketch

| # | Slide | Content |
|---|---|---|
| 1 | Title | |
| 2 | "Every vendor is shipping an agent platform" | The thesis |
| 3 | The parts you already run | KEDA, Strimzi/Kafka, CloudEvents, SPIFFE/SPIRE *(as upgrade path)* |
| 4 | Architecture | HTTP → EventBridge → Kafka:requests → EventRunner → agent → Kafka:responses. KEDA on lag |
| 5 | **The four seams** | The take-home artifact |
| 6 | Seam 4 first: unbound audit | `ce_causationid`. Short — it's solved |
| 7 | **Lag means "not finished," not "not read"** | The best result. The 300 s/429 anecdote |
| 8 | The wake contract | Detached JWS, Ed25519, signed attribute set |
| 9 | **Canonicalization must be injective** | The newline collision. Concrete, memorable, genuinely instructive |
| 10 | What a signature proves — and does not | Terminals only; proves who *finished*, not what it *said*. Keyset ≠ attestation |
| 11 | Trust-scope drift | The egress block, and why a deploy-time allowlist *is* the seam |
| 12 | Demo (recorded) | Mock mode, stated on-slide |
| 13 | **Thirteen claims that did not hold** | |
| 14 | The five questions | |
| 15 | "Call the real function. Assert the outcome." | |
| 16 | What's next | SPIRE upgrade + the EdDSA/JWKS obstacles; Phase 3 tenancy as design |
| 17 | Links | Repo, `agentdocs/`, the five issues |

### 6.5 Things to say out loud, verbatim

Lifted from the code's own docstrings, which are better than a paraphrase:

- *"What it does not do is attest anything. The set records which keys an operator
  approved, not which workloads a platform vouched for."* (`keyset.py`)
- *"This proves who finished a run, not what it said along the way."* (§4.4 step 2)
- *"A security demo that oversells its guarantee is worse than one that does not exist."*
  (Phase 2 §7)

And one framing worth preparing: **"compromising EventBridge means being able to claim
any user."** That is standard PEP design and the alternative is worse (spreading a live
GitHub credential across every runner), but somebody will ask. Phase 2 §2.7 has the
answer ready.

### 6.6 Likely questions, with answers that exist

| Question | Where the answer is |
|---|---|
| Why not SPIRE already? | §3 above; §8.3's EdDSA/JWKS obstacles |
| Why not a JWT library / Keycloak? | Phase 2 §2.1 (opaque token), §4.3 (Keycloak proves holding a secret, which a compromised pod also holds) |
| Why hand-rolled Ed25519? | Phase 1 §1.1; Phase 2 §4.3. Note the `_scalar_mult` timing caveat before someone else does |
| Why not HMAC — it's 170,000× faster? | Phase 2 §4.3: symmetric, so EventBridge could forge any agent's response |
| Kafka ACLs? | Phase 2 §3.3: the authorizer is global, not per-listener, so the demo is either broken or vacuous |
| Multi-tenancy? | `DESIGN_PHASE3.md` §2–§3, **design only** |
| What's the signing overhead? | 222 ms sign / 227 ms verify, measured (`IMPLEMENTATION_REPORT1.md` §9). Pure Python; `cryptography` does it in microseconds. Confirm on-cluster in M2 |

---

## 7. Risks

| Risk | Mitigation |
|---|---|
| D3 (egress block) cannot be built on Kind's default CNI | Resolved in the MVP: M3 cuts the beat and substitutes the #888 asymmetry. If D3 is wanted anyway, record it on ykt1 as a separate, separately-labelled clip (§4/W3) |
| Something else promised in the demo list goes unbuilt | **Cut the beat and say so**, rather than narrating something that did not happen. §1.1's D1-D5 numbering exists so a cut is explicit rather than silent |
| A reviewer finds #888 from the slides | Fix in M1; present it in the §8 section regardless — and after M3 it *is* a demo beat |
| Live demo fails | Pre-record (§6.1) |
| Real-agent path still budget-blocked | Mock mode for the recording; state it on-slide |
| SPIRE question dominates Q&A | Prepare §3's two options as a 30-second answer |
| Talk runs long | The §8 section is the one thing not to cut. Compress slides 6–8 |
| Kind vs cluster timings get mixed | Pick one for the recorded numbers and say which (§4 sequencing) |

---

## 8. Observations worth carrying forward

Six things from the 2026-10-05 review that are not obvious from any single document:

1. **The docs are more rigorous than the code, and that is unusual and valuable.** §8
   exists because somebody restated every claim for a reader who could not see the code,
   then checked each by running it. That process is the talk's best content.

2. **Reading the code is not enough, and neither is the test suite.** All thirteen
   findings coexisted with a passing suite, because the tests exercised the configuration
   each claim was true in. ~900 passing tests did not catch #885 or #888.

3. **My own first attempt at the group-forgery test reported no issue** — because I used
   wrong event-type constants (`io.rossoctl.…` instead of `dev.rossoctl.agent.group.
   completed.v1`). The finding only appeared once I used `ce.TYPE_GROUP_COMPLETED`. A
   negative result from a hand-written harness is worth very little until you have proved
   the harness can produce a positive one. Re-verify E before relying on it.

4. **Half-configured is where everything breaks.** §8.5.3 is the highest-yield of the
   five questions. A control with two variables has four cases, and the three that are
   not "both set" are the ones a reader hits. The worst finding (#888) is exactly that.

5. **Attribution vs restriction is the subtler keyset point** (§8.3) and is under-drawn
   in the design: a flat keyset tells you *which approved key* signed, not *that this
   particular agent was permitted to answer this particular request*. Phase 2 draws this
   conclusion for group events and pins them; it does not for member answers. If anyone
   in the room works on authorization, this is the question they will ask.

6. **§8.5.1 applies to infrastructure, not just to code — and this document broke the
   rule while citing it.** The first version of §4/W3 recommended a `NetworkPolicy` as
   the no-SPIRE MVP for the egress beat, written without checking which CNI the Kind
   config creates. It is kindnetd, which accepts NetworkPolicy objects and enforces
   none of them, so the recommendation would have produced a demo that appeared to work
   and blocked nothing — the exact failure the talk is about, and the same shape as the
   thirteen: a control claim that was true of the configuration imagined rather than the
   one that exists. "Which code path, by name?" has to include "on which CNI, which
   broker, which cluster".

### Caveats on the review this document is based on

The code was read and the decision functions were run, but there was **no cluster
access** — everything stated about cluster behaviour comes from `IMPLEMENTATION_REPORT1.md`
rather than from observation. Cluster-dependent claims in §2 should be re-confirmed in M2.

The CNI finding in §8 observation 6 was reached by reading `k8s/kind/kind-cluster.yaml`, not by
applying a NetworkPolicy to a running Kind cluster and watching it fail to block. That
is the right direction for the conclusion — kindnetd's lack of NetworkPolicy support is
documented upstream — but by `DESIGN_PHASE2.md` §8.6's rule it is still a claim that has
been read rather than run. Confirm it before spending anything on either CNI option.

---

## 9. Change log

| Date | Change |
|---|---|
| 2026-10-05 | Created. Captures the accepted abstract, status against it as of `main`, the SPIRE decision, the W1–W6 plan, and the 30-minute slide/demo shape |
| 2026-10-05 | Added §5, the M1-M4 MVP cut (~2 days) with sequencing, explicit cuts and the talk it supports. Corrected §4/W3: Kind's default CNI is kindnetd and does not enforce NetworkPolicy, so the `NetworkPolicy` MVP was unbuildable as written — three options tabulated, M3 recommends substituting the #888 asymmetry for the egress beat. §7's risk table and §8's observations updated; the 30-minute shape renumbered §5→§6, risks §6→§7, observations §7→§8 |
| 2026-10-06 | M1 and M2 filed as #895 and #896; §5 gains a tracking table |
