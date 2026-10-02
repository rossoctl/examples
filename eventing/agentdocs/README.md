# agentdocs

Long-form design and build records for the `eventing/` demo, kept in the repository
rather than in someone's scratch directory so the claims in them can be checked
against the code they describe.

They are written to be read by whoever picks this up next — human or agent — and
they are deliberately specific: measured numbers, failures with their root causes,
and what is *not* verified.

## The files

| File | What it is |
|---|---|
| [`DESIGN_PHASE0.md`](DESIGN_PHASE0.md) | The original design: the CloudEvent wire contract, the two topics, the correlation-ID and `uuid5` session scheme, the per-correlation FIFO router, and the eight questions-and-experiments (Q&E-1..8) that settled it. |
| [`IMPLEMENTATION_REPORT0.md`](IMPLEMENTATION_REPORT0.md) | What Phase 0 actually built against that design, with test results, the deltas from the plan, and the follow-ups it left behind. |
| [`README_PHASE0.md`](README_PHASE0.md) | The Phase 0 runbook: how to stand up a local Kafka, run both services on a laptop, and drive a turn end to end. |
| [`DESIGN_PHASE1.md`](DESIGN_PHASE1.md) | The Phase 1 design — a delta over Phase 0, not a replacement: KEDA scaling on consumer lag, scale-to-zero, the three cluster findings that shaped it, the §16 gaps (A: rebalance floor, B: ephemeral session state, C: idle replay), §3.2 on supporting a local Kind cluster, and §21 designing agent **groups** — batch fan-out with a tracked fan-in, its own page and exactly two notifications. |
| [`IMPLEMENTATION_REPORT1.md`](IMPLEMENTATION_REPORT1.md) | What Phase 1 built, the measured results on both clusters, 28 findings including two real bugs only a scale-to-zero deployment could expose, and an honest account of what is still blocked and why. |
| [`README_PHASE1.md`](README_PHASE1.md) | The Phase 1 runbook in full detail, covering all four ways to run it: locally without containers, under Docker, on a Kind cluster, and on OpenShift. |
| [`DESIGN_PHASE2.md`](DESIGN_PHASE2.md) | The Phase 2 design — identity on the event path. Why GitHub's opaque user token rules out local verification and forces a `GET /user` lookup plus a load-bearing cache; why `401` and `403` are kept distinct; what `ce_submitter` is and is not worth, and what a signature over it does and does not prove; how the `kid` and the approved-key set prove *which agent* answered, why group events are pinned to EventBridge's own key, and why a rejected response is stored as an error rather than dropped. |
| [`DESIGN_PHASE3.md`](DESIGN_PHASE3.md) | The Phase 3 design — per-user isolation, declarative agents, and event triggers. Why a raw `submitter` cannot be a tenancy key and why the derived `userkey` ends in a hash; per-user Kafka topics (`{prefix}-{userkey}-requests`), one EventRunner Deployment and ScaledObject per user, and the honest difference between separately-named topics and ACL-enforced isolation; why a per-user ntfy topic must *not* contain the user's name, and the option of running ntfy as a service in the cluster; the `AgentSpec` — an agent baked into the image or assembled from skills fetched from GitHub, S3 or a URL, and the five gates a fetched bundle passes; authenticating and scoping the session transcript; and a Knative-inspired trigger API with the four loop controls that keep an event-driven agent system from becoming a cost incident. **Design only — not implemented.** |

## Reading order

- **Running it?** [`README_PHASE1.md`](README_PHASE1.md), or the [Phase 1
  section](../README.md#phase-1--kubernetes-and-keda) of the component README for
  the short version.
- **Changing it?** [`DESIGN_PHASE0.md`](DESIGN_PHASE0.md) for the wire contract that
  must not change, then [`DESIGN_PHASE1.md`](DESIGN_PHASE1.md) for the deployment
  model.
- **Working on auth or identity?** [`DESIGN_PHASE2.md`](DESIGN_PHASE2.md) §2.1 first:
  the opaque-token constraint is what rules out the design most people reach for.
- **Debugging something odd?** [`IMPLEMENTATION_REPORT1.md`](IMPLEMENTATION_REPORT1.md)
  §4 — most surprises are already recorded there with their cause.
- **Presenting it?** [`DESIGN_PHASE2.md`](DESIGN_PHASE2.md) §2.6 and §3, for what the
  identity controls do *not* prove.
- **Working on multi-tenancy, agent definitions or triggers?**
  [`DESIGN_PHASE3.md`](DESIGN_PHASE3.md) — §9 for the rollout order (the first four steps are
  refactors; everything risky is behind a flag that defaults off), §3.6 for what isolation does
  and does not claim without Kafka ACLs, and §7.7 before writing any of the trigger code.

The `PHASE0` documents describe a laptop demo and remain accurate for that; where
Phase 1 supersedes them it says so explicitly rather than editing them in place.
