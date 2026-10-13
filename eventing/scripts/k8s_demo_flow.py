#!/usr/bin/env python3
"""The §16 four-stage demo flow, asserted rather than narrated. Tasks T5.3-T5.7.

    stage 0   idle at zero          minReplicaCount: 0
    stage 1   a task event wakes the agent
    stage 2   the agent runs and streams events
    stage 2b  /continue on a WARM pod resumes the same session
    stage 2c  /continue after a FULL scale-to-zero cycle also resumes  <- Gap B
    stage 3   back to zero when idle

Stages 2c and 3 are the point. Without them the demo works on the day it is built
and fails a week later, which is the worst possible failure mode for something
whose entire purpose is sitting idle at zero.

On `--overlay kind-signed` two more run (#896), which are the talk's D2 beat and the
one the abstract cannot back without them:

    signed wake        the terminal response carried a signature that VERIFIED, on a
                       cluster — produced by a runner pod from a Secret-mounted seed and
                       checked by a bridge pod against a ConfigMap keyset
    forgery refused    a throwaway pod publishes a correctly-encoded but UNSIGNED
                       terminal and group.completed onto the responses topic; the batch
                       does not settle, the genuine answer survives, and the attempt is
                       on record
    audit trail        the topic read back with `audit_topic.py --chains`: which events
                       are signed, and which result answers which signed wake event.
                       Run AFTER the forgery on purpose — an audit that showed a clean
                       topic after an attack would be worse than no audit

  python3 scripts/k8s_demo_flow.py                         # mock mode, free
  python3 scripts/k8s_demo_flow.py --overlay kind-signed   # + the signed beats
  python3 scripts/k8s_demo_flow.py --overlay demo          # real claude
  python3 scripts/k8s_demo_flow.py --skip-idle-replay
  python3 scripts/k8s_demo_flow.py --skip-cold-continue   # faster, skips 2c

Boundary to state out loud when demoing: the AGENT scales to zero, not the whole
system. EventBridge stays at one replica — something has to accept the POST and
hold the event store (§8.6). The claim is "no agent capacity is consumed while
idle", not "nothing is running".
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import k8s_preflight  # noqa: E402
from imagelib import DEFAULT_REGISTRY, DEFAULT_TAG  # noqa: E402
from k8slib import Kubectl, condition_is, condition_reason, dig, wait_for  # noqa: E402
from proclib import Checks  # noqa: E402

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from shared import signing  # noqa: E402

# The forgery, run inside a throwaway pod from the eventrunner image (#896). Kept as a
# module constant rather than inline so it reads as the attacker's program it is.
#
# It imports the REAL `shared.ce`, so the events are byte-identical in shape to a
# genuine publisher's — which is the point: the demo must refuse a correct forgery, not
# a malformed one. It signs nothing, because an attacker has no approved key; that is
# precisely what the keyset makes expensive.
FORGE_SRC = """
import json, sys
sys.path.insert(0, "/app")
from kafka import KafkaProducer
from shared import ce

GID, CORR = %(gid)r, %(corr)r
p = KafkaProducer(bootstrap_servers=%(bootstrap)r, acks="all", retries=3)

# 1. A forged terminal for a member that already answered: overwrite the real answer
#    and fail a healthy agent.
#
#    **The sequence is chosen NOT to collide**, which is the opposite of what an
#    attacker would do and is deliberate. `next_seq` starts at 1, so a forgery at
#    `sequence=1` always lands on a sequence the genuine run already holds — and #955's
#    store rule then refuses the rejected row rather than storing it. That is the
#    STRONGER outcome, but it leaves no row carrying `data.signature_rejected`, so the
#    "the refusal is on record" half of the beat finds nothing and fails for a reason
#    that has nothing to do with signing.
#
#    So: a high sequence for the recorded beat, and `--forge-collide` on the flow to
#    demonstrate the collision case separately, where the assertion is that the genuine
#    row SURVIVES rather than that a rejection was stored.
member = ce.new_event(
    type=ce.TYPE_RESPONSE, source="rossoctl://attacker/forge",
    datacontenttype="application/json", correlationid=CORR,
    sessionuuid=ce.session_uuid(CORR), sequence=%(seq)d, phase="error", final="true",
    groupid=GID, data={"text": "Ship the goods", "role": "final"})

# 2. A forged group.completed: end the batch early and page the operator with a
#    "finished" notification for work that never ran.
group = ce.new_event(
    type=ce.TYPE_GROUP_COMPLETED, source="rossoctl://attacker/forge",
    datacontenttype="application/json", groupid=GID,
    data={"reason": "all", "finished": 2, "failed": 0})

n = 0
for evt, key in ((member, CORR), (group, GID)):
    headers, value = ce.to_kafka_binary(evt)
    p.send(%(topic)r, key=key.encode(), value=value,
           headers=[(k, v) for k, v in headers])
    n += 1
p.flush(timeout=30)
print("FORGED", n, "events onto", %(topic)r)
"""

PREFIX = "flow"
KAFKA_NS = "kafka"

START_PROMPT = ("Remember this codeword: FLOW-7321. Reply with exactly OK and "
                "nothing else. Do not use any tools.")
WARM_PROMPT = ("What codeword did I give you? Reply with just the codeword. "
               "Do not use any tools.")
COLD_PROMPT = ("Once more: what was the codeword? Reply with just the codeword. "
               "Do not use any tools.")
CODEWORD = "FLOW-7321"


def get_json(url, *, timeout=10.0, method="GET", body=None):
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, (json.loads(raw) if raw else None)


class Flow:
    def __init__(self, k: Kubectl, c: Checks, args) -> None:
        self.k = k
        self.c = c
        self.args = args
        self.ns = args.namespace
        self.base = ""
        self.corr = ""
        # Mock unless the overlay supplies a credential, which only `demo` does
        # (`eventrunner-demo-patch.yaml` is the one place `ER_MOCK_CLAUDE=false` is set).
        # This read `args.overlay == "test"`, which was wrong for `kind` before
        # `kind-signed` existed: the pod auto-detects mock mode with no credential
        # mounted, while the flow asserted a real-agent reply. Worse than a clean
        # failure, because the mock reply is `MOCK-REPLY[<prompt>]` and the prompt
        # contains the expected text — so stage 2's `expect_contains(text, "OK")` could
        # pass by accident while proving nothing.
        self.mock = args.overlay != "demo"
        # #896 — default on for the overlay that enforces signatures, since that is the
        # only reason `kind-signed` exists. Explicit flags win, so the beats can also be
        # driven against any other enforcing deployment.
        self.signed_beats = (args.signed_beats if args.signed_beats is not None
                             else args.overlay == "kind-signed")

    # ---- helpers ----
    def replicas(self) -> int:
        return self.k.replicas("eventrunner", namespace=self.ns)

    def so(self) -> dict:
        return self.k.get("scaledobject", "eventrunner",
                          namespace=self.ns, missing_ok=True) or {}

    def turns(self) -> list[dict]:
        try:
            _, body = get_json(f"{self.base}/v0/agents/{self.corr}/turns")
        except (urllib.error.URLError, OSError):
            return []
        return (body or {}).get("turns") or []

    def wait_turn(self, n: int, timeout: float) -> tuple[bool, float]:
        """Wait until turn n has completed (its assistant reply is present)."""
        started = time.monotonic()
        out = wait_for(lambda: len([t for t in self.turns()
                                    if t.get("assistant_text") is not None
                                    or t.get("stats")]) >= n,
                       timeout=timeout, interval=2)
        return bool(out), time.monotonic() - started

    def reply_text(self, turn_index: int) -> str:
        """Assistant text for a turn, falling back to the stored events.

        The fallback matters: on-wire dedupe strips `text` from the terminal event
        when it equals the prior assistant message, so `/turns` is authoritative
        but a raw event scan is the backstop.
        """
        for t in self.turns():
            if t.get("turn_index") == turn_index and t.get("assistant_text"):
                return t["assistant_text"]
        try:
            _, body = get_json(f"{self.base}/v0/agents/{self.corr}/events")
        except (urllib.error.URLError, OSError):
            return ""
        return " ".join((e.get("data") or {}).get("text") or ""
                        for e in (body or {}).get("events") or [])

    def wait_for_zero(self, why: str) -> bool:
        """Wait for zero replicas AND no surviving pod.

        A pod that is terminating is still a consumer-group member and still
        consuming, so `spec.replicas == 0` alone does not mean the agent is idle —
        and a stage that posts work right afterwards would be served by the dying
        pod instead of waking a new one.
        """
        cooldown = int(dig(self.so(), "spec", "cooldownPeriod", default=300))
        budget = cooldown + self.args.cooldown_slack
        self.c.log(f"waiting up to {budget:.0f}s for scale-to-zero "
                   f"(cooldownPeriod={cooldown}s) — {why}")
        out = wait_for(lambda: self.replicas() == 0 and not self.k.pod_names(
            "app.kubernetes.io/name=eventrunner", namespace=self.ns),
            timeout=budget, interval=5)
        return bool(out)

    # ---- stage 0 ----
    def stage0(self) -> None:
        self.c.section("stage 0 — idle at zero")
        pf = k8s_preflight.Preflight(self.k, self.c, self.args)
        self.base = self.args.public_url or pf.route_url()
        if not self.c.expect(bool(self.base), f"public URL: {self.base or '(none)'}"):
            return
        self.c.expect(self.k.ready_replicas("eventbridge", namespace=self.ns) >= 1,
                      "EventBridge is up — it stays at 1 replica BY DESIGN (§8.6); "
                      "it is the agent that scales to zero")
        settled = wait_for(lambda: self.replicas() == 0
                           and not self.k.pod_names(
                               "app.kubernetes.io/name=eventrunner", namespace=self.ns)
                           and condition_is(self.so(), "Active", "False"),
                           timeout=self.args.idle_timeout, interval=5)
        self.c.expect(settled,
                      "eventrunner at 0 replicas, no lingering pod, and Active=False "
                      "before anything is posted",
                      f"replicas={self.replicas()} "
                      f"pods={self.k.pod_names('app.kubernetes.io/name=eventrunner', namespace=self.ns)} "
                      f"Active={condition_reason(self.so(), 'Active')}")
        self.c.log(f"  watch it live:  kubectl -n {self.ns} get deploy eventrunner -w")

    # ---- stage 1 ----
    def stage1(self) -> None:
        self.c.section("stage 1 — a task event wakes the agent")
        posted = time.monotonic()
        try:
            status, body = get_json(f"{self.base}/v0/agents", method="POST", timeout=25,
                                    body=json.dumps({"prompt": START_PROMPT,
                                                     "max_turns": 4}).encode())
        except (urllib.error.URLError, OSError) as e:
            self.c.fail("POST /v0/agents accepted", str(e))
            return
        self.corr = (body or {}).get("correlationid", "")
        self.c.expect(status == 202 and self.corr,
                      f"POST accepted -> {status} correlationid={self.corr}")
        if not self.corr:
            return
        woke = wait_for(lambda: condition_is(self.so(), "Active") or self.replicas() >= 1,
                        timeout=90, interval=2)
        self.c.expect(woke, f"KEDA flipped Active=True and scaled up within 90s "
                            f"({woke.elapsed_s:.1f}s)",
                      f"Active={condition_reason(self.so(), 'Active')} "
                      f"replicas={self.replicas()}")
        self.c.note("stage 1 wake latency", f"{woke.elapsed_s:.1f}s")
        # §16 Gap A: ~3s of this is the broker's group.initial.rebalance.delay.ms,
        # which scale-from-zero pays on every wake because it forms a new group
        # generation. It is a shared broker-wide setting, so Phase 1 does not
        # change it — it is recorded as a floor.
        if woke.ok:
            self.c.log("  (≈3s of this is the broker's 3000ms "
                       "group.initial.rebalance.delay.ms — §16 Gap A)")
        running = wait_for(lambda: self.k.ready_replicas("eventrunner",
                                                        namespace=self.ns) >= 1,
                           timeout=180, interval=3)
        self.c.expect(running, f"an eventrunner pod reached Ready "
                               f"({running.elapsed_s:.0f}s after the scale-up)")
        self.c.note("stage 1 pod ready", f"{time.monotonic() - posted:.1f}s after POST")

    # ---- stage 2 ----
    def stage2(self) -> None:
        self.c.section("stage 2 — the agent runs and streams events")
        streamed = wait_for(lambda: self._has_phase("stdout"), timeout=180, interval=2)
        self.c.expect(streamed, f"a phase=stdout event arrived "
                                f"({streamed.elapsed_s:.1f}s)")

        # RQ-1 doing double duty: the offset is committed only after the terminal
        # event, so lag stays >=1 for the whole run and KEDA cannot scale a
        # streaming pod away.
        #
        # This has to be sampled DURING the run, in a background thread. Checking
        # `replicas` after the terminal event arrived measures the wrong moment: a
        # mock "run" lasts milliseconds, so by the time the test has observed the
        # terminal event over HTTP the cooldown may already have elapsed and a
        # legitimate scale-down reads as a mid-run kill.
        samples: list[int] = []
        stop = threading.Event()

        def sample():
            while not stop.is_set():
                samples.append(self.replicas())
                stop.wait(1.0)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            done, took = self.wait_turn(1, timeout=self.args.turn_timeout)
        finally:
            stop.set()
            sampler.join(timeout=3)
        self.c.expect(done, f"turn 1 reached a terminal event ({took:.0f}s)")

        observed = [n for n in samples if n >= 0]
        if took < 5.0:
            # Honest about what was and was not measured.
            self.c.log(f"  the run took {took:.1f}s — too short to sample "
                       f"meaningfully; asserting only that a pod was present "
                       f"(samples={observed[:6]})")
            self.c.expect(any(n >= 1 for n in observed) or self.replicas() >= 1,
                          "a pod was running while the turn executed",
                          f"replica samples: {observed}")
        else:
            self.c.expect(observed and min(observed) >= 1,
                          f"replicas stayed >=1 for the whole {took:.0f}s run "
                          f"(RQ-1: the commit is deferred, so lag never hit zero "
                          f"mid-stream)",
                          f"replica samples dipped to {min(observed) if observed else '?'}: "
                          f"{observed}")
        text = self.reply_text(1)
        self.c.log(f"  turn 1 reply: {text.strip()[:100]!r}")
        if self.mock:
            self.c.expect_contains(text, "MOCK-REPLY", "the mock reply rode the wire")
        else:
            self.c.expect_contains(text.upper(), "OK", "the agent answered turn 1")
        self.c.log(f"  transcript: {self.base}/v0/agents/{self.corr}")

    def _has_phase(self, phase: str) -> bool:
        try:
            _, body = get_json(f"{self.base}/v0/agents/{self.corr}/events", timeout=8)
        except (urllib.error.URLError, OSError):
            return False
        return any(e.get("phase") == phase for e in (body or {}).get("events") or [])

    # ---- stage 2b ----
    def stage2b(self) -> None:
        self.c.section("stage 2b — /continue on a WARM pod resumes the session")
        before = self.replicas()
        self.c.log(f"  replicas before: {before} (warm if >=1)")
        if not self._continue(WARM_PROMPT):
            return
        done, took = self.wait_turn(2, timeout=self.args.turn_timeout)
        self.c.expect(done, f"turn 2 completed on the warm pod ({took:.0f}s)")
        if not done:
            return
        text = self.reply_text(2)
        self.c.log(f"  turn 2 reply: {text.strip()[:100]!r}")
        if self.mock:
            self.c.expect_contains(text, "MOCK-REPLY",
                                   "the second turn produced a reply "
                                   "(mock cannot demonstrate retained context)")
        else:
            self.c.expect_contains(text, CODEWORD,
                                   f"the reply contains {CODEWORD} — the claude "
                                   f"session was genuinely resumed")
        self._assert_checkpointed()

    def _assert_checkpointed(self) -> None:
        """The transcript must be in EventBridge, or stage 2c cannot work."""
        try:
            req = urllib.request.Request(
                f"{self.base}/v0/agents/{self.corr}/transcript", method="GET")
            with urllib.request.urlopen(req, timeout=20) as r:
                size = len(r.read())
            self.c.expect(size > 0,
                          f"the transcript is checkpointed in EventBridge "
                          f"({size} bytes) — this is what makes stage 2c possible")
        except urllib.error.HTTPError as e:
            if self.mock:
                self.c.log(f"  no transcript checkpoint (HTTP {e.code}) — expected in "
                           f"mock mode: no `claude` runs, so there is none to store")
            else:
                self.c.fail("the transcript is checkpointed in EventBridge",
                            f"HTTP {e.code} — §16 Gap B's fix did not run; a "
                            f"/continue after scale-to-zero will fail")
        except (urllib.error.URLError, OSError) as e:
            self.c.fail("the transcript endpoint is reachable", str(e))

    # ---- stage 3 ----
    def stage3(self) -> None:
        self.c.section("stage 3 — back to zero when idle")
        back = self.wait_for_zero("the run has finished")
        self.c.expect(back, "eventrunner returned to 0 replicas",
                      f"replicas={self.replicas()}. If lag never reached zero the "
                      f"offsets were not committed — check the terminal event was "
                      f"emitted (RQ-1).")
        inactive = wait_for(lambda: condition_is(self.so(), "Active", "False"),
                            timeout=120, interval=5)
        self.c.expect(inactive, "ScaledObject is Active=False again",
                      condition_reason(self.so(), "Active"))

    # ---- stage 2c: the Gap B regression gate ----
    def stage2c(self) -> None:
        self.c.section("stage 2c — /continue AFTER a full scale-to-zero cycle (Gap B)")
        if self.replicas() != 0:
            if not self.c.expect(self.wait_for_zero("stage 2c needs a cold start"),
                                 "scaled to zero before the cold /continue"):
                return
        self.c.log("  the pod that ran the earlier turns is now gone — its filesystem, "
                   "and the claude transcript on it, with it")
        if not self._continue(COLD_PROMPT):
            return
        woke = wait_for(lambda: self.replicas() >= 1, timeout=90, interval=2)
        self.c.expect(woke, f"the /continue woke a NEW pod ({woke.elapsed_s:.1f}s)")
        done, took = self.wait_turn(3, timeout=self.args.turn_timeout)
        self.c.expect(done, f"the cold turn completed ({took:.0f}s)")
        if not done:
            self.c.log(self.k.logs("deploy/eventrunner", namespace=self.ns, tail=60))
            return
        text = self.reply_text(3)
        self.c.log(f"  cold turn reply: {text.strip()[:120]!r}")
        if self.mock:
            self.c.expect_contains(text, "MOCK-REPLY",
                                   "the cold turn produced a reply (mock mode cannot "
                                   "demonstrate resumed context — run --overlay demo "
                                   "for the real Gap B gate)")
        else:
            self.c.expect_contains(
                text, CODEWORD,
                f"the cold pod recovered the session and answered {CODEWORD} — "
                f"§16 Gap B is fixed: the transcript came from EventBridge and was "
                f"passed to `claude --resume <path>`")
        pods = self.k.pod_names("app.kubernetes.io/name=eventrunner", namespace=self.ns)
        for p in pods:
            log = self.k.logs(p, namespace=self.ns, tail=120)
            if "resuming" in log or "no transcript available" in log:
                for line in log.splitlines():
                    if "resuming" in line or "no transcript available" in line:
                        self.c.log(f"  pod log: {line.strip()[:160]}")

    def _continue(self, prompt: str) -> bool:
        try:
            status, _ = get_json(f"{self.base}/v0/agents/{self.corr}/continue",
                                 method="POST", timeout=25,
                                 body=json.dumps({"prompt": prompt}).encode())
        except (urllib.error.URLError, OSError) as e:
            return self.c.fail("POST /continue accepted", str(e))
        return self.c.expect(status == 202, f"POST /continue -> {status}")

    # ---- T1.10: concurrency across pods ----
    def assert_concurrency(self, n: int) -> None:
        """T1.10's gate: N concurrent correlations run on more than one pod, and
        each `/continue` still resumes ITS OWN session.

        Both halves matter and they pull against each other. Scaling past one pod
        is only correct because Phase 0 keys request messages by `correlationid`:
        all turns for one conversation land on one partition, and Kafka gives each
        partition to exactly one consumer in the group — so the per-correlation
        ordering guarantee the in-process Router provides across threads is
        provided by Kafka across pods, with no coordination. If sessions crossed
        wires here, that reasoning would be wrong.
        """
        self.c.section(f"T1.10 — {n} concurrent correlations across pods")
        so = self.so()
        max_replicas = int(dig(so, "spec", "maxReplicaCount", default=1))
        if max_replicas < 2:
            self.c.warn(f"maxReplicaCount is {max_replicas}; concurrency across pods "
                        f"cannot be demonstrated")
            return
        if self.replicas() != 0:
            self.wait_for_zero("starting the concurrency check from cold")

        codewords = {}
        corrs = []
        for i in range(n):
            word = f"PAR-{1000 + i}"
            prompt = (f"Remember this codeword: {word}. Reply with exactly OK and "
                      f"nothing else. Do not use any tools.")
            try:
                _, body = get_json(f"{self.base}/v0/agents", method="POST", timeout=25,
                                   body=json.dumps({"prompt": prompt,
                                                    "max_turns": 4}).encode())
            except (urllib.error.URLError, OSError) as e:
                self.c.fail(f"posted correlation {i + 1}/{n}", str(e))
                return
            corr = (body or {}).get("correlationid", "")
            corrs.append(corr)
            codewords[corr] = word
        self.c.ok(f"posted {n} independent correlations: {corrs}")

        # Watch the replica count climb while the runs are in flight.
        peak = {"n": 0}
        stop = threading.Event()

        def sample():
            while not stop.is_set():
                peak["n"] = max(peak["n"], self.replicas())
                stop.wait(1.0)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            done = wait_for(lambda: all(self._turn_count(c) >= 1 for c in corrs),
                            timeout=self.args.turn_timeout, interval=3)
        finally:
            stop.set()
            sampler.join(timeout=3)
        self.c.expect(done, f"all {n} first turns completed")
        self.c.expect(peak["n"] >= 2,
                      f"KEDA scaled past one pod while {n} correlations ran "
                      f"(peak replicas={peak['n']})",
                      f"peak was {peak['n']}. Lag must exceed lagThreshold on more "
                      f"than one partition; with {n} correlations hashing to the "
                      f"same partition this can legitimately stay at 1 — re-run, or "
                      f"raise the correlation count.")
        self.c.note("T1.10 peak replicas", peak["n"])

        # Now the part that could silently be wrong: each /continue must resume its
        # own session, not another correlation's.
        for corr in corrs:
            if not self._continue_corr(corr, "What codeword did I give you? Reply "
                                             "with just the codeword. Do not use "
                                             "any tools."):
                return
        second = wait_for(lambda: all(self._turn_count(c) >= 2 for c in corrs),
                          timeout=self.args.turn_timeout, interval=3)
        self.c.expect(second, f"all {n} second turns completed")
        for corr in corrs:
            text = self._reply_for(corr, 2)
            want = codewords[corr]
            others = [w for c, w in codewords.items() if c != corr]
            self.c.log(f"  {corr}: {text.strip()[:70]!r}")
            if self.mock:
                self.c.expect_contains(text, "MOCK-REPLY",
                                       f"{corr} produced a second reply")
            else:
                ok = want in text and not any(o in text for o in others)
                self.c.expect(ok,
                              f"{corr} resumed ITS OWN session ({want}, and no "
                              f"other correlation's codeword)",
                              f"expected {want}, got {text.strip()[:120]!r}")

    def _turn_count(self, corr: str) -> int:
        try:
            _, body = get_json(f"{self.base}/v0/agents/{corr}/turns", timeout=8)
        except (urllib.error.URLError, OSError):
            return 0
        return len([t for t in (body or {}).get("turns") or []
                    if t.get("assistant_text") is not None or t.get("stats")])

    def _reply_for(self, corr: str, turn_index: int) -> str:
        try:
            _, body = get_json(f"{self.base}/v0/agents/{corr}/turns", timeout=8)
        except (urllib.error.URLError, OSError):
            return ""
        for t in (body or {}).get("turns") or []:
            if t.get("turn_index") == turn_index:
                return t.get("assistant_text") or ""
        return ""

    def _continue_corr(self, corr: str, prompt: str) -> bool:
        try:
            status, _ = get_json(f"{self.base}/v0/agents/{corr}/continue",
                                 method="POST", timeout=25,
                                 body=json.dumps({"prompt": prompt}).encode())
        except (urllib.error.URLError, OSError) as e:
            return self.c.fail(f"POST /continue for {corr}", str(e))
        return self.c.expect(status == 202, f"POST /continue for {corr} -> {status}")

    # ---- the signed path (#896) --------------------------------------------
    def assert_signed_wake(self) -> None:
        """D2: the terminal response that ended a run carried a signature that verified.

        The abstract's second demo beat, and until now the only backing for it was a
        unit test. What makes this different from `test_signing.py` is that the
        signature was produced by a runner pod from a Secret-mounted seed and checked by
        a bridge pod against a ConfigMap keyset — the half of the claim no unit test
        reaches, and the half §4.4 step 2's bug lived in.
        """
        self.c.section("signed wake — the terminal response verifies (D2)")
        try:
            raw = urllib.request.urlopen(
                f"{self.base}/v0/agents/{self.corr}/events.jsonl", timeout=15
            ).read().decode()
        except (urllib.error.URLError, OSError) as e:
            return self.c.fail("GET events.jsonl", str(e))
        events = [json.loads(line) for line in raw.splitlines() if line.strip()]
        terminal = [e for e in events
                    if str(e.get("final", "false")).lower() == "true"]
        self.c.expect(bool(terminal), f"a terminal response is stored ({len(events)} events)")
        if not terminal:
            return
        t = terminal[-1]
        self.c.expect(bool(t.get("signature")),
                      "the terminal response carries ce_signature",
                      f"attrs={sorted(k for k in t if not k.startswith('__'))}")
        # The outcome, not the verdict (§8.5.1). A signature that failed verification
        # would have been rewritten to phase=error by the consumer, so a terminal still
        # presenting its own phase is the bridge having accepted it.
        self.c.expect(t.get("phase") == "result",
                      "and was ACCEPTED — stored as phase=result, not rewritten to error",
                      f"phase={t.get('phase')} data={str(t.get('data'))[:160]}")
        self.c.expect(not t.get("signature_rejected")
                      and not (t.get("data") or {}).get("signature_rejected"),
                      "with no rejection marker on the stored row")
        kid = signing.token_kid(t.get("signature") or "") or "(unnamed)"
        self.c.note("terminal signed by kid", kid)
        # §4.4: signing is terminal-only, because a signature costs ~222ms and signing
        # every stdout frame would add minutes to a chatty run. Worth asserting so the
        # slide can state the cost honestly rather than implying every frame is signed.
        signed = [e for e in events if e.get("signature")]
        self.c.note("signed / total events", f"{len(signed)} / {len(events)} "
                                             f"(terminal-only by policy, §4.4)")

    def assert_audit_trail(self) -> None:
        """R4: the broker log read back as an audit trail, after the forgery.

        Runs `audit_topic.py --chains` for the operator-facing view and prints it, then
        asserts the two facts the abstract's "tamper-evident audit" claim rests on: the
        genuine run's result chains back to its *signed* wake event, and the forgery is
        visible rather than indistinguishable.

        Deliberately NOT `--verify`: that asserts a clean topic, and by this point the
        topic contains the forgery the previous stage published. A demo that needed a
        clean topic to pass would be a demo that cannot survive its own attack.
        """
        self.c.section("audit trail — results chain back to signed wakes (R4)")
        import contextlib
        import io

        import audit_topic
        argv = ["--namespace", self.ns, "--chains",
                "--registry", self.args.registry, "--tag", self.args.tag]
        if self.args.kubeconfig:
            argv += ["--kubeconfig", self.args.kubeconfig]
        if self.args.context:
            argv += ["--context", self.args.context]
        # Captured rather than streamed so the stage can assert on what the operator
        # sees, then print it — the output IS the artifact this beat produces.
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                audit_topic.main(argv)
        except SystemExit:
            pass
        out = buf.getvalue()
        print(out.rstrip())
        self.c.expect("BOUND" in out,
                      "at least one result chains back to a verified wake event",
                      out[-400:])
        self.c.expect("terminals" in out, "the summary reports terminal coverage")
        # The forgery from the previous stage must still be visible here. An audit tool
        # that showed a clean topic after an attack would be worse than no tool.
        self.c.expect("signed-BAD" in out or "UNVERIFIED" in out
                      or "ORPHANED" in out or "unsigned" in out,
                      "and the forged event is visible in the audit rather than hidden")

    def assert_forged_event_refused(self) -> None:
        """The money shot: publish a forgery onto the responses topic and watch it fail.

        Runs from a throwaway pod built on the eventrunner image, which already carries
        `shared/` and `kafka-python` — so the forgery is encoded by the SAME codec a
        real attacker would use rather than by a hand-rolled snippet, and it is
        published from inside the cluster, which is the attacker position the threat
        model actually describes (§3.3: the broker is plaintext and anything with
        network access can write to it).

        Asserts the OUTCOME, per §8.5.1 and #885: not that a verdict was computed, but
        that the batch did not settle and the genuine answer survived.
        """
        self.c.section("forged event refused — enforcement acts (#885)")
        # A batch of 2 with only ONE prompt submitted, so the group stays OPEN: a forged
        # `group.completed` has something to end prematurely, which is the whole point.
        try:
            status, body = get_json(
                f"{self.base}/v0/groups", method="POST", timeout=60,
                body=json.dumps({"label": "forgery-target", "expected": 2,
                                 "prompts": ["Say ok and nothing else."],
                                 "max_turns": 1}).encode())
        except (urllib.error.URLError, OSError) as e:
            return self.c.fail("POST /v0/groups for the forgery target", str(e))
        gid = (body or {}).get("groupid", "")
        if not self.c.expect(status == 202 and gid,
                             f"an OPEN group of 2 created with 1 real member -> {gid}",
                             f"status={status} body={body}"):
            return
        self.c.log(f"  {self.base}/v0/groups/{gid}")
        done = wait_for(lambda: (self._group_status(gid) or {}).get("terminal", 0) >= 1,
                        timeout=self.args.turn_timeout, interval=3)
        self.c.expect(done, f"the genuine member finished ({done.elapsed_s:.0f}s), "
                            f"so the batch is open with 1 of 2 terminal")
        st = self._group_status(gid) or {}
        victim = ((st.get("members") or [{}])[0]).get("correlationid", "")
        if not self.c.expect(bool(victim), f"the victim correlation is {victim}"):
            return
        if not self._publish_forgery(gid, victim):
            return
        # Nothing to wait FOR — the assertion is that nothing happened. A few consumer
        # poll intervals is ample for the events to have been read and decided.
        time.sleep(20)
        st = self._group_status(gid) or {}
        self.c.expect(not st.get("completed_utc"),
                      "the forged group.completed did NOT settle the batch",
                      f"completed_utc={st.get('completed_utc')} "
                      f"terminal={st.get('terminal')} failed={st.get('failed')}")
        self.c.expect(st.get("failed", 0) == 0,
                      "and the forged member terminal did NOT fail a healthy member",
                      f"failed={st.get('failed')} finished={st.get('finished')}")
        self.c.expect(st.get("finished", 0) == 1,
                      "the genuine answer still counts as finished",
                      f"finished={st.get('finished')}")
        try:
            raw = urllib.request.urlopen(
                f"{self.base}/v0/agents/{victim}/events.jsonl", timeout=15
            ).read().decode()
            rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
        except (urllib.error.URLError, OSError) as e:
            return self.c.fail("GET the victim's transcript", str(e))
        self.c.expect(not any("Ship the goods" in json.dumps(r) for r in rows),
                      "and the forged text is nowhere in the victim's transcript")
        rejected = [r for r in rows
                    if (r.get("data") or {}).get("signature_rejected")]
        if self.args.forge_collide:
            # The forgery reused a sequence the genuine run holds, so #955's store rule
            # refused it the row entirely. Nothing to find — and that is the stronger
            # outcome, so assert THAT rather than the forensic row.
            self.c.expect(not rejected,
                          "a forgery reusing a verified row's sequence is refused the "
                          "row outright (#955) — no rejection is stored because none "
                          "could be written without destroying the genuine answer")
            self.c.log("  (--forge-collide: the audit evidence is the surviving "
                       "genuine row, not a rejection row)")
        else:
            self.c.expect(bool(rejected),
                          f"the refusal is ON RECORD rather than dropped "
                          f"({len(rejected)} rejected row(s)) — §4.4, a drop is "
                          f"indistinguishable from an agent that never answered")
            if rejected:
                self.c.log(f"  reason: "
                           f"{(rejected[0].get('data') or {}).get('reason')!r}")
        logs = self.k.logs("deploy/eventbridge", namespace=self.ns, tail=200) or ""
        self.c.expect("unverified response" in logs,
                      "and the bridge logged it, so an operator can see the attempt")

    def _group_status(self, gid: str) -> dict | None:
        try:
            _, body = get_json(f"{self.base}/v0/groups/{gid}/status?full=1", timeout=15)
            return body
        except (urllib.error.URLError, OSError):
            return None

    def _publish_forgery(self, gid: str, victim: str) -> bool:
        """Publish two forgeries from a throwaway in-cluster pod. Returns success.

        The eventrunner image is used because it already carries `shared/` and
        `kafka-python`, so the events are encoded by the production codec rather than a
        hand-rolled approximation — the thing that makes this a demo of the real control
        rather than of a snippet. Publishing from inside the cluster is also the
        attacker position §3.3 actually describes: the broker is plaintext and anything
        with network access to it can write.
        """
        cfg = self.k.get("configmap", "eventing-config", namespace=self.ns) or {}
        bootstrap = dig(cfg, "data", "KAFKA_BOOTSTRAP") or \
            "my-cluster-kafka-bootstrap.kafka.svc:9092"
        topic = dig(cfg, "data", "RESPONSE_TOPIC") or "kev1-responses"
        # A sequence the genuine run cannot already hold (next_seq starts at 1), unless
        # --forge-collide asks for the collision case on purpose.
        seq = 1 if self.args.forge_collide else 9001
        forge = FORGE_SRC % {"topic": topic, "gid": gid, "corr": victim,
                             "bootstrap": bootstrap, "seq": seq}
        name = f"forger-{int(time.time())}"
        self.c.log(f"  publishing from a throwaway pod -> {topic} @ {bootstrap}")
        res = self.k.call(
            ["run", name, "--rm", "-i", "--restart=Never", "--quiet",
             "--image", f"{self.args.registry}/rossoctl-eventrunner:{self.args.tag}",
             "--command", "--", "python", "-c", forge],
            namespace=self.ns, timeout=240)
        out = ((res.out or "") + (res.err or "")).strip()
        ok = res.ok and "FORGED 2" in out
        self.c.expect(ok, "a throwaway pod published 2 forged events onto the "
                          "responses topic (in-cluster, production codec)",
                      out[-500:])
        return ok

    # ---- §21: a group of N agents, tracked ----
    def assert_group(self, n: int) -> None:
        """Launch a group of N agents through the EventBridge API and track it.

        This is the batch demo: one POST /v0/groups submits all N together, so lag
        reaches N and KEDA scales out; the group page and /status then show progress in
        user units while it drains. Mock agents sleep 1-5s each
        (ER_MOCK_DELAY_MIN_S/MAX_S) so there is something to watch — without that a
        100-agent batch finishes faster than the page can render.
        """
        self.c.section(f"§21 — a group of {n} agents")
        if self.replicas() != 0:
            self.wait_for_zero("starting the group from cold")

        prompts = [f"Batch item {i + 1}: say the word ok." for i in range(n)]
        try:
            status, body = get_json(f"{self.base}/v0/groups", method="POST",
                                    timeout=120,
                                    body=json.dumps({"label": f"demo-{n}",
                                                     "prompts": prompts,
                                                     "max_turns": 1}).encode())
        except (urllib.error.URLError, OSError) as e:
            self.c.fail(f"POST /v0/groups with {n} prompts", str(e))
            return
        gid = (body or {}).get("groupid", "")
        if not self.c.expect(status == 202 and gid,
                             f"group created with {n} members in ONE call -> {gid}",
                             f"status={status} body={body}"):
            return
        self.c.log(f"  {self.base}/v0/groups/{gid}")

        # Sample replicas while it drains, so the scaling claim is measured.
        peak = {"n": 0}
        stop = threading.Event()

        def sample():
            while not stop.is_set():
                peak["n"] = max(peak["n"], self.replicas())
                stop.wait(1.0)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        seen_partial = {"ok": False}

        def done():
            try:
                _, p = get_json(f"{self.base}/v0/groups/{gid}/status", timeout=15)
            except (urllib.error.URLError, OSError):
                return False
            # Prove the page shows work IN PROGRESS, not just a final state.
            if 0 < (p.get("terminal") or 0) < n:
                seen_partial["ok"] = True
            return bool(p.get("completed_utc"))

        budget = max(180.0, n * 2.0)
        self.c.log(f"tracking to completion (up to {budget:.0f}s)")
        finished = wait_for(done, timeout=budget, interval=3)
        try:
            stop.set()
            sampler.join(timeout=3)
        finally:
            pass

        _, p = get_json(f"{self.base}/v0/groups/{gid}/status", timeout=20)
        self.c.expect(finished, f"the group completed ({finished.elapsed_s:.0f}s)",
                      f"state={p.get('state')} {p.get('terminal')}/{p.get('denominator')}")
        self.c.expect(p.get("terminal") == n,
                      f"all {n} members reached a terminal state",
                      f"terminal={p.get('terminal')} failed={p.get('failed')}")
        self.c.expect(p.get("completion_reason") == "all",
                      "completed for the right reason (all), not a deadline",
                      f"reason={p.get('completion_reason')}")
        self.c.expect(peak["n"] >= 2,
                      f"KEDA scaled past one pod for the batch (peak {peak['n']})",
                      f"peak={peak['n']}; with {n} queued requests lag far exceeds "
                      f"lagThreshold, so this should scale to maxReplicaCount")
        self.c.expect(seen_partial["ok"],
                      "progress was observable mid-flight (not just a final state)",
                      "every poll saw either 0 or all members done — the mock delay "
                      "may be disabled (ER_MOCK_DELAY_MAX_S)")
        self.c.note(f"group of {n}", f"{p.get('finished')} finished, "
                                     f"{p.get('failed')} failed, {p.get('elapsed')}, "
                                     f"peak {peak['n']} replicas")

        # The group page must render and link its members.
        try:
            import urllib.request as _u
            with _u.urlopen(f"{self.base}/v0/groups/{gid}", timeout=20) as r:
                html = r.read().decode()
            self.c.expect(r.status == 200 and "agents finished" in html,
                          "the group page renders with counts in user units")
            self.c.expect("/v0/agents/" in html,
                          "the page links each member to its own agent page")
        except (urllib.error.URLError, OSError) as e:
            self.c.fail("the group page renders", str(e))

    # ---- T5.6 idle-replay guard ----
    def assert_idle_replay_guard(self) -> None:
        """§16 Gap C, exercised by DELETING the group's offsets rather than waiting
        a week for them to expire.

        A group with zero members for longer than the broker's
        `offsets.retention.minutes` (7 days) loses its committed offsets, falls
        back to `auto_offset_reset=earliest`, and replays every request still
        inside the topic's retention — one new task re-running a whole day of agent
        work at real API cost.

        Two details make this demonstrable in a test rather than in a week:

        * deleting the group is equivalent to the offsets expiring, and it makes
          KEDA see the whole retained topic as lag, so a pod wakes with no new
          request needed and no new API spend;
        * `ER_MAX_REQUEST_AGE_S` is temporarily lowered so the requests this flow
          posted minutes ago already count as stale. At its production value (1 h)
          nothing recent is old enough and the check could only ever prove the
          guard by waiting an hour.
        """
        self.c.section("T5.6 — an expired consumer group does not replay the backlog")
        group = f"{self.ns}-eventrunner"
        if not self.c.expect(self.replicas() == 0 or self.wait_for_zero(
                "offsets can only be deleted while the group has no members"),
                "the group has no members, so its offsets can be deleted"):
            return

        broker = self.k.pod_names("strimzi.io/name=my-cluster-kafka", namespace=KAFKA_NS)
        if not broker:
            self.c.warn("could not find the Kafka broker pod — skipping T5.6")
            return
        pod = broker[0]

        # Lower the guard so already-posted requests qualify as stale, and make the
        # change visible to the NEXT pod KEDA starts.
        age = str(int(self.args.replay_max_age))
        restored = False
        if not self.c.expect_run(
                self.k.set_env("deploy/eventrunner", f"ER_MAX_REQUEST_AGE_S={age}",
                               namespace=self.ns),
                f"temporarily lowered ER_MAX_REQUEST_AGE_S to {age}s"):
            return
        try:
            # `--delete --group` removes the group and with it its committed
            # offsets, which is exactly what the broker does after
            # offsets.retention.minutes of no members. (`--delete-offsets` needs an
            # explicit `--topic`; `--all-topics` belongs to `--reset-offsets` and
            # fails with a usage dump.)
            res = self.k.call(["exec", pod, "--", "bin/kafka-consumer-groups.sh",
                               "--bootstrap-server", "localhost:9092",
                               "--delete", "--group", group],
                              namespace=KAFKA_NS, timeout=120)
            if not res.ok and "not empty" in (res.out + res.err).lower():
                self.c.warn("the group still has members — waiting and retrying once")
                time.sleep(20)
                res = self.k.call(["exec", pod, "--", "bin/kafka-consumer-groups.sh",
                                   "--bootstrap-server", "localhost:9092",
                                   "--delete", "--group", group],
                                  namespace=KAFKA_NS, timeout=120)
            if not self.c.expect_run(res, f"deleted the committed offsets for {group} "
                                          f"(equivalent to a week of idleness)"):
                return

            # A new request is what wakes the pod, and that is the scenario §16
            # Gap C actually describes: "one new task triggering a re-run of the
            # whole backlog". Measured: deleting the group alone did NOT make KEDA
            # report lag for the retained messages, so it never scaled up — so the
            # trigger has to be a real post, not just the missing offset.
            try:
                _, body = get_json(f"{self.base}/v0/agents", method="POST", timeout=25,
                                   body=json.dumps({"prompt": "post-expiry probe",
                                                    "max_turns": 1}).encode())
            except (urllib.error.URLError, OSError) as e:
                self.c.fail("posted one request after the offsets expired", str(e))
                return
            self.c.ok(f"posted one fresh request "
                      f"({(body or {}).get('correlationid', '?')}) — the only new work")
            woke = wait_for(lambda: self.k.ready_replicas("eventrunner",
                                                         namespace=self.ns) >= 1,
                            timeout=240, interval=3)
            if not self.c.expect(woke, "a pod woke to handle it, and now faces the "
                                       "whole retained topic (this IS the replay storm)"):
                return

            deadline = time.monotonic() + 180
            dropped = 0
            while time.monotonic() < deadline:
                logs = "\n".join(
                    self.k.logs(p, namespace=self.ns, tail=400)
                    for p in self.k.pod_names("app.kubernetes.io/name=eventrunner",
                                              namespace=self.ns))
                dropped = logs.count("dropping stale request")
                if dropped:
                    for line in logs.splitlines():
                        if "dropping stale request" in line:
                            self.c.log(f"  {line.strip()[:150]}")
                            break
                    break
                time.sleep(5)
            self.c.expect(
                dropped > 0,
                f"the replay guard dropped {dropped} stale request(s) instead of "
                f"re-running them — their offsets were committed, no agent ran",
                "no 'dropping stale request' line appeared. Either every retained "
                "request is younger than the lowered guard (raise "
                "--replay-max-age's strictness by running this later in the flow), "
                "or the guard did not run.")
        finally:
            # Always put the production value back, or the next run silently drops
            # legitimate work.
            restore = self.k.set_env("deploy/eventrunner", "ER_MAX_REQUEST_AGE_S-",
                                     namespace=self.ns)
            restored = restore.ok
            self.c.expect(restored,
                          "ER_MAX_REQUEST_AGE_S restored to the ConfigMap value",
                          restore.tail(4))


def build_args(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--namespace", "-n", default=os.environ.get("NS", "kev1"))
    ap.add_argument("--overlay", default="test",
                    choices=("test", "kind", "kind-signed", "demo"))
    ap.add_argument("--kubeconfig", default=None,
                    help="kubeconfig file (e.g. .kube/config-kind for a Kind cluster)")
    ap.add_argument("--context", default=None)
    ap.add_argument("--yes", action="store_true", default=True)
    ap.add_argument("--registry", default=os.environ.get("REGISTRY_PREFIX", DEFAULT_REGISTRY))
    ap.add_argument("--tag", default=os.environ.get("TAG", DEFAULT_TAG))
    ap.add_argument("--ingress-port", type=int, default=30080,
                    help="host port the ingress controller is reachable on (Kind)")
    ap.add_argument("--public-url", default=None)
    ap.add_argument("--public-url-timeout", type=float, default=120.0)
    ap.add_argument("--idle-timeout", type=float, default=240.0)
    ap.add_argument("--turn-timeout", type=float, default=300.0)
    ap.add_argument("--cooldown-slack", type=float, default=120.0)
    ap.add_argument("--skip-cold-continue", action="store_true",
                    help="skip stage 2c (the Gap B gate) — faster but weaker")
    ap.add_argument("--replay-max-age", type=float, default=60.0,
                    help="ER_MAX_REQUEST_AGE_S to use while proving the T5.6 guard "
                         "(default 60s, restored afterwards)")
    ap.add_argument("--group", type=int, default=0, metavar="N",
                    help="§21: launch a group of N agents and track it to completion "
                         "(0 = skip). 100 is the demo figure.")
    ap.add_argument("--concurrency", type=int, default=0,
                    help="T1.10: run N concurrent correlations and assert they land "
                         "on more than one pod, each resuming its own session "
                         "(0 = skip)")
    ap.add_argument("--skip-idle-replay", action="store_true",
                    help="skip T5.6, which deletes the consumer group's offsets")
    ap.add_argument("--signed-beats", dest="signed_beats", action="store_true",
                    default=None,
                    help="#896: assert the signed wake (D2) and that a forged event is "
                         "refused. Default on for --overlay kind-signed")
    ap.add_argument("--no-signed-beats", dest="signed_beats", action="store_false",
                    help="skip the signed beats even on an enforcing overlay")
    ap.add_argument("--forge-collide", action="store_true",
                    help="publish the forged terminal at sequence 1, colliding with the "
                         "genuine run's first event. #955's store rule then refuses it "
                         "the row outright, so the evidence is the surviving genuine "
                         "answer rather than a stored rejection")
    ap.add_argument("--skip", default="")
    ap.add_argument("--phase", default="pre")
    ap.add_argument("--url", default=None)
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = build_args(argv)
    c = Checks(prefix=PREFIX)
    k = Kubectl(context=args.context, namespace=args.namespace,
                kubeconfig=args.kubeconfig)
    f = Flow(k, c, args)
    c.log(f"context={k.current_context()} namespace={args.namespace} "
          f"overlay={args.overlay} "
          f"({'mock — free and deterministic' if f.mock else 'REAL claude, costs tokens'})")

    f.stage0()
    if not f.base:
        return c.summary()
    f.stage1()
    if not f.corr:
        return c.summary()
    f.stage2()
    f.stage2b()
    f.stage3()
    # #896 — the signed beats, on by default for `kind-signed` because that overlay
    # exists for nothing else. `--signed-beats`/`--no-signed-beats` overrides either way,
    # so the forgery can be demonstrated on any enforcing deployment.
    if f.signed_beats:
        f.assert_signed_wake()
        f.assert_forged_event_refused()
        f.assert_audit_trail()
        f.stage3()
    if not args.skip_cold_continue:
        f.stage2c()
        f.stage3()       # and back to zero once more, after the cold turn
    if args.group:
        f.assert_group(args.group)
        f.stage3()
    if args.concurrency:
        f.assert_concurrency(args.concurrency)
        f.stage3()
    if not args.skip_idle_replay:
        f.assert_idle_replay_guard()
        f.stage3()

    c.note("transcript", f"{f.base}/v0/agents/{f.corr}")
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
