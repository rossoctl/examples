#!/usr/bin/env python3
"""List the events in a topic, say which are signed, and chain results to their wakes.

R4 of the KubeCon demo list — "the event broker log as tamper-evident audit". The
abstract claims a completion can be bound back to the signed wake event that caused it;
this prints that, per event, against a real topic.

  python3 scripts/audit_topic.py                            # both topics, in-cluster
  python3 scripts/audit_topic.py --topic kev1-responses
  python3 scripts/audit_topic.py --chains                   # the binding, not the list
  python3 scripts/audit_topic.py --local --bootstrap localhost:9092 \
      --keyset ~/.eventing-keys/agents.json                 # laptop Kafka

**Where it runs.** By default it runs itself inside a throwaway pod built on the
eventrunner image, because that image already has `shared/`, `kafka-python` and the
keyset ConfigMap mounted — so there is no port-forward, nothing to install, and the
keyset it verifies against is literally the one the deployment uses. `--local` skips that
and talks to a broker directly, for a laptop Kafka.

**What the verdicts mean**, and the three limits the output states rather than implies:

    signed-ok     a signature verified against the approved key set
    signed-BAD    a signature was present and did NOT verify — a forgery, or a key that
                  is no longer approved
    unsigned      no signature on an event that policy says should have one (a terminal,
                  or a group lifecycle event)
    unsigned-ok   no signature on a streamed non-terminal, which is CORRECT: `emit()`
                  signs terminals only, because ~222 ms per signature would add minutes
                  to a chatty run
    no-keyset     nothing to check against; the tool is listing, not auditing

  * A signature proves the signer held an approved key. It does not make the content
    true. What it does bind is `causationid`, which is in `SIGNED_ATTRS`, so the LINK
    between a result and its wake event cannot be rewritten in flight.
  * A topic written before signing was enabled is entirely unsigned, and no signature
    can distinguish that from events an attacker wrote. Both read `unsigned`. That is
    the honest answer, not a gap in the tool.
  * Reading a topic mid-run shows chains in state `open`. That is not a finding either.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from imagelib import DEFAULT_REGISTRY, DEFAULT_TAG  # noqa: E402
from k8slib import Kubectl, dig  # noqa: E402
from proclib import Checks, die  # noqa: E402

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from shared import audit, ce, keyset  # noqa: E402

PREFIX = "audit"

_STATE_NOTE = {
    "bound": "wake and result both verified",
    "result-only": "the result verified; its wake event did not",
    "contested": "a verified answer AND a forgery are both on the topic",
    "unverified": "a terminal exists and did not verify",
    "open": "no terminal yet — still running, or read too early",
    "orphaned": "results naming a wake event that is not on this topic",
    "unknown": "no keyset: nothing was checked",
}


def read_topic(bootstrap: str, topic: str, *, limit: int, timeout_ms: int):
    """Consume a topic from the beginning. Returns [(offset, CloudEvent)].

    No consumer group and no commit: this is a reader, and joining the live group would
    both wait for a rebalance and move the offsets the live consumer owns.
    """
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False,
                      group_id=None, consumer_timeout_ms=timeout_ms)
    try:
        parts = c.partitions_for_topic(topic)
        if not parts:
            return []
        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        c.assign(tps)
        for tp in tps:
            c.seek_to_beginning(tp)
        out = []
        for rec in c:
            try:
                out.append((rec.offset,
                            ce.from_kafka_binary(rec.headers or [], rec.value)))
            except Exception as e:  # noqa: BLE001 - a junk record must not stop the audit
                print(f"[{PREFIX}] offset {rec.offset}: undecodable ({e!r})")
            if len(out) >= limit:
                break
        return out
    finally:
        c.close()


def render_rows(rows: list[audit.Row], title: str) -> str:
    out = [f"\n{title} — {len(rows)} event(s)", "-" * 78]
    if not rows:
        out.append("  (empty)")
        return "\n".join(out)
    for r in rows:
        who = r.correlationid or r.groupid or "-"
        kind = (r.type or "").rsplit(".", 2)[-2] if "." in (r.type or "") else r.type
        flag = "T" if r.is_terminal else " "
        out.append(f"  {r.offset:>6} {flag} {kind:<9} {who:<22} "
                   f"seq={r.sequence or '-':<4} kid={(r.kid or '-'):<11} "
                   f"{r.verdict}")
        if r.verdict in (audit.VERDICT_BAD, audit.VERDICT_UNSIGNED):
            out.append(f"         └─ {r.reason}")
    return "\n".join(out)


def render_chains(cs: list[audit.Chain]) -> str:
    out = ["\nresult → wake binding", "-" * 78]
    if not cs:
        out.append("  (nothing to bind)")
        return "\n".join(out)
    for c in sorted(cs, key=lambda x: (x.state != "bound", x.correlationid)):
        label = (c.request_id[:8] if c.request_id else "(none)")
        out.append(f"  wake {label}  {c.correlationid or '-':<22} "
                   f"{len(c.results):>2} result(s)  {c.state.upper()}")
        out.append(f"         {_STATE_NOTE.get(c.state, '')}")
        t = c.terminal
        if t:
            out.append(f"         terminal seq={t.sequence} kid={t.kid or '-'} "
                       f"{t.verdict}")
    return "\n".join(out)


def audit_locally(args, c: Checks) -> int:
    ks = None
    if args.keyset:
        try:
            ks = keyset.load(args.keyset)
            c.log(f"keyset {args.keyset} — {len(ks)} kid(s): {', '.join(ks.kids)}")
        except Exception as e:  # noqa: BLE001
            die(f"could not load the keyset: {e}", prefix=PREFIX)
    else:
        c.log("no --keyset: listing events without verifying any of them")

    topics = args.topic or [args.request_topic, args.response_topic]
    rows_by_topic: dict[str, list[audit.Row]] = {}
    for topic in topics:
        events = read_topic(args.bootstrap, topic, limit=args.limit,
                            timeout_ms=args.timeout_ms)
        rows_by_topic[topic] = [
            audit.row_for(e, off, ks, bridge_kid=args.bridge_kid or None)
            for off, e in events]
        if not args.chains:
            print(render_rows(rows_by_topic[topic], topic))

    all_rows = [r for rows in rows_by_topic.values() for r in rows]
    reqs = rows_by_topic.get(args.request_topic, [])
    resps = rows_by_topic.get(args.response_topic, [])
    if args.chains or args.verify:
        print(render_chains(audit.chains(reqs, resps)))

    s = audit.summarise(all_rows)
    print("\nsummary", "-" * 70)
    for k in (audit.VERDICT_OK, audit.VERDICT_BAD, audit.VERDICT_UNSIGNED,
              audit.VERDICT_EXEMPT, audit.VERDICT_UNKNOWN):
        if s.get(k):
            print(f"  {k:<13} {s[k]}")
    print(f"  {'terminals':<13} {s['terminals']} "
          f"({s['terminals_verified']} verified)")
    if args.json:
        print("\n" + json.dumps({"summary": s, "chains": [
            {"wake": c.request_id, "correlationid": c.correlationid,
             "state": c.state, "results": len(c.results)}
            for c in audit.chains(reqs, resps)]}, indent=2))

    if not args.verify:
        # Listing mode asserts nothing, and `Checks.summary()` exits non-zero when no
        # assertion ran — correctly, since a harness that asserted nothing has proven
        # nothing. Reading a topic is not a harness, so return 0 directly rather than
        # inventing a pass.
        return 0
    # --verify turns the audit into an assertion, for the demo flow and for CI.
    c.expect(s.get(audit.VERDICT_BAD, 0) == 0,
             "no event on either topic carries a signature that failed to verify",
             f"signed-BAD={s.get(audit.VERDICT_BAD)}")
    c.expect(s.get(audit.VERDICT_UNSIGNED, 0) == 0,
             "every event that policy says must be signed is signed",
             f"unsigned={s.get(audit.VERDICT_UNSIGNED)}")
    c.expect(s["terminals"] > 0 and s["terminals_verified"] == s["terminals"],
             f"every terminal verified ({s['terminals_verified']}/{s['terminals']})")
    bad_chains = [x for x in audit.chains(reqs, resps)
                  if x.state in ("contested", "unverified", "orphaned")]
    c.expect(not bad_chains,
             "every result chains back to a verified wake event",
             "; ".join(f"{x.correlationid or x.request_id[:8]}={x.state}"
                       for x in bad_chains))
    return c.summary()


def audit_in_cluster(args, c: Checks) -> int:
    """Run this same script inside a throwaway pod on the eventrunner image."""
    k = Kubectl(context=args.context, namespace=args.namespace,
                kubeconfig=args.kubeconfig)
    cfg = k.get("configmap", "eventing-config", namespace=args.namespace) or {}
    bootstrap = dig(cfg, "data", "KAFKA_BOOTSTRAP") or args.bootstrap
    req_t = dig(cfg, "data", "REQUEST_TOPIC") or args.request_topic
    res_t = dig(cfg, "data", "RESPONSE_TOPIC") or args.response_topic
    ks_path = dig(cfg, "data", "EB_VERIFY_KEYSET_PATH") or ""
    c.log(f"bootstrap={bootstrap} topics={req_t},{res_t} keyset={ks_path or '(none)'}")
    if not ks_path:
        c.log("  the deployment has no keyset configured, so nothing will be verified "
              "— this is a listing, not an audit (§8.5.2: state the default)")

    src = pathlib.Path(__file__).read_text()
    inner = ["python", "-c",
             "import sys,pathlib;"
             "p=pathlib.Path('/tmp/audit_topic.py');"
             f"p.write_text({src!r});"
             "sys.argv=['audit_topic.py','--local',"
             f"'--bootstrap',{bootstrap!r},"
             f"'--request-topic',{req_t!r},'--response-topic',{res_t!r},"
             + (f"'--keyset',{ks_path!r}," if ks_path else "")
             + (f"'--bridge-kid',{args.bridge_kid!r}," if args.bridge_kid else "")
             + ("'--chains'," if args.chains else "")
             + ("'--verify'," if args.verify else "")
             + ("'--json'," if args.json else "")
             + f"'--limit',{str(args.limit)!r}];"
             "sys.path.insert(0,'/app');sys.path.insert(0,'/tmp');"
             "exec(compile(p.read_text(),'audit_topic.py','exec'),"
             "{'__name__':'__main__','__file__':'/app/scripts/audit_topic.py'})"]
    name = f"audit-{int(time.time())}"
    res = k.call(["run", name, "--rm", "-i", "--restart=Never", "--quiet",
                  "--image", f"{args.registry}/rossoctl-eventrunner:{args.tag}",
                  "--command", "--", *inner],
                 namespace=args.namespace, timeout=args.pod_timeout)
    out = (res.out or "") + (res.err or "")
    print(out.rstrip())
    if not res.ok:
        die("the audit pod did not complete; see its output above", prefix=PREFIX)
    return 0 if "FAIL" not in out else 1


def build_args(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--namespace", "-n", default=os.environ.get("NS", "kev1"))
    ap.add_argument("--kubeconfig", default=None)
    ap.add_argument("--context", default=None)
    ap.add_argument("--local", action="store_true",
                    help="talk to --bootstrap directly instead of via a pod")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", action="append", default=None,
                    help="audit only this topic (repeatable)")
    ap.add_argument("--request-topic", default="kev1-requests")
    ap.add_argument("--response-topic", default="kev1-responses")
    ap.add_argument("--keyset", default=None,
                    help="approved-key set to verify against; without it nothing is "
                         "verified and the tool only lists")
    ap.add_argument("--bridge-kid", default="",
                    help="the only kid accepted on group lifecycle events")
    ap.add_argument("--chains", action="store_true",
                    help="show the result-to-wake binding instead of the event list")
    ap.add_argument("--verify", action="store_true",
                    help="assert the audit holds and exit non-zero if it does not")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--timeout-ms", type=int, default=4000)
    ap.add_argument("--pod-timeout", type=float, default=240.0)
    ap.add_argument("--registry", default=os.environ.get("REGISTRY_PREFIX",
                                                         DEFAULT_REGISTRY))
    ap.add_argument("--tag", default=os.environ.get("TAG", DEFAULT_TAG))
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = build_args(argv)
    c = Checks(prefix=PREFIX)
    if args.local:
        return audit_locally(args, c)
    return audit_in_cluster(args, c)


if __name__ == "__main__":
    raise SystemExit(main())
