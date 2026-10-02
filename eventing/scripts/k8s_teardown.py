#!/usr/bin/env python3
"""Return the demo to zero cost without destroying anything expensive. §17 / T4.4.

Default mode PAUSES rather than deletes. Explicitly preserved: the namespace, the
ScaledObject, the KafkaTopics AND THEIR DATA, and KEDA itself.

  python3 scripts/k8s_teardown.py
  python3 scripts/k8s_teardown.py --namespace kev2
  python3 scripts/k8s_teardown.py --resume        # undo it
  python3 scripts/k8s_teardown.py --purge         # really delete (asks first)

Why `paused-replicas=0` and not the plain `paused: "true"` annotation: verified on
this cluster, `paused-replicas=0` moves the ScaledObject to
`Paused=True reason=ScaledObjectPaused` and holds the target at 0 replicas EVEN
WHILE Active=True — i.e. with real lag on the topic, KEDA still does not scale up.
Plain `paused` freezes the count wherever it happens to be, which is not what
teardown wants.

Two things that look alarming and are correct:
  * a paused ScaledObject still reports Active=True while messages sit in the
    topic;
  * the Route stays resolvable while pointing at zero endpoints, so it serves a
    503 rather than a DNS error.
"""
from __future__ import annotations

import argparse
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from k8slib import Kubectl, condition_is, condition_reason, wait_for  # noqa: E402
from proclib import Checks  # noqa: E402

PREFIX = "teardown"
KAFKA_NS = "kafka"
PAUSE_ANNOTATION = "autoscaling.keda.sh/paused-replicas"

PRESERVED = """\
deliberately NOT done, and why:
  delete namespace          destroys the Route hostname, any PVC and its history;
                            the namespace is the stable home for this demo
  delete the KafkaTopic CRs deleting the CR deletes the REAL TOPIC and all its
                            data. Recreating is cheap; losing an audit trail
                            mid-investigation is not
  delete the ScaledObject   pausing achieves the same idle cost and keeps the
                            tested configuration in place
  touch KEDA or Strimzi     cluster-shared operators; other namespaces depend on
                            them
  delete Secrets            left so resuming needs no credential re-entry. Remove
                            them by hand if the cluster is shared\
"""


def pause(k: Kubectl, c: Checks, ns: str) -> None:
    c.section(f"pausing the demo in ns {ns}")
    res = k.annotate("scaledobject", "eventrunner", f"{PAUSE_ANNOTATION}=0", namespace=ns)
    c.expect_run(res, f"ScaledObject pinned at zero via {PAUSE_ANNOTATION}=0")

    for target in ("deploy/eventrunner", "deploy/eventbridge"):
        c.expect_run(k.scale(target, 0, namespace=ns), f"{target} scaled to 0")

    paused = wait_for(
        lambda: condition_is(k.get("scaledobject", "eventrunner",
                                   namespace=ns, missing_ok=True) or {}, "Paused"),
        timeout=90, interval=3)
    so = k.get("scaledobject", "eventrunner", namespace=ns, missing_ok=True) or {}
    c.expect(paused, "ScaledObject reports Paused=True",
             condition_reason(so, "Paused"))
    if condition_is(so, "Active"):
        c.log("  note: Active=True while paused is CORRECT — there is lag on the "
              "topic and KEDA is deliberately ignoring it")

    for deploy in ("eventrunner", "eventbridge"):
        at_zero = wait_for(lambda d=deploy: k.replicas(d, namespace=ns) == 0,
                           timeout=120, interval=3)
        c.expect(at_zero, f"{deploy} is at 0 replicas",
                 f"replicas={k.replicas(deploy, namespace=ns)}")

    # What must still be here afterwards — T4.4's gate.
    c.expect(k.get("namespace", ns, namespace=False, missing_ok=True) is not None,
             f"namespace {ns} still exists")
    for topic in (f"{ns}-requests", f"{ns}-responses"):
        c.expect(k.get("kafkatopic", topic, namespace=KAFKA_NS, missing_ok=True) is not None,
                 f"KafkaTopic {topic} still exists (its data is preserved)")
    c.expect(k.get("scaledobject", "eventrunner", namespace=ns, missing_ok=True) is not None,
             "the ScaledObject still exists (paused, not deleted)")
    c.log("")
    for line in PRESERVED.splitlines():
        c.log(line)


def resume(k: Kubectl, c: Checks, ns: str) -> None:
    c.section(f"resuming the demo in ns {ns}")
    res = k.annotate("scaledobject", "eventrunner",
                     f"{PAUSE_ANNOTATION}-", "autoscaling.keda.sh/paused-",
                     namespace=ns)
    c.expect_run(res, "pause annotations removed")
    c.expect_run(k.scale("deploy/eventbridge", 1, namespace=ns),
                 "eventbridge scaled back to 1")
    # EventRunner is deliberately NOT scaled by hand: KEDA owns it, and it should
    # come back at 0 and wake on the next request. Scaling it here would fight the
    # ScaledObject and mask a broken trigger.
    ready = wait_for(
        lambda: condition_is(k.get("scaledobject", "eventrunner",
                                   namespace=ns, missing_ok=True) or {}, "Ready"),
        timeout=120, interval=3)
    so = k.get("scaledobject", "eventrunner", namespace=ns, missing_ok=True) or {}
    c.expect(ready, "ScaledObject is Ready again", condition_reason(so, "Ready"))
    c.expect(not condition_is(so, "Paused"), "ScaledObject is no longer paused",
             condition_reason(so, "Paused"))
    c.log("  eventrunner is left to KEDA — it should stay at 0 and wake on the "
          "next POST")


def purge(k: Kubectl, c: Checks, ns: str, *, assume_yes: bool) -> None:
    """A genuinely complete removal — for abandoning the demo, not idling it.

    Prints exactly what it is about to destroy, because one of those things is a
    Kafka topic and its entire history.
    """
    doomed = [
        f"ns/{ns}: deploy/eventbridge, deploy/eventrunner, svc/eventbridge, "
        f"route/eventbridge, scaledobject/eventrunner, configmap/eventing-config",
        f"ns/{ns}: secret/anthropic-credentials (if present)",
        # eventbridge-ntfy was listed nowhere and so survived --purge, which means a
        # "purged" namespace kept a capability that lets anyone who learns it read and
        # publish your notifications.
        f"ns/{ns}: secret/eventbridge-ntfy (if present)",
        # §11 — the private keys. Leaving these behind means a later deploy silently
        # inherits an identity the operator thought they had destroyed.
        f"ns/{ns}: secret/eventbridge-signing-key, secret/eventrunner-signing-key "
        f"(PRIVATE KEYS, if present)",
        f"ns/{ns}: configmap/eventing-keyset (the approved-agent list, if present)",
        f"ns/{ns}: pvc/eventbridge-data AND ITS CONTENTS (if present)",
        f"ns/{KAFKA_NS}: kafkatopic/{ns}-requests AND ALL ITS MESSAGES",
        f"ns/{KAFKA_NS}: kafkatopic/{ns}-responses AND ALL ITS MESSAGES",
    ]
    c.section("--purge: about to DESTROY")
    for line in doomed:
        c.log(f"  {line}")
    c.log(f"  (the namespace {ns} itself is kept)")
    if not assume_yes:
        try:
            answer = input(f"[{PREFIX}] type the namespace name to confirm: ").strip()
        except EOFError:
            answer = ""
        if answer != ns:
            c.fail("purge confirmed", f"got {answer!r}, expected {ns!r} — nothing deleted")
            return
    for kind, name in (("scaledobject", "eventrunner"),
                       ("deploy", "eventrunner"), ("deploy", "eventbridge"),
                       ("svc", "eventbridge"), ("route", "eventbridge"),
                       ("configmap", "eventing-config"),
                       ("configmap", "eventing-keyset"),
                       ("secret", "anthropic-credentials"),
                       ("secret", "eventbridge-ntfy"),
                       ("secret", "eventbridge-signing-key"),
                       ("secret", "eventrunner-signing-key"),
                       ("pvc", "eventbridge-data")):
        c.expect_run(k.delete(kind, name, namespace=ns), f"deleted {kind}/{name} from {ns}")
    for topic in (f"{ns}-requests", f"{ns}-responses"):
        c.expect_run(k.delete("kafkatopic", topic, namespace=KAFKA_NS),
                     f"deleted kafkatopic/{topic} (and its data)")
    # The digest stamp must go too, or a later deploy thinks it is up to date.
    k.annotate("namespace", ns, "rossoctl.dev/manifest-sha256-", namespace=False)
    c.log("  cleared the manifest digest annotation so a redeploy is detected")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--namespace", "-n", default=os.environ.get("NS", "kev1"))
    ap.add_argument("--kubeconfig", default=None,
                    help="kubeconfig file (e.g. .kube/config-kind for a Kind cluster)")
    ap.add_argument("--context", default=None)
    ap.add_argument("--resume", action="store_true", help="undo a teardown")
    ap.add_argument("--purge", action="store_true",
                    help="DELETE the app objects and both KafkaTopics (asks first)")
    ap.add_argument("--yes", action="store_true", help="skip the --purge confirmation")
    args = ap.parse_args(argv)

    c = Checks(prefix=PREFIX)
    k = Kubectl(context=args.context, namespace=args.namespace,
                kubeconfig=args.kubeconfig)
    if args.purge:
        purge(k, c, args.namespace, assume_yes=args.yes)
    elif args.resume:
        resume(k, c, args.namespace)
    else:
        pause(k, c, args.namespace)
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
