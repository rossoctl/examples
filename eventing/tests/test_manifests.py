"""Manifest invariants. Gates T3.2 and T3.4 (DESIGN_PHASE1.md §20).

Two levels, because of a constraint worth stating: §1.1 bans `pyyaml` (it ships a
C extension), so there is no YAML parser available here, and
`kubectl apply -o json` needs API discovery — it is not offline. So:

  * offline checks use `kubectl kustomize` (which needs no cluster) and scope
    their assertions by document and by indentation;
  * structured checks ask the API server to render fully-defaulted JSON, which is
    the better fixture — it is what the cluster will actually store — and skip
    when no cluster is reachable.
"""
import json
import pathlib
import re
import shutil
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
K8S = ROOT / "k8s"
# `kind-signed` (#884) is included deliberately: it was absent when that PR added the
# overlay, so every assertion in this file silently skipped the one overlay that turns
# signing on — including the seed-path rule the PR exists to respect.
OVERLAYS = {"test": K8S / "overlays" / "test",
            "kind": K8S / "overlays" / "kind",
            "demo": K8S / "overlays" / "demo",
            "kind-signed": K8S / "overlays" / "kind-signed"}

_HAVE_KUBECTL = shutil.which("kubectl") is not None


def _run(argv, timeout=120):
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)


def _cluster_reachable() -> bool:
    if not _HAVE_KUBECTL:
        return False
    try:
        return _run(["kubectl", "version", "-o", "json"], timeout=30).returncode == 0
    except Exception:
        return False


_HAVE_CLUSTER = _cluster_reachable()
needs_kubectl = pytest.mark.skipif(not _HAVE_KUBECTL, reason="kubectl not on PATH")
needs_cluster = pytest.mark.skipif(not _HAVE_CLUSTER, reason="no reachable cluster")


# ---- offline: rendered YAML, scoped by document -----------------------------

def render(overlay: str) -> str:
    r = _run(["kubectl", "kustomize", str(OVERLAYS[overlay])])
    assert r.returncode == 0, f"kustomize failed:\n{r.stderr}"
    return r.stdout


def documents(rendered: str) -> list[str]:
    return [d for d in re.split(r"(?m)^---\s*$", rendered) if d.strip()]


def find_doc(rendered: str, kind: str, name: str) -> str:
    for doc in documents(rendered):
        if re.search(rf"(?m)^kind:\s*{kind}\s*$", doc) and \
           re.search(rf"(?m)^\s+name:\s*{name}\s*$", doc):
            return doc
    raise AssertionError(f"no {kind}/{name} in the rendered overlay")


@needs_kubectl
def test_both_overlays_render():
    for overlay in OVERLAYS:
        assert render(overlay).strip(), f"{overlay} rendered empty"


@needs_kubectl
def test_the_eventrunner_deployment_declares_no_spec_replicas():
    """T3.2's gate, and the §14.1 prerequisite.

    KEDA owns spec.replicas. If the manifest pins it, `kubectl diff -k` reports a
    difference the moment KEDA scales — so "has anything changed?" answers yes on
    every run and change detection becomes useless.
    """
    for overlay in OVERLAYS:
        doc = find_doc(render(overlay), "Deployment", "eventrunner")
        # spec: sits at column 0 in kustomize output, so a spec-level replicas is
        # exactly two spaces of indent. A replicas deeper than that would belong
        # to something else.
        assert not re.search(r"(?m)^  replicas:", doc), \
            f"{overlay} overlay pins spec.replicas on the KEDA-managed Deployment"


@needs_kubectl
def test_the_eventbridge_deployment_does_pin_one_replica():
    """The opposite requirement: single replica by constraint (§8.6), because the
    SQLite store cannot be shared."""
    for overlay in OVERLAYS:
        doc = find_doc(render(overlay), "Deployment", "eventbridge")
        assert re.search(r"(?m)^  replicas: 1$", doc), f"{overlay}: expected replicas: 1"
        assert "Recreate" in doc, "Recreate avoids two pods sharing the volume mid-rollout"


@needs_kubectl
def test_the_demo_overlay_puts_home_on_the_volume():
    """T3.4's gate (§16 Gap B). claude keeps session transcripts under
    $HOME/.claude; if HOME is on the container's ephemeral layer a restart loses
    every resumable session."""
    doc = find_doc(render("demo"), "Deployment", "eventrunner")
    assert re.search(r"name: HOME\s*\n\s*value: /data/home", doc), \
        "the demo overlay must move HOME onto the mounted volume"
    assert re.search(r"name: CLAUDE_CONFIG_DIR\s*\n\s*value: /data/home/\.claude", doc)


@needs_kubectl
def test_the_test_overlay_mounts_no_credential_so_mock_mode_is_automatic():
    """What makes the e2e test free and deterministic is that NOTHING can inject an
    **Anthropic credential** into these pods, so config.py's auto-detection selects mock
    mode itself. §14 assertion 7 then checks the pod log says `auto:` — which also proves
    the detection path works in-cluster rather than bypassing it with an override.

    **The assertion is about credentials, not about Secrets in general**, and that
    distinction is now load-bearing. The base mounts the per-service *signing* Secrets as
    `optional: true` volumes, so `secretName:` does appear in this overlay — a bare
    `"secretRef" not in doc` kept passing only because a volume uses the other spelling,
    which would have made this test green for a reason it does not document. It is
    checked by name here: any Secret reference other than the two signing ones has to be
    justified, and an `ANTHROPIC`-shaped one fails outright.
    """
    doc = find_doc(render("test"), "Deployment", "eventrunner")
    # `secretName` only — the volume is also called `signing-key`, which is a volume
    # name rather than a Secret reference and must not be mistaken for one.
    allowed_secrets = {"eventbridge-signing-key", "eventrunner-signing-key"}
    referenced = set(re.findall(r"secretName:\s*(\S+)", doc))
    assert referenced <= allowed_secrets, \
        f"unexpected Secret reference: {sorted(referenced - allowed_secrets)}"
    # No credential Secret, by either spelling, and nothing credential-shaped.
    assert "secretRef" not in doc, "the test overlay must mount no credential"
    assert "anthropic" not in doc.lower(), \
        "the test overlay must reference no Anthropic credential Secret"
    assert "ANTHROPIC" not in doc
    assert "ER_MOCK_CLAUDE" not in doc, \
        "leave the mode to auto-detection so the e2e exercises it"


@needs_kubectl
def test_no_credential_is_ever_rendered_into_a_manifest():
    """The overlays reference a Secret; they must never contain one.

    A kustomize secretGenerator would inline the value into `kubectl kustomize`
    output — which the §14.1 digest hashes and which people paste into terminals.
    """
    for overlay in OVERLAYS:
        rendered = render(overlay)
        assert "kind: Secret" not in rendered, \
            f"{overlay} renders a Secret — credentials must be applied separately"
        for marker in ("sk-ant-", "sk-", "ANTHROPIC_AUTH_TOKEN:", "ANTHROPIC_API_KEY:"):
            assert marker not in rendered, f"{overlay} may leak a credential ({marker!r})"


#: Phase 3 §8.3, continuing Phase 2 §5's rule. Every variable naming a path that
#: contains key material must come from a **Secret** mount, never from the committed
#: `config.toml` and never with a literal value in a ConfigMap. The rule is only worth
#: having if a test enforces it, which is T22.
#:
#: NOTE: none of these are read by `config.py` yet — they arrive with T8 (transcript
#: auth), T9 (capability keys), T10 (derived ntfy topics) and T16 (webhook ingress). They
#: are listed now on purpose, so the guard is already in place when the variable lands
#: rather than being remembered afterwards. `test_every_phase3_variable_is_classified`
#: deliberately does NOT require these to be read, which is why it only asserts the
#: ConfigMap side against live code.
SECRET_VALUE_VARS = (
    "EB_NTFY_TOPIC_SECRET_PATH",
    "EB_CAPABILITY_SECRET_PATH",
    "EB_WEBHOOK_SECRETS_PATH",
)

#: The signing seed paths, from #884. Same §5 rule, different obligation — and the
#: distinction is the one that PR had to settle: these hold a **mount point**, not key
#: material, so a literal value is correct. What they must not do is come from the
#: committed ConfigMap: `configmap.yaml` says seed paths "name Secret mounts and belong
#: in the Deployment beside the volume", and #884 moved them to a container `env` entry
#: for exactly that reason after an earlier revision put them in `eventing-config` and
#: quoted the rule immediately before breaking it.
SEED_PATH_VARS = (
    "ER_SIGNING_KEY_PATH",
    "EB_SIGNING_KEY_PATH",
)

#: Both groups obey "never from `config.toml`", which is what
#: `test_secret_path_variables_are_never_set_from_the_committed_config` checks.
SECRET_PATH_VARS = SECRET_VALUE_VARS + SEED_PATH_VARS

#: The other side of the same rule: these record what an operator approved and grant
#: nothing, so a ConfigMap (or an image path) is correct and a Secret would be cargo
#: cult. Listed explicitly so the next variable added lands on one side deliberately.
CONFIGMAP_VARS = (
    "EB_TENANCY_MODE",
    "EB_TOPIC_PREFIX",
    "EB_USER_REGISTRY_PATH",
    "EB_MAX_OPEN_STORES",
    "ER_AGENT_DIR",
    "ER_AGENT_NAME",
    "ER_USERKEY",
)


def test_secret_path_variables_are_never_set_from_the_committed_config():
    """A seed path in `config.toml` means the whole signing/notification block can be
    half-configured from a file in git."""
    toml = (ROOT / "eventbridge" / "config.toml").read_text()
    for var in SECRET_PATH_VARS:
        assert var not in toml, f"{var} must be env-only (DESIGN_PHASE3.md §8.3)"
        # The TOML spelling of the same thing — a lower-cased key without the prefix.
        key = var.removeprefix("EB_").lower()
        assert key not in toml, f"{key} in config.toml reintroduces {var} by another name"


@needs_kubectl
def test_no_phase3_secret_is_inlined_into_a_manifest():
    """The §8.3 rule, checked where it would actually leak: rendered output.

    **A path naming a secret is not a secret.** `*_SIGNING_KEY_PATH` holds
    `/etc/ce-signing/seed.hex` — a mount point, not key material — and #884 deliberately
    sets it as a container `env` entry so it sits *beside the volume that supplies the
    seed*, which is what `configmap.yaml` asks for. So a literal value is correct for
    those, and the rule they must obey is a different one: the **seed itself** must never
    appear, and the path must not be set from the committed ConfigMap.

    The variables whose *value* is key material — none of them read yet, see the note on
    SECRET_PATH_VARS — must have no literal value anywhere.
    """
    for overlay in OVERLAYS:
        rendered = render(overlay)
        for var in SECRET_VALUE_VARS:
            # Referencing the variable is fine; giving it a literal sibling value is
            # not. A `secretKeyRef` has no `value:` on the same key.
            #
            # `\s*` rather than hardcoded indentation: ten spaces matches the demo
            # overlay's container `env:` today, but the same variable inlined in an
            # initContainer, a sidecar or a deeper patch would pass silently.
            # test_manifests.py's own earlier tests use `\s*` for this reason.
            inlined = re.search(rf"name:\s*{re.escape(var)}\s*\n\s*value:", rendered)
            assert f"{var}:" not in rendered and not inlined, \
                f"{overlay} appears to inline {var} rather than mounting a Secret"
        # The seed-path variables: a literal value is fine, a ConfigMap key is not.
        # `f"{var}:"` matches the ConfigMap `data:` spelling (`VAR: value`) but not the
        # env-entry spelling (`- name: VAR` / `value: …`), which is the distinction.
        for var in SEED_PATH_VARS:
            assert f"{var}:" not in rendered, \
                (f"{overlay} sets {var} as a ConfigMap key; `configmap.yaml` says seed "
                 f"paths name Secret mounts and belong in the Deployment beside the "
                 f"volume (see #884)")
    # And no seed material, by any route, in any overlay.
    for overlay in OVERLAYS:
        rendered = render(overlay)
        assert "seed.hex:" not in rendered, f"{overlay} may inline a seed"


#: Variables that pre-date §8.3 and are therefore out of its scope. Retro-classifying all
#: 36 is a separate change from making the rule hold for anything new, so they are listed
#: here explicitly — and `test_the_baseline_list_is_still_accurate` keeps the list honest
#: by checking it against the config files rather than trusting it.
PRE_PHASE3_VARS = (
    "EB_ALLOWED_USERS", "EB_AUTH_TOKENS", "EB_GITHUB_CACHE_TTL_S",
    "EB_GITHUB_CLIENT_ID", "EB_GROUP_DEADLINE_S", "EB_HTTP_ADDR",
    "EB_REQUIRE_RESPONSE_SIGNATURE", "EB_SIGNING_KEY_PATH", "EB_SIGNING_KID",
    "EB_TRANSCRIPT_MAX_BYTES", "EB_VERIFY_KEYSET_PATH", "EB_WORKERS",
    "ER_COMMIT_AFTER_TERMINAL", "ER_CONSUMER_GROUP", "ER_DEDUPE_FINAL_TEXT",
    "ER_DRAIN_TIMEOUT_S", "ER_EMIT_SYSTEM_HOOKS", "ER_EVENTBRIDGE_URL",
    "ER_FINAL_COMMIT_WAIT_S", "ER_HEARTBEAT_MAX_AGE_S", "ER_HEARTBEAT_PATH",
    "ER_INCLUDE_RAW", "ER_KAFKA_RETRY_INITIAL_S", "ER_KAFKA_RETRY_MAX_S",
    "ER_MAX_CONCURRENT", "ER_MAX_REQUEST_AGE_S", "ER_MOCK_CLAUDE",
    "ER_MOCK_DELAY_MAX_S", "ER_MOCK_DELAY_MIN_S", "ER_REQUIRE_SIGNATURE",
    "ER_SIGNING_KEY_PATH", "ER_SIGNING_KID", "ER_TRANSCRIPT_MAX_BYTES",
    "ER_TRANSCRIPT_SYNC", "ER_VERIFY_KEYSET_PATH", "ER_VERIFY_KEY_PATH",
)


def _vars_read(path: pathlib.Path) -> set[str]:
    """Every `EB_*`/`ER_*` name a config file reads via the `e("VAR")` idiom."""
    return set(re.findall(r'e\("((?:EB|ER)_[A-Z0-9_]+)"', path.read_text()))


def test_the_baseline_list_is_still_accurate():
    """A stale `PRE_PHASE3_VARS` entry silently widens the exemption above.

    If a pre-Phase-3 variable is removed from `config.py` and left here, it keeps
    exempting a name nobody reads — harmless — but if one is *renamed*, the new name
    becomes unclassified and the check above catches it. This asserts the simpler
    property: every name here is still read somewhere.
    """
    found = (_vars_read(ROOT / "eventbridge" / "config.py")
             | _vars_read(ROOT / "eventrunner" / "config.py"))
    stale = set(PRE_PHASE3_VARS) - found
    assert not stale, (
        f"{sorted(stale)} are in PRE_PHASE3_VARS but no config.py reads them; "
        f"remove them so the §8.3 exemption stays as narrow as it should be")


def test_every_phase3_variable_is_classified():
    """A variable in neither list is one nobody answered the §8.3 question about.

    **Both directions matter, and only one of them enforces the rule.**
    `CONFIGMAP_VARS ⊆ found` catches a stale entry here. The direction that catches a NEW
    variable classified as neither is `found ⊆ CONFIGMAP_VARS ∪ SECRET_PATH_VARS` — which
    is the case this test exists for, and which an earlier version of it did not check.
    """
    eb = ROOT / "eventbridge" / "config.py"
    er = ROOT / "eventrunner" / "config.py"
    found = _vars_read(eb) | _vars_read(er)

    # The sanity check that this test is looking at the right thing at all. If the
    # `e("VAR")` idiom ever changes, `found` goes empty and every assertion below passes
    # vacuously — so fail loudly here instead.
    assert "EB_TENANCY_MODE" in found and len(found) > 10, \
        'config.py does not look like it was parsed; did the `e("VAR")` idiom change?'

    # Every ConfigMap-side variable must actually be read by the code — a stale entry
    # here would silently stop checking anything. The Secret-side ones are deliberately
    # exempt: they are declared ahead of the tasks that introduce them (see the note on
    # SECRET_PATH_VARS), and requiring them now would just force a placeholder read.
    for var in CONFIGMAP_VARS:
        assert var in found, f"{var} is listed here but no config.py reads it"

    # THE DIRECTION THAT ENFORCES §8.3. Scoped to variables Phase 3 introduced: the
    # pre-Phase-3 ones pre-date the rule, and retro-classifying 36 of them is a separate
    # change from making the rule hold for new ones.
    baseline = set(PRE_PHASE3_VARS)
    unclassified = found - set(CONFIGMAP_VARS) - set(SECRET_PATH_VARS) - baseline
    assert not unclassified, (
        f"{sorted(unclassified)} are read by config.py but classified as neither "
        f"ConfigMap nor Secret. DESIGN_PHASE3.md §8.3 requires the question be answered: "
        f"add each to CONFIGMAP_VARS or SECRET_PATH_VARS in this file.")


def test_the_classification_check_can_actually_fail():
    """The deliberate-failure check the review asked for.

    A test whose failure mode has never been observed is a test nobody should trust —
    `test_every_phase3_variable_is_classified` replaced an assertion that could not fail
    for its stated reason, so this one proves the replacement does.
    """
    baseline = set(PRE_PHASE3_VARS)
    pretend_found = {"EB_TENANCY_MODE", "EB_SOMETHING_NEW_X"} | baseline
    unclassified = pretend_found - set(CONFIGMAP_VARS) - set(SECRET_PATH_VARS) - baseline
    assert unclassified == {"EB_SOMETHING_NEW_X"}, \
        "an unclassified new variable must be detected"


@needs_kubectl
def test_the_demo_overlay_requires_the_credential_secret():
    doc = find_doc(render("demo"), "Deployment", "eventrunner")
    assert "anthropic-credentials" in doc
    assert re.search(r"optional: false", doc), \
        "a demo with no credential would accept requests and fail every one"


@needs_kubectl
def test_the_consumer_group_matches_the_scaledobject_trigger():
    """A mismatch here is a silent no-scale failure: KEDA measures the lag of a
    group nobody joins, so it never sees work and never scales."""
    for overlay in OVERLAYS:
        rendered = render(overlay)
        cm = find_doc(rendered, "ConfigMap", "eventing-config")
        so = find_doc(rendered, "ScaledObject", "eventrunner")
        cm_group = re.search(r"ER_CONSUMER_GROUP:\s*(\S+)", cm).group(1)
        so_group = re.search(r"consumerGroup:\s*(\S+)", so).group(1)
        assert cm_group == so_group, f"{overlay}: {cm_group!r} != {so_group!r}"


@needs_kubectl
def test_the_scaledobject_topic_matches_the_request_topic():
    for overlay in OVERLAYS:
        rendered = render(overlay)
        cm = find_doc(rendered, "ConfigMap", "eventing-config")
        so = find_doc(rendered, "ScaledObject", "eventrunner")
        assert re.search(r"REQUEST_TOPIC:\s*(\S+)", cm).group(1) == \
               re.search(r"(?m)^\s+topic:\s*(\S+)", so).group(1)


@needs_kubectl
def test_scale_to_zero_is_configured():
    for overlay in OVERLAYS:
        so = find_doc(render(overlay), "ScaledObject", "eventrunner")
        assert re.search(r"minReplicaCount: 0", so), "an idle demo must cost nothing"
        assert re.search(r"activationLagThreshold: \"0\"", so), "0 -> 1 on any lag"
        assert re.search(r"offsetResetPolicy: earliest", so), \
            "must match the application's auto_offset_reset (§7.1)"


@needs_kubectl
def test_max_replicas_never_exceeds_the_partition_count():
    """Kafka gives each partition to exactly one consumer in a group, so pods
    beyond the partition count idle — and the per-correlation ordering guarantee
    depends on that 1:1 mapping."""
    topics = (K8S / "topics" / "kafkatopics.yaml").read_text()
    partitions = min(int(m) for m in re.findall(r"(?m)^\s+partitions:\s*(\d+)", topics))
    for overlay in OVERLAYS:
        so = find_doc(render(overlay), "ScaledObject", "eventrunner")
        mx = int(re.search(r"maxReplicaCount:\s*(\d+)", so).group(1))
        assert mx <= partitions, f"{overlay}: maxReplicaCount {mx} > {partitions} partitions"


@needs_kubectl
def test_the_demo_overlays_shorten_the_cooldown_so_scale_to_zero_is_quick():
    """5s, because the cooldown is the only part of the loop that was ever slow.

    Measured on ykt1 with cooldownPeriod 30: POST -> scale-up 1.7s, pod Ready 13.7s,
    back to zero 46.5s. KEDA's detection was never the bottleneck (pollingInterval is
    5s); the 30s was an idle pod lingering. Safe to shorten because RQ-1 keeps lag
    >=1 for the whole run, so KEDA cannot scale away a pod that is still working.
    """
    for overlay in ("test", "kind"):
        so = find_doc(render(overlay), "ScaledObject", "eventrunner")
        assert re.search(r"cooldownPeriod: 5\b", so), f"{overlay} should use 5"
    # The demo overlay keeps the base's 300: a real conversation usually continues,
    # and a warm pod skips the cold start on every /continue.
    demo_so = find_doc(render("demo"), "ScaledObject", "eventrunner")
    assert re.search(r"cooldownPeriod: 300", demo_so)


@needs_kubectl
def test_polling_interval_is_short_in_every_overlay():
    """KEDA's default is 30s, which would make the wake itself feel slow. 5s is what
    made POST -> scale-up measure 1.7s."""
    for overlay in OVERLAYS:
        so = find_doc(render(overlay), "ScaledObject", "eventrunner")
        assert re.search(r"pollingInterval: 5\b", so), f"{overlay}: expected 5"


@needs_kubectl
def test_only_one_overlay_declares_an_externally_reachable_object():
    """The two environments differ here and only here (§3.2): OpenShift has the
    Route API, Kind has ingress-nginx. The kind overlay must therefore DELETE the
    inherited Route — leaving it in makes every apply fail with
    `no matches for kind "Route" in version "route.openshift.io/v1"`."""
    kind_rendered = render("kind")
    assert "route.openshift.io" not in kind_rendered, \
        "the kind overlay must delete the inherited Route"
    assert "kind: Ingress" in kind_rendered
    assert "eventbridge.127.0.0.1.nip.io" in kind_rendered, \
        "nip.io avoids needing /etc/hosts edits or wildcard DNS"
    for overlay in ("test", "demo"):
        assert "route.openshift.io" in render(overlay), f"{overlay} should keep the Route"
        assert "kind: Ingress" not in render(overlay)


@needs_kubectl
def test_the_kind_ingress_does_not_buffer_sse():
    """EventBridge streams Server-Sent Events; nginx buffers proxied responses by
    default, which would hold frames until the buffer filled and make the stream
    look hung."""
    ing = find_doc(render("kind"), "Ingress", "eventbridge")
    assert "proxy-buffering: \"off\"" in ing


def test_the_kind_cluster_config_enables_ingress():
    """Both halves are required: ingress-nginx's kind variant needs the node label
    to schedule AND the host port mappings to be reachable. A cluster created by a
    plain `kind create cluster` cannot serve an Ingress at all."""
    cfg = (K8S / "kind" / "kind-cluster.yaml").read_text()
    assert "ingress-ready=true" in cfg
    assert "extraPortMappings" in cfg
    assert "hostPort: 30080" in cfg


def test_the_kind_kafka_matches_the_reference_bootstrap_address():
    """What lets base/configmap.yaml be identical across both environments."""
    cfg = (K8S / "kind" / "kafka-cluster.yaml").read_text()
    assert "name: my-cluster" in cfg and "namespace: kafka" in cfg
    cm = (K8S / "base" / "configmap.yaml").read_text()
    assert "my-cluster-kafka-bootstrap.kafka.svc:9092" in cm


def test_the_kind_kafka_is_single_broker_consistent():
    """All replication factors must be 1 on one broker, or topics stay
    Ready=False forever waiting for replicas that cannot exist."""
    cfg = (K8S / "kind" / "kafka-cluster.yaml").read_text()
    for key in ("default.replication.factor", "offsets.topic.replication.factor",
                "transaction.state.log.replication.factor",
                "transaction.state.log.min.isr", "min.insync.replicas"):
        assert re.search(rf"{re.escape(key)}: 1\b", cfg), f"{key} must be 1"
    assert re.search(r"replicas: 1", cfg)


@needs_kubectl
def test_the_drain_timeout_fits_inside_the_termination_grace_period():
    """If ER_DRAIN_TIMEOUT_S exceeds terminationGracePeriodSeconds the kubelet
    SIGKILLs mid-drain and the politeness in §8.3 is wasted."""
    rendered = render("test")
    cm = find_doc(rendered, "ConfigMap", "eventing-config")
    dep = find_doc(rendered, "Deployment", "eventrunner")
    drain = float(re.search(r'ER_DRAIN_TIMEOUT_S:\s*"?(\d+)"?', cm).group(1))
    grace = float(re.search(r"terminationGracePeriodSeconds:\s*(\d+)", dep).group(1))
    assert drain < grace, f"drain {drain}s must be under the {grace}s grace period"


@needs_kubectl
def test_the_liveness_probe_uses_the_heartbeat_checker():
    dep = find_doc(render("test"), "Deployment", "eventrunner")
    assert "eventrunner.healthcheck" in dep, \
        "§8.4: a process check cannot see a dead consumer thread"


@needs_kubectl
def test_no_manifest_sets_run_as_user():
    """On OpenShift the SCC assigns a UID from the namespace range and overrides
    the image's USER. Setting runAsUser ourselves would fight that; the image is
    built so an arbitrary UID works (§8.5)."""
    for overlay in OVERLAYS:
        assert "runAsUser" not in render(overlay)
        assert "runAsNonRoot: true" in render(overlay)


def test_kafkatopics_use_the_only_served_api_version():
    """Strimzi 1.0.x serves ONLY kafka.strimzi.io/v1. A v1beta2 manifest fails
    outright with `no matches for kind "KafkaTopic"`."""
    # Scoped to non-comment lines: the file's own comment explains the v1beta2
    # trap, and a naive substring check would trip over the explanation.
    lines = (K8S / "topics" / "kafkatopics.yaml").read_text().splitlines()
    body = "\n".join(ln for ln in lines if not ln.lstrip().startswith("#"))
    assert "apiVersion: kafka.strimzi.io/v1" in body
    assert "v1beta2" not in body


def test_kafkatopics_target_the_operators_own_namespace():
    """Finding 1: STRIMZI_NAMESPACE is a fieldRef to the operator's own namespace,
    so a KafkaTopic in kev1 is accepted by the API server and then SILENTLY
    ignored — no reconcile, no event, no error."""
    text = (K8S / "topics" / "kafkatopics.yaml").read_text()
    assert text.count("namespace: kafka") == 2
    assert "namespace: kev1" not in text


def test_topic_retention_is_below_the_brokers_offset_retention():
    """§16 Gap C: offsets.retention.minutes is 7 days on this broker. Topic
    retention must be shorter so a group idled past offset expiry replays at most
    a day of requests, not a week."""
    text = (K8S / "topics" / "kafkatopics.yaml").read_text()
    for ms in re.findall(r'retention\.ms:\s*"(\d+)"', text):
        assert int(ms) <= 7 * 24 * 3600 * 1000, "retention exceeds offset retention"
        assert int(ms) == 86_400_000, "the design settled on 24h"


def test_topics_are_namespace_prefixed():
    """The broker in ns kafka is shared and already carries an unrelated
    keda-test-topic, so generic names would be a collision risk (§5)."""
    text = (K8S / "topics" / "kafkatopics.yaml").read_text()
    assert "name: kev1-requests" in text and "name: kev1-responses" in text


# ---- cluster-gated: fully defaulted objects ---------------------------------

def server_objects(overlay: str) -> dict[tuple[str, str], dict]:
    r = _run(["kubectl", "apply", "-k", str(OVERLAYS[overlay]),
              "--dry-run=server", "-o", "json"], timeout=180)
    assert r.returncode == 0, f"server dry-run failed:\n{r.stderr}"
    doc = json.loads(r.stdout)
    items = doc.get("items", [doc])
    return {(i["kind"], i["metadata"]["name"]): i for i in items}


@needs_cluster
def test_every_object_is_accepted_by_the_api_server():
    """T3.1/T3.2/T3.3's shared gate: `apply --dry-run=server` clean. This is the
    check that catches a wrong apiVersion, an unknown CRD field or a webhook
    rejection before a real deploy."""
    for overlay in OVERLAYS:
        objs = server_objects(overlay)
        kinds = {k for k, _ in objs}
        assert {"ConfigMap", "Service", "Deployment", "ScaledObject"} <= kinds, kinds


@needs_cluster
def test_the_topics_are_accepted_by_strimzi():
    r = _run(["kubectl", "apply", "-f", str(K8S / "topics" / "kafkatopics.yaml"),
              "--dry-run=server"], timeout=120)
    assert r.returncode == 0, r.stderr
    assert "kev1-requests" in r.stdout and "kev1-responses" in r.stdout


@needs_cluster
def test_applying_the_overlay_never_overwrites_the_replica_count_keda_owns():
    """The premise §14.1 change detection rests on, asserted directly.

    Omitting spec.replicas does NOT make the field absent from the live object —
    the API server defaults it to 1 on create. What keeps `kubectl diff -k`
    stable is `kubectl apply`'s merge semantics: a field present in neither the
    manifest nor the last-applied-configuration is left untouched. So whatever
    KEDA wrote survives an apply, and the diff is empty.

    This test asserts the outcome in whichever state the cluster is in:
      * not deployed yet -> the server defaults replicas to 1;
      * already deployed -> a dry-run apply reports exactly the LIVE value,
        including 0 after a scale-to-zero, rather than reverting it.
    """
    for overlay in OVERLAYS:
        # Retried, because KEDA may be scaling the Deployment WHILE this runs: the
        # dry-run and the live read are two separate API calls, and a scale event
        # between them is a false mismatch rather than a real regression.
        last = None
        for _ in range(4):
            dry = server_objects(overlay)[("Deployment", "eventrunner")]
            applied = dry["spec"].get("replicas")
            live_raw = _run(["kubectl", "-n", "kev1", "get", "deploy", "eventrunner",
                             "-o", "json"], timeout=60)
            if live_raw.returncode != 0:
                assert applied == 1, ("on create the server defaults replicas to 1; "
                                      f"got {applied}")
                last = None
                break
            live = json.loads(live_raw.stdout)["spec"].get("replicas")
            if applied == live:
                last = None
                break
            last = (live, applied)
        assert last is None, (
            f"{overlay}: a dry-run apply would change replicas from {last[0]} to "
            f"{last[1]} — KEDA's value must be left alone or change detection "
            f"reports drift forever")


@needs_cluster
def test_the_server_keeps_eventbridge_at_one_replica():
    for overlay in OVERLAYS:
        dep = server_objects(overlay)[("Deployment", "eventbridge")]
        assert dep["spec"]["replicas"] == 1
        assert dep["spec"]["strategy"]["type"] == "Recreate"


@needs_cluster
def test_the_scaledobject_survives_the_keda_admission_webhook():
    """KEDA's webhook rejects, for example, two ScaledObjects targeting the same
    Deployment. A clean dry-run means the trigger shape is acceptable to it."""
    for overlay in OVERLAYS:
        so = server_objects(overlay)[("ScaledObject", "eventrunner")]
        assert so["spec"]["minReplicaCount"] == 0
        assert so["spec"]["triggers"][0]["type"] == "kafka"


# ---- ntfy: a capability, not a label ----------------------------------------

@needs_kubectl
def test_no_overlay_ever_renders_an_ntfy_topic():
    """An ntfy topic name lets anyone who knows it BOTH read and publish your
    notifications, so it is treated like the API credential: supplied from the
    environment at deploy time, never committed and never present in
    `kubectl kustomize` output."""
    for overlay in OVERLAYS:
        rendered = render(overlay)
        assert "NTFY_TOPIC" not in rendered, f"{overlay} renders an ntfy topic"
        assert "NTFY_TOKEN" not in rendered


@needs_kubectl
def test_eventbridge_reads_the_optional_ntfy_secret_after_the_configmap():
    """Order is load-bearing: for a key present in several envFrom sources the LAST
    source wins, which is how NTFY_ENABLED=true in the Secret overrides the
    ConfigMap's "false". `optional: true` keeps overlays without ntfy working."""
    for overlay in OVERLAYS:
        doc = find_doc(render(overlay), "Deployment", "eventbridge")
        cm_at = doc.index("configMapRef")
        secret_at = doc.index("eventbridge-ntfy")
        assert cm_at < secret_at, f"{overlay}: the ntfy Secret must come after the ConfigMap"
        assert re.search(r"name: eventbridge-ntfy\s*\n\s*optional: true", doc), \
            f"{overlay}: the ntfy Secret must be optional"


@needs_kubectl
def test_the_configmap_still_defaults_ntfy_off():
    for overlay in OVERLAYS:
        cm = find_doc(render(overlay), "ConfigMap", "eventing-config")
        assert re.search(r'NTFY_ENABLED: "false"', cm), \
            "ntfy must be off unless a Secret turns it on"


def test_both_images_install_a_ca_bundle():
    """debian:*-slim ships no CA store, and this interpreter is a standalone CPython
    that looks for /etc/ssl/cert.pem while Debian writes
    /etc/ssl/certs/ca-certificates.crt — so BOTH the package and the symlink are
    needed. Without them every outbound HTTPS call (ntfy, the selftest's
    public-tunnel probes) fails with CERTIFICATE_VERIFY_FAILED at runtime."""
    for name in ("Dockerfile-eventbridge", "Dockerfile-eventrunner"):
        text = (ROOT / name).read_text()
        assert "ca-certificates" in text, f"{name} must install ca-certificates"
        assert "ln -sf /etc/ssl/certs/ca-certificates.crt /etc/ssl/cert.pem" in text, \
            f"{name} must point OpenSSL's default cafile at Debian's bundle"
        assert "no CA bundle" in text, f"{name} must assert it at build time"
