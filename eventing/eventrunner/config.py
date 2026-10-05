"""EventRunner config: env > baked defaults."""
from __future__ import annotations

import os
import pathlib
from collections.abc import Mapping
from dataclasses import dataclass

# Env vars that carry an Anthropic credential. Their presence is what decides
# whether we default to real `claude` or to mock mode — a container with no key
# would otherwise spawn claude only to have it fail on auth, turning every
# request into an error event. runner.py also redacts these when logging.
API_KEY_VARS = ("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY")


def has_api_credentials(env: Mapping[str, str] | None = None) -> bool:
    """True when at least one API_KEY_VARS entry is set to a non-blank value."""
    e = os.environ if env is None else env
    return any((e.get(k) or "").strip() for k in API_KEY_VARS)


def _bool(raw: str | None, default: bool) -> bool:
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Cfg:
    kafka_bootstrap: str = "localhost:9092"
    request_topic: str = "requests"
    response_topic: str = "responses"
    source_uri: str = "rossoctl://eventrunner/local"
    max_concurrent: int = 4
    claude_bin: str = "claude"
    tmpdir: str = ""
    mock_claude: bool = False
    # Human-readable justification for mock_claude, printed at startup so an
    # auto-selected mode is never silent.
    mock_reason: str = ""
    # §21 demo realism: a mock turn that returns in microseconds makes a batch of 100
    # finish before the page can render, so the progress display never shows progress.
    # A uniform 1-5s per turn looks like real work while keeping a 100-agent demo under
    # a minute. Tests set both to 0.
    mock_delay_min_s: float = 1.0
    mock_delay_max_s: float = 5.0
    emit_system_hooks: bool = False
    include_raw: bool = True
    dedupe_final_text: bool = True

    # ---- Phase 1 (DESIGN_PHASE1.md §8.9) ----

    # §8.2 — must match the ScaledObject's trigger consumerGroup EXACTLY. A
    # mismatch is a silent no-scale failure: KEDA watches a group nobody joins,
    # so lag never falls and the pod never scales down (or the reverse).
    consumer_group: str = "eventrunner"

    # §8.1 — bounded-backoff retry around KafkaConsumer construction. Retries for
    # the life of the pod: KEDA may start a pod while the broker is rolling, and
    # giving up would leave a Running pod that consumes nothing.
    kafka_retry_initial_s: float = 1.0
    kafka_retry_max_s: float = 30.0

    # §8.4 — liveness heartbeat, touched on every poll.
    heartbeat_path: str = ""
    heartbeat_max_age_s: float = 90.0

    # §8.3 / RQ-2 — SIGTERM waits for in-flight runs. Keep below the
    # Deployment's terminationGracePeriodSeconds (600) or the kubelet SIGKILLs
    # mid-drain and the politeness is wasted.
    drain_timeout_s: float = 570.0
    # Belt-and-braces wait inside the consumer's own shutdown path. __main__
    # already drains before stopping the consumer, so in practice in-flight is
    # zero by the time this runs and it returns immediately.
    final_commit_wait_s: float = 10.0

    # RQ-1 — commit only after the terminal event, so lag means "work not
    # finished" and KEDA cannot scale a streaming pod away. Switchable purely so
    # the old behaviour is testable; production wants True.
    commit_after_terminal: bool = True

    # §16 Gap C — a group idle past the broker's offsets.retention (7 days) loses
    # its committed offsets, falls back to auto_offset_reset=earliest, and
    # replays every request still inside the topic's 24 h retention. Requests
    # older than this are dropped with a logged reason instead of re-running a
    # day of agent work. 0 disables the guard.
    max_request_age_s: float = 3600.0

    # §16 Gap B — transcript checkpointing through EventBridge.
    transcript_sync: bool = True
    eventbridge_url: str = ""
    transcript_max_bytes: int = 32 * 1024 * 1024
    claude_config_dir: str = ""

    # §11 — signed envelopes. Feature-flagged off; the e2e path is unaffected.
    require_signature: bool = False
    signing_key_path: str = ""
    verify_key_path: str = ""
    # The approved-key set: kid -> public key. When set, request verification
    # selects a key by the token's kid instead of assuming one key, which is what
    # makes it an allowlist of several agents rather than a single-key check. Empty
    # falls back to verify_key_path, so the older one-key deployment is unchanged.
    verify_keyset_path: str = ""
    # The kid this runner names in the responses it signs. Empty still signs, and a
    # verifier holding exactly one key accepts it; name it as soon as there are two.
    signing_kid: str = ""

    # ---- Phase 3 (DESIGN_PHASE3.md) ----

    # §3.3 — which tenant this runner serves. Stamped on every response as
    # `ce_userkey`, which is what lets EventBridge file the response in the right
    # store. Empty is single-tenant mode (the default, = Phase 2). Required once
    # REQUEST_TOPIC is not the default: a runner that stamps no userkey produces
    # events EventBridge cannot attribute, and a silent default here would route one
    # user's output into another user's store. `load` enforces that, loudly.
    userkey: str = ""

    # §5.2 — where baked AgentSpecs live. A directory in the image, not a mount:
    # an agent definition the agent itself could rewrite is not a policy.
    agent_dir: str = "/etc/rossoctl/agents"
    # §5.1 — the fallback agent when neither the request nor the registry names one.
    # `default` with no file on disk is the built-in spec, i.e. Phase 2's argv.
    agent_name: str = "default"


def load() -> Cfg:
    e = os.environ.get
    cfg = Cfg()
    cfg.kafka_bootstrap = e("KAFKA_BOOTSTRAP", cfg.kafka_bootstrap)
    cfg.request_topic   = e("REQUEST_TOPIC",   cfg.request_topic)
    cfg.response_topic  = e("RESPONSE_TOPIC",  cfg.response_topic)
    cfg.consumer_group  = e("ER_CONSUMER_GROUP", cfg.consumer_group)
    cfg.max_concurrent  = int(e("ER_MAX_CONCURRENT", str(cfg.max_concurrent)))
    cfg.claude_bin      = e("CLAUDE_BIN", cfg.claude_bin)
    # Mock mode: an explicit ER_MOCK_CLAUDE always wins. With the var unset we
    # pick the mode that can actually work — real claude when a credential is
    # present, mock otherwise. Note that `claude` can also be authenticated by
    # `claude login` with no env var in sight; set ER_MOCK_CLAUDE=false to force
    # real mode in that case.
    mock_env = (e("ER_MOCK_CLAUDE") or "").strip()
    if mock_env:
        cfg.mock_claude = mock_env.lower() == "true"
        cfg.mock_reason = f"explicit ER_MOCK_CLAUDE={mock_env.lower()}"
    elif has_api_credentials():
        cfg.mock_claude = False
        cfg.mock_reason = "auto: API credential present"
    else:
        cfg.mock_claude = True
        cfg.mock_reason = f"auto: none of {'/'.join(API_KEY_VARS)} set"
    cfg.mock_delay_min_s = float(e("ER_MOCK_DELAY_MIN_S", str(cfg.mock_delay_min_s)))
    cfg.mock_delay_max_s = float(e("ER_MOCK_DELAY_MAX_S", str(cfg.mock_delay_max_s)))
    cfg.emit_system_hooks = _bool(e("ER_EMIT_SYSTEM_HOOKS"), False)
    cfg.include_raw       = _bool(e("ER_INCLUDE_RAW"), True)
    cfg.dedupe_final_text = _bool(e("ER_DEDUPE_FINAL_TEXT"), True)

    cfg.kafka_retry_initial_s = float(e("ER_KAFKA_RETRY_INITIAL_S", str(cfg.kafka_retry_initial_s)))
    cfg.kafka_retry_max_s     = float(e("ER_KAFKA_RETRY_MAX_S",     str(cfg.kafka_retry_max_s)))
    cfg.heartbeat_max_age_s   = float(e("ER_HEARTBEAT_MAX_AGE_S",   str(cfg.heartbeat_max_age_s)))
    cfg.drain_timeout_s       = float(e("ER_DRAIN_TIMEOUT_S",       str(cfg.drain_timeout_s)))
    cfg.final_commit_wait_s   = float(e("ER_FINAL_COMMIT_WAIT_S",   str(cfg.final_commit_wait_s)))
    cfg.commit_after_terminal = _bool(e("ER_COMMIT_AFTER_TERMINAL"), True)
    cfg.max_request_age_s     = float(e("ER_MAX_REQUEST_AGE_S",     str(cfg.max_request_age_s)))

    cfg.transcript_sync       = _bool(e("ER_TRANSCRIPT_SYNC"), True)
    cfg.eventbridge_url       = e("ER_EVENTBRIDGE_URL", cfg.eventbridge_url)
    cfg.transcript_max_bytes  = int(e("ER_TRANSCRIPT_MAX_BYTES", str(cfg.transcript_max_bytes)))

    cfg.require_signature  = _bool(e("ER_REQUIRE_SIGNATURE"), False)
    cfg.signing_key_path   = e("ER_SIGNING_KEY_PATH", cfg.signing_key_path)
    cfg.verify_key_path    = e("ER_VERIFY_KEY_PATH",  cfg.verify_key_path)
    cfg.verify_keyset_path = e("ER_VERIFY_KEYSET_PATH", cfg.verify_keyset_path)
    cfg.signing_kid        = e("ER_SIGNING_KID",      cfg.signing_kid)

    # §3.3, §5.1-§5.2.
    cfg.userkey    = (e("ER_USERKEY") or "").strip()
    cfg.agent_dir  = e("ER_AGENT_DIR",  cfg.agent_dir)
    cfg.agent_name = e("ER_AGENT_NAME", cfg.agent_name)
    # A per-user runner is pointed at a per-user topic, so a non-default REQUEST_TOPIC
    # with no ER_USERKEY means the deployment was rendered wrong. Refusing to start is
    # the only safe direction: the alternative is responses EventBridge files into the
    # `shared` store or drops, which looks exactly like an agent that never answered.
    if cfg.request_topic != Cfg.request_topic and not cfg.userkey:
        raise SystemExit(
            f"ER_USERKEY is required when REQUEST_TOPIC is not {Cfg.request_topic!r} "
            f"(got {cfg.request_topic!r}). A runner that stamps no userkey on its "
            f"responses produces events EventBridge cannot attribute. "
            f"See DESIGN_PHASE3.md §3.3.")

    base = e("TMPDIR", "/tmp").rstrip("/")
    cfg.tmpdir = f"{base}/rossoctl-keda1"
    pathlib.Path(cfg.tmpdir, "eventrunner").mkdir(parents=True, exist_ok=True)
    cfg.heartbeat_path = e("ER_HEARTBEAT_PATH") or f"{cfg.tmpdir}/eventrunner/heartbeat"

    # claude keeps session transcripts under CLAUDE_CONFIG_DIR (default ~/.claude).
    # Pinning it under TMPDIR puts them on the same volume as everything else we
    # write, which matters because HOME may be on the container's ephemeral layer
    # while /data is a mounted volume.
    cfg.claude_config_dir = e("CLAUDE_CONFIG_DIR") or f"{cfg.tmpdir}/eventrunner/claude"
    pathlib.Path(cfg.claude_config_dir).mkdir(parents=True, exist_ok=True)

    # Transcript checkpointing needs somewhere to PUT. With no URL configured the
    # feature is simply off — Phase 0 behaviour, and a local `docker run` with no
    # EventBridge reachable does not spew connection errors.
    if not cfg.eventbridge_url:
        cfg.transcript_sync = False
    return cfg
