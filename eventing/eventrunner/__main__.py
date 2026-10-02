"""EventRunner entrypoint."""
from __future__ import annotations

import signal
import sys
import threading

from eventrunner.config import load
from eventrunner.consume import Consumer
from eventrunner.emit import Emitter
from eventrunner.router import Router
from eventrunner.runner import log_forwarded_env, run_agent
from eventrunner.transcript import TranscriptStore
from shared import keyset, signing
from shared.heartbeat import Heartbeat
from shared.pidfile import PidFile


def _elog(msg: str) -> None:
    """§8.3: drain progress on stderr — unbuffered, and it interleaves correctly
    with a crash trace. `kubectl logs` shows both streams."""
    print(f"[eventrunner] {msg}", file=sys.stderr, flush=True)


def main() -> int:
    cfg = load()
    pidfile = PidFile("eventrunner")
    pidfile.__enter__()
    print(f"[eventrunner] bootstrap={cfg.kafka_bootstrap} group={cfg.consumer_group} "
          f"topics={cfg.request_topic}->{cfg.response_topic} "
          f"max_concurrent={cfg.max_concurrent} "
          f"mock_claude={cfg.mock_claude} ({cfg.mock_reason})")
    print(f"[eventrunner] commit_after_terminal={cfg.commit_after_terminal} "
          f"max_request_age_s={cfg.max_request_age_s:.0f} "
          f"drain_timeout_s={cfg.drain_timeout_s:.0f} "
          f"heartbeat={cfg.heartbeat_path} (max_age={cfg.heartbeat_max_age_s:.0f}s)")
    if cfg.mock_claude:
        print("[eventrunner] mock mode — deterministic replies, no subprocess, no API cost")
        print("[eventrunner] set ER_MOCK_CLAUDE=false to force real claude "
              "(e.g. when it is authenticated via `claude login` rather than an API key)")
    else:
        log_forwarded_env()

    # §16 Gap B: per-correlation transcript checkpointing through EventBridge, so
    # `/continue` survives the pod that ran the first turn being destroyed.
    transcripts = None
    if cfg.transcript_sync and cfg.eventbridge_url:
        transcripts = TranscriptStore(cfg.eventbridge_url,
                                      max_bytes=cfg.transcript_max_bytes)
        print(f"[eventrunner] transcript checkpointing -> {cfg.eventbridge_url} "
              f"(config_dir={cfg.claude_config_dir}, cap={cfg.transcript_max_bytes} bytes)")
    else:
        print("[eventrunner] transcript checkpointing OFF "
              "(set ER_EVENTBRIDGE_URL to enable; /continue then requires the same pod)")

    heartbeat = Heartbeat(cfg.heartbeat_path)
    heartbeat.touch()      # before Kafka, so an absent file always means "never started"

    # §11 — key material is loaded ONCE, here, and deliberately not caught. A runner
    # that believes it is signing but is not, or that cannot read the set it verifies
    # against, is worse than one that refuses to start: the first failure mode is
    # silent and the second is in `kubectl logs` before any request is accepted.
    seed = None
    if cfg.signing_key_path:
        seed = signing.load_seed(cfg.signing_key_path)
        print(f"[eventrunner] response signing ON as kid={cfg.signing_kid or '(unnamed)'} "
              f"(seed {cfg.signing_key_path}) — terminal events only")
    else:
        print("[eventrunner] response signing OFF (set ER_SIGNING_KEY_PATH to enable)")

    # Load once, never per record: keyset.py states that live reload is deliberately
    # absent, and re-reading per event would let a mid-run edit silently widen the
    # set of agents this runner trusts.
    ks = keyset.load_if_set(cfg.verify_keyset_path)
    if ks is not None:
        print(f"[eventrunner] request verification uses the approved set "
              f"{cfg.verify_keyset_path} — {len(ks)} kid(s): {', '.join(ks.kids)}")
    elif cfg.require_signature:
        print("[eventrunner] request verification uses a single key "
              f"({cfg.verify_key_path or cfg.signing_key_path}); "
              "set ER_VERIFY_KEYSET_PATH for an approved-agent list")

    emitter = Emitter(cfg.kafka_bootstrap, cfg.response_topic, cfg.source_uri,
                      seed=seed, kid=cfg.signing_kid or None,
                      userkey=cfg.userkey or None)
    # Phase 3 §3.3/§5.1. Printed because both are silent otherwise: a runner serving the
    # wrong tenant and a runner running the wrong agent both look like working pods.
    print(f"[eventrunner] tenancy={'userkey=' + cfg.userkey if cfg.userkey else 'single'} "
          f"agent={cfg.agent_name} agent_dir={cfg.agent_dir}")

    def _run(event):
        run_agent(cfg, emitter, event, transcripts=transcripts)

    router = Router(_run, cfg.max_concurrent)
    consumer = Consumer(cfg, router, heartbeat=heartbeat, keyset=ks)
    consumer.start()

    stop_evt = threading.Event()
    signal_count = {"n": 0}

    def _sig(signum, _frame):
        signal_count["n"] += 1
        if signal_count["n"] == 1:
            _elog(f"caught signal {signum}, shutting down (signal again to force)")
            stop_evt.set()
        else:
            _elog("second signal — forcing exit")
            import os as _os
            _os._exit(130)

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    try:
        while not stop_evt.wait(timeout=1.0):
            pass
    finally:
        # Shutdown order is load-bearing (§8.3 / RQ-2):
        #  1. stop taking new records, but keep the poll loop alive — it is what
        #     commits offsets as draining runs finish;
        #  2. stop dispatching anything still queued (those offsets stay
        #     uncommitted, so Kafka redelivers them to the next pod);
        #  3. WAIT for in-flight `claude` runs, reporting what we are waiting on;
        #  4. only then stop the consumer and let it make its final commit.
        consumer.stop_intake()
        router.stop()
        drained = router.drain(cfg.drain_timeout_s, log=_elog)
        consumer.stop()
        consumer.join(timeout=20.0)
        emitter.close()
        pidfile.__exit__(None, None, None)
        if consumer.skipped_stale:
            _elog(f"note: dropped {consumer.skipped_stale} stale request(s) this run "
                  f"(§16 Gap C replay guard)")
        print(f"[eventrunner] shut down (clean drain={drained})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
