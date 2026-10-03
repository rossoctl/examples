"""Spawn `claude` per request, stream stdout/stderr, emit response CloudEvents.

The public entry point is `run_agent(event)`. It never returns until the
subprocess exits and the terminal `result` event has been emitted. See
DESIGN_PHASE0.md §4.3 and §4.5.
"""
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import threading
import time

from eventrunner import agentspec
from eventrunner.agentspec import AgentSpec
from eventrunner.config import API_KEY_VARS, Cfg
from eventrunner.emit import Emitter
from shared import ce

# Env vars forwarded to the `claude` subprocess. The child does NOT inherit
# our full environment — only these keys pass through when they are set on
# EventRunner. This is an allowlist, not a denylist: adding a new var here
# is the deliberate way to expose it to the subprocess.
_OS_BASICS = ("PATH", "HOME", "USER", "LOGNAME", "TMPDIR", "SHELL",
              "LANG", "LC_ALL", "TERM")
_CLAUDE_ROUTING = (
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_MODEL",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS",
    # Phase 1 §16 Gap B: where claude keeps session transcripts. Forwarded so the
    # runner controls the location instead of inheriting a $HOME that may sit on
    # the container's ephemeral layer while /data is the mounted volume.
    "CLAUDE_CONFIG_DIR",
    "CLAUDE_CODE_PROJECT_DIR_NAME",
)
_SECRETS = frozenset(API_KEY_VARS)


def child_env(cfg: Cfg | None = None) -> dict[str, str]:
    """Build the curated env dict for the claude subprocess.

    With a `cfg` the session-storage location is pinned explicitly rather than
    left to whatever the pod inherited: `CLAUDE_CONFIG_DIR` under TMPDIR (so
    transcripts land on the same volume as everything else we write), and
    `CLAUDE_CODE_PROJECT_DIR_NAME` so the `projects/<name>` path segment is
    stable instead of being derived from cwd by a lossy encoding.
    """
    out: dict[str, str] = {}
    for k in (*_OS_BASICS, *_CLAUDE_ROUTING):
        v = os.environ.get(k)
        if v:
            out[k] = v
    if cfg is not None and cfg.claude_config_dir:
        out["CLAUDE_CONFIG_DIR"] = cfg.claude_config_dir
    return out


def _redact(k: str, v: str) -> str:
    if k in _SECRETS:
        head = v[:4] if len(v) > 8 else ""
        return f"{head}…({len(v)} chars)"
    return v


def log_forwarded_env(logger=print) -> None:
    """Print (once at startup) which claude-routing vars are being passed through."""
    interesting = {k: os.environ[k] for k in _CLAUDE_ROUTING if os.environ.get(k)}
    if interesting:
        pretty = " ".join(f"{k}={_redact(k, v)}" for k, v in interesting.items())
        logger(f"[eventrunner] forwarding to claude subprocess: {pretty}")
    else:
        logger("[eventrunner] no ANTHROPIC_*/CLAUDE_CODE_* env vars set — claude uses its own defaults")


def _mock_delay(cfg: Cfg) -> float:
    """A plausible per-turn duration for mock mode.

    Uniform in [min, max]. Without it a batch of 100 mock agents completes faster than
    the progress page can render, so nothing is ever observed in flight — the opposite
    of what a scaling demo needs to show.
    """
    import random
    lo, hi = max(0.0, cfg.mock_delay_min_s), max(0.0, cfg.mock_delay_max_s)
    if hi <= 0:
        return 0.0
    return random.uniform(min(lo, hi), hi)


def _payload(event) -> dict:
    d = event.data
    if isinstance(d, (bytes, bytearray)):
        d = json.loads(d.decode())
    if isinstance(d, str):
        d = json.loads(d)
    return d or {}


# ---- stream-json → compact CloudEvent data payload -------------------------
#
# The claude CLI's stream-json protocol emits several frame types (system,
# assistant, user, result, ...). Faithfully forwarding each frame produces
# very verbose CloudEvents that bury the actual model output in nested JSON.
# `classify()` compresses a frame into a small, uniform payload:
#
#   {
#     "type": <claude frame type>,
#     "role": "assistant" | "final" | "system" | "raw",
#     "text": <extracted human-readable text, may be None>,
#     "stats": <compact usage/cost/duration, only for `role=final`>,
#     "raw":   <the full original frame, when include_raw is True>,
#   }
#
# Consumers should read `text` first; `raw` is a debug escape hatch. This is
# also what tells `_pump` to promote claude's own `type=result` frame to
# `phase=result final=true` so we don't emit a redundant synthesized terminal.

_ASSISTANT_TEXT_MAX = 64_000


def _assistant_text(frame: dict) -> str | None:
    """Concatenate every text block in an `assistant` message; note tool_use blocks."""
    msg = frame.get("message") or {}
    content = msg.get("content")
    if not isinstance(content, list):
        return None
    parts: list[str] = []
    for c in content:
        if not isinstance(c, dict):
            continue
        if c.get("type") == "text":
            parts.append(c.get("text", "") or "")
        elif c.get("type") == "tool_use":
            name = c.get("name") or "?"
            parts.append(f"[tool_use:{name}]")
        elif c.get("type") == "tool_result":
            parts.append("[tool_result]")
    if not parts:
        return None
    joined = "\n".join(parts).strip()
    return joined[:_ASSISTANT_TEXT_MAX] if joined else None


def _result_stats(frame: dict) -> dict:
    """Keep the numbers you'd actually want on a dashboard; drop the rest."""
    usage = frame.get("usage") or {}
    return {
        "num_turns":       frame.get("num_turns"),
        "duration_ms":     frame.get("duration_ms"),
        "duration_api_ms": frame.get("duration_api_ms"),
        "total_cost_usd":  frame.get("total_cost_usd"),
        "stop_reason":     frame.get("stop_reason"),
        "is_error":        frame.get("is_error"),
        "usage": {k: usage.get(k) for k in (
            "input_tokens", "output_tokens",
            "cache_creation_input_tokens", "cache_read_input_tokens",
        ) if usage.get(k) is not None},
    }


def classify(frame, *, include_raw: bool = True) -> dict:
    """Compress a claude stream-json frame into a compact response CloudEvent data payload.

    Accepts non-dict frames (arbitrary text / parse failures) and returns a
    minimal `{"text": …, "role": "raw"}` payload for them.
    """
    if not isinstance(frame, dict):
        text = frame if isinstance(frame, str) else json.dumps(frame)
        out: dict = {"role": "raw", "text": text}
        if include_raw:
            out["raw"] = frame
        return out

    t = frame.get("type", "unknown")
    out = {"type": t}

    if t == "assistant":
        out["role"] = "assistant"
        out["text"] = _assistant_text(frame)
    elif t == "result":
        out["role"] = "final"
        out["text"] = frame.get("result")
        out["stats"] = _result_stats(frame)
    elif t == "system":
        sub = frame.get("subtype", "?")
        out["role"] = "system"
        out["subtype"] = sub
        if sub == "init":
            tools = frame.get("tools") or []
            out["text"] = (f"init: model={frame.get('model','?')} "
                           f"session={(frame.get('session_id') or '?')[:8]}… "
                           f"tools={len(tools)}")
        elif sub in ("hook_started", "hook_response"):
            out["text"] = f"{sub}: {frame.get('hook_name','?')}"
        else:
            out["text"] = f"system:{sub}"
    elif t == "user":
        out["role"] = "user"
        out["text"] = _assistant_text(frame)   # same content shape
    else:
        out["role"] = "raw"
        out["text"] = None

    if include_raw:
        out["raw"] = frame
    return out


def is_hook_frame(frame) -> bool:
    return (isinstance(frame, dict) and frame.get("type") == "system"
            and (frame.get("subtype") or "").startswith("hook_"))


def build_cmd(cfg: Cfg, event, payload: dict, *,
              resume_target: str | None = None,
              spec: AgentSpec | None = None) -> list[str]:
    """Assemble the claude argv for one turn.

    `resume_target` overrides what `--resume` gets. Phase 0 always passed the
    session UUID, which only works when the transcript is on this machine's disk;
    Phase 1 passes an absolute transcript path when it has one, which is what
    makes `/continue` survive a scale-to-zero (§16 Gap B, verified by
    scripts/verify_resume_by_path.py).

    Phase 3 §5.5: `spec` is an `AgentSpec` carrying the tool policy, system prompt and
    model. `None` — and the built-in `default` spec — produce **byte-identical argv to
    Phase 2**, which is what keeps the e2e path and the existing tests unchanged;
    `tests/test_agentspec.py` asserts that equality rather than trusting it.

    Precedence between the request and the spec is deliberate and differs by field:

    * `max_turns` and `model` — the **request wins** when it names one. Both are
      per-turn parameters a caller has always been able to send, and a spec silently
      overriding them would break Phase 0's wire contract.
    * the tool policy, permission mode and system prompt — **only the spec**. These are
      the sandbox (§7.4), and a request that could widen them would be a request that
      can escape them, which is the whole control inverted.

    The prompt goes through `argv`, never a shell: `Popen` with a list and no
    `shell=True` is what makes §7.4's attacker-influenced prompt a prompt-injection
    problem rather than also a command-injection one. The obvious "improvement" of
    building a command string would silently reintroduce it.
    """
    spec = spec or AgentSpec()
    session = event["sessionuuid"]
    mode = event.get("mode", "start")
    prompt = payload.get("prompt", "")
    # `is not None`, not `or`: `max_turns: 0` is nonsensical input, and with `or` it
    # silently became the spec's value while the spec path REJECTS zero
    # (`test_a_nonsensical_max_turns_is_refused`). Two paths disagreeing about the same
    # bad input is worse than either answer — the HTTP boundary is where it should be
    # refused, and until then both paths at least agree it is what the caller asked for.
    req_turns = payload.get("max_turns")
    max_turns = spec.max_turns if req_turns is None else req_turns
    cmd = [
        cfg.claude_bin, "-p", prompt,
        "--output-format", "stream-json",
        "--verbose",
        "--max-turns", str(max_turns),
        "--permission-mode", spec.permission_mode,
    ]
    if mode == "start":
        cmd += ["--session-id", session]
    else:
        cmd += ["--resume", resume_target or session]
    if model := (payload.get("model") or spec.model):
        cmd += ["--model", model]
    # Appended after the Phase 2 flags, so the default spec's argv is a prefix-equal
    # match rather than merely an equivalent set — easier to assert and to eyeball in a
    # log line.
    if spec.allowed_tools:
        cmd += ["--allowedTools", ",".join(spec.allowed_tools)]
    if spec.disallowed_tools:
        cmd += ["--disallowedTools", ",".join(spec.disallowed_tools)]
    if spec.system_prompt:
        cmd += ["--append-system-prompt", spec.system_prompt]
    if spec.settings_path:
        cmd += ["--settings", spec.settings_path]
    if spec.mcp_config_path:
        cmd += ["--mcp-config", spec.mcp_config_path]
    return cmd


def resolve_resume_target(cfg: Cfg, transcripts, corr: str, session: str,
                          scratch: pathlib.Path) -> str | None:
    """Find an absolute transcript path for a `mode=continue` turn.

    Returns None when neither this pod's disk nor EventBridge has the transcript,
    in which case the caller falls back to `--resume <uuid>` — which will fail if
    the session was started in a pod that no longer exists. That is Gap B, and
    the fallback logs it rather than producing a mystery.
    """
    if transcripts is None:
        return None
    try:
        return transcripts.restore_for_resume(corr, session, cfg.claude_config_dir, scratch)
    except Exception as e:  # noqa: BLE001 - never fail a turn over a checkpoint
        print(f"[eventrunner] transcript restore for {corr} failed: {e!r}",
              file=sys.stderr, flush=True)
        return None


def run_agent(cfg: Cfg, emitter: Emitter, event, *, transcripts=None) -> None:
    corr    = event["correlationid"]
    session = event["sessionuuid"]
    mode    = event.get("mode", "start")
    # §11 causation binding: every response event this turn emits names the
    # request event that caused it, so a /continue turn's output is attributable
    # to its specific triggering request rather than only to the conversation.
    causation = event.get("id") or None
    # §21: echo the batch membership onto every response event, so EventBridge can
    # attribute progress without consulting the request topic.
    groupid = event.get("groupid") or None
    payload = _payload(event)
    workdir = pathlib.Path(cfg.tmpdir, "eventrunner", "work", corr)
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / "prompt.txt").write_text(payload.get("prompt", ""))

    # Sequence continuity across pods. MUST happen before the first emit: this
    # pod's Emitter starts its per-correlation counter at zero, and without
    # seeding it from what EventBridge already holds, a second turn served by a
    # second pod re-uses sequences 1..N and overwrites the first turn's stored
    # events (the store keys rows on (correlationid, sequence)).
    if transcripts is not None:
        try:
            transcripts.seed_emitter(emitter, corr)
        except Exception as e:  # noqa: BLE001 - never fail a turn over this
            print(f"[eventrunner] could not seed sequence for {corr}: {e!r}; "
                  f"events from this turn may collide with an earlier turn's",
                  file=sys.stderr, flush=True)

    started = time.monotonic()

    # §5.1 resolution order, most specific first. A request naming an agent this runner
    # does not have is an ERROR EVENT, never a silent fallback to `default`: running a
    # different agent than the one asked for means a policy the caller believes applied
    # did not, which is worse than not running at all.
    agent_name = agentspec.resolve_name(event.get(ce.EXT_AGENT), cfg.agent_name)
    try:
        spec = agentspec.load(cfg.agent_dir, agent_name)
    except agentspec.SpecError as e:
        print(f"[eventrunner] {corr}: {e}", file=sys.stderr, flush=True)
        emitter.emit(correlationid=corr, sessionuuid=session,
                     sequence=emitter.next_seq(corr), phase="error", final=True,
                     data={"error": f"agent spec unavailable: {e}",
                           "agent": agent_name},
                     causationid=causation, groupid=groupid)
        return
    if not spec.is_default:
        print(f"[eventrunner] {corr}: agent={spec.name} "
              f"permission_mode={spec.permission_mode} "
              f"allowed={list(spec.allowed_tools)} "
              f"disallowed={list(spec.disallowed_tools)}", flush=True)

    if cfg.mock_claude:
        _run_mock(emitter, corr, session, payload, causation=causation,
                  groupid=groupid, delay=_mock_delay(cfg))
        return

    resume_target = None
    if mode != "start":
        resume_target = resolve_resume_target(cfg, transcripts, corr, session, workdir)
        if resume_target:
            print(f"[eventrunner] resuming {corr} from transcript {resume_target}",
                  flush=True)
        else:
            print(f"[eventrunner] no transcript available for {corr}; falling back to "
                  f"--resume {session} (will fail if the session was started in a "
                  f"pod that is gone — DESIGN_PHASE1.md §16 Gap B)",
                  file=sys.stderr, flush=True)

    cmd = build_cmd(cfg, event, payload, resume_target=resume_target, spec=spec)
    try:
        proc = subprocess.Popen(cmd, cwd=str(workdir),
                                stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, bufsize=1,
                                env=child_env(cfg))
    except FileNotFoundError as e:
        emitter.emit(correlationid=corr, sessionuuid=session,
                     sequence=emitter.next_seq(corr), phase="error", final=True,
                     data={"error": f"claude binary not found: {e}"},
                     causationid=causation, groupid=groupid)
        return

    # Signalled from _pump when claude emits its own type=result frame — we then
    # forward THAT as phase=result final=true and skip our synthesized terminal.
    saw_result = threading.Event()
    # _pump uses this to dedupe the `text` on a role=final event when it equals
    # the last-seen role=assistant text (claude's stream-json duplicates the
    # model's last reply once as assistant and once inside its own result frame).
    last_assistant: dict[str, str | None] = {"text": None}

    try:
        with proc:
            t_out = threading.Thread(
                target=_pump,
                args=(proc.stdout, corr, session, "stdout", emitter, workdir / "stdout.jsonl", cfg, saw_result, last_assistant, causation, groupid),
                daemon=True,
            )
            t_err = threading.Thread(
                target=_pump,
                args=(proc.stderr, corr, session, "stderr", emitter, workdir / "stderr.log", cfg, None, None, causation, groupid),
                daemon=True,
            )
            t_out.start(); t_err.start()
            rc = proc.wait()
            t_out.join(timeout=5); t_err.join(timeout=5)
    finally:
        # §16 Gap B: checkpoint the transcript once the turn is over, whatever the
        # outcome. Exactly one upload per turn, and a failure here is logged but
        # never turned into an agent error — the turn's response events are
        # already on the wire; only the NEXT turn's resume is affected.
        if transcripts is not None:
            try:
                transcripts.checkpoint_after_turn(corr, session, cfg.claude_config_dir)
            except Exception as e:  # noqa: BLE001
                print(f"[eventrunner] transcript checkpoint for {corr} failed: {e!r}",
                      file=sys.stderr, flush=True)

    if saw_result.is_set() and rc == 0:
        # Claude's own `type=result` frame was already forwarded as phase=result
        # final=true — no need to emit a second terminal event.
        return

    duration_ms = int((time.monotonic() - started) * 1000)
    emitter.emit(
        correlationid=corr, sessionuuid=session,
        sequence=emitter.next_seq(corr),
        phase="error" if rc != 0 else "result",
        final=True,
        data={"final": True, "exit_code": rc, "duration_ms": duration_ms,
              "text": None if rc == 0 else f"claude exited with code {rc}"},
        causationid=causation, groupid=groupid,
    )


def _pump(stream, corr: str, session: str, phase: str, emitter: Emitter,
          log_path: pathlib.Path, cfg: Cfg | None, saw_result: threading.Event | None,
          last_assistant: dict | None = None, causation: str | None = None,
          groupid: str | None = None) -> None:
    if stream is None:
        return
    with open(log_path, "a", encoding="utf-8", errors="replace") as sink:
        for line in stream:
            line = line.rstrip()
            if not line:
                continue
            sink.write(line + "\n"); sink.flush()

            try:
                frame = json.loads(line)
            except json.JSONDecodeError:
                emitter.emit(correlationid=corr, sessionuuid=session,
                             sequence=emitter.next_seq(corr), phase=phase, final=False,
                             data={"role": "raw", "text": line}, causationid=causation,
                             groupid=groupid)
                continue

            # Optionally drop the SessionStart hook chatter; it adds noise.
            if cfg is not None and not cfg.emit_system_hooks and is_hook_frame(frame):
                continue

            data = classify(frame, include_raw=(cfg is None or cfg.include_raw))
            role = data.get("role")
            is_final_frame = (role == "final")

            # Dedupe: claude's stream-json emits the model's last reply twice —
            # once in the assistant frame and once in the result frame's `result`.
            # When both `text` strings match, strip it from the result event so
            # the same string doesn't ride the wire twice.
            if (last_assistant is not None
                    and cfg is not None and cfg.dedupe_final_text
                    and is_final_frame and data.get("text")
                    and last_assistant.get("text")
                    and data["text"].strip() == last_assistant["text"].strip()):
                data["text"] = None
                data["text_echoes_prior_assistant"] = True

            if last_assistant is not None and role == "assistant" and data.get("text"):
                last_assistant["text"] = data["text"]

            emit_phase = "result" if is_final_frame else phase
            emitter.emit(
                correlationid=corr, sessionuuid=session,
                sequence=emitter.next_seq(corr),
                phase=emit_phase, final=is_final_frame, data=data,
                causationid=causation, groupid=groupid,
            )
            if is_final_frame and saw_result is not None:
                saw_result.set()


def _run_mock(emitter: Emitter, corr: str, session: str, payload: dict,
              *, causation: str | None = None, groupid: str | None = None,
              delay: float = 0.0) -> None:
    """Deterministic emit sequence, no subprocess — used by tests when ER_MOCK_CLAUDE=true."""
    prompt = payload.get("prompt", "")
    emitter.emit(correlationid=corr, sessionuuid=session,
                 sequence=emitter.next_seq(corr), phase="stdout", final=False,
                 data={"type": "system", "role": "system", "subtype": "init",
                       "text": f"init: mock session={session[:8]}…"},
                 causationid=causation, groupid=groupid)
    # The pause sits between the init frame and the reply on purpose: the run shows up
    # as `running` on the group page for a realistic interval instead of appearing and
    # vanishing within one poll.
    if delay > 0:
        time.sleep(delay)
    reply = f"MOCK-REPLY[{prompt[:60]}]"
    emitter.emit(correlationid=corr, sessionuuid=session,
                 sequence=emitter.next_seq(corr), phase="stdout", final=False,
                 data={"type": "assistant", "role": "assistant", "text": reply,
                       "raw": {"type": "assistant",
                               "message": {"role": "assistant",
                                           "content": [{"type": "text", "text": reply}]}}},
                 causationid=causation, groupid=groupid)
    emitter.emit(correlationid=corr, sessionuuid=session,
                 sequence=emitter.next_seq(corr), phase="result", final=True,
                 data={"type": "result", "role": "final", "text": None,
                       "text_echoes_prior_assistant": True,
                       "stats": {"num_turns": 1, "total_cost_usd": 0.0,
                                 "usage": {"input_tokens": len(prompt), "output_tokens": len(reply)},
                                 "duration_ms": int(delay * 1000)}},
                 causationid=causation, groupid=groupid)
