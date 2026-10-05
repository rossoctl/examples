"""Declarative agent definitions. DESIGN_PHASE3.md §5.1, §5.2, §5.5.

Up to Phase 2 "the agent" was five flags in `build_cmd()`: there was no notion of
*which* agent, no system prompt, and no tool policy. A trigger (§7) has to answer "run
**what**", so that notion has to exist first — which is why §9 puts this before triggers
rather than after.

One TOML file per agent, matching the `config.toml` precedent rather than introducing a
second config language:

    /etc/rossoctl/agents/
      default/agent.toml
      triager/
        agent.toml
        system.md
        skills/issue-triage/SKILL.md

**The tool policy IS the sandbox.** A trigger-supplied prompt is attacker-influenced
text (§7.4) and the tools are what turn text into consequences, so a spec written for
trigger use is default-deny. The loaded `default` spec is deliberately NOT: an
unspecified field inherits Phase 2's behaviour, so `build_cmd` emits byte-identical argv
and the existing e2e tests keep passing. That is what makes this additive, and
`tests/test_agentspec.py` pins the argv equality rather than trusting the claim.

Stdlib only (`tomllib`, `pathlib`), per Phase 1 §1.1.
"""
from __future__ import annotations

import pathlib
import re
import tomllib
from dataclasses import dataclass, field

# An agent name becomes a path component under ER_AGENT_DIR, so it is validated as a
# DNS-1123 label rather than merely non-empty. That bounds it to characters that cannot
# traverse (`.` and `/` are both excluded) and keeps it usable as a Kubernetes object
# name later without a migration — the same reasoning §7.6 applies to trigger names.
NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
NAME_MAX = 63

# Phase 2's argv used `acceptEdits` unconditionally. It stays the default here so the
# `default` spec reproduces it; a spec intended for a trigger should say `plan`.
DEFAULT_PERMISSION_MODE = "acceptEdits"
PERMISSION_MODES = ("plan", "acceptEdits", "bypassPermissions")

# Phase 0's wire default, applied when neither the request payload nor the spec says.
DEFAULT_MAX_TURNS = 3

SPEC_FILENAME = "agent.toml"


class SpecError(Exception):
    """A spec that cannot be loaded or is not valid.

    Raised rather than returning a default, because §5.1 is explicit: a request naming
    an agent the runner does not have is an error event, never a silent fallback to
    `default`. Running a different agent than the one asked for is worse than not
    running — the caller believes a policy applied that did not.
    """


@dataclass(frozen=True)
class Skill:
    """One entry from `[[skills]]`.

    Phase 3 step 9 loads these but fetches nothing: only `image://` sources resolve,
    because they are already present. `sha256` and `sig_kid` are carried so a spec
    written today stays valid when T19 turns fetching on, and are validated here so a
    bundle reference that could never verify fails at load rather than at fetch.
    """
    name: str
    source: str
    sha256: str = ""
    sig_kid: str = ""

    @property
    def is_baked(self) -> bool:
        return self.source.startswith("image://")


@dataclass(frozen=True)
class Limits:
    """Per-run ceilings from `[limits]`.

    **Carried, not yet enforced** — the same arrangement as `Skill.sha256`: parsed and
    validated here so a spec written today stays valid, and applied when the code that
    applies it lands. `max_events` is T14's (§5.6's emit socket). `timeout_s` and
    `max_output_bytes` need a wrapper around `run_agent`'s `Popen` and have none, so a
    spec that sets them loads clean and bounds nothing.

    Said plainly because the alternative is an operator writing `timeout_s = 900`,
    getting no error, and believing there is a deadline.

    Types AND ranges are validated at load even though nothing reads the values yet, so
    the code that eventually applies them inherits a value it can trust.
    """
    timeout_s: float = 0.0            # 0 = no spec-imposed deadline
    max_output_bytes: int = 0         # 0 = unbounded by the spec
    max_events: int = 2000            # §5.6 response events per run


@dataclass(frozen=True)
class AgentSpec:
    """A loaded agent definition.

    Every field defaults to the Phase 2 behaviour, so `AgentSpec(name="default")` with
    no file is exactly what the runner did before this module existed.
    """
    name: str = "default"
    description: str = ""
    model: str = ""
    max_turns: int = DEFAULT_MAX_TURNS
    permission_mode: str = DEFAULT_PERMISSION_MODE
    allowed_tools: tuple[str, ...] = ()
    disallowed_tools: tuple[str, ...] = ()
    # Resolved to an absolute path at load time and read at build time, because
    # `--append-system-prompt` takes the prompt text, not a path.
    system_prompt: str = ""
    settings_path: str = ""
    mcp_config_path: str = ""
    skills: tuple[Skill, ...] = ()
    limits: Limits = field(default_factory=Limits)

    @property
    def is_default(self) -> bool:
        """True when this spec imposes nothing beyond Phase 2's argv."""
        return (not self.model and not self.allowed_tools and not self.disallowed_tools
                and not self.system_prompt and not self.settings_path
                and not self.mcp_config_path
                and self.permission_mode == DEFAULT_PERMISSION_MODE)


def _as_tuple(raw, field_name: str) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
        raise SpecError(f"{field_name} must be a list of strings")
    return tuple(x.strip() for x in raw if x.strip())


def _resolve_under(base: pathlib.Path, rel: str, field_name: str) -> str:
    """Resolve a spec-relative path, refusing anything that escapes the agent dir.

    A spec is operator-supplied so this is not the primary trust boundary, but
    `../../../etc/shadow` in `append_system_prompt_file` would read an arbitrary file
    into the prompt — and once T19 allows a *fetched* bundle to carry an `agent.toml`,
    that same field is attacker-supplied. Enforcing containment now means the gate
    already exists when the threat model changes, which is cheaper than remembering to
    add it then.
    """
    base = base.resolve()
    target = (base / rel).resolve()
    if target != base and base not in target.parents:
        raise SpecError(f"{field_name} {rel!r} escapes the agent directory")
    if not target.is_file():
        raise SpecError(f"{field_name} {rel!r} does not exist")
    return str(target)


def validate_name(name: str) -> str:
    if not name or len(name) > NAME_MAX or not NAME_RE.match(name):
        raise SpecError(
            f"agent name {name!r} must be a DNS-1123 label of at most {NAME_MAX} chars")
    return name


def parse(raw: str, *, name: str, base_dir: pathlib.Path | None = None) -> AgentSpec:
    """Parse one `agent.toml`. `base_dir` resolves the file-valued fields.

    Validation is strict and refuses rather than coercing: a spec is a *policy*, and a
    policy that silently drops the clause it could not understand is worse than one that
    fails to load. An unparseable `disallowed_tools` would otherwise hand a
    trigger-driven agent the Bash tool the operator meant to remove.
    """
    validate_name(name)
    try:
        d = tomllib.loads(raw)
    except tomllib.TOMLDecodeError as e:
        raise SpecError(f"agent {name!r}: invalid TOML: {e}") from e

    # The file may name itself, but the directory wins: the directory is what the
    # resolution order in `load` looks up, so a disagreement means a request for `x`
    # would run a spec calling itself `y`. Refusing names both.
    if (declared := d.get("name")) and declared != name:
        raise SpecError(f"agent {name!r}: spec declares name={declared!r}; they must match")

    mode = str(d.get("permission_mode", DEFAULT_PERMISSION_MODE))
    if mode not in PERMISSION_MODES:
        raise SpecError(
            f"agent {name!r}: permission_mode {mode!r} not one of {PERMISSION_MODES}")

    try:
        max_turns = int(d.get("max_turns", DEFAULT_MAX_TURNS))
    except (TypeError, ValueError) as e:
        raise SpecError(f"agent {name!r}: max_turns must be an integer") from e
    if max_turns < 1:
        raise SpecError(f"agent {name!r}: max_turns must be >= 1, got {max_turns}")

    allowed = _as_tuple(d.get("allowed_tools"), "allowed_tools")
    disallowed = _as_tuple(d.get("disallowed_tools"), "disallowed_tools")
    # Both lists naming one tool is a contradiction the CLI resolves by its own
    # precedence, which is not something a policy should depend on. The safe reading
    # (deny wins) would also silently differ from what the operator wrote, so refuse.
    if overlap := set(allowed) & set(disallowed):
        raise SpecError(
            f"agent {name!r}: {sorted(overlap)} in both allowed_tools and disallowed_tools")

    system_prompt = ""
    if rel := d.get("append_system_prompt_file"):
        if base_dir is None:
            raise SpecError(f"agent {name!r}: append_system_prompt_file needs a base_dir")
        system_prompt = pathlib.Path(
            _resolve_under(base_dir, str(rel), "append_system_prompt_file")
        ).read_text()

    settings_path = ""
    if rel := d.get("settings_file"):
        if base_dir is None:
            raise SpecError(f"agent {name!r}: settings_file needs a base_dir")
        settings_path = _resolve_under(base_dir, str(rel), "settings_file")

    mcp_config_path = ""
    if rel := (d.get("mcp", {}) or {}).get("config_file"):
        if base_dir is None:
            raise SpecError(f"agent {name!r}: mcp.config_file needs a base_dir")
        mcp_config_path = _resolve_under(base_dir, str(rel), "mcp.config_file")

    skills: list[Skill] = []
    for entry in d.get("skills", []) or []:
        if not isinstance(entry, dict):
            raise SpecError(f"agent {name!r}: each [[skills]] entry must be a table")
        s_name = str(entry.get("name", "")).strip()
        source = str(entry.get("source", "")).strip()
        if not s_name or not source:
            raise SpecError(f"agent {name!r}: a skill needs both name and source")
        sha = str(entry.get("sha256", "")).strip().lower()
        if sha and not re.fullmatch(r"[0-9a-f]{64}", sha):
            raise SpecError(f"agent {name!r}: skill {s_name!r} sha256 must be 64 hex chars")
        # §5.4 gate: a non-baked source without a digest can never be verified, so a
        # spec that omits it is rejected at load rather than at fetch. Stated as a
        # positive check on `image://` so a new scheme added later inherits the
        # requirement instead of bypassing it.
        if not source.startswith("image://") and not sha:
            raise SpecError(
                f"agent {name!r}: skill {s_name!r} has a fetched source and no sha256")
        skills.append(Skill(name=s_name, source=source, sha256=sha,
                            sig_kid=str(entry.get("sig_kid", "")).strip()))

    lim = d.get("limits", {}) or {}
    if not isinstance(lim, dict):
        raise SpecError(f"agent {name!r}: limits must be a table")
    # Wrapped for the same reason `max_turns` is, 50 lines up: these three conversions
    # raise ValueError/TypeError, and `run_agent` catches only `SpecError` — so
    # `timeout_s = "900s"` propagated out of `run_agent` instead of becoming the
    # `phase=error` event §5.1 promises. This block was the one place in `parse` that
    # escaped the module's own contract.
    try:
        limits = Limits(
            timeout_s=float(lim.get("timeout_s", 0.0)),
            max_output_bytes=int(lim.get("max_output_bytes", 0)),
            max_events=int(lim.get("max_events", Limits.max_events)),
        )
    except (TypeError, ValueError) as e:
        raise SpecError(f"agent {name!r}: limits must be numbers: {e}") from None
    # Ranges, not just types. The review left this as a noted gap on the grounds that
    # `Limits` is documented inert — but a negative deadline or a zero event budget is
    # nonsensical input whether or not anything reads it yet, and the module's own
    # standard is that a policy which cannot be understood fails to load. Rejecting now
    # also means the code that eventually wraps `Popen` inherits a value it can trust
    # rather than having to re-validate.
    if limits.timeout_s < 0:
        raise SpecError(f"agent {name!r}: limits.timeout_s must be >= 0 "
                        f"(0 means no spec-imposed deadline), got {limits.timeout_s}")
    if limits.max_output_bytes < 0:
        raise SpecError(f"agent {name!r}: limits.max_output_bytes must be >= 0 "
                        f"(0 means unbounded), got {limits.max_output_bytes}")
    if limits.max_events < 1:
        raise SpecError(f"agent {name!r}: limits.max_events must be >= 1 — a run that "
                        f"may emit no events cannot report its own result, got "
                        f"{limits.max_events}")

    return AgentSpec(
        name=name,
        description=str(d.get("description", "")),
        model=str(d.get("model", "")).strip(),
        max_turns=max_turns,
        permission_mode=mode,
        allowed_tools=allowed,
        disallowed_tools=disallowed,
        system_prompt=system_prompt,
        settings_path=settings_path,
        mcp_config_path=mcp_config_path,
        skills=tuple(skills),
        limits=limits,
    )


def load(agent_dir: str, name: str) -> AgentSpec:
    """Load `{agent_dir}/{name}/agent.toml`.

    `default` with no file on disk returns the built-in spec — Phase 2's behaviour —
    because the overwhelmingly common deployment bakes no agents at all and must keep
    working. Any OTHER missing name raises: §5.1 requires an error event rather than a
    silent fallback.
    """
    validate_name(name)
    base = pathlib.Path(agent_dir) / name
    path = base / SPEC_FILENAME
    if not path.is_file():
        if name == "default":
            return AgentSpec()
        raise SpecError(f"agent {name!r}: no {SPEC_FILENAME} under {base}")
    try:
        raw = path.read_text()
    except OSError as e:
        raise SpecError(f"agent {name!r}: cannot read {path}: {e}") from e
    return parse(raw, name=name, base_dir=base)


def resolve_name(*candidates: str | None, fallback: str = "default") -> str:
    """§5.1's resolution order, most specific first: the trigger's agent, then the
    registry user's, then `ER_AGENT_NAME`, then `default`.

    Written as a helper so the order lives in one place rather than being re-expressed
    as an `or` chain at each call site, where one site eventually gets it backwards.
    """
    for c in candidates:
        if c and c.strip():
            return c.strip()
    return fallback
