"""Declarative agent definitions and the argv they produce. DESIGN_PHASE3.md §5.1-§5.5.

The load-bearing test here is `test_default_spec_reproduces_phase2_argv_exactly`. §5.1's
whole claim to being additive is that an unspecified field inherits Phase 2's behaviour,
and the only way to know that holds is to assert the argv against a literal copy of what
Phase 2 emitted rather than against the current implementation.
"""
from __future__ import annotations

import pathlib
import shutil
import subprocess

import pytest

from eventrunner import agentspec
from eventrunner.agentspec import AgentSpec, SpecError
from eventrunner.config import Cfg as ErCfg
from eventrunner.runner import build_cmd
from shared import ce


def _event(mode="start", corr="brave-otter-4718", **over):
    attrs = {"correlationid": corr, "sessionuuid": ce.session_uuid(corr), "mode": mode}
    attrs.update(over)
    return ce.CloudEvent(attrs=attrs)


def _spec_dir(tmp_path: pathlib.Path, name: str, toml: str, **files) -> pathlib.Path:
    d = tmp_path / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "agent.toml").write_text(toml)
    for fname, content in files.items():
        (d / fname.replace("__", ".")).write_text(content)
    return tmp_path


# ---- the additive guarantee -------------------------------------------------

def test_default_spec_reproduces_phase2_argv_exactly():
    """§5.1: the `default` agent must be byte-identical to Phase 2's `build_cmd`.

    The expected list is a literal transcription of the pre-Phase-3 implementation, NOT
    a call into the current one — otherwise this test would follow a regression instead
    of catching it.
    """
    cfg = ErCfg()
    corr = "brave-otter-4718"
    session = ce.session_uuid(corr)
    event = _event(corr=corr)
    expected = [
        cfg.claude_bin, "-p", "hello",
        "--output-format", "stream-json",
        "--verbose",
        "--max-turns", "3",
        "--permission-mode", "acceptEdits",
        "--session-id", session,
    ]
    assert build_cmd(cfg, event, {"prompt": "hello"}) == expected
    # Explicitly passing the built-in spec must be the same as passing none.
    assert build_cmd(cfg, event, {"prompt": "hello"}, spec=AgentSpec()) == expected
    # And so must loading `default` from a directory that has no file in it.
    loaded = agentspec.load("/nonexistent-agent-dir", "default")
    assert build_cmd(cfg, event, {"prompt": "hello"}, spec=loaded) == expected


def test_default_spec_with_model_and_max_turns_matches_phase2():
    cfg = ErCfg()
    corr = "brave-otter-4718"
    event = _event(corr=corr)
    cmd = build_cmd(cfg, event, {"prompt": "hi", "model": "sonnet", "max_turns": 9})
    assert cmd == [
        cfg.claude_bin, "-p", "hi",
        "--output-format", "stream-json",
        "--verbose",
        "--max-turns", "9",
        "--permission-mode", "acceptEdits",
        "--session-id", ce.session_uuid(corr),
        "--model", "sonnet",
    ]


def test_builtin_default_is_recognised_as_imposing_nothing():
    assert AgentSpec().is_default
    assert not AgentSpec(permission_mode="plan").is_default
    assert not AgentSpec(allowed_tools=("Read",)).is_default
    assert not AgentSpec(disallowed_tools=("Bash",)).is_default
    assert not AgentSpec(model="sonnet").is_default


# ---- argv from a real spec --------------------------------------------------

def test_build_cmd_emits_the_tool_policy():
    cfg = ErCfg()
    spec = AgentSpec(name="triager", model="sonnet", max_turns=8,
                     permission_mode="plan",
                     allowed_tools=("Read", "Grep", "Glob"),
                     disallowed_tools=("Bash", "Write"))
    cmd = build_cmd(cfg, _event(), {"prompt": "triage"}, spec=spec)
    assert cmd[cmd.index("--permission-mode") + 1] == "plan"
    assert cmd[cmd.index("--allowedTools") + 1] == "Read,Grep,Glob"
    assert cmd[cmd.index("--disallowedTools") + 1] == "Bash,Write"
    assert cmd[cmd.index("--model") + 1] == "sonnet"
    assert cmd[cmd.index("--max-turns") + 1] == "8"


def test_the_request_wins_on_max_turns_and_model_but_not_on_policy():
    """§5.5's precedence split. The per-turn parameters a caller has always been able to
    send keep working; the sandbox is spec-only, because a request that could widen the
    tool policy is a request that can escape it."""
    cfg = ErCfg()
    spec = AgentSpec(name="triager", model="haiku", max_turns=8,
                     permission_mode="plan", disallowed_tools=("Bash",))
    cmd = build_cmd(cfg, _event(), {"prompt": "x", "model": "opus", "max_turns": 2},
                    spec=spec)
    assert cmd[cmd.index("--model") + 1] == "opus"
    assert cmd[cmd.index("--max-turns") + 1] == "2"
    # The policy is untouched by anything in the payload.
    assert cmd[cmd.index("--permission-mode") + 1] == "plan"
    assert cmd[cmd.index("--disallowedTools") + 1] == "Bash"


def test_a_request_cannot_widen_the_tool_policy():
    """The payload keys an attacker would try. None of them reach argv."""
    cfg = ErCfg()
    spec = AgentSpec(name="t", permission_mode="plan", disallowed_tools=("Bash",))
    cmd = build_cmd(cfg, _event(), {
        "prompt": "x",
        "permission_mode": "bypassPermissions",
        "allowed_tools": ["Bash"],
        "allowedTools": ["Bash"],
        "disallowed_tools": [],
    }, spec=spec)
    assert "bypassPermissions" not in cmd
    assert cmd[cmd.index("--permission-mode") + 1] == "plan"
    assert cmd[cmd.index("--disallowedTools") + 1] == "Bash"
    assert "--allowedTools" not in cmd


def test_spec_max_turns_applies_when_the_request_omits_it():
    cfg = ErCfg()
    cmd = build_cmd(cfg, _event(), {"prompt": "x"}, spec=AgentSpec(max_turns=12))
    assert cmd[cmd.index("--max-turns") + 1] == "12"


def test_build_cmd_emits_system_prompt_settings_and_mcp():
    cfg = ErCfg()
    spec = AgentSpec(name="t", system_prompt="be terse",
                     settings_path="/etc/rossoctl/agents/t/settings.json",
                     mcp_config_path="/etc/rossoctl/agents/t/.mcp.json")
    cmd = build_cmd(cfg, _event(), {"prompt": "x"}, spec=spec)
    assert cmd[cmd.index("--append-system-prompt") + 1] == "be terse"
    assert cmd[cmd.index("--settings") + 1] == "/etc/rossoctl/agents/t/settings.json"
    assert cmd[cmd.index("--mcp-config") + 1] == "/etc/rossoctl/agents/t/.mcp.json"


def test_the_prompt_is_one_argv_element_however_hostile():
    """§5.5: the prompt goes through argv, never a shell. A prompt that would be a
    command injection in a shell string must stay a single element here."""
    cfg = ErCfg()
    nasty = "look; rm -rf / && echo $(whoami) `id` | tee /tmp/x"
    cmd = build_cmd(cfg, _event(), {"prompt": nasty}, spec=AgentSpec())
    assert cmd.count(nasty) == 1
    assert cmd[cmd.index("-p") + 1] == nasty


def test_resume_still_works_with_a_spec():
    cfg = ErCfg()
    spec = AgentSpec(name="t", permission_mode="plan")
    cmd = build_cmd(cfg, _event(mode="continue"), {"prompt": "go on"},
                    resume_target="/data/x.jsonl", spec=spec)
    assert cmd[cmd.index("--resume") + 1] == "/data/x.jsonl"
    assert "--session-id" not in cmd


# ---- parsing ---------------------------------------------------------------

TRIAGER = """
name        = "triager"
description = "Triage an incoming issue."
model       = "sonnet"
max_turns   = 8
permission_mode  = "plan"
allowed_tools    = ["Read", "Grep", "Glob", "Skill"]
disallowed_tools = ["Bash", "Write", "Edit"]
append_system_prompt_file = "system.md"

[[skills]]
name   = "eventbridge"
source = "image:///etc/rossoctl/agents/triager/skills/eventbridge"

[limits]
timeout_s        = 900
max_output_bytes = 4000000
max_events       = 1000
"""


def test_parse_a_full_spec(tmp_path):
    root = _spec_dir(tmp_path, "triager", TRIAGER, system__md="Be terse.\n")
    spec = agentspec.load(str(root), "triager")
    assert spec.name == "triager"
    assert spec.model == "sonnet"
    assert spec.max_turns == 8
    assert spec.permission_mode == "plan"
    assert spec.allowed_tools == ("Read", "Grep", "Glob", "Skill")
    assert spec.disallowed_tools == ("Bash", "Write", "Edit")
    assert spec.system_prompt == "Be terse.\n"
    assert len(spec.skills) == 1 and spec.skills[0].is_baked
    assert spec.limits.timeout_s == 900
    assert spec.limits.max_events == 1000
    assert not spec.is_default


def test_an_empty_spec_is_the_phase2_default(tmp_path):
    root = _spec_dir(tmp_path, "bare", "")
    spec = agentspec.load(str(root), "bare")
    assert spec.is_default
    assert spec.permission_mode == "acceptEdits"
    assert spec.max_turns == 3


def test_a_missing_non_default_agent_raises(tmp_path):
    """§5.1: an error event, never a silent fallback to `default`."""
    with pytest.raises(SpecError, match="no agent.toml"):
        agentspec.load(str(tmp_path), "nope")


def test_a_missing_default_agent_is_the_builtin(tmp_path):
    """The common deployment bakes no agents at all and must keep working."""
    assert agentspec.load(str(tmp_path), "default").is_default


@pytest.mark.parametrize("name", [
    "", "Triager", "tri_ager", "-triager", "triager-", "tri.ager",
    "../etc", "a/b", "x" * 64,
])
def test_invalid_agent_names_are_refused(tmp_path, name):
    with pytest.raises(SpecError):
        agentspec.load(str(tmp_path), name)


def test_a_spec_whose_declared_name_disagrees_is_refused(tmp_path):
    root = _spec_dir(tmp_path, "triager", 'name = "something-else"\n')
    with pytest.raises(SpecError, match="must match"):
        agentspec.load(str(root), "triager")


def test_invalid_toml_is_refused(tmp_path):
    root = _spec_dir(tmp_path, "broken", "this is not = = toml\n")
    with pytest.raises(SpecError, match="invalid TOML"):
        agentspec.load(str(root), "broken")


@pytest.mark.parametrize("mode", ["", "yolo", "AcceptEdits", "accept-edits"])
def test_an_unknown_permission_mode_is_refused(tmp_path, mode):
    root = _spec_dir(tmp_path, "a", f'permission_mode = "{mode}"\n')
    with pytest.raises(SpecError, match="permission_mode"):
        agentspec.load(str(root), "a")


@pytest.mark.parametrize("value", ["0", "-1"])
def test_a_nonsensical_max_turns_is_refused(tmp_path, value):
    root = _spec_dir(tmp_path, "a", f"max_turns = {value}\n")
    with pytest.raises(SpecError, match="max_turns"):
        agentspec.load(str(root), "a")


def test_a_non_list_tool_policy_is_refused(tmp_path):
    """A policy that silently drops the clause it could not parse would hand a
    trigger-driven agent the tool the operator meant to remove."""
    root = _spec_dir(tmp_path, "a", 'disallowed_tools = "Bash"\n')
    with pytest.raises(SpecError, match="list of strings"):
        agentspec.load(str(root), "a")


def test_a_tool_in_both_lists_is_refused(tmp_path):
    root = _spec_dir(tmp_path, "a",
                     'allowed_tools = ["Read", "Bash"]\ndisallowed_tools = ["Bash"]\n')
    with pytest.raises(SpecError, match="both allowed_tools and disallowed_tools"):
        agentspec.load(str(root), "a")


def test_a_system_prompt_file_outside_the_agent_dir_is_refused(tmp_path):
    (tmp_path / "secret.txt").write_text("sensitive")
    root = _spec_dir(tmp_path, "a", 'append_system_prompt_file = "../secret.txt"\n')
    with pytest.raises(SpecError, match="escapes the agent directory"):
        agentspec.load(str(root), "a")


def test_an_absolute_system_prompt_path_is_refused(tmp_path):
    root = _spec_dir(tmp_path, "a", 'append_system_prompt_file = "/etc/hosts"\n')
    with pytest.raises(SpecError, match="escapes the agent directory"):
        agentspec.load(str(root), "a")


def test_a_missing_system_prompt_file_is_refused(tmp_path):
    root = _spec_dir(tmp_path, "a", 'append_system_prompt_file = "absent.md"\n')
    with pytest.raises(SpecError, match="does not exist"):
        agentspec.load(str(root), "a")


# ---- skills ----------------------------------------------------------------

def test_a_fetched_skill_without_a_digest_is_refused(tmp_path):
    """§5.4: a bundle reference that could never verify fails at load, not at fetch."""
    root = _spec_dir(tmp_path, "a", """
[[skills]]
name   = "triage"
source = "github://rossoctl/skills@9f1c0ab3d2e4f5061728394a5b6c7d8e9f012345//triage"
""")
    with pytest.raises(SpecError, match="fetched source and no sha256"):
        agentspec.load(str(root), "a")


def test_a_baked_skill_needs_no_digest(tmp_path):
    root = _spec_dir(tmp_path, "a", """
[[skills]]
name   = "eventbridge"
source = "image:///etc/rossoctl/agents/a/skills/eventbridge"
""")
    spec = agentspec.load(str(root), "a")
    assert spec.skills[0].is_baked and not spec.skills[0].sha256


def test_a_malformed_digest_is_refused(tmp_path):
    root = _spec_dir(tmp_path, "a", """
[[skills]]
name   = "t"
source = "https://example.com/b.tar.gz"
sha256 = "not-a-digest"
""")
    with pytest.raises(SpecError, match="64 hex"):
        agentspec.load(str(root), "a")


def test_a_skill_missing_name_or_source_is_refused(tmp_path):
    root = _spec_dir(tmp_path, "a", '[[skills]]\nname = "t"\n')
    with pytest.raises(SpecError, match="both name and source"):
        agentspec.load(str(root), "a")


# ---- resolution order ------------------------------------------------------

def test_resolve_name_prefers_the_most_specific():
    assert agentspec.resolve_name("from-trigger", "from-env") == "from-trigger"
    assert agentspec.resolve_name(None, "from-env") == "from-env"
    assert agentspec.resolve_name(None, "") == "default"
    assert agentspec.resolve_name("  ", None) == "default"
    assert agentspec.resolve_name("  spaced  ", None) == "spaced"


# ---- the pinned CLI --------------------------------------------------------

# Every NEW flag Phase 3 teaches `build_cmd` to emit. §5.5 wants a flag that vanishes in
# a CLI upgrade to fail a build rather than a production run, and this is that check.
#
# Deliberately excludes the flags Phase 2 already shipped (`-p`, `--output-format`,
# `--verbose`, `--max-turns`, `--permission-mode`, `--session-id`, `--resume`,
# `--model`). Those are proven by the e2e path on the pinned CLI, and at least one of
# them (`--max-turns`) is *supported but undocumented* in `--help` on 2.1.270 — so
# asserting them here would fail on a working deployment, which is a worse outcome than
# not asserting them. Scope the check to what Phase 3 adds, where a typo is a real risk.
EMITTED_FLAGS = [
    "--allowedTools", "--disallowedTools",
    "--append-system-prompt", "--settings", "--mcp-config",
]


@pytest.mark.skipif(shutil.which("claude") is None,
                    reason="claude CLI not on PATH; the image build runs this")
def test_every_new_flag_build_cmd_emits_exists_in_the_pinned_cli():
    out = subprocess.run(["claude", "--help"], capture_output=True, text=True,
                         timeout=60).stdout
    missing = [f for f in EMITTED_FLAGS if f not in out]
    assert not missing, f"claude --help does not mention {missing}"
