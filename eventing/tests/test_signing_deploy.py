"""Key generation and the deploy-time key material. §11.

Signing was wired in #879 but could not be switched on in a cluster: nothing generated
keys and no Deployment mounted any. These cover the two pieces that close that, and the
properties worth pinning are mostly about what must NOT happen — a seed must not be
printed, must not be world-readable, must not be shared between the two services, and a
half-configured key directory must not reach a cluster that is enforcing.
"""
from __future__ import annotations

import json
import pathlib
import stat
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "scripts"))

import gen_signing_keys as G  # noqa: E402
from k8s_deploy import ensure_signing_material  # noqa: E402
from proclib import Checks, Result  # noqa: E402

from shared import ce, keyset  # noqa: E402
from shared import signing as S  # noqa: E402

# ---- generation -------------------------------------------------------------

def test_generate_makes_one_bridge_key_and_the_asked_for_runners():
    keys = G.generate(3)
    assert sorted(keys) == ["eb-01", "runner-01", "runner-02", "runner-03"]
    assert all(len(s) == 32 for s in keys.values()), "an Ed25519 seed is 32 bytes"
    assert len(set(keys.values())) == 4, "every seed must be distinct"


def test_the_bridge_and_runner_never_share_a_seed():
    """The property the whole design rests on. One shared seed would mean EventBridge
    holds the key it verifies runner responses with, so it could forge any agent's
    answer — which is exactly why HMAC was rejected (DESIGN_PHASE2.md §4.3)."""
    keys = G.generate(1)
    assert keys["eb-01"] != keys["runner-01"]


def test_the_keyset_holds_public_keys_derived_from_the_seeds(tmp_path):
    """Derived, never typed: a keyset that disagrees with the keys it authorizes shows
    up as "signing is broken" with nothing pointing at the key set."""
    keys = G.generate(2)
    ks = json.loads(G.keyset_json(keys))
    assert sorted(ks) == sorted(keys)
    for kid, seed in keys.items():
        assert ks[kid] == S.public_key(seed).hex()
        assert ks[kid] != seed.hex(), "a seed must never land in the keyset"


def test_the_generated_keyset_loads_and_authorizes_its_own_signatures(tmp_path):
    """End to end through the real verification path, not just shape checks."""
    keys = G.generate(1)
    G.write_seeds(tmp_path, keys)
    (tmp_path / "agents.json").write_text(G.keyset_json(keys))
    ks = keyset.load(str(tmp_path / "agents.json"))

    seed = S.load_seed(str(tmp_path / "runner-01" / "seed.hex"))
    assert seed == keys["runner-01"], "load_seed must read back what was written"
    evt = ce.new_event(type=ce.TYPE_RESPONSE, source="rossoctl://er/test",
                       datacontenttype="application/json", correlationid="c",
                       sessionuuid="s", sequence=1, phase="result", final="true",
                       data={"text": "hi"})
    S.sign_into(evt, seed, "runner-01")
    back = ce.from_kafka_binary(*ce.to_kafka_binary(evt))
    ok, why = S.verify_with_keyset(back, ks)
    assert ok, why


def test_seeds_are_written_unreadable_by_anyone_else(tmp_path):
    """A seed written world-readable is the control gone, and it is invisible from the
    outside — nothing fails, the key is just no longer private."""
    keys = G.generate(1)
    paths = G.write_seeds(tmp_path, keys)
    assert (stat.S_IMODE(tmp_path.stat().st_mode) & 0o077) == 0, \
        "the key directory must not be group- or world-accessible"
    for p in paths:
        mode = stat.S_IMODE(p.stat().st_mode)
        assert mode == 0o600, f"{p} is {oct(mode)}, must be 0600"


def test_regenerating_refuses_to_clobber_a_deployed_key(tmp_path):
    """An existing seed may already be deployed and in an approved keyset. Silently
    replacing it revokes a working identity with no indication why."""
    keys = G.generate(1)
    G.write_seeds(tmp_path, keys)
    with pytest.raises(SystemExit, match="refusing to overwrite"):
        G.write_seeds(tmp_path, G.generate(1))
    # --force is the deliberate path.
    rotated = G.generate(1)
    G.write_seeds(tmp_path, rotated, force=True)
    assert S.load_seed(str(tmp_path / "eb-01" / "seed.hex")) == rotated["eb-01"]


def test_the_cli_never_prints_a_seed(tmp_path, capsys):
    """stdout and stderr both. A terminal transcript pasted into an issue is the most
    likely way a demo key escapes."""
    rc = G.main(["--out", str(tmp_path), "--runners", "2"])
    assert rc == 0
    out = capsys.readouterr()
    printed = out.out + out.err
    for kid in ("eb-01", "runner-01", "runner-02"):
        seed_hex = S.load_seed(str(tmp_path / kid / "seed.hex")).hex()
        assert seed_hex not in printed, f"{kid}'s seed was printed"
        assert S.public_key(bytes.fromhex(seed_hex)).hex() in printed, \
            f"{kid}'s public key should be shown"


def test_print_keyset_alone_writes_nothing(tmp_path, capsys):
    rc = G.main(["--print-keyset"])
    assert rc == 0
    ks = json.loads(capsys.readouterr().out)
    assert sorted(ks) == ["eb-01", "runner-01"]
    assert list(tmp_path.iterdir()) == [], "no files should be written"


# ---- deploy-time application ------------------------------------------------

class _FakeKubectl:
    """Records what would be applied. `get` returns None — nothing pre-exists."""

    def __init__(self):
        self.applied: list[dict] = []

    def get(self, kind, name, namespace=None, missing_ok=False):
        return None

    def apply_stdin(self, body, namespace=None):
        self.applied.append(json.loads(body))
        return Result(argv=["kubectl", "apply"], rc=0, out="configured", err="",
                      duration_s=0.01, launched=True, timed_out=False)


def _keydir(tmp_path, *, runners=1):
    keys = G.generate(runners)
    G.write_seeds(tmp_path, keys)
    (tmp_path / "agents.json").write_text(G.keyset_json(keys))
    return keys


def _apply(keydir, **kw):
    k, c = _FakeKubectl(), Checks(prefix="test")
    ensure_signing_material(k, c, "kev1", keydir, dry_run=kw.get("dry_run", False))
    return k, c


def test_a_valid_key_directory_applies_two_secrets_and_one_configmap(tmp_path):
    keys = _keydir(tmp_path)
    k, c = _apply(str(tmp_path))
    assert c.failed == 0
    names = [(m["kind"], m["metadata"]["name"]) for m in k.applied]
    assert names == [("Secret", "eventbridge-signing-key"),
                     ("Secret", "eventrunner-signing-key"),
                     ("ConfigMap", "eventing-keyset")]
    secrets = [m for m in k.applied if m["kind"] == "Secret"]
    # The data key is the file name the mount and *_SIGNING_KEY_PATH both assume.
    assert all(list(m["stringData"]) == ["seed.hex"] for m in secrets)
    assert (secrets[0]["stringData"]["seed.hex"]
            != secrets[1]["stringData"]["seed.hex"]), \
        "the two services must be given different seeds"
    assert secrets[0]["stringData"]["seed.hex"].strip() == keys["eb-01"].hex()


def test_the_applied_configmap_holds_only_public_keys(tmp_path):
    keys = _keydir(tmp_path)
    k, _ = _apply(str(tmp_path))
    cm = [m for m in k.applied if m["kind"] == "ConfigMap"][0]
    ks = json.loads(cm["data"]["agents.json"])
    assert sorted(ks) == sorted(keys)
    for kid, seed in keys.items():
        assert ks[kid] == S.public_key(seed).hex()
        assert seed.hex() not in json.dumps(cm), "a seed leaked into the ConfigMap"


def test_without_a_key_directory_nothing_is_applied(tmp_path):
    """A redeploy must not silently switch enforcement off and strand a signed topic."""
    k, c = _apply(None)
    assert c.failed == 0 and k.applied == []


def test_a_missing_key_directory_fails_with_the_command_to_run(tmp_path, capsys):
    """`Checks.failures` keeps only the description; the actionable detail is printed.
    Assert on the output, since that is what the operator actually sees."""
    k, c = _apply(str(tmp_path / "absent"))
    assert c.failed == 1 and k.applied == []
    assert "gen_signing_keys.py" in capsys.readouterr().out, \
        "the failure should name the command that fixes it"


def test_an_empty_keyset_is_refused(tmp_path):
    """`{}` is a valid keyset that approves nobody — deployed under enforcement it
    rejects every event, which reads as a broken system rather than a config error."""
    (tmp_path / "agents.json").write_text("{}")
    k, c = _apply(str(tmp_path))
    assert c.failed == 1 and k.applied == []


def test_a_malformed_keyset_is_refused_before_it_reaches_the_cluster(tmp_path):
    (tmp_path / "agents.json").write_text('{"runner-01": "not-a-key"}')
    k, c = _apply(str(tmp_path))
    assert c.failed == 1 and k.applied == []


def test_a_kid_missing_from_the_approved_set_is_refused(tmp_path):
    """The service would start, sign under that kid, and have every event refused by
    the other side — a failure with no local symptom."""
    _keydir(tmp_path)
    (tmp_path / "agents.json").write_text(json.dumps({"someone-else": "ab" * 32}))
    k, c = _apply(str(tmp_path))
    assert c.failed == 1 and k.applied == []


def test_a_missing_seed_file_is_refused(tmp_path):
    keys = _keydir(tmp_path)
    (tmp_path / "runner-01" / "seed.hex").unlink()
    k, c = _apply(str(tmp_path))
    assert c.failed == 1
    # The bridge's Secret may already have been applied; the runner's must not be.
    assert not any(m["metadata"]["name"] == "eventrunner-signing-key"
                   for m in k.applied)
    assert keys  # keys were generated; the point is the deploy stopped


def test_dry_run_applies_nothing(tmp_path):
    _keydir(tmp_path)
    k, c = _apply(str(tmp_path), dry_run=True)
    assert c.failed == 0 and k.applied == []
