"""The approved-user registry and the `Caller` it produces. DESIGN_PHASE3.md §2.4, §2.5.

Two refusals carry most of the weight here, and both are startup refusals rather than
per-request errors:

* a recorded `userkey` that disagrees with this build's derivation (§2.5) — the
  alternative is a bridge that publishes into new, empty topics while every user's
  history appears to vanish;
* two identities colliding on one key (§2.3's 32-bit birthday bound) — the alternative
  is a silent cross-tenant leak.
"""
from __future__ import annotations

import json

import pytest

from eventbridge import auth, registry
from eventbridge.config import Cfg
from shared import tenancy


def _reg(users, version=1) -> str:
    return json.dumps({"version": version, "users": users})


def _gh(userid, **over):
    e = {"issuer": "github", "userid": userid,
         "userkey": tenancy.userkey("github", userid)}
    e.update(over)
    return e


# ---- parsing ---------------------------------------------------------------

def test_parse_a_full_registry():
    raw = _reg([
        {"issuer": "github", "userid": "mrsabath",
         "userkey": tenancy.userkey("github", "mrsabath"),
         "tier": "isolated", "agent": "triager",
         "limits": {"requests_per_hour": 100, "max_concurrent": 3,
                    "transcript_bytes": 268435456}},
        _gh("aslom", tier="isolated"),
        {"issuer": "static", "userid": "ops-break-glass",
         "userkey": tenancy.userkey("static", "ops-break-glass"), "tier": "shared"},
    ])
    r = registry.parse(raw)
    assert len(r) == 3
    u = r.lookup("github", "mrsabath")
    assert u is not None
    assert u.tier == "isolated" and u.isolated
    assert u.agent == "triager"
    assert u.limits.requests_per_hour == 100
    assert u.limits.max_concurrent == 3
    assert u.limits.transcript_bytes == 268435456
    assert not r.lookup("github", "ops-break-glass")


def test_an_empty_registry_denies_everyone():
    """Phase 2 §2.4's reading: the other one turns a missing file into an open door."""
    r = registry.parse(_reg([]))
    assert len(r) == 0
    assert r.lookup("github", "anyone") is None


def test_a_missing_users_key_is_an_empty_registry():
    assert len(registry.parse(json.dumps({"version": 1}))) == 0


def test_a_recorded_userkey_that_disagrees_is_a_startup_refusal():
    """§2.5's core check. The refusal must name BOTH values — an operator needs to know
    which one the live topics were named after."""
    raw = _reg([{"issuer": "github", "userid": "mrsabath",
                 "userkey": "gh-mrsabath-deadbeef"}])
    with pytest.raises(registry.RegistryError) as e:
        registry.parse(raw)
    msg = str(e.value)
    assert "gh-mrsabath-deadbeef" in msg
    assert tenancy.userkey("github", "mrsabath") in msg


def test_a_recorded_userkey_may_be_omitted():
    """An operator with ten logins should not have to compute digests by hand."""
    r = registry.parse(_reg([{"issuer": "github", "userid": "mrsabath"}]))
    assert r.users[0].userkey == tenancy.userkey("github", "mrsabath")


def test_two_identities_colliding_on_one_key_is_refused():
    """§2.3's 32-bit birthday bound, turned from a silent leak into a refused signup.

    Forced here by listing one identity twice, which produces the same collision the
    bound describes — the registry cannot tell the two cases apart and must refuse both.
    """
    raw = _reg([_gh("mrsabath"), _gh("MrSabath")])
    with pytest.raises(registry.RegistryError, match="collides with"):
        registry.parse(raw)


def test_an_unsupported_version_is_refused():
    """A v2 file may mean something different by a field this code thinks it knows."""
    for v in (0, 2, "1", None):
        with pytest.raises(registry.RegistryError, match="not supported"):
            registry.parse(_reg([], version=v))


def test_malformed_json_is_refused():
    with pytest.raises(registry.RegistryError, match="not valid JSON"):
        registry.parse("{not json")


def test_a_non_object_registry_is_refused():
    with pytest.raises(registry.RegistryError, match="must be a JSON object"):
        registry.parse("[]")


def test_an_entry_without_issuer_or_userid_is_refused():
    for e in ({"userid": "x"}, {"issuer": "github"}, {}):
        with pytest.raises(registry.RegistryError, match="needs both"):
            registry.parse(_reg([e]))


def test_an_unknown_tier_is_refused():
    with pytest.raises(registry.RegistryError, match="tier"):
        registry.parse(_reg([_gh("a", tier="platinum")]))


def test_non_integer_limits_are_refused():
    with pytest.raises(registry.RegistryError, match="integers"):
        registry.parse(_reg([_gh("a", limits={"max_concurrent": "lots"})]))


def test_a_non_object_entry_is_refused():
    with pytest.raises(registry.RegistryError, match="must be an object"):
        registry.parse(_reg(["mrsabath"]))


def test_an_entry_is_refused_rather_than_skipped():
    """Unlike `auth.parse_tokens`, which skips malformed entries on purpose. Here a
    skipped user is one whose topics exist and whose events have nowhere to go."""
    raw = _reg([_gh("good"), {"issuer": "github"}])
    with pytest.raises(registry.RegistryError):
        registry.parse(raw)


# ---- lookup ----------------------------------------------------------------

def test_lookup_inherits_per_issuer_canonicalisation():
    """Matching goes through `tenancy.userkey`, so case folding is not reimplemented."""
    r = registry.parse(_reg([_gh("mrsabath")]))
    for spelling in ("mrsabath", "MrSabath", "MRSABATH", " mrsabath "):
        assert r.lookup("github", spelling) is not None, spelling


def test_lookup_separates_issuers():
    r = registry.parse(_reg([_gh("alice")]))
    assert r.lookup("github", "alice") is not None
    assert r.lookup("oidc", "alice") is None
    assert r.lookup("static", "alice") is None


def test_lookup_of_a_static_identity_is_byte_exact():
    raw = _reg([{"issuer": "static", "userid": "Ops"}])
    r = registry.parse(raw)
    assert r.lookup("static", "Ops") is not None
    assert r.lookup("static", "ops") is None


def test_lookup_with_no_userid_is_none():
    r = registry.parse(_reg([_gh("alice")]))
    assert r.lookup("github", None) is None
    assert r.lookup(None, None) is None


def test_by_userkey():
    r = registry.parse(_reg([_gh("alice")]))
    key = tenancy.userkey("github", "alice")
    assert r.by_userkey(key).userid == "alice"
    assert r.by_userkey("gh-nobody-00000000") is None


def test_userkeys_property():
    r = registry.parse(_reg([_gh("alice"), _gh("bob")]))
    assert r.userkeys == (tenancy.userkey("github", "alice"),
                          tenancy.userkey("github", "bob"))


# ---- load ------------------------------------------------------------------

def test_load_an_empty_path_is_an_empty_registry():
    """Whether "nobody approved" is fatal depends on the tenancy mode, so `load` does
    not decide it — `__main__` does, where the mode is known."""
    assert len(registry.load("")) == 0


def test_load_a_missing_file_is_refused():
    with pytest.raises(registry.RegistryError, match="does not exist"):
        registry.load("/nonexistent/registry.json")


def test_load_reads_a_real_file(tmp_path):
    p = tmp_path / "registry.json"
    p.write_text(_reg([_gh("alice")]))
    assert len(registry.load(str(p))) == 1


# ---- Caller ----------------------------------------------------------------

def test_caller_for_single_mode_leaves_the_key_unset():
    """`ce_userkey` must stay off the wire entirely for a Phase 2 deployment."""
    c = auth.caller_for("mrsabath", "github", tenancy_mode=tenancy.SINGLE)
    assert c.userid == "mrsabath"
    assert c.issuer == "github"
    assert c.userkey is None
    assert not c.anonymous


def test_caller_for_multi_mode_derives_the_key():
    c = auth.caller_for("mrsabath", "github", tenancy_mode=tenancy.MULTI)
    assert c.userkey == tenancy.userkey("github", "mrsabath")


def test_an_anonymous_caller_carries_nothing():
    c = auth.caller_for(None, None, tenancy_mode=tenancy.MULTI)
    assert c.anonymous
    assert c.userid is None and c.issuer is None and c.userkey is None


def test_submitter_iss_stays_absent_for_a_static_identity():
    """Phase 2 §2.6 made an absent issuer mean "an operator typed this name". A static
    identity needs an issuer for its tenancy key to separate from a GitHub login of the
    same name, but promoting that onto the wire would upgrade an operator's assertion
    into a verified claim."""
    c = auth.caller_for("ops", None, tenancy_mode=tenancy.MULTI)
    assert c.submitter_iss is None
    assert c.userkey == tenancy.userkey("static", "ops")

    gh = auth.caller_for("alice", "github", tenancy_mode=tenancy.MULTI)
    assert gh.submitter_iss == "github"


def test_a_static_and_a_github_identity_of_one_name_are_two_tenants():
    """The §2.3 impersonation failure, asserted at the Caller level."""
    a = auth.caller_for("alice", "github", tenancy_mode=tenancy.MULTI)
    b = auth.caller_for("alice", None, tenancy_mode=tenancy.MULTI)
    assert a.userkey != b.userkey


# ---- resolve_caller --------------------------------------------------------

def _cfg(**over):
    c = Cfg()
    for k, v in over.items():
        setattr(c, k, v)
    return c


def _environ(token=None):
    return {"HTTP_AUTHORIZATION": f"Bearer {token}"} if token else {}


def test_resolve_caller_single_mode_is_phase2_anonymous():
    """With nothing configured: allowed and anonymous, which keeps the demo working."""
    caller, status, why = auth.resolve_caller(_environ(), _cfg())
    assert status is None and why is None
    assert caller.anonymous and caller.userkey is None


def test_resolve_caller_single_mode_with_a_static_token():
    cfg = _cfg(auth_tokens={"tok": "alice"})
    caller, status, _ = auth.resolve_caller(_environ("tok"), cfg)
    assert status is None
    assert caller.userid == "alice"
    assert caller.userkey is None          # single mode
    assert caller.submitter_iss is None    # static


def test_resolve_caller_multi_mode_refuses_anonymous_with_401():
    """§2.4: Phase 2's open default cannot coexist with per-user isolation — there is
    no user to isolate."""
    cfg = _cfg(tenancy_mode=tenancy.MULTI)
    caller, status, why = auth.resolve_caller(_environ(), cfg,
                                              registry=registry.Registry())
    assert caller is None
    assert status == 401
    assert "multi-tenant" in why


def test_resolve_caller_multi_mode_refuses_an_unregistered_user_with_403():
    """`403`, not `404`: this is "I know who you are and you are not approved", and
    naming the identity is what makes it actionable."""
    cfg = _cfg(tenancy_mode=tenancy.MULTI, auth_tokens={"tok": "bob"})
    reg = registry.parse(_reg([_gh("alice")]))
    caller, status, why = auth.resolve_caller(_environ("tok"), cfg, registry=reg)
    assert caller is None
    assert status == 403
    assert "bob" in why


def test_resolve_caller_multi_mode_accepts_a_registered_user():
    cfg = _cfg(tenancy_mode=tenancy.MULTI, auth_tokens={"tok": "ops"})
    reg = registry.parse(_reg([{"issuer": "static", "userid": "ops",
                                "tier": "isolated"}]))
    caller, status, _ = auth.resolve_caller(_environ("tok"), cfg, registry=reg)
    assert status is None
    assert caller.userid == "ops"
    assert caller.userkey == tenancy.userkey("static", "ops")
    assert caller.tier == "isolated"


def test_resolve_caller_multi_mode_uses_the_registrys_recorded_key():
    cfg = _cfg(tenancy_mode=tenancy.MULTI, auth_tokens={"tok": "ops"})
    reg = registry.parse(_reg([{"issuer": "static", "userid": "ops"}]))
    caller, _, _ = auth.resolve_caller(_environ("tok"), cfg, registry=reg)
    assert caller.userkey == reg.users[0].userkey


def test_resolve_caller_propagates_a_401_from_resolve():
    cfg = _cfg(tenancy_mode=tenancy.MULTI, auth_tokens={"tok": "ops"})
    caller, status, _ = auth.resolve_caller(_environ("wrong"), cfg,
                                            registry=registry.Registry())
    assert caller is None and status == 401
