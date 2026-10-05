"""Regressions for the five must-fixes in the #883 review.

Each test fails without its fix. They live together because they share a theme worth
naming: **every one was invisible to a suite of 192 test functions**, and for the same
reason — the fixtures assembled the collaborators by hand with the arguments the
production call sites fail to supply. So each test here drives the real entry point
(`Handlers`, `__main__`'s wiring order, `SIGNED_ATTRS`) rather than the piece underneath.
"""
from __future__ import annotations

import io
import json
import pathlib
import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from eventbridge import auth, registry
from eventbridge.config import Cfg
from eventbridge.correlation import Minter
from eventbridge.group_service import GroupService
from eventbridge.handlers import Handlers
from eventbridge.kafka_out import Producer
from eventbridge.owner_index import OwnerIndex
from eventbridge.store import Store
from eventbridge.store_registry import StoreRegistry
from shared import ce, keyset, tenancy
from shared import signing as S

# These tests authenticate with static bearer tokens (`EB_AUTH_TOKENS`), which resolve
# to issuer `static` — so the registry lists the users under that issuer. Using `github`
# here would be a 403, correctly: §2.3 keeps a static `alice` and a GitHub `alice` as two
# different tenants on purpose.
OWNER = "alice"
UK = tenancy.userkey(auth.ISSUER_STATIC, OWNER)
OTHER = tenancy.userkey(auth.ISSUER_STATIC, "bob")


def _api(tmp_path: pathlib.Path, *, multi=True, real_producer=False):
    """Handlers wired as `__main__` wires it, so the tests exercise the real paths."""
    cfg = Cfg()
    cfg.tmpdir = str(tmp_path)
    cfg.tenancy_mode = tenancy.MULTI if multi else tenancy.SINGLE
    root = tmp_path / "eventbridge"
    owners = OwnerIndex(root) if multi else None
    stores = StoreRegistry(root, multi=multi)
    store = stores.for_userkey(None)
    minter = Minter(index=owners)
    if real_producer:
        with patch("eventbridge.kafka_out.KafkaProducer") as kp:
            kp.return_value = MagicMock()
            producer = Producer("b", "requests", "src",
                                response_topic="responses", topics=cfg.topics)
    else:
        producer = MagicMock()
        producer.publish_request.return_value = "evt-1"
    groups = GroupService(cfg, store, producer, minter,
                          stores=stores if multi else None)
    reg = registry.parse(json.dumps({
        "version": 1,
        "users": [{"issuer": auth.ISSUER_STATIC, "userid": u, "tier": "isolated"}
                  for u in (OWNER, "bob")]})) if multi else None
    h = Handlers(cfg, store, producer, minter, groups=groups, registry=reg,
                 stores=stores if multi else None, owners=owners)
    return h, producer, owners, stores


class _Start:
    def __init__(self):
        self.status = None

    def __call__(self, status, headers):
        self.status = status


def _env(body=None, *, token=None, form=False):
    if form:
        raw = "&".join(f"{k}={v}" for k, v in (body or {}).items()).encode()
        ctype = "application/x-www-form-urlencoded"
    else:
        raw = json.dumps(body or {}).encode()
        ctype = "application/json"
    e = {"wsgi.input": io.BytesIO(raw), "CONTENT_LENGTH": str(len(raw)),
         "CONTENT_TYPE": ctype, "REQUEST_METHOD": "POST", "QUERY_STRING": ""}
    if token:
        e["HTTP_AUTHORIZATION"] = f"Bearer {token}"
    return e


def _as(h, userid):
    """Authenticate as a registry user, via a static token."""
    h.cfg.auth_tokens = {f"tok-{userid}": userid}
    return f"tok-{userid}"


# ---- 1. the single-mode index seeding regression --------------------------

def test_the_index_is_seeded_from_an_existing_store(tmp_path):
    """Regression, and the serious one: a `single`-mode regression against the exact
    guarantee the phase claims.

    On the first start after an upgrade `owners.sqlite` is new and empty while
    `sessions.sqlite` is full, so `exists()` reported live ids as free and `Minter` could
    reissue one — `upsert_session` then overwrites that session and the new prompt
    appends to somebody's existing conversation. Phase 2's odds were zero, because it
    seeded its `seen` set from the store.
    """
    root = tmp_path / "eventbridge"
    store = Store(root)
    pre = Minter(seed=1)
    live = []
    for _ in range(40):
        c = pre.mint()
        store.upsert_session(c, "u", "/w", "an existing conversation")
        live.append(c)

    owners = OwnerIndex(root)
    assert owners.count() == 0, "a fresh index starts empty — the precondition"
    seeded = owners.seed_from(store.all_correlations(limit=10000))
    assert seeded == 40
    assert not [c for c in live if not owners.exists(c)], \
        "a live correlation is still reported free, so Minter can reissue it"


def test_a_seeded_scope_is_not_seeded_again(tmp_path):
    """Round 2. §6.1a claims the backfill is one-time; the first implementation re-opened
    every tenant store and re-scanned `all_correlations` on EVERY boot — the per-start
    N-store seeding §2.6 rejects, reintroduced by the fix for the missing seed.

    `needs_seeding` is what lets `__main__` skip the store open entirely. Measured effect
    at 100 tenants x 200 correlations: 210 ms -> 0.8 ms per restart, 101 store opens -> 1.
    """
    owners = OwnerIndex(tmp_path / "eventbridge")
    assert owners.needs_seeding()
    owners.seed_from(["brave-otter-0001"])
    assert not owners.needs_seeding(), "the shared scope was not marked seeded"


def test_seed_scopes_are_tracked_independently(tmp_path):
    """A seeded tenant must not mark another, or a new tenant's existing ids would stay
    invisible to the uniqueness check."""
    owners = OwnerIndex(tmp_path / "eventbridge")
    owners.seed_from(["brave-otter-0001"], UK)
    assert not owners.needs_seeding(UK)
    assert owners.needs_seeding(OTHER)
    assert owners.needs_seeding()          # the shared scope is its own


def test_an_empty_store_is_still_marked_seeded(tmp_path):
    """Otherwise it is re-opened and re-scanned on every boot forever."""
    owners = OwnerIndex(tmp_path / "eventbridge")
    assert owners.seed_from([]) == 0
    assert not owners.needs_seeding()


def test_the_seed_mark_survives_a_restart(tmp_path):
    root = tmp_path / "eventbridge"
    one = OwnerIndex(root)
    one.seed_from(["brave-otter-0001"], UK)
    one.close()
    assert not OwnerIndex(root).needs_seeding(UK)


def test_seeding_is_idempotent_across_restarts(tmp_path):
    root = tmp_path / "eventbridge"
    store = Store(root)
    store.upsert_session("brave-otter-0001", "u", "/w", "hi")
    owners = OwnerIndex(root)
    assert owners.seed_from(store.all_correlations()) == 1
    assert owners.seed_from(store.all_correlations()) == 0


def test_seeding_does_not_steal_an_existing_owner(tmp_path):
    """A correlation a tenant already claimed keeps its owner when the store is seeded."""
    root = tmp_path / "eventbridge"
    owners = OwnerIndex(root)
    owners.claim("brave-otter-0001", UK)
    owners.seed_from(["brave-otter-0001"], None)
    assert owners.owner_of("brave-otter-0001") == (UK, True)


def test_claim_raises_collision_not_integrityerror(tmp_path):
    """`Minter.mint` catches `Collision` specifically, so a conflict presenting as
    `sqlite3.IntegrityError` would surface a 500 rather than retrying. The database is
    the authority now, not the in-process lock."""
    from eventbridge.owner_index import Collision
    owners = OwnerIndex(tmp_path)
    owners.claim("brave-otter-0001", UK)
    with pytest.raises(Collision):
        owners.claim("brave-otter-0001", OTHER)


# ---- 2. create_group did not pass the userkey ----------------------------

def test_create_group_puts_the_group_in_the_callers_store(tmp_path):
    """Regression. `submit_members` got the userkey and `groups.create` did not, so the
    group row landed in the shared store while members landed in the tenant's: the batch
    reported 0 members forever and never completed.

    This drives `Handlers.create_group`, which is what no existing test did — every
    multi-mode group test called `GroupService.create` directly WITH the userkey.
    """
    h, _, owners, stores = _api(tmp_path)
    tok = _as(h, OWNER)
    sr = _Start()
    out = h.create_group(_env({"prompts": ["p1", "p2"]}, token=tok), sr)
    assert sr.status.startswith("202"), sr.status
    gid = json.loads(b"".join(out))["groupid"]

    assert stores.for_userkey(UK).get_group(gid) is not None, \
        "group row is not in the caller's store"
    assert stores.for_userkey(None).get_group(gid) is None, \
        "group row leaked into the shared store"
    assert owners.owner_of(gid) == (UK, True)
    assert len(stores.for_userkey(UK).group_members(gid)) == 2


def test_a_retried_create_group_returns_its_members(tmp_path):
    """The mirror image: `not created` read the tenant store `create` never wrote, so an
    Idempotency-Key retry answered `members: []` — worse than a second batch, because it
    looks like success."""
    h, _, _, _ = _api(tmp_path)
    tok = _as(h, OWNER)
    e1 = _env({"prompts": ["p1", "p2"]}, token=tok)
    e1["HTTP_IDEMPOTENCY_KEY"] = "key-1"
    first = json.loads(b"".join(h.create_group(e1, _Start())))
    e2 = _env({"prompts": ["p1", "p2"]}, token=tok)
    e2["HTTP_IDEMPOTENCY_KEY"] = "key-1"
    second = json.loads(b"".join(h.create_group(e2, _Start())))
    assert second["created"] is False
    assert second["groupid"] == first["groupid"]
    # Compared as sets: `snapshot` does not promise member order, and the bug was that
    # the retry returned NOTHING — an empty list where the original had two members.
    assert set(second["members"]) == set(first["members"])
    assert second["members"], "a retry returned no members at all"


# ---- 3. close/cancel were unauthenticated cross-tenant mutations ---------

@pytest.mark.parametrize("route", ["close_group", "cancel_group"])
def test_group_mutation_requires_authentication(tmp_path, route):
    """Regression. An unauthenticated POST resolved any tenant's group from the global
    index and mutated it. T7 covers reads staying open; Phase 2 had no cross-tenant
    mutation anywhere, so this was a capability being introduced."""
    h, _, _, stores = _api(tmp_path)
    tok = _as(h, OWNER)
    gid = json.loads(b"".join(
        h.create_group(_env({"prompts": ["p1"]}, token=tok), _Start())))["groupid"]

    sr = _Start()
    getattr(h, route)(_env(), sr, groupid=gid)          # no credential
    assert sr.status.startswith("401"), sr.status
    assert not stores.for_userkey(UK).get_group(gid)["completed_utc"]


@pytest.mark.parametrize("route", ["close_group", "cancel_group"])
def test_group_mutation_is_404_for_another_tenants_group(tmp_path, route):
    """§6.2's rule: "exists but not yours" must be indistinguishable from "does not
    exist", or anyone can enumerate live ids across tenants."""
    h, _, _, _ = _api(tmp_path)
    gid = json.loads(b"".join(h.create_group(
        _env({"prompts": ["p1"]}, token=_as(h, OWNER)), _Start())))["groupid"]

    h.cfg.auth_tokens = {"tok-alice": OWNER, "tok-bob": "bob"}
    sr = _Start()
    getattr(h, route)(_env(token="tok-bob"), sr, groupid=gid)
    assert sr.status.startswith("404"), sr.status


@pytest.mark.parametrize("route", ["close_group", "cancel_group"])
def test_group_mutation_works_for_the_owner(tmp_path, route):
    h, _, _, _ = _api(tmp_path)
    tok = _as(h, OWNER)
    gid = json.loads(b"".join(
        h.create_group(_env({"prompts": ["p1"]}, token=tok), _Start())))["groupid"]
    sr = _Start()
    getattr(h, route)(_env(token=tok), sr, groupid=gid)
    assert sr.status.startswith("200"), sr.status


@pytest.mark.parametrize("route", ["close_group", "cancel_group"])
def test_single_mode_group_mutation_stays_open(tmp_path, route):
    """Phase 2's behaviour is unchanged: no tenants, nothing to authorize."""
    h, _, _, _ = _api(tmp_path, multi=False)
    gid, _created = h.groups.create(label="b", expected=1)
    sr = _Start()
    getattr(h, route)(_env(), sr, groupid=gid)
    assert sr.status.startswith("200"), sr.status


# ---- 4. `agent` was outside SIGNED_ATTRS ---------------------------------

SEED = bytes.fromhex(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")


def test_agent_is_signed(tmp_path):
    """Regression. `ce_agent` selects the AgentSpec that supplies `--permission-mode`
    and the tool allowlists — §5.1 calls that "the sandbox". Outside `SIGNED_ATTRS`, a
    forged value widened the sandbox AND THE SIGNATURE STILL VERIFIED, which makes the
    signed configuration worse than the unsigned one."""
    assert ce.EXT_AGENT in S.SIGNED_ATTRS

    corr = "brave-otter-4718"
    event = ce.new_event(type=ce.TYPE_REQUEST, source="rossoctl://eventbridge/test",
                         datacontenttype="application/json",
                         correlationid=corr, sessionuuid=ce.session_uuid(corr),
                         mode="start", agent="triager",
                         data={"prompt": "hi"})
    S.sign_into(event, SEED, "eb-01")
    ks = keyset.KeySet({"eb-01": S.public_key(SEED)})
    assert S.verify_with_keyset(event, ks)[0]

    # The attack: name a spec with a wider policy.
    event.attrs[ce.EXT_AGENT] = "unsandboxed"
    assert not S.verify_with_keyset(event, ks)[0]


def test_removing_the_agent_attribute_also_invalidates(tmp_path):
    """Stripping it must not be a way around covering it — that would fall back to
    `ER_AGENT_NAME`, which is a different policy than the caller asked for."""
    corr = "brave-otter-4718"
    event = ce.new_event(type=ce.TYPE_REQUEST, source="s",
                         datacontenttype="application/json",
                         correlationid=corr, sessionuuid=ce.session_uuid(corr),
                         mode="start", agent="triager", data={"prompt": "hi"})
    S.sign_into(event, SEED, "eb-01")
    del event.attrs[ce.EXT_AGENT]
    assert not S.verify_with_keyset(
        event, keyset.KeySet({"eb-01": S.public_key(SEED)}))[0]


# ---- 5. userkey reached a path join unvalidated ---------------------------

@pytest.mark.parametrize("hostile", [
    "../users/gh-victim-0bad0bad",
    "../../../../tmp/pwned",
    "..",
    "gh-alice-a1b2c3d4/../../etc",
    "/absolute/path",
    "gh-alice-A1B2C3D4",      # upper-case hex: not a key this module produces
    "GH-alice-a1b2c3d4",
])
def test_a_hostile_userkey_cannot_escape_the_users_directory(tmp_path, hostile):
    """Regression. The key arrives from an inbound Kafka header and
    `Store.__init__` calls `mkdir(parents=True)`, so `..` created and wrote to a
    directory in another tenant's store — or outside the tree entirely."""
    root = tmp_path / "eventbridge"
    r = StoreRegistry(root, multi=True)
    resolved = r._dir_for(hostile).resolve()
    assert resolved == (root / "shared").resolve(), \
        f"{hostile!r} resolved outside shared/: {resolved}"


def test_a_hostile_userkey_is_counted_as_unattributed(tmp_path):
    """It is truthy, so a bare `not userkey` check would let it past uncounted. The
    forgery has to be visible, which is the counter's whole purpose."""
    r = StoreRegistry(tmp_path, multi=True)
    r.for_event(ce.CloudEvent(attrs={ce.EXT_USERKEY: "../../tmp/pwned"}))
    assert r.unattributed == 1


def test_a_hostile_userkey_shares_the_shared_cache_entry(tmp_path):
    """`_cache_key` must agree with `_dir_for`, or an invalid key gets its own entry
    whose Store points at `shared/` — two entries, one directory, two connections."""
    r = StoreRegistry(tmp_path, multi=True)
    assert r.for_userkey("../../tmp/pwned") is r.for_userkey(None)
    assert r.open_count == 1


def test_the_validator_round_trips_over_the_adversarial_corpus():
    """Anything `userkey()` produces must pass `is_valid_userkey`, or a legitimate
    tenant would be silently demoted to `shared/`."""
    corpus = [("github", "MrSabath"), ("github", "a"), ("oidc", "a.b@x.com"),
              ("oidc", "very.long.local.part.that.exceeds.the.budget@example.com"),
              ("static", "...."), ("static", "@@@"), ("static", "ünïcødé"),
              ("static", "-leading-and-trailing-"), ("unknown", "someone")]
    for issuer, userid in corpus:
        key = tenancy.userkey(issuer, userid)
        assert tenancy.is_valid_userkey(key), f"{issuer}/{userid} -> {key}"


# ---- the "known but unowned" /continue 500 -------------------------------

def test_a_shared_tier_continue_is_refused_not_a_500(tmp_path):
    """Regression. A correlation KNOWN to the index with a NULL owner gave
    `owner=None, unresolved=None`, so the handler proceeded, wrote a prompt row, and then
    `TopicSet.requests(None)` raised — a WSGI 500 with an orphan row already committed,
    so the conversation showed a turn that was never submitted."""
    h, producer, owners, stores = _api(tmp_path, real_producer=True)
    corr = h.minter.mint(userkey=None)          # shared tier
    stores.for_userkey(None).upsert_session(corr, ce.session_uuid(corr), "/w", "hi")

    sr = _Start()
    h.continue_agent(_env({"prompt": "go on"}), sr, correlationid=corr)
    assert sr.status.startswith(("404", "503")), sr.status
    assert stores.for_userkey(None).get_prompts(corr) == [], "orphan prompt row written"


def test_a_shared_tier_correlation_is_still_readable(tmp_path):
    """Reading and publishing have different requirements: its data lives in `shared/`
    and must stay readable, even though a turn cannot be published for it."""
    h, _, _, stores = _api(tmp_path)
    corr = h.minter.mint(userkey=None)
    stores.for_userkey(None).upsert_session(corr, ce.session_uuid(corr), "/w", "hi")
    assert h._store_of(corr) is not None
    assert h._publishable_owner(corr)[1] is not None


# ---- the agent-name boundary check ---------------------------------------

@pytest.mark.parametrize("bad", ["../../etc", "Triager", "a/b", {"x": 1}, ["a"],
                                 "x" * 64])
def test_a_bad_agent_name_is_400_not_an_async_error_event(tmp_path, bad):
    """`202 Accepted` followed minutes later by a `phase=error` event the submitter may
    never look at is the wrong answer to a typo in the call that made it.

    **And a `400` must write nothing** — the same property
    `test_a_shared_tier_continue_is_refused_not_a_500` pins for `/continue`, which this
    test originally failed to assert 25 lines away. Round 1 added the validation but put
    it AFTER the writes, so a `400` left a session row, a prompt row and a claimed
    correlation id behind: a transcript page showing a turn that was never submitted,
    plus an id nothing will ever un-claim, since tombstoning is for deletion rather than
    abandonment.
    """
    h, producer, owners, stores = _api(tmp_path)
    sr = _Start()
    h.start_agent(_env({"prompt": "hi", "agent": bad}, token=_as(h, OWNER)), sr)
    assert sr.status.startswith("400"), sr.status
    producer.publish_request.assert_not_called()
    assert stores.for_userkey(UK).all_correlations(limit=10) == [], \
        "a refused submit committed a session row"
    assert owners.count() == 0, "a refused submit claimed a correlation id"


def test_a_bad_agent_name_on_create_group_writes_nothing(tmp_path):
    """The same ordering bug on the group path, where the state becomes PERMANENT.

    `groups.create` commits the group row and publishes `group.started`, so a `400` after
    it left a group with no members that could never gain any — and because the row is
    keyed on the Idempotency-Key, every retry read back `created: false, members: []`,
    which looks like success. The retry is the point of this test.
    """
    h, producer, owners, stores = _api(tmp_path)
    tok = _as(h, OWNER)
    env = _env({"prompts": ["a", "b"], "agent": "../../etc"}, token=tok)
    env["HTTP_IDEMPOTENCY_KEY"] = "retry-me"
    sr = _Start()
    h.create_group(env, sr)
    assert sr.status.startswith("400"), sr.status
    assert stores.for_userkey(UK).all_groups() == [], "a refused batch created a group"
    assert owners.count() == 0, "a refused batch claimed a groupid"

    # The retry, with the same key and a valid name, must actually launch the batch.
    good = _env({"prompts": ["a", "b"], "agent": "triager"}, token=tok)
    good["HTTP_IDEMPOTENCY_KEY"] = "retry-me"
    sr2 = _Start()
    out = json.loads(b"".join(h.create_group(good, sr2)))
    assert sr2.status.startswith("202"), sr2.status
    assert out["created"] is True and len(out["members"]) == 2


def test_a_good_agent_name_still_rides(tmp_path):
    h, producer, _, _ = _api(tmp_path)
    sr = _Start()
    h.start_agent(_env({"prompt": "hi", "agent": "triager"},
                       token=_as(h, OWNER)), sr)
    assert sr.status.startswith("202"), sr.status
    assert producer.publish_request.call_args[1]["agent"] == "triager"


# ---- max_turns: 0 --------------------------------------------------------

def test_max_turns_zero_is_not_silently_rewritten():
    """`or` made `max_turns: 0` become the spec's value, while the spec path REJECTS
    zero — two paths disagreeing about the same nonsensical input."""
    from eventrunner.agentspec import AgentSpec
    from eventrunner.config import Cfg as ErCfg
    from eventrunner.runner import build_cmd
    corr = "brave-otter-4718"
    event = ce.CloudEvent(attrs={"correlationid": corr,
                                 "sessionuuid": ce.session_uuid(corr),
                                 "mode": "start"})
    cmd = build_cmd(ErCfg(), event, {"prompt": "x", "max_turns": 0},
                    spec=AgentSpec(max_turns=8))
    assert cmd[cmd.index("--max-turns") + 1] == "0"


def test_an_absent_max_turns_still_takes_the_spec_value():
    from eventrunner.agentspec import AgentSpec
    from eventrunner.config import Cfg as ErCfg
    from eventrunner.runner import build_cmd
    corr = "brave-otter-4718"
    event = ce.CloudEvent(attrs={"correlationid": corr,
                                 "sessionuuid": ce.session_uuid(corr),
                                 "mode": "start"})
    cmd = build_cmd(ErCfg(), event, {"prompt": "x"}, spec=AgentSpec(max_turns=8))
    assert cmd[cmd.index("--max-turns") + 1] == "8"


# ---- config validation ---------------------------------------------------

@pytest.mark.parametrize("prefix", ["a.b", "a_b", "has space", "a/b", "", "x" * 33])
def test_a_bad_topic_prefix_is_refused_readably(monkeypatch, prefix):
    """The prefix is the one topic-name component `_slug` never sees, and `.`/`_`
    together collide in Kafka's JMX metric names."""
    from eventbridge import config as ebcfg
    monkeypatch.setenv("EB_TOPIC_PREFIX", prefix)
    with pytest.raises(SystemExit, match="EB_TOPIC_PREFIX"):
        ebcfg.load()


def test_a_non_integer_max_open_stores_is_refused_readably(monkeypatch):
    from eventbridge import config as ebcfg
    monkeypatch.setenv("EB_MAX_OPEN_STORES", "lots")
    with pytest.raises(SystemExit, match="EB_MAX_OPEN_STORES"):
        ebcfg.load()


def test_a_broken_index_is_not_reported_as_an_exhausted_id_space():
    """Already covered in test_owner_index.py; asserted here too because it is one of
    the review's findings and belongs in the same place as its siblings."""
    class Broken:
        def exists(self, corr):
            return False

        def claim(self, corr, userkey):
            raise sqlite3.OperationalError("database or disk is full")

    with pytest.raises(sqlite3.OperationalError):
        Minter(index=Broken()).mint(userkey=UK)


# ---- round 3: the three fixes that shipped without a regression test ------
#
# The #883 approval noted that reverting `agentspec`'s try/except, `claim`'s tombstone
# check or `registry`'s `validate_name` left all 858 tests passing — so three of the six
# fixes in 258da7a were invisible to the suite. The standard this PR has applied since
# 6ebdd80 is "every fix has a regression test, verified by reverting its fix", and it
# had been applied to half that commit. These close the gap.


@pytest.mark.parametrize("field,value", [
    ("timeout_s", '"900s"'),
    ("max_output_bytes", '"4MB"'),
    ("max_events", '"lots"'),
])
def test_a_non_numeric_limit_raises_specerror_not_valueerror(tmp_path, field, value):
    """`run_agent` catches only `SpecError`, so a bare `ValueError` from these three
    conversions propagated out of it instead of becoming the `phase=error` event §5.1
    promises. `max_turns` is wrapped for exactly this reason; `[limits]` was the one
    block in `parse` that escaped the module's own stated contract."""
    from eventrunner import agentspec
    d = tmp_path / "a"
    d.mkdir()
    (d / "agent.toml").write_text(f"[limits]\n{field} = {value}\n")
    with pytest.raises(agentspec.SpecError, match="limits must be numbers"):
        agentspec.load(str(tmp_path), "a")


@pytest.mark.parametrize("block,match", [
    ("[limits]\ntimeout_s = -5\n", "timeout_s must be >= 0"),
    ("[limits]\nmax_output_bytes = -1\n", "max_output_bytes must be >= 0"),
    ("[limits]\nmax_events = 0\n", "max_events must be >= 1"),
])
def test_a_nonsensical_limit_range_is_refused(tmp_path, block, match):
    """The review left this as a noted gap, since `Limits` is documented inert. Closed
    anyway: a negative deadline is nonsensical whether or not anything reads it yet, and
    whoever wires up the `Popen` wrapper should inherit a value they can trust rather
    than re-validate. `max_events = 0` is the interesting one — a run that may emit no
    events cannot report its own result."""
    from eventrunner import agentspec
    d = tmp_path / "a"
    d.mkdir()
    (d / "agent.toml").write_text(block)
    with pytest.raises(agentspec.SpecError, match=match):
        agentspec.load(str(tmp_path), "a")


def test_the_documented_sentinels_still_load(tmp_path):
    """`0` means "no spec-imposed deadline"/"unbounded" per the docstring, so the range
    check must not reject the documented defaults."""
    from eventrunner import agentspec
    d = tmp_path / "a"
    d.mkdir()
    (d / "agent.toml").write_text("[limits]\ntimeout_s = 0\nmax_output_bytes = 0\n")
    spec = agentspec.load(str(tmp_path), "a")
    assert spec.limits.timeout_s == 0.0
    assert spec.limits.max_output_bytes == 0
    assert spec.limits.max_events == 2000


def test_a_non_table_limits_block_is_refused(tmp_path):
    from eventrunner import agentspec
    d = tmp_path / "a"
    d.mkdir()
    (d / "agent.toml").write_text('limits = "nope"\n')
    with pytest.raises(agentspec.SpecError, match="limits must be a table"):
        agentspec.load(str(tmp_path), "a")


def test_a_tombstoned_correlation_cannot_be_reclaimed_by_anyone(tmp_path):
    """`forget` clears `userkey` to NULL, so `claim(corr, None)` hit `None == None` and
    returned as an idempotent re-claim — contradicting the guarantee `forget` makes, that
    the id is reserved forever because a reissue derives the same unsalted `sessionuuid`.

    Unreachable from `Minter.mint`, which checks `exists()` first; reachable as soon as a
    `correlationid` arrives from outside, which §7's triggers bring.
    """
    from eventbridge.owner_index import Collision, OwnerIndex
    owners = OwnerIndex(tmp_path / "eventbridge")
    owners.claim("brave-otter-0001", UK)
    owners.forget("brave-otter-0001")

    # The NULL owner is the case `row[0] == userkey` got wrong.
    with pytest.raises(Collision, match="tombstoned"):
        owners.claim("brave-otter-0001", None)
    # A tenant failed correctly before, but must keep failing for the RIGHT reason: the
    # message should say "tombstoned", not "already owned by (single-tenant)".
    with pytest.raises(Collision, match="tombstoned"):
        owners.claim("brave-otter-0001", OTHER)
    assert owners.is_tombstoned("brave-otter-0001")


def test_a_registry_agent_name_is_validated_at_startup():
    """`agent` was the only field `parse` accepted unchecked, in a function whose
    docstring promises every inconsistency is a refusal. It reaches `ce_agent` via
    `by_userkey(...).agent` on a path that never sees `validate_name`, so a typo meant an
    asynchronous `phase=error` on EVERY request from that user — the failure mode the
    request path was moved away from in `6ebdd80`."""
    raw = json.dumps({"version": 1, "users": [
        {"issuer": "github", "userid": "alice", "agent": "Not A Label"}]})
    with pytest.raises(registry.RegistryError, match="DNS-1123"):
        registry.parse(raw)


@pytest.mark.parametrize("bad", ["../../etc", "a/b", "x" * 64, "UPPER"])
def test_every_invalid_registry_agent_shape_is_refused(bad):
    raw = json.dumps({"version": 1, "users": [
        {"issuer": "github", "userid": "alice", "agent": bad}]})
    with pytest.raises(registry.RegistryError):
        registry.parse(raw)


def test_a_valid_registry_agent_is_accepted_and_an_absent_one_falls_through():
    """The gate must not reject what it should allow: a good name loads, and an omitted
    `agent` stays empty so the runner's own `ER_AGENT_NAME` applies."""
    r = registry.parse(json.dumps({"version": 1, "users": [
        {"issuer": "github", "userid": "alice", "agent": "triager"},
        {"issuer": "github", "userid": "bob"}]}))
    assert r.lookup("github", "alice").agent == "triager"
    assert r.lookup("github", "bob").agent == ""
