"""Tenancy keys, topic names and derived ntfy topics. DESIGN_PHASE3.md §2.3, §3.1, §4.2.

The collision tests are the point of this file. `_slug` is deliberately lossy, so the
only thing standing between a readable key and a cross-tenant leak is the appended
digest — and the one way to know it is actually load-bearing is to feed it the
adversarial identifiers that slug identically and assert the keys still differ.
"""
from __future__ import annotations

import re

import pytest

from eventbridge import ghauth
from shared import tenancy as T

# DNS-1123 label: lower-case alphanumerics and dashes, no leading or trailing dash,
# 63 characters. A userkey has to satisfy this to name a Kubernetes object and to be
# a Kafka topic component at the same time.
DNS1123 = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
# Kafka's legal topic character set. A userkey is one *component* of a topic name, so
# it has to be legal here too.
KAFKA_TOPIC = re.compile(r"^[a-zA-Z0-9._-]+$")
# ntfy's topic rule, from the ntfy server source.
NTFY_TOPIC = re.compile(r"^[-_A-Za-z0-9]{1,64}$")

# Identifiers chosen to break a naive implementation: characters illegal in every
# target namespace, runs of separators that collapse, upper case, leading and trailing
# punctuation, something that slugs to nothing at all, and an over-long local part.
#
# Every entry must map to a DISTINCT key (see test_all_adversarial_keys_are_distinct),
# so deliberately-equivalent spellings do NOT belong here: `MrSabath` and `mrsabath` are
# one GitHub identity by design, and live in the case-folding tests at the bottom.
ADVERSARIAL = [
    ("github", "mrsabath"),
    ("github", "a"),
    ("github", "a-b-c"),
    ("oidc", "alice@example.com"),
    ("oidc", "a.b@x.com"),
    ("oidc", "a_b@x.com"),
    ("oidc", "a-b@x.com"),
    ("oidc", "A.Lice@Gmail.COM"),
    ("oidc", "very.long.local.part.that.exceeds.the.slug.budget@example.com"),
    ("static", "ops-break-glass"),
    ("static", "ops break glass"),
    ("static", "...."),
    ("static", "@@@"),
    ("static", "-leading-and-trailing-"),
    ("static", "ünïcødé"),
    ("static", "a/b\\c:d*e"),
    ("unknown-issuer", "someone"),
]


@pytest.mark.parametrize("issuer,userid", ADVERSARIAL)
def test_userkey_is_legal_everywhere(issuer, userid):
    """One key has to satisfy three different namespaces at once."""
    k = T.userkey(issuer, userid)
    assert DNS1123.match(k), f"{k!r} is not a DNS-1123 label"
    assert KAFKA_TOPIC.match(k), f"{k!r} is not legal in a Kafka topic"
    assert NTFY_TOPIC.match(k), f"{k!r} is not a legal ntfy topic"
    assert len(k) <= 63, f"{k!r} exceeds a 63-char DNS-1123 label"
    # The separator rule from §3.1: `.` and `_` together collide in Kafka's JMX metric
    # names, so the slug must emit neither.
    assert "." not in k and "_" not in k


@pytest.mark.parametrize("issuer,userid", ADVERSARIAL)
def test_userkey_is_stable(issuer, userid):
    assert T.userkey(issuer, userid) == T.userkey(issuer, userid)


def test_userkey_shape():
    k = T.userkey("github", "alice")
    assert k == "gh-alice-" + k.rsplit("-", 1)[1]
    assert re.fullmatch(r"gh-alice-[0-9a-f]{8}", k)


def test_all_adversarial_keys_are_distinct():
    """The whole corpus must map to distinct keys.

    Several of these slug identically — `a.b@x.com`, `a_b@x.com` and `a-b@x.com` all
    render `a-b-x-com`, and `....`/`@@@` both render nothing. If the digest were
    dropped or computed over the slug instead of the canonical form, this fails, and
    the production symptom is one user reading another's events.
    """
    keys = [T.userkey(i, u) for i, u in ADVERSARIAL]
    dupes = {k for k in keys if keys.count(k) > 1}
    assert not dupes, f"colliding userkeys: {dupes}"


@pytest.mark.parametrize("a,b", [
    ("a.b@x.com", "a_b@x.com"),
    ("a.b@x.com", "a-b@x.com"),
    ("a_b@x.com", "a-b@x.com"),
])
def test_separator_variants_do_not_merge(a, b):
    """The classic lossy-normalisation bug, asserted directly."""
    assert T._slug(T._canonicalise("oidc", a)) == T._slug(T._canonicalise("oidc", b))
    assert T.userkey("oidc", a) != T.userkey("oidc", b)


def test_slug_collapses_to_nothing_still_yields_a_legal_label():
    for userid in ("....", "@@@", "   ", "///"):
        k = T.userkey("static", userid)
        assert DNS1123.match(k), k
        assert "--" not in k, f"{k!r} has an empty slug segment"


def test_issuer_is_load_bearing_not_cosmetic():
    """Same userid under two issuers must be two tenants.

    Without the issuer inside the hashed input, configuring a second issuer becomes a
    way to impersonate an identity on the first.
    """
    gh = T.userkey("github", "alice")
    oi = T.userkey("oidc", "alice")
    assert gh != oi
    # Not merely different prefixes — the digests must differ too, or stripping the
    # prefix (as a future rename or a shorter tag might) would re-merge them.
    assert gh.rsplit("-", 1)[1] != oi.rsplit("-", 1)[1]


def test_issuer_userid_boundary_is_unambiguous():
    """issuer `a` + userid `b-c` must not hash like issuer `a-b` + userid `c`.

    This is why the digest input is joined with `\\x1f` rather than a character that can
    appear in either field.
    """
    assert T.userkey("a", "b-c") != T.userkey("a-b", "c")


def test_unknown_issuer_gets_the_xx_tag_and_still_separates():
    k = T.userkey("whatever", "alice")
    assert k.startswith("xx-")
    assert k != T.userkey("github", "alice")
    # Two unknown issuers must not merge just because they share the `xx` tag: the
    # digest covers the real issuer string, not the tag.
    assert T.userkey("iss-one", "alice") != T.userkey("iss-two", "alice")


# ---- the agreement with ghauth.is_allowed -----------------------------------

@pytest.mark.parametrize("login", ["Alice", "alice", "ALICE", "aLiCe"])
def test_github_case_folding_agrees_with_is_allowed(login):
    """§2.3: if the allow-list folds case and the key does not, every spelling is
    approved and each lands in a different tenant — one human with their history split
    in half, which reads as data loss. So the two functions have to agree, and this is
    the cheap place to find out that they do not."""
    allowed = ghauth.parse_allowed_users("alice")
    assert ghauth.is_allowed(login, allowed)
    assert T.userkey("github", login) == T.userkey("github", "alice")


def test_github_whitespace_is_stripped_like_the_allow_list():
    allowed = ghauth.parse_allowed_users(" alice , bob ")
    assert ghauth.is_allowed("alice", allowed)
    assert T.userkey("github", " alice ") == T.userkey("github", "alice")


def test_email_local_part_stays_case_sensitive_but_domain_does_not():
    """Only what RFC 5321 permits. Erring toward two tenants for one human is
    wasteful; erring the other way is a leak."""
    assert T.userkey("oidc", "Alice@example.com") != T.userkey("oidc", "alice@example.com")
    assert T.userkey("oidc", "alice@EXAMPLE.com") == T.userkey("oidc", "alice@example.com")


def test_email_with_at_in_the_local_part_splits_on_the_last_at():
    assert T._canonicalise("oidc", '"a@b"@EXAMPLE.com') == '"a@b"@example.com'


def test_static_identities_are_byte_exact():
    """An operator typed these; normalising on their behalf is how two of them merge."""
    assert T.userkey("static", "Ops") != T.userkey("static", "ops")


# ---- TopicSet --------------------------------------------------------------

def test_single_mode_reproduces_phase2_topics_exactly():
    """Step 2 of §9's rollout: zero behaviour change. The existing suite is the real
    check, but this pins the intent."""
    ts = T.TopicSet("kev1", tenancy=T.SINGLE,
                    request_topic="requests", response_topic="responses")
    assert ts.requests() == "requests"
    assert ts.responses() == "responses"
    # A userkey is ignored entirely in single mode rather than changing the answer.
    assert ts.requests("gh-alice-a1b2c3d4") == "requests"
    assert ts.responses("gh-alice-a1b2c3d4") == "responses"
    assert not ts.multi


def test_single_mode_honours_configured_topic_names():
    ts = T.TopicSet("kev1", request_topic="my-reqs", response_topic="my-resps")
    assert ts.requests() == "my-reqs"
    assert ts.responses() == "my-resps"


def test_multi_mode_names():
    ts = T.TopicSet("kev1", tenancy=T.MULTI)
    uk = "gh-mrsabath-4c1d9e07"
    assert ts.requests(uk) == "kev1-gh-mrsabath-4c1d9e07-requests"
    assert ts.responses(uk) == "kev1-gh-mrsabath-4c1d9e07-responses"
    assert ts.events(uk) == "kev1-gh-mrsabath-4c1d9e07-events"
    assert ts.dead(uk) == "kev1-gh-mrsabath-4c1d9e07-dead"
    assert ts.multi


def test_multi_mode_refuses_to_name_a_topic_without_a_userkey():
    """Falling back to a shared topic here would publish one tenant's work onto
    another tenant's runner, under that tenant's credential."""
    ts = T.TopicSet("kev1", tenancy=T.MULTI)
    for method in (ts.requests, ts.responses, ts.events, ts.dead):
        with pytest.raises(ValueError, match="userkey is required"):
            method(None)
        with pytest.raises(ValueError, match="userkey is required"):
            method("")


def test_longest_realistic_topic_name_fits_kafkas_limit():
    """§3.1 computes 51 against Kafka's 249. Worth checking rather than assuming."""
    uk = T.userkey("oidc", "a" * 64 + "@" + "b" * 60 + ".example.com")
    longest = T.TopicSet("kev1", tenancy=T.MULTI).responses(uk)
    assert len(longest) <= 249
    assert len(longest) < 60, f"unexpectedly long: {longest!r} ({len(longest)})"
    assert KAFKA_TOPIC.match(longest)


def test_tenancy_mode_is_validated():
    with pytest.raises(ValueError):
        T.TopicSet("kev1", tenancy="sort-of")


def test_single_mode_inbox_and_dead_are_derived_from_the_prefix():
    """Unlike requests/responses these have no Phase 2 predecessor to stay
    compatible with, so there is nothing to configure them from."""
    ts = T.TopicSet("kev1")
    assert ts.events() == "kev1-events"
    assert ts.dead() == "kev1-dead"


# ---- ntfy_topic ------------------------------------------------------------

SECRET = b"a-test-ntfy-topic-secret"


def test_ntfy_topic_shape_and_legality():
    t = T.ntfy_topic("kev1", "gh-alice-a1b2c3d4", SECRET)
    assert NTFY_TOPIC.match(t), t
    assert t.startswith("kev1-")
    assert len(t) == len("kev1-") + 26
    assert t == t.lower()


def test_ntfy_topic_contains_no_identifier():
    """§4.1 is the whole design: the name travels to a phone."""
    t = T.ntfy_topic("kev1", T.userkey("github", "mrsabath"), SECRET)
    assert "mrsabath" not in t
    # No assertion about the issuer tag: base32's alphabet is `a-z2-7`, so "gh" appears
    # in 26 random characters about 2.4% of the time. Testing that a 130-bit digest does
    # not happen to contain a common bigram is a flaky test, not a property.


def test_ntfy_topic_is_stable_and_per_user():
    a = T.ntfy_topic("kev1", "gh-alice-a1b2c3d4", SECRET)
    b = T.ntfy_topic("kev1", "gh-bob-9e8f7a6b", SECRET)
    assert a == T.ntfy_topic("kev1", "gh-alice-a1b2c3d4", SECRET)
    assert a != b


def test_ntfy_topic_is_keyed_not_merely_hashed():
    """The userkey is public, so an unkeyed digest would be computable by anyone.
    Rotating the secret must rotate every topic."""
    a = T.ntfy_topic("kev1", "gh-alice-a1b2c3d4", SECRET)
    b = T.ntfy_topic("kev1", "gh-alice-a1b2c3d4", b"a-different-secret")
    assert a != b


def test_ntfy_topic_carries_enough_entropy():
    """26 base32 characters is 130 bits. Assert the tag is not accidentally short or
    padded, which truncation bugs produce."""
    tag = T.ntfy_topic("kev1", "gh-alice-a1b2c3d4", SECRET).removeprefix("kev1-")
    assert len(tag) == 26
    assert "=" not in tag
    assert set(tag) <= set("abcdefghijklmnopqrstuvwxyz234567")
