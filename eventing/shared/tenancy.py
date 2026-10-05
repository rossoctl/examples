"""Tenancy keys and the names derived from them. DESIGN_PHASE3.md §2.3, §3.1, §4.2.

This is the ONLY place an identity becomes a name. Phase 2 put a login on the event
as an audit label (`ce_submitter`); Phase 3 turns it into a tenancy key that names
Kafka topics, Kubernetes objects and an ntfy topic. Those four jobs have four
different legality rules, and a raw login satisfies none of them reliably:

| Job | Why a raw `submitter` fails |
|---|---|
| Kafka topic | `[a-zA-Z0-9._-]`, max 249. `alice@example.com` has an illegal `@`. |
| Kubernetes object | DNS-1123: lower-case `[a-z0-9-]`, 63 for a label. `MrSabath` has upper case. |
| ntfy topic | `[-_A-Za-z0-9]{1,64}`, and the name travels to a phone (§4.1). |
| Separate two tenants | The one that matters: any lossy mapping can merge two identities. |

The fourth is a security bug rather than a cosmetic one, and it is easy to walk into:
replacing every illegal character with `-` maps `a.b@x.com`, `a_b@x.com` and
`a-b@x.com` onto one key, and then user B reads user A's events. `userkey` is built so
that cannot happen — see the digest note there.

Pure stdlib (`hashlib`, `hmac`, `base64`), per Phase 1 §1.1, and every function here is
pure: no I/O, no clock, no config. That is what makes the whole module testable against
a corpus of adversarial identifiers without a cluster.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import re

# Short issuer tags. Two characters so the readable middle of the key keeps as much
# room as possible, and in the key at all so a human reading `kubectl get kafkatopics`
# can tell which issuer a topic belongs to.
ISS_PREFIX = {"github": "gh", "oidc": "oi", "static": "st"}

# Mode names for TopicSet. `single` is the default everywhere: it reproduces Phase 2
# byte for byte, which is what makes this phase additive.
SINGLE = "single"
MULTI = "multi"

# How much of the canonical identity's digest is appended to the key. 32 bits, which
# is NOT collision-resistant in the cryptographic sense — a birthday collision is
# expected around 2**16 ~ 65,000 users. The registry's duplicate check (§2.5) is what
# covers that, by refusing a key already held by a different identity. Raise to 12 if
# a deployment ever approaches that scale.
_DIGEST_HEX = 8

# The readable middle, before the digest. Keeps the longest realistic topic name
# (`kev1-` + 2 + 1 + 24 + 1 + 8 + `-responses` = 51) comfortably inside Kafka's 249
# and inside a 63-character DNS-1123 label.
_SLUG_MAX = 24


def _canonicalise(issuer: str, userid: str) -> str:
    """The ONLY lossy step, and deliberately minimal — per issuer, never guessed.

    Each branch is a claim about the issuer's own identifier semantics, and getting one
    wrong in the permissive direction merges two humans into one tenant:

    * **GitHub** logins are case-insensitive, so lower-casing is *correct* here. It must
      also match `ghauth.is_allowed`'s compare exactly — if the allow-list folds case
      and this does not, `Alice` and `alice` are both approved and land in different
      tenants, which reads to the user as half their history vanishing.
    * **Email** normalisation is provider-specific and we must not guess:
      `A.Lice@Gmail.com` and `alice@gmail.com` are one mailbox at Gmail and may be two
      elsewhere. So only what RFC 5321 permits — lower-case the domain, leave the local
      part byte-exact. One human with two spellings therefore gets two tenants, which
      is wasteful, not unsafe. The unsafe direction (two humans, one tenant) is what the
      digest in `userkey` makes impossible.
    * **Static** identities are byte-exact — *unless they are address-shaped*. The `@`
      branch above is checked before the issuer, so a static `EB_AUTH_TOKENS` name of
      `Ops@Example.COM` canonicalises to `Ops@example.com` rather than byte-exact. That
      is deliberate: the email rule applies to anything containing `@` whatever the
      issuer, because an operator who types an address means an address. Everything else
      from a static issuer is left exactly as typed — normalising somebody's
      hand-written string on their behalf is how you merge two of them.

    `rpartition` rather than `partition` because the local part of an address may itself
    contain `@` when quoted; the domain is what follows the LAST one.
    """
    s = userid.strip()
    if issuer == "github":
        return s.lower()
    if "@" in s:
        local, _, domain = s.rpartition("@")
        return f"{local}@{domain.lower()}"
    return s


def _slug(canon: str) -> str:
    """A readable, DNS-1123-safe rendering of the canonical identity. Lossy on purpose.

    Lossiness is the point: it is what keeps `gh-alice-a1b2c3d4` legible in a log line
    and in `kubectl get pods`. It is survivable only because `userkey` appends a digest
    over the canonical form, so two identities that slug identically still differ in the
    key.

    Emits `-` as the only separator, never `.` or `_`. That is not stylistic: Kafka's
    JMX metric names flatten both `.` and `_`, so a topic `a.b_c` and a topic `a_b.c`
    collide and one silently overwrites the other's metrics. Keeping `-` as the sole
    separator makes that unreachable — stated here because the next person to "improve"
    this function needs to know.
    """
    out: list[str] = []
    prev_dash = False
    for ch in canon.lower():
        if ch.isascii() and (ch.isalnum()):
            out.append(ch)
            prev_dash = False
        elif not prev_dash:
            # Any run of illegal characters collapses to one dash, so `a..b` and `a@b`
            # both render `a-b`. Safe only because of the digest; see `userkey`.
            out.append("-")
            prev_dash = True
    return "".join(out)


def userkey(issuer: str, userid: str) -> str:
    """Map (issuer, userid) to a short, legal, COLLISION-FREE tenancy key.

    Shape: `<iss>-<slug>-<8 hex>`

        gh-alice-a1b2c3d4
        oi-alice-example-com-9e8f7a6b
        st-ops-break-glass-00ff1122

    Legal as a Kafka topic component, a DNS-1123 label, and an ntfy topic component.

    Three properties, each defending a decision:

    **The digest is the security control, not decoration.** `_slug` is lossy, which is
    what makes the key readable. That is only safe because 32 bits of SHA-256 over the
    canonical form are appended, so two distinct canonical identities cannot produce one
    key no matter how the slug collapses them.

    **The issuer is in the key AND in the digest input.** GitHub user `alice` and OIDC
    subject `alice` are different people. Without the issuer inside the hashed bytes, a
    second configured issuer becomes a way to impersonate the first — the single worst
    failure available here. It appears in the prefix so a human can see it, and in the
    digest so it is load-bearing rather than cosmetic.

    **The separator inside the digest input is `\\x1f`**, a unit separator, which cannot
    occur in an issuer name. Concatenating with a character that *can* occur in either
    field would make the input ambiguous: with `-`, issuer `a` + user `b-c` and issuer
    `a-b` + user `c` hash identically, which is exactly the cross-issuer collision the
    issuer was added to prevent.
    """
    iss = ISS_PREFIX.get(issuer, "xx")
    canon = _canonicalise(issuer, userid)
    digest = hashlib.sha256(f"{issuer}\x1f{canon}".encode()).hexdigest()[:_DIGEST_HEX]
    # `strip("-")` after truncating, so a slug cut mid-run does not end in a dash —
    # a DNS-1123 label may not start or end with one. `or "u"` covers an identifier
    # with no alphanumerics at all, which would otherwise slug to "" and produce
    # `gh--a1b2c3d4`: still unique thanks to the digest, but not a legal label.
    slug = _slug(canon)[:_SLUG_MAX].strip("-") or "u"
    return f"{iss}-{slug}-{digest}"


# The exact shape `userkey()` produces: `<2-letter iss>-<slug>-<8 hex>`. Exported as a
# validator because this module is "the ONLY place an identity becomes a name" and a
# producer's guarantee is worth nothing if no consumer can check it.
#
# The slug is at most _SLUG_MAX characters, starts with an alphanumeric (`_slug` strips
# leading dashes) and contains only `[a-z0-9-]`.
USERKEY_RE = re.compile(
    rf"^[a-z]{{2}}-[a-z0-9][a-z0-9-]{{0,{_SLUG_MAX - 1}}}-[0-9a-f]{{{_DIGEST_HEX}}}$")


def is_valid_userkey(value: str | None) -> bool:
    """Whether `value` is a key this module could have produced.

    **This is a security gate, not a type check.** A `userkey` reaches a filesystem path
    join (`store_registry._dir_for`) and in multi-tenant mode it arrives from an inbound
    Kafka header, so an unvalidated one containing `..` escapes `users/` into another
    tenant's store — or out of the tree entirely, since `Store.__init__` calls
    `mkdir(parents=True)` and therefore creates the directory rather than rejecting it.

    Two reasons that is reachable rather than theoretical:

    * `userkey` is in `signing.SIGNED_ATTRS`, but `EB_REQUIRE_RESPONSE_SIGNATURE` defaults
      to `false` and audit mode stores the event unchanged — verification is what would
      catch the forgery and it is off by default.
    * §3.6 concedes that in Tier A anything with network access to the shared broker can
      write to any topic. That is an accepted property for *reading* events; it must not
      also be a filesystem write primitive.

    Callers treat an invalid key exactly like a missing one — the `shared` store plus the
    `unattributed` counter — so a forged value is counted and visible rather than acted on.
    Note that an invalid key is *truthy*, so it would otherwise sail past a `if not
    userkey` check straight into the join.
    """
    return bool(value) and bool(USERKEY_RE.fullmatch(value))


def ntfy_topic(prefix: str, userkey: str, secret: bytes) -> str:
    """An unguessable, stable, per-user ntfy topic that contains no identifier.

        kev1-k7qf3mz2xa9pbw4nsdhe6tcyu5      (prefix + 26 chars of base32)

    §4.1 is why the obvious `{prefix}-{userkey}-ntfy` is wrong: the topic name travels
    to a phone and appears in the notification UI, so embedding a login publishes who
    the user is to anyone who sees the screen — and on a public server, anyone who
    guesses the name reads the stream.

    The decisions:

    * **Keyed, not hashed.** A bare `sha256(userkey)` is computable by anyone, because
      the userkey is public — it names topics and Kubernetes objects. The HMAC secret
      (`EB_NTFY_TOPIC_SECRET_PATH`, a Secret mount) is what makes the topic unguessable
      rather than merely ugly.
    * **Derived, not stored.** No table and no row to drift from the Kafka topics.
      Rotating the secret rotates every user's topic at once, which is the right
      response to a leak. The cost is that in-flight notifications on the old topic are
      orphaned, which is why nothing durable is keyed by the ntfy topic.
    * **Base32, lower-cased.** ntfy allows `[-_A-Za-z0-9]{1,64}`; base32 is
      case-insensitive-safe at 5 bits per character, so 26 characters carry 130 bits.
      The truncation discards 126 of the 256 HMAC bits deliberately — the topic has to
      be typeable, and 130 bits is not the weak link in any threat model here.

    The `b"ntfy|"` domain separator keeps this HMAC from colliding with any other use of
    the same secret added later: without it, a second derivation over the bare userkey
    would produce the same tag for a different purpose.
    """
    mac = hmac.new(secret, b"ntfy|" + userkey.encode(), hashlib.sha256).digest()
    tag = base64.b32encode(mac).decode().rstrip("=").lower()[:26]
    return f"{prefix}-{tag}"


class TopicSet:
    """The one place §3.1's topic layout is written down.

    | Topic | Name | Written by | Read by |
    |---|---|---|---|
    | requests | `{prefix}-{userkey}-requests` | EventBridge | that user's EventRunner |
    | responses | `{prefix}-{userkey}-responses` | that user's EventRunner | EventBridge |
    | inbox | `{prefix}-{userkey}-events` | EventBridge ingress | the trigger dispatcher |
    | dead letter | `{prefix}-{userkey}-dead` | EventBridge | an operator with a console consumer |

    In `single` mode every method returns the configured request/response topic and
    ignores `userkey` entirely. That is what makes the whole phase additive: a Phase
    0/1/2 deployment gets the same two topics it has now, through the same code path the
    multi-tenant one uses — so the existing suite is the check on step 2 of §9's rollout.

    Not a dataclass because `single` mode has to carry the two explicit topic names
    while `multi` mode derives all four from a prefix, and a constructor that validates
    that pairing is clearer than four optional fields.
    """

    def __init__(self, prefix: str, *, tenancy: str = SINGLE,
                 request_topic: str = "requests",
                 response_topic: str = "responses") -> None:
        if tenancy not in (SINGLE, MULTI):
            raise ValueError(f"tenancy must be {SINGLE!r} or {MULTI!r}, got {tenancy!r}")
        self.prefix = prefix
        self.tenancy = tenancy
        self._request_topic = request_topic
        self._response_topic = response_topic

    @property
    def multi(self) -> bool:
        return self.tenancy == MULTI

    def _derived(self, userkey: str | None, suffix: str, fallback: str) -> str:
        if not self.multi:
            return fallback
        if not userkey:
            # A missing userkey in multi mode is a programming error, not a condition to
            # paper over with a default: a request published to a fallback topic is one
            # tenant's work executed by another tenant's runner, under that tenant's
            # credential. Refusing loudly here is the only safe direction.
            raise ValueError(f"userkey is required to name the {suffix!r} topic in multi mode")
        return f"{self.prefix}-{userkey}-{suffix}"

    def requests(self, userkey: str | None = None) -> str:
        return self._derived(userkey, "requests", self._request_topic)

    def responses(self, userkey: str | None = None) -> str:
        return self._derived(userkey, "responses", self._response_topic)

    def events(self, userkey: str | None = None) -> str:
        """The §7 trigger inbox. In single mode there is one, named off the prefix:
        unlike requests/responses it has no Phase 2 predecessor to stay compatible
        with, so it is derived rather than configured."""
        if not self.multi:
            return f"{self.prefix}-events"
        return self._derived(userkey, "events", "")

    def dead(self, userkey: str | None = None) -> str:
        """The §7.5 dead letter topic. Same reasoning as `events`."""
        if not self.multi:
            return f"{self.prefix}-dead"
        return self._derived(userkey, "dead", "")
