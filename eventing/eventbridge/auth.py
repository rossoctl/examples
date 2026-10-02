"""Bearer-token identity for the submit path. Stdlib only.

Scope is deliberately narrow: this answers "who is asking me to run an agent?"
on the two routes that CREATE work. It is not a general authorization layer.

Two mechanisms live here. `ghauth` verifies a GitHub sign-in and is the real
path; the static token map below is the fallback that keeps tests off the network
and an offline demo possible. `resolve()` is the entry point that picks.

Two properties worth stating, because both are easy to get wrong:

* **Constant-time comparison.** Tokens are compared with
  `hmac.compare_digest`, not `==`. A short-circuiting compare leaks the shared
  prefix length through timing, which over enough requests recovers the token
  one byte at a time.
* **Identity is a name, not a boolean.** The config maps a name to each token,
  so a validated request yields `"alice"` rather than `True`. That name rides
  onto the request event as `ce_submitter`, which is the whole point — a `401`
  tells you nothing after the fact, a recorded submitter does.

What this is NOT, and the two limits that still apply. `submitter` is now inside
`signing.SIGNED_ATTRS` and EventBridge signs the requests it publishes, so when
signing is configured the attribute cannot be altered in flight without invalidating
the signature. But:

* **Signing is opt-in.** With no `EB_SIGNING_KEY_PATH` the events are unsigned, the
  broker is plaintext, and anyone who can write to the `requests` topic can forge a
  submitter. The claim only holds where verification is actually enabled.
* **A signature proves the assertion, not the identity.** It shows EventBridge said
  this, not that the name is real. A static `EB_AUTH_TOKENS` entry is a name an
  operator typed into an env var; `ce_submitteriss` is what distinguishes it from a
  login GitHub verified.

So the honest claim is "EventBridge refuses unauthenticated submissions, records who
it believes submitted, and — when signing is on — makes that record tamper-evident."
"""
from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Any

from shared import tenancy

# Returned verbatim as the `WWW-Authenticate` value on a 401 so a client knows
# which scheme to retry with.
CHALLENGE = 'Bearer realm="eventbridge"'

# The issuer string recorded for a static `EB_AUTH_TOKENS` identity. `ce_submitteriss`
# stays ABSENT for these (Phase 2 §2.6: absent issuer = an operator typed this name), but
# the tenancy key still needs an issuer in its hashed input — otherwise a static `alice`
# and a GitHub `alice` would be one tenant, which is the §2.3 impersonation failure.
ISSUER_STATIC = "static"


@dataclass(frozen=True)
class Caller:
    """Who is asking, and the tenancy key that follows from it. §2.4.

    `userkey` is carried here so no caller derives it twice: it is needed to pick a
    topic, a store and an ntfy topic, and three independent derivations are three
    chances to disagree.

    `None` for all of `userid`/`issuer`/`userkey` is the anonymous case, which is what
    Phase 2's "no auth configured" default produces and is what keeps the demo working
    out of the box. §2.4 is explicit that this is incompatible with per-user isolation —
    there is no user to isolate — so `multi` mode refuses it with `401`. The two modes
    disagree about this on purpose.
    """
    userid: str | None = None
    issuer: str | None = None
    userkey: str | None = None
    tier: str = "shared"

    @property
    def anonymous(self) -> bool:
        return self.userid is None

    @property
    def submitter_iss(self) -> str | None:
        """What rides on the event as `ce_submitteriss`.

        Deliberately NOT the same as `issuer`: a static identity has an issuer here (so
        its tenancy key separates from a GitHub login of the same name) but must stay
        absent on the wire, because Phase 2 made absence mean "an operator typed this".
        Collapsing the two would quietly upgrade an operator's assertion into a verified
        claim.
        """
        return None if self.issuer in (None, ISSUER_STATIC) else self.issuer


def caller_for(userid: str | None, issuer: str | None, *,
               tenancy_mode: str = tenancy.SINGLE, tier: str = "shared") -> Caller:
    """Build a `Caller`, deriving the tenancy key exactly once.

    The key is derived only in `multi` mode. In `single` mode it stays `None`, which is
    what keeps `ce_userkey` off the wire entirely for a Phase 2 deployment — the
    attribute is additive, and an event that does not need it does not carry it.
    """
    if userid is None:
        return Caller()
    key = None
    if tenancy_mode == tenancy.MULTI:
        key = tenancy.userkey(issuer or ISSUER_STATIC, userid)
    return Caller(userid=userid, issuer=issuer, userkey=key, tier=tier)


def parse_tokens(raw: str) -> dict[str, str]:
    """`"alice:tok1,bob:tok2"` -> `{"tok1": "alice", "tok2": "bob"}`.

    Keyed by token because lookup goes token -> name. Malformed entries (no
    colon, empty name, empty token) are skipped rather than raising: a typo in
    one entry must not take the whole service down at startup, and a skipped
    entry fails closed — that token simply does not authenticate.
    """
    out: dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        name, _, token = entry.partition(":")
        name, token = name.strip(), token.strip()
        if name and token:
            out[token] = name
    return out


def _bearer(environ: dict[str, Any]) -> tuple[str | None, str | None]:
    """Pull the bearer token out of the environ. `(token, error)`.

    Reads only headers. It must not touch `wsgi.input`: `handlers._read_json`
    reads the body from a non-seekable stream, so consuming it here would leave
    every downstream handler with an empty body.
    """
    header = environ.get("HTTP_AUTHORIZATION") or ""
    if not header:
        return None, "authentication required"
    scheme, _, presented = header.partition(" ")
    if scheme.lower() != "bearer":
        return None, "unsupported authentication scheme; expected Bearer"
    presented = presented.strip()
    if not presented:
        return None, "empty bearer token"
    return presented, None


def resolve_identity(environ: dict[str, Any], tokens: dict[str, str]) -> tuple[str | None, str | None]:
    """Resolve the caller from a static token map.

    Returns `(identity, error)`:

    * `(None, None)`   — auth is disabled (no tokens configured). Callers treat
                         this as "allowed, anonymous", which keeps the default
                         demo path working unchanged.
    * `(name, None)`   — a valid credential for `name`.
    * `(None, reason)` — reject with 401; `reason` is safe to return to the
                         client (it never echoes the presented token).
    """
    if not tokens:
        return None, None

    presented, why = _bearer(environ)
    if why:
        return None, why

    # Compare against every configured token so the work done is independent of
    # which entry matches (and of whether any does). `compare_digest` on str
    # requires ASCII, so a non-ASCII token is rejected rather than raising.
    try:
        matched = None
        for token, name in tokens.items():
            if hmac.compare_digest(token, presented):
                matched = name
    except TypeError:
        return None, "malformed bearer token"

    if matched is None:
        return None, "invalid bearer token"
    return matched, None


# ---- the combined entry point ----------------------------------------------

def resolve(environ: dict[str, Any], cfg, *, cache=None,
            fetch=None) -> tuple[str | None, str | None, int | None, str | None]:
    """Resolve the caller. `(identity, issuer, status, reason)` — handlers call this.

    `issuer` records *who vouched* for the identity: `"github"` for a verified
    sign-in, `None` for a static token. A reader of the event can then tell a
    real identity from a name an operator typed into an environment variable.

    `status` is the HTTP status to refuse with, and it carries real information:

    * `401` — "I do not know you": no credential, a malformed one, or one GitHub
      does not recognise.
    * `403` — "I know exactly who you are, and you are not approved." A real,
      authenticated person who is not on the list.

    Collapsing those into one answer would tell an operator less, and would tell
    a user debugging their own access much less.

    GitHub sign-in is active when a client id **or** an approved-user list is set
    — deliberately an `or`, so a half-configured deployment fails closed rather
    than silently falling back to no authentication. The two halves behave
    differently, and it is worth knowing which mistake you have made:

    * **client id only** — every request gets `403`, because the approved list is
      empty and an empty list approves nobody.
    * **approved list only** — any GitHub token from an approved login is
      accepted, with no OAuth App involved. Useful for a quick test with a
      personal access token; not what you want in a deployment.

    Within that, checked in order:

    1. **Static tokens** (`EB_AUTH_TOKENS`) — a local constant-time compare, and
       the break-glass path when GitHub is unreachable, so it goes first.
    2. **GitHub sign-in** — the real path.

    Static tokens also keep tests off the network and an offline demo possible,
    which is why they are not removed now that sign-in exists.

    With neither configured the result is all-`None` — allowed and
    anonymous, which is what keeps the default demo working out of the box.
    """
    from eventbridge import ghauth

    github_on = bool(getattr(cfg, "github_client_id", "")) or bool(
        getattr(cfg, "allowed_users", frozenset()))

    if github_on:
        presented, why = _bearer(environ)
        if why:
            # No usable credential at all. Falling back to a static token here
            # would be dead code: `resolve_identity` re-reads the same header via
            # `_bearer` and fails for the same reason. The break-glass path is the
            # branch below, which is the case that matters — a token WAS presented
            # and GitHub could not vouch for it.
            return None, None, 401, why

        # Static tokens first, because they are the break-glass path and this is a
        # local constant-time compare. Checking GitHub first made the fallback
        # slowest exactly when it is needed: during an outage every break-glass
        # request paid a full `fetch_login` timeout on a call that could never
        # succeed, against the same API budget the cache exists to protect.
        #
        # Nothing is shadowed by the order. A GitHub token is `gho_`/`ghp_`-shaped
        # and an operator choosing a static secret that collides with a live
        # GitHub token would have to do so deliberately.
        if cfg.auth_tokens:
            name, _ = resolve_identity(environ, cfg.auth_tokens)
            if name:
                return name, None, None, None

        login, err = ghauth.resolve(
            presented, cache, **({"fetch": fetch} if fetch else {}))
        if login is None:
            return None, None, 401, err or "could not identify this token"

        if not ghauth.is_allowed(login, cfg.allowed_users):
            # Name the login in the refusal: the user knows who they are, and
            # being told which identity was refused is what makes it actionable.
            return None, None, 403, f"{login} is not on the approved-user list"
        return login, "github", None, None

    name, why = resolve_identity(environ, cfg.auth_tokens)
    if why:
        return None, None, 401, why
    return name, None, None, None


def resolve_caller(environ: dict[str, Any], cfg, *, cache=None, fetch=None,
                   registry=None) -> tuple[Caller | None, int | None, str | None]:
    """`resolve`, plus the tenancy key and the registry check. `(caller, status, reason)`.

    This is the Phase 3 entry point; `resolve` keeps its 4-tuple shape so Phase 2's
    callers and tests are untouched.

    Two refusals exist only in `multi` mode, and both are deliberate disagreements with
    the single-tenant defaults rather than oversights:

    * **Anonymous is `401`.** Phase 2's "no auth configured means allowed and anonymous"
      is what makes the demo work out of the box, and it cannot coexist with per-user
      isolation: there is no user to isolate, so there is no store to write to and no
      topic to publish on.
    * **Not in the registry is `403`.** The registry is the approved list in `multi`
      mode, and an empty one approves nobody — Phase 2 §2.4's reading, for Phase 2's
      reason. `403` rather than `404` follows Phase 2 §2.4 too: this is the "I know
      exactly who you are and you are not approved" case, and naming the refused
      identity is what makes it actionable for someone who already knows who they are.
      (§6.2's `404`-not-`403` rule is a different question — hiding whether another
      tenant's correlation exists — and does not apply here.)
    """
    userid, issuer, status, reason = resolve(environ, cfg, cache=cache, fetch=fetch)
    if status:
        return None, status, reason

    mode = getattr(cfg, "tenancy_mode", tenancy.SINGLE)
    if mode != tenancy.MULTI:
        return caller_for(userid, issuer, tenancy_mode=mode), None, None

    if userid is None:
        return None, 401, ("this deployment is multi-tenant; an authenticated identity "
                           "is required (EB_TENANCY_MODE=multi)")

    user = registry.lookup(issuer, userid) if registry is not None else None
    if user is None:
        return None, 403, f"{userid} is not in the user registry"

    caller = caller_for(userid, issuer, tenancy_mode=mode, tier=user.tier)
    # The registry's recorded key is authoritative when present: `parse` has already
    # refused any file whose recorded key disagrees with this build's derivation, so
    # these are equal — using the registry's value documents which one wins if that
    # check is ever relaxed.
    return Caller(userid=caller.userid, issuer=caller.issuer,
                  userkey=user.userkey, tier=user.tier), None, None
