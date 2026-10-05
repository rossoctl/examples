"""The approved-user registry. DESIGN_PHASE3.md §2.5.

Phase 2's `EB_ALLOWED_USERS` answers one question — may this person submit? Per-user
topics, quotas, agent defaults and a runner Deployment need more, so the list gains a
structured sibling. It does not *replace* it: an operator with ten logins in an env var
should not have to write a file, so both are read and `EB_ALLOWED_USERS` keeps working.

Four decisions, each defending itself:

* **`userkey` is written in the file, not only computed.** It is derived, so it could be
  recomputed at every startup — but it names Kafka topics, Kubernetes objects and an
  ntfy topic that already exist. If a future change to `_slug` or `ISS_PREFIX` shifted
  the derivation, a recomputing bridge would silently start publishing to new, empty
  topics and every user's history would appear to vanish. So the file records it,
  startup recomputes it, and a mismatch is a **startup refusal naming both values**.
  Pinning the output of a derivation you intend to keep stable is cheap; discovering it
  drifted is not.
* **An empty registry denies everyone**, exactly as Phase 2 §2.4's empty list does, and
  for the same reason: the other reading turns a missing file into an open door.
* **It is a ConfigMap, not a Secret.** Nothing in it grants access; it records what an
  operator approved. Per-user ntfy tokens and Kafka SCRAM passwords are the parts that
  *do* grant access and live in Secrets provisioned alongside.
* **Provisioning is not automatic.** A user in the registry whose topics do not exist
  gets a `503` naming the missing topic, never a request published into the void.
  `scripts/k8s_tenant.py` (T12) renders the objects from this same file.

Stdlib only (`json`, `pathlib`).
"""
from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass, field

from shared import tenancy

# §3.8. `isolated` gets its own topics, runner Deployment and store; `shared` rides the
# shared tier's topics and store, which is what single-tenant mode uses for everyone.
TIER_SHARED = "shared"
TIER_ISOLATED = "isolated"
TIERS = (TIER_SHARED, TIER_ISOLATED)

SUPPORTED_VERSION = 1


class RegistryError(Exception):
    """A registry that cannot be loaded, or whose contents disagree with themselves.

    Raised at startup rather than per-request: a bridge that half-loaded its tenancy
    configuration cannot be trusted to route events, and the failure mode of continuing
    is one user's events in another user's store.
    """


@dataclass(frozen=True)
class Limits:
    """Per-user ceilings. 0 means "no limit from the registry"."""
    requests_per_hour: int = 0
    max_concurrent: int = 0
    transcript_bytes: int = 0


@dataclass(frozen=True)
class User:
    issuer: str
    userid: str
    userkey: str
    tier: str = TIER_SHARED
    # §5.1: the agent this user's requests default to, when the request does not name
    # one. Empty falls through to the runner's own ER_AGENT_NAME, then `default`.
    agent: str = ""
    limits: Limits = field(default_factory=Limits)

    @property
    def isolated(self) -> bool:
        return self.tier == TIER_ISOLATED


@dataclass(frozen=True)
class Registry:
    """An immutable snapshot, loaded once at startup.

    Live reload is deliberately absent, matching `keyset.py`'s reasoning: a mid-run edit
    must not silently widen the set of tenants this bridge serves, and a registry change
    needs `k8s_tenant.py` to provision topics anyway, so it is already not a
    file-edit-only operation.
    """
    users: tuple[User, ...] = ()

    def __len__(self) -> int:
        return len(self.users)

    @property
    def userkeys(self) -> tuple[str, ...]:
        return tuple(u.userkey for u in self.users)

    def lookup(self, issuer: str | None, userid: str | None) -> User | None:
        """Find an approved user by (issuer, userid).

        Matching goes through `tenancy.userkey` rather than comparing `userid` strings,
        so the comparison inherits the per-issuer canonicalisation for free: a GitHub
        login differing only in case matches, an email differing only in domain case
        matches, and a static identity must be byte-exact. Re-implementing that compare
        here is how the two drift apart.
        """
        if not userid:
            return None
        want = tenancy.userkey(issuer or "static", userid)
        for u in self.users:
            if u.userkey == want:
                return u
        return None

    def by_userkey(self, userkey: str) -> User | None:
        for u in self.users:
            if u.userkey == userkey:
                return u
        return None


def parse(raw: str) -> Registry:
    """Parse registry JSON. Every inconsistency is a refusal, never a skipped entry.

    This differs from `auth.parse_tokens`, which skips malformed entries on purpose — a
    typo in one token must not take the service down, and a skipped token simply fails
    to authenticate, which is closed. Here the failure direction is the opposite: a
    skipped user is a user whose topics exist and whose events have nowhere to go, or
    worse, one whose `userkey` collides with somebody else's. So this refuses loudly.
    """
    try:
        d = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RegistryError(f"registry is not valid JSON: {e}") from e
    if not isinstance(d, dict):
        raise RegistryError("registry must be a JSON object")

    version = d.get("version")
    if version != SUPPORTED_VERSION:
        # Refusing an unknown version rather than reading what it recognises: a v2 file
        # may well mean something different by a field this code thinks it understands.
        raise RegistryError(
            f"registry version {version!r} is not supported (expected {SUPPORTED_VERSION})")

    entries = d.get("users")
    if entries is None:
        entries = []
    if not isinstance(entries, list):
        raise RegistryError("registry `users` must be a list")

    users: list[User] = []
    seen_keys: dict[str, str] = {}
    for i, e in enumerate(entries):
        where = f"users[{i}]"
        if not isinstance(e, dict):
            raise RegistryError(f"{where} must be an object")
        issuer = str(e.get("issuer", "")).strip()
        userid = str(e.get("userid", "")).strip()
        if not issuer or not userid:
            raise RegistryError(f"{where} needs both issuer and userid")

        computed = tenancy.userkey(issuer, userid)
        declared = str(e.get("userkey", "")).strip()
        if declared and declared != computed:
            # The startup refusal §2.5 asks for, naming BOTH values — the whole point is
            # that an operator can see which one the live topics were named after.
            raise RegistryError(
                f"{where} ({issuer}/{userid}): registry records userkey {declared!r} but "
                f"this build derives {computed!r}. The recorded value names topics and "
                f"Kubernetes objects that already exist, so this build would publish to "
                f"new, empty topics and the user's history would appear to vanish. "
                f"Refusing to start. See DESIGN_PHASE3.md §2.5.")

        # The 32-bit birthday bound from §2.3, turned from a silent cross-tenant leak
        # into a refused signup. Two DIFFERENT identities producing one key is the
        # failure; the same identity listed twice is merely a duplicate, and is also
        # refused because it makes the per-user limits ambiguous.
        if computed in seen_keys:
            raise RegistryError(
                f"{where} ({issuer}/{userid}) collides with {seen_keys[computed]} on "
                f"userkey {computed!r}. Two identities cannot share a tenancy key.")
        seen_keys[computed] = f"{issuer}/{userid}"

        tier = str(e.get("tier", TIER_SHARED)).strip() or TIER_SHARED
        if tier not in TIERS:
            raise RegistryError(f"{where}: tier {tier!r} not one of {TIERS}")

        lim = e.get("limits") or {}
        if not isinstance(lim, dict):
            raise RegistryError(f"{where}: limits must be an object")
        try:
            limits = Limits(
                requests_per_hour=int(lim.get("requests_per_hour", 0)),
                max_concurrent=int(lim.get("max_concurrent", 0)),
                transcript_bytes=int(lim.get("transcript_bytes", 0)),
            )
        except (TypeError, ValueError) as exc:
            raise RegistryError(f"{where}: limits must be integers") from exc

        agent = str(e.get("agent", "")).strip()
        if agent:
            # The same gate the HTTP boundary applies (`handlers._agent_for`), and the
            # last field in this function that was accepted unchecked — against a
            # docstring promising "every inconsistency is a refusal". A startup refusal
            # rather than a 400, because this value reaches `ce_agent` via
            # `by_userkey(...).agent` on a path that never sees `validate_name`: the
            # runner then rejects it, so a registry typo would mean an asynchronous
            # `phase=error` on EVERY request from that user — the failure mode the
            # request path was moved away from.
            from eventrunner import agentspec
            try:
                agentspec.validate_name(agent)
            except agentspec.SpecError as exc:
                raise RegistryError(f"{where} ({issuer}/{userid}): {exc}") from None

        users.append(User(issuer=issuer, userid=userid, userkey=computed, tier=tier,
                          agent=agent, limits=limits))

    return Registry(users=tuple(users))


def load(path: str) -> Registry:
    """Load the registry from `EB_USER_REGISTRY_PATH`. Empty path = empty registry.

    An empty registry is returned rather than raising, because whether "nobody is
    approved" is an error depends on the tenancy mode: it is fatal in `multi` and
    irrelevant in `single`. `__main__` decides that, where the mode is known.
    """
    if not path:
        return Registry()
    p = pathlib.Path(path)
    if not p.is_file():
        raise RegistryError(f"registry path {path!r} does not exist")
    try:
        raw = p.read_text()
    except OSError as e:
        raise RegistryError(f"cannot read registry {path!r}: {e}") from e
    return parse(raw)
