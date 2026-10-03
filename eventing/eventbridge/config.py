"""Environment > config.toml > baked defaults. Stdlib only."""
from __future__ import annotations

import os
import pathlib
import re
import tomllib
from dataclasses import dataclass, field

from eventbridge import auth, ghauth
from shared import tenancy


@dataclass
class NtfyCfg:
    enabled: bool = True
    base_url: str = "https://ntfy.sh"
    topic: str = ""
    token: str = ""
    phases: tuple[str, ...] = ("result", "error")
    # §21.6/§21.9: notify on a group MEMBER's error even while the group is open.
    # Default false to match the brief as written; recommended true, because a
    # silently failing batch looks exactly like a healthy one until it ends.
    group_notify_errors: bool = False


@dataclass
class Cfg:
    kafka_bootstrap: str = "localhost:9092"
    request_topic: str = "requests"
    response_topic: str = "responses"
    http_addr: str = "127.0.0.1:8080"
    http_workers: int = 8
    public_base_url: str = "http://127.0.0.1:8080"
    source_uri: str = "rossoctl://eventbridge/local"
    tmpdir: str = ""
    # §16 Gap B / T1.8: cap on a checkpointed session transcript. A trivial
    # one-turn transcript already measures ~222 KB (the system prompt and tool
    # definitions dominate), so a small cap would reject the very first
    # checkpoint. 32 MiB is a very long conversation and still fits in memory.
    transcript_max_bytes: int = 32 * 1024 * 1024
    # §21.9.2 straggler cutoff. Without a deadline a group that is short one member
    # never completes, and therefore never sends its completion notification — the
    # failure mode is silence, which is the worst one. 0 disables.
    group_deadline_s: float = 3600.0
    # Bearer tokens for the submit path, as {token: name}. Empty disables auth
    # entirely, which is the default: the demo must keep working out of the box,
    # and a token is a capability that cannot be baked into a default. Supplied
    # only via EB_AUTH_TOKENS — deliberately never read from config.toml, which
    # is committed (tests/test_manifests.py pins the same rule for NTFY_TOKEN).
    auth_tokens: dict[str, str] = field(default_factory=dict)
    # GitHub sign-in. The client id is PUBLIC — the OAuth device flow has no
    # client secret, which is why it can be committed while an ntfy topic cannot.
    github_client_id: str = ""
    # Who may submit. Empty means nobody is approved: if GitHub sign-in is
    # configured at all, an operator has to say who may use it, because the other
    # reading ("empty means everybody") turns a missing variable into an open door.
    allowed_users: frozenset[str] = frozenset()
    # Token -> login lookups are cached for this long. Not an optimisation: a
    # GitHub API call per request would spend a 5000/hour budget and add GitHub's
    # latency to the request path.
    github_cache_ttl_s: float = 300.0
    # §11 — signing the requests and group events EventBridge publishes. Empty
    # disables it, which is the default: a key is a capability, and the demo has to
    # work with none. Env only, never config.toml — the file it points at is a
    # Secret mount (same rule as auth_tokens and NTFY_TOKEN above).
    signing_key_path: str = ""
    # The kid written into the protected header, naming EventBridge's own key in the
    # approved set. Also the kid group lifecycle events must be signed by: the set is
    # otherwise flat, so without this any approved runner could forge a
    # group.completed and end a batch early.
    signing_kid: str = ""
    # §11 — the approved-key set used to verify responses coming back off the
    # responses topic. Empty disables verification entirely. A ConfigMap path, not a
    # Secret: only public keys belong in it.
    verify_keyset_path: str = ""
    # Whether a failed verification is ENFORCED. With a keyset but this false,
    # EventBridge verifies and logs but stores the event unchanged — audit mode.
    # Enforcement rewrites persisted rows and raises a priority-5 notification, so
    # there has to be a way to watch the reject rate before turning it on.
    require_response_signature: bool = False

    # ---- Phase 3 (DESIGN_PHASE3.md §8.1) ----

    # §3.2 / §8.1 — `single` is Phase 2 behaviour: one topic pair, one store, no
    # ownership checks. `multi` is everything in §3-§7. Default `single` so an existing
    # deployment that upgrades and changes nothing behaves exactly as it did.
    tenancy_mode: str = tenancy.SINGLE
    # §3.1 — first component of every per-user topic name. Unused in `single` mode,
    # where the configured request/response topics are returned verbatim.
    topic_prefix: str = "kev1"
    # §2.5 — the user registry ConfigMap. Empty in `multi` mode denies everyone, for
    # the same reason Phase 2's empty allowed_users does: the other reading turns a
    # missing file into an open door.
    user_registry_path: str = ""
    # §6.1 — LRU ceiling on open per-user SQLite stores. Two connections per tenant,
    # each in WAL mode (main + `-wal` + `-shm`), so the default 1024 soft RLIMIT_NOFILE
    # bounds this near 150-200 tenants; 64 leaves room for the Kafka sockets, the HTTP
    # listener and the worker pool.
    max_open_stores: int = 64

    ntfy: NtfyCfg = field(default_factory=NtfyCfg)

    @property
    def topics(self) -> tenancy.TopicSet:
        """§3.2's helper, built from this config.

        A property rather than a field so it cannot drift from `request_topic` /
        `response_topic` after `load()` — the topic names and the set that derives from
        them are one fact, and storing it twice is how they disagree.
        """
        return tenancy.TopicSet(
            self.topic_prefix,
            tenancy=self.tenancy_mode,
            request_topic=self.request_topic,
            response_topic=self.response_topic,
        )


def _root_tmpdir() -> str:
    base = os.environ.get("TMPDIR", "/tmp").rstrip("/")
    return f"{base}/rossoctl-keda1"


def load() -> Cfg:
    cfg = Cfg()
    cfg.tmpdir = _root_tmpdir()

    toml_path = pathlib.Path(__file__).parent / "config.toml"
    if toml_path.exists():
        d = tomllib.loads(toml_path.read_text())
        k = d.get("kafka", {})
        cfg.kafka_bootstrap = k.get("bootstrap", cfg.kafka_bootstrap)
        cfg.request_topic   = k.get("request_topic",  cfg.request_topic)
        cfg.response_topic  = k.get("response_topic", cfg.response_topic)
        h = d.get("http", {})
        cfg.http_addr    = h.get("addr", cfg.http_addr)
        cfg.http_workers = int(h.get("workers", cfg.http_workers))
        n = d.get("ntfy", {})
        cfg.ntfy = NtfyCfg(
            enabled=bool(n.get("enabled", cfg.ntfy.enabled)),
            base_url=n.get("base_url", cfg.ntfy.base_url),
            topic=n.get("topic", cfg.ntfy.topic),
            token=n.get("token", cfg.ntfy.token),
            phases=tuple(n.get("phases", cfg.ntfy.phases)),
        )
        cfg.public_base_url = n.get("public_base_url", cfg.public_base_url)
        g = d.get("github", {})
        cfg.github_client_id = g.get("client_id", cfg.github_client_id)
        if g.get("allowed_users"):
            cfg.allowed_users = frozenset(
                str(u).strip().lower() for u in g["allowed_users"] if str(u).strip())

    # env overrides
    e = os.environ.get
    cfg.kafka_bootstrap = e("KAFKA_BOOTSTRAP", cfg.kafka_bootstrap)
    cfg.request_topic   = e("REQUEST_TOPIC",   cfg.request_topic)
    cfg.response_topic  = e("RESPONSE_TOPIC",  cfg.response_topic)
    cfg.http_addr       = e("EB_HTTP_ADDR",    cfg.http_addr)
    cfg.http_workers    = int(e("EB_WORKERS",  str(cfg.http_workers)))
    # EVENT_BRIDGE_PUBLIC_BASE_URL is the externally-reachable URL EventBridge
    # advertises in the HTML view + ntfy Click/Actions. Used VERBATIM — the
    # ntfy publisher does not second-guess whether it's loopback or LAN.
    cfg.public_base_url = e("EVENT_BRIDGE_PUBLIC_BASE_URL", cfg.public_base_url)
    cfg.transcript_max_bytes = int(e("EB_TRANSCRIPT_MAX_BYTES",
                                     str(cfg.transcript_max_bytes)))
    cfg.group_deadline_s = float(e("EB_GROUP_DEADLINE_S", str(cfg.group_deadline_s))) or None
    # `name:token,name2:token2`. Absent or empty leaves auth disabled — same
    # guard shape as NTFY_PHASES below, so an unset var never clobbers a default.
    auth_env = e("EB_AUTH_TOKENS")
    if auth_env:
        cfg.auth_tokens = auth.parse_tokens(auth_env)
    cfg.github_client_id = e("EB_GITHUB_CLIENT_ID", cfg.github_client_id)
    allowed_env = e("EB_ALLOWED_USERS")
    if allowed_env:
        cfg.allowed_users = ghauth.parse_allowed_users(allowed_env)
    cfg.github_cache_ttl_s = float(e("EB_GITHUB_CACHE_TTL_S",
                                     str(cfg.github_cache_ttl_s)))
    # §11. Env only: the seed path names a Secret mount, and keeping the whole
    # signing block in one layer means it cannot be half-configured from a
    # committed file.
    cfg.signing_key_path    = e("EB_SIGNING_KEY_PATH",    cfg.signing_key_path)
    cfg.signing_kid         = e("EB_SIGNING_KID",         cfg.signing_kid)
    cfg.verify_keyset_path  = e("EB_VERIFY_KEYSET_PATH",  cfg.verify_keyset_path)
    cfg.require_response_signature = (
        e("EB_REQUIRE_RESPONSE_SIGNATURE",
          "true" if cfg.require_response_signature else "false").lower() == "true")

    # §8.1. Env only for the mode and the registry path: tenancy is a deployment-shape
    # decision, and a committed config.toml that could flip it on is a way to get
    # per-user isolation half-enabled by accident.
    cfg.tenancy_mode = (e("EB_TENANCY_MODE", cfg.tenancy_mode) or "").strip().lower()
    if cfg.tenancy_mode not in (tenancy.SINGLE, tenancy.MULTI):
        raise SystemExit(
            f"EB_TENANCY_MODE must be {tenancy.SINGLE!r} or {tenancy.MULTI!r}, "
            f"got {cfg.tenancy_mode!r}. See DESIGN_PHASE3.md §8.1.")
    cfg.topic_prefix = e("EB_TOPIC_PREFIX", cfg.topic_prefix)
    # The prefix is the one component of a topic name `tenancy._slug` never sees, so it
    # is validated here instead. A prefix containing `_` or `.` produces exactly the JMX
    # metric-name collision that `_slug`'s docstring warns about (`a.b_c` and `a_b.c`
    # flatten to the same metric and one silently overwrites the other), and a space or
    # `/` is simply an illegal topic name.
    if not re.fullmatch(r"[a-zA-Z0-9-]{1,32}", cfg.topic_prefix or ""):
        raise SystemExit(
            f"EB_TOPIC_PREFIX must be 1-32 of [a-zA-Z0-9-], got "
            f"{cfg.topic_prefix!r}. `.` and `_` are excluded on purpose: Kafka's JMX "
            f"metric names flatten both, so two topics differing only in which one they "
            f"use silently share one metric. See DESIGN_PHASE3.md §3.1.")
    cfg.user_registry_path = e("EB_USER_REGISTRY_PATH", cfg.user_registry_path)
    # Readable refusal rather than a bare ValueError traceback, matching every other
    # refusal on this startup path.
    try:
        cfg.max_open_stores = int(e("EB_MAX_OPEN_STORES", str(cfg.max_open_stores)))
    except ValueError:
        raise SystemExit(
            f"EB_MAX_OPEN_STORES must be an integer, got "
            f"{e('EB_MAX_OPEN_STORES')!r}") from None

    cfg.ntfy.enabled  = (e("NTFY_ENABLED", "true" if cfg.ntfy.enabled else "false").lower() == "true")
    cfg.ntfy.base_url = e("NTFY_BASE_URL", cfg.ntfy.base_url)
    cfg.ntfy.topic    = e("NTFY_TOPIC",    cfg.ntfy.topic)
    cfg.ntfy.token    = e("NTFY_TOKEN",    cfg.ntfy.token)
    cfg.ntfy.group_notify_errors = (
        e("NTFY_GROUP_NOTIFY_ERRORS", "true" if cfg.ntfy.group_notify_errors else "false")
        .lower() == "true")
    phases_env = e("NTFY_PHASES")
    if phases_env:
        cfg.ntfy.phases = tuple(p.strip() for p in phases_env.split(",") if p.strip())

    (pathlib.Path(cfg.tmpdir) / "eventbridge").mkdir(parents=True, exist_ok=True)
    return cfg
