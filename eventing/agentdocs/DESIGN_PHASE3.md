# DESIGN — Phase 3: per-user isolation, declarative agents, and event triggers

Status: draft (revision 1) — **design only, nothing in this document is implemented yet.**
Scope: **delta over `DESIGN_PHASE2.md`.** Read that first, and §2.1 of it before
anything else here.

Phase 2 learned *who* is asking. It put a name on the event (`ce_submitter`) and
refused submissions from anyone not on a list. But the name was an **audit label**:
every user's events still flowed through one pair of Kafka topics, into one SQLite
store, out to one ntfy topic, and were executed by one pool of EventRunner pods
holding one Anthropic credential. Knowing who asked is not the same as keeping two
people apart.

Phase 3 turns `{userid}` from a label into a **tenancy key**, and then uses the
isolation that creates to make two things safe that would be reckless without it:

1. **Agents defined by data rather than by the image** — a skill fetched from GitHub,
   S3 or a URL, run headless, reporting back over the event path (§5).
2. **Triggers** — "an event of type X runs agent Y", modelled on Knative Eventing
   (§7). A trigger is an agent that starts without a human in the loop, which is
   exactly when a blast radius needs a boundary around it.

```text
   👤 alice ──┐                        ┌─ kev1-gh-alice-a1b2c3d4-requests ─▶ 🤖 runner-alice (KEDA 0..3)
              │   ╔════════════════╗   │                                          │
   👤 bob ────┼──▶║ 🔒 EventBridge  ║───┤                                         ▼
              │   ║  PEP + Broker  ║   │  ◀── kev1-gh-alice-a1b2c3d4-responses ───┘
   🌐 webhook ┘   ╚════════════════╝   └─ kev1-ms-bob-9e8f7a6b-requests ───▶ 🤖 runner-bob (KEDA 0..3)
                    │         │
                    │         └─▶ 📱 ntfy topic kev1-k7qf3m… (derived, not bob's name)
                    └─▶ triggers: type/source/subject ──▶ which agent, which prompt
```

Three things are worth saying before the detail, because they are what this phase is
actually about:

- **Isolation here is enforced by EventBridge, not by Kafka**, unless you take §3.6's
  dedicated-broker option. Phase 2 §3.3 already rejected enabling the shared
  broker's authorizer, and that decision still binds. Separate topics without ACLs
  are *organisation*; separate topics with ACLs are *isolation*. This document says
  which one you get in each configuration, and never blurs them.
- **A trigger spends money without a human present.** Every safety control in §7.7
  exists because the natural failure of an event-driven agent system is not a crash,
  it is a loop that bills.
- **A fetched skill is code.** `SKILL.md` is instructions to a model with tools, and
  a `settings.json` with hooks is arbitrary command execution. §5.4 treats a skill
  bundle as a supply-chain artifact, with pinned digests and the same approved-key
  set that already authorizes agents.

---

## 1. What does NOT change

Stated explicitly, because the temptation in a multi-tenancy document is to rewrite
the parts that already work:

- **The CloudEvent contract** (Phase 0 §2). Phase 3 *adds* extension attributes
  (`userkey`, `agent`, `triggerid`, `depth`) and two event types (§7.2). No existing
  attribute changes shape or meaning. Every new name obeys the CloudEvents v1.0
  lower-case `[a-z0-9]` rule that Phase 2 §2.6 got wrong once already, and
  `test_roundtrip_binary.py` already pins that over every `EXT_*` constant.
- **Kafka message key = `correlationid`**, the per-correlation FIFO router, and the
  per-correlation ordering guarantee. Per-user topics *narrow* the key space; they do
  not change the keying rule.
- **`sessionuuid = uuid5(NAMESPACE, correlationid)`.** This is the one derivation it
  would be most tempting to salt with the user, and §2.6 explains why that would be a
  mistake and what is done instead.
- **EventBridge is single-replica** (Phase 1 §8.6). Per-user topics make that
  *more* load-bearing, not less — see §3.3 and the Phase 2 §6.1 split-consumer trap,
  which gets worse with pattern subscriptions.
- **KEDA scaling on consumer lag and scale-to-zero.** Per-user ScaledObjects are
  more of the same object, not a new mechanism. Scale-to-zero is what makes a runner
  per user affordable at all (§3.5).
- **Pure-Python discipline** (Phase 1 §1.1). No new runtime dependency anywhere in
  this phase: `urllib`, `hmac`, `hashlib`, `json`, `tarfile`, `base64`, `sqlite3`.
  This rules out two designs people reach for first — a Jinja template engine for
  trigger prompts (§7.4), and an AWS SDK for S3 skill sources (§5.3) — and §6.4
  explains why it also rules out encrypting transcripts ourselves.
- **Signing and the approved-key set** (Phase 2 §4). Phase 3 reuses `shared/keyset.py`
  for two new jobs — authenticating a runner's transcript PUT (§6.3) and authorizing a
  skill bundle (§5.4) — precisely because it is already the authorization list.
- **Phase 2's GitHub device flow and the opaque-token constraint** (§2.1 there). Phase
  3 adds a second issuer shape (email, §2.3) without changing how GitHub is verified.
- **Single-user mode is the default.** `EB_TENANCY_MODE=single` reproduces Phase 2
  byte for byte: the generic `requests`/`responses` topics, one store, one ntfy topic,
  no ownership checks. Multi-tenancy is opt-in, and the existing test suite runs
  unchanged against the default. That is the check that this is additive.

---

## 2. `{userid}`: from an audit label to a tenancy key

### 2.1 Why Phase 2's `submitter` cannot be used directly

`ce_submitter` holds whatever the issuer called the user: `mrsabath` from GitHub,
`alice@example.com` from a static token an operator typed, `Alice Example` if someone
is careless. Phase 2 only ever compared it and printed it, so its shape never
mattered.

As a tenancy key it has to do four new jobs, and it fails at all four:

| Job | Why raw `submitter` fails |
|---|---|
| Name a Kafka topic | Kafka topics allow `[a-zA-Z0-9._-]` only, max 249 chars. `alice@example.com` has an illegal `@`. |
| Name a Kubernetes object | DNS-1123: lower-case, `[a-z0-9-]`, no `.` for a label value, 63 chars for a label, 253 for a name. `MrSabath` has upper case. |
| Name an ntfy topic | `[-_A-Za-z0-9]{1,64}`. And the name *travels to the phone* — see §4.1. |
| Separate two tenants | This is the one that matters. Any lossy normalisation can map two identities onto one key, and then user B reads user A's events. |

The fourth is a security bug, not a cosmetic one, and it is easy to walk into.
Replacing every illegal character with `-` maps `a.b@x.com`, `a_b@x.com` and
`a-b@x.com` onto the same `a-b-x-com`. Lower-casing maps GitHub's `Alice` and `alice`
onto one key — which is *correct* for GitHub, because its logins are
case-insensitive (Phase 2 §2.4), and *wrong* for an issuer whose identifiers are not.

### 2.2 The two identifier shapes, and what each is worth

The brief asks for "an identifier such as a GitHub username or email". These are not
interchangeable and the difference is worth recording.

**A GitHub login** is already almost a valid key: 1–39 characters of
`[a-zA-Z0-9-]`, no leading or trailing hyphen, no consecutive hyphens,
case-insensitive. Lower-cased, it is a legal Kafka topic component, a legal DNS-1123
label, and a legal ntfy topic — with no transformation at all. That is why the common
case stays readable, and it is worth not throwing away.

**An email address** is the hard case, in three ways:

- *It is not a verified claim unless somebody verified it.* Phase 2's static-token
  path is an operator typing a string into an environment variable. An email from
  there is exactly as trustworthy as the operator, which `ce_submitteriss` already
  records (absent issuer = static). Phase 3 does not add email verification; it adds
  a *slot* for an issuer that verified one (OIDC `iss` + `email_verified`), and until
  such an issuer is configured an email identity is the weaker claim and is labelled
  as such.
- *Normalisation is provider-specific and we must not guess.* `A.Lice@Gmail.com` and
  `alice@gmail.com` are the same mailbox at Gmail and may be different mailboxes
  elsewhere. We therefore normalise only what the RFCs permit — lower-case the
  domain, leave the local part byte-exact — and accept the consequence: one human
  with two spellings gets two tenants. That is **wasteful, not unsafe**, and it is the
  right direction to err. The unsafe direction (two humans, one tenant) is what the
  digest in §2.3 makes impossible.
- *It is long.* 254 octets, of which 64 may be the local part. It cannot be embedded
  whole in a 64-character ntfy topic alongside a prefix.

### 2.3 `userkey` — the normalisation, and why it ends in a hash

One pure function, in a new `shared/tenancy.py`, is the only place an identity becomes
a name:

```python
# shared/tenancy.py
ISS_PREFIX = {"github": "gh", "oidc": "oi", "static": "st"}

def userkey(issuer: str, userid: str) -> str:
    """Map (issuer, userid) to a short, legal, COLLISION-FREE tenancy key.

    Shape: <iss>-<slug>-<8 hex>   e.g. gh-alice-a1b2c3d4
                                       oi-alice-example-com-9e8f7a6b
                                       st-ops-break-glass-00ff1122

    Legal as: a Kafka topic component, a DNS-1123 label, an ntfy topic component.
    """
    iss = ISS_PREFIX.get(issuer, "xx")
    canon = _canonicalise(issuer, userid)          # the ONLY lossy step, see below
    digest = hashlib.sha256(
        f"{issuer}\x1f{canon}".encode()).hexdigest()[:8]
    slug = _slug(canon)[:24].strip("-") or "u"
    return f"{iss}-{slug}-{digest}"
```

Three properties, each defending a decision:

**The digest is the security control, not decoration.** `_slug` is deliberately
lossy — that is what makes the key readable in `kubectl get kafkatopics` and in a log
line. Lossiness is survivable *only* because the 32 bits of SHA-256 over the
canonical form are appended, so two distinct canonical identities cannot produce one
key. 32 bits is not collision-*resistant* in the cryptographic sense — a birthday
collision is expected around 2^16 ≈ 65,000 users — so the registry (§2.5) rejects a
key that is already taken by a different identity, turning a one-in-65,000 accident
into a refused signup instead of a silent cross-tenant leak. Raise to 12 hex if a
deployment ever approaches that scale.

**The issuer is in the key and in the digest input.** GitHub user `alice` and OIDC
subject `alice` are different people. Without the issuer in the hashed input, a
second issuer is a way to impersonate the first — the single worst failure available
here. It appears twice on purpose: in the prefix so a human can see which issuer a
topic belongs to, and in the digest so it is load-bearing rather than cosmetic.

**Canonicalisation is per-issuer and minimal:**

```python
def _canonicalise(issuer: str, userid: str) -> str:
    s = userid.strip()
    if issuer == "github":
        return s.lower()                     # GitHub logins ARE case-insensitive
    if "@" in s:                             # email: domain only, per RFC 5321
        local, _, domain = s.rpartition("@")
        return f"{local}@{domain.lower()}"
    return s                                 # static: byte-exact, operator's problem
```

The GitHub branch must match `ghauth.is_allowed`'s case-insensitive compare (Phase 2
§2.4) exactly. If the allow-list compares case-insensitively and the key does not,
`Alice` and `alice` are both approved and land in different tenants — one human with
two halves of their history, which reads as data loss. A single test asserting the
two functions agree is cheaper than discovering it live.

### 2.4 Where the identity comes from, per issuer

`auth.resolve()` already returns `(identity, issuer, status, reason)`. Phase 3 widens
the return to carry the key, so no caller derives it twice:

```python
# eventbridge/auth.py
@dataclass(frozen=True)
class Caller:
    userid: str | None        # "mrsabath" | "alice@example.com" | None (anonymous)
    issuer: str | None        # "github" | "oidc" | None (static)
    userkey: str | None       # tenancy.userkey(...), None in single-tenant mode
    tier: str = "shared"      # §3.8
```

| Issuer | `userid` is | Verified by | `ce_submitteriss` |
|---|---|---|---|
| `github` | the login from `GET /user` | GitHub, per Phase 2 §2.1 | `github` |
| `oidc` | `email` when `email_verified`, else `sub` | **not implemented in Phase 3** — the slot exists, the verifier does not | `oidc` |
| `static` | the name left of the colon in `EB_AUTH_TOKENS` | an operator | absent |

The OIDC row is a slot, and saying so is the point: it is where a Keycloak or
Entra deployment plugs in, and Phase 2 §2.1's constraint (no verifiable GitHub user
token) does not apply there, so it can be a local JWT verification with no network
call and no cache. Until something fills it, "email" identities in Phase 3 come from
the static path and carry the weaker claim.

`anonymous`: in `EB_TENANCY_MODE=multi`, an anonymous caller is refused with `401`.
Phase 2's "no auth configured means allowed and anonymous" default is what keeps the
demo working out of the box, and it is incompatible with per-user isolation — there is
no user to isolate. The two modes disagree about this on purpose, and startup prints
which one is in force.

### 2.5 The user registry replaces `EB_ALLOWED_USERS`

A comma-separated list of logins answers one question (may this person submit?). Per-user
topics, quotas, agent defaults and a runner Deployment need more, so `EB_ALLOWED_USERS`
gains a structured sibling — and keeps working, because an operator with ten logins in
an env var should not have to write a file.

```json
// EB_USER_REGISTRY_PATH — a ConfigMap. No secrets: see the note below.
{
  "version": 1,
  "users": [
    { "issuer": "github", "userid": "mrsabath",
      "userkey": "gh-mrsabath-4c1d9e07",
      "tier": "isolated",
      "agent": "triager",
      "limits": { "requests_per_hour": 100, "max_concurrent": 3,
                  "transcript_bytes": 268435456 } },
    { "issuer": "github", "userid": "aslom",
      "userkey": "gh-aslom-7b2e55a1", "tier": "isolated" },
    { "issuer": "static", "userid": "ops-break-glass",
      "userkey": "st-ops-break-glass-00ff1122", "tier": "shared" }
  ]
}
```

Four decisions here:

- **`userkey` is written in the file, not only computed.** It is derived, so it *could*
  be recomputed at every startup — but it names Kafka topics, Kubernetes objects and
  an ntfy topic that already exist. If a future change to `_slug` or `ISS_PREFIX`
  shifts the derivation, a recomputing bridge would silently start publishing to new,
  empty topics and every user's history would appear to vanish. So the file records it,
  startup recomputes it, and a mismatch is a **startup refusal** naming both values.
  Pinning the output of a derivation you intend to keep stable is cheap; discovering
  it drifted is not.
- **An empty registry denies everyone**, exactly as Phase 2 §2.4's empty list does, and
  for the same reason: the other reading turns a missing file into an open door.
- **It is a ConfigMap, not a Secret.** Nothing in it grants access; it records what an
  operator approved. Phase 2 §5 draws this line for keysets and seeds and this file
  sits on the same side. Per-user ntfy tokens and Kafka SCRAM passwords are the parts
  that *do* grant access, and they live in Secrets provisioned alongside (§3.6, §4.3).
- **Provisioning is not automatic.** A new approved user needs topics, a runner
  Deployment, a ScaledObject and possibly a KafkaUser. EventBridge has no Kubernetes
  client (Phase 1 §8.7 / §1.1) and will not grow one. So `scripts/k8s_tenant.py`
  (§9) renders and applies those objects from this same file, and the registry is the
  single source both it and EventBridge read. A user in the registry whose topics do
  not exist gets a `503` naming the missing topic, never a request published into the
  void.

### 2.6 What rides on the event, and the `sessionuuid` trap

One new extension attribute, and a deliberate non-change:

```text
ce_submitter:    mrsabath                 # unchanged, Phase 2 §2.6
ce_submitteriss: github                   # unchanged
ce_userkey:      gh-mrsabath-4c1d9e07     # NEW — the tenancy key
```

`userkey` joins `signing.SIGNED_ATTRS`. It must: it is what decides which store a
response is written into and which ntfy topic it is announced on, so an unsigned,
mutable `userkey` would let anything with write access to a topic file an event into
another user's history. See §8.3 on the canonicalisation break that adding it causes,
and why it has to land with `depth` in one change rather than two.

**The trap.** It is tempting to salt the session derivation —
`uuid5(NAMESPACE, userkey + "/" + corr)` — so two tenants can never collide in
`claude`'s session store. Do not. Three reasons:

1. It breaks every existing session. `--resume` is keyed on that uuid, and Phase 1
   §16 Gap B's checkpoint/restore path (`verify_resume_by_path.py`) is built on it.
2. It is listed in Phase 2 §1 as a thing that does not change, and a reader of these
   documents should be able to trust that list.
3. **It is not necessary.** The collision it prevents is two users minting the same
   `correlationid`, and §6.2 already has to prevent that for an unrelated reason:
   owner-scoped reads need a global `correlationid → userkey` index, consulted before
   any tenant is chosen. A **unique** constraint on `correlationid` in that index is
   therefore a global uniqueness check costing one indexed lookup per mint — and it has
   to exist whether or not the session derivation is salted.

So `Minter.mint()` consults that index instead of a `seen` set seeded from every
tenant's store. That is the cheaper design as well as the simpler one: seeding would
mean reading `all_correlations()` from N per-user stores (§6.1), so N SQLite opens
before the socket binds, and at 100 tenants with 10,000 correlations each — the cap
`__main__.py` already applies to the single-store seed today — that is a measurable
startup delay for a guarantee one `SELECT` gives directly.

**The constraint has to be on `correlationid` alone**, and this is the part to get
right. `(userkey, correlationid)` is the correct primary key *inside* a tenant's store
(§6.1) and is exactly the wrong constraint for the global index: two tenants minting one
`corr` produce two different tuples, so it would never conflict and the collision would
pass through silently — which is the failure the whole guarantee exists to prevent.

---

## 3. Kafka topic isolation

### 3.1 Naming

The brief's shape, with `{userid}` replaced by `{userkey}` for the reasons in §2.3:

| Topic | Name | Written by | Read by |
|---|---|---|---|
| requests | `{prefix}-{userkey}-requests` | EventBridge | that user's EventRunner |
| responses | `{prefix}-{userkey}-responses` | that user's EventRunner | EventBridge |
| inbox (§7) | `{prefix}-{userkey}-events` | EventBridge ingress (`POST /v0/events`) | EventBridge trigger dispatcher |
| dead letter (§7.5) | `{prefix}-{userkey}-dead` | EventBridge | nobody; an operator with `kafka-console-consumer` |

`{prefix}` defaults to the namespace (`kev1`), configurable as `EB_TOPIC_PREFIX`,
exactly as Phase 1 §5 prefixes the shared-broker topics today. Longest realistic
name: `kev1-` + 2 + 1 + 24 + 1 + 8 + `-responses` = 51 characters, against Kafka's
249 — comfortable, and worth having checked rather than assumed.

Two naming rules that bite if ignored:

- **Never put both `.` and `_` in a topic name.** Kafka's JMX metric names flatten
  both, so `a.b_c` and `a_b.c` collide and one topic's metrics silently overwrite the
  other's. `_slug` emits `-` only, so this cannot happen — stated here because the
  next person to "improve" the slug needs to know.
- **A Strimzi `KafkaTopic`'s `metadata.name` is a Kubernetes name, not a topic name.**
  They happen to coincide here because `userkey` is DNS-safe by construction. Where
  they would not, `spec.topicName` carries the real one. Relying on the coincidence
  without `spec.topicName` is the kind of thing that works until a userkey gets a
  character Kubernetes rejects.

### 3.2 EventBridge: the producer is easy, the consumer is not

**Producing is a one-line change.** One `KafkaProducer` can send to any topic; the
topic is an argument to `send()`, not to the constructor. `Producer.publish_request`
grows a `topic` parameter resolved from the caller's `userkey`:

```python
# eventbridge/kafka_out.py
def publish_request(self, *, prompt, correlationid, ..., userkey=None):
    topic = self._topics.requests(userkey)      # {prefix}-{userkey}-requests
    ...
    future = self._prod.send(topic, key=correlationid.encode(), ...)
```

No second producer, no connection per user, and the ~150–200 ms Ed25519 cost per
published request (Phase 2's `kafka_out` docstring) is unchanged.

**Consuming is where the design is.** EventBridge runs three consumers today —
`kafka_in.Consumer` on responses, `RequestsMirror` on requests, `GroupMirror`
one-shot on responses — each on one fixed topic. With N users there are N response
topics. Three options:

| Option | Why not / why |
|---|---|
| One consumer thread per user | N threads, N TCP connections, N consumer groups. At 100 users that is 300 threads across the three consumers, on the free-threaded interpreter, in the single-replica bridge. Rejected on cost. |
| `subscribe(pattern=r"^kev1-.*-responses$")` | One thread, one group, and new topics are picked up automatically — by **metadata refresh**, which is the hazard. See below. |
| Explicit topic list, re-`subscribe()` when the registry changes | One thread, one group, deterministic. **Chosen.** |

The pattern option fails in a way that looks like data loss. `metadata.max.age.ms`
defaults to 5 minutes, so a brand-new user's response topic may not be in the
consumer's metadata for up to 5 minutes. Their first agent runs, finishes, publishes
its response — and nobody is subscribed. With `auto_offset_reset=latest` that event is
never read: the HTML page stays empty forever, the group counter never advances, and
no error appears anywhere. It is Phase 2 §6.1's split-consumer trap again — symptoms
that look like lost responses rather than like a subscription problem.

So: an explicit list, and the ordering is the fix.

```python
# eventbridge/kafka_in.py
class Consumer(threading.Thread):
    def ensure_subscribed(self, userkey: str) -> None:
        """Add this user's response topic and BLOCK until the assignment includes it.

        Called by the submit path BEFORE publish_request. The reverse order is the
        bug described above: a fast agent's response arrives before anyone is
        listening, and with auto_offset_reset=latest it is gone.
        """
```

Four details that make `ensure_subscribed` correct rather than approximately correct:

- **It blocks.** Returning optimistically reintroduces the race it exists to close.
  It polls for the assignment with a deadline (`EB_SUBSCRIBE_TIMEOUT_S`, default 15 s)
  and on timeout the submit returns `503` with the topic named. Refusing to publish is
  the safe direction: a refused request is a retry, an unwatched request is a ghost.
- **`auto_offset_reset=earliest`** on this consumer, so even if the assignment lands
  late nothing published in the gap is skipped. The response topics' 24 h retention
  (Phase 1 §6) bounds what that can replay, and `insert_response`'s
  `(correlationid, sequence)` primary key makes a replay idempotent.
- **Re-subscribe triggers a rebalance**, which pauses consumption for all users
  briefly. The group has exactly one member — the single-replica bridge — so it is
  cheap, but it is not free, and it happens once per *new* user, not per request. The
  subscription set is also persisted so a restart re-subscribes to everything at once
  rather than N times.
- **`RequestsMirror` needs the same treatment**, for the same reason: it is what
  back-fills prompts for correlations the bridge did not originate. `GroupMirror` is
  one-shot at startup and can take the full list directly.

A `TopicSet` helper is the only thing that knows the naming scheme, so §3.1's layout
appears exactly once in the codebase:

```python
# shared/tenancy.py
class TopicSet:
    def __init__(self, prefix: str, *, tenancy: str):  # "single" | "multi"
        ...
    def requests(self, userkey: str | None) -> str: ...
    def responses(self, userkey: str | None) -> str: ...
    def events(self, userkey: str | None) -> str: ...
    def dead(self, userkey: str | None) -> str: ...
```

In `single` mode every method returns the configured `REQUEST_TOPIC` /
`RESPONSE_TOPIC` and ignores `userkey` entirely. That is what makes the whole phase
additive: Phase 0/1/2 deployments get the same two topics they have now, through the
same code path the multi-tenant one uses.

### 3.3 EventRunner: one Deployment per user

EventRunner is configured with exactly one topic and one consumer group, and that
stays true — what changes is how many EventRunners there are.

| Option | Isolation it gives | Verdict |
|---|---|---|
| One Deployment, subscribed to every user's requests topic | None worth the name: one pod runs both users' agents, holds one Anthropic credential, and shares `/data`, `$HOME` and `CLAUDE_CONFIG_DIR` between them. | Rejected |
| One Deployment **per user** | Separate pods, separate volume, separate `CLAUDE_CONFIG_DIR`, separate signing seed, separate Anthropic credential, separate `ER_MAX_CONCURRENT`, separate KEDA ceiling. | **Chosen** |

The per-user Deployment is a copy of `k8s/base/eventrunner-deployment.yaml` with
`{userkey}` in the name and five environment values changed:

```yaml
# rendered by scripts/k8s_tenant.py from the registry (§2.5)
metadata:
  name: eventrunner-gh-mrsabath-4c1d9e07
  labels:
    app.kubernetes.io/name: eventrunner
    app.kubernetes.io/part-of: rossoctl-eventing
    rossoctl.dev/userkey: gh-mrsabath-4c1d9e07     # DNS-1123 label: <=63 chars, OK
spec:
  template:
    spec:
      containers:
        - name: eventrunner
          envFrom:
            - configMapRef: { name: eventing-config }        # the shared parts
          env:
            - name: ER_USERKEY
              value: gh-mrsabath-4c1d9e07
            - name: REQUEST_TOPIC
              value: kev1-gh-mrsabath-4c1d9e07-requests
            - name: RESPONSE_TOPIC
              value: kev1-gh-mrsabath-4c1d9e07-responses
            - name: ER_CONSUMER_GROUP
              value: kev1-gh-mrsabath-4c1d9e07-eventrunner    # MUST match the ScaledObject
            - name: ER_SIGNING_KID
              value: runner-gh-mrsabath-4c1d9e07
            - name: ANTHROPIC_AUTH_TOKEN
              valueFrom:
                secretKeyRef:
                  name: anthropic-gh-mrsabath-4c1d9e07        # per-user credential
                  key: auth-token
                  optional: true                              # falls back to mock mode
```

Three things this buys that are worth naming:

- **Cost attribution becomes real.** One Anthropic credential per user means the bill
  is already split, with no accounting layer. `optional: true` on the secret ref is
  deliberate: a user with no credential gets `ER_MOCK_CLAUDE` auto-selected by the
  existing `has_api_credentials()` logic and a working demo, rather than a
  `CreateContainerConfigError`.
- **A runaway agent is contained.** `ER_MAX_CONCURRENT` and the ScaledObject's
  `maxReplicaCount` are per user, so one user's 100-agent batch cannot starve another's
  single request. Under the shared-pool design it could, and nothing in Phase 1 or 2
  prevents it.
- **`ER_SIGNING_KID` becomes a per-user identity.** The approved-key set now says not
  only "this is an approved runner" but "this is *alice's* runner", which is what makes
  the ownership check on transcript writes possible at all (§6.3).

`ER_USERKEY` is the new required variable in multi-tenant mode, and the runner
**refuses to start without it** when `REQUEST_TOPIC` does not equal the bridge's
single-tenant default. A runner that stamps no `userkey` on its responses produces
events EventBridge cannot file, and a silent default here would route one user's
output into another user's store.

### 3.4 KEDA: one ScaledObject per user

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: eventrunner-gh-mrsabath-4c1d9e07
spec:
  scaleTargetRef:
    name: eventrunner-gh-mrsabath-4c1d9e07
  minReplicaCount: 0
  maxReplicaCount: 3                 # <= the per-user topic's partition count (§3.7)
  pollingInterval: 5
  cooldownPeriod: 60
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: my-cluster-kafka-bootstrap.kafka.svc:9092
        consumerGroup: kev1-gh-mrsabath-4c1d9e07-eventrunner   # EXACTLY ER_CONSUMER_GROUP
        topic: kev1-gh-mrsabath-4c1d9e07-requests
        lagThreshold: "1"
```

Phase 1 §8.2's warning now applies N times over: a `consumerGroup` that does not match
`ER_CONSUMER_GROUP` exactly is a silent no-scale failure, and the failure is per user,
so it looks like "it works for everyone except Bob". Both strings come from one
template in `k8s_tenant.py` and a preflight check compares the rendered ScaledObject
against the rendered Deployment before either is applied. Generating two strings from
one source is the only reliable fix; a comment telling the next person to keep them in
sync is not.

**The control-plane cost, stated as arithmetic.** Each ScaledObject polls Kafka every
`pollingInterval` seconds. At 100 users and 5 s that is 20 lag queries per second
against the broker's admin API, forever, including for the 99 users who are asleep.
Two mitigations, both cheap:

- Raise `pollingInterval` for idle tenants (30 s), accepting up to 30 s of cold-start
  latency against Phase 1 RQ-4's < 10 s assumption. For a trigger-driven agent nobody
  is watching, 30 s is free; for an interactive one it is not. So it is per-tier, not
  global.
- `scripts/k8s_tenant.py suspend <userkey>` sets `autoscaling.keda.sh/paused: "true"`
  on the ScaledObject for a dormant user — KEDA stops polling and holds the
  Deployment where it is. This is Phase 1 §17's teardown mechanism applied per tenant.

### 3.5 Provisioning: who creates the topics

EventBridge has no Kubernetes client and will not get one (Phase 1 §8.7). Nor will it
create topics through the Kafka admin API: `kafka-python` can, but auto-creating a
topic on first use means a typo'd `userkey` silently provisions a topic, and the
registry (§2.5) exists precisely so that approving a tenant is an operator action with
a reviewable diff.

So provisioning is `scripts/k8s_tenant.py`, consistent with Phase 1 §9's "Python only,
no shell scripts":

```bash
# Render and apply everything one tenant needs. Idempotent; prints a diff first.
python3 scripts/k8s_tenant.py apply --registry users.json --user gh-mrsabath-4c1d9e07
python3 scripts/k8s_tenant.py apply --registry users.json --all
python3 scripts/k8s_tenant.py diff  --registry users.json --all   # Phase 1 §14.1's signal
python3 scripts/k8s_tenant.py suspend gh-mrsabath-4c1d9e07
python3 scripts/k8s_tenant.py delete  gh-mrsabath-4c1d9e07 --yes-delete-data
```

Per tenant it renders: 4 `KafkaTopic` (in ns `kafka` — Phase 1 §6's operator-namespace
rule still applies and still fails silently if ignored), 1 `Deployment`, 1
`ScaledObject`, 1 `Secret` placeholder for the Anthropic credential, and in Tier B
(§3.6) 1 `KafkaUser`. Seven or eight objects per user; at 100 users, ~750 objects in two
namespaces. That is unremarkable for a cluster and worth stating so nobody assumes it
is the limit. The limit is partitions (§3.7).

`delete` requires `--yes-delete-data` and prints what it will destroy, because deleting
a `KafkaTopic` CR deletes the topic and its data. §6.5 covers what "delete my data"
can and cannot honestly promise.

### 3.6 The part that makes it isolation: Kafka ACLs, and the obstacle

Everything above gives each user their own topics. **It does not stop user B's runner
from reading user A's topics**, because the shared broker has no authorizer. Phase 2
§3.3 recorded why that was not fixed, and the reasoning still holds:

> the Kafka authorizer is global rather than per-listener, so enabling it either denies
> every existing PLAINTEXT client or, with `allow.everyone.if.no.acl.found=true`, makes
> the ACL demo vacuous.

That is a property of the *shared* broker in ns `kafka`, which carries other people's
workloads. Phase 3 therefore offers two tiers and is explicit about what each claims.

**Tier A — shared broker, no ACLs (default).** Separate topics, separate runners,
separate credentials, separate stores. The honest claim:

> EventBridge will not serve one user another user's events, and each user's agents run
> in their own pods with their own credentials. Anything with network access to the
> broker can still read any topic.

That is a real and useful boundary — it covers every path a user actually has — and it
is not confidentiality against an attacker on the cluster network. Say the second
sentence out loud when demoing it.

**Tier B — our own broker, real ACLs.** Deploy a second Strimzi `Kafka` in ns `kev1`
with SASL and `simple` authorization. Then the authorizer decision is ours and breaks
nobody else's clients, which is the whole obstacle removed:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: eventrunner-gh-mrsabath-4c1d9e07
  namespace: kev1                            # the namespace of OUR Kafka CR
  labels:
    strimzi.io/cluster: kev1-kafka
spec:
  authentication:
    type: scram-sha-512                      # password lands in a Secret of the same name
  authorization:
    type: simple
    acls:
      # Read its own requests, write its own responses. Nothing else.
      - resource: { type: topic, name: kev1-gh-mrsabath-4c1d9e07-requests,  patternType: literal }
        operations: [Read, Describe]
      - resource: { type: topic, name: kev1-gh-mrsabath-4c1d9e07-responses, patternType: literal }
        operations: [Write, Describe]
      - resource: { type: group, name: kev1-gh-mrsabath-4c1d9e07-,          patternType: prefix }
        operations: [Read]
```

Four notes, including one admission:

- **Verify this shape against the installed CRD before trusting it.** The exact
  `AclRule` field names, the `patternType` enum and the operation spellings come from
  the Strimzi API and are version-dependent; two attempts to fetch the reference for
  the deployed version during this design returned truncated pages. The check is
  `kubectl explain kafkauser.spec.authorization.acls --recursive` and
  `kubectl apply --dry-run=server`, which is how Phase 1 §6 validated its topic
  manifests. Treat the YAML above as the intended shape, not as verified text.
- **`apiVersion: kafka.strimzi.io/v1`**, matching the `KafkaTopic` manifests and the
  note in `k8s/topics/kafkatopics.yaml`: Strimzi 1.0.x no longer serves `v1beta2`, and
  an older apiVersion fails outright.
- **Literal topic ACLs, prefixed group ACLs.** A prefix ACL on topics
  (`kev1-gh-mrsabath-4c1d9e07-`) would be shorter but grants the user any *future*
  topic starting with their key, including ones a later phase gives a different
  meaning. Groups need the prefix because a consumer group name is constructed at
  runtime. Being generous on groups and exact on topics is the right asymmetry: a
  group name grants nothing by itself.
- **EventBridge needs the mirror image** — write on every `-requests`, read on every
  `-responses` and `-events`, write on every `-dead` — which is a prefix ACL on
  `{prefix}-`, i.e. it is the one principal that can reach everything. That is correct
  and is the same property Phase 2 §2.7 already records: *compromising EventBridge
  means being able to claim any user.* Tier B does not change that; it bounds what a
  compromised **runner** can do, which is the new thing.

**KEDA also needs credentials in Tier B.** The Kafka scaler authenticates with the
same SASL mechanism, via a `TriggerAuthentication` referencing a Secret with `sasl`,
`username` and `password` keys, named from the ScaledObject's
`authenticationRef`. Forgetting it is another silent no-scale failure — KEDA cannot
read the lag, reports an error on the ScaledObject's status, and the Deployment sits at
zero while requests pile up. `k8s_tenant.py diff` asserts the ScaledObject has an
`authenticationRef` whenever Tier B is configured.

### 3.7 The real ceiling: partition arithmetic

Per-user topics multiply partitions, and partitions are the scarce resource on a
single-broker demo cluster.

Today: 2 topics × 12 partitions = 24. Per user, naively copied: 4 topics × 12 = 48.
At 100 users that is 4,800 partitions on one broker, which is at or past the
rule-of-thumb ceiling for a single broker and well past what a demo cluster's
ephemeral storage wants to be doing.

The fix follows from what the partitions are *for*. Phase 1 raised requests to 12 so
one 100-agent batch could spread over 10+ pods — a property of the **whole system**.
With per-user runners, the per-user topic only needs enough partitions for *one user's*
concurrency ceiling:

| Topic | Partitions | Why |
|---|---|---|
| `-requests` | 3 | `maxReplicaCount: 3`; a partition is the unit of pod assignment, so 3 is the per-user pod ceiling |
| `-responses` | 3 | consumed by the single-replica bridge; more partitions buy nothing and cost metadata |
| `-events` | 1 | trigger evaluation is single-threaded by design (§7.6) and ordering within the inbox is worth keeping |
| `-dead` | 1 | nobody consumes it continuously |

8 partitions per user. At 100 users: 800 — comfortable. At 500: 4,000 — the number to
watch. **Note what changed in exchange:** the *aggregate* ceiling went up, not down.
Phase 1's 12 partitions capped the whole system at 12 concurrent runner pods; 100
tenants at 3 each is a ceiling of 300 pods, limited by cluster capacity rather than by
partition count. Per-user topics are a scaling improvement that happens to look like an
isolation feature.

Two one-way doors to flag, both from Phase 1 §6's notes: partitions can be increased
but never decreased, and raising the count re-maps the key→partition hash, so a
conversation whose turns straddle the change can land on different partitions and
different pods — the one case where per-correlation ordering does not hold. Per-user
topics make this *less* dangerous, because the change can be made for one idle tenant at
a time instead of for everybody at once.

### 3.8 Tiers, and what the shared tier is for

The registry's `tier` field has two values:

- **`isolated`** — own topics, own runner Deployment, own ntfy topic, own store. What
  everything above describes.
- **`shared`** — the Phase 2 arrangement: the generic `{prefix}-requests` /
  `{prefix}-responses` topics, the shared runner pool, the shared ntfy topic, and
  `ce_submitter` as an audit label only.

The shared tier is not a lesser default, it is for three specific cases: the
break-glass static operator credential (Phase 2 §2.5 — it must work when per-user
provisioning is exactly what is broken), a conference demo where provisioning N tenants
for N curious people is absurd, and the existing e2e tests, which must keep passing
unchanged. A `shared`-tier user is told so in the HTML view, because a user who
believes they are isolated and is not has been actively misled.

---

## 4. ntfy isolation

### 4.1 Why `{prefix}-{userid}-ntfy` is the wrong name

The obvious move is to name the ntfy topic the way the Kafka topics are named:
`kev1-gh-mrsabath-4c1d9e07`. On `ntfy.sh` that is a confidentiality hole, and the
reason is already written down in this repository:

> An **ntfy topic name is a capability** — anyone who knows it can read every
> notification on it. (`README.md`, Phase 1 notes)

A name containing the userid is **guessable**. Anyone who knows that a deployment
exists, that it uses the `kev1` prefix, and that `mrsabath` is a user can subscribe to
`https://ntfy.sh/kev1-gh-mrsabath-4c1d9e07` and receive every notification: the
prompt, the assistant's reply, and the stats footer — because `compose_body()`
deliberately packs all of that into the message so a phone that cannot reach
EventBridge still shows the useful result. The feature that makes the notification
good is what makes the leak complete. And the digest in the userkey does not help: it
is derived from a public identifier by a public function, so it is not a secret.

The ntfy topic name also travels further than any other name here: into the payload
sent to ntfy.sh, ntfy's message store, the phone's notification history, every other
subscriber, and — for iOS on a self-hosted server — as a SHA-256 to ntfy.sh's Firebase
topic (§4.3). It is the single worst place to put a username.

### 4.2 The derived topic

```python
# shared/tenancy.py
def ntfy_topic(prefix: str, userkey: str, secret: bytes) -> str:
    """An unguessable, stable, per-user ntfy topic. Contains no identifier.

    kev1-k7qf3mz2xa9pbw4nsdhe6tcyu5     (prefix + 26 chars of base32)
    """
    mac = hmac.new(secret, b"ntfy|" + userkey.encode(), hashlib.sha256).digest()
    tag = base64.b32encode(mac).decode().rstrip("=").lower()[:26]
    return f"{prefix}-{tag}"
```

The decisions:

- **Keyed, not hashed.** A bare `sha256(userkey)` is computable by anyone who knows the
  userkey, which is public. The HMAC key (`EB_NTFY_TOPIC_SECRET_PATH`, a Secret mount,
  env-only, never `config.toml` — Phase 2 §5's rule) is what makes the topic
  unguessable rather than merely ugly.
- **Derived, not stored.** No table, no migration, no row to get out of sync with the
  Kafka topics. Rotating the secret rotates every user's topic at once, which is the
  correct response to a leaked secret and is worth having as a one-variable operation.
  The cost is that in-flight notifications on the old topic are orphaned; for a
  notification stream that is acceptable, and it is why nothing durable is keyed by the
  ntfy topic.
- **Base32, lower-cased.** ntfy topics allow `[-_A-Za-z0-9]{1,64}`. Base32 is
  case-insensitive-safe and gives 5 bits per character, so 26 characters carry 130 bits
  — far past guessing. Base64url would be denser but `+`/`/` are illegal and the `-`/`_`
  variant is harder to read aloud, which matters because an operator occasionally has
  to.
- **The prefix stays readable.** `kev1-` makes it obvious which deployment a topic
  belongs to when an operator is looking at a phone, and leaks only the deployment name,
  which the Route hostname already leaks.

The 26-character truncation discards 126 of the 256 HMAC bits. That is deliberate (the
topic has to fit, and be typeable) and 130 bits is not the weak link in any threat
model here.

### 4.3 Option: ntfy as a service in the cluster

`ntfy.sh` is a third party holding every notification body. For an isolation phase
that is the wrong default, and self-hosting ntfy inside the same cluster removes the
whole category of problem that Phase 2 §3.1 is about: **the message bodies, and any
capability key baked into a Continue… action, never leave the cluster.**

`EB_NTFY_MODE` selects between them:

| | `sh` (Phase 2 behaviour) | `incluster` |
|---|---|---|
| Who holds the message bodies | ntfy.sh | you |
| Access control | the unguessable topic name only | topic name **and** ACLs (`auth-default-access: deny-all`) |
| Reachable from a phone off-VPN | yes | only if the Route/Ingress is |
| Android instant delivery | yes | yes — no Firebase, no upstream needed |
| iOS instant delivery | yes | needs `upstream-base-url` (see below) |
| Works on kind | yes | yes for the laptop; see the kind caveat |

#### The manifests

A new `k8s/ntfy/` directory, applied into the same namespace as everything else
(`kev1`), kept out of `k8s/base/` so a deployment using `ntfy.sh` or no notifications
at all never applies it:

```yaml
# k8s/ntfy/configmap.yaml — mounted at /etc/ntfy/server.yml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ntfy-config
data:
  server.yml: |
    # Must be the EXTERNAL URL, not the Service DNS: ntfy puts this in the links it
    # generates and the iOS app uses it to fetch message bodies. Set per cluster —
    # OpenShift (ykt1): https://ntfy-kev1.apps.ykt1.hcp.res.ibm.com
    # kind:             http://ntfy.127.0.0.1.nip.io:30080
    base-url: "https://ntfy-kev1.apps.ykt1.hcp.res.ibm.com"
    listen-http: ":8080"
    cache-file: "/var/lib/ntfy/cache.db"
    cache-duration: "12h"
    auth-file: "/var/lib/ntfy/auth.db"
    # The control that makes this isolation rather than obscurity. Without it the
    # unguessable topic name is the ONLY thing separating two users, same as ntfy.sh.
    auth-default-access: "deny-all"
    attachment-cache-dir: "/var/lib/ntfy/attachments"
    # TLS terminates at the Route/Ingress. Without this ntfy sees one source IP and
    # rate-limits every subscriber as if they were a single client.
    behind-proxy: true
    enable-login: true
    # iOS only — see the privacy note below. Omit entirely for Android-only use.
    # upstream-base-url: "https://ntfy.sh"
```

```yaml
# k8s/ntfy/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ntfy
  labels:
    app.kubernetes.io/name: ntfy
    app.kubernetes.io/part-of: rossoctl-eventing
spec:
  replicas: 1                      # SQLite-backed cache and auth db: exactly one writer
  strategy:
    type: Recreate                 # RWO volume; a rolling update deadlocks on the mount
  selector:
    matchLabels: { app.kubernetes.io/name: ntfy }
  template:
    metadata:
      labels: { app.kubernetes.io/name: ntfy }
    spec:
      securityContext:
        runAsNonRoot: true
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: ntfy
          image: docker.io/binwiederhier/ntfy:v2.11.0   # pinned, not :latest
          args: ["serve"]
          ports:
            - { name: http, containerPort: 8080 }
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: { drop: ["ALL"] }
          volumeMounts:
            - { name: config, mountPath: /etc/ntfy }
            - { name: data,   mountPath: /var/lib/ntfy }
          livenessProbe:
            httpGet: { path: /v1/health, port: http }
            initialDelaySeconds: 10
            periodSeconds: 30
          resources:
            requests: { cpu: 50m, memory: 64Mi }
            limits:   { cpu: 500m, memory: 256Mi }
      volumes:
        - { name: config, configMap: { name: ntfy-config } }
        - { name: data,   persistentVolumeClaim: { claimName: ntfy-data } }
---
apiVersion: v1
kind: Service
metadata:
  name: ntfy
spec:
  selector: { app.kubernetes.io/name: ntfy }
  ports:
    - { name: http, port: 80, targetPort: http }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ntfy-data
spec:
  accessModes: ["ReadWriteOnce"]
  resources: { requests: { storage: 2Gi } }
```

Exposure, per cluster — the same split `k8s_preflight.py` check 8 already makes between
Route and Ingress:

```yaml
# k8s/ntfy/route.yaml — OpenShift (ykt1)
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: ntfy
spec:
  to: { kind: Service, name: ntfy }
  port: { targetPort: http }
  tls:
    termination: edge
    insecureEdgeTerminationPolicy: Redirect
```

```yaml
# k8s/ntfy/ingress-kind.yaml — kind, matching overlays/kind/ingress.yaml's nip.io scheme
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ntfy
spec:
  rules:
    - host: ntfy.127.0.0.1.nip.io
      http:
        paths:
          - path: /
            pathType: Prefix
            backend: { service: { name: ntfy, port: { number: 80 } } }
```

#### Users, ACLs and tokens

Two kinds of principal, not one per direction per user — that asymmetry is the design:

```bash
NTFY="kubectl -n kev1 exec deploy/ntfy -- ntfy"

# ONE publisher: EventBridge. Write-only on every topic with our prefix.
# Write-only matters: a compromised bridge token cannot read back the history it
# published, which is a cheap bound on an otherwise all-powerful principal (§3.6).
$NTFY user add --role=user eventbridge
$NTFY access eventbridge 'kev1-*' write-only
$NTFY token add --label "eventbridge publisher" eventbridge     # -> tk_...

# ONE reader per user, scoped to exactly that user's derived topic.
$NTFY user add --role=user gh-mrsabath-4c1d9e07
$NTFY access gh-mrsabath-4c1d9e07 kev1-k7qf3mz2xa9pbw4nsdhe6tcyu5 read-only
$NTFY token add --label "phone" gh-mrsabath-4c1d9e07            # -> tk_...

$NTFY access                                                    # the reviewable output
```

Five notes:

- **Never give any principal `--role=admin`.** An admin has read/write on every topic
  and no ACL applies, which silently undoes this entire section. The publisher gets a
  write-only prefix ACL instead; that is the whole point of `auth-default-access:
  deny-all` plus explicit grants.
- **The reader's ACL is the user's derived topic, literal.** A prefix grant like
  `kev1-*` on a reader would hand them everyone's notifications, which is precisely the
  failure this section exists to prevent. `k8s_tenant.py` emits the exact literal name
  it derived, so no human types it.
- **An ntfy token grants full access to its user account** (except password change and
  deletion), so a per-user reader token is as powerful as that user's ntfy account —
  which, scoped to one read-only topic, is nothing much. That property is the reason
  not to reuse one token across users.
- **These commands edit the auth database directly**, so they only work on the server
  itself — hence `kubectl exec`. They are not idempotent in a useful way either
  (`user add` fails if the user exists), so `k8s_tenant.py` reads `ntfy access` first
  and emits only the missing grants. The declarative alternative,
  `auth-users` / `auth-access` in `server.yml`, is tempting and has a trap worth
  knowing: a user provisioned through the config file is **deleted from the database**
  when it is removed from the config, so a half-edited ConfigMap silently deprovisions
  people. We use the CLI and keep the ConfigMap for server settings only.
- **The publisher token is a Secret** (`ntfy-publisher`, key `token`), consumed as
  `NTFY_TOKEN` by EventBridge exactly as today. Per-user reader tokens are **not**
  stored in the cluster at all — they are handed to the user once, at provisioning
  time, and `k8s_tenant.py` prints them to the operator's terminal and nowhere else.
  Storing them would create a place where every user's notification access is
  collected, for no benefit: EventBridge never reads.

#### What EventBridge has to change

Almost nothing, which is the appeal. `NtfyPublisher` already takes a `base_url`, a
`topic` and a `token`. The changes are:

```python
# eventbridge/ntfy.py
def _topic_for(self, event: dict) -> str:
    """Per-user topic in multi-tenant mode, the configured one otherwise."""
    if self._tenancy != "multi":
        return self._cfg.topic
    uk = event.get("userkey")
    if not uk:
        # An event with no userkey in multi-tenant mode cannot be announced: there is
        # no topic it belongs on, and the shared one would leak it to everybody.
        # Count it and drop it — loudly, because it means something upstream is wrong.
        self._unattributed += 1
        return ""
```

and `base_url` becomes `http://ntfy.kev1.svc` in `incluster` mode — the **Service** DNS
for publishing, while the **Route** hostname is what goes in `base-url` in
`server.yml` and in the Click/Action links. Getting those two backwards is the likely
mistake: publishing through the Route works but leaves the cluster and comes back, and
putting the Service DNS in `base-url` produces notification links a phone cannot open.

Phase 2 §2.7's trust statement extends here unchanged: EventBridge decides which topic
an event is announced on, so compromising it means being able to announce to anybody.

#### The caveats worth knowing before you demo it

- **A phone must reach the Route.** On OpenShift (`ykt1`) that is a real hostname with
  a real certificate and it works. On **kind** it is `127.0.0.1.nip.io`, which resolves
  to the phone's own loopback — so notifications are visible in the ntfy **web UI** on
  the laptop, and not on a phone at all, unless a tunnel is involved. That is a real
  limitation of the kind target, not a configuration mistake, and it is the same
  constraint Phase 1 records as the "remote access caveat".
- **A self-signed certificate breaks the Android app silently.** It fails to connect
  and shows no useful error. Use the cluster's real ingress certificate.
- **iOS needs `upstream-base-url: https://ntfy.sh`** or messages arrive in minutes to
  hours rather than instantly — Apple's background-execution limits make instant push
  impossible without an APNs-connected server. What that forwards is narrow and worth
  stating exactly: the message **ID** and the **SHA-256 of the topic URL**, with the
  literal body text `New message`. Titles, bodies and tags never leave the cluster; the
  app fetches the real content from your server. But note the hash is deterministic, so
  anyone who guesses the topic URL can compute the Firebase topic — which is the second
  reason §4.2's topic name is keyed rather than derived from the public userkey.
  **Android needs none of this** and gets instant delivery from a self-hosted server
  with no upstream and no Firebase.
- **`replicas: 1` and `Recreate` are not negotiable** with the SQLite auth/cache files
  on an RWO volume. A second replica corrupts them; a rolling update deadlocks waiting
  for a mount the old pod still holds.
- **Losing the PVC loses the ACLs**, not just the cache — the auth database lives there.
  `k8s_tenant.py apply --all` re-creates every user, grant and token from the registry,
  which is why the registry is the source of truth and the auth db is a cache of it.
  The per-user tokens change, so every user has to be handed a new one; that is the
  recovery cost and it is worth knowing in advance.

### 4.4 Closing Phase 2 §3.1: the capability key

Phase 2 left `/continue` unauthenticated and explained why: an ntfy `http` action is a
recipe that comes to rest in four places outside our control, so a bearer token must
not go in it. It also named the planned fix. Per-user isolation makes that fix
mandatory rather than optional — "continuing requires knowing an unguessable
correlationid" is an acceptable argument when there is one user and an unacceptable one
when user B continuing user A's conversation means spending A's credential inside A's
runner.

So, as planned there:

```python
# eventbridge/capability.py — stdlib hmac, nothing stored
def mint(secret: bytes, corr: str, userkey: str, ttl_s: int = 86400) -> str:
    exp = int(time.time()) + ttl_s
    mac = hmac.new(secret, f"{userkey}|{corr}|{exp}".encode(), hashlib.sha256).digest()
    return f"{exp}.{base64.urlsafe_b64encode(mac[:16]).decode().rstrip('=')}"
```

`?k=<key>` is baked into the ntfy action URL. `/continue` accepts **either** a bearer
token **or** a valid key, and a valid key authorises exactly one correlation, for one
user, until it expires. The `userkey` is inside the MAC input, so a key minted for
alice's correlation cannot be replayed against bob's even if the correlation ids were
somehow confused.

The honest bound, unchanged from Phase 2's framing: a leaked notification then grants
one conversation with an expiry, instead of the API. Combined with `incluster` ntfy
(§4.3) the leak surface shrinks again, because the action URL containing the key never
reaches a third party at all. Those two controls compose; neither replaces the other.

---

## 5. The agent definition: baked, fetched, or both

Up to Phase 2, "the agent" is five lines in `build_cmd()`: `claude -p <prompt>
--output-format stream-json --verbose --max-turns N --permission-mode acceptEdits`,
plus `--model` when the request names one. There is no notion of *which* agent, no
system prompt, no tool policy, and no skills. A trigger (§7) has to answer "run **what**",
so that notion has to exist first.

### 5.1 `AgentSpec`

One TOML file per agent, matching the `config.toml` precedent rather than introducing
a second config language:

```toml
# /etc/rossoctl/agents/triager/agent.toml
name        = "triager"
description = "Triage an incoming issue and propose a label and a next action."
model       = "sonnet"
max_turns   = 8

# The tool policy IS the sandbox. Default-deny, because the prompt an agent receives
# from a trigger is attacker-influenced text (§7.4) and the tools are what turn text
# into consequences.
permission_mode  = "plan"                      # plan | acceptEdits | bypassPermissions
allowed_tools    = ["Read", "Grep", "Glob", "Skill"]
disallowed_tools = ["Bash", "Write", "Edit", "WebFetch", "WebSearch"]

append_system_prompt_file = "system.md"

[[skills]]
name   = "eventbridge"
source = "image:///etc/rossoctl/agents/triager/skills/eventbridge"

[[skills]]
name    = "issue-triage"
source  = "github://rossoctl/skills@9f1c0ab3d2e4f5061728394a5b6c7d8e9f012345//triage"
sha256  = "4b2d…"                              # over the fetched archive, REQUIRED
sig_kid = "skills-01"                          # optional; required when the source is not image://

[limits]
timeout_s        = 900
max_output_bytes = 4000000
max_events       = 2000                        # response events per run; see §5.6

[mcp]
config_file = ""                               # a .mcp.json to pass to --mcp-config
```

Resolution order, most specific first: the trigger's `agent` (§7.2) → the registry
user's `agent` (§2.5) → `ER_AGENT_NAME` → `default`. A request naming an agent the
runner does not have is an **error event**, not a silent fallback to `default`: running
a different agent than the one asked for is worse than not running.

An unspecified field inherits the current behaviour, so the `default` agent is
`permission_mode = "acceptEdits"` with no tool restrictions — byte-identical to Phase
2's `build_cmd`. That is what keeps the e2e tests passing and makes this additive.

### 5.2 Baked into the image — the default, and the one to prefer

```text
/etc/rossoctl/agents/
  default/agent.toml
  triager/
    agent.toml
    system.md
    skills/
      issue-triage/SKILL.md
```

A derived image, exactly like `Dockerfile-eventrunner-claude` derives from the base
runner:

```dockerfile
ARG BASE_IMAGE=quay.io/aslomnet/rossoctl-eventrunner-claude:dev
FROM ${BASE_IMAGE}
USER 0
COPY --chown=0:0 agents/ /etc/rossoctl/agents/
# Phase 1 §8.5's arbitrary-UID rule: readable by gid 0, never writable. An agent
# definition the agent itself could rewrite is not a policy, it is a suggestion.
RUN chgrp -R 0 /etc/rossoctl && chmod -R g=rX,o= /etc/rossoctl
USER 10001
```

Four properties no fetched skill can match, which is why this is the recommended path
and not merely the simple one:

- **Reviewable.** It arrives through a pull request and a build, not at runtime.
- **Immutable and attestable.** The image digest covers it. A later SPIRE-based
  attestation (Phase 2 §4.3's upgrade path) can vouch for the whole thing at once.
- **It works offline and costs no cold-start latency.** A runner scaled from zero by a
  100-agent batch does not make 100 outbound fetches, and a GitHub outage does not stop
  agents from running.
- **Read-only.** §5.4's entire threat model is about a bundle that can write.

### 5.3 Fetched skills: GitHub, S3, URL

Three source schemes, each resolving to exactly one HTTPS GET of one archive:

| Scheme | Form | Resolves to |
|---|---|---|
| `image://` | `image:///etc/rossoctl/agents/<a>/skills/<s>` | nothing — already present |
| `github://` | `github://<owner>/<repo>@<40-hex-sha>//<subpath>` | `https://codeload.github.com/<owner>/<repo>/tar.gz/<sha>` |
| `https://` | `https://host/path/bundle.tar.gz` | itself |
| `s3://` | **a presigned HTTPS URL only** | itself |

The two decisions in that table:

**`github://` pins a 40-hex commit sha, never a branch or tag.** A branch moves by
design; a tag can be moved by anyone with push access. Pinning to a tag and verifying a
digest would be contradictory — if the digest is checked, the tag adds nothing but the
illusion of readability. `k8s_tenant.py`-adjacent tooling can resolve a tag to a sha at
*authoring* time and write the sha into the TOML, which is where that convenience
belongs.

**S3 is a presigned URL, deliberately.** Implementing SigV4 over `hmac`/`hashlib` is
possible and has been done in fewer than 100 lines, and it is still the wrong trade:
it puts long-lived AWS credentials into every runner pod, adds a credential rotation
problem, and adds a hand-rolled signer to a security path. A presigned URL is an
expiring capability that the operator mints out-of-band, needs no SDK (Phase 1 §1.1
satisfied for free), and leaves the runner holding nothing durable. The cost — presigned
URLs expire, so a long-lived `agent.toml` referencing one will eventually 403 — is stated
in the error message, and is an argument for baking (§5.2) rather than for SigV4.

### 5.4 A skill bundle is a supply-chain artifact

This is the section that matters. A skill bundle is not data:

- `SKILL.md` is **instructions to a model that holds tools.** A malicious one does not
  need an exploit; it asks.
- `.claude/settings.json` can define **hooks**, which are shell commands the CLI runs.
  That is arbitrary code execution inside the runner pod, with the pod's credential.
- `.mcp.json` names **MCP servers**, which are processes or network endpoints the agent
  will talk to and trust.

So a fetched bundle passes five gates, in this order, and fails closed at each:

**1. Source allowlist.** `ER_SKILL_SOURCES` is a comma-separated list of allowed
prefixes, **empty by default, and empty means no fetching at all.** Without this the
runner is an SSRF proxy with cluster-internal network access: a source URL of
`http://169.254.169.254/latest/meta-data/` or
`http://my-cluster-kafka-bootstrap.kafka.svc:9092` is a request the runner would
happily make. The resolver additionally refuses, after DNS resolution and before
connecting, any address that is loopback, link-local, or in a private range — checked on
the **resolved IP**, because an allowlisted hostname can resolve anywhere and a redirect
can go anywhere. Redirects are not followed at all.

**2. Digest pinning.** `sha256` is **required** for every non-`image://` source. No
digest, no fetch — not a warning. This is what makes "pin a commit sha" meaningful:
`codeload.github.com` serving a tarball for a given sha is reproducible in content but
not byte-identical across time (compression metadata), so in practice the digest is
taken over the *canonicalised extracted tree* — a sorted list of `path\0mode\0sha256`
lines, hashed — rather than over the archive bytes. That is a real complication and the
alternative (digest over the archive) is brittle in a way that would be discovered as a
mysterious verification failure months later.

**3. Signature, for anything that can execute.** A bundle containing `settings.json`,
any `hooks` key, `.mcp.json`, or any file with an executable bit **must** carry
`skill.sig` — a detached JWS over the same canonical tree digest — verified against
`ER_VERIFY_KEYSET_PATH` with the `kid` from the spec. This reuses `shared/signing.py`
and `shared/keyset.py` wholesale, which is the right kind of reuse: the keyset is
already "the authorization list" (its own docstring), and approving a skill publisher is
the same operation as approving an agent. Unsigned bundles are still usable — they are
just stripped (gate 4).

**4. Strip what was not signed.** An unsigned bundle keeps `SKILL.md` and plain data
files. `settings.json`, `settings.local.json`, `.mcp.json`, `hooks/`, and every
executable bit are removed, not rejected — so a well-meaning bundle with a stray
`settings.json` works in reduced form and says so in the run's event stream. The
distinction is deliberate: refusing would push people toward signing everything
reflexively, which devalues the signature.

**5. Safe extraction.** `tarfile.extractall(..., filter="data")`, which is stdlib and
rejects absolute paths, `..` traversal, links pointing outside the destination, device
and special files, and strips setuid/setgid bits. Hand-rolling these checks is a
classic source of zip-slip bugs and there is no reason to. On top of it: **no symlinks
at all** (the `data` filter permits ones that stay inside; we do not need them and they
complicate the canonical digest), and hard caps — 8 MiB total uncompressed, 1 MiB per
file, 256 files, 8 directory levels — checked while streaming so a decompression bomb
is refused rather than extracted.

Then placement:

```text
{tmpdir}/eventrunner/work/{corr}/.claude/skills/{name}/SKILL.md
```

**Per-correlation, not in `CLAUDE_CONFIG_DIR`.** This is the subtle one. The config dir
is per-pod and persists across runs, so a skill fetched for one request would still be
discoverable by the next request that pod serves — a different conversation, possibly
driven by a different trigger with a different tool policy. Scoping to the
per-correlation working directory (which is already `cwd` for the subprocess) means a
skill's lifetime is exactly one run. Cleanup is the existing workdir cleanup; nothing
new to forget.

The fetch cache (`ER_SKILL_CACHE_DIR`, keyed by the canonical digest) sits *before*
placement, so a pod fetches a given bundle once and copies it per run. With
scale-to-zero that is once per cold start, which means a 100-agent batch across 3 pods
pays 3 fetches, not 100 — and 0 if the skill is baked. That arithmetic is the strongest
practical argument for §5.2.

### 5.5 How the headless agent is actually invoked

`build_cmd` grows from five flags to a function of the `AgentSpec`:

```python
cmd = [cfg.claude_bin, "-p", prompt,
       "--output-format", "stream-json", "--verbose",
       "--max-turns", str(spec.max_turns),
       "--permission-mode", spec.permission_mode]
if spec.model:            cmd += ["--model", spec.model]
if spec.allowed_tools:    cmd += ["--allowedTools", ",".join(spec.allowed_tools)]
if spec.disallowed_tools: cmd += ["--disallowedTools", ",".join(spec.disallowed_tools)]
if spec.system_prompt:    cmd += ["--append-system-prompt", spec.system_prompt]
if spec.settings_path:    cmd += ["--settings", spec.settings_path]
if spec.mcp_config_path:  cmd += ["--mcp-config", spec.mcp_config_path]
cmd += ["--session-id", session] if mode == "start" else ["--resume", resume_target or session]
```

Three notes:

- **The prompt goes through `argv`, never a shell.** `subprocess.Popen` with a list and
  no `shell=True` is already how this works, and it is what makes §7.4's
  attacker-controlled prompt text a *prompt-injection* problem rather than also a
  command-injection one. Worth stating because the obvious "improvement" — building a
  command string — would silently reintroduce it.
- **Flag support must be asserted against the pinned CLI.** `Dockerfile-eventrunner-claude`
  pins `CLAUDE_VERSION=2.1.278` for exactly this reason. A test parses `claude --help`
  in the built image and asserts every flag the spec can emit exists; a flag that
  vanishes in a CLI upgrade should fail a build, not a production run.
- **`child_env` stays an allowlist.** Phase 0's `_OS_BASICS` / `_CLAUDE_ROUTING` design
  is right and gets more right in a multi-tenant world: the agent sees no Kafka
  bootstrap, no signing seed, no ntfy token, and no EventBridge credential. §5.6 is why
  it does not need them.

### 5.6 Sending events back: the agent never holds the keys

The agent's `stdout` already becomes response CloudEvents via `_pump`. What is new is
an agent that wants to emit a **domain** event — "I filed issue 42", "the build is
green" — so a trigger (§7) or another system can react.

The wrong way is to give the agent a Kafka client or the signing seed. Then the agent
can forge any `userkey`, any event type, and a `group.completed` that ends somebody
else's batch — and the seed is in a process running attacker-influenced instructions.

Instead, the runner opens a **UNIX socket** in the per-correlation workdir and puts a
tiny client on `PATH` in the image:

```bash
# inside the agent, via the Bash tool (when the spec allows it)
rossoctl-emit --type dev.rossoctl.agent.custom.issue-filed \
              --subject issue-42 \
              --data '{"number": 42, "url": "https://github.com/..."}'
```

```text
agent ──unix socket──▶ runner ──stamps correlationid, userkey, sequence, causationid
                                 ──signs with ER_SIGNING_KEY_PATH
                                 ──publishes to {prefix}-{userkey}-responses
```

The properties that follow, each the reason for the design:

- The agent **cannot choose** `correlationid`, `userkey`, `sequence` or the topic. The
  runner knows them; the socket carries only `type`, `subject` and `data`.
- The agent **never sees the seed**, so it cannot sign anything the runner did not
  stamp. Phase 2 §4's "which agent produced this" claim survives contact with an agent
  running fetched instructions.
- The type must match `dev.rossoctl.agent.custom.*`. Without that prefix check an agent
  could emit `dev.rossoctl.agent.group.completed.v1` or a `request`, and self-trigger —
  §7.7's loop, reached from inside the sandbox instead of through the API.
- `limits.max_events` caps emissions per run. A loop in the agent's own logic is a
  cheaper way to flood the topic than a loop in the trigger graph, and it needs its own
  ceiling.
- A socket, not an HTTP endpoint on `localhost`: it is filesystem-permissioned to the
  run's own directory, it cannot be reached from another pod, and it requires no port
  allocation. The agent's network access is governed by the tool policy (§5.1) and a
  `NetworkPolicy`; the emit path deliberately does not depend on either.

---

## 6. Session storage, with security

Phase 1 §16 Gap B put the `claude` transcript in EventBridge so `/continue` survives a
scale-to-zero. Phase 2 §3.2 recorded, plainly, what that left open:

> `PUT /transcript` is unauthenticated. EventRunner uses it for session checkpointing
> and has no credential concept anywhere in `eventrunner/config.py`.

A transcript is the most sensitive object in the system — the full conversation,
including whatever the agent read. Today anyone who can reach EventBridge can read any
transcript by correlation id, and can overwrite one. With per-user tenancy that is no
longer a deferred nicety: an overwritable transcript is a way to inject arbitrary resume
state into another user's next turn, which means putting words in their agent's mouth.

### 6.1 Per-user stores, as separate files

```text
{tmpdir}/eventbridge/
  users/
    gh-mrsabath-4c1d9e07/
      responses.sqlite        # responses, keyed (correlationid, sequence)
      sessions.sqlite         # sessions, prompts, groups, group_members, transcripts
    gh-aslom-7b2e55a1/
      ...
  shared/                     # tier `shared` (§3.8), and all of single-tenant mode
```

The alternative is one database with a `userkey` column and a `WHERE` clause on every
query. It is cheaper — one connection pair, one startup, no file-descriptor arithmetic —
and it is rejected:

**One missed `WHERE userkey = ?` is a cross-tenant leak.** `store.py` has 28 methods, and
every one of them reads or writes tenant data. A predicate that must be remembered 28
times, and in every method added later, is a predicate that will eventually be forgotten,
and the symptom is a user seeing someone else's conversation. Separate files make the
mistake structurally unavailable: the connection *is* the tenant, and a query cannot
reach rows that are not in the file it is executing against. For a phase whose entire
subject is isolation, buying that property with file descriptors is the right trade.

Two secondary wins that are not the reason but matter: "delete my data" becomes
`rm -rf` of one directory (§6.5), and a per-user size cap is a `du` on one directory
rather than a `GROUP BY`.

**The cost, as arithmetic.** Two SQLite connections per tenant, each in WAL mode (so
the main DB plus `-wal` and `-shm`). The default soft `RLIMIT_NOFILE` of 1024 therefore
bounds this somewhere around 150–200 concurrently-open tenants before anything else the
process holds — not 500, because WAL mode multiplies the count. So `StoreRegistry`
opens lazily and closes on an LRU:

```python
# eventbridge/store_registry.py
class StoreRegistry:
    """userkey -> Store, opened on demand, closed on an LRU (EB_MAX_OPEN_STORES=64).

    Closing is safe because a Store is stateless above SQLite — every method opens a
    transaction and commits. The ONE thing that is not: `Store.subscribe()` holds
    threading.Events for live SSE viewers, so a store with subscribers is pinned and
    exempt from eviction. Evicting it would make an open transcript page stop updating
    with no error anywhere, which is the Phase 2 §6.1 class of bug — symptoms that look
    like lost events.
    """
```

The response consumer writes to whichever store `ce_userkey` names. An event with no
`userkey` in multi-tenant mode goes to `shared/` and increments an `unattributed`
counter surfaced on `/healthz` — never silently into a user's store, and never dropped.

### 6.2 Reads become owner-scoped, and the capability URL retires

This is a **behaviour change**, not an addition, and the only one in Phase 3. Today
these are open:

| Route | Today | Multi-tenant mode |
|---|---|---|
| `GET /v0/agents/{corr}` (HTML) | open | owner, or a valid `?k=` key (§4.4) |
| `GET /v0/agents/{corr}/turns`, `/events`, `/events.jsonl`, `/events.sse` | open | owner only |
| `GET /v0/groups`, `/v0/groups/{id}` | open | owner only; the list shows only the caller's groups |
| `GET /v0/agents/{corr}/transcript` | open | the owner, or that user's runner (§6.3) |
| `PUT /v0/agents/{corr}/transcript` | open | that user's runner only (§6.3) |
| `POST /v0/agents/{corr}/continue` | open | owner, or a valid `?k=` key |

Phase 2 §3.1 defended the open reads as a **capability URL**: the correlation id is
unguessable, so knowing it is the authorization. With one user that argument holds. With
many it does not, for a reason worth being precise about: the id space is roughly
`|adjectives| × |animals| × 10,000`, which is large but is *not* a 128-bit secret, and
correlation ids are not treated as secrets anywhere — they appear in group pages, in
`k8s_demo_flow.py` output, in logs, in ntfy notification bodies, and in this
documentation. A value that is printed everywhere cannot also be the only access
control.

So in multi-tenant mode, ownership is checked: `store_registry.owner_of(corr)` consults
a small global index (`correlationid → userkey`, **unique on `correlationid`**, the one
table that is deliberately cross-tenant, because the lookup must happen before a tenant
is chosen) and compares to the caller. That uniqueness is not only for this lookup —
§2.6 relies on it as the global collision check that lets the `sessionuuid` derivation
stay unsalted. A mismatch is **`404`, not `403`** — the opposite of Phase 2 §2.4's
reasoning, and for a different question. There, `403` was right because naming the
refused *identity* makes the error actionable for a user who already knows who they
are. Here, distinguishing "this correlation exists but is not yours" from "this
correlation does not exist" would let anyone enumerate which ids are live across all
tenants. The user learns nothing actionable from the difference, and an attacker learns
something. Same document, opposite answer, because the information disclosed is
different.

Single-tenant mode keeps every route open, so the Phase 0/1/2 demo and the existing
tests are untouched.

### 6.3 Authenticating the transcript PUT

Two options; both are implemented, because they serve different deployments.

**Option A — a bearer token (`ER_EVENTBRIDGE_TOKEN`).** Simple, obvious, and already
has the machinery: `auth.resolve_identity` does a constant-time compare against
`EB_AUTH_TOKENS`. It does not answer *which user's runner* this is without a token per
user, which is a secret per tenant to provision and rotate.

**Option B — a signed PUT (recommended).** The runner signs a header over the body
digest, using the seed it already has for signing responses:

```text
PUT /v0/agents/sweet-crab-0956/transcript
X-Rossoctl-Kid: runner-gh-mrsabath-4c1d9e07
X-Rossoctl-Ts: 1759392000
X-Rossoctl-Signature: <base64url Ed25519 over "v1|{corr}|{ts}|{sha256(body)}">
```

Verified against `EB_VERIFY_KEYSET_PATH` — the same approved-key set that already
authorizes responses. Three things fall out:

- **No new secret.** Phase 2 already provisions a seed per runner, and Phase 3 §3.3
  already gives each user's runner its own `kid`. The credential for this exists
  before the feature does.
- **The `kid` names the tenant.** `EB_RUNNER_KIDS` maps `userkey → kid` (or, more
  cheaply, the convention `runner-{userkey}`), so the bridge can assert that the runner
  writing alice's transcript is *alice's* runner. A valid signature from bob's approved
  runner over alice's correlation is refused. This is the check that makes the whole
  section worth doing — a shared bearer token cannot make it.
- **The timestamp bounds replay**, with a ±300 s window and the signature covering the
  body digest, so a captured PUT cannot be replayed later to roll a session back to an
  earlier state. Rolling a transcript backwards is a subtle and effective attack on a
  conversation and it costs one header to prevent.

The cost: ~150–200 ms of pure-Python Ed25519 per PUT. That is once per *turn*, not per
frame — the same distinction Phase 2 §4.4 step 2 drew when it signed terminal responses
only, for the same reason, and the same arithmetic says it is fine here.

`ER_TRANSCRIPT_AUTH = none | token | signature` selects, defaulting to `none` in
single-tenant mode and **required** (`token` or `signature`) in multi-tenant mode: the
bridge refuses to start in `multi` with `none`, because an unauthenticated transcript
write is the injection path described at the top of this section.

### 6.4 Encryption at rest: what we do, and what we will not hand-roll

The honest answer is the short one: **we do not encrypt transcripts in the
application.**

The reasoning is Phase 1 §1.1. There is no `cryptography` dependency, so encryption would
mean a hand-rolled AEAD. Phase 2 already hand-rolled Ed25519 and measured the result at
~150–200 ms per signature; a pure-Python ChaCha20-Poly1305 or AES-GCM would be in the
low single-digit MB/s range, so a 32 MiB transcript at the configured cap would cost
seconds of CPU per turn on the single-replica bridge — and more importantly it would put
a hand-written cipher in the path of the most sensitive data in the system, where a
side-channel or a nonce-reuse bug is both likely and silent. A signature that is wrong
fails loudly; encryption that is wrong does not. (Those throughput figures are an
estimate from the measured Ed25519 cost, not a measurement — establish them before
quoting them.)

What is done instead, in order of how much it actually buys:

1. **Per-user files with `0700` directories and `0600` files.** Inside the container the
   process is one UID, so this is not a boundary against the bridge itself; it is a
   boundary against anything else that gets a filesystem view — a debug sidecar, a
   backup job, an `oc rsync`.
2. **Volume-level encryption**, which is the right layer. On OpenShift that is an
   encrypted StorageClass; on a laptop it is FileVault or LUKS. This is a one-line
   deployment change that encrypts everything including the SQLite WAL and any temp
   files, which an application-level scheme would miss.
3. **Short retention** (§6.5). Data that is gone is the only data that is safe.
4. **A smaller cap in multi-tenant mode.** `limits.transcript_bytes` per user (§2.5),
   defaulting to 256 MiB total per tenant rather than per transcript, so one user cannot
   fill the shared volume and deny service to the rest. Volume exhaustion on the
   single-replica bridge takes down everybody, which makes a per-tenant quota an
   availability control, not just a tidiness one.

If a deployment genuinely needs application-level encryption, the honest path is the
`cryptography` dependency conversation (Phase 2 §4.3 reaches the same conclusion about
fast signatures), not a hand-rolled cipher. Recording that here is the point: the next
person to want this should know it was considered and why it was refused.

### 6.5 Retention and deletion

```text
DELETE /v0/users/me/data        # owner only; 202 + a summary of what was removed
```

`EB_TRANSCRIPT_RETENTION_DAYS` (default 7) drives a sweeper on the existing group
deadline thread. Deletion removes the tenant's store directory, its transcripts, its
triggers (§7) and its ntfy ACL.

**What it cannot do, stated plainly**, because a deletion endpoint that over-promises is
worse than none:

- **Kafka history is not deleted.** The events are in the user's topics with 24 h
  retention (Phase 1 §6). The response really is "they expire within a day", or an
  operator deletes the `KafkaTopic` CR, which is `k8s_tenant.py delete --yes-delete-data`.
- **Notifications already delivered are gone from our control**, and on `ntfy.sh` they
  are in a third party's store and on the user's own phone. `incluster` mode (§4.3)
  bounds this to the cache file, whose `cache-duration` is 12 h in the manifest above.
- **Logs.** `kubectl logs` holds correlation ids and prompts in whatever the cluster's
  log retention is. Nothing in this repository can reach them.

---

## 7. Event triggers: "what event runs what agent"

### 7.1 What Knative Eventing gets right, and what we borrow

Knative's Broker/Trigger model is the obvious prior art and most of it is worth copying
outright:

| Knative property | Borrowed? | Why |
|---|---|---|
| A Trigger is a **declarative named object**, re-appliable | **Yes** | Re-applying must not create a second trigger. This drives `PUT /v0/triggers/{name}` rather than `POST` (§7.6). |
| Filter on **CloudEvent attributes**, not on `data` | **Yes** | Attributes are indexed, bounded and typed; `data` is arbitrary. Also matches the spec, so a trigger is portable reasoning. |
| **Multiple filters are ANDed** | **Yes** | One obvious semantic, no precedence to learn. |
| Filter **dialects**: `exact`, `prefix`, `suffix`, `all`, `any`, `not`, `cesql` | **All but `cesql`** | §7.3. |
| Every **matching** Trigger gets the event (fan-out) | **Yes, with a cap** | §7.7 — fan-out over an LLM is a cost multiplier. |
| `spec.delivery`: `retry`, `backoffPolicy`, `backoffDelay`, `deadLetterSink` | **Yes, renamed** | §7.5. |
| An event matching **no** Trigger is silently dropped | **No** | §7.4 — silence is the worst debugging experience and we can afford a counter. |
| The subscriber is an **addressable URI** | **No** | §7.2 — our subscriber is an agent, not an endpoint. |

Two differences are worth stating as differences rather than as improvements, because
they change what a Knative user should expect:

- **`spec.subscriber` becomes `agent`.** Knative delivers an event to an HTTP endpoint.
  We *start an agent with a rendered prompt*, which is a conversion, not a delivery. An
  agent is not addressable, has no HTTP contract, and takes minutes. Keeping the Knative
  field name for a different thing would be worse than renaming it.
- **There is no Broker object.** EventBridge *is* the broker: it has the ingress
  (`POST /v0/events`), the channel (the user's `-events` topic) and the dispatcher. A
  Broker CR exists in Knative because the implementation is pluggable; here it is not,
  and inventing a configuration object with one possible value is cost without benefit.

### 7.2 The Trigger

```json
PUT /v0/triggers/issue-triage
{
  "filters": [
    { "exact":  { "type": "com.github.issues.opened" } },
    { "prefix": { "source": "https://github.com/rossoctl/" } },
    { "not":    { "exact": { "subject": "issue-spam" } } }
  ],
  "agent": "triager",
  "prompt": "Triage this issue.\n\n<issue>\n{{data.issue.title}}\n\n{{data.issue.body|truncate:4000}}\n</issue>",
  "max_turns": 8,
  "model": "sonnet",
  "delivery": { "retry": 3, "backoffPolicy": "exponential", "backoffDelay": "PT2S" },
  "limits": { "max_per_hour": 20, "max_concurrent": 2 },
  "enabled": true
}
```

The response adds server-owned state, in the spirit of a Knative `status`:

```json
{
  "name": "issue-triage",
  "userkey": "gh-mrsabath-4c1d9e07",
  "ready": true,
  "created_utc": "2026-10-02T14:03:11.221Z",
  "updated_utc": "2026-10-02T14:03:11.221Z",
  "match_count": 37,
  "last_matched_utc": "2026-10-02T15:58:02.004Z",
  "last_error": null,
  "dropped_rate_limited": 2,
  "dead_lettered": 0
}
```

Two new event types, so a trigger firing is itself visible on the event path rather than
only in a log:

```text
dev.rossoctl.trigger.matched.v1     # a trigger matched; carries triggerid + the corr it started
dev.rossoctl.trigger.dropped.v1     # matched but refused: rate limit, depth, fan-out cap, bad template
```

Both ride the **responses** topic, for the reason `ce.py` already gives for group
events: EventRunner consumes `requests` and would try to execute anything it found
there, while the bridge's own consumer, store and ntfy publisher are already wired to
responses. They are signed under the bridge's `kid`, and the verifier accepts only that
kid for them — identical to the group-event rule in Phase 2 §4.4 step 5, and for the
identical reason: a forged `trigger.dropped` is a way to make an operator believe work
was refused when it ran, or vice versa.

### 7.3 Filter dialects, and why `cesql` is omitted

`exact`, `prefix`, `suffix`, `all`, `any`, `not` — implemented as a ~60-line recursive
pure function over the attribute dict, matching Knative's semantics including the detail
that non-string attributes are compared against their string rendering (which, in our
codec, they already are: `ce.to_kafka_binary` encodes every attribute as UTF-8 bytes
and `from_kafka_binary` decodes every one to `str`).

`cesql` is deliberately **not** implemented. It is a full expression language with a
grammar, operator precedence, type coercion and functions; a hand-rolled parser for it
would be several hundred lines of new attack surface sitting in the routing decision of
a system that spends money, and Phase 1 §1.1 forbids pulling in an implementation. The
escape hatch is that `all`/`any`/`not` compose, which covers everything except `LIKE`
and arithmetic. If a deployment truly needs `cesql`, that is the dependency
conversation, not a weekend parser.

The legacy `filter.attributes` map is accepted as sugar for a single `exact` — Knative
keeps it for backwards compatibility and we have no compatibility to keep, but accepting
it costs four lines and means a Knative Trigger can be pasted in and mostly work, which
is worth four lines.

**Filtering on `data` is not supported**, matching Knative. The split is clean and worth
naming: **routing reads attributes, prompts read `data`.** Attributes are small, bounded
and cheap to match on the hot path; `data` is arbitrary and is the attacker-controlled
part (§7.4). Letting a filter walk into `data` would make matching cost unbounded and
would invite exactly the mistake of treating payload content as a security decision.

### 7.4 Rendering the prompt, and the risk that cannot be designed away

The template language is **deliberately tiny**: `{{dotted.path}}` lookups into the
event's JSON, with two filters, `|json` and `|truncate:N`. Not Jinja — a dependency
Phase 1 §1.1 forbids, and a sandbox-escape surface in a path that builds instructions
for a tool-holding model. Missing paths render as the empty string and are recorded in
the trigger's `last_error`, because a trigger silently sending a prompt with a hole in
it is worse than one that reports it.

**Now the part that matters.** Event `data` is attacker-controlled text — a GitHub issue
body is written by whoever opened the issue — and it becomes the agent's instructions.
This is prompt injection, it has no complete technical fix, and a trigger API is the
feature that makes it reachable without a human reading the input first. What can be
done, and is:

- **Default-deny tools** (§5.1). `permission_mode = "plan"` with `Bash`, `Write`,
  `Edit` and the network tools disallowed means a successful injection yields *text*,
  which a human then reads. This is the control that actually bounds the damage; the
  others reduce the odds.
- **Delimit and label the data.** The rendered prompt wraps every interpolation in a
  named block, and the agent's `append_system_prompt` says that content inside those
  blocks is data to be analysed and never instructions to follow. This helps and does
  not solve; say so when demoing it.
- **Cap the interpolated size** (`|truncate:N`, and a hard total cap). A long injection
  has more room to work, and an unbounded one is also a cost attack.
- **A per-trigger budget** (`limits.max_per_hour`), so a successful injection that
  provokes more events cannot run away before anyone notices.
- **`POST /v0/triggers:test`** (§7.6) renders the prompt without running anything, so
  the exact text an agent would receive is reviewable before the trigger is enabled.

The honest statement for the demo: *a trigger turns an untrusted document into a prompt.
The tool policy is what keeps that from turning into an action.*

### 7.5 Delivery, retry and the dead letter topic

Knative's four fields, with one rename:

| Field | Ours | Note |
|---|---|---|
| `retry` | same | attempts *after* the first, matching Knative, so total = `retry` + 1 |
| `backoffPolicy` | same | `linear` or `exponential` (`backoffDelay * 2^n`) |
| `backoffDelay` | same | ISO 8601, e.g. `PT2S` — parsed by a 20-line stdlib parser, not a dependency |
| `deadLetterSink` | **`deadLetterTopic`** | ours is a Kafka topic, `{prefix}-{userkey}-dead`, not an addressable URI |

**What "delivery failure" means here is not what it means in Knative**, and conflating
them would be a real design error. Knative retries an HTTP POST. We publish a request to
Kafka and an agent runs for minutes. So:

- **Retry covers the *dispatch*, not the agent run.** A failure to publish — broker
  unreachable, topic missing, signing error — is retried per this policy. An agent that
  runs and fails has already produced a terminal `phase=error` response event, which is
  the existing machinery (store, red card, priority-5 ntfy), and re-running it is a
  decision only a human should make: Phase 1 RQ-1 assumes agents are idempotent for
  *redelivery of the same request*, which is a much weaker assumption than
  "automatically re-run a failed agent at full API cost".
- **The dead letter record carries why.** Modelled on Knative's channel-level extension
  attributes, with our own names so nobody mistakes them for the Knative ones:
  `ce_rossoctlerrorreason` (`publish-failed` | `rate-limited` | `depth-exceeded` |
  `fanout-exceeded` | `template-error` | `agent-unknown`), `ce_rossoctlerrortrigger`,
  and the original event's `id` in `ce_causationid`. An operator reads it with
  `kafka-console-consumer` and the reason is the first thing they see.
- **Nothing consumes `-dead` continuously.** A consumer would need a policy, and the
  only correct policy is a human. It has 1 partition and the default 24 h retention,
  and `dead_lettered` on the trigger's status is what tells anyone to go look.

### 7.6 API surface

```text
POST   /v0/events                      # ingress — the Broker's door. CloudEvents HTTP binding.
PUT    /v0/triggers/{name}             # create or replace. Declarative: idempotent by name.
GET    /v0/triggers                    # list the CALLER's triggers (JSON or HTML, like /v0/groups)
GET    /v0/triggers/{name}             # one, with status
DELETE /v0/triggers/{name}
POST   /v0/triggers/{name}/disable     # keep the definition, stop matching
POST   /v0/triggers:test               # dry run: body = {event, triggers?} -> what matches, rendered prompts
```

Four notes:

- **`PUT`, not `POST`.** A trigger has a name the caller chooses, and re-applying a
  definition must converge rather than accumulate. Knative is declarative and that is
  the property worth copying; `POST` with an `Idempotency-Key` (what `/v0/groups` does)
  is the right shape for *creating work*, and the wrong shape for *declaring
  configuration*. The route regex is `[a-z0-9-]{1,63}` — a DNS-1123 label, so a trigger
  name can become a Kubernetes object name later without a migration.
- **`POST /v0/events` speaks the CloudEvents HTTP binding**, which means a prefix
  detail that will otherwise cost someone an afternoon: **the HTTP binding uses `ce-`
  with a hyphen; the Kafka binding uses `ce_` with an underscore.** `shared/ce.py`
  hard-codes `CE_HEADER_PREFIX = "ce_"`. So `from_http_binary` is not a copy of
  `from_kafka_binary` — the prefix is parameterised, structured mode
  (`Content-Type: application/cloudevents+json`) is also accepted, and a test pins both
  spellings against one event. Getting this wrong produces an event with no attributes
  at all, which looks like a filter bug.
- **Ingress authenticates two ways.** A bearer token (§2) for a human or a script, or an
  HMAC webhook signature for a system that has no bearer token — GitHub's
  `X-Hub-Signature-256` over the raw body, verified with `hmac.compare_digest` against a
  per-trigger-source secret. Stdlib, and it means a GitHub webhook can point straight at
  the ingress. The webhook secret is a Secret, per user, provisioned alongside the
  registry. An unauthenticated ingress would be a way to make someone else's agents run
  at someone else's cost, which is the most expensive open endpoint imaginable.
- **`POST /v0/triggers:test` is the feature Knative users miss most.** Give it an event,
  get back which of your triggers match, which filter clause failed for the ones that
  did not, and the exact rendered prompt for the ones that did — without publishing,
  running or spending anything. For a system where a mistake costs money and a filter
  typo is silent, a dry run is not a convenience.

### 7.7 Loop prevention — the control this API lives or dies by

A trigger starts an agent. An agent emits events (§5.6). Events can match triggers.
That is a cycle, and in this system a cycle does not hang — **it bills**, in parallel,
until someone notices. Four independent controls, because any one of them can be
defeated by a configuration mistake:

**1. A signed hop counter.** New extension attribute `ce_depth`, an integer as a string.
`POST /v0/events` sets `0`. A request published by a trigger carries `depth + 1`.
Response events inherit their request's depth. A trigger refuses to fire on an event
whose depth is `>= EB_TRIGGER_MAX_DEPTH` (default **3**) and records a
`trigger.dropped` with reason `depth-exceeded`.

`depth` joins `signing.SIGNED_ATTRS`, and it must: an unsigned counter can be reset to
zero in flight, which turns the whole control off. See §8.3 — this is the second
canonicalisation-breaking change in Phase 3 and it has to land together with `userkey`.

**2. Responses are not trigger-eligible by default.** `EB_TRIGGER_ON_RESPONSES=false`.
Agent chaining — "when the triager finishes, run the fixer" — is a legitimate and
powerful pattern, and it is also the shortest path to a loop. It is opt-in, and when
enabled the depth limit is doing real work rather than being a backstop.

**3. A fan-out cap.** `EB_TRIGGER_MAX_FANOUT` (default 5). One event matching 40
triggers starts 40 agents, and the natural way to reach 40 is a `prefix` filter that is
broader than its author realised. Beyond the cap the dispatcher fires the first N in
creation order and dead-letters the rest with `fanout-exceeded`, so the outcome is
deterministic and visible rather than "some of them ran".

**4. Per-user and per-trigger rate limits.** `limits.max_per_hour` per trigger, and
`EB_USER_RATE_LIMIT_PER_MIN` per tenant as the backstop. A token bucket in memory on
the single-replica bridge — which is the one place where Phase 1 §8.6's single-replica
constraint is an *advantage*, since a distributed rate limiter needs shared state the
bridge deliberately does not have.

The ordering is also a design decision: depth is checked **before** rendering the
prompt, which is before publishing. A cheap check first means a loop at depth 4 costs a
dictionary lookup, not a template render.

### 7.8 Where triggers are evaluated, and per-user scoping

```text
POST /v0/events ──▶ {prefix}-{userkey}-events ──▶ TriggerDispatcher (one thread, in EventBridge)
                                                      │ match attributes against the user's triggers
                                                      │ check depth, fan-out, rate
                                                      │ render prompt
                                                      ▼
                                                 publish_request(topic={prefix}-{userkey}-requests)
```

- **Triggers are per user.** They live in the tenant's `sessions.sqlite` (§6.1), so the
  dispatcher evaluating alice's inbox can only ever see alice's triggers. The isolation
  is the same file boundary as everything else, not a `WHERE` clause.
- **One dispatcher thread, over the `-events` topics**, subscribed through the same
  `ensure_subscribed` discipline as §3.2 and for the same reason. 1 partition per inbox
  (§3.7) keeps evaluation ordered per tenant, which makes `max_per_hour` accounting
  trivially correct.
- **The dispatcher does not run agents**; it publishes requests. So everything already
  built applies unchanged: KEDA sees the lag and scales the user's runner, the
  per-correlation router serialises, offsets commit after the terminal event, the
  transcript is checkpointed. A trigger is a *producer*, which is why this section is
  short — that was the point of putting it there.
- **Ingress does not evaluate.** `POST /v0/events` writes to Kafka and returns `202`
  with the event id. Evaluating synchronously would put template rendering and N
  `publish_request` calls (~150–200 ms of Ed25519 each) inside a webhook's timeout, and
  GitHub gives 10 seconds. Durability first, evaluation after, which is also what makes
  the retry policy in §7.5 implementable at all.

**Unmatched events are counted, not silently dropped** — the one place we deliberately
diverge from Knative. `unmatched_total` per user, the last 20 unmatched events' type and
source kept in a ring buffer, both on the triggers HTML page. The most common trigger
problem is a filter that matches nothing, and in Knative the symptom is silence. A
counter and twenty recent type strings turn a half-hour of guessing into a glance.

---

## 8. Configuration

Every variable below is off or single-tenant by default, so an existing deployment that
upgrades and changes nothing behaves exactly as it did in Phase 2.

### 8.1 EventBridge

| Variable | Default | Effect |
|---|---|---|
| `EB_TENANCY_MODE` | `single` | `single` = Phase 2 behaviour, one topic pair, no ownership checks. `multi` = everything in §3–§7. |
| `EB_TOPIC_PREFIX` | `kev1` | First component of every per-user topic name (§3.1). |
| `EB_USER_REGISTRY_PATH` | empty | The user registry ConfigMap (§2.5). Empty in `multi` mode denies everyone. |
| `EB_SUBSCRIBE_TIMEOUT_S` | `15` | How long `ensure_subscribed` waits for an assignment before answering `503` (§3.2). |
| `EB_MAX_OPEN_STORES` | `64` | LRU ceiling on open per-user SQLite stores (§6.1). |
| `EB_NTFY_MODE` | `sh` | `sh` = ntfy.sh. `incluster` = the in-cluster service (§4.3). |
| `EB_NTFY_TOPIC_SECRET_PATH` | empty | HMAC seed for derived ntfy topics (§4.2). **Required** in `multi` mode. Secret mount, env only. |
| `EB_CAPABILITY_SECRET_PATH` | empty | HMAC seed for `?k=` continue keys (§4.4). Empty disables them, so `/continue` is owner-only. |
| `EB_RUNNER_KIDS` | `runner-{userkey}` | `userkey:kid,…`, or the default convention. Which runner may write which tenant's transcript (§6.3). |
| `EB_TRANSCRIPT_AUTH` | `none` | `none` \| `token` \| `signature`. `none` is refused at startup in `multi` mode. |
| `EB_TRANSCRIPT_RETENTION_DAYS` | `7` | Sweeper cutoff (§6.5). `0` disables. |
| `EB_TRIGGERS_ENABLED` | `false` | The whole of §7. Off by default — it starts agents with no human present. |
| `EB_TRIGGER_MAX_DEPTH` | `3` | Hop limit on `ce_depth` (§7.7). |
| `EB_TRIGGER_MAX_FANOUT` | `5` | Triggers one event may fire (§7.7). |
| `EB_TRIGGER_ON_RESPONSES` | `false` | Whether response events are trigger-eligible. Opt-in agent chaining. |
| `EB_USER_RATE_LIMIT_PER_MIN` | `30` | Per-tenant requests-created backstop. |
| `EB_WEBHOOK_SECRETS_PATH` | empty | Per-user HMAC secrets for `POST /v0/events` webhook signatures (§7.6). Secret mount. |

### 8.2 EventRunner

| Variable | Default | Effect |
|---|---|---|
| `ER_USERKEY` | empty | Which tenant this runner serves (§3.3). Stamped on every response as `ce_userkey`. Required once `REQUEST_TOPIC` is not the default. |
| `ER_AGENT_DIR` | `/etc/rossoctl/agents` | Where baked `AgentSpec`s live (§5.2). |
| `ER_AGENT_NAME` | `default` | Fallback agent when neither the trigger nor the registry names one. |
| `ER_SKILL_SOURCES` | empty | Allowed source prefixes. **Empty means no fetching at all** (§5.4 gate 1). |
| `ER_SKILL_REQUIRE_SIGNATURE` | `true` | Whether an executable-carrying bundle must be signed. `false` is for development and says so at startup. |
| `ER_SKILL_CACHE_DIR` | `{tmpdir}/eventrunner/skills` | Digest-keyed fetch cache (§5.4). |
| `ER_SKILL_MAX_BYTES` | `8388608` | Total uncompressed bundle size. |
| `ER_EMIT_SOCKET` | per-run path | Where `rossoctl-emit` reaches the runner (§5.6). |
| `ER_EMIT_MAX_EVENTS` | `2000` | Custom events per run. |
| `ER_TRANSCRIPT_AUTH` | `none` | Must match `EB_TRANSCRIPT_AUTH`. A mismatch is a `401` on every checkpoint, so §6.3's startup check prints both. |

### 8.3 Two notes that are not table rows

**The signed-attribute set changes twice, and both changes must land together.**
`userkey` (§2.6) and `depth` (§7.7) both join `signing.SIGNED_ATTRS`. Phase 2 §4.2
already set the precedent and the rule: adding an attribute changes canonicalisation, so
a signer and a verifier on different versions disagree about every signature. Doing it
in two commits would invalidate canonicalisation twice for no benefit, so it is **one
change**, and deployments with signing already enabled must upgrade both services
together. Deployments with signing off (the default) are unaffected, which is most of
them.

**Secret-vs-ConfigMap, continuing Phase 2 §5's rule.** Seed and HMAC-secret paths
(`EB_NTFY_TOPIC_SECRET_PATH`, `EB_CAPABILITY_SECRET_PATH`, `EB_WEBHOOK_SECRETS_PATH`)
name **Secret** mounts and are env-only, never `config.toml`. The user registry, the
keysets and the agent directory name **ConfigMaps** or image paths — they record what an
operator approved and grant nothing. `test_manifests.py` already pins this rule for
`NTFY_TOPIC`/`NTFY_TOKEN` and should be extended to every new variable here, because the
rule is only worth having if it is enforced by a test.

---

## 9. Rollout order

The dependency order is real — several steps are unsafe before the one above them:

1. **`shared/tenancy.py`** — `userkey`, `TopicSet`, `ntfy_topic`. Pure functions, fully
   testable, no deployment. Everything else imports it.
2. **`TopicSet` threaded through both services, `single` mode only.** No behaviour
   change whatsoever; the existing suite is the check. This is the largest diff in the
   phase and the least risky, which is a good shape for a first step.
3. **The registry and `Caller`** (§2.4, §2.5), still in `single` mode. `ce_userkey` is
   stamped on events but nothing routes on it — the Phase 2 §4.4 "audit before enforce"
   pattern, applied to tenancy.
4. **`SIGNED_ATTRS` += `userkey`, `depth`** (§8.3). One change, both services.
5. **Per-user stores** (§6.1) and **owner-scoped reads** (§6.2), behind
   `EB_TENANCY_MODE=multi`. Now `multi` is usable by one real tenant.
6. **Transcript auth** (§6.3). Must come before any second tenant exists: §6's opening
   paragraph is why.
7. **`scripts/k8s_tenant.py`** and the per-user Deployment/ScaledObject/KafkaTopic
   templates (§3.3–§3.5). Two tenants on a cluster, Tier A.
8. **ntfy**: derived topics (§4.2) first, then `incluster` (§4.3), then capability keys
   (§4.4). Each is independently useful and each closes part of Phase 2 §3.1.
9. **`AgentSpec`, baked only** (§5.1, §5.2). No fetching. Useful on its own: a tool
   policy is the control §7.4 depends on, so it has to exist before triggers do.
10. **Triggers** (§7), with `EB_TRIGGER_ON_RESPONSES=false` and a low
    `EB_USER_RATE_LIMIT_PER_MIN`. The dry-run endpoint lands in the same change as the
    dispatcher, not after it.
11. **Fetched skills** (§5.3, §5.4). Deliberately last: it is the largest new attack
    surface and the least necessary for the demo to be impressive.
12. **Tier B — the dedicated broker and KafkaUser ACLs** (§3.6). Independent of
    everything above; it can be done at any point after 7 and is what upgrades the claim
    from "EventBridge will not serve it to you" to "the broker will not".

Steps 1–4 are safe to merge while `multi` has never been switched on anywhere. That is
the property worth preserving: the first four steps are refactors with tests, and
everything risky is behind a flag that defaults off.

---

## 10. What is deliberately left open

In the spirit of Phase 2 §3, the things a demo of this must not claim:

- **Tier A is not confidentiality against the cluster network** (§3.6). It is the claim
  "EventBridge will not serve one user another user's events". Anything that can reach
  the shared broker can read any topic. Tier B fixes it and costs a broker.
- **Prompt injection is mitigated, not solved** (§7.4). The tool policy bounds the
  damage; nothing prevents the injection.
- **Transcripts are not encrypted by the application** (§6.4), and the reasoning is a
  refusal to hand-roll an AEAD, not an argument that it does not matter.
- **Email identities are unverified in Phase 3** (§2.4). The OIDC slot exists; the
  verifier does not. Until it does, an email identity is an operator's assertion, and
  `ce_submitteriss` is what lets a reader tell.
- **EventBridge remains the single point of trust** (Phase 2 §2.7). Per-user isolation
  bounds what a compromised *runner* can do. A compromised bridge can still claim any
  user, publish to any topic, announce on any ntfy topic and mint any capability key.
  Multi-tenancy makes this *more* significant, not less, and a future phase that wanted
  to address it would be looking at per-tenant bridge instances, which costs the
  single-writer property every bit of §6.1 and §7.7 relies on.
- **EventBridge is still single-replica** (Phase 1 §8.6), and now it is on the hot path
  for N tenants, holds N SQLite stores, runs the trigger dispatcher and does the
  Ed25519 signing. Phase 2 §6.1's split-consumer-group trap is still live and now has
  more ways to bite. **The per-tenant throughput ceiling has not been measured** and
  should be, before anyone deploys this for more than a handful of users.
- **The approved lists are still files** (Phase 2 §3.3). The registry records what an
  operator approved, not what a platform attested. SPIRE remains the upgrade path, and
  §3.3's per-user `kid` is the thing that makes it a shorter path than it was.
- **No per-tenant network policy is specified.** Per-user runner pods should not be able
  to reach each other, or the bridge's store, or arbitrary egress — that is a
  `NetworkPolicy` per tenant, which `k8s_tenant.py` is the right place to render and
  which this document does not design.
- **Kafka history outlives deletion** (§6.5), bounded by 24 h retention.
- **`cesql` is absent** (§7.3), and so is any filtering on `data`.

---

## 11. Implementation tasks

| | Task | Depends on |
|---|---|---|
| **T1** | `shared/tenancy.py`: `userkey`, `_canonicalise`, `_slug`, `TopicSet`, `ntfy_topic`. Tests: collision cases, the GitHub case-folding agreement with `ghauth.is_allowed`, topic/DNS/ntfy legality over a corpus of adversarial identifiers. | — |
| **T2** | Thread `TopicSet` through `kafka_out`, `kafka_in`, `RequestsMirror`, `GroupMirror`, `eventrunner/consume`. `single` mode only; zero behaviour change. | T1 |
| **T3** | `Caller`, the registry loader, `auth.resolve` returning `userkey`; `ce.EXT_USERKEY`; stamp it on requests. | T1 |
| **T4** | `SIGNED_ATTRS` += `userkey`, `depth`, in one change, both services. | T3, T13 |
| **T5** | `StoreRegistry` + per-user store files + the global `correlationid → userkey` index + lazy `Minter` seeding (§2.6). | T3 |
| **T6** | `Consumer.ensure_subscribed` with the blocking assignment wait, `auto_offset_reset=earliest`, and submit-path ordering. Test the race explicitly: publish before subscribe must fail the test. | T2 |
| **T7** | Owner-scoped reads: `404` not `403`, the group-list filter, and the single-tenant bypass. | T5 |
| **T8** | Transcript auth: `X-Rossoctl-Kid`/`Ts`/`Signature`, the `EB_RUNNER_KIDS` tenant check, the ±300 s replay window, and the runner side. | T5 |
| **T9** | `capability.py` + `?k=` on `/continue` + the ntfy action URL. | T7 |
| **T10** | ntfy: derived topics, per-event topic selection, the `unattributed` counter. | T1 |
| **T11** | ntfy `incluster`: `k8s/ntfy/` manifests, Route and kind Ingress, the ACL/token provisioning in `k8s_tenant.py`, and the Service-vs-Route URL distinction. | T10 |
| **T12** | `scripts/k8s_tenant.py`: render/diff/apply/suspend/delete; the ScaledObject-vs-Deployment consumer-group equality check; the `userkey` recomputation check. | T3 |
| **T13** | `AgentSpec` loading, `build_cmd` from a spec, the `claude --help` flag assertion test, and the `default` spec that reproduces Phase 2's argv exactly. | — |
| **T14** | `rossoctl-emit` + the runner's socket listener + the `dev.rossoctl.agent.custom.*` prefix check + `max_events`. | T13 |
| **T15** | Trigger store, `PUT`/`GET`/`DELETE`/`disable`, the filter evaluator, the template renderer, `POST /v0/triggers:test`. | T5 |
| **T16** | `POST /v0/events`: `from_http_binary` (the `ce-` vs `ce_` prefix, both modes), webhook HMAC verification, publish to the inbox. | T15 |
| **T17** | `TriggerDispatcher`: the inbox consumer, depth/fan-out/rate checks in that order, `trigger.matched`/`trigger.dropped` events, the dead-letter writer, `unmatched_total` + the ring buffer. | T16, T4 |
| **T18** | Triggers HTML page, in the style of the group page: triggers with status, recent matches, recent unmatched types, dead-letter count. | T17 |
| **T19** | Skill fetching: the five gates of §5.4, the canonical tree digest, `tarfile` `filter="data"` plus the extra caps, the digest-keyed cache, per-correlation placement. | T13 |
| **T20** | Tier B: a `Kafka` CR in `kev1`, `KafkaUser` per tenant, SASL in both services, KEDA `TriggerAuthentication`. Verify the `AclRule` shape against the installed CRD first (§3.6). | T12 |
| **T21** | Retention sweeper + `DELETE /v0/users/me/data`, with the summary of what it could not delete. | T5 |
| **T22** | Extend `test_manifests.py` to pin the Secret-vs-ConfigMap rule over every variable added in §8. | T3 |

---

## 12. Reading order for whoever picks this up

- **Running it?** `README_PHASE1.md` for the base deployment, then §8.1's
  `EB_TENANCY_MODE` and §9's rollout order. Do not start at §7.
- **Adding multi-tenancy to an existing deployment?** §9, in order. Steps 1–4 are safe
  to merge before anything is switched on; step 4 is the one that requires both services
  to move together.
- **Changing how an identity becomes a name?** §2.3 first, and specifically why the key
  ends in a hash. The lossy slug is safe *only* because of the digest, and the registry's
  collision refusal is what covers the 32-bit birthday bound.
- **Working on notifications?** §4.1 before §4.2 — why the topic name must not contain
  the user's identifier is the whole design, and §4.3's iOS note is why it matters even
  on a self-hosted server.
- **Deploying ntfy in the cluster?** §4.3, and read the caveats at the end of it before
  promising anyone notifications on a phone from a kind cluster.
- **Building the trigger API?** §7.7 first. Every other part of §7 is plumbing; the loop
  controls are the part that keeps an event-driven agent system from being a cost
  incident.
- **Adding a skill source?** §5.4's five gates, in order, and §5.2 for why baking is
  still the recommendation.
- **Presenting it?** §10, and specifically the Tier A/Tier B distinction in §3.6. A
  tenancy demo that says "isolated" when it means "separately named" is worse than one
  that does not exist.
