# DESIGN — Phase 2: identity on the event path

Status: draft (revision 2)
Scope: **delta over `DESIGN_PHASE1.md`.** Read that first. This document records
only what changes when the demo stops trusting whoever can reach it.

Phase 0 proved the wire shape. Phase 1 decided where the consumer runs and how
many of them there are. Phase 2 answers two questions neither phase asked:

1. **Which person asked for this work?** — and is that person allowed to ask?
2. **Which agent produced this answer?** — and is that agent one we approved?

```text
   👤 ──sign in──▶ 🌐 GitHub
   │                    │ GET /user -> login
   │  Bearer <token>    ▼
   └──────────▶ ╔═══════════════════╗
                ║ 🔒 EventBridge     ║  401 no credential
                ║    the PEP        ║  403 known, not approved
                ╚═════════╤═════════╝
                          │ ce_submitter, ce_submitteriss
                          ▼
                   Kafka:requests ─▶ EventRunner ─▶ Kafka:responses
```

The single new moving part in this half is GitHub: EventBridge verifies a
sign-in once at the edge, then records *who* on the request event. The second
half — proving which agent answered — has its groundwork in place (§4) but is
not yet wired, and this document says so plainly rather than describing it as
done.

---

## 1. What does NOT change

Stated explicitly, because the temptation in a security document is to redesign
things that already work:

- **The CloudEvent contract** (Phase 0 §2). Two new extension attributes are
  *added* (`submitter`, `submitteriss`); nothing existing changes shape.
  `ce.new_event(**attrs)` already accepts arbitrary attributes and
  `to_kafka_binary` already emits every non-empty one as a `ce_*` header, so the
  codec needed no change at all.
- **Kafka message key = `correlationid`**, the per-correlation FIFO router, the
  `uuid5` session derivation, `--session-id` / `--resume` mechanics.
- **KEDA scaling on consumer lag** and scale-to-zero. Identity is checked at the
  HTTP edge, so it never touches the scaling path.
- **Pure-Python discipline** (Phase 1 §1.1). No new runtime dependency:
  `urllib`, `hmac`, `hashlib`, `json`. In particular **no JWT library**, because
  there is no JWT to verify — see §2.1.
- **Auth is off by default.** With no client id, no approved-user list and no
  static tokens configured, `resolve()` returns "allowed, anonymous" and the
  Phase 0/1 demo behaves exactly as before. Every existing test passes
  untouched; that is the check that this is additive.

---

## 2. User identity

### 2.1 The constraint that shapes everything

**GitHub does not issue a verifiable token for user login.** The OAuth device
flow returns an *opaque* access token: no signature, no claims, nothing to check
offline. The JWKS at `token.actions.githubusercontent.com` is for Actions
workloads, not users, and there is no user-facing equivalent.

This is the fact that rules out the obvious design. We cannot validate a token
locally the way AuthBridge validates a Keycloak JWT. EventBridge must ask GitHub
who holds the token, via `GET https://api.github.com/user`.

Three consequences, all accepted deliberately:

| Consequence | Why it is acceptable, and what it costs |
|---|---|
| Sign-in depends on GitHub being reachable | A failed lookup is a `401`, never an allow. Failing closed is the only safe direction for "who is this". A GitHub outage means nobody can submit — correct, and an operator can keep a static break-glass token (§2.5). |
| A lookup per request, against a 5000/hour budget | The cache (§2.3) is therefore **load-bearing, not an optimisation**. |
| GitHub's latency joins the request path | Same answer: the cache. A cache hit costs nothing. |

### 2.2 Why the device flow

The alternative — an OAuth web flow with a redirect — needs EventBridge to host
a callback endpoint at a URL GitHub can reach. The demo runs on a laptop behind
a VPN (Phase 1 §"Remote access caveat"), so that is precisely what it cannot do.

The device flow inverts it: the CLI asks GitHub for a code, the user types the
code into a page GitHub already hosts, and the CLI polls. Nothing needs to reach
*us*. It also needs **no client secret**, which is why the client id is a
committed default rather than a capability like an ntfy topic.

`login` prints the code and blocks rather than opening a browser. Auto-opening
fails silently over SSH and inside a container, which is where this is most
often run.

**Scopes requested: none.** Verified against the live API — `GET /user` answers
with `x-accepted-oauth-scopes:` empty, so an unscoped token reads the login. The
demo asks for the least access that answers its question, and cannot read a
repository even if the token leaks.

GitHub's pacing contract is honoured exactly: `authorization_pending` means keep
polling, `slow_down` means keep polling and add five seconds. Polling faster
than asked rate-limits the whole OAuth App, which would break sign-in for
everyone rather than just the impatient caller.

### 2.3 The cache

Token → login, TTL 300 s by default, keyed by **`sha256(token)`**.

The hash is not decoration. A cache keyed by the raw token means a memory dump,
a careless `repr`, or a debug log yields a working credential. Keyed by hash, it
yields nothing.

**Failures are not cached**, so a GitHub outage cannot pin a legitimate user to a
refusal for the whole TTL — each request retries.

**It does not speed up revocation**, and an earlier revision of this document
claimed it did. A cache hit short-circuits `resolve()` before `fetch_login` runs,
so a token revoked on GitHub keeps authenticating until its *positive* entry
expires. Measured with an injected clock: still accepted at t+299 s, refused at
t+300 s. The window is the full TTL.

That is bounded, configurable via `EB_GITHUB_CACHE_TTL_S`, and 300 s is a
defensible trade against a 5000/hour budget — but it is a real window, and
`test_a_revoked_token_keeps_working_until_its_positive_entry_expires` now pins it
so the claim cannot drift again. Lower the TTL if prompt revocation matters more
than API calls.

### 2.4 `401` and `403` are different answers

This is the design decision most worth defending, because collapsing them is
the easy thing to do.

| Status | Meaning | Carries `WWW-Authenticate`? |
|---|---|---|
| `401` | "I do not know you." No credential, a malformed one, or one GitHub does not recognise. | Yes — retrying with a credential is the remedy. |
| `403` | "I know exactly who you are, and you are not approved." | **No** — retrying with another credential is *not* the remedy. |

The `403` body names the login that was refused (`mrsabath is not on the
approved-user list`). A user who is told only "forbidden" goes looking for a
broken token; a user told *which identity* was refused knows to ask an operator.
That is the difference between an actionable error and a support ticket.

An empty approved-user list **denies everyone**. The other reading — empty means
everybody — would turn a missing environment variable into an open door, which
is exactly the class of failure a security feature must not have.

Logins compare case-insensitively, because GitHub logins are. Comparing exactly
would refuse a genuinely approved user over capitalisation, which reads as a
broken deployment rather than a policy.

### 2.5 Static tokens remain, on purpose

`EB_AUTH_TOKENS` is not deprecated. It serves three things GitHub sign-in cannot:

- **Tests must not reach the network.** A suite that calls GitHub is slow,
  flaky, rate-limited, and fails on a machine without credentials.
- **An offline demo has to stay possible.** Conference wifi is not a dependency
  worth accepting.
- **A break-glass credential.** When GitHub is unreachable, an operator with a
  static token can still drive the system. `resolve()` checks the static map
  **before** GitHub, precisely so the fallback is fastest when it is needed: the
  reverse order made every break-glass request pay a full `fetch_login` timeout
  on a call that could never succeed, against the budget §2.3 says the cache
  exists to protect. Nothing is shadowed — a static secret would have to
  deliberately collide with a live `gho_`-shaped token.

### 2.6 Identity on the event, and what it is worth

Two attributes ride the request:

```text
ce_submitter:    mrsabath
ce_submitteriss: github
```

`submitteriss` exists because without it a reader cannot tell a verified GitHub
login from a name an operator typed into an environment variable. Absent issuer
means "static token" — the weaker claim, visible as such.

**On the spelling.** CloudEvents v1.0 requires attribute names to be lower-case
`[a-z0-9]` only — no underscore, hyphen or upper case — because an event crosses
several hops and protocols disagree about metadata case-sensitivity. This first
shipped as `submitter_iss` and was caught in review, not by the code: the codec
here only adds and strips the `ce_` prefix, so a non-compliant name round-trips
locally and is rejected or silently dropped by a spec-compliant SDK, an
HTTP-binding gateway or a Knative broker further along. `test_roundtrip_binary.py`
now asserts the rule over every `EXT_*` constant, so the next extension cannot
repeat it.

**Both are unsigned.** Anything with write access to the `requests` topic can
forge them, and the broker is plaintext. The honest claim after this phase is:

> A real GitHub user, on an approved list, authorised this request — as recorded
> by EventBridge.

**Not** "the event proves who submitted it." Making it provable needs `submitter`
inside `signing.SIGNED_ATTRS` *and* a producer that signs. See §4.

### 2.7 Where the check lives, and the trust that follows

EventBridge is the **policy enforcement point**. It verifies once, then attests
by recording. EventRunner never contacts GitHub.

That means **compromising EventBridge means being able to claim any user.** This
is standard PEP design, and the alternative is worse: passing the user's GitHub
token through to every runner would spread a live credential across every
workload and make each one a lookup client. Stated here so it is a documented
property rather than a discovery during questions.

---

## 3. What is deliberately left open

### 3.1 `/continue` is unauthenticated

`ntfy.py` emits an ntfy `http` action so a notification has a **Continue…**
button. That action is a recipe serialised into the notification, so any
credential it carries comes to rest in four places outside our control: the
payload sent to `ntfy.sh`, ntfy's message store, the phone's notification
history, and every other subscriber of the topic.

A long-lived bearer token must not go there. So `/continue` stays open, and the
reasoning is not a shrug: continuing requires already knowing an unguessable
correlationid, which is a **capability URL** — the same model the HTML transcript
already relies on. Creating *new* work is the privileged act.

**Planned fix**: derive `key = HMAC(server_secret, correlationid + exp)`, bake
`?k=<key>` into the action URL, and accept either a bearer token or a valid key.
A leaked notification then grants one conversation, with an expiry, instead of
the API. Stdlib `hmac`, nothing stored.

### 3.2 `PUT /transcript` is unauthenticated

EventRunner uses it for session checkpointing and has no credential concept
anywhere in `eventrunner/config.py`. Gating it breaks `/continue` after a cold
pod — the Phase 1 §16 Gap B path. Fixing it properly means giving EventRunner an
identity, which is §4's territory.

### 3.3 Not addressed at all

- **Kafka is plaintext**, with no transport authentication. SASL and ACLs were
  evaluated and rejected for this demo: the Kafka authorizer is global rather
  than per-listener, so enabling it either denies every existing PLAINTEXT client
  or, with `allow.everyone.if.no.acl.found=true`, makes the ACL demo vacuous.
  More importantly they authenticate the *connection*, not the payload — they
  cannot tell a real EventRunner from anything else holding valid credentials.
- **No rate limiting.** Refusing an invalid request is cheap but not free.
- **The approved lists are files.** They record what an operator approved, not
  what a platform attested.

---

## 4. Agent identity: wired

The second question — *which agent produced this answer?* — matters more than it
first appears. Before this, anything with write access to the `responses` topic
got its output stored, rendered in the HTML transcript, and pushed to the
operator's phone **as a legitimate agent answer**. With verification enabled it
lands as a rejection instead.

### 4.1 What exists

- `signing.sign_event(event, seed, kid=None)` writes a key id into the JWS
  protected header. Because the header is part of the signed input, **the `kid`
  cannot be swapped** to relabel an event as coming from another agent.
  Omitting it is byte-identical to before, so this was additive — no
  canonicalisation break and no signature migration.
- `signing.token_kid(token)` reads the `kid` *before* verification, to choose a
  key. It is a hint until verification succeeds with the key it named.
- `shared/keyset.py` maps `kid` → Ed25519 public key from a JSON file. **That
  file is the authorization list**: an unknown or absent `kid` has no key and the
  event is refused.

`select(None)` returns a key only when exactly one is approved. With several,
an unnamed token is ambiguous, and guessing would mean accepting a signature
from *any* approved agent for an event that named none of them.

### 4.2 The blocker, now cleared

This section used to read "**`sign_event()` has no production caller**" —
`ER_REQUIRE_SIGNATURE=true` rejected 100% of traffic, a kill switch rather than a
feature. That is fixed. EventBridge signs the requests and group events it
publishes, EventRunner signs terminal responses, and each verifies the other's
output against the approved-key set.

`submitter`, `submitteriss` and `groupid` joined `SIGNED_ATTRS` **after** the
signing side existed, in that order deliberately: covering them earlier would
have invalidated canonicalisation twice for no benefit. Nothing had ever
published a signed event, so the change needed no compatibility flag — but if
signing is already enabled somewhere, both services must be upgraded together,
because every grouped request now carries a signed `groupid` that an older
verifier omits when it recomputes.

### 4.3 Why not Keycloak, and why not HMAC

**Keycloak client credentials** prove an agent *holds a secret* — which a
compromised pod also holds. It adds a token-issuing dependency while proving the
least of the available options.

**HMAC** is symmetric: EventBridge would hold the key it verifies with, so it
could forge any agent's response and any agent could forge another's. That fails
the goal as stated. It is ~170,000× faster than the hand-rolled Ed25519
(0.0013 ms/op against ~150 ms), which is tempting and still wrong here.

**Ed25519** proves possession of a private key that never leaves the runner, and
upgrades cleanly: SPIRE later distributes the same keys rooted in workload
attestation, and the verification code does not change — only where keys come
from.

### 4.4 How it is wired

1. **Requests** are signed in `kafka_out.publish_request`, after `ce.new_event`
   fills `id`/`time` (both signed) and before `to_kafka_binary`, so the signature
   covers exactly what goes on the wire.
2. **Terminal responses only** are signed in `emit.py`. The seed lives on the
   Emitter, whose constructor runs once, so none of the seven `emit()` call sites
   changed. `emit()` runs per `stdout` frame and Ed25519 costs ~150-200 ms here,
   so signing every frame would add minutes to a chatty run.

   **The verifier has to match that policy, and initially did not.** Verifying
   all-or-nothing rewrote every streamed frame of every genuine run to
   `phase="error"` — found by running it against a live broker, not by any unit
   test, because every test until then used terminal events. So an event carrying
   **no** signature and **not** terminal is passed through; an unsigned *terminal*
   event is still refused, since that is the one the transcript presents as the
   answer. A frame that presents a bad signature is still checked — the exemption
   is for absence, not for failure.

   The honest limit that remains: this proves *who finished a run*, not *what it
   said along the way*. Forged `final=false` frames still render. Closing that needs
   either cheap signatures or a signed digest chain across frames.
3. **Requests are verified** in `consume.py`, keyed on the token's `kid` when a
   keyset is configured and falling back to the single-key path otherwise. A
   rejection commits the offset without running, because a bad signature is still
   bad on redelivery, and increments `rejected_unsigned` so it is distinguishable
   from the stale-request drop.
4. **Responses are verified** in `kafka_in.py` while the `CloudEvent` is still in
   hand — the check needs `.attrs`/`.data`, which `envelope_dict` has flattened.
   The decision itself is a pure function in `shared/signing.py`, which is what
   makes this path testable at all: the consumer had no behavioural test before.
5. **Group events are signed** under EventBridge's own `kid`, and the verifier
   accepts *only* that kid for them. Skipping them instead would have left the one
   hole worth closing — a forged `group.completed` ends a batch early and fires a
   "finished" notification for work that never ran. A flat keyset is not enough
   here: any approved runner would otherwise do.
6. `submitter`, `submitteriss` and `groupid` joined `SIGNED_ATTRS` last (§4.2).

**Both hazards in step 4 were real.** `from_kafka_binary` and `insert_response`
are not inside a `try`, so anything raising in the verification path would end the
consume loop for the life of the pod — silently, with the process still healthy.
The guard around the decision call fails closed only where enforcement is on: if
the verifier itself is broken, an unverifiable event is not evidence of anything.

On failure the event is stored with `phase="error"` rather than dropped — a drop is
indistinguishable from an agent that never answered. That reuses machinery already
wired: ntfy **priority 5** with an error tag (the error body comes from
`data["text"]`, so the key name is load-bearing), a red card in the live SSE
transcript, and `raw_json` persistence for audit.

**Rollout is two flags.** A keyset alone verifies and logs while storing events
unchanged; `EB_REQUIRE_RESPONSE_SIGNATURE=true` is what rewrites them. Enforcement
mutates persisted rows and pages a phone, so there is a step where the reject rate
is observable first. EventBridge refuses to start with a keyset but no
`EB_SIGNING_KID`, since there would be nothing to attribute a group event to.

---

## 5. Configuration

| Variable | Default | Effect |
|---|---|---|
| `EB_GITHUB_CLIENT_ID` | from `config.toml` | OAuth App client id. **Public** — the device flow has no client secret. |
| `EB_ALLOWED_USERS` | empty | Comma-separated approved logins. Empty denies everyone. |
| `EB_GITHUB_CACHE_TTL_S` | `300` | Token → login cache lifetime. |
| `EB_AUTH_TOKENS` | empty | `name:token,...` fallback. Empty plus no GitHub config means auth is off. |
| `EVENTBRIDGE_TOKEN` | unset | CLI: overrides the stored token, so a shell can act as another identity. |

Agent identity (§4). Every one of these is off by default, so the e2e path is
unaffected until an operator opts in:

| Variable | Default | Effect |
|---|---|---|
| `EB_SIGNING_KEY_PATH` | empty | Ed25519 seed EventBridge signs requests and group events with. Empty = no signing. |
| `EB_SIGNING_KID` | empty | Names EventBridge's key. Also the **only** kid accepted on group lifecycle events. |
| `EB_VERIFY_KEYSET_PATH` | empty | Approved-key set for responses. Empty = no verification. |
| `EB_REQUIRE_RESPONSE_SIGNATURE` | `false` | `false` = verify and log (audit); `true` = rewrite failures to `phase=error`. |
| `ER_SIGNING_KEY_PATH` | empty | Seed EventRunner signs terminal responses with. |
| `ER_SIGNING_KID` | empty | Names this runner's key. Needed once more than one runner is approved. |
| `ER_REQUIRE_SIGNATURE` | `false` | Refuse unsigned or badly-signed requests. |
| `ER_VERIFY_KEYSET_PATH` | empty | Approved-key set for requests. Empty falls back to `ER_VERIFY_KEY_PATH`'s single key. |

Seed paths name **Secret** mounts and are env-only, never `config.toml`. Keyset
paths name **ConfigMaps** — only public keys belong in them, and `keyset.load()`
cannot tell a seed from a public key by length, so the file's location is what keeps
the distinction reviewable.

The GitHub client id lives in `config.toml` because it is not a capability.
`test_manifests.py` pins the opposite rule for `NTFY_TOPIC`/`NTFY_TOKEN`, and that
distinction is the point: one is public by construction, the others grant access.

---

## 6. Verification

**Tests: 490 passed, 5 skipped.** Baseline on the same tree is 450/5, so this
adds 40 and regresses nothing. No test reaches the network — the device flow and
`GET /user` are exercised through injected fakes.

Verified end to end against a real GitHub account, not only in unit tests:

| Check | Result |
|---|---|
| `login` browser flow | `✔ signed in as mrsabath`, token stored `0600` |
| No credential | `401` + `WWW-Authenticate` |
| Unknown token | `401` |
| Real user, not on the list | `403` — `mrsabath is not on the approved-user list` |
| Approved user | `202`, agent ran, `final=True` |
| On the wire | `ce_submitter:mrsabath`, `ce_submitteriss:github` |
| Group of 5 | every member carried both attributes |

### 6.1 One environment finding worth recording

During the group test the completion counter read 1/5 while all five members had
run. Cause: several EventBridge instances were running against one Kafka, and the
**response consumer uses a fixed shared group** (`eventbridge-responses`) while
the requests mirror uses a per-PID one. The instances therefore split the twelve
response partitions between them and no single one saw every response.

Not a defect in this change, and not a bug in Phase 1 either — a single deployed
EventBridge is the intended topology, and Phase 1 lists multi-replica EventBridge
as deferred. But it is a real trap for anyone running two copies on a laptop:
**symptoms look like lost responses, not like a split consumer group.** Worth
knowing before a demo.

---

## 7. Reading order for whoever picks this up

- **Running it?** `README_PHASE1.md`, then `EB_GITHUB_CLIENT_ID` and
  `EB_ALLOWED_USERS` from §5.
- **Changing the identity model?** §2.1 first — the opaque-token constraint is
  what rules out the design most people reach for.
- **Finishing agent identity?** §4.2 for the blocker, then §4.4 in order.
- **Presenting it?** §2.6 and §3 — what the controls do *not* prove. A security
  demo that oversells its guarantee is worse than one that does not exist.

---

## 8. What the docs review found (added after rossoctl/rossoctl#2609)

**Scope.** §8.1–§8.4 are about **Phase 2** specifically: thirteen claims *this
document* made about the identity and signing controls, which do not hold in the code.
§8.5–§8.8 are the general procedure that came out of them, and apply to a claim made in
any phase — Phase 3's design makes claims of the same shape about tenancy and the signed
attribute set, and nothing here is Phase 2 only.

Writing the user-facing pages for this path meant restating every claim in this
document for a reader who cannot see the code. Six review rounds then checked each
restated claim **by running it**, against `eventing/` at `d9677ddd`. Thirteen did
not hold.

They are recorded here rather than edited into §3 and §4.4 in place, so the reasoning
above stays readable as the record of what was intended, and this section says what
the code does. That convention is this repository's own: where a later phase supersedes
an earlier one it says so explicitly rather than editing in place, and a correction that
deletes the original intent loses the more useful half.

### 8.1 Promises in §4.4 that the code does not keep

| §4.4 says | The code does | Issue |
|---|---|---|
| "A keyset alone verifies **and logs**" | Verifies and returns a verdict. Nothing is logged or counted, so the observable reject rate the two-flag rollout depends on does not exist. | [#886](https://github.com/rossoctl/examples/issues/886) |
| The verifier accepts "**only**" EventBridge's kid for group events, so an approved runner "cannot forge a `group.completed` and end a batch early" | Flags it, then applies it anyway. `kafka_in.py` rewrites `phase`/`data` but keeps `final` and `groupid`, so the rewritten event still reaches `on_group_event`. The group mirror replays group events unverified on restart. | [#885](https://github.com/rossoctl/examples/issues/885) |
| Step 2: "this proves *who finished a run*" | Not once stored. `insert_response` uses `INSERT OR REPLACE` on `(correlationid, sequence)`, so a later **unsigned** frame reusing a sequence number replaces the verified terminal row. | [#885](https://github.com/rossoctl/examples/issues/885) |
| "EventBridge refuses to start with a keyset but no `EB_SIGNING_KID`" | True — but nothing checks that the kid is *in* the keyset. EventBridge starts, then flags its own group events. | [#888](https://github.com/rossoctl/examples/issues/888) |

A fifth, promised nowhere but worse than any of them:

**`EB_REQUIRE_RESPONSE_SIGNATURE=true` with no keyset refuses nothing.** The decision
is only reached when a keyset is loaded — `kafka_in.py` has `ok, why = True, "not
checked"` behind `if self._keyset is not None` — so `require` is never consulted. A
forged terminal response is stored as an answer, and the startup line says `response
verification OFF`. The request side does the opposite: with `ER_REQUIRE_SIGNATURE=true`
and no key at all, `verify_event` refuses every request. **The two sides fail in
opposite directions, and only one of them is loud.**
([#888](https://github.com/rossoctl/examples/issues/888))

### 8.2 §3's "deliberately left open" is wider than stated

- **§3.1's capability-URL argument rests on ids being unguessable.** `GET /v0/groups`
  returns the 100 most recent groups without sign-in, and `GET /v0/groups/{id}/status`
  lists every member's correlation id. For any conversation in a group, the capability
  is published. ([#887](https://github.com/rossoctl/examples/issues/887))
- **§3.2 understates what `PUT /transcript` allows.** It is the checkpoint EventRunner
  **resumes from** on a cold pod, so an unauthenticated write changes what the agent
  continues with. Request signing does not cover it.
  ([#887](https://github.com/rossoctl/examples/issues/887))
- **Group-member push suppression is defeated by omitting an attribute.** `ntfy.py`
  decides on the *event's own* `ce_groupid`, not on group membership. A forged
  non-terminal frame without it is pushed, stored in the member's transcript and shown
  on the group page; a forged terminal without it is pushed at priority 5 even with
  `NTFY_GROUP_NOTIFY_ERRORS` off, and does not count the member as failed.

### 8.3 Three things about the keys

- **§2.6 records `submitter`/`submitteriss` as unsigned.** They joined `SIGNED_ATTRS`
  in §4.2, so the signature covers them. The limit that remains is a different one and
  worth stating as such: a signature proves EventBridge *asserted* the name.
- **§4.3's "the verification code does not change" under SPIRE does not hold.** The
  verifier accepts EdDSA only, and SPIRE issues EC or RSA keys. The keyset is also a
  flat JSON map of kid to key, and `keyset.load` rejects a JWKS document outright. The
  kid lookup survives; the algorithm and the file format do not.
- **A flat keyset makes the asymmetric keys attribution, not restriction.** §4.4 step 5
  says a flat keyset "is not enough" for group events, and pins them. The same
  reasoning applies to member answers and is not drawn: EventBridge's own key is in
  `EB_VERIFY_KEYSET_PATH` by requirement, so it verifies on any run's answer, as does
  any approved runner's key.

### 8.4 Why this is worth the space

Every item above is a claim this document made that a reader could have relied on. The
pattern is not carelessness about the code — §4.4 is unusually precise about
mechanism. It is that **each claim was written from the change that introduced it**,
and stayed true only for the configuration that change was exercised in. The
half-configured cases, and the cases a later change moved, are where all thirteen
were found.

The rest of this section turns that into something checkable before the next design
document is written.

### 8.5 Checking a control claim before it ships

§8.1–§8.4 are what thirteen unchecked claims cost. What follows is the procedure that
came out of them: five questions to ask of any sentence that says a security control
does something, how to check it cheaply, and what a correction should preserve.

**It is not specific to Phase 2.** The findings above are — they are claims this
document made — but the questions apply to any phase, and Phase 3's design makes
claims of exactly the same shape about tenancy and the signed attribute set. It lives
here because this is the phase whose review produced it, and because a procedure
filed away from the findings that motivated it is a procedure nobody reads.

#### The five questions

Ask these of every sentence that says a control does something. A claim that cannot
answer all five is not ready to publish.

#### 8.5.1 Which code path, by name?

Name the function or module the claim rests on, and read it. Not the change that
introduced it — the code as it stands.

> "Group events are pinned to EventBridge's kid" rested on `verify_with_keyset`'s
> `expect_kid`, which does refuse a wrong kid. The claim was about **the batch not
> ending**, and that depends on `kafka_in.py` and `group_service.py`, which no one
> re-read. The verdict was correct and ignored.

A claim about an *outcome* has to follow the path to that outcome, not stop at the
check.

#### 8.5.2 What is the default?

State it. Most controls here default to off, and a claim that reads as a description
of the system is a claim about a configuration almost nobody runs.

#### 8.5.3 What happens when it is half-configured?

Every control here has more than one variable. For each combination where one is
missing, does the system fail **closed**, fail **open**, or refuse to start?

This is where the worst finding came from. `EB_REQUIRE_RESPONSE_SIGNATURE=true` with
no keyset refuses nothing, silently, while the equivalent on the request side refuses
everything. Both were documented as "refusal is on"; only one was true.

Tabulate it. A control with two variables has four cases, and the three that are not
"both set" are the ones a reader will hit.

#### 8.5.4 Can an attacker choose the input the check reads?

If the decision reads an attribute off the event, the forger controls that attribute
— including by leaving it out.

> Group-member push suppression reads the event's own `ce_groupid`. A forged frame
> that omits it is not a group member as far as that check is concerned, so it is
> pushed.

Prefer deciding on state the attacker does not supply: group membership from the
store, not the groupid on the frame.

#### 8.5.5 What does the claim become one release from now?

If the claim depends on an open bug, it expires when the bug is fixed. Add a comment
naming the release and the issue, per the docs contributor guide's accuracy rule 2:

```markdown
<!-- VERIFY v0.9.0: drop "nothing records the failure" once rossoctl/examples#886 lands. -->
```

Say what should change, not just that something will. A maintainer acting on a vague
comment deletes the wrong sentence — and check whether the claim is *still partly
true* afterwards. "Anything that can write to the responses topic can end a batch"
survives the fix for #885 whenever enforcement is off, which is the default.

### 8.6 Run it — reading the code is not enough

Reading the code is not enough, and neither is the test suite: every one of the
thirteen coexisted with a passing suite, because the tests exercised the
configuration the claim was true in.

The cheap version is enough. For `eventing/`, a venv and a few lines driving the real
function beat a cluster:

```bash
cd eventing && python3 -m venv .venv && .venv/bin/pip install -q -e .
.venv/bin/python - <<'PY'
from shared import signing, keyset, ce
# ... build the event, call the real decision function, print the verdict
PY
```

Three rules for the harness:

- **Call the real function.** A reimplementation of the guard proves nothing about the
  guard. Where the real call site is awkward, copy its shape and cite the file and
  line in a comment.
- **Assert the outcome, not the verdict.** `response_decision` returning `False` is
  not the same as the batch not completing.
- **Print what a reader would see.** The notification text, the stored row, the
  startup line. Several findings were only visible in the output: a genuine batch
  whose group event could not be signed reports `0 finished`, which no verdict shows.

### 8.7 When a claim does not survive

Say what the code does, and keep the reasoning. These documents are the record of what
was intended; a correction that deletes the intent loses the more useful half.

The pattern that worked: leave the design section as written, add a findings section
that says what the code does, and link the issue. §8.1–§8.3 above are that shape.

And file the issue. Most of the thirteen were code bugs rather than wording mistakes,
and they are tracked as [#885](https://github.com/rossoctl/examples/issues/885),
[#886](https://github.com/rossoctl/examples/issues/886),
[#887](https://github.com/rossoctl/examples/issues/887) and
[#888](https://github.com/rossoctl/examples/issues/888).
[#889](https://github.com/rossoctl/examples/issues/889), a pidfile crash-loop, came
out of the same review without being a claim in any document. Documenting a bug
honestly is not the same as accepting it.

### 8.8 One note on the review process itself

Most of the late rounds on #2609 fixed wording that a *previous round had suggested*.
Both the author and the reviewer were writing sentences from the code's docstrings
without running them, and both were wrong at about the same rate.

Two habits came out of it:

- **Change only the clause that was challenged.** Three separate rounds were spent
  re-fixing something a previous fix had over-corrected, including one that deleted a
  sentence which was itself the fix for an earlier round.
- **A suggested wording is a hypothesis.** Run it before adopting it, including your
  reviewer's — especially when it reads as though it came from a docstring.

