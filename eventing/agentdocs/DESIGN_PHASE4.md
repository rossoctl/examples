# DESIGN — Phase 4: enrollment, and asking a human for permission

Status: draft (revision 1)
Scope: **delta over `DESIGN_PHASE2.md` and `DESIGN_PHASE3.md`.** Read those first —
Phase 2 §2.1 for the opaque-token constraint that shapes every identity decision here,
Phase 3 §4.2 for the derived ntfy topic this phase hands to a user, and Phase 3 §4.4 for
the capability key this phase extends.

Phase 2 answered *which person asked for this work*. Phase 3 answered *which tenant does
it belong to*, and designed a per-user ntfy topic that contains no identifier. Phase 4
answers two questions neither asked, and they are the two halves of one idea — **the
notification channel is bidirectional**:

1. **How does a user's phone come to be subscribed at all?** Phase 3 derives the topic
   and provisions the reader account, both operator-side. Nothing hands it to the user.
2. **How does the system ask that user for something?** Every notification so far has
   been an announcement. An agent that needs a permission it was not granted has no way
   to ask, and a human has no way to answer.

```text
   👤 ──login (Phase 2)──▶ 🌐 GitHub
   │                           │
   │  POST /v0/me/ntfy/enroll  ▼
   └──────────────▶ ╔════════════════════╗ ──derive topic (P3 §4.2)──▶ 📱 subscribed
                    ║  🔒 EventBridge     ║
                    ║     the PEP        ║ ◀──decide (?k=…)──────────── 📱 [Approve][Deny]
                    ╚═════════╤══════════╝                                      ▲
                              │ verdict (long-poll)                             │ ask
                              ▼                                                 │
                     EventRunner ──PreToolUse hook──▶ claude (tool call paused) ─┘
```

The second half is what makes Phase 3 §5.1's default-deny tool policy survivable. Phase 3
§7.4 concedes that a trigger's rendered prompt is attacker-influenced text, and §5.1
answers with a default-deny sandbox. Default-deny with **no escalation path** means the useful
work is simply refused — so the operator's rational move is to widen the allowlist,
permanently, for a capability that was needed once. An approval path is what keeps a
narrow allowlist narrow.

---

## 1. What does NOT change

Stated first, because the temptation in a phase about permissions is to redesign the
permission model:

- **The CloudEvent contract** (Phase 0 §2). One new extension attribute is *added*
  (`approvalid`, §5.3) and one new event type (`TYPE_APPROVAL_REQUESTED`). Nothing
  existing changes shape; `ce.new_event(**attrs)` and `to_kafka_binary` already emit any
  non-empty attribute as a `ce_*` header, so the codec needs no change — the same
  property Phase 2 §1 relied on.
- **`--permission-mode` stays the `AgentSpec`'s** (Phase 3 §5.1). This phase does not
  add a request-settable permission; §5.5's precedence rule ("the tool policy, permission
  mode and system prompt — **only the spec**") is exactly right and an approval is not a
  way around it. An approval permits *one call*, it does not widen the mode.
- **`PERMISSION_MODES`** remains `("plan", "acceptEdits", "bypassPermissions")`
  (`eventrunner/agentspec.py:44`). §2.3 explains why no new mode is needed.
- **EventBridge is the PEP** (Phase 2 §2.7). It verified the sign-in; it now also records
  the verdict. The trust statement is unchanged and extends: **compromising EventBridge
  means being able to approve as anybody.**
- **Pure-Python discipline** (Phase 1 §1.1). `hmac`, `hashlib`, `json`, `urllib`,
  `sqlite3`. No new runtime dependency, and in particular no QR library — see §3.4.
- **Everything is off by default.** No guarded tools, no enrollment endpoint reachable
  without auth, no approval hook in any baked agent. `ER_APPROVAL_TOOLS` empty means
  nothing is guarded and the argv is byte-identical to Phase 3's.

---

## 2. The mechanism, settled by running it

Everything in §5 rests on one question: **can a tool call be paused, mid-run, in headless
`claude -p`, until an external system answers?** If it cannot, this phase is a different
and much weaker design — a notification after the fact rather than a gate.

Phase 2 §8.6's rule applies ("run it — reading the code is not enough"), and it earned
its keep twice here: the first answer taken from documentation named a flag that does not
exist on this CLI, and the second mis-predicted which permission mode to use.

Measured against `claude 2.1.270`, with `--output-format stream-json --verbose
--max-turns 2 --settings <file>`:

| Check | Result |
|---|---|
| `PreToolUse` hook fires at all in `-p` (non-interactive) mode | yes |
| Hook **blocks** for 75 s; the tool does not run while it waits | yes — 93 s wall clock for the run |
| Per-hook `"timeout": 120` in the settings JSON is honoured | yes — the 75 s hook was not killed |
| `permissionDecision: "deny"` → the tool never executes | yes |
| `permissionDecisionReason` reaches the agent as the tool result | yes — it then explained the refusal in its answer |
| `permissionDecision: "allow"` → the tool executes | yes — `echo HELLO_FROM_TOOL` returned `HELLO_FROM_TOOL` |

The hook contract used, which is the one §5.2 depends on:

```json
{"hookSpecificOutput": {"hookEventName": "PreToolUse",
                        "permissionDecision": "allow" | "deny",
                        "permissionDecisionReason": "<shown to the agent>"}}
```

<!-- VERIFY: re-run §2's table against CLAUDE_VERSION in Dockerfile-eventrunner-claude
     on every CLI bump. The claim "the tool does not run" is version-dependent and is
     the one claim in this document that no unit test can hold. -->

### 2.1 `--permission-prompt-tool` does not exist, and the flag that does needs the SDK

The design most people reach for — and the one the public documentation points at — is a
permission-prompt MCP tool. **There is no `--permission-prompt-tool` on this CLI.** The
flag is `--permission-prompts <host|none>`, and its own help text defines `host` as *"the
SDK host"*: it requires the Agent SDK driving the session in-process. `canUseTool` is
likewise SDK-only.

EventRunner spawns a subprocess (`eventrunner/runner.py`, `subprocess.Popen` with
`stdin=DEVNULL`). Adopting either would mean rebuilding EventRunner around the SDK, which
is a far larger change than this phase, and would trade Phase 1 §1.1's pure-Python
property for it. **So the `PreToolUse` hook is the only mechanism available to a
subprocess-spawning runner**, and that is a constraint, not a preference.

### 2.2 The hook needs no new plumbing

`AgentSpec` already carries `settings_path`, populated from a spec's `settings_file` and
resolved by `_resolve_under` so it cannot escape the agent directory
(`eventrunner/agentspec.py:216-220`), and `build_cmd` already passes it as `--settings`.
So the approval hook ships as **a settings file in the baked agent directory** — which
Phase 3 §5.2 already requires to be readable by gid 0 and never writable by the agent,
for exactly the reason that matters here:

> An agent definition the agent itself could rewrite is not a policy, it is a suggestion.

That sentence is the whole security argument for where the hook lives, and it was already
written down. The hook script sits beside the settings file under the same ownership.

### 2.3 Why no new permission mode, and why not `plan`

`plan` is the wrong mode for an agent subject to approval, and this is worth recording
because Phase 3 §5.1 proposes `plan` as the safe default. Measured: under `plan` the
agent wrote a plan file and tried to call `ExitPlanMode` instead of attempting the guarded
tool — so the hook never adjudicated anything, because the call never happened. For an
agent that should *do* something subject to approval, the mode must be one where the tool
is attempted and the hook decides. `acceptEdits` is that mode, and the hook is what
narrows it.

The CLI also offers `manual` and `dontAsk` modes that `PERMISSION_MODES` does not include.
Leaving them out is correct: `manual` presumes an interactive answerer, which is precisely
what a scaled-to-zero pod does not have. The hook *is* this system's answerer.

---

## 3. Enrollment: binding a GitHub identity to a phone

### 3.1 What is missing today

Phase 3 §4.2 derives the topic (`shared/tenancy.ntfy_topic`, implemented) and §4.3
provisions the per-user ntfy reader account and token. Both are operator-side:
`k8s_tenant.py` and `kubectl exec`, with the token "handed to the user once, at
provisioning time", printed to the **operator's** terminal.

That is a provisioning story, not an enrollment story. A user who has signed in with
GitHub still has no way to learn their own topic, and the operator is a required
participant in every signup. It does not scale past the operator's own phone — which is
the configuration the demo has today.

Also true, and the reason this cannot simply be "print the topic": `NtfyPublisher` is
still single-topic. It reads `self._cfg.topic` for every event
(`eventbridge/ntfy.py`), and Phase 3 §4.2's `_topic_for` is design only. Enrollment
without per-user publishing would hand a user a topic nothing publishes to.

### 3.2 The surface

```
$ rossoctl-events login                      # Phase 2 device flow
  ✔ signed in as mrsabath

$ rossoctl-events ntfy enroll
  ✔ enrolled gh-mrsabath-4c1d9e07
  topic : kev1-k7qf3mz2xa9pbw4nsdhe6tcyu5
  app   : ntfy://ntfy-kev1.apps.ykt1.hcp.res.ibm.com/kev1-k7qf3mz2xa9pbw4nsdhe6tcyu5
  reader token: tk_...                       (shown once — not stored by us)
```

`POST /v0/me/ntfy/enroll`, authenticated by the Phase 2 bearer token.
`auth.resolve_caller` already returns a `Caller` carrying `userkey`
(`eventbridge/auth.py:271`), so the handler derives the topic with
`tenancy.ntfy_topic(prefix, caller.userkey, secret)` and returns it.

A matching `GET /v0/me` reports the caller's identity, userkey and enrollment state —
useful on its own, and the thing a user hits when a notification did not arrive.

### 3.3 The decisions

- **The user supplies nothing.** The topic is derived server-side from the `userkey`
  `resolve_caller` already resolved. There is no user-controlled input to validate, and
  therefore no way to enroll onto somebody else's topic. This is Phase 2 §8.5.4 applied
  at a new surface: *do not let the caller choose the input the decision reads.* The
  "register the ntfy topic you already use" variant is rejected for that reason, and
  because Phase 3 §4.1 already argues at length that the name must be server-derived and
  unguessable.
- **Enrollment is a read, not a write.** The topic is *derived, not stored* (Phase 3
  §4.2), so calling `enroll` twice returns the same topic and creates no durable state
  keyed by it. What *is* stateful is the ntfy reader account, which lives in ntfy's own
  auth db — so the row this endpoint writes records only `enrolled_utc` and the
  provisioning state, never the topic. Nothing durable is keyed by the ntfy topic, which
  is what lets Phase 3 §4.2's secret rotation rotate every topic at once.
- **The reader token is shown once and never stored here.** Phase 3 §4.3's reasoning
  stands unchanged: storing per-user reader tokens would collect every user's
  notification access in one place for no benefit, because EventBridge never reads. A
  lost token means re-enrolling, which mints a new one.
- **`incluster` mode is the one to demo** (Phase 3 §4.3). On `ntfy.sh` the unguessable
  name is the *only* control; in-cluster adds `auth-default-access: deny-all` plus a
  read-only ACL on exactly one literal topic, so enrollment has something to grant.

### 3.4 The QR code, and why it is optional

Typing 26 random base32 characters into a phone is the step where enrollment fails in
practice, so the `ntfy://` deep link exists to be scanned rather than typed. But Phase 1
Phase 1 §1.1 forbids `qrcode`/`Pillow`, so a terminal QR means ~150 lines of hand-rolled
encoder in this repository forever.

**Recommendation: ship the deep link as text first, and add the QR only if a demo shows
the link is not enough.** A link is copy-pasteable from a terminal over SSH, which is how
this is most often run (Phase 2 §2.2 makes the same argument about not auto-opening a
browser). Recording the recommendation rather than silently choosing is the point: the QR
is a nice-to-have whose cost is a permanent maintenance obligation, and that trade should
be visible.

### 3.5 The honest bound

Enrollment proves that **the GitHub account asked for a topic**. It does not prove that
the phone which later subscribes belongs to that person: anyone who sees the terminal
output, the deep link or the token can subscribe. The claim is therefore the same shape as
Phase 2 §2.6's — *a real GitHub user, on an approved list, requested this topic, as
recorded by EventBridge* — and **not** "this topic reaches only that person's phone".

What bounds it: the name is unguessable rather than derived from a public identifier
(Phase 3 §4.2), the display is one-time rather than a name anyone can reconstruct, and in
`incluster` mode the ACL is read-only on one topic. A pairing-code flow — where the phone
redeems a short-lived code — would prove possession of the phone, and is the upgrade path.
It is not in this phase because it needs a user-reachable web surface, which Phase 2 §2.2
records that this deployment specifically cannot assume.

---

## 4. What an approval is

An approval is **one decision, by one named human, about one tool call, with one
outcome**. It is deliberately not a role, not a grant, and not a policy. Those are §8's
territory and are left out on purpose.

It is a new resource rather than a field on an agent or a group, because its lifecycle
does not match either: it is created by a *runner* (not a caller), decided by a *phone*
(not the API), read by a *hook*, and it is useful after the run that created it has ended.

---

## 5. The approval round trip

### 5.1 The surface

| Method | Path | Who calls it | Purpose |
|---|---|---|---|
| `POST` | `/v0/approvals` | the hook, in the runner | register a request; returns `approvalid` |
| `GET` | `/v0/approvals/{id}?wait=N` | the hook | **long-poll** for the verdict |
| `POST` | `/v0/approvals/{id}/decide` | the phone (`?k=…`) | record allow or deny |
| `GET` | `/v0/approvals/{id}` | the phone | HTML card: *what am I approving?* |
| `GET` | `/v0/approvals` | a browser | the owner's pending and recent decisions |

```text
claude ──PreToolUse(Bash)──▶ hook
                               │ POST /v0/approvals {tool, input, corr}
                               │                      ──▶ ntfy ──▶ 📱 [Approve][Deny][Details]
                               │ GET …?wait=110   (blocks)                  │
                               ◀── {"decision":"allow","by":"mrsabath"} ◀───┘
       {"permissionDecision":"allow","permissionDecisionReason":"mrsabath approved"}
  tool runs
```

The long-poll reuses the machinery `events.sse` already has: `store.subscribe(corr)` /
`unsubscribe` is a `threading.Event` fan-out (`eventbridge/store.py`), and the SSE
generator's shape — wait with a timeout, re-read, yield, repeat — is the same loop with a
single answer instead of a stream. No new concurrency primitive.

### 5.2 The hook

One script, in the baked agent directory, referenced by the spec's `settings_file`
(§2.2):

```json
{"hooks": {"PreToolUse": [{"matcher": "Bash|Write|WebFetch",
  "hooks": [{"type": "command",
             "command": "/etc/rossoctl/agents/triager/approve.py",
             "timeout": 120}]}]}}
```

It reads the hook payload on stdin, and:

1. If the tool is not in `ER_APPROVAL_TOOLS`, exit 0 — no decision, normal flow. The
   matcher is a coarse filter; the env var is the authority, so a spec cannot widen the
   guarded set by editing a matcher.
2. `POST /v0/approvals` with `correlationid`, tool name, the canonical tool input and its
   digest.
3. `GET …?wait=<ER_APPROVAL_TIMEOUT_S>` and block.
4. Print the `permissionDecision` JSON on stdout.

The ordering matters: **register before notifying, notify before waiting.** A notification
for an approval row that does not exist yet is a button that 404s, which is the same
ordering hazard Phase 1 §21 records for group membership ("membership before publication:
the reverse order leaves a window where a fast agent's terminal event arrives for a member
nobody has recorded").

### 5.3 What rides on the event

One new extension attribute and one new event type, following `shared/ce.py`'s existing
naming exactly (bare lower-case, no separators — the rule Phase 2 §2.6 records was
violated once by `submitter_iss` and is now pinned by `test_roundtrip_binary.py` over
every `EXT_*`):

```text
ce_approvalid: appr-0f3c9a2b           # NEW — which approval a notification refers to
TYPE_APPROVAL_REQUESTED = "dev.rossoctl.agent.approval.requested.v1"
```

`approvalid` **joins `signing.SIGNED_ATTRS`**, for the same reason Phase 3 §8.3 gives for
`agent`: it selects which pending decision an event refers to, so outside the signed set
anything with write access to a topic could repoint a notification at a different
approval and the signature would still verify. And per Phase 3 §8.3's rule, adding it breaks
canonicalisation, so **it lands in one change** with any other attribute added in the same
release — not three commits invalidating canonicalisation three times.

Like the group lifecycle events (Phase 1 §21), the approval event rides the **responses**
topic: EventRunner consumes requests and would try to execute anything there as an agent
run, while EventBridge already consumes responses and feeds the ntfy publisher from them.

### 5.4 The decision is bound to the tool input, not just the approval

This is the most important control in the phase, and it is the one Phase 3 §4.4 does not
have — because Phase 3 §4.4's subject is *continuing a conversation*, where the only thing to
bind is the correlation. Here the subject is *authorising an action*, so the action is
part of what is authorised.

Phase 3 §4.4 mints `HMAC(secret, f"{userkey}|{corr}|{exp}")`. An approval key extends the
input:

```python
# eventbridge/capability.py — stdlib hmac, nothing stored.
# Phase 3 §4.4's module, with one more mint.
def mint_approval(secret: bytes, approvalid: str, userkey: str,
                  input_sha256: str, ttl_s: int = 3600) -> str:
    exp = int(time.time()) + ttl_s
    mac = hmac.new(secret,
                   f"approve|{userkey}|{approvalid}|{input_sha256}|{exp}".encode(),
                   hashlib.sha256).digest()
    return f"{exp}.{base64.urlsafe_b64encode(mac[:16]).decode().rstrip('=')}"
```

Without `input_sha256` in the MAC, a key that approved `echo hi` could be presented
against a *rewritten* input on the same approval id. So the hook, before returning
`allow`, **re-computes the digest of the input it is about to permit and compares it to
the digest the verdict was recorded against.** A mismatch is a deny, logged as
`input-changed`. The human approved a specific call; anything else is a different call.

The `approve|` domain separator is there for the reason `tenancy.ntfy_topic`'s `b"ntfy|"`
is: without it, two derivations over the same secret for different purposes can produce
the same tag.

### 5.5 Decided once, enforced in SQL

A new table in the tenant's `sessions.sqlite`, following `store.py`'s convention exactly
— a `CREATE TABLE IF NOT EXISTS` block appended to the schema string with a
`-- Phase N §X:` rationale comment. There is no migration framework and this phase does
not add one.

```sql
-- Phase 4 §5.5: one human decision about one tool call. `decided_utc IS NULL` is the
-- pending state and the concurrency control both: the verdict is written with a guarded
-- UPDATE, so a double-tap on the phone cannot flip a decision already made.
CREATE TABLE IF NOT EXISTS approvals (
  approvalid    TEXT PRIMARY KEY,
  correlationid TEXT NOT NULL,
  userkey       TEXT NOT NULL,
  tool          TEXT NOT NULL,
  input_json    TEXT NOT NULL,
  input_sha256  TEXT NOT NULL,
  requested_utc TEXT NOT NULL,
  expires_utc   TEXT NOT NULL,
  decided_utc   TEXT,              -- NULL = pending
  decision      TEXT,              -- allow | deny | timeout | expired
  decided_by    TEXT               -- the userid, never the key
);
CREATE INDEX IF NOT EXISTS approvals_by_corr ON approvals(correlationid);
```

`UPDATE approvals SET … WHERE approvalid = ? AND decided_utc IS NULL`, accepting the
write only on `cur.rowcount == 1`. That is the once-only pattern `mark_member_finished`
and `complete_group` already use (`eventbridge/store.py`), and it is in SQL rather than in
memory for the reason those are: the bridge is single-replica today, and a guarantee that
depends on that is a guarantee that breaks quietly when it stops being true.

**The verdict is read from this row, never from the event or the request.** Phase 2 §8.5.4
is the rule, and group-member push suppression is its cautionary tale: a check that reads
an attribute off the event is defeated by omitting that attribute.

### 5.6 Status codes

Continuing Phase 2 §2.4's discipline, where the point is that collapsing two answers into
one is the easy and wrong thing to do:

| Status | When | Why not something else |
|---|---|---|
| `401` | no credential on `/v0/approvals` or `/v0/me/*` | retrying with a credential is the remedy |
| `403` | key invalid, expired, or wrong user | retrying is *not* the remedy |
| `404` | unknown approval id, **or one owned by another tenant** | Phase 3 §6.2's rule: a `403` would confirm another tenant's id exists |
| `409` | valid key, already decided | the answer is already in; returns the existing verdict rather than pretending to accept a second one |
| `410` | past `expires_utc` | distinct from `403`: the key was good, the window closed |

### 5.7 Timeout is a deny, and is recorded as a timeout

When the long-poll budget expires the hook returns `deny` with reason `no answer in
<N>s`, and the row records `timeout` — **not** `deny`. "Nobody was awake" and "a human
said no" are different operational facts, and collapsing them hides which one is
happening. A deployment where every approval times out needs a different fix from one
where every approval is refused, and the counter is how an operator tells them apart.

The budget ordering is a real constraint and belongs in the configuration table (§7):

```
ER_APPROVAL_TIMEOUT_S  <  the settings file's per-hook "timeout"  <  the turn's wall clock
```

Getting it backwards means the CLI kills the hook before it can answer, and a killed hook
is a *non-blocking error* — execution proceeds, which is to say **the tool runs
unapproved**. That is the failure direction this phase cannot have, so §7 pins the
inequality and a startup check prints all three values.

### 5.8 The notification carries what is being approved

```
🔐 approval needed · triager
Bash — needs your approval

$ gh pr merge 892 --squash

❓ triage the open PRs and merge anything green
👤 requested by mrsabath · expires in 60m
📎 appr-0f3c9a2b
```

A human approving a `Bash` call they cannot read is a rubber stamp, and a rubber stamp is
worse than no control at all because it manufactures an audit trail. So the body carries
the tool, the canonical input (truncated), the prompt that led here and the requester —
which is the same argument `ntfy.py`'s `compose_body` already makes for results:

> the notification body itself carries enough info that a phone user who cannot reach
> EventBridge's HTTP URL still sees the useful result at a glance

### 5.9 Three actions, and the existing notification already uses all three

**ntfy allows at most three action buttons per message** and caps the message at **4096
bytes** (`ntfy.py`'s `MSG_CAP = 3500` is safely under). `_publish` already emits exactly
three — *Open history*, *Raw CloudEvents*, *Continue…*.

So the obvious move — add an **Approve** button to the result notification — **silently
drops one of those three.** An approval is therefore its own notification shape, with its
own budget:

```
[Approve]  →  POST /v0/approvals/{id}/decide?k=…  {"decision":"allow"}
[Deny]     →  POST /v0/approvals/{id}/decide?k=…  {"decision":"deny"}
[Details]  →  view  /v0/approvals/{id}
```

Both `http` actions carry the capability key in the URL and `clear: true`, so the
notification dismisses when the request succeeds and stays if it fails — which is what
tells the user their tap did not land. The ntfy `http` action can also carry an
`Authorization` header, and deliberately does not here: Phase 2 §3.1's reasoning is
unchanged, a bearer token in an action comes to rest in four places outside our control.
The capability key is the thing designed to go there.

Priority 4 and a distinct tag, because an approval request is the one notification with a
deadline. Results are priority 3 and errors 5; an approval sits between, since a missed
approval costs the run but is not itself a failure.

---

## 6. The fallback, so a sleeping operator cannot wedge a pod

The blocking hook is the primary path and is not sufficient alone. A pod holding a Kafka
partition while a hook waits on a human is spending the `ER_DRAIN_TIMEOUT_S` /
`terminationGracePeriodSeconds` budget that `test_manifests.py` already pins, and KEDA
will not scale a pod away mid-run. An unbounded wait is therefore not available, however
much the control would like one.

So the sequence, which is why §5.7's timeout is a *deny* rather than an error:

1. **The hook denies on timeout.** The agent finishes its turn normally, reporting what it
   could not do. The partition is released on the usual path.
2. **The approval row outlives the run**, and the notification with it. `expires_utc` is
   the capability key's TTL, not the hook's budget — so a human who answers twenty minutes
   later is answering a live question.
3. **A late approval starts a new turn.** The notification's action becomes
   `POST /v0/agents/{corr}/continue?k=…` — the existing `/continue` path with Phase 3
   Phase 3 §4.4's capability key — and the recorded `allow` means the hook permits that one call
   without asking again, matched on `(correlationid, tool, input_sha256)`.

The cost is honest: **the agent redoes the work up to that point.** The grant is scoped to
one recorded input digest, so it is a one-shot replay permission and not a widened
allowlist. The trade is a bounded cost (one repeated turn) against an unbounded one (a pod
pinned on a human who went to bed), and this phase takes the bounded one.

---

## 7. Configuration

Every variable is off or inert by default, so a deployment that upgrades and changes
nothing behaves exactly as it did in Phase 3.

### 7.1 EventBridge

| Variable | Default | Effect |
|---|---|---|
| `EB_CAPABILITY_SECRET_PATH` | empty | HMAC seed for `?k=` keys (Phase 3 §4.4, extended by §5.4). **Empty disables approvals entirely** — there is no way to authorise a decision, so `/decide` answers `503`. |
| `EB_APPROVALS_ENABLED` | `false` | The whole of §5. Off means the endpoints 404. |
| `EB_APPROVAL_TTL_S` | `3600` | `expires_utc`, and the key's TTL. Independent of the hook's budget (§5.7). |
| `EB_NTFY_TOPIC_SECRET_PATH` | empty | Phase 3 §4.2. **Required** for §3 — without it there is no per-user topic to enroll onto. |
| `EB_ENROLL_ENABLED` | `false` | `/v0/me/ntfy/enroll`. Requires an authenticated caller, so it is inert when auth is off. |

### 7.2 EventRunner

| Variable | Default | Effect |
|---|---|---|
| `ER_APPROVAL_TOOLS` | empty | Comma-separated tool names that need approval. **Empty guards nothing** — the whole phase is inert. |
| `ER_APPROVAL_TIMEOUT_S` | `110` | The hook's long-poll budget. Must be **below** the settings file's per-hook `timeout` (§5.7). |
| `ER_APPROVAL_FAIL_OPEN` | `false` | What the hook does when EventBridge is unreachable. `false` = deny. §9 is why the default cannot be `true`. |

Secret-vs-ConfigMap, continuing Phase 2 §5's rule and Phase 3 §8.3's: both secret paths
name **Secret** mounts and are env-only, never `config.toml`. `test_manifests.py` already
pins this for `NTFY_TOPIC`/`NTFY_TOKEN` and should be extended to these, because the rule
is only worth having if a test enforces it.

---

## 8. What is deliberately left open

- **Approval delegation.** Alice cannot approve for Bob, and there is no on-call rotation.
  The decider is the correlation's owner, full stop. Delegation needs a model of who may
  speak for whom, which is a bigger idea than this phase.
- **Approval policy.** No "auto-approve this tool after N manual approvals", no rules
  engine. This is the most-requested feature and the one most likely to quietly undo the
  control: a policy that learns from approvals converges on approving everything, and the
  operator who added it cannot tell when that happened. If it is ever added, the counter
  in §5.7 is the thing to watch.
- **Approval of anything but a tool call.** Not a budget, not a model choice, not an
  egress. The hook point is `PreToolUse`, so the vocabulary is tools.
- **The hook runs in the agent's pod.** §9 states the residual hole this leaves.
- **Phase 3's own open items are unchanged**: Kafka is still plaintext (Phase 2 §3.3),
  the registry still records what an operator approved rather than what a platform
  attested, and SPIRE remains the upgrade path — with Phase 2 §8.3's correction that the
  verifier accepts EdDSA only, so that path is not as free as Phase 2 §4.3 claimed.

---

## 9. The Phase 2 §8.5 questions, answered before this ships

Phase 2 §8 exists because thirteen control claims in this repository did not survive being
run, and §8.5 turned that into five questions to ask before publishing a claim. Applied
here, to this document's own claims:

### 9.1 Which code path, by name?

The guarantee *"the tool does not run"* rests on the **CLI's** `PreToolUse` handling of
`permissionDecision`, measured in §2 — not on anything in this repository. EventBridge
only records a verdict; the enforcement is in the subprocess.

That is the weakest link and it deserves to be named as one: a CLI upgrade that changes
hook semantics weakens this control **silently**, with every test still passing, because
no test here can assert the CLI's behaviour. Phase 3 §5.5 already requires a test that
parses `claude --help` in the built image and fails a build on a vanished flag; this phase
extends it to assert the hook contract end to end against the pinned binary. That test is
the only thing standing between a version bump and a control that has stopped working.

### 9.2 What is the default?

Off, in three independent places: `EB_APPROVALS_ENABLED=false`, `ER_APPROVAL_TOOLS` empty,
and no baked agent referencing an approval settings file. With all three at their
defaults, `build_cmd`'s argv is byte-identical to Phase 3's.

### 9.3 What happens when it is half-configured?

Four cases, tabulated because Phase 2 §8.5.3's lesson is that the three that are not "both set"
are the ones a reader hits:

| `ER_APPROVAL_TOOLS` | EventBridge reachable | Outcome |
|---|---|---|
| empty | — | nothing guarded; Phase 3 behaviour exactly |
| set | yes | the round trip in §5 |
| set | **no** | **the hook denies**, and logs `bridge-unreachable` at every occurrence |
| set | yes, but `EB_APPROVALS_ENABLED=false` | `404` from `POST /v0/approvals` → the hook denies, logging `approvals-disabled` |

The third row is the one that matters, and it is deliberately the opposite of Phase 2
§8.1's worst finding — where `EB_REQUIRE_RESPONSE_SIGNATURE=true` with no keyset refused
*nothing*, silently, while the request side with no key refused *everything*. Two sides
failing in opposite directions, only one of them loud. Here both sides fail closed, and
both say so.

`ER_APPROVAL_FAIL_OPEN=true` exists for a development loop and is the one way to invert
that row. It prints a warning at startup naming itself, per Phase 3's precedent for
`ER_SKILL_REQUIRE_SIGNATURE=false`.

### 9.4 Can an attacker choose the input the check reads?

Partly, and this is the sharpest question for this phase.

The hook receives `tool_input` from the agent, and the agent is running
attacker-influenced text (Phase 3 §7.4). Two consequences, handled differently:

- **The input could change between the ask and the allow.** Handled: the digest is in the
  MAC (§5.4) and re-checked before the hook returns `allow`.
- **The hook itself runs inside the pod the agent runs in.** *Not* fully handled. An agent
  that can write the settings file or the hook script disables its own guard. The
  mitigation is Phase 3 §5.2's existing rule — the agent directory is readable by gid 0
  and never writable by the agent's UID — and it is a filesystem permission, not a
  cryptographic control. An agent with an unexpected write primitive defeats it.

Stated plainly because Phase 2 §8.4 diagnosed exactly this pattern: each of the thirteen
claims "was written from the change that introduced it, and stayed true only for the
configuration that change was exercised in". The configuration this one is false in is
one where the agent's tool policy permits writing to `/etc/rossoctl/agents`.

### 9.5 What does the claim become one release from now?

The §6 fallback exists *only* because a hook cannot block indefinitely. If EventRunner is
ever rebuilt on the Agent SDK, `canUseTool` (§2.1) replaces the hook, the timeout stops
being structural, and §6 becomes a compatibility path rather than a necessity.

```markdown
<!-- VERIFY: if EventRunner moves to the Agent SDK, §2.1's constraint lifts and §6's
     "timeout is a deny" is no longer forced. Rewrite §6 as an option, not a
     requirement — and keep §5.7's timeout/deny distinction, which survives either way. -->
```

Per Phase 2 §8.5.5, that says *what should change*, not merely that something will — and the
second clause matters, because the part of §6 that survives the fix is the part a
maintainer acting on a vague comment would delete.

---

## 10. Verification

What has been run, and what has not. §2's table is the measured part; everything else in
this document is design.

**Measured** — against `claude 2.1.270`, `--output-format stream-json --verbose
--max-turns 2 --settings <file>`:

| Check | Result |
|---|---|
| 75 s blocking `PreToolUse` hook, `permissionDecision: deny` | tool never ran; 93 s wall clock; reason reached the agent |
| 5 s hook, `permissionDecision: allow`, `--permission-mode acceptEdits` | tool ran; `echo HELLO_FROM_TOOL` → `HELLO_FROM_TOOL` |
| the same `allow` hook under `--permission-mode plan` | guarded tool never attempted (§2.3) |
| `--permission-prompt-tool` | not a flag on this CLI (§2.1) |

**Not verified, and not claimed:**

- **Nothing here has run on a cluster.** This document was written without cluster access,
  so every statement about cluster behaviour is inherited from `IMPLEMENTATION_REPORT1.md`
  and Phase 3's design rather than observed. The same caveat `KUBECON_NA_2026.md` §7
  records, for the same reason.
- **The CLI version is not the pinned one.** `Dockerfile-eventrunner-claude` pins
  `CLAUDE_VERSION=2.1.278`; §2 was measured on 2.1.270. The contract is expected to hold
  across that gap and **has not been confirmed on 2.1.278** — which is precisely what
  §9.1's test is for, and the first thing to run when implementing.
- **No implementation exists.** `eventbridge/capability.py` does not exist in the tree;
  Phase 3 §4.4 designs it and this phase extends that design. `NtfyPublisher` is still
  single-topic, so §3 depends on Phase 3 §4.2's `_topic_for` landing first.

**Claim hygiene applied to this document**, per #892's findings: every `§N.N` reference
resolves to a heading that exists here or in the Phase 2/3 document it names; every count
reconciles with its enumeration (§5.9's three actions, §9.3's four cases, §6's three
steps); and the ntfy limits in §5.9 are cited from ntfy's publish documentation rather
than inferred from `ntfy.py`'s constants.

---

## 11. Implementation tasks

Ordered by dependency — the first three are prerequisites owned by Phase 3, and nothing
below them is safe to start first.

| # | Task | Depends on |
|---|---|---|
| T1 | `eventbridge/capability.py` — Phase 3 §4.4's `mint`/`verify`, plus §5.4's `mint_approval` | — |
| T2 | `NtfyPublisher._topic_for` — Phase 3 §4.2's per-user topic, and the unattributed-event counter | Phase 3 §4.2 |
| T3 | `k8s/ntfy/` + the reader-account provisioning in `k8s_tenant.py` | Phase 3 §4.3 |
| T4 | `approvals` table and store methods, with the guarded-`UPDATE` once-only test | — |
| T5 | `/v0/approvals` endpoints + the HTML card, and the `openapi.py` entries (hand-maintained; `test_openapi.py` pins the required paths) | T1, T4 |
| T6 | `TYPE_APPROVAL_REQUESTED`, `EXT_APPROVALID`, and `SIGNED_ATTRS` += `approvalid` — **one change** (§5.3) | T4 |
| T7 | The approval notification shape in `ntfy.py` (§5.8, §5.9) | T1, T2, T4 |
| T8 | `approve.py` hook + the settings file, baked into an agent dir | T5 |
| T9 | `POST /v0/me/ntfy/enroll`, `GET /v0/me`, and the CLI `login` / `ntfy enroll` subcommands | T2, T3 |
| T10 | The pinned-CLI hook-contract test of §9.1 — **the one test this phase's guarantee rests on** | T8 |

The tests to imitate, since this repository's conventions are specific: `test_groups.py`
for the full shape (wire → store idempotency → pure function → HTML → ntfy → HTTP, with a
`FakeProducer` duck type and handlers called directly on a hand-built `environ`),
`test_store.py` for the table, `test_ntfy_body.py` for the notification body, and
`test_roundtrip_binary.py` for the new attribute's lower-case compliance.

---

## 12. Reading order for whoever picks this up

- **Implementing it?** §2 first — the measured mechanism is what the whole phase rests on,
  and §2.1 rules out the design most people reach for. Then §11 in order.
- **Reviewing the security?** §5.4 (the input digest), §5.5 (once-only in SQL), then §9.4
  for the hole that remains.
- **Wiring the phone?** §3.2, then Phase 3 §4.3 for the in-cluster server and its caveats
  — especially that on kind a phone cannot reach the Ingress at all.
- **Presenting it?** §3.5 and §9 — what enrollment does *not* prove, and the one control
  whose enforcement lives outside this repository. Phase 2 §7's rule applies: a security
  demo that oversells its guarantee is worse than one that does not exist.
