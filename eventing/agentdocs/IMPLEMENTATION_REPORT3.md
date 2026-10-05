# IMPLEMENTATION REPORT — Phase 3 (partial: §9 steps 1–4, plus T5)

Status: **in progress.** This report covers what landed on
`feat/eventing-phase3-tenancy-foundation` (PR #883): §9's rollout steps 1–4, plus T5, T13
and T22. It will be extended as later steps land rather than rewritten.

Written to the same contract as `IMPLEMENTATION_REPORT1.md`: measured figures rather than
estimates, failures with their root causes, and an explicit list of what is **not**
verified — which for this phase is longer and more useful than the list of what is.

One framing note, because it shapes everything below. Phase 3 is a 12-step, 22-task phase
and this is the first slice of it. §9 singles out steps 1–4 as safe to merge while `multi`
has never been switched on anywhere: *"the first four steps are refactors with tests, and
everything risky is behind a flag that defaults off."* That property held, and it is the
reason the largest diff in the phase is also the least risky. **It did not hold on the
first attempt**, and §4.1 is about why.

---

## 1. What was built

| Task | Module | Lines |
|---|---|---|
| T1 | `shared/tenancy.py` — `userkey()`, `TopicSet`, `ntfy_topic()`, `is_valid_userkey()` | 292 |
| T2 | `TopicSet` threaded through `kafka_out`, `kafka_in`, both mirrors, `eventrunner/consume` | — |
| T3 | `eventbridge/registry.py` — the approved-user registry; `auth.Caller` | 221 |
| T4 | `SIGNED_ATTRS` += `userkey`, `depth`, `agent` | — |
| T5 | `eventbridge/store_registry.py` — per-user stores, LRU | 262 |
| T5 | `eventbridge/owner_index.py` — the global `correlationid → userkey` index | 276 |
| T13 | `eventrunner/agentspec.py` — declarative agent definitions | 290 |
| T22 | `test_manifests.py` — the Secret-vs-ConfigMap rule, both directions | — |

**As of `3278d75`, the commit before this report:** ten commits, 40 files,
**+5933/−173**. Counted from a named point rather than "currently", because a report that
counts the branch it is committed on can never be right about its own diff — at
`c25a0c77` the figures are 11 commits, 41 files, +6298/−173, the difference being this
file and its index entry. A reader checking either number should get the one the text
claims.

New configuration: `EB_TENANCY_MODE` (default
`single`), `EB_TOPIC_PREFIX`, `EB_USER_REGISTRY_PATH`, `EB_MAX_OPEN_STORES`, `ER_USERKEY`,
`ER_AGENT_DIR`, `ER_AGENT_NAME`.

### The additive guarantee, and how it was checked

`EB_TENANCY_MODE=single` is the default and reproduces Phase 2. The check is that the
**525 test functions that existed before this branch pass untouched** — three test *fakes*
were widened to match a producer signature, and no assertion was weakened or removed.

Two specific claims were pinned rather than asserted in prose:

- `test_default_spec_reproduces_phase2_argv_exactly` compares against a **literal
  transcription** of the pre-Phase-3 `build_cmd`, not a call into the current one. A test
  that calls the implementation to decide what the implementation should produce follows a
  regression rather than catching it.
- Single-tenant mode keeps its SQLite files in the bridge root, not under `shared/`, so an
  upgraded deployment's sessions and transcripts stay where the UI looks for them. §6.1's
  diagram says `shared/`; the code deliberately differs and §6.1 now records why.

---

## 2. Test results

### Local unit tests

```
855 passed, 5 skipped in 56s     (Python 3.14.2 free-threaded, macOS arm64)
860 collected                     (the 5 skips are the difference)
```

**525 → 757 unique test functions** (+232); 860 collected with parametrisation. All five
skips are `test_manifests.py`'s `needs_kubectl` tests, reported as "no reachable cluster"
— disclosed, and §5 is what they would have covered.

One skip that did *not* fire locally is worth naming because it fires in CI:
`test_agentspec.py`'s `skipif(shutil.which("claude") is None)`. That is the test pinning
§5.5's CLI flag set, and the runner has no `claude`, so **the assertion that a flag
vanishing in a CLI upgrade fails a build does not actually run in CI** — it ran here.

`ruff check` and `ruff format --check` clean against the pinned 0.11.4.

### What was NOT run

No cluster. No two-tenant deployment. No Kafka. Every result above is from unit tests and
in-process fakes. §5 is the full list; stating it here because the table above otherwise
reads like more assurance than it is.

### Measurements against the design's assumptions

§6.1 estimates the per-tenant file-descriptor cost and derives a ceiling from it. Measured:

| Quantity | §6.1's estimate | Measured |
|---|---|---|
| Descriptors per open tenant store | "two connections … main plus `-wal` and `-shm`" | **6.0** (10 tenants, 60 fds) |
| Concurrently-open tenants under the soft limit | "around 150–200" | **~680** at `RLIMIT_NOFILE=4096` |

The design's arithmetic was right per connection and its *conclusion* was anchored to a
1024 soft limit. The measurement is on a host with 4096, so the two numbers do not
disagree — 1024/6 ≈ 170, which is squarely inside "150–200". Recorded because the ceiling
is deployment-specific and `EB_MAX_OPEN_STORES=64` is well under either.

Ownership index, 5,000 rows, same host:

| Operation | Measured |
|---|---|
| `claim()` | 27.4 µs |
| `exists()` | 1.7 µs |
| `seed_from()`, 10,000 ids | 270 ms |

That last figure settles the question §2.6 raises. §2.6 rejects seeding on the grounds
that reading `all_correlations()` from N stores is "a measurable startup delay" — true for
*per-start* seeding at 100 tenants, and the reason the index exists. But the **one-time**
backfill it also appeared to rule out costs 270 ms for the 10,000-id cap Phase 2 already
applied, once, on the first start after an upgrade. §4.1 is why that distinction was not
academic.

---

## 3. Deltas from the design, recorded as decisions

Not drift. Each of these is a place the implementation chose differently and the design has
been corrected to match (§9's "the code is the authority where implemented" rule).

1. **Single-tenant stores live in the bridge root, not `shared/`.** Upgrade safety: a
   Phase 2 deployment already has `responses.sqlite` and `sessions.sqlite` in
   `{tmpdir}/eventbridge/`, and relocating them makes every existing session vanish from
   the UI. `shared/` is used only in multi-tenant mode.
2. **The LRU evicts by dropping a reference; it does not close.** §6.1 said "closes on an
   LRU" and that "closing is safe". The premise was true and the conclusion false — §4.1.
3. **Deletion tombstones; an id is never reissued.** §6.5 implied `forget()` frees the id.
   It cannot: `sessionuuid` is unsalted, so a reissued `correlationid` derives the same
   session uuid and a leftover transcript becomes resumable by a different user's run.
4. **`events`/`dead` have single-mode names** (`{prefix}-events`, `{prefix}-dead`). §3.1
   defined only the per-user forms; neither topic has a Phase 2 predecessor to stay
   compatible with, so deriving them from the prefix is free.
5. **`agent` joins `SIGNED_ATTRS`.** §2.6 named only `userkey`, and §8.3 only `userkey` and
   `depth`. `ce_agent` selects the `AgentSpec` that supplies the tool policy — §5.1's
   "sandbox" — so leaving it unsigned made a signed deployment *worse* than an unsigned one.
6. **`tenancy.py` exports a validator.** §2.3 defines the key's shape in one place and gave
   consumers no way to check it, which is what made §4.5 possible.
7. **`EB_TOPIC_PREFIX` is validated** `[a-zA-Z0-9-]{1,32}`. It is the one component of a
   topic name `_slug` never sees, so `.` or `_` there reproduces exactly the JMX
   metric-name collision §3.1 warns about.

---

## 4. Findings

Ordered by what they cost to find, not by severity. Every one was reproduced by executing
the code before being fixed; the commit is named so the diff is reviewable alongside.

### 4.1 The `single`-mode regression that shipped behind a claim it could not happen

**Found by:** review (#883). **Fixed in:** `6ebdd80`.

Dropping Phase 2's `Minter` seeding loop left nothing to populate its replacement. On the
first start after an upgrade, `owners.sqlite` is new and empty while `sessions.sqlite`
holds every prior correlation, so `exists()` answers "free" for ids that are in use.
Reproduced directly:

```
sessions in store: 50
index count on first start after upgrade: 0
live ids the index reports as FREE: 50 of 50
```

The id space is 50 adjectives × 50 animals × 10,000 = 25,000,000, so a deployment with
~1,000 live correlations has roughly a **1-in-25,000 chance per mint** of reissuing one.
When it hits, `upsert_session` overwrites that session and `insert_prompt` appends the new
prompt to the old conversation. Phase 2's chance was zero.

Three things make this the most instructive finding in the phase:

- **It is a regression in the DEFAULT configuration**, which is the one thing §9's
  merge-safety property promises cannot happen — and the PR claiming that property was the
  PR that broke it.
- **The suite could not see it.** No test covered an empty index with a populated store,
  because every fixture built the two together. The state that causes it is only reachable
  by *upgrading*, which no test simulated.
- **The design contributed.** §2.6 argues convincingly that one `SELECT` per mint beats
  re-seeding a `seen` set on every start, and in arguing that it reads as though the index
  needs no seeding at all. Those are different claims. The distinction — *per-start
  seeding* rejected, *one-time backfill* required — is now explicit in §2.6 and §6.1a.

### 4.2 Eviction closed stores that callers still held

**Found by:** self-review before merge. **Fixed in:** `49aed05`.

`StoreRegistry` eviction called `Store.close()`. A handler resolves a store and then makes
several calls on it — `get_html` makes five — so a concurrent request for a *different*
tenant could push the cache over `EB_MAX_OPEN_STORES` and close it mid-use. Reproduced with
`max_open=1` and two tenants:

```
sqlite3.ProgrammingError: Cannot operate on a closed database.
```

Every multi-call read path was exposed. Eviction now drops the registry's reference and
lets the interpreter close at the last holder, which is exactly the lifetime that is safe.

**The lesson is the first fix, not the second.** The first attempt pinned only the SSE
path, because that was the case easiest to see — a generator holding a store across a
two-minute stream. It closed one instance and left the class open. The bug was in the
**eviction contract**, not in one caller. A fix aimed at the symptom you can picture is a
fix that will be re-found later.

Secondary: `protect=` was then needed because a cache whose older entries are all pinned
evicted the newest — and handed the caller the store it had just closed.

### 4.3 `except Exception` hid three different programming errors

**Found by:** three separate occasions. **Fixed in:** `77807b6`, `3278d75`.

The same mistake three times, which is why it is one finding:

1. Adding `userkey=` to `publish_group_event` made every group event stop publishing. The
   broad handler around the call turned a `TypeError` signature mismatch into a log line,
   and the only visible symptom was a batch that never completed.
2. `Minter.mint` caught `Exception` around `claim` to treat a lost race as a retry, so a
   genuinely broken index (disk full, locked database) was retried 2,000 times and reported
   as `RuntimeError("correlation ID space exhausted")` — a message that sends the reader to
   the word lists instead of the database.
3. The same group-publish handler swallowed `ValueError` from `TopicSet`, so a caller
   forgetting a `userkey` also produced a silently never-announcing group. Found by the new
   integration test (§4.6) while mutating an unrelated fix.

All three now re-raise the programming error and still handle a genuine broker failure. The
pattern worth extracting: **a broad `except` on a publish path converts "this code is
wrong" into "the broker had a bad moment", and the two need opposite responses.**

### 4.4 `GroupMirror` never learned about tenancy

**Found by:** self-review. **Fixed in:** `77807b6`.

Two bugs, one cause — the mirror knew the per-user *topic* names and not which tenant a
replayed group belonged to:

- `_settle` called `maybe_complete(gid)` with no `userkey`, so it looked in the shared
  store for a group living in a tenant's store, found nothing, returned `False`. Every
  group a restart left unfinished stayed unfinished **forever**, and the failure mode is
  silence — which `DESIGN_PHASE1.md` §21.6 calls the worst one.
- Replayed events published *before* Phase 3 carry no `ce_userkey`, so member rows went to
  `shared/` while the group row sat in the tenant's store, leaving the counts permanently
  wrong. Pre-existing Kafka history is precisely what the mirror replays, so this is the
  **normal upgrade path**, not an edge case.

The mirror now resolves the owner from the index and back-fills a missing `userkey` — only
ever *filling*, never overriding, since `userkey` is signed and the index is not
authoritative over a signed assertion.

### 4.5 A `userkey` reached a filesystem path join unvalidated

**Found by:** review (#883). **Fixed in:** `6ebdd80`.

In multi mode the key arrives from an inbound Kafka header, and `Store.__init__` calls
`mkdir(parents=True)` — so it is created, not rejected:

```
'gh-alice-a1b2c3d4'            -> …/users/gh-alice-a1b2c3d4
'../users/gh-victim-0bad0bad'  -> …/users/gh-victim-0bad0bad   # another tenant
'../../../../tmp/pwned'        -> /tmp/pwned                    # outside the tree
'..'                           -> …/eventbridge                 # the bridge root
```

Reachable because `EB_REQUIRE_RESPONSE_SIGNATURE` defaults to `false` and §3.6 already
concedes that in Tier A anything reaching the shared broker can write to any topic — an
accepted property for *reading* events, which must not also be a filesystem write
primitive.

Two details worth keeping:

- **An invalid key is truthy**, so it sailed past the existing `if not userkey` branch
  straight into the join. The guard that looked like it covered this did not.
- `agentspec.py` got the same problem right 150 lines away — `_resolve_under` refuses a
  path that escapes the agent directory and reasons explicitly about `../../../etc/shadow`.
  The gate was present on operator-supplied input and missing on the one input that is
  genuinely attacker-supplied.

An invalid key is now treated exactly like a missing one — `shared/` plus the
`unattributed` counter — so a forgery is *counted and visible* rather than rejected
silently.

### 4.6 The suite's structural blind spot

**Found by:** review (#883). **Fixed in:** `3278d75`.

Two of the defects above survived 200-odd new test functions, for one reason: every multi-mode
test either called `GroupService` directly **with** the `userkey` the handler fails to
pass, or replaced `Producer` with a mock and asserted on its keyword arguments. Nothing
asserted a **topic string**, which is the thing that decides whose runner executes a turn.

`tests/test_topic_routing_e2e.py` fakes one layer lower — at `KafkaProducer`, so the real
handler, the real `Producer` and the real `TopicSet` all run. Confirmed by reverting each
fix that it catches both. It then found §4.3's third instance unprompted.

Generalisable: **a fake placed at the boundary you are testing cannot test that
boundary.** The fakes were at `Producer`, and the bug was in what the handler passed to
`Producer`.

### 4.7 A fix that introduced a new bug class: validation after the writes

**Found by:** review, round 2. **Fixed in:** this round.

Round 1 added validation of the request's `agent` name at the HTTP boundary — the right
place, for the right reason — and placed it **after** the writes in both submit paths. So
a `400` left committed state behind:

```
POST /v0/groups {"prompts":["a","b"],"agent":"../../etc"}  Idempotency-Key: retry-me
  -> 400 Bad Request
  topics published: ['kev1-st-alice-c8ff0431-responses']      # group.started went out
  groups in alice's store: ['wild-weasel-1995']               # 0 members, forever
  retry, same key, valid name -> 200 {created: false, members: []}

POST /v0/agents {"prompt":"hi","agent":"Not A Label"}
  -> 400 Bad Request
  sessions committed: ['fancy-puma-7571']                     # a turn never submitted
  ownership index count: 1                                    # an id nothing un-claims
```

The group case is the worse of the two and the more interesting: the state is permanent
*because the Idempotency-Key worked*. Every retry reads back the member-less row, which
is verbatim the failure the `userkey=` comment added in the same round describes — reached
through a route that fix did not cover.

Three things make this worth its own section rather than a line in §4.9:

- **It is the same orphan-write shape as §4.9's `/continue` 500**, which round 1 fixed.
  Fixing an instance did not teach me to look for the class — the same lesson §4.2 records
  about the eviction contract, learned again.
- **The test for the new validation asserted the status and that nothing was published**,
  both of which held. The assertion that would have caught it —
  `get_prompts(corr) == []`, *a refusal writes nothing* — already existed 25 lines
  earlier in the same file, on a different route.
- **`test_topic_routing_e2e.py` did not catch it either**, because it asserts topics and
  the `start_agent` case publishes nothing. A test aimed at one invariant does not
  incidentally cover another.

Both resolutions now happen before the first write; `_agent_for` needs only `caller` and
`body`, so there was never anything to gain by deferring it.

### 4.8 Three places the design still disagreed with the code

**Found by:** review, round 2. **Fixed in:** this round. Grouped because the cause is one
thing: §3 of this report claimed the design/code gap was closed, and checking it found
three items it had missed.

- **The backfill was per-start, not one-time.** §6.1a says "seeded once" and §2 of this
  report said "once, on the first start after an upgrade"; the loop re-opened every tenant
  store and re-read `all_correlations` on every boot. `seed_from` is `INSERT OR IGNORE` so
  correctness held — what repeated was exactly the cost §2.6's argument is about. Measured
  at 100 tenants × 200 correlations: **210 ms per restart, 101 store opens**, and at
  §2.6's own worked example (100 × 10,000) it extrapolates to ~6 s on every boot. A
  `seed_state` table now records each scope, so an already-seeded store is never opened:
  **0.8 ms and 1 store open** per restart. The claim is now true.
- **`AgentSpec.limits` was parsed and enforced nowhere, silently.** `Skill` states plainly
  that it carries `sha256` for T19; `Limits` made no such disclaimer, so `timeout_s = 900`
  loaded clean and did nothing. Now says so. Its three conversions also escaped the
  module's `SpecError` contract — `timeout_s = "900s"` raised a bare `ValueError` past
  `run_agent`'s handler instead of becoming the `phase=error` event §5.1 promises.
- **§8.3 was not updated with §2.6.** It still said the signed set "changes twice" and
  named two attributes while §2.6, `signing.py` and `ce.py` all said three — and
  `signing.py` cited §8.3 as the authority for a rule §8.3 stated over the wrong count.

Two smaller ones from the same pass: a tombstoned correlation could be re-claimed by the
NULL owner (`row[0] == userkey` reads as "already mine" when both are `NULL`),
contradicting `forget`'s guarantee — unreachable from `Minter.mint`, which checks
`exists()` first, but reachable as soon as a `correlationid` arrives from outside. And
`registry.parse` accepted `agent` unchecked, the only field it did not validate, in a
function whose docstring promises every inconsistency is a refusal.

### 4.9 Smaller findings

- **`/continue` 500 on an unresolvable owner** (`7590ec8`), and again on a *known but
  unowned* correlation (`6ebdd80`). The second taught something: reading and publishing
  need different answers. Shared-tier data must stay readable while a turn for it is
  unpublishable, so `_store_of` and `_publishable_owner` are separate.
- **Unauthenticated cross-tenant group mutation** (`6ebdd80`). `close_group`/`cancel_group`
  called no auth and resolved any tenant's group from the global index. "T7 is not in yet"
  does not cover it: T7 is about reads *staying* as open as Phase 2's, and Phase 2 had no
  cross-tenant mutation to preserve.
- **A test that could not fail for its stated reason** (`3278d75`). T22's classification
  check asserted `CONFIGMAP_VARS ⊆ found`, catching a stale entry; the direction that
  catches a *new* unclassified variable is the reverse. Now verified by deliberate failure.
- **A test flaky by construction** (`6ebdd80`). `"gh" not in` 26 characters of base32 fails
  ~2.4% of the time — base32's alphabet contains `g` and `h`. It also tested nothing: the
  `"mrsabath" not in` assertion above it is the property §4.1 cares about.
- **`max_turns: 0` silently rewritten** (`6ebdd80`), because `payload.get(...) or
  spec.max_turns` treats `0` as absent while the spec path *rejects* zero. Two paths
  disagreeing about the same nonsensical input.

---

## 5. Not verified, and honest limits

- **`EB_TENANCY_MODE=multi` does not work end to end.** Until T6, neither consumer
  subscribes to the per-user topics, so **no response is ever consumed**: transcript pages
  stay empty, group counters never advance, nothing is logged. `multi` is for developing
  against, not running. This is a more useful warning than "not yet isolated", which
  implies a working-but-unisolated mode.
- **No cluster run, and no two-tenant deployment.** Everything measured here is unit tests
  and in-process fakes on one laptop. `scripts/k8s_tenant.py` (T12) does not exist, so
  there is no way to provision a second tenant's topics, Deployment or ScaledObject.
- **Reads are not owner-scoped.** Per-user stores exist and a read routes to the owning
  tenant's store, but nothing checks the *caller* is that owner (T7). `PUT /transcript` is
  still unauthenticated (T8) — §6's opening paragraph is explicit that this must land
  before a second tenant exists, since an overwritable transcript injects resume state into
  another user's next turn.
- **Tier A is not isolation**, and no part of this changes that. Separately-named topics
  without Kafka ACLs are organisation; §3.6's dedicated-broker option (T20) is what earns
  the word.
- **The per-tenant throughput ceiling is still unmeasured**, as §10 asks. EventBridge is
  single-replica and now holds N stores, the ownership index and the Ed25519 signing. The
  figures in §2 are per-operation costs, not a throughput model.
- **The `claude --help` flag assertion does not run in CI** (no `claude` on the runner), so
  §5.5's promise that a flag vanishing in a CLI upgrade fails a build is currently a
  promise about a local run.
- **`seed_from` is bounded at 10,000 ids per store**, matching Phase 2's cap. A store with
  more than that keeps the §4.1 hazard for the remainder. The cap should become a config
  value, or the seed should page.
- **`is_valid_userkey` is shape validation, not authorisation.** It stops a path escape; it
  does not establish that the named tenant exists or that the sender may act for it. With
  signature verification off — the default — a forged-but-well-formed key still files events
  into `shared/`.
- **No measurement of the LRU under real contention.** The pinning rule is unit-tested;
  what has not been observed is a cache thrashing at `max_open` with live SSE viewers.

---

## 6. Next actions

1. **T6** — `ensure_subscribed`, which is what makes `multi` function at all. The race must
   be written as a genuinely failing test first: publish before subscribe must fail.
2. **T7 + T8** — owner-scoped reads and transcript auth, in that order, and both before a
   second tenant exists anywhere.
3. **T12** — `scripts/k8s_tenant.py`, which is the prerequisite for any of the above being
   verified on a cluster rather than in fakes.
4. **Measure the per-tenant ceiling** (§10) before this is deployed for more than a handful
   of users. It is the one open item that could invalidate a design decision rather than
   just leave a gap.
