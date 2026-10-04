# CLAIMS — checking a control claim before it ships

A procedure for any statement in these documents, or in the user-facing docs, about
what a security control does. It exists because thirteen such claims in
[`DESIGN_PHASE2.md`](DESIGN_PHASE2.md) turned out not to hold when a reviewer ran
them — see its §8 for the list, and
[rossoctl/rossoctl#2609](https://github.com/rossoctl/rossoctl/pull/2609) for the
review.

None of those thirteen was a careless statement about the mechanism. Each was written
from the change that introduced the control, and was true for the configuration that
change was exercised in. What broke them was a half-configured deployment, or a later
change that moved the code the claim depended on.

## The five questions

Ask these of every sentence that says a control does something. A claim that cannot
answer all five is not ready to publish.

### 1. Which code path, by name?

Name the function or module the claim rests on, and read it. Not the change that
introduced it — the code as it stands.

> "Group events are pinned to EventBridge's kid" rested on `verify_with_keyset`'s
> `expect_kid`, which does refuse a wrong kid. The claim was about **the batch not
> ending**, and that depends on `kafka_in.py` and `group_service.py`, which no one
> re-read. The verdict was correct and ignored.

A claim about an *outcome* has to follow the path to that outcome, not stop at the
check.

### 2. What is the default?

State it. Most controls here default to off, and a claim that reads as a description
of the system is a claim about a configuration almost nobody runs.

### 3. What happens when it is half-configured?

Every control here has more than one variable. For each combination where one is
missing, does the system fail **closed**, fail **open**, or refuse to start?

This is where the worst finding came from. `EB_REQUIRE_RESPONSE_SIGNATURE=true` with
no keyset refuses nothing, silently, while the equivalent on the request side refuses
everything. Both were documented as "refusal is on"; only one was true.

Tabulate it. A control with two variables has four cases, and the three that are not
"both set" are the ones a reader will hit.

### 4. Can an attacker choose the input the check reads?

If the decision reads an attribute off the event, the forger controls that attribute
— including by leaving it out.

> Group-member push suppression reads the event's own `ce_groupid`. A forged frame
> that omits it is not a group member as far as that check is concerned, so it is
> pushed.

Prefer deciding on state the attacker does not supply: group membership from the
store, not the groupid on the frame.

### 5. What does the claim become one release from now?

If the claim depends on an open bug, it expires when the bug is fixed. Add a comment
naming the release and the issue, per the docs contributor guide's accuracy rule 2:

```markdown
<!-- VERIFY v0.9.0: drop "nothing records the failure" once rossoctl/examples#886 lands. -->
```

Say what should change, not just that something will. A maintainer acting on a vague
comment deletes the wrong sentence — and check whether the claim is *still partly
true* afterwards. "Anything that can write to the responses topic can end a batch"
survives the fix for #885 whenever enforcement is off, which is the default.

## Run it

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

## When a claim does not survive

Say what the code does, and keep the reasoning. These documents are the record of what
was intended; a correction that deletes the intent loses the more useful half.

The pattern that worked: leave the design section as written, add a findings section
that says what the code does, and link the issue. `DESIGN_PHASE2.md` §8 is that shape.

And file the issue. Most of the thirteen were code bugs rather than wording mistakes,
and they are tracked as [#885](https://github.com/rossoctl/examples/issues/885),
[#886](https://github.com/rossoctl/examples/issues/886),
[#887](https://github.com/rossoctl/examples/issues/887) and
[#888](https://github.com/rossoctl/examples/issues/888).
[#889](https://github.com/rossoctl/examples/issues/889), a pidfile crash-loop, came
out of the same review without being a claim in any document. Documenting a bug
honestly is not the same as accepting it.

## One note on review

Most of the late rounds on #2609 fixed wording that a *previous round had suggested*.
Both the author and the reviewer were writing sentences from the code's docstrings
without running them, and both were wrong at about the same rate.

Two habits came out of it:

- **Change only the clause that was challenged.** Three separate rounds were spent
  re-fixing something a previous fix had over-corrected, including one that deleted a
  sentence which was itself the fix for an earlier round.
- **A suggested wording is a hypothesis.** Run it before adopting it, including your
  reviewer's — especially when it reads as though it came from a docstring.
