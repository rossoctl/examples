"""Read a topic as an audit trail: what is signed, and what each result answers.

R4 of the KubeCon demo list — "the event broker log as tamper-evident audit". The claim
the abstract makes is that a result event can be chained back to the signed wake event
that caused it, and this is the module that can show it rather than assert it.

**Pure, and deliberately not a Kafka client.** It takes already-decoded CloudEvents and
returns a verdict per event plus the chain between them, so the whole thing is testable
without a broker — which matters because every one of `DESIGN_PHASE2.md` §8's thirteen
findings coexisted with a passing suite that exercised the configuration each claim was
true in. The Kafka half lives in `scripts/audit_topic.py`.

**What "tamper-evident" does and does not mean here.** Three limits, all of which the
output states rather than implies:

  * **Signing is terminal-only** (`emit()`, §4.4). A streamed `stdout` frame carries no
    signature by policy, because Ed25519 costs ~222 ms here and signing every frame
    would add minutes to a chatty run. So an unsigned non-terminal is not a finding, and
    the summary counts it separately from one that should have been signed and was not.
  * **A signature proves the signer held an approved key, not that the content is true.**
    It binds `causationid` — which is in `SIGNED_ATTRS` — so the *link* between a result
    and its wake event cannot be rewritten in flight. What the agent said is a separate
    question from who finished the run.
  * **Pre-signing history is indistinguishable from a forgery.** A topic written before
    signing was enabled is entirely unsigned, and nothing in a signature can tell those
    apart from events an attacker wrote. The verdict for both is `unsigned`, and that is
    the honest answer rather than a gap.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from shared import ce, signing

# A result event is bound to its wake event by `causationid == request.id`, and both
# attributes are covered by the signature (`SIGNED_ATTRS`), which is what makes the chain
# worth reading at all: an unsigned chain is a suggestion.
VERDICT_OK = "signed-ok"
VERDICT_BAD = "signed-BAD"
VERDICT_UNSIGNED = "unsigned"
VERDICT_EXEMPT = "unsigned-ok"   # non-terminal, which signing policy does not cover
VERDICT_UNKNOWN = "no-keyset"    # nothing to check against


@dataclass
class Row:
    """One event's audit line."""
    offset: int
    event_id: str
    type: str
    correlationid: str
    groupid: str
    sequence: str
    phase: str
    final: bool
    kid: str | None
    verdict: str
    reason: str
    causationid: str

    @property
    def is_terminal(self) -> bool:
        return self.final

    @property
    def signed(self) -> bool:
        return self.verdict in (VERDICT_OK, VERDICT_BAD)


@dataclass
class Chain:
    """A wake event and the results that name it."""
    request_id: str
    correlationid: str
    request_verdict: str
    results: list[Row] = field(default_factory=list)

    @property
    def terminals(self) -> list[Row]:
        return [r for r in self.results if r.is_terminal]

    @property
    def terminal(self) -> Row | None:
        """The verified terminal if there is one, else the last terminal seen.

        Preferring the verified one is not optimism: a genuine answer and a forgery can
        both be present at different sequence numbers, and `(correlationid, sequence)`
        means both are stored. `state` is what reports that; this just makes `terminal`
        name the answer rather than whichever arrived last.
        """
        for r in self.terminals:
            if r.verdict == VERDICT_OK:
                return r
        return self.terminals[-1] if self.terminals else None

    @property
    def state(self) -> str:
        """Why this chain is or is not bound — never a bare boolean.

        `bound=False` would read identically for "no terminal at all", "the terminal is
        unsigned" and "a genuine answer is present alongside a forgery", which are three
        different operational facts. Collapsing them is the mistake `DESIGN_PHASE4.md`
        §5.7 names about recording a timeout as a refusal.
        """
        if not self.request_id:
            return "orphaned"           # results naming no wake event on this topic
        terms = self.terminals
        if not terms:
            return "open"               # the run has not finished, or we read too early
        verified = [r for r in terms if r.verdict == VERDICT_OK]
        bad = [r for r in terms if r.verdict in (VERDICT_BAD, VERDICT_UNSIGNED)]
        if verified and bad:
            return "contested"          # the genuine answer survives, next to a forgery
        if bad:
            return "unverified"
        if not verified:
            return "unknown"            # no keyset: nothing was checked either way
        return "bound" if self.request_verdict == VERDICT_OK else "result-only"

    @property
    def bound(self) -> bool:
        """Both ends verified and nothing contesting the answer.

        Both ends is the point. A signed result naming an unsigned request proves who
        finished a run that anyone could have started; a signed request whose result is
        unsigned proves the reverse. The audit claim needs the pair — which is why
        `result-only` is a distinct state rather than a pass.
        """
        return self.state == "bound"


def classify(event, ks, *, bridge_kid: str | None = None) -> tuple[str, str, str | None]:
    """(verdict, reason, kid) for one event. `ks=None` means nothing to check against.

    Reuses `verify_with_keyset` rather than reimplementing the check — a reimplementation
    of the guard proves nothing about the guard (§8.6). `bridge_kid` pins group lifecycle
    events to EventBridge's own key, exactly as the live consumer does, so an approved
    runner's `group.completed` reads as BAD here too.
    """
    token = event.get("signature") or ""
    kid = signing.token_kid(token) if token else None
    if ks is None:
        return VERDICT_UNKNOWN, "no keyset supplied", kid
    if not token:
        terminal = str(event.get("final", "false")).lower() == "true"
        if terminal or ce.is_group_event(event):
            return VERDICT_UNSIGNED, "no ce_signature on a terminal event", kid
        return VERDICT_EXEMPT, "unsigned non-terminal (signing covers terminals)", kid
    expect = bridge_kid if (bridge_kid and ce.is_group_event(event)) else None
    ok, why = signing.verify_with_keyset(event, ks, expect_kid=expect)
    return (VERDICT_OK if ok else VERDICT_BAD), why, kid


def row_for(event, offset: int, ks, *, bridge_kid: str | None = None) -> Row:
    verdict, reason, kid = classify(event, ks, bridge_kid=bridge_kid)
    return Row(
        offset=offset,
        event_id=event.get("id") or "",
        type=event.get("type") or "",
        correlationid=event.get(ce.EXT_CORRELATIONID) or "",
        groupid=event.get(ce.EXT_GROUPID) or "",
        sequence=event.get(ce.EXT_SEQUENCE) or "",
        phase=event.get(ce.EXT_PHASE) or "",
        final=str(event.get("final", "false")).lower() == "true",
        kid=kid,
        verdict=verdict,
        reason=reason,
        causationid=event.get(ce.EXT_CAUSATIONID) or "",
    )


def chains(requests: list[Row], responses: list[Row]) -> list[Chain]:
    """Bind results to the wake events they name, by `causationid == request.id`.

    Results whose `causationid` matches no request are returned under a synthetic chain
    with an empty `request_id` — deliberately visible rather than dropped, because that
    is what an injected response looks like: an answer to a question nobody asked.
    """
    by_id = {r.event_id: Chain(request_id=r.event_id,
                               correlationid=r.correlationid,
                               request_verdict=r.verdict)
             for r in requests if r.event_id}
    orphans = Chain(request_id="", correlationid="", request_verdict=VERDICT_UNKNOWN)
    for resp in responses:
        target = by_id.get(resp.causationid)
        (target or orphans).results.append(resp)
    out = list(by_id.values())
    if orphans.results:
        out.append(orphans)
    return out


def summarise(rows: list[Row]) -> dict[str, int]:
    """Counts by verdict, plus the two totals a slide actually wants."""
    out: dict[str, int] = {}
    for r in rows:
        out[r.verdict] = out.get(r.verdict, 0) + 1
    out["total"] = len(rows)
    out["terminals"] = sum(1 for r in rows if r.is_terminal)
    # The number that matters: terminals carrying a signature that verified. A terminal
    # is the event the transcript presents as the answer, so this is the proportion of
    # answers whose origin is provable.
    out["terminals_verified"] = sum(1 for r in rows
                                    if r.is_terminal and r.verdict == VERDICT_OK)
    return out
