"""WSGI handlers for /v0/agents endpoints."""
from __future__ import annotations

import json
import pathlib
import time
import urllib.parse
from typing import Any

from eventbridge import auth, ghauth
from eventbridge.config import Cfg
from eventbridge.correlation import REGEX as CORR_REGEX
from eventbridge.correlation import Minter, mint_for
from eventbridge.html_view import render
from eventbridge.kafka_out import Producer
from eventbridge.openapi import SWAGGER_HTML, spec
from eventbridge.store import Store
from eventrunner import agentspec
from shared import ce


def _json(start_response, status: str, obj: Any, headers: list[tuple[str, str]] | None = None) -> list[bytes]:
    body = json.dumps(obj).encode()
    hdr = [("Content-Type", "application/json"), ("Content-Length", str(len(body)))]
    if headers: hdr += headers
    start_response(status, hdr)
    return [body]


def _read_json(environ) -> dict:
    try:
        n = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        n = 0
    if n <= 0:
        return {}
    body = environ["wsgi.input"].read(n)
    try:
        return json.loads(body or b"{}")
    except json.JSONDecodeError:
        return {}


def _read_body(environ, limit: int) -> tuple[bytes | None, str | None]:
    """Read a raw request body up to `limit` bytes.

    Returns (body, error). Checks the declared Content-Length *before* reading so
    an oversized upload is rejected without buffering it, then caps the actual
    read too — a client is free to lie about the length.
    """
    try:
        declared = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        return None, "unparseable Content-Length"
    if declared <= 0:
        return None, "empty body"
    if declared > limit:
        return None, f"body is {declared} bytes, over the {limit}-byte limit"
    body = environ["wsgi.input"].read(min(declared, limit))
    if not body:
        return None, "empty body"
    return body, None


def _deny(start_response, reason: str, status: int = 401) -> list[bytes]:
    """Refuse with 401 or 403. `_json` already takes extra headers.

    The challenge header goes on 401 only. On a 403 the credential was fine and
    retrying with a different one is not the remedy, so advertising a scheme
    would be misleading.
    """
    if status == 403:
        return _json(start_response, "403 Forbidden", {"error": reason})
    return _json(start_response, "401 Unauthorized", {"error": reason},
                 [("WWW-Authenticate", auth.CHALLENGE)])


def _read_form(environ) -> dict:
    try:
        n = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        n = 0
    if n <= 0:
        return {}
    body = environ["wsgi.input"].read(n).decode()
    return {k: v[0] for k, v in urllib.parse.parse_qs(body).items()}


# A group id is minted by the same generator as a correlationid, so it has the same
# shape; the two live in separate URL namespaces (/v0/agents vs /v0/groups).
GROUP_REGEX = CORR_REGEX


class _BadRequest(Exception):
    """A 400 raised from a helper so the caller does not have to thread an error back.

    Only `_agent_for` uses it today. A helper that validates caller input needs some way
    to refuse, and returning a sentinel would mean every call site remembering to check
    one — which is the shape of mistake this phase keeps finding.
    """


class Handlers:
    def __init__(self, cfg: Cfg, store: Store, producer: Producer, minter: Minter,
                 groups=None, registry=None, stores=None, owners=None) -> None:
        self.cfg = cfg
        # The single store. In single-tenant mode this is the only one and every read
        # and write goes through it, exactly as in Phase 2. In multi-tenant mode it is
        # the `shared` tier's store and `_store_of` picks the per-tenant one instead.
        self.store = store
        self.producer = producer
        self.minter = minter
        self.groups = groups
        # §2.5 — the approved-user registry, or None in single-tenant mode, where there
        # is nothing to look a caller up in.
        self.registry = registry
        # §6.1 — `userkey -> Store`. None in single-tenant mode.
        self.stores = stores
        # §2.6/§6.2 — the global `correlationid -> userkey` index. None in single-tenant
        # mode, where there is only one tenant and nothing to scope to.
        self.owners = owners
        # One cache for the process, so a burst of requests from the same user
        # costs one GitHub call rather than one each.
        self.logins = ghauth.LoginCache(cfg.github_cache_ttl_s)

    def _agent_for(self, caller, body) -> str | None:
        """§5.1's resolution order, for the part EventBridge controls.

        The request may name an agent, otherwise the registry's default for this user.
        Anything below that (`ER_AGENT_NAME`, then `default`) is the runner's decision
        and is deliberately not second-guessed here — EventBridge does not know which
        specs a given runner image has baked in.
        """
        if named := (body or {}).get("agent"):
            # Validated at the HTTP boundary, where the caller is still listening.
            # `str(named)` would otherwise accept a dict, a list or `"../../etc"` and ship
            # it as `ce_agent`; the runner then answers with an asynchronous `phase=error`
            # event minutes later, which the submitter may never look at, instead of a
            # `400` on the call that made the mistake.
            candidate = str(named).strip()
            try:
                agentspec.validate_name(candidate)
            except agentspec.SpecError as e:
                raise _BadRequest(str(e)) from None
            return candidate
        if self.registry is not None and caller.userid:
            user = self.registry.by_userkey(caller.userkey) if caller.userkey else None
            if user and user.agent:
                return user.agent
        return None

    def _caller(self, environ):
        """§2.4: resolve the caller once, carrying the tenancy key.

        In `single` mode this is Phase 2's `auth.resolve` with the key left `None`, so
        `ce_userkey` never reaches the wire and behaviour is unchanged.
        """
        return auth.resolve_caller(environ, self.cfg, cache=self.logins,
                                   registry=self.registry)

    def _owner_userkey(self, correlationid: str) -> tuple[str | None, str | None]:
        """Which tenant owns this correlation. §2.6, §6.2.

        Returns `(userkey, unresolved_reason)`. Exactly one is ever set:

        * `(None, None)`   — single-tenant mode, or the `shared` tier; no key needed.
        * `(key, None)`    — the owning tenant.
        * `(None, reason)` — multi mode and the owner cannot be determined.

        The two-value return exists because the two `None` cases need opposite handling
        and a bare `None` cannot tell them apart. Passing an unresolved `None` into
        `TopicSet.requests()` raises `ValueError` — correctly, since publishing to a
        shared topic would run the turn on another tenant's runner — but that surfaces as
        a WSGI 500 instead of a status code, so callers refuse with a `503` naming the
        problem.

        This is now **one indexed lookup** against the global `correlationid -> userkey`
        table. The previous implementation derived the owner from the recorded
        `submitter`, which could not distinguish a GitHub `alice` from a static `alice`
        and went wrong the moment a second issuer was configured; §2.6 always required
        the index to exist for the uniqueness guarantee, so using it here costs nothing
        extra and removes that limitation.

        `/continue` is still unauthenticated — Phase 2 §3.1's capability-URL design — so
        this answers *who owns it*, not *who is asking*. The turn therefore reaches the
        right tenant's runner, which is what stops a resume from executing under another
        user's credential. Checking that the caller IS the owner is §6.2's read-scoping
        (T7), still to come.
        """
        if not self.cfg.topics.multi or self.owners is None:
            return None, None
        userkey, known = self.owners.owner_of(correlationid)
        if not known:
            return None, (f"{correlationid} is not in the ownership index, so the "
                          f"owning tenant cannot be determined")
        # A known correlation with a NULL owner is the `shared` tier (or one the mirror
        # discovered). That is a real answer for READING — its data lives in `shared/` —
        # and NOT a usable answer for PUBLISHING, because `TopicSet.requests(None)` raises
        # in multi mode: there is no per-user topic to send a turn to. The two needs
        # differ, so they are separate methods: `_store_of` resolves a store and accepts
        # this case, `_publishable_owner` refuses it. Conflating them either makes
        # shared-tier data unreadable or lets a `/continue` reach the ValueError.
        return userkey, None

    def _publishable_owner(self, correlationid: str) -> tuple[str | None, str | None]:
        """The owner to publish a turn for, or a reason it cannot be published.

        Stricter than `_owner_userkey`: in multi mode a correlation with no owning tenant
        is unpublishable, because there is no per-user requests topic to send to. Refusing
        here is what stops `TopicSet.requests(None)`'s `ValueError` becoming a WSGI 500
        with an orphan prompt row already committed — a conversation showing a turn that
        was never submitted.
        """
        userkey, unresolved = self._owner_userkey(correlationid)
        if unresolved:
            return None, unresolved
        if self.cfg.topics.multi and userkey is None:
            return None, (f"{correlationid} has no owning tenant (it predates tenancy or "
                          f"belongs to the shared tier), so there is no per-user topic to "
                          f"publish a turn to")
        return userkey, None

    def _store_of(self, correlationid: str):
        """The store holding one correlation's data, or `None` if it cannot be placed.

        Single-tenant mode always returns `self.store`, so every read path below is
        byte-identical to Phase 2. In multi-tenant mode an unplaceable correlation
        returns `None` and the caller answers `404` — which is also what §6.2 wants a
        *foreign* correlation to look like, so the unknown and the not-yours cases are
        indistinguishable to a client by construction rather than by remembering to make
        them so.
        """
        if self.stores is None:
            return self.store
        userkey, unresolved = self._owner_userkey(correlationid)
        if unresolved:
            return None
        return self.stores.for_userkey(userkey)

    # ---- start ----
    def start_agent(self, environ, start_response, **_):
        caller, status, why = self._caller(environ)
        if status:
            return _deny(start_response, why, status)
        submitter, sub_iss = caller.userid, caller.submitter_iss
        body = _read_json(environ)
        prompt = body.get("prompt")
        if not prompt:
            return _json(start_response, "400 Bad Request", {"error": "prompt required"})
        max_turns = int(body.get("max_turns", 3))
        model     = body.get("model")

        groupid = body.get("groupid")
        if groupid and not GROUP_REGEX.match(str(groupid)):
            return _json(start_response, "400 Bad Request", {"error": "bad groupid"})

        # EVERY refusal happens before the first write. `_agent_for` can reject, and
        # validating after the session and prompt rows are committed leaves a transcript
        # page showing a turn that was never submitted — plus a correlation id claimed in
        # the ownership index that nothing ever un-claims, since tombstoning is for
        # deletion rather than for abandonment. That is the same orphan-write shape as the
        # `/continue` 500 fixed in 7590ec8, reached by a route that fix did not cover.
        #
        # It depends only on `caller` and `body`, so there is nothing to gain by deferring
        # it: resolve first, mint and write second.
        try:
            agent = self._agent_for(caller, body)
        except _BadRequest as e:
            return _json(start_response, "400 Bad Request", {"error": str(e)})

        corr = mint_for(self.minter, caller.userkey)
        sess = ce.session_uuid(corr)
        workdir = str(pathlib.Path(self.cfg.tmpdir) / "eventrunner" / "work" / corr)
        # The submit path is the one place the tenant is known from the CALLER rather than
        # looked up: `mint_for` just claimed this correlation for `caller.userkey`, so
        # going back through the index would be a round trip for an answer already held.
        store = self.stores.for_userkey(caller.userkey) if self.stores else self.store
        store.upsert_session(corr, sess, workdir, prompt)
        store.insert_prompt(corr, "start", prompt, submitter=submitter)
        if groupid:
            # Membership before publication: the reverse order leaves a window where a
            # fast agent's terminal event arrives for a member nobody has recorded.
            store.add_group_member(groupid, corr)
        event_id = self.producer.publish_request(
            prompt=prompt, correlationid=corr, sessionuuid=sess,
            mode="start", model=model, max_turns=max_turns, subject="start",
            groupid=groupid, submitter=submitter, submitter_iss=sub_iss,
            userkey=caller.userkey, agent=agent,
        )
        out = {
            "correlationid": corr, "sessionuuid": sess,
            "event_id": event_id,
            "topic": self.cfg.topics.requests(caller.userkey),
            "html_url": f"{self.cfg.public_base_url}/v0/agents/{corr}",
        }
        if groupid:
            out["groupid"] = groupid
            out["group_url"] = f"{self.cfg.public_base_url}/v0/groups/{groupid}"
        return _json(start_response, "202 Accepted", out)

    # ---- groups (§21) ----
    def create_group(self, environ, start_response, **_):
        """Create a group, optionally fanning out its members in the same call.

        One call rather than create-then-submit removes an ordering hazard (a member
        finishing before the group row exists) and preserves the property that makes
        the batch scale at all: every request is published before any is watched, so
        lag reaches N and KEDA scales past one pod.
        """
        caller, status, why = self._caller(environ)
        if status:
            return _deny(start_response, why, status)
        submitter, sub_iss = caller.userid, caller.submitter_iss
        if self.groups is None:
            return _json(start_response, "503 Service Unavailable",
                         {"error": "group support not enabled"})
        body = _read_json(environ)
        prompts = body.get("prompts") or []
        expected = body.get("expected")
        if not isinstance(prompts, list):
            return _json(start_response, "400 Bad Request", {"error": "prompts must be a list"})
        if not prompts and not expected:
            return _json(start_response, "400 Bad Request",
                         {"error": "give either prompts[] or expected"})
        if prompts and expected and int(expected) != len(prompts):
            return _json(start_response, "400 Bad Request",
                         {"error": f"expected={expected} contradicts {len(prompts)} prompts"})

        idem = environ.get("HTTP_IDEMPOTENCY_KEY") or None

        # Before `groups.create`, which commits the group row AND publishes
        # `group.started`. A `400` after that leaves a group with no members that can
        # never gain any — and because the row is keyed on the Idempotency-Key, every
        # retry reads back `created: false, members: []`, which looks like success. That
        # is verbatim the failure the `userkey=` comment below describes, reached through
        # a different route: the state is permanent precisely because the key worked.
        try:
            agent = self._agent_for(caller, body)
        except _BadRequest as e:
            return _json(start_response, "400 Bad Request", {"error": str(e)})

        groupid, created = self.groups.create(
            label=body.get("label"),
            expected=int(expected) if expected else (len(prompts) or None),
            min_success=int(body["min_success"]) if body.get("min_success") else None,
            deadline_s=float(body["deadline_s"]) if body.get("deadline_s") else
                       self.cfg.group_deadline_s,
            idempotency_key=idem,
            # Without this the group ROW and the groupid's owner-index claim land in the
            # shared store with a NULL owner while every member row lands in the caller's
            # store: the batch reports 0 members forever, `maybe_complete` never fires, so
            # it never completes and never notifies — and a retried POST carrying an
            # Idempotency-Key reads the tenant store `create` never wrote and returns
            # `members: []`, which looks like success.
            userkey=caller.userkey,
        )
        if not created:
            # A retried POST must not launch a second batch.
            snap = self.groups.snapshot(groupid, userkey=caller.userkey) or {}
            return _json(start_response, "200 OK", {
                "groupid": groupid, "created": False,
                "members": [m["correlationid"] for m in snap.get("members") or []],
                "html_url": f"{self.cfg.public_base_url}/v0/groups/{groupid}",
            })
        corrs = self.groups.submit_members(
            groupid, [str(x) for x in prompts],
            max_turns=int(body.get("max_turns", 3)), model=body.get("model"),
            submitter=submitter, submitter_iss=sub_iss,
            userkey=caller.userkey, agent=agent)
        return _json(start_response, "202 Accepted", {
            "groupid": groupid, "created": True, "expected": expected or len(corrs),
            "members": corrs,
            "html_url": f"{self.cfg.public_base_url}/v0/groups/{groupid}",
        })

    def list_groups(self, environ, start_response, **_):
        """One URL, two representations.

        A browser gets the page; the CLI (and the page's own poller) get JSON. Content
        negotiation rather than a second path keeps /v0/groups the single name for
        "the list of batches".
        """
        # §6.2: the list shows only the CALLER's groups in multi-tenant mode. Reading
        # from their own store is what enforces that — not a filter applied afterwards,
        # which is the kind of predicate §6.1 says will eventually be forgotten.
        if self.stores is not None:
            caller, status, why = self._caller(environ)
            if status:
                return _deny(start_response, why, status)
            groups = self.stores.for_userkey(caller.userkey).all_groups()
        else:
            groups = self.store.all_groups()
        accept = environ.get("HTTP_ACCEPT", "")
        wants_html = ("text/html" in accept
                      and "application/json" not in accept
                      and environ.get("QUERY_STRING", "").find("format=json") < 0)
        if wants_html:
            from eventbridge.group_list_view import render as render_list
            body = render_list(groups).encode()
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"),
                                      ("Content-Length", str(len(body)))])
            return [body]
        return _json(start_response, "200 OK", {"groups": groups})

    def group_status(self, environ, start_response, groupid: str, **_):
        """The JSON the CLI polls. Same arithmetic as the page, one source of truth."""
        if not GROUP_REGEX.match(groupid):
            return _json(start_response, "400 Bad Request", {"error": "bad groupid"})
        owner, unresolved = self._owner_userkey(groupid)
        snap = (self.groups.snapshot(groupid, userkey=owner)
                if self.groups and not unresolved else None)
        if snap is None:
            return _json(start_response, "404 Not Found", {"error": "unknown groupid"})
        full = environ.get("QUERY_STRING", "").find("full=1") >= 0
        out = dict(snap)
        out["members"] = (snap["members"] if full else
                          [{"correlationid": m["correlationid"], "status": m["status"]}
                           for m in snap["members"]])
        out["html_url"] = f"{self.cfg.public_base_url}/v0/groups/{groupid}"
        return _json(start_response, "200 OK", out)

    def group_html(self, environ, start_response, groupid: str, **_):
        if not GROUP_REGEX.match(groupid):
            start_response("400 Bad Request", [("Content-Type", "text/plain")])
            return [b"bad groupid"]
        owner, unresolved = self._owner_userkey(groupid)
        snap = (self.groups.snapshot(groupid, userkey=owner)
                if self.groups and not unresolved else None)
        if snap is None:
            start_response("404 Not Found", [("Content-Type", "text/plain")])
            return [b"unknown groupid"]
        from eventbridge.group_view import render
        snap = dict(snap)
        snap["members"] = [self._decorate_member(m) for m in snap["members"]]
        body = render(snap, base_url=self.cfg.public_base_url).encode()
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"),
                                  ("Content-Length", str(len(body)))])
        return [body]

    def _decorate_member(self, m: dict) -> dict:
        """Add the latest text and a duration — the 'labor illusion' half of the page.

        Showing what each agent is actually doing buys patience and, for finished
        members, doubles as an interim artifact: enough to abandon a doomed batch after
        a minute instead of ten.
        """
        out = dict(m)
        member_store = self._store_of(m["correlationid"])
        events = member_store.events_for(m["correlationid"]) if member_store else []
        text = None
        for e in reversed(events):
            t = (e.get("data") or {}).get("text")
            if t:
                text = t.strip().replace("\n", " ")
                break
        out["text"] = text
        stats = None
        for e in reversed(events):
            stats = (e.get("data") or {}).get("stats")
            if stats:
                break
        ms = (stats or {}).get("duration_ms")
        out["duration"] = f"{ms / 1000:.1f}s" if ms else None
        return out

    def _authorize_mutation(self, environ, groupid: str):
        """Gate a route that CHANGES a group. Returns `(userkey, error)`.

        Mutating routes are authorized in `multi` mode even though reads are not yet
        (§6.2/T7). The distinction matters: T7 is about reads *staying* as open as Phase
        2's, and Phase 2 had no tenants to cross and no cross-tenant mutation anywhere.
        An unauthenticated `POST /v0/groups/{gid}/cancel` that resolves any tenant's group
        from the global index and cancels their queued members is a capability this phase
        would be *introducing*, not an openness it preserves — so it is refused here
        rather than deferred.

        `404` for a groupid this caller does not own, matching §6.2: distinguishing
        "exists but not yours" from "does not exist" lets anyone enumerate live ids
        across tenants, and the caller learns nothing actionable from the difference.
        """
        if self.stores is None:
            # Single-tenant mode: Phase 2's behaviour, no ownership to check.
            return None, None
        caller, status, why = self._caller(environ)
        if status:
            return None, (why, status)
        owner, unresolved = self._owner_userkey(groupid)
        if unresolved or owner != caller.userkey:
            return None, ("unknown groupid", 404)
        return owner, None

    def _mutation_denied(self, start_response, err):
        reason, status = err
        if status == 404:
            return _json(start_response, "404 Not Found", {"error": reason})
        return _deny(start_response, reason, status)

    def close_group(self, environ, start_response, groupid: str, **_):
        if self.groups is None:
            return _json(start_response, "503 Service Unavailable", {"error": "disabled"})
        owner, err = self._authorize_mutation(environ, groupid)
        if err:
            return self._mutation_denied(start_response, err)
        body = _read_json(environ)
        ok = self.groups.close(groupid,
                               int(body["expected"]) if body.get("expected") else None,
                               userkey=owner)
        return _json(start_response, "200 OK", {"groupid": groupid, "closed": ok})

    def cancel_group(self, environ, start_response, groupid: str, **_):
        if self.groups is None:
            return _json(start_response, "503 Service Unavailable", {"error": "disabled"})
        owner, err = self._authorize_mutation(environ, groupid)
        if err:
            return self._mutation_denied(start_response, err)
        ok = self.groups.cancel(groupid, userkey=owner)
        return _json(start_response, "200 OK", {
            "groupid": groupid, "cancelled": ok,
            "note": "queued members will not be submitted; agents already running "
                    "are NOT interrupted",
        })

    # ---- continue (JSON) ----
    def continue_agent(self, environ, start_response, correlationid: str, **_):
        if not CORR_REGEX.match(correlationid):
            return _json(start_response, "400 Bad Request", {"error": "bad correlationid"})
        body = _read_json(environ)
        prompt = body.get("prompt")
        if not prompt:
            return _json(start_response, "400 Bad Request", {"error": "prompt required"})
        # One lookup decides both which store holds this conversation and which tenant
        # the resume must be published for. An unplaceable correlation is a 404 — the same
        # answer a foreign one gets (§6.2), so the two are indistinguishable to a client.
        owner, unresolved = self._publishable_owner(correlationid)
        store = None if unresolved else (
            self.stores.for_userkey(owner) if self.stores else self.store)
        session = store.get_session(correlationid) if store else None
        if session is None:
            return _json(start_response, "404 Not Found", {"error": "unknown correlationid"})
        sess = session["sessionuuid"]
        store.upsert_session(correlationid, sess, session["workdir"], None)
        turn_index = store.insert_prompt(correlationid, "continue", prompt)
        event_id = self.producer.publish_request(
            prompt=prompt, correlationid=correlationid, sessionuuid=sess,
            mode="continue", subject="resume",
            userkey=owner,
        )
        next_seq = len(store.events_for(correlationid)) + 1
        return _json(start_response, "202 Accepted", {
            "correlationid": correlationid, "sequence": next_seq,
            "turn_index": turn_index, "event_id": event_id,
        })

    # ---- continue (HTML form) ----
    def continue_agent_html(self, environ, start_response, correlationid: str, **_):
        if not CORR_REGEX.match(correlationid):
            return _json(start_response, "400 Bad Request", {"error": "bad correlationid"})
        form = _read_form(environ)
        prompt = form.get("prompt", "").strip()
        if prompt:
            owner, unresolved = self._publishable_owner(correlationid)
            if unresolved:
                # The form posts from the transcript page, so the useful answer is the
                # page with an error on it rather than a silent 303 — redirecting would
                # look like the turn was accepted and then vanished, which is the Phase 2
                # §6.1 class of bug this phase keeps trying to avoid.
                start_response("503 Service Unavailable",
                               [("Content-Type", "text/plain; charset=utf-8")])
                return [f"cannot resume {correlationid}: {unresolved}\n".encode()]
            store = self.stores.for_userkey(owner) if self.stores else self.store
            session = store.get_session(correlationid)
            if session is not None:
                sess = session["sessionuuid"]
                store.upsert_session(correlationid, sess, session["workdir"], None)
                store.insert_prompt(correlationid, "continue", prompt)
                self.producer.publish_request(
                    prompt=prompt, correlationid=correlationid, sessionuuid=sess,
                    mode="continue", subject="resume",
                    userkey=owner,
                )
        start_response("303 See Other", [("Location", f"/v0/agents/{correlationid}")])
        return [b""]

    # ---- HTML view ----
    def get_html(self, environ, start_response, correlationid: str, **_):
        if not CORR_REGEX.match(correlationid):
            start_response("400 Bad Request", [("Content-Type", "text/plain")])
            return [b"bad correlationid"]
        store = self._store_of(correlationid)
        session = store.get_session(correlationid) if store else None
        events  = store.events_for(correlationid) if store else []
        prompts = store.get_prompts(correlationid) if store else []
        if session is None and not events and not prompts:
            start_response("404 Not Found", [("Content-Type", "text/plain")])
            return [b"unknown correlationid"]
        # Compose form is hidden by default (`?continue=1` opts in) so read-only
        # viewers don't accidentally submit; add ?continue=1 to reveal.
        qs = urllib.parse.parse_qs(environ.get("QUERY_STRING", ""))
        show_continue = (qs.get("continue", ["0"])[0]).lower() in ("1", "true", "yes", "on")
        # A member reached from the group page needs a way back; without it the
        # operator has to hand-edit the URL to return to what they were watching.
        groupid = store.group_of(correlationid)
        group_label = None
        if groupid:
            g = store.get_group(groupid) or {}
            group_label = g.get("label")
        html = render(correlationid, session, events, prompts=prompts,
                      show_continue=show_continue,
                      groupid=groupid, group_label=group_label)
        body = html.encode()
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))])
        return [body]

    # ---- JSON: turn-grouped view (chat) ----
    def get_turns(self, environ, start_response, correlationid: str, **_):
        store = self._store_of(correlationid)
        session = store.get_session(correlationid) if store else None
        events  = store.events_for(correlationid) if store else []
        prompts = store.get_prompts(correlationid) if store else []
        if session is None and not events and not prompts:
            return _json(start_response, "404 Not Found", {"error": "unknown correlationid"})
        from eventbridge.html_view import group_by_turns
        groups = group_by_turns(events, prompts)
        out = []
        for t in groups:
            # Drop the (large) events list unless ?full=1
            keep_events = environ.get("QUERY_STRING", "").find("full=1") >= 0
            row = {
                "turn_index":    t.get("turn_index"),
                "mode":          t.get("mode"),
                "submitter":     t.get("submitter"),
                "prompt":        t.get("prompt"),
                "assistant_text": t.get("assistant_text"),
                "started":       t.get("started"),
                "finished":      t.get("finished"),
                "stats":         t.get("stats"),
                "event_count":   len(t.get("events") or []),
            }
            if keep_events:
                row["events"] = t.get("events")
            out.append(row)
        body = {
            "correlationid": correlationid,
            "turns": out,
            "final": store.final_seen(correlationid),
        }
        gid = store.group_of(correlationid)
        if gid:
            body["groupid"] = gid
            body["group_url"] = f"{self.cfg.public_base_url}/v0/groups/{gid}"
        return _json(start_response, "200 OK", body)

    # ---- JSON events ----
    def get_events(self, environ, start_response, correlationid: str, **_):
        qs = urllib.parse.parse_qs(environ.get("QUERY_STRING", ""))
        since = int((qs.get("since", ["0"])[0]) or 0)
        store = self._store_of(correlationid)
        if store is None:
            # §6.2: an unplaceable correlation reads as absent, never as "exists but not
            # yours" — the latter would let anyone enumerate live ids across tenants.
            return _json(start_response, "404 Not Found", {"error": "unknown correlationid"})
        events = store.events_for(correlationid, since_seq=since)
        return _json(start_response, "200 OK", {
            "correlationid": correlationid,
            "events": events,
            "final": store.final_seen(correlationid),
        })

    # ---- Raw CloudEvents (JSONL) ----
    def get_events_jsonl(self, environ, start_response, correlationid: str, **_):
        store = self._store_of(correlationid)
        if store is None:
            return _json(start_response, "404 Not Found", {"error": "unknown correlationid"})
        raws = store.raw_events_for(correlationid)
        body = "\n".join(json.dumps(e) for e in raws).encode() + b"\n"
        start_response("200 OK", [
            ("Content-Type", "application/x-ndjson"),
            ("Content-Length", str(len(body))),
        ])
        return [body]

    # ---- SSE ----
    def get_events_sse(self, environ, start_response, correlationid: str, **_):
        qs = urllib.parse.parse_qs(environ.get("QUERY_STRING", ""))
        try:
            since = int((qs.get("since", ["0"])[0]) or 0)
        except ValueError:
            since = 0
        start_response("200 OK", [
            ("Content-Type", "text/event-stream; charset=utf-8"),
            ("Cache-Control", "no-cache"),
            ("X-Accel-Buffering", "no"),
        ])
        return self._sse_generator(correlationid, since_seq=since)

    def _sse_generator(self, correlationid: str, since_seq: int = 0):
        # Replay any events past `since_seq`. When the caller passes the last
        # sequence already rendered server-side, this stream only delivers NEW
        # events — no duplicates, no reload loop.
        sent = since_seq
        # Resolved ONCE and held for the life of the generator. Re-resolving per poll
        # would be a correctness bug, not just a cost: `StoreRegistry` may return a
        # *different* Store object after an eviction and reopen, and then `unsubscribe`
        # in the `finally` would run against a store that never saw the `subscribe` —
        # leaking the Event and leaving the old store pinned forever. Holding the
        # reference is also what makes the registry's "pinned while subscribed" rule
        # work, since the subscriber list lives on this object.
        store = self._store_of(correlationid)
        if store is None:
            return

        # SUBSCRIBE FIRST, before the replay. The registry pins a store only while it has
        # subscribers, so between `_store_of` and `subscribe` this store is evictable —
        # and another request for a different tenant can close it underneath us, making
        # the replay below fail with `sqlite3.ProgrammingError: Cannot operate on a closed
        # database`. Subscribing first closes that window: from here until the `finally`
        # the store cannot be evicted.
        #
        # Subscribing before the replay is also harmless for correctness: `ev` only ever
        # means "there may be something new", every read is `since_seq=sent`, and a set
        # Event simply costs one extra query.
        ev = store.subscribe(correlationid)
        try:
            events = store.events_for(correlationid, since_seq=since_seq)
            seen_final = False
            for e in events:
                yield f"data: {json.dumps(e)}\n\n".encode()
                sent = max(sent, e["sequence"])
                if e.get("final"):
                    seen_final = True
            if seen_final:
                # Nothing more to stream — connection closes cleanly instead of
                # holding open and waiting for events that will never arrive.
                return

            deadline = time.monotonic() + 120.0
            while time.monotonic() < deadline:
                if ev.wait(timeout=15.0):
                    ev.clear()
                new = store.events_for(correlationid, since_seq=sent)
                for e in new:
                    yield f"data: {json.dumps(e)}\n\n".encode()
                    sent = max(sent, e["sequence"])
                    if e.get("final"):
                        return
                # keepalive comment
                yield b": keepalive\n\n"
        finally:
            store.unsubscribe(correlationid, ev)

    # ---- transcripts (§16 Gap B) ----
    def put_transcript(self, environ, start_response, correlationid: str, **_):
        """Checkpoint a `claude` session transcript for this correlation.

        EventRunner calls this after every turn. It exists because the runner is
        stateless and its `$HOME` is a pod's ephemeral layer, so without a
        checkpoint a `/continue` after a scale-to-zero has no transcript to
        resume from (DESIGN_PHASE1.md §16 Gap B). EventBridge is already the
        single-replica component with a volume and a store (§8.6), so this needs
        no new backing service.
        """
        if not CORR_REGEX.match(correlationid):
            return _json(start_response, "400 Bad Request", {"error": "bad correlationid"})
        # Require a known correlation: accepting a transcript for an id we never
        # minted would let anyone seed arbitrary resume state.
        store = self._store_of(correlationid)
        if store is None or store.get_session(correlationid) is None:
            return _json(start_response, "404 Not Found",
                         {"error": "unknown correlationid"})
        body, err = _read_body(environ, self.cfg.transcript_max_bytes)
        if err:
            status = ("413 Payload Too Large" if "over the" in err
                      else "400 Bad Request")
            return _json(start_response, status, {"error": err})
        meta = store.put_transcript(correlationid, body)
        return _json(start_response, "200 OK", meta)

    def get_transcript(self, environ, start_response, correlationid: str, **_):
        """Return the stored transcript verbatim, for `--resume <path>`."""
        if not CORR_REGEX.match(correlationid):
            return _json(start_response, "400 Bad Request", {"error": "bad correlationid"})
        store = self._store_of(correlationid)
        body = store.get_transcript(correlationid) if store else None
        if body is None:
            return _json(start_response, "404 Not Found",
                         {"error": "no transcript checkpointed for this correlationid"})
        meta = store.transcript_meta(correlationid) or {}
        start_response("200 OK", [
            ("Content-Type", "application/x-ndjson"),
            ("Content-Length", str(len(body))),
            ("ETag", f'"{meta.get("sha256", "")}"'),
        ])
        return [body]

    # ---- selftest ----
    def selftest(self, environ, start_response, **_):
        from eventbridge.selftest import probe_candidates
        qs = urllib.parse.parse_qs(environ.get("QUERY_STRING", ""))
        extras = qs.get("url", [])
        _host, port_s = self.cfg.http_addr.rsplit(":", 1)
        results = probe_candidates(int(port_s), current=self.cfg.public_base_url, extra_urls=extras)
        return _json(start_response, "200 OK", {
            "current_public_base_url": self.cfg.public_base_url,
            "http_addr": self.cfg.http_addr,
            "candidates": [r.to_dict() for r in results],
        })

    # ---- meta ----
    def openapi_json(self, environ, start_response, **_):
        return _json(start_response, "200 OK", spec())

    def docs(self, environ, start_response, **_):
        body = SWAGGER_HTML.encode()
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))])
        return [body]

    def healthz(self, environ, start_response, **_):
        """Liveness, plus the tenancy counters §6.1 asks to be surfaced.

        `unattributed` is the one worth watching: it counts responses that arrived in
        multi-tenant mode with no `userkey` and were filed under `shared/` rather than
        guessed into a tenant's store. A non-zero and rising value means a runner is
        misconfigured (no `ER_USERKEY`), which otherwise looks like a working pod whose
        output lands somewhere nobody is reading. `ok` stays true regardless — this is
        information for an operator, not a reason to fail a liveness probe and restart a
        bridge that is working.
        """
        out = {"ok": True}
        if self.stores is not None:
            out["tenancy"] = {
                "mode": self.cfg.tenancy_mode,
                "unattributed": self.stores.unattributed,
                "open_stores": self.stores.open_count,
                "evictions": self.stores.evictions,
            }
            if self.owners is not None:
                out["tenancy"]["correlations"] = self.owners.count()
        return _json(start_response, "200 OK", out)
