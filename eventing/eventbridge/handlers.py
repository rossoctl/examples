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
from eventbridge.correlation import Minter
from eventbridge.html_view import render
from eventbridge.kafka_out import Producer
from eventbridge.openapi import SWAGGER_HTML, spec
from eventbridge.store import Store
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


class Handlers:
    def __init__(self, cfg: Cfg, store: Store, producer: Producer, minter: Minter,
                 groups=None, registry=None) -> None:
        self.cfg = cfg
        self.store = store
        self.producer = producer
        self.minter = minter
        self.groups = groups
        # §2.5 — the approved-user registry, or None in single-tenant mode, where there
        # is nothing to look a caller up in.
        self.registry = registry
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
            return str(named).strip() or None
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

    def _owner_userkey(self, correlationid: str) -> str | None:
        """Which tenant owns this correlation, for a `/continue` turn.

        `None` in single-tenant mode, where topics do not depend on it.

        `/continue` is still unauthenticated here — Phase 2 §3.1's capability-URL
        design — so this resolves the owner from what was *recorded at submit time*
        rather than from the caller. The turn therefore reaches the right tenant's
        runner, which is what stops a resume from being executed under another user's
        credential. It does NOT yet check that the caller is that owner: owner-scoped
        reads and `?k=` capability keys are §6.2/§4.4 (T7/T9, rollout step 5), and until
        they land `multi` mode's `/continue` is as open as Phase 2's.

        The lookup goes through the recorded submitter rather than a
        `correlationid -> userkey` index because that index is T5's global table, which
        does not exist yet. When it does, this becomes one indexed `SELECT` and the
        derivation here goes away — §2.6 is explicit that the index has to exist anyway,
        for the uniqueness guarantee that keeps `sessionuuid` unsalted.
        """
        if not self.cfg.topics.multi:
            return None
        prompts = self.store.get_prompts(correlationid) or []
        for p in prompts:
            if submitter := p.get("submitter"):
                # The issuer is not recorded alongside the submitter today, so this
                # cannot distinguish a GitHub `alice` from a static `alice`. In `multi`
                # mode anonymous and static submissions are already refused at the
                # submit path (§2.4), so GitHub is the only issuer that reaches here —
                # stated rather than assumed, because the day an OIDC issuer is wired
                # up (§2.4's slot) this becomes wrong and has to move to the index.
                if user := (self.registry.lookup("github", submitter)
                            if self.registry else None):
                    return user.userkey
        return None

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

        corr = self.minter.mint()
        sess = ce.session_uuid(corr)
        workdir = str(pathlib.Path(self.cfg.tmpdir) / "eventrunner" / "work" / corr)
        self.store.upsert_session(corr, sess, workdir, prompt)
        self.store.insert_prompt(corr, "start", prompt, submitter=submitter)
        if groupid:
            # Membership before publication: the reverse order leaves a window where a
            # fast agent's terminal event arrives for a member nobody has recorded.
            self.store.add_group_member(groupid, corr)
        event_id = self.producer.publish_request(
            prompt=prompt, correlationid=corr, sessionuuid=sess,
            mode="start", model=model, max_turns=max_turns, subject="start",
            groupid=groupid, submitter=submitter, submitter_iss=sub_iss,
            userkey=caller.userkey, agent=self._agent_for(caller, body),
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
        groupid, created = self.groups.create(
            label=body.get("label"),
            expected=int(expected) if expected else (len(prompts) or None),
            min_success=int(body["min_success"]) if body.get("min_success") else None,
            deadline_s=float(body["deadline_s"]) if body.get("deadline_s") else
                       self.cfg.group_deadline_s,
            idempotency_key=idem,
        )
        if not created:
            # A retried POST must not launch a second batch.
            snap = self.groups.snapshot(groupid) or {}
            return _json(start_response, "200 OK", {
                "groupid": groupid, "created": False,
                "members": [m["correlationid"] for m in snap.get("members") or []],
                "html_url": f"{self.cfg.public_base_url}/v0/groups/{groupid}",
            })
        corrs = self.groups.submit_members(
            groupid, [str(x) for x in prompts],
            max_turns=int(body.get("max_turns", 3)), model=body.get("model"),
            submitter=submitter, submitter_iss=sub_iss,
            userkey=caller.userkey, agent=self._agent_for(caller, body))
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
        snap = self.groups.snapshot(groupid) if self.groups else None
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
        snap = self.groups.snapshot(groupid) if self.groups else None
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
        events = self.store.events_for(m["correlationid"])
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

    def close_group(self, environ, start_response, groupid: str, **_):
        if self.groups is None:
            return _json(start_response, "503 Service Unavailable", {"error": "disabled"})
        body = _read_json(environ)
        ok = self.groups.close(groupid, int(body["expected"]) if body.get("expected") else None)
        return _json(start_response, "200 OK", {"groupid": groupid, "closed": ok})

    def cancel_group(self, environ, start_response, groupid: str, **_):
        if self.groups is None:
            return _json(start_response, "503 Service Unavailable", {"error": "disabled"})
        ok = self.groups.cancel(groupid)
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
        session = self.store.get_session(correlationid)
        if session is None:
            return _json(start_response, "404 Not Found", {"error": "unknown correlationid"})
        sess = session["sessionuuid"]
        self.store.upsert_session(correlationid, sess, session["workdir"], None)
        turn_index = self.store.insert_prompt(correlationid, "continue", prompt)
        event_id = self.producer.publish_request(
            prompt=prompt, correlationid=correlationid, sessionuuid=sess,
            mode="continue", subject="resume",
            userkey=self._owner_userkey(correlationid),
        )
        next_seq = len(self.store.events_for(correlationid)) + 1
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
            session = self.store.get_session(correlationid)
            if session is not None:
                sess = session["sessionuuid"]
                self.store.upsert_session(correlationid, sess, session["workdir"], None)
                self.store.insert_prompt(correlationid, "continue", prompt)
                self.producer.publish_request(
                    prompt=prompt, correlationid=correlationid, sessionuuid=sess,
                    mode="continue", subject="resume",
                    userkey=self._owner_userkey(correlationid),
                )
        start_response("303 See Other", [("Location", f"/v0/agents/{correlationid}")])
        return [b""]

    # ---- HTML view ----
    def get_html(self, environ, start_response, correlationid: str, **_):
        if not CORR_REGEX.match(correlationid):
            start_response("400 Bad Request", [("Content-Type", "text/plain")])
            return [b"bad correlationid"]
        session = self.store.get_session(correlationid)
        events  = self.store.events_for(correlationid)
        prompts = self.store.get_prompts(correlationid)
        if session is None and not events and not prompts:
            start_response("404 Not Found", [("Content-Type", "text/plain")])
            return [b"unknown correlationid"]
        # Compose form is hidden by default (`?continue=1` opts in) so read-only
        # viewers don't accidentally submit; add ?continue=1 to reveal.
        qs = urllib.parse.parse_qs(environ.get("QUERY_STRING", ""))
        show_continue = (qs.get("continue", ["0"])[0]).lower() in ("1", "true", "yes", "on")
        # A member reached from the group page needs a way back; without it the
        # operator has to hand-edit the URL to return to what they were watching.
        groupid = self.store.group_of(correlationid)
        group_label = None
        if groupid:
            g = self.store.get_group(groupid) or {}
            group_label = g.get("label")
        html = render(correlationid, session, events, prompts=prompts,
                      show_continue=show_continue,
                      groupid=groupid, group_label=group_label)
        body = html.encode()
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))])
        return [body]

    # ---- JSON: turn-grouped view (chat) ----
    def get_turns(self, environ, start_response, correlationid: str, **_):
        session = self.store.get_session(correlationid)
        events  = self.store.events_for(correlationid)
        prompts = self.store.get_prompts(correlationid)
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
            "final": self.store.final_seen(correlationid),
        }
        gid = self.store.group_of(correlationid)
        if gid:
            body["groupid"] = gid
            body["group_url"] = f"{self.cfg.public_base_url}/v0/groups/{gid}"
        return _json(start_response, "200 OK", body)

    # ---- JSON events ----
    def get_events(self, environ, start_response, correlationid: str, **_):
        qs = urllib.parse.parse_qs(environ.get("QUERY_STRING", ""))
        since = int((qs.get("since", ["0"])[0]) or 0)
        events = self.store.events_for(correlationid, since_seq=since)
        return _json(start_response, "200 OK", {
            "correlationid": correlationid,
            "events": events,
            "final": self.store.final_seen(correlationid),
        })

    # ---- Raw CloudEvents (JSONL) ----
    def get_events_jsonl(self, environ, start_response, correlationid: str, **_):
        raws = self.store.raw_events_for(correlationid)
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
        events = self.store.events_for(correlationid, since_seq=since_seq)
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

        ev = self.store.subscribe(correlationid)
        try:
            deadline = time.monotonic() + 120.0
            while time.monotonic() < deadline:
                if ev.wait(timeout=15.0):
                    ev.clear()
                new = self.store.events_for(correlationid, since_seq=sent)
                for e in new:
                    yield f"data: {json.dumps(e)}\n\n".encode()
                    sent = max(sent, e["sequence"])
                    if e.get("final"):
                        return
                # keepalive comment
                yield b": keepalive\n\n"
        finally:
            self.store.unsubscribe(correlationid, ev)

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
        if self.store.get_session(correlationid) is None:
            return _json(start_response, "404 Not Found",
                         {"error": "unknown correlationid"})
        body, err = _read_body(environ, self.cfg.transcript_max_bytes)
        if err:
            status = ("413 Payload Too Large" if "over the" in err
                      else "400 Bad Request")
            return _json(start_response, status, {"error": err})
        meta = self.store.put_transcript(correlationid, body)
        return _json(start_response, "200 OK", meta)

    def get_transcript(self, environ, start_response, correlationid: str, **_):
        """Return the stored transcript verbatim, for `--resume <path>`."""
        if not CORR_REGEX.match(correlationid):
            return _json(start_response, "400 Bad Request", {"error": "bad correlationid"})
        body = self.store.get_transcript(correlationid)
        if body is None:
            return _json(start_response, "404 Not Found",
                         {"error": "no transcript checkpointed for this correlationid"})
        meta = self.store.transcript_meta(correlationid) or {}
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
        return _json(start_response, "200 OK", {"ok": True})
