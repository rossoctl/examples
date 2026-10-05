"""SQLite gateways for responses + sessions. WAL mode; single-writer lock."""
from __future__ import annotations

import json
import pathlib
import sqlite3
import threading
from typing import Any

_SCHEMA_RESPONSES = """
CREATE TABLE IF NOT EXISTS responses (
  correlationid TEXT NOT NULL,
  sequence      INTEGER NOT NULL,
  phase         TEXT NOT NULL,
  event_id      TEXT NOT NULL,
  event_time    TEXT NOT NULL,
  data_json     TEXT NOT NULL,
  final         INTEGER NOT NULL DEFAULT 0,
  raw_json      TEXT NOT NULL,
  PRIMARY KEY (correlationid, sequence)
);
CREATE INDEX IF NOT EXISTS responses_by_corr ON responses(correlationid);
"""

_SCHEMA_SESSIONS = """
CREATE TABLE IF NOT EXISTS sessions (
  correlationid TEXT PRIMARY KEY,
  sessionuuid   TEXT NOT NULL,
  workdir       TEXT NOT NULL,
  first_prompt  TEXT,
  created_utc   TEXT NOT NULL,
  updated_utc   TEXT NOT NULL,
  turns         INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS prompts (
  correlationid TEXT NOT NULL,
  turn_index    INTEGER NOT NULL,      -- 1-based; N = the Nth POST for this corr
  mode          TEXT NOT NULL,         -- 'start' | 'continue'
  prompt        TEXT NOT NULL,
  submitted_utc TEXT NOT NULL,
  submitter     TEXT,                  -- authenticated caller; NULL when auth is off
  PRIMARY KEY (correlationid, turn_index)
);
CREATE INDEX IF NOT EXISTS prompts_by_corr ON prompts(correlationid);
-- Phase 1 §21: agent groups — batch fan-out with a tracked fan-in.
--
-- Counts are DERIVED from group_members, never held as a mutable counter. §21.5:
-- RQ-1 accepts at-least-once delivery, so a member can emit a second final event; a
-- decrementing counter would then reach zero while agents are still running and fire
-- "all done" early, which is the one lie a progress display must never tell.
CREATE TABLE IF NOT EXISTS groups (
  groupid           TEXT PRIMARY KEY,
  label             TEXT,
  expected          INTEGER,        -- NULL for an open-ended group
  min_success       INTEGER,        -- quorum; NULL = every member must finish
  deadline_utc      TEXT,           -- straggler cutoff; NULL = none
  created_utc       TEXT NOT NULL,
  closed_utc        TEXT,           -- when `expected` was fixed by POST /close
  completed_utc     TEXT,           -- the once-only transition guard
  completion_reason TEXT,           -- all | quorum | deadline | cancelled
  cancelled_utc     TEXT,
  idempotency_key   TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS groups_idempotency
  ON groups(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS group_members (
  groupid         TEXT NOT NULL,
  correlationid   TEXT NOT NULL,
  status          TEXT NOT NULL,    -- submitted | running | finished | failed
  submitted_utc   TEXT NOT NULL,
  first_event_utc TEXT,
  finished_utc    TEXT,
  terminal_phase  TEXT,             -- result | error
  PRIMARY KEY (groupid, correlationid)
);
-- What lets the responses consumer answer "does this event belong to a group?" with a
-- primary-key hit instead of a scan, on the hot path.
CREATE INDEX IF NOT EXISTS group_members_by_corr ON group_members(correlationid);
-- Phase 1 §16 Gap B: the `claude` session transcript, checkpointed here after
-- each turn so a `/continue` can resume even though the pod that ran the first
-- turn has been scaled away. One row per correlation, overwritten each turn —
-- the transcript is cumulative, so only the latest matters.
CREATE TABLE IF NOT EXISTS transcripts (
  correlationid TEXT PRIMARY KEY,
  body          BLOB NOT NULL,
  size          INTEGER NOT NULL,
  sha256        TEXT NOT NULL,
  checkpoints   INTEGER NOT NULL DEFAULT 1,   -- how many turns have written it
  updated_utc   TEXT NOT NULL
);
"""


class Store:
    def __init__(self, root: str | pathlib.Path) -> None:
        base = pathlib.Path(root)
        base.mkdir(parents=True, exist_ok=True)
        self._r = sqlite3.connect(base / "responses.sqlite", check_same_thread=False, isolation_level=None)
        self._s = sqlite3.connect(base / "sessions.sqlite",  check_same_thread=False, isolation_level=None)
        for c in (self._r, self._s):
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
        self._r.executescript(_SCHEMA_RESPONSES)
        self._s.executescript(_SCHEMA_SESSIONS)
        # `CREATE TABLE IF NOT EXISTS` is a no-op on an existing file, so a column
        # added after a DB was created needs an explicit ALTER. Idempotent: SQLite
        # raises OperationalError when the column is already there.
        try:
            self._s.execute("ALTER TABLE prompts ADD COLUMN submitter TEXT")
        except sqlite3.OperationalError:
            pass
        self._lock = threading.Lock()
        self._sub_lock = threading.Lock()
        self._subscribers: dict[str, list[threading.Event]] = {}

    # ---- responses ----
    def insert_response(self, event: dict[str, Any]) -> None:
        corr     = event["correlationid"]
        sequence = int(event.get("sequence", "0"))
        phase    = event.get("phase", "stdout")
        final    = 1 if str(event.get("final", "false")).lower() == "true" else 0
        with self._lock:
            self._r.execute(
                "INSERT OR REPLACE INTO responses(correlationid,sequence,phase,event_id,event_time,data_json,final,raw_json) VALUES (?,?,?,?,?,?,?,?)",
                (corr, sequence, phase, event.get("id", ""), event.get("time", ""),
                 json.dumps(event.get("data")), final, json.dumps(event)),
            )
        self._notify(corr)

    def events_for(self, corr: str, since_seq: int = 0) -> list[dict[str, Any]]:
        rows = self._r.execute(
            "SELECT sequence, phase, event_id, event_time, data_json, final FROM responses WHERE correlationid=? AND sequence>? ORDER BY sequence",
            (corr, since_seq),
        ).fetchall()
        out = []
        for seq, phase, ev_id, ev_time, data_json, final in rows:
            out.append({
                "sequence": seq, "phase": phase, "id": ev_id, "time": ev_time,
                "data": json.loads(data_json) if data_json else None,
                "final": bool(final),
            })
        return out

    def raw_events_for(self, corr: str) -> list[dict[str, Any]]:
        rows = self._r.execute(
            "SELECT raw_json FROM responses WHERE correlationid=? ORDER BY sequence",
            (corr,),
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def final_seen(self, corr: str) -> bool:
        row = self._r.execute(
            "SELECT COUNT(*) FROM responses WHERE correlationid=? AND final=1", (corr,)
        ).fetchone()
        return bool(row and row[0])

    # ---- sessions ----
    def upsert_session(self, correlationid: str, sessionuuid: str, workdir: str, first_prompt: str | None) -> None:
        now = _now_iso()
        with self._lock:
            row = self._s.execute("SELECT correlationid, turns FROM sessions WHERE correlationid=?", (correlationid,)).fetchone()
            if row is None:
                self._s.execute(
                    "INSERT INTO sessions(correlationid,sessionuuid,workdir,first_prompt,created_utc,updated_utc,turns) VALUES (?,?,?,?,?,?,0)",
                    (correlationid, sessionuuid, workdir, first_prompt or "", now, now),
                )
            else:
                self._s.execute(
                    "UPDATE sessions SET updated_utc=?, turns=turns+1 WHERE correlationid=?", (now, correlationid),
                )

    def insert_prompt(self, correlationid: str, mode: str, prompt: str,
                       submitted_utc: str | None = None,
                       submitter: str | None = None) -> int:
        """Append a new prompt to this corr's turn tape. Returns the 1-based turn_index."""
        ts = submitted_utc or _now_iso()
        with self._lock:
            row = self._s.execute(
                "SELECT COALESCE(MAX(turn_index), 0) FROM prompts WHERE correlationid=?",
                (correlationid,),
            ).fetchone()
            next_idx = (row[0] if row else 0) + 1
            self._s.execute(
                "INSERT INTO prompts(correlationid,turn_index,mode,prompt,submitted_utc,submitter) VALUES (?,?,?,?,?,?)",
                (correlationid, next_idx, mode, prompt, ts, submitter),
            )
        return next_idx

    def backfill_prompt_if_missing(self, correlationid: str, mode: str,
                                    prompt: str, submitted_utc: str | None = None) -> int | None:
        """Insert a prompt only if no row for this (correlationid, prompt) exists.

        Used by the Kafka `requests` mirror: on every request event we see, we
        insert the prompt at the tail of the turn tape unless the exact same
        (mode, prompt) has already been recorded for this correlation.
        Returns the new turn_index or None if it was already recorded.
        """
        with self._lock:
            row = self._s.execute(
                "SELECT turn_index FROM prompts WHERE correlationid=? AND mode=? AND prompt=?",
                (correlationid, mode, prompt),
            ).fetchone()
            if row is not None:
                return None
            row = self._s.execute(
                "SELECT COALESCE(MAX(turn_index), 0) FROM prompts WHERE correlationid=?",
                (correlationid,),
            ).fetchone()
            next_idx = (row[0] if row else 0) + 1
            self._s.execute(
                "INSERT INTO prompts(correlationid,turn_index,mode,prompt,submitted_utc) VALUES (?,?,?,?,?)",
                (correlationid, next_idx, mode, prompt, submitted_utc or _now_iso()),
            )
        return next_idx

    def get_prompts(self, correlationid: str) -> list[dict[str, Any]]:
        rows = self._s.execute(
            "SELECT turn_index, mode, prompt, submitted_utc, submitter FROM prompts WHERE correlationid=? ORDER BY turn_index",
            (correlationid,),
        ).fetchall()
        return [{"turn_index": t, "mode": m, "prompt": p, "submitted_utc": s, "submitter": w}
                for t, m, p, s, w in rows]

    def get_session(self, correlationid: str) -> dict[str, Any] | None:
        row = self._s.execute(
            "SELECT correlationid, sessionuuid, workdir, first_prompt, created_utc, updated_utc, turns FROM sessions WHERE correlationid=?",
            (correlationid,),
        ).fetchone()
        if not row:
            return None
        keys = ("correlationid", "sessionuuid", "workdir", "first_prompt", "created_utc", "updated_utc", "turns")
        return dict(zip(keys, row))

    def all_correlations(self, limit: int = 100) -> list[str]:
        rows = self._s.execute(
            "SELECT correlationid FROM sessions ORDER BY updated_utc DESC LIMIT ?", (limit,)
        ).fetchall()
        return [r[0] for r in rows]

    # ---- groups (§21) ----
    def create_group(self, groupid: str, *, label: str | None, expected: int | None,
                     min_success: int | None = None, deadline_utc: str | None = None,
                     idempotency_key: str | None = None) -> dict[str, Any]:
        """Create a group. With an idempotency key, a repeat returns the original.

        The key matters because a client retry of "submit 100 agents" would otherwise
        silently launch 200.
        """
        now = _now_iso()
        with self._lock:
            if idempotency_key:
                row = self._s.execute(
                    "SELECT groupid FROM groups WHERE idempotency_key=?",
                    (idempotency_key,)).fetchone()
                if row:
                    return {"groupid": row[0], "created": False}
            self._s.execute(
                "INSERT OR IGNORE INTO groups"
                "(groupid,label,expected,min_success,deadline_utc,created_utc,"
                "idempotency_key) VALUES (?,?,?,?,?,?,?)",
                (groupid, label, expected, min_success, deadline_utc, now,
                 idempotency_key))
        return {"groupid": groupid, "created": True}

    def get_group(self, groupid: str) -> dict[str, Any] | None:
        cols = ("groupid", "label", "expected", "min_success", "deadline_utc",
                "created_utc", "closed_utc", "completed_utc", "completion_reason",
                "cancelled_utc")
        row = self._s.execute(
            f"SELECT {','.join(cols)} FROM groups WHERE groupid=?", (groupid,)).fetchone()
        return dict(zip(cols, row)) if row else None

    def all_groups(self, limit: int = 100) -> list[dict[str, Any]]:
        """Recent groups with their counts and last activity.

        The counts and `last_activity_utc` are joined in rather than fetched per group,
        so a list of 100 groups is one query instead of 201.
        """
        rows = self._s.execute(
            "SELECT g.groupid, g.label, g.expected, g.created_utc, g.completed_utc, "
            "       g.completion_reason, g.cancelled_utc, "
            "       COUNT(m.correlationid), "
            "       SUM(CASE WHEN m.status='finished' THEN 1 ELSE 0 END), "
            "       SUM(CASE WHEN m.status='failed'   THEN 1 ELSE 0 END), "
            "       MAX(COALESCE(m.finished_utc, m.first_event_utc, m.submitted_utc)) "
            "FROM groups g LEFT JOIN group_members m ON m.groupid = g.groupid "
            "GROUP BY g.groupid ORDER BY g.created_utc DESC LIMIT ?",
            (limit,)).fetchall()
        out = []
        for (gid, label, expected, created, completed, reason, cancelled,
             members, finished, failed, last) in rows:
            out.append({
                "groupid": gid, "label": label, "expected": expected,
                "created_utc": created, "completed_utc": completed,
                "completion_reason": reason, "cancelled_utc": cancelled,
                "members": members or 0,
                "finished": finished or 0, "failed": failed or 0,
                "last_activity_utc": last or created,
            })
        return out

    def add_group_member(self, groupid: str, correlationid: str) -> None:
        """Record membership. Idempotent, and accepted for a group row that does not
        exist yet — an EventBridge restart re-scans the topic and a fast agent can
        finish before its group's started event is processed (§21.4)."""
        with self._lock:
            self._s.execute(
                "INSERT OR IGNORE INTO group_members"
                "(groupid,correlationid,status,submitted_utc) VALUES (?,?,'submitted',?)",
                (groupid, correlationid, _now_iso()))

    def mark_member_running(self, groupid: str, correlationid: str) -> None:
        with self._lock:
            self._s.execute(
                "UPDATE group_members SET status='running', first_event_utc=? "
                "WHERE groupid=? AND correlationid=? AND first_event_utc IS NULL",
                (_now_iso(), groupid, correlationid))

    def mark_member_finished(self, groupid: str, correlationid: str,
                             terminal_phase: str) -> bool:
        """Record a member's terminal event. Returns True only on the FIRST call.

        `finished_utc IS NULL` makes this idempotent by construction, which is the
        whole §21.5 correction: a redelivered request re-runs the agent and emits a
        second final event, and this must not count twice.
        """
        status = "failed" if terminal_phase == "error" else "finished"
        with self._lock:
            cur = self._s.execute(
                "UPDATE group_members SET status=?, finished_utc=?, terminal_phase=? "
                "WHERE groupid=? AND correlationid=? AND finished_utc IS NULL",
                (status, _now_iso(), terminal_phase, groupid, correlationid))
            return cur.rowcount == 1

    def group_of(self, correlationid: str) -> str | None:
        row = self._s.execute(
            "SELECT groupid FROM group_members WHERE correlationid=? LIMIT 1",
            (correlationid,)).fetchone()
        return row[0] if row else None

    def group_members(self, groupid: str) -> list[dict[str, Any]]:
        cols = ("correlationid", "status", "submitted_utc", "first_event_utc",
                "finished_utc", "terminal_phase")
        rows = self._s.execute(
            f"SELECT {','.join(cols)} FROM group_members WHERE groupid=? "
            f"ORDER BY submitted_utc, correlationid", (groupid,)).fetchall()
        return [dict(zip(cols, r)) for r in rows]

    def group_counts(self, groupid: str) -> dict[str, int]:
        """Counts derived by aggregation — never a stored number (§21.5)."""
        rows = self._s.execute(
            "SELECT status, COUNT(*) FROM group_members WHERE groupid=? GROUP BY status",
            (groupid,)).fetchall()
        out = {"submitted": 0, "running": 0, "finished": 0, "failed": 0}
        for status, n in rows:
            out[status] = n
        out["members"] = sum(out[k] for k in ("submitted", "running", "finished", "failed"))
        out["terminal"] = out["finished"] + out["failed"]
        return out

    def close_group(self, groupid: str, expected: int | None = None) -> bool:
        with self._lock:
            cur = self._s.execute(
                "UPDATE groups SET closed_utc=?, expected=COALESCE(?, expected) "
                "WHERE groupid=? AND closed_utc IS NULL",
                (_now_iso(), expected, groupid))
            return cur.rowcount == 1

    def cancel_group(self, groupid: str) -> bool:
        with self._lock:
            cur = self._s.execute(
                "UPDATE groups SET cancelled_utc=? WHERE groupid=? AND cancelled_utc IS NULL",
                (_now_iso(), groupid))
            return cur.rowcount == 1

    def complete_group(self, groupid: str, reason: str) -> bool:
        """The once-only completion transition. True for exactly one caller, ever.

        Guarded in SQLite rather than in memory so it holds across an EventBridge
        restart, which an in-process flag would not (§21.5). EventBridge is
        single-replica by constraint (§8.6), so a row is a sufficient lock.
        """
        with self._lock:
            cur = self._s.execute(
                "UPDATE groups SET completed_utc=?, completion_reason=? "
                "WHERE groupid=? AND completed_utc IS NULL",
                (_now_iso(), reason, groupid))
            return cur.rowcount == 1

    def groups_past_deadline(self) -> list[str]:
        """Open groups whose deadline has passed — the straggler sweep (§21.9.2)."""
        rows = self._s.execute(
            "SELECT groupid FROM groups WHERE completed_utc IS NULL "
            "AND deadline_utc IS NOT NULL AND deadline_utc <= ?", (_now_iso(),)).fetchall()
        return [r[0] for r in rows]

    # ---- transcripts (§16 Gap B) ----
    def put_transcript(self, correlationid: str, body: bytes) -> dict[str, Any]:
        """Store (or replace) this correlation's session transcript.

        Idempotent by design: EventRunner uploads after every turn and the
        transcript is cumulative, so the newest upload supersedes the previous
        one. `checkpoints` counts the writes, which is how you tell "resumed
        three times" from "written once" when debugging a session.
        """
        import hashlib
        digest = hashlib.sha256(body).hexdigest()
        now = _now_iso()
        with self._lock:
            row = self._s.execute(
                "SELECT checkpoints FROM transcripts WHERE correlationid=?",
                (correlationid,),
            ).fetchone()
            n = (row[0] if row else 0) + 1
            self._s.execute(
                "INSERT OR REPLACE INTO transcripts"
                "(correlationid,body,size,sha256,checkpoints,updated_utc)"
                " VALUES (?,?,?,?,?,?)",
                (correlationid, sqlite3.Binary(body), len(body), digest, n, now),
            )
        return {"correlationid": correlationid, "size": len(body),
                "sha256": digest, "checkpoints": n, "updated_utc": now}

    def get_transcript(self, correlationid: str) -> bytes | None:
        row = self._s.execute(
            "SELECT body FROM transcripts WHERE correlationid=?", (correlationid,)
        ).fetchone()
        return bytes(row[0]) if row else None

    def transcript_meta(self, correlationid: str) -> dict[str, Any] | None:
        row = self._s.execute(
            "SELECT correlationid,size,sha256,checkpoints,updated_utc "
            "FROM transcripts WHERE correlationid=?", (correlationid,)
        ).fetchone()
        if not row:
            return None
        keys = ("correlationid", "size", "sha256", "checkpoints", "updated_utc")
        return dict(zip(keys, row))

    # ---- subscribers (SSE) ----
    def subscribe(self, corr: str) -> threading.Event:
        ev = threading.Event()
        with self._sub_lock:
            self._subscribers.setdefault(corr, []).append(ev)
        return ev

    def unsubscribe(self, corr: str, ev: threading.Event) -> None:
        with self._sub_lock:
            lst = self._subscribers.get(corr, [])
            if ev in lst:
                lst.remove(ev)

    def _notify(self, corr: str) -> None:
        with self._sub_lock:
            for ev in self._subscribers.get(corr, []):
                ev.set()

    def close(self) -> None:
        """Close both connections. Added for Phase 3 §6.1's LRU eviction.

        Safe to call on a store that may be reopened later: a `Store` holds no state
        above SQLite — every method opens a transaction and commits — so reconstructing
        it against the same directory yields an equivalent object.

        Wakes any SSE subscriber first. `StoreRegistry` will not evict a store that has
        subscribers, so in that path this set is empty; it matters on shutdown, where a
        viewer blocked on `Event.wait()` would otherwise hang until its own timeout
        instead of noticing the process is going away.
        """
        with self._sub_lock:
            for evs in self._subscribers.values():
                for ev in evs:
                    ev.set()
        with self._lock:
            for c in (self._r, self._s):
                try:
                    c.close()
                except Exception:  # noqa: BLE001 - shutdown must not raise
                    pass


def _now_iso() -> str:
    import datetime as dt
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
