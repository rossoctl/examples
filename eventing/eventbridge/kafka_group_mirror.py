"""Rebuild group state from the responses topic on every start. DESIGN_PHASE1 §21.2.

§21.2 claims the group lifecycle is "replayable from Kafka alone". It was not, in
practice: the live responses consumer uses a fixed group id and commits offsets, so
after a restart it resumes where it left off and never re-reads anything. With the test
overlay's `emptyDir` at /data that meant a new pod started with an empty store and every
earlier group returned 404 — while its two notifications had already been delivered, so
clicking one landed on "unknown groupid".

The same problem was already solved for prompts by `kafka_requests_mirror.py`, which
re-scans on every start. This does the same for groups, reading from the beginning to a
snapshot of the end offsets.

Two rules make the replay safe:

  * **It never publishes and never notifies.** A replay rebuilds state; it does not
    create new facts. `replay=True` suppresses the group-completed publish, which would
    otherwise send a duplicate notification for a batch that finished hours ago.
  * **It is one-shot.** It scans to the end of the topic and stops, rather than shadowing
    the live consumer forever. The live consumer owns everything from then on.

The one case where it does publish is a group the topic leaves unfinished: if EventBridge
died between the last member's terminal event and the `group.completed` it should have
written, that completion is missing from the topic and settling it is a new fact, not a
duplicate. `GroupService.maybe_complete` is once-only, so a group already completed on
the topic stays quiet.

Only group rows are rebuilt — `groups` and `group_members`. Response events are left to
the committing consumer, because re-inserting every response on every start is the cost
the committing consumer exists to avoid.
"""
from __future__ import annotations

import threading

from kafka import KafkaConsumer, TopicPartition

from shared import ce, tenancy


def _log(msg: str) -> None:
    print(f"[group-mirror] {msg}", flush=True)


class GroupMirror(threading.Thread):
    """One-shot catch-up scan of the responses topic, rebuilding group state."""

    def __init__(self, bootstrap: str, response_topic: str, groups,
                 metadata_tries: int = 20, max_empty_polls: int = 15,
                 topics: tenancy.TopicSet | None = None,
                 userkeys: tuple[str, ...] = (),
                 owners=None) -> None:
        super().__init__(daemon=True, name="kafka-group-mirror")
        self._bootstrap_servers = bootstrap
        # §3.2: unlike the live consumers this one is one-shot at startup, so it "can
        # take the full list directly" — there is no new-user race to lose, because a
        # user who did not exist when it ran has no group history to rebuild.
        self._topics = topics or tenancy.TopicSet(
            "", response_topic=response_topic, request_topic="")
        if self._topics.multi:
            self._all_topics = tuple(self._topics.responses(uk) for uk in userkeys)
        else:
            self._all_topics = (self._topics.responses(),)
        # Kept for the log lines and the single-topic metadata path below.
        self._topic = self._all_topics[0] if self._all_topics else response_topic
        self._groups = groups
        # §2.6's ownership index. Used to resolve which tenant a replayed group belongs
        # to, both for settling and for back-filling a missing `userkey`. `None` keeps
        # the single-tenant behaviour, where there is one store and nothing to resolve.
        self._owners = owners
        # Partitions are assigned by hand and nothing is ever committed: the whole
        # point is to re-read from the beginning on every start, which the live
        # committing consumer cannot do. See run() for why there is no group id.
        self._metadata_tries = metadata_tries
        self._max_empty_polls = max_empty_polls
        self._stopping = threading.Event()
        self.total_to_read = 0
        self.rebuilt_groups = 0
        self.rebuilt_members = 0
        self.settled = 0

    def stop(self) -> None:
        self._stopping.set()

    def _partitions(self, c):
        """Topic metadata is fetched lazily, so the first call can legitimately return
        None. Retry briefly rather than concluding the topic is empty.

        With several topics (multi mode) the retry is over the whole set, and a topic
        that never resolves is skipped with a log line rather than failing the rebuild:
        one tenant whose topic has not been provisioned yet must not cost every other
        tenant their group history."""
        found: list[TopicPartition] = []
        for topic in self._all_topics:
            for _ in range(self._metadata_tries):
                parts = c.partitions_for_topic(topic)
                if parts:
                    found += [TopicPartition(topic, p) for p in sorted(parts)]
                    break
                if self._stopping.wait(0.5):
                    return found
            else:
                if len(self._all_topics) > 1:
                    _log(f"no metadata for {topic!r}; skipping it")
        return found

    def run(self) -> None:
        # No consumer group: we assign every partition by hand. A group would make this
        # wait for a coordinator and a rebalance, and an earlier version that treated
        # "three empty polls" as caught-up read 0 events for exactly that reason — the
        # empty polls were the join, not the end of the topic. End offsets are a
        # snapshot taken once: catch up to "now", then leave the rest to the live
        # consumer, which owns the topic from then on.
        try:
            c = KafkaConsumer(
                bootstrap_servers=self._bootstrap_servers,
                enable_auto_commit=False,
                group_id=None,
                consumer_timeout_ms=1000,
            )
        except Exception as e:  # noqa: BLE001 - a missing broker must not kill startup
            _log(f"could not start ({type(e).__name__}: {e}); group history not rebuilt")
            return
        touched: set[str] = set()
        try:
            tps = self._partitions(c)
            if not tps:
                _log(f"no metadata for {self._topic!r}; group history not rebuilt")
                return
            c.assign(tps)
            ends = c.end_offsets(tps)
            c.seek_to_beginning(*tps)
            pending = {tp for tp in tps if c.position(tp) < ends[tp]}
            self.total_to_read = sum(ends[tp] - c.position(tp) for tp in pending)
            if not pending:
                _log(f"{self._topic} is empty across {len(tps)} partition(s)")
                return
            _log(f"replaying {self.total_to_read} record(s) across "
                 f"{len(pending)}/{len(tps)} non-empty partition(s)")
            empty = 0
            while pending and not self._stopping.is_set():
                batch = c.poll(timeout_ms=1000)
                if not batch:
                    # A gap is possible (aborted transactions, deleted segments), so
                    # trust the positions over the absence of records.
                    empty += 1
                    if empty > self._max_empty_polls:
                        _log(f"gave up waiting on {len(pending)} partition(s) after "
                             f"{empty} empty polls")
                        break
                else:
                    empty = 0
                    for _tp, records in batch.items():
                        for rec in records:
                            gid = self._apply(rec)
                            if gid:
                                touched.add(gid)
                pending = {tp for tp in pending if c.position(tp) < ends[tp]}
        except Exception as e:  # noqa: BLE001
            _log(f"replay aborted ({type(e).__name__}: {e}); "
                 f"applied {self.rebuilt_groups + self.rebuilt_members} event(s) so far")
        finally:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass
        self._settle(touched)
        _log(f"rebuilt {len(touched)} group(s) from {self.rebuilt_groups} group event(s) "
             f"and {self.rebuilt_members} member event(s)"
             + (f"; settled {self.settled} left unfinished by a restart"
                if self.settled else ""))

    def _settle(self, touched: set[str]) -> None:
        """Complete any group whose members all finished but whose `group.completed` is
        not on the topic — the crash window between the two. Publishing is correct here
        and `maybe_complete` will not fire twice for one already completed.

        The owning tenant is resolved per group. Without that, `maybe_complete` looked in
        the shared store for a group that lives in a tenant's store, found nothing, and
        returned False — so in multi-tenant mode every group a restart left unfinished
        stayed unfinished forever, and the failure mode is silence (§21.6's worst one).
        """
        for gid in sorted(touched):
            try:
                if self._groups.maybe_complete(gid, userkey=self._owner_of(gid)):
                    self.settled += 1
                    _log(f"{gid} was left unfinished by a restart; completed it now")
            except Exception as e:  # noqa: BLE001
                _log(f"could not settle {gid}: {e!r}")

    def _owner_of(self, correlationid: str) -> str | None:
        """Which tenant owns this id, from the ownership index. `None` without one."""
        if self._owners is None:
            return None
        userkey, _known = self._owners.owner_of(correlationid)
        return userkey

    def _apply(self, rec) -> str | None:
        try:
            evt = ce.from_kafka_binary(rec.headers or [], rec.value)
        except Exception:  # noqa: BLE001
            return None
        d = ce.envelope_dict(evt)
        gid = d.get("groupid")
        if not gid:
            return None
        # Back-fill the tenancy key for events published BEFORE Phase 3, which carry no
        # `ce_userkey` at all. Without this, replayed member rows go to `shared/` while
        # the group row sits in the tenant's store, so the group's counts are permanently
        # wrong and `_decorate_member` reads from a third place. Pre-existing Kafka
        # history is exactly what this mirror exists to replay, so that is the normal
        # upgrade path rather than an edge case.
        #
        # Only ever FILLS a missing value — an event that carries its own `userkey` keeps
        # it, because that one is signed (§2.6) and the index is not authoritative over a
        # signed attribute.
        if not d.get(ce.EXT_USERKEY):
            owner = self._owner_of(d.get("correlationid") or gid)
            if owner:
                d[ce.EXT_USERKEY] = owner
        try:
            if ce.is_group_event(evt):
                self._groups.on_group_event(d, replay=True)
                self.rebuilt_groups += 1
            else:
                self._groups.on_member_event(d, replay=True)
                self.rebuilt_members += 1
        except Exception as e:  # noqa: BLE001
            _log(f"could not apply an event for {gid}: {e!r}")
            return None
        return gid
