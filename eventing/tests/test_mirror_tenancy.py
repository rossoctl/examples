"""GroupMirror replay in multi-tenant mode. DESIGN_PHASE3.md §6.1, §21.2.

The mirror is what makes §21.2's "replayable from Kafka alone" claim true, and it is the
normal upgrade path: the history it replays was published before Phase 3 existed, so none
of it carries `ce_userkey`. Both bugs covered here come from that — the mirror knew about
per-user topics but not about which tenant a replayed group belonged to.
"""
from __future__ import annotations

import json
import pathlib
from unittest.mock import MagicMock

from eventbridge import registry
from eventbridge.config import Cfg
from eventbridge.correlation import Minter
from eventbridge.group_service import GroupService
from eventbridge.kafka_group_mirror import GroupMirror
from eventbridge.owner_index import OwnerIndex
from eventbridge.store_registry import StoreRegistry
from shared import ce, tenancy

OWNER = "alice"
UK = tenancy.userkey("github", OWNER)


def _svc(tmp_path: pathlib.Path):
    cfg = Cfg()
    cfg.tmpdir = str(tmp_path)
    cfg.tenancy_mode = tenancy.MULTI
    root = tmp_path / "eventbridge"
    owners = OwnerIndex(root)
    stores = StoreRegistry(root, multi=True)
    producer = MagicMock()
    minter = Minter(index=owners)
    svc = GroupService(cfg, stores.for_userkey(None), producer, minter, stores=stores)
    return svc, owners, stores, producer


def _reg():
    return registry.parse(json.dumps({
        "version": 1,
        "users": [{"issuer": "github", "userid": OWNER, "tier": "isolated"}]}))


class _Rec:
    """A Kafka record carrying a CloudEvent, as the mirror's `_apply` expects."""

    def __init__(self, event):
        self.headers, self.value = ce.to_kafka_binary(event)
        self.topic, self.partition, self.offset = "kev1-x-responses", 0, 0


def _member_event(corr, groupid, *, userkey=None, final=True):
    attrs = {"type": ce.TYPE_RESPONSE, "source": "rossoctl://eventrunner/test",
             "datacontenttype": "application/json",
             ce.EXT_CORRELATIONID: corr, ce.EXT_SESSIONUUID: ce.session_uuid(corr),
             ce.EXT_SEQUENCE: "1", ce.EXT_PHASE: "result",
             ce.EXT_FINAL: "true" if final else "false",
             ce.EXT_GROUPID: groupid}
    if userkey:
        attrs[ce.EXT_USERKEY] = userkey
    return ce.new_event(**attrs, data={"text": "done"})


# ---- bug 1: settling completes against the owning tenant's store ----------

def test_a_group_left_unfinished_is_settled_in_the_owning_tenants_store(tmp_path):
    """Regression. `_settle` used to call `maybe_complete(gid)` with no userkey, so it
    looked in the SHARED store for a group that lives in a tenant's store, found nothing,
    and returned False — leaving every restart-orphaned group unfinished forever, with
    silence as the symptom (§21.6's worst failure mode)."""
    svc, owners, stores, _ = _svc(tmp_path)
    gid, _ = svc.create(label="b", expected=1, userkey=UK)
    corrs = svc.submit_members(gid, ["p1"], userkey=UK)
    # The member finished on the topic but `group.completed` never got written.
    svc.on_member_event(_member_event(corrs[0], gid, userkey=UK), replay=True)

    mirror = GroupMirror("localhost:9092", "responses", svc,
                         topics=tenancy.TopicSet("kev1", tenancy=tenancy.MULTI),
                         userkeys=(UK,), owners=owners)
    mirror._settle({gid})
    assert mirror.settled == 1
    assert stores.for_userkey(UK).get_group(gid)["completed_utc"]


def test_settling_without_an_index_still_works_single_tenant(tmp_path):
    """Single-tenant mode has one store and nothing to resolve."""
    cfg = Cfg()
    cfg.tmpdir = str(tmp_path)
    root = tmp_path / "eventbridge"
    stores = StoreRegistry(root, multi=False)
    store = stores.for_userkey(None)
    svc = GroupService(cfg, store, MagicMock(), Minter())
    gid, _ = svc.create(label="b", expected=1)
    corrs = svc.submit_members(gid, ["p1"])
    svc.on_member_event(_member_event(corrs[0], gid), replay=True)

    mirror = GroupMirror("localhost:9092", "responses", svc)
    mirror._settle({gid})
    assert mirror.settled == 1
    assert store.get_group(gid)["completed_utc"]


# ---- bug 2: a pre-Phase-3 event gets its userkey back-filled --------------

def test_a_replayed_event_without_a_userkey_is_attributed_from_the_index(tmp_path):
    """Regression. Events published before Phase 3 carry no `ce_userkey`, so replayed
    member rows went to `shared/` while the group row sat in the tenant's store — the
    group's counts then permanently wrong. Pre-existing history is exactly what the
    mirror replays, so this is the upgrade path, not an edge case."""
    svc, owners, stores, _ = _svc(tmp_path)
    gid, _ = svc.create(label="b", expected=1, userkey=UK)
    corrs = svc.submit_members(gid, ["p1"], userkey=UK)

    mirror = GroupMirror("localhost:9092", "responses", svc,
                         topics=tenancy.TopicSet("kev1", tenancy=tenancy.MULTI),
                         userkeys=(UK,), owners=owners)
    # No userkey on the wire, as a Phase 2 runner would have published it.
    mirror._apply(_Rec(_member_event(corrs[0], gid, userkey=None)))

    assert stores.for_userkey(UK).group_members(gid), "member row missing from the tenant"
    assert not stores.for_userkey(None).group_members(gid), "leaked into shared/"


def test_a_signed_userkey_on_the_event_is_never_overridden(tmp_path):
    """The index only FILLS a missing value. `userkey` is signed (§2.6), so the index is
    not authoritative over what the event itself asserts."""
    svc, owners, stores, _ = _svc(tmp_path)
    gid, _ = svc.create(label="b", expected=1, userkey=UK)
    corrs = svc.submit_members(gid, ["p1"], userkey=UK)
    other = tenancy.userkey("github", "bob")

    mirror = GroupMirror("localhost:9092", "responses", svc,
                         topics=tenancy.TopicSet("kev1", tenancy=tenancy.MULTI),
                         userkeys=(UK, other), owners=owners)
    mirror._apply(_Rec(_member_event(corrs[0], gid, userkey=other)))
    # Followed the event, not the index.
    assert stores.for_userkey(other).group_members(gid)


def test_an_unknown_correlation_is_left_unattributed(tmp_path):
    """Nothing to back-fill from: it goes to `shared/` rather than being guessed."""
    svc, owners, stores, _ = _svc(tmp_path)
    mirror = GroupMirror("localhost:9092", "responses", svc,
                         topics=tenancy.TopicSet("kev1", tenancy=tenancy.MULTI),
                         userkeys=(UK,), owners=owners)
    mirror._apply(_Rec(_member_event("brave-otter-9999", "brave-otter-9998")))
    assert stores.for_userkey(None).group_members("brave-otter-9998")
