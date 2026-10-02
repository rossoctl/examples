"""`userkey` and `depth` inside the signed attribute set. DESIGN_PHASE3.md §2.6, §7.7, §8.3.

Why these two have to be signed rather than merely present on the event:

* `userkey` decides which store a response is written into and which ntfy topic
  announces it. An unsigned, mutable one lets anything with write access to a topic file
  an event into another user's history.
* `depth` is the hop limit. A hop limit an attacker can reset does not limit hops, and
  §7.7 is the control the whole trigger API lives or dies by.

§8.3 requires both to land in ONE change, because adding an attribute changes
canonicalisation and a signer and verifier on different versions disagree about every
signature.
"""
from __future__ import annotations

import binascii

import pytest

from shared import ce, keyset
from shared import signing as S

SEED = binascii.unhexlify(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
PUB = S.public_key(SEED)

CORR = "brave-otter-4718"
UK = "gh-mrsabath-4c1d9e07"


def _request(**over):
    attrs = {"type": ce.TYPE_REQUEST, "source": "rossoctl://eventbridge/test",
             "datacontenttype": "application/json",
             ce.EXT_CORRELATIONID: CORR,
             ce.EXT_SESSIONUUID: ce.session_uuid(CORR),
             ce.EXT_MODE: "start"}
    attrs.update(over)
    return ce.new_event(**attrs, data={"prompt": "hello"})


def test_both_attributes_are_in_the_signed_set():
    """§8.3: one change, both attributes."""
    assert ce.EXT_USERKEY in S.SIGNED_ATTRS
    assert ce.EXT_DEPTH in S.SIGNED_ATTRS


def test_tampering_with_the_userkey_invalidates_the_signature():
    """The attack this exists to stop: filing an event into another user's history."""
    event = _request(**{ce.EXT_USERKEY: UK})
    S.sign_into(event, SEED, "eb-01")
    ks = keyset.KeySet({"eb-01": PUB})
    assert S.verify_with_keyset(event, ks)[0]

    event.attrs[ce.EXT_USERKEY] = "gh-victim-0bad0bad"
    ok, why = S.verify_with_keyset(event, ks)
    assert not ok, why


def test_removing_the_userkey_invalidates_the_signature():
    """Stripping the attribute must not be a way around covering it — otherwise an
    attacker downgrades a tenanted event to an untenanted one, which §6.1 files into
    the `shared` store."""
    event = _request(**{ce.EXT_USERKEY: UK})
    S.sign_into(event, SEED, "eb-01")
    ks = keyset.KeySet({"eb-01": PUB})
    del event.attrs[ce.EXT_USERKEY]
    assert not S.verify_with_keyset(event, ks)[0]


def test_adding_a_userkey_to_an_unkeyed_signed_event_invalidates_it():
    """The reverse direction: a single-tenant event must not be promotable into
    somebody's tenant."""
    event = _request()
    S.sign_into(event, SEED, "eb-01")
    ks = keyset.KeySet({"eb-01": PUB})
    assert S.verify_with_keyset(event, ks)[0]
    event.attrs[ce.EXT_USERKEY] = UK
    assert not S.verify_with_keyset(event, ks)[0]


def test_tampering_with_the_depth_invalidates_the_signature():
    """§7.7: a resettable hop count is not a hop limit."""
    event = _request(**{ce.EXT_USERKEY: UK, ce.EXT_DEPTH: "3"})
    S.sign_into(event, SEED, "eb-01")
    ks = keyset.KeySet({"eb-01": PUB})
    assert S.verify_with_keyset(event, ks)[0]
    event.attrs[ce.EXT_DEPTH] = "0"
    assert not S.verify_with_keyset(event, ks)[0]


def test_a_signed_event_survives_the_kafka_wire_with_both_attributes():
    event = _request(**{ce.EXT_USERKEY: UK, ce.EXT_DEPTH: "2"})
    S.sign_into(event, SEED, "eb-01")
    headers, value = ce.to_kafka_binary(event)
    back = ce.from_kafka_binary(headers, value)
    assert back.attrs[ce.EXT_USERKEY] == UK
    assert back.attrs[ce.EXT_DEPTH] == "2"
    assert S.verify_with_keyset(back, keyset.KeySet({"eb-01": PUB}))[0]


@pytest.mark.parametrize("attr", [ce.EXT_USERKEY, ce.EXT_DEPTH, ce.EXT_AGENT,
                                  ce.EXT_TRIGGERID])
def test_new_attribute_names_obey_the_cloudevents_character_rule(attr):
    """CloudEvents v1.0 allows lower-case `[a-z0-9]` only in an attribute name. Phase 2
    §2.6 got this wrong once already with `submitter_iss`, and the codec would not have
    caught it: `to_kafka_binary` just prefixes `ce_` and validates nothing, so a bad
    name round-trips here and is dropped by a spec-compliant consumer a hop later."""
    assert attr.isascii() and attr.islower() and attr.isalnum()


def test_an_absent_attribute_is_omitted_not_signed_as_empty():
    """`canonical` omits absent attributes, which is what lets a single-tenant event
    (no userkey, no depth) verify against a build that knows about both."""
    no_uk = S.canonical(_request().attrs, {"prompt": "hello"})
    assert b"userkey" not in no_uk
    assert b"depth" not in no_uk
