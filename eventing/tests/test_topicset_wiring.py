"""`TopicSet` threaded through the producer and consumers. DESIGN_PHASE3.md §3.2, T2.

§9's step 2 is "no behaviour change whatsoever; the existing suite is the check". That
suite is the real assertion; these tests pin the *intent* so a later change that breaks
single-mode compatibility fails with a message about compatibility rather than as a
mystery somewhere else.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from eventbridge.config import Cfg
from eventbridge.kafka_out import Producer
from shared import ce, tenancy

UK = "gh-mrsabath-4c1d9e07"


def _producer(**over):
    """A Producer with the KafkaProducer replaced, so `send` is observable."""
    with patch("eventbridge.kafka_out.KafkaProducer") as kp:
        kp.return_value = MagicMock()
        p = Producer("localhost:9092", "requests", "rossoctl://eventbridge/test",
                     response_topic="responses", **over)
    return p, p._prod


def _sent_topic(prod):
    return prod.send.call_args[0][0]


def _sent_event(prod):
    kw = prod.send.call_args[1]
    return ce.from_kafka_binary(kw["headers"], kw["value"])


# ---- single mode: unchanged ------------------------------------------------

def test_single_mode_publishes_to_the_configured_request_topic():
    p, prod = _producer()
    p.publish_request(prompt="hi", correlationid="brave-otter-4718",
                      sessionuuid=ce.session_uuid("brave-otter-4718"), mode="start")
    assert _sent_topic(prod) == "requests"


def test_single_mode_ignores_a_userkey_for_topic_selection():
    """A userkey must not change the topic in single mode — that is what makes the
    whole phase additive."""
    p, prod = _producer()
    p.publish_request(prompt="hi", correlationid="brave-otter-4718",
                      sessionuuid=ce.session_uuid("brave-otter-4718"), mode="start",
                      userkey=UK)
    assert _sent_topic(prod) == "requests"


def test_no_userkey_attribute_when_none_is_given():
    """`ce_userkey` must stay off the wire entirely for a Phase 2 deployment."""
    p, prod = _producer()
    p.publish_request(prompt="hi", correlationid="brave-otter-4718",
                      sessionuuid=ce.session_uuid("brave-otter-4718"), mode="start")
    assert ce.EXT_USERKEY not in _sent_event(prod).attrs


def test_group_events_still_go_to_the_responses_topic():
    p, prod = _producer()
    p.publish_group_event(type_=ce.TYPE_GROUP_STARTED, groupid="brave-otter-0001",
                          data={"expected": 2})
    assert _sent_topic(prod) == "responses"


# ---- multi mode -----------------------------------------------------------

def _multi_producer():
    topics = tenancy.TopicSet("kev1", tenancy=tenancy.MULTI)
    with patch("eventbridge.kafka_out.KafkaProducer") as kp:
        kp.return_value = MagicMock()
        p = Producer("localhost:9092", "requests", "rossoctl://eventbridge/test",
                     response_topic="responses", topics=topics)
    return p, p._prod


def test_multi_mode_publishes_to_the_per_user_topic():
    p, prod = _multi_producer()
    p.publish_request(prompt="hi", correlationid="brave-otter-4718",
                      sessionuuid=ce.session_uuid("brave-otter-4718"), mode="start",
                      userkey=UK)
    assert _sent_topic(prod) == f"kev1-{UK}-requests"


def test_multi_mode_stamps_the_userkey_on_the_event():
    """§2.6: the runner stamps it back onto the response, which is how EventBridge
    knows which store to file the response in."""
    p, prod = _multi_producer()
    p.publish_request(prompt="hi", correlationid="brave-otter-4718",
                      sessionuuid=ce.session_uuid("brave-otter-4718"), mode="start",
                      userkey=UK)
    assert _sent_event(prod).attrs[ce.EXT_USERKEY] == UK


def test_multi_mode_refuses_to_publish_without_a_userkey():
    """Falling back to a shared topic would run one tenant's work on another tenant's
    runner, under that tenant's credential."""
    p, _ = _multi_producer()
    with pytest.raises(ValueError, match="userkey is required"):
        p.publish_request(prompt="hi", correlationid="brave-otter-4718",
                          sessionuuid=ce.session_uuid("brave-otter-4718"),
                          mode="start")


def test_multi_mode_group_events_go_to_the_users_responses_topic():
    p, prod = _multi_producer()
    p.publish_group_event(type_=ce.TYPE_GROUP_STARTED, groupid="brave-otter-0001",
                          data={"expected": 2}, userkey=UK)
    assert _sent_topic(prod) == f"kev1-{UK}-responses"


def test_the_agent_attribute_rides_when_named():
    p, prod = _multi_producer()
    p.publish_request(prompt="hi", correlationid="brave-otter-4718",
                      sessionuuid=ce.session_uuid("brave-otter-4718"), mode="start",
                      userkey=UK, agent="triager")
    assert _sent_event(prod).attrs[ce.EXT_AGENT] == "triager"


# ---- the config property --------------------------------------------------

def test_cfg_topics_defaults_to_single_mode():
    cfg = Cfg()
    assert cfg.tenancy_mode == tenancy.SINGLE
    assert not cfg.topics.multi
    assert cfg.topics.requests() == cfg.request_topic
    assert cfg.topics.responses() == cfg.response_topic


def test_cfg_topics_tracks_a_renamed_topic():
    """The property exists so the set cannot drift from the configured names."""
    cfg = Cfg()
    cfg.request_topic = "my-requests"
    assert cfg.topics.requests() == "my-requests"


def test_cfg_topics_multi_mode_uses_the_prefix():
    cfg = Cfg()
    cfg.tenancy_mode = tenancy.MULTI
    cfg.topic_prefix = "kev9"
    assert cfg.topics.requests(UK) == f"kev9-{UK}-requests"


# ---- the runner side ------------------------------------------------------

def test_the_emitter_stamps_its_userkey_on_every_response():
    """§3.3: held on the Emitter rather than passed per call, because this pod serves
    exactly one tenant and a per-call parameter is one more thing to forget."""
    from eventrunner.emit import Emitter
    with patch("eventrunner.emit.KafkaProducer") as kp:
        kp.return_value = MagicMock()
        em = Emitter("localhost:9092", "responses", "rossoctl://eventrunner/test",
                     userkey=UK)
    em.emit(correlationid="brave-otter-4718",
            sessionuuid=ce.session_uuid("brave-otter-4718"),
            sequence=1, phase="result", final=True, data={"text": "hi"})
    assert _sent_event(em._prod).attrs[ce.EXT_USERKEY] == UK


def test_the_emitter_omits_the_userkey_in_single_mode():
    from eventrunner.emit import Emitter
    with patch("eventrunner.emit.KafkaProducer") as kp:
        kp.return_value = MagicMock()
        em = Emitter("localhost:9092", "responses", "rossoctl://eventrunner/test")
    em.emit(correlationid="brave-otter-4718",
            sessionuuid=ce.session_uuid("brave-otter-4718"),
            sequence=1, phase="result", final=True, data={"text": "hi"})
    assert ce.EXT_USERKEY not in _sent_event(em._prod).attrs


def test_the_runner_refuses_a_non_default_request_topic_without_a_userkey(monkeypatch):
    """§3.3: a runner that stamps no userkey produces events EventBridge cannot
    attribute, and a silent default would route one user's output into another's store."""
    from eventrunner import config as ercfg
    monkeypatch.setenv("REQUEST_TOPIC", f"kev1-{UK}-requests")
    monkeypatch.delenv("ER_USERKEY", raising=False)
    with pytest.raises(SystemExit, match="ER_USERKEY is required"):
        ercfg.load()


def test_the_runner_accepts_a_per_user_topic_with_a_userkey(monkeypatch):
    from eventrunner import config as ercfg
    monkeypatch.setenv("REQUEST_TOPIC", f"kev1-{UK}-requests")
    monkeypatch.setenv("ER_USERKEY", UK)
    cfg = ercfg.load()
    assert cfg.userkey == UK


def test_the_runner_default_topic_needs_no_userkey(monkeypatch):
    """Phase 2's deployment, unchanged."""
    from eventrunner import config as ercfg
    monkeypatch.delenv("REQUEST_TOPIC", raising=False)
    monkeypatch.delenv("ER_USERKEY", raising=False)
    assert ercfg.load().userkey == ""
