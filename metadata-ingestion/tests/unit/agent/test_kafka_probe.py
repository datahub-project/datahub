from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.subtypes import DatasetSubTypes

# The probe reuses the connector's real consumer factory, which lives in a module
# that imports confluent_kafka at import time — skip when the kafka extra is absent.
pytest.importorskip("datahub.ingestion.source.kafka.kafka")

import datahub.ingestion.source.kafka.kafka as kafka_mod
from datahub.ingestion.source.kafka.kafka_probe import (
    list_kafka_children,
)
from tests.unit.agent.probe_conformance import assert_verdicts


class _FakeConsumer:
    def __init__(self, topics):
        self.topics = {name: object() for name in topics}
        self.closed = False

    def list_topics(self, timeout=None):
        return SimpleNamespace(topics=self.topics)

    def close(self):
        self.closed = True


def _config():
    return SimpleNamespace(
        topic_patterns=AllowDenyPattern(allow=[".*"], deny=["^_.*"]),
        connection=SimpleNamespace(client_timeout_seconds=10),
    )


def test_list_topics_reuses_topic_patterns_verdict(monkeypatch):
    consumer = _FakeConsumer(["orders", "users", "_offsets"])
    monkeypatch.setattr(kafka_mod, "get_kafka_consumer", lambda conn: consumer)
    result = list_kafka_children(_config(), [], 100)
    assert result.supported
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TOPIC
    assert by_name["orders"].pattern_field == "topic_patterns"
    # The connector's own default deny (^_) drops internal topics — reused, not re-implemented.
    assert_verdicts(
        result, included=["orders"], excluded={"_offsets": "topic_patterns"}
    )
    assert consumer.closed


def test_topics_are_a_flat_level(monkeypatch):
    consumer = _FakeConsumer(["orders"])
    monkeypatch.setattr(kafka_mod, "get_kafka_consumer", lambda conn: consumer)
    # Topics have no children, so a non-empty parent path lists nothing.
    result = list_kafka_children(_config(), ["orders"], 100)
    assert result.nodes == []
