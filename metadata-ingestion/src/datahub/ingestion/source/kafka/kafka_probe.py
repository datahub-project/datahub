from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def _consumer(config: Any) -> Any:
    # Lazy import: confluent_kafka ships only with the `kafka` extra. Reusing the
    # connector's own consumer factory keeps auth/SSL behaviour identical to a run.
    from datahub.ingestion.source.kafka.kafka import get_kafka_consumer

    return get_kafka_consumer(config.connection)


def _topics(consumer: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    timeout = max(10, getattr(config.connection, "client_timeout_seconds", 10))
    return sorted(consumer.list_topics(timeout=timeout).topics.keys())


# Kafka is a flat topic namespace filtered by the connector's own topic_patterns
# (its default also denies internal ^_ topics) — one ProbeLevel, no bespoke code.
KAFKA_PROBE = ClientProbe(
    client_factory=_consumer,
    close=lambda consumer: consumer.close(),
    levels=[ProbeLevel(DatasetSubTypes.TOPIC, "topic_patterns", _topics)],
)

KAFKA_PROBE_HIERARCHY: List[ProbeNodeKind] = KAFKA_PROBE.hierarchy()


def list_kafka_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return KAFKA_PROBE.list_children(config, parent_path, limit)
