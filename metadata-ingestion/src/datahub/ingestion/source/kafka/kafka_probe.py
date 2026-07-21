from typing import Any, Callable, List

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import Verdict, container_nodes
from datahub.ingestion.source.common.subtypes import DatasetSubTypes

# Kafka is a flat topic namespace — a single probeable level filtered by
# topic_patterns. No SQL-style container tree; the existing probe interface fits
# as-is once the connector opts in.
KAFKA_PROBE_HIERARCHY: List[ProbeNodeKind] = [DatasetSubTypes.TOPIC]


def _topic_classifier(config: Any) -> Callable[[str, str], Verdict]:
    # Reuse the connector's OWN topic filter so the probe's included/excluded_by
    # verdict matches exactly what ingestion would keep (the default pattern also
    # denies internal ^_ topics).
    topic_patterns = config.topic_patterns

    def classify(name: str, node_fqn: str) -> Verdict:
        if not topic_patterns.allowed(name):
            return (False, "topic_patterns")
        return (True, None)

    return classify


def list_kafka_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    # Topics are the only level, so a non-empty parent path has no children.
    if parent_path:
        return ProbeResult(source_type="", supported=True, parent_path=parent_path)

    # Lazy import: confluent_kafka ships only with the `kafka` extra, and reusing
    # the connector's own consumer factory keeps auth/SSL behaviour identical to a
    # real run.
    from datahub.ingestion.source.kafka.kafka import get_kafka_consumer

    consumer = get_kafka_consumer(config.connection)
    try:
        timeout = max(10, getattr(config.connection, "client_timeout_seconds", 10))
        metadata = consumer.list_topics(timeout=timeout)
        names = sorted(metadata.topics.keys())
        nodes, truncated = container_nodes(
            names,
            limit,
            DatasetSubTypes.TOPIC,
            "topic_patterns",
            classify=_topic_classifier(config),
        )
        return ProbeResult(
            source_type="",
            supported=True,
            parent_path=parent_path,
            nodes=nodes,
            truncated=truncated,
        )
    finally:
        consumer.close()
