from typing import Any, Dict, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.agent.probe_methods import probe_method
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
    levels=[ProbeLevel(DatasetSubTypes.TOPIC, list_names=_topics)],
)

KAFKA_PROBE_HIERARCHY: List[ProbeNodeKind] = KAFKA_PROBE.hierarchy()


def list_kafka_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return KAFKA_PROBE.list_children(config, parent_path, limit)


class KafkaMetadataProbe:
    """Metadata-only probe methods over a Kafka cluster + Schema Registry.

    Returns topic/partition metadata, topic configs, consumer-group ids, and
    registry subjects/schemas — never message payloads.
    """

    def __init__(
        self, consumer: Any, admin: Any, registry: Any, timeout: int = 10
    ) -> None:
        self._consumer = consumer
        self._admin = admin
        self._registry = registry
        self._timeout = timeout

    def __enter__(self) -> "KafkaMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        close = getattr(self._consumer, "close", None)
        if callable(close):
            close()

    @probe_method()
    def topics(self, limit: int = 500) -> List[Dict[str, object]]:
        """Live topics on the broker with partition and replication counts.
        Internal topics (name starting with '__') are hidden. Metadata only —
        never message contents."""
        meta = self._consumer.list_topics(timeout=self._timeout)
        out: List[Dict[str, object]] = []
        for name, tmeta in sorted(meta.topics.items()):
            if name.startswith("__"):
                continue
            parts = tmeta.partitions
            replication = max((len(p.replicas) for p in parts.values()), default=0)
            out.append(
                {"name": name, "partitions": len(parts), "replication": replication}
            )
        return out[:limit]

    @probe_method()
    def topic_config(self, topic: str) -> Dict[str, object]:
        """Broker-side configuration for one topic (retention.ms, cleanup.policy,
        max.message.bytes, ...)."""
        # lazy: confluent_kafka.admin is a heavy optional dep, only needed here
        from confluent_kafka.admin import ConfigResource

        resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        future = self._admin.describe_configs([resource])[resource]
        entries = future.result(timeout=self._timeout)
        return {name: entry.value for name, entry in entries.items()}

    @probe_method()
    def consumer_groups(self, limit: int = 200) -> List[str]:
        """Consumer-group ids known to the cluster."""
        future = self._admin.list_consumer_groups(request_timeout=self._timeout)
        listing = future.result(timeout=self._timeout)
        groups = [g.group_id for g in getattr(listing, "valid", [])]
        return sorted(groups)[:limit]

    @probe_method()
    def subjects(self, limit: int = 500) -> List[str]:
        """Schema Registry subjects (the schemas that map to topics)."""
        return sorted(self._registry.get_subjects())[:limit]

    @probe_method()
    def schema(self, subject: str, version: str = "latest") -> Dict[str, object]:
        """A subject's registered schema (Avro/Protobuf/JSON) — the shape, not
        the data. Use 'latest' or an explicit version number."""
        rv = (
            self._registry.get_latest_version(subject)
            if version == "latest"
            else self._registry.get_version(subject, int(version))
        )
        schema_obj = rv.schema
        return {
            "subject": subject,
            "version": rv.version,
            "id": rv.schema_id,
            "schema_type": getattr(schema_obj, "schema_type", "AVRO"),
            "schema_str": schema_obj.schema_str,
        }


def build_registry_client(config: Any) -> Any:
    # lazy: confluent_kafka.schema_registry is an optional dep
    from confluent_kafka.schema_registry import SchemaRegistryClient

    conn = config.connection
    return SchemaRegistryClient(
        {"url": conn.schema_registry_url, **conn.schema_registry_config}
    )
