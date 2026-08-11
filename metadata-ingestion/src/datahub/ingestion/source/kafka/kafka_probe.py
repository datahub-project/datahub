from typing import Any, Dict, List

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


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

    @probe_method(kind=DatasetSubTypes.TOPIC, row_limit_param="limit")
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
        # request_timeout mirrors kafka.py's own fetch_topic_configurations --
        # without it only the client-side future.result(timeout=) below
        # bounds the call, leaving the broker-side request itself unbounded.
        future = self._admin.describe_configs(
            [resource], request_timeout=self._timeout
        )[resource]
        entries = future.result(timeout=self._timeout)
        return {name: entry.value for name, entry in entries.items()}

    @probe_method(row_limit_param="limit")
    def consumer_groups(self, limit: int = 200) -> List[str]:
        """Consumer-group ids known to the cluster."""
        future = self._admin.list_consumer_groups(request_timeout=self._timeout)
        listing = future.result(timeout=self._timeout)
        groups = [g.group_id for g in getattr(listing, "valid", [])]
        return sorted(groups)[:limit]

    @probe_method(row_limit_param="limit")
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
