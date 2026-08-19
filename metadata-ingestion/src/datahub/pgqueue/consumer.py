"""Poll pgQueue tables and deserialize Confluent Avro payloads like Kafka consumers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Union, cast

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from datahub.emitter.kafka_emitter import MCE_KEY, MCP_KEY
from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass as MetadataChangeEvent,
    MetadataChangeLogClass,
    MetadataChangeProposalClass as MetadataChangeProposal,
)
from datahub.pgqueue.compression import decode_stored, from_wire
from datahub.pgqueue.config import PgQueueConsumerConfig
from datahub.pgqueue.connection import consumer_lock_owner, create_pgqueue_connection
from datahub.pgqueue.priority_bands import resolve_priority_bands_json
from datahub.pgqueue.repository import (
    PgQueueMessageHandle,
    PgQueueReceivedMessage,
    PgQueueRepository,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PgQueueConsumedRecord:
    """One leased message plus a deserialized Avro payload (MCP/MCE/MCL or PE dict)."""

    route_key: str
    topic_name: str
    raw: PgQueueReceivedMessage
    record: Union[
        MetadataChangeEvent,
        MetadataChangeProposal,
        MetadataChangeLogClass,
        Dict[str, Any],
    ]


def build_pg_queue_event_meta(rec: PgQueueConsumedRecord) -> Dict[str, Any]:
    """Build ``EventEnvelope.meta`` so consumers can ack via :class:`PgQueueMessageHandle`."""
    h = rec.raw.handle
    return {
        "pg_queue": {
            "topic": rec.topic_name,
            "partition": h.partition_id,
            "enqueue_seq": h.enqueue_seq,
            "routing_key": rec.raw.routing_key,
            "handle": h.to_meta_dict(),
        }
    }


class DatahubPgQueueConsumer(Closeable):
    """Poll partitions under a logical Kafka-style topic name stored in pgQueue."""

    def __init__(
        self,
        config: PgQueueConsumerConfig,
        server_config: Optional[Dict[str, Any]] = None,
    ):
        self.config = config
        qconf = config.queue
        self._repo = PgQueueRepository(qconf.queue_schema, qconf.table_prefix)
        self._conn = create_pgqueue_connection(qconf)
        self._lock_owner = consumer_lock_owner(config.consumer_group)
        self._priority_bands_json = resolve_priority_bands_json(
            server_config=server_config,
            env_override=None,
        )

        sr_conf = {"url": config.schema_registry_url, **config.schema_registry_config}
        registry = SchemaRegistryClient(sr_conf)
        self._avro_deserializer = AvroDeserializer(
            schema_registry_client=registry,
            return_record_name=True,
        )

    def visibility_timedelta(self) -> timedelta:
        secs = self.config.visibility_timeout_seconds
        if secs is None:
            secs = self.config.queue.topic_defaults.visibility_timeout_seconds
        return timedelta(seconds=float(secs))

    def poll_route_key(
        self,
        route_key: str,
        *,
        max_messages: int = 100,
        partition_ids: Optional[Sequence[int]] = None,
    ) -> List[PgQueueConsumedRecord]:
        """Drain up to ``max_messages`` messages with acquired per-group leases."""
        if route_key not in self.config.topic_routes:
            raise ValueError(f"route_key {route_key!r} missing from topic_routes")

        topic_name = self.config.topic_routes[route_key]
        meta = self._repo.fetch_topic_row(self._conn, topic_name)
        if meta is None:
            logger.debug("pgQueue topic %s does not exist yet", topic_name)
            return []

        topic_id, partition_count, _default_content_type_id = meta
        self._repo.register_consumer(self._conn, self.config.consumer_group, topic_id)
        parts_tuple = tuple(
            int(pid)
            for pid in (
                partition_ids if partition_ids is not None else range(partition_count)
            )
        )

        vis = self.visibility_timedelta()
        locked = self._repo.receive_batch_for_group(
            self._conn,
            consumer_group=self.config.consumer_group,
            topic_id=topic_id,
            partition_ids=parts_tuple,
            lock_owner=self._lock_owner,
            visibility_timeout=vis,
            max_messages=max_messages,
            priority_bands_json=self._priority_bands_json,
        )

        out: List[PgQueueConsumedRecord] = []
        for raw in locked:
            inner = decode_stored(raw.payload, from_wire(raw.payload_compression))
            record = self._deserialize(route_key, topic_name, inner)
            out.append(
                PgQueueConsumedRecord(
                    route_key=route_key,
                    topic_name=topic_name,
                    raw=raw,
                    record=record,
                )
            )

        return out

    def poll_route_keys(
        self,
        route_keys: Sequence[str],
        *,
        max_messages: int = 100,
        partition_ids: Optional[Sequence[int]] = None,
    ) -> List[PgQueueConsumedRecord]:
        """Drain each route key in order, up to ``max_messages`` per key."""
        out: List[PgQueueConsumedRecord] = []
        for rk in route_keys:
            out.extend(
                self.poll_route_key(
                    rk, max_messages=max_messages, partition_ids=partition_ids
                )
            )
        return out

    def ack(self, handles: Sequence[PgQueueMessageHandle]) -> int:
        """Advance consumer group offsets for the given handles."""
        return self._repo.commit_for_group(
            self._conn,
            self.config.consumer_group,
            handles,
        )

    def ack_records(self, records: Sequence[PgQueueConsumedRecord]) -> int:
        return self.ack([r.raw.handle for r in records])

    def extend_visibility(
        self,
        handles: Sequence[PgQueueMessageHandle],
        *,
        extend_by: timedelta,
    ) -> int:
        return self._repo.extend_visibility_for_group(
            self._conn,
            self.config.consumer_group,
            handles,
            lock_owner=self._lock_owner,
            extend_by=extend_by,
        )

    def _deserialize(
        self, route_key: str, topic_name: str, payload: bytes
    ) -> Union[
        MetadataChangeEvent,
        MetadataChangeProposal,
        MetadataChangeLogClass,
        Dict[str, Any],
    ]:
        kind = self.config.payload_kind_by_route_key.get(route_key, "mcp")
        ctx = SerializationContext(topic_name, MessageField.VALUE)
        deserialized = self._avro_deserializer(payload, ctx)
        if kind == "mcp":
            return MetadataChangeProposal.from_obj(deserialized, tuples=True)
        if kind == "mce":
            return MetadataChangeEvent.from_obj(deserialized, tuples=True)
        if kind == "mcl":
            return MetadataChangeLogClass.from_obj(deserialized, tuples=True)
        if kind == "pe":
            return cast(Dict[str, Any], deserialized)
        raise ValueError(f"Unsupported payload kind {kind!r} for route {route_key}")

    def lock_owner(self) -> str:
        return self._lock_owner

    def close(self) -> None:
        self._conn.close()


def poll_mcp_messages(
    consumer: DatahubPgQueueConsumer,
    *,
    max_messages: int = 100,
    partition_ids: Optional[Sequence[int]] = None,
) -> List[PgQueueConsumedRecord]:
    """Convenience wrapper matching default MCP-focused ingestion."""
    return consumer.poll_route_key(
        MCP_KEY, max_messages=max_messages, partition_ids=partition_ids
    )


def poll_mce_messages(
    consumer: DatahubPgQueueConsumer,
    *,
    max_messages: int = 100,
    partition_ids: Optional[Sequence[int]] = None,
) -> List[PgQueueConsumedRecord]:
    return consumer.poll_route_key(
        MCE_KEY, max_messages=max_messages, partition_ids=partition_ids
    )
