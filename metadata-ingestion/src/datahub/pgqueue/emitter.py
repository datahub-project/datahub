"""Kafka-compatible Avro producer backed by pgQueue tables."""

from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.kafka_emitter import (
    MCP_KEY,
    KafkaEmitterConfig,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass as MetadataChangeEvent,
    MetadataChangeProposalClass as MetadataChangeProposal,
)
from datahub.metadata.schemas import (
    getMetadataChangeEventSchema,
    getMetadataChangeProposalSchema,
)
from datahub.pgqueue.compression import (
    PgQueuePayloadCompression,
    encode_inner,
)
from datahub.pgqueue.config import PgQueueEmitterConfig
from datahub.pgqueue.connection import create_pgqueue_connection, flush_pg_connection
from datahub.pgqueue.repository import EnqueueBatchItem, PgQueueRepository

logger = logging.getLogger(__name__)


class DatahubPgQueueEmitter(Closeable, Emitter):
    """Serialize MCP/MCE like ``DatahubKafkaEmitter`` but persist bytes to PostgreSQL."""

    def __init__(self, config: PgQueueEmitterConfig):
        self.config = config
        qconf = config.queue
        self._repo = PgQueueRepository(qconf.queue_schema, qconf.table_prefix)

        schema_registry_conf = {
            "url": config.schema_registry_url,
            **config.schema_registry_config,
        }
        registry = SchemaRegistryClient(schema_registry_conf)

        def mce_dict(mce: MetadataChangeEvent, ctx: SerializationContext) -> dict:
            return mce.to_obj(tuples=True)

        def mcp_dict(
            mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
            ctx: SerializationContext,
        ) -> dict:
            return mcp.to_obj(tuples=True)

        self._mce_serializer = AvroSerializer(
            schema_str=getMetadataChangeEventSchema(),
            schema_registry_client=registry,
            to_dict=mce_dict,
        )
        self._mcp_serializer = AvroSerializer(
            schema_str=getMetadataChangeProposalSchema(),
            schema_registry_client=registry,
            to_dict=mcp_dict,
        )

        self._conn = create_pgqueue_connection(qconf)

    def _serialize_for_enqueue(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ],
    ) -> Tuple[str, str, bytes]:
        """Return logical queue topic name, routing key, Avro payload bytes."""
        payload_obj: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
        if isinstance(item, (MetadataChangeProposal, MetadataChangeProposalWrapper)):
            topic_name = self.config.topic_routes[MCP_KEY]
            kafka_topic_for_sr = topic_name
            urn = item.entityUrn
            if urn is None:
                raise ValueError(
                    "MetadataChangeProposal requires entityUrn for pgQueue routing key"
                )
            key = urn
            serializer = self._mcp_serializer
            payload_obj = item
        else:
            raise ValueError(
                "MetadataChangeEvent (legacy snapshot MCE) is not supported with pgQueue "
                "messaging. Use MetadataChangeProposal (MCP) ingestion instead."
            )

        ctx = SerializationContext(kafka_topic_for_sr, MessageField.VALUE)
        serialized = serializer(payload_obj, ctx)
        if serialized is None:
            raise RuntimeError("Avro serialization returned None")
        return topic_name, key, serialized

    def emit(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        cb = callback or _error_reporting_callback
        try:
            topic_name, key, serialized = self._serialize_for_enqueue(item)
            td = self.config.queue.merged_topic_defaults_for(topic_name)
            mode = PgQueuePayloadCompression[self.config.payload_compression]
            stored = encode_inner(serialized, mode)
            self._repo.enqueue(
                self._conn,
                topic_name=topic_name,
                routing_key=key,
                partition_count=td.partition_count,
                retention_max_age_seconds=td.retention_max_age_seconds,
                max_rows_per_topic=td.max_rows_per_topic,
                max_total_payload_bytes=td.max_total_payload_bytes_per_topic,
                default_content_type_mime=td.default_content_type_mime,
                aggressive_retention=td.aggressive_retention,
                priority=self.config.default_priority,
                payload=stored,
                content_type=self.config.content_type,
                headers=(),
                payload_compression=int(mode),
            )
            cb(None, "pgQueue enqueue succeeded")  # type: ignore[arg-type]
        except Exception as e:
            cb(e, "pgQueue enqueue failed")

    def emit_batch(
        self,
        items: Sequence[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """Serialize many records and enqueue them in a single PostgreSQL transaction."""
        cb = callback or _error_reporting_callback
        if not items:
            return
        try:
            batch_items: List[EnqueueBatchItem] = []
            topic_names: List[str] = []
            mode = PgQueuePayloadCompression[self.config.payload_compression]
            for item in items:
                topic_name, key, serialized = self._serialize_for_enqueue(item)
                topic_names.append(topic_name)
                stored = encode_inner(serialized, mode)
                batch_items.append(
                    EnqueueBatchItem(
                        topic_name=topic_name,
                        routing_key=key,
                        priority=self.config.default_priority,
                        payload=stored,
                        content_type=self.config.content_type,
                        headers=(),
                        payload_compression=int(mode),
                    )
                )
            if len(set(topic_names)) != 1:
                raise ValueError(
                    "emit_batch requires a single topic per batch for pgQueue defaults resolution; "
                    f"got topics={sorted(set(topic_names))}"
                )
            td = self.config.queue.merged_topic_defaults_for(topic_names[0])
            self._repo.enqueue_batch(
                self._conn,
                batch_items,
                partition_count=td.partition_count,
                retention_max_age_seconds=td.retention_max_age_seconds,
                max_rows_per_topic=td.max_rows_per_topic,
                max_total_payload_bytes=td.max_total_payload_bytes_per_topic,
                default_content_type_mime=td.default_content_type_mime,
                aggressive_retention=td.aggressive_retention,
            )
            for _ in items:
                cb(None, "pgQueue enqueue succeeded")  # type: ignore[arg-type]
        except Exception as e:
            cb(e, "pgQueue enqueue failed")

    def flush(self) -> None:
        """Ensure the session matches Kafka sink semantics at work-unit boundaries.

        Each ``enqueue`` / ``enqueue_batch`` commits independently; this commits any
        still-open transaction (e.g. stray ``idle in transaction``) and rolls back
        ``idle in failed transaction`` before autocommit restore in the repository.
        """
        flush_pg_connection(self._conn)

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._conn.close()


def kafka_emitter_config_from_pg_queue(
    kafka_subset: KafkaEmitterConfig,
) -> Dict[str, object]:
    """Helper for configs that mirror Kafka emitter Schema Registry settings."""
    return {
        "schema_registry_url": kafka_subset.connection.schema_registry_url,
        "schema_registry_config": kafka_subset.connection.schema_registry_config,
        "topic_routes": kafka_subset.topic_routes,
    }


def _error_reporting_callback(err: Optional[Exception], msg: str) -> None:
    if err:
        logger.error("Failed to emit to pgQueue: %s %s", err, msg)
