"""Topic-agnostic MCL producer backed by pgQueue tables.

Python counterpart to Java ``PgQueueEventProducer.publishRawTopicConfluentAvro`` /
``produceMCL``. Callers pass the logical queue topic name on every ``produce`` call
(e.g. ``WorkEvent_v1``, ``MetadataChangeLog_Versioned_v1``).

Unlike Java's soft-skip when Schema Registry lacks a subject for the topic, this
producer fails hard (callback with error; callers that need raise-on-failure should
re-raise from the callback). Executor redelivery depends on that.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.schema_classes import MetadataChangeLogClass
from datahub.pgqueue.compression import (
    PgQueuePayloadCompression,
    encode_inner,
)
from datahub.pgqueue.config import PgQueueMclProducerConfig
from datahub.pgqueue.connection import create_pgqueue_connection, flush_pg_connection
from datahub.pgqueue.repository import PgQueueRepository

logger = logging.getLogger(__name__)


class DatahubPgQueueMclProducer(Closeable):
    """Serialize MCL like Kafka WorkEventProducer but persist bytes to PostgreSQL."""

    def __init__(self, config: PgQueueMclProducerConfig) -> None:
        self.config = config
        qconf = config.queue
        self._repo = PgQueueRepository(qconf.queue_schema, qconf.table_prefix)

        schema_registry_conf = {
            "url": config.schema_registry_url,
            **config.schema_registry_config,
        }
        registry = SchemaRegistryClient(schema_registry_conf)

        def mcl_dict(mcl: MetadataChangeLogClass, ctx: SerializationContext) -> dict:
            return mcl.to_obj(tuples=True)

        self._mcl_serializer = AvroSerializer(
            schema_str=str(MetadataChangeLogClass.RECORD_SCHEMA),
            schema_registry_client=registry,
            to_dict=mcl_dict,
        )

        self._conn = create_pgqueue_connection(qconf)

    def produce(
        self,
        topic: str,
        event: MetadataChangeLogClass,
        *,
        routing_key: Optional[str] = None,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """Enqueue a Confluent-Avro-serialized MCL on the given logical queue topic.

        Unlike Java ``PgQueueEventProducer``, serialization / Schema Registry failures
        do not soft-skip — the callback receives the error (and callers may re-raise).
        """
        cb = callback or _error_reporting_callback
        try:
            key = routing_key if routing_key is not None else event.entityUrn
            if not key:
                raise ValueError(
                    "MetadataChangeLog requires entityUrn (or routing_key) for pgQueue routing key"
                )

            ctx = SerializationContext(topic, MessageField.VALUE)
            serialized = self._mcl_serializer(event, ctx)
            if serialized is None:
                raise RuntimeError("Avro serialization returned None")

            td = self.config.queue.merged_topic_defaults_for(topic)
            mode = PgQueuePayloadCompression[self.config.payload_compression]
            stored = encode_inner(serialized, mode)
            self._repo.enqueue(
                self._conn,
                topic_name=topic,
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

    def flush(self) -> None:
        flush_pg_connection(self._conn)

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._conn.close()


def _error_reporting_callback(err: Optional[Exception], msg: str) -> None:
    if err:
        logger.error("Failed to produce MCL to pgQueue: %s %s", err, msg)
