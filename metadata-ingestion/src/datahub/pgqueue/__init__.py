"""PostgreSQL-backed DataHub queue client (pgQueue DDL).

The psycopg2-facing modules (:mod:`datahub.pgqueue.repository`, :mod:`datahub.pgqueue.connection`)
do not import catalog ingestion sources. Emitter/consumer classes are loaded lazily so importing
those modules—or ``datahub.pgqueue``—does not pull Schema Registry / Avro helpers unless you use
the Kafka-compatible client entrypoints.
"""

from __future__ import annotations

from typing import Any

from datahub.pgqueue.config import (
    PayloadRouteKind,
    PgQueueAuthMode,
    PgQueueConnectionConfig,
    PgQueueConsumerConfig,
    PgQueueEmitterConfig,
    PgQueueTopicDefaultsConfig,
)
from datahub.pgqueue.repository import EnqueueBatchItem, PgQueueMessageHandle

__all__ = [
    "EnqueueBatchItem",
    "PgQueueMessageHandle",
    "PgQueueConsumedRecord",
    "build_pg_queue_event_meta",
    "DatahubPgQueueConsumer",
    "DatahubPgQueueEmitter",
    "PgQueueAuthMode",
    "PgQueueConnectionConfig",
    "PayloadRouteKind",
    "PgQueueConsumerConfig",
    "PgQueueEmitterConfig",
    "PgQueueTopicDefaultsConfig",
]


def __getattr__(name: str) -> Any:
    if name == "DatahubPgQueueConsumer":
        from datahub.pgqueue.consumer import DatahubPgQueueConsumer

        return DatahubPgQueueConsumer
    if name == "PgQueueConsumedRecord":
        from datahub.pgqueue.consumer import PgQueueConsumedRecord

        return PgQueueConsumedRecord
    if name == "build_pg_queue_event_meta":
        from datahub.pgqueue.consumer import build_pg_queue_event_meta

        return build_pg_queue_event_meta
    if name == "DatahubPgQueueEmitter":
        from datahub.pgqueue.emitter import DatahubPgQueueEmitter

        return DatahubPgQueueEmitter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
