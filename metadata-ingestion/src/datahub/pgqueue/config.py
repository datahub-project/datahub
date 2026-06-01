"""Configuration models for PostgreSQL queue connectivity and defaults."""

from __future__ import annotations

import logging
from typing import Any, Dict, Literal, Optional

from pydantic import Field, field_validator, model_validator
from pydantic.types import SecretStr

from datahub.configuration.common import ConfigModel
from datahub.configuration.kafka import _get_schema_registry_url
from datahub.configuration.validate_host_port import validate_host_port
from datahub.emitter.kafka_emitter import (
    DEFAULT_MCE_KAFKA_TOPIC,
    DEFAULT_MCP_KAFKA_TOPIC,
    MCE_KEY,
    MCP_KEY,
)
from datahub.pgqueue.sql import validate_pg_identifier
from datahub.utilities.str_enum import StrEnum

PayloadRouteKind = Literal["mcp", "mce", "mcl", "pe"]

logger = logging.getLogger(__name__)


def _default_payload_kinds() -> Dict[str, PayloadRouteKind]:
    return {MCE_KEY: "mce", MCP_KEY: "mcp"}


class PgQueueAuthMode(StrEnum):
    """Authentication mode for PostgreSQL (matches Postgres source semantics)."""

    PASSWORD = "PASSWORD"
    AWS_IAM = "AWS_IAM"


class PgQueueTopicPartialConfig(ConfigModel):
    """Optional overrides keyed by topic name (extends topic_defaults for that topic)."""

    partition_count: Optional[int] = Field(default=None, ge=1, le=4096)
    priority_bands: Optional[str] = Field(
        default=None,
        description="JSON: priority band ranges and weights for weighted fair queuing.",
    )
    retention_max_age_seconds: Optional[int] = Field(default=None, ge=0)
    max_rows_per_topic: Optional[int] = Field(default=None, ge=0)
    max_total_payload_bytes_per_topic: Optional[int] = Field(default=None, ge=0)
    aggressive_retention: Optional[bool] = Field(default=None)


class PgQueueTopicDefaultsConfig(ConfigModel):
    """Defaults applied when auto-creating a logical topic row (GMS-aligned names)."""

    partition_count: int = Field(
        default=2,
        ge=1,
        le=4096,
        description="Number of partitions for new topics (postgres.pgQueue.topicDefaults.partitionCount).",
    )
    visibility_timeout_seconds: int = Field(
        default=600,
        ge=1,
        le=604800,
        description="Consumer lock duration in seconds after dequeue.",
    )
    priority_bands: str = Field(
        default='[{"range":[0,3],"weight":70},{"range":[4,6],"weight":20},{"range":[7,9],"weight":10}]',
        description="JSON: priority band ranges and weights for weighted fair queuing.",
    )
    retention_max_age_seconds: int = Field(
        default=604800,
        ge=0,
        description="Retention age for rows (0 = disabled at topic level).",
    )
    max_rows_per_topic: int = Field(
        default=0,
        ge=0,
        description="Max rows per topic (0 = unlimited).",
    )
    max_total_payload_bytes_per_topic: int = Field(
        default=0,
        ge=0,
        description="Max total payload bytes per topic (0 = unlimited).",
    )
    aggressive_retention: bool = Field(
        default=False,
        description=(
            "When true, purge messages once all registered consumers have read past them "
            "(postgres.pgQueue.topicDefaults.aggressiveRetention)."
        ),
    )
    default_content_type_mime: str = Field(
        default="application/avro",
        description=(
            "MIME for topic.default_content_type_id (lookup in *_content_type); aligns with "
            "postgres.pgQueue.topicDefaults.defaultContentTypeMime / DATAHUB_PGQUEUE_TOPIC_DEFAULT_CONTENT_TYPE_MIME."
        ),
    )


class PgQueueConnectionConfig(ConfigModel):
    """JDBC-like connectivity to the PostgreSQL database hosting pgQueue tables."""

    host_port: str = Field(description="Host or host:port for PostgreSQL.")
    database: str = Field(description="Database name.")
    username: str = Field(
        description="PostgreSQL user (IAM DB user when using AWS_IAM)."
    )

    password: Optional[SecretStr] = Field(
        default=None,
        description="Password when auth_mode is PASSWORD.",
    )

    sslmode: str = Field(
        default="prefer",
        description="libpq sslmode (require recommended for RDS IAM).",
    )

    auth_mode: PgQueueAuthMode = Field(
        default=PgQueueAuthMode.PASSWORD,
        description="PASSWORD or AWS_IAM (RDS IAM token as password).",
    )

    aws_config: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "AWS SDK settings when auth_mode is AWS_IAM. Same keys as "
            "datahub.ingestion.source.aws.aws_common.AwsConnectionConfig; validated when "
            "opening a connection (see datahub.pgqueue.connection)."
        ),
    )

    queue_schema: str = Field(
        default="queue",
        description="PostgreSQL schema containing pgQueue tables (DATAHUB_PGQUEUE_SCHEMA).",
    )

    table_prefix: str = Field(
        default="metadata_queue",
        description="Table prefix: {prefix}_topic, {prefix}_message, ...",
    )

    topic_defaults: PgQueueTopicDefaultsConfig = Field(
        default_factory=PgQueueTopicDefaultsConfig,
        description="Applied when inserting a missing logical topic.",
    )

    topic_overrides: Dict[str, PgQueueTopicPartialConfig] = Field(
        default_factory=dict,
        description=(
            "Per-topic row settings keyed by topic name (e.g. MetadataChangeLog_Timeseries_v1); "
            "merges onto topic_defaults for emit/enqueue (parity with postgres.pgQueue.topics in GMS)."
        ),
    )

    connect_timeout_seconds: int = Field(default=60, ge=1, le=3600)

    def merged_topic_defaults_for(self, topic_name: str) -> PgQueueTopicDefaultsConfig:
        """Defaults for topic_name with optional topic_overrides applied."""
        base = self.topic_defaults.model_copy(deep=True)
        partial = self.topic_overrides.get(topic_name)
        if partial is None:
            return base
        if partial.partition_count is not None:
            base.partition_count = partial.partition_count
        if partial.priority_bands is not None:
            base.priority_bands = partial.priority_bands
        if partial.retention_max_age_seconds is not None:
            base.retention_max_age_seconds = partial.retention_max_age_seconds
        if partial.max_rows_per_topic is not None:
            base.max_rows_per_topic = partial.max_rows_per_topic
        if partial.max_total_payload_bytes_per_topic is not None:
            base.max_total_payload_bytes_per_topic = (
                partial.max_total_payload_bytes_per_topic
            )
        if partial.aggressive_retention is not None:
            base.aggressive_retention = partial.aggressive_retention
        return base

    @field_validator("host_port", mode="after")
    @classmethod
    def host_port_validate(cls, val: str) -> str:
        validate_host_port(val)
        return val

    @field_validator("queue_schema", "table_prefix", mode="after")
    @classmethod
    def validate_sql_identifiers(cls, val: str) -> str:
        validate_pg_identifier(val, "queue_schema/table_prefix")
        return val

    @model_validator(mode="after")
    def validate_auth(self) -> PgQueueConnectionConfig:
        if self.auth_mode == PgQueueAuthMode.PASSWORD:
            if not self.password:
                raise ValueError("password is required when auth_mode is PASSWORD")
        elif self.auth_mode == PgQueueAuthMode.AWS_IAM:
            if ":" not in self.host_port:
                raise ValueError(
                    "host_port must include explicit port for AWS_IAM (e.g. host:5432)."
                )
        return self


class PgQueueEmitterConfig(ConfigModel):
    """Produce MCP/MCE records into pgQueue using Kafka-compatible Avro payloads."""

    queue: PgQueueConnectionConfig

    schema_registry_url: str = Field(
        default_factory=_get_schema_registry_url,
        description="Schema Registry URL (same as Kafka emitter).",
    )

    schema_registry_config: Dict[str, object] = Field(
        default_factory=dict,
        description="Extra Schema Registry client options.",
    )

    topic_routes: Dict[str, str] = Field(
        default_factory=lambda: {
            MCE_KEY: DEFAULT_MCE_KAFKA_TOPIC,
            MCP_KEY: DEFAULT_MCP_KAFKA_TOPIC,
        },
        description="Logical event key → pgQueue logical topic name (often same as Kafka topic name).",
    )

    content_type: str = Field(
        default="application/avro",
        description=(
            "Logical payload MIME after decompression; resolved to message.content_type_id (or omitted "
            "when equal to the topic default)."
        ),
    )

    payload_compression: Literal["NONE", "SNAPPY"] = Field(
        default="SNAPPY",
        description=(
            "Application-layer codec for serialized Avro bytes; stored in message.payload_compression. "
            "Align with postgres.pgQueue.payloadCompression / DATAHUB_PGQUEUE_PAYLOAD_COMPRESSION when "
            "emitting to GMS."
        ),
    )

    default_priority: int = Field(
        default=5,
        ge=0,
        le=9,
        description="Enqueue priority (0 = highest, 9 = lowest, 5 = default/normal).",
    )

    @field_validator("topic_routes", mode="after")
    @classmethod
    def validate_topic_routes(cls, v: Dict[str, str]) -> Dict[str, str]:
        assert MCP_KEY in v, f"topic_routes must contain a route for {MCP_KEY}"
        if MCE_KEY not in v:
            logger.warning(
                "%s not configured in topic_routes; MCE emissions will fail.", MCE_KEY
            )
        return v


class PgQueueConsumerConfig(ConfigModel):
    """Consume pgQueue messages and deserialize Avro like DataHubKafkaReader."""

    queue: PgQueueConnectionConfig

    schema_registry_url: str = Field(default_factory=_get_schema_registry_url)

    schema_registry_config: Dict[str, object] = Field(default_factory=dict)

    topic_routes: Dict[str, str] = Field(
        default_factory=lambda: {
            MCE_KEY: DEFAULT_MCE_KAFKA_TOPIC,
            MCP_KEY: DEFAULT_MCP_KAFKA_TOPIC,
        },
        description="Logical event key → pgQueue logical topic name.",
    )

    consumer_group: str = Field(
        default="datahub_ingestion_pgqueue",
        description="Stored in consumer_offset and lock_owner prefix.",
    )

    visibility_timeout_seconds: Optional[int] = Field(
        default=None,
        description="Override queue.topic_defaults.visibility_timeout_seconds when set.",
    )

    payload_kind_by_route_key: Dict[str, PayloadRouteKind] = Field(
        default_factory=_default_payload_kinds,
        description="Controls Avro deserialization wrapper for each logical route key.",
    )
