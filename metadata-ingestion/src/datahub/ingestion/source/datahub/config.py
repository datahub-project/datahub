import os
from typing import Optional, Set

import pydantic
from pydantic import Field, root_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
)

DEFAULT_DATABASE_TABLE_NAME = "metadata_aspect_v2"
DEFAULT_KAFKA_TOPIC_NAME = "MetadataChangeLog_Timeseries_v1"
DEFAULT_DATABASE_BATCH_SIZE = 10_000
DEFAULT_EXCLUDE_ASPECTS = {
    "dataHubIngestionSourceKey",
    "dataHubIngestionSourceInfo",
    "datahubIngestionRunSummary",
    "datahubIngestionCheckpoint",
    "dataHubSecretKey",
    "dataHubSecretValue",
    "globalSettingsKey",
    "globalSettingsInfo",
    "testResults",
    "dataHubExecutionRequestKey",
    "dataHubExecutionRequestInput",
    "dataHubExecutionRequestSignal",
    "dataHubExecutionRequestResult",
}


class DataHubSourceConfig(StatefulIngestionConfigBase):
    database_connection: Optional[SQLAlchemyConnectionConfig] = Field(
        default=None,
        description="Database connection config",
    )

    kafka_connection: Optional[KafkaConsumerConnectionConfig] = Field(
        default=None,
        description="Kafka connection config",
    )

    include_all_versions: bool = Field(
        default=False,
        description=(
            "If enabled, include all versions of each aspect. "
            "Otherwise, only include the latest version of each aspect. "
        ),
    )

    include_soft_deleted_entities: bool = Field(
        default=True,
        description=(
            "If enabled, include entities that have been soft deleted. "
            "Otherwise, include all entities regardless of removal status. "
        ),
    )

    exclude_aspects: Set[str] = Field(
        default=DEFAULT_EXCLUDE_ASPECTS,
        description="Set of aspect names to exclude from ingestion",
    )

    database_query_batch_size: int = Field(
        default=DEFAULT_DATABASE_BATCH_SIZE,
        description="Number of records to fetch from the database at a time",
    )

    database_table_name: str = Field(
        default=DEFAULT_DATABASE_TABLE_NAME,
        description="Name of database table containing all versioned aspects",
    )

    kafka_topic_name: str = Field(
        default=DEFAULT_KAFKA_TOPIC_NAME,
        description="Name of kafka topic containing timeseries MCLs",
    )

    # Override from base class to make this enabled by default
    stateful_ingestion: StatefulIngestionConfig = Field(
        default=StatefulIngestionConfig(enabled=True),
        description="Stateful Ingestion Config",
    )

    commit_state_interval: Optional[int] = Field(
        default=1000,
        description="Number of records to process before committing state",
    )

    commit_with_parse_errors: bool = Field(
        default=False,
        description=(
            "Whether to update createdon timestamp and kafka offset despite parse errors. "
            "Enable if you want to ignore the errors."
        ),
    )

    pull_from_datahub_api: bool = Field(
        default=False,
        description="Use the DataHub API to fetch versioned aspects.",
        hidden_from_docs=True,
    )

    max_workers: int = Field(
        default=5 * (os.cpu_count() or 4),
        description="Number of worker threads to use for datahub api ingestion.",
        hidden_from_docs=True,
    )

    urn_pattern: AllowDenyPattern = Field(default=AllowDenyPattern())

    drop_duplicate_schema_fields: bool = Field(
        default=False,
        description="Whether to drop duplicate schema fields in the schemaMetadata aspect. "
        "Useful if the source system has duplicate field paths in the db, but we're pushing to a system with server-side duplicate checking.",
    )

    structured_properties_template_cache_invalidation_interval: int = Field(
        hidden_from_docs=True,
        default=60,
        description="Interval in seconds to invalidate the structured properties template cache.",
    )

    query_timeout: Optional[int] = Field(
        default=None,
        description="Timeout for each query in seconds. ",
    )

    @root_validator(skip_on_failure=True)
    def check_ingesting_data(cls, values):
        if (
            not values.get("database_connection")
            and not values.get("kafka_connection")
            and not values.get("pull_from_datahub_api")
        ):
            raise ValueError(
                "Your current config will not ingest any data."
                " Please specify at least one of `database_connection` or `kafka_connection`, ideally both."
            )
        return values

    @pydantic.validator("database_connection")
    def validate_mysql_scheme(
        cls, v: SQLAlchemyConnectionConfig
    ) -> SQLAlchemyConnectionConfig:
        if "mysql" in v.scheme:
            if v.scheme != "mysql+pymysql":
                raise ValueError("For MySQL, the scheme must be mysql+pymysql.")
        return v
