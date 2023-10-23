from typing import Optional

from pydantic import Field, root_validator

from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
)

DEFAULT_DATABASE_TABLE_NAME = "metadata_aspect_v2"
DEFAULT_KAFKA_TOPIC_NAME = "MetadataChangeLog_Timeseries_v1"
DEFAULT_DATABASE_BATCH_SIZE = 10_000


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

    @root_validator
    def check_ingesting_data(cls, values):
        if not values.get("database_connection") and not values.get("kafka_connection"):
            raise ValueError(
                "Your current config will not ingest any data."
                " Please specify at least one of `database_connection` or `kafka_connection`, ideally both."
            )
        return values
