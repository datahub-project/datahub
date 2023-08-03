from typing import Optional

from pydantic import Field

from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
)


class DataHubSourceConfig(StatefulIngestionConfigBase):
    mysql_connection: MySQLConfig = Field(
        # TODO: Check, do these defaults make sense?
        default=MySQLConfig(username="datahub", password="datahub", database="datahub"),
        description="MySQL connection config",
    )

    kafka_connection: KafkaConsumerConnectionConfig = Field(
        default=KafkaConsumerConnectionConfig(),
        description="Kafka connection config",
    )

    include_all_versions: bool = Field(
        default=False,
        description=(
            "If enabled, include all versions of each aspect. "
            "Otherwise, only include the latest version of each aspect."
        ),
    )

    mysql_table_name: str = Field(
        default="metadata_aspect_v2",
        description="Name of MySQL table containing all versioned aspects",
    )

    kafka_topic_name: str = Field(
        default="MetadataChangeLog_Timeseries_v1",
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
