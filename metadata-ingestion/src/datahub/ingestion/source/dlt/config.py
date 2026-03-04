"""Configuration classes for the dlt DataHub connector."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from pydantic import Field, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class DestinationPlatformConfig(ConfigModel):
    """Per-destination URN construction config.

    Maps a dlt destination type (e.g. "postgres", "bigquery") to a DataHub
    platform instance and environment so that outlet Dataset URNs produced by
    the dlt connector match those emitted by the destination's own DataHub
    connector — enabling lineage stitching.

    Example:
        postgres:
          platform_instance: null   # local dev, no instance
          env: "DEV"
        bigquery:
          platform_instance: "my-gcp-project"
          env: "PROD"
    """

    database: Optional[str] = Field(
        default=None,
        description=(
            "Database name prefix for outlet Dataset URN construction. "
            "Required for SQL destinations where the DataHub connector uses a "
            "3-part name (database.schema.table), e.g. Postgres uses "
            "'chess.chess_data.players_games'. "
            "dlt only stores the schema (dataset_name), not the database name, "
            "so this must be supplied manually. "
            "Leave null for destinations like BigQuery where the project is "
            "captured in platform_instance instead."
        ),
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "DataHub platform instance for this destination. "
            "Must exactly match the platform_instance used when ingesting the "
            "destination platform (e.g. your Snowflake or BigQuery connector). "
            "Leave null if no platform_instance was configured for that connector."
        ),
    )
    env: str = Field(
        default="PROD",
        description=(
            "DataHub environment for this destination (PROD, DEV, STAGING, etc.). "
            "Must match the env used by the destination platform's own connector."
        ),
    )


class DltRunHistoryConfig(BaseTimeWindowConfig):
    """Time window config for querying _dlt_loads run history.

    Extends BaseTimeWindowConfig which provides start_time, end_time, and
    bucket_duration — the DataHub standard for all time-windowed queries.
    """

    enabled: bool = Field(
        default=True,
        description="Whether to query _dlt_loads and emit DataProcessInstance run history.",
    )


class DltSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Configuration for the dlt DataHub source connector.

    Reads pipeline metadata from dlt's local state directory
    (~/.dlt/pipelines/ by default) and emits DataFlow, DataJob, and lineage
    metadata to DataHub.
    """

    pipelines_dir: str = Field(
        default="~/.dlt/pipelines",
        description=(
            "Path to the dlt pipelines directory. "
            "dlt stores all pipeline state, schemas, and load packages here. "
            "Defaults to ~/.dlt/pipelines/ which is dlt's standard location. "
            "Override when pipelines are stored in a non-default location "
            "(e.g. /data/dlt-pipelines in a Docker environment)."
        ),
    )

    pipeline_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter pipelines by name. "
            "Matched against pipeline_name (the value passed to dlt.pipeline()). "
            "Example: allow ['^prod_.*'] to only ingest production pipelines."
        ),
    )

    include_run_history: bool = Field(
        default=False,
        description=(
            "Whether to query the destination's _dlt_loads table and emit "
            "DataProcessInstance run history. "
            "Requires the destination (e.g. Postgres, BigQuery) to be accessible "
            "and dlt credentials to be configured in ~/.dlt/secrets.toml. "
            "Disabled by default to avoid requiring destination access."
        ),
    )

    run_history_config: DltRunHistoryConfig = Field(
        default_factory=DltRunHistoryConfig,
        description=(
            "Time window for run history extraction. "
            "Uses standard DataHub BaseTimeWindowConfig — supports relative times "
            "like '-7 days' or absolute ISO timestamps."
        ),
    )

    include_lineage: bool = Field(
        default=True,
        description=(
            "Whether to emit outlet lineage from dlt DataJobs to destination Dataset URNs. "
            "Constructs URNs using destination_platform_map. "
            "For lineage to stitch in DataHub, destination_platform_map env/instance "
            "must match the destination connector's configuration."
        ),
    )

    destination_platform_map: Dict[str, DestinationPlatformConfig] = Field(
        default_factory=dict,
        description=(
            "Maps dlt destination type names to DataHub platform configuration. "
            "Used to construct correct Dataset URNs for lineage. "
            "The destination type is the value passed to dlt.pipeline(destination='...'). "
            "Example:\n"
            "  postgres:\n"
            "    platform_instance: null\n"
            "    env: DEV\n"
            "  bigquery:\n"
            "    platform_instance: my-gcp-project\n"
            "    env: PROD"
        ),
    )

    source_dataset_urns: Dict[str, List[str]] = Field(
        default_factory=dict,
        description=(
            "Optional: manually specify inlet (upstream) Dataset URNs per pipeline. "
            "All listed URNs are applied as inlets to every DataJob in the pipeline. "
            "Use for REST API sources where every task shares the same upstream source. "
            "For SQL sources where each task reads from exactly one table, use "
            "source_table_dataset_urns instead. "
            "Key is the pipeline_name; value is a list of Dataset URN strings. "
            "Example:\n"
            "  crm_sync:\n"
            "    - 'urn:li:dataset:(urn:li:dataPlatform:postgres,prod.crm.customers,PROD)'"
        ),
    )

    source_table_dataset_urns: Dict[str, Dict[str, List[str]]] = Field(
        default_factory=dict,
        description=(
            "Optional: manually specify inlet Dataset URNs per pipeline per table. "
            "Use for sql_database sources where each DataJob reads from exactly one "
            "source table and 1:1 lineage is desired. "
            "Outer key is pipeline_name; inner key is table_name; value is a list of URNs. "
            "Example:\n"
            "  my_pipeline:\n"
            "    my_table:\n"
            "      - 'urn:li:dataset:(urn:li:dataPlatform:postgres,prod_db.public.my_table,PROD)'\n"
            "    other_table:\n"
            "      - 'urn:li:dataset:(urn:li:dataPlatform:postgres,prod_db.public.other_table,PROD)'"
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Stateful ingestion configuration. "
            "When enabled, automatically removes DataFlow/DataJob entities from DataHub "
            "if the corresponding dlt pipeline is deleted or no longer found in pipelines_dir."
        ),
    )

    @field_validator("pipelines_dir", mode="after")
    @classmethod
    def expand_user(cls, v: str) -> str:
        """Expand ~ in pipelines_dir to the user's home directory."""
        return str(Path(v).expanduser())

    @property
    def get_pipelines_dir(self) -> Path:
        """Returns the resolved pipelines directory as a Path."""
        return Path(self.pipelines_dir)
