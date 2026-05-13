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
from datahub.emitter.mce_builder import ALL_ENV_TYPES, DEFAULT_ENV
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.metadata.urns import DatasetUrn


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
        default=DEFAULT_ENV,
        description=(
            "DataHub environment for this destination (PROD, DEV, STAGING, etc.). "
            "Must match the env used by the destination platform's own connector. "
            f"One of {sorted(ALL_ENV_TYPES)}."
        ),
    )

    @field_validator("env", mode="after")
    @classmethod
    def _env_must_be_valid(cls, v: str) -> str:
        """Reject typos like 'PORD' / 'prd' at config-load time.

        A bad env value silently produces unstitched lineage URNs (the dlt
        outlet URN won't match the destination connector's URN), so failing
        fast here prevents a confusing-to-debug ingestion result. Mirrors the
        validator on EnvConfigMixin.env in datahub.configuration.source_common.
        """
        if v.upper() not in ALL_ENV_TYPES:
            raise ValueError(f"env must be one of {sorted(ALL_ENV_TYPES)}, got {v!r}")
        return v.upper()


class DltRunHistoryConfig(BaseTimeWindowConfig):
    """Time window config for querying _dlt_loads run history.

    Extends BaseTimeWindowConfig which provides start_time, end_time, and
    bucket_duration — the DataHub standard for all time-windowed queries.
    Use include_run_history on DltSourceConfig to enable/disable run history.
    """


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

    @field_validator("source_dataset_urns", mode="after")
    @classmethod
    def _validate_source_dataset_urns(
        cls, v: Dict[str, List[str]]
    ) -> Dict[str, List[str]]:
        """Reject malformed Dataset URNs at config-load time.

        Without this, a typo in a YAML config would surface as a per-URN
        report.warning at workunit emission time and silently produce no
        upstream lineage. Failing fast here gives operators an immediate
        actionable signal during 'datahub ingest' startup.
        """
        for pipeline_name, urn_list in v.items():
            for urn_str in urn_list:
                try:
                    DatasetUrn.from_string(urn_str)
                except Exception as e:
                    raise ValueError(
                        f"Invalid Dataset URN in source_dataset_urns[{pipeline_name!r}]: "
                        f"{urn_str!r} ({type(e).__name__}: {e})"
                    ) from e
        return v

    @field_validator("source_table_dataset_urns", mode="after")
    @classmethod
    def _validate_source_table_dataset_urns(
        cls, v: Dict[str, Dict[str, List[str]]]
    ) -> Dict[str, Dict[str, List[str]]]:
        """Reject malformed Dataset URNs in the per-table inlet config at load time."""
        for pipeline_name, table_map in v.items():
            for table_name, urn_list in table_map.items():
                for urn_str in urn_list:
                    try:
                        DatasetUrn.from_string(urn_str)
                    except Exception as e:
                        raise ValueError(
                            f"Invalid Dataset URN in "
                            f"source_table_dataset_urns[{pipeline_name!r}][{table_name!r}]: "
                            f"{urn_str!r} ({type(e).__name__}: {e})"
                        ) from e
        return v
