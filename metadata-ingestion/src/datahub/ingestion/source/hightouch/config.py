import dataclasses
import logging
from typing import Optional

from pydantic import ConfigDict, Field, SecretStr, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)


class Constant:
    """
    Constants used in the Hightouch plugin
    """

    ORCHESTRATOR = "hightouch"
    # Threshold for warning about excessive sync run ingestion
    MAX_SYNC_RUNS_WARNING_THRESHOLD = 100


class HightouchAPIConfig(ConfigModel):
    """Configuration for connecting to the Hightouch API"""

    api_key: SecretStr = Field(description="Hightouch API key for authentication")
    base_url: str = Field(
        default="https://api.hightouch.com/api/v1",
        description="Hightouch API base URL",
    )
    request_timeout_sec: int = Field(
        default=30, description="Request timeout in seconds"
    )


class PlatformDetail(ConfigModel):
    """Platform details for source/destination mapping"""

    platform: Optional[str] = Field(
        default=None,
        description="Override the platform type detection.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that all assets produced by this recipe belong to",
    )
    env: str = Field(
        default=DEFAULT_ENV,
        description="The environment that all assets produced by DataHub platform ingestion source belong to",
    )
    database: Optional[str] = Field(
        default=None,
        description="The database that all assets produced belong to.",
    )
    include_schema_in_urn: bool = Field(
        default=True,
        description="Include schema in the dataset URN.",
    )


@dataclasses.dataclass
class HightouchSourceReport(StaleEntityRemovalSourceReport):
    """Report for Hightouch source"""

    syncs_scanned: int = 0
    models_scanned: int = 0
    models_emitted: int = 0
    sources_scanned: int = 0
    destinations_scanned: int = 0
    sync_runs_scanned: int = 0
    filtered_syncs: LossyList[str] = dataclasses.field(default_factory=LossyList)
    filtered_models: LossyList[str] = dataclasses.field(default_factory=LossyList)
    api_calls_count: int = 0
    sql_parsing_attempts: int = 0
    sql_parsing_successes: int = 0
    sql_parsing_failures: int = 0

    def report_syncs_scanned(self, count: int = 1) -> None:
        self.syncs_scanned += count

    def report_models_scanned(self, count: int = 1) -> None:
        self.models_scanned += count

    def report_models_emitted(self, count: int = 1) -> None:
        self.models_emitted += count

    def report_sources_scanned(self, count: int = 1) -> None:
        self.sources_scanned += count

    def report_destinations_scanned(self, count: int = 1) -> None:
        self.destinations_scanned += count

    def report_sync_runs_scanned(self, count: int = 1) -> None:
        self.sync_runs_scanned += count

    def report_syncs_dropped(self, sync: str) -> None:
        self.filtered_syncs.append(sync)

    def report_models_dropped(self, model: str) -> None:
        self.filtered_models.append(model)

    def report_api_call(self) -> None:
        self.api_calls_count += 1


class HightouchSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    """Configuration for Hightouch source"""

    model_config = ConfigDict(protected_namespaces=())

    api_config: HightouchAPIConfig = Field(description="Hightouch API configuration")

    sync_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filtering regex patterns for sync names.",
    )

    model_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filtering regex patterns for model names.",
    )

    emit_models_as_datasets: bool = Field(
        default=True,
        description="Whether to emit Hightouch models as Dataset entities. "
        "When enabled, models are emitted as datasets with the platform 'hightouch', "
        "and lineage is created from source tables to models, and from models to syncs.",
    )

    include_model_lineage: bool = Field(
        default=True,
        description="Whether to include lineage from source tables to Hightouch models. "
        "Only relevant when emit_models_as_datasets is enabled.",
    )

    include_sync_runs: bool = Field(
        default=True,
        description="Whether to ingest sync run history as DataProcessInstances.",
    )

    max_sync_runs_per_sync: int = Field(
        default=10,
        description="Maximum number of sync runs to ingest per sync.",
    )

    include_column_lineage: bool = Field(
        default=True,
        description="Whether to extract field mappings and emit fine-grained (column-level) lineage. "
        "This extracts field mappings from sync configurations and creates column-to-column lineage.",
    )

    parse_model_sql: bool = Field(
        default=True,
        description="Whether to parse raw SQL from models to extract upstream table lineage. "
        "When enabled, SQL queries in models are parsed to identify source tables and create lineage.",
    )

    sources_to_platform_instance: dict[str, PlatformDetail] = Field(
        default={},
        description="A mapping from source id to its platform/instance/env/database details.",
    )

    destinations_to_platform_instance: dict[str, PlatformDetail] = Field(
        default={},
        description="A mapping of destination id to its platform/instance/env details.",
    )

    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Hightouch Stateful Ingestion Config."
    )

    @field_validator("max_sync_runs_per_sync")
    @classmethod
    def validate_max_sync_runs(cls, v: int) -> int:
        if v < 0:
            raise ValueError("max_sync_runs_per_sync must be non-negative")
        if v > Constant.MAX_SYNC_RUNS_WARNING_THRESHOLD:
            logger.warning(
                f"max_sync_runs_per_sync is set to {v}, which is quite high. "
                f"This may result in many API calls and slower ingestion. "
                f"Consider using a value below {Constant.MAX_SYNC_RUNS_WARNING_THRESHOLD}."
            )
        return v
