import dataclasses
import logging
from typing import Dict, Optional

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
from datahub.utilities.lossy_collections import LossyDict, LossyList

logger = logging.getLogger(__name__)


class Constant:
    ORCHESTRATOR = "hightouch"
    # Threshold for warning about excessive sync run ingestion
    MAX_SYNC_RUNS_WARNING_THRESHOLD = 100


class HightouchAPIConfig(ConfigModel):
    api_key: SecretStr = Field(description="Hightouch API key for authentication")
    base_url: str = Field(
        default="https://api.hightouch.com/api/v1",
        description="Hightouch API base URL",
    )
    request_timeout_sec: int = Field(
        default=30, description="Request timeout in seconds"
    )


class PlatformDetail(ConfigModel):
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
    syncs_scanned: int = 0
    models_scanned: int = 0
    models_emitted: int = 0
    sources_scanned: int = 0
    destinations_scanned: int = 0
    sync_runs_scanned: int = 0
    contracts_scanned: int = 0
    contracts_emitted: int = 0
    contract_runs_scanned: int = 0
    filtered_syncs: LossyList[str] = dataclasses.field(default_factory=LossyList)
    filtered_models: LossyList[str] = dataclasses.field(default_factory=LossyList)
    filtered_contracts: LossyList[str] = dataclasses.field(default_factory=LossyList)
    api_calls_count: int = 0
    sql_parsing_attempts: int = 0
    sql_parsing_successes: int = 0
    sql_parsing_failures: int = 0
    model_schemas_emitted: int = 0
    model_schemas_skipped: int = 0
    model_schemas_skip_reasons: LossyDict[str, int] = dataclasses.field(
        default_factory=LossyDict
    )
    model_schemas_from_datahub: int = 0
    model_schemas_datahub_not_found: LossyList[str] = dataclasses.field(
        default_factory=LossyList
    )
    destinations_emitted: int = 0
    schemas_from_referenced_columns: int = 0
    column_lineage_emitted: int = 0
    tags_emitted: int = 0
    folders_processed: int = 0
    workspaces_emitted: int = 0
    folders_emitted: int = 0

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

    def report_contracts_scanned(self, count: int = 1) -> None:
        self.contracts_scanned += count

    def report_contracts_emitted(self, count: int = 1) -> None:
        self.contracts_emitted += count

    def report_contract_runs_scanned(self, count: int = 1) -> None:
        self.contract_runs_scanned += count

    def report_syncs_dropped(self, sync: str) -> None:
        self.filtered_syncs.append(sync)

    def report_models_dropped(self, model: str) -> None:
        self.filtered_models.append(model)

    def report_contracts_dropped(self, contract: str) -> None:
        self.filtered_contracts.append(contract)

    def report_api_call(self) -> None:
        self.api_calls_count += 1

    def report_model_schemas_emitted(self, count: int = 1) -> None:
        self.model_schemas_emitted += count

    def report_model_schemas_skipped(self, reason: str) -> None:
        self.model_schemas_skipped += 1
        self.model_schemas_skip_reasons[reason] = (
            self.model_schemas_skip_reasons.get(reason, 0) + 1
        )

    def report_model_schema_from_datahub(self, count: int = 1) -> None:
        self.model_schemas_from_datahub += count

    def report_model_schema_datahub_not_found(self, model_name: str) -> None:
        self.model_schemas_datahub_not_found.append(model_name)

    def report_destinations_emitted(self, count: int = 1) -> None:
        self.destinations_emitted += count


class HightouchSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
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

    include_sync_runs: bool = Field(
        default=True,
        description="Whether to ingest sync run history as DataProcessInstances.",
    )

    max_sync_runs_per_sync: int = Field(
        default=10,
        description="Maximum number of sync runs to ingest per sync.",
    )

    parse_model_sql: bool = Field(
        default=True,
        description="Whether to parse raw SQL from models to extract upstream table lineage. "
        "When enabled, SQL queries in models are parsed to identify source tables and create lineage.",
    )

    include_contracts: bool = Field(
        default=True,
        description="Whether to ingest Event Contracts as DataHub Assertions. "
        "Event Contracts are data quality validation rules that Hightouch enforces.",
    )

    max_contract_runs_per_contract: int = Field(
        default=10,
        description="Maximum number of contract validation runs to ingest per contract.",
    )

    contract_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filtering regex patterns for contract names.",
    )

    sources_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default={},
        description="A mapping from source id to its platform/instance/env/database details.",
    )

    destinations_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default={},
        description="A mapping of destination id to its platform/instance/env details.",
    )

    include_table_lineage_to_sibling: bool = Field(
        default=True,
        description="For table-type models, whether to create lineage to the underlying source table "
        "and establish a sibling relationship. When enabled (default), the Hightouch model (emitted on 'hightouch' platform) "
        "becomes a sibling of the source table (e.g., Snowflake table), with the source table as primary. "
        "The source table URN is used in lineage/inputOutput instead of the Hightouch model URN. "
        "Similar to Trino's 'trino_as_primary' relationship with Hive tables. "
        "When enabled, ensure sources_to_platform_instance is configured to match your warehouse connector settings.",
    )

    extract_workspaces_to_containers: bool = Field(
        default=True,
        description="Whether to extract Hightouch workspaces as DataHub containers. "
        "When enabled, models and syncs are organized under workspace containers, providing hierarchical navigation. "
        "Models with folder assignments are nested under folder containers within their workspace.",
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
