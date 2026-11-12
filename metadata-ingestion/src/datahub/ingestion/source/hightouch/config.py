import dataclasses
import logging
from typing import Annotated, Dict, Optional
from urllib.parse import urlparse

from pydantic import ConfigDict, Field, SecretStr, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.common.subtypes import (
    DataJobSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList

logger = logging.getLogger(__name__)

MAX_SYNC_RUNS_WARNING_THRESHOLD = 100

# Platforms whose DataHub connector lowercases dataset URNs by default.
_PLATFORMS_WITH_LOWERCASE_URNS = {"snowflake"}


class HightouchAPIConfig(ConfigModel):
    api_key: SecretStr = Field(
        description="Hightouch API key (a workspace API key with read access) used "
        "as a Bearer token on every request.",
    )
    base_url: str = Field(
        default="https://api.hightouch.com/api/v1",
        description="Base URL of the Hightouch REST API. Override for EU or "
        "self-hosted deployments. A trailing slash is stripped automatically.",
    )
    request_timeout_sec: int = Field(
        default=30,
        description="Per-request timeout in seconds for calls to the Hightouch API. "
        "Increase this if large workspaces time out on listing endpoints.",
    )

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, v: SecretStr) -> SecretStr:
        # A blank secret passes pydantic's type check but produces an unusable
        # "Authorization: Bearer " header.
        if not v.get_secret_value().strip():
            raise ValueError("api_key cannot be empty")
        return v

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        # The API key is sent as a Bearer token on every request, so require TLS
        # even for a loopback endpoint.
        if urlparse(v.strip()).scheme != "https":
            raise ValueError(
                "base_url must use https:// so the API key is not sent over plaintext"
            )
        return v.strip().rstrip("/")

    @field_validator("request_timeout_sec")
    @classmethod
    def validate_request_timeout(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("request_timeout_sec must be positive")
        return v


class PlatformDetail(ConfigModel):
    platform: Optional[str] = Field(
        default=None,
        description="DataHub platform of the source or destination. Detected from "
        "the Hightouch type when unset.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance used by the connector that ingests this "
        "source or destination.",
    )
    env: Optional[str] = Field(
        default=None,
        description="Environment of the source or destination datasets. When unset, "
        "the connector-level `env` is used.",
    )
    database: Optional[str] = Field(
        default=None,
        description="Database used to qualify table names. Defaults to the "
        "`database` in the Hightouch source configuration.",
    )
    include_schema_in_urn: bool = Field(
        default=True,
        description="Include the schema in dataset URNs. Turn off for platforms "
        "whose connector emits two-part `database.table` names.",
    )
    convert_urns_to_lowercase: Optional[bool] = Field(
        default=None,
        description="Lowercase table names, including database and schema, so they "
        "match the platform connector's URNs. Defaults to true for Snowflake, whose "
        "connector lowercases URNs by default, and false otherwise.",
    )

    @property
    def lowercase_urns(self) -> bool:
        if self.convert_urns_to_lowercase is not None:
            return self.convert_urns_to_lowercase
        return self.platform in _PLATFORMS_WITH_LOWERCASE_URNS


@dataclasses.dataclass
class HightouchSourceReport(StaleEntityRemovalSourceReport):
    api_calls: int = 0
    syncs_scanned: int = 0
    models_emitted: int = 0
    sync_runs_emitted: int = 0
    contracts_emitted: int = 0
    contract_events_emitted: int = 0
    contract_assertions_emitted: int = 0
    event_sources_emitted: int = 0
    filtered_syncs: LossyList[str] = dataclasses.field(default_factory=LossyList)
    filtered_models: LossyList[str] = dataclasses.field(default_factory=LossyList)
    filtered_contracts: LossyList[str] = dataclasses.field(default_factory=LossyList)
    sql_parsing_successes: int = 0
    sql_parsing_failures: int = 0
    model_schemas_from_datahub: int = 0
    model_schemas_from_sql: int = 0
    model_schemas_from_referenced_columns: int = 0
    column_lineage_emitted: int = 0
    entity_parse_failures: LossyDict[str, int] = dataclasses.field(
        default_factory=LossyDict
    )
    unknown_sync_run_statuses: LossyDict[str, int] = dataclasses.field(
        default_factory=LossyDict
    )
    field_mappings_dropped: int = 0
    unresolved_destinations: LossyList[str] = dataclasses.field(
        default_factory=LossyList
    )
    datahub_lookup_failures: LossyDict[str, int] = dataclasses.field(
        default_factory=LossyDict
    )

    def report_datahub_lookup_failure(
        self, category: str, context: str, exc: Exception
    ) -> None:
        """Counted per category; only the first failure of each category becomes a
        report warning so a DataHub outage does not flood the report."""
        first = category not in self.datahub_lookup_failures
        self.datahub_lookup_failures[category] = (
            self.datahub_lookup_failures.get(category, 0) + 1
        )
        if first:
            self.warning(
                title="Could not read metadata from DataHub",
                message="A lookup the connector uses to align schemas or lineage "
                "failed, so that detail may be missing. Later failures of the same "
                "kind are counted in datahub_lookup_failures.",
                context=f"{category}: {context}",
                exc=exc,
            )


class HightouchSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    model_config = ConfigDict(protected_namespaces=())

    api_config: HightouchAPIConfig = Field(description="Hightouch API configuration")

    sync_patterns: Annotated[
        AllowDenyPattern, Filters(DataJobSubTypes.HIGHTOUCH_SYNC)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against each sync's slug.",
    )

    model_patterns: Annotated[
        AllowDenyPattern, Filters(DatasetSubTypes.HIGHTOUCH_MODEL)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against each model's display name.",
    )

    contract_patterns: Annotated[
        AllowDenyPattern, Filters(DatasetSubTypes.HIGHTOUCH_EVENT_CONTRACT)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against each event contract's name.",
    )

    emit_models_as_datasets: bool = Field(
        default=True,
        description="Emit Hightouch models as datasets on the `hightouch` platform, "
        "with lineage from the warehouse tables they read. When disabled, syncs read "
        "directly from those warehouse tables.",
    )

    include_sync_runs: bool = Field(
        default=True,
        description="Ingest each sync's recent runs as DataProcessInstances.",
    )

    max_sync_runs_per_sync: int = Field(
        default=10,
        description="Maximum number of runs to ingest per sync. Must be at least 1; "
        "set include_sync_runs: false to skip runs.",
    )

    parse_model_sql: bool = Field(
        default=True,
        description="Parse raw SQL models to derive table and column lineage from "
        "the tables they query.",
    )

    include_contracts: bool = Field(
        default=False,
        description="Ingest Hightouch Event Contracts. Each event becomes a dataset "
        "with its JSON Schema, a DataContract holding a schema assertion, and lineage "
        "from the contract's event sources. Event Contracts are not available on "
        "every Hightouch plan.",
    )

    extract_owners: bool = Field(
        default=True,
        description="Set the user who created a model or sync as its technical "
        "owner, using their email address as the DataHub user id.",
    )

    sources_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default={},
        description="Platform details keyed by Hightouch source id, so upstream "
        "dataset URNs match those of the source platform's connector.",
    )

    destination_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default={},
        description="Platform details keyed by Hightouch destination id, so "
        "destination dataset URNs match those of the destination's connector.",
    )

    include_sibling_relationships: bool = Field(
        default=True,
        description="Make each table model a sibling of the warehouse table it "
        "reads, with the Hightouch model as the primary sibling. The warehouse table "
        "only gets the back-link when it already exists in DataHub.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Hightouch Stateful Ingestion Config."
    )

    @field_validator("max_sync_runs_per_sync")
    @classmethod
    def validate_max_sync_runs(cls, v: int) -> int:
        if v < 1:
            raise ValueError(
                "max_sync_runs_per_sync must be at least 1; set include_sync_runs: "
                "false to skip sync runs"
            )
        if v > MAX_SYNC_RUNS_WARNING_THRESHOLD:
            logger.warning(
                f"max_sync_runs_per_sync is {v}; runs are fetched per sync, so a high "
                "value slows ingestion."
            )
        return v
