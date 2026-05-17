import threading
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Literal, Optional

import cachetools
from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, TransparentSecretStr
from datahub.configuration.source_common import (
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

# Maps SQLMesh connection type_ values to DataHub platform names.
# Most match directly; only a handful need remapping.
SQLMESH_TO_DATAHUB_PLATFORM: Dict[str, str] = {
    "gcp_postgres": "postgres",
    "motherduck": "duckdb",
}

# Tobiko Cloud token file reads are cached for 60s so projected Kubernetes
# secret mounts pick up rotated tokens without a process restart, while still
# avoiding a disk read on every ingest.
_TOBIKO_TOKEN_FILE_CACHE_TTL_SEC = 60
_tobiko_token_file_cache: cachetools.TTLCache = cachetools.TTLCache(
    maxsize=8, ttl=_TOBIKO_TOKEN_FILE_CACHE_TTL_SEC
)
_tobiko_token_file_cache_lock = threading.Lock()


@cachetools.cached(_tobiko_token_file_cache, lock=_tobiko_token_file_cache_lock)
def _read_tobiko_cloud_token_file(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


@dataclass
class SqlmeshSourceReport(StaleEntityRemovalSourceReport):
    # Entity counts
    models_scanned: int = 0
    models_failed: LossyList[str] = field(default_factory=LossyList)
    num_models_with_column_lineage: int = 0
    num_columns_with_lineage: int = 0
    num_column_lineage_parse_failures: int = 0
    num_embedded_models: int = 0
    num_external_models: int = 0
    num_undeclared_upstream_refs: int = 0  # category-3 deps routed to warehouse URN
    num_containers_emitted: int = 0
    num_snapshots_without_physical_name: int = 0

    # Config flags surfaced in report (matches Snowflake/BigQuery pattern)
    include_column_lineage: bool = False
    include_lineage: bool = False

    # Per-phase performance timers (use as context managers: `with self.report.context_load_sec:`)
    context_load_sec: PerfTimer = field(default_factory=PerfTimer)
    schema_extraction_sec: PerfTimer = field(default_factory=PerfTimer)
    lineage_extraction_sec: PerfTimer = field(default_factory=PerfTimer)
    column_lineage_sec: PerfTimer = field(default_factory=PerfTimer)
    container_emission_sec: PerfTimer = field(default_factory=PerfTimer)

    def report_model_failed(self, model_name: str, reason: str) -> None:
        self.models_failed.append(model_name)
        self.warning(
            title="Failed to process model",
            message="Model processing failed and will be skipped.",
            context=f"{model_name}: {reason}",
        )


class SqlmeshSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig],
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    project_path: str = Field(
        description="Path to the SQLMesh project directory.",
    )
    gateway: Optional[str] = Field(
        default=None,
        description="SQLMesh gateway name. Defaults to the project's default gateway.",
    )
    tobiko_cloud_token: Optional[TransparentSecretStr] = Field(
        default=None,
        description=(
            "Tobiko Cloud API token. Set this when the SQLMesh project is configured "
            "against Tobiko Cloud (an ``EnterpriseConfig`` with a cloud state connection) "
            "and DataHub should read from the real cloud state store. Mutually "
            "exclusive with ``tobiko_cloud_token_file``. When neither is set, DataHub "
            "falls back to a local DuckDB stub so Context init succeeds without "
            "creds — model definitions still come from the project files, but anything "
            "that depends on remote state (snapshot history, environment promotions) "
            "is unavailable. Requires ``gateway`` to be set; the gateway name "
            "determines which ``SQLMESH__GATEWAYS__<gw>__STATE_CONNECTION__*`` "
            "variables get populated for tobikodata to read."
        ),
    )
    tobiko_cloud_token_file: Optional[str] = Field(
        default=None,
        description=(
            "Path to a file containing the Tobiko Cloud API token (single line). "
            "Re-read with a 60-second cache TTL so projected Kubernetes secret "
            "mounts pick up rotated tokens without a process restart. Mutually "
            "exclusive with ``tobiko_cloud_token``."
        ),
    )
    tobiko_cloud_url: Optional[str] = Field(
        default=None,
        description=(
            "Tobiko Cloud state-store URL. Only needed when the project's "
            "``config.py`` does not already declare it on its cloud state "
            "connection. Ignored when no token is configured."
        ),
    )
    environment: str = Field(
        default="prod",
        description="SQLMesh environment to ingest from (e.g. prod, dev).",
    )
    target_platform: Optional[str] = Field(
        default=None,
        description=(
            "Warehouse platform SQLMesh writes to (e.g. snowflake, bigquery, databricks). "
            "Auto-detected from the gateway connection type if not set — only specify "
            "this when auto-detection produces the wrong value. "
            "Must match the platform used in your warehouse connector recipe so that "
            "sibling URNs stitch correctly."
        ),
    )
    target_platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "Platform instance for the target warehouse. Must exactly match the "
            "platform_instance configured in your warehouse connector recipe so that "
            "sibling URNs stitch correctly."
        ),
    )
    sqlmesh_platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "Platform instance for the sqlmesh entities themselves. Use this to "
            "namespace the urn:li:dataPlatform:sqlmesh entities and avoid collisions "
            "when multiple SQLMesh projects write to the same warehouse."
        ),
    )
    default_catalog: Optional[str] = Field(
        default=None,
        description=(
            "Default catalog (database) to prepend to model names that are only "
            "two-part (schema.model). Required for sibling URN stitching when your "
            "warehouse connector emits three-part URNs (catalog.schema.table) but "
            "SQLMesh model names omit the catalog. "
            "Example: set to 'analytics' so that 'star.dim_developer' becomes "
            "'analytics.star.dim_developer', matching what the Snowflake connector emits."
        ),
    )
    sqlmesh_is_primary_sibling: bool = Field(
        default=True,
        description=(
            "When true (default), the SQLMesh entity is the primary sibling — "
            "its name, description, and lineage take precedence in the merged UI view. "
            "The warehouse entity contributes runtime metadata (tags, query history, "
            "profiling, usage). Matches dbt's dbt_is_primary_sibling=true default. "
            "Set to false if your warehouse entity carries authoritative documentation."
        ),
    )
    include_schema: bool = Field(
        default=True,
        description=(
            "Emit column schema metadata for each model. "
            "Disable to reduce ingestion volume when schema is already captured "
            "by a warehouse connector."
        ),
    )
    include_lineage: bool = Field(
        default=True,
        description=(
            "Emit model-to-model lineage derived from SQLMesh DAG dependencies. "
            "Disable if lineage is managed by another connector or not needed."
        ),
    )
    skip_external_models_in_lineage: bool = Field(
        default=False,
        description=(
            "When false (default), declared external models (defined in external_models.yaml) "
            "appear as SQLMesh 'Source' entities in the lineage graph. "
            "When true, lineage from managed models points directly to the warehouse URN "
            "for external models — skipping the SQLMesh entity. Produces a cleaner graph "
            "if external models are already well-represented by the warehouse connector."
        ),
    )
    include_database_name: bool = Field(
        default=True,
        description=(
            "Whether to include the database/catalog component in warehouse sibling URNs. "
            "Set to false for platforms like Athena that omit the catalog from their URNs. "
            "When false, 'analytics.star.dim_developer' becomes 'star.dim_developer' in "
            "the warehouse URN."
        ),
    )
    include_column_lineage: bool = Field(
        default=True,
        description=(
            "Emit column-level lineage derived from SQLMesh's SQL parsing (via SQLGlot). "
            "Available for all SQL models natively — no separate parsing step needed. "
            "Disable for very large projects where per-column analysis is too slow."
        ),
    )
    convert_column_urns_to_lowercase: Optional[bool] = Field(
        default=None,
        description=(
            "Force column names in field URNs to lowercase. "
            "Defaults to the same value as convert_urns_to_lowercase when not set. "
            "Set explicitly when column name casing in your warehouse connector differs "
            "from the dataset URN casing (e.g. Snowflake uppercases column names)."
        ),
    )
    include_model_properties: bool = Field(
        default=True,
        description=(
            "Emit dataset properties (description, custom properties) for each model. "
            "Disable to ingest schema and lineage only."
        ),
    )
    incremental_lineage: bool = Field(
        default=True,
        description=(
            "Use patch/incremental lineage mode for non-SQLMesh entities (e.g. external "
            "warehouse tables referenced in lineage). When enabled, the plugin adds "
            "lineage edges without overwriting edges the warehouse connector previously "
            "discovered. Must match the warehouse connector's incremental_lineage setting."
        ),
    )
    incremental_mode: Literal["full", "changed"] = Field(
        default="full",
        description=(
            "Controls which models are processed on each run. "
            "'full' (default): process all models every run. "
            "'changed': compare snapshot fingerprints against the previous DataHub "
            "checkpoint and only process models whose fingerprint changed or are new. "
            "Significantly reduces column-level lineage computation time for large projects."
        ),
    )
    audit_results_path: Optional[str] = Field(
        default=None,
        description=(
            "Path to a JSON file containing SQLMesh audit pass/fail results. "
            "When set, the connector emits AssertionRunEvent aspects for each result, "
            "making pass/fail status visible on the DataHub Data Quality tab. "
            "The file must exist at ingestion time; results with no matching assertion "
            "definition are silently skipped.\n\n"
            "Expected JSON format::\n\n"
            "  {\n"
            '    "metadata": {"generated_at": "<ISO-8601 timestamp>"},\n'
            '    "results": [\n'
            "      {\n"
            '        "model": "myschema.orders",\n'
            '        "audit": "not_null",\n'
            '        "columns": ["order_id"],\n'
            '        "status": "pass",\n'
            '        "failing_rows": 0\n'
            "      }\n"
            "    ]\n"
            "  }\n\n"
            "Valid ``status`` values: ``pass``, ``fail``, ``skip``."
        ),
    )
    preview_urns: bool = Field(
        default=False,
        description=(
            "Before emitting metadata, print a sample of generated sqlmesh URNs and "
            "expected warehouse sibling URNs side-by-side to the log. "
            "Helps validate URN stitching before a full run. "
            "Set to true for a dry-run style check, or use --dry-run on the CLI."
        ),
    )
    preview_urns_sample_size: int = Field(
        default=10,
        description="Number of sample models to include in the URN preview output.",
    )
    model_name_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to allow or deny specific models by fully-qualified name "
            "(matched after catalog qualification, before lowercasing). "
            "Also applied to lineage dependencies — denied models are excluded as upstream nodes."
        ),
    )
    model_kind_filter: Optional[List[str]] = Field(
        default=None,
        description=(
            "Filter which model kinds to ingest. When set, only models whose kind "
            "matches one of the listed values are processed. "
            "Valid values: FULL, INCREMENTAL_BY_TIME_RANGE, INCREMENTAL_BY_UNIQUE_KEY, "
            "INCREMENTAL_BY_PARTITION, SCD_TYPE_2_BY_TIME, SCD_TYPE_2_BY_COLUMN, "
            "VIEW, SEED, EXTERNAL, EMBEDDED. "
            "Default: all kinds."
        ),
    )
    tag_prefix: str = Field(
        default="sqlmesh:",
        description=(
            "Prefix prepended to SQLMesh model tags when creating DataHub tags. "
            "Example: a model tag 'pii' becomes DataHub tag 'sqlmesh:pii'. "
            "Set to empty string to use tags as-is."
        ),
    )
    owner_extraction_pattern: Optional[str] = Field(
        default=None,
        description=(
            "Regex pattern to extract the owner identity from the SQLMesh model "
            "``owner`` field. The first capture group is used as the owner. "
            "Example: ``(.*)@.*`` extracts the username from an email address. "
            "When not set, the owner field value is used as-is."
        ),
    )
    write_semantics: str = Field(
        default="PATCH",
        description=(
            "Controls how tags and ownership emitted by this connector interact with "
            "existing metadata. "
            "PATCH (default): adds alongside existing metadata from other sources. "
            "OVERRIDE: replaces all tags/ownership managed by this connector."
        ),
    )

    @field_validator("write_semantics", mode="after")
    @classmethod
    def validate_write_semantics(cls, v: str) -> str:
        if v.upper() not in ("PATCH", "OVERRIDE"):
            raise ValueError("write_semantics must be 'PATCH' or 'OVERRIDE'")
        return v.upper()

    @field_validator("target_platform", mode="after")
    @classmethod
    def validate_target_platform(cls, v: Optional[str]) -> Optional[str]:
        if v and v.lower() == "sqlmesh":
            raise ValueError(
                "target_platform cannot be 'sqlmesh'. It should be the warehouse "
                "platform that SQLMesh writes to (e.g. snowflake, bigquery, databricks)."
            )
        return v

    @model_validator(mode="before")
    @classmethod
    def set_lowercase_for_snowflake(cls, values: dict) -> dict:
        # Auto-enable URN lowercasing for Snowflake, matching dbt connector behaviour.
        values = deepcopy(values)
        if (values.get("target_platform") or "").lower() == "snowflake":
            values.setdefault("convert_urns_to_lowercase", True)
        return values

    @model_validator(mode="after")
    def validate_tobiko_cloud_token(self) -> "SqlmeshSourceConfig":
        if self.tobiko_cloud_token and self.tobiko_cloud_token_file:
            raise ValueError(
                "Set at most one of tobiko_cloud_token / tobiko_cloud_token_file."
            )
        if (
            self.tobiko_cloud_token or self.tobiko_cloud_token_file
        ) and not self.gateway:
            raise ValueError(
                "gateway is required when tobiko_cloud_token or tobiko_cloud_token_file "
                "is set; the gateway name determines which "
                "SQLMESH__GATEWAYS__<gw>__STATE_CONNECTION__* env vars get populated."
            )
        return self

    def resolve_tobiko_cloud_token(self) -> Optional[str]:
        """Resolve the Tobiko Cloud token from inline value or file. None if neither.

        File reads go through the module-level TTL cache so secret rotations
        take effect within the cache window without a process restart.
        """
        if self.tobiko_cloud_token is not None:
            return self.tobiko_cloud_token.get_secret_value()
        if self.tobiko_cloud_token_file:
            return _read_tobiko_cloud_token_file(self.tobiko_cloud_token_file)
        return None
