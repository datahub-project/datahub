import threading
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import Field, field_validator, model_validator

# cachetools ships only with the ``[sqlmesh]`` extra. Guard the import so this
# module still loads with base deps (see the aws_common note below and the
# token-file cache); when absent the token cache is simply never built because
# the code path that needs it requires the extra to be installed anyway.
try:
    import cachetools
except ImportError:
    # Bind to None (rather than leaving the name unbound) so the token-file read
    # path can detect the missing dep and fall back to an uncached read instead
    # of raising a bare NameError.
    cachetools = None  # type: ignore[assignment]

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    TransparentSecretStr,
)
from datahub.configuration.git import GitInfo
from datahub.configuration.source_common import (
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field

# is_s3_uri is pure-stdlib (safe to import with base deps); AwsConnectionConfig
# pulls in boto3, which only ships with the ``[sqlmesh]`` / aws extras. Import it
# under a guard so this module (and therefore the whole connector) still imports
# with base deps — `datahub check plugins` and the CI plugin-import validation
# load the source class without installing the extra. When boto3 is absent the
# ``aws_connection`` field's forward ref stays unresolved (the model is only
# fully built once someone actually configures S3, which requires boto3 anyway).
from datahub.ingestion.source.aws.s3_util import is_s3_uri

try:
    from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
except ImportError:
    pass

from datahub.ingestion.source.sqlmesh.constants import (
    SNOWFLAKE_PLATFORM,
    SQLMESH_PLATFORM,
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


def map_sqlmesh_platform(raw: Optional[str]) -> Optional[str]:
    """Map a SQLMesh connection type / adapter dialect to a DataHub platform.

    Single source of truth for the ``SQLMESH_TO_DATAHUB_PLATFORM`` lookup so the
    default-gateway path (connection type_) and the per-gateway path (adapter
    dialect) can't drift if the mapping ever gains normalisation or new keys.
    Returns ``None`` unchanged so callers keep their own not-found handling.
    """
    if raw is None:
        return None
    return SQLMESH_TO_DATAHUB_PLATFORM.get(raw, raw)


# Maps SQLMesh model kind names to DataHub dataset subtypes. Also the closed
# set of values accepted by ``model_kind_filter`` (see VALID_MODEL_KINDS).
MODEL_KIND_TO_SUBTYPE: Dict[str, str] = {
    "FULL": "Model",
    "INCREMENTAL_BY_TIME_RANGE": "Model",
    "INCREMENTAL_BY_UNIQUE_KEY": "Model",
    "INCREMENTAL_BY_PARTITION": "Model",
    "INCREMENTAL_UNMANAGED": "Model",
    "SCD_TYPE_2": "Model",
    "SCD_TYPE_2_BY_TIME": "Model",
    "SCD_TYPE_2_BY_COLUMN": "Model",
    "CUSTOM": "Model",
    "MANAGED": "Model",
    "VIEW": "Model",
    "SEED": "Seed",
    "EXTERNAL": "Source",
    "EMBEDDED": "Embedded",
}

# Valid values for ``model_kind_filter`` — the SQLMesh model kind names above.
VALID_MODEL_KINDS: frozenset = frozenset(MODEL_KIND_TO_SUBTYPE)


def _reject_sqlmesh_target_platform(v: Optional[str]) -> Optional[str]:
    # target_platform is the warehouse SQLMesh writes to; setting it to "sqlmesh"
    # points sibling URNs back at the SQLMesh entity itself and silently breaks
    # stitching. Reject it at both the top level and per-gateway override.
    if v and v.lower() == SQLMESH_PLATFORM:
        raise ValueError(
            "target_platform cannot be 'sqlmesh'. It should be the warehouse "
            "platform that SQLMesh writes to (e.g. snowflake, bigquery, databricks)."
        )
    return v


# Tobiko Cloud token file reads are cached for 60s so projected Kubernetes
# secret mounts pick up rotated tokens without a process restart, while still
# avoiding a disk read on every ingest.
_TOBIKO_TOKEN_FILE_CACHE_TTL_SEC = 60
_tobiko_token_file_cache_lock = threading.Lock()
# Built on first use. cachetools ships only in the ``[sqlmesh]`` extra, so
# importing it at module load would make the whole connector unimportable with
# base deps — which breaks `datahub check plugins` (and the CI plugin-import
# validation) whenever the extra isn't installed.
_tobiko_token_file_cache: Optional["cachetools.TTLCache"] = None


def _get_tobiko_token_file_cache() -> "Optional[cachetools.TTLCache]":
    # Returns None when cachetools isn't installed (base deps only); callers then
    # read the token uncached. In practice reaching here means the [sqlmesh] extra
    # is installed (so cachetools is present) — this is purely defensive.
    if cachetools is None:
        return None
    global _tobiko_token_file_cache
    if _tobiko_token_file_cache is None:
        _tobiko_token_file_cache = cachetools.TTLCache(
            maxsize=8, ttl=_TOBIKO_TOKEN_FILE_CACHE_TTL_SEC
        )
    return _tobiko_token_file_cache


def _read_tobiko_cloud_token_file(path: str) -> str:
    cache = _get_tobiko_token_file_cache()
    if cache is not None:
        with _tobiko_token_file_cache_lock:
            cached = cache.get(path)
        if cached is not None:
            return cached
    # Surface a config-actionable error naming the key, rather than letting a
    # raw FileNotFoundError/PermissionError abort ingestion with an opaque trace.
    # Failures are not cached, so a transient IO error isn't stuck.
    try:
        value = Path(path).read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError) as e:
        raise ValueError(
            f"Could not read tobiko_cloud_token_file at {path!r}: {e}. "
            "Verify the path, file permissions, and that it is UTF-8 text."
        ) from e
    if cache is not None:
        with _tobiko_token_file_cache_lock:
            cache[path] = value
    return value


@dataclass
class SqlmeshSourceReport(StaleEntityRemovalSourceReport):
    # Entity counts
    models_scanned: int = 0
    models_failed: LossyList[str] = field(default_factory=LossyList)
    num_models_with_column_lineage: int = 0
    num_columns_with_lineage: int = 0
    num_column_lineage_parse_failures: int = 0
    # Sample of "<model>.<column>" whose column lineage could not be parsed,
    # so the failures are inspectable in the ingestion summary (not just counted).
    column_lineage_failures: LossyList[str] = field(default_factory=LossyList)
    num_embedded_models: int = 0
    num_external_models: int = 0
    num_undeclared_upstream_refs: int = 0  # category-3 deps routed to warehouse URN
    num_containers_emitted: int = 0
    num_snapshots_without_physical_name: int = 0
    num_assertions_failed: int = 0
    num_profiles_failed: int = 0
    num_operations_skipped: int = 0

    # Remote project sourcing (git clone / s3 download). Only set when
    # project_path points at a remote location.
    git_checkout: Optional[str] = None
    num_project_files_downloaded: int = 0

    # Config flags surfaced in report (matches Snowflake/BigQuery pattern)
    include_column_lineage: bool = False
    include_lineage: bool = False

    # Capability probe results: which signals are available for this run.
    # Set once after Context load; consumed by emitters to choose fallback paths.
    has_state_store_access: Optional[bool] = None
    has_warehouse_query_access: Optional[bool] = None
    has_graph_access: Optional[bool] = None

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


class GatewayOverride(ConfigModel):
    """Per-gateway overrides for warehouse-URN construction.

    SQLMesh projects can declare multiple gateways (e.g. Snowflake for some
    models, BigQuery for others). The top-level ``target_platform`` /
    ``target_platform_instance`` / ``default_catalog`` apply to the default
    gateway; ``gateway_overrides`` lets you set per-gateway values for the
    others. Anything left ``None`` falls back to auto-detection from
    ``ctx.engine_adapters[gateway].dialect``.
    """

    target_platform: Optional[str] = Field(
        default=None,
        description=(
            "Warehouse platform for this gateway. Auto-detected from the "
            "gateway connection type if not set."
        ),
    )
    target_platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "platform_instance for this gateway. Must match the warehouse "
            "connector's platform_instance for sibling URN stitching."
        ),
    )
    default_catalog: Optional[str] = Field(
        default=None,
        description=(
            "default_catalog for this gateway. Prepended to 2-part model "
            "names to build 3-part warehouse URNs."
        ),
    )
    convert_urns_to_lowercase: Optional[bool] = Field(
        default=None,
        description=(
            "Force-lowercase URNs for this gateway. Defaults to the "
            "project-level value, or True for Snowflake."
        ),
    )

    @field_validator("target_platform", mode="after")
    @classmethod
    def validate_target_platform(cls, v: Optional[str]) -> Optional[str]:
        return _reject_sqlmesh_target_platform(v)


class SqlmeshSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig],
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    project_path: str = Field(
        default=".",
        description=(
            "Location of the SQLMesh project. One of: a local directory path; an "
            "``s3://bucket/prefix`` pointing at the project tree (requires "
            "``aws_connection``); or — when ``git_info`` is set — a path *relative "
            "to the cloned repository* (``.``, the default, is the repo root)."
        ),
    )
    aws_connection: Optional["AwsConnectionConfig"] = Field(
        default=None,
        description=(
            "AWS connection details for loading the project from an ``s3://`` "
            "``project_path``. Required whenever ``project_path`` is an S3 URI. "
            "The entire prefix is downloaded to a temp directory for the run."
        ),
    )
    git_info: Optional[GitInfo] = Field(
        default=None,
        description=(
            "Git repository to shallow-clone (authenticated with an SSH deploy "
            "key) and load the SQLMesh project from. When set, ``project_path`` is "
            "interpreted relative to the checkout (e.g. ``project_path: sqlmesh/`` "
            "for a project in a repo subdirectory)."
        ),
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
            "connection. Used for both static-token and SSO auth, so it must be "
            "https:// whenever it is set (credentials/state travel over it)."
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
    gateway_overrides: Dict[str, GatewayOverride] = Field(
        default_factory=dict,
        description=(
            "Per-gateway overrides for multi-gateway SQLMesh projects (different "
            "models writing to different warehouses). Keyed by gateway name. "
            "The top-level ``target_platform`` / ``target_platform_instance`` / "
            "``default_catalog`` continue to apply to the default gateway; this "
            "block sets values for the others. Any field left ``None`` is "
            "auto-detected from the gateway's connection config. Single-gateway "
            "projects can ignore this entirely.\n\n"
            "Example::\n\n"
            "  gateway_overrides:\n"
            "    bigquery_lake:\n"
            "      target_platform: bigquery\n"
            "      target_platform_instance: prod_bigquery\n"
            "      default_catalog: lake-prod\n"
            "    snowflake_dwh:\n"
            "      target_platform_instance: prod_snowflake\n"
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
    detect_stale_fingerprints: bool = Field(
        default=False,
        description=(
            "When enabled, detect SQLMesh fingerprint tables that haven't been regenerated "
            "recently (no plan/apply runs). Use this to monitor if SQLMesh transformations "
            "are running on their expected schedules. Reads snapshot timestamps from the "
            "SQLMesh state store; silently skipped when state is unreachable. "
            "When a fingerprint is stale, a custom property 'sqlmesh.fingerprint_stale' "
            "is added to the dataset."
        ),
    )
    fingerprint_staleness_threshold_hours: int = Field(
        default=48,
        ge=0,
        description=(
            "Number of hours before a fingerprint table is considered stale. "
            "Only used when detect_stale_fingerprints=True. "
            "A fingerprint that hasn't been updated (via plan/apply) within this many "
            "hours will be flagged as stale. Default: 48 hours (2 days)."
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
            '    "metadata": {"generated_at": "2024-01-01T00:00:00Z"},\n'
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
    # Removed: the connector no longer synthesises FRESHNESS / VOLUME assertion
    # definitions, nor an anomaly-detection opt-in marker. Freshness and volume
    # monitoring is expressed with DataHub monitors created against the
    # OperationAspect / DatasetProfile timeseries this connector emits.
    _emit_freshness_assertions = pydantic_removed_field(
        "emit_freshness_assertions", month="August", year=2026
    )
    _emit_volume_assertions = pydantic_removed_field(
        "emit_volume_assertions", month="August", year=2026
    )
    _emit_smart_assertion_anomaly_detection = pydantic_removed_field(
        "emit_smart_assertion_anomaly_detection", month="August", year=2026
    )
    emit_incidents_on_failure: bool = Field(
        default=True,
        description=(
            "Emit a DataHub Incident entity (``urn:li:incident:…``) every time "
            '``_emit_audit_run_events`` reads a ``"fail"`` result from the '
            "``audit_results_path`` JSON file. The incident links back to the "
            "assertion via ``IncidentSource(type=ASSERTION_FAILURE, "
            "sourceUrn=<assertion>)`` so the Incidents tab on the dataset shows "
            "the failure history. Standard DataHub entity — works regardless of "
            "edition. Cloud additionally adds Slack threading and triage ML on "
            "top. Re-emitting the same incident is idempotent because the URN "
            "is derived from a hash of (assertion_urn, run_id)."
        ),
    )

    @field_validator("model_kind_filter", mode="after")
    @classmethod
    def validate_model_kind_filter(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        # A typo (e.g. "INCREMENTAL") would otherwise silently match no model
        # and produce an empty ingestion with zero warning. Fail fast on the
        # closed set of SQLMesh kind names instead.
        if not v:
            return v
        unknown = [k for k in v if k not in VALID_MODEL_KINDS]
        if unknown:
            raise ValueError(
                f"model_kind_filter contains unknown model kind(s): {unknown}. "
                f"Valid kinds: {sorted(VALID_MODEL_KINDS)}."
            )
        return v

    @field_validator("tobiko_cloud_url", mode="after")
    @classmethod
    def validate_tobiko_cloud_url_is_https(cls, v: Optional[str]) -> Optional[str]:
        # The URL is injected as the Tobiko Cloud state-connection endpoint and
        # carries the cloud token/state over the wire for both static-token and
        # SSO auth. A plaintext http:// value would transmit those credentials
        # unencrypted, so reject anything that isn't https whenever it is set.
        if v and not v.lower().startswith("https://"):
            raise ValueError(
                "tobiko_cloud_url must use https:// so cloud credentials and "
                f"state are not sent over plaintext HTTP (got {v!r})."
            )
        return v

    @field_validator("target_platform", mode="after")
    @classmethod
    def validate_target_platform(cls, v: Optional[str]) -> Optional[str]:
        return _reject_sqlmesh_target_platform(v)

    @model_validator(mode="before")
    @classmethod
    def set_lowercase_for_snowflake(cls, values: dict) -> dict:
        # Auto-enable URN lowercasing for Snowflake, matching dbt connector behaviour.
        values = deepcopy(values)
        if (values.get("target_platform") or "").lower() == SNOWFLAKE_PLATFORM:
            values.setdefault("convert_urns_to_lowercase", True)
        return values

    @model_validator(mode="after")
    def validate_project_location(self) -> "SqlmeshSourceConfig":
        project_is_s3 = is_s3_uri(self.project_path)
        if self.git_info is not None and project_is_s3:
            raise ValueError(
                "project_path cannot be an s3:// URI when git_info is set; with "
                "git_info it must be a path relative to the cloned repository."
            )
        if project_is_s3 and self.aws_connection is None:
            raise ValueError(
                "aws_connection is required because project_path is an s3:// URI."
            )
        if project_is_s3:
            # Require an explicit key prefix. A bare bucket (s3://bucket or
            # s3://bucket/) would download the entire bucket into a temp dir,
            # pulling unrelated objects and potentially exhausting disk/runtime.
            _, _, rest = self.project_path.partition("://")
            _bucket, _, key = rest.partition("/")
            if not key.strip("/"):
                raise ValueError(
                    "project_path must include a key prefix pointing at the SQLMesh "
                    "project (e.g. s3://my-bucket/sqlmesh_project), not a bare bucket."
                )
        return self

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
