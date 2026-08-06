import contextlib
import hashlib
import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

try:
    from sqlmesh import Context as SqlmeshContext
except ImportError:
    # sqlmesh is an optional dependency; callers must check `SqlmeshContext is None` before use
    SqlmeshContext = None  # type: ignore[assignment,misc]

if SqlmeshContext is not None:
    # SQLMesh's ProcessPoolExecutor(mp_context=fork) deadlocks when the DataHub
    # async sink thread pool is already running — the child process inherits
    # locks held by other threads (allocator arena, stdio buffer, libcurl
    # connection cache) but no thread alive in the child to release them.
    # Repro is reliable on macOS (libdispatch + malloc_zone hold non-atfork
    # locks); on Linux glibc's pthread_atfork handlers reset most of these so
    # the same scenario "usually" works. Patch unconditionally because the
    # remaining locks (logging, numpy C-ext init, requests session pool) can
    # still strand a fork on Linux under contention, and the parallel-parse
    # speedup is small in practice.
    #
    # These are private SQLMesh internals, so a version bump can rename them.
    # sqlmesh itself is installed at this point, so a failure here means an
    # API rename rather than a missing package — log loudly and carry on
    # without the patch instead of pretending sqlmesh is absent.
    try:
        from sqlmesh.utils.process import SynchronousPoolExecutor

        def _sync_pool(*args: object, **kwargs: object) -> SynchronousPoolExecutor:
            return SynchronousPoolExecutor(
                initializer=kwargs.get("initializer"),  # type: ignore[arg-type]
                initargs=kwargs.get("initargs", ()),  # type: ignore[arg-type]
            )

        # Patch every module that captured create_process_pool_executor by name
        # at import time. Hitting the factory in sqlmesh.utils.process is not
        # enough — call sites that did `from ... import create_process_pool_executor`
        # have their own binding.
        import sqlmesh.core.loader as _loader_mod
        import sqlmesh.core.model.cache as _cache_mod

        _loader_mod.create_process_pool_executor = _sync_pool  # type: ignore[attr-defined]
        _cache_mod.create_process_pool_executor = _sync_pool  # type: ignore[attr-defined]
    except ImportError:
        logging.getLogger(__name__).warning(
            "Could not patch SQLMesh's process-pool factory (private API moved "
            "in this sqlmesh version). Model parsing will fork worker processes, "
            "which can hang when the DataHub async sink is active.",
            exc_info=True,
        )

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    SchemaKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.ingestion.source.sqlmesh.sqlmesh_config import (
    SQLMESH_TO_DATAHUB_PLATFORM,
    SqlmeshSourceConfig,
    SqlmeshSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    AuditStampClass,
    CustomAssertionInfoClass,
    DataPlatformInfoClass,
    DatasetAssertionScopeClass,
    DatasetProfileClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    IncidentInfoClass,
    IncidentSourceClass,
    IncidentSourceTypeClass,
    IncidentStateClass,
    IncidentStatusClass,
    IncidentTypeClass,
    NullTypeClass,
    OperationClass,
    OperationTypeClass,
    PlatformTypeClass,
    SiblingsClass,
    StatusClass,
    TestDefinitionClass,
    TestDefinitionTypeClass,
    TestInfoClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import Dataset
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.tag_urn import TagUrn

if TYPE_CHECKING:
    from sqlmesh import Context as SqlmeshContextType, Model as SqlmeshModel
    from sqlmesh.core.snapshot import Snapshot

    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

# SQLMesh uses ProcessPoolExecutor internally to parse SQL models. Serialise
# context initialisation to avoid racing over worker process spawning.
# The lock is held only for SqlmeshContext.__init__ (~sub-second).
_sqlmesh_context_load_lock = threading.Lock()

SQLMESH_PLATFORM = "sqlmesh"

# Exact substring of the ConfigError raised by RemoteCloudSchedulerConfig when
# no Tobiko Cloud token is available. We match on this so the shim never
# swallows any other kind of scheduler failure.
_TOBIKO_CLOUD_NO_CREDS_ERR_FRAGMENT = (
    "Cloud scheduler requires a cloud state connection"
)

# Sentinels for the EnterpriseConfig compat patches below.
_TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL = "_datahub_snowflake_app_literal_relaxed"
_TOBIKO_CONVERT_PATCH_SENTINEL = "_datahub_convert_config_type_isinstance_patched"


def _install_enterprise_config_compat_patches() -> None:
    """When tobikodata is installed, the project's ``config.py`` may return an
    ``EnterpriseConfig`` carrying a Snowflake connection with
    ``application="Tobiko_TobikoCloud"``. Loading that through plain
    ``sqlmesh.Context`` trips two distinct failures seen on enterprise Tobiko
    Cloud projects:

    - The OSS ``SnowflakeConnectionConfig`` declares
      ``application: Literal["Tobiko_SQLMesh"]``, which pydantic rejects the
      enterprise value against.
    - ``sqlmesh.core.config.loader`` uses strict ``type(config) != Config``
      checks in three places; an ``EnterpriseConfig`` subclass fails the
      check and gets re-instantiated as plain ``Config(extra="forbid")``,
      dropping enterprise-only fields like ``allow_prod_deploy``.

    Tobiko's own ``tcloud`` sidesteps both by going through ``EnterpriseContext``
    rather than ``sqlmesh.Context``; we can't import ``EnterpriseContext``
    without confirming its path, so we apply two targeted runtime patches
    instead. Both gated on ``tobikodata`` being importable so an OSS-only
    install is untouched. Idempotent via sentinel attrs.
    """
    try:
        import tobikodata  # noqa: F401
    except ImportError:
        return

    # Patch 1: relax SnowflakeConnectionConfig.application Literal so the
    # enterprise value "Tobiko_TobikoCloud" validates. The field is only used
    # as a client-identifier string passed to Snowflake's connector — there's
    # no semantic value in pinning it to a single Literal.
    try:
        from sqlmesh.core.config.connection import SnowflakeConnectionConfig

        if not getattr(
            SnowflakeConnectionConfig, _TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL, False
        ):
            field = SnowflakeConnectionConfig.model_fields.get("application")
            if field is not None:
                field.annotation = str
                SnowflakeConnectionConfig.model_rebuild(force=True)
                setattr(
                    SnowflakeConnectionConfig,
                    _TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL,
                    True,
                )
    except ImportError:
        pass

    # Patch 2: short-circuit convert_config_type when the object is already an
    # instance of the target type. The OSS loader otherwise re-instantiates
    # any non-exact-type config through ``config_type.parse_obj(obj.dict())``,
    # which drops enterprise-only fields and chokes on extra="forbid". A
    # single replacement at this seam covers all three strict-type call sites
    # in sqlmesh/core/config/loader.py (lines 55, 188, 246).
    try:
        import sqlmesh.core.config.loader as _loader_mod

        if not getattr(
            _loader_mod.convert_config_type, _TOBIKO_CONVERT_PATCH_SENTINEL, False
        ):
            _original_convert = _loader_mod.convert_config_type

            def _convert_config_type_isinstance(config_obj, config_type):  # type: ignore[no-untyped-def]
                if isinstance(config_obj, config_type):
                    return config_obj
                return _original_convert(config_obj, config_type)

            _convert_config_type_isinstance._datahub_convert_config_type_isinstance_patched = True  # type: ignore[attr-defined]
            _loader_mod.convert_config_type = _convert_config_type_isinstance
    except ImportError:
        pass


# Sentinel attribute used to make _install_tobiko_local_state_fallback_shim()
# idempotent across multiple ingest runs in the same process.
_TOBIKO_SHIM_SENTINEL_ATTR = "_datahub_local_state_shim_installed"

# Invoked with a human-readable reason when the local-state fallback actually
# activates, so the running source can put it on its ingestion report. The
# patch is installed once per process but a process may run several
# ingestions, so the callback is read at call time, not captured at install.
_tobiko_local_state_fallback_callback: Optional[Callable[[str], None]] = None


def _install_tobiko_local_state_fallback_shim(
    on_fallback: Optional[Callable[[str], None]] = None,
) -> None:
    """When tobikodata is installed but no Tobiko Cloud token is configured,
    let SQLMesh's Context init succeed against an EnterpriseConfig project by
    swapping the cloud state sync for an in-memory DuckDB stub.

    The shim only catches the very specific ConfigError raised by
    ``RemoteCloudSchedulerConfig.get_cloud_connection()`` when creds are
    absent; any other scheduler failure surfaces. No-op when tobikodata isn't
    installed (OSS-only projects don't have a cloud scheduler to patch).

    ``on_fallback`` is called if and when the fallback is actually used, so the
    caller can surface it on the ingestion report — the degraded mode silently
    drops every state-derived signal.
    """
    global _tobiko_local_state_fallback_callback
    _tobiko_local_state_fallback_callback = on_fallback

    try:
        from tobikodata.sqlmesh_enterprise.config.scheduler import (  # type: ignore[import-not-found]
            RemoteCloudSchedulerConfig,
        )
    except ImportError:
        return

    if getattr(RemoteCloudSchedulerConfig, _TOBIKO_SHIM_SENTINEL_ATTR, False):
        return

    from sqlmesh.core.config.connection import DuckDBConnectionConfig
    from sqlmesh.core.state_sync import EngineAdapterStateSync
    from sqlmesh.utils.errors import ConfigError

    _original_create = RemoteCloudSchedulerConfig.create_state_sync
    _original_fingerprint = RemoteCloudSchedulerConfig.state_sync_fingerprint

    def _create_state_sync_with_fallback(self, context):  # type: ignore[no-untyped-def]
        try:
            return _original_create(self, context)
        except ConfigError as e:
            if _TOBIKO_CLOUD_NO_CREDS_ERR_FRAGMENT not in str(e):
                raise
            reason = (
                "Tobiko Cloud state store unreachable (no token configured). "
                "Falling back to an in-memory DuckDB state so the SQLMesh "
                "Context can initialise from project files. Snapshot history "
                "and environment promotions read from cloud state are "
                "unavailable in this mode. Set tobiko_cloud_token / "
                "tobiko_cloud_token_file to read from the real cloud state."
            )
            logger.info(reason)
            if _tobiko_local_state_fallback_callback is not None:
                _tobiko_local_state_fallback_callback(reason)
            engine_adapter = DuckDBConnectionConfig().create_engine_adapter()
            schema = context.config.get_state_schema(context.gateway)
            return EngineAdapterStateSync(
                engine_adapter,
                schema=schema,
                cache_dir=context.cache_dir,
                console=context.console,
            )

    def _state_sync_fingerprint_with_fallback(self, context):  # type: ignore[no-untyped-def]
        try:
            return _original_fingerprint(self, context)
        except ConfigError as e:
            if _TOBIKO_CLOUD_NO_CREDS_ERR_FRAGMENT not in str(e):
                raise
            return "datahub-tobiko-local-state-fallback"

    RemoteCloudSchedulerConfig.create_state_sync = _create_state_sync_with_fallback
    RemoteCloudSchedulerConfig.state_sync_fingerprint = (
        _state_sync_fingerprint_with_fallback
    )
    setattr(RemoteCloudSchedulerConfig, _TOBIKO_SHIM_SENTINEL_ATTR, True)


def _tobiko_state_connection_env_keys(gateway: str) -> Dict[str, str]:
    """Return the SQLMesh env-var keys that override a gateway's state
    connection. Matches what tcloud's installer.py does verbatim — this is
    the only injection channel tobikodata exposes."""
    prefix = f"SQLMESH__GATEWAYS__{gateway.upper()}__STATE_CONNECTION"
    return {
        "TYPE": f"{prefix}__TYPE",
        "URL": f"{prefix}__URL",
        "TOKEN": f"{prefix}__TOKEN",
    }


@contextlib.contextmanager
def _scoped_tobiko_cloud_env(
    token: Optional[str], gateway: Optional[str], url: Optional[str]
) -> Iterator[None]:
    """Scope SQLMESH__GATEWAYS__<gw>__STATE_CONNECTION__{TYPE,URL,TOKEN} env
    vars to a single block, restoring previous values on exit.

    tobikodata exposes no programmatic injection API for cloud creds — even
    tcloud itself sets these env vars (see
    tcloud/installer.py:_configure_state_connection). We mirror tcloud's
    pattern: TYPE and URL are always injected when tobiko_cloud_url is
    configured; TOKEN is injected only when a static token is explicitly set.
    Without TOKEN, tobikodata falls back to SSO auth via ~/.tcloud/auth.yaml,
    matching the normal tcloud SSO flow.

    No-op when gateway is not configured or neither url nor token is set
    (OSS SQLMesh projects that don't use Tobiko Cloud).
    """
    if gateway is None or (url is None and token is None):
        yield
        return

    keys = _tobiko_state_connection_env_keys(gateway)
    tracked = [keys["TYPE"], keys["TOKEN"], keys["URL"], "SQLMESH__DEFAULT_GATEWAY"]
    saved: Dict[str, Optional[str]] = {k: os.environ.get(k) for k in tracked}

    os.environ[keys["TYPE"]] = "cloud"
    if url:
        os.environ[keys["URL"]] = url
    if token:
        os.environ[keys["TOKEN"]] = token
    os.environ["SQLMESH__DEFAULT_GATEWAY"] = gateway
    try:
        yield
    finally:
        for k, original in saved.items():
            if original is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = original


# Maps SQLMesh model kind names to DataHub dataset subtypes.
_MODEL_KIND_TO_SUBTYPE: Dict[str, str] = {
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


# Describes the semantics of each SQLMesh built-in audit. Every audit becomes a
# CUSTOM DataHub assertion (SQLMesh, not DataHub, executes them) — these values
# are carried as customProperties so the check's shape stays inspectable.
# Audits not listed here are emitted as CUSTOM without the semantic properties.
@dataclass
class _AuditAssertionParams:
    scope: str
    operator: str
    aggregation: str
    uses_columns: bool = True  # True when audit columns → individual field assertions
    row_count_threshold: bool = False  # True for number_of_rows


_SQLMESH_AUDIT_MAP: Dict[str, _AuditAssertionParams] = {
    "not_null": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.NOT_NULL,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
    "unique_values": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.EQUAL_TO,
        aggregation=AssertionStdAggregationClass.UNIQUE_PROPOTION,
    ),
    "unique_combination_of_columns": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_ROWS,
        operator=AssertionStdOperatorClass._NATIVE_,
        aggregation=AssertionStdAggregationClass._NATIVE_,
        uses_columns=False,
    ),
    "number_of_rows": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_ROWS,
        operator=AssertionStdOperatorClass.GREATER_THAN,
        aggregation=AssertionStdAggregationClass.ROW_COUNT,
        uses_columns=False,
        row_count_threshold=True,
    ),
    "forall": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_ROWS,
        operator=AssertionStdOperatorClass._NATIVE_,
        aggregation=AssertionStdAggregationClass._NATIVE_,
        uses_columns=False,
    ),
    "accepted_range": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.BETWEEN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
    "accepted_values": _AuditAssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.IN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
}


def _build_count_query(physical_name: str, dialect: Optional[str] = None) -> str:
    """Render ``SELECT COUNT(*) FROM <table>`` with dialect-correct quoting.

    The naive ``f"SELECT COUNT(*) FROM {physical_name}"`` form breaks on
    catalogs / schemas / tables that contain hyphens or other identifier-
    significant characters (e.g. SQLMesh's example sushi project uses the
    catalog ``sushi-example``, which DuckDB parses as ``sushi - example``).
    Parsing the dotted form back into a SQLGlot Table also fails for the
    same reason. The robust path is to split on ``.`` and build the Table
    expression by parts, letting SQLGlot quote per dialect.

    Supports 1-, 2-, and 3-part names. The default identifier policy uses
    SQLGlot's ``identify=True`` which double-quotes everything (or backticks
    for BigQuery).
    """
    from sqlglot import exp

    parts = physical_name.split(".")
    if len(parts) == 3:
        table_expr = exp.Table(
            this=exp.to_identifier(parts[2]),
            db=exp.to_identifier(parts[1]),
            catalog=exp.to_identifier(parts[0]),
        )
    elif len(parts) == 2:
        table_expr = exp.Table(
            this=exp.to_identifier(parts[1]),
            db=exp.to_identifier(parts[0]),
        )
    else:
        table_expr = exp.Table(this=exp.to_identifier(physical_name))

    return (
        exp.select(exp.Count(this=exp.Star()))
        .from_(table_expr)
        .sql(dialect=dialect, identify=True)
    )


@dataclass
class _EffectiveProjectConfig:
    """Per-project resolved config: project-level overrides merged with global defaults."""

    project_path: str
    gateway: Optional[str]
    environment: str
    target_platform: Optional[str]  # None until auto-detected from context
    target_platform_instance: Optional[str]
    sqlmesh_platform_instance: Optional[str]
    default_catalog: Optional[str]
    convert_urns_to_lowercase: bool
    # Set after context loads — controls how non-prod warehouse sibling URNs are named.
    # One of "schema" (default), "table", or "catalog".
    env_suffix_target: str = "schema"
    # Maps env name regex → catalog override (mutually exclusive with catalog suffix mode).
    env_catalog_mapping: Dict[str, str] = field(default_factory=dict)


@dataclass
class _CapabilityProbes:
    """Which data sources are reachable for this ingestion.

    State store, data warehouse, and DataHub Graph are three INDEPENDENT
    access concerns. Tobiko Cloud puts state in an HTTP API while data
    stays on the user's warehouse; multi-gateway OSS configs can also
    split state and data across gateways. Each emitter consults the
    relevant probe and picks the appropriate fallback signal.
    """

    has_state: bool = False
    has_warehouse_query: bool = False
    has_graph: bool = False


def _probe_capabilities(
    sqlmesh_ctx: "SqlmeshContextType",
    graph: Optional["DataHubGraph"],
    report: SqlmeshSourceReport,
) -> _CapabilityProbes:
    """Probe each signal once. Failures degrade gracefully, but each one silently
    removes an emission, so a failed probe is reported as a warning rather than
    only logged.
    """
    probes = _CapabilityProbes(has_graph=graph is not None)

    # State probe: smallest possible call into the state reader. Listing
    # environments hits the state store but doesn't load any per-model
    # snapshot detail, so it's cheap even on large projects.
    try:
        sqlmesh_ctx.state_reader.get_environments()
        probes.has_state = True
    except Exception as e:
        report.warning(
            title="SQLMesh state store unreachable",
            message="Last-rebuild operation aspects and stale-fingerprint detection are skipped for this run. Check the state connection / state schema grants.",
            exc=e,
        )

    # Warehouse-query probe: ping the engine adapter. We use a no-op
    # `SELECT 1` rather than schema introspection so the probe stays
    # uniform across dialects.
    try:
        sqlmesh_ctx.engine_adapter.fetchone("SELECT 1")
        probes.has_warehouse_query = True
    except Exception as e:
        report.warning(
            title="Data warehouse unreachable via SQLMesh engine adapter",
            message="Row-count dataset profiles are skipped for this run. Check the gateway connection and SELECT grants on the SQLMesh physical tables.",
            exc=e,
        )

    return probes


@platform_name("SQLMesh")
@config_class(SqlmeshSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DESCRIPTIONS, "Supported when model descriptions are defined"
)
class SqlmeshSource(StatefulIngestionSourceBase):
    """
    Ingests metadata from SQLMesh projects into DataHub, following the same
    pattern as the dbt connector.

    Creates ``urn:li:dataPlatform:sqlmesh`` entities for each model and links
    them as siblings to the corresponding warehouse view (Snowflake, BigQuery,
    etc.). The warehouse connector handles runtime metadata (tags, query history,
    profiling, usage); SQLMesh contributes lineage, schema, and model definitions.
    DataHub's SiblingAssociationHook merges both in the UI.

    **Recommended workflow:**

    1. Run warehouse ingestion with ``schema_pattern.deny: ["^sqlmesh__.*"]`` to
       exclude SQLMesh's internal fingerprinted tables.
    2. Run this connector — creates SQLMesh entities and siblings.
    3. DataHub merges both views automatically.

    **URN stitching:** sibling URNs must match exactly. Key settings:

    - ``target_platform``: auto-detected from gateway connection; override only
      when detection is wrong (e.g. force ``postgres`` instead of ``gcp_postgres``)
    - ``target_platform_instance``: must match your warehouse connector's
      ``platform_instance`` exactly
    - ``default_catalog``: set when model names are 2-part (``schema.model``) but
      your warehouse connector emits 3-part URNs (``catalog.schema.table``)

    Example recipe (OSS SQLMesh on Snowflake, run from GitHub Actions)::

        source:
          type: sqlmesh
          config:
            project_path: .              # checked-out repo root
            gateway: snowflake_prod
            target_platform_instance: prod_snowflake  # must match Snowflake connector
            default_catalog: analytics                 # if model names are 2-part
            env: PROD
    """

    config: SqlmeshSourceConfig
    report: SqlmeshSourceReport

    def __init__(self, config: SqlmeshSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = SqlmeshSourceReport()
        self._platform_registered = False
        self.platform = SQLMESH_PLATFORM  # used by StaleEntityRemovalHandler for job ID
        # Surface config flags in report (matches Snowflake/BigQuery pattern)
        self.report.include_lineage = config.include_lineage
        self.report.include_column_lineage = config.include_column_lineage
        self.compiled_owner_extraction_pattern: Optional[re.Pattern] = None
        if config.owner_extraction_pattern:
            self.compiled_owner_extraction_pattern = re.compile(
                config.owner_extraction_pattern
            )
        # Resolved project config (with auto-detected target_platform and env
        # suffix settings from the loaded SQLMesh Context). Populated by
        # _ingest_project's per-model loop so _emit_audit_run_events can build
        # warehouse URNs identical to those used in _emit_assertions —
        # keeping assertion-definition and run-event URN hashes consistent.
        self._resolved_effective: Optional[_EffectiveProjectConfig] = None
        # Per-gateway resolved configs for multi-gateway projects. Keyed by
        # gateway name. Built after Context loads from ctx.engine_adapters.
        # Single-gateway projects end up with a one-entry dict; emitters use
        # _effective_for_model(model) to look up the right one. None until
        # _ingest_project_with_worker populates it.
        self._effective_by_gateway: Dict[str, _EffectiveProjectConfig] = {}
        self._selected_gateway: Optional[str] = None
        # Capability probes (set after Context loads). Emitters consult these
        # to choose signal sources; e.g. pipeline-freshness prefers state but
        # falls back to engine_adapter, volume prefers engine_adapter but
        # falls back to Graph profile.
        self._capabilities: _CapabilityProbes = _CapabilityProbes()
        # SQLMesh dataset URN per model key (logical FQN, model.name, model.fqn),
        # populated while emitting models. _emit_audit_run_events looks up here
        # so run events land on the URN the assertion definitions used, even for
        # models routed through a non-default gateway.
        self._sqlmesh_urn_by_model_key: Dict[str, str] = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SqlmeshSource":
        config = SqlmeshSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> SqlmeshSourceReport:
        return self.report

    def _report_tobiko_local_state_fallback(self, reason: str) -> None:
        self.report.warning(
            title="Tobiko Cloud state store replaced by a local stub",
            message="Everything derived from SQLMesh state (last-rebuild operation aspects, row-count profiles, stale-fingerprint detection) is unavailable for this run.",
            context=reason,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self._emit_platform_registration()
        yield from self._ingest_project()
        if self.config.emit_metadata_tests:
            yield from self._emit_metadata_tests()
        if self.config.audit_results_path:
            yield from self._emit_audit_run_events(self.config.audit_results_path)

    def _emit_metadata_tests(self) -> Iterable[MetadataWorkUnit]:
        """Emit governance Metadata Test entities scoped to this project's models.

        The Test entity is part of the core metadata model, so any DataHub
        instance accepts and stores these definitions; evaluating them requires
        a deployment with a Metadata Tests runner (DataHub Cloud). The test URN
        is derived from the platform/instance scope so re-ingestion is
        idempotent and two projects with distinct ``sqlmesh_platform_instance``
        values get distinct tests.
        """
        platform_urn = mce_builder.make_data_platform_urn(SQLMESH_PLATFORM)
        conditions: List[Dict[str, Any]] = [
            {
                "property": "dataPlatformInstance.platform",
                "operator": "equals",
                "value": platform_urn,
            }
        ]
        scope_key = platform_urn
        scope_label = "SQLMesh"
        if self.config.sqlmesh_platform_instance:
            instance_urn = mce_builder.make_dataplatform_instance_urn(
                SQLMESH_PLATFORM, self.config.sqlmesh_platform_instance
            )
            conditions.append(
                {
                    "property": "dataPlatformInstance.instance",
                    "operator": "equals",
                    "value": instance_urn,
                }
            )
            scope_key = instance_urn
            scope_label = f"SQLMesh ({self.config.sqlmesh_platform_instance})"

        tests = [
            (
                "documentation",
                f"{scope_label}: models have documentation",
                "Every SQLMesh model in this project should carry a description, "
                "either from the model definition or added in DataHub.",
                {
                    "or": [
                        {
                            "property": "datasetProperties.description",
                            "operator": "exists",
                        },
                        {
                            "property": "editableDatasetProperties.description",
                            "operator": "exists",
                        },
                    ]
                },
            ),
            (
                "ownership",
                f"{scope_label}: models have owners",
                "Every SQLMesh model in this project should have an owner, "
                "either from the model's owner field or assigned in DataHub.",
                {"and": [{"property": "ownership.owners.owner", "operator": "exists"}]},
            ),
        ]
        scope_hash = hashlib.md5(scope_key.encode("utf-8")).hexdigest()[:12]
        for suffix, name, description, rules in tests:
            definition = {
                "on": {"types": ["dataset"], "conditions": {"and": conditions}},
                "rules": rules,
            }
            yield MetadataChangeProposalWrapper(
                entityUrn=f"urn:li:test:sqlmesh-{scope_hash}-{suffix}",
                aspect=TestInfoClass(
                    name=name,
                    category="SQLMesh",
                    description=description,
                    definition=TestDefinitionClass(
                        type=TestDefinitionTypeClass.JSON,
                        json=json.dumps(definition, indent=2),
                    ),
                ),
            ).as_workunit()

    # -------------------------------------------------------------------------
    # Platform registration (REQ-14)
    # -------------------------------------------------------------------------

    def _emit_platform_registration(self) -> Iterable[MetadataWorkUnit]:
        """Register the sqlmesh platform in DataHub so entities render with correct branding."""
        platform_urn = mce_builder.make_data_platform_urn(SQLMESH_PLATFORM)
        yield MetadataChangeProposalWrapper(
            entityUrn=platform_urn,
            aspect=DataPlatformInfoClass(
                name=SQLMESH_PLATFORM,
                displayName="SQLMesh",
                type=PlatformTypeClass.OTHERS,
                datasetNameDelimiter=".",
                # Must match the bootstrap entry in data-platforms.yaml so this
                # UPSERT doesn't wipe the logo on every ingestion run.
                logoUrl="assets/platforms/sqlmeshlogo.png",
            ),
        ).as_workunit()

    def _detect_target_platform(
        self, sqlmesh_ctx: "SqlmeshContextType", effective: _EffectiveProjectConfig
    ) -> str:
        """
        Resolve the warehouse platform name, auto-detecting from the gateway
        connection type if not explicitly configured.
        """
        if effective.target_platform:
            return effective.target_platform

        try:
            connection_type = sqlmesh_ctx.connection_config.type_
            platform = SQLMESH_TO_DATAHUB_PLATFORM.get(connection_type, connection_type)
            logger.info(
                "Auto-detected target_platform=%r from gateway connection type %r",
                platform,
                connection_type,
            )
            return platform
        except Exception as e:
            # Falling back to "unknown" yields structurally-valid-but-wrong
            # warehouse URNs (urn:li:dataPlatform:unknown,...), so record it on
            # the report — not just a logger.warning that never reaches the summary.
            self.report.warning(
                title="Could not auto-detect target_platform",
                message="Falling back to 'unknown'; warehouse sibling URNs will be wrong. Set target_platform explicitly in your recipe config.",
                exc=e,
            )
            return "unknown"

    def _read_selected_gateway(
        self,
        sqlmesh_ctx: "SqlmeshContextType",
        effective: _EffectiveProjectConfig,
    ) -> str:
        """Return the canonical name of the currently-selected gateway.

        SQLMesh normalises gateway names to lowercase. We prefer the value
        SQLMesh resolved (``ctx.selected_gateway``) over the user's config
        because the user can omit the gateway and let the project default
        kick in.
        """
        gw = getattr(sqlmesh_ctx, "selected_gateway", None) or effective.gateway
        if not gw:
            # Final fallback — SQLMesh always has SOME selected gateway, but
            # be defensive about API drift.
            gw = "default"
        return str(gw).lower()

    def _build_per_gateway_effectives(
        self,
        sqlmesh_ctx: "SqlmeshContextType",
        default_effective: _EffectiveProjectConfig,
    ) -> Dict[str, _EffectiveProjectConfig]:
        """Build one _EffectiveProjectConfig per gateway visible to the Context.

        Single-gateway projects produce a one-entry dict that mirrors
        ``default_effective`` exactly — no behavioral change for those.

        For multi-gateway projects we iterate ``ctx.engine_adapters``, read
        each adapter's dialect to auto-detect ``target_platform``, and layer
        the user's ``gateway_overrides`` on top. The default gateway always
        appears under its own name so ``_effective_for_model`` can fall back
        when ``model.gateway`` is None.
        """
        result: Dict[str, _EffectiveProjectConfig] = {}

        # Always include the default gateway exactly as the caller resolved
        # it — preserves auto-detection / Snowflake-lowercase / etc. that
        # already ran for the default.
        result[self._selected_gateway or default_effective.gateway or "default"] = (
            default_effective
        )

        # Discover additional gateways from ctx.engine_adapters when SQLMesh
        # exposes the multi-gateway map. Older / minimal Context mocks may
        # not, so degrade silently to single-gateway in that case.
        engine_adapters = getattr(sqlmesh_ctx, "engine_adapters", None) or {}
        default_catalogs = (
            getattr(sqlmesh_ctx, "default_catalog_per_gateway", None) or {}
        )

        for gw_name in engine_adapters:
            gw_key = str(gw_name).lower()
            if gw_key in result:
                continue  # already covered by the default-gateway entry

            override = self.config.gateway_overrides.get(
                gw_name
            ) or self.config.gateway_overrides.get(gw_key)

            # Auto-detect target_platform from this gateway's adapter dialect.
            # We can't reuse _detect_target_platform because it reads
            # ctx.connection_config (default-gateway only); for non-default
            # gateways we inspect engine_adapters[gw].dialect directly.
            auto_platform = None
            try:
                auto_platform = SQLMESH_TO_DATAHUB_PLATFORM.get(
                    str(engine_adapters[gw_name].dialect).lower(),
                    str(engine_adapters[gw_name].dialect).lower(),
                )
            except Exception:
                pass
            target_platform = (
                (override.target_platform if override else None)
                or auto_platform
                or "unknown"
            )

            # convert_urns_to_lowercase: project-level default, with
            # auto-on for Snowflake matching the default-gateway logic.
            convert_lc = (
                override.convert_urns_to_lowercase
                if override and override.convert_urns_to_lowercase is not None
                else (
                    default_effective.convert_urns_to_lowercase
                    or target_platform == "snowflake"
                )
            )

            result[gw_key] = _EffectiveProjectConfig(
                project_path=default_effective.project_path,
                gateway=gw_name,
                environment=default_effective.environment,
                target_platform=target_platform,
                target_platform_instance=(
                    override.target_platform_instance if override else None
                ),
                sqlmesh_platform_instance=default_effective.sqlmesh_platform_instance,
                default_catalog=(override.default_catalog if override else None)
                or default_catalogs.get(gw_name),
                convert_urns_to_lowercase=convert_lc,
                env_suffix_target=default_effective.env_suffix_target,
                env_catalog_mapping=dict(default_effective.env_catalog_mapping),
            )

        return result

    def _effective_for_model(
        self, model: Optional["SqlmeshModel"]
    ) -> _EffectiveProjectConfig:
        """Resolve the right _EffectiveProjectConfig for this model's gateway.

        Falls back to the selected/default gateway when the model has no
        explicit ``gateway`` field, when the gateway isn't in our map (e.g.
        SQLMesh added it after Context load — unlikely), or when called
        with a None model (legacy code paths). Always returns a non-None
        value so callers can use it without further guarding.
        """
        if not self._effective_by_gateway:
            # Pre-Context-load paths — return _resolved_effective if set,
            # else a stub so we never raise.
            return self._resolved_effective or _EffectiveProjectConfig(
                project_path=self.config.project_path,
                gateway=self.config.gateway,
                environment=self.config.environment,
                target_platform=self.config.target_platform,
                target_platform_instance=self.config.target_platform_instance,
                sqlmesh_platform_instance=self.config.sqlmesh_platform_instance,
                default_catalog=self.config.default_catalog,
                convert_urns_to_lowercase=self.config.convert_urns_to_lowercase,
            )

        gw_name = None
        if model is not None:
            gw_name = getattr(model, "gateway", None)
        gw_key = str(gw_name).lower() if gw_name else (self._selected_gateway or "")

        return self._effective_by_gateway.get(
            gw_key,
            # Last-resort: the selected gateway's config. Better than raising
            # and stopping ingest because of one quirky model.
            self._effective_by_gateway.get(
                self._selected_gateway or "",
                self._resolved_effective
                or next(iter(self._effective_by_gateway.values())),
            ),
        )

    # -------------------------------------------------------------------------
    # Project ingestion
    # -------------------------------------------------------------------------

    def _ingest_project(self) -> Iterable[MetadataWorkUnit]:
        if SqlmeshContext is None:
            raise ImportError(
                "sqlmesh package is required for this source. "
                "Install it with: pip install 'acryl-datahub[sqlmesh]'"
            )

        effective = _EffectiveProjectConfig(
            project_path=self.config.project_path,
            gateway=self.config.gateway,
            environment=self.config.environment,
            target_platform=self.config.target_platform,
            target_platform_instance=self.config.target_platform_instance,
            sqlmesh_platform_instance=self.config.sqlmesh_platform_instance,
            default_catalog=self.config.default_catalog,
            convert_urns_to_lowercase=self.config.convert_urns_to_lowercase,
        )

        init_kwargs: Dict[str, Any] = {"paths": [effective.project_path]}
        if effective.gateway:
            init_kwargs["gateway"] = effective.gateway

        # Apply EnterpriseConfig load-time compat patches (Snowflake application
        # Literal + loader convert_config_type isinstance short-circuit). Both
        # are gated on tobikodata being installed, so OSS-only projects are
        # untouched. Idempotent.
        _install_enterprise_config_compat_patches()

        tobiko_token = self.config.resolve_tobiko_cloud_token()
        if tobiko_token is None:
            # No creds configured: let RemoteCloudSchedulerConfig fall back to
            # a local DuckDB stub on the specific "Cloud scheduler requires a
            # cloud state connection" ConfigError so Context init succeeds
            # against an EnterpriseConfig project. Pure no-op when the project
            # doesn't use Tobiko Cloud.
            _install_tobiko_local_state_fallback_shim(
                on_fallback=self._report_tobiko_local_state_fallback
            )

        # tobikodata reads the cloud token lazily on first state access, not
        # during Context.__init__. The env-var scope must cover the entire
        # ingestion — from Context init through the capability probe and all
        # subsequent state reads.
        with _scoped_tobiko_cloud_env(
            token=tobiko_token,
            gateway=effective.gateway,
            url=self.config.tobiko_cloud_url,
        ):
            try:
                logger.info(
                    "Acquiring SQLMesh context load lock for project: %s",
                    effective.project_path,
                )
                with self.report.context_load_sec, _sqlmesh_context_load_lock:
                    sqlmesh_ctx = SqlmeshContext(**init_kwargs)
                logger.info(
                    "SQLMesh context loaded and lock released for project: %s",
                    effective.project_path,
                )
            except Exception as e:
                self.report.failure(
                    title="Failed to load SQLMesh project",
                    message="Could not initialize SQLMesh context.",
                    context=effective.project_path,
                    exc=e,
                )
                return

            try:
                # Probe capabilities once. The result drives which optional
                # signals (operation aspects, row-count profiles) can be emitted.
                self._capabilities = _probe_capabilities(
                    sqlmesh_ctx, self.ctx.graph, self.report
                )
                self.report.has_state_store_access = self._capabilities.has_state
                self.report.has_warehouse_query_access = (
                    self._capabilities.has_warehouse_query
                )
                self.report.has_graph_access = self._capabilities.has_graph
                logger.info(
                    "SQLMesh capability probes: state=%s warehouse=%s graph=%s",
                    self._capabilities.has_state,
                    self._capabilities.has_warehouse_query,
                    self._capabilities.has_graph,
                )

                # Resolve target_platform (auto-detect if not configured) — for the
                # default gateway. Multi-gateway projects get one effective per
                # gateway built immediately below.
                target_platform = self._detect_target_platform(sqlmesh_ctx, effective)

                # Read environment suffix config directly from the loaded Context — no user config needed.
                env_suffix_target = "schema"
                env_catalog_mapping: Dict[str, str] = {}
                try:
                    env_suffix_target = (
                        str(sqlmesh_ctx.config.environment_suffix_target)
                        .split(".")[-1]
                        .lower()
                    )  # e.g. "EnvironmentSuffixTarget.SCHEMA" → "schema"
                    env_catalog_mapping = dict(
                        getattr(sqlmesh_ctx.config, "environment_catalog_mapping", {})
                        or {}
                    )
                except Exception as e:
                    logger.debug(
                        "Could not read environment suffix config from context: %s", e
                    )

                effective = _EffectiveProjectConfig(
                    project_path=effective.project_path,
                    gateway=effective.gateway,
                    environment=effective.environment,
                    target_platform=target_platform,
                    target_platform_instance=effective.target_platform_instance,
                    sqlmesh_platform_instance=effective.sqlmesh_platform_instance,
                    default_catalog=effective.default_catalog,
                    convert_urns_to_lowercase=effective.convert_urns_to_lowercase
                    or target_platform == "snowflake",
                    env_suffix_target=env_suffix_target,
                    env_catalog_mapping=env_catalog_mapping,
                )
                # Cache for _emit_audit_run_events so it can build warehouse URNs
                # the same way _emit_assertions does (consistent assertion hash).
                self._resolved_effective = effective

                # Build per-gateway effectives. For single-gateway projects this
                # produces a one-entry dict equivalent to `effective`; multi-gateway
                # projects get one entry per gateway with platform / instance /
                # catalog auto-detected per gateway and user overrides applied.
                self._selected_gateway = self._read_selected_gateway(
                    sqlmesh_ctx, effective
                )
                self._effective_by_gateway = self._build_per_gateway_effectives(
                    sqlmesh_ctx, effective
                )
                if len(self._effective_by_gateway) > 1:
                    logger.info(
                        "Multi-gateway project: %d gateways (%s)",
                        len(self._effective_by_gateway),
                        ", ".join(sorted(self._effective_by_gateway)),
                    )

                logger.info(
                    "Ingesting SQLMesh project %r (gateway=%r, env=%r, warehouse=%r)",
                    effective.project_path,
                    effective.gateway,
                    effective.environment,
                    target_platform,
                )

                physical_name_by_model: Dict[str, str] = self._build_physical_name_map(
                    sqlmesh_ctx, effective
                )

                # Build the full FQN list first (needed for containers, preview, and changed-mode).
                # For multi-gateway projects each model uses its own gateway's
                # default_catalog when qualifying its name; single-gateway projects
                # see no difference because every lookup returns the same effective.
                all_fqns: Dict[str, "SqlmeshModel"] = {}  # fqn → model
                for model_name_key, model in sqlmesh_ctx.models.items():
                    model_effective = self._effective_for_model(model)
                    fqn = self._build_logical_fqn(str(model_name_key), model_effective)
                    if not self.config.model_name_pattern.allowed(fqn):
                        continue
                    if self.config.model_kind_filter:
                        kind_name = self._get_kind_name(model)
                        if kind_name and kind_name not in self.config.model_kind_filter:
                            continue
                    all_fqns[fqn] = model

                # URN preview (REQ-16 / Phase 8) — print before emitting anything.
                if self.config.preview_urns:
                    self._log_urn_preview(all_fqns, effective)

                # Containers (Phase 6) — emit before models so browsing works on first run.
                with self.report.container_emission_sec:
                    yield from self._emit_containers(set(all_fqns.keys()), effective)

                for fqn, model in all_fqns.items():
                    self.report.models_scanned += 1
                    try:
                        yield from self._emit_model(
                            model, fqn, physical_name_by_model, effective, sqlmesh_ctx
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to process model %s: %s", fqn, e, exc_info=True
                        )
                        self.report.report_model_failed(fqn, str(e))

                # Release state-sync and evaluator resources so that repeated Context()
                # calls in the same process (e.g. multi-project recipes) don't accumulate
                # open connections or file handles.
                sqlmesh_ctx.close()
            except Exception as e:
                # Any failure in post-load setup (capability probe, per-gateway
                # effectives, physical-name map, container emission) should surface
                # as a report failure, not crash ingestion with a raw traceback.
                self.report.failure(
                    title="Failed during SQLMesh ingestion setup",
                    message="Error after loading the SQLMesh context; ingestion aborted.",
                    context=effective.project_path,
                    exc=e,
                )
                return

    # -------------------------------------------------------------------------
    # Phase 6: Container emission
    # -------------------------------------------------------------------------

    def _emit_containers(
        self, fqns: Set[str], effective: _EffectiveProjectConfig
    ) -> Iterable[MetadataWorkUnit]:
        """Emit Database and Schema container entities for the sqlmesh platform."""
        seen_databases: Set[str] = set()
        seen_schemas: Set[str] = set()

        for fqn in sorted(fqns):
            parts = fqn.split(".")
            if len(parts) >= 3:
                catalog, schema = parts[0], parts[1]
            elif len(parts) == 2:
                catalog, schema = None, parts[0]
            else:
                continue  # 1-part name — no containers

            if catalog and catalog not in seen_databases:
                seen_databases.add(catalog)
                db_key = DatabaseKey(
                    platform=SQLMESH_PLATFORM,
                    instance=effective.sqlmesh_platform_instance,
                    env=self.config.env,
                    database=catalog,
                )
                yield from gen_containers(
                    container_key=db_key,
                    name=catalog,
                    sub_types=["Database"],
                )
                self.report.num_containers_emitted += 1

            schema_key_str = f"{catalog}.{schema}" if catalog else schema
            if schema_key_str not in seen_schemas:
                self.report.num_containers_emitted += 1
                seen_schemas.add(schema_key_str)
                if catalog:
                    db_key = DatabaseKey(
                        platform=SQLMESH_PLATFORM,
                        instance=effective.sqlmesh_platform_instance,
                        env=self.config.env,
                        database=catalog,
                    )
                    schema_key = SchemaKey(
                        platform=SQLMESH_PLATFORM,
                        instance=effective.sqlmesh_platform_instance,
                        env=self.config.env,
                        database=catalog,
                        schema=schema,
                    )
                    yield from gen_containers(
                        container_key=schema_key,
                        name=schema,
                        sub_types=["Schema"],
                        parent_container_key=db_key,
                    )
                else:
                    schema_key = SchemaKey(
                        platform=SQLMESH_PLATFORM,
                        instance=effective.sqlmesh_platform_instance,
                        env=self.config.env,
                        database="",
                        schema=schema,
                    )
                    yield from gen_containers(
                        container_key=schema_key,
                        name=schema,
                        sub_types=["Schema"],
                    )

    # -------------------------------------------------------------------------
    # Phase 7: Incremental changed-only mode
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Phase 8: URN preview / dry-run
    # -------------------------------------------------------------------------

    def _log_urn_preview(
        self, all_fqns: Dict[str, "SqlmeshModel"], effective: _EffectiveProjectConfig
    ) -> None:
        """
        Log a sample of sqlmesh ↔ warehouse URN pairs before emitting.
        Helps users validate that sibling URNs will match their warehouse connector.

        For multi-gateway projects each entry shows the gateway in brackets so
        users can spot routing problems (e.g. a model on the wrong gateway).
        """
        sample = list(all_fqns.items())[: self.config.preview_urns_sample_size]
        lines = ["URN preview (sqlmesh → warehouse sibling):"]
        for fqn, model in sample:
            model_effective = self._effective_for_model(model)
            sqlmesh_urn = self._make_sqlmesh_urn(fqn, model_effective)
            warehouse_urn = self._make_warehouse_urn(fqn, model_effective)
            # Always show the gateway label so multi-gateway routing is
            # diagnosable. The default-gateway effective has gateway=None
            # because the top-level config field can be unset; fall back to
            # the SQLMesh-resolved selected_gateway in that case.
            gw_name = (
                getattr(model, "gateway", None)
                or model_effective.gateway
                or self._selected_gateway
                or "default"
            )
            lines.append(f"  sqlmesh : {sqlmesh_urn} [{gw_name}]")
            lines.append(f"  warehouse: {warehouse_urn}")
            lines.append("")
        logger.info("\n".join(lines))

    def _build_physical_name_map(
        self,
        sqlmesh_ctx: "SqlmeshContextType",
        effective: _EffectiveProjectConfig,
    ) -> Dict[str, str]:
        """Map logical FQN → physical fingerprinted table name (for custom property only).

        Computed from model attributes (catalog, physical_schema, schema_name, view_name,
        data_hash) to avoid sqlmesh_ctx.snapshots, which triggers an internal
        ProcessPoolExecutor(mp_context=fork) that hangs on macOS when the DataHub async
        sink thread pool is already running.

        Physical table name format:
          {catalog}.{physical_schema}.{schema_name}__{view_name}__{data_hash}
        e.g. analytics.sqlmesh__myschema.myschema__orders__3732581953
        """
        result: Dict[str, str] = {}
        for model in sqlmesh_ctx.models.values():
            try:
                # Multi-gateway: each model's gateway has its own catalog
                # naming + URN-lowercasing settings. Single-gateway projects
                # see this as a no-op since every model resolves to the same
                # effective.
                model_effective = self._effective_for_model(model)
                catalog = getattr(model, "catalog", None)
                physical_schema = getattr(model, "physical_schema", None)
                schema_name = getattr(model, "schema_name", None)
                view_name = getattr(model, "view_name", None)
                data_hash = getattr(model, "data_hash", None)
                model_name = str(getattr(model, "name", ""))

                if physical_schema and schema_name and view_name and data_hash:
                    parts = [
                        f"{physical_schema}.{schema_name}__{view_name}__{data_hash}"
                    ]
                    if catalog:
                        parts = [catalog] + parts
                    phys = self._normalize_name(".".join(parts), model_effective)
                    logical_fqn = self._build_logical_fqn(model_name, model_effective)
                    result[logical_fqn] = phys
            except Exception as e:
                self.report.num_snapshots_without_physical_name += 1
                logger.debug(
                    "Could not resolve physical table name for model %s: %s",
                    getattr(model, "name", "?"),
                    e,
                )
        return result

    def _snapshot_physical_name(
        self, snapshot: "Snapshot", effective: _EffectiveProjectConfig
    ) -> Optional[str]:
        """Extract physical table name from snapshot, handling SQLMesh API version differences."""
        for kwargs in [
            {"is_dev": False, "ignore_mapping": True},
            {"is_dev": False},
        ]:
            try:
                result = snapshot.table_name(**kwargs)
                return self._normalize_name(str(result), effective) if result else None
            except TypeError:
                continue
            except Exception as e:
                logger.debug(
                    "snapshot.table_name(%s) raised unexpected error: %s", kwargs, e
                )
                break

        try:
            fallback: Any = snapshot.table_name
            if callable(fallback):
                fallback = fallback()
            return self._normalize_name(str(fallback), effective) if fallback else None
        except Exception as e:
            logger.debug("Fallback physical name access failed: %s", e)
            return None

    # -------------------------------------------------------------------------
    # Name and URN helpers
    # -------------------------------------------------------------------------

    def _normalize_name(self, name: str, effective: _EffectiveProjectConfig) -> str:
        """Strip SQL quoting, return dot-separated name, optionally lowercased."""
        parts = []
        for part in name.split("."):
            cleaned = part.strip(" \t\"'`")
            if cleaned:
                parts.append(cleaned)
        joined = ".".join(parts)
        return joined.lower() if effective.convert_urns_to_lowercase else joined

    def _qualify_fqn(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        """Prepend default_catalog to 2-part names to match warehouse connector URNs."""
        if effective.default_catalog and fqn.count(".") < 2:
            catalog = effective.default_catalog
            if effective.convert_urns_to_lowercase:
                catalog = catalog.lower()
            return f"{catalog}.{fqn}"
        return fqn

    def _build_logical_fqn(
        self, raw_name: str, effective: _EffectiveProjectConfig
    ) -> str:
        """Normalize + catalog-qualify a model name."""
        return self._qualify_fqn(self._normalize_name(raw_name, effective), effective)

    def _make_sqlmesh_urn(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        """URN for the SQLMesh entity (urn:li:dataPlatform:sqlmesh,...)."""
        return mce_builder.make_dataset_urn_with_platform_instance(
            platform=SQLMESH_PLATFORM,
            name=fqn,
            platform_instance=effective.sqlmesh_platform_instance,
            env=self.config.env,
        )

    def _apply_env_suffix(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        """
        Apply SQLMesh's environment suffix to a model FQN to get the warehouse view name
        for non-prod environments.

        SQLMesh's environment_suffix_target config controls where the env name is appended:
        - schema (default): catalog.schema__<env>.model
        - table:            catalog.schema.model__<env>
        - catalog:          catalog__<env>.schema.model

        For prod, or when environment_catalog_mapping overrides the env, apply those instead.
        Auto-detected from context.config — no user configuration needed.
        """
        env = effective.environment.lower()
        if env == "prod":
            return fqn  # no suffix in prod

        parts = fqn.split(".")

        # environment_catalog_mapping takes precedence over suffix modes.
        # It maps env name regex → catalog name for that environment.
        for pattern, catalog_override in effective.env_catalog_mapping.items():
            if re.search(pattern, env):
                # Replace the catalog component with the mapped catalog
                if len(parts) >= 3:
                    parts[0] = catalog_override
                elif len(parts) == 2:
                    parts = [catalog_override] + parts
                return ".".join(parts)

        # No catalog mapping matched — apply suffix based on mode.
        suffix = f"__{env}"
        mode = effective.env_suffix_target  # "schema", "table", or "catalog"

        if mode == "catalog":
            if parts:
                parts[0] = f"{parts[0]}{suffix}"
        elif mode == "table":
            if parts:
                parts[-1] = f"{parts[-1]}{suffix}"
        else:  # "schema" (default)
            if len(parts) >= 2:
                parts[-2] = f"{parts[-2]}{suffix}"
            elif len(parts) == 1:
                parts[0] = f"{parts[0]}{suffix}"

        return ".".join(parts)

    def _make_warehouse_urn(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        """URN for the warehouse view sibling (urn:li:dataPlatform:<target_platform>,...)."""
        # Apply environment suffix for non-prod environments before any other transforms.
        name = self._apply_env_suffix(fqn, effective)

        if not self.config.include_database_name:
            # Drop the catalog prefix for platforms like Athena that omit it.
            parts = name.split(".")
            if len(parts) >= 3:
                name = ".".join(parts[1:])

        return mce_builder.make_dataset_urn_with_platform_instance(
            platform=effective.target_platform or "unknown",
            name=name,
            platform_instance=effective.target_platform_instance,
            env=self.config.env,
        )

    # -------------------------------------------------------------------------
    # Per-model workunit emission
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Model kind helpers
    # -------------------------------------------------------------------------

    def _get_kind_name(self, model: "SqlmeshModel") -> Optional[str]:
        kind = getattr(model, "kind", None)
        if kind is None:
            return None
        kind_name = getattr(kind, "model_kind_name", None)
        return str(kind_name) if kind_name is not None else None

    def _is_embedded(self, model: "SqlmeshModel") -> bool:
        kind = getattr(model, "kind", None)
        return bool(getattr(kind, "is_embedded", False)) if kind else False

    def _get_subtype(self, model: "SqlmeshModel") -> Optional[str]:
        kind_name = self._get_kind_name(model)
        return _MODEL_KIND_TO_SUBTYPE.get(kind_name, "Model") if kind_name else "Model"

    def _get_tags(self, model: "SqlmeshModel") -> List[str]:
        """Build DataHub tag URNs from model.tags with the configured prefix."""
        raw = getattr(model, "tags", None)
        raw_tags: List[str] = [t for t in (raw or []) if isinstance(t, str)]
        if not raw_tags:
            return []
        prefix = self.config.tag_prefix
        return [str(TagUrn(f"{prefix}{tag}")) for tag in raw_tags]

    def _get_owner_urn(self, model: "SqlmeshModel") -> Optional[str]:
        """Extract owner URN from model.owner, applying extraction pattern if set."""
        owner_raw = getattr(model, "owner", None)
        if not owner_raw or not isinstance(owner_raw, str):
            return None

        if self.compiled_owner_extraction_pattern:
            match = self.compiled_owner_extraction_pattern.search(owner_raw)
            if match and match.lastindex:
                owner_raw = match.group(1)
            elif match:
                owner_raw = match.group(0)

        return mce_builder.make_user_urn(owner_raw)

    # -------------------------------------------------------------------------
    # Per-model workunit emission
    # -------------------------------------------------------------------------

    def _emit_model(
        self,
        model: "SqlmeshModel",
        fqn: str,
        physical_name_by_model: Dict[str, str],
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
    ) -> Iterable[MetadataWorkUnit]:
        # For multi-gateway projects, the model's own gateway dictates the
        # warehouse platform / instance / catalog. For single-gateway projects
        # this returns the same value as the `effective` parameter.
        effective = self._effective_for_model(model)
        physical_name = physical_name_by_model.get(fqn) or physical_name_by_model.get(
            self._build_logical_fqn(str(getattr(model, "name", fqn)), effective)
        )

        is_embedded = self._is_embedded(model)
        if is_embedded:
            self.report.num_embedded_models += 1

        kind_name = self._get_kind_name(model)
        if kind_name == "EXTERNAL":
            self.report.num_external_models += 1

        custom_props: Optional[Dict[str, str]] = None
        if self.config.include_model_properties:
            custom_props = self._build_custom_properties(
                fqn, physical_name, effective, model
            )
            if self.config.detect_stale_fingerprints and self._is_fingerprint_stale(
                model, sqlmesh_ctx
            ):
                custom_props["sqlmesh.fingerprint_stale"] = "true"

        with self.report.schema_extraction_sec:
            schema_fields = (
                self._build_schema_fields(model, effective)
                if self.config.include_schema
                else None
            )

        tags = self._get_tags(model)
        owner_urn = self._get_owner_urn(model)

        # Compute the sqlmesh URN up front so _build_column_lineage can use it
        # for field URN construction before the Dataset object is created.
        sqlmesh_urn = mce_builder.make_dataset_urn_with_platform_instance(
            platform=SQLMESH_PLATFORM,
            name=fqn,
            platform_instance=effective.sqlmesh_platform_instance,
            env=self.config.env,
        )

        # Remember the URN under every name an audit-results file might use, so
        # _emit_audit_run_events links run events to the same URN the assertion
        # definitions were emitted against — including for models on a
        # non-default gateway, whose FQN can't be rebuilt from the default
        # gateway's effective config.
        for key in (
            fqn,
            str(getattr(model, "name", "") or ""),
            str(getattr(model, "fqn", "") or ""),
        ):
            if key:
                self._sqlmesh_urn_by_model_key[key] = sqlmesh_urn

        # Build table-level and column-level lineage, then combine into a single
        # UpstreamLineage aspect. This avoids emitting duplicate aspect writes.
        combined_upstreams: Optional[UpstreamLineageClass] = None
        if self.config.include_lineage:
            with self.report.lineage_extraction_sec:
                table_lineage = self._build_upstreams(model, effective, sqlmesh_ctx)
            with self.report.column_lineage_sec:
                fine_grained = (
                    self._build_column_lineage(
                        model, sqlmesh_urn, effective, sqlmesh_ctx
                    )
                    if self.config.include_column_lineage
                    else []
                )
            if fine_grained:
                self.report.num_models_with_column_lineage += 1
                self.report.num_columns_with_lineage += len(fine_grained)
            if table_lineage or fine_grained:
                combined_upstreams = UpstreamLineageClass(
                    upstreams=table_lineage.upstreams if table_lineage else [],
                    fineGrainedLineages=fine_grained if fine_grained else None,
                )

        # Emit status FIRST so the MAE consumer can always hydrate the entity,
        # even if it processes this MCL before other aspects are committed.
        # dbt uses the same pattern (StatusClass appended before MCE bundling).
        yield MetadataChangeProposalWrapper(
            entityUrn=sqlmesh_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Emit the SQLMesh entity on the sqlmesh platform
        dataset = Dataset(
            platform=SQLMESH_PLATFORM,
            name=fqn,
            platform_instance=effective.sqlmesh_platform_instance,
            env=self.config.env,
            description=getattr(model, "description", None) or None,
            custom_properties=custom_props,
            schema=schema_fields,
            upstreams=combined_upstreams,
            subtype=self._get_subtype(model),
            tags=tags if tags else None,
            owners=[CorpUserUrn.from_string(owner_urn)] if owner_urn else None,
        )
        yield from dataset.as_workunits()

        # Link dataset to its schema container.
        parts = fqn.split(".")
        if len(parts) >= 3:
            catalog, schema = parts[0], parts[1]
            schema_key: Optional[SchemaKey] = SchemaKey(
                platform=SQLMESH_PLATFORM,
                instance=effective.sqlmesh_platform_instance,
                env=self.config.env,
                database=catalog,
                schema=schema,
            )
        elif len(parts) == 2:
            schema_key = SchemaKey(
                platform=SQLMESH_PLATFORM,
                instance=effective.sqlmesh_platform_instance,
                env=self.config.env,
                database="",
                schema=parts[0],
            )
        else:
            schema_key = None

        if schema_key is not None:
            yield from add_dataset_to_container(schema_key, str(dataset.urn))

        # EMBEDDED models have no warehouse object — skip sibling.
        # All other kinds (including EXTERNAL) have a warehouse view to link to.
        if not is_embedded:
            warehouse_urn = self._make_warehouse_urn(fqn, effective)
            yield from self._emit_siblings(sqlmesh_urn, warehouse_urn)

        # Audits are properties of the SQLMesh model definition, not of any
        # particular materialized output. In SQLMesh the "physical counterpart"
        # is a virtual view pointing at a fingerprint table that rotates as
        # the model evolves — there is no stable physical target equivalent to
        # dbt's model→table mapping. The SQLMesh URN is the only stable,
        # semantically meaningful target for the audit; siblings let users
        # navigate from the logical model to its current materialization.
        yield from self._emit_assertions(model, sqlmesh_urn)

        # Freshness and volume monitoring is left to DataHub monitors the user
        # creates against these two timeseries aspects. We don't synthesise
        # FRESHNESS / VOLUME assertion definitions: nothing in the connector
        # (or in Cloud, without an explicit monitor) would ever evaluate them.
        yield from self._emit_pipeline_operation(
            sqlmesh_urn=sqlmesh_urn, model=model, sqlmesh_ctx=sqlmesh_ctx
        )
        yield from self._emit_row_count_profile(
            model=model,
            sqlmesh_urn=sqlmesh_urn,
            physical_name=physical_name,
            sqlmesh_ctx=sqlmesh_ctx,
        )

    def _is_fingerprint_stale(
        self,
        model: "SqlmeshModel",
        sqlmesh_ctx: Optional["SqlmeshContextType"],
    ) -> bool:
        """Whether the model's fingerprint table is older than the staleness threshold.

        Uses the same SQLMesh state lookup as ``_emit_pipeline_operation``
        (``ctx.snapshots[...].updated_ts``, epoch millis). Returns False when
        state is unreachable or the snapshot carries no timestamp — unknown is
        deliberately not reported as stale.
        """
        if not self._capabilities.has_state or sqlmesh_ctx is None:
            return False
        try:
            snapshots = sqlmesh_ctx.snapshots
            snapshot = snapshots.get(
                str(getattr(model, "fqn", "") or "")
            ) or snapshots.get(str(getattr(model, "name", "")))
            updated_ts = (
                int(getattr(snapshot, "updated_ts", 0)) if snapshot is not None else 0
            )
        except Exception:
            logger.debug(
                "Could not read snapshot.updated_ts for staleness check on %s",
                getattr(model, "name", "?"),
                exc_info=True,
            )
            return False
        if updated_ts <= 0:
            return False
        threshold_ms = self.config.fingerprint_staleness_threshold_hours * 3_600_000
        return (int(time.time() * 1000) - updated_ts) > threshold_ms

    def _build_custom_properties(
        self,
        fqn: str,
        physical_name: Optional[str],
        effective: _EffectiveProjectConfig,
        model: "SqlmeshModel",
    ) -> Dict[str, str]:
        props: Dict[str, str] = {
            "sqlmesh.model_name": fqn,
            "sqlmesh.environment": effective.environment,
            "sqlmesh.warehouse": effective.target_platform or "unknown",
        }
        if effective.gateway:
            props["sqlmesh.gateway"] = effective.gateway
        if physical_name:
            props["sqlmesh.physical_table"] = physical_name
        if effective.target_platform_instance:
            props["sqlmesh.warehouse_instance"] = effective.target_platform_instance
        kind = getattr(model, "kind", None)
        if kind is not None:
            props["sqlmesh.model_kind"] = str(kind)

        cron = getattr(model, "cron", None)
        if cron:
            props["sqlmesh.cron"] = str(cron)

        start = getattr(model, "start", None)
        if start:
            props["sqlmesh.start"] = str(start)

        time_column = getattr(model, "time_column", None)
        if time_column is not None:
            try:
                props["sqlmesh.time_column"] = str(time_column.column)
            except Exception:
                props["sqlmesh.time_column"] = str(time_column)

        partitioned_by = getattr(model, "partitioned_by", None)
        if partitioned_by:
            try:
                cols = [str(c.name) for c in partitioned_by if hasattr(c, "name")]
                if cols:
                    props["sqlmesh.partitioned_by"] = ",".join(cols)
            except Exception:
                pass

        grains = getattr(model, "grains", None)
        if grains:
            try:
                grain_cols = [str(g.name) for g in grains if hasattr(g, "name")]
                if grain_cols:
                    props["sqlmesh.grain"] = ",".join(grain_cols)
            except Exception:
                pass

        audits = getattr(model, "audits", None)
        if audits:
            try:
                audit_names = [str(a[0]) for a in audits if a]
                if audit_names:
                    props["sqlmesh.audits"] = ",".join(audit_names)
            except Exception:
                pass

        return props

    def _build_schema_fields(
        self, model: "SqlmeshModel", effective: _EffectiveProjectConfig
    ) -> Optional[List[SchemaField]]:
        columns_to_types: Dict[str, Any] = (
            getattr(model, "columns_to_types", None) or {}
        )
        if not columns_to_types:
            logger.debug(
                "Model %s has no column type information; skipping schema",
                getattr(model, "name", "?"),
            )
            return None

        col_descriptions: Dict[str, str] = (
            getattr(model, "column_descriptions", None) or {}
        )

        fields = []
        for col_name, col_type in columns_to_types.items():
            type_str = str(col_type) if col_type is not None else ""
            resolved = resolve_sql_type(type_str, effective.target_platform or "")
            fields.append(
                SchemaField(
                    fieldPath=col_name,
                    type=SchemaFieldDataType(type=resolved or NullTypeClass()),
                    nativeDataType=type_str,
                    nullable=True,
                    description=col_descriptions.get(col_name) or None,
                )
            )
        return fields or None

    def _dep_effective(
        self,
        dep_name: str,
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
    ) -> _EffectiveProjectConfig:
        """Gateway-effective config that owns a dependency.

        Falls back to the caller's config for undeclared deps, where we have no
        better signal. Callers must use this — not their own effective — when
        building a dep's FQN, so the name they filter on is the same name the
        emitted URN is built from.
        """
        dep_model = sqlmesh_ctx.get_model(dep_name)
        return (
            self._effective_for_model(dep_model) if dep_model is not None else effective
        )

    def _resolve_dep_urn(
        self,
        dep_name: str,
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
        count_undeclared: bool = True,
    ) -> str:
        """
        Map a dependency name to the correct DataHub URN using 3-category logic.

        Category 1 — managed model: sqlmesh URN
        Category 2 — declared external (EXTERNAL kind): sqlmesh URN by default,
                      warehouse URN when skip_external_models_in_lineage=True
        Category 3 — undeclared implicit (get_model returns None): warehouse URN

        For multi-gateway projects: the dep's OWN gateway determines its
        warehouse URN, not the caller's gateway. If the dep is in our model
        map we resolve via _effective_for_model(dep_model); otherwise we
        fall back to the caller's effective (the dep is undeclared and we
        have no better signal).

        ``count_undeclared`` gates the category-3 report counter. Table lineage
        visits each dep exactly once, so it counts; column lineage revisits the
        same deps once per column and must not inflate the number.
        """
        dep_model = sqlmesh_ctx.get_model(dep_name)
        dep_effective = self._dep_effective(dep_name, effective, sqlmesh_ctx)
        dep_fqn = self._build_logical_fqn(dep_name, dep_effective)

        if dep_model is None:
            logger.debug(
                "Dep %r not in SQLMesh context; routing lineage to warehouse URN",
                dep_name,
            )
            if count_undeclared:
                self.report.num_undeclared_upstream_refs += 1
            return self._make_warehouse_urn(dep_fqn, dep_effective)

        kind = getattr(dep_model, "kind", None)
        is_external = str(getattr(kind, "model_kind_name", "")).upper() == "EXTERNAL"
        if is_external and self.config.skip_external_models_in_lineage:
            return self._make_warehouse_urn(dep_fqn, dep_effective)
        return self._make_sqlmesh_urn(dep_fqn, dep_effective)

    def _build_upstreams(
        self,
        model: "SqlmeshModel",
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
    ) -> Optional[UpstreamLineageClass]:
        """
        Build upstream lineage using 3-category handling:

        Category 1 — Managed models (in context.models):
            target → urn:li:dataPlatform:sqlmesh,...

        Category 2 — Declared external (context.models, kind=EXTERNAL):
            default → urn:li:dataPlatform:sqlmesh,... (Source entity)
            skip_external_models_in_lineage=True → warehouse URN directly

        Category 3 — Undeclared implicit (context.get_model() returns None):
            target → warehouse URN directly (no sqlmesh entity exists for these)
        """
        raw_deps: Set[Any] = getattr(model, "depends_on", None) or set()
        if not raw_deps:
            logger.debug(
                "Model %s has no dependencies; skipping lineage",
                getattr(model, "name", "?"),
            )
            return None

        upstreams = []
        for dep in raw_deps:
            dep_name = str(dep)
            dep_effective = self._dep_effective(dep_name, effective, sqlmesh_ctx)
            dep_fqn = self._build_logical_fqn(dep_name, dep_effective)
            if not self.config.model_name_pattern.allowed(dep_fqn):
                continue
            upstreams.append(
                UpstreamClass(
                    dataset=self._resolve_dep_urn(dep_name, effective, sqlmesh_ctx),
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

        return UpstreamLineageClass(upstreams=upstreams) if upstreams else None

    def _build_column_lineage(
        self,
        model: "SqlmeshModel",
        model_sqlmesh_urn: str,
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
    ) -> List[FineGrainedLineageClass]:
        """
        Build column-level lineage using SQLMesh's column_dependencies API.

        SQLMesh parses all SQL through SQLGlot, so column lineage is available natively
        for SQL models without a separate parsing step. Python DataFrame models may not
        have column-level lineage available.

        The first call per model is CPU-expensive (renders Jinja + qualifies full AST)
        but SQLMesh caches the result per model object identity.
        """
        try:
            from sqlmesh.core.lineage import column_dependencies
        except ImportError:
            logger.debug("sqlmesh.core.lineage not available; skipping column lineage")
            return []

        columns_to_types: Dict[str, Any] = (
            getattr(model, "columns_to_types", None) or {}
        )
        if not columns_to_types:
            return []

        model_name = str(getattr(model, "name", ""))
        convert_lower = (
            self.config.convert_column_urns_to_lowercase
            if self.config.convert_column_urns_to_lowercase is not None
            else effective.convert_urns_to_lowercase
        )

        fine_grained: List[FineGrainedLineageClass] = []
        for col_name in columns_to_types:
            try:
                deps: Dict[str, Set[str]] = column_dependencies(
                    sqlmesh_ctx, model_name, col_name
                )
            except Exception as e:
                # Column lineage is a headline feature (include_column_lineage
                # defaults on), so a parse failure must be visible in the
                # summary — not just counted and logged at debug.
                self.report.num_column_lineage_parse_failures += 1
                self.report.column_lineage_failures.append(f"{model_name}.{col_name}")
                self.report.warning(
                    title="Column lineage extraction failed",
                    message="Could not resolve column dependencies; column lineage skipped for this column (Python model or unsupported SQL).",
                    context=f"{model_name}.{col_name}",
                    exc=e,
                )
                continue

            if not deps:
                continue

            downstream_col = col_name.lower() if convert_lower else col_name
            downstream_field_urn = mce_builder.make_schema_field_urn(
                model_sqlmesh_urn, downstream_col
            )

            upstream_field_urns: List[str] = []
            for upstream_model_name, upstream_cols in deps.items():
                dep_name = str(upstream_model_name)
                # Honour model_name_pattern here too; otherwise a model denied
                # at the table-lineage level reappears as a column-lineage edge.
                dep_effective = self._dep_effective(dep_name, effective, sqlmesh_ctx)
                if not self.config.model_name_pattern.allowed(
                    self._build_logical_fqn(dep_name, dep_effective)
                ):
                    continue
                upstream_dataset_urn = self._resolve_dep_urn(
                    dep_name,
                    effective,
                    sqlmesh_ctx,
                    # Table lineage already counted this dep once; counting again
                    # per column would multiply the reported number by the column
                    # count.
                    count_undeclared=False,
                )
                for upstream_col in upstream_cols:
                    up_col = upstream_col.lower() if convert_lower else upstream_col
                    upstream_field_urns.append(
                        mce_builder.make_schema_field_urn(upstream_dataset_urn, up_col)
                    )

            if upstream_field_urns:
                fine_grained.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=upstream_field_urns,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[downstream_field_urn],
                    )
                )

        return fine_grained

    def _emit_siblings(
        self, sqlmesh_urn: str, warehouse_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Link the SQLMesh entity and its warehouse counterpart as siblings.

        SQLMesh is primary by default (it owns the model definition, lineage and
        descriptions), matching dbt's ``dbt_is_primary_sibling=True``.

        The SQLMesh entity's aspect is written outright — this connector owns
        that entity. The warehouse entity is *patched* instead, so a sibling
        edge added by another connector (dbt, or a second SQLMesh project) isn't
        clobbered, and the workunit is marked non-authoritative because we are
        not the source of truth for warehouse metadata. Same split as dbt.
        """
        sqlmesh_is_primary = self.config.sqlmesh_is_primary_sibling

        # TODO: migrate to SDK V2 when SiblingsClass is supported
        yield MetadataChangeProposalWrapper(
            entityUrn=sqlmesh_urn,
            aspect=SiblingsClass(siblings=[warehouse_urn], primary=sqlmesh_is_primary),
        ).as_workunit()

        warehouse_patch = DatasetPatchBuilder(warehouse_urn)
        warehouse_patch.add_sibling(sqlmesh_urn, primary=not sqlmesh_is_primary)
        for mcp in warehouse_patch.build():
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(mcp),
                mcp_raw=mcp,
                is_primary_source=False,
            )

    # -------------------------------------------------------------------------
    # Audit → Assertion emission
    # -------------------------------------------------------------------------

    def _extract_audit_columns(self, kw: Dict[str, Any]) -> List[str]:
        """Extract column name strings from a SQLGlot Array of Column expressions."""
        col_array = kw.get("columns")
        if col_array is None:
            return []
        try:
            return [
                expr.name
                for expr in col_array.expressions
                if hasattr(expr, "name") and expr.name
            ]
        except Exception:
            # Not fatal — the audit still becomes an assertion, just without
            # per-column targeting — but silently dropping the columns hides a
            # sqlglot shape we don't handle.
            logger.warning(
                "Could not extract audit columns from %r; emitting the audit "
                "without column targets",
                col_array,
                exc_info=True,
            )
            return []

    def _extract_literal_value(self, kw: Dict[str, Any], key: str) -> Optional[str]:
        """Extract a scalar literal value from a SQLGlot Literal expression."""
        expr = kw.get(key)
        if expr is None:
            return None
        try:
            return str(expr.this)
        except Exception:
            return None

    def _assertion_urn(self, dataset_urn: str, audit_name: str, suffix: str) -> str:
        raw = f"{dataset_urn}:{audit_name}:{suffix}"
        return mce_builder.make_assertion_urn(hashlib.md5(raw.encode()).hexdigest())

    # -------------------------------------------------------------------------
    # Audit run events (read from sqlmesh audit --output file)
    # -------------------------------------------------------------------------

    def _emit_audit_run_events(self, path: str) -> Iterable[MetadataWorkUnit]:
        """
        Read a JSON file produced by ``sqlmesh audit --output <file>`` and emit
        AssertionRunEvent aspects so DataHub shows pass/fail on the Data Quality tab.

        Each entry in the file is matched back to an assertion URN using the same
        deterministic hash used in _emit_assertions (model → dataset_urn, audit +
        columns → suffix), so definitions and run events link up automatically.
        """
        try:
            with open(path) as f:
                payload = json.load(f)
        except Exception as e:
            self.report.warning(
                title="Could not read audit results file",
                message="Skipping audit run event emission.",
                context=f"{path}: {e}",
            )
            return

        generated_at = payload.get("metadata", {}).get("generated_at", "")
        try:
            from datetime import datetime

            ts_ms = int(datetime.fromisoformat(generated_at).timestamp() * 1000)
        except Exception:
            ts_ms = int(time.time() * 1000)

        run_id = f"sqlmesh-audit-{ts_ms}"
        results: List[Dict[str, Any]] = payload.get("results", [])
        emitted = 0

        for entry in results:
            try:
                events = list(self._audit_run_events_for_entry(entry, run_id, ts_ms))
            except Exception as e:
                # One malformed entry must not abort the rest of the file.
                self.report.warning(
                    title="Could not emit audit run event",
                    message="An entry in the audit results file was skipped.",
                    context=str(entry),
                    exc=e,
                )
                continue
            emitted += len(events)
            yield from events

        logger.info("Emitted %d assertion run events from %s", emitted, path)

    def _audit_run_events_for_entry(
        self, entry: Dict[str, Any], run_id: str, ts_ms: int
    ) -> Iterable[MetadataWorkUnit]:
        """Turn one audit-results entry into run events (plus incidents on failure)."""
        model_name: str = entry.get("model", "")
        audit_name: str = entry.get("audit", "").lower()
        columns: List[str] = entry.get("columns", [])
        status: str = entry.get("status", "skip")
        failing_rows: int = entry.get("failing_rows", 0)

        if not model_name or not audit_name or status == "skip":
            return

        dataset_urn = self._sqlmesh_urn_for_audit_result(model_name)
        if dataset_urn is None:
            return

        # Suffixes must match what _emit_single_audit used, so run events land
        # on the assertions whose definitions we already emitted.
        params = _SQLMESH_AUDIT_MAP.get(audit_name)
        suffixes = (
            list(columns or [""])
            if params and params.uses_columns
            else [",".join(columns)]
        )
        for suffix in suffixes:
            assertion_urn = self._assertion_urn(dataset_urn, audit_name, suffix)
            yield self._make_run_event(
                assertion_urn, dataset_urn, run_id, ts_ms, status, failing_rows
            )
            if status == "fail":
                yield from self._emit_incident_for_failure(
                    assertion_urn=assertion_urn,
                    dataset_urn=dataset_urn,
                    run_id=run_id,
                    ts_ms=ts_ms,
                    audit_name=audit_name,
                    failing_rows=failing_rows,
                )

    def _sqlmesh_urn_for_audit_result(self, model_name: str) -> Optional[str]:
        """Resolve the SQLMesh URN an audit result belongs to.

        Prefers the URN cached while the model was emitted: rebuilding it from
        ``_resolved_effective`` uses the *default* gateway's platform instance
        and catalog, which is wrong for any model routed through another
        gateway, and would silently produce run events on a URN no assertion
        definition exists for.
        """
        cached = self._sqlmesh_urn_by_model_key.get(model_name)
        if cached is not None:
            return cached

        effective = self._resolved_effective
        if effective is None:
            self.report.warning(
                title="Skipped audit run events for a model",
                message="No SQLMesh model was ingested in this run, so the audit result cannot be matched to an assertion. Ensure project ingestion succeeds before audit results are read.",
                context=model_name,
            )
            return None

        # Not seen during ingestion: filtered out by model_name_pattern /
        # model_kind_filter, renamed, or named differently in the results file.
        # Fall back to the default gateway's config, which is right for
        # single-gateway projects — the common case.
        normalized = self._build_logical_fqn(model_name, effective)
        fallback = self._sqlmesh_urn_by_model_key.get(normalized)
        if fallback is not None:
            return fallback
        self.report.warning(
            title="Audit result for an un-ingested model",
            message="No assertion definition was emitted for this model, so its run events may not link to anything. Check model_name_pattern / model_kind_filter against the audit results file.",
            context=model_name,
        )
        return self._make_sqlmesh_urn(normalized, effective)

    def _emit_incident_for_failure(
        self,
        *,
        assertion_urn: str,
        dataset_urn: str,
        run_id: str,
        ts_ms: int,
        audit_name: str,
        failing_rows: int,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a DataHub Incident pointing at the failing dataset + assertion.

        URN is derived deterministically from (assertion_urn, run_id), so
        re-ingesting the same audit results JSON produces the same incident
        URN and updates the existing entity instead of creating a duplicate.

        Incident type is CUSTOM with customType="SQLMESH_AUDIT" because the
        SQLMesh audit set (not_null, unique_values, forall, ...) doesn't
        cleanly map to FRESHNESS / VOLUME / FIELD / DATA_SCHEMA / SQL. The
        full audit name lives in customType so the UI can render it.
        """
        if not self.config.emit_incidents_on_failure:
            return

        incident_id = hashlib.md5(f"{assertion_urn}:{run_id}".encode()).hexdigest()
        incident_urn = f"urn:li:incident:{incident_id}"

        title = f"SQLMesh audit '{audit_name}' failed ({failing_rows} failing rows)"
        description = (
            f"The `{audit_name}` audit on this dataset failed with "
            f"{failing_rows} failing rows in run {run_id}. See the "
            f"associated assertion for details."
        )
        created = AuditStampClass(
            time=ts_ms,
            actor=mce_builder.make_user_urn("__sqlmesh_ingest__"),
        )
        incident_info = IncidentInfoClass(
            type=IncidentTypeClass.CUSTOM,
            customType=f"SQLMESH_AUDIT/{audit_name}",
            title=title,
            description=description,
            entities=[dataset_urn],
            status=IncidentStatusClass(
                state=IncidentStateClass.ACTIVE,
                lastUpdated=created,
            ),
            source=IncidentSourceClass(
                type=IncidentSourceTypeClass.ASSERTION_FAILURE,
                sourceUrn=assertion_urn,
            ),
            startedAt=ts_ms,
            created=created,
        )
        # Note: deliberately NOT emitting StatusClass on the incident entity —
        # blue (and likely other OSS GMS deployments) registers IncidentInfo
        # as an aspect on Incident but doesn't accept Status on it, returning
        # HTTP 422 "Unknown aspect status for entity incident". The
        # incidentInfo aspect alone is sufficient to create the entity.
        yield MetadataChangeProposalWrapper(
            entityUrn=incident_urn, aspect=incident_info
        ).as_workunit()

    def _make_run_event(
        self,
        assertion_urn: str,
        dataset_urn: str,
        run_id: str,
        ts_ms: int,
        status: str,
        failing_rows: int,
    ) -> MetadataWorkUnit:
        result_type = (
            AssertionResultTypeClass.SUCCESS
            if status == "pass"
            else AssertionResultTypeClass.FAILURE
        )
        return MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=AssertionRunEventClass(
                timestampMillis=ts_ms,
                assertionUrn=assertion_urn,
                asserteeUrn=dataset_urn,
                runId=run_id,
                result=AssertionResultClass(
                    type=result_type,
                    nativeResults={"failing_rows": str(failing_rows)},
                ),
                status=AssertionRunStatusClass.COMPLETE,
            ),
        ).as_workunit()

    def _emit_assertions(
        self,
        model: "SqlmeshModel",
        sqlmesh_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit DataHub Assertion entities for each SQLMesh audit on the model."""
        audits: List[Tuple[str, Dict[str, Any]]] = getattr(model, "audits", None) or []
        for audit_name_raw, kw in audits:
            audit_name = str(audit_name_raw).lower()
            params = _SQLMESH_AUDIT_MAP.get(audit_name)

            try:
                yield from self._emit_single_audit(audit_name, kw, params, sqlmesh_urn)
            except Exception as e:
                self.report.num_assertions_failed += 1
                self.report.warning(
                    title="Failed to emit assertion",
                    message="An audit could not be converted into a DataHub assertion; data-quality metadata for it is missing.",
                    context=f"{audit_name} on {sqlmesh_urn}",
                    exc=e,
                )

    def _emit_pipeline_operation(
        self,
        *,
        sqlmesh_urn: str,
        model: "SqlmeshModel",
        sqlmesh_ctx: Optional["SqlmeshContextType"],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit an ``OperationAspect`` carrying the fingerprint rebuild time.

        ``OperationAspect.lastUpdatedTimestamp`` is the canonical timeseries the
        rest of DataHub reads to answer "when was this dataset last touched?".
        Every warehouse connector emits this aspect for INSERT/UPDATE events on
        the source table; we do the equivalent for SQLMesh by mapping a
        fingerprint rebuild to a single ``CUSTOM`` operation at
        ``snapshot.updated_ts``. Users see the operation history on the
        dataset's Activity tab, and can point a freshness monitor at it.

        Source of the timestamp: ``ctx.snapshots[fqn].updated_ts`` from
        SQLMesh state. Skipped when state is unreachable.
        """
        if not self._capabilities.has_state or sqlmesh_ctx is None:
            return

        try:
            snapshots = sqlmesh_ctx.snapshots
            snapshot = snapshots.get(
                str(getattr(model, "fqn", "") or "")
            ) or snapshots.get(str(getattr(model, "name", "")))
            updated_ts = (
                int(getattr(snapshot, "updated_ts", 0)) if snapshot is not None else 0
            )
        except Exception as e:
            self.report.num_operations_skipped += 1
            self.report.warning(
                title="Could not read snapshot timestamp; skipping operation",
                message="Failed to read snapshot.updated_ts; the pipeline operation aspect (last-rebuild time) is missing for this model.",
                context=str(getattr(model, "name", "?")),
                exc=e,
            )
            return

        if updated_ts <= 0:
            return

        now_ms = int(time.time() * 1000)
        operation = OperationClass(
            timestampMillis=now_ms,
            operationType=OperationTypeClass.CUSTOM,
            customOperationType="SQLMESH_FINGERPRINT_REBUILD",
            lastUpdatedTimestamp=updated_ts,
            actor=mce_builder.make_user_urn("__sqlmesh_ingest__"),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=sqlmesh_urn, aspect=operation
        ).as_workunit()

    def _emit_row_count_profile(
        self,
        *,
        model: "SqlmeshModel",
        sqlmesh_urn: str,
        physical_name: Optional[str] = None,
        sqlmesh_ctx: Optional["SqlmeshContextType"] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit ``DatasetProfile.rowCount`` from a ``SELECT COUNT(*)`` on the
        model's current fingerprint table.

        This is the canonical timeseries warehouse profilers populate, so a
        volume monitor created in DataHub against the SQLMesh entity has real
        history to work with. Cheap on most warehouses (Snowflake/BigQuery serve
        it from metadata; DuckDB streams the table); a full scan on
        Postgres-style engines for huge tables — accepted for now.

        External and embedded models are skipped (no materialised output of
        their own to count).
        """
        kind_name = self._get_kind_name(model) or ""
        if kind_name.upper() in ("EXTERNAL", "EMBEDDED"):
            return

        if not self._capabilities.has_warehouse_query:
            return
        if sqlmesh_ctx is None:
            return

        # Prefer the snapshot's authoritative table_name() because the
        # derived `physical_name` (built from model.data_hash) doesn't
        # always match the hash SQLMesh actually used to materialise the
        # fingerprint. ``snapshot.table_name()`` returns a SQL fragment
        # already quoted for the dialect (e.g. ``"sushi-example".schema.t``)
        # so we splice it directly. The model-attribute fallback is
        # unquoted, so it goes through SQLGlot to be safe.
        live_physical_name = None
        snapshot_provided = False
        try:
            if self._capabilities.has_state:
                snapshots = sqlmesh_ctx.snapshots
                snapshot = snapshots.get(
                    str(getattr(model, "fqn", "") or "")
                ) or snapshots.get(str(getattr(model, "name", "")))
                if snapshot is not None:
                    tn = snapshot.table_name()
                    if tn:
                        live_physical_name = str(tn)
                        snapshot_provided = True
        except Exception as e:
            logger.debug(
                "Could not read snapshot.table_name() for %s (%s); using derived name",
                getattr(model, "name", "?"),
                e,
            )

        if not live_physical_name:
            live_physical_name = physical_name
        if not live_physical_name:
            return

        try:
            if snapshot_provided:
                # SQLMesh's table_name() is already dialect-quoted; splice directly.
                query = f"SELECT COUNT(*) FROM {live_physical_name}"
            else:
                dialect = getattr(sqlmesh_ctx.engine_adapter, "DIALECT", None)
                if not isinstance(dialect, str):
                    dialect = None
                query = _build_count_query(live_physical_name, dialect=dialect)
            row = sqlmesh_ctx.engine_adapter.fetchone(query)
            row_count = int(row[0]) if row and row[0] is not None else 0
        except Exception as e:
            # Profiling was requested; a query failure (permissions, table not
            # materialized, dialect quoting) must surface, not vanish silently.
            self.report.num_profiles_failed += 1
            self.report.warning(
                title="Row-count query failed; skipping profile",
                message="Could not query the physical table row count; the row-count profile is missing for this model.",
                context=live_physical_name,
                exc=e,
            )
            return

        ts_ms = int(time.time() * 1000)
        profile = DatasetProfileClass(
            timestampMillis=ts_ms,
            rowCount=row_count,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=sqlmesh_urn, aspect=profile
        ).as_workunit()

    def _audit_native_parameters(self, kw: Dict[str, Any]) -> Optional[str]:
        """JSON-encode an audit's kwargs as flat key → string pairs.

        SQLMesh hands audit arguments over as SQLGlot expressions whose default
        repr is the whole parse tree, so long values are truncated rather than
        dumped into a custom property.
        """
        rendered: Dict[str, str] = {}
        for key, value in (kw or {}).items():
            text = str(value)
            rendered[str(key)] = text if len(text) <= 200 else text[:200] + "…"
        return json.dumps(rendered, sort_keys=True) if rendered else None

    def _emit_custom_audit(
        self,
        audit_name: str,
        kw: Dict[str, Any],
        params: Optional[_AuditAssertionParams],
        dataset_urn: str,
        *,
        assertion_urn: str,
        field_urn: Optional[str] = None,
        extra_properties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit one SQLMesh audit as an ``AssertionTypeClass.CUSTOM`` assertion.

        CUSTOM is the honest type: SQLMesh executes these audits itself as part
        of ``sqlmesh run`` / ``sqlmesh audit``, and DataHub only records the
        definition plus whatever results arrive through ``audit_results_path``.
        Typing them as DATASET or SQL implied DataHub could evaluate them, which
        it can't — and the SQL variant needed a fake ``SELECT 0`` statement to
        satisfy the schema.

        The audit's semantics (scope / operator / aggregation for the built-ins)
        and its arguments are carried as custom properties so the check stays
        inspectable in the UI.
        """
        custom_properties: Dict[str, str] = {"sqlmesh.audit": audit_name}
        if params is not None:
            custom_properties["sqlmesh.scope"] = params.scope
            custom_properties["sqlmesh.operator"] = params.operator
            custom_properties["sqlmesh.aggregation"] = params.aggregation
        native_parameters = self._audit_native_parameters(kw)
        if native_parameters:
            custom_properties["sqlmesh.native_parameters"] = native_parameters
        if extra_properties:
            custom_properties.update(extra_properties)

        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.CUSTOM,
            source=mce_builder.make_assertion_source(),
            customProperties=custom_properties,
            description=f"SQLMesh audit '{audit_name}'. Executed by SQLMesh; results are ingested from audit_results_path.",
            customAssertion=CustomAssertionInfoClass(
                type="SQLMesh",
                entity=dataset_urn,
                field=field_urn,
                logic=self._extract_audit_logic(kw),
            ),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn, aspect=assertion_info
        ).as_workunit()

    def _extract_audit_logic(self, kw: Dict[str, Any]) -> Optional[str]:
        """Return the audit's own SQL when SQLMesh exposes it in the kwargs.

        Non-standard audits carry their predicate under ``criteria`` /
        ``condition``. When neither is present we leave ``logic`` unset rather
        than inventing a statement — the authoritative SQL lives in the model
        file.
        """
        for key in ("criteria", "condition"):
            expr = (kw or {}).get(key)
            if expr is None:
                continue
            try:
                return str(expr.sql())
            except Exception:
                return str(expr)
        return None

    def _emit_single_audit(
        self,
        audit_name: str,
        kw: Dict[str, Any],
        params: Optional[_AuditAssertionParams],
        dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        if params is None:
            # Unknown / custom audit: no semantic properties to carry, and no
            # column targeting since we can't tell which kwarg is a column.
            yield from self._emit_custom_audit(
                audit_name,
                kw,
                params,
                dataset_urn,
                assertion_urn=self._assertion_urn(dataset_urn, audit_name, ""),
            )
            return

        cols = self._extract_audit_columns(kw)

        if params.uses_columns:
            # Column-level: one assertion per column.
            for col in cols or [""]:
                extra: Dict[str, str] = {}
                if audit_name == "accepted_range":
                    min_v = self._extract_literal_value(kw, "min_v")
                    max_v = self._extract_literal_value(kw, "max_v")
                    if min_v is not None:
                        extra["sqlmesh.min_value"] = min_v
                    if max_v is not None:
                        extra["sqlmesh.max_value"] = max_v
                elif audit_name == "accepted_values":
                    values = self._extract_expression_values(kw, "values")
                    if values:
                        extra["sqlmesh.accepted_values"] = ",".join(values)

                yield from self._emit_custom_audit(
                    audit_name,
                    kw,
                    params,
                    dataset_urn,
                    assertion_urn=self._assertion_urn(dataset_urn, audit_name, col),
                    field_urn=(
                        mce_builder.make_schema_field_urn(dataset_urn, col)
                        if col
                        else None
                    ),
                    extra_properties=extra,
                )
            return

        # Dataset-level: one assertion covering all columns the audit names.
        extra = {}
        if cols:
            extra["sqlmesh.fields"] = ",".join(cols)
        if params.row_count_threshold:
            threshold = self._extract_literal_value(kw, "threshold")
            if threshold is not None:
                extra["sqlmesh.threshold"] = threshold

        yield from self._emit_custom_audit(
            audit_name,
            kw,
            params,
            dataset_urn,
            assertion_urn=self._assertion_urn(dataset_urn, audit_name, ",".join(cols)),
            extra_properties=extra,
        )

    def _extract_expression_values(self, kw: Dict[str, Any], key: str) -> List[str]:
        """Extract scalar literals from a SQLGlot expression list (e.g. IN values)."""
        expr = kw.get(key)
        if expr is None:
            return []
        try:
            return [str(e.this) for e in expr.expressions]
        except Exception:
            logger.warning(
                "Could not extract %r values from audit kwargs", key, exc_info=True
            )
            return []
