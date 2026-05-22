import contextlib
import hashlib
import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
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
    # sqlmesh is an optional dependency; callers must check `SqlmeshContext is None` before use
    SqlmeshContext = None  # type: ignore[assignment,misc]

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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.ingestion.source.sqlmesh.sqlmesh_config import (
    SQLMESH_TO_DATAHUB_PLATFORM,
    SqlmeshSourceConfig,
    SqlmeshSourceReport,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
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
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    CalendarIntervalClass,
    DataPlatformInfoClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    FixedIntervalScheduleClass,
    FreshnessAssertionInfoClass,
    FreshnessAssertionScheduleClass,
    FreshnessAssertionScheduleTypeClass,
    FreshnessAssertionTypeClass,
    NullTypeClass,
    PlatformTypeClass,
    RowCountTotalClass,
    SiblingsClass,
    StatusClass,
    VolumeAssertionInfoClass,
    VolumeAssertionTypeClass,
)
from datahub.sdk import Dataset
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
    ``sqlmesh.Context`` trips two distinct failures (described by Gen Digital's
    customer):

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


def _install_tobiko_local_state_fallback_shim() -> None:
    """When tobikodata is installed but no Tobiko Cloud token is configured,
    let SQLMesh's Context init succeed against an EnterpriseConfig project by
    swapping the cloud state sync for an in-memory DuckDB stub.

    The shim only catches the very specific ConfigError raised by
    ``RemoteCloudSchedulerConfig.get_cloud_connection()`` when creds are
    absent; any other scheduler failure surfaces. No-op when tobikodata isn't
    installed (OSS-only projects don't have a cloud scheduler to patch).
    """
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
            logger.info(
                "Tobiko Cloud state store unreachable (no token configured). "
                "Falling back to an in-memory DuckDB state so the SQLMesh "
                "Context can initialise from project files. Snapshot history "
                "and environment promotions read from cloud state are "
                "unavailable in this mode. Set tobiko_cloud_token / "
                "tobiko_cloud_token_file to read from the real cloud state."
            )
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
    pattern but narrow the window the token sits in /proc/<pid>/environ to a
    single SqlmeshContext.__init__ call. No-op when no token is configured.
    """
    if token is None or gateway is None:
        yield
        return

    keys = _tobiko_state_connection_env_keys(gateway)
    tracked = [keys["TYPE"], keys["TOKEN"], keys["URL"], "SQLMESH__DEFAULT_GATEWAY"]
    saved: Dict[str, Optional[str]] = {k: os.environ.get(k) for k in tracked}

    os.environ[keys["TYPE"]] = "cloud"
    os.environ[keys["TOKEN"]] = token
    if url:
        os.environ[keys["URL"]] = url
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


# Maps SQLMesh built-in audit names to DataHub assertion parameters.
# Audits not listed here fall back to a DATASET_ROWS / _NATIVE_ assertion.
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


# Freshness SLA windows derived from a model's interval_unit. The values are
# (CalendarIntervalClass.X, multiple) tuples meaning "the model is considered
# stale if it hasn't been refreshed in N units". Roughly 3× the cron interval
# with a 1-hour floor for sub-hourly schedules — tight enough to catch a
# stalled pipeline within a couple of missed runs, loose enough to absorb
# normal scheduling jitter.
#
# Keys are SQLMesh IntervalUnit string values (see sqlmesh.core.node.IntervalUnit).
_INTERVAL_UNIT_TO_SLA: Dict[str, Tuple[str, int]] = {
    "five_minute": (CalendarIntervalClass.HOUR, 1),
    "quarter_hour": (CalendarIntervalClass.HOUR, 1),
    "half_hour": (CalendarIntervalClass.HOUR, 2),
    "hour": (CalendarIntervalClass.HOUR, 3),
    "day": (CalendarIntervalClass.HOUR, 36),
    "month": (CalendarIntervalClass.DAY, 35),
    "year": (CalendarIntervalClass.DAY, 366),
}
# Fallback when interval_unit is missing or unrecognised (rare; only happens
# for embedded/external models with no cron).
_DEFAULT_FRESHNESS_SLA: Tuple[str, int] = (CalendarIntervalClass.HOUR, 24)


def _freshness_sla_for_model(model: "SqlmeshModel") -> Tuple[str, int]:
    """Pick a (CalendarInterval, multiple) SLA window for the given model.

    Reads ``model.interval_unit`` (SQLMesh's inferred granularity from the
    cron string). Returns a sensible default when interval_unit isn't set
    or maps to an unknown value.
    """
    interval_unit = getattr(model, "interval_unit", None)
    if interval_unit is None:
        return _DEFAULT_FRESHNESS_SLA
    # IntervalUnit is a str-Enum in sqlmesh; both .value and str() work.
    key = getattr(interval_unit, "value", str(interval_unit)).lower()
    return _INTERVAL_UNIT_TO_SLA.get(key, _DEFAULT_FRESHNESS_SLA)


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
    env_catalog_mapping: Dict[str, str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.env_catalog_mapping is None:
            self.env_catalog_mapping = {}


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
) -> _CapabilityProbes:
    """Probe each signal once. Failures degrade gracefully — the emitter
    layer decides how to handle a missing capability (skip with info log,
    fall back to a cheaper signal, etc.).
    """
    probes = _CapabilityProbes(has_graph=graph is not None)

    # State probe: smallest possible call into the state reader. Listing
    # environments hits the state store but doesn't load any per-model
    # snapshot detail, so it's cheap even on large projects.
    try:
        sqlmesh_ctx.state_reader.get_environments()
        probes.has_state = True
    except Exception as e:
        logger.info(
            "State store unreachable; pipeline-rebuild-lag freshness will "
            "fall back to engine-adapter INFORMATION_SCHEMA. (%s)",
            e,
        )

    # Warehouse-query probe: ping the engine adapter. We use a no-op
    # `SELECT 1` rather than schema introspection so the probe stays
    # uniform across dialects.
    try:
        sqlmesh_ctx.engine_adapter.fetchone("SELECT 1")
        probes.has_warehouse_query = True
    except Exception as e:
        logger.info(
            "Data warehouse unreachable via engine adapter; volume + "
            "pipeline-freshness will fall back to DataHub Graph profile "
            "where available. (%s)",
            e,
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

    def __init__(self, config: SqlmeshSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
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
        # Capability probes (set after Context loads). Emitters consult these
        # to choose signal sources; e.g. pipeline-freshness prefers state but
        # falls back to engine_adapter, volume prefers engine_adapter but
        # falls back to Graph profile.
        self._capabilities: _CapabilityProbes = _CapabilityProbes()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SqlmeshSource":
        config = SqlmeshSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> SqlmeshSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        processors = super().get_workunit_processors()
        processors.append(
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor
        )
        return processors

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self._emit_platform_registration()
        yield from self._ingest_project()
        if self.config.audit_results_path:
            yield from self._emit_audit_run_events(self.config.audit_results_path)

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
            logger.warning(
                "Could not auto-detect target_platform from gateway connection: %s. "
                "Set target_platform explicitly in your recipe config.",
                e,
            )
            return "unknown"

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
            _install_tobiko_local_state_fallback_shim()

        try:
            logger.info(
                "Acquiring SQLMesh context load lock for project: %s",
                effective.project_path,
            )
            with (
                self.report.context_load_sec,
                _sqlmesh_context_load_lock,
                _scoped_tobiko_cloud_env(
                    token=tobiko_token,
                    gateway=effective.gateway,
                    url=self.config.tobiko_cloud_url,
                ),
            ):
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

        # Probe capabilities once. The result drives fallback decisions for
        # freshness and volume assertions later in the pipeline.
        self._capabilities = _probe_capabilities(sqlmesh_ctx, self.ctx.graph)
        self.report.has_state_store_access = self._capabilities.has_state
        self.report.has_warehouse_query_access = self._capabilities.has_warehouse_query
        self.report.has_graph_access = self._capabilities.has_graph
        logger.info(
            "SQLMesh capability probes: state=%s warehouse=%s graph=%s",
            self._capabilities.has_state,
            self._capabilities.has_warehouse_query,
            self._capabilities.has_graph,
        )

        # Resolve target_platform (auto-detect if not configured)
        target_platform = self._detect_target_platform(sqlmesh_ctx, effective)

        # Read environment suffix config directly from the loaded Context — no user config needed.
        env_suffix_target = "schema"
        env_catalog_mapping: Dict[str, str] = {}
        try:
            env_suffix_target = (
                str(sqlmesh_ctx.config.environment_suffix_target).split(".")[-1].lower()
            )  # e.g. "EnvironmentSuffixTarget.SCHEMA" → "schema"
            env_catalog_mapping = dict(
                getattr(sqlmesh_ctx.config, "environment_catalog_mapping", {}) or {}
            )
        except Exception as e:
            logger.debug("Could not read environment suffix config from context: %s", e)

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
        all_fqns: Dict[str, Any] = {}  # fqn → model
        for model_name_key, model in sqlmesh_ctx.models.items():
            fqn = self._build_logical_fqn(str(model_name_key), effective)
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

        # Changed-only mode (Phase 7) — filter to models with changed fingerprints.
        if self.config.incremental_mode == "changed":
            all_fqns = self._filter_changed_models(all_fqns, sqlmesh_ctx, effective)

        for fqn, model in all_fqns.items():
            self.report.models_scanned += 1
            try:
                yield from self._emit_model(
                    model, fqn, physical_name_by_model, effective, sqlmesh_ctx
                )
            except Exception as e:
                logger.warning("Failed to process model %s: %s", fqn, e, exc_info=True)
                self.report.report_model_failed(fqn, str(e))

        # Release state-sync and evaluator resources so that repeated Context()
        # calls in the same process (e.g. multi-project recipes) don't accumulate
        # open connections or file handles.
        sqlmesh_ctx.close()

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

    def _filter_changed_models(
        self,
        all_fqns: Dict[str, Any],
        sqlmesh_ctx: "SqlmeshContextType",
        effective: _EffectiveProjectConfig,
    ) -> Dict[str, Any]:
        """
        [NOT YET FULLY IMPLEMENTED] Planned to return only models whose snapshot
        fingerprint changed since the last run. Requires a custom CheckpointState
        subclass to persist fingerprints between runs — that infrastructure does not
        yet exist. Always falls back to full mode until implemented.

        TODO: implement SqlmeshCheckpointState(CheckpointStateBase) with a
        sqlmesh_fingerprints: Dict[str, str] field, register it with the stateful
        ingestion framework, then compare current vs stored fingerprints here.
        """
        logger.warning(
            "incremental_mode='changed' is not yet fully implemented — "
            "requires a custom CheckpointState subclass to persist fingerprints "
            "between runs. Processing all %d models (full mode).",
            len(all_fqns),
        )
        return all_fqns

    # -------------------------------------------------------------------------
    # Phase 8: URN preview / dry-run
    # -------------------------------------------------------------------------

    def _log_urn_preview(
        self, all_fqns: Dict[str, Any], effective: _EffectiveProjectConfig
    ) -> None:
        """
        Log a sample of sqlmesh ↔ warehouse URN pairs before emitting.
        Helps users validate that sibling URNs will match their warehouse connector.
        """
        sample = list(all_fqns.keys())[: self.config.preview_urns_sample_size]
        lines = ["URN preview (sqlmesh → warehouse sibling):"]
        for fqn in sample:
            sqlmesh_urn = self._make_sqlmesh_urn(fqn, effective)
            warehouse_urn = self._make_warehouse_urn(fqn, effective)
            lines.append(f"  sqlmesh : {sqlmesh_urn}")
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
                    phys = self._normalize_name(".".join(parts), effective)
                    logical_fqn = self._build_logical_fqn(model_name, effective)
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
            result = snapshot.table_name
            if callable(result):
                result = result()
            return self._normalize_name(str(result), effective) if result else None
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
            owners=[owner_urn] if owner_urn else None,
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
        yield from self._emit_freshness_assertions(model, sqlmesh_urn)
        yield from self._emit_volume_assertions(model, sqlmesh_urn)

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

    def _resolve_dep_urn(
        self,
        dep_name: str,
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
    ) -> str:
        """
        Map a dependency name to the correct DataHub URN using 3-category logic.

        Category 1 — managed model: sqlmesh URN
        Category 2 — declared external (EXTERNAL kind): sqlmesh URN by default,
                      warehouse URN when skip_external_models_in_lineage=True
        Category 3 — undeclared implicit (get_model returns None): warehouse URN
        """
        dep_fqn = self._build_logical_fqn(dep_name, effective)
        dep_model = sqlmesh_ctx.get_model(dep_name)

        if dep_model is None:
            logger.debug(
                "Dep %r not in SQLMesh context; routing lineage to warehouse URN",
                dep_name,
            )
            self.report.num_undeclared_upstream_refs += 1
            return self._make_warehouse_urn(dep_fqn, effective)

        kind = getattr(dep_model, "kind", None)
        is_external = str(getattr(kind, "model_kind_name", "")).upper() == "EXTERNAL"
        if is_external and self.config.skip_external_models_in_lineage:
            return self._make_warehouse_urn(dep_fqn, effective)
        return self._make_sqlmesh_urn(dep_fqn, effective)

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
            dep_fqn = self._build_logical_fqn(str(dep), effective)
            if not self.config.model_name_pattern.allowed(dep_fqn):
                continue
            upstreams.append(
                UpstreamClass(
                    dataset=self._resolve_dep_urn(str(dep), effective, sqlmesh_ctx),
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
                self.report.num_column_lineage_parse_failures += 1
                logger.debug(
                    "column_dependencies failed for %s.%s: %s — "
                    "skipping column lineage for this column (Python model or unsupported SQL)",
                    model_name,
                    col_name,
                    e,
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
                upstream_dataset_urn = self._resolve_dep_urn(
                    str(upstream_model_name), effective, sqlmesh_ctx
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
        # SQLMesh is primary by default (owns model definition, lineage, descriptions)
        # matching dbt's dbt_is_primary_sibling=True default.
        if self.config.sqlmesh_is_primary_sibling:
            primary_urn, secondary_urn = sqlmesh_urn, warehouse_urn
        else:
            primary_urn, secondary_urn = warehouse_urn, sqlmesh_urn

        # TODO: migrate to SDK V2 when SiblingsClass is supported
        yield MetadataChangeProposalWrapper(
            entityUrn=primary_urn,
            aspect=SiblingsClass(siblings=[secondary_urn], primary=True),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=secondary_urn,
            aspect=SiblingsClass(siblings=[primary_urn], primary=False),
        ).as_workunit()

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
            model_name: str = entry.get("model", "")
            audit_name: str = entry.get("audit", "").lower()
            columns: List[str] = entry.get("columns", [])
            status: str = entry.get("status", "skip")
            failing_rows: int = entry.get("failing_rows", 0)

            if not model_name or not audit_name or status == "skip":
                continue

            # Build the same SQLMesh URN _emit_assertions used so the derived
            # assertion hash matches. Requires _ingest_project to have populated
            # self._resolved_effective so env_suffix_target / env_catalog_mapping
            # / sqlmesh_platform_instance flow through FQN construction (the
            # previous synthetic minimal config hardcoded environment="prod"
            # and dropped these, producing wrong URNs in non-default setups).
            effective = self._resolved_effective
            if effective is None:
                logger.warning(
                    "Skipping audit run events for %s — _ingest_project must "
                    "succeed first to populate the resolved project config used "
                    "for FQN construction.",
                    model_name,
                )
                continue
            fqn = self._build_logical_fqn(model_name, effective)
            dataset_urn = self._make_sqlmesh_urn(fqn, effective)

            # Suffix matches what _emit_single_audit uses
            params = _SQLMESH_AUDIT_MAP.get(audit_name)
            if params and params.uses_columns:
                # one run event per column
                for col in columns or [""]:
                    assertion_urn = self._assertion_urn(dataset_urn, audit_name, col)
                    yield self._make_run_event(
                        assertion_urn, dataset_urn, run_id, ts_ms, status, failing_rows
                    )
                    emitted += 1
            else:
                # dataset-level: single run event, suffix = comma-joined columns
                suffix = ",".join(columns)
                assertion_urn = self._assertion_urn(dataset_urn, audit_name, suffix)
                yield self._make_run_event(
                    assertion_urn, dataset_urn, run_id, ts_ms, status, failing_rows
                )
                emitted += 1

        logger.info("Emitted %d assertion run events from %s", emitted, path)

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
                logger.debug(
                    "Failed to emit assertion for audit %r on %s: %s",
                    audit_name,
                    sqlmesh_urn,
                    e,
                )

    def _emit_freshness_assertions(
        self,
        model: "SqlmeshModel",
        sqlmesh_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit two FRESHNESS assertions per model:

          1. ``pipeline_freshness`` — fails when SQLMesh stops rebuilding the
             fingerprint table on schedule (plan/apply skipped, failed, paused).
          2. ``upstream_freshness`` — fails when MIN(upstream lastModified) is
             older than the SLA, signalling that source data is behind.

        Both use the same SLA window (3× the model's cron interval, with a
        1-hour floor) — separation is purely diagnostic so the Validation tab
        shows which side broke. Customer-side tuning happens by overriding
        the assertion on DataHub directly; we don't expose per-model thresholds
        in config to keep the connector declarative.

        Only emits the assertion **definitions** here. The actual evaluation
        is performed by DataHub's monitor framework against the dataset's
        lastModified property (set by the warehouse connector / freshness
        compute). On OSS without monitors, the assertions appear in the UI
        but won't fire automatically — a future commit will add explicit
        AssertionRunEvent emission for OSS users.
        """
        if not self.config.emit_freshness_assertions:
            return

        # External and embedded models have no rebuild schedule — they're
        # source tables (external) or inlined (embedded). Skip freshness on
        # them; we'd be asserting against data we don't produce.
        kind_name = self._get_kind_name(model) or ""
        if kind_name.upper() in ("EXTERNAL", "EMBEDDED"):
            return

        unit, multiple = _freshness_sla_for_model(model)
        schedule = FreshnessAssertionScheduleClass(
            type=FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
            fixedInterval=FixedIntervalScheduleClass(unit=unit, multiple=multiple),
        )

        for kind, description in (
            (
                "pipeline_freshness",
                "SQLMesh fingerprint table must be rebuilt within "
                f"{multiple} {unit.lower()}(s) of its cron schedule. Fires "
                "when plan/apply stops running, signalling pipeline drift.",
            ),
            (
                "upstream_freshness",
                "Upstream source tables must have been updated within "
                f"{multiple} {unit.lower()}(s). Fires when raw data feeding "
                "the model is behind schedule.",
            ),
        ):
            assertion_urn = self._freshness_assertion_urn(sqlmesh_urn, kind)
            assertion_info = AssertionInfoClass(
                type=AssertionTypeClass.FRESHNESS,
                source=mce_builder.make_assertion_source(),
                customProperties=self._anomaly_custom_props(
                    {
                        "sqlmesh.freshness_kind": kind,
                        "sqlmesh.interval_unit": str(
                            getattr(model, "interval_unit", "") or ""
                        ),
                    }
                ),
                description=description,
                freshnessAssertion=FreshnessAssertionInfoClass(
                    type=FreshnessAssertionTypeClass.DATASET_CHANGE,
                    entity=sqlmesh_urn,
                    schedule=schedule,
                ),
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=StatusClass(removed=False)
            ).as_workunit()
            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=assertion_info
            ).as_workunit()

    def _freshness_assertion_urn(self, sqlmesh_urn: str, kind: str) -> str:
        """Stable urn:li:assertion:... for the named freshness assertion."""
        raw = f"{sqlmesh_urn}:freshness:{kind}"
        return mce_builder.make_assertion_urn(hashlib.md5(raw.encode()).hexdigest())

    def _volume_assertion_urn(self, sqlmesh_urn: str) -> str:
        """Stable urn:li:assertion:... for the model's row-count assertion."""
        raw = f"{sqlmesh_urn}:volume:row_count"
        return mce_builder.make_assertion_urn(hashlib.md5(raw.encode()).hexdigest())

    def _emit_volume_assertions(
        self,
        model: "SqlmeshModel",
        sqlmesh_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a VOLUME assertion per model — row count must be >= 1.

        The "at least one row" floor is universally true for healthy models
        and detects the most catastrophic failure mode (empty table after
        rebuild). Tighter expected-value thresholds aren't predictable from
        the model definition alone — those come from anomaly detection on
        the run-event history that DataHub monitors accumulate over time.

        When emit_smart_assertion_anomaly_detection is set, a customProperty
        marker requests Acryl Cloud's monitor framework to treat the
        threshold as a baseline rather than a hard rule, so the ML detector
        flags drops below historical norms even when the count stays >= 1.

        External and embedded models are skipped (no warehouse output to
        count).
        """
        if not self.config.emit_volume_assertions:
            return

        kind_name = self._get_kind_name(model) or ""
        if kind_name.upper() in ("EXTERNAL", "EMBEDDED"):
            return

        assertion_urn = self._volume_assertion_urn(sqlmesh_urn)
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.VOLUME,
            source=mce_builder.make_assertion_source(),
            customProperties=self._anomaly_custom_props(
                {"sqlmesh.volume_kind": "row_count_min"}
            ),
            description=(
                "Row count must be at least 1. Detects catastrophic rebuild "
                "failures (empty table). Cloud anomaly detection, when "
                "opted in, flags drops below historical norms."
            ),
            volumeAssertion=VolumeAssertionInfoClass(
                type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity=sqlmesh_urn,
                rowCountTotal=RowCountTotalClass(
                    operator=AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                    parameters=AssertionStdParametersClass(
                        value=AssertionStdParameterClass(
                            value="1",
                            type=AssertionStdParameterTypeClass.NUMBER,
                        ),
                    ),
                ),
            ),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn, aspect=assertion_info
        ).as_workunit()

    def _anomaly_custom_props(
        self, base: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """Return customProperties dict with the anomaly-detection opt-in marker
        applied when config.emit_smart_assertion_anomaly_detection is set.

        Acryl Cloud's monitor framework reads this marker to decide whether
        to wrap the assertion's static threshold in its ML anomaly detector
        (so a drop-below-historical-baseline fires even when the static rule
        passes). The marker is silently ignored on OSS DataHub — assertions
        still evaluate as static pass/fail.
        """
        props = dict(base or {})
        if self.config.emit_smart_assertion_anomaly_detection:
            props["sqlmesh.anomaly_detection"] = "requested"
        return props

    def _emit_single_audit(
        self,
        audit_name: str,
        kw: Dict[str, Any],
        params: Optional[_AuditAssertionParams],
        dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        if params is None:
            # Unknown audit type — emit a single native dataset-rows assertion.
            assertion_urn = self._assertion_urn(dataset_urn, audit_name, "")
            assertion_info = AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                source=mce_builder.make_assertion_source(),
                customProperties=self._anomaly_custom_props(
                    {"sqlmesh.audit": audit_name}
                ),
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=dataset_urn,
                    scope=DatasetAssertionScopeClass.DATASET_ROWS,
                    operator=AssertionStdOperatorClass._NATIVE_,
                    aggregation=AssertionStdAggregationClass._NATIVE_,
                    nativeType=audit_name,
                ),
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=StatusClass(removed=False)
            ).as_workunit()
            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=assertion_info
            ).as_workunit()
            return

        if params.uses_columns:
            # Column-level: one assertion per column.
            cols = self._extract_audit_columns(kw)
            for col in cols or [""]:
                field_urn = (
                    mce_builder.make_schema_field_urn(dataset_urn, col) if col else None
                )
                assertion_urn = self._assertion_urn(dataset_urn, audit_name, col)

                std_params: Optional[AssertionStdParametersClass] = None
                if audit_name == "unique_values":
                    std_params = AssertionStdParametersClass(
                        value=AssertionStdParameterClass(
                            value="1.0", type=AssertionStdParameterTypeClass.NUMBER
                        )
                    )
                elif audit_name == "accepted_range":
                    min_v = self._extract_literal_value(kw, "min_v")
                    max_v = self._extract_literal_value(kw, "max_v")
                    std_params = AssertionStdParametersClass(
                        minValue=AssertionStdParameterClass(
                            value=min_v or "0",
                            type=AssertionStdParameterTypeClass.NUMBER,
                        ),
                        maxValue=AssertionStdParameterClass(
                            value=max_v or "0",
                            type=AssertionStdParameterTypeClass.NUMBER,
                        ),
                    )
                elif audit_name == "accepted_values":
                    vals_expr = kw.get("values")
                    vals = []
                    if vals_expr is not None:
                        try:
                            vals = [str(e.this) for e in vals_expr.expressions]
                        except Exception:
                            pass
                    std_params = AssertionStdParametersClass(
                        value=AssertionStdParameterClass(
                            value=str(vals), type=AssertionStdParameterTypeClass.SET
                        )
                    )

                assertion_info = AssertionInfoClass(
                    type=AssertionTypeClass.DATASET,
                    source=mce_builder.make_assertion_source(),
                    customProperties=self._anomaly_custom_props(
                        {"sqlmesh.audit": audit_name}
                    ),
                    datasetAssertion=DatasetAssertionInfoClass(
                        dataset=dataset_urn,
                        scope=params.scope,
                        operator=params.operator,
                        aggregation=params.aggregation,
                        fields=[field_urn] if field_urn else [],
                        nativeType=audit_name,
                        parameters=std_params,
                    ),
                )
                yield MetadataChangeProposalWrapper(
                    entityUrn=assertion_urn, aspect=StatusClass(removed=False)
                ).as_workunit()
                yield MetadataChangeProposalWrapper(
                    entityUrn=assertion_urn, aspect=assertion_info
                ).as_workunit()
        else:
            # Dataset-level: one assertion covering all columns.
            cols = self._extract_audit_columns(kw)
            col_suffix = ",".join(cols)
            assertion_urn = self._assertion_urn(dataset_urn, audit_name, col_suffix)

            std_params = None
            if params.row_count_threshold:
                threshold = self._extract_literal_value(kw, "threshold")
                std_params = AssertionStdParametersClass(
                    value=AssertionStdParameterClass(
                        value=threshold or "0",
                        type=AssertionStdParameterTypeClass.NUMBER,
                    )
                )

            field_urns = [
                mce_builder.make_schema_field_urn(dataset_urn, c) for c in cols
            ]
            assertion_info = AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                source=mce_builder.make_assertion_source(),
                customProperties=self._anomaly_custom_props(
                    {"sqlmesh.audit": audit_name}
                ),
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=dataset_urn,
                    scope=params.scope,
                    operator=params.operator,
                    aggregation=params.aggregation,
                    fields=field_urns,
                    nativeType=audit_name,
                    parameters=std_params,
                ),
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=StatusClass(removed=False)
            ).as_workunit()
            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn, aspect=assertion_info
            ).as_workunit()
