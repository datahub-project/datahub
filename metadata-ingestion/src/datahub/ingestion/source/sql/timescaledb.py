import json
import logging
import time
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union

from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import text
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import DatabaseError

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.common.subtypes import (
    DatasetSubTypes,
    FlowContainerSubTypes,
    JobContainerSubTypes,
)
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_utils import gen_database_key, gen_schema_key
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure
from datahub.metadata.schema_classes import (
    AuditStampClass,
    AzkabanJobTypeClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    RunResultTypeClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
)
from datahub.utilities.str_enum import StrEnum

logger: logging.Logger = logging.getLogger(__name__)

TableMetadataTuple = Tuple[Optional[str], Dict[str, str], Optional[str]]


class _TimescaleDBQueries:
    """SQL queries for TimescaleDB metadata extraction"""

    HYPERTABLES = """
        SELECT 
            ht.hypertable_name,
            ht.num_dimensions,
            ht.num_chunks,
            ht.compression_enabled,
            (
                SELECT json_agg(json_build_object(
                    'column_name', d.column_name,
                    'column_type', d.column_type,
                    'time_interval', d.time_interval::text,
                    'integer_interval', d.integer_interval,
                    'num_partitions', d.num_partitions
                ))
                FROM timescaledb_information.dimensions d
                WHERE d.hypertable_schema = ht.hypertable_schema 
                    AND d.hypertable_name = ht.hypertable_name
            ) as dimensions,
            (
                SELECT json_build_object('drop_after', j.config->>'drop_after')
                FROM timescaledb_information.jobs j
                WHERE j.hypertable_schema = ht.hypertable_schema
                    AND j.hypertable_name = ht.hypertable_name
                    AND j.proc_name = :retention_policy_name
                LIMIT 1
            ) as retention_policy
        FROM timescaledb_information.hypertables ht
        WHERE ht.hypertable_schema = :schema
        """

    CONTINUOUS_AGGREGATES = """
        SELECT 
            ca.view_name,
            ca.materialized_only,
            ca.compression_enabled,
            ca.hypertable_schema,
            ca.hypertable_name,
            COALESCE(pv.definition, ca.view_definition) as view_definition,
            (
                SELECT json_build_object(
                    'schedule_interval', j.schedule_interval::text,
                    'config', j.config
                )
                FROM timescaledb_information.jobs j
                WHERE j.hypertable_schema = ca.materialization_hypertable_schema
                    AND j.hypertable_name = ca.materialization_hypertable_name
                    AND j.proc_name = :refresh_policy_name
                LIMIT 1
            ) as refresh_policy
        FROM timescaledb_information.continuous_aggregates ca
        LEFT JOIN pg_views pv ON pv.viewname = ca.view_name AND pv.schemaname = ca.view_schema
        WHERE ca.view_schema = :schema
        """

    JOBS = """
        SELECT 
            j.job_id,
            j.application_name,
            j.schedule_interval,
            j.max_runtime,
            j.max_retries,
            j.retry_period,
            j.proc_schema,
            j.proc_name,
            j.scheduled,
            j.fixed_schedule,
            j.initial_start,
            j.config,
            j.hypertable_schema,
            j.hypertable_name
        FROM timescaledb_information.jobs j
        WHERE (j.hypertable_schema = :schema OR j.proc_schema = :schema)
        """

    JOB_EXECUTION_HISTORY = """
        SELECT 
            js.job_id,
            js.last_run_started_at,
            js.last_successful_finish,
            js.last_run_status,
            js.total_runs,
            js.total_successes,
            js.total_failures,
            js.total_crashes,
            js.consecutive_failures,
            js.consecutive_crashes
        FROM timescaledb_information.job_stats js
        WHERE js.job_id = :job_id
        LIMIT :limit
        """


class _TimescaleDBConstants:
    """Platform and URN identifiers"""

    PLATFORM = "timescaledb"
    BACKGROUND_JOBS_FLOW_SUFFIX = "background_jobs"


class _TimescaleDBTags:
    """Tag names for datasets"""

    HYPERTABLE = "hypertable"
    CONTINUOUS_AGGREGATE = "continuous_aggregate"


class _TimescaleDBPolicies:
    """TimescaleDB policy procedure names"""

    REFRESH_CONTINUOUS_AGGREGATE = "policy_refresh_continuous_aggregate"
    RETENTION = "policy_retention"
    COMPRESSION = "policy_compression"
    REORDER = "policy_reorder"


class _TimescaleDBCacheKeys:
    """Keys for metadata cache dictionary"""

    HYPERTABLES = "hypertables"
    CONTINUOUS_AGGREGATES = "continuous_aggregates"
    JOBS = "jobs"


class _TimescaleDBProperties:
    """Custom property keys for DataHub metadata"""

    # Hypertable properties
    IS_HYPERTABLE = "is_hypertable"
    NUM_DIMENSIONS = "num_dimensions"
    NUM_CHUNKS = "num_chunks"
    COMPRESSION_ENABLED = "compression_enabled"
    RETENTION_PERIOD = "retention_period"

    # Continuous aggregate properties
    IS_CONTINUOUS_AGGREGATE = "is_continuous_aggregate"
    MATERIALIZED = "materialized"
    MATERIALIZED_ONLY = "materialized_only"
    SOURCE_HYPERTABLE = "source_hypertable"
    REFRESH_INTERVAL = "refresh_interval"

    # Job properties
    JOB_ID = "job_id"
    APPLICATION_NAME = "application_name"
    SCHEDULE_INTERVAL = "schedule_interval"
    MAX_RUNTIME = "max_runtime"
    MAX_RETRIES = "max_retries"
    RETRY_PERIOD = "retry_period"
    PROC_SCHEMA = "proc_schema"
    PROC_NAME = "proc_name"
    SCHEDULED = "scheduled"
    FIXED_SCHEDULE = "fixed_schedule"
    INITIAL_START = "initial_start"
    CONFIG = "config"
    HYPERTABLE = "hypertable"

    # Container properties
    DATABASE = "database"
    SCHEMA = "schema"
    ORCHESTRATOR = "orchestrator"


class _TimescaleDBDisplay:
    """Display strings for UI"""

    BACKGROUND_JOBS = "TimescaleDB Background Jobs"


class _TimescaleDBValues:
    """Common constant values"""

    TRUE = "true"


class TimescaleDBEnvironment(StrEnum):
    SELF_HOSTED = "self_hosted"
    CLOUD = "cloud"
    UNKNOWN = "unknown"


def safe_get_from_row(row: Any, key: str, default: Any = None) -> Any:
    """Safely get a value from SQLAlchemy Row, handling missing columns"""
    try:
        return row[key]
    except (KeyError, AttributeError):
        return default


def format_timedelta_human_readable(td: timedelta) -> str:
    """Convert timedelta to human-readable format like '1 hour', '30 days', '5 minutes'"""
    total_seconds = int(td.total_seconds())

    if total_seconds == 0:
        return "0 seconds"

    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if seconds > 0 and not parts:
        parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")

    return " ".join(parts)


def safe_str_convert(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return format_timedelta_human_readable(value)
    return str(value)


class TableMetadata(BaseModel):
    """Container for table metadata"""

    description: Optional[str] = None
    custom_properties: Dict[str, str] = Field(default_factory=dict)
    location_urn: Optional[str] = None

    def to_tuple(self) -> TableMetadataTuple:
        """Convert to tuple format expected by parent class"""
        return self.description, self.custom_properties, self.location_urn

    @classmethod
    def from_tuple(
        cls,
        description: Optional[str],
        properties: Dict[str, str],
        location_urn: Optional[str],
    ) -> "TableMetadata":
        """Create from tuple format returned by parent class"""
        return cls(
            description=description,
            custom_properties=properties,
            location_urn=location_urn,
        )


TModel = TypeVar("TModel", bound="TimescaleDBModel")


class TimescaleDBModel(BaseModel):
    """Base class for TimescaleDB data models"""

    @classmethod
    def from_db_row(cls: Type[TModel], row: Any) -> TModel:
        """Factory method that maps database row to Pydantic model"""
        field_values = {}
        for field_name, field_info in cls.model_fields.items():
            raw_value = safe_get_from_row(row, field_name)

            if raw_value is None and field_info.default is not None:
                continue

            field_values[field_name] = raw_value

        return cls(**field_values)


class HypertableDimension(BaseModel):
    column_name: str
    column_type: str
    time_interval: Optional[str] = None
    integer_interval: Optional[int] = None
    num_partitions: Optional[int] = None


class RetentionPolicy(BaseModel):
    drop_after: Optional[str] = None


class RefreshPolicy(BaseModel):
    schedule_interval: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class Hypertable(TimescaleDBModel):
    name: str = Field(alias="hypertable_name")
    num_dimensions: int = 0
    num_chunks: int = 0
    compression_enabled: bool = False
    dimensions: List[HypertableDimension] = Field(default_factory=list)
    retention_policy: Optional[RetentionPolicy] = None

    model_config = {"populate_by_name": True}

    @classmethod
    def from_db_row(cls, row: Any) -> "Hypertable":
        # Parse nested dimensions
        dimensions_data = safe_get_from_row(row, "dimensions")
        dimensions = (
            [HypertableDimension(**dim) for dim in dimensions_data]
            if dimensions_data
            else []
        )

        # Parse nested retention policy
        retention_data = safe_get_from_row(row, "retention_policy")
        retention_policy = RetentionPolicy(**retention_data) if retention_data else None

        return cls(
            name=safe_get_from_row(row, "hypertable_name", ""),
            num_dimensions=safe_get_from_row(row, "num_dimensions", 0),
            num_chunks=safe_get_from_row(row, "num_chunks", 0),
            compression_enabled=safe_get_from_row(row, "compression_enabled", False),
            dimensions=dimensions,
            retention_policy=retention_policy,
        )


class ContinuousAggregate(TimescaleDBModel):
    name: str = Field(alias="view_name")
    materialized_only: bool = False
    compression_enabled: bool = False
    hypertable_schema: Optional[str] = None
    hypertable_name: Optional[str] = None
    view_definition: Optional[str] = None
    refresh_policy: Optional[RefreshPolicy] = None

    model_config = {"populate_by_name": True}

    @classmethod
    def from_db_row(cls, row: Any) -> "ContinuousAggregate":
        # Parse nested refresh policy
        refresh_data = safe_get_from_row(row, "refresh_policy")
        refresh_policy = RefreshPolicy(**refresh_data) if refresh_data else None

        return cls(
            name=safe_get_from_row(row, "view_name", ""),
            materialized_only=safe_get_from_row(row, "materialized_only", False),
            compression_enabled=safe_get_from_row(row, "compression_enabled", False),
            hypertable_schema=safe_get_from_row(row, "hypertable_schema"),
            hypertable_name=safe_get_from_row(row, "hypertable_name"),
            view_definition=safe_get_from_row(row, "view_definition"),
            refresh_policy=refresh_policy,
        )


class JobExecution(TimescaleDBModel):
    job_id: int
    last_run_started_at: Optional[str] = None
    last_successful_finish: Optional[str] = None
    last_run_status: str = "unknown"
    total_runs: int = 0
    total_successes: int = 0
    total_failures: int = 0
    total_crashes: int = 0
    consecutive_failures: int = 0
    consecutive_crashes: int = 0


class TimescaleDBJob(TimescaleDBModel):
    job_id: int
    application_name: Optional[str] = None
    schedule_interval: Optional[str] = None
    max_runtime: Optional[str] = None
    max_retries: int = 0
    retry_period: Optional[str] = None
    proc_schema: Optional[str] = None
    proc_name: Optional[str] = None
    scheduled: bool = False
    fixed_schedule: bool = False
    initial_start: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    hypertable_schema: Optional[str] = None
    hypertable_name: Optional[str] = None

    @classmethod
    def from_db_row(cls, row: Any) -> "TimescaleDBJob":
        # Convert timedelta fields to strings
        field_values = {
            "job_id": safe_get_from_row(row, "job_id", 0),
            "application_name": safe_get_from_row(row, "application_name"),
            "schedule_interval": safe_str_convert(
                safe_get_from_row(row, "schedule_interval")
            ),
            "max_runtime": safe_str_convert(safe_get_from_row(row, "max_runtime")),
            "max_retries": safe_get_from_row(row, "max_retries", 0),
            "retry_period": safe_str_convert(safe_get_from_row(row, "retry_period")),
            "proc_schema": safe_get_from_row(row, "proc_schema"),
            "proc_name": safe_get_from_row(row, "proc_name"),
            "scheduled": safe_get_from_row(row, "scheduled", False),
            "fixed_schedule": safe_get_from_row(row, "fixed_schedule", False),
            "initial_start": safe_str_convert(safe_get_from_row(row, "initial_start")),
            "config": safe_get_from_row(row, "config"),
            "hypertable_schema": safe_get_from_row(row, "hypertable_schema"),
            "hypertable_name": safe_get_from_row(row, "hypertable_name"),
        }
        return cls(**field_values)

    def get_display_name(self) -> str:
        proc_name = self.proc_name or "unknown"

        policy_names = {
            _TimescaleDBPolicies.REFRESH_CONTINUOUS_AGGREGATE: "Refresh Continuous Aggregate",
            _TimescaleDBPolicies.RETENTION: "Data Retention",
            _TimescaleDBPolicies.COMPRESSION: "Compression Policy",
            _TimescaleDBPolicies.REORDER: "Reorder Policy",
        }

        display_name = policy_names.get(proc_name)
        if not display_name:
            display_name = proc_name.replace("_", " ").title()

        if self.hypertable_name:
            return f"{display_name} - {self.hypertable_name}"
        return display_name

    def get_description(self) -> str:
        proc_name = self.proc_name or "unknown"
        description_parts = []

        policy_descriptions = {
            _TimescaleDBPolicies.REFRESH_CONTINUOUS_AGGREGATE: "Refreshes continuous aggregate materialized data",
            _TimescaleDBPolicies.RETENTION: "Manages data retention by dropping old chunks",
            _TimescaleDBPolicies.COMPRESSION: "Compresses hypertable chunks to save storage",
            _TimescaleDBPolicies.REORDER: "Reorders chunks to optimize query performance",
        }

        description = policy_descriptions.get(proc_name)
        if description:
            description_parts.append(description)
        else:
            description_parts.append(f"TimescaleDB background job: {proc_name}")

        if self.hypertable_name:
            description_parts.append(f"for hypertable '{self.hypertable_name}'")

        if self.schedule_interval:
            description_parts.append(f"running every {self.schedule_interval}")

        return " ".join(description_parts) + "."

    def get_custom_properties(self) -> Dict[str, str]:
        custom_properties = {
            _TimescaleDBProperties.JOB_ID: str(self.job_id),
            _TimescaleDBProperties.APPLICATION_NAME: self.application_name or "",
            _TimescaleDBProperties.SCHEDULE_INTERVAL: str(self.schedule_interval or ""),
            _TimescaleDBProperties.MAX_RUNTIME: str(self.max_runtime or ""),
            _TimescaleDBProperties.MAX_RETRIES: str(self.max_retries),
            _TimescaleDBProperties.RETRY_PERIOD: str(self.retry_period or ""),
            _TimescaleDBProperties.PROC_SCHEMA: self.proc_schema or "",
            _TimescaleDBProperties.PROC_NAME: self.proc_name or "",
            _TimescaleDBProperties.SCHEDULED: str(self.scheduled),
            _TimescaleDBProperties.FIXED_SCHEDULE: str(self.fixed_schedule),
            _TimescaleDBProperties.INITIAL_START: str(self.initial_start or ""),
        }

        if self.config:
            if isinstance(self.config, str):
                custom_properties[_TimescaleDBProperties.CONFIG] = self.config
            elif isinstance(self.config, dict):
                custom_properties[_TimescaleDBProperties.CONFIG] = json.dumps(
                    self.config
                )

        if self.hypertable_schema and self.hypertable_name:
            custom_properties[_TimescaleDBProperties.HYPERTABLE] = (
                f"{self.hypertable_schema}.{self.hypertable_name}"
            )

        return custom_properties


class TimescaleDBConfig(PostgresConfig):
    emit_timescaledb_metadata: bool = Field(
        default=True,
        description="Include TimescaleDB-specific metadata (hypertables, continuous aggregates, dimensions, compression, retention policies) as custom properties on datasets.",
    )

    tag_hypertables: bool = Field(
        default=True,
        description="Add 'hypertable' tag to hypertable datasets for easy identification.",
    )

    tag_continuous_aggregates: bool = Field(
        default=True,
        description="Add 'continuous_aggregate' tag to continuous aggregate views.",
    )

    include_background_jobs: bool = Field(
        default=False,
        description="Include TimescaleDB background jobs (continuous aggregate refresh policies, compression policies, retention policies, reorder policies) as DataJob entities. "
        "When disabled, only user-defined stored procedures are included. Enable this to track automated data maintenance processes.",
    )

    job_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter TimescaleDB background jobs by display name (e.g., 'Refresh Continuous Aggregate.*'). Only applies when include_background_jobs is enabled.",
    )


@platform_name("TimescaleDB", id="timescaledb")
@config_class(TimescaleDBConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled for continuous aggregates via column-level lineage",
)
class TimescaleDBSource(PostgresSource):
    """
    TimescaleDB source that extends PostgreSQL source with:
    - Continuous aggregates with column-level lineage (handled as views)
    - Hypertable metadata and dimensions
    - Compression policies and settings
    - Data retention policies
    - Background jobs as DataJob entities
    - Chunk information

    This connector leverages the parent PostgreSQL source's infrastructure:
    - Tables and views are discovered through standard PostgreSQL introspection
    - Continuous aggregates appear as views and are processed by the parent's view logic
    """

    config: TimescaleDBConfig
    _timescaledb_metadata_cache: Dict[str, Dict[str, Any]]
    _timescaledb_environment: Optional[TimescaleDBEnvironment]
    _timescaledb_enabled: Optional[bool]

    def __init__(self, config: TimescaleDBConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self._timescaledb_metadata_cache = {}
        self._timescaledb_environment = None
        self._timescaledb_enabled = None

    def get_platform(self):
        return _TimescaleDBConstants.PLATFORM

    @classmethod
    def create(cls, config_dict, ctx):
        config = TimescaleDBConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def add_information_for_schema(self, inspector: Inspector, schema: str) -> None:
        super().add_information_for_schema(inspector, schema)

        if not self._is_timescaledb_enabled(inspector):
            return

        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"

        if cache_key not in self._timescaledb_metadata_cache:
            continuous_aggregates = self._get_continuous_aggregates(inspector, schema)

            self._timescaledb_metadata_cache[cache_key] = {
                _TimescaleDBCacheKeys.HYPERTABLES: self._get_hypertables(
                    inspector, schema
                ),
                _TimescaleDBCacheKeys.CONTINUOUS_AGGREGATES: continuous_aggregates,
                _TimescaleDBCacheKeys.JOBS: self._get_jobs(inspector, schema)
                if self.config.include_background_jobs
                else {},
            }

    def _get_view_definition(self, inspector: Inspector, schema: str, view: str) -> str:
        """Returns the user-defined SQL query for continuous aggregates, enabling accurate column-level lineage"""
        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})
        continuous_aggregates = metadata.get(
            _TimescaleDBCacheKeys.CONTINUOUS_AGGREGATES, {}
        )

        if view in continuous_aggregates:
            cagg = continuous_aggregates[view]
            if cagg.view_definition and cagg.view_definition.strip():
                logger.debug(
                    f"Using TimescaleDB continuous aggregate definition for {schema}.{view}"
                )
                return cagg.view_definition
            else:
                logger.warning(
                    f"Continuous aggregate {schema}.{view} has no view definition in TimescaleDB metadata. "
                    f"Falling back to PostgreSQL view definition."
                )

        return super()._get_view_definition(inspector, schema, view)

    def get_procedures_for_schema(
        self, inspector: Inspector, schema: str, db_name: str
    ) -> List[BaseProcedure]:
        """Filters out TimescaleDB policy procedures, which are surfaced as DataJob entities for better job tracking"""
        all_procedures = super().get_procedures_for_schema(inspector, schema, db_name)

        timescaledb_job_procedures = set()
        if self._is_timescaledb_enabled(inspector):
            cache_key = f"{db_name}.{schema}"
            metadata = self._timescaledb_metadata_cache.get(cache_key, {})

            if (
                not metadata.get(_TimescaleDBCacheKeys.JOBS)
                and not self.config.include_background_jobs
            ):
                temp_jobs = self._get_jobs(inspector, schema)
                for job in temp_jobs.values():
                    if job.proc_name:
                        timescaledb_job_procedures.add(job.proc_name)
            else:
                jobs = metadata.get("jobs", {})
                for job in jobs.values():
                    if job.proc_name:
                        timescaledb_job_procedures.add(job.proc_name)

        filtered_procedures = []
        for procedure in all_procedures:
            if procedure.name in timescaledb_job_procedures:
                logger.debug(
                    f"Skipping TimescaleDB background job procedure: {procedure.name} (handled as background job instead)"
                )
            else:
                filtered_procedures.append(procedure)

        return filtered_procedures

    def get_schema_level_workunits(
        self,
        inspector: Inspector,
        schema: str,
        database: str,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from super().get_schema_level_workunits(
            inspector=inspector,
            schema=schema,
            database=database,
        )

        if self.config.include_background_jobs and self._is_timescaledb_enabled(
            inspector
        ):
            yield from self._process_timescaledb_jobs(inspector, schema, database)

    def _is_timescaledb_enabled(self, inspector: Inspector) -> bool:
        if self._timescaledb_enabled is not None:
            return self._timescaledb_enabled

        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
                )
                self._timescaledb_enabled = result.rowcount > 0
                return self._timescaledb_enabled
        except DatabaseError as e:
            logger.warning(
                f"Could not check for TimescaleDB extension: {e}. "
                "If TimescaleDB is installed, ensure the user has permissions to query pg_extension."
            )
            self._timescaledb_enabled = False
            return False

    def _detect_timescaledb_environment(
        self, inspector: Inspector
    ) -> TimescaleDBEnvironment:
        if self._timescaledb_environment is not None:
            return self._timescaledb_environment

        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT 1 FROM information_schema.schemata "
                        "WHERE schema_name = 'timescaledb_information'"
                    )
                )
                has_info_schema = result.rowcount > 0

                if has_info_schema:
                    self._timescaledb_environment = TimescaleDBEnvironment.SELF_HOSTED
                    logger.info(
                        "Detected TimescaleDB environment: using timescaledb_information schema"
                    )
                else:
                    logger.warning(
                        "Could not find timescaledb_information schema. "
                        "Ensure you have sufficient permissions or are using a supported TimescaleDB version."
                    )
                    self._timescaledb_environment = TimescaleDBEnvironment.UNKNOWN

                return self._timescaledb_environment

        except DatabaseError as e:
            logger.warning(
                f"Could not detect TimescaleDB environment: {e}. "
                "Metadata extraction may fail if permissions are insufficient."
            )
            self._timescaledb_environment = TimescaleDBEnvironment.UNKNOWN
            return TimescaleDBEnvironment.UNKNOWN

    def _execute_timescaledb_query(
        self,
        inspector: Inspector,
        query: str,
        params: Dict[str, Any],
        operation_name: str,
    ) -> List[Any]:
        """Executes query with graceful error handling - returns empty list on failure to allow ingestion to continue"""
        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(text(query), params)
                return list(result)
        except DatabaseError as e:
            error_msg = str(e).lower()

            if "permission denied" in error_msg or "access denied" in error_msg:
                self.report.warning(
                    title=f"Permission Denied for {operation_name}",
                    message=f"User lacks permissions to query TimescaleDB metadata for {operation_name}. "
                    f"Grant SELECT on timescaledb_information schema or disable TimescaleDB metadata extraction. "
                    f"Schema: {params.get('schema', 'unknown')}",
                    exc=e,
                )
            elif "does not exist" in error_msg:
                logger.warning(
                    f"TimescaleDB metadata query failed for {operation_name} - schema or table may not exist: {e}. "
                    f"This could indicate missing permissions or schema changes."
                )
            else:
                self.report.warning(
                    title=f"Failed to execute {operation_name}",
                    message=f"Could not fetch {operation_name} for schema {params.get('schema', 'unknown')}. "
                    "The connector will continue but TimescaleDB-specific metadata may be incomplete.",
                    exc=e,
                )

            return []

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> TableMetadataTuple:
        """Returns table metadata including TimescaleDB-specific properties"""
        parent_result = super().get_table_properties(inspector, schema, table)
        table_props = TableMetadata.from_tuple(*parent_result)

        if not self.config.emit_timescaledb_metadata:
            return table_props.to_tuple()

        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})

        hypertables = metadata.get(_TimescaleDBCacheKeys.HYPERTABLES, {})
        if table in hypertables:
            self._enrich_hypertable_properties(table_props, hypertables[table])

        continuous_aggregates = metadata.get(
            _TimescaleDBCacheKeys.CONTINUOUS_AGGREGATES, {}
        )
        if table in continuous_aggregates:
            self._enrich_continuous_aggregate_properties(
                table_props, continuous_aggregates[table]
            )

        return table_props.to_tuple()

    def _enrich_hypertable_properties(
        self, table_props: TableMetadata, hypertable: Hypertable
    ) -> None:
        """Add hypertable-specific metadata to table properties"""
        props = table_props.custom_properties
        props[_TimescaleDBProperties.IS_HYPERTABLE] = _TimescaleDBValues.TRUE
        props[_TimescaleDBProperties.NUM_DIMENSIONS] = str(hypertable.num_dimensions)
        props[_TimescaleDBProperties.NUM_CHUNKS] = str(hypertable.num_chunks)
        props[_TimescaleDBProperties.COMPRESSION_ENABLED] = str(
            hypertable.compression_enabled
        )

        for i, dim in enumerate(hypertable.dimensions):
            prefix = f"dimension_{i}"
            props[f"{prefix}_column"] = dim.column_name
            props[f"{prefix}_type"] = dim.column_type
            if dim.time_interval:
                props[f"{prefix}_interval"] = dim.time_interval

        if hypertable.retention_policy and hypertable.retention_policy.drop_after:
            props[_TimescaleDBProperties.RETENTION_PERIOD] = (
                hypertable.retention_policy.drop_after
            )

    def _enrich_continuous_aggregate_properties(
        self, table_props: TableMetadata, cagg: ContinuousAggregate
    ) -> None:
        """Add continuous aggregate-specific metadata to table properties"""
        props = table_props.custom_properties
        props[_TimescaleDBProperties.IS_CONTINUOUS_AGGREGATE] = _TimescaleDBValues.TRUE
        props[_TimescaleDBProperties.MATERIALIZED_ONLY] = str(cagg.materialized_only)
        props[_TimescaleDBProperties.COMPRESSION_ENABLED] = str(
            cagg.compression_enabled
        )

        if cagg.hypertable_schema and cagg.hypertable_name:
            props[_TimescaleDBProperties.SOURCE_HYPERTABLE] = (
                f"{cagg.hypertable_schema}.{cagg.hypertable_name}"
            )

        if cagg.refresh_policy:
            if cagg.refresh_policy.schedule_interval:
                props[_TimescaleDBProperties.REFRESH_INTERVAL] = str(
                    cagg.refresh_policy.schedule_interval
                )

            if cagg.refresh_policy.config:
                config = cagg.refresh_policy.config
                if isinstance(config, str):
                    try:
                        config = json.loads(config)
                    except json.JSONDecodeError:
                        config = {}
                if isinstance(config, dict):
                    if "start_offset" in config:
                        props["refresh_start_offset"] = str(config["start_offset"])
                    if "end_offset" in config:
                        props["refresh_end_offset"] = str(config["end_offset"])

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        yield from super()._process_table(
            dataset_name, inspector, schema, table, sql_config, data_reader
        )

        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})

        hypertables = metadata.get(_TimescaleDBCacheKeys.HYPERTABLES, {})
        if table in hypertables:
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.get_platform(),
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )

            subtype_workunit = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[
                        DatasetSubTypes.TIMESCALEDB_HYPERTABLE,
                        DatasetSubTypes.TABLE,
                    ]
                ),
            ).as_workunit()
            yield subtype_workunit

            if self.config.tag_hypertables:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(
                        tags=[
                            TagAssociationClass(
                                tag=make_tag_urn(_TimescaleDBTags.HYPERTABLE)
                            )
                        ]
                    ),
                ).as_workunit()

    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        yield from super()._process_view(
            dataset_name, inspector, schema, view, sql_config
        )

        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})

        continuous_aggregates = metadata.get(
            _TimescaleDBCacheKeys.CONTINUOUS_AGGREGATES, {}
        )
        if view in continuous_aggregates:
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.get_platform(),
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[
                        DatasetSubTypes.TIMESCALEDB_CONTINUOUS_AGGREGATE,
                        DatasetSubTypes.VIEW,
                    ]
                ),
            ).as_workunit()

            display_name = view
            _, existing_properties, _ = self.get_table_properties(
                inspector, schema, view
            )

            all_properties = {
                **existing_properties,
                _TimescaleDBProperties.MATERIALIZED: _TimescaleDBValues.TRUE,
                _TimescaleDBTags.CONTINUOUS_AGGREGATE: _TimescaleDBValues.TRUE,
            }

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    name=display_name,
                    customProperties=all_properties,
                ),
            ).as_workunit()

            if self.config.tag_continuous_aggregates:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(
                        tags=[
                            TagAssociationClass(
                                tag=make_tag_urn(_TimescaleDBTags.CONTINUOUS_AGGREGATE)
                            )
                        ]
                    ),
                ).as_workunit()

    def _process_timescaledb_jobs(
        self, inspector: Inspector, schema: str, database: str
    ) -> Iterable[MetadataWorkUnit]:
        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})
        jobs = metadata.get("jobs", {})

        if not jobs:
            return

        flow_urn = self._create_jobs_container(database, schema)
        yield from self._emit_jobs_container(flow_urn, database, schema)

        for job_id, job in jobs.items():
            job_name = job.get_display_name()

            job_identifier = f"{job_id}_{job.proc_name or 'unknown'}"
            if job.hypertable_name:
                job_identifier = (
                    f"{job_id}_{job.hypertable_name}_{job.proc_name or 'unknown'}"
                )

            job_urn = make_data_job_urn(
                orchestrator=_TimescaleDBConstants.PLATFORM,
                flow_id=f"{database}.{schema}.{_TimescaleDBConstants.BACKGROUND_JOBS_FLOW_SUFFIX}",
                job_id=job_identifier,
                cluster=self.config.env,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInfoClass(
                    name=job_name,
                    type=AzkabanJobTypeClass.COMMAND,
                    description=job.get_description(),
                    customProperties=job.get_custom_properties(),
                ),
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn, aspect=StatusClass(removed=False)
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=SubTypesClass(
                    typeNames=[JobContainerSubTypes.TIMESCALEDB_BACKGROUND_JOB]
                ),
            ).as_workunit()

            database_container_key = gen_database_key(
                database=database,
                platform=self.get_platform(),
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            schema_container_key = gen_schema_key(
                db_name=database,
                schema=schema,
                platform=self.get_platform(),
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=ContainerClass(container=schema_container_key.as_urn()),
            ).as_workunit()

            yield from self._emit_job_run_instances(inspector, job_urn, job_id, job)

            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(
                            id=database_container_key.as_urn(),
                            urn=database_container_key.as_urn(),
                        ),
                        BrowsePathEntryClass(
                            id=schema_container_key.as_urn(),
                            urn=schema_container_key.as_urn(),
                        ),
                    ]
                ),
            ).as_workunit()

            inputs = []
            outputs = []

            if job.hypertable_schema and job.hypertable_name:
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.get_platform(),
                    self.get_identifier(
                        schema=job.hypertable_schema,
                        entity=job.hypertable_name,
                        inspector=inspector,
                    ),
                    self.config.platform_instance,
                    self.config.env,
                )

                proc_name = job.proc_name or ""
                if proc_name == _TimescaleDBPolicies.REFRESH_CONTINUOUS_AGGREGATE:
                    outputs.append(dataset_urn)
                elif proc_name in (
                    _TimescaleDBPolicies.RETENTION,
                    _TimescaleDBPolicies.COMPRESSION,
                    _TimescaleDBPolicies.REORDER,
                ):
                    inputs.append(dataset_urn)
                    outputs.append(dataset_urn)
                else:
                    inputs.append(dataset_urn)
                    outputs.append(dataset_urn)

            if inputs or outputs:
                yield MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=DataJobInputOutputClass(
                        inputDatasets=inputs,
                        outputDatasets=outputs,
                    ),
                ).as_workunit()

    def _get_hypertables(
        self, inspector: Inspector, schema: str
    ) -> Dict[str, Hypertable]:
        hypertables: Dict[str, Hypertable] = {}

        # Ensure TimescaleDB environment is detected
        env = self._detect_timescaledb_environment(inspector)
        if env == TimescaleDBEnvironment.UNKNOWN:
            logger.warning(
                f"Skipping hypertable extraction for schema {schema} - TimescaleDB environment could not be detected. "
                f"Ensure TimescaleDB is properly installed and accessible."
            )
            return hypertables

        query = _TimescaleDBQueries.HYPERTABLES

        rows = self._execute_timescaledb_query(
            inspector,
            query,
            {"schema": schema, "retention_policy_name": _TimescaleDBPolicies.RETENTION},
            "hypertable metadata",
        )

        for row in rows:
            try:
                hypertable = Hypertable.from_db_row(row)
                hypertables[hypertable.name] = hypertable
            except (ValidationError, KeyError, TypeError, ValueError) as e:
                logger.warning(
                    f"Failed to parse hypertable metadata for row {row}: {e}. Skipping this hypertable."
                )

        return hypertables

    def _get_continuous_aggregates(
        self, inspector: Inspector, schema: str
    ) -> Dict[str, ContinuousAggregate]:
        continuous_aggregates: Dict[str, ContinuousAggregate] = {}

        env = self._detect_timescaledb_environment(inspector)
        if env == TimescaleDBEnvironment.UNKNOWN:
            logger.warning(
                f"Skipping continuous aggregate extraction for schema {schema} - TimescaleDB environment could not be detected. "
                f"Ensure TimescaleDB is properly installed and accessible."
            )
            return continuous_aggregates

        query = _TimescaleDBQueries.CONTINUOUS_AGGREGATES

        rows = self._execute_timescaledb_query(
            inspector,
            query,
            {
                "schema": schema,
                "refresh_policy_name": _TimescaleDBPolicies.REFRESH_CONTINUOUS_AGGREGATE,
            },
            "continuous aggregate metadata",
        )

        for row in rows:
            try:
                cagg = ContinuousAggregate.from_db_row(row)
                continuous_aggregates[cagg.name] = cagg
            except (ValidationError, KeyError, TypeError, ValueError) as e:
                logger.warning(
                    f"Failed to parse continuous aggregate metadata for row {row}: {e}. Skipping this aggregate."
                )

        return continuous_aggregates

    def _get_jobs(self, inspector: Inspector, schema: str) -> Dict[int, TimescaleDBJob]:
        jobs: Dict[int, TimescaleDBJob] = {}

        env = self._detect_timescaledb_environment(inspector)
        if env == TimescaleDBEnvironment.UNKNOWN:
            logger.warning(
                f"Skipping job extraction for schema {schema} - TimescaleDB environment could not be detected. "
                f"Ensure TimescaleDB is properly installed and accessible."
            )
            return jobs

        query = _TimescaleDBQueries.JOBS

        rows = self._execute_timescaledb_query(
            inspector, query, {"schema": schema}, "job metadata"
        )

        for row in rows:
            try:
                job_id = row["job_id"]
                job = TimescaleDBJob.from_db_row(row)

                job_display_name = job.get_display_name()
                if not self.config.job_pattern.allowed(job_display_name):
                    self.report.report_dropped(f"Job: {job_display_name}")
                    continue

                jobs[job_id] = job
            except (ValidationError, KeyError, TypeError, ValueError) as e:
                logger.warning(
                    f"Failed to parse job metadata for row {row}: {e}. Skipping this job."
                )

        return jobs

    def _get_job_execution_history(
        self, inspector: Inspector, job_id: int, limit: int = 10
    ) -> List[JobExecution]:
        executions = []

        query = _TimescaleDBQueries.JOB_EXECUTION_HISTORY

        rows = self._execute_timescaledb_query(
            inspector,
            query,
            {"job_id": job_id, "limit": limit},
            "job execution history",
        )

        for row in rows:
            try:
                executions.append(JobExecution.from_db_row(row))
            except (ValidationError, KeyError, TypeError, ValueError) as e:
                logger.warning(
                    f"Failed to parse job execution history for job {job_id}: {e}. "
                    f"Row data: {row}. Skipping this execution record."
                )

        return executions

    def _emit_job_run_instances(
        self, inspector: Inspector, job_urn: str, job_id: int, job: TimescaleDBJob
    ) -> Iterable[MetadataWorkUnit]:
        executions = self._get_job_execution_history(inspector, job_id)

        if not executions:
            return

        for execution in executions:
            run_id = f"{job_id}_run_{execution.last_run_started_at or 'unknown'}"
            run_instance_urn = mce_builder.make_data_process_instance_urn(run_id)

            last_run_status = execution.last_run_status.lower()
            if last_run_status in ["success", "successful"]:
                run_status = DataProcessRunStatusClass.COMPLETE
                result = DataProcessInstanceRunResultClass(
                    type=RunResultTypeClass.SUCCESS, nativeResultType="TimescaleDB"
                )
            elif last_run_status in ["failed", "failure", "error"]:
                run_status = DataProcessRunStatusClass.COMPLETE
                result = DataProcessInstanceRunResultClass(
                    type=RunResultTypeClass.FAILURE, nativeResultType="TimescaleDB"
                )
            else:
                run_status = DataProcessRunStatusClass.STARTED
                result = DataProcessInstanceRunResultClass(
                    type=RunResultTypeClass.UP_FOR_RETRY, nativeResultType="TimescaleDB"
                )

            properties = {
                "job_id": str(job_id),
                "job_name": job.get_display_name(),
                "total_runs": str(execution.total_runs),
                "total_successes": str(execution.total_successes),
                "total_failures": str(execution.total_failures),
                "consecutive_failures": str(execution.consecutive_failures),
            }

            if job.hypertable_name:
                properties["hypertable"] = (
                    f"{job.hypertable_schema}.{job.hypertable_name}"
                )

            created_timestamp = int(time.time() * 1000)
            if execution.last_run_started_at:
                try:
                    started_at = execution.last_run_started_at
                    if hasattr(started_at, "timestamp"):
                        created_timestamp = int(started_at.timestamp() * 1000)
                except (AttributeError, TypeError, ValueError, OverflowError):
                    pass  # Use current time as fallback

            yield MetadataChangeProposalWrapper(
                entityUrn=run_instance_urn,
                aspect=DataProcessInstancePropertiesClass(
                    name=f"{job.get_display_name()} - Run",
                    created=AuditStampClass(
                        time=created_timestamp,
                        actor="urn:li:corpuser:datahub",
                    ),
                    customProperties=properties,
                ),
            ).as_workunit()

            last_run_time = execution.last_run_started_at
            if last_run_time:
                timestamp_millis = (
                    created_timestamp  # Use the same timestamp as the instance creation
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=run_instance_urn,
                    aspect=DataProcessInstanceRunEventClass(
                        timestampMillis=timestamp_millis,
                        status=run_status,
                        result=result,
                        attempt=1,  # TimescaleDB doesn't track individual attempts
                    ),
                ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=run_instance_urn,
                aspect=DataProcessInstanceRelationshipsClass(
                    parentTemplate=job_urn,
                    upstreamInstances=[],
                    parentInstance=None,
                ),
            ).as_workunit()

    def _create_jobs_container(self, database: str, schema: str) -> str:
        return make_data_flow_urn(
            orchestrator=_TimescaleDBConstants.PLATFORM,
            flow_id=f"{database}.{schema}.{_TimescaleDBConstants.BACKGROUND_JOBS_FLOW_SUFFIX}",
            cluster=self.config.env,
        )

    def _emit_jobs_container(
        self, flow_urn: str, database: str, schema: str
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=f"{_TimescaleDBDisplay.BACKGROUND_JOBS} ({schema})",
                customProperties={
                    _TimescaleDBProperties.DATABASE: database,
                    _TimescaleDBProperties.SCHEMA: schema,
                    _TimescaleDBProperties.ORCHESTRATOR: self.get_platform(),
                },
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=SubTypesClass(
                typeNames=[FlowContainerSubTypes.TIMESCALEDB_BACKGROUND_JOBS]
            ),
        ).as_workunit()

        database_container_key = gen_database_key(
            database=database,
            platform=self.get_platform(),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        schema_container_key = gen_schema_key(
            db_name=database,
            schema=schema,
            platform=self.get_platform(),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(
                        id=database_container_key.as_urn(),
                        urn=database_container_key.as_urn(),
                    ),
                    BrowsePathEntryClass(
                        id=schema_container_key.as_urn(),
                        urn=schema_container_key.as_urn(),
                    ),
                ]
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=ContainerClass(container=schema_container_key.as_urn()),
        ).as_workunit()
