import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern
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
from datahub.metadata.schema_classes import (
    AzkabanJobTypeClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
)

logger: logging.Logger = logging.getLogger(__name__)


def safe_get_from_row(row: Any, key: str, default: Any = None) -> Any:
    """Safely get a value from SQLAlchemy Row, handling missing columns"""
    try:
        return row[key]
    except (KeyError, AttributeError):
        return default


class HypertableDimension(BaseModel):
    """Represents a TimescaleDB hypertable dimension"""

    column_name: str
    column_type: str
    time_interval: Optional[str] = None
    integer_interval: Optional[int] = None
    num_partitions: Optional[int] = None


class RetentionPolicy(BaseModel):
    """Represents a TimescaleDB retention policy"""

    drop_after: Optional[str] = None


class RefreshPolicy(BaseModel):
    """Represents a continuous aggregate refresh policy"""

    schedule_interval: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class Hypertable(BaseModel):
    """Represents a TimescaleDB hypertable with metadata"""

    name: str
    num_dimensions: int = 0
    num_chunks: int = 0
    compression_enabled: bool = False
    dimensions: List[HypertableDimension] = Field(default_factory=list)
    retention_policy: Optional[RetentionPolicy] = None

    @classmethod
    def from_db_row(cls, row: Any) -> "Hypertable":
        """Create a Hypertable from database row"""
        dimensions = []
        dimensions_data = safe_get_from_row(row, "dimensions")
        if dimensions_data:
            for dim_data in dimensions_data:
                dimensions.append(HypertableDimension(**dim_data))

        retention_policy = None
        retention_data = safe_get_from_row(row, "retention_policy")
        if retention_data:
            retention_policy = RetentionPolicy(**retention_data)

        return cls(
            name=safe_get_from_row(row, "hypertable_name", ""),
            num_dimensions=safe_get_from_row(row, "num_dimensions", 0),
            num_chunks=safe_get_from_row(row, "num_chunks", 0),
            compression_enabled=safe_get_from_row(row, "compression_enabled", False),
            dimensions=dimensions,
            retention_policy=retention_policy,
        )


class ContinuousAggregate(BaseModel):
    """Represents a TimescaleDB continuous aggregate with metadata"""

    name: str
    materialized_only: bool = False
    compression_enabled: bool = False
    hypertable_schema: Optional[str] = None
    hypertable_name: Optional[str] = None
    view_definition: Optional[str] = None
    refresh_policy: Optional[RefreshPolicy] = None

    @classmethod
    def from_db_row(cls, row: Any) -> "ContinuousAggregate":
        """Create a ContinuousAggregate from database row"""
        refresh_policy = None
        refresh_data = safe_get_from_row(row, "refresh_policy")
        if refresh_data:
            refresh_policy = RefreshPolicy(**refresh_data)

        return cls(
            name=safe_get_from_row(row, "view_name", ""),
            materialized_only=safe_get_from_row(row, "materialized_only", False),
            compression_enabled=safe_get_from_row(row, "compression_enabled", False),
            hypertable_schema=safe_get_from_row(row, "hypertable_schema", ""),
            hypertable_name=safe_get_from_row(row, "hypertable_name", ""),
            view_definition=safe_get_from_row(row, "view_definition", ""),
            refresh_policy=refresh_policy,
        )


class TimescaleDBJob(BaseModel):
    """Represents a TimescaleDB background job"""

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
        """Create a TimescaleDBJob from database row"""

        def safe_str_convert(value: Any) -> Optional[str]:
            """Safely convert value to string, handling timedelta and None"""
            if value is None:
                return None
            if hasattr(value, "total_seconds"):  # timedelta object
                return str(value)
            return str(value)

        return cls(
            job_id=safe_get_from_row(row, "job_id", 0),
            application_name=safe_get_from_row(row, "application_name"),
            schedule_interval=safe_str_convert(
                safe_get_from_row(row, "schedule_interval")
            ),
            max_runtime=safe_str_convert(safe_get_from_row(row, "max_runtime")),
            max_retries=safe_get_from_row(row, "max_retries", 0),
            retry_period=safe_str_convert(safe_get_from_row(row, "retry_period")),
            proc_schema=safe_get_from_row(row, "proc_schema"),
            proc_name=safe_get_from_row(row, "proc_name"),
            scheduled=safe_get_from_row(row, "scheduled", False),
            fixed_schedule=safe_get_from_row(row, "fixed_schedule", False),
            initial_start=safe_str_convert(safe_get_from_row(row, "initial_start")),
            config=safe_get_from_row(row, "config"),
            hypertable_schema=safe_get_from_row(row, "hypertable_schema"),
            hypertable_name=safe_get_from_row(row, "hypertable_name"),
        )

    def get_display_name(self) -> str:
        """Generate a human-readable display name for the job"""
        proc_name = self.proc_name or "unknown"

        # Create meaningful names based on job type
        if "refresh" in proc_name and self.hypertable_name:
            return f"Refresh Continuous Aggregate - {self.hypertable_name}"
        elif "retention" in proc_name and self.hypertable_name:
            return f"Data Retention - {self.hypertable_name}"
        elif "compression" in proc_name and self.hypertable_name:
            return f"Compression Policy - {self.hypertable_name}"
        elif self.hypertable_name:
            return f"{proc_name.replace('_', ' ').title()} - {self.hypertable_name}"
        else:
            return f"{proc_name.replace('_', ' ').title()}"

    def get_description(self) -> str:
        """Generate a description for the job"""
        proc_name = self.proc_name or "unknown"
        description_parts = []

        # Add job type description
        if "refresh" in proc_name:
            description_parts.append("Refreshes continuous aggregate materialized data")
        elif "retention" in proc_name:
            description_parts.append("Manages data retention by dropping old chunks")
        elif "compression" in proc_name:
            description_parts.append("Compresses hypertable chunks to save storage")
        else:
            description_parts.append(f"TimescaleDB background job: {proc_name}")

        # Add target information
        if self.hypertable_name:
            description_parts.append(f"for hypertable '{self.hypertable_name}'")

        # Add scheduling information
        if self.schedule_interval:
            description_parts.append(f"running every {self.schedule_interval}")

        return " ".join(description_parts) + "."

    def get_custom_properties(self) -> Dict[str, str]:
        """Get custom properties for DataHub metadata"""
        custom_properties = {
            "job_id": str(self.job_id),
            "application_name": self.application_name or "",
            "schedule_interval": str(self.schedule_interval or ""),
            "max_runtime": str(self.max_runtime or ""),
            "max_retries": str(self.max_retries),
            "retry_period": str(self.retry_period or ""),
            "proc_schema": self.proc_schema or "",
            "proc_name": self.proc_name or "",
            "scheduled": str(self.scheduled),
            "fixed_schedule": str(self.fixed_schedule),
            "initial_start": str(self.initial_start or ""),
        }

        # Add config if present
        if self.config:
            if isinstance(self.config, str):
                custom_properties["config"] = self.config
            elif isinstance(self.config, dict):
                custom_properties["config"] = json.dumps(self.config)

        # Add hypertable info if present
        if self.hypertable_schema and self.hypertable_name:
            custom_properties["hypertable"] = (
                f"{self.hypertable_schema}.{self.hypertable_name}"
            )

        return custom_properties


class TimescaleDBConfig(PostgresConfig):
    """Configuration for TimescaleDB connector"""

    emit_timescaledb_metadata: bool = Field(
        default=True,
        description="Emit TimescaleDB-specific metadata as custom properties",
    )

    tag_hypertables: bool = Field(
        default=True, description="Add 'hypertable' tag to hypertables"
    )

    tag_continuous_aggregates: bool = Field(
        default=True,
        description="Add 'continuous_aggregate' tag to continuous aggregates",
    )

    include_jobs: bool = Field(
        default=False,
        description="Include TimescaleDB background jobs as DataJob entities",
    )

    job_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for TimescaleDB jobs to filter in ingestion",
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
    _timescaledb_metadata_cache: Dict[
        str, Dict[str, Any]
    ]  # Will store models instead of dicts

    def __init__(self, config: TimescaleDBConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self._timescaledb_metadata_cache = {}

    def get_platform(self):
        return "timescaledb"

    @classmethod
    def create(cls, config_dict, ctx):
        config = TimescaleDBConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def add_information_for_schema(self, inspector: Inspector, schema: str) -> None:
        """
        Called before processing each schema. Cache TimescaleDB metadata.
        This is called by the parent class before processing tables/views.
        """
        super().add_information_for_schema(inspector, schema)

        if not self._is_timescaledb_enabled(inspector):
            return

        # Cache all TimescaleDB metadata for this schema
        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"

        if cache_key not in self._timescaledb_metadata_cache:
            self._timescaledb_metadata_cache[cache_key] = {
                "hypertables": self._get_hypertables(inspector, schema),
                "continuous_aggregates": self._get_continuous_aggregates(
                    inspector, schema
                ),
                "jobs": self._get_jobs(inspector, schema)
                if self.config.include_jobs
                else {},
            }

    def get_schema_level_workunits(
        self,
        inspector: Inspector,
        schema: str,
        database: str,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        """Override to add TimescaleDB jobs after standard processing"""

        # First yield all standard PostgreSQL workunits
        yield from super().get_schema_level_workunits(
            inspector=inspector,
            schema=schema,
            database=database,
        )

        # Then add TimescaleDB jobs if configured
        if self.config.include_jobs and self._is_timescaledb_enabled(inspector):
            yield from self._process_timescaledb_jobs(inspector, schema, database)

    def _is_timescaledb_enabled(self, inspector: Inspector) -> bool:
        """Check if TimescaleDB extension is installed"""
        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
                )
                return result.rowcount > 0
        except Exception as e:
            logger.debug(f"Could not check for TimescaleDB extension: {e}")
            return False

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        """
        Override to add TimescaleDB-specific properties to tables.
        Called for both tables and views by the parent class.
        """
        description, properties, location_urn = super().get_table_properties(
            inspector, schema, table
        )

        if not self.config.emit_timescaledb_metadata:
            return description, properties, location_urn

        # Get cached metadata
        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})

        # Check if this is a hypertable
        hypertables = metadata.get("hypertables", {})
        if table in hypertables:
            hypertable: Hypertable = hypertables[table]
            properties["is_hypertable"] = "true"
            properties["num_dimensions"] = str(hypertable.num_dimensions)
            properties["num_chunks"] = str(hypertable.num_chunks)
            properties["compression_enabled"] = str(hypertable.compression_enabled)

            # Add dimension information
            for i, dim in enumerate(hypertable.dimensions):
                prefix = f"dimension_{i}"
                properties[f"{prefix}_column"] = dim.column_name
                properties[f"{prefix}_type"] = dim.column_type
                if dim.time_interval:
                    properties[f"{prefix}_interval"] = dim.time_interval

            # Add retention policy if exists
            if hypertable.retention_policy and hypertable.retention_policy.drop_after:
                properties["retention_period"] = hypertable.retention_policy.drop_after

        # Check if this is a continuous aggregate (view)
        continuous_aggregates = metadata.get("continuous_aggregates", {})
        if table in continuous_aggregates:
            cagg: ContinuousAggregate = continuous_aggregates[table]
            properties["is_continuous_aggregate"] = "true"
            properties["materialized_only"] = str(cagg.materialized_only)
            properties["compression_enabled"] = str(cagg.compression_enabled)

            if cagg.hypertable_schema and cagg.hypertable_name:
                properties["source_hypertable"] = (
                    f"{cagg.hypertable_schema}.{cagg.hypertable_name}"
                )

            # Add refresh policy if exists
            if cagg.refresh_policy:
                if cagg.refresh_policy.schedule_interval:
                    properties["refresh_interval"] = str(
                        cagg.refresh_policy.schedule_interval
                    )

                if cagg.refresh_policy.config:
                    config = cagg.refresh_policy.config
                    # Handle both dict and JSON string formats
                    if isinstance(config, str):
                        try:
                            config = json.loads(config)
                        except json.JSONDecodeError:
                            config = {}
                    if isinstance(config, dict):
                        if "start_offset" in config:
                            properties["refresh_start_offset"] = str(
                                config["start_offset"]
                            )
                        if "end_offset" in config:
                            properties["refresh_end_offset"] = str(config["end_offset"])

        return description, properties, location_urn

    def get_extra_tags(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[Dict[str, List[str]]]:
        """
        Override to add TimescaleDB-specific column tags for hypertables.
        """
        if not self.config.tag_hypertables:
            return None

        # Get cached metadata
        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})

        # Check if this is a hypertable
        hypertables = metadata.get("hypertables", {})
        if table in hypertables:
            # Add hypertable tag to all columns
            columns = inspector.get_columns(table, schema=schema)
            column_tags = {}
            for column in columns:
                column_name = column["name"]
                column_tags[column_name] = ["hypertable"]
            return column_tags

        return None

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        Override to add TimescaleDB-specific subtypes.
        Let parent handle all standard processing.
        """
        # First, yield all standard table processing from parent
        yield from super()._process_table(
            dataset_name, inspector, schema, table, sql_config, data_reader
        )

        # Then add TimescaleDB-specific enhancements
        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})

        # Add hypertable subtype and dataset-level tag
        hypertables = metadata.get("hypertables", {})
        if table in hypertables:
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.get_platform(),
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )

            # Update subtype
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

            # Add dataset-level tag
            if self.config.tag_hypertables:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(
                        tags=[TagAssociationClass(tag=make_tag_urn("hypertable"))]
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
        """
        Override to add TimescaleDB-specific subtypes for continuous aggregates.
        Let parent handle all standard view processing including lineage.
        """
        # First, yield all standard view processing from parent
        # This includes view lineage if enabled
        yield from super()._process_view(
            dataset_name, inspector, schema, view, sql_config
        )

        # Then add TimescaleDB-specific enhancements
        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})

        # Add continuous aggregate subtype and dataset-level tag
        continuous_aggregates = metadata.get("continuous_aggregates", {})
        if view in continuous_aggregates:
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.get_platform(),
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )

            # Update subtype
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[
                        DatasetSubTypes.TIMESCALEDB_CONTINUOUS_AGGREGATE,
                        DatasetSubTypes.VIEW,
                    ]
                ),
            ).as_workunit()

            # Update properties to indicate it's materialized
            # Note: Most properties are already added in get_table_properties
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    customProperties={
                        "materialized": "true",
                        "continuous_aggregate": "true",
                    }
                ),
            ).as_workunit()

            # Add dataset-level tag
            if self.config.tag_continuous_aggregates:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(
                        tags=[
                            TagAssociationClass(
                                tag=make_tag_urn("continuous_aggregate")
                            )
                        ]
                    ),
                ).as_workunit()

    def _process_timescaledb_jobs(
        self, inspector: Inspector, schema: str, database: str
    ) -> Iterable[MetadataWorkUnit]:
        """Process TimescaleDB jobs and emit them as DataJob entities"""

        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})
        jobs = metadata.get("jobs", {})

        if not jobs:
            return

        # First, create a container (DataFlow) for TimescaleDB background jobs
        flow_urn = self._create_jobs_container(database, schema)
        yield from self._emit_jobs_container(flow_urn, database, schema)

        for job_id, job in jobs.items():
            # Use TimescaleDBJob model methods
            job_name = job.get_display_name()

            # Use a more descriptive job ID in the URN
            job_identifier = f"{job_id}_{job.proc_name or 'unknown'}"
            if job.hypertable_name:
                job_identifier = (
                    f"{job_id}_{job.hypertable_name}_{job.proc_name or 'unknown'}"
                )

            job_urn = make_data_job_urn(
                orchestrator="timescaledb",
                flow_id=f"{database}.{schema}.background_jobs",
                job_id=job_identifier,
                cluster=self.config.env,
            )

            # Emit job info
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInfoClass(
                    name=job_name,
                    type=AzkabanJobTypeClass.COMMAND,
                    description=job.get_description(),
                    customProperties=job.get_custom_properties(),
                ),
            ).as_workunit()

            # Add job status
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn, aspect=StatusClass(removed=False)
            ).as_workunit()

            # Add subtype for the job
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=SubTypesClass(
                    typeNames=[JobContainerSubTypes.TIMESCALEDB_BACKGROUND_JOB]
                ),
            ).as_workunit()

            # Add container relationship (DataJob belongs to the DataFlow)
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn, aspect=ContainerClass(container=flow_urn)
            ).as_workunit()

            # Add browse paths for the job
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
                entityUrn=job_urn,
                aspect=ContainerClass(container=schema_container_key.as_urn()),
            ).as_workunit()

            # Add lineage if job is associated with a hypertable or continuous aggregate
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

                # Determine if it's input or output based on job type
                proc_name = job.proc_name or ""
                if "refresh" in proc_name:
                    outputs.append(dataset_urn)
                elif "retention" in proc_name or "compression" in proc_name:
                    inputs.append(dataset_urn)
                    outputs.append(
                        dataset_urn
                    )  # Also output since it modifies the table
                else:
                    # Default to both for unknown job types
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
        """Get all hypertables in a schema with their metadata"""
        hypertables = {}

        query = """
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
                    AND j.proc_name = 'policy_retention'
                LIMIT 1
            ) as retention_policy
        FROM timescaledb_information.hypertables ht
        WHERE ht.hypertable_schema = :schema
        """

        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(text(query), {"schema": schema})
                for row in result:
                    hypertable = Hypertable.from_db_row(row)
                    hypertables[hypertable.name] = hypertable
        except Exception as e:
            self.report.warning(
                title="Failed to get hypertables",
                message=f"Could not fetch hypertable information for schema {schema}",
                exc=e,
            )

        return hypertables

    def _get_continuous_aggregates(
        self, inspector: Inspector, schema: str
    ) -> Dict[str, ContinuousAggregate]:
        """Get all continuous aggregates in a schema with their metadata"""
        continuous_aggregates = {}

        query = """
        SELECT 
            ca.view_name,
            ca.materialized_only,
            ca.compression_enabled,
            ca.hypertable_schema,
            ca.hypertable_name,
            ca.view_definition,
            (
                SELECT json_build_object(
                    'schedule_interval', j.schedule_interval::text,
                    'config', j.config
                )
                FROM timescaledb_information.jobs j
                WHERE j.hypertable_schema = ca.materialization_hypertable_schema
                    AND j.hypertable_name = ca.materialization_hypertable_name
                    AND j.proc_name = 'policy_refresh_continuous_aggregate'
                LIMIT 1
            ) as refresh_policy
        FROM timescaledb_information.continuous_aggregates ca
        WHERE ca.view_schema = :schema
        """

        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(text(query), {"schema": schema})
                for row in result:
                    cagg = ContinuousAggregate.from_db_row(row)
                    continuous_aggregates[cagg.name] = cagg
        except Exception as e:
            self.report.warning(
                title="Failed to get continuous aggregates",
                message=f"Could not fetch continuous aggregate information for schema {schema}",
                exc=e,
            )

        return continuous_aggregates

    def _get_jobs(self, inspector: Inspector, schema: str) -> Dict[int, TimescaleDBJob]:
        """Get all TimescaleDB jobs for a schema"""
        jobs = {}

        # Note: timezone column is not available in all TimescaleDB versions
        query = """
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

        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(text(query), {"schema": schema})
                for row in result:
                    job_id = row["job_id"]
                    job = TimescaleDBJob.from_db_row(row)

                    # Check if job matches pattern using the display name
                    job_display_name = job.get_display_name()
                    if not self.config.job_pattern.allowed(job_display_name):
                        self.report.report_dropped(f"Job: {job_display_name}")
                        continue

                    jobs[job_id] = job
        except Exception as e:
            self.report.warning(
                title="Failed to get jobs",
                message=f"Could not fetch job information for schema {schema}",
                exc=e,
            )

        return jobs

    def _create_jobs_container(self, database: str, schema: str) -> str:
        """Create a URN for the TimescaleDB jobs container (DataFlow)"""

        return make_data_flow_urn(
            orchestrator="timescaledb",
            flow_id=f"{database}.{schema}.background_jobs",
            cluster=self.config.env,
        )

    def _emit_jobs_container(
        self, flow_urn: str, database: str, schema: str
    ) -> Iterable[MetadataWorkUnit]:
        """Emit metadata for the TimescaleDB jobs container"""

        # Create container info
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=f"TimescaleDB Background Jobs ({schema})",
                customProperties={
                    "database": database,
                    "schema": schema,
                    "orchestrator": self.get_platform(),
                },
            ),
        ).as_workunit()

        # Add status
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        # Add subtype
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=SubTypesClass(
                typeNames=[FlowContainerSubTypes.TIMESCALEDB_BACKGROUND_JOBS]
            ),
        ).as_workunit()

        # Add browse paths
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
