import json
import logging
from typing import Any, Dict, Iterable, Optional, Tuple, Union

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
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
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
)

logger: logging.Logger = logging.getLogger(__name__)


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
@support_status(SupportStatus.CERTIFIED)
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
    - We enhance these with TimescaleDB-specific metadata
    """

    config: TimescaleDBConfig
    _timescaledb_metadata_cache: Dict[str, Dict[str, Any]]

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
            ht_info = hypertables[table]
            properties["is_hypertable"] = "true"
            properties["timescale_num_dimensions"] = str(
                ht_info.get("num_dimensions", 0)
            )
            properties["timescale_num_chunks"] = str(ht_info.get("num_chunks", 0))
            properties["timescale_compression_enabled"] = str(
                ht_info.get("compression_enabled", False)
            )

            # Add dimension information
            for i, dim in enumerate(ht_info.get("dimensions", [])):
                prefix = f"timescale_dimension_{i}"
                properties[f"{prefix}_column"] = dim["column_name"]
                properties[f"{prefix}_type"] = dim["column_type"]
                if dim.get("time_interval"):
                    properties[f"{prefix}_interval"] = dim["time_interval"]

            # Add retention policy if exists
            retention = ht_info.get("retention_policy", {})
            if retention.get("drop_after"):
                properties["timescale_retention_period"] = retention["drop_after"]

        # Check if this is a continuous aggregate (view)
        continuous_aggregates = metadata.get("continuous_aggregates", {})
        if table in continuous_aggregates:
            cagg_info = continuous_aggregates[table]
            properties["is_continuous_aggregate"] = "true"
            properties["timescale_materialized_only"] = str(
                cagg_info.get("materialized_only", False)
            )
            properties["timescale_compression_enabled"] = str(
                cagg_info.get("compression_enabled", False)
            )
            properties["timescale_source_hypertable"] = (
                f"{cagg_info.get('hypertable_schema', schema)}.{cagg_info.get('hypertable_name', '')}"
            )

            # Add refresh policy if exists
            refresh = cagg_info.get("refresh_policy", {})
            if refresh.get("schedule_interval"):
                properties["timescale_refresh_interval"] = str(
                    refresh["schedule_interval"]
                )
            if refresh.get("config"):
                config = refresh["config"]
                # Handle both dict and JSON string formats
                if isinstance(config, str):
                    try:
                        config = json.loads(config)
                    except json.JSONDecodeError:
                        config = {}
                if isinstance(config, dict):
                    if "start_offset" in config:
                        properties["timescale_refresh_start_offset"] = str(
                            config["start_offset"]
                        )
                    if "end_offset" in config:
                        properties["timescale_refresh_end_offset"] = str(
                            config["end_offset"]
                        )

        return description, properties, location_urn

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
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=["Hypertable", "Table"]),
            ).as_workunit()

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
                aspect=SubTypesClass(typeNames=["Continuous Aggregate", "View"]),
            ).as_workunit()

            # Update properties to indicate it's materialized
            # Note: Most properties are already added in get_table_properties
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    customProperties={
                        "materialized": "true",
                        "timescale_continuous_aggregate": "true",
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

        for job_id, job_info in jobs.items():
            job_name = f"{schema}.job_{job_id}_{job_info['proc_name']}"
            job_urn = make_data_job_urn(
                orchestrator="timescaledb",
                flow_id=f"{database}.{schema}",
                job_id=str(job_id),
                cluster=self.config.env,
            )

            # Create job info
            custom_properties = {
                "job_id": str(job_id),
                "application_name": job_info.get("application_name", ""),
                "schedule_interval": str(job_info.get("schedule_interval", "")),
                "max_runtime": str(job_info.get("max_runtime", "")),
                "max_retries": str(job_info.get("max_retries", 0)),
                "retry_period": str(job_info.get("retry_period", "")),
                "proc_schema": job_info.get("proc_schema", ""),
                "proc_name": job_info.get("proc_name", ""),
                "scheduled": str(job_info.get("scheduled", False)),
                "fixed_schedule": str(job_info.get("fixed_schedule", False)),
                "initial_start": str(job_info.get("initial_start", "")),
                "timezone": job_info.get("timezone", ""),
            }

            # Add config if present
            if job_info.get("config"):
                config = job_info["config"]
                if isinstance(config, str):
                    custom_properties["config"] = config
                elif isinstance(config, dict):
                    custom_properties["config"] = json.dumps(config)

            # Add hypertable info if present
            if job_info.get("hypertable_schema") and job_info.get("hypertable_name"):
                custom_properties["hypertable"] = (
                    f"{job_info['hypertable_schema']}.{job_info['hypertable_name']}"
                )

            # Emit job info
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInfoClass(
                    name=job_name,
                    type="TIMESCALEDB_JOB",
                    description=f"TimescaleDB {job_info['proc_name']} job",
                    customProperties=custom_properties,
                ),
            ).as_workunit()

            # Add job status
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn, aspect=StatusClass(removed=False)
            ).as_workunit()

            # Add lineage if job is associated with a hypertable or continuous aggregate
            inputs = []
            outputs = []

            if job_info.get("hypertable_schema") and job_info.get("hypertable_name"):
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.get_platform(),
                    self.get_identifier(
                        schema=job_info["hypertable_schema"],
                        entity=job_info["hypertable_name"],
                        inspector=inspector,
                    ),
                    self.config.platform_instance,
                    self.config.env,
                )

                # Determine if it's input or output based on job type
                if "refresh" in job_info["proc_name"]:
                    outputs.append(dataset_urn)
                elif (
                    "retention" in job_info["proc_name"]
                    or "compression" in job_info["proc_name"]
                ):
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
                        inputDatajobs=[],
                    ),
                ).as_workunit()

    def _get_hypertables(self, inspector: Inspector, schema: str) -> Dict[str, Dict]:
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
                    hypertables[row["hypertable_name"]] = {
                        "num_dimensions": row["num_dimensions"],
                        "num_chunks": row["num_chunks"],
                        "compression_enabled": row["compression_enabled"],
                        "dimensions": row["dimensions"] or [],
                        "retention_policy": row["retention_policy"] or {},
                    }
        except Exception as e:
            self.report.warning(
                title="Failed to get hypertables",
                message=f"Could not fetch hypertable information for schema {schema}",
                exc=e,
            )

        return hypertables

    def _get_continuous_aggregates(
        self, inspector: Inspector, schema: str
    ) -> Dict[str, Dict]:
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
                    continuous_aggregates[row["view_name"]] = {
                        "materialized_only": row["materialized_only"],
                        "compression_enabled": row["compression_enabled"],
                        "hypertable_schema": row["hypertable_schema"],
                        "hypertable_name": row["hypertable_name"],
                        "view_definition": row["view_definition"],
                        "refresh_policy": row["refresh_policy"] or {},
                    }
        except Exception as e:
            self.report.warning(
                title="Failed to get continuous aggregates",
                message=f"Could not fetch continuous aggregate information for schema {schema}",
                exc=e,
            )

        return continuous_aggregates

    def _get_jobs(self, inspector: Inspector, schema: str) -> Dict[int, Dict]:
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
                    job_name = f"{schema}.job_{job_id}_{row['proc_name']}"

                    # Check if job matches pattern
                    if not self.config.job_pattern.allowed(job_name):
                        self.report.report_dropped(job_name)
                        continue

                    jobs[job_id] = {
                        "application_name": row["application_name"],
                        "schedule_interval": row["schedule_interval"],
                        "max_runtime": row["max_runtime"],
                        "max_retries": row["max_retries"],
                        "retry_period": row["retry_period"],
                        "proc_schema": row["proc_schema"],
                        "proc_name": row["proc_name"],
                        "scheduled": row["scheduled"],
                        "fixed_schedule": row["fixed_schedule"],
                        "initial_start": row["initial_start"],
                        "config": row["config"],
                        "hypertable_schema": row["hypertable_schema"],
                        "hypertable_name": row["hypertable_name"],
                    }
        except Exception as e:
            self.report.warning(
                title="Failed to get jobs",
                message=f"Could not fetch job information for schema {schema}",
                exc=e,
            )

        return jobs
