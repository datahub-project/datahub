import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.engine.reflection import Inspector

from datahub.emitter.mce_builder import (
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
    DatasetPropertiesClass,
    GlobalTagsClass,
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
            }

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

    def get_extra_tags(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[Dict[str, List[str]]]:
        """
        Override to add TimescaleDB-specific tags.
        Returns tags per column as expected by parent class.
        """
        tags = super().get_extra_tags(inspector, schema, table) or {}

        # Get cached metadata
        db_name = self.get_db_name(inspector)
        cache_key = f"{db_name}.{schema}"
        metadata = self._timescaledb_metadata_cache.get(cache_key, {})

        # Add hypertable tags
        if self.config.tag_hypertables:
            hypertables = metadata.get("hypertables", {})
            if table in hypertables:
                # Get columns to tag all of them
                try:
                    columns = inspector.get_columns(table, schema)
                    for column in columns:
                        col_name = column["name"]
                        if col_name not in tags:
                            tags[col_name] = []
                        tags[col_name].append("hypertable")
                except Exception as e:
                    logger.debug(f"Could not get columns for tagging: {e}")

        # Add continuous aggregate tags
        if self.config.tag_continuous_aggregates:
            continuous_aggregates = metadata.get("continuous_aggregates", {})
            if table in continuous_aggregates:
                try:
                    columns = inspector.get_columns(table, schema)
                    for column in columns:
                        col_name = column["name"]
                        if col_name not in tags:
                            tags[col_name] = []
                        tags[col_name].append("continuous_aggregate")
                except Exception as e:
                    logger.debug(f"Could not get columns for tagging: {e}")

        return tags if tags else None

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

    def _get_hypertables(self, inspector: Inspector, schema: str) -> Dict[str, Dict]:
        """Get all hypertables in a schema with their metadata"""
        hypertables = {}

        query = """
        SELECT 
            ht.table_name,
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
                WHERE d.hypertable_schema = ht.schema_name 
                    AND d.hypertable_name = ht.table_name
            ) as dimensions,
            (
                SELECT json_build_object('drop_after', j.config->>'drop_after')
                FROM timescaledb_information.jobs j
                WHERE j.hypertable_schema = ht.schema_name
                    AND j.hypertable_name = ht.table_name
                    AND j.proc_name = 'policy_retention'
                LIMIT 1
            ) as retention_policy
        FROM timescaledb_information.hypertables ht
        WHERE ht.schema_name = :schema
        """

        try:
            with inspector.engine.connect() as conn:
                result = conn.execute(text(query), {"schema": schema})
                for row in result:
                    hypertables[row["table_name"]] = {
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
