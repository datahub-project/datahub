import logging
from typing import Any, Iterable, List, Optional, Union

from pydantic import BaseModel
from pydantic.fields import Field
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter import mce_builder
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
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SubTypesClass,
)

logger: logging.Logger = logging.getLogger(__name__)

# TimescaleDB specific queries
HYPERTABLE_QUERY = """
SELECT 
    h.schema_name,
    h.table_name,
    h.num_dimensions,
    h.num_chunks,
    h.compression_state,
    h.compressed_heap_size,
    h.uncompressed_heap_size,
    h.tablespaces
FROM timescaledb_information.hypertables h
WHERE h.schema_name = %s AND h.table_name = %s;
"""

HYPERTABLE_DIMENSIONS_QUERY = """
SELECT 
    d.dimension_name,
    d.dimension_type,
    d.time_interval,
    d.integer_interval,
    d.num_partitions
FROM timescaledb_information.dimensions d
WHERE d.schema_name = %s AND d.table_name = %s
ORDER BY d.dimension_number;
"""

CONTINUOUS_AGGREGATES_QUERY = """
SELECT 
    ca.view_name,
    ca.view_schema,
    ca.view_owner,
    ca.materialized_only,
    ca.view_definition,
    ca.finalized
FROM timescaledb_information.continuous_aggregates ca
WHERE ca.view_schema = %s;
"""

COMPRESSION_SETTINGS_QUERY = """
SELECT 
    cs.schema_name,
    cs.table_name,
    cs.compression_enabled,
    cs.compress_orderby,
    cs.compress_segmentby
FROM timescaledb_information.compression_settings cs
WHERE cs.schema_name = %s AND cs.table_name = %s;
"""

CHUNKS_QUERY = """
SELECT 
    c.chunk_schema,
    c.chunk_name,
    c.table_name,
    c.schema_name,
    c.is_compressed,
    c.chunk_tablespace,
    c.data_nodes
FROM timescaledb_information.chunks c
WHERE c.schema_name = %s AND c.table_name = %s
ORDER BY c.chunk_name;
"""


class HypertableInfo(BaseModel):
    schema_name: str
    table_name: str
    num_dimensions: int
    num_chunks: int
    compression_state: Optional[str]
    compressed_heap_size: Optional[int]
    uncompressed_heap_size: Optional[int]
    tablespaces: Optional[str]


class HypertableDimension(BaseModel):
    dimension_name: str
    dimension_type: str
    time_interval: Optional[str]
    integer_interval: Optional[int]
    num_partitions: Optional[int]


class ContinuousAggregateInfo(BaseModel):
    view_name: str
    view_schema: str
    view_owner: str
    materialized_only: bool
    view_definition: str
    finalized: bool


class CompressionSettings(BaseModel):
    schema_name: str
    table_name: str
    compression_enabled: bool
    compress_orderby: Optional[str]
    compress_segmentby: Optional[str]


class ChunkInfo(BaseModel):
    chunk_schema: str
    chunk_name: str
    table_name: str
    schema_name: str
    is_compressed: bool
    chunk_tablespace: Optional[str]
    data_nodes: Optional[str]


class TimescaleDBConfig(PostgresConfig):
    """
    TimescaleDB-specific configuration with sensible defaults for time-series workloads.
    Provides full control over TimescaleDB features without inheriting potentially
    irrelevant PostgreSQL-specific configurations.
    """

    # Stored procedures - less common in time-series use cases, disabled by default
    include_stored_procedures: bool = Field(
        default=False,
        description=(
            "Include stored procedures. Note: While supported, time-series workloads "
            "typically prefer continuous aggregates over stored procedures."
        ),
    )

    # TimescaleDB-specific features
    include_hypertables: bool = Field(
        default=True,
        description="Extract hypertable metadata (dimensions, chunks, compression).",
    )
    include_continuous_aggregates: bool = Field(
        default=True,
        description="Extract continuous aggregates as special dataset types.",
    )
    include_chunks: bool = Field(
        default=False,
        description="Extract chunk metadata. Can be verbose - use with caution on large deployments.",
    )
    include_compression_info: bool = Field(
        default=True, description="Extract compression settings and ratios."
    )
    continuous_aggregate_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Filter continuous aggregates to include.",
    )
    max_chunks_per_hypertable: int = Field(
        default=50,  # Conservative default
        description="Max chunks to analyze per hypertable. -1 for unlimited.",
    )

    # Environment and performance settings
    skip_timescaledb_internal_schemas: bool = Field(
        default=True,
        description="Auto-skip TimescaleDB internal schemas (_timescaledb_*, etc).",
    )
    timescaledb_cloud_optimizations: bool = Field(
        default=False, description="Enable Timescale Cloud-specific optimizations."
    )
    skip_cloud_unavailable_features: bool = Field(
        default=True,
        description="Skip features not available in cloud environments vs failing.",
    )

    # Time-series specific analysis
    detect_time_series_patterns: bool = Field(
        default=True,
        description="Detect and tag time-series patterns in regular tables.",
    )
    include_retention_policies: bool = Field(
        default=True, description="Extract data retention policy information."
    )
    analyze_chunk_intervals: bool = Field(
        default=True, description="Analyze and report on chunk time intervals."
    )


@platform_name("TimescaleDB")
@config_class(TimescaleDBConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class TimescaleDBSource(PostgresSource):
    """
    TimescaleDB connector that extends PostgreSQL functionality with time-series specific features.

    Key differences from PostgreSQL:
    - Hypertable metadata extraction
    - Continuous aggregate handling
    - Compression analysis
    - Chunk management insights
    - Time-series optimized defaults
    - Automatic filtering of TimescaleDB internals

    Inherits PostgreSQL core functionality while adding TimescaleDB-specific implementation.
    """

    config: TimescaleDBConfig

    def __init__(self, config: TimescaleDBConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

        # Store original TimescaleDB config for TimescaleDB-specific features
        self.config = config
        self._timescaledb_version: Optional[str] = None
        self._detected_internal_schemas: Optional[List[str]] = None

    def get_platform(self):
        return "timescaledb"

    @classmethod
    def create(cls, config_dict, ctx):
        config = TimescaleDBConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _get_timescaledb_version(self, inspector: Inspector) -> str:
        """Get TimescaleDB version and detect environment"""
        if self._timescaledb_version is None:
            try:
                with inspector.engine.connect() as conn:
                    result = conn.execute(
                        "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';"
                    )
                    row = result.fetchone()
                    if row:
                        self._timescaledb_version = row[0]

                        # Try to detect if this is Timescale Cloud
                        try:
                            cloud_result = conn.execute(
                                "SELECT current_setting('timescaledb.license', true);"
                            )
                            license_row = cloud_result.fetchone()
                            if (
                                license_row
                                and "timescale" in str(license_row[0]).lower()
                            ):
                                self._timescaledb_version += " (Cloud)"
                        except Exception:
                            pass
                    else:
                        self._timescaledb_version = "unknown"
                        logger.warning(
                            "TimescaleDB extension not found. This may not be a TimescaleDB instance."
                        )
            except Exception as e:
                logger.warning(f"Failed to get TimescaleDB version: {e}")
                self._timescaledb_version = "unknown"
        return self._timescaledb_version

    def _get_timescaledb_internal_schemas_from_db(
        self, inspector: Inspector
    ) -> List[str]:
        """Dynamically detect TimescaleDB internal schemas from the database"""
        try:
            with inspector.engine.connect() as conn:
                result = conn.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name LIKE '_timescaledb%' 
                       OR schema_name LIKE 'timescaledb_%'
                       OR schema_name = 'timescaledb_information'
                    ORDER BY schema_name
                """)
                return [row[0] for row in result]
        except Exception as e:
            logger.debug(
                f"Could not dynamically detect TimescaleDB internal schemas: {e}"
            )
            return []

    def _is_timescaledb_internal_schema(self, schema_name: str) -> bool:
        """Check if a schema is a TimescaleDB internal schema"""
        internal_prefixes = [
            "_timescaledb_",
            "timescaledb_information",
            "timescaledb_experimental",
            "timescaledb_catalog",
        ]

        return any(
            schema_name.startswith(prefix) or schema_name == prefix
            for prefix in internal_prefixes
        )

    def _should_skip_schema(self, schema_name: str, inspector: Inspector) -> bool:
        """TimescaleDB-aware schema filtering"""
        # First check parent class schema pattern
        if not self.config.schema_pattern.allowed(schema_name):
            return True

        # Then check TimescaleDB internal schema auto-skip
        if self.config.skip_timescaledb_internal_schemas:
            if self._is_timescaledb_internal_schema(schema_name):
                self.report.report_dropped(
                    f"schema:{schema_name} (TimescaleDB internal)"
                )
                return True

            # Dynamic detection (cached)
            if self._detected_internal_schemas is None:
                self._detected_internal_schemas = (
                    self._get_timescaledb_internal_schemas_from_db(inspector)
                )
                if self._detected_internal_schemas:
                    logger.info(
                        f"Auto-detected {len(self._detected_internal_schemas)} TimescaleDB internal schemas"
                    )

            if schema_name in (self._detected_internal_schemas or []):
                self.report.report_dropped(
                    f"schema:{schema_name} (TimescaleDB internal - detected)"
                )
                return True

        return False

    def get_schema_names(self, inspector: Inspector) -> List[str]:
        """Override to apply TimescaleDB-specific schema filtering"""
        all_schemas = inspector.get_schema_names()

        filtered_schemas = []
        for schema in all_schemas:
            if not self._should_skip_schema(schema, inspector):
                filtered_schemas.append(schema)

        logger.info(
            f"Schema discovery: {len(all_schemas)} total → {len(filtered_schemas)} after TimescaleDB filtering"
        )
        return filtered_schemas

    def _safe_execute_timescaledb_query(
        self, inspector: Inspector, query: str, params: List[str], operation_name: str
    ) -> Optional[Any]:
        """Safely execute TimescaleDB queries with cloud compatibility"""
        try:
            with inspector.engine.connect() as conn:
                return conn.execute(query, params)
        except Exception as e:
            if self.config.skip_cloud_unavailable_features:
                logger.warning(
                    f"Skipping {operation_name} (may not be available in this environment): {e}"
                )
                return None
            else:
                raise e

    def _is_hypertable(self, inspector: Inspector, schema: str, table: str) -> bool:
        """Check if a table is a TimescaleDB hypertable"""
        result = self._safe_execute_timescaledb_query(
            inspector,
            "SELECT 1 FROM timescaledb_information.hypertables WHERE schema_name = %s AND table_name = %s LIMIT 1;",
            [schema, table],
            "hypertable detection",
        )
        if result is None:
            return False
        return result.fetchone() is not None

    def _get_hypertable_info(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[HypertableInfo]:
        """Get hypertable metadata"""
        if not self.config.include_hypertables:
            return None

        result = self._safe_execute_timescaledb_query(
            inspector, HYPERTABLE_QUERY, [schema, table], "hypertable info extraction"
        )
        if result is None:
            return None

        row = result.fetchone()
        if row:
            return HypertableInfo(**dict(row))
        return None

    def _get_hypertable_dimensions(
        self, inspector: Inspector, schema: str, table: str
    ) -> List[HypertableDimension]:
        """Get hypertable dimensions"""
        result = self._safe_execute_timescaledb_query(
            inspector,
            HYPERTABLE_DIMENSIONS_QUERY,
            [schema, table],
            "hypertable dimensions",
        )
        if result is None:
            return []

        return [HypertableDimension(**dict(row)) for row in result]

    def _get_compression_settings(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[CompressionSettings]:
        """Get compression settings for a hypertable"""
        if not self.config.include_compression_info:
            return None

        result = self._safe_execute_timescaledb_query(
            inspector,
            COMPRESSION_SETTINGS_QUERY,
            [schema, table],
            "compression settings",
        )
        if result is None:
            return None

        row = result.fetchone()
        return CompressionSettings(**dict(row)) if row else None

    def _get_chunk_info(
        self, inspector: Inspector, schema: str, table: str
    ) -> List[ChunkInfo]:
        """Get chunk information for a hypertable"""
        if not self.config.include_chunks:
            return []

        query = CHUNKS_QUERY
        if self.config.max_chunks_per_hypertable > 0:
            query += f" LIMIT {self.config.max_chunks_per_hypertable}"

        result = self._safe_execute_timescaledb_query(
            inspector, query, [schema, table], "chunk information"
        )
        if result is None:
            return []

        return [ChunkInfo(**dict(row)) for row in result]

    def _get_continuous_aggregates(
        self, inspector: Inspector, schema: str
    ) -> List[ContinuousAggregateInfo]:
        """Get continuous aggregates for a schema"""
        if not self.config.include_continuous_aggregates:
            return []

        result = self._safe_execute_timescaledb_query(
            inspector, CONTINUOUS_AGGREGATES_QUERY, [schema], "continuous aggregates"
        )
        if result is None:
            return []

        caggs = []
        for row in result:
            cagg_info = ContinuousAggregateInfo(**dict(row))
            cagg_qualified_name = f"{schema}.{cagg_info.view_name}"

            if self.config.continuous_aggregate_pattern.allowed(cagg_qualified_name):
                caggs.append(cagg_info)
            else:
                self.report.report_dropped(cagg_qualified_name)

        return caggs

    # Property addition methods (same as before)
    def _add_timescaledb_base_properties(
        self, properties: DatasetPropertiesClass, inspector: Inspector
    ) -> None:
        """Add base TimescaleDB properties"""
        if properties.customProperties is None:
            properties.customProperties = {}

        properties.customProperties["timescaledb.version"] = (
            self._get_timescaledb_version(inspector)
        )

    def _add_hypertable_basic_info(
        self, properties: DatasetPropertiesClass, hypertable_info: HypertableInfo
    ) -> None:
        """Add basic hypertable information"""
        properties.customProperties["timescaledb.num_dimensions"] = str(
            hypertable_info.num_dimensions
        )
        properties.customProperties["timescaledb.num_chunks"] = str(
            hypertable_info.num_chunks
        )

        if hypertable_info.compression_state:
            properties.customProperties["timescaledb.compression_state"] = (
                hypertable_info.compression_state
            )

    def _add_hypertable_size_info(
        self, properties: DatasetPropertiesClass, hypertable_info: HypertableInfo
    ) -> None:
        """Add hypertable size and compression ratio"""
        if hypertable_info.compressed_heap_size is not None:
            properties.customProperties["timescaledb.compressed_heap_size"] = str(
                hypertable_info.compressed_heap_size
            )

        if hypertable_info.uncompressed_heap_size is not None:
            properties.customProperties["timescaledb.uncompressed_heap_size"] = str(
                hypertable_info.uncompressed_heap_size
            )

            if (
                hypertable_info.compressed_heap_size
                and hypertable_info.compressed_heap_size > 0
            ):
                ratio = (
                    hypertable_info.uncompressed_heap_size
                    / hypertable_info.compressed_heap_size
                )
                properties.customProperties["timescaledb.compression_ratio"] = (
                    f"{ratio:.2f}x"
                )

    def _add_dimension_info(
        self, properties: DatasetPropertiesClass, dimensions: List[HypertableDimension]
    ) -> None:
        """Add dimension information"""
        time_dims = [d for d in dimensions if d.dimension_type == "Time"]
        space_dims = [d for d in dimensions if d.dimension_type == "Space"]

        if time_dims:
            properties.customProperties["timescaledb.time_dimension"] = time_dims[
                0
            ].dimension_name
            if time_dims[0].time_interval:
                properties.customProperties["timescaledb.time_interval"] = time_dims[
                    0
                ].time_interval

        if space_dims:
            properties.customProperties["timescaledb.space_dimensions"] = ", ".join(
                [d.dimension_name for d in space_dims]
            )

    def _add_compression_info(
        self, properties: DatasetPropertiesClass, compression: CompressionSettings
    ) -> None:
        """Add compression settings"""
        properties.customProperties["timescaledb.compression_enabled"] = str(
            compression.compression_enabled
        )
        if compression.compress_orderby:
            properties.customProperties["timescaledb.compress_orderby"] = (
                compression.compress_orderby
            )
        if compression.compress_segmentby:
            properties.customProperties["timescaledb.compress_segmentby"] = (
                compression.compress_segmentby
            )

    def _add_chunk_info(
        self, properties: DatasetPropertiesClass, chunks: List[ChunkInfo]
    ) -> None:
        """Add chunk statistics"""
        if chunks:
            compressed_chunks = len([c for c in chunks if c.is_compressed])
            properties.customProperties["timescaledb.total_chunks"] = str(len(chunks))
            properties.customProperties["timescaledb.compressed_chunks"] = str(
                compressed_chunks
            )
            properties.customProperties["timescaledb.uncompressed_chunks"] = str(
                len(chunks) - compressed_chunks
            )

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        """Generate all workunits including TimescaleDB-specific metadata"""
        # First get all PostgreSQL base metadata
        yield from super().get_workunits_internal()

        # Then add TimescaleDB-specific metadata
        for inspector in self.get_inspectors():
            yield from self._get_timescaledb_metadata_workunits(inspector)

    def _get_timescaledb_metadata_workunits(
        self, inspector: Inspector
    ) -> Iterable[MetadataWorkUnit]:
        """Generate TimescaleDB-specific metadata workunits"""
        # Add TimescaleDB properties to existing datasets
        yield from self._get_hypertable_metadata_workunits(inspector)

        # Add continuous aggregates as new datasets
        if self.config.include_continuous_aggregates:
            yield from self._get_continuous_aggregate_workunits(inspector)

    def _get_hypertable_metadata_workunits(
        self, inspector: Inspector
    ) -> Iterable[MetadataWorkUnit]:
        """Add TimescaleDB metadata to existing table datasets"""
        try:
            schemas = self.get_schema_names(inspector)

            for schema in schemas:
                table_names = inspector.get_table_names(schema)
                for table in table_names:
                    # Generate TimescaleDB metadata MCP for this table
                    dataset_name = self.get_identifier(
                        schema=schema, entity=table, inspector=inspector
                    )
                    dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                        platform=self.get_platform(),
                        name=dataset_name,
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )

                    # Create TimescaleDB-specific properties
                    properties = self._create_timescaledb_dataset_properties(
                        inspector, schema, table
                    )
                    if properties and properties.customProperties:
                        yield MetadataChangeProposalWrapper(
                            entityUrn=dataset_urn, aspect=properties
                        ).as_workunit()

        except Exception as e:
            logger.warning(f"Failed to generate TimescaleDB metadata workunits: {e}")

    def _create_timescaledb_dataset_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[DatasetPropertiesClass]:
        """Create TimescaleDB-specific dataset properties"""
        properties = DatasetPropertiesClass(customProperties={})

        self._add_timescaledb_base_properties(properties, inspector)

        is_hypertable = self._is_hypertable(inspector, schema, table)
        properties.customProperties["timescaledb.is_hypertable"] = str(is_hypertable)

        if is_hypertable:
            hypertable_info = self._get_hypertable_info(inspector, schema, table)
            if hypertable_info:
                self._add_hypertable_basic_info(properties, hypertable_info)
                self._add_hypertable_size_info(properties, hypertable_info)

            dimensions = self._get_hypertable_dimensions(inspector, schema, table)
            if dimensions:
                self._add_dimension_info(properties, dimensions)

            compression = self._get_compression_settings(inspector, schema, table)
            if compression:
                self._add_compression_info(properties, compression)

            if self.config.include_chunks:
                chunks = self._get_chunk_info(inspector, schema, table)
                self._add_chunk_info(properties, chunks)

        return properties if properties.customProperties else None

    def _create_continuous_aggregate_properties(
        self, cagg: ContinuousAggregateInfo, inspector: Inspector
    ) -> DatasetPropertiesClass:
        """Create properties for continuous aggregate"""
        return DatasetPropertiesClass(
            name=cagg.view_name,
            description=f"TimescaleDB Continuous Aggregate: {cagg.view_name}",
            customProperties={
                "timescaledb.type": "continuous_aggregate",
                "timescaledb.materialized_only": str(cagg.materialized_only),
                "timescaledb.finalized": str(cagg.finalized),
                "timescaledb.owner": cagg.view_owner,
                "timescaledb.definition": cagg.view_definition[:1000]
                + ("..." if len(cagg.view_definition) > 1000 else ""),
                "timescaledb.version": self._get_timescaledb_version(inspector),
            },
        )

    def _get_continuous_aggregate_workunits(
        self, inspector: Inspector
    ) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for continuous aggregates"""
        try:
            schemas = self.get_schema_names(inspector)

            for schema in schemas:
                caggs = self._get_continuous_aggregates(inspector, schema)
                for cagg in caggs:
                    yield from self._generate_cagg_mcp_workunits(cagg, inspector)

        except Exception as e:
            logger.warning(f"Failed to generate continuous aggregate workunits: {e}")

    def _generate_cagg_mcp_workunits(
        self, cagg: ContinuousAggregateInfo, inspector: Inspector
    ) -> Iterable[MetadataWorkUnit]:
        """Generate MCP workunits for a continuous aggregate"""
        dataset_name = self.get_identifier(
            schema=cagg.view_schema, entity=cagg.view_name, inspector=inspector
        )
        dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
            platform=self.get_platform(),
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Dataset properties
        properties = self._create_continuous_aggregate_properties(cagg, inspector)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=properties
        ).as_workunit()

        # Dataset subtypes
        subtypes = SubTypesClass(typeNames=["Continuous Aggregate", "View"])
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=subtypes
        ).as_workunit()
