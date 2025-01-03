import logging
import traceback
from typing import Dict, Iterable, List, Optional

import jaydebeapi

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.jdbc.config import JDBCSourceConfig
from datahub.ingestion.source.jdbc.connection import ConnectionManager
from datahub.ingestion.source.jdbc.container_entities import (
    ContainerRegistry,
    Containers,
    SchemaContainerBuilder,
)
from datahub.ingestion.source.jdbc.datasets import Datasets
from datahub.ingestion.source.jdbc.reporting import JDBCSourceReport
from datahub.ingestion.source.jdbc.sql_utils import SQLUtils
from datahub.ingestion.source.jdbc.stored_procedures import StoredProcedures
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)


@platform_name("JDBC", id="jdbc")
@config_class(JDBCSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class JDBCSource(StatefulIngestionSourceBase):
    """
    The JDBC source plugin provides comprehensive metadata extraction capabilities for JDBC-compliant databases. It supports:
    - Extraction of database structure (tables, views, columns)
    - Schema metadata including data types and constraints
    - View definitions and dependencies
    - Stored procedures (optional)
    - SSL connections with certificate management
    - Maven-based driver management
    - Flexible pattern matching for schema/table filtering

    The plugin uses Java Database Connectivity (JDBC) APIs through JPype and JayDeBeApi, allowing it to support any database with a JDBC driver.
    It handles connection pooling, retries, and proper resource cleanup to ensure reliable metadata extraction.
    """

    config: JDBCSourceConfig
    report: JDBCSourceReport

    def __init__(self, config: JDBCSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.platform = config.platform
        self.platform_instance = config.platform_instance
        self.env = config.env
        self.report = JDBCSourceReport()

        # Initialize managers and utilities
        self.connection_manager = ConnectionManager()
        self.container_registry = ContainerRegistry()
        self.schema_container_builder = SchemaContainerBuilder(
            platform=self.platform,
            platform_instance=self.platform_instance,
            env=self.env,
            registry=self.container_registry,
        )

        # Initialize component handlers
        self.containers = Containers(
            platform=self.platform,
            platform_instance=self.platform_instance,
            env=self.env,
            uri=self.config.connection.uri,
            schema_pattern=self.config.schema_pattern,
            container_registry=self.container_registry,
            schema_container_builder=self.schema_container_builder,
            report=self.report,
        )

        # Set SQL dialect
        SQLUtils.set_dialect(self.config.sqlglot_dialect)

        # Initialize SQL parsing
        self.sql_parsing_aggregator = SqlParsingAggregator(
            platform=make_data_platform_urn(self.platform),
            platform_instance=self.platform_instance,
            env=self.config.env,
            generate_queries=False,
            generate_query_subject_fields=False,
            generate_query_usage_statistics=False,
            generate_usage_statistics=False,
            generate_operations=False,
            graph=ctx.graph,
            usage_config=self.config.usage,
        )

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "JDBCSource":
        """Create a new instance of JDBCSource."""
        config = JDBCSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Generate metadata work units."""
        try:
            conn = self.connection_manager.get_connection(
                driver_class=self.config.driver.driver_class,
                uri=self.config.connection.uri,
                username=self.config.connection.username,
                password=self.config.connection.password,
                driver_path=self.config.driver.driver_path,
                maven_coordinates=self.config.driver.maven_coordinates,
                properties=self.config.connection.properties,
                ssl_config=self.config.connection.ssl_config,
                jvm_args=self.config.jvm_args,
            )

            with conn as connection:
                metadata: jaydebeapi.Connection.cursor = connection.jconn.getMetaData()

                # Extract database container
                yield from self.containers.extract_database_metadata(metadata)

                # Extract schema containers
                yield from self.containers.extract_schema_containers(metadata)

                # Extract tables and views
                if self.config.include_tables or self.config.include_views:
                    dataset_extractor = Datasets(
                        platform=self.platform,
                        platform_instance=self.platform_instance,
                        env=self.env,
                        schema_pattern=self.config.schema_pattern,
                        table_pattern=self.config.table_pattern,
                        view_pattern=self.config.view_pattern,
                        include_tables=self.config.include_tables,
                        include_views=self.config.include_views,
                        report=self.report,
                        container_registry=self.container_registry,
                        schema_container_builder=self.schema_container_builder,
                        sql_parsing_aggregator=self.sql_parsing_aggregator,
                        connection=connection,
                    )
                    yield from dataset_extractor.extract_datasets(
                        metadata, self.containers.get_database_name(metadata)
                    )

                # Extract stored procedures if enabled
                if self.config.include_stored_procedures:
                    stored_proc_extractor = StoredProcedures(
                        platform=self.platform,
                        platform_instance=self.platform_instance,
                        env=self.env,
                        schema_pattern=self.config.schema_pattern,
                        report=self.report,
                    )
                    yield from stored_proc_extractor.extract_procedures(
                        metadata, self.containers.get_database_name(metadata)
                    )

                # Process any additional metadata
                for wu in self.sql_parsing_aggregator.gen_metadata():
                    self.report.report_workunit(wu.as_workunit())
                    yield wu.as_workunit()

        except Exception as e:
            self.report.report_failure("jdbc-source", f"Extraction failed: {str(e)}")
            logger.error(f"JDBC extraction failed: {str(e)}")
            logger.debug(traceback.format_exc())

    def get_report(self):
        return self.report

    def close(self):
        """Clean up resources."""
        self.connection_manager.close()
