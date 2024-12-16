import os
import sys
import subprocess
import time
import traceback
import urllib.request
import tempfile
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Optional, Union, Tuple, Set, Iterable
from dataclasses import dataclass, field
from pathlib import Path
import hashlib
import logging
import base64
import re
from urllib.parse import parse_qs, urlparse
import jaydebeapi
import glob
from pydantic import Field, validator

from datahub.configuration.common import ConfigModel, AllowDenyPattern
from datahub.configuration.source_common import PlatformInstanceConfigMixin, EnvConfigMixin
from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport, MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dremio.dremio_config import ProfileConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import StaleEntityRemovalSourceReport, \
    StatefulStaleMetadataRemovalConfig, StaleEntityRemovalHandler
from datahub.ingestion.source.state.stateful_ingestion_base import StatefulIngestionConfigBase
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.operation_config import is_profiling_enabled
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.metadata._schema_classes import SchemaFieldDataTypeClass, OtherSchemaClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import OtherSchema
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    SchemaFieldClass,
    ViewPropertiesClass
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_dataset_to_container, gen_containers

from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


class SSLConfig(ConfigModel):
    """SSL Certificate configuration for database connections"""
    cert_path: Optional[str] = Field(
        default=None,
        description="Path to the SSL certificate file on local filesystem",
    )

    cert_content: Optional[str] = Field(
        default=None,
        description="Base64 encoded certificate content that will be written to a temporary file",
    )

    cert_type: str = Field(
        default="pem",
        description="Certificate file type - supported values are: pem, jks, p12",
    )

    cert_password: Optional[str] = Field(
        default=None,
        description="Password for certificate if it is password protected",
    )

    @validator("cert_type")
    def validate_cert_type(cls, v):
        if v not in ["pem", "jks", "p12"]:
            raise ValueError("cert_type must be one of: pem, jks, p12")
        return v


class JDBCConnectionConfig(ConfigModel):
    """JDBC Connection configuration with flexible connection options"""
    full_uri: Optional[str] = Field(
        default=None,
        description="Complete JDBC URI including all connection parameters (e.g. jdbc:postgresql://localhost:5432/mydb?sslmode=require)",
    )

    base_uri: Optional[str] = Field(
        default=None,
        description="Basic JDBC URI without connection parameters (e.g. jdbc:postgresql://localhost:5432/mydb)",
    )

    username: Optional[str] = Field(
        default=None,
        description="Username for database authentication",
    )

    password: Optional[str] = Field(
        default=None,
        description="Password for database authentication",
    )

    properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional JDBC connection properties to be passed to the driver",
    )

    ssl_config: Optional[SSLConfig] = Field(
        default=None,
        description="SSL/TLS configuration for secure database connections",
    )

    @validator("full_uri", "base_uri")
    def validate_uri(cls, v, values):
        if not values.get("full_uri") and not values.get("base_uri"):
            raise ValueError("Either full_uri or base_uri must be specified")
        if values.get("full_uri") and v == "base_uri":
            raise ValueError("Cannot specify both full_uri and base_uri")
        return v


class JDBCDriverConfig(ConfigModel):
    """JDBC Driver configuration supporting multiple source options"""
    driver_class: str = Field(
        description="Fully qualified class name of the JDBC driver (e.g. org.postgresql.Driver)",
    )

    maven_coordinates: Optional[str] = Field(
        default=None,
        description="Maven artifact coordinates in format 'groupId:artifactId:version' (e.g. org.postgresql:postgresql:42.3.1)",
    )

    driver_url: Optional[str] = Field(
        default=None,
        description="Direct download URL for the JDBC driver JAR file",
    )

    driver_path: Optional[str] = Field(
        default=None,
        description="Absolute path to an existing JDBC driver JAR file on the local filesystem",
    )

    driver_dir: Optional[str] = Field(
        default=None,
        description="Directory containing JDBC driver JAR files - first matching JAR will be used",
    )

    @validator("driver_class")
    def validate_driver_class(cls, v):
        if not v:
            raise ValueError("driver_class must be specified")
        return v


class JDBCSourceConfig(
    StatefulIngestionConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    """Configuration class for JDBC metadata extraction"""

    jdbc_driver: JDBCDriverConfig = Field(
        description="JDBC driver configuration including class and source location",
    )

    connection: JDBCConnectionConfig = Field(
        description="Database connection configuration including URI and authentication details",
    )

    include_tables: bool = Field(
        default=True,
        description="Whether to include tables in the metadata extraction process",
    )

    include_views: bool = Field(
        default=True,
        description="Whether to include views in the metadata extraction process",
    )

    parse_view_definitions: bool = Field(
        default=True,
        description="Whether to parse and extract view definitions for lineage analysis",
    )

    # Entity Filters
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter schemas for inclusion/exclusion in ingestion",
    )

    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter tables for inclusion/exclusion in ingestion",
    )

    usage: BaseUsageConfig = Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    # Profiling
    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to profile",
    )
    profiling: ProfileConfig = Field(
        default=ProfileConfig(),
        description="Configuration for profiling",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

@dataclass
class JDBCSourceReport(
    SQLSourceReport, StaleEntityRemovalSourceReport, IngestionStageReport
):
    num_containers_failed: int = 0
    num_datasets_failed: int = 0
    containers_scanned: int = 0
    containers_filtered: int = 0

    def report_upstream_latency(self, start_time: datetime, end_time: datetime) -> None:
        # recording total combined latency is not very useful, keeping this method as a placeholder
        # for future implementation of min / max / percentiles etc.
        pass

    def report_container_scanned(self, name: str) -> None:
        """
        Record that a container was successfully scanned
        """
        self.containers_scanned += 1

    def report_container_filtered(self, container_name: str) -> None:
        """
        Record that a container was filtered out
        """
        self.containers_filtered += 1
        self.report_dropped(container_name)

    def set_ingestion_stage(self, dataset: str, stage: str) -> None:
        self.report_ingestion_stage_start(f"{dataset}: {stage}")


class JDBCSource(Source):
    """JDBC Source for DataHub metadata extraction with full feature support"""
    config: JDBCSourceConfig
    report: JDBCSourceReport

    def __init__(self, config: JDBCSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.platform = "jdbc"
        self.platform_instance = self.config.platform_instance
        self.report = JDBCSourceReport()

        # Initialize SQL parsing aggregator
        self.sql_aggregator = SqlParsingAggregator(
            platform=self.platform,
            platform_instance=self.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph if ctx else None,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=self.config.usage,
        )

        # Connection management
        self._connection = None
        self.connection_timeout = 60  # seconds
        self.max_retries = 3

    def health_check(self) -> bool:
        """Perform basic health check by testing database connection"""
        try:
            with self._get_connection():
                return True
        except Exception as e:
            self.report.report_failure("health", f"Failed health check: {str(e)}")
            return False

    def test_connection(self) -> None:
        """Test database connection and driver availability"""
        try:
            # Test driver path first
            driver_path = self.get_driver_path(self.config.jdbc_driver)
            if not os.path.exists(driver_path):
                raise FileNotFoundError(f"JDBC driver not found at: {driver_path}")

            # Test connection
            with self._get_connection() as conn:
                metadata = conn.getMetaData()
                db_name = metadata.getDatabaseProductName()
                version = metadata.getDatabaseProductVersion()
                logger.info(f"Successfully connected to {db_name} version {version}")

        except Exception as e:
            self.report.report_failure("connection", f"Connection test failed: {str(e)}")
            raise


    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def is_checkpointing_enabled(self) -> bool:
        """Enable stateful ingestion"""
        return True

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Main entry point for metadata extraction"""
        if self.is_checkpointing_enabled() and self.ctx.graph:
            # Initialize checkpoint state
            self.stale_entity_removal_handler = StaleEntityRemovalHandler(
                source=self,
                config=self.config,
                state_provider=self.ctx.pipeline_context.state_provider,
            )

        # Generate all workunits
        yield from self.get_workunits_internal()

        # Handle stale entity removal if enabled
        if self.is_checkpointing_enabled() and self.ctx.graph:
            yield from self.stale_entity_removal_handler.get_workunits()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Implementation of core metadata extraction logic"""
        with self._get_connection() as conn:
            metadata = conn.getMetaData()

            # Extract containers (databases/schemas)
            yield from self._extract_container_workunits(metadata)

            # Extract tables and views
            for workunit in self._extract_tables_and_views(metadata):
                # Report progress
                self.report_progress(workunit)
                yield workunit

            # Extract database-level metadata (if available)
            yield from self._extract_database_metadata(metadata)

    def get_driver_path(self, config: JDBCDriverConfig) -> str:
        """Get JDBC driver path, downloading if necessary"""
        if config.driver_path:
            return config.driver_path

        if config.driver_dir:
            # Search for JAR files in directory
            jar_files = glob.glob(os.path.join(config.driver_dir, "*.jar"))
            if not jar_files:
                raise FileNotFoundError(f"No JAR files found in {config.driver_dir}")
            return jar_files[0]

        driver_dir = Path.home() / ".datahub" / "drivers"
        driver_dir.mkdir(parents=True, exist_ok=True)

        if config.maven_coordinates:
            return self._download_from_maven(config.maven_coordinates, driver_dir)
        elif config.driver_url:
            return self._download_from_url(config.driver_url, driver_dir)
        else:
            raise ValueError("No driver source specified")

    def _download_from_maven(self, coordinates: str, driver_dir: Path) -> str:
        """Download driver from Maven"""
        logger.info(f"Downloading JDBC driver from Maven: {coordinates}")

        maven_cmd = [
            "mvn",
            "dependency:get",
            f"-Dartifact={coordinates}",
            "-DremoteRepositories=https://repo1.maven.org/maven2/",
            f"-DoutputDirectory={driver_dir}"
        ]

        subprocess.run(maven_cmd, check=True, capture_output=True)

        artifact_id, version = coordinates.split(":")[1:3]
        driver_path = driver_dir / f"{artifact_id}-{version}.jar"

        if not driver_path.exists():
            raise FileNotFoundError(f"Maven download succeeded but file not found at {driver_path}")

        return str(driver_path)

    def _download_from_url(self, url: str, driver_dir: Path) -> str:
        """Download driver from URL"""
        logger.info(f"Downloading JDBC driver from URL: {url}")

        filename = f"jdbc-driver-{hashlib.md5(url.encode()).hexdigest()[:8]}.jar"
        driver_path = driver_dir / filename

        urllib.request.urlretrieve(url, driver_path)
        return str(driver_path)

    def setup_ssl_cert(self, ssl_config: SSLConfig) -> Optional[str]:
        """Set up SSL certificate and return path"""
        if not ssl_config:
            return None

        if ssl_config.cert_content:
            cert_content = base64.b64decode(ssl_config.cert_content)
            fd, temp_path = tempfile.mkstemp(suffix=f'.{ssl_config.cert_type}')
            with os.fdopen(fd, 'wb') as f:
                f.write(cert_content)
            return temp_path

        return ssl_config.cert_path

    def get_connection_details(self, config: JDBCConnectionConfig) -> Tuple[str, Dict[str, str]]:
        """Get connection URL and properties"""
        props = dict(config.properties)

        if config.username:
            props["user"] = config.username
        if config.password:
            props["password"] = config.password

        if config.ssl_config:
            cert_path = self.setup_ssl_cert(config.ssl_config)
            if cert_path:
                props["ssl"] = "true"
                props["sslmode"] = "verify-full"
                props["sslcert"] = cert_path

        url = config.full_uri or config.base_uri
        return url, props

    @contextmanager
    def _get_connection(self):
        """Get database connection with retry logic"""
        retry_count = 0
        while True:
            try:
                if not self._connection:
                    url, props = self.get_connection_details(self.config.connection)
                    driver_path = self.get_driver_path(self.config.jdbc_driver)

                    self._connection = jaydebeapi.connect(
                        self.config.jdbc_driver.driver_class,
                        url,
                        props,
                        driver_path,
                        timeout=self.connection_timeout
                    )
                yield self._connection
                break
            except Exception as e:
                if retry_count >= self.max_retries:
                    raise
                retry_count += 1
                logger.warning(f"Connection attempt {retry_count} failed: {str(e)}")
                time.sleep(1)
                if self._connection:
                    try:
                        self._connection.close()
                    except:
                        pass
                    self._connection = None

    def _extract_container_workunits(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract database and schema containers"""
        try:
            with metadata.getCatalogs() as rs:
                catalogs = [rs.getString(1) for rs in rs] or [None]

            for catalog in catalogs:
                if catalog and not self._should_include_container(catalog):
                    self.report.report_container_filtered(catalog)
                    continue

                # Create container for database
                if catalog:
                    container_urn = f"urn:li:container:{self.platform}:{catalog}"
                    container_workunits = gen_containers(
                        container_key=catalog,
                        name=catalog,
                        sub_types=["Database"],
                        description=None,
                        owner_urn=None
                    )
                    for wu in container_workunits:
                        self.report.report_container_scanned(catalog)
                        yield wu

                # Get schemas in this database
                with metadata.getSchemas(catalog, None) as rs:
                    schemas = [rs.getString(1) for rs in rs] or [None]

                for schema in schemas:
                    if schema and not self._should_include_container(schema):
                        self.report.report_container_filtered(schema)
                        continue

                    # Create container for schema
                    if schema:
                        schema_container_key = f"{catalog}.{schema}" if catalog else schema
                        container_workunits = gen_containers(
                            container_key=schema_container_key,
                            name=schema,
                            sub_types=["Schema"],
                            parent_container_key=catalog if catalog else None,
                            description=None,
                            owner_urn=None
                        )
                        for wu in container_workunits:
                            self.report.report_container_scanned(schema)
                            yield wu

        except Exception as e:
            self.report.report_failure("containers", f"Failed to extract containers: {str(e)}")

    def _extract_tables_and_views(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract table and view metadata"""
        try:
            with metadata.getCatalogs() as rs:
                catalogs = [rs.getString(1) for rs in rs] or [None]

            for catalog in catalogs:
                with metadata.getSchemas(catalog, None) as rs:
                    schemas = [rs.getString(1) for rs in rs] or [None]

                for schema in schemas:
                    if not self._should_include_container(schema):
                        continue

                    # Determine which types to extract
                    table_types = []
                    if self.config.include_tables:
                        table_types.extend(["TABLE", "SYSTEM TABLE", "SYSTEM VIEW"])
                    if self.config.include_views:
                        table_types.append("VIEW")

                    if not table_types:
                        continue

                    # Get tables/views
                    with metadata.getTables(catalog, schema, None, table_types) as rs:
                        for table in rs:
                            try:
                                table_catalog = table.getString(1)
                                table_schema = table.getString(2)
                                table_name = table.getString(3)
                                table_type = table.getString(4)
                                remarks = table.getString(5)

                                if not self._should_include_table(table_schema, table_name, table_type):
                                    continue

                                # Generate basic metadata
                                yield from self._generate_basic_table_metadata(
                                    metadata,
                                    table_catalog,
                                    table_schema,
                                    table_name,
                                    table_type,
                                    remarks
                                )

                                # Generate profiling metadata if enabled
                                if (self.config.is_profiling_enabled() and
                                        table_type in ["TABLE", "SYSTEM TABLE"] and
                                        self.config.profile_pattern.allowed(f"{table_schema}.{table_name}")):
                                    yield from self._generate_profile_metadata(
                                        table_catalog,
                                        table_schema,
                                        table_name
                                    )

                            except Exception as e:
                                self._report_metadata_extraction_error(
                                    f"{table_schema}.{table_name}",
                                    e
                                )

        except Exception as e:
            self.report.report_failure("tables", f"Failed to extract tables: {str(e)}")

    def _generate_basic_table_metadata(
            self,
            metadata,
            catalog: str,
            schema: str,
            table: str,
            table_type: str,
            remarks: Optional[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Generate basic table/view metadata"""
        dataset_name = f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"
        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})"

        # Get columns
        schema_fields = []
        pk_constraints = set()
        foreign_keys = []

        with metadata.getColumns(catalog, schema, table, None) as col_rs:
            for col in col_rs:
                field = SchemaFieldClass(
                    fieldPath=col.getString("COLUMN_NAME"),
                    nativeDataType=col.getString("TYPE_NAME"),
                    type=SchemaFieldDataTypeClass(type=col.getString("TYPE_NAME")),
                    description=col.getString("REMARKS"),
                    nullable=bool(col.getInt("NULLABLE"))
                )
                schema_fields.append(field)

        # Get primary key
        try:
            with metadata.getPrimaryKeys(catalog, schema, table) as pk_rs:
                for pk in pk_rs:
                    pk_constraints.add(pk.getString("COLUMN_NAME"))
        except:
            pass

        # Get foreign keys
        try:
            with metadata.getImportedKeys(catalog, schema, table) as fk_rs:
                for fk in fk_rs:
                    foreign_keys.append({
                        "name": fk.getString("FK_NAME"),
                        "sourceColumn": fk.getString("FKCOLUMN_NAME"),
                        "targetTable": f"{fk.getString('PKTABLE_SCHEM')}.{fk.getString('PKTABLE_NAME')}",
                        "targetColumn": fk.getString("PKCOLUMN_NAME")
                    })
        except:
            pass

        # Generate schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            platformSchema=OtherSchemaClass(""),
            hash="",
            version=0,
            fields=schema_fields,
            primaryKeys=list(pk_constraints) if pk_constraints else None,
            foreignKeys=foreign_keys if foreign_keys else None
        )

        self.sql_aggregator.register_schema(dataset_urn, schema_metadata)

        yield MetadataWorkUnit(
            id=f"{dataset_name}-schema",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                changeType="UPSERT",
                aspectName="schemaMetadata",
                aspect=schema_metadata
            )
        )

        # Generate view properties if applicable
        if table_type == "VIEW" and self.config.parse_view_definitions:
            view_definition = self._get_view_definition(metadata, catalog, schema, table)
            if view_definition:
                self.sql_aggregator.register_view(schema, table, view_definition)
                upstream_tables = self.sql_aggregator.get_upstream_tables(schema, table)



                yield MetadataWorkUnit(
                    id=f"{dataset_name}-view",
                    mcp=MetadataChangeProposalWrapper(
                        entityType="dataset",
                        entityUrn=dataset_urn,
                        changeType="UPSERT",
                        aspectName="viewProperties",
                        aspect=ViewPropertiesClass(
                            materialized=False,
                            viewLogic=view_definition,
                            viewLanguage="SQL",
                            upstreamTables=list(upstream_tables) if upstream_tables else None
                        )
                    )
                )

    def _generate_profile_metadata(
            self,
            catalog: str,
            schema: str,
            table: str
    ) -> Iterable[MetadataWorkUnit]:
        """Generate profiling metadata for a table"""
        try:
            profile_query = self.config.profiling.get_profile_query(
                catalog=catalog,
                schema=schema,
                table=table
            )

            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(profile_query)
                    profile_data = cursor.fetchall()

            # Generate profile MCPs based on results
            # Implementation depends on specific profiling requirements

        except Exception as e:
            self.report.report_failure(
                f"profile-{schema}.{table}",
                f"Failed to generate profile: {str(e)}"
            )

    def _get_view_definition(
            self,
            metadata,
            catalog: str,
            schema: str,
            view_name: str
    ) -> Optional[str]:
        """Get view definition using multiple approaches"""
        try:
            # Try standard JDBC metadata
            view_def = None
            try:
                with metadata.getTables(catalog, schema, view_name, ["VIEW"]) as rs:
                    table = next(rs)
                    view_def = table.getString("VIEW_DEFINITION")
            except:
                pass

            if not view_def:
                # Try querying system tables
                with self._get_connection() as conn:
                    with conn.cursor() as cursor:
                        for query in [
                            f"SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{view_name}'",
                            f"SHOW CREATE VIEW {schema}.{view_name}",
                            f"SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID('{schema}.{view_name}')"
                        ]:
                            try:
                                cursor.execute(query)
                                result = cursor.fetchone()
                                if result and result[0]:
                                    view_def = result[0]
                                    break
                            except:
                                continue

            if view_def:
                return self._clean_view_definition(view_def)

            return None

        except Exception as e:
            self.report.report_failure(
                f"view-definition-{schema}.{view_name}",
                f"Failed to get view definition: {str(e)}"
            )
            return None

    def _clean_view_definition(self, definition: str) -> str:
        """Clean and standardize view definition"""
        if not definition:
            return ""

        # Remove CREATE VIEW statement
        definition = re.sub(
            r'^CREATE(?:\s+OR\s+REPLACE)?\s+VIEW\s+.*?\s+AS\s+',
            '',
            definition,
            flags=re.IGNORECASE | re.MULTILINE
        )

        # Remove trailing semicolon
        definition = definition.rstrip(';')

        # Standardize whitespace
        definition = re.sub(r'\s+', ' ', definition.strip())

        return definition

    def _should_include_container(self, name: str) -> bool:
        """Check if container should be included based on patterns"""
        if not name:
            return True

        return self.config.schema_pattern.allowed(name)

    def _should_include_table(
            self,
            schema: str,
            table: str,
            table_type: str
    ) -> bool:
        """Check if table/view should be included based on patterns"""
        full_name = f"{schema}.{table}"

        # Check schema pattern
        if not self.config.schema_pattern.allowed(schema):
            return False

        # Check dataset pattern
        if not self.config.dataset_pattern.allowed(full_name):
            return False

        # Check table type
        if table_type == "VIEW" and not self.config.include_views:
            return False
        if table_type in ["TABLE", "SYSTEM TABLE"] and not self.config.include_tables:
            return False

        return True

    def _extract_database_metadata(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract database-level metadata if available"""
        try:
            # Get database properties
            props = {
                "productName": metadata.getDatabaseProductName(),
                "productVersion": metadata.getDatabaseProductVersion(),
                "driverName": metadata.getDriverName(),
                "driverVersion": metadata.getDriverVersion(),
            }

            # Create database container if not already created
            database_name = props["productName"].lower()
            container_urn = f"urn:li:container:{self.platform}:{database_name}"

            container_workunits = gen_containers(
                container_key=database_name,
                name=database_name,
                sub_types=["Database"],
                description=f"Version: {props['productVersion']}",
                properties=props
            )

            for wu in container_workunits:
                self.report.report_container_scanned(database_name)
                yield wu

        except Exception as e:
            self.report.report_failure(
                "database-metadata",
                f"Failed to extract database metadata: {str(e)}"
            )

    def _report_metadata_extraction_error(
            self,
            identifier: str,
            error: Exception,
            detailed_traceback: bool = False
    ) -> None:
        """Report metadata extraction error with consistent formatting"""
        error_str = str(error)
        if detailed_traceback:
            error_str = f"{error_str}\n{''.join(traceback.format_tb(error.__traceback__))}"

        logger.error(f"Error extracting metadata for {identifier}: {error_str}")
        self.report.report_failure(identifier, error_str)

    def report_progress(self, workunit: MetadataWorkUnit) -> None:
        """Report ingestion progress"""
        if not hasattr(self, '_progress_counter'):
            self._progress_counter = 0
        self._progress_counter += 1

        if self._progress_counter % 100 == 0:
            logger.info(f"Extracted {self._progress_counter} metadata units")

    def close(self) -> None:
        """Clean up resources"""
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None

def basic_sql_filter(sql: str) -> str:
    """Basic SQL filtering/cleaning for view definitions"""
    # Remove comments
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

    # Normalize whitespace
    sql = re.sub(r'\s+', ' ', sql.strip())

    return sql