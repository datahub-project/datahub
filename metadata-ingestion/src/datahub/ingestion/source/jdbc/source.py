import base64
import hashlib
import logging
import os
import re
import subprocess
import tempfile
import time
import traceback
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import jaydebeapi

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.source.jdbc.config import JDBCSourceConfig, SSLConfig
from datahub.ingestion.source.jdbc.constants import (
    ContainerType,
    ProcedureType,
    TableType,
)
from datahub.ingestion.source.jdbc.containers import (
    ContainerRegistry,
    JDBCContainerKey,
    SchemaContainerBuilder,
    SchemaPath,
)
from datahub.ingestion.source.jdbc.maven_install import MavenManager
from datahub.ingestion.source.jdbc.reporting import JDBCSourceReport
from datahub.ingestion.source.jdbc.types import JDBCColumn, JDBCTable, StoredProcedure
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.schema_classes import (
    ContainerClass,
    OtherSchemaClass,
    SchemaMetadataClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing import sqlglot_utils
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)


@platform_name("jdbc")
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

    The plugin uses Java Database Connectivity (JDBC) APIs through JPype and JayDeBeApi, allowing it to support any database with a JDBC driver. It handles connection pooling, retries, and proper resource cleanup to ensure reliable metadata extraction.

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
        self._connection = None
        self._temp_files: List[str] = []
        self.container_registry = ContainerRegistry()
        self.schema_container_builder = SchemaContainerBuilder(
            self, self.container_registry
        )
        self.set_dialect(self.config.sqlglot_dialect)
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

    def set_dialect(self, custom_dialect: Optional[str]) -> None:
        def custom_sql_dialect(platform: str) -> str:
            return custom_dialect if custom_dialect else "postgres"

        sqlglot_utils._get_dialect_str = custom_sql_dialect

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
            with self._get_connection() as conn:
                metadata = conn.jconn.getMetaData()

                # Extract database container
                yield from self._extract_database_metadata(metadata)

                # Extract schema containers
                yield from self._extract_schema_containers(metadata)

                # Extract tables and views
                yield from self._extract_tables_and_views(metadata)

                # Extract stored procedures if enabled
                if self.config.include_stored_procedures:
                    yield from self._extract_stored_procedures(metadata)

                for wu in self.sql_parsing_aggregator.gen_metadata():
                    self.report.report_workunit(wu.as_workunit())
                    yield wu.as_workunit()

        except Exception as e:
            self.report.report_failure("jdbc-source", f"Extraction failed: {str(e)}")
            logger.error(f"JDBC extraction failed: {str(e)}")
            logger.debug(traceback.format_exc())

    def _get_connection(self) -> jaydebeapi.Connection:
        """Get JDBC connection with retry logic."""
        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                if not self._connection or self._connection.closed:
                    driver_path = self._get_driver_path()
                    url = self.config.connection.uri
                    props = self._get_connection_properties()

                    # Use JVM args from config
                    os.environ["_JAVA_OPTIONS"] = " ".join(self.config.jvm_args)

                    self._connection = jaydebeapi.connect(
                        self.config.driver.driver_class, url, props, driver_path
                    )
                    return self._connection
            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(
                        f"Failed to create connection after {max_retries} attempts: {str(e)}"
                    )
                logger.warning(
                    f"Connection attempt {attempt + 1} failed, retrying in {retry_delay}s: {str(e)}"
                )
                time.sleep(retry_delay)

        raise Exception("Failed to establish connection after all retries")

    def _get_driver_path(self) -> str:
        """Get JDBC driver path."""
        if self.config.driver.driver_path:
            path = os.path.expanduser(self.config.driver.driver_path)
            if not os.path.exists(path):
                raise FileNotFoundError(f"Driver not found at: {path}")
            return path

        if self.config.driver.maven_coordinates:
            try:
                maven = MavenManager()
                if not maven.is_maven_installed:
                    maven.setup_environment()
                return self._download_driver_from_maven(
                    self.config.driver.maven_coordinates
                )
            except Exception as e:
                raise Exception(f"Failed to download driver from Maven: {str(e)}")

        raise ValueError("Either driver_path or maven_coordinates must be specified")

    def _download_driver_from_maven(self, coords: str) -> str:
        """Download driver from Maven."""
        driver_dir = Path.home() / ".datahub" / "drivers"
        driver_dir.mkdir(parents=True, exist_ok=True)

        # Parse Maven coordinates
        try:
            group_id, artifact_id, version = coords.split(":")
        except ValueError:
            raise ValueError(
                f"Invalid Maven coordinates: {coords}. Format should be groupId:artifactId:version"
            )

        # Create hash of coordinates for cache key
        coords_hash = hashlib.sha256(coords.encode()).hexdigest()[:12]
        cache_path = driver_dir / f"driver-{coords_hash}.jar"

        if cache_path.exists():
            logger.info(f"Using cached driver from {cache_path}")
            return str(cache_path)

        # Download driver
        with tempfile.TemporaryDirectory() as temp_dir:
            maven_cmd = [
                "mvn",
                "dependency:copy",
                f"-Dartifact={coords}",
                f"-DoutputDirectory={temp_dir}",
                "-Dmdep.stripVersion=true",
                "-q",
            ]

            try:
                logger.info(f"Downloading driver for {coords}")
                subprocess.run(maven_cmd, check=True, capture_output=True, text=True)

                downloaded_path = Path(temp_dir) / f"{artifact_id}.jar"
                if not downloaded_path.exists():
                    raise FileNotFoundError(
                        f"Maven download succeeded but file not found at {downloaded_path}"
                    )

                # Copy to cache location
                import shutil

                shutil.copy2(downloaded_path, cache_path)
                logger.info(f"Driver downloaded and cached at {cache_path}")

                return str(cache_path)

            except subprocess.CalledProcessError as e:
                error_msg = e.stderr if e.stderr else e.stdout
                raise Exception(f"Maven download failed: {error_msg}")

    def _get_connection_properties(self) -> Dict[str, str]:
        """Get connection properties."""
        props = dict(self.config.connection.properties)

        if self.config.connection.username:
            props["user"] = self.config.connection.username
        if self.config.connection.password:
            props["password"] = self.config.connection.password

        if self.config.connection.ssl_config:
            props.update(self._get_ssl_properties(self.config.connection.ssl_config))

        return props

    def _get_ssl_properties(self, ssl_config: SSLConfig) -> Dict[str, str]:
        """Get SSL properties."""
        props = {"ssl": "true"}

        if ssl_config.cert_path:
            props["sslcert"] = ssl_config.cert_path
        elif ssl_config.cert_content:
            try:
                cert_content = base64.b64decode(ssl_config.cert_content)
                fd, temp_path = tempfile.mkstemp(suffix=f".{ssl_config.cert_type}")
                self._temp_files.append(temp_path)
                with os.fdopen(fd, "wb") as f:
                    f.write(cert_content)
                props["sslcert"] = temp_path
            except Exception as e:
                raise Exception(f"Failed to create SSL certificate file: {str(e)}")

        if ssl_config.cert_password:
            props["sslpassword"] = ssl_config.cert_password

        return props

    def _get_database_name(
        self, metadata: jaydebeapi.Connection.cursor
    ) -> Optional[str]:
        """Extract database name from connection metadata."""
        try:
            database = metadata.getConnection().getCatalog()
            if not database:
                url = self.config.connection.uri
                match = re.search(r"jdbc:[^:]+://[^/]+/([^?;]+)", url)
                if match:
                    database = match.group(1)
            return database if database else None
        except Exception:
            return None

    def _extract_database_metadata(
        self, metadata: jaydebeapi.Connection.cursor
    ) -> Iterable[MetadataWorkUnit]:
        """Extract database container metadata."""
        try:
            database_name = self._get_database_name(metadata)
            if not database_name:
                return

            # Create container for database
            container = self.schema_container_builder.build_container(
                SchemaPath([database_name]), ContainerType.DATABASE
            )

            # Add additional database properties
            container.custom_properties.update(
                {
                    "productName": metadata.getDatabaseProductName(),
                    "productVersion": metadata.getDatabaseProductVersion(),
                    "driverName": metadata.getDriverName(),
                    "driverVersion": metadata.getDriverVersion(),
                    "url": self.config.connection.uri,
                    "maxConnections": str(metadata.getMaxConnections()),
                    "supportsBatchUpdates": str(metadata.supportsBatchUpdates()),
                    "supportsTransactions": str(metadata.supportsTransactions()),
                    "defaultTransactionIsolation": str(
                        metadata.getDefaultTransactionIsolation()
                    ),
                }
            )

            # Register and emit container
            if not self.container_registry.has_container(
                container.container_key.as_urn()
            ):
                self.container_registry.register_container(container)
                yield from container.generate_workunits()

        except Exception as e:
            self.report.report_failure(
                "database-metadata", f"Failed to extract database metadata: {str(e)}"
            )
            logger.error(f"Failed to extract database metadata: {str(e)}")
            logger.debug(traceback.format_exc())

    def get_container_key(
        self, name: Optional[str], path: Optional[List[str]]
    ) -> JDBCContainerKey:
        """Get container key with proper None handling"""
        if not name:
            raise ValueError("Container name cannot be None")

        # Construct key
        if path:
            # Filter out None values from path
            valid_path = [p for p in path if p is not None]
            if valid_path:
                key = f"{'.'.join(valid_path)}.{name}"
            else:
                key = name
        else:
            key = name

        return JDBCContainerKey(
            platform=make_data_platform_urn(self.platform),
            instance=self.platform_instance,
            env=self.env,
            key=key,
        )

    def _extract_schema_containers(
        self, metadata: jaydebeapi.Connection.cursor
    ) -> Iterable[MetadataWorkUnit]:
        """Extract schema containers."""
        try:
            with metadata.getSchemas() as rs:
                # Collect and filter schemas
                schemas = []
                while rs.next():
                    schema_name = rs.getString(1)
                    if self.config.schema_pattern.allowed(schema_name):
                        schemas.append(schema_name)
                    else:
                        self.report.report_dropped(f"Schema: {schema_name}")

            database_name = self._get_database_name(metadata)

            # Process all schemas
            for schema_name in sorted(schemas):
                try:
                    schema_path = SchemaPath.from_schema_name(
                        schema_name, database_name
                    )

                    # Process each container path
                    for container_path in sorted(schema_path.get_container_paths()):
                        current_path = SchemaPath.from_schema_name(
                            container_path, database_name
                        )

                        # Determine container type
                        container_type = (
                            ContainerType.SCHEMA
                            if len(current_path.parts) == 1
                            else ContainerType.FOLDER
                        )

                        # Build and register container
                        container = self.schema_container_builder.build_container(
                            current_path, container_type
                        )

                        # Only emit if not already processed
                        if not self.container_registry.has_container(
                            container.container_key.as_urn()
                        ):
                            self.container_registry.register_container(container)
                            yield from container.generate_workunits()

                except Exception as e:
                    self.report.report_failure(
                        f"schema-{schema_name}",
                        f"Failed to process schema: {str(e)}",
                    )

        except Exception as e:
            self.report.report_failure(
                "schemas", f"Failed to extract schemas: {str(e)}"
            )
            logger.error(f"Failed to extract schemas: {str(e)}")
            logger.debug(traceback.format_exc())

    def _extract_tables_and_views(
        self, metadata: jaydebeapi.Connection.cursor
    ) -> Iterable[MetadataWorkUnit]:
        """Extract tables and views with batch metadata retrieval."""
        try:
            database_name = self._get_database_name(metadata)

            with metadata.getSchemas() as schema_rs:
                while schema_rs.next():
                    schema_name = schema_rs.getString(1)

                    if not self.config.schema_pattern.allowed(schema_name):
                        continue

                    # Determine which types to extract
                    table_types = []
                    if self.config.include_tables:
                        table_types.append(TableType.TABLE.value)
                    if self.config.include_views:
                        table_types.append(TableType.VIEW.value)

                    if not table_types:
                        continue

                    logger.info(f"Pre-fetching metadata for schema {schema_name}")
                    # Pre-fetch metadata for the entire schema
                    columns_by_table = self._batch_extract_columns(
                        metadata=metadata, schema=schema_name
                    )
                    pk_by_table = self._batch_extract_primary_keys(
                        metadata=metadata, schema=schema_name
                    )
                    fk_by_table = self._batch_extract_foreign_keys(
                        metadata=metadata, schema=schema_name
                    )
                    view_definitions = (
                        self._batch_extract_view_definitions(schema_name)
                        if TableType.VIEW.value in table_types
                        else {}
                    )

                    # Process tables using pre-fetched metadata
                    with metadata.getTables(
                        None, schema_name, None, table_types
                    ) as table_rs:
                        while table_rs.next():
                            try:
                                table_name = table_rs.getString(3)
                                table_type = table_rs.getString(4)
                                remarks = table_rs.getString(5)

                                # Apply filtering before more expensive operations
                                full_name = f"{schema_name}.{table_name}"
                                if table_type == TableType.TABLE.value:
                                    if not self.config.table_pattern.allowed(full_name):
                                        self.report.report_dropped(full_name)
                                        continue
                                else:  # VIEW
                                    if not self.config.view_pattern.allowed(full_name):
                                        self.report.report_dropped(f"View: {full_name}")
                                        continue

                                # Use pre-fetched metadata
                                columns = columns_by_table.get(table_name, [])
                                pk_columns = pk_by_table.get(table_name, set())

                                # Extract foreign keys for tables only
                                table_foreign_keys = []
                                if table_type == TableType.TABLE.value:
                                    for fk in fk_by_table.get(table_name, []):
                                        table_foreign_keys.append(
                                            JDBCTable.create_foreign_key_constraint(
                                                name=fk["name"],
                                                source_column=fk["sourceColumn"],
                                                target_schema=fk["targetSchema"],
                                                target_table=fk["targetTable"],
                                                target_column=fk["targetColumn"],
                                                platform=self.platform,
                                                platform_instance=self.platform_instance,
                                                env=self.env,
                                            )
                                        )

                                table = JDBCTable(
                                    name=table_name,
                                    schema=schema_name,
                                    type=table_type,
                                    remarks=remarks,
                                    columns=columns,
                                    pk_columns=pk_columns,
                                    foreign_keys=table_foreign_keys,
                                )

                                # Use pre-fetched view definition
                                view_definition = (
                                    view_definitions.get(table_name)
                                    if table_type == TableType.VIEW.value
                                    else None
                                )

                                yield from self._generate_table_metadata(
                                    table=table,
                                    database=database_name,
                                    view_definition=view_definition,
                                )

                                # Report success
                                if table_type == TableType.TABLE.value:
                                    self.report.report_table_scanned(full_name)
                                else:
                                    self.report.report_view_scanned(full_name)

                            except Exception as e:
                                self.report.report_failure(
                                    f"table-{schema_name}.{table_name}",
                                    f"Failed to extract table: {str(e)}",
                                )

        except Exception as e:
            self.report.report_failure(
                "tables-and-views", f"Failed to extract tables and views: {str(e)}"
            )
            logger.error(f"Failed to extract tables and views: {str(e)}")
            logger.debug(traceback.format_exc())

    def _batch_extract_columns(
        self, metadata: jaydebeapi.Connection.cursor, schema: str
    ) -> Dict[str, List[JDBCColumn]]:
        """Extract columns for all tables in a schema at once."""
        columns_by_table: Dict[str, List[JDBCColumn]] = {}
        try:
            with metadata.getColumns(None, schema, None, None) as rs:
                while rs.next():
                    table_name = rs.getString("TABLE_NAME")
                    column = JDBCColumn(
                        name=rs.getString("COLUMN_NAME"),
                        type_name=rs.getString("TYPE_NAME").upper(),
                        nullable=rs.getBoolean("NULLABLE"),
                        remarks=rs.getString("REMARKS"),
                        column_size=rs.getInt("COLUMN_SIZE"),
                        decimal_digits=rs.getInt("DECIMAL_DIGITS"),
                    )
                    if table_name not in columns_by_table:
                        columns_by_table[table_name] = []
                    columns_by_table[table_name].append(column)
        except Exception as e:
            logger.debug(f"Could not get columns for schema {schema}: {e}")
        return columns_by_table

    def _batch_extract_primary_keys(
        self, metadata: jaydebeapi.Connection.cursor, schema: str
    ) -> Dict[str, Set[str]]:
        """Extract primary keys for all tables in a schema at once."""
        pks_by_table: Dict[str, Set[str]] = {}
        try:
            with metadata.getPrimaryKeys(None, schema, None) as rs:
                while rs.next():
                    table_name = rs.getString("TABLE_NAME")
                    if table_name not in pks_by_table:
                        pks_by_table[table_name] = set()
                    pks_by_table[table_name].add(rs.getString("COLUMN_NAME"))
        except Exception as e:
            logger.debug(f"Could not get primary keys for schema {schema}: {e}")
        return pks_by_table

    def _batch_extract_foreign_keys(
        self, metadata: jaydebeapi.Connection.cursor, schema: str
    ) -> Dict[str, List[Dict]]:
        """Extract foreign keys for all tables in a schema at once."""
        fks_by_table: Dict[str, List[Dict[str, str]]] = {}
        try:
            with metadata.getExportedKeys(None, schema, None) as rs:
                while rs.next():
                    table_name = rs.getString("FKTABLE_NAME")
                    if table_name not in fks_by_table:
                        fks_by_table[table_name] = []
                    fks_by_table[table_name].append(
                        {
                            "name": rs.getString("FK_NAME"),
                            "sourceColumn": rs.getString("FKCOLUMN_NAME"),
                            "targetSchema": rs.getString("PKTABLE_SCHEM"),
                            "targetTable": rs.getString("PKTABLE_NAME"),
                            "targetColumn": rs.getString("PKCOLUMN_NAME"),
                        }
                    )
        except Exception as e:
            logger.debug(f"Could not get foreign keys for schema {schema}: {e}")
        return fks_by_table

    def _try_query_for_views(
        self, connection: Optional[jaydebeapi.Connection], query: str
    ) -> Dict[str, str]:
        """Try to execute a single view definition query."""
        view_definitions: Dict[str, str] = {}

        if connection is not None:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    if rows:
                        for row in rows:
                            if row[0] and row[1]:  # table_name and definition
                                view_definitions[row[0]] = self._clean_sql(row[1])
            except Exception:
                pass
        return view_definitions

    def _get_info_schema_view_definitions(self, schema: str) -> Dict[str, str]:
        """Get view definitions from information schema."""
        if self._connection is None:
            return {}

        info_schema_queries = [
            f"SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema}'",
            f"SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE SCHEMA_NAME = '{schema}'",
            f"SELECT VIEW_NAME as TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE VIEW_SCHEMA = '{schema}'",
        ]

        for query in info_schema_queries:
            try:
                with self._connection.cursor() as cursor:
                    view_definitions = self._try_query_for_views(cursor, query)
                    if view_definitions:
                        return view_definitions
            except Exception:
                continue
        return {}

    def _get_system_view_definitions(self, schema: str) -> Dict[str, str]:
        """Get view definitions from system views."""
        if self._connection is None:
            return {}

        system_queries = [
            f"""
            SELECT
                OBJECT_NAME(object_id) as TABLE_NAME,
                definition as VIEW_DEFINITION
            FROM sys.sql_modules
            WHERE object_id IN (
                SELECT object_id FROM sys.objects
                WHERE SCHEMA_NAME(schema_id) = '{schema}'
            )
            """,
            f"SELECT VIEW_NAME as TABLE_NAME, TEXT as VIEW_DEFINITION FROM ALL_VIEWS WHERE OWNER = '{schema}'",
        ]

        for query in system_queries:
            try:
                with self._connection.cursor() as cursor:
                    view_definitions = self._try_query_for_views(cursor, query)
                    if view_definitions:
                        return view_definitions
            except Exception:
                continue
        return {}

    def _get_view_names(self, schema: str) -> Set[str]:
        """Get list of view names for a schema."""
        views: Set[str] = set()
        if self._connection is not None:
            try:
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema}'"
                    )
                    rows = cursor.fetchall()
                    if rows:
                        views.update(row[0] for row in rows if row[0])
            except Exception:
                pass
        return views

    def _get_view_definition(self, schema: str, view: str) -> Optional[str]:
        """Get view definition for a single view."""
        queries = [
            # Standard information schema query
            f"SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{view}'",
            # SQL Server specific
            f"""
            SELECT definition
            FROM sys.sql_modules m
            INNER JOIN sys.objects o ON m.object_id = o.object_id
            WHERE o.type = 'V'
            AND SCHEMA_NAME(o.schema_id) = '{schema}'
            AND o.name = '{view}'
            """,
            # Oracle style
            f"SELECT TEXT FROM ALL_VIEWS WHERE OWNER = '{schema}' AND VIEW_NAME = '{view}'",
        ]

        if self._connection is not None:
            for query in queries:
                try:
                    with self._connection.cursor() as cursor:
                        cursor.execute(query)
                        row = cursor.fetchone()
                        if row and row[0]:
                            return self._clean_sql(row[0])
                except Exception:
                    continue

        return None

    def _get_fallback_view_definitions(self, schema: str) -> Dict[str, str]:
        """Get view definitions using fallback method."""
        view_definitions = {}
        views = self._get_view_names(schema)

        for view in views:
            try:
                definition = self._get_view_definition(schema, view)
                if definition:
                    view_definitions[view] = definition
            except Exception:
                pass
        return view_definitions

    def _batch_extract_view_definitions(self, schema: str) -> Dict[str, str]:
        """Extract view definitions for all views in a schema at once."""
        try:
            # Try information schema first
            view_definitions = self._get_info_schema_view_definitions(schema)
            if view_definitions:
                return view_definitions

            # Try system views
            view_definitions = self._get_system_view_definitions(schema)
            if view_definitions:
                return view_definitions

            # Fallback to individual queries
            logger.debug(f"Falling back to individual view queries for schema {schema}")
            return self._get_fallback_view_definitions(schema)

        except Exception as e:
            logger.debug(f"Could not get view definitions for schema {schema}: {e}")
            return {}

    def _generate_table_metadata(
        self,
        table: JDBCTable,
        database: Optional[str] = None,
        view_definition: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Generate metadata workunits for a table or view."""
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform, table.full_name, self.platform_instance, self.env
        )

        # Convert columns to schema fields
        fields = [col.to_schema_field() for col in table.columns]

        # Create schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName=table.full_name,
            platform=make_data_platform_urn(self.platform),
            platformSchema=OtherSchemaClass(rawSchema=self._get_raw_schema_sql(table)),
            fields=fields,
            primaryKeys=list(table.pk_columns) if table.pk_columns else None,
            foreignKeys=table.foreign_keys if table.foreign_keys else None,
            version=0,
            hash="",
        )

        # Emit schema metadata
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

        self.sql_parsing_aggregator.register_schema(
            urn=dataset_urn,
            schema=schema_metadata,
        )

        # Dataset properties
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetProperties(
                name=table.name,
                description=table.remarks,
                qualifiedName=f"{database}.{table.full_name}"
                if database
                else table.full_name,
            ),
        ).as_workunit()

        # Subtype
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[table.type.title()]),
        ).as_workunit()

        # For views, add view properties
        if table.type == TableType.VIEW.value and view_definition:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ViewPropertiesClass(
                    materialized=False,
                    viewLogic=view_definition,
                    viewLanguage="SQL",
                ),
            ).as_workunit()

            self.sql_parsing_aggregator.add_view_definition(
                view_urn=dataset_urn,
                view_definition=view_definition,
                default_schema=table.schema,
                default_db=database,
            )

        # Add dataset to container
        schema_container_metadata = self.schema_container_builder.build_container(
            SchemaPath.from_schema_name(table.schema, database),
            ContainerType.SCHEMA,
        )
        container_urn = schema_container_metadata.container_key.as_urn()

        # Register if not already registered
        if not self.container_registry.has_container(container_urn):
            self.container_registry.register_container(schema_container_metadata)

        schema_container = self.container_registry.get_container(container_urn)
        if schema_container:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ContainerClass(
                    container=schema_container.container_key.as_urn()
                ),
            ).as_workunit()

    def _get_raw_schema_sql(self, table: JDBCTable) -> str:
        """Get raw DDL schema for table/view."""
        if self._connection is None:
            return ""

        try:
            ddl_queries = [
                f'SHOW CREATE TABLE "{table.schema}"."{table.name}"',
                f"SELECT DDL FROM ALL_OBJECTS WHERE OWNER = '{table.schema}' AND OBJECT_NAME = '{table.name}'",
            ]

            for query in ddl_queries:
                try:
                    with self._connection.cursor() as cursor:
                        cursor.execute(query)
                        row = cursor.fetchone()
                        if row and row[0]:
                            return self._clean_sql(row[0])
                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"Could not get raw schema for {table.full_name}: {e}")

        return ""

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL string."""
        if not sql:
            return ""

        # Remove comments
        sql = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

        # Normalize whitespace
        sql = re.sub(r"\s+", " ", sql.strip())

        return sql

    def _extract_stored_procedures(
        self, metadata: jaydebeapi.Connection.cursor
    ) -> Iterable[MetadataWorkUnit]:
        """Extract stored procedures metadata."""
        if not self.config.include_stored_procedures:
            return

        try:
            database_name = self._get_database_name(metadata)

            with metadata.getProcedures(None, None, None) as proc_rs:
                while proc_rs.next():
                    try:
                        proc = StoredProcedure(
                            name=proc_rs.getString(3),
                            schema=proc_rs.getString(2),
                            remarks=proc_rs.getString(7),
                            proc_type=proc_rs.getShort(8),
                        )

                        if not self.config.schema_pattern.allowed(proc.schema):
                            continue

                        # Create container for stored procedure
                        container = self.schema_container_builder.build_container(
                            SchemaPath([proc.schema, proc.name], database_name),
                            ContainerType.STORED_PROCEDURE,
                        )

                        # Add procedure-specific properties
                        container.custom_properties.update(
                            {
                                "type": ProcedureType.from_value(proc.proc_type).name,
                                "language": proc.language,
                                "remarks": proc.remarks or "",
                            }
                        )

                        # Only emit if not already processed
                        if not self.container_registry.has_container(
                            container.container_key.as_urn()
                        ):
                            self.container_registry.register_container(container)
                            yield from container.generate_workunits()

                    except Exception as e:
                        self.report.report_failure(
                            f"proc-{proc.schema}.{proc.name}",
                            f"Failed to extract stored procedure: {str(e)}",
                        )

        except Exception as e:
            self.report.report_failure(
                "stored-procedures", f"Failed to extract stored procedures: {str(e)}"
            )
            logger.error(f"Failed to extract stored procedures: {str(e)}")
            logger.debug(traceback.format_exc())

    def get_report(self):
        return self.report

    def close(self):
        """Clean up resources."""
        if self._connection:
            try:
                self._connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")
            self._connection = None

        # Clean up temporary files
        for temp_file in self._temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                logger.warning(f"Error removing temporary file {temp_file}: {str(e)}")
        self._temp_files.clear()
