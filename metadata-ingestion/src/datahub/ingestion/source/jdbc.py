import base64
import hashlib
import logging
import os
import re
import subprocess
import tempfile
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import jaydebeapi
from pydantic import Field, validator
from sqlglot import dialects

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, add_dataset_to_container
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DateTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    TimeTypeClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing import sqlglot_utils
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)

# Extended JDBC type mapping with more types
JDBC_TYPE_MAP = {
    # Standard types
    "CHAR": StringTypeClass,
    "VARCHAR": StringTypeClass,
    "LONGVARCHAR": StringTypeClass,
    "NCHAR": StringTypeClass,
    "NVARCHAR": StringTypeClass,
    "LONGNVARCHAR": StringTypeClass,
    "NUMERIC": NumberTypeClass,
    "DECIMAL": NumberTypeClass,
    "BIT": BooleanTypeClass,
    "BOOLEAN": BooleanTypeClass,
    "TINYINT": NumberTypeClass,
    "SMALLINT": NumberTypeClass,
    "INTEGER": NumberTypeClass,
    "BIGINT": NumberTypeClass,
    "REAL": NumberTypeClass,
    "FLOAT": NumberTypeClass,
    "DOUBLE": NumberTypeClass,
    "BINARY": BytesTypeClass,
    "VARBINARY": BytesTypeClass,
    "LONGVARBINARY": BytesTypeClass,
    "DATE": DateTypeClass,
    "TIME": TimeTypeClass,
    "TIMESTAMP": DateTypeClass,
    # Additional types
    "CLOB": StringTypeClass,
    "NCLOB": StringTypeClass,
    "BLOB": BytesTypeClass,
    "XML": StringTypeClass,
    "JSON": StringTypeClass,
    "ARRAY": StringTypeClass,
    "STRUCT": StringTypeClass,
    "INTERVAL": StringTypeClass,
}


class JDBCContainerKey(ContainerKey):
    """Container key for JDBC entities"""

    key: str


@dataclass
class ViewDefinitionMethod:
    """Represents a successful method for retrieving view definitions"""

    method_type: str  # 'table_metadata' or 'information_schema'
    column_index: Optional[int] = None
    column_name: Optional[str] = None
    query_pattern: Optional[str] = None


@dataclass
class RawSchemaMethod:
    """Represents a successful method for retrieving raw schema DDL"""

    query_pattern: str


class SSLConfig(ConfigModel):
    """SSL Certificate configuration"""

    cert_path: Optional[str] = Field(
        default=None,
        description="Path to SSL certificate file",
    )
    cert_content: Optional[str] = Field(
        default=None,
        description="Base64 encoded certificate content",
    )
    cert_type: str = Field(
        default="pem",
        description="Certificate type (pem, jks, p12)",
    )
    cert_password: Optional[str] = Field(
        default=None,
        description="Certificate password if required",
    )

    @validator("cert_type")
    def validate_cert_type(cls, v: str) -> str:
        valid_types = ["pem", "jks", "p12"]
        if v.lower() not in valid_types:
            raise ValueError(f"cert_type must be one of: {', '.join(valid_types)}")
        return v.lower()


class JDBCConnectionConfig(ConfigModel):
    """JDBC Connection configuration"""

    uri: str = Field(
        description="JDBC URI (jdbc:protocol://host:port/database)",
    )
    username: Optional[str] = Field(
        default=None,
        description="Database username",
    )
    password: Optional[str] = Field(
        default=None,
        description="Database password",
    )
    properties: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional JDBC properties",
    )
    ssl_config: Optional[SSLConfig] = Field(
        default=None,
        description="SSL configuration",
    )

    @validator("uri")
    def validate_uri(cls, v: str) -> str:
        if not v.startswith("jdbc:"):
            raise ValueError("URI must start with 'jdbc:'")
        return v


class JDBCDriverConfig(ConfigModel):
    """JDBC Driver configuration"""

    driver_class: str = Field(
        description="Fully qualified JDBC driver class name",
    )
    driver_path: Optional[str] = Field(
        default=None,
        description="Path to JDBC driver JAR",
    )
    maven_coordinates: Optional[str] = Field(
        default=None,
        description="Maven coordinates (groupId:artifactId:version)",
    )

    @validator("driver_class")
    def validate_driver_class(cls, v: str) -> str:
        if not v:
            raise ValueError("driver_class must be specified")
        return v

    @validator("maven_coordinates")
    def validate_maven_coordinates(cls, v: Optional[str]) -> Optional[str]:
        if v and not re.match(r"^[^:]+:[^:]+:[^:]+$", v):
            raise ValueError(
                "maven_coordinates must be in format 'groupId:artifactId:version'"
            )
        return v


class JDBCSourceConfig(
    StatefulIngestionConfigBase,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
):
    """Configuration for JDBC metadata extraction"""

    driver: JDBCDriverConfig = Field(
        description="JDBC driver configuration",
    )

    connection: JDBCConnectionConfig = Field(
        description="Database connection configuration",
    )

    platform: str = Field(
        description="Name of platform being ingested, used in constructing URNs.",
    )

    include_tables: bool = Field(
        default=True,
        description="Include tables in extraction",
    )

    include_views: bool = Field(
        default=True,
        description="Include views in extraction",
    )

    include_stored_procedures: bool = Field(
        default=False,
        description="Include stored procedures in extraction",
    )

    sqlglot_dialect: Optional[str] = Field(
        default=None,
        description="sqlglot dialect to use for SQL transpiling",
    )

    jvm_args: List[str] = Field(
        default=[],
        description="""JVM arguments for JDBC driver
        E.g. '-Xmx1g',
        '--add-opens=java.base/java.nio=ALL-UNNAMED',
        '--add-opens=java.base/java.lang=ALL-UNNAMED',
        '--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED'""",
    )

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas",
    )

    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables",
    )

    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views",
    )

    profile_table_size: bool = Field(
        default=False,
        description="Include table size metrics in profiling",
    )

    profile_table_row_counts: bool = Field(
        default=True,
        description="Include table row counts in profiling",
    )

    usage: BaseUsageConfig = Field(
        description="Usage statistics configuration",
        default=BaseUsageConfig(),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("sqlglot_dialect")
    def validate_dialect(cls, v):
        if v is None:
            return v

        # Get available dialects (excluding private attributes)
        valid_dialects = [d for d in dir(dialects) if not d.startswith("_")]

        if v not in valid_dialects:
            raise ValueError(
                f"Invalid dialect '{v}'. Must be one of: {', '.join(sorted(valid_dialects))}"
            )
        return v


@dataclass
class JDBCSourceReport(
    SQLSourceReport, StaleEntityRemovalSourceReport, IngestionStageReport
):
    """Report for JDBC source ingestion"""

    tables_scanned: int = 0
    views_scanned: int = 0
    stored_procedures_scanned: int = 0
    filtered_schemas: int = 0
    filtered_tables: int = 0
    filtered_views: int = 0
    filtered_stored_procedures: int = 0

    def report_table_scanned(self, table: str) -> None:
        super().report_entity_scanned(table)
        self.tables_scanned += 1

    def report_view_scanned(self, view: str) -> None:
        super().report_entity_scanned(view)
        self.views_scanned += 1

    def report_stored_procedure_scanned(self, proc: str) -> None:
        super().report_entity_scanned(proc)
        self.stored_procedures_scanned += 1

    def report_schema_filtered(self, schema: str) -> None:
        self.filtered_schemas += 1
        self.report_dropped(f"Schema: {schema}")

    def report_table_filtered(self, table: str) -> None:
        self.filtered_tables += 1
        self.report_dropped(f"Table: {table}")

    def report_view_filtered(self, view: str) -> None:
        self.filtered_views += 1
        self.report_dropped(f"View: {view}")

    def report_stored_procedure_filtered(self, proc: str) -> None:
        self.filtered_stored_procedures += 1
        self.report_dropped(f"Stored Procedure: {proc}")

    def set_ingestion_stage(self, dataset: str, stage: str) -> None:
        self.report_ingestion_stage_start(f"{dataset}: {stage}")


class JDBCSource(StatefulIngestionSourceBase):
    """Generic JDBC Source"""

    config: JDBCSourceConfig
    report: JDBCSourceReport

    def __init__(self, config: JDBCSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.platform = self.config.platform
        self.platform_instance = self.config.platform_instance
        self.env = self.config.env
        self.report = JDBCSourceReport()
        self._connection = None
        self._temp_files: List[str] = []
        self.set_dialect(self.config.sqlglot_dialect)
        self._view_definition_method: Optional[ViewDefinitionMethod] = None
        self.sql_parsing_aggregator = SqlParsingAggregator(
            platform=make_data_platform_urn(self.platform),
            platform_instance=self.platform_instance,
            env=self.config.env,
            graph=ctx.graph,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=self.config.usage,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
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

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "JDBCSource":
        config = JDBCSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.get_workunits_internal()

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

    def set_dialect(self, custom_dialect: Optional[str]) -> None:
        def custom_sql_dialect(platform: str) -> str:
            return custom_dialect if custom_dialect else "generic"

        sqlglot_utils._get_dialect_str = custom_sql_dialect

    def _get_connection(self) -> jaydebeapi.Connection:
        """Get JDBC connection with retry logic"""
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
        """Get JDBC driver path with improved error handling"""
        if self.config.driver.driver_path:
            path = os.path.expanduser(self.config.driver.driver_path)
            if not os.path.exists(path):
                raise FileNotFoundError(f"Driver not found at: {path}")
            return path

        if self.config.driver.maven_coordinates:
            try:
                return self._download_driver_from_maven()
            except Exception as e:
                raise Exception(f"Failed to download driver from Maven: {str(e)}")

        raise ValueError("Either driver_path or maven_coordinates must be specified")

    def _download_driver_from_maven(self) -> str:
        """Download driver from Maven with improved error handling and path management"""
        coords = self.config.driver.maven_coordinates
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

        # Create a temporary directory for download
        with tempfile.TemporaryDirectory() as temp_dir:
            maven_cmd = [
                "mvn",
                "dependency:copy",
                f"-Dartifact={coords}",
                f"-DoutputDirectory={temp_dir}",
                "-Dmdep.stripVersion=true",
                "-q",  # Quiet mode
            ]

            try:
                logger.info(f"Downloading driver for {coords}")
                result = subprocess.run(
                    maven_cmd, check=True, capture_output=True, text=True
                )

                # The file will be named artifactId.jar due to stripVersion=true
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
            except Exception as e:
                raise Exception(f"Failed to download driver: {str(e)}")

    def _get_connection_properties(self) -> Dict[str, str]:
        """Get connection properties with improved SSL handling"""
        props = dict(self.config.connection.properties)

        if self.config.connection.username:
            props["user"] = self.config.connection.username
        if self.config.connection.password:
            props["password"] = self.config.connection.password

        if self.config.connection.ssl_config:
            props.update(self._get_ssl_properties())

        return props

    def _get_ssl_properties(self) -> Dict[str, str]:
        """Get SSL properties with temporary file cleanup"""
        ssl_config = self.config.connection.ssl_config
        props = {"ssl": "true"}

        if ssl_config.cert_path:
            props["sslcert"] = ssl_config.cert_path
        elif ssl_config.cert_content:
            try:
                cert_content = base64.b64decode(ssl_config.cert_content)
                fd, temp_path = tempfile.mkstemp(suffix=f".{ssl_config.cert_type}")
                self._temp_files.append(temp_path)  # Track for cleanup
                with os.fdopen(fd, "wb") as f:
                    f.write(cert_content)
                props["sslcert"] = temp_path
            except Exception as e:
                raise Exception(f"Failed to create SSL certificate file: {str(e)}")

        if ssl_config.cert_password:
            props["sslpassword"] = ssl_config.cert_password

        return props

    def _get_database_name(self, metadata) -> Optional[str]:
        """Extract database name from connection metadata"""
        try:
            database = metadata.getConnection().getCatalog()
            if not database:
                url = self.config.connection.uri
                match = re.search(r"jdbc:[^:]+://[^/]+/([^?;]+)", url)
                if match:
                    database = match.group(1)
            # Return None if no database found
            return database if database else None
        except Exception:
            return None

    def _extract_database_metadata(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract database container with improved metadata"""
        try:
            database_name = self._get_database_name(metadata)

            # Skip database container creation if no valid name
            if not database_name:
                return

            props = {
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

            # Generate database container
            container_key = self.get_container_key(database_name, None)

            yield MetadataChangeProposalWrapper(
                entityUrn=container_key.as_urn(),
                aspect=ContainerPropertiesClass(
                    name=database_name,
                    customProperties=props,
                    description=f"Database {database_name}",
                ),
            ).as_workunit()

            # Add subtype
            yield MetadataChangeProposalWrapper(
                entityUrn=container_key.as_urn(),
                aspect=SubTypesClass(typeNames=["Database"]),
            ).as_workunit()

            # Add status
            yield MetadataChangeProposalWrapper(
                entityUrn=container_key.as_urn(),
                aspect=StatusClass(removed=False)
            ).as_workunit()

            # Add platform instance
            yield MetadataChangeProposalWrapper(
                entityUrn=container_key.as_urn(),
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform)
                ),
            ).as_workunit()

        except Exception as e:
            self.report.report_failure(
                "database-metadata", f"Failed to extract database metadata: {str(e)}"
            )
            logger.error(f"Failed to extract database metadata: {str(e)}")
            logger.debug(traceback.format_exc())

    def _extract_schema_containers(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract schema containers with proper container hierarchy"""
        try:
            with metadata.getSchemas() as rs:
                # First collect all schemas to ensure we create containers in the right order
                schemas = []
                while rs.next():
                    schema_name = rs.getString(1)
                    if self.config.schema_pattern.allowed(schema_name):
                        schemas.append(schema_name)
                    else:
                        self.report.report_schema_filtered(schema_name)

                # Process schemas in order to ensure parent containers are created before children
                for schema_name in sorted(schemas):  # Sorting ensures consistent order
                    try:
                        # Split schema on dots to handle nested paths
                        schema_parts = schema_name.split('.')

                        # Keep track of the full path as we build it
                        current_path = []
                        current_container_key = None

                        # Create container for each path segment
                        for part in schema_parts:
                            current_path.append(part)

                            # Create container for this level
                            container_key = self.get_container_key(
                                part,
                                current_path[:-1] if len(current_path) > 1 else None
                            )
                            logger.error(container_key)

                            # Link to parent container if we have one
                            if current_container_key:
                                yield MetadataChangeProposalWrapper(
                                    entityUrn=container_key.as_urn(),
                                    aspect=ContainerClass(
                                        container=current_container_key.as_urn()
                                    ),
                                ).as_workunit()

                            # Container properties
                            yield MetadataChangeProposalWrapper(
                                entityUrn=container_key.as_urn(),
                                aspect=ContainerPropertiesClass(
                                    name=part,
                                    description=f"Schema {'.'.join(current_path)}",
                                    customProperties={
                                        "full_path": '.'.join(current_path)
                                    },
                                ),
                            ).as_workunit()

                            # Add subtype
                            yield MetadataChangeProposalWrapper(
                                entityUrn=container_key.as_urn(),
                                aspect=SubTypesClass(typeNames=[
                                    "Schema"
                                    if current_container_key is None
                                    else "Folder"
                                ]),
                            ).as_workunit()

                            # Add status
                            yield MetadataChangeProposalWrapper(
                                entityUrn=container_key.as_urn(),
                                aspect=StatusClass(removed=False),
                            ).as_workunit()

                            # Update current container key for next iteration
                            current_container_key = container_key

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

    def _extract_tables_and_views(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract tables and views with improved metadata handling"""
        try:
            database_name = self._get_database_name(metadata)

            with metadata.getSchemas() as schema_rs:
                while schema_rs.next():
                    schema_name = schema_rs.getString(1)

                    if not self.config.schema_pattern.allowed(schema_name):
                        continue

                    table_types = []
                    if self.config.include_tables:
                        table_types.append("TABLE")
                    if self.config.include_views:
                        table_types.append("VIEW")

                    if not table_types:
                        continue

                    with metadata.getTables(
                        None, schema_name, None, table_types
                    ) as table_rs:
                        while table_rs.next():
                            try:
                                table_name = table_rs.getString(3)
                                table_type = table_rs.getString(4)
                                table_remarks = table_rs.getString(5)
                                full_name = f"{schema_name}.{table_name}"

                                if not self.config.table_pattern.allowed(full_name):
                                    self.report.report_table_filtered(full_name)
                                    continue

                                if (
                                    table_type == "VIEW"
                                    and not self.config.view_pattern.allowed(full_name)
                                ):
                                    self.report.report_view_filtered(full_name)
                                    continue

                                yield from self._extract_table_metadata(
                                    metadata,
                                    database_name,
                                    schema_name,
                                    table_name,
                                    table_type,
                                    table_remarks,
                                )

                                if table_type == "TABLE":
                                    self.report.report_table_scanned(full_name)
                                else:
                                    self.report.report_view_scanned(full_name)

                            except Exception as e:
                                self.report.report_failure(
                                    f"table-{schema_name}.{table_name}",
                                    f"Failed to extract table: {str(e)}",
                                )

        except Exception as exc:
            self.report.report_failure(
                message="Failed to extract table",
                context=table_name,
                exc=exc,
            )
            logger.error(f"Failed to extract tables and views: {str(exc)}")
            logger.debug(traceback.format_exc())


    def _extract_table_metadata(
        self,
        metadata,
        database: str,
        schema: str,
        table: str,
        table_type: str,
        remarks: Optional[str],
    ) -> Iterable[MetadataWorkUnit]:
        """Extract metadata for a table/view with improved type handling"""
        full_name = f"{schema}.{table}"
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(self.platform),
            name=full_name,
            platform_instance=self.platform_instance,
            env=self.env,
        )

        # Extract columns
        fields = []
        pk_columns = set()
        foreign_keys = []

        # Get columns and their types
        with metadata.getColumns(None, schema, table, None) as rs:
            while rs.next():
                col_name = rs.getString("COLUMN_NAME")
                type_name = rs.getString("TYPE_NAME").upper()
                nullable = rs.getBoolean("NULLABLE")
                remarks = rs.getString("REMARKS")
                column_size = rs.getInt("COLUMN_SIZE")
                decimal_digits = rs.getInt("DECIMAL_DIGITS")

                # Map JDBC type to DataHub type
                type_class = JDBC_TYPE_MAP.get(type_name, StringTypeClass)

                # Add native type parameters
                native_type = type_name
                if column_size > 0:
                    if type_name in ["CHAR", "VARCHAR", "BINARY", "VARBINARY"]:
                        native_type = f"{type_name}({column_size})"
                    elif type_name in ["DECIMAL", "NUMERIC"] and decimal_digits >= 0:
                        native_type = f"{type_name}({column_size},{decimal_digits})"

                field = SchemaFieldClass(
                    fieldPath=col_name,
                    nativeDataType=native_type,
                    type=SchemaFieldDataTypeClass(type=type_class()),
                    description=remarks if remarks else None,
                    nullable=nullable,
                )
                fields.append(field)

        # Get primary key info
        try:
            with metadata.getPrimaryKeys(None, schema, table) as rs:
                while rs.next():
                    pk_columns.add(rs.getString("COLUMN_NAME"))
        except Exception as e:
            logger.debug(f"Could not get primary key info for {full_name}: {e}")

        # Get foreign key info
        try:
            with metadata.getImportedKeys(None, schema, table) as rs:
                while rs.next():
                    fk_name = rs.getString("FK_NAME")
                    fk_column = rs.getString("FKCOLUMN_NAME")
                    pk_schema = rs.getString("PKTABLE_SCHEM")
                    pk_table = rs.getString("PKTABLE_NAME")
                    pk_column = rs.getString("PKCOLUMN_NAME")

                    foreign_keys.append(
                        {
                            "name": fk_name,
                            "sourceColumn": fk_column,
                            "targetSchema": pk_schema,
                            "targetTable": pk_table,
                            "targetColumn": pk_column,
                        }
                    )
        except Exception as e:
            logger.debug(f"Could not get foreign key info for {full_name}: {e}")

        # Create schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName=full_name,
            platform=make_data_platform_urn(self.platform),
            platformSchema=OtherSchemaClass(
                rawSchema=self._get_raw_schema_sql(
                    metadata=metadata,
                    schema=schema,
                    table=table,
                )
            ),
            hash="",
            version=0,
            fields=fields,
            primaryKeys=list(pk_columns) if pk_columns else None,
            foreignKeys=foreign_keys if foreign_keys else None,
        )

        self.sql_parsing_aggregator.register_schema(
            urn=dataset_urn,
            schema=schema_metadata,
        )

        # Add dataset to container
        schema_parts = schema.split('.')
        final_container_key = self.get_container_key(
            schema_parts[-1],
            ([database] if database else []) + schema_parts[:-1]
        )
        yield from add_dataset_to_container(
            container_key=final_container_key, dataset_urn=dataset_urn
        )

        # Generate schema metadata workunit
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetProperties(
                name=table, qualifiedName=f"{database}.{schema}.{table}"
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=SubTypesClass(typeNames=[table_type.title()])
        ).as_workunit()

        # For views, get view definition
        if table_type == "VIEW":
            try:
                view_definition = self._get_view_definition(metadata, schema, table)
                if view_definition:
                    view_properties = ViewPropertiesClass(
                        materialized=False,
                        viewLogic=view_definition,
                        viewLanguage="SQL",
                    )

                    self.sql_parsing_aggregator.add_view_definition(
                        view_urn=dataset_urn,
                        view_definition=view_definition,
                        default_db=database,
                        #default_schema=schema,
                    )

                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=view_properties
                    ).as_workunit()

            except Exception as e:
                logger.debug(f"Could not get view definition for {full_name}: {e}")

    def _get_view_definition(self, metadata, schema: str, view: str) -> Optional[str]:
        """Get view definition using cached method or discover new method"""
        try:
            # If we already know the successful method, use it directly
            if self._view_definition_method:
                logger.debug(f"Using cached method type: {self._view_definition_method.method_type}")
                try:
                    method = self._view_definition_method
                    if method.method_type == 'table_metadata':
                        with metadata.getTables(None, schema, view, ["VIEW"]) as rs:
                            if rs.next():
                                definition = rs.getString(method.column_index)
                                if definition:
                                    return self._clean_sql(definition)
                    elif method.method_type == 'information_schema':
                        with metadata.getConnection().createStatement() as stmt:
                            query = method.query_pattern.format(schema=schema, view=view)
                            with stmt.executeQuery(query) as rs:
                                if rs.next():
                                    definition = rs.getString(1)
                                    if definition:
                                        return self._clean_sql(definition)

                    # If we get here, the cached method failed for this view
                    logger.debug("Cached method failed to get view definition")
                except Exception as e:
                    logger.debug(f"Error using cached method: {e}")
                    self._view_definition_method = None

            # If we don't have a cached method or it failed, discover one
            if not self._view_definition_method:
                logger.debug("Discovering view definition method")
                # Try table metadata method first
                definition = self._try_table_metadata_method(metadata, schema, view)
                if definition:
                    return definition

                # Try information schema method
                definition = self._try_information_schema_method(metadata, schema, view)
                if definition:
                    return definition

            return None

        except Exception as e:
            logger.debug(f"Failed to get view definition: {str(e)}")
            logger.debug(traceback.format_exc())
            return None

    def _try_table_metadata_method(self, metadata, schema: str, view: str) -> Optional[str]:
        """Try getting view definition from table metadata"""
        try:
            with metadata.getTables(None, schema, view, ["VIEW"]) as rs:
                if rs.next():
                    rs_metadata = rs.getMetaData()
                    column_count = rs_metadata.getColumnCount()

                    # Try all string-type columns
                    for i in range(1, column_count + 1):
                        col_name = rs_metadata.getColumnName(i)
                        col_type = rs_metadata.getColumnTypeName(i)

                        # Check if it's a string type column
                        if col_type.upper() in (
                                "VARCHAR", "CLOB", "TEXT", "LONGVARCHAR", "NVARCHAR",
                                "CHARACTER VARYING", "STRING"
                        ):
                            try:
                                definition = rs.getString(i)
                                if definition and self._is_valid_sql(definition):
                                    # Cache the successful method
                                    self._view_definition_method = ViewDefinitionMethod(
                                        method_type='table_metadata',
                                        column_index=i,
                                        column_name=col_name
                                    )
                                    logger.debug(f"Found view definition in column {col_name}")
                                    return self._clean_sql(definition)
                            except Exception as e:
                                logger.debug(f"Failed to get definition from column {col_name}: {e}")
        except Exception as e:
            logger.debug(f"Table metadata method failed: {str(e)}")
        return None

    def _try_information_schema_method(self, metadata, schema: str, view: str) -> Optional[str]:
        """Try getting view definition from INFORMATION_SCHEMA"""
        try:
            with metadata.getConnection().createStatement() as stmt:
                stmt.setQueryTimeout(5)

                # Try direct view definition query first (most common case)
                view_queries = [
                    f"SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{view}'",
                    f"SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE SCHEMA_NAME = '{schema}' AND TABLE_NAME = '{view}'",
                    f"SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE VIEW_SCHEMA = '{schema}' AND VIEW_NAME = '{view}'"
                ]

                for query in view_queries:
                    try:
                        with stmt.executeQuery(query) as rs:
                            if rs.next():
                                definition = rs.getString(1)
                                if definition and self._is_valid_sql(definition):
                                    # Found working query - cache this pattern
                                    query_pattern = query.replace(schema, "{schema}").replace(view, "{view}")
                                    self._view_definition_method = ViewDefinitionMethod(
                                        method_type='information_schema',
                                        query_pattern=query_pattern
                                    )
                                    return self._clean_sql(definition)
                    except Exception:
                        continue

        except Exception as e:
            logger.debug(f"Information schema method failed: {str(e)}")
        return None

    def _is_valid_sql(self, sql: str) -> bool:
        """Check if the string appears to be a valid SQL definition"""
        if not sql or len(sql.strip()) < 20:
            return False

        sql_upper = sql.upper().strip()
        # Basic check for SQL content
        return ("SELECT" in sql_upper or "WITH" in sql_upper) and "FROM" in sql_upper

    def _get_raw_schema_sql(self, metadata, schema: str, table: str) -> str:
        """Get raw DDL schema for table/view"""
        try:
            # If we have a cached method, use it
            if hasattr(self, '_raw_schema_method'):
                try:
                    with metadata.getConnection().createStatement() as stmt:
                        query = self._raw_schema_method.query_pattern.format(schema=schema, table=table)
                        with stmt.executeQuery(query) as rs:
                            if rs.next():
                                ddl = rs.getString(1)
                                if ddl:
                                    return self._clean_sql(ddl)
                except Exception as e:
                    logger.debug(f"Cached raw schema method failed: {e}")
                    self._raw_schema_method = None

            # If no cached method or it failed, try to discover one
            if not hasattr(self, '_raw_schema_method'):
                # Common DDL queries for different databases
                ddl_queries = [
                    "SHOW CREATE TABLE \"{schema}\".\"{table}\"",
                    "SELECT DDL FROM ALL_OBJECTS WHERE OWNER = '{schema}' AND OBJECT_NAME = '{table}'",
                    "SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID('{schema}.{table}')"
                ]

                for query_pattern in ddl_queries:
                    try:
                        with metadata.getConnection().createStatement() as stmt:
                            query = query_pattern.format(schema=schema, table=table)
                            with stmt.executeQuery(query) as rs:
                                if rs.next():
                                    ddl = rs.getString(1)
                                    if ddl:
                                        # Cache the successful query pattern
                                        self._raw_schema_method = RawSchemaMethod(query_pattern=query_pattern)
                                        return self._clean_sql(ddl)
                    except Exception:
                        continue

            return ""

        except Exception as e:
            logger.debug(f"Failed to get raw schema: {str(e)}")
            return ""

    def _extract_stored_procedures(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract stored procedures metadata if supported"""
        if not self.config.include_stored_procedures:
            return

        try:
            database_name = self._get_database_name(metadata)

            with metadata.getProcedures(None, None, None) as proc_rs:
                while proc_rs.next():
                    try:
                        schema_name = proc_rs.getString(2)
                        proc_name = proc_rs.getString(3)
                        remarks = proc_rs.getString(7)
                        proc_type = proc_rs.getShort(8)

                        if not self.config.schema_pattern.allowed(schema_name):
                            continue

                        full_name = f"{schema_name}.{proc_name}"

                        # Create stored procedure container
                        container_key = self.get_container_key(
                            proc_name, [database_name, schema_name]
                        )

                        # Add to schema container
                        schema_container_key = self.get_container_key(
                            schema_name, [database_name]
                        )
                        yield MetadataChangeProposalWrapper(
                            entityUrn=container_key.as_urn(),
                            aspect=ContainerClass(
                                container=schema_container_key.as_urn()
                            ),
                        ).as_workunit()

                        # Add properties
                        yield MetadataChangeProposalWrapper(
                            entityUrn=container_key.as_urn(),
                            aspect=ContainerPropertiesClass(
                                name=proc_name,
                                description=remarks if remarks else None,
                                customProperties={
                                    "type": self._get_procedure_type(proc_type),
                                },
                            ),
                        ).as_workunit()

                        # Add subtype
                        yield MetadataChangeProposalWrapper(
                            entityUrn=container_key.as_urn(),
                            aspect=SubTypesClass(typeNames=["StoredProcedure"]),
                        ).as_workunit()

                        self.report.report_stored_procedure_scanned(full_name)

                    except Exception as e:
                        self.report.report_failure(
                            f"proc-{schema_name}.{proc_name}",
                            f"Failed to extract stored procedure: {str(e)}",
                        )

        except Exception as e:
            self.report.report_failure(
                "stored-procedures", f"Failed to extract stored procedures: {str(e)}"
            )
            logger.error(f"Failed to extract stored procedures: {str(e)}")
            logger.debug(traceback.format_exc())

    def _get_procedure_type(self, type_value: int) -> str:
        """Map procedure type value to string"""
        # Based on DatabaseMetaData.procedureNoResult etc.
        type_map = {0: "NO_RESULT", 1: "RETURNS_RESULT", 2: "RETURNS_OUTPUT"}
        return type_map.get(type_value, "UNKNOWN")

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL string"""
        if not sql:
            return ""

        # Remove comments
        sql = re.sub(r"--.*", "", sql, flags=re.MULTILINE)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

        # Normalize whitespace
        sql = re.sub(r"\s+", " ", sql.strip())

        return sql

    def get_report(self) -> SourceReport:
        """
        Get the source report.
        """
        return self.report

    def close(self) -> None:
        """Clean up resources"""
        # Close database connection
        if self._connection:
            try:
                self._connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")
            self._connection = None

        # Clean up temporary SSL certificate files
        for temp_file in self._temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                logger.warning(f"Error removing temporary file {temp_file}: {str(e)}")
        self._temp_files.clear()
