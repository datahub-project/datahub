"""
Generic JDBC Source for DataHub metadata ingestion.
Supports any JDBC-compliant database driver with improved database and schema handling.
"""

import os
import subprocess
import time
import traceback
import tempfile
from typing import Dict, List, Optional, Iterable
from dataclasses import dataclass, field
from pathlib import Path
import hashlib
import logging
import base64
import re
import jaydebeapi
from pydantic import Field, validator

from datahub.api.entities.common.data_platform_instance import DataPlatformInstance
from datahub.configuration.common import ConfigModel, AllowDenyPattern
from datahub.configuration.source_common import PlatformInstanceConfigMixin, EnvConfigMixin
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
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
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    SubTypesClass,
    StatusClass,
    ContainerClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    ViewPropertiesClass,
    BooleanTypeClass,
    NumberTypeClass,
    StringTypeClass,
    BytesTypeClass,
    TimeTypeClass,
    DateTypeClass,
    TimeStampClass,
    OtherSchemaClass,
    ContainerPropertiesClass,
    SchemaFieldDataTypeClass,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

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
    "TIMESTAMP": TimeTypeClass,
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
        if v and not re.match(r'^[^:]+:[^:]+:[^:]+$', v):
            raise ValueError("maven_coordinates must be in format 'groupId:artifactId:version'")
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


@dataclass
class JDBCSourceReport(SQLSourceReport, StaleEntityRemovalSourceReport, IngestionStageReport):
    """Report for JDBC source ingestion"""
    tables_scanned: int = 0
    views_scanned: int = 0
    stored_procedures_scanned: int = 0
    filtered_schemas: int = 0
    filtered_tables: int = 0
    filtered_views: int = 0
    filtered_stored_procedures: int = 0
    #failures: List[str] = field(default_factory=list)

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
    """Generic JDBC Source implementation with improved database and schema handling"""
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

        except Exception as e:
            self.report.report_failure("jdbc-source", f"Extraction failed: {str(e)}")
            logger.error(f"JDBC extraction failed: {str(e)}")
            logger.debug(traceback.format_exc())

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "DremioSource":
        config = JDBCSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.get_workunits_internal()

    def get_container_key(
        self, name: Optional[str], path: Optional[List[str]]
    ) -> JDBCContainerKey:
        key = name
        if path:
            key = ".".join(path) + "." + name if name else ".".join(path)

        return JDBCContainerKey(
            platform=make_data_platform_urn(self.platform),
            instance=self.platform_instance,
            env=str(self.env),
            key=key,
        )

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

                    # Add JVM args for Arrow Flight JDBC
                    jvm_args = [
                        '-Xmx1g',  # Max heap size
                        '--add-opens=java.base/java.nio=ALL-UNNAMED',
                        '--add-opens=java.base/java.lang=ALL-UNNAMED',
                        '--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED'
                    ]

                    os.environ['_JAVA_OPTIONS'] = ' '.join(jvm_args)

                    self._connection = jaydebeapi.connect(
                        self.config.driver.driver_class,
                        url,
                        props,
                        driver_path
                    )
                    return self._connection
            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to create connection after {max_retries} attempts: {str(e)}")
                logger.warning(f"Connection attempt {attempt + 1} failed, retrying in {retry_delay}s: {str(e)}")
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
            raise ValueError(f"Invalid Maven coordinates: {coords}. Format should be groupId:artifactId:version")

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
                "-q"  # Quiet mode
            ]

            try:
                logger.info(f"Downloading driver for {coords}")
                result = subprocess.run(
                    maven_cmd,
                    check=True,
                    capture_output=True,
                    text=True
                )

                # The file will be named artifactId.jar due to stripVersion=true
                downloaded_path = Path(temp_dir) / f"{artifact_id}.jar"

                if not downloaded_path.exists():
                    raise FileNotFoundError(f"Maven download succeeded but file not found at {downloaded_path}")

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
                fd, temp_path = tempfile.mkstemp(suffix=f'.{ssl_config.cert_type}')
                self._temp_files.append(temp_path)  # Track for cleanup
                with os.fdopen(fd, 'wb') as f:
                    f.write(cert_content)
                props["sslcert"] = temp_path
            except Exception as e:
                raise Exception(f"Failed to create SSL certificate file: {str(e)}")

        if ssl_config.cert_password:
            props["sslpassword"] = ssl_config.cert_password

        return props

    def _get_database_name(self, metadata) -> str:
        """Extract database name from connection metadata"""
        try:
            database = metadata.getConnection().getCatalog()
            if not database:
                url = self.config.connection.uri
                match = re.search(r'jdbc:[^:]+://[^/]+/([^?;]+)', url)
                if match:
                    database = match.group(1)
            return database or "default"
        except Exception:
            return "default"

    def _extract_database_metadata(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract database container with improved metadata"""
        try:
            database_name = self._get_database_name(metadata)

            props = {
                "productName": metadata.getDatabaseProductName(),
                "productVersion": metadata.getDatabaseProductVersion(),
                "driverName": metadata.getDriverName(),
                "driverVersion": metadata.getDriverVersion(),
                "url": self.config.connection.uri,
                "maxConnections": str(metadata.getMaxConnections()),
                "supportsBatchUpdates": str(metadata.supportsBatchUpdates()),
                "supportsTransactions": str(metadata.supportsTransactions()),
                "defaultTransactionIsolation": str(metadata.getDefaultTransactionIsolation()),
            }

            # Generate database container
            container_key = self.get_container_key(database_name, None)

            yield MetadataChangeProposalWrapper(
                entityUrn=container_key.as_urn(),
                aspect=ContainerPropertiesClass(
                    name=database_name,
                    customProperties=props,
                    description=f"Database {database_name}",
                )
            ).as_workunit()

            # Add subtype
            yield MetadataChangeProposalWrapper(
                entityUrn=container_key.as_urn(),
                aspect=SubTypesClass(typeNames=["Database"])
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
                )
            ).as_workunit()

        except Exception as e:
            self.report.report_failure(
                "database-metadata",
                f"Failed to extract database metadata: {str(e)}"
            )
            logger.error(f"Failed to extract database metadata: {str(e)}")
            logger.debug(traceback.format_exc())

    def _extract_schema_containers(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract schema containers with proper container hierarchy"""
        try:
            database_name = self._get_database_name(metadata)
            database_container_key = self.get_container_key(database_name, None)

            with metadata.getSchemas() as rs:
                while rs.next():
                    schema_name = rs.getString(1)

                    if not self.config.schema_pattern.allowed(schema_name):
                        self.report.report_schema_filtered(schema_name)
                        continue

                    try:
                        # Create schema container
                        schema_container_key = self.get_container_key(schema_name, [database_name])

                        # Link schema to database
                        yield MetadataChangeProposalWrapper(
                            entityUrn=schema_container_key.as_urn(),
                            aspect=ContainerClass(
                                container=database_container_key.as_urn()
                            )
                        ).as_workunit()

                        # Add schema properties
                        yield MetadataChangeProposalWrapper(
                            entityUrn=schema_container_key.as_urn(),
                            aspect=ContainerPropertiesClass(
                                name=schema_name,
                                description=f"Schema {schema_name}",
                                customProperties={
                                    "database": database_name,
                                }
                            )
                        ).as_workunit()

                        # Add subtype
                        yield MetadataChangeProposalWrapper(
                            entityUrn=schema_container_key.as_urn(),
                            aspect=SubTypesClass(typeNames=["Schema"])
                        ).as_workunit()

                        # Add status
                        yield MetadataChangeProposalWrapper(
                            entityUrn=schema_container_key.as_urn(),
                            aspect=StatusClass(removed=False)
                        ).as_workunit()

                    except Exception as e:
                        self.report.report_failure(
                            f"schema-{schema_name}",
                            f"Failed to process schema: {str(e)}"
                        )

        except Exception as e:
            self.report.report_failure(
                "schemas",
                f"Failed to extract schemas: {str(e)}"
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

                    with metadata.getTables(None, schema_name, None, table_types) as table_rs:
                        while table_rs.next():
                            try:
                                table_name = table_rs.getString(3)
                                table_type = table_rs.getString(4)
                                table_remarks = table_rs.getString(5)
                                full_name = f"{schema_name}.{table_name}"

                                if not self.config.table_pattern.allowed(full_name):
                                    self.report.report_table_filtered(full_name)
                                    continue

                                if table_type == "VIEW" and not self.config.view_pattern.allowed(full_name):
                                    self.report.report_view_filtered(full_name)
                                    continue

                                yield from self._extract_table_metadata(
                                    metadata,
                                    database_name,
                                    schema_name,
                                    table_name,
                                    table_type,
                                    table_remarks
                                )

                                if table_type == "TABLE":
                                    self.report.report_table_scanned(full_name)
                                else:
                                    self.report.report_view_scanned(full_name)

                            except Exception as e:
                                self.report.report_failure(
                                    f"table-{schema_name}.{table_name}",
                                    f"Failed to extract table: {str(e)}"
                                )

        except Exception as e:
            self.report.report_failure("tables", f"Failed to extract tables: {str(e)}")
            logger.error(f"Failed to extract tables and views: {str(e)}")
            logger.debug(traceback.format_exc())

    def _extract_table_metadata(
            self,
            metadata,
            database: str,
            schema: str,
            table: str,
            table_type: str,
            remarks: Optional[str]
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
                    nullable=nullable
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

                    foreign_keys.append({
                        "name": fk_name,
                        "sourceColumn": fk_column,
                        "targetSchema": pk_schema,
                        "targetTable": pk_table,
                        "targetColumn": pk_column
                    })
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

        # Add dataset to container
        schema_container_key = self.get_container_key(schema, [database])
        yield from add_dataset_to_container(
            container_key=schema_container_key,
            dataset_urn=dataset_urn
        )

        # Generate schema metadata workunit
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(
                typeNames=[
                    table_type.title()
                ]
            )
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
                        #description=remarks if remarks else None
                    )

                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=view_properties
                    ).as_workunit()

            except Exception as e:
                logger.debug(f"Could not get view definition for {full_name}: {e}")

    def _get_view_definition(self, metadata, schema: str, view: str) -> Optional[str]:
        """Get view definition with fallback methods"""
        try:
            # Try standard JDBC metadata method first
            with metadata.getTables(None, schema, view, ["VIEW"]) as rs:
                if rs.next():
                    view_definition = rs.getString("VIEW_DEFINITION")
                    if view_definition:
                        return self._clean_sql(view_definition)

            # Fallback: try querying system tables based on common DBMS patterns
            view_queries = [
                f"SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{view}'",
                f"SELECT TEXT FROM ALL_VIEWS WHERE OWNER = '{schema}' AND VIEW_NAME = '{view}'",
                f"SELECT definition FROM sys.sql_modules m JOIN sys.objects o ON m.object_id = o.object_id WHERE o.schema_id = SCHEMA_ID('{schema}') AND o.name = '{view}'"
            ]

            for query in view_queries:
                try:
                    with metadata.getConnection().createStatement() as stmt:
                        with stmt.executeQuery(query) as rs:
                            if rs.next():
                                view_definition = rs.getString(1)
                                if view_definition:
                                    return self._clean_sql(view_definition)
                except Exception:
                    continue

            return None

        except Exception as e:
            logger.debug(f"Failed to get view definition: {str(e)}")
            return None

    def _get_raw_schema_sql(self, metadata, schema: str, table: str) -> str:
        """Get raw DDL schema for table/view"""
        try:
            # Common DDL queries for different databases
            ddl_queries = [
                f"SHOW CREATE TABLE {schema}.{table}",
                f"SELECT DDL FROM ALL_OBJECTS WHERE OWNER = '{schema}' AND OBJECT_NAME = '{table}'",
                f"SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID('{schema}.{table}')"
            ]

            for query in ddl_queries:
                try:
                    with metadata.getConnection().createStatement() as stmt:
                        with stmt.executeQuery(query) as rs:
                            if rs.next():
                                ddl = rs.getString(1)
                                if ddl:
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
                        container_key = self.get_container_key(proc_name, [database_name, schema_name])

                        # Add to schema container
                        schema_container_key = self.get_container_key(schema_name, [database_name])
                        yield MetadataChangeProposalWrapper(
                            entityUrn=container_key.as_urn(),
                            aspect=ContainerClass(
                                container=schema_container_key.as_urn()
                            )
                        ).as_workunit()

                        # Add properties
                        yield MetadataChangeProposalWrapper(
                            entityUrn=container_key.as_urn(),
                            aspect=ContainerPropertiesClass(
                                name=proc_name,
                                description=remarks if remarks else None,
                                customProperties={
                                    "type": self._get_procedure_type(proc_type),
                                }
                            )
                        ).as_workunit()

                        # Add subtype
                        yield MetadataChangeProposalWrapper(
                            entityUrn=container_key.as_urn(),
                            aspect=SubTypesClass(typeNames=["StoredProcedure"])
                        ).as_workunit()

                        self.report.report_stored_procedure_scanned(full_name)

                    except Exception as e:
                        self.report.report_failure(
                            f"proc-{schema_name}.{proc_name}",
                            f"Failed to extract stored procedure: {str(e)}"
                        )

        except Exception as e:
            self.report.report_failure(
                "stored-procedures",
                f"Failed to extract stored procedures: {str(e)}"
            )
            logger.error(f"Failed to extract stored procedures: {str(e)}")
            logger.debug(traceback.format_exc())

    def _get_procedure_type(self, type_value: int) -> str:
        """Map procedure type value to string"""
        # Based on DatabaseMetaData.procedureNoResult etc.
        type_map = {
            0: "NO_RESULT",
            1: "RETURNS_RESULT",
            2: "RETURNS_OUTPUT"
        }
        return type_map.get(type_value, "UNKNOWN")

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL string"""
        if not sql:
            return ""

        # Remove comments
        sql = re.sub(r'--.*', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql.strip())

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