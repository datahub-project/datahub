"""
Generic JDBC Source for DataHub metadata ingestion.
Supports any JDBC-compliant database driver.
"""

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
import jaydebeapi
import glob
from pydantic import Field, validator

from datahub.configuration.common import ConfigModel, AllowDenyPattern
from datahub.configuration.source_common import PlatformInstanceConfigMixin, EnvConfigMixin
from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn_with_platform_instance, \
    make_container_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport, MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import StatefulIngestionConfigBase, \
    StatefulIngestionSourceBase
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata._schema_classes import SchemaFieldDataTypeClass, OtherSchemaClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import OtherSchema
from datahub.metadata.schema_classes import (
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
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_dataset_to_container, gen_containers
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)

# Generic JDBC type mapping
JDBC_TYPE_MAP = {
    # Map standard JDBC types to DataHub types
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
    "TIMESTAMP": TimeStampClass,
}


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

    platform: str = Field(description="Name of platform being ingested, used in constructing URNs.")

    include_tables: bool = Field(
        default=True,
        description="Include tables in extraction",
    )

    include_views: bool = Field(
        default=True,
        description="Include views in extraction",
    )

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas",
    )

    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables",
    )

    usage: BaseUsageConfig = Field(
        description="Usage statistics configuration",
        default=BaseUsageConfig(),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class JDBCSourceReport(SQLSourceReport, StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    views_scanned: int = 0
    filtered_schemas: int = 0
    filtered_tables: int = 0
    filtered_views: int = 0
    failures: List[str] = field(default_factory=list)

    def report_table_scanned(self, table: str) -> None:
        self.tables_scanned += 1

    def report_view_scanned(self, view: str) -> None:
        self.views_scanned += 1

    def report_schema_filtered(self, schema: str) -> None:
        self.filtered_schemas += 1
        self.report_dropped(f"Schema: {schema}")

    def report_table_filtered(self, table: str) -> None:
        self.filtered_tables += 1
        self.report_dropped(f"Table: {table}")

    def report_view_filtered(self, view: str) -> None:
        self.filtered_views += 1
        self.report_dropped(f"View: {view}")

    def report_failure(self, key: str, reason: str) -> None:
        self.failures.append(f"{key}: {reason}")
        super().report_failure(key, reason)


class JDBCSource(StatefulIngestionSourceBase):
    """Generic JDBC Source implementation"""
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
        self.sql_parsing_aggregator = SqlParsingAggregator(
            platform=make_data_platform_urn(self.platform),
            platform_instance=self.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=self.config.usage,
        )

    def is_checkpointing_enabled(self) -> bool:
        return True

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        try:
            with self._get_connection() as conn:
                metadata = conn.getMetaData()

                # Extract containers (schemas)
                yield from self._extract_containers(metadata)

                # Extract tables and views
                yield from self._extract_tables_and_views(metadata)

        except Exception as e:
            self.report.report_failure("jdbc-source", f"Extraction failed: {str(e)}")

    def _get_connection(self):
        """Get JDBC connection"""
        try:
            if not self._connection:
                driver_path = self._get_driver_path()
                url = self.config.connection.uri
                props = self._get_connection_properties()

                self._connection = jaydebeapi.connect(
                    self.config.driver.driver_class,
                    url,
                    props,
                    driver_path
                )
            return self._connection
        except Exception as e:
            raise Exception(f"Failed to create connection: {str(e)}")

    def _get_driver_path(self) -> str:
        """Get JDBC driver path"""
        if self.config.driver.driver_path:
            if not os.path.exists(self.config.driver.driver_path):
                raise FileNotFoundError(f"Driver not found: {self.config.driver.driver_path}")
            return self.config.driver.driver_path

        if self.config.driver.maven_coordinates:
            return self._download_driver_from_maven()

        raise ValueError("Either driver_path or maven_coordinates must be specified")

    def _download_driver_from_maven(self) -> str:
        """Download driver from Maven"""
        coords = self.config.driver.maven_coordinates
        driver_dir = Path.home() / ".datahub" / "drivers"
        driver_dir.mkdir(parents=True, exist_ok=True)

        maven_cmd = [
            "mvn",
            "dependency:get",
            f"-Dartifact={coords}",
            "-DremoteRepositories=https://repo1.maven.org/maven2/",
            f"-DoutputDirectory={driver_dir}"
        ]

        subprocess.run(maven_cmd, check=True, capture_output=True)

        artifact_id, version = coords.split(":")[1:3]
        driver_path = driver_dir / f"{artifact_id}-{version}.jar"

        if not driver_path.exists():
            raise FileNotFoundError(f"Driver not found at {driver_path}")

        return str(driver_path)

    def _get_connection_properties(self) -> Dict[str, str]:
        """Get connection properties"""
        props = dict(self.config.connection.properties)

        if self.config.connection.username:
            props["user"] = self.config.connection.username
        if self.config.connection.password:
            props["password"] = self.config.connection.password

        if self.config.connection.ssl_config:
            props.update(self._get_ssl_properties())

        return props

    def _get_ssl_properties(self) -> Dict[str, str]:
        """Get SSL properties"""
        ssl_config = self.config.connection.ssl_config
        props = {"ssl": "true"}

        if ssl_config.cert_path:
            props["sslcert"] = ssl_config.cert_path
        elif ssl_config.cert_content:
            cert_content = base64.b64decode(ssl_config.cert_content)
            fd, temp_path = tempfile.mkstemp(suffix=f'.{ssl_config.cert_type}')
            with os.fdopen(fd, 'wb') as f:
                f.write(cert_content)
            props["sslcert"] = temp_path

        if ssl_config.cert_password:
            props["sslpassword"] = ssl_config.cert_password

        return props

    def health_check(self) -> bool:
        try:
            with self._get_connection():
                return True
        except Exception as e:
            self.report.report_failure("health", f"Failed health check: {str(e)}")
            return False

    def test_connection(self) -> None:
        try:
            driver_path = self._get_driver_path()
            if not os.path.exists(driver_path):
                raise FileNotFoundError(f"JDBC driver not found at: {driver_path}")

            with self._get_connection() as conn:
                metadata = conn.getMetaData()
                db_name = metadata.getDatabaseProductName()
                version = metadata.getDatabaseProductVersion()
                logger.info(f"Successfully connected to {db_name} version {version}")
        except Exception as e:
            self.report.report_failure("connection", f"Connection test failed: {str(e)}")
            raise

    def _extract_database_metadata(self, metadata) -> Iterable[MetadataWorkUnit]:
        try:
            props = {
                "productName": metadata.getDatabaseProductName(),
                "productVersion": metadata.getDatabaseProductVersion(),
                "driverName": metadata.getDriverName(),
                "driverVersion": metadata.getDriverVersion(),
            }

            database_name = props["productName"].lower()
            container_urn = make_container_urn(
                make_container_key()
            )f"urn:li:container:{self.platform}:{database_name}"

            container_workunits = gen_containers(
                container_key=database_name,
                name=database_name,
                sub_types=["Database"],
                description=f"Version: {props['productVersion']}",
                properties=props
            )

            for wu in container_workunits:
                yield wu

        except Exception as e:
            self.report.report_failure(
                "database-metadata",
                f"Failed to extract database metadata: {str(e)}"
            )

    def _extract_containers(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract database/schema containers"""
        try:
            with metadata.getSchemas() as rs:
                for schema in rs:
                    schema_name = schema.getString(1)
                    if not self.config.schema_pattern.allowed(schema_name):
                        self.report.report_schema_filtered(schema_name)
                        continue

                    container_key = schema_name
                    container_workunits = gen_containers(
                        container_key=container_key,
                        name=schema_name,
                        sub_types=["Schema"],
                        description=None,
                        owner_urn=None
                    )

                    for wu in container_workunits:
                        yield wu

        except Exception as e:
            self.report.report_failure("containers", f"Failed to extract containers: {str(e)}")

    def _extract_tables_and_views(self, metadata) -> Iterable[MetadataWorkUnit]:
        """Extract tables and views"""
        try:
            with metadata.getSchemas() as schema_rs:
                for schema in schema_rs:
                    schema_name = schema.getString(1)
                    if not self.config.schema_pattern.allowed(schema_name):
                        continue

                    table_types = []
                    if self.config.include_tables:
                        table_types.append("TABLE")
                    if self.config.include_views:
                        table_types.append("VIEW")

                    with metadata.getTables(None, schema_name, None, table_types) as table_rs:
                        for table in table_rs:
                            try:
                                table_name = table.getString(3)
                                table_type = table.getString(4)
                                full_name = f"{schema_name}.{table_name}"

                                if not self.config.table_pattern.allowed(full_name):
                                    self.report.report_table_filtered(full_name)
                                    continue

                                yield from self._extract_table_metadata(
                                    metadata,
                                    schema_name,
                                    table_name,
                                    table_type
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

    def _extract_table_metadata(
            self,
            metadata,
            schema: str,
            table: str,
            table_type: str
    ) -> Iterable[MetadataWorkUnit]:
        """Extract metadata for a table/view"""
        full_name = f"{schema}.{table}"
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
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
            for column in rs:
                col_name = column.getString("COLUMN_NAME")
                type_name = column.getString("TYPE_NAME").upper()
                nullable = column.getBoolean("NULLABLE")
                remarks = column.getString("REMARKS")

                # Map JDBC type to DataHub type
                type_class = JDBC_TYPE_MAP.get(type_name, StringTypeClass)

                field = SchemaFieldClass(
                    fieldPath=col_name,
                    nativeDataType=type_name,
                    type=SchemaFieldDataTypeClass(type=type_class()),
                    description=remarks if remarks else None,
                    nullable=nullable
                )
                fields.append(field)

        # Try to get primary key info
        try:
            with metadata.getPrimaryKeys(None, schema, table) as rs:
                for pk in rs:
                    pk_columns.add(pk.getString("COLUMN_NAME"))
        except Exception as e:
            logger.debug(f"Could not get primary key info for {full_name}: {e}")

        # Try to get foreign key info
        try:
            with metadata.getImportedKeys(None, schema, table) as rs:
                for fk in rs:
                    fk_name = fk.getString("FK_NAME")
                    fk_column = fk.getString("FKCOLUMN_NAME")
                    pk_schema = fk.getString("PKTABLE_SCHEM")
                    pk_table = fk.getString("PKTABLE_NAME")
                    pk_column = fk.getString("PKCOLUMN_NAME")

                    foreign_keys.append({
                        "name": fk_name,
                        "column": fk_column,
                        "parent_schema": pk_schema,
                        "parent_table": pk_table,
                        "parent_column": pk_column
                    })
        except Exception as e:
            logger.debug(f"Could not get foreign key info for {full_name}: {e}")

        # Create schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName=full_name,
            platform=make_data_platform_urn(platform),
            platformSchema=OtherSchemaClass(""),
            version=0,
            fields=fields,
            primaryKeys=list(pk_columns) if pk_columns else None,
            foreignKeys=foreign_keys if foreign_keys else None
        )

        # Generate schema metadata workunit
        schema_wu = MetadataWorkUnit(
            id=f"{full_name}-schema",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                changeType="UPSERT",
                aspectName="schemaMetadata",
                aspect=schema_metadata
            )
        )
        yield schema_wu

        # For views, try to get view definition
        if table_type == "VIEW":
            try:
                view_definition = None
                # Try standard JDBC metadata method first
                with metadata.getTables(None, schema, table, ["VIEW"]) as rs:
                    for view in rs:
                        view_definition = view.getString("VIEW_DEFINITION")
                        break

                if view_definition:
                    view_properties = ViewPropertiesClass(
                        materialized=False,
                        viewLogic=view_definition,
                        viewLanguage="SQL"
                    )

                    view_wu = MetadataWorkUnit(
                        id=f"{full_name}-view",
                        mcp=MetadataChangeProposalWrapper(
                            entityType="dataset",
                            entityUrn=dataset_urn,
                            changeType="UPSERT",
                            aspectName="viewProperties",
                            aspect=view_properties
                        )
                    )
                    yield view_wu

            except Exception as e:
                logger.debug(f"Could not get view definition for {full_name}: {e}")

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL string"""
        if not sql:
            return ""

        # Remove comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
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
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None