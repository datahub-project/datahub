import logging
import traceback
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set

import jaydebeapi

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.jdbc.constants import ContainerType, TableType
from datahub.ingestion.source.jdbc.containers import (
    ContainerRegistry,
    SchemaContainerBuilder,
    SchemaPath,
)
from datahub.ingestion.source.jdbc.reporting import JDBCSourceReport
from datahub.ingestion.source.jdbc.sql_utils import SQLUtils
from datahub.ingestion.source.jdbc.types import JDBCColumn, JDBCTable
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.schema_classes import (
    ContainerClass,
    OtherSchemaClass,
    SchemaMetadataClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)


@dataclass
class Datasets:
    """
    Extracts dataset (tables and views) metadata from JDBC databases.
    """

    platform: str
    platform_instance: Optional[str]
    env: str
    schema_pattern: AllowDenyPattern
    table_pattern: AllowDenyPattern
    view_pattern: AllowDenyPattern
    include_tables: bool
    include_views: bool
    report: JDBCSourceReport
    container_registry: ContainerRegistry
    schema_container_builder: SchemaContainerBuilder
    sql_parsing_aggregator: SqlParsingAggregator
    connection: Optional[jaydebeapi.Connection]

    def extract_datasets(
        self, metadata: jaydebeapi.Connection.cursor, database_name: Optional[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Extract dataset metadata."""
        try:
            schemas = self._get_schemas(metadata, database_name)
            if not schemas:
                return

            for schema_name in schemas:
                if not self.schema_pattern.allowed(schema_name):
                    continue

                yield from self._process_schema_tables(
                    metadata=metadata,
                    schema_name=schema_name,
                    database_name=database_name if database_name else None,
                    has_schema_support=len(schemas) > 1 or schemas[0] != database_name,
                )

        except Exception as e:
            self.report.report_failure(
                "tables-and-views", f"Failed to extract tables and views: {str(e)}"
            )
            logger.error(f"Failed to extract tables and views: {str(e)}")
            logger.debug(traceback.format_exc())

    def _get_schemas(
        self, metadata: jaydebeapi.Connection.cursor, database_name: Optional[str]
    ) -> List[str]:
        """Get list of schemas to process."""
        schemas = []
        try:
            with metadata.getSchemas() as schema_rs:
                while schema_rs.next():
                    schema_name = schema_rs.getString(1)
                    if schema_name:
                        schemas.append(schema_name)
        except Exception:
            logger.debug("Database doesn't support schema metadata")

        # If no schemas found, use database as the container
        if not schemas:
            if database_name:
                schemas = [database_name]
            else:
                schemas = []

        return schemas

    def _get_table_types(self) -> List[str]:
        """Get list of table types to extract based on configuration."""
        table_types = []
        if self.include_tables:
            table_types.append(TableType.TABLE.value)
        if self.include_views:
            table_types.append(TableType.VIEW.value)
        return table_types

    def _process_schema_tables(
        self,
        metadata: jaydebeapi.Connection.cursor,
        schema_name: str,
        database_name: Optional[str],
        has_schema_support: bool,
    ) -> Iterable[MetadataWorkUnit]:
        """Process all tables in a schema."""
        table_types = self._get_table_types()
        if not table_types:
            return

        # Pre-fetch metadata
        metadata_bundle = self._fetch_schema_metadata(
            metadata=metadata,
            schema_name=schema_name,
            table_types=table_types,
        )

        # Process tables
        catalog = None if has_schema_support else database_name
        with metadata.getTables(catalog, schema_name, None, table_types) as table_rs:
            while table_rs.next():
                yield from self._process_table(
                    table_rs=table_rs,
                    has_schema_support=has_schema_support,
                    schema_name=schema_name,
                    database_name=database_name,
                    metadata_bundle=metadata_bundle,
                )

    def _fetch_schema_metadata(
        self,
        metadata: jaydebeapi.Connection.cursor,
        schema_name: str,
        table_types: List[str],
    ) -> Dict:
        """Fetch all metadata for a schema."""
        return {
            "columns": self._batch_extract_columns(metadata, schema_name),
            "primary_keys": self._batch_extract_primary_keys(metadata, schema_name),
            "foreign_keys": self._batch_extract_foreign_keys(metadata, schema_name),
            "view_definitions": (
                self._batch_extract_view_definitions(schema_name)
                if TableType.VIEW.value in table_types
                else {}
            ),
        }

    def _process_table(
        self,
        table_rs: jaydebeapi.Connection.cursor,
        has_schema_support: bool,
        schema_name: str,
        database_name: Optional[str],
        metadata_bundle: Dict,
    ) -> Iterable[MetadataWorkUnit]:
        """Process a single table or view."""
        table_name = table_rs.getString(3)
        try:
            table_type = table_rs.getString(4)
            remarks = table_rs.getString(5)

            effective_schema = schema_name if has_schema_support else None
            full_name = (
                f"{effective_schema}.{table_name}" if effective_schema else table_name
            )

            # Apply filtering
            if not self._should_process_table(table_type, full_name):
                return

            # Create table object
            table = self._create_table_object(
                table_name=table_name,
                table_type=table_type,
                effective_schema=effective_schema,
                remarks=remarks,
                metadata_bundle=metadata_bundle,
            )

            yield from self._generate_table_metadata(
                table=table,
                database=database_name,
                view_definition=metadata_bundle["view_definitions"].get(table_name),
            )

            # Report success
            if table_type == TableType.TABLE.value:
                self.report.report_table_scanned(full_name)
            else:
                self.report.report_view_scanned(full_name)

        except Exception as exc:
            self.report.report_failure(
                message="Failed to extract table",
                context=f"{schema_name}.{table_name}",
                exc=exc,
            )

    def _should_process_table(self, table_type: str, full_name: str) -> bool:
        """Determine if table should be processed based on patterns."""
        if table_type == TableType.TABLE.value:
            return self.table_pattern.allowed(full_name)
        return self.view_pattern.allowed(full_name)

    def _create_table_object(
        self,
        table_name: str,
        table_type: str,
        effective_schema: Optional[str],
        remarks: Optional[str],
        metadata_bundle: Dict,
    ) -> JDBCTable:
        """Create JDBCTable object with metadata."""
        columns = metadata_bundle["columns"].get(table_name, [])
        pk_columns = metadata_bundle["primary_keys"].get(table_name, set())

        table_foreign_keys = []
        if table_type == TableType.TABLE.value:
            for fk in metadata_bundle["foreign_keys"].get(table_name, []):
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

        return JDBCTable(
            name=table_name,
            schema=effective_schema,
            type=table_type,
            remarks=remarks,
            columns=columns,
            pk_columns=pk_columns,
            foreign_keys=table_foreign_keys,
        )

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
            # Try with None catalog first, and if no results try with database name
            catalog = None
            has_keys = False
            with metadata.getPrimaryKeys(catalog, schema, None) as rs:
                while rs.next():
                    has_keys = True
                    table_name = rs.getString("TABLE_NAME")
                    if table_name not in pks_by_table:
                        pks_by_table[table_name] = set()
                    pks_by_table[table_name].add(rs.getString("COLUMN_NAME"))

            # If no keys found, try with database name as catalog
            if not has_keys:
                with metadata.getTables(None, schema, None, ["TABLE"]) as table_rs:
                    while table_rs.next():
                        table_name = table_rs.getString(3)
                        try:
                            with metadata.getPrimaryKeys(
                                None, schema, table_name
                            ) as rs:
                                while rs.next():
                                    if table_name not in pks_by_table:
                                        pks_by_table[table_name] = set()
                                    pks_by_table[table_name].add(
                                        rs.getString("COLUMN_NAME")
                                    )
                        except Exception as e:
                            logger.debug(
                                f"Could not get primary keys for table {table_name}: {e}"
                            )

        except Exception as e:
            logger.debug(f"Could not get primary keys for schema {schema}: {e}")
        return pks_by_table

    def _batch_extract_foreign_keys(
        self, metadata: jaydebeapi.Connection.cursor, schema: str
    ) -> Dict[str, List[Dict]]:
        """Extract foreign keys for all tables in a schema at once."""
        fks_by_table: Dict[str, List[Dict[str, str]]] = {}
        try:
            # Try first with None catalog
            catalog = None
            methods = [metadata.getImportedKeys, metadata.getExportedKeys]
            has_keys = False

            for method in methods:
                try:
                    with method(catalog, schema, None) as rs:
                        while rs.next():
                            has_keys = True
                            table_name = rs.getString("FKTABLE_NAME")
                            if table_name not in fks_by_table:
                                fks_by_table[table_name] = []

                            fk_info = {
                                "name": rs.getString("FK_NAME"),
                                "sourceColumn": rs.getString("FKCOLUMN_NAME"),
                                "targetSchema": rs.getString("PKTABLE_SCHEM"),
                                "targetTable": rs.getString("PKTABLE_NAME"),
                                "targetColumn": rs.getString("PKCOLUMN_NAME"),
                            }
                            if fk_info not in fks_by_table[table_name]:
                                fks_by_table[table_name].append(fk_info)
                except Exception as e:
                    logger.debug(
                        f"Could not get keys with method {method.__name__}: {e}"
                    )
                    continue

            # If no keys found, try table by table
            if not has_keys:
                with metadata.getTables(None, schema, None, ["TABLE"]) as table_rs:
                    while table_rs.next():
                        table_name = table_rs.getString(3)
                        for method in methods:
                            try:
                                with method(None, schema, table_name) as rs:
                                    while rs.next():
                                        if table_name not in fks_by_table:
                                            fks_by_table[table_name] = []
                                        fk_info = {
                                            "name": rs.getString("FK_NAME"),
                                            "sourceColumn": rs.getString(
                                                "FKCOLUMN_NAME"
                                            ),
                                            "targetSchema": rs.getString(
                                                "PKTABLE_SCHEM"
                                            ),
                                            "targetTable": rs.getString("PKTABLE_NAME"),
                                            "targetColumn": rs.getString(
                                                "PKCOLUMN_NAME"
                                            ),
                                        }
                                        if fk_info not in fks_by_table[table_name]:
                                            fks_by_table[table_name].append(fk_info)
                            except Exception as e:
                                logger.debug(
                                    f"Could not get keys for table {table_name} with method {method.__name__}: {e}"
                                )

        except Exception as e:
            logger.debug(f"Could not get foreign keys for schema {schema}: {e}")
        return fks_by_table

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

    def _get_info_schema_view_definitions(self, schema: str) -> Dict[str, str]:
        """Get view definitions from information schema."""
        if self.connection is None:
            return {}

        info_schema_queries = [
            f"SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema}'",
            f"SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE SCHEMA_NAME = '{schema}'",
            f"SELECT VIEW_NAME as TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE VIEW_SCHEMA = '{schema}'",
        ]

        for query in info_schema_queries:
            try:
                with self.connection.cursor() as cursor:
                    view_definitions = self._try_query_for_views(cursor, query)
                    if view_definitions:
                        return view_definitions
            except Exception:
                continue
        return {}

    def _get_system_view_definitions(self, schema: str) -> Dict[str, str]:
        """Get view definitions from system views."""
        if self.connection is None:
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
                with self.connection.cursor() as cursor:
                    view_definitions = self._try_query_for_views(cursor, query)
                    if view_definitions:
                        return view_definitions
            except Exception:
                continue
        return {}

    def _get_view_names(self, schema: str) -> Set[str]:
        """Get list of view names for a schema."""
        views: Set[str] = set()
        if self.connection is not None:
            try:
                with self.connection.cursor() as cursor:
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
        if self.connection is None:
            return None

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

        for query in queries:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query)
                    row = cursor.fetchone()
                    if row and row[0]:
                        return SQLUtils.clean_sql(row[0])
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

    def _try_query_for_views(
        self, cursor: jaydebeapi.Connection.cursor, query: str
    ) -> Dict[str, str]:
        """Try to execute a single view definition query."""
        view_definitions: Dict[str, str] = {}
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    if row[0] and row[1]:  # table_name and definition
                        view_definitions[row[0]] = SQLUtils.clean_sql(row[1])
        except Exception:
            pass
        return view_definitions

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

        if table.schema:
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
        else:
            database_container_metadata = self.schema_container_builder.build_container(
                SchemaPath([database] if database else []),
                ContainerType.DATABASE,
            )
            container_urn = database_container_metadata.container_key.as_urn()

            if not self.container_registry.has_container(container_urn):
                self.container_registry.register_container(database_container_metadata)

            database_container = self.container_registry.get_container(container_urn)
            if database_container:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=ContainerClass(
                        container=database_container.container_key.as_urn()
                    ),
                ).as_workunit()

    def _get_raw_schema_sql(self, table: JDBCTable) -> str:
        """Get raw DDL schema for table/view."""
        if self.connection is None:
            return ""

        try:
            ddl_queries = [
                f'SHOW CREATE TABLE "{table.schema}"."{table.name}"',
                f"SELECT DDL FROM ALL_OBJECTS WHERE OWNER = '{table.schema}' AND OBJECT_NAME = '{table.name}'",
            ]

            for query in ddl_queries:
                try:
                    with self.connection.cursor() as cursor:
                        cursor.execute(query)
                        row = cursor.fetchone()
                        if row and row[0]:
                            return SQLUtils.clean_sql(row[0])
                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"Could not get raw schema for {table.full_name}: {e}")

        return ""
