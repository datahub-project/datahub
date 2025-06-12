"""
Manage the communication with DataBricks Server using SQL queries and information schema tables
"""

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union

import cachetools
from cachetools import cached
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogType,
    ColumnTypeName,
    DataSourceFormat,
    SecurableType,
    TableType,
)
from databricks.sdk.service.sql import QueryStatementType
from databricks.sdk.service.workspace import ObjectType
from databricks.sql import connect

from datahub._version import nice_version_name
from datahub.api.entities.external.unity_catalog_external_entites import UnityCatalogTag
from datahub.emitter.mce_builder import parse_ts_millis
from datahub.ingestion.source.unity.hive_metastore_proxy import HiveMetastoreProxy
from datahub.ingestion.source.unity.proxy_profiling import (
    UnityCatalogProxyProfilingMixin,
)
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Column,
    CustomCatalogType,
    ExternalTableReference,
    Metastore,
    Notebook,
    Query,
    Schema,
    ServicePrincipal,
    Table,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport

logger: logging.Logger = logging.getLogger(__name__)


class UnityCatalogSqlProxy(UnityCatalogProxyProfilingMixin):
    """Unity Catalog proxy that uses SQL queries instead of REST API for better performance"""

    _workspace_client: WorkspaceClient
    _workspace_url: str
    report: UnityCatalogReport
    warehouse_id: str
    _sql_connection = None
    _catalog_table_lineage: Dict[str, List[Dict[str, str]]]
    _catalog_column_lineage: Dict[str, List[Dict[str, str]]]

    def __init__(
        self,
        workspace_url: str,
        personal_access_token: str,
        warehouse_id: Optional[str],
        report: UnityCatalogReport,
        hive_metastore_proxy: Optional[HiveMetastoreProxy] = None,
    ):
        self._workspace_client = WorkspaceClient(
            host=workspace_url,
            token=personal_access_token,
            product="datahub",
            product_version=nice_version_name(),
        )
        self.warehouse_id = warehouse_id or ""
        self.report = report
        self.hive_metastore_proxy = hive_metastore_proxy
        self._sql_connection_params = {
            "server_hostname": self._workspace_client.config.host.replace(
                "https://", ""
            ),
            "http_path": f"/sql/1.0/warehouses/{self.warehouse_id}",
            "access_token": self._workspace_client.config.token,
        }

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection"""
        self.close()

    def close(self):
        """Close the SQL connection"""
        if self._sql_connection:
            try:
                self._sql_connection.close()
            except Exception as e:
                logger.warning(f"Error closing SQL connection: {e}")
            finally:
                self._sql_connection = None

    def _get_connection(self):
        """Get or create a reusable SQL connection"""
        if self._sql_connection is None:
            try:
                self._sql_connection = connect(**self._sql_connection_params)
            except Exception as e:
                logger.error(f"Failed to create SQL connection: {e}")
                raise
        return self._sql_connection

    def check_basic_connectivity(self) -> bool:
        """Test connectivity by querying system.information_schema.catalogs"""
        try:
            query = (
                "SELECT catalog_name FROM system.information_schema.catalogs LIMIT 1"
            )
            result = self._execute_sql_query(query)
            return len(result) >= 0  # Even empty result means connection works
        except Exception as e:
            logger.error(f"Connectivity check failed: {e}")
            return False

    def assigned_metastore(self) -> Optional[Metastore]:
        """Get metastore information using SQL query"""
        try:
            # Query metastore information from system tables
            query = """
                    SELECT
                        metastore_id,
                        name,
                        owner,
                        region,
                        cloud
                    FROM system.information_schema.metastores
                             LIMIT 1 \
                    """
            rows = self._execute_sql_query(query)
            if rows:
                row = rows[0]
                return Metastore(
                    name=row[1],
                    id=self._escape_sequence(row[1]),
                    global_metastore_id=row[0],
                    metastore_id=row[0],
                    owner=row[2],
                    region=row[3],
                    cloud=row[4],
                    comment=None,
                )
        except Exception as e:
            logger.warning(f"Error getting metastore info: {e}")
        return None

    def catalogs(self, metastore: Optional[Metastore]) -> Iterable[Catalog]:
        """Get catalogs using SQL query"""
        if self.hive_metastore_proxy:
            yield self.hive_metastore_proxy.hive_metastore_catalog(metastore)

        try:
            query = """
                    SELECT
                        catalog_name,
                        catalog_owner,
                        comment
                    FROM system.information_schema.catalogs
                    WHERE catalog_name != 'hive_metastore'
                    ORDER BY catalog_name \
                    """
            rows = self._execute_sql_query(query)

            for row in rows:
                catalog_name, owner, comment = row
                if catalog_name:
                    catalog = self._create_catalog_from_sql(
                        metastore, catalog_name, None, owner, comment
                    )
                    if catalog:
                        yield catalog
        except Exception as e:
            logger.error(f"Error getting catalogs: {e}")

    def catalog(
        self, catalog_name: str, metastore: Optional[Metastore]
    ) -> Optional[Catalog]:
        """Get specific catalog using SQL query"""
        try:
            query = """
                    SELECT
                        catalog_name,
                        catalog_owner,
                        comment
                    FROM system.information_schema.catalogs
                    WHERE catalog_name = %s \
                    """
            rows = self._execute_sql_query(query, [catalog_name])

            if rows:
                row = rows[0]
                return self._create_catalog_from_sql(
                    metastore, row[0], None, row[1], row[2]
                )
        except Exception as e:
            logger.error(f"Error getting catalog {catalog_name}: {e}")
        return None

    def schemas(self, catalog: Catalog) -> Iterable[Schema]:
        """Get schemas using SQL query with batched lineage loading"""
        if (
            self.hive_metastore_proxy
            and catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG
        ):
            yield from self.hive_metastore_proxy.hive_metastore_schemas(catalog)
            return

        try:
            query = """
                    SELECT
                        schema_name,
                        schema_owner,
                        comment
                    FROM system.information_schema.schemata
                    WHERE catalog_name = %s
                    ORDER BY schema_name \
                    """
            rows = self._execute_sql_query(query, [catalog.name])

            # Pre-load all lineage data for this catalog
            self._preload_lineage_for_catalog(catalog.name)

            for row in rows:
                schema_name, owner, comment = row
                if schema_name:
                    schema = Schema(
                        name=schema_name,
                        id=f"{catalog.id}.{self._escape_sequence(schema_name)}",
                        catalog=catalog,
                        comment=comment,
                        owner=owner,
                    )
                    yield schema
        except Exception as e:
            logger.error(f"Error getting schemas for catalog {catalog.name}: {e}")

    def tables(self, schema: Schema) -> Iterable[Table]:
        """Get tables using SQL query with batched column and view definition loading"""
        if (
            self.hive_metastore_proxy
            and schema.catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG
        ):
            yield from self.hive_metastore_proxy.hive_metastore_tables(schema)
            return

        try:
            query = """
                    SELECT
                        table_name,
                        table_type,
                        table_owner,
                        comment,
                        data_source_format,
                        storage_path,
                        created,
                        created_by,
                        last_altered,
                        last_altered_by
                    FROM system.information_schema.tables
                    WHERE table_catalog = %s AND table_schema = %s
                    ORDER BY table_name \
                    """
            rows = self._execute_sql_query(query, [schema.catalog.name, schema.name])

            if not rows:
                return

            # Batch load all columns for this schema at once
            columns_by_table = self._get_all_columns_for_schema(schema)

            # Batch load all view definitions for this schema at once
            view_definitions = self._get_all_view_definitions_for_schema(schema)

            for row in rows:
                try:
                    table = self._create_table_from_sql(schema, row)
                    if table:
                        # Get columns from batch-loaded data
                        table.columns = columns_by_table.get(table.name, [])

                        # Get view definition from batch-loaded data if it's a view
                        if table.table_type and "VIEW" in str(table.table_type).upper():
                            table.view_definition = view_definitions.get(table.name)

                        yield table
                except Exception as e:
                    logger.warning(f"Error parsing table: {e}")
                    self.report.report_warning("table-parse", str(e))
        except Exception as e:
            logger.error(f"Error getting tables for schema {schema.id}: {e}")

    def _get_all_view_definitions_for_schema(self, schema: Schema) -> Dict[str, str]:
        """Get all view definitions for all views in a schema in one query"""
        try:
            query = """
                    SELECT
                        table_name,
                        view_definition
                    FROM system.information_schema.views
                    WHERE table_catalog = %s AND table_schema = %s \
                    """
            rows = self._execute_sql_query(query, [schema.catalog.name, schema.name])

            view_definitions: Dict[str, str] = {}

            for row in rows:
                table_name, view_definition = row
                if table_name and view_definition:
                    view_definitions[table_name] = view_definition

            return view_definitions
        except Exception as e:
            logger.warning(
                f"Error getting view definitions for schema {schema.id}: {e}"
            )
            return {}

    def _get_all_columns_for_schema(self, schema: Schema) -> Dict[str, List[Column]]:
        """Get all columns for all tables in a schema in one query"""
        try:
            query = """
                    SELECT
                        table_name,
                        column_name,
                        data_type,
                        ordinal_position,
                        is_nullable,
                        column_default,
                        comment,
                        numeric_precision,
                        numeric_scale
                    FROM system.information_schema.columns
                    WHERE table_catalog = %s AND table_schema = %s
                    ORDER BY table_name, ordinal_position \
                    """
            rows = self._execute_sql_query(query, [schema.catalog.name, schema.name])

            columns_by_table: Dict[str, List[Column]] = {}

            for row in rows:
                (
                    table_name,
                    column_name,
                    data_type,
                    position,
                    is_nullable,
                    default,
                    comment,
                    precision,
                    scale,
                ) = row
                if table_name and column_name:
                    if table_name not in columns_by_table:
                        columns_by_table[table_name] = []

                    table_id = f"{schema.id}.{self._escape_sequence(table_name)}"
                    column = Column(
                        name=column_name,
                        id=f"{table_id}.{self._escape_sequence(column_name)}",
                        type_text=data_type or "",
                        type_name=self._map_data_type_to_column_type(data_type),
                        type_scale=self._safe_int_convert(scale),
                        type_precision=self._safe_int_convert(precision),
                        position=self._safe_int_convert(position),
                        nullable=is_nullable.lower() == "yes" if is_nullable else None,
                        comment=comment,
                    )
                    columns_by_table[table_name].append(column)

            return columns_by_table
        except Exception as e:
            logger.warning(f"Error getting columns for schema {schema.id}: {e}")
            return {}

    def _get_table_columns(self, table: Table) -> Iterable[Column]:
        """Get columns for a table using SQL query"""
        try:
            query = """
                    SELECT
                        column_name,
                        data_type,
                        ordinal_position,
                        is_nullable,
                        column_default,
                        comment,
                        numeric_precision,
                        numeric_scale
                    FROM system.information_schema.columns
                    WHERE table_catalog = %s AND table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position \
                    """
            rows = self._execute_sql_query(
                query, [table.schema.catalog.name, table.schema.name, table.name]
            )

            for row in rows:
                (
                    column_name,
                    data_type,
                    position,
                    is_nullable,
                    default,
                    comment,
                    precision,
                    scale,
                ) = row
                if column_name:
                    column = Column(
                        name=column_name,
                        id=f"{table.id}.{self._escape_sequence(column_name)}",
                        type_text=data_type or "",
                        type_name=self._map_data_type_to_column_type(data_type),
                        type_scale=self._safe_int_convert(scale),
                        type_precision=self._safe_int_convert(precision),
                        position=self._safe_int_convert(position),
                        nullable=is_nullable.lower() == "yes" if is_nullable else None,
                        comment=comment,
                    )
                    yield column
        except Exception as e:
            logger.warning(f"Error getting columns for table {table.id}: {e}")

    def service_principals(self) -> Iterable[ServicePrincipal]:
        """Get service principals using workspace API (no SQL equivalent)"""
        try:
            for principal in self._workspace_client.service_principals.list():
                optional_sp = self._create_service_principal(principal)
                if optional_sp:
                    yield optional_sp
        except Exception as e:
            logger.error(f"Error getting service principals: {e}")

    def groups(self):
        """Get groups using workspace API (no SQL equivalent)"""
        try:
            group_list: List[Optional[str]] = []
            for group in self._workspace_client.groups.list():
                group_list.append(group.display_name)
            return group_list
        except Exception as e:
            logger.error(f"Error getting groups: {e}")
            return []

    def workspace_notebooks(self) -> Iterable[Notebook]:
        """Get notebooks using workspace API (no SQL equivalent)"""
        try:
            for obj in self._workspace_client.workspace.list("/", recursive=True):
                if (
                    obj.object_type == ObjectType.NOTEBOOK
                    and obj.object_id
                    and obj.path
                ):
                    yield Notebook(
                        id=obj.object_id,
                        path=obj.path,
                        language=obj.language,
                        created_at=parse_ts_millis(obj.created_at),
                        modified_at=parse_ts_millis(obj.modified_at),
                    )
        except Exception as e:
            logger.error(f"Error getting notebooks: {e}")

    def query_history(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> Iterable[Query]:
        """Get query history using SQL query"""
        try:
            query = """
                    SELECT
                        statement_id,
                        statement_text,
                        statement_type,
                        start_time,
                        end_time,
                        executed_by,
                        executed_by_user_id,
                        executed_as,
                        executed_as_user_id
                    FROM system.query.history
                    WHERE start_time >= %s
                      AND end_time <= %s
                      AND statement_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER')
                    ORDER BY start_time DESC \
                    """

            rows = self._execute_sql_query(
                query, [start_time.isoformat(), end_time.isoformat()]
            )

            for row in rows:
                try:
                    query_obj = self._create_query_from_sql(row)
                    if query_obj:
                        yield query_obj
                except Exception as e:
                    logger.warning(f"Error parsing query: {e}")
                    self.report.report_warning("query-parse", str(e))
        except Exception as e:
            logger.error(f"Error getting query history: {e}")

    @cached(cachetools.TTLCache(maxsize=10, ttl=600))  # Cache for 10 minutes
    def _preload_lineage_for_catalog(self, catalog_name: str) -> None:
        """Pre-load all lineage data for a catalog to enable batch lookups"""
        logger.info(f"Pre-loading lineage data for catalog: {catalog_name}")

        # Load table lineage
        try:
            query = """
                    SELECT
                        target_table_full_name,
                        source_table_full_name,
                        source_type
                    FROM system.access.table_lineage
                    WHERE target_table_full_name LIKE %s \
                    """
            rows = self._execute_sql_query(query, [f"{catalog_name}.%"])

            if not hasattr(self, "_catalog_table_lineage"):
                self._catalog_table_lineage = {}

            for row in rows:
                target_table, source_table, source_type = row
                if target_table not in self._catalog_table_lineage:
                    self._catalog_table_lineage[target_table] = []
                self._catalog_table_lineage[target_table].append(
                    {"source_table": source_table, "source_type": source_type}
                )

        except Exception as e:
            logger.debug(f"Error pre-loading table lineage for {catalog_name}: {e}")

        # Load column lineage
        try:
            query = """
                    SELECT
                        target_table_full_name,
                        target_column_name,
                        source_table_full_name,
                        source_column_name
                    FROM system.access.column_lineage
                    WHERE target_table_full_name LIKE %s \
                    """
            rows = self._execute_sql_query(query, [f"{catalog_name}.%"])

            if not hasattr(self, "_catalog_column_lineage"):
                self._catalog_column_lineage = {}

            for row in rows:
                target_table, target_column, source_table, source_column = row
                key = f"{target_table}.{target_column}"
                if key not in self._catalog_column_lineage:
                    self._catalog_column_lineage[key] = []
                self._catalog_column_lineage[key].append(
                    {"source_table": source_table, "source_column": source_column}
                )

        except Exception as e:
            logger.debug(f"Error pre-loading column lineage for {catalog_name}: {e}")

    def table_lineage(self, table: Table, include_entity_lineage: bool) -> None:
        """Get table lineage using pre-loaded data"""
        if table.schema.catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG:
            return None

        try:
            # Use pre-loaded lineage data
            if not hasattr(self, "_catalog_table_lineage"):
                return

            qualified_name = table.ref.qualified_table_name
            lineage_entries = self._catalog_table_lineage.get(qualified_name, [])

            for entry in lineage_entries:
                source_table = entry["source_table"]
                source_type = entry["source_type"]

                if source_type == "TABLE":
                    table_ref = self._create_table_reference_from_qualified_name(
                        source_table, table.schema.catalog.metastore
                    )
                    if table_ref:
                        table.upstreams[table_ref] = {}
                elif source_type == "FILE":
                    external_ref = ExternalTableReference(
                        path=source_table,
                        has_permission=True,
                        name=source_table,
                        type=SecurableType.EXTERNAL_LOCATION,
                        storage_location=source_table,
                    )
                    table.external_upstreams.add(external_ref)

        except Exception as e:
            logger.warning(f"Error getting lineage for table {table.ref}: {e}")

    def get_column_lineage(self, table: Table, column_name: str) -> None:
        """Get column lineage using pre-loaded data"""
        try:
            # Use pre-loaded lineage data
            if not hasattr(self, "_catalog_column_lineage"):
                return

            qualified_name = table.ref.qualified_table_name
            key = f"{qualified_name}.{column_name}"
            lineage_entries = self._catalog_column_lineage.get(key, [])

            for entry in lineage_entries:
                source_table = entry["source_table"]
                source_column = entry["source_column"]

                table_ref = self._create_table_reference_from_qualified_name(
                    source_table, table.schema.catalog.metastore
                )
                if table_ref:
                    table.upstreams.setdefault(table_ref, {}).setdefault(
                        column_name, []
                    ).append(source_column)

        except Exception as e:
            logger.warning(
                f"Error getting column lineage for {table.ref}.{column_name}: {e}"
            )

    def _execute_sql_query(
        self, query: str, params: Optional[List[Any]] = None
    ) -> List[List[str]]:
        """Execute SQL query using reusable databricks-sql connector"""
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            logger.warning(f"Failed to execute SQL query: {e}")
            # If connection failed, reset it for next attempt
            if "connection" in str(e).lower():
                self._sql_connection = None
            return []

    @cached(cachetools.TTLCache(maxsize=50, ttl=300))  # Cache for 5 minutes
    def _get_all_tags_for_catalog(
        self, catalog_name: str
    ) -> Dict[str, Dict[str, List[UnityCatalogTag]]]:
        """Batch load all tags for a catalog in optimized queries"""
        result: Dict[str, Dict[str, List[UnityCatalogTag]]] = {
            "schema_tags": {},
            "table_tags": {},
            "column_tags": {},
            "catalog_tags": {},
        }

        try:
            # Get schema tags
            query = f"SELECT catalog_name, schema_name, tag_name, tag_value FROM {catalog_name}.information_schema.schema_tags"
            rows = self._execute_sql_query(query)
            for row in rows:
                cat_name, schema_name, tag_name, tag_value = row
                schema_key = f"{cat_name}.{schema_name}"
                if schema_key not in result["schema_tags"]:
                    result["schema_tags"][schema_key] = []
                result["schema_tags"][schema_key].append(
                    UnityCatalogTag(key=tag_name, value=tag_value)
                )
        except Exception as e:
            logger.debug(f"Error loading schema tags for {catalog_name}: {e}")

        try:
            # Get table tags
            query = f"SELECT catalog_name, schema_name, table_name, tag_name, tag_value FROM {catalog_name}.information_schema.table_tags"
            rows = self._execute_sql_query(query)
            for row in rows:
                cat_name, schema_name, table_name, tag_name, tag_value = row
                table_key = f"{cat_name}.{schema_name}.{table_name}"
                if table_key not in result["table_tags"]:
                    result["table_tags"][table_key] = []
                result["table_tags"][table_key].append(
                    UnityCatalogTag(key=tag_name, value=tag_value)
                )
        except Exception as e:
            logger.debug(f"Error loading table tags for {catalog_name}: {e}")

        try:
            # Get column tags
            query = f"SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value FROM {catalog_name}.information_schema.column_tags"
            rows = self._execute_sql_query(query)
            for row in rows:
                cat_name, schema_name, table_name, column_name, tag_name, tag_value = (
                    row
                )
                column_key = f"{cat_name}.{schema_name}.{table_name}.{column_name}"
                if column_key not in result["column_tags"]:
                    result["column_tags"][column_key] = []
                result["column_tags"][column_key].append(
                    UnityCatalogTag(key=tag_name, value=tag_value)
                )
        except Exception as e:
            logger.debug(f"Error loading column tags for {catalog_name}: {e}")

        try:
            # Get catalog tags
            query = f"SELECT catalog_name, tag_name, tag_value FROM {catalog_name}.information_schema.catalog_tags"
            rows = self._execute_sql_query(query)
            for row in rows:
                cat_name, tag_name, tag_value = row
                if cat_name not in result["catalog_tags"]:
                    result["catalog_tags"][cat_name] = []
                result["catalog_tags"][cat_name].append(
                    UnityCatalogTag(key=tag_name, value=tag_value)
                )
        except Exception as e:
            logger.debug(f"Error loading catalog tags for {catalog_name}: {e}")

        return result

    def get_schema_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Get schema tags using cached batch loading"""
        logger.info(f"Fetching schema tags for catalog: {catalog}")
        all_tags = self._get_all_tags_for_catalog(catalog)
        return all_tags["schema_tags"]

    def get_catalog_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Get catalog tags using cached batch loading"""
        logger.info(f"Fetching catalog tags for catalog: {catalog}")
        all_tags = self._get_all_tags_for_catalog(catalog)
        return all_tags["catalog_tags"]

    def get_table_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Get table tags using cached batch loading"""
        logger.info(f"Fetching table tags for catalog: {catalog}")
        all_tags = self._get_all_tags_for_catalog(catalog)
        return all_tags["table_tags"]

    def get_column_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Get column tags using cached batch loading"""
        logger.info(f"Fetching column tags for catalog: {catalog}")
        all_tags = self._get_all_tags_for_catalog(catalog)
        return all_tags["column_tags"]

    def _create_table_reference_from_qualified_name(
        self, qualified_name: str, metastore: Optional[Metastore]
    ) -> Optional[TableReference]:
        """Create TableReference from qualified table name"""
        try:
            parts = qualified_name.split(".")
            if len(parts) != 3:
                logger.warning(f"Invalid qualified table name format: {qualified_name}")
                return None

            catalog_name, schema_name, table_name = parts
            return TableReference(
                catalog=catalog_name,
                schema=schema_name,
                table=table_name,
                metastore=metastore.name if metastore else None,
            )
        except Exception as e:
            logger.warning(f"Error creating table reference from {qualified_name}: {e}")
            return None

    @staticmethod
    def _escape_sequence(value: str) -> str:
        """Escape special characters in identifiers"""
        return value.replace(" ", "_")

    def _safe_int_convert(self, value: Any) -> Optional[int]:
        """Safely convert value to int or return None"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _create_catalog_from_sql(
        self,
        metastore: Optional[Metastore],
        name: str,
        catalog_type: Optional[str],
        owner: str,
        comment: str,
    ) -> Optional[Catalog]:
        """Create Catalog object from SQL query results"""
        if not name:
            self.report.num_catalogs_missing_name += 1
            return None

        catalog_name = self._escape_sequence(name)

        # Default to MANAGED_CATALOG since catalog_type is not available in information_schema
        catalog_type_enum = CatalogType.MANAGED_CATALOG
        if catalog_type:
            catalog_type_enum = self._map_catalog_type(catalog_type)

        return Catalog(
            name=name,
            id=f"{metastore.id}.{catalog_name}" if metastore else catalog_name,
            metastore=metastore,
            comment=comment,
            owner=owner,
            type=catalog_type_enum,
        )

    def _create_table_from_sql(self, schema: Schema, row: List[Any]) -> Optional[Table]:
        """Create Table object from SQL query results"""
        (
            table_name,
            table_type,
            owner,
            comment,
            data_source_format,
            storage_path,
            created,
            created_by,
            last_altered,
            last_altered_by,
        ) = row

        if not table_name:
            self.report.num_tables_missing_name += 1
            return None

        table_obj_id = f"{schema.id}.{self._escape_sequence(table_name)}"

        # View definition will be set by the caller from batch-loaded data
        return Table(
            name=table_name,
            id=table_obj_id,
            table_type=self._map_table_type(table_type),
            schema=schema,
            storage_location=storage_path,
            data_source_format=self._map_data_source_format(data_source_format),
            columns=[],  # Will be populated separately
            view_definition=None,  # Will be set by caller for views
            properties={},
            owner=owner,
            generation=None,
            created_at=self._parse_datetime(created),
            created_by=created_by,
            updated_at=self._parse_datetime(last_altered),
            updated_by=last_altered_by,
            table_id=None,  # Not available in information schema
            comment=comment,
        )

    def _create_service_principal(self, obj: Any) -> Optional[ServicePrincipal]:
        """Create ServicePrincipal object"""
        if not obj.display_name or not obj.application_id:
            return None
        return ServicePrincipal(
            id=f"{obj.id}.{self._escape_sequence(obj.display_name)}",
            display_name=obj.display_name,
            application_id=obj.application_id,
            active=obj.active,
        )

    def _create_query_from_sql(self, row: List[Any]) -> Optional[Query]:
        """Create Query object from SQL query results"""
        (
            statement_id,
            query_text,
            statement_type,
            start_time,
            end_time,
            user_id,
            user_name,
            executed_as_user_id,
            executed_as_user_name,
        ) = row

        if not query_text or not start_time or not end_time:
            self.report.num_queries_missing_info += 1
            return None

        parsed_start_time = self._parse_datetime(start_time)
        parsed_end_time = self._parse_datetime(end_time)

        if parsed_start_time is None or parsed_end_time is None:
            self.report.num_queries_missing_info += 1
            return None

        return Query(
            query_id=statement_id,
            query_text=query_text,
            statement_type=self._map_statement_type(statement_type),
            start_time=parsed_start_time,
            end_time=parsed_end_time,
            user_id=user_id,
            user_name=user_name,
            executed_as_user_id=executed_as_user_id,
            executed_as_user_name=executed_as_user_name,
        )

    def _map_catalog_type(self, catalog_type_str: str) -> CatalogType:
        """Map string catalog type to enum"""
        type_mapping = {
            "MANAGED_CATALOG": CatalogType.MANAGED_CATALOG,
            "EXTERNAL_CATALOG": CatalogType.SYSTEM_CATALOG,  # Fixed attribute access
            "DELTASHARING_CATALOG": CatalogType.DELTASHARING_CATALOG,
        }
        return type_mapping.get(catalog_type_str, CatalogType.MANAGED_CATALOG)

    def _map_table_type(self, table_type_str: str) -> TableType:
        """Map string table type to enum"""
        type_mapping = {
            "MANAGED": TableType.MANAGED,
            "EXTERNAL": TableType.EXTERNAL,
            "VIEW": TableType.VIEW,
            "MATERIALIZED_VIEW": TableType.MATERIALIZED_VIEW,
            "STREAMING_TABLE": TableType.STREAMING_TABLE,
        }
        return type_mapping.get(table_type_str, TableType.MANAGED)

    def _map_data_type_to_column_type(self, data_type: str) -> ColumnTypeName:
        """Map data type string to column type enum"""
        if not data_type:
            return ColumnTypeName.USER_DEFINED_TYPE

        data_type_lower = data_type.lower()

        if "string" in data_type_lower or "varchar" in data_type_lower:
            return ColumnTypeName.STRING
        elif "int" in data_type_lower:
            return ColumnTypeName.INT
        elif "long" in data_type_lower or "bigint" in data_type_lower:
            return ColumnTypeName.LONG
        elif "double" in data_type_lower or "float" in data_type_lower:
            return ColumnTypeName.DOUBLE
        elif "decimal" in data_type_lower:
            return ColumnTypeName.DECIMAL
        elif "boolean" in data_type_lower:
            return ColumnTypeName.BOOLEAN
        elif "timestamp" in data_type_lower:
            return ColumnTypeName.TIMESTAMP
        elif "date" in data_type_lower:
            return ColumnTypeName.DATE
        elif "binary" in data_type_lower:
            return ColumnTypeName.BINARY
        elif "array" in data_type_lower:
            return ColumnTypeName.ARRAY
        elif "map" in data_type_lower:
            return ColumnTypeName.MAP
        elif "struct" in data_type_lower:
            return ColumnTypeName.STRUCT
        else:
            return ColumnTypeName.USER_DEFINED_TYPE

    def _map_statement_type(self, statement_type_str: str) -> QueryStatementType:
        """Map statement type string to enum"""
        type_mapping = {
            "SELECT": QueryStatementType.SELECT,
            "INSERT": QueryStatementType.INSERT,
            "UPDATE": QueryStatementType.UPDATE,
            "DELETE": QueryStatementType.DELETE,
            "CREATE": QueryStatementType.CREATE,
            "DROP": QueryStatementType.DROP,
            "ALTER": QueryStatementType.ALTER,
        }
        return type_mapping.get(statement_type_str, QueryStatementType.SELECT)

    def _get_view_definition(
        self, catalog_name: str, schema_name: str, table_name: str
    ) -> Optional[str]:
        """Get view definition using SHOW CREATE TABLE command"""
        try:
            query = f"SHOW CREATE TABLE `{catalog_name}`.`{schema_name}`.`{table_name}`"
            rows = self._execute_sql_query(query)
            if rows and len(rows) > 0:
                # The result typically contains the CREATE VIEW statement
                return rows[0][0] if rows[0] else None
        except Exception as e:
            logger.debug(
                f"Could not get view definition for {catalog_name}.{schema_name}.{table_name}: {e}"
            )
        return None

    def _map_data_source_format(
        self, data_source_format_str: Optional[str]
    ) -> Optional[DataSourceFormat]:
        """Map data source format string to enum"""
        if not data_source_format_str:
            return None

        try:
            # First try to create the enum directly
            try:
                return DataSourceFormat(data_source_format_str)
            except ValueError:
                pass

            # If direct creation fails, try with uppercase
            try:
                return DataSourceFormat(data_source_format_str.upper())
            except ValueError:
                pass

            # Manual mapping for common formats
            format_mapping = {
                "DELTA": "DELTA",
                "CSV": "CSV",
                "JSON": "JSON",
                "AVRO": "AVRO",
                "PARQUET": "PARQUET",
                "ORC": "ORC",
                "TEXT": "TEXT",
            }

            data_source_upper = data_source_format_str.upper()
            if data_source_upper in format_mapping:
                try:
                    return DataSourceFormat(format_mapping[data_source_upper])
                except ValueError:
                    pass

            # If no match found, log and return None instead of failing
            logger.debug(
                f"Unknown data source format: {data_source_format_str}, defaulting to None"
            )
            return None

        except Exception as e:
            logger.warning(
                f"Error mapping data source format '{data_source_format_str}': {e}"
            )
            return None

    def _parse_datetime(
        self, datetime_input: Optional[Union[str, datetime]]
    ) -> Optional[datetime]:
        """Parse datetime string or datetime object to datetime object"""
        if not datetime_input:
            return None

        # If it's already a datetime object, return it
        if isinstance(datetime_input, datetime):
            return datetime_input

        # If it's a string, try to parse it
        if isinstance(datetime_input, str):
            try:
                # Handle different datetime formats
                if "T" in datetime_input:
                    return datetime.fromisoformat(datetime_input.replace("Z", "+00:00"))
                else:
                    return datetime.strptime(datetime_input, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError) as e:
                logger.warning(f"Error parsing datetime string {datetime_input}: {e}")
                return None

        logger.warning(f"Unexpected datetime input type: {type(datetime_input)}")
        return None
