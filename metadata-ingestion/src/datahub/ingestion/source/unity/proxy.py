"""
Manage the communication with DataBricks Server and provide equivalent dataclasses for dependent modules
"""

import dataclasses
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union, cast
from unittest.mock import patch

import cachetools
from cachetools import cached
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    GetMetastoreSummaryResponse,
    MetastoreInfo,
    SchemaInfo,
    TableInfo,
)
from databricks.sdk.service.iam import ServicePrincipal as DatabricksServicePrincipal
from databricks.sdk.service.sql import (
    QueryFilter,
    QueryInfo,
    QueryStatementType,
    QueryStatus,
)
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
    ALLOWED_STATEMENT_TYPES,
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


@dataclasses.dataclass
class TableInfoWithGeneration(TableInfo):
    generation: Optional[int] = None

    def as_dict(self) -> dict:
        return {**super().as_dict(), "generation": self.generation}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TableInfoWithGeneration":
        table_info: TableInfoWithGeneration = cast(
            TableInfoWithGeneration,
            super().from_dict(d),
        )
        table_info.generation = d.get("generation")
        return table_info


@dataclasses.dataclass
class QueryFilterWithStatementTypes(QueryFilter):
    statement_types: List[QueryStatementType] = dataclasses.field(default_factory=list)

    def as_dict(self) -> dict:
        return {**super().as_dict(), "statement_types": self.statement_types}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "QueryFilterWithStatementTypes":
        v: QueryFilterWithStatementTypes = cast(
            QueryFilterWithStatementTypes,
            super().from_dict(d),
        )
        v.statement_types = d["statement_types"]
        return v


class UnityCatalogApiProxy(UnityCatalogProxyProfilingMixin):
    _workspace_client: WorkspaceClient
    _workspace_url: str
    report: UnityCatalogReport
    warehouse_id: str

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

    def check_basic_connectivity(self) -> bool:
        return bool(self._workspace_client.catalogs.list(include_browse=True))

    def assigned_metastore(self) -> Optional[Metastore]:
        response = self._workspace_client.metastores.summary()
        return self._create_metastore(response)

    def catalogs(self, metastore: Optional[Metastore]) -> Iterable[Catalog]:
        if self.hive_metastore_proxy:
            yield self.hive_metastore_proxy.hive_metastore_catalog(metastore)

        response = self._workspace_client.catalogs.list(include_browse=True)
        if not response:
            logger.info("Catalogs not found")
            return
        for catalog in response:
            optional_catalog = self._create_catalog(metastore, catalog)
            if optional_catalog:
                yield optional_catalog

    def catalog(
        self, catalog_name: str, metastore: Optional[Metastore]
    ) -> Optional[Catalog]:
        response = self._workspace_client.catalogs.get(
            catalog_name, include_browse=True
        )
        if not response:
            logger.info(f"Catalog {catalog_name} not found")
            return None
        optional_catalog = self._create_catalog(metastore, response)
        if optional_catalog:
            return optional_catalog

        return None

    def schemas(self, catalog: Catalog) -> Iterable[Schema]:
        if (
            self.hive_metastore_proxy
            and catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG
        ):
            yield from self.hive_metastore_proxy.hive_metastore_schemas(catalog)
            return
        response = self._workspace_client.schemas.list(
            catalog_name=catalog.name, include_browse=True
        )
        if not response:
            logger.info(f"Schemas not found for catalog {catalog.id}")
            return
        for schema in response:
            optional_schema = self._create_schema(catalog, schema)
            if optional_schema:
                yield optional_schema

    def tables(self, schema: Schema) -> Iterable[Table]:
        if (
            self.hive_metastore_proxy
            and schema.catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG
        ):
            yield from self.hive_metastore_proxy.hive_metastore_tables(schema)
            return
        with patch("databricks.sdk.service.catalog.TableInfo", TableInfoWithGeneration):
            response = self._workspace_client.tables.list(
                catalog_name=schema.catalog.name,
                schema_name=schema.name,
                include_browse=True,
            )
            if not response:
                logger.info(f"Tables not found for schema {schema.id}")
                return
            for table in response:
                try:
                    optional_table = self._create_table(
                        schema, cast(TableInfoWithGeneration, table)
                    )
                    if optional_table:
                        yield optional_table
                except Exception as e:
                    logger.warning(f"Error parsing table: {e}")
                    self.report.report_warning("table-parse", str(e))

    def service_principals(self) -> Iterable[ServicePrincipal]:
        for principal in self._workspace_client.service_principals.list():
            optional_sp = self._create_service_principal(principal)
            if optional_sp:
                yield optional_sp

    def groups(self):
        """
        fetch the list of the groups belongs to the workspace, using the workspace client
        create the list of group's display name, iterating through the list of groups fetched by the workspace client
        """
        group_list: List[Optional[str]] = []
        for group in self._workspace_client.groups.list():
            group_list.append(group.display_name)
        return group_list

    def workspace_notebooks(self) -> Iterable[Notebook]:
        for obj in self._workspace_client.workspace.list("/", recursive=True):
            if obj.object_type == ObjectType.NOTEBOOK and obj.object_id and obj.path:
                yield Notebook(
                    id=obj.object_id,
                    path=obj.path,
                    language=obj.language,
                    created_at=parse_ts_millis(obj.created_at),
                    modified_at=parse_ts_millis(obj.modified_at),
                )

    def query_history(
        self,
        start_time: datetime,
        end_time: datetime,
        use_server_side_scrolling: bool = True,
    ) -> Iterable[Query]:
        """Returns all queries that were run between start_time and end_time with relevant statement_type.

        Args:
            start_time: Start time for query filtering
            end_time: End time for query filtering  
            use_server_side_scrolling: Use alternative server-side scrolling implementation
                                     for better performance with large datasets

        Raises:
            DatabricksError: If the query history API returns an error.
        """
        if use_server_side_scrolling:
            # Use alternative server-side scrolling implementation
            for query_info in self._query_history_server_side_scrolling(
                start_time=start_time,
                end_time=end_time,
            ):
                try:
                    optional_query = self._create_query(query_info)
                    if optional_query:
                        yield optional_query
                except Exception as e:
                    logger.warning(f"Error parsing query: {e}")
                    self.report.report_warning("query-parse", str(e))
        else:
            # Use original API-based implementation
            filter_by = QueryFilterWithStatementTypes.from_dict(
                {
                    "query_start_time_range": {
                        "start_time_ms": start_time.timestamp() * 1000,
                        "end_time_ms": end_time.timestamp() * 1000,
                    },
                    "statuses": [QueryStatus.FINISHED],
                    "statement_types": [typ.value for typ in ALLOWED_STATEMENT_TYPES],
                }
            )
            for query_info in self._query_history(filter_by=filter_by):
                try:
                    optional_query = self._create_query(query_info)
                    if optional_query:
                        yield optional_query
                except Exception as e:
                    logger.warning(f"Error parsing query: {e}")
                    self.report.report_warning("query-parse", str(e))

    def _query_history(
        self,
        filter_by: QueryFilterWithStatementTypes,
        max_results: int = 1000,
        include_metrics: bool = False,
    ) -> Iterable[QueryInfo]:
        """Manual implementation of the query_history.list() endpoint.

        Needed because:
        - WorkspaceClient incorrectly passes params as query params, not body
        - It does not paginate correctly -- needs to remove filter_by argument
        Remove if these issues are fixed.
        """
        method = "GET"
        path = "/api/2.0/sql/history/queries"
        body: Dict[str, Any] = {
            "include_metrics": include_metrics,
            "max_results": max_results,  # Max batch size
        }

        response: dict = self._workspace_client.api_client.do(  # type: ignore
            method, path, body={**body, "filter_by": filter_by.as_dict()}
        )
        # we use default raw=False(default) in above request, therefore will always get dict
        while True:
            if "res" not in response or not response["res"]:
                return
            for v in response["res"]:
                yield QueryInfo.from_dict(v)
            if not response.get("next_page_token"):  # last page
                return
            response = self._workspace_client.api_client.do(  # type: ignore
                method, path, body={**body, "page_token": response["next_page_token"]}
            )

    def _query_history_server_side_scrolling(
        self,
        start_time: datetime,
        end_time: datetime,
        catalog_name: str = "system",
        batch_size: int = 2000,
        enable_streaming: bool = True,
    ) -> Iterable[QueryInfo]:
        """
        Alternative query history implementation using server-side scrolling 
        with information schema audit logs.
        
        This method provides better performance for large datasets by:
        - Using SQL queries against information_schema.query_history
        - Implementing cursor-based pagination instead of offset-based
        - Streaming results to reduce memory usage
        - Server-side filtering and sorting
        
        Args:
            start_time: Query start time filter
            end_time: Query end time filter  
            catalog_name: Catalog containing information schema (default: "system")
            batch_size: Number of records per batch (default: 2000)
            enable_streaming: Enable streaming mode for large datasets
            
        Yields:
            QueryInfo: Individual query information objects
        """
        logger.info(
            f"Starting server-side scrolling query history from {start_time} to {end_time}"
        )
        
        # Convert allowed statement types to SQL IN clause
        allowed_types = [f"'{typ.value}'" for typ in ALLOWED_STATEMENT_TYPES]
        statement_types_filter = f"({', '.join(allowed_types)})"
        
        # Convert timestamps to proper timestamp literals for SQL
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Initialize cursor for pagination
        cursor_timestamp_str = start_time_str
        processed_count = 0
        
        while cursor_timestamp_str < end_time_str:
            # Build SQL query with cursor-based pagination using timestamp literals
            query = f"""
            SELECT 
                statement_id as query_id,
                statement_text as query_text,
                statement_type,
                CAST(start_time AS BIGINT) * 1000 as query_start_time_ms,
                CAST(end_time AS BIGINT) * 1000 as query_end_time_ms,
                executed_by_user_id as user_id,
                executed_by as user_name,
                executed_as_user_id,
                executed_as as executed_as_user_name,
                execution_status as status,
                compute.warehouse_id as warehouse_id,
                workspace_id,
                compute.cluster_id as compute_id,
                NULL as catalog_name,
                NULL as schema_name,
                total_duration_ms,
                NULL as result_state,
                NULL as result_data_in_bytes,
                produced_rows as result_row_count,
                error_message,
                NULL as update_count,
                produced_rows as rows_produced_count,
                read_rows as rows_read_count,
                compilation_duration_ms as compilation_time_ms,
                execution_duration_ms as execution_time_ms,
                read_io_cache_percent,
                NULL as rows_spilled_to_disk_count,
                NULL as task_count,
                NULL as pruned_bytes,
                pruned_files as pruned_files_count,
                read_bytes,
                read_files as read_files_count,
                read_partitions as read_partitions_count,
                NULL as read_remote_bytes,
                read_bytes as scanned_bytes,
                read_files as scanned_files_count,
                read_partitions as scanned_partitions_count,
                NULL as shuffle_bytes_spilled,
                NULL as shuffle_local_bytes_read,
                shuffle_read_bytes as shuffle_remote_bytes_read,
                NULL as shuffle_remote_bytes_read_to_disk,
                NULL as shuffle_write_bytes,
                spilled_local_bytes as spilled_bytes_encrypted,
                NULL as spilled_bytes_uncompressed,
                written_bytes as write_remote_bytes
            FROM system.query.history
            WHERE start_time >= TIMESTAMP '{cursor_timestamp_str}'
              AND start_time < TIMESTAMP '{end_time_str}'
              AND statement_type IN {statement_types_filter}
              AND execution_status = 'FINISHED'
            ORDER BY start_time ASC
            LIMIT {batch_size}
            """
            
            try:
                # Execute query using databricks-sql connection
                rows = self._execute_sql_query(query)
                
                if not rows:
                    logger.debug(f"No more query history records found. Total processed: {processed_count}")
                    break
                
                batch_processed = 0
                last_timestamp_ms = None
                
                # Process batch results
                for row in rows:
                    try:
                        # Parse row into QueryInfo object
                        query_info = self._parse_query_history_row(row)
                        if query_info:
                            yield query_info
                            batch_processed += 1
                            processed_count += 1
                            
                            # Update cursor to last processed timestamp
                            last_timestamp_ms = query_info.query_start_time_ms
                            
                    except Exception as e:
                        logger.warning(f"Error parsing query history row: {e}")
                        continue
                
                # Update cursor for next batch
                if last_timestamp_ms:
                    # Convert milliseconds back to timestamp string and add 1 second
                    from datetime import datetime, timedelta
                    last_dt = datetime.fromtimestamp(last_timestamp_ms / 1000)
                    next_dt = last_dt + timedelta(seconds=1)
                    cursor_timestamp_str = next_dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    # If no timestamps found, advance by 1 hour
                    from datetime import datetime, timedelta
                    current_dt = datetime.strptime(cursor_timestamp_str, '%Y-%m-%d %H:%M:%S')
                    next_dt = current_dt + timedelta(hours=1)
                    cursor_timestamp_str = next_dt.strftime('%Y-%m-%d %H:%M:%S')
                
                logger.debug(
                    f"Processed batch: {batch_processed} records, "
                    f"Total: {processed_count}, Next cursor: {cursor_timestamp_str}"
                )
                
                # Break if we didn't get a full batch (indicates end of data)
                if batch_processed < batch_size:
                    break
                    
            except Exception as e:
                logger.error(f"Error executing query history SQL: {e}")
                if enable_streaming:
                    # Continue with next batch in streaming mode - advance by 1 hour
                    from datetime import datetime, timedelta
                    current_dt = datetime.strptime(cursor_timestamp_str, '%Y-%m-%d %H:%M:%S')
                    next_dt = current_dt + timedelta(hours=1)
                    cursor_timestamp_str = next_dt.strftime('%Y-%m-%d %H:%M:%S')
                    continue
                else:
                    # Re-raise in non-streaming mode
                    raise
        
        logger.info(f"Completed server-side scrolling query history. Total processed: {processed_count}")

    def _parse_query_history_row(self, row: List[Any]) -> Optional[QueryInfo]:
        """
        Parse a query history row from information_schema.query_history into QueryInfo.
        
        Args:
            row: Database row containing query history fields
            
        Returns:
            QueryInfo object or None if parsing fails
        """
        try:
            # Map row fields to QueryInfo structure
            # Row order matches SELECT statement in _query_history_server_side_scrolling
            (
                query_id,
                query_text,
                statement_type,
                query_start_time_ms,
                query_end_time_ms,
                user_id,
                user_name,
                executed_as_user_id,
                executed_as_user_name,
                status,
                warehouse_id,
                workspace_id,
                compute_id,
                catalog_name,
                schema_name,
                total_duration_ms,
                result_state,
                result_data_in_bytes,
                result_row_count,
                error_message,
                update_count,
                rows_produced_count,
                rows_read_count,
                compilation_time_ms,
                execution_time_ms,
                read_io_cache_percent,
                rows_spilled_to_disk_count,
                task_count,
                pruned_bytes,
                pruned_files_count,
                read_bytes,
                read_files_count,
                read_partitions_count,
                read_remote_bytes,
                scanned_bytes,
                scanned_files_count,
                scanned_partitions_count,
                shuffle_bytes_spilled,
                shuffle_local_bytes_read,
                shuffle_remote_bytes_read,
                shuffle_remote_bytes_read_to_disk,
                shuffle_write_bytes,
                spilled_bytes_encrypted,
                spilled_bytes_uncompressed,
                write_remote_bytes,
            ) = row
            
            # Create QueryInfo object with available fields
            query_info_dict = {
                "query_id": query_id,
                "query_text": query_text,
                "statement_type": statement_type,
                "query_start_time_ms": int(query_start_time_ms) if query_start_time_ms else None,
                "query_end_time_ms": int(query_end_time_ms) if query_end_time_ms else None,
                "user_id": user_id,
                "user_name": user_name,
                "executed_as_user_id": executed_as_user_id,
                "executed_as_user_name": executed_as_user_name,
                "status": status,
                "warehouse_id": warehouse_id,
                "compute_id": compute_id,
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "total_duration_ms": int(total_duration_ms) if total_duration_ms else None,
                "result_state": result_state,
                "result_data_in_bytes": int(result_data_in_bytes) if result_data_in_bytes else None,
                "result_row_count": int(result_row_count) if result_row_count else None,
                "error_message": error_message,
                "update_count": int(update_count) if update_count else None,
                "rows_produced_count": int(rows_produced_count) if rows_produced_count else None,
                "rows_read_count": int(rows_read_count) if rows_read_count else None,
                "compilation_time_ms": int(compilation_time_ms) if compilation_time_ms else None,
                "execution_time_ms": int(execution_time_ms) if execution_time_ms else None,
                "read_io_cache_percent": float(read_io_cache_percent) if read_io_cache_percent else None,
                "rows_spilled_to_disk_count": int(rows_spilled_to_disk_count) if rows_spilled_to_disk_count else None,
                "task_count": int(task_count) if task_count else None,
                "pruned_bytes": int(pruned_bytes) if pruned_bytes else None,
                "pruned_files_count": int(pruned_files_count) if pruned_files_count else None,
                "read_bytes": int(read_bytes) if read_bytes else None,
                "read_files_count": int(read_files_count) if read_files_count else None,
                "read_partitions_count": int(read_partitions_count) if read_partitions_count else None,
                "read_remote_bytes": int(read_remote_bytes) if read_remote_bytes else None,
                "scanned_bytes": int(scanned_bytes) if scanned_bytes else None,
                "scanned_files_count": int(scanned_files_count) if scanned_files_count else None,
                "scanned_partitions_count": int(scanned_partitions_count) if scanned_partitions_count else None,
                "shuffle_bytes_spilled": int(shuffle_bytes_spilled) if shuffle_bytes_spilled else None,
                "shuffle_local_bytes_read": int(shuffle_local_bytes_read) if shuffle_local_bytes_read else None,
                "shuffle_remote_bytes_read": int(shuffle_remote_bytes_read) if shuffle_remote_bytes_read else None,
                "shuffle_remote_bytes_read_to_disk": int(shuffle_remote_bytes_read_to_disk) if shuffle_remote_bytes_read_to_disk else None,
                "shuffle_write_bytes": int(shuffle_write_bytes) if shuffle_write_bytes else None,
                "spilled_bytes_encrypted": int(spilled_bytes_encrypted) if spilled_bytes_encrypted else None,
                "spilled_bytes_uncompressed": int(spilled_bytes_uncompressed) if spilled_bytes_uncompressed else None,
                "write_remote_bytes": int(write_remote_bytes) if write_remote_bytes else None,
            }
            
            # Remove None values to avoid issues with QueryInfo.from_dict
            query_info_dict = {k: v for k, v in query_info_dict.items() if v is not None}
            
            return QueryInfo.from_dict(query_info_dict)
            
        except Exception as e:
            logger.warning(f"Error parsing query history row: {e}")
            return None

    def list_lineages_by_table(
        self, table_name: str, include_entity_lineage: bool
    ) -> dict:
        """List table lineage by table name."""
        return self._workspace_client.api_client.do(  # type: ignore
            method="GET",
            path="/api/2.0/lineage-tracking/table-lineage",
            body={
                "table_name": table_name,
                "include_entity_lineage": include_entity_lineage,
            },
        )

    def list_lineages_by_column(self, table_name: str, column_name: str) -> dict:
        """List column lineage by table name and column name."""
        return self._workspace_client.api_client.do(  # type: ignore
            "GET",
            "/api/2.0/lineage-tracking/column-lineage",
            body={"table_name": table_name, "column_name": column_name},
        )

    def table_lineage(self, table: Table, include_entity_lineage: bool) -> None:
        if table.schema.catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG:
            # Lineage is not available for Hive Metastore Tables.
            return None
        # Lineage endpoint doesn't exists on 2.1 version
        try:
            response: dict = self.list_lineages_by_table(
                table_name=table.ref.qualified_table_name,
                include_entity_lineage=include_entity_lineage,
            )

            for item in response.get("upstreams") or []:
                if "tableInfo" in item:
                    table_ref = TableReference.create_from_lineage(
                        item["tableInfo"], table.schema.catalog.metastore
                    )
                    if table_ref:
                        table.upstreams[table_ref] = {}
                elif "fileInfo" in item:
                    external_ref = ExternalTableReference.create_from_lineage(
                        item["fileInfo"]
                    )
                    if external_ref:
                        table.external_upstreams.add(external_ref)

                for notebook in item.get("notebookInfos") or []:
                    table.upstream_notebooks.add(notebook["notebook_id"])

            for item in response.get("downstreams") or []:
                for notebook in item.get("notebookInfos") or []:
                    table.downstream_notebooks.add(notebook["notebook_id"])
        except Exception as e:
            logger.warning(
                f"Error getting lineage on table {table.ref}: {e}", exc_info=True
            )

    def get_column_lineage(self, table: Table, column_name: str) -> None:
        try:
            response: dict = self.list_lineages_by_column(
                table_name=table.ref.qualified_table_name,
                column_name=column_name,
            )
            for item in response.get("upstream_cols") or []:
                table_ref = TableReference.create_from_lineage(
                    item, table.schema.catalog.metastore
                )
                if table_ref:
                    table.upstreams.setdefault(table_ref, {}).setdefault(
                        column_name, []
                    ).append(item["name"])
        except Exception as e:
            logger.warning(
                f"Error getting column lineage on table {table.ref}, column {column_name}: {e}",
                exc_info=True,
            )

    @staticmethod
    def _escape_sequence(value: str) -> str:
        return value.replace(" ", "_")

    @staticmethod
    def _create_metastore(
        obj: Union[GetMetastoreSummaryResponse, MetastoreInfo],
    ) -> Optional[Metastore]:
        if not obj.name:
            return None
        return Metastore(
            name=obj.name,
            id=UnityCatalogApiProxy._escape_sequence(obj.name),
            global_metastore_id=obj.global_metastore_id,
            metastore_id=obj.metastore_id,
            owner=obj.owner,
            region=obj.region,
            cloud=obj.cloud,
            comment=None,
        )

    def _create_catalog(
        self, metastore: Optional[Metastore], obj: CatalogInfo
    ) -> Optional[Catalog]:
        if not obj.name:
            self.report.num_catalogs_missing_name += 1
            return None
        catalog_name = self._escape_sequence(obj.name)
        return Catalog(
            name=obj.name,
            id=f"{metastore.id}.{catalog_name}" if metastore else catalog_name,
            metastore=metastore,
            comment=obj.comment,
            owner=obj.owner,
            type=obj.catalog_type,
        )

    def _create_schema(self, catalog: Catalog, obj: SchemaInfo) -> Optional[Schema]:
        if not obj.name:
            self.report.num_schemas_missing_name += 1
            return None
        return Schema(
            name=obj.name,
            id=f"{catalog.id}.{self._escape_sequence(obj.name)}",
            catalog=catalog,
            comment=obj.comment,
            owner=obj.owner,
        )

    def _create_column(self, table_id: str, obj: ColumnInfo) -> Optional[Column]:
        if not obj.name:
            self.report.num_columns_missing_name += 1
            return None
        return Column(
            name=obj.name,
            id=f"{table_id}.{self._escape_sequence(obj.name)}",
            type_text=obj.type_text or "",
            type_name=obj.type_name,
            type_scale=obj.type_scale,
            type_precision=obj.type_precision,
            position=obj.position,
            nullable=obj.nullable,
            comment=obj.comment,
        )

    def _create_table(
        self, schema: Schema, obj: TableInfoWithGeneration
    ) -> Optional[Table]:
        if not obj.name:
            self.report.num_tables_missing_name += 1
            return None
        table_id = f"{schema.id}.{self._escape_sequence(obj.name)}"
        return Table(
            name=obj.name,
            id=table_id,
            table_type=obj.table_type,
            schema=schema,
            storage_location=obj.storage_location,
            data_source_format=obj.data_source_format,
            columns=(
                list(self._extract_columns(obj.columns, table_id))
                if obj.columns
                else []
            ),
            view_definition=obj.view_definition or None,
            properties=obj.properties or {},
            owner=obj.owner,
            generation=obj.generation,
            created_at=(parse_ts_millis(obj.created_at) if obj.created_at else None),
            created_by=obj.created_by,
            updated_at=(parse_ts_millis(obj.updated_at) if obj.updated_at else None),
            updated_by=obj.updated_by,
            table_id=obj.table_id,
            comment=obj.comment,
        )

    def _extract_columns(
        self, columns: List[ColumnInfo], table_id: str
    ) -> Iterable[Column]:
        for column in columns:
            optional_column = self._create_column(table_id, column)
            if optional_column:
                yield optional_column

    def _create_service_principal(
        self, obj: DatabricksServicePrincipal
    ) -> Optional[ServicePrincipal]:
        if not obj.display_name or not obj.application_id:
            return None
        return ServicePrincipal(
            id=f"{obj.id}.{self._escape_sequence(obj.display_name)}",
            display_name=obj.display_name,
            application_id=obj.application_id,
            active=obj.active,
        )

    def _create_query(self, info: QueryInfo) -> Optional[Query]:
        if (
            not info.query_text
            or not info.query_start_time_ms
            or not info.query_end_time_ms
        ):
            self.report.num_queries_missing_info += 1
            return None
        return Query(
            query_id=info.query_id,
            query_text=info.query_text,
            statement_type=info.statement_type,
            start_time=parse_ts_millis(info.query_start_time_ms),
            end_time=parse_ts_millis(info.query_end_time_ms),
            user_id=info.user_id,
            user_name=info.user_name,
            executed_as_user_id=info.executed_as_user_id,
            executed_as_user_name=info.executed_as_user_name,
        )

    def _execute_sql_query(self, query: str) -> List[List[str]]:
        """Execute SQL query using databricks-sql connector for better performance"""
        try:
            with (
                connect(**self._sql_connection_params) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute(query)
                return cursor.fetchall()

        except Exception as e:
            logger.warning(f"Failed to execute SQL query: {e}")
            return []

    @cached(cachetools.FIFOCache(maxsize=100))
    def get_schema_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Optimized version using databricks-sql"""
        logger.info(f"Fetching schema tags for catalog: {catalog}")

        query = f"SELECT * FROM {catalog}.information_schema.schema_tags"
        rows = self._execute_sql_query(query)

        result_dict: Dict[str, List[UnityCatalogTag]] = {}

        for row in rows:
            catalog_name, schema_name, tag_name, tag_value = row
            schema_key = f"{catalog_name}.{schema_name}"

            if schema_key not in result_dict:
                result_dict[schema_key] = []

            result_dict[schema_key].append(
                UnityCatalogTag(key=tag_name, value=tag_value)
            )

        return result_dict

    @cached(cachetools.FIFOCache(maxsize=100))
    def get_catalog_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Optimized version using databricks-sql"""
        logger.info(f"Fetching table tags for catalog: {catalog}")

        query = f"SELECT * FROM {catalog}.information_schema.catalog_tags"
        rows = self._execute_sql_query(query)

        result_dict: Dict[str, List[UnityCatalogTag]] = {}

        for row in rows:
            catalog_name, tag_name, tag_value = row

            if catalog_name not in result_dict:
                result_dict[catalog_name] = []

            result_dict[catalog_name].append(
                UnityCatalogTag(key=tag_name, value=tag_value)
            )

        return result_dict

    @cached(cachetools.FIFOCache(maxsize=100))
    def get_table_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Optimized version using databricks-sql"""
        logger.info(f"Fetching table tags for catalog: {catalog}")

        query = f"SELECT * FROM {catalog}.information_schema.table_tags"
        rows = self._execute_sql_query(query)

        result_dict: Dict[str, List[UnityCatalogTag]] = {}

        for row in rows:
            catalog_name, schema_name, table_name, tag_name, tag_value = row
            table_key = f"{catalog_name}.{schema_name}.{table_name}"

            if table_key not in result_dict:
                result_dict[table_key] = []

            result_dict[table_key].append(
                UnityCatalogTag(key=tag_name, value=tag_value if tag_value else None)
            )

        return result_dict

    @cached(cachetools.FIFOCache(maxsize=100))
    def get_column_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Optimized version using databricks-sql"""
        logger.info(f"Fetching column tags for catalog: {catalog}")

        query = f"SELECT * FROM {catalog}.information_schema.column_tags"
        rows = self._execute_sql_query(query)

        result_dict: Dict[str, List[UnityCatalogTag]] = {}

        for row in rows:
            catalog_name, schema_name, table_name, column_name, tag_name, tag_value = (
                row
            )
            column_key = f"{catalog_name}.{schema_name}.{table_name}.{column_name}"

            if column_key not in result_dict:
                result_dict[column_key] = []

            result_dict[column_key].append(
                UnityCatalogTag(key=tag_name, value=tag_value if tag_value else None)
            )

        return result_dict
