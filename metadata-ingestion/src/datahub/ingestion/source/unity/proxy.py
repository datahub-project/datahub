"""
Manage the communication with DataBricks Server and provide equivalent dataclasses for dependent modules
"""
import dataclasses
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Union
from unittest.mock import patch

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

import datahub
from datahub.ingestion.source.unity.proxy_profiling import (
    UnityCatalogProxyProfilingMixin,
)
from datahub.ingestion.source.unity.proxy_types import (
    ALLOWED_STATEMENT_TYPES,
    Catalog,
    Column,
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


class TableInfoWithGeneration(TableInfo):
    generation: Optional[int] = None

    @classmethod
    def as_dict(self) -> dict:
        return {**super().as_dict(), "generation": self.generation}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TableInfoWithGeneration":
        table_info = super().from_dict(d)
        table_info.generation = d.get("generation")
        return table_info


@dataclasses.dataclass
class QueryFilterWithStatementTypes(QueryFilter):
    statement_types: List[QueryStatementType] = dataclasses.field(default_factory=list)

    def as_dict(self) -> dict:
        return {**super().as_dict(), "statement_types": self.statement_types}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "QueryFilterWithStatementTypes":
        v = super().from_dict(d)
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
    ):
        self._workspace_client = WorkspaceClient(
            host=workspace_url,
            token=personal_access_token,
            product="datahub",
            product_version=datahub.nice_version_name(),
        )
        self.warehouse_id = warehouse_id or ""
        self.report = report

    def check_basic_connectivity(self) -> bool:
        return bool(self._workspace_client.catalogs.list())

    def assigned_metastore(self) -> Metastore:
        response = self._workspace_client.metastores.summary()
        return self._create_metastore(response)

    def catalogs(self, metastore: Optional[Metastore]) -> Iterable[Catalog]:
        response = self._workspace_client.catalogs.list()
        if not response:
            logger.info("Catalogs not found")
            return []
        for catalog in response:
            yield self._create_catalog(metastore, catalog)

    def schemas(self, catalog: Catalog) -> Iterable[Schema]:
        response = self._workspace_client.schemas.list(catalog_name=catalog.name)
        if not response:
            logger.info(f"Schemas not found for catalog {catalog.id}")
            return []
        for schema in response:
            yield self._create_schema(catalog, schema)

    def tables(self, schema: Schema) -> Iterable[Table]:
        with patch("databricks.sdk.service.catalog.TableInfo", TableInfoWithGeneration):
            response = self._workspace_client.tables.list(
                catalog_name=schema.catalog.name, schema_name=schema.name
            )
            if not response:
                logger.info(f"Tables not found for schema {schema.id}")
                return []
            for table in response:
                try:
                    yield self._create_table(schema, table)
                except Exception as e:
                    logger.warning(f"Error parsing table: {e}")
                    self.report.report_warning("table-parse", str(e))

    def service_principals(self) -> Iterable[ServicePrincipal]:
        for principal in self._workspace_client.service_principals.list():
            yield self._create_service_principal(principal)

    def workspace_notebooks(self) -> Iterable[Notebook]:
        for obj in self._workspace_client.workspace.list("/", recursive=True):
            if obj.object_type == ObjectType.NOTEBOOK:
                yield Notebook(
                    id=obj.object_id,
                    path=obj.path,
                    language=obj.language,
                    created_at=datetime.fromtimestamp(
                        obj.created_at / 1000, tz=timezone.utc
                    ),
                    modified_at=datetime.fromtimestamp(
                        obj.modified_at / 1000, tz=timezone.utc
                    ),
                )

    def query_history(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> Iterable[Query]:
        """Returns all queries that were run between start_time and end_time with relevant statement_type.

        Raises:
            DatabricksError: If the query history API returns an error.
        """
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
                yield self._create_query(query_info)
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

        response: dict = self._workspace_client.api_client.do(
            method, path, body={**body, "filter_by": filter_by.as_dict()}
        )
        while True:
            if "res" not in response or not response["res"]:
                return
            for v in response["res"]:
                yield QueryInfo.from_dict(v)
            response = self._workspace_client.api_client.do(
                method, path, body={**body, "page_token": response["next_page_token"]}
            )

    def list_lineages_by_table(
        self, table_name: str, include_entity_lineage: bool
    ) -> dict:
        """List table lineage by table name."""
        return self._workspace_client.api_client.do(
            method="GET",
            path="/api/2.0/lineage-tracking/table-lineage",
            body={
                "table_name": table_name,
                "include_entity_lineage": include_entity_lineage,
            },
        )

    def list_lineages_by_column(self, table_name: str, column_name: str) -> dict:
        """List column lineage by table name and column name."""
        return self._workspace_client.api_client.do(
            "GET",
            "/api/2.0/lineage-tracking/column-lineage",
            body={"table_name": table_name, "column_name": column_name},
        )

    def table_lineage(self, table: Table, include_entity_lineage: bool) -> None:
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
        obj: Union[GetMetastoreSummaryResponse, MetastoreInfo]
    ) -> Metastore:
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
    ) -> Catalog:
        catalog_name = self._escape_sequence(obj.name)
        return Catalog(
            name=obj.name,
            id=f"{metastore.id}.{catalog_name}" if metastore else catalog_name,
            metastore=metastore,
            comment=obj.comment,
            owner=obj.owner,
            type=obj.catalog_type,
        )

    def _create_schema(self, catalog: Catalog, obj: SchemaInfo) -> Schema:
        return Schema(
            name=obj.name,
            id=f"{catalog.id}.{self._escape_sequence(obj.name)}",
            catalog=catalog,
            comment=obj.comment,
            owner=obj.owner,
        )

    def _create_column(self, table_id: str, obj: ColumnInfo) -> Column:
        return Column(
            name=obj.name,
            id=f"{table_id}.{self._escape_sequence(obj.name)}",
            type_text=obj.type_text,
            type_name=obj.type_name,
            type_scale=obj.type_scale,
            type_precision=obj.type_precision,
            position=obj.position,
            nullable=obj.nullable,
            comment=obj.comment,
        )

    def _create_table(self, schema: Schema, obj: TableInfoWithGeneration) -> Table:
        table_id = f"{schema.id}.{self._escape_sequence(obj.name)}"
        return Table(
            name=obj.name,
            id=table_id,
            table_type=obj.table_type,
            schema=schema,
            storage_location=obj.storage_location,
            data_source_format=obj.data_source_format,
            columns=[
                self._create_column(table_id, column) for column in obj.columns or []
            ],
            view_definition=obj.view_definition or None,
            properties=obj.properties or {},
            owner=obj.owner,
            generation=obj.generation,
            created_at=datetime.fromtimestamp(obj.created_at / 1000, tz=timezone.utc),
            created_by=obj.created_by,
            updated_at=datetime.fromtimestamp(obj.updated_at / 1000, tz=timezone.utc)
            if obj.updated_at
            else None,
            updated_by=obj.updated_by,
            table_id=obj.table_id,
            comment=obj.comment,
        )

    def _create_service_principal(
        self, obj: DatabricksServicePrincipal
    ) -> ServicePrincipal:
        return ServicePrincipal(
            id=f"{obj.id}.{self._escape_sequence(obj.display_name)}",
            display_name=obj.display_name,
            application_id=obj.application_id,
            active=obj.active,
        )

    @staticmethod
    def _create_query(info: QueryInfo) -> Query:
        return Query(
            query_id=info.query_id,
            query_text=info.query_text,
            statement_type=info.statement_type,
            start_time=datetime.fromtimestamp(
                info.query_start_time_ms / 1000, tz=timezone.utc
            ),
            end_time=datetime.fromtimestamp(
                info.query_end_time_ms / 1000, tz=timezone.utc
            ),
            user_id=info.user_id,
            user_name=info.user_name,
            executed_as_user_id=info.executed_as_user_id,
            executed_as_user_name=info.executed_as_user_name,
        )
