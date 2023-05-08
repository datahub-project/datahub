"""
Manage the communication with DataBricks Server and provide equivalent dataclasses for dependent modules
"""
import dataclasses
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import patch

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import ApiClient
from databricks.sdk.service.sql import (
    QueryFilter,
    QueryInfo,
    QueryStatementType,
    QueryStatus,
)
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.unity_catalog.api import UnityCatalogApi

from datahub.ingestion.source.unity.proxy_types import (
    ALLOWED_STATEMENT_TYPES,
    DATA_TYPE_REGISTRY,
    Catalog,
    Column,
    Metastore,
    Query,
    Schema,
    ServicePrincipal,
    Table,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import SchemaFieldDataTypeClass

logger: logging.Logger = logging.getLogger(__name__)


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


class UnityCatalogApiProxy:
    _unity_catalog_api: UnityCatalogApi
    _workspace_client: WorkspaceClient
    _workspace_url: str
    report: UnityCatalogReport

    def __init__(
        self, workspace_url: str, personal_access_token: str, report: UnityCatalogReport
    ):
        self._workspace_client = WorkspaceClient(
            host=workspace_url, token=personal_access_token
        )
        self._unity_catalog_api = UnityCatalogApi(
            ApiClient(host=workspace_url, token=personal_access_token)
        )
        self._workspace_url = workspace_url
        self.report = report

    def check_connectivity(self) -> bool:
        self._unity_catalog_api.list_metastores()
        return True

    def assigned_metastore(self) -> Optional[Metastore]:
        response: dict = self._unity_catalog_api.get_metastore_summary()
        if response.get("metastore_id") is None:
            logger.info("Not found assigned metastore")
            return None
        return self._create_metastore(response)

    def metastores(self) -> Iterable[Metastore]:
        response: dict = self._unity_catalog_api.list_metastores()
        if response.get("metastores") is None:
            logger.info("Metastores not found")
            return []
        # yield here to support paginated response later
        for metastore in response["metastores"]:
            yield self._create_metastore(metastore)

    def catalogs(self, metastore: Metastore) -> Iterable[Catalog]:
        response: dict = self._unity_catalog_api.list_catalogs()
        num_catalogs: int = 0
        if response.get("catalogs") is None:
            logger.info(f"Catalogs not found for metastore {metastore.name}")
            return []

        for obj in response["catalogs"]:
            if obj["metastore_id"] == metastore.metastore_id:
                yield self._create_catalog(metastore, obj)
                num_catalogs += 1

        if num_catalogs == 0:
            logger.info(
                f"Catalogs not found for metastore where metastore_id is {metastore.metastore_id}"
            )

    def schemas(self, catalog: Catalog) -> Iterable[Schema]:
        response: dict = self._unity_catalog_api.list_schemas(
            catalog_name=catalog.name, name_pattern=None
        )
        if response.get("schemas") is None:
            logger.info(f"Schemas not found for catalog {catalog.name}")
            return []

        for schema in response["schemas"]:
            yield self._create_schema(catalog, schema)

    def tables(self, schema: Schema) -> Iterable[Table]:
        response: dict = self._unity_catalog_api.list_tables(
            catalog_name=schema.catalog.name,
            schema_name=schema.name,
            name_pattern=None,
        )

        if response.get("tables") is None:
            logger.info(
                f"Tables not found for schema {schema.catalog.name}.{schema.name}"
            )
            return []

        for table in response["tables"]:
            yield self._create_table(schema=schema, obj=table)

    def service_principals(self) -> Iterable[ServicePrincipal]:
        start_index = 1  # Unfortunately 1-indexed
        items_per_page = 0
        total_results = float("inf")
        while start_index + items_per_page <= total_results:
            response: dict = self._unity_catalog_api.client.client.perform_query(
                "GET", "/account/scim/v2/ServicePrincipals"
            )
            start_index = response["startIndex"]
            items_per_page = response["itemsPerPage"]
            total_results = response["totalResults"]
            for principal in response["Resources"]:
                yield self._create_service_principal(principal)

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
                "statuses": [QueryStatus.FINISHED.value],
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

    def list_lineages_by_table(self, table_name=None, headers=None):
        """
        List table lineage by table name
        """
        _data = {}
        if table_name is not None:
            _data["table_name"] = table_name

        return self._unity_catalog_api.client.client.perform_query(
            "GET",
            "/lineage-tracking/table-lineage/get",
            data=_data,
            headers=headers,
            version="2.0",
        )

    def list_lineages_by_column(self, table_name=None, column_name=None, headers=None):
        """
        List column lineage by table name and comlumn name
        """
        # Lineage endpoint doesn't exists on 2.1 version
        _data = {}
        if table_name is not None:
            _data["table_name"] = table_name
        if column_name is not None:
            _data["column_name"] = column_name

        return self._unity_catalog_api.client.client.perform_query(
            "GET",
            "/lineage-tracking/column-lineage/get",
            data=_data,
            headers=headers,
            version="2.0",
        )

    def table_lineage(self, table: Table) -> None:
        # Lineage endpoint doesn't exists on 2.1 version
        try:
            response: dict = self.list_lineages_by_table(
                table_name=f"{table.schema.catalog.name}.{table.schema.name}.{table.name}"
            )
            table.upstreams = {
                TableReference(
                    table.schema.catalog.metastore.id,
                    item["catalog_name"],
                    item["schema_name"],
                    item["name"],
                ): {}
                for item in response.get("upstream_tables", [])
            }
        except Exception as e:
            logger.error(f"Error getting lineage: {e}")

    def get_column_lineage(self, table: Table) -> None:
        try:
            table_lineage_response: dict = self.list_lineages_by_table(
                table_name=f"{table.schema.catalog.name}.{table.schema.name}.{table.name}"
            )
            if table_lineage_response:
                for column in table.columns:
                    response: dict = self.list_lineages_by_column(
                        table_name=f"{table.schema.catalog.name}.{table.schema.name}.{table.name}",
                        column_name=column.name,
                    )
                    for item in response.get("upstream_cols", []):
                        table_ref = TableReference(
                            table.schema.catalog.metastore.id,
                            item["catalog_name"],
                            item["schema_name"],
                            item["table_name"],
                        )
                        table.upstreams.setdefault(table_ref, {}).setdefault(
                            column.name, []
                        ).append(item["name"])

        except Exception as e:
            logger.error(f"Error getting lineage: {e}")

    @staticmethod
    def _escape_sequence(value: str) -> str:
        return value.replace(" ", "_")

    @staticmethod
    def _create_metastore(obj: Any) -> Metastore:
        return Metastore(
            name=obj["name"],
            id=UnityCatalogApiProxy._escape_sequence(obj["name"]),
            metastore_id=obj["metastore_id"],
            type="Metastore",
            comment=obj.get("comment"),
            owner=obj.get("owner"),
        )

    def _create_catalog(self, metastore: Metastore, obj: Any) -> Catalog:
        return Catalog(
            name=obj["name"],
            id="{}.{}".format(
                metastore.id,
                self._escape_sequence(obj["name"]),
            ),
            metastore=metastore,
            type="Catalog",
            comment=obj.get("comment"),
            owner=obj.get("owner"),
        )

    def _create_schema(self, catalog: Catalog, obj: Any) -> Schema:
        return Schema(
            name=obj["name"],
            id="{}.{}".format(
                catalog.id,
                self._escape_sequence(obj["name"]),
            ),
            catalog=catalog,
            type="Schema",
            comment=obj.get("comment"),
            owner=obj.get("owner"),
        )

    def _create_column(self, table_id: str, obj: Any) -> Column:
        return Column(
            name=obj["name"],
            id="{}.{}".format(table_id, self._escape_sequence(obj["name"])),
            type_text=obj["type_text"],
            type_name=SchemaFieldDataTypeClass(
                type=DATA_TYPE_REGISTRY[obj["type_name"]]()
            ),
            type_scale=obj["type_scale"],
            type_precision=obj["type_precision"],
            position=obj["position"],
            nullable=obj["nullable"],
            comment=obj.get("comment"),
            type="Column",
        )

    def _create_table(self, schema: Schema, obj: Any) -> Table:
        table_id: str = "{}.{}".format(schema.id, self._escape_sequence(obj["name"]))
        return Table(
            name=obj["name"],
            id=table_id,
            table_type=obj["table_type"],
            schema=schema,
            storage_location=obj.get("storage_location"),
            data_source_format=obj.get("data_source_format"),
            columns=[self._create_column(table_id, column) for column in obj["columns"]]
            if obj.get("columns") is not None
            else [],
            type="view" if str(obj["table_type"]).lower() == "view" else "table",
            view_definition=obj.get("view_definition", None),
            properties=obj.get("properties", {}),
            owner=obj.get("owner"),
            generation=obj["generation"],
            created_at=datetime.utcfromtimestamp(obj["created_at"] / 1000),
            created_by=obj["created_by"],
            updated_at=datetime.utcfromtimestamp(obj["updated_at"] / 1000)
            if "updated_at" in obj
            else None,
            updated_by=obj.get("updated_by", None),
            table_id=obj["table_id"],
            comment=obj.get("comment"),
        )

    def _create_service_principal(self, obj: dict) -> ServicePrincipal:
        display_name = obj["displayName"]
        return ServicePrincipal(
            id="{}.{}".format(obj["id"], self._escape_sequence(display_name)),
            display_name=display_name,
            application_id=obj["applicationId"],
            active=obj.get("active"),
        )

    @staticmethod
    def _create_query(info: QueryInfo) -> Query:
        return Query(
            query_id=info.query_id,
            query_text=info.query_text,
            statement_type=info.statement_type,
            start_time=datetime.utcfromtimestamp(info.query_start_time_ms / 1000),
            end_time=datetime.utcfromtimestamp(info.query_end_time_ms / 1000),
            user_id=info.user_id,
            user_name=info.user_name,
            executed_as_user_id=info.executed_as_user_id,
            executed_as_user_name=info.executed_as_user_name,
        )
