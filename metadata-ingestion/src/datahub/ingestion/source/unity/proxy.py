"""
Manage the communication with DataBricks Server and provide equivalent dataclasses for dependent modules
"""
import datetime
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.unity_catalog.api import UnityCatalogApi

from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

logger: logging.Logger = logging.getLogger(__name__)

# Supported types are available at
# https://api-docs.databricks.com/rest/latest/unity-catalog-api-specification-2-1.html?_ga=2.151019001.1795147704.1666247755-2119235717.1666247755

DATA_TYPE_REGISTRY: dict = {
    "BOOLEAN": BooleanTypeClass,
    "BYTE": BytesTypeClass,
    "DATE": DateTypeClass,
    "SHORT": NumberTypeClass,
    "INT": NumberTypeClass,
    "LONG": NumberTypeClass,
    "FLOAT": NumberTypeClass,
    "DOUBLE": NumberTypeClass,
    "TIMESTAMP": TimeTypeClass,
    "STRING": StringTypeClass,
    "BINARY": BytesTypeClass,
    "DECIMAL": NumberTypeClass,
    "INTERVAL": TimeTypeClass,
    "ARRAY": ArrayTypeClass,
    "STRUCT": RecordTypeClass,
    "MAP": MapTypeClass,
    "CHAR": StringTypeClass,
    "NULL": NullTypeClass,
}


@dataclass
class CommonProperty:
    id: str
    name: str
    type: str
    comment: Optional[str]


@dataclass
class Metastore(CommonProperty):
    metastore_id: str


@dataclass
class Catalog(CommonProperty):
    metastore: Metastore


@dataclass
class Schema(CommonProperty):
    catalog: Catalog


@dataclass
class Column(CommonProperty):
    type_text: str
    type_name: SchemaFieldDataTypeClass
    type_precision: int
    type_scale: int
    position: int
    nullable: bool
    comment: Optional[str]


@dataclass
class ColumnLineage:
    source: str
    destination: str


@dataclass
class Table(CommonProperty):
    schema: Schema
    columns: List[Column]
    storage_location: Optional[str]
    data_source_format: Optional[str]
    comment: Optional[str]
    table_type: str
    owner: str
    generation: int
    created_at: datetime.datetime
    created_by: str
    updated_at: Optional[datetime.datetime]
    updated_by: Optional[str]
    table_id: str
    view_definition: Optional[str]
    properties: Dict[str, str]
    upstreams: Dict[str, Dict[str, List[str]]] = field(default_factory=dict)

    # lineage: Optional[Lineage]


class UnityCatalogApiProxy:
    _unity_catalog_api: UnityCatalogApi
    _workspace_url: str
    report: UnityCatalogReport

    def __init__(
        self, workspace_url: str, personal_access_token: str, report: UnityCatalogReport
    ):
        self._unity_catalog_api = UnityCatalogApi(
            ApiClient(
                host=workspace_url,
                token=personal_access_token,
            )
        )
        self._workspace_url = workspace_url
        self.report = report

    def check_connectivity(self) -> bool:
        self._unity_catalog_api.list_metastores()
        return True

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
            logger.info(f"Tables not found for schema {schema.name}")
            return []

        for table in response["tables"]:
            yield self._create_table(schema=schema, obj=table)

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
                f"{item['catalog_name']}.{item['schema_name']}.{item['name']}": {}
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
                        table_name = f"{item['catalog_name']}.{item['schema_name']}.{item['table_name']}"
                        col_name = item["name"]
                        if not table.upstreams.get(table_name):
                            table.upstreams[table_name] = {column.name: [col_name]}
                        else:
                            if column.name in table.upstreams[table_name]:
                                table.upstreams[table_name][column.name].append(
                                    col_name
                                )
                            else:
                                table.upstreams[table_name][column.name] = [col_name]

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
            owner=obj["owner"],
            generation=obj["generation"],
            created_at=datetime.datetime.utcfromtimestamp(obj["created_at"] / 1000),
            created_by=obj["created_by"],
            updated_at=datetime.datetime.utcfromtimestamp(obj["updated_at"] / 1000)
            if "updated_at" in obj
            else None,
            updated_by=obj.get("updated_by", None),
            table_id=obj["table_id"],
            comment=obj.get("comment"),
        )
