"""
Manage the communication with DataBricks Server and provide equivalent dataclasses for dependent modules
"""
import logging
from dataclasses import dataclass
from functools import partial
from typing import Any, Iterable, List, Optional

import requests
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.unity_catalog.api import UnityCatalogApi

from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

LOGGER = logging.getLogger(__name__)

# Supported types are available at
# https://api-docs.databricks.com/rest/latest/unity-catalog-api-specification-2-1.html?_ga=2.151019001.1795147704.1666247755-2119235717.1666247755

DATA_TYPE_REGISTRY: dict = {
    "BOOLEAN": BooleanTypeClass,
    "BYTE": BytesTypeClass,
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


@dataclass
class TableInfo(CommonProperty):
    pass


@dataclass
class Lineage:
    upstreams: TableInfo
    downstreams: TableInfo


@dataclass
class Table(CommonProperty):
    schema: Schema
    columns: List[Column]
    storage_location: Optional[str]
    data_source_format: Optional[str]
    table_type: str
    # lineage: Optional[Lineage]


def _escape_sequence(value: str) -> str:
    return value.replace(" ", "_")


def _create_metastore(obj: Any) -> Metastore:
    return Metastore(
        name=obj["name"],
        id=_escape_sequence(obj["name"]),
        metastore_id=obj["metastore_id"],
        type="Metastore",
    )


def _create_catalog(metastore: Metastore, obj: Any) -> Catalog:
    return Catalog(
        name=obj["name"],
        id="{}.{}".format(
            metastore.id,
            _escape_sequence(obj["name"]),
        ),
        metastore=metastore,
        type="Catalog",
    )


def _create_schema(catalog: Catalog, obj: Any) -> Schema:
    return Schema(
        name=obj["name"],
        id="{}.{}".format(
            catalog.id,
            _escape_sequence(obj["name"]),
        ),
        catalog=catalog,
        type="Schema",
    )


def _create_column(table_id: str, obj: Any) -> Column:
    return Column(
        name=obj["name"],
        id="{}.{}".format(table_id, _escape_sequence(obj["name"])),
        type_text=obj["type_text"],
        type_name=SchemaFieldDataTypeClass(type=DATA_TYPE_REGISTRY[obj["type_name"]]()),
        type_scale=obj["type_scale"],
        type_precision=obj["type_precision"],
        position=obj["position"],
        nullable=obj["nullable"],
        type="Column",
    )


def _create_table(schema: Schema, obj: Any) -> Table:
    table_id: str = "{}.{}".format(schema.id, _escape_sequence(obj["name"]))
    return Table(
        name=obj["name"],
        id=table_id,
        table_type=obj["table_type"],
        schema=schema,
        storage_location=obj.get("storage_location"),
        data_source_format=obj.get("data_source_format"),
        columns=list(map(partial(_create_column, table_id), obj["columns"]))
        if obj.get("columns") is not None
        else [],
        type="Table",
    )


class UnityCatalogApiProxy:
    _unity_catalog_api: UnityCatalogApi
    _workspace_url: str

    def __init__(self, workspace_url, personal_access_token):
        self._unity_catalog_api = UnityCatalogApi(
            ApiClient(
                host=workspace_url,
                token=personal_access_token,
            )
        )
        self._workspace_url = workspace_url

    def check_connectivity(self) -> bool:
        try:
            requests.get(self._workspace_url)
            return True
        except Exception as e:
            LOGGER.debug(e, exc_info=e)
        return False

    def metastores(self) -> Iterable[List[Metastore]]:
        response: dict = self._unity_catalog_api.list_metastores()
        if response.get("metastores") is None:
            LOGGER.info("Metastores not found")
            return []

        # yield here to support paginated response later
        yield list(map(_create_metastore, response["metastores"]))

    def catalogs(self, metastore: Metastore) -> Iterable[List[Catalog]]:
        response: dict = self._unity_catalog_api.list_catalogs()
        if response.get("catalogs") is None:
            LOGGER.info(f"Catalogs not found for metastore {metastore.name}")
            return []

        filtered_catalogs = [
            obj
            for obj in response["catalogs"]
            if obj["metastore_id"] == metastore.metastore_id
        ]
        if len(filtered_catalogs) == 0:
            LOGGER.info(
                f"Catalogs not found for metastore where metastore_id is {metastore.metastore_id}"
            )
            return []

        yield list(map(partial(_create_catalog, metastore), filtered_catalogs))

    def schemas(self, catalog: Catalog) -> Iterable[List[Schema]]:
        response: dict = self._unity_catalog_api.list_schemas(
            catalog_name=catalog.name, name_pattern=None
        )
        if response.get("schemas") is None:
            LOGGER.info(f"Schemas not found for catalog {catalog.name}")
            return []

        yield list(map(partial(_create_schema, catalog), response["schemas"]))

    def tables(self, schema: Schema) -> Iterable[List[Table]]:
        response: dict = self._unity_catalog_api.list_tables(
            catalog_name=schema.catalog.name,
            schema_name=schema.name,
            name_pattern=None,
        )

        if response.get("tables") is None:
            LOGGER.info(f"Tables not found for schema {schema.name}")
            return []

        tables: List[Table] = list(
            map(partial(_create_table, schema), response["tables"])
        )

        yield tables
