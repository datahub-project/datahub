"""
Manage the communication with DataBricks Server and provide equivalent dataclasses for dependent modules
"""
import logging
from dataclasses import dataclass
from typing import Iterable, List

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
class Table(CommonProperty):
    schema: Schema
    columns: List[Column]
    storage_location: str
    data_source_format: str
    table_type: str


def _escape_sequence(value: str) -> str:
    return value.replace(" ", "_")


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
        metastores: List[Metastore] = []
        response: dict = self._unity_catalog_api.list_metastores()

        if response.get("metastores") is None:
            LOGGER.info("Metastores not found")
            return metastores

        for obj in response["metastores"]:
            metastores.append(
                Metastore(
                    name=obj["name"],
                    id=_escape_sequence(obj["name"]),
                    metastore_id=obj["metastore_id"],
                    type="Metastore",
                )
            )
        # yield here to support paginated response later
        yield metastores

    def catalogs(self, metastore: Metastore) -> Iterable[List[Catalog]]:
        catalogs: List[Catalog] = []
        response: dict = self._unity_catalog_api.list_catalogs()
        if response.get("catalogs") is None:
            LOGGER.info("Catalogs not found")
            return catalogs

        for obj in response["catalogs"]:
            if obj["metastore_id"] == metastore.metastore_id:
                catalogs.append(
                    Catalog(
                        name=obj["name"],
                        id="{}.{}".format(
                            metastore.id,
                            _escape_sequence(obj["name"]),
                        ),
                        metastore=metastore,
                        type="Catalog",
                    )
                )
        yield catalogs

    def schemas(self, catalog: Catalog) -> Iterable[List[Schema]]:
        schemas: List[Schema] = []
        response: dict = self._unity_catalog_api.list_schemas(catalog_name=catalog.name)
        if response.get("schemas") is None:
            LOGGER.info("Schemas not found")
            return schemas

        for obj in response["schemas"]:
            schemas.append(
                Schema(
                    name=obj["name"],
                    id="{}.{}".format(
                        catalog.id,
                        _escape_sequence(obj["name"]),
                    ),
                    catalog=catalog,
                    type="Schema",
                )
            )

        yield schemas

    def tables(self, schema: Schema) -> Iterable[List[Table]]:
        tables: List[Table] = []
        response: dict = self._unity_catalog_api.list_tables(
            catalog_name=schema.catalog.name,
            schema_name=schema.name,
        )

        if response.get("tables") is None:
            LOGGER.info("Tables not found")
            return tables

        for obj in response["tables"]:
            table = Table(
                name=obj["name"],
                id="{}.{}".format(schema.id, _escape_sequence(obj["name"])),
                table_type=obj["table_type"],
                schema=schema,
                storage_location=obj["storage_location"],
                data_source_format=obj["data_source_format"],
                columns=[],
                type="Table",
            )
            for column in obj["columns"]:
                table.columns.append(
                    Column(
                        name=column["name"],
                        id="{}.{}".format(table.id, _escape_sequence(column["name"])),
                        type_text=column["type_text"],
                        type_name=SchemaFieldDataTypeClass(
                            type=DATA_TYPE_REGISTRY[column["type_name"]]()
                        ),
                        type_scale=column["type_scale"],
                        type_precision=column["type_precision"],
                        position=column["position"],
                        nullable=column["nullable"],
                        type="Column",
                    )
                )
            tables.append(table)

        yield tables
