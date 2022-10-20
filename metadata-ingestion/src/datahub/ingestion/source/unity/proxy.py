"""
Manage the communication with DataBricks Server and provide equivalent dataclasses for dependent modules
"""
import logging
from dataclasses import dataclass
from typing import Iterable, List

import requests
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.unity_catalog.api import UnityCatalogApi

LOGGER = logging.getLogger(__name__)


@dataclass
class CommonProperty:
    id: str
    name: str
    type: str


@dataclass
class Metastore(CommonProperty):
    pass


@dataclass
class Catalog(CommonProperty):
    metastore: Metastore


@dataclass
class Schema(CommonProperty):
    catalog: Catalog


@dataclass
class Column(CommonProperty):
    type_text: str
    type_name: str
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

        for obj in response.get("metastores"):
            metastores.append(
                Metastore(
                    name=obj["name"],
                    id=obj["metastore_id"],
                    type="Metastore",
                )
            )
        # yield here to support paginated response later
        yield metastores

    def catalogs(self, metastore: Metastore) -> Iterable[List[Metastore]]:
        catalogs: List[Catalog] = []
        response: dict = self._unity_catalog_api.list_catalogs()
        if response.get("catalogs") is None:
            LOGGER.info("Catalogs not found")
            return catalogs

        for obj in response.get("catalogs"):
            if obj["metastore_id"] == metastore.id:
                catalogs.append(
                    Catalog(
                        name=obj["name"],
                        id="{}.{}".format(
                            metastore.id,
                            obj["name"],
                        ),
                        metastore=metastore,
                        type="Catalog",
                    )
                )
        yield catalogs

    def schemas(self, catalog: Catalog) -> Iterable[List[Metastore]]:
        schemas: List[Schema] = []
        response: dict = self._unity_catalog_api.list_schemas(catalog_name=catalog.name)
        if response.get("schemas") is None:
            LOGGER.info("Schemas not found")
            return schemas

        for obj in response.get("schemas"):
            schemas.append(
                Schema(
                    name=obj["name"],
                    id="{}.{}".format(
                        catalog.id,
                        obj["name"],
                    ),
                    catalog=catalog,
                    type="Schema",
                )
            )

        yield schemas

    def tables(self, schema: Schema) -> Iterable[List[Metastore]]:
        tables: List[Table] = []
        response: dict = self._unity_catalog_api.list_tables(
            catalog_name=schema.catalog.name,
            schema_name=schema.name,
        )

        if response.get("tables") is None:
            LOGGER.info("Tables not found")
            return tables

        for obj in response.get("tables"):
            table = Table(
                name=obj["name"],
                id="{}.{}".format(schema.id, obj["name"]),
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
                        id="{}.{}".format(table.id, column["name"]),
                        type_text=column["type_text"],
                        type_name=column["type_name"],
                        type_scale=column["type_scale"],
                        type_precision=column["type_precision"],
                        position=column["position"],
                        nullable=column["nullable"],
                        type="Column",
                    )
                )
            tables.append(table)

        yield tables
