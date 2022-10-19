from typing import List, Iterable

import requests
import logging

from dataclasses import dataclass
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
    type: str = "Metastore"


@dataclass
class Catalog(CommonProperty):
    metastore: Metastore
    type: str = "Catalog"


@dataclass
class Schema(CommonProperty):
    catalog: Catalog
    type: str = "Schema"

@dataclass
class Table(CommonProperty):
    schema: Schema
    type: str = "Table"


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
                    id=obj["metastore_id"]
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
                        id="{}_{}".format(
                            metastore.id,
                            obj["name"],
                        ),
                        metastore=metastore,
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
                    id="{}_{}".format(
                        catalog.id,
                        obj["name"],
                    ),
                    catalog=catalog,
                )
            )

        yield schemas

    def tables(self, schema: Schema):
        table: List[Table] = []
        response: dict =