import logging
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    PERSONAL_SPACE_DICT,
    App,
    Item,
    QlikDataset,
    QlikTable,
    Space,
)
from datahub.ingestion.source.qlik_sense.qlik_base_api import (
    IsUnchanged,
    QlikBaseAPI,
    ReadErrors,
    log_read_error,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    SPACE_SEPARATOR,
    DataConnectionInfo,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import (
    EngineError,
    WebsocketConnection,
)

# Logger instance
logger = logging.getLogger(__name__)


class QlikAPI(QlikBaseAPI):
    def __init__(self, config: QlikSourceConfig) -> None:
        super().__init__(config)
        if not config.tenant_hostname or config.api_key is None:
            raise ValueError("Qlik Cloud requires tenant_hostname and api_key")
        self.tenant_hostname = config.tenant_hostname
        self.api_key = config.api_key.get_secret_value()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )
        # Qlik Cloud rate-limits the tenant, and parallel app reads make 429s
        # likely. The lineage overview call is a POST but only reads.
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429],
            allowed_methods=None,
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.rest_api_url = f"https://{self.tenant_hostname}/api/v1"
        # Test connection by fetching list of api keys
        logger.info(f"Trying to connect to {self.rest_api_url}")
        self.session.get(f"{self.rest_api_url}/api-keys").raise_for_status()

    def _paginate(self, url: str) -> Iterable[Dict]:
        next_url: Optional[str] = url
        while next_url:
            response = self.session.get(next_url)
            response.raise_for_status()
            page = response.json()
            yield from page.get(Constant.DATA) or []
            next_url = (page.get(Constant.LINKS, {}).get(Constant.NEXT) or {}).get(
                Constant.HREF
            )

    def get_spaces(self) -> List[Space]:
        spaces: List[Space] = []
        try:
            for space_dict in self._paginate(f"{self.rest_api_url}/spaces"):
                space = Space.model_validate(space_dict)
                spaces.append(space)
                self.spaces[space.id] = space.name
            # Add personal space entity
            spaces.append(Space.model_validate(PERSONAL_SPACE_DICT))
            self.spaces[Constant.PERSONAL_SPACE_ID] = Constant.PERSONAL_SPACE_NAME
        except Exception as e:
            self._record_unread("spaces", f"Unable to fetch spaces. Exception: {e}", e)
        return spaces

    def _get_dataset(self, dataset_id: str, item_id: str) -> QlikDataset:
        response = self.session.get(f"{self.rest_api_url}/data-sets/{dataset_id}")
        response.raise_for_status()
        response_dict = response.json()
        response_dict[Constant.ITEMID] = item_id
        return QlikDataset.model_validate(response_dict)

    def get_user_name(self, user_id: str) -> Optional[str]:
        try:
            if user_id in self.users:
                # To avoid fetching same user details again
                return self.users[user_id]
            else:
                response = self.session.get(f"{self.rest_api_url}/users/{user_id}")
                response.raise_for_status()
                user_name = response.json()[Constant.NAME]
                self.users[user_id] = user_name
                return user_name
        except Exception as e:
            log_read_error(f"Unable to fetch user with id {user_id}. Exception: {e}")
        return None

    def _open_engine(self, app_id: str) -> WebsocketConnection:
        return WebsocketConnection.for_cloud(self.tenant_hostname, self.api_key, app_id)

    def _sheet_meta(self, meta: Dict, sheet_id: str) -> Optional[Dict]:
        # A sheet without an owner is a private sheet.
        return meta if Constant.OWNERID in meta else None

    def _add_qri_of_tables(
        self, tables: List[QlikTable], app_id: str, errors: ReadErrors
    ) -> None:
        table_qri_dict: Dict[str, str] = {}
        app_qri = quote(f"qri:app:sense://{app_id}", safe="")
        try:
            response = self.session.get(
                f"{self.rest_api_url}/lineage-graphs/nodes/{app_qri}/actions/expand?node={app_qri}&level=TABLE"
            )
            response.raise_for_status()
            for table_node_qri in response.json()[Constant.GRAPH][Constant.NODES]:
                table_node_qri = quote(table_node_qri, safe="")
                response = self.session.get(
                    f"{self.rest_api_url}/lineage-graphs/nodes/{app_qri}/actions/expand?node={table_node_qri}&level=FIELD"
                )
                response.raise_for_status()
                field_nodes_qris = list(
                    response.json()[Constant.GRAPH][Constant.NODES].keys()
                )
                for field_node_qri in field_nodes_qris:
                    response = self.session.post(
                        f"{self.rest_api_url}/lineage-graphs/nodes/{app_qri}/overview",
                        json=[field_node_qri],
                    )
                    response.raise_for_status()
                    # Some fields might not have lineage overview, in that case status code is 207
                    if response.status_code == 200:
                        for each_lineage in response.json()[Constant.RESOURCES][0][
                            Constant.LINEAGE
                        ]:
                            table_name = (
                                each_lineage[Constant.TABLELABEL]
                                .replace('"', "")
                                .split(".")[-1]
                            )
                            table_qri_dict[table_name] = each_lineage[Constant.TABLEQRI]
                        break
            for table in tables:
                if table.tableName in table_qri_dict:
                    table.tableQri = table_qri_dict[table.tableName]
        except Exception as e:
            errors.record(
                f"Unable to add QRI for tables of app {app_id}. Exception: {e}"
            )

    def _get_app_used_tables(
        self,
        websocket_connection: WebsocketConnection,
        app_id: str,
        errors: ReadErrors,
    ) -> List[QlikTable]:
        tables: List[QlikTable] = []
        # The app handle must be on top again afterwards, as GetScript runs on it.
        with websocket_connection.restore_handle():
            try:
                response = websocket_connection.websocket_send_request(
                    method="GetObject",
                    params=["LoadModel"],
                )
                if not response[Constant.QRETURN][Constant.QTYPE]:
                    logger.debug(f"App {app_id} has no LoadModel object")
                    return []
                response = websocket_connection.websocket_send_request(
                    method="GetLayout"
                )
                for table_dict in response[Constant.QLAYOUT][Constant.TABLES]:
                    tables.append(QlikTable.model_validate(table_dict))
                self._add_qri_of_tables(tables, app_id, errors)
                logger.debug(
                    f"App {app_id} LoadModel tables: "
                    f"{[(t.tableName, t.type.value, t.dataconnectorPlatform, t.tableQri) for t in tables]}"
                )
            except Exception as e:
                errors.record(
                    f"Unable to fetch tables used by app {app_id}. Exception: {e}"
                )
        return tables

    def get_data_connections(self) -> Dict[str, DataConnectionInfo]:
        # Scripts name a connection as `<space>:<connection>`, or by its bare name
        # when it is in the app's own space. Several spaces can hold a connection
        # with the same name, so a bare name is only kept when it is unique.
        connections: Dict[str, DataConnectionInfo] = {}
        by_name: Dict[str, List[DataConnectionInfo]] = {}
        try:
            for connection in self._paginate(f"{self.rest_api_url}/data-connections"):
                name = connection.get(Constant.QNAME)
                if not name:
                    continue
                info = self._infer_connection(
                    name,
                    connection,
                    (
                        Constant.DATASOURCEID,
                        Constant.QCONNECTSTATEMENT,
                        Constant.QTYPE,
                    ),
                )
                # A personal connection has no space, like a personal app.
                space_name = self.spaces.get(
                    connection.get(Constant.SPACE) or Constant.PERSONAL_SPACE_ID
                )
                if space_name:
                    connections[f"{space_name}{SPACE_SEPARATOR}{name}"] = info
                by_name.setdefault(name, []).append(info)
        except Exception as e:
            self._record_unread(
                "data connections",
                f"Unable to fetch data connections. Exception: {e}",
                e,
            )
        for name, infos in by_name.items():
            if len(infos) == 1:
                connections.setdefault(name, infos[0])
        logger.debug(f"Qlik Cloud data connections: {connections}")
        return connections

    def _get_app(self, app_id: str) -> App:
        errors = ReadErrors()
        websocket_connection = self._open_engine(app_id)
        try:
            websocket_connection.websocket_send_request(
                method="OpenDoc",
                params={"qDocName": app_id},
            )
            response = websocket_connection.websocket_send_request(
                method="GetAppLayout"
            )
            app = App.model_validate(response[Constant.QLAYOUT])
            app.sheets = self._get_app_sheets(websocket_connection, app_id, errors)
            app.tables = self._get_app_used_tables(websocket_connection, app_id, errors)
            if not app.tables or not all(table.tableQri for table in app.tables):
                try:
                    app.script = websocket_connection.get_script()
                except Exception as e:
                    if isinstance(e, EngineError) and e.access_denied:
                        # Cloud only lets Owner, Can manage and Can operate read
                        # the load script, so a denied read leaves the rest of
                        # the app complete.
                        log_read_error(
                            f"Access denied to load script of app {app_id}: {e}"
                        )
                        app.script_read_failed = True
                    else:
                        errors.record(
                            f"Unable to fetch load script of app {app_id}. Exception: {e}"
                        )
            app.read_errors = errors.messages
            return app
        finally:
            self._close_engine(websocket_connection, app_id)

    def _list_items(self, space_id: str) -> Iterable[Dict]:
        try:
            for item in self._paginate(f"{self.rest_api_url}/items?spaceId={space_id}"):
                # spaceId none indicates item present in personal space
                if not item.get(Constant.SPACEID):
                    item[Constant.SPACEID] = Constant.PERSONAL_SPACE_ID
                if self.config.space_pattern.allowed(
                    self.spaces[item[Constant.SPACEID]]
                ):
                    yield item
        except Exception as e:
            self._record_unread(
                f"items of space {space_id}",
                f"Unable to fetch items. Exception: {e}",
                e,
            )

    def _load_item(
        self, item: Dict, is_unchanged: Optional[IsUnchanged]
    ) -> Optional[Item]:
        resource_type = item[Constant.RESOURCETYPE]
        resource_id = item[Constant.RESOURCEID]
        try:
            if resource_type == Constant.APP:
                return self._get_app(resource_id)
            if resource_type == Constant.DATASET:
                return self._get_dataset(resource_id, item[Constant.ID])
        except Exception as e:
            self._record_unread(
                resource_id,
                f"Unable to fetch {resource_type} with id {resource_id}. Exception: {e}",
                e,
            )
        return None
