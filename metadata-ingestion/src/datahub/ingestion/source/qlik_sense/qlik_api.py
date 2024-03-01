import logging
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    PERSONAL_SPACE_DICT,
    App,
    Chart,
    Item,
    QlikDataset,
    QlikTable,
    Sheet,
    Space,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import WebsocketConnection

# Logger instance
logger = logging.getLogger(__name__)


class QlikAPI:
    def __init__(self, config: QlikSourceConfig) -> None:
        self.spaces: Dict = {}
        self.users: Dict = {}
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
            }
        )
        self.rest_api_url = f"https://{self.config.tenant_hostname}/api/v1"
        # Test connection by fetching list of api keys
        logger.info("Trying to connect to {}".format(self.rest_api_url))
        self.session.get(f"{self.rest_api_url}/api-keys").raise_for_status()

    def _log_http_error(self, message: str) -> Any:
        logger.warning(message)
        _, e, _ = sys.exc_info()
        if isinstance(e, requests.exceptions.HTTPError):
            logger.warning(f"HTTP status-code = {e.response.status_code}")
        logger.debug(msg=message, exc_info=e)
        return e

    def get_spaces(self) -> List[Space]:
        spaces: List[Space] = []
        try:
            response = self.session.get(f"{self.rest_api_url}/spaces")
            response.raise_for_status()
            for space_dict in response.json()[Constant.DATA]:
                space = Space.parse_obj(space_dict)
                spaces.append(space)
                self.spaces[space.id] = space.name
            # Add personal space entity
            spaces.append(Space.parse_obj(PERSONAL_SPACE_DICT))
            self.spaces[PERSONAL_SPACE_DICT[Constant.ID]] = PERSONAL_SPACE_DICT[
                Constant.NAME
            ]
        except Exception as e:
            self._log_http_error(message=f"Unable to fetch spaces. Exception: {e}")
        return spaces

    def _get_dataset(self, dataset_id: str, item_id: str) -> Optional[QlikDataset]:
        try:
            response = self.session.get(f"{self.rest_api_url}/data-sets/{dataset_id}")
            response.raise_for_status()
            response_dict = response.json()
            response_dict[Constant.ITEMID] = item_id
            return QlikDataset.parse_obj(response_dict)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch dataset with id {dataset_id}. Exception: {e}"
            )
        return None

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
            self._log_http_error(
                message=f"Unable to fetch user with id {user_id}. Exception: {e}"
            )
        return None

    def _get_chart(
        self,
        websocket_connection: WebsocketConnection,
        chart_id: str,
        sheet_id: str,
    ) -> Optional[Chart]:
        try:
            websocket_connection.websocket_send_request(
                method="GetChild", params={"qId": chart_id}
            )
            response = websocket_connection.websocket_send_request(method="GetLayout")
            return Chart.parse_obj(response[Constant.QLAYOUT])
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch chart {chart_id} of sheet {sheet_id}. Exception: {e}"
            )
        return None

    def _get_sheet(
        self,
        websocket_connection: WebsocketConnection,
        sheet_id: str,
    ) -> Optional[Sheet]:
        try:
            websocket_connection.websocket_send_request(
                method="GetObject", params={"qId": sheet_id}
            )
            response = websocket_connection.websocket_send_request(method="GetLayout")
            sheet_dict = response[Constant.QLAYOUT]
            if Constant.OWNERID not in sheet_dict[Constant.QMETA]:
                # That means sheet is private sheet
                return None
            sheet = Sheet.parse_obj(sheet_dict[Constant.QMETA])
            for i, chart_dict in enumerate(
                sheet_dict[Constant.QCHILDLIST][Constant.QITEMS]
            ):
                chart = self._get_chart(
                    websocket_connection,
                    chart_dict[Constant.QINFO][Constant.QID],
                    sheet_id,
                )
                if chart:
                    if not chart.title:
                        chart.title = f"Object {i+1} of Sheet '{sheet.title}'"
                    sheet.charts.append(chart)
                websocket_connection.handle.pop()
            return sheet
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sheet with id {sheet_id}. Exception: {e}"
            )
        return None

    def _add_qri_of_tables(self, tables: List[QlikTable], app_id: str) -> None:
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
            self._log_http_error(
                message=f"Unable to add QRI for tables of app {app_id}. Exception: {e}"
            )

    def _get_app_used_tables(
        self, websocket_connection: WebsocketConnection, app_id: str
    ) -> List[QlikTable]:
        tables: List[QlikTable] = []
        try:
            response = websocket_connection.websocket_send_request(
                method="GetObject",
                params=["LoadModel"],
            )
            if not response[Constant.QRETURN][Constant.QTYPE]:
                return []
            response = websocket_connection.websocket_send_request(method="GetLayout")
            for table_dict in response[Constant.QLAYOUT][Constant.TABLES]:
                tables.append(QlikTable.parse_obj(table_dict))
            websocket_connection.handle.pop()
            self._add_qri_of_tables(tables, app_id)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch tables used by app {app_id}. Exception: {e}"
            )
        return tables

    def _get_app_sheets(
        self, websocket_connection: WebsocketConnection, app_id: str
    ) -> List[Sheet]:
        sheets: List[Sheet] = []
        try:
            response = websocket_connection.websocket_send_request(
                method="GetObjects",
                params={
                    "qOptions": {
                        "qTypes": ["sheet"],
                    }
                },
            )
            for sheet_dict in response[Constant.QLIST]:
                sheet = self._get_sheet(
                    websocket_connection=websocket_connection,
                    sheet_id=sheet_dict[Constant.QINFO][Constant.QID],
                )
                if sheet:
                    sheets.append(sheet)
                websocket_connection.handle.pop()
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sheets for app {app_id}. Exception: {e}"
            )
        return sheets

    def _get_app(self, app_id: str) -> Optional[App]:
        try:
            websocket_connection = WebsocketConnection(
                self.config.tenant_hostname, self.config.api_key, app_id
            )
            websocket_connection.websocket_send_request(
                method="OpenDoc",
                params={"qDocName": app_id},
            )
            response = websocket_connection.websocket_send_request(
                method="GetAppLayout"
            )
            app = App.parse_obj(response[Constant.QLAYOUT])
            app.sheets = self._get_app_sheets(websocket_connection, app_id)
            app.tables = self._get_app_used_tables(websocket_connection, app_id)
            websocket_connection.close_websocket()
            return app
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch app with id {app_id}. Exception: {e}"
            )
        return None

    def get_items(self) -> List[Item]:
        items: List[Item] = []
        try:
            url = f"{self.rest_api_url}/items"
            while True:
                response = self.session.get(url)
                response.raise_for_status()
                response_dict = response.json()
                for item in response_dict[Constant.DATA]:
                    # spaceId none indicates item present in personal space
                    if not item.get(Constant.SPACEID):
                        item[Constant.SPACEID] = Constant.PERSONAL_SPACE_ID
                    if self.config.space_pattern.allowed(
                        self.spaces[item[Constant.SPACEID]]
                    ):
                        resource_type = item[Constant.RESOURCETYPE]
                        if resource_type == Constant.APP:
                            app = self._get_app(app_id=item[Constant.RESOURCEID])
                            if app:
                                items.append(app)
                        elif resource_type == Constant.DATASET:
                            dataset = self._get_dataset(
                                dataset_id=item[Constant.RESOURCEID],
                                item_id=item[Constant.ID],
                            )
                            if dataset:
                                items.append(dataset)
                if Constant.NEXT in response_dict[Constant.LINKS]:
                    url = response_dict[Constant.LINKS][Constant.NEXT][Constant.HREF]
                else:
                    break

        except Exception as e:
            self._log_http_error(message=f"Unable to fetch items. Exception: {e}")
        return items
