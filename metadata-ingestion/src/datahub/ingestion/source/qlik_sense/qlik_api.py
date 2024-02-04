import logging
import sys
from typing import Any, Dict, List, Optional

import requests

from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    PERSONAL_SPACE_DICT,
    App,
    Chart,
    Item,
    QlikAppDataset,
    QlikDataset,
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
        except Exception:
            self._log_http_error(message="Unable to fetch spaces")
        return spaces

    def _get_dataset(self, dataset_id: str) -> Optional[QlikDataset]:
        try:
            response = self.session.get(f"{self.rest_api_url}/data-sets/{dataset_id}")
            response.raise_for_status()
            return QlikDataset.parse_obj(response.json())
        except Exception:
            self._log_http_error(
                message=f"Unable to fetch dataset with id {dataset_id}"
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
        except Exception:
            self._log_http_error(message=f"Unable to fetch user with id {user_id}")
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
            sheet = Sheet.parse_obj(sheet_dict[Constant.QMETA])
            for chart_dict in sheet_dict[Constant.QCHILDLIST][Constant.QITEMS]:
                sheet.charts.append(Chart.parse_obj(chart_dict[Constant.QINFO]))
            return sheet
        except Exception:
            self._log_http_error(message=f"Unable to fetch sheet with id {sheet_id}")
        return None

    def _get_app_used_datasets(
        self, websocket_connection: WebsocketConnection, app_id: str
    ) -> List[QlikAppDataset]:
        datasets: List[QlikAppDataset] = []
        try:
            websocket_connection.websocket_send_request(
                method="GetObject",
                params=["LoadModel"],
            )
            response = websocket_connection.websocket_send_request(method="GetLayout")
            for table_dict in response[Constant.QLAYOUT][Constant.TABLES]:
                # Condition to Add connection based table only
                if table_dict["boxType"] == "blackbox":
                    datasets.append(QlikAppDataset.parse_obj(table_dict))
            websocket_connection.handle.pop()
        except Exception:
            self._log_http_error(
                message=f"Unable to fetch app used datasets for app {app_id}"
            )
        return datasets

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
        except Exception:
            self._log_http_error(message=f"Unable to fetch sheets for app {app_id}")
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
            app.datasets = self._get_app_used_datasets(websocket_connection, app_id)
            websocket_connection.close_websocket()
            return app
        except Exception:
            self._log_http_error(message=f"Unable to fetch app with id {app_id}")
        return None

    def get_items(self) -> List[Item]:
        items: List[Item] = []
        try:
            response = self.session.get(f"{self.rest_api_url}/items")
            response.raise_for_status()
            data = response.json()[Constant.DATA]
            for item in data:
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
                            dataset_id=item[Constant.RESOURCEID]
                        )
                        if dataset:
                            items.append(dataset)

        except Exception:
            self._log_http_error(message="Unable to fetch items")
        return items
