import json
import logging
import sys
from typing import Any, Dict, List, Optional

import requests
from websocket import WebSocket, create_connection

from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    Chart,
    Item,
    QlikDataset,
    Sheet,
    Space,
)

# Logger instance
logger = logging.getLogger(__name__)


class QlikAPI:
    def __init__(self, config: QlikSourceConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
            }
        )
        self.rest_api_url = f"https://{self.config.tenant_hostname}/api/v1"
        self.websocket_url = f"wss://{self.config.tenant_hostname}/app"
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

    def _websocket_send_request(
        self, socket_connection: WebSocket, request: dict
    ) -> Dict:
        """
        Method to send request to websocket
        """
        socket_connection.send(json.dumps(request))
        resp = socket_connection.recv()
        if request["handle"] == -1:
            resp = socket_connection.recv()
        return json.loads(resp)

    def _get_websocket_request_dict(
        self, id: int, handle: int, method: str, params: Dict = {}
    ) -> Dict:
        return {
            "jsonrpc": "2.0",
            "id": id,
            "handle": handle,
            "method": method,
            "params": params,
        }

    def get_spaces(self) -> List[Space]:
        spaces: List[Space] = []
        try:
            response = self.session.get(f"{self.rest_api_url}/spaces")
            response.raise_for_status()
            for space_dict in response.json()[Constant.DATA]:
                spaces.append(Space.parse_obj(space_dict))
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

    def get_items(self) -> List[Item]:
        items: List[Item] = []
        try:
            response = self.session.get(f"{self.rest_api_url}/items")
            response.raise_for_status()
            data = response.json()[Constant.DATA]
            for item in data:
                resource_type = item[Constant.RESOURCETYPE]
                resource_attributes = item[Constant.RESOURCEATTRIBUTES]
                if resource_type == Constant.APP:
                    app = App.parse_obj(resource_attributes)
                    app.sheets = self._get_app_sheets(app_id=app.id)
                    items.append(app)
                elif resource_type == Constant.DATASET:
                    dataset = self._get_dataset(dataset_id=item[Constant.RESOURCEID])
                    if dataset:
                        items.append(dataset)

        except Exception:
            self._log_http_error(message="Unable to fetch items")
        return items

    def _get_sheet(
        self,
        socket_connection: WebSocket,
        request_id: int,
        current_handle: int,
        sheet_id: str,
    ) -> Optional[Sheet]:
        try:
            response = self._websocket_send_request(
                socket_connection=socket_connection,
                request=self._get_websocket_request_dict(
                    id=request_id,
                    handle=current_handle,
                    method="GetObject",
                    params={"qId": sheet_id},
                ),
            )
            request_id += 1
            current_handle = response[Constant.RESULT][Constant.QRETURN][
                Constant.QHANDLE
            ]
            response = self._websocket_send_request(
                socket_connection=socket_connection,
                request=self._get_websocket_request_dict(
                    id=request_id, handle=current_handle, method="GetLayout"
                ),
            )
            request_id += 1
            sheet_dict = response[Constant.RESULT][Constant.QLAYOUT]
            sheet = Sheet.parse_obj(sheet_dict[Constant.QMETA])
            for chart_dict in sheet_dict[Constant.QCHILDLIST][Constant.QITEMS]:
                sheet.charts.append(Chart.parse_obj(chart_dict[Constant.QINFO]))
            return sheet
        except Exception:
            self._log_http_error(message=f"Unable to fetch sheet with id {sheet_id}")
        return None

    def _get_app_sheets(self, app_id: str) -> List[Sheet]:
        sheets: List[Sheet] = []
        request_id = 1
        current_handle = -1
        try:
            socket_connection = create_connection(
                f"{self.websocket_url}/{app_id}",
                header={"Authorization": f"Bearer {self.config.api_key}"},
            )
            response = self._websocket_send_request(
                socket_connection=socket_connection,
                request=self._get_websocket_request_dict(
                    id=request_id,
                    handle=current_handle,
                    method="OpenDoc",
                    params={"qDocName": app_id},
                ),
            )
            request_id += 1
            current_handle = response["result"]["qReturn"]["qHandle"]
            response = self._websocket_send_request(
                socket_connection=socket_connection,
                request=self._get_websocket_request_dict(
                    id=request_id,
                    handle=current_handle,
                    method="GetObjects",
                    params={
                        "qOptions": {
                            "qTypes": ["sheet"],
                        }
                    },
                ),
            )
            request_id += 1
            for sheet_dict in response[Constant.RESULT][Constant.QLIST]:
                sheet = self._get_sheet(
                    socket_connection=socket_connection,
                    request_id=request_id,
                    current_handle=current_handle,
                    sheet_id=sheet_dict[Constant.QINFO][Constant.QID],
                )
                if sheet:
                    sheets.append(sheet)
        except Exception:
            self._log_http_error(message="Unable to fetch sheets")
        return sheets
