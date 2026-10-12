import logging
import secrets
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from datahub.ingestion.source.qlik_sense.config import (
    QLIK_DATETIME_FORMAT,
    Constant,
    QlikSourceConfig,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    Chart,
    Item,
    Sheet,
    Space,
    SpaceType,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import WebsocketConnection

logger = logging.getLogger(__name__)

UNPUBLISHED_APPS_ID = "unpublished-apps"
UNPUBLISHED_APPS_NAME = "Unpublished"
_XRFKEY_LENGTH = 16


def normalize_qlik_timestamp(value: str) -> str:
    # QRS sometimes omits the fractional seconds that Cloud always sends.
    if value.endswith("Z") and "." not in value:
        return f"{value[:-1]}.000Z"
    return value


class QlikOnPremAPI:
    def __init__(self, config: QlikSourceConfig) -> None:
        self.config = config
        self.spaces: Dict[str, str] = {}
        self.users: Dict[str, str] = {}
        server = config.server
        client_cert = config.client_cert
        client_key = config.client_key
        root_cert = config.root_cert
        if not server or not client_cert or not client_key or not root_cert:
            raise ValueError("On-prem Qlik Sense requires server and certificates")
        self.xrfkey = secrets.token_hex(_XRFKEY_LENGTH // 2)
        self.session = requests.Session()
        self.session.cert = (client_cert, client_key)
        self.session.verify = root_cert
        self.session.headers.update(
            {
                "X-Qlik-User": config.qlik_user_header(config.qrs_user_id),
                "X-Qlik-Xrfkey": self.xrfkey,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self.qrs_url = f"https://{server}:{config.qrs_port}/qrs"
        logger.info(f"Trying to connect to {self.qrs_url}")
        self._get("/about")

    def _log_http_error(self, message: str) -> Any:
        logger.warning(message)
        _, e, _ = sys.exc_info()
        if isinstance(e, requests.exceptions.HTTPError):
            logger.warning(f"HTTP status-code = {e.response.status_code}")
        logger.debug(msg=message, exc_info=e)
        return e

    def _get(
        self, path: str, params: Optional[Dict[str, str]] = None
    ) -> requests.Response:
        # QRS rejects the call unless this 16-character key is in both places.
        query = dict(params or {})
        query["xrfkey"] = self.xrfkey
        response = self.session.get(f"{self.qrs_url}{path}", params=query)
        response.raise_for_status()
        return response

    def _remember_owner(self, owner: Optional[Dict]) -> Optional[str]:
        if not owner:
            return None
        owner_id = owner.get(Constant.ID)
        if not owner_id:
            return None
        name = owner.get(Constant.NAME) or owner.get("userId")
        if name:
            self.users[owner_id] = name
        return owner_id

    def get_user_name(self, user_id: str) -> Optional[str]:
        if not user_id:
            return None
        if user_id in self.users:
            return self.users[user_id]
        try:
            user = self._get(f"/user/{user_id}").json()
            name = user.get(Constant.NAME) or user.get("userId")
            if name:
                self.users[user_id] = name
            return name
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch user with id {user_id}. Exception: {e}"
            )
        return None

    def _space_from_stream(self, stream: Dict) -> Space:
        created = normalize_qlik_timestamp(stream[Constant.CREATEDDATE])
        modified = normalize_qlik_timestamp(stream[Constant.MODIFIEDDATE])
        return Space.model_validate(
            {
                Constant.ID: stream[Constant.ID],
                Constant.NAME: stream[Constant.NAME],
                "description": stream.get("description") or "",
                Constant.TYPE: SpaceType.STREAM.value,
                Constant.CREATEDAT: created,
                Constant.UPDATEDAT: modified,
                Constant.OWNERID: self._remember_owner(stream.get("owner")),
            }
        )

    def get_spaces(self) -> List[Space]:
        spaces: List[Space] = []
        try:
            for stream in self._get("/stream/full").json():
                space = self._space_from_stream(stream)
                spaces.append(space)
                self.spaces[space.id] = space.name
            if self.config.ingest_unpublished_apps:
                now = datetime.now().strftime(QLIK_DATETIME_FORMAT)
                unpublished = Space.model_validate(
                    {
                        Constant.ID: UNPUBLISHED_APPS_ID,
                        Constant.NAME: UNPUBLISHED_APPS_NAME,
                        "description": "Apps that are not published to a stream",
                        Constant.TYPE: SpaceType.STREAM.value,
                        Constant.CREATEDAT: now,
                        Constant.UPDATEDAT: now,
                    }
                )
                spaces.append(unpublished)
                self.spaces[unpublished.id] = unpublished.name
        except Exception as e:
            self._log_http_error(message=f"Unable to fetch streams. Exception: {e}")
        return spaces

    def _app_from_qrs(self, app: Dict) -> App:
        stream = app.get("stream") or {}
        return App.model_validate(
            {
                Constant.ID: app[Constant.ID],
                "description": app.get("description") or "",
                Constant.OWNERID: self._remember_owner(app.get("owner")) or "",
                Constant.SPACEID: stream.get(Constant.ID) or UNPUBLISHED_APPS_ID,
                Constant.CREATEDDATE: normalize_qlik_timestamp(
                    app[Constant.CREATEDDATE]
                ),
                Constant.MODIFIEDDATE: normalize_qlik_timestamp(
                    app[Constant.MODIFIEDDATE]
                ),
                "qTitle": app.get(Constant.NAME) or "",
                "qUsage": "",
            }
        )

    def _open_engine(self, app_id: str) -> WebsocketConnection:
        assert self.config.server is not None
        assert self.config.client_cert is not None
        assert self.config.client_key is not None
        assert self.config.root_cert is not None
        return WebsocketConnection.for_on_prem(
            host=self.config.server,
            port=self.config.engine_port,
            app_id=app_id,
            user_header=self.config.qlik_user_header(self.config.engine_user_id),
            client_cert=self.config.client_cert,
            client_key=self.config.client_key,
            root_cert=self.config.root_cert,
        )

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
            q_layout = response[Constant.QLAYOUT]
            if Constant.HYPERCUBE not in q_layout:
                logger.warning(
                    f"Chart with id {chart_id} of sheet {sheet_id} does not have hypercube. q_layout: {q_layout}"
                )
                return None
            return Chart.model_validate(q_layout)
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
                return None
            sheet = Sheet.model_validate(sheet_dict[Constant.QMETA])
            if Constant.QCHILDLIST not in sheet_dict:
                logger.warning(
                    f"Sheet {sheet.title} with id {sheet_id} does not have any charts. sheet_dict: {sheet_dict}"
                )
                return sheet
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
                        chart.title = f"Object {i + 1} of Sheet '{sheet.title}'"
                    sheet.charts.append(chart)
                websocket_connection.handle.pop()
            return sheet
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sheet with id {sheet_id}. Exception: {e}"
            )
        return None

    def _get_app_sheets(
        self, websocket_connection: WebsocketConnection, app_id: str
    ) -> List[Sheet]:
        sheets: List[Sheet] = []
        try:
            response = websocket_connection.websocket_send_request(
                method="GetObjects",
                params={"qOptions": {"qTypes": ["sheet"]}},
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

    def _load_app(self, app_dict: Dict) -> Optional[App]:
        try:
            app = self._app_from_qrs(app_dict)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to parse app {app_dict.get(Constant.ID)}. Exception: {e}"
            )
            return None
        try:
            websocket_connection = self._open_engine(app.id)
            websocket_connection.websocket_send_request(
                method="OpenDoc",
                params={"qDocName": app.id},
            )
            app.sheets = self._get_app_sheets(websocket_connection, app.id)
            websocket_connection.close_websocket()
        except Exception as e:
            self._log_http_error(
                message=f"Unable to open app {app.id} in the Engine. Exception: {e}"
            )
        return app

    def get_items(self, space_id: str) -> List[Item]:
        items: List[Item] = []
        if space_id not in self.spaces:
            return items
        if not self.config.space_pattern.allowed(self.spaces[space_id]):
            return items
        qrs_filter = (
            "published eq false"
            if space_id == UNPUBLISHED_APPS_ID
            else f"stream.id eq {space_id}"
        )
        try:
            for app_dict in self._get(
                "/app/full", params={"filter": qrs_filter}
            ).json():
                app = self._load_app(app_dict)
                if app:
                    items.append(app)
        except Exception as e:
            self._log_http_error(message=f"Unable to fetch apps. Exception: {e}")
        return items
