import logging
import secrets
import socket
import ssl
import sys
import threading
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Union

import requests
from requests.adapters import HTTPAdapter

from datahub.ingestion.source.qlik_sense.config import (
    QLIK_DATETIME_FORMAT,
    Constant,
    QlikSourceConfig,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    Item,
    Sheet,
    Space,
    SpaceType,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    DataConnectionInfo,
    infer_data_connection,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import WebsocketConnection

logger = logging.getLogger(__name__)

UNPUBLISHED_APPS_ID = "unpublished-apps"
UNPUBLISHED_APPS_NAME = "Unpublished"
_XRFKEY_LENGTH = 16


class _AssertHostnameAdapter(HTTPAdapter):
    # Qlik server certificates are issued to the node name, so connecting by IP
    # or alias fails the name check even though the chain verifies.
    def __init__(self, assert_hostname: Union[str, Literal[False]]) -> None:
        self._assert_hostname = assert_hostname
        super().__init__()

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        kwargs["assert_hostname"] = self._assert_hostname
        super().init_poolmanager(*args, **kwargs)


def _presented_certificate(
    host: str, port: int, client_cert: str, client_key: str
) -> str:
    # Diagnostic only: the handshake skips verification so the certificate the
    # server actually sends can be compared with root_cert.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        context.load_cert_chain(client_cert, client_key)
        with socket.create_connection((host, port), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=host) as tls:
                der = tls.getpeercert(binary_form=True)
    except (OSError, ssl.SSLError) as e:
        return f"<unable to read: {e}>"
    return ssl.DER_cert_to_PEM_cert(der) if der else "<no certificate presented>"


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
        self._thread_state = threading.local()
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
        assert_hostname = config.tls_assert_hostname()
        if assert_hostname is not None:
            self.session.mount(self.qrs_url, _AssertHostnameAdapter(assert_hostname))
        logger.info(f"Trying to connect to {self.qrs_url}")
        try:
            self._get("/about")
        except requests.exceptions.SSLError:
            logger.warning(
                f"TLS verification against root_cert failed. Certificate presented by "
                f"{server}:{config.qrs_port}:\n"
                f"{_presented_certificate(server, config.qrs_port, client_cert, client_key)}"
            )
            raise

    def _log_http_error(self, message: str) -> Any:
        self._thread_state.read_failed = True
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
        # Passed per request because REQUESTS_CA_BUNDLE / CURL_CA_BUNDLE, common
        # on executors, otherwise override session.verify and drop the Qlik CA.
        response = self.session.get(
            f"{self.qrs_url}{path}", params=query, verify=self.session.verify
        )
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
            assert_hostname=self.config.tls_assert_hostname(),
        )

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
            # Enterprise reports the sheet owner as an object rather than ownerId.
            meta = dict(sheet_dict[Constant.QMETA])
            meta.setdefault(Constant.ID, sheet_id)
            if not meta.get(Constant.OWNERID):
                meta[Constant.OWNERID] = self._remember_owner(meta.get("owner"))
            sheet = Sheet.model_validate(meta)
            if Constant.QCHILDLIST not in sheet_dict:
                logger.debug(f"Sheet {sheet.title} with id {sheet_id} has no charts")
                return sheet
            for i, chart_dict in enumerate(
                sheet_dict[Constant.QCHILDLIST][Constant.QITEMS]
            ):
                for chart in websocket_connection.get_charts(
                    chart_dict[Constant.QINFO][Constant.QID],
                    sheet_id,
                    on_error=self._log_http_error,
                ):
                    if not chart.title:
                        chart.title = f"Object {i + 1} of Sheet '{sheet.title}'"
                    sheet.charts.append(chart)
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
                depth = len(websocket_connection.handle)
                sheet = self._get_sheet(
                    websocket_connection=websocket_connection,
                    sheet_id=sheet_dict[Constant.QINFO][Constant.QID],
                )
                del websocket_connection.handle[depth:]
                if sheet:
                    sheets.append(sheet)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sheets for app {app_id}. Exception: {e}"
            )
        return sheets

    def _load_app(self, app_dict: Dict) -> Optional[App]:
        self._thread_state.read_failed = False
        try:
            app = self._app_from_qrs(app_dict)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to parse app {app_dict.get(Constant.ID)}. Exception: {e}"
            )
            return None
        try:
            websocket_connection = self._open_engine(app.id)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to connect to the Engine for app {app.id}. Exception: {e}"
            )
            app.read_complete = False
            return app
        try:
            websocket_connection.websocket_send_request(
                method="OpenDoc",
                # Only metadata is read, and loading each app's data into the
                # Engine takes minutes per app on large sites.
                params={"qDocName": app.id, "qNoData": True},
            )
            app.sheets = self._get_app_sheets(websocket_connection, app.id)
            try:
                app.script = websocket_connection.get_script()
                logger.debug(
                    f"App {app.id} load script has {len(app.script)} characters"
                )
            except Exception as e:
                self._log_http_error(
                    message=f"Unable to fetch load script of app {app.id}. Exception: {e}"
                )
        except Exception as e:
            self._log_http_error(
                message=f"Unable to open app {app.id} in the Engine. Exception: {e}"
            )
        finally:
            try:
                websocket_connection.close_websocket()
            except Exception as e:
                logger.debug(f"Unable to close Engine session of app {app.id}: {e}")
        app.read_complete = not self._thread_state.read_failed
        return app

    def get_data_connections(self) -> Dict[str, DataConnectionInfo]:
        connections: Dict[str, DataConnectionInfo] = {}
        try:
            for connection in self._get("/dataconnection/full").json():
                name = connection.get(Constant.NAME)
                if not name:
                    continue
                descriptor = ";".join(
                    str(connection.get(key) or "")
                    for key in (Constant.CONNECTIONSTRING, Constant.TYPE)
                )
                connections[name] = infer_data_connection(name, descriptor)
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch data connections. Exception: {e}"
            )
        logger.debug(f"QRS data connections: {list(connections.values())}")
        return connections

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
