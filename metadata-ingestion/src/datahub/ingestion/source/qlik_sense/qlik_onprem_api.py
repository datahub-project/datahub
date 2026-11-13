import hashlib
import json
import logging
import secrets
import socket
import ssl
from datetime import datetime
from typing import Any, Dict, Iterable, List, Literal, Optional, Union

import requests
from requests.adapters import DEFAULT_POOLSIZE, HTTPAdapter
from websocket import WebSocketTimeoutException

from datahub.ingestion.source.qlik_sense.config import (
    QLIK_DATETIME_FORMAT,
    Constant,
    QlikSourceConfig,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    Space,
    SpaceType,
    UnchangedApp,
)
from datahub.ingestion.source.qlik_sense.qlik_base_api import (
    UNREAD_DATA_CONNECTIONS,
    IsUnchanged,
    QlikBaseAPI,
    ReadErrors,
    log_read_error,
)
from datahub.ingestion.source.qlik_sense.script_lineage import DataConnectionInfo
from datahub.ingestion.source.qlik_sense.websocket_connection import (
    EngineError,
    WebsocketConnection,
)

logger = logging.getLogger(__name__)


def _data_load_failed(e: Exception) -> bool:
    """Whether opening an app with its data failed on the data itself."""
    return (isinstance(e, EngineError) and e.access_denied) or isinstance(
        e, (TimeoutError, WebSocketTimeoutException)
    )


UNPUBLISHED_APPS_ID = "unpublished-apps"
UNPUBLISHED_APPS_NAME = "Unpublished"
_XRFKEY_LENGTH = 16
# The Repository Service lists sheets, stories and master items as app objects with
# their own modifiedDate, so object edits are caught even if the app's own
# modifiedDate does not move.
_APP_FINGERPRINT_FIELDS = (
    "name",
    "description",
    "modifiedDate",
    "lastReloadTime",
    "published",
    "publishTime",
)
_OBJECT_FINGERPRINT_FIELDS = (
    "engineObjectId",
    "objectType",
    "name",
    "modifiedDate",
    "contentHash",
    "published",
    "approved",
)


class _QrsAdapter(HTTPAdapter):
    def __init__(
        self,
        assert_hostname: Union[str, Literal[False], None],
        key_password: Optional[str],
        pool_maxsize: int = DEFAULT_POOLSIZE,
    ) -> None:
        self._assert_hostname = assert_hostname
        self._key_password = key_password
        super().__init__(pool_maxsize=pool_maxsize)

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        # Qlik server certificates are issued to the node name, so connecting by
        # IP or alias fails the name check even though the chain verifies.
        if self._assert_hostname is not None:
            kwargs["assert_hostname"] = self._assert_hostname
        # requests has no setting for an encrypted client key; urllib3 does.
        if self._key_password:
            kwargs["key_password"] = self._key_password
        super().init_poolmanager(*args, **kwargs)


def _presented_certificate(
    host: str,
    port: int,
    client_cert: str,
    client_key: str,
    key_password: Optional[str] = None,
) -> str:
    # Diagnostic only: the handshake skips verification so the certificate the
    # server actually sends can be compared with root_cert.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        context.load_cert_chain(client_cert, client_key, key_password)
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


class QlikOnPremAPI(QlikBaseAPI):
    def __init__(self, config: QlikSourceConfig) -> None:
        super().__init__(config)
        if config.on_prem is None:
            raise ValueError("On-prem Qlik Sense requires the on_prem block")
        self.on_prem = on_prem = config.on_prem
        self.xrfkey = secrets.token_hex(_XRFKEY_LENGTH // 2)
        self.session = requests.Session()
        self.session.cert = (on_prem.client_cert, on_prem.client_key)
        self.session.verify = on_prem.root_cert
        self.session.headers.update(
            {
                "X-Qlik-User": on_prem.qlik_user_header(on_prem.qrs_user_id),
                "X-Qlik-Xrfkey": self.xrfkey,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self.qrs_url = f"https://{on_prem.server}:{on_prem.qrs_port}/qrs"
        self.key_password = (
            on_prem.client_key_password.get_secret_value()
            if on_prem.client_key_password
            else None
        )
        # Every worker can hold a connection at once.
        self.session.mount(
            self.qrs_url,
            _QrsAdapter(
                on_prem.tls_assert_hostname(),
                self.key_password,
                pool_maxsize=max(DEFAULT_POOLSIZE, config.resolved_max_workers()),
            ),
        )
        logger.info(f"Trying to connect to {self.qrs_url}")
        try:
            self._get("/about")
        except requests.exceptions.SSLError:
            logger.warning(
                f"TLS verification against root_cert failed. Certificate presented by "
                f"{on_prem.server}:{on_prem.qrs_port}:\n"
                f"{_presented_certificate(on_prem.server, on_prem.qrs_port, on_prem.client_cert, on_prem.client_key, self.key_password)}"
            )
            raise

    def _get(
        self, path: str, params: Optional[Dict[str, str]] = None
    ) -> requests.Response:
        # QRS rejects the call unless this 16-character key is in both places.
        query = dict(params or {})
        query["xrfkey"] = self.xrfkey
        # Passed per request because REQUESTS_CA_BUNDLE / CURL_CA_BUNDLE, common
        # on executors, otherwise override session.verify and drop the Qlik CA.
        response = self.session.get(
            f"{self.qrs_url}{path}",
            params=query,
            verify=self.session.verify,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        return response

    def _remember_owner(self, owner: Optional[Dict]) -> Optional[str]:
        if not owner:
            return None
        owner_id = owner.get(Constant.ID)
        if not owner_id:
            return None
        # The user id, sAMAccountName for Windows directories, joins with users
        # ingested from AD or LDAP. The display name would not.
        name = owner.get(Constant.USERID) or owner.get(Constant.NAME)
        if name:
            self.users[owner_id] = name
        return owner_id

    def get_user_name(self, user_id: str) -> Optional[str]:
        if not user_id:
            return None
        if user_id in self.users:
            return self.users[user_id]
        if user_id in self.failed_user_lookups:
            return None
        try:
            user = self._get(f"/user/{user_id}").json()
            name = user.get(Constant.USERID) or user.get(Constant.NAME)
            if name:
                self.users[user_id] = name
            return name
        except Exception as e:
            self._record_failed_user_lookup(user_id, e)
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
            if self.on_prem.ingest_unpublished_apps:
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
            self._record_unread(
                "streams", f"Unable to fetch streams. Exception: {e}", e
            )
        return spaces

    def _app_from_qrs(self, app: Dict) -> App:
        stream = app.get("stream") or {}
        return App.model_validate(
            {
                Constant.ID: app[Constant.ID],
                "description": app.get("description") or "",
                Constant.OWNERID: self._remember_owner(app.get("owner")),
                Constant.SPACEID: stream.get(Constant.ID) or UNPUBLISHED_APPS_ID,
                Constant.CREATEDDATE: normalize_qlik_timestamp(
                    app[Constant.CREATEDDATE]
                ),
                Constant.MODIFIEDDATE: normalize_qlik_timestamp(
                    app[Constant.MODIFIEDDATE]
                ),
                "qTitle": app.get(Constant.NAME) or "",
            }
        )

    def _open_engine(self, app_id: str) -> WebsocketConnection:
        return WebsocketConnection.for_on_prem(
            host=self.on_prem.server,
            port=self.on_prem.engine_port,
            app_id=app_id,
            user_header=self.on_prem.qlik_user_header(self.on_prem.engine_user_id),
            client_cert=self.on_prem.client_cert,
            client_key=self.on_prem.client_key,
            root_cert=self.on_prem.root_cert,
            assert_hostname=self.on_prem.tls_assert_hostname(),
            key_password=self.key_password,
            timeout=self.config.timeout_seconds,
        )

    def check_engine_port(self) -> None:
        """Raises when the Engine port is not reachable, which a firewall that
        only opens the Repository Service port would cause."""
        with socket.create_connection(
            (self.on_prem.server, self.on_prem.engine_port),
            timeout=min(self.config.timeout_seconds, 30),
        ):
            pass

    def _sheet_meta(self, meta: Dict, sheet_id: str) -> Optional[Dict]:
        # Enterprise reports the sheet owner as an object rather than ownerId.
        meta = dict(meta)
        meta.setdefault(Constant.ID, sheet_id)
        for key in (Constant.CREATEDDATE, Constant.MODIFIEDDATE):
            if meta.get(key):
                meta[key] = normalize_qlik_timestamp(meta[key])
        if not meta.get(Constant.OWNERID):
            meta[Constant.OWNERID] = self._remember_owner(meta.get("owner"))
        return meta

    def _load_item(
        self, item: Dict, is_unchanged: Optional[IsUnchanged]
    ) -> Union[App, UnchangedApp, None]:
        app_id = str(item.get(Constant.ID))
        try:
            return self._read_or_skip_app(item, is_unchanged)
        except Exception as e:
            self._record_unread(
                app_id, f"Unable to load app {app_id}. Exception: {e}", e
            )
        return None

    def _read_app(
        self, app_dict: Dict, fingerprint: Optional[str] = None
    ) -> Optional[App]:
        try:
            app = self._app_from_qrs(app_dict)
            app.fingerprint = fingerprint
        except Exception as e:
            self._record_unread(
                str(app_dict.get(Constant.ID)),
                f"Unable to parse app {app_dict.get(Constant.ID)}. Exception: {e}",
                e,
            )
            return None
        errors = ReadErrors()
        try:
            websocket_connection = self._open_engine(app.id)
        except Exception as e:
            errors.record(
                f"Unable to connect to the Engine for app {app.id}. Exception: {e}"
            )
            app.read_errors = errors.messages
            app.sheets_complete = False
            return app
        with_data = self.on_prem.read_data_model
        try:
            try:
                self._open_doc(websocket_connection, app.id, with_data)
            except Exception as e:
                if not (with_data and _data_load_failed(e)):
                    raise
                # Section access can deny the data to the service user, and a
                # large app can time out while its data loads, but the app's
                # script, sheets and variables are still readable without it.
                self._close_engine(websocket_connection, app.id)
                websocket_connection = self._open_engine(app.id)
                self._open_doc(websocket_connection, app.id, with_data=False)
                with_data = False
                app.data_load_failed = True
                self.opened_without_data.append(f"{app.id}: {e}")
            app.sheets = self._get_app_sheets(
                websocket_connection,
                app.id,
                errors,
                # Every sheet of an unpublished app is still private.
                keep_private=app.spaceId == UNPUBLISHED_APPS_ID,
            )
            variables = self._read_variables(websocket_connection, app.id)
            app.variables = variables or {}
            app.variables_read_failed = variables is None
            app.script = self._read_load_script(
                websocket_connection, app.id, on_error=errors.record
            )
            app.tables_read_failed = app.script is None
            if with_data:
                data_model_errors = ReadErrors()
                app.data_model_tables = self._read_data_model(
                    websocket_connection, app.id, data_model_errors
                )
                if data_model_errors.messages:
                    # Read as an app opened without its data: the script,
                    # sheets and QVDs are complete, and the next run tries the
                    # data model again.
                    app.data_load_failed = True
                    self.opened_without_data.extend(
                        f"{app.id}: {message}" for message in data_model_errors.messages
                    )
        except Exception as e:
            errors.record(f"Unable to open app {app.id} in the Engine. Exception: {e}")
            # It may have failed before the sheets were read.
            errors.sheets_unread = True
        finally:
            self._close_engine(websocket_connection, app.id)
        app.read_errors = errors.messages
        app.sheets_complete = not errors.sheets_unread
        app.read_fallbacks = errors.fallbacks
        return app

    @staticmethod
    def _open_doc(
        websocket_connection: WebsocketConnection, app_id: str, with_data: bool
    ) -> None:
        websocket_connection.websocket_send_request(
            method="OpenDoc",
            # Without its data the app has no data model to read, but it opens
            # much faster on large sites.
            params={"qDocName": app_id, "qNoData": not with_data},
        )

    def _read_or_skip_app(
        self,
        app_dict: Dict,
        is_unchanged: Optional[IsUnchanged],
    ) -> Union[App, UnchangedApp, None]:
        fingerprint: Optional[str] = None
        if is_unchanged:
            # Dropping the app here would let stale entity removal delete it, so
            # any error while checking for changes falls back to reading it.
            try:
                fingerprint = self._app_fingerprint(app_dict)
                if fingerprint and is_unchanged(app_dict[Constant.ID], fingerprint):
                    return UnchangedApp(
                        id=app_dict[Constant.ID], fingerprint=fingerprint
                    )
            except Exception as e:
                logger.warning(
                    f"Unable to check whether app {app_dict.get(Constant.ID)} "
                    f"changed, reading it instead. Exception: {e}"
                )
                self.change_check_failures.append(f"{app_dict.get(Constant.ID)}: {e}")
        return self._read_app(app_dict, fingerprint)

    def _app_fingerprint(self, app_dict: Dict) -> Optional[str]:
        """Hash of everything the Repository Service knows about the app that the
        Engine read depends on. None when it cannot be computed."""
        try:
            objects = self._get(
                "/app/object/full",
                params={"filter": f"app.id eq {app_dict[Constant.ID]}"},
            ).json()
        except Exception as e:
            log_read_error(
                f"Unable to fetch objects of app {app_dict.get(Constant.ID)}. Exception: {e}"
            )
            self.change_check_failures.append(
                f"{app_dict.get(Constant.ID)}: unable to fetch app objects: {e}"
            )
            return None
        owner = app_dict.get("owner") or {}
        payload = {
            "app": {key: app_dict.get(key) for key in _APP_FINGERPRINT_FIELDS},
            "stream": (app_dict.get("stream") or {}).get(Constant.ID),
            "owner": {key: owner.get(key) for key in ("id", "name", "userId")},
            "objects": sorted(
                json.dumps(
                    {
                        **{key: obj.get(key) for key in _OBJECT_FINGERPRINT_FIELDS},
                        # A sheet that changes owner is read again.
                        "owner": (obj.get("owner") or {}).get(Constant.ID),
                    },
                    sort_keys=True,
                )
                for obj in objects
            ),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def get_data_connections(self) -> Dict[str, DataConnectionInfo]:
        connections: Dict[str, DataConnectionInfo] = {}
        try:
            for connection in self._get("/dataconnection/full").json():
                name = connection.get(Constant.NAME)
                if not name:
                    continue
                info = self._infer_connection(
                    name, connection, (Constant.CONNECTIONSTRING, Constant.TYPE)
                )
                if str(connection.get(Constant.TYPE) or "").lower() == "folder":
                    info.folder_path = connection.get(Constant.CONNECTIONSTRING)
                connections[name] = info
        except Exception as e:
            self._record_unread(
                UNREAD_DATA_CONNECTIONS,
                f"Unable to fetch data connections. Exception: {e}",
                e,
            )
        logger.debug(f"QRS data connections: {list(connections.values())}")
        return connections

    def _list_items(self, space_id: str) -> Iterable[Dict]:
        if space_id not in self.spaces:
            return []
        if not self.config.space_pattern.allowed(self.spaces[space_id]):
            return []
        qrs_filter = (
            "published eq false"
            if space_id == UNPUBLISHED_APPS_ID
            else f"stream.id eq {space_id}"
        )
        try:
            app_dicts = self._get("/app/full", params={"filter": qrs_filter}).json()
        except Exception as e:
            self._record_unread(
                f"apps of stream {space_id}",
                f"Unable to fetch apps. Exception: {e}",
                e,
            )
            return []
        logger.info(f"Stream {self.spaces[space_id]} has {len(app_dicts)} apps")
        return app_dicts
