import json
import logging
import ssl
import time
from contextlib import contextmanager
from typing import Callable, Dict, Iterator, List, Literal, Optional, Set, Union

from websocket import WebSocket, create_connection

from datahub.ingestion.source.qlik_sense.config import Constant
from datahub.ingestion.source.qlik_sense.data_classes import Chart

logger = logging.getLogger(__name__)

# Engine JSON-RPC error code LOCERR_GENERIC_ACCESS_DENIED.
ENGINE_ACCESS_DENIED = 5


class EngineError(RuntimeError):
    def __init__(self, error: Dict) -> None:
        super().__init__(str(error))
        self.code = error.get("code")

    @property
    def access_denied(self) -> bool:
        return self.code == ENGINE_ACCESS_DENIED


class WebsocketConnection:
    def __init__(
        self,
        url: str,
        header: Dict[str, str],
        match_response_id: bool = False,
        sslopt: Optional[Dict[str, object]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self._match_response_id = match_response_id
        # Applies to the handshake and to each receive.
        self._timeout = timeout
        self.socket_connection: Optional[WebSocket] = create_connection(
            url, header=header, sslopt=sslopt or {}, timeout=timeout
        )
        self.request_id = 0
        # Engine object handles, innermost last; -1 is the global handle.
        self._handles = [-1]

    @classmethod
    def for_cloud(
        cls,
        tenant_hostname: str,
        api_key: str,
        app_id: str,
        timeout: Optional[float] = None,
    ) -> "WebsocketConnection":
        return cls(
            f"wss://{tenant_hostname}/app/{app_id}",
            header={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )

    @classmethod
    def for_on_prem(
        cls,
        host: str,
        port: int,
        app_id: str,
        user_header: str,
        client_cert: str,
        client_key: str,
        root_cert: str,
        assert_hostname: Union[str, Literal[False], None] = None,
        key_password: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> "WebsocketConnection":
        # Direct Engine access. Certificate auth does not go through the proxy,
        # and the engine interleaves JSON-RPC replies with push notifications.
        sslopt: Dict[str, object] = {
            "certfile": client_cert,
            "keyfile": client_key,
            "ca_certs": root_cert,
            "cert_reqs": ssl.CERT_REQUIRED,
        }
        if key_password:
            sslopt["password"] = key_password
        if assert_hostname is False:
            sslopt["check_hostname"] = False
        elif assert_hostname:
            sslopt["server_hostname"] = assert_hostname
        return cls(
            f"wss://{host}:{port}/app/{app_id}",
            header={"X-Qlik-User": user_header},
            match_response_id=True,
            sslopt=sslopt,
            timeout=timeout,
        )

    @property
    def handles(self) -> List[int]:
        return list(self._handles)

    @contextmanager
    def restore_handle(self) -> Iterator[None]:
        depth = len(self._handles)
        try:
            yield
        finally:
            del self._handles[depth:]

    def _build_websocket_request_dict(
        self, method: str, params: Optional[Union[Dict, List]] = None
    ) -> Dict:
        params = params or {}
        return {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "handle": self._handles[-1],
            "method": method,
            "params": params,
        }

    @staticmethod
    def _result(message: Dict) -> Dict:
        if "error" in message:
            raise EngineError(message["error"])
        return message[Constant.RESULT]

    def _recv_matching(self, socket_connection: WebSocket, request_id: int) -> Dict:
        # Push notifications keep each receive short of the socket timeout, so
        # the whole wait for the reply is bounded too.
        deadline = time.monotonic() + self._timeout if self._timeout else None
        while True:
            if deadline is not None:
                # Each receive only waits for what is left of the deadline.
                socket_connection.settimeout(max(deadline - time.monotonic(), 0.001))
            message = json.loads(socket_connection.recv())
            if deadline is not None and time.monotonic() > deadline:
                raise TimeoutError(
                    f"No Engine reply to request {request_id} within {self._timeout}s"
                )
            if message.get("id") == request_id:
                return self._result(message)

    def _send_request(self, request: Dict) -> Dict:
        if not self.socket_connection:
            return {}
        self.socket_connection.send(json.dumps(request))
        if self._match_response_id:
            return self._recv_matching(self.socket_connection, request["id"])
        resp = self.socket_connection.recv()
        if request["handle"] == -1:
            resp = self.socket_connection.recv()
        return self._result(json.loads(resp))

    def websocket_send_request(
        self, method: str, params: Optional[Union[Dict, List]] = None
    ) -> Dict:
        """
        Method to send request to websocket
        """
        params = params or {}
        self.request_id += 1
        request = self._build_websocket_request_dict(method, params)
        response = self._send_request(request=request)
        handle = response.get(Constant.QRETURN, {}).get(Constant.QHANDLE)
        if handle:
            self._handles.append(handle)
        return response

    def get_script(self) -> str:
        """Load script of the open app. Must be called on the app handle."""
        response = self.websocket_send_request(method="GetScript")
        return response.get(Constant.QSCRIPT) or ""

    def get_charts(
        self,
        object_id: str,
        sheet_id: str,
        on_error: Callable[[str], None],
        seen: Optional[Set[str]] = None,
    ) -> List[Chart]:
        """Charts of a sheet object, including the charts held in a container's tabs.
        Must be called on the handle of the object's parent."""
        # A container can hold a link that resolves back to itself.
        seen = set() if seen is None else seen
        if object_id in seen:
            return []
        seen.add(object_id)
        with self.restore_handle():
            try:
                self.websocket_send_request(
                    method="GetChild", params={"qId": object_id}
                )
                q_layout = self.websocket_send_request(method="GetLayout")[
                    Constant.QLAYOUT
                ]
                if Constant.HYPERCUBE in q_layout:
                    return [Chart.model_validate(q_layout)]
                object_type = q_layout.get(Constant.QINFO, {}).get(Constant.QTYPE)
                if object_type != Constant.CONTAINER:
                    logger.debug(
                        f"Skipping {object_type} object {object_id} of sheet {sheet_id}: it has no hypercube"
                    )
                    return []
                tab_labels = {
                    tab.get(Constant.REFID): tab.get(Constant.LABEL)
                    for tab in q_layout.get(Constant.CHILDREN, [])
                }
                charts: List[Chart] = []
                for item in q_layout.get(Constant.QCHILDLIST, {}).get(
                    Constant.QITEMS, []
                ):
                    tab_id = item.get(Constant.QDATA, {}).get(Constant.CONTAINERCHILDID)
                    for chart in self.get_charts(
                        item[Constant.QINFO][Constant.QID], sheet_id, on_error, seen
                    ):
                        if not chart.title:
                            chart.title = tab_labels.get(tab_id) or ""
                        charts.append(chart)
                return charts
            except Exception as e:
                on_error(
                    f"Unable to fetch chart {object_id} of sheet {sheet_id}. Exception: {e}"
                )
                return []

    def close_websocket(self) -> None:
        if self.socket_connection:
            self.socket_connection.close()
