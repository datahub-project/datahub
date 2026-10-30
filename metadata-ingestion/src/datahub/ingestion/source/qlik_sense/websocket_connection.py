import json
import ssl
from typing import Dict, List, Optional, Union

from websocket import WebSocket, create_connection

from datahub.ingestion.source.qlik_sense.config import Constant


class WebsocketConnection:
    def __init__(self, tenant_hostname: str, api_key: str, app_id: str) -> None:
        self.websocket_url = f"wss://{tenant_hostname}/app"
        self._match_response_id = False
        self.socket_connection: Optional[WebSocket] = create_connection(
            f"{self.websocket_url}/{app_id}",
            header={"Authorization": f"Bearer {api_key}"},
        )
        self.request_id = 0
        self.handle = [-1]

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
    ) -> "WebsocketConnection":
        # Direct Engine access. Certificate auth does not go through the proxy,
        # and the engine interleaves JSON-RPC replies with push notifications.
        connection = cls.__new__(cls)
        connection.websocket_url = f"wss://{host}:{port}/app"
        connection._match_response_id = True
        connection.socket_connection = create_connection(
            f"{connection.websocket_url}/{app_id}",
            header={"X-Qlik-User": user_header},
            sslopt={
                "certfile": client_cert,
                "keyfile": client_key,
                "ca_certs": root_cert,
                "cert_reqs": ssl.CERT_REQUIRED,
            },
        )
        connection.request_id = 0
        connection.handle = [-1]
        return connection

    def _build_websocket_request_dict(
        self, method: str, params: Optional[Union[Dict, List]] = None
    ) -> Dict:
        params = params or {}
        return {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "handle": self.handle[-1],
            "method": method,
            "params": params,
        }

    def _recv_matching(self, request_id: int) -> Dict:
        if not self.socket_connection:
            return {}
        while True:
            message = json.loads(self.socket_connection.recv())
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError(str(message["error"]))
            return message[Constant.RESULT]

    def _send_request(self, request: Dict) -> Dict:
        if self.socket_connection:
            self.socket_connection.send(json.dumps(request))
            if self._match_response_id:
                return self._recv_matching(request["id"])
            resp = self.socket_connection.recv()
            if request["handle"] == -1:
                resp = self.socket_connection.recv()
            return json.loads(resp)[Constant.RESULT]
        return {}

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
            self.handle.append(handle)
        return response

    def close_websocket(self) -> None:
        if self.socket_connection:
            self.socket_connection.close()
