import json
from typing import Dict, List, Optional, Union

from websocket import WebSocket, create_connection

from datahub.ingestion.source.qlik_sense.config import Constant


class WebsocketConnection:
    def __init__(self, tenant_hostname: str, api_key: str, app_id: str) -> None:
        self.websocket_url = f"wss://{tenant_hostname}/app"
        self.socket_connection: Optional[WebSocket] = create_connection(
            f"{self.websocket_url}/{app_id}",
            header={"Authorization": f"Bearer {api_key}"},
        )
        self.request_id = 0
        self.handle = [-1]

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

    def _send_request(self, request: Dict) -> Dict:
        if self.socket_connection:
            self.socket_connection.send(json.dumps(request))
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
