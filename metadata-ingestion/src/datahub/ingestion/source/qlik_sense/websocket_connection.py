import json
import logging
import ssl
from typing import Callable, Dict, List, Literal, Optional, Union

from websocket import WebSocket, create_connection

from datahub.ingestion.source.qlik_sense.config import Constant
from datahub.ingestion.source.qlik_sense.data_classes import Chart, Sheet

logger = logging.getLogger(__name__)


class WebsocketConnection:
    def __init__(self, tenant_hostname: str, api_key: str, app_id: str) -> None:
        self.websocket_url = f"wss://{tenant_hostname}/app"
        self._match_response_id = False
        self.socket_connection: Optional[WebSocket] = create_connection(
            f"{self.websocket_url}/{app_id}",
            header={"Authorization": f"Bearer {api_key}"},
            skip_utf8_validation=True,
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
        assert_hostname: Union[str, Literal[False], None] = None,
    ) -> "WebsocketConnection":
        # Direct Engine access. Certificate auth does not go through the proxy,
        # and the engine interleaves JSON-RPC replies with push notifications.
        connection = cls.__new__(cls)
        connection.websocket_url = f"wss://{host}:{port}/app"
        connection._match_response_id = True
        sslopt: Dict[str, object] = {
            "certfile": client_cert,
            "keyfile": client_key,
            "ca_certs": root_cert,
            "cert_reqs": ssl.CERT_REQUIRED,
        }
        if assert_hostname is False:
            sslopt["check_hostname"] = False
        elif assert_hostname:
            sslopt["server_hostname"] = assert_hostname
        connection.socket_connection = create_connection(
            f"{connection.websocket_url}/{app_id}",
            header={"X-Qlik-User": user_header},
            sslopt=sslopt,
            # The pure-Python frame check costs more CPU than the rest of an app
            # read, and recv() already rejects invalid UTF-8 when it decodes.
            skip_utf8_validation=True,
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
            message = json.loads(resp)
            if "error" in message:
                raise RuntimeError(str(message["error"]))
            return message[Constant.RESULT]
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

    def get_script(self) -> str:
        """Load script of the open app. Must be called on the app handle."""
        response = self.websocket_send_request(method="GetScript")
        return response.get(Constant.QSCRIPT) or ""

    def get_charts(
        self, object_id: str, sheet_id: str, on_error: Callable[[str], None]
    ) -> List[Chart]:
        """Charts of a sheet object, including the charts held in a container's tabs.
        Must be called on the handle of the object's parent."""
        depth = len(self.handle)
        try:
            self.websocket_send_request(method="GetChild", params={"qId": object_id})
            q_layout = self.websocket_send_request(method="GetLayout")[Constant.QLAYOUT]
            if Constant.HYPERCUBE in q_layout:
                chart = Chart.model_validate(q_layout)
                self._add_measure_definitions(chart, sheet_id, on_error)
                return [chart]
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
            for item in q_layout.get(Constant.QCHILDLIST, {}).get(Constant.QITEMS, []):
                tab_id = item.get(Constant.QDATA, {}).get(Constant.CONTAINERCHILDID)
                for chart in self.get_charts(
                    item[Constant.QINFO][Constant.QID], sheet_id, on_error
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
        finally:
            del self.handle[depth:]

    def _add_measure_definitions(
        self, chart: Chart, sheet_id: str, on_error: Callable[[str], None]
    ) -> None:
        # The layout only carries measure titles; the expressions are in the properties.
        try:
            properties = self.websocket_send_request(method="GetProperties")[
                Constant.QPROPERTY
            ]
        except Exception as e:
            on_error(
                f"Unable to read the measures of chart {chart.qId} of sheet {sheet_id}. Exception: {e}"
            )
            return
        for measure in properties.get(Constant.QHYPERCUBEDEF, {}).get(
            Constant.QMEASURES, []
        ):
            if measure.get(Constant.QLIBRARYID):
                chart.measure_library_ids.append(measure[Constant.QLIBRARYID])
            elif measure.get(Constant.QDEF, {}).get(Constant.QDEF):
                chart.field_expressions.append(measure[Constant.QDEF][Constant.QDEF])

    def add_master_measure_definitions(
        self, sheets: List[Sheet], on_error: Callable[[str], None]
    ) -> None:
        """Fill in the expressions of master measures used by the charts.
        Must be called on the app handle."""
        charts = [chart for sheet in sheets for chart in sheet.charts]
        expressions: Dict[str, str] = {}
        for library_id in dict.fromkeys(
            library_id for chart in charts for library_id in chart.measure_library_ids
        ):
            depth = len(self.handle)
            try:
                self.websocket_send_request(
                    method="GetMeasure", params={"qId": library_id}
                )
                q_layout = self.websocket_send_request(method="GetLayout")[
                    Constant.QLAYOUT
                ]
                expression = q_layout.get(Constant.QMEASURE, {}).get(Constant.QDEF)
                if expression:
                    expressions[library_id] = expression
            except Exception as e:
                on_error(f"Unable to read master measure {library_id}. Exception: {e}")
            finally:
                del self.handle[depth:]
        for chart in charts:
            chart.field_expressions.extend(
                expressions[library_id]
                for library_id in chart.measure_library_ids
                if library_id in expressions
            )

    def close_websocket(self) -> None:
        if self.socket_connection:
            self.socket_connection.close()
