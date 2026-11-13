import json
import logging
import ssl
import time
from contextlib import contextmanager
from typing import Callable, Dict, Iterator, List, Literal, Optional, Set, Union

from websocket import (
    WebSocket,
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
    create_connection,
)

from datahub.ingestion.source.qlik_sense.config import Constant
from datahub.ingestion.source.qlik_sense.data_classes import Chart, Sheet

logger = logging.getLogger(__name__)

# Engine JSON-RPC error code LOCERR_GENERIC_ACCESS_DENIED.
ENGINE_ACCESS_DENIED = 5

# The Engine's connection can time out or drop while it is being opened, as
# happens on busy tenants. Nothing has been sent yet, so it is safe to retry.
# Authentication and certificate errors are not retried.
CONNECT_ATTEMPTS = 3
CONNECT_RETRY_SECONDS = 2.0
_TRANSIENT_CONNECT_ERRORS = (
    TimeoutError,
    ConnectionError,
    WebSocketTimeoutException,
    WebSocketConnectionClosedException,
)


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
        self.socket_connection: Optional[WebSocket] = self._connect(
            url, header, sslopt or {}, timeout
        )
        self.request_id = 0
        # Engine object handles, innermost last; -1 is the global handle.
        self._handles = [-1]

    @staticmethod
    def _connect(
        url: str,
        header: Dict[str, str],
        sslopt: Dict[str, object],
        timeout: Optional[float],
    ) -> WebSocket:
        def connect() -> WebSocket:
            return create_connection(
                url,
                header=header,
                sslopt=sslopt,
                # The pure-Python frame check costs more CPU than the rest of
                # an app read, and recv() already rejects invalid UTF-8 when it
                # decodes.
                skip_utf8_validation=True,
                timeout=timeout,
            )

        for attempt in range(1, CONNECT_ATTEMPTS):
            try:
                return connect()
            except _TRANSIENT_CONNECT_ERRORS as e:
                logger.info(
                    f"Retrying Engine connection to {url} after attempt "
                    f"{attempt} failed: {e}"
                )
                time.sleep(CONNECT_RETRY_SECONDS * attempt)
        return connect()

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

    def get_variables(self) -> Dict[str, str]:
        """Definitions of the app's variables, by name, as the app was last
        saved or reloaded, including variables set in included script files.
        Must be called on the app handle."""
        with self.restore_handle():
            self.websocket_send_request(
                method="CreateSessionObject",
                params={
                    "qProp": {
                        "qInfo": {"qType": "VariableList"},
                        "qVariableListDef": {
                            "qType": "variable",
                            "qShowReserved": False,
                            "qShowConfig": False,
                        },
                    }
                },
            )
            layout = self.websocket_send_request(method="GetLayout")
        items = layout[Constant.QLAYOUT]["qVariableList"]["qItems"]
        return {item["qName"]: item.get("qDefinition") or "" for item in items}

    def get_data_model_tables(self) -> List[Dict]:
        """The tables of the app's data model, with their fields, as the last
        reload left them. Needs the app opened with its data."""
        response = self.websocket_send_request(
            method="GetTablesAndKeys",
            params={
                "qWindowSize": {"qcx": 0, "qcy": 0},
                "qNullSize": {"qcx": 0, "qcy": 0},
                "qCellHeight": 0,
                "qSyntheticMode": False,
                "qIncludeSysVars": False,
            },
        )
        return response.get("qtr") or []

    def get_object_properties(
        self, on_fallback: Callable[[str], None]
    ) -> Dict[str, Dict]:
        """Properties of the open object and every object below it, by id, in one
        request rather than one per chart. Empty if they cannot be read."""
        try:
            entry = self.websocket_send_request(method="GetFullPropertyTree")[
                Constant.QPROPENTRY
            ]
        except Exception as e:
            on_fallback(
                "Unable to read the property tree, so chart properties are read "
                f"one by one. Exception: {e}"
            )
            return {}
        properties: Dict[str, Dict] = {}
        entries = [entry]
        while entries:
            entry = entries.pop()
            prop = entry.get(Constant.QPROPERTYTREEITEM, {})
            object_id = prop.get(Constant.QINFO, {}).get(Constant.QID)
            if object_id:
                properties[object_id] = prop
            entries.extend(entry.get(Constant.QCHILDREN, []))
        return properties

    def get_charts(
        self,
        object_id: str,
        sheet_id: str,
        on_error: Callable[[str], None],
        properties: Optional[Dict[str, Dict]] = None,
        seen: Optional[Set[str]] = None,
        on_measure_error: Optional[Callable[[str], None]] = None,
    ) -> List[Chart]:
        """Charts of a sheet object, including the charts held in a container's tabs.
        Must be called on the handle of the object's parent. `properties` holds
        prefetched object properties; a chart missing from it is read on its own.
        `on_error` reports a chart that could not be read; `on_measure_error`
        reports a chart that was read but whose measures were not."""
        on_measure_error = on_measure_error or on_error
        # A container can hold a link that resolves back to itself.
        seen = set() if seen is None else seen
        if object_id in seen:
            return []
        seen.add(object_id)
        prop = (properties or {}).get(object_id)
        if (
            prop is not None
            and Constant.QHYPERCUBEDEF not in prop
            and not prop.get(Constant.QEXTENDSID)
            and prop.get(Constant.QINFO, {}).get(Constant.QTYPE) != Constant.CONTAINER
        ):
            # Getting a layout makes the Engine calculate the object, which is
            # slow for filter panes and other objects that are not charts.
            logger.debug(
                f"Skipping object {object_id} of sheet {sheet_id}: not a chart"
            )
            return []
        with self.restore_handle():
            try:
                self.websocket_send_request(
                    method="GetChild", params={"qId": object_id}
                )
                q_layout = self.websocket_send_request(method="GetLayout")[
                    Constant.QLAYOUT
                ]
                if Constant.HYPERCUBE in q_layout:
                    chart = Chart.model_validate(q_layout)
                    self._add_measure_definitions(
                        chart,
                        (properties or {}).get(chart.qId),
                        sheet_id,
                        on_measure_error,
                    )
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
                for item in q_layout.get(Constant.QCHILDLIST, {}).get(
                    Constant.QITEMS, []
                ):
                    tab_id = item.get(Constant.QDATA, {}).get(Constant.CONTAINERCHILDID)
                    for chart in self.get_charts(
                        item[Constant.QINFO][Constant.QID],
                        sheet_id,
                        on_error,
                        properties,
                        seen,
                        on_measure_error,
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

    def _add_measure_definitions(
        self,
        chart: Chart,
        properties: Optional[Dict],
        sheet_id: str,
        on_error: Callable[[str], None],
    ) -> None:
        # The layout only carries measure titles; the expressions are in the properties.
        try:
            if properties is None:
                properties = self.websocket_send_request(method="GetProperties")[
                    Constant.QPROPERTY
                ]
            if properties.get(Constant.QEXTENDSID):
                # A linked master visualization keeps its hypercube on the master.
                properties = self.websocket_send_request(
                    method="GetEffectiveProperties"
                )[Constant.QPROPERTY]
        except Exception as e:
            on_error(
                f"Unable to read the measures of chart {chart.qId} of sheet {sheet_id}. Exception: {e}"
            )
            chart.fields_complete = False
            chart.fields_unread = True
            return
        measures = properties.get(Constant.QHYPERCUBEDEF, {}).get(
            Constant.QMEASURES, []
        )
        if len(measures) < len(chart.qMeasure):
            chart.fields_complete = False
            chart.fields_unread = True
        for measure in measures:
            if measure.get(Constant.QLIBRARYID):
                chart.measure_library_ids.append(measure[Constant.QLIBRARYID])
            elif measure.get(Constant.QDEF, {}).get(Constant.QDEF):
                chart.field_expressions.append(measure[Constant.QDEF][Constant.QDEF])
            else:
                chart.fields_complete = False

    def add_master_measure_definitions(
        self,
        sheets: List[Sheet],
        on_error: Callable[[str], None],
        on_fallback: Callable[[str], None],
    ) -> None:
        """Fill in the expressions of master measures used by the charts.
        Must be called on the app handle."""
        charts = [chart for sheet in sheets for chart in sheet.charts]
        library_ids = list(
            dict.fromkeys(
                library_id
                for chart in charts
                for library_id in chart.measure_library_ids
            )
        )
        if not library_ids:
            return
        listed = self._get_master_measure_list(on_fallback)
        unread: Set[str] = set()
        expressions = {
            library_id: expression
            for library_id, expression in (listed or {}).items()
            if expression
        }
        for library_id in library_ids:
            if library_id in expressions:
                continue
            if listed is not None and library_id not in listed:
                # The chart refers to a master measure that was deleted, so
                # there is nothing to read.
                logger.debug(f"Master measure {library_id} does not exist")
                continue
            with self.restore_handle():
                try:
                    reply = self.websocket_send_request(
                        method="GetMeasure", params={"qId": library_id}
                    )
                    if not reply.get(Constant.QRETURN, {}).get(Constant.QHANDLE):
                        logger.debug(f"Master measure {library_id} does not exist")
                        continue
                    q_layout = self.websocket_send_request(method="GetLayout")[
                        Constant.QLAYOUT
                    ]
                    expression = q_layout.get(Constant.QMEASURE, {}).get(Constant.QDEF)
                    if expression:
                        expressions[library_id] = expression
                except Exception as e:
                    on_error(
                        f"Unable to read master measure {library_id}. Exception: {e}"
                    )
                    unread.add(library_id)
        for chart in charts:
            for library_id in chart.measure_library_ids:
                if library_id in expressions:
                    chart.field_expressions.append(expressions[library_id])
                else:
                    chart.fields_complete = False
                    chart.fields_unread = chart.fields_unread or library_id in unread

    def _get_master_measure_list(
        self, on_fallback: Callable[[str], None]
    ) -> Optional[Dict[str, str]]:
        """Every master measure of the app and its expression, empty if the list has
        none, from one session object. None if it cannot be read.
        Must be called on the app handle."""
        with self.restore_handle():
            try:
                self.websocket_send_request(
                    method="CreateSessionObject",
                    params=[
                        {
                            Constant.QINFO: {Constant.QTYPE: "MeasureList"},
                            "qMeasureListDef": {
                                Constant.QTYPE: "measure",
                                Constant.QDATA: {"measure": "/qMeasure"},
                            },
                        }
                    ],
                )
                items = self.websocket_send_request(method="GetLayout")[
                    Constant.QLAYOUT
                ]["qMeasureList"][Constant.QITEMS]
                return {
                    item[Constant.QINFO][Constant.QID]: item.get(Constant.QDATA, {})
                    .get("measure", {})
                    .get(Constant.QDEF)
                    or ""
                    for item in items
                }
            except Exception as e:
                on_fallback(
                    "Unable to list master measures, so they are read one by one. "
                    f"Exception: {e}"
                )
                return None

    def close_websocket(self) -> None:
        if self.socket_connection:
            self.socket_connection.close()
