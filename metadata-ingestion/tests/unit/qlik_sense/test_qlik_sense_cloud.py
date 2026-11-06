from functools import partial
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest
import requests
import requests_mock as requests_mock_module

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    QlikTable,
    Sheet,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import ReadErrors
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import infer_data_connection
from datahub.ingestion.source.qlik_sense.websocket_connection import (
    EngineError,
    WebsocketConnection,
)
from datahub.metadata.schema_classes import UpstreamLineageClass as UpstreamLineage

CLOUD_CONFIG = {"tenant_hostname": "tenant.example.com", "api_key": "key"}


@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_load_model_failure_leaves_app_handle_on_top(
    mock_session_cls: MagicMock,
) -> None:
    api = QlikAPI(
        QlikSourceConfig.model_validate(
            {"tenant_hostname": "tenant.example.com", "api_key": "key"}
        )
    )
    websocket = MagicMock()
    websocket._handles = [-1, 1]
    websocket.restore_handle = partial(WebsocketConnection.restore_handle, websocket)
    replies: List[object] = [
        {"qReturn": {"qHandle": 2, "qType": "LoadModel"}},
        RuntimeError("Access denied"),
    ]

    def send(method: str, params: object = None) -> object:
        reply = replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        websocket._handles.append(2)
        return reply

    websocket.websocket_send_request.side_effect = send

    errors = ReadErrors()
    assert api._get_app_used_tables(websocket, "app-1", errors) == []
    assert websocket._handles == [-1, 1]
    assert len(errors.messages) == 1


@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_missing_load_model_leaves_app_handle_on_top(
    mock_session_cls: MagicMock,
) -> None:
    api = QlikAPI(
        QlikSourceConfig.model_validate(
            {"tenant_hostname": "tenant.example.com", "api_key": "key"}
        )
    )
    websocket = MagicMock()
    websocket._handles = [-1, 1]
    websocket.restore_handle = partial(WebsocketConnection.restore_handle, websocket)

    def send(method: str, params: object = None) -> object:
        websocket._handles.append(2)
        return {"qReturn": {"qHandle": 2, "qType": None}}

    websocket.websocket_send_request.side_effect = send

    errors = ReadErrors()
    assert api._get_app_used_tables(websocket, "app-1", errors) == []
    assert websocket._handles == [-1, 1]
    assert errors.messages == []


@pytest.mark.parametrize(
    "call, unread",
    [
        ("get_spaces", "spaces"),
        ("get_data_connections", "data connections"),
        ("get_items", "items of space space-1"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_failed_request_is_recorded_as_unread(
    mock_session_cls: MagicMock, call: str, unread: str
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    mock_session_cls.return_value.get.side_effect = requests.ConnectionError("down")

    getattr(api, call)(*(["space-1"] if call == "get_items" else []))

    assert api.unread_items == [UnreadItem(name=unread, cause="down")]


@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_partly_read_app_is_marked_incomplete(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    app = MagicMock(tables=[])
    mock_app_cls.model_validate.return_value = app

    def unreadable_sheets(
        websocket: object, app_id: str, errors: ReadErrors
    ) -> List[Sheet]:
        errors.record("Unable to fetch sheets")
        return []

    with (
        patch.object(api, "_get_app_sheets", side_effect=unreadable_sheets),
        patch.object(api, "_get_app_used_tables", return_value=[]),
        patch.object(api, "_read_load_script", return_value=None),
    ):
        assert api._get_app("app-1") is app

    assert app.read_errors == ["Unable to fetch sheets"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_items_are_reported_as_failures(mock_api_cls: MagicMock) -> None:
    mock_api_cls.return_value.get_spaces.return_value = []
    mock_api_cls.return_value.unread_items = [
        UnreadItem(name="spaces", cause="503 Server Error"),
        UnreadItem(name="data connections", cause="timed out"),
    ]
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    list(source.get_workunits_internal())

    (failure,) = source.reporter.failures
    assert failure.title == "Unable to read from Qlik"
    assert list(failure.context) == [
        "spaces: 503 Server Error",
        "data connections: timed out",
    ]


@pytest.mark.parametrize(
    "error, denied",
    [
        (EngineError({"code": 5, "message": "Access denied"}), True),
        (EngineError({"code": 11, "message": "Internal error"}), False),
        (TimeoutError("timed out"), False),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_only_denied_load_script_keeps_app_complete(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
    error: Exception,
    denied: bool,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    app = _cloud_app()
    mock_app_cls.model_validate.return_value = app
    websocket = mock_websocket_cls.for_cloud.return_value
    websocket.get_script.side_effect = error

    with (
        patch.object(api, "_get_app_sheets", return_value=[]),
        patch.object(api, "_get_app_used_tables", return_value=[]),
    ):
        assert api._get_app("app-1") is app

    assert app.script is None
    assert app.script_read_failed is denied
    assert app.read_complete is denied


def _cloud_app(**extra: object) -> App:
    return App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "qUsage": "ANALYTICS",
            "ownerId": "user-1",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            **extra,
        }
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_denied_load_script_is_a_warning(mock_api_cls: MagicMock) -> None:
    space = MagicMock(id="space-1")
    space.name = "Finance"
    mock_api_cls.return_value.get_spaces.return_value = [space]
    mock_api_cls.return_value.get_items.return_value = [
        _cloud_app(script_read_failed=True)
    ]
    mock_api_cls.return_value.unread_items = []
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    with patch.object(source, "_gen_space_workunit", return_value=[]):
        list(source.get_workunits_internal())

    assert not source.reporter.failures
    assert [warning.title for warning in source.reporter.warnings] == [
        "Unable to read Qlik app load script"
    ]


def _data_connection(name: str, space: str, datasource: str) -> Dict[str, str]:
    return {"qName": name, "space": space, "datasourceID": datasource}


def test_data_connections_are_keyed_by_space(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    base_url = "https://tenant.example.com/api/v1"
    url = f"{base_url}/data-connections"
    requests_mock.get(f"{base_url}/api-keys", json={})
    requests_mock.get(
        url,
        complete_qs=True,
        json={
            "data": [
                _data_connection("Warehouse", "space-1", "snowflake"),
                _data_connection("Orders", "space-1", "sqlserver"),
            ],
            "links": {"next": {"href": f"{url}?next=2"}},
        },
    )
    requests_mock.get(
        f"{url}?next=2",
        json={
            "data": [_data_connection("Warehouse", "space-2", "postgres")],
            "links": {},
        },
    )
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    api.spaces = {"space-1": "Finance", "space-2": "Sales"}

    connections = api.get_data_connections()

    assert {name: info.platform for name, info in connections.items()} == {
        "Finance:Warehouse": "snowflake",
        "Finance:Orders": "mssql",
        "Sales:Warehouse": "postgres",
        "Orders": "mssql",
    }


def test_data_connections_page_without_data_is_empty(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    base_url = "https://tenant.example.com/api/v1"
    requests_mock.get(f"{base_url}/api-keys", json={})
    requests_mock.get(f"{base_url}/data-connections", json={"links": {}})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))

    assert api.get_data_connections() == {}
    assert api.unread_items == []


@pytest.mark.parametrize(
    "app_space, platform", [("space-1", "snowflake"), ("space-3", None)]
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_bare_connection_name_resolves_in_app_space(
    mock_api_cls: MagicMock, app_space: str, platform: str
) -> None:
    mock_api_cls.return_value.spaces = {
        "space-1": "Finance",
        "space-2": "Sales",
        "space-3": "Marketing",
    }
    # Two spaces hold a "Warehouse" connection, so the bare name is not
    # listed on its own.
    mock_api_cls.return_value.get_data_connections.return_value = {
        "Finance:Warehouse": infer_data_connection("Warehouse", "snowflake"),
        "Sales:Warehouse": infer_data_connection("Warehouse", "postgres"),
    }
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    resolved = source._resolve_script_connection("Warehouse", app_space)

    assert (resolved.platform if resolved else None) == platform


def _blackbox_table(select: str) -> QlikTable:
    return QlikTable.model_validate(
        {
            "tableName": "orders",
            "boxType": "blackbox",
            "tableAlias": "orders",
            "dataconnectorName": "Warehouse",
            "connectionInfo": {
                "id": "conn-1",
                "sourceConnectorID": "gbq",
                "space": "space-1",
            },
            "fields": [{"name": "order_id"}],
            "selectStatement": select,
        }
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_lineage_uses_connection_mapping(mock_api_cls: MagicMock) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {
                **CLOUD_CONFIG,
                "data_connection_to_platform_instance": {
                    "Warehouse": {
                        "platform": "snowflake",
                        "database": "analytics",
                        "default_schema": "public",
                    }
                },
            }
        ),
        PipelineContext(run_id="test-cloud"),
    )
    downstream = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)"

    workunit = source._gen_app_table_upstream_lineage(
        downstream, _blackbox_table("SELECT order_id FROM orders")
    )
    assert workunit is not None
    lineage = workunit.get_aspect_of_type(UpstreamLineage)
    assert lineage is not None
    assert [upstream.dataset for upstream in lineage.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.orders,PROD)"
    ]

    assert (
        source._gen_app_table_upstream_lineage(
            downstream, _blackbox_table("SELECT FROM WHERE")
        )
        is None
    )
    assert len(source.reporter.warnings) == 1
