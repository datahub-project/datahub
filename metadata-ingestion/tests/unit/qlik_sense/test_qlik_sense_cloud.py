import io
import ssl
from functools import partial
from typing import Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import requests
import requests_mock as requests_mock_module
from requests.adapters import HTTPAdapter
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import NewConnectionError, ProtocolError
from urllib3.response import HTTPResponse
from urllib3.util.retry import Retry

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.app_state import AppCheckpoint
from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    QlikTable,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import ReadErrors
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ResolvedConnection,
    ScriptTable,
    infer_data_connection,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import (
    CONNECT_ATTEMPTS,
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

    list(getattr(api, call)(*([["space-1"]] if call == "get_items" else [])))

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
    app = _cloud_app()
    mock_app_cls.model_validate.return_value = app

    def engine_reply(method: str, params: object = None) -> Dict:
        if method == "GetObjects":
            raise RuntimeError("Engine closed the session")
        return {"qLayout": {}}

    mock_websocket_cls.for_cloud.return_value.websocket_send_request.side_effect = (
        engine_reply
    )
    mock_websocket_cls.for_cloud.return_value.get_script.return_value = ""
    with (
        patch.object(api, "_get_app_used_tables", return_value=[]),
        patch.object(api, "get_table_lineage", return_value=[]),
        patch.object(api, "_read_data_model", return_value=[]),
        patch.object(api, "_read_variables", return_value={}),
    ):
        assert api._get_app("app-1") is app

    assert len(app.read_errors) == 1
    assert "Unable to fetch sheets" in app.read_errors[0]
    # The app dashboard must not replace the sheet edges of the last full read.
    assert not app.sheets_complete


@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_unread_data_model_marks_app_tables_unread(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    app = _cloud_app()
    mock_app_cls.model_validate.return_value = app

    def unreadable_tables(
        websocket: object, app_id: str, errors: ReadErrors
    ) -> List[QlikTable]:
        errors.record("Unable to fetch tables")
        return []

    with (
        patch.object(api, "_get_app_sheets", return_value=[]),
        patch.object(api, "_get_app_used_tables", side_effect=unreadable_tables),
    ):
        api._get_app("app-1")

    assert app.tables_read_failed
    assert not app.read_complete


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_items_are_reported_as_failures(mock_api_cls: MagicMock) -> None:
    mock_api_cls.return_value.get_spaces.return_value = []
    mock_api_cls.return_value.unread_items = [
        UnreadItem(name="spaces", cause="503 Server Error"),
        UnreadItem(name="data connections", cause="timed out"),
    ]
    mock_api_cls.return_value.engine_refusals = None
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


@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_close_error_keeps_loaded_app(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    mock_websocket_cls.for_cloud.return_value.close_websocket.side_effect = OSError(
        "closed"
    )
    with (
        patch.object(api, "_get_app_sheets", return_value=[]),
        patch.object(api, "_get_app_used_tables", return_value=[]),
    ):
        app = api._get_app("app-1")

    assert app is mock_app_cls.model_validate.return_value
    mock_websocket_cls.for_cloud.return_value.close_websocket.assert_called_once()


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
        patch.object(api, "get_table_lineage", return_value=[]),
    ):
        assert api._get_app("app-1") is app

    assert app.script is None
    assert app.script_read_failed is denied
    # A denial lasts, so charts get the data model tables. Other failures leave
    # the script tables unknown.
    assert app.tables_read_failed is not denied
    assert app.read_complete is denied


@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_unread_data_model_leaves_tables_unknown(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    app = _cloud_app()
    mock_app_cls.model_validate.return_value = app
    websocket = mock_websocket_cls.for_cloud.return_value
    websocket.get_script.return_value = "Orders: SQL SELECT * FROM orders;"
    websocket.get_data_model_tables.side_effect = TimeoutError("timed out")

    with (
        patch.object(api, "_get_app_sheets", return_value=[]),
        patch.object(api, "_get_app_used_tables", return_value=[]),
        patch.object(api, "get_table_lineage", return_value=[]),
    ):
        api._get_app("app-1")

    # Unlabelled script tables are named from the data model.
    assert app.tables_read_failed
    assert not app.read_complete


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
    mock_api_cls.return_value.failed_user_lookups = {}
    mock_api_cls.return_value.lineage_read_failures = []
    mock_api_cls.return_value.field_lineage_failures = []
    mock_api_cls.return_value.opened_without_data = []
    mock_api_cls.return_value.include_read_failures = []
    mock_api_cls.return_value.reload_statement_read_failures = []
    mock_api_cls.return_value.engine_refusals = None
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


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_included_script_is_a_warning(mock_api_cls: MagicMock) -> None:
    mock_api_cls.return_value.get_spaces.return_value = []
    mock_api_cls.return_value.unread_items = []
    mock_api_cls.return_value.failed_user_lookups = {}
    mock_api_cls.return_value.lineage_read_failures = []
    mock_api_cls.return_value.field_lineage_failures = []
    mock_api_cls.return_value.opened_without_data = []
    mock_api_cls.return_value.engine_refusals = None
    mock_api_cls.return_value.include_read_failures = [
        "lib://Common:DataFiles/x.qvs: Access denied"
    ]
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    list(source.get_workunits_internal())

    assert "Unable to read included Qlik scripts" in [
        warning.title for warning in source.reporter.warnings
    ]


# A cleared or `=` expression app value leaves the recipe's in place.
@pytest.mark.parametrize(
    "app_variables, recipe_variables",
    [
        ({"vCommon": "'Common:'"}, {}),
        ({"vCommon": ""}, {"vCommon": "Common:"}),
        ({"vCommon": "=Upper('x:')"}, {"vCommon": "Common:"}),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_app_script_holds_the_scripts_it_includes(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
    app_variables: Dict[str, str],
    recipe_variables: Dict[str, str],
) -> None:
    api = QlikAPI(
        QlikSourceConfig.model_validate(
            {**CLOUD_CONFIG, "script_variables": recipe_variables}
        )
    )
    app = _cloud_app()
    mock_app_cls.model_validate.return_value = app
    included = "Rates: LOAD * FROM [lib://Common:Lake/rates.csv];"

    def engine_reply(method: str, params: object = None) -> Dict:
        if method == "GetIncludeFileContent":
            assert params == {"qPath": "lib://Common:DataFiles/x.qvs"}
            return {"qContent": included}
        return {"qLayout": {}}

    engine = mock_websocket_cls.for_cloud.return_value
    engine.websocket_send_request.side_effect = engine_reply
    engine.get_script.return_value = "$(Include=lib://$(vCommon)DataFiles/x.qvs);"
    with (
        patch.object(api, "_get_app_sheets", return_value=[]),
        patch.object(api, "_get_app_used_tables", return_value=[]),
        patch.object(api, "get_table_lineage", return_value=[]),
        patch.object(api, "_read_data_model", return_value=[]),
        patch.object(api, "_read_variables", return_value=app_variables),
    ):
        api._get_app("app-1")

    assert app.script is not None and included in app.script
    assert "Include" not in app.script
    assert api.include_read_failures == []


@pytest.mark.parametrize("space_id, reads", [("space-1", 1), (None, 2)])
@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_personal_apps_read_their_own_includes(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
    space_id: Optional[str],
    reads: int,
) -> None:
    # An unqualified path resolves in the app's space, and each personal app
    # has its owner's.
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    mock_app_cls.model_validate.side_effect = lambda _: _cloud_app(spaceId=space_id)
    include_reads: List[object] = []

    def engine_reply(method: str, params: object = None) -> Dict:
        if method == "GetIncludeFileContent":
            include_reads.append(params)
            return {"qContent": "Rates: LOAD * FROM [lib://DataFiles/rates.csv];"}
        return {"qLayout": {}}

    engine = mock_websocket_cls.for_cloud.return_value
    engine.websocket_send_request.side_effect = engine_reply
    engine.get_script.return_value = "$(Include=lib://DataFiles/x.qvs);"
    with (
        patch.object(api, "_get_app_sheets", return_value=[]),
        patch.object(api, "_get_app_used_tables", return_value=[]),
        patch.object(api, "get_table_lineage", return_value=[]),
        patch.object(api, "_read_data_model", return_value=[]),
        patch.object(api, "_read_variables", return_value={}),
    ):
        api._get_app("app-1")
        api._get_app("app-2")

    assert len(include_reads) == reads


def _read_app_with_include_replies(
    api: QlikAPI,
    mock_websocket_cls: MagicMock,
    script: str,
    include_reply: Callable[[object], Dict],
) -> None:
    def engine_reply(method: str, params: object = None) -> Dict:
        if method == "GetIncludeFileContent":
            return include_reply(params)
        return {"qLayout": {}}

    engine = mock_websocket_cls.for_cloud.return_value
    engine.websocket_send_request.side_effect = engine_reply
    engine.get_script.return_value = script
    with (
        patch.object(api, "_get_app_sheets", return_value=[]),
        patch.object(api, "_get_app_used_tables", return_value=[]),
        patch.object(api, "get_table_lineage", return_value=[]),
        patch.object(api, "_read_data_model", return_value=[]),
        patch.object(api, "_read_variables", return_value={}),
    ):
        api._get_app("app-1")


@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_include_failing_outside_the_engine_is_read_once_an_app(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    api.spaces["space-1"] = "Finance"
    mock_app_cls.model_validate.return_value = _cloud_app()
    include_reads: List[object] = []

    def include_reply(params: object) -> Dict:
        include_reads.append(params)
        raise TimeoutError("no reply")

    _read_app_with_include_replies(
        api,
        mock_websocket_cls,
        "$(Include=lib://DataFiles/x.qvs);\n$(Include=lib://DataFiles/x.qvs);",
        include_reply,
    )

    assert len(include_reads) == 1
    assert api.include_read_failures
    assert all(
        "app-1" in entry and "Finance" in entry for entry in api.include_read_failures
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_api.inline_includes")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_app_keeps_its_script_when_inlining_fails(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
    mock_inline: MagicMock,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    app = _cloud_app()
    mock_app_cls.model_validate.return_value = app
    mock_inline.side_effect = MemoryError()
    script = "$(Include=lib://DataFiles/x.qvs);\nX: LOAD * FROM [lib://S:L/x.csv];"

    _read_app_with_include_replies(
        api, mock_websocket_cls, script, lambda params: {"qContent": ""}
    )

    assert app.script == script
    assert not app.tables_read_failed
    assert len(api.include_read_failures) == 1


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


def test_personal_data_connections_are_keyed_by_personal_space(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    base_url = "https://tenant.example.com/api/v1"
    requests_mock.get(f"{base_url}/api-keys", json={})
    requests_mock.get(
        f"{base_url}/data-connections",
        json={
            "data": [
                {"qName": "Warehouse", "space": None, "datasourceID": "snowflake"},
                _data_connection("Warehouse", "space-1", "postgres"),
            ]
        },
    )
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    api.spaces = {"space-1": "Finance", "personal-space-id": "personal_space"}

    connections = api.get_data_connections()

    assert {name: info.platform for name, info in connections.items()} == {
        "personal_space:Warehouse": "snowflake",
        "Finance:Warehouse": "postgres",
    }


@pytest.mark.parametrize(
    "app_space, platform",
    [("space-1", "snowflake"), ("space-3", None), ("personal-space-id", "redshift")],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_bare_connection_name_resolves_in_app_space(
    mock_api_cls: MagicMock, app_space: str, platform: str
) -> None:
    mock_api_cls.return_value.spaces = {
        "space-1": "Finance",
        "space-2": "Sales",
        "space-3": "Marketing",
        "personal-space-id": "personal_space",
    }
    # Two spaces hold a "Warehouse" connection, so the bare name is not
    # listed on its own.
    mock_api_cls.return_value.get_data_connections.return_value = {
        "Finance:Warehouse": infer_data_connection("Warehouse", "snowflake"),
        "Sales:Warehouse": infer_data_connection("Warehouse", "postgres"),
        "personal_space:Warehouse": infer_data_connection("Warehouse", "redshift"),
    }
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    resolved = source._resolve_script_connection("Warehouse", app_space)

    assert (resolved.platform if resolved else None) == platform


@pytest.mark.parametrize(
    "app_space, platform", [("space-1", None), ("space-unknown", "snowflake")]
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_bare_connection_name_does_not_resolve_in_another_space(
    mock_api_cls: MagicMock, app_space: str, platform: str
) -> None:
    mock_api_cls.return_value.spaces = {"space-1": "Finance", "space-2": "Sales"}
    mock_api_cls.return_value.get_data_connections.return_value = {
        "Sales:Warehouse": infer_data_connection("Warehouse", "snowflake"),
        "Warehouse": infer_data_connection("Warehouse", "snowflake"),
    }
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    resolved = source._resolve_script_connection("Warehouse", app_space)

    assert (resolved.platform if resolved else None) == platform


def _blackbox_table(select: str, **reported: str) -> QlikTable:
    return QlikTable.model_validate(
        {
            **reported,
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


@pytest.mark.parametrize(
    "reported, upstream",
    [
        pytest.param(
            {"databaseName": "sales", "schemaName": "dbo"},
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.dbo.orders,PROD)",
            id="reported_by_qlik",
        ),
        pytest.param(
            {},
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.orders,PROD)",
            id="configured",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_lineage_prefers_database_reported_by_qlik(
    mock_api_cls: MagicMock, reported: Dict[str, str], upstream: str
) -> None:
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

    workunit = source._gen_app_table_upstream_lineage(
        "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)",
        _blackbox_table("SELECT order_id FROM orders", **reported),
    )

    assert workunit is not None
    lineage = workunit.get_aspect_of_type(UpstreamLineage)
    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == [upstream]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_lineage_skips_unqualified_upstream(mock_api_cls: MagicMock) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {
                **CLOUD_CONFIG,
                "data_connection_to_platform_instance": {
                    "Warehouse": {"platform": "snowflake"}
                },
            }
        ),
        PipelineContext(run_id="test-cloud"),
    )

    assert (
        source._gen_app_table_upstream_lineage(
            "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)",
            _blackbox_table("SELECT order_id FROM orders"),
        )
        is None
    )
    assert len(source.reporter.warnings) == 1


def test_failed_owner_lookup_is_tried_once_and_reported_once(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    base_url = "https://tenant.example.com/api/v1"
    requests_mock.get(f"{base_url}/api-keys", json={})
    users = requests_mock.get(f"{base_url}/users/user-1", status_code=503)
    requests_mock.get(f"{base_url}/items?resourceType=dataset", json={"data": []})
    with patch(
        "datahub.ingestion.source.qlik_sense.qlik_sense.QlikSenseSource._create_api",
        side_effect=QlikAPI,
    ):
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(CLOUD_CONFIG),
            PipelineContext(run_id="test-cloud"),
        )

    assert source.qlik_api.get_user_name("user-1") is None
    assert source.qlik_api.get_user_name("user-1") is None
    with patch.object(source.qlik_api, "get_spaces", return_value=[]):
        list(source.get_workunits_internal())

    assert users.call_count == 1
    assert [warning.title for warning in source.reporter.warnings] == [
        "Unable to look up Qlik owners"
    ]


def _urllib3_response(status: int, method: str, url: str) -> HTTPResponse:
    return HTTPResponse(
        body=io.BytesIO(b"{}"),
        status=status,
        headers={"Content-Type": "application/json"},
        preload_content=False,
        request_method=method,
        request_url=url,
    )


def test_rate_limited_requests_are_retried_through_the_adapter() -> None:
    sent: List[str] = []

    def make_request(
        pool: HTTPConnectionPool, conn: object, method: str, url: str, **_: object
    ) -> HTTPResponse:
        sent.append(method)
        # Every first attempt is rate-limited.
        return _urllib3_response(429 if len(sent) % 2 else 200, method, url)

    with (
        patch.object(HTTPConnectionPool, "_make_request", make_request),
        patch.object(Retry, "sleep"),
    ):
        api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
        response = api.session.post(
            "https://tenant.example.com/api/v1/lineage-graphs/nodes/app/overview",
            json=["field"],
        )

    assert response.status_code == 200
    assert sent == ["GET", "GET", "POST", "POST"]


def test_dropped_connection_is_retried_through_the_adapter() -> None:
    sent: List[str] = []

    def make_request(
        pool: HTTPConnectionPool, conn: object, method: str, url: str, **_: object
    ) -> HTTPResponse:
        sent.append(url)
        if len(sent) == 1:
            raise ProtocolError("Connection aborted.")
        return _urllib3_response(200, method, url)

    with (
        patch.object(HTTPConnectionPool, "_make_request", make_request),
        patch.object(Retry, "sleep"),
    ):
        QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))

    assert len(sent) == 2


def test_app_without_lineage_graph_is_flagged_not_a_read_error(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    requests_mock.get("https://tenant.example.com/api/v1/api-keys", json={})
    requests_mock.get(
        "https://tenant.example.com/api/v1/lineage-graphs/nodes/"
        "qri%3Aapp%3Asense%3A%2F%2Fapp-1/actions/expand",
        status_code=404,
    )
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    table = _blackbox_table("SELECT order_id FROM orders")
    errors = ReadErrors()

    api._add_qri_of_tables([table], "app-1", errors)

    assert errors.messages == []
    assert errors.lineage_graph_missing
    assert table.tableQri is None


@patch("datahub.ingestion.source.qlik_sense.qlik_api.App")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.WebsocketConnection")
@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_missing_table_lineage_graph_is_checked_like_a_missing_app_graph(
    mock_session_cls: MagicMock,
    mock_websocket_cls: MagicMock,
    mock_app_cls: MagicMock,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    app = _cloud_app()
    mock_app_cls.model_validate.return_value = app

    def tables_without_graph(
        websocket: object, app_id: str, errors: ReadErrors
    ) -> List[QlikTable]:
        errors.lineage_graph_missing = True
        return [_blackbox_table("SELECT order_id FROM orders")]

    with (
        patch.object(api, "_get_app_sheets", return_value=[]),
        patch.object(api, "_get_app_used_tables", side_effect=tables_without_graph),
    ):
        api._get_app("app-1")

    # The source then compares with the last read before trusting the tables.
    assert app.lineage_graph_missing


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_data_model_app_without_lineage_graph_keeps_its_last_read(
    mock_api_cls: MagicMock,
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )
    source.app_state = MagicMock()
    source.stale_entity_removal_handler = MagicMock()
    table_urn = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)"
    source.app_state.previous.return_value = AppCheckpoint(
        urns=["urn:li:container:app-1", table_urn],
        table_urns_by_qri={"qri:db:orders": table_urn},
    )
    # Its data model has tables, but with no lineage graph none has a QRI.
    app = _cloud_app(tables=[_blackbox_table("SELECT order_id FROM orders")])
    app.lineage_graph_missing = True

    workunits = list(source._gen_item_workunits(app, "Finance", True))

    assert workunits == []
    assert "Unable to fully read Qlik app" in [
        warning.title for warning in source.reporter.warnings
    ]
    assert not list(source.reporter.failures)
    kept = source.app_state.record.call_args.args[1]
    assert kept.urns == ["urn:li:container:app-1", table_urn]
    assert kept.table_urns_by_qri == {"qri:db:orders": table_urn}
    assert kept.partial_reads == 1


def test_table_from_a_personal_space_connection_is_kept() -> None:
    table = QlikTable.model_validate(
        {
            "tableName": "orders",
            "boxType": "load-file",
            "tableAlias": "orders",
            "dataconnectorName": "Files",
            "connectionInfo": {"id": "conn-1", "sourceConnectorID": "File_AmazonS3"},
            "fields": [],
        }
    )

    assert table.spaceId == Constant.PERSONAL_SPACE_ID


def test_unreachable_tenant_is_not_retried() -> None:
    attempts: List[str] = []

    def make_request(
        pool: HTTPConnectionPool, conn: object, method: str, url: str, **_: object
    ) -> HTTPResponse:
        attempts.append(url)
        raise NewConnectionError(conn, "connection refused")  # type: ignore[arg-type]

    with (
        patch.object(HTTPConnectionPool, "_make_request", make_request),
        patch.object(Retry, "sleep"),
        pytest.raises(requests.ConnectionError),
    ):
        QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))

    assert len(attempts) == 1


@patch("datahub.ingestion.source.qlik_sense.qlik_api.requests.Session")
def test_unexpected_item_error_is_recorded_and_other_items_kept(
    mock_session_cls: MagicMock,
) -> None:
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    app = _cloud_app()

    def load(item: Dict[str, str], *_: object) -> App:
        if item["id"] == "broken":
            raise KeyError("resourceType")
        return app

    with (
        patch.object(
            api, "_list_items", return_value=[{"id": "broken"}, {"id": "app-1"}]
        ),
        patch.object(api, "_load_item", side_effect=load),
    ):
        items = list(api.get_items(["space-1"]))

    assert items == [app]
    assert [unread.name for unread in api.unread_items] == [
        "item broken of space space-1"
    ]


@pytest.mark.parametrize(
    "mapping, env",
    [
        pytest.param({"platform": "snowflake"}, "DEV", id="env_not_set"),
        pytest.param({"platform": "snowflake", "env": "QA"}, "QA", id="env_set"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_lineage_env_falls_back_to_recipe_env(
    mock_api_cls: MagicMock, mapping: Dict[str, str], env: str
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {
                **CLOUD_CONFIG,
                "env": "DEV",
                "data_connection_to_platform_instance": {"Warehouse": mapping},
            }
        ),
        PipelineContext(run_id="test-cloud"),
    )

    workunit = source._gen_app_table_upstream_lineage(
        "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)",
        _blackbox_table("SELECT order_id FROM db.s.orders"),
    )

    assert workunit is not None
    lineage = workunit.get_aspect_of_type(UpstreamLineage)
    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == [
        f"urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.orders,{env})"
    ]


def test_connection_pool_fits_max_workers() -> None:
    with patch.object(requests.Session, "get"):
        api = QlikAPI(
            QlikSourceConfig.model_validate({**CLOUD_CONFIG, "max_workers": 20})
        )

    adapter = api.session.get_adapter("https://tenant.example.com/api/v1")
    assert isinstance(adapter, HTTPAdapter)
    assert adapter.poolmanager.connection_pool_kw["maxsize"] == 20


@pytest.mark.parametrize(
    "first_error, retried",
    [
        (TimeoutError("[Errno 110] Connection timed out"), True),
        (ConnectionResetError("reset by peer"), True),
        # An authentication or certificate failure would fail again.
        (ssl.SSLCertVerificationError("certificate verify failed"), False),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.websocket_connection.time.sleep")
@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
def test_engine_connection_is_retried_only_on_transient_errors(
    mock_connect: MagicMock,
    mock_sleep: MagicMock,
    first_error: Exception,
    retried: bool,
) -> None:
    mock_connect.side_effect = [first_error, MagicMock()]

    if retried:
        WebsocketConnection.for_cloud("tenant.example.com", "key", "app-1")
        assert mock_connect.call_count == 2
    else:
        with pytest.raises(ssl.SSLCertVerificationError):
            WebsocketConnection.for_cloud("tenant.example.com", "key", "app-1")
        assert mock_connect.call_count == 1


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.time.sleep")
@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
def test_refused_cloud_engine_connection_stops_after_the_attempt_limit(
    mock_connect: MagicMock, mock_sleep: MagicMock
) -> None:
    mock_connect.side_effect = ConnectionRefusedError()

    with pytest.raises(ConnectionRefusedError):
        WebsocketConnection.for_cloud("tenant.example.com", "key", "app-1")

    assert mock_connect.call_count == CONNECT_ATTEMPTS


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_sql_with_an_unset_variable_names_it(mock_api_cls: MagicMock) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    result = source._parse_script_table(
        "app-1",
        ScriptTable(label="Orders", sql="SELECT * FROM orders_$(vYear)"),
        ResolvedConnection(platform="postgres", env="PROD"),
    )

    assert result is None
    [warning] = source.reporter.warnings
    assert "settable=vYear" in str(warning.context)
    assert "script_variables" in str(warning.message)


@pytest.mark.parametrize(
    "script_table",
    [
        pytest.param(
            ScriptTable(
                label="Orders",
                sql="SELECT * FROM orders_$(vRegion)",
                runtime_variables=["vRegion"],
            ),
            id="loop variable",
        ),
        pytest.param(
            ScriptTable(
                label="Orders", sql="SELECT * FROM orders WHERE d > $(=Today())"
            ),
            id="expression",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_sql_variable_the_recipe_cannot_set_is_not_pointed_to_it(
    mock_api_cls: MagicMock, script_table: ScriptTable
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    source._parse_script_table(
        "app-1", script_table, ResolvedConnection(platform="postgres", env="PROD")
    )

    [warning] = source.reporter.warnings
    assert "script_variables" not in str(warning.message)
    assert "settable=" not in str(warning.context)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM orders_$(v$(vRegion))",
        "SELECT * FROM orders WHERE day > $(=Date(Today()))",
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_sql_with_an_unnamed_variable_shows_the_sql(
    mock_api_cls: MagicMock, sql: str
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-cloud"),
    )

    source._parse_script_table(
        "app-1",
        ScriptTable(label="Orders", sql=sql),
        ResolvedConnection(platform="postgres", env="PROD"),
    )

    [warning] = source.reporter.warnings
    assert sql in str(warning.context)


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
def test_cloud_push_notifications_do_not_shift_replies(
    mock_connect: MagicMock,
) -> None:
    mock_connect.return_value.recv.side_effect = [
        '{"jsonrpc": "2.0", "method": "OnConnected", "params": {}}',
        '{"jsonrpc": "2.0", "id": 1, "result": {"qReturn": {"qHandle": 1}}}',
        '{"jsonrpc": "2.0", "method": "OnSessionTimedOut", "params": {}}',
        '{"jsonrpc": "2.0", "id": 2, "result": {"qLayout": {"title": "a"}}}',
        '{"jsonrpc": "2.0", "id": 3, "result": {"qLayout": {"title": "b"}}}',
    ]
    connection = WebsocketConnection.for_cloud("tenant.example.com", "key", "app-1")

    connection.websocket_send_request(method="OpenDoc")
    first = connection.websocket_send_request(method="GetLayout")
    second = connection.websocket_send_request(method="GetLayout")

    # A notification read as a reply would hand each layout to the next request.
    assert first == {"qLayout": {"title": "a"}}
    assert second == {"qLayout": {"title": "b"}}


@patch("datahub.ingestion.source.qlik_sense.websocket_connection.create_connection")
def test_engine_reply_without_result_names_the_request(
    mock_connect: MagicMock,
) -> None:
    mock_connect.return_value.recv.side_effect = [
        '{"jsonrpc": "2.0", "id": 1, "method": "OnSessionTimedOut"}',
    ]
    connection = WebsocketConnection.for_cloud("tenant.example.com", "key", "app-1")

    with pytest.raises(EngineError, match="OpenDoc.*OnSessionTimedOut"):
        connection.websocket_send_request(method="OpenDoc")
