import functools
from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    PERSONAL_SPACE_DICT,
    App,
    Sheet,
    Space,
)
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    auto_stale_entity_removal,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from tests.test_helpers.state_helpers import mock_datahub_graph  # noqa: F401
from tests.unit.qlik_sense.qlik_fakes import (
    LOAD_SCRIPT,
    ONPREM_CONFIG,
    FakeResponse,
    fake_engine_calls,
    fake_qrs_get,
)


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_stateful_ingestion_registers_stale_removal_processor(mock_qlik_api):
    config = QlikSourceConfig.model_validate(
        {
            "tenant_hostname": "tenant.example",
            "api_key": "secret",
            "stateful_ingestion": {
                "enabled": True,
                "remove_stale_metadata": True,
            },
        }
    )
    ctx = PipelineContext(
        run_id="test-stateful",
        pipeline_name="test-pipeline",
        graph=MagicMock(),
    )
    source = QlikSenseSource(config, ctx)

    processors = source.get_workunit_processors()

    # The source owns its handler, so it can keep entities of apps it skips
    # or only partly reads, and replaces the automatic processor.
    assert source.get_excluded_workunit_processors() == [
        AutoStaleEntityRemovalProcessor
    ]
    owned = [
        processor
        for processor in processors
        if isinstance(processor, functools.partial)
        and processor.func is auto_stale_entity_removal
        and processor.args == (source.stale_entity_removal_handler,)
    ]
    assert len(owned) == 1


def _onprem_run(
    graph: MagicMock,
    run_id: str,
    engine_fails: bool,
    failing_endpoint: Optional[str] = None,
) -> Tuple[QlikSenseSource, List[MetadataWorkUnit]]:
    def qrs_get(
        url: str, params: Optional[Dict[str, str]] = None, **kwargs: object
    ) -> FakeResponse:
        if failing_endpoint and url.endswith(failing_endpoint):
            raise OSError("QRS unavailable")
        return fake_qrs_get(url, params, **kwargs)

    def open_engine(**_: object) -> MagicMock:
        if engine_fails:
            raise OSError("engine port closed")
        websocket = MagicMock()
        websocket.websocket_send_request.side_effect = fake_engine_calls()
        websocket.get_script.return_value = LOAD_SCRIPT
        websocket.get_variables.return_value = {}
        return websocket

    with (
        patch(
            "datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session"
        ) as session_cls,
        patch(
            "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem",
            side_effect=open_engine,
        ),
    ):
        session_cls.return_value.headers = {}
        session_cls.return_value.get.side_effect = qrs_get
        ctx = PipelineContext(run_id=run_id, pipeline_name="qlik-onprem", graph=graph)
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(
                {
                    **ONPREM_CONFIG,
                    "stateful_ingestion": {
                        "enabled": True,
                        "remove_stale_metadata": True,
                    },
                }
            ),
            ctx,
        )
        workunits = list(source.get_workunits())
        source.close()
        for _, committable in ctx.get_committables():
            committable.commit()
    return source, workunits


def test_engine_outage_keeps_app_contents_from_last_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    _, first_wus = _onprem_run(mock_datahub_graph, "run-1", engine_fails=False)
    second, second_wus = _onprem_run(mock_datahub_graph, "run-2", engine_fails=True)

    assert any(":dashboard:" in wu.get_urn() for wu in first_wus)
    assert not any(":dashboard:" in wu.get_urn() for wu in second_wus)
    assert not [wu for wu in second_wus if wu.id.startswith("soft-delete-")]
    # The app's last read is kept, so stale removal still runs for the rest.
    assert not second.reporter.failures
    (warning,) = [
        warning
        for warning in second.reporter.warnings
        if warning.title == "Unable to fully read Qlik app"
    ]
    (context,) = warning.context
    assert context.startswith("app-1: ")
    assert "engine port closed" in context


def test_engine_outage_on_first_run_is_a_failure(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    first, first_wus = _onprem_run(mock_datahub_graph, "run-1", engine_fails=True)

    assert not [wu for wu in first_wus if wu.id.startswith("soft-delete-")]
    assert [failure.title for failure in first.reporter.failures] == [
        "Unable to fully read Qlik app"
    ]


@pytest.mark.parametrize("failing_endpoint", ["/app/full", "/dataconnection/full"])
def test_qrs_failure_does_not_soft_delete(
    mock_datahub_graph: MagicMock,  # noqa: F811
    failing_endpoint: str,
) -> None:
    _onprem_run(mock_datahub_graph, "run-1", engine_fails=False)
    second, second_wus = _onprem_run(
        mock_datahub_graph,
        "run-2",
        engine_fails=False,
        failing_endpoint=failing_endpoint,
    )

    assert not [wu for wu in second_wus if wu.id.startswith("soft-delete-")]
    assert [failure.title for failure in second.reporter.failures] == [
        "Unable to read from Qlik"
    ]


def _cloud_app(read_errors: List[str]) -> App:
    app = App.model_validate(
        {
            "id": "app-1",
            "qTitle": "Sales",
            "qUsage": "ANALYTICS",
            "ownerId": "user-1",
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
        }
    )
    if not read_errors:
        app.sheets = [
            Sheet.model_validate(
                {
                    "id": "sheet-1",
                    "title": "Overview",
                    "description": "",
                    "createdDate": "2024-01-15T11:01:49.704Z",
                    "modifiedDate": "2024-01-29T12:23:46.868Z",
                }
            )
        ]
    app.read_errors = read_errors
    return app


def _cloud_run(
    graph: MagicMock, run_id: str, read_errors: List[str]
) -> Tuple[QlikSenseSource, List[MetadataWorkUnit]]:
    with patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI") as api_cls:
        api = api_cls.return_value
        api.get_spaces.return_value = [
            Space.model_validate(
                {
                    **PERSONAL_SPACE_DICT,
                    "id": "space-1",
                    "name": "Finance",
                    "type": "shared",
                }
            )
        ]
        api.get_items.return_value = [_cloud_app(read_errors)]
        api.unread_items = []
        api.failed_user_lookups = {}
        api.change_check_failures = []
        api.lineage_read_failures = []
        api.field_lineage_failures = []
        api.opened_without_data = []
        api.get_user_name.return_value = None
        ctx = PipelineContext(run_id=run_id, pipeline_name="qlik-cloud", graph=graph)
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(
                {
                    "tenant_hostname": "tenant.example",
                    "api_key": "secret",
                    "stateful_ingestion": {
                        "enabled": True,
                        "remove_stale_metadata": True,
                    },
                }
            ),
            ctx,
        )
        workunits = list(source.get_workunits())
        source.close()
        for _, committable in ctx.get_committables():
            committable.commit()
    return source, workunits


def test_cloud_partly_read_app_keeps_last_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    _, first_wus = _cloud_run(mock_datahub_graph, "run-1", read_errors=[])
    second, second_wus = _cloud_run(
        mock_datahub_graph, "run-2", read_errors=["Unable to fetch sheets"]
    )

    sheet = "urn:li:dashboard:(qlik-sense,sheet-1)"
    assert sheet in {wu.get_urn() for wu in first_wus}
    assert sheet not in {wu.get_urn() for wu in second_wus}
    assert not [wu for wu in second_wus if wu.id.startswith("soft-delete-")]
    assert not second.reporter.failures
    assert "Unable to fully read Qlik app" in [
        warning.title for warning in second.reporter.warnings
    ]
