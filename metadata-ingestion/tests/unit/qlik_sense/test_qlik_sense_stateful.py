from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
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

    matching = [
        processor
        for processor in processors
        if isinstance(
            getattr(processor, "__self__", None), AutoStaleEntityRemovalProcessor
        )
    ]
    assert len(matching) == 1


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


def test_engine_outage_does_not_soft_delete_app_contents(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    _, first_wus = _onprem_run(mock_datahub_graph, "run-1", engine_fails=False)
    second, second_wus = _onprem_run(mock_datahub_graph, "run-2", engine_fails=True)

    assert any(":dashboard:" in wu.get_urn() for wu in first_wus)
    assert not any(":dashboard:" in wu.get_urn() for wu in second_wus)
    assert not [wu for wu in second_wus if wu.id.startswith("soft-delete-")]
    (failure,) = second.reporter.failures
    assert failure.title == "Unable to fully read Qlik app"
    (context,) = failure.context
    assert context.startswith("app-1: ")
    assert "engine port closed" in context


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
