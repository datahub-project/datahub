from typing import Dict, Iterable, List, Optional, Set, Tuple, Type
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    StatusClass,
    _Aspect,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from tests.test_helpers.state_helpers import mock_datahub_graph  # noqa: F401
from tests.unit.qlik_sense.test_qlik_sense_onprem import (  # type: ignore[import-untyped]
    APP,
    DATA_CONNECTION,
    LOAD_SCRIPT,
    ONPREM_CONFIG,
    STREAM,
    _engine_calls,
    _Response,
)

CONFIG = {
    **ONPREM_CONFIG,
    "skip_unchanged_apps": True,
    "stateful_ingestion": {"enabled": True, "remove_stale_metadata": True},
}
APP_OBJECTS = [
    {
        "engineObjectId": "sheet-1",
        "objectType": "sheet",
        "name": "Overview",
        "modifiedDate": "2024-01-29T12:23:46.868Z",
    }
]


class _Qlik:
    """QRS and Engine stand-in whose app can be edited between runs."""

    def __init__(self) -> None:
        self.app = dict(APP)
        self.objects = [dict(obj) for obj in APP_OBJECTS]
        self.engine_opens = 0
        self.engine_fails = False

    def qrs_get(
        self, url: str, params: Optional[Dict[str, str]] = None, **_: object
    ) -> _Response:
        if url.endswith("/about"):
            return _Response({"buildVersion": "14.129.3"})
        if url.endswith("/stream/full"):
            return _Response([STREAM])
        if url.endswith("/app/full"):
            return _Response([self.app])
        if url.endswith("/app/object/full"):
            assert params is not None and params["filter"] == "app.id eq app-1"
            return _Response(self.objects)
        if url.endswith("/dataconnection/full"):
            return _Response([DATA_CONNECTION])
        raise AssertionError(url)

    def open_engine(self, **_: object) -> MagicMock:
        self.engine_opens += 1
        if self.engine_fails:
            raise OSError("engine port closed")
        websocket = MagicMock()
        websocket.websocket_send_request.side_effect = _engine_calls()
        websocket.get_script.return_value = LOAD_SCRIPT
        return websocket


class _DataHub:
    """Aspects written by previous runs, served back through graph.get_aspect."""

    def __init__(self, graph: MagicMock) -> None:
        self.aspects: Dict[Tuple[str, str], _Aspect] = {}
        self.soft_deleted: Set[str] = set()
        graph.get_aspect.side_effect = self.get_aspect
        graph.get_urns_by_filter.side_effect = self.soft_deleted_urns
        graph._make_schema_resolver.side_effect = (
            lambda platform, platform_instance, env, **_: SchemaResolver(
                platform=platform, platform_instance=platform_instance, env=env
            )
        )

    def get_aspect(self, urn: str, aspect_type: Type[_Aspect]) -> Optional[_Aspect]:
        return self.aspects.get((urn, aspect_type.ASPECT_NAME))

    def soft_deleted_urns(self, **_: object) -> Iterable[str]:
        return iter(sorted(self.soft_deleted))

    def write(self, workunits: List[MetadataWorkUnit]) -> None:
        for wu in workunits:
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                assert wu.metadata.entityUrn and wu.metadata.aspect
                self.aspects[
                    (wu.metadata.entityUrn, wu.metadata.aspect.ASPECT_NAME)
                ] = wu.metadata.aspect


def _run(
    graph: MagicMock, qlik: _Qlik, run_id: str, **overrides: object
) -> Tuple[QlikSenseSource, List[MetadataWorkUnit]]:
    with (
        patch(
            "datahub.ingestion.source.qlik_sense.qlik_onprem_api.requests.Session"
        ) as session_cls,
        patch(
            "datahub.ingestion.source.qlik_sense.qlik_onprem_api.WebsocketConnection.for_on_prem",
            side_effect=qlik.open_engine,
        ),
    ):
        session_cls.return_value.headers = {}
        session_cls.return_value.get.side_effect = qlik.qrs_get
        ctx = PipelineContext(run_id=run_id, pipeline_name="qlik-onprem", graph=graph)
        source = QlikSenseSource(
            QlikSourceConfig.model_validate({**CONFIG, **overrides}), ctx
        )
        workunits = list(source.get_workunits())
        source.close()
        for _, committable in ctx.get_committables():
            committable.commit()
    return source, workunits


def _stale_urns(source: QlikSenseSource) -> List[str]:
    assert source.stale_entity_removal_handler is not None
    checkpoint = source.state_provider.get_current_checkpoint(
        source.stale_entity_removal_handler.job_id
    )
    assert checkpoint is not None
    assert isinstance(checkpoint.state, GenericCheckpointState)
    return checkpoint.state.urns


def test_unchanged_app_is_skipped_and_kept(mock_datahub_graph: MagicMock) -> None:  # noqa: F811
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    second, second_wus = _run(mock_datahub_graph, qlik, "run-2")

    assert qlik.engine_opens == 1
    assert second.reporter.apps_skipped_unchanged == 1
    assert not [wu for wu in second_wus if wu.id.startswith("soft-delete-")]
    app_urns = set(_stale_urns(first))
    assert any(":chart:" in urn or ":dashboard:" in urn for urn in app_urns)
    assert app_urns <= set(_stale_urns(second))


def _container_urn(source: QlikSenseSource) -> str:
    return source._gen_app_key("app-1").as_urn()


@pytest.mark.parametrize(
    "change",
    [
        "app_saved",
        "sheet_edited",
        "metadata_not_in_datahub",
        "first_read_failed",
        "recipe_changed",
        "container_soft_deleted",
        "sheet_soft_deleted",
        "datahub_fingerprint_differs",
        "ignore_old_state",
    ],
)
def test_app_is_read_again(mock_datahub_graph: MagicMock, change: str) -> None:  # noqa: F811
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)
    qlik.engine_fails = change == "first_read_failed"

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    if change != "metadata_not_in_datahub":
        datahub.write(first_wus)
    qlik.engine_fails = False
    container_urn = _container_urn(first)
    overrides: Dict[str, object] = {}
    if change == "app_saved":
        qlik.app["modifiedDate"] = "2024-02-01T09:00:00.000Z"
    if change == "sheet_edited":
        qlik.objects[0]["modifiedDate"] = "2024-02-01T09:00:00.000Z"
    if change == "recipe_changed":
        overrides["env"] = "DEV"
    if change == "container_soft_deleted":
        datahub.aspects[(container_urn, StatusClass.ASPECT_NAME)] = StatusClass(
            removed=True
        )
    if change == "sheet_soft_deleted":
        datahub.soft_deleted.add(
            next(urn for urn in _stale_urns(first) if ":dashboard:" in urn)
        )
    if change == "datahub_fingerprint_differs":
        properties = datahub.aspects[
            (container_urn, ContainerPropertiesClass.ASPECT_NAME)
        ]
        assert isinstance(properties, ContainerPropertiesClass)
        properties.customProperties["ingestionFingerprint"] = "from-another-run"
    if change == "ignore_old_state":
        overrides["stateful_ingestion"] = {
            **CONFIG["stateful_ingestion"],  # type: ignore[dict-item]
            "ignore_old_state": True,
        }
    second, _ = _run(mock_datahub_graph, qlik, "run-2", **overrides)

    assert qlik.engine_opens == 2
    assert second.reporter.apps_skipped_unchanged == 0
    assert second.reporter.apps_read == 1


def test_failed_read_keeps_entities_from_last_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    qlik.app["modifiedDate"] = "2024-02-01T09:00:00.000Z"
    qlik.engine_fails = True
    second, second_wus = _run(mock_datahub_graph, qlik, "run-2")
    qlik.engine_fails = False
    third, _ = _run(mock_datahub_graph, qlik, "run-3")

    assert not [wu for wu in second_wus if wu.id.startswith("soft-delete-")]
    assert set(_stale_urns(first)) <= set(_stale_urns(second))
    assert third.reporter.apps_read == 1
    assert qlik.engine_opens == 3
