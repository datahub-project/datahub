import json
import pathlib
from typing import Dict, List, Optional, Set, Tuple, Type
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.qlik_sense.app_state import (
    AppCheckpoint,
    QlikAppStateHandler,
)
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import App
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    MetadataChangeProposalClass,
    StatusClass,
    UpstreamLineageClass,
    _Aspect,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import mock_datahub_graph  # noqa: F401
from tests.unit.qlik_sense.qlik_fakes import (
    APP,
    DATA_CONNECTION,
    LOAD_SCRIPT,
    ON_PREM,
    STREAM,
    FakeResponse,
    fake_engine_calls,
)

CONFIG = {
    "on_prem": {**ON_PREM, "skip_unchanged_apps": True},
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


SECOND_APP: Dict[str, object] = {**APP, "id": "app-2", "name": "Marketing"}
SECOND_STREAM = {**STREAM, "id": "stream-2", "name": "Finance"}


def _app_engine_calls(app_id: str) -> List[Dict[str, object]]:
    # Sheet ids are unique across apps in Qlik.
    return json.loads(
        json.dumps(fake_engine_calls()).replace('"sheet-1"', f'"sheet-of-{app_id}"')
    )


class _Qlik:
    """QRS and Engine stand-in whose apps can be edited between runs."""

    def __init__(self) -> None:
        self.app = dict(APP)
        self.other_apps: List[Dict[str, object]] = []
        self.objects: List[Dict[str, object]] = [dict(obj) for obj in APP_OBJECTS]
        self.data_connection = dict(DATA_CONNECTION)
        self.engine_opens = 0
        self.engine_fails = False
        self.engine_fails_for: Set[str] = set()
        self.objects_fail = False
        self.apps_without_sheets: Set[str] = set()
        self.failing_streams: Set[str] = set()
        self.scripts: Dict[str, str] = {}
        self.extra_connections: List[Dict[str, object]] = []

    def qrs_get(
        self, url: str, params: Optional[Dict[str, str]] = None, **_: object
    ) -> FakeResponse:
        if url.endswith("/about"):
            return FakeResponse({"buildVersion": "14.129.3"})
        if url.endswith("/stream/full"):
            return FakeResponse([STREAM, SECOND_STREAM])
        if url.endswith("/app/full"):
            assert params is not None
            if any(
                params["filter"] == f"stream.id eq {stream}"
                for stream in self.failing_streams
            ):
                return FakeResponse({}, status_code=503)
            return FakeResponse(
                [
                    app
                    for app in [self.app, *self.other_apps]
                    if params["filter"] == f"stream.id eq {app['stream']['id']}"  # type: ignore[index]
                ]
            )
        if url.endswith("/app/object/full"):
            assert params is not None and params["filter"].startswith("app.id eq ")
            if self.objects_fail:
                return FakeResponse({}, status_code=503)
            return FakeResponse(self.objects)
        if url.endswith("/dataconnection/full"):
            return FakeResponse([self.data_connection, *self.extra_connections])
        raise AssertionError(url)

    def open_engine(self, app_id: str, **_: object) -> MagicMock:
        self.engine_opens += 1
        if self.engine_fails or app_id in self.engine_fails_for:
            raise OSError("engine port closed")
        websocket = MagicMock()
        websocket.websocket_send_request.side_effect = (
            [{"qReturn": {"qHandle": 1}}, {"qList": []}]
            if app_id in self.apps_without_sheets
            else fake_engine_calls()
            if app_id == "app-1"
            else _app_engine_calls(app_id)
        )
        websocket.get_script.return_value = self.scripts.get(app_id, LOAD_SCRIPT)
        websocket.get_variables.return_value = {}
        return websocket


class _DataHub:
    """Aspects written by previous runs, served back through graph.get_aspect."""

    def __init__(self, graph: MagicMock) -> None:
        self.aspects: Dict[Tuple[str, str], _Aspect] = {}
        self.soft_deleted: Set[str] = set()
        graph.get_aspect.side_effect = self.get_aspect
        graph.get_entities.side_effect = self.get_entities
        graph._make_schema_resolver.side_effect = (
            lambda platform, platform_instance, env, **_: SchemaResolver(
                platform=platform, platform_instance=platform_instance, env=env
            )
        )

    def get_aspect(self, urn: str, aspect_type: Type[_Aspect]) -> Optional[_Aspect]:
        return self.aspects.get((urn, aspect_type.ASPECT_NAME))

    def get_entities(
        self, entity_name: str, urns: List[str], aspects: List[str]
    ) -> Dict[str, Dict[str, Tuple[_Aspect, None]]]:
        found: Dict[str, Dict[str, Tuple[_Aspect, None]]] = {}
        for urn in urns:
            if aspects == [StatusClass.ASPECT_NAME]:
                status = self.aspects.get((urn, StatusClass.ASPECT_NAME))
                if isinstance(status, StatusClass):
                    removed = status.removed or urn in self.soft_deleted
                    found[urn] = {
                        StatusClass.ASPECT_NAME: (StatusClass(removed=removed), None)
                    }
            else:
                assert aspects == [UpstreamLineageClass.ASPECT_NAME]
                lineage = self.aspects.get((urn, UpstreamLineageClass.ASPECT_NAME))
                if lineage is not None:
                    found[urn] = {UpstreamLineageClass.ASPECT_NAME: (lineage, None)}
        return found

    def write(self, workunits: List[MetadataWorkUnit]) -> None:
        for wu in workunits:
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                assert wu.metadata.entityUrn and wu.metadata.aspect
                self.aspects[
                    (wu.metadata.entityUrn, wu.metadata.aspect.ASPECT_NAME)
                ] = wu.metadata.aspect


def _run(
    graph: MagicMock,
    qlik: _Qlik,
    run_id: str,
    **overrides: object,
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
            QlikSourceConfig.model_validate(
                {
                    **CONFIG,
                    "on_prem": {
                        **ON_PREM,
                        "skip_unchanged_apps": overrides.pop(
                            "skip_unchanged_apps", True
                        ),
                    },
                    **overrides,
                }
            ),
            ctx,
        )
        workunits = list(source.get_workunits())
        source.close()
        for _, committable in ctx.get_committables():
            committable.commit()
    return source, workunits


_previous_checkpoint = QlikAppStateHandler.previous


def _checkpoint_without_urns(
    handler: QlikAppStateHandler, app_id: str
) -> Optional[AppCheckpoint]:
    """The stored checkpoint with its entity URNs removed."""
    checkpoint = _previous_checkpoint(handler, app_id)
    return checkpoint.model_copy(update={"urns": []}) if checkpoint else None


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
    third, third_wus = _run(mock_datahub_graph, qlik, "run-3")

    assert qlik.engine_opens == 1
    app_urns = set(_stale_urns(first))
    assert any(":chart:" in urn or ":dashboard:" in urn for urn in app_urns)
    for later, later_wus in ((second, second_wus), (third, third_wus)):
        assert later.reporter.apps_skipped_unchanged == 1
        assert not [wu for wu in later_wus if wu.id.startswith("soft-delete-")]
        assert app_urns <= set(_stale_urns(later))


def test_app_opened_without_its_data_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)
    read_app = QlikOnPremAPI._read_app

    def without_data(api: QlikOnPremAPI, *args: object, **kwargs: object) -> object:
        app = read_app(api, *args, **kwargs)  # type: ignore[arg-type]
        if isinstance(app, App):
            app.data_load_failed = True
        return app

    with patch.object(QlikOnPremAPI, "_read_app", without_data):
        first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    second, _ = _run(mock_datahub_graph, qlik, "run-2")

    # A timeout or section access may have kept the data model out, which a
    # later run can read.
    assert qlik.engine_opens == 2
    assert second.reporter.apps_skipped_unchanged == 0


def test_app_with_removed_child_is_still_skipped(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    datahub.soft_deleted.add("urn:li:dashboard:(qlik-sense,qlik-sense.removed-sheet)")
    second, _ = _run(mock_datahub_graph, qlik, "run-2")

    assert qlik.engine_opens == 1
    assert second.reporter.apps_skipped_unchanged == 1


def test_app_is_read_when_checkpoint_cannot_be_loaded(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    with patch(
        "datahub.ingestion.source.qlik_sense.app_state.QlikAppStateHandler.previous",
        side_effect=RuntimeError("checkpoint unavailable"),
    ):
        second, second_wus = _run(mock_datahub_graph, qlik, "run-2")

    assert qlik.engine_opens == 2
    assert second.reporter.apps_read == 1
    assert not [wu for wu in second_wus if wu.id.startswith("soft-delete-")]


def test_failed_read_with_unloadable_checkpoint_is_a_failure(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    qlik.engine_fails = True
    with patch(
        "datahub.ingestion.source.qlik_sense.app_state.QlikAppStateHandler.previous",
        side_effect=RuntimeError("checkpoint unavailable"),
    ):
        second, second_wus = _run(mock_datahub_graph, qlik, "run-2")

    assert second.reporter.apps_read == 1
    assert not _soft_deleted_urns(second_wus)
    assert "Unable to fully read Qlik app" in [
        failure.title for failure in second.reporter.failures
    ]
    assert list(second.reporter.apps_change_check_failed)


def _container_urn(source: QlikSenseSource) -> str:
    return source._gen_app_key("app-1").as_urn()


@pytest.mark.parametrize(
    "change",
    [
        "app_saved",
        "sheet_edited",
        "metadata_not_in_datahub",
        "sink_failed_after_container",
        "first_read_failed",
        "recipe_changed",
        "container_soft_deleted",
        "sheet_soft_deleted",
        "sheet_hard_deleted",
        "datahub_fingerprint_differs",
        "ignore_old_state",
        "data_connection_changed",
        "app_reloaded",
        "app_moved_stream",
        "sheet_owner_changed",
        "datahub_unreachable",
    ],
)
def test_app_is_read_again(mock_datahub_graph: MagicMock, change: str) -> None:  # noqa: F811
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)
    qlik.engine_fails = change == "first_read_failed"

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    if change == "sink_failed_after_container":
        first_sheet = next(
            index for index, wu in enumerate(first_wus) if ":dashboard:" in wu.get_urn()
        )
        datahub.write(first_wus[:first_sheet])
    elif change != "metadata_not_in_datahub":
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
    if change == "data_connection_changed":
        qlik.data_connection["connectionstring"] = (
            "OLEDB CONNECT TO [Provider=MSOLEDBSQL;Data Source=db.example.com;"
            "Initial Catalog=archive_db]"
        )
    if change == "app_reloaded":
        qlik.app["lastReloadTime"] = "2024-02-01T09:00:00.000Z"
    if change == "app_moved_stream":
        qlik.app["stream"] = {"id": "stream-2", "name": "Finance"}
    if change == "sheet_owner_changed":
        qlik.objects[0]["owner"] = {"id": "user-2", "userId": "grace"}
    if change == "datahub_unreachable":
        mock_datahub_graph.get_aspect.side_effect = RuntimeError("timed out")
    if change == "container_soft_deleted":
        datahub.aspects[(container_urn, StatusClass.ASPECT_NAME)] = StatusClass(
            removed=True
        )
    if change == "sheet_soft_deleted":
        datahub.soft_deleted.add(
            next(urn for urn in _stale_urns(first) if ":dashboard:" in urn)
        )
    if change == "sheet_hard_deleted":
        sheet_urn = next(urn for urn in _stale_urns(first) if ":dashboard:" in urn)
        datahub.aspects = {
            key: aspect
            for key, aspect in datahub.aspects.items()
            if key[0] != sheet_urn
        }
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
    if change == "datahub_unreachable":
        assert [warning.title for warning in second.reporter.warnings] == [
            "Unable to check whether Qlik apps changed"
        ]


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
    assert not [wu for wu in second_wus if wu.get_urn() == _container_urn(first)]
    assert set(_stale_urns(first)) <= set(_stale_urns(second))
    assert "Unable to fully read Qlik app" in [
        warning.title for warning in second.reporter.warnings
    ]
    assert third.reporter.apps_read == 1
    assert qlik.engine_opens == 3


def _soft_deleted_urns(workunits: List[MetadataWorkUnit]) -> Set[str]:
    return {wu.get_urn() for wu in workunits if wu.id.startswith("soft-delete-")}


def test_app_is_skipped_when_only_max_workers_changes(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    second, _ = _run(mock_datahub_graph, qlik, "run-2", max_workers=1)

    assert qlik.engine_opens == 1
    assert second.reporter.apps_skipped_unchanged == 1


@pytest.mark.parametrize("skip_unchanged_apps", [True, False])
def test_deleted_app_is_soft_deleted(
    mock_datahub_graph: MagicMock,  # noqa: F811
    skip_unchanged_apps: bool,
) -> None:
    qlik = _Qlik()
    qlik.other_apps = [SECOND_APP]
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(
        mock_datahub_graph, qlik, "run-1", skip_unchanged_apps=skip_unchanged_apps
    )
    datahub.write(first_wus)
    qlik.other_apps = []
    second, second_wus = _run(
        mock_datahub_graph, qlik, "run-2", skip_unchanged_apps=skip_unchanged_apps
    )

    deleted_app_urns = {
        first._gen_app_key("app-2").as_urn(),
        "urn:li:dashboard:(qlik-sense,sheet-of-app-2)",
    }
    assert deleted_app_urns <= {wu.get_urn() for wu in first_wus}
    assert deleted_app_urns <= _soft_deleted_urns(second_wus)
    assert first._gen_app_key("app-1").as_urn() not in _soft_deleted_urns(second_wus)
    assert not second.reporter.failures


def test_partly_read_app_keeps_last_read_without_skip_unchanged_apps(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    qlik.other_apps = [SECOND_APP]
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(
        mock_datahub_graph, qlik, "run-1", skip_unchanged_apps=False
    )
    datahub.write(first_wus)
    qlik.engine_fails_for = {"app-2"}
    second, second_wus = _run(
        mock_datahub_graph, qlik, "run-2", skip_unchanged_apps=False
    )

    sheet = "urn:li:dashboard:(qlik-sense,sheet-of-app-2)"
    assert sheet in {wu.get_urn() for wu in first_wus}
    assert sheet in _stale_urns(second)
    assert not _soft_deleted_urns(second_wus)
    assert not second.reporter.failures
    assert "Unable to fully read Qlik app" in [
        warning.title for warning in second.reporter.warnings
    ]


def test_app_is_read_when_fingerprint_cannot_be_computed(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    qlik.objects_fail = True
    second, second_wus = _run(mock_datahub_graph, qlik, "run-2")

    assert qlik.engine_opens == 2
    assert second.reporter.apps_skipped_unchanged == 0
    assert not _soft_deleted_urns(second_wus)
    assert [
        failure.split(":")[0] for failure in second.reporter.apps_change_check_failed
    ] == ["app-1"]
    container = first._gen_app_key("app-1").as_urn()
    assert not [
        wu
        for wu in second_wus
        if wu.get_urn() == container and wu.id.endswith("-fingerprint")
    ]


def test_app_checkpoint_is_trusted_again_after_a_full_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    _, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    # Commits the stale entity removal state but no app checkpoints.
    _, second_wus = _run(mock_datahub_graph, qlik, "run-2", skip_unchanged_apps=False)
    datahub.write(second_wus)
    third, third_wus = _run(mock_datahub_graph, qlik, "run-3")
    qlik.engine_fails = True
    fourth, fourth_wus = _run(mock_datahub_graph, qlik, "run-4")

    assert third.reporter.apps_skipped_unchanged == 0
    assert third.reporter.apps_read == 1
    assert "Unable to fully read Qlik app" not in [
        failure.title for failure in fourth.reporter.failures
    ]
    assert not _soft_deleted_urns(fourth_wus)


def test_partly_read_app_after_unsaved_app_checkpoint_is_a_failure(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    _, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    # Run 2 saves the stale removal state but not the app checkpoint, as when
    # the checkpoint is over the size limit.
    with patch.object(QlikAppStateHandler, "create_checkpoint", return_value=None):
        _, second_wus = _run(mock_datahub_graph, qlik, "run-2")
    datahub.write(second_wus)
    qlik.engine_fails = True
    third, third_wus = _run(mock_datahub_graph, qlik, "run-3")

    assert "Unable to fully read Qlik app" in [
        failure.title for failure in third.reporter.failures
    ]
    assert not _soft_deleted_urns(third_wus)


def test_checkpoint_without_entities_is_not_trusted(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    _, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    with patch.object(QlikAppStateHandler, "previous", _checkpoint_without_urns):
        second, _ = _run(mock_datahub_graph, qlik, "run-2")

    assert second.reporter.apps_skipped_unchanged == 0
    assert qlik.engine_opens == 2


def test_unchanged_app_whose_checkpoint_fails_to_reload_keeps_its_last_read(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    _, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    previous = QlikAppStateHandler.previous
    calls: List[str] = []

    def fails_after_skip_check(
        handler: QlikAppStateHandler, app_id: str
    ) -> Optional[AppCheckpoint]:
        calls.append(app_id)
        if len(calls) > 1:
            raise RuntimeError("checkpoint unavailable")
        return previous(handler, app_id)

    with patch.object(QlikAppStateHandler, "previous", fails_after_skip_check):
        second, _ = _run(mock_datahub_graph, qlik, "run-2")
    third, third_wus = _run(mock_datahub_graph, qlik, "run-3")

    assert second.reporter.apps_skipped_unchanged == 1
    assert "Unable to load the last checkpoint of a Qlik app" in [
        failure.title for failure in second.reporter.failures
    ]
    # The failed run carries the app's last read forward, as it does the stale
    # entity removal state, so the app is still unchanged.
    assert third.reporter.apps_skipped_unchanged == 1
    assert not _soft_deleted_urns(third_wus)
    assert qlik.engine_opens == 1


def test_partial_read_without_last_read_is_not_recorded(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    _DataHub(mock_datahub_graph)
    qlik.engine_fails = True

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    second, second_wus = _run(mock_datahub_graph, qlik, "run-2")

    for run in (first, second):
        assert "Unable to fully read Qlik app" in [
            failure.title for failure in run.reporter.failures
        ]
    assert not _soft_deleted_urns(second_wus)


def test_skip_unchanged_apps_without_stateful_ingestion_is_ignored(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    _DataHub(mock_datahub_graph)

    first, _ = _run(
        mock_datahub_graph, qlik, "run-1", stateful_ingestion={"enabled": False}
    )
    second, _ = _run(
        mock_datahub_graph, qlik, "run-2", stateful_ingestion={"enabled": False}
    )

    assert "skip_unchanged_apps is ignored" in [
        warning.title for warning in first.reporter.warnings
    ]
    assert second.reporter.apps_skipped_unchanged == 0
    assert qlik.engine_opens == 2


def test_run_with_ignore_new_state_records_no_checkpoint(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    _, first_wus = _run(
        mock_datahub_graph,
        qlik,
        "run-1",
        stateful_ingestion={
            **CONFIG["stateful_ingestion"],  # type: ignore[dict-item]
            "ignore_new_state": True,
        },
    )
    datahub.write(first_wus)
    second, _ = _run(mock_datahub_graph, qlik, "run-2")

    assert second.reporter.apps_skipped_unchanged == 0
    assert qlik.engine_opens == 2


def test_partial_read_with_checkpoint_without_entities_is_a_failure(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)

    _, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    qlik.engine_fails = True
    with patch.object(QlikAppStateHandler, "previous", _checkpoint_without_urns):
        second, second_wus = _run(mock_datahub_graph, qlik, "run-2")

    assert "Unable to fully read Qlik app" in [
        failure.title for failure in second.reporter.failures
    ]
    assert not _soft_deleted_urns(second_wus)


def test_sheet_removed_in_qlik_is_soft_deleted_when_change_check_fails(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)
    qlik.objects_fail = True

    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    second, second_wus = _run(mock_datahub_graph, qlik, "run-2")
    qlik.apps_without_sheets = {"app-1"}
    third, third_wus = _run(mock_datahub_graph, qlik, "run-3")

    sheet = "urn:li:dashboard:(qlik-sense,sheet-1)"
    assert sheet in _stale_urns(second)
    assert sheet in _soft_deleted_urns(third_wus)
    assert not third.reporter.failures
    assert "Unable to check whether Qlik apps changed" in [
        warning.title for warning in third.reporter.warnings
    ]


def test_oversized_app_checkpoint_is_reported(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    _DataHub(mock_datahub_graph)

    # The framework logs the skipped commit with a format string that fails
    # under pytest's log capture.
    with patch("datahub.ingestion.source.state.stateful_ingestion_base.logger"):
        first, _ = _run(
            mock_datahub_graph,
            qlik,
            "run-1",
            stateful_ingestion={
                **CONFIG["stateful_ingestion"],  # type: ignore[dict-item]
                "max_checkpoint_state_size": 16,
            },
        )

    assert "Qlik app checkpoint is too large to save" in [
        warning.title for warning in first.reporter.warnings
    ]


def test_failed_run_keeps_last_read_of_apps_it_did_not_reach(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    qlik.other_apps = [{**SECOND_APP, "stream": {"id": "stream-2", "name": "Finance"}}]
    datahub = _DataHub(mock_datahub_graph)

    _, first_wus = _run(mock_datahub_graph, qlik, "run-1", skip_unchanged_apps=False)
    datahub.write(first_wus)
    qlik.failing_streams = {"stream-2"}
    second, _ = _run(mock_datahub_graph, qlik, "run-2", skip_unchanged_apps=False)
    qlik.failing_streams = set()
    qlik.engine_fails_for = {"app-2"}
    third, third_wus = _run(
        mock_datahub_graph, qlik, "run-3", skip_unchanged_apps=False
    )

    assert "Unable to read from Qlik" in [
        failure.title for failure in second.reporter.failures
    ]
    assert not third.reporter.failures
    assert "Unable to fully read Qlik app" in [
        warning.title for warning in third.reporter.warnings
    ]
    assert "urn:li:dashboard:(qlik-sense,sheet-of-app-2)" in _stale_urns(third)
    assert not _soft_deleted_urns(third_wus)


def test_partly_read_app_is_kept_for_a_limited_number_of_runs(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _Qlik()
    datahub = _DataHub(mock_datahub_graph)
    container: Optional[str] = None

    def run(run_id: str, engine_fails: bool) -> Tuple[str, List[MetadataWorkUnit]]:
        nonlocal container
        qlik.engine_fails = engine_fails
        source, workunits = _run(
            mock_datahub_graph,
            qlik,
            run_id,
            skip_unchanged_apps=False,
            max_partial_reads_kept=2,
        )
        datahub.write(workunits)
        container = _container_urn(source)
        titles = [entry.title for entry in source.reporter.failures] + [
            entry.title for entry in source.reporter.warnings
        ]
        outcome = (
            "limit"
            if "Qlik app has been only partly read for too many runs" in titles
            else "kept"
            if "Unable to fully read Qlik app" in titles
            else "complete"
        )
        return outcome, workunits

    outcomes = [
        run("run-1", engine_fails=False),
        run("run-2", engine_fails=True),
        run("run-3", engine_fails=True),
        run("run-4", engine_fails=True),
        run("run-5", engine_fails=False),
        run("run-6", engine_fails=True),
    ]

    assert [outcome for outcome, _ in outcomes] == [
        "complete",
        "kept",
        "kept",
        "limit",
        "complete",
        "kept",
    ]
    # Kept runs emit nothing for the app; the run at the limit ingests the
    # partial read.
    assert not [wu for wu in outcomes[1][1] if wu.get_urn() == container]
    assert [wu for wu in outcomes[3][1] if wu.get_urn() == container]
    assert not _soft_deleted_urns(outcomes[3][1])


QVD_FOLDER: Dict[str, object] = {
    "name": "QVD Data",
    "connectionstring": "\\\\nas\\qvd",
    "type": "folder",
}
STORING_SCRIPT = """
Orders: LOAD id, amount FROM [lib://QVD Data/raw/orders.qvd] (qvd);
STORE Orders INTO [lib://QVD Data/clean/orders.qvd] (qvd);
"""
READING_SCRIPT = "Sales: LOAD id FROM [lib://QVD Data/clean/orders.qvd] (qvd);"
CLEAN_QVD = (
    "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,//nas/qvd/clean/orders.qvd,PROD)"
)


def _qvd_qlik() -> _Qlik:
    qlik = _Qlik()
    qlik.other_apps = [SECOND_APP]
    qlik.extra_connections = [QVD_FOLDER]
    qlik.scripts = {"app-1": STORING_SCRIPT, "app-2": READING_SCRIPT}
    return qlik


def _qvd_lineage_ops(workunits: List[MetadataWorkUnit]) -> List[Tuple[str, str]]:
    """The (op, upstream dataset) of each table edge patched onto the QVD."""
    ops = []
    for wu in workunits:
        if (
            wu.get_urn() == CLEAN_QVD
            and isinstance(wu.metadata, MetadataChangeProposalClass)
            and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
            and wu.metadata.aspect is not None
        ):
            for op in json.loads(wu.metadata.aspect.value):
                parts = op["path"].split("/")
                if parts[1] == "upstreams":
                    ops.append((op["op"], parts[2].replace("~1", "/")))
    return ops


def _qvd_upstreams(workunits: List[MetadataWorkUnit]) -> List[str]:
    return [urn for op, urn in _qvd_lineage_ops(workunits) if op == "add"]


def test_kept_unchanged_app_still_writes_its_qvd(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _qvd_qlik()
    datahub = _DataHub(mock_datahub_graph)
    first, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    writer = "urn:li:dataset:(urn:li:dataPlatform:qlik-sense,app-1.orders,PROD)"
    assert _qvd_upstreams(first_wus) == [writer]

    # Only the reading app changed, so the writing app is kept from its
    # checkpoint, whose QVD writes keep its edge.
    edited_app = {**SECOND_APP, "modifiedDate": "2024-02-01T09:00:00.000Z"}
    qlik.other_apps = [edited_app]
    second, second_wus = _run(mock_datahub_graph, qlik, "run-2")

    assert second.reporter.apps_skipped_unchanged == 1
    assert _qvd_upstreams(second_wus) == [writer]


def test_app_checkpoint_from_before_qvd_lineage_is_read_again(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _qvd_qlik()
    datahub = _DataHub(mock_datahub_graph)
    _, first_wus = _run(mock_datahub_graph, qlik, "run-1")
    datahub.write(first_wus)
    opens = qlik.engine_opens

    def without_qvds(
        handler: QlikAppStateHandler, app_id: str
    ) -> Optional[AppCheckpoint]:
        checkpoint = _previous_checkpoint(handler, app_id)
        return checkpoint.model_copy(update={"qvds": None}) if checkpoint else None

    with patch.object(QlikAppStateHandler, "previous", without_qvds):
        second, second_wus = _run(mock_datahub_graph, qlik, "run-2")

    # Its QVD writes are unknown, so it is read rather than kept, and the QVD's
    # lineage is written from what it writes now instead of being blocked.
    assert qlik.engine_opens == opens + 2
    assert second.reporter.apps_skipped_unchanged == 0
    assert _qvd_upstreams(second_wus)


def test_partly_read_app_kept_from_its_checkpoint_only_adds_qvd_lineage(
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    qlik = _qvd_qlik()
    datahub = _DataHub(mock_datahub_graph)
    _, first_wus = _run(mock_datahub_graph, qlik, "run-1", skip_unchanged_apps=False)
    datahub.write(first_wus)

    qlik.engine_fails_for = {"app-1"}
    second, second_wus = _run(
        mock_datahub_graph, qlik, "run-2", skip_unchanged_apps=False
    )

    # Its last full read may no longer be what it writes, so the QVD's edges
    # are added to rather than replaced.
    assert not [op for op, _ in _qvd_lineage_ops(second_wus) if op == "remove"]
    assert any(
        wu.get_urn() == CLEAN_QVD
        and isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.changeType == "PATCH"
        for wu in second_wus
    )
    assert "Lineage of Qlik QVDs only added to" in [
        warning.title for warning in second.reporter.warnings
    ]


def test_onprem_qvd_lineage_golden(
    pytestconfig: pytest.Config,
    tmp_path: pathlib.Path,
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    # One app loads a raw QVD and stores a clean one, which another app loads.
    _DataHub(mock_datahub_graph)
    _, workunits = _run(
        mock_datahub_graph, _qvd_qlik(), "run-1", skip_unchanged_apps=False
    )
    output_path = tmp_path / "qlik_onprem_qvd_lineage.json"
    write_metadata_file(output_path, [wu.metadata for wu in workunits])

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=pathlib.Path(__file__).parent / "golden_onprem_qvd_lineage.json",
    )
