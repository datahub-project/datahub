from typing import Dict, List, Optional, Set, Tuple, Type, TypeVar, Union
from unittest.mock import MagicMock, patch

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import StructuredLogEntry
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.grafana.constants import (
    DASHBOARD_FOLDER_NOT_INGESTED_TITLE,
)
from datahub.ingestion.source.grafana.grafana_config import GrafanaSourceConfig
from datahub.ingestion.source.grafana.grafana_source import GrafanaSource
from datahub.ingestion.source.grafana.models import Dashboard, Folder, Panel
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    SubTypesClass,
    _Aspect,
)

AspectT = TypeVar("AspectT", bound=_Aspect)

GRAFANA_API_CLIENT = "datahub.ingestion.source.grafana.grafana_source.GrafanaAPIClient"


@pytest.fixture
def mock_config():
    return GrafanaSourceConfig(
        url="http://grafana.test",
        service_account_token="test-token",
        platform_instance="test-instance",
    )


@pytest.fixture
def mock_context():
    return PipelineContext(run_id="test")


@pytest.fixture
def mock_api():
    with patch(GRAFANA_API_CLIENT) as mock_api_class:
        yield mock_api_class.return_value


@pytest.fixture
def mock_folder():
    return Folder(id="1", title="Test Folder", description="Test Description")


def _make_dashboard(folder_id: Optional[Union[int, str]]) -> Dashboard:
    meta: Dict[str, Union[int, str]] = (
        {} if folder_id is None else {"folderId": folder_id}
    )
    return Dashboard(
        uid="dash1",
        title="Test Dashboard",
        description="Test Description",
        version="1",
        panels=[
            Panel(
                id="1",
                title="Test Panel",
                type="graph",
                datasource={"type": "postgres", "uid": "postgres_uid"},
                targets=[{"rawSql": "SELECT * FROM test_table"}],
            )
        ],
        tags=["test"],
        schemaVersion="1.0",
        meta=meta,
    )


@pytest.fixture
def mock_dashboard(mock_folder):
    return _make_dashboard(mock_folder.id)


def _run(
    config: GrafanaSourceConfig,
    ctx: PipelineContext,
    mock_api: MagicMock,
    folders: List[Folder],
    dashboards: List[Dashboard],
) -> Tuple[GrafanaSource, List[MetadataWorkUnit]]:
    mock_api.get_folders.return_value = folders
    mock_api.get_dashboards.return_value = dashboards
    source = GrafanaSource(config, ctx)
    return source, list(source.get_workunits_internal())


def _dashboard_container_urn(source: GrafanaSource, dashboard: Dashboard) -> str:
    return source._dashboard_container_key(dashboard).as_urn()


def _folder_urn(source: GrafanaSource, folder: Folder) -> str:
    return source._folder_key(folder.id).as_urn()


def _aspects(
    workunits: List[MetadataWorkUnit], urn: str, aspect_type: Type[AspectT]
) -> List[AspectT]:
    aspects = [
        wu.get_aspect_of_type(aspect_type) for wu in workunits if wu.get_urn() == urn
    ]
    return [aspect for aspect in aspects if aspect is not None]


def _dashboard_parents(workunits: List[MetadataWorkUnit], urn: str) -> List[str]:
    return [c.container for c in _aspects(workunits, urn, ContainerClass)]


def _is_emitted(workunits: List[MetadataWorkUnit], urn: str) -> bool:
    return any(wu.get_urn() == urn for wu in workunits)


def _chart_urns(workunits: List[MetadataWorkUnit]) -> Set[str]:
    return {
        wu.get_urn() for wu in workunits if wu.get_urn().startswith("urn:li:chart:")
    }


def _emitted_container_urns(workunits: List[MetadataWorkUnit]) -> Set[str]:
    return {
        wu.get_urn()
        for wu in workunits
        if wu.get_aspect_of_type(ContainerPropertiesClass) is not None
    }


def _folder_not_ingested_warnings(source: GrafanaSource) -> List[StructuredLogEntry]:
    return [
        w
        for w in source.report.warnings
        if w.title == DASHBOARD_FOLDER_NOT_INGESTED_TITLE
    ]


def test_process_folder_emits_folder_container(mock_api, mock_config, mock_context):
    source = GrafanaSource(mock_config, mock_context)
    folder = Folder(id="7", title="Team Folder", description="Team dashboards")

    workunits = list(source._process_folder(folder))

    folder_urn = _folder_urn(source, folder)
    properties = _aspects(workunits, folder_urn, ContainerPropertiesClass)
    assert [(p.name, p.description) for p in properties] == [
        ("Team Folder", "Team dashboards")
    ]
    sub_types = _aspects(workunits, folder_urn, SubTypesClass)
    assert [s.typeNames for s in sub_types] == [[BIContainerSubTypes.LOOKER_FOLDER]]


def test_process_dashboard_emits_charts(mock_api, mock_config, mock_context):
    source = GrafanaSource(mock_config, mock_context)

    workunits = list(source._process_dashboard(_make_dashboard(folder_id=None)))

    assert _chart_urns(workunits) == {"urn:li:chart:(grafana,test-instance.dash1.1)"}
    assert source.report.charts_scanned == 1


def test_dashboard_placed_in_ingested_folder(
    mock_api, mock_config, mock_context, mock_folder, mock_dashboard
):
    # Runs the real folder-then-dashboard order of get_workunits_internal.
    source, workunits = _run(
        mock_config, mock_context, mock_api, [mock_folder], [mock_dashboard]
    )

    parents = _dashboard_parents(
        workunits, _dashboard_container_urn(source, mock_dashboard)
    )
    assert parents == [_folder_urn(source, mock_folder)]
    # The parent must be a container this run emitted, not just a matching key.
    assert set(parents) <= _emitted_container_urns(workunits)
    assert source.report.folders_scanned == 1
    assert source.report.dashboards_scanned == 1
    assert source.report.dashboards_without_folder_container == 0
    assert not _folder_not_ingested_warnings(source)


def test_general_folder_dashboard_has_no_parent(mock_api, mock_config, mock_context):
    # Grafana returns the General folder as the integer 0
    dashboard = _make_dashboard(folder_id=0)

    source, workunits = _run(mock_config, mock_context, mock_api, [], [dashboard])

    dashboard_container_urn = _dashboard_container_urn(source, dashboard)
    assert _is_emitted(workunits, dashboard_container_urn)
    assert not _dashboard_parents(workunits, dashboard_container_urn)
    assert source.report.dashboards_without_folder_container == 0
    assert source.report.dashboards_without_folder_id == 0
    assert source.report.dashboards_in_filtered_folders == 0
    assert not _folder_not_ingested_warnings(source)


def test_integer_folder_ids_match(mock_api, mock_config, mock_context):
    # Grafana sends folder ids as integers on both /api/folders and dashboard meta.
    folder = Folder.model_validate({"id": 7, "title": "Team Folder"})
    dashboard = _make_dashboard(folder_id=7)

    source, workunits = _run(mock_config, mock_context, mock_api, [folder], [dashboard])

    assert _dashboard_parents(
        workunits, _dashboard_container_urn(source, dashboard)
    ) == [_folder_urn(source, folder)]
    assert not _folder_not_ingested_warnings(source)


def test_dashboard_in_excluded_folder_has_no_parent_and_no_warning(
    mock_api, mock_config, mock_context, mock_folder, mock_dashboard
):
    mock_config.folder_pattern = AllowDenyPattern(deny=[mock_folder.title])

    source, workunits = _run(
        mock_config, mock_context, mock_api, [mock_folder], [mock_dashboard]
    )

    dashboard_container_urn = _dashboard_container_urn(source, mock_dashboard)
    assert source.report.folders_scanned == 0
    assert _is_emitted(workunits, dashboard_container_urn)
    assert not _dashboard_parents(workunits, dashboard_container_urn)
    assert source.report.dashboards_in_filtered_folders == 1
    assert source.report.dashboards_without_folder_container == 0
    assert not _folder_not_ingested_warnings(source)


def test_dashboard_in_unlisted_folder_warns(
    mock_api, mock_config, mock_context, mock_dashboard
):
    # e.g. a nested folder /api/folders does not list, or a failed folder page
    source, workunits = _run(mock_config, mock_context, mock_api, [], [mock_dashboard])

    dashboard_container_urn = _dashboard_container_urn(source, mock_dashboard)
    assert _is_emitted(workunits, dashboard_container_urn)
    assert not _dashboard_parents(workunits, dashboard_container_urn)
    assert source.report.dashboards_without_folder_container == 1
    warnings = _folder_not_ingested_warnings(source)
    assert len(warnings) == 1
    assert list(warnings[0].context) == ["Test Dashboard (uid dash1, folder id 1)"]


def test_dashboard_without_folder_id_is_counted(mock_api, mock_config, mock_context):
    dashboard = _make_dashboard(folder_id=None)

    source, workunits = _run(mock_config, mock_context, mock_api, [], [dashboard])

    dashboard_container_urn = _dashboard_container_urn(source, dashboard)
    assert _is_emitted(workunits, dashboard_container_urn)
    assert not _dashboard_parents(workunits, dashboard_container_urn)
    assert source.report.dashboards_without_folder_id == 1
    assert source.report.dashboards_without_folder_container == 0
    assert not _folder_not_ingested_warnings(source)


def test_chart_urns_omit_platform_instance_when_unset(mock_api, mock_context):
    config = GrafanaSourceConfig(
        url="http://grafana.test",
        service_account_token="test-token",
    )
    source = GrafanaSource(config, mock_context)

    workunits = list(source._process_dashboard(_make_dashboard(folder_id=None)))

    assert _chart_urns(workunits) == {"urn:li:chart:(grafana,dash1.1)"}
