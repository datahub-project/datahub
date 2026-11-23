from typing import Dict, Iterator, List, Optional, Set, Tuple, Union
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.grafana.constants import (
    DASHBOARD_FOLDER_NOT_INGESTED_TITLE,
    DASHBOARD_SEARCH_ERROR_TITLE,
    DASHBOARD_WITHOUT_FOLDER_ID_TITLE,
    DASHBOARDS_SKIPPED_TITLE,
    FOLDERS_UNPARSEABLE_TITLE,
    PANEL_SKIPPED_TITLE,
)
from datahub.ingestion.source.grafana.grafana_config import GrafanaSourceConfig
from datahub.ingestion.source.grafana.grafana_source import GrafanaSource
from datahub.ingestion.source.grafana.models import (
    Dashboard,
    DashboardContainerKey,
    DashboardListing,
    Folder,
    FolderKey,
    FolderListing,
    Panel,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    SubTypesClass,
)
from tests.unit.conftest import (
    aspects_by_urn,
    aspects_of_type,
    log_contexts,
    log_titles,
)

GRAFANA_API_CLIENT = "datahub.ingestion.source.grafana.grafana_source.GrafanaAPIClient"
PLATFORM_INSTANCE = "test-instance"
FOLDER_TITLE = "Test Folder"
DASHBOARD_UID = "dash1"

FOLDER_COUNTERS = (
    "dashboards_in_filtered_folders",
    "dashboards_without_folder_container",
    "dashboards_without_folder_id",
)

GRAFANA_REQUESTS_GET = "datahub.ingestion.source.grafana.grafana_source.requests.get"


@pytest.fixture
def mock_config() -> GrafanaSourceConfig:
    return GrafanaSourceConfig(
        url="http://grafana.test",
        service_account_token="test-token",
        platform_instance=PLATFORM_INSTANCE,
    )


@pytest.fixture
def mock_context() -> PipelineContext:
    return PipelineContext(run_id="test")


@pytest.fixture
def mock_api() -> Iterator[MagicMock]:
    with patch(GRAFANA_API_CLIENT) as mock_api_class:
        yield mock_api_class.return_value


def _make_dashboard(folder_id: Optional[Union[int, str]]) -> Dashboard:
    meta: Dict[str, Union[int, str]] = (
        {} if folder_id is None else {"folderId": folder_id}
    )
    return Dashboard(
        uid=DASHBOARD_UID,
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


def _run(
    config: GrafanaSourceConfig,
    ctx: PipelineContext,
    mock_api: MagicMock,
    *,
    folders: List[Folder],
    dashboards: List[Dashboard],
) -> Tuple[GrafanaSource, List[MetadataWorkUnit]]:
    mock_api.get_folders.return_value = FolderListing(folders=folders)
    mock_api.get_dashboards.return_value = DashboardListing(dashboards=dashboards)
    source = GrafanaSource(config, ctx)
    return source, list(source.get_workunits_internal())


# Expected URNs are built here, not with the source's own key helpers, so a
# regression in key construction cannot change the expected value with it.
def _folder_urn(folder_id: str) -> str:
    return FolderKey(
        platform="grafana", instance=PLATFORM_INSTANCE, folder_id=folder_id
    ).as_urn()


def _dashboard_container_urn(folder_id: Optional[str]) -> str:
    return DashboardContainerKey(
        platform="grafana",
        instance=PLATFORM_INSTANCE,
        dashboard_id=DASHBOARD_UID,
        folder_id=folder_id,
    ).as_urn()


def _parents(workunits: List[MetadataWorkUnit], urn: str) -> List[str]:
    aspects = aspects_by_urn(workunits).get(urn, [])
    return [c.container for c in aspects_of_type(aspects, ContainerClass)]


def _emitted_container_urns(workunits: List[MetadataWorkUnit]) -> Set[str]:
    return {
        urn
        for urn, aspects in aspects_by_urn(workunits).items()
        if aspects_of_type(aspects, ContainerPropertiesClass)
    }


def _chart_urns(workunits: List[MetadataWorkUnit]) -> Set[str]:
    return {
        wu.get_urn() for wu in workunits if wu.get_urn().startswith("urn:li:chart:")
    }


def _folder_counters(source: GrafanaSource) -> Dict[str, int]:
    return {name: getattr(source.report, name) for name in FOLDER_COUNTERS}


@pytest.mark.usefixtures("mock_api")
def test_process_folder_emits_folder_container(mock_config, mock_context):
    source = GrafanaSource(mock_config, mock_context)
    folder = Folder(id="7", title="Team Folder", description="Team dashboards")

    aspects = aspects_by_urn(source._process_folder(folder))[_folder_urn("7")]

    properties = aspects_of_type(aspects, ContainerPropertiesClass)
    assert [(p.name, p.description) for p in properties] == [
        ("Team Folder", "Team dashboards")
    ]
    sub_types = aspects_of_type(aspects, SubTypesClass)
    assert [s.typeNames for s in sub_types] == [[BIContainerSubTypes.LOOKER_FOLDER]]


@pytest.mark.usefixtures("mock_api")
def test_process_dashboard_emits_charts(mock_config, mock_context):
    source = GrafanaSource(mock_config, mock_context)

    workunits = list(source._process_dashboard(_make_dashboard(folder_id=None)))

    assert _chart_urns(workunits) == {"urn:li:chart:(grafana,test-instance.dash1.1)"}
    assert source.report.charts_scanned == 1


@pytest.mark.parametrize(
    "folder, dashboard_folder_id",
    [
        pytest.param(Folder(id="1", title=FOLDER_TITLE), "1", id="string-ids"),
        # Grafana sends ids as integers on both /api/folders and dashboard meta.
        pytest.param(
            Folder.model_validate({"id": 7, "title": FOLDER_TITLE}), 7, id="int-ids"
        ),
    ],
)
def test_dashboard_placed_in_ingested_folder(
    mock_api, mock_config, mock_context, folder, dashboard_folder_id
):
    # Runs the real folder-then-dashboard order of get_workunits_internal.
    source, workunits = _run(
        mock_config,
        mock_context,
        mock_api,
        folders=[folder],
        dashboards=[_make_dashboard(folder_id=dashboard_folder_id)],
    )

    folder_id = str(dashboard_folder_id)
    parents = _parents(workunits, _dashboard_container_urn(folder_id))
    assert parents == [_folder_urn(folder_id)]
    # The parent must be a container this run emitted, not just a matching key.
    assert set(parents) <= _emitted_container_urns(workunits)
    assert source.report.folders_scanned == 1
    assert source.report.dashboards_scanned == 1
    assert _folder_counters(source) == dict.fromkeys(FOLDER_COUNTERS, 0)
    assert not source.report.warnings


def test_general_folder_dashboard_has_no_parent(mock_api, mock_config, mock_context):
    # General is Grafana's root folder, sent as the integer 0.
    source, workunits = _run(
        mock_config,
        mock_context,
        mock_api,
        folders=[],
        dashboards=[_make_dashboard(folder_id=0)],
    )

    dashboard_container_urn = _dashboard_container_urn("0")
    assert dashboard_container_urn in _emitted_container_urns(workunits)
    assert not _parents(workunits, dashboard_container_urn)
    assert _folder_counters(source) == dict.fromkeys(FOLDER_COUNTERS, 0)
    assert not source.report.warnings


@pytest.mark.parametrize(
    "dashboard_folder_id, folders, folder_pattern, expected_counter, expected_warnings",
    [
        pytest.param(
            "1",
            [Folder(id="1", title=FOLDER_TITLE)],
            AllowDenyPattern(deny=[FOLDER_TITLE]),
            "dashboards_in_filtered_folders",
            set(),
            id="excluded-folder",
        ),
        # e.g. a nested folder /api/folders does not list, or a failed page
        pytest.param(
            "1",
            [],
            AllowDenyPattern.allow_all(),
            "dashboards_without_folder_container",
            {DASHBOARD_FOLDER_NOT_INGESTED_TITLE},
            id="unlisted-folder",
        ),
        pytest.param(
            None,
            [],
            AllowDenyPattern.allow_all(),
            "dashboards_without_folder_id",
            {DASHBOARD_WITHOUT_FOLDER_ID_TITLE},
            id="no-folder-id",
        ),
    ],
)
def test_dashboard_not_placed_in_folder(
    mock_api,
    mock_config,
    mock_context,
    dashboard_folder_id,
    folders,
    folder_pattern,
    expected_counter,
    expected_warnings,
):
    mock_config.folder_pattern = folder_pattern

    source, workunits = _run(
        mock_config,
        mock_context,
        mock_api,
        folders=folders,
        dashboards=[_make_dashboard(folder_id=dashboard_folder_id)],
    )

    dashboard_container_urn = _dashboard_container_urn(dashboard_folder_id)
    assert dashboard_container_urn in _emitted_container_urns(workunits)
    assert not _parents(workunits, dashboard_container_urn)
    # No folder in this run was ingested, so none may be emitted as a container.
    emitted = _emitted_container_urns(workunits)
    assert not {_folder_urn(folder.id) for folder in folders} & emitted
    assert _folder_counters(source) == {
        **dict.fromkeys(FOLDER_COUNTERS, 0),
        expected_counter: 1,
    }
    assert log_titles(source.report.warnings) == expected_warnings
    assert all(DASHBOARD_UID in c for c in log_contexts(source.report.warnings))


@pytest.mark.parametrize(
    "stateful_ingestion, expect_failure",
    [
        pytest.param(None, False, id="stateful-not-configured"),
        # remove_stale_metadata defaults to true even when stateful is disabled.
        pytest.param(
            StatefulStaleMetadataRemovalConfig(enabled=False),
            False,
            id="stateful-disabled",
        ),
        pytest.param(
            StatefulStaleMetadataRemovalConfig(
                enabled=True, remove_stale_metadata=False
            ),
            False,
            id="stale-removal-off",
        ),
        pytest.param(
            StatefulStaleMetadataRemovalConfig(enabled=True),
            True,
            id="stale-removal-on",
        ),
    ],
)
def test_unparseable_folders_fail_run_only_when_removing_stale_metadata(
    mock_api, mock_config, mock_context, stateful_ingestion, expect_failure
):
    # A failure makes stale-entity removal skip this run, so the skipped
    # folders' containers are not soft-deleted. Without stale removal there is
    # nothing to protect, and the per-folder warnings are enough.
    source = GrafanaSource(mock_config, mock_context)
    source.config.stateful_ingestion = stateful_ingestion
    mock_api.get_folders.return_value = FolderListing(folders=[], unparseable=2)
    mock_api.get_dashboards.return_value = DashboardListing()

    list(source.get_workunits_internal())

    failure_titles = log_titles(source.report.failures)
    assert (FOLDERS_UNPARSEABLE_TITLE in failure_titles) == expect_failure


@pytest.mark.usefixtures("mock_api")
def test_chart_urns_omit_platform_instance_when_unset(mock_context):
    config = GrafanaSourceConfig(
        url="http://grafana.test",
        service_account_token="test-token",
    )
    source = GrafanaSource(config, mock_context)

    workunits = list(source._process_dashboard(_make_dashboard(folder_id=None)))

    assert _chart_urns(workunits) == {"urn:li:chart:(grafana,dash1.1)"}


def _basic_mode_source(mock_context: PipelineContext) -> GrafanaSource:
    config = GrafanaSourceConfig(
        url="http://grafana.test",
        service_account_token="test-token",
        basic_mode=True,
    )
    return GrafanaSource(config, mock_context)


def _search_response(body: object) -> MagicMock:
    response = MagicMock()
    response.json.return_value = body
    return response


def test_basic_mode_skips_malformed_search_results(mock_context):
    source = _basic_mode_source(mock_context)
    items = [
        {"uid": "ok", "title": "OK", "url": "/d/ok"},
        {"uid": "no-url", "title": "No URL"},
        "not an object",
    ]
    with patch(GRAFANA_REQUESTS_GET, return_value=_search_response(items)):
        workunits = list(source.get_workunits_internal())

    assert {wu.get_urn() for wu in workunits} == {"urn:li:dashboard:(grafana,ok)"}
    assert source.report.dashboard_search_hits_skipped == 2
    assert not source.report.failures


@pytest.mark.parametrize(
    "json_result",
    [
        pytest.param({"message": "proxy error"}, id="not-a-list"),
        pytest.param(
            requests.exceptions.JSONDecodeError("Expecting value", "<html>", 0),
            id="not-json",
        ),
    ],
)
def test_basic_mode_reports_non_list_response(mock_context, json_result):
    source = _basic_mode_source(mock_context)
    response = MagicMock(**{"json.side_effect": [json_result]})
    with patch(GRAFANA_REQUESTS_GET, return_value=response):
        workunits = list(source.get_workunits_internal())

    assert not workunits
    assert log_titles(source.report.failures) == {DASHBOARD_SEARCH_ERROR_TITLE}


@pytest.mark.parametrize(
    "remove_stale_metadata, expect_failure",
    [
        pytest.param(False, False, id="stale-removal-off"),
        pytest.param(True, True, id="stale-removal-on"),
    ],
)
def test_skipped_dashboards_fail_run_only_when_removing_stale_metadata(
    mock_api, mock_config, mock_context, remove_stale_metadata, expect_failure
):
    # Otherwise stale-entity removal would soft-delete a dashboard that was
    # skipped only because Grafana returned it in a shape that could not be read.
    source = GrafanaSource(mock_config, mock_context)
    source.config.stateful_ingestion = StatefulStaleMetadataRemovalConfig(
        enabled=True, remove_stale_metadata=remove_stale_metadata
    )
    mock_api.get_folders.return_value = FolderListing()
    mock_api.get_dashboards.return_value = DashboardListing(skipped=1)

    list(source.get_workunits_internal())

    failure_titles = log_titles(source.report.failures)
    assert (DASHBOARDS_SKIPPED_TITLE in failure_titles) == expect_failure


def test_basic_mode_skipped_result_fails_run_when_removing_stale_metadata(
    mock_context,
):
    source = _basic_mode_source(mock_context)
    source.config.stateful_ingestion = StatefulStaleMetadataRemovalConfig(enabled=True)
    items = [{"uid": "ok", "title": "OK", "url": "/d/ok"}, {"uid": "no-url"}]
    with patch(GRAFANA_REQUESTS_GET, return_value=_search_response(items)):
        list(source.get_workunits_internal())

    assert log_titles(source.report.failures) == {DASHBOARDS_SKIPPED_TITLE}


@pytest.mark.usefixtures("mock_api")
def test_process_dashboard_reports_skipped_panels(mock_config, mock_context):
    source = GrafanaSource(mock_config, mock_context)
    dashboard = Dashboard.model_validate(
        {
            "dashboard": {
                "uid": "dash1",
                "title": "Test Dashboard",
                "panels": [{"id": "2", "type": "graph", "title": ["bad"]}],
            }
        }
    )

    list(source._process_dashboard(dashboard))

    assert source.report.panel_parsing_warnings == 1
    [warning] = [w for w in source.report.warnings if w.title == PANEL_SKIPPED_TITLE]
    [context] = warning.context
    assert "dash1" in context and "id=2" in context
