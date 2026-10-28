"""Sheet (chart) and dashboard emission in TableauSiteSource: hidden-asset
filtering, external/embed URLs, inputs, usage stats, ownership, containers and
browse paths.
"""

from typing import Iterable, List, Type, TypeVar, cast
from unittest import mock

from tableauserverclient.models import UserItem

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    SiteIdContentUrl,
    TableauConfig,
    TableauProject,
    TableauSiteSource,
    TableauSourceReport,
    UsageStat,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChartInfoClass,
    ChartUsageStatisticsClass,
    ContainerClass,
    DashboardInfoClass,
    EmbedClass,
    GlobalTagsClass,
    InputFieldsClass,
    OwnershipClass,
)
from tests.unit.tableau.test_tableau_config import default_config

T = TypeVar("T")

CONNECT_URI = default_config["connect_uri"]
SITE_CONTENT_URL = "site-a"
PROJECT_ID = "project-1"
WORKBOOK = {
    c.ID: "wb-1",
    c.NAME: "Sales WB",
    c.LUID: "wb-luid-1",
    c.PROJECT_NAME: "default",
    c.OWNER: {c.USERNAME: "alice", c.EMAIL: "alice@example.com"},
}


def _project(path: List[str]) -> TableauProject:
    return TableauProject(
        id=PROJECT_ID,
        name=path[-1],
        description="",
        parent_id=None,
        parent_name=None,
        path=path,
    )


def _make_source(**overrides: object) -> TableauSiteSource:
    config = TableauConfig.model_validate({**default_config, **overrides})
    server = mock.MagicMock()
    server.user_id = "user-1"
    server.site_id = "site-1"
    server.users.get_by_id.return_value = UserItem(
        name="user", site_role=UserItem.Roles.SiteAdministratorExplorer
    )
    source = TableauSiteSource(
        config=config,
        ctx=PipelineContext(run_id="test"),
        platform="tableau",
        site=SiteIdContentUrl(site_id="site-1", site_content_url=SITE_CONTENT_URL),
        report=TableauSourceReport(),
        server=server,
    )
    source.tableau_project_registry = {PROJECT_ID: _project(["default"])}
    source.workbook_project_map = {"wb-luid-1": PROJECT_ID}
    return source


def _sheet(**overrides: object) -> dict:
    sheet: dict = {
        c.ID: "sheet-1",
        c.NAME: "My Sheet",
        c.LUID: "sheet-luid-1",
        c.PATH: "SalesWB/MySheet",
        c.CREATED_AT: "2021-01-01T00:00:00Z",
        c.UPDATED_AT: "2021-01-02T00:00:00Z",
        c.WORKBOOK: WORKBOOK,
        c.TAGS: [{c.NAME: "finance"}],
    }
    sheet.update(overrides)
    return sheet


def _dashboard(**overrides: object) -> dict:
    dashboard: dict = {
        c.ID: "dash-1",
        c.NAME: "Q1/Overview",
        c.LUID: "dash-luid-1",
        c.PATH: "SalesWB/Overview",
        c.CREATED_AT: "2021-01-01T00:00:00Z",
        c.UPDATED_AT: "2021-01-02T00:00:00Z",
        c.SHEETS: [{c.ID: "sheet-1"}, {c.ID: "sheet-2"}],
        c.WORKBOOK: WORKBOOK,
        c.TAGS: [],
    }
    dashboard.update(overrides)
    return dashboard


def _snapshot_aspect(work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]) -> T:
    for wu in work_units:
        if isinstance(wu.metadata, MetadataChangeEvent):
            for aspect in wu.metadata.proposedSnapshot.aspects:
                if isinstance(aspect, aspect_type):
                    return aspect
    raise AssertionError(f"no {aspect_type.__name__} aspect in snapshot")


def _mcp_aspects(
    work_units: Iterable[MetadataWorkUnit], aspect_type: Type[T]
) -> List[T]:
    return [
        wu.metadata.aspect
        for wu in work_units
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


def test_hidden_sheet_is_skipped_unless_hidden_worksheets_are_opted_in() -> None:
    hidden_sheet = _sheet(luid=None, path=None)

    source = _make_source(ingest_hidden_assets=["dashboard"])
    with mock.patch.object(
        source, "get_connection_objects", return_value=[hidden_sheet]
    ):
        assert list(source.emit_sheets()) == []
    assert source.report.num_hidden_assets_skipped == 1

    source = _make_source(
        ingest_hidden_assets=["worksheet"], tags_for_hidden_assets=["hidden"]
    )
    with mock.patch.object(
        source, "get_connection_objects", return_value=[hidden_sheet]
    ):
        work_units = list(source.emit_sheets())
    tags = _snapshot_aspect(work_units, GlobalTagsClass)
    assert [tag.tag for tag in tags.tags] == ["urn:li:tag:finance", "urn:li:tag:hidden"]
    # A hidden sheet has no view path, so there is nothing to link or embed.
    assert _snapshot_aspect(work_units, ChartInfoClass).externalUrl is None
    assert _mcp_aspects(work_units, EmbedClass) == []


def test_sheet_external_url_and_embed_follow_the_view_path() -> None:
    source = _make_source(ingest_embed_url=True)

    work_units = list(source.emit_sheets_as_charts(_sheet(), WORKBOOK))

    expected_url = f"{CONNECT_URI}/#/site/{SITE_CONTENT_URL}/views/SalesWB/MySheet"
    assert _snapshot_aspect(work_units, ChartInfoClass).externalUrl == expected_url
    assert [e.renderUrl for e in _mcp_aspects(work_units, EmbedClass)] == [expected_url]


def test_sheet_inside_a_dashboard_links_to_the_authoring_url() -> None:
    source = _make_source()
    sheet = _sheet(path=None, containedInDashboards=[{c.PATH: "SalesWB/Overview"}])

    work_units = list(source.emit_sheets_as_charts(sheet, WORKBOOK))

    assert (
        _snapshot_aspect(work_units, ChartInfoClass).externalUrl
        == f"{CONNECT_URI}/t/{SITE_CONTENT_URL}/authoring/SalesWB/Overview/My%20Sheet"
    )


def test_sheet_inputs_and_input_fields_come_from_datasource_fields() -> None:
    source = _make_source()
    sheet = _sheet(
        datasourceFields=[
            {c.NAME: "amount", c.DATA_SOURCE: {c.ID: "ds-2"}, "dataType": "INTEGER"},
            {c.NAME: "region", c.DATA_SOURCE: {c.ID: "ds-1"}, "dataType": "STRING"},
            None,
            {c.NAME: None, c.DATA_SOURCE: {c.ID: "ds-2"}},
        ]
    )

    work_units = list(source.emit_sheets_as_charts(sheet, WORKBOOK))

    ds_urns = {
        ds_id: make_dataset_urn_with_platform_instance("tableau", ds_id, None, "PROD")
        for ds_id in ("ds-1", "ds-2")
    }
    assert _snapshot_aspect(work_units, ChartInfoClass).inputs == [
        ds_urns["ds-1"],
        ds_urns["ds-2"],
    ]
    input_fields = _mcp_aspects(work_units, InputFieldsClass)
    assert [f.schemaFieldUrn for f in input_fields[0].fields] == [
        make_schema_field_urn(ds_urns["ds-1"], "region"),
        make_schema_field_urn(ds_urns["ds-2"], "amount"),
    ]
    # Datasources referenced by sheets are queued for published datasource ingestion.
    assert sorted(source.datasource_ids_being_used) == ["ds-1", "ds-2"]


def test_sheet_usage_stat_is_emitted_only_for_luids_in_the_registry() -> None:
    source = _make_source()
    source.tableau_stat_registry["sheet-luid-1"] = UsageStat(view_count=7)

    known = list(source.emit_sheets_as_charts(_sheet(), WORKBOOK))
    unknown = list(source.emit_sheets_as_charts(_sheet(luid="other-luid"), WORKBOOK))

    assert [s.viewsCount for s in _mcp_aspects(known, ChartUsageStatisticsClass)] == [7]
    assert _mcp_aspects(unknown, ChartUsageStatisticsClass) == []


def test_sheet_owner_container_and_browse_path_come_from_the_workbook() -> None:
    source = _make_source(use_email_as_username=True)
    source.tableau_project_registry = {PROJECT_ID: _project(["Parent", "Child"])}

    work_units = list(source.emit_sheets_as_charts(_sheet(), WORKBOOK))

    assert [o.owner for o in _snapshot_aspect(work_units, OwnershipClass).owners] == [
        "urn:li:corpuser:alice@example.com"
    ]
    assert [cn.container for cn in _mcp_aspects(work_units, ContainerClass)] == [
        source.gen_workbook_key("wb-1").as_urn()
    ]
    assert _snapshot_aspect(work_units, BrowsePathsClass).paths == [
        "/tableau/Parent/Child/Sales WB"
    ]


def test_browse_path_falls_back_to_project_name_when_project_is_unresolved() -> None:
    source = _make_source()
    source.workbook_project_map = {}
    cast(mock.MagicMock, source.server).workbooks.get_by_id.side_effect = Exception(
        "forbidden"
    )
    workbook = {**WORKBOOK, c.PROJECT_NAME: "Finance/Q1"}

    browse_paths = source.get_browse_paths_aspect(workbook)

    assert browse_paths is not None
    assert browse_paths.paths == ["/tableau/Finance|Q1/Sales WB"]


def test_dashboard_links_its_sheets_and_emits_embed_url() -> None:
    source = _make_source(ingest_embed_url=True)

    work_units = list(source.emit_dashboard(_dashboard(), WORKBOOK))

    info = _snapshot_aspect(work_units, DashboardInfoClass)
    assert info.title == "Q1|Overview"
    assert info.charts == [
        make_chart_urn("tableau", "sheet-1"),
        make_chart_urn("tableau", "sheet-2"),
    ]
    expected_url = f"{CONNECT_URI}/#/site/{SITE_CONTENT_URL}/views/SalesWB/Overview"
    assert info.dashboardUrl == expected_url
    assert [e.renderUrl for e in _mcp_aspects(work_units, EmbedClass)] == [expected_url]
    assert [cn.container for cn in _mcp_aspects(work_units, ContainerClass)] == [
        source.gen_workbook_key("wb-1").as_urn()
    ]
    assert [o.owner for o in _snapshot_aspect(work_units, OwnershipClass).owners] == [
        "urn:li:corpuser:alice"
    ]


def test_hidden_dashboard_is_skipped_when_hidden_assets_are_disabled() -> None:
    source = _make_source(ingest_hidden_assets=False)

    with mock.patch.object(
        source, "get_connection_objects", return_value=[_dashboard(luid=None)]
    ):
        assert list(source.emit_dashboards()) == []

    assert source.report.num_hidden_assets_skipped == 1
