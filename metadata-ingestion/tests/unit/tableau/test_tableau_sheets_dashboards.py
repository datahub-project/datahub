"""Sheet (chart) and dashboard emission in TableauSiteSource: hidden-asset
filtering, external/embed URLs, inputs, usage stats, ownership, containers and
browse paths.
"""

from unittest import mock

from datahub.emitter.mce_builder import make_chart_urn, make_schema_field_urn
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import UsageStat
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
from tests.unit.tableau.conftest import (
    PROJECT_ID,
    SITE_CONTENT_URL,
    WORKBOOK,
    SiteSourceFactory,
    default_config,
    mcp_aspects,
    mock_server_of,
    registry_project,
    snapshot_aspect,
    tableau_urn,
)

CONNECT_URI = default_config["connect_uri"]


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


def test_hidden_sheet_is_skipped_unless_hidden_worksheets_are_opted_in(
    site_source: SiteSourceFactory,
) -> None:
    """A sheet without a luid or view path is hidden. It is skipped by default;
    when opted in it is tagged with tags_for_hidden_assets and, having no view
    path, gets neither an external URL nor an embed aspect."""
    hidden_sheet = _sheet(luid=None, path=None)

    source = site_source(seed_project=True, ingest_hidden_assets=["dashboard"])
    with mock.patch.object(
        source, "get_connection_objects", autospec=True, return_value=[hidden_sheet]
    ):
        assert list(source.emit_sheets()) == []
    assert source.report.num_hidden_assets_skipped == 1

    source = site_source(
        seed_project=True,
        ingest_hidden_assets=["worksheet"],
        tags_for_hidden_assets=["hidden"],
    )
    with mock.patch.object(
        source, "get_connection_objects", autospec=True, return_value=[hidden_sheet]
    ):
        work_units = list(source.emit_sheets())
    tags = snapshot_aspect(work_units, GlobalTagsClass)
    assert [tag.tag for tag in tags.tags] == ["urn:li:tag:finance", "urn:li:tag:hidden"]
    assert snapshot_aspect(work_units, ChartInfoClass).externalUrl is None
    assert mcp_aspects(work_units, EmbedClass) == []


def test_sheet_external_url_and_embed_follow_the_view_path(
    site_source: SiteSourceFactory,
) -> None:
    source = site_source(seed_project=True, ingest_embed_url=True)

    work_units = list(source.emit_sheets_as_charts(_sheet(), WORKBOOK))

    expected_url = f"{CONNECT_URI}/#/site/{SITE_CONTENT_URL}/views/SalesWB/MySheet"
    assert snapshot_aspect(work_units, ChartInfoClass).externalUrl == expected_url
    assert [e.renderUrl for e in mcp_aspects(work_units, EmbedClass)] == [expected_url]


def test_sheet_inside_a_dashboard_links_to_the_authoring_url(
    site_source: SiteSourceFactory,
) -> None:
    """A sheet that only exists inside a dashboard has no view path of its own,
    so the link points at the dashboard's authoring URL for that sheet."""
    source = site_source(seed_project=True)
    sheet = _sheet(path=None, containedInDashboards=[{c.PATH: "SalesWB/Overview"}])

    work_units = list(source.emit_sheets_as_charts(sheet, WORKBOOK))

    assert (
        snapshot_aspect(work_units, ChartInfoClass).externalUrl
        == f"{CONNECT_URI}/t/{SITE_CONTENT_URL}/authoring/SalesWB/Overview/My%20Sheet"
    )


def test_sheet_inputs_and_input_fields_come_from_datasource_fields(
    site_source: SiteSourceFactory,
) -> None:
    """Datasource fields become the chart's inputs (one per datasource) and its
    input fields; null and nameless entries are skipped, and every datasource
    referenced is queued for published datasource ingestion."""
    source = site_source(seed_project=True)
    sheet = _sheet(
        datasourceFields=[
            {c.NAME: "amount", c.DATA_SOURCE: {c.ID: "ds-2"}, "dataType": "INTEGER"},
            {c.NAME: "region", c.DATA_SOURCE: {c.ID: "ds-1"}, "dataType": "STRING"},
            None,
            {c.NAME: None, c.DATA_SOURCE: {c.ID: "ds-2"}},
        ]
    )

    work_units = list(source.emit_sheets_as_charts(sheet, WORKBOOK))

    assert snapshot_aspect(work_units, ChartInfoClass).inputs == [
        tableau_urn("ds-1"),
        tableau_urn("ds-2"),
    ]
    input_fields = mcp_aspects(work_units, InputFieldsClass)
    assert [f.schemaFieldUrn for f in input_fields[0].fields] == [
        make_schema_field_urn(tableau_urn("ds-1"), "region"),
        make_schema_field_urn(tableau_urn("ds-2"), "amount"),
    ]
    assert sorted(source.datasource_ids_being_used) == ["ds-1", "ds-2"]


def test_sheet_usage_stat_is_emitted_only_for_luids_in_the_registry(
    site_source: SiteSourceFactory,
) -> None:
    source = site_source(seed_project=True)
    source.tableau_stat_registry["sheet-luid-1"] = UsageStat(view_count=7)

    known = list(source.emit_sheets_as_charts(_sheet(), WORKBOOK))
    unknown = list(source.emit_sheets_as_charts(_sheet(luid="other-luid"), WORKBOOK))

    assert [s.viewsCount for s in mcp_aspects(known, ChartUsageStatisticsClass)] == [7]
    assert mcp_aspects(unknown, ChartUsageStatisticsClass) == []


def test_sheet_owner_container_and_browse_path_come_from_the_workbook(
    site_source: SiteSourceFactory,
) -> None:
    source = site_source(seed_project=True, use_email_as_username=True)
    source.tableau_project_registry = {
        PROJECT_ID: registry_project(path=["Parent", "Child"])
    }

    work_units = list(source.emit_sheets_as_charts(_sheet(), WORKBOOK))

    assert [o.owner for o in snapshot_aspect(work_units, OwnershipClass).owners] == [
        "urn:li:corpuser:alice@example.com"
    ]
    assert [cn.container for cn in mcp_aspects(work_units, ContainerClass)] == [
        source.gen_workbook_key("wb-1").as_urn()
    ]
    assert snapshot_aspect(work_units, BrowsePathsClass).paths == [
        "/tableau/Parent/Child/Sales WB"
    ]


def test_browse_path_falls_back_to_project_name_when_project_is_unresolved(
    site_source: SiteSourceFactory,
) -> None:
    """When the workbook's project cannot be resolved (not in the map, REST
    lookup fails) the browse path is built from the project name Tableau gave,
    with "/" escaped so it does not split the path."""
    source = site_source(seed_project=True)
    source.workbook_project_map = {}
    mock_server_of(source).workbooks.get_by_id.side_effect = Exception("forbidden")
    workbook = {**WORKBOOK, c.PROJECT_NAME: "Finance/Q1"}

    browse_paths = source.get_browse_paths_aspect(workbook)

    assert browse_paths is not None
    assert browse_paths.paths == ["/tableau/Finance|Q1/Sales WB"]


def test_dashboard_links_its_sheets_and_emits_embed_url(
    site_source: SiteSourceFactory,
) -> None:
    source = site_source(seed_project=True, ingest_embed_url=True)

    work_units = list(source.emit_dashboard(_dashboard(), WORKBOOK))

    info = snapshot_aspect(work_units, DashboardInfoClass)
    assert info.title == "Q1|Overview"
    assert info.charts == [
        make_chart_urn("tableau", "sheet-1"),
        make_chart_urn("tableau", "sheet-2"),
    ]
    expected_url = f"{CONNECT_URI}/#/site/{SITE_CONTENT_URL}/views/SalesWB/Overview"
    assert info.dashboardUrl == expected_url
    assert [e.renderUrl for e in mcp_aspects(work_units, EmbedClass)] == [expected_url]
    assert [cn.container for cn in mcp_aspects(work_units, ContainerClass)] == [
        source.gen_workbook_key("wb-1").as_urn()
    ]
    assert [o.owner for o in snapshot_aspect(work_units, OwnershipClass).owners] == [
        "urn:li:corpuser:alice"
    ]


def test_hidden_dashboard_is_skipped_when_hidden_assets_are_disabled(
    site_source: SiteSourceFactory,
) -> None:
    source = site_source(seed_project=True, ingest_hidden_assets=False)

    with mock.patch.object(
        source,
        "get_connection_objects",
        autospec=True,
        return_value=[_dashboard(luid=None)],
    ):
        assert list(source.emit_dashboards()) == []

    assert source.report.num_hidden_assets_skipped == 1
