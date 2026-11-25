"""Unit tests for the PowerBI ``Mapper`` and the source-level emission paths.

These cover the PowerBI -> DataHub mapping branches (endorsement filter,
containers, ownership, profiling, dashboard/report/app emission) without
any HTTP: the mapper is driven from hand-built data classes.
"""

from typing import Any, Dict, Iterable, List, Optional
from unittest import mock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.powerbi import PowerBiDashboardSource
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    App,
    AppDashboard,
    AppReport,
    Measure,
    MeasureProfile,
    Page,
    PowerBIDataset,
    ReportType,
    Tile,
    Workspace,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ChartInfoClass,
    ContainerClass,
    DashboardInfoClass,
    DatasetProfileClass,
    GlobalTagsClass,
    MetadataChangeProposalClass,
    OwnershipClass,
    SubTypesClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from tests.unit.powerbi.conftest import (
    POWERBI_BASE_CONFIG,
    MsalStub,
    make_column,
    make_dashboard,
    make_dataset as _bare_dataset,
    make_mapper,
    make_report,
    make_table,
    make_tile,
    make_user,
    make_workspace,
    powerbi_config,
)

_ORDERS_URN = "urn:li:dataset:(urn:li:dataPlatform:powerbi,Model.Orders,PROD)"


def _dataset(
    dataset_id: str = "ds-1",
    tags: Optional[List[str]] = None,
    expression: Optional[str] = None,
    configured_by: Optional[str] = None,
) -> PowerBIDataset:
    """A ``Model`` dataset with one ``Orders`` table carrying an ``amount`` column."""
    dataset = _bare_dataset(dataset_id, tags=tags, configured_by=configured_by)
    dataset.tables = [
        make_table(
            "Orders",
            expression=expression,
            columns=[make_column("amount")],
            dataset=dataset,
        )
    ]
    return dataset


def _aspects(mcps: Iterable[MetadataChangeProposalWrapper], cls: type) -> List[Any]:
    return [m.aspect for m in mcps if isinstance(m.aspect, cls)]


def _wu_aspects(wus: Iterable[MetadataWorkUnit], cls: Any) -> List[Any]:
    found = [wu.get_aspect_of_type(cls) for wu in wus]
    return [a for a in found if a is not None]


# --- datasets ---------------------------------------------------------------


def test_dataset_outside_the_endorsement_filter_is_skipped() -> None:
    config = powerbi_config(filter_dataset_endorsements={"allow": ["Certified"]})
    mapper = make_mapper(config, make_workspace())
    assert (
        mapper.to_datahub_dataset(_dataset(tags=["Promoted"]), make_workspace()) == []
    )
    assert mapper.to_datahub_dataset(_dataset(tags=["Certified"]), make_workspace())


def test_dataset_tables_carry_view_logic_and_configured_by_owner() -> None:
    """``#(lf)`` escapes in the M expression are unescaped for viewLogic, the
    dataset's configuredBy becomes the owner, and every table is attached to
    the workspace container by default."""
    config = powerbi_config(
        extract_ownership=True,
        ownership={"dataset_configured_by_as_owner": True, "remove_email_suffix": True},
    )
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    mcps = mapper.to_datahub_dataset(
        _dataset(expression='ROW("a",#(lf)1)', configured_by="bob@example.com"),
        workspace,
    )
    views = _aspects(mcps, ViewPropertiesClass)
    assert [v.viewLogic for v in views] == ['ROW("a",\n1)']
    owners = _aspects(mcps, OwnershipClass)
    assert [o.owner for o in owners[0].owners] == ["urn:li:corpuser:bob"]
    containers = _aspects(mcps, ContainerClass)
    assert [c.container for c in containers] == [
        mapper.make_container_urn_for_workspace(workspace)
    ]


def test_dataset_tables_are_grouped_under_a_dataset_container_when_enabled() -> None:
    config = powerbi_config(
        extract_datasets_to_containers=True,
        extract_endorsements_to_tags=True,
    )
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    dataset = _dataset(tags=["Certified"])
    mcps = mapper.to_datahub_dataset(dataset, workspace)

    dataset_container_urn = mapper.make_container_urn_for_dataset(dataset)
    table_containers = [
        m.aspect.container
        for m in mcps
        if isinstance(m.aspect, ContainerClass) and m.entityUrn == _ORDERS_URN
    ]
    assert table_containers == [dataset_container_urn]
    # The dataset container itself is emitted and parented to the workspace.
    parent_links = [
        m.aspect.container
        for m in mcps
        if isinstance(m.aspect, ContainerClass) and m.entityUrn == dataset_container_urn
    ]
    assert parent_links == [mapper.make_container_urn_for_workspace(workspace)]
    tags = _aspects(mcps, GlobalTagsClass)
    assert [t.tag for t in tags[0].tags] == ["urn:li:tag:Certified"]


def test_profile_aspect_skips_hidden_and_denied_columns() -> None:
    config = powerbi_config(
        profiling={"enabled": True},
        profile_pattern={"deny": [".*\\.denied$"]},
    )
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    dataset = _dataset()
    table = dataset.tables[0]
    table.row_count, table.column_count = 42, 1
    stats = MeasureProfile(min="1", max="9", unique_count=3, sample_values=["1", "9"])
    table.columns = [
        make_column("amount"),
        make_column("hidden", hidden=True),
        make_column("denied"),
    ]
    for column in table.columns:
        column.measure_profile = stats

    mcps: List[MetadataChangeProposalWrapper] = []
    mapper.extract_profile(mcps, workspace, dataset, table, "urn:li:dataset:x")
    profiles = _aspects(mcps, DatasetProfileClass)
    assert len(profiles) == 1
    assert (profiles[0].rowCount, profiles[0].columnCount) == (42, 1)
    assert profiles[0].fieldProfiles is not None
    assert [f.fieldPath for f in profiles[0].fieldProfiles] == ["amount"]
    field = profiles[0].fieldProfiles[0]
    assert (field.min, field.max, field.uniqueCount, field.sampleValues) == (
        "1",
        "9",
        3,
        ["1", "9"],
    )


def test_profile_aspect_is_not_emitted_when_profiling_is_off() -> None:
    mapper = make_mapper(powerbi_config(), make_workspace())
    dataset = _dataset()
    mcps: List[MetadataChangeProposalWrapper] = []
    mapper.extract_profile(mcps, make_workspace(), dataset, dataset.tables[0], "urn:x")
    assert mcps == []


# --- dashboards and reports ------------------------------------------------


def test_dashboard_work_units_wire_charts_to_datasets_and_reports() -> None:
    """A tile created from a report becomes a chart whose inputs are the
    report's dataset tables and whose dashboard links back to the report;
    with create_corp_user off the owner is named without a corpuser entity."""
    config = powerbi_config(
        extract_ownership=True, ownership={"create_corp_user": False}
    )
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    dataset = _dataset()
    report = make_report(dataset)
    wus = mapper.to_datahub_work_units(
        make_dashboard([make_tile(dataset, report)], users=[make_user()]), workspace
    )

    charts = _wu_aspects(wus, ChartInfoClass)
    assert len(charts) == 1
    assert charts[0].inputs == [_ORDERS_URN]
    assert charts[0].externalUrl == report.webUrl
    assert charts[0].customProperties["reportId"] == "r-1"

    dashboards = _wu_aspects(wus, DashboardInfoClass)
    assert len(dashboards) == 1
    assert dashboards[0].charts == ["urn:li:chart:(powerbi,charts.t-1)"]
    assert dashboards[0].dashboards is not None
    assert [e.destinationUrn for e in dashboards[0].dashboards] == [
        mapper.make_dashboard_urn_for_report(report)
    ]
    assert dashboards[0].customProperties["chartCount"] == "1"

    owners = _wu_aspects(wus, OwnershipClass)
    assert [o.owner for o in owners[0].owners] == ["urn:li:corpuser:alice@example.com"]
    assert all(wu.is_primary_source for wu in wus)
    assert not any("corpuser" in wu.get_urn() for wu in wus)


def test_dashboard_user_work_units_are_secondary_when_users_are_created() -> None:
    """Stateful ingestion must never soft-delete users PowerBI stops listing,
    so the corpuser work units are flagged as non-primary."""
    config = powerbi_config(
        extract_ownership=True,
        ownership={"create_corp_user": True, "use_powerbi_email": True},
    )
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    wus = mapper.to_datahub_work_units(make_dashboard(users=[make_user()]), workspace)

    user_wus = [wu for wu in wus if wu.get_urn() == "urn:li:corpuser:alice@example.com"]
    assert user_wus
    assert not any(wu.is_primary_source for wu in user_wus)
    owners = _wu_aspects(wus, OwnershipClass)
    assert [o.owner for o in owners[0].owners] == ["urn:li:corpuser:alice@example.com"]


def test_report_work_units_make_pages_charts_with_dataset_edges() -> None:
    workspace = make_workspace()
    mapper = make_mapper(powerbi_config(), workspace)
    dataset = _dataset()
    pages = [Page(id="r-1.Summary", displayName="Summary", name="Summary", order=2)]
    wus = list(
        mapper.report_to_datahub_work_units(
            make_report(dataset, pages=pages), workspace
        )
    )

    charts = _wu_aspects(wus, ChartInfoClass)
    assert [(c.title, c.customProperties["order"]) for c in charts] == [
        ("Summary", "2")
    ]
    subtypes: Dict[str, List[str]] = {}
    for wu in wus:
        subtype = wu.get_aspect_of_type(SubTypesClass)
        if subtype is not None:
            subtypes[wu.get_urn()] = subtype.typeNames
    assert subtypes["urn:li:chart:(powerbi,pages.r-1.Summary)"] == ["PowerBI Page"]
    assert subtypes["urn:li:dashboard:(powerbi,reports.r-1)"] == [
        ReportType.PowerBIReport.value
    ]
    dashboards = _wu_aspects(wus, DashboardInfoClass)
    assert dashboards[0].charts == ["urn:li:chart:(powerbi,pages.r-1.Summary)"]
    assert dashboards[0].datasetEdges is not None
    assert [e.destinationUrn for e in dashboards[0].datasetEdges] == [_ORDERS_URN]


# --- source-level emission --------------------------------------------------


def _source(
    config: PowerBiDashboardSourceConfig, workspace: Workspace
) -> PowerBiDashboardSource:
    # Bypass __init__: it would build a PowerBiAPI (MSAL + HTTP).
    source = PowerBiDashboardSource.__new__(PowerBiDashboardSource)
    source.source_config = config
    source.reporter = PowerBiDashboardSourceReport()
    source.mapper = make_mapper(config, workspace)
    return source


def _app_workspace() -> Workspace:
    workspace = make_workspace()
    workspace.app = App(
        id="app-1",
        name="Sales App",
        description=None,
        last_update="2024-05-01T10:00:00.000Z",
        dashboards=[AppDashboard(id="d-app", original_dashboard_id="d-1")],
        reports=[AppReport(id="r-app", original_report_id="r-1")],
    )
    return workspace


def test_app_is_not_emitted_unless_opted_in() -> None:
    workspace = _app_workspace()
    source = _source(powerbi_config(), workspace)
    assert list(source.emit_app(workspace)) == []
    assert any(i.title == "App Ingestion Is Disabled" for i in source.reporter.infos)


def test_app_links_to_original_assets_and_uses_the_configured_url_pattern() -> None:
    """App dashboards/reports are copies; the app entity must link to the
    original workspace assets, not to the copies' ids."""
    workspace = _app_workspace()
    source = _source(
        powerbi_config(extract_app=True, app_url_pattern="REDIRECT_BASED"), workspace
    )
    mcps = list(source.emit_app(workspace))

    info = _aspects(mcps, DashboardInfoClass)[0]
    assert (
        info.dashboardUrl
        == "https://app.powerbi.com/Redirect?action=OpenApp&appId=app-1"
    )
    assert info.dashboards is not None
    assert sorted(e.destinationUrn for e in info.dashboards) == [
        "urn:li:dashboard:(powerbi,dashboards.d-1)",
        "urn:li:dashboard:(powerbi,reports.r-1)",
    ]
    assert info.lastModified.lastModified is not None
    assert {m.entityUrn for m in mcps} == {"urn:li:dashboard:(powerbi,apps.app-1)"}


def test_app_url_defaults_to_the_workspace_based_pattern() -> None:
    workspace = _app_workspace()
    source = _source(powerbi_config(extract_app=True), workspace)
    info = _aspects(source.emit_app(workspace), DashboardInfoClass)[0]
    assert info.dashboardUrl == "https://app.powerbi.com/groups/ws-1/apps/app-1"


def test_independent_datasets_are_reported_but_not_emitted_by_default() -> None:
    workspace = make_workspace()
    workspace.independent_datasets = {"ds-1": _dataset()}
    source = _source(powerbi_config(), workspace)
    assert list(source.extract_independent_datasets(workspace)) == []
    assert any(
        i.title == "Skipped Independent Dataset" and "Model" in str(i.context)
        for i in source.reporter.infos
    )

    source = _source(powerbi_config(extract_independent_datasets=True), workspace)
    wus = list(source.extract_independent_datasets(workspace))
    assert {wu.get_urn() for wu in wus} == {_ORDERS_URN}


@pytest.mark.parametrize("patch_metadata", [True, False])
def test_dashboard_info_is_patched_only_when_enabled(patch_metadata: bool) -> None:
    """Only dashboardInfo is converted into a PATCH (so other sources' chart
    lists survive); every other aspect passes through untouched."""
    workspace = make_workspace()
    source = _source(powerbi_config(patch_metadata=patch_metadata), workspace)
    info_wu = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dashboard:(powerbi,reports.r-1)",
        aspect=DashboardInfoClass(
            title="Report",
            description="",
            charts=[],
            lastModified=ChangeAuditStampsClass(),
        ),
    ).as_workunit()
    other_wu = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dashboard:(powerbi,reports.r-1)",
        aspect=SubTypesClass(typeNames=["Report"]),
    ).as_workunit()

    patched = source._get_dashboard_patch_work_unit(info_wu)
    assert patched is not None
    assert isinstance(
        patched.metadata, (MetadataChangeProposalClass, MetadataChangeProposalWrapper)
    )
    assert (patched.metadata.changeType == "PATCH") is patch_metadata
    assert source._get_dashboard_patch_work_unit(other_wu) is other_wu


def test_unknown_dataset_type_mapping_platform_is_rejected() -> None:
    workspace = make_workspace()
    source = _source(
        powerbi_config(dataset_type_mapping={"Teradata": "teradata"}), workspace
    )
    with pytest.raises(ValueError, match="Teradata"):
        source.validate_dataset_type_mapping()


def test_stale_entity_removal_runs_per_workspace_under_modified_since() -> None:
    """A modified_since run only sees changed workspaces, so the global
    stale-entity processor would wipe every workspace not in this run."""
    workspace = make_workspace()
    source = _source(
        powerbi_config(modified_since="2024-01-01T00:00:00.0000000Z"), workspace
    )
    assert source.get_excluded_workunit_processors() == [
        AutoStaleEntityRemovalProcessor
    ]
    assert source.get_workunit_processors() == []


# --- lineage and schema -----------------------------------------------------

_MSSQL_M_QUERY = (
    'let\n Source = Sql.Database("sql-host", "library"),\n'
    ' tbl = Source{[Schema="dbo",Item="book_issue"]}[Data]\nin\n tbl'
)


def test_extract_lineage_emits_upstream_and_column_lineage_for_mquery_tables() -> None:
    config = powerbi_config(extract_column_level_lineage=True)
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    dataset = _dataset(expression=_MSSQL_M_QUERY)
    ds_urn = "urn:li:dataset:(urn:li:dataPlatform:powerbi,model.orders,PROD)"

    mcps = mapper.extract_lineage(dataset.tables[0], ds_urn, workspace)

    lineage = _aspects(mcps, UpstreamLineageClass)
    assert len(lineage) == 1
    assert [u.dataset for u in lineage[0].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,library.dbo.book_issue,PROD)"
    ]
    assert lineage[0].fineGrainedLineages is not None
    assert [f.downstreams for f in lineage[0].fineGrainedLineages] == [
        [f"urn:li:schemaField:({ds_urn},amount)"]
    ]


def test_extract_lineage_skips_platforms_outside_dataset_type_mapping() -> None:
    config = powerbi_config(
        dataset_type_mapping={"PostgreSQL": "postgres"},
        extract_column_level_lineage=False,
    )
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    dataset = _dataset(expression=_MSSQL_M_QUERY)

    assert (
        mapper.extract_lineage(dataset.tables[0], "urn:li:dataset:x", workspace) == []
    )


def test_fine_grained_lineage_is_omitted_when_column_lineage_is_disabled() -> None:
    config = powerbi_config(extract_column_level_lineage=False)
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    dataset = _dataset(expression=_MSSQL_M_QUERY)

    mcps = mapper.extract_lineage(dataset.tables[0], "urn:li:dataset:x", workspace)
    assert _aspects(mcps, UpstreamLineageClass)[0].fineGrainedLineages is None


def test_schema_fields_carry_column_and_measure_descriptions() -> None:
    mapper = make_mapper(powerbi_config(), make_workspace())
    table = make_table(
        "Orders",
        columns=[make_column("amount", description="Order total")],
        measures=[
            Measure(
                name="total",
                expression="SUM(Orders[amount])",
                isHidden=False,
                description="Sum of totals",
            )
        ],
    )
    fields = {
        f.fieldPath: f.description for f in mapper.to_datahub_schema(table).fields
    }
    assert fields == {
        "amount": "Order total",
        "total": "SUM(Orders[amount]) Sum of totals",
    }


# --- ownership ---------------------------------------------------------------


def test_owner_criteria_keeps_only_users_with_a_matching_access_right() -> None:
    """Viewers and groups are dropped by the owner criteria even though the
    group has the right access level: only User principals become owners."""
    config = powerbi_config(
        extract_ownership=True,
        ownership={"create_corp_user": False, "owner_criteria": ["Owner"]},
    )
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    owner = make_user("owner")
    viewer = make_user(
        "viewer", display_name="Vic", email="vic@example.com", access_right="Read"
    )
    group = make_user("grp", display_name="Group", email=None, principal_type="Group")

    wus = mapper.to_datahub_work_units(
        make_dashboard(users=[owner, viewer, group]), workspace
    )
    owners = _wu_aspects(wus, OwnershipClass)
    assert [o.owner for o in owners[0].owners] == ["urn:li:corpuser:alice@example.com"]


def test_report_owners_are_emitted_and_users_are_secondary_work_units() -> None:
    config = powerbi_config(
        extract_ownership=True,
        ownership={"create_corp_user": True, "remove_email_suffix": True},
    )
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    report = make_report(_dataset(), users=[make_user()])

    wus = list(mapper.report_to_datahub_work_units(report, workspace))

    owners = _wu_aspects(wus, OwnershipClass)
    assert [o.owner for o in owners[0].owners] == ["urn:li:corpuser:alice"]
    user_wus = [wu for wu in wus if wu.get_urn() == "urn:li:corpuser:alice"]
    assert user_wus and not any(wu.is_primary_source for wu in user_wus)
    # A report without pages produces no chart children.
    assert _wu_aspects(wus, ChartInfoClass) == []


# --- containers and profiling -----------------------------------------------


def test_no_container_is_attached_when_containers_are_disabled() -> None:
    config = powerbi_config(extract_workspaces_to_containers=False)
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    mcps = mapper.to_datahub_dataset(_dataset(), workspace)
    assert _aspects(mcps, ContainerClass) == []


def test_profile_is_skipped_for_tables_outside_the_profile_pattern() -> None:
    config = powerbi_config(
        profiling={"enabled": True},
        profile_pattern={"deny": ["Sales\\.Model\\.Orders"]},
    )
    workspace = make_workspace()
    mapper = make_mapper(config, workspace)
    dataset = _dataset()
    mcps: List[MetadataChangeProposalWrapper] = []
    mapper.extract_profile(mcps, workspace, dataset, dataset.tables[0], "urn:x")
    assert mcps == []


def test_tile_without_a_resolved_dataset_still_becomes_a_chart() -> None:
    workspace = make_workspace()
    mapper = make_mapper(powerbi_config(), workspace)
    tile = make_tile(
        dataset_id="ds-missing", created_from=Tile.CreatedFrom.VISUALIZATION
    )
    wus = mapper.to_datahub_work_units(make_dashboard([tile]), workspace)
    charts = _wu_aspects(wus, ChartInfoClass)
    assert len(charts) == 1
    assert charts[0].inputs == []


# --- source-level emission --------------------------------------------------


def test_workspace_work_units_cover_container_dashboards_and_reports() -> None:
    workspace = make_workspace()
    config = powerbi_config(extract_workspaces_to_containers=True)
    source = _source(config, workspace)
    dataset = _dataset()
    workspace.datasets = {dataset.id: dataset}
    workspace.dashboards = {"d-1": make_dashboard()}
    workspace.reports = {"r-1": make_report(dataset)}

    wus = list(source.get_workspace_workunit(workspace))

    urns = {wu.get_urn() for wu in wus}
    assert source.mapper.make_container_urn_for_workspace(workspace) in urns
    assert "urn:li:dashboard:(powerbi,dashboards.d-1)" in urns
    assert "urn:li:dashboard:(powerbi,reports.r-1)" in urns
    assert source.reporter.dashboards_scanned == 1
    # Stale-entity removal stays global when no modified_since window is set.
    assert source.get_excluded_workunit_processors() == []


def test_test_connection_reports_authentication_outcome(msal_stub: MsalStub) -> None:
    ok = PowerBiDashboardSource.test_connection(dict(POWERBI_BASE_CONFIG))
    assert ok.basic_connectivity is not None and ok.basic_connectivity.capable

    with mock.patch(
        "msal.ConfidentialClientApplication",
        side_effect=RuntimeError("tenant not found"),
    ):
        failed = PowerBiDashboardSource.test_connection(dict(POWERBI_BASE_CONFIG))
    assert failed.basic_connectivity is not None
    assert not failed.basic_connectivity.capable
    assert "tenant not found" in str(failed.basic_connectivity.failure_reason)


def test_source_exits_when_the_powerbi_api_cannot_be_reached() -> None:
    with mock.patch(
        "msal.ConfidentialClientApplication",
        side_effect=RuntimeError("tenant not found"),
    ):
        with pytest.raises(SystemExit):
            PowerBiDashboardSource.create(
                dict(POWERBI_BASE_CONFIG), PipelineContext(run_id="t")
            )


def test_created_source_validates_mapping_and_closes_cleanly(
    msal_stub: MsalStub,
) -> None:
    source = PowerBiDashboardSource.create(
        {**POWERBI_BASE_CONFIG, "dataset_type_mapping": {"Sql": "mssql"}},
        PipelineContext(run_id="t"),
    )
    source.validate_dataset_type_mapping()
    assert source.get_workunit_processors()
    source.close()
