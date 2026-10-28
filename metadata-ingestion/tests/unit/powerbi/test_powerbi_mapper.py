"""Unit tests for the PowerBI ``Mapper`` and the source-level emission paths.

These cover the PowerBI -> DataHub mapping branches (endorsement filter,
containers, ownership, profiling, dashboard/report/app emission) without
any HTTP: the mapper is driven from hand-built data classes.
"""

from typing import Any, Dict, Iterable, List, Optional

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper, PowerBiDashboardSource
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    App,
    AppDashboard,
    AppReport,
    Column,
    Dashboard,
    MeasureProfile,
    Page,
    PowerBIDataset,
    Report,
    ReportType,
    Table,
    Tile,
    User,
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
    NumberTypeClass,
    OwnershipClass,
    SubTypesClass,
    ViewPropertiesClass,
)

_BASE = {"tenant_id": "t", "client_id": "c", "client_secret": "s"}


def _config(**overrides: Any) -> PowerBiDashboardSourceConfig:
    return PowerBiDashboardSourceConfig.model_validate({**_BASE, **overrides})


def _workspace() -> Workspace:
    return Workspace(
        id="ws-1",
        name="Sales",
        type="Workspace",
        webUrl="https://app.powerbi.com/groups/ws-1",
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def _mapper(config: PowerBiDashboardSourceConfig, workspace: Workspace) -> Mapper:
    mapper = Mapper(
        ctx=PipelineContext(run_id="test"),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )
    # The source emits the workspace container first; that call also fixes
    # the parent container key every later entity is attached to.
    list(mapper.generate_container_for_workspace(workspace))
    return mapper


def _column(name: str, hidden: bool = False) -> Column:
    return Column(
        name=name, dataType="Int64", isHidden=hidden, datahubDataType=NumberTypeClass()
    )


def _dataset(
    dataset_id: str = "ds-1",
    tags: Optional[List[str]] = None,
    expression: Optional[str] = None,
    configured_by: Optional[str] = None,
) -> PowerBIDataset:
    dataset = PowerBIDataset(
        id=dataset_id,
        name="Model",
        description="desc",
        webUrl="https://app.powerbi.com/groups/ws-1/datasets/ds-1",
        workspace_id="ws-1",
        workspace_name="Sales",
        parameters={},
        tables=[],
        tags=tags or [],
        configuredBy=configured_by,
    )
    dataset.tables = [
        Table(
            name="Orders",
            full_name="Model.Orders",
            expression=expression,
            columns=[_column("amount")],
            dataset=dataset,
        )
    ]
    return dataset


def _report(
    dataset: Optional[PowerBIDataset], pages: Optional[List[Page]] = None
) -> Report:
    return Report(
        id="r-1",
        name="Report",
        type=ReportType.PowerBIReport,
        webUrl="https://app.powerbi.com/groups/ws-1/reports/r-1",
        embedUrl=None,
        description="",
        dataset_id=dataset.id if dataset else None,
        dataset=dataset,
        pages=pages or [],
        users=[],
        tags=[],
    )


def _dashboard(tiles: List[Tile], users: Optional[List[User]] = None) -> Dashboard:
    return Dashboard(
        id="d-1",
        displayName="Dash",
        description="",
        embedUrl=None,
        isReadOnly=False,
        workspace_id="ws-1",
        workspace_name="Sales",
        tiles=tiles,
        users=users or [],
        tags=[],
        webUrl=None,
    )


def _user(user_id: str = "u-1") -> User:
    return User(
        id=user_id,
        displayName="Alice",
        emailAddress="alice@example.com",
        graphId="g-1",
        principalType="User",
        dashboardUserAccessRight="Owner",
    )


def _aspects(mcps: Iterable[MetadataChangeProposalWrapper], cls: type) -> List[Any]:
    return [m.aspect for m in mcps if isinstance(m.aspect, cls)]


def _wu_aspects(wus: Iterable[MetadataWorkUnit], cls: Any) -> List[Any]:
    found = [wu.get_aspect_of_type(cls) for wu in wus]
    return [a for a in found if a is not None]


# --- datasets ---------------------------------------------------------------


def test_dataset_outside_the_endorsement_filter_is_skipped() -> None:
    config = _config(filter_dataset_endorsements={"allow": ["Certified"]})
    mapper = _mapper(config, _workspace())
    assert mapper.to_datahub_dataset(_dataset(tags=["Promoted"]), _workspace()) == []
    assert mapper.to_datahub_dataset(_dataset(tags=["Certified"]), _workspace())


def test_dataset_tables_carry_view_logic_and_configured_by_owner() -> None:
    config = _config(
        extract_ownership=True,
        ownership={"dataset_configured_by_as_owner": True, "remove_email_suffix": True},
    )
    workspace = _workspace()
    mapper = _mapper(config, workspace)
    mcps = mapper.to_datahub_dataset(
        _dataset(expression='ROW("a",#(lf)1)', configured_by="bob@example.com"),
        workspace,
    )
    views = _aspects(mcps, ViewPropertiesClass)
    assert [v.viewLogic for v in views] == ['ROW("a",\n1)']
    owners = _aspects(mcps, OwnershipClass)
    assert [o.owner for o in owners[0].owners] == ["urn:li:corpuser:bob"]
    # Every table is attached to the workspace container by default.
    containers = _aspects(mcps, ContainerClass)
    assert [c.container for c in containers] == [
        mapper.make_container_urn_for_workspace(workspace)
    ]


def test_dataset_tables_are_grouped_under_a_dataset_container_when_enabled() -> None:
    config = _config(
        extract_datasets_to_containers=True,
        extract_endorsements_to_tags=True,
    )
    workspace = _workspace()
    mapper = _mapper(config, workspace)
    dataset = _dataset(tags=["Certified"])
    mcps = mapper.to_datahub_dataset(dataset, workspace)

    dataset_container_urn = mapper.make_container_urn_for_dataset(dataset)
    table_urn = next(
        m.entityUrn
        for m in mcps
        if isinstance(m.aspect, ViewPropertiesClass) or m.entityType == "dataset"
    )
    table_containers = [
        m.aspect.container
        for m in mcps
        if isinstance(m.aspect, ContainerClass) and m.entityUrn == table_urn
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
    config = _config(
        profiling={"enabled": True},
        profile_pattern={"deny": [".*\\.denied$"]},
    )
    workspace = _workspace()
    mapper = _mapper(config, workspace)
    dataset = _dataset()
    table = dataset.tables[0]
    table.row_count, table.column_count = 42, 1
    stats = MeasureProfile(min="1", max="9", unique_count=3, sample_values=["1", "9"])
    table.columns = [
        _column("amount"),
        _column("hidden", hidden=True),
        _column("denied"),
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
    mapper = _mapper(_config(), _workspace())
    dataset = _dataset()
    mcps: List[MetadataChangeProposalWrapper] = []
    mapper.extract_profile(mcps, _workspace(), dataset, dataset.tables[0], "urn:x")
    assert mcps == []


# --- dashboards and reports ------------------------------------------------


def test_dashboard_work_units_wire_charts_to_datasets_and_reports() -> None:
    config = _config(extract_ownership=True, ownership={"create_corp_user": False})
    workspace = _workspace()
    mapper = _mapper(config, workspace)
    dataset = _dataset()
    report = _report(dataset)
    tile = Tile(
        id="t-1",
        title="Tile",
        embedUrl=None,
        dataset_id=dataset.id,
        report_id=report.id,
        createdFrom=Tile.CreatedFrom.REPORT,
        dataset=dataset,
        report=report,
    )
    wus = mapper.to_datahub_work_units(_dashboard([tile], users=[_user()]), workspace)

    charts = _wu_aspects(wus, ChartInfoClass)
    assert len(charts) == 1
    assert charts[0].inputs == [
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Model.Orders,PROD)"
    ]
    assert charts[0].externalUrl == report.webUrl
    assert charts[0].customProperties["reportId"] == "r-1"

    dashboards = _wu_aspects(wus, DashboardInfoClass)
    assert len(dashboards) == 1
    assert dashboards[0].charts == [
        "urn:li:chart:(powerbi,charts.t-1)",
    ]
    assert dashboards[0].dashboards is not None
    assert [e.destinationUrn for e in dashboards[0].dashboards] == [
        mapper.make_dashboard_urn_for_report(report)
    ]
    assert dashboards[0].customProperties["chartCount"] == "1"

    # Soft-reference mode names the owner without creating a corpuser entity.
    owners = _wu_aspects(wus, OwnershipClass)
    assert [o.owner for o in owners[0].owners] == ["urn:li:corpuser:alice@example.com"]
    assert all(wu.is_primary_source for wu in wus)
    assert not any("corpuser" in wu.get_urn() for wu in wus)


def test_dashboard_user_work_units_are_secondary_when_users_are_created() -> None:
    config = _config(
        extract_ownership=True,
        ownership={"create_corp_user": True, "use_powerbi_email": True},
    )
    workspace = _workspace()
    mapper = _mapper(config, workspace)
    wus = mapper.to_datahub_work_units(_dashboard([], users=[_user()]), workspace)

    user_wus = [wu for wu in wus if wu.get_urn() == "urn:li:corpuser:alice@example.com"]
    assert user_wus
    # Stateful ingestion must never soft-delete users PowerBI stops listing.
    assert not any(wu.is_primary_source for wu in user_wus)
    owners = _wu_aspects(wus, OwnershipClass)
    assert [o.owner for o in owners[0].owners] == ["urn:li:corpuser:alice@example.com"]


def test_report_work_units_make_pages_charts_with_dataset_edges() -> None:
    config = _config()
    workspace = _workspace()
    mapper = _mapper(config, workspace)
    dataset = _dataset()
    pages = [Page(id="r-1.Summary", displayName="Summary", name="Summary", order=2)]
    wus = list(mapper.report_to_datahub_work_units(_report(dataset, pages), workspace))

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
    assert [e.destinationUrn for e in dashboards[0].datasetEdges] == [
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Model.Orders,PROD)"
    ]


# --- source-level emission --------------------------------------------------


def _source(
    config: PowerBiDashboardSourceConfig, workspace: Workspace
) -> PowerBiDashboardSource:
    # Bypass __init__: it would build a PowerBiAPI (MSAL + HTTP).
    source = PowerBiDashboardSource.__new__(PowerBiDashboardSource)
    source.source_config = config
    source.reporter = PowerBiDashboardSourceReport()
    source.mapper = _mapper(config, workspace)
    return source


def _app_workspace() -> Workspace:
    workspace = _workspace()
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
    source = _source(_config(), workspace)
    assert list(source.emit_app(workspace)) == []
    assert any(i.title == "App Ingestion Is Disabled" for i in source.reporter.infos)


def test_app_links_to_original_assets_and_uses_the_configured_url_pattern() -> None:
    workspace = _app_workspace()
    source = _source(
        _config(extract_app=True, app_url_pattern="REDIRECT_BASED"), workspace
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
    source = _source(_config(extract_app=True), workspace)
    info = _aspects(source.emit_app(workspace), DashboardInfoClass)[0]
    assert info.dashboardUrl == "https://app.powerbi.com/groups/ws-1/apps/app-1"


def test_independent_datasets_are_reported_but_not_emitted_by_default() -> None:
    workspace = _workspace()
    workspace.independent_datasets = {"ds-1": _dataset()}
    source = _source(_config(), workspace)
    assert list(source.extract_independent_datasets(workspace)) == []
    assert any(
        i.title == "Skipped Independent Dataset" and "Model" in str(i.context)
        for i in source.reporter.infos
    )

    source = _source(_config(extract_independent_datasets=True), workspace)
    wus = list(source.extract_independent_datasets(workspace))
    assert {wu.get_urn() for wu in wus} == {
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Model.Orders,PROD)"
    }


@pytest.mark.parametrize("patch_metadata", [True, False])
def test_dashboard_info_is_patched_only_when_enabled(patch_metadata: bool) -> None:
    workspace = _workspace()
    source = _source(_config(patch_metadata=patch_metadata), workspace)
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
    workspace = _workspace()
    source = _source(_config(dataset_type_mapping={"Teradata": "teradata"}), workspace)
    with pytest.raises(ValueError, match="Teradata"):
        source.validate_dataset_type_mapping()


def test_stale_entity_removal_runs_per_workspace_under_modified_since() -> None:
    workspace = _workspace()
    source = _source(_config(modified_since="2024-01-01T00:00:00.0000000Z"), workspace)
    # The global processor would wipe every workspace not in this partial run.
    assert source.get_excluded_workunit_processors() == [
        AutoStaleEntityRemovalProcessor
    ]
    assert source.get_workunit_processors() == []
