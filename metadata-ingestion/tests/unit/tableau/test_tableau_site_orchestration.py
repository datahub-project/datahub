"""Site-level orchestration in the Tableau source: the REST-backed registries
(usage stats, groups, projects, datasource/workbook project maps, database
server hostnames), the stage ordering of ``ingest_tableau_site``, and the
single-site entry point / shutdown of ``TableauSource``.
"""

from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, cast
from unittest import mock

import tableauserverclient as TSC
from tableauserverclient import GroupItem, PermissionsRule
from tableauserverclient.models import UserItem
from tableauserverclient.models.reference_item import ResourceReference

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    SiteIdContentUrl,
    TableauConfig,
    TableauProject,
    TableauSiteSource,
    TableauSource,
    TableauSourceReport,
    UsageStat,
)
from datahub.ingestion.source.tableau.tableau_common import MetadataQueryException
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DashboardUsageStatisticsClass,
)
from tests.unit.tableau.test_tableau_config import default_config

SITE_CONTENT_URL = "site-a"
PAGER = "datahub.ingestion.source.tableau.tableau.TSC.Pager"


def _site_item(site_id: str = "site-1", name: str = "Acryl") -> TSC.SiteItem:
    site = TSC.SiteItem(name=name, content_url=SITE_CONTENT_URL)
    site._id = site_id
    return site


def _make_source(
    site: Optional[TSC.SiteItem] = None, **overrides: object
) -> TableauSiteSource:
    config = TableauConfig.model_validate({**default_config, **overrides})
    server = mock.MagicMock()
    server.user_id = "user-1"
    server.site_id = "site-1"
    server.users.get_by_id.return_value = UserItem(
        name="user", site_role=UserItem.Roles.SiteAdministratorExplorer
    )
    return TableauSiteSource(
        config=config,
        ctx=PipelineContext(run_id="test"),
        platform="tableau",
        site=site
        or SiteIdContentUrl(site_id="site-1", site_content_url=SITE_CONTENT_URL),
        report=TableauSourceReport(),
        server=server,
    )


def _pager_for(
    source: TableauSiteSource, **items: Sequence[Any]
) -> Callable[..., List[Any]]:
    """A TSC.Pager stand-in that returns the configured items for each REST
    endpoint (``views``, ``groups``, ``projects``, ``datasources``, ``workbooks``)."""
    server = cast(mock.MagicMock, source.server)
    by_endpoint = {id(getattr(server, name)): list(seq) for name, seq in items.items()}

    def pager(endpoint: Any, **_kwargs: Any) -> List[Any]:
        return by_endpoint.get(id(endpoint), [])

    return pager


def _view(view_id: Optional[str], total_views: int) -> TSC.ViewItem:
    view = TSC.ViewItem()
    view._id = view_id
    view._total_views = total_views
    return view


def _group(group_id: Optional[str], name: str) -> GroupItem:
    group = GroupItem(name)
    group._id = group_id
    return group


def _project(project_id: Optional[str], name: Optional[str]) -> TSC.ProjectItem:
    project = TSC.ProjectItem(name=name)
    project._id = project_id
    return project


def _datasource(
    ds_id: Optional[str], project_id: str, name: str = "ds"
) -> TSC.DatasourceItem:
    datasource = TSC.DatasourceItem(project_id=project_id, name=name)
    datasource._id = ds_id
    return datasource


def _workbook(
    wb_id: Optional[str], project_id: str, name: str = "wb"
) -> TSC.WorkbookItem:
    workbook = TSC.WorkbookItem(project_id=project_id, name=name)
    workbook._id = wb_id
    return workbook


def _registry_project(project_id: str, name: str = "default") -> TableauProject:
    return TableauProject(
        id=project_id,
        name=name,
        description="",
        parent_id=None,
        parent_name=None,
        path=[name],
    )


def test_usage_stat_registry_is_keyed_by_view_id() -> None:
    source = _make_source()

    with mock.patch(
        PAGER, side_effect=_pager_for(source, views=[_view("v-1", 5), _view(None, 9)])
    ):
        source._populate_usage_stat_registry()

    assert source.tableau_stat_registry == {"v-1": UsageStat(view_count=5)}


def test_group_map_skips_groups_without_an_id() -> None:
    source = _make_source(permission_ingestion={})

    with mock.patch(
        PAGER,
        side_effect=_pager_for(
            source, groups=[_group("g-1", "Consumers"), _group(None, "Ghost")]
        ),
    ):
        source._fetch_groups()

    assert list(source.group_map) == ["g-1"]
    assert [w.title for w in source.report.warnings] == ["Group without an id skipped"]


def test_database_server_hostname_map_keeps_only_servers_with_an_id_and_host() -> None:
    source = _make_source(database_hostname_to_platform_instance_map={})
    servers = [
        {
            c.ID: "srv-1",
            c.NAME: "warehouse",
            c.HOST_NAME: "https://db.example.com:5432/sales",
            c.CONNECTION_TYPE: "postgres",
        },
        {c.ID: "srv-2", c.NAME: "plain", c.HOST_NAME: "plain-host"},
        {c.ID: None, c.NAME: "no id", c.HOST_NAME: "orphan-host"},
        {c.ID: "srv-4", c.NAME: "no host", c.HOST_NAME: None},
    ]

    with mock.patch.object(source, "get_connection_objects", return_value=servers):
        source._populate_database_server_hostname_map()

    assert source.database_server_hostname_map == {
        "srv-1": "db.example.com",
        "srv-2": "plain-host",
    }


def test_projects_registry_maps_assets_to_projects() -> None:
    source = _make_source()
    pager = _pager_for(
        source,
        projects=[_project("p-1", "default"), _project("p-2", "Unselected")],
        datasources=[
            _datasource("d-1", "p-1"),
            _datasource("d-2", "p-2"),
            _datasource(None, "p-1", name="idless"),
        ],
        workbooks=[
            _workbook("w-1", "p-1"),
            _workbook("w-2", "p-2"),
            _workbook(None, "p-1", name="idless"),
        ],
    )

    with mock.patch(PAGER, side_effect=pager):
        all_projects = source._populate_projects_registry()

    assert set(all_projects) == {"p-1", "p-2"}
    # Only projects matching the recipe's `projects` are ingested.
    assert list(source.tableau_project_registry) == ["p-1"]
    # Datasources are only mapped for selected projects ...
    assert source.datasource_project_map == {"d-1": "p-1"}
    # ... but every workbook is recorded so that unselected ones can be
    # distinguished from unresolved ones later without another REST call.
    assert source.workbook_project_map == {"w-1": "p-1", "w-2": "p-2"}
    assert sorted(w.title or "" for w in source.report.warnings) == [
        "Datasource without an id skipped",
        "Workbook without an id skipped",
    ]


def test_published_datasource_project_falls_back_to_rest_when_listing_failed() -> None:
    source = _make_source()
    source.tableau_project_registry = {"p-1": _registry_project("p-1")}
    server = cast(mock.MagicMock, source.server)
    pager = _pager_for(source, projects=[], workbooks=[])

    def failing_pager(endpoint: Any, **kwargs: Any) -> List[Any]:
        if endpoint is server.datasources:
            raise RuntimeError("listing datasources is broken")
        return pager(endpoint, **kwargs)

    with mock.patch(PAGER, side_effect=failing_pager):
        source._init_datasource_registry()
    assert source.report.get_all_datasources_query_failed

    server.datasources.get_by_id.side_effect = [
        _datasource("d-1", "p-1"),
        _datasource("d-2", "p-unselected"),
        RuntimeError("forbidden"),
    ]

    assert source._get_published_datasource_project_luid({c.LUID: "d-1"}) == "p-1"
    assert source._get_published_datasource_project_luid({c.LUID: "d-2"}) is None
    assert source._get_published_datasource_project_luid({c.LUID: "d-3"}) is None

    assert source.datasource_project_map == {"d-1": "p-1"}
    assert source.report.num_get_datasource_query_failures == 1


def test_browse_path_name_without_hierarchy_is_the_datasource_project_name() -> None:
    source = _make_source(extract_project_hierarchy=False)
    workbook = {c.ID: "wb-1", c.NAME: "WB", c.PROJECT_NAME: "Sales"}

    assert (
        source._get_project_browse_path_name(
            {c.TYPE_NAME: c.PUBLISHED_DATA_SOURCE, c.PROJECT_NAME: "Finance"}
        )
        == "Finance"
    )
    assert (
        source._get_project_browse_path_name(
            {c.TYPE_NAME: c.EMBEDDED_DATA_SOURCE, c.WORKBOOK: workbook}
        )
        == "Sales"
    )
    assert source._get_project_browse_path_name({c.TYPE_NAME: "CustomSQLTable"}) is None


def test_browse_path_name_with_hierarchy_requires_a_resolvable_project() -> None:
    source = _make_source(extract_project_hierarchy=True)

    # An embedded datasource that lost its workbook and an unsupported type
    # cannot be placed in the project tree.
    assert (
        source._get_project_browse_path_name(
            {c.TYPE_NAME: c.EMBEDDED_DATA_SOURCE, c.NAME: "orphan"}
        )
        is None
    )
    assert source._get_project_browse_path_name({c.TYPE_NAME: "CustomSQLTable"}) is None


def _container_properties(
    work_units: Iterable[MetadataWorkUnit],
) -> ContainerPropertiesClass:
    for wu in work_units:
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, ContainerPropertiesClass):
            return aspect
    raise AssertionError("no ContainerProperties aspect emitted")


def test_workbook_permissions_are_read_over_rest_when_enabled() -> None:
    source = _make_source(permission_ingestion={"enable_workbooks": True})
    source.tableau_project_registry = {"p-1": _registry_project("p-1")}
    source.workbook_project_map = {"wb-luid-1": "p-1"}
    source.group_map = {"g-1": _group("g-1", "Consumers")}
    server = cast(mock.MagicMock, source.server)
    server.workbooks.get_by_id.return_value.permissions = [
        PermissionsRule(
            grantee=ResourceReference("g-1", "group"),
            capabilities={"Read": "Allow", "Write": "Deny"},
        )
    ]
    workbook = {c.ID: "wb-1", c.NAME: "Sales WB", c.LUID: "wb-luid-1"}

    props = _container_properties(source.emit_workbook_as_container(workbook))

    server.workbooks.get_by_id.assert_called_once_with("wb-luid-1")
    assert props.customProperties is not None
    assert props.customProperties[c.LUID] == "wb-luid-1"
    assert (
        props.customProperties["permissions"]
        == '[{"group": "Consumers", "capabilities": ["Read"]}]'
    )


def test_workbook_without_a_luid_skips_permissions_with_a_warning() -> None:
    source = _make_source(permission_ingestion={"enable_workbooks": True})
    source.tableau_project_registry = {"p-1": _registry_project("p-1")}
    workbook = {c.ID: "wb-1", c.NAME: "Sales WB", c.PROJECT_NAME: "default"}

    props = _container_properties(source.emit_workbook_as_container(workbook))

    cast(mock.MagicMock, source.server).workbooks.get_by_id.assert_not_called()
    assert props.customProperties is not None
    assert "permissions" not in props.customProperties
    assert [w.title for w in source.report.warnings] == ["Workbook permissions skipped"]


def test_dashboard_usage_stat_is_emitted_only_for_luids_in_the_registry() -> None:
    source = _make_source()
    source.tableau_stat_registry["dash-luid-1"] = UsageStat(view_count=3)
    dashboard_urn = "urn:li:dashboard:(tableau,dash-1)"

    known = source._get_dashboard_stat_wu(
        {c.ID: "dash-1", c.LUID: "dash-luid-1"}, dashboard_urn
    )
    unknown = source._get_dashboard_stat_wu(
        {c.ID: "dash-2", c.LUID: "other"}, dashboard_urn
    )
    hidden = source._get_dashboard_stat_wu(
        {c.ID: "dash-3", c.LUID: None}, dashboard_urn
    )

    assert known is not None
    aspect = getattr(known.metadata, "aspect", None)
    assert isinstance(aspect, DashboardUsageStatisticsClass)
    assert aspect.viewsCount == 3
    assert unknown is None
    assert hidden is None


def test_site_ingestion_emits_only_containers_when_the_site_is_empty() -> None:
    source = _make_source(
        site=_site_item(), extract_usage_stats=False, add_site_container=True
    )
    pager = _pager_for(
        source, projects=[_project("p-1", "default")], datasources=[], workbooks=[]
    )

    with (
        mock.patch(PAGER, side_effect=pager) as pager_mock,
        mock.patch.object(source, "get_connection_objects", return_value=[]),
    ):
        work_units = list(source.ingest_tableau_site())

    # Site and project containers are emitted even though nothing lives in them.
    assert {wu.metadata.entityUrn for wu in work_units} == {
        source.gen_site_key("site-1").as_urn(),
        source.gen_project_key("p-1").as_urn(),
    }
    # Views and groups are not listed when their features are off.
    paged = {call.args[0] for call in pager_mock.call_args_list}
    server = cast(mock.MagicMock, source.server)
    assert server.views not in paged
    assert server.groups not in paged
    assert source.report.extract_usage_stats_timer == {}
    assert source.report.fetch_groups_timer == {}
    # Stages that had nothing to do never ran.
    assert source.report.emit_sheets_timer == {}
    assert source.report.emit_published_datasources_timer == {}
    assert source.report.emit_upstream_tables_timer == {}
    assert SITE_CONTENT_URL in source.report.emit_workbooks_timer


def test_site_ingestion_runs_stages_in_dependency_order() -> None:
    """Virtual connection lookup must run after datasources (which collect VC
    references) and before VC emission; custom SQL and upstream tables must
    come after every stage that can register them."""
    source = _make_source(
        permission_ingestion={},
        database_hostname_to_platform_instance_map={},
        ingest_virtual_connections=True,
    )
    order: List[str] = []

    def stage(name: str, after: Optional[Callable[[], None]] = None) -> mock.MagicMock:
        def run(*_args: Any, **_kwargs: Any) -> Iterable[MetadataWorkUnit]:
            order.append(name)
            if after:
                after()
            return iter(())

        return mock.MagicMock(side_effect=run)

    def discover_assets() -> None:
        source.sheet_ids.append("sheet-1")
        source.dashboard_ids.append("dash-1")
        source.embedded_datasource_ids_being_used.append("eds-1")

    def discover_upstreams() -> None:
        source.datasource_ids_being_used.append("ds-1")
        source.custom_sql_ids_being_used.append("csql-1")
        source.vc_processor.vc_table_ids_for_lookup.add("vct-1")
        source.database_tables[
            "urn:li:dataset:(urn:li:dataPlatform:postgres,t,PROD)"
        ] = mock.MagicMock()

    def resolve_vc() -> None:
        source.vc_processor.virtual_connection_ids_being_used.append("vc-1")

    patches: Dict[str, mock.MagicMock] = {
        "_populate_usage_stat_registry": stage("usage_stats"),
        "_fetch_groups": stage("groups"),
        "_populate_database_server_hostname_map": stage("hostnames"),
        "_populate_projects_registry": stage("projects"),
        "emit_project_containers": stage("project_containers"),
        "emit_workbooks": stage("workbooks", discover_assets),
        "emit_sheets": stage("sheets"),
        "emit_dashboards": stage("dashboards"),
        "emit_embedded_datasources": stage("embedded_datasources", discover_upstreams),
        "emit_published_datasources": stage("published_datasources"),
        "emit_custom_sql_datasources": stage("custom_sql"),
        "emit_upstream_tables": stage("upstream_tables"),
    }
    vc_patches: Dict[str, mock.MagicMock] = {
        "lookup_vc_ids_from_table_ids": stage("vc_lookup", resolve_vc),
        "emit_virtual_connections": stage("virtual_connections"),
    }
    db_lookup = stage("db_table_lookup")
    db_lookup.side_effect = RuntimeError("metadata API refused databaseTables")

    with (
        mock.patch.multiple(source, **patches),
        mock.patch.multiple(source.vc_processor, **vc_patches),
        mock.patch.object(source, "_build_database_tables_lookup", db_lookup),
    ):
        list(source.ingest_tableau_site())

    assert order == [
        "usage_stats",
        "groups",
        "hostnames",
        "projects",
        "project_containers",
        "workbooks",
        "sheets",
        "dashboards",
        "embedded_datasources",
        "published_datasources",
        "vc_lookup",
        "virtual_connections",
        "custom_sql",
        "upstream_tables",
    ]
    # A failing database-table lookup degrades VC lineage; it does not abort the site.
    db_lookup.assert_called_once()
    assert source.report.num_virtual_connections_table_references_found == 1
    assert source.report.num_virtual_connections_processed == 1
    assert SITE_CONTENT_URL in source.report.emit_virtual_connections_timer


def _make_tableau_source(server: mock.MagicMock) -> TableauSource:
    config = {**default_config}
    del config["stateful_ingestion"]
    with (
        mock.patch.object(TableauConfig, "make_tableau_client", return_value=server),
        mock.patch("datahub.ingestion.source.tableau.tableau.report_user_role"),
    ):
        return TableauSource(TableauConfig.model_validate(config), PipelineContext("t"))


def test_single_site_ingestion_uses_the_signed_in_site() -> None:
    server = mock.MagicMock()
    server.is_signed_in.return_value = True
    server.site_id = "site-1"
    site = _site_item()
    server.sites.get_by_id.return_value = site
    source = _make_tableau_source(server)

    with mock.patch(
        "datahub.ingestion.source.tableau.tableau.TableauSiteSource"
    ) as site_source_cls:
        site_source_cls.return_value.ingest_tableau_site.return_value = iter(())
        list(source.get_workunits_internal())

    server.sites.get_by_id.assert_called_once_with("site-1")
    assert site_source_cls.call_args.kwargs["site"] is site
    assert source.report.warnings == []


def test_single_site_ingestion_falls_back_to_the_configured_site_url() -> None:
    server = mock.MagicMock()
    server.is_signed_in.return_value = True
    server.site_id = "site-1"
    server.sites.get_by_id.side_effect = RuntimeError("403 forbidden")
    source = _make_tableau_source(server)

    with mock.patch(
        "datahub.ingestion.source.tableau.tableau.TableauSiteSource"
    ) as site_source_cls:
        site_source_cls.return_value.ingest_tableau_site.return_value = iter(())
        list(source.get_workunits_internal())

    assert site_source_cls.call_args.kwargs["site"] == SiteIdContentUrl(
        site_id="site-1", site_content_url=str(default_config["site"])
    )
    assert [w.title for w in source.report.warnings] == [
        "Unable to fetch site details. Site hierarchy may be incomplete and external urls may be missing."
    ]


def test_metadata_query_failure_is_reported_not_raised() -> None:
    server = mock.MagicMock()
    server.is_signed_in.return_value = True
    server.site_id = "site-1"
    server.sites.get_by_id.return_value = _site_item()
    source = _make_tableau_source(server)

    with mock.patch.object(
        TableauSiteSource,
        "ingest_tableau_site",
        side_effect=MetadataQueryException("metadata api down"),
    ):
        assert list(source.get_workunits_internal()) == []

    assert [f.title for f in source.report.failures] == [
        "Failed to Retrieve Tableau Metadata"
    ]


def test_close_survives_a_failed_sign_out() -> None:
    server = mock.MagicMock()
    server.auth.sign_out.side_effect = RuntimeError("session already gone")
    source = _make_tableau_source(server)

    source.close()

    server.auth.sign_out.assert_called_once()
    assert source.server is None
