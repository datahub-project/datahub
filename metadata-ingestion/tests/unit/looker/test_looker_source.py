"""LookerDashboardSource driven against a fake Looker SDK client.

The SDK client (``looker_sdk.init40``) is the only thing mocked; everything
above it -- dashboard/element parsing, explore resolution, folder containers,
usage statistics and the Looker API wrapper -- runs for real.
"""

import datetime
import json
from typing import Any, Dict, Iterable, List, Optional, Union
from unittest import mock

import pytest
from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardBase,
    DashboardElement,
    Folder,
    FolderBase,
    Look,
    LookmlModel,
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    LookmlModelExploreJoins,
    LookmlModelNavExplore,
    LookWithQuery,
    PermissionSet,
    Query,
    ResultMakerFilterables,
    ResultMakerFilterablesListen,
    ResultMakerWithIdVisConfigAndDynamicFields,
    Role,
    User,
    WriteQuery,
)

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.looker.looker_common import (
    LookerDashboardElement,
    LookerExplore,
    LookerFolder,
)
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    BASIC_INGEST_REQUIRED_PERMISSIONS,
)
from datahub.ingestion.source.looker.looker_query_model import (
    HistoryViewField,
    LookViewField,
    QueryViewField,
    UserViewField,
)
from datahub.ingestion.source.looker.looker_source import LookerDashboardSource
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ChartTypeClass,
    ChartUsageStatisticsClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DashboardUsageStatisticsClass,
    DatasetUsageStatisticsClass,
    EmbedClass,
    InputFieldsClass,
    OwnershipClass,
    UpstreamLineageClass,
)
from datahub.sdk.entity import Entity

BASE_URL = "https://looker.example.com"
SHARED = FolderBase(id="f-shared", name="Shared")
SALES = FolderBase(id="f-sales", name="Sales", parent_id="f-shared")
PERSONAL = FolderBase(
    id="f-personal",
    name="Someone",
    parent_id="f-users",
    is_personal=True,
    is_personal_descendant=True,
)
ANCESTORS: Dict[str, List[Folder]] = {
    "f-shared": [],
    "f-sales": [Folder(id="f-shared", name="Shared")],
    "f-personal": [Folder(id="f-users", name="Users")],
}

DYNAMIC_FIELDS = json.dumps(
    [
        {"table_calculation": "calc", "label": "Calc", "expression": "1"},
        {"measure": "orders.total", "label": "Total", "based_on": "orders.amount"},
        {"dimension": "orders.bucket", "label": "Bucket"},
    ]
)

QUERY_ELEMENT = DashboardElement(
    id="e-query",
    type="vis",
    title="By query",
    subtitle_text="query element",
    dashboard_id="d-1",
    query=Query(
        model="sales",
        view="orders",
        fields=["orders.id", "orders.amount"],
        filters={"orders.status": "shipped"},
        dynamic_fields=DYNAMIC_FIELDS,
        slug="slug-query",
    ),
)
# Mirrors the integration fixtures: the element shares the look's id, which is
# also what usage extraction keys chart usage on.
LOOK_ELEMENT = DashboardElement(
    id="l-1",
    type="vis",
    title="",
    dashboard_id="d-1",
    look_id="l-1",
    look=LookWithQuery(
        id="l-1",
        title="Look title",
        user_id="u-1",
        view_count=4,
        folder=SALES,
        query=Query(model="sales", view="orders", fields=["orders.id"], slug="s1"),
    ),
)
RESULT_MAKER_ELEMENT = DashboardElement(
    id="e-rm",
    type="looker_line",
    title="By result maker",
    dashboard_id="d-1",
    result_maker=ResultMakerWithIdVisConfigAndDynamicFields(
        query=Query(model="sales", view="orders", fields=["orders.id"]),
        filterables=[
            ResultMakerFilterables(
                model="finance",
                view="invoices",
                listen=[
                    ResultMakerFilterablesListen(
                        dashboard_filter_name="date", field="invoices.date"
                    )
                ],
            )
        ],
    ),
)
TEXT_ELEMENT = DashboardElement(id="e-text", type="text", title="notes")

LIVE_DASHBOARD = Dashboard(
    id="d-1",
    title="Revenue",
    description="desc",
    folder=SALES,
    user_id="u-1",
    last_updater_id="u-2",
    created_at=datetime.datetime(2024, 1, 1),
    updated_at=datetime.datetime(2024, 1, 2),
    favorite_count=3,
    view_count=10,
    last_viewed_at=datetime.datetime(2024, 1, 3),
    dashboard_elements=[
        QUERY_ELEMENT,
        LOOK_ELEMENT,
        RESULT_MAKER_ELEMENT,
        TEXT_ELEMENT,
    ],
)
PERSONAL_DASHBOARD = Dashboard(
    id="d-personal", title="Mine", folder=PERSONAL, dashboard_elements=[]
)
DELETED_DASHBOARD = Dashboard(
    id="d-deleted", title="Old", folder=SALES, deleted=True, dashboard_elements=[]
)
NO_FOLDER_DASHBOARD = Dashboard(id="d-nofolder", title="Loose", dashboard_elements=[])

EXPLORE_FIELDS = LookmlModelExploreFieldset(
    dimensions=[
        LookmlModelExploreField(
            name="orders.id",
            type="number",
            label_short="Id",
            primary_key=True,
            view="orders",
            original_view="orders",
        ),
        LookmlModelExploreField(
            name="orders.created_date",
            type="date_date",
            dimension_group="orders.created",
            field_group_variant="Date",
            view="orders",
        ),
        LookmlModelExploreField(name=None, type="string"),
        LookmlModelExploreField(
            name="customers.email",
            type="string",
            view="customers",
            original_view="customers",
            field_group_label="Contact",
        ),
    ],
    measures=[
        LookmlModelExploreField(
            name="orders.amount", type="sum", view="orders", original_view="orders"
        ),
        LookmlModelExploreField(name=None, type="sum"),
    ],
    parameters=[
        LookmlModelExploreField(name="orders.period", type="string", view="orders")
    ],
)


def explore(model: str, name: str) -> LookmlModelExplore:
    return LookmlModelExplore(
        id=f"{model}::{name}",
        name=name,
        label=f"{name} label",
        description="an explore",
        view_name="orders",
        project_name="proj",
        source_file="orders.explore.lkml",
        tags=["finance"],
        fields=EXPLORE_FIELDS,
        joins=[
            LookmlModelExploreJoins(
                name="buyers",
                from_="customers",
                sql_on="${orders.customer_id} = ${buyers.id}",
                relationship="many_to_one",
                type="left_outer",
                dependent_fields=["orders.customer_id", "buyers.id", "no_view_prefix"],
            ),
            LookmlModelExploreJoins(name="items", dependent_fields=None),
        ],
    )


def _usage_rows(body: WriteQuery) -> List[Dict[str, Any]]:
    fields = list(body.fields or [])
    if HistoryViewField.HISTORY_DASHBOARD_ID in fields:
        row: Dict[str, Any] = {
            HistoryViewField.HISTORY_DASHBOARD_ID: "d-1",
            HistoryViewField.HISTORY_CREATED_DATE: "2024-01-05",
            HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT: 7,
        }
        if UserViewField.USER_ID in fields:
            row[UserViewField.USER_ID] = "u-1"
        else:
            row[HistoryViewField.HISTORY_DASHBOARD_USER] = 2
        return [row]
    if LookViewField.LOOK_ID in fields:
        row = {
            LookViewField.LOOK_ID: 1,
            HistoryViewField.HISTORY_CREATED_DATE: "2024-01-05",
            HistoryViewField.HISTORY_COUNT: 3,
        }
        if UserViewField.USER_ID in fields:
            row[UserViewField.USER_ID] = "u-1"
        return [{**row, LookViewField.LOOK_ID: "l-1"}]
    if QueryViewField.QUERY_VIEW in fields:
        row = {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2024-01-05",
            HistoryViewField.HISTORY_COUNT: 5,
        }
        if UserViewField.USER_ID in fields:
            row[UserViewField.USER_ID] = "u-1"
        if QueryViewField.QUERY_FIELDS in fields:
            row[QueryViewField.QUERY_FIELDS] = '["orders.id", "orders.amount"]'
        return [row]
    return []


def fake_client() -> mock.MagicMock:
    client = mock.MagicMock()
    client.me.return_value = User(id="api", email="api@example.com")
    client.all_dashboards.return_value = [
        DashboardBase(id="d-1"),
        DashboardBase(id="d-personal"),
        DashboardBase(id="d-denied"),
        DashboardBase(id="d-nofolder"),
        DashboardBase(id=None),
    ]
    client.search_dashboards.return_value = [DELETED_DASHBOARD]
    by_id = {
        d.id: d
        for d in [
            LIVE_DASHBOARD,
            PERSONAL_DASHBOARD,
            DELETED_DASHBOARD,
            NO_FOLDER_DASHBOARD,
        ]
    }

    def dashboard(
        dashboard_id: str, fields: Any = None, transport_options: Any = None
    ) -> Dashboard:
        if dashboard_id not in by_id:
            raise SDKError("<title>Looker Not Found (404)</title>")
        return by_id[dashboard_id]

    client.dashboard.side_effect = dashboard
    client.folder_ancestors.side_effect = (
        lambda folder_id, fields=None, transport_options=None: list(
            ANCESTORS.get(folder_id, [])
        )
    )
    client.lookml_model_explore.side_effect = (
        lambda model, explore_name, fields=None, transport_options=None: explore(
            model, explore_name
        )
    )
    client.all_lookml_models.return_value = [
        LookmlModel(
            name="sales",
            project_name="proj",
            explores=[
                LookmlModelNavExplore(name="orders"),
                LookmlModelNavExplore(name="unused"),
                LookmlModelNavExplore(name=None),
            ],
        ),
        LookmlModel(name=None, explores=[]),
    ]
    users = {
        "u-1": User(id="u-1", email="one@example.com"),
        "u-2": User(id="u-2", email=None, display_name="Two"),
    }
    client.all_users.return_value = list(users.values())
    client.user.side_effect = lambda id_, fields=None, transport_options=None: users[
        id_
    ]
    client.run_inline_query.side_effect = (
        lambda result_format, body, transport_options=None, cache=None: json.dumps(
            _usage_rows(body)
        )
    )
    client.user_roles.return_value = [
        Role(
            permission_set=PermissionSet(
                permissions=sorted(BASIC_INGEST_REQUIRED_PERMISSIONS)
            )
        )
    ]
    client.all_looks.return_value = []
    client.search_looks.return_value = []
    return client


def make_config(**overrides: Any) -> LookerDashboardSourceConfig:
    raw: Dict[str, Any] = {
        "base_url": BASE_URL,
        "client_id": "id",
        "client_secret": "secret",
        "max_threads": 1,
        "extract_usage_history": False,
        "extract_owners": True,
    }
    raw.update(overrides)
    return LookerDashboardSourceConfig.model_validate(raw)


def make_source(
    client: Optional[mock.MagicMock] = None, **overrides: Any
) -> LookerDashboardSource:
    ctx = PipelineContext("t")
    if overrides.get("stateful_ingestion"):
        ctx = PipelineContext("t", pipeline_name="p", graph=mock.MagicMock())
    with mock.patch("looker_sdk.init40", return_value=client or fake_client()):
        return LookerDashboardSource(make_config(**overrides), ctx)


class Emitted:
    def __init__(self, items: Iterable[Union[MetadataWorkUnit, Entity]]):
        self.workunits: List[MetadataWorkUnit] = []
        for item in items:
            if isinstance(item, Entity):
                self.workunits.extend(item.as_workunits())
            else:
                self.workunits.append(item)

    def aspects(self, cls: type) -> Dict[str, List[Any]]:
        found: Dict[str, List[Any]] = {}
        for wu in self.workunits:
            aspect = wu.get_aspect_of_type(cls)
            if aspect is not None:
                found.setdefault(wu.get_urn(), []).append(aspect)
        return found

    def urns(self, cls: type) -> List[str]:
        return sorted(self.aspects(cls))


def run(source: LookerDashboardSource) -> Emitted:
    return Emitted(source.get_workunits_internal())


class TestConnectionTest:
    def test_reports_capabilities_from_granted_permissions(self) -> None:
        client = fake_client()
        with mock.patch("looker_sdk.init40", return_value=client):
            report = LookerDashboardSource.test_connection(
                {"base_url": BASE_URL, "client_id": "id", "client_secret": "s"}
            )
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable
        assert report.capability_report is not None
        assert report.capability_report[SourceCapability.DESCRIPTIONS].capable
        # see_system_activity is not granted, so usage is reported as not capable.
        assert not report.capability_report[SourceCapability.USAGE_STATS].capable

    def test_authentication_failure_is_an_internal_failure(self) -> None:
        client = fake_client()
        client.me.side_effect = SDKError("nope")
        with mock.patch("looker_sdk.init40", return_value=client):
            report = LookerDashboardSource.test_connection(
                {"base_url": BASE_URL, "client_id": "id", "client_secret": "s"}
            )
        assert report.internal_failure
        assert report.basic_connectivity is not None
        assert not report.basic_connectivity.capable


class TestQueryInputFields:
    def test_dynamic_fields_filters_and_fields_become_input_fields(self) -> None:
        source = make_source()
        assert QUERY_ELEMENT.query is not None
        fields = source._get_input_fields_from_query(QUERY_ELEMENT.query)
        by_name = {f.name: f for f in fields}
        assert by_name["calc"].view_field is not None
        assert by_name["orders.total"].view_field is not None
        # A custom measure also references the field it is based on.
        assert by_name["orders.amount"].view_field is None
        assert by_name["orders.amount"].explore == "orders"
        assert by_name["orders.bucket"].view_field is not None
        assert by_name["orders.status"].model == "sales"

    def test_unparseable_dynamic_fields_are_ignored(self) -> None:
        source = make_source()
        fields = source._get_input_fields_from_query(
            Query(
                model="sales", view="orders", fields=["orders.id"], dynamic_fields="{"
            )
        )
        assert [f.name for f in fields] == ["orders.id"]

    def test_no_query_yields_no_fields(self) -> None:
        assert make_source()._get_input_fields_from_query(None) == []

    def test_fields_without_view_prefix_are_reported(self) -> None:
        source = make_source()
        assert source._get_views_from_fields(["orders.id", "bare"]) == ["orders"]
        assert any(
            w.title == "Failed to Extract View Name from Field"
            for w in source.reporter.warnings
        )


class TestDashboardElements:
    def test_look_backed_element_uses_look_title_folder_and_owner(self) -> None:
        source = make_source()
        element = source._get_looker_dashboard_element(LOOK_ELEMENT)
        assert element is not None
        assert element.title == "Look title"
        assert element.look_id == "l-1"
        assert element.query_slug == "s1"
        assert element.folder_path == "Shared/Sales"
        assert element.owner is not None
        assert element.owner.email == "one@example.com"
        assert [e.name for e in element.upstream_explores] == ["orders"]
        assert ("sales", "orders") in source.reachable_explores

    def test_result_maker_element_collects_explores_from_query_and_filters(
        self,
    ) -> None:
        source = make_source()
        element = source._get_looker_dashboard_element(RESULT_MAKER_ELEMENT)
        assert element is not None
        assert sorted((e.model_name, e.name) for e in element.upstream_explores) == [
            ("finance", "invoices"),
            ("sales", "orders"),
        ]
        assert element.input_fields is not None
        assert "invoices.date" in [f.name for f in element.input_fields]

    def test_element_without_query_look_or_result_maker_is_dropped(self) -> None:
        assert make_source()._get_looker_dashboard_element(TEXT_ELEMENT) is None

    def test_element_without_id_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            make_source()._get_looker_dashboard_element(DashboardElement(id=None))

    def test_chart_type_mapping(self) -> None:
        source = make_source()

        def element(chart_type: Optional[str]) -> LookerDashboardElement:
            return LookerDashboardElement(
                id="x",
                title="",
                query_slug="",
                upstream_explores=[],
                look_id=None,
                type=chart_type,
            )

        assert source._get_chart_type(element("looker_line")) == ChartTypeClass.LINE
        assert source._get_chart_type(element("looker_map")) is None
        assert source._get_chart_type(element("unknown_vis")) is None
        assert source._get_chart_type(element(None)) is None
        assert len(source.reporter.infos) == 2


class TestGetWorkunits:
    def test_dashboards_charts_folders_and_explores(self) -> None:
        source = make_source(
            include_deleted=True,
            extract_embed_urls=True,
            skip_personal_folders=True,
            emit_used_explores_only=False,
            dashboard_pattern={"deny": ["^d-denied$"]},
        )
        emitted = run(source)

        dashboards = emitted.aspects(DashboardInfoClass)
        assert sorted(dashboards) == [
            "urn:li:dashboard:(looker,dashboards.d-1)",
            "urn:li:dashboard:(looker,dashboards.d-deleted)",
            "urn:li:dashboard:(looker,dashboards.d-nofolder)",
        ]
        revenue = dashboards["urn:li:dashboard:(looker,dashboards.d-1)"][0]
        assert revenue.dashboardUrl == f"{BASE_URL}/dashboards/d-1"
        # Only `vis` elements become charts; the looker_line element is skipped.
        assert sorted(e.destinationUrn for e in revenue.chartEdges or []) == [
            "urn:li:chart:(looker,dashboard_elements.e-query)",
            "urn:li:chart:(looker,dashboard_elements.l-1)",
        ]

        charts = emitted.aspects(ChartInfoClass)
        assert charts["urn:li:chart:(looker,dashboard_elements.e-query)"][
            0
        ].chartUrl == (f"{BASE_URL}/x/slug-query")
        assert charts["urn:li:chart:(looker,dashboard_elements.l-1)"][0].chartUrl == (
            f"{BASE_URL}/looks/l-1"
        )
        embeds = emitted.aspects(EmbedClass)
        assert embeds["urn:li:chart:(looker,dashboard_elements.l-1)"][0].renderUrl == (
            f"{BASE_URL}/embed/looks/l-1"
        )
        assert "urn:li:chart:(looker,dashboard_elements.e-query)" not in embeds
        assert "urn:li:dashboard:(looker,dashboards.d-1)" in embeds

        containers = emitted.aspects(ContainerPropertiesClass)
        # Folder containers plus one container per LookML model listed by the API.
        assert sorted(c[0].name for c in containers.values()) == [
            "Sales",
            "Shared",
            "sales",
        ]

        # Every explore of every listed model is fetched, used or not.
        explores = emitted.urns(UpstreamLineageClass)
        assert explores == [
            "urn:li:dataset:(urn:li:dataPlatform:looker,sales.explore.orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:looker,sales.explore.unused,PROD)",
        ]
        owners = emitted.aspects(OwnershipClass)[
            "urn:li:dashboard:(looker,dashboards.d-1)"
        ][0]
        assert [o.owner for o in owners.owners] == ["urn:li:corpuser:one@example.com"]

        assert source.reporter.total_dashboards == 4
        assert source.reporter.dashboards_scanned == 3
        assert list(source.reporter.filtered_dashboards) == ["d-denied", "d-personal"]
        assert source.reporter.looks_scanned == 4
        assert source.reporter.resolved_user_ids > 0
        assert source.reporter.email_ids_missing == 1
        # The user-id -> email platform resource is always emitted.
        assert any("platformResource" in wu.get_urn() for wu in emitted.workunits)

    def test_used_explores_only_fetches_reachable_explores(self) -> None:
        source = make_source()
        emitted = run(source)
        assert emitted.urns(UpstreamLineageClass) == [
            "urn:li:dataset:(urn:li:dataPlatform:looker,finance.explore.invoices,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:looker,sales.explore.orders,PROD)",
        ]
        assert source.reporter.total_explores == 2

    def test_explore_input_fields_are_hydrated_from_explore_schema(self) -> None:
        emitted = run(make_source())
        chart_fields = emitted.aspects(InputFieldsClass)[
            "urn:li:chart:(looker,dashboard_elements.e-query)"
        ][0]
        paths = sorted(f.schemaFieldUrn for f in chart_fields.fields)
        assert (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:looker,sales.explore.orders,PROD),orders.id)"
            in paths
        )
        # Table calculations live on the chart itself, not on the explore.
        assert (
            "urn:li:schemaField:(urn:li:chart:(looker,dashboard_elements.e-query),calc)"
            in paths
        )

    def test_missing_owner_emails_everywhere_is_reported(self) -> None:
        client = fake_client()
        client.all_users.return_value = [User(id="u-1", email=None), User(id="u-2")]
        client.user.side_effect = lambda id_, fields=None, transport_options=None: User(
            id=id_, email=None
        )
        source = make_source(client)
        run(source)
        assert any(
            "Failed to extract owner emails" in (w.message or "")
            for w in source.reporter.warnings
        )

    def test_folder_path_pattern_drops_dashboards(self) -> None:
        source = make_source(folder_path_pattern={"deny": ["^Shared/Sales$"]})
        emitted = run(source)
        assert "urn:li:dashboard:(looker,dashboards.d-1)" not in emitted.urns(
            DashboardInfoClass
        )
        assert "d-1" in list(source.reporter.filtered_dashboards)

    def test_unfetchable_dashboard_is_skipped_with_warning(self) -> None:
        client = fake_client()
        client.all_dashboards.return_value = [DashboardBase(id="missing")]
        source = make_source(client)
        emitted = run(source)
        assert emitted.urns(DashboardInfoClass) == []
        assert any(
            w.title == "Failed to fetch dashboard from the Looker API"
            for w in source.reporter.warnings
        )

    def test_dashboard_without_title_is_rejected(self) -> None:
        source = make_source()
        with pytest.raises(ValueError):
            source._get_looker_dashboard(Dashboard(id="x", title=None))


class TestUsageStatistics:
    def test_usage_stats_emitted_for_dashboards_charts_and_explores(self) -> None:
        source = make_source(extract_usage_history=True)
        emitted = run(source)

        dashboard_usage = emitted.aspects(DashboardUsageStatisticsClass)[
            "urn:li:dashboard:(looker,dashboards.d-1)"
        ]
        absolute = [a for a in dashboard_usage if a.eventGranularity is None]
        daily = [a for a in dashboard_usage if a.eventGranularity is not None]
        assert absolute[0].viewsCount == 10
        assert absolute[0].favoritesCount == 3
        assert daily[0].executionsCount == 7
        assert daily[0].uniqueUserCount == 2
        assert daily[0].userCounts is not None
        assert daily[0].userCounts[0].user == "urn:li:corpuser:one@example.com"

        chart_usage = emitted.aspects(ChartUsageStatisticsClass)[
            "urn:li:chart:(looker,dashboard_elements.l-1)"
        ]
        assert [a.viewsCount for a in chart_usage] == [4, 3]
        assert chart_usage[1].userCounts is not None
        assert chart_usage[1].userCounts[0].viewsCount == 3

        explore_usage = emitted.aspects(DatasetUsageStatisticsClass)[
            "urn:li:dataset:(urn:li:dataPlatform:looker,sales.explore.orders,PROD)"
        ]
        assert len(explore_usage) == 1
        assert explore_usage[0].totalSqlQueries == 5
        assert explore_usage[0].uniqueUserCount == 1
        assert explore_usage[0].fieldCounts is not None
        assert {f.fieldPath: f.count for f in explore_usage[0].fieldCounts} == {
            "orders.id": 5,
            "orders.amount": 5,
        }
        assert "d-1" in source.reporter.dashboards_with_activity
        assert "l-1" in source.reporter.charts_with_activity

    def test_chart_usage_skipped_for_looks_not_ingested(self) -> None:
        client = fake_client()
        # The look is referenced by the dashboard but its element type is not a vis.
        client.dashboard.side_effect = None
        client.dashboard.return_value = Dashboard(
            id="d-1",
            title="Revenue",
            dashboard_elements=[
                DashboardElement(
                    id="e-look",
                    type="text",
                    look_id="l-1",
                    look=LookWithQuery(
                        id="l-1",
                        view_count=4,
                        query=Query(model="sales", view="orders"),
                    ),
                )
            ],
        )
        client.all_dashboards.return_value = [DashboardBase(id="d-1")]
        source = make_source(client, extract_usage_history=True)
        emitted = run(source)
        assert emitted.urns(ChartUsageStatisticsClass) == []
        assert "l-1" in source.reporter.charts_skipped_for_usage

    def test_usage_query_failure_leaves_absolute_stats_only(self) -> None:
        client = fake_client()
        client.run_inline_query.side_effect = SDKError("system activity disabled")
        source = make_source(client, extract_usage_history=True)
        emitted = run(source)
        dashboard_usage = emitted.aspects(DashboardUsageStatisticsClass)[
            "urn:li:dashboard:(looker,dashboards.d-1)"
        ]
        assert len(dashboard_usage) == 1
        assert dashboard_usage[0].eventGranularity is None


class TestIndependentLooks:
    def _client_with_looks(self) -> mock.MagicMock:
        client = fake_client()
        client.all_looks.return_value = [
            Look(id="l-1", title="On dashboard", query_id="q1", folder=SALES),
            Look(
                id="l-2", title="Standalone", query_id="q2", folder=SALES, user_id="u-1"
            ),
            Look(id="l-3", title="No query", query_id=None, folder=SALES),
            Look(id="l-4", title="Personal", query_id="q4", folder=PERSONAL),
            Look(id="l-5", title="Unreadable", query_id="q5", folder=SALES),
            Look(id=None, title="No id", query_id="q6"),
        ]

        def look(
            look_id: str, fields: Any = None, transport_options: Any = None
        ) -> LookWithQuery:
            if look_id == "l-5":
                raise SDKError("gone")
            return LookWithQuery(
                query=Query(
                    id=f"q-{look_id}",
                    model="sales",
                    view="orders",
                    fields=["orders.id"],
                    slug="slug",
                )
            )

        client.look.side_effect = look
        return client

    def test_only_standalone_looks_with_queries_are_emitted(self) -> None:
        source = make_source(
            self._client_with_looks(),
            extract_independent_looks=True,
            skip_personal_folders=True,
            stateful_ingestion={"enabled": True},
        )
        # l-1 is placed on the live dashboard, so it is not standalone.
        list(source.get_workunits_internal())
        emitted = Emitted(source.extract_independent_looks())

        assert emitted.urns(ChartInfoClass) == [
            "urn:li:chart:(looker,dashboard_elements.looks_l-2)"
        ]
        chart = emitted.aspects(ChartInfoClass)[
            "urn:li:chart:(looker,dashboard_elements.looks_l-2)"
        ][0]
        assert chart.chartUrl == f"{BASE_URL}/looks/l-2"
        assert chart.inputs == [
            "urn:li:dataset:(urn:li:dataPlatform:looker,sales.explore.orders,PROD)"
        ]
        assert "l-4" in list(source.reporter.filtered_looks)
        assert "l-5" not in list(source.reporter.filtered_looks)
        owners = emitted.aspects(OwnershipClass)[
            "urn:li:chart:(looker,dashboard_elements.looks_l-2)"
        ][0]
        assert [o.owner for o in owners.owners] == ["urn:li:corpuser:one@example.com"]

    def test_soft_deleted_looks_are_listed_when_include_deleted(self) -> None:
        client = self._client_with_looks()
        client.all_looks.return_value = []
        client.search_looks.return_value = [
            Look(id="l-9", title="Deleted", query_id="q9", folder=SALES)
        ]
        source = make_source(
            client,
            extract_independent_looks=True,
            include_deleted=True,
            stateful_ingestion={"enabled": True},
        )
        emitted = Emitted(source.extract_independent_looks())
        assert emitted.urns(ChartInfoClass) == [
            "urn:li:chart:(looker,dashboard_elements.looks_l-9)"
        ]
        client.search_looks.assert_called_once()


class TestFolders:
    def test_root_folder_container_has_no_ancestors(self) -> None:
        source = make_source()
        emitted = Emitted(
            source._get_folder_and_ancestors_containers(
                LookerFolder(id="f-sales", name="Sales", parent_id="f-shared")
            )
        )
        names = {
            urn: a[0].name
            for urn, a in emitted.aspects(ContainerPropertiesClass).items()
        }
        assert sorted(names.values()) == ["Sales", "Shared"]
        # Folders are emitted once even if requested again.
        assert (
            list(
                source._emit_folder_as_container(
                    LookerFolder(id="f-sales", name="Sales", parent_id="f-shared")
                )
            )
            == []
        )


class TestExploreLookup:
    def test_explore_metadata_reflects_api_fields_joins_and_tags(self) -> None:
        source = make_source()
        looker_explore = source.explore_registry.get_explore("sales", "orders")
        assert looker_explore is not None
        assert looker_explore.project_name == "proj"
        assert sorted(v.include for v in looker_explore.upstream_views or []) == [
            "customers",
            "items",
            "orders",
        ]
        assert looker_explore.fields is not None
        by_name = {f.name: f for f in looker_explore.fields}
        # dimension-group variant is stripped to the base column name
        created = by_name["orders.created_date"].upstream_fields[0]
        assert created.column == "created"
        assert by_name["customers.email"].group_label == "Contact"
        assert by_name["orders.amount"].upstream_fields[0].column == "amount"
        assert any(w.title == "Missing View Name" for w in source.reporter.warnings)
        # The registry caches by (model, explore).
        hits_before = source.explore_registry.compute_stats()["cache_info"].hits
        assert source.explore_registry.get_explore("sales", "orders") is looker_explore
        assert (
            source.explore_registry.compute_stats()["cache_info"].hits
            == hits_before + 1
        )

    def test_missing_explore_returns_none(self) -> None:
        client = fake_client()
        client.lookml_model_explore.side_effect = SDKError(
            "<title>Looker Not Found (404)</title>"
        )
        source = make_source(client)
        assert source.explore_registry.get_explore("sales", "gone") is None
        assert source.fetch_one_explore("sales", "gone")[0] is None

    def test_explore_without_project_name_is_reported(self) -> None:
        client = fake_client()
        client.lookml_model_explore.side_effect = (
            lambda model, explore_name, fields=None, transport_options=None: (
                LookmlModelExplore(
                    name=explore_name,
                    view_name=explore_name,
                    project_name=None,
                    fields=EXPLORE_FIELDS,
                )
            )
        )
        source = make_source(client)
        assert source.explore_registry.get_explore("sales", "orders") is None
        assert any(w.title == "Unable to find Views" for w in source.reporter.warnings)

    def test_explore_dataset_carries_lineage_schema_and_view_logic(self) -> None:
        source = make_source(extract_embed_urls=True)
        looker_explore = source.explore_registry.get_explore("sales", "orders")
        assert looker_explore is not None
        dataset = looker_explore._to_metadata_events(
            source.source_config, source.reporter, BASE_URL, extract_embed_urls=True
        )
        emitted = Emitted([dataset])
        urn = "urn:li:dataset:(urn:li:dataPlatform:looker,sales.explore.orders,PROD)"
        lineage = emitted.aspects(UpstreamLineageClass)[urn][0]
        assert sorted(u.dataset for u in lineage.upstreams) == [
            "urn:li:dataset:(urn:li:dataPlatform:looker,proj.view.customers,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:looker,proj.view.items,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:looker,proj.view.orders,PROD)",
        ]
        assert lineage.fineGrainedLineages is not None
        assert len(lineage.fineGrainedLineages) == 4
        assert emitted.aspects(EmbedClass)[urn][0].renderUrl == (
            f"{BASE_URL}/embed/explore/sales/orders"
        )
        view_props = [
            wu.metadata.aspect
            for wu in emitted.workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.aspectName == "viewProperties"
        ]
        assert len(view_props) == 1
        assert "buyers" in view_props[0].viewLogic  # type: ignore[union-attr]

    def test_explore_browse_path(self) -> None:
        source = make_source()
        looker_explore = LookerExplore(name="orders", model_name="sales")
        assert looker_explore.get_explore_browse_path(source.source_config) == (
            "/Explore/sales"
        )
