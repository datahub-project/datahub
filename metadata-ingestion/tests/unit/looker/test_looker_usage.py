import datetime
from typing import Optional
from unittest import mock

from looker_sdk.sdk.api40.models import Dashboard, DashboardElement, LookWithQuery

from datahub.ingestion.source.looker import looker_usage
from datahub.ingestion.source.looker.looker_common import (
    LookerCommonConfig,
    LookerDashboardSourceReport,
    LookerUser,
)
from datahub.ingestion.source.looker.looker_query_model import (
    HistoryViewField,
    LookerModel,
    LookViewField,
    QueryViewField,
    UserViewField,
)
from datahub.metadata.schema_classes import (
    ChartUsageStatisticsClass,
    DashboardUsageStatisticsClass,
    DatasetUsageStatisticsClass,
)


def _stat_config(
    api: Optional[mock.MagicMock] = None,
    user_registry: Optional[mock.MagicMock] = None,
) -> looker_usage.StatGeneratorConfig:
    return looker_usage.StatGeneratorConfig(
        looker_api_wrapper=api or mock.MagicMock(),
        looker_user_registry=user_registry or mock.MagicMock(),
        strip_user_ids_from_email=False,
        interval="2022-07-01 to 2022-07-08",
        max_threads=1,
    )


def test_explore_usage_queries_target_system_activity_query_view():
    per_day = looker_usage.query_collection[
        looker_usage.QueryId.EXPLORE_PER_DAY_USAGE_STAT
    ]
    per_user = looker_usage.query_collection[
        looker_usage.QueryId.EXPLORE_PER_USER_PER_DAY_USAGE_STAT
    ]

    for query in (per_day, per_user):
        assert query.model == LookerModel.SYSTEM_ACTIVITY
        # An explore is identified by (query.model, query.view) in System Activity.
        assert QueryViewField.QUERY_MODEL in query.fields
        assert QueryViewField.QUERY_VIEW in query.fields
        assert HistoryViewField.HISTORY_COUNT in query.fields

    # Only the per-user variant carries the user dimension.
    assert UserViewField.USER_ID not in per_day.fields
    assert UserViewField.USER_ID in per_user.fields

    # The per-field query is separate (adding query.fields to the per-day query
    # would fragment the (model, view, date) grouping) and carries query.fields.
    per_field = looker_usage.query_collection[
        looker_usage.QueryId.EXPLORE_PER_FIELD_PER_DAY_USAGE_STAT
    ]
    assert per_field.model == LookerModel.SYSTEM_ACTIVITY
    assert QueryViewField.QUERY_FIELDS in per_field.fields
    assert QueryViewField.QUERY_MODEL in per_field.fields
    assert QueryViewField.QUERY_VIEW in per_field.fields
    assert UserViewField.USER_ID not in per_field.fields
    assert QueryViewField.QUERY_FIELDS not in per_day.fields


def test_explore_stat_generator_builds_explore_dataset_urn():
    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(),
        report=LookerDashboardSourceReport(),
        source_config=LookerCommonConfig(),
        looker_explores=[
            looker_usage.LookerExploreForUsage(
                id=None, model_name="sales", name="orders"
            )
        ],
    )

    urn = generator._get_urn(
        looker_usage.LookerExploreForUsage(id=None, model_name="sales", name="orders")
    )
    assert urn.startswith("urn:li:dataset:(urn:li:dataPlatform:looker,")
    # Default explore_naming_pattern is "{model}.explore.{name}".
    assert "sales.explore.orders" in urn


def test_explore_stat_generator_emits_usage_stats():
    entity_rows = [
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            HistoryViewField.HISTORY_COUNT: 30,
        }
    ]
    # Two field-set buckets for the same (explore, day): orders.count appears in
    # both, so its per-field count sums across rows (18 + 12 = 30).
    field_rows = [
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            QueryViewField.QUERY_FIELDS: "orders.count,orders.created_date",
            HistoryViewField.HISTORY_COUNT: 18,
        },
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            QueryViewField.QUERY_FIELDS: "orders.count",
            HistoryViewField.HISTORY_COUNT: 12,
        },
    ]
    user_rows = [
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            UserViewField.USER_ID: 1,
            HistoryViewField.HISTORY_COUNT: 20,
        },
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            UserViewField.USER_ID: 2,
            HistoryViewField.HISTORY_COUNT: 10,
        },
    ]

    mock_api = mock.MagicMock()
    # Execution order: entity query, then the per-field query (from the augment
    # hook), then the per-user query.
    mock_api.execute_query.side_effect = [entity_rows, field_rows, user_rows]

    def fake_user(user_id: int) -> mock.MagicMock:
        user = mock.MagicMock()
        user.get_urn.return_value = f"urn:li:corpuser:user{user_id}"
        user.email = f"user{user_id}@example.com"
        return user

    mock_registry = mock.MagicMock()
    mock_registry.get_by_id.side_effect = fake_user

    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(api=mock_api, user_registry=mock_registry),
        report=LookerDashboardSourceReport(),
        source_config=LookerCommonConfig(),
        looker_explores=[
            looker_usage.LookerExploreForUsage(
                id=None, model_name="sales", name="orders"
            )
        ],
    )

    mcps = list(generator.generate_usage_stat_mcps())

    assert len(mcps) == 1
    aspect = mcps[0].aspect
    assert isinstance(aspect, DatasetUsageStatisticsClass)
    assert aspect.totalSqlQueries == 30
    assert aspect.uniqueUserCount == 2
    assert aspect.userCounts is not None
    assert {uc.count for uc in aspect.userCounts} == {20, 10}
    entity_urn = mcps[0].entityUrn
    assert entity_urn is not None and "sales.explore.orders" in entity_urn

    # query.fields is exploded and summed into per-field usage counts.
    assert aspect.fieldCounts is not None
    field_counts = {fc.fieldPath: fc.count for fc in aspect.fieldCounts}
    assert field_counts == {"orders.count": 30, "orders.created_date": 18}


def test_parse_query_fields_handles_delimiters_and_blanks():
    parse = looker_usage.ExploreStatGenerator._parse_query_fields
    assert parse("orders.count,orders.created_date") == [
        "orders.count",
        "orders.created_date",
    ]
    assert parse("orders.count\norders.created_date") == [
        "orders.count",
        "orders.created_date",
    ]
    assert parse(" orders.count , ,\n orders.state ") == [
        "orders.count",
        "orders.state",
    ]
    assert parse("") == []
    # System Activity serialises the Query model's fields (Sequence[str])
    # into a JSON array string when returned as a dimension value.
    assert parse('["orders.count","orders.created_date"]') == [
        "orders.count",
        "orders.created_date",
    ]


def test_explore_stat_key_round_trips_between_model_and_row():
    # The generator keys ingested explores by get_id() and matches System
    # Activity rows back by get_id_from_row(). If these diverge, the >100-explore
    # post_filter path silently drops every row, so lock the contract here.
    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(),
        report=LookerDashboardSourceReport(),
        source_config=LookerCommonConfig(),
        looker_explores=[
            looker_usage.LookerExploreForUsage(
                id=None, model_name="sales", name="orders"
            )
        ],
    )

    explore = looker_usage.LookerExploreForUsage(
        id=None, model_name="sales", name="orders"
    )
    row = {
        QueryViewField.QUERY_MODEL: "sales",
        QueryViewField.QUERY_VIEW: "orders",
    }
    assert generator.get_id(explore) == generator.get_id_from_row(row)


def test_explore_stat_generator_skips_unresolved_users():
    entity_rows = [
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            HistoryViewField.HISTORY_COUNT: 30,
        }
    ]
    user_rows = [
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            UserViewField.USER_ID: 1,
            HistoryViewField.HISTORY_COUNT: 20,
        },
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            UserViewField.USER_ID: 2,
            HistoryViewField.HISTORY_COUNT: 10,
        },
    ]

    mock_api = mock.MagicMock()
    mock_api.execute_query.side_effect = [entity_rows, [], user_rows]

    def fake_user(user_id: int) -> mock.MagicMock:
        user = mock.MagicMock()
        # User 2 has no resolvable urn (e.g. a deleted Looker user).
        user.get_urn.return_value = (
            f"urn:li:corpuser:user{user_id}" if user_id == 1 else None
        )
        user.email = f"user{user_id}@example.com"
        return user

    mock_registry = mock.MagicMock()
    mock_registry.get_by_id.side_effect = fake_user

    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(api=mock_api, user_registry=mock_registry),
        report=LookerDashboardSourceReport(),
        source_config=LookerCommonConfig(),
        looker_explores=[
            looker_usage.LookerExploreForUsage(
                id=None, model_name="sales", name="orders"
            )
        ],
    )

    mcps = list(generator.generate_usage_stat_mcps())

    assert len(mcps) == 1
    aspect = mcps[0].aspect
    assert isinstance(aspect, DatasetUsageStatisticsClass)
    # Entity-level totals are unaffected by the unresolved user.
    assert aspect.totalSqlQueries == 30
    # Only the resolvable user contributes to per-user counts.
    assert aspect.uniqueUserCount == 1
    assert aspect.userCounts is not None
    assert {uc.count for uc in aspect.userCounts} == {20}


def test_explore_stat_generator_reports_non_ingested_explore_as_skipped():
    # System Activity returns usage for an explore we never ingested; it should
    # not be emitted and should be recorded in the skip set for observability.
    entity_rows = [
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            HistoryViewField.HISTORY_COUNT: 30,
        },
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "returns",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            HistoryViewField.HISTORY_COUNT: 5,
        },
    ]

    mock_api = mock.MagicMock()
    mock_api.execute_query.side_effect = [entity_rows, [], []]

    report = LookerDashboardSourceReport()
    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(api=mock_api),
        report=report,
        source_config=LookerCommonConfig(),
        looker_explores=[
            looker_usage.LookerExploreForUsage(
                id=None, model_name="sales", name="orders"
            )
        ],
    )

    mcps = list(generator.generate_usage_stat_mcps())

    emitted_urns = [mcp.entityUrn or "" for mcp in mcps]
    assert any("sales.explore.orders" in urn for urn in emitted_urns)
    assert not any("sales.explore.returns" in urn for urn in emitted_urns)
    assert "sales::returns" in report.explores_skipped_for_usage


def test_dashboard_stats_are_post_filtered_when_too_many_dashboards():
    # With more than 100 dashboards the id filter is not sent to Looker (the
    # filter string would be too long); rows are filtered locally instead.
    dashboards = [
        looker_usage.LookerDashboardForUsage(
            id=str(i), view_count=1, favorite_count=0, last_viewed_at=None
        )
        for i in range(101)
    ]
    entity_rows = [
        {
            HistoryViewField.HISTORY_DASHBOARD_ID: dashboard_id,
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            HistoryViewField.HISTORY_DASHBOARD_USER: 1,
            HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT: 2,
        }
        for dashboard_id in ("7", "not-ingested")
    ]
    mock_api = mock.MagicMock()
    mock_api.execute_query.side_effect = [entity_rows, []]

    generator = looker_usage.create_dashboard_stat_generator(
        config=_stat_config(api=mock_api),
        report=LookerDashboardSourceReport(),
        urn_builder=lambda name: f"urn:li:dashboard:(looker,{name})",
        looker_dashboards=dashboards,
    )
    mcps = list(generator.generate_usage_stat_mcps())

    assert generator.post_filter
    sent_query = mock_api.execute_query.call_args_list[0].kwargs["write_query"]
    assert (sent_query.filters or {}).get(
        HistoryViewField.HISTORY_DASHBOARD_ID
    ) != ",".join(d.id for d in dashboards if d.id)
    daily = [
        mcp
        for mcp in mcps
        if isinstance(mcp.aspect, DashboardUsageStatisticsClass)
        and mcp.aspect.eventGranularity is not None
    ]
    assert [mcp.entityUrn for mcp in daily] == [
        "urn:li:dashboard:(looker,dashboards.7)"
    ]


def test_user_rows_for_unknown_entities_or_days_are_skipped():
    entity_rows = [
        {
            LookViewField.LOOK_ID: 1,
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            HistoryViewField.HISTORY_COUNT: 3,
        }
    ]
    user_rows = [
        # Look that was never registered with the generator.
        {
            LookViewField.LOOK_ID: 99,
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            UserViewField.USER_ID: 1,
            HistoryViewField.HISTORY_COUNT: 1,
        },
        # Known look, but a day with no entity-level row.
        {
            LookViewField.LOOK_ID: 1,
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-06",
            UserViewField.USER_ID: 1,
            HistoryViewField.HISTORY_COUNT: 1,
        },
        # Known look and day, but an unresolvable user.
        {
            LookViewField.LOOK_ID: 1,
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            UserViewField.USER_ID: 2,
            HistoryViewField.HISTORY_COUNT: 1,
        },
        # Known look and day, user without an email.
        {
            LookViewField.LOOK_ID: 1,
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            UserViewField.USER_ID: 3,
            HistoryViewField.HISTORY_COUNT: 1,
        },
    ]
    mock_api = mock.MagicMock()
    mock_api.execute_query.side_effect = [entity_rows, user_rows]
    mock_registry = mock.MagicMock()
    users = {
        1: LookerUser("1", "one@example.com", None, None, None),
        2: None,
        3: LookerUser("3", None, None, None, None),
    }
    mock_registry.get_by_id.side_effect = lambda user_id: users[user_id]

    generator = looker_usage.create_chart_stat_generator(
        config=_stat_config(api=mock_api, user_registry=mock_registry),
        report=LookerDashboardSourceReport(),
        urn_builder=lambda name: f"urn:li:chart:(looker,{name})",
        looker_looks=[looker_usage.LookerChartForUsage(id="1", view_count=0)],
    )
    mcps = list(generator.generate_usage_stat_mcps())

    daily = [
        mcp.aspect
        for mcp in mcps
        if isinstance(mcp.aspect, ChartUsageStatisticsClass)
        and mcp.aspect.eventGranularity is not None
    ]
    assert len(daily) == 1
    assert daily[0].viewsCount == 3
    assert daily[0].userCounts == []


def test_usage_models_built_from_api_dashboard():
    dashboard = Dashboard(
        id="d",
        view_count=5,
        favorite_count=1,
        last_viewed_at=datetime.datetime(2022, 7, 1, tzinfo=datetime.timezone.utc),
        dashboard_elements=[
            DashboardElement(id="e1", look=LookWithQuery(id="l1", view_count=2)),
            DashboardElement(id="e2"),
        ],
    )
    usage = looker_usage.LookerDashboardForUsage.from_dashboard(dashboard)
    assert usage.id == "d"
    assert usage.last_viewed_at == 1656633600000
    assert [(look.id, look.view_count) for look in usage.looks] == [("l1", 2)]
