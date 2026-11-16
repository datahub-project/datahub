"""Usage statistics generators over Looker System Activity rows.

The ``LookerAPI`` wrapper and the user registry are specced mocks; the row
aggregation, user resolution and aspect building run for real."""

import datetime
from typing import Dict, List, Optional
from unittest import mock

from looker_sdk.sdk.api40.models import Dashboard, DashboardElement, LookWithQuery

from datahub.ingestion.source.looker import looker_usage
from datahub.ingestion.source.looker.looker_common import (
    LookerCommonConfig,
    LookerDashboardSourceReport,
    LookerUser,
    LookerUserRegistry,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
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

ORDERS_EXPLORE = looker_usage.LookerExploreForUsage(
    id=None, model_name="sales", name="orders"
)


def _stat_config(
    api: Optional[mock.MagicMock] = None,
    user_registry: Optional[mock.MagicMock] = None,
) -> looker_usage.StatGeneratorConfig:
    return looker_usage.StatGeneratorConfig(
        looker_api_wrapper=api or mock.MagicMock(spec=LookerAPI),
        looker_user_registry=user_registry or mock.MagicMock(spec=LookerUserRegistry),
        strip_user_ids_from_email=False,
        interval="2022-07-01 to 2022-07-08",
        max_threads=1,
    )


def _registry(users: Dict[int, Optional[LookerUser]]) -> mock.MagicMock:
    registry = mock.MagicMock(spec=LookerUserRegistry)
    registry.get_by_id.side_effect = lambda user_id: users[user_id]
    return registry


def _user(user_id: int, email: Optional[str] = None) -> LookerUser:
    return LookerUser(str(user_id), email, None, None, None)


def _explore_rows(count: int, **extra: object) -> List[Dict]:
    return [
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "orders",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            HistoryViewField.HISTORY_COUNT: count,
            **extra,
        }
    ]


def _dashboard(dashboard_id: str) -> looker_usage.LookerDashboardForUsage:
    return looker_usage.LookerDashboardForUsage(
        id=dashboard_id, view_count=1, favorite_count=0, last_viewed_at=None
    )


def test_explore_usage_queries_target_system_activity_query_view() -> None:
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


def test_explore_stat_generator_builds_explore_dataset_urn() -> None:
    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(),
        report=LookerDashboardSourceReport(),
        source_config=LookerCommonConfig(),
        looker_explores=[ORDERS_EXPLORE],
    )

    urn = generator._get_urn(ORDERS_EXPLORE)
    assert urn.startswith("urn:li:dataset:(urn:li:dataPlatform:looker,")
    # Default explore_naming_pattern is "{model}.explore.{name}".
    assert "sales.explore.orders" in urn


def test_explore_stat_generator_emits_usage_stats() -> None:
    """Entity rows give the totals, per-field rows are exploded and summed per
    field, per-user rows give the user counts; all land on one aspect."""
    entity_rows = _explore_rows(30)
    # Two field-set buckets for the same (explore, day): orders.count appears in
    # both, so its per-field count sums across rows (18 + 12 = 30).
    field_rows = [
        *_explore_rows(
            18, **{QueryViewField.QUERY_FIELDS: "orders.count,orders.created_date"}
        ),
        *_explore_rows(12, **{QueryViewField.QUERY_FIELDS: "orders.count"}),
    ]
    user_rows = [
        *_explore_rows(20, **{UserViewField.USER_ID: 1}),
        *_explore_rows(10, **{UserViewField.USER_ID: 2}),
    ]

    mock_api = mock.MagicMock(spec=LookerAPI)
    # Execution order: entity query, then the per-field query (from the augment
    # hook), then the per-user query.
    mock_api.execute_query.side_effect = [entity_rows, field_rows, user_rows]
    registry = _registry(
        {1: _user(1, "user1@example.com"), 2: _user(2, "user2@example.com")}
    )

    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(api=mock_api, user_registry=registry),
        report=LookerDashboardSourceReport(),
        source_config=LookerCommonConfig(),
        looker_explores=[ORDERS_EXPLORE],
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


def test_parse_query_fields_handles_delimiters_and_blanks() -> None:
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


def test_explore_stat_key_round_trips_between_model_and_row() -> None:
    """The generator keys ingested explores by get_id() and matches System
    Activity rows back by get_id_from_row(). If these diverge, the >100-explore
    post_filter path silently drops every row."""
    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(),
        report=LookerDashboardSourceReport(),
        source_config=LookerCommonConfig(),
        looker_explores=[ORDERS_EXPLORE],
    )

    row = {
        QueryViewField.QUERY_MODEL: "sales",
        QueryViewField.QUERY_VIEW: "orders",
    }
    assert generator.get_id(ORDERS_EXPLORE) == generator.get_id_from_row(row)


def test_explore_stat_generator_skips_unresolved_users() -> None:
    """A user without a resolvable URN (e.g. deleted in Looker, no email) is
    left out of the per-user counts; the entity totals are unaffected."""
    user_rows = [
        *_explore_rows(20, **{UserViewField.USER_ID: 1}),
        *_explore_rows(10, **{UserViewField.USER_ID: 2}),
    ]
    mock_api = mock.MagicMock(spec=LookerAPI)
    mock_api.execute_query.side_effect = [_explore_rows(30), [], user_rows]
    registry = _registry({1: _user(1, "user1@example.com"), 2: _user(2)})

    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(api=mock_api, user_registry=registry),
        report=LookerDashboardSourceReport(),
        source_config=LookerCommonConfig(),
        looker_explores=[ORDERS_EXPLORE],
    )

    mcps = list(generator.generate_usage_stat_mcps())

    assert len(mcps) == 1
    aspect = mcps[0].aspect
    assert isinstance(aspect, DatasetUsageStatisticsClass)
    assert aspect.totalSqlQueries == 30
    assert aspect.uniqueUserCount == 1
    assert aspect.userCounts is not None
    assert {uc.count for uc in aspect.userCounts} == {20}


def test_explore_stat_generator_reports_non_ingested_explore_as_skipped() -> None:
    # System Activity returns usage for an explore we never ingested; it should
    # not be emitted and should be recorded in the skip set for observability.
    entity_rows = [
        *_explore_rows(30),
        {
            QueryViewField.QUERY_MODEL: "sales",
            QueryViewField.QUERY_VIEW: "returns",
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            HistoryViewField.HISTORY_COUNT: 5,
        },
    ]
    mock_api = mock.MagicMock(spec=LookerAPI)
    mock_api.execute_query.side_effect = [entity_rows, [], []]

    report = LookerDashboardSourceReport()
    generator = looker_usage.create_explore_stat_generator(
        config=_stat_config(api=mock_api),
        report=report,
        source_config=LookerCommonConfig(),
        looker_explores=[ORDERS_EXPLORE],
    )

    mcps = list(generator.generate_usage_stat_mcps())

    emitted_urns = [mcp.entityUrn or "" for mcp in mcps]
    assert any("sales.explore.orders" in urn for urn in emitted_urns)
    assert not any("sales.explore.returns" in urn for urn in emitted_urns)
    assert "sales::returns" in report.explores_skipped_for_usage


def test_dashboard_stats_are_post_filtered_when_too_many_dashboards() -> None:
    """With more than 100 dashboards the id filter is not sent to Looker (the
    filter string would be too long); rows are filtered locally instead."""
    dashboards = [_dashboard(str(i)) for i in range(101)]
    entity_rows = [
        {
            HistoryViewField.HISTORY_DASHBOARD_ID: dashboard_id,
            HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
            HistoryViewField.HISTORY_DASHBOARD_USER: 1,
            HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT: 2,
        }
        for dashboard_id in ("7", "not-ingested")
    ]
    mock_api = mock.MagicMock(spec=LookerAPI)
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
    assert HistoryViewField.HISTORY_DASHBOARD_ID.value not in (sent_query.filters or {})
    daily = [
        mcp
        for mcp in mcps
        if isinstance(mcp.aspect, DashboardUsageStatisticsClass)
        and mcp.aspect.eventGranularity is not None
    ]
    assert [mcp.entityUrn for mcp in daily] == [
        "urn:li:dashboard:(looker,dashboards.7)"
    ]


def test_filters_do_not_leak_between_generators() -> None:
    """The query definitions in ``query_collection`` are process-wide, so the
    id filter of one run must not be appended to them: a later run that relies
    on post-filtering would otherwise send the previous run's id filter."""
    per_day = looker_usage.query_collection[
        looker_usage.QueryId.DASHBOARD_PER_DAY_USAGE_STAT
    ]
    original_filters = dict(per_day.filters)
    mock_api = mock.MagicMock(spec=LookerAPI)
    mock_api.execute_query.return_value = []

    def run(dashboards: List[looker_usage.LookerDashboardForUsage]) -> None:
        generator = looker_usage.create_dashboard_stat_generator(
            config=_stat_config(api=mock_api),
            report=LookerDashboardSourceReport(),
            urn_builder=lambda name: f"urn:li:dashboard:(looker,{name})",
            looker_dashboards=dashboards,
        )
        list(generator.generate_usage_stat_mcps())

    run([_dashboard("7")])
    first = mock_api.execute_query.call_args_list[0].kwargs["write_query"]
    assert first.filters[HistoryViewField.HISTORY_DASHBOARD_ID.value] == "7"
    assert per_day.filters == original_filters

    run([_dashboard(str(i)) for i in range(101)])
    later = mock_api.execute_query.call_args_list[2].kwargs["write_query"]
    assert HistoryViewField.HISTORY_DASHBOARD_ID.value not in (later.filters or {})


def test_user_rows_for_unknown_entities_or_days_are_skipped() -> None:
    """Per-user rows only count when they match an entity and a day seen in
    the entity query and the user resolves to a URN."""
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
    mock_api = mock.MagicMock(spec=LookerAPI)
    mock_api.execute_query.side_effect = [entity_rows, user_rows]
    registry = _registry({1: _user(1, "one@example.com"), 2: None, 3: _user(3)})

    generator = looker_usage.create_chart_stat_generator(
        config=_stat_config(api=mock_api, user_registry=registry),
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


def test_usage_models_built_from_api_dashboard() -> None:
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
