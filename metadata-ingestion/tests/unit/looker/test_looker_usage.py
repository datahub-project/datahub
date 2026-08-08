from typing import Optional
from unittest import mock

from datahub.ingestion.source.looker import looker_usage
from datahub.ingestion.source.looker.looker_common import (
    LookerCommonConfig,
    LookerDashboardSourceReport,
)
from datahub.ingestion.source.looker.looker_query_model import (
    HistoryViewField,
    LookerModel,
    QueryViewField,
    UserViewField,
)
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass


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
