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


def _stat_config() -> looker_usage.StatGeneratorConfig:
    return looker_usage.StatGeneratorConfig(
        looker_api_wrapper=mock.MagicMock(),
        looker_user_registry=mock.MagicMock(),
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

    config = _stat_config()
    # entity query is executed first, then the per-user query.
    config.looker_api_wrapper.execute_query.side_effect = [entity_rows, user_rows]

    def fake_user(user_id: int):
        user = mock.MagicMock()
        user.get_urn.return_value = f"urn:li:corpuser:user{user_id}"
        user.email = f"user{user_id}@example.com"
        return user

    config.looker_user_registry.get_by_id.side_effect = fake_user

    generator = looker_usage.create_explore_stat_generator(
        config=config,
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
    assert "sales.explore.orders" in mcps[0].entityUrn


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

    config = _stat_config()
    config.looker_api_wrapper.execute_query.side_effect = [entity_rows, user_rows]

    def fake_user(user_id: int):
        user = mock.MagicMock()
        # User 2 has no resolvable urn (e.g. a deleted Looker user).
        user.get_urn.return_value = (
            f"urn:li:corpuser:user{user_id}" if user_id == 1 else None
        )
        user.email = f"user{user_id}@example.com"
        return user

    config.looker_user_registry.get_by_id.side_effect = fake_user

    generator = looker_usage.create_explore_stat_generator(
        config=config,
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

    config = _stat_config()
    config.looker_api_wrapper.execute_query.side_effect = [entity_rows, []]

    report = LookerDashboardSourceReport()
    generator = looker_usage.create_explore_stat_generator(
        config=config,
        report=report,
        source_config=LookerCommonConfig(),
        looker_explores=[
            looker_usage.LookerExploreForUsage(
                id=None, model_name="sales", name="orders"
            )
        ],
    )

    mcps = list(generator.generate_usage_stat_mcps())

    emitted_urns = [mcp.entityUrn for mcp in mcps]
    assert any("sales.explore.orders" in urn for urn in emitted_urns)
    assert not any("sales.explore.returns" in urn for urn in emitted_urns)
    assert "sales::returns" in report.explores_skipped_for_usage
