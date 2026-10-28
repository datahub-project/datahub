import json
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence, Union
from unittest.mock import MagicMock

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.constants import SnowflakeEdition
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeUsageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    DatasetUsageStatisticsClass,
    OperationClass,
    OperationTypeClass,
)

START = datetime(2022, 6, 6, tzinfo=timezone.utc)
END = datetime(2022, 6, 8, tzinfo=timezone.utc)
TABLE_1 = "test_db.test_schema.table_1"
TABLE_2 = "test_db.test_schema.table_2"
TABLE_3 = "test_db.test_schema.table_3"

QueryResult = Union[Sequence[dict], Exception]
QueryHandler = Callable[[str], QueryResult]


def _as_mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


class _Cursor(list):
    rownumber: Optional[int] = None

    @property
    def rowcount(self) -> int:
        return len(self)


def _is_usage_query(query: str) -> bool:
    return "BUCKET_START_TIME" in query


def _is_operations_query(query: str) -> bool:
    return "OBJECTS_MODIFIED" in query


def _connection(handler: QueryHandler) -> MagicMock:
    connection = MagicMock()

    def query(sql: str) -> _Cursor:
        result = handler(sql)
        if isinstance(result, Exception):
            raise result
        return _Cursor(result)

    connection.query.side_effect = query
    return connection


def _date_range_row() -> dict:
    return {"MIN_TIME": START, "MAX_TIME": END}


def _usage_row(object_name: str, **overrides: object) -> dict:
    row = {
        "OBJECT_NAME": object_name,
        "OBJECT_DOMAIN": "Table",
        "BUCKET_START_TIME": START,
        "TOTAL_QUERIES": 7,
        "TOTAL_USERS": 2,
        "TOP_SQL_QUERIES": json.dumps(
            ["select b from table_1", "select a from table_1"]
        ),
        "FIELD_COUNTS": json.dumps(
            [{"col": "COL_B", "total": 3}, {"col": "COL_A", "total": 5}]
        ),
        "USER_COUNTS": json.dumps(
            [
                {"email": "Zed@Example.com", "user_name": "ZED", "total": 4},
                {"email": "amy@example.com", "user_name": "AMY", "total": 3},
            ]
        ),
    }
    row.update(overrides)
    return row


def _operation_row(query_type: str, objects_modified: List[dict]) -> dict:
    return {
        "QUERY_START_TIME": datetime(2022, 6, 7, 4, 0, tzinfo=timezone.utc),
        "QUERY_TEXT": f"{query_type} table_1",
        "QUERY_TYPE": query_type,
        "ROWS_INSERTED": 0,
        "ROWS_UPDATED": 0,
        "ROWS_DELETED": 0,
        "BASE_OBJECTS_ACCESSED": "[]",
        "DIRECT_OBJECTS_ACCESSED": "[]",
        "OBJECTS_MODIFIED": json.dumps(objects_modified),
        "USER_NAME": "AMY",
        "FIRST_NAME": None,
        "LAST_NAME": None,
        "DISPLAY_NAME": None,
        "EMAIL": "amy@example.com",
        "ROLE_NAME": "ACCOUNTADMIN",
    }


def _table_object(qualified_name: str, domain: str = "Table") -> dict:
    return {"objectDomain": domain, "objectName": qualified_name, "objectId": 1}


def _make_extractor(
    handler: QueryHandler,
    *,
    edition: SnowflakeEdition = SnowflakeEdition.ENTERPRISE,
    skip_handler: Optional[MagicMock] = None,
    **config_overrides: object,
) -> SnowflakeUsageExtractor:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "user",
            "password": "pass",
            "start_time": START,
            "end_time": END,
            "format_sql_queries": False,
            # Pinned so the column-case sweep (DATAHUB_TEST_PRESERVE_COLUMN_CASE=1 in
            # CI, see conftest.py) does not change the lowercased field paths asserted here.
            "preserve_column_case": False,
            **config_overrides,
        }
    )
    report = SnowflakeV2Report()
    report.edition = edition
    return SnowflakeUsageExtractor(
        config=config,
        report=report,
        connection=_connection(handler),
        filter=SnowflakeFilter(filter_config=config, structured_reporter=report),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        redundant_run_skip_handler=skip_handler,
    )


def _dispatch(
    usage: QueryResult = (),
    operations: QueryResult = (),
    date_range: QueryResult = (_date_range_row(),),
) -> QueryHandler:
    def handler(query: str) -> QueryResult:
        if query == SnowflakeQuery.get_access_history_date_range():
            return date_range if isinstance(date_range, Exception) else list(date_range)
        if _is_usage_query(query):
            return usage if isinstance(usage, Exception) else list(usage)
        if _is_operations_query(query):
            return operations if isinstance(operations, Exception) else list(operations)
        raise AssertionError(f"Unexpected query: {query}")

    return handler


def _usage_by_urn(
    workunits: List[MetadataWorkUnit],
) -> Dict[str, List[DatasetUsageStatisticsClass]]:
    result: Dict[str, List[DatasetUsageStatisticsClass]] = {}
    for wu in workunits:
        aspect = wu.get_aspect_of_type(DatasetUsageStatisticsClass)
        if aspect is not None:
            result.setdefault(wu.get_urn(), []).append(aspect)
    return result


def _operations(workunits: List[MetadataWorkUnit]) -> Dict[str, OperationClass]:
    result: Dict[str, OperationClass] = {}
    for wu in workunits:
        aspect = wu.get_aspect_of_type(OperationClass)
        if aspect is not None:
            result[wu.get_urn()] = aspect
    return result


def test_access_history_row_is_aggregated_into_usage_statistics() -> None:
    extractor = _make_extractor(
        _dispatch(usage=[_usage_row("TEST_DB.TEST_SCHEMA.TABLE_1")])
    )

    usage = _usage_by_urn(list(extractor.get_usage_workunits([TABLE_1, TABLE_2])))

    table_1_urn = extractor.identifiers.gen_dataset_urn(TABLE_1)
    stats = usage[table_1_urn][0]
    assert stats.totalSqlQueries == 7
    assert stats.uniqueUserCount == 2
    assert stats.timestampMillis == int(START.timestamp() * 1000)
    # Column and user entries are sorted so repeated runs emit identical aspects.
    assert [(f.fieldPath, f.count) for f in stats.fieldCounts or []] == [
        ("col_a", 5),
        ("col_b", 3),
    ]
    assert [(u.user, u.count, u.userEmail) for u in stats.userCounts or []] == [
        ("urn:li:corpuser:amy@example.com", 3, "amy@example.com"),
        ("urn:li:corpuser:zed@example.com", 4, "Zed@Example.com"),
    ]
    assert stats.topSqlQueries == ["select a from table_1", "select b from table_1"]

    # A discovered dataset with no access-history rows still gets an empty
    # aspect so stale usage from a previous window is overwritten.
    table_2_stats = usage[extractor.identifiers.gen_dataset_urn(TABLE_2)]
    assert all(s.totalSqlQueries == 0 for s in table_2_stats)


def test_usage_rows_for_undiscovered_or_denied_datasets_are_skipped() -> None:
    extractor = _make_extractor(
        _dispatch(
            usage=[
                _usage_row("TEST_DB.TEST_SCHEMA.TABLE_1"),
                _usage_row("TEST_DB.TEST_SCHEMA.TABLE_2"),
                _usage_row("TEST_DB.TEST_SCHEMA.TABLE_3"),
            ]
        ),
        table_pattern={"deny": [".*\\.table_3"]},
    )

    usage = _usage_by_urn(list(extractor._get_workunits_internal([TABLE_1, TABLE_3])))

    assert set(usage) == {extractor.identifiers.gen_dataset_urn(TABLE_1)}


def test_user_counts_fall_back_to_email_domain_and_honour_email_filter() -> None:
    user_counts = json.dumps(
        [
            {"email": None, "user_name": "NO_EMAIL", "total": 1},
            {"email": "bob@blocked.com", "user_name": "BOB", "total": 2},
            {"email": "amy@example.com", "user_name": "AMY", "total": 3},
        ]
    )
    row = _usage_row("TEST_DB.TEST_SCHEMA.TABLE_1", USER_COUNTS=user_counts)

    with_domain = _make_extractor(
        _dispatch(),
        email_domain="example.com",
        user_email_pattern={"deny": [".*@blocked\\.com"]},
    )
    wu = with_domain.build_usage_statistics_for_dataset(TABLE_1, row)
    assert wu is not None
    stats = wu.get_aspect_of_type(DatasetUsageStatisticsClass)
    assert stats is not None
    assert [(u.user, u.userEmail) for u in stats.userCounts or []] == [
        ("urn:li:corpuser:amy@example.com", "amy@example.com"),
        ("urn:li:corpuser:no_email@example.com", "no_email@example.com"),
    ]

    # Without a configured domain there is no email to attribute, so the user
    # is dropped rather than emitted with a bogus urn.
    without_domain = _make_extractor(_dispatch())
    wu = without_domain.build_usage_statistics_for_dataset(TABLE_1, row)
    assert wu is not None
    stats = wu.get_aspect_of_type(DatasetUsageStatisticsClass)
    assert stats is not None
    assert [u.user for u in stats.userCounts or []] == [
        "urn:li:corpuser:amy@example.com",
        "urn:li:corpuser:bob@blocked.com",
    ]


def test_top_queries_share_the_character_budget_or_are_omitted() -> None:
    long_query = "select " + ", ".join(f"col_{i}" for i in range(40))
    row = _usage_row(
        "TEST_DB.TEST_SCHEMA.TABLE_1", TOP_SQL_QUERIES=json.dumps([long_query])
    )

    budgeted = _make_extractor(
        _dispatch(), top_n_queries=2, queries_character_limit=100
    )
    wu = budgeted.build_usage_statistics_for_dataset(TABLE_1, row)
    assert wu is not None
    stats = wu.get_aspect_of_type(DatasetUsageStatisticsClass)
    assert stats is not None and stats.topSqlQueries is not None
    assert len(stats.topSqlQueries) == 1
    assert len(stats.topSqlQueries[0]) == 50
    assert stats.topSqlQueries[0].endswith(" ...")

    disabled = _make_extractor(_dispatch(), include_top_n_queries=False)
    wu = disabled.build_usage_statistics_for_dataset(TABLE_1, row)
    assert wu is not None
    stats = wu.get_aspect_of_type(DatasetUsageStatisticsClass)
    assert stats is not None
    assert stats.topSqlQueries is None


def test_malformed_usage_row_is_reported_and_skipped() -> None:
    extractor = _make_extractor(_dispatch())
    row = _usage_row("TEST_DB.TEST_SCHEMA.TABLE_1", USER_COUNTS="not json")

    assert extractor.build_usage_statistics_for_dataset(TABLE_1, row) is None
    assert len(extractor.report.warnings) == 1


def test_standard_edition_skips_usage_without_querying() -> None:
    extractor = _make_extractor(_dispatch(), edition=SnowflakeEdition.STANDARD)

    assert list(extractor.get_usage_workunits([TABLE_1])) == []
    _as_mock(extractor.connection).query.assert_not_called()


def test_permission_error_on_date_range_aborts_usage_extraction() -> None:
    extractor = _make_extractor(
        _dispatch(date_range=SnowflakePermissionError("not authorized"))
    )

    assert list(extractor.get_usage_workunits([TABLE_1])) == []
    # Not stateful, so the missing grant is a failure rather than a warning,
    # and neither the usage nor the operations query is attempted.
    assert len(extractor.report.failures) == 1
    assert _as_mock(extractor.connection).query.call_count == 1


def test_empty_access_history_range_skips_usage_aggregation() -> None:
    extractor = _make_extractor(
        _dispatch(date_range=[{"MIN_TIME": None, "MAX_TIME": None}])
    )

    assert list(extractor.get_usage_workunits([TABLE_1])) == []
    assert extractor.report.min_access_history_time is None
    assert len(extractor.report.warnings) == 1
    assert _as_mock(extractor.connection).query.call_count == 1


def test_usage_query_failure_still_lets_operations_run() -> None:
    extractor = _make_extractor(
        _dispatch(
            usage=RuntimeError("boom"),
            operations=[
                _operation_row("INSERT", [_table_object("TEST_DB.TEST_SCHEMA.TABLE_1")])
            ],
        )
    )

    workunits = list(extractor.get_usage_workunits([TABLE_1]))

    assert len(extractor.report.failures) == 1
    operations = _operations(workunits)
    assert list(operations) == [extractor.identifiers.gen_dataset_urn(TABLE_1)]
    assert operations[extractor.identifiers.gen_dataset_urn(TABLE_1)].operationType == (
        OperationTypeClass.INSERT
    )


def test_operation_aspects_only_for_discovered_modified_tables() -> None:
    merge = _operation_row(
        "MERGE",
        [
            _table_object("TEST_DB.TEST_SCHEMA.TABLE_1"),
            _table_object("TEST_DB.TEST_SCHEMA.TABLE_2"),
            _table_object("TEST_DB.TEST_SCHEMA.MY_STAGE", domain="Stage"),
        ],
    )
    truncate = _operation_row(
        "TRUNCATE_TABLE", [_table_object("TEST_DB.TEST_SCHEMA.TABLE_3")]
    )
    extractor = _make_extractor(
        _dispatch(operations=[merge, truncate]), include_usage_stats=False
    )

    operations = _operations(list(extractor.get_usage_workunits([TABLE_1, TABLE_3])))

    table_1_urn = extractor.identifiers.gen_dataset_urn(TABLE_1)
    table_3_urn = extractor.identifiers.gen_dataset_urn(TABLE_3)
    assert set(operations) == {table_1_urn, table_3_urn}

    merge_op = operations[table_1_urn]
    assert merge_op.operationType == OperationTypeClass.UPDATE
    assert merge_op.customOperationType is None
    assert merge_op.actor == "urn:li:corpuser:amy@example.com"
    assert merge_op.lastUpdatedTimestamp == int(
        datetime(2022, 6, 7, 4, 0, tzinfo=timezone.utc).timestamp() * 1000
    )

    truncate_op = operations[table_3_urn]
    assert truncate_op.operationType == OperationTypeClass.CUSTOM
    assert truncate_op.customOperationType == "TRUNCATE_TABLE"


def test_unparseable_history_rows_are_counted_not_fatal() -> None:
    no_text = _operation_row("INSERT", [_table_object("TEST_DB.TEST_SCHEMA.TABLE_1")])
    no_text["QUERY_TEXT"] = ""
    bad_json = _operation_row("INSERT", [])
    bad_json["OBJECTS_MODIFIED"] = "{not json"
    good = _operation_row("INSERT", [_table_object("TEST_DB.TEST_SCHEMA.TABLE_1")])
    extractor = _make_extractor(
        _dispatch(operations=[no_text, bad_json, good]), include_usage_stats=False
    )

    operations = _operations(list(extractor.get_usage_workunits([TABLE_1])))

    assert len(operations) == 1
    assert extractor.report.rows_processed == 3
    assert extractor.report.rows_missing_query_text == 1
    assert extractor.report.rows_parsing_error == 1


def test_redundant_run_is_skipped_and_state_is_updated_otherwise() -> None:
    skipping = MagicMock()
    skipping.suggest_run_time_window.return_value = (START, END)
    skipping.should_skip_this_run.return_value = True
    extractor = _make_extractor(_dispatch(), skip_handler=skipping)

    assert list(extractor.get_usage_workunits([TABLE_1])) == []
    _as_mock(extractor.connection).query.assert_not_called()
    skipping.update_state.assert_not_called()

    running = MagicMock()
    running.suggest_run_time_window.return_value = (START, END)
    running.should_skip_this_run.return_value = False
    extractor = _make_extractor(_dispatch(), skip_handler=running)

    list(extractor.get_usage_workunits([TABLE_1]))

    running.update_state.assert_called_once_with(
        START, END, extractor.config.bucket_duration
    )
