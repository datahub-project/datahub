import json
from datetime import datetime, timezone
from typing import Callable, List, Optional, Sequence, Union
from unittest.mock import MagicMock

from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage
from datahub.ingestion.source.snowflake.constants import SnowflakeEdition
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownLineageMapping,
    KnownQueryLineageInfo,
)
from datahub.sql_parsing.sqlglot_lineage import ColumnRef

START = datetime(2022, 6, 6, tzinfo=timezone.utc)
END = datetime(2022, 6, 8, tzinfo=timezone.utc)
TABLE_1 = "test_db.test_schema.table_1"
TABLE_2 = "test_db.test_schema.table_2"
QUERY_ID = "01b2576e-0804-4957-0034-7d83066cd0ee"

QueryResult = Union[Sequence[dict], Exception]
QueryHandler = Callable[[str], QueryResult]


def _as_mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


def _is_copy_history_query(query: str) -> bool:
    return "copy_history" in query


def _is_table_lineage_query(query: str) -> bool:
    return "UPSTREAM_TABLES" in query


def _dispatch(
    table_lineage: QueryResult = (), copy_history: QueryResult = ()
) -> QueryHandler:
    def handler(query: str) -> QueryResult:
        if _is_copy_history_query(query):
            return (
                copy_history
                if isinstance(copy_history, Exception)
                else list(copy_history)
            )
        if _is_table_lineage_query(query):
            return (
                table_lineage
                if isinstance(table_lineage, Exception)
                else list(table_lineage)
            )
        raise AssertionError(f"Unexpected query: {query}")

    return handler


def _make_extractor(
    handler: QueryHandler,
    *,
    edition: SnowflakeEdition = SnowflakeEdition.ENTERPRISE,
    **config_overrides: object,
) -> SnowflakeLineageExtractor:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "user",
            "password": "pass",
            "start_time": START,
            "end_time": END,
            # Pinned so the column-case sweep (DATAHUB_TEST_PRESERVE_COLUMN_CASE=1 in
            # CI, see conftest.py) does not change the lowercased field paths asserted here.
            "preserve_column_case": False,
            **config_overrides,
        }
    )
    report = SnowflakeV2Report()
    report.edition = edition
    connection = MagicMock()

    def query(sql: str) -> List[dict]:
        result = handler(sql)
        if isinstance(result, Exception):
            raise result
        return list(result)

    connection.query.side_effect = query
    return SnowflakeLineageExtractor(
        config=config,
        report=report,
        connection=connection,
        filters=SnowflakeFilter(filter_config=config, structured_reporter=report),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        redundant_run_skip_handler=None,
        sql_aggregator=MagicMock(),
    )


def _upstream_table(name: str, query_id: str = QUERY_ID, domain: str = "TABLE") -> dict:
    return {
        "upstream_object_name": name,
        "upstream_object_domain": domain,
        "query_id": query_id,
    }


def _column_upstreams(
    column_name: str, sources: List[str], query_id: str = QUERY_ID
) -> dict:
    return {
        "column_name": column_name,
        "upstreams": [
            {
                "query_id": query_id,
                "column_upstreams": [
                    {
                        "object_name": source.rsplit(".", 1)[0],
                        "object_domain": "Table",
                        "column_name": source.rsplit(".", 1)[1],
                    }
                    for source in sources
                ],
            }
        ],
    }


def _lineage_row(
    downstream: str,
    upstream_tables: List[dict],
    upstream_columns: Optional[List[dict]] = None,
    queries: Optional[Union[List[dict], str]] = None,
) -> dict:
    if queries is None:
        queries = [
            {
                "query_id": QUERY_ID,
                "query_text": f"insert into {downstream} select * from table_2",
                "start_time": "2022-06-07 04:00:00",
            }
        ]
    return {
        "DOWNSTREAM_TABLE_NAME": downstream,
        "DOWNSTREAM_TABLE_DOMAIN": "TABLE",
        "UPSTREAM_TABLES": json.dumps(upstream_tables),
        "UPSTREAM_COLUMNS": json.dumps(upstream_columns or []),
        "QUERIES": queries if isinstance(queries, str) else json.dumps(queries),
    }


def _known_query_lineage(extractor: SnowflakeLineageExtractor) -> KnownQueryLineageInfo:
    _as_mock(extractor.sql_aggregator).add_known_query_lineage.assert_called_once()
    lineage = _as_mock(extractor.sql_aggregator).add_known_query_lineage.call_args.args[
        0
    ]
    assert isinstance(lineage, KnownQueryLineageInfo)
    return lineage


def test_access_history_edge_becomes_known_query_lineage() -> None:
    row = _lineage_row(
        "TEST_DB.TEST_SCHEMA.TABLE_1",
        [
            _upstream_table("TEST_DB.TEST_SCHEMA.TABLE_2"),
            _upstream_table("TEST_DB.TEST_SCHEMA.VIEW_1", domain="VIEW"),
        ],
        [_column_upstreams("COL_A", ["TEST_DB.TEST_SCHEMA.TABLE_2.COL_X"])],
    )
    extractor = _make_extractor(_dispatch(table_lineage=[row]))

    extractor.add_time_based_lineage_to_aggregator([TABLE_1, TABLE_2], [])

    lineage = _known_query_lineage(extractor)
    ids = extractor.identifiers
    assert lineage.downstream == ids.gen_dataset_urn(TABLE_1)
    assert set(lineage.upstreams) == {
        ids.gen_dataset_urn(TABLE_2),
        ids.gen_dataset_urn("test_db.test_schema.view_1"),
    }
    assert lineage.column_lineage is not None
    assert lineage.column_lineage[0].downstream.column == "col_a"
    assert lineage.column_lineage[0].upstreams == [
        ColumnRef(table=ids.gen_dataset_urn(TABLE_2), column="col_x")
    ]
    assert lineage.timestamp == datetime(2022, 6, 7, 4, 0, tzinfo=timezone.utc)
    assert extractor.report.num_tables_with_known_upstreams == 1


def test_upstreams_are_restricted_to_the_query_and_allowed_patterns() -> None:
    row = _lineage_row(
        "TEST_DB.TEST_SCHEMA.TABLE_1",
        [
            _upstream_table("TEST_DB.TEST_SCHEMA.TABLE_2"),
            _upstream_table("OTHER_DB.OTHER_SCHEMA.TABLE_9"),
            _upstream_table("TEST_DB.TEST_SCHEMA.TABLE_3", query_id="other-query"),
        ],
        [
            _column_upstreams(
                "COL_A",
                [
                    "TEST_DB.TEST_SCHEMA.TABLE_2.COL_X",
                    "OTHER_DB.OTHER_SCHEMA.TABLE_9.COL_Z",
                ],
            ),
            _column_upstreams(
                "COL_B", ["TEST_DB.TEST_SCHEMA.TABLE_3.COL_Y"], query_id="other-query"
            ),
        ],
    )
    extractor = _make_extractor(
        _dispatch(table_lineage=[row]),
        validate_upstreams_against_patterns=True,
        database_pattern={"deny": ["OTHER_DB"]},
    )

    extractor.add_time_based_lineage_to_aggregator([TABLE_1], [])

    lineage = _known_query_lineage(extractor)
    table_2_urn = extractor.identifiers.gen_dataset_urn(TABLE_2)
    assert lineage.upstreams == [table_2_urn]
    assert lineage.column_lineage is not None
    assert [cl.downstream.column for cl in lineage.column_lineage] == ["col_a"]
    assert lineage.column_lineage[0].upstreams == [
        ColumnRef(table=table_2_urn, column="col_x")
    ]


def test_rows_without_usable_lineage_are_skipped_without_failing() -> None:
    no_queries = _lineage_row(
        "TEST_DB.TEST_SCHEMA.TABLE_1",
        [_upstream_table("TEST_DB.TEST_SCHEMA.TABLE_2")],
        queries="[\n  {}\n]",
    )
    malformed = _lineage_row("TEST_DB.TEST_SCHEMA.TABLE_1", [])
    malformed["UPSTREAM_TABLES"] = "{not json"
    undiscovered = _lineage_row(
        "TEST_DB.TEST_SCHEMA.TABLE_2", [_upstream_table("TEST_DB.TEST_SCHEMA.TABLE_1")]
    )
    extractor = _make_extractor(
        _dispatch(table_lineage=[no_queries, malformed, undiscovered])
    )

    extractor.add_time_based_lineage_to_aggregator([TABLE_1], [])

    _as_mock(extractor.sql_aggregator).add_known_query_lineage.assert_not_called()
    assert extractor.report.num_upstream_lineage_edge_parsing_failed == 1
    assert len(extractor.report.warnings) == 1
    assert extractor.report.failures == []


def test_standard_edition_skips_access_history_but_keeps_copy_history() -> None:
    seen: List[str] = []

    def handler(query: str) -> QueryResult:
        seen.append(query)
        return []

    extractor = _make_extractor(handler, edition=SnowflakeEdition.STANDARD)

    extractor.add_time_based_lineage_to_aggregator([TABLE_1], [])

    assert [_is_copy_history_query(q) for q in seen] == [True]


def test_copy_history_adds_s3_upstreams_for_discovered_tables() -> None:
    rows: List[dict] = [
        {
            "DOWNSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.TABLE_1",
            "UPSTREAM_LOCATIONS": json.dumps(
                ["azure://container/path", "s3://bucket/path/"]
            ),
        },
        {
            "DOWNSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.TABLE_2",
            "UPSTREAM_LOCATIONS": json.dumps(["s3://bucket/other/"]),
        },
        {
            "DOWNSTREAM_TABLE_NAME": "TEST_DB.TEST_SCHEMA.TABLE_1",
            "UPSTREAM_LOCATIONS": None,
        },
    ]
    extractor = _make_extractor(_dispatch(copy_history=rows))

    extractor.add_time_based_lineage_to_aggregator([TABLE_1], [])

    _as_mock(extractor.sql_aggregator).add.assert_called_once_with(
        KnownLineageMapping(
            upstream_urn=make_s3_urn_for_lineage("s3://bucket/path/", "PROD"),
            downstream_urn=extractor.identifiers.gen_dataset_urn(TABLE_1),
        )
    )
    assert extractor.report.num_external_table_edges_scanned == 1


def test_permission_error_fails_run_but_other_errors_only_warn() -> None:
    denied = _make_extractor(
        _dispatch(table_lineage=SnowflakePermissionError("not authorized"))
    )
    denied.add_time_based_lineage_to_aggregator([TABLE_1], [])
    assert len(denied.report.failures) == 1
    _as_mock(denied.sql_aggregator).add_known_query_lineage.assert_not_called()

    flaky = _make_extractor(_dispatch(copy_history=RuntimeError("transient")))
    flaky.add_time_based_lineage_to_aggregator([TABLE_1], [])
    assert flaky.report.failures == []
    assert len(flaky.report.warnings) == 1


def test_ignore_start_time_lineage_queries_from_the_epoch() -> None:
    seen: List[str] = []

    def handler(query: str) -> QueryResult:
        seen.append(query)
        return []

    extractor = _make_extractor(handler, ignore_start_time_lineage=True)

    extractor.add_time_based_lineage_to_aggregator([TABLE_1], [])

    assert extractor.report.lineage_start_time == datetime(
        1970, 1, 1, tzinfo=timezone.utc
    )
    expected = SnowflakeQuery.table_to_table_lineage_history_v2(
        start_time_millis=0,
        end_time_millis=int(END.timestamp() * 1000),
        upstreams_deny_pattern=extractor.config.temporary_tables_pattern,
        include_column_lineage=True,
    )
    assert expected in seen
