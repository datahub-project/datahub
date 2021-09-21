import datetime

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.usage.usage_from_sql_table import (
    UsageFromSqlTableConfig,
    UsageFromSqlTableSource,
)


def get_simple_source():
    return UsageFromSqlTableSource(
        ctx=PipelineContext(run_id="source-test"),
        config=UsageFromSqlTableConfig(
            connect_uri="", usage_query="", platform="athena"
        ),
    )


def get_source_with_prefix(prefix):
    return UsageFromSqlTableSource(
        ctx=PipelineContext(run_id="source-test"),
        config=UsageFromSqlTableConfig(
            connect_uri="", usage_query="", platform="athena", 
            table_default_prefix=prefix
        ),
    )


def test_basic_query_parse():
    event_dict = {
        "query_id": "query-1",
        "query_text": "select col1 from table1, table2 where col2 is not null"
    }
    get_simple_source().parse_query(event_dict)
    assert event_dict["tables"] == ["table1", "table2"]
    assert event_dict["columns"] == ["col1", "col2"]


def test_process_row():
    row = {
        "query_id": "query-1",
        "query_text": "select col1 from table1, table2 where col2 is not null",
        "query_start_time": datetime.datetime.fromisoformat('2021-09-14T12:30:00+05:30')
    }
    event_dict = get_simple_source().process_row(row)
    assert event_dict["tables"] == ["table1", "table2"]
    assert event_dict["columns"] == ["col1", "col2"]
    assert event_dict["query_start_time"] == datetime.datetime(2021, 9, 14, 7, 0, tzinfo=datetime.timezone.utc)

def test_parse_with_default_prefix():
    event_dict = {
        "query_id": "query-1",
        "query_text": "select col1 from table1, table2 where col2 is not null"
    }
    get_source_with_prefix('analytics').parse_query(event_dict)
    assert event_dict["tables"] == ["analytics.table1", "analytics.table2"]
    assert event_dict["columns"] == ["col1", "col2"]
