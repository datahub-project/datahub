"""Pagination of database-wide ``SHOW`` commands.

Snowflake's ``SHOW <objects> IN DATABASE`` output is "ordered lexicographically by
database, schema, and name", but its ``LIMIT ... FROM '<name>'`` cursor matches on the
object *name* alone. The cursor key is therefore not the sort key, and paging a
database-wide result both skips objects (anything in a later schema whose name sorts
before the marker) and returns others twice. Views, streams and dynamic tables all
shared that bug; each must now page per schema instead, where name *is* the whole
sort key.
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import (
    SHOW_COMMAND_MAX_PAGE_SIZE,
    SHOW_STREAM_MAX_PAGE_SIZE,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeDynamicTable,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)

VIEWS = "VIEWS"
STREAMS = "STREAMS"
DYNAMIC_TABLES = "DYNAMIC TABLES"

SHOW_IN_SCHEMA = re.compile(
    r'SHOW (?P<kind>VIEWS|STREAMS|DYNAMIC TABLES) IN SCHEMA "(?P<db>[^"]+)"\."(?P<schema>[^"]+)"'
)
SHOW_IN_DATABASE = re.compile(
    r'SHOW (?P<kind>VIEWS|STREAMS|DYNAMIC TABLES) IN DATABASE "(?P<db>[^"]+)"'
)
LIMIT_CLAUSE = re.compile(r"LIMIT (?P<limit>\d+)")
FROM_CLAUSE = re.compile(r"FROM '(?P<marker>[^']+)'")


def _row(kind: str, schema_name: str, name: str) -> Dict[str, Any]:
    created = datetime(2026, 1, 1)
    if kind == VIEWS:
        return {
            "name": name,
            "schema_name": schema_name,
            "created_on": created,
            "comment": None,
            "text": f"CREATE VIEW {name} AS SELECT 1",
            "is_materialized": "false",
            "is_secure": "false",
        }
    if kind == STREAMS:
        return {
            "name": name,
            "schema_name": schema_name,
            "database_name": "TEST_DB",
            "created_on": created,
            "owner": "TEST_ROLE",
            "comment": None,
            "source_type": "Table",
            "type": "DELTA",
            "stale": "false",
            "mode": "DEFAULT",
            "invalid_reason": None,
            "owner_role_type": "ROLE",
            "stale_after": None,
            "table_name": f"TEST_DB.{schema_name}.source_table",
            "base_tables": None,
        }
    if kind == DYNAMIC_TABLES:
        return {
            "name": name,
            "schema_name": schema_name,
            "created_on": created,
            "comment": None,
            "text": f"CREATE DYNAMIC TABLE {name} AS SELECT 1",
            "target_lag": "1 hour",
            "bytes": 0,
            "rows": 0,
        }
    raise AssertionError(f"Unknown object kind: {kind}")


class FakeShowConnection:
    """Emulates Snowflake's documented ``SHOW`` semantics: output ordered by
    ``(schema, name)``, and ``FROM '<name>'`` a cursor on the name alone."""

    def __init__(self, objects: Dict[str, List[Tuple[str, str]]]) -> None:
        self._objects = {kind: sorted(rows) for kind, rows in objects.items()}
        self.queries: List[str] = []

    def query(self, query: str) -> List[Dict[str, Any]]:
        self.queries.append(query)

        in_schema = SHOW_IN_SCHEMA.search(query)
        in_database = SHOW_IN_DATABASE.search(query)
        if in_schema:
            kind = in_schema.group("kind")
            rows = [
                o
                for o in self._objects.get(kind, [])
                if o[0] == in_schema.group("schema")
            ]
        elif in_database:
            kind = in_database.group("kind")
            rows = list(self._objects.get(kind, []))
        else:
            # Non-SHOW metadata queries (e.g. the dynamic-table graph history) play no
            # part in pagination; answer them with no rows.
            return []

        marker = FROM_CLAUSE.search(query)
        if marker:
            rows = [o for o in rows if o[1] > marker.group("marker")]

        limit = LIMIT_CLAUSE.search(query)
        if limit:
            rows = rows[: int(limit.group("limit"))]

        return [_row(kind, schema_name, name) for schema_name, name in rows]

    def show_queries(self, kind: str) -> List[str]:
        return [q for q in self.queries if f"SHOW {kind} " in q]


class IgnoresFromClauseConnection(FakeShowConnection):
    """Mimics behaviour observed live: for some object classes (Snowflake's built-in
    INFORMATION_SCHEMA views) the ``FROM '<name>'`` cursor is ignored outright, so every
    page repeats the first one and a marker-driven loop never terminates."""

    def query(self, query: str) -> List[Dict[str, Any]]:
        return super().query(FROM_CLAUSE.sub("", query))


def _make_schema_gen(connection: FakeShowConnection) -> SnowflakeSchemaGenerator:
    config = SnowflakeV2Config.parse_obj(
        {
            "account_id": "test_account",
            "username": "test_user",
            "password": "test_password",
        }
    )
    return SnowflakeSchemaGenerator(
        config=config,
        report=SnowflakeV2Report(),
        connection=connection,  # type: ignore[arg-type]
        filters=MagicMock(),
        identifiers=MagicMock(),
        domain_registry=None,
        profiler=None,
        aggregator=None,
        snowsight_url_builder=None,
    )


def _make_data_dictionary(connection: FakeShowConnection) -> SnowflakeDataDictionary:
    return SnowflakeDataDictionary(
        connection=connection,  # type: ignore[arg-type]
        report=SnowflakeV2Report(),
        fetch_views_from_information_schema=False,
    )


def _boundary_skips_a_schema(page_size: int) -> List[Tuple[str, str]]:
    """``SCHEMA_A`` alone fills a full page and its names all sort *after*
    ``SCHEMA_B``'s, so a name-only cursor taken from the end of page 1 skips every
    object in ``SCHEMA_B``.

    This is the shape of the reported production failure: the alphabetically earliest
    views of a later schema went missing while their siblings were ingested.
    """
    return [("SCHEMA_A", f"m_{i:05d}") for i in range(page_size)] + [
        ("SCHEMA_B", f"a_{i:03d}") for i in range(100)
    ]


def _boundary_repeats_a_schema(page_size: int) -> List[Tuple[str, str]]:
    """Both schemas together fill exactly one page, and that page's last row (in
    ``SCHEMA_B``) has a name sorting *before* all of ``SCHEMA_A``'s -- so a name-only
    cursor rewinds into ``SCHEMA_A`` and returns it a second time. This is the
    inflated ``views_scanned`` counter seen in production."""
    half = page_size // 2
    return [("SCHEMA_A", f"z_{i:05d}") for i in range(half)] + [
        ("SCHEMA_B", f"a_{i:05d}") for i in range(half)
    ]


SKIPPED_NAMES = [f"a_{i:03d}" for i in range(100)]


# --- views ---------------------------------------------------------------------


def test_views_in_later_schema_survive_a_database_over_the_page_limit():
    connection = FakeShowConnection(
        {VIEWS: _boundary_skips_a_schema(SHOW_COMMAND_MAX_PAGE_SIZE)}
    )
    schema_gen = _make_schema_gen(connection)

    views = schema_gen.get_views_for_schema("SCHEMA_B", "TEST_DB")

    assert [v.name for v in views] == SKIPPED_NAMES


def test_no_view_is_returned_twice_for_a_database_over_the_page_limit():
    connection = FakeShowConnection(
        {VIEWS: _boundary_repeats_a_schema(SHOW_COMMAND_MAX_PAGE_SIZE)}
    )
    schema_gen = _make_schema_gen(connection)

    names = [v.name for v in schema_gen.get_views_for_schema("SCHEMA_A", "TEST_DB")]

    assert len(names) == len(set(names)) == SHOW_COMMAND_MAX_PAGE_SIZE // 2


def test_per_schema_show_views_pages_through_every_view_exactly_once():
    total = SHOW_COMMAND_MAX_PAGE_SIZE + 2000
    connection = FakeShowConnection(
        {VIEWS: [("SCHEMA_A", f"view_{i:05d}") for i in range(total)]}
    )

    views = _make_data_dictionary(connection).get_views_for_schema_using_show(
        db_name="TEST_DB", schema_name="SCHEMA_A"
    )

    assert [v.name for v in views] == [f"view_{i:05d}" for i in range(total)]


def test_per_schema_paging_stops_when_the_cursor_does_not_advance():
    """A broken cursor must produce a bounded, duplicate-free result plus a warning --
    never an endless loop. Observed live against INFORMATION_SCHEMA views."""
    connection = IgnoresFromClauseConnection(
        {
            VIEWS: [
                ("SCHEMA_A", f"view_{i:05d}")
                for i in range(SHOW_COMMAND_MAX_PAGE_SIZE + 500)
            ]
        }
    )
    data_dictionary = _make_data_dictionary(connection)

    views = data_dictionary.get_views_for_schema_using_show(
        db_name="TEST_DB", schema_name="SCHEMA_A"
    )

    names = [v.name for v in views]
    assert len(names) == len(set(names)) == SHOW_COMMAND_MAX_PAGE_SIZE
    assert len(connection.show_queries(VIEWS)) == 2
    assert data_dictionary.report.warnings


def test_database_under_the_page_limit_uses_a_single_database_wide_view_query():
    """Regression guard: the cheap one-query-per-database path must stay in place."""
    connection = FakeShowConnection(
        {
            VIEWS: [("SCHEMA_A", f"view_{i:03d}") for i in range(10)]
            + [("SCHEMA_B", "other")]
        }
    )
    schema_gen = _make_schema_gen(connection)

    assert len(schema_gen.get_views_for_schema("SCHEMA_A", "TEST_DB")) == 10
    assert len(schema_gen.get_views_for_schema("SCHEMA_B", "TEST_DB")) == 1
    assert len(connection.show_queries(VIEWS)) == 1


# --- streams -------------------------------------------------------------------


def test_streams_in_later_schema_survive_a_database_over_the_page_limit():
    connection = FakeShowConnection(
        {STREAMS: _boundary_skips_a_schema(SHOW_STREAM_MAX_PAGE_SIZE)}
    )
    schema_gen = _make_schema_gen(connection)

    streams = schema_gen.get_streams_for_schema("SCHEMA_B", "TEST_DB")

    assert [s.name for s in streams] == SKIPPED_NAMES


def test_per_schema_show_streams_pages_through_every_stream_exactly_once():
    total = SHOW_STREAM_MAX_PAGE_SIZE + 2000
    connection = FakeShowConnection(
        {STREAMS: [("SCHEMA_A", f"stream_{i:05d}") for i in range(total)]}
    )

    streams = _make_data_dictionary(connection).get_streams_for_schema_using_show(
        db_name="TEST_DB", schema_name="SCHEMA_A"
    )

    assert [s.name for s in streams] == [f"stream_{i:05d}" for i in range(total)]


def test_database_under_the_page_limit_uses_a_single_database_wide_stream_query():
    connection = FakeShowConnection(
        {STREAMS: [("SCHEMA_A", "stream_a"), ("SCHEMA_B", "stream_b")]}
    )
    schema_gen = _make_schema_gen(connection)

    assert len(schema_gen.get_streams_for_schema("SCHEMA_A", "TEST_DB")) == 1
    assert len(schema_gen.get_streams_for_schema("SCHEMA_B", "TEST_DB")) == 1
    assert len(connection.show_queries(STREAMS)) == 1


# --- dynamic tables ------------------------------------------------------------


def _dynamic_table(name: str) -> SnowflakeDynamicTable:
    return SnowflakeDynamicTable(
        name=name,
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        is_dynamic=True,
    )


def test_dynamic_table_definition_is_populated_for_a_later_schema():
    connection = FakeShowConnection(
        {DYNAMIC_TABLES: _boundary_skips_a_schema(SHOW_COMMAND_MAX_PAGE_SIZE)}
    )
    table = _dynamic_table("a_000")

    _make_data_dictionary(connection).populate_dynamic_table_definitions(
        {"SCHEMA_B": [table]}, "TEST_DB"
    )

    assert table.definition == "CREATE DYNAMIC TABLE a_000 AS SELECT 1"


def test_per_schema_show_dynamic_tables_pages_through_every_table_exactly_once():
    total = SHOW_COMMAND_MAX_PAGE_SIZE + 2000
    connection = FakeShowConnection(
        {DYNAMIC_TABLES: [("SCHEMA_A", f"dt_{i:05d}") for i in range(total)]}
    )

    tables = _make_data_dictionary(connection).get_dynamic_tables_for_schema_using_show(
        db_name="TEST_DB", schema_name="SCHEMA_A"
    )

    assert [t.name for t in tables] == [f"dt_{i:05d}" for i in range(total)]


def test_database_under_the_page_limit_uses_a_single_database_wide_dynamic_table_query():
    connection = FakeShowConnection(
        {DYNAMIC_TABLES: [("SCHEMA_A", "dt_a"), ("SCHEMA_B", "dt_b")]}
    )
    data_dictionary = _make_data_dictionary(connection)
    dt_a = _dynamic_table("dt_a")
    dt_b = _dynamic_table("dt_b")

    data_dictionary.populate_dynamic_table_definitions(
        {"SCHEMA_A": [dt_a], "SCHEMA_B": [dt_b]}, "TEST_DB"
    )

    assert dt_a.definition == "CREATE DYNAMIC TABLE dt_a AS SELECT 1"
    assert dt_b.definition == "CREATE DYNAMIC TABLE dt_b AS SELECT 1"
    assert len(connection.show_queries(DYNAMIC_TABLES)) == 1


@pytest.mark.parametrize("kind", [VIEWS, STREAMS, DYNAMIC_TABLES])
def test_empty_database_issues_no_per_schema_queries(kind):
    connection = FakeShowConnection({kind: []})
    data_dictionary = _make_data_dictionary(connection)

    if kind == VIEWS:
        assert data_dictionary.get_views_for_database("TEST_DB") == {}
    elif kind == STREAMS:
        assert data_dictionary.get_streams_for_database("TEST_DB") == {}
    else:
        assert data_dictionary.get_dynamic_tables_with_definitions("TEST_DB") == {}

    assert len(connection.show_queries(kind)) == 1
