"""Pagination of database-wide ``SHOW`` commands.

A database-wide ``SHOW`` sorts by (database, schema, name) but its cursor matches on name
alone, so paging one both skips and duplicates objects - see
``SnowflakeQuery.show_objects_for_database`` for the full rule. Views, streams and dynamic
tables all shared the bug and all now page per schema, where name is the whole sort key.
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.constants import SnowflakeShowKind
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_query import (
    SHOW_COMMAND_MAX_PAGE_SIZE,
    SHOW_STREAM_MAX_PAGE_SIZE,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeDynamicTable,
    SnowflakeSchema,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)

VIEWS = SnowflakeShowKind.VIEWS
STREAMS = SnowflakeShowKind.STREAMS
DYNAMIC_TABLES = SnowflakeShowKind.DYNAMIC_TABLES

# Built from the enum so the fake connection recognises exactly the object classes the
# connector can ask for - a new kind cannot be paged without this fixture understanding it.
_KINDS = "|".join(kind.value for kind in SnowflakeShowKind)

# The object name is captured loosely, up to the LIMIT that every SHOW this fake sees
# carries, so that a badly quoted identifier reaches _parse_identifiers as a hard error
# rather than failing the match and being answered with an empty result set.
SHOW_IN_SCHEMA = re.compile(rf"SHOW (?P<kind>{_KINDS}) IN SCHEMA (?P<name>.+?) LIMIT ")
SHOW_IN_DATABASE = re.compile(
    rf"SHOW (?P<kind>{_KINDS}) IN DATABASE (?P<name>.+?) LIMIT "
)
LIMIT_CLAUSE = re.compile(r"LIMIT (?P<limit>\d+)")
# Spans the whole literal, including any doubled quotes, so that a badly escaped marker is
# visible to _parse_marker rather than being silently truncated at the first quote.
FROM_CLAUSE = re.compile(r"FROM '(?P<marker>.*)'\s*;", re.DOTALL)
# A double-quoted identifier: any run of non-quote characters, with a literal quote
# written doubled.
LEADING_QUOTED_IDENTIFIER = re.compile(r'"((?:[^"]|"")*)"')


def _parse_marker(query: str) -> Optional[str]:
    """Read the ``FROM '<name>'`` cursor the way Snowflake would.

    Snowflake rejects a statement whose string literal contains a bare quote, so an
    unescaped marker is a hard error here too - not a truncated cursor. Without that,
    a fake quietly recovers from bad escaping and the test proves nothing.
    """
    match = FROM_CLAUSE.search(query)
    if match is None:
        return None

    literal = match.group("marker")
    if "'" in literal.replace("''", ""):
        raise ValueError(f"Malformed SQL - unescaped quote in literal: {query!r}")
    return literal.replace("''", "'").replace("\\\\", "\\")


def _parse_identifiers(name: str, query: str) -> List[str]:
    """Split a dot-qualified quoted identifier the way Snowflake would.

    The counterpart to _parse_marker, for the other escaping rule the same statement
    relies on: inside a double-quoted identifier a literal quote is written doubled, and
    a bare one ends the identifier early - a syntax error to Snowflake, so a hard error
    here too. A fake that instead read a truncated schema name would let the connector
    ship a statement Snowflake rejects and still pass.
    """
    identifiers: List[str] = []
    rest = name
    while True:
        match = LEADING_QUOTED_IDENTIFIER.match(rest)
        if match is None:
            raise ValueError(f"Malformed SQL - bad quoted identifier in: {query!r}")
        identifiers.append(match.group(1).replace('""', '"'))
        rest = rest[match.end() :]
        if not rest:
            return identifiers
        if not rest.startswith("."):
            raise ValueError(f"Malformed SQL - bad quoted identifier in: {query!r}")
        rest = rest[1:]


def _row(kind: SnowflakeShowKind, schema_name: str, name: str) -> Dict[str, Any]:
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

    def __init__(
        self,
        objects: Dict[SnowflakeShowKind, List[Tuple[str, str]]],
        max_queries: int = 10,
    ) -> None:
        self._objects = {kind: sorted(rows) for kind, rows in objects.items()}
        self.queries: List[str] = []
        self._max_queries = max_queries

    def query(self, query: str) -> List[Dict[str, Any]]:
        self.queries.append(query)
        # SHOW statements only: counting every query would let a wide fan-out fixture trip
        # a message claiming the caller never terminates. Only paging repeats a SHOW shape.
        show_queries = [q for q in self.queries if q.lstrip().startswith("SHOW ")]
        if len(show_queries) > self._max_queries:
            # Fail fast rather than hang: without this, a caller that stops advancing its
            # cursor pages forever and the test times out CI instead of reporting.
            raise AssertionError(
                f"Runaway pagination: {len(show_queries)} SHOW queries issued for "
                f"{self._max_queries} allowed; the caller is not terminating."
            )

        # What Snowflake acts on. A subclass may differ from what was sent (see
        # IgnoresFromClauseConnection); self.queries always keeps the statement as built.
        effective = self._effective(query)

        in_schema = SHOW_IN_SCHEMA.search(effective)
        in_database = SHOW_IN_DATABASE.search(effective)
        if in_schema:
            kind = SnowflakeShowKind(in_schema.group("kind"))
            _, schema = _parse_identifiers(in_schema.group("name"), effective)
            rows = [o for o in self._objects.get(kind, []) if o[0] == schema]
        elif in_database:
            kind = SnowflakeShowKind(in_database.group("kind"))
            _parse_identifiers(in_database.group("name"), effective)
            rows = list(self._objects.get(kind, []))
        else:
            # Non-SHOW metadata queries (e.g. the dynamic-table graph history) play no
            # part in pagination; answer them with no rows.
            return []

        marker = _parse_marker(effective)
        if marker is not None:
            rows = [o for o in rows if o[1] > marker]

        limit = LIMIT_CLAUSE.search(effective)
        if limit:
            rows = rows[: int(limit.group("limit"))]

        return [_row(kind, schema_name, name) for schema_name, name in rows]

    def _effective(self, query: str) -> str:
        """The statement Snowflake behaves as though it received. Overridden to model a
        server that quietly disregards part of what was sent."""
        return query

    def show_queries(self, kind: SnowflakeShowKind) -> List[str]:
        return [q for q in self.queries if f"SHOW {kind} " in q]


class IgnoresFromClauseConnection(FakeShowConnection):
    """Mimics behaviour observed live: for some object classes (Snowflake's built-in
    INFORMATION_SCHEMA views) the ``FROM '<name>'`` cursor is ignored outright, so every
    page repeats the first one and a marker-driven loop never terminates."""

    def _effective(self, query: str) -> str:
        return FROM_CLAUSE.sub("", query)


class FailsDatabaseWideConnection(FakeShowConnection):
    """A database-wide ``SHOW`` can fail on its own - a statement timeout, or a result set
    too large for the account's limits - while the narrower per-schema queries succeed."""

    def query(self, query: str) -> List[Dict[str, Any]]:
        if SHOW_IN_DATABASE.search(query):
            raise ValueError("SHOW ... IN DATABASE failed")
        return super().query(query)


class FailsEverySchemaConnection(FakeShowConnection):
    """Every SHOW fails - the shape of a systemic problem such as a missing grant, where
    neither the database-wide query nor the per-schema fallback can succeed."""

    def query(self, query: str) -> List[Dict[str, Any]]:
        super().query(query)
        if SHOW_IN_DATABASE.search(query) or SHOW_IN_SCHEMA.search(query):
            raise ValueError("SHOW failed")
        return []


class DeniesEveryShowConnection(FakeShowConnection):
    """A missing grant. The connection layer classifies Snowflake's "does not exist or not
    authorized" as SnowflakePermissionError, which is not a size problem - so the per-schema
    fallback cannot recover it and must not be attempted."""

    def query(self, query: str) -> List[Dict[str, Any]]:
        super().query(query)
        raise SnowflakePermissionError(
            "002003 (02000): SQL compilation error: "
            "Database 'TEST_DB' does not exist or not authorized."
        )


class DeniesOnlyPerSchemaConnection(FakeShowConnection):
    """The database-wide SHOW succeeds but fills its page, so the caller falls back per
    schema - and only there is the denial hit. This is the shape of a role holding USAGE on
    the database while lacking it on one schema, and the only way to reach the per-schema
    handler's permission branch: when every query is denied, the database-wide probe raises
    first and the fallback never runs."""

    def query(self, query: str) -> List[Dict[str, Any]]:
        if SHOW_IN_SCHEMA.search(query):
            self.queries.append(query)
            raise SnowflakePermissionError(
                "002003 (02000): SQL compilation error: "
                "Schema 'SCHEMA_A' does not exist or not authorized."
            )
        return super().query(query)


class ReturnsAnUnmappableRowConnection(FakeShowConnection):
    """A row the mapper cannot handle: _map_show_view lowercases is_materialized, which a
    NULL column value breaks."""

    def query(self, query: str) -> List[Dict[str, Any]]:
        rows = super().query(query)
        for row in rows:
            row["is_materialized"] = None
        return rows


class FailsAfterFirstPageConnection(FakeShowConnection):
    """Page 1 succeeds and page 2 raises, so the caller ends up holding a partial result it
    cannot tell apart from a complete one."""

    def query(self, query: str) -> List[Dict[str, Any]]:
        if SHOW_IN_SCHEMA.search(query) and any(
            SHOW_IN_SCHEMA.search(q) for q in self.queries
        ):
            self.queries.append(query)
            raise ValueError("page 2 failed")
        return super().query(query)


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


@pytest.mark.parametrize(
    "kind,page_size,fetch",
    [
        (
            VIEWS,
            SHOW_COMMAND_MAX_PAGE_SIZE,
            lambda gen: gen.get_views_for_schema("SCHEMA_B", "TEST_DB"),
        ),
        (
            STREAMS,
            SHOW_STREAM_MAX_PAGE_SIZE,
            lambda gen: gen.get_streams_for_schema("SCHEMA_B", "TEST_DB"),
        ),
    ],
    ids=["views", "streams"],
)
def test_objects_in_a_later_schema_survive_a_database_over_the_page_limit(
    kind, page_size, fetch
):
    """The page boundary is one rule shared by every object type, so each kind is checked
    against the same fixture and expectation rather than in a hand-copied test."""
    connection = FakeShowConnection({kind: _boundary_skips_a_schema(page_size)})

    objects = fetch(_make_schema_gen(connection))

    assert [o.name for o in objects] == SKIPPED_NAMES


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


@pytest.mark.parametrize(
    "kind,page_size,fetch",
    [
        (
            VIEWS,
            SHOW_COMMAND_MAX_PAGE_SIZE,
            lambda dd: dd.get_views_for_schema_using_show(
                db_name="TEST_DB", schema_name="SCHEMA_A"
            ),
        ),
        (
            STREAMS,
            SHOW_STREAM_MAX_PAGE_SIZE,
            lambda dd: dd.get_streams_for_schema_using_show(
                db_name="TEST_DB", schema_name="SCHEMA_A"
            ),
        ),
        (
            DYNAMIC_TABLES,
            SHOW_COMMAND_MAX_PAGE_SIZE,
            lambda dd: dd.get_dynamic_tables_for_schema_using_show(
                db_name="TEST_DB", schema_name="SCHEMA_A"
            ),
        ),
    ],
    ids=["views", "streams", "dynamic_tables"],
)
def test_per_schema_paging_survives_an_object_name_needing_escaping(
    kind, page_size, fetch
):
    """A quoted Snowflake identifier may contain an apostrophe or a backslash, and the
    marker is embedded in a single-quoted SQL literal. Unescaped, page 2 is malformed SQL
    and the rest of the schema is lost. Escaping is applied at three call sites, so it is
    checked at all three - one passing kind would otherwise cover for two broken ones.
    """
    # The awkward name has to be the LAST row of a full page, because that is the row whose
    # name becomes the cursor for page 2 - anywhere else and the quote never reaches a query.
    head = [f"obj_{i:05d}" for i in range(page_size - 1)]
    awkward = "w_it's_a\\_name"
    tail = [f"x_{i:02d}" for i in range(10)]
    expected = head + [awkward] + tail
    assert sorted(expected) == expected, "fixture must already be in SHOW order"

    connection = FakeShowConnection({kind: [("SCHEMA_A", n) for n in expected]})

    objects = fetch(_make_data_dictionary(connection))

    assert [o.name for o in objects] == expected
    # Quote doubled and backslash doubled, in that order.
    assert "FROM 'w_it''s_a\\\\_name'" in connection.show_queries(kind)[1], (
        f"page 2's cursor must carry the escaped name; got {connection.show_queries(kind)[1]!r}"
    )


def test_per_schema_paging_escapes_a_schema_name_containing_a_quote():
    """The other half of the escaping the same statement depends on. A quoted Snowflake
    identifier may contain a quote, doubled - so a schema named a"b closes the identifier
    early unless escaped, and the statement is rejected. The fake enforces that rule (see
    _parse_identifiers), so an unescaped identifier fails here rather than silently
    reading a truncated schema name."""
    schema = 'SCHEMA"A'
    connection = FakeShowConnection(
        {VIEWS: [(schema, "view_a"), ("SCHEMA_B", "view_b")]}
    )

    views = _make_data_dictionary(connection).get_views_for_schema_using_show(
        db_name='TEST"DB', schema_name=schema
    )

    assert [v.name for v in views] == ["view_a"]
    assert 'IN SCHEMA "TEST""DB"."SCHEMA""A"' in connection.show_queries(VIEWS)[0]


def test_per_schema_paging_stops_when_the_cursor_does_not_advance():
    """A broken cursor must produce a bounded, duplicate-free result plus a failure --
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
    issued = connection.show_queries(VIEWS)
    assert len(issued) == 2
    # Must be a failure, not a warning: the result is knowingly short, and stale-entity
    # removal only skips soft-deletion when the source reported a failure. Asserting the
    # severity is the point - a warning here would soft-delete the objects left unlisted.
    assert data_dictionary.report.failures
    assert not data_dictionary.report.warnings
    # The recorded statement is what the connector built, not what this fake pretended
    # Snowflake acted on: page 2 did carry a cursor, and the server disregarded it.
    assert "FROM '" in issued[1], (
        f"page 2 should have been built with a cursor: {issued[1]!r}"
    )


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


def test_dynamic_table_definitions_fall_back_per_schema_when_the_database_wide_show_fails():
    connection = FailsDatabaseWideConnection({DYNAMIC_TABLES: [("SCHEMA_A", "dt_a")]})
    table = _dynamic_table("dt_a")

    _make_data_dictionary(connection).populate_dynamic_table_definitions(
        {"SCHEMA_A": [table]}, "TEST_DB"
    )

    assert table.definition == "CREATE DYNAMIC TABLE dt_a AS SELECT 1"


def test_a_failed_dynamic_table_fetch_is_reported_not_only_logged():
    """The fallback is the last resort: if it fails too, every definition and its lineage
    is lost. That must reach the ingestion report, not just a debug log."""
    connection = FailsEverySchemaConnection({DYNAMIC_TABLES: [("SCHEMA_A", "dt_a")]})
    data_dictionary = _make_data_dictionary(connection)

    data_dictionary.populate_dynamic_table_definitions(
        {"SCHEMA_A": [_dynamic_table("dt_a")]}, "TEST_DB"
    )

    messages = [f.message for f in data_dictionary.report.failures]
    # Assert the per-schema handler specifically. The database-wide failure warns first, so
    # a bare `assert report.warnings` passes even with the terminal handler silenced - which
    # is the handler that actually loses data.
    assert any("for schema" in m for m in messages), (
        f"the terminal per-schema failure must be reported; got {messages}"
    )


@pytest.mark.parametrize(
    "kind,fetch",
    [
        (VIEWS, lambda gen: gen.get_views_for_schema("SCHEMA_A", "TEST_DB")),
        (STREAMS, lambda gen: gen.get_streams_for_schema("SCHEMA_A", "TEST_DB")),
    ],
    ids=["views", "streams"],
)
def test_a_failed_database_wide_show_falls_back_per_schema(kind, fetch):
    """A database-wide SHOW that cannot run at all - a missing grant, a statement timeout -
    must still reach the exact per-schema path. Losing every object in the database because
    one wide query failed is the outcome this whole fallback exists to avoid."""
    connection = FailsDatabaseWideConnection({kind: [("SCHEMA_A", "obj_a")]})

    objects = fetch(_make_schema_gen(connection))

    assert [o.name for o in objects] == ["obj_a"]


def test_a_denied_show_propagates_instead_of_falling_back_per_schema():
    """A missing grant is not a size problem, so the fallback cannot recover it. Swallowing
    it cost the operator the permission classification the callers already implement (see
    SnowflakeSchemaGenerator.fetch_views_for_schema) and issued one doomed query per schema
    in a database it could not read."""
    connection = DeniesEveryShowConnection({VIEWS: [("SCHEMA_A", "view_a")]})
    data_dictionary = _make_data_dictionary(connection)

    with pytest.raises(SnowflakePermissionError):
        data_dictionary.get_views_for_database("TEST_DB")

    assert len(connection.queries) == 1, (
        f"a denial must not be retried per schema; issued {connection.queries}"
    )
    # Reporting it here would pre-empt the caller's permission-specific failure with a
    # generic one, so this handler must stay silent.
    assert not data_dictionary.report.failures
    assert not data_dictionary.report.warnings


@pytest.mark.parametrize(
    "kind,fetch",
    [
        (
            VIEWS,
            lambda gen, schema: gen.fetch_views_for_schema(
                schema, "TEST_DB", "SCHEMA_A"
            ),
        ),
        (STREAMS, lambda gen, schema: gen.fetch_streams_for_schema(schema, "TEST_DB")),
    ],
    ids=["views", "streams"],
)
def test_a_denied_listing_reaches_the_permission_classifier(kind, fetch):
    """Both kinds must let a denial past, because returning [] drops the schema's objects
    while the run still exits clean - and stale-entity removal stands down only on a
    failure, which get_workunits_internal records from this exception. Checked for both
    because streams silently diverged from views here: it recognised the error class well
    enough to reword the warning, and still swallowed it."""
    connection = DeniesEveryShowConnection({kind: [("SCHEMA_A", "obj_a")]})
    schema_gen = _make_schema_gen(connection)
    schema = SnowflakeSchema(
        name="SCHEMA_A", created=None, last_altered=None, comment=None
    )

    with pytest.raises(SnowflakePermissionError):
        fetch(schema_gen, schema)


def test_a_denial_only_on_the_per_schema_fallback_still_propagates():
    """The per-schema handler has its own permission branch, distinct from the database-wide
    probe's, and it is reached only on the fallback: the database-wide query succeeds and
    fills its page, then the schema-scoped retry is denied - a role holding database USAGE
    but not schema USAGE. Driven through get_views_for_schema rather than the per-schema
    fetch directly, because calling that directly never issues the database-wide query and
    so never reaches this handler by the route production takes."""
    connection = DeniesOnlyPerSchemaConnection(
        {VIEWS: [("SCHEMA_A", f"v_{i:05d}") for i in range(SHOW_COMMAND_MAX_PAGE_SIZE)]}
    )
    schema_gen = _make_schema_gen(connection)

    with pytest.raises(SnowflakePermissionError):
        schema_gen.get_views_for_schema("SCHEMA_A", "TEST_DB")

    issued = connection.show_queries(VIEWS)
    assert any("IN DATABASE" in q for q in issued), (
        f"the database-wide probe must have run and filled its page; got {issued}"
    )
    assert any("IN SCHEMA" in q for q in issued), (
        f"the denial must have come from the per-schema fallback; got {issued}"
    )
    assert not schema_gen.report.failures, (
        "the denial must reach the caller's permission classifier, "
        "not be pre-empted by a generic incomplete-listing failure"
    )


def test_a_non_permission_stream_failure_still_degrades_to_a_warning():
    """Only a denial escalates. Anything else must stay the warning-and-return-[] it always
    was, or a transient error would abort the run. Raised from the filter rather than the
    connection because a failing query is already absorbed by the per-schema pager - this
    handler only ever sees what gets past it."""
    connection = FakeShowConnection({STREAMS: [("SCHEMA_A", "stream_a")]})
    schema_gen = _make_schema_gen(connection)
    schema_gen.filters.is_dataset_pattern_allowed.side_effect = ValueError("boom")  # type: ignore[attr-defined]
    schema = SnowflakeSchema(
        name="SCHEMA_A", created=None, last_altered=None, comment=None
    )

    assert schema_gen.fetch_streams_for_schema(schema, "TEST_DB") == []
    titles = [w.title for w in schema_gen.report.warnings]
    assert "Failed to get streams for schema" in titles, (
        f"expected the stream warning; got {titles}"
    )


def test_an_unmappable_row_does_not_re_issue_the_database_wide_query():
    """The mapping runs inside the same try as the query because get_views_for_database is
    cached and serialized_lru_cache does not cache exceptions - a mapper error escaping it
    would re-issue the 10,000-row database-wide SHOW once per schema."""
    connection = ReturnsAnUnmappableRowConnection({VIEWS: [("SCHEMA_A", "view_a")]})
    data_dictionary = _make_data_dictionary(connection)

    assert data_dictionary.get_views_for_database("TEST_DB") is None, (
        "an unmappable row must be answered with 'unusable', not raised"
    )
    assert len(connection.queries) == 1
    assert data_dictionary.report.warnings


def test_a_mid_paging_failure_reports_how_many_rows_were_kept():
    """Failing on page 2 leaves a partial result the caller cannot distinguish from a
    complete one, so the kept count has to reach the report - otherwise a truncated schema
    looks exactly like a small one."""
    total = SHOW_COMMAND_MAX_PAGE_SIZE + 10
    connection = FailsAfterFirstPageConnection(
        {DYNAMIC_TABLES: [("SCHEMA_A", f"dt_{i:05d}") for i in range(total)]}
    )
    data_dictionary = _make_data_dictionary(connection)

    tables = data_dictionary.get_dynamic_tables_for_schema_using_show(
        db_name="TEST_DB", schema_name="SCHEMA_A"
    )

    assert len(tables) == SHOW_COMMAND_MAX_PAGE_SIZE, "page 1 should still be salvaged"
    reported = " ".join(str(f) for f in data_dictionary.report.failures)
    assert f"kept {SHOW_COMMAND_MAX_PAGE_SIZE}" in reported, (
        f"the kept row count must be reported; got {reported!r}"
    )


def test_dynamic_table_fallback_skips_schemas_without_dynamic_tables():
    """The per-schema fallback costs one query per schema, so it must only visit schemas
    that actually hold a dynamic table awaiting a definition."""
    connection = FakeShowConnection(
        {DYNAMIC_TABLES: _boundary_skips_a_schema(SHOW_COMMAND_MAX_PAGE_SIZE)}
    )
    data_dictionary = _make_data_dictionary(connection)

    plain_table = SnowflakeTable(
        name="plain",
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
    )
    data_dictionary.populate_dynamic_table_definitions(
        {
            "SCHEMA_B": [_dynamic_table("a_000")],
            "SCHEMA_NO_DT": [plain_table],
        },
        "TEST_DB",
    )

    per_schema = [
        q for q in connection.show_queries(DYNAMIC_TABLES) if "IN SCHEMA" in q
    ]
    # The negative alone is vacuously true if the fallback visits nothing at all, so pin
    # the positive too: the schema that does need definitions must be queried.
    assert any("SCHEMA_B" in q for q in per_schema), (
        f"the schema holding a dynamic table was not queried: {per_schema}"
    )
    assert all("SCHEMA_NO_DT" not in q for q in per_schema), (
        f"queried a schema with no dynamic tables: {per_schema}"
    )


class GraphHistoryConnection(FakeShowConnection):
    """Answers the dynamic-table graph-history query as Snowflake does, so the producer of
    that mapping and the consumer of it are exercised together - the seam where a key-format
    mismatch hides when each side is tested alone."""

    def __init__(
        self,
        objects: Dict[SnowflakeShowKind, List[Tuple[str, str]]],
        graph_rows: List[Dict[str, Any]],
    ) -> None:
        super().__init__(objects)
        self._graph_rows = graph_rows

    def query(self, query: str) -> List[Dict[str, Any]]:
        if "DYNAMIC_TABLE_GRAPH_HISTORY" in query:
            self.queries.append(query)
            return self._graph_rows
        return super().query(query)


def test_dynamic_table_upstreams_come_through_the_graph_history():
    """The graph history carries a dynamic table's INPUTS, which is where its upstream
    lineage and its fallback target lag come from. Verified against a live account: the
    function returns NAME, SCHEMA_NAME and DATABASE_NAME per row."""
    connection = GraphHistoryConnection(
        {DYNAMIC_TABLES: [("SCHEMA_A", "dt_a")]},
        graph_rows=[
            {
                "NAME": "dt_a",
                "SCHEMA_NAME": "SCHEMA_A",
                "DATABASE_NAME": "TEST_DB",
                "INPUTS": '[{"kind": "TABLE", "name": "TEST_DB.SCHEMA_A.base"}]',
                "TARGET_LAG_TYPE": "USER_DEFINED",
                "TARGET_LAG_SEC": 3600,
                "SCHEDULING_STATE": None,
                "ALTER_TRIGGER": None,
            }
        ],
    )

    tables = _make_data_dictionary(connection).get_dynamic_tables_for_schema_using_show(
        db_name="TEST_DB", schema_name="SCHEMA_A"
    )

    assert [t.name for t in tables] == ["dt_a"]
    assert [u.name for u in tables[0].upstream_tables] == ["TEST_DB.SCHEMA_A.base"], (
        "upstream lineage from the graph history was dropped"
    )
    # SHOW already reports a target lag, and it wins; the graph value is only a fallback
    # for when it doesn't.
    assert tables[0].target_lag == "1 hour"


def test_dynamic_table_graph_info_does_not_take_another_databases_row():
    """DYNAMIC_TABLE_GRAPH_HISTORY is account-scoped: invoked from one database's
    INFORMATION_SCHEMA it returns rows for every database in the account - measured live, 12
    rows spanning three other databases and none from the one it was invoked from. So a row
    must be keyed by its own DATABASE_NAME; stamping the caller's would let a same-named
    schema.table in a different database supply this one's lineage."""
    connection = GraphHistoryConnection(
        {DYNAMIC_TABLES: [("SCHEMA_A", "dt_a")]},
        graph_rows=[
            {
                "NAME": "dt_a",
                "SCHEMA_NAME": "SCHEMA_A",
                "DATABASE_NAME": "TEST_DB",
                "INPUTS": '[{"kind": "TABLE", "name": "TEST_DB.SCHEMA_A.right"}]',
                "TARGET_LAG_TYPE": "USER_DEFINED",
                "TARGET_LAG_SEC": 3600,
                "SCHEDULING_STATE": None,
                "ALTER_TRIGGER": None,
            },
            # Listed AFTER the correct row deliberately: with the caller's db_name stamped
            # on every row the two collide and the last one wins, so this ordering is what
            # makes the test fail when the key is wrong.
            {
                "NAME": "dt_a",
                "SCHEMA_NAME": "SCHEMA_A",
                "DATABASE_NAME": "OTHER_DB",
                "INPUTS": '[{"kind": "TABLE", "name": "OTHER_DB.SCHEMA_A.wrong"}]',
                "TARGET_LAG_TYPE": "USER_DEFINED",
                "TARGET_LAG_SEC": 1,
                "SCHEDULING_STATE": None,
                "ALTER_TRIGGER": None,
            },
        ],
    )

    tables = _make_data_dictionary(connection).get_dynamic_tables_for_schema_using_show(
        db_name="TEST_DB", schema_name="SCHEMA_A"
    )

    assert [u.name for u in tables[0].upstream_tables] == ["TEST_DB.SCHEMA_A.right"], (
        "lineage was taken from a same-named dynamic table in a different database"
    )


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
