"""Unit tests for Snowflake interactive table discovery."""

from datetime import datetime, timezone
from typing import Dict, List, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
import snowflake.connector.errors as snowflake_connector_errors

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.constants import (
    SnowflakeObjectDomain,
    SnowflakeShowKind,
)
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeIdentifierConfig,
    TagOption,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_query import (
    SHOW_COMMAND_MAX_PAGE_SIZE,
    SnowflakeQuery,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeDataDictionary,
    SnowflakeDynamicTable,
    SnowflakeInteractiveTable,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
    SnowsightUrlBuilder,
)
from datahub.metadata.schema_classes import UpstreamLineageClass
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    QueryLogSetting,
    SqlParsingAggregator,
)
from tests.unit.snowflake.conftest import (
    CREATED,
    DB,
    SCHEMA,
    QueryHandler,
    fake_connection,
    make_gen,
    table,
)
from tests.unit.snowflake.test_snowflake_show_pagination import (
    FakeShowConnection,
    _boundary_skips_a_schema,
    _make_data_dictionary,
)

_DEFAULT_TABLE_TYPES = {"BASE TABLE", "EXTERNAL TABLE", "INTERACTIVE TABLE"}
INTERACTIVE_TABLES = SnowflakeShowKind.INTERACTIVE_TABLES
SCHEMA_A = "SCHEMA_A"
SCHEMA_B = "SCHEMA_B"
STATIC_DDL = (
    "CREATE OR REPLACE INTERACTIVE TABLE INT_STATIC CLUSTER BY (COL_1) "
    "AS SELECT * FROM TEST_DB.PUBLIC.SOURCE"
)
DYNAMIC_IT_DDL = (
    "create or replace dynamic table INT_DYN cluster by (COL_1) "
    "target_lag = '1 hour' warehouse = WH as "
    "select * from TEST_DB.TEST_SCHEMA.TABLE_1"
)
GET_DDL_DYNAMIC_IT = (
    "CREATE INTERACTIVE TABLE INT_DYN TARGET_LAG = '1 hour' WAREHOUSE = W "
    "AS SELECT * FROM TEST_DB.TEST_SCHEMA.TABLE_1"
)


def _schema_gen_with_mocks() -> SnowflakeSchemaGenerator:
    gen = SnowflakeSchemaGenerator.__new__(SnowflakeSchemaGenerator)
    gen.aggregator = MagicMock()
    gen.report = SnowflakeV2Report()
    gen.dynamic_table_identifiers = set()
    gen.identifiers = MagicMock()
    gen.identifiers.get_dataset_identifier = lambda name, schema, db: (
        f"{db}.{schema}.{name}".lower()
    )
    gen.identifiers.gen_dataset_urn = lambda ident: f"urn:li:dataset:({ident})"
    gen.filters = MagicMock()
    gen.filters.is_dataset_pattern_allowed = lambda ident, domain: True
    return gen


def _interactive_with_definition(
    name: str,
    *,
    definition: str | None,
    target_lag: str | None = None,
    is_dynamic: bool = False,
) -> SnowflakeInteractiveTable:
    return SnowflakeInteractiveTable(
        name=name,
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=0,
        rows_count=0,
        type="INTERACTIVE TABLE",
        is_dynamic=is_dynamic,
        definition=definition,
        target_lag=target_lag,
    )


def _interactive_table(name: str) -> SnowflakeInteractiveTable:
    return SnowflakeInteractiveTable(
        name=name,
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=0,
        rows_count=0,
        type="INTERACTIVE TABLE",
    )


def _make_dictionary(dispatch: QueryHandler) -> SnowflakeDataDictionary:
    return SnowflakeDataDictionary(fake_connection(dispatch), SnowflakeV2Report())


def _table_row(name: str, **overrides: object) -> dict:
    row = {
        "TABLE_SCHEMA": SCHEMA,
        "TABLE_NAME": name,
        "TABLE_TYPE": "INTERACTIVE TABLE",
        "CREATED": CREATED,
        "LAST_ALTERED": CREATED,
        "BYTES": 10,
        "ROW_COUNT": 3,
        "COMMENT": "interactive comment",
        "CLUSTERING_KEY": "LINEAR(COL_1)",
        "IS_DYNAMIC": "NO",
        "IS_ICEBERG": "NO",
        "IS_HYBRID": "NO",
        "RETENTION_TIME": 1,
    }
    row.update(overrides)
    return row


def _map_database_rows(rows: List[dict]) -> Dict[str, List[SnowflakeTable]]:
    def dispatch(sql: str) -> List[dict]:
        if "DYNAMIC_TABLE" in sql.upper() or "SHOW DYNAMIC" in sql.upper():
            return []
        return rows

    result = _make_dictionary(dispatch).get_tables_for_database(
        DB,
        table_types=frozenset(_DEFAULT_TABLE_TYPES),
        exclude_dynamic_tables=False,
    )
    assert result is not None
    return result


def test_interactive_row_maps_to_interactive_table_class() -> None:
    tables = _map_database_rows([_table_row("INT_TAB")])
    table = tables[SCHEMA][0]
    assert isinstance(table, SnowflakeInteractiveTable)
    assert not isinstance(table, SnowflakeDynamicTable)
    assert table.get_subtype() == DatasetSubTypes.INTERACTIVE_TABLE
    assert table.clustering_key == "LINEAR(COL_1)"
    assert table.comment == "interactive comment"
    assert table.type == "INTERACTIVE TABLE"


def test_interactive_row_with_is_dynamic_is_interactive() -> None:
    tables = _map_database_rows(
        [_table_row("INT_DYN", IS_DYNAMIC="YES", TABLE_TYPE="INTERACTIVE TABLE")]
    )
    table = tables[SCHEMA][0]
    assert isinstance(table, SnowflakeInteractiveTable)
    assert not isinstance(table, SnowflakeDynamicTable)
    assert table.is_dynamic is True
    assert table.get_subtype() == DatasetSubTypes.INTERACTIVE_TABLE


def test_base_table_is_dynamic_stays_dynamic_table() -> None:
    row = _table_row("REG_DYN", TABLE_TYPE="BASE TABLE", IS_DYNAMIC="YES")

    def dispatch(sql: str) -> List[dict]:
        if "SHOW DYNAMIC" in sql.upper():
            return []
        return [row]

    tables = _make_dictionary(dispatch).get_tables_for_schema(
        SCHEMA, DB, table_types=_DEFAULT_TABLE_TYPES, exclude_dynamic_tables=False
    )
    assert len(tables) == 1
    assert isinstance(tables[0], SnowflakeDynamicTable)
    assert not isinstance(tables[0], SnowflakeInteractiveTable)


def test_no_interactive_rows_issue_no_new_query() -> None:
    issued: List[str] = []

    def dispatch(sql: str) -> List[dict]:
        issued.append(sql)
        if "SHOW INTERACTIVE" in sql.upper() or "GET_DDL" in sql.upper():
            pytest.fail(f"unexpected enrichment query: {sql}")
        return [
            {
                "TABLE_SCHEMA": SCHEMA,
                "TABLE_NAME": "BASE_ONLY",
                "TABLE_TYPE": "BASE TABLE",
                "CREATED": CREATED,
                "LAST_ALTERED": CREATED,
                "BYTES": 1,
                "ROW_COUNT": 1,
                "COMMENT": None,
                "CLUSTERING_KEY": None,
                "IS_DYNAMIC": "NO",
                "IS_ICEBERG": "NO",
                "IS_HYBRID": "NO",
                "RETENTION_TIME": 1,
            }
        ]

    _make_dictionary(dispatch).get_tables_for_schema(
        SCHEMA, DB, table_types=_DEFAULT_TABLE_TYPES, exclude_dynamic_tables=False
    )


def test_dynamic_interactive_table_gets_show_definition_and_lag() -> None:
    connection = FakeShowConnection({INTERACTIVE_TABLES: [(SCHEMA, "INT_DYN")]})
    issued: List[str] = []

    def dispatch(sql: str) -> List[dict]:
        issued.append(sql)
        if "GET_DDL" in sql.upper():
            pytest.fail(f"dynamic interactive table should not call GET_DDL: {sql}")
        if sql.lstrip().startswith("SHOW "):
            return connection.query(sql)
        if "information_schema.tables" in sql.lower():
            return [_table_row("INT_DYN", IS_DYNAMIC="YES")]
        return []

    dictionary = _make_dictionary(dispatch)
    tables = dictionary.get_tables_for_schema(
        SCHEMA, DB, table_types=_DEFAULT_TABLE_TYPES, exclude_dynamic_tables=False
    )
    table = tables[0]
    assert isinstance(table, SnowflakeInteractiveTable)
    assert table.definition == "CREATE DYNAMIC TABLE INT_DYN AS SELECT 1"
    assert table.target_lag == "1 hour"
    assert not any("GET_DDL" in q for q in issued)


def test_get_table_ddl_escapes_embedded_quotes_in_identifiers() -> None:
    """GET_DDL's object name is a single-quoted literal around a quoted identifier."""
    sql = SnowflakeQuery.get_table_ddl('DB"X', 'SCHEMA"A', "tab'le")
    assert (
        sql == """SELECT GET_DDL('TABLE', '"DB""X"."SCHEMA""A"."tab''le"') AS "DDL";"""
    )


# Snowflake hides the definition from non-owner roles as an empty string (seen on a live account).
@pytest.mark.parametrize("show_text", [None, ""])
@pytest.mark.parametrize("is_dynamic", [True, False])
def test_get_ddl_only_for_dynamic_interactive_table(
    show_text: Optional[str], is_dynamic: bool
) -> None:
    issued: List[str] = []

    def dispatch(sql: str) -> List[dict]:
        issued.append(sql)
        if "GET_DDL" in sql.upper():
            return [{"DDL": GET_DDL_DYNAMIC_IT}]
        if sql.lstrip().startswith("SHOW "):
            return [
                {
                    "name": "INT_IT",
                    "schema_name": SCHEMA,
                    "created_on": CREATED,
                    "comment": None,
                    "text": show_text,
                    "target_lag": "1 hour" if is_dynamic else None,
                    "bytes": 0,
                    "rows": 0,
                }
            ]
        if "information_schema.tables" in sql.lower():
            return [_table_row("INT_IT", IS_DYNAMIC="YES" if is_dynamic else "NO")]
        return []

    tables = _make_dictionary(dispatch).get_tables_for_schema(
        SCHEMA, DB, table_types=_DEFAULT_TABLE_TYPES, exclude_dynamic_tables=False
    )
    table = tables[0]
    assert isinstance(table, SnowflakeInteractiveTable)
    # Static GET_DDL has no AS SELECT, so it is never issued; static lineage comes from the query log.
    assert any("GET_DDL" in q for q in issued) == is_dynamic
    assert table.definition == (GET_DDL_DYNAMIC_IT if is_dynamic else None)


def test_database_show_runs_once_on_schema_path() -> None:
    connection = FakeShowConnection(
        {
            INTERACTIVE_TABLES: [
                (SCHEMA_A, "int_a"),
                (SCHEMA_B, "int_b"),
            ]
        }
    )

    def dispatch(sql: str) -> List[dict]:
        if sql.lstrip().startswith("SHOW "):
            return connection.query(sql)
        if "information_schema.tables" in sql.lower():
            if f"'{SCHEMA_A}'" in sql or f'"{SCHEMA_A}"' in sql:
                return [_table_row("int_a", TABLE_SCHEMA=SCHEMA_A, TABLE_NAME="int_a")]
            return [_table_row("int_b", TABLE_SCHEMA=SCHEMA_B, TABLE_NAME="int_b")]
        return []

    dictionary = SnowflakeDataDictionary(fake_connection(dispatch), SnowflakeV2Report())
    dictionary.get_tables_for_schema(
        SCHEMA_A, DB, table_types=_DEFAULT_TABLE_TYPES, exclude_dynamic_tables=False
    )
    dictionary.get_tables_for_schema(
        SCHEMA_B, DB, table_types=_DEFAULT_TABLE_TYPES, exclude_dynamic_tables=False
    )

    db_wide = [
        q for q in connection.show_queries(INTERACTIVE_TABLES) if "IN DATABASE" in q
    ]
    assert len(db_wide) == 1


def test_full_page_falls_back_per_schema() -> None:
    connection = FakeShowConnection(
        {INTERACTIVE_TABLES: _boundary_skips_a_schema(SHOW_COMMAND_MAX_PAGE_SIZE)}
    )
    table = _interactive_table("a_000")

    dictionary = _make_data_dictionary(connection)
    dictionary.populate_interactive_table_definitions({SCHEMA_B: [table]}, DB)

    assert table.definition == "CREATE DYNAMIC TABLE a_000 AS SELECT 1"
    schema_queries = [
        q for q in connection.show_queries(INTERACTIVE_TABLES) if "IN SCHEMA" in q
    ]
    assert schema_queries, "expected a per-schema SHOW after a full database page"


def test_show_permission_error_keeps_tables() -> None:
    def dispatch(sql: str) -> List[dict]:
        if "SHOW INTERACTIVE TABLES" in sql.upper():
            raise SnowflakePermissionError(Exception("Insufficient privileges"))
        if "information_schema.tables" in sql.lower():
            return [_table_row("INT_TAB")]
        return []

    dictionary = _make_dictionary(dispatch)
    tables = dictionary.get_tables_for_schema(
        SCHEMA, DB, table_types=_DEFAULT_TABLE_TYPES, exclude_dynamic_tables=False
    )
    assert len(tables) == 1
    assert isinstance(tables[0], SnowflakeInteractiveTable)
    assert tables[0].definition is None
    assert any("interactive" in w.message.lower() for w in dictionary.report.warnings)


def test_get_ddl_error_is_warned_and_counted() -> None:
    ddl_query = SnowflakeQuery.get_table_ddl(DB, SCHEMA, "INT_DYN")

    def dispatch(sql: str) -> List[dict]:
        if sql == ddl_query:
            raise snowflake_connector_errors.ProgrammingError(
                msg="GET_DDL is not authorized"
            )
        if "SHOW INTERACTIVE TABLES" in sql.upper():
            return []
        if "information_schema.tables" in sql.lower():
            return [_table_row("INT_DYN", IS_DYNAMIC="YES")]
        return []

    dictionary = _make_dictionary(dispatch)
    tables = dictionary.get_tables_for_schema(
        SCHEMA, DB, table_types=_DEFAULT_TABLE_TYPES, exclude_dynamic_tables=False
    )
    assert len(tables) == 1
    table = tables[0]
    assert isinstance(table, SnowflakeInteractiveTable)
    assert table.definition is None
    assert dictionary.report.num_interactive_tables_missing_from_show == 1
    assert dictionary.report.num_interactive_tables_failed_get_ddl == 1
    assert any("DDL" in w.message for w in dictionary.report.warnings)


def test_missing_show_row_is_counted() -> None:
    connection = FakeShowConnection({INTERACTIVE_TABLES: []})
    table = _interactive_table("MISSING")

    report = SnowflakeV2Report()
    dictionary = SnowflakeDataDictionary(fake_connection(connection.query), report)
    dictionary.populate_interactive_table_definitions({SCHEMA: [table]}, DB)

    assert table.definition is None
    assert report.num_interactive_tables_missing_from_show == 1


def test_register_interactive_static_definition_rewritten_for_lineage() -> None:
    gen = _schema_gen_with_mocks()
    table = _interactive_with_definition("INT_STATIC", definition=STATIC_DDL)
    gen._register_interactive_table_upstreams(table, DB, SCHEMA)

    aggregator = cast(MagicMock, gen.aggregator)
    aggregator.add_view_definition.assert_not_called()
    aggregator.add_observed_query.assert_called_once()
    observed = aggregator.add_observed_query.call_args.args[0]
    query_text = observed.query
    assert "INTERACTIVE TABLE" not in query_text.upper()
    assert "TABLE INT_STATIC" in query_text.upper()


def test_register_interactive_dynamic_definition_passes_show_text() -> None:
    gen = _schema_gen_with_mocks()
    table = _interactive_with_definition(
        "INT_DYN",
        definition=DYNAMIC_IT_DDL,
        target_lag="1 hour",
        is_dynamic=True,
    )
    gen._register_interactive_table_upstreams(table, DB, SCHEMA)

    aggregator = cast(MagicMock, gen.aggregator)
    aggregator.add_view_definition.assert_called_once()
    assert aggregator.add_view_definition.call_args.kwargs["view_definition"] == (
        DYNAMIC_IT_DDL
    )


def test_register_interactive_is_dynamic_alone_uses_view_definition() -> None:
    gen = _schema_gen_with_mocks()
    table = _interactive_with_definition(
        "INT_DYN", definition=STATIC_DDL, target_lag=None, is_dynamic=True
    )
    gen._register_interactive_table_upstreams(table, DB, SCHEMA)

    aggregator = cast(MagicMock, gen.aggregator)
    aggregator.add_view_definition.assert_called_once()
    aggregator.add_observed_query.assert_not_called()


def test_register_interactive_dynamic_get_ddl_form_uses_view_definition() -> None:
    gen = _schema_gen_with_mocks()
    table = _interactive_with_definition(
        "INT_DYN",
        definition=GET_DDL_DYNAMIC_IT,
        target_lag=None,
        is_dynamic=True,
    )
    gen._register_interactive_table_upstreams(table, DB, SCHEMA)

    aggregator = cast(MagicMock, gen.aggregator)
    aggregator.add_view_definition.assert_called_once()
    aggregator.add_observed_query.assert_not_called()
    rewritten = aggregator.add_view_definition.call_args.kwargs["view_definition"]
    assert "INTERACTIVE TABLE" not in rewritten.upper()


@pytest.mark.parametrize("is_dynamic", [True, False])
def test_register_interactive_without_definition_counts_only_dynamic(
    is_dynamic: bool,
) -> None:
    gen = _schema_gen_with_mocks()
    table = _interactive_with_definition(
        "EMPTY", definition=None, is_dynamic=is_dynamic
    )
    assert not gen._register_interactive_table_upstreams(table, DB, SCHEMA)
    cast(MagicMock, gen.aggregator).add_view_definition.assert_not_called()
    assert gen.report.num_interactive_tables_without_definition == int(is_dynamic)


_COL_1 = SnowflakeColumn(
    name="col_1",
    ordinal_position=1,
    is_nullable=True,
    data_type="NUMBER",
    comment=None,
    character_maximum_length=None,
    numeric_precision=38,
    numeric_scale=0,
)


def test_static_interactive_get_ddl_select_star_yields_column_lineage() -> None:
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.STORE_ALL,
    )
    gen = make_gen(
        include_technical_schema=True,
        extract_tags=TagOption.skip,
        include_primary_keys=False,
        include_foreign_keys=False,
    )
    gen.aggregator = aggregator
    source = table("SOURCE")
    static = _interactive_with_definition("INT_STATIC", definition=STATIC_DDL)
    columns_by_name = {"SOURCE": [_COL_1], "INT_STATIC": [_COL_1]}

    snowflake_schema = MagicMock()
    snowflake_schema.name = "PUBLIC"

    def _pass_through_classification(
        table_wu_generator: object, *_args: object
    ) -> object:
        yield from table_wu_generator  # type: ignore[misc]

    with (
        patch(
            "datahub.ingestion.source.snowflake.snowflake_schema_gen.classification_workunit_processor",
            side_effect=_pass_through_classification,
        ),
        patch.object(
            gen,
            "get_columns_for_table",
            side_effect=lambda name, *_args: columns_by_name[name],
        ),
    ):
        list(
            gen._process_tables(
                [source, static],
                snowflake_schema=snowflake_schema,
                db_name="TEST_DB",
                schema_name="PUBLIC",
            )
        )

    downstream = gen.identifiers.gen_dataset_urn(
        gen.identifiers.get_dataset_identifier("INT_STATIC", "PUBLIC", "TEST_DB")
    )
    fine_grained = None
    for mcp in aggregator.gen_metadata():
        if getattr(mcp, "entityUrn", None) != downstream:
            continue
        aspect = getattr(mcp, "aspect", None)
        if isinstance(aspect, UpstreamLineageClass):
            fine_grained = aspect.fineGrainedLineages
    assert fine_grained


def test_rewritten_get_ddl_yields_upstream() -> None:
    ids = SnowflakeIdentifierBuilder(
        identifier_config=SnowflakeIdentifierConfig(),
        structured_reporter=SourceReport(),
    )
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.STORE_ALL,
    )
    gen = _schema_gen_with_mocks()
    gen.identifiers = ids
    gen.aggregator = aggregator
    gen.report = SnowflakeV2Report()
    gen._register_interactive_table_upstreams(
        _interactive_with_definition("INT_STATIC", definition=STATIC_DDL),
        "TEST_DB",
        "PUBLIC",
    )

    downstream = ids.gen_dataset_urn(
        ids.get_dataset_identifier("INT_STATIC", "PUBLIC", "TEST_DB")
    )
    upstream = ids.gen_dataset_urn(
        ids.get_dataset_identifier_from_qualified_name("TEST_DB.PUBLIC.SOURCE")
    )
    upstreams = None
    for mcp in aggregator.gen_metadata():
        if getattr(mcp, "entityUrn", None) != downstream:
            continue
        aspect = getattr(mcp, "aspect", None)
        if isinstance(aspect, UpstreamLineageClass):
            upstreams = {u.dataset for u in aspect.upstreams}
    assert upstreams == {upstream}


def test_static_interactive_query_log_preferred_over_get_ddl_definition() -> None:
    ids = SnowflakeIdentifierBuilder(
        identifier_config=SnowflakeIdentifierConfig(),
        structured_reporter=SourceReport(),
    )
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        generate_queries=True,
        query_log=QueryLogSetting.STORE_ALL,
    )
    gen = _schema_gen_with_mocks()
    gen.identifiers = ids
    gen.aggregator = aggregator
    gen.report = SnowflakeV2Report()
    gen._register_interactive_table_upstreams(
        _interactive_with_definition("INT_STATIC", definition=STATIC_DDL),
        "TEST_DB",
        "PUBLIC",
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query=(
                "INSERT INTO TEST_DB.PUBLIC.INT_STATIC "
                "SELECT * FROM TEST_DB.PUBLIC.SOURCE"
            ),
            default_db="TEST_DB",
            default_schema="PUBLIC",
            timestamp=datetime(2022, 6, 6, tzinfo=timezone.utc),
        )
    )

    downstream = ids.gen_dataset_urn(
        ids.get_dataset_identifier("INT_STATIC", "PUBLIC", "TEST_DB")
    )
    upstream_lineage = None
    for mcp in aggregator.gen_metadata():
        if getattr(mcp, "entityUrn", None) != downstream:
            continue
        aspect = getattr(mcp, "aspect", None)
        if isinstance(aspect, UpstreamLineageClass):
            upstream_lineage = aspect

    assert upstream_lineage is not None
    assert len(upstream_lineage.upstreams) == 1
    assert upstream_lineage.upstreams[0].type == "TRANSFORMED"
    query_urn = upstream_lineage.upstreams[0].query
    assert query_urn is not None
    assert not query_urn.startswith("urn:li:query:view_")


def test_static_interactive_not_suppressed() -> None:
    gen = _schema_gen_with_mocks()
    gen.config = MagicMock()
    gen.config.include_technical_schema = False
    gen.dynamic_table_identifiers = set()
    table = _interactive_with_definition("INT_STATIC", definition=STATIC_DDL)

    list(
        gen._process_tables(
            [table], snowflake_schema=MagicMock(), db_name=DB, schema_name=SCHEMA
        )
    )

    assert gen.dynamic_table_identifiers == set()


def test_dynamic_interactive_added_to_suppression_set() -> None:
    gen = _schema_gen_with_mocks()
    gen.config = MagicMock()
    gen.config.include_technical_schema = False
    gen.dynamic_table_identifiers = set()
    table = _interactive_with_definition(
        "INT_DYN",
        definition=DYNAMIC_IT_DDL,
        target_lag="1 hour",
        is_dynamic=True,
    )

    list(
        gen._process_tables(
            [table], snowflake_schema=MagicMock(), db_name=DB, schema_name=SCHEMA
        )
    )

    assert gen.dynamic_table_identifiers == {f"{DB}.{SCHEMA}.INT_DYN".lower()}


def test_dynamic_interactive_get_ddl_only_added_to_suppression_set() -> None:
    gen = _schema_gen_with_mocks()
    gen.config = MagicMock()
    gen.config.include_technical_schema = False
    gen.dynamic_table_identifiers = set()
    table = _interactive_with_definition(
        "INT_DYN",
        definition=GET_DDL_DYNAMIC_IT,
        target_lag=None,
        is_dynamic=True,
    )

    list(
        gen._process_tables(
            [table], snowflake_schema=MagicMock(), db_name=DB, schema_name=SCHEMA
        )
    )

    assert gen.dynamic_table_identifiers == {f"{DB}.{SCHEMA}.INT_DYN".lower()}


def test_process_tables_one_failing_observed_query_flush_does_not_drop_siblings() -> (
    None
):
    """A failure flushing one static interactive table's observed query is caught and
    reported so siblings still register -- the post-loop flush has no per-table guard."""
    gen = _schema_gen_with_mocks()
    gen.config = MagicMock()
    gen.config.include_technical_schema = False
    gen.dynamic_table_identifiers = set()
    gen.report = MagicMock()

    bad_ddl = (
        "CREATE OR REPLACE INTERACTIVE TABLE INT_BAD CLUSTER BY (COL_1) "
        "AS SELECT * FROM TEST_DB.PUBLIC.SOURCE"
    )
    good_ddl = (
        "CREATE OR REPLACE INTERACTIVE TABLE INT_GOOD CLUSTER BY (COL_1) "
        "AS SELECT * FROM TEST_DB.PUBLIC.SOURCE2"
    )
    bad = _interactive_with_definition("INT_BAD", definition=bad_ddl)
    good = _interactive_with_definition("INT_GOOD", definition=good_ddl)
    flushed: list[str] = []

    def fake_add_observed(observed: ObservedQuery) -> None:
        flushed.append(observed.query)
        if "INT_BAD" in observed.query.upper():
            raise RuntimeError("flush boom")

    cast(MagicMock, gen.aggregator).add_observed_query = fake_add_observed

    list(
        gen._process_tables(
            [bad, good],
            snowflake_schema=MagicMock(),
            db_name=DB,
            schema_name=SCHEMA,
        )
    )

    assert len(flushed) == 2
    cast(MagicMock, gen.report).warning.assert_called_once()
    warn = cast(MagicMock, gen.report).warning.call_args
    assert warn.args[1] == f"{DB}.{SCHEMA}.INT_BAD"
    assert isinstance(warn.kwargs.get("exc"), RuntimeError)


def test_process_tables_flushes_static_observed_queries_after_process_table_failure() -> (
    None
):
    """Queued static interactive observed queries flush even when table emission aborts."""
    gen = _schema_gen_with_mocks()
    gen.config = MagicMock()
    gen.config.include_technical_schema = True
    gen.dynamic_table_identifiers = set()
    gen.make_data_reader = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
    gen.classification_handler = MagicMock()

    static = _interactive_with_definition("INT_STATIC", definition=STATIC_DDL)
    regular = table("REGULAR")

    def fake_process_table(
        tbl: SnowflakeTable, *_args: object, **_kwargs: object
    ) -> object:
        if tbl.name == "REGULAR":
            raise RuntimeError("emit failed")
        return iter(())

    with (
        patch.object(gen, "_process_table", side_effect=fake_process_table),
        patch(
            "datahub.ingestion.source.snowflake.snowflake_schema_gen.classification_workunit_processor",
            side_effect=lambda wu_gen, *_a: wu_gen,
        ),
        pytest.raises(RuntimeError, match="emit failed"),
    ):
        list(
            gen._process_tables(
                [static, regular],
                snowflake_schema=MagicMock(),
                db_name=DB,
                schema_name=SCHEMA,
            )
        )

    cast(MagicMock, gen.aggregator).add_observed_query.assert_called_once()


def test_interactive_dataset_properties_target_lag_and_table_url() -> None:
    gen = make_gen()
    gen.snowsight_url_builder = SnowsightUrlBuilder(
        account_locator="abc123",
        region="aws_us_west_2",
    )
    table = SnowflakeInteractiveTable(
        name="INT_TAB",
        comment=None,
        created=CREATED,
        last_altered=CREATED,
        size_in_bytes=0,
        rows_count=0,
        type="INTERACTIVE TABLE",
        is_dynamic=True,
        target_lag="1 hour",
    )
    props = gen.get_dataset_properties(table, SCHEMA, DB)
    assert props.customProperties["TARGET_LAG"] == "1 hour"
    external_url = props.externalUrl
    assert external_url is not None
    assert "/table/INT_TAB/" in external_url
    assert "/dynamic-table/" not in external_url

    builder = SnowsightUrlBuilder(account_locator="abc123", region="aws_us_west_2")
    table_url = builder.get_external_url_for_table(
        "INT_TAB", SCHEMA, DB, SnowflakeObjectDomain.TABLE
    )
    assert table_url is not None
    assert table_url in external_url
