import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.m_query.native_sql_parser import (
    _has_real_semicolons,
    _insert_statement_separators,
    parse_custom_sql,
    remove_drop_statement,
    remove_special_characters,
)

# ---------------------------------------------------------------------------
# Shared SQL fixture used by both the _insert_statement_separators structural
# test and the end-to-end parse_custom_sql regression test.
#
# Key structural properties exercised:
#   - Nested WITH (a WITH clause inside the body of an outer CTE)
#   - Blank lines before every SELECT, including those inside CTE bodies
#   - Semicolon inside a SQL comment (must not trigger multi-statement parsing)
#   - Multiple blank lines before the final depth-0 SELECT
#
# These properties combine to produce the bug where the outer CTE alias was
# mistakenly emitted as a real upstream table: blank-line SELECT detection
# split the query, leaving the final SELECT without its CTE definitions.
# ---------------------------------------------------------------------------

_NESTED_CTE_SQL = """\
-- Find merged records; see ticket #42

With outer_cte as (
with inner_cte_a as
(

SELECT
      a.id as record_id
      , count(a.grp) as grp_count

FROM

    db_a.schema_a.table_a a

group by
    a.id

),
inner_cte_b as
(

SELECT
      b.id as record_id
      , count(b.grp) as grp_count_b

FROM

    db_b.schema_b.table_b b

group by
    b.id

)
SELECT
    a.record_id
    , a.grp_count
    , b.grp_count_b
FROM
    inner_cte_a a
      left join inner_cte_b b
        on a.record_id = b.record_id
GROUP BY
    a.record_id
    , a.grp_count
    , b.grp_count_b
HAVING a.grp_count = b.grp_count_b
)
,
cte_c as
(
  SELECT distinct
     c.id as record_id
    , c.val
FROM
    db_a.schema_a.table_a c
)
,
cte_d as
(
    SELECT
     d.record_id
FROM
    cte_c d
group by
      d.record_id
having count(d.record_id) = 1
)



SELECT
    e.id
    , f.val

FROM

    outer_cte oc
      left join cte_d d
        on oc.record_id = d.record_id
      left join db_c.schema_c.table_c e
        on d.record_id = e.id

where e.is_active = 'TRUE'"""

# ---------------------------------------------------------------------------
# _has_real_semicolons
# ---------------------------------------------------------------------------


def test_has_real_semicolons_false_for_comment_only():
    sql = "-- This query is done; next step\nSELECT 1"
    assert _has_real_semicolons(sql) is False


def test_has_real_semicolons_false_for_block_comment():
    sql = "/* done; continue */ SELECT 1"
    assert _has_real_semicolons(sql) is False


def test_has_real_semicolons_true_for_real_semicolon():
    sql = "SELECT 1;\nSELECT 2"
    assert _has_real_semicolons(sql) is True


def test_has_real_semicolons_false_for_string_literal():
    sql = "SELECT 'status; pending' AS status FROM t"
    assert _has_real_semicolons(sql) is False


def test_has_real_semicolons_true_when_mixed():
    # Real semicolon present even though a comment also has one
    sql = "-- comment; info\nSELECT 1;\nSELECT 2"
    assert _has_real_semicolons(sql) is True


# ---------------------------------------------------------------------------
# _insert_statement_separators
# ---------------------------------------------------------------------------


def test_insert_separators_does_not_add_inside_cte_body():
    """Blank lines before SELECT inside a CTE body must not produce a separator."""
    sql = "WITH cte AS (\n\nSELECT 1 AS x FROM source\n)\nSELECT * FROM cte"
    result = _insert_statement_separators(sql)
    assert ";" not in result


def test_insert_separators_does_not_split_cte_final_select():
    """The final SELECT of a CTE query must not be separated from the WITH clause."""
    sql = "WITH cte AS (SELECT 1 AS x FROM source)\n\n\nSELECT * FROM cte"
    result = _insert_statement_separators(sql)
    assert ";" not in result


def test_insert_separators_cte_no_blank_before_final_select():
    """CTE closing SELECT with no blank line — flag must reset so the next
    blank-line-separated SELECT still gets a separator."""
    sql = "WITH cte AS (SELECT 1 AS x FROM t1)\nSELECT * FROM cte\n\n\nSELECT id FROM other"
    result = _insert_statement_separators(sql)
    assert ";" in result
    stmts = [s.strip() for s in result.split(";") if s.strip()]
    assert len(stmts) == 2


def test_insert_separators_adds_between_standalone_selects():
    """Two blank-line-separated standalone SELECTs should get a separator."""
    sql = "SELECT 1 AS x FROM t1\n\n\nSELECT 2 AS y FROM t2"
    result = _insert_statement_separators(sql)
    assert ";" in result
    stmts = [s.strip() for s in result.split(";") if s.strip()]
    assert len(stmts) == 2


def test_insert_separators_block_comment_unbalanced_paren_same_line():
    """Block comment with unbalanced '(' on the same line must not corrupt depth."""
    sql = "/* setup (config */ SELECT 1 AS x FROM t1\n\n\nSELECT 2 AS y FROM t2"
    result = _insert_statement_separators(sql)
    assert ";" in result
    stmts = [s.strip() for s in result.split(";") if s.strip()]
    assert len(stmts) == 2


def test_insert_separators_block_comment_unbalanced_paren_multi_line():
    """Block comment spanning multiple lines with unbalanced parens must not
    corrupt depth tracking — the in_block_comment flag persists across lines."""
    sql = (
        "/* multi-line\n"
        "   comment (with paren\n"
        "   still in comment */\n"
        "SELECT 1 AS x FROM t1\n\n\nSELECT 2 AS y FROM t2"
    )
    result = _insert_statement_separators(sql)
    assert ";" in result
    stmts = [s.strip() for s in result.split(";") if s.strip()]
    assert len(stmts) == 2


def test_insert_separators_nested_cte_with_comment_semicolon():
    """No separator should be inserted anywhere — the whole query is one statement.

    Uses _NESTED_CTE_SQL which contains all three properties that previously
    triggered incorrect splitting: nested CTEs, blank lines before SELECTs, and
    a comment semicolon.
    """
    result = _insert_statement_separators(_NESTED_CTE_SQL)
    # The function must not have inserted any new statement separators.
    # (The original SQL has a ";" inside the comment, which must be preserved unchanged.)
    assert result == _NESTED_CTE_SQL, (
        f"Query should be returned unchanged, but got:\n{result}"
    )


# ---------------------------------------------------------------------------
# parse_custom_sql – end-to-end correctness
# ---------------------------------------------------------------------------


@pytest.fixture
def pipeline_ctx() -> PipelineContext:
    return PipelineContext(run_id="test")


def test_parse_real_semicolons_path(pipeline_ctx: PipelineContext):
    """Query with real semicolons takes the _has_real_semicolons=True path."""
    sql = "SELECT id FROM db_a.schema_a.table_a;\nSELECT val FROM db_b.schema_b.table_b"
    result = parse_custom_sql(
        ctx=pipeline_ctx,
        query=sql,
        schema=None,
        database="test_db",
        platform="redshift",
        env="PROD",
        platform_instance=None,
    )
    assert result is not None
    in_tables = set(result.in_tables)
    assert any("table_a" in urn for urn in in_tables)
    assert any("table_b" in urn for urn in in_tables)


def test_parse_blank_line_separated_statements(pipeline_ctx: PipelineContext):
    """Blank-line-separated SELECTs (no real semicolons) get separators inserted
    and are parsed via create_lineage_from_sql_statements."""
    sql = "SELECT id FROM db_a.schema_a.table_a\n\n\nSELECT val FROM db_b.schema_b.table_b"
    result = parse_custom_sql(
        ctx=pipeline_ctx,
        query=sql,
        schema=None,
        database="test_db",
        platform="redshift",
        env="PROD",
        platform_instance=None,
    )
    assert result is not None
    in_tables = set(result.in_tables)
    assert any("table_a" in urn for urn in in_tables)
    assert any("table_b" in urn for urn in in_tables)


def test_parse_nested_cte_no_cte_alias_in_upstreams(pipeline_ctx: PipelineContext):
    """Regression test: CTE aliases must NOT appear in upstream URNs.

    Runs _NESTED_CTE_SQL through the full production pre-processing sequence
    (remove_special_characters → remove_drop_statement → parse_custom_sql) to
    guard against the bug where the outer CTE alias leaked into upstream URNs.
    """
    # Mirror the production call sequence in OdbcLineage.query_lineage
    sql = remove_special_characters(_NESTED_CTE_SQL)
    sql = remove_drop_statement(sql)

    result = parse_custom_sql(
        ctx=pipeline_ctx,
        query=sql,
        schema=None,
        database="test_db",
        platform="redshift",
        env="PROD",
        platform_instance=None,
    )

    assert result is not None
    in_tables = set(result.in_tables)

    # CTE aliases must NOT appear as upstreams
    assert not any("outer_cte" in urn for urn in in_tables), (
        f"CTE alias leaked into upstreams: {in_tables}"
    )
    assert not any("cte_d" in urn for urn in in_tables), (
        f"CTE alias leaked into upstreams: {in_tables}"
    )
    assert not any("inner_cte_a" in urn for urn in in_tables), (
        f"CTE alias leaked into upstreams: {in_tables}"
    )

    # Real source tables must appear
    assert any("table_a" in urn for urn in in_tables), (
        f"Real table missing from upstreams: {in_tables}"
    )
    assert any("table_b" in urn for urn in in_tables), (
        f"Real table missing from upstreams: {in_tables}"
    )
    assert any("table_c" in urn for urn in in_tables), (
        f"Real table missing from upstreams: {in_tables}"
    )
