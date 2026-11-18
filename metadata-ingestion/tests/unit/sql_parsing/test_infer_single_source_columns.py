from typing import Optional

import pytest
import sqlglot
from sqlglot.optimizer.qualify import qualify
from sqlglot.schema import ensure_schema

from datahub.sql_parsing._infer_single_source_columns import (
    infer_single_source_columns,
)


def _qualify(sql: str, schema: Optional[dict] = None) -> str:
    sqlglot_schema = ensure_schema(schema or {})
    qualified = qualify(
        sqlglot.parse_one(sql),
        schema=sqlglot_schema,
        validate_qualify_columns=False,
        allow_partial_qualification=True,
        identify=False,
    )
    return infer_single_source_columns(qualified, sqlglot_schema).sql()


@pytest.mark.parametrize(
    "sql, expected",
    [
        pytest.param(
            "SELECT a FROM t",
            "SELECT t.a AS a FROM t AS t",
            id="single_table",
        ),
        pytest.param(
            "WITH c AS (SELECT a, k FROM t2) SELECT t1.x, c.a FROM t1 JOIN c ON t1.k = c.k",
            "WITH c AS (SELECT t2.a AS a, t2.k AS k FROM t2 AS t2) "
            "SELECT t1.x AS x, c.a AS a FROM t1 AS t1 JOIN c AS c ON t1.k = c.k",
            id="cte_joined_to_table",
        ),
        pytest.param(
            # The CTE's columns are known, so t1 is the only source without schema.
            "WITH c AS (SELECT a, id FROM t2) SELECT x, c.a FROM t1 JOIN c ON t1.id = c.id",
            "WITH c AS (SELECT t2.a AS a, t2.id AS id FROM t2 AS t2) "
            "SELECT t1.x AS x, c.a AS a FROM t1 AS t1 JOIN c AS c ON t1.id = c.id",
            id="sole_schemaless_source_among_several",
        ),
        pytest.param(
            "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id",
            "SELECT a AS a, b AS b FROM t1 AS t1 JOIN t2 AS t2 ON t1.id = t2.id",
            id="ambiguous_between_two_tables",
        ),
        pytest.param(
            "SELECT a + 1 AS x FROM t ORDER BY x",
            "SELECT t.a + 1 AS x FROM t AS t ORDER BY x",
            id="order_by_projection_alias",
        ),
        pytest.param(
            "SELECT s.f, a FROM t",
            "SELECT t.s.f AS f, t.a AS a FROM t AS t",
            id="struct_field",
        ),
        pytest.param(
            # t1.k in the subquery is the outer table's column, not a struct path.
            "SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k)",
            "SELECT t1.a AS a FROM t1 AS t1 "
            'WHERE EXISTS(SELECT 1 AS "1" FROM t2 AS t2 WHERE t2.k = t1.k)',
            id="correlated_outer_column",
        ),
        pytest.param(
            # Once inferred, the parent scope must not still see `c` as an outer
            # column and turn `t2.c` into a struct path of t1.
            "SELECT t1.a FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.b = t1.k AND c > 0)",
            "SELECT t1.a AS a FROM t1 AS t1 "
            'WHERE EXISTS(SELECT 1 AS "1" FROM t2 AS t2 WHERE t2.b = t1.k AND t2.c > 0)',
            id="correlated_subquery_inferred_column",
        ),
        pytest.param(
            # qualify gives the lateral's `y` the lateral's own alias.
            "SELECT a, l.y FROM t, LATERAL (SELECT y FROM u WHERE u.k = t.k) AS l",
            "SELECT t.a AS a, l.y AS y FROM t AS t, "
            "LATERAL (SELECT u.y AS y FROM u AS u WHERE u.k = t.k) AS l",
            id="lateral_subquery",
        ),
        pytest.param(
            # `a` is also a projection, but a window's ORDER BY can't name one.
            "SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM t",
            "SELECT t.a AS a, ROW_NUMBER() OVER (ORDER BY t.a) AS rn FROM t AS t",
            id="window_order_by_is_not_projection_alias",
        ),
    ],
)
def test_infers_sole_schemaless_source(sql: str, expected: str) -> None:
    assert _qualify(sql) == expected


def test_skips_partial_schema() -> None:
    # sqlglot 30.21 only inferred with an entirely empty schema.
    assert (
        _qualify("SELECT a FROM t1 CROSS JOIN t2", schema={"t2": {"b": "INT"}})
        == "SELECT a AS a FROM t1 AS t1 CROSS JOIN t2 AS t2"
    )
