# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sql_parsing.sqlglot_utils import detach_ctes


def test_detach_ctes_simple():
    original = "WITH __cte_0 AS (SELECT * FROM table1) SELECT * FROM table2 JOIN __cte_0 ON table2.id = __cte_0.id"
    detached_expr = detach_ctes(
        original,
        platform="snowflake",
        cte_mapping={"__cte_0": "_my_cte_table"},
    )
    detached = detached_expr.sql(dialect="snowflake")

    assert (
        detached
        == "SELECT * FROM table2 JOIN _my_cte_table ON table2.id = _my_cte_table.id"
    )


def test_detach_ctes_with_alias():
    original = "WITH __cte_0 AS (SELECT * FROM table1) SELECT * FROM table2 JOIN __cte_0 AS tablealias ON table2.id = tablealias.id"
    detached_expr = detach_ctes(
        original,
        platform="snowflake",
        cte_mapping={"__cte_0": "_my_cte_table"},
    )
    detached = detached_expr.sql(dialect="snowflake")

    assert (
        detached
        == "SELECT * FROM table2 JOIN _my_cte_table AS tablealias ON table2.id = tablealias.id"
    )


def test_detach_ctes_with_multipart_replacement():
    original = "WITH __cte_0 AS (SELECT * FROM table1) SELECT * FROM table2 JOIN __cte_0 ON table2.id = __cte_0.id"
    detached_expr = detach_ctes(
        original,
        platform="snowflake",
        cte_mapping={"__cte_0": "my_db.my_schema.my_table"},
    )
    detached = detached_expr.sql(dialect="snowflake")

    assert (
        detached
        == "SELECT * FROM table2 JOIN my_db.my_schema.my_table ON table2.id = my_db.my_schema.my_table.id"
    )
