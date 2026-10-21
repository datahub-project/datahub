import pytest

from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

# sqlglot<30.13.0 types a BigQuery UNNEST column by walking every path through
# the CTE graph, so heavily reused CTEs make optimize() effectively hang. A hang
# fails these tests through the timeout instead of stalling the run.
pytestmark = pytest.mark.timeout(60, method="thread")


def _resolver() -> SchemaResolver:
    resolver = SchemaResolver(platform="bigquery")
    resolver.add_raw_schema_info(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.events,PROD)",
        {
            "col_a": "INT64",
            "event_name": "STRING",
            "items": "ARRAY<INT64>",
            "tags": "ARRAY<STRING>",
            "cnt": "INT64",
            "event_params": "ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING>>>",
        },
    )
    return resolver


def _reused_cte_query(levels: int) -> str:
    # Each CTE references the previous one twice, doubling the paths the
    # resolver walks per level. The array is built in a CTE, so its type is not
    # found in any base table and the walk never stops early.
    ctes = ["c0 AS (SELECT col_a FROM my_project.my_dataset.events)"]
    for i in range(1, levels + 1):
        ctes.append(
            f"c{i} AS (SELECT x.col_a FROM c{i - 1} AS x "
            f"JOIN c{i - 1} AS y ON x.col_a = y.col_a)"
        )
    ctes.append(f"body AS (SELECT col_a, [col_a] AS arr FROM c{levels})")
    return (
        "CREATE TABLE my_project.my_dataset.out_table AS WITH "
        + ", ".join(ctes)
        + " SELECT col_a, (SELECT MAX(e) FROM UNNEST(arr) AS e) AS max_e FROM body"
    )


def test_unnest_over_heavily_reused_ctes_skips_column_lineage() -> None:
    result = sqlglot_lineage(
        _reused_cte_query(levels=16),
        schema_resolver=_resolver(),
        default_db="my_project",
        default_schema="my_dataset",
    )
    assert result.in_tables and result.out_tables
    assert result.debug_info.table_error is None
    assert "UNNEST" in str(result.debug_info.column_error)


def test_unnest_over_few_reused_ctes_keeps_column_lineage() -> None:
    result = sqlglot_lineage(
        _reused_cte_query(levels=4),
        schema_resolver=_resolver(),
        default_db="my_project",
        default_schema="my_dataset",
    )
    assert result.debug_info.column_error is None
    assert result.column_lineage


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(
            "SELECT e.event_name, (SELECT value.string_value FROM UNNEST(e.event_params) "
            "WHERE key = 'page') AS page FROM my_project.my_dataset.events AS e "
            "JOIN my_project.my_dataset.unknown_table AS u ON e.col_a = u.col_a",
            id="scalar-subquery",
        ),
        pytest.param(
            "SELECT col_a FROM my_project.my_dataset.events WHERE 'x' IN UNNEST(tags)",
            id="in-unnest",
        ),
        pytest.param(
            "SELECT col_a FROM my_project.my_dataset.events "
            "WHERE EXISTS (SELECT 1 FROM UNNEST(items) AS i WHERE i > 0)",
            id="exists",
        ),
        pytest.param(
            "SELECT col_a, n FROM my_project.my_dataset.events, "
            "UNNEST(GENERATE_ARRAY(1, cnt)) AS n",
            id="generate-array",
        ),
        pytest.param(
            "SELECT col_a, item, pos FROM my_project.my_dataset.events, "
            "UNNEST(items) AS item WITH OFFSET AS pos",
            id="with-offset",
        ),
        pytest.param(
            "INSERT INTO my_project.my_dataset.out_table (col_a, item) "
            "SELECT col_a, item FROM my_project.my_dataset.events, UNNEST(items) AS item",
            id="insert",
        ),
        pytest.param(
            "WITH c AS (SELECT col_a, items FROM my_project.my_dataset.events) "
            "SELECT col_a, item FROM c, UNNEST(items) AS item",
            id="cte",
        ),
    ],
)
def test_common_unnest_shapes_keep_column_lineage(sql: str) -> None:
    result = sqlglot_lineage(
        sql,
        schema_resolver=_resolver(),
        default_db="my_project",
        default_schema="my_dataset",
    )
    assert result.debug_info.column_error is None
    assert result.column_lineage
