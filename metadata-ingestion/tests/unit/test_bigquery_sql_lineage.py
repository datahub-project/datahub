from typing import List

from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigQueryTableRef
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage


class BigQuerySQLParser:
    def __init__(self, sql_query: str, schema_resolver: SchemaResolver) -> None:
        self.result = sqlglot_lineage(sql_query, schema_resolver)

    def get_tables(self) -> List[str]:
        ans = []
        for urn in self.result.in_tables:
            table_ref = BigQueryTableRef.from_urn(urn)
            ans.append(str(table_ref.table_identifier))
        return ans

    def get_columns(self) -> List[str]:
        ans = []
        for col_info in self.result.column_lineage or []:
            for col_ref in col_info.upstreams:
                ans.append(col_ref.column)
        return ans


def test_bigquery_sql_lineage_basic():
    parser = BigQuerySQLParser(
        sql_query="""SELECT * FROM project_1.database_1.view_1""",
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["project_1.database_1.view_1"]


def test_bigquery_sql_lineage_hash_as_comment_sign_is_accepted():
    parser = BigQuerySQLParser(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `project.dataset.trg_tbl`AS
#This, comment will not break sqllineage
SELECT foo
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
FROM `project.dataset.src_tbl`
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["project.dataset.src_tbl"]


def test_bigquery_sql_lineage_camel_case_table():
    """
    This test aims to test the parameter to ignore sqllineage lowercasing.
    On the BigQuery service, it's possible to use uppercase name un datasets and tables.
    The lowercasing, by default, breaks the lineage construction in these cases.
    """
    parser = BigQuerySQLParser(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `project.dataset.trg_tbl`AS
#This, comment will not break sqllineage
SELECT foo, bar
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
FROM `project.dataset.CamelCaseTable`
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["project.dataset.CamelCaseTable"]


def test_bigquery_sql_lineage_camel_case_dataset():
    """
    This test aims to test the parameter to ignore sqllineage lowercasing.
    On the BigQuery service, it's possible to use uppercase name un datasets and tables.
    The lowercasing, by default, breaks the lineage construction in these cases.
    """
    parser = BigQuerySQLParser(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `project.dataset.trg_tbl`AS
#This, comment will not break sqllineage
SELECT foo, bar
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
FROM `project.DataSet.table`
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["project.DataSet.table"]


def test_bigquery_sql_lineage_camel_case_table_and_dataset():
    """
    This test aims to test the parameter to ignore sqllineage lowercasing.
    On the BigQuery service, it's possible to use uppercase name un datasets and tables.
    The lowercasing, by default, breaks the lineage construction in these cases.
    """
    parser = BigQuerySQLParser(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `project.dataset.trg_tbl`AS
#This, comment will not break sqllineage
SELECT foo, bar
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
FROM `project.DataSet.CamelTable`
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["project.DataSet.CamelTable"]


def test_bigquery_sql_lineage_camel_case_table_and_dataset_subquery():
    """
    This test aims to test the parameter to ignore sqllineage lowercasing.
    On the BigQuery service, it's possible to use uppercase name un datasets and tables.
    The lowercasing, by default, breaks the lineage construction in these cases.
    """
    parser = BigQuerySQLParser(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `project.dataset.trg_tbl`AS
#This, comment will not break sqllineage
SELECT foo, bar
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
FROM (
    # this comment will not break sqllineage either
    SELECT * FROM `project.DataSet.CamelTable`
)
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["project.DataSet.CamelTable"]


def test_bigquery_sql_lineage_camel_case_table_and_dataset_joins():
    """
    This test aims to test the parameter to ignore sqllineage lowercasing.
    On the BigQuery service, it's possible to use uppercase name un datasets and tables.
    The lowercasing, by default, breaks the lineage construction in these cases.
    """
    parser = BigQuerySQLParser(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `project.dataset.trg_tbl`AS
#This, comment will not break sqllineage
SELECT foo, bar
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
FROM `project.DataSet1.CamelTable`
INNER JOIN `project.DataSet2.CamelTable2`
    ON b.id = a.id
LEFT JOIN `project.DataSet3.CamelTable3`
    on c.id = b.id
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == [
        "project.DataSet1.CamelTable",
        "project.DataSet2.CamelTable2",
        "project.DataSet3.CamelTable3",
    ]


def test_bigquery_sql_lineage_camel_case_table_and_dataset_joins_and_subquery():
    """
    This test aims to test the parameter to ignore sqllineage lowercasing.
    On the BigQuery service, it's possible to use uppercase name un datasets and tables.
    The lowercasing, by default, breaks the lineage construction in these cases.
    """
    parser = BigQuerySQLParser(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `project.dataset.trg_tbl`AS
#This, comment will not break sqllineage
SELECT foo, bar
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
FROM `project.DataSet1.CamelTable` a
INNER JOIN `project.DataSet2.CamelTable2` b
    ON b.id = a.id
LEFT JOIN (SELECT * FROM `project.DataSet3.CamelTable3`) c
    ON c.id = b.id
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == [
        "project.DataSet1.CamelTable",
        "project.DataSet2.CamelTable2",
        "project.DataSet3.CamelTable3",
    ]


def test_bigquery_sql_lineage_keyword_data_is_accepted():
    parser = BigQuerySQLParser(
        sql_query="""
            WITH data AS (
                SELECT
                    *,
                    'foo' AS bar
                FROM `project.example_dataset.example_table`
            )
            SELECT * FROM data
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["project.example_dataset.example_table"]


def test_bigquery_sql_lineage_keyword_admin_is_accepted():
    parser = BigQuerySQLParser(
        sql_query="""
            WITH admin AS (
                SELECT *
                FROM `project.example_dataset.example_table`
            )
            SELECT * FROM admin
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["project.example_dataset.example_table"]


def test_bigquery_sql_lineage_cte_alias_as_keyword_is_accepted():
    parser = BigQuerySQLParser(
        sql_query="""
CREATE OR REPLACE TABLE `project.dataset.test_table` AS
WITH map AS (
        SELECT a.col_1,
               b.col_2
          FROM (
                SELECT DISTINCT *
                  FROM (
                        SELECT col_1
                          FROM `project.dataset.source_table_a`
                       )
               ) a
          JOIN `project.dataset.source_table_b` b
            ON a.col_1 = b.col_1
       )
SELECT *
  FROM map
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == [
        "project.dataset.source_table_a",
        "project.dataset.source_table_b",
    ]


def test_bigquery_sql_lineage_create_or_replace_view_name_with_hyphens_is_accepted():
    parser = BigQuerySQLParser(
        sql_query="""
            CREATE OR REPLACE VIEW test-project.dataset.test_view AS
            SELECT *
            FROM project.dataset.src_table_a
            UNION ALL
            SELECT * FROM `project.dataset.src_table_b`
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == [
        "project.dataset.src_table_a",
        "project.dataset.src_table_b",
    ]


def test_bigquery_sql_lineage_source_table_name_with_hyphens_is_accepted():
    parser = BigQuerySQLParser(
        sql_query="""
            CREATE OR REPLACE VIEW `project.dataset.test_view` AS
            SELECT *
            FROM test-project.dataset.src_table
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["test-project.dataset.src_table"]


def test_bigquery_sql_lineage_from_as_column_name_is_accepted():
    parser = BigQuerySQLParser(
        sql_query="""
            CREATE OR REPLACE VIEW `project.dataset.test_view` AS
            SELECT x.from AS col
            FROM project.dataset.src_table AS x
        """,
        schema_resolver=SchemaResolver(platform="bigquery"),
    )

    assert parser.get_tables() == ["project.dataset.src_table"]
