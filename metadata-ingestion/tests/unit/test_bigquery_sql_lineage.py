from datahub.utilities.bigquery_sql_parser import BigQuerySQLParser


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
        """
    )

    assert parser.get_tables() == ["project.dataset.src_tbl"]


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
        """
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
        """
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
        """
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
            UNION
            SELECT * FROM `project.dataset.src_table_b`
        """
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
        """
    )

    assert parser.get_tables() == ["test-project.dataset.src_table"]


def test_bigquery_sql_lineage_from_as_column_name_is_accepted():
    parser = BigQuerySQLParser(
        sql_query="""
            CREATE OR REPLACE VIEW `project.dataset.test_view` AS
            SELECT x.from AS col
            FROM project.dataset.src_table AS x
        """
    )

    assert parser.get_tables() == ["project.dataset.src_table"]
