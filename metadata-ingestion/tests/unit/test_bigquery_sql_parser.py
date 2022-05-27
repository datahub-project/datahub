from datahub.utilities.bigquery_sql_parser import BigQuerySQLParser


def test_bigquery_sql_parser_comments_are_removed():
    parser = BigQuerySQLParser(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `project.dataset.test_view` AS
#This, comment will not break sqllineage
SELECT foo
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
  FROM `project.dataset.src_table`
"""
    )

    assert (
        parser._parsed_sql_query
        == """CREATE OR REPLACE TABLE `project.dataset.test_view` AS SELECT foo
  FROM `project.dataset.src_table`"""
    )

    assert parser.get_tables() == ["project.dataset.src_table"]


def test_bigquery_sql_parser_formats_input_sql():
    parser = BigQuerySQLParser(
        sql_query="""
CREATE OR REPLACE TABLE `project.dataset.test_view` AS
SELECT foo FROM `project.dataset.src_table_a` AS a
INNER JOIN `project.dataset.src_table_b` AS b ON a.key_field = b.key_field
"""
    )

    assert (
        parser._parsed_sql_query
        == """CREATE OR REPLACE TABLE `project.dataset.test_view` AS SELECT foo
  FROM `project.dataset.src_table_a` AS a
 INNER JOIN `project.dataset.src_table_b` AS b
    ON a.key_field = b.key_field"""
    )

    assert parser.get_tables() == [
        "project.dataset.src_table_a",
        "project.dataset.src_table_b",
    ]


def test_remove_comma_before_from():
    assert (
        BigQuerySQLParser._remove_comma_before_from(
            """
select a, b,from `project.dataset.table_name_1`
"""
        )
        == """
select a, b from `project.dataset.table_name_1`
"""
    )

    assert (
        BigQuerySQLParser._remove_comma_before_from(
            """
select
    a,
    b,
from `project.dataset.table_name_1`
"""
        )
        == """
select
    a,
    b from `project.dataset.table_name_1`
"""
    )


def test_bigquery_sql_parser_comment_sign_switched_correctly():
    sql_query = BigQuerySQLParser._parse_bigquery_comment_sign(
        """
#upper comment
SELECT * FROM hello
# lower comment
"""
    )

    assert (
        sql_query
        == """
-- upper comment
SELECT * FROM hello
--  lower comment
"""
    )


def test_bigquery_sql_parser_keyword_from_is_escaped_if_used_as_fieldname():
    sql_query = BigQuerySQLParser._escape_keyword_from_as_field_name(
        """
SELECT hello.from AS col FROM hello
"""
    )

    assert (
        sql_query
        == """
SELECT `hello.from` AS col FROM hello
"""
    )


def test_bigquery_sql_parser_first_cte_name_is_escaped():
    sql_query = BigQuerySQLParser._escape_cte_name_after_keyword_with(
        """
CREATE OR REPLACE VIEW `test_view` AS
WITH cte_1 AS (
    SELECT * FROM foo
),
cte_2 AS (
    SELECT * FROM bar
)
SELECT * FROM cte_1 UNION ALL
SELECT * FROM cte_2
"""
    )

    assert (
        sql_query
        == """
CREATE OR REPLACE VIEW `test_view` AS
WITH `cte_1` AS (
    SELECT * FROM foo
),
cte_2 AS (
    SELECT * FROM bar
)
SELECT * FROM cte_1 UNION ALL
SELECT * FROM cte_2
"""
    )


def test_bigquery_sql_parser_table_name_is_escaped_at_create_statement():
    sql_query_create = BigQuerySQLParser._escape_table_or_view_name_at_create_statement(
        """
CREATE TABLE project.dataset.test_table AS
col_1 STRING,
col_2 STRING
"""
    )

    sql_query_create_or_replace = BigQuerySQLParser._escape_table_or_view_name_at_create_statement(
        """
CREATE OR REPLACE TABLE project.dataset.test_table AS
col_1 STRING,
col_2 STRING
"""
    )

    assert (
        sql_query_create
        == """
CREATE TABLE `project.dataset.test_table` AS
col_1 STRING,
col_2 STRING
"""
    )
    assert (
        sql_query_create_or_replace
        == """
CREATE OR REPLACE TABLE `project.dataset.test_table` AS
col_1 STRING,
col_2 STRING
"""
    )


def test_bigquery_sql_parser_view_name_is_escaped_at_create_statement():
    sql_query_create = BigQuerySQLParser._escape_table_or_view_name_at_create_statement(
        """
CREATE VIEW project.dataset.test_view AS
SELECT * FROM project.dataset.src_table
"""
    )

    sql_query_create_or_replace = BigQuerySQLParser._escape_table_or_view_name_at_create_statement(
        """
CREATE OR REPLACE VIEW project.dataset.test_view AS
SELECT * FROM project.dataset.src_table
"""
    )

    assert (
        sql_query_create
        == """
CREATE VIEW `project.dataset.test_view` AS
SELECT * FROM project.dataset.src_table
"""
    )
    assert (
        sql_query_create_or_replace
        == """
CREATE OR REPLACE VIEW `project.dataset.test_view` AS
SELECT * FROM project.dataset.src_table
"""
    )


def test_bigquery_sql_parser_object_name_is_escaped_after_keyword_from():
    sql_query = BigQuerySQLParser._escape_object_name_after_keyword_from(
        """
CREATE OR REPLACE VIEW `project.dataset.test_view` AS
SELECT * FROM src-project.dataset.src_table_a UNION ALL
SELECT * FROM project.dataset.src_table_b
"""
    )

    assert (
        sql_query
        == """
CREATE OR REPLACE VIEW `project.dataset.test_view` AS
SELECT * FROM `src-project.dataset.src_table_a` UNION ALL
SELECT * FROM `project.dataset.src_table_b`
"""
    )


def test_bigquery_sql_parser_field_name_is_not_escaped_after_keyword_from_in_datetime_functions():
    sql_query = BigQuerySQLParser._escape_object_name_after_keyword_from(
        """
CREATE OR REPLACE VIEW `project.dataset.test_view` AS
SELECT
EXTRACT(MICROSECOND FROM time_field)    AS col_1,
EXTRACT(MILLISECOND FROM time_field)    AS col_2,
EXTRACT(SECOND FROM time_field)         AS col_3,
EXTRACT(MINUTE FROM time_field)         AS col_4,
EXTRACT(HOUR FROM time_field)           AS col_5,
EXTRACT(DAYOFWEEK FROM time_field)      AS col_6,
EXTRACT(DAY FROM time_field)            AS col_7,
EXTRACT(DAYOFYEAR FROM time_field)      AS col_8,
EXTRACT(WEEK FROM time_field)           AS col_9,
EXTRACT(WEEK FROM time_field)           AS col_10,
EXTRACT(ISOWEEK FROM time_field)        AS col_11,
EXTRACT(MONTH FROM time_field)          AS col_12,
EXTRACT(QUARTER FROM time_field)        AS col_13,
EXTRACT(YEAR FROM time_field)           AS col_14,
EXTRACT(ISOYEAR FROM time_field)        AS col_15,
EXTRACT(DATE FROM time_field)           AS col_16,
EXTRACT(TIME FROM time_field)           AS col_17
FROM src-project.dataset.src_table_a
"""
    )

    assert (
        sql_query
        == """
CREATE OR REPLACE VIEW `project.dataset.test_view` AS
SELECT
EXTRACT(MICROSECOND FROM time_field)    AS col_1,
EXTRACT(MILLISECOND FROM time_field)    AS col_2,
EXTRACT(SECOND FROM time_field)         AS col_3,
EXTRACT(MINUTE FROM time_field)         AS col_4,
EXTRACT(HOUR FROM time_field)           AS col_5,
EXTRACT(DAYOFWEEK FROM time_field)      AS col_6,
EXTRACT(DAY FROM time_field)            AS col_7,
EXTRACT(DAYOFYEAR FROM time_field)      AS col_8,
EXTRACT(WEEK FROM time_field)           AS col_9,
EXTRACT(WEEK FROM time_field)           AS col_10,
EXTRACT(ISOWEEK FROM time_field)        AS col_11,
EXTRACT(MONTH FROM time_field)          AS col_12,
EXTRACT(QUARTER FROM time_field)        AS col_13,
EXTRACT(YEAR FROM time_field)           AS col_14,
EXTRACT(ISOYEAR FROM time_field)        AS col_15,
EXTRACT(DATE FROM time_field)           AS col_16,
EXTRACT(TIME FROM time_field)           AS col_17
FROM `src-project.dataset.src_table_a`
"""
    )
