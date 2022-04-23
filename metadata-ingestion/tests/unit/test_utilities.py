import sys

import pytest

from datahub.utilities.delayed_iter import delayed_iter
from datahub.utilities.sql_parser import MetadataSQLSQLParser, SqlLineageSQLParser


def test_delayed_iter():
    events = []

    def maker(n):
        for i in range(n):
            events.append(("add", i))
            yield i

    for i in delayed_iter(maker(4), 2):
        events.append(("remove", i))

    assert events == [
        ("add", 0),
        ("add", 1),
        ("add", 2),
        ("remove", 0),
        ("add", 3),
        ("remove", 1),
        ("remove", 2),
        ("remove", 3),
    ]

    events.clear()
    for i in delayed_iter(maker(2), None):
        events.append(("remove", i))

    assert events == [
        ("add", 0),
        ("add", 1),
        ("remove", 0),
        ("remove", 1),
    ]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_metadatasql_sql_parser_get_tables_from_simple_query():
    sql_query = "SELECT foo.a, foo.b, bar.c FROM foo JOIN bar ON (foo.a == bar.b);"

    tables_list = MetadataSQLSQLParser(sql_query).get_tables()
    tables_list.sort()
    assert tables_list == ["bar", "foo"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_sqllineage_sql_parser_get_tables_from_simple_query():
    sql_query = "SELECT foo.a, foo.b, bar.c FROM foo JOIN bar ON (foo.a == bar.b);"

    tables_list = MetadataSQLSQLParser(sql_query).get_tables()
    tables_list.sort()
    assert tables_list == ["bar", "foo"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_sqllineage_sql_parser_get_tables_from_complex_query():
    sql_query = """
(
SELECT
    CAST(substring(e, 1, 10) AS date) AS __d_a_t_e,
    e AS e,
    u AS u,
    x,
    c,
    count(*)
FROM
    schema1.foo
WHERE
    datediff('day',
    substring(e, 1, 10) :: date,
    date :: date) <= 7
    AND CAST(substring(e, 1, 10) AS date) >= date('2010-01-01')
    AND CAST(substring(e, 1, 10) AS date) < getdate()
GROUP BY
    1,
    2,
    3,
    4,
    5)
UNION ALL(
SELECT
CAST(substring(e, 1, 10) AS date) AS date,
e AS e,
u AS u,
x,
c,
count(*)
FROM
schema2.bar
WHERE
datediff('day',
substring(e, 1, 10) :: date,
date :: date) <= 7
    AND CAST(substring(e, 1, 10) AS date) >= date('2020-08-03')
        AND CAST(substring(e, 1, 10) AS date) < getdate()
    GROUP BY
        1,
        2,
        3,
        4,
        5)
"""

    tables_list = SqlLineageSQLParser(sql_query).get_tables()
    tables_list.sort()
    assert tables_list == ["schema1.foo", "schema2.bar"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_sqllineage_sql_parser_get_columns_with_join():
    sql_query = "SELECT foo.a, foo.b, bar.c FROM foo JOIN bar ON (foo.a == bar.b);"

    columns_list = SqlLineageSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["a", "b", "c"]


def test_sqllineage_sql_parser_get_columns_from_simple_query():
    sql_query = "SELECT foo.a, foo.b FROM foo;"

    columns_list = SqlLineageSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["a", "b"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_metadatasql_sql_parser_get_columns_with_alias_and_count_star():
    sql_query = "SELECT foo.a, foo.b, bar.c as test, count(*) as count FROM foo JOIN bar ON (foo.a == bar.b);"

    columns_list = SqlLineageSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["a", "b", "count", "test"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_metadatasql_sql_parser_get_columns_with_more_complex_join():
    sql_query = """
    INSERT
    INTO
    foo
SELECT
    pl.pi pi,
    REGEXP_REPLACE(pl.tt, '_', ' ') pt,
    pl.tt pu,
    fp.v,
    fp.bs
FROM
    bar pl
JOIN baz fp ON
    fp.rt = pl.rt
WHERE
    fp.dt = '2018-01-01'
    """

    columns_list = SqlLineageSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["bs", "pi", "pt", "pu", "v"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_sqllineage_sql_parser_get_columns_complex_query_with_union():
    sql_query = """
(
SELECT
    CAST(substring(e, 1, 10) AS date) AS date ,
    e AS e,
    u AS u,
    x,
    c,
    count(*)
FROM
    foo
WHERE
    datediff('day',
    substring(e, 1, 10) :: date,
    date :: date) <= 7
    AND CAST(substring(e, 1, 10) AS date) >= date('2010-01-01')
    AND CAST(substring(e, 1, 10) AS date) < getdate()
GROUP BY
    1,
    2,
    3,
    4,
    5)
UNION ALL(
SELECT
CAST(substring(e, 1, 10) AS date) AS date,
e AS e,
u AS u,
x,
c,
count(*)
FROM
bar
WHERE
datediff('day',
substring(e, 1, 10) :: date,
date :: date) <= 7
    AND CAST(substring(e, 1, 10) AS date) >= date('2020-08-03')
        AND CAST(substring(e, 1, 10) AS date) < getdate()
    GROUP BY
        1,
        2,
        3,
        4,
        5)
"""

    columns_list = SqlLineageSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["c", "date", "e", "u", "x"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_metadatasql_sql_parser_get_tables_from_templated_query():
    sql_query = """
        SELECT
          country,
          city,
          timestamp,
          measurement
        FROM
          ${my_view.SQL_TABLE_NAME} AS my_view
"""
    tables_list = MetadataSQLSQLParser(sql_query).get_tables()
    tables_list.sort()
    assert tables_list == ["my_view.SQL_TABLE_NAME"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_sqllineage_sql_parser_get_tables_from_templated_query():
    sql_query = """
        SELECT
          country,
          city,
          timestamp,
          measurement
        FROM
          ${my_view.SQL_TABLE_NAME} AS my_view
"""
    tables_list = SqlLineageSQLParser(sql_query).get_tables()
    tables_list.sort()
    assert tables_list == ["my_view.SQL_TABLE_NAME"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_metadatasql_sql_parser_get_columns_from_templated_query():
    sql_query = """
        SELECT
          country,
          city,
          timestamp,
          measurement
        FROM
          ${my_view.SQL_TABLE_NAME} AS my_view
"""
    columns_list = MetadataSQLSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["city", "country", "measurement", "timestamp"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_sqllineage_sql_parser_get_columns_from_templated_query():
    sql_query = """
        SELECT
          country,
          city,
          timestamp,
          measurement
        FROM
          ${my_view.SQL_TABLE_NAME} AS my_view
"""
    columns_list = SqlLineageSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["city", "country", "measurement", "timestamp"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_sqllineage_sql_parser_with_weird_lookml_query():
    sql_query = """
    SELECT date DATE,
             platform VARCHAR(20) AS aliased_platform,
             country VARCHAR(20) FROM fragment_derived_view'
    """
    columns_list = SqlLineageSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["aliased_platform", "country", "date"]


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="The LookML source requires Python 3.7+"
)
def test_metadatasql_sql_parser_with_weird_lookml_query():
    sql_query = """
    SELECT date DATE,
             platform VARCHAR(20) AS aliased_platform,
             country VARCHAR(20) FROM fragment_derived_view'
    """
    columns_list = MetadataSQLSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["aliased_platform", "country", "date"]


def test_metadatasql_sql_parser_tables_from_redash_query():
    sql_query = """SELECT
name,
SUM(quantity * list_price * (1 - discount)) AS total,
YEAR(order_date) as order_year
FROM
`orders` o
INNER JOIN `order_items` i ON i.order_id = o.order_id
INNER JOIN `staffs` s ON s.staff_id = o.staff_id
GROUP BY
name,
year(order_date)"""
    table_list = MetadataSQLSQLParser(sql_query).get_tables()
    table_list.sort()
    assert table_list == ["order_items", "orders", "staffs"]


def test_sqllineage_sql_parser_tables_from_redash_query():
    sql_query = """SELECT
name,
SUM(quantity * list_price * (1 - discount)) AS total,
YEAR(order_date) as order_year
FROM
`orders` o
INNER JOIN `order_items` i ON i.order_id = o.order_id
INNER JOIN `staffs` s ON s.staff_id = o.staff_id
GROUP BY
name,
year(order_date)"""
    table_list = SqlLineageSQLParser(sql_query).get_tables()
    table_list.sort()
    assert table_list == ["order_items", "orders", "staffs"]


def test_hash_in_sql_query_with_no_space():
    parser = SqlLineageSQLParser(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `foo.bar.trg_tbl`AS
#This, comment will not break sqllineage
SELECT foo
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
FROM `foo.bar.src_tbl`
        """
    )

    assert parser.get_tables() == ["foo.bar.src_tbl"]


def test_with_keyword_data():
    parser = SqlLineageSQLParser(
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


def test_with_keyword_admin():
    parser = SqlLineageSQLParser(
        sql_query="""
            WITH admin AS (
                SELECT *
                FROM `project.example_dataset.example_table`
            )
            SELECT * FROM admin
        """
    )

    assert parser.get_tables() == ["project.example_dataset.example_table"]


def test_sqllineage_sql_parser_create_or_replace_table_name_not_wrapped_in_backticks():
    parser = SqlLineageSQLParser(
        sql_query="""
            CREATE OR REPLACE TABLE project.dataset.test_table AS
            WITH cte AS (
                SELECT
                    *,
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
                FROM project.dataset.src_table_a
            )
            SELECT * FROM cte
            UNION
            SELECT *
            FROM project.dataset.src_table_b
            UNION
            SELECT * FROM `project.dataset.src_table_c`
        """
    )

    assert parser.get_tables() == [
        "project.dataset.src_table_a",
        "project.dataset.src_table_b",
        "project.dataset.src_table_c",
    ]


def test_sqllineage_sql_parser_create_or_replace_view_name_not_wrapped_in_backticks():
    parser = SqlLineageSQLParser(
        sql_query="""
            CREATE OR REPLACE VIEW project.dataset.test_view AS
            WITH cte AS (
                SELECT
                    *,
                    'foo' AS bar
                FROM project.dataset.src_table_a
            )
            SELECT * FROM cte
            UNION
            SELECT *
            FROM project.dataset.src_table_b
            UNION
            SELECT * FROM `project.dataset.src_table_c`
        """
    )

    assert parser.get_tables() == [
        "project.dataset.src_table_a",
        "project.dataset.src_table_b",
        "project.dataset.src_table_c",
    ]


def test_sqllineage_sql_parser_create_table_name_not_wrapped_in_backticks():
    parser = SqlLineageSQLParser(
        sql_query="""
            CREATE TABLE project.dataset.test_table AS
            WITH cte AS (
                SELECT
                    *,
                    'foo' AS bar
                FROM project.dataset.src_table_a
            )
            SELECT * FROM cte
            UNION
            SELECT *
            FROM project.dataset.src_table_b
            UNION
            SELECT * FROM `project.dataset.src_table_c`
        """
    )

    assert parser.get_tables() == [
        "project.dataset.src_table_a",
        "project.dataset.src_table_b",
        "project.dataset.src_table_c",
    ]


def test_sqllineage_sql_parser_create_view_name_not_wrapped_in_backticks():
    parser = SqlLineageSQLParser(
        sql_query="""
            CREATE VIEW project.dataset.test_view AS
            WITH cte AS (
                SELECT
                    *,
                    'foo' AS bar
                FROM project.dataset.src_table_a
            )
            SELECT * FROM cte
            UNION
            SELECT *
            FROM project.dataset.src_table_b
            UNION
            SELECT * FROM `project.dataset.src_table_c`
        """
    )

    assert parser.get_tables() == [
        "project.dataset.src_table_a",
        "project.dataset.src_table_b",
        "project.dataset.src_table_c",
    ]


def test_sqllineage_sql_parser_from_as_column_name_is_escaped():
    parser = SqlLineageSQLParser(
        sql_query="""
            CREATE TABLE project.dataset.test_table AS
            SELECT x.from AS col
            FROM project.dataset.src_table AS x
        """
    )

    assert parser.get_tables() == [
        "project.dataset.src_table",
    ]


def test_sqllineage_sql_parser_cte_alias_near_with_keyword_is_escaped():
    parser = SqlLineageSQLParser(
        sql_query="""
            create or replace view `project.dataset.test_view` as
            with map as (
                    select col_1
                        col_2
                    from `project.dataset.src_table_a` a
                    join `project.dataset.src_table_b` b
                        on a.col_1 = b.col_2
                ),
                cte_2 as (
                    select * from `project.dataset.src_table_c`
                )
            select * from map
            union all
            select * from cte_2
        """
    )

    # results would (somehow) also contain "cte_2" which should be considered a subquery rather than a table
    assert set(
        [
            "project.dataset.src_table_a",
            "project.dataset.src_table_b",
            "project.dataset.src_table_c",
        ]
    ).issubset(parser.get_tables())
