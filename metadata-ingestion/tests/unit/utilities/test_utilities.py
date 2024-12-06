import doctest
from typing import List

from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import SqlParsingDebugInfo, sqlglot_lineage
from datahub.utilities.delayed_iter import delayed_iter
from datahub.utilities.is_pytest import is_pytest_running
from datahub.utilities.urns.dataset_urn import DatasetUrn


class SqlglotSQLParser:
    def __init__(self, sql_query: str, platform: str = "bigquery") -> None:
        self.result = sqlglot_lineage(sql_query, SchemaResolver(platform=platform))

    def get_tables(self) -> List[str]:
        ans = []
        for urn in self.result.in_tables:
            table_ref = DatasetUrn.from_string(urn)
            ans.append(str(table_ref.name))
        return ans

    def get_columns(self) -> List[str]:
        ans = set()
        for col_info in self.result.column_lineage or []:
            for col_ref in col_info.upstreams:
                ans.add(col_ref.column)
        return list(ans)

    def get_downstream_columns(self) -> List[str]:
        ans = set()
        for col_info in self.result.column_lineage or []:
            ans.add(col_info.downstream.column)
        return list(ans)

    def debug_info(self) -> SqlParsingDebugInfo:
        return self.result.debug_info


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


def test_sqllineage_sql_parser_get_tables_from_simple_query():
    sql_query = "SELECT foo.a, foo.b, bar.c FROM foo JOIN bar ON (foo.a == bar.b);"

    tables_list = SqlglotSQLParser(sql_query).get_tables()
    tables_list.sort()
    assert tables_list == ["bar", "foo"]


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

    tables_list = SqlglotSQLParser(sql_query).get_tables()
    tables_list.sort()
    assert tables_list == ["schema1.foo", "schema2.bar"]


def test_sqllineage_sql_parser_get_columns_with_join():
    sql_query = "SELECT foo.a, foo.b, bar.c FROM foo JOIN bar ON (foo.a == bar.b);"

    columns_list = SqlglotSQLParser(sql_query).get_columns()
    columns_list.sort()
    assert columns_list == ["a", "b", "c"]


def test_sqllineage_sql_parser_get_columns_from_simple_query():
    sql_query = "SELECT foo.a, foo.b FROM foo;"

    parser = SqlglotSQLParser(sql_query)
    assert sorted(parser.get_columns()) == ["a", "b"]


def test_sqllineage_sql_parser_get_columns_with_alias_and_count_star():
    sql_query = "SELECT foo.a, foo.b, bar.c as test, count(*) as count FROM foo JOIN bar ON (foo.a == bar.b);"
    parser = SqlglotSQLParser(sql_query)
    assert sorted(parser.get_columns()) == ["a", "b", "c"]
    assert sorted(parser.get_downstream_columns()) == ["a", "b", "count", "test"]


def test_sqllineage_sql_parser_get_columns_with_more_complex_join():
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
    parser = SqlglotSQLParser(sql_query)
    assert sorted(parser.get_columns()) == ["bs", "pi", "tt", "v"]
    assert sorted(parser.get_downstream_columns()) == ["bs", "pi", "pt", "pu", "v"]


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
    parser = SqlglotSQLParser(sql_query)
    columns_list = parser.get_columns()
    assert sorted(columns_list) == ["c", "e", "u", "x"]
    assert sorted(parser.get_downstream_columns()) == [
        "c",
        "count(*)",
        "date",
        "e",
        "u",
        "x",
    ]


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
    parser = SqlglotSQLParser(sql_query)
    tables_list = parser.get_tables()
    tables_list.sort()
    assert tables_list == []
    assert parser.debug_info().table_error is None


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
    parser = SqlglotSQLParser(sql_query)
    assert sorted(parser.get_columns()) == []
    assert sorted(parser.get_downstream_columns()) == [
        "city",
        "country",
        "measurement",
        "timestamp",
    ]
    assert parser.debug_info().column_error is None


def test_sqllineage_sql_parser_with_weird_lookml_query():
    sql_query = """
    SELECT date DATE,
             platform VARCHAR(20) AS aliased_platform,
             country VARCHAR(20) FROM fragment_derived_view'
    """
    parser = SqlglotSQLParser(sql_query)
    columns_list = parser.get_columns()
    columns_list.sort()
    assert columns_list == []
    assert (
        str(parser.debug_info().table_error)
        == "Error tokenizing 'untry VARCHAR(20) FROM fragment_derived_view'\n   ': Missing ' from 5:143"
    )


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
    table_list = SqlglotSQLParser(sql_query).get_tables()
    table_list.sort()
    assert table_list == ["order_items", "orders", "staffs"]


def test_sqllineage_sql_parser_tables_with_special_names():
    # The hyphen appears after the special token in tables names, and before the special token in the column names.
    sql_query = """
SELECT `column-date`, `column-hour`, `column-timestamp`, `column-data`, `column-admin`
FROM `date-table` d
JOIN `hour-table` h on d.`column-date`= h.`column-hour`
JOIN `timestamp-table` t on d.`column-date` = t.`column-timestamp`
JOIN `data-table` da on d.`column-date` = da.`column-data`
JOIN `admin-table` a on d.`column-date` = a.`column-admin`
"""
    expected_tables = [
        "admin-table",
        "data-table",
        "date-table",
        "hour-table",
        "timestamp-table",
    ]
    expected_columns = [
        "column-admin",
        "column-data",
        "column-date",
        "column-hour",
        "column-timestamp",
    ]
    assert sorted(SqlglotSQLParser(sql_query).get_tables()) == expected_tables
    assert sorted(SqlglotSQLParser(sql_query).get_columns()) == []
    assert (
        sorted(SqlglotSQLParser(sql_query).get_downstream_columns()) == expected_columns
    )


def test_logging_name_extraction():
    import datahub.utilities.logging_manager

    assert (
        doctest.testmod(
            datahub.utilities.logging_manager, raise_on_error=True
        ).attempted
        > 0
    )


def test_is_pytest_running() -> None:
    assert is_pytest_running()
