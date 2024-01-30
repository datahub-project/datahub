import sqlglot

from datahub.utilities.sqlglot_lineage import (
    _UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT,
    QueryType,
    _get_dialect,
    _is_dialect_instance,
    get_query_type_of_sql,
)


def test_update_from_select():
    assert _UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT == {"returning", "this"}


def test_is_dialect_instance():
    snowflake = _get_dialect("snowflake")

    assert _is_dialect_instance(snowflake, "snowflake")
    assert not _is_dialect_instance(snowflake, "bigquery")

    redshift = _get_dialect("redshift")
    assert _is_dialect_instance(redshift, ["redshift", "snowflake"])
    assert _is_dialect_instance(redshift, ["postgres", "snowflake"])


def test_query_types():
    assert get_query_type_of_sql(
        sqlglot.parse_one(
            "create temp table foo as select * from bar", dialect="redshift"
        ),
        dialect=sqlglot.Dialect.get_or_raise("redshift"),
    ) == (QueryType.CREATE_TABLE_AS_SELECT, {"kind": "TABLE", "temporary": True})

    assert get_query_type_of_sql(
        sqlglot.parse_one("create table #foo as select * from bar", dialect="redshift"),
        dialect=sqlglot.Dialect.get_or_raise("redshift"),
    ) == (QueryType.CREATE_TABLE_AS_SELECT, {"kind": "TABLE", "temporary": True})

    assert get_query_type_of_sql(
        sqlglot.parse_one("create view foo as select * from bar", dialect="redshift"),
        dialect=sqlglot.Dialect.get_or_raise("redshift"),
    ) == (QueryType.CREATE_VIEW, {"kind": "VIEW"})
