from datahub.utilities.sqlglot_lineage import (
    _UPDATE_ARGS_NOT_SUPPORTED_BY_SELECT,
    _get_dialect,
    _is_dialect_instance,
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
