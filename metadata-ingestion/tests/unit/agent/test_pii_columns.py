"""Identity columns are withheld from probe results.

A catalog relation can be worth admitting for the structure it carries and
still have one column that names a person. ACCOUNT_USAGE.ACCESS_HISTORY forced
this: it is how Snowflake lineage works and whether it is empty is the
difference between "lineage will work" and "lineage silently returns nothing",
so refusing the whole relation costs a real capability -- but USER_NAME is a
person and a probe result is read into a model's context.
"""

from datahub.ingestion.agent.redact import mask_identity_columns
from datahub.ingestion.agent.sql_passthrough import sql_result


def test_an_identity_column_is_masked_and_its_neighbours_are_not():
    columns = ["USER_NAME", "QUERY_START_TIME", "QUERY_ID"]
    rows = [["alice@example.com", "2026-01-01", "abc"]]
    assert mask_identity_columns(columns, rows) == [["***", "2026-01-01", "abc"]]


def test_the_column_is_kept_rather_than_dropped():
    """A masked value says "withheld"; a missing column says nothing at all.

    Silently narrowing a result is the failure this interface exists to
    prevent, so dropping the column would be the wrong fix even though it is
    the simpler one.
    """
    shaped = sql_result(["USER_NAME", "QUERY_ID"], [["alice", "abc"]], limit=10)
    assert shaped["columns"] == ["USER_NAME", "QUERY_ID"]
    assert shaped["rows"] == [["***", "abc"]]


def test_matching_is_case_insensitive():
    assert mask_identity_columns(["user_name"], [["a"]]) == [["***"]]
    assert mask_identity_columns(["User_Name"], [["a"]]) == [["***"]]
    assert mask_identity_columns(["USER_NAME"], [["a"]]) == [["***"]]


def test_a_column_that_merely_contains_an_identity_word_is_left_alone():
    """Whole-name matching, not substring. `owner` is a role on most catalog
    views, and over-masking is its own failure -- a secret equal to a schema
    name already made `target` unreadable one layer up."""
    columns = ["OWNER", "OWNER_ROLE", "USER_TYPE", "EMAIL_ENABLED"]
    rows = [["SYSADMIN", "ANALYST", "PERSON", "true"]]
    assert mask_identity_columns(columns, rows) == [
        ["SYSADMIN", "ANALYST", "PERSON", "true"]
    ]


def test_a_null_identity_value_stays_null():
    """`***` would claim a value was withheld where there was none."""
    assert mask_identity_columns(["EMAIL"], [[None]]) == [[None]]


def test_a_result_with_no_identity_column_is_untouched():
    columns = ["TABLE_SCHEMA", "TABLE_NAME"]
    rows = [["public", "orders"], ["public", "users"]]
    assert mask_identity_columns(columns, rows) == rows


def test_masking_survives_truncation_shaping():
    """sql_result clamps and masks in one pass; neither may skip the other."""
    rows = [["alice", i] for i in range(5)]
    shaped = sql_result(["USER_NAME", "N"], rows, limit=2)
    assert shaped["rows"] == [["***", 0], ["***", 1]]
    assert shaped["truncated"] is True
