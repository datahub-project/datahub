import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.sql.sql_utils import (
    check_table_with_profile_pattern,
    gen_schema_key,
)


def test_guid_generators():
    expected_guid = "f5e571e4a9acce86333e6b427ba1651f"
    schema_key = gen_schema_key(
        db_name="hive",
        schema="db1",
        platform="hive",
        platform_instance="PROD",
        env="PROD",
    )

    guid = schema_key.guid()
    assert guid == expected_guid


test_profile_pattern_matching_on_table_allow_list_test_data = [
    ("db.table.column", "db.table", True),
    ("db.table.column2", "db.table", True),
    ("db.table..*", "db.table", True),
    ("db.*", "db.table", True),
    ("db.*", "db.table", True),
    ("db.*", "db.schema.table", True),
    ("db.schema.*", "db.schema.table", True),
    ("db\\.schema\\..*", "db.schema.table", True),
    ("db\\.schema\\.table\\.column_prefix.*", "db.schema.table", True),
    ("db\\.schema\\.table\\.column", "db.schema.table", True),
    ("db\\.schema\\.table2\\.column", "db.schema.table", False),
    ("db2\\.schema.*", "db.schema.table", False),
    ("db2\\.schema.*", "db.schema.table", False),
    ("db\\.schema\\.table\\..*", "db.table2", False),
]


@pytest.mark.parametrize(
    "allow_pattern, table_name, result",
    test_profile_pattern_matching_on_table_allow_list_test_data,
)
def test_profile_pattern_matching_on_table_allow_list(
    allow_pattern: str, table_name: str, result: bool
) -> None:
    pattern = AllowDenyPattern(allow=[allow_pattern])
    assert check_table_with_profile_pattern(pattern, table_name) == result


test_profile_pattern_matching_on_table_deny_list_test_data = [
    ("db.table.column", "db.table", True),
    ("db.table.column2", "db.table", True),
    ("db.table..*", "db.table", True),
    ("db.*", "db.table", False),
    ("db.*", "db.table", False),
    ("db.*", "db.schema.table", False),
    ("db.schema.*", "db.schema.table", False),
    ("db\\.schema\\..*", "db.schema.table", False),
    ("db\\.schema\\.table\\.column_prefix.*", "db.schema.table", True),
    ("db\\.schema\\.table\\.column", "db.schema.table", True),
    ("db\\.schema\\.table2\\.column", "db.schema.table", True),
    ("db2\\.schema.*", "db.schema.table", True),
    ("db2\\.schema.*", "db.schema.table", True),
    ("db\\.schema\\.table\\..*", "db.table2", True),
]


@pytest.mark.parametrize(
    "deny_pattern, table_name, result",
    test_profile_pattern_matching_on_table_deny_list_test_data,
)
def test_profile_pattern_matching_on_table_deny_list(
    deny_pattern: str, table_name: str, result: bool
) -> None:
    pattern = AllowDenyPattern(deny=[deny_pattern])
    assert check_table_with_profile_pattern(pattern, table_name) == result
