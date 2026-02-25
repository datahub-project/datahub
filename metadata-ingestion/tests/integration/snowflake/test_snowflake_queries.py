import os
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.snowflake.snowflake_queries import (
    SnowflakeQueriesExtractor,
    SnowflakeQueriesSource,
)


@patch("snowflake.connector.connect")
def test_source_close_cleans_tmp(snowflake_connect, tmp_path):
    with patch("tempfile.tempdir", str(tmp_path)):
        source = SnowflakeQueriesSource.create(
            {
                "connection": {
                    "account_id": "ABC12345.ap-south-1.aws",
                    "username": "TST_USR",
                    "password": "TST_PWD",
                }
            },
            PipelineContext("run-id"),
        )
        assert len(os.listdir(tmp_path)) > 0
        # This closes QueriesExtractor which in turn closes SqlParsingAggregator
        source.close()
        assert len(os.listdir(tmp_path)) == 0


@patch("snowflake.connector.connect")
def test_user_identifiers_email_as_identifier(snowflake_connect, tmp_path):
    source = SnowflakeQueriesSource.create(
        {
            "connection": {
                "account_id": "ABC12345.ap-south-1.aws",
                "username": "TST_USR",
                "password": "TST_PWD",
            },
            "email_domain": "example.com",
        },
        PipelineContext("run-id"),
    )
    assert (
        source.identifiers.get_user_identifier("username", "username@example.com")
        == "username@example.com"
    )
    assert (
        source.identifiers.get_user_identifier("username", None)
        == "username@example.com"
    )

    # We'd do best effort to use email as identifier, but would keep username as is,
    # if email can't be formed.
    source.identifiers.identifier_config.email_domain = None

    assert (
        source.identifiers.get_user_identifier("username", "username@example.com")
        == "username@example.com"
    )

    assert source.identifiers.get_user_identifier("username", None) == "username"


@patch("snowflake.connector.connect")
def test_user_identifiers_user_email_as_identifier(snowflake_connect, tmp_path):
    source = SnowflakeQueriesSource.create(
        {
            "connection": {
                "account_id": "ABC12345.ap-south-1.aws",
                "username": "TST_USR",
                "password": "TST_PWD",
            },
        },
        PipelineContext("run-id"),
    )
    assert (
        source.identifiers.get_user_identifier("username", "username@example.com")
        == "username@example.com"
    )
    assert source.identifiers.get_user_identifier("username", None) == "username"


def test_snowflake_has_temp_keyword():
    cases = [
        ("CREATE TEMP VIEW my_table__dbt_tmp ...", True),
        ("CREATE TEMPORARY VIEW my_table__dbt_tmp ...", True),
        ("CREATE VIEW my_table__dbt_tmp ...", False),
        # Test case sensitivity
        ("create TEMP view test", True),
        ("CREATE temporary VIEW test", True),
        ("create temp view test", True),
        # Test with whitespace variations
        ("CREATE\nTEMP\tVIEW test", True),
        ("CREATE  TEMPORARY    VIEW test", True),
        # Test with partial matches that should be false
        ("SELECT * FROM my_template_table", False),
        ("CREATE TEMPERATURE VIEW test", False),
        ("SELECT * FROM TEMPDB.table", False),
        ("CREATE VIEW temporary_table", False),
        # Note that this method has some edge cases that don't quite work.
        # But it's good enough for our purposes.
        # ("SELECT 'TEMPORARY' FROM table", False),
    ]
    for query, expected in cases:
        assert SnowflakeQueriesExtractor._has_temp_keyword(query) == expected
