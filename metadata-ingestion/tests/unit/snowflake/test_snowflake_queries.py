import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.snowflake.snowflake_queries import (
    _build_database_filter_condition,
)


class TestBuildDatabaseFilterCondition:
    """Test cases for _build_database_filter_condition function."""

    @pytest.mark.parametrize(
        "database_pattern,expected",
        [
            pytest.param(None, "TRUE", id="none_pattern"),
            pytest.param(
                AllowDenyPattern(allow=[".*"]), "TRUE", id="default_allow_all"
            ),
            pytest.param(AllowDenyPattern(), "TRUE", id="empty_pattern"),
            pytest.param(AllowDenyPattern(allow=[], deny=[]), "TRUE", id="empty_lists"),
            pytest.param(
                AllowDenyPattern(allow=["^PROD_.*"]),
                "(database_name RLIKE '^PROD_.*')",
                id="single_allow",
            ),
            pytest.param(
                AllowDenyPattern(allow=["^PROD_.*", "^STAGING_.*"]),
                "(database_name RLIKE '^PROD_.*' OR database_name RLIKE '^STAGING_.*')",
                id="multiple_allow",
            ),
            pytest.param(
                AllowDenyPattern(deny=[".*_TEMP$"]),
                "(database_name NOT RLIKE '.*_TEMP$')",
                id="single_deny",
            ),
            pytest.param(
                AllowDenyPattern(deny=[".*_TEMP$", "^UTIL_.*"]),
                "(database_name NOT RLIKE '.*_TEMP$' AND database_name NOT RLIKE '^UTIL_.*')",
                id="multiple_deny",
            ),
            pytest.param(
                AllowDenyPattern(allow=["^PROD_.*"], deny=[".*_TEMP$"]),
                "(database_name RLIKE '^PROD_.*') AND (database_name NOT RLIKE '.*_TEMP$')",
                id="allow_and_deny",
            ),
            pytest.param(
                AllowDenyPattern(
                    allow=["^PROD_.*", "^STAGING_.*"], deny=[".*_TEMP$", "^UTIL_.*"]
                ),
                "(database_name RLIKE '^PROD_.*' OR database_name RLIKE '^STAGING_.*') AND (database_name NOT RLIKE '.*_TEMP$' AND database_name NOT RLIKE '^UTIL_.*')",
                id="multiple_allow_and_deny",
            ),
            pytest.param(
                AllowDenyPattern(allow=["^SMOKE_TEST_DB$", "^SMOKE_TEST_DB_2$"]),
                "(database_name RLIKE '^SMOKE_TEST_DB$' OR database_name RLIKE '^SMOKE_TEST_DB_2$')",
                id="exact_database_names",
            ),
            pytest.param(
                AllowDenyPattern(allow=["^(PROD|STAGING)_.*"], deny=[".*_(TEMP|TMP)$"]),
                "(database_name RLIKE '^(PROD|STAGING)_.*') AND (database_name NOT RLIKE '.*_(TEMP|TMP)$')",
                id="complex_regex",
            ),
            pytest.param(
                AllowDenyPattern(allow=[".*"], deny=["^UTIL_DB$", "^SNOWFLAKE$"]),
                "(database_name NOT RLIKE '^UTIL_DB$' AND database_name NOT RLIKE '^SNOWFLAKE$')",
                id="allow_all_with_deny",
            ),
            pytest.param(
                AllowDenyPattern(allow=["^TESTS?[a-z]?_DB$"]),
                "(database_name RLIKE '^TESTS?[a-z]?_DB$')",
                id="special_characters",
            ),
            pytest.param(
                AllowDenyPattern(allow=["^TEST DB$"]),
                "(database_name RLIKE '^TEST DB$')",
                id="whitespace_in_pattern",
            ),
            pytest.param(
                AllowDenyPattern(allow=["xxx'yyy"]),
                "(database_name RLIKE 'xxx''yyy')",
                id="single_quote_in_allow_pattern",
            ),
            pytest.param(
                AllowDenyPattern(deny=["xxx'yyy"]),
                "(database_name NOT RLIKE 'xxx''yyy')",
                id="single_quote_in_deny_pattern",
            ),
        ],
    )
    def test_build_database_filter_condition(self, database_pattern, expected):
        """Test _build_database_filter_condition with various patterns."""
        result = _build_database_filter_condition(database_pattern)
        assert result == expected
