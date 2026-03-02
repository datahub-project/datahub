import json
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import Mock, patch

import pytest
import sqlglot
from sqlglot.dialects.snowflake import Snowflake

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    BucketDuration,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.snowflake.snowflake_config import (
    QueryDedupStrategyType,
    SnowflakeIdentifierConfig,
)
from datahub.ingestion.source.snowflake.snowflake_queries import (
    QueryLogQueryBuilder,
    SnowflakeQueriesExtractor,
    SnowflakeQueriesExtractorConfig,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantQueriesRunSkipHandler,
)
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    PreparsedQuery,
    TableRename,
    TableSwap,
)


class TestBuildAccessHistoryDatabaseFilterCondition:
    @pytest.mark.parametrize(
        "database_pattern,additional_database_names,expected",
        [
            pytest.param(
                None,
                None,
                "TRUE",
                id="no_pattern_no_additional_dbs",
            ),
            pytest.param(
                AllowDenyPattern(),
                None,
                "TRUE",
                id="empty_pattern_no_additional_dbs",
            ),
            pytest.param(
                None,
                [],
                "TRUE",
                id="no_pattern_empty_additional_dbs",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*')) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*')) > 0 OR (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*'))",
                id="allow_pattern_only",
            ),
            pytest.param(
                AllowDenyPattern(deny=[".*_TEMP"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP')) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP')) > 0 OR (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP'))",
                id="deny_pattern_only",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*"], deny=[".*_TEMP"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' AND SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' AND SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*' AND SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP')))",
                id="allow_and_deny_patterns",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*", "DEV_.*"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'DEV_.*'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'DEV_.*'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'DEV_.*')))",
                id="multiple_allow_patterns",
            ),
            pytest.param(
                AllowDenyPattern(deny=[".*_TEMP", ".*_STAGING"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> ((SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP' AND SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_STAGING')))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> ((SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP' AND SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_STAGING')))) > 0 OR (((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP' AND SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_STAGING'))))",
                id="multiple_deny_patterns",
            ),
            pytest.param(
                None,
                ["DB1", "DB2"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB2'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB2'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB2')))",
                id="multiple_additional_database_names",
            ),
            pytest.param(
                None,
                ["SPECIAL_DB"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) = 'SPECIAL_DB')) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) = 'SPECIAL_DB')) > 0 OR (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'SPECIAL_DB'))",
                id="single_additional_database_name",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*"]),
                ["DB1", "DB2"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB2') OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*')) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB2') OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*')) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB1' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB2') OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*'))",
                id="pattern_with_additional_database_names",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD_.*", "DEV_.*"], deny=[".*_TEMP"]),
                ["SPECIAL_DB"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) = 'SPECIAL_DB' OR ((SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'DEV_.*') AND SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) = 'SPECIAL_DB' OR ((SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'DEV_.*') AND SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP'))) > 0 OR (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'SPECIAL_DB' OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD_.*' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'DEV_.*') AND SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP')))",
                id="complex_pattern_with_additional_database_names",
            ),
            pytest.param(
                AllowDenyPattern(allow=[".*"]),
                None,
                "TRUE",
                id="default_allow_pattern_ignored",
            ),
            pytest.param(
                AllowDenyPattern(allow=[".*"], deny=[".*_TEMP"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP')) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*_TEMP')) > 0 OR (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*_TEMP'))",
                id="default_allow_pattern_with_deny_pattern",
            ),
            pytest.param(
                AllowDenyPattern(allow=["PROD'_.*"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD''_.*')) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) RLIKE 'PROD''_.*')) > 0 OR (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) RLIKE 'PROD''_.*'))",
                id="sql_injection_protection_allow_pattern",
            ),
            pytest.param(
                AllowDenyPattern(deny=[".*'_TEMP"]),
                None,
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*''_TEMP')) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> SPLIT_PART(UPPER(o:objectName), '.', 1) NOT RLIKE '.*''_TEMP')) > 0 OR (SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) NOT RLIKE '.*''_TEMP'))",
                id="sql_injection_protection_deny_pattern",
            ),
            pytest.param(
                None,
                ["DB'1", "DB'2"],
                "(ARRAY_SIZE(FILTER(direct_objects_accessed, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB''1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB''2'))) > 0 OR ARRAY_SIZE(FILTER(objects_modified, o -> (SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB''1' OR SPLIT_PART(UPPER(o:objectName), '.', 1) = 'DB''2'))) > 0 OR ((SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB''1' OR SPLIT_PART(UPPER(object_modified_by_ddl:objectName), '.', 1) = 'DB''2')))",
                id="sql_injection_protection_additional_database_names",
            ),
            pytest.param(
                AllowDenyPattern(allow=[]),
                None,
                "TRUE",
                id="empty_allow_pattern_list",
            ),
            pytest.param(
                AllowDenyPattern(deny=[]),
                None,
                "TRUE",
                id="empty_deny_pattern_list",
            ),
            pytest.param(
                AllowDenyPattern(allow=[], deny=[]),
                None,
                "TRUE",
                id="both_empty_pattern_lists",
            ),
        ],
    )
    def test_build_access_history_database_filter_condition(
        self, database_pattern, additional_database_names, expected
    ):
        """Test the _build_access_history_database_filter_condition method with various inputs."""
        # Create a QueryLogQueryBuilder instance to test the method
        builder = QueryLogQueryBuilder(
            start_time=datetime(year=2021, month=1, day=1),
            end_time=datetime(year=2021, month=1, day=2),
            bucket_duration=BucketDuration.HOUR,
            dedup_strategy=QueryDedupStrategyType.STANDARD,
            database_pattern=database_pattern,
            additional_database_names=additional_database_names,
        )

        result = builder._build_access_history_database_filter_condition(
            database_pattern, additional_database_names
        )
        assert result == expected


class TestQueryLogQueryBuilder:
    def test_non_implemented_strategy(self):
        with pytest.raises(NotImplementedError):
            QueryLogQueryBuilder(
                start_time=datetime(year=2021, month=1, day=1),
                end_time=datetime(year=2021, month=1, day=1),
                bucket_duration=BucketDuration.HOUR,
                deny_usernames=None,
                dedup_strategy="DUMMY",  # type: ignore[arg-type]
            ).build_enriched_query_log_query()

    def test_fetch_query_for_all_strategies(self):
        for strategy in QueryDedupStrategyType:
            query = QueryLogQueryBuilder(
                start_time=datetime(year=2021, month=1, day=1),
                end_time=datetime(year=2021, month=1, day=1),
                bucket_duration=BucketDuration.HOUR,
                dedup_strategy=strategy,
            ).build_enriched_query_log_query()
            # SQL parsing should succeed
            sqlglot.parse(query, dialect=Snowflake)

    def test_query_with_database_pattern_filtering(self):
        """Test that database pattern filtering generates valid SQL."""
        database_pattern = AllowDenyPattern(allow=["PROD_.*"], deny=[".*_TEMP"])

        query = QueryLogQueryBuilder(
            start_time=datetime(year=2021, month=1, day=1),
            end_time=datetime(year=2021, month=1, day=2),
            bucket_duration=BucketDuration.HOUR,
            deny_usernames=None,
            dedup_strategy=QueryDedupStrategyType.STANDARD,
            database_pattern=database_pattern,
        ).build_enriched_query_log_query()

        # SQL parsing should succeed
        sqlglot.parse(query, dialect=Snowflake)

    def test_query_with_additional_database_names(self):
        """Test that additional database names generate valid SQL."""
        additional_database_names = ["SPECIAL_DB", "ANALYTICS_DB"]

        query = QueryLogQueryBuilder(
            start_time=datetime(year=2021, month=1, day=1),
            end_time=datetime(year=2021, month=1, day=2),
            bucket_duration=BucketDuration.HOUR,
            dedup_strategy=QueryDedupStrategyType.NONE,
            additional_database_names=additional_database_names,
        ).build_enriched_query_log_query()

        # SQL parsing should succeed
        sqlglot.parse(query, dialect=Snowflake)

    def test_query_with_combined_database_filtering(self):
        """Test that both database patterns and additional database names generate valid SQL."""
        database_pattern = AllowDenyPattern(allow=["PROD_.*"])
        additional_database_names = ["SPECIAL_DB"]

        query = QueryLogQueryBuilder(
            start_time=datetime(year=2021, month=1, day=1),
            end_time=datetime(year=2021, month=1, day=2),
            bucket_duration=BucketDuration.HOUR,
            deny_usernames=None,
            dedup_strategy=QueryDedupStrategyType.STANDARD,
            database_pattern=database_pattern,
            additional_database_names=additional_database_names,
        ).build_enriched_query_log_query()

        # SQL parsing should succeed
        sqlglot.parse(query, dialect=Snowflake)


class TestBuildUserFilter:
    @pytest.mark.parametrize(
        "deny_usernames,allow_usernames,expected",
        [
            pytest.param(
                None,
                None,
                "TRUE",
                id="no_filters",
            ),
            pytest.param(
                [],
                [],
                "TRUE",
                id="empty_lists",
            ),
            pytest.param(
                None,
                [],
                "TRUE",
                id="none_deny_empty_allow",
            ),
            pytest.param(
                [],
                None,
                "TRUE",
                id="empty_deny_none_allow",
            ),
            pytest.param(
                ["SERVICE_USER"],
                None,
                "(user_name NOT ILIKE 'SERVICE_USER')",
                id="single_deny_exact",
            ),
            pytest.param(
                ["SERVICE_%"],
                None,
                "(user_name NOT ILIKE 'SERVICE_%')",
                id="single_deny_pattern",
            ),
            pytest.param(
                ["SERVICE_%", "ADMIN_%"],
                None,
                "(user_name NOT ILIKE 'SERVICE_%' AND user_name NOT ILIKE 'ADMIN_%')",
                id="multiple_deny_patterns",
            ),
            pytest.param(
                None,
                ["ANALYST_USER"],
                "(user_name ILIKE 'ANALYST_USER')",
                id="single_allow_exact",
            ),
            pytest.param(
                None,
                ["ANALYST_%"],
                "(user_name ILIKE 'ANALYST_%')",
                id="single_allow_pattern",
            ),
            pytest.param(
                None,
                ["ANALYST_%", "%_USER"],
                "(user_name ILIKE 'ANALYST_%' OR user_name ILIKE '%_USER')",
                id="multiple_allow_patterns",
            ),
            pytest.param(
                ["SERVICE_%"],
                ["ANALYST_%"],
                "(user_name NOT ILIKE 'SERVICE_%') AND (user_name ILIKE 'ANALYST_%')",
                id="single_deny_and_single_allow",
            ),
            pytest.param(
                ["SERVICE_%", "ADMIN_%"],
                ["ANALYST_%", "%_USER"],
                "(user_name NOT ILIKE 'SERVICE_%' AND user_name NOT ILIKE 'ADMIN_%') AND (user_name ILIKE 'ANALYST_%' OR user_name ILIKE '%_USER')",
                id="multiple_deny_and_multiple_allow",
            ),
            pytest.param(
                ["TEST_ANALYST_%"],
                ["TEST_%"],
                "(user_name NOT ILIKE 'TEST_ANALYST_%') AND (user_name ILIKE 'TEST_%')",
                id="overlapping_deny_and_allow_patterns",
            ),
            pytest.param(
                ["'SPECIAL_USER'"],
                None,
                "(user_name NOT ILIKE '''SPECIAL_USER''')",
                id="sql_injection_protection_deny",
            ),
            pytest.param(
                None,
                ["'SPECIAL_USER'"],
                "(user_name ILIKE '''SPECIAL_USER''')",
                id="sql_injection_protection_allow",
            ),
            pytest.param(
                ["USER_O'CONNOR"],
                ["ANALYST_O'BRIEN"],
                "(user_name NOT ILIKE 'USER_O''CONNOR') AND (user_name ILIKE 'ANALYST_O''BRIEN')",
                id="sql_injection_protection_both",
            ),
        ],
    )
    def test_build_user_filter(self, deny_usernames, allow_usernames, expected):
        """Test the _build_user_filter method with various combinations of deny and allow patterns."""
        # Create a QueryLogQueryBuilder instance to test the method
        builder = QueryLogQueryBuilder(
            start_time=datetime(year=2021, month=1, day=1),
            end_time=datetime(year=2021, month=1, day=2),
            bucket_duration=BucketDuration.HOUR,
            deny_usernames=deny_usernames,
            allow_usernames=allow_usernames,
            dedup_strategy=QueryDedupStrategyType.STANDARD,
        )

        result = builder._build_user_filter(deny_usernames, allow_usernames)
        assert result == expected


class TestSnowflakeViewQueries:
    def test_get_views_for_database_query_syntax(self):
        query = SnowflakeQuery.get_views_for_database("TEST_DB")

        # Should be parseable by sqlglot
        parsed = sqlglot.parse(query, dialect=Snowflake)
        assert len(parsed) == 1

        # Validate SQL structure
        statement = parsed[0]
        assert statement is not None
        assert statement.find(sqlglot.exp.Select) is not None

        # Check that it's selecting from information_schema.views in the correct database
        from_clause = statement.find(sqlglot.exp.From)
        assert from_clause is not None
        table_name = str(from_clause.this).replace('"', "")
        assert table_name == "TEST_DB.information_schema.views"

    def test_get_views_for_schema_query_syntax(self):
        query = SnowflakeQuery.get_views_for_schema("TEST_DB", "PUBLIC")

        # Should be parseable by sqlglot
        parsed = sqlglot.parse(query, dialect=Snowflake)
        assert len(parsed) == 1

        # Validate SQL structure
        statement = parsed[0]
        assert statement is not None
        assert statement.find(sqlglot.exp.Select) is not None

        # Check that it's selecting from information_schema.views in the correct database
        from_clause = statement.find(sqlglot.exp.From)
        assert from_clause is not None
        table_name = str(from_clause.this).replace('"', "")
        assert table_name == "TEST_DB.information_schema.views"

        # Check that it has a WHERE clause filtering by schema
        where_clause = statement.find(sqlglot.exp.Where)
        assert where_clause is not None
        where_str = str(where_clause).upper()
        assert "TABLE_SCHEMA" in where_str and "PUBLIC" in where_str


class TestSnowflakeQueriesExtractorOptimization:
    """Tests for the query fetch optimization when all features are disabled."""

    def _create_mock_extractor(
        self,
        include_lineage: bool = False,
        include_queries: bool = False,
        include_usage_statistics: bool = False,
        include_query_usage_statistics: bool = False,
        include_operations: bool = False,
    ) -> SnowflakeQueriesExtractor:
        """Helper to create a SnowflakeQueriesExtractor with mocked dependencies."""
        mock_connection = Mock()
        mock_connection.query.return_value = []

        config = SnowflakeQueriesExtractorConfig(
            window=BaseTimeWindowConfig(
                start_time=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2021, 1, 2, tzinfo=timezone.utc),
            ),
            include_lineage=include_lineage,
            include_queries=include_queries,
            include_usage_statistics=include_usage_statistics,
            include_query_usage_statistics=include_query_usage_statistics,
            include_operations=include_operations,
        )

        mock_report = Mock()
        mock_filters = Mock()
        mock_identifiers = Mock()
        mock_identifiers.platform = "snowflake"
        mock_identifiers.identifier_config = SnowflakeIdentifierConfig()

        extractor = SnowflakeQueriesExtractor(
            connection=mock_connection,
            config=config,
            structured_report=mock_report,
            filters=mock_filters,
            identifiers=mock_identifiers,
        )

        return extractor

    def test_skip_query_fetch_when_all_features_disabled(self):
        """Test that query fetching is skipped when all query features are disabled."""
        extractor = self._create_mock_extractor(
            include_lineage=False,
            include_queries=False,
            include_usage_statistics=False,
            include_query_usage_statistics=False,
            include_operations=False,
        )

        # Mock the fetch methods
        with (
            patch.object(extractor, "fetch_users", return_value={}) as mock_fetch_users,
            patch.object(
                extractor, "fetch_copy_history", return_value=[]
            ) as mock_fetch_copy_history,
            patch.object(
                extractor, "fetch_query_log", return_value=[]
            ) as mock_fetch_query_log,
        ):
            # Execute the method
            list(extractor.get_workunits_internal())

            # Verify fetch_users was called (always needed for setup)
            mock_fetch_users.assert_called_once()

            # Verify expensive fetches were NOT called
            mock_fetch_copy_history.assert_not_called()
            mock_fetch_query_log.assert_not_called()

    def test_fetch_queries_when_lineage_enabled(self):
        """Test that query fetching happens when lineage is enabled."""
        extractor = self._create_mock_extractor(
            include_lineage=True,
            include_queries=False,
            include_usage_statistics=False,
            include_query_usage_statistics=False,
            include_operations=False,
        )

        with (
            patch.object(extractor, "fetch_users", return_value={}) as mock_fetch_users,
            patch.object(
                extractor, "fetch_copy_history", return_value=[]
            ) as mock_fetch_copy_history,
            patch.object(
                extractor, "fetch_query_log", return_value=[]
            ) as mock_fetch_query_log,
        ):
            list(extractor.get_workunits_internal())

            mock_fetch_users.assert_called_once()
            mock_fetch_copy_history.assert_called_once()
            mock_fetch_query_log.assert_called_once()

    def test_fetch_queries_when_usage_statistics_enabled(self):
        """Test that query fetching happens when usage statistics are enabled."""
        extractor = self._create_mock_extractor(
            include_lineage=False,
            include_queries=False,
            include_usage_statistics=True,
            include_query_usage_statistics=False,
            include_operations=False,
        )

        with (
            patch.object(extractor, "fetch_users", return_value={}),
            patch.object(
                extractor, "fetch_copy_history", return_value=[]
            ) as mock_fetch_copy_history,
            patch.object(
                extractor, "fetch_query_log", return_value=[]
            ) as mock_fetch_query_log,
        ):
            list(extractor.get_workunits_internal())

            mock_fetch_copy_history.assert_called_once()
            mock_fetch_query_log.assert_called_once()

    def test_fetch_queries_when_operations_enabled(self):
        """Test that query fetching happens when operations are enabled."""
        extractor = self._create_mock_extractor(
            include_lineage=False,
            include_queries=False,
            include_usage_statistics=False,
            include_query_usage_statistics=False,
            include_operations=True,
        )

        with (
            patch.object(extractor, "fetch_users", return_value={}),
            patch.object(
                extractor, "fetch_copy_history", return_value=[]
            ) as mock_fetch_copy_history,
            patch.object(
                extractor, "fetch_query_log", return_value=[]
            ) as mock_fetch_query_log,
        ):
            list(extractor.get_workunits_internal())

            mock_fetch_copy_history.assert_called_once()
            mock_fetch_query_log.assert_called_once()

    def test_fetch_queries_when_any_single_feature_enabled(self):
        """Test that query fetching happens when any single feature is enabled."""
        features = [
            "include_lineage",
            "include_queries",
            "include_usage_statistics",
            "include_query_usage_statistics",
            "include_operations",
        ]

        for feature in features:
            kwargs = {f: False for f in features}
            kwargs[feature] = True

            extractor = self._create_mock_extractor(**kwargs)

            with (
                patch.object(extractor, "fetch_users", return_value={}),
                patch.object(
                    extractor, "fetch_copy_history", return_value=[]
                ) as mock_fetch_copy_history,
                patch.object(
                    extractor, "fetch_query_log", return_value=[]
                ) as mock_fetch_query_log,
            ):
                list(extractor.get_workunits_internal())

                # Verify fetches were called
                mock_fetch_copy_history.assert_called_once()
                mock_fetch_query_log.assert_called_once()

    def test_report_counts_with_disabled_features(self):
        """Test that report counts are zero when features are disabled."""
        extractor = self._create_mock_extractor(
            include_lineage=False,
            include_queries=False,
            include_usage_statistics=False,
            include_query_usage_statistics=False,
            include_operations=False,
        )

        with (
            patch.object(extractor, "fetch_users", return_value={}),
            patch.object(extractor, "fetch_copy_history", return_value=[]),
            patch.object(extractor, "fetch_query_log", return_value=[]),
        ):
            list(extractor.get_workunits_internal())

            # Verify that num_preparsed_queries is 0
            assert extractor.report.sql_aggregator is not None
            assert extractor.report.sql_aggregator.num_preparsed_queries == 0


class TestSnowflakeQueryParser:
    """
    Tests for SnowflakeQueriesExtractor._parse_audit_log_row,
    focusing on handling of corrupted audit log results from Snowflake.
    """

    def test_parse_query_with_empty_column_name_returns_observed_query(self):
        """Test that a corrupted audit log entry with an empty column name falls back to ObservedQuery."""
        mock_connection = Mock()
        config = SnowflakeQueriesExtractorConfig(
            window=BaseTimeWindowConfig(
                start_time=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2021, 1, 2, tzinfo=timezone.utc),
            ),
        )
        mock_report = Mock()
        mock_filters = Mock()
        mock_identifiers = Mock(spec=SnowflakeIdentifierBuilder)
        mock_identifiers.platform = "snowflake"
        mock_identifiers.identifier_config = SnowflakeIdentifierConfig()
        mock_identifiers.gen_dataset_urn = Mock(
            return_value="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        )
        mock_identifiers.get_dataset_identifier_from_qualified_name = Mock(
            return_value="test_db.test_schema.test_table"
        )
        mock_identifiers.snowflake_identifier = Mock(side_effect=lambda x: x)
        mock_identifiers.get_user_identifier = Mock(return_value="test_user")

        extractor = SnowflakeQueriesExtractor(
            connection=mock_connection,
            config=config,
            structured_report=mock_report,
            filters=mock_filters,
            identifiers=mock_identifiers,
        )

        # Simulate a Snowflake access history row with empty column name
        row = {
            "QUERY_ID": "test_query_123",
            "ROOT_QUERY_ID": None,
            "QUERY_TEXT": "SELECT * FROM test_table WHERE id = 1",
            "QUERY_TYPE": "SELECT",
            "SESSION_ID": "session_123",
            "USER_NAME": "test_user",
            "ROLE_NAME": "test_role",
            "QUERY_START_TIME": datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "END_TIME": datetime(2021, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
            "QUERY_DURATION": 1000,
            "ROWS_INSERTED": 0,
            "ROWS_UPDATED": 0,
            "ROWS_DELETED": 0,
            "DEFAULT_DB": "test_db",
            "DEFAULT_SCHEMA": "test_schema",
            "QUERY_COUNT": 1,
            "QUERY_SECONDARY_FINGERPRINT": "fingerprint_123",
            "DIRECT_OBJECTS_ACCESSED": json.dumps(
                [
                    {
                        "objectName": "test_db.test_schema.test_table",
                        "objectDomain": "Table",
                        "columns": [
                            {"columnName": "id"},
                            {"columnName": ""},  # Empty column name
                            {"columnName": "name"},
                        ],
                    }
                ]
            ),
            "OBJECTS_MODIFIED": json.dumps([]),
            "OBJECT_MODIFIED_BY_DDL": None,
        }

        users: dict = {}

        result = extractor._parse_audit_log_row(row, users)

        # Assert that an ObservedQuery is returned when there's an empty column
        assert isinstance(result, ObservedQuery), (
            f"Expected ObservedQuery but got {type(result)}"
        )
        assert result.query == "SELECT * FROM test_table WHERE id = 1"
        assert result.session_id == "session_123"
        assert result.default_db == "test_db"
        assert result.default_schema == "test_schema"

    def test_parse_query_with_valid_columns_returns_preparsed_query(self):
        """Test that queries with all valid column names return PreparsedQuery."""
        mock_connection = Mock()
        config = SnowflakeQueriesExtractorConfig(
            window=BaseTimeWindowConfig(
                start_time=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2021, 1, 2, tzinfo=timezone.utc),
            ),
        )
        mock_report = Mock()
        mock_filters = Mock()
        mock_identifiers = Mock(spec=SnowflakeIdentifierBuilder)
        mock_identifiers.platform = "snowflake"
        mock_identifiers.identifier_config = SnowflakeIdentifierConfig()
        mock_identifiers.gen_dataset_urn = Mock(
            return_value="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        )
        mock_identifiers.get_dataset_identifier_from_qualified_name = Mock(
            return_value="test_db.test_schema.test_table"
        )
        mock_identifiers.snowflake_identifier = Mock(side_effect=lambda x: x)
        mock_identifiers.get_user_identifier = Mock(return_value="test_user")

        extractor = SnowflakeQueriesExtractor(
            connection=mock_connection,
            config=config,
            structured_report=mock_report,
            filters=mock_filters,
            identifiers=mock_identifiers,
        )

        # Simulate a Snowflake access history row with valid column names
        row = {
            "QUERY_ID": "test_query_456",
            "ROOT_QUERY_ID": None,
            "QUERY_TEXT": "SELECT id, name FROM test_table",
            "QUERY_TYPE": "SELECT",
            "SESSION_ID": "session_456",
            "USER_NAME": "test_user",
            "ROLE_NAME": "test_role",
            "QUERY_START_TIME": datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "END_TIME": datetime(2021, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
            "QUERY_DURATION": 1000,
            "ROWS_INSERTED": 0,
            "ROWS_UPDATED": 0,
            "ROWS_DELETED": 0,
            "DEFAULT_DB": "test_db",
            "DEFAULT_SCHEMA": "test_schema",
            "QUERY_COUNT": 1,
            "QUERY_SECONDARY_FINGERPRINT": "fingerprint_456",
            "DIRECT_OBJECTS_ACCESSED": json.dumps(
                [
                    {
                        "objectName": "test_db.test_schema.test_table",
                        "objectDomain": "Table",
                        "columns": [
                            {"columnName": "id"},
                            {"columnName": "name"},
                        ],
                    }
                ]
            ),
            "OBJECTS_MODIFIED": json.dumps([]),
            "OBJECT_MODIFIED_BY_DDL": None,
        }

        users: dict = {}

        result = extractor._parse_audit_log_row(row, users)

        # Assert that a PreparsedQuery is returned when all columns are valid
        assert isinstance(result, PreparsedQuery), (
            f"Expected PreparsedQuery but got {type(result)}"
        )
        assert result.query_text == "SELECT id, name FROM test_table"


class TestParseDdlQuery:
    """Tests for parse_ddl_query handling of Snowflake access_history DDL payloads.

    Snowflake's 2026_01 BCR bundle (which includes BCR-2178) changed the format
    of object_modified_by_ddl properties from {"key": {"value": "..."}} to
    {"key": "..."}. parse_ddl_query must handle both formats.

    All test payloads are from real Snowflake access_history rows observed on a
    test instance before and after enabling the 2026_01 BCR bundle.
    """

    @staticmethod
    def _create_extractor() -> SnowflakeQueriesExtractor:
        mock_connection = Mock()
        config = SnowflakeQueriesExtractorConfig(
            window=BaseTimeWindowConfig(
                start_time=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2021, 1, 2, tzinfo=timezone.utc),
            ),
        )
        structured_report = SourceReport()
        mock_filters = Mock()
        mock_identifiers = Mock(spec=SnowflakeIdentifierBuilder)
        mock_identifiers.platform = "snowflake"
        mock_identifiers.identifier_config = SnowflakeIdentifierConfig()
        mock_identifiers.gen_dataset_urn = Mock(
            side_effect=lambda x: f"urn:li:dataset:(urn:li:dataPlatform:snowflake,{x},PROD)"
        )
        mock_identifiers.get_dataset_identifier_from_qualified_name = Mock(
            side_effect=lambda x: x.lower()
        )
        mock_identifiers.snowflake_identifier = Mock(side_effect=lambda x: x)
        mock_identifiers.get_user_identifier = Mock(return_value="test_user")

        return SnowflakeQueriesExtractor(
            connection=mock_connection,
            config=config,
            structured_report=structured_report,
            filters=mock_filters,
            identifiers=mock_identifiers,
        )

    def test_rename_table_pre_2026_01_bcr(self):
        """Pre-BCR RENAME_TABLE: properties use {"value": ...} wrapper."""
        extractor = self._create_extractor()
        ts = datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc)

        ddl = {
            "objectDomain": "Table",
            "objectId": 6307261,
            "objectName": "MAX_TEST.PUBLIC.TEST_DDL_CLONE",
            "operationType": "ALTER",
            "properties": {"objectName": {"value": "MAX_TEST.PUBLIC.TEST_DDL_RENAMED"}},
        }

        result = extractor.parse_ddl_query(
            query="ALTER TABLE TEST_DDL_CLONE RENAME TO TEST_DDL_RENAMED",
            session_id="session-1",
            timestamp=ts,
            object_modified_by_ddl=ddl,
            query_type="RENAME_TABLE",
        )

        assert isinstance(result, TableRename)
        assert "test_ddl_clone" in result.original_urn
        assert "test_ddl_renamed" in result.new_urn

    def test_rename_table_post_2026_01_bcr(self):
        """Post-BCR RENAME_TABLE: properties are flattened strings."""
        extractor = self._create_extractor()
        ts = datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc)

        ddl = {
            "objectDomain": "Table",
            "objectId": 6311319,
            "objectName": "MAX_TEST.PUBLIC.TEST_DDL_CLONE",
            "operationType": "ALTER",
            "properties": {"objectName": "MAX_TEST.PUBLIC.TEST_DDL_RENAMED"},
        }

        result = extractor.parse_ddl_query(
            query="ALTER TABLE TEST_DDL_CLONE RENAME TO TEST_DDL_RENAMED",
            session_id="session-1",
            timestamp=ts,
            object_modified_by_ddl=ddl,
            query_type="RENAME_TABLE",
        )

        assert isinstance(result, TableRename)
        assert "test_ddl_clone" in result.original_urn
        assert "test_ddl_renamed" in result.new_urn

    def test_swap_table_pre_2026_01_bcr(self):
        """Pre-BCR SWAP: properties use {"value": ...} wrapper."""
        extractor = self._create_extractor()
        ts = datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc)

        ddl = {
            "objectDomain": "Table",
            "objectId": 6321270,
            "objectName": "MAX_TEST.PUBLIC.TEST_DDL_CLONE",
            "operationType": "ALTER",
            "properties": {
                "swapTargetDomain": {"value": "Table"},
                "swapTargetId": {"value": 6321270},
                "swapTargetName": {"value": "MAX_TEST.PUBLIC.TEST_DDL_REPRO"},
            },
        }

        result = extractor.parse_ddl_query(
            query="ALTER TABLE TEST_DDL_CLONE SWAP WITH TEST_DDL_REPRO",
            session_id="session-1",
            timestamp=ts,
            object_modified_by_ddl=ddl,
            query_type="ALTER",
        )

        assert isinstance(result, TableSwap)
        assert "test_ddl_clone" in result.urn1
        assert "test_ddl_repro" in result.urn2

    def test_swap_table_post_2026_01_bcr(self):
        """Post-BCR SWAP: properties are flattened strings."""
        extractor = self._create_extractor()
        ts = datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc)

        ddl = {
            "objectDomain": "Table",
            "objectId": 6290396,
            "objectName": "MAX_TEST.PUBLIC.TEST_DDL_CLONE",
            "operationType": "ALTER",
            "properties": {
                "swapTargetDomain": "Table",
                "swapTargetId": 6290396,
                "swapTargetName": "MAX_TEST.PUBLIC.TEST_DDL_REPRO",
            },
        }

        result = extractor.parse_ddl_query(
            query="ALTER TABLE TEST_DDL_CLONE SWAP WITH TEST_DDL_REPRO",
            session_id="session-1",
            timestamp=ts,
            object_modified_by_ddl=ddl,
            query_type="ALTER",
        )

        assert isinstance(result, TableSwap)
        assert "test_ddl_clone" in result.urn1
        assert "test_ddl_repro" in result.urn2

    def test_non_dict_ddl_returns_none(self):
        """Non-dict object_modified_by_ddl (e.g. a plain string from
        json.loads of a JSON string value) is gracefully dropped."""
        extractor = self._create_extractor()
        ts = datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc)

        result = extractor.parse_ddl_query(
            query="ALTER SESSION SET QUERY_TAG = 'test'",
            session_id="session-1",
            timestamp=ts,
            object_modified_by_ddl="ALTER SESSION SET QUERY_TAG = 'test'",  # type: ignore[arg-type]
            query_type="ALTER_SESSION",
        )
        assert result is None

    def test_non_alter_ddl_dropped(self):
        """A well-formed CREATE DDL is dropped — parse_ddl_query only handles
        ALTER RENAME/SWAP."""
        extractor = self._create_extractor()
        ts = datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc)

        ddl = {
            "objectDomain": "Table",
            "objectId": 12345,
            "objectName": "MAX_TEST.PUBLIC.TEST_DDL_REPRO",
            "operationType": "CREATE",
            "properties": {},
        }

        result = extractor.parse_ddl_query(
            query="CREATE TABLE TEST_DDL_REPRO (id INT)",
            session_id="session-1",
            timestamp=ts,
            object_modified_by_ddl=ddl,
            query_type="CREATE",
        )
        assert result is None

    # -- _parse_audit_log_row integration tests --

    @staticmethod
    def _make_row(
        object_modified_by_ddl_json: str,
        query_text: str = "ALTER TABLE T RENAME TO T2",
        query_type: str = "RENAME_TABLE",
    ) -> dict:
        """Build a minimal access_history row."""
        return {
            "QUERY_ID": "test-query-1",
            "ROOT_QUERY_ID": None,
            "QUERY_TEXT": query_text,
            "QUERY_TYPE": query_type,
            "SESSION_ID": "session-1",
            "USER_NAME": "SVC_DATAHUB",
            "ROLE_NAME": "DATAHUB_ROLE",
            "QUERY_START_TIME": datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc),
            "END_TIME": datetime(2026, 2, 25, 12, 0, 1, tzinfo=timezone.utc),
            "QUERY_DURATION": 100,
            "ROWS_INSERTED": 0,
            "ROWS_UPDATED": 0,
            "ROWS_DELETED": 0,
            "DEFAULT_DB": "MAX_TEST",
            "DEFAULT_SCHEMA": "PUBLIC",
            "QUERY_COUNT": 1,
            "QUERY_SECONDARY_FINGERPRINT": "fp-1",
            "DIRECT_OBJECTS_ACCESSED": json.dumps([]),
            "OBJECTS_MODIFIED": json.dumps([]),
            "OBJECT_MODIFIED_BY_DDL": object_modified_by_ddl_json,
        }

    def test_audit_log_row_string_ddl_no_failures(self):
        """_parse_audit_log_row gracefully handles a string-valued
        object_modified_by_ddl without accumulating report failures."""
        extractor = self._create_extractor()
        row = self._make_row(
            json.dumps("ALTER SESSION SET QUERY_TAG = 'test'"),
            query_text="ALTER SESSION SET QUERY_TAG = 'test'",
            query_type="ALTER_SESSION",
        )

        result = extractor._parse_audit_log_row(row, {})
        assert result is None
        assert not extractor.structured_reporter.failures

    def test_audit_log_row_rename_post_2026_01_bcr(self):
        """End-to-end: _parse_audit_log_row correctly produces a TableRename
        from a post-BCR RENAME_TABLE payload."""
        extractor = self._create_extractor()
        ddl = {
            "objectDomain": "Table",
            "objectId": 6311319,
            "objectName": "MAX_TEST.PUBLIC.TEST_DDL_CLONE",
            "operationType": "ALTER",
            "properties": {"objectName": "MAX_TEST.PUBLIC.TEST_DDL_RENAMED"},
        }
        row = self._make_row(json.dumps(ddl))

        result = extractor._parse_audit_log_row(row, {})
        assert isinstance(result, TableRename)


class TestSnowflakeQueriesExtractorStatefulTimeWindowIngestion:
    """Tests for stateful time window ingestion support in queries v2."""

    def _create_mock_extractor(
        self,
        include_usage_statistics: bool = False,
        redundant_run_skip_handler: Optional[RedundantQueriesRunSkipHandler] = None,
        bucket_duration: BucketDuration = BucketDuration.DAY,
    ) -> SnowflakeQueriesExtractor:
        """Helper to create a SnowflakeQueriesExtractor with mocked dependencies."""
        mock_connection = Mock()
        mock_connection.query.return_value = []

        config = SnowflakeQueriesExtractorConfig(
            window=BaseTimeWindowConfig(
                start_time=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2021, 1, 2, tzinfo=timezone.utc),
                bucket_duration=bucket_duration,
            ),
            include_usage_statistics=include_usage_statistics,
        )

        mock_report = Mock()
        mock_filters = Mock()
        mock_identifiers = Mock()
        mock_identifiers.platform = "snowflake"
        mock_identifiers.identifier_config = SnowflakeIdentifierConfig()

        extractor = SnowflakeQueriesExtractor(
            connection=mock_connection,
            config=config,
            structured_report=mock_report,
            filters=mock_filters,
            identifiers=mock_identifiers,
            redundant_run_skip_handler=redundant_run_skip_handler,
        )

        return extractor

    def test_time_window_adjusted_with_handler(self):
        """Test that time window is adjusted when handler is provided."""
        adjusted_start_time = datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        adjusted_end_time = datetime(2021, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

        mock_handler = Mock(spec=RedundantQueriesRunSkipHandler)
        mock_handler.suggest_run_time_window.return_value = (
            adjusted_start_time,
            adjusted_end_time,
        )

        extractor = self._create_mock_extractor(
            redundant_run_skip_handler=mock_handler,
        )

        mock_handler.suggest_run_time_window.assert_called_once()
        assert extractor.start_time == adjusted_start_time
        assert extractor.end_time == adjusted_end_time

    def test_time_window_not_adjusted_without_handler(self):
        """Test that time window is not adjusted when no handler is provided."""
        original_start_time = datetime(2021, 1, 1, tzinfo=timezone.utc)
        original_end_time = datetime(2021, 1, 2, tzinfo=timezone.utc)

        extractor = self._create_mock_extractor(
            redundant_run_skip_handler=None,
        )

        assert extractor.start_time == original_start_time
        assert extractor.end_time == original_end_time

    def test_bucket_alignment_with_usage_statistics(self):
        """Test that start_time is aligned to bucket boundaries when usage statistics are enabled."""
        # Start time at 14:30 should be aligned to beginning of day (00:00)
        start_time_with_offset = datetime(2021, 1, 1, 14, 30, 0, tzinfo=timezone.utc)
        mock_handler = Mock(spec=RedundantQueriesRunSkipHandler)
        mock_handler.suggest_run_time_window.return_value = (
            start_time_with_offset,
            datetime(2021, 1, 2, tzinfo=timezone.utc),
        )

        extractor = self._create_mock_extractor(
            include_usage_statistics=True,
            redundant_run_skip_handler=mock_handler,
        )

        # Start time should be aligned to beginning of day
        expected_aligned_start = datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert extractor.start_time == expected_aligned_start
        # End time should remain unchanged
        assert extractor.end_time == datetime(2021, 1, 2, tzinfo=timezone.utc)

    def test_no_bucket_alignment_without_usage_statistics(self):
        """Test that start_time is NOT aligned when usage statistics are disabled."""
        start_time_with_offset = datetime(2021, 1, 1, 14, 30, 0, tzinfo=timezone.utc)
        mock_handler = Mock(spec=RedundantQueriesRunSkipHandler)
        mock_handler.suggest_run_time_window.return_value = (
            start_time_with_offset,
            datetime(2021, 1, 2, tzinfo=timezone.utc),
        )

        extractor = self._create_mock_extractor(
            include_usage_statistics=False,
            redundant_run_skip_handler=mock_handler,
        )

        # Start time should NOT be aligned
        assert extractor.start_time == start_time_with_offset
        assert extractor.end_time == datetime(2021, 1, 2, tzinfo=timezone.utc)

    def test_bucket_alignment_hourly_with_usage_statistics(self):
        """Test that start_time is aligned to hour boundaries when hourly buckets are configured."""
        start_time_with_offset = datetime(2021, 1, 1, 14, 30, 45, tzinfo=timezone.utc)
        mock_handler = Mock(spec=RedundantQueriesRunSkipHandler)
        mock_handler.suggest_run_time_window.return_value = (
            start_time_with_offset,
            datetime(2021, 1, 2, tzinfo=timezone.utc),
        )

        extractor = self._create_mock_extractor(
            include_usage_statistics=True,
            redundant_run_skip_handler=mock_handler,
            bucket_duration=BucketDuration.HOUR,
        )

        expected_aligned_start = datetime(2021, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
        assert extractor.start_time == expected_aligned_start
        assert extractor.end_time == datetime(2021, 1, 2, tzinfo=timezone.utc)

    def test_state_updated_after_successful_extraction(self):
        """Test that state is updated after successful extraction when handler is provided."""
        mock_handler = Mock(spec=RedundantQueriesRunSkipHandler)
        mock_handler.suggest_run_time_window.return_value = (
            datetime(2021, 1, 1, tzinfo=timezone.utc),
            datetime(2021, 1, 2, tzinfo=timezone.utc),
        )

        extractor = self._create_mock_extractor(
            redundant_run_skip_handler=mock_handler,
        )

        with (
            patch.object(extractor, "fetch_users", return_value={}),
            patch.object(extractor, "fetch_copy_history", return_value=[]),
            patch.object(extractor, "fetch_query_log", return_value=[]),
        ):
            list(extractor.get_workunits_internal())

            mock_handler.update_state.assert_called_once_with(
                datetime(2021, 1, 1, tzinfo=timezone.utc),
                datetime(2021, 1, 2, tzinfo=timezone.utc),
                BucketDuration.DAY,
            )

    def test_state_not_updated_without_handler(self):
        """Test that state is not updated when no handler is provided."""
        extractor = self._create_mock_extractor(
            redundant_run_skip_handler=None,
        )

        with (
            patch.object(extractor, "fetch_users", return_value={}),
            patch.object(extractor, "fetch_copy_history", return_value=[]),
            patch.object(extractor, "fetch_query_log", return_value=[]),
        ):
            list(extractor.get_workunits_internal())

    def test_queries_extraction_always_runs_with_handler(self):
        """Test that queries extraction always runs even with a skip handler."""
        mock_handler = Mock(spec=RedundantQueriesRunSkipHandler)
        mock_handler.suggest_run_time_window.return_value = (
            datetime(2021, 1, 1, tzinfo=timezone.utc),
            datetime(2021, 1, 2, tzinfo=timezone.utc),
        )

        extractor = self._create_mock_extractor(
            redundant_run_skip_handler=mock_handler,
        )

        with (
            patch.object(extractor, "fetch_users", return_value={}) as mock_fetch_users,
            patch.object(
                extractor, "fetch_copy_history", return_value=[]
            ) as mock_fetch_copy_history,
            patch.object(
                extractor, "fetch_query_log", return_value=[]
            ) as mock_fetch_query_log,
        ):
            list(extractor.get_workunits_internal())

            mock_fetch_users.assert_called_once()
            mock_fetch_copy_history.assert_called_once()
            mock_fetch_query_log.assert_called_once()
