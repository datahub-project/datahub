import datetime
from unittest.mock import Mock, patch

import pytest
import sqlglot
from sqlglot.dialects.snowflake import Snowflake

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import (
    BaseTimeWindowConfig,
    BucketDuration,
)
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
            start_time=datetime.datetime(year=2021, month=1, day=1),
            end_time=datetime.datetime(year=2021, month=1, day=2),
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
                start_time=datetime.datetime(year=2021, month=1, day=1),
                end_time=datetime.datetime(year=2021, month=1, day=1),
                bucket_duration=BucketDuration.HOUR,
                deny_usernames=None,
                dedup_strategy="DUMMY",  # type: ignore[arg-type]
            ).build_enriched_query_log_query()

    def test_fetch_query_for_all_strategies(self):
        for strategy in QueryDedupStrategyType:
            query = QueryLogQueryBuilder(
                start_time=datetime.datetime(year=2021, month=1, day=1),
                end_time=datetime.datetime(year=2021, month=1, day=1),
                bucket_duration=BucketDuration.HOUR,
                dedup_strategy=strategy,
            ).build_enriched_query_log_query()
            # SQL parsing should succeed
            sqlglot.parse(query, dialect=Snowflake)

    def test_query_with_database_pattern_filtering(self):
        """Test that database pattern filtering generates valid SQL."""
        database_pattern = AllowDenyPattern(allow=["PROD_.*"], deny=[".*_TEMP"])

        query = QueryLogQueryBuilder(
            start_time=datetime.datetime(year=2021, month=1, day=1),
            end_time=datetime.datetime(year=2021, month=1, day=2),
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
            start_time=datetime.datetime(year=2021, month=1, day=1),
            end_time=datetime.datetime(year=2021, month=1, day=2),
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
            start_time=datetime.datetime(year=2021, month=1, day=1),
            end_time=datetime.datetime(year=2021, month=1, day=2),
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
            start_time=datetime.datetime(year=2021, month=1, day=1),
            end_time=datetime.datetime(year=2021, month=1, day=2),
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
                start_time=datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc),
                end_time=datetime.datetime(2021, 1, 2, tzinfo=datetime.timezone.utc),
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
