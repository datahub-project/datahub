"""Unit tests for Snowflake Semantic View usage and query extraction."""

import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from datahub.configuration.time_window_config import BucketDuration
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowflake.snowflake_config import (
    SemanticViewsConfig,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SemanticViewQuery,
    SemanticViewUsageRecord,
    UserQueryCount,
)
from datahub.ingestion.source.snowflake.snowflake_semantic_view_usage import (
    SemanticViewUsageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetUsageStatistics
from datahub.metadata.com.linkedin.pegasus2avro.query import QuerySubjects


class TestSemanticViewsConfig:
    """Tests for SemanticViewsConfig configuration."""

    def test_warning_usage_without_enabled(self):
        """Test that warnings are logged when usage is enabled but semantic_views is not."""
        with patch(
            "datahub.ingestion.source.snowflake.snowflake_config.logger"
        ) as mock_logger:
            SemanticViewsConfig(
                enabled=False,
                include_usage=True,
            )
            mock_logger.warning.assert_called()

    def test_include_usage_with_emit_semantic_model_entities_no_warning(self):
        """include_usage is fully supported in semanticModel mode; enabling both
        flags together must not log a warning."""
        with patch(
            "datahub.ingestion.source.snowflake.snowflake_config.logger"
        ) as mock_logger:
            SemanticViewsConfig(
                enabled=True,
                include_usage=True,
                emit_semantic_model_entities=True,
            )
            mock_logger.warning.assert_not_called()

    def test_max_queries_per_view_validation(self):
        """Test that max_queries_per_view validation rejects invalid values."""
        with pytest.raises(ValidationError):
            SemanticViewsConfig(max_queries_per_view=0)

        with pytest.raises(ValidationError):
            SemanticViewsConfig(max_queries_per_view=-1)

        with pytest.raises(ValidationError):
            SemanticViewsConfig(max_queries_per_view=10001)


class TestSemanticModelEntitiesRequiresTechnicalSchema:
    """emit_semantic_model_entities lives on the nested SemanticViewsConfig, while
    include_technical_schema is top-level; the validator must live on
    SnowflakeV2Config, where both are visible."""

    def test_warns_when_technical_schema_disabled(self):
        with patch(
            "datahub.ingestion.source.snowflake.snowflake_config.logger"
        ) as mock_logger:
            SnowflakeV2Config.model_validate(
                {
                    "account_id": "test_account",
                    "username": "user",
                    "password": "pass",
                    "include_technical_schema": False,
                    "semantic_views": {"emit_semantic_model_entities": True},
                }
            )
            mock_logger.warning.assert_called()

    def test_no_warning_when_technical_schema_enabled(self):
        with patch(
            "datahub.ingestion.source.snowflake.snowflake_config.logger"
        ) as mock_logger:
            SnowflakeV2Config.model_validate(
                {
                    "account_id": "test_account",
                    "username": "user",
                    "password": "pass",
                    "include_technical_schema": True,
                    "semantic_views": {"emit_semantic_model_entities": True},
                }
            )
            mock_logger.warning.assert_not_called()


class TestSemanticModelEntitiesUsageCasingWarning:
    """Belt-and-suspenders warning for a casing mode that is more prone to URN
    mismatches: emit_semantic_model_entities + include_usage with
    convert_urns_to_lowercase=False."""

    def test_warns_when_lowercasing_disabled(self):
        with patch(
            "datahub.ingestion.source.snowflake.snowflake_config.logger"
        ) as mock_logger:
            SnowflakeV2Config.model_validate(
                {
                    "account_id": "test_account",
                    "username": "user",
                    "password": "pass",
                    "convert_urns_to_lowercase": False,
                    "semantic_views": {
                        "enabled": True,
                        "emit_semantic_model_entities": True,
                        "include_usage": True,
                    },
                }
            )
            assert any(
                "convert_urns_to_lowercase" in str(call.args)
                for call in mock_logger.warning.call_args_list
            )

    def test_no_warning_when_lowercasing_enabled(self):
        with patch(
            "datahub.ingestion.source.snowflake.snowflake_config.logger"
        ) as mock_logger:
            SnowflakeV2Config.model_validate(
                {
                    "account_id": "test_account",
                    "username": "user",
                    "password": "pass",
                    "convert_urns_to_lowercase": True,
                    "semantic_views": {
                        "enabled": True,
                        "emit_semantic_model_entities": True,
                        "include_usage": True,
                    },
                }
            )
            assert not any(
                "casing mismatches" in str(call.args)
                for call in mock_logger.warning.call_args_list
            )


class TestSnowflakeQuerySemanticViewUsage:
    """Tests for SQL query generation."""

    def test_semantic_view_usage_statistics_query_daily(self):
        """Test usage statistics query generation with daily buckets."""
        query = SnowflakeQuery.semantic_view_usage_statistics(
            start_time_millis=1704067200000,  # 2024-01-01
            end_time_millis=1704153600000,  # 2024-01-02
            time_bucket_size=BucketDuration.DAY,
        )

        # Verify QUERY_HISTORY with pattern matching is used
        assert "query_history" in query.lower()
        assert "SEMANTIC_VIEW(" in query
        # Verify aggregation settings
        assert "DAY" in query
        assert "bucket_start_time" in query.lower()
        # Verify Cortex Analyst detection
        assert "Generated by Cortex Analyst" in query
        assert "DIRECT_SQL_QUERIES" in query
        assert "CORTEX_ANALYST_QUERIES" in query

    def test_semantic_view_usage_statistics_query_hourly(self):
        """Test usage statistics query generation with hourly buckets."""
        query = SnowflakeQuery.semantic_view_usage_statistics(
            start_time_millis=1704067200000,
            end_time_millis=1704153600000,
            time_bucket_size=BucketDuration.HOUR,
        )

        assert "HOUR" in query
        assert "query_history" in query.lower()
        assert "SEMANTIC_VIEW(" in query

    def test_semantic_view_queries(self):
        """Test queries SQL generation."""
        query = SnowflakeQuery.semantic_view_queries(
            start_time_millis=1704067200000,
            end_time_millis=1704153600000,
            max_queries=50,
        )

        assert "query_history" in query.lower()
        assert "SEMANTIC_VIEW(" in query
        # Verify query source detection
        assert "QUERY_SOURCE" in query
        assert "Generated by Cortex Analyst" in query
        assert "LIMIT 50" in query


class TestSemanticViewDataModels:
    """Tests for semantic view usage and query data models."""

    def test_semantic_view_usage_record(self):
        """Test SemanticViewUsageRecord creation."""
        record = SemanticViewUsageRecord(
            semantic_view_name="db.schema.sales_view",
            bucket_start_time=datetime.datetime(
                2024, 1, 1, tzinfo=datetime.timezone.utc
            ),
            total_queries=100,
            unique_users=10,
            direct_sql_queries=80,
            cortex_analyst_queries=20,
            avg_execution_time_ms=150.5,
            total_rows_produced=5000,
            user_counts=[UserQueryCount(user_name="analyst", query_count=50)],
        )

        assert record.semantic_view_name == "db.schema.sales_view"
        assert record.total_queries == 100
        assert record.direct_sql_queries == 80
        assert record.cortex_analyst_queries == 20

    def test_semantic_view_query(self):
        """Test SemanticViewQuery creation."""
        query = SemanticViewQuery(
            query_id="query-123",
            query_text="SELECT * FROM SEMANTIC_VIEW(db.schema.view ...)",
            semantic_view_name="db.schema.view",
            user_name="analyst",
            role_name="ANALYST_ROLE",
            warehouse_name="COMPUTE_WH",
            start_time=datetime.datetime(
                2024, 1, 1, 10, 30, tzinfo=datetime.timezone.utc
            ),
            total_elapsed_time=1500,
            rows_produced=100,
            query_source="DIRECT_SQL",
        )

        assert query.query_id == "query-123"
        assert query.user_name == "analyst"
        assert query.total_elapsed_time == 1500
        assert query.query_source == "DIRECT_SQL"


class TestSemanticViewUsageExtractor:
    """Tests for SemanticViewUsageExtractor.

    The default config exercises legacy dataset mode (emit_semantic_model_entities=False),
    matching the connector's default. Tests specific to emit_semantic_model_entities=True
    flip that flag explicitly on the extractor's config.
    """

    @pytest.fixture
    def mock_config(self) -> SnowflakeV2Config:
        """Create a mock Snowflake config."""
        config = MagicMock(spec=SnowflakeV2Config)
        config.semantic_views = SemanticViewsConfig(
            enabled=True,
            include_usage=True,
            include_queries=True,
            max_queries_per_view=100,
        )
        config.start_time = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        config.end_time = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
        config.bucket_duration = BucketDuration.DAY
        config.email_domain = "test.com"
        return config

    @pytest.fixture
    def mock_connection(self) -> MagicMock:
        """Create a mock Snowflake connection."""
        return MagicMock()

    @pytest.fixture
    def mock_identifiers(self) -> MagicMock:
        """Create a mock identifier builder."""
        identifiers = MagicMock(spec=SnowflakeIdentifierBuilder)
        identifiers.gen_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.schema.view,PROD)"
        )
        identifiers.gen_semantic_model_urn.return_value = (
            "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test.schema,view)"
        )
        identifiers.get_user_identifier.return_value = "analyst@test.com"
        return identifiers

    @pytest.fixture
    def extractor(
        self,
        mock_config: SnowflakeV2Config,
        mock_connection: MagicMock,
        mock_identifiers: MagicMock,
    ) -> SemanticViewUsageExtractor:
        """Create a SemanticViewUsageExtractor instance."""
        report = SnowflakeV2Report()
        return SemanticViewUsageExtractor(
            config=mock_config,
            report=report,
            connection=mock_connection,
            identifiers=mock_identifiers,
        )

    def test_normalize_semantic_view_name(
        self, extractor: SemanticViewUsageExtractor
    ) -> None:
        """Test semantic view name normalization."""
        assert (
            extractor._normalize_semantic_view_name("DB.SCHEMA.VIEW")
            == "db.schema.view"
        )
        assert (
            extractor._normalize_semantic_view_name("db.schema.view")
            == "db.schema.view"
        )

    def test_build_identifier_lookup_maps_lowercase_to_canonical(
        self, extractor: SemanticViewUsageExtractor
    ) -> None:
        """The lookup must map the lowercased key back to the exact, original
        (potentially mixed-case) identifier discovered during schema generation."""
        discovered = {"TEST_DB.PUBLIC.Sales_View", "other_db.other_schema.other_view"}

        lookup = extractor._build_identifier_lookup(discovered)

        assert lookup["test_db.public.sales_view"] == "TEST_DB.PUBLIC.Sales_View"
        assert lookup["other_db.other_schema.other_view"] == (
            "other_db.other_schema.other_view"
        )
        assert len(lookup) == 2

    def test_semantic_model_urn_from_identifier_splits_db_schema_view(
        self, extractor: SemanticViewUsageExtractor, mock_identifiers: MagicMock
    ) -> None:
        """The semanticModel URN helper must split db.schema.view correctly."""
        urn = extractor._semantic_model_urn_from_identifier("test_db.public.sales_view")

        mock_identifiers.gen_semantic_model_urn.assert_called_once_with(
            "sales_view", "public", "test_db"
        )
        assert urn == mock_identifiers.gen_semantic_model_urn.return_value

    def test_generate_query_name_with_semantic_view(
        self, extractor: SemanticViewUsageExtractor
    ) -> None:
        """Test query name generation from SQL with SEMANTIC_VIEW."""
        query_text = (
            "SELECT * FROM SEMANTIC_VIEW(db.schema.sales_view DIMENSIONS region)"
        )
        name = extractor._generate_query_name(query_text)
        assert "Query on sales_view" in name

    def test_generate_query_name_fallback(
        self, extractor: SemanticViewUsageExtractor
    ) -> None:
        """Test query name generation fallback."""
        query_text = "SELECT * FROM some_table"
        name = extractor._generate_query_name(query_text, max_length=20)
        assert len(name) <= 20
        assert name.endswith("...")

    def test_parse_usage_results(self, extractor: SemanticViewUsageExtractor) -> None:
        """Test parsing usage results from query."""
        mock_results: List[Dict[str, Any]] = [
            {
                "SEMANTIC_VIEW_NAME": "db.schema.sales_view",
                "BUCKET_START_TIME": datetime.datetime(
                    2024, 1, 1, tzinfo=datetime.timezone.utc
                ),
                "TOTAL_QUERIES": 50,
                "UNIQUE_USERS": 5,
                "DIRECT_SQL_QUERIES": 40,
                "CORTEX_ANALYST_QUERIES": 10,
                "AVG_EXECUTION_TIME_MS": 200.0,
                "TOTAL_ROWS_PRODUCED": 1000,
                "USER_COUNTS": '[{"user_name": "analyst", "query_count": 30}]',
                "TOP_SQL_QUERIES": '["SELECT * FROM SEMANTIC_VIEW(db.schema.sales_view)", "SELECT region FROM SEMANTIC_VIEW(db.schema.sales_view)"]',
            }
        ]

        records = list(extractor._parse_usage_results(mock_results))

        assert len(records) == 1
        assert records[0].semantic_view_name == "db.schema.sales_view"
        assert records[0].total_queries == 50
        assert records[0].direct_sql_queries == 40
        assert records[0].cortex_analyst_queries == 10
        assert len(records[0].user_counts) == 1
        assert len(records[0].top_sql_queries) == 2
        assert (
            "SELECT * FROM SEMANTIC_VIEW(db.schema.sales_view)"
            in records[0].top_sql_queries
        )

    def test_map_user_counts(self, extractor: SemanticViewUsageExtractor) -> None:
        """Test mapping user counts to DatasetUserUsageCounts."""
        user_counts = [
            UserQueryCount(user_name="analyst1", query_count=30),
            UserQueryCount(user_name="analyst2", query_count=20),
        ]

        result = extractor._map_user_counts(user_counts)

        assert len(result) == 2
        assert result[0].count == 30 or result[0].count == 20

    def test_get_semantic_view_usage_workunits_disabled(
        self, extractor: SemanticViewUsageExtractor
    ) -> None:
        """Test that no workunits are generated when usage is disabled."""
        extractor.config.semantic_views.include_usage = False

        workunits = list(
            extractor.get_semantic_view_usage_workunits({"db.schema.view"})
        )

        assert len(workunits) == 0

    def test_get_semantic_view_usage_workunits_empty_discovered(
        self, extractor: SemanticViewUsageExtractor
    ) -> None:
        """Test that no workunits are generated with empty discovered views."""
        workunits = list(extractor.get_semantic_view_usage_workunits(set()))

        assert len(workunits) == 0

    def test_get_semantic_view_query_workunits_disabled(
        self, extractor: SemanticViewUsageExtractor
    ) -> None:
        """Test that no query workunits are generated when disabled."""
        extractor.config.semantic_views.include_queries = False

        workunits = list(
            extractor.get_semantic_view_query_workunits({"db.schema.view"})
        )

        assert len(workunits) == 0

    def test_build_query_workunits_legacy_dataset_mode(
        self,
        extractor: SemanticViewUsageExtractor,
        mock_identifiers: MagicMock,
    ) -> None:
        """With emit_semantic_model_entities=False (default), QuerySubjects must
        point at the dataset URN (legacy behavior)."""
        query = SemanticViewQuery(
            query_id="query-123",
            query_text="SELECT * FROM SEMANTIC_VIEW(db.schema.view ...)",
            semantic_view_name="db.schema.view",
            user_name="analyst",
            role_name="ANALYST_ROLE",
            warehouse_name="COMPUTE_WH",
            start_time=datetime.datetime(
                2024, 1, 1, 10, 30, tzinfo=datetime.timezone.utc
            ),
            total_elapsed_time=1500,
            rows_produced=100,
            query_source="DIRECT_SQL",
        )

        workunits = list(extractor._build_query_workunits(query, "db.schema.view"))

        # Should emit QueryProperties and QuerySubjects
        assert len(workunits) == 2

        query_props_wu = workunits[0]
        assert isinstance(query_props_wu.metadata, MetadataChangeProposalWrapper)
        assert query_props_wu.metadata.entityUrn is not None
        assert "query" in query_props_wu.metadata.entityUrn

        query_subjects_wu = workunits[1]
        assert isinstance(query_subjects_wu.metadata, MetadataChangeProposalWrapper)
        subjects = query_subjects_wu.metadata.aspect
        assert isinstance(subjects, QuerySubjects)
        assert (
            subjects.subjects[0].entity == mock_identifiers.gen_dataset_urn.return_value
        )

    def test_build_query_workunits_emits_semantic_model_urn(
        self,
        extractor: SemanticViewUsageExtractor,
        mock_identifiers: MagicMock,
    ) -> None:
        """With emit_semantic_model_entities=True, the query subject must be the
        semanticModel URN, not a dataset URN."""
        extractor.config.semantic_views.emit_semantic_model_entities = True

        query = SemanticViewQuery(
            query_id="query-123",
            query_text="SELECT * FROM SEMANTIC_VIEW(db.schema.view ...)",
            semantic_view_name="db.schema.view",
            user_name="analyst",
            role_name="ANALYST_ROLE",
            warehouse_name="COMPUTE_WH",
            start_time=datetime.datetime(
                2024, 1, 1, 10, 30, tzinfo=datetime.timezone.utc
            ),
            total_elapsed_time=1500,
            rows_produced=100,
            query_source="DIRECT_SQL",
        )

        workunits = list(extractor._build_query_workunits(query, "db.schema.view"))

        assert len(workunits) == 2

        query_subjects_wu = workunits[1]
        assert isinstance(query_subjects_wu.metadata, MetadataChangeProposalWrapper)
        subjects = query_subjects_wu.metadata.aspect
        assert isinstance(subjects, QuerySubjects)
        assert subjects.subjects[0].entity == (
            mock_identifiers.gen_semantic_model_urn.return_value
        )

    def test_build_usage_statistics_workunit_legacy_dataset_mode(
        self,
        extractor: SemanticViewUsageExtractor,
        mock_identifiers: MagicMock,
    ) -> None:
        """With emit_semantic_model_entities=False (default), the usage stats
        entityUrn must be the dataset URN (legacy behavior)."""
        record = SemanticViewUsageRecord(
            semantic_view_name="db.schema.view",
            bucket_start_time=datetime.datetime(
                2024, 1, 1, tzinfo=datetime.timezone.utc
            ),
            total_queries=10,
            unique_users=2,
            direct_sql_queries=8,
            cortex_analyst_queries=2,
            avg_execution_time_ms=100.0,
            total_rows_produced=200,
            user_counts=[],
        )

        wu = extractor._build_usage_statistics_workunit(record, "db.schema.view")

        assert wu is not None
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert wu.metadata.entityUrn == mock_identifiers.gen_dataset_urn.return_value

    def test_build_usage_statistics_workunit_emits_semantic_model_urn(
        self,
        extractor: SemanticViewUsageExtractor,
        mock_identifiers: MagicMock,
    ) -> None:
        """With emit_semantic_model_entities=True, the DatasetUsageStatistics MCP's
        entityUrn must be the semanticModel URN, not a dataset URN."""
        extractor.config.semantic_views.emit_semantic_model_entities = True

        record = SemanticViewUsageRecord(
            semantic_view_name="db.schema.view",
            bucket_start_time=datetime.datetime(
                2024, 1, 1, tzinfo=datetime.timezone.utc
            ),
            total_queries=10,
            unique_users=2,
            direct_sql_queries=8,
            cortex_analyst_queries=2,
            avg_execution_time_ms=100.0,
            total_rows_produced=200,
            user_counts=[],
        )

        wu = extractor._build_usage_statistics_workunit(record, "db.schema.view")

        assert wu is not None
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert isinstance(wu.metadata.aspect, DatasetUsageStatistics)
        assert wu.metadata.entityUrn == (
            mock_identifiers.gen_semantic_model_urn.return_value
        )

    def test_query_extraction_respects_max_queries_per_view_efficiently(
        self,
        mock_config: SnowflakeV2Config,
        mock_connection: MagicMock,
        mock_identifiers: MagicMock,
    ) -> None:
        """Test that query extraction limits during iteration to avoid memory issues."""
        # Set a low limit
        mock_config.semantic_views.include_queries = True
        mock_config.semantic_views.max_queries_per_view = 2

        # Create many queries for the same view (simulating high volume)
        base_time = datetime.datetime(2024, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)
        mock_query_results: List[Dict[str, Any]] = [
            {
                "QUERY_ID": f"query-{i}",
                "QUERY_TEXT": f"SELECT {i} FROM SEMANTIC_VIEW(db.schema.view)",
                "SEMANTIC_VIEW_NAME": "db.schema.view",
                "USER_NAME": "analyst",
                "ROLE_NAME": "ROLE",
                "WAREHOUSE_NAME": "WH",
                "START_TIME": base_time + datetime.timedelta(seconds=i),
                "TOTAL_ELAPSED_TIME": 100,
                "ROWS_PRODUCED": 10,
                "QUERY_SOURCE": "DIRECT_SQL",
            }
            for i in range(100)  # 100 queries, but limit is 2
        ]
        mock_connection.query.return_value = mock_query_results

        report = SnowflakeV2Report()
        extractor = SemanticViewUsageExtractor(
            config=mock_config,
            report=report,
            connection=mock_connection,
            identifiers=mock_identifiers,
        )

        discovered = {"db.schema.view"}
        workunits = list(extractor.get_semantic_view_query_workunits(discovered))

        # Should emit exactly 2 queries * 2 aspects (QueryProperties + QuerySubjects)
        assert len(workunits) == 4


class TestSemanticViewUsageIntegration:
    """Integration tests for semantic view usage extraction (legacy dataset mode)."""

    @patch(
        "datahub.ingestion.source.snowflake.snowflake_semantic_view_usage.SnowflakeConnection"
    )
    def test_usage_extraction_end_to_end(self, mock_connection_class):
        """Test end-to-end usage extraction with mocked Snowflake."""
        # Setup mock config
        config = MagicMock(spec=SnowflakeV2Config)
        config.semantic_views = SemanticViewsConfig(
            enabled=True,
            include_usage=True,
            include_queries=True,
            max_queries_per_view=10,
        )
        config.start_time = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        config.end_time = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
        config.bucket_duration = BucketDuration.DAY
        config.email_domain = "test.com"

        # Setup mock connection
        mock_connection = MagicMock()
        mock_usage_results = MagicMock()
        mock_usage_results.__iter__ = lambda self: iter(
            [
                {
                    "SEMANTIC_VIEW_NAME": "test_db.public.sales_view",
                    "BUCKET_START_TIME": datetime.datetime(
                        2024, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "TOTAL_QUERIES": 25,
                    "UNIQUE_USERS": 3,
                    "DIRECT_SQL_QUERIES": 20,
                    "CORTEX_ANALYST_QUERIES": 5,
                    "AVG_EXECUTION_TIME_MS": 150.0,
                    "TOTAL_ROWS_PRODUCED": 500,
                    "USER_COUNTS": "[]",
                    "TOP_SQL_QUERIES": [
                        "SELECT * FROM SEMANTIC_VIEW(test_db.public.sales_view)"
                    ],
                }
            ]
        )
        mock_connection.query.return_value = mock_usage_results

        # Setup mock identifiers
        mock_identifiers = MagicMock(spec=SnowflakeIdentifierBuilder)
        mock_identifiers.gen_dataset_urn.return_value = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.sales_view,PROD)"
        mock_identifiers.get_user_identifier.return_value = "test@test.com"

        # Create extractor
        report = SnowflakeV2Report()
        extractor = SemanticViewUsageExtractor(
            config=config,
            report=report,
            connection=mock_connection,
            identifiers=mock_identifiers,
        )

        # Run extraction
        discovered = {"test_db.public.sales_view"}
        workunits = list(extractor.get_semantic_view_usage_workunits(discovered))

        # Verify
        assert len(workunits) == 1
        metadata = workunits[0].metadata
        assert isinstance(metadata, MetadataChangeProposalWrapper)
        usage_stats = metadata.aspect
        assert isinstance(usage_stats, DatasetUsageStatistics)
        assert usage_stats.topSqlQueries is not None
        assert len(usage_stats.topSqlQueries) == 1
        assert (
            "SELECT * FROM SEMANTIC_VIEW(test_db.public.sales_view)"
            in usage_stats.topSqlQueries
        )

    @patch(
        "datahub.ingestion.source.snowflake.snowflake_semantic_view_usage.SnowflakeConnection"
    )
    def test_usage_extraction_end_to_end_semantic_model_mode(
        self, mock_connection_class
    ):
        """With emit_semantic_model_entities=True, the emitted DatasetUsageStatistics
        MCP's entityUrn must be the semanticModel URN, not a dataset URN."""
        config = MagicMock(spec=SnowflakeV2Config)
        config.semantic_views = SemanticViewsConfig(
            enabled=True,
            include_usage=True,
            include_queries=True,
            emit_semantic_model_entities=True,
            max_queries_per_view=10,
        )
        config.start_time = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        config.end_time = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
        config.bucket_duration = BucketDuration.DAY
        config.email_domain = "test.com"

        mock_connection = MagicMock()
        mock_usage_results = MagicMock()
        mock_usage_results.__iter__ = lambda self: iter(
            [
                {
                    "SEMANTIC_VIEW_NAME": "test_db.public.sales_view",
                    "BUCKET_START_TIME": datetime.datetime(
                        2024, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "TOTAL_QUERIES": 25,
                    "UNIQUE_USERS": 3,
                    "DIRECT_SQL_QUERIES": 20,
                    "CORTEX_ANALYST_QUERIES": 5,
                    "AVG_EXECUTION_TIME_MS": 150.0,
                    "TOTAL_ROWS_PRODUCED": 500,
                    "USER_COUNTS": "[]",
                    "TOP_SQL_QUERIES": [
                        "SELECT * FROM SEMANTIC_VIEW(test_db.public.sales_view)"
                    ],
                }
            ]
        )
        mock_connection.query.return_value = mock_usage_results

        # Use a real identifier builder so the emitted URN reflects the actual
        # gen_semantic_model_urn implementation, not a mock stand-in.
        identifier_config = SnowflakeV2Config.model_validate(
            {
                "account_id": "test_account",
                "username": "user",
                "password": "pass",
            }
        )
        identifiers = SnowflakeIdentifierBuilder(identifier_config, SnowflakeV2Report())

        report = SnowflakeV2Report()
        extractor = SemanticViewUsageExtractor(
            config=config,
            report=report,
            connection=mock_connection,
            identifiers=identifiers,
        )

        discovered = {"test_db.public.sales_view"}
        workunits = list(extractor.get_semantic_view_usage_workunits(discovered))

        assert len(workunits) == 1
        metadata = workunits[0].metadata
        assert isinstance(metadata, MetadataChangeProposalWrapper)
        assert isinstance(metadata.aspect, DatasetUsageStatistics)
        assert metadata.entityUrn == (
            "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,test_db.public,sales_view)"
        )


class TestSemanticModelUrnCasingConsistency:
    """Regression coverage for the usage/query semanticModel URN case mismatch:
    with convert_urns_to_lowercase=False, discovered_semantic_views retains the
    original (mixed-case) Snowflake identifiers, matching what schema generation
    used to mint the semanticModel URN. QUERY_HISTORY-derived names must still
    match case-insensitively, but the emitted URN must be built from the
    canonical discovered identifier, not a freshly-lowercased match key -
    otherwise it silently diverges from the schema-gen URN (a ghost URN that
    usage stats/queries attach to instead of the real semanticModel entity)."""

    def _make_identifiers(
        self, convert_urns_to_lowercase: bool
    ) -> SnowflakeIdentifierBuilder:
        identifier_config = SnowflakeV2Config.model_validate(
            {
                "account_id": "test_account",
                "username": "user",
                "password": "pass",
                "convert_urns_to_lowercase": convert_urns_to_lowercase,
            }
        )
        return SnowflakeIdentifierBuilder(identifier_config, SnowflakeV2Report())

    def _make_config(self) -> MagicMock:
        config = MagicMock(spec=SnowflakeV2Config)
        config.semantic_views = SemanticViewsConfig(
            enabled=True,
            include_usage=True,
            include_queries=True,
            emit_semantic_model_entities=True,
            max_queries_per_view=10,
        )
        config.start_time = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        config.end_time = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
        config.bucket_duration = BucketDuration.DAY
        config.email_domain = "test.com"
        return config

    def test_usage_stats_urn_matches_schema_gen_when_lowercasing_disabled(self):
        # Mirrors a real Snowflake semantic view: schema generation discovers it
        # with its original (unquoted-uppercase) casing when
        # convert_urns_to_lowercase=False, while QUERY_HISTORY may report a
        # different case for the same identifier.
        identifiers = self._make_identifiers(convert_urns_to_lowercase=False)
        schema_gen_identifier = "TEST_DB.PUBLIC.SALES_VIEW"
        expected_urn = identifiers.gen_semantic_model_urn(
            "SALES_VIEW", "PUBLIC", "TEST_DB"
        )

        config = self._make_config()
        mock_connection = MagicMock()
        mock_usage_results = MagicMock()
        mock_usage_results.__iter__ = lambda self: iter(
            [
                {
                    "SEMANTIC_VIEW_NAME": "test_db.public.sales_view",
                    "BUCKET_START_TIME": datetime.datetime(
                        2024, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "TOTAL_QUERIES": 25,
                    "UNIQUE_USERS": 3,
                    "DIRECT_SQL_QUERIES": 20,
                    "CORTEX_ANALYST_QUERIES": 5,
                    "AVG_EXECUTION_TIME_MS": 150.0,
                    "TOTAL_ROWS_PRODUCED": 500,
                    "USER_COUNTS": "[]",
                    "TOP_SQL_QUERIES": [],
                }
            ]
        )
        mock_connection.query.return_value = mock_usage_results

        extractor = SemanticViewUsageExtractor(
            config=config,
            report=SnowflakeV2Report(),
            connection=mock_connection,
            identifiers=identifiers,
        )

        workunits = list(
            extractor.get_semantic_view_usage_workunits({schema_gen_identifier})
        )

        assert len(workunits) == 1
        metadata = workunits[0].metadata
        assert isinstance(metadata, MetadataChangeProposalWrapper)
        assert isinstance(metadata.aspect, DatasetUsageStatistics)
        assert metadata.entityUrn == expected_urn

    def test_query_subjects_urn_matches_schema_gen_when_lowercasing_disabled(self):
        identifiers = self._make_identifiers(convert_urns_to_lowercase=False)
        schema_gen_identifier = "TEST_DB.PUBLIC.SALES_VIEW"
        expected_urn = identifiers.gen_semantic_model_urn(
            "SALES_VIEW", "PUBLIC", "TEST_DB"
        )

        config = self._make_config()
        mock_connection = MagicMock()
        mock_query_results = MagicMock()
        mock_query_results.__iter__ = lambda self: iter(
            [
                {
                    "QUERY_ID": "query-123",
                    "QUERY_TEXT": "SELECT * FROM SEMANTIC_VIEW(test_db.public.sales_view ...)",
                    "SEMANTIC_VIEW_NAME": "test_db.public.sales_view",
                    "USER_NAME": "analyst",
                    "ROLE_NAME": "ANALYST_ROLE",
                    "WAREHOUSE_NAME": "COMPUTE_WH",
                    "START_TIME": datetime.datetime(
                        2024, 1, 1, 10, 30, tzinfo=datetime.timezone.utc
                    ),
                    "TOTAL_ELAPSED_TIME": 1500,
                    "ROWS_PRODUCED": 100,
                    "QUERY_SOURCE": "DIRECT_SQL",
                }
            ]
        )
        mock_connection.query.return_value = mock_query_results

        extractor = SemanticViewUsageExtractor(
            config=config,
            report=SnowflakeV2Report(),
            connection=mock_connection,
            identifiers=identifiers,
        )

        workunits = list(
            extractor.get_semantic_view_query_workunits({schema_gen_identifier})
        )

        query_subjects_wus = [
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, QuerySubjects)
        ]
        assert len(query_subjects_wus) == 1
        query_subjects_metadata = query_subjects_wus[0].metadata
        assert isinstance(query_subjects_metadata, MetadataChangeProposalWrapper)
        subjects = query_subjects_metadata.aspect
        assert isinstance(subjects, QuerySubjects)
        assert subjects.subjects[0].entity == expected_urn

    def test_usage_stats_urn_matches_schema_gen_default_lowercase_mode(self):
        """Matching test for the default (convert_urns_to_lowercase=True) mode:
        discovered_semantic_views is already lowercased by schema generation, and
        the fix must not regress this existing behavior."""
        identifiers = self._make_identifiers(convert_urns_to_lowercase=True)
        schema_gen_identifier = "test_db.public.sales_view"
        expected_urn = identifiers.gen_semantic_model_urn(
            "sales_view", "public", "test_db"
        )

        config = self._make_config()
        mock_connection = MagicMock()
        mock_usage_results = MagicMock()
        mock_usage_results.__iter__ = lambda self: iter(
            [
                {
                    "SEMANTIC_VIEW_NAME": "TEST_DB.PUBLIC.SALES_VIEW",
                    "BUCKET_START_TIME": datetime.datetime(
                        2024, 1, 1, tzinfo=datetime.timezone.utc
                    ),
                    "TOTAL_QUERIES": 25,
                    "UNIQUE_USERS": 3,
                    "DIRECT_SQL_QUERIES": 20,
                    "CORTEX_ANALYST_QUERIES": 5,
                    "AVG_EXECUTION_TIME_MS": 150.0,
                    "TOTAL_ROWS_PRODUCED": 500,
                    "USER_COUNTS": "[]",
                    "TOP_SQL_QUERIES": [],
                }
            ]
        )
        mock_connection.query.return_value = mock_usage_results

        extractor = SemanticViewUsageExtractor(
            config=config,
            report=SnowflakeV2Report(),
            connection=mock_connection,
            identifiers=identifiers,
        )

        workunits = list(
            extractor.get_semantic_view_usage_workunits({schema_gen_identifier})
        )

        assert len(workunits) == 1
        metadata = workunits[0].metadata
        assert isinstance(metadata, MetadataChangeProposalWrapper)
        assert metadata.entityUrn == expected_urn
