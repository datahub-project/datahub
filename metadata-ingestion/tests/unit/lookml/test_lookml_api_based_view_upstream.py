from unittest.mock import MagicMock, patch

import pytest
from looker_sdk.sdk.api40.models import WriteQuery

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_common import (
    LookerViewId,
    ViewField,
    ViewFieldType,
)
from datahub.ingestion.source.looker.looker_constant import (
    NAME,
    VIEW_FIELD_INTERVALS_ATTRIBUTE,
    VIEW_FIELD_TIMEFRAMES_ATTRIBUTE,
    VIEW_FIELD_TYPE_ATTRIBUTE,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.looker_view_id_cache import LookerViewIdCache
from datahub.ingestion.source.looker.lookml_concept_context import (
    LookerFieldContext,
    LookerViewContext,
)
from datahub.ingestion.source.looker.lookml_config import (
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.view_upstream import (
    LookerQueryAPIBasedViewUpstream,
)
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult


def create_mock_sql_parsing_result(
    table_error=None, column_error=None, in_tables=None, column_lineage=None
):
    """Helper function to create a properly mocked SqlParsingResult."""
    mock_spr = MagicMock(spec=SqlParsingResult)
    mock_debug_info = MagicMock()
    mock_debug_info.table_error = table_error
    mock_debug_info.column_error = column_error
    mock_spr.debug_info = mock_debug_info
    mock_spr.in_tables = in_tables or []
    mock_spr.column_lineage = column_lineage or []
    return mock_spr


class TestLookMLAPIBasedViewUpstream:
    """Test suite for LookerQueryAPIBasedViewUpstream functionality."""

    @pytest.fixture
    def mock_view_context(self):
        """Create a mock LookerViewContext for testing."""
        view_context = MagicMock(spec=LookerViewContext)
        view_context.name.return_value = "test_view"
        view_context.base_folder_path = "/test/path"
        view_context.dimensions.return_value = [
            {NAME: "user_id", "type": "string"},
            {NAME: "email", "type": "string"},
        ]
        view_context.measures.return_value = [
            {NAME: "total_users", "type": "number"},
        ]
        view_context.dimension_groups.return_value = []

        # Mock view_connection
        mock_connection = MagicMock()
        mock_connection.default_schema = "public"
        mock_connection.default_db = "test_db"
        mock_connection.platform = "postgres"
        mock_connection.platform_instance = None
        mock_connection.platform_env = None
        view_context.view_connection = mock_connection

        return view_context

    @pytest.fixture
    def mock_looker_view_id_cache(self):
        """Create a mock LookerViewIdCache for testing."""
        cache = MagicMock(spec=LookerViewIdCache)
        cache.model_name = "test_model"
        return cache

    @pytest.fixture
    def mock_config(self):
        """Create a mock LookMLSourceConfig for testing."""
        config = MagicMock(spec=LookMLSourceConfig)
        config.use_api_for_view_lineage = True
        config.use_api_cache_for_view_lineage = False
        config.env = "PROD"
        return config

    @pytest.fixture
    def mock_reporter(self):
        """Create a mock LookMLSourceReport for testing."""
        return MagicMock(spec=LookMLSourceReport)

    @pytest.fixture
    def mock_ctx(self):
        """Create a mock PipelineContext for testing."""
        ctx = MagicMock(spec=PipelineContext)
        ctx.graph = MagicMock()
        return ctx

    @pytest.fixture
    def mock_looker_client(self):
        """Create a mock LookerAPI client for testing."""
        client = MagicMock(spec=LookerAPI)
        return client

    @pytest.fixture
    def view_to_explore_map(self):
        """Create a view to explore mapping for testing."""
        return {"test_view": "test_explore"}

    @pytest.fixture
    def upstream_instance(
        self,
        mock_view_context,
        mock_looker_view_id_cache,
        mock_config,
        mock_reporter,
        mock_ctx,
        mock_looker_client,
        view_to_explore_map,
    ):
        """Create a LookerQueryAPIBasedViewUpstream instance for testing."""
        # Mock the API response to prevent initialization errors
        mock_looker_client.generate_sql_query.return_value = [
            {"sql": "SELECT test_view.user_id FROM test_table"}
        ]

        # Mock the view ID cache
        mock_view_id = MagicMock(spec=LookerViewId)
        mock_view_id.get_urn.return_value = "urn:li:dataset:test"
        mock_looker_view_id_cache.get_looker_view_id.return_value = mock_view_id

        with patch(
            "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
        ) as mock_create_lineage:
            # Mock successful SQL parsing
            mock_spr = create_mock_sql_parsing_result()
            mock_create_lineage.return_value = mock_spr

            return LookerQueryAPIBasedViewUpstream(
                view_context=mock_view_context,
                looker_view_id_cache=mock_looker_view_id_cache,
                config=mock_config,
                reporter=mock_reporter,
                ctx=mock_ctx,
                looker_client=mock_looker_client,
                view_to_explore_map=view_to_explore_map,
            )

    def test_time_dimension_group_handling(self, upstream_instance):
        """Test that time dimension groups are handled correctly."""
        dim_group = {
            NAME: "created",
            VIEW_FIELD_TYPE_ATTRIBUTE: "time",
            VIEW_FIELD_TIMEFRAMES_ATTRIBUTE: ["date", "week", "month"],
        }

        result = upstream_instance._get_time_dim_group_field_name(dim_group)
        assert result == "created_date"

    def test_time_dimension_group_without_timeframes(self, upstream_instance):
        """Test time dimension group handling when timeframes are not specified."""
        dim_group = {
            NAME: "created",
            VIEW_FIELD_TYPE_ATTRIBUTE: "time",
        }

        result = upstream_instance._get_time_dim_group_field_name(dim_group)
        assert result == "created_raw"

    def test_duration_dimension_group_handling(self, upstream_instance):
        """Test that duration dimension groups are handled correctly."""
        dim_group = {
            NAME: "since_event",
            VIEW_FIELD_TYPE_ATTRIBUTE: "duration",
            VIEW_FIELD_INTERVALS_ATTRIBUTE: ["hour", "day", "week"],
        }

        result = upstream_instance._get_duration_dim_group_field_name(dim_group)
        assert result == "hours_since_event"

    def test_duration_dimension_group_without_intervals(self, upstream_instance):
        """Test duration dimension group handling when intervals are not specified."""
        dim_group = {
            NAME: "since_event",
            VIEW_FIELD_TYPE_ATTRIBUTE: "duration",
        }

        result = upstream_instance._get_duration_dim_group_field_name(dim_group)
        assert result == "days_since_event"

    def test_get_looker_api_field_name(self, upstream_instance):
        """Test field name translation to Looker API format."""
        result = upstream_instance._get_looker_api_field_name("user_id")
        assert result == "test_view.user_id"

    def test_get_field_name_from_looker_api_field_name(self, upstream_instance):
        """Test field name translation from Looker API format."""
        result = upstream_instance._get_field_name_from_looker_api_field_name(
            "test_view.user_id"
        )
        assert result == "user_id"

    def test_get_field_name_from_looker_api_field_name_mismatch(
        self, upstream_instance
    ):
        """Test field name translation when view name doesn't match."""
        result = upstream_instance._get_field_name_from_looker_api_field_name(
            "other_view.user_id"
        )
        assert result == "other_view.user_id"

    def test_get_sql_write_query_success(self, upstream_instance):
        """Test successful WriteQuery construction."""
        query = upstream_instance._get_sql_write_query()

        assert isinstance(query, WriteQuery)
        assert query.model == "test_model"
        assert query.view == "test_explore"
        assert query.limit == "1"
        assert query.fields is not None
        assert "test_view.user_id" in query.fields
        assert "test_view.email" in query.fields
        assert "test_view.total_users" in query.fields

    def test_get_sql_write_query_no_fields(self, upstream_instance, mock_view_context):
        """Test WriteQuery construction when no fields are found."""
        mock_view_context.dimensions.return_value = []
        mock_view_context.measures.return_value = []
        mock_view_context.dimension_groups.return_value = []

        with pytest.raises(ValueError, match="No fields found for view"):
            upstream_instance._get_sql_write_query()

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_execute_query_success(
        self, mock_create_lineage, upstream_instance, mock_looker_client
    ):
        """Test successful query execution."""
        # Mock the SQL response
        mock_sql_response = "SELECT test_view.user_id FROM test_table"
        mock_looker_client.generate_sql_query.return_value = mock_sql_response

        # Mock the SQL parsing result
        mock_spr = create_mock_sql_parsing_result(
            in_tables=["urn:li:dataset:(urn:li:dataPlatform:postgres,test_table,PROD)"]
        )
        mock_create_lineage.return_value = mock_spr

        result = upstream_instance._execute_query(MagicMock(spec=WriteQuery))
        assert result == "SELECT test_view.user_id FROM test_table"

    def test_execute_query_no_sql_response(self, upstream_instance, mock_looker_client):
        """Test query execution when no SQL is returned."""
        mock_looker_client.generate_sql_query.return_value = []

        with pytest.raises(ValueError, match="No SQL found in response"):
            upstream_instance._execute_query(MagicMock(spec=WriteQuery))

    def test_execute_query_invalid_response_format(
        self, upstream_instance, mock_looker_client
    ):
        """Test query execution with invalid response format."""
        mock_looker_client.generate_sql_query.return_value = None

        with pytest.raises(ValueError, match="No SQL found in response"):
            upstream_instance._execute_query(MagicMock(spec=WriteQuery))

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_get_spr_table_error(
        self, mock_create_lineage, upstream_instance, mock_looker_client
    ):
        """Test SQL parsing result when table extraction fails."""
        # Clear the cache to force re-execution
        upstream_instance._get_spr.cache_clear()

        # Mock the SQL response
        mock_sql_response = [{"sql": "SELECT * FROM test_table"}]
        mock_looker_client.generate_sql_query.return_value = mock_sql_response

        # Mock the SQL parsing result with table error
        mock_spr = create_mock_sql_parsing_result(
            table_error=Exception("Table parsing failed")
        )
        mock_create_lineage.return_value = mock_spr

        with pytest.raises(
            ValueError, match="Error in parsing SQL for upstream tables"
        ):
            upstream_instance._get_spr()

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_get_spr_column_error(
        self, mock_create_lineage, upstream_instance, mock_looker_client
    ):
        """Test SQL parsing result when column extraction fails."""
        # Clear the cache to force re-execution
        upstream_instance._get_spr.cache_clear()

        # Mock the SQL response
        mock_sql_response = [{"sql": "SELECT * FROM test_table"}]
        mock_looker_client.generate_sql_query.return_value = mock_sql_response

        # Mock the SQL parsing result with column error
        mock_spr = create_mock_sql_parsing_result(
            column_error=Exception("Column parsing failed")
        )
        mock_create_lineage.return_value = mock_spr

        with pytest.raises(ValueError, match="Error in parsing SQL for column lineage"):
            upstream_instance._get_spr()

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_get_upstream_dataset_urn(
        self, mock_create_lineage, upstream_instance, mock_looker_client
    ):
        """Test upstream dataset URN extraction."""
        # Clear all caches to force re-execution
        upstream_instance._get_spr.cache_clear()
        upstream_instance._get_upstream_dataset_urn.cache_clear()

        # Mock the SQL response
        mock_sql_response = [{"sql": "SELECT * FROM test_table"}]
        mock_looker_client.generate_sql_query.return_value = mock_sql_response

        # Mock the SQL parsing result
        mock_spr = create_mock_sql_parsing_result(
            in_tables=["urn:li:dataset:(urn:li:dataPlatform:postgres,test_table,PROD)"]
        )
        mock_create_lineage.return_value = mock_spr

        result = upstream_instance.get_upstream_dataset_urn()
        assert len(result) == 1
        assert "test_table" in result[0]

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_get_upstream_column_ref(
        self, mock_create_lineage, upstream_instance, mock_looker_client
    ):
        """Test upstream column reference extraction."""
        # Clear the cache to force re-execution
        upstream_instance._get_spr.cache_clear()

        # Mock the SQL response
        mock_sql_response = [{"sql": "SELECT user_id FROM test_table"}]
        mock_looker_client.generate_sql_query.return_value = mock_sql_response

        # Mock the SQL parsing result with column lineage
        mock_column_lineage = [
            MagicMock(
                downstream=MagicMock(column="test_view.user_id"),
                upstreams=[MagicMock(table="test_table", column="user_id")],
            )
        ]
        mock_spr = create_mock_sql_parsing_result(column_lineage=mock_column_lineage)
        mock_create_lineage.return_value = mock_spr

        # Mock field context
        field_context = MagicMock(spec=LookerFieldContext)
        field_context.name.return_value = "user_id"
        field_context.raw_field = {NAME: "user_id"}

        result = upstream_instance.get_upstream_column_ref(field_context)
        assert len(result) == 1
        assert result[0].table == "test_table"
        assert result[0].column == "user_id"

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_get_upstream_column_ref_dimension_group(
        self, mock_create_lineage, upstream_instance, mock_looker_client
    ):
        """Test upstream column reference extraction for dimension groups."""
        # Clear the cache to force re-execution
        upstream_instance._get_spr.cache_clear()

        # Mock the SQL response
        mock_sql_response = [{"sql": "SELECT created_date FROM test_table"}]
        mock_looker_client.generate_sql_query.return_value = mock_sql_response

        # Mock the SQL parsing result with column lineage
        mock_column_lineage = [
            MagicMock(
                downstream=MagicMock(column="test_view.created_date"),
                upstreams=[MagicMock(table="test_table", column="created_at")],
            )
        ]
        mock_spr = create_mock_sql_parsing_result(column_lineage=mock_column_lineage)
        mock_create_lineage.return_value = mock_spr

        # Mock field context for time dimension group
        field_context = MagicMock(spec=LookerFieldContext)
        field_context.name.return_value = "created"
        field_context.raw_field = {
            NAME: "created",
            VIEW_FIELD_TYPE_ATTRIBUTE: "time",
            VIEW_FIELD_TIMEFRAMES_ATTRIBUTE: ["date"],
        }

        result = upstream_instance.get_upstream_column_ref(field_context)
        assert len(result) == 1
        assert result[0].table == "test_table"
        assert result[0].column == "created_at"

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_create_fields(
        self, mock_create_lineage, upstream_instance, mock_looker_client
    ):
        """Test ViewField creation from SQL parsing result."""
        # Clear the cache to force re-execution
        upstream_instance._get_spr.cache_clear()

        # Mock the SQL response
        mock_sql_response = [{"sql": "SELECT user_id FROM test_table"}]
        mock_looker_client.generate_sql_query.return_value = mock_sql_response

        # Mock the SQL parsing result with column lineage
        mock_column_lineage = [
            MagicMock(
                downstream=MagicMock(
                    column="test_view.user_id", native_column_type="string"
                ),
                upstreams=[MagicMock(table="test_table", column="user_id")],
            )
        ]
        mock_spr = create_mock_sql_parsing_result(column_lineage=mock_column_lineage)
        mock_create_lineage.return_value = mock_spr

        result = upstream_instance.create_fields()
        assert len(result) == 1
        assert isinstance(result[0], ViewField)
        assert result[0].name == "user_id"
        assert result[0].type == "string"
        assert result[0].field_type == ViewFieldType.UNKNOWN

    def test_create_fields_no_column_lineage(self, upstream_instance):
        """Test ViewField creation when no column lineage is available."""
        # Mock the SQL parsing result without column lineage
        mock_spr = MagicMock(spec=SqlParsingResult)
        mock_spr.column_lineage = None

        with patch.object(upstream_instance, "_get_spr", return_value=mock_spr):
            result = upstream_instance.create_fields()
            assert result == []

    def test_api_failure_fallback(
        self,
        mock_view_context,
        mock_looker_view_id_cache,
        mock_config,
        mock_reporter,
        mock_ctx,
        mock_looker_client,
        view_to_explore_map,
    ):
        """Test that API failures are handled gracefully."""
        # Mock the Looker client to raise an exception
        mock_looker_client.generate_sql_query.side_effect = Exception("API call failed")

        # This should not raise an exception, but should be handled by the fallback mechanism
        # in the factory function
        with pytest.raises(Exception, match="API call failed"):
            LookerQueryAPIBasedViewUpstream(
                view_context=mock_view_context,
                looker_view_id_cache=mock_looker_view_id_cache,
                config=mock_config,
                reporter=mock_reporter,
                ctx=mock_ctx,
                looker_client=mock_looker_client,
                view_to_explore_map=view_to_explore_map,
            )

    def test_latency_tracking(
        self, upstream_instance, mock_looker_client, mock_reporter
    ):
        """Test that API latency is tracked and reported."""
        # Clear the cache to force re-execution
        upstream_instance._get_spr.cache_clear()

        # Mock the SQL response
        mock_sql_response = [{"sql": "SELECT * FROM test_table"}]
        mock_looker_client.generate_sql_query.return_value = mock_sql_response

        # Mock the view ID cache to return a valid view ID
        mock_view_id = MagicMock(spec=LookerViewId)
        mock_view_id.get_urn.return_value = "urn:li:dataset:test"
        upstream_instance.looker_view_id_cache.get_looker_view_id.return_value = (
            mock_view_id
        )

        with patch(
            "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
        ) as mock_create_lineage:
            mock_spr = create_mock_sql_parsing_result()
            mock_create_lineage.return_value = mock_spr

            upstream_instance._execute_query(MagicMock(spec=WriteQuery))

            # Verify that latency was reported (may be called multiple times due to caching)
            assert mock_reporter.report_looker_query_api_latency.call_count >= 1
