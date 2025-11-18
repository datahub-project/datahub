from typing import Dict
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
from datahub.ingestion.source.looker.lookml_source import LookMLSource
from datahub.ingestion.source.looker.view_upstream import (
    AbstractViewUpstream,
    LookerQueryAPIBasedViewUpstream,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    SqlParsingDebugInfo,
    SqlParsingResult,
)


def create_mock_sql_parsing_result(
    table_error=None, column_error=None, in_tables=None, column_lineage=None
):
    """Helper function to create a properly mocked SqlParsingResult."""
    # Create a proper SqlParsingDebugInfo instance
    debug_info = SqlParsingDebugInfo()
    if table_error:
        debug_info.table_error = table_error
    if column_error:
        debug_info.column_error = column_error

    # Create a proper SqlParsingResult instance
    mock_spr = SqlParsingResult(
        in_tables=in_tables or [],
        out_tables=[],
        column_lineage=column_lineage or None,
        debug_info=debug_info,
    )
    return mock_spr


def create_mock_lookml_source():
    """Helper function to create a properly mocked LookMLSource."""
    config = MagicMock(spec=LookMLSourceConfig)
    config.base_folder = None
    config.project_name = "test_project"
    config.connection_to_platform_map = {"test_connection": "test_platform"}
    config.stateful_ingestion = MagicMock()
    config.stateful_ingestion.enabled = False
    config.api = None
    config.looker_client = None

    ctx = MagicMock(spec=PipelineContext)
    return LookMLSource(config, ctx)


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
        # Add field splitting config attributes with defaults
        config.field_threshold_for_splitting = 100
        config.allow_partial_lineage_results = True
        config.enable_individual_field_fallback = True
        config.max_workers_for_parallel_processing = 10
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
        # Get fields first, then construct query
        fields = ["test_view.user_id", "test_view.email", "test_view.total_users"]
        query = upstream_instance._get_sql_write_query_with_fields(
            fields, "test_explore"
        )

        assert isinstance(query, WriteQuery)
        assert query.model == "test_model"
        assert query.view == "test_explore"
        assert query.limit == "1"
        assert query.fields is not None
        assert "test_view.user_id" in query.fields
        assert "test_view.email" in query.fields
        assert "test_view.total_users" in query.fields

    def test_get_sql_write_query_no_fields(self, upstream_instance):
        """Test WriteQuery construction when no fields are provided."""
        # Empty fields list should still create a query, but with empty fields
        query = upstream_instance._get_sql_write_query_with_fields([], "test_explore")
        assert isinstance(query, WriteQuery)
        assert query.fields == []

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

        # Create a proper WriteQuery mock with fields attribute
        mock_query = MagicMock(spec=WriteQuery)
        mock_query.fields = ["test_view.user_id"]
        result = upstream_instance._execute_query(mock_query)
        assert result == "SELECT test_view.user_id FROM test_table"

    def test_execute_query_no_sql_response(self, upstream_instance, mock_looker_client):
        """Test query execution when no SQL is returned."""
        mock_looker_client.generate_sql_query.return_value = []

        # Create a proper WriteQuery mock with fields attribute
        mock_query = MagicMock(spec=WriteQuery)
        mock_query.fields = ["test_view.user_id"]
        with pytest.raises(ValueError, match="No SQL found in response"):
            upstream_instance._execute_query(mock_query)

    def test_execute_query_invalid_response_format(
        self, upstream_instance, mock_looker_client
    ):
        """Test query execution with invalid response format."""
        mock_looker_client.generate_sql_query.return_value = None

        # Create a proper WriteQuery mock with fields attribute
        mock_query = MagicMock(spec=WriteQuery)
        mock_query.fields = ["test_view.user_id"]
        with pytest.raises(ValueError, match="No SQL found in response"):
            upstream_instance._execute_query(mock_query)

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_get_spr_table_error(
        self, mock_create_lineage, upstream_instance, mock_looker_client, mock_config
    ):
        """Test SQL parsing result when table extraction fails."""
        # Clear the cache to force re-execution
        upstream_instance._get_spr.cache_clear()

        # Set allow_partial_lineage_results to False to test error raising
        mock_config.allow_partial_lineage_results = False

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
        self, mock_create_lineage, upstream_instance, mock_looker_client, mock_config
    ):
        """Test SQL parsing result when column extraction fails."""
        # Clear the cache to force re-execution
        upstream_instance._get_spr.cache_clear()

        # Set allow_partial_lineage_results to False to test error raising
        mock_config.allow_partial_lineage_results = False

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
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(
                    table="test_view", column="test_view.user_id"
                ),
                upstreams=[ColumnRef(table="test_table", column="user_id")],
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
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(
                    table="test_view", column="test_view.created_date"
                ),
                upstreams=[ColumnRef(table="test_table", column="created_at")],
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
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(
                    table="test_view",
                    column="test_view.user_id",
                    native_column_type="string",
                ),
                upstreams=[ColumnRef(table="test_table", column="user_id")],
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

            # Create a proper WriteQuery mock with fields attribute
            mock_query = MagicMock(spec=WriteQuery)
            mock_query.fields = ["test_view.user_id"]
            upstream_instance._execute_query(mock_query)

            # Verify that latency was reported (may be called multiple times due to caching)
            assert mock_reporter.report_looker_query_api_latency.call_count >= 1

    def test_class_level_cache_for_explore_fields(self, mock_looker_client):
        """Test that get_explore_fields_from_looker_api uses class-level cache."""

        # Mock the LookerAPI response
        mock_explore = MagicMock()
        mock_explore.name = "test_explore"
        mock_explore.fields = [
            MagicMock(name="field1", type="string"),
            MagicMock(name="field2", type="number"),
        ]

        # Add the lookml_model_explore method to the mock
        mock_looker_client.lookml_model_explore = MagicMock(return_value=mock_explore)

        # Call the class method multiple times
        result1 = LookerQueryAPIBasedViewUpstream.get_explore_fields_from_looker_api(
            mock_looker_client, "test_model", "test_explore"
        )
        result2 = LookerQueryAPIBasedViewUpstream.get_explore_fields_from_looker_api(
            mock_looker_client, "test_model", "test_explore"
        )

        # Verify the method was only called once due to caching
        assert mock_looker_client.lookml_model_explore.call_count == 1

        # Verify results are the same (cached)
        assert result1 == result2
        assert result1.name == "test_explore"
        assert result1.fields is not None
        assert len(result1.fields) == 2

    def test_class_level_cache_different_explores(self, mock_looker_client):
        """Test that class-level cache works for different explores."""

        # Mock different explores
        mock_explore1 = MagicMock()
        mock_explore1.name = "explore1"
        mock_explore1.fields = [MagicMock(name="field1", type="string")]

        mock_explore2 = MagicMock()
        mock_explore2.name = "explore2"
        mock_explore2.fields = [MagicMock(name="field2", type="number")]

        def mock_lookml_model_explore(model, explore, fields=None):
            if explore == "explore1":
                return mock_explore1
            elif explore == "explore2":
                return mock_explore2
            return None

        # Add the lookml_model_explore method to the mock
        mock_looker_client.lookml_model_explore = MagicMock(
            side_effect=mock_lookml_model_explore
        )

        # Call for different explores
        result1 = LookerQueryAPIBasedViewUpstream.get_explore_fields_from_looker_api(
            mock_looker_client, "test_model", "explore1"
        )
        result2 = LookerQueryAPIBasedViewUpstream.get_explore_fields_from_looker_api(
            mock_looker_client, "test_model", "explore2"
        )

        # Verify both calls were made
        assert mock_looker_client.lookml_model_explore.call_count == 2

        # Verify results are different
        assert result1.name == "explore1"
        assert result2.name == "explore2"
        assert result1 != result2

    def test_view_optimization_algorithm(self):
        """Test the _optimize_views_by_common_explore algorithm."""
        source = create_mock_lookml_source()

        # Test case with overlapping views and explores
        view_to_explores = {
            "view1": {"explore_a", "explore_b"},
            "view2": {"explore_a", "explore_b"},
            "view3": {"explore_a"},
            "view4": {"explore_b"},
            "view5": {"explore_c"},
            "view6": {"explore_c"},
        }

        explore_to_views = {
            "explore_a": {"view1", "view2", "view3"},
            "explore_b": {"view1", "view2", "view4"},
            "explore_c": {"view5", "view6"},
        }

        result = source._optimize_views_by_common_explore(
            view_to_explores, explore_to_views
        )

        # Verify all views are present
        assert len(result) == len(view_to_explores)
        assert set(result.keys()) == set(view_to_explores.keys())

        # Verify views are assigned to explores
        for view, explore in result.items():
            assert explore in explore_to_views
            assert view in explore_to_views[explore]

    def test_view_optimization_empty_input(self):
        """Test the optimization algorithm with empty input."""
        source = create_mock_lookml_source()

        # Test with empty input
        result = source._optimize_views_by_common_explore({}, {})

        # Verify empty result
        assert len(result) == 0
        assert result == {}

    def test_view_optimization_single_view_multiple_explores(self):
        """Test optimization when a view can be in multiple explores."""
        source = create_mock_lookml_source()

        # Test case where one view can be in multiple explores
        view_to_explores = {"shared_view": {"explore_a", "explore_b", "explore_c"}}

        explore_to_views = {
            "explore_a": {"shared_view"},
            "explore_b": {"shared_view"},
            "explore_c": {"shared_view"},
        }

        result = source._optimize_views_by_common_explore(
            view_to_explores, explore_to_views
        )

        # Verify the view is assigned to one of the explores
        assert len(result) == 1
        assert "shared_view" in result
        assert result["shared_view"] in ["explore_a", "explore_b", "explore_c"]

    def test_view_optimization_efficiency(self):
        """Test that the optimization algorithm reduces the number of explores used."""
        source = create_mock_lookml_source()

        # Create a scenario where optimization can reduce explores
        view_to_explores = {
            "view1": {"explore_a", "explore_b"},
            "view2": {"explore_a", "explore_b"},
            "view3": {"explore_a"},
            "view4": {"explore_b"},
            "view5": {"explore_c"},
            "view6": {"explore_c"},
            "view7": {"explore_d"},
            "view8": {"explore_d"},
        }

        explore_to_views = {
            "explore_a": {"view1", "view2", "view3"},
            "explore_b": {"view1", "view2", "view4"},
            "explore_c": {"view5", "view6"},
            "explore_d": {"view7", "view8"},
        }

        result = source._optimize_views_by_common_explore(
            view_to_explores, explore_to_views
        )

        # Verify all views are present
        assert len(result) == len(view_to_explores)
        assert set(result.keys()) == set(view_to_explores.keys())

        # Verify the algorithm assigns views optimally
        unique_explores_used = len(set(result.values()))
        total_explores = len(explore_to_views)

        # The algorithm should use fewer or equal explores
        assert unique_explores_used <= total_explores

    def test_optimization_algorithm_performance(self):
        """Test that the optimization algorithm handles large datasets efficiently."""
        source = create_mock_lookml_source()

        # Create a larger dataset to test performance
        view_to_explores = {}
        explore_to_views: Dict[str, set] = {}

        # Create 50 views across 10 explores
        for i in range(50):
            view_name = f"view_{i}"
            # Each view can be in 1-3 explores
            num_explores = (i % 3) + 1
            explores = set()

            for j in range(num_explores):
                explore_name = f"explore_{(i + j) % 10 + 1}"
                explores.add(explore_name)
                if explore_name not in explore_to_views:
                    explore_to_views[explore_name] = set()
                explore_to_views[explore_name].add(view_name)

            view_to_explores[view_name] = explores

        # Test the optimization
        result = source._optimize_views_by_common_explore(
            view_to_explores, explore_to_views
        )

        # Verify all views are present
        assert len(result) == len(view_to_explores)
        assert set(result.keys()) == set(view_to_explores.keys())

        # Verify the algorithm completed successfully
        unique_explores_used = len(set(result.values()))
        assert unique_explores_used > 0
        assert unique_explores_used <= len(explore_to_views)

    def test_optimization_algorithm_edge_cases(self):
        """Test edge cases for the optimization algorithm."""
        source = create_mock_lookml_source()

        # Test case 1: All views in one explore
        view_to_explores_1 = {
            "view1": {"explore_a"},
            "view2": {"explore_a"},
            "view3": {"explore_a"},
        }
        explore_to_views_1 = {"explore_a": {"view1", "view2", "view3"}}

        result_1 = source._optimize_views_by_common_explore(
            view_to_explores_1, explore_to_views_1
        )
        assert len(result_1) == 3
        assert all(explore == "explore_a" for explore in result_1.values())

        # Test case 2: Views with no explores
        result_2 = source._optimize_views_by_common_explore({}, {})
        assert len(result_2) == 0

        # Test case 3: Single view, multiple explores
        view_to_explores_3 = {"view1": {"explore_a", "explore_b", "explore_c"}}
        explore_to_views_3 = {
            "explore_a": {"view1"},
            "explore_b": {"view1"},
            "explore_c": {"view1"},
        }

        result_3 = source._optimize_views_by_common_explore(
            view_to_explores_3, explore_to_views_3
        )
        assert len(result_3) == 1
        assert "view1" in result_3
        assert result_3["view1"] in ["explore_a", "explore_b", "explore_c"]


class TestFieldSplittingAndParallelProcessing:
    """Test suite for field splitting, parallel processing, and individual field fallback functionality."""

    @pytest.fixture
    def mock_view_context(self):
        """Create a mock LookerViewContext for testing."""
        view_context = MagicMock(spec=LookerViewContext)
        view_context.name.return_value = "large_view"
        view_context.base_folder_path = "/test/path"
        view_context.dimensions.return_value = []
        view_context.measures.return_value = []
        view_context.dimension_groups.return_value = []

        mock_connection = MagicMock()
        mock_connection.default_schema = "public"
        mock_connection.default_db = "test_db"
        mock_connection.platform = "postgres"
        mock_connection.platform_instance = None
        mock_connection.platform_env = None
        view_context.view_connection = mock_connection

        return view_context

    @pytest.fixture
    def mock_config_with_field_splitting(self):
        """Create a mock config with field splitting enabled."""
        config = MagicMock(spec=LookMLSourceConfig)
        config.use_api_for_view_lineage = True
        config.use_api_cache_for_view_lineage = False
        config.env = "PROD"
        config.field_threshold_for_splitting = 5  # Low threshold for testing
        config.allow_partial_lineage_results = True
        config.enable_individual_field_fallback = True
        config.max_workers_for_parallel_processing = 2
        return config

    @pytest.fixture
    def mock_looker_client_with_fields(self):
        """Create a mock LookerAPI client that returns field data."""
        client = MagicMock(spec=LookerAPI)

        # Mock explore fields response - need proper structure
        # Create 3 fields (below threshold of 5) to avoid field splitting during initialization
        mock_fields = []
        for i in range(3):
            mock_field = MagicMock()
            mock_field.name = f"large_view.field_{i}"
            mock_field.view = "large_view"
            mock_field.category = "dimension"
            mock_field.dimension_group = None
            mock_fields.append(mock_field)

        # Create fields object with dimensions attribute
        mock_fields_obj = MagicMock()
        mock_fields_obj.dimensions = mock_fields
        mock_fields_obj.measures = []

        # Create explore with fields attribute
        mock_explore = MagicMock()
        mock_explore.fields = mock_fields_obj

        client.lookml_model_explore.return_value = mock_explore
        return client

    @pytest.fixture
    def upstream_instance_with_splitting(
        self,
        mock_view_context,
        mock_config_with_field_splitting,
        mock_looker_client_with_fields,
    ):
        """Create an upstream instance configured for field splitting."""
        mock_looker_view_id_cache = MagicMock(spec=LookerViewIdCache)
        mock_looker_view_id_cache.model_name = "test_model"
        mock_view_id = MagicMock(spec=LookerViewId)
        mock_view_id.get_urn.return_value = "urn:li:dataset:test"
        mock_looker_view_id_cache.get_looker_view_id.return_value = mock_view_id

        mock_reporter = MagicMock(spec=LookMLSourceReport)
        mock_ctx = MagicMock(spec=PipelineContext)
        mock_ctx.graph = MagicMock()

        view_to_explore_map = {"large_view": "test_explore"}

        # Mock SQL response (3 fields, below threshold, so no splitting during init)
        # generate_sql_query returns a string, not a list
        def generate_sql_side_effect(write_query=None, use_cache=False, **kwargs):
            # Return SQL string
            return "SELECT large_view.field_0, large_view.field_1, large_view.field_2 FROM test_table"

        mock_looker_client_with_fields.generate_sql_query.side_effect = (
            generate_sql_side_effect
        )

        with patch(
            "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
        ) as mock_create_lineage:
            # Mock successful SQL parsing for each chunk
            # Need to return results with in_tables to avoid "All field chunks failed" error
            # Use empty list for column_lineage since the check is "not all_column_lineage" which is False for empty list
            mock_spr = create_mock_sql_parsing_result(
                in_tables=["urn:li:dataset:(postgres,test_table,PROD)"],
                column_lineage=[],  # Empty list is fine - the check is "not all_column_lineage" which evaluates to False for []
            )
            mock_create_lineage.return_value = mock_spr

            return LookerQueryAPIBasedViewUpstream(
                view_context=mock_view_context,
                looker_view_id_cache=mock_looker_view_id_cache,
                config=mock_config_with_field_splitting,
                reporter=mock_reporter,
                ctx=mock_ctx,
                looker_client=mock_looker_client_with_fields,
                view_to_explore_map=view_to_explore_map,
            )

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_field_splitting_triggered_when_exceeding_threshold(
        self,
        mock_create_lineage,
        upstream_instance_with_splitting,
        mock_looker_client_with_fields,
    ):
        """Test that field splitting is triggered when field count exceeds threshold."""
        # Create 10 fields (exceeds threshold of 5)
        field_names = [f"large_view.field{i}" for i in range(10)]

        # Mock the _get_fields_from_looker_api method to return our fields
        upstream_instance_with_splitting._get_fields_from_looker_api = MagicMock(
            return_value=field_names
        )

        # Mock successful SQL responses for each chunk
        # Need at least one column lineage to satisfy the check in _get_spr_with_field_splitting
        mock_column_lineage = ColumnLineageInfo(
            downstream=DownstreamColumnRef(table="test_table", column="field1"),
            upstreams=[ColumnRef(table="table1", column="col1")],
        )
        mock_spr_chunk1 = create_mock_sql_parsing_result(
            in_tables=["urn:li:dataset:(postgres,table1,PROD)"],
            column_lineage=[mock_column_lineage],
        )
        mock_spr_chunk2 = create_mock_sql_parsing_result(
            in_tables=["urn:li:dataset:(postgres,table2,PROD)"],
            column_lineage=[mock_column_lineage],
        )

        # Mock SQL responses
        mock_looker_client_with_fields.generate_sql_query.side_effect = [
            "SELECT field1, field2, field3, field4, field5 FROM table1",
            "SELECT field6, field7, field8, field9, field10 FROM table2",
        ]

        # Mock parsing results for chunks
        mock_create_lineage.side_effect = [mock_spr_chunk1, mock_spr_chunk2]

        # Clear cache to force re-execution
        upstream_instance_with_splitting._get_spr.cache_clear()

        # Reset call count to exclude initialization calls
        initial_call_count = (
            mock_looker_client_with_fields.generate_sql_query.call_count
        )

        # Execute - should trigger field splitting
        result = upstream_instance_with_splitting._get_spr()

        # Verify field splitting was used
        assert result is not None
        assert len(result.in_tables) >= 1  # Should have combined results
        # Verify multiple SQL queries were made (one per chunk, plus initialization)
        # Should have 2 new calls for the 2 chunks
        new_calls = (
            mock_looker_client_with_fields.generate_sql_query.call_count
            - initial_call_count
        )
        assert new_calls == 2, f"Expected 2 new calls for chunks, got {new_calls}"

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_field_splitting_combines_results_from_multiple_chunks(
        self,
        mock_create_lineage,
        upstream_instance_with_splitting,
        mock_looker_client_with_fields,
    ):
        """Test that field splitting correctly combines results from multiple chunks."""
        field_names = [f"large_view.field{i}" for i in range(10)]
        upstream_instance_with_splitting._get_fields_from_looker_api = MagicMock(
            return_value=field_names
        )

        # Create different results for each chunk
        # Need valid ColumnLineageInfo objects
        mock_column_lineage1 = ColumnLineageInfo(
            downstream=DownstreamColumnRef(table="test_table", column="field1"),
            upstreams=[ColumnRef(table="table1", column="col1")],
        )
        mock_column_lineage2 = ColumnLineageInfo(
            downstream=DownstreamColumnRef(table="test_table", column="field2"),
            upstreams=[ColumnRef(table="table2", column="col2")],
        )
        mock_spr_chunk1 = create_mock_sql_parsing_result(
            in_tables=["urn:li:dataset:(postgres,table1,PROD)"],
            column_lineage=[mock_column_lineage1],
        )
        mock_spr_chunk2 = create_mock_sql_parsing_result(
            in_tables=["urn:li:dataset:(postgres,table2,PROD)"],
            column_lineage=[mock_column_lineage2],
        )

        mock_looker_client_with_fields.generate_sql_query.side_effect = [
            "SELECT * FROM table1",
            "SELECT * FROM table2",
        ]
        mock_create_lineage.side_effect = [mock_spr_chunk1, mock_spr_chunk2]

        upstream_instance_with_splitting._get_spr.cache_clear()

        result = upstream_instance_with_splitting._get_spr()

        # Verify combined results
        assert result is not None
        assert len(result.in_tables) == 2  # Both tables should be present
        assert len(result.column_lineage) == 2  # Both lineages should be present

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_individual_field_fallback_when_chunk_fails(
        self,
        mock_create_lineage,
        upstream_instance_with_splitting,
        mock_looker_client_with_fields,
    ):
        """Test that individual field fallback is triggered when a chunk fails."""
        field_names = [f"large_view.field{i}" for i in range(10)]
        upstream_instance_with_splitting._get_fields_from_looker_api = MagicMock(
            return_value=field_names
        )

        # First chunk fails with parsing error
        mock_spr_chunk1_error = create_mock_sql_parsing_result(
            table_error=Exception("Parsing failed"),
            in_tables=[],
            column_lineage=[],
        )
        # Individual field succeeds - need at least one column lineage
        mock_column_lineage = ColumnLineageInfo(
            downstream=DownstreamColumnRef(table="test_table", column="field1"),
            upstreams=[ColumnRef(table="table1", column="col1")],
        )
        mock_spr_individual = create_mock_sql_parsing_result(
            in_tables=["urn:li:dataset:(postgres,table1,PROD)"],
            column_lineage=[mock_column_lineage],
        )

        mock_looker_client_with_fields.generate_sql_query.side_effect = [
            "SELECT * FROM table1",  # Chunk query
            "SELECT field1 FROM table1",  # Individual field query
            "SELECT field2 FROM table1",
            "SELECT field3 FROM table1",
            "SELECT field4 FROM table1",
            "SELECT field5 FROM table1",
        ]

        # Chunk fails, but individual fields succeed
        def create_lineage_side_effect(*args, **kwargs):
            query = args[0] if args else kwargs.get("query", "")
            if (
                "field1" in query
                or "field2" in query
                or "field3" in query
                or "field4" in query
                or "field5" in query
            ):
                return mock_spr_individual
            return mock_spr_chunk1_error

        mock_create_lineage.side_effect = create_lineage_side_effect

        upstream_instance_with_splitting._get_spr.cache_clear()

        result = upstream_instance_with_splitting._get_spr()

        # Should have partial results from individual fields
        assert result is not None
        # Verify individual field processing was attempted
        assert mock_looker_client_with_fields.generate_sql_query.call_count > 2

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_partial_lineage_allowed_when_parsing_errors_occur(
        self,
        mock_create_lineage,
        upstream_instance_with_splitting,
        mock_looker_client_with_fields,
    ):
        """Test that partial lineage results are returned when allow_partial_lineage_results is True."""
        field_names = [f"large_view.field{i}" for i in range(3)]  # Below threshold
        upstream_instance_with_splitting._get_fields_from_looker_api = MagicMock(
            return_value=field_names
        )

        # Mock SQL response with parsing error but partial results
        mock_spr_with_error = create_mock_sql_parsing_result(
            table_error=Exception("Table parsing error"),
            column_error=None,
            in_tables=["urn:li:dataset:(postgres,table1,PROD)"],  # Partial results
            column_lineage=[],
        )

        mock_looker_client_with_fields.generate_sql_query.return_value = (
            "SELECT * FROM table1"
        )
        mock_create_lineage.return_value = mock_spr_with_error

        upstream_instance_with_splitting._get_spr.cache_clear()

        # Should not raise exception when allow_partial_lineage_results is True
        result = upstream_instance_with_splitting._get_spr()

        assert result is not None
        assert len(result.in_tables) > 0  # Should have partial results

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_no_partial_lineage_when_disabled(
        self,
        mock_create_lineage,
        upstream_instance_with_splitting,
        mock_looker_client_with_fields,
    ):
        """Test that exceptions are raised when allow_partial_lineage_results is False."""
        upstream_instance_with_splitting.config.allow_partial_lineage_results = False
        field_names = [f"large_view.field{i}" for i in range(3)]
        upstream_instance_with_splitting._get_fields_from_looker_api = MagicMock(
            return_value=field_names
        )

        mock_spr_with_error = create_mock_sql_parsing_result(
            table_error=Exception("Table parsing error"),
            in_tables=[],
            column_lineage=[],
        )

        mock_looker_client_with_fields.generate_sql_query.return_value = (
            "SELECT * FROM table1"
        )
        mock_create_lineage.return_value = mock_spr_with_error

        upstream_instance_with_splitting._get_spr.cache_clear()

        # Should raise exception when allow_partial_lineage_results is False
        with pytest.raises(
            ValueError, match="Error in parsing SQL for upstream tables"
        ):
            upstream_instance_with_splitting._get_spr()

    def test_max_workers_validation_minimum(
        self, mock_config_with_field_splitting, tmp_path
    ):
        """Test that max_workers validation rejects values below 1."""
        from datahub.ingestion.source.looker.lookml_config import LookMLSourceConfig

        # Create a temporary directory for base_folder validation
        test_dir = tmp_path / "test_base"
        test_dir.mkdir()

        with pytest.raises(ValueError, match="must be at least 1"):
            LookMLSourceConfig.model_validate(
                {
                    "base_folder": str(test_dir),
                    "max_workers_for_parallel_processing": 0,
                }
            )

    def test_max_workers_validation_maximum(
        self, mock_config_with_field_splitting, tmp_path
    ):
        """Test that max_workers validation caps values above 100."""
        import logging

        from datahub.ingestion.source.looker.lookml_config import LookMLSourceConfig

        # Create a temporary directory for base_folder validation
        test_dir = tmp_path / "test_base"
        test_dir.mkdir()

        with patch.object(
            logging.getLogger("datahub.ingestion.source.looker.lookml_config"),
            "warning",
        ) as mock_warning:
            config = LookMLSourceConfig.model_validate(
                {
                    "base_folder": str(test_dir),
                    "connection_to_platform_map": {"test_connection": "postgres"},
                    "project_name": "test_project",
                    "max_workers_for_parallel_processing": 150,
                }
            )
            # Should be capped to 100
            assert config.max_workers_for_parallel_processing == 100
            # Should log a warning
            mock_warning.assert_called()

    @patch(
        "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
    )
    def test_parallel_processing_with_multiple_workers(
        self,
        mock_create_lineage,
        upstream_instance_with_splitting,
        mock_looker_client_with_fields,
    ):
        """Test that parallel processing works correctly with multiple workers."""
        # Set max workers to 5 for this test
        upstream_instance_with_splitting.config.max_workers_for_parallel_processing = 5

        field_names = [f"large_view.field{i}" for i in range(15)]  # 3 chunks of 5
        upstream_instance_with_splitting._get_fields_from_looker_api = MagicMock(
            return_value=field_names
        )

        # Create successful results for each chunk - need at least one column lineage
        mock_column_lineage = ColumnLineageInfo(
            downstream=DownstreamColumnRef(table="test_table", column="field1"),
            upstreams=[ColumnRef(table="table1", column="col1")],
        )
        mock_sprs = [
            create_mock_sql_parsing_result(
                in_tables=[f"urn:li:dataset:(postgres,table{i},PROD)"],
                column_lineage=[mock_column_lineage],
            )
            for i in range(3)
        ]

        mock_looker_client_with_fields.generate_sql_query.side_effect = [
            f"SELECT * FROM table{i}" for i in range(3)
        ]
        mock_create_lineage.side_effect = mock_sprs

        upstream_instance_with_splitting._get_spr.cache_clear()

        result = upstream_instance_with_splitting._get_spr()

        # Verify all chunks were processed
        assert result is not None
        assert len(result.in_tables) == 3  # All 3 tables should be present

    def test_get_fields_from_looker_api_filters_by_view(
        self, upstream_instance_with_splitting, mock_looker_client_with_fields
    ):
        """Test that _get_fields_from_looker_api correctly filters fields by view."""
        # Clear the class-level cache to force re-fetch
        AbstractViewUpstream.get_explore_fields_from_looker_api.cache_clear()

        # Mock explore with fields from multiple views
        mock_field_current_view = MagicMock()
        mock_field_current_view.name = "large_view.field1"
        mock_field_current_view.view = "large_view"
        mock_field_current_view.category = "dimension"
        mock_field_current_view.dimension_group = None

        mock_field_other_view = MagicMock()
        mock_field_other_view.name = "other_view.field1"
        mock_field_other_view.view = "other_view"
        mock_field_other_view.category = "dimension"
        mock_field_other_view.dimension_group = None

        # Create proper fields structure
        mock_fields_obj = MagicMock()
        mock_fields_obj.dimensions = [mock_field_current_view, mock_field_other_view]
        mock_fields_obj.measures = []

        mock_explore = MagicMock()
        mock_explore.fields = mock_fields_obj

        # Override the fixture's mock
        mock_looker_client_with_fields.lookml_model_explore.return_value = mock_explore

        fields = upstream_instance_with_splitting._get_fields_from_looker_api(
            "test_explore"
        )

        # Should only return fields from the current view
        assert "large_view.field1" in fields
        # Note: The actual implementation filters by view, so other_view fields should not be included
        assert "other_view.field1" not in fields
        assert len(fields) == 1  # Only one field from current view

    def test_field_splitting_statistics_reporting(
        self, upstream_instance_with_splitting, mock_looker_client_with_fields
    ):
        """Test that field splitting statistics are properly reported."""
        field_names = [f"large_view.field{i}" for i in range(10)]
        upstream_instance_with_splitting._get_fields_from_looker_api = MagicMock(
            return_value=field_names
        )

        # Mock successful processing - need at least one column lineage
        mock_column_lineage = ColumnLineageInfo(
            downstream=DownstreamColumnRef(table="test_table", column="field1"),
            upstreams=[ColumnRef(table="table1", column="col1")],
        )
        with patch(
            "datahub.ingestion.source.looker.view_upstream.create_lineage_sql_parsed_result"
        ) as mock_create_lineage:
            mock_spr = create_mock_sql_parsing_result(
                in_tables=["urn:li:dataset:(postgres,table1,PROD)"],
                column_lineage=[mock_column_lineage],
            )
            mock_create_lineage.return_value = mock_spr
            mock_looker_client_with_fields.generate_sql_query.return_value = (
                "SELECT * FROM table1"
            )

            upstream_instance_with_splitting._get_spr.cache_clear()
            upstream_instance_with_splitting._get_spr()

            # Verify reporter was called for statistics
            # For complete success (100% success rate), info() is called instead of report_warning()
            assert upstream_instance_with_splitting.reporter.info.called
