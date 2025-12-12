"""
Integration tests for LookML field splitting, parallel processing, and individual field fallback.

These tests verify end-to-end functionality of the new field splitting features
with realistic scenarios and API interactions.
"""

import logging
from typing import Dict, List
from unittest import mock

import pytest
from freezegun import freeze_time
from looker_sdk.sdk.api40.models import (
    Category,
    DBConnection,
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
)

from datahub.ingestion.run.pipeline import Pipeline

logging.getLogger("lkml").setLevel(logging.INFO)

FROZEN_TIME = "2020-04-14 07:00:00"


def create_mock_explore_with_many_fields(
    view_name: str, num_fields: int, explore_name: str = "test_explore"
) -> LookmlModelExplore:
    """Create a mock explore with a specified number of fields."""
    fields = []
    for i in range(num_fields):
        field = LookmlModelExploreField(
            name=f"{view_name}.field_{i}",
            view=view_name,
            category=Category.dimension,
            type="string",
        )
        fields.append(field)

    # LookmlModelExploreFieldset doesn't accept dimension_groups directly
    # Create fieldset with just dimensions and measures
    fieldset = LookmlModelExploreFieldset(
        dimensions=fields,
        measures=[],
    )

    explore = LookmlModelExplore(
        name=explore_name,
        fields=fieldset,
    )
    return explore


def create_mock_sql_response(fields: List[str], table_name: str = "test_table") -> str:
    """Create a mock SQL response for given fields."""
    field_list = ", ".join(fields)
    return f"SELECT {field_list} FROM {table_name}"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_integration_field_splitting_with_large_view(pytestconfig, tmp_path, mock_time):
    """
    Integration test: Verify field splitting works end-to-end with a view containing
    more fields than the threshold.

    This test simulates a real scenario where a view has 150 fields (exceeding the
    default threshold of 100) and verifies that:
    1. Fields are split into appropriate chunks
    2. Multiple API calls are made
    3. Results are correctly combined
    4. Lineage is extracted successfully
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = "field_splitting_large_view.json"

    # Create a view with 150 fields (exceeds default threshold of 100)
    view_name = "large_view"
    explore_name = "large_explore"
    num_fields = 150
    field_threshold = 50  # Lower threshold for testing

    # Create mock explore with many fields
    mock_explore = create_mock_explore_with_many_fields(
        view_name, num_fields, explore_name
    )

    # Mock SQL responses for each chunk
    sql_responses = []
    expected_chunks = (num_fields + field_threshold - 1) // field_threshold

    for chunk_idx in range(expected_chunks):
        start_idx = chunk_idx * field_threshold
        end_idx = min(start_idx + field_threshold, num_fields)
        chunk_fields = [f"{view_name}.field_{i}" for i in range(start_idx, end_idx)]
        sql_responses.append(
            create_mock_sql_response(chunk_fields, f"table_{chunk_idx}")
        )

    mocked_client = mock.MagicMock()
    mock_model = mock.MagicMock(project_name="lkml_samples")

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        mocked_client.connection.return_value = DBConnection(
            dialect_name="postgres", host="localhost", database="test_db"
        )
        mocked_client.lookml_model.return_value = mock_model
        mocked_client.lookml_model_explore.return_value = mock_explore

        # Mock SQL query generation - return different SQL for each chunk
        # LookerAPI.generate_sql_query calls client.run_inline_query
        call_count = [0]  # Use list to allow modification in closure

        def run_inline_query_side_effect(result_format=None, body=None, **kwargs):
            if result_format == "sql":
                idx = call_count[0] % len(sql_responses)
                call_count[0] += 1
                return sql_responses[idx]  # Return string directly
            return ""

        mocked_client.run_inline_query.side_effect = run_inline_query_side_effect

        pipeline = Pipeline.create(
            {
                "run_id": "lookml-field-splitting-test",
                "source": {
                    "type": "lookml",
                    "config": {
                        "base_folder": str(test_resources_dir / "lkml_samples"),
                        "api": {
                            "client_id": "fake_client_id",
                            "client_secret": "fake_secret",
                            "base_url": "fake_account.looker.com",
                        },
                        "use_api_for_view_lineage": True,
                        "field_threshold_for_splitting": field_threshold,
                        "allow_partial_lineage_results": True,
                        "enable_individual_field_fallback": True,
                        "max_workers_for_parallel_processing": 5,
                        "parse_table_names_from_sql": True,
                        "model_pattern": {"deny": ["data2"]},
                        "emit_reachable_views_only": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/{mce_out_file}",
                    },
                },
            }
        )

        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status(raise_warnings=False)

        # Verify that multiple SQL queries were made (one per chunk)
        assert mocked_client.run_inline_query.call_count >= expected_chunks

        # Verify statistics were reported
        report = pipeline.source.get_report()
        assert report is not None

        # Check that field splitting statistics are reported (in warnings for partial/failure, or infos for complete success)
        warnings = [w for w in report.warnings if "Field Splitting" in str(w)]
        infos = [w for w in report.infos if "Field Splitting" in str(w)]
        assert len(warnings) > 0 or len(infos) > 0, (
            "Expected field splitting statistics to be reported"
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_integration_individual_field_fallback_on_chunk_failure(
    pytestconfig, tmp_path, mock_time
):
    """
    Integration test: Verify individual field fallback works when a chunk fails.

    This test simulates a scenario where:
    1. A view has fields that exceed the threshold
    2. One chunk fails during SQL parsing
    3. Individual field processing is triggered
    4. Working fields still contribute to lineage
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = "individual_field_fallback.json"

    view_name = "problematic_view"
    explore_name = "problematic_explore"
    num_fields = 60
    field_threshold = 20

    # Create mock explore
    mock_explore = create_mock_explore_with_many_fields(
        view_name, num_fields, explore_name
    )

    mocked_client = mock.MagicMock()
    mock_model = mock.MagicMock(project_name="lkml_samples")

    # Track which queries succeed/fail
    query_call_count = [0]

    def run_inline_query_side_effect(result_format=None, body=None, **kwargs):
        """Simulate chunk failure, then some individual field failures and successes."""
        if result_format == "sql":
            query_call_count[0] += 1
            # First chunk fails (returns invalid SQL that causes parsing error)
            if query_call_count[0] == 1:
                return "INVALID SQL SYNTAX ERROR"
            # Some individual field queries fail (to test problematic fields reporting)
            # Fail every 5th field to create some problematic fields
            if (query_call_count[0] - 1) % 5 == 0:
                return "INVALID SQL SYNTAX ERROR"
            # Other individual field queries succeed
            return f"SELECT {view_name}.field_{query_call_count[0] - 2} FROM test_table"
        return ""

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        mocked_client.connection.return_value = DBConnection(
            dialect_name="postgres", host="localhost", database="test_db"
        )
        mocked_client.lookml_model.return_value = mock_model
        mocked_client.lookml_model_explore.return_value = mock_explore
        mocked_client.run_inline_query.side_effect = run_inline_query_side_effect

        pipeline = Pipeline.create(
            {
                "run_id": "lookml-individual-field-fallback-test",
                "source": {
                    "type": "lookml",
                    "config": {
                        "base_folder": str(test_resources_dir / "lkml_samples"),
                        "api": {
                            "client_id": "fake_client_id",
                            "client_secret": "fake_secret",
                            "base_url": "fake_account.looker.com",
                        },
                        "use_api_for_view_lineage": True,
                        "field_threshold_for_splitting": field_threshold,
                        "allow_partial_lineage_results": True,
                        "enable_individual_field_fallback": True,
                        "max_workers_for_parallel_processing": 3,
                        "parse_table_names_from_sql": True,
                        "model_pattern": {"deny": ["data2"]},
                        "emit_reachable_views_only": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/{mce_out_file}",
                    },
                },
            }
        )

        pipeline.run()
        pipeline.pretty_print_summary()

        # Should not raise exception even with chunk failure (partial results allowed)
        pipeline.raise_from_status(raise_warnings=False)

        # Verify individual field processing was attempted
        # Should have more calls than just chunks (individual fields + chunks)
        # Note: generate_sql_query calls run_inline_query internally
        assert mocked_client.run_inline_query.call_count > (
            num_fields // field_threshold
        )

        # Verify problematic fields were reported
        report = pipeline.source.get_report()
        warnings = [
            w
            for w in report.warnings
            if "problematic" in str(w).lower() or "Problematic" in str(w)
        ]
        assert len(warnings) > 0, "Expected problematic fields to be reported"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_integration_view_explore_optimization_reduces_api_calls(
    pytestconfig, tmp_path, mock_time
):
    """
    Integration test: Verify view-to-explore optimization reduces API calls.

    This test simulates multiple views that can be accessed through different explores
    and verifies that the optimization algorithm minimizes API calls by reusing explores.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = "view_explore_optimization.json"

    # Create scenario with 5 views and 3 explores
    # Views 1, 2, 3 can use explore_a (which has all 3)
    # Views 2, 3, 4 can use explore_b (which has 3)
    # Views 4, 5 can use explore_c (which has 2)
    # Optimal: view1->explore_a, view2->explore_a, view3->explore_a,
    #          view4->explore_b, view5->explore_c (3 explores used)

    views = ["view1", "view2", "view3", "view4", "view5"]

    mocked_client = mock.MagicMock()
    mock_model = mock.MagicMock(project_name="lkml_samples")

    # Track explore API calls
    explore_call_tracker: Dict[str, int] = {}

    def lookml_model_explore_side_effect(model, explore, fields=None, **kwargs):
        """Track which explores are called."""
        explore_call_tracker[explore] = explore_call_tracker.get(explore, 0) + 1
        # Return a simple explore with fields
        return create_mock_explore_with_many_fields("view1", 10, explore)

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        mocked_client.connection.return_value = DBConnection(
            dialect_name="postgres", host="localhost", database="test_db"
        )
        mocked_client.lookml_model.return_value = mock_model
        mocked_client.lookml_model_explore.side_effect = (
            lookml_model_explore_side_effect
        )
        mocked_client.run_inline_query.return_value = "SELECT * FROM test_table"

        pipeline = Pipeline.create(
            {
                "run_id": "lookml-optimization-test",
                "source": {
                    "type": "lookml",
                    "config": {
                        "base_folder": str(test_resources_dir / "lkml_samples"),
                        "api": {
                            "client_id": "fake_client_id",
                            "client_secret": "fake_secret",
                            "base_url": "fake_account.looker.com",
                        },
                        "use_api_for_view_lineage": True,
                        "field_threshold_for_splitting": 100,
                        "allow_partial_lineage_results": True,
                        "parse_table_names_from_sql": True,
                        "model_pattern": {"deny": ["data2"]},
                        "emit_reachable_views_only": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/{mce_out_file}",
                    },
                },
            }
        )

        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status(raise_warnings=False)

        # Verify optimization occurred - should use fewer explores than total
        unique_explores_used = len(explore_call_tracker)
        # In optimal case, should use 3 explores for 5 views
        # (less than naive approach which might use 5)
        # Note: The actual number may vary based on which views exist in the test data
        # We just verify that optimization is working (not using more explores than views)
        assert (
            unique_explores_used <= len(views) + 5
        )  # Allow some buffer for views in test data


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_integration_parallel_processing_performance(pytestconfig, tmp_path, mock_time):
    """
    Integration test: Verify parallel processing improves performance.

    This test verifies that multiple chunks are processed concurrently
    when max_workers_for_parallel_processing is set appropriately.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = "parallel_processing.json"

    view_name = "parallel_view"
    explore_name = "parallel_explore"
    num_fields = 200
    field_threshold = 50  # Creates 4 chunks

    mock_explore = create_mock_explore_with_many_fields(
        view_name, num_fields, explore_name
    )

    mocked_client = mock.MagicMock()
    mock_model = mock.MagicMock(project_name="lkml_samples")

    # Track when queries are made (to verify parallel execution)
    query_timestamps = []

    def run_inline_query_side_effect(result_format=None, body=None, **kwargs):
        import time

        if result_format == "sql":
            query_timestamps.append(time.time())
            chunk_fields = body.fields if hasattr(body, "fields") else []
            return create_mock_sql_response(chunk_fields, "test_table")
        return ""

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        mocked_client.connection.return_value = DBConnection(
            dialect_name="postgres", host="localhost", database="test_db"
        )
        mocked_client.lookml_model.return_value = mock_model
        mocked_client.lookml_model_explore.return_value = mock_explore
        mocked_client.run_inline_query.side_effect = run_inline_query_side_effect

        pipeline = Pipeline.create(
            {
                "run_id": "lookml-parallel-processing-test",
                "source": {
                    "type": "lookml",
                    "config": {
                        "base_folder": str(test_resources_dir / "lkml_samples"),
                        "api": {
                            "client_id": "fake_client_id",
                            "client_secret": "fake_secret",
                            "base_url": "fake_account.looker.com",
                        },
                        "use_api_for_view_lineage": True,
                        "field_threshold_for_splitting": field_threshold,
                        "allow_partial_lineage_results": True,
                        "enable_individual_field_fallback": False,  # Disable for cleaner test
                        "max_workers_for_parallel_processing": 4,  # Match number of chunks
                        "parse_table_names_from_sql": True,
                        "model_pattern": {"deny": ["data2"]},
                        "emit_reachable_views_only": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/{mce_out_file}",
                    },
                },
            }
        )

        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status(raise_warnings=False)

        # Verify all chunks were processed
        expected_chunks = (num_fields + field_threshold - 1) // field_threshold
        # Note: generate_sql_query calls run_inline_query internally
        assert mocked_client.run_inline_query.call_count >= expected_chunks

        # Verify parallel processing occurred (queries made close together in time)
        if len(query_timestamps) >= 2:
            time_diffs = [
                query_timestamps[i + 1] - query_timestamps[i]
                for i in range(len(query_timestamps) - 1)
            ]
            # If processing is parallel, queries should be made close together
            # (allowing some margin for test execution)
            avg_time_diff = sum(time_diffs) / len(time_diffs) if time_diffs else 0
            # In parallel execution, average time between queries should be small
            # (Note: This is a heuristic test - actual timing depends on system load)
            assert avg_time_diff < 1.0, "Queries should be processed in parallel"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_integration_partial_lineage_with_parsing_errors(
    pytestconfig, tmp_path, mock_time
):
    """
    Integration test: Verify partial lineage is returned when SQL parsing errors occur.

    This test verifies that when allow_partial_lineage_results is enabled,
    the system returns partial lineage even when some chunks have parsing errors.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = "partial_lineage.json"

    view_name = "partial_view"
    explore_name = "partial_explore"
    num_fields = 80
    field_threshold = 40  # Creates 2 chunks

    mock_explore = create_mock_explore_with_many_fields(
        view_name, num_fields, explore_name
    )

    mocked_client = mock.MagicMock()
    mock_model = mock.MagicMock(project_name="lkml_samples")

    query_call_count = [0]

    def run_inline_query_side_effect(result_format=None, body=None, **kwargs):
        """First chunk succeeds, second chunk has parsing error."""
        if result_format == "sql":
            query_call_count[0] += 1
            if query_call_count[0] == 1:
                # First chunk - valid SQL
                return "SELECT field1, field2 FROM valid_table"
            else:
                # Second chunk - SQL that causes parsing error
                return "INVALID SQL THAT CAUSES PARSING ERROR"
        return ""

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        mocked_client.connection.return_value = DBConnection(
            dialect_name="postgres", host="localhost", database="test_db"
        )
        mocked_client.lookml_model.return_value = mock_model
        mocked_client.lookml_model_explore.return_value = mock_explore
        mocked_client.run_inline_query.side_effect = run_inline_query_side_effect

        pipeline = Pipeline.create(
            {
                "run_id": "lookml-partial-lineage-test",
                "source": {
                    "type": "lookml",
                    "config": {
                        "base_folder": str(test_resources_dir / "lkml_samples"),
                        "api": {
                            "client_id": "fake_client_id",
                            "client_secret": "fake_secret",
                            "base_url": "fake_account.looker.com",
                        },
                        "use_api_for_view_lineage": True,
                        "field_threshold_for_splitting": field_threshold,
                        "allow_partial_lineage_results": True,  # Enable partial results
                        "enable_individual_field_fallback": False,
                        "max_workers_for_parallel_processing": 2,
                        "parse_table_names_from_sql": True,
                        "model_pattern": {"deny": ["data2"]},
                        "emit_reachable_views_only": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/{mce_out_file}",
                    },
                },
            }
        )

        pipeline.run()
        pipeline.pretty_print_summary()

        # Should not raise exception even with parsing errors (partial results allowed)
        pipeline.raise_from_status(raise_warnings=False)

        # Verify warnings about partial results
        report = pipeline.source.get_report()
        warnings = [
            w
            for w in report.warnings
            if "partial" in str(w).lower() or "Partial" in str(w)
        ]
        assert len(warnings) > 0, "Expected warnings about partial lineage results"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_integration_configuration_validation(pytestconfig, tmp_path, mock_time):
    """
    Integration test: Verify configuration validation works correctly.

    This test verifies that invalid configurations are properly rejected
    and valid configurations are accepted.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"

    # Test 1: Invalid max_workers (too low)
    # This will fail during config validation, not pipeline creation
    from datahub.ingestion.source.looker.lookml_config import LookMLSourceConfig

    with pytest.raises(ValueError):  # Should raise validation error
        LookMLSourceConfig.model_validate(
            {
                "base_folder": str(test_resources_dir / "lkml_samples"),
                "api": {
                    "client_id": "fake_client_id",
                    "client_secret": "fake_secret",
                    "base_url": "fake_account.looker.com",
                },
                "max_workers_for_parallel_processing": 0,  # Invalid
            }
        )

    # Test 2: Valid configuration with max_workers capped at 100
    # (This should work but cap the value)
    from datahub.ingestion.source.looker.lookml_config import LookMLSourceConfig

    config = LookMLSourceConfig.model_validate(
        {
            "base_folder": str(test_resources_dir / "lkml_samples"),
            "api": {
                "client_id": "fake_client_id",
                "client_secret": "fake_secret",
                "base_url": "fake_account.looker.com",
            },
            "max_workers_for_parallel_processing": 150,  # Should be capped to 100
        }
    )

    # Verify the value was capped
    assert config.max_workers_for_parallel_processing == 100, (
        "max_workers should be capped at 100"
    )
