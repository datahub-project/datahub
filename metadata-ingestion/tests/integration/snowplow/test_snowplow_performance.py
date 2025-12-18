"""
Performance integration tests for Snowplow source.

Tests verify that performance optimizations work correctly:
- Parallel deployment fetching
- Caching reduces redundant API calls
- Large dataset handling
"""

import time
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.snowplow.snowplow import SnowplowSource
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig


def create_mock_context():
    """Create a mock context for SnowplowSource initialization."""
    mock_ctx = MagicMock()
    mock_ctx.graph = None
    mock_ctx.pipeline_name = None
    return mock_ctx


def generate_mock_data_structures(count: int) -> List[Dict[str, Any]]:
    """Generate mock data structures for performance testing."""
    structures = []
    for i in range(count):
        vendor = f"com.example.vendor{i % 10}"
        name = f"test_schema_{i}"
        version = "1-0-0"

        structures.append(
            {
                "organizationId": "test-org",
                "vendor": vendor,
                "name": name,
                "format": "jsonschema",
                "meta": {
                    "schemaType": "event" if i % 2 == 0 else "entity",
                    "isHidden": False,
                },
                "data": {
                    "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
                    "self": {
                        "vendor": vendor,
                        "name": name,
                        "format": "jsonschema",
                        "version": version,
                    },
                    "type": "object",
                    "properties": {
                        "field1": {"type": "string"},
                        "field2": {"type": "integer"},
                    },
                },
                "hash": f"hash_{i}",
                "deployments": [
                    {
                        "version": version,
                        "ts": "2024-01-01T00:00:00Z",
                        "initiator": "Test User",
                        "initiatorId": "user123",
                    }
                ],
            }
        )
    return structures


@pytest.mark.integration
def test_parallel_fetching_performance(pytestconfig, tmp_path):
    """
    Test that parallel deployment fetching is significantly faster than sequential.

    This test compares performance with parallel fetching enabled vs disabled.
    """
    # Generate dataset with 100 schemas (enough to see performance difference)
    mock_data_structures = generate_mock_data_structures(100)

    # Simulate API delay (10ms per call to make difference measurable)
    def mock_get_deployments(schema_hash: str):
        """Simulate API delay."""
        time.sleep(0.01)  # 10ms delay
        return [
            {
                "version": "1-0-0",
                "ts": "2024-01-01T00:00:00Z",
                "initiator": "Test User",
                "initiatorId": "user123",
            }
        ]

    # Test 1: Sequential fetching (parallel disabled)
    config_sequential = {
        "bdp_connection": {
            "organization_id": "test-org",
            "api_key_id": "test-key",
            "api_key": "test-secret",
        },
        "field_tagging": {"track_field_versions": True},
        "performance": {
            "enable_parallel_fetching": False,
            "max_concurrent_api_calls": 10,
        },
    }

    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        mock_client = mock_client_class.return_value
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_token"

        from datahub.ingestion.source.snowplow.snowplow_models import DataStructure

        mock_client.get_data_structures.return_value = [
            DataStructure.model_validate(ds) for ds in mock_data_structures
        ]
        mock_client.get_data_structure_deployments.side_effect = mock_get_deployments

        config = SnowplowSourceConfig.model_validate(config_sequential)
        source = SnowplowSource(config, create_mock_context())
        source.bdp_client = mock_client

        start_time = time.time()
        list(source._get_data_structures_filtered())
        sequential_time = time.time() - start_time

    # Test 2: Parallel fetching (parallel enabled)
    config_parallel = {
        "bdp_connection": {
            "organization_id": "test-org",
            "api_key_id": "test-key",
            "api_key": "test-secret",
        },
        "field_tagging": {"track_field_versions": True},
        "performance": {
            "enable_parallel_fetching": True,
            "max_concurrent_api_calls": 10,
        },
    }

    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        mock_client = mock_client_class.return_value
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_token"
        mock_client.get_data_structures.return_value = [
            DataStructure.model_validate(ds) for ds in mock_data_structures
        ]
        mock_client.get_data_structure_deployments.side_effect = mock_get_deployments

        config = SnowplowSourceConfig.model_validate(config_parallel)
        source = SnowplowSource(config, create_mock_context())
        source.bdp_client = mock_client

        start_time = time.time()
        list(source._get_data_structures_filtered())
        parallel_time = time.time() - start_time

    # Performance assertions
    speedup = sequential_time / parallel_time
    print("\nPerformance Results:")
    print(f"  Sequential time: {sequential_time:.2f}s")
    print(f"  Parallel time: {parallel_time:.2f}s")
    print(f"  Speedup: {speedup:.2f}x")

    # Parallel should be at least 3x faster with 10 workers
    # (100 schemas / 10 workers = ~10 sequential batches vs 100 sequential calls)
    assert parallel_time < sequential_time / 3, (
        f"Parallel fetching should be at least 3x faster (got {speedup:.2f}x)"
    )


@pytest.mark.integration
def test_caching_reduces_api_calls(pytestconfig):
    """
    Test that caching prevents redundant API calls within single ingestion run.

    Verifies that:
    1. First call fetches from API
    2. Second call uses cache
    3. Total API calls are reduced
    """
    # Generate small dataset for quick test
    mock_data_structures = generate_mock_data_structures(20)

    config = {
        "bdp_connection": {
            "organization_id": "test-org",
            "api_key_id": "test-key",
            "api_key": "test-secret",
        },
    }

    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        mock_client = mock_client_class.return_value
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_token"

        from datahub.ingestion.source.snowplow.snowplow_models import DataStructure

        mock_client.get_data_structures.return_value = [
            DataStructure.model_validate(ds) for ds in mock_data_structures
        ]

        config_obj = SnowplowSourceConfig.model_validate(config)
        source = SnowplowSource(config_obj, create_mock_context())
        source.bdp_client = mock_client

        # First call - should hit API
        result1 = source._get_data_structures_filtered()
        first_call_count = mock_client.get_data_structures.call_count

        # Second call - should use cache
        result2 = source._get_data_structures_filtered()
        second_call_count = mock_client.get_data_structures.call_count

        # Third call - should still use cache
        result3 = source._get_data_structures_filtered()
        third_call_count = mock_client.get_data_structures.call_count

        # Assertions
        assert len(result1) == 20, "First call should return all structures"
        assert len(result2) == 20, "Cached call should return same results"
        assert len(result3) == 20, "Cached call should return same results"

        # API should only be called once (first time)
        assert first_call_count == 1, "First call should hit API once"
        assert second_call_count == 1, (
            "Second call should use cache (no additional API calls)"
        )
        assert third_call_count == 1, (
            "Third call should use cache (no additional API calls)"
        )

        print("\nCaching Results:")
        print(f"  Total API calls: {third_call_count} (expected: 1)")
        print("  Cache hits: 2")
        print(f"  Data structures fetched: {len(result1)}")


@pytest.mark.integration
def test_event_schema_urn_caching(pytestconfig):
    """
    Test that data structure fetching is cached within a single ingestion run.

    After schemas are processed, the extracted URNs are stored in state
    and subsequent processors can access them without reprocessing.
    """
    mock_data_structures = generate_mock_data_structures(50)

    config = {
        "bdp_connection": {
            "organization_id": "test-org",
            "api_key_id": "test-key",
            "api_key": "test-secret",
        },
    }

    # Mock API call with artificial delay to make timing measurable
    def slow_get_data_structures(*args, **kwargs):
        """Simulate slow API call (1.5 seconds)."""
        time.sleep(1.5)
        from datahub.ingestion.source.snowplow.snowplow_models import DataStructure

        return [DataStructure.model_validate(ds) for ds in mock_data_structures]

    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        mock_client = mock_client_class.return_value
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_token"
        mock_client.get_data_structures.side_effect = slow_get_data_structures

        config_obj = SnowplowSourceConfig.model_validate(config)
        source = SnowplowSource(config_obj, create_mock_context())
        source.bdp_client = mock_client

        # First call - should fetch from API (slow)
        start_time = time.time()
        structures1 = source._get_data_structures_filtered()
        first_call_time = time.time() - start_time

        # Second call - should use cache (fast)
        start_time = time.time()
        structures2 = source._get_data_structures_filtered()
        second_call_time = time.time() - start_time

        # Third call - should still use cache (fast)
        start_time = time.time()
        structures3 = source._get_data_structures_filtered()
        third_call_time = time.time() - start_time

        # Assertions
        assert len(structures1) == 50, "Should return all 50 schemas"
        assert len(structures2) == 50, "Cached results should have same count"
        assert len(structures3) == 50, "Cached results should have same count"

        # Verify only one API call was made (caching worked)
        assert mock_client.get_data_structures.call_count == 1, (
            "API should only be called once, subsequent calls use cache"
        )

        # First call should be slow (>1 second due to API delay)
        assert first_call_time > 1.0, (
            f"First call should hit slow API (got {first_call_time:.2f}s)"
        )

        # Cached calls should be much faster (at least 10x faster)
        assert second_call_time < first_call_time / 10, (
            f"Cached call should be at least 10x faster "
            f"(first: {first_call_time:.2f}s, second: {second_call_time:.4f}s)"
        )
        assert third_call_time < first_call_time / 10, (
            f"Cached call should be at least 10x faster "
            f"(first: {first_call_time:.2f}s, third: {third_call_time:.4f}s)"
        )

        print("\nData Structure Caching Results:")
        print(f"  First call time: {first_call_time:.2f}s")
        print(f"  Second call time: {second_call_time * 1000:.2f}ms")
        print(f"  Third call time: {third_call_time * 1000:.2f}ms")
        print(f"  Speedup: {first_call_time / second_call_time:.0f}x")
        print(f"  API calls: {mock_client.get_data_structures.call_count}")


@pytest.mark.integration
def test_large_dataset_performance(pytestconfig):
    """
    Test performance with large dataset (1000 schemas).

    Verifies that:
    1. Parallel fetching handles large datasets efficiently
    2. Memory usage is reasonable
    3. Caching works at scale
    """
    # Generate large dataset
    mock_data_structures = generate_mock_data_structures(1000)

    config = {
        "bdp_connection": {
            "organization_id": "test-org",
            "api_key_id": "test-key",
            "api_key": "test-secret",
        },
        "field_tagging": {"track_field_versions": True},
        "performance": {
            "enable_parallel_fetching": True,
            "max_concurrent_api_calls": 20,  # Higher concurrency for large dataset
        },
    }

    # Mock API call with artificial delay to make timing measurable
    def slow_get_data_structures(*args, **kwargs):
        """Simulate slow API call (2 seconds for large dataset)."""
        time.sleep(2.0)
        from datahub.ingestion.source.snowplow.snowplow_models import DataStructure

        return [DataStructure.model_validate(ds) for ds in mock_data_structures]

    # Mock deployment fetching with minimal delay
    def mock_get_deployments(schema_hash: str):
        time.sleep(0.001)  # 1ms delay
        return [
            {
                "version": "1-0-0",
                "ts": "2024-01-01T00:00:00Z",
                "initiator": "Test User",
                "initiatorId": "user123",
            }
        ]

    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        mock_client = mock_client_class.return_value
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_token"
        mock_client.get_data_structures.side_effect = slow_get_data_structures
        mock_client.get_data_structure_deployments.side_effect = mock_get_deployments

        config_obj = SnowplowSourceConfig.model_validate(config)
        source = SnowplowSource(config_obj, create_mock_context())
        source.bdp_client = mock_client

        # Measure performance for initial fetch (slow)
        start_time = time.time()
        structures = source._get_data_structures_filtered()
        fetch_time = time.time() - start_time

        # Second fetch should use cache (fast)
        start_time = time.time()
        structures_cached = source._get_data_structures_filtered()
        cache_time = time.time() - start_time

        # Assertions
        assert len(structures) == 1000, "Should fetch all 1000 structures"
        assert len(structures_cached) == 1000, "Cached fetch should return same count"

        # Count event schemas (50% should be events based on mock generation)
        event_count = sum(
            1 for ds in structures if ds.meta and ds.meta.schema_type == "event"
        )
        assert event_count == 500, "Should have 500 event schemas"

        # First call should be slow (>2 seconds due to API delay + processing)
        assert fetch_time > 2.0, (
            f"First call should hit slow API (got {fetch_time:.2f}s)"
        )

        # Cached fetch should be much faster (at least 20x faster)
        assert cache_time < fetch_time / 20, (
            f"Cached fetch should be at least 20x faster "
            f"(first: {fetch_time:.2f}s, cached: {cache_time:.4f}s)"
        )

        # Verify caching worked (only one API call)
        assert mock_client.get_data_structures.call_count == 1, (
            "Should only call API once with caching enabled"
        )

        print("\nLarge Dataset Performance Results:")
        print("  Dataset size: 1000 schemas")
        print(f"  Initial fetch time: {fetch_time:.2f}s")
        print(f"  Cached fetch time: {cache_time * 1000:.2f}ms")
        print(f"  Event schemas: {event_count}")
        print(f"  Throughput: {len(structures) / fetch_time:.0f} schemas/second")
        print(f"  Cache speedup: {fetch_time / cache_time:.0f}x")


@pytest.mark.integration
def test_api_call_count_without_field_tracking(pytestconfig):
    """
    Test that without field version tracking, no deployment fetching occurs.

    This verifies optimization: deployment history is only fetched when needed.
    """
    mock_data_structures = generate_mock_data_structures(100)

    config = {
        "bdp_connection": {
            "organization_id": "test-org",
            "api_key_id": "test-key",
            "api_key": "test-secret",
        },
        "field_tagging": {
            "track_field_versions": False  # Disabled - no deployment fetching
        },
    }

    with patch(
        "datahub.ingestion.source.snowplow.snowplow.SnowplowBDPClient"
    ) as mock_client_class:
        mock_client = mock_client_class.return_value
        mock_client._authenticate = lambda: None
        mock_client._jwt_token = "mock_token"

        from datahub.ingestion.source.snowplow.snowplow_models import DataStructure

        mock_client.get_data_structures.return_value = [
            DataStructure.model_validate(ds) for ds in mock_data_structures
        ]

        config_obj = SnowplowSourceConfig.model_validate(config)
        source = SnowplowSource(config_obj, create_mock_context())
        source.bdp_client = mock_client

        # Fetch structures
        structures = source._get_data_structures_filtered()

        # Assertions
        assert len(structures) == 100, "Should fetch all structures"

        # get_data_structure_deployments should NEVER be called
        assert mock_client.get_data_structure_deployments.call_count == 0, (
            "Deployments should not be fetched when field tracking disabled"
        )

        # Only get_data_structures should be called (once, then cached)
        assert mock_client.get_data_structures.call_count == 1, (
            "Data structures should be fetched once"
        )

        print("\nAPI Call Count Results (field tracking disabled):")
        print("  get_data_structures calls: 1")
        print("  get_data_structure_deployments calls: 0")
        print("  Total API calls: 1")
