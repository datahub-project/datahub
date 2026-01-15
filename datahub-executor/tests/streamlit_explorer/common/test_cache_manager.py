"""Tests for the cache_manager module."""

import tempfile
from pathlib import Path

import pandas as pd

from scripts.streamlit_explorer.common.cache_manager import (
    ALL_ASPECTS,
    METRIC_CUBE_ASPECTS,
    MONITOR_ASPECTS,
    AnomalyEdit,
    AnomalyEditTracker,
    AspectCacheInfo,
    CacheIndexData,
    EndpointCache,
    EndpointRegistry,
    RunEventCache,
    hostname_to_dir,
    url_to_hostname,
)


class TestHostnameConversions:
    """Tests for hostname/URL conversion utilities."""

    def test_hostname_to_dir_simple(self):
        assert hostname_to_dir("gms.example.com") == "gms.example.com"

    def test_hostname_to_dir_with_port(self):
        assert hostname_to_dir("localhost:8080") == "localhost_8080"

    def test_hostname_to_dir_with_protocol(self):
        assert hostname_to_dir("https://gms.example.com") == "gms.example.com"

    def test_url_to_hostname_with_port(self):
        assert (
            url_to_hostname("https://gms.example.com:8080/path")
            == "gms.example.com:8080"
        )

    def test_url_to_hostname_simple(self):
        assert url_to_hostname("http://localhost:8080") == "localhost:8080"

    def test_url_to_hostname_without_port(self):
        assert url_to_hostname("https://gms.example.com") == "gms.example.com"


class TestEndpointRegistry:
    """Tests for EndpointRegistry."""

    def test_add_and_get_endpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = EndpointRegistry(Path(tmpdir))

            # Add endpoint
            endpoint = registry.add_endpoint("https://test.example.com", alias="Test")

            assert endpoint.hostname == "test.example.com"
            assert endpoint.alias == "Test"

            # Get endpoint
            retrieved = registry.get_endpoint("test.example.com")
            assert retrieved is not None
            assert retrieved.hostname == "test.example.com"

    def test_list_endpoints(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = EndpointRegistry(Path(tmpdir))

            registry.add_endpoint("https://one.example.com", alias="One")
            registry.add_endpoint("https://two.example.com", alias="Two")

            endpoints = registry.list_endpoints()
            assert len(endpoints) == 2
            hostnames = {ep.hostname for ep in endpoints}
            assert hostnames == {"one.example.com", "two.example.com"}

    def test_update_endpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = EndpointRegistry(Path(tmpdir))

            registry.add_endpoint("https://test.example.com", alias="Old")
            registry.update_endpoint("test.example.com", alias="New")

            endpoint = registry.get_endpoint("test.example.com")
            assert endpoint is not None
            assert endpoint.alias == "New"

    def test_remove_endpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = EndpointRegistry(Path(tmpdir))

            registry.add_endpoint("https://test.example.com")
            assert registry.get_endpoint("test.example.com") is not None

            result = registry.remove_endpoint("test.example.com")
            assert result is True
            assert registry.get_endpoint("test.example.com") is None

    def test_remove_nonexistent_endpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            registry = EndpointRegistry(Path(tmpdir))
            result = registry.remove_endpoint("nonexistent.example.com")
            assert result is False

    def test_registry_persistence(self):
        """Test that registry data persists across instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create first registry and add endpoint
            registry1 = EndpointRegistry(Path(tmpdir))
            registry1.add_endpoint("https://test.example.com", alias="Persistent")

            # Create second registry pointing to same directory
            registry2 = EndpointRegistry(Path(tmpdir))
            endpoint = registry2.get_endpoint("test.example.com")

            assert endpoint is not None
            assert endpoint.alias == "Persistent"


class TestCacheIndexData:
    """Tests for CacheIndexData dataclass."""

    def test_version(self):
        index = CacheIndexData()
        assert index.version == "2.0"

    def test_get_total_event_count_empty(self):
        index = CacheIndexData()
        assert index.get_total_event_count() == 0

    def test_get_total_event_count_with_aspects(self):
        index = CacheIndexData()
        # Use dict format instead of AspectCacheInfo directly
        index.aspects["assertionRunEvent"] = {"event_count": 100}
        index.aspects["assertionDryRunEvent"] = {"event_count": 50}

        assert index.get_total_event_count() == 150

    def test_get_aspect_info(self):
        index = CacheIndexData()
        # Set aspect info using dict
        index.aspects["assertionRunEvent"] = {
            "event_count": 100,
            "unique_entities": 10,
        }

        retrieved = index.get_aspect_info("assertionRunEvent")
        assert retrieved is not None
        assert retrieved.event_count == 100
        assert retrieved.unique_entities == 10

    def test_last_sync_across_aspects(self):
        index = CacheIndexData()
        index.aspects["assertionRunEvent"] = {
            "event_count": 100,
            "last_sync": "2024-01-01T00:00:00",
        }
        index.aspects["assertionDryRunEvent"] = {
            "event_count": 50,
            "last_sync": "2024-01-02T00:00:00",
        }

        # Should return the latest sync time
        assert index.last_sync == "2024-01-02T00:00:00"


class TestAspectCacheInfo:
    """Tests for AspectCacheInfo dataclass."""

    def test_from_dict(self):
        data = {
            "event_count": 100,
            "unique_entities": 10,
            "last_sync": "2024-01-01T00:00:00",
            "data_range_start": "2023-12-01T00:00:00",
            "data_range_end": "2024-01-01T00:00:00",
        }
        info = AspectCacheInfo.from_dict(data)

        assert info.event_count == 100
        assert info.unique_entities == 10
        assert info.last_sync == "2024-01-01T00:00:00"

    def test_create_and_to_dict(self):
        info = AspectCacheInfo(
            aspect_name="assertionRunEvent",
            entity_type="assertion",
            cache_path="/tmp/test.parquet",
            event_count=100,
            unique_entities=10,
            last_sync="2024-01-01T00:00:00",
        )
        d = info.to_dict()

        assert d["event_count"] == 100
        assert d["unique_entities"] == 10
        assert d["last_sync"] == "2024-01-01T00:00:00"
        assert d["aspect_name"] == "assertionRunEvent"


class TestEndpointCache:
    """Tests for EndpointCache."""

    def test_save_and_load_aspect_events(self):
        """Test saving and loading events for a specific aspect."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Create test data
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:1",
                    ],
                    "status": ["COMPLETE", "COMPLETE", "COMPLETE"],
                }
            )

            # Save for a specific aspect
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Load
            loaded = cache.load_aspect_events("assertionRunEvent")
            assert loaded is not None
            assert len(loaded) == 3

    def test_cache_size(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "assertionUrn": ["urn:li:assertion:1"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            size = cache.get_cache_size_mb()
            assert size > 0

    def test_multiple_aspects(self):
        """Test saving and loading multiple aspect types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save assertionRunEvent
            df1 = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:2"],
                    "status": ["COMPLETE", "COMPLETE"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df1, sync_type="full")

            # Save assertionDryRunEvent
            df2 = pd.DataFrame(
                {
                    "timestampMillis": [3000],
                    "assertionUrn": ["urn:li:assertion:1"],
                    "status": ["COMPLETE"],
                }
            )
            cache.save_aspect_events("assertionDryRunEvent", df2, sync_type="full")

            # Verify both are stored separately
            loaded1 = cache.load_aspect_events("assertionRunEvent")
            loaded2 = cache.load_aspect_events("assertionDryRunEvent")

            assert loaded1 is not None
            assert loaded2 is not None
            assert len(loaded1) == 2
            assert len(loaded2) == 1

            # Verify index updated correctly
            assert "assertionRunEvent" in cache.index.data.aspects
            assert "assertionDryRunEvent" in cache.index.data.aspects

    def test_load_aspect_events_with_filter(self):
        """Test filtering when loading aspect events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:1",
                    ],
                    "status": ["COMPLETE", "COMPLETE", "COMPLETE"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Filter by entity URN
            loaded = cache.load_aspect_events(
                "assertionRunEvent", entity_urn="urn:li:assertion:1"
            )
            assert loaded is not None
            assert len(loaded) == 2

    def test_get_aspect_path(self):
        """Test aspect path generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            path = cache.get_aspect_path("assertionRunEvent")
            assert "timeseries" in str(path)
            assert "assertion" in str(path)
            assert "assertionRunEvent.parquet" in str(path)

    def test_clear_specific_aspect(self):
        """Test clearing a specific aspect cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save two aspects
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "assertionUrn": ["urn:li:assertion:1"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")
            cache.save_aspect_events("assertionDryRunEvent", df, sync_type="full")

            # Clear only one
            cache.clear("assertionRunEvent")

            # Verify
            assert cache.load_aspect_events("assertionRunEvent") is None
            assert cache.load_aspect_events("assertionDryRunEvent") is not None


class TestRunEventCache:
    """Tests for RunEventCache - the high-level interface."""

    def test_save_aspect_events(self):
        """Test saving events for a specific aspect."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = RunEventCache(Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:2"],
                    "status": ["COMPLETE", "COMPLETE"],
                }
            )

            cache.save_aspect_events(
                hostname="test.example.com",
                aspect_name="assertionRunEvent",
                df=df,
                alias="Test",
                sync_type="full",
            )

            # Verify via get_cached_aspect_events
            events = cache.get_cached_aspect_events(
                "test.example.com", "assertionRunEvent"
            )
            assert events is not None
            assert len(events) == 2

    def test_save_aspect_events_empty(self):
        """Test that empty dataframe is handled gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = RunEventCache(Path(tmpdir))

            df = pd.DataFrame()

            # Should not raise, should just return
            cache.save_aspect_events(
                hostname="test.example.com",
                aspect_name="assertionRunEvent",
                df=df,
            )

            # No endpoint should be registered for empty data
            endpoint = cache.registry.get_endpoint("test.example.com")
            assert endpoint is None

    def test_sync_from_parquet(self):
        """Test syncing from a parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = RunEventCache(Path(tmpdir))

            # Create a test parquet file with metric cube data structure
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "metricCubeUrn": [
                        "urn:li:dataHubMetricCube:1",
                        "urn:li:dataHubMetricCube:2",
                    ],
                    "measure": [100.0, 200.0],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)

            # Sync from parquet
            cache.sync_from_parquet("test.example.com", parquet_path)

            # Verify - sync_from_parquet uses default aspect "dataHubMetricCubeEvent"
            events = cache.get_cached_aspect_events(
                "test.example.com", "dataHubMetricCubeEvent"
            )
            assert events is not None
            assert len(events) == 2

    def test_get_cache_stats(self):
        """Test getting cache statistics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = RunEventCache(Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:2"],
                }
            )
            cache.save_aspect_events(
                hostname="test.example.com",
                aspect_name="assertionRunEvent",
                df=df,
                sync_type="full",
            )

            stats = cache.get_cache_stats()
            assert stats["total_endpoints"] == 1
            assert stats["total_events"] == 2
            assert stats["total_size_mb"] > 0


class TestListEntitiesPaginated:
    """Tests for list_entities_paginated - DuckDB-based pagination."""

    def test_pagination_basic(self):
        """Test basic pagination returns correct page of results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Create test data with multiple assertions
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000, 4000, 5000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:3",
                        "urn:li:assertion:4",
                        "urn:li:assertion:5",
                    ],
                    "asserteeUrn": [
                        "urn:li:dataset:a",
                        "urn:li:dataset:b",
                        "urn:li:dataset:c",
                        "urn:li:dataset:d",
                        "urn:li:dataset:e",
                    ],
                    "event_result_assertion_type": [
                        "FIELD",
                        "VOLUME",
                        "FIELD",
                        "FRESHNESS",
                        "FIELD",
                    ],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Get first page
            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=0,
                page_size=2,
            )

            assert total == 5
            assert len(results) == 2

    def test_pagination_second_page(self):
        """Test getting second page of results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000, 4000, 5000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:3",
                        "urn:li:assertion:4",
                        "urn:li:assertion:5",
                    ],
                    "asserteeUrn": [
                        "urn:li:dataset:a",
                        "urn:li:dataset:b",
                        "urn:li:dataset:c",
                        "urn:li:dataset:d",
                        "urn:li:dataset:e",
                    ],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Get second page (page=1)
            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=1,
                page_size=2,
            )

            assert total == 5
            assert len(results) == 2

    def test_pagination_last_page_partial(self):
        """Test last page with fewer results than page_size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000, 4000, 5000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:3",
                        "urn:li:assertion:4",
                        "urn:li:assertion:5",
                    ],
                    "asserteeUrn": ["ds"] * 5,
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Get last page (page=2 with page_size=2)
            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=2,
                page_size=2,
            )

            assert total == 5
            assert len(results) == 1  # Only 1 result on last page

    def test_pagination_search_filter(self):
        """Test pagination with search filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "assertionUrn": [
                        "urn:li:assertion:test_findme",
                        "urn:li:assertion:other",
                        "urn:li:assertion:another_findme",
                    ],
                    "asserteeUrn": ["ds"] * 3,
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=0,
                page_size=100,
                search_filter="findme",
            )

            assert total == 2
            assert len(results) == 2

    def test_pagination_type_filter(self):
        """Test pagination with assertion type filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:3",
                    ],
                    "asserteeUrn": ["ds"] * 3,
                    "event_result_assertion_type": ["FIELD", "VOLUME", "FIELD"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=0,
                page_size=100,
                type_filter="FIELD",
            )

            assert total == 2
            assert len(results) == 2

    def test_pagination_empty_results(self):
        """Test pagination when no results match."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "assertionUrn": ["urn:li:assertion:1"],
                    "asserteeUrn": ["ds"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=0,
                page_size=100,
                search_filter="nonexistent",
            )

            assert total == 0
            assert len(results) == 0

    def test_pagination_no_cache(self):
        """Test pagination when cache file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=0,
                page_size=100,
            )

            assert total == 0
            assert len(results) == 0

    def test_pagination_monitors(self):
        """Test pagination works for monitor aspects."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "monitorUrn": [
                        "urn:li:monitor:1",
                        "urn:li:monitor:2",
                        "urn:li:monitor:3",
                    ],
                    "assertionUrn": ["urn:li:assertion:a"] * 3,
                    "state": ["CONFIRMED", "REJECTED", "CONFIRMED"],
                }
            )
            cache.save_aspect_events("monitorAnomalyEvent", df, sync_type="full")

            results, total = cache.list_entities_paginated(
                aspect_name="monitorAnomalyEvent",
                page=0,
                page_size=2,
            )

            assert total == 3
            assert len(results) == 2

    def test_pagination_monitors_with_entity_urn(self):
        """Test pagination returns monitored_entity_urn for monitors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "monitorUrn": [
                        "urn:li:monitor:1",
                        "urn:li:monitor:2",
                        "urn:li:monitor:3",
                    ],
                    "assertionUrn": ["urn:li:assertion:a"] * 3,
                    "entityUrn": [
                        "urn:li:dataset:ds1",
                        "urn:li:dataset:ds2",
                        "urn:li:dataset:ds3",
                    ],
                    "state": ["CONFIRMED", "REJECTED", "CONFIRMED"],
                }
            )
            cache.save_aspect_events("monitorAnomalyEvent", df, sync_type="full")

            results, total = cache.list_entities_paginated(
                aspect_name="monitorAnomalyEvent",
                page=0,
                page_size=10,
            )

            assert total == 3
            assert len(results) == 3

            # Verify monitored_entity_urn is returned
            for r in results:
                assert "monitored_entity_urn" in r
                assert r["monitored_entity_urn"] is not None
                assert r["monitored_entity_urn"].startswith("urn:li:dataset:")

    def test_pagination_aggregates_multiple_events(self):
        """Test that aggregation works correctly for entities with multiple events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Same assertion appears multiple times with different timestamps
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000, 4000, 5000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:1",
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:2",
                    ],
                    "asserteeUrn": ["ds"] * 5,
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=0,
                page_size=100,
            )

            # Should have 2 unique assertions
            assert total == 2
            assert len(results) == 2

            # Check point counts are aggregated
            point_counts = {r["entity_urn"]: r["point_count"] for r in results}
            assert point_counts["urn:li:assertion:1"] == 3
            assert point_counts["urn:li:assertion:2"] == 2


class TestAspectConstants:
    """Tests for aspect-related constants."""

    def test_monitor_aspects_defined(self):
        assert "monitorAnomalyEvent" in MONITOR_ASPECTS
        assert "monitorTimeseriesState" in MONITOR_ASPECTS

    def test_metric_cube_aspects_defined(self):
        assert "dataHubMetricCubeEvent" in METRIC_CUBE_ASPECTS

    def test_all_aspects_contains_all(self):
        for aspect in MONITOR_ASPECTS:
            assert aspect in ALL_ASPECTS

        for aspect in METRIC_CUBE_ASPECTS:
            assert aspect in ALL_ASPECTS

    def test_all_aspects_have_entity_type(self):
        for _aspect, info in ALL_ASPECTS.items():
            assert "entity_type" in info
            assert info["entity_type"] in ("monitor", "dataHubMetricCube")


class TestAnomalyEditTracker:
    """Tests for AnomalyEditTracker - local-first anomaly editing."""

    def test_set_and_get_local_state(self):
        """Test setting and getting local state overrides."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            # Set a local state
            tracker.set_local_state(
                monitor_urn="urn:li:monitor:test",
                assertion_urn="urn:li:assertion:test",
                timestamp_ms=1000,
                original_state=None,
                new_state="CONFIRMED",
            )

            # Get effective state
            effective = tracker.get_effective_state("urn:li:monitor:test", 1000, None)
            assert effective == "CONFIRMED"

    def test_effective_state_without_edit(self):
        """Test that effective state returns original when no edit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            effective = tracker.get_effective_state(
                "urn:li:monitor:test", 1000, "REJECTED"
            )
            assert effective == "REJECTED"

    def test_clear_local_edit(self):
        """Test clearing a local edit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            tracker.set_local_state(
                monitor_urn="urn:li:monitor:test",
                assertion_urn="urn:li:assertion:test",
                timestamp_ms=1000,
                original_state=None,
                new_state="CONFIRMED",
            )

            # Clear the edit
            result = tracker.clear_local_edit("urn:li:monitor:test", 1000)
            assert result is True

            # Verify it's gone
            effective = tracker.get_effective_state("urn:li:monitor:test", 1000, None)
            assert effective is None

    def test_clear_nonexistent_edit(self):
        """Test clearing an edit that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            result = tracker.clear_local_edit("urn:li:monitor:nonexistent", 1000)
            assert result is False

    def test_get_pending_changes(self):
        """Test getting all pending changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            # Add multiple edits
            tracker.set_local_state(
                "urn:li:monitor:test1", "urn:li:assertion:a1", 1000, None, "CONFIRMED"
            )
            tracker.set_local_state(
                "urn:li:monitor:test2",
                "urn:li:assertion:a2",
                2000,
                "CONFIRMED",
                "REJECTED",
            )

            pending = tracker.get_pending_changes()
            assert len(pending) == 2

            # Verify AnomalyEdit structure
            for edit in pending:
                assert isinstance(edit, AnomalyEdit)
                assert edit.local_state in ("CONFIRMED", "REJECTED")

    def test_get_pending_count(self):
        """Test pending count."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            assert tracker.get_pending_count() == 0

            tracker.set_local_state(
                "urn:li:monitor:test", "urn:li:assertion:a", 1000, None, "CONFIRMED"
            )
            assert tracker.get_pending_count() == 1

            tracker.set_local_state(
                "urn:li:monitor:test2", "urn:li:assertion:b", 2000, None, "REJECTED"
            )
            assert tracker.get_pending_count() == 2

    def test_clear_all_edits(self):
        """Test clearing all local edits."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            tracker.set_local_state(
                "urn:li:monitor:test1", "urn:li:assertion:a", 1000, None, "CONFIRMED"
            )
            tracker.set_local_state(
                "urn:li:monitor:test2", "urn:li:assertion:b", 2000, None, "REJECTED"
            )

            count = tracker.clear_all_edits()
            assert count == 2
            assert tracker.get_pending_count() == 0

    def test_mark_as_published(self):
        """Test marking edits as published."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            tracker.set_local_state(
                "urn:li:monitor:test1", "urn:li:assertion:a", 1000, None, "CONFIRMED"
            )
            tracker.set_local_state(
                "urn:li:monitor:test2", "urn:li:assertion:b", 2000, None, "REJECTED"
            )

            pending = tracker.get_pending_changes()

            # Mark first one as published
            removed = tracker.mark_as_published([pending[0]])
            assert removed == 1
            assert tracker.get_pending_count() == 1

    def test_get_edits_in_range(self):
        """Test getting edits within a time range."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            tracker.set_local_state(
                "urn:li:monitor:test", "urn:li:assertion:a", 1000, None, "CONFIRMED"
            )
            tracker.set_local_state(
                "urn:li:monitor:test", "urn:li:assertion:b", 5000, None, "CONFIRMED"
            )
            tracker.set_local_state(
                "urn:li:monitor:test", "urn:li:assertion:c", 10000, None, "CONFIRMED"
            )

            # Get edits in range 2000-7000
            edits = tracker.get_edits_in_range(2000, 7000)
            assert len(edits) == 1
            assert edits[0].timestamp_ms == 5000

    def test_get_local_edit(self):
        """Test getting a specific local edit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            tracker.set_local_state(
                "urn:li:monitor:test",
                "urn:li:assertion:a",
                1000,
                "REJECTED",
                "CONFIRMED",
            )

            edit = tracker.get_local_edit("urn:li:monitor:test", 1000)
            assert edit is not None
            assert edit.original_state == "REJECTED"
            assert edit.local_state == "CONFIRMED"
            assert edit.assertion_urn == "urn:li:assertion:a"

    def test_get_local_edit_not_found(self):
        """Test getting a non-existent edit returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            edit = tracker.get_local_edit("urn:li:monitor:nonexistent", 1000)
            assert edit is None

    def test_persistence(self):
        """Test that edits persist across instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create first tracker and add edit
            tracker1 = AnomalyEditTracker(Path(tmpdir))
            tracker1.set_local_state(
                "urn:li:monitor:test", "urn:li:assertion:a", 1000, None, "CONFIRMED"
            )

            # Create second tracker pointing to same directory
            tracker2 = AnomalyEditTracker(Path(tmpdir))

            effective = tracker2.get_effective_state("urn:li:monitor:test", 1000, None)
            assert effective == "CONFIRMED"

    def test_update_existing_edit(self):
        """Test updating an existing edit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            # Set initial edit
            tracker.set_local_state(
                "urn:li:monitor:test", "urn:li:assertion:a", 1000, None, "CONFIRMED"
            )

            # Update to different state
            tracker.set_local_state(
                "urn:li:monitor:test", "urn:li:assertion:a", 1000, None, "REJECTED"
            )

            # Should still be only 1 edit
            assert tracker.get_pending_count() == 1

            # Should reflect the update
            effective = tracker.get_effective_state("urn:li:monitor:test", 1000, None)
            assert effective == "REJECTED"

    def test_create_new_anomaly(self):
        """Test creating a new anomaly from a run event."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            # Create new anomaly
            tracker.create_new_anomaly(
                monitor_urn="urn:li:monitor:test",
                assertion_urn="urn:li:assertion:test",
                run_event_timestamp_ms=1234567890,
            )

            # Check it was created
            assert tracker.get_pending_count() == 1
            assert tracker.get_new_anomaly_count() == 1

            # Get the edit
            edit = tracker.get_local_edit("urn:li:monitor:test", 1234567890)
            assert edit is not None
            assert edit.is_new is True
            assert edit.local_state == "CONFIRMED"
            assert edit.run_event_timestamp_ms == 1234567890
            assert edit.original_state is None

    def test_get_new_anomalies(self):
        """Test getting only new anomalies (not state changes)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            # Add a state change (existing anomaly)
            tracker.set_local_state(
                "urn:li:monitor:test",
                "urn:li:assertion:a",
                1000,
                "REJECTED",
                "CONFIRMED",
            )

            # Add new anomalies
            tracker.create_new_anomaly(
                "urn:li:monitor:test", "urn:li:assertion:b", 2000
            )
            tracker.create_new_anomaly(
                "urn:li:monitor:test", "urn:li:assertion:c", 3000
            )

            # Should have 3 total pending changes
            assert tracker.get_pending_count() == 3

            # But only 2 new anomalies
            new_anomalies = tracker.get_new_anomalies()
            assert len(new_anomalies) == 2
            assert tracker.get_new_anomaly_count() == 2

            # Verify all returned are new
            for edit in new_anomalies:
                assert edit.is_new is True

    def test_remove_new_anomaly(self):
        """Test removing a new anomaly marking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            # Create new anomaly
            tracker.create_new_anomaly(
                "urn:li:monitor:test", "urn:li:assertion:test", 1234567890
            )
            assert tracker.get_new_anomaly_count() == 1

            # Remove it
            result = tracker.remove_new_anomaly("urn:li:monitor:test", 1234567890)
            assert result is True
            assert tracker.get_new_anomaly_count() == 0

    def test_remove_nonexistent_new_anomaly(self):
        """Test removing a new anomaly that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            result = tracker.remove_new_anomaly("urn:li:monitor:nonexistent", 1000)
            assert result is False

    def test_new_anomaly_persists(self):
        """Test that new anomalies persist across instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create first tracker and add new anomaly
            tracker1 = AnomalyEditTracker(Path(tmpdir))
            tracker1.create_new_anomaly(
                "urn:li:monitor:test", "urn:li:assertion:test", 1234567890
            )

            # Create second tracker
            tracker2 = AnomalyEditTracker(Path(tmpdir))

            # Should find the new anomaly
            assert tracker2.get_new_anomaly_count() == 1
            edit = tracker2.get_local_edit("urn:li:monitor:test", 1234567890)
            assert edit is not None
            assert edit.is_new is True

    def test_is_new_flag_in_pending_changes(self):
        """Test that is_new flag is correctly included in pending changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            # Mix of state changes and new anomalies
            tracker.set_local_state(
                "urn:li:monitor:test",
                "urn:li:assertion:a",
                1000,
                "REJECTED",
                "CONFIRMED",
                is_new=False,
            )
            tracker.create_new_anomaly(
                "urn:li:monitor:test", "urn:li:assertion:b", 2000
            )

            pending = tracker.get_pending_changes()
            assert len(pending) == 2

            # Find the state change
            state_change = next((e for e in pending if e.timestamp_ms == 1000), None)
            assert state_change is not None
            assert state_change.is_new is False

            # Find the new anomaly
            new_anomaly = next((e for e in pending if e.timestamp_ms == 2000), None)
            assert new_anomaly is not None
            assert new_anomaly.is_new is True

    def test_is_new_flag_in_edits_range(self):
        """Test that is_new flag is included in edits_in_range."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = AnomalyEditTracker(Path(tmpdir))

            tracker.create_new_anomaly(
                "urn:li:monitor:test", "urn:li:assertion:a", 5000
            )

            edits = tracker.get_edits_in_range(0, 10000)
            assert len(edits) == 1
            assert edits[0].is_new is True
            assert edits[0].run_event_timestamp_ms == 5000


class TestDeleteEntityEvents:
    """Tests for EndpointCache.delete_entity_events method."""

    def test_delete_entity_events_single_entity(self):
        """Delete one entity's events, verify others remain."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Create test data with multiple entities
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000, 4000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:3",
                    ],
                    "status": ["COMPLETE"] * 4,
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Delete one entity
            deleted = cache.delete_entity_events(
                "urn:li:assertion:1", "assertionRunEvent"
            )

            assert deleted == 2  # Two events for assertion:1

            # Verify remaining events
            loaded = cache.load_aspect_events("assertionRunEvent")
            assert loaded is not None
            assert len(loaded) == 2
            assert "urn:li:assertion:1" not in loaded["assertionUrn"].values
            assert "urn:li:assertion:2" in loaded["assertionUrn"].values
            assert "urn:li:assertion:3" in loaded["assertionUrn"].values

    def test_delete_entity_events_all_entities(self):
        """Delete last entity's events, verify file is removed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Create test data with single entity
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:1"],
                    "status": ["COMPLETE", "COMPLETE"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Verify file exists
            path = cache.get_aspect_path("assertionRunEvent")
            assert path.exists()

            # Delete the only entity
            deleted = cache.delete_entity_events(
                "urn:li:assertion:1", "assertionRunEvent"
            )

            assert deleted == 2

            # Verify file is removed
            assert not path.exists()

            # Verify index is updated
            assert "assertionRunEvent" not in cache.index.data.aspects

    def test_delete_entity_events_nonexistent_entity(self):
        """Delete non-existent URN returns 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "assertionUrn": ["urn:li:assertion:1"],
                    "status": ["COMPLETE"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Try to delete non-existent entity
            deleted = cache.delete_entity_events(
                "urn:li:assertion:nonexistent", "assertionRunEvent"
            )

            assert deleted == 0

            # Verify original data unchanged
            loaded = cache.load_aspect_events("assertionRunEvent")
            assert loaded is not None
            assert len(loaded) == 1

    def test_delete_entity_events_nonexistent_file(self):
        """Returns 0 when no cache file exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Try to delete without any cached data
            deleted = cache.delete_entity_events(
                "urn:li:assertion:1", "assertionRunEvent"
            )

            assert deleted == 0

    def test_delete_entity_events_index_updated(self):
        """Verify index counts are updated after deletion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "monitorUrn": [
                        "urn:li:monitor:1",
                        "urn:li:monitor:1",
                        "urn:li:monitor:2",
                    ],
                    "state": ["CONFIRMED"] * 3,
                }
            )
            cache.save_aspect_events("monitorAnomalyEvent", df, sync_type="full")

            # Initial index state
            initial_info = cache.index.data.get_aspect_info("monitorAnomalyEvent")
            assert initial_info is not None
            assert initial_info.event_count == 3
            assert initial_info.unique_entities == 2

            # Delete one entity
            cache.delete_entity_events("urn:li:monitor:1", "monitorAnomalyEvent")

            # Verify index updated
            updated_info = cache.index.data.get_aspect_info("monitorAnomalyEvent")
            assert updated_info is not None
            assert updated_info.event_count == 1
            assert updated_info.unique_entities == 1


class TestSyncModes:
    """Tests for sync mode behaviors in save_aspect_events."""

    def test_incremental_sync_appends_new_events(self):
        """New events are added to existing cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Initial save
            df1 = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:2"],
                    "status": ["COMPLETE", "COMPLETE"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df1, sync_type="full")

            # Incremental save with new events
            df2 = pd.DataFrame(
                {
                    "timestampMillis": [3000, 4000],
                    "assertionUrn": ["urn:li:assertion:3", "urn:li:assertion:4"],
                    "status": ["COMPLETE", "COMPLETE"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df2, sync_type="incremental")

            # Verify all events present
            loaded = cache.load_aspect_events("assertionRunEvent")
            assert loaded is not None
            assert len(loaded) == 4
            assert set(loaded["assertionUrn"]) == {
                "urn:li:assertion:1",
                "urn:li:assertion:2",
                "urn:li:assertion:3",
                "urn:li:assertion:4",
            }

    def test_incremental_sync_deduplicates(self):
        """Same (timestamp, urn) keeps latest value."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Initial save
            df1 = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:1"],
                    "status": ["COMPLETE", "COMPLETE"],
                    "value": ["old1", "old2"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df1, sync_type="full")

            # Incremental save with duplicate timestamp+urn but different value
            df2 = pd.DataFrame(
                {
                    "timestampMillis": [1000],  # Same timestamp as existing
                    "assertionUrn": ["urn:li:assertion:1"],  # Same URN
                    "status": ["COMPLETE"],
                    "value": ["new1"],  # Different value
                }
            )
            cache.save_aspect_events("assertionRunEvent", df2, sync_type="incremental")

            # Verify deduplication kept latest
            loaded = cache.load_aspect_events("assertionRunEvent")
            assert loaded is not None
            assert len(loaded) == 2  # Should still have 2 unique events

            # The event at timestamp 1000 should have the new value
            event_1000 = loaded[loaded["timestampMillis"] == 1000]
            assert len(event_1000) == 1
            assert event_1000["value"].iloc[0] == "new1"

    def test_full_sync_replaces_all(self):
        """Full sync completely replaces existing file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Initial save with multiple entities
            df1 = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:3",
                    ],
                    "status": ["COMPLETE"] * 3,
                }
            )
            cache.save_aspect_events("assertionRunEvent", df1, sync_type="full")

            # Full sync with only one entity
            df2 = pd.DataFrame(
                {
                    "timestampMillis": [4000],
                    "assertionUrn": ["urn:li:assertion:new"],
                    "status": ["COMPLETE"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df2, sync_type="full")

            # Verify only new data exists
            loaded = cache.load_aspect_events("assertionRunEvent")
            assert loaded is not None
            assert len(loaded) == 1
            assert loaded["assertionUrn"].iloc[0] == "urn:li:assertion:new"

    def test_full_sync_with_urns_targeted_delete(self):
        """Targeted replacement: delete specific URNs then append."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Initial save with multiple entities
            df1 = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:3",
                    ],
                    "status": ["COMPLETE"] * 3,
                    "value": ["orig1", "orig2", "orig3"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df1, sync_type="full")

            # Simulate targeted replace: delete specific URN first
            cache.delete_entity_events("urn:li:assertion:1", "assertionRunEvent")

            # Then incremental save with new data for that URN
            df2 = pd.DataFrame(
                {
                    "timestampMillis": [4000],
                    "assertionUrn": ["urn:li:assertion:1"],
                    "status": ["COMPLETE"],
                    "value": ["refreshed1"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df2, sync_type="incremental")

            # Verify result
            loaded = cache.load_aspect_events("assertionRunEvent")
            assert loaded is not None
            assert len(loaded) == 3  # 1 refreshed + 2 preserved

            # Check assertion:1 has new value
            a1 = loaded[loaded["assertionUrn"] == "urn:li:assertion:1"]
            assert len(a1) == 1
            assert a1["value"].iloc[0] == "refreshed1"
            assert a1["timestampMillis"].iloc[0] == 4000

            # Check others unchanged
            a2 = loaded[loaded["assertionUrn"] == "urn:li:assertion:2"]
            assert a2["value"].iloc[0] == "orig2"
            a3 = loaded[loaded["assertionUrn"] == "urn:li:assertion:3"]
            assert a3["value"].iloc[0] == "orig3"


class TestDuckDBOptimizations:
    """Tests for DuckDB query optimizations."""

    def test_single_pass_pagination_returns_correct_total(self):
        """Verify total count is correct with single-pass window function approach."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Create test data with multiple entities
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000, 4000, 5000, 6000, 7000],
                    "assertionUrn": [
                        "urn:li:assertion:1",
                        "urn:li:assertion:1",
                        "urn:li:assertion:2",
                        "urn:li:assertion:3",
                        "urn:li:assertion:4",
                        "urn:li:assertion:5",
                        "urn:li:assertion:6",
                    ],
                    "asserteeUrn": ["ds"] * 7,
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Get first page with small page size
            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=0,
                page_size=2,
            )

            # Should report correct total despite only returning 2 results
            assert total == 6  # 6 unique entities
            assert len(results) == 2

            # Verify second page also has correct total
            results2, total2 = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=1,
                page_size=2,
            )

            assert total2 == 6  # Same total
            assert len(results2) == 2

            # Verify last page
            results3, total3 = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=2,
                page_size=2,
            )

            assert total3 == 6
            assert len(results3) == 2  # Last 2 entities

    def test_connection_reuse_same_instance(self):
        """Verify same DuckDB connection is returned on multiple accesses."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Create test data so we have something to query
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "assertionUrn": ["urn:li:assertion:1"],
                    "asserteeUrn": ["ds"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Access duckdb_conn multiple times
            conn1 = cache.duckdb_conn
            conn2 = cache.duckdb_conn
            conn3 = cache.duckdb_conn

            # Should be the exact same object
            assert conn1 is conn2
            assert conn2 is conn3

            # Should be able to execute queries
            result = conn1.execute("SELECT 1 as test").fetchone()
            assert result[0] == 1

    def test_schema_caching_persists_after_sync(self):
        """Verify schema is cached and available after save operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Initially no schema cached
            assert len(cache._schema_cache) == 0

            # Save data
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:2"],
                    "asserteeUrn": ["ds1", "ds2"],
                    "custom_col": ["val1", "val2"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Schema should be invalidated after save (not pre-populated)
            assert "assertionRunEvent" not in cache._schema_cache

            # Now access schema
            columns = cache.get_schema_columns("assertionRunEvent")

            # Should be cached now
            assert "assertionRunEvent" in cache._schema_cache
            assert columns == cache._schema_cache["assertionRunEvent"]

            # Verify expected columns present
            assert "timestampMillis" in columns
            assert "assertionUrn" in columns
            assert "asserteeUrn" in columns
            assert "custom_col" in columns

    def test_schema_cache_used_in_pagination(self):
        """Verify cached schema is used in pagination (no DESCRIBE query)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save data
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:2"],
                    "asserteeUrn": ["ds1", "ds2"],
                    "event_result_assertion_type": ["FIELD", "VOLUME"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Pre-populate schema cache
            columns = cache.get_schema_columns("assertionRunEvent")
            assert "assertionRunEvent" in cache._schema_cache

            # Call pagination - should use cached schema
            results, total = cache.list_entities_paginated(
                aspect_name="assertionRunEvent",
                page=0,
                page_size=10,
                type_filter="FIELD",
            )

            # Verify it worked (filter applied using cached schema info)
            assert total == 1  # Only one FIELD type
            assert len(results) == 1

            # Schema cache should still contain same entry (wasn't re-queried)
            assert cache._schema_cache["assertionRunEvent"] == columns

    def test_column_projection_loads_subset(self):
        """Verify only requested columns are loaded when column projection is used."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save data with many columns
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:2"],
                    "asserteeUrn": ["ds1", "ds2"],
                    "status": ["COMPLETE", "COMPLETE"],
                    "col1": ["a", "b"],
                    "col2": ["c", "d"],
                    "col3": ["e", "f"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Load with specific columns only
            loaded = cache.load_aspect_events(
                "assertionRunEvent",
                columns=["timestampMillis", "assertionUrn"],
            )

            assert loaded is not None
            assert len(loaded) == 2

            # Should only have requested columns
            assert set(loaded.columns) == {"timestampMillis", "assertionUrn"}
            assert "status" not in loaded.columns
            assert "col1" not in loaded.columns

            # Load all columns (no projection)
            loaded_all = cache.load_aspect_events("assertionRunEvent")

            assert loaded_all is not None
            # Should have all columns
            assert "timestampMillis" in loaded_all.columns
            assert "assertionUrn" in loaded_all.columns
            assert "asserteeUrn" in loaded_all.columns
            assert "status" in loaded_all.columns
            assert "col1" in loaded_all.columns
            assert "col2" in loaded_all.columns
            assert "col3" in loaded_all.columns

    def test_column_projection_invalid_columns_fallback(self):
        """Verify fallback to all columns when requested columns don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "assertionUrn": ["urn:li:assertion:1"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Request columns that don't exist
            loaded = cache.load_aspect_events(
                "assertionRunEvent",
                columns=["nonexistent_col1", "nonexistent_col2"],
            )

            assert loaded is not None
            # Should fall back to all columns since none of the requested exist
            assert "timestampMillis" in loaded.columns
            assert "assertionUrn" in loaded.columns

    def test_schema_invalidation_on_save(self):
        """Verify schema cache is invalidated when new data is saved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save initial data
            df1 = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "assertionUrn": ["urn:li:assertion:1"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df1, sync_type="full")

            # Populate schema cache
            cols1 = cache.get_schema_columns("assertionRunEvent")
            assert "timestampMillis" in cols1
            assert "assertionUrn" in cols1
            assert "newColumn" not in cols1

            # Save new data with additional column
            df2 = pd.DataFrame(
                {
                    "timestampMillis": [2000],
                    "assertionUrn": ["urn:li:assertion:2"],
                    "newColumn": ["value"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df2, sync_type="full")

            # Schema cache should be invalidated
            assert "assertionRunEvent" not in cache._schema_cache

            # Re-query schema should show new column
            cols2 = cache.get_schema_columns("assertionRunEvent")
            assert "newColumn" in cols2

    def test_schema_invalidation_on_clear(self):
        """Verify schema cache is invalidated when cache is cleared."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "assertionUrn": ["urn:li:assertion:1"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Populate schema cache
            cache.get_schema_columns("assertionRunEvent")
            assert "assertionRunEvent" in cache._schema_cache

            # Clear specific aspect
            cache.clear("assertionRunEvent")

            # Schema should be invalidated
            assert "assertionRunEvent" not in cache._schema_cache

    def test_schema_invalidation_on_delete_entity(self):
        """Verify schema cache is invalidated when entity events are deleted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "assertionUrn": ["urn:li:assertion:1", "urn:li:assertion:2"],
                }
            )
            cache.save_aspect_events("assertionRunEvent", df, sync_type="full")

            # Populate schema cache
            cache.get_schema_columns("assertionRunEvent")
            assert "assertionRunEvent" in cache._schema_cache

            # Delete one entity
            cache.delete_entity_events("urn:li:assertion:1", "assertionRunEvent")

            # Schema should be invalidated (file was rewritten)
            assert "assertionRunEvent" not in cache._schema_cache


class TestInferenceDataStorage:
    """Tests for EndpointCache inference data storage methods."""

    def test_save_and_load_inference_data(self):
        """Test saving and loading inference data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save inference data
            predictions_df = pd.DataFrame(
                {
                    "timestamp_ms": [1000, 2000, 3000],
                    "yhat": [100.0, 105.0, 102.0],
                }
            )

            result = cache.save_inference_data(
                entity_urn="urn:li:assertion:test123",
                model_config_dict={"forecast_model_name": "prophet"},
                preprocessing_config_json='{"normalize": true}',
                forecast_config_json='{"model": "prophet"}',
                anomaly_config_json='{"threshold": 0.95}',
                forecast_evals_json='{"aggregated": {"mae": 0.1}}',
                anomaly_evals_json='{"aggregated": {"precision": 0.9}}',
                predictions_df=predictions_df,
                generated_at=1700000000000,
            )

            assert result is True

            # Load and verify
            loaded = cache.load_inference_data("urn:li:assertion:test123")

            assert loaded is not None
            assert loaded["entity_urn"] == "urn:li:assertion:test123"
            assert loaded["generated_at"] == 1700000000000
            assert loaded["model_config"]["forecast_model_name"] == "prophet"
            assert loaded["preprocessing_config_json"] == '{"normalize": true}'
            assert loaded["forecast_config_json"] == '{"model": "prophet"}'
            assert loaded["anomaly_config_json"] == '{"threshold": 0.95}'
            assert loaded["forecast_evals_json"] == '{"aggregated": {"mae": 0.1}}'
            assert loaded["anomaly_evals_json"] == '{"aggregated": {"precision": 0.9}}'

            # Verify predictions DataFrame
            assert loaded["predictions_df"] is not None
            assert len(loaded["predictions_df"]) == 3

    def test_load_nonexistent_inference_data(self):
        """Test loading inference data that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            loaded = cache.load_inference_data("urn:li:assertion:nonexistent")
            assert loaded is None

    def test_list_saved_inference_data(self):
        """Test listing saved inference data entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save multiple entries
            cache.save_inference_data(
                entity_urn="urn:li:assertion:test1",
                model_config_dict={"forecast_model_name": "prophet"},
                preprocessing_config_json='{"a": 1}',
                generated_at=1000,
            )
            cache.save_inference_data(
                entity_urn="urn:li:assertion:test2",
                model_config_dict={"anomaly_model_name": "iforest"},
                anomaly_config_json='{"b": 2}',
                generated_at=2000,
            )

            entries = cache.list_saved_inference_data()

            assert len(entries) == 2
            urns = {e["entity_urn"] for e in entries}
            assert urns == {"urn:li:assertion:test1", "urn:li:assertion:test2"}

            # Check metadata extraction
            test1_entry = next(
                e for e in entries if e["entity_urn"] == "urn:li:assertion:test1"
            )
            assert test1_entry["has_preprocessing_config"] is True
            assert test1_entry["has_forecast_config"] is False
            assert test1_entry["forecast_model_name"] == "prophet"

    def test_delete_inference_data(self):
        """Test deleting inference data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save data
            cache.save_inference_data(
                entity_urn="urn:li:assertion:test",
                preprocessing_config_json='{"test": true}',
            )

            # Verify it exists
            assert cache.inference_data_exists("urn:li:assertion:test") is True

            # Delete
            result = cache.delete_inference_data("urn:li:assertion:test")
            assert result is True

            # Verify it's gone
            assert cache.inference_data_exists("urn:li:assertion:test") is False

    def test_delete_nonexistent_inference_data(self):
        """Test deleting inference data that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            result = cache.delete_inference_data("urn:li:assertion:nonexistent")
            assert result is False

    def test_inference_data_exists(self):
        """Test checking if inference data exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            assert cache.inference_data_exists("urn:li:assertion:test") is False

            cache.save_inference_data(
                entity_urn="urn:li:assertion:test",
                preprocessing_config_json='{"test": true}',
            )

            assert cache.inference_data_exists("urn:li:assertion:test") is True

    def test_update_inference_config_preprocessing(self):
        """Test updating preprocessing config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save initial data
            cache.save_inference_data(
                entity_urn="urn:li:assertion:test",
                preprocessing_config_json='{"old": true}',
            )

            # Update
            result = cache.update_inference_config(
                entity_urn="urn:li:assertion:test",
                preprocessing_config_json='{"new": true}',
            )
            assert result is True

            # Verify update
            loaded = cache.load_inference_data("urn:li:assertion:test")
            assert loaded is not None
            assert loaded["preprocessing_config_json"] == '{"new": true}'

    def test_update_inference_config_forecast(self):
        """Test updating forecast config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save initial data (no forecast config)
            cache.save_inference_data(
                entity_urn="urn:li:assertion:test",
                preprocessing_config_json='{"a": 1}',
            )

            # Add forecast config via update
            result = cache.update_inference_config(
                entity_urn="urn:li:assertion:test",
                forecast_config_json='{"model": "prophet"}',
            )
            assert result is True

            # Verify
            loaded = cache.load_inference_data("urn:li:assertion:test")
            assert loaded is not None
            assert loaded["forecast_config_json"] == '{"model": "prophet"}'
            # Preprocessing should still be there
            assert loaded["preprocessing_config_json"] == '{"a": 1}'

    def test_update_nonexistent_inference_data(self):
        """Test updating inference data that doesn't exist fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            result = cache.update_inference_config(
                entity_urn="urn:li:assertion:nonexistent",
                preprocessing_config_json='{"test": true}',
            )
            assert result is False

    def test_save_inference_data_minimal(self):
        """Test saving with minimal data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save with only entity_urn
            result = cache.save_inference_data(entity_urn="urn:li:assertion:minimal")
            assert result is True

            # Load and verify
            loaded = cache.load_inference_data("urn:li:assertion:minimal")
            assert loaded is not None
            assert loaded["entity_urn"] == "urn:li:assertion:minimal"
            assert loaded.get("preprocessing_config_json") is None

    def test_inference_data_urn_escaping(self):
        """Test that URNs with special characters are handled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # URN with special characters
            entity_urn = "urn:li:assertion:test,dataset:abc"

            result = cache.save_inference_data(
                entity_urn=entity_urn,
                preprocessing_config_json='{"test": true}',
            )
            assert result is True

            # Verify it can be loaded
            loaded = cache.load_inference_data(entity_urn)
            assert loaded is not None
            assert loaded["entity_urn"] == entity_urn
