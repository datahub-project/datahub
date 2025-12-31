"""Tests for the data_loaders module."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from scripts.streamlit_explorer.common.data_loaders import DataLoader, get_data_loader


class TestDataLoaderInit:
    """Tests for DataLoader initialization."""

    def test_default_initialization(self):
        """Test DataLoader can be initialized with defaults."""
        loader = DataLoader()
        assert loader.cache_dir is not None
        assert loader.cache is not None
        assert loader.registry is not None

    def test_custom_cache_dir(self):
        """Test DataLoader with custom cache directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))
            assert loader.cache_dir == Path(tmpdir)


class TestLocalParquetLoading:
    """Tests for loading local parquet files."""

    def test_load_local_parquet_success(self):
        """Test loading a valid parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Create test parquet file
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "assertionUrn": ["urn:li:assertion:1"] * 3,
                    "value": [1.0, 2.0, 3.0],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)

            # Load
            loaded = loader.load_local_parquet(parquet_path)
            assert len(loaded) == 3
            assert "timestampMillis" in loaded.columns

    def test_load_local_parquet_not_found(self):
        """Test loading a non-existent parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            with pytest.raises(FileNotFoundError):
                loader.load_local_parquet("/nonexistent/path.parquet")

    def test_load_local_parquet_with_limit(self):
        """Test loading with a row limit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Create test data with many rows
            df = pd.DataFrame(
                {
                    "timestampMillis": range(100),
                    "value": range(100),
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)

            # Load with limit
            loaded = loader.load_local_parquet(parquet_path, limit=10, use_duckdb=False)
            assert len(loaded) == 10

    def test_load_local_parquet_with_sample(self):
        """Test loading with sampling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Create test data
            df = pd.DataFrame(
                {
                    "timestampMillis": range(1000),
                    "value": range(1000),
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)

            # Load with sampling (use pandas fallback for deterministic test)
            loaded = loader.load_local_parquet(
                parquet_path, sample_fraction=0.1, use_duckdb=False
            )
            # Allow some variance due to random sampling
            assert len(loaded) < 500
            assert len(loaded) > 0


class TestParquetMetadata:
    """Tests for parquet metadata extraction."""

    def test_get_local_parquet_metadata(self):
        """Test getting metadata from a parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Create test parquet file
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "assertionUrn": ["urn:li:assertion:1"] * 3,
                    "value": [1.0, 2.0, 3.0],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)

            # Get metadata
            metadata = loader.get_local_parquet_metadata(parquet_path)

            assert metadata["path"] == str(parquet_path)
            assert metadata["row_count"] == 3
            assert "timestampMillis" in metadata["columns"]
            assert "assertionUrn" in metadata["columns"]
            assert metadata["size_mb"] > 0

    def test_get_local_parquet_metadata_not_found(self):
        """Test getting metadata from non-existent file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            with pytest.raises(FileNotFoundError):
                loader.get_local_parquet_metadata("/nonexistent/path.parquet")


class TestCachedDataLoading:
    """Tests for cached data operations."""

    def test_cache_from_parquet_and_load(self):
        """Test caching from parquet and then loading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            from scripts.streamlit_explorer.common.metric_cube_extractor import (
                build_metric_cube_urn,
            )

            monitor_urn = "urn:li:monitor:test"
            cube_urn = build_metric_cube_urn(monitor_urn)

            # Create test parquet file with metric cube data
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "metricCubeUrn": [cube_urn] * 3,
                    "monitorUrn": [monitor_urn] * 3,
                    "measure": [1.0, 2.0, 3.0],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)

            # Cache from parquet using cache manager directly
            loader.cache.sync_from_parquet(
                hostname="test.example.com",
                parquet_path=parquet_path,
                aspect_name="dataHubMetricCubeEvent",
            )

            # Load cached events
            loaded = loader.load_cached_events(
                "test.example.com", aspect_name="dataHubMetricCubeEvent"
            )
            assert loaded is not None
            assert len(loaded) == 3

    def test_load_cached_events_nonexistent(self):
        """Test loading from non-existent cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            result = loader.load_cached_events("nonexistent.example.com")
            assert result is None

    def test_load_cached_events_with_entity_filter(self):
        """Test loading cached events with entity URN filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            from scripts.streamlit_explorer.common.metric_cube_extractor import (
                build_metric_cube_urn,
            )

            monitor_urn1 = "urn:li:monitor:1"
            monitor_urn2 = "urn:li:monitor:2"
            cube_urn1 = build_metric_cube_urn(monitor_urn1)
            cube_urn2 = build_metric_cube_urn(monitor_urn2)

            # Create and cache test data with multiple monitors
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "metricCubeUrn": [cube_urn1, cube_urn2, cube_urn1],
                    "monitorUrn": [monitor_urn1, monitor_urn2, monitor_urn1],
                    "measure": [1.0, 2.0, 3.0],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com", parquet_path, aspect_name="dataHubMetricCubeEvent"
            )

            # Load with filter using metricCubeUrn
            loaded = loader.load_cached_events(
                "test.example.com",
                aspect_name="dataHubMetricCubeEvent",
                entity_urn=cube_urn1,
            )
            assert loaded is not None
            assert len(loaded) == 2

    def test_load_cached_metric_cube_timeseries(self):
        """Test loading a time series from metric cube cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            from scripts.streamlit_explorer.common.metric_cube_extractor import (
                build_metric_cube_urn,
            )

            monitor_urn = "urn:li:monitor:test"
            cube_urn = build_metric_cube_urn(monitor_urn)

            # Create and cache test data
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "metricCubeUrn": [cube_urn] * 3,
                    "monitorUrn": [monitor_urn] * 3,
                    "measure": [1.0, 2.0, 3.0],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com", parquet_path, aspect_name="dataHubMetricCubeEvent"
            )

            # Load timeseries
            ts = loader.load_cached_metric_cube_timeseries(
                "test.example.com",
                monitor_urn=monitor_urn,
            )
            assert len(ts) == 3
            assert "ds" in ts.columns
            assert "y" in ts.columns


class TestEndpointManagement:
    """Tests for endpoint and cache management."""

    def test_list_endpoints_empty(self):
        """Test listing endpoints when none exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))
            endpoints = loader.list_endpoints()
            assert endpoints == []

    def test_list_endpoints_with_data(self):
        """Test listing endpoints after caching data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            from scripts.streamlit_explorer.common.metric_cube_extractor import (
                build_metric_cube_urn,
            )

            monitor_urn = "urn:li:monitor:test"
            cube_urn = build_metric_cube_urn(monitor_urn)

            # Cache some metric cube data
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "metricCubeUrn": [cube_urn],
                    "monitorUrn": [monitor_urn],
                    "measure": [1.0],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="dataHubMetricCubeEvent",
            )

            # List endpoints
            endpoints = loader.list_endpoints()
            assert len(endpoints) == 1

    def test_get_endpoint_info(self):
        """Test getting detailed endpoint info."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            from scripts.streamlit_explorer.common.metric_cube_extractor import (
                build_metric_cube_urn,
            )

            monitor_urn1 = "urn:li:monitor:1"
            monitor_urn2 = "urn:li:monitor:2"
            cube_urn1 = build_metric_cube_urn(monitor_urn1)
            cube_urn2 = build_metric_cube_urn(monitor_urn2)

            # Cache some metric cube data
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "metricCubeUrn": [cube_urn1, cube_urn2],
                    "monitorUrn": [monitor_urn1, monitor_urn2],
                    "measure": [1.0, 2.0],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="dataHubMetricCubeEvent",
            )

            # Get info
            info = loader.get_endpoint_info("test.example.com")
            assert info is not None
            assert info["hostname"] == "test.example.com"
            assert info["cache_size_mb"] >= 0

    def test_get_endpoint_info_nonexistent(self):
        """Test getting info for non-existent endpoint."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))
            info = loader.get_endpoint_info("nonexistent.example.com")
            assert info is None

    def test_get_cached_monitors(self):
        """Test listing cached monitors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Cache anomaly event data with monitors
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000, 4000],
                    "monitorUrn": [
                        "urn:li:monitor:1",
                        "urn:li:monitor:1",
                        "urn:li:monitor:2",
                        "urn:li:monitor:2",
                    ],
                    "state": ["CONFIRMED"] * 4,
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com", parquet_path, aspect_name="monitorAnomalyEvent"
            )

            # Get monitors
            monitors = loader.get_cached_monitors(
                "test.example.com", aspect_name="monitorAnomalyEvent"
            )
            assert len(monitors) == 2

    def test_clear_cache_specific_endpoint(self):
        """Test clearing cache for a specific endpoint."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            from scripts.streamlit_explorer.common.metric_cube_extractor import (
                build_metric_cube_urn,
            )

            monitor_urn = "urn:li:monitor:test"
            cube_urn = build_metric_cube_urn(monitor_urn)

            # Cache data for two endpoints
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "metricCubeUrn": [cube_urn],
                    "monitorUrn": [monitor_urn],
                    "measure": [1.0],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)

            loader.cache.sync_from_parquet(
                "endpoint1.example.com",
                parquet_path,
                aspect_name="dataHubMetricCubeEvent",
            )
            loader.cache.sync_from_parquet(
                "endpoint2.example.com",
                parquet_path,
                aspect_name="dataHubMetricCubeEvent",
            )

            # Clear only one
            loader.clear_cache("endpoint1.example.com")

            # Verify first is cleared
            assert (
                loader.load_cached_events(
                    "endpoint1.example.com", aspect_name="dataHubMetricCubeEvent"
                )
                is None
            )
            # Second should still exist
            loaded = loader.load_cached_events(
                "endpoint2.example.com", aspect_name="dataHubMetricCubeEvent"
            )
            assert loaded is not None


class TestDiscoverParquetFiles:
    """Tests for parquet file discovery."""

    def test_discover_local_parquet_files(self):
        """Test discovering parquet files in a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Create test files
            for i in range(3):
                df = pd.DataFrame({"value": [i]})
                df.to_parquet(Path(tmpdir) / f"file{i}.parquet")

            # Also create a non-parquet file
            (Path(tmpdir) / "other.txt").write_text("not parquet")

            # Discover
            files = loader.discover_local_parquet_files(tmpdir)
            assert len(files) == 3

            for f in files:
                assert f["path"].endswith(".parquet")
                assert f["size_mb"] > 0
                assert "modified" in f

    def test_discover_local_parquet_files_empty_dir(self):
        """Test discovering in empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            files = loader.discover_local_parquet_files(tmpdir)
            assert files == []

    def test_discover_local_parquet_files_nonexistent_dir(self):
        """Test discovering in non-existent directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            files = loader.discover_local_parquet_files("/nonexistent/path")
            assert files == []


class TestGetDataLoader:
    """Tests for the convenience function."""

    def test_get_data_loader_default(self):
        """Test getting a DataLoader with defaults."""
        loader = get_data_loader()
        assert isinstance(loader, DataLoader)

    def test_get_data_loader_custom_dir(self):
        """Test getting a DataLoader with custom directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = get_data_loader(Path(tmpdir))
            assert loader.cache_dir == Path(tmpdir)


class TestPaginatedMethods:
    """Tests for paginated data loading methods."""

    def test_get_cached_monitors_paginated_basic(self):
        """Test basic paginated monitor loading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "monitorUrn": [
                        "urn:li:monitor:1",
                        "urn:li:monitor:2",
                        "urn:li:monitor:3",
                    ],
                    "assertionUrn": ["urn:li:assertion:a"] * 3,
                    "state": ["CONFIRMED"] * 3,
                }
            )
            parquet_path = Path(tmpdir) / "monitors.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            monitors, total = loader.get_cached_monitors_paginated(
                "test.example.com",
                page=0,
                page_size=2,
                aspect_name="monitorAnomalyEvent",
            )

            assert total == 3
            assert len(monitors) == 2
            # Verify we get MonitorMetadata objects
            assert hasattr(monitors[0], "monitor_urn")
            assert hasattr(monitors[0], "point_count")

    def test_get_cached_monitors_paginated_with_search(self):
        """Test paginated monitor loading with search filter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "monitorUrn": [
                        "urn:li:monitor:findme",
                        "urn:li:monitor:other",
                        "urn:li:monitor:another_findme",
                    ],
                    "assertionUrn": ["urn:li:assertion:a"] * 3,
                    "state": ["CONFIRMED"] * 3,
                }
            )
            parquet_path = Path(tmpdir) / "monitors.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            monitors, total = loader.get_cached_monitors_paginated(
                "test.example.com",
                page=0,
                page_size=100,
                search_filter="findme",
                aspect_name="monitorAnomalyEvent",
            )

            assert total == 2
            assert len(monitors) == 2


class TestGetConfirmedAnomaliesAsExclusions:
    """Tests for anomaly-based exclusions using correct timestamps."""

    def test_uses_source_event_timestamp_for_exclusions(self):
        """Test that source_sourceEventTimestampMillis is used for exclusion ranges.

        The exclusion should be at the frequency-aligned run time (source event),
        not at the anomaly event creation time.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Create anomaly events where:
            # - timestampMillis (anomaly creation): 5000ms
            # - source_sourceEventTimestampMillis (actual data point): 1000ms
            # The exclusion should be at 1000ms, not 5000ms
            df = pd.DataFrame(
                {
                    "timestampMillis": [5000],
                    "source_sourceEventTimestampMillis": [1000],
                    "monitorUrn": ["urn:li:monitor:test"],
                    "assertionUrn": ["urn:li:assertion:test"],
                    "state": ["CONFIRMED"],
                }
            )
            parquet_path = Path(tmpdir) / "anomalies.parquet"
            df.to_parquet(parquet_path)
            # Use cache manager directly to specify aspect_name
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            # Get exclusions (without local edits to avoid edit tracker complexity)
            exclusions = loader.get_confirmed_anomalies_as_exclusions(
                "test.example.com",
                use_local_edits=False,
            )

            # Should have 1 exclusion
            assert len(exclusions) == 1

            # Exclusion should be at source event time (1000ms),
            # not anomaly creation time (5000ms).
            # Use pd.to_datetime for UTC consistency (matches the implementation)
            expected_time = pd.to_datetime(1000, unit="ms").to_pydatetime()
            assert exclusions[0].start == expected_time

    def test_falls_back_to_timestamp_millis_when_source_missing(self):
        """Test fallback to timestampMillis when source timestamp is unavailable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Create anomaly event without source_sourceEventTimestampMillis
            df = pd.DataFrame(
                {
                    "timestampMillis": [3000],
                    "monitorUrn": ["urn:li:monitor:test"],
                    "assertionUrn": ["urn:li:assertion:test"],
                    "state": ["CONFIRMED"],
                }
            )
            parquet_path = Path(tmpdir) / "anomalies.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            exclusions = loader.get_confirmed_anomalies_as_exclusions(
                "test.example.com",
                use_local_edits=False,
            )

            assert len(exclusions) == 1

            # Should fall back to timestampMillis (3000ms)
            # Use pd.to_datetime for UTC consistency (matches the implementation)
            expected_time = pd.to_datetime(3000, unit="ms").to_pydatetime()
            assert exclusions[0].start == expected_time

    def test_only_confirmed_anomalies_included(self):
        """Test that only CONFIRMED state anomalies are included."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000],
                    "source_sourceEventTimestampMillis": [1000, 2000, 3000],
                    "monitorUrn": ["urn:li:monitor:test"] * 3,
                    "assertionUrn": ["urn:li:assertion:test"] * 3,
                    "state": ["CONFIRMED", "REJECTED", None],
                }
            )
            parquet_path = Path(tmpdir) / "anomalies.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            exclusions = loader.get_confirmed_anomalies_as_exclusions(
                "test.example.com",
                use_local_edits=False,
            )

            # Only the CONFIRMED anomaly should be included
            assert len(exclusions) == 1

    def test_window_minutes_creates_range(self):
        """Test that window_minutes parameter creates a time range around the point."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            df = pd.DataFrame(
                {
                    "timestampMillis": [60000],  # 1 minute in ms
                    "source_sourceEventTimestampMillis": [60000],
                    "monitorUrn": ["urn:li:monitor:test"],
                    "assertionUrn": ["urn:li:assertion:test"],
                    "state": ["CONFIRMED"],
                }
            )
            parquet_path = Path(tmpdir) / "anomalies.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            # Get exclusions with 5-minute window
            exclusions = loader.get_confirmed_anomalies_as_exclusions(
                "test.example.com",
                use_local_edits=False,
                window_minutes=5,
            )

            assert len(exclusions) == 1

            from datetime import timedelta

            # Use pd.to_datetime for UTC consistency (matches the implementation)
            center_time = pd.to_datetime(60000, unit="ms").to_pydatetime()
            expected_start = center_time - timedelta(minutes=5)
            expected_end = center_time + timedelta(minutes=5)

            assert exclusions[0].start == expected_start
            assert exclusions[0].end == expected_end

    def test_no_exclusions_when_no_anomalies(self):
        """Test that empty list is returned when no anomaly events exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            exclusions = loader.get_confirmed_anomalies_as_exclusions(
                "nonexistent.example.com",
                use_local_edits=False,
            )

            assert exclusions == []

    def test_entity_urn_filter(self):
        """Test filtering by entity URN (source_sourceUrn column)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Anomaly events use source_sourceUrn to link to the assertion
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000],
                    "source_sourceEventTimestampMillis": [1000, 2000],
                    "monitorUrn": ["urn:li:monitor:test"] * 2,
                    "source_sourceUrn": ["urn:li:assertion:A", "urn:li:assertion:B"],
                    "state": ["CONFIRMED", "CONFIRMED"],
                }
            )
            parquet_path = Path(tmpdir) / "anomalies.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            # Filter to only assertion A
            exclusions = loader.get_confirmed_anomalies_as_exclusions(
                "test.example.com",
                entity_urn="urn:li:assertion:A",
                use_local_edits=False,
            )

            assert len(exclusions) == 1


class TestMonitorCacheManagement:
    """Tests for monitor-specific cache management operations."""

    def test_delete_monitor_cache(self):
        """Test deleting a specific monitor's cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Cache anomaly event data with multiple monitors
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000, 2000, 3000, 4000],
                    "monitorUrn": [
                        "urn:li:monitor:1",
                        "urn:li:monitor:1",
                        "urn:li:monitor:2",
                        "urn:li:monitor:3",
                    ],
                    "state": ["CONFIRMED"] * 4,
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com", parquet_path, aspect_name="monitorAnomalyEvent"
            )

            # Delete one monitor's cache
            deleted = loader.delete_monitor_cache(
                "test.example.com",
                "urn:li:monitor:1",
                aspect_name="monitorAnomalyEvent",
            )

            assert deleted == 2  # Two events for monitor:1

            # Verify monitor:1 is gone but others remain
            loaded = loader.load_cached_events(
                "test.example.com", aspect_name="monitorAnomalyEvent"
            )
            assert loaded is not None
            assert len(loaded) == 2
            assert "urn:li:monitor:1" not in loaded["monitorUrn"].values
            assert "urn:li:monitor:2" in loaded["monitorUrn"].values
            assert "urn:li:monitor:3" in loaded["monitorUrn"].values

    def test_delete_monitor_cache_nonexistent_monitor(self):
        """Test deleting a non-existent monitor returns 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Cache some data
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000],
                    "monitorUrn": ["urn:li:monitor:1"],
                    "state": ["CONFIRMED"],
                }
            )
            parquet_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com", parquet_path, aspect_name="monitorAnomalyEvent"
            )

            # Try to delete non-existent monitor
            deleted = loader.delete_monitor_cache(
                "test.example.com",
                "urn:li:monitor:nonexistent",
                aspect_name="monitorAnomalyEvent",
            )

            assert deleted == 0

            # Verify original data unchanged
            loaded = loader.load_cached_events(
                "test.example.com", aspect_name="monitorAnomalyEvent"
            )
            assert loaded is not None
            assert len(loaded) == 1

    def test_delete_monitor_cache_nonexistent_endpoint(self):
        """Test deleting from non-existent endpoint returns 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Try to delete from endpoint with no data
            deleted = loader.delete_monitor_cache(
                "nonexistent.example.com",
                "urn:li:monitor:1",
                aspect_name="monitorAnomalyEvent",
            )

            assert deleted == 0


class TestGetConfirmedAnomalyTimestamps:
    """Tests for get_confirmed_anomaly_timestamps method."""

    def test_returns_empty_list_when_no_events(self):
        """Test returns empty list when no anomaly events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            result = loader.get_confirmed_anomaly_timestamps(
                "test.example.com",
                entity_urn=None,
            )

            assert result == []

    def test_returns_timestamps_for_confirmed_anomalies(self):
        """Test returns timestamps for confirmed anomalies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Cache anomaly events
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000000, 2000000, 3000000],
                    "monitorUrn": ["urn:li:monitor:1"] * 3,
                    "state": ["CONFIRMED", "PENDING", "CONFIRMED"],
                    "source_sourceEventTimestampMillis": [1500000, 2500000, 3500000],
                }
            )
            parquet_path = Path(tmpdir) / "anomalies.parquet"
            df.to_parquet(parquet_path)
            # Use cache manager directly to specify aspect_name
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            result = loader.get_confirmed_anomaly_timestamps(
                "test.example.com",
                entity_urn=None,
                use_local_edits=False,  # Disable local edits for test
            )

            # Should return source timestamps for confirmed anomalies (not PENDING)
            assert len(result) == 2
            assert 1500000 in result
            assert 3500000 in result
            assert 2500000 not in result  # PENDING state

    def test_uses_anomaly_timestamp_when_source_missing(self):
        """Test uses anomaly timestamp when source timestamp is missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Cache anomaly event without source timestamp
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000000],
                    "monitorUrn": ["urn:li:monitor:1"],
                    "state": ["CONFIRMED"],
                    # No source_sourceEventTimestampMillis
                }
            )
            parquet_path = Path(tmpdir) / "anomalies.parquet"
            df.to_parquet(parquet_path)
            # Use cache manager directly to specify aspect_name
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            result = loader.get_confirmed_anomaly_timestamps(
                "test.example.com",
                entity_urn=None,
                use_local_edits=False,
            )

            # Should use the anomaly timestamp since source is missing
            assert result == [1000000]

    def test_filters_by_entity_urn(self):
        """Test filters anomalies by entity URN."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Cache anomaly events for different entities
            df = pd.DataFrame(
                {
                    "timestampMillis": [1000000, 2000000],
                    "monitorUrn": ["urn:li:monitor:1", "urn:li:monitor:2"],
                    "source_sourceUrn": ["urn:li:assertion:1", "urn:li:assertion:2"],
                    "state": ["CONFIRMED", "CONFIRMED"],
                    "source_sourceEventTimestampMillis": [1500000, 2500000],
                }
            )
            parquet_path = Path(tmpdir) / "anomalies.parquet"
            df.to_parquet(parquet_path)
            # Use cache manager directly to specify aspect_name
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="monitorAnomalyEvent",
            )

            result = loader.get_confirmed_anomaly_timestamps(
                "test.example.com",
                entity_urn="urn:li:assertion:1",
                use_local_edits=False,
            )

            # Should only return timestamp for assertion:1
            assert result == [1500000]


class TestMetricCubeLoading:
    """Tests for metric cube data loading methods."""

    def test_load_cached_metric_cube_events_empty(self):
        """Test loading metric cube events when none cached."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            result = loader.load_cached_metric_cube_events(
                "test.example.com",
                monitor_urn="urn:li:monitor:test",
            )

            assert result is None

    def test_load_cached_metric_cube_events_success(self):
        """Test loading cached metric cube events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Cache metric cube events
            from scripts.streamlit_explorer.common.metric_cube_extractor import (
                build_metric_cube_urn,
            )

            monitor_urn = "urn:li:monitor:test"
            cube_urn = build_metric_cube_urn(monitor_urn)

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000000, 2000000, 3000000],
                    "metricCubeUrn": [cube_urn] * 3,
                    "monitorUrn": [monitor_urn] * 3,
                    "measure": [10.0, 20.0, 30.0],
                }
            )
            parquet_path = Path(tmpdir) / "metric_cube.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="dataHubMetricCubeEvent",
            )

            result = loader.load_cached_metric_cube_events(
                "test.example.com",
                monitor_urn=monitor_urn,
            )

            assert result is not None
            assert len(result) == 3

    def test_load_cached_metric_cube_timeseries(self):
        """Test loading metric cube timeseries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Cache metric cube events
            from scripts.streamlit_explorer.common.metric_cube_extractor import (
                build_metric_cube_urn,
            )

            monitor_urn = "urn:li:monitor:test"
            cube_urn = build_metric_cube_urn(monitor_urn)

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000000, 2000000, 3000000],
                    "metricCubeUrn": [cube_urn] * 3,
                    "monitorUrn": [monitor_urn] * 3,
                    "measure": [10.0, 20.0, 30.0],
                }
            )
            parquet_path = Path(tmpdir) / "metric_cube.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="dataHubMetricCubeEvent",
            )

            result = loader.load_cached_metric_cube_timeseries(
                "test.example.com",
                monitor_urn=monitor_urn,
            )

            assert len(result) == 3
            assert "ds" in result.columns
            assert "y" in result.columns
            assert list(result["y"]) == [10.0, 20.0, 30.0]

    def test_get_cached_metric_cubes_empty(self):
        """Test listing metric cubes when none cached."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            result = loader.get_cached_metric_cubes("test.example.com")

            assert result == []

    def test_get_cached_metric_cubes_success(self):
        """Test listing cached metric cubes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = DataLoader(cache_dir=Path(tmpdir))

            # Cache metric cube events
            from scripts.streamlit_explorer.common.metric_cube_extractor import (
                build_metric_cube_urn,
            )

            monitor_urn = "urn:li:monitor:test"
            cube_urn = build_metric_cube_urn(monitor_urn)

            df = pd.DataFrame(
                {
                    "timestampMillis": [1000000, 2000000, 3000000],
                    "metricCubeUrn": [cube_urn] * 3,
                    "monitorUrn": [monitor_urn] * 3,
                    "measure": [10.0, 20.0, 30.0],
                }
            )
            parquet_path = Path(tmpdir) / "metric_cube.parquet"
            df.to_parquet(parquet_path)
            loader.cache.sync_from_parquet(
                "test.example.com",
                parquet_path,
                aspect_name="dataHubMetricCubeEvent",
            )

            result = loader.get_cached_metric_cubes("test.example.com")

            assert len(result) == 1
            assert result[0].point_count == 3
