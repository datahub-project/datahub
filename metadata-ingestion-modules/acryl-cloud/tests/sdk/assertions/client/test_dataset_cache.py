"""Tests for dataset existence cache functionality."""

from unittest.mock import MagicMock, patch

import pytest

from acryl_datahub_cloud.sdk.assertions_client import AssertionsClient
from acryl_datahub_cloud.sdk.errors import SDKUsageError


class TestAssertionsClientCache:
    """Test that AssertionsClient uses functools.lru_cache correctly for dataset existence checks."""

    def test_check_dataset_exists_uses_cache(self) -> None:
        """Test that _check_dataset_exists uses the cache to avoid redundant calls."""
        # Create a mock client with a mock graph
        mock_client = MagicMock()
        mock_graph = MagicMock()
        mock_client._graph = mock_graph

        # Configure the mock to return True for exists
        mock_graph.exists.return_value = True

        # Mock time.time to avoid flaky tests at minute boundaries
        with patch("time.time", return_value=30.0):
            assertions_client = AssertionsClient(mock_client)
            urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,table,PROD)"

            # First call should hit the backend
            assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 1

            # Second call should use the cache
            assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 1  # Still 1, not 2

            # Third call should also use the cache
            assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 1  # Still 1, not 3

    def test_check_dataset_exists_caches_nonexistent_datasets(self) -> None:
        """Test that _check_dataset_exists caches non-existent datasets too."""
        # Create a mock client with a mock graph
        mock_client = MagicMock()
        mock_graph = MagicMock()
        mock_client._graph = mock_graph

        # Configure the mock to return False for exists
        mock_graph.exists.return_value = False

        # Mock time.time to avoid flaky tests at minute boundaries
        with patch("time.time", return_value=30.0):
            assertions_client = AssertionsClient(mock_client)
            urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,nonexistent,PROD)"

            # First call should hit the backend and raise an error
            with pytest.raises(SDKUsageError, match="does not exist"):
                assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 1

            # Second call should use the cache and still raise an error
            with pytest.raises(SDKUsageError, match="does not exist"):
                assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 1  # Still 1, not 2

    def test_check_dataset_exists_with_different_urns(self) -> None:
        """Test that cache works correctly with different URNs."""
        # Create a mock client with a mock graph
        mock_client = MagicMock()
        mock_graph = MagicMock()
        mock_client._graph = mock_graph

        # Configure the mock to return True for exists
        mock_graph.exists.return_value = True

        # Mock time.time to avoid flaky tests at minute boundaries
        with patch("time.time", return_value=30.0):
            assertions_client = AssertionsClient(mock_client)
            urn1 = "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
            urn2 = "urn:li:dataset:(urn:li:dataPlatform:snowflake,table2,PROD)"

            # Check first URN
            assertions_client._check_dataset_exists(urn1, skip_check=False)
            assert mock_graph.exists.call_count == 1

            # Check second URN (different, so should hit backend)
            assertions_client._check_dataset_exists(urn2, skip_check=False)
            assert mock_graph.exists.call_count == 2

            # Check first URN again (should use cache)
            assertions_client._check_dataset_exists(urn1, skip_check=False)
            assert mock_graph.exists.call_count == 2

            # Check second URN again (should use cache)
            assertions_client._check_dataset_exists(urn2, skip_check=False)
            assert mock_graph.exists.call_count == 2

    def test_check_dataset_exists_skips_cache_when_skip_check_true(self) -> None:
        """Test that skip_check=True bypasses both cache and backend check."""
        # Create a mock client with a mock graph
        mock_client = MagicMock()
        mock_graph = MagicMock()
        mock_client._graph = mock_graph

        # Configure the mock to return False (but it shouldn't be called)
        mock_graph.exists.return_value = False

        assertions_client = AssertionsClient(mock_client)
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,table,PROD)"

        # With skip_check=True, should not call backend and not raise
        assertions_client._check_dataset_exists(urn, skip_check=True)
        assert mock_graph.exists.call_count == 0

    def test_cache_expires_after_ttl(self) -> None:
        """Test that cache entries expire after ~60 seconds due to time bucketing."""
        # Create a mock client with a mock graph
        mock_client = MagicMock()
        mock_graph = MagicMock()
        mock_client._graph = mock_graph

        # Configure the mock to return True for exists
        mock_graph.exists.return_value = True

        with patch("time.time") as mock_time:
            # Start at time 0 (bucket 0)
            mock_time.return_value = 0.0

            assertions_client = AssertionsClient(mock_client)
            urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,table,PROD)"

            # First call hits backend (time bucket 0)
            assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 1

            # Call at time 30 seconds - same bucket (0), uses cache
            mock_time.return_value = 30.0
            assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 1

            # Call at time 59 seconds - still same bucket (0), uses cache
            mock_time.return_value = 59.0
            assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 1

            # Call at time 60 seconds - new bucket (1), hits backend again
            mock_time.return_value = 60.0
            assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 2

            # Call at time 90 seconds - same bucket (1), uses cache
            mock_time.return_value = 90.0
            assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 2

            # Call at time 120 seconds - new bucket (2), hits backend again
            mock_time.return_value = 120.0
            assertions_client._check_dataset_exists(urn, skip_check=False)
            assert mock_graph.exists.call_count == 3
