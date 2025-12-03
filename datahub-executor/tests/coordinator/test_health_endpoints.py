import time
from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

# Patch create_datahub_graph at module level BEFORE imports to prevent hanging during test collection.
# The import chain for health_endpoints triggers celery_sqs/app.py which calls update_celery_credentials
# at module level. We need to ensure get_executor_configs returns an empty list to prevent TypeError.
_patch_create_graph = patch(
    "datahub_executor.common.client.config.resolver.create_datahub_graph"
)
_mock_create_graph = _patch_create_graph.start()
_mock_create_graph.return_value = Mock()

# Patch update_celery_credentials to avoid calling resolver during import
# This prevents TypeError when update_celery_credentials is called during import
_patch_update_credentials = patch(
    "datahub_executor.worker.celery_sqs.config.update_celery_credentials"
)
_mock_update_credentials = _patch_update_credentials.start()
_mock_update_credentials.return_value = True

from datahub.ingestion.graph.client import DataHubGraph  # noqa: E402

from datahub_executor.common.client.config.resolver import (  # noqa: E402
    ExecutorConfigResolver,
)
from datahub_executor.coordinator.health_endpoints import health  # noqa: E402

# Update mock to return proper spec
_mock_create_graph.return_value = Mock(spec=DataHubGraph)


class TestHealthEndpoints:
    """Test health endpoint integration with retry state"""

    @patch(
        "datahub_executor.coordinator.health_endpoints.datahub_executor.coordinator.helpers.manager"
    )
    @patch("datahub_executor.coordinator.health_endpoints._resolver")
    def test_health_returns_healthy_when_manager_alive(
        self, mock_resolver: Mock, mock_manager: Mock
    ) -> None:
        """Test that health endpoint returns healthy when manager is alive."""
        mock_manager.alive.return_value = True
        mock_resolver.is_actively_retrying.return_value = False

        result = health()

        assert result["healthy"] is True
        assert result["manager_alive"] is True
        assert result["actively_retrying"] is False

    @patch(
        "datahub_executor.coordinator.health_endpoints.datahub_executor.coordinator.helpers.manager"
    )
    @patch("datahub_executor.coordinator.health_endpoints._resolver")
    def test_health_returns_healthy_when_actively_retrying(
        self, mock_resolver: Mock, mock_manager: Mock
    ) -> None:
        """Test that health endpoint returns healthy when actively retrying."""
        mock_manager.alive.return_value = False
        mock_resolver.is_actively_retrying.return_value = True

        result = health()

        assert result["healthy"] is True
        assert result["manager_alive"] is False
        assert result["actively_retrying"] is True

    @patch(
        "datahub_executor.coordinator.health_endpoints.datahub_executor.coordinator.helpers.manager"
    )
    @patch("datahub_executor.coordinator.health_endpoints._resolver")
    def test_health_returns_healthy_when_manager_alive_and_retrying(
        self, mock_resolver: Mock, mock_manager: Mock
    ) -> None:
        """Test that health endpoint returns healthy when both manager is alive and retrying."""
        mock_manager.alive.return_value = True
        mock_resolver.is_actively_retrying.return_value = True

        result = health()

        assert result["healthy"] is True
        assert result["manager_alive"] is True
        assert result["actively_retrying"] is True

    @patch(
        "datahub_executor.coordinator.health_endpoints.datahub_executor.coordinator.helpers.manager"
    )
    @patch("datahub_executor.coordinator.health_endpoints._resolver")
    def test_health_raises_exception_when_unhealthy(
        self, mock_resolver: Mock, mock_manager: Mock
    ) -> None:
        """Test that health endpoint raises exception when service is unhealthy."""
        mock_manager.alive.return_value = False
        mock_resolver.is_actively_retrying.return_value = False

        with pytest.raises(HTTPException) as exc_info:
            health()

        assert exc_info.value.status_code == 500
        assert "Manager is not healthy" in str(exc_info.value.detail)

    @patch(
        "datahub_executor.coordinator.health_endpoints.datahub_executor.coordinator.helpers.manager"
    )
    @patch("datahub_executor.coordinator.health_endpoints._resolver")
    def test_health_handles_none_manager(
        self, mock_resolver: Mock, mock_manager: Mock
    ) -> None:
        """Test that health endpoint handles None manager gracefully."""
        mock_manager_instance = None  # type: ignore[assignment]
        with patch(
            "datahub_executor.coordinator.health_endpoints.datahub_executor.coordinator.helpers.manager",
            mock_manager_instance,
        ):
            mock_resolver.is_actively_retrying.return_value = True

            result = health()

            assert result["healthy"] is True
            assert result["manager_alive"] is False
            assert result["actively_retrying"] is True

    @patch(
        "datahub_executor.coordinator.health_endpoints.datahub_executor.coordinator.helpers.manager"
    )
    @patch("datahub_executor.coordinator.health_endpoints._resolver")
    def test_health_uses_correct_staleness_window(
        self, mock_resolver: Mock, mock_manager: Mock
    ) -> None:
        """Test that health endpoint uses correct staleness window (300 seconds)."""
        mock_manager.alive.return_value = False
        mock_resolver.is_actively_retrying.return_value = True

        health()

        # Verify is_actively_retrying was called with correct staleness window
        mock_resolver.is_actively_retrying.assert_called_once_with(
            max_staleness_seconds=300
        )

    @patch(
        "datahub_executor.coordinator.health_endpoints.datahub_executor.coordinator.helpers.manager"
    )
    @patch("datahub_executor.coordinator.health_endpoints._resolver")
    def test_health_response_structure(
        self, mock_resolver: Mock, mock_manager: Mock
    ) -> None:
        """Test that health endpoint returns correct response structure."""
        mock_manager.alive.return_value = True
        mock_resolver.is_actively_retrying.return_value = False

        result = health()

        # Verify response has all expected keys
        assert "healthy" in result
        assert "manager_alive" in result
        assert "actively_retrying" in result
        assert isinstance(result["healthy"], bool)
        assert isinstance(result["manager_alive"], bool)
        assert isinstance(result["actively_retrying"], bool)

    @patch(
        "datahub_executor.coordinator.health_endpoints.datahub_executor.coordinator.helpers.manager"
    )
    def test_health_integration_with_actual_retry_state(
        self, mock_manager: Mock
    ) -> None:
        """Test health endpoint integration with actual retry state."""
        mock_manager.alive.return_value = False
        # Create a real resolver instance with a mock graph for testing
        mock_graph = Mock(spec=DataHubGraph)
        resolver = ExecutorConfigResolver(mock_graph)

        # Patch the module-level resolver with our test resolver
        with patch("datahub_executor.coordinator.health_endpoints._resolver", resolver):
            # Reset retry state
            with resolver._retry_state_lock:
                resolver._retry_state["last_retry_attempt"] = None
                resolver._retry_state["is_retrying"] = False
                resolver._retry_state["current_exception_category"] = None

            # Initially unhealthy (no retries, manager down)
            with pytest.raises(HTTPException):
                health()

            # Simulate recent retry
            with resolver._retry_state_lock:
                resolver._retry_state["last_retry_attempt"] = (
                    time.time() - 60
                )  # 60 seconds ago
                resolver._retry_state["is_retrying"] = True

            # Should be healthy now (actively retrying)
            result = health()
            assert result["healthy"] is True
            assert result["actively_retrying"] is True

            # Simulate stale retry
            with resolver._retry_state_lock:
                resolver._retry_state["last_retry_attempt"] = (
                    time.time() - 400
                )  # 400 seconds ago (stale)

            # Should be unhealthy again (stale retry)
            with pytest.raises(HTTPException):
                health()
