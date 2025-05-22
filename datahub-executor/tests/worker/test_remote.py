import unittest
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from acryl.executor.request.execution_request import ExecutionRequest

# Import the module we're testing
import datahub_executor.worker.remote as remote


class TestApplyRemoteMonitorTraining(unittest.TestCase):
    def setUp(self) -> None:
        # Create a sample ExecutionRequest for monitor training
        self.executor_id = "test-executor"
        self.sample_exec_id = "exec-1234"
        self.monitor_urn = "urn:li:assertion:monitor-1234"

        # Create a sample monitor argument
        self.monitor_args: Dict[str, Any] = {
            "urn": self.monitor_urn,
            "monitor": {
                "urn": self.monitor_urn,
                "type": "METADATA_VALUE_ASSERTION",
                "properties": {"field": "test_field"},
            },
        }

        # Create a sample execution request
        self.execution_request = ExecutionRequest(
            name="Test Execution Request",
            exec_id=self.sample_exec_id,
            executor_id="monitor_training_task",
            args=self.monitor_args,
        )

    @patch("datahub_executor.worker.remote.logger")
    @patch("datahub_executor.worker.remote.update_celery_credentials")
    @patch("datahub_executor.worker.remote.monitor_training_request.apply_async")
    def test_apply_remote_monitor_training_request_success(
        self,
        mock_apply_async: MagicMock,
        mock_update_credentials: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Test successful monitor training request submission."""
        # Configure mocks
        mock_update_credentials.return_value = True

        # Call the function under test
        with patch(
            "datahub_executor.worker.remote.DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION",
            "default",
        ):
            result = remote.apply_remote_monitor_training_request(
                self.execution_request, self.executor_id
            )

        # Assert the results
        self.assertEqual(result, self.sample_exec_id)
        mock_logger.info.assert_called_once()
        mock_update_credentials.assert_called_once_with(
            remote.app, False, self.executor_id
        )

        # Verify apply_async was called with the correct arguments
        mock_apply_async.assert_called_once_with(
            args=[self.execution_request],
            task_id=self.sample_exec_id,
            queue=self.executor_id,
            routing_key=f"{self.executor_id}.monitor_training",
        )

    @patch("datahub_executor.worker.remote.CLI_EXECUTOR_ID", "cli-executor")
    def test_apply_remote_monitor_training_cli_executor(self) -> None:
        """Test with CLI executor ID returns None."""
        # Call the function with CLI_EXECUTOR_ID
        result = remote.apply_remote_monitor_training_request(
            self.execution_request, "cli-executor"
        )

        # Should return None for CLI executor
        self.assertIsNone(result)

    @patch("datahub_executor.worker.remote.logger")
    @patch("datahub_executor.worker.remote.update_celery_credentials")
    @patch("datahub_executor.worker.remote.monitor_training_request.apply_async")
    def test_apply_remote_monitor_training_invalid_credentials(
        self,
        mock_apply_async: MagicMock,
        mock_update_credentials: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Test when credentials update fails."""
        # Configure mocks
        mock_update_credentials.return_value = False

        # Call the function under test
        with patch(
            "datahub_executor.worker.remote.DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION",
            "default",
        ):
            result = remote.apply_remote_monitor_training_request(
                self.execution_request, self.executor_id
            )

        # Should return the URN when credentials fail
        self.assertEqual(result, self.monitor_urn)

        # Verify method calls
        mock_logger.info.assert_called_once()
        mock_update_credentials.assert_called_once()
        mock_apply_async.assert_not_called()

    @patch("datahub_executor.worker.remote.logger")
    @patch("datahub_executor.worker.remote.METRIC")
    @patch("datahub_executor.worker.remote.update_celery_credentials")
    @patch("datahub_executor.worker.remote.monitor_training_request.apply_async")
    def test_apply_remote_monitor_training_message_too_large(
        self,
        mock_apply_async: MagicMock,
        mock_update_credentials: MagicMock,
        mock_metric: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Test when message is too large for SQS."""
        # Configure mocks
        mock_update_credentials.return_value = True
        mock_metric_instance = MagicMock()
        mock_metric.return_value = mock_metric_instance

        # Call the function with message size check
        with patch(
            "datahub_executor.worker.remote.DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION",
            "default",
        ):
            with patch(
                "datahub_executor.worker.remote.DATAHUB_EXECUTOR_SQS_MESSAGE_MAX_LENGTH",
                10,
            ):
                result = remote.apply_remote_monitor_training_request(
                    self.execution_request, self.executor_id
                )

        # Should return the monitor URN
        self.assertEqual(result, self.monitor_urn)

        # Verify method calls
        mock_logger.info.assert_called_once()
        mock_update_credentials.assert_called_once()
        mock_metric.assert_called_once_with("SCHEDULER_MESSAGE_SIZE_EXCEEDED")
        mock_metric_instance.inc.assert_called_once()
        mock_apply_async.assert_not_called()

    @patch("datahub_executor.worker.remote.logger")
    def test_apply_remote_monitor_training_unsupported_implementation(
        self, mock_logger: MagicMock
    ) -> None:
        """Test with unsupported worker implementation."""
        # Call the function with unsupported implementation
        with patch(
            "datahub_executor.worker.remote.DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION",
            "unsupported",
        ):
            result = remote.apply_remote_monitor_training_request(
                self.execution_request, self.executor_id
            )

        # Should return None for unsupported implementation
        self.assertIsNone(result)

        # Only error log should be called
        mock_logger.error.assert_called_once()


if __name__ == "__main__":
    unittest.main()
