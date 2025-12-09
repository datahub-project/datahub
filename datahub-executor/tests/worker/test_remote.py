import json
import unittest
from types import ModuleType
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.metadata.schema_classes import GenericAspectClass

from datahub_executor.common.types import CloudLoggingConfig

# Module-level patch to prevent network calls during import
_mock_update_credentials = None
remote: ModuleType


def setUpModule() -> None:
    """Set up module-level mocks before any tests run."""
    global _mock_update_credentials
    _mock_update_credentials = patch(
        "datahub_executor.worker.celery_sqs.config.update_celery_credentials"
    )
    _mock_update_credentials.start()

    # Import the module we're testing after patching
    global remote
    import datahub_executor.worker.remote as remote


def tearDownModule() -> None:
    """Clean up module-level mocks after all tests are done."""
    global _mock_update_credentials
    if _mock_update_credentials:
        _mock_update_credentials.stop()


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
        # Verify update_celery_credentials was called with resolver parameter
        mock_update_credentials.assert_called_once()
        call_args = mock_update_credentials.call_args
        self.assertEqual(call_args[0][0], remote.app)
        self.assertEqual(call_args[0][1], False)
        self.assertEqual(call_args[0][2], self.executor_id)
        # Verify resolver was passed as 4th argument
        self.assertEqual(len(call_args[0]), 4)
        self.assertIsNotNone(call_args[0][3])  # resolver should be passed

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


class TestAddCloudLoggingConfigs(unittest.TestCase):
    def test_add_cloud_logging_configs_none_cloud_logging_config(self) -> None:
        """Test when cloud_logging_config is None."""
        # Create a mock aspect
        aspect_value = {"args": {"extra_env_vars": "{}"}}
        aspect = GenericAspectClass(
            contentType=JSON_CONTENT_TYPE, value=json.dumps(aspect_value).encode()
        )

        original_value = aspect.value

        # Call function with None cloud_logging_config
        remote.add_cloud_logging_configs(aspect, None)

        # Aspect should remain unchanged
        self.assertEqual(aspect.value, original_value)

    def test_add_cloud_logging_configs_success(self) -> None:
        """Test successful addition of cloud logging configs."""
        # Create a valid cloud logging config
        cloud_config = CloudLoggingConfig(
            s3_bucket="test-bucket",
            s3_prefix="logs/prefix",
            remote_executor_logging_enabled=True,
        )

        # Create a mock aspect with existing env vars
        aspect_value = {
            "args": {"extra_env_vars": '{"EXISTING_VAR": "existing_value"}'}
        }
        aspect = GenericAspectClass(
            contentType=JSON_CONTENT_TYPE, value=json.dumps(aspect_value).encode()
        )

        # Call the function
        remote.add_cloud_logging_configs(aspect, cloud_config)

        # Check that env vars were added
        updated_aspect_value = json.loads(aspect.value.decode())
        updated_env_vars = json.loads(updated_aspect_value["args"]["extra_env_vars"])

        self.assertEqual(updated_env_vars["DATAHUB_CLOUD_LOG_BUCKET"], "test-bucket")
        self.assertEqual(updated_env_vars["DATAHUB_CLOUD_LOG_PATH"], "logs/prefix")
        self.assertEqual(updated_env_vars["EXISTING_VAR"], "existing_value")

    def test_add_cloud_logging_configs_error_case(self) -> None:
        """Test error case with invalid JSON in aspect."""
        # Create a valid cloud logging config
        cloud_config = CloudLoggingConfig(
            s3_bucket="test-bucket",
            s3_prefix="logs/prefix",
            remote_executor_logging_enabled=True,
        )

        # Create a mock aspect with invalid JSON
        aspect = GenericAspectClass(
            contentType=JSON_CONTENT_TYPE, value=b"invalid json"
        )

        # Call the function and expect it to raise an exception
        with self.assertRaises(json.JSONDecodeError):
            remote.add_cloud_logging_configs(aspect, cloud_config)


if __name__ == "__main__":
    unittest.main()
