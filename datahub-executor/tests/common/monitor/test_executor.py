from unittest.mock import MagicMock, patch

import pytest
from acryl.executor.request.execution_request import ExecutionRequest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.constants import RUN_MONITOR_TRAINING_TASK_NAME
from datahub_executor.common.monitor.executor import MonitorExecutor
from datahub_executor.common.monitor.inference.monitor_training_engine import (
    MonitorTrainingEngine,
)
from datahub_executor.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_executor.common.types import (
    AssertionMonitor,
    Monitor,
    MonitorMode,
    MonitorType,
)


@pytest.fixture
def mock_graph() -> MagicMock:
    """Return a mock DataHubGraph instance."""
    return MagicMock(spec=DataHubGraph)


@pytest.fixture
def mock_engine() -> MagicMock:
    """Return a mock MonitorTrainingEngine instance."""
    return MagicMock(spec=MonitorTrainingEngine)


@pytest.fixture
def mock_thread_pool() -> MagicMock:
    """Return a mock ThreadPoolExecutorWithQueueSizeLimit instance."""
    return MagicMock(spec=ThreadPoolExecutorWithQueueSizeLimit)


@pytest.fixture
def test_monitor() -> Monitor:
    """Return a test Monitor instance."""
    return Monitor(
        urn="urn:li:dataHubAssetMonitor:test-monitor",
        type=MonitorType.ASSERTION,
        assertion_monitor=AssertionMonitor(assertions=[], settings=None),
        mode=MonitorMode.ACTIVE,
        executorId=None,
    )


@pytest.fixture
def test_execution_request(test_monitor: Monitor) -> ExecutionRequest:
    """Return a test ExecutionRequest instance."""
    return ExecutionRequest(
        exec_id="test-exec-id",
        name=RUN_MONITOR_TRAINING_TASK_NAME,
        args={"monitor": test_monitor.dict()},
    )


class TestMonitorExecutor:
    """Tests for the MonitorExecutor class."""

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    @patch("datahub_executor.common.monitor.executor.create_monitor_training_engine")
    @patch(
        "datahub_executor.common.monitor.executor.ThreadPoolExecutorWithQueueSizeLimit"
    )
    def test_init(
        self,
        mock_thread_pool_class: MagicMock,
        mock_create_training_engine: MagicMock,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        mock_engine: MagicMock,
        mock_thread_pool: MagicMock,
    ) -> None:
        """Test initialization of the MonitorExecutor class."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph
        mock_create_training_engine.return_value = mock_engine
        mock_thread_pool_class.return_value = mock_thread_pool

        # Create executor
        executor = MonitorExecutor()

        # Verify initialization
        mock_create_graph.assert_called_once()
        assert executor.graph == mock_graph

        # Verify engine creation
        mock_create_training_engine.assert_called_once()
        assert executor.engine == mock_engine

        # Verify expected arguments to engine constructor
        args, kwargs = mock_create_training_engine.call_args
        assert args[0] == mock_graph

        # Verify thread pool creation
        mock_thread_pool_class.assert_called_once()
        assert executor.tp == mock_thread_pool

        # Verify initial state
        assert executor.stop is False

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    def test_get_active_thread_count(
        self, mock_create_graph: MagicMock, mock_graph: MagicMock
    ) -> None:
        """Test getting the active thread count."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph

        # Create executor with mocked thread pool
        executor = MonitorExecutor()
        executor.tp = MagicMock()
        executor.tp.get_active_thread_count.return_value = 5

        # Call method and verify
        result = executor.get_active_thread_count()
        assert result == 5
        executor.tp.get_active_thread_count.assert_called_once()

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    def test_execute_submits_to_thread_pool(
        self,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        test_execution_request: ExecutionRequest,
    ) -> None:
        """Test that execute submits the job to the thread pool."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph

        # Create executor with mocked thread pool
        executor = MonitorExecutor()
        executor.tp = MagicMock()

        # Call execute
        executor.execute(test_execution_request)

        # Verify thread pool submit was called with worker and request
        executor.tp.submit.assert_called_once_with(
            executor.worker, test_execution_request
        )

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    def test_execute_does_not_submit_when_stopped(
        self,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        test_execution_request: ExecutionRequest,
    ) -> None:
        """Test that execute does not submit when executor is stopped."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph

        # Create executor with mocked thread pool and set to stopped
        executor = MonitorExecutor()
        executor.tp = MagicMock()
        executor.stop = True

        # Call execute
        executor.execute(test_execution_request)

        # Verify thread pool submit was not called
        executor.tp.submit.assert_not_called()

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    def test_worker_calls_evaluate(
        self,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        test_execution_request: ExecutionRequest,
    ) -> None:
        """Test that worker calls evaluate_monitor_training."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph

        # Create executor with mocked evaluate_monitor_training
        executor = MonitorExecutor()
        executor.evaluate_monitor_training = MagicMock()  # type: ignore

        # Call worker
        executor.worker(test_execution_request)

        # Verify evaluate_monitor_training was called
        executor.evaluate_monitor_training.assert_called_once_with(
            test_execution_request
        )

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    def test_worker_handles_exceptions(
        self,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        test_execution_request: ExecutionRequest,
    ) -> None:
        """Test that worker handles exceptions without propagating them."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph

        # Create executor with mocked evaluate_monitor_training that raises an exception
        executor = MonitorExecutor()
        executor.evaluate_monitor_training = MagicMock(
            side_effect=Exception("Test error")
        )  # type: ignore

        # Call worker - should not raise an exception
        executor.worker(test_execution_request)

        # Verify evaluate_monitor_training was called
        executor.evaluate_monitor_training.assert_called_once_with(
            test_execution_request
        )

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    def test_worker_does_not_evaluate_when_stopped(
        self,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        test_execution_request: ExecutionRequest,
    ) -> None:
        """Test that worker does not evaluate when executor is stopped."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph

        # Create executor with mocked evaluate_monitor_training and set to stopped
        executor = MonitorExecutor()
        executor.evaluate_monitor_training = MagicMock()  # type: ignore
        executor.stop = True

        # Call worker
        executor.worker(test_execution_request)

        # Verify evaluate_monitor_training was not called
        executor.evaluate_monitor_training.assert_not_called()

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    def test_shutdown(
        self, mock_create_graph: MagicMock, mock_graph: MagicMock
    ) -> None:
        """Test shutting down the executor."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph

        # Create executor with mocked thread pool
        executor = MonitorExecutor()
        executor.tp = MagicMock()

        # Call shutdown
        executor.shutdown()

        # Verify executor is stopped and thread pool shutdown was called
        assert executor.stop is True
        executor.tp.shutdown.assert_called_once_with(True)

        # Test with wait=False
        executor.stop = False
        executor.tp.reset_mock()

        executor.shutdown(wait=False)

        assert executor.stop is True
        executor.tp.shutdown.assert_called_once_with(False)

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    @patch("datahub_executor.common.monitor.executor.METRIC")
    def test_evaluate_monitor_training_valid_request(
        self,
        mock_metric: MagicMock,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        test_execution_request: ExecutionRequest,
        test_monitor: Monitor,
    ) -> None:
        """Test evaluate_monitor_training with a valid request."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph
        mock_metric.return_value.time.return_value.__enter__ = MagicMock()
        mock_metric.return_value.time.return_value.__exit__ = MagicMock()

        # Create executor with mocked engine
        executor = MonitorExecutor()
        executor.engine = MagicMock()

        # Call evaluate_monitor_training
        executor.evaluate_monitor_training(test_execution_request)

        # Verify monitor was parsed and engine.train was called
        # Note: We can't compare the actual Monitor instances because they're different instances
        # but we can check that train was called and extract the arg to verify its URN
        executor.engine.train.assert_called_once()
        called_monitor = executor.engine.train.call_args[0][0]
        assert called_monitor.urn == test_monitor.urn
        assert called_monitor.type == test_monitor.type
        assert called_monitor.mode == test_monitor.mode

    @patch("datahub_executor.common.monitor.executor.create_datahub_graph")
    @patch("datahub_executor.common.monitor.executor.METRIC")
    def test_evaluate_monitor_training_invalid_task(
        self,
        mock_metric: MagicMock,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
    ) -> None:
        """Test evaluate_monitor_training with an invalid task name."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph
        mock_metric.return_value.time.return_value.__enter__ = MagicMock()
        mock_metric.return_value.time.return_value.__exit__ = MagicMock()

        # Create executor with mocked engine
        executor = MonitorExecutor()
        executor.engine = MagicMock()

        # Create a request with invalid task name
        invalid_request = ExecutionRequest(
            exec_id="test-exec-id",
            name="INVALID_TASK_NAME",
            args={},
        )

        # Call evaluate_monitor_training and verify it raises an exception
        with pytest.raises(Exception) as excinfo:
            executor.evaluate_monitor_training(invalid_request)

        assert "unrecognized task" in str(excinfo.value)
        assert "INVALID_TASK_NAME" in str(excinfo.value)

        # Verify engine.train was not called
        executor.engine.train.assert_not_called()
