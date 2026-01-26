from unittest.mock import MagicMock, patch

import pytest
from acryl.executor.request.execution_request import ExecutionRequest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.assertion.executor import AssertionExecutor
from datahub_executor.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionEvaluationSpec,
    AssertionResultErrorType,
    AssertionResultType,
    AssertionType,
    CronSchedule,
)


@pytest.fixture
def mock_graph() -> MagicMock:
    """Return a mock DataHubGraph instance."""
    return MagicMock(spec=DataHubGraph)


@pytest.fixture
def mock_engine() -> MagicMock:
    """Return a mock AssertionEngine instance."""
    return MagicMock(spec=AssertionEngine)


@pytest.fixture
def mock_thread_pool() -> MagicMock:
    """Return a mock ThreadPoolExecutorWithQueueSizeLimit instance."""
    return MagicMock(spec=ThreadPoolExecutorWithQueueSizeLimit)


@pytest.fixture
def test_assertion() -> Assertion:
    """Return a test Assertion instance."""
    return Assertion(
        urn="urn:li:assertion:test-assertion-123",
        entity=AssertionEntity(
            urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)",
            platform_urn="urn:li:dataPlatform:snowflake",
        ),
        type=AssertionType.FRESHNESS,
    )


@pytest.fixture
def test_schedule() -> CronSchedule:
    """Return a test CronSchedule instance."""
    return CronSchedule(cron="0 0 * * *", timezone="UTC")


@pytest.fixture
def test_parameters() -> AssertionEvaluationParameters:
    """Return a test AssertionEvaluationParameters instance."""
    return AssertionEvaluationParameters(
        type=AssertionEvaluationParametersType.DATASET_FRESHNESS
    )


@pytest.fixture
def test_assertion_spec(
    test_assertion: Assertion,
    test_schedule: CronSchedule,
    test_parameters: AssertionEvaluationParameters,
) -> AssertionEvaluationSpec:
    """Return a test AssertionEvaluationSpec instance with parameters."""
    return AssertionEvaluationSpec(
        assertion=test_assertion,
        schedule=test_schedule,
        parameters=test_parameters,
    )


@pytest.fixture
def test_assertion_spec_no_parameters(
    test_assertion: Assertion, test_schedule: CronSchedule
) -> AssertionEvaluationSpec:
    """Return a test AssertionEvaluationSpec instance without parameters."""
    return AssertionEvaluationSpec(
        assertion=test_assertion,
        schedule=test_schedule,
        parameters=None,
    )


@pytest.fixture
def test_execution_request(
    test_assertion_spec: AssertionEvaluationSpec,
) -> ExecutionRequest:
    """Return a test ExecutionRequest instance with valid assertion spec."""
    return ExecutionRequest(
        exec_id="test-exec-id",
        name="RUN_ASSERTION",
        args={"assertion_spec": test_assertion_spec.model_dump(by_alias=True)},
    )


@pytest.fixture
def test_execution_request_no_parameters(
    test_assertion_spec_no_parameters: AssertionEvaluationSpec,
) -> ExecutionRequest:
    """Return a test ExecutionRequest instance with assertion spec missing parameters."""
    return ExecutionRequest(
        exec_id="test-exec-id",
        name="RUN_ASSERTION",
        args={
            "assertion_spec": test_assertion_spec_no_parameters.model_dump(
                by_alias=True
            )
        },
    )


class TestAssertionExecutor:
    """Tests for the AssertionExecutor class."""

    @patch("datahub_executor.common.assertion.executor.create_datahub_graph")
    @patch("datahub_executor.common.assertion.executor.create_assertion_engine")
    @patch(
        "datahub_executor.common.assertion.executor.ThreadPoolExecutorWithQueueSizeLimit"
    )
    def test_init(
        self,
        mock_thread_pool_class: MagicMock,
        mock_create_engine: MagicMock,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        mock_engine: MagicMock,
        mock_thread_pool: MagicMock,
    ) -> None:
        """Test initialization of the AssertionExecutor class."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph
        mock_create_engine.return_value = mock_engine
        mock_thread_pool_class.return_value = mock_thread_pool

        # Create executor
        executor = AssertionExecutor()

        # Verify initialization
        mock_create_graph.assert_called_once()
        assert executor.graph == mock_graph

        # Verify engine creation
        mock_create_engine.assert_called_once_with(mock_graph)
        assert executor.engine == mock_engine

        # Verify thread pool creation
        mock_thread_pool_class.assert_called_once()
        assert executor.tp == mock_thread_pool

        # Verify initial state
        assert executor.stop is False

    @patch("datahub_executor.common.assertion.executor.create_datahub_graph")
    @patch("datahub_executor.common.assertion.executor.create_assertion_engine")
    @patch("datahub_executor.common.assertion.executor.METRIC")
    def test_evaluate_assertion_emits_error_when_parameters_none(
        self,
        mock_metric: MagicMock,
        mock_create_engine: MagicMock,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        mock_engine: MagicMock,
        test_execution_request_no_parameters: ExecutionRequest,
    ) -> None:
        """Test that evaluate_assertion emits error when parameters is None."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph
        mock_create_engine.return_value = mock_engine
        mock_metric.return_value.time.return_value = MagicMock(
            __enter__=MagicMock(), __exit__=MagicMock()
        )
        mock_handler = MagicMock()
        mock_engine.result_handlers = [mock_handler]

        # Create executor
        executor = AssertionExecutor()
        executor.engine = mock_engine

        executor.evaluate_assertion(test_execution_request_no_parameters)

        # Verify engine.evaluate was not called
        mock_engine.evaluate.assert_not_called()
        mock_handler.handle.assert_called_once()
        result_arg = mock_handler.handle.call_args[0][2]
        assert isinstance(result_arg, AssertionEvaluationResult)
        assert result_arg.type == AssertionResultType.ERROR
        assert result_arg.error is not None
        assert (
            result_arg.error.type
            == AssertionResultErrorType.MISSING_EVALUATION_PARAMETERS
        )

    @patch("datahub_executor.common.assertion.executor.create_datahub_graph")
    @patch("datahub_executor.common.assertion.executor.create_assertion_engine")
    @patch("datahub_executor.common.assertion.executor.METRIC")
    def test_evaluate_assertion_calls_engine_when_parameters_present(
        self,
        mock_metric: MagicMock,
        mock_create_engine: MagicMock,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        mock_engine: MagicMock,
        test_execution_request: ExecutionRequest,
    ) -> None:
        """Test that evaluate_assertion calls engine.evaluate when parameters are present."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph
        mock_create_engine.return_value = mock_engine
        mock_metric.return_value.time.return_value = MagicMock(
            __enter__=MagicMock(), __exit__=MagicMock()
        )

        # Create executor
        executor = AssertionExecutor()
        executor.engine = mock_engine

        # Call evaluate_assertion
        executor.evaluate_assertion(test_execution_request)

        # Verify engine.evaluate was called
        mock_engine.evaluate.assert_called_once()

        # Verify the assertion URN is correct
        call_args = mock_engine.evaluate.call_args
        called_assertion = call_args[0][0]
        assert called_assertion.urn == "urn:li:assertion:test-assertion-123"

    @patch("datahub_executor.common.assertion.executor.create_datahub_graph")
    @patch("datahub_executor.common.assertion.executor.create_assertion_engine")
    def test_worker_handles_missing_parameters(
        self,
        mock_create_engine: MagicMock,
        mock_create_graph: MagicMock,
        mock_graph: MagicMock,
        mock_engine: MagicMock,
        test_execution_request_no_parameters: ExecutionRequest,
    ) -> None:
        """Test that worker handles missing parameters without propagating."""
        # Setup mocks
        mock_create_graph.return_value = mock_graph
        mock_create_engine.return_value = mock_engine
        mock_handler = MagicMock()
        mock_engine.result_handlers = [mock_handler]

        # Create executor
        executor = AssertionExecutor()
        executor.engine = mock_engine

        # Call worker - should not raise an exception (it catches and logs)
        executor.worker(test_execution_request_no_parameters)

        # Verify engine.evaluate was not called (error happened before)
        mock_engine.evaluate.assert_not_called()
        mock_handler.handle.assert_called_once()
