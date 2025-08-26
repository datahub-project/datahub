from typing import Dict, cast
from unittest.mock import MagicMock, patch

import pytest
from datahub.metadata.schema_classes import (
    MonitorErrorTypeClass,
    MonitorStateClass,
)

from datahub_executor.common.assertion.engine.evaluator.evaluator import (
    AssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.utils.errors import (
    InsufficientDataException,
    SourceConnectionErrorException,
)
from datahub_executor.common.exceptions import (
    InvalidParametersException,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionResultErrorType,
    AssertionResultType,
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    AssertionValueChangeType,
)


class TestEvaluator(AssertionEvaluator):
    @property
    def type(self) -> AssertionType:
        return AssertionType.VOLUME

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
            datasetFieldParameters=None,
            datasetFreshnessParameters=None,
            datasetSchemaParameters=None,
            datasetVolumeParameters=None,
        )

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        # Simple implementation for testing
        return AssertionEvaluationResult(AssertionResultType.SUCCESS)


class TestAssertionEvaluator:
    @pytest.fixture
    def mock_dependencies(self) -> Dict[str, MagicMock]:
        """Set up mock dependencies for the evaluator"""
        connection_provider = MagicMock()
        state_provider = MagicMock()
        source_provider = MagicMock()
        monitor_client = MagicMock()

        return {
            "connection_provider": connection_provider,
            "state_provider": state_provider,
            "source_provider": source_provider,
            "monitor_client": monitor_client,
        }

    @pytest.fixture
    def evaluator(self, mock_dependencies: Dict[str, MagicMock]) -> TestEvaluator:
        """Create a test evaluator instance"""
        return TestEvaluator(
            connection_provider=mock_dependencies["connection_provider"],
            state_provider=mock_dependencies["state_provider"],
            source_provider=mock_dependencies["source_provider"],
            monitor_client=mock_dependencies["monitor_client"],
        )

    @pytest.fixture
    def assertion(self) -> MagicMock:
        """Create a sample assertion for testing"""
        assertion = MagicMock()
        assertion.urn = "urn:li:assertion:123"
        return assertion

    @pytest.fixture
    def parameters(self) -> AssertionEvaluationParameters:
        """Create test parameters"""
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
            datasetFieldParameters=None,
            datasetFreshnessParameters=None,
            datasetSchemaParameters=None,
            datasetVolumeParameters=None,
        )

    @pytest.fixture
    def context(self) -> MagicMock:
        """Create test context"""
        context = MagicMock(spec=AssertionEvaluationContext)
        context.monitor_urn = "urn:li:monitor:123"
        return context

    @pytest.fixture
    def context_no_monitor(self) -> MagicMock:
        """Create test context without monitor URN"""
        context = MagicMock(spec=AssertionEvaluationContext)
        context.monitor_urn = None
        return context

    # Tests for evaluate method
    def test_evaluate_success(
        self,
        evaluator: TestEvaluator,
        assertion: MagicMock,
        parameters: AssertionEvaluationParameters,
        context: MagicMock,
        mock_dependencies: Dict[str, MagicMock],
    ) -> None:
        """Test successful evaluation"""
        result = evaluator.evaluate(assertion, parameters, context)

        assert result.type == AssertionResultType.SUCCESS
        mock_dependencies["monitor_client"].patch_monitor_state.assert_called_once_with(
            monitor_urn=context.monitor_urn,
            new_state=MonitorStateClass.EVALUATION,
            error=None,
        )

    def test_evaluate_init(
        self,
        evaluator: TestEvaluator,
        assertion: MagicMock,
        parameters: AssertionEvaluationParameters,
        context: MagicMock,
        mock_dependencies: Dict[str, MagicMock],
    ) -> None:
        """Test initialization evaluation"""
        # Patch the _evaluate_internal method to return INIT type
        with patch.object(
            evaluator,
            "_evaluate_internal",
            return_value=AssertionEvaluationResult(AssertionResultType.INIT),
        ):
            result = evaluator.evaluate(assertion, parameters, context)

            assert result.type == AssertionResultType.INIT
            mock_dependencies[
                "monitor_client"
            ].patch_monitor_state.assert_called_once_with(
                monitor_urn=context.monitor_urn,
                new_state=MonitorStateClass.TRAINING,
                error=None,
            )

    def test_evaluate_no_monitor(
        self,
        evaluator: TestEvaluator,
        assertion: MagicMock,
        parameters: AssertionEvaluationParameters,
        context_no_monitor: MagicMock,
        mock_dependencies: Dict[str, MagicMock],
    ) -> None:
        """Test evaluation without monitor URN"""
        result = evaluator.evaluate(assertion, parameters, context_no_monitor)

        assert result.type == AssertionResultType.SUCCESS
        # Should not call patch_monitor_state when no monitor_urn is present
        mock_dependencies["monitor_client"].patch_monitor_state.assert_not_called()

    def test_evaluate_with_source_connection_error(
        self,
        evaluator: TestEvaluator,
        assertion: MagicMock,
        parameters: AssertionEvaluationParameters,
        context: MagicMock,
        mock_dependencies: Dict[str, MagicMock],
    ) -> None:
        """Test evaluation with SourceConnectionErrorException"""
        # Create source connection error
        exception = SourceConnectionErrorException(
            message="Failed to connect to database",
            connection_urn="urn:li:connection:test",
        )

        with patch.object(evaluator, "_evaluate_internal", side_effect=exception):
            result = evaluator.evaluate(assertion, parameters, context)

            assert result.type == AssertionResultType.ERROR
            assert result.error is not None  # Check that error is not None
            assert result.error.type == AssertionResultErrorType.SOURCE_CONNECTION_ERROR
            assert (
                result.error.properties is not None
            )  # Check that properties is not None
            assert "Failed to connect to database" in result.error.properties.get(
                "message", ""
            )
            assert (
                result.error.properties.get("connection_urn")
                == "urn:li:connection:test"
            )

            # Check monitor state update
            mock_dependencies["monitor_client"].patch_monitor_state.assert_called_once()
            call_args = mock_dependencies[
                "monitor_client"
            ].patch_monitor_state.call_args[1]
            assert call_args["monitor_urn"] == context.monitor_urn
            assert call_args["new_state"] == MonitorStateClass.ERROR

    def test_evaluate_with_insufficient_data(
        self,
        evaluator: TestEvaluator,
        assertion: MagicMock,
        parameters: AssertionEvaluationParameters,
        context: MagicMock,
        mock_dependencies: Dict[str, MagicMock],
    ) -> None:
        """Test evaluation with InsufficientDataException"""
        exception = InsufficientDataException(
            message="Not enough data to evaluate assertion"
        )

        with patch.object(evaluator, "_evaluate_internal", side_effect=exception):
            result = evaluator.evaluate(assertion, parameters, context)

            assert result.type == AssertionResultType.ERROR
            assert result.error is not None  # Check that error is not None
            assert result.error.type == AssertionResultErrorType.INSUFFICIENT_DATA
            assert (
                result.error.properties is not None
            )  # Check that properties is not None
            message = result.error.properties.get("message", "")
            assert "Not enough data" in message

    def test_evaluate_with_generic_exception(
        self,
        evaluator: TestEvaluator,
        assertion: MagicMock,
        parameters: AssertionEvaluationParameters,
        context: MagicMock,
        mock_dependencies: Dict[str, MagicMock],
    ) -> None:
        """Test evaluation with generic Exception"""
        exception = Exception("Generic error")

        with patch.object(evaluator, "_evaluate_internal", side_effect=exception):
            result = evaluator.evaluate(assertion, parameters, context)

            assert result.type == AssertionResultType.ERROR
            assert result.error is not None  # Check that error is not None
            assert result.error.type == AssertionResultErrorType.UNKNOWN_ERROR
            assert (
                result.error.properties is not None
            )  # Check that properties is not None
            assert result.error.properties["assertion_urn"] == assertion.urn
            assert "Generic error" in result.error.properties["message"]
            assert "stacktrace" in result.error.properties

            # Check monitor state update
            mock_dependencies["monitor_client"].patch_monitor_state.assert_called_once()
            call_args = mock_dependencies[
                "monitor_client"
            ].patch_monitor_state.call_args[1]
            assert call_args["monitor_urn"] == context.monitor_urn
            assert (
                call_args["new_state"] == "ERROR"
            )  # Note: using string instead of enum
            assert call_args["error"] is not None
            assert "An unknown error occurred" in call_args["error"].message

    def test_evaluate_with_default_parameters(
        self,
        evaluator: TestEvaluator,
        assertion: MagicMock,
        context: MagicMock,
        mock_dependencies: Dict[str, MagicMock],
    ) -> None:
        """Test evaluation using default parameters when None is provided"""
        # Use an explicit cast to help the type checker understand this is intentional
        default_params = cast(AssertionEvaluationParameters, None)
        result = evaluator.evaluate(assertion, default_params, context)

        assert result.type == AssertionResultType.SUCCESS
        mock_dependencies["monitor_client"].patch_monitor_state.assert_called_once()

    # Tests for _compare_values method
    def test_compare_values_between_valid(self, evaluator: TestEvaluator) -> None:
        """Test BETWEEN operator with valid values"""
        result = evaluator._compare_values(
            50, AssertionStdOperator.BETWEEN, None, 0, 100
        )
        assert result is True

        result = evaluator._compare_values(
            0, AssertionStdOperator.BETWEEN, None, 0, 100
        )
        assert result is True

        result = evaluator._compare_values(
            100, AssertionStdOperator.BETWEEN, None, 0, 100
        )
        assert result is True

        result = evaluator._compare_values(
            -10, AssertionStdOperator.BETWEEN, None, 0, 100
        )
        assert result is False

        result = evaluator._compare_values(
            110, AssertionStdOperator.BETWEEN, None, 0, 100
        )
        assert result is False

    def test_compare_values_between_missing_params(
        self, evaluator: TestEvaluator
    ) -> None:
        """Test BETWEEN operator with missing parameters"""
        with pytest.raises(InvalidParametersException):
            evaluator._compare_values(50, AssertionStdOperator.BETWEEN, None, None, 100)

        with pytest.raises(InvalidParametersException):
            evaluator._compare_values(50, AssertionStdOperator.BETWEEN, None, 0, None)

    def test_compare_values_greater_than(self, evaluator: TestEvaluator) -> None:
        """Test GREATER_THAN operator"""
        result = evaluator._compare_values(
            50, AssertionStdOperator.GREATER_THAN, 25, None, None
        )
        assert result is True

        result = evaluator._compare_values(
            50, AssertionStdOperator.GREATER_THAN, 50, None, None
        )
        assert result is False

        result = evaluator._compare_values(
            50, AssertionStdOperator.GREATER_THAN, 75, None, None
        )
        assert result is False

    def test_compare_values_greater_than_equal(self, evaluator: TestEvaluator) -> None:
        """Test GREATER_THAN_OR_EQUAL_TO operator"""
        result = evaluator._compare_values(
            50, AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO, 25, None, None
        )
        assert result is True

        result = evaluator._compare_values(
            50, AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO, 50, None, None
        )
        assert result is True

        result = evaluator._compare_values(
            50, AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO, 75, None, None
        )
        assert result is False

    def test_compare_values_equal(self, evaluator: TestEvaluator) -> None:
        """Test EQUAL_TO operator"""
        result = evaluator._compare_values(
            50, AssertionStdOperator.EQUAL_TO, 50, None, None
        )
        assert result is True

        result = evaluator._compare_values(
            50, AssertionStdOperator.EQUAL_TO, 25, None, None
        )
        assert result is False

    def test_compare_values_not_equal(self, evaluator: TestEvaluator) -> None:
        """Test NOT_EQUAL_TO operator"""
        result = evaluator._compare_values(
            50, AssertionStdOperator.NOT_EQUAL_TO, 25, None, None
        )
        assert result is True

        result = evaluator._compare_values(
            50, AssertionStdOperator.NOT_EQUAL_TO, 50, None, None
        )
        assert result is False

    def test_compare_values_less_than(self, evaluator: TestEvaluator) -> None:
        """Test LESS_THAN operator"""
        result = evaluator._compare_values(
            50, AssertionStdOperator.LESS_THAN, 75, None, None
        )
        assert result is True

        result = evaluator._compare_values(
            50, AssertionStdOperator.LESS_THAN, 50, None, None
        )
        assert result is False

        result = evaluator._compare_values(
            50, AssertionStdOperator.LESS_THAN, 25, None, None
        )
        assert result is False

    def test_compare_values_less_than_equal(self, evaluator: TestEvaluator) -> None:
        """Test LESS_THAN_OR_EQUAL_TO operator"""
        result = evaluator._compare_values(
            50, AssertionStdOperator.LESS_THAN_OR_EQUAL_TO, 75, None, None
        )
        assert result is True

        result = evaluator._compare_values(
            50, AssertionStdOperator.LESS_THAN_OR_EQUAL_TO, 50, None, None
        )
        assert result is True

        result = evaluator._compare_values(
            50, AssertionStdOperator.LESS_THAN_OR_EQUAL_TO, 25, None, None
        )
        assert result is False

    def test_compare_values_missing_value(self, evaluator: TestEvaluator) -> None:
        """Test with missing value parameter for operators that require it"""
        with pytest.raises(InvalidParametersException):
            evaluator._compare_values(
                50, AssertionStdOperator.GREATER_THAN, None, None, None
            )

    def test_compare_values_unsupported_operator(
        self, evaluator: TestEvaluator
    ) -> None:
        """Test with unsupported operator"""
        # Create a mock operator that's not handled by the method
        unsupported_operator = MagicMock()
        unsupported_operator.name = "UNSUPPORTED"

        with pytest.raises(InvalidParametersException):
            evaluator._compare_values(50, unsupported_operator, 25, None, None)

    # Tests for _evaluate_value method
    def test_evaluate_value(self, evaluator: TestEvaluator) -> None:
        """Test _evaluate_value method"""
        # Create mock value parameters
        value_param = MagicMock()
        value_param.value = "25"

        min_value_param = MagicMock()
        min_value_param.value = "0"

        max_value_param = MagicMock()
        max_value_param.value = "100"

        # Create parameters with different combinations
        params_with_value = MagicMock(spec=AssertionStdParameters)
        params_with_value.value = value_param
        params_with_value.min_value = None
        params_with_value.max_value = None

        params_with_min_max = MagicMock(spec=AssertionStdParameters)
        params_with_min_max.value = None
        params_with_min_max.min_value = min_value_param
        params_with_min_max.max_value = max_value_param

        # Test _evaluate_value with GREATER_THAN
        with patch.object(
            evaluator, "_compare_values", return_value=True
        ) as mock_compare:
            result = evaluator._evaluate_value(
                50, AssertionStdOperator.GREATER_THAN, params_with_value
            )
            assert result is True
            mock_compare.assert_called_once_with(
                50, AssertionStdOperator.GREATER_THAN, 25.0, None, None
            )

        # Test _evaluate_value with BETWEEN
        with patch.object(
            evaluator, "_compare_values", return_value=True
        ) as mock_compare:
            result = evaluator._evaluate_value(
                50, AssertionStdOperator.BETWEEN, params_with_min_max
            )
            assert result is True
            mock_compare.assert_called_once_with(
                50, AssertionStdOperator.BETWEEN, None, 0.0, 100.0
            )

    # Tests for _evaluate_value_change method
    def test_evaluate_value_change_absolute(self, evaluator: TestEvaluator) -> None:
        """Test _evaluate_value_change method with ABSOLUTE change type"""
        # Create mock value parameters
        value_param = MagicMock()
        value_param.value = "10"  # Absolute change of +10

        min_value_param = MagicMock()
        min_value_param.value = "5"  # Absolute change of +5

        max_value_param = MagicMock()
        max_value_param.value = "15"  # Absolute change of +15

        # Create parameters
        params = MagicMock(spec=AssertionStdParameters)
        params.value = value_param
        params.min_value = min_value_param
        params.max_value = max_value_param

        # Test with ABSOLUTE change type
        with patch.object(
            evaluator, "_compare_values", return_value=True
        ) as mock_compare:
            result = evaluator._evaluate_value_change(
                AssertionValueChangeType.ABSOLUTE,
                100,
                110,
                AssertionStdOperator.EQUAL_TO,
                params,
            )
            assert result is True
            mock_compare.assert_called_once_with(
                110, AssertionStdOperator.EQUAL_TO, 110.0, 105.0, 115.0
            )

    def test_evaluate_value_change_percentage(self, evaluator: TestEvaluator) -> None:
        """Test _evaluate_value_change method with PERCENTAGE change type"""
        # Create mock value parameters
        value_param = MagicMock()
        value_param.value = "10"  # 10% increase

        min_value_param = MagicMock()
        min_value_param.value = "5"  # 5% increase

        max_value_param = MagicMock()
        max_value_param.value = "15"  # 15% increase

        # Create parameters
        params = MagicMock(spec=AssertionStdParameters)
        params.value = value_param
        params.min_value = min_value_param
        params.max_value = max_value_param

        # Test with PERCENTAGE change type
        with patch.object(
            evaluator, "_compare_values", return_value=True
        ) as mock_compare:
            result = evaluator._evaluate_value_change(
                AssertionValueChangeType.PERCENTAGE,
                100,
                110,
                AssertionStdOperator.EQUAL_TO,
                params,
            )
            assert result is True

            # Use pytest.approx() to handle floating-point precision issues
            mock_compare.assert_called_once()
            call_args = mock_compare.call_args[0]

            # Check first two arguments exactly (they're not floating-point calculations)
            assert call_args[0] == 110
            assert call_args[1] == AssertionStdOperator.EQUAL_TO

            # Check floating-point values with approx
            assert call_args[2] == pytest.approx(110.0)
            assert call_args[3] == pytest.approx(105.0)
            assert call_args[4] == pytest.approx(115.0)

    # Tests for _update_monitor_state method
    def test_update_monitor_state_success(
        self, evaluator: TestEvaluator, mock_dependencies: Dict[str, MagicMock]
    ) -> None:
        """Test _update_monitor_state without error message"""
        evaluator._update_monitor_state(
            "urn:li:monitor:123", MonitorStateClass.EVALUATION, None
        )

        mock_dependencies["monitor_client"].patch_monitor_state.assert_called_once_with(
            monitor_urn="urn:li:monitor:123",
            new_state=MonitorStateClass.EVALUATION,
            error=None,
        )

    def test_update_monitor_state_with_error(
        self, evaluator: TestEvaluator, mock_dependencies: Dict[str, MagicMock]
    ) -> None:
        """Test _update_monitor_state with error message"""
        evaluator._update_monitor_state(
            "urn:li:monitor:123", MonitorStateClass.ERROR, "Test error message"
        )

        mock_dependencies["monitor_client"].patch_monitor_state.assert_called_once()
        call_args = mock_dependencies["monitor_client"].patch_monitor_state.call_args[1]
        assert call_args["monitor_urn"] == "urn:li:monitor:123"
        assert call_args["new_state"] == MonitorStateClass.ERROR
        assert call_args["error"] is not None
        assert call_args["error"].type == MonitorErrorTypeClass.UNKNOWN
        assert call_args["error"].message == "Test error message"

    def test_update_monitor_state_with_exception(
        self, evaluator: TestEvaluator, mock_dependencies: Dict[str, MagicMock]
    ) -> None:
        """Test _update_monitor_state handling exception from monitor_client"""
        mock_dependencies["monitor_client"].patch_monitor_state.side_effect = Exception(
            "Connection error"
        )

        # Should not raise the exception
        with patch("logging.Logger.exception") as mock_logger:
            evaluator._update_monitor_state(
                "urn:li:monitor:123", MonitorStateClass.EVALUATION, None
            )

            # Verify logging
            mock_logger.assert_called_once()
            assert "Failed to patch monitor state" in mock_logger.call_args[0][0]
