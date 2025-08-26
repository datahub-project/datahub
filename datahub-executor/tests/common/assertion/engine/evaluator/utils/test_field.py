from unittest.mock import MagicMock

import pytest

from datahub_executor.common.assertion.engine.evaluator.utils.field import (
    convert_field_parameters_to_metric_resolver_strategy,
)
from datahub_executor.common.metric.types import (
    MetricResolverStrategy,
    MetricSourceType,
)
from datahub_executor.common.types import (
    DatasetFieldAssertionParameters,
    DatasetFieldSourceType,
)


class TestFieldUtils:
    """Test suite for field utility functions."""

    def test_convert_all_rows_query(self) -> None:
        """Test conversion for ALL_ROWS_QUERY source type."""
        # Create mock parameters with ALL_ROWS_QUERY source type
        parameters = MagicMock(spec=DatasetFieldAssertionParameters)
        parameters.source_type = DatasetFieldSourceType.ALL_ROWS_QUERY

        # Convert parameters to metric resolver strategy
        strategy = convert_field_parameters_to_metric_resolver_strategy(parameters)

        # Verify the result
        assert isinstance(strategy, MetricResolverStrategy)
        assert strategy.source_type.value == MetricSourceType.QUERY.value

    def test_convert_changed_rows_query(self) -> None:
        """Test conversion for CHANGED_ROWS_QUERY source type."""
        # Create mock parameters with CHANGED_ROWS_QUERY source type
        parameters = MagicMock(spec=DatasetFieldAssertionParameters)
        parameters.source_type = DatasetFieldSourceType.CHANGED_ROWS_QUERY

        # Convert parameters to metric resolver strategy
        strategy = convert_field_parameters_to_metric_resolver_strategy(parameters)

        # Verify the result
        assert isinstance(strategy, MetricResolverStrategy)
        assert strategy.source_type.value == MetricSourceType.QUERY.value

    def test_convert_datahub_dataset_profile(self) -> None:
        """Test conversion for DATAHUB_DATASET_PROFILE source type."""
        # Create mock parameters with DATAHUB_DATASET_PROFILE source type
        parameters = MagicMock(spec=DatasetFieldAssertionParameters)
        parameters.source_type = DatasetFieldSourceType.DATAHUB_DATASET_PROFILE

        # Convert parameters to metric resolver strategy
        strategy = convert_field_parameters_to_metric_resolver_strategy(parameters)

        # Verify the result
        assert isinstance(strategy, MetricResolverStrategy)
        assert (
            strategy.source_type.value == MetricSourceType.DATAHUB_DATASET_PROFILE.value
        )

    def test_convert_unsupported_source_type(self) -> None:
        """Test conversion with an unsupported source type."""
        # Create mock parameters with an unsupported source type
        parameters = MagicMock(spec=DatasetFieldAssertionParameters)
        parameters.source_type = "UNSUPPORTED_TYPE"

        # Attempt to convert parameters with an unsupported source type
        with pytest.raises(Exception) as excinfo:
            convert_field_parameters_to_metric_resolver_strategy(parameters)

        # Verify the exception message
        assert "Unsupported field source type" in str(excinfo.value)
        assert "UNSUPPORTED_TYPE" in str(excinfo.value)
        assert "No matching Metric Resolver Strategy" in str(excinfo.value)
