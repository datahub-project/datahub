"""Tests for FieldTrainerV2."""

import pytest

pytest.importorskip("datahub_observe")

from unittest.mock import MagicMock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference_v2.field_trainer_v2 import (
    CEILING_100_METRICS,
    FIELD_METRIC_TO_CATEGORY,
    FLOOR_ZERO_METRICS,
    FieldTrainerV2,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationSpec,
)


@pytest.fixture
def trainer() -> FieldTrainerV2:
    """Create a trainer instance with mock dependencies."""
    graph = MagicMock(spec=DataHubGraph)
    metrics_client = MagicMock(spec=MetricClient)
    monitor_client = MagicMock(spec=MonitorClient)
    return FieldTrainerV2(graph, metrics_client, monitor_client)


class MockMetric:
    """Simple mock class for FieldMetricType that properly exposes the name attribute."""

    def __init__(self, name: str) -> None:
        self.name = name


def _make_assertion_with_metric(metric_name: str) -> MagicMock:
    """Create a mock assertion with the specified field metric type."""
    assertion = MagicMock(spec=Assertion)
    assertion.entity = MagicMock()
    assertion.entity.urn = "urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)"

    # Set up the nested field_assertion structure
    # Use a simple class-based mock for the metric because MagicMock's 'name'
    # attribute has special behavior that interferes with our test
    assertion.field_assertion = MagicMock()
    assertion.field_assertion.field_metric_assertion = MagicMock()
    assertion.field_assertion.field_metric_assertion.metric = MockMetric(metric_name)

    return assertion


class TestGetTrainingContext:
    """Tests for get_training_context method."""

    def test_floor_zero_metric_sets_is_delta_false(
        self, trainer: FieldTrainerV2
    ) -> None:
        """Metrics with floor=0 should set is_delta=False to filter negative values."""
        for metric_type in FLOOR_ZERO_METRICS:
            assertion = _make_assertion_with_metric(metric_type)
            eval_spec = MagicMock(spec=AssertionEvaluationSpec)
            eval_spec.context = None

            context = trainer.get_training_context(assertion, None, eval_spec)

            assert context.is_delta is False, (
                f"Metric {metric_type} should set is_delta=False"
            )
            assert context.floor_value == 0.0, (
                f"Metric {metric_type} should set floor_value=0.0"
            )

    def test_ceiling_100_metric_sets_ceiling(self, trainer: FieldTrainerV2) -> None:
        """Percentage metrics should set ceiling_value=100.0."""
        for metric_type in CEILING_100_METRICS:
            assertion = _make_assertion_with_metric(metric_type)
            eval_spec = MagicMock(spec=AssertionEvaluationSpec)
            eval_spec.context = None

            context = trainer.get_training_context(assertion, None, eval_spec)

            assert context.ceiling_value == 100.0, (
                f"Metric {metric_type} should set ceiling_value=100.0"
            )

    def test_statistic_metric_does_not_set_is_delta(
        self, trainer: FieldTrainerV2
    ) -> None:
        """Statistics metrics (MIN, MAX, MEAN, etc.) should not set is_delta."""
        statistic_metrics = ["MIN", "MAX", "MEAN", "MEDIAN", "STDDEV"]

        for metric_type in statistic_metrics:
            assertion = _make_assertion_with_metric(metric_type)
            eval_spec = MagicMock(spec=AssertionEvaluationSpec)
            eval_spec.context = None

            context = trainer.get_training_context(assertion, None, eval_spec)

            assert context.is_delta is None, (
                f"Metric {metric_type} should leave is_delta as None"
            )
            assert context.floor_value is None, (
                f"Metric {metric_type} should have no floor_value"
            )
            assert context.ceiling_value is None, (
                f"Metric {metric_type} should have no ceiling_value"
            )

    def test_unknown_metric_does_not_set_floor_ceiling_or_is_delta(
        self, trainer: FieldTrainerV2
    ) -> None:
        """Unknown metric types should not set floor, ceiling, or is_delta."""
        assertion = _make_assertion_with_metric("UNKNOWN_METRIC_TYPE")
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = None

        context = trainer.get_training_context(assertion, None, eval_spec)

        assert context.is_delta is None
        assert context.floor_value is None
        assert context.ceiling_value is None

    def test_count_metric_sets_is_delta_false_and_floor(
        self, trainer: FieldTrainerV2
    ) -> None:
        """Count metrics should set is_delta=False and floor=0."""
        assertion = _make_assertion_with_metric("NULL_COUNT")
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = None

        context = trainer.get_training_context(assertion, None, eval_spec)

        # NULL_COUNT is in FLOOR_ZERO_METRICS
        assert context.floor_value == 0.0
        assert context.is_delta is False

    def test_percentage_metric_sets_both_floor_and_ceiling(
        self, trainer: FieldTrainerV2
    ) -> None:
        """Percentage metrics should set both floor=0 and ceiling=100."""
        assertion = _make_assertion_with_metric("NULL_PERCENTAGE")
        eval_spec = MagicMock(spec=AssertionEvaluationSpec)
        eval_spec.context = None

        context = trainer.get_training_context(assertion, None, eval_spec)

        # NULL_PERCENTAGE is in both FLOOR_ZERO_METRICS and CEILING_100_METRICS
        assert context.floor_value == 0.0
        assert context.ceiling_value == 100.0
        assert context.is_delta is False


class TestGetMetricType:
    """Tests for _get_metric_type method."""

    def test_extracts_metric_type_from_assertion(self, trainer: FieldTrainerV2) -> None:
        """Correctly extracts the metric type name from a field assertion."""
        assertion = _make_assertion_with_metric("UNIQUE_COUNT")

        result = trainer._get_metric_type(assertion)

        assert result == "UNIQUE_COUNT"

    def test_returns_empty_string_when_no_field_assertion(
        self, trainer: FieldTrainerV2
    ) -> None:
        """Returns empty string when assertion has no field_assertion."""
        assertion = MagicMock(spec=Assertion)
        assertion.field_assertion = None

        result = trainer._get_metric_type(assertion)

        assert result == ""

    def test_returns_empty_string_when_no_metric(self, trainer: FieldTrainerV2) -> None:
        """Returns empty string when field_metric_assertion has no metric."""
        assertion = MagicMock(spec=Assertion)
        assertion.field_assertion = MagicMock()
        assertion.field_assertion.field_metric_assertion = MagicMock()
        assertion.field_assertion.field_metric_assertion.metric = None

        result = trainer._get_metric_type(assertion)

        assert result == ""


class TestGetCategoryForMetricType:
    """Tests for _get_category_for_metric_type method."""

    def test_maps_count_metrics_to_volume(self, trainer: FieldTrainerV2) -> None:
        """Count metrics should map to 'volume' category."""
        count_metrics = [
            "UNIQUE_COUNT",
            "NULL_COUNT",
            "NEGATIVE_COUNT",
            "ZERO_COUNT",
            "EMPTY_COUNT",
        ]

        for metric in count_metrics:
            assert trainer._get_category_for_metric_type(metric) == "volume"

    def test_maps_percentage_metrics_to_rate(self, trainer: FieldTrainerV2) -> None:
        """Percentage metrics should map to 'rate' category."""
        percentage_metrics = [
            "UNIQUE_PERCENTAGE",
            "NULL_PERCENTAGE",
            "NEGATIVE_PERCENTAGE",
            "ZERO_PERCENTAGE",
            "EMPTY_PERCENTAGE",
        ]

        for metric in percentage_metrics:
            assert trainer._get_category_for_metric_type(metric) == "rate"

    def test_maps_statistics_metrics_to_statistic(
        self, trainer: FieldTrainerV2
    ) -> None:
        """Statistics metrics should map to 'statistic' category."""
        stats_metrics = ["MIN", "MAX", "MEAN", "MEDIAN", "STDDEV"]

        for metric in stats_metrics:
            assert trainer._get_category_for_metric_type(metric) == "statistic"

    def test_maps_length_metrics_to_length(self, trainer: FieldTrainerV2) -> None:
        """Length metrics should map to 'length' category."""
        length_metrics = ["MIN_LENGTH", "MAX_LENGTH"]

        for metric in length_metrics:
            assert trainer._get_category_for_metric_type(metric) == "length"

    def test_unknown_metric_defaults_to_rate(self, trainer: FieldTrainerV2) -> None:
        """Unknown metric types should default to 'rate'."""
        assert trainer._get_category_for_metric_type("UNKNOWN_METRIC") == "rate"

    def test_case_insensitive_mapping(self, trainer: FieldTrainerV2) -> None:
        """Metric type mapping should be case-insensitive."""
        assert trainer._get_category_for_metric_type("null_count") == "volume"
        assert trainer._get_category_for_metric_type("Null_Count") == "volume"
        assert trainer._get_category_for_metric_type("NULL_COUNT") == "volume"


class TestMetricSets:
    """Tests for FLOOR_ZERO_METRICS and CEILING_100_METRICS constant sets."""

    def test_all_ceiling_metrics_also_have_floor(self) -> None:
        """All ceiling=100 metrics should also be in the floor=0 set."""
        for metric in CEILING_100_METRICS:
            assert metric in FLOOR_ZERO_METRICS, f"{metric} has ceiling but no floor"

    def test_all_metrics_have_category_mapping(self) -> None:
        """All floor/ceiling metrics should have a category mapping."""
        all_special_metrics = FLOOR_ZERO_METRICS | CEILING_100_METRICS

        for metric in all_special_metrics:
            assert metric in FIELD_METRIC_TO_CATEGORY, (
                f"{metric} missing from FIELD_METRIC_TO_CATEGORY"
            )
