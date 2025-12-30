"""Tests for metrics_registry module."""

import pytest

from datahub_integrations.observability.metrics_constants import (
    ACTIONS_EXECUTION_DURATION,
    ACTIONS_VENV_SETUP_DURATION,
    ANALYTICS_QUERY_DURATION,
    BOT_DATAHUB_QUERY_DURATION,
    BOT_OAUTH_DURATION,
    BOT_SLACK_API_DURATION,
    BOT_TEAMS_API_DURATION,
    GENAI_LLM_CALL_DURATION,
    GENAI_TOOL_CALL_DURATION,
    GENAI_USER_MESSAGE_DURATION,
)
from datahub_integrations.observability.metrics_registry import (
    DURATION_METRICS,
    BucketProfile,
    DurationMetricDefinition,
    get_all_metric_names,
    get_metrics_by_profile,
    validate_metric_exists,
)


class TestBucketProfile:
    """Tests for BucketProfile enum."""

    def test_fast_profile_value(self):
        """Test FAST profile has correct string value."""
        assert BucketProfile.FAST.value == "fast"

    def test_slow_profile_value(self):
        """Test SLOW profile has correct string value."""
        assert BucketProfile.SLOW.value == "slow"

    def test_bucket_profile_is_string_enum(self):
        """Test BucketProfile is a string enum."""
        assert isinstance(BucketProfile.FAST, str)
        assert isinstance(BucketProfile.SLOW, str)


class TestDurationMetricDefinition:
    """Tests for DurationMetricDefinition dataclass."""

    def test_metric_definition_creation(self):
        """Test creating a metric definition."""
        metric = DurationMetricDefinition(
            name="test_metric_duration_seconds",
            description="Test metric description",
            bucket_profile=BucketProfile.FAST,
            labels=["label1", "label2"],
        )

        assert metric.name == "test_metric_duration_seconds"
        assert metric.description == "Test metric description"
        assert metric.bucket_profile == BucketProfile.FAST
        assert metric.labels == ["label1", "label2"]

    def test_metric_definition_is_frozen(self):
        """Test metric definitions are immutable."""
        metric = DurationMetricDefinition(
            name="test_metric",
            description="Test",
            bucket_profile=BucketProfile.FAST,
            labels=[],
        )

        with pytest.raises(AttributeError):
            metric.name = "changed"  # type: ignore[misc]


class TestDurationMetricsRegistry:
    """Tests for DURATION_METRICS registry."""

    def test_registry_is_not_empty(self):
        """Test registry contains metric definitions."""
        assert len(DURATION_METRICS) > 0

    def test_registry_has_expected_count(self):
        """Test registry has all 14 duration metrics."""
        assert len(DURATION_METRICS) == 14

    def test_all_metrics_have_valid_names(self):
        """Test all metrics have non-empty names."""
        for metric in DURATION_METRICS:
            assert metric.name
            assert isinstance(metric.name, str)
            assert len(metric.name) > 0

    def test_all_metrics_have_descriptions(self):
        """Test all metrics have descriptions."""
        for metric in DURATION_METRICS:
            assert metric.description
            assert isinstance(metric.description, str)
            assert len(metric.description) > 10  # Meaningful description

    def test_all_metrics_have_bucket_profiles(self):
        """Test all metrics have valid bucket profiles."""
        for metric in DURATION_METRICS:
            assert metric.bucket_profile in [BucketProfile.FAST, BucketProfile.SLOW]

    def test_all_metrics_have_labels(self):
        """Test all metrics have labels list (can be empty)."""
        for metric in DURATION_METRICS:
            assert isinstance(metric.labels, list)

    def test_no_duplicate_metric_names(self):
        """Test registry has no duplicate metric names."""
        names = [m.name for m in DURATION_METRICS]
        assert len(names) == len(set(names)), "Found duplicate metric names"

    def test_fast_profile_metrics_exist(self):
        """Test registry contains fast profile metrics."""
        fast_metrics = [
            m for m in DURATION_METRICS if m.bucket_profile == BucketProfile.FAST
        ]
        assert len(fast_metrics) > 0

    def test_slow_profile_metrics_exist(self):
        """Test registry contains slow profile metrics."""
        slow_metrics = [
            m for m in DURATION_METRICS if m.bucket_profile == BucketProfile.SLOW
        ]
        assert len(slow_metrics) > 0

    def test_expected_fast_metrics_are_present(self):
        """Test expected fast metrics are in registry."""
        expected_fast = [
            "http.server.request.duration",
            "http.server.duration",
            BOT_DATAHUB_QUERY_DURATION,
            BOT_SLACK_API_DURATION,
            BOT_TEAMS_API_DURATION,
            BOT_OAUTH_DURATION,
            ANALYTICS_QUERY_DURATION,
            ACTIONS_VENV_SETUP_DURATION,
            "slack_command_duration_seconds",
        ]

        registry_names = [m.name for m in DURATION_METRICS]
        for expected_name in expected_fast:
            assert expected_name in registry_names, (
                f"Missing fast metric: {expected_name}"
            )

    def test_expected_slow_metrics_are_present(self):
        """Test expected slow metrics are in registry."""
        expected_slow = [
            GENAI_USER_MESSAGE_DURATION,
            GENAI_LLM_CALL_DURATION,
            GENAI_TOOL_CALL_DURATION,
            ACTIONS_EXECUTION_DURATION,
        ]

        registry_names = [m.name for m in DURATION_METRICS]
        for expected_name in expected_slow:
            assert expected_name in registry_names, (
                f"Missing slow metric: {expected_name}"
            )

    def test_all_metric_names_use_constants(self):
        """Test metrics use constants from metrics_constants.py where applicable."""
        # All metrics should either be from constants or be standard OpenTelemetry names
        constants_values = {
            BOT_DATAHUB_QUERY_DURATION,
            BOT_SLACK_API_DURATION,
            BOT_TEAMS_API_DURATION,
            BOT_OAUTH_DURATION,
            ANALYTICS_QUERY_DURATION,
            ACTIONS_VENV_SETUP_DURATION,
            ACTIONS_EXECUTION_DURATION,
            GENAI_USER_MESSAGE_DURATION,
            GENAI_LLM_CALL_DURATION,
            GENAI_TOOL_CALL_DURATION,
        }

        standard_otel_names = {
            "http.server.request.duration",
            "http.server.duration",
            "http_server_request_duration",
            "slack_command_duration_seconds",
        }

        for metric in DURATION_METRICS:
            assert (
                metric.name in constants_values or metric.name in standard_otel_names
            ), f"Metric {metric.name} not from constants or standard OTEL names"


class TestGetMetricsByProfile:
    """Tests for get_metrics_by_profile function."""

    def test_get_fast_metrics(self):
        """Test getting fast profile metrics."""
        fast_metrics = get_metrics_by_profile(BucketProfile.FAST)

        assert len(fast_metrics) > 0
        assert all(m.bucket_profile == BucketProfile.FAST for m in fast_metrics)

    def test_get_slow_metrics(self):
        """Test getting slow profile metrics."""
        slow_metrics = get_metrics_by_profile(BucketProfile.SLOW)

        assert len(slow_metrics) > 0
        assert all(m.bucket_profile == BucketProfile.SLOW for m in slow_metrics)

    def test_fast_and_slow_metrics_are_disjoint(self):
        """Test fast and slow metrics don't overlap."""
        fast_metrics = get_metrics_by_profile(BucketProfile.FAST)
        slow_metrics = get_metrics_by_profile(BucketProfile.SLOW)

        fast_names = {m.name for m in fast_metrics}
        slow_names = {m.name for m in slow_metrics}

        assert fast_names.isdisjoint(slow_names), "Fast and slow metrics overlap"

    def test_fast_and_slow_cover_all_metrics(self):
        """Test fast and slow metrics together cover all metrics."""
        fast_metrics = get_metrics_by_profile(BucketProfile.FAST)
        slow_metrics = get_metrics_by_profile(BucketProfile.SLOW)

        assert len(fast_metrics) + len(slow_metrics) == len(DURATION_METRICS)

    def test_expected_fast_count(self):
        """Test fast profile has expected number of metrics."""
        fast_metrics = get_metrics_by_profile(BucketProfile.FAST)
        # 3 HTTP + 4 bot APIs + 1 analytics + 1 actions venv + 1 command = 10
        assert len(fast_metrics) == 10

    def test_expected_slow_count(self):
        """Test slow profile has expected number of metrics."""
        slow_metrics = get_metrics_by_profile(BucketProfile.SLOW)
        # 3 GenAI + 1 actions execution = 4
        assert len(slow_metrics) == 4


class TestGetAllMetricNames:
    """Tests for get_all_metric_names function."""

    def test_returns_all_metric_names(self):
        """Test function returns all metric names."""
        names = get_all_metric_names()

        assert len(names) == len(DURATION_METRICS)
        assert isinstance(names, list)
        assert all(isinstance(name, str) for name in names)

    def test_returned_names_match_registry(self):
        """Test returned names match those in registry."""
        names = get_all_metric_names()
        registry_names = [m.name for m in DURATION_METRICS]

        assert set(names) == set(registry_names)

    def test_no_duplicate_names(self):
        """Test returned names have no duplicates."""
        names = get_all_metric_names()
        assert len(names) == len(set(names))


class TestValidateMetricExists:
    """Tests for validate_metric_exists function."""

    def test_validates_existing_fast_metric(self):
        """Test validation succeeds for existing fast metric."""
        assert validate_metric_exists(BOT_DATAHUB_QUERY_DURATION) is True

    def test_validates_existing_slow_metric(self):
        """Test validation succeeds for existing slow metric."""
        assert validate_metric_exists(GENAI_LLM_CALL_DURATION) is True

    def test_validates_http_metric(self):
        """Test validation succeeds for HTTP metric."""
        assert validate_metric_exists("http.server.request.duration") is True

    def test_fails_for_nonexistent_metric(self):
        """Test validation fails for nonexistent metric."""
        assert validate_metric_exists("nonexistent_metric") is False

    def test_fails_for_empty_string(self):
        """Test validation fails for empty string."""
        assert validate_metric_exists("") is False

    def test_fails_for_similar_but_wrong_name(self):
        """Test validation fails for similar but incorrect name."""
        assert (
            validate_metric_exists("bot_datahub_query_duration") is False
        )  # Missing _seconds
        assert (
            validate_metric_exists("genai_llm_call_latency_seconds") is False
        )  # Wrong suffix


class TestRegistryIntegration:
    """Integration tests for metrics registry."""

    def test_all_fast_metrics_can_be_retrieved(self):
        """Test all fast metrics can be retrieved by profile."""
        fast_metrics = get_metrics_by_profile(BucketProfile.FAST)

        for metric in fast_metrics:
            assert metric in DURATION_METRICS
            assert metric.bucket_profile == BucketProfile.FAST

    def test_all_slow_metrics_can_be_retrieved(self):
        """Test all slow metrics can be retrieved by profile."""
        slow_metrics = get_metrics_by_profile(BucketProfile.SLOW)

        for metric in slow_metrics:
            assert metric in DURATION_METRICS
            assert metric.bucket_profile == BucketProfile.SLOW

    def test_metric_names_match_validation(self):
        """Test all registry names pass validation."""
        for metric in DURATION_METRICS:
            assert validate_metric_exists(metric.name), (
                f"Metric {metric.name} failed validation"
            )
