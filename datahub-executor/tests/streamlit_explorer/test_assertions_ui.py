# ruff: noqa: INP001
"""Tests for assertions_ui sample data URN generation."""

import pytest

from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    default_volume_assertion_urn,
    default_volume_monitor_urn,
    make_monitor_metric_cube_urn,
)


class TestDefaultVolumeUrns:
    """Test the default volume URN generation functions."""

    SAMPLE_DATASET_URN = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "long_tail_companions.analytics.pet_details,PROD)"
    )

    def test_default_volume_monitor_urn_returns_monitor_urn(self):
        """Ensure default_volume_monitor_urn returns a Monitor URN, not Dataset."""
        monitor_urn = default_volume_monitor_urn(self.SAMPLE_DATASET_URN)

        # Monitor URNs should start with "urn:li:monitor:"
        assert monitor_urn.startswith("urn:li:monitor:"), (
            f"Expected Monitor URN but got: {monitor_urn}"
        )
        # Should NOT be a dataset URN
        assert not monitor_urn.startswith("urn:li:dataset:"), (
            f"Got Dataset URN instead of Monitor URN: {monitor_urn}"
        )

    def test_default_volume_assertion_urn_returns_assertion_urn(self):
        """Ensure default_volume_assertion_urn returns an Assertion URN."""
        assertion_urn = default_volume_assertion_urn(self.SAMPLE_DATASET_URN)

        # Assertion URNs should start with "urn:li:assertion:"
        assert assertion_urn.startswith("urn:li:assertion:"), (
            f"Expected Assertion URN but got: {assertion_urn}"
        )
        # Should NOT be a dataset URN
        assert not assertion_urn.startswith("urn:li:dataset:"), (
            f"Got Dataset URN instead of Assertion URN: {assertion_urn}"
        )

    def test_monitor_and_assertion_urns_are_different(self):
        """Monitor and assertion URNs for the same dataset should be different."""
        monitor_urn = default_volume_monitor_urn(self.SAMPLE_DATASET_URN)
        assertion_urn = default_volume_assertion_urn(self.SAMPLE_DATASET_URN)

        assert monitor_urn != assertion_urn, (
            "Monitor URN and Assertion URN should be different"
        )

    def test_invalid_dataset_urn_raises_error(self):
        """Passing an invalid URN should raise an error."""
        with pytest.raises(Exception):
            default_volume_monitor_urn("not-a-valid-urn")

        with pytest.raises(Exception):
            default_volume_assertion_urn("not-a-valid-urn")

    def test_monitor_urn_contains_dataset_reference(self):
        """Monitor URN should contain a reference to the source dataset."""
        monitor_urn = default_volume_monitor_urn(self.SAMPLE_DATASET_URN)

        # The monitor URN format is urn:li:monitor:(dataset_urn,id)
        # So it should contain the dataset URN somehow
        assert "snowflake" in monitor_urn or "pet_details" in monitor_urn, (
            f"Monitor URN should reference the dataset: {monitor_urn}"
        )


class TestMakeMonitorMetricCubeUrn:
    """Test the make_monitor_metric_cube_urn function."""

    SAMPLE_DATASET_URN = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
        "long_tail_companions.analytics.pet_details,PROD)"
    )

    def test_requires_monitor_urn_not_dataset_urn(self):
        """make_monitor_metric_cube_urn should reject dataset URNs."""
        from datahub.utilities.urns.error import InvalidUrnError

        # This is the bug that was fixed - passing a dataset URN should fail
        with pytest.raises((AssertionError, InvalidUrnError)):
            make_monitor_metric_cube_urn(self.SAMPLE_DATASET_URN)

    def test_accepts_monitor_urn(self):
        """make_monitor_metric_cube_urn should accept a valid monitor URN."""
        monitor_urn = default_volume_monitor_urn(self.SAMPLE_DATASET_URN)

        # Should not raise an exception
        metric_urn = make_monitor_metric_cube_urn(monitor_urn)

        # Should return a DataHub Metric Cube URN
        assert metric_urn.startswith("urn:li:dataHubMetricCube:"), (
            f"Expected DataHub Metric Cube URN but got: {metric_urn}"
        )

    def test_sample_data_flow_uses_correct_urn(self):
        """
        Simulate the sample data generation flow to ensure correct URN usage.

        This tests the fix for the bug where generate_volume_sample_data
        was passing a dataset URN directly to make_monitor_metric_cube_urn.
        """
        dataset_urn = self.SAMPLE_DATASET_URN

        # The correct flow: dataset_urn -> monitor_urn -> metric_urn
        monitor_urn = default_volume_monitor_urn(dataset_urn)
        metric_urn = make_monitor_metric_cube_urn(monitor_urn)

        assert metric_urn.startswith("urn:li:dataHubMetricCube:"), (
            f"Expected DataHub Metric Cube URN but got: {metric_urn}"
        )
