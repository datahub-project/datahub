from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Union
from unittest.mock import Mock

import pytest

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    CalendarInterval,
    TimeWindowSize,
)
from acryl_datahub_cloud.sdk.assertion_input.freshness_assertion_input import (
    FreshnessAssertionScheduleCheckType,
    _FreshnessAssertionInput,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models


@dataclass
class FreshnessAssertionTestParams:
    """Test parameters for freshness assertion input validation."""

    freshness_schedule_check_type: Optional[
        Union[
            str,
            FreshnessAssertionScheduleCheckType,
            models.FreshnessAssertionScheduleTypeClass,
        ]
    ]
    lookback_window: Optional[
        Union[
            models.TimeWindowSizeClass,
            models.FixedIntervalScheduleClass,
            TimeWindowSize,
            dict[str, Union[str, int]],
        ]
    ]
    expected_error: Optional[str] = None
    should_succeed: bool = True


class TestFreshnessAssertionInput:
    """Test suite for _FreshnessAssertionInput validation."""

    @pytest.fixture
    def mock_entity_client(self) -> Mock:
        """Create a mock entity client for testing."""
        return Mock()

    @pytest.fixture
    def base_params(self) -> dict[str, Any]:
        """Base parameters for creating _FreshnessAssertionInput instances."""
        return {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "entity_client": Mock(),
            "created_by": "urn:li:corpuser:test_user",
            "created_at": datetime(2021, 1, 1, tzinfo=timezone.utc),
            "updated_by": "urn:li:corpuser:test_user",
            "updated_at": datetime(2021, 1, 1, tzinfo=timezone.utc),
        }

    @pytest.mark.parametrize(
        "test_params",
        [
            # ============ SUCCESSFUL CASES ============
            # Test default behavior (no parameters provided)
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=None,
                    lookback_window=None,
                    should_succeed=True,
                ),
                id="default_behavior_no_parameters",
            ),
            # Test SINCE_THE_LAST_CHECK with no lookback window
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.SINCE_THE_LAST_CHECK,
                    lookback_window=None,
                    should_succeed=True,
                ),
                id="since_last_check_no_lookback_window",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type="SINCE_THE_LAST_CHECK",
                    lookback_window=None,
                    should_succeed=True,
                ),
                id="since_last_check_string_no_lookback_window",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
                    lookback_window=None,
                    should_succeed=True,
                ),
                id="since_last_check_model_no_lookback_window",
            ),
            # Test FIXED_INTERVAL with various lookback window types
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window=TimeWindowSize(
                        unit=CalendarInterval.MINUTE, multiple=10
                    ),
                    should_succeed=True,
                ),
                id="fixed_interval_with_timewindowsize_minutes",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window=TimeWindowSize(
                        unit=CalendarInterval.HOUR, multiple=2
                    ),
                    should_succeed=True,
                ),
                id="fixed_interval_with_timewindowsize_hours",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window=TimeWindowSize(
                        unit=CalendarInterval.DAY, multiple=1
                    ),
                    should_succeed=True,
                ),
                id="fixed_interval_with_timewindowsize_days",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window=TimeWindowSize(unit="MINUTE", multiple=30),
                    should_succeed=True,
                ),
                id="fixed_interval_with_timewindowsize_string_unit",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window=models.TimeWindowSizeClass(
                        unit=models.CalendarIntervalClass.MINUTE, multiple=15
                    ),
                    should_succeed=True,
                ),
                id="fixed_interval_with_timewindowsizeclass",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window=models.FixedIntervalScheduleClass(
                        unit=models.CalendarIntervalClass.HOUR, multiple=6
                    ),
                    should_succeed=True,
                ),
                id="fixed_interval_with_fixedintervalscheduleclass",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window={"unit": "MINUTE", "multiple": 45},
                    should_succeed=True,
                ),
                id="fixed_interval_with_dict_lookback_window",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window={"unit": "HOUR", "multiple": 12},
                    should_succeed=True,
                ),
                id="fixed_interval_with_dict_lookback_window_hours",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window={"unit": "DAY", "multiple": 7},
                    should_succeed=True,
                ),
                id="fixed_interval_with_dict_lookback_window_days",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type="FIXED_INTERVAL",
                    lookback_window=TimeWindowSize(
                        unit=CalendarInterval.MINUTE, multiple=5
                    ),
                    should_succeed=True,
                ),
                id="fixed_interval_string_with_timewindowsize",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
                    lookback_window=TimeWindowSize(
                        unit=CalendarInterval.HOUR, multiple=1
                    ),
                    should_succeed=True,
                ),
                id="fixed_interval_model_with_timewindowsize",
            ),
            # ============ CASE INSENSITIVE TESTS ============
            # Test case insensitive SINCE_THE_LAST_CHECK
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type="since_the_last_check",
                    lookback_window=None,
                    should_succeed=True,
                ),
                id="since_last_check_lowercase",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type="Since_The_Last_Check",
                    lookback_window=None,
                    should_succeed=True,
                ),
                id="since_last_check_mixed_case",
            ),
            # Test case insensitive FIXED_INTERVAL
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type="fixed_interval",
                    lookback_window=TimeWindowSize(
                        unit=CalendarInterval.MINUTE, multiple=10
                    ),
                    should_succeed=True,
                ),
                id="fixed_interval_lowercase",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type="Fixed_Interval",
                    lookback_window=TimeWindowSize(
                        unit=CalendarInterval.HOUR, multiple=2
                    ),
                    should_succeed=True,
                ),
                id="fixed_interval_mixed_case",
            ),
            # ============ ERROR CASES ============
            # Test FIXED_INTERVAL without required lookback window
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                    lookback_window=None,
                    expected_error="Fixed interval freshness assertions must have a lookback_window provided.",
                    should_succeed=False,
                ),
                id="fixed_interval_missing_lookback_window",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type="FIXED_INTERVAL",
                    lookback_window=None,
                    expected_error="Fixed interval freshness assertions must have a lookback_window provided.",
                    should_succeed=False,
                ),
                id="fixed_interval_string_missing_lookback_window",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
                    lookback_window=None,
                    expected_error="Fixed interval freshness assertions must have a lookback_window provided.",
                    should_succeed=False,
                ),
                id="fixed_interval_model_missing_lookback_window",
            ),
            # Test SINCE_THE_LAST_CHECK with lookback window (should fail)
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.SINCE_THE_LAST_CHECK,
                    lookback_window=TimeWindowSize(
                        unit=CalendarInterval.MINUTE, multiple=10
                    ),
                    expected_error="Since the last check freshness assertions cannot have a lookback_window provided.",
                    should_succeed=False,
                ),
                id="since_last_check_with_lookback_window",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type="SINCE_THE_LAST_CHECK",
                    lookback_window=TimeWindowSize(
                        unit=CalendarInterval.HOUR, multiple=2
                    ),
                    expected_error="Since the last check freshness assertions cannot have a lookback_window provided.",
                    should_succeed=False,
                ),
                id="since_last_check_string_with_lookback_window",
            ),
            pytest.param(
                FreshnessAssertionTestParams(
                    freshness_schedule_check_type=models.FreshnessAssertionScheduleTypeClass.SINCE_THE_LAST_CHECK,
                    lookback_window={"unit": "DAY", "multiple": 1},
                    expected_error="Since the last check freshness assertions cannot have a lookback_window provided.",
                    should_succeed=False,
                ),
                id="since_last_check_model_with_lookback_window",
            ),
        ],
    )
    def test_freshness_assertion_input_validation(
        self, test_params: FreshnessAssertionTestParams, base_params: dict[str, Any]
    ) -> None:
        """Test that _FreshnessAssertionInput correctly validates freshness_schedule_check_type and lookback_window combinations."""

        if test_params.should_succeed:
            # Test successful cases
            freshness_assertion_input = _FreshnessAssertionInput(
                **base_params,
                freshness_schedule_check_type=test_params.freshness_schedule_check_type,
                lookback_window=test_params.lookback_window,
            )

            # Verify the parsed values
            if test_params.freshness_schedule_check_type is None:
                # Should default to SINCE_THE_LAST_CHECK
                assert (
                    freshness_assertion_input.freshness_schedule_check_type
                    == FreshnessAssertionScheduleCheckType.SINCE_THE_LAST_CHECK
                )
            else:
                # Should parse correctly
                if isinstance(
                    test_params.freshness_schedule_check_type,
                    str,
                ):
                    # For string inputs, determine expected value based on case-insensitive comparison
                    input_upper = test_params.freshness_schedule_check_type.upper()
                    if input_upper == "SINCE_THE_LAST_CHECK":
                        expected = (
                            FreshnessAssertionScheduleCheckType.SINCE_THE_LAST_CHECK
                        )
                    elif input_upper == "FIXED_INTERVAL":
                        expected = FreshnessAssertionScheduleCheckType.FIXED_INTERVAL
                    else:
                        # This should not happen in our test cases, but fall back to original behavior
                        expected = FreshnessAssertionScheduleCheckType(
                            test_params.freshness_schedule_check_type
                        )
                elif isinstance(
                    test_params.freshness_schedule_check_type,
                    models.FreshnessAssertionScheduleTypeClass,
                ):
                    expected = FreshnessAssertionScheduleCheckType(
                        test_params.freshness_schedule_check_type
                    )
                else:
                    expected = test_params.freshness_schedule_check_type
                assert (
                    freshness_assertion_input.freshness_schedule_check_type == expected
                )

            # Verify lookback window
            if test_params.lookback_window is None:
                assert freshness_assertion_input.lookback_window is None
            else:
                assert freshness_assertion_input.lookback_window is not None
                # The lookback window should be converted to models.TimeWindowSizeClass
                assert isinstance(
                    freshness_assertion_input.lookback_window,
                    models.TimeWindowSizeClass,
                )

        else:
            # Test failure cases
            with pytest.raises(SDKUsageError) as exc_info:
                _FreshnessAssertionInput(
                    **base_params,
                    freshness_schedule_check_type=test_params.freshness_schedule_check_type,
                    lookback_window=test_params.lookback_window,
                )

            if test_params.expected_error:
                assert test_params.expected_error in str(exc_info.value)

    @pytest.mark.parametrize(
        "lookback_window,expected_error",
        [
            # Test invalid lookback window formats
            (
                {"unit": "INVALID_UNIT", "multiple": 10},
                "Invalid calendar interval: INVALID_UNIT",
            ),
            (
                {"unit": "MINUTE", "multiple": "not_a_number"},
                "Invalid time window size: {'unit': 'MINUTE', 'multiple': 'not_a_number'}",
            ),
            (
                {"unit": "MINUTE"},  # Missing multiple
                "Invalid time window size: {'unit': 'MINUTE'}",
            ),
            (
                {"multiple": 10},  # Missing unit
                "Invalid time window size: {'multiple': 10}",
            ),
            # Note: Empty dict {} will fail validation before reaching lookback window parsing
            # because FIXED_INTERVAL requires a lookback window, so we test it separately
            (
                "invalid_string",  # Invalid type
                "Invalid time window size: invalid_string",
            ),
            (
                123,  # Invalid type
                "Invalid time window size: 123",
            ),
        ],
    )
    def test_freshness_assertion_invalid_lookback_window(
        self,
        lookback_window: Union[dict[str, Union[str, int]], str, int],
        expected_error: str,
        base_params: dict[str, Any],
    ) -> None:
        """Test that _FreshnessAssertionInput correctly handles invalid lookback window formats."""

        with pytest.raises(SDKUsageError) as exc_info:
            _FreshnessAssertionInput(
                **base_params,
                freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                lookback_window=lookback_window,  # type: ignore[arg-type]  # Some of the test cases are invalid, so we ignore the type error
            )

        assert expected_error in str(exc_info.value)

    def test_freshness_assertion_empty_dict_lookback_window(
        self, base_params: dict[str, Any]
    ) -> None:
        """Test that empty dict lookback window fails with the correct error."""

        with pytest.raises(SDKUsageError) as exc_info:
            _FreshnessAssertionInput(
                **base_params,
                freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
                lookback_window={},
            )

        # Empty dict should fail with the validation error for missing lookback_window
        assert (
            "Fixed interval freshness assertions must have a lookback_window provided."
            in str(exc_info.value)
        )

    @pytest.mark.parametrize(
        "freshness_schedule_check_type,expected_error",
        [
            # Test invalid freshness schedule check types
            (
                "INVALID_TYPE",
                "INVALID_TYPE' is not a valid FreshnessAssertionScheduleCheckType",
            ),
            (
                "random_string",
                "random_string' is not a valid FreshnessAssertionScheduleCheckType",
            ),
        ],
    )
    def test_freshness_assertion_invalid_schedule_check_type(
        self,
        freshness_schedule_check_type: str,
        expected_error: str,
        base_params: dict[str, Any],
    ) -> None:
        """Test that _FreshnessAssertionInput correctly handles invalid freshness schedule check types."""

        with pytest.raises(ValueError) as exc_info:
            _FreshnessAssertionInput(
                **base_params,
                freshness_schedule_check_type=freshness_schedule_check_type,
                lookback_window=TimeWindowSize(
                    unit=CalendarInterval.MINUTE, multiple=10
                ),
            )

        assert expected_error in str(exc_info.value)

    def test_freshness_assertion_creation_with_detection_mechanisms(
        self, base_params: dict[str, Any]
    ) -> None:
        """Test that _FreshnessAssertionInput works with different detection mechanisms."""

        # Test with information schema detection mechanism
        freshness_assertion_input = _FreshnessAssertionInput(
            **base_params,
            detection_mechanism="information_schema",
        )

        assert (
            freshness_assertion_input.freshness_schedule_check_type
            == FreshnessAssertionScheduleCheckType.SINCE_THE_LAST_CHECK
        )
        assert freshness_assertion_input.lookback_window is None

        # Test with datahub operation detection mechanism
        freshness_assertion_input = _FreshnessAssertionInput(
            **base_params,
            detection_mechanism="datahub_operation",
            freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
            lookback_window=TimeWindowSize(unit=CalendarInterval.HOUR, multiple=1),
        )

        assert (
            freshness_assertion_input.freshness_schedule_check_type
            == FreshnessAssertionScheduleCheckType.FIXED_INTERVAL
        )
        assert freshness_assertion_input.lookback_window is not None
        assert freshness_assertion_input.lookback_window.multiple == 1
        assert (
            freshness_assertion_input.lookback_window.unit
            == models.CalendarIntervalClass.HOUR
        )

    def test_freshness_assertion_edge_cases(self, base_params: dict[str, Any]) -> None:
        """Test edge cases for _FreshnessAssertionInput."""

        # Test with zero multiple (should be valid)
        freshness_assertion_input = _FreshnessAssertionInput(
            **base_params,
            freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
            lookback_window=TimeWindowSize(unit=CalendarInterval.MINUTE, multiple=0),
        )

        assert freshness_assertion_input.lookback_window is not None
        assert freshness_assertion_input.lookback_window.multiple == 0

        # Test with large multiple
        freshness_assertion_input = _FreshnessAssertionInput(
            **base_params,
            freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
            lookback_window=TimeWindowSize(unit=CalendarInterval.DAY, multiple=365),
        )

        assert freshness_assertion_input.lookback_window is not None
        assert freshness_assertion_input.lookback_window.multiple == 365

        # Test with negative multiple (should be valid as it's just a number)
        freshness_assertion_input = _FreshnessAssertionInput(
            **base_params,
            freshness_schedule_check_type=FreshnessAssertionScheduleCheckType.FIXED_INTERVAL,
            lookback_window=TimeWindowSize(unit=CalendarInterval.MINUTE, multiple=-1),
        )

        assert freshness_assertion_input.lookback_window is not None
        assert freshness_assertion_input.lookback_window.multiple == -1
