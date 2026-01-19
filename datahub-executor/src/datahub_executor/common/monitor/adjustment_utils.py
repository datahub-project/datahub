"""
Shared utilities for assertion adjustment settings.

These utilities are used across both inference and inference_v2 modules.
"""

from typing import Optional

from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    encode_monitor_urn,
)
from datahub_executor.common.types import AssertionAdjustmentSettings
from datahub_executor.config import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
)


def get_metric_cube_urn(monitor_urn: str) -> str:
    """Get the metric cube URN for a monitor."""
    return f"urn:li:dataHubMetricCube:{encode_monitor_urn(monitor_urn)}"


def extract_lookback_days(
    adjustment_settings: Optional[AssertionAdjustmentSettings],
) -> int:
    """
    Extract the lookback window in days from adjustment settings.

    Returns the configured value or the default if not set.
    """
    if adjustment_settings and adjustment_settings.training_data_lookback_window_days:
        return adjustment_settings.training_data_lookback_window_days
    return ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS


def get_sensitivity_level(
    adjustment_settings: Optional[AssertionAdjustmentSettings],
    default: int,
) -> int:
    """
    Extract sensitivity level from adjustment settings.

    Args:
        adjustment_settings: The adjustment settings to extract from.
        default: The default sensitivity level to use if not configured.

    Returns:
        The configured sensitivity level or the default.
    """
    if (
        adjustment_settings
        and adjustment_settings.sensitivity
        and adjustment_settings.sensitivity.level is not None
    ):
        return adjustment_settings.sensitivity.level
    return default
