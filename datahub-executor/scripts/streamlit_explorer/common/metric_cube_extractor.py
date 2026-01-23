# ruff: noqa: INP001
"""
Metric cube extraction utilities.

This module provides functions to transform metric cube events
into time series format (ds, y) suitable for preprocessing and forecasting.

Unlike run_event_extractor.py which extracts metrics from nested assertion run event
fields, metric cube events have a clean structure with the measure value directly
available, making extraction simpler and more reliable.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    decode_metric_cube_urn,
    make_monitor_metric_cube_urn as build_metric_cube_urn,
)

# Re-export for backwards compatibility
__all__ = ["build_metric_cube_urn", "decode_metric_cube_urn"]

# Timestamp column
TIMESTAMP_COLUMN = "timestampMillis"


@dataclass
class MetricCubeMetadata:
    """Metadata about a metric cube extracted from events."""

    metric_cube_urn: str
    monitor_urn: Optional[str]
    point_count: int
    first_event: Optional[datetime]
    last_event: Optional[datetime]
    value_min: Optional[float]
    value_max: Optional[float]
    value_mean: Optional[float]
    anomaly_count: int


def list_metric_cubes(
    events_df: pd.DataFrame,
) -> list[MetricCubeMetadata]:
    """List all unique metric cubes in the events with metadata.

    Args:
        events_df: DataFrame containing metric cube events

    Returns:
        List of MetricCubeMetadata objects with summary info for each cube
    """
    if events_df is None or len(events_df) == 0:
        return []

    df = events_df.copy()

    # Determine URN column - prefer metricCubeUrn, fall back to monitorUrn
    urn_col = None
    if "metricCubeUrn" in df.columns:
        urn_col = "metricCubeUrn"
    elif "monitorUrn" in df.columns:
        urn_col = "monitorUrn"
    else:
        return []

    cubes = []
    for urn in df[urn_col].unique():
        if pd.isna(urn):
            continue

        cube_df = df[df[urn_col] == urn]

        # Determine metric cube URN and monitor URN
        if urn_col == "metricCubeUrn":
            metric_cube_urn = urn
            monitor_urn = decode_metric_cube_urn(urn)
        else:
            # monitorUrn column - derive metric cube URN
            monitor_urn = urn
            metric_cube_urn = build_metric_cube_urn(urn)

        # Get timestamp range
        first_event = None
        last_event = None
        if TIMESTAMP_COLUMN in cube_df.columns:
            ts_series = cube_df[TIMESTAMP_COLUMN].dropna()
            if len(ts_series) > 0:
                first_event = pd.to_datetime(ts_series.min(), unit="ms").to_pydatetime()
                last_event = pd.to_datetime(ts_series.max(), unit="ms").to_pydatetime()

        # Get value statistics
        value_min = None
        value_max = None
        value_mean = None
        if "measure" in cube_df.columns:
            value_series = pd.to_numeric(cube_df["measure"], errors="coerce").dropna()
            if len(value_series) > 0:
                value_min = float(value_series.min())
                value_max = float(value_series.max())
                value_mean = float(value_series.mean())

        # Count anomalies
        anomaly_count = 0
        if "anomaly_state" in cube_df.columns:
            anomaly_mask = cube_df["anomaly_state"].notna()
            anomaly_count = int(anomaly_mask.sum())

        cubes.append(
            MetricCubeMetadata(
                metric_cube_urn=metric_cube_urn,
                monitor_urn=monitor_urn,
                point_count=len(cube_df),
                first_event=first_event,
                last_event=last_event,
                value_min=value_min,
                value_max=value_max,
                value_mean=value_mean,
                anomaly_count=anomaly_count,
            )
        )

    # Sort by point count descending
    cubes.sort(key=lambda x: x.point_count, reverse=True)
    return cubes


def extract_timeseries(
    events_df: pd.DataFrame,
    monitor_urn: Optional[str] = None,
    metric_cube_urn: Optional[str] = None,
    include_anomalies: bool = True,
) -> pd.DataFrame:
    """Extract time series data from metric cube events.

    Unlike assertion run events, metric cube events have a clean structure
    with the measure value directly available. This makes extraction simpler.

    Args:
        events_df: DataFrame containing metric cube events
        monitor_urn: Optional monitor URN to filter by
        metric_cube_urn: Optional metric cube URN to filter by
        include_anomalies: If True, include anomaly_state column

    Returns:
        DataFrame with columns 'ds' (datetime), 'y' (value),
        'timestampMillis' (original timestamp for anomaly matching),
        and optionally 'anomaly_timestampMillis' (indicates anomaly exists)
        and 'anomaly_state' (user review state, may be null for unreviewed).
    """
    base_cols = ["ds", "y", "anomaly_state"] if include_anomalies else ["ds", "y"]

    if events_df is None or len(events_df) == 0:
        return pd.DataFrame(columns=base_cols)

    df = events_df.copy()

    # Filter by URN if provided
    if monitor_urn:
        if "monitorUrn" in df.columns:
            df = df[df["monitorUrn"] == monitor_urn]
        elif "metricCubeUrn" in df.columns:
            # Convert monitor URN to metric cube URN for filtering
            target_cube_urn = build_metric_cube_urn(monitor_urn)
            df = df[df["metricCubeUrn"] == target_cube_urn]

    if metric_cube_urn:
        if "metricCubeUrn" in df.columns:
            df = df[df["metricCubeUrn"] == metric_cube_urn]
        elif "monitorUrn" in df.columns:
            # Decode metric cube URN to monitor URN for filtering
            target_monitor = decode_metric_cube_urn(metric_cube_urn)
            if target_monitor:
                df = df[df["monitorUrn"] == target_monitor]

    if len(df) == 0:
        return pd.DataFrame(columns=base_cols)

    # Check required columns
    if TIMESTAMP_COLUMN not in df.columns:
        return pd.DataFrame(columns=base_cols)

    if "measure" not in df.columns:
        return pd.DataFrame(columns=base_cols)

    # Build result DataFrame
    result = pd.DataFrame()
    result["ds"] = pd.to_datetime(df[TIMESTAMP_COLUMN], unit="ms")
    result["y"] = pd.to_numeric(df["measure"], errors="coerce")
    # Preserve timestampMillis for anomaly matching (used by assertion_browser.py)
    result["timestampMillis"] = df[TIMESTAMP_COLUMN].values

    # Add anomaly data if requested and available
    # Note: anomaly_timestampMillis indicates an anomaly exists (auto-detected or confirmed)
    # anomaly_state is the user's review state (can be null for unreviewed anomalies)
    if include_anomalies:
        if "anomaly_timestampMillis" in df.columns:
            result["anomaly_timestampMillis"] = df["anomaly_timestampMillis"].values
        else:
            result["anomaly_timestampMillis"] = None
        if "anomaly_state" in df.columns:
            result["anomaly_state"] = df["anomaly_state"].values
        else:
            result["anomaly_state"] = None

    # Drop rows with null ds or y
    result = result.dropna(subset=["ds", "y"])

    # Sort by timestamp
    result = result.sort_values("ds").reset_index(drop=True)

    return result


def extract_timeseries_with_anomalies(
    events_df: pd.DataFrame,
    monitor_urn: Optional[str] = None,
    metric_cube_urn: Optional[str] = None,
) -> pd.DataFrame:
    """Extract time series data with detailed anomaly information.

    Args:
        events_df: DataFrame containing metric cube events
        monitor_urn: Optional monitor URN to filter by
        metric_cube_urn: Optional metric cube URN to filter by

    Returns:
        DataFrame with columns:
        - 'ds': datetime
        - 'y': the metric value
        - 'is_anomaly': boolean indicating if point is anomalous
        - 'anomaly_state': the anomaly state (e.g., CONFIRMED, REJECTED)
        - 'anomaly_source_type': source of anomaly detection
    """
    columns = ["ds", "y", "is_anomaly", "anomaly_state", "anomaly_source_type"]

    if events_df is None or len(events_df) == 0:
        return pd.DataFrame(columns=columns)

    df = events_df.copy()

    # Filter by URN if provided
    if monitor_urn and "monitorUrn" in df.columns:
        df = df[df["monitorUrn"] == monitor_urn]
    if metric_cube_urn and "metricCubeUrn" in df.columns:
        df = df[df["metricCubeUrn"] == metric_cube_urn]

    if len(df) == 0:
        return pd.DataFrame(columns=columns)

    # Check required columns
    if TIMESTAMP_COLUMN not in df.columns or "measure" not in df.columns:
        return pd.DataFrame(columns=columns)

    # Build result DataFrame
    result = pd.DataFrame()
    result["ds"] = pd.to_datetime(df[TIMESTAMP_COLUMN], unit="ms")
    result["y"] = pd.to_numeric(df["measure"], errors="coerce")

    # Add anomaly information
    if "anomaly_state" in df.columns:
        result["is_anomaly"] = df["anomaly_state"].notna()
        result["anomaly_state"] = df["anomaly_state"].values
    else:
        result["is_anomaly"] = False
        result["anomaly_state"] = None

    if "anomaly_source_type" in df.columns:
        result["anomaly_source_type"] = df["anomaly_source_type"].values
    else:
        result["anomaly_source_type"] = None

    # Drop rows with null ds or y
    result = result.dropna(subset=["ds", "y"])

    # Sort by timestamp
    result = result.sort_values("ds").reset_index(drop=True)

    return result


def get_metric_cube_metadata(
    events_df: pd.DataFrame,
    monitor_urn: Optional[str] = None,
    metric_cube_urn: Optional[str] = None,
) -> Optional[MetricCubeMetadata]:
    """Get metadata for a specific metric cube.

    Args:
        events_df: DataFrame containing metric cube events
        monitor_urn: The monitor URN
        metric_cube_urn: The metric cube URN

    Returns:
        MetricCubeMetadata object, or None if not found
    """
    if events_df is None or len(events_df) == 0:
        return None

    df = events_df.copy()

    # Filter by URN
    if monitor_urn and "monitorUrn" in df.columns:
        df = df[df["monitorUrn"] == monitor_urn]
    elif metric_cube_urn and "metricCubeUrn" in df.columns:
        df = df[df["metricCubeUrn"] == metric_cube_urn]
    else:
        return None

    if len(df) == 0:
        return None

    # Use list_metric_cubes to get the metadata
    all_cubes = list_metric_cubes(df)
    if all_cubes:
        return all_cubes[0]

    return None


def filter_events_by_time(
    events_df: pd.DataFrame,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    """Filter metric cube events by time range.

    Args:
        events_df: DataFrame containing metric cube events
        start_time: Optional start time (inclusive)
        end_time: Optional end time (inclusive)

    Returns:
        Filtered DataFrame
    """
    if events_df is None or len(events_df) == 0:
        return events_df

    if TIMESTAMP_COLUMN not in events_df.columns:
        return events_df

    df = events_df.copy()

    if start_time:
        ts_ms = int(start_time.timestamp() * 1000)
        df = df[df[TIMESTAMP_COLUMN] >= ts_ms]

    if end_time:
        ts_ms = int(end_time.timestamp() * 1000)
        df = df[df[TIMESTAMP_COLUMN] <= ts_ms]

    return df


def get_time_range(
    events_df: pd.DataFrame,
) -> tuple[Optional[datetime], Optional[datetime]]:
    """Get the time range of events in the DataFrame.

    Args:
        events_df: DataFrame containing metric cube events

    Returns:
        Tuple of (start_time, end_time), or (None, None) if no events
    """
    if events_df is None or len(events_df) == 0:
        return None, None

    if TIMESTAMP_COLUMN not in events_df.columns:
        return None, None

    ts_series = events_df[TIMESTAMP_COLUMN].dropna()

    if len(ts_series) == 0:
        return None, None

    start_time = pd.to_datetime(ts_series.min(), unit="ms").to_pydatetime()
    end_time = pd.to_datetime(ts_series.max(), unit="ms").to_pydatetime()

    return start_time, end_time


def get_anomaly_counts(
    events_df: pd.DataFrame,
    monitor_urn: Optional[str] = None,
) -> dict[str, int]:
    """Get counts of anomaly states in the DataFrame.

    Args:
        events_df: DataFrame containing metric cube events
        monitor_urn: Optional filter by monitor URN

    Returns:
        Dictionary mapping anomaly state to count
    """
    if events_df is None or len(events_df) == 0:
        return {}

    df = events_df
    if monitor_urn and "monitorUrn" in df.columns:
        df = df[df["monitorUrn"] == monitor_urn]

    if "anomaly_state" not in df.columns:
        return {}

    # Only count non-null states
    states = df["anomaly_state"].dropna()
    if len(states) == 0:
        return {}

    return states.value_counts().to_dict()


@dataclass
class MonitoredAssertionMetadata:
    """Metadata about an assertion that has monitoring data."""

    assertion_urn: str
    monitor_urn: Optional[str]
    metric_cube_urn: Optional[str]
    point_count: int
    first_event: Optional[datetime]
    last_event: Optional[datetime]
    value_min: Optional[float]
    value_max: Optional[float]
    value_mean: Optional[float]
    anomaly_count: int
    monitor_status: Optional[str] = None  # "ACTIVE" or "PAUSED"
    # Assertion type info (populated from API enrichment)
    assertion_type: Optional[str] = None  # VOLUME, FIELD, SQL, FRESHNESS, etc.
    field_metric_type: Optional[str] = None  # For FIELD assertions: NULL_COUNT, etc.


def list_monitored_assertions(
    events_df: pd.DataFrame,
) -> list[MonitoredAssertionMetadata]:
    """List all unique assertions from metric cube data with metadata.

    This function groups metric cube events by assertionUrn to provide
    a list of assertions that have monitoring data available.

    Args:
        events_df: DataFrame containing metric cube events with assertionUrn column

    Returns:
        List of MonitoredAssertionMetadata objects with summary info for each assertion
    """
    if events_df is None or len(events_df) == 0:
        return []

    df = events_df.copy()

    # Must have assertionUrn column
    if "assertionUrn" not in df.columns:
        return []

    # Filter to rows with non-null assertionUrn
    df = df[df["assertionUrn"].notna()]

    if len(df) == 0:
        return []

    # Get unique assertion URNs
    assertion_urns = df["assertionUrn"].unique()

    results = []
    for assertion_urn in assertion_urns:
        assertion_df = df[df["assertionUrn"] == assertion_urn]

        # Get monitor URN (first one if multiple exist)
        monitor_urn = None
        if "monitorUrn" in assertion_df.columns:
            monitor_urns = assertion_df["monitorUrn"].dropna().unique()
            if len(monitor_urns) > 0:
                monitor_urn = monitor_urns[0]

        # Get metric cube URN
        metric_cube_urn = None
        if "metricCubeUrn" in assertion_df.columns:
            cube_urns = assertion_df["metricCubeUrn"].dropna().unique()
            if len(cube_urns) > 0:
                metric_cube_urn = cube_urns[0]
        elif monitor_urn:
            metric_cube_urn = build_metric_cube_urn(monitor_urn)

        # Time range
        first_event = None
        last_event = None
        if TIMESTAMP_COLUMN in assertion_df.columns:
            ts_series = assertion_df[TIMESTAMP_COLUMN].dropna()
            if len(ts_series) > 0:
                first_event = pd.to_datetime(ts_series.min(), unit="ms").to_pydatetime()
                last_event = pd.to_datetime(ts_series.max(), unit="ms").to_pydatetime()

        # Value statistics
        value_min = None
        value_max = None
        value_mean = None
        if "measure" in assertion_df.columns:
            values = assertion_df["measure"].dropna()
            if len(values) > 0:
                value_min = float(values.min())
                value_max = float(values.max())
                value_mean = float(values.mean())

        # Anomaly count
        anomaly_count = 0
        if "anomaly_state" in assertion_df.columns:
            anomaly_count = int(assertion_df["anomaly_state"].notna().sum())

        # Monitor status (if available from fetch enrichment)
        monitor_status = None
        if "monitor_status" in assertion_df.columns:
            statuses = assertion_df["monitor_status"].dropna().unique()
            if len(statuses) > 0:
                monitor_status = statuses[0]

        results.append(
            MonitoredAssertionMetadata(
                assertion_urn=assertion_urn,
                monitor_urn=monitor_urn,
                metric_cube_urn=metric_cube_urn,
                point_count=len(assertion_df),
                first_event=first_event,
                last_event=last_event,
                value_min=value_min,
                value_max=value_max,
                value_mean=value_mean,
                anomaly_count=anomaly_count,
                monitor_status=monitor_status,
            )
        )

    # Sort by most recent event
    results.sort(key=lambda x: x.last_event or datetime.min, reverse=True)

    return results


# Export all functions
__all__ = [
    "build_metric_cube_urn",
    "decode_metric_cube_urn",
    "MetricCubeMetadata",
    "MonitoredAssertionMetadata",
    "list_metric_cubes",
    "list_monitored_assertions",
    "extract_timeseries",
    "extract_timeseries_with_anomalies",
    "get_metric_cube_metadata",
    "filter_events_by_time",
    "get_time_range",
    "get_anomaly_counts",
]
