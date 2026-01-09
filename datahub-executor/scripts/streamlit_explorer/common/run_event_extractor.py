# ruff: noqa: INP001
"""
Run event extraction utilities.

This module provides functions to transform raw assertion run events
into time series format (ds, y) suitable for preprocessing and forecasting.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

# Mapping of assertion types to their primary value columns
ASSERTION_TYPE_VALUE_COLUMNS = {
    "FIELD": "event_result_nativeResults_Metric_Value",
    "VOLUME": "event_result_nativeResults_Metric_Value",
    "FRESHNESS": "event_result_nativeResults_Metric_Value",
}

# All columns that might contain metric values, in priority order
METRIC_VALUE_COLUMNS = [
    "event_result_nativeResults_Metric_Value",
    "event_result_metric_value",  # From GraphQL metric field
    "event_result_actualAggValue",
    "event_result_rowCount",  # Fallback for SQL assertions
    "event_result_nativeResults_Compared_Max_Value",
    "event_result_nativeResults_Compared_Min_Value",
    # Freshness-specific columns
    "event_result_nativeResults_Window_End_Time",
    "event_result_nativeResults_Window_Start_Time",
]

# Mapping of assertion types to their primary value columns
# Note: FRESHNESS uses Window times as fallback since it may not have Metric_Value
ASSERTION_TYPE_VALUE_COLUMNS_EXTENDED = {
    "FIELD": ["event_result_nativeResults_Metric_Value"],
    "VOLUME": ["event_result_nativeResults_Metric_Value", "event_result_rowCount"],
    "FRESHNESS": [
        "event_result_nativeResults_Metric_Value",
        "event_result_nativeResults_Window_End_Time",  # Point-in-time freshness
    ],
    "SQL": ["event_result_rowCount"],
}

# Timestamp column
TIMESTAMP_COLUMN = "timestampMillis"


def extract_entity_from_monitor_urn(monitor_urn: Optional[str]) -> Optional[str]:
    """Extract the entity URN from a monitor URN.

    Monitor URNs have the format: urn:li:monitor:(entity_urn,monitor_name)
    The entity_urn can contain nested parentheses and commas.

    Args:
        monitor_urn: The full monitor URN string, or None

    Returns:
        The entity URN (e.g., dataset URN) or None if parsing fails
    """
    if not monitor_urn or not monitor_urn.startswith("urn:li:monitor:"):
        return None

    try:
        # Find the opening parenthesis after "urn:li:monitor:"
        prefix = "urn:li:monitor:"
        if not monitor_urn.startswith(prefix):
            return None

        inner = monitor_urn[len(prefix) :]
        if not inner.startswith("(") or not inner.endswith(")"):
            return None

        # Remove outer parentheses
        content = inner[1:-1]

        # Find the last comma that separates entity_urn from monitor_name
        # by tracking parenthesis depth
        depth = 0
        last_comma_at_depth_0 = -1

        for i, char in enumerate(content):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                last_comma_at_depth_0 = i

        if last_comma_at_depth_0 == -1:
            return None

        entity_urn = content[:last_comma_at_depth_0]
        return entity_urn if entity_urn else None

    except Exception:
        return None


@dataclass
class AssertionMetadata:
    """Metadata about an assertion extracted from run events."""

    assertion_urn: str
    assertee_urn: Optional[str]
    assertion_type: Optional[str]
    metric_name: Optional[str]
    field_path: Optional[str]
    point_count: int
    first_event: Optional[datetime]
    last_event: Optional[datetime]
    value_min: Optional[float]
    value_max: Optional[float]
    value_mean: Optional[float]


@dataclass
class MonitorMetadata:
    """Metadata about a monitor extracted from timeseries events."""

    monitor_urn: str
    entity_urn: Optional[str]
    event_type: Optional[str]  # "anomaly" or "state"
    point_count: int
    first_event: Optional[datetime]
    last_event: Optional[datetime]
    anomaly_count: Optional[int]  # Number of anomaly events
    state_values: Optional[list[str]]  # Unique state values observed


def list_monitors(
    events_df: pd.DataFrame,
    aspect_name: str = "monitorAnomalyEvent",
) -> list[MonitorMetadata]:
    """List all unique monitors in the timeseries events with metadata.

    Args:
        events_df: DataFrame containing monitor events
        aspect_name: The aspect type (monitorAnomalyEvent or monitorTimeseriesState)

    Returns:
        List of MonitorMetadata objects with summary info for each monitor
    """
    if events_df is None or len(events_df) == 0:
        return []

    df = events_df.copy()

    # Get unique monitors
    if "monitorUrn" not in df.columns:
        return []

    monitors = []
    for urn in df["monitorUrn"].unique():
        if pd.isna(urn):
            continue

        monitor_df = df[df["monitorUrn"] == urn]

        # Extract entity URN from monitor URN structure
        # Monitor URN format: urn:li:monitor:(entity_urn,monitor_name)
        entity_urn = extract_entity_from_monitor_urn(urn)

        # Determine event type from aspect name
        event_type = "anomaly" if "anomaly" in aspect_name.lower() else "state"

        # Get timestamp range
        first_event = None
        last_event = None
        if TIMESTAMP_COLUMN in monitor_df.columns:
            ts_series = monitor_df[TIMESTAMP_COLUMN].dropna()
            if len(ts_series) > 0:
                first_event = pd.to_datetime(ts_series.min(), unit="ms").to_pydatetime()
                last_event = pd.to_datetime(ts_series.max(), unit="ms").to_pydatetime()

        # Count anomalies (for anomaly events)
        anomaly_count = None
        if "state" in monitor_df.columns and event_type == "anomaly":
            anomaly_mask = monitor_df["state"].str.upper() == "ANOMALY"
            anomaly_count = int(anomaly_mask.sum())

        # Get unique state values
        state_values = None
        if "state" in monitor_df.columns:
            unique_states = monitor_df["state"].dropna().unique().tolist()
            state_values = unique_states if unique_states else None

        monitors.append(
            MonitorMetadata(
                monitor_urn=urn,
                entity_urn=entity_urn,
                event_type=event_type,
                point_count=len(monitor_df),
                first_event=first_event,
                last_event=last_event,
                anomaly_count=anomaly_count,
                state_values=state_values,
            )
        )

    # Sort by point count (most events first)
    monitors.sort(key=lambda x: x.point_count, reverse=True)
    return monitors


def list_assertions(
    events_df: pd.DataFrame,
    assertion_type: Optional[str] = None,
) -> list[AssertionMetadata]:
    """List all unique assertions in the run events with metadata.

    Args:
        events_df: DataFrame containing run events
        assertion_type: Optional filter by assertion type (FIELD, VOLUME, FRESHNESS)

    Returns:
        List of AssertionMetadata objects with summary info for each assertion
    """
    if events_df is None or len(events_df) == 0:
        return []

    # Filter by assertion type if specified
    df = events_df.copy()
    if assertion_type and "event_result_assertion_type" in df.columns:
        df = df[df["event_result_assertion_type"] == assertion_type]

    if len(df) == 0:
        return []

    # Get unique assertions
    if "assertionUrn" not in df.columns:
        return []

    assertions = []
    for urn in df["assertionUrn"].unique():
        if pd.isna(urn):
            continue

        assertion_df = df[df["assertionUrn"] == urn]

        # Extract metadata
        assertee_urn = None
        if "asserteeUrn" in assertion_df.columns:
            assertee_urn = assertion_df["asserteeUrn"].iloc[0]
            if pd.isna(assertee_urn):
                assertee_urn = None

        assertion_type_val = None
        if "event_result_assertion_type" in assertion_df.columns:
            assertion_type_val = assertion_df["event_result_assertion_type"].iloc[0]
            if pd.isna(assertion_type_val):
                assertion_type_val = None

        metric_name = None
        if (
            "event_result_assertion_fieldAssertion_fieldMetricAssertion_metric"
            in assertion_df.columns
        ):
            metric_name = assertion_df[
                "event_result_assertion_fieldAssertion_fieldMetricAssertion_metric"
            ].iloc[0]
            if pd.isna(metric_name):
                metric_name = None

        field_path = None
        if (
            "event_result_assertion_fieldAssertion_fieldMetricAssertion_field_path"
            in assertion_df.columns
        ):
            field_path = assertion_df[
                "event_result_assertion_fieldAssertion_fieldMetricAssertion_field_path"
            ].iloc[0]
            if pd.isna(field_path):
                field_path = None

        # Get timestamp range
        first_event = None
        last_event = None
        if TIMESTAMP_COLUMN in assertion_df.columns:
            ts_series = assertion_df[TIMESTAMP_COLUMN].dropna()
            if len(ts_series) > 0:
                first_event = pd.to_datetime(ts_series.min(), unit="ms").to_pydatetime()
                last_event = pd.to_datetime(ts_series.max(), unit="ms").to_pydatetime()

        # Get value statistics
        value_min = None
        value_max = None
        value_mean = None
        value_col = ASSERTION_TYPE_VALUE_COLUMNS.get(
            str(assertion_type_val) if assertion_type_val else "",
            METRIC_VALUE_COLUMNS[0],
        )
        if value_col in assertion_df.columns:
            # Convert to numeric - values may be stored as strings
            value_series = pd.to_numeric(
                assertion_df[value_col], errors="coerce"
            ).dropna()
            if len(value_series) > 0:
                value_min = float(value_series.min())
                value_max = float(value_series.max())
                value_mean = float(value_series.mean())

        assertions.append(
            AssertionMetadata(
                assertion_urn=urn,
                assertee_urn=assertee_urn,
                assertion_type=assertion_type_val,
                metric_name=metric_name,
                field_path=field_path,
                point_count=len(assertion_df),
                first_event=first_event,
                last_event=last_event,
                value_min=value_min,
                value_max=value_max,
                value_mean=value_mean,
            )
        )

    # Sort by point count descending
    assertions.sort(key=lambda x: x.point_count, reverse=True)

    return assertions


def extract_timeseries(
    events_df: pd.DataFrame,
    assertion_urn: str,
    value_column: Optional[str] = None,
    exclude_errors: bool = False,
    exclude_init: bool = False,
    include_type_column: bool = False,
) -> pd.DataFrame:
    """Extract time series data for a specific assertion.

    Args:
        events_df: DataFrame containing run events
        assertion_urn: URN of the assertion to extract
        value_column: Optional override for the value column to use.
                     If None, auto-detected based on assertion type.
        exclude_errors: If True, exclude FAILURE and ERROR result types.
                       Keeps SUCCESS and INIT (unless exclude_init is also True).
        exclude_init: If True, exclude INIT result types. DEPRECATED: Use
                     include_type_column=True and let InitDataFilterTransformer
                     handle INIT filtering via type-aware preprocessing.
        include_type_column: If True, include the 'type' column with result types
                            (INIT, SUCCESS, FAILURE, ERROR) for type-aware preprocessing.
                            When True, exclude_init is ignored as INIT handling is
                            delegated to the preprocessing transformers.

    Result type filtering combinations:
        - include_type_column=True: All result types with 'type' column for preprocessing
        - exclude_errors=False, exclude_init=False: All result types (for browsing)
        - exclude_errors=True, exclude_init=False: SUCCESS + INIT (preprocessing default)
        - exclude_errors=True, exclude_init=True: SUCCESS only (exclude INIT)
        - exclude_errors=False, exclude_init=True: All except INIT

    Returns:
        DataFrame with columns 'ds' (datetime), 'y' (value), and optionally 'type',
        sorted by timestamp.
    """
    base_cols = ["ds", "y", "type"] if include_type_column else ["ds", "y"]

    if events_df is None or len(events_df) == 0:
        return pd.DataFrame(columns=base_cols)

    # Filter to the specific assertion
    if "assertionUrn" not in events_df.columns:
        return pd.DataFrame(columns=base_cols)

    df = events_df[events_df["assertionUrn"] == assertion_urn].copy()

    if len(df) == 0:
        return pd.DataFrame(columns=base_cols)

    result_type_col = "event_result_result_type"

    # Optionally exclude FAILURE and ERROR (keeps SUCCESS and INIT)
    if exclude_errors and result_type_col in df.columns:
        df = df[~df[result_type_col].isin(["FAILURE", "ERROR"])]

        if len(df) == 0:
            return pd.DataFrame(columns=base_cols)

    # Optionally exclude INIT results (skipped if include_type_column is True)
    # When type column is included, INIT handling is delegated to preprocessing
    if exclude_init and not include_type_column and result_type_col in df.columns:
        df = df[df[result_type_col] != "INIT"]

        if len(df) == 0:
            return pd.DataFrame(columns=base_cols)

    # Determine the value column
    if value_column is None:
        # Try to detect based on assertion type
        assertion_type = None
        if "event_result_assertion_type" in df.columns:
            assertion_type = df["event_result_assertion_type"].iloc[0]

        # Try type-specific columns first (with fallbacks)
        if assertion_type and assertion_type in ASSERTION_TYPE_VALUE_COLUMNS_EXTENDED:
            for candidate_col in ASSERTION_TYPE_VALUE_COLUMNS_EXTENDED[assertion_type]:
                if candidate_col in df.columns and df[candidate_col].notna().any():
                    value_column = candidate_col
                    break

        # If no type-specific column found, fall back to general list
        if value_column is None:
            for col in METRIC_VALUE_COLUMNS:
                if col in df.columns and df[col].notna().any():
                    value_column = col
                    break

    if value_column is None or value_column not in df.columns:
        return pd.DataFrame(columns=base_cols)

    # Check timestamp column exists
    if TIMESTAMP_COLUMN not in df.columns:
        return pd.DataFrame(columns=base_cols)

    # Build columns to extract
    cols_to_extract = [TIMESTAMP_COLUMN, value_column]
    if include_type_column and result_type_col in df.columns:
        cols_to_extract.append(result_type_col)

    # Extract time series
    ts_df = df[cols_to_extract].copy()
    ts_df["ds"] = pd.to_datetime(ts_df[TIMESTAMP_COLUMN], unit="ms")
    # Convert value column to numeric - values may be stored as strings
    ts_df["y"] = pd.to_numeric(ts_df[value_column], errors="coerce")

    # Add type column if requested
    if include_type_column and result_type_col in df.columns:
        ts_df["type"] = ts_df[result_type_col]
        result_cols = ["ds", "y", "type"]
    else:
        result_cols = ["ds", "y"]

    ts_df = ts_df[result_cols].dropna(subset=["ds", "y"])
    ts_df = ts_df.sort_values("ds").reset_index(drop=True)

    return ts_df


def extract_timeseries_with_bounds(
    events_df: pd.DataFrame,
    assertion_urn: str,
    exclude_errors: bool = False,
    exclude_init: bool = False,
    include_type_column: bool = False,
) -> pd.DataFrame:
    """Extract time series data with boundary values for a specific assertion.

    This includes the min/max bounds that were compared against, in addition
    to the actual metric value.

    Args:
        events_df: DataFrame containing run events
        assertion_urn: URN of the assertion to extract
        exclude_errors: If True, exclude FAILURE and ERROR result types.
                       Keeps SUCCESS and INIT (unless exclude_init is also True).
        exclude_init: If True, exclude INIT result types. DEPRECATED: Use
                     include_type_column=True and let InitDataFilterTransformer
                     handle INIT filtering via type-aware preprocessing.
        include_type_column: If True, include the 'type' column with result types
                            (INIT, SUCCESS, FAILURE, ERROR) for type-aware preprocessing.
                            When True, exclude_init is ignored as INIT handling is
                            delegated to the preprocessing transformers.

    Result type filtering combinations:
        - include_type_column=True: All result types with 'type' column for preprocessing
        - exclude_errors=False, exclude_init=False: All result types (for browsing)
        - exclude_errors=True, exclude_init=False: SUCCESS + INIT (preprocessing default)
        - exclude_errors=True, exclude_init=True: SUCCESS only (exclude INIT)
        - exclude_errors=False, exclude_init=True: All except INIT

    Returns:
        DataFrame with columns:
        - 'ds': datetime
        - 'y': the metric value
        - 'y_min': the min bound (if available)
        - 'y_max': the max bound (if available)
        - 'type': result type (if include_type_column=True)
    """
    base_cols = (
        ["ds", "y", "y_min", "y_max", "type"]
        if include_type_column
        else ["ds", "y", "y_min", "y_max"]
    )

    if events_df is None or len(events_df) == 0:
        return pd.DataFrame(columns=base_cols)

    # Filter to the specific assertion
    if "assertionUrn" not in events_df.columns:
        return pd.DataFrame(columns=base_cols)

    df = events_df[events_df["assertionUrn"] == assertion_urn].copy()

    if len(df) == 0:
        return pd.DataFrame(columns=base_cols)

    result_type_col = "event_result_result_type"

    # Optionally exclude FAILURE and ERROR (keeps SUCCESS and INIT)
    if exclude_errors and result_type_col in df.columns:
        df = df[~df[result_type_col].isin(["FAILURE", "ERROR"])]

        if len(df) == 0:
            return pd.DataFrame(columns=base_cols)

    # Optionally exclude INIT results (skipped if include_type_column is True)
    # When type column is included, INIT handling is delegated to preprocessing
    if exclude_init and not include_type_column and result_type_col in df.columns:
        df = df[df[result_type_col] != "INIT"]

        if len(df) == 0:
            return pd.DataFrame(columns=base_cols)

    # Check timestamp column exists
    if TIMESTAMP_COLUMN not in df.columns:
        return pd.DataFrame(columns=base_cols)

    # Build the result DataFrame
    result = pd.DataFrame()
    result["ds"] = pd.to_datetime(df[TIMESTAMP_COLUMN], unit="ms")

    # Main value - convert to numeric (values may be stored as strings)
    value_col = "event_result_nativeResults_Metric_Value"
    if value_col in df.columns:
        result["y"] = pd.to_numeric(df[value_col], errors="coerce")
    else:
        result["y"] = pd.NA

    # Min bound - convert to numeric
    min_col = "event_result_nativeResults_Compared_Min_Value"
    if min_col in df.columns:
        result["y_min"] = pd.to_numeric(df[min_col], errors="coerce")
    else:
        result["y_min"] = pd.NA

    # Max bound - convert to numeric
    max_col = "event_result_nativeResults_Compared_Max_Value"
    if max_col in df.columns:
        result["y_max"] = pd.to_numeric(df[max_col], errors="coerce")
    else:
        result["y_max"] = pd.NA

    # Add type column if requested
    if include_type_column and result_type_col in df.columns:
        result["type"] = df[result_type_col].values

    # Sort by timestamp and reset index
    result = result.sort_values("ds").reset_index(drop=True)

    return result


def get_assertion_metadata(
    events_df: pd.DataFrame,
    assertion_urn: str,
) -> Optional[AssertionMetadata]:
    """Get metadata for a specific assertion.

    Args:
        events_df: DataFrame containing run events
        assertion_urn: URN of the assertion

    Returns:
        AssertionMetadata object, or None if assertion not found
    """
    if events_df is None or len(events_df) == 0:
        return None

    if "assertionUrn" not in events_df.columns:
        return None

    df = events_df[events_df["assertionUrn"] == assertion_urn]

    if len(df) == 0:
        return None

    # Use list_assertions to get the metadata
    # This is a bit inefficient but ensures consistency
    all_assertions = list_assertions(df)

    if all_assertions:
        return all_assertions[0]

    return None


def filter_events_by_time(
    events_df: pd.DataFrame,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    """Filter events by time range.

    Args:
        events_df: DataFrame containing run events
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
        events_df: DataFrame containing run events

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


def get_assertion_types(events_df: pd.DataFrame) -> dict[str, int]:
    """Get counts of each assertion type in the DataFrame.

    Args:
        events_df: DataFrame containing run events

    Returns:
        Dictionary mapping assertion type to count
    """
    if events_df is None or len(events_df) == 0:
        return {}

    if "event_result_assertion_type" not in events_df.columns:
        return {}

    return events_df["event_result_assertion_type"].value_counts().to_dict()


def get_assertion_metrics(events_df: pd.DataFrame) -> dict[str, int]:
    """Get counts of each metric name in the DataFrame.

    Args:
        events_df: DataFrame containing run events

    Returns:
        Dictionary mapping metric name to count
    """
    if events_df is None or len(events_df) == 0:
        return {}

    metric_col = "event_result_assertion_fieldAssertion_fieldMetricAssertion_metric"
    if metric_col not in events_df.columns:
        return {}

    # Filter out null/empty values and get counts
    metrics = events_df[metric_col].dropna()
    if len(metrics) == 0:
        return {}

    return metrics.value_counts().to_dict()


def get_result_type_counts(
    events_df: pd.DataFrame,
    assertion_urn: Optional[str] = None,
) -> dict[str, int]:
    """Get counts of each result type in the DataFrame.

    Args:
        events_df: DataFrame containing run events
        assertion_urn: Optional filter by assertion URN

    Returns:
        Dictionary mapping result type to count (e.g., {'SUCCESS': 100, 'FAILURE': 5})
    """
    if events_df is None or len(events_df) == 0:
        return {}

    df = events_df
    if assertion_urn and "assertionUrn" in df.columns:
        df = df[df["assertionUrn"] == assertion_urn]

    result_type_col = "event_result_result_type"
    if result_type_col not in df.columns:
        return {}

    return df[result_type_col].value_counts().to_dict()
