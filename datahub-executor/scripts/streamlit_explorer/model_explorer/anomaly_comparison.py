# ruff: noqa: INP001
"""Anomaly Comparison page for training and comparing anomaly detection models."""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any, MutableMapping, Optional, cast

import numpy as np
import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
import streamlit as st

from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
    get_primary_anomaly_score,
)

from ..common import (
    _LOADED_TIMESERIES,
    DataLoader,
    _extract_hash_from_run_id,
    _hex_to_rgba,
    clear_all_auto_inference_v2_runs,
    delete_auto_inference_v2_run_complete,
    get_auto_inference_v2_runs,
    get_model_hyperparameters,
    init_explorer_state,
    load_auto_inference_v2_runs,
)
from ..common.cache_manager import EndpointCache
from ..common.preprocessing_keys import display_preprocessing_id
from .model_training import TrainingRun, _get_training_runs
from .observe_models_adapter import (
    _create_train_fn,
    get_anomaly_model_configs,
    is_global_forecasting_model,
    is_observe_models_available,
)

# =============================================================================
# Anomaly Detection Runs Storage
# =============================================================================

_STANDALONE_PREPROCESSING_KEY = "standalone_preprocessing_id"

_ANOMALY_RUNS_KEY = "anomaly_detection_runs"
_ANOMALY_TRAINING_STATUS_KEY = "anomaly_training_status"
_ANOMALY_SELECTED_RUNS_KEY = "anomaly_selected_runs"

_AUTO_V2_RUN_PREFIX = "auto_v2::"


@dataclass
class AnomalyDetectionRun:
    """Results from an anomaly detection run."""

    run_id: str
    anomaly_model_key: str
    anomaly_model_name: str
    train_df: pd.DataFrame  # Training data (for context in visualization)
    test_df: pd.DataFrame  # Test data (where anomalies are detected)
    detection_results: (
        pd.DataFrame
    )  # Results with is_anomaly, scores, bands (test only)
    model: object
    timestamp: datetime
    forecast_run_id: str | None = None  # Reference to forecasting model (if used)
    forecast_model_name: str | None = None  # Name of forecast model (if used)
    preprocessing_id: str | None = None  # Preprocessing ID for standalone models
    sensitivity_level: int | None = None  # Sensitivity level (1-10) used for training

    @property
    def display_name(self) -> str:
        """Display name combining anomaly model, forecast/preprocessing info, and hash ID."""
        hash_id = _extract_hash_from_run_id(self.run_id)
        hash_suffix = f" [{hash_id}]" if hash_id else ""

        if self.forecast_model_name:
            # Ensure forecast model name includes its hash if available
            forecast_display = self.forecast_model_name
            if self.forecast_run_id:
                forecast_hash = _extract_hash_from_run_id(self.forecast_run_id)
                if forecast_hash and f"[{forecast_hash}]" not in forecast_display:
                    # Hash not in forecast model name, append it
                    forecast_display = f"{forecast_display} [{forecast_hash}]"
            return f"{self.anomaly_model_name} (on {forecast_display}){hash_suffix}"
        elif self.preprocessing_id:
            # Standalone model - show preprocessing ID like forecast models do
            prep_display = display_preprocessing_id(self.preprocessing_id)
            return f"{self.anomaly_model_name} + {prep_display}{hash_suffix}"
        return f"{self.anomaly_model_name}{hash_suffix}"


def _get_anomaly_runs() -> dict[str, AnomalyDetectionRun]:
    """Get stored anomaly detection runs from session state."""
    if _ANOMALY_RUNS_KEY not in st.session_state:
        st.session_state[_ANOMALY_RUNS_KEY] = {}
    else:
        # Clear incompatible runs from old dataclass structure
        runs = st.session_state[_ANOMALY_RUNS_KEY]
        if runs:
            # Check if any run is missing the new train_df/test_df attributes
            first_run = next(iter(runs.values()), None)
            if first_run and not hasattr(first_run, "train_df"):
                st.session_state[_ANOMALY_RUNS_KEY] = {}
    return st.session_state[_ANOMALY_RUNS_KEY]


def _store_anomaly_run(run: AnomalyDetectionRun) -> None:
    """Store an anomaly detection run in session state and persist to disk."""
    runs = _get_anomaly_runs()
    runs[run.run_id] = run

    # Also persist to disk
    loader = DataLoader()
    hostname = st.session_state.get("current_hostname")
    if hostname:
        endpoint_cache = loader.cache.get_endpoint_cache(hostname)
        assertion_urn = st.session_state.get("selected_assertion_urn")
        endpoint_cache.save_anomaly_run(
            run_id=run.run_id,
            anomaly_model_key=run.anomaly_model_key,
            anomaly_model_name=run.anomaly_model_name,
            train_df=run.train_df,
            test_df=run.test_df,
            detection_results=run.detection_results,
            forecast_run_id=run.forecast_run_id,
            forecast_model_name=run.forecast_model_name,
            preprocessing_id=run.preprocessing_id,
            sensitivity_level=run.sensitivity_level,
            assertion_urn=assertion_urn,
        )


def _delete_anomaly_run(run_id: str, delete_from_cache: bool = True) -> None:
    """Delete an anomaly detection run from session state and optionally from disk cache.

    Args:
        run_id: The anomaly run ID to delete
        delete_from_cache: Whether to also delete from disk cache (default: True)
    """
    # Remove from session state
    runs = _get_anomaly_runs()
    if run_id in runs:
        del runs[run_id]

    # Delete from disk if requested
    if delete_from_cache:
        loader = DataLoader()
        hostname = st.session_state.get("current_hostname")
        if hostname:
            endpoint_cache = loader.cache.get_endpoint_cache(hostname)
            endpoint_cache.delete_anomaly_run(run_id)


def _clear_anomaly_runs() -> None:
    """Clear all stored anomaly detection runs from session state and disk."""
    # Delete from disk
    loader = DataLoader()
    hostname = st.session_state.get("current_hostname")
    if hostname:
        endpoint_cache = loader.cache.get_endpoint_cache(hostname)
        assertion_urn = st.session_state.get("selected_assertion_urn")
        cached_runs = endpoint_cache.list_saved_anomaly_runs(
            assertion_urn=assertion_urn
        )
        for run_meta in cached_runs:
            endpoint_cache.delete_anomaly_run(run_meta["run_id"])

    # Clear from session state
    st.session_state[_ANOMALY_RUNS_KEY] = {}


def _load_cached_anomaly_runs() -> None:
    """Load saved anomaly detection runs from disk into session state."""
    loader = DataLoader()
    hostname = st.session_state.get("current_hostname")
    if not hostname:
        return

    endpoint_cache = loader.cache.get_endpoint_cache(hostname)
    assertion_urn = st.session_state.get("selected_assertion_urn")

    # Get cached runs for this assertion
    cached_runs = endpoint_cache.list_saved_anomaly_runs(assertion_urn=assertion_urn)

    if not cached_runs:
        return

    # Ensure runs dict exists in session state
    if _ANOMALY_RUNS_KEY not in st.session_state:
        st.session_state[_ANOMALY_RUNS_KEY] = {}
    runs = st.session_state[_ANOMALY_RUNS_KEY]

    # Load each run into session state (if not already present)
    for run_meta in cached_runs:
        run_id = run_meta["run_id"]
        if run_id not in runs:
            # Load full run data
            run_data = endpoint_cache.load_anomaly_run(run_id)
            if run_data:
                run = AnomalyDetectionRun(
                    run_id=run_data["run_id"],
                    anomaly_model_key=run_data["anomaly_model_key"],
                    anomaly_model_name=run_data["anomaly_model_name"],
                    train_df=run_data["train_df"],
                    test_df=run_data["test_df"],
                    detection_results=run_data["detection_results"],
                    model=run_data["model"],  # Will be None (not persisted)
                    timestamp=run_data["timestamp"],
                    forecast_run_id=run_data.get("forecast_run_id"),
                    forecast_model_name=run_data.get("forecast_model_name"),
                    preprocessing_id=run_data.get("preprocessing_id"),
                    sensitivity_level=run_data.get("sensitivity_level"),
                )
                runs[run_id] = run


def _set_training_status(
    success_count: int = 0,
    errors: list[str] | None = None,
    skipped: list[str] | None = None,
    trained_run_ids: list[str] | None = None,
) -> None:
    """Set training status for display after rerun."""
    st.session_state[_ANOMALY_TRAINING_STATUS_KEY] = {
        "success_count": success_count,
        "errors": errors or [],
        "skipped": skipped or [],
        "trained_run_ids": trained_run_ids or [],
    }


def _get_and_clear_training_status() -> dict | None:
    """Get and clear training status (one-time display)."""
    status = st.session_state.pop(_ANOMALY_TRAINING_STATUS_KEY, None)
    return status


def _generate_anomaly_run_id(
    anomaly_key: str,
    forecast_id: str | None,
    preprocessing_id: str | None,
    sensitivity_level: int,
) -> str:
    """Generate a unique run ID for anomaly detection using deterministic hash.

    Args:
        anomaly_key: Key identifying the anomaly model type
        forecast_id: ID of the forecast model (if used), or None
        preprocessing_id: ID of the preprocessing (for standalone models), or None
        sensitivity_level: Sensitivity level (1-10) used for training

    Returns:
        Unique run ID in format: {anomaly_key}__{forecast_id_or_prep_id}__{hash}
    """
    # Use forecast_id if available, otherwise use preprocessing_id
    base_id = forecast_id if forecast_id else (preprocessing_id or "default")

    # Create deterministic hash from distinguishing factors
    hash_input = f"{anomaly_key}{base_id}{sensitivity_level}"
    hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()
    hash_short = hash_digest[:8]  # Use first 8 characters for brevity
    return f"{anomaly_key}__{base_id}__{hash_short}"


# =============================================================================
# Hyperparameters Extraction
# =============================================================================


def _render_hyperparameters_display(params: dict[str, object]) -> None:
    """Render hyperparameters in a nicely formatted display.

    Args:
        params: Dictionary of hyperparameters to display
    """
    param_lines = []
    for key, value in params.items():
        if isinstance(value, dict):
            param_lines.append(f"- **{key}**: `{value}`")
        elif isinstance(value, float):
            if np.isnan(value):
                param_lines.append(f"- **{key}**: `NaN`")
            else:
                param_lines.append(f"- **{key}**: `{value:.6g}`")
        else:
            param_lines.append(f"- **{key}**: `{value}`")
    st.markdown("\n".join(param_lines))


# =============================================================================
# Ground Truth Data
# =============================================================================


def _load_ground_truth_anomalies(hostname: str, assertion_urn: str) -> pd.DataFrame:
    """
    Load ground truth anomalies from monitor events.

    Returns DataFrame with columns: ds, status (confirmed/rejected/unconfirmed)
    """
    loader = DataLoader()

    try:
        # Load anomaly events from cache
        raw_events = loader.load_cached_events(
            hostname, entity_urn=assertion_urn, aspect_name="monitorAnomalyEvent"
        )

        if raw_events is None or raw_events.empty:
            return pd.DataFrame(columns=["ds", "status"])

        # Extract anomaly timestamps and their status
        anomalies = []
        for _, row in raw_events.iterrows():
            # Use source event timestamp (the metric/run event time), not the anomaly event time
            timestamp = (
                row.get("source_assertionMetric_timestampMillis")
                or row.get("source_sourceEventTimestampMillis")
                or row.get("timestampMillis")
            )
            if timestamp:
                # Determine status from state field
                state = row.get("state")
                if state == "CONFIRMED":
                    status = "confirmed"
                elif state == "REJECTED":
                    status = "rejected"
                else:
                    status = "unconfirmed"

                anomalies.append(
                    {
                        "ds": pd.to_datetime(timestamp, unit="ms"),
                        "status": status,
                    }
                )

        return pd.DataFrame(anomalies)

    except Exception:
        return pd.DataFrame(columns=["ds", "status"])


def _convert_ground_truth_for_training(ground_truth_df: pd.DataFrame) -> pd.DataFrame:
    """Convert ground truth to format expected by observe-models.

    observe-models expects:
    - ds: timestamp column
    - is_anomaly_gt: boolean (True = anomaly)

    Streamlit has:
    - ds: timestamp column
    - status: 'confirmed', 'rejected', 'unconfirmed'

    Only 'confirmed' status is treated as ground truth anomaly.
    """
    if ground_truth_df.empty:
        return pd.DataFrame(columns=["ds", "is_anomaly_gt"])

    result = ground_truth_df.copy()
    result["is_anomaly_gt"] = result["status"] == "confirmed"
    return result[["ds", "is_anomaly_gt"]]


# =============================================================================
# Cumulative View Transformation
# =============================================================================


def _was_differencing_applied() -> tuple[bool, int]:
    """Check if differencing was applied in preprocessing.

    Returns:
        Tuple of (was_applied, difference_order)
    """
    # Check applied_preprocessing_config in session state
    config = st.session_state.get("applied_preprocessing_config", {})
    darts_transformers = config.get("darts_transformers", [])

    for transformer in darts_transformers:
        if transformer.get("type") == "difference" and transformer.get("enabled"):
            return True, transformer.get("order", 1)

    # Also check UI state as fallback
    state = st.session_state.get("preprocessing_state")
    if state and hasattr(state, "differencing_enabled") and state.differencing_enabled:
        return True, getattr(state, "difference_order", 1)

    return False, 0


def _get_cumulative_anchor(train_df: pd.DataFrame, diff_order: int = 1) -> float | None:
    """Get the anchor value for cumulative transformation.

    Uses the original (pre-differenced) timeseries from session state.
    The anchor is the cumulative value that, when differencing was applied,
    produced the first value in train_df.

    For first-order differencing: y_diff[0] = y_orig[1] - y_orig[0]
    So the anchor is y_orig[0] (the value "before" the first differenced point).

    Args:
        train_df: Training dataframe with differenced values
        diff_order: Order of differencing (1 or 2)

    Returns:
        Anchor value, or None if original data not available
    """
    original_ts = st.session_state.get(_LOADED_TIMESERIES)
    if original_ts is None or original_ts.empty:
        return None

    # Ensure timestamps are comparable
    original_ts = original_ts.copy()
    original_ts["ds"] = pd.to_datetime(original_ts["ds"])
    train_start = pd.to_datetime(train_df["ds"].min())

    # Strategy: Find the closest timestamp in original data that is < train_start
    # We need the value BEFORE the first differenced point, not AT it
    original_before = original_ts[original_ts["ds"] < train_start]

    if original_before.empty:
        # Fallback: use first original value
        # This may happen if preprocessing truncated early data
        return float(original_ts["y"].iloc[0])

    # For diff_order=1: anchor is the value just before the differenced series starts
    # For diff_order=2: we'd need 2 anchor values (more complex, handle as extension)
    if diff_order == 1:
        return float(original_before["y"].iloc[-1])
    else:
        # For order 2, return the last value but note this is simplified
        # Full support for order 2 would require storing 2 anchor values
        return float(original_before["y"].iloc[-1])


def _transform_to_cumulative(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    detection_results: pd.DataFrame,
    anchor_value: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Transform differenced data back to cumulative space.

    Algorithm: Each cumulative point is anchored to the previous actual
    cumulative value. This preserves the anomaly detection relationship.

    For each point i:
        y_cum[i] = y_cum[i-1] + y_diff[i]
        yhat_cum[i] = y_cum[i-1] + yhat_diff[i]
        bands_cum[i] = y_cum[i-1] + bands_diff[i]

    This ensures that if y_diff > upper_diff (anomaly in differenced space),
    then y_cum > upper_cum (anomaly preserved in cumulative space).

    Uses canonical names detection_band_upper/detection_band_lower. Accepts
    detection_upper/detection_lower (observe-models) and normalizes once.

    Args:
        train_df: Training data with differenced y values
        test_df: Test data with differenced y values
        detection_results: Detection results with yhat and detection band columns
        anchor_value: Starting value for cumulative sum

    Returns:
        Tuple of (cumulative_train_df, cumulative_test_df, cumulative_results_df)
    """
    # Normalize: ensure detection_band_* so rest of function uses one convention
    dr = detection_results.copy()
    if (
        "detection_band_upper" not in dr.columns
        and "detection_upper" in dr.columns
        and "detection_band_lower" not in dr.columns
        and "detection_lower" in dr.columns
    ):
        dr["detection_band_upper"] = dr["detection_upper"]
        dr["detection_band_lower"] = dr["detection_lower"]

    # Transform training data: simple cumsum from anchor
    cum_train = train_df.copy()
    cum_train["y"] = anchor_value + train_df["y"].fillna(0).cumsum()

    # Last cumulative training value becomes anchor for test period
    train_end_value = cum_train["y"].iloc[-1] if len(cum_train) > 0 else anchor_value

    # Transform test data (actual values): simple cumsum from training end
    cum_test = test_df.copy()
    cum_test["y"] = train_end_value + test_df["y"].fillna(0).cumsum()

    # Transform detection results with recursive anchoring
    cum_results = dr.copy()
    n = len(cum_results)

    if n == 0:
        return cum_train, cum_test, cum_results

    # Build cumulative y values recursively
    y_diff = dr["y"].fillna(0).values
    y_cum = np.zeros(n)
    y_cum[0] = train_end_value + y_diff[0]
    for i in range(1, n):
        y_cum[i] = y_cum[i - 1] + y_diff[i]
    cum_results["y"] = y_cum

    # Forecast: anchor each prediction to previous actual cumulative
    if "yhat" in cum_results.columns:
        yhat_diff = dr["yhat"].fillna(0).values
        yhat_cum = np.zeros(n)
        yhat_cum[0] = train_end_value + yhat_diff[0]
        for i in range(1, n):
            yhat_cum[i] = y_cum[i - 1] + yhat_diff[i]
        cum_results["yhat"] = yhat_cum

    # Bands: anchor each band to previous actual cumulative (canonical names only)
    if (
        "detection_band_upper" in cum_results.columns
        and "detection_band_lower" in cum_results.columns
    ):
        upper_diff = dr["detection_band_upper"].fillna(0).values
        lower_diff = dr["detection_band_lower"].fillna(0).values

        upper_cum = np.zeros(n)
        lower_cum = np.zeros(n)

        upper_cum[0] = train_end_value + upper_diff[0]
        lower_cum[0] = train_end_value + lower_diff[0]

        for i in range(1, n):
            upper_cum[i] = y_cum[i - 1] + upper_diff[i]
            lower_cum[i] = y_cum[i - 1] + lower_diff[i]

        cum_results["detection_band_upper"] = upper_cum
        cum_results["detection_band_lower"] = lower_cum

    return cum_train, cum_test, cum_results


# =============================================================================
# Visualization
# =============================================================================

# Standard colors for anomaly detection visualizations
# (No color differentiation needed since each combination gets its own chart)
_DETECTION_BAND_COLOR = "#9467bd"  # Purple for detection bands
_MODEL_DETECTED_COLOR = "#d62728"  # Red for model-detected anomalies


def _add_detection_bands_to_figure(
    fig: go.Figure,
    results_df: pd.DataFrame,
    scatter_type: type,
    show_detection_bands: bool = True,
) -> None:
    """Add detection bands (filled area + upper/lower lines) to a figure.

    Uses canonical names detection_band_upper/detection_band_lower. Accepts
    detection_upper/detection_lower (observe-models) and normalizes once.

    Args:
        fig: Plotly figure to add bands to
        results_df: DataFrame with detection band columns
        scatter_type: Scatter type to use (go.Scatter or go.Scattergl)
        show_detection_bands: Whether to show bands (if available)
    """
    # Normalize: ensure detection_band_* so rest of function uses one convention
    df = results_df.copy()
    if (
        "detection_band_upper" not in df.columns
        and "detection_upper" in df.columns
        and "detection_band_lower" not in df.columns
        and "detection_lower" in df.columns
    ):
        df["detection_band_upper"] = df["detection_upper"]
        df["detection_band_lower"] = df["detection_lower"]

    has_detection_bands = (
        "detection_band_upper" in df.columns and "detection_band_lower" in df.columns
    )
    if not show_detection_bands or not has_detection_bands:
        return

    # Add filled band area
    fig.add_trace(
        go.Scatter(
            x=pd.concat([df["ds"], df["ds"][::-1]]),
            y=pd.concat(
                [
                    df["detection_band_upper"],
                    df["detection_band_lower"][::-1],
                ]
            ),
            fill="toself",
            fillcolor=f"rgba{_hex_to_rgba(_DETECTION_BAND_COLOR, 0.15)}",
            line=dict(color="rgba(255,255,255,0)"),
            hoverinfo="skip",
            showlegend=True,
            name="Detection Band",
        )
    )

    # Add upper and lower band lines (dashed)
    fig.add_trace(
        scatter_type(
            x=df["ds"],
            y=df["detection_band_upper"],
            mode="lines",
            name="Upper Band",
            line=dict(color=_DETECTION_BAND_COLOR, width=1, dash="dash"),
            showlegend=False,
        )
    )
    fig.add_trace(
        scatter_type(
            x=df["ds"],
            y=df["detection_band_lower"],
            mode="lines",
            name="Lower Band",
            line=dict(color=_DETECTION_BAND_COLOR, width=1, dash="dash"),
            showlegend=False,
        )
    )


def _get_scatter_type(total_size: int) -> type:
    """Determine scatter type based on data size for performance optimization.

    Args:
        total_size: Total number of data points

    Returns:
        go.Scattergl for large datasets (>5000), go.Scatter otherwise
    """
    return go.Scattergl if total_size > 5000 else go.Scatter


def _add_training_data_trace(
    fig: go.Figure,
    train_df: pd.DataFrame,
    scatter_type: type,
) -> None:
    """Add training data trace to figure (context - lighter color).

    Args:
        fig: Plotly figure to add trace to
        train_df: Training DataFrame with 'ds' and 'y' columns
        scatter_type: Scatter type to use (go.Scatter or go.Scattergl)
    """
    fig.add_trace(
        scatter_type(
            x=train_df["ds"],
            y=train_df["y"],
            mode="lines",
            name="Training Data",
            line=dict(color="#a0a0a0", width=1.5),  # Gray for training context
            opacity=0.7,
        )
    )


def _add_test_data_trace(
    fig: go.Figure,
    test_df: pd.DataFrame,
    scatter_type: type,
) -> None:
    """Add test data trace to figure (main time series - blue).

    Args:
        fig: Plotly figure to add trace to
        test_df: Test DataFrame with 'ds' and 'y' columns
        scatter_type: Scatter type to use (go.Scatter or go.Scattergl)
    """
    fig.add_trace(
        scatter_type(
            x=test_df["ds"],
            y=test_df["y"],
            mode="lines",
            name="Test Data",
            line=dict(color="#1f77b4", width=2),  # Blue for test data
        )
    )


def _add_forecast_trace(
    fig: go.Figure,
    results_df: pd.DataFrame,
    scatter_type: type,
    name: str = "Forecast",
) -> None:
    """Add forecast prediction line trace to figure.

    Args:
        fig: Plotly figure to add trace to
        results_df: Results DataFrame with 'ds' and 'yhat' columns
        scatter_type: Scatter type to use (go.Scatter or go.Scattergl)
        name: Name for the trace (default: "Forecast")
    """
    if "yhat" in results_df.columns:
        fig.add_trace(
            scatter_type(
                x=results_df["ds"],
                y=results_df["yhat"],
                mode="lines",
                name=name,
                line=dict(color="#ff7f0e", width=2),  # Orange
            )
        )


def _add_split_marker(
    fig: go.Figure,
    split_time: pd.Timestamp | datetime,
    label: str = "Split",
) -> None:
    """Add vertical line and annotation marking a time split.

    Args:
        fig: Plotly figure to add marker to
        split_time: Timestamp for the split
        label: Text label for the split (default: "Split")
    """
    fig.add_shape(
        type="line",
        x0=split_time,
        x1=split_time,
        y0=0,
        y1=1,
        yref="paper",
        line=dict(color="gray", width=1, dash="dot"),
    )
    fig.add_annotation(
        x=split_time,
        y=1,
        yref="paper",
        text=label,
        showarrow=False,
        yanchor="bottom",
        font=dict(size=10, color="gray"),
    )


def _add_train_test_split_marker(
    fig: go.Figure,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    label: str = "Train/Test Split",
) -> None:
    """Add vertical line and annotation marking train/test split.

    Args:
        fig: Plotly figure to add marker to
        train_df: Training DataFrame
        test_df: Test DataFrame
        label: Text label for the split (default: "Train/Test Split")
    """
    if len(train_df) > 0 and len(test_df) > 0:
        split_time = test_df["ds"].min()
        _add_split_marker(fig, split_time, label)


def _add_model_detected_anomalies(
    fig: go.Figure,
    results_df: pd.DataFrame,
    test_df: pd.DataFrame,
    show_model_detected: bool = True,
) -> None:
    """Add model-detected anomalies to figure.

    Always uses actual test data values (y), never forecast values (yhat).

    Args:
        fig: Plotly figure to add anomalies to
        results_df: Results DataFrame with 'ds' and 'is_anomaly' columns
        test_df: Test DataFrame with 'ds' and 'y' columns (for actual values)
        show_model_detected: Whether to show detected anomalies
    """
    if not show_model_detected or "is_anomaly" not in results_df.columns:
        return

    if "ds" not in results_df.columns or "ds" not in test_df.columns:
        return

    # Handle different is_anomaly formats (boolean, int 1/0, string "True"/"False")
    try:
        # Convert to boolean: handle True/False, 1/0, "True"/"False", etc.
        is_anomaly_series = results_df["is_anomaly"]
        if is_anomaly_series.dtype == "object":
            # String or mixed types - handle "True"/"False" explicitly before numeric
            s_str = is_anomaly_series.astype(str).str.strip().str.lower()
            truthy = {"true", "1", "yes"}
            falsy = {"false", "0", "no", ""}
            from_str = s_str.map(
                lambda v: True if v in truthy else (False if v in falsy else None)
            )
            numeric_vals = pd.to_numeric(is_anomaly_series, errors="coerce")
            numeric_bool = numeric_vals.fillna(0).astype(bool)
            is_anomaly_bool = from_str.where(from_str.notna(), numeric_bool).astype(
                bool
            )
        else:
            # Numeric or boolean - convert to bool
            is_anomaly_bool = is_anomaly_series.astype(bool)

        flagged = results_df[is_anomaly_bool].copy()
    except Exception:
        # Fallback: try direct comparison
        try:
            flagged = results_df[results_df["is_anomaly"] == True].copy()  # noqa: E712
        except Exception:
            flagged = results_df.iloc[0:0]

    if len(flagged) == 0:
        return

    # Ensure both DataFrames have 'ds' as datetime for proper merging
    # Check that test_df has 'y' column
    if "y" not in test_df.columns:
        return

    # Select only 'ds' from flagged to avoid column conflicts during merge
    flagged_ds = pd.to_datetime(flagged["ds"])
    test_df_for_merge = test_df.copy()
    test_df_for_merge["ds"] = pd.to_datetime(test_df_for_merge["ds"])

    # Verify test_df_for_merge has the required columns before merging
    if "y" not in test_df_for_merge.columns or "ds" not in test_df_for_merge.columns:
        return

    # Create a simple DataFrame with just ds from flagged for merging
    flagged_for_merge = pd.DataFrame({"ds": flagged_ds})

    # Merge on datetime, using left join to keep all flagged points
    # Then filter to only points where we successfully matched test data
    try:
        merged = flagged_for_merge.merge(
            test_df_for_merge[["ds", "y"]],
            on="ds",
            how="left",
        )
    except Exception:
        return

    # Verify merge succeeded and 'y' column exists
    if "y" not in merged.columns:
        return

    # Filter to only points where we successfully matched test data
    merged = merged[merged["y"].notna()]

    if len(merged) == 0:
        return

    # Use only y from test data (never use yhat)
    fig.add_trace(
        go.Scatter(
            x=merged["ds"],
            y=merged["y"],
            mode="markers",
            name="Model Detected",
            marker=dict(
                size=10,
                color=_MODEL_DETECTED_COLOR,
                symbol="x",
                line=dict(width=2),
            ),
            legendgroup="model_detected",
        )
    )


def _format_model_key_from_dict(name: object, version: object) -> str:
    if not isinstance(name, str) or not name:
        return ""
    if "@" in name:
        return name
    if isinstance(version, str) and version:
        return f"{name}@{version}"
    return name


def _render_auto_inference_v2_anomaly_chart(
    *,
    title: str,
    train_df: pd.DataFrame,
    prediction_df: Optional[pd.DataFrame],
    show_detection_bands: bool = True,
    eval_train_df: Optional[pd.DataFrame] = None,
    eval_df: Optional[pd.DataFrame] = None,
    evaluation_detection_results: Optional[pd.DataFrame] = None,
) -> go.Figure:
    """
    Render an anomaly-style chart for inference_v2 runs.

    When evaluation data is available (eval_train_df, eval_df, evaluation_detection_results),
    uses train/test split visualization matching training runs. Otherwise falls back to
    history/future visualization for backwards compatibility.

    Args:
        title: Chart title
        train_df: Full training DataFrame (used when eval data not available)
        prediction_df: Future prediction DataFrame (used when eval data not available)
        show_detection_bands: Whether to show detection bands
        eval_train_df: Training split from train/test split (preferred)
        eval_df: Test split from train/test split (preferred)
        evaluation_detection_results: Detection results for test split (preferred)
    """
    fig = go.Figure()

    # Prefer evaluation split data for train/test visualization (matches training runs)
    if (
        isinstance(eval_train_df, pd.DataFrame)
        and len(eval_train_df) > 0
        and isinstance(eval_df, pd.DataFrame)
        and len(eval_df) > 0
        and isinstance(evaluation_detection_results, pd.DataFrame)
        and len(evaluation_detection_results) > 0
    ):
        # Use train/test split visualization (matches training runs)
        # Ensure evaluation_detection_results has ds before sorting (avoids KeyError when only timestamp_ms exists)
        results_df = evaluation_detection_results.copy()
        if "timestamp_ms" in results_df.columns and "ds" not in results_df.columns:
            results_df["ds"] = pd.to_datetime(results_df["timestamp_ms"], unit="ms")
        if "ds" not in results_df.columns and "ds" in eval_df.columns:
            results_df = results_df.reset_index(drop=True)
            eval_df_reset = eval_df.reset_index(drop=True)
            if len(results_df) == len(eval_df_reset):
                results_df["ds"] = eval_df_reset["ds"].values

        train_sorted = (
            eval_train_df.sort_values("ds").copy()
            if "ds" in eval_train_df.columns
            else eval_train_df.copy()
        )
        test_sorted = (
            eval_df.sort_values("ds").copy()
            if "ds" in eval_df.columns
            else eval_df.copy()
        )
        results_df = (
            results_df.sort_values("ds").copy()
            if "ds" in results_df.columns
            else results_df.copy()
        )

        total_size = len(train_sorted) + len(test_sorted)
        scatter_type = _get_scatter_type(total_size)

        # Add training and test data traces
        _add_training_data_trace(fig, train_sorted, scatter_type)
        _add_test_data_trace(fig, test_sorted, scatter_type)

        # Add train/test split marker
        _add_train_test_split_marker(fig, train_sorted, test_sorted)

        # Add forecast prediction line (if available) - TEST PERIOD ONLY
        _add_forecast_trace(fig, results_df, scatter_type)

        # Detection bands (if available and enabled) - TEST PERIOD ONLY
        _add_detection_bands_to_figure(
            fig=fig,
            results_df=results_df,
            scatter_type=scatter_type,
            show_detection_bands=show_detection_bands,
        )

        # Model-detected anomalies on the test period (when provided)
        _add_model_detected_anomalies(
            fig, results_df, test_sorted, show_model_detected=True
        )

    else:
        # Fallback to history/future visualization (backwards compatibility)
        train_sorted = (
            train_df.sort_values("ds").copy() if "ds" in train_df.columns else train_df
        )

        total_size = len(train_sorted) + (
            len(prediction_df) if isinstance(prediction_df, pd.DataFrame) else 0
        )
        scatter_type = _get_scatter_type(total_size)

        # Training history (context) - all available historical data
        _add_training_data_trace(fig, train_sorted, scatter_type)

        pred = None
        if isinstance(prediction_df, pd.DataFrame) and len(prediction_df) > 0:
            pred = prediction_df.copy()
            if "timestamp_ms" in pred.columns and "ds" not in pred.columns:
                pred["ds"] = pd.to_datetime(pred["timestamp_ms"], unit="ms")
            pred = pred.sort_values("ds").copy() if "ds" in pred.columns else pred

            has_bands = (
                "detection_band_lower" in pred.columns
                and "detection_band_upper" in pred.columns
                and pred["detection_band_lower"].notna().any()
                and pred["detection_band_upper"].notna().any()
            )

            if "yhat" not in pred.columns and has_bands:
                pred["yhat"] = (
                    pred["detection_band_lower"] + pred["detection_band_upper"]
                ) / 2.0

            if "yhat" in pred.columns and pred["yhat"].notna().any():
                fig.add_trace(
                    scatter_type(
                        x=pred["ds"],
                        y=pred["yhat"],
                        mode="lines",
                        name="Forecast (future)",
                        line=dict(color="#ff7f0e", width=2),
                    )
                )

            if show_detection_bands and has_bands:
                fig.add_trace(
                    go.Scatter(
                        x=pd.concat([pred["ds"], pred["ds"][::-1]]),
                        y=pd.concat(
                            [
                                pred["detection_band_upper"],
                                pred["detection_band_lower"][::-1],
                            ]
                        ),
                        fill="toself",
                        fillcolor=f"rgba{_hex_to_rgba(_DETECTION_BAND_COLOR, 0.15)}",
                        line=dict(color="rgba(255,255,255,0)"),
                        hoverinfo="skip",
                        showlegend=True,
                        name="Detection Band (future)",
                    )
                )

            if "is_anomaly" in pred.columns and pred["is_anomaly"].notna().any():
                try:
                    flagged = pred[pred["is_anomaly"] == True]  # noqa: E712
                except Exception:
                    flagged = pred.iloc[0:0]
                if len(flagged) > 0 and "yhat" in flagged.columns:
                    # In fallback mode (history/future), use yhat since we don't have actual test data
                    fig.add_trace(
                        go.Scatter(
                            x=flagged["ds"],
                            y=flagged["yhat"],
                            mode="markers",
                            name="Model Detected",
                            marker=dict(
                                size=10,
                                color=_MODEL_DETECTED_COLOR,
                                symbol="x",
                                line=dict(width=2),
                            ),
                        )
                    )

        # Boundary between history and future (at end of training data)
        if len(train_sorted) > 0:
            split_time = train_sorted["ds"].max()
            _add_split_marker(fig, split_time, "History/Future")

    fig.update_layout(
        title=dict(text=title, pad=dict(t=10)),
        xaxis_title="Time",
        yaxis_title="Value",
        height=500,
        margin=dict(l=10, r=10, t=80, b=10),
        hovermode="x unified",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
        ),
    )
    return fig


def _load_auto_inference_v2_run_metadatas(
    *, loader: DataLoader, hostname: str, assertion_urn: str
) -> list[dict[str, Any]]:
    """List saved inference_v2 runs for the current assertion.

    Also loads runs into session state using centralized function.
    """
    # Load runs into session state
    load_auto_inference_v2_runs(
        hostname=hostname,
        assertion_urn=assertion_urn,
        session_state=cast(MutableMapping[str, Any], st.session_state),
    )

    # Get runs from session state and return metadata
    auto_runs = get_auto_inference_v2_runs(
        cast(MutableMapping[str, Any], st.session_state)
    )
    return [
        {"run_id": run_id, **run_data.get("metadata", {})}
        for run_id, run_data in auto_runs.items()
        if isinstance(run_data, dict)
    ]


def _format_auto_run_label(auto_meta: dict[str, Any]) -> str:
    created_at = str(auto_meta.get("created_at") or "")
    when = created_at[:16] if created_at else ""
    preproc = display_preprocessing_id(str(auto_meta.get("preprocessing_id") or ""))
    bits = [b for b in (when, preproc) if b]
    return "  ·  ".join(bits) if bits else str(auto_meta.get("run_id") or "")


def _render_auto_inference_v2_runs_list(
    *, loader: DataLoader, hostname: str, assertion_urn: str
) -> dict[str, dict[str, Any]]:
    """
    Render inference_v2 runs as a collapsed list (no charts),
    focusing on anomaly-model details.

    Returns:
        Mapping of prefixed run_id -> lightweight info dict for summary/viz.
    """
    endpoint_cache = loader.cache.get_endpoint_cache(hostname)
    auto_metas = _load_auto_inference_v2_run_metadatas(
        loader=loader, hostname=hostname, assertion_urn=assertion_urn
    )
    if not auto_metas:
        return {}

    st.subheader("inference_v2 Runs")

    # Clear all button
    col_clear, col_spacer = st.columns([2, 2])
    with col_clear:
        if st.button("Clear inference_v2 Runs", key="anomaly_compare__clear_auto_runs"):
            current_assertion_urn = st.session_state.get("current_assertion_urn")
            deleted_count = clear_all_auto_inference_v2_runs(
                hostname=hostname,
                assertion_urn=current_assertion_urn,
                session_state=cast(MutableMapping[str, Any], st.session_state),
            )
            if deleted_count > 0:
                st.rerun()

    info_by_id: dict[str, dict[str, Any]] = {}
    for meta in auto_metas:
        run_id = meta.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            continue

        label = _format_auto_run_label(meta)
        hash_id: str = _extract_hash_from_run_id(run_id) or ""
        expander_title = (
            f"inference_v2 + {label} [{hash_id}]"
            if hash_id
            else f"inference_v2 + {label}"
        )
        with st.expander(expander_title, expanded=False):
            # Delete single run
            col_del, col_del_spacer = st.columns([2, 8])
            with col_del:
                if st.button(
                    "Delete",
                    key=f"anomaly_compare__delete_auto__{run_id}",
                    use_container_width=True,
                ):
                    delete_auto_inference_v2_run_complete(
                        run_id=run_id,
                        hostname=hostname,
                        session_state=cast(MutableMapping[str, Any], st.session_state),
                    )
                    st.rerun()

            # Load model config (includes anomaly model + scores). This also loads train/pred,
            # but run counts are typically small in Streamlit usage.
            run_data = endpoint_cache.load_auto_inference_v2_run(run_id)
            if not isinstance(run_data, dict):
                st.caption("Failed to load run details.")
                continue

            model_cfg: Any = run_data.get("model_config") or {}
            # Extract detected_count from evaluation detection results (test period)
            # This matches how training runs count anomalies on the test split
            evaluation_detection_results: Any = run_data.get(
                "evaluation_detection_results"
            )
            detected_count: Optional[int] = None
            if (
                isinstance(evaluation_detection_results, pd.DataFrame)
                and "is_anomaly" in evaluation_detection_results.columns
            ):
                try:
                    detected_count = int(
                        pd.to_numeric(
                            evaluation_detection_results["is_anomaly"], errors="coerce"
                        )
                        .fillna(0)
                        .astype(int)
                        .sum()
                    )
                except Exception:
                    detected_count = None
            # Fallback to prediction_df if evaluation_detection_results not available
            # (for backwards compatibility with old saved runs)
            if detected_count is None:
                prediction_df: Any = run_data.get("prediction_df")
                if (
                    isinstance(prediction_df, pd.DataFrame)
                    and "is_anomaly" in prediction_df.columns
                ):
                    try:
                        detected_count = int(
                            pd.to_numeric(prediction_df["is_anomaly"], errors="coerce")
                            .fillna(0)
                            .astype(int)
                            .sum()
                        )
                    except Exception:
                        detected_count = None
            if isinstance(model_cfg, dict):
                anomaly_key = _format_model_key_from_dict(
                    model_cfg.get("anomaly_model_name"),
                    model_cfg.get("anomaly_model_version"),
                )
                forecast_key = _format_model_key_from_dict(
                    model_cfg.get("forecast_model_name"),
                    model_cfg.get("forecast_model_version"),
                )
                anomaly_score = model_cfg.get("anomaly_score")
                forecast_score = model_cfg.get("forecast_score")

                st.markdown("**Selected models**")
                if anomaly_key:
                    st.caption(f"Anomaly: `{anomaly_key}`")
                if forecast_key:
                    st.caption(f"Forecast: `{forecast_key}`")

                st.markdown("**Scores**")
                if isinstance(anomaly_score, (int, float)):
                    st.caption(f"Anomaly score: `{float(anomaly_score):.4f}`")
                else:
                    st.caption("Anomaly score: (unavailable)")
                if isinstance(forecast_score, (int, float)):
                    st.caption(f"Forecast score: `{float(forecast_score):.4f}`")

                run_meta = run_data.get("metadata") or {}
                sensitivity_level = run_meta.get("sensitivity_level")
                if isinstance(sensitivity_level, (int, float)):
                    sens_int = int(sensitivity_level)
                    sens_desc = (
                        "Aggressive"
                        if sens_int >= 7
                        else "Balanced"
                        if sens_int >= 4
                        else "Conservative"
                    )
                    st.markdown("**Sensitivity**")
                    st.caption(f"Sensitivity level: `{sens_int}` ({sens_desc})")

                info_by_id[f"{_AUTO_V2_RUN_PREFIX}{run_id}"] = {
                    "run_id": run_id,
                    "preprocessing_id": str(meta.get("preprocessing_id") or ""),
                    "created_at": str(meta.get("created_at") or ""),
                    "anomaly_model_key": anomaly_key,
                    "forecast_model_key": forecast_key,
                    "anomaly_score": float(anomaly_score)
                    if isinstance(anomaly_score, (int, float))
                    else None,
                    "forecast_score": float(forecast_score)
                    if isinstance(forecast_score, (int, float))
                    else None,
                    "detected_count": detected_count,
                }
            else:
                st.caption("Model config unavailable.")

    return info_by_id


def _render_anomaly_chart(
    run: AnomalyDetectionRun,
    ground_truth_df: pd.DataFrame,
    show_detection_bands: bool = True,
    show_ground_truth: bool = True,
    show_model_detected: bool = True,
    ground_truth_filter: list[str] | None = None,
    override_train_df: pd.DataFrame | None = None,
    override_test_df: pd.DataFrame | None = None,
    override_results_df: pd.DataFrame | None = None,
    is_cumulative_view: bool = False,
) -> go.Figure:
    """Render anomaly detection chart with optional layers.

    Shows training data as context (lighter) and test data where detection occurs.
    Each combination (forecast + anomaly model) gets its own chart.

    Args:
        run: The anomaly detection run with train/test data and results
        ground_truth_df: Ground truth anomaly labels
        show_detection_bands: Whether to show detection bands
        show_ground_truth: Whether to show ground truth markers
        show_model_detected: Whether to show model-detected anomaly markers
        ground_truth_filter: Filter for ground truth status types
        override_train_df: Optional transformed training data (for cumulative view)
        override_test_df: Optional transformed test data (for cumulative view)
        override_results_df: Optional transformed results (for cumulative view)
        is_cumulative_view: Whether this is a cumulative view (affects title/labels)

    Returns:
        Plotly figure object
    """
    # Use overrides if provided (for cumulative view)
    train_df = override_train_df if override_train_df is not None else run.train_df
    test_df = override_test_df if override_test_df is not None else run.test_df
    results_df = (
        override_results_df
        if override_results_df is not None
        else run.detection_results
    )

    fig = go.Figure()

    # Determine total data size for performance optimization
    total_size = len(train_df) + len(test_df)
    scatter_type = _get_scatter_type(total_size)

    # Add training and test data traces
    _add_training_data_trace(fig, train_df, scatter_type)
    _add_test_data_trace(fig, test_df, scatter_type)

    # Add train/test split marker
    _add_train_test_split_marker(fig, train_df, test_df)

    # Add forecast prediction line (if available) - TEST PERIOD ONLY
    _add_forecast_trace(fig, results_df, scatter_type)

    # Detection bands (if available and enabled) - TEST PERIOD ONLY
    # observe-models returns: detection_lower, detection_upper
    _add_detection_bands_to_figure(
        fig=fig,
        results_df=results_df,
        scatter_type=scatter_type,
        show_detection_bands=show_detection_bands,
    )

    # Ground truth anomalies (if enabled) - TEST PERIOD ONLY
    if show_ground_truth and not ground_truth_df.empty:
        # Filter ground truth to test period only
        test_start = test_df["ds"].min()
        test_end = test_df["ds"].max()
        gt_in_test = ground_truth_df[
            (ground_truth_df["ds"] >= test_start) & (ground_truth_df["ds"] <= test_end)
        ]

        filter_statuses = ground_truth_filter or [
            "confirmed",
            "rejected",
            "unconfirmed",
        ]

        # Marker styles for each status
        status_styles = {
            "confirmed": {
                "color": "#d62728",
                "symbol": "triangle-down",
                "name": "GT: Confirmed",
            },
            "rejected": {
                "color": "#7f7f7f",
                "symbol": "circle-open",
                "name": "GT: Rejected",
            },
            "unconfirmed": {
                "color": "#ff7f0e",
                "symbol": "diamond",
                "name": "GT: Unconfirmed",
            },
        }

        for status in filter_statuses:
            if status in status_styles:
                style = status_styles[status]
                status_df = gt_in_test[gt_in_test["status"] == status]

                if not status_df.empty:
                    # Match ground truth timestamps with test data values
                    merged = status_df.merge(test_df[["ds", "y"]], on="ds", how="left")

                    fig.add_trace(
                        go.Scatter(
                            x=merged["ds"],
                            y=merged["y"],
                            mode="markers",
                            name=style["name"],
                            marker=dict(
                                color=style["color"],
                                size=12,
                                symbol=style["symbol"],
                                line=dict(width=2, color=style["color"]),
                            ),
                            legendgroup="ground_truth",
                        )
                    )

    # Model-detected anomalies (if enabled)
    _add_model_detected_anomalies(fig, results_df, test_df, show_model_detected)

    # Update layout with view mode indicator
    title_suffix = " (Cumulative)" if is_cumulative_view else ""
    fig.update_layout(
        title=run.display_name + title_suffix,
        xaxis_title="Time",
        yaxis_title="Cumulative Value" if is_cumulative_view else "Value",
        height=500,
        margin=dict(l=10, r=10, t=80, b=10),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
        ),
        hovermode="x unified",
    )

    return fig


def _compute_classification_metrics(
    model_detected_df: pd.DataFrame,
    ground_truth_df: pd.DataFrame,
    tolerance_seconds: int = 3600,
) -> dict[str, float]:
    """
    Compute classification metrics comparing model detections to ground truth.

    Args:
        model_detected_df: DataFrame with 'ds' column for model-detected anomalies
        ground_truth_df: DataFrame with 'ds' and 'status' columns
        tolerance_seconds: Time tolerance for matching detections to ground truth

    Returns:
        Dictionary with TP, FP, FN counts and precision, recall, F1
    """
    # Filter ground truth to only confirmed anomalies (these are "true" anomalies)
    true_anomalies = ground_truth_df[ground_truth_df["status"] == "confirmed"]

    if model_detected_df.empty:
        return {
            "TP": 0,
            "FP": 0,
            "FN": len(true_anomalies),
            "Precision": 0.0,
            "Recall": 0.0,
            "F1": 0.0,
        }

    if true_anomalies.empty:
        return {
            "TP": 0,
            "FP": len(model_detected_df),
            "FN": 0,
            "Precision": 0.0,
            "Recall": float("nan"),
            "F1": float("nan"),
        }

    # Convert timestamps for comparison
    model_times = pd.to_datetime(model_detected_df["ds"])
    true_times = pd.to_datetime(true_anomalies["ds"])

    tolerance = pd.Timedelta(seconds=tolerance_seconds)

    # Count true positives and false negatives
    tp = 0
    matched_model_indices: set[int] = set()

    for true_time in true_times:
        # Check if any model detection is within tolerance
        for i, model_time in enumerate(model_times):
            if abs(model_time - true_time) <= tolerance:
                tp += 1
                matched_model_indices.add(i)
                break

    fn = len(true_anomalies) - tp
    fp = len(model_detected_df) - len(matched_model_indices)

    # Calculate metrics
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    return {
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "Precision": precision,
        "Recall": recall,
        "F1": f1,
    }


def _render_anomaly_performance_summary(
    anomaly_runs: dict[str, AnomalyDetectionRun],
    ground_truth_df: pd.DataFrame,
    *,
    auto_run_infos: dict[str, dict[str, Any]] | None = None,
) -> None:
    """Render a performance summary table for all anomaly detection runs.

    Args:
        anomaly_runs: Dictionary of anomaly detection runs
        ground_truth_df: Ground truth anomaly DataFrame with 'ds' and 'status' columns
    """
    if not anomaly_runs and not auto_run_infos:
        return

    st.subheader("Performance Summary")

    # Build comparison data
    table_data: list[dict[str, object]] = []

    for _run_id, run in anomaly_runs.items():
        # Get detection count
        detected_count = 0
        if (
            run.detection_results is not None
            and "is_anomaly" in run.detection_results.columns
        ):
            detected_count = int(run.detection_results["is_anomaly"].sum())

        # Primary anomaly score: same logic as inference_v2 (GROUND_TRUTH then CLEAN_TEST
        # then best_model_score) so manual and auto runs are comparable.
        anomaly_score = (
            get_primary_anomaly_score(run.model) if run.model is not None else None
        )
        anomaly_score_display = (
            f"{anomaly_score:.4f}" if anomaly_score is not None else "N/A"
        )

        # Compute classification metrics if ground truth is available
        precision_display = "N/A"
        recall_display = "N/A"
        f1_display = "N/A"
        f1_raw = float("nan")

        if (
            ground_truth_df is not None
            and not ground_truth_df.empty
            and run.detection_results is not None
            and "is_anomaly" in run.detection_results.columns
        ):
            model_detected = run.detection_results[
                run.detection_results["is_anomaly"] == True  # noqa: E712
            ]
            if not model_detected.empty or detected_count == 0:
                metrics = _compute_classification_metrics(
                    model_detected_df=model_detected,
                    ground_truth_df=ground_truth_df,
                )
                if not np.isnan(metrics["Precision"]):
                    precision_display = f"{metrics['Precision']:.2%}"
                if not np.isnan(metrics["Recall"]):
                    recall_display = f"{metrics['Recall']:.2%}"
                if not np.isnan(metrics["F1"]):
                    f1_display = f"{metrics['F1']:.2%}"
                    f1_raw = metrics["F1"]

        # Determine base model info
        if run.forecast_model_name:
            base_info = run.forecast_model_name
            # Ensure hash is included - extract from forecast_run_id if not in display name
            if run.forecast_run_id:
                forecast_hash = _extract_hash_from_run_id(run.forecast_run_id)
                if forecast_hash and f"[{forecast_hash}]" not in base_info:
                    # Hash not in display name, append it
                    base_info = f"{base_info} [{forecast_hash}]"
        elif run.preprocessing_id:
            base_info = display_preprocessing_id(run.preprocessing_id)
        else:
            base_info = "N/A"

        # Extract hash ID for display
        hash_id = _extract_hash_from_run_id(run.run_id)
        anomaly_model_display = (
            f"{run.anomaly_model_name} [{hash_id}]"
            if hash_id
            else run.anomaly_model_name
        )

        table_data.append(
            {
                "_run_id": _run_id,  # Store run_id for reference
                "Type": "Manual",
                "Anomaly Model": anomaly_model_display,
                "Base": base_info,
                "Detected": detected_count,
                "Anomaly Score": anomaly_score_display,
                "Precision": precision_display,
                "Recall": recall_display,
                "F1": f1_display,
                "_f1_raw": f1_raw,  # For sorting
                "_anomaly_score_raw": anomaly_score
                if anomaly_score is not None
                else float("-inf"),
            }
        )

    # Auto (inference_v2) runs: include as rows (focus on anomaly model + anomaly_score).
    for prefixed_id, info in (auto_run_infos or {}).items():
        anomaly_score = info.get("anomaly_score")
        preprocessing_id = str(info.get("preprocessing_id") or "")
        detected_count_raw = info.get("detected_count")
        auto_detected_count: Optional[int] = (
            int(detected_count_raw)
            if isinstance(detected_count_raw, (int, float))
            else None
        )
        hash_id = _extract_hash_from_run_id(str(info.get("run_id") or ""))
        # Display format: inference_v2 + {preprocessing} [{hash}] (consistent with Time Series Comparison)
        prep_display = display_preprocessing_id(preprocessing_id)
        if hash_id:
            anomaly_model_display = f"inference_v2 + {prep_display} [{hash_id}]"
        else:
            anomaly_model_display = (
                f"inference_v2 + {prep_display}" if prep_display else "inference_v2"
            )

        table_data.append(
            {
                "_run_id": prefixed_id,
                "Type": "inference_v2",
                "Anomaly Model": anomaly_model_display,
                "Base": display_preprocessing_id(preprocessing_id),
                "Detected": auto_detected_count
                if auto_detected_count is not None
                else "N/A",
                "Anomaly Score": f"{float(anomaly_score):.4f}"
                if isinstance(anomaly_score, (int, float))
                else "N/A",
                "Precision": "N/A",
                "Recall": "N/A",
                "F1": "N/A",
                "_f1_raw": float("nan"),
                "_anomaly_score_raw": float(anomaly_score)
                if isinstance(anomaly_score, (int, float))
                else float("-inf"),
            }
        )

    # Sort by F1 score (highest/best first), then by grid score
    def _sort_key(x: dict[str, object]) -> tuple[float, float]:
        f1_val = float(x["_f1_raw"])  # type: ignore[arg-type]
        anomaly_val = float(x["_anomaly_score_raw"])  # type: ignore[arg-type]
        return (
            -f1_val if not np.isnan(f1_val) else float("inf"),
            -anomaly_val,
        )

    table_data.sort(key=_sort_key)

    # Remove raw sorting columns and internal run_id before display
    for row in table_data:
        del row["_f1_raw"]
        del row["_anomaly_score_raw"]
        del row["_run_id"]

    summary_df = pd.DataFrame(table_data)

    st.dataframe(
        summary_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Type": st.column_config.TextColumn(
                "Type",
                width="small",
                help="Run type (Manual vs Inference_v2)",
            ),
            "Anomaly Model": st.column_config.TextColumn(
                "Anomaly Model",
                width="medium",
                help="Anomaly detection model name",
            ),
            "Base": st.column_config.TextColumn(
                "Base",
                width="medium",
                help="Forecast model or preprocessing configuration",
            ),
            "Detected": st.column_config.NumberColumn(
                "Detected",
                width="small",
                help="Number of anomalies detected",
            ),
            "Anomaly Score": st.column_config.TextColumn(
                "Anomaly Score",
                width="small",
                help="Best anomaly score from tuning (Manual) or Inference_v2 evaluation. Higher is better.",
            ),
            "Precision": st.column_config.TextColumn(
                "Precision",
                width="small",
                help="TP / (TP + FP) - higher is better",
            ),
            "Recall": st.column_config.TextColumn(
                "Recall",
                width="small",
                help="TP / (TP + FN) - higher is better",
            ),
            "F1": st.column_config.TextColumn(
                "F1",
                width="small",
                help="Harmonic mean of Precision and Recall - higher is better",
            ),
        },
    )

    # Show legend for metrics
    with st.expander("Metrics Guide", expanded=False):
        st.markdown("""
        - **Detected**: Number of anomalies detected by the model in the test period
        - **Anomaly Score**: Best anomaly score from hyperparameter optimization (Manual) or Inference_v2 evaluation
        - **Precision**: Of all detected anomalies, how many were correct? (TP / (TP + FP))
        - **Recall**: Of all actual anomalies, how many were detected? (TP / (TP + FN))
        - **F1**: Harmonic mean of Precision and Recall (balanced measure)

        *Note: Classification metrics require ground truth anomaly labels to compute.*
        """)


# =============================================================================
# Main Page
# =============================================================================


def render_anomaly_comparison_page() -> None:
    """Render the Anomaly Comparison page."""
    st.header("Anomaly Comparison")

    init_explorer_state()

    # Load cached anomaly runs from disk
    _load_cached_anomaly_runs()

    # Navigation - Back to Time Series Comparison / Forward to Model Override
    col_nav_back, col_nav_spacer, col_nav_forward = st.columns([1, 3, 1])
    with col_nav_back:
        if st.button("← Time Series Comparison"):
            from . import timeseries_comparison_page

            st.switch_page(timeseries_comparison_page)
    with col_nav_forward:
        if st.button("Model Override →"):
            from . import model_override_page

            st.switch_page(model_override_page)

    st.markdown("---")

    # Check if observe-models is available
    if not is_observe_models_available():
        st.warning(
            "The observe-models package is not installed. "
            "Anomaly detection models require this package."
        )
        return

    # Get grid search setting from session state (defaults to True)
    grid_search_enabled = st.session_state.get("anomaly_enable_grid_search", True)

    # Get training runs (forecasting models) - needed for forecast-based anomaly models
    all_training_runs = _get_training_runs()

    # Filter to only observe-models forecast models
    # Forecast-based anomaly detection models require DartsBaseForecastModel instances
    compatible_training_runs = {
        k: v for k, v in all_training_runs.items() if v.is_observe_model
    }

    # Note: We don't return early if no forecast models - standalone anomaly models
    # (like DeepSVDD) don't require them

    # Common resources needed for preprocessing
    loader = DataLoader()
    hostname = st.session_state.get("current_hostname")
    auto_infos: dict[str, dict[str, Any]] = {}
    endpoint_cache: Optional[EndpointCache] = None
    if hostname:
        endpoint_cache = loader.cache.get_endpoint_cache(hostname)

    # Two-column layout
    col_config, col_results = st.columns([1, 1])

    with col_config:
        st.subheader("Configuration")

        # Anomaly sensitivity control (before model selection so configs use correct sensitivity)
        st.markdown("**Anomaly Model Sensitivity**")
        sensitivity_level = st.slider(
            "Anomaly Sensitivity Level",
            min_value=1,
            max_value=10,
            value=st.session_state.get("anomaly_sensitivity_level", 5),
            step=1,
            help=(
                "Sensitivity level (1-10) for anomaly detection model behavior adjustment. "
                "This controls the contamination parameter for anomaly models only. "
                "Forecast models use their own sensitivity from when they were trained. "
                "Higher values (7-10): More aggressive detection (higher contamination = more anomalies detected). "
                "Lower values (1-3): More conservative detection (lower contamination = fewer anomalies detected). "
                "Medium values (4-6): Balanced defaults."
            ),
            key="anomaly_sensitivity_level",
        )
        st.caption(
            f"Anomaly Sensitivity: {sensitivity_level} "
            f"({'Aggressive' if sensitivity_level >= 7 else 'Balanced' if sensitivity_level >= 4 else 'Conservative'})"
        )
        st.markdown("---")

        # Load configs with current sensitivity
        anomaly_configs = get_anomaly_model_configs(
            enable_grid_search=grid_search_enabled,
            sensitivity_level=sensitivity_level,
        )
        if not anomaly_configs:
            st.warning("No anomaly detection models available in the registry.")
            return

        # 1. Anomaly model selection FIRST - use checkboxes for visibility
        anomaly_options = list(anomaly_configs.keys())
        anomaly_labels = {k: v.name for k, v in anomaly_configs.items()}

        st.write("**Anomaly Detection Models**")
        selected_anomaly_keys: list[str] = []
        for akey in anomaly_options:
            config = anomaly_configs[akey]
            # Show indicator if model requires forecast
            label = anomaly_labels.get(akey, akey)
            if config.requires_forecast_model:
                label += " ⚡"  # Indicator for forecast-required
            # Default first one to checked
            default_checked = akey == anomaly_options[0] if anomaly_options else False
            is_checked = st.checkbox(
                label,
                value=default_checked,
                key=f"anomaly_cb_{akey}",
            )
            if is_checked:
                selected_anomaly_keys.append(akey)

        if not selected_anomaly_keys:
            st.warning("Please select at least one anomaly detection model.")
            return

        # Check which selected models require forecast
        models_requiring_forecast = [
            k
            for k in selected_anomaly_keys
            if anomaly_configs[k].requires_forecast_model
        ]
        models_standalone = [
            k
            for k in selected_anomaly_keys
            if not anomaly_configs[k].requires_forecast_model
        ]

        # Check if any selected model requires GlobalForecastingModel
        models_requiring_global = [
            k
            for k in models_requiring_forecast
            if anomaly_configs[k].requires_global_forecast_model
        ]

        # 2. Forecasting model selection - only if any selected model requires it
        selected_forecast_ids: list[str] = []
        if models_requiring_forecast:
            st.markdown("---")
            st.write("**Forecasting Model(s)** ⚡ _required for selected models_")
            st.caption(
                f"Models requiring forecast: {', '.join(anomaly_configs[k].name for k in models_requiring_forecast)}"
            )

            # Show warning if some models require GlobalForecastingModel
            if models_requiring_global:
                st.caption(
                    f"⚠️ Models requiring GlobalForecastingModel (NBEATS, Chronos2): "
                    f"{', '.join(anomaly_configs[k].name for k in models_requiring_global)}"
                )

            forecast_options = list(compatible_training_runs.keys())
            forecast_labels = {
                k: v.display_name for k, v in compatible_training_runs.items()
            }

            if not forecast_options:
                st.warning(
                    "No compatible forecast models available. "
                    "Train observe-models forecast models first, or select only standalone anomaly models."
                )
            else:
                for fid in forecast_options:
                    # Check if this forecast model is a GlobalForecastingModel
                    # Use TrainingRun.registry_key directly - no string parsing
                    training_run = compatible_training_runs[fid]
                    is_global = is_global_forecasting_model(training_run.registry_key)

                    # Build label with compatibility indicator
                    label = forecast_labels.get(fid, fid)
                    if models_requiring_global and not is_global:
                        label += " ⚠️"  # Warn about incompatibility

                    # Default first compatible model to checked
                    # If there are models requiring global, prefer global models
                    if models_requiring_global:
                        first_global = next(
                            (
                                f
                                for f in forecast_options
                                if is_global_forecasting_model(
                                    compatible_training_runs[f].registry_key
                                )
                            ),
                            None,
                        )
                        default_checked = fid == first_global
                    else:
                        default_checked = fid == forecast_options[0]

                    is_checked = st.checkbox(
                        label,
                        value=default_checked,
                        key=f"forecast_cb_{fid}",
                    )
                    if is_checked:
                        selected_forecast_ids.append(fid)

                if not selected_forecast_ids:
                    st.warning(
                        "Please select at least one forecasting model for the ⚡ models."
                    )

                # Warn if incompatible combinations are selected
                if models_requiring_global and selected_forecast_ids:
                    incompatible_forecasts = [
                        f
                        for f in selected_forecast_ids
                        if not is_global_forecasting_model(
                            compatible_training_runs[f].registry_key
                        )
                    ]
                    if incompatible_forecasts:
                        st.warning(
                            f"⚠️ {', '.join(forecast_labels.get(f, f) for f in incompatible_forecasts)} "
                            f"are not compatible with {', '.join(anomaly_configs[k].name for k in models_requiring_global)}. "
                            f"These combinations will be skipped."
                        )

        # 3. Preprocessing selection for standalone models
        selected_preprocessing_id: str | None = None
        train_split_pct: int = 80  # Default train/test split

        if models_standalone:
            st.markdown("---")
            st.write("**Preprocessing Data** 📊 _required for standalone models_")
            st.caption(
                f"Standalone models: {', '.join(anomaly_configs[k].name for k in models_standalone)}"
            )

            preprocessing_options: list[str] = []
            preprocessing_labels: dict[str, str] = {}

            # Add current session preprocessing if available
            current_preprocessed_df = st.session_state.get("preprocessed_df")
            if current_preprocessed_df is not None and len(current_preprocessed_df) > 0:
                preprocessing_options.append("__current__")
                preprocessing_labels["__current__"] = (
                    f"Current Session ({len(current_preprocessed_df)} rows)"
                )

            # Add saved preprocessings (filtered by current assertion)
            if endpoint_cache is not None:
                assertion_urn = st.session_state.get("selected_assertion_urn")
                saved_preprocessings = endpoint_cache.list_saved_preprocessings(
                    assertion_urn=assertion_urn
                )
                for item in saved_preprocessings:
                    pid = item["preprocessing_id"]
                    row_count = item.get("row_count", 0)
                    preprocessing_options.append(pid)
                    short = display_preprocessing_id(pid, metadata=item.get("metadata"))
                    preprocessing_labels[pid] = f"{short} ({row_count} rows)"

            if not preprocessing_options:
                st.warning(
                    "No preprocessing available. "
                    "Go to the Preprocessing page to create and save a preprocessing configuration."
                )
            else:
                # Restore previous selection if valid
                prev_selection = st.session_state.get(_STANDALONE_PREPROCESSING_KEY)
                default_idx = 0
                if prev_selection and prev_selection in preprocessing_options:
                    default_idx = preprocessing_options.index(prev_selection)

                selected_preprocessing_id = st.selectbox(
                    "Select Preprocessing",
                    options=preprocessing_options,
                    index=default_idx,
                    format_func=lambda x: preprocessing_labels.get(x, x),
                    key="standalone_preprocessing_select",
                )

                # Store selection
                st.session_state[_STANDALONE_PREPROCESSING_KEY] = (
                    selected_preprocessing_id
                )

                # Train/test split ratio
                train_split_pct = st.slider(
                    "Train/Test Split",
                    min_value=50,
                    max_value=95,
                    value=80,
                    step=5,
                    format="%d%%",
                    key="standalone_train_split",
                    help="Percentage of data to use for training",
                )

        st.markdown("---")

        # 4. Manual run options
        st.write("**Manual Run Options**")
        enable_grid_search = st.checkbox(
            "Enable Hyperparameter Grid Search",
            value=True,
            key="anomaly_enable_grid_search",
            help="Use automatic grid search to find optimal hyperparameters. "
            "Uses ground truth anomalies for scoring when available.",
        )

        st.markdown("---")

        # Calculate what will be trained (considering compatibility)
        standalone_count = len(models_standalone)

        # Count compatible forecast-anomaly combinations
        compatible_combos = 0
        skipped_combos = 0
        for fid in selected_forecast_ids:
            is_global = is_global_forecasting_model(
                compatible_training_runs[fid].registry_key
            )
            for akey in models_requiring_forecast:
                if (
                    anomaly_configs[akey].requires_global_forecast_model
                    and not is_global
                ):
                    skipped_combos += 1
                else:
                    compatible_combos += 1

        total_to_train = compatible_combos + standalone_count

        if total_to_train == 0:
            st.warning(
                "No models to train. Select models and forecast models (if required) above."
            )
        else:
            info_parts = []
            if standalone_count > 0:
                info_parts.append(f"- {standalone_count} standalone model(s)")
            if compatible_combos > 0:
                info_parts.append(f"- {compatible_combos} compatible combination(s)")
            if skipped_combos > 0:
                info_parts.append(
                    f"- {skipped_combos} incompatible combination(s) will be skipped"
                )

            st.info(
                f"⏱️ Will train **{total_to_train}** model(s):\n"
                + "\n".join(info_parts)
                + "\n\nManual runs may take some time."
            )

        # Train button
        train_clicked = st.button(
            "Train Anomaly Models",
            type="primary",
            use_container_width=True,
        )

        if train_clicked:
            if total_to_train == 0:
                st.warning("No models to train. Select models above.")
            else:
                trained_count = 0
                failed_models: list[str] = []
                skipped_models: list[str] = []
                trained_run_ids: list[str] = []  # Track for auto-selection

                # Load ground truth for grid search (if enabled)
                ground_truth_for_training = None
                if enable_grid_search:
                    gt_hostname = str(
                        st.session_state.get("current_hostname", "") or ""
                    )
                    gt_assertion_urn = str(
                        st.session_state.get("selected_assertion_urn", "") or ""
                    )
                    gt_raw = _load_ground_truth_anomalies(gt_hostname, gt_assertion_urn)
                    if not gt_raw.empty:
                        ground_truth_for_training = _convert_ground_truth_for_training(
                            gt_raw
                        )
                        n_confirmed = int(
                            ground_truth_for_training["is_anomaly_gt"].sum()
                        )
                        st.info(
                            f"Grid search enabled with {n_confirmed} confirmed "
                            f"anomalies for scoring"
                        )
                    else:
                        st.warning(
                            "No ground truth anomalies found. "
                            "Grid search will minimize false positives."
                        )

                # Build training tasks: (forecast_id or None, anomaly_key)
                training_tasks: list[tuple[str | None, str]] = []

                # Add standalone models (no forecast needed)
                for akey in models_standalone:
                    training_tasks.append((None, akey))

                # Add forecast-based combinations (only compatible ones)
                for fid in selected_forecast_ids:
                    fid_run = compatible_training_runs[fid]
                    is_global = is_global_forecasting_model(fid_run.registry_key)
                    for akey in models_requiring_forecast:
                        # Skip incompatible combinations
                        if (
                            anomaly_configs[akey].requires_global_forecast_model
                            and not is_global
                        ):
                            skipped_models.append(
                                f"{anomaly_configs[akey].name} + {fid_run.display_name}"
                            )
                            continue
                        training_tasks.append((fid, akey))

                # Use a spinner with progress for immediate feedback
                with st.spinner(f"Running {len(training_tasks)} manual model(s)..."):
                    progress_bar = st.progress(0, text="Starting manual run...")

                    for i, (forecast_id, anomaly_key) in enumerate(training_tasks):
                        anomaly_config = anomaly_configs[anomaly_key]

                        # Get forecast run if needed
                        forecast_run: TrainingRun | None = (
                            compatible_training_runs.get(forecast_id)
                            if forecast_id
                            else None
                        )

                        # Progress text - check if forecast model needs re-training
                        needs_retrain = (
                            forecast_run
                            and forecast_run.model is None
                            and forecast_run.registry_key
                        )
                        if forecast_run:
                            if needs_retrain:
                                progress_text = (
                                    f"Re-running {forecast_run.display_name} (cached) + "
                                    f"{anomaly_config.name}... ({i + 1}/{len(training_tasks)})"
                                )
                            else:
                                progress_text = (
                                    f"Running {anomaly_config.name} on "
                                    f"{forecast_run.display_name}... ({i + 1}/{len(training_tasks)})"
                                )
                        else:
                            progress_text = (
                                f"Running {anomaly_config.name} (standalone)... "
                                f"({i + 1}/{len(training_tasks)})"
                            )

                        progress_bar.progress(
                            (i + 0.5) / len(training_tasks),
                            text=progress_text,
                        )

                        try:
                            if (
                                anomaly_config.train_fn is None
                                or anomaly_config.detect_fn is None
                            ):
                                failed_models.append(
                                    f"{anomaly_config.name}: Missing train/detect functions"
                                )
                                continue

                            # Get training data - from forecast run or from preprocessing
                            if forecast_run:
                                forecast_model = forecast_run.model
                                train_df = forecast_run.train_df
                                test_df = forecast_run.test_df

                                # If forecast model is None (loaded from cache), re-train it
                                if forecast_model is None:
                                    if forecast_run.registry_key:
                                        # Re-train the forecast model using cached train_df
                                        retrain_fn = _create_train_fn(
                                            forecast_run.registry_key
                                        )
                                        forecast_model = retrain_fn(train_df)
                                        # Store re-trained model in session state run
                                        forecast_run.model = forecast_model
                                    else:
                                        # Non-observe-model (e.g., local DataHub Base Prophet)
                                        # can't be re-trained this way
                                        failed_models.append(
                                            f"{anomaly_config.name} on {forecast_run.display_name}: "
                                            "Forecast model was loaded from cache and cannot be "
                                            "re-trained. Please re-train the forecast model first, "
                                            "or use an observe-models forecast model (prophet, datahub)."
                                        )
                                        continue
                            else:
                                # Standalone model - use selected preprocessing
                                forecast_model = None

                                # Load preprocessed data based on selection
                                full_df: pd.DataFrame | None = None

                                if selected_preprocessing_id == "__current__":
                                    full_df = st.session_state.get("preprocessed_df")
                                elif selected_preprocessing_id and hostname:
                                    # Load saved preprocessing
                                    try:
                                        endpoint_cache = (
                                            loader.cache.get_endpoint_cache(hostname)
                                        )
                                        full_df = endpoint_cache.load_preprocessing(
                                            selected_preprocessing_id
                                        )
                                    except Exception as e:
                                        failed_models.append(
                                            f"{anomaly_config.name}: Failed to load "
                                            f"preprocessing '{selected_preprocessing_id}': {e}"
                                        )
                                        continue

                                if full_df is None or len(full_df) == 0:
                                    failed_models.append(
                                        f"{anomaly_config.name}: No preprocessed data. "
                                        "Select a preprocessing configuration above."
                                    )
                                    continue

                                # Use the train/test split from the UI slider
                                train_ratio = train_split_pct / 100.0

                                # Sort by timestamp and split
                                full_df_sorted = full_df.sort_values("ds").reset_index(
                                    drop=True
                                )
                                split_idx = int(len(full_df_sorted) * train_ratio)
                                train_df = full_df_sorted.iloc[:split_idx].copy()
                                test_df = full_df_sorted.iloc[split_idx:].copy()

                            # Train anomaly model (with ground truth for grid search)
                            anomaly_model = anomaly_config.train_fn(
                                train_df, forecast_model, ground_truth_for_training
                            )

                            # Run detection on TEST DATA ONLY
                            detection_results = anomaly_config.detect_fn(
                                anomaly_model, test_df
                            )

                            # Store result
                            # Generate run_id using deterministic hash
                            if forecast_id:
                                prep_id_for_run = (
                                    None  # Preprocessing is in forecast model name
                                )
                            else:
                                # Standalone model - include preprocessing_id
                                prep_id_for_run = selected_preprocessing_id or "default"

                            run_id = _generate_anomaly_run_id(
                                anomaly_key=anomaly_key,
                                forecast_id=forecast_id,
                                preprocessing_id=prep_id_for_run,
                                sensitivity_level=sensitivity_level,
                            )

                            anomaly_detection_run = AnomalyDetectionRun(
                                run_id=run_id,
                                anomaly_model_key=anomaly_key,
                                anomaly_model_name=anomaly_config.name,
                                train_df=train_df,
                                test_df=test_df,
                                detection_results=detection_results,
                                model=anomaly_model,
                                timestamp=datetime.now(),
                                forecast_run_id=forecast_id,
                                forecast_model_name=forecast_run.display_name
                                if forecast_run
                                else None,
                                preprocessing_id=prep_id_for_run,
                                sensitivity_level=sensitivity_level,
                            )
                            _store_anomaly_run(anomaly_detection_run)
                            trained_run_ids.append(run_id)
                            trained_count += 1

                        except Exception as e:
                            import traceback

                            if forecast_run:
                                error_detail = f"{anomaly_config.name} on {forecast_run.display_name}: {e}"
                            else:
                                error_detail = (
                                    f"{anomaly_config.name} (standalone): {e}"
                                )
                            failed_models.append(error_detail)
                            # Log full traceback for debugging
                            traceback.print_exc()

                        progress_bar.progress((i + 1) / len(training_tasks))

                # Store status for display after rerun
                _set_training_status(
                    success_count=trained_count,
                    errors=failed_models,
                    skipped=skipped_models,
                    trained_run_ids=trained_run_ids,
                )
                st.rerun()

        # Display training status messages (after rerun)
        training_status = _get_and_clear_training_status()
        if training_status:
            if training_status["errors"]:
                for error_msg in training_status["errors"]:
                    st.error(f"❌ Manual run failed: {error_msg}")
            if training_status.get("skipped"):
                skipped_list = ", ".join(training_status["skipped"])
                st.warning(
                    f"⏭️ Skipped {len(training_status['skipped'])} incompatible combination(s): {skipped_list}"
                )
            if training_status["success_count"] > 0:
                st.success(
                    f"✅ Successfully completed {training_status['success_count']} manual run(s)!"
                )
                # Auto-select newly trained runs for visualization (up to 2)
                trained_ids = training_status.get("trained_run_ids", [])
                if trained_ids:
                    st.session_state[_ANOMALY_SELECTED_RUNS_KEY] = trained_ids[:2]

    with col_results:
        # -------------------------------------------------------------
        # inference_v2 runs (collapsed list, no charts)
        # -------------------------------------------------------------
        assertion_urn = st.session_state.get("selected_assertion_urn")
        if hostname and assertion_urn:
            auto_infos = _render_auto_inference_v2_runs_list(
                loader=loader,
                hostname=str(hostname),
                assertion_urn=str(assertion_urn),
            )

        st.subheader("Anomaly Detection Runs")

        anomaly_runs = _get_anomaly_runs()

        if not anomaly_runs:
            st.info("No anomaly detection runs yet. Train models to see results.")
        else:
            # Clear all button
            if st.button("Clear All Runs", key="clear_anomaly_runs"):
                _clear_anomaly_runs()
                st.rerun()

            # List runs
            for _run_id, anomaly_run in anomaly_runs.items():
                # Ensure hash is visible in the expander title
                hash_id = _extract_hash_from_run_id(anomaly_run.run_id)
                expander_title = anomaly_run.display_name
                # Double-check hash is in display name, add if missing
                if hash_id and f"[{hash_id}]" not in expander_title:
                    expander_title = f"{expander_title} [{hash_id}]"

                with st.expander(expander_title, expanded=False):
                    # Delete button at the top
                    col_del, _ = st.columns([2, 10])
                    with col_del:
                        if st.button(
                            "🗑️ Delete",
                            key=f"del_anomaly_run_{anomaly_run.run_id}",
                            help="Delete this run",
                            type="secondary",
                            use_container_width=True,
                        ):
                            _delete_anomaly_run(anomaly_run.run_id)
                            st.rerun()
                    st.markdown("---")
                    # Show run ID with hash for reference
                    if hash_id:
                        st.caption(
                            f"Run ID: `{anomaly_run.run_id}` (Hash: `{hash_id}`)"
                        )
                    st.caption(
                        f"Trained: {anomaly_run.timestamp.strftime('%Y-%m-%d %H:%M')}"
                    )
                    if "is_anomaly" in anomaly_run.detection_results.columns:
                        detected_count = anomaly_run.detection_results[
                            "is_anomaly"
                        ].sum()
                        st.caption(f"Detected anomalies: {detected_count}")

                    # Show model information
                    st.markdown("**Model Information**")

                    # Model Hyperparameters
                    if anomaly_run.model is not None:
                        params = get_model_hyperparameters(anomaly_run.model)
                        if params and "note" not in params:
                            st.markdown("**Hyperparameters:**")
                            _render_hyperparameters_display(params)
                            st.markdown("")

                    # Manual Configuration
                    st.markdown("**Manual Configuration:**")
                    config_lines = []
                    if anomaly_run.sensitivity_level is not None:
                        sensitivity_desc = (
                            "Aggressive"
                            if anomaly_run.sensitivity_level >= 7
                            else "Balanced"
                            if anomaly_run.sensitivity_level >= 4
                            else "Conservative"
                        )
                        config_lines.append(
                            f"- **Sensitivity Level**: {anomaly_run.sensitivity_level} ({sensitivity_desc})"
                        )
                    if anomaly_run.preprocessing_id:
                        config_lines.append(
                            f"- **Preprocessing ID**: `{display_preprocessing_id(anomaly_run.preprocessing_id)}`"
                        )
                    if anomaly_run.forecast_run_id:
                        config_lines.append(
                            f"- **Forecast Model**: `{anomaly_run.forecast_run_id}`"
                        )
                    st.markdown(
                        "\n".join(config_lines)
                        if config_lines
                        else "- _No additional configuration_"
                    )
                    st.markdown("")

                    # Model Metadata
                    st.markdown("**Model Metadata:**")
                    metadata_lines = [
                        f"- **Model Key**: `{anomaly_run.anomaly_model_key}`"
                    ]
                    if anomaly_run.forecast_model_name:
                        metadata_lines.append(
                            f"- **Forecast Model Name**: {anomaly_run.forecast_model_name}"
                        )
                    st.markdown("\n".join(metadata_lines))

                    # Anomaly score (primary: GROUND_TRUTH then CLEAN_TEST then best_model_score)
                    if anomaly_run.model is not None:
                        primary = get_primary_anomaly_score(anomaly_run.model)
                        if primary is not None:
                            st.markdown("")
                            st.caption(f"Anomaly score: {primary:.4f}")

    # Performance Summary section (below the two columns)
    st.markdown("---")

    anomaly_runs = _get_anomaly_runs()

    if not anomaly_runs and not auto_infos:
        st.info(
            "Train anomaly detection models to see performance summary and visualizations."
        )
        return

    # Load ground truth for performance metrics
    gt_hostname = str(st.session_state.get("current_hostname", "") or "")
    gt_assertion_urn = str(st.session_state.get("selected_assertion_urn", "") or "")
    ground_truth_df = _load_ground_truth_anomalies(gt_hostname, gt_assertion_urn)

    # Render performance summary table
    _render_anomaly_performance_summary(
        anomaly_runs,
        ground_truth_df,
        auto_run_infos=auto_infos,
    )

    # Visualization section
    st.markdown("---")
    st.subheader("Visualization")

    # Select which runs to visualize (Training + Auto)
    run_options = list(anomaly_runs.keys()) + list(auto_infos.keys())

    # Create run labels ensuring hash IDs are always included
    run_labels: dict[str, str] = {}
    for run_id, run in anomaly_runs.items():
        label = run.display_name
        # Double-check hash is included, add if missing
        hash_id = _extract_hash_from_run_id(run_id)
        if hash_id and f"[{hash_id}]" not in label:
            label = f"{label} [{hash_id}]"
        run_labels[run_id] = label
    for prefixed_id, info in auto_infos.items():
        rid = str(info.get("run_id") or "")
        preproc = display_preprocessing_id(str(info.get("preprocessing_id") or ""))
        hash_id = _extract_hash_from_run_id(rid)
        run_labels[prefixed_id] = (
            f"inference_v2 + {preproc} [{hash_id}]"
            if hash_id
            else f"inference_v2 + {preproc}"
        )

    # Get previously selected runs from session state, filter to valid options
    # Note: We don't auto-select runs here - only after training completes
    if _ANOMALY_SELECTED_RUNS_KEY not in st.session_state:
        st.session_state[_ANOMALY_SELECTED_RUNS_KEY] = []
    else:
        # Filter out any runs that no longer exist (but don't auto-fill)
        st.session_state[_ANOMALY_SELECTED_RUNS_KEY] = [
            r for r in st.session_state[_ANOMALY_SELECTED_RUNS_KEY] if r in run_options
        ]

    previous_selection = list(st.session_state[_ANOMALY_SELECTED_RUNS_KEY])

    # If no previous selection and we have auto inference_v2 runs available,
    # include them in default selection (up to 2, matching training run behavior)
    if not previous_selection and auto_infos:
        auto_run_ids = [
            rid for rid in run_options if rid.startswith(_AUTO_V2_RUN_PREFIX)
        ]
        if auto_run_ids:
            # Sort by anomaly score (best first) and take up to 2
            auto_run_ids_sorted = sorted(
                auto_run_ids,
                key=lambda rid: (
                    auto_infos.get(rid, {}).get("anomaly_score") or float("-inf")
                ),
                reverse=True,
            )
            previous_selection = auto_run_ids_sorted[:2]

    # Sort run_options by grid score (highest first, then by run_id for stability)
    def get_grid_score(run_id: str) -> float:
        if run_id.startswith(_AUTO_V2_RUN_PREFIX):
            info = auto_infos.get(run_id) or {}
            v = info.get("anomaly_score")
            try:
                return float(v) if v is not None else float("-inf")
            except Exception:
                return float("-inf")
        run = anomaly_runs.get(run_id)
        if run and run.model:
            best_model_score = getattr(run.model, "best_model_score", None)
            if best_model_score is not None and hasattr(best_model_score, "value"):
                return best_model_score.value
        return float("-inf")  # Put runs without grid score at the end

    # Sort by grid score (descending), then by run_id for stability
    run_options_sorted = sorted(
        run_options,
        key=lambda rid: (-get_grid_score(rid), rid),
    )

    # Use run_ids as the multiselect options (stable), format for display.
    # This avoids fragile label->id mapping and fixes first-click selection glitches.
    default_run_ids = [rid for rid in run_options_sorted if rid in previous_selection]
    selected_run_ids = st.multiselect(
        "**Select Runs to Visualize** (displays 2 per row)",
        options=run_options_sorted,
        default=default_run_ids,
        format_func=lambda rid: run_labels.get(rid, rid),
        key="anomaly_visualization_runs",
        help="Select one or more runs to visualize.",
    )

    # Update session state with current selection
    st.session_state[_ANOMALY_SELECTED_RUNS_KEY] = selected_run_ids

    # Check if differencing was applied (for cumulative view toggle)
    differencing_applied, diff_order = _was_differencing_applied()

    # Visualization options in a collapsible section
    with st.expander("Visualization Options", expanded=False):
        opt_col1, opt_col2, opt_col3, opt_col4 = st.columns(4)
        with opt_col1:
            show_detection_bands = st.checkbox("Detection Bands", value=True)
        with opt_col2:
            show_ground_truth = st.checkbox("Ground Truth", value=True)
        with opt_col3:
            show_model_detected = st.checkbox("Model Detected", value=True)
        with opt_col4:
            show_classification = st.checkbox(
                "Metrics",
                value=False,
                help="Show TP/FP/FN when ground truth is available; otherwise show anomaly and forecast scores.",
            )

        # Cumulative view toggle (only show if differencing was applied)
        show_cumulative = False
        if differencing_applied:
            # Check if original timeseries is available for anchor
            has_original = st.session_state.get(_LOADED_TIMESERIES) is not None

            if has_original:
                show_cumulative = st.checkbox(
                    "Show Cumulative View",
                    value=False,
                    key="show_cumulative_view",
                    help="Transform differenced predictions back to original cumulative metric values. "
                    "Bands widen proportionally to sqrt(n) due to variance accumulation.",
                )

        # Ground truth filter (only if ground truth is enabled)
        if show_ground_truth:
            ground_truth_filter = st.multiselect(
                "Ground Truth Status Filter",
                options=["confirmed", "rejected", "unconfirmed"],
                default=["confirmed", "unconfirmed"],
                help="Filter which ground truth anomaly statuses to display",
            )
        else:
            ground_truth_filter = []

    if not selected_run_ids:
        st.info("Select at least one run to visualize.")
        return

    # Note: ground_truth_df is already loaded above for the Performance Summary
    # Render charts in rows of 2, supporting both Training + Auto items.
    # endpoint_cache is already defined at function level

    for row_start in range(0, len(selected_run_ids), 2):
        row_ids = selected_run_ids[row_start : row_start + 2]
        chart_cols = st.columns(2)

        for idx, rid in enumerate(row_ids):
            with chart_cols[idx]:
                if rid.startswith(_AUTO_V2_RUN_PREFIX):
                    run_id = str((auto_infos.get(rid) or {}).get("run_id") or "")
                    if endpoint_cache is None or not run_id:
                        st.info("inference_v2 run is unavailable.")
                        continue
                    run_data = endpoint_cache.load_auto_inference_v2_run(run_id)
                    if not isinstance(run_data, dict):
                        st.warning("Failed to load inference_v2 run data.")
                        continue
                    auto_train_df: Any = run_data.get("train_df")
                    auto_prediction_df: Any = run_data.get("prediction_df")
                    eval_train_df: Any = run_data.get("eval_train_df")
                    eval_df: Any = run_data.get("eval_df")
                    evaluation_detection_results: Any = run_data.get(
                        "evaluation_detection_results"
                    )
                    if (
                        isinstance(auto_train_df, pd.DataFrame)
                        and "ds" in auto_train_df.columns
                    ):
                        chart_eval_train_df: pd.DataFrame | None = None
                        chart_eval_df: pd.DataFrame | None = None
                        chart_eval_results_df: pd.DataFrame | None = None

                        if (
                            show_cumulative
                            and differencing_applied
                            and isinstance(eval_train_df, pd.DataFrame)
                            and len(eval_train_df) > 0
                            and "y" in eval_train_df.columns
                            and isinstance(eval_df, pd.DataFrame)
                            and len(eval_df) > 0
                            and "y" in eval_df.columns
                            and isinstance(evaluation_detection_results, pd.DataFrame)
                            and len(evaluation_detection_results) > 0
                        ):
                            anchor = _get_cumulative_anchor(eval_train_df, diff_order)
                            if anchor is not None:
                                results_for_cum = evaluation_detection_results.copy()
                                if (
                                    "timestamp_ms" in results_for_cum.columns
                                    and "ds" not in results_for_cum.columns
                                ):
                                    results_for_cum["ds"] = pd.to_datetime(
                                        results_for_cum["timestamp_ms"],
                                        unit="ms",
                                    )
                                if (
                                    "y" not in results_for_cum.columns
                                    and "y" in eval_df.columns
                                ):
                                    results_for_cum = results_for_cum.merge(
                                        eval_df[["ds", "y"]],
                                        on="ds",
                                        how="left",
                                    )
                                if "y" in results_for_cum.columns:
                                    results_for_cum = results_for_cum.sort_values(
                                        "ds"
                                    ).copy()
                                    (
                                        chart_eval_train_df,
                                        chart_eval_df,
                                        chart_eval_results_df,
                                    ) = _transform_to_cumulative(
                                        eval_train_df.sort_values("ds").copy(),
                                        eval_df.sort_values("ds").copy(),
                                        results_for_cum,
                                        anchor,
                                    )

                        if (
                            chart_eval_train_df is not None
                            and chart_eval_df is not None
                            and chart_eval_results_df is not None
                        ):
                            fig = _render_auto_inference_v2_anomaly_chart(
                                title=run_labels.get(rid, "inference_v2"),
                                train_df=auto_train_df,
                                prediction_df=auto_prediction_df,
                                show_detection_bands=show_detection_bands,
                                eval_train_df=chart_eval_train_df,
                                eval_df=chart_eval_df,
                                evaluation_detection_results=chart_eval_results_df,
                            )
                        else:
                            fig = _render_auto_inference_v2_anomaly_chart(
                                title=run_labels.get(rid, "inference_v2"),
                                train_df=auto_train_df,
                                prediction_df=auto_prediction_df,
                                show_detection_bands=show_detection_bands,
                                eval_train_df=eval_train_df,
                                eval_df=eval_df,
                                evaluation_detection_results=evaluation_detection_results,
                            )
                        st.plotly_chart(
                            fig, use_container_width=True, key=f"chart_{rid}"
                        )

                        # Show detection results table for inference_v2 runs
                        # (matching training runs functionality)
                        if (
                            isinstance(evaluation_detection_results, pd.DataFrame)
                            and len(evaluation_detection_results) > 0
                        ):
                            with st.expander("View Detection Results", expanded=False):
                                tab_results, tab_details = st.tabs(
                                    ["Results", "Model Details"]
                                )

                                with tab_results:
                                    display_cols = ["ds"]
                                    # Convert timestamp_ms to ds if needed
                                    results_df = evaluation_detection_results.copy()
                                    if (
                                        "timestamp_ms" in results_df.columns
                                        and "ds" not in results_df.columns
                                    ):
                                        results_df["ds"] = pd.to_datetime(
                                            results_df["timestamp_ms"], unit="ms"
                                        )
                                    # Merge with eval_df to get y values
                                    if (
                                        isinstance(eval_df, pd.DataFrame)
                                        and "y" in eval_df.columns
                                    ):
                                        results_df = results_df.merge(
                                            eval_df[["ds", "y"]], on="ds", how="left"
                                        )
                                        display_cols.append("y")
                                    if "yhat" in results_df.columns:
                                        display_cols.append("yhat")
                                    if "is_anomaly" in results_df.columns:
                                        display_cols.append("is_anomaly")
                                    if "anomaly_score" in results_df.columns:
                                        display_cols.append("anomaly_score")
                                    # Canonical names only; normalize from observe-models alias if needed
                                    if (
                                        "detection_band_upper" not in results_df.columns
                                        and "detection_upper" in results_df.columns
                                        and "detection_band_lower"
                                        not in results_df.columns
                                        and "detection_lower" in results_df.columns
                                    ):
                                        results_df["detection_band_upper"] = results_df[
                                            "detection_upper"
                                        ]
                                        results_df["detection_band_lower"] = results_df[
                                            "detection_lower"
                                        ]
                                    if "detection_band_upper" in results_df.columns:
                                        display_cols.extend(
                                            [
                                                "detection_band_lower",
                                                "detection_band_upper",
                                            ]
                                        )

                                    # Filter display_cols to only include columns that actually exist
                                    available_display_cols = [
                                        col
                                        for col in display_cols
                                        if col in results_df.columns
                                    ]

                                    if (
                                        len(available_display_cols) > 1
                                    ):  # More than just "ds"
                                        st.dataframe(
                                            results_df[
                                                available_display_cols
                                            ].sort_values("ds"),
                                            use_container_width=True,
                                        )
                                    else:
                                        st.info("No detection results available.")

                                with tab_details:
                                    model_cfg: Any = run_data.get("model_config") or {}
                                    if isinstance(model_cfg, dict):
                                        st.json(model_cfg)
                                    else:
                                        st.info("Model config unavailable.")

                        # Metrics for inference_v2: TP/FP/FN when ground truth available, else scores
                        if show_classification:
                            has_classification = (
                                show_ground_truth
                                and show_model_detected
                                and isinstance(
                                    evaluation_detection_results, pd.DataFrame
                                )
                                and len(evaluation_detection_results) > 0
                                and "is_anomaly" in evaluation_detection_results.columns
                            )
                            if has_classification:
                                results_df = evaluation_detection_results.copy()
                                if (
                                    "timestamp_ms" in results_df.columns
                                    and "ds" not in results_df.columns
                                ):
                                    results_df["ds"] = pd.to_datetime(
                                        results_df["timestamp_ms"], unit="ms"
                                    )
                                model_detected = results_df[
                                    results_df["is_anomaly"] == True  # noqa: E712
                                ]
                                metrics = _compute_classification_metrics(
                                    model_detected_df=model_detected,
                                    ground_truth_df=ground_truth_df,
                                )
                                m_col1, m_col2, m_col3 = st.columns(3)
                                with m_col1:
                                    st.metric(
                                        "TP/FP/FN",
                                        f"{metrics['TP']}/{metrics['FP']}/{metrics['FN']}",
                                    )
                                with m_col2:
                                    prec_val = (
                                        f"{metrics['Precision']:.0%}"
                                        if not np.isnan(metrics["Precision"])
                                        else "N/A"
                                    )
                                    st.metric("Precision", prec_val)
                                with m_col3:
                                    f1_val = (
                                        f"{metrics['F1']:.0%}"
                                        if not np.isnan(metrics["F1"])
                                        else "N/A"
                                    )
                                    st.metric("F1", f1_val)
                            else:
                                # No ground truth or detections: show anomaly and forecast scores
                                score_config: Any = run_data.get("model_config") or {}
                                if isinstance(score_config, dict):
                                    anomaly_score = score_config.get("anomaly_score")
                                    forecast_score = score_config.get("forecast_score")
                                    score_cols = st.columns(
                                        2
                                        if isinstance(forecast_score, (int, float))
                                        else 1
                                    )
                                    with score_cols[0]:
                                        st.metric(
                                            "Anomaly Score",
                                            f"{float(anomaly_score):.4f}"
                                            if isinstance(anomaly_score, (int, float))
                                            else "N/A",
                                        )
                                    if len(score_cols) > 1 and isinstance(
                                        forecast_score, (int, float)
                                    ):
                                        with score_cols[1]:
                                            st.metric(
                                                "Forecast Score",
                                                f"{float(forecast_score):.4f}",
                                            )
                    else:
                        st.warning("inference_v2 run training data is unavailable.")
                else:
                    selected_run = anomaly_runs[rid]
                    chart_train_df: pd.DataFrame | None = None
                    chart_test_df: pd.DataFrame | None = None
                    chart_results_df: pd.DataFrame | None = None

                    if show_cumulative and differencing_applied:
                        anchor = _get_cumulative_anchor(
                            selected_run.train_df, diff_order
                        )
                        if anchor is not None:
                            chart_train_df, chart_test_df, chart_results_df = (
                                _transform_to_cumulative(
                                    selected_run.train_df,
                                    selected_run.test_df,
                                    selected_run.detection_results,
                                    anchor,
                                )
                            )

                    fig = _render_anomaly_chart(
                        run=selected_run,
                        ground_truth_df=ground_truth_df,
                        show_detection_bands=show_detection_bands,
                        show_ground_truth=show_ground_truth,
                        show_model_detected=show_model_detected,
                        ground_truth_filter=ground_truth_filter,
                        override_train_df=chart_train_df,
                        override_test_df=chart_test_df,
                        override_results_df=chart_results_df,
                        is_cumulative_view=show_cumulative
                        and chart_train_df is not None,
                    )
                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        key=f"chart_{selected_run.run_id}",
                    )

                    # Metrics: TP/FP/FN when ground truth available, else anomaly score
                    if show_classification:
                        has_classification = (
                            show_ground_truth
                            and show_model_detected
                            and "is_anomaly" in selected_run.detection_results.columns
                        )
                        if has_classification:
                            model_detected = selected_run.detection_results[
                                selected_run.detection_results["is_anomaly"]  # noqa: E712
                                == True
                            ]
                            metrics = _compute_classification_metrics(
                                model_detected_df=model_detected,
                                ground_truth_df=ground_truth_df,
                            )
                            m_col1, m_col2, m_col3 = st.columns(3)
                            with m_col1:
                                st.metric(
                                    "TP/FP/FN",
                                    f"{metrics['TP']}/{metrics['FP']}/{metrics['FN']}",
                                )
                            with m_col2:
                                prec_val = (
                                    f"{metrics['Precision']:.0%}"
                                    if not np.isnan(metrics["Precision"])
                                    else "N/A"
                                )
                                st.metric("Precision", prec_val)
                            with m_col3:
                                f1_val = (
                                    f"{metrics['F1']:.0%}"
                                    if not np.isnan(metrics["F1"])
                                    else "N/A"
                                )
                                st.metric("F1", f1_val)
                        else:
                            anomaly_score = (
                                get_primary_anomaly_score(selected_run.model)
                                if selected_run.model is not None
                                else None
                            )
                            st.metric(
                                "Anomaly Score",
                                f"{anomaly_score:.4f}"
                                if anomaly_score is not None
                                else "N/A",
                            )

                    with st.expander("View Detection Results", expanded=False):
                        tab_results, tab_details = st.tabs(["Results", "Model Details"])

                        with tab_results:
                            display_cols = ["ds", "y"]
                            if "yhat" in selected_run.detection_results.columns:
                                display_cols.append("yhat")
                            if "is_anomaly" in selected_run.detection_results.columns:
                                display_cols.append("is_anomaly")
                            if (
                                "anomaly_score"
                                in selected_run.detection_results.columns
                            ):
                                display_cols.append("anomaly_score")
                            if (
                                "detection_upper"
                                in selected_run.detection_results.columns
                            ):
                                display_cols.extend(
                                    ["detection_lower", "detection_upper"]
                                )

                            available_cols = [
                                c
                                for c in display_cols
                                if c in selected_run.detection_results.columns
                            ]
                            st.dataframe(
                                selected_run.detection_results[available_cols],
                                hide_index=True,
                                use_container_width=True,
                            )

                        with tab_details:
                            if selected_run.model:
                                params = get_model_hyperparameters(selected_run.model)
                                if (
                                    params
                                    and params.get("note")
                                    != "No hyperparameters available"
                                ):
                                    st.markdown("**Hyperparameters:**")
                                    _render_hyperparameters_display(params)
                                    st.markdown("")

                            st.markdown("**Manual Configuration:**")
                            config_lines = []
                            if selected_run.sensitivity_level is not None:
                                sensitivity_desc = (
                                    "Aggressive"
                                    if selected_run.sensitivity_level >= 7
                                    else "Balanced"
                                    if selected_run.sensitivity_level >= 4
                                    else "Conservative"
                                )
                                config_lines.append(
                                    f"- **Sensitivity Level**: {selected_run.sensitivity_level} ({sensitivity_desc})"
                                )
                            if selected_run.preprocessing_id:
                                config_lines.append(
                                    f"- **Preprocessing ID**: `{display_preprocessing_id(selected_run.preprocessing_id)}`"
                                )
                            if selected_run.forecast_run_id:
                                config_lines.append(
                                    f"- **Forecast Model**: `{selected_run.forecast_run_id}`"
                                )
                            st.markdown(
                                "\n".join(config_lines)
                                if config_lines
                                else "- _No additional configuration_"
                            )
                            st.markdown("")

                            st.markdown("**Model Metadata:**")
                            metadata_lines = [
                                f"- **Model Key**: `{selected_run.anomaly_model_key}`"
                            ]
                            if selected_run.forecast_model_name:
                                metadata_lines.append(
                                    f"- **Forecast Model Name**: {selected_run.forecast_model_name}"
                                )
                            st.markdown("\n".join(metadata_lines))

                            if selected_run.model:
                                best_model_score = getattr(
                                    selected_run.model, "best_model_score", None
                                )
                                if best_model_score is not None and hasattr(
                                    best_model_score, "value"
                                ):
                                    st.caption(
                                        f"Grid search best score: {best_model_score.value:.4f}"
                                    )
                                if hasattr(
                                    selected_run.model, "all_grid_search_results"
                                ):
                                    num_configs = len(
                                        selected_run.model.all_grid_search_results
                                    )
                                    if num_configs > 0:
                                        st.caption(
                                            f"Evaluated {num_configs} configurations"
                                        )

        if row_start + 2 < len(selected_run_ids):
            st.markdown("---")

    # Show note about metrics when Metrics checkbox is enabled
    if show_classification:
        st.caption(
            "**Note**: When ground truth and model detections are available, metrics show TP/FP/FN vs confirmed anomalies (1-hour tolerance). "
            "Otherwise anomaly and forecast scores are shown when available."
        )


__all__ = ["render_anomaly_comparison_page"]
