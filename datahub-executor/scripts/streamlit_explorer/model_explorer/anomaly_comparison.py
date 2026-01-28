# ruff: noqa: INP001
"""Anomaly Comparison page for training and comparing anomaly detection models."""

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
import streamlit as st

from ..common import (
    _LOADED_TIMESERIES,
    DataLoader,
    _hex_to_rgba,
    get_model_hyperparameters,
    init_explorer_state,
)
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

    @property
    def display_name(self) -> str:
        """Display name combining anomaly model and forecast/preprocessing info."""
        if self.forecast_model_name:
            return f"{self.anomaly_model_name} (on {self.forecast_model_name})"
        elif self.preprocessing_id:
            # Standalone model - show preprocessing ID like forecast models do
            return f"{self.anomaly_model_name} + {self.preprocessing_id}"
        return self.anomaly_model_name


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
    """Store an anomaly detection run in session state."""
    runs = _get_anomaly_runs()
    runs[run.run_id] = run


def _clear_anomaly_runs() -> None:
    """Clear all stored anomaly detection runs."""
    st.session_state[_ANOMALY_RUNS_KEY] = {}


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

    Args:
        train_df: Training data with differenced y values
        test_df: Test data with differenced y values
        detection_results: Detection results with yhat, detection_upper/lower
        anchor_value: Starting value for cumulative sum

    Returns:
        Tuple of (cumulative_train_df, cumulative_test_df, cumulative_results_df)
    """
    # Transform training data: simple cumsum from anchor
    cum_train = train_df.copy()
    cum_train["y"] = anchor_value + train_df["y"].fillna(0).cumsum()

    # Last cumulative training value becomes anchor for test period
    train_end_value = cum_train["y"].iloc[-1] if len(cum_train) > 0 else anchor_value

    # Transform test data (actual values): simple cumsum from training end
    cum_test = test_df.copy()
    cum_test["y"] = train_end_value + test_df["y"].fillna(0).cumsum()

    # Transform detection results with recursive anchoring
    cum_results = detection_results.copy()
    n = len(cum_results)

    if n == 0:
        return cum_train, cum_test, cum_results

    # Build cumulative y values recursively
    y_diff = detection_results["y"].fillna(0).values
    y_cum = np.zeros(n)
    y_cum[0] = train_end_value + y_diff[0]
    for i in range(1, n):
        y_cum[i] = y_cum[i - 1] + y_diff[i]
    cum_results["y"] = y_cum

    # Forecast: anchor each prediction to previous actual cumulative
    if "yhat" in cum_results.columns:
        yhat_diff = detection_results["yhat"].fillna(0).values
        yhat_cum = np.zeros(n)
        yhat_cum[0] = train_end_value + yhat_diff[0]
        for i in range(1, n):
            yhat_cum[i] = y_cum[i - 1] + yhat_diff[i]
        cum_results["yhat"] = yhat_cum

    # Bands: anchor each band to previous actual cumulative
    if (
        "detection_upper" in cum_results.columns
        and "detection_lower" in cum_results.columns
    ):
        upper_diff = detection_results["detection_upper"].fillna(0).values
        lower_diff = detection_results["detection_lower"].fillna(0).values

        upper_cum = np.zeros(n)
        lower_cum = np.zeros(n)

        upper_cum[0] = train_end_value + upper_diff[0]
        lower_cum[0] = train_end_value + lower_diff[0]

        for i in range(1, n):
            upper_cum[i] = y_cum[i - 1] + upper_diff[i]
            lower_cum[i] = y_cum[i - 1] + lower_diff[i]

        cum_results["detection_upper"] = upper_cum
        cum_results["detection_lower"] = lower_cum

    return cum_train, cum_test, cum_results


# =============================================================================
# Visualization
# =============================================================================

# Standard colors for anomaly detection visualizations
# (No color differentiation needed since each combination gets its own chart)
_DETECTION_BAND_COLOR = "#9467bd"  # Purple for detection bands
_MODEL_DETECTED_COLOR = "#d62728"  # Red for model-detected anomalies


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
    scatter_type = go.Scattergl if total_size > 5000 else go.Scatter

    # Training data (context - lighter color)
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

    # Test data (actual values - main time series)
    fig.add_trace(
        scatter_type(
            x=test_df["ds"],
            y=test_df["y"],
            mode="lines",
            name="Test Data",
            line=dict(color="#1f77b4", width=2),  # Blue for test data
        )
    )

    # Add vertical line at train/test split
    if len(train_df) > 0 and len(test_df) > 0:
        split_time = test_df["ds"].min()
        # Use add_shape instead of add_vline to avoid timestamp annotation issues
        fig.add_shape(
            type="line",
            x0=split_time,
            x1=split_time,
            y0=0,
            y1=1,
            yref="paper",
            line=dict(color="gray", width=1, dash="dot"),
        )
        # Add annotation separately
        fig.add_annotation(
            x=split_time,
            y=1,
            yref="paper",
            text="Train/Test Split",
            showarrow=False,
            yanchor="bottom",
            font=dict(size=10, color="gray"),
        )

    # Forecast prediction line (if available) - TEST PERIOD ONLY
    # observe-models returns: yhat, yhat_lower, yhat_upper
    if "yhat" in results_df.columns:
        fig.add_trace(
            scatter_type(
                x=results_df["ds"],
                y=results_df["yhat"],
                mode="lines",
                name="Forecast",
                line=dict(color="#ff7f0e", width=2),  # Orange
            )
        )

    # Detection bands (if available and enabled) - TEST PERIOD ONLY
    # observe-models returns: detection_lower, detection_upper
    has_detection_bands = (
        "detection_upper" in results_df.columns
        and "detection_lower" in results_df.columns
    )
    if show_detection_bands and has_detection_bands:
        fig.add_trace(
            go.Scatter(
                x=pd.concat([results_df["ds"], results_df["ds"][::-1]]),
                y=pd.concat(
                    [
                        results_df["detection_upper"],
                        results_df["detection_lower"][::-1],
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

        # Upper and lower band lines
        fig.add_trace(
            scatter_type(
                x=results_df["ds"],
                y=results_df["detection_upper"],
                mode="lines",
                name="Upper Band",
                line=dict(color=_DETECTION_BAND_COLOR, width=1, dash="dash"),
                showlegend=False,
            )
        )
        fig.add_trace(
            scatter_type(
                x=results_df["ds"],
                y=results_df["detection_lower"],
                mode="lines",
                name="Lower Band",
                line=dict(color=_DETECTION_BAND_COLOR, width=1, dash="dash"),
                showlegend=False,
            )
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
    if show_model_detected and "is_anomaly" in results_df.columns:
        detected_df = results_df[results_df["is_anomaly"] == True]  # noqa: E712
        if not detected_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=detected_df["ds"],
                    y=detected_df["y"],
                    mode="markers",
                    name="Model Detected",
                    marker=dict(
                        color=_MODEL_DETECTED_COLOR,
                        size=10,
                        symbol="x",
                        line=dict(width=2),
                    ),
                    legendgroup="model_detected",
                )
            )

    # Update layout with view mode indicator
    title_suffix = " (Cumulative)" if is_cumulative_view else ""
    fig.update_layout(
        title=run.display_name + title_suffix,
        xaxis_title="Time",
        yaxis_title="Cumulative Value" if is_cumulative_view else "Value",
        height=500,
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
) -> None:
    """Render a performance summary table for all anomaly detection runs.

    Args:
        anomaly_runs: Dictionary of anomaly detection runs
        ground_truth_df: Ground truth anomaly DataFrame with 'ds' and 'status' columns
    """
    if not anomaly_runs:
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

        # Get grid search score if available
        grid_score = None
        grid_score_display = "N/A"
        if hasattr(run.model, "best_score") and run.model.best_score is not None:
            grid_score = run.model.best_score
            grid_score_display = f"{grid_score:.4f}"

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
        elif run.preprocessing_id:
            base_info = run.preprocessing_id
        else:
            base_info = "N/A"

        table_data.append(
            {
                "Anomaly Model": run.anomaly_model_name,
                "Base": base_info,
                "Detected": detected_count,
                "Grid Score": grid_score_display,
                "Precision": precision_display,
                "Recall": recall_display,
                "F1": f1_display,
                "_f1_raw": f1_raw,  # For sorting
                "_grid_raw": grid_score if grid_score is not None else float("-inf"),
            }
        )

    # Sort by F1 score (highest/best first), then by grid score
    def _sort_key(x: dict[str, object]) -> tuple[float, float]:
        f1_val = float(x["_f1_raw"])  # type: ignore[arg-type]
        grid_val = float(x["_grid_raw"])  # type: ignore[arg-type]
        return (
            -f1_val if not np.isnan(f1_val) else float("inf"),
            -grid_val,
        )

    table_data.sort(key=_sort_key)

    # Remove raw sorting columns before display
    for row in table_data:
        del row["_f1_raw"]
        del row["_grid_raw"]

    summary_df = pd.DataFrame(table_data)

    st.dataframe(
        summary_df,
        hide_index=True,
        use_container_width=True,
        column_config={
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
            "Grid Score": st.column_config.TextColumn(
                "Grid Score",
                width="small",
                help="Best score from hyperparameter grid search (higher is better)",
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
        - **Grid Score**: Best score from hyperparameter optimization (requires ground truth)
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

    # Get available anomaly model configs
    # Pass grid search setting from session state (defaults to True)
    grid_search_enabled = st.session_state.get("anomaly_enable_grid_search", True)
    anomaly_configs = get_anomaly_model_configs(enable_grid_search=grid_search_enabled)
    if not anomaly_configs:
        st.warning("No anomaly detection models available in the registry.")
        return

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

    # Two-column layout
    col_config, col_results = st.columns([1, 1])

    with col_config:
        st.subheader("Configuration")

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
            if hostname:
                endpoint_cache = loader.cache.get_endpoint_cache(hostname)
                assertion_urn = st.session_state.get("selected_assertion_urn")
                saved_preprocessings = endpoint_cache.list_saved_preprocessings(
                    assertion_urn=assertion_urn
                )
                for item in saved_preprocessings:
                    pid = item["preprocessing_id"]
                    row_count = item.get("row_count", 0)
                    preprocessing_options.append(pid)
                    preprocessing_labels[pid] = f"{pid} ({row_count} rows)"

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

        # 4. Training Options
        st.write("**Training Options**")
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
                + "\n\nTraining may take some time."
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
                with st.spinner(f"Training {len(training_tasks)} model(s)..."):
                    progress_bar = st.progress(0, text="Starting training...")

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
                                    f"Re-training {forecast_run.display_name} (cached) + "
                                    f"{anomaly_config.name}... ({i + 1}/{len(training_tasks)})"
                                )
                            else:
                                progress_text = (
                                    f"Training {anomaly_config.name} on "
                                    f"{forecast_run.display_name}... ({i + 1}/{len(training_tasks)})"
                                )
                        else:
                            progress_text = (
                                f"Training {anomaly_config.name} (standalone)... "
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
                            # Generate run_id based on whether it's forecast-based or standalone
                            if forecast_id:
                                run_id = f"{anomaly_key}__{forecast_id}"
                                prep_id_for_run = (
                                    None  # Preprocessing is in forecast model name
                                )
                            else:
                                # Standalone model - include preprocessing_id in run_id
                                prep_id_for_run = selected_preprocessing_id or "default"
                                run_id = f"{anomaly_key}__{prep_id_for_run}"

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
                    st.error(f"❌ Training failed: {error_msg}")
            if training_status.get("skipped"):
                skipped_list = ", ".join(training_status["skipped"])
                st.warning(
                    f"⏭️ Skipped {len(training_status['skipped'])} incompatible combination(s): {skipped_list}"
                )
            if training_status["success_count"] > 0:
                st.success(
                    f"✅ Successfully trained {training_status['success_count']} combination(s)!"
                )
                # Auto-select newly trained runs for visualization (up to 2)
                trained_ids = training_status.get("trained_run_ids", [])
                if trained_ids:
                    st.session_state[_ANOMALY_SELECTED_RUNS_KEY] = trained_ids[:2]

    with col_results:
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
                with st.expander(anomaly_run.display_name, expanded=False):
                    st.caption(
                        f"Trained: {anomaly_run.timestamp.strftime('%Y-%m-%d %H:%M')}"
                    )
                    if "is_anomaly" in anomaly_run.detection_results.columns:
                        detected_count = anomaly_run.detection_results[
                            "is_anomaly"
                        ].sum()
                        st.caption(f"Detected anomalies: {detected_count}")

                    # Show best hyperparameters (from grid search or model config)
                    if anomaly_run.model is not None:
                        params = get_model_hyperparameters(anomaly_run.model)
                        if params and "note" not in params:
                            st.markdown("**Best Hyperparameters:**")
                            _render_hyperparameters_display(params)

                            # Grid search info
                            if (
                                hasattr(anomaly_run.model, "best_score")
                                and anomaly_run.model.best_score is not None
                            ):
                                st.caption(
                                    f"Grid search score: {anomaly_run.model.best_score:.4f}"
                                )

    # Performance Summary section (below the two columns)
    st.markdown("---")

    anomaly_runs = _get_anomaly_runs()

    if not anomaly_runs:
        st.info(
            "Train anomaly detection models to see performance summary and visualizations."
        )
        return

    # Load ground truth for performance metrics
    gt_hostname = str(st.session_state.get("current_hostname", "") or "")
    gt_assertion_urn = str(st.session_state.get("selected_assertion_urn", "") or "")
    ground_truth_df = _load_ground_truth_anomalies(gt_hostname, gt_assertion_urn)

    # Render performance summary table
    _render_anomaly_performance_summary(anomaly_runs, ground_truth_df)

    # Visualization section
    st.markdown("---")
    st.subheader("Visualization")

    # Select which runs to visualize using checkboxes
    run_options = list(anomaly_runs.keys())
    run_labels = {k: v.display_name for k, v in anomaly_runs.items()}

    # Get previously selected runs from session state, filter to valid options
    # Note: We don't auto-select runs here - only after training completes
    if _ANOMALY_SELECTED_RUNS_KEY not in st.session_state:
        st.session_state[_ANOMALY_SELECTED_RUNS_KEY] = []
    else:
        # Filter out any runs that no longer exist (but don't auto-fill)
        st.session_state[_ANOMALY_SELECTED_RUNS_KEY] = [
            r for r in st.session_state[_ANOMALY_SELECTED_RUNS_KEY] if r in run_options
        ]

    st.write("**Select Runs to Visualize** (displays 2 per row)")

    # Use checkboxes for clear visibility of all options
    selected_run_ids: list[str] = []
    num_cols = min(3, len(run_options))  # Max 3 columns for checkbox layout
    cols = st.columns(num_cols) if num_cols > 0 else []

    previous_selection = list(st.session_state[_ANOMALY_SELECTED_RUNS_KEY])

    for idx, run_id in enumerate(run_options):
        col_idx = idx % num_cols if num_cols > 0 else 0
        with cols[col_idx] if cols else st.container():
            # Use sub-columns for checkbox + info button
            cb_col, info_col = st.columns([0.9, 0.1])

            with cb_col:
                # Determine default checked state
                default_checked = run_id in previous_selection
                is_checked = st.checkbox(
                    run_labels.get(run_id, run_id),
                    value=default_checked,
                    key=f"anomaly_run_cb_{run_id}",
                )
                if is_checked:
                    selected_run_ids.append(run_id)

            with info_col:
                # Info button to show hyperparameters
                if st.button(
                    "ℹ️",
                    key=f"anomaly_info_{run_id}",
                    help="View hyperparameters",
                    type="tertiary",
                ):
                    st.session_state[
                        f"show_anomaly_params_{run_id}"
                    ] = not st.session_state.get(f"show_anomaly_params_{run_id}", False)

            # Show hyperparameters if toggled
            if st.session_state.get(f"show_anomaly_params_{run_id}", False):
                run = anomaly_runs.get(run_id)
                if run and run.model:
                    params = get_model_hyperparameters(run.model)
                    with st.container():
                        st.markdown("**Anomaly Model Hyperparameters:**")
                        _render_hyperparameters_display(params)

                        # Also show grid search info if available
                        if (
                            hasattr(run.model, "best_score")
                            and run.model.best_score is not None
                        ):
                            st.caption(
                                f"Grid search best score: {run.model.best_score:.4f}"
                            )
                        if hasattr(run.model, "all_grid_search_results"):
                            num_configs = len(run.model.all_grid_search_results)
                            if num_configs > 0:
                                st.caption(f"Evaluated {num_configs} configurations")
                        st.markdown("---")

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
            show_classification = st.checkbox("TP/FP/FN Metrics", value=False)

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
    # Get the list of selected runs
    selected_runs = [
        anomaly_runs[rid] for rid in selected_run_ids if rid in anomaly_runs
    ]

    # Render charts in rows of 2
    for row_start in range(0, len(selected_runs), 2):
        row_runs = selected_runs[row_start : row_start + 2]
        chart_cols = st.columns(2)

        for idx, selected_run in enumerate(row_runs):
            with chart_cols[idx]:
                # Prepare data for charting (apply cumulative transform if enabled)
                chart_train_df: pd.DataFrame | None = None
                chart_test_df: pd.DataFrame | None = None
                chart_results_df: pd.DataFrame | None = None

                if show_cumulative and differencing_applied:
                    anchor = _get_cumulative_anchor(selected_run.train_df, diff_order)
                    if anchor is not None:
                        chart_train_df, chart_test_df, chart_results_df = (
                            _transform_to_cumulative(
                                selected_run.train_df,
                                selected_run.test_df,
                                selected_run.detection_results,
                                anchor,
                            )
                        )

                # Render chart (title is in the Plotly chart)
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
                    is_cumulative_view=show_cumulative and chart_train_df is not None,
                )
                st.plotly_chart(
                    fig, use_container_width=True, key=f"chart_{selected_run.run_id}"
                )

                # Classification metrics for this run
                if show_classification and show_ground_truth and show_model_detected:
                    if "is_anomaly" in selected_run.detection_results.columns:
                        model_detected = selected_run.detection_results[
                            selected_run.detection_results["is_anomaly"] == True  # noqa: E712
                        ]

                        metrics = _compute_classification_metrics(
                            model_detected_df=model_detected,
                            ground_truth_df=ground_truth_df,
                        )

                        # Compact metrics display for side-by-side layout
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

                # Data table expander for this run
                with st.expander("View Detection Results", expanded=False):
                    display_cols = ["ds", "y"]
                    if "yhat" in selected_run.detection_results.columns:
                        display_cols.append("yhat")
                    if "is_anomaly" in selected_run.detection_results.columns:
                        display_cols.append("is_anomaly")
                    if "anomaly_score" in selected_run.detection_results.columns:
                        display_cols.append("anomaly_score")
                    # observe-models uses detection_lower/detection_upper
                    if "detection_upper" in selected_run.detection_results.columns:
                        display_cols.extend(["detection_lower", "detection_upper"])

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

        # Add separator between rows if there are multiple rows
        if row_start + 2 < len(selected_runs):
            st.markdown("---")

    # Show note about metrics if classification is enabled
    if show_classification and show_ground_truth and show_model_detected:
        st.caption(
            "**Note**: Metrics compare model-detected anomalies against confirmed ground truth. "
            "A 1-hour tolerance window is used for matching."
        )


__all__ = ["render_anomaly_comparison_page"]
