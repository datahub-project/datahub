# ruff: noqa: INP001
"""Anomaly Comparison page for training and comparing anomaly detection models."""

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
import streamlit as st

from ..common import DataLoader, init_explorer_state
from .model_training import TrainingRun
from .observe_models_adapter import (
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


def _get_training_runs() -> dict[str, TrainingRun]:
    """Get stored training runs from session state."""
    if "training_runs" not in st.session_state:
        st.session_state["training_runs"] = {}
    return st.session_state["training_runs"]


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
            timestamp = row.get("timestampMillis") or row.get("timestamp")
            if timestamp:
                # Determine status from event data
                status = "unconfirmed"
                if row.get("confirmed") is True:
                    status = "confirmed"
                elif row.get("rejected") is True:
                    status = "rejected"

                anomalies.append(
                    {
                        "ds": pd.to_datetime(timestamp, unit="ms"),
                        "status": status,
                    }
                )

        return pd.DataFrame(anomalies)

    except Exception:
        return pd.DataFrame(columns=["ds", "status"])


# =============================================================================
# Visualization
# =============================================================================


def _hex_to_rgba(hex_color: str, alpha: float) -> tuple:
    """Convert hex color to RGBA tuple."""
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return (r, g, b, alpha)


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
) -> go.Figure:
    """Render anomaly detection chart with optional layers.

    Shows training data as context (lighter) and test data where detection occurs.
    Each combination (forecast + anomaly model) gets its own chart.
    """
    train_df = run.train_df
    test_df = run.test_df
    results_df = run.detection_results

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

    fig.update_layout(
        title=run.display_name,
        xaxis_title="Time",
        yaxis_title="Value",
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


# =============================================================================
# Main Page
# =============================================================================


def render_anomaly_comparison_page() -> None:
    """Render the Anomaly Comparison page."""
    st.header("Anomaly Comparison")

    init_explorer_state()

    # Navigation - Back to Time Series Comparison
    col_nav_back, col_nav_spacer = st.columns([1, 4])
    with col_nav_back:
        if st.button("← Time Series Comparison"):
            from . import timeseries_comparison_page

            st.switch_page(timeseries_comparison_page)

    st.markdown("---")

    # Check if observe-models is available
    if not is_observe_models_available():
        st.warning(
            "The observe-models package is not installed. "
            "Anomaly detection models require this package."
        )
        return

    # Get available anomaly model configs
    anomaly_configs = get_anomaly_model_configs()
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

                        # Progress text
                        if forecast_run:
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

                            # Train anomaly model
                            anomaly_model = anomaly_config.train_fn(
                                train_df, forecast_model
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

    # Visualization section (below the two columns)
    st.markdown("---")
    st.subheader("Visualization")

    anomaly_runs = _get_anomaly_runs()

    if not anomaly_runs:
        st.info("Train anomaly detection models to see visualizations.")
        return

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
            # Determine default checked state
            default_checked = run_id in previous_selection
            is_checked = st.checkbox(
                run_labels.get(run_id, run_id),
                value=default_checked,
                key=f"anomaly_run_cb_{run_id}",
            )
            if is_checked:
                selected_run_ids.append(run_id)

    # Update session state with current selection
    st.session_state[_ANOMALY_SELECTED_RUNS_KEY] = selected_run_ids

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

    # Load ground truth (shared across all visualizations)
    gt_hostname = str(st.session_state.get("current_hostname", "") or "")
    gt_assertion_urn = str(st.session_state.get("selected_assertion_urn", "") or "")
    ground_truth_df = _load_ground_truth_anomalies(gt_hostname, gt_assertion_urn)

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
                # Render chart (title is in the Plotly chart)
                fig = _render_anomaly_chart(
                    run=selected_run,
                    ground_truth_df=ground_truth_df,
                    show_detection_bands=show_detection_bands,
                    show_ground_truth=show_ground_truth,
                    show_model_detected=show_model_detected,
                    ground_truth_filter=ground_truth_filter,
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
