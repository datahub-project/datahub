# ruff: noqa: INP001
"""Time Series Comparison page for comparing and visualizing manual and inference_v2 run results."""

from typing import Any, MutableMapping, SupportsFloat, cast

import numpy as np
import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
import streamlit as st

from ..common import (
    _extract_hash_from_run_id,
    _hex_to_rgba,
    get_model_hyperparameters,
    init_explorer_state,
    load_auto_inference_v2_runs,
)
from ..common.preprocessing_keys import display_preprocessing_id
from .model_training import (
    TrainingRun,
    _get_training_runs,
    _load_cached_training_runs,
)

# =============================================================================
# Visualization
# =============================================================================


def _create_individual_model_chart(run: TrainingRun) -> go.Figure:
    """Create a single model chart showing training + test data."""
    train_df = run.train_df
    test_df = run.test_df
    forecast = run.forecast
    has_test = isinstance(test_df, pd.DataFrame) and len(test_df) > 0
    test_forecast = (
        forecast[forecast["ds"].isin(test_df["ds"])].copy()
        if has_test
        else forecast.copy()
    )

    # Use Scattergl for better performance with large datasets
    scatter_type = go.Scattergl if len(train_df) + len(test_df) > 5000 else go.Scatter

    fig = go.Figure()

    # Training data (blue)
    fig.add_trace(
        scatter_type(
            x=train_df["ds"],
            y=train_df["y"],
            mode="lines+markers" if len(train_df) < 200 else "lines",
            name="Training Data",
            line=dict(color="#1f77b4", width=2),
            marker=dict(size=3),
        )
    )

    if has_test:
        # Test actual values (orange)
        fig.add_trace(
            scatter_type(
                x=test_df["ds"],
                y=test_df["y"],
                mode="lines+markers" if len(test_df) < 200 else "lines",
                name="Test Actual",
                line=dict(color="#ff7f0e", width=2),
                marker=dict(size=3),
            )
        )

    # Predicted values
    fig.add_trace(
        scatter_type(
            x=test_forecast["ds"],
            y=test_forecast["yhat"],
            mode="lines+markers" if len(test_forecast) < 200 else "lines",
            name="Predicted" if has_test else "Forecast (future)",
            line=dict(color=run.color, width=2, dash=run.dash),
            marker=dict(size=3),
        )
    )

    # Confidence interval in test area only; width is informed by training-period
    # residuals/variance in the backend (residual_std / conformal calibration).
    has_valid_ci = (
        "yhat_upper" in test_forecast.columns
        and "yhat_lower" in test_forecast.columns
        and test_forecast["yhat_upper"].notna().any()
        and test_forecast["yhat_lower"].notna().any()
    )
    if has_valid_ci:
        fig.add_trace(
            go.Scatter(
                x=pd.concat([test_forecast["ds"], test_forecast["ds"][::-1]]),
                y=pd.concat(
                    [test_forecast["yhat_upper"], test_forecast["yhat_lower"][::-1]]
                ),
                fill="toself",
                fillcolor=f"rgba{_hex_to_rgba(run.color, 0.2)}",
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                showlegend=True,
                name="Forecast CI",
            )
        )

    # Auto-size y-axis based on Actual + point forecast, ignoring CI.
    # Otherwise CI bands can dominate the axis range and compress the actual series.
    y_min = None
    y_max = None
    try:
        y_series: list[pd.Series] = []
        if "y" in train_df.columns:
            y_series.append(pd.to_numeric(train_df["y"], errors="coerce"))
        if has_test and "y" in test_df.columns:
            y_series.append(pd.to_numeric(test_df["y"], errors="coerce"))
        if "yhat" in test_forecast.columns:
            y_series.append(pd.to_numeric(test_forecast["yhat"], errors="coerce"))
        if y_series:
            y_all = pd.concat(y_series, ignore_index=True).dropna()
            if len(y_all) > 0:
                y_min = float(y_all.min())
                y_max = float(y_all.max())
    except Exception:
        y_min = None
        y_max = None

    if y_min is not None and y_max is not None:
        pad = (y_max - y_min) * 0.05
        if not np.isfinite(pad) or pad == 0:
            pad = 1.0
        fig.update_yaxes(range=[y_min - pad, y_max + pad])

    # Add vertical line at train/test split (only when test exists)
    if has_test:
        split_time = train_df["ds"].max()
        if hasattr(split_time, "to_pydatetime"):
            split_time = split_time.to_pydatetime()
        fig.add_shape(
            type="line",
            x0=split_time,
            x1=split_time,
            y0=0,
            y1=1,
            yref="paper",
            line=dict(color="gray", width=1, dash="dash"),
        )

    fig.update_layout(
        title=run.display_name,
        xaxis_title="Date",
        yaxis_title="Value",
        height=400,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            font=dict(size=10),
        ),
        hovermode="x unified",
        margin=dict(l=50, r=20, t=40, b=50),
    )

    return fig


def _render_individual_model_charts(
    selected_runs: list[TrainingRun],
) -> None:
    """Render individual full charts (training + test) for each model.

    Displays up to 2 plots per row, creating additional rows as needed.
    Wrapped in a collapsible expander to allow focus on prediction comparison.
    """
    if not selected_runs:
        return

    st.subheader("Individual Model Results")
    with st.expander("Training + Test Data", expanded=True):
        # Render in rows of 2
        for row_start in range(0, len(selected_runs), 2):
            row_runs = selected_runs[row_start : row_start + 2]
            cols = st.columns(2)

            for idx, run in enumerate(row_runs):
                fig = _create_individual_model_chart(run)
                with cols[idx]:
                    st.plotly_chart(fig, use_container_width=True)


def _create_prediction_chart(run: TrainingRun) -> go.Figure:
    """Create a prediction window chart for a single model."""
    test_df = run.test_df
    forecast = run.forecast
    has_test = isinstance(test_df, pd.DataFrame) and len(test_df) > 0
    test_forecast = (
        forecast[forecast["ds"].isin(test_df["ds"])].copy()
        if has_test
        else forecast.copy()
    )

    # Use Scattergl for better performance
    scatter_type = go.Scattergl if len(test_df) > 5000 else go.Scatter

    fig = go.Figure()

    if has_test:
        # Test actual values (orange)
        fig.add_trace(
            scatter_type(
                x=test_df["ds"],
                y=test_df["y"],
                mode="lines+markers" if len(test_df) < 200 else "lines",
                name="Actual",
                line=dict(color="#ff7f0e", width=2),
                marker=dict(size=4),
            )
        )

    # Predicted values
    fig.add_trace(
        scatter_type(
            x=test_forecast["ds"],
            y=test_forecast["yhat"],
            mode="lines+markers" if len(test_forecast) < 200 else "lines",
            name="Predicted" if has_test else "Forecast (future)",
            line=dict(color=run.color, width=2, dash=run.dash),
            marker=dict(size=4),
        )
    )

    # Confidence interval (only if columns exist AND have non-null values)
    has_valid_ci = (
        "yhat_upper" in test_forecast.columns
        and "yhat_lower" in test_forecast.columns
        and test_forecast["yhat_upper"].notna().any()
        and test_forecast["yhat_lower"].notna().any()
    )
    if has_valid_ci:
        fig.add_trace(
            go.Scatter(
                x=pd.concat([test_forecast["ds"], test_forecast["ds"][::-1]]),
                y=pd.concat(
                    [test_forecast["yhat_upper"], test_forecast["yhat_lower"][::-1]]
                ),
                fill="toself",
                fillcolor=f"rgba{_hex_to_rgba(run.color, 0.2)}",
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                showlegend=True,
                name="Forecast CI",
            )
        )

    # Auto-size y-axis based on Actual + point forecast, ignoring CI.
    # Otherwise CI bands can dominate the axis range and compress the actual series.
    y_min = None
    y_max = None
    try:
        y_series: list[pd.Series] = []
        if has_test and "y" in test_df.columns:
            y_series.append(pd.to_numeric(test_df["y"], errors="coerce"))
        if "yhat" in test_forecast.columns:
            y_series.append(pd.to_numeric(test_forecast["yhat"], errors="coerce"))
        if y_series:
            y_all = pd.concat(y_series, ignore_index=True).dropna()
            if len(y_all) > 0:
                y_min = float(y_all.min())
                y_max = float(y_all.max())
    except Exception:
        y_min = None
        y_max = None

    if y_min is not None and y_max is not None:
        pad = (y_max - y_min) * 0.05
        if not np.isfinite(pad) or pad == 0:
            pad = 1.0
        fig.update_yaxes(range=[y_min - pad, y_max + pad])

    fig.update_layout(
        title=f"{run.display_name} - Test Period",
        xaxis_title="Date",
        yaxis_title="Value",
        height=350,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            font=dict(size=10),
        ),
        hovermode="x unified",
        margin=dict(l=50, r=20, t=40, b=50),
    )

    return fig


def _render_prediction_comparison_chart(
    selected_runs: list[TrainingRun],
) -> None:
    """Render side-by-side comparison of just the prediction window.

    Displays up to 2 plots per row, creating additional rows as needed.
    """
    if not selected_runs or len(selected_runs) < 2:
        return

    st.subheader("Prediction Window Comparison")

    # Render in rows of 2
    for row_start in range(0, len(selected_runs), 2):
        row_runs = selected_runs[row_start : row_start + 2]
        cols = st.columns(2)

        for idx, run in enumerate(row_runs):
            fig = _create_prediction_chart(run)
            with cols[idx]:
                st.plotly_chart(fig, use_container_width=True)


def _render_metrics_summary_table(runs: list[TrainingRun]) -> None:
    """Render a summary table comparing metrics across all runs."""
    if not runs:
        return

    st.subheader("Performance Summary")

    # Build comparison data with raw values for sorting
    table_data: list[dict[str, object]] = []
    for run in runs:
        metrics = run.metrics
        mae_val = float(metrics.get("MAE", float("nan")))
        rmse_val = float(metrics.get("RMSE", float("nan")))
        mape_val = float(metrics.get("MAPE", float("nan")))
        mape_display = f"{mape_val:.1f}%" if not np.isnan(mape_val) else "N/A"
        # Display score with threshold indicator
        if run.score is not None:
            from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
                DEFAULT_FORECAST_SCORE_THRESHOLD,
            )

            threshold = DEFAULT_FORECAST_SCORE_THRESHOLD
            meets_threshold = run.score >= threshold
            threshold_indicator = "✓" if meets_threshold else "⚠"
            score_display = f"{run.score:.3f} {threshold_indicator} (≥{threshold})"
        else:
            score_display = "N/A"
        # Use display_name which includes model + preprocessing + hash (consistent with other pages)
        model_display = run.display_name
        table_data.append(
            {
                "Model": model_display,
                "Preprocessing": display_preprocessing_id(run.preprocessing_id),
                "Score": score_display,
                "MAE": f"{mae_val:.2f}" if not np.isnan(mae_val) else "N/A",
                "RMSE": f"{rmse_val:.2f}" if not np.isnan(rmse_val) else "N/A",
                "MAPE": mape_display,
                "_score_raw": run.score
                if run.score is not None
                else float("-inf"),  # For sorting
                "_mae_raw": mae_val,  # For sorting
            }
        )

    # Sort by score (highest/best first), then by MAE (lowest/best first)
    table_data.sort(
        key=lambda x: (
            -float(cast(SupportsFloat, x["_score_raw"]))
            if x["_score_raw"] != float("-inf")
            else float("inf"),
            float(cast(SupportsFloat, x["_mae_raw"]))
            if not np.isnan(float(cast(SupportsFloat, x["_mae_raw"])))
            else float("inf"),
        )
    )

    # Remove raw sorting columns before display
    for row in table_data:
        del row["_score_raw"]
        del row["_mae_raw"]

    summary_df = pd.DataFrame(table_data)

    st.dataframe(
        summary_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Model": st.column_config.TextColumn("Model", width="medium"),
            "Preprocessing": st.column_config.TextColumn(
                "Preprocessing",
                width="medium",
                help="Preprocessing configuration ID",
            ),
            "Score": st.column_config.TextColumn(
                "Score",
                width="small",
                help="Normalized forecast score (0-1) - higher is better. ✓ = meets threshold (≥0.3), ⚠ = below threshold",
            ),
            "MAE": st.column_config.TextColumn(
                "MAE",
                width="small",
                help="Mean Absolute Error - lower is better",
            ),
            "RMSE": st.column_config.TextColumn(
                "RMSE",
                width="small",
                help="Root Mean Squared Error - lower is better",
            ),
            "MAPE": st.column_config.TextColumn(
                "MAPE",
                width="small",
                help="Mean Absolute Percentage Error - lower is better",
            ),
        },
    )


def _render_detailed_comparison_table(
    test_df: pd.DataFrame, selected_runs: list[TrainingRun]
) -> None:
    """Render detailed comparison table with predictions from selected runs."""
    if not selected_runs:
        return

    # Start with test data
    comparison_df = test_df[["ds", "y"]].copy()
    comparison_df = comparison_df.rename(columns={"y": "Actual"})

    # Build column config dynamically
    column_config = {
        "Date": st.column_config.DatetimeColumn("Date", format="YYYY-MM-DD HH:mm"),
        "Actual": st.column_config.NumberColumn("Actual", format="%.2f"),
    }

    # Add predictions from each run (including CI if available)
    for run in selected_runs:
        forecast = run.forecast
        test_forecast = forecast[forecast["ds"].isin(test_df["ds"])].copy()

        # Select columns to include
        cols_to_select = ["ds", "yhat"]
        # Check if CI columns exist AND have non-null values
        has_ci = (
            "yhat_lower" in forecast.columns
            and "yhat_upper" in forecast.columns
            and forecast["yhat_lower"].notna().any()
            and forecast["yhat_upper"].notna().any()
        )

        if has_ci:
            cols_to_select.extend(["yhat_lower", "yhat_upper"])

        test_forecast = test_forecast[cols_to_select].copy()

        # Rename columns with run name prefix
        rename_map = {"yhat": run.display_name}
        column_config[run.display_name] = st.column_config.NumberColumn(
            run.display_name, format="%.2f"
        )

        if has_ci:
            lower_col = f"{run.display_name}_lower"
            upper_col = f"{run.display_name}_upper"
            rename_map["yhat_lower"] = lower_col
            rename_map["yhat_upper"] = upper_col
            column_config[lower_col] = st.column_config.NumberColumn(
                f"{run.display_name} CI Lower", format="%.2f"
            )
            column_config[upper_col] = st.column_config.NumberColumn(
                f"{run.display_name} CI Upper", format="%.2f"
            )

        test_forecast = test_forecast.rename(columns=rename_map)
        comparison_df = comparison_df.merge(test_forecast, on="ds", how="left")

    comparison_df = comparison_df.rename(columns={"ds": "Date"})

    with st.expander("View Detailed Predictions", expanded=False):
        st.dataframe(
            comparison_df,
            hide_index=True,
            use_container_width=True,
            column_config=column_config,
        )


def _render_model_details(selected_runs: list[TrainingRun]) -> None:
    """Render model details expander."""
    with st.expander("Model Details", expanded=False):
        for run in selected_runs:
            st.markdown(f"**{run.display_name}**")

            hash_id = _extract_hash_from_run_id(run.run_id)
            if hash_id:
                st.caption(f"Run ID: `{run.run_id}` • Hash: `{hash_id}`")
            else:
                st.caption(f"Run ID: `{run.run_id}`")

            # -----------------------------------------------------------------
            # Manual Configuration (always available)
            # -----------------------------------------------------------------
            st.markdown("**Manual Configuration**")
            config_lines: list[str] = [
                f"- **Preprocessing ID**: `{display_preprocessing_id(run.preprocessing_id)}`"
            ]

            if run.sensitivity_level is not None:
                sensitivity_desc = (
                    "Aggressive"
                    if run.sensitivity_level >= 7
                    else "Balanced"
                    if run.sensitivity_level >= 4
                    else "Conservative"
                )
                config_lines.append(
                    f"- **Forecast Sensitivity**: {run.sensitivity_level} ({sensitivity_desc})"
                )

            if run.score is not None:
                config_lines.append(f"- **Score**: `{run.score:.6g}`")

            st.markdown("\n".join(config_lines))

            # -----------------------------------------------------------------
            # Model Metadata (always available)
            # -----------------------------------------------------------------
            st.markdown("**Model Metadata**")
            metadata_lines: list[str] = []
            if run.is_observe_model:
                metadata_lines.append("- **Model Type**: Observe Model")
                if run.registry_key:
                    metadata_lines.append(f"- **Registry Key**: `{run.registry_key}`")
            else:
                metadata_lines.append("- **Model Type**: Standard")
            metadata_lines.append(f"- **Model Key**: `{run.model_key}`")
            st.markdown("\n".join(metadata_lines))

            # -----------------------------------------------------------------
            # Hyperparameters (best-effort; may be unavailable for cached runs)
            # -----------------------------------------------------------------
            st.markdown("**Hyperparameters**")
            model = run.model
            params = get_model_hyperparameters(model)

            note = params.get("note")
            if isinstance(note, str):
                # When loaded from disk cache, run.model is intentionally not persisted.
                st.caption(note)
            else:
                param_lines: list[str] = []
                for key, value in sorted(params.items()):
                    if isinstance(value, dict):
                        param_lines.append(f"- **{key}**: `{value}`")
                    elif isinstance(value, float):
                        param_lines.append(f"- **{key}**: `{value:.6g}`")
                    else:
                        param_lines.append(f"- **{key}**: `{value}`")
                st.markdown("\n".join(param_lines) if param_lines else "_None_")

            # Extra fallback for Prophet models (if we couldn't extract hyperparams)
            if (
                isinstance(note, str)
                and note in {"No hyperparameters available", "Model not available"}
                and model is not None
                and hasattr(model, "changepoint_prior_scale")
            ):
                st.markdown("**Prophet Parameters (fallback)**")
                prophet_params = {
                    "changepoint_prior_scale": getattr(
                        model, "changepoint_prior_scale", None
                    ),
                    "seasonality_prior_scale": getattr(
                        model, "seasonality_prior_scale", None
                    ),
                    "weekly_seasonality": getattr(model, "weekly_seasonality", None),
                    "daily_seasonality": getattr(model, "daily_seasonality", None),
                    "interval_width": getattr(model, "interval_width", None),
                }
                st.markdown(
                    "\n".join([f"- **{k}**: `{v}`" for k, v in prophet_params.items()])
                )

            st.markdown("---")


# =============================================================================
# Main Page
# =============================================================================


def render_timeseries_comparison_page() -> None:
    """Render the Time Series Comparison page."""
    st.header("Time Series Comparison")

    init_explorer_state()

    # Ensure cached runs (including Auto inference_v2 runs mapped into TrainingRun)
    # are available when navigating here directly.
    try:
        _load_cached_training_runs()
        # Also load auto inference_v2 runs separately
        hostname = st.session_state.get("current_hostname")
        assertion_urn = st.session_state.get("current_assertion_urn")
        if hostname and assertion_urn:
            load_auto_inference_v2_runs(
                hostname=hostname,
                assertion_urn=assertion_urn,
                session_state=cast(MutableMapping[str, Any], st.session_state),
            )
    except Exception:
        pass

    # Navigation - Back to Training
    col_nav_back, col_nav_spacer, col_nav_forward = st.columns([1, 3, 1])
    with col_nav_back:
        if st.button("← Model Training"):
            from . import model_training_page

            st.switch_page(model_training_page)
    with col_nav_forward:
        if st.button("Anomaly Comparison →"):
            from . import anomaly_comparison_page

            st.switch_page(anomaly_comparison_page)

    st.markdown("---")
    # Runs (Training + Auto are treated the same).
    all_runs = _get_training_runs()

    if not all_runs:
        st.warning(
            "No runs available. Go to the Model Training page to run manual or inference_v2 models first."
        )
        from . import model_training_page

        if st.button("← Go to Model Training"):
            st.switch_page(model_training_page)
        return

    runs_list = list(all_runs.values())

    st.subheader("Select Runs to Compare")

    run_options = [r.run_id for r in runs_list]
    run_labels = {r.run_id: r.display_name for r in runs_list}

    st.write("**Runs** (Manual + Inference_v2)")

    selected_run_ids: list[str] = []
    num_cols = min(3, len(run_options))  # Max 3 columns
    cols = st.columns(num_cols) if num_cols > 0 else []

    for idx, run_id in enumerate(run_options):
        col_idx = idx % num_cols if num_cols > 0 else 0
        with cols[col_idx] if cols else st.container():
            is_checked = st.checkbox(
                run_labels.get(run_id, run_id),
                value=True,  # Default all selected
                key=f"ts_run_cb_{run_id}",
            )
            if is_checked:
                selected_run_ids.append(run_id)

    if not selected_run_ids:
        st.info("Select at least one run to view results.")
        return

    selected_runs = [all_runs[rid] for rid in selected_run_ids if rid in all_runs]

    _render_metrics_summary_table(selected_runs)

    st.markdown("---")

    st.subheader("Visualization")

    def _mae_for_sort(run: TrainingRun) -> float:
        v = run.metrics.get("MAE", float("inf"))
        try:
            f = float(v)
        except Exception:
            return float("inf")
        return f if np.isfinite(f) else float("inf")

    if len(selected_runs) > 2:
        sorted_by_mae = sorted(selected_runs, key=_mae_for_sort)
        best_two_ids = [sorted_by_mae[0].run_id, sorted_by_mae[1].run_id]

        st.write(
            "**Select runs to visualize** (defaults to best 2 by MAE, displays 2 per row)"
        )

        viz_run_ids: list[str] = []
        num_cols = min(3, len(selected_runs))  # Max 3 columns
        cols = st.columns(num_cols) if num_cols > 0 else []

        for idx, run in enumerate(selected_runs):
            col_idx = idx % num_cols if num_cols > 0 else 0
            with cols[col_idx] if cols else st.container():
                default_checked = run.run_id in best_two_ids
                is_checked = st.checkbox(
                    run_labels.get(run.run_id, run.run_id),
                    value=default_checked,
                    key=f"ts_viz_cb_{run.run_id}",
                )
                if is_checked:
                    viz_run_ids.append(run.run_id)

        viz_runs = [all_runs[rid] for rid in viz_run_ids if rid in all_runs]
    else:
        viz_runs = selected_runs

    if viz_runs:
        test_df = None
        for r in viz_runs:
            if isinstance(r.test_df, pd.DataFrame) and len(r.test_df) > 0:
                test_df = r.test_df
                break

        _render_individual_model_charts(viz_runs)

        st.markdown("---")

        if len(viz_runs) >= 2:
            _render_prediction_comparison_chart(viz_runs)
            st.markdown("---")

        if test_df is not None:
            _render_detailed_comparison_table(test_df, viz_runs)
        else:
            st.caption(
                "Detailed predictions table is unavailable because the selected runs do not have a test window."
            )

        _render_model_details(viz_runs)

        st.markdown("---")


__all__ = ["render_timeseries_comparison_page"]
