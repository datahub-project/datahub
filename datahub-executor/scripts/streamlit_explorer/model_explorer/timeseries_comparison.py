# ruff: noqa: INP001
"""Time Series Comparison page for comparing and visualizing training run results."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
import streamlit as st

from ..common import _hex_to_rgba, init_explorer_state
from .model_training import TrainingRun, _get_training_runs

# =============================================================================
# Visualization
# =============================================================================


def _create_individual_model_chart(run: TrainingRun) -> go.Figure:
    """Create a single model chart showing training + test data."""
    train_df = run.train_df
    test_df = run.test_df
    forecast = run.forecast
    test_forecast = forecast[forecast["ds"].isin(test_df["ds"])].copy()

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
            name="Predicted",
            line=dict(color=run.color, width=2, dash=run.dash),
            marker=dict(size=3),
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
                name="95% CI",
            )
        )

    # Add vertical line at train/test split
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
    test_forecast = forecast[forecast["ds"].isin(test_df["ds"])].copy()

    # Use Scattergl for better performance
    scatter_type = go.Scattergl if len(test_df) > 5000 else go.Scatter

    fig = go.Figure()

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
            name="Predicted",
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
                name="95% CI",
            )
        )

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
        mape_display = (
            f"{metrics['MAPE']:.1f}%" if not np.isnan(metrics["MAPE"]) else "N/A"
        )
        table_data.append(
            {
                "Model": run.model_name,
                "Preprocessing": run.preprocessing_id,
                "MAE": f"{metrics['MAE']:.2f}",
                "RMSE": f"{metrics['RMSE']:.2f}",
                "MAPE": mape_display,
                "_mae_raw": metrics["MAE"],  # For sorting
            }
        )

    # Sort by MAE (lowest/best first)
    table_data.sort(key=lambda x: float(str(x["_mae_raw"])))

    # Remove raw sorting column before display
    for row in table_data:
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
            model = run.model
            if hasattr(model, "changepoint_prior_scale"):
                params_info = {
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
                for param, value in params_info.items():
                    st.text(f"  {param}: {value}")
            st.markdown("---")


# =============================================================================
# Main Page
# =============================================================================


def render_timeseries_comparison_page() -> None:
    """Render the Time Series Comparison page."""
    st.header("Time Series Comparison")

    init_explorer_state()

    # Navigation - Back to Training
    col_nav_back, col_nav_spacer, col_nav_forward = st.columns([1, 3, 1])
    with col_nav_back:
        if st.button("← Model Training"):
            from . import model_training_page

            st.switch_page(model_training_page)

    st.markdown("---")

    # Get training runs
    all_runs = _get_training_runs()

    if not all_runs:
        st.warning(
            "No training runs available. "
            "Go to the Model Training page to run models first."
        )
        from . import model_training_page

        if st.button("← Go to Model Training"):
            st.switch_page(model_training_page)
        return

    runs_list = list(all_runs.values())

    # Run selection
    st.subheader("Select Runs to Compare")

    run_options = [r.run_id for r in runs_list]
    run_labels = {r.run_id: r.display_name for r in runs_list}

    # Use checkboxes for clear visibility
    st.write("**Training Runs** (select runs to include in metrics)")

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

    # Metrics summary table (all selected runs)
    _render_metrics_summary_table(selected_runs)

    st.markdown("---")

    # Visualization selection
    st.subheader("Visualization")

    if len(selected_runs) > 2:
        # Sort by MAE to get best models for default selection
        sorted_by_mae = sorted(
            selected_runs,
            key=lambda r: float(r.metrics.get("MAE", float("inf"))),
        )
        best_two_ids = [sorted_by_mae[0].run_id, sorted_by_mae[1].run_id]

        # Use checkboxes for clear visibility of all options
        st.write(
            "**Select runs to visualize** (defaults to best 2 by MAE, "
            "displays 2 per row)"
        )

        viz_run_ids: list[str] = []
        num_cols = min(3, len(selected_runs))  # Max 3 columns
        cols = st.columns(num_cols) if num_cols > 0 else []

        for idx, run in enumerate(selected_runs):
            col_idx = idx % num_cols if num_cols > 0 else 0
            with cols[col_idx] if cols else st.container():
                # Default to best 2 by MAE
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
        # Use the first run's test data for the detailed table
        test_df = viz_runs[0].test_df

        # Render individual model charts (training + test for each)
        _render_individual_model_charts(viz_runs)

        st.markdown("---")

        # Render prediction comparison (if 2+ runs selected)
        if len(viz_runs) >= 2:
            _render_prediction_comparison_chart(viz_runs)
            st.markdown("---")

        # Render detailed comparison table
        _render_detailed_comparison_table(test_df, viz_runs)

        # Render model details
        _render_model_details(viz_runs)

    # Navigation to Anomaly Comparison
    st.markdown("---")
    if st.button("Anomaly Comparison →", type="primary", use_container_width=True):
        from . import anomaly_comparison_page

        st.switch_page(anomaly_comparison_page)


__all__ = ["render_timeseries_comparison_page"]
