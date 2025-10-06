# ruff: noqa: INP001

import json
import math
import random
import time
from datetime import datetime, timedelta, timezone
from typing import List

import datahub.metadata.schema_classes as models
import pandas as pd
import plotly.graph_objects as go
import pydantic.v1.class_validators
import streamlit as st
import streamlit_ext as ste
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.urns import DatasetUrn
from plotly.subplots import make_subplots
from pydantic.v1.json import pydantic_encoder

from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    default_volume_assertion_urn,
    default_volume_monitor_urn,
    make_volume_metric_cube_urn,
)
from datahub_executor.common.client.fetcher.monitors.graphql.query import (
    GRAPHQL_GET_MONITOR_OPERATION,
    GRAPHQL_LIST_MONITORS_OPERATION,
    GRAPHQL_LIST_MONITORS_QUERY,
)
from datahub_executor.common.client.fetcher.monitors.mapper import (
    graphql_to_monitor,
    graphql_to_monitors,
)
from datahub_executor.common.constants import LIST_MONITORS_BATCH_SIZE
from datahub_executor.common.helpers import paginate_datahub_query_results
from datahub_executor.common.metric.client.client import (
    MetricClient,
)
from datahub_executor.common.metric.types import (
    Metric,
)
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference.base_assertion_trainer import (
    BaseAssertionTrainer,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
    MetricPredictor,
)
from datahub_executor.common.monitor.inference.utils import (
    get_metric_ceiling_value,
    get_metric_floor_value,
    is_metric_anomaly,
)
from datahub_executor.common.types import (
    AssertionAdjustmentSettings,
    AssertionExclusionWindow,
    AssertionMonitorSensitivity,
    Monitor,
)
from datahub_executor.config import VOLUME_DEFAULT_SENSITIVITY_LEVEL

_SELECTED_MONITOR_KEY = "selected_monitor"

# Initialize connection to DataHub
graph = get_default_graph()
metrics_client = MetricClient(graph)
monitor_client = MonitorClient(graph)

graph.execute_graphql = st.cache_data(graph.execute_graphql)  # type: ignore
metrics_client.fetch_row_count_metric_values = st.cache_data(  # type: ignore
    metrics_client.fetch_row_count_metric_values
)


def generate_sample_metrics(
    num_points: int = 60,
    trend_factor: float = 1.0,
    seasonality: float = 20.0,
    noise_level: float = 5.0,
    base_value: float = 100.0,
) -> List[Metric]:
    """
    Generate synthetic time series data with trend, seasonality, and noise.
    """
    metrics = []
    now = int(time.time() * 1000)

    # Generate data points going back in time
    for i in range(num_points, 0, -1):
        # Generate timestamp (going backward from now)
        timestamp_ms = now - (i * 24 * 60 * 60 * 1000)  # daily data

        # Generate value with trend, seasonality, and noise
        trend = trend_factor * (num_points - i)
        season = seasonality * (0.5 + 0.5 * (1 + (i % 7) / 7))  # weekly seasonality
        noise = random.normalvariate(0, noise_level)

        value = base_value + trend + season + noise
        value = max(0, value)  # Ensure non-negative values

        metrics.append(Metric(timestamp_ms=timestamp_ms, value=value))

    return metrics


def create_volume_smart_assertion(dataset_urn: str) -> tuple[str, str]:
    """Creates a monitor and assertion for a dataset. Returns the URNs."""

    monitor_urn = default_volume_monitor_urn(dataset_urn)
    assertion_urn = default_volume_assertion_urn(dataset_urn)

    if graph.exists(monitor_urn):
        raise ValueError(f"Monitor {monitor_urn} already exists")

    if graph.exists(assertion_urn):
        raise ValueError(f"Assertion {assertion_urn} already exists")

    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=monitor_urn,
            aspect=models.MonitorInfoClass(
                type=models.MonitorTypeClass.ASSERTION,
                assertionMonitor=models.AssertionMonitorClass(
                    assertions=[
                        models.AssertionEvaluationSpecClass(
                            assertion=assertion_urn,
                            parameters=models.AssertionEvaluationParametersClass(
                                type=models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME,
                                datasetVolumeParameters=models.DatasetVolumeAssertionParametersClass(
                                    sourceType=models.DatasetVolumeSourceTypeClass.DATAHUB_DATASET_PROFILE
                                ),
                            ),
                            schedule=models.CronScheduleClass(
                                cron="0 0 * * *",
                                timezone="UTC",
                            ),
                        )
                    ]
                ),
                status=models.MonitorStatusClass(mode=models.MonitorModeClass.INACTIVE),
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=models.AssertionInfoClass(
                source=models.AssertionSourceClass(
                    type=models.AssertionSourceTypeClass.INFERRED
                ),
                type=models.AssertionTypeClass.VOLUME,
                volumeAssertion=models.VolumeAssertionInfoClass(
                    type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                    entity=dataset_urn,
                ),
            ),
        ),
    ]

    graph.emit_mcps(mcps)
    return assertion_urn, monitor_urn


def generate_volume_sample_data(dataset_urn: str) -> None:
    """Generates sample row count data for a dataset."""
    metrics = generate_sample_metrics()
    metric_urn = make_volume_metric_cube_urn(dataset_urn)

    for metric in metrics:
        metrics_client.save_metric_value(
            metric_urn=metric_urn,
            metric=metric,
        )


def render_create_assertion_page() -> None:
    """Render the create assertion page."""
    st.header("Create New Volume Assertion")

    with st.form("create_assertion"):
        dataset_urn = st.text_input(
            "Dataset URN", help="Format: urn:li:dataset:(platform,name,env)"
        )

        submitted = st.form_submit_button("Create")

        if submitted:
            try:
                # Validate URN format
                DatasetUrn.from_string(dataset_urn)

                # Create assertion and monitor
                assertion_urn, monitor_urn = create_volume_smart_assertion(dataset_urn)

                st.success("Successfully created volume assertion!")
                st.write("**Assertion URN:**", assertion_urn)
                st.write("**Monitor URN:**", monitor_urn)

            except ValueError as e:
                st.error(f"Error: {str(e)}")
            except Exception as e:
                st.error(f"Unexpected error: {str(e)}")


def _get_monitor(monitor_urn: str) -> Monitor:
    """Get a monitor from the DataHub GraphQL API."""

    graphql_monitor = graph.execute_graphql(
        GRAPHQL_LIST_MONITORS_QUERY,
        operation_name=GRAPHQL_GET_MONITOR_OPERATION,
        variables={
            "urn": monitor_urn,
        },
    )["entity"]
    return graphql_to_monitor(graphql_monitor)


def render_monitor_detail_page() -> None:
    """Render details for a specific monitor."""

    st.header("Monitor Details")

    default_monitor_urn = st.session_state.get(_SELECTED_MONITOR_KEY, "")
    monitor_urn = ste.text_input(
        "Monitor URN", value=default_monitor_urn, key=_SELECTED_MONITOR_KEY
    )
    if not monitor_urn:
        st.error("Monitor URN not provided")
        return

    monitor = _get_monitor(monitor_urn)
    st.json(monitor.json(), expanded=False)

    # TODO: only render for volume assertions.
    render_volume_assertion_simulation_ui(monitor)


def render_volume_assertion_simulation_ui(monitor: Monitor) -> None:
    """Render the assertion simulation UI."""
    st.header("Assertion Simulation")

    assertion = (
        monitor.assertion_monitor.assertions[0] if monitor.assertion_monitor else None
    )
    dataset = assertion.assertion.entity if assertion else None
    if not dataset:
        st.error("No dataset found for assertion")
        return
    dataset_urn = dataset.urn

    original_adjustment_settings = (
        monitor.assertion_monitor.settings.inference_settings
        if monitor.assertion_monitor and monitor.assertion_monitor.settings
        else None
    )

    # Settings.

    last_day_cutoff_date = st.date_input(
        "Last day of metrics to include", value=datetime.now(timezone.utc)
    )
    last_day_cutoff = datetime.combine(
        last_day_cutoff_date, datetime.min.time(), tzinfo=timezone.utc
    )

    exclusion_windows_raw = st.text_area(
        "Exclusion Windows",
        value=(
            json.dumps(
                original_adjustment_settings.exclusion_windows, default=pydantic_encoder
            )
            if original_adjustment_settings
            and original_adjustment_settings.exclusion_windows
            else ""
        ),
        help="""\
Example exclusion windows:

{
    "type": "FIXED_RANGE",
    "fixedRange": {
        "startTimeMillis": "2025-02-27T00:00:00+00:00",
        "endTimeMillis": "2025-02-28T00:00:00+00:00"
    }
}

{ "type": "WEEKLY", "weekly": { "days_of_week": ["SATURDAY", "SUNDAY"] } }
""",
    )
    default_lookback = (
        BaseAssertionTrainer.extract_lookback_days_from_adjustment_settings(
            original_adjustment_settings
        )
    )
    lookback = st.number_input(
        "Lookback", value=default_lookback, min_value=1, max_value=365, step=1
    )

    sensitivity = st.slider(
        "Sensitivity",
        min_value=1,
        max_value=10,
        value=VOLUME_DEFAULT_SENSITIVITY_LEVEL,
        step=1,
    )

    user_config_raw = st.text_area("User Config", value="{}")

    prediction_unit_options = {
        "Hourly": timedelta(hours=1),
        "Daily": timedelta(days=1),
    }
    selected_unit = st.select_slider(
        "Prediction Boundary Unit",
        options=list(prediction_unit_options.keys()),
        value="Daily",
    )
    unit = prediction_unit_options[selected_unit]
    multiple = math.ceil(timedelta(days=14) / unit)

    # Simulation logic.
    metric_urn = make_volume_metric_cube_urn(monitor.urn)
    st.write(f"Fetching all metrics for metric_urn {metric_urn}")
    all_metrics = metrics_client.fetch_row_count_metric_values(
        metric_urn=metric_urn,
        dataset_urn=dataset_urn,
        start_time=last_day_cutoff - timedelta(days=lookback + 5),
        end_time=datetime.now(timezone.utc),
    )
    st.json(
        {"all_metrics": {m.timestamp().isoformat(): m.value for m in all_metrics}},
        expanded=False,
    )

    exclusion_windows: List[AssertionExclusionWindow] = pydantic.parse_obj_as(
        List[AssertionExclusionWindow],  # type: ignore
        json.loads(exclusion_windows_raw or "[]"),
    )
    user_config = json.loads(user_config_raw)

    adjustment_settings = AssertionAdjustmentSettings(
        exclusion_windows=exclusion_windows,
        training_data_lookback_window_days=lookback,
        sensitivity=AssertionMonitorSensitivity(level=sensitivity),
    )

    historical_metrics = metrics_client.fetch_row_count_metric_values(
        metric_urn=metric_urn,
        dataset_urn=dataset_urn,
        start_time=last_day_cutoff - timedelta(days=lookback),
        end_time=last_day_cutoff,
    )

    # Fetch anomalies
    anomalies = monitor_client.fetch_monitor_anomalies(
        urn=monitor.urn,
        lookback=timedelta(days=lookback),
        limit=2000,
    )
    st.write(f"Fetched {len(anomalies)} anomalies")
    st.json(
        {
            "anomalies": {
                a.metric.timestamp().isoformat(): a.metric.value for a in anomalies
            }
        },
        expanded=False,
    )

    # Filter out anomalies to avoid using in training
    metrics_without_anomalies = [
        metric
        for metric in historical_metrics
        if not is_metric_anomaly(metric, anomalies)
    ]

    st.write(
        f"Fetched {len(metrics_without_anomalies)} after filtering out anomalies (vs {len(historical_metrics)} prior to filtering)."
    )

    training_metrics = BaseAssertionTrainer.filter_training_timeseries(
        metrics_without_anomalies, adjustment_settings
    )
    st.write(
        f"Fetched {len(training_metrics)} training samples after applying exclusions (vs {len(metrics_without_anomalies)} prior to exclusion)."
    )
    st.json(
        {
            "training_metrics": {
                m.timestamp().isoformat(): m.value for m in training_metrics
            }
        },
        expanded=False,
    )

    if not training_metrics:
        st.info("No training data found")
        return

    metric_predictor = MetricPredictor(user_config=user_config)

    metric_type = (
        assertion.assertion.field_assertion.field_metric_assertion.metric
        if assertion.assertion.field_assertion
        else None
    )
    metric_floor_value = None
    metric_ceiling_value = None
    if metric_type:
        metric_floor_value = get_metric_floor_value(metric_type.name)
        metric_ceiling_value = get_metric_ceiling_value(metric_type.name)
    st.write(
        f"Predicting metric boundaries for {metric_type} with floor value {metric_floor_value} and ceiling value {metric_ceiling_value}"
    )
    metric_boundaries = metric_predictor.predict_metric_boundaries(
        training_metrics,
        unit=unit,
        multiple=multiple,
        sensitivity_level=sensitivity,
        start_time=last_day_cutoff,
        floor_value=metric_floor_value,
        ceiling_value=metric_ceiling_value,
    )

    fig = plot_metrics_and_predictions(
        training_metrics=training_metrics,
        all_metrics=all_metrics,
        prediction_start_time=last_day_cutoff,
        metric_boundaries=metric_boundaries,
    )
    st.plotly_chart(fig)


def plot_metrics_and_predictions(
    training_metrics: List[Metric],
    metric_boundaries: List[MetricBoundary],
    prediction_start_time: datetime,
    all_metrics: List[Metric] | None = None,
) -> go.Figure:
    """
    Plot the original metrics and the predicted boundaries using Plotly.
    """

    training_timestamps = [m.timestamp_ms for m in training_metrics]
    non_training_metrics = [
        m for m in (all_metrics or []) if m.timestamp_ms not in training_timestamps
    ]

    # Create a DataFrame for easier plotting
    training_df = pd.DataFrame(
        {
            "timestamp": [m.timestamp() for m in training_metrics],
            "value": [m.value for m in training_metrics],
        }
    )
    non_training_df = pd.DataFrame(
        {
            "timestamp": [m.timestamp() for m in non_training_metrics],
            "value": [m.value for m in non_training_metrics],
        }
    )

    # Initialize the figure using make_subplots
    fig = make_subplots()

    # Add historical data
    fig.add_trace(
        go.Scatter(
            x=training_df["timestamp"],
            y=training_df["value"],
            mode="lines+markers",
            name="Training Data",
            line=dict(color="#1f77b4", width=2),
            opacity=0.7,
        )
    )

    # Add future boundaries as filled regions and lines
    lower_bound_x = []
    lower_bound_y = []
    upper_bound_x = []
    upper_bound_y = []

    for i, boundary in enumerate(metric_boundaries):
        start_time = datetime.fromtimestamp(boundary.start_time_ms / 1000)
        end_time = datetime.fromtimestamp(boundary.end_time_ms / 1000)
        lower = boundary.lower_bound.value
        upper = boundary.upper_bound.value

        # Add to arrays for connecting lines
        lower_bound_x.extend([start_time, end_time])
        lower_bound_y.extend([lower, lower])
        upper_bound_x.extend([start_time, end_time])
        upper_bound_y.extend([upper, upper])

        # Add filled area
        fig.add_trace(
            go.Scatter(
                x=[start_time, end_time],
                y=[upper, upper],
                mode="lines",
                line=dict(width=0),
                showlegend=False if i > 0 else False,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[start_time, end_time],
                y=[lower, lower],
                mode="lines",
                line=dict(width=0),
                fill="tonexty",
                fillcolor="rgba(44, 160, 44, 0.2)",
                name="Future Prediction Range" if i == 0 else None,
                showlegend=i == 0,
            )
        )

    # Add connecting lines for bounds
    fig.add_trace(
        go.Scatter(
            x=lower_bound_x,
            y=lower_bound_y,
            mode="lines",
            line=dict(color="green", width=1, dash=None),
            name="Lower Bound",
            opacity=0.7,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=upper_bound_x,
            y=upper_bound_y,
            mode="lines",
            line=dict(color="green", width=1, dash=None),
            name="Upper Bound",
            opacity=0.7,
        )
    )

    # Add non-training data as orange dots.
    if not non_training_df.empty:
        fig.add_trace(
            go.Scatter(
                x=non_training_df["timestamp"],
                y=non_training_df["value"],
                mode="markers",
                name="Non-Training Data",
                marker=dict(color="#ff7f0e", size=4),
                opacity=0.7,
            )
        )

    # Add vertical line for "now"
    fig.add_shape(
        type="line",
        x0=prediction_start_time,
        x1=prediction_start_time,
        y0=0,
        y1=1,
        yref="paper",
        line=dict(color="black", width=1, dash="dash"),
        opacity=0.5,
    )

    # Update layout
    fig.update_layout(
        title="Metric Prediction Simulation",
        xaxis_title="Time",
        yaxis_title="Value",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
        ),
    )

    return fig


def _list_monitors(paginate: bool = False) -> List[Monitor]:
    """List the monitors using the DataHub GraphQL API."""

    params: dict = {
        "input": {
            "types": ["MONITOR"],
            "query": "*",
            "searchFlags": {"skipCache": True},
        }
    }

    if paginate:
        result = paginate_datahub_query_results(
            graph=graph,
            query=GRAPHQL_LIST_MONITORS_QUERY,
            operation_name=GRAPHQL_LIST_MONITORS_OPERATION,
            query_key="searchAcrossEntities",
            result_key="searchResults",
            user_params=params,
            page_size=LIST_MONITORS_BATCH_SIZE,
        )
    else:
        params["input"]["count"] = 100
        result = graph.execute_graphql(
            GRAPHQL_LIST_MONITORS_QUERY,
            operation_name=GRAPHQL_LIST_MONITORS_OPERATION,
            variables=params,
        )["searchAcrossEntities"]["searchResults"]

    monitors = [item["entity"] for item in result]
    return graphql_to_monitors(monitors)


def render_monitor_list_page() -> None:
    """Render the list of monitors."""
    st.header("Monitors")

    limited = st.checkbox("Limit to first 100", value=True)

    monitors = _list_monitors(paginate=not limited)

    # Create a dataframe with key monitor information
    monitor_data = []
    for monitor in monitors:
        assertion = (
            monitor.assertion_monitor.assertions[0]
            if monitor.assertion_monitor
            else None
        )
        dataset = assertion.assertion.entity if assertion else None

        monitor_info = {
            "Monitor": monitor.urn,
            "Mode": monitor.mode.value if monitor.mode else None,
            "Dataset": dataset.table_name if dataset else None,
            "Assertion Type": assertion.assertion.type.value if assertion else None,
            "Assertion": assertion.json() if assertion else None,
            "Settings": (
                monitor.assertion_monitor.settings.json()
                if monitor.assertion_monitor and monitor.assertion_monitor.settings
                else None
            ),
            "Executor ID": monitor.executor_id,
        }
        monitor_data.append(monitor_info)

    monitor_data_df = pd.DataFrame(monitor_data)

    st.text(
        "Select a row using the checkbox on the left to view details. "
        "Click the 'View Monitor Details' button to view the details of the selected monitor."
    )
    monitor_df = st.dataframe(
        monitor_data_df,
        column_config={
            "Monitor": st.column_config.TextColumn(width="small"),
            "Dataset": st.column_config.TextColumn(width="medium"),
            "Assertion": st.column_config.JsonColumn(width="small"),
            "Settings": st.column_config.JsonColumn(width="small"),
        },
        on_select="rerun",
        selection_mode="single-row",
    )

    if row_selection := monitor_df.get("selection", {}).get("rows"):
        # Go to details when a row is selected.
        monitor_urn = monitor_data_df.iloc[row_selection[0]]["Monitor"]
        st.session_state[_SELECTED_MONITOR_KEY] = monitor_urn
        st.switch_page(detail_page)


def render_load_sample_data_page() -> None:
    """Render the load sample data page."""
    st.header("Load Sample Data")

    with st.form("load_sample"):
        dataset_urn = st.text_input(
            "Dataset URN", help="Format: urn:li:dataset:(platform,name,env)"
        )

        submitted = st.form_submit_button("Generate Sample Data")

        if submitted:
            try:
                # Validate URN format
                DatasetUrn.from_string(dataset_urn)

                # Generate and save sample data
                generate_volume_sample_data(dataset_urn)

                st.success("Successfully generated sample data!")

                # Link to view assertion page
                st.query_params[_SELECTED_MONITOR_KEY] = default_volume_assertion_urn(
                    dataset_urn
                )
                st.switch_page(detail_page)

            except ValueError as e:
                st.error(f"Error: {str(e)}")
            except Exception as e:
                st.error(f"Unexpected error: {str(e)}")


st.set_page_config(
    page_title="Observe Control Panel",
    layout="wide",
)

# Define pages
create_page = st.Page(
    lambda: render_create_assertion_page(),
    title="Create Assertion",
    icon="➕",
    url_path="/create",
)

view_page = st.Page(
    lambda: render_monitor_list_page(),
    title="All Monitors",
    icon="📋",
    url_path="/view",
    default=True,
)

sample_data_page = st.Page(
    lambda: render_load_sample_data_page(),
    title="Load Sample Row Counts",
    icon="💾",
    url_path="/sample_data",
)

detail_page = st.Page(
    lambda: render_monitor_detail_page(),
    title="Monitor Details",
    icon="🔍",
    url_path="/detail",
)

# Navigation menu
pages = {
    "Monitors": [create_page, view_page, detail_page],
    "Tools": [sample_data_page],
}
page = st.navigation(pages)
page.run()

# See https://github.com/streamlit/streamlit/issues/3218
pydantic.class_validators._FUNCS.clear()
