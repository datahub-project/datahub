# ruff: noqa: INP001
"""Metric Cube Browser page for Time Series Explorer.

This page displays metric cube data for monitored assertions.
It uses the monitor-centric architecture where metric cubes are the primary
source of timeseries data. Assertions are shown as a reference to identify
which entity the data relates to.
"""

from typing import Optional

import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
import streamlit as st

from ..common import (
    _LOADED_TIMESERIES,
    _MONITOR_URN_FOR_ASSERTION,
    _RAW_EVENTS_DF,
    _SELECTED_ASSERTION,
    _SELECTED_ENDPOINT,
    ASSERTION_TYPE_FILTER_OPTIONS,
    FIELD_METRIC_FILTER_OPTIONS,
    AnomalyEdit,
    AnomalyEditTracker,
    DataLoader,
    _render_urn_with_link,
    connection_matches_endpoint,
    get_active_config,
    get_cache_dir,
    hostname_to_dir,
    init_explorer_state,
    logger,
    render_connection_status,
    set_explorer_context,
)
from ..common.metric_cube_extractor import MonitoredAssertionMetadata

# Constants
MAX_DISPLAY_EVENTS = 8784  # Hours in a leap year (366 * 24)


def _render_metric_cube_nav_buttons(
    *, hostname: str, assertion_urn: str, monitor_urn: Optional[str], key_prefix: str
) -> None:
    """Render navigation buttons used across the Metric Cube Browser page."""
    col_nav1, col_nav2, col_nav3 = st.columns(3)

    with col_nav1:
        if st.button("Monitor Browser →", key=f"{key_prefix}__go_to_monitor_browser"):
            # Store monitor URN for the Monitor Browser to filter to
            from ..common.shared import _SELECTED_MONITOR

            if monitor_urn:
                st.session_state[_SELECTED_MONITOR] = monitor_urn
            set_explorer_context(hostname, assertion_urn, monitor_urn)
            from . import monitor_browser_page

            st.switch_page(monitor_browser_page)

    with col_nav2:
        if st.button("Preprocessing →", key=f"{key_prefix}__go_to_preprocessing"):
            set_explorer_context(hostname, assertion_urn, monitor_urn)
            from ..model_explorer import preprocessing_page

            st.switch_page(preprocessing_page)

    with col_nav3:
        if st.button("Model Training →", key=f"{key_prefix}__go_to_model_training"):
            set_explorer_context(hostname, assertion_urn, monitor_urn)
            from ..model_explorer import model_training_page

            st.switch_page(model_training_page)


def _render_timeseries_viewer_section(
    hostname: str,
    assertion_urn: str,
    loader: DataLoader,
    assertion_metadata: Optional[MonitoredAssertionMetadata] = None,
) -> None:
    """Render the time series viewer section within assertion browser.

    Uses metric cube data as the primary source.
    """
    st.markdown("---")
    st.subheader("Time Series Viewer")

    # Get monitor URN from metadata or session state
    monitor_urn = None
    if assertion_metadata and assertion_metadata.monitor_urn:
        monitor_urn = assertion_metadata.monitor_urn
        st.session_state[_MONITOR_URN_FOR_ASSERTION] = monitor_urn
    else:
        # Fallback to session state cache
        monitor_urn = st.session_state.get(_MONITOR_URN_FOR_ASSERTION)

    if not monitor_urn:
        st.warning(
            "⚠️ No monitor found for this assertion. Metric cube data is not available.\n\n"
            "Create a monitor for this assertion to enable timeseries data collection."
        )
        return

    # Load metric cube timeseries
    ts_df = loader.load_cached_metric_cube_timeseries(
        hostname, monitor_urn, include_anomalies=True
    )

    if ts_df is None or len(ts_df) == 0:
        st.warning(
            "No metric cube data found for this assertion.\n\n"
            "The monitor may need time to collect data, or the cache may need to be refreshed."
        )
        return

    st.info(f"📊 Data source: Metric Cube ({len(ts_df)} points)")

    # Also load raw events for anomaly marking
    raw_events = loader.load_cached_events(
        hostname, aspect_name="dataHubMetricCubeEvent"
    )
    if raw_events is not None and "monitorUrn" in raw_events.columns:
        raw_events = raw_events[raw_events["monitorUrn"] == monitor_urn].copy()
    else:
        raw_events = ts_df.copy()

    st.session_state[_RAW_EVENTS_DF] = raw_events
    st.session_state[_LOADED_TIMESERIES] = ts_df

    st.markdown("**Time Range**")

    min_date = ts_df["ds"].min().date()
    max_date = ts_df["ds"].max().date()

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=min_date,
            min_value=min_date,
            max_value=max_date,
            key="ts_start_date",
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="ts_end_date",
        )

    mask = (ts_df["ds"].dt.date >= start_date) & (ts_df["ds"].dt.date <= end_date)
    filtered_df = ts_df[mask].copy()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Points", f"{len(filtered_df):,}")
    if len(filtered_df) > 0:
        col2.metric("Min", f"{filtered_df['y'].min():.2f}")
        col3.metric("Max", f"{filtered_df['y'].max():.2f}")
        col4.metric("Mean", f"{filtered_df['y'].mean():.2f}")
    else:
        col2.metric("Min", "N/A")
        col3.metric("Max", "N/A")
        col4.metric("Mean", "N/A")

    cache_dir = get_cache_dir() / hostname_to_dir(hostname)
    edit_tracker = AnomalyEditTracker(cache_dir)

    pending_new_anomalies = [
        e for e in edit_tracker.get_new_anomalies() if e.assertion_urn == assertion_urn
    ]

    st.markdown("**Time Series Plot**")

    pending_timestamps_ms = {
        e.run_event_timestamp_ms
        for e in pending_new_anomalies
        if e.run_event_timestamp_ms
    }

    scatter_type = go.Scattergl if len(filtered_df) > 5000 else go.Scatter

    fig = go.Figure()

    fig.add_trace(
        scatter_type(
            x=filtered_df["ds"],
            y=filtered_df["y"],
            mode="lines+markers" if len(filtered_df) < 500 else "lines",
            name="Value",
            line=dict(color="#1f77b4", width=2),
            marker=dict(size=4),
        )
    )

    # Show anomalies from metric cube data, differentiated by state:
    # - CONFIRMED: orange diamonds
    # - REJECTED: gray X (crossed out)
    # - Unreviewed (null state): yellow diamonds
    if "anomaly_timestampMillis" in filtered_df.columns:
        anomaly_points = filtered_df[filtered_df["anomaly_timestampMillis"].notna()]
        has_state = "anomaly_state" in anomaly_points.columns

        if len(anomaly_points) > 0 and has_state:
            # Split by state for different visualization
            confirmed = anomaly_points[anomaly_points["anomaly_state"] == "CONFIRMED"]
            rejected = anomaly_points[anomaly_points["anomaly_state"] == "REJECTED"]
            unreviewed = anomaly_points[
                (anomaly_points["anomaly_state"].isna())
                | (~anomaly_points["anomaly_state"].isin(["CONFIRMED", "REJECTED"]))
            ]

            if len(confirmed) > 0:
                fig.add_trace(
                    go.Scatter(
                        x=confirmed["ds"],
                        y=confirmed["y"],
                        mode="markers",
                        name="Confirmed Anomaly",
                        marker=dict(
                            size=12,
                            color="orange",
                            symbol="diamond",
                            line=dict(width=2, color="darkorange"),
                        ),
                    )
                )

            if len(rejected) > 0:
                fig.add_trace(
                    go.Scatter(
                        x=rejected["ds"],
                        y=rejected["y"],
                        mode="markers",
                        name="Rejected (Not Anomaly)",
                        marker=dict(
                            size=10,
                            color="gray",
                            symbol="x",
                            line=dict(width=2, color="darkgray"),
                        ),
                    )
                )

            if len(unreviewed) > 0:
                fig.add_trace(
                    go.Scatter(
                        x=unreviewed["ds"],
                        y=unreviewed["y"],
                        mode="markers",
                        name="Unreviewed Anomaly",
                        marker=dict(
                            size=12,
                            color="#FFD700",  # Gold/yellow
                            symbol="diamond",
                            line=dict(width=2, color="#DAA520"),  # Darker gold
                        ),
                    )
                )
        elif len(anomaly_points) > 0:
            # No state column - show all as generic anomalies
            fig.add_trace(
                go.Scatter(
                    x=anomaly_points["ds"],
                    y=anomaly_points["y"],
                    mode="markers",
                    name="Anomaly",
                    marker=dict(
                        size=12,
                        color="orange",
                        symbol="diamond",
                        line=dict(width=2, color="darkorange"),
                    ),
                )
            )

    # Show pending anomalies
    if pending_timestamps_ms and "timestampMillis" in filtered_df.columns:
        anomaly_mask = filtered_df["timestampMillis"].isin(pending_timestamps_ms)
        pending_points = filtered_df[anomaly_mask]
        if len(pending_points) > 0:
            fig.add_trace(
                go.Scatter(
                    x=pending_points["ds"],
                    y=pending_points["y"],
                    mode="markers",
                    name="Pending Anomaly",
                    marker=dict(
                        size=12,
                        color="red",
                        symbol="x",
                        line=dict(width=2, color="darkred"),
                    ),
                )
            )

    fig.update_layout(
        height=400,
        xaxis_title="Time",
        yaxis_title="Value",
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)

    # Navigation buttons (below visualization)
    _render_metric_cube_nav_buttons(
        hostname=hostname,
        assertion_urn=assertion_urn,
        monitor_urn=monitor_urn,
        key_prefix="metric_cube_viewer_nav",
    )

    st.markdown("---")
    _render_anomaly_marking_section_metric_cube(
        hostname=hostname,
        assertion_urn=assertion_urn,
        monitor_urn=monitor_urn,
        raw_events=raw_events,
        edit_tracker=edit_tracker,
        pending_new_anomalies=pending_new_anomalies,
    )

    with st.expander("View Data Table"):
        st.dataframe(filtered_df, hide_index=True, use_container_width=True)


def _render_anomaly_marking_section_metric_cube(
    hostname: str,
    assertion_urn: str,
    monitor_urn: str,
    raw_events: pd.DataFrame,
    edit_tracker: AnomalyEditTracker,
    pending_new_anomalies: list[AnomalyEdit],
) -> None:
    """Render the anomaly marking section for metric cube data."""
    st.subheader("Mark Anomalies")

    anomaly_pending = len(pending_new_anomalies)

    if anomaly_pending > 0:
        st.info(f"**{anomaly_pending} pending anomaly marking(s)** ready to publish.")

    if "timestampMillis" not in raw_events.columns:
        st.error("Cannot mark anomalies: timestampMillis column not found.")
        return

    # Prepare display events
    display_events = raw_events.copy()
    display_events = display_events.sort_values("timestampMillis", ascending=False)

    display_events["Time"] = pd.to_datetime(
        display_events["timestampMillis"], unit="ms"
    )

    # For metric cube data, use measure column (raw) or y column (from extract_timeseries)
    if "measure" in display_events.columns:
        value_col = "measure"
    elif "y" in display_events.columns:
        value_col = "y"
    else:
        value_col = None
    has_anomaly_state = "anomaly_state" in display_events.columns

    # Debug logging for metric value extraction
    logger.info(
        "Anomaly marking: columns=%s, value_col=%s",
        list(display_events.columns),
        value_col,
    )

    # =========================================================================
    # Column Filters (Quick Filters)
    # =========================================================================
    with st.expander("🔍 Quick Filters", expanded=True):
        filter_cols = st.columns(4)

        # Anomaly state filter
        with filter_cols[0]:
            state_filter: list[str] = []
            if has_anomaly_state:
                state_values = sorted(
                    display_events["anomaly_state"].dropna().unique().tolist()
                )
                if state_values:
                    state_filter = st.multiselect(
                        "Anomaly State",
                        options=state_values,
                        default=[],
                        key="anomaly_state_filter",
                        placeholder="All states",
                    )

        # Date range filter
        with filter_cols[1]:
            min_date = display_events["Time"].min().date()
            max_date = display_events["Time"].max().date()
            date_range = st.date_input(
                "Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                key="anomaly_date_filter",
            )

        # Value range filter
        with filter_cols[2]:
            value_range = None
            if value_col:
                val_series = pd.to_numeric(
                    display_events[value_col], errors="coerce"
                ).dropna()
                if len(val_series) > 0:
                    min_val = float(val_series.min())
                    max_val = float(val_series.max())
                    if min_val < max_val:
                        value_range = st.slider(
                            "Value Range",
                            min_value=min_val,
                            max_value=max_val,
                            value=(min_val, max_val),
                            key="anomaly_value_filter",
                        )
            else:
                st.caption("No value column")

        # Show only pending anomalies filter
        with filter_cols[3]:
            show_pending_only = st.checkbox(
                "Pending only",
                value=False,
                key="anomaly_pending_filter",
                help="Show only rows already marked as pending anomalies",
            )

    # Apply filters
    filtered_events = display_events.copy()

    if state_filter:
        filtered_events = filtered_events[
            filtered_events["anomaly_state"].isin(state_filter)
        ]

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        filtered_events = filtered_events[
            (filtered_events["Time"].dt.date >= start_date)
            & (filtered_events["Time"].dt.date <= end_date)
        ]

    if value_range and value_col:
        min_v, max_v = value_range
        numeric_values = pd.to_numeric(filtered_events[value_col], errors="coerce")
        filtered_events = filtered_events[
            (numeric_values >= min_v) & (numeric_values <= max_v)
        ]

    # Get anomaly marking status
    pending_ts_set = {
        e.run_event_timestamp_ms
        for e in pending_new_anomalies
        if e.run_event_timestamp_ms
    }
    filtered_events["mark_anomaly"] = filtered_events["timestampMillis"].isin(
        pending_ts_set
    )

    if show_pending_only:
        filtered_events = filtered_events[filtered_events["mark_anomaly"]]

    # Show filtered count
    total_events = len(display_events)
    filtered_count = len(filtered_events)
    if filtered_count < total_events:
        st.caption(f"Showing {filtered_count} of {total_events} events (filtered)")

    # Build display columns
    display_cols = ["Time"]

    if value_col:
        display_cols.append(value_col)

    if has_anomaly_state:
        display_cols.append("anomaly_state")

    display_cols.append("mark_anomaly")

    limited_events = filtered_events.head(MAX_DISPLAY_EVENTS)

    rename_cols = {
        "anomaly_state": "Current State",
        "mark_anomaly": "Mark Anomaly",
    }
    if value_col:
        rename_cols[value_col] = "Value"

    column_config = {
        "Time": st.column_config.DatetimeColumn(
            "Timestamp",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "Current State": st.column_config.TextColumn("Current State", disabled=True),
        "Mark Anomaly": st.column_config.CheckboxColumn(
            "Mark Anomaly",
            help="Check to mark this point as an anomaly",
            default=False,
        ),
    }
    if value_col:
        column_config["Value"] = st.column_config.NumberColumn("Value", format="%.2f")

    disabled_cols = ["Time", "Current State"]
    if value_col:
        disabled_cols.append("Value")

    edited_df = st.data_editor(
        limited_events[display_cols].rename(columns=rename_cols),
        column_config=column_config,
        disabled=disabled_cols,
        hide_index=True,
        use_container_width=True,
        key="anomaly_marking_table",
    )

    # Action buttons
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Apply Changes", type="primary", key="apply_anomaly_changes"):
            anomaly_additions = 0
            anomaly_removals = 0

            for i, (_idx, row) in enumerate(edited_df.iterrows()):
                ts_ms = int(limited_events.iloc[i]["timestampMillis"])
                # Get metric value for the anomaly event payload
                metric_val = None
                if value_col and value_col in limited_events.columns:
                    raw_val = limited_events.iloc[i][value_col]
                    if pd.notna(raw_val):
                        metric_val = float(raw_val)

                is_now_marked = row["Mark Anomaly"]
                was_marked = ts_ms in pending_ts_set

                if is_now_marked and not was_marked:
                    logger.info(
                        "Creating anomaly: ts=%s, value_col=%s, metric_val=%s",
                        ts_ms,
                        value_col,
                        metric_val,
                    )
                    edit_tracker.create_new_anomaly(
                        monitor_urn=monitor_urn,
                        assertion_urn=assertion_urn,
                        run_event_timestamp_ms=ts_ms,
                        metric_value=metric_val,
                    )
                    anomaly_additions += 1
                elif not is_now_marked and was_marked:
                    edit_tracker.remove_new_anomaly(
                        monitor_urn=monitor_urn,
                        run_event_timestamp_ms=ts_ms,
                    )
                    anomaly_removals += 1

            if anomaly_additions or anomaly_removals:
                msg_parts = []
                if anomaly_additions:
                    msg_parts.append(f"{anomaly_additions} anomaly addition(s)")
                if anomaly_removals:
                    msg_parts.append(f"{anomaly_removals} anomaly removal(s)")
                st.success(f"Applied: {', '.join(msg_parts)}")
                st.rerun()

    with col2:
        if anomaly_pending > 0:
            if st.button("Clear All", type="secondary", key="clear_all_anomalies"):
                for edit in pending_new_anomalies:
                    if edit.run_event_timestamp_ms:
                        edit_tracker.remove_new_anomaly(
                            monitor_urn=edit.monitor_urn,
                            run_event_timestamp_ms=edit.run_event_timestamp_ms,
                        )
                st.success("Cleared all pending anomaly markings.")
                st.rerun()

    with col3:
        if anomaly_pending > 0:
            if st.button(
                f"Publish {anomaly_pending} Anomalies",
                type="primary",
                key="publish_anomalies",
            ):
                _publish_anomalies(
                    hostname=hostname,
                    monitor_urn=monitor_urn,
                    edit_tracker=edit_tracker,
                    pending_anomalies=pending_new_anomalies,
                )


def _publish_anomalies(
    hostname: str,
    monitor_urn: Optional[str],
    edit_tracker: AnomalyEditTracker,
    pending_anomalies: list[AnomalyEdit],
) -> None:
    """Publish pending anomaly markings to the API."""
    env_config = get_active_config()
    if not env_config:
        st.error("No API configuration found. Please configure a connection first.")
        return

    base_url = env_config.server.rstrip("/")
    headers = (
        {"Authorization": f"Bearer {env_config.token}"} if env_config.token else {}
    )

    if not pending_anomalies or not monitor_urn:
        st.warning("No anomalies to publish or no monitor configured.")
        return

    from .monitor_browser import _create_anomaly_event_rest

    total_success = 0
    total_errors = []

    with st.spinner(f"Publishing {len(pending_anomalies)} anomalies..."):
        successful_anomalies = []
        for anomaly in pending_anomalies:
            logger.info(
                "Publishing anomaly: monitor_urn=%s, ts=%s",
                anomaly.monitor_urn,
                anomaly.run_event_timestamp_ms,
            )
            success, error = _create_anomaly_event_rest(base_url, headers, anomaly)
            if success:
                total_success += 1
                successful_anomalies.append(anomaly)
                if anomaly.run_event_timestamp_ms:
                    edit_tracker.remove_new_anomaly(
                        monitor_urn=anomaly.monitor_urn,
                        run_event_timestamp_ms=anomaly.run_event_timestamp_ms,
                    )
            else:
                total_errors.append(f"Anomaly: {error}")

        if successful_anomalies:
            cache_dir = get_cache_dir() / hostname_to_dir(hostname)
            from ..common.cache_manager import EndpointCache

            endpoint_cache = EndpointCache(hostname, cache_dir)
            endpoint_cache.update_anomaly_events_after_publish(successful_anomalies)

    # Show results
    if total_success > 0:
        st.success(f"Published {total_success} anomalies successfully.")

    if total_errors:
        st.error(f"Failed to publish {len(total_errors)} anomalies:")
        for err in total_errors[:5]:
            st.text(f"  • {err}")

    if total_success > 0:
        st.rerun()


def _render_data_management_section(
    hostname: str, assertion_urn: str, monitor_urn: Optional[str] = None
) -> None:
    """Render the data management section with navigation to Data Source.

    Args:
        hostname: The endpoint hostname
        assertion_urn: The assertion URN
        monitor_urn: The monitor URN (required for refresh to work properly)
    """
    with st.expander("🔧 Data Management", expanded=False):
        if not monitor_urn:
            st.warning(
                "No monitor URN available for this assertion. "
                "Refresh requires a monitor URN to fetch metric cube data."
            )
            return

        st.markdown(
            "Refresh this assertion's cached data with full control over fetch options."
        )
        if st.button(
            "🔄 Refresh in Data Source →",
            key="go_to_data_source_refresh",
            type="primary",
            help="Go to Data Source page to refresh this monitor's metric cube data",
        ):
            # Pass the monitor URN (not assertion URN) since fetch is monitor-based
            st.session_state["_data_source_filter_monitor"] = monitor_urn
            st.session_state["_data_source_filter_hostname"] = hostname
            # Switch to Fetch Data view (has all the options)
            st.session_state["data_source_view"] = "📥 Fetch Data"
            # Import here to avoid circular import
            from . import data_source_page

            st.switch_page(data_source_page)


def render_assertion_browser_page():
    """Render the metric cube browser page.

    This page displays metric cube data for monitored assertions.
    It uses the monitor-centric architecture where monitors and metric cubes
    are the primary data sources.
    """
    st.header("Metric Cube Browser")
    render_connection_status()

    st.caption(
        "📈 Browsing metric cube timeseries data. "
        "Showing monitors with their associated assertions and metric values."
    )

    init_explorer_state()
    loader = DataLoader()

    hostname = st.session_state.get(_SELECTED_ENDPOINT)

    if not hostname:
        endpoints = loader.list_endpoints()
        if endpoints:
            hostname = endpoints[0].hostname
            st.session_state[_SELECTED_ENDPOINT] = hostname
        else:
            st.warning("No data loaded. Go to Data Source to import data.")
            return

    endpoints = loader.list_endpoints()
    endpoint_options = {f"{ep.alias} ({ep.hostname})": ep.hostname for ep in endpoints}

    current_display = next(
        (k for k, v in endpoint_options.items() if v == hostname),
        list(endpoint_options.keys())[0] if endpoint_options else None,
    )

    if endpoint_options:
        selected_display = st.selectbox(
            "Endpoint",
            options=list(endpoint_options.keys()),
            index=list(endpoint_options.keys()).index(current_display)
            if current_display
            else 0,
        )
        hostname = endpoint_options[selected_display]
        st.session_state[_SELECTED_ENDPOINT] = hostname

    st.markdown("---")

    # Search and filter controls
    col_search, col_status = st.columns([3, 1])

    with col_search:
        search_query = st.text_input(
            "Search Assertion URN", placeholder="Search by URN..."
        )

    with col_status:
        status_options = {"All": None, "Active": "ACTIVE", "Inactive": "PAUSED"}
        selected_status_label = st.selectbox(
            "Status",
            options=list(status_options.keys()),
            index=0,
        )
        status_filter = status_options[selected_status_label]

    # Advanced filters (assertion type, field metric, saved preprocessing)
    with st.expander("🔍 Advanced Filters", expanded=False):
        filter_cols = st.columns(3)

        with filter_cols[0]:
            selected_type_label = st.selectbox(
                "Assertion Type",
                options=list(ASSERTION_TYPE_FILTER_OPTIONS.keys()),
                index=0,
                key="assertion_type_filter",
            )
            assertion_type_filter = ASSERTION_TYPE_FILTER_OPTIONS[selected_type_label]

        with filter_cols[1]:
            field_metric_filter = None
            if assertion_type_filter == "FIELD":
                selected_metric_label = st.selectbox(
                    "Field Metric",
                    options=list(FIELD_METRIC_FILTER_OPTIONS.keys()),
                    index=0,
                    key="field_metric_filter",
                )
                field_metric_filter = FIELD_METRIC_FILTER_OPTIONS[selected_metric_label]
            else:
                st.caption("Select 'Field' type to filter by metric")

        with filter_cols[2]:
            has_saved_preprocessing = st.checkbox(
                "With saved preprocessing",
                value=False,
                key="has_saved_preprocessing_filter",
                help="Show only assertions/monitors that have at least one saved preprocessing",
            )

    # Pagination state
    page_size = 100
    page_key = "assertion_browser_page"

    # Reset page when filters change
    filter_state_key = "assertion_browser_filter_state"
    current_filter_state = (
        f"{hostname}|{search_query}|{status_filter}|"
        f"{assertion_type_filter}|{field_metric_filter}|{has_saved_preprocessing}"
    )
    if st.session_state.get(filter_state_key) != current_filter_state:
        st.session_state[page_key] = 0
        st.session_state[filter_state_key] = current_filter_state

    page = st.session_state.get(page_key, 0)

    # Warn when active connection does not match selected endpoint (enrichment would fail).
    matches, connection_mismatch_msg = connection_matches_endpoint(hostname)
    if not matches and connection_mismatch_msg:
        st.warning(connection_mismatch_msg)

    # Load first page only from cache (DuckDB pagination). When type filter is
    # applied we fetch type info for all matching pages and filter by type.
    search_filter = search_query.strip() if search_query else None
    type_filter_active = bool(assertion_type_filter or field_metric_filter)

    # Avoid spinners here: Streamlit reruns from the top on every interaction
    # (page change, nav button, filter). A spinner would show on every click and
    # block the UI before the button handler runs. Use the type-loading caption
    # below for feedback instead.
    if type_filter_active:
        # Type filter applied: load all pages matching search+status, enrich each
        # page (only missing from cache), then filter by type and paginate.
        all_with_type: list = []
        enrich_total = 0
        enrich_from_cache = 0
        enrich_from_api = 0
        p = 0
        while True:
            page_list, total, stats = loader.get_monitored_assertions_page(
                hostname,
                page=p,
                page_size=page_size,
                search_filter=search_filter,
                status_filter=status_filter,
                has_saved_preprocessing_filter=has_saved_preprocessing,
                enrich=True,
            )
            if not page_list:
                break
            enrich_total += stats.get("total", 0)
            enrich_from_cache += stats.get("from_cache", 0)
            enrich_from_api += stats.get("from_api", 0)
            all_with_type.extend(page_list)
            if (p + 1) * page_size >= total:
                break
            p += 1

        # Filter by type
        filtered_by_type = all_with_type
        if assertion_type_filter:
            filtered_by_type = [
                a for a in filtered_by_type if a.assertion_type == assertion_type_filter
            ]
        if field_metric_filter:
            filtered_by_type = [
                a
                for a in filtered_by_type
                if a.assertion_type == "FIELD"
                and a.field_metric_type == field_metric_filter
            ]
        total_count = len(filtered_by_type)
        start_idx = page * page_size
        end_idx = start_idx + page_size
        assertions = filtered_by_type[start_idx:end_idx]
        type_indicator = (
            f"Type info: {enrich_total} loaded ({enrich_from_cache} from cache, "
            f"{enrich_from_api} from API)"
            if enrich_total
            else ""
        )
    else:
        # No type filter: load only the current page from cache, enrich that page.
        page_assertions, total_count, stats = loader.get_monitored_assertions_page(
            hostname,
            page=page,
            page_size=page_size,
            search_filter=search_filter,
            status_filter=status_filter,
            has_saved_preprocessing_filter=has_saved_preprocessing,
            enrich=True,
        )
        assertions = page_assertions
        from_cache = stats.get("from_cache", 0)
        from_api = stats.get("from_api", 0)
        total_typed = stats.get("total", 0)
        if total_typed:
            type_indicator = (
                f"Type info: {total_typed} on this page "
                f"({from_cache} from cache, {from_api} from API)"
            )
        else:
            type_indicator = ""

    # Calculate pagination info
    total_pages = max(1, (total_count + page_size - 1) // page_size)

    # Pagination controls and type-loading indicator
    st.markdown(f"**Found {total_count:,} monitored assertions**")
    if type_indicator:
        st.caption(f"📋 {type_indicator}")

    if total_count == 0:
        st.info(
            "No monitored assertions found. This could mean:\n"
            "- No monitors have been created for assertions\n"
            "- The cache needs to be refreshed from the API\n\n"
            "Go to **Data Source** to fetch monitor data."
        )
        return

    if total_pages > 1:
        col_prev, col_info, col_next = st.columns([1, 2, 1])
        with col_prev:
            if st.button("← Previous", disabled=page == 0, key="assertion_prev"):
                st.session_state[page_key] = page - 1
                st.rerun()
        with col_info:
            st.markdown(
                f"<div style='text-align: center'>Page {page + 1} of {total_pages}</div>",
                unsafe_allow_html=True,
            )
        with col_next:
            if st.button(
                "Next →", disabled=page >= total_pages - 1, key="assertion_next"
            ):
                st.session_state[page_key] = page + 1
                st.rerun()

    if assertions:
        urns_with_preprocessing: set[str] = set()
        try:
            endpoint_cache = loader.cache.get_endpoint_cache(hostname)
            urns_with_preprocessing = (
                endpoint_cache.get_assertion_urns_with_saved_preprocessing()
            )
        except Exception:
            pass

        table_data = []
        for a in assertions:
            # Format status with emoji
            # Note: None/missing status is treated as ACTIVE
            if a.monitor_status == "ACTIVE" or a.monitor_status is None:
                status_display = "🟢 Active"
            elif a.monitor_status == "PAUSED":
                status_display = "⏸️ Paused"
            else:
                status_display = "-"

            # Format assertion type
            type_display = a.assertion_type or "-"

            # Format field metric (only for FIELD assertions)
            metric_display = "-"
            if a.assertion_type == "FIELD" and a.field_metric_type:
                metric_display = a.field_metric_type

            preprocessing_display = (
                "✓" if a.assertion_urn in urns_with_preprocessing else "—"
            )

            table_data.append(
                {
                    "Assertion URN": a.assertion_urn,
                    "Type": type_display,
                    "Metric": metric_display,
                    "Status": status_display,
                    "Preprocessing": preprocessing_display,
                    "Points": a.point_count,
                    "Anomalies": a.anomaly_count,
                    "First": a.first_event.strftime("%Y-%m-%d")
                    if a.first_event
                    else "-",
                    "Last": a.last_event.strftime("%Y-%m-%d") if a.last_event else "-",
                }
            )

        df = pd.DataFrame(table_data)

        # Check if there's a previously selected assertion to restore
        saved_assertion_urn = st.session_state.get(_SELECTED_ASSERTION)

        selected_rows = st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "Assertion URN": st.column_config.TextColumn(
                    "Assertion URN", width="large"
                ),
                "Type": st.column_config.TextColumn("Type", width="small"),
                "Metric": st.column_config.TextColumn("Metric", width="small"),
                "Status": st.column_config.TextColumn("Status", width="small"),
                "Preprocessing": st.column_config.TextColumn(
                    "Preprocessing", width="small"
                ),
            },
            key="assertion_table",
        )

        # Determine which assertion to show
        selected_assertion: Optional[MonitoredAssertionMetadata] = None

        if selected_rows and selected_rows.get("selection", {}).get("rows"):
            selected_idx = selected_rows["selection"]["rows"][0]
            selected_assertion = assertions[selected_idx]
            set_explorer_context(
                hostname,
                selected_assertion.assertion_urn,
                selected_assertion.monitor_urn,
            )
        elif saved_assertion_urn:
            for a in assertions:
                if a.assertion_urn == saved_assertion_urn:
                    selected_assertion = a
                    set_explorer_context(hostname, a.assertion_urn, a.monitor_urn)
                    break

        # Navigation buttons (below assertions table, after selection)
        if selected_assertion:
            _render_metric_cube_nav_buttons(
                hostname=hostname,
                assertion_urn=selected_assertion.assertion_urn,
                monitor_urn=selected_assertion.monitor_urn,
                key_prefix="metric_cube_table_nav",
            )

        if selected_assertion:
            st.markdown("---")
            st.subheader("Selected Assertion")

            # Store assertion context for other pages (preprocessing / model training).
            # Keep these as raw model fields; mapping to preprocessing semantics is owned
            # by inference_v2 code paths.
            st.session_state["current_assertion_type"] = (
                selected_assertion.assertion_type.lower()
                if selected_assertion.assertion_type
                else None
            )
            st.session_state["current_assertion_metric"] = (
                selected_assertion.field_metric_type
            )
            st.session_state["current_volume_assertion_type"] = (
                selected_assertion.volume_assertion_type
            )

            col1, col2 = st.columns(2)
            with col1:
                # Assertion type / subtype details (useful for preprocessing semantics).
                raw_type = (
                    str(selected_assertion.assertion_type).upper()
                    if selected_assertion.assertion_type
                    else None
                )
                st.markdown(f"**Assertion Type:** {raw_type or '-'}")

                if raw_type == "VOLUME":
                    st.markdown(
                        f"**Volume Subtype:** {selected_assertion.volume_assertion_type or '-'}"
                    )
                    # Also surface cumulative/delta semantics when possible.
                    try:
                        from datahub_executor.common.monitor.inference_v2.volume_semantics import (
                            resolve_volume_series_semantics,
                        )

                        is_cumulative, is_delta = resolve_volume_series_semantics(
                            selected_assertion.volume_assertion_type
                        )
                        if is_delta is not None:
                            st.markdown(
                                f"**Semantics:** {'cumulative' if is_cumulative else 'delta'}"
                            )
                    except Exception:
                        # Best-effort only; avoid breaking UI if deps not available.
                        pass

                if raw_type == "FIELD":
                    st.markdown(
                        f"**Field Metric Type:** {selected_assertion.field_metric_type or '-'}"
                    )

                # Assertion URN with link and copy button
                _render_urn_with_link(
                    "Assertion URN",
                    selected_assertion.assertion_urn,
                    hostname,
                    max_display_length=50,
                )

                # Monitor URN
                if selected_assertion.monitor_urn:
                    _render_urn_with_link(
                        "Monitor URN",
                        selected_assertion.monitor_urn,
                        hostname,
                        max_display_length=50,
                    )

            with col2:
                if selected_assertion.monitor_status:
                    st.markdown(
                        f"**Monitor Status:** {selected_assertion.monitor_status}"
                    )
                st.markdown(f"**Data Points:** {selected_assertion.point_count:,}")
                st.markdown(f"**Anomalies:** {selected_assertion.anomaly_count}")
                st.markdown(
                    f"**Date Range:** {selected_assertion.first_event} to {selected_assertion.last_event}"
                )
                if selected_assertion.value_mean is not None:
                    st.markdown(
                        f"**Value Range:** {selected_assertion.value_min:.2f} to {selected_assertion.value_max:.2f}"
                    )
                    st.markdown(f"**Mean:** {selected_assertion.value_mean:.2f}")

            # Data Management section
            _render_data_management_section(
                hostname,
                selected_assertion.assertion_urn,
                selected_assertion.monitor_urn,
            )

            # Navigation buttons (below selected assertion data management)
            _render_metric_cube_nav_buttons(
                hostname=hostname,
                assertion_urn=selected_assertion.assertion_urn,
                monitor_urn=selected_assertion.monitor_urn,
                key_prefix="metric_cube_data_mgmt_nav",
            )

            # Render time series viewer section inline
            _render_timeseries_viewer_section(
                hostname, selected_assertion.assertion_urn, loader, selected_assertion
            )


__all__ = ["render_assertion_browser_page"]
