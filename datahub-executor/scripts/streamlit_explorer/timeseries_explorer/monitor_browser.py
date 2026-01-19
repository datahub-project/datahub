# ruff: noqa: INP001
"""Monitor Browser page for Time Series Explorer."""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
import requests
import streamlit as st

from ..common import (
    _SELECTED_ASSERTION,
    _SELECTED_ENDPOINT,
    _SELECTED_MONITOR,
    AnomalyEditTracker,
    DataLoader,
    _get_retry_session,
    _render_urn_with_link,
    _shorten_urn,
    init_explorer_state,
    logger,
)


def _load_metric_cube_context_events(
    loader: DataLoader,
    hostname: str,
    monitor_urn: str,
    first_anomaly_datetime: datetime,
    lookback_days: int = 30,
) -> Optional[pd.DataFrame]:
    """Load metric cube events for context around anomalies.

    This provides cleaner metric data than assertion run events since
    the measure value is directly available.

    Args:
        loader: DataLoader instance
        hostname: The endpoint hostname
        monitor_urn: The monitor URN
        first_anomaly_datetime: The datetime of the first anomaly
        lookback_days: Number of days before first anomaly to include

    Returns:
        DataFrame with metric cube events, or None if not available
    """
    if not monitor_urn:
        return None

    # Load metric cube events
    events_df = loader.load_cached_metric_cube_events(hostname, monitor_urn=monitor_urn)

    if events_df is None or len(events_df) == 0:
        return None

    # Calculate the start time (lookback_days before first anomaly)
    start_time = first_anomaly_datetime - timedelta(days=lookback_days)
    start_time_ms = int(start_time.timestamp() * 1000)

    # Filter to events within the context window
    if "timestampMillis" in events_df.columns:
        events_df = events_df[events_df["timestampMillis"] >= start_time_ms].copy()

    if len(events_df) == 0:
        return None

    # Add datetime column if not present
    if "datetime" not in events_df.columns:
        events_df["datetime"] = pd.to_datetime(events_df["timestampMillis"], unit="ms")

    # Use the measure column as metric_value for consistency
    if "measure" in events_df.columns:
        events_df["metric_value"] = pd.to_numeric(events_df["measure"], errors="coerce")

    return events_df


def _load_context_events(
    loader: DataLoader,
    hostname: str,
    monitor_urn: str,
    first_anomaly_datetime: datetime,
    lookback_days: int = 30,
) -> Optional[pd.DataFrame]:
    """Load context events from metric cube data.

    Args:
        loader: DataLoader instance
        hostname: The endpoint hostname
        monitor_urn: The monitor URN
        first_anomaly_datetime: The datetime of the first anomaly
        lookback_days: Number of days before first anomaly to include

    Returns:
        DataFrame with metric cube events, or None if not available
    """
    if not monitor_urn:
        return None

    metric_df = _load_metric_cube_context_events(
        loader, hostname, monitor_urn, first_anomaly_datetime, lookback_days
    )
    return metric_df


def render_monitor_browser_page():
    """Render the monitor browser page for viewing monitor anomaly events."""
    st.header("Monitor Browser")

    init_explorer_state()
    loader = DataLoader()

    # Navigation - Back to Metric Cube Browser
    col_nav_back, col_nav_spacer = st.columns([1, 4])
    with col_nav_back:
        if st.button("← Metric Cube Events", key="go_to_metric_cube_events"):
            from . import metric_cube_browser_page

            st.switch_page(metric_cube_browser_page)

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

    # Tabs for different views
    tab_inference, tab_events = st.tabs(["Monitor Inference Data", "Monitor Events"])

    with tab_inference:
        _render_monitor_inference_data(loader, hostname)

    with tab_events:
        _render_monitor_events_content(loader, hostname)


def _render_monitor_events_content(loader: DataLoader, hostname: str):
    """Render the monitor events content."""
    col1, col2 = st.columns(2)

    with col1:
        aspect_options = {
            "monitorAnomalyEvent": "Anomaly Events",
            "monitorTimeseriesState": "State Snapshots",
        }
        selected_aspect = st.selectbox(
            "Event Type",
            options=list(aspect_options.keys()),
            format_func=lambda x: aspect_options[x],
        )

    with col2:
        search_query = st.text_input("Search (URN)", placeholder="Search...")

    # Pagination state
    page_size = 100
    page_key = "monitor_browser_page"

    # Reset page when filters change
    filter_state_key = "monitor_browser_filter_state"
    current_filter_state = f"{hostname}|{selected_aspect}|{search_query}"
    if st.session_state.get(filter_state_key) != current_filter_state:
        st.session_state[page_key] = 0
        st.session_state[filter_state_key] = current_filter_state

    page = st.session_state.get(page_key, 0)

    # Use paginated loading with search pushed to DuckDB
    with st.spinner("Loading monitors..."):
        monitors, total_count = loader.get_cached_monitors_paginated(
            hostname,
            page=page,
            page_size=page_size,
            search_filter=search_query if search_query else None,
            aspect_name=selected_aspect,
        )

    # Calculate pagination info
    total_pages = max(1, (total_count + page_size - 1) // page_size)

    # Check for previously selected monitor in session state
    saved_monitor_urn = st.session_state.get(_SELECTED_MONITOR)

    # Pagination controls
    st.markdown(f"**Found {total_count:,} monitors**")

    if total_pages > 1:
        col_prev, col_info, col_next = st.columns([1, 2, 1])
        with col_prev:
            if st.button("← Previous", disabled=page == 0, key="monitor_prev"):
                st.session_state[page_key] = page - 1
                st.rerun()
        with col_info:
            st.markdown(
                f"<div style='text-align: center'>Page {page + 1} of {total_pages}</div>",
                unsafe_allow_html=True,
            )
        with col_next:
            if st.button(
                "Next →", disabled=page >= total_pages - 1, key="monitor_next"
            ):
                st.session_state[page_key] = page + 1
                st.rerun()

    if monitors:
        table_data = []
        for m in monitors:
            table_data.append(
                {
                    "Monitor URN": m.monitor_urn,
                    "Entity URN": m.entity_urn or "-",
                    "Type": m.event_type or "-",
                    "Events": m.point_count,
                    "First": m.first_event.strftime("%Y-%m-%d")
                    if m.first_event
                    else "-",
                    "Last": m.last_event.strftime("%Y-%m-%d") if m.last_event else "-",
                }
            )

        df = pd.DataFrame(table_data)

        selected_rows = st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "Monitor URN": st.column_config.TextColumn(
                    "Monitor URN", width="large"
                ),
                "Entity URN": st.column_config.TextColumn("Entity URN", width="medium"),
            },
        )

        # Determine which monitor to show
        selected_monitor = None
        if selected_rows and selected_rows.get("selection", {}).get("rows"):
            selected_idx = selected_rows["selection"]["rows"][0]
            selected_monitor = monitors[selected_idx]
            st.session_state[_SELECTED_MONITOR] = selected_monitor.monitor_urn
        elif saved_monitor_urn:
            # Use saved monitor from session state if no row currently selected
            for m in monitors:
                if m.monitor_urn == saved_monitor_urn:
                    selected_monitor = m
                    break

        if selected_monitor:
            if selected_aspect == "monitorAnomalyEvent":
                _render_monitor_anomaly_review_section(
                    hostname, selected_monitor.monitor_urn, loader
                )
            else:
                st.markdown("---")
                st.subheader("Selected Monitor")
                col1, col2 = st.columns(2)
                with col1:
                    # Monitor URN (monitors don't have direct links)
                    _render_urn_with_link(
                        "URN",
                        selected_monitor.monitor_urn,
                        hostname,
                        max_display_length=50,
                    )

                    # Entity URN with link
                    _render_urn_with_link(
                        "Entity",
                        selected_monitor.entity_urn,
                        hostname,
                        max_display_length=50,
                    )
                with col2:
                    st.markdown(f"**Events:** {selected_monitor.point_count:,}")
                    st.markdown(
                        f"**Date Range:** {selected_monitor.first_event} to {selected_monitor.last_event}"
                    )
    else:
        st.info(
            f"No monitors found with {aspect_options[selected_aspect].lower()}. "
            "Make sure you've fetched monitor data from the API."
        )


def _render_monitor_anomaly_review_section(
    hostname: str, monitor_urn: str, loader: DataLoader
) -> None:
    """Render the anomaly review section for a selected monitor."""
    st.markdown("---")
    st.subheader("Anomaly Review")

    # Show selected monitor info with linkable entity URN
    # Extract entity URN from monitor URN if possible
    from ..common.run_event_extractor import extract_entity_from_monitor_urn

    entity_urn = extract_entity_from_monitor_urn(monitor_urn)

    col_urn, col_entity = st.columns(2)
    with col_urn:
        _render_urn_with_link("Monitor", monitor_urn, hostname, max_display_length=50)
    with col_entity:
        _render_urn_with_link("Entity", entity_urn, hostname, max_display_length=50)

    endpoint_cache = loader.cache.get_endpoint_cache(hostname)
    edit_tracker = AnomalyEditTracker(endpoint_cache.endpoint_dir)

    pending_count = edit_tracker.get_pending_count()
    new_anomaly_count = edit_tracker.get_new_anomaly_count()
    state_change_count = pending_count - new_anomaly_count

    if pending_count > 0:
        st.info(
            f"**{pending_count} pending local changes** not yet published to API "
            f"({new_anomaly_count} new anomalies, {state_change_count} state changes). "
            "Use the **Publish** button below to send changes to the API."
        )

        if new_anomaly_count > 0:
            with st.expander(f"View {new_anomaly_count} pending new anomalies"):
                new_anomalies = edit_tracker.get_new_anomalies()
                new_data = []
                for na in new_anomalies:
                    new_data.append(
                        {
                            "Time": pd.to_datetime(
                                na.run_event_timestamp_ms or 0, unit="ms"
                            ),
                            "Assertion": na.assertion_urn,
                            "Monitor": na.monitor_urn,
                            "Created": na.edited_at[:16],
                        }
                    )
                if new_data:
                    st.dataframe(
                        pd.DataFrame(new_data),
                        hide_index=True,
                        use_container_width=True,
                        column_config={
                            "Time": st.column_config.DatetimeColumn(
                                "Time", format="YYYY-MM-DD HH:mm"
                            ),
                            "Assertion": st.column_config.TextColumn(
                                "Assertion", width="medium"
                            ),
                            "Monitor": st.column_config.TextColumn(
                                "Monitor", width="medium"
                            ),
                        },
                    )

    if st.session_state.get("show_publish_dialog"):
        _render_publish_dialog(edit_tracker, hostname, loader)
        return

    events_df = loader.load_cached_events(
        hostname, aspect_name="monitorAnomalyEvent", entity_urn=monitor_urn
    )

    if events_df is None or len(events_df) == 0:
        if new_anomaly_count > 0:
            st.info(
                "No existing anomaly events, "
                f"but you have **{new_anomaly_count} new anomalies** ready to publish."
            )
        else:
            st.info("No anomaly events found for this monitor.")
        return

    monitor_events = events_df[events_df["monitorUrn"] == monitor_urn].copy()

    if len(monitor_events) == 0:
        st.info("No events for this monitor.")
        return

    # Use source_sourceEventTimestampMillis for display datetime (correlates to assertion run)
    # Fall back to timestampMillis if source timestamp not available
    if "source_sourceEventTimestampMillis" in monitor_events.columns:
        monitor_events["display_timestamp_ms"] = pd.to_numeric(
            monitor_events["source_sourceEventTimestampMillis"], errors="coerce"
        )
        # Fill NaN with timestampMillis fallback
        if "timestampMillis" in monitor_events.columns:
            monitor_events["display_timestamp_ms"] = monitor_events[
                "display_timestamp_ms"
            ].fillna(monitor_events["timestampMillis"])
    elif "timestampMillis" in monitor_events.columns:
        monitor_events["display_timestamp_ms"] = monitor_events["timestampMillis"]

    if "display_timestamp_ms" in monitor_events.columns:
        monitor_events["datetime"] = pd.to_datetime(
            monitor_events["display_timestamp_ms"], unit="ms"
        )
        monitor_events = monitor_events.sort_values(
            "display_timestamp_ms", ascending=False
        )

    def get_effective_state(row):
        original = row.get("state")
        return edit_tracker.get_effective_state(
            monitor_urn, row.get("timestampMillis", 0), original
        )

    monitor_events["effective_state"] = monitor_events.apply(
        get_effective_state, axis=1
    )

    def has_local_edit(row):
        edit = edit_tracker.get_local_edit(monitor_urn, row.get("timestampMillis", 0))
        return edit is not None

    monitor_events["has_local_edit"] = monitor_events.apply(has_local_edit, axis=1)

    # Display assertion URN(s) for this monitor
    if "assertionUrn" in monitor_events.columns:
        assertion_urns = monitor_events["assertionUrn"].dropna().unique().tolist()
        if len(assertion_urns) == 1:
            _render_urn_with_link(
                "Assertion", assertion_urns[0], hostname, max_display_length=60
            )
        elif len(assertion_urns) > 1:
            st.markdown(f"**Assertions:** {len(assertion_urns)} assertions")
            with st.expander("View assertion URNs"):
                for urn in assertion_urns:
                    st.code(urn, language=None)
        else:
            st.caption("No assertion URN found in events")
    else:
        st.caption("No assertionUrn column in monitor events")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Events", len(monitor_events))
    with col2:
        confirmed = (monitor_events["effective_state"] == "CONFIRMED").sum()
        st.metric("Confirmed", confirmed)
    with col3:
        rejected = (monitor_events["effective_state"] == "REJECTED").sum()
        st.metric("Rejected", rejected)
    with col4:
        local_edits = monitor_events["has_local_edit"].sum()
        st.metric("Local Edits", local_edits)

    # =========================================================================
    # Quick Filters
    # =========================================================================
    # Detect value column early for filter UI
    value_col_for_filter = None
    for col_name in [
        "source_assertionMetric_value",
        "event_result_nativeResults_Metric_Value",
    ]:
        if col_name in monitor_events.columns:
            value_col_for_filter = col_name
            break

    with st.expander("🔍 Quick Filters", expanded=True):
        filter_cols = st.columns(4)

        # State filter
        with filter_cols[0]:
            state_filter = st.multiselect(
                "State",
                options=["CONFIRMED", "REJECTED", "Unreviewed", "Local Edit"],
                default=[],
                key="monitor_state_filter",
                placeholder="All states",
            )

        # Date range filter
        with filter_cols[1]:
            if "datetime" in monitor_events.columns and len(monitor_events) > 0:
                min_date = monitor_events["datetime"].min().date()
                max_date = monitor_events["datetime"].max().date()
                date_range = st.date_input(
                    "Date Range",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                    key="monitor_date_filter",
                )
            else:
                date_range = None
                st.caption("No date data")

        # Value range filter
        with filter_cols[2]:
            value_range = None
            if value_col_for_filter:
                val_series = monitor_events[value_col_for_filter].dropna()
                if len(val_series) > 0:
                    min_val = float(val_series.min())
                    max_val = float(val_series.max())
                    if min_val < max_val:
                        value_range = st.slider(
                            "Value Range",
                            min_value=min_val,
                            max_value=max_val,
                            value=(min_val, max_val),
                            key="monitor_value_filter",
                        )
            else:
                st.caption("No value column")

        # Assertion filter (for monitors with multiple assertions)
        with filter_cols[3]:
            assertion_filter: list[str] = []
            if "assertionUrn" in monitor_events.columns:
                assertion_urns = sorted(
                    monitor_events["assertionUrn"].dropna().unique().tolist()
                )
                if len(assertion_urns) > 1:
                    # Show shortened URNs in the selector
                    assertion_filter = st.multiselect(
                        "Assertion",
                        options=assertion_urns,
                        default=[],
                        key="monitor_assertion_filter",
                        placeholder="All assertions",
                        format_func=lambda x: x.split(":")[-1][:20]
                        if ":" in x
                        else x[:20],
                    )
                else:
                    st.caption("Single assertion")
            else:
                st.caption("No assertion data")

    # Apply filters
    filtered_events = monitor_events.copy()

    if state_filter:
        masks = []
        if "CONFIRMED" in state_filter:
            masks.append(filtered_events["effective_state"] == "CONFIRMED")
        if "REJECTED" in state_filter:
            masks.append(filtered_events["effective_state"] == "REJECTED")
        if "Unreviewed" in state_filter:
            masks.append(filtered_events["effective_state"].isna())
        if "Local Edit" in state_filter:
            masks.append(filtered_events["has_local_edit"])
        if masks:
            combined_mask = masks[0]
            for m in masks[1:]:
                combined_mask = combined_mask | m
            filtered_events = filtered_events[combined_mask]

    # Date range filter
    if (
        date_range
        and isinstance(date_range, tuple)
        and len(date_range) == 2
        and "datetime" in filtered_events.columns
    ):
        start_date, end_date = date_range
        filtered_events = filtered_events[
            (filtered_events["datetime"].dt.date >= start_date)
            & (filtered_events["datetime"].dt.date <= end_date)
        ]

    # Value range filter
    if value_range and value_col_for_filter:
        min_v, max_v = value_range
        filtered_events = filtered_events[
            (filtered_events[value_col_for_filter] >= min_v)
            & (filtered_events[value_col_for_filter] <= max_v)
        ]

    # Assertion filter
    if assertion_filter and "assertionUrn" in filtered_events.columns:
        filtered_events = filtered_events[
            filtered_events["assertionUrn"].isin(assertion_filter)
        ]

    # Show filtered count
    total_events = len(monitor_events)
    filtered_count = len(filtered_events)
    if filtered_count < total_events:
        st.caption(f"Showing {filtered_count} of {total_events} events (filtered)")

    if "datetime" in filtered_events.columns and len(filtered_events) > 0:
        from plotly.subplots import make_subplots  # type: ignore[import-untyped]

        has_metric_values = (
            "source_assertionMetric_value" in filtered_events.columns
            and filtered_events["source_assertionMetric_value"].notna().any()
        )

        # Load context events (up to 30 days before first anomaly)
        # Prefer metric cube data with fallback to assertion run events
        context_df = None
        context_status = ""
        first_anomaly_dt = filtered_events["datetime"].min()

        if pd.notna(first_anomaly_dt):
            context_df = _load_context_events(
                loader,
                hostname,
                monitor_urn,
                first_anomaly_dt.to_pydatetime(),
                lookback_days=30,
            )

            if context_df is not None and len(context_df) > 0:
                context_status = f"✓ Loaded {len(context_df)} metric cube events"
            else:
                context_status = "⚠ No metric cube events found in cache"
        else:
            context_status = "⚠ No valid anomaly timestamps"

        # For backward compatibility, also set assertion_context_df
        assertion_context_df = context_df

        if context_status:
            st.caption(f"Assertion context: {context_status}")

        if has_metric_values:
            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.05,
                row_heights=[0.7, 0.3],
                subplot_titles=("Metric Values", "Anomaly States"),
            )

            # Add assertion context events first (background trace)
            if (
                assertion_context_df is not None
                and "metric_value" in assertion_context_df.columns
                and assertion_context_df["metric_value"].notna().any()
            ):
                context_sorted = assertion_context_df.sort_values("datetime")
                fig.add_trace(
                    go.Scatter(
                        x=context_sorted["datetime"],
                        y=context_sorted["metric_value"],
                        mode="lines",
                        name="Assertion Values",
                        line=dict(color="lightgray", width=1),
                        opacity=0.7,
                    ),
                    row=1,
                    col=1,
                )

            sorted_events = filtered_events.sort_values("datetime")
            fig.add_trace(
                go.Scatter(
                    x=sorted_events["datetime"],
                    y=sorted_events["source_assertionMetric_value"],
                    mode="lines+markers",
                    name="Anomaly Values",
                    line=dict(color="blue", width=1),
                    marker=dict(size=4),
                ),
                row=1,
                col=1,
            )

            state_colors = {"CONFIRMED": "green", "REJECTED": "gray", None: "orange"}
            for state, color in state_colors.items():
                mask = filtered_events["effective_state"] == state
                if state is None:
                    mask = filtered_events["effective_state"].isna()
                if mask.any():
                    subset = filtered_events[mask]
                    has_edits = subset["has_local_edit"]
                    fig.add_trace(
                        go.Scatter(
                            x=subset["datetime"],
                            y=subset["source_assertionMetric_value"],
                            mode="markers",
                            name=state or "Unreviewed",
                            marker=dict(
                                color=color,
                                size=12,
                                symbol=[
                                    "circle-open" if e else "circle" for e in has_edits
                                ],
                            ),
                        ),
                        row=1,
                        col=1,
                    )
                    fig.add_trace(
                        go.Scatter(
                            x=subset["datetime"],
                            y=[1] * len(subset),
                            mode="markers",
                            showlegend=False,
                            marker=dict(color=color, size=10),
                        ),
                        row=2,
                        col=1,
                    )

            fig.update_layout(height=450, showlegend=True)
            fig.update_yaxes(title_text="Value", row=1, col=1)
            fig.update_yaxes(visible=False, row=2, col=1)
        else:
            fig = go.Figure()
            state_colors = {"CONFIRMED": "green", "REJECTED": "gray", None: "orange"}
            for state, color in state_colors.items():
                mask = filtered_events["effective_state"] == state
                if state is None:
                    mask = filtered_events["effective_state"].isna()
                if mask.any():
                    subset = filtered_events[mask]
                    has_edits = subset["has_local_edit"]
                    fig.add_trace(
                        go.Scatter(
                            x=subset["datetime"],
                            y=[1] * len(subset),
                            mode="markers",
                            name=state or "Unreviewed",
                            marker=dict(
                                color=color,
                                size=12,
                                symbol=[
                                    "circle-open" if e else "circle" for e in has_edits
                                ],
                            ),
                        )
                    )
            fig.update_layout(
                xaxis_title="Time",
                yaxis_visible=False,
                height=250,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )

        st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Bulk Actions**")
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        if st.button("✅ Confirm All", key="monitor_confirm_all"):
            _bulk_update_anomalies(
                edit_tracker, filtered_events, monitor_urn, "CONFIRMED"
            )
            st.success(f"Marked {len(filtered_events)} as CONFIRMED")
            st.rerun()
    with col2:
        if st.button("❌ Reject All", key="monitor_reject_all"):
            _bulk_update_anomalies(
                edit_tracker, filtered_events, monitor_urn, "REJECTED"
            )
            st.success(f"Marked {len(filtered_events)} as REJECTED")
            st.rerun()
    with col3:
        local_edit_count = (
            filtered_events["has_local_edit"].sum()
            if "has_local_edit" in filtered_events.columns
            else 0
        )
        if st.button(
            f"🗑️ Clear {local_edit_count} Edits",
            disabled=local_edit_count == 0,
            key="monitor_clear_edits",
        ):
            for _, row in filtered_events.iterrows():
                if row.get("has_local_edit"):
                    edit_tracker.clear_local_edit(
                        monitor_urn, int(row["timestampMillis"])
                    )
            st.success(f"Cleared {local_edit_count} local edits")
            st.rerun()
    with col4:
        pending_count = edit_tracker.get_pending_count()
        if st.button(
            "🗑️ Discard All",
            type="secondary",
            disabled=pending_count == 0,
            key="monitor_discard_all",
        ):
            edit_tracker.clear_all_edits()
            st.success("All local changes discarded")
            st.rerun()
    with col5:
        pending_count = edit_tracker.get_pending_count()
        if st.button(
            f"🚀 Publish {pending_count}",
            type="primary",
            disabled=pending_count == 0,
            key="monitor_publish_btn",
        ):
            st.session_state["show_publish_dialog"] = True
            st.rerun()

    st.markdown("**Review Anomalies**")
    st.caption(
        "Select rows and use bulk actions, or edit the Review column directly. "
        "Changes saved locally, then published to API."
    )

    if len(filtered_events) > 0:
        edit_df = filtered_events.head(100).copy()

        def get_after_state(row):
            """Only show After state if there's a local edit."""
            if row.get("has_local_edit"):
                state = row.get("effective_state")
                if pd.isna(state) or state is None or state == "":
                    return "CONFIRMED"
                s_upper = str(state).upper()
                if s_upper in ("CONFIRMED", "REJECTED", "DELETE"):
                    return s_upper
                return "CONFIRMED"
            return ""  # Empty if no local edit

        edit_df["new_state"] = edit_df.apply(get_after_state, axis=1)

        # Add selection column
        edit_df["select"] = False

        # Build column order: select, datetime, assertion, value, then Before/After at end
        display_cols = ["select", "datetime"]
        if "assertionUrn" in edit_df.columns:
            # Show shortened assertion URN
            edit_df["assertion_short"] = edit_df["assertionUrn"].apply(
                lambda x: _shorten_urn(x, 30) if x else "-"
            )
            display_cols.append("assertion_short")

        value_col = None
        for col_name in [
            "source_assertionMetric_value",
            "event_result_nativeResults_Metric_Value",
        ]:
            if col_name in edit_df.columns:
                value_col = col_name
                break
        if value_col:
            edit_df["value"] = pd.to_numeric(edit_df[value_col], errors="coerce")
            display_cols.append("value")

        # Before and After columns at the end, next to each other
        display_cols.extend(["state", "new_state"])

        available_cols = [c for c in display_cols if c in edit_df.columns]

        column_config = {
            "select": st.column_config.CheckboxColumn(
                "✓", default=False, width="small"
            ),
            "datetime": st.column_config.DatetimeColumn(
                "Time", format="YYYY-MM-DD HH:mm"
            ),
            "assertion_short": st.column_config.TextColumn(
                "Assertion", disabled=True, width="medium"
            ),
            "state": st.column_config.TextColumn("Before", disabled=True),
            "new_state": st.column_config.SelectboxColumn(
                "After", options=["", "CONFIRMED", "REJECTED", "DELETE"], required=False
            ),
            "assertionUrn": st.column_config.TextColumn(
                "Assertion", disabled=True, width="medium"
            ),
        }
        if value_col:
            column_config["value"] = st.column_config.NumberColumn(
                "Value", format="%.2f", disabled=True
            )

        # Use a version counter in the key to force refresh after bulk edits
        table_version = st.session_state.get("anomaly_table_version", 0)
        edited_df = st.data_editor(
            edit_df[available_cols],
            column_config=column_config,
            disabled=[c for c in available_cols if c not in ("new_state", "select")],
            hide_index=True,
            use_container_width=True,
            key=f"monitor_anomaly_table_{table_version}",
        )

        # Count selected rows
        selected_count = (
            edited_df["select"].sum() if "select" in edited_df.columns else 0
        )

        # Bulk actions for selected rows
        if selected_count > 0:
            st.markdown(f"**{selected_count} rows selected**")
            bcol1, bcol2, bcol3 = st.columns(3)
            with bcol1:
                if st.button(
                    f"✅ Confirm {selected_count}",
                    type="primary",
                    key="bulk_confirm_selected",
                ):
                    for idx in range(len(edited_df)):
                        if edited_df.iloc[idx]["select"]:
                            original_row = edit_df.iloc[idx]
                            edit_tracker.set_local_state(
                                monitor_urn,
                                str(original_row.get("assertionUrn", "")),
                                int(original_row.get("timestampMillis", 0)),
                                str(original_row.get("state"))
                                if original_row.get("state")
                                else None,
                                "CONFIRMED",
                            )
                    st.session_state["anomaly_table_version"] = table_version + 1
                    st.rerun()
            with bcol2:
                if st.button(
                    f"❌ Reject {selected_count}",
                    type="secondary",
                    key="bulk_reject_selected",
                ):
                    for idx in range(len(edited_df)):
                        if edited_df.iloc[idx]["select"]:
                            original_row = edit_df.iloc[idx]
                            edit_tracker.set_local_state(
                                monitor_urn,
                                str(original_row.get("assertionUrn", "")),
                                int(original_row.get("timestampMillis", 0)),
                                str(original_row.get("state"))
                                if original_row.get("state")
                                else None,
                                "REJECTED",
                            )
                    st.session_state["anomaly_table_version"] = table_version + 1
                    st.rerun()
            with bcol3:
                if st.button(
                    f"🗑️ Delete {selected_count}",
                    type="secondary",
                    key="bulk_delete_selected",
                ):
                    for idx in range(len(edited_df)):
                        if edited_df.iloc[idx]["select"]:
                            original_row = edit_df.iloc[idx]
                            edit_tracker.set_local_state(
                                monitor_urn,
                                str(original_row.get("assertionUrn", "")),
                                int(original_row.get("timestampMillis", 0)),
                                str(original_row.get("state"))
                                if original_row.get("state")
                                else None,
                                "DELETE",
                            )
                    st.session_state["anomaly_table_version"] = table_version + 1
                    st.rerun()

        # Check for inline edits (changes made directly in the After column)
        pending_table_changes = []
        for idx in range(len(edited_df)):
            edited_row = edited_df.iloc[idx]
            original_row = edit_df.iloc[idx]
            new_state = str(edited_row.get("new_state", "")).strip()
            original_effective = str(original_row.get("new_state", "")).strip()

            # Only count as change if new_state is a valid action and different from original
            if (
                new_state in ("CONFIRMED", "REJECTED", "DELETE")
                and new_state != original_effective
            ):
                pending_table_changes.append(
                    {
                        "idx": idx,
                        "timestamp_ms": int(original_row["timestampMillis"])
                        if "timestampMillis" in original_row.index
                        else 0,
                        "original_state": original_row["state"]
                        if "state" in original_row.index
                        else None,
                        "new_state": new_state,
                        "assertion_urn": str(original_row["assertionUrn"])
                        if "assertionUrn" in original_row.index
                        else "",
                    }
                )

        if pending_table_changes:
            st.info(
                f"📝 **{len(pending_table_changes)} inline changes** - click 'Save' below"
            )
            if st.button(
                f"💾 Save {len(pending_table_changes)} Inline Changes",
                type="primary",
                key="monitor_save_changes",
            ):
                for change in pending_table_changes:
                    edit_tracker.set_local_state(
                        monitor_urn,
                        str(change["assertion_urn"]),
                        int(change["timestamp_ms"]) if change["timestamp_ms"] else 0,
                        str(change["original_state"])
                        if change["original_state"]
                        else None,
                        str(change["new_state"]),
                    )
                st.session_state["anomaly_table_version"] = table_version + 1
                st.rerun()

        if len(filtered_events) > 100:
            st.caption(f"Showing first 100 of {len(filtered_events)} events")
    else:
        st.info("No anomaly events match the current filters.")


def _bulk_update_anomalies(
    edit_tracker,
    events_df: pd.DataFrame,
    monitor_urn: str,
    new_state: str,
) -> None:
    """Apply bulk local state updates."""
    for _, row in events_df.iterrows():
        timestamp_ms = int(row.get("timestampMillis", 0))
        original_state = row.get("state")
        assertion_urn = row.get("assertionUrn", "")
        edit_tracker.set_local_state(
            monitor_urn, assertion_urn, timestamp_ms, original_state, new_state
        )


def _render_publish_dialog(edit_tracker, hostname: str, loader) -> None:
    """Render the publish confirmation dialog."""
    st.markdown("---")
    st.subheader("Publish Changes to API")

    pending = edit_tracker.get_pending_changes()

    if not pending:
        st.info("No pending changes to publish.")
        st.session_state["show_publish_dialog"] = False
        return

    st.markdown(f"**{len(pending)} changes** will be published to the API:")

    confirm_count = sum(1 for p in pending if p.local_state == "CONFIRMED")
    reject_count = sum(1 for p in pending if p.local_state == "REJECTED")
    delete_count = sum(1 for p in pending if p.local_state == "DELETE")
    new_count = sum(1 for p in pending if p.is_new)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Confirmed", confirm_count)
    with col2:
        st.metric("Rejected", reject_count)
    with col3:
        st.metric("Delete", delete_count)
    with col4:
        st.metric("New Anomalies", new_count)

    with st.expander("Preview Changes", expanded=True):
        preview_data = [
            {
                "Monitor": p.monitor_urn,
                "Timestamp": datetime.fromtimestamp(p.timestamp_ms / 1000).strftime(
                    "%Y-%m-%d %H:%M"
                ),
                "Original": p.original_state or "Unreviewed",
                "New State": p.local_state,
            }
            for p in pending[:50]
        ]
        st.dataframe(
            pd.DataFrame(preview_data),
            hide_index=True,
            use_container_width=True,
            column_config={
                "Monitor": st.column_config.TextColumn("Monitor", width="large"),
            },
        )
        if len(pending) > 50:
            st.caption(f"... and {len(pending) - 50} more")

    if st.session_state.get("publishing"):
        # Execute publish and show results
        _execute_publish(edit_tracker, pending, hostname, loader)
        st.session_state["publishing"] = False
        st.session_state["show_publish_dialog"] = False
        st.markdown("---")
        if st.button(
            "✓ Done - View Updated Results", type="primary", key="publish_done"
        ):
            st.rerun()
    else:
        # Show confirm/cancel buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Confirm & Publish", type="primary"):
                st.session_state["publishing"] = True
                st.rerun()
        with col2:
            if st.button("Cancel"):
                st.session_state["show_publish_dialog"] = False
                st.rerun()


def _execute_publish(edit_tracker, changes, hostname: str, loader) -> None:
    """Execute the API publish operation."""
    try:
        from ..common.env_config import list_env_files, load_env_config
    except ImportError:
        from env_config import (  # type: ignore[import-not-found,no-redef]
            list_env_files,
            load_env_config,
        )

    config = None
    env_files = list_env_files()
    for _file_path, cfg in env_files:
        if cfg and hostname in cfg.server:
            config = cfg
            break

    if not config:
        config = load_env_config()
        if config and hostname not in config.server:
            config = None

    if not config:
        st.error(f"Could not find API credentials for {hostname}")
        return

    graphql_url = config.server.rstrip("/") + "/api/graphql"
    headers = {"Authorization": f"Bearer {config.token}"} if config.token else {}

    success_count, error_count, errors, successful_changes = _publish_anomaly_changes(
        graphql_url,
        headers,
        changes,
        st.progress,
    )

    if success_count > 0:
        st.success(f"Published {success_count} changes successfully")
        removed = edit_tracker.mark_as_published(successful_changes)
        logger.info("Marked %d/%d changes as published", removed, success_count)

        # Update local cache to reflect published changes (deletes and state updates)
        endpoint_cache = loader.cache.get_endpoint_cache(hostname)
        cache_updated = endpoint_cache.update_anomaly_events_after_publish(
            successful_changes
        )
        if cache_updated > 0:
            logger.info("Updated %d entries in local cache", cache_updated)

    if error_count > 0:
        st.error(f"Failed to publish {error_count} changes")
        for err in errors[:5]:
            st.warning(err)


def _publish_anomaly_changes(
    graphql_url: str,
    headers: dict,
    changes: list,
    progress_callback,
    max_workers: int = 5,
) -> tuple[int, int, list[str], list]:
    """Publish pending local changes to API using concurrent requests."""

    success_count = 0
    error_count = 0
    errors = []
    successful_changes = []
    completed = 0

    progress_bar = progress_callback(0)
    total = len(changes)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_change = {
            executor.submit(
                _publish_single_anomaly, graphql_url, headers, change
            ): change
            for change in changes
        }

        for future in as_completed(future_to_change):
            change = future_to_change[future]
            success, error_msg = future.result()
            if success:
                success_count += 1
                successful_changes.append(change)
            else:
                error_count += 1
                if error_msg:
                    errors.append(error_msg)

            completed += 1
            progress_bar.progress(completed / total)

    return success_count, error_count, errors, successful_changes


def _publish_single_anomaly(
    graphql_url: str,
    headers: dict,
    change,
) -> tuple[bool, Optional[str]]:
    """Publish a single anomaly change using REST API."""
    base_url = graphql_url.replace("/api/graphql", "")

    if change.local_state == "DELETE":
        return _delete_anomaly_event(graphql_url, headers, change)

    return _create_anomaly_event_rest(base_url, headers, change)


def _delete_anomaly_event(
    graphql_url: str,
    headers: dict,
    change,
) -> tuple[bool, Optional[str]]:
    """Delete an anomaly event via the GMS REST API.

    Uses POST /entities?action=delete with time bounds to delete specific
    timeseries events by timestamp.
    """
    base_url = graphql_url.replace("/api/graphql", "")

    timestamp_ms = (
        change.run_event_timestamp_ms if change.is_new else change.timestamp_ms
    )

    # Use GMS REST API for timeseries deletion
    delete_url = f"{base_url}/entities?action=delete"

    # Payload for deleting specific timeseries event by timestamp
    payload = {
        "urn": change.monitor_urn,
        "aspectName": "monitorAnomalyEvent",
        "startTimeMillis": timestamp_ms,
        "endTimeMillis": timestamp_ms,
    }

    logger.info(
        "DELETE request: url=%s, payload=%s",
        delete_url,
        payload,
    )

    try:
        session = _get_retry_session()
        response = session.post(
            delete_url,
            json=payload,
            headers={**headers, "Content-Type": "application/json"},
            timeout=30,
        )

        logger.info(
            "DELETE response: status=%s, body=%s",
            response.status_code,
            response.text[:500] if response.text else "(empty)",
        )

        if response.status_code == 200:
            # Parse response to check if rows were actually deleted
            try:
                result = response.json()
                timeseries_rows = result.get("value", {}).get("timeseriesRows", 0)
                if timeseries_rows > 0:
                    logger.info(
                        "DELETE successful: %d timeseries rows deleted", timeseries_rows
                    )
                    return True, None
                else:
                    logger.warning(
                        "DELETE returned 200 but no rows deleted at timestamp %s",
                        timestamp_ms,
                    )
                    return (
                        False,
                        f"No rows deleted: {_shorten_urn(change.monitor_urn, 40)} @ {timestamp_ms}",
                    )
            except (ValueError, KeyError):
                # If we can't parse the response, treat 200 as success
                logger.info("DELETE returned 200, assuming success")
                return True, None

        # Error statuses
        logger.error("DELETE failed with status %s", response.status_code)
        return (
            False,
            f"Delete failed ({response.status_code}): {_shorten_urn(change.monitor_urn, 40)}",
        )

    except requests.RequestException as e:
        logger.error("DELETE failed: %s", str(e))
        return (
            False,
            f"Delete failed for {_shorten_urn(change.monitor_urn, 40)}: {str(e)}",
        )


def _create_anomaly_event_rest(
    base_url: str,
    headers: dict,
    change,
) -> tuple[bool, Optional[str]]:
    """Create/update an anomaly event via the REST API.

    Uses the entity endpoint which can create/upsert the monitor entity
    along with the aspect, rather than requiring the entity to exist first.
    """
    source_event_ts = (
        change.run_event_timestamp_ms if change.is_new else change.timestamp_ms
    )

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    # Entity endpoint payload - must be an array of entities
    entity_payload = [
        {
            "urn": change.monitor_urn,
            "monitorAnomalyEvent": {
                "value": {
                    "timestampMillis": now_ms,
                    "state": change.local_state,
                    "source": {
                        "type": "USER_FEEDBACK",
                        "sourceUrn": change.assertion_urn,
                        "sourceEventTimestampMillis": source_event_ts,
                    },
                    "created": {"time": now_ms},
                    "lastUpdated": {"time": now_ms},
                }
            },
        }
    ]

    create_url = f"{base_url}/openapi/v3/entity/monitor"

    logger.info(
        "POST request to create anomaly: url=%s, payload=%s",
        create_url,
        entity_payload,
    )

    try:
        session = _get_retry_session()
        response = session.post(
            create_url,
            json=entity_payload,
            headers={**headers, "Content-Type": "application/json"},
            timeout=30,
        )

        logger.info(
            "POST response: status=%s, body=%s",
            response.status_code,
            response.text[:500] if response.text else "(empty)",
        )

        if response.status_code >= 400:
            return (
                False,
                f"{_shorten_urn(change.monitor_urn, 40)}: HTTP {response.status_code}",
            )

        return True, None

    except requests.RequestException as e:
        return (
            False,
            f"Create failed for {_shorten_urn(change.monitor_urn, 40)}: {str(e)}",
        )


# =============================================================================
# Monitor Inference Data Section
# =============================================================================


def _render_monitor_inference_data(loader: DataLoader, hostname: str):
    """Render inference data from monitors."""
    cache = loader.cache.get_endpoint_cache(hostname)

    # List saved inference data
    inference_entries = cache.list_saved_inference_data()

    if not inference_entries:
        st.info(
            "No inference data found.\n\n"
            "Enable **Fetch Inference Data** in Data Source → Cached API Data → "
            "Advanced Options when fetching monitor data."
        )
        return

    st.subheader(f"Saved Inference Data ({len(inference_entries)})")

    # Display as table
    display_data = []
    for entry in inference_entries:
        row = {
            "Entity URN": entry.get("entity_urn", "")[-40:],
            "Forecast Model": entry.get("forecast_model_name") or "—",
            "Anomaly Model": entry.get("anomaly_model_name") or "—",
            "Has Preprocessing": "✓" if entry.get("has_preprocessing_config") else "—",
            "Has Forecast Evals": "✓" if entry.get("has_forecast_evals") else "—",
            "Has Anomaly Evals": "✓" if entry.get("has_anomaly_evals") else "—",
            "Predictions": entry.get("prediction_count", 0),
            "Saved At": entry.get("saved_at", "")[:16],
        }
        display_data.append(row)

    st.dataframe(pd.DataFrame(display_data), hide_index=True, use_container_width=True)

    # Select entry for details
    st.markdown("---")
    st.subheader("Inference Details")

    entity_urns = [
        e.get("entity_urn") for e in inference_entries if e.get("entity_urn")
    ]

    # Restore previously selected assertion if it exists in the list
    saved_assertion = st.session_state.get(_SELECTED_ASSERTION)
    default_index = 0
    if saved_assertion and saved_assertion in entity_urns:
        default_index = entity_urns.index(saved_assertion)

    selected_urn = st.selectbox(
        "Select Entity",
        options=entity_urns,
        index=default_index,
        format_func=lambda x: x[-50:] if x else "",
        key="inference_entity_select",
    )

    # Save selection to session state for persistence
    if selected_urn:
        st.session_state[_SELECTED_ASSERTION] = selected_urn
        inference_data = cache.load_inference_data(selected_urn)
        if inference_data:
            _render_inference_details(
                inference_data, cache=cache, entity_urn=selected_urn
            )
        else:
            st.error(f"Failed to load inference data for: {selected_urn}")


def _render_inference_details(
    data: dict[str, Any], cache: Any = None, entity_urn: str = ""
):
    """Render details for inference data entry.

    Args:
        data: The inference data dictionary
        cache: Optional EndpointCache for editing configs
        entity_urn: Entity URN for saving edits
    """
    col1, col2 = st.columns(2)

    model_config = data.get("model_config") or {}

    with col1:
        st.markdown("**Forecast Model:**")
        forecast_name = model_config.get("forecast_model_name") or "Not configured"
        forecast_version = model_config.get("forecast_model_version") or ""
        st.write(f"{forecast_name} {forecast_version}")

    with col2:
        st.markdown("**Anomaly Model:**")
        anomaly_name = model_config.get("anomaly_model_name") or "Not configured"
        anomaly_version = model_config.get("anomaly_model_version") or ""
        st.write(f"{anomaly_name} {anomaly_version}")

    # Confidence and generation info
    confidence = model_config.get("confidence")
    generated_at = data.get("generated_at")

    info_cols = st.columns(2)
    with info_cols[0]:
        st.metric("Confidence", f"{confidence:.2f}" if confidence else "—")
    with info_cols[1]:
        if generated_at:
            gen_dt = datetime.fromtimestamp(generated_at / 1000)
            st.metric("Generated At", gen_dt.strftime("%Y-%m-%d %H:%M"))
        else:
            st.metric("Generated At", "—")

    # Tabs for different config types
    config_tabs = st.tabs(
        [
            "Predictions",
            "Preprocessing Config",
            "Forecast Config",
            "Anomaly Config",
            "Forecast Evals",
            "Anomaly Evals",
            "Export",
        ]
    )

    with config_tabs[0]:
        _render_predictions(data.get("predictions_df"))

    with config_tabs[1]:
        _render_config_json(
            data.get("preprocessing_config_json"),
            "Preprocessing",
            cache=cache,
            entity_urn=entity_urn,
        )

    with config_tabs[2]:
        _render_config_json(
            data.get("forecast_config_json"),
            "Forecast",
            cache=cache,
            entity_urn=entity_urn,
        )

    with config_tabs[3]:
        _render_config_json(
            data.get("anomaly_config_json"),
            "Anomaly",
            cache=cache,
            entity_urn=entity_urn,
        )

    with config_tabs[4]:
        _render_evals(data.get("forecast_evals_json"), "Forecast")

    with config_tabs[5]:
        _render_evals(data.get("anomaly_evals_json"), "Anomaly")

    with config_tabs[6]:
        _render_config_export(data)


def _render_config_json(
    config_json: Optional[str],
    config_type: str,
    cache: Any = None,
    entity_urn: str = "",
):
    """Render a configuration JSON with optional editing capability.

    Args:
        config_json: The configuration JSON string
        config_type: Type of config (Preprocessing, Forecast, Anomaly)
        cache: Optional EndpointCache for saving edits
        entity_urn: Entity URN for saving edits
    """
    # Check if serializers are available for validation
    import importlib.util

    HAS_SERIALIZERS = (
        importlib.util.find_spec(
            "datahub_executor.common.monitor.inference_v2.inference_utils"
        )
        is not None
    )

    config_type_lower = config_type.lower()
    can_edit = cache is not None and entity_urn

    if not config_json:
        st.info(f"No {config_type_lower} configuration stored.")

        # Allow creating new config if editing is enabled
        if can_edit:
            if st.checkbox(
                f"Create new {config_type_lower} config",
                key=f"create_{config_type_lower}",
            ):
                _render_config_editor(
                    cache=cache,
                    entity_urn=entity_urn,
                    config_type=config_type_lower,
                    initial_json="{}",
                    has_serializers=HAS_SERIALIZERS,
                )
        return

    # View mode vs Edit mode
    if can_edit:
        edit_mode = st.checkbox(
            f"Edit {config_type_lower} config", key=f"edit_{config_type_lower}"
        )
    else:
        edit_mode = False

    if edit_mode:
        _render_config_editor(
            cache=cache,
            entity_urn=entity_urn,
            config_type=config_type_lower,
            initial_json=config_json,
            has_serializers=HAS_SERIALIZERS,
        )
    else:
        try:
            config = json.loads(config_json)
            st.json(config)
        except json.JSONDecodeError:
            st.text(config_json)


def _render_config_editor(
    cache: Any,
    entity_urn: str,
    config_type: str,
    initial_json: str,
    has_serializers: bool = False,
):
    """Render a JSON editor with validation and save capability.

    Args:
        cache: EndpointCache instance
        entity_urn: The entity URN
        config_type: Type of config (preprocessing, forecast, anomaly)
        initial_json: Initial JSON string
        has_serializers: Whether serializers are available for schema validation
    """
    # Format initial JSON nicely
    try:
        formatted = json.dumps(json.loads(initial_json), indent=2)
    except json.JSONDecodeError:
        formatted = initial_json

    edited_json = st.text_area(
        f"{config_type.title()} Configuration",
        value=formatted,
        height=300,
        key=f"json_editor_{config_type}",
    )

    # Validation
    is_valid = False
    validation_error = None

    try:
        json.loads(edited_json)  # Validate JSON syntax
        is_valid = True

        # Additional validation with serializers if available
        if has_serializers:
            try:
                from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
                    AnomalyConfigSerializer,
                    ForecastConfigSerializer,
                    PreprocessingConfigSerializer,
                )

                if config_type == "preprocessing":
                    PreprocessingConfigSerializer.deserialize(edited_json)
                elif config_type == "forecast":
                    ForecastConfigSerializer.deserialize(edited_json)
                elif config_type == "anomaly":
                    AnomalyConfigSerializer.deserialize(edited_json)
            except Exception as e:
                # JSON is valid but doesn't match expected schema
                st.warning(f"⚠️ JSON is valid but may not match expected schema: {e}")

    except json.JSONDecodeError as e:
        is_valid = False
        validation_error = str(e)

    if is_valid:
        st.success("✓ Valid JSON")
    else:
        st.error(f"Invalid JSON: {validation_error}")

    # Save button
    col1, col2 = st.columns(2)

    with col1:
        if st.button(
            f"Save {config_type.title()} Config",
            disabled=not is_valid,
            key=f"save_{config_type}",
        ):
            # Use the update_inference_config method
            kwargs = {}
            if config_type == "preprocessing":
                kwargs["preprocessing_config_json"] = edited_json
            elif config_type == "forecast":
                kwargs["forecast_config_json"] = edited_json
            elif config_type == "anomaly":
                kwargs["anomaly_config_json"] = edited_json

            success = cache.update_inference_config(entity_urn, **kwargs)

            if success:
                st.success(f"Saved {config_type} configuration")
                st.rerun()
            else:
                st.error(f"Failed to save {config_type} configuration")

    with col2:
        if st.button(f"Reset {config_type.title()}", key=f"reset_{config_type}"):
            st.rerun()


def _render_evals(evals_json: Optional[str], eval_type: str):
    """Render training evaluations."""
    if not evals_json:
        st.info(f"No {eval_type.lower()} training evaluations stored.")
        return

    try:
        evals = json.loads(evals_json)

        # Display aggregated metrics prominently
        aggregated = evals.get("aggregated") or {}
        if aggregated:
            st.markdown("**Aggregated Metrics:**")
            metric_cols = st.columns(min(len(aggregated), 4))
            for i, (name, value) in enumerate(aggregated.items()):
                if value is not None:
                    with metric_cols[i % len(metric_cols)]:
                        if isinstance(value, float):
                            st.metric(name.upper(), f"{value:.4f}")
                        else:
                            st.metric(name.upper(), str(value))

        # Show per-horizon metrics if available
        per_horizon = evals.get("per_horizon") or []
        if per_horizon:
            st.markdown("**Per-Horizon Metrics:**")
            horizon_df = pd.DataFrame(per_horizon)
            st.dataframe(horizon_df, hide_index=True, use_container_width=True)

        # Show full JSON in expander
        with st.expander("Full Evaluation Data"):
            st.json(evals)

    except json.JSONDecodeError:
        st.text(evals_json)


def _render_config_export(data: dict[str, Any]):
    """Render configuration export options.

    Args:
        data: The inference data dictionary containing configs
    """
    st.markdown("Export configurations for use in other environments.")

    export_format = st.radio(
        "Export Format",
        options=["JSON", "Python Dict"],
        horizontal=True,
        key="inference_export_format",
    )

    export_cols = st.columns(3)

    with export_cols[0]:
        _export_config_button(
            data.get("preprocessing_config_json"),
            "preprocessing_config",
            export_format,
        )

    with export_cols[1]:
        _export_config_button(
            data.get("forecast_config_json"),
            "forecast_config",
            export_format,
        )

    with export_cols[2]:
        _export_config_button(
            data.get("anomaly_config_json"),
            "anomaly_config",
            export_format,
        )


def _export_config_button(
    config_json: Optional[str],
    filename_base: str,
    format_type: str,
):
    """Render export button for a configuration.

    Args:
        config_json: The configuration JSON string
        filename_base: Base name for the export file
        format_type: Export format ("JSON" or "Python Dict")
    """
    if not config_json:
        st.button(
            f"Export {filename_base.replace('_', ' ').title()}",
            disabled=True,
            key=f"export_{filename_base}",
        )
        return

    try:
        config = json.loads(config_json)
    except json.JSONDecodeError:
        st.button(
            f"Export {filename_base.replace('_', ' ').title()}",
            disabled=True,
            key=f"export_{filename_base}",
        )
        return

    if format_type == "JSON":
        content = json.dumps(config, indent=2)
        filename = f"{filename_base}.json"
        mime = "application/json"
    else:  # Python Dict
        content = f"# {filename_base}\nconfig = {repr(config)}"
        filename = f"{filename_base}.py"
        mime = "text/x-python"

    st.download_button(
        label=f"Download {filename_base.replace('_', ' ').title()}",
        data=content,
        file_name=filename,
        mime=mime,
        key=f"download_{filename_base}",
    )


def _render_predictions(predictions_df: Optional[pd.DataFrame]):
    """Render stored predictions with comprehensive visualization."""
    if predictions_df is None or len(predictions_df) == 0:
        st.info("No predictions stored.")
        return

    # Convert timestamp_ms to datetime if needed
    df_plot = predictions_df.copy()
    time_col = None
    for col in ["timestamp_ms", "ds", "datetime"]:
        if col in df_plot.columns:
            time_col = col
            break

    if time_col == "timestamp_ms":
        df_plot["datetime"] = pd.to_datetime(df_plot[time_col], unit="ms")
        time_col = "datetime"

    # Sort by time
    if time_col:
        df_plot = df_plot.sort_values(time_col)

    # Summary statistics
    st.markdown(f"### Stored Predictions ({len(df_plot)} points)")

    # Calculate summary metrics
    has_actuals = "y" in df_plot.columns
    has_predictions = "yhat" in df_plot.columns
    has_detection_bands = (
        "detection_band_lower" in df_plot.columns
        and "detection_band_upper" in df_plot.columns
    )
    has_anomalies = "is_anomaly" in df_plot.columns
    has_anomaly_scores = "anomaly_score" in df_plot.columns

    # Summary metrics row - use compact display
    metric_cols = st.columns(5)

    with metric_cols[0]:
        st.markdown("**Data Points**")
        st.write(f"{len(df_plot):,}")

    with metric_cols[1]:
        st.markdown("**Date Range**")
        if time_col and len(df_plot) > 0:
            start_date = df_plot[time_col].min().strftime("%Y-%m-%d")
            end_date = df_plot[time_col].max().strftime("%Y-%m-%d")
            st.write(f"{start_date}")
            st.caption(f"→ {end_date}")
        else:
            st.write("—")

    with metric_cols[2]:
        st.markdown("**Anomalies**")
        if has_anomalies:
            anomaly_count = df_plot["is_anomaly"].sum()
            anomaly_pct = anomaly_count / len(df_plot) * 100 if len(df_plot) > 0 else 0
            st.write(f"{anomaly_count} ({anomaly_pct:.1f}%)")
        else:
            st.write("—")

    with metric_cols[3]:
        st.markdown("**MAE**")
        if has_actuals and has_predictions:
            mae = (df_plot["y"] - df_plot["yhat"]).abs().mean()
            st.write(f"{mae:.4f}")
        else:
            st.write("—")

    with metric_cols[4]:
        st.markdown("**Coverage**")
        if has_detection_bands and has_actuals:
            in_band = (
                (df_plot["y"] >= df_plot["detection_band_lower"])
                & (df_plot["y"] <= df_plot["detection_band_upper"])
            ).mean()
            st.write(f"{in_band:.1%}")
        else:
            st.write("—")

    # Visualization tabs
    viz_tabs = st.tabs(["📈 Time Series Plot", "📊 Summary Stats", "📋 Data Table"])

    with viz_tabs[0]:
        _render_predictions_chart(
            df_plot,
            time_col,
            has_actuals,
            has_predictions,
            has_detection_bands,
            has_anomalies,
        )

    with viz_tabs[1]:
        _render_predictions_stats(
            df_plot,
            has_actuals,
            has_predictions,
            has_detection_bands,
            has_anomalies,
            has_anomaly_scores,
        )

    with viz_tabs[2]:
        # Show data table with filtering
        st.markdown("**Stored Prediction Data**")

        # Filter options
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            show_anomalies_only = st.checkbox(
                "Show anomalies only",
                disabled=not has_anomalies,
                key="pred_anomaly_filter",
            )
        with filter_col2:
            max_rows = st.number_input(
                "Max rows",
                min_value=1,
                max_value=max(1, len(df_plot)),
                value=min(100, len(df_plot)),
                key="pred_max_rows",
            )

        display_df = df_plot
        if show_anomalies_only and has_anomalies:
            display_df = df_plot[df_plot["is_anomaly"] == True]  # noqa: E712

        st.dataframe(
            display_df.head(int(max_rows)),
            hide_index=True,
            use_container_width=True,
        )

        if len(display_df) > int(max_rows):
            st.caption(f"Showing {int(max_rows)} of {len(display_df)} rows")


def _render_predictions_chart(
    df_plot: pd.DataFrame,
    time_col: Optional[str],
    has_actuals: bool,
    has_predictions: bool,
    has_detection_bands: bool,
    has_anomalies: bool,
):
    """Render the predictions time series chart."""
    if not time_col:
        st.warning("No timestamp column available for visualization.")
        return

    fig = go.Figure()

    # Detection bands (render first so they appear in background)
    if has_detection_bands:
        fig.add_trace(
            go.Scatter(
                x=df_plot[time_col],
                y=df_plot["detection_band_upper"],
                mode="lines",
                name="Detection Band Upper",
                line=dict(color="rgba(255, 165, 0, 0.5)", width=1, dash="dash"),
                showlegend=True,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_plot[time_col],
                y=df_plot["detection_band_lower"],
                mode="lines",
                name="Detection Band",
                fill="tonexty",
                line=dict(color="rgba(255, 165, 0, 0.5)", width=1, dash="dash"),
                fillcolor="rgba(255, 165, 0, 0.1)",
            )
        )

    # Prediction interval (forecast confidence bands)
    if "yhat_lower" in df_plot.columns and "yhat_upper" in df_plot.columns:
        fig.add_trace(
            go.Scatter(
                x=df_plot[time_col],
                y=df_plot["yhat_upper"],
                mode="lines",
                name="Forecast CI Upper",
                line=dict(color="rgba(100, 149, 237, 0)", width=0),
                showlegend=False,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_plot[time_col],
                y=df_plot["yhat_lower"],
                mode="lines",
                name="Forecast CI",
                fill="tonexty",
                line=dict(color="rgba(100, 149, 237, 0)", width=0),
                fillcolor="rgba(100, 149, 237, 0.2)",
            )
        )

    # Predictions line
    if has_predictions:
        fig.add_trace(
            go.Scatter(
                x=df_plot[time_col],
                y=df_plot["yhat"],
                mode="lines",
                name="Predicted",
                line=dict(color="cornflowerblue", width=2),
            )
        )

    # Actual values - split into normal and anomaly points
    if has_actuals:
        if has_anomalies:
            normal_mask = ~df_plot["is_anomaly"].fillna(False)
            anomaly_mask = df_plot["is_anomaly"].fillna(False)

            # Normal points
            if normal_mask.any():
                fig.add_trace(
                    go.Scatter(
                        x=df_plot.loc[normal_mask, time_col],
                        y=df_plot.loc[normal_mask, "y"],
                        mode="markers",
                        name="Actual",
                        marker=dict(color="blue", size=5),
                    )
                )

            # Anomaly points
            if anomaly_mask.any():
                fig.add_trace(
                    go.Scatter(
                        x=df_plot.loc[anomaly_mask, time_col],
                        y=df_plot.loc[anomaly_mask, "y"],
                        mode="markers",
                        name="Anomaly",
                        marker=dict(
                            color="red",
                            size=10,
                            symbol="x",
                            line=dict(width=2),
                        ),
                    )
                )
        else:
            fig.add_trace(
                go.Scatter(
                    x=df_plot[time_col],
                    y=df_plot["y"],
                    mode="markers",
                    name="Actual",
                    marker=dict(color="blue", size=5),
                )
            )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Value",
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)

    # Legend explanation
    if has_detection_bands or has_anomalies:
        legend_items = []
        if has_detection_bands:
            legend_items.append("🟧 Orange bands = Anomaly detection thresholds")
        if "yhat_lower" in df_plot.columns:
            legend_items.append("🟦 Blue shaded = Forecast confidence interval")
        if has_anomalies:
            legend_items.append("❌ Red X = Detected anomaly")
        st.caption(" | ".join(legend_items))


def _render_predictions_stats(
    df_plot: pd.DataFrame,
    has_actuals: bool,
    has_predictions: bool,
    has_detection_bands: bool,
    has_anomalies: bool,
    has_anomaly_scores: bool,
):
    """Render summary statistics for predictions."""
    stats_col1, stats_col2 = st.columns(2)

    with stats_col1:
        st.markdown("**Value Statistics**")
        value_stats = {}

        if has_actuals:
            value_stats["Actual Mean"] = f"{df_plot['y'].mean():.4f}"
            value_stats["Actual Std"] = f"{df_plot['y'].std():.4f}"
            value_stats["Actual Min"] = f"{df_plot['y'].min():.4f}"
            value_stats["Actual Max"] = f"{df_plot['y'].max():.4f}"

        if has_predictions:
            value_stats["Predicted Mean"] = f"{df_plot['yhat'].mean():.4f}"
            value_stats["Predicted Std"] = f"{df_plot['yhat'].std():.4f}"

        if value_stats:
            st.dataframe(
                pd.DataFrame(list(value_stats.items()), columns=["Metric", "Value"]),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No value statistics available.")

    with stats_col2:
        st.markdown("**Prediction Performance**")
        perf_stats = {}

        if has_actuals and has_predictions:
            residuals = df_plot["y"] - df_plot["yhat"]
            perf_stats["MAE"] = f"{residuals.abs().mean():.4f}"
            perf_stats["RMSE"] = f"{(residuals**2).mean() ** 0.5:.4f}"
            perf_stats["Mean Error"] = f"{residuals.mean():.4f}"
            perf_stats["Residual Std"] = f"{residuals.std():.4f}"

            # MAPE (avoiding division by zero)
            non_zero_mask = df_plot["y"].abs() > 1e-10
            if non_zero_mask.any():
                mape = (
                    residuals[non_zero_mask].abs() / df_plot.loc[non_zero_mask, "y"]
                ).mean() * 100
                perf_stats["MAPE"] = f"{mape:.2f}%"

        if perf_stats:
            st.dataframe(
                pd.DataFrame(list(perf_stats.items()), columns=["Metric", "Value"]),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No performance metrics available.")

    # Detection bands and anomaly stats
    if has_detection_bands or has_anomalies or has_anomaly_scores:
        st.markdown("---")
        st.markdown("**Anomaly Detection Statistics**")

        anomaly_cols = st.columns(3)

        with anomaly_cols[0]:
            if has_detection_bands:
                band_width = (
                    df_plot["detection_band_upper"] - df_plot["detection_band_lower"]
                )
                st.markdown("**Detection Band Stats**")
                band_stats = {
                    "Mean Width": f"{band_width.mean():.4f}",
                    "Min Width": f"{band_width.min():.4f}",
                    "Max Width": f"{band_width.max():.4f}",
                }
                for name, val in band_stats.items():
                    st.text(f"{name}: {val}")

        with anomaly_cols[1]:
            if has_anomalies:
                anomaly_count = df_plot["is_anomaly"].sum()
                total_count = len(df_plot)
                st.markdown("**Anomaly Summary**")
                st.text(f"Total Anomalies: {anomaly_count}")
                st.text(f"Anomaly Rate: {anomaly_count / total_count * 100:.2f}%")
                st.text(f"Normal Points: {total_count - anomaly_count}")

        with anomaly_cols[2]:
            if has_anomaly_scores:
                st.markdown("**Anomaly Score Stats**")
                scores = df_plot["anomaly_score"].dropna()
                if len(scores) > 0:
                    st.text(f"Mean Score: {scores.mean():.4f}")
                    st.text(f"Max Score: {scores.max():.4f}")
                    st.text(f"Min Score: {scores.min():.4f}")


__all__ = ["render_monitor_browser_page"]
