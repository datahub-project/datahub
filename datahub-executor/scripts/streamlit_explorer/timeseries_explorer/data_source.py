# ruff: noqa: INP001
"""Data Source page for Time Series Explorer."""

import pandas as pd
import streamlit as st

from ..common import (
    METRIC_CUBE_ASPECTS,
    MONITOR_ASPECTS,
    DataLoader,
    _render_cached_source,
    init_explorer_state,
    render_connection_status,
)

_DATA_SOURCE_VIEW = "data_source_view"


def render_data_source_page():
    """Render the data source page with cache management."""
    st.header("Data Source")
    render_connection_status()

    init_explorer_state()
    loader = DataLoader()

    # Cache statistics overview
    stats = loader.cache.get_cache_stats()
    col1, col2, col3 = st.columns(3)
    col1.metric("Cached Endpoints", stats["total_endpoints"])
    col2.metric("Total Events", f"{stats['total_events']:,}")
    col3.metric("Cache Size", f"{stats['total_size_mb']:.1f} MB")

    st.markdown("---")

    # Check if we should switch to manage view after successful fetch
    if st.session_state.get("_switch_to_manage_cache"):
        st.session_state[_DATA_SOURCE_VIEW] = "💾 Manage Cache"
        st.session_state["_switch_to_manage_cache"] = False

    # Check if navigated from assertion browser to refresh specific monitor
    # Stay on Fetch Data view since it has all the options
    if st.session_state.get("_data_source_filter_monitor"):
        st.session_state[_DATA_SOURCE_VIEW] = "📥 Fetch Data"

    # Use radio buttons for view switching (controllable via session state)
    view_options = ["📥 Fetch Data", "💾 Manage Cache"]
    default_idx = (
        view_options.index(st.session_state.get(_DATA_SOURCE_VIEW, "📥 Fetch Data"))
        if st.session_state.get(_DATA_SOURCE_VIEW) in view_options
        else 0
    )

    selected_view = st.radio(
        "View",
        view_options,
        index=default_idx,
        horizontal=True,
        label_visibility="collapsed",
    )
    st.session_state[_DATA_SOURCE_VIEW] = selected_view

    if selected_view == "📥 Fetch Data":
        _render_cached_source(loader)
    else:
        _render_cache_manager_content(loader)


def _render_preprocessings_cache_section(cache) -> None:
    """Render the saved preprocessings management section."""
    from datetime import datetime

    from ..common.shared import _shorten_urn

    # Get all preprocessings (unfiltered)
    all_preprocessings = cache.list_saved_preprocessings()

    if not all_preprocessings:
        return

    with st.expander(
        f"Saved Preprocessings ({len(all_preprocessings)})",
        expanded=len(all_preprocessings) > 0,
    ):
        # Group by assertion URN
        by_assertion: dict[str, list[dict]] = {}
        for item in all_preprocessings:
            assertion_urn = item.get("source_assertion_urn") or "(No assertion)"
            if assertion_urn not in by_assertion:
                by_assertion[assertion_urn] = []
            by_assertion[assertion_urn].append(item)

        # Build table data
        table_data = []
        for item in all_preprocessings:
            preprocessing_id = item["preprocessing_id"]
            row_count = item.get("row_count", "?")
            created_at = item.get("created_at", "")
            source_urn = item.get("source_assertion_urn", "")

            if created_at:
                try:
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    created_str = dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    created_str = (
                        created_at[:16] if len(created_at) > 16 else created_at
                    )
            else:
                created_str = ""

            short_urn = _shorten_urn(source_urn, max_length=40) if source_urn else "-"

            table_data.append(
                {
                    "ID": preprocessing_id,
                    "Assertion": short_urn,
                    "Rows": row_count,
                    "Created": created_str,
                }
            )

        st.dataframe(
            pd.DataFrame(table_data),
            use_container_width=True,
            hide_index=True,
        )

        # Summary by assertion
        st.caption(
            f"**{len(all_preprocessings)}** preprocessings across "
            f"**{len(by_assertion)}** assertions"
        )

        # Delete controls
        st.markdown("**Delete Preprocessings**")
        col_select, col_delete, col_delete_all = st.columns([3, 1, 1])

        preprocessing_ids = [item["preprocessing_id"] for item in all_preprocessings]
        with col_select:
            selected_to_delete = st.multiselect(
                "Select preprocessings to delete",
                options=preprocessing_ids,
                key="cache_mgr_preproc_delete_select",
                label_visibility="collapsed",
            )
        with col_delete:
            if st.button(
                f"Delete ({len(selected_to_delete)})",
                disabled=len(selected_to_delete) == 0,
                key="cache_mgr_preproc_delete_btn",
            ):
                for pid in selected_to_delete:
                    cache.delete_preprocessing(pid)
                st.success(f"Deleted {len(selected_to_delete)} preprocessing(s)")
                st.rerun()
        with col_delete_all:
            if st.button(
                "Delete All",
                type="secondary",
                key="cache_mgr_preproc_delete_all_btn",
            ):
                for item in all_preprocessings:
                    cache.delete_preprocessing(item["preprocessing_id"])
                st.success(f"Deleted all {len(all_preprocessings)} preprocessing(s)")
                st.rerun()

        # Bulk delete by assertion
        if len(by_assertion) > 1:
            st.markdown("**Delete by Assertion**")
            assertion_options = list(by_assertion.keys())
            col_assertion, col_count, col_del = st.columns([3, 1, 1])
            with col_assertion:
                selected_assertion = st.selectbox(
                    "Select assertion",
                    options=assertion_options,
                    format_func=lambda x: f"{_shorten_urn(x, 50)} ({len(by_assertion[x])})"
                    if x != "(No assertion)"
                    else f"(No assertion) ({len(by_assertion[x])})",
                    key="cache_mgr_preproc_assertion_select",
                    label_visibility="collapsed",
                )
            with col_count:
                st.markdown(
                    f"**{len(by_assertion.get(selected_assertion, []))}** items"
                )
            with col_del:
                if st.button(
                    "Delete All",
                    key="cache_mgr_preproc_delete_assertion_btn",
                ):
                    items = by_assertion.get(selected_assertion, [])
                    for item in items:
                        cache.delete_preprocessing(item["preprocessing_id"])
                    st.success(f"Deleted {len(items)} preprocessing(s)")
                    st.rerun()


def _render_training_runs_cache_section(cache) -> None:
    """Render the saved manual runs management section."""
    from datetime import datetime

    from ..common.shared import _shorten_urn

    # Get all training runs (unfiltered)
    all_runs = cache.list_saved_training_runs()

    if not all_runs:
        return

    with st.expander(
        f"Saved Manual Runs ({len(all_runs)})", expanded=len(all_runs) > 0
    ):
        # Group by assertion URN
        by_assertion: dict[str, list[dict]] = {}
        for item in all_runs:
            assertion_urn = item.get("assertion_urn") or "(No assertion)"
            if assertion_urn not in by_assertion:
                by_assertion[assertion_urn] = []
            by_assertion[assertion_urn].append(item)

        # Build table data
        table_data = []
        for item in all_runs:
            run_id = item["run_id"]
            model_name = item.get("model_name", "?")
            preprocessing_id = item.get("preprocessing_id", "?")
            created_at = item.get("created_at", "")
            assertion_urn = item.get("assertion_urn", "")
            metrics = item.get("metrics", {})

            if created_at:
                try:
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    created_str = dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    created_str = (
                        created_at[:16] if len(created_at) > 16 else created_at
                    )
            else:
                created_str = ""

            short_urn = (
                _shorten_urn(assertion_urn, max_length=35) if assertion_urn else "-"
            )
            mae = metrics.get("MAE", "?")
            mae_str = f"{mae:.2f}" if isinstance(mae, (int, float)) else str(mae)

            table_data.append(
                {
                    "Run ID": run_id[:30] + "..." if len(run_id) > 30 else run_id,
                    "Model": model_name,
                    "Preprocessing": preprocessing_id,
                    "Assertion": short_urn,
                    "MAE": mae_str,
                    "Created": created_str,
                }
            )

        st.dataframe(
            pd.DataFrame(table_data),
            use_container_width=True,
            hide_index=True,
        )

        # Summary by assertion
        st.caption(
            f"**{len(all_runs)}** manual runs across **{len(by_assertion)}** assertions"
        )

        # Delete controls
        st.markdown("**Delete Manual Runs**")
        col_select, col_delete, col_delete_all = st.columns([3, 1, 1])

        run_ids = [item["run_id"] for item in all_runs]
        with col_select:
            selected_to_delete = st.multiselect(
                "Select runs to delete",
                options=run_ids,
                format_func=lambda x: x[:40] + "..." if len(x) > 40 else x,
                key="cache_mgr_runs_delete_select",
                label_visibility="collapsed",
            )
        with col_delete:
            if st.button(
                f"Delete ({len(selected_to_delete)})",
                disabled=len(selected_to_delete) == 0,
                key="cache_mgr_runs_delete_btn",
            ):
                for rid in selected_to_delete:
                    cache.delete_training_run(rid)
                st.success(f"Deleted {len(selected_to_delete)} manual run(s)")
                st.rerun()
        with col_delete_all:
            if st.button(
                "Delete All",
                type="secondary",
                key="cache_mgr_runs_delete_all_btn",
            ):
                for item in all_runs:
                    cache.delete_training_run(item["run_id"])
                st.success(f"Deleted all {len(all_runs)} manual run(s)")
                st.rerun()

        # Bulk delete by assertion
        if len(by_assertion) > 1:
            st.markdown("**Delete by Assertion**")
            assertion_options = list(by_assertion.keys())
            col_assertion, col_count, col_del = st.columns([3, 1, 1])
            with col_assertion:
                selected_assertion = st.selectbox(
                    "Select assertion",
                    options=assertion_options,
                    format_func=lambda x: f"{_shorten_urn(x, 50)} ({len(by_assertion[x])})"
                    if x != "(No assertion)"
                    else f"(No assertion) ({len(by_assertion[x])})",
                    key="cache_mgr_runs_assertion_select",
                    label_visibility="collapsed",
                )
            with col_count:
                st.markdown(f"**{len(by_assertion.get(selected_assertion, []))}** runs")
            with col_del:
                if st.button(
                    "Delete All",
                    key="cache_mgr_runs_delete_assertion_btn",
                ):
                    items = by_assertion.get(selected_assertion, [])
                    for item in items:
                        cache.delete_training_run(item["run_id"])
                    st.success(f"Deleted {len(items)} manual run(s)")
                    st.rerun()


def _render_cache_manager_content(loader: DataLoader):
    """Render cache manager content (endpoint list and actions)."""
    endpoints = loader.list_endpoints()

    if endpoints:
        st.subheader("Cached Endpoints")

        default_endpoint = loader.registry.get_default_endpoint()

        endpoints_data = []
        for ep in endpoints:
            cache = loader.cache.get_endpoint_cache(ep.hostname)
            index = cache.index.data

            monitor_events = 0
            metric_cube_events = 0
            for aspect_name, aspect_info in index.aspects.items():
                if hasattr(aspect_info, "event_count"):
                    count = aspect_info.event_count
                elif isinstance(aspect_info, dict):
                    count = aspect_info.get("event_count", 0)
                else:
                    count = 0

                if aspect_name in MONITOR_ASPECTS:
                    monitor_events += count
                elif aspect_name in METRIC_CUBE_ASPECTS:
                    metric_cube_events += count

            is_default = ep.hostname == default_endpoint
            preprocessings_count = cache.get_preprocessings_count()
            training_runs_count = cache.get_training_runs_count()

            endpoints_data.append(
                {
                    "Default": "✓" if is_default else "",
                    "Alias": ep.alias,
                    "Hostname": ep.hostname,
                    "Monitor Events": f"{monitor_events:,}",
                    "Metric Cube Events": f"{metric_cube_events:,}",
                    "Preprocessings": preprocessings_count,
                    "Manual Runs": training_runs_count,
                    "Size (MB)": f"{cache.get_cache_size_mb():.1f}",
                    "Last Sync": index.last_sync[:16] if index.last_sync else "Never",
                }
            )

        st.dataframe(
            pd.DataFrame(endpoints_data), hide_index=True, use_container_width=True
        )

        st.markdown("---")
        st.subheader("Endpoint Details")

        endpoint_hostnames = [ep.hostname for ep in endpoints]
        default_endpoint = loader.registry.get_default_endpoint()

        # Check if we navigated from assertion browser with a specific hostname
        nav_hostname = st.session_state.get("_data_source_filter_hostname")

        initial_index = 0
        if nav_hostname and nav_hostname in endpoint_hostnames:
            # Auto-select the endpoint from navigation
            initial_index = endpoint_hostnames.index(nav_hostname)
        elif default_endpoint and default_endpoint in endpoint_hostnames:
            initial_index = endpoint_hostnames.index(default_endpoint)

        selected_hostname = st.selectbox(
            "Select Endpoint",
            options=endpoint_hostnames,
            index=initial_index,
            key="cache_manager_endpoint_select",
        )

        # Endpoint actions - right next to the selector
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button(
                "Set as Default", type="secondary", key="cache_mgr_set_default"
            ):
                loader.registry.set_default_endpoint(selected_hostname)
                st.success(f"Set {selected_hostname} as default")
                st.rerun()

        with col2:
            if st.button("Clear Cache", type="secondary", key="cache_mgr_clear"):
                loader.clear_cache(selected_hostname)
                st.success(f"Cleared cache for {selected_hostname}")
                st.rerun()

        with col3:
            if st.button("Remove Endpoint", type="secondary", key="cache_mgr_remove"):
                loader.cache.clear_cache(selected_hostname)
                loader.registry.remove_endpoint(selected_hostname)
                st.success(f"Removed {selected_hostname}")
                st.rerun()

        st.markdown("---")

        if selected_hostname:
            cache = loader.cache.get_endpoint_cache(selected_hostname)
            index = cache.index.data

            with st.expander("Aspect Breakdown", expanded=len(index.aspects) > 0):
                aspect_data = []
                for aspect_name, aspect_info in index.aspects.items():
                    if hasattr(aspect_info, "event_count"):
                        count = aspect_info.event_count
                        entities = aspect_info.unique_entities
                        last_sync = aspect_info.last_sync
                    elif isinstance(aspect_info, dict):
                        count = aspect_info.get("event_count", 0)
                        entities = aspect_info.get("unique_entities", 0)
                        last_sync = aspect_info.get("last_sync")
                    else:
                        continue

                    if aspect_name in MONITOR_ASPECTS:
                        entity_type = "Monitor"
                    elif aspect_name in METRIC_CUBE_ASPECTS:
                        entity_type = "Metric Cube"
                    else:
                        entity_type = "Unknown"

                    aspect_data.append(
                        {
                            "Aspect": aspect_name,
                            "Type": entity_type,
                            "Events": f"{count:,}",
                            "Entities": entities,
                            "Last Sync": last_sync[:16] if last_sync else "Never",
                        }
                    )

                if aspect_data:
                    st.dataframe(
                        pd.DataFrame(aspect_data),
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.info("No aspects cached for this endpoint.")

            monitors = loader.get_cached_monitors(
                selected_hostname, aspect_name="monitorAnomalyEvent"
            )
            if monitors:
                with st.expander("Monitor Anomaly Statistics", expanded=True):
                    total_anomalies = sum(m.anomaly_count or 0 for m in monitors)
                    total_points = sum(m.point_count for m in monitors)

                    col1, col2, col3 = st.columns(3)
                    col1.metric("Monitors", len(monitors))
                    col2.metric("Total Events", f"{total_points:,}")
                    col3.metric("Anomalies Detected", f"{total_anomalies:,}")

                    events_df = loader.load_cached_events(
                        selected_hostname, aspect_name="monitorAnomalyEvent"
                    )
                    if events_df is not None and "state" in events_df.columns:
                        state_counts = events_df["state"].value_counts()
                        confirmed = state_counts.get("CONFIRMED", 0)
                        rejected = state_counts.get("REJECTED", 0)
                        unreviewed = len(events_df) - confirmed - rejected

                        st.markdown("**Review Status:**")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Confirmed", confirmed)
                        col2.metric("Rejected", rejected)
                        col3.metric("Unreviewed", unreviewed)

            # Saved Preprocessings section
            _render_preprocessings_cache_section(cache)

            # Saved Manual Runs section
            _render_training_runs_cache_section(cache)

    else:
        st.info(
            "No cached endpoints. Use the **Fetch Data** tab to fetch data from the API."
        )

    st.markdown("---")
    with st.expander("Danger Zone"):
        st.warning("This will permanently delete all cached data.")
        if st.button("Clear All Caches", type="primary", key="cache_mgr_clear_all"):
            loader.clear_cache()
            st.success("All caches cleared")
            st.rerun()


__all__ = ["render_data_source_page"]
