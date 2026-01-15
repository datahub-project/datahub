# ruff: noqa: INP001
"""
API fetch logic for Time Series Explorer.

This module contains all the GraphQL and REST API functions for:
- Scrolling and fetching assertions
- Fetching timeseries events (run events, anomaly events, etc.)
- Monitor relationship lookups
- Data conversion to DataFrames
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .inference_loader import (
    fetch_inference_data,
)
from .shared import (
    _SELECTED_ENDPOINT,
    METRIC_CUBE_ASPECTS,
    MONITOR_ASPECTS,
    DataHubEnvConfig,
    DataLoader,
    FetchConfig,
    _check_cancelled,
    _set_cancelled,
    _shorten_urn,
    get_active_config,
    get_cache_dir,
    hostname_to_dir,
    logger,
)

# Thread-local session with retry logic for transient network failures
_thread_local_session: Optional[requests.Session] = None


def _get_retry_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (500, 502, 503, 504),
) -> requests.Session:
    """Get a requests session with retry logic for transient network failures.

    This handles DNS resolution errors, connection errors, and server errors
    with exponential backoff.

    Args:
        retries: Number of retry attempts (default: 3)
        backoff_factor: Exponential backoff factor (default: 0.5)
            - 1st retry waits 0.5s
            - 2nd retry waits 1.0s
            - 3rd retry waits 2.0s
        status_forcelist: HTTP status codes to retry on

    Returns:
        A configured requests.Session with retry logic
    """
    global _thread_local_session

    # Reuse session within thread for connection pooling
    if _thread_local_session is None:
        _thread_local_session = requests.Session()

        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            # Retry on these methods
            allowed_methods=[
                "HEAD",
                "GET",
                "POST",
                "PUT",
                "DELETE",
                "OPTIONS",
                "TRACE",
            ],
            # Retry on connection errors including DNS resolution failures
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        _thread_local_session.mount("http://", adapter)
        _thread_local_session.mount("https://", adapter)

    return _thread_local_session


def _render_cached_source(loader: DataLoader):
    """Render cached API data source options."""
    active_config: DataHubEnvConfig | None = get_active_config()

    if not active_config:
        st.subheader("Cached API Data")
        st.warning(
            "⚠️ No environment selected. Configure a connection in Settings → Connection Settings."
        )
        return

    hostname = active_config.hostname
    cache_dir = get_cache_dir()
    endpoint_dir = cache_dir / hostname_to_dir(hostname)

    # Check for cache existence using the cache index (not legacy parquet path)
    cache_index_path = endpoint_dir / "cache_index.json"
    has_cache = cache_index_path.exists()

    st.subheader(f"Endpoint: {hostname}")

    if has_cache:
        st.success("✓ Cache found")
        st.session_state[_SELECTED_ENDPOINT] = hostname
        from .shared import _DATA_SOURCE

        st.session_state[_DATA_SOURCE] = "cache"

        info = loader.get_endpoint_info(hostname)
        if info:
            col1, col2, col3 = st.columns(3)
            col1.metric("Events", f"{info['event_count']:,}")
            col2.metric("Cache Size", f"{info['cache_size_mb']:.1f} MB")
            col3.metric(
                "Last Sync", info["last_sync"][:10] if info["last_sync"] else "Never"
            )

            aspects = info.get("aspects", {})
            if aspects:
                st.markdown("**Cached Aspects:**")
                aspect_rows = []
                for aspect_name, aspect_info in aspects.items():
                    aspect_rows.append(
                        {
                            "Aspect": aspect_name,
                            "Events": aspect_info.get("event_count", 0),
                            "Entities": aspect_info.get("unique_entities", 0),
                            "Last Sync": aspect_info.get("last_sync", "")[:10]
                            if aspect_info.get("last_sync")
                            else "",
                        }
                    )
                st.dataframe(pd.DataFrame(aspect_rows), hide_index=True)

        st.markdown("---")
        _render_api_fetch_controls(active_config, endpoint_dir, is_update=True)
    else:
        st.info("No cached data found. Fetch data from the API to populate the cache.")
        st.markdown(f"**Cache location:** `{endpoint_dir}`")
        st.markdown("---")
        _render_api_fetch_controls(active_config, endpoint_dir, is_update=False)


def _render_api_fetch_controls(
    config: DataHubEnvConfig,
    cache_path: Path,
    is_update: bool = False,
):
    """Render controls for fetching data from the API.

    This UI uses a monitor-centric architecture where monitors are the primary
    entities discovered via scroll, and metric cubes provide timeseries data.
    """

    st.markdown("### Fetch from API" if not is_update else "### Refresh Cache")

    lookback_days = st.number_input(
        "Lookback (days)",
        min_value=1,
        max_value=365,
        value=90,
        help="Number of days of historical data to fetch",
    )

    default_alias = ""
    if config.source_file:
        source_name = Path(config.source_file).name
        if source_name.startswith(".datahubenv"):
            suffix = source_name.replace(".datahubenv", "").lstrip("_").lstrip("-")
            if suffix:
                default_alias = suffix.replace("_", " ").replace("-", " ").title()

    if not is_update:
        alias = st.text_input(
            "Alias (optional)",
            value=default_alias,
            placeholder="Production",
            help="Friendly name for this endpoint",
        )
    else:
        alias = default_alias

    # Check for pre-populated monitor URN from navigation
    prefilled_urn = st.session_state.get("_data_source_filter_monitor", "")
    prefilled_hostname = st.session_state.get("_data_source_filter_hostname", None)

    # Warn if the source endpoint doesn't match the current endpoint
    if prefilled_urn and prefilled_hostname and prefilled_hostname != config.hostname:
        st.warning(
            f"⚠️ **Endpoint Mismatch**: The monitor you navigated from is on endpoint "
            f"**{prefilled_hostname}**, but you are currently connected to **{config.hostname}**.\n\n"
            f"To refresh the correct monitor, please switch to the matching endpoint in "
            f"**Settings → Connection Settings** or the monitor will not be found."
        )

    sync_mode = st.radio(
        "Sync Mode",
        options=["incremental", "full"],
        format_func=lambda x: "Append new data"
        if x == "incremental"
        else "Replace data",
        index=1 if prefilled_urn else (0 if is_update else 1),
        horizontal=True,
        help="Append: Add new events to existing cache. Replace: Clear and fetch fresh.",
    )

    with st.expander("Advanced Options", expanded=bool(prefilled_urn)):
        st.markdown("**Monitor Mode Filter**")
        mode_cols = st.columns(2)
        with mode_cols[0]:
            include_active = st.checkbox(
                "Active Monitors",
                value=True,
                key="include_active_monitors",
                help="Include monitors that are actively running",
            )
        with mode_cols[1]:
            include_inactive = st.checkbox(
                "Inactive Monitors",
                value=True,
                key="include_inactive_monitors",
                help="Include monitors that are paused or inactive",
            )

        st.markdown("---")

        st.markdown("**Monitor URN Filter (optional)**")
        urn_filter_text = st.text_area(
            "Filter to specific monitors",
            value=prefilled_urn,
            placeholder="urn:li:monitor:abc123\nurn:li:monitor:def456",
            help="Enter specific monitor URNs to fetch (one per line or comma-separated). "
            "Leave empty to fetch all monitors.",
            height=80,
        )

        monitor_urns: list[str] = []
        if urn_filter_text.strip():
            for line in urn_filter_text.split("\n"):
                for urn in line.split(","):
                    urn = urn.strip()
                    if urn and urn.startswith("urn:li:monitor:"):
                        monitor_urns.append(urn)
            if monitor_urns:
                st.info(f"Will fetch **{len(monitor_urns)}** specific monitor(s)")

        st.markdown("---")

        st.markdown("**Monitor Anomaly Events**")
        monitor_aspects: list[str] = []
        cols = st.columns(len(MONITOR_ASPECTS))
        for i, (aspect_name, aspect_info) in enumerate(MONITOR_ASPECTS.items()):
            with cols[i]:
                if st.checkbox(
                    str(aspect_info["label"]),
                    value=bool(aspect_info["default"]),
                    key=f"aspect_{aspect_name}",
                ):
                    monitor_aspects.append(aspect_name)

        st.markdown("---")

        st.markdown("**Metric Cube Events (Timeseries Data)**")
        metric_cube_aspects: list[str] = []
        cols = st.columns(len(METRIC_CUBE_ASPECTS))
        for i, (aspect_name, aspect_info) in enumerate(METRIC_CUBE_ASPECTS.items()):
            with cols[i]:
                if st.checkbox(
                    str(aspect_info["label"]),
                    value=bool(aspect_info["default"]),
                    key=f"aspect_{aspect_name}",
                ):
                    metric_cube_aspects.append(aspect_name)

        st.markdown("---")

        st.markdown("**Inference Data (Model Configs & Training Evals)**")
        fetch_inference = st.checkbox(
            "Fetch Inference Data",
            value=True,
            key="fetch_inference_data",
            help="Fetch model configurations, training evaluations, and predictions "
            "from AssertionEvaluationContext. Enables viewing stored ML model configs "
            "and historical training results.",
        )

        st.markdown("---")

        max_workers = st.number_input(
            "Concurrency",
            min_value=1,
            max_value=100,
            value=30,
            help="Number of parallel API requests. Higher values speed up fetching but may hit rate limits.",
        )

    if config.token:
        st.success("✓ API token configured")
    else:
        st.warning("⚠️ No API token configured - authentication may fail")

    fetch_config = FetchConfig(
        lookback_days=lookback_days,
        monitor_urns=monitor_urns,
        monitor_aspects=monitor_aspects if monitor_aspects else ["monitorAnomalyEvent"],
        metric_cube_aspects=metric_cube_aspects
        if metric_cube_aspects
        else ["dataHubMetricCubeEvent"],
        alias=alias,
        sync_mode=sync_mode,
        max_workers=max_workers,
        include_active_monitors=include_active,
        include_inactive_monitors=include_inactive,
        fetch_inference_data=fetch_inference,
    )

    btn_label = "Refresh Cache" if is_update else "Fetch and Cache"
    can_fetch = monitor_aspects or metric_cube_aspects
    if st.button(btn_label, type="primary", disabled=not can_fetch):
        _set_cancelled(False)
        _execute_api_fetch(config=config, fetch_config=fetch_config)


def _execute_api_fetch(
    config: DataHubEnvConfig,
    fetch_config: FetchConfig,
):
    """Execute monitor-centric scroll and fetch for timeseries aspects.

    This function scrolls through monitors (not assertions) and fetches:
    1. Monitor anomaly events
    2. Metric cube events (timeseries data)

    The assertion URN is extracted from each monitor and included in the
    metric cube events for linking purposes.
    """

    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=fetch_config.lookback_days)
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(end_time.timestamp() * 1000)

        logger.info(
            "Fetch time range: %s to %s (ms: %d to %d)",
            start_time.isoformat(),
            end_time.isoformat(),
            start_time_ms,
            end_time_ms,
        )

        base_url = config.server.rstrip("/")
        graphql_url = f"{base_url}/api/graphql"

        headers = {"Content-Type": "application/json"}
        if config.token:
            headers["Authorization"] = f"Bearer {config.token}"

        monitor_aspects_str = ", ".join(fetch_config.monitor_aspects)
        metric_aspects_str = ", ".join(fetch_config.metric_cube_aspects)

        # Build mode filter description
        mode_filters = []
        if fetch_config.include_active_monitors:
            mode_filters.append("active")
        if fetch_config.include_inactive_monitors:
            mode_filters.append("inactive")
        mode_str = " + ".join(mode_filters) if mode_filters else "none"

        st.info(f"🔄 Fetching monitor data from **{config.hostname}**")
        st.caption(
            f"Monitor aspects: {monitor_aspects_str} | Metric cube aspects: {metric_aspects_str} | "
            f"Modes: {mode_str} | "
            f"Time range: {start_time.strftime('%Y-%m-%d %H:%M')} to "
            f"{end_time.strftime('%Y-%m-%d %H:%M')} ({fetch_config.lookback_days} days)"
        )
        if fetch_config.monitor_urns:
            st.caption(
                f"Filtering to {len(fetch_config.monitor_urns)} specific monitor(s)"
            )

        progress_bar = st.progress(0, text="Starting...")
        status_text = st.empty()

        cancel_col1, cancel_col2 = st.columns([1, 4])
        with cancel_col1:
            if st.button("Cancel", type="secondary", key="cancel_fetch"):
                _set_cancelled(True)
                st.warning("Cancelling fetch... will save partial results.")

        # Initialize aspect events containers
        aspect_events: dict[str, list[dict]] = {}
        for aspect in fetch_config.monitor_aspects:
            aspect_events[aspect] = []
        for aspect in fetch_config.metric_cube_aspects:
            aspect_events[aspect] = []

        # Track monitor -> assertion mapping for enrichment
        monitor_assertion_map: dict[str, str] = {}
        futures_map: dict = {}
        scroll_id: str | None = None
        total_discovered = 0
        total_skipped = 0
        completed = 0
        errors = 0
        scroll_pages = 0
        cancelled = False
        error_messages: list[str] = []
        sample_urn: str | None = None

        with ThreadPoolExecutor(max_workers=fetch_config.max_workers) as pool:
            if fetch_config.monitor_urns:
                # Fetch specific monitors
                for monitor_urn in fetch_config.monitor_urns:
                    if _check_cancelled():
                        cancelled = True
                        break

                    # Fetch monitor anomaly events
                    for aspect in fetch_config.monitor_aspects:
                        future = pool.submit(
                            _fetch_entity_aspect_events,
                            graphql_url,
                            headers,
                            monitor_urn,
                            aspect,
                            start_time_ms,
                            end_time_ms,
                        )
                        futures_map[future] = (monitor_urn, aspect, None)

                    # Fetch metric cube events
                    for aspect in fetch_config.metric_cube_aspects:
                        future = pool.submit(
                            _fetch_metric_cube_events_for_monitor,
                            graphql_url,
                            headers,
                            monitor_urn,
                            start_time_ms,
                            end_time_ms,
                        )
                        futures_map[future] = (monitor_urn, aspect, None)

                    total_discovered += 1
            else:
                # Scroll through all monitors
                while not cancelled:
                    if _check_cancelled():
                        cancelled = True
                        break

                    result = _graphql_scroll_monitors(
                        graphql_url,
                        headers,
                        scroll_id,
                    )
                    scroll_pages += 1

                    search_results = result.get("searchResults", [])
                    scroll_total = result.get("total", 0)
                    scroll_count = result.get("count", 0)
                    logger.info(
                        "Scroll page %d: total=%d, count=%d, results=%d",
                        scroll_pages,
                        scroll_total,
                        scroll_count,
                        len(search_results),
                    )

                    for search_result in search_results:
                        if _check_cancelled():
                            cancelled = True
                            break

                        entity = search_result.get("entity", {})
                        if not entity:
                            continue

                        monitor_urn = entity.get("urn")
                        if not monitor_urn:
                            continue

                        # Filter by monitor mode
                        monitor_info = entity.get("info", {})
                        status_info = monitor_info.get("status", {})
                        monitor_mode = status_info.get("mode", "ACTIVE")

                        is_active = monitor_mode == "ACTIVE"
                        if is_active and not fetch_config.include_active_monitors:
                            total_skipped += 1
                            continue
                        if not is_active and not fetch_config.include_inactive_monitors:
                            total_skipped += 1
                            continue

                        # Extract assertion URN from monitor
                        assertion_urn = _extract_assertion_urn_from_monitor(entity)
                        if assertion_urn:
                            monitor_assertion_map[monitor_urn] = assertion_urn

                        if sample_urn is None:
                            sample_urn = monitor_urn

                        # Submit fetch tasks for this monitor
                        # Include monitor_mode for enrichment
                        for aspect in fetch_config.monitor_aspects:
                            future = pool.submit(
                                _fetch_entity_aspect_events,
                                graphql_url,
                                headers,
                                monitor_urn,
                                aspect,
                                start_time_ms,
                                end_time_ms,
                            )
                            futures_map[future] = (
                                monitor_urn,
                                aspect,
                                assertion_urn,
                                monitor_mode,
                            )

                        for aspect in fetch_config.metric_cube_aspects:
                            future = pool.submit(
                                _fetch_metric_cube_events_for_monitor,
                                graphql_url,
                                headers,
                                monitor_urn,
                                start_time_ms,
                                end_time_ms,
                            )
                            futures_map[future] = (
                                monitor_urn,
                                aspect,
                                assertion_urn,
                                monitor_mode,
                            )

                        total_discovered += 1

                    # Process completed futures while scrolling
                    skipped_str = f" (skipped {total_skipped})" if total_skipped else ""
                    status_text.markdown(
                        f"📡 Found **{total_discovered}** monitors{skipped_str}, "
                        f"page {scroll_pages} | "
                        f"fetched **{sum(len(e) for e in aspect_events.values()):,}** events"
                    )

                    done_futures = [f for f in futures_map if f.done()]
                    for future in done_futures:
                        (
                            monitor_urn,
                            aspect,
                            assertion_urn,
                            monitor_status,
                        ) = futures_map.pop(future)
                        try:
                            result_data = future.result()
                            if aspect in aspect_events and result_data:
                                # Enrich events with assertion URN and monitor status
                                for evt in result_data:
                                    if assertion_urn and "assertionUrn" not in evt:
                                        evt["assertionUrn"] = assertion_urn
                                    if monitor_status:
                                        evt["monitor_status"] = monitor_status
                                aspect_events[aspect].extend(result_data)
                            completed += 1
                        except Exception as e:
                            errors += 1
                            completed += 1
                            error_messages.append(f"{aspect} for {monitor_urn}: {e}")

                        pending = len(futures_map)
                        total_tasks = completed + pending
                        if total_tasks > 0:
                            progress = completed / total_tasks
                            progress_bar.progress(
                                min(0.95, progress),
                                text=f"Fetching: {completed:,} done, {pending:,} pending",
                            )

                    scroll_id = result.get("nextScrollId")
                    if not scroll_id:
                        break

            # Process remaining futures
            remaining_count = len(futures_map)
            if not cancelled and futures_map:
                status_text.markdown(
                    f"📡 Discovered **{total_discovered:,}** monitors | "
                    f"Finishing **{remaining_count:,}** fetches..."
                )

            remaining_futures = list(futures_map.keys())
            for future in as_completed(remaining_futures):
                if _check_cancelled():
                    cancelled = True
                future_data = futures_map.get(future, (None, None, None, None))
                monitor_urn, aspect, assertion_urn, monitor_status = future_data
                if monitor_urn is None:
                    continue
                try:
                    result_data = future.result()
                    if aspect in aspect_events and result_data:
                        # Enrich events with assertion URN and monitor status
                        for evt in result_data:
                            if assertion_urn and "assertionUrn" not in evt:
                                evt["assertionUrn"] = assertion_urn
                            if monitor_status:
                                evt["monitor_status"] = monitor_status
                        aspect_events[aspect].extend(result_data)
                    completed += 1
                except Exception as e:
                    errors += 1
                    completed += 1
                    error_messages.append(f"{aspect} for {monitor_urn}: {e}")

                total_events_so_far = sum(len(e) for e in aspect_events.values())
                still_remaining = len([f for f in remaining_futures if not f.done()])
                total_work = completed + still_remaining
                progress = min(0.95, completed / max(total_work, 1))
                progress_bar.progress(
                    progress,
                    text=f"Finishing: {still_remaining:,} remaining | Events: {total_events_so_far:,}",
                )

        total_events = sum(len(e) for e in aspect_events.values())
        progress_bar.progress(0.95, text=f"Collected {total_events:,} events")

        aspect_summary = []
        for aspect_name, events in aspect_events.items():
            if events:
                aspect_summary.append(f"{aspect_name}: {len(events):,}")
        aspect_str = " | ".join(aspect_summary) if aspect_summary else "none"

        if cancelled:
            status_text.markdown(
                f"⚠️ **Cancelled**: {total_discovered} monitors, "
                f"{total_events:,} events (partial), {errors} errors\n\n"
                f"**Per aspect:** {aspect_str}"
            )
        else:
            status_text.markdown(
                f"✅ **Done**: {total_discovered} monitors, "
                f"{total_events:,} events, {errors} errors\n\n"
                f"**Per aspect:** {aspect_str}"
            )

        if total_events == 0:
            st.warning(
                f"**Debug info:** Scroll returned {total_discovered} monitors, "
                f"{errors} errors. Sample URN: `{sample_urn}`"
            )

            if total_discovered == 0:
                st.info(
                    "**No monitors found.** Possible causes:\n"
                    "- No monitors exist in the system\n"
                    "- The DataHub instance may not have monitors configured\n"
                    "- Try checking the GraphQL API directly"
                )

            if sample_urn:
                st.info(
                    f"Try this GraphQL query manually:\n"
                    f"```\n"
                    f"listMonitorMetrics(input: {{\n"
                    f'  monitorUrn: "{sample_urn}"\n'
                    f"  startTimeMillis: {start_time_ms}\n"
                    f"  endTimeMillis: {end_time_ms}\n"
                    f"}}) {{ metrics {{ assertionMetric {{ timestampMillis value }} }} }}\n"
                    f"```"
                )

        if error_messages:
            with st.expander(
                f"⚠️ {len(error_messages)} errors occurred", expanded=False
            ):
                for msg in error_messages[:50]:
                    st.text(msg)
                if len(error_messages) > 50:
                    st.text(f"... and {len(error_messages) - 50} more")

        if total_events == 0:
            st.warning("No events found in the specified time range.")
            return

        status_text.markdown("💾 Saving to cache...")
        progress_bar.progress(0.98, text="Saving to cache...")

        loader = DataLoader()
        saved_aspects = []

        # Determine effective sync type
        targeted_replace = (
            fetch_config.sync_mode == "full" and len(fetch_config.monitor_urns) > 0
        )

        if targeted_replace:
            # Delete only the specified monitors' data before saving
            cache = loader.cache.get_endpoint_cache(config.hostname)
            for monitor_urn in fetch_config.monitor_urns:
                for aspect_name in fetch_config.monitor_aspects:
                    cache.delete_entity_events(monitor_urn, aspect_name)
                # Also delete metric cube events
                metric_cube_urn = build_metric_cube_urn(monitor_urn)
                for aspect_name in fetch_config.metric_cube_aspects:
                    cache.delete_entity_events(metric_cube_urn, aspect_name)

        for aspect_name, events in aspect_events.items():
            if events:
                df = _convert_aspect_events_to_dataframe(events, aspect_name)
                if df is not None and len(df) > 0:
                    effective_sync_type = (
                        "incremental" if targeted_replace else fetch_config.sync_mode
                    )
                    loader.cache.save_aspect_events(
                        hostname=config.hostname,
                        aspect_name=aspect_name,
                        df=df,
                        alias=fetch_config.alias or config.hostname,
                        sync_type=effective_sync_type,
                    )
                    saved_aspects.append(f"{aspect_name}: {len(df):,}")

        # Fetch inference data if enabled
        inference_saved_count = 0
        if fetch_config.fetch_inference_data and monitor_assertion_map:
            status_text.markdown(
                "🔬 Fetching inference data (model configs & evals)..."
            )
            progress_bar.progress(0.96, text="Fetching inference data...")

            cache = loader.cache.get_endpoint_cache(config.hostname)
            inference_errors = []

            for _monitor_urn, assertion_urn in monitor_assertion_map.items():
                if _check_cancelled():
                    break

                try:
                    inference_data = fetch_inference_data(
                        graphql_url=graphql_url,
                        headers=headers,
                        assertion_urn=assertion_urn,
                    )

                    if inference_data.has_inference_data:
                        # Save inference data to cache
                        model_config = inference_data.model_config
                        cache.save_inference_data(
                            entity_urn=assertion_urn,
                            model_config_dict=model_config.model_dump()
                            if model_config
                            else None,
                            preprocessing_config_json=model_config.preprocessing_config_json
                            if model_config
                            else None,
                            forecast_config_json=model_config.forecast_config_json
                            if model_config
                            else None,
                            anomaly_config_json=model_config.anomaly_config_json
                            if model_config
                            else None,
                            forecast_evals_json=model_config.forecast_evals_json
                            if model_config
                            else None,
                            anomaly_evals_json=model_config.anomaly_evals_json
                            if model_config
                            else None,
                            predictions_df=inference_data.predictions_df,
                            generated_at=inference_data.generated_at,
                        )
                        inference_saved_count += 1
                except Exception as e:
                    inference_errors.append(f"{assertion_urn}: {e}")

            if inference_saved_count > 0:
                saved_aspects.append(f"inference data: {inference_saved_count}")

            if inference_errors:
                with st.expander(
                    f"⚠️ {len(inference_errors)} inference fetch errors", expanded=False
                ):
                    for msg in inference_errors[:20]:
                        st.text(msg)

        progress_bar.progress(1.0, text="Complete!")

        if saved_aspects:
            st.success(
                f"✅ Cached events for {config.hostname}:\n"
                + "\n".join(f"  - {s}" for s in saved_aspects)
            )
            # Switch to Manage Cache view after successful fetch
            st.session_state["_switch_to_manage_cache"] = True
        else:
            st.warning("No valid events to cache.")

        st.session_state[_SELECTED_ENDPOINT] = config.hostname
        _set_cancelled(False)

        # Clear navigation state after successful fetch
        st.session_state.pop("_data_source_filter_monitor", None)
        st.session_state.pop("_data_source_filter_hostname", None)

        st.rerun()

    except requests.exceptions.HTTPError as e:
        st.error(
            f"API request failed: {e.response.status_code} - {e.response.text[:500]}"
        )
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to connect to API: {e}")
    except Exception as e:
        st.error(f"Failed to fetch from API: {e}")
        import traceback

        st.code(traceback.format_exc())
    finally:
        _set_cancelled(False)


def _graphql_scroll_monitors(
    graphql_url: str,
    headers: dict,
    scroll_id: str | None = None,
    batch_size: int = 100,
) -> dict:
    """Scroll one page of monitors using scrollAcrossEntities.

    This is the primary discovery mechanism for the monitor-centric architecture.
    It returns monitors along with their associated assertion and entity URNs.

    Args:
        graphql_url: The GraphQL endpoint URL
        headers: HTTP headers including auth token
        scroll_id: Scroll ID for pagination (None for first page)
        batch_size: Number of monitors to fetch per page

    Returns:
        Dictionary with nextScrollId, count, total, and searchResults
    """
    query = """
    query ScrollMonitors($input: ScrollAcrossEntitiesInput!) {
        scrollAcrossEntities(input: $input) {
            nextScrollId
            count
            total
            searchResults {
                entity {
                    ... on Monitor {
                        urn
                        info {
                            type
                            assertionMonitor {
                                assertions {
                                    assertion {
                                        urn
                                    }
                                }
                            }
                            status {
                                mode
                            }
                        }
                        relationships(
                            input: {
                                types: ["Evaluates"]
                                direction: OUTGOING
                                start: 0
                                count: 1
                            }
                        ) {
                            relationships {
                                entity {
                                    urn
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """

    variables: dict = {
        "input": {
            "types": ["MONITOR"],
            "query": "*",
            "count": batch_size,
        }
    }

    if scroll_id:
        variables["input"]["scrollId"] = scroll_id

    session = _get_retry_session()
    response = session.post(
        graphql_url,
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()

    if "errors" in data and data["errors"]:
        raise ValueError(f"GraphQL error: {data['errors']}")

    return data.get("data", {}).get("scrollAcrossEntities", {})


def _extract_assertion_urn_from_monitor(monitor_entity: dict) -> str | None:
    """Extract the assertion URN from a monitor entity.

    The assertion URN can be found in:
    1. info.assertionMonitor.assertions[0].assertion.urn - direct reference
    2. relationships[Evaluates].entity.urn - relationship to assertion

    Args:
        monitor_entity: The monitor entity from GraphQL

    Returns:
        The assertion URN if found, None otherwise
    """
    # Try direct reference first
    info = monitor_entity.get("info") or {}
    assertion_monitor = info.get("assertionMonitor") or {}
    assertions = assertion_monitor.get("assertions") or []
    if assertions and len(assertions) > 0:
        assertion_obj = assertions[0].get("assertion") or {}
        assertion_urn = assertion_obj.get("urn")
        if assertion_urn:
            return assertion_urn

    # Try relationships
    relationships_data = monitor_entity.get("relationships") or {}
    relationships = relationships_data.get("relationships") or []
    for rel in relationships:
        entity = rel.get("entity") or {}
        urn = entity.get("urn")
        if urn and urn.startswith("urn:li:assertion:"):
            return urn

    return None


def _fetch_entity_aspect_events(
    graphql_url: str,
    headers: dict,
    entity_urn: str,
    aspect_name: str,
    start_time_ms: int,
    end_time_ms: int,
    limit_per_request: int = 1000,
) -> list[dict]:
    """Fetch all events for a specific entity and aspect with time pagination."""
    logger.debug(
        "_fetch_entity_aspect_events: Starting for %s, aspect=%s",
        entity_urn[:50],
        aspect_name,
    )

    all_events: list[dict] = []
    current_end_ms = end_time_ms
    max_iterations = 100

    is_assertion = ":assertion:" in entity_urn
    is_monitor = ":monitor:" in entity_urn

    for _iteration in range(max_iterations):
        if _check_cancelled():
            break

        events = _graphql_timeseries_aspect(
            graphql_url=graphql_url,
            headers=headers,
            entity_urn=entity_urn,
            aspect_name=aspect_name,
            start_time_ms=start_time_ms,
            end_time_ms=current_end_ms,
            limit=limit_per_request,
        )

        if not events:
            break

        for event in events:
            if is_assertion:
                event["assertionUrn"] = entity_urn
            elif is_monitor:
                event["monitorUrn"] = entity_urn
            event["entityUrn"] = entity_urn
            event["aspectName"] = aspect_name

        all_events.extend(events)

        if len(events) >= limit_per_request:
            min_ts = min(e.get("timestampMillis", current_end_ms) for e in events)
            if min_ts >= current_end_ms:
                break
            current_end_ms = min_ts - 1
            if current_end_ms < start_time_ms:
                break
        else:
            break

    return all_events


def _graphql_timeseries_aspect(
    graphql_url: str,
    headers: dict,
    entity_urn: str,
    aspect_name: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int = 1000,
) -> list[dict]:
    """Execute a GraphQL query for a timeseries aspect."""
    if aspect_name == "monitorAnomalyEvent" and ":monitor:" in entity_urn:
        return _graphql_monitor_anomaly_events(
            graphql_url, headers, entity_urn, start_time_ms, end_time_ms, limit
        )

    return []


def _graphql_monitor_anomaly_events(
    graphql_url: str,
    headers: dict,
    monitor_urn: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int = 1000,
) -> list[dict]:
    """Fetch monitor anomaly events, trying GraphQL first then REST API fallback."""
    logger.debug(
        "_graphql_monitor_anomaly_events: urn=%s, startTime=%s, endTime=%s",
        _shorten_urn(monitor_urn, 50),
        start_time_ms,
        end_time_ms,
    )

    events = _try_graphql_monitor_anomalies(
        graphql_url, headers, monitor_urn, start_time_ms, end_time_ms
    )
    if events is not None:
        return events

    logger.debug(
        "Falling back to REST API for monitor anomaly events: %s",
        _shorten_urn(monitor_urn, 50),
    )
    return _rest_monitor_anomaly_events(
        graphql_url, headers, monitor_urn, start_time_ms, end_time_ms, limit
    )


def _try_graphql_monitor_anomalies(
    graphql_url: str,
    headers: dict,
    monitor_urn: str,
    start_time_ms: int,
    end_time_ms: int,
) -> Optional[list[dict]]:
    """Try to fetch anomalies via GraphQL. Returns None if API not available."""
    query = """
    query ListMonitorAnomalies($input: ListMonitorAnomaliesInput!) {
        listMonitorAnomalies(input: $input) {
            anomalies {
                state
                timestampMillis
                source {
                    type
                    sourceUrn
                    sourceEventTimestampMillis
                    properties {
                        assertionMetric {
                            timestampMillis
                            value
                        }
                    }
                }
            }
        }
    }
    """

    variables = {
        "input": {
            "monitorUrn": monitor_urn,
            "startTimeMillis": start_time_ms,
            "endTimeMillis": end_time_ms,
        }
    }

    try:
        session = _get_retry_session()
        response = session.post(
            graphql_url,
            json={"query": query, "variables": variables},
            headers=headers,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data and data["errors"]:
            error_msgs = [e.get("message", "") for e in data["errors"]]
            if any(
                "undefined" in msg.lower() or "unknown" in msg.lower()
                for msg in error_msgs
            ):
                return None
            # ES query exceptions are server-side issues, return empty
            if any(
                "esqueryexception" in msg.lower()
                or "search query failed" in msg.lower()
                for msg in error_msgs
            ):
                logger.debug(
                    "listMonitorAnomalies search error for %s: %s",
                    _shorten_urn(monitor_urn, 50),
                    error_msgs[0][:100] if error_msgs else "unknown",
                )
                return []
            return []

        result = data.get("data", {}).get("listMonitorAnomalies")
        if not result:
            return []

        events = result.get("anomalies", [])

        for event in events:
            event["monitorUrn"] = monitor_urn

        return events

    except requests.exceptions.RequestException:
        logger.debug(
            "_try_graphql_monitor_anomalies: Request failed for %s",
            _shorten_urn(monitor_urn, 50),
        )
        return None
    except Exception:
        logger.debug(
            "_try_graphql_monitor_anomalies: Exception for %s",
            _shorten_urn(monitor_urn, 50),
        )
        return []


def _rest_monitor_anomaly_events(
    graphql_url: str,
    headers: dict,
    monitor_urn: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int = 1000,
) -> list[dict]:
    """Fetch monitor anomaly events via REST API (fallback for older DataHub)."""

    base_url = graphql_url.replace("/api/graphql", "")
    rest_url = f"{base_url}/openapi/v2/timeseries/monitor/monitorAnomalyEvent"

    all_events = []
    scroll_id = None
    session = _get_retry_session()

    try:
        while True:
            params = {
                "count": min(limit, 100),
                "startTimeMillis": start_time_ms,
                "endTimeMillis": end_time_ms,
            }
            if scroll_id:
                params["scrollId"] = scroll_id

            response = session.get(
                rest_url,
                headers=headers,
                params=params,
                timeout=60,
            )

            if response.status_code == 404:
                return []

            if response.status_code >= 500:
                # Server error - don't log full traceback, just debug
                logger.debug(
                    "_rest_monitor_anomaly_events: Server error %s for %s",
                    response.status_code,
                    _shorten_urn(monitor_urn, 50),
                )
                return []

            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            if not results:
                break

            for result in results:
                if result.get("urn") == monitor_urn:
                    event = result.get("event", {})
                    event["monitorUrn"] = monitor_urn
                    event["timestampMillis"] = result.get("timestampMillis")
                    all_events.append(event)

            scroll_id = data.get("scrollId")
            if not scroll_id or len(all_events) >= limit:
                break

        return all_events

    except Exception:
        logger.exception("_rest_monitor_anomaly_events: Exception")
        return []


def _convert_aspect_events_to_dataframe(
    events: list[dict], aspect_name: str
) -> pd.DataFrame:
    """Convert timeseries events to DataFrame format, preserving raw data."""
    if aspect_name == "monitorAnomalyEvent":
        return _convert_monitor_anomaly_events(events)
    elif aspect_name == "monitorTimeseriesState":
        return _convert_monitor_state_events(events)
    elif aspect_name == "dataHubMetricCubeEvent":
        return _convert_metric_cube_events(events)
    else:
        return _convert_generic_events(events)


def _convert_monitor_anomaly_events(events: list[dict]) -> pd.DataFrame:
    """Convert monitor anomaly events to DataFrame format."""
    rows = []

    for event in events:
        row = {
            "timestampMillis": event.get("timestampMillis"),
            "monitorUrn": event.get("monitorUrn"),
            "entityUrn": event.get("entityUrn"),
            "aspectName": event.get("aspectName", "monitorAnomalyEvent"),
            "state": event.get("state"),
        }

        source = event.get("source", {}) or {}
        row["source_type"] = source.get("type")
        source_urn = source.get("sourceUrn")
        row["source_sourceUrn"] = source_urn
        row["source_sourceEventTimestampMillis"] = source.get(
            "sourceEventTimestampMillis"
        )

        # Extract assertionUrn from source.sourceUrn if it's an assertion URN
        # This enables linking anomaly events to their source assertion
        if source_urn and ":assertion:" in str(source_urn):
            row["assertionUrn"] = source_urn

        source_props = source.get("properties", {}) or {}
        assertion_metric = source_props.get("assertionMetric", {}) or {}
        if assertion_metric:
            row["source_assertionMetric_timestampMillis"] = assertion_metric.get(
                "timestampMillis"
            )
            row["source_assertionMetric_value"] = assertion_metric.get("value")

        rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if "timestampMillis" in df.columns:
        df["timestampMillis"] = pd.to_numeric(df["timestampMillis"], errors="coerce")

    # For anomaly events, use source_sourceEventTimestampMillis for display datetime
    # This is the timestamp of the original assertion run event being marked as anomaly
    if "source_sourceEventTimestampMillis" in df.columns:
        df["source_sourceEventTimestampMillis"] = pd.to_numeric(
            df["source_sourceEventTimestampMillis"], errors="coerce"
        )
        df["datetime"] = pd.to_datetime(
            df["source_sourceEventTimestampMillis"], unit="ms", errors="coerce"
        )
    elif "timestampMillis" in df.columns:
        # Fallback to anomaly event timestamp if source timestamp not available
        df["datetime"] = pd.to_datetime(
            df["timestampMillis"], unit="ms", errors="coerce"
        )

    return df


def _convert_monitor_state_events(events: list[dict]) -> pd.DataFrame:
    """Convert monitor timeseries state events to DataFrame format."""
    rows = []

    for event in events:
        row = {
            "timestampMillis": event.get("timestampMillis"),
            "monitorUrn": event.get("monitorUrn"),
            "entityUrn": event.get("entityUrn"),
            "aspectName": event.get("aspectName", "monitorTimeseriesState"),
            "id": event.get("id"),
        }

        custom_props = event.get("customProperties", {}) or {}
        for key, value in custom_props.items():
            row[f"prop_{key}"] = value

        rows.append(row)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _convert_generic_events(events: list[dict]) -> pd.DataFrame:
    """Convert generic timeseries events to DataFrame format."""
    import json

    rows = []

    def flatten(obj: dict, target_row: dict, prefix: str = "") -> None:
        for key, value in obj.items():
            full_key = f"{prefix}_{key}" if prefix else key
            if isinstance(value, dict):
                flatten(value, target_row, full_key)
            elif isinstance(value, list):
                target_row[full_key] = json.dumps(value)
            else:
                target_row[full_key] = value

    for event in events:
        row: dict = {}
        flatten(event, row)
        rows.append(row)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# =============================================================================
# Metric Cube Functions
# =============================================================================


def build_metric_cube_urn(monitor_urn: str) -> str:
    """Build a metric cube URN from a monitor URN.

    The metric cube URN is derived from the monitor URN using Base64 URL-safe encoding.
    This matches the Java implementation in MonitorMetricsResolver.java.

    Args:
        monitor_urn: The monitor URN (e.g., "urn:li:monitor:...")

    Returns:
        The metric cube URN (e.g., "urn:li:dataHubMetricCube:...")
    """
    # Re-use the shared utility
    from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
        make_monitor_metric_cube_urn,
    )

    return make_monitor_metric_cube_urn(monitor_urn)


def _graphql_list_monitor_metrics(
    graphql_url: str,
    headers: dict,
    monitor_urn: str,
    start_time_ms: int,
    end_time_ms: int,
) -> list[dict]:
    """Fetch metric cube events for a monitor via the listMonitorMetrics GraphQL query.

    This query returns metrics with hydrated anomaly events, providing a cleaner
    data source than extracting metrics from assertion run events.

    Args:
        graphql_url: The GraphQL endpoint URL
        headers: HTTP headers including auth token
        monitor_urn: The monitor URN
        start_time_ms: Start time in milliseconds
        end_time_ms: End time in milliseconds

    Returns:
        List of metric events with optional anomaly data
    """
    query = """
    query ListMonitorMetrics($input: ListMonitorMetricsInput!) {
        listMonitorMetrics(input: $input) {
            metrics {
                assertionMetric {
                    timestampMillis
                    value
                }
                anomalyEvent {
                    timestampMillis
                    state
                    source {
                        type
                        sourceUrn
                        sourceEventTimestampMillis
                    }
                }
            }
        }
    }
    """

    variables = {
        "input": {
            "monitorUrn": monitor_urn,
            "startTimeMillis": start_time_ms,
            "endTimeMillis": end_time_ms,
        }
    }

    logger.debug(
        "_graphql_list_monitor_metrics: urn=%s, startTime=%s, endTime=%s",
        _shorten_urn(monitor_urn, 50),
        start_time_ms,
        end_time_ms,
    )

    try:
        session = _get_retry_session()
        response = session.post(
            graphql_url,
            json={"query": query, "variables": variables},
            headers=headers,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data and data["errors"]:
            error_msgs = [e.get("message", "") for e in data["errors"]]
            # Check if the API is not available (older DataHub versions)
            if any(
                "undefined" in msg.lower()
                or "unknown" in msg.lower()
                or "not found" in msg.lower()
                for msg in error_msgs
            ):
                logger.warning(
                    "listMonitorMetrics API not available: %s", error_msgs[0][:100]
                )
                return []
            # Check for server-side search/query errors (log at debug level)
            if any(
                "esqueryexception" in msg.lower()
                or "search query failed" in msg.lower()
                for msg in error_msgs
            ):
                logger.debug(
                    "listMonitorMetrics search error for %s: %s",
                    _shorten_urn(monitor_urn, 50),
                    error_msgs[0][:100],
                )
                return []
            logger.error("GraphQL error in listMonitorMetrics: %s", error_msgs)
            return []

        result = data.get("data", {}).get("listMonitorMetrics")
        if not result:
            return []

        metrics = result.get("metrics", [])

        # Augment each metric with the monitor URN
        events: list[dict[str, Any]] = []
        for metric in metrics:
            event: dict[str, Any] = {
                "monitorUrn": monitor_urn,
                "metricCubeUrn": build_metric_cube_urn(monitor_urn),
            }

            assertion_metric = metric.get("assertionMetric") or {}
            event["timestampMillis"] = assertion_metric.get("timestampMillis")
            event["measure"] = assertion_metric.get("value")

            # Include anomaly event data if present
            anomaly_event = metric.get("anomalyEvent")
            if anomaly_event:
                event["anomaly_timestampMillis"] = anomaly_event.get("timestampMillis")
                event["anomaly_state"] = anomaly_event.get("state")
                source = anomaly_event.get("source") or {}
                event["anomaly_source_type"] = source.get("type")
                event["anomaly_source_urn"] = source.get("sourceUrn")
                event["anomaly_source_eventTimestampMillis"] = source.get(
                    "sourceEventTimestampMillis"
                )

            events.append(event)

        return events

    except requests.exceptions.RequestException:
        logger.exception(
            "_graphql_list_monitor_metrics: Request failed for %s", monitor_urn
        )
        return []
    except Exception:
        logger.exception("_graphql_list_monitor_metrics: Exception for %s", monitor_urn)
        return []


def _convert_metric_cube_events(events: list[dict]) -> pd.DataFrame:
    """Convert metric cube events to DataFrame format.

    Args:
        events: List of metric cube events from listMonitorMetrics

    Returns:
        DataFrame with metric data, assertion URN, and optional anomaly information
    """
    if not events:
        return pd.DataFrame()

    rows = []
    for event in events:
        row = {
            "timestampMillis": event.get("timestampMillis"),
            "monitorUrn": event.get("monitorUrn"),
            "metricCubeUrn": event.get("metricCubeUrn"),
            "assertionUrn": event.get("assertionUrn"),  # From monitor enrichment
            "measure": event.get("measure"),
            "aspectName": "dataHubMetricCubeEvent",
        }

        # Add anomaly data if present
        if event.get("anomaly_state"):
            row["anomaly_timestampMillis"] = event.get("anomaly_timestampMillis")
            row["anomaly_state"] = event.get("anomaly_state")
            row["anomaly_source_type"] = event.get("anomaly_source_type")
            row["anomaly_source_urn"] = event.get("anomaly_source_urn")
            row["anomaly_source_eventTimestampMillis"] = event.get(
                "anomaly_source_eventTimestampMillis"
            )

        rows.append(row)

    df = pd.DataFrame(rows)

    # Ensure numeric types
    if "timestampMillis" in df.columns:
        df["timestampMillis"] = pd.to_numeric(df["timestampMillis"], errors="coerce")
    if "measure" in df.columns:
        df["measure"] = pd.to_numeric(df["measure"], errors="coerce")

    # Add datetime column
    if "timestampMillis" in df.columns:
        df["datetime"] = pd.to_datetime(
            df["timestampMillis"], unit="ms", errors="coerce"
        )

    return df


def _fetch_metric_cube_events_for_monitor(
    graphql_url: str,
    headers: dict,
    monitor_urn: str,
    start_time_ms: int,
    end_time_ms: int,
) -> list[dict]:
    """Fetch metric cube events for a monitor.

    This function attempts to fetch metric cube events via the listMonitorMetrics
    GraphQL query. If the API is not available (older DataHub versions), it returns
    an empty list and the caller should fall back to assertion run events.

    Args:
        graphql_url: The GraphQL endpoint URL
        headers: HTTP headers including auth token
        monitor_urn: The monitor URN
        start_time_ms: Start time in milliseconds
        end_time_ms: End time in milliseconds

    Returns:
        List of metric cube events, or empty list if not available
    """
    return _graphql_list_monitor_metrics(
        graphql_url=graphql_url,
        headers=headers,
        monitor_urn=monitor_urn,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
    )


# =============================================================================
# Monitor Creation Functions
# =============================================================================


def _graphql_create_assertion_monitor(
    graphql_url: str,
    headers: dict,
    entity_urn: str,
    assertion_urn: str,
    cron_schedule: str = "0 0 * * *",
    timezone: str = "UTC",
    mode: str = "ACTIVE",
) -> Optional[str]:
    """Create an assertion monitor via GraphQL mutation.

    This allows assertions without monitors to start collecting metric cube data.

    Args:
        graphql_url: The GraphQL endpoint URL
        headers: HTTP headers including auth token
        entity_urn: The dataset/entity URN being monitored
        assertion_urn: The assertion URN to monitor
        cron_schedule: Cron schedule for evaluation (default: daily at midnight)
        timezone: Timezone for the schedule (default: UTC)
        mode: Monitor mode - ACTIVE or PAUSED (default: ACTIVE)

    Returns:
        The created monitor URN, or None if creation failed
    """
    mutation = """
    mutation CreateAssertionMonitor($input: CreateAssertionMonitorInput!) {
        createAssertionMonitor(input: $input) {
            urn
        }
    }
    """

    variables = {
        "input": {
            "entityUrn": entity_urn,
            "assertionUrn": assertion_urn,
            "schedule": {
                "type": "CRON",
                "cron": {
                    "cron": cron_schedule,
                    "timezone": timezone,
                },
            },
            "mode": mode,
        }
    }

    logger.debug(
        "_graphql_create_assertion_monitor: entity=%s, assertion=%s, cron=%s",
        entity_urn[:50] if entity_urn else "None",
        assertion_urn[:50] if assertion_urn else "None",
        cron_schedule,
    )

    try:
        session = _get_retry_session()
        response = session.post(
            graphql_url,
            json={"query": mutation, "variables": variables},
            headers=headers,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data and data["errors"]:
            error_msgs = [e.get("message", "") for e in data["errors"]]
            logger.error("GraphQL error creating monitor: %s", error_msgs)
            return None

        result = data.get("data", {}).get("createAssertionMonitor")
        if result:
            monitor_urn = result.get("urn")
            logger.info("Created monitor: %s", monitor_urn)
            return monitor_urn

        return None

    except requests.exceptions.RequestException as e:
        logger.exception("Failed to create assertion monitor: %s", e)
        return None
    except Exception:
        logger.exception("Unexpected error creating assertion monitor")
        return None


def _graphql_get_assertion_info(
    graphql_url: str,
    headers: dict,
    assertion_urn: str,
) -> Optional[dict]:
    """Get assertion info including the assertee (entity) URN.

    This is needed to create a monitor since we need the entity URN.

    Args:
        graphql_url: The GraphQL endpoint URL
        headers: HTTP headers including auth token
        assertion_urn: The assertion URN

    Returns:
        Dictionary with assertion info including 'asserteeUrn', or None if not found
    """
    query = """
    query GetAssertionInfo($urn: String!) {
        assertion(urn: $urn) {
            urn
            info {
                type
                volumeAssertion {
                    entityUrn
                }
                freshnessAssertion {
                    entityUrn
                }
                fieldAssertion {
                    entityUrn
                }
                sqlAssertion {
                    entityUrn
                }
                datasetAssertion {
                    datasetUrn
                }
                schemaAssertion {
                    entityUrn
                }
            }
        }
    }
    """

    try:
        session = _get_retry_session()
        response = session.post(
            graphql_url,
            json={"query": query, "variables": {"urn": assertion_urn}},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data and data["errors"]:
            return None

        assertion_data = data.get("data", {}).get("assertion")
        if not assertion_data:
            return None

        info = assertion_data.get("info") or {}
        assertion_type = info.get("type")

        # Extract entity URN based on assertion type
        entity_urn = None
        if info.get("volumeAssertion"):
            entity_urn = info["volumeAssertion"].get("entityUrn")
        elif info.get("freshnessAssertion"):
            entity_urn = info["freshnessAssertion"].get("entityUrn")
        elif info.get("fieldAssertion"):
            entity_urn = info["fieldAssertion"].get("entityUrn")
        elif info.get("sqlAssertion"):
            entity_urn = info["sqlAssertion"].get("entityUrn")
        elif info.get("datasetAssertion"):
            entity_urn = info["datasetAssertion"].get("datasetUrn")
        elif info.get("schemaAssertion"):
            entity_urn = info["schemaAssertion"].get("entityUrn")

        return {
            "assertionUrn": assertion_urn,
            "assertionType": assertion_type,
            "entityUrn": entity_urn,
        }

    except Exception:
        logger.exception("Failed to get assertion info for %s", assertion_urn)
        return None


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "_render_cached_source",
    "_render_api_fetch_controls",
    "_execute_api_fetch",
    # HTTP utilities
    "_get_retry_session",
    # Monitor scroll and discovery
    "_graphql_scroll_monitors",
    "_extract_assertion_urn_from_monitor",
    # Entity aspect fetching
    "_fetch_entity_aspect_events",
    "_graphql_timeseries_aspect",
    "_graphql_monitor_anomaly_events",
    "_try_graphql_monitor_anomalies",
    "_rest_monitor_anomaly_events",
    # DataFrame conversion
    "_convert_aspect_events_to_dataframe",
    "_convert_monitor_anomaly_events",
    "_convert_monitor_state_events",
    "_convert_generic_events",
    # Metric cube functions
    "build_metric_cube_urn",
    "_graphql_list_monitor_metrics",
    "_convert_metric_cube_events",
    "_fetch_metric_cube_events_for_monitor",
    # Monitor creation functions
    "_graphql_create_assertion_monitor",
    "_graphql_get_assertion_info",
]
