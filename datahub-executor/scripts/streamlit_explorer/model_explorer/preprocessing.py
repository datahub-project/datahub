# ruff: noqa: INP001
"""Preprocessing page for Time Series Explorer."""

import re

import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
import streamlit as st

from ..common import (
    _LOADED_TIMESERIES,
    DataLoader,
    apply_preprocessing,
    init_explorer_state,
    render_before_after_chart,
    render_preprocessing_config_panel,
    render_preprocessing_stats,
)
from ..common.preprocessing_ui import (
    build_config_for_display,
    mark_anomalies_in_type_column,
    render_config_expander,
    serialize_preprocessing_state,
)

# Serializers for deserializing stored configs
try:
    from datahub_executor.common.monitor.inference.inference_utils import (
        PreprocessingConfigSerializer,
    )

    HAS_PREPROCESSING_SERIALIZER = True
except ImportError:
    HAS_PREPROCESSING_SERIALIZER = False


def _count_excluded_points(ts_df: pd.DataFrame, exclusion) -> int:
    """Count how many data points fall within an exclusion range."""
    if ts_df is None or "ds" not in ts_df.columns or len(ts_df) == 0:
        return 0

    count = 0
    for ts in ts_df["ds"]:
        # Handle both datetime and Timestamp types
        if isinstance(ts, pd.Timestamp):
            ts_dt = ts.to_pydatetime()
        else:
            ts_dt = ts
        if exclusion.start <= ts_dt <= exclusion.end:
            count += 1
    return count


def _render_saved_preprocessings(
    loader: DataLoader,
    hostname: str,
    original_df: pd.DataFrame | None = None,
    filter_by_assertion: bool = True,
) -> None:
    """Render the list of saved preprocessings as a table with load/delete options.

    Shows all saved preprocessings with non-matching ones greyed out but still
    deletable. A filter toggle allows users to show/hide non-matching items.

    Args:
        loader: DataLoader instance
        hostname: The endpoint hostname
        original_df: Optional original DataFrame for comparison visualization
        filter_by_assertion: Default filter state (can be toggled by user)
    """
    endpoint_cache = loader.cache.get_endpoint_cache(hostname)

    # Get current assertion URN for filtering
    current_assertion_urn = st.session_state.get("selected_assertion_urn")

    # Always get all saved preprocessings
    all_saved = endpoint_cache.list_saved_preprocessings()
    if not all_saved:
        return

    from datetime import datetime

    from ..common.shared import _shorten_urn

    # Count matching vs non-matching
    matching_count = sum(
        1
        for item in all_saved
        if item.get("source_assertion_urn") == current_assertion_urn
    )
    non_matching_count = len(all_saved) - matching_count

    # Header with filter toggle
    header_col, filter_col = st.columns([3, 1])
    with header_col:
        if current_assertion_urn and non_matching_count > 0:
            st.subheader(
                f"Saved Preprocessings ({matching_count} matching, {len(all_saved)} total)"
            )
        else:
            st.subheader(f"Saved Preprocessings ({len(all_saved)})")

    # Filter toggle - initialize from session state or default
    filter_key = "preprocessing_filter_by_assertion"
    if filter_key not in st.session_state:
        st.session_state[filter_key] = filter_by_assertion

    with filter_col:
        show_all = st.checkbox(
            "Show all",
            value=not st.session_state[filter_key],
            key="preprocessing_show_all_toggle",
            help="Show preprocessings from all assertions (non-matching shown greyed out)",
        )
        st.session_state[filter_key] = not show_all

    # Determine which items to show
    if st.session_state[filter_key] and current_assertion_urn:
        # Filtered: only show matching, but still allow seeing others via toggle
        display_list = [
            item
            for item in all_saved
            if item.get("source_assertion_urn") == current_assertion_urn
        ]
        if not display_list and non_matching_count > 0:
            st.info(
                f"No saved preprocessings for this assertion. "
                f"Enable 'Show all' to see {non_matching_count} other(s)."
            )
            return
    else:
        # Show all
        display_list = all_saved

    if not display_list:
        return

    # Build table data with match indicator
    table_data = []
    for item in display_list:
        preprocessing_id = item["preprocessing_id"]
        row_count = item.get("row_count", "?")
        created_at = item.get("created_at", "")
        source_urn = item.get("source_assertion_urn", "")
        is_matching = source_urn == current_assertion_urn

        # Format created_at for display
        if created_at:
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                created_str = dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                created_str = created_at[:16] if len(created_at) > 16 else created_at
        else:
            created_str = ""

        # Shorten the assertion URN for display
        short_urn = _shorten_urn(source_urn, max_length=40) if source_urn else "-"

        # Add match indicator
        if is_matching:
            match_indicator = "✓"
        else:
            match_indicator = ""

        table_data.append(
            {
                "Match": match_indicator,
                "ID": preprocessing_id,
                "Assertion": short_urn,
                "Rows": row_count,
                "Created": created_str,
                "_is_matching": is_matching,  # Hidden column for logic
                "_source_urn": source_urn,  # Hidden column for full URN
            }
        )

    # Create DataFrame for display
    full_df = pd.DataFrame(table_data)
    display_columns = ["Match", "ID", "Assertion", "Rows", "Created"]

    # Extract matching flags before filtering columns
    matching_flags = full_df["_is_matching"].tolist()

    # Create display DataFrame with only visible columns
    display_df = full_df[display_columns].copy()

    # Apply styling to grey out non-matching rows
    def style_row(row_idx):
        if not matching_flags[row_idx]:
            return ["color: #888888; font-style: italic;"] * len(display_columns)
        return [""] * len(display_columns)

    styled_df = display_df.style.apply(lambda x: style_row(x.name), axis=1)

    # Display as styled table
    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
    )

    # Actions: select + load/delete buttons
    col_select, col_load, col_del, col_spacer = st.columns([2, 1, 1, 2])
    preprocessing_ids = [item["preprocessing_id"] for item in display_list]

    # Create a mapping of ID to matching status
    id_to_matching = {
        item["preprocessing_id"]: item.get("source_assertion_urn")
        == current_assertion_urn
        for item in display_list
    }

    with col_select:
        # Format options to show match status
        def format_option(pid):
            if id_to_matching.get(pid, False):
                return pid
            return f"{pid} (other assertion)"

        selected_id = st.selectbox(
            "Select preprocessing",
            options=preprocessing_ids,
            format_func=format_option,
            key="saved_preprocessing_select",
            label_visibility="collapsed",
        )

    # Check if selected preprocessing matches current assertion
    selected_matches = id_to_matching.get(selected_id, False) if selected_id else False

    with col_load:
        # Disable load button for non-matching preprocessings
        load_disabled = not selected_matches
        load_help = (
            "Load this preprocessing for the current assertion"
            if selected_matches
            else "Cannot load: this preprocessing is from a different assertion"
        )
        if st.button(
            "📂 Load",
            key="load_preprocessing_btn",
            type="primary",
            disabled=load_disabled,
            help=load_help,
        ):
            if selected_id and selected_matches:
                loaded_df = endpoint_cache.load_preprocessing(selected_id)
                if loaded_df is not None:
                    # Update the main preprocessed_df so the visualization above updates
                    st.session_state["preprocessed_df"] = loaded_df
                    # Load the metadata with config
                    metadata = endpoint_cache.get_preprocessing_metadata(selected_id)
                    if metadata:
                        saved_config = metadata.get("preprocessing_config")
                        if saved_config:
                            st.session_state["applied_preprocessing_config"] = (
                                saved_config
                            )
                    # Track which saved preprocessing is loaded
                    st.session_state["loaded_saved_preprocessing_id"] = selected_id
                    st.success(f"Loaded preprocessing '{selected_id}'")
                    st.rerun()
                else:
                    st.error(f"Failed to load '{selected_id}'")

    with col_del:
        # Delete is always enabled
        if st.button("🗑️ Delete", key="delete_preprocessing_btn"):
            if selected_id:
                endpoint_cache.delete_preprocessing(selected_id)
                # Clear preprocessed data if it was from the deleted preprocessing
                if st.session_state.get("loaded_saved_preprocessing_id") == selected_id:
                    st.session_state.pop("preprocessed_df", None)
                    st.session_state.pop("applied_preprocessing_config", None)
                    st.session_state.pop("loaded_saved_preprocessing_id", None)
                st.rerun()

    # Show which saved preprocessing is currently loaded
    loaded_id = st.session_state.get("loaded_saved_preprocessing_id")
    if loaded_id:
        st.caption(f"Currently loaded: **{loaded_id}**")


def _try_auto_load_inference_config(
    loader: DataLoader,
    hostname: str,
    assertion_urn: str | None,
) -> None:
    """Auto-load inference config for the current assertion if available.

    Checks if inference data with a preprocessing config exists for the
    currently selected assertion. If so, loads it and makes "From Inference"
    available in the Preprocessing Mode dropdown.

    Args:
        loader: DataLoader instance
        hostname: The endpoint hostname
        assertion_urn: The currently selected assertion URN
    """
    if not HAS_PREPROCESSING_SERIALIZER:
        return

    if not assertion_urn:
        return

    # Check if already loaded for this assertion
    currently_loaded_urn = st.session_state.get("_loaded_inference_source_urn")
    if currently_loaded_urn == assertion_urn:
        return  # Already loaded

    endpoint_cache = loader.cache.get_endpoint_cache(hostname)

    # Try to load inference data for the current assertion
    inference_data = endpoint_cache.load_inference_data(assertion_urn)
    if inference_data:
        preprocessing_json = inference_data.get("preprocessing_config_json")
        if preprocessing_json:
            try:
                # Deserialize the preprocessing config
                config = PreprocessingConfigSerializer.deserialize(preprocessing_json)

                # Store in session state for use by preprocessing UI
                st.session_state["_loaded_inference_preprocessing_config"] = config
                st.session_state["_loaded_inference_source_urn"] = assertion_urn

                # Note: Don't auto-select "from_inference" mode - let user choose
                # The option will now appear in the dropdown

            except Exception:
                pass  # Silently ignore deserialization errors


def render_preprocessing_page():
    """Render the preprocessing configuration and visualization page."""
    st.header("Preprocessing Pipeline")

    init_explorer_state()
    loader = DataLoader()

    # Navigation buttons
    col_nav_back, col_nav_spacer, col_nav_forward = st.columns([1, 3, 1])
    with col_nav_back:
        if st.button("← Metric Cube Events", key="go_to_metric_cube_events"):
            from ..timeseries_explorer import metric_cube_browser_page

            st.switch_page(metric_cube_browser_page)
    with col_nav_forward:
        if st.button("Model Training →", key="go_to_model_training"):
            from . import model_training_page

            st.switch_page(model_training_page)

    ts_df_all = st.session_state.get(_LOADED_TIMESERIES)

    if ts_df_all is None or len(ts_df_all) == 0:
        st.warning("No time series loaded. Go to Assertion Browser to load data.")
        return

    hostname = st.session_state.get("current_hostname")
    assertion_urn = st.session_state.get("selected_assertion_urn")

    # Auto-load inference config if available for current assertion
    # This makes "From Inference" appear in the Preprocessing Mode dropdown
    if hostname and assertion_urn:
        _try_auto_load_inference_config(loader, hostname, assertion_urn)

    # Use the already-extracted time series from metric cube data
    # This is set by assertion_browser using metric_cube_extractor.extract_timeseries()
    ts_df = ts_df_all

    # Count anomaly states from metric cube data (replaces legacy result type counts)
    anomaly_state_counts: dict[str, int] = {}
    if ts_df is not None and "anomaly_state" in ts_df.columns:
        state_series = ts_df["anomaly_state"].dropna()
        if len(state_series) > 0:
            anomaly_state_counts = state_series.value_counts().to_dict()

    # Load confirmed anomaly timestamps for type-based filtering
    anomaly_timestamps: list[int] = []
    anomaly_count = 0
    if hostname:
        try:
            # Load confirmed anomaly timestamps (all - not filtered by entity)
            anomaly_timestamps = loader.get_confirmed_anomaly_timestamps(
                hostname,
                entity_urn=None,
                use_local_edits=True,
            )
            anomaly_count = len(anomaly_timestamps)
        except Exception:
            pass  # Silently ignore errors loading anomaly timestamps

    col1, col2 = st.columns([1, 2])

    with col1:
        # Render config panel - init_count is 0 for metric cube data (no INIT events)
        state = render_preprocessing_config_panel(
            anomaly_count=anomaly_count, init_count=0
        )

    if ts_df is None or len(ts_df) == 0:
        st.warning(
            "No data found for preprocessing. Check that metric cube data is loaded."
        )
        return

    # Build filtering info message for metric cube data
    filtering_info_msg = None
    if anomaly_state_counts:
        # Show anomaly state breakdown
        state_parts = []
        for state_name, count in sorted(anomaly_state_counts.items()):
            state_parts.append(f"{state_name}: {count}")
        filtering_info_msg = (
            f"**Anomaly States:** {', '.join(state_parts)} | "
            f"**Total:** {len(ts_df)} events for preprocessing"
        )
    else:
        filtering_info_msg = f"**Total:** {len(ts_df)} events for preprocessing"

    with col1:
        # Check if current config matches already-applied preprocessing
        # Use UI state for comparison (actual config includes time-sensitive data)
        preprocessed_df = st.session_state.get("preprocessed_df")
        applied_ui_state = st.session_state.get("applied_preprocessing_state")
        current_ui_state = serialize_preprocessing_state(state)
        config_unchanged = (
            preprocessed_df is not None
            and applied_ui_state is not None
            and applied_ui_state == current_ui_state
        )

        if config_unchanged:
            st.success("✓ Preprocessing already applied with current configuration")

        if st.button("Apply Preprocessing", type="primary"):
            with st.spinner("Applying preprocessing..."):
                # Use the metric cube time series directly
                ts_df_for_preprocessing = ts_df.copy()

                if ts_df_for_preprocessing is None or len(ts_df_for_preprocessing) == 0:
                    st.error("No data available for preprocessing.")
                    st.stop()

                manual_exclusions = []

                # Collect manual exclusion info for display
                if state.filtering_enabled and state.exclusion_ranges:
                    from datetime import datetime

                    for r in state.exclusion_ranges:
                        if r.get("start") and r.get("end"):
                            manual_exclusions.append(
                                {
                                    "type": "Manual",
                                    "start": datetime.combine(
                                        r["start"], datetime.min.time()
                                    ),
                                    "end": datetime.combine(
                                        r["end"], datetime.max.time()
                                    ),
                                }
                            )

                # Handle anomaly exclusions for metric cube data
                # Metric cube data has 'anomaly_state' column with CONFIRMED/REJECTED states
                anomaly_exclusion_info = []
                if state.use_anomalies_as_exclusions:
                    # For metric cube data, use the existing anomaly_state column
                    if "anomaly_state" in ts_df_for_preprocessing.columns:
                        # Count confirmed anomalies that will be excluded
                        confirmed_mask = (
                            ts_df_for_preprocessing["anomaly_state"] == "CONFIRMED"
                        )
                        anomaly_marked_count = confirmed_mask.sum()

                        if anomaly_marked_count > 0:
                            # Create a 'type' column for preprocessing compatibility
                            # Mark CONFIRMED anomalies as "ANOMALY" type
                            ts_df_for_preprocessing["type"] = "NORMAL"
                            ts_df_for_preprocessing.loc[confirmed_mask, "type"] = (
                                "ANOMALY"
                            )

                            anomaly_exclusion_info.append(
                                {
                                    "type": "Confirmed Anomalies",
                                    "count": anomaly_marked_count,
                                }
                            )
                    elif anomaly_timestamps:
                        # Fallback: use timestamp-based anomaly matching
                        ts_df_for_preprocessing = mark_anomalies_in_type_column(
                            ts_df_for_preprocessing,
                            anomaly_timestamps,
                            type_col="type",
                            datetime_col="ds",
                        )
                        anomaly_marked_count = (
                            (ts_df_for_preprocessing["type"] == "ANOMALY").sum()
                            if "type" in ts_df_for_preprocessing.columns
                            else 0
                        )
                        if anomaly_marked_count > 0:
                            anomaly_exclusion_info.append(
                                {
                                    "type": "Anomaly (timestamp-based)",
                                    "count": anomaly_marked_count,
                                }
                            )

                # Store exclusion info for display
                st.session_state["applied_exclusions"] = (
                    manual_exclusions + anomaly_exclusion_info
                )

                # Apply preprocessing - anomalies are now marked in the type column
                # and will be filtered by AnomalyDataFilterTransformer
                result_df = apply_preprocessing(
                    ts_df_for_preprocessing,
                    state=state,
                )

                if result_df is not None:
                    st.session_state["preprocessed_df"] = result_df
                    # Store serialized config for display - use actual pipeline config
                    config_overrides = state.pipeline_configs.get(
                        state.pipeline_mode, {}
                    )
                    st.session_state["applied_preprocessing_config"] = (
                        build_config_for_display(
                            state,
                            config_overrides=config_overrides,
                        )
                    )
                    # Also store UI state for change detection
                    st.session_state["applied_preprocessing_state"] = (
                        serialize_preprocessing_state(state)
                    )
                    st.success(
                        f"Preprocessed: {len(ts_df_for_preprocessing)} → {len(result_df)} points"
                    )

        # Save preprocessing section - show if preprocessed data exists
        # (can be from current Apply or from unchanged config)
        preprocessed_df_for_save = st.session_state.get("preprocessed_df")
        if preprocessed_df_for_save is not None and len(preprocessed_df_for_save) > 0:
            st.markdown("---")
            st.markdown("**Save Preprocessing**")

            # Auto-populate ID from pipeline mode when switching to a predefined pipeline
            prev_pipeline_mode = st.session_state.get("_prev_pipeline_mode", "custom")
            if state.pipeline_mode != prev_pipeline_mode:
                # Pipeline mode changed - auto-populate ID if switching to predefined
                if state.pipeline_mode != "custom":
                    st.session_state["preprocessing_id_input"] = state.pipeline_mode
                st.session_state["_prev_pipeline_mode"] = state.pipeline_mode

            preprocessing_id = st.text_input(
                "Preprocessing ID",
                key="preprocessing_id_input",
                help="Unique identifier for this preprocessing configuration",
                placeholder="e.g., daily_no_anomalies"
                if state.pipeline_mode == "custom"
                else "",
            )

            # Validate ID format (alphanumeric, underscores, hyphens)
            id_valid = bool(
                preprocessing_id and re.match(r"^[a-zA-Z0-9_-]+$", preprocessing_id)
            )

            if preprocessing_id and not id_valid:
                st.warning(
                    "ID must contain only letters, numbers, underscores, and hyphens"
                )

            col_save1, col_save2 = st.columns(2)
            with col_save1:
                save_clicked = st.button(
                    "Save Preprocessing",
                    disabled=not id_valid,
                    key="save_preprocessing_btn",
                )

            if save_clicked and hostname and id_valid:
                endpoint_cache = loader.cache.get_endpoint_cache(hostname)

                # Check if ID already exists
                if endpoint_cache.preprocessing_exists(preprocessing_id):
                    st.error(
                        f"Preprocessing ID '{preprocessing_id}' already exists. Choose a different ID."
                    )
                else:
                    # Build metadata including the preprocessing config
                    assertion_urn = st.session_state.get("selected_assertion_urn")
                    applied_config = st.session_state.get(
                        "applied_preprocessing_config", {}
                    )
                    metadata = {
                        "source_assertion_urn": assertion_urn,
                        "source_hostname": hostname,
                        "original_row_count": len(ts_df) if ts_df is not None else 0,
                        "preprocessing_config": applied_config,
                    }

                    endpoint_cache.save_preprocessing(
                        preprocessing_id, preprocessed_df_for_save, metadata
                    )
                    st.success(f"Saved preprocessing '{preprocessing_id}'")
                    st.rerun()

            # Navigation to Model Training page
            st.markdown("---")
            # Check if config changed since last apply (use UI state for comparison)
            applied_ui_cfg = st.session_state.get("applied_preprocessing_state")
            current_ui_cfg = serialize_preprocessing_state(state)
            config_is_stale = (
                applied_ui_cfg is not None and applied_ui_cfg != current_ui_cfg
            )

            if config_is_stale:
                st.warning(
                    "⚠️ Configuration changed since last apply. "
                    "Click 'Apply Preprocessing' to use the new settings, "
                    "or proceed with the previously applied configuration."
                )

            if st.button("Train Models →", type="primary", key="go_to_training"):
                from . import model_training_page

                st.switch_page(model_training_page)

    with col2:
        preprocessed_df = st.session_state.get("preprocessed_df")

        if preprocessed_df is not None:
            render_before_after_chart(ts_df, preprocessed_df)
            render_preprocessing_stats(ts_df, preprocessed_df)

            # Show filtering info below the before/after chart
            if filtering_info_msg:
                st.info(filtering_info_msg)

            # Show the applied preprocessing configuration
            applied_config = st.session_state.get("applied_preprocessing_config")
            if applied_config:
                render_config_expander(
                    applied_config, title="Applied Preprocessing Configuration"
                )

            # Show applied exclusions info
            applied_exclusions = st.session_state.get("applied_exclusions", [])
            if applied_exclusions:
                st.markdown("---")
                st.markdown("**Applied Exclusions**")

                # Separate manual exclusions (time-range based) from type-based anomalies
                manual_exclusions = [e for e in applied_exclusions if "start" in e]
                type_based_exclusions = [e for e in applied_exclusions if "count" in e]

                # Show type-based anomaly exclusions (simple count display)
                if type_based_exclusions:
                    for excl_info in type_based_exclusions:
                        st.info(
                            f"🔍 **{excl_info['type']}**: {excl_info['count']} rows marked "
                            f"as ANOMALY will be filtered during preprocessing"
                        )

                # Show manual exclusion ranges table (if any)
                if manual_exclusions:
                    st.markdown("**Manual Exclusion Ranges**")

                    # Build table data with match counts
                    table_data = []
                    for excl_info in manual_exclusions:
                        # Create a mock exclusion object for counting
                        class MockRange:
                            def __init__(self, start, end):
                                self.start = start
                                self.end = end

                        mock_excl = MockRange(excl_info["start"], excl_info["end"])
                        points_matched = _count_excluded_points(ts_df, mock_excl)

                        start_dt = excl_info["start"]
                        end_dt = excl_info["end"]
                        table_data.append(
                            {
                                "Type": excl_info["type"],
                                "Start": start_dt.strftime(  # type: ignore[attr-defined]
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "End": end_dt.strftime("%Y-%m-%d %H:%M:%S"),  # type: ignore[attr-defined]
                                "Points Matched": points_matched,
                            }
                        )

                    excl_df = pd.DataFrame(table_data)
                    st.dataframe(
                        excl_df,
                        hide_index=True,
                        use_container_width=True,
                        column_config={
                            "Type": st.column_config.TextColumn("Type", width="small"),
                            "Start": st.column_config.TextColumn(
                                "Start", width="medium"
                            ),
                            "End": st.column_config.TextColumn("End", width="medium"),
                            "Points Matched": st.column_config.NumberColumn(
                                "Points Matched",
                                width="small",
                                help="Number of data points within this exclusion range",
                            ),
                        },
                    )

                # Summary
                total_matched = (
                    sum(row["Points Matched"] for row in table_data)
                    if manual_exclusions
                    else 0
                )
                anomaly_matched = sum(e.get("count", 0) for e in type_based_exclusions)
                manual_matched = sum(
                    row["Points Matched"]
                    for row in (table_data if manual_exclusions else [])
                    if row["Type"] == "Manual"
                )

                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    total_exclusions = len(manual_exclusions) + len(
                        type_based_exclusions
                    )
                    st.metric("Total Exclusions", total_exclusions)
                with col_b:
                    total_points = total_matched + anomaly_matched
                    st.metric(
                        "Total Points Excluded",
                        total_points,
                        help="Total data points that will be excluded",
                    )
                with col_c:
                    has_anomaly_exclusions = len(type_based_exclusions) > 0
                    if anomaly_matched == 0 and has_anomaly_exclusions:
                        st.metric(
                            "Anomaly Points",
                            f"⚠️ {anomaly_matched}",
                            help="No anomaly exclusions matched any data points!",
                        )
                    else:
                        st.metric(
                            "Anomaly / Manual",
                            f"{anomaly_matched} / {manual_matched}",
                        )
        else:
            st.subheader("Original Data Preview")

            fig = go.Figure()
            scatter_type = go.Scattergl if len(ts_df) > 5000 else go.Scatter
            fig.add_trace(
                scatter_type(
                    x=ts_df["ds"],
                    y=ts_df["y"],
                    mode="lines",
                    name="Original",
                    line=dict(color="#1f77b4"),
                )
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

    # Show saved preprocessings (full width, outside columns)
    if hostname:
        st.markdown("---")
        _render_saved_preprocessings(loader, hostname, ts_df)


__all__ = ["render_preprocessing_page"]
