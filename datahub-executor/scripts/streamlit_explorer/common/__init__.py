# ruff: noqa: INP001
"""
Common utilities for Streamlit applications.

This module provides shared components used across the timeseries_explorer
and model_explorer modules:
- Cache management
- Data loading
- Environment configuration
- Preprocessing utilities
- Run event extraction
- Fetch utilities
- Shared state and helper functions
"""

# Re-export from cache_manager
# Re-export from auto_inference_v2
from .auto_inference_v2 import (
    AutoPreprocessingPreset,
    AutoTrainingRunResult,
    build_input_data_context_from_session,
    get_all_pairing_identifiers,
    get_default_pairing_identifiers,
    parse_pairings_csv,
    run_auto_training_pipeline,
    suggest_preprocessing_preset,
)

# Re-export from auto_inference_v2_runs
from .auto_inference_v2_runs import (
    _AUTO_INFERENCE_V2_RUNS_KEY,
    _AUTO_V2_RUN_DETAILS_KEY_PREFIX,
    clear_all_auto_inference_v2_runs,
    delete_auto_inference_v2_run_complete,
    get_auto_inference_v2_runs,
    load_auto_inference_v2_runs,
)
from .cache_manager import (
    ALL_ASPECTS,
    METRIC_CUBE_ASPECTS,
    MONITOR_ASPECTS,
    AnomalyEdit,
    AnomalyEditTracker,
    AspectCacheInfo,
    CacheIndex,
    CacheIndexData,
    EndpointCache,
    EndpointInfo,
    EndpointRegistry,
    RunEventCache,
    SyncHistoryEntry,
    get_cache_dir,
    hostname_to_dir,
    url_to_hostname,
)

# Re-export from data_loaders
from .data_loaders import DataLoader, get_data_loader

# Re-export from env_config
from .env_config import (
    DataHubEnvConfig,
    get_env_config_summary,
    list_env_files,
    load_env_config,
    load_env_config_from_file,
)

# Re-export from fetch
from .fetch import (
    _convert_aspect_events_to_dataframe,
    _convert_generic_events,
    _convert_monitor_anomaly_events,
    _convert_monitor_state_events,
    _execute_api_fetch,
    _extract_assertion_urn_from_monitor,
    _fetch_entity_aspect_events,
    _get_retry_session,
    _graphql_monitor_anomaly_events,
    _graphql_scroll_monitors,
    _graphql_timeseries_aspect,
    _render_api_fetch_controls,
    _render_cached_source,
    _rest_monitor_anomaly_events,
    _try_graphql_monitor_anomalies,
)

# Re-export from preprocessing_ui
from .preprocessing_ui import (
    HAS_PREPROCESSING,
    HAS_REGISTRY,
    PreprocessingState,
    apply_preprocessing,
    get_available_pipelines,
    get_pipeline_options,
    init_preprocessing_state,
    instantiate_pipeline,
    mark_anomalies_in_type_column,
    render_anomaly_exclusion_config,
    render_before_after_chart,
    render_config_expander,
    render_differencing_config,
    render_filtering_config,
    render_inference_config_info,
    render_missing_data_config,
    render_pipeline_info,
    render_pipeline_selector,
    render_preprocessing_config_panel,
    render_preprocessing_context,
    render_preprocessing_stats,
    render_resampling_config,
    serialize_applied_config,
    serialize_preprocessing_state,
    state_to_config,
)

# Re-export from run_event_extractor (monitor-related only)
from .run_event_extractor import (
    TIMESTAMP_COLUMN,
    MonitorMetadata,
    extract_entity_from_monitor_urn,
    filter_events_by_time,
    get_time_range,
    list_monitors,
)

# Re-export from shared
from .shared import (
    _DATA_SOURCE,
    _FETCH_CANCELLED,
    _FETCH_IN_PROGRESS,
    _LOADED_TIMESERIES,
    _MONITOR_URN_FOR_ASSERTION,
    _RAW_EVENTS_DF,
    _SELECTED_ASSERTION,
    _SELECTED_ENDPOINT,
    _SELECTED_MONITOR,
    ACTIVE_ENV_CONFIG,
    ASSERTION_TYPE_FILTER_OPTIONS,
    FIELD_METRIC_FILTER_OPTIONS,
    FetchConfig,
    _cancel_event,
    _check_cancelled,
    _extract_hash_from_run_id,
    _get_datahub_url,
    _hex_to_rgba,
    _make_urn_link,
    _render_urn_with_link,
    _set_cancelled,
    _shorten_urn,
    connection_matches_endpoint,
    get_active_config,
    get_model_hyperparameters,
    init_explorer_state,
    logger,
    render_connection_status,
    set_explorer_context,
)

# Note: EXPLORER_PAGES is assembled at the top-level streamlit/__init__.py
# to avoid circular imports between common, timeseries_explorer, and model_explorer

__all__ = [
    # Cache manager
    "ALL_ASPECTS",
    "METRIC_CUBE_ASPECTS",
    "MONITOR_ASPECTS",
    "AnomalyEdit",
    "AnomalyEditTracker",
    "AspectCacheInfo",
    "CacheIndex",
    "CacheIndexData",
    "EndpointCache",
    "EndpointInfo",
    "EndpointRegistry",
    "RunEventCache",
    "SyncHistoryEntry",
    "get_cache_dir",
    "hostname_to_dir",
    "url_to_hostname",
    # Data loaders
    "DataLoader",
    "get_data_loader",
    # Env config
    "DataHubEnvConfig",
    "get_env_config_summary",
    "list_env_files",
    "load_env_config",
    "load_env_config_from_file",
    # Run event extractor (monitor-related only)
    "TIMESTAMP_COLUMN",
    "MonitorMetadata",
    "extract_entity_from_monitor_urn",
    "filter_events_by_time",
    "get_time_range",
    "list_monitors",
    # Preprocessing UI
    "HAS_PREPROCESSING",
    "HAS_REGISTRY",
    "PreprocessingState",
    "apply_preprocessing",
    "get_available_pipelines",
    "get_pipeline_options",
    "init_preprocessing_state",
    "instantiate_pipeline",
    "mark_anomalies_in_type_column",
    "render_anomaly_exclusion_config",
    "render_before_after_chart",
    "render_config_expander",
    "render_differencing_config",
    "render_filtering_config",
    "render_inference_config_info",
    "render_missing_data_config",
    "render_pipeline_info",
    "render_pipeline_selector",
    "render_preprocessing_config_panel",
    "render_preprocessing_context",
    "render_preprocessing_stats",
    "render_resampling_config",
    "serialize_applied_config",
    "serialize_preprocessing_state",
    "state_to_config",
    # Shared
    "ACTIVE_ENV_CONFIG",
    "FetchConfig",
    "_cancel_event",
    "_check_cancelled",
    "connection_matches_endpoint",
    "_DATA_SOURCE",
    "_extract_hash_from_run_id",
    "_FETCH_CANCELLED",
    "_FETCH_IN_PROGRESS",
    "_hex_to_rgba",
    "_LOADED_TIMESERIES",
    "_MONITOR_URN_FOR_ASSERTION",
    "_RAW_EVENTS_DF",
    "_SELECTED_ASSERTION",
    "_SELECTED_ENDPOINT",
    "_SELECTED_MONITOR",
    "_set_cancelled",
    "ASSERTION_TYPE_FILTER_OPTIONS",
    "FIELD_METRIC_FILTER_OPTIONS",
    "set_explorer_context",
    "_shorten_urn",
    "_render_urn_with_link",
    "_get_datahub_url",
    "_make_urn_link",
    "get_active_config",
    "get_model_hyperparameters",
    "init_explorer_state",
    "logger",
    "render_connection_status",
    # Fetch
    "_convert_aspect_events_to_dataframe",
    "_convert_generic_events",
    "_convert_monitor_anomaly_events",
    "_convert_monitor_state_events",
    "_execute_api_fetch",
    "_extract_assertion_urn_from_monitor",
    "_fetch_entity_aspect_events",
    "_get_retry_session",
    "_graphql_monitor_anomaly_events",
    "_graphql_scroll_monitors",
    "_graphql_timeseries_aspect",
    "_render_api_fetch_controls",
    "_render_cached_source",
    "_rest_monitor_anomaly_events",
    "_try_graphql_monitor_anomalies",
    # inference_v2 auto helpers
    "AutoPreprocessingPreset",
    "AutoTrainingRunResult",
    "build_input_data_context_from_session",
    "get_all_pairing_identifiers",
    "get_default_pairing_identifiers",
    "parse_pairings_csv",
    "run_auto_training_pipeline",
    "suggest_preprocessing_preset",
    # auto inference_v2 runs management
    "_AUTO_INFERENCE_V2_RUNS_KEY",
    "_AUTO_V2_RUN_DETAILS_KEY_PREFIX",
    "clear_all_auto_inference_v2_runs",
    "delete_auto_inference_v2_run_complete",
    "get_auto_inference_v2_runs",
    "load_auto_inference_v2_runs",
]
