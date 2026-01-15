# ruff: noqa: INP001
"""
Preprocessing UI components for Streamlit.

This module provides UI widgets for configuring and visualizing
time series preprocessing pipelines, including support for predefined
pipelines from the observe-models registry.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import plotly.graph_objects as go  # type: ignore[import-untyped]
import streamlit as st
from plotly.subplots import make_subplots  # type: ignore[import-untyped]

# Import preprocessing from observe-models package
try:
    from datahub_observe.algorithms.preprocessing import (  # type: ignore[import-untyped]
        AnomalyDataFilterConfig,
        DataFilterConfig,
        DifferenceConfig,
        FrequencyAlignmentConfig,
        InitDataFilterConfig,
        MissingDataConfig,
        PreprocessingConfig,
        ResamplingConfig,
        TimeRange,
        TimeSeriesPreprocessor,
        default_darts_transformers,
        default_pandas_transformers,
    )

    HAS_PREPROCESSING = True
except ImportError:
    HAS_PREPROCESSING = False
    PreprocessingConfig = None  # type: ignore[assignment,misc]
    TimeSeriesPreprocessor = None  # type: ignore[assignment,misc]
    InitDataFilterConfig = None  # type: ignore[assignment,misc]
    FrequencyAlignmentConfig = None  # type: ignore[assignment,misc]
    AnomalyDataFilterConfig = None  # type: ignore[assignment,misc]
    default_pandas_transformers = None  # type: ignore[assignment]
    default_darts_transformers = None  # type: ignore[assignment]

# Import registry for predefined pipelines
try:
    from datahub_observe.registry import (  # type: ignore[import-untyped]
        EntryNotFoundError,
        PreprocessorType,
        RegistryEntry,
        get_preprocessing_registry,
    )

    HAS_REGISTRY = True
except ImportError:
    HAS_REGISTRY = False
    get_preprocessing_registry = None  # type: ignore[assignment,misc]
    PreprocessorType = None  # type: ignore[assignment,misc]
    RegistryEntry = None  # type: ignore[assignment,misc]
    EntryNotFoundError = Exception  # type: ignore[assignment,misc]


# =============================================================================
# Anomaly Type Marking Functions
# =============================================================================


def mark_anomalies_in_type_column(
    df: pd.DataFrame,
    anomaly_timestamps_ms: list[int],
    type_col: str = "type",
    datetime_col: str = "ds",
) -> pd.DataFrame:
    """Mark rows as ANOMALY type based on confirmed anomaly timestamps.

    This function updates the type column in the DataFrame to mark rows
    that correspond to confirmed anomaly timestamps. This allows the
    AnomalyDataFilterTransformer to filter them out during preprocessing.

    Args:
        df: DataFrame with time series data
        anomaly_timestamps_ms: List of anomaly timestamps in milliseconds
        type_col: Name of the type column to update
        datetime_col: Name of the datetime column

    Returns:
        DataFrame with anomaly rows marked in the type column
    """
    if not anomaly_timestamps_ms or type_col not in df.columns:
        return df

    if len(df) == 0:
        return df

    result = df.copy()

    # Convert anomaly timestamps to datetime for comparison
    anomaly_datetimes = pd.to_datetime(anomaly_timestamps_ms, unit="ms")

    # Mark matching rows as ANOMALY
    # Need to normalize both to compare (remove timezone info if present)
    df_times = pd.to_datetime(result[datetime_col]).dt.tz_localize(None)
    anomaly_times_normalized = anomaly_datetimes.tz_localize(None)

    mask = df_times.isin(anomaly_times_normalized)
    result.loc[mask, type_col] = "ANOMALY"

    marked_count = mask.sum()
    if marked_count > 0:
        import logging

        logger = logging.getLogger(__name__)
        logger.info(f"Marked {marked_count} rows as ANOMALY type")

    return result


# =============================================================================
# Registry Integration Functions
# =============================================================================


def get_available_pandas_transformers() -> list:
    """Dynamically fetch all available pandas transformers from the registry.

    Returns:
        List of RegistryEntry objects for all registered PANDAS_TRANSFORMER types.
        Returns empty list if registry is not available.
    """
    if not HAS_REGISTRY or get_preprocessing_registry is None:
        return []

    try:
        registry = get_preprocessing_registry()
        return registry.list(preprocessor_type=PreprocessorType.PANDAS_TRANSFORMER)
    except Exception:
        return []


def get_available_darts_transformers() -> list:
    """Dynamically fetch all available Darts transformers from the registry.

    Returns:
        List of RegistryEntry objects for all registered TRANSFORMER types.
        Returns empty list if registry is not available.
    """
    if not HAS_REGISTRY or get_preprocessing_registry is None:
        return []

    try:
        registry = get_preprocessing_registry()
        return registry.list(preprocessor_type=PreprocessorType.TRANSFORMER)
    except Exception:
        return []


def get_transformer_config_class(name: str) -> Optional[type]:
    """Get the configuration class for a transformer by name.

    Args:
        name: The transformer name (e.g., "init_data_filter", "resampling")

    Returns:
        The configuration class, or None if not found.
    """
    if not HAS_REGISTRY or get_preprocessing_registry is None:
        return None

    try:
        registry = get_preprocessing_registry()
        return registry.get_config_class(name)
    except Exception:
        return None


def get_available_pipelines() -> list:
    """Dynamically fetch all available pipelines from the registry.

    Returns:
        List of RegistryEntry objects for all registered pipelines.
        Returns empty list if registry is not available.
    """
    if not HAS_REGISTRY or get_preprocessing_registry is None:
        return []

    try:
        registry = get_preprocessing_registry()
        return registry.list(preprocessor_type=PreprocessorType.PIPELINE)
    except Exception:
        return []


def get_pipeline_options() -> list[tuple[str, str]]:
    """Get pipeline options for selectbox.

    Returns:
        List of (value, display_label) tuples. Always includes "custom" as first option.
        If an inference config is loaded, includes "from_inference" option.
    """
    options = [("custom", "Custom Configuration")]

    # Check if inference config is loaded in session state
    loaded_config = st.session_state.get("_loaded_inference_preprocessing_config")
    if loaded_config is not None:
        # Determine the type of loaded config for display
        source_urn = st.session_state.get("_loaded_inference_source_urn", "")
        short_urn = source_urn[-30:] if len(source_urn) > 30 else source_urn

        # Get config type for label
        config_type = "custom"
        if hasattr(loaded_config, "type"):
            config_type = loaded_config.type
        elif isinstance(loaded_config, dict) and "type" in loaded_config:
            config_type = loaded_config["type"]

        label = f"From Inference ({config_type}) - {short_urn}"
        options.append(("from_inference", label))

    pipelines = get_available_pipelines()
    for p in pipelines:
        # Truncate description if too long
        desc = p.description[:60] + "..." if len(p.description) > 60 else p.description
        label = f"{p.name.title()} - {desc}"
        options.append((p.name, label))

    return options


def instantiate_pipeline(
    name: str,
    config_overrides: Optional[dict] = None,
) -> Any:
    """Instantiate a pipeline by name from the registry.

    Args:
        name: The pipeline name (e.g., "volume", "field")
        config_overrides: Optional dict of config parameters to override defaults

    Returns:
        An instance of the pipeline class

    Raises:
        EntryNotFoundError: If the pipeline name is not found in registry
        ImportError: If the registry is not available
    """
    if not HAS_REGISTRY or get_preprocessing_registry is None:
        raise ImportError("Registry not available. Install observe-models package.")

    registry = get_preprocessing_registry()
    entry = registry.get(name)
    pipeline_cls = entry.cls

    # If config overrides provided and pipeline has a config class, use from_config
    if config_overrides:
        config_cls = get_pipeline_config_class(name)
        if config_cls is not None:
            # Build config from overrides, filtering out None values and special fields
            config_kwargs = {
                k: v
                for k, v in config_overrides.items()
                if k not in ("type", "preprocessing_config") and v is not None
            }
            config = config_cls(**config_kwargs)

            # Use from_config if available
            if hasattr(pipeline_cls, "from_config"):
                return pipeline_cls.from_config(config)
            else:
                # Fall back to passing config as first arg
                return pipeline_cls(config)

    # No overrides - use defaults
    return pipeline_cls()


@dataclass
class PreprocessingState:
    """State for preprocessing configuration UI."""

    # Pipeline mode: "custom" or any pipeline name from registry (e.g., "volume", "field")
    pipeline_mode: str = "custom"

    # Type-aware processing (new architecture)
    # When enabled, the 'type' column (INIT, SUCCESS, etc.) is passed through
    # and used for type-aware chunk processing by transformers
    type_aware_enabled: bool = True  # Enable type-aware preprocessing
    type_col: str = "type"  # Name of the type column in DataFrame

    # Init data filter settings (replaces exclude_init_results)
    # Uses InitDataFilterTransformer for INIT chunk handling
    init_filter_enabled: bool = False  # Enable INIT filtering via transformer
    init_trim_count: int = 1  # Number of INIT values to trim from the beginning

    # Result type filtering (shared by custom and predefined modes)
    # DEPRECATED: Use init_filter_enabled instead. Kept for backward compatibility.
    exclude_init_results: bool = False  # Legacy: If True, exclude INIT result types

    # Data filtering (used only in custom mode)
    filtering_enabled: bool = False
    exclusion_ranges: list = field(default_factory=list)
    inclusion_ranges: list = field(default_factory=list)

    # Anomaly-based filtering (used only in custom mode)
    use_anomalies_as_exclusions: bool = False
    anomaly_window_minutes: int = 0  # Window around each anomaly point (0 = point only)
    include_unreviewed_anomalies: bool = (
        False  # Whether to include unreviewed anomalies
    )

    # Resampling (used only in custom mode)
    resampling_enabled: bool = True
    frequency: str = "auto"
    aggregation_method: str = "sum"

    # Differencing (used only in custom mode)
    differencing_enabled: bool = False
    difference_order: int = 1

    # Missing data (used only in custom mode)
    missing_data_strategy: str = "propagate"
    missing_data_fill_value: Optional[float] = None

    # =========================================================================
    # Pipeline-specific configuration (used when pipeline_mode != "custom")
    # Dynamic configuration stored as dict keyed by pipeline name
    # =========================================================================
    pipeline_configs: dict = field(
        default_factory=dict
    )  # {pipeline_name: {param: value}}


def init_preprocessing_state() -> PreprocessingState:
    """Initialize preprocessing state in Streamlit session state.

    Handles migration from older state objects that may be missing new fields.
    """
    if "preprocessing_state" not in st.session_state:
        st.session_state.preprocessing_state = PreprocessingState()
    else:
        # Handle migration: add missing attributes from newer versions
        state = st.session_state.preprocessing_state

        # Legacy field migrations
        if not hasattr(state, "exclude_init_results"):
            state.exclude_init_results = False

        # New type-aware processing fields
        if not hasattr(state, "type_aware_enabled"):
            state.type_aware_enabled = True
        if not hasattr(state, "type_col"):
            state.type_col = "type"
        if not hasattr(state, "init_filter_enabled"):
            # Migrate from exclude_init_results if it was set
            state.init_filter_enabled = getattr(state, "exclude_init_results", False)
        if not hasattr(state, "init_trim_count"):
            state.init_trim_count = 1

        # Pipeline-specific dynamic config
        if not hasattr(state, "pipeline_configs"):
            state.pipeline_configs = {}

    return st.session_state.preprocessing_state


def get_assertion_context() -> tuple[Optional[str], Optional[str]]:
    """Get the current assertion type and metric from session state.

    Returns:
        Tuple of (assertion_type, metric_name) from session state.
        Both may be None if not available.
    """
    assertion_type = st.session_state.get("current_assertion_type")
    metric_name = st.session_state.get("current_assertion_metric")
    return assertion_type, metric_name


def serialize_preprocessing_state(state: PreprocessingState) -> dict:
    """Serialize preprocessing state to a dictionary for storage/display.

    DEPRECATED: Use serialize_applied_config() instead to get the actual
    observe-models configuration.

    Args:
        state: The preprocessing state to serialize

    Returns:
        Dictionary representation of the UI preprocessing state
    """
    config = {
        "pipeline_mode": state.pipeline_mode,
        "init_filter": {
            "enabled": state.init_filter_enabled,
            "trim_count": state.init_trim_count,
        },
        "anomaly_exclusion": {
            "enabled": state.use_anomalies_as_exclusions,
            "window_minutes": state.anomaly_window_minutes,
        },
    }

    if state.pipeline_mode == "custom":
        # Include custom mode settings
        config["resampling"] = {
            "enabled": state.resampling_enabled,
            "frequency": state.frequency,
            "aggregation_method": state.aggregation_method,
        }
        config["differencing"] = {
            "enabled": state.differencing_enabled,
            "order": state.difference_order,
        }
        config["missing_data"] = {
            "strategy": state.missing_data_strategy,
            "fill_value": state.missing_data_fill_value,
        }
        if state.filtering_enabled:
            config["data_filtering"] = {
                "enabled": True,
                "exclusion_ranges": len(state.exclusion_ranges),
                "inclusion_ranges": len(state.inclusion_ranges),
            }
    else:
        # Include pipeline-specific config
        pipeline_config = state.pipeline_configs.get(state.pipeline_mode, {})
        if pipeline_config:
            config["pipeline_options"] = pipeline_config

    return config


def serialize_applied_config(
    config: Any,
) -> Optional[dict]:
    """Serialize the actual PreprocessingConfig used by observe-models.

    This uses the observe-models serialization utilities to produce
    a dictionary representation of the real configuration object.

    Args:
        config: The PreprocessingConfig object from state_to_config()

    Returns:
        Dictionary representation of the actual PreprocessingConfig,
        or None if serialization not available
    """
    if config is None:
        return None

    if not HAS_PREPROCESSING:
        return None

    try:
        from datahub_observe.algorithms.preprocessing.serialization import (  # type: ignore[import-untyped]
            config_to_dict,
        )

        return config_to_dict(config)
    except ImportError:
        # Fallback if serialization not available
        return {"error": "Serialization not available"}


def build_config_for_display(
    state: PreprocessingState,
    config_overrides: Optional[dict] = None,
) -> Optional[dict]:
    """Build the complete config for display, handling predefined, custom, and inference pipelines.

    For predefined pipelines, this instantiates the pipeline and serializes its actual config.

    For custom mode, this uses state_to_config() which builds the config from UI state,
    including AnomalyDataFilterConfig if anomaly exclusion is enabled.

    For from_inference mode, this serializes the loaded inference config.

    Note: Anomaly exclusions are now handled via the type column. The DataFrame should
    have rows marked with type="ANOMALY" before preprocessing.

    Args:
        state: The preprocessing state
        config_overrides: Optional dict of pipeline config overrides

    Returns:
        Dictionary representation of the config for display
    """
    if not HAS_PREPROCESSING:
        return None

    try:
        from datahub_observe.algorithms.preprocessing.serialization import (
            config_to_dict,
            pipeline_config_to_dict,
        )
    except ImportError:
        return {"error": "Serialization not available"}

    result: dict = {}

    if state.pipeline_mode == "custom":
        # Custom mode - use state_to_config which builds config from UI state
        config = state_to_config(state)
        if config is not None:
            result = config_to_dict(config)
    elif state.pipeline_mode == "from_inference":
        # From inference mode - serialize the loaded inference config
        loaded_config = st.session_state.get("_loaded_inference_preprocessing_config")
        if loaded_config is not None:
            try:
                # Check if it's a pipeline config (volume/field)
                if hasattr(loaded_config, "type") and loaded_config.type in (
                    "volume",
                    "field",
                ):
                    result = pipeline_config_to_dict(loaded_config)
                elif isinstance(loaded_config, dict):
                    result = loaded_config
                else:
                    result = config_to_dict(loaded_config)
                result["_source"] = "inference"
                source_urn = st.session_state.get("_loaded_inference_source_urn", "")
                if source_urn:
                    result["_source_urn"] = source_urn
            except Exception as e:
                result = {"error": f"Could not serialize inference config: {e}"}
        else:
            result = {"error": "No inference config loaded"}
    else:
        # Predefined pipeline - get the actual pipeline config
        try:
            preprocessor = instantiate_pipeline(state.pipeline_mode, config_overrides)
            if hasattr(preprocessor, "config") and preprocessor.config is not None:
                result = config_to_dict(preprocessor.config)
                result["_pipeline"] = state.pipeline_mode
        except Exception as e:
            result = {"error": f"Could not get pipeline config: {e}"}

        # Note if anomaly exclusion is enabled (handled via type column)
        if state.use_anomalies_as_exclusions:
            result["_anomaly_exclusion_enabled"] = True

    return result if result else None


def render_config_expander(config: dict, title: str = "Configuration") -> None:
    """Render a preprocessing configuration in an expandable section.

    Args:
        config: The configuration dictionary to display
        title: The title for the expander
    """

    with st.expander(f"📋 {title}", expanded=False):
        st.json(config)


def get_pipeline_config_class(pipeline_name: str) -> Optional[type]:
    """Get the configuration class for a pipeline from the registry.

    Args:
        pipeline_name: The pipeline name (e.g., "volume", "field")

    Returns:
        The configuration class, or None if not found.
    """
    if not HAS_REGISTRY or get_preprocessing_registry is None:
        return None

    try:
        registry = get_preprocessing_registry()
        if not hasattr(registry, "get_config_class"):
            return None
        result = registry.get_config_class(pipeline_name)
        return result
    except Exception as e:
        # Log error for debugging but don't fail
        import logging

        logging.getLogger(__name__).warning(
            f"Failed to get config class for '{pipeline_name}': {type(e).__name__}: {e}"
        )
        return None


def get_pipeline_config_defaults(pipeline_name: str) -> dict:
    """Get default values for a pipeline's configuration.

    Args:
        pipeline_name: The pipeline name (e.g., "volume", "field")

    Returns:
        Dictionary of parameter names to default values.
    """
    config_cls = get_pipeline_config_class(pipeline_name)
    if config_cls is None:
        return {}

    # Check if config class has a get_defaults method
    if hasattr(config_cls, "get_defaults"):
        return config_cls.get_defaults()

    # Fall back to inspecting dataclass fields
    from dataclasses import MISSING, fields as dc_fields

    defaults = {}
    for f in dc_fields(config_cls):
        if f.name == "type":  # Skip type discriminator
            continue
        if f.name == "preprocessing_config":  # Skip nested config
            continue
        if f.default is not MISSING:
            defaults[f.name] = f.default
        elif f.default_factory is not MISSING:
            defaults[f.name] = f.default_factory()
    return defaults


def get_pipeline_config_metadata(pipeline_name: str) -> dict:
    """Get metadata about a pipeline's configuration from the registry.

    Args:
        pipeline_name: The pipeline name (e.g., "volume", "field")

    Returns:
        Dictionary with metadata (supported values, descriptions, etc.)
    """
    if not HAS_REGISTRY or get_preprocessing_registry is None:
        return {}

    try:
        registry = get_preprocessing_registry()
        entry = registry.get(pipeline_name)
        return entry.metadata or {}
    except Exception:
        return {}


def render_pipeline_config_ui(state: PreprocessingState, pipeline_name: str) -> None:
    """Render dynamic UI for pipeline-specific configuration.

    Discovers configuration options from the registry and renders
    appropriate UI controls based on field types.

    Args:
        state: The preprocessing state to update
        pipeline_name: The pipeline name (e.g., "volume", "field")
    """
    try:
        config_cls = get_pipeline_config_class(pipeline_name)
        if config_cls is None:
            return

        # Get defaults and current values
        defaults = get_pipeline_config_defaults(pipeline_name)
        metadata = get_pipeline_config_metadata(pipeline_name)

        # Initialize pipeline config if not present
        if pipeline_name not in state.pipeline_configs:
            state.pipeline_configs[pipeline_name] = defaults.copy()

        current_config = state.pipeline_configs[pipeline_name]

        # Get assertion context for auto-detection
        assertion_type, assertion_metric = get_assertion_context()

        # Render UI for each configurable field
        from dataclasses import fields as dc_fields

        st.markdown(f"##### {pipeline_name.title()} Pipeline Options")

        for f in dc_fields(config_cls):
            # Skip non-configurable fields
            if f.name in ("type", "preprocessing_config"):
                continue

            field_type = f.type
            default_val = defaults.get(f.name)
            current_val = current_config.get(f.name, default_val)

            # Special handling for metric_type field (for field pipeline)
            if f.name == "metric_type":
                # Auto-populate from assertion context if available
                if current_val is None and assertion_metric:
                    current_val = assertion_metric
                    current_config[f.name] = current_val

                # Get supported metrics from metadata
                supported_metrics = metadata.get("supported_metric_types", [])
                if supported_metrics:
                    options = [None] + supported_metrics
                    current_idx = 0
                    if current_val in options:
                        current_idx = options.index(current_val)

                    selected: Optional[str] = st.selectbox(
                        "Metric Type",
                        options=options,
                        index=current_idx,
                        format_func=lambda x: x if x else "(auto-detect)",
                        key=f"pipeline_{pipeline_name}_{f.name}",
                        help=f"Field metric type. Auto-detected from assertion: {assertion_metric or 'N/A'}",
                    )
                    current_config[f.name] = selected
                else:
                    # Fallback to text input
                    val = st.text_input(
                        "Metric Type",
                        value=current_val or "",
                        key=f"pipeline_{pipeline_name}_{f.name}",
                        help=f"Field metric type (e.g., NULL_COUNT, MEAN). Auto-detected: {assertion_metric or 'N/A'}",
                    )
                    current_config[f.name] = val if val else None

            # Boolean fields
            elif field_type is bool or str(field_type) == "bool":
                label = _format_field_label(f.name)
                help_text = _get_field_help(f.name, pipeline_name, metadata)
                val = st.checkbox(
                    label,
                    value=bool(current_val)
                    if current_val is not None
                    else bool(default_val),
                    key=f"pipeline_{pipeline_name}_{f.name}",
                    help=help_text,
                )
                current_config[f.name] = val

            # Optional[bool] fields
            elif "Optional[bool]" in str(field_type):
                label = _format_field_label(f.name)
                help_text = _get_field_help(f.name, pipeline_name, metadata)
                bool_options: list[Optional[bool]] = [None, True, False]
                labels: dict[Optional[bool], str] = {
                    None: "Not set",
                    True: "Yes",
                    False: "No",
                }
                current_idx = 0
                if current_val in bool_options:
                    current_idx = bool_options.index(current_val)

                def format_bool_option(
                    x: Optional[bool], lbl: dict[Optional[bool], str] = labels
                ) -> str:
                    return lbl.get(x, str(x))

                selected_bool: Optional[bool] = st.selectbox(
                    label,
                    options=bool_options,
                    index=current_idx,
                    format_func=format_bool_option,
                    key=f"pipeline_{pipeline_name}_{f.name}",
                    help=help_text,
                )
                current_config[f.name] = selected_bool

            # String fields
            elif field_type is str or "str" in str(field_type):
                label = _format_field_label(f.name)
                val = st.text_input(
                    label,
                    value=current_val or "",
                    key=f"pipeline_{pipeline_name}_{f.name}",
                )
                current_config[f.name] = val if val else None

        # Update state
        state.pipeline_configs[pipeline_name] = current_config
    except Exception as e:
        st.error(f"Error rendering pipeline config UI: {type(e).__name__}: {e}")


def _format_field_label(field_name: str) -> str:
    """Format a field name as a human-readable label."""
    # Convert snake_case to Title Case
    return field_name.replace("_", " ").title()


def _get_field_help(field_name: str, pipeline_name: str, metadata: dict) -> str:
    """Get help text for a field."""
    # Common help texts
    help_texts = {
        "convert_cumulative": (
            "Enable to convert cumulative/running total values to per-period deltas. "
            "Use when your data shows total counts (e.g., 'total rows = 1M') rather than changes."
        ),
        "convert_to_delta": (
            "Enable to convert absolute values to period-over-period changes. "
            "Field metrics like NULL_COUNT are absolute values that need differencing for forecasting."
        ),
        "allow_negative": (
            "Set to 'No' to validate that values are non-negative. "
            "Useful for metrics that should never be negative (e.g., row counts)."
        ),
        "metric_type": (
            "The field metric type determines aggregation behavior. "
            "Count metrics (NULL_COUNT, etc.) use 'sum', rate metrics use 'mean'."
        ),
    }
    return help_texts.get(field_name, "")


def render_pipeline_selector(state: PreprocessingState) -> str:
    """Render the pipeline mode selector.

    Args:
        state: The preprocessing state to update

    Returns:
        The selected pipeline mode ("custom" or a pipeline name)
    """
    options = get_pipeline_options()

    # Build option values and labels
    option_values = [opt[0] for opt in options]
    option_labels = {opt[0]: opt[1] for opt in options}

    # Determine current index
    try:
        current_index = option_values.index(state.pipeline_mode)
    except ValueError:
        current_index = 0  # Default to "custom" if saved mode not found

    selected = st.selectbox(
        "Preprocessing Mode",
        options=option_values,
        index=current_index,
        format_func=lambda x: option_labels.get(x, x),
        key="pipeline_mode_selector",
        help="Choose a predefined pipeline or configure custom preprocessing",
    )

    state.pipeline_mode = selected
    return selected


def render_pipeline_info(
    pipeline_name: str,
    state: Optional[PreprocessingState] = None,
) -> None:
    """Display information about a predefined pipeline from registry.

    Args:
        pipeline_name: The name of the pipeline to display info for
        state: Optional preprocessing state for rendering config UI
    """
    if not HAS_REGISTRY or get_preprocessing_registry is None:
        st.warning("Registry not available")
        return

    try:
        registry = get_preprocessing_registry()
        entry = registry.get(pipeline_name)

        # Display pipeline info
        st.info(f"**{entry.name.title()} Pipeline** (v{entry.version})")
        st.markdown(entry.description)

        # Display tags if available
        if entry.tags:
            tags_str = ", ".join(sorted(entry.tags))
            st.caption(f"Tags: {tags_str}")

        # Display metadata if available
        if entry.metadata:
            with st.expander("Pipeline Details", expanded=False):
                for key, value in entry.metadata.items():
                    st.markdown(f"**{key}:** {value}")

        # Render pipeline-specific configuration UI if state provided
        if state is not None:
            config_cls = get_pipeline_config_class(pipeline_name)
            if config_cls is not None:
                st.markdown("---")  # Visual separator
                render_pipeline_config_ui(state, pipeline_name)

    except EntryNotFoundError:
        st.error(f"Pipeline '{pipeline_name}' not found in registry")
    except Exception as e:
        st.error(f"Error loading pipeline info: {e}")


def render_inference_config_info() -> None:
    """Display information about the loaded inference preprocessing config.

    Shows the loaded config details and source URN. The config will be used
    directly when preprocessing is applied with "from_inference" mode selected.
    """
    loaded_config = st.session_state.get("_loaded_inference_preprocessing_config")
    source_urn = st.session_state.get("_loaded_inference_source_urn", "")

    if loaded_config is None:
        st.warning("No inference config loaded")
        return

    # Determine config type
    config_type = "custom"
    if hasattr(loaded_config, "type"):
        config_type = loaded_config.type
    elif isinstance(loaded_config, dict) and "type" in loaded_config:
        config_type = loaded_config["type"]

    st.info(f"**From Inference** - Using {config_type} config from monitor")

    # Show source URN
    if source_urn:
        st.caption(f"Source: `{source_urn[-60:]}`")

    # Show config details
    try:
        if hasattr(loaded_config, "__class__") and hasattr(
            loaded_config.__class__, "__name__"
        ):
            st.markdown(f"**Config Type:** `{loaded_config.__class__.__name__}`")

        # Try to serialize for display
        if HAS_PREPROCESSING:
            try:
                from datahub_observe.algorithms.preprocessing.serialization import (
                    config_to_dict,
                    pipeline_config_to_dict,
                )

                if hasattr(loaded_config, "type") and loaded_config.type in (
                    "volume",
                    "field",
                ):
                    config_dict = pipeline_config_to_dict(loaded_config)
                elif not isinstance(loaded_config, dict):
                    config_dict = config_to_dict(loaded_config)
                else:
                    config_dict = loaded_config

                render_config_expander(
                    config_dict, title="Loaded Inference Config (Read-Only)"
                )
            except Exception:
                # Fallback: show raw representation
                if isinstance(loaded_config, dict):
                    render_config_expander(
                        loaded_config, title="Loaded Inference Config (Read-Only)"
                    )
    except Exception as e:
        st.caption(f"Config loaded (details unavailable: {e})")

    # Button to clear the loaded config
    if st.button("Clear Loaded Config", key="clear_inference_config_btn"):
        st.session_state.pop("_loaded_inference_preprocessing_config", None)
        st.session_state.pop("_loaded_inference_source_urn", None)
        # Reset pipeline mode to custom if it was set to from_inference
        if st.session_state.get("preprocessing_state"):
            state = st.session_state.preprocessing_state
            if state.pipeline_mode == "from_inference":
                state.pipeline_mode = "custom"
        st.rerun()


def render_filtering_config(
    state: PreprocessingState,
    anomaly_count: int = 0,
) -> None:
    """Render the data filtering configuration UI.

    Args:
        state: The preprocessing state
        anomaly_count: Number of confirmed anomalies available for exclusion
    """
    with st.expander("Data Filtering", expanded=state.filtering_enabled):
        state.filtering_enabled = st.checkbox(
            "Enable Data Filtering",
            value=state.filtering_enabled,
            key="filtering_enabled",
        )

        if state.filtering_enabled:
            # Anomaly-based exclusions section
            st.markdown("**Anomaly-Based Exclusions**")

            state.use_anomalies_as_exclusions = st.checkbox(
                "Exclude confirmed anomalies",
                value=state.use_anomalies_as_exclusions,
                key="use_anomalies_as_exclusions",
                help="Automatically exclude time points marked as confirmed anomalies",
            )

            if state.use_anomalies_as_exclusions:
                col1, col2 = st.columns(2)
                with col1:
                    state.anomaly_window_minutes = st.number_input(
                        "Window around anomaly (minutes)",
                        min_value=0,
                        max_value=1440,  # Max 1 day
                        value=state.anomaly_window_minutes,
                        key="anomaly_window_minutes",
                        help="Time window to exclude around each anomaly point. 0 = point only.",
                    )
                with col2:
                    state.include_unreviewed_anomalies = st.checkbox(
                        "Include unreviewed anomalies",
                        value=state.include_unreviewed_anomalies,
                        key="include_unreviewed_anomalies",
                        help="Also exclude anomalies that haven't been reviewed yet",
                    )

                if anomaly_count > 0:
                    st.info(f"{anomaly_count} anomalies will be excluded")
                else:
                    st.caption("No confirmed anomalies found in cache")

            st.markdown("---")
            st.markdown(
                "**Manual Exclusion Ranges** - Time ranges to exclude from the data"
            )

            # Display existing exclusion ranges
            for i, range_item in enumerate(state.exclusion_ranges):
                col1, col2, col3 = st.columns([4, 4, 1])
                with col1:
                    start = st.date_input(
                        f"Start {i + 1}",
                        value=range_item.get("start"),
                        key=f"excl_start_{i}",
                    )
                with col2:
                    end = st.date_input(
                        f"End {i + 1}", value=range_item.get("end"), key=f"excl_end_{i}"
                    )
                with col3:
                    if st.button("X", key=f"remove_excl_{i}"):
                        state.exclusion_ranges.pop(i)
                        st.rerun()

                state.exclusion_ranges[i] = {"start": start, "end": end}

            if st.button("+ Add Exclusion Range"):
                state.exclusion_ranges.append(
                    {"start": datetime.now().date(), "end": datetime.now().date()}
                )
                st.rerun()


def render_resampling_config(state: PreprocessingState) -> None:
    """Render the resampling configuration UI."""
    with st.expander("Resampling", expanded=True):
        state.resampling_enabled = st.checkbox(
            "Enable Resampling",
            value=state.resampling_enabled,
            key="resampling_enabled",
        )

        if state.resampling_enabled:
            col1, col2 = st.columns(2)

            with col1:
                state.frequency = st.selectbox(
                    "Target Frequency",
                    options=["auto", "none", "hourly", "daily", "weekly"],
                    index=["auto", "none", "hourly", "daily", "weekly"].index(
                        state.frequency
                    ),
                    key="frequency",
                    help="""
                    - **auto**: Automatically determine based on input frequency
                    - **none**: Keep original frequency (no resampling)
                    - **hourly/daily/weekly**: Resample to fixed frequency
                    """,
                )

            with col2:
                state.aggregation_method = st.selectbox(
                    "Aggregation Method",
                    options=["sum", "mean", "median", "min", "max", "first", "last"],
                    index=[
                        "sum",
                        "mean",
                        "median",
                        "min",
                        "max",
                        "first",
                        "last",
                    ].index(state.aggregation_method),
                    key="aggregation_method",
                    help="How to aggregate values when resampling",
                )


def render_differencing_config(state: PreprocessingState) -> None:
    """Render the differencing configuration UI."""
    with st.expander("Differencing", expanded=state.differencing_enabled):
        state.differencing_enabled = st.checkbox(
            "Enable Differencing",
            value=state.differencing_enabled,
            key="differencing_enabled",
            help="Convert absolute values to period-over-period changes",
        )

        if state.differencing_enabled:
            state.difference_order = st.number_input(
                "Difference Order",
                min_value=1,
                max_value=2,
                value=state.difference_order,
                key="difference_order",
                help="1 = first difference (changes), 2 = second difference (acceleration)",
            )


def render_missing_data_config(state: PreprocessingState) -> None:
    """Render the missing data handling configuration UI."""
    with st.expander("Missing Data Handling", expanded=True):
        state.missing_data_strategy = st.selectbox(
            "Strategy",
            options=["propagate", "drop", "fill_zero", "fill_value", "interpolate"],
            index=["propagate", "drop", "fill_zero", "fill_value", "interpolate"].index(
                state.missing_data_strategy
            ),
            key="missing_data_strategy",
            help="""
            - **propagate**: Leave as NaN (good for Prophet)
            - **drop**: Remove rows with NaN
            - **fill_zero**: Fill with 0
            - **fill_value**: Fill with specific value
            - **interpolate**: Linear interpolation
            """,
        )

        if state.missing_data_strategy == "fill_value":
            state.missing_data_fill_value = st.number_input(
                "Fill Value",
                value=state.missing_data_fill_value or 0.0,
                key="missing_data_fill_value",
            )


def render_anomaly_exclusion_config(
    state: PreprocessingState,
    anomaly_count: int = 0,
) -> None:
    """Render the anomaly exclusion configuration UI (shared by custom and predefined).

    Args:
        state: The preprocessing state
        anomaly_count: Number of confirmed anomalies available for exclusion
    """
    with st.expander("Anomaly Exclusions", expanded=state.use_anomalies_as_exclusions):
        state.use_anomalies_as_exclusions = st.checkbox(
            "Exclude confirmed anomalies",
            value=state.use_anomalies_as_exclusions,
            key="use_anomalies_as_exclusions_shared",
            help="Automatically exclude time points marked as confirmed anomalies",
        )

        if state.use_anomalies_as_exclusions:
            col1, col2 = st.columns(2)
            with col1:
                state.anomaly_window_minutes = st.number_input(
                    "Window around anomaly (minutes)",
                    min_value=0,
                    max_value=1440,  # Max 1 day
                    value=state.anomaly_window_minutes,
                    key="anomaly_window_minutes_shared",
                    help="Time window to exclude around each anomaly point. 0 = point only.",
                )
            with col2:
                state.include_unreviewed_anomalies = st.checkbox(
                    "Include unreviewed anomalies",
                    value=state.include_unreviewed_anomalies,
                    key="include_unreviewed_anomalies_shared",
                    help="Also exclude anomalies that haven't been reviewed yet",
                )

            if anomaly_count > 0:
                st.info(f"{anomaly_count} anomalies will be excluded")
            else:
                st.caption("No confirmed anomalies found in cache")


def render_result_type_filter_config(
    state: PreprocessingState,
    init_count: int = 0,
) -> None:
    """Render the result type filtering configuration UI (shared by custom and predefined).

    Uses the new type-aware InitDataFilterTransformer architecture. The 'type' column
    containing result types (INIT, SUCCESS, etc.) is passed through to preprocessing,
    and the InitDataFilterTransformer handles INIT data appropriately.

    Args:
        state: The preprocessing state
        init_count: Number of INIT events in the current data (for informational purposes)
    """
    with st.expander("INIT Data Handling", expanded=state.init_filter_enabled):
        state.init_filter_enabled = st.checkbox(
            "Enable INIT data filtering",
            value=state.init_filter_enabled,
            key="init_filter_enabled",
            help="Filter INIT result types using type-aware preprocessing. "
            "INIT events typically represent initial/calibration runs.",
        )

        if state.init_filter_enabled:
            state.init_trim_count = st.number_input(
                "Trim count",
                min_value=0,
                max_value=10,
                value=state.init_trim_count,
                key="init_trim_count",
                help="Number of initial INIT values to trim. Set to 0 to keep all INIT data.",
            )

            if init_count > 0:
                if state.init_trim_count > 0:
                    trim_msg = (
                        f"First {state.init_trim_count} INIT event(s) will be trimmed"
                    )
                    st.info(f"{init_count} INIT events found. {trim_msg}")
                else:
                    st.info(f"{init_count} INIT events found (will be kept)")
            else:
                st.caption("No INIT events found in current data")

        # Keep exclude_init_results in sync for backward compatibility
        state.exclude_init_results = state.init_filter_enabled


def render_preprocessing_config_panel(
    anomaly_count: int = 0,
    init_count: int = 0,
) -> PreprocessingState:
    """Render the complete preprocessing configuration panel.

    Supports both custom configuration and predefined pipelines from the registry.
    When a predefined pipeline is selected, custom config options are hidden,
    but anomaly exclusion and result type filtering options remain available.

    Args:
        anomaly_count: Number of confirmed anomalies available for exclusion
        init_count: Number of INIT events in the current data

    Returns:
        The current PreprocessingState
    """
    st.subheader("Preprocessing Configuration")

    state = init_preprocessing_state()

    # Show pipeline selector (always visible)
    selected = render_pipeline_selector(state)

    # Result type filtering (available for all modes)
    render_result_type_filter_config(state, init_count=init_count)

    # Anomaly exclusion options (available for all modes)
    render_anomaly_exclusion_config(state, anomaly_count=anomaly_count)

    if selected == "custom":
        # Show all custom configuration options
        render_filtering_config(state, anomaly_count=anomaly_count)
        render_resampling_config(state)
        render_differencing_config(state)
        render_missing_data_config(state)
    elif selected == "from_inference":
        # Show loaded inference config info (read-only)
        render_inference_config_info()
    else:
        # Show predefined pipeline info with dynamic configuration options
        render_pipeline_info(selected, state=state)

    return state


def state_to_config(
    state: PreprocessingState,
) -> Optional[Any]:
    """Convert PreprocessingState to PreprocessingConfig.

    Uses the new list-based transformer architecture with separate
    pandas_transformers and darts_transformers lists.

    Note: Anomaly exclusions are now handled via the type column. Rows with
    type="ANOMALY" should be marked in the DataFrame before calling this,
    and the AnomalyDataFilterTransformer will filter them out.

    Args:
        state: The preprocessing state

    Returns:
        PreprocessingConfig object, or None if preprocessing not available
    """
    if not HAS_PREPROCESSING:
        st.error("Preprocessing module not available. Install observe-models package.")
        return None

    # Build pandas transformers list
    pandas_transformers: list = []

    # 1. Init data filter (handles INIT result types via type-aware processing)
    if state.init_filter_enabled:
        pandas_transformers.append(
            InitDataFilterConfig(
                enabled=True,
                trim_count=state.init_trim_count,
            )
        )
    else:
        # Include disabled config to maintain transformer order
        pandas_transformers.append(InitDataFilterConfig(enabled=False))

    # 2. Anomaly data filter (filters rows with type="ANOMALY")
    # The DataFrame should have anomaly rows marked before preprocessing
    pandas_transformers.append(
        AnomalyDataFilterConfig(
            enabled=state.use_anomalies_as_exclusions,
            type_values=["ANOMALY"],
        )
    )

    # 3. Data filtering (manual exclusion ranges)
    exclusion_ranges = []
    if state.filtering_enabled:
        for r in state.exclusion_ranges:
            if r.get("start") and r.get("end"):
                exclusion_ranges.append(
                    TimeRange(
                        start=datetime.combine(r["start"], datetime.min.time()),
                        end=datetime.combine(r["end"], datetime.max.time()),
                    )
                )
    pandas_transformers.append(DataFilterConfig(exclusion_ranges=exclusion_ranges))

    # 3. Frequency alignment (always included for proper time series handling)
    pandas_transformers.append(FrequencyAlignmentConfig())

    # 4. Missing data handling
    pandas_transformers.append(
        MissingDataConfig(
            strategy=state.missing_data_strategy,
            fill_value=state.missing_data_fill_value,
        )
    )

    # Build darts transformers list
    darts_transformers: list = []

    # 1. Differencing
    darts_transformers.append(
        DifferenceConfig(
            enabled=state.differencing_enabled,
            order=state.difference_order if state.differencing_enabled else 1,
        )
    )

    # 2. Resampling
    darts_transformers.append(
        ResamplingConfig(
            frequency=state.frequency if state.resampling_enabled else "none",
            aggregation_method=state.aggregation_method,
        )
    )

    # Determine type_col based on type-aware setting
    type_col = state.type_col if state.type_aware_enabled else None

    return PreprocessingConfig(
        type_col=type_col,  # type: ignore[arg-type]
        pandas_transformers=pandas_transformers,
        darts_transformers=darts_transformers,
    )


def apply_preprocessing(
    df: pd.DataFrame,
    config: Optional[Any] = None,
    state: Optional[PreprocessingState] = None,
) -> Optional[pd.DataFrame]:
    """Apply preprocessing to a DataFrame.

    Supports both custom configuration and predefined pipelines from the registry.
    If state.pipeline_mode is not "custom", uses the predefined pipeline.
    If state.pipeline_mode is "from_inference", uses the loaded inference config.

    Note: For anomaly exclusion, the DataFrame should have rows marked with
    type="ANOMALY" before calling this function. Use mark_anomalies_in_type_column()
    to mark confirmed anomaly timestamps. The AnomalyDataFilterTransformer will
    filter these rows during preprocessing.

    Args:
        df: Input DataFrame with 'ds', 'y', and optionally 'type' columns
        config: PreprocessingConfig object (if None, built from state)
        state: PreprocessingState (if config is None)

    Returns:
        Preprocessed DataFrame, or None if error
    """
    if not HAS_PREPROCESSING:
        st.error("Preprocessing module not available. Install observe-models package.")
        return None

    # Get state if not provided
    if state is None:
        state = init_preprocessing_state()

    # Check if using loaded inference config
    if state.pipeline_mode == "from_inference" and config is None:
        return _apply_inference_config(df, state)

    # Check if using a predefined pipeline
    if state.pipeline_mode != "custom" and config is None:
        # Get config overrides from state's pipeline_configs
        config_overrides = state.pipeline_configs.get(state.pipeline_mode, {})
        return _apply_predefined_pipeline(
            df, state.pipeline_mode, state, config_overrides
        )

    # Custom configuration path
    if config is None:
        config = state_to_config(state)

    if config is None:
        return None

    try:
        preprocessor = TimeSeriesPreprocessor(config)
        ts = preprocessor.process(df, datetime_col="ds", value_col="y")

        # Convert back to DataFrame
        result_df = ts.to_dataframe().reset_index()
        result_df.columns = ["ds", "y"]

        return result_df
    except Exception as e:
        st.error(f"Preprocessing error: {e}")
        return None


def _apply_predefined_pipeline(
    df: pd.DataFrame,
    pipeline_name: str,
    state: PreprocessingState,
    config_overrides: Optional[dict] = None,
) -> Optional[pd.DataFrame]:
    """Apply a predefined pipeline from the registry.

    Note: For anomaly exclusion, the DataFrame should have rows marked with
    type="ANOMALY" before calling this function. The AnomalyDataFilterTransformer
    is applied before the pipeline if anomaly exclusion is enabled.

    Args:
        df: Input DataFrame with 'ds', 'y', and optionally 'type' columns
        pipeline_name: Name of the pipeline in the registry
        state: PreprocessingState containing anomaly exclusion settings
        config_overrides: Optional dict of config parameters to override defaults

    Returns:
        Preprocessed DataFrame, or None if error
    """
    if not HAS_REGISTRY:
        st.error("Registry not available. Install observe-models package.")
        return None

    try:
        # Apply anomaly data filter if enabled (filters rows with type="ANOMALY")
        filtered_df = df.copy()
        if state.use_anomalies_as_exclusions and AnomalyDataFilterConfig is not None:
            anomaly_filter = AnomalyDataFilterConfig(
                enabled=True,
                type_values=["ANOMALY"],
            ).create_transformer()
            # Apply with type_col from state
            filtered_df = anomaly_filter.transform(filtered_df, type_col=state.type_col)

        # Instantiate pipeline from registry with config overrides
        preprocessor = instantiate_pipeline(pipeline_name, config_overrides)

        # Process the filtered data
        ts = preprocessor.process(filtered_df, datetime_col="ds", value_col="y")

        # Convert back to DataFrame
        result_df = ts.to_dataframe().reset_index()
        result_df.columns = ["ds", "y"]

        return result_df

    except EntryNotFoundError:
        st.error(f"Pipeline '{pipeline_name}' not found in registry")
        return None
    except Exception as e:
        st.error(f"Preprocessing error with '{pipeline_name}' pipeline: {e}")
        return None


def _apply_inference_config(
    df: pd.DataFrame,
    state: PreprocessingState,
) -> Optional[pd.DataFrame]:
    """Apply the loaded inference preprocessing config directly.

    Uses the config stored in session state from "Load from Monitor Inference".
    Supports VolumePreprocessorConfig, FieldMetricPreprocessorConfig, and
    PreprocessingConfig types.

    Note: For anomaly exclusion, the DataFrame should have rows marked with
    type="ANOMALY" before calling this function. The AnomalyDataFilterTransformer
    is applied before the inference config if anomaly exclusion is enabled.

    Args:
        df: Input DataFrame with 'ds', 'y', and optionally 'type' columns
        state: PreprocessingState containing anomaly exclusion settings

    Returns:
        Preprocessed DataFrame, or None if error
    """
    loaded_config = st.session_state.get("_loaded_inference_preprocessing_config")

    if loaded_config is None:
        st.error("No inference config loaded. Load a config first.")
        return None

    try:
        # Apply anomaly data filter if enabled (filters rows with type="ANOMALY")
        filtered_df = df.copy()
        if state.use_anomalies_as_exclusions and AnomalyDataFilterConfig is not None:
            anomaly_filter = AnomalyDataFilterConfig(
                enabled=True,
                type_values=["ANOMALY"],
            ).create_transformer()
            # Apply with type_col from state
            filtered_df = anomaly_filter.transform(filtered_df, type_col=state.type_col)

        # Determine the type of loaded config and create appropriate preprocessor
        config_type = None
        if hasattr(loaded_config, "type"):
            config_type = loaded_config.type
        elif isinstance(loaded_config, dict) and "type" in loaded_config:
            config_type = loaded_config["type"]

        # Import base type for annotation
        from datahub_observe.algorithms.preprocessing import TimeSeriesPreprocessor

        preprocessor: TimeSeriesPreprocessor

        if config_type == "volume":
            # VolumePreprocessorConfig - use VolumeTimeSeriesPreprocessor
            from datahub_observe.algorithms.preprocessing import (
                VolumeTimeSeriesPreprocessor,
            )

            preprocessor = VolumeTimeSeriesPreprocessor.from_config(loaded_config)
            ts = preprocessor.process(filtered_df, datetime_col="ds", value_col="y")

        elif config_type == "field":
            # FieldMetricPreprocessorConfig - use FieldMetricTimeSeriesPreprocessor
            from datahub_observe.algorithms.preprocessing import (
                FieldMetricTimeSeriesPreprocessor,
            )

            preprocessor = FieldMetricTimeSeriesPreprocessor.from_config(loaded_config)
            ts = preprocessor.process(filtered_df, datetime_col="ds", value_col="y")

        elif isinstance(loaded_config, dict):
            # Raw dict - try to deserialize as PreprocessingConfig
            from datahub_observe.algorithms.preprocessing.serialization import (
                config_from_dict,
            )

            config = config_from_dict(loaded_config, check_schema=False)
            if isinstance(config, dict):
                st.error(
                    "Could not deserialize inference config. "
                    "The config format may be incompatible."
                )
                return None
            preprocessor = TimeSeriesPreprocessor(config)
            ts = preprocessor.process(filtered_df, datetime_col="ds", value_col="y")

        else:
            # Assume it's a PreprocessingConfig
            preprocessor = TimeSeriesPreprocessor(loaded_config)
            ts = preprocessor.process(filtered_df, datetime_col="ds", value_col="y")

        # Convert back to DataFrame
        result_df = ts.to_dataframe().reset_index()
        result_df.columns = ["ds", "y"]

        return result_df

    except Exception as e:
        st.error(f"Preprocessing error with inference config: {e}")
        import traceback

        st.caption(f"Details: {traceback.format_exc()}")
        return None


def render_before_after_chart(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    title: str = "Before/After Preprocessing",
) -> None:
    """Render a side-by-side comparison chart.

    Args:
        before_df: Original DataFrame with 'ds' and 'y'
        after_df: Preprocessed DataFrame with 'ds' and 'y'
        title: Chart title
    """
    fig = make_subplots(
        rows=2,
        cols=1,
        subplot_titles=("Before Preprocessing", "After Preprocessing"),
        shared_xaxes=True,
        vertical_spacing=0.1,
    )

    # Use Scattergl for large datasets
    scatter_type = go.Scattergl if len(before_df) > 5000 else go.Scatter

    # Before
    fig.add_trace(
        scatter_type(
            x=before_df["ds"],
            y=before_df["y"],
            mode="lines+markers" if len(before_df) < 500 else "lines",
            name="Original",
            line=dict(color="#1f77b4"),
            marker=dict(size=3),
        ),
        row=1,
        col=1,
    )

    # After
    fig.add_trace(
        scatter_type(
            x=after_df["ds"],
            y=after_df["y"],
            mode="lines+markers" if len(after_df) < 500 else "lines",
            name="Preprocessed",
            line=dict(color="#2ca02c"),
            marker=dict(size=3),
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        height=500,
        title_text=title,
        showlegend=True,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_preprocessing_stats(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> None:
    """Render statistics comparing before and after preprocessing.

    Args:
        before_df: Original DataFrame with 'ds' and 'y'
        after_df: Preprocessed DataFrame with 'ds' and 'y'
    """
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Points",
            f"{len(after_df):,}",
            f"{len(after_df) - len(before_df):+,}",
            delta_color="normal" if len(after_df) <= len(before_df) else "off",
        )

    with col2:
        before_nulls = before_df["y"].isna().sum()
        after_nulls = after_df["y"].isna().sum()
        st.metric(
            "Missing Values",
            f"{after_nulls:,}",
            f"{after_nulls - before_nulls:+,}",
            delta_color="inverse",
        )

    with col3:
        before_std = before_df["y"].std()
        after_std = after_df["y"].std()
        pct_change = ((after_std - before_std) / before_std * 100) if before_std else 0
        st.metric(
            "Std Dev",
            f"{after_std:.2f}",
            f"{pct_change:+.1f}%",
        )

    # Value range comparison
    st.markdown("**Value Range**")
    range_data = {
        "": ["Before", "After"],
        "Min": [before_df["y"].min(), after_df["y"].min()],
        "Max": [before_df["y"].max(), after_df["y"].max()],
        "Mean": [before_df["y"].mean(), after_df["y"].mean()],
        "Median": [before_df["y"].median(), after_df["y"].median()],
    }
    st.dataframe(pd.DataFrame(range_data), hide_index=True)
