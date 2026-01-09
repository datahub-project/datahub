# ruff: noqa: INP001
"""Model Training page for running prediction models on preprocessed data."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from prophet import Prophet

from ..common import DataLoader, init_explorer_state
from .observe_models_adapter import (
    get_model_preprocessing_defaults,
    get_observe_model_configs,
    is_observe_models_available,
)

# =============================================================================
# Model Registry
# =============================================================================


@dataclass
class ModelConfig:
    """Configuration for a prediction model."""

    name: str
    description: str
    train_fn: Callable[[pd.DataFrame], object]
    predict_fn: Callable[[object, pd.DataFrame], pd.DataFrame]
    color: str
    dash: Optional[str] = None
    is_observe_model: bool = False  # True for observe-models registry models
    registry_key: Optional[str] = None  # Key in observe-models registry (if applicable)


def _train_datahub_base(train_df: pd.DataFrame) -> Prophet:
    """Train the DataHub Base Prophet model."""
    model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=False,
        interval_width=0.95,
        changepoint_prior_scale=0.05,
    )
    model.fit(train_df)
    return model


def _predict_prophet(model: Prophet, future_df: pd.DataFrame) -> pd.DataFrame:
    """Generate predictions using a Prophet model."""
    return model.predict(future_df)


# Model registry - add new models here
MODEL_REGISTRY: dict[str, ModelConfig] = {
    "datahub_base": ModelConfig(
        name="DataHub Base",
        description="Prophet model with weekly seasonality optimized for DataHub metrics",
        train_fn=_train_datahub_base,
        predict_fn=_predict_prophet,
        color="#2ca02c",  # Green
        dash="dash",
    ),
}

# Extend registry with observe-models forecasters if available
if is_observe_models_available():
    for key, obs_config in get_observe_model_configs().items():
        # Only add if train_fn and predict_fn are defined
        if obs_config.train_fn is not None and obs_config.predict_fn is not None:
            MODEL_REGISTRY[key] = ModelConfig(
                name=obs_config.name,
                description=obs_config.description,
                train_fn=obs_config.train_fn,
                predict_fn=obs_config.predict_fn,
                color=obs_config.color,
                dash=obs_config.dash,
                is_observe_model=True,
                registry_key=obs_config.registry_key,
            )


# =============================================================================
# Preprocessing Compatibility
# =============================================================================


def check_preprocessing_model_compatibility(
    preprocessing_config: Optional[object],
    model_registry_key: Optional[str],
) -> list[str]:
    """
    Check if preprocessing configuration is compatible with model's preferred defaults.

    Compares the user's preprocessing config against the model's PreprocessingDefaults
    from the registry. Returns a list of warning messages for any mismatches.

    Args:
        preprocessing_config: The PreprocessingConfig object being used
        model_registry_key: The model's registry key (e.g., "prophet", "nbeats")

    Returns:
        List of warning messages. Empty list if fully compatible or unable to check.
    """
    if preprocessing_config is None or model_registry_key is None:
        return []

    # Get model's preprocessing defaults from registry
    defaults = get_model_preprocessing_defaults(model_registry_key)
    if defaults is None:
        return []  # No defaults defined, no warnings needed

    warnings: list[str] = []

    try:
        # Check transformer_defaults (individual transformer overrides)
        transformer_defaults = getattr(defaults, "transformer_defaults", None)
        if transformer_defaults:
            for transformer_name, default_config in transformer_defaults.items():
                _check_transformer_config(
                    preprocessing_config,
                    transformer_name,
                    default_config,
                    warnings,
                )
    except Exception:
        # If checking fails, don't block the user - just skip warnings
        pass

    return warnings


def _check_transformer_config(
    preprocessing_config: object,
    transformer_name: str,
    default_config: object,
    warnings: list[str],
) -> None:
    """
    Check if a specific transformer in the config differs from model defaults.

    Args:
        preprocessing_config: The PreprocessingConfig object
        transformer_name: Name of the transformer to check
        default_config: The model's default config for this transformer
        warnings: List to append warnings to
    """
    # Check pandas_transformers
    pandas_transformers = getattr(preprocessing_config, "pandas_transformers", [])
    for transformer in pandas_transformers:
        config_name = type(transformer).__name__.lower().replace("config", "")
        if transformer_name.replace("_", "") in config_name.replace("_", ""):
            # Found matching transformer - compare key attributes
            for attr in ["strategy", "enabled", "fill_value"]:
                user_val = getattr(transformer, attr, None)
                default_val = getattr(default_config, attr, None)
                if user_val is not None and default_val is not None:
                    if user_val != default_val:
                        warnings.append(
                            f"'{transformer_name}' uses '{attr}={user_val}' "
                            f"but model prefers '{attr}={default_val}'"
                        )

    # Check darts_transformers
    darts_transformers = getattr(preprocessing_config, "darts_transformers", [])
    for transformer in darts_transformers:
        config_name = type(transformer).__name__.lower().replace("config", "")
        if transformer_name.replace("_", "") in config_name.replace("_", ""):
            # Found matching transformer - compare key attributes
            for attr in ["enabled", "order", "frequency"]:
                user_val = getattr(transformer, attr, None)
                default_val = getattr(default_config, attr, None)
                if user_val is not None and default_val is not None:
                    if user_val != default_val:
                        warnings.append(
                            f"'{transformer_name}' uses '{attr}={user_val}' "
                            f"but model prefers '{attr}={default_val}'"
                        )


# =============================================================================
# Data Processing
# =============================================================================


def _split_train_test(
    df: pd.DataFrame, train_ratio: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split time series data into train and test sets."""
    df_sorted = df.sort_values("ds").reset_index(drop=True)
    split_idx = int(len(df_sorted) * train_ratio)
    train_df = df_sorted.iloc[:split_idx].copy()
    test_df = df_sorted.iloc[split_idx:].copy()
    return train_df, test_df


def _compute_metrics(actual: pd.Series, predicted: pd.Series) -> dict[str, float]:
    """Compute model performance metrics."""
    actual_arr = np.asarray(actual.values)
    predicted_arr = np.asarray(predicted.values)

    # Mean Absolute Error
    mae = float(np.mean(np.abs(actual_arr - predicted_arr)))

    # Root Mean Squared Error
    rmse = float(np.sqrt(np.mean((actual_arr - predicted_arr) ** 2)))

    # Mean Absolute Percentage Error (handle zeros)
    non_zero_mask = actual_arr != 0
    if np.any(non_zero_mask):
        mape = float(
            np.mean(
                np.abs(
                    (actual_arr[non_zero_mask] - predicted_arr[non_zero_mask])
                    / actual_arr[non_zero_mask]
                )
            )
            * 100
        )
    else:
        mape = float("nan")

    return {"MAE": mae, "RMSE": rmse, "MAPE": mape}


# =============================================================================
# Training Runs Storage
# =============================================================================

# Session state keys for training context
_TRAINING_RUNS_KEY = "training_runs"
_TRAINING_ASSERTION_KEY = "training_assertion_urn"


@dataclass
class TrainingRun:
    """Results from a training run."""

    run_id: str
    model_key: str
    model_name: str
    preprocessing_id: str
    train_df: pd.DataFrame
    test_df: pd.DataFrame
    forecast: pd.DataFrame
    model: object
    metrics: dict[str, float]
    color: str
    dash: Optional[str]
    timestamp: datetime
    assertion_urn: Optional[str] = None  # Track which assertion this run belongs to
    is_observe_model: bool = False  # Whether this is from observe-models registry
    registry_key: Optional[str] = None  # Key in observe-models registry (if applicable)
    preprocessing_config_dict: Optional[dict] = None  # Serialized preprocessing config

    @property
    def display_name(self) -> str:
        """Display name combining model and preprocessing."""
        return f"{self.model_name} + {self.preprocessing_id}"

    def get_preprocessing_warnings(self) -> list[str]:
        """Get warnings about preprocessing/model compatibility.

        Returns:
            List of warning messages if preprocessing conflicts with model defaults.
        """
        if not self.is_observe_model or not self.registry_key:
            return []

        return check_preprocessing_model_compatibility(
            preprocessing_config=None,  # Would need to reconstruct from dict
            model_registry_key=self.registry_key,
        )


def _get_current_assertion_urn() -> Optional[str]:
    """Get the current assertion URN from session state."""
    return st.session_state.get("selected_assertion_urn")


def _check_assertion_context_changed() -> bool:
    """Check if the assertion context has changed and reload cached runs if so."""
    current_assertion = _get_current_assertion_urn()
    stored_assertion = st.session_state.get(_TRAINING_ASSERTION_KEY)

    if current_assertion != stored_assertion:
        # Assertion changed - clear session runs (not cache) and reload from cache
        st.session_state[_TRAINING_RUNS_KEY] = {}
        st.session_state[_TRAINING_ASSERTION_KEY] = current_assertion
        # Load cached runs for new assertion
        _load_cached_training_runs()
        return True
    return False


_CACHE_LOADED_KEY = "training_runs_cache_loaded"


def _get_training_runs() -> dict[str, TrainingRun]:
    """Get stored training runs from session state, loading from cache if needed."""
    # Check if assertion context changed and clear if needed
    _check_assertion_context_changed()

    if _TRAINING_RUNS_KEY not in st.session_state:
        st.session_state[_TRAINING_RUNS_KEY] = {}

    # Load from cache on first access (once per session)
    if not st.session_state.get(_CACHE_LOADED_KEY, False):
        st.session_state[_CACHE_LOADED_KEY] = True
        _load_cached_training_runs()

    return st.session_state[_TRAINING_RUNS_KEY]


def _store_training_run(run: TrainingRun, save_to_cache: bool = True) -> None:
    """Store a training run in session state and optionally to disk cache.

    Args:
        run: The training run to store
        save_to_cache: If True, also persist to disk cache
    """
    runs = _get_training_runs()
    runs[run.run_id] = run

    # Save to disk cache
    if save_to_cache:
        hostname = st.session_state.get("current_hostname")
        if hostname:
            loader = DataLoader()
            endpoint_cache = loader.cache.get_endpoint_cache(hostname)
            endpoint_cache.save_training_run(
                run_id=run.run_id,
                model_key=run.model_key,
                model_name=run.model_name,
                preprocessing_id=run.preprocessing_id,
                train_df=run.train_df,
                test_df=run.test_df,
                forecast=run.forecast,
                metrics=run.metrics,
                color=run.color,
                dash=run.dash,
                assertion_urn=run.assertion_urn,
                is_observe_model=run.is_observe_model,
                registry_key=run.registry_key,
            )


def _delete_training_run(run_id: str, delete_from_cache: bool = True) -> None:
    """Delete a training run from session state and optionally from disk cache.

    Args:
        run_id: The run ID to delete
        delete_from_cache: If True, also delete from disk cache
    """
    runs = _get_training_runs()
    if run_id in runs:
        del runs[run_id]

    # Delete from disk cache
    if delete_from_cache:
        hostname = st.session_state.get("current_hostname")
        if hostname:
            loader = DataLoader()
            endpoint_cache = loader.cache.get_endpoint_cache(hostname)
            endpoint_cache.delete_training_run(run_id)


def _clear_training_runs(clear_cache: bool = False) -> None:
    """Clear all stored training runs from session state.

    Args:
        clear_cache: If True, also clear all runs from disk cache
    """
    # If clearing cache, delete each run from disk first
    if clear_cache:
        hostname = st.session_state.get("current_hostname")
        if hostname:
            loader = DataLoader()
            endpoint_cache = loader.cache.get_endpoint_cache(hostname)
            runs = _get_training_runs()
            for run_id in list(runs.keys()):
                endpoint_cache.delete_training_run(run_id)

    st.session_state[_TRAINING_RUNS_KEY] = {}


def _load_cached_training_runs() -> None:
    """Load training runs from disk cache into session state.

    Note: This function directly accesses session state to avoid circular
    reference with _get_training_runs().
    """
    hostname = st.session_state.get("current_hostname")
    assertion_urn = _get_current_assertion_urn()

    if not hostname:
        return

    loader = DataLoader()
    endpoint_cache = loader.cache.get_endpoint_cache(hostname)

    # Get cached runs for this assertion
    cached_runs = endpoint_cache.list_saved_training_runs(assertion_urn=assertion_urn)

    if not cached_runs:
        return

    # Ensure runs dict exists in session state
    if _TRAINING_RUNS_KEY not in st.session_state:
        st.session_state[_TRAINING_RUNS_KEY] = {}
    runs = st.session_state[_TRAINING_RUNS_KEY]

    # Load each run into session state (if not already present)
    for run_meta in cached_runs:
        run_id = run_meta["run_id"]
        if run_id not in runs:
            # Load full run data
            run_data = endpoint_cache.load_training_run(run_id)
            if run_data:
                run = TrainingRun(
                    run_id=run_data["run_id"],
                    model_key=run_data["model_key"],
                    model_name=run_data["model_name"],
                    preprocessing_id=run_data["preprocessing_id"],
                    train_df=run_data["train_df"],
                    test_df=run_data["test_df"],
                    forecast=run_data["forecast"],
                    model=run_data["model"],  # Will be None (not persisted)
                    metrics=run_data["metrics"],
                    color=run_data["color"],
                    dash=run_data["dash"],
                    timestamp=run_data["timestamp"],
                    assertion_urn=run_data["assertion_urn"],
                    is_observe_model=run_data["is_observe_model"],
                    registry_key=run_data["registry_key"],
                )
                runs[run_id] = run


def _generate_run_id(model_key: str, preprocessing_id: str) -> str:
    """Generate a unique run ID."""
    return f"{model_key}__{preprocessing_id}"


# =============================================================================
# UI Components
# =============================================================================


def _get_model_hyperparameters(model: object) -> dict[str, object]:
    """Extract hyperparameters from a trained model.

    Works with both observe-models and local Prophet models.
    """
    params: dict[str, object] = {}

    if model is None:
        return {"note": "Model not available (loaded from cache)"}

    # observe-models: best_params from hyperparameter tuning
    if hasattr(model, "best_params") and model.best_params:
        params.update(model.best_params)

    # Prophet model attributes
    prophet_attrs = [
        "changepoint_prior_scale",
        "seasonality_prior_scale",
        "holidays_prior_scale",
        "seasonality_mode",
        "changepoint_range",
        "n_changepoints",
        "yearly_seasonality",
        "weekly_seasonality",
        "daily_seasonality",
        "interval_width",
    ]
    for attr in prophet_attrs:
        if hasattr(model, attr):
            val = getattr(model, attr)
            if val is not None:
                params[attr] = val

    # observe-models: check for inner model (DartsBaseForecastModel wraps models)
    if hasattr(model, "model") and model.model is not None:
        inner = model.model
        # Darts Prophet wrapper
        if hasattr(inner, "model") and inner.model is not None:
            inner_prophet = inner.model
            for attr in prophet_attrs:
                if hasattr(inner_prophet, attr) and attr not in params:
                    val = getattr(inner_prophet, attr)
                    if val is not None:
                        params[attr] = val

    # NBEATS/Neural models: check for model config
    if hasattr(model, "darts_model_config"):
        config = model.darts_model_config
        if config:
            params["model_config"] = config

    return params if params else {"note": "No hyperparameters available"}


def _render_training_runs_panel() -> None:
    """Render panel showing all training runs with delete buttons."""
    runs = _get_training_runs()

    if not runs:
        st.info(
            "No training runs yet. Select preprocessing and model, then run training."
        )
        return

    st.subheader("Training Runs")

    # Header with clear all button
    col_header, col_clear = st.columns([4, 1])
    with col_header:
        st.caption(f"{len(runs)} run(s) in session")
    with col_clear:
        if st.button("Clear All", key="clear_all_runs"):
            _clear_training_runs()
            st.rerun()

    # List runs sorted by MAE (best/lowest first)
    sorted_runs = sorted(
        runs.items(),
        key=lambda x: float(x[1].metrics.get("MAE", float("inf"))),
    )
    for run_id, run in sorted_runs:
        col_info, col_metrics, col_time, col_info_btn, col_del = st.columns(
            [3, 2, 1.5, 0.5, 0.5]
        )

        with col_info:
            st.markdown(f"**{run.model_name}** + {run.preprocessing_id}")

        with col_metrics:
            mae = run.metrics.get("MAE", 0)
            st.caption(f"MAE: {mae:.2f}")

        with col_time:
            # Handle both timezone-aware and naive datetimes
            now = datetime.now(timezone.utc)
            ts = run.timestamp
            if ts.tzinfo is None:
                # Make naive timestamp UTC-aware for comparison
                ts = ts.replace(tzinfo=timezone.utc)
            time_ago = now - ts
            if time_ago.total_seconds() < 60:
                time_str = "just now"
            elif time_ago.total_seconds() < 3600:
                time_str = f"{int(time_ago.total_seconds() // 60)}m ago"
            elif time_ago.days == 0:
                time_str = f"Today {run.timestamp.strftime('%H:%M')}"
            elif time_ago.days == 1:
                time_str = f"Yesterday {run.timestamp.strftime('%H:%M')}"
            else:
                time_str = run.timestamp.strftime("%Y-%m-%d %H:%M")
            st.caption(time_str)

        with col_info_btn:
            # Info button to show hyperparameters
            if st.button("ℹ️", key=f"info_run_{run_id}", help="View hyperparameters"):
                st.session_state[f"show_params_{run_id}"] = not st.session_state.get(
                    f"show_params_{run_id}", False
                )

        with col_del:
            if st.button("🗑️", key=f"del_run_{run_id}", help="Delete run"):
                _delete_training_run(run_id)
                st.rerun()

        # Show hyperparameters if toggled
        if st.session_state.get(f"show_params_{run_id}", False):
            params = _get_model_hyperparameters(run.model)
            with st.container():
                st.markdown("**Hyperparameters:**")
                # Format as a nice display
                param_lines = []
                for key, value in params.items():
                    if isinstance(value, dict):
                        param_lines.append(f"- **{key}**: `{value}`")
                    elif isinstance(value, float):
                        param_lines.append(f"- **{key}**: `{value:.6g}`")
                    else:
                        param_lines.append(f"- **{key}**: `{value}`")
                st.markdown("\n".join(param_lines))
                st.markdown("---")


# =============================================================================
# Training Groups Storage
# =============================================================================

_TRAINING_GROUPS_KEY = "training_groups"


@dataclass
class TrainingGroup:
    """A group of models to train on a specific preprocessing."""

    preprocessing_id: str
    model_keys: list[str]


def _get_training_groups() -> list[TrainingGroup]:
    """Get training groups from session state."""
    if _TRAINING_GROUPS_KEY not in st.session_state:
        st.session_state[_TRAINING_GROUPS_KEY] = []
    return st.session_state[_TRAINING_GROUPS_KEY]


def _set_training_groups(groups: list[TrainingGroup]) -> None:
    """Set training groups in session state."""
    st.session_state[_TRAINING_GROUPS_KEY] = groups


def _clear_training_groups() -> None:
    """Clear all training groups."""
    st.session_state[_TRAINING_GROUPS_KEY] = []


# =============================================================================
# Main Page
# =============================================================================


def render_model_training_page() -> None:
    """Render the Model Training page."""
    st.header("Model Training")

    init_explorer_state()
    loader = DataLoader()

    # Navigation - Back to Preprocessing
    col_nav_back, col_nav_spacer = st.columns([1, 4])
    with col_nav_back:
        if st.button("← Preprocessing"):
            from . import preprocessing_page

            st.switch_page(preprocessing_page)

    st.markdown("---")

    # Get hostname and load saved preprocessings
    hostname = st.session_state.get("current_hostname")
    assertion_urn = _get_current_assertion_urn()
    saved_preprocessings: list[dict] = []
    if hostname:
        endpoint_cache = loader.cache.get_endpoint_cache(hostname)
        # Filter by current assertion to only show relevant preprocessings
        saved_preprocessings = endpoint_cache.list_saved_preprocessings(
            assertion_urn=assertion_urn
        )

    # Build preprocessing options
    preprocessing_options: list[str] = []
    preprocessing_labels: dict[str, str] = {}
    preprocessing_row_counts: dict[str, int] = {}

    # Add current session preprocessing if available
    current_preprocessed_df = st.session_state.get("preprocessed_df")
    if current_preprocessed_df is not None and len(current_preprocessed_df) > 0:
        preprocessing_options.append("__current__")
        preprocessing_labels["__current__"] = (
            f"Current Session ({len(current_preprocessed_df)} rows)"
        )
        preprocessing_row_counts["__current__"] = len(current_preprocessed_df)

    # Add saved preprocessings
    for item in saved_preprocessings:
        pid = item["preprocessing_id"]
        row_count = item.get("row_count", 0)
        preprocessing_options.append(pid)
        preprocessing_labels[pid] = f"{pid} ({row_count} rows)"
        preprocessing_row_counts[pid] = row_count if isinstance(row_count, int) else 0

    if not preprocessing_options:
        st.warning(
            "No preprocessed data available. "
            "Go to the Preprocessing page to configure and save preprocessing first."
        )
        from . import preprocessing_page

        if st.button("← Go to Preprocessing"):
            st.switch_page(preprocessing_page)
        return

    # Two-column layout
    col_config, col_runs = st.columns([1, 1])

    with col_config:
        # =================================================================
        # 1. Train/Test Split (moved to top)
        # =================================================================
        st.subheader("Train/Test Split")

        col_min, col_slider, col_max = st.columns([1, 4, 1])
        with col_min:
            st.caption("Min: 50%")
        with col_slider:
            train_pct = st.slider(
                "Train/Test Split",
                min_value=50,
                max_value=95,
                value=80,
                step=5,
                format="%d%%",
                help="Percentage of data to use for training (applies to all preprocessings)",
                label_visibility="collapsed",
            )
        with col_max:
            st.caption("Max: 95%")

        train_ratio = train_pct / 100.0
        st.caption(f"Training: {train_pct}% | Testing: {100 - train_pct}%")

        st.markdown("---")

        # =================================================================
        # 2. Grouped Training Selection
        # =================================================================
        st.subheader("Training Configuration")

        # Model options
        available_models = list(MODEL_REGISTRY.keys())
        model_labels = {k: v.name for k, v in MODEL_REGISTRY.items()}

        # Primary training group
        selected_preprocessing = st.selectbox(
            "Preprocessing",
            options=preprocessing_options,
            format_func=lambda x: preprocessing_labels.get(x, x),
            help="Select a preprocessing configuration",
            key="primary_preprocessing",
        )

        # Show data point info for selected preprocessing
        row_count = preprocessing_row_counts.get(selected_preprocessing, 0)
        if row_count > 0:
            train_pts = int(row_count * train_ratio)
            test_pts = row_count - train_pts
            st.caption(f"Data points: {train_pts} train | {test_pts} test")

        # Use checkboxes for model selection - clear visibility
        st.write("**Models to train**")
        selected_model_keys: list[str] = []
        model_cols = st.columns(min(3, len(available_models)))
        for idx, model_key in enumerate(available_models):
            col_idx = idx % len(model_cols)
            with model_cols[col_idx]:
                # Default datahub_base to checked
                default_checked = model_key == "datahub_base"
                is_checked = st.checkbox(
                    model_labels.get(model_key, model_key),
                    value=default_checked,
                    key=f"primary_model_cb_{model_key}",
                )
                if is_checked:
                    selected_model_keys.append(model_key)

        # Additional training groups
        if "additional_groups" not in st.session_state:
            st.session_state["additional_groups"] = []

        additional_groups: list[dict] = st.session_state["additional_groups"]

        # Render additional groups
        groups_to_remove: list[int] = []
        for idx, group in enumerate(additional_groups):
            with st.expander(f"Additional Group {idx + 1}", expanded=True):
                col_preproc, col_remove = st.columns([4, 1])
                with col_preproc:
                    group["preprocessing"] = st.selectbox(
                        "Preprocessing",
                        options=preprocessing_options,
                        format_func=lambda x: preprocessing_labels.get(x, x),
                        key=f"group_{idx}_preprocessing",
                        index=preprocessing_options.index(
                            group.get("preprocessing", preprocessing_options[0])
                        )
                        if group.get("preprocessing") in preprocessing_options
                        else 0,
                    )
                with col_remove:
                    if st.button("✕", key=f"remove_group_{idx}"):
                        groups_to_remove.append(idx)

                # Model selection with checkboxes
                st.write("**Models**")
                group_selected_models: list[str] = []
                group_model_cols = st.columns(min(3, len(available_models)))
                for m_idx, model_key in enumerate(available_models):
                    m_col_idx = m_idx % len(group_model_cols)
                    with group_model_cols[m_col_idx]:
                        prev_selected = group.get("models", [])
                        is_checked = st.checkbox(
                            model_labels.get(model_key, model_key),
                            value=model_key in prev_selected,
                            key=f"group_{idx}_model_cb_{model_key}",
                        )
                        if is_checked:
                            group_selected_models.append(model_key)
                group["models"] = group_selected_models

        # Remove marked groups
        for idx in reversed(groups_to_remove):
            additional_groups.pop(idx)
            st.rerun()

        # Add another group button
        if st.button("+ Add Another Preprocessing", key="add_group"):
            additional_groups.append(
                {
                    "preprocessing": preprocessing_options[0]
                    if preprocessing_options
                    else "",
                    "models": [],
                }
            )
            st.rerun()

        st.markdown("---")

        # =================================================================
        # 3. Training Summary
        # =================================================================
        st.subheader("Training Summary")

        # Build list of all training runs to execute
        all_training_items: list[
            tuple[str, str, str]
        ] = []  # (preprocessing_id, model_key, display_label)

        # Primary group
        preprocessing_id = (
            "current"
            if selected_preprocessing == "__current__"
            else selected_preprocessing
        )
        for model_key in selected_model_keys:
            display_label = f"{preprocessing_labels.get(selected_preprocessing, preprocessing_id)} → {model_labels.get(model_key, model_key)}"
            all_training_items.append((preprocessing_id, model_key, display_label))

        # Additional groups
        for group in additional_groups:
            grp_preprocessing = group.get("preprocessing", "")
            grp_preprocessing_id = (
                "current" if grp_preprocessing == "__current__" else grp_preprocessing
            )
            for model_key in group.get("models", []):
                display_label = f"{preprocessing_labels.get(grp_preprocessing, grp_preprocessing_id)} → {model_labels.get(model_key, model_key)}"
                all_training_items.append(
                    (grp_preprocessing_id, model_key, display_label)
                )

        if all_training_items:
            st.caption(f"{len(all_training_items)} training run(s) selected:")
            for _, _, label in all_training_items:
                st.markdown(f"• {label}")
        else:
            st.warning(
                "No training runs configured. Select at least one preprocessing and model."
            )

        st.markdown("---")

        # =================================================================
        # 4. Training Notice and Button
        # =================================================================
        st.info(
            "⏱️ Training may take some time depending on the number of models "
            "and data size. Please wait for all runs to complete."
        )

        run_clicked = st.button(
            "Train Models",
            type="primary",
            use_container_width=True,
            disabled=len(all_training_items) == 0,
        )

        if run_clicked and all_training_items:
            progress_bar = st.progress(0)
            status_text = st.empty()
            trained_count = 0
            failed_models: list[str] = []
            total_runs = len(all_training_items)

            # Cache for loaded preprocessings to avoid reloading
            preprocessing_cache: dict[str, pd.DataFrame] = {}

            for i, (prep_id, model_key, label) in enumerate(all_training_items):
                config = MODEL_REGISTRY[model_key]
                status_text.text(f"Training: {label} ({i + 1}/{total_runs})")

                try:
                    # Load preprocessing data (with caching)
                    if prep_id not in preprocessing_cache:
                        if prep_id == "current" and current_preprocessed_df is not None:
                            preprocessing_cache[prep_id] = current_preprocessed_df
                        elif hostname:
                            endpoint_cache = loader.cache.get_endpoint_cache(hostname)
                            loaded_df = endpoint_cache.load_preprocessing(prep_id)
                            if loaded_df is not None:
                                preprocessing_cache[prep_id] = loaded_df

                    preprocessed_df = preprocessing_cache.get(prep_id)
                    if preprocessed_df is None or len(preprocessed_df) == 0:
                        failed_models.append(
                            f"{label}: Failed to load preprocessing data"
                        )
                        continue

                    # Validate data points
                    total_points = len(preprocessed_df)
                    train_points = int(total_points * train_ratio)
                    test_points = total_points - train_points

                    if train_points < 4:
                        failed_models.append(
                            f"{label}: Not enough training points ({train_points})"
                        )
                        continue
                    if test_points < 1:
                        failed_models.append(
                            f"{label}: Not enough test points ({test_points})"
                        )
                        continue

                    # Split data
                    train_df, test_df = _split_train_test(preprocessed_df, train_ratio)
                    future_df = pd.DataFrame({"ds": test_df["ds"]})

                    # Train model
                    model = config.train_fn(train_df)

                    # Generate predictions
                    forecast = config.predict_fn(model, future_df)

                    # Compute metrics
                    merged = test_df.merge(
                        forecast[["ds", "yhat"]], on="ds", how="inner"
                    )
                    metrics = _compute_metrics(merged["y"], merged["yhat"])

                    # Store result
                    run_id = _generate_run_id(model_key, prep_id)
                    run = TrainingRun(
                        run_id=run_id,
                        model_key=model_key,
                        model_name=config.name,
                        preprocessing_id=prep_id,
                        train_df=train_df,
                        test_df=test_df,
                        forecast=forecast,
                        model=model,
                        metrics=metrics,
                        color=config.color,
                        dash=config.dash,
                        timestamp=datetime.now(),
                        assertion_urn=_get_current_assertion_urn(),
                        is_observe_model=config.is_observe_model,
                        registry_key=config.registry_key,
                    )
                    _store_training_run(run)
                    trained_count += 1

                except Exception as e:
                    failed_models.append(f"{label}: {e}")

                progress_bar.progress((i + 1) / total_runs)

            status_text.empty()
            progress_bar.empty()

            # Store results in session state so they persist across rerun
            st.session_state["last_training_result"] = {
                "trained_count": trained_count,
                "total_count": total_runs,
                "failed_models": failed_models,
            }
            st.rerun()

    # Display training results from last run (persisted across rerun)
    if "last_training_result" in st.session_state:
        result = st.session_state.pop("last_training_result")
        if result["failed_models"]:
            for error_msg in result["failed_models"]:
                st.error(f"Training failed: {error_msg}")
        if result["trained_count"] > 0:
            st.success(
                f"Successfully trained {result['trained_count']} of {result['total_count']} model(s)!"
            )
        elif not result["failed_models"]:
            st.error("All models failed to train.")

    with col_runs:
        # Training runs panel
        _render_training_runs_panel()

        # Navigation to comparison
        runs = _get_training_runs()
        if runs:
            st.markdown("---")
            if st.button(
                "Time Series Comparison →", type="primary", use_container_width=True
            ):
                from . import timeseries_comparison_page

                st.switch_page(timeseries_comparison_page)


__all__ = ["render_model_training_page"]
