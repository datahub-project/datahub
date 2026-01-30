# ruff: noqa: INP001
"""Model Training page for running prediction models on preprocessed data."""

import hashlib
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, MutableMapping, Optional, Sequence, Tuple, cast

import numpy as np
import pandas as pd
import streamlit as st
from prophet import Prophet

try:
    # Type-only warm-state config used by inference_v2.
    # Aliased to avoid collision with this module's `ModelConfig` (UI model registry).
    from datahub_executor.common.monitor.inference_v2.inference_utils import (
        ModelConfig as InferenceV2ModelConfig,
    )
except Exception:
    InferenceV2ModelConfig = None  # type: ignore[assignment,misc]

from ..common import (
    _AUTO_INFERENCE_V2_RUNS_KEY,
    _AUTO_V2_RUN_DETAILS_KEY_PREFIX,
    DataLoader,
    _extract_hash_from_run_id,
    build_input_data_context_from_session,
    clear_all_auto_inference_v2_runs,
    delete_auto_inference_v2_run_complete,
    get_all_pairing_identifiers,
    get_auto_inference_v2_runs,
    get_default_pairing_identifiers,
    get_model_hyperparameters,
    init_explorer_state,
    load_auto_inference_v2_runs,
    parse_pairings_csv,
    run_auto_training_pipeline,
)
from ..common.preprocessing_keys import display_preprocessing_id
from .observe_models_adapter import (
    ObserveModelConfig as ObserveRegistryModelConfig,
    get_model_preprocessing_defaults,
    get_observe_model_configs,
    is_observe_models_available,
)

try:
    from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
        compute_forecast_score,
    )
except ImportError:
    # Fallback if evaluator not available
    compute_forecast_score = None  # type: ignore[assignment]

# =============================================================================
# Model Registry
# =============================================================================


def _clamp_sensitivity_level(value: Any, *, default: int = 5) -> int:
    """
    Clamp a sensitivity value into the supported 1-10 range.

    Streamlit widget values are typically int, but we defensively accept any object
    and fall back to a default if coercion fails.
    """
    try:
        v = int(value)
    except Exception:
        v = int(default)
    return max(1, min(10, v))


def _get_inference_v2_default_sensitivity_level(assertion_type: Optional[str]) -> int:
    """
    Best-effort default sensitivity matching inference_v2 trainer defaults.

    inference_v2 defaults come from `datahub_executor.config` constants and are
    selected by trainer/assertion type. Streamlit does not always have access to
    monitor-level `AssertionAdjustmentSettings`, so this helper focuses on the
    trainer default (and callers may apply explicit overrides on top).
    """
    from datahub_executor.config import (
        FIELD_METRIC_DEFAULT_SENSITIVITY_LEVEL,
        FRESHNESS_DEFAULT_SENSITIVITY_LEVEL,
        SQL_METRIC_DEFAULT_SENSITIVITY_LEVEL,
        VOLUME_DEFAULT_SENSITIVITY_LEVEL,
    )

    t = (assertion_type or "").strip().lower()
    if t == "sql":
        return int(SQL_METRIC_DEFAULT_SENSITIVITY_LEVEL)
    if t == "field":
        return int(FIELD_METRIC_DEFAULT_SENSITIVITY_LEVEL)
    if t == "freshness":
        return int(FRESHNESS_DEFAULT_SENSITIVITY_LEVEL)
    if t == "volume":
        return int(VOLUME_DEFAULT_SENSITIVITY_LEVEL)
    # Other categories (rate/statistic/length) currently do not have distinct
    # trainer-level defaults; keep the common default.
    return 5


_AUTO_V2_SENS_ASSERTION_KEY = "model_training__auto_v2_sensitivity_assertion_urn"
_AUTO_V2_SENS_VALUE_KEY = "model_training__auto_v2_sensitivity"
_AUTO_V2_DEFAULT_SENS_VALUE_KEY = "model_training__auto_v2_sensitivity_default"


def _initialize_auto_v2_sensitivity(
    *,
    session_state: Any,
    assertion_urn: Optional[str],
    assertion_type: Optional[str],
) -> int:
    """
    Initialize inference_v2 sensitivity once per assertion.

    This value is intentionally **independent** from the Training Configuration
    sensitivities (which are per-preprocessing and used by manual training runs).
    """
    default_value = _get_inference_v2_default_sensitivity_level(assertion_type)

    prev_assertion = session_state.get(_AUTO_V2_SENS_ASSERTION_KEY)
    if prev_assertion != assertion_urn:
        session_state[_AUTO_V2_SENS_ASSERTION_KEY] = assertion_urn
        session_state[_AUTO_V2_DEFAULT_SENS_VALUE_KEY] = int(default_value)
        session_state[_AUTO_V2_SENS_VALUE_KEY] = int(default_value)
    else:
        # Always keep the "default sensitivity" synced to inference_v2 defaults
        # for the current assertion type. This must NOT drift when users override.
        if session_state.get(_AUTO_V2_DEFAULT_SENS_VALUE_KEY) != int(default_value):
            session_state[_AUTO_V2_DEFAULT_SENS_VALUE_KEY] = int(default_value)
        session_state.setdefault(_AUTO_V2_SENS_VALUE_KEY, int(default_value))

    return _clamp_sensitivity_level(session_state.get(_AUTO_V2_SENS_VALUE_KEY))


def apply_auto_suggestions_to_training_group(
    *,
    session_state: MutableMapping[str, Any],
    group_key: str,
    suggested_preproc: Optional[str],
    suggested_models: list[str],
    all_models: list[str],
    suggested_sensitivity_level: Optional[int],
) -> None:
    """
    Apply inference_v2 "auto" suggestions to a training group (Streamlit state).

    In addition to preprocessing + models, this can also set the per-preprocessing
    forecast sensitivity so manual training runs match inference_v2 behavior:
    - Use `suggested_sensitivity_level` when provided (explicit override for instance)
    - Otherwise, keep the group's existing sensitivity mapping intact
    """
    group: dict[str, Any] = dict(session_state.get(group_key) or {})

    if suggested_preproc:
        group["preprocessing"] = suggested_preproc
        session_state["model_training__primary_preprocessing"] = suggested_preproc

        if suggested_sensitivity_level is not None:
            stored = group.get("sensitivity_by_preprocessing")
            sensitivity_by_preproc: dict[str, int] = (
                dict(stored) if isinstance(stored, dict) else {}
            )
            sensitivity_by_preproc[suggested_preproc] = _clamp_sensitivity_level(
                suggested_sensitivity_level
            )
            group["sensitivity_by_preprocessing"] = sensitivity_by_preproc

            # Prime the widget state for the suggested preprocessing so the slider
            # reflects the applied sensitivity on the next rerun.
            session_state[
                f"model_training__primary_forecast_sensitivity__{suggested_preproc}"
            ] = sensitivity_by_preproc[suggested_preproc]

    if suggested_models:
        group["models"] = list(suggested_models)
        for mk in all_models:
            session_state[f"model_training__primary_model_cb__{mk}"] = (
                mk in suggested_models
            )

    session_state[group_key] = group


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

# Note: observe-models configs are now loaded dynamically with sensitivity
# See the training page where get_observe_model_configs(sensitivity_level=...) is called


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


def _display_truncation_warning() -> None:
    """Display a warning if preprocessing truncated data due to frequency shift.

    Reads the preprocessing result dict from session state and displays
    a warning with truncation details if data was truncated.
    """
    result_dict = st.session_state.get("_preprocessing_result_dict")
    if result_dict is None:
        return

    if not result_dict.get("was_truncated", False):
        return

    # Extract truncation info from the nested context
    context = result_dict.get("context", {})
    freq_analysis = context.get("frequency_analysis", {})

    if not freq_analysis:
        return

    # Calculate original vs truncated counts
    timeseries_length = result_dict.get("timeseries_length", 0)
    regime_start_idx = freq_analysis.get("regime_start_idx", 0)
    original_count = timeseries_length + regime_start_idx
    truncated_count = regime_start_idx
    pct_removed = (truncated_count / original_count * 100) if original_count > 0 else 0

    # Build warning message with available info
    regime_start = freq_analysis.get("regime_start_timestamp")
    original_start = freq_analysis.get("original_start_timestamp")
    original_end = freq_analysis.get("original_end_timestamp")
    kept_freq = freq_analysis.get("most_recent_class")
    freq_classes = freq_analysis.get("significant_classes", [])

    warning_parts = ["⚠️ **Data was truncated due to frequency shift.**"]

    # Show data point counts
    warning_parts.append(
        f"**{truncated_count}** of **{original_count}** data points removed "
        f"({pct_removed:.1f}%)"
    )

    if regime_start and original_start:
        warning_parts.append(f"Truncated at: `{regime_start}`")
        warning_parts.append(f"Original range: `{original_start}` to `{original_end}`")

    if kept_freq:
        warning_parts.append(f"Keeping frequency: **{kept_freq}**")

    if len(freq_classes) > 1:
        warning_parts.append(f"Detected frequencies: {', '.join(freq_classes)}")

    st.warning("\n\n".join(warning_parts))


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
    score: Optional[float] = None  # Normalized forecast score (0-1, higher is better)
    sensitivity_level: Optional[int] = (
        None  # Sensitivity level (1-10) used for training
    )

    @property
    def display_name(self) -> str:
        """Display name combining model, preprocessing, and hash ID."""
        hash_id = _extract_hash_from_run_id(self.run_id)
        prep_display = display_preprocessing_id(self.preprocessing_id)
        if hash_id:
            return f"{self.model_name} + {prep_display} [{hash_id}]"
        return f"{self.model_name} + {prep_display}"

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


def _load_cached_inference_v2_warm_state(
    *, hostname: Optional[str], assertion_urn: Optional[str]
) -> tuple[Optional["InferenceV2ModelConfig"], Optional[dict[str, Any]]]:
    """
    Load cached inference_v2 warm-start state (ModelConfig) for an assertion.

    Returns:
        Tuple of (existing_model_config, payload_dict)
        - existing_model_config: a `datahub_executor.common.monitor.inference_v2.inference_utils.ModelConfig`
          instance when available and parseable, else None.
        - payload_dict: the dict used for inspection / debugging (best-effort), else None.
    """
    if not hostname or not assertion_urn:
        return None, None

    if InferenceV2ModelConfig is None:
        return None, None

    try:
        loader = DataLoader()
        endpoint_cache = loader.cache.get_endpoint_cache(hostname)
        cached = endpoint_cache.load_inference_data(assertion_urn)
    except Exception:
        return None, None

    if not isinstance(cached, dict):
        return None, None

    model_cfg = cached.get("model_config")
    if not isinstance(model_cfg, dict) or not model_cfg:
        return None, None

    payload: dict[str, Any] = dict(model_cfg)

    # Some cache formats store the JSON blobs outside model_config; coalesce them so
    # the inspector shows what would be passed to inference_v2.
    for key in (
        "preprocessing_config_json",
        "forecast_config_json",
        "anomaly_config_json",
        "forecast_evals_json",
        "anomaly_evals_json",
    ):
        if payload.get(key) in (None, "") and cached.get(key) not in (None, ""):
            payload[key] = cached.get(key)

    if payload.get("generated_at") is None and isinstance(
        cached.get("generated_at"), int
    ):
        payload["generated_at"] = cached.get("generated_at")

    try:
        return InferenceV2ModelConfig(**payload), payload
    except Exception:
        # If the payload can't be parsed into a typed ModelConfig, treat warm state
        # as unavailable for execution (but still show the payload in the inspector).
        return None, payload


def _select_existing_model_config_for_auto_run(
    *,
    run_fresh: bool,
    warm_state_model_config: Optional["InferenceV2ModelConfig"],
) -> Optional["InferenceV2ModelConfig"]:
    """Select the inference_v2 existing_model_config to pass for an Auto run."""
    if run_fresh:
        return None
    return warm_state_model_config


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
                score=run.score,
                sensitivity_level=run.sensitivity_level,
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


def _add_auto_inference_v2_run_to_session(
    *,
    run_id: str,
    auto_run_data: dict[str, Any],
    assertion_urn: str,
) -> None:
    """Add an auto inference_v2 run to session state runs dict.

    This converts the auto run data structure into a TrainingRun object
    and adds it to the session state, making it appear in the runs list.
    """
    # Access session state directly to avoid triggering assertion context checks
    # that might clear the runs dict
    if _TRAINING_RUNS_KEY not in st.session_state:
        st.session_state[_TRAINING_RUNS_KEY] = {}
    runs = st.session_state[_TRAINING_RUNS_KEY]

    meta = auto_run_data.get("metadata") or {}
    train_df = auto_run_data.get("train_df")
    pred_df = auto_run_data.get("prediction_df")
    if not isinstance(train_df, pd.DataFrame) or "ds" not in train_df.columns:
        return
    if not isinstance(pred_df, pd.DataFrame) or len(pred_df) == 0:
        return

    forecast = pred_df.copy()
    if "timestamp_ms" in forecast.columns and "ds" not in forecast.columns:
        forecast["ds"] = pd.to_datetime(forecast["timestamp_ms"], unit="ms")

    # Normalize to common forecast columns used in comparison UI.
    if "yhat" not in forecast.columns:
        if (
            "detection_band_lower" in forecast.columns
            and "detection_band_upper" in forecast.columns
        ):
            forecast["yhat"] = (
                forecast["detection_band_lower"] + forecast["detection_band_upper"]
            ) / 2.0
    if (
        "yhat_lower" not in forecast.columns
        and "detection_band_lower" in forecast.columns
    ):
        forecast["yhat_lower"] = forecast["detection_band_lower"]
    if (
        "yhat_upper" not in forecast.columns
        and "detection_band_upper" in forecast.columns
    ):
        forecast["yhat_upper"] = forecast["detection_band_upper"]

    # Auto runs persist the inference_v2 ModelConfig, which includes forecast_score.
    auto_forecast_score: Optional[float] = None
    model_cfg = auto_run_data.get("model_config")
    if isinstance(model_cfg, dict):
        fs_obj = model_cfg.get("forecast_score")
        if isinstance(fs_obj, (int, float)):
            try:
                fs = float(fs_obj)
                if np.isfinite(fs):
                    auto_forecast_score = fs
            except Exception:
                auto_forecast_score = None

    preprocessing_id = ""
    sensitivity_level = None
    created_at = None
    if isinstance(meta, dict):
        preproc_obj = meta.get("preprocessing_id")
        if isinstance(preproc_obj, str):
            preprocessing_id = preproc_obj

        sens_obj = meta.get("sensitivity_level")
        if isinstance(sens_obj, int):
            sensitivity_level = sens_obj

        created_obj = meta.get("created_at")
        if isinstance(created_obj, str):
            created_at = created_obj

    # Best-effort timestamp parsing
    ts = datetime.now(timezone.utc)
    if isinstance(created_at, str) and created_at:
        try:
            ts = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(timezone.utc)

    # Keep the label short; we show pairing details inside the Auto run details UI.
    model_label = "inference_v2"

    empty_test_df = (
        train_df.head(0)[["ds", "y"]]
        if "y" in train_df.columns
        else train_df.head(0)[["ds"]]
    )

    # Store the full run data in session state for rendering details
    # This avoids needing to reload from cache, which can fail
    st.session_state[f"{_AUTO_V2_RUN_DETAILS_KEY_PREFIX}{run_id}"] = auto_run_data

    # Update any existing in-memory run (e.g. if it was loaded before we started
    # persisting forecast_score) so the comparison table reflects the latest data.
    existing = runs.get(run_id)
    if isinstance(existing, TrainingRun) and existing.model_key == "auto_inference_v2":
        existing.model_name = model_label
        existing.preprocessing_id = preprocessing_id or "current"
        existing.train_df = train_df
        existing.test_df = empty_test_df
        existing.forecast = forecast
        existing.color = "#9467bd"
        existing.dash = "dot"
        existing.timestamp = ts
        existing.assertion_urn = assertion_urn
        existing.score = auto_forecast_score
        existing.sensitivity_level = sensitivity_level
    else:
        runs[run_id] = TrainingRun(
            run_id=run_id,
            model_key="auto_inference_v2",
            model_name=model_label,
            preprocessing_id=preprocessing_id or "current",
            train_df=train_df,
            test_df=empty_test_df,
            forecast=forecast,
            model=None,
            metrics={
                "MAE": float("nan"),
                "RMSE": float("nan"),
                "MAPE": float("nan"),
            },
            color="#9467bd",  # plotly purple
            dash="dot",
            timestamp=ts,
            assertion_urn=assertion_urn,
            is_observe_model=False,
            registry_key=None,
            score=auto_forecast_score,
            sensitivity_level=sensitivity_level,
        )


def _delete_auto_inference_v2_run(
    *,
    session_state: MutableMapping[str, Any],
    hostname: Optional[str],
    run_id: str,
    delete_from_cache: bool = True,
) -> None:
    """Delete a saved inference_v2 run and its derived artifacts.

    Uses centralized deletion function for complete cleanup.
    """
    if not delete_from_cache or not hostname:
        # If not deleting from cache, just remove from session state
        auto_runs = session_state.get(_AUTO_INFERENCE_V2_RUNS_KEY)
        if isinstance(auto_runs, dict):
            auto_runs.pop(run_id, None)
        runs_obj = session_state.get(_TRAINING_RUNS_KEY)
        if isinstance(runs_obj, dict):
            runs_obj.pop(run_id, None)
            runs_obj.pop(f"auto_v2_eval__{run_id}", None)
        session_state.pop(f"{_AUTO_V2_RUN_DETAILS_KEY_PREFIX}{run_id}", None)
        return

    # Use centralized deletion function
    delete_auto_inference_v2_run_complete(
        run_id=run_id,
        hostname=hostname,
        session_state=session_state,
    )


def _clear_auto_inference_v2_runs(
    *,
    session_state: MutableMapping[str, Any],
    hostname: Optional[str],
    assertion_urn: Optional[str],
) -> int:
    """Clear all saved inference_v2 runs for the current assertion.

    Uses centralized clear function for complete cleanup.

    Returns:
        Number of runs deleted.
    """
    if not hostname:
        return 0

    return clear_all_auto_inference_v2_runs(
        hostname=hostname,
        assertion_urn=assertion_urn,
        session_state=session_state,
    )


def _clear_training_runs(clear_cache: bool = False) -> None:
    """Clear all stored training runs from session state.

    Args:
        clear_cache: If True, also clear all runs from disk cache
    """
    runs = _get_training_runs()

    # If clearing cache, delete each run from disk first
    if clear_cache:
        hostname = st.session_state.get("current_hostname")
        if hostname:
            loader = DataLoader()
            endpoint_cache = loader.cache.get_endpoint_cache(hostname)
            # Delete training runs and auto inference_v2 runs separately
            for run_id, run in list(runs.items()):
                if run.model_key == "auto_inference_v2":
                    endpoint_cache.delete_auto_inference_v2_run(run_id)
                else:
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

    # Ensure runs dict exists in session state
    if _TRAINING_RUNS_KEY not in st.session_state:
        st.session_state[_TRAINING_RUNS_KEY] = {}
    runs = st.session_state[_TRAINING_RUNS_KEY]

    # Load each manual run into session state (if not already present)
    for run_meta in cached_runs or []:
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
                    score=run_data.get("score"),  # May be None for older cached runs
                    sensitivity_level=run_data.get(
                        "sensitivity_level"
                    ),  # May be None for older cached runs
                )
                runs[run_id] = run

    # Load auto inference_v2 runs separately (not mixed with training runs)
    # This ensures they're managed independently and don't cause persistence issues
    if hostname:
        load_auto_inference_v2_runs(
            hostname=hostname,
            assertion_urn=assertion_urn,
            session_state=cast(MutableMapping[str, Any], st.session_state),
        )


def _generate_run_id(
    model_key: str, preprocessing_id: str, sensitivity_level: int
) -> str:
    """Generate a unique run ID using deterministic hash.

    Args:
        model_key: Key identifying the model type
        preprocessing_id: ID of the preprocessing used
        sensitivity_level: Sensitivity level (1-10) used for training

    Returns:
        Unique run ID in format: {model_key}__{preprocessing_id}__{hash}
    """
    # Create deterministic hash from distinguishing factors
    hash_input = f"{model_key}{preprocessing_id}{sensitivity_level}"
    hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()
    hash_short = hash_digest[:8]  # Use first 8 characters for brevity
    return f"{model_key}__{preprocessing_id}__{hash_short}"


# =============================================================================
# UI Components
# =============================================================================


def _render_training_runs_panel(*, show_header: bool = True) -> None:
    """Render panel showing all manual and inference_v2 runs with delete buttons.

    Inference_v2 runs are displayed first, then manual runs, in separate sections.
    """
    runs = _get_training_runs()
    auto_runs_raw = get_auto_inference_v2_runs(
        cast(MutableMapping[str, Any], st.session_state)
    )
    # Only show inference_v2 runs for the currently selected assertion
    current_assertion = _get_current_assertion_urn()
    auto_runs = {
        k: v
        for k, v in auto_runs_raw.items()
        if isinstance(v, dict)
        and (v.get("metadata") or {}).get("assertion_urn") == current_assertion
    }

    # Filter out auto runs from training runs (they're stored separately now)
    training_runs_only = {
        k: v for k, v in runs.items() if v.model_key != "auto_inference_v2"
    }

    if not training_runs_only and not auto_runs:
        st.info("No manual runs yet. Select preprocessing and model, then run.")
        return

    if show_header:
        st.subheader("Runs")

    # Header with clear all button
    col_header, col_clear = st.columns([4, 1])
    with col_header:
        training_count = len(training_runs_only)
        auto_count = len(auto_runs)
        if auto_count > 0:
            st.caption(
                f"{training_count} manual run(s), {auto_count} inference_v2 run(s)"
            )
        else:
            st.caption(f"{training_count} manual run(s) in session")
    with col_clear:
        if st.button("Clear All", key="clear_all_runs"):
            _clear_training_runs(clear_cache=True)  # Also delete from disk cache
            # Also clear auto inference_v2 runs
            hostname = st.session_state.get("current_hostname")
            assertion_urn = _get_current_assertion_urn()
            if hostname and assertion_urn:
                _clear_auto_inference_v2_runs(
                    session_state=cast(MutableMapping[str, Any], st.session_state),
                    hostname=hostname,
                    assertion_urn=assertion_urn,
                )
            st.rerun()

    # inference_v2 runs first, then training runs
    if auto_runs:
        st.markdown("### inference_v2 Runs")
        sorted_auto_runs = sorted(
            auto_runs.items(),
            key=lambda x: (
                x[1].get("metadata", {}).get("created_at", "")
                if isinstance(x[1], dict)
                else ""
            ),
            reverse=True,
        )
        for run_id, run_data in sorted_auto_runs:
            if not isinstance(run_data, dict):
                continue

            meta = run_data.get("metadata") or {}
            model_cfg = run_data.get("model_config") or {}

            # Extract score from model config
            auto_forecast_score: Optional[float] = None
            if isinstance(model_cfg, dict):
                fs_obj = model_cfg.get("forecast_score")
                if isinstance(fs_obj, (int, float)):
                    try:
                        fs = float(fs_obj)
                        if np.isfinite(fs):
                            auto_forecast_score = fs
                    except Exception:
                        pass

            preprocessing_id = str(meta.get("preprocessing_id", "current"))
            created_at = meta.get("created_at", "")
            sensitivity_level = meta.get("sensitivity_level")

            col_info, col_metrics, col_time, col_info_btn, col_del = st.columns(
                [3, 2, 1.5, 0.5, 0.5]
            )

            with col_info:
                prep_display = display_preprocessing_id(preprocessing_id)
                hash_id = _extract_hash_from_run_id(run_id)
                if hash_id:
                    st.markdown(f"**inference_v2** + {prep_display} `[{hash_id}]`")
                else:
                    st.markdown(f"**inference_v2** + {prep_display}")

            with col_metrics:
                if auto_forecast_score is not None:
                    from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
                        DEFAULT_FORECAST_SCORE_THRESHOLD,
                    )

                    threshold = DEFAULT_FORECAST_SCORE_THRESHOLD
                    meets_threshold = auto_forecast_score >= threshold
                    threshold_indicator = "✓" if meets_threshold else "⚠"
                    score_text = f"Score: {auto_forecast_score:.3f} {threshold_indicator} (≥{threshold})"
                    st.caption(score_text)
                else:
                    st.caption("Score: N/A")
                if isinstance(sensitivity_level, (int, float)):
                    sens_int = int(sensitivity_level)
                    sens_desc = (
                        "Aggressive"
                        if sens_int >= 7
                        else "Balanced"
                        if sens_int >= 4
                        else "Conservative"
                    )
                    st.caption(f"Sens: {sens_int} ({sens_desc})")

            with col_time:
                if created_at:
                    try:
                        ts = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                        now = datetime.now(timezone.utc)
                        time_ago = now - ts
                        if time_ago.total_seconds() < 60:
                            time_str = "just now"
                        elif time_ago.total_seconds() < 3600:
                            time_str = f"{int(time_ago.total_seconds() // 60)}m ago"
                        elif time_ago.days == 0:
                            time_str = f"Today {ts.strftime('%H:%M')}"
                        elif time_ago.days == 1:
                            time_str = f"Yesterday {ts.strftime('%H:%M')}"
                        else:
                            time_str = ts.strftime("%Y-%m-%d %H:%M")
                        st.caption(time_str)
                    except Exception:
                        st.caption("")
                else:
                    st.caption("")

            with col_info_btn:
                if st.button(
                    "ℹ️",
                    key=f"info_auto_run_{run_id}",
                    help="View model information",
                    type="tertiary",
                ):
                    st.session_state[
                        f"show_params_auto_{run_id}"
                    ] = not st.session_state.get(f"show_params_auto_{run_id}", False)

            with col_del:
                if st.button(
                    "🗑️",
                    key=f"del_auto_run_{run_id}",
                    help="Delete run",
                    type="tertiary",
                ):
                    hostname = st.session_state.get("current_hostname")
                    _delete_auto_inference_v2_run(
                        session_state=cast(MutableMapping[str, Any], st.session_state),
                        hostname=hostname,
                        run_id=run_id,
                        delete_from_cache=True,
                    )
                    st.rerun()

            # Show model information if toggled
            if st.session_state.get(f"show_params_auto_{run_id}", False):
                with st.container():
                    st.markdown("**Model Information**")
                    _render_auto_inference_v2_run_details(run_id=run_id)
                    st.markdown("---")

        if training_runs_only:
            st.markdown("---")

    if training_runs_only:
        st.markdown("### Manual Runs")

    # List training runs sorted by score (best/highest first), then by MAE (best/lowest first)
    sorted_runs = sorted(
        training_runs_only.items(),
        key=lambda x: (
            -float(x[1].score) if x[1].score is not None else float("inf"),
            float(x[1].metrics.get("MAE", float("inf")))
            if not np.isnan(float(x[1].metrics.get("MAE", float("nan"))))
            else float("inf"),
        ),
    )
    for run_id, run in sorted_runs:
        col_info, col_metrics, col_time, col_info_btn, col_del = st.columns(
            [3, 2, 1.5, 0.5, 0.5]
        )

        with col_info:
            hash_id = _extract_hash_from_run_id(run.run_id)
            prep_display = display_preprocessing_id(run.preprocessing_id)
            if hash_id:
                st.markdown(f"**{run.model_name}** + {prep_display} `[{hash_id}]`")
            else:
                st.markdown(f"**{run.model_name}** + {prep_display}")

        with col_metrics:
            # Display score if available, otherwise fall back to MAE
            if run.score is not None:
                from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
                    DEFAULT_FORECAST_SCORE_THRESHOLD,
                )

                threshold = DEFAULT_FORECAST_SCORE_THRESHOLD
                meets_threshold = run.score >= threshold
                threshold_indicator = "✓" if meets_threshold else "⚠"
                score_text = (
                    f"Score: {run.score:.3f} {threshold_indicator} (≥{threshold})"
                )
                st.caption(score_text)
            else:
                mae = float(run.metrics.get("MAE", float("nan")))
                st.caption("MAE: N/A" if np.isnan(mae) else f"MAE: {mae:.2f}")

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
            # Info button to show model information - tertiary type for no border
            if st.button(
                "ℹ️",
                key=f"info_run_{run_id}",
                help="View model information",
                type="tertiary",
            ):
                st.session_state[f"show_params_{run_id}"] = not st.session_state.get(
                    f"show_params_{run_id}", False
                )

        with col_del:
            if st.button(
                "🗑️",
                key=f"del_run_{run_id}",
                help="Delete run",
                type="tertiary",
            ):
                # Handle deletion for both training runs and auto inference_v2 runs
                if run.model_key == "auto_inference_v2":
                    hostname = st.session_state.get("current_hostname")
                    _delete_auto_inference_v2_run(
                        session_state=cast(MutableMapping[str, Any], st.session_state),
                        hostname=hostname,
                        run_id=run_id,
                        delete_from_cache=True,
                    )
                else:
                    _delete_training_run(run_id)
                st.rerun()

        # Show model information if toggled
        if st.session_state.get(f"show_params_{run_id}", False):
            with st.container():
                st.markdown("**Model Information**")

                # Training runs only (auto runs handled separately)
                if run.model_key != "auto_inference_v2":
                    # Standard training run details
                    # Model Hyperparameters
                    params = get_model_hyperparameters(run.model)
                    if params and params.get("note") != "No hyperparameters available":
                        st.markdown("**Hyperparameters:**")
                        param_lines = []
                        for key, value in sorted(params.items()):
                            if isinstance(value, dict):
                                param_lines.append(f"- **{key}**: `{value}`")
                            elif isinstance(value, float):
                                param_lines.append(f"- **{key}**: `{value:.6g}`")
                            else:
                                param_lines.append(f"- **{key}**: `{value}`")
                        st.markdown("\n".join(param_lines))
                        st.markdown("")

                    # Manual Configuration
                    st.markdown("**Manual Configuration:**")
                    config_lines = []
                    if run.sensitivity_level is not None:
                        sensitivity_desc = (
                            "Aggressive"
                            if run.sensitivity_level >= 7
                            else "Balanced"
                            if run.sensitivity_level >= 4
                            else "Conservative"
                        )
                        config_lines.append(
                            f"- **Sensitivity Level**: {run.sensitivity_level} ({sensitivity_desc})"
                        )
                    config_lines.append(
                        f"- **Preprocessing ID**: `{display_preprocessing_id(run.preprocessing_id)}`"
                    )
                    st.markdown("\n".join(config_lines))
                    st.markdown("")

                    # Model Metadata
                    st.markdown("**Model Metadata:**")
                    metadata_lines = []
                    if run.is_observe_model:
                        metadata_lines.append("- **Model Type**: Observe Model")
                        if run.registry_key:
                            metadata_lines.append(
                                f"- **Registry Key**: `{run.registry_key}`"
                            )
                    else:
                        metadata_lines.append("- **Model Type**: Standard")
                    metadata_lines.append(f"- **Model Key**: `{run.model_key}`")
                    st.markdown("\n".join(metadata_lines))
                st.markdown("---")


def _render_auto_inference_v2_run_details(
    *, run_id: str, run: Optional[TrainingRun] = None
) -> None:
    """Render detailed information for an auto inference_v2 run in the info panel."""
    # Get run data from separate auto runs session state
    auto_runs = get_auto_inference_v2_runs(
        cast(MutableMapping[str, Any], st.session_state)
    )
    run_data = auto_runs.get(run_id)

    # Fall back to detail state key for backward compatibility
    if not isinstance(run_data, dict):
        run_data = st.session_state.get(f"{_AUTO_V2_RUN_DETAILS_KEY_PREFIX}{run_id}")

    # Fall back to cache if not in session state
    endpoint_cache = None
    if not isinstance(run_data, dict):
        hostname = st.session_state.get("current_hostname")
        if not hostname:
            st.caption("Hostname not available.")
            return

        loader = DataLoader()
        endpoint_cache = loader.cache.get_endpoint_cache(hostname)
        run_data = endpoint_cache.load_auto_inference_v2_run(run_id)
        if not isinstance(run_data, dict):
            st.caption("Failed to load inference_v2 run data.")
            return
    else:
        # We have run_data from session state, but we still need endpoint_cache for eval run
        hostname = st.session_state.get("current_hostname")
        if hostname:
            loader = DataLoader()
            endpoint_cache = loader.cache.get_endpoint_cache(hostname)

    detail_meta: Any = run_data.get("metadata") or {}
    model_cfg: Any = run_data.get("model_config") or {}

    if isinstance(detail_meta, dict):
        warm_source = detail_meta.get("warm_start_source")
        if warm_source:
            st.caption(f"Warm start source: `{warm_source}`")
        timing = detail_meta.get("execution_timing")
        if isinstance(timing, dict) and "total_seconds" in timing:
            total_sec = timing["total_seconds"]
            if isinstance(total_sec, (int, float)) and total_sec >= 0:
                st.caption(f"Execution time: **{total_sec:.1f}** s")

    def _format_model_key(name: Optional[object], version: Optional[object]) -> str:
        n = str(name).strip() if name is not None else ""
        v = str(version).strip() if version is not None else ""
        if not n:
            return ""
        if v:
            return f"{n}@{v}"
        return n

    tab_selected, tab_alt = st.tabs(["Selected (winner)", "Alternatives"])

    with tab_selected:
        st.markdown("**Winning Model Selection (inference_v2)**")

        forecast_key = ""
        anomaly_key = ""
        forecast_score = None
        anomaly_score = None
        if isinstance(model_cfg, dict):
            forecast_key = _format_model_key(
                model_cfg.get("forecast_model_name"),
                model_cfg.get("forecast_model_version"),
            )
            anomaly_key = _format_model_key(
                model_cfg.get("anomaly_model_name"),
                model_cfg.get("anomaly_model_version"),
            )
            forecast_score = model_cfg.get("forecast_score")
            anomaly_score = model_cfg.get("anomaly_score")

        if forecast_key:
            st.caption(f"Forecast: `{forecast_key}`")
        if anomaly_key:
            st.caption(f"Anomaly: `{anomaly_key}`")
        if forecast_key and anomaly_key:
            st.caption(f"Pairing: `{forecast_key}_{anomaly_key}`")

        score_bits: list[str] = []
        if forecast_score is not None:
            score_bits.append(f"forecast_score={forecast_score}")
        if anomaly_score is not None:
            score_bits.append(f"anomaly_score={anomaly_score}")
        if score_bits:
            st.caption("Scores: " + " · ".join(f"`{b}`" for b in score_bits))
        if not forecast_key and not anomaly_key:
            st.caption("Model config unavailable for this run.")

        st.markdown("**Strict Holdout Evaluation (forecast only)**")
        eval_run_id = f"auto_v2_eval__{run_id}"
        eval_run_data = None
        if endpoint_cache:
            eval_run_data = endpoint_cache.load_training_run(eval_run_id)
        if isinstance(eval_run_data, dict):
            metrics = eval_run_data.get("metrics") or {}
            mae = metrics.get("MAE")
            rmse = metrics.get("RMSE")
            mape = metrics.get("MAPE")
            col_mae, col_rmse, col_mape = st.columns(3)
            with col_mae:
                st.metric("MAE", "N/A" if mae is None else f"{float(mae):.4g}")
            with col_rmse:
                st.metric("RMSE", "N/A" if rmse is None else f"{float(rmse):.4g}")
            with col_mape:
                st.metric("MAPE", "N/A" if mape is None else f"{float(mape):.4g}")
        else:
            st.caption("No saved strict-holdout eval run found for this Auto run.")

        with st.expander("ModelConfig + configs (raw)", expanded=False):
            st.json(
                model_cfg
                if isinstance(model_cfg, dict)
                else {"model_config": model_cfg}
            )

    with tab_alt:
        # Best-effort: show per-pairing evaluation results if they were persisted.
        alt_results = None
        pairings_used = None
        if isinstance(detail_meta, dict):
            alt_results = detail_meta.get("combination_results")
            if alt_results is None:
                alt_results = detail_meta.get("pairing_results")
            pairings_used = detail_meta.get("pairings_used")

        if isinstance(alt_results, list) and alt_results:
            rows: list[dict[str, object]] = []
            for item in alt_results:
                if isinstance(item, dict):
                    rows.append(dict(item))
            if rows:
                df = pd.DataFrame(rows)
                preferred = [
                    "combination_name",
                    "combined_score",
                    "forecast_model_key",
                    "anomaly_model_key",
                    "forecast_score",
                    "anomaly_score",
                    "success",
                    "errors",
                ]
                cols = [c for c in preferred if c in df.columns] + [
                    c for c in df.columns if c not in preferred
                ]
                st.dataframe(df[cols], use_container_width=True, hide_index=True)
                st.caption(
                    "These are the evaluated pairings; the selected winner is shown in the other tab."
                )
            else:
                st.caption("No parseable alternative pairing results found.")
        elif isinstance(pairings_used, list) and pairings_used:
            st.markdown("**Pairings evaluated**")
            st.markdown("\n".join([f"- `{str(p)}`" for p in pairings_used]))
            st.caption(
                "This run did not persist per-pairing scores/rankings; only the winner is stored."
            )
        else:
            st.caption("No alternative pairing information is available for this run.")


def _render_auto_inference_v2_runs_panel(
    *, hostname: Optional[str], assertion_urn: Optional[str], show_header: bool = True
) -> None:
    """Render saved inference_v2 runs (from disk cache) with delete controls.

    DEPRECATED: This function is kept for backwards compatibility but is no longer
    used in the main UI. Auto runs are now shown in the consolidated runs list.
    """
    if show_header:
        st.subheader("inference_v2 Runs")

    if not hostname:
        st.caption("Select an endpoint to view saved inference_v2 runs.")
        return

    # Load auto runs into session state using centralized function
    load_auto_inference_v2_runs(
        hostname=hostname,
        assertion_urn=assertion_urn,
        session_state=cast(MutableMapping[str, Any], st.session_state),
    )

    auto_runs_dict = get_auto_inference_v2_runs(
        cast(MutableMapping[str, Any], st.session_state)
    )

    if not auto_runs_dict:
        st.info("No saved inference_v2 runs found for this assertion.")
        return

    col_header, col_clear = st.columns([4, 1])
    with col_header:
        st.caption(f"{len(auto_runs_dict)} run(s) saved")
    with col_clear:
        if st.button(
            "Clear All",
            key="model_training__clear_all_auto_v2_runs",
            help="Delete all saved inference_v2 runs for this assertion",
        ):
            _ = _clear_auto_inference_v2_runs(
                session_state=cast(MutableMapping[str, Any], st.session_state),
                hostname=hostname,
                assertion_urn=assertion_urn,
            )
            st.rerun()

    loader = DataLoader()
    endpoint_cache = loader.cache.get_endpoint_cache(hostname)

    for run_id, run_data in auto_runs_dict.items():
        if not isinstance(run_data, dict):
            continue

        meta = run_data.get("metadata") or {}
        created_at = str(meta.get("created_at") or "")
        created_short = created_at[:16] if created_at else ""
        preproc_id = str(meta.get("preprocessing_id") or "")
        sensitivity_level = meta.get("sensitivity_level")
        warm_used = meta.get("warm_start_used")
        pairings_used = meta.get("pairings_used")

        pairings_str = ""
        if isinstance(pairings_used, list) and pairings_used:
            pairings_str = ", ".join(str(p) for p in pairings_used if p is not None)

        title_bits: list[str] = [created_short or "Saved run"]
        if preproc_id:
            title_bits.append(display_preprocessing_id(preproc_id))
        expander_title = "  ·  ".join(title_bits)

        # Each run entry should be collapsed by default.
        with st.expander(expander_title, expanded=False):
            # Quick metadata + delete control inside the entry.
            top_left, top_right = st.columns([4, 1])
            with top_left:
                if pairings_str:
                    st.caption(f"Pairings: {pairings_str}")

                meta_lines: list[str] = []
                if isinstance(sensitivity_level, int):
                    meta_lines.append(f"Sens: `{sensitivity_level}`")
                if warm_used is not None:
                    meta_lines.append(f"Warm: `{bool(warm_used)}`")
                if meta_lines:
                    st.caption("  ·  ".join(meta_lines))

            with top_right:
                if st.button(
                    "Delete",
                    key=f"model_training__auto_v2_delete__{run_id}",
                    help="Delete saved inference_v2 run",
                    type="secondary",
                ):
                    _delete_auto_inference_v2_run(
                        session_state=cast(MutableMapping[str, Any], st.session_state),
                        hostname=hostname,
                        run_id=run_id,
                        delete_from_cache=True,
                    )
                    st.rerun()

            # Run data is already loaded from session state
            detail_meta: Any = meta
            model_cfg: Any = run_data.get("model_config") or {}

            if isinstance(detail_meta, dict):
                warm_source = detail_meta.get("warm_start_source")
                if warm_source:
                    st.caption(f"Warm start source: `{warm_source}`")

            def _format_model_key(
                name: Optional[object], version: Optional[object]
            ) -> str:
                n = str(name).strip() if name is not None else ""
                v = str(version).strip() if version is not None else ""
                if not n:
                    return ""
                if v:
                    return f"{n}@{v}"
                return n

            tab_selected, tab_alt = st.tabs(["Selected (winner)", "Alternatives"])

            with tab_selected:
                st.markdown("**Winning Model Selection (inference_v2)**")

                forecast_key = ""
                anomaly_key = ""
                forecast_score = None
                anomaly_score = None
                if isinstance(model_cfg, dict):
                    forecast_key = _format_model_key(
                        model_cfg.get("forecast_model_name"),
                        model_cfg.get("forecast_model_version"),
                    )
                    anomaly_key = _format_model_key(
                        model_cfg.get("anomaly_model_name"),
                        model_cfg.get("anomaly_model_version"),
                    )
                    forecast_score = model_cfg.get("forecast_score")
                    anomaly_score = model_cfg.get("anomaly_score")

                if forecast_key:
                    st.caption(f"Forecast: `{forecast_key}`")
                if anomaly_key:
                    st.caption(f"Anomaly: `{anomaly_key}`")
                if forecast_key and anomaly_key:
                    st.caption(f"Pairing: `{forecast_key}_{anomaly_key}`")

                score_bits: list[str] = []
                if forecast_score is not None:
                    score_bits.append(f"forecast_score={forecast_score}")
                if anomaly_score is not None:
                    score_bits.append(f"anomaly_score={anomaly_score}")
                if score_bits:
                    st.caption("Scores: " + " · ".join(f"`{b}`" for b in score_bits))
                if not forecast_key and not anomaly_key:
                    st.caption("Model config unavailable for this run.")

                st.markdown("**Strict Holdout Evaluation (forecast only)**")
                eval_run_id = f"auto_v2_eval__{run_id}"
                eval_run_data = endpoint_cache.load_training_run(eval_run_id)
                if isinstance(eval_run_data, dict):
                    metrics = eval_run_data.get("metrics") or {}
                    mae = metrics.get("MAE")
                    rmse = metrics.get("RMSE")
                    mape = metrics.get("MAPE")
                    col_mae, col_rmse, col_mape = st.columns(3)
                    with col_mae:
                        st.metric("MAE", "N/A" if mae is None else f"{float(mae):.4g}")
                    with col_rmse:
                        st.metric(
                            "RMSE", "N/A" if rmse is None else f"{float(rmse):.4g}"
                        )
                    with col_mape:
                        st.metric(
                            "MAPE", "N/A" if mape is None else f"{float(mape):.4g}"
                        )
                else:
                    st.caption(
                        "No saved strict-holdout eval run found for this Auto run."
                    )

                with st.expander("ModelConfig + configs (raw)", expanded=False):
                    st.json(
                        model_cfg
                        if isinstance(model_cfg, dict)
                        else {"model_config": model_cfg}
                    )

            with tab_alt:
                # Best-effort: show per-pairing evaluation results if they were persisted.
                # Some runs may only store the evaluated pairing names (pairings_used).
                alt_results = None
                pairings_used = None
                if isinstance(detail_meta, dict):
                    alt_results = detail_meta.get("combination_results")
                    if alt_results is None:
                        alt_results = detail_meta.get("pairing_results")
                    pairings_used = detail_meta.get("pairings_used")

                if isinstance(alt_results, list) and alt_results:
                    rows: list[dict[str, object]] = []
                    for item in alt_results:
                        if isinstance(item, dict):
                            rows.append(dict(item))
                    if rows:
                        df = pd.DataFrame(rows)
                        # Try to present core columns first when present.
                        preferred = [
                            "combination_name",
                            "combined_score",
                            "forecast_model_key",
                            "anomaly_model_key",
                            "forecast_score",
                            "anomaly_score",
                            "success",
                            "errors",
                        ]
                        cols = [c for c in preferred if c in df.columns] + [
                            c for c in df.columns if c not in preferred
                        ]
                        st.dataframe(
                            df[cols], use_container_width=True, hide_index=True
                        )
                        st.caption(
                            "These are the evaluated pairings; the selected winner is shown in the other tab."
                        )
                    else:
                        st.caption("No parseable alternative pairing results found.")
                elif isinstance(pairings_used, list) and pairings_used:
                    st.markdown("**Pairings evaluated**")
                    st.markdown("\n".join([f"- `{str(p)}`" for p in pairings_used]))
                    st.caption(
                        "This run did not persist per-pairing scores/rankings; only the winner is stored."
                    )
                else:
                    st.caption(
                        "No alternative pairing information is available for this run."
                    )


def _render_auto_inference_v2_pipeline_tab(
    *,
    loader: "DataLoader",
    hostname: Optional[str],
    assertion_urn: Optional[str],
    preprocessing_options: list[str],
    preprocessing_labels: dict[str, str],
    train_ratio: float,
    selected_preprocessing: str,
) -> None:
    """Render the inference_v2 pipeline UI (run + inspection)."""
    if not preprocessing_options:
        st.warning("No preprocessing options available.")
        return

    if _PRIMARY_TRAINING_GROUP_KEY not in st.session_state:
        st.session_state[_PRIMARY_TRAINING_GROUP_KEY] = {
            "preprocessing": preprocessing_options[0],
            "models": [],
            "sensitivity_by_preprocessing": {},
        }

    primary_group_preprocessing_default = str(selected_preprocessing or "")
    if (
        preprocessing_options
        and primary_group_preprocessing_default not in preprocessing_options
    ):
        primary_group_preprocessing_default = preprocessing_options[0]
    # Keep the primary group in sync with the shared preprocessing selector.
    st.session_state[_PRIMARY_TRAINING_GROUP_KEY]["preprocessing"] = (
        primary_group_preprocessing_default
    )

    _ = _initialize_auto_v2_sensitivity(
        session_state=st.session_state,
        assertion_urn=assertion_urn,
        assertion_type=st.session_state.get("current_assertion_type")
        if isinstance(st.session_state.get("current_assertion_type"), str)
        else None,
    )
    auto_default_sensitivity = _clamp_sensitivity_level(
        st.session_state.get(
            _AUTO_V2_DEFAULT_SENS_VALUE_KEY,
            _get_inference_v2_default_sensitivity_level(
                st.session_state.get("current_assertion_type")
                if isinstance(st.session_state.get("current_assertion_type"), str)
                else None
            ),
        )
    )

    st.caption(
        "Runs the full inference_v2 training pipeline (defaults/tuning/pairing evaluation) "
        "to produce detection bands and a persisted ModelConfig for comparison."
    )

    st.caption(
        f"Preprocessing: {preprocessing_labels.get(primary_group_preprocessing_default, primary_group_preprocessing_default)}"
    )
    s_col_default, s_col_input = st.columns([1, 2])
    with s_col_default:
        st.metric(
            "Default sensitivity",
            int(auto_default_sensitivity),
            help=(
                "Derived from inference_v2 trainer defaults for this assertion type. "
                "This is independent from the Manual Configuration sensitivities."
            ),
        )
        st.progress((int(auto_default_sensitivity) - 1) / 9)
        st.caption("1 = conservative • 10 = aggressive")

    with s_col_input:
        auto_sensitivity = st.number_input(
            "Sensitivity (override for this Auto run)",
            min_value=1,
            max_value=10,
            step=1,
            key=_AUTO_V2_SENS_VALUE_KEY,
            help=(
                "Defaults to inference_v2 trainer defaults for this assertion type, "
                "but can be overridden for this Auto run."
            ),
        )
        delta = int(auto_sensitivity) - int(auto_default_sensitivity)
        st.caption(
            "Using default sensitivity."
            if delta == 0
            else f"Overriding default by {delta:+d}."
        )

    warm_state_model_config, warm_state_payload = _load_cached_inference_v2_warm_state(
        hostname=hostname, assertion_urn=assertion_urn
    )
    run_fresh = st.checkbox(
        "Run fresh (ignore warm state)",
        value=False,
        key="model_training__auto_v2_run_fresh",
        help=(
            "When unchecked, Auto will reuse the cached inference ModelConfig for this assertion "
            "(warm start) when available."
        ),
    )

    if isinstance(warm_state_payload, dict) and warm_state_payload:
        generated_at = warm_state_payload.get("generated_at")
        forecast_name = warm_state_payload.get("forecast_model_name") or "(none)"
        forecast_ver = warm_state_payload.get("forecast_model_version") or ""
        anomaly_name = warm_state_payload.get("anomaly_model_name") or "(none)"
        anomaly_ver = warm_state_payload.get("anomaly_model_version") or ""

        summary_lines = [
            f"- Forecast: `{forecast_name}{('@' + str(forecast_ver)) if forecast_ver else ''}`",
            f"- Anomaly: `{anomaly_name}{('@' + str(anomaly_ver)) if anomaly_ver else ''}`",
            f"- generated_at: `{generated_at}`"
            if generated_at
            else "- generated_at: (unknown)",
        ]

        if run_fresh:
            st.info("Warm state: available but ignored (fresh run).")
        elif warm_state_model_config is None:
            st.warning(
                "Warm state: available in cache but could not be parsed; running fresh."
            )
        else:
            st.success("Warm state: using cached inference ModelConfig.")

        with st.expander("Warm state (ModelConfig) inspector", expanded=False):
            st.markdown("\n".join(summary_lines))
            st.markdown("**Raw payload**")
            st.json(warm_state_payload)
    else:
        st.caption("Warm state: none available (fresh run).")

    auto_interval_hours = 1
    from datahub_executor.common.monitor.inference_v2.inference_utils import (
        get_default_prediction_num_intervals,
    )

    auto_num_intervals = get_default_prediction_num_intervals(
        interval_hours=int(auto_interval_hours)
    )

    with st.expander("Model pairings override (optional)", expanded=False):
        override_pairings = st.checkbox(
            "Override pairings evaluated by inference_v2",
            value=False,
            key="model_training__auto_v2_override_pairings",
        )
        pairings_input_mode = st.radio(
            "Pairings source",
            options=["select", "text"],
            index=0,
            horizontal=True,
            key="model_training__auto_v2_pairings_mode",
            disabled=not override_pairings,
        )

        selected_pairings: list[str] = []
        pairings_text: str = ""
        if pairings_input_mode == "select":
            all_pairings = get_all_pairing_identifiers()
            default_pairings = get_default_pairing_identifiers()
            selected_pairings = st.multiselect(
                "Pairings to evaluate",
                options=all_pairings,
                default=default_pairings,
                key="model_training__auto_v2_pairings_select",
                disabled=not override_pairings,
                help="Pairing identifiers follow inference_v2 naming (forecast_anomaly).",
            )
        else:
            pairings_text = st.text_input(
                "Pairings (CSV)",
                value=",".join(get_default_pairing_identifiers()),
                key="model_training__auto_v2_pairings_text",
                disabled=not override_pairings,
                help="Comma-separated identifiers (e.g., prophet_adaptive_band,datahub_datahub_forecast_anomaly).",
            )

    with st.expander("Advanced: evaluation & grids", expanded=False):
        env_pairings = os.environ.get("DATAHUB_EXECUTOR_MODEL_PAIRINGS")
        st.markdown("**Pairings configuration**")
        st.caption(
            "inference_v2 evaluates one or more forecast+anomaly pairings and selects the best."
        )
        if env_pairings:
            st.markdown(f"- **DATAHUB_EXECUTOR_MODEL_PAIRINGS**: `{env_pairings}`")
        else:
            st.markdown(
                "- **DATAHUB_EXECUTOR_MODEL_PAIRINGS**: (not set; using defaults)"
            )

        from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
            DEFAULT_ANOMALY_SCORE_THRESHOLD,
            DEFAULT_DROP_THRESHOLD,
            DEFAULT_FORECAST_SCORE_THRESHOLD,
            DEFAULT_MIN_SAMPLES_FOR_CV,
        )

        st.markdown("**Evaluation / retune policy**")
        st.markdown(
            "\n".join(
                [
                    f"- **Forecast score threshold**: `{DEFAULT_FORECAST_SCORE_THRESHOLD}` "
                    "(env: `DATAHUB_EXECUTOR_FORECAST_SCORE_THRESHOLD`)",
                    f"- **Anomaly score threshold**: `{DEFAULT_ANOMALY_SCORE_THRESHOLD}` "
                    "(env: `DATAHUB_EXECUTOR_ANOMALY_SCORE_THRESHOLD`)",
                    f"- **Score drop threshold**: `{DEFAULT_DROP_THRESHOLD}` "
                    "(env: `DATAHUB_EXECUTOR_SCORE_DROP_THRESHOLD`)",
                    f"- **Min samples for CV**: `{DEFAULT_MIN_SAMPLES_FOR_CV}` "
                    "(fallback when model does not specify `min_samples_for_cv`)",
                ]
            )
        )

        st.markdown("**Grid search / CV split**")
        st.caption(
            "Below is a best-effort inspection of the actual `param_grid` exposed by the "
            "anomaly model(s) for the effective pairings. This is computed by instantiating "
            "the model classes (no training)."
        )

        effective_pairings = None
        if override_pairings:
            try:
                effective_pairings = (
                    parse_pairings_csv(",".join(selected_pairings))
                    if pairings_input_mode == "select"
                    else parse_pairings_csv(pairings_text)
                )
            except Exception as e:
                st.warning(f"Could not parse pairing override: {e}")
                effective_pairings = None
        else:
            try:
                from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
                    get_pairings_from_env_or_default,
                )

                effective_pairings = get_pairings_from_env_or_default()
            except Exception as e:
                st.warning(f"Could not resolve pairings from env/default: {e}")
                effective_pairings = None

        if effective_pairings:
            st.markdown(
                "- **Effective pairings**: "
                + ", ".join(f"`{p.name}`" for p in effective_pairings)
            )
        else:
            st.markdown("- **Effective pairings**: (unknown)")

        if not is_observe_models_available():
            st.error("observe-models is required to inspect model grids.")
        elif not effective_pairings:
            st.warning("No effective pairings available to inspect.")
        else:
            with st.spinner("Computing train-faithful grids..."):
                try:
                    from datahub_observe.registry import (
                        get_model_registry,  # type: ignore[import-untyped]
                    )

                    from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
                        get_defaults_for_context,
                    )

                    ctx_for_inspection = build_input_data_context_from_session(
                        sensitivity_level=int(auto_sensitivity)
                    )
                    defaults = get_defaults_for_context(ctx_for_inspection)
                    preprocessing_cfg = defaults.preprocessing_config()
                    forecast_cfg = defaults.forecast_config()
                    anomaly_cfg_base = defaults.anomaly_config()

                    registry = get_model_registry()

                    # Load the exact train_data we would feed to inference_v2 for this
                    # preprocessing choice so train-faithful grid APIs can apply any
                    # data-dependent gating.
                    auto_prep_id = (
                        "current"
                        if primary_group_preprocessing_default == "__current__"
                        else primary_group_preprocessing_default
                    )
                    train_data_for_grid: Optional[pd.DataFrame] = None
                    if auto_prep_id == "current":
                        train_data_for_grid = st.session_state.get("preprocessed_df")
                    elif hostname:
                        endpoint_cache = loader.cache.get_endpoint_cache(hostname)
                        train_data_for_grid = endpoint_cache.load_preprocessing(
                            auto_prep_id
                        )

                    if isinstance(train_data_for_grid, pd.DataFrame):
                        train_data_for_grid = train_data_for_grid.dropna(
                            subset=["y"]
                        ).copy()
                    else:
                        train_data_for_grid = None

                    for p in effective_pairings:
                        with st.expander(f"Pairing `{p.name}`", expanded=False):
                            st.markdown(
                                f"- **Forecast model**: `{p.forecast_model_key}`"
                            )
                            st.markdown(f"- **Anomaly model**: `{p.anomaly_model_key}`")

                            anomaly_cfg = anomaly_cfg_base
                            if p.forecast_model_key:
                                try:
                                    anomaly_cfg.forecast_model_config = forecast_cfg
                                except Exception:
                                    try:
                                        from datahub_observe.algorithms.anomaly_detection.config import (  # type: ignore[import-untyped]
                                            AnomalyModelConfig,
                                        )

                                        data = anomaly_cfg.model_dump()  # type: ignore[attr-defined]
                                        data["forecast_model_config"] = forecast_cfg
                                        anomaly_cfg = AnomalyModelConfig(**data)
                                    except Exception:
                                        pass

                            def _grid_size(grid: object) -> Optional[int]:
                                """Best-effort cartesian product size for a param_grid dict."""
                                if not isinstance(grid, dict):
                                    return None
                                total = 1
                                for v in grid.values():
                                    if isinstance(v, list):
                                        total *= len(v)
                                    else:
                                        # Non-list values are treated as a single option.
                                        total *= 1
                                return int(total)

                            forecast_param_grid = None
                            anomaly_param_grid = None

                            # Use observe-models train-faithful grid introspection.
                            # Forecast grids may be data-dependent (train_data required).
                            try:
                                if p.forecast_model_key:
                                    forecast_param_grid = (
                                        registry.get_tuning_param_grid(  # type: ignore[attr-defined]
                                            p.forecast_model_key,
                                            preprocessing_cfg,
                                            forecast_cfg,
                                            train_data=train_data_for_grid,
                                        )
                                    )
                            except Exception as e:
                                st.caption(f"Forecast grid unavailable: {e}")
                                forecast_param_grid = None

                            try:
                                anomaly_param_grid = registry.get_tuning_param_grid(  # type: ignore[attr-defined]
                                    p.anomaly_model_key,
                                    preprocessing_cfg,
                                    anomaly_cfg,
                                    train_data=None,
                                )
                            except Exception as e:
                                st.caption(f"Anomaly grid unavailable: {e}")
                                anomaly_param_grid = None

                            # These are anomaly-model attributes; for grid-only inspection we
                            # don't instantiate the model. Keep display aligned with inference_v2.
                            cv_split_ratio = None
                            min_samples_for_cv = None

                            st.markdown(
                                f"- **Grid search enabled**: `{bool(anomaly_param_grid)}` "
                                "(enabled when `param_grid` is non-empty)"
                            )
                            st.markdown(
                                f"- **cv_split_ratio**: `{cv_split_ratio if cv_split_ratio is not None else 0.7}`"
                            )
                            if min_samples_for_cv is not None:
                                st.markdown(
                                    f"- **min_samples_for_cv**: `{min_samples_for_cv}`"
                                )
                            else:
                                st.markdown(
                                    f"- **min_samples_for_cv**: (model default; fallback `{DEFAULT_MIN_SAMPLES_FOR_CV}`)"
                                )

                            if p.forecast_model_key:
                                st.markdown("**forecast param_grid**")
                                size = _grid_size(forecast_param_grid)
                                if size is not None:
                                    st.caption(f"Cartesian configs: **{size}**")
                                if isinstance(forecast_param_grid, dict):
                                    st.json(forecast_param_grid)
                                elif forecast_param_grid is None:
                                    st.caption("(none)")
                                else:
                                    st.code(repr(forecast_param_grid), language=None)

                            st.markdown("**anomaly param_grid**")
                            a_size = _grid_size(anomaly_param_grid)
                            if a_size is not None:
                                st.caption(f"Cartesian configs: **{a_size}**")
                            if isinstance(anomaly_param_grid, dict):
                                st.json(anomaly_param_grid)
                            elif anomaly_param_grid is None:
                                st.caption("(none)")
                            else:
                                st.code(repr(anomaly_param_grid), language=None)
                except Exception as e:
                    st.error(f"Grid inspection failed: {e}")

    run_auto_clicked = st.button(
        "Run inference_v2 pipeline",
        type="primary",
        use_container_width=True,
        key="model_training__run_auto_v2",
    )

    if run_auto_clicked:
        if not is_observe_models_available():
            st.error("observe-models is required to run the inference_v2 pipeline.")
            return

        auto_prep_id = (
            "current"
            if primary_group_preprocessing_default == "__current__"
            else primary_group_preprocessing_default
        )
        preprocessed_df: Optional[pd.DataFrame] = None
        if auto_prep_id == "current":
            preprocessed_df = st.session_state.get("preprocessed_df")
        elif hostname:
            endpoint_cache = loader.cache.get_endpoint_cache(hostname)
            preprocessed_df = endpoint_cache.load_preprocessing(auto_prep_id)

        if preprocessed_df is None or len(preprocessed_df) == 0:
            st.error("Failed to load preprocessing data for Auto run.")
            return

        df_for_auto = preprocessed_df.dropna(subset=["y"]).copy()
        ctx = build_input_data_context_from_session(
            sensitivity_level=int(auto_sensitivity)
        )

        existing_model_config = _select_existing_model_config_for_auto_run(
            run_fresh=bool(run_fresh),
            warm_state_model_config=warm_state_model_config,
        )

        model_pairings = None
        if override_pairings:
            try:
                model_pairings = (
                    parse_pairings_csv(",".join(selected_pairings))
                    if pairings_input_mode == "select"
                    else parse_pairings_csv(pairings_text)
                )
            except Exception as e:
                st.error(f"Invalid pairing override: {e}")
                model_pairings = None

        progress_text = st.empty()
        progress_bar = st.progress(0)

        def _auto_v2_progress_hook(
            message: str,
            progress: Optional[float],
            step_name: Optional[str] = None,
            step_result: Optional[object] = None,
        ) -> None:
            progress_text.markdown(f"**Running inference_v2**: {message}")
            if progress is not None:
                pct = int(max(0.0, min(1.0, float(progress))) * 100)
                progress_bar.progress(pct)

        progress_text.markdown("**Running inference_v2**: starting…")
        try:
            # Keep the split consistent with inference_v2 evaluation semantics (strict holdout).
            # This is used only for persisting a comparable Training Run artifact.
            from datahub_executor.common.monitor.inference_v2.inference_utils import (
                split_time_series_df,
            )

            try:
                eval_train_df, eval_df = split_time_series_df(
                    df_for_auto, train_ratio=float(train_ratio)
                )
            except ValueError as e:
                raise RuntimeError(
                    f"Auto evaluation requires a valid train/test split: {e}"
                ) from e

            _auto_v2_start = time.perf_counter()
            result = run_auto_training_pipeline(
                df=df_for_auto,
                context=ctx,
                num_intervals=int(auto_num_intervals),
                interval_hours=int(auto_interval_hours),
                sensitivity_level=int(auto_sensitivity),
                existing_model_config=existing_model_config,
                model_pairings=model_pairings,
                eval_train_ratio=float(train_ratio),
                progress_hooks=_auto_v2_progress_hook,
            )
            _auto_v2_elapsed = time.perf_counter() - _auto_v2_start
            execution_timing: dict[str, float] = {
                "total_seconds": _auto_v2_elapsed,
            }

            if hostname:
                endpoint_cache = loader.cache.get_endpoint_cache(hostname)
                run_id = (
                    "auto_v2_"
                    + hashlib.sha1(
                        f"{assertion_urn}::{auto_prep_id}::{datetime.now(timezone.utc).isoformat()}".encode()
                    ).hexdigest()[:8]
                )
                # Extract combination_results from training_result if available
                combination_results = None
                if (
                    hasattr(result.training_result, "combination_results")
                    and result.training_result.combination_results
                ):
                    combination_results = result.training_result.combination_results

                ok = endpoint_cache.save_auto_inference_v2_run(
                    run_id=run_id,
                    assertion_urn=assertion_urn,
                    preprocessing_id=auto_prep_id,
                    train_df=df_for_auto,
                    prediction_df=result.training_result.prediction_df,
                    model_config_dict=result.training_result.model_config.model_dump(),
                    pairings_used=result.pairings_used,
                    sensitivity_level=int(auto_sensitivity),
                    interval_hours=int(auto_interval_hours),
                    num_intervals=int(auto_num_intervals),
                    warm_start_used=existing_model_config is not None,
                    warm_start_source="cached_inference_data_for_assertion",
                    warm_start_generated_at=(
                        warm_state_payload.get("generated_at")
                        if isinstance(warm_state_payload, dict)
                        and isinstance(warm_state_payload.get("generated_at"), int)
                        else None
                    ),
                    eval_train_df=eval_train_df,
                    eval_df=eval_df,
                    evaluation_detection_results=result.training_result.evaluation_detection_results,
                    combination_results=combination_results,
                    execution_timing=execution_timing,
                )
                if ok:
                    # Add the auto run to separate session state immediately so it appears in the runs list
                    # without requiring a cache reload (which only happens once per session).
                    auto_run_data = {
                        "metadata": {
                            "run_id": run_id,
                            "assertion_urn": assertion_urn,
                            "preprocessing_id": auto_prep_id,
                            "sensitivity_level": int(auto_sensitivity),
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "pairings_used": result.pairings_used,
                            "combination_results": combination_results,
                            "execution_timing": execution_timing,
                        },
                        "train_df": df_for_auto,
                        "prediction_df": result.training_result.prediction_df,
                        "model_config": result.training_result.model_config.model_dump(),
                    }
                    # Store in separate auto runs session state (not mixed with training runs)
                    if _AUTO_INFERENCE_V2_RUNS_KEY not in st.session_state:
                        st.session_state[_AUTO_INFERENCE_V2_RUNS_KEY] = {}
                    st.session_state[_AUTO_INFERENCE_V2_RUNS_KEY][run_id] = (
                        auto_run_data
                    )
                    # Also store in detail state for backward compatibility
                    st.session_state[f"{_AUTO_V2_RUN_DETAILS_KEY_PREFIX}{run_id}"] = (
                        auto_run_data
                    )

                    # Also persist a comparable Training Run using the strict holdout forecast.
                    # This enables comparing Auto results side-by-side with other runs.
                    eval_forecast_df = result.training_result.evaluation_forecast_df
                    if (
                        isinstance(eval_df, pd.DataFrame)
                        and len(eval_df) > 0
                        and isinstance(eval_forecast_df, pd.DataFrame)
                        and len(eval_forecast_df) > 0
                    ):
                        merged = eval_df.merge(
                            eval_forecast_df[["ds", "yhat"]],
                            on="ds",
                            how="inner",
                        )
                        metrics = _compute_metrics(merged["y"], merged["yhat"])
                        auto_eval_score = None
                        try:
                            fs_obj = getattr(
                                result.training_result.model_config,
                                "forecast_score",
                                None,
                            )
                            if isinstance(fs_obj, (int, float)):
                                fs = float(fs_obj)
                                if np.isfinite(fs):
                                    auto_eval_score = fs
                        except Exception:
                            auto_eval_score = None
                        eval_run_id = f"auto_v2_eval__{run_id}"
                        endpoint_cache.save_training_run(
                            run_id=eval_run_id,
                            model_key="auto_inference_v2",
                            model_name="inference_v2",
                            preprocessing_id=auto_prep_id,
                            train_df=eval_train_df,
                            test_df=eval_df,
                            forecast=eval_forecast_df,
                            metrics=metrics,
                            color="#9467bd",
                            dash="dot",
                            assertion_urn=assertion_urn,
                            is_observe_model=False,
                            registry_key=None,
                            score=auto_eval_score,
                            sensitivity_level=int(auto_sensitivity),
                        )

                    st.session_state["last_auto_inference_v2_run"] = {
                        "run_id": run_id,
                        "pairings_used": result.pairings_used,
                    }
                    st.rerun()
                else:
                    st.error("Failed to persist Auto run to cache.")
            else:
                st.success(
                    "inference_v2 run completed (not saved: no hostname selected)."
                )
        except Exception as e:
            # Extract detailed error information if available
            error_msg = str(e)
            from datahub_executor.common.exceptions import TrainingErrorException

            if isinstance(e, TrainingErrorException) and e.properties:
                # Show additional context from exception properties
                error_details = []
                if "errors" in e.properties:
                    # errors is a pipe-separated string, split and format nicely
                    errors_str = e.properties.get("errors", "")
                    if errors_str:
                        errors_list = errors_str.split(" | ")
                        error_details.extend(errors_list)
                # Add other relevant properties
                for key in ["step", "pairings_tried"]:
                    if key in e.properties:
                        error_details.append(f"{key}: {e.properties[key]}")

                if error_details:
                    error_msg = f"{error_msg}\n\nDetails:\n" + "\n".join(
                        f"  • {detail}" for detail in error_details
                    )

            st.error(f"Auto pipeline run failed: {error_msg}")
        finally:
            progress_text.empty()
            progress_bar.empty()


def _render_training_configuration_tab(
    *,
    loader: "DataLoader",
    hostname: Optional[str],
    assertion_urn: Optional[str],
    preprocessing_options: list[str],
    preprocessing_labels: dict[str, str],
    preprocessing_row_counts: dict[str, int],
    saved_preprocessings: list[dict],
    current_preprocessed_df: Optional[pd.DataFrame],
    train_ratio: float,
    selected_preprocessing: str,
) -> None:
    """Render the Manual Configuration UI and execute selected manual runs."""
    st.caption(
        "Train and compare forecasting models (supports multiple preprocessings)."
    )

    # Cache observe-model configs by sensitivity for use during training
    observe_configs_by_sensitivity: dict[
        int, dict[str, ObserveRegistryModelConfig]
    ] = {}

    def _get_model_config_for_training(
        model_key: str, sensitivity_level: int
    ) -> ModelConfig:
        base_config = MODEL_REGISTRY[model_key]
        if not base_config.is_observe_model:
            return base_config

        obs_configs = observe_configs_by_sensitivity.get(sensitivity_level)
        if obs_configs is None:
            obs_configs = get_observe_model_configs(sensitivity_level=sensitivity_level)
            observe_configs_by_sensitivity[sensitivity_level] = obs_configs

        obs_config = obs_configs.get(model_key)
        if (
            obs_config is not None
            and obs_config.train_fn is not None
            and obs_config.predict_fn is not None
        ):
            return ModelConfig(
                name=obs_config.name,
                description=obs_config.description,
                train_fn=obs_config.train_fn,
                predict_fn=obs_config.predict_fn,
                color=obs_config.color,
                dash=obs_config.dash,
                is_observe_model=True,
                registry_key=obs_config.registry_key,
            )

        return base_config

    available_models = list(MODEL_REGISTRY.keys())
    model_labels = {k: v.name for k, v in MODEL_REGISTRY.items()}

    if _PRIMARY_TRAINING_GROUP_KEY not in st.session_state:
        st.session_state[_PRIMARY_TRAINING_GROUP_KEY] = {
            "preprocessing": preprocessing_options[0] if preprocessing_options else "",
            "models": [],
            "sensitivity_by_preprocessing": {},
        }

    primary_group: dict[str, object] = st.session_state[_PRIMARY_TRAINING_GROUP_KEY]
    primary_group_preprocessing = str(selected_preprocessing or "")
    if (
        preprocessing_options
        and primary_group_preprocessing not in preprocessing_options
    ):
        primary_group_preprocessing = preprocessing_options[0]
    primary_group["preprocessing"] = primary_group_preprocessing
    selected_preprocessing = primary_group_preprocessing

    st.markdown("**Forecast Model Sensitivity**")
    stored_sensitivity_by_preproc = primary_group.get("sensitivity_by_preprocessing")
    primary_sensitivity_by_preproc: dict[str, int] = (
        dict(stored_sensitivity_by_preproc)
        if isinstance(stored_sensitivity_by_preproc, dict)
        else {}
    )
    primary_sensitivity_state_key = (
        f"model_training__primary_forecast_sensitivity__{selected_preprocessing}"
    )
    primary_sensitivity_level = st.slider(
        "Forecast Sensitivity Level",
        min_value=1,
        max_value=10,
        value=int(primary_sensitivity_by_preproc.get(selected_preprocessing, 5)),
        step=1,
        help=(
            "Sensitivity level (1-10) for forecast model behavior adjustment. "
            "This controls forecast model parameters that impact interval/band tightness. "
            "Higher values (7-10): More aggressive (narrower intervals, more anomalies). "
            "Lower values (1-3): More conservative (wider intervals, fewer anomalies). "
            "Medium values (4-6): Balanced defaults."
        ),
        key=primary_sensitivity_state_key,
    )
    primary_sensitivity_by_preproc[selected_preprocessing] = int(
        primary_sensitivity_level
    )
    primary_group["sensitivity_by_preprocessing"] = primary_sensitivity_by_preproc
    st.caption(
        f"Forecast Sensitivity: {primary_sensitivity_level} "
        f"({'Aggressive' if primary_sensitivity_level >= 7 else 'Balanced' if primary_sensitivity_level >= 4 else 'Conservative'})"
    )

    st.write("**Models to train**")
    selected_model_keys: list[str] = []
    model_cols = st.columns(min(3, len(available_models))) if available_models else []

    default_model_keys = {"obs_prophet", "obs_datahub"}
    stored_primary_models = primary_group.get("models", [])
    if isinstance(stored_primary_models, list):
        primary_selected_models = [str(m) for m in stored_primary_models]
    else:
        primary_selected_models = []
    if not primary_selected_models:
        primary_selected_models = [
            m for m in available_models if m in default_model_keys
        ]

    for idx, model_key in enumerate(available_models):
        col_idx = idx % len(model_cols) if model_cols else 0
        with model_cols[col_idx] if model_cols else st.container():
            is_checked = st.checkbox(
                model_labels.get(model_key, model_key),
                value=model_key in primary_selected_models,
                key=f"model_training__primary_model_cb__{model_key}",
            )
            if is_checked:
                selected_model_keys.append(model_key)
    primary_group["models"] = list(selected_model_keys)

    with st.expander("Auto (inference_v2) suggestions", expanded=False):
        try:
            default_pairings = get_default_pairing_identifiers()
            suggested_forecast_models = {
                p.split("_", 1)[0] for p in default_pairings if "_" in p
            }

            suggested_model_keys: list[str] = []
            for mk in available_models:
                rk = MODEL_REGISTRY[mk].registry_key
                if not rk:
                    continue
                base = rk.split("@", 1)[0]
                if base in suggested_forecast_models:
                    suggested_model_keys.append(mk)

            suggested_preproc_option: Optional[str] = None
            for item in saved_preprocessings:
                pid = item.get("preprocessing_id")
                meta = item.get("metadata") or {}
                if (
                    isinstance(pid, str)
                    and pid in preprocessing_options
                    and isinstance(meta, dict)
                ):
                    cfg = meta.get("preprocessing_config") or {}
                    if (
                        isinstance(cfg, dict)
                        and cfg.get("_source") == "auto_inference_v2"
                    ):
                        suggested_preproc_option = pid
                        break

            st.caption(
                "Defaults are derived from inference_v2’s `DEFAULT_MODEL_PAIRINGS` "
                f"(e.g., {', '.join(default_pairings) if default_pairings else 'n/a'})."
            )
            if suggested_preproc_option:
                st.caption(
                    f"Suggested preprocessing: {preprocessing_labels.get(suggested_preproc_option, suggested_preproc_option)}"
                )
            if suggested_model_keys:
                st.caption(
                    f"Suggested models: {', '.join(model_labels.get(k, k) for k in suggested_model_keys)}"
                )

            suggested_sensitivity_level = _clamp_sensitivity_level(
                st.session_state.get(
                    _AUTO_V2_SENS_VALUE_KEY,
                    _get_inference_v2_default_sensitivity_level(
                        st.session_state.get("current_assertion_type")
                        if isinstance(
                            st.session_state.get("current_assertion_type"), str
                        )
                        else None
                    ),
                )
            )

            if st.button(
                "Apply Auto suggestions to primary config",
                key="model_training__apply_auto_suggestions_primary",
                disabled=not (suggested_model_keys or suggested_preproc_option),
                on_click=apply_auto_suggestions_to_training_group,
                kwargs={
                    "session_state": st.session_state,
                    "group_key": _PRIMARY_TRAINING_GROUP_KEY,
                    "suggested_preproc": suggested_preproc_option,
                    "suggested_models": suggested_model_keys,
                    "all_models": available_models,
                    "suggested_sensitivity_level": int(suggested_sensitivity_level),
                },
            ):
                pass
        except Exception as e:
            st.caption(f"Auto suggestions unavailable: {e}")

    if "additional_groups" not in st.session_state:
        st.session_state["additional_groups"] = []

    additional_groups: list[dict] = st.session_state["additional_groups"]

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

            group_row_count = preprocessing_row_counts.get(
                group.get("preprocessing", ""), 0
            )
            if group_row_count > 0:
                train_pts = int(group_row_count * train_ratio)
                test_pts = group_row_count - train_pts
                st.caption(f"Data points: {train_pts} train | {test_pts} test")

            st.markdown("**Forecast Model Sensitivity**")
            group_sensitivity_level = st.slider(
                "Forecast Sensitivity Level",
                min_value=1,
                max_value=10,
                value=int(group.get("sensitivity_level", 5)),
                step=1,
                help=(
                    "Sensitivity level (1-10) for forecast model behavior adjustment. "
                    "This controls forecast model parameters that impact interval/band tightness. "
                    "Higher values (7-10): More aggressive (narrower intervals, more anomalies). "
                    "Lower values (1-3): More conservative (wider intervals, fewer anomalies). "
                    "Medium values (4-6): Balanced defaults."
                ),
                key=f"group_{idx}_forecast_sensitivity",
            )
            group["sensitivity_level"] = group_sensitivity_level
            st.caption(
                f"Forecast Sensitivity: {group_sensitivity_level} "
                f"({'Aggressive' if group_sensitivity_level >= 7 else 'Balanced' if group_sensitivity_level >= 4 else 'Conservative'})"
            )

            st.write("**Models**")
            group_selected_models: list[str] = []
            group_model_cols = (
                st.columns(min(3, len(available_models))) if available_models else []
            )
            for m_idx, model_key in enumerate(available_models):
                m_col_idx = m_idx % len(group_model_cols) if group_model_cols else 0
                with (
                    group_model_cols[m_col_idx] if group_model_cols else st.container()
                ):
                    prev_selected = group.get("models", [])
                    is_checked = st.checkbox(
                        model_labels.get(model_key, model_key),
                        value=model_key in prev_selected,
                        key=f"group_{idx}_model_cb_{model_key}",
                    )
                    if is_checked:
                        group_selected_models.append(model_key)
            group["models"] = group_selected_models

    for idx in reversed(groups_to_remove):
        additional_groups.pop(idx)
        st.rerun()

    if st.button("+ Add Another Preprocessing", key="add_group"):
        additional_groups.append(
            _build_new_additional_group(
                preprocessing_options=preprocessing_options,
                base_selected_model_keys=selected_model_keys,
                base_selected_preprocessing=selected_preprocessing,
            )
        )
        st.rerun()

    st.subheader("Manual Summary")

    all_training_items: list[tuple[str, str, str, int]] = []

    preprocessing_id = (
        "current" if selected_preprocessing == "__current__" else selected_preprocessing
    )
    for model_key in selected_model_keys:
        display_label = (
            f"{preprocessing_labels.get(selected_preprocessing, preprocessing_id)} "
            f"→ {model_labels.get(model_key, model_key)} "
            f"(sens {primary_sensitivity_level})"
        )
        all_training_items.append(
            (preprocessing_id, model_key, display_label, int(primary_sensitivity_level))
        )

    for group in additional_groups:
        grp_preprocessing = group.get("preprocessing", "")
        grp_preprocessing_id = (
            "current" if grp_preprocessing == "__current__" else grp_preprocessing
        )
        grp_sensitivity_level = int(group.get("sensitivity_level", 5))
        for model_key in group.get("models", []):
            display_label = (
                f"{preprocessing_labels.get(grp_preprocessing, grp_preprocessing_id)} "
                f"→ {model_labels.get(model_key, model_key)} "
                f"(sens {grp_sensitivity_level})"
            )
            all_training_items.append(
                (grp_preprocessing_id, model_key, display_label, grp_sensitivity_level)
            )

    if all_training_items:
        st.caption(f"{len(all_training_items)} manual run(s) selected:")
        for _, _, label, _ in all_training_items:
            st.markdown(f"• {label}")
    else:
        st.warning(
            "No manual runs configured. Select at least one preprocessing and model."
        )

    st.markdown("---")

    st.info(
        "⏱️ Manual runs may take some time depending on the number of models "
        "and data size. Please wait for all runs to complete."
    )

    run_clicked = st.button(
        "Run Manual",
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

        preprocessing_cache: dict[str, pd.DataFrame] = {}

        for i, (prep_id, model_key, label, sensitivity_level) in enumerate(
            all_training_items
        ):
            config = _get_model_config_for_training(
                model_key, sensitivity_level=sensitivity_level
            )
            status_text.text(f"Manual run: {label} ({i + 1}/{total_runs})")

            try:
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
                    failed_models.append(f"{label}: Failed to load preprocessing data")
                    continue

                preprocessed_df = preprocessed_df.dropna(subset=["y"]).copy()

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

                train_df, test_df = _split_train_test(preprocessed_df, train_ratio)
                future_df = pd.DataFrame({"ds": test_df["ds"]})

                model = config.train_fn(train_df)
                forecast = config.predict_fn(model, future_df)

                merged = test_df.merge(forecast[["ds", "yhat"]], on="ds", how="inner")
                metrics = _compute_metrics(merged["y"], merged["yhat"])

                score = None
                if compute_forecast_score is not None:
                    try:
                        score_metrics: dict[str, float] = {}
                        if metrics.get("MAE") is not None:
                            score_metrics["mae"] = metrics["MAE"]
                        if metrics.get("RMSE") is not None:
                            score_metrics["rmse"] = metrics["RMSE"]
                        if metrics.get("MAPE") is not None:
                            score_metrics["mape"] = metrics["MAPE"]
                        if metrics.get("coverage") is not None:
                            score_metrics["coverage"] = metrics["coverage"]
                        y_range = None
                        if hasattr(model, "y_range") and model.y_range is not None:
                            y_range = model.y_range
                        elif len(merged) > 0:
                            y_range = float(merged["y"].max() - merged["y"].min())
                        if compute_forecast_score is not None and score_metrics:
                            score = compute_forecast_score(
                                score_metrics, y_range=y_range
                            )
                        else:
                            score = None
                    except Exception:
                        pass

                run_id = _generate_run_id(model_key, prep_id, sensitivity_level)
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
                    assertion_urn=assertion_urn,
                    is_observe_model=config.is_observe_model,
                    registry_key=config.registry_key,
                    score=score,
                    sensitivity_level=sensitivity_level,
                )
                _store_training_run(run)
                trained_count += 1
            except Exception as e:
                failed_models.append(f"{label}: {e}")

            progress_bar.progress((i + 1) / total_runs)

        status_text.empty()
        progress_bar.empty()

        st.session_state["last_training_result"] = {
            "trained_count": trained_count,
            "total_count": total_runs,
            "failed_models": failed_models,
        }
        st.rerun()


# =============================================================================
# Training Groups Storage
# =============================================================================

_TRAINING_GROUPS_KEY = "training_groups"
_PRIMARY_TRAINING_GROUP_KEY = "model_training__primary_training_group"


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


def _build_new_additional_group(
    preprocessing_options: Sequence[str],
    base_selected_model_keys: Sequence[str],
    base_selected_preprocessing: Optional[str] = None,
) -> dict[str, object]:
    """
    Build a new additional training group dict for session state.

    The additional group should inherit the base group's selected models and
    preprocessing to minimize repetitive clicking when adding multiple training
    configurations.
    """
    selected_preproc: str = ""
    if (
        base_selected_preprocessing
        and base_selected_preprocessing in preprocessing_options
    ):
        selected_preproc = base_selected_preprocessing
    elif preprocessing_options:
        selected_preproc = preprocessing_options[0]
    return {
        "preprocessing": selected_preproc,
        "models": list(base_selected_model_keys),
    }


# =============================================================================
# Main Page
# =============================================================================


def render_model_training_page() -> None:
    """Render the Model Training page."""
    st.header("Model Training")

    init_explorer_state()
    loader = DataLoader()

    # Resolve these early so top-nav actions can be enabled/disabled.
    hostname = st.session_state.get("current_hostname")
    assertion_urn = _get_current_assertion_urn()

    # Navigation - Back to Preprocessing
    col_nav_back, col_nav_spacer, col_nav_compare = st.columns([1, 3, 2])
    with col_nav_back:
        if st.button("← Preprocessing"):
            from . import preprocessing_page

            st.switch_page(preprocessing_page)
    with col_nav_compare:
        has_training_runs = bool(_get_training_runs())
        has_auto_runs = False
        if hostname:
            try:
                endpoint_cache = loader.cache.get_endpoint_cache(hostname)
                cached_training = endpoint_cache.list_saved_training_runs(
                    assertion_urn=assertion_urn
                )
                cached_auto = endpoint_cache.list_saved_auto_inference_v2_runs(
                    assertion_urn=assertion_urn
                )
                has_training_runs = has_training_runs or bool(cached_training)
                has_auto_runs = bool(cached_auto)
            except Exception:
                pass

        if st.button(
            "Time Series Comparison →",
            disabled=not (has_training_runs or has_auto_runs),
            help=(
                "Compare and visualize Manual Runs and Inference_v2 Runs."
                if (has_training_runs or has_auto_runs)
                else "No Manual Runs or Inference_v2 Runs available to compare yet."
            ),
            key="model_training__to_timeseries_comparison_top",
        ):
            # Ensure cached runs (including Auto runs mapped into TrainingRun objects)
            # are loaded into session state before navigating.
            try:
                _load_cached_training_runs()
            except Exception:
                pass
            from . import timeseries_comparison_page

            st.switch_page(timeseries_comparison_page)

    st.markdown("---")

    # Get hostname and load saved preprocessings
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
        short = display_preprocessing_id(pid, metadata=item.get("metadata"))
        preprocessing_labels[pid] = f"{short} ({row_count} rows)"
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

        # Display truncation warning if data was truncated during preprocessing
        _display_truncation_warning()

        col_min, col_slider, col_max = st.columns([1, 4, 1])
        with col_min:
            st.caption("Min: 50%")
        with col_slider:
            train_pct = st.slider(
                "Train/Test Split",
                min_value=50,
                max_value=95,
                value=70,
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
        # 2. Primary Preprocessing (shared across Auto + Training tabs)
        # =================================================================
        st.subheader("Preprocessing")

        if _PRIMARY_TRAINING_GROUP_KEY not in st.session_state:
            st.session_state[_PRIMARY_TRAINING_GROUP_KEY] = {
                "preprocessing": preprocessing_options[0]
                if preprocessing_options
                else "",
                "models": [],
                "sensitivity_by_preprocessing": {},
            }

        primary_group: dict[str, object] = st.session_state[_PRIMARY_TRAINING_GROUP_KEY]
        stored_primary = st.session_state.get("model_training__primary_preprocessing")
        primary_default = (
            str(stored_primary)
            if isinstance(stored_primary, str) and stored_primary
            else str(primary_group.get("preprocessing", preprocessing_options[0]))
        )
        if preprocessing_options and primary_default not in preprocessing_options:
            primary_default = preprocessing_options[0]

        selected_preprocessing = st.selectbox(
            "Preprocessing",
            options=preprocessing_options,
            format_func=lambda x: preprocessing_labels.get(x, x),
            help="Select a preprocessing configuration (applies to Inference_v2 + Manual)",
            index=preprocessing_options.index(primary_default)
            if preprocessing_options
            else 0,
            key="model_training__primary_preprocessing",
        )
        primary_group["preprocessing"] = selected_preprocessing

        row_count = preprocessing_row_counts.get(selected_preprocessing, 0)
        if row_count > 0:
            train_pts = int(row_count * train_ratio)
            test_pts = row_count - train_pts
            st.caption(f"Data points: {train_pts} train | {test_pts} test")

        # =================================================================
        # 3. Load observe-models with current sensitivity
        # =================================================================
        # Load observe-models configs for MODEL SELECTION DISPLAY.
        #
        # IMPORTANT:
        # Sensitivity is per Training Configuration (per group), so we don't bind
        # a single sensitivity here. We load using a baseline value so models
        # appear in the UI; the actual training loop instantiates observe models
        # per-run using the run's configured sensitivity.
        _DISPLAY_SENSITIVITY_LEVEL = 5
        if is_observe_models_available():
            observe_configs = get_observe_model_configs(
                sensitivity_level=_DISPLAY_SENSITIVITY_LEVEL
            )
            for key, obs_config in observe_configs.items():
                # Only add if train_fn and predict_fn are defined
                if (
                    obs_config.train_fn is not None
                    and obs_config.predict_fn is not None
                ):
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

        # Put Auto first so it is selected by default.
        tab_auto, tab_training = st.tabs(
            ["Inference_v2 Pipeline", "Manual Configuration"]
        )

        with tab_auto:
            _render_auto_inference_v2_pipeline_tab(
                loader=loader,
                hostname=hostname,
                assertion_urn=assertion_urn,
                preprocessing_options=preprocessing_options,
                preprocessing_labels=preprocessing_labels,
                train_ratio=float(train_ratio),
                selected_preprocessing=selected_preprocessing,
            )

        with tab_training:
            _render_training_configuration_tab(
                loader=loader,
                hostname=hostname,
                assertion_urn=assertion_urn,
                preprocessing_options=preprocessing_options,
                preprocessing_labels=preprocessing_labels,
                preprocessing_row_counts=preprocessing_row_counts,
                saved_preprocessings=saved_preprocessings,
                current_preprocessed_df=current_preprocessed_df
                if isinstance(current_preprocessed_df, pd.DataFrame)
                else None,
                train_ratio=float(train_ratio),
                selected_preprocessing=selected_preprocessing,
            )

    # Display training results from last run (persisted across rerun)
    if "last_training_result" in st.session_state:
        result = st.session_state.pop("last_training_result")
        if result["failed_models"]:
            for error_msg in result["failed_models"]:
                st.error(f"Manual run failed: {error_msg}")
        if result["trained_count"] > 0:
            st.success(
                f"Successfully trained {result['trained_count']} of {result['total_count']} model(s)!"
            )
        elif not result["failed_models"]:
            st.error("All models failed to train.")

    if "last_auto_inference_v2_run" in st.session_state:
        r = st.session_state.pop("last_auto_inference_v2_run")
        run_id = r.get("run_id", "")
        hash_id = _extract_hash_from_run_id(run_id)
        hash_display = f" [{hash_id}]" if hash_id else ""
        st.success(
            f"Saved inference_v2 run `{run_id}`{hash_display} "
            f"(pairings: {', '.join(r.get('pairings_used') or [])})."
        )

    with col_runs:
        # Consolidated single list showing both training runs and auto inference_v2 runs
        _render_training_runs_panel(show_header=False)

        # Navigation moved to top (Preprocessing row).


__all__ = [
    "render_model_training_page",
    "TrainingRun",
    "_get_training_runs",
]
