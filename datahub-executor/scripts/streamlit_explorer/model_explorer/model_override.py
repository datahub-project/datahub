# ruff: noqa: INP001
"""
Model Override page for saving trained model configurations to inference data.

This page allows users to:
1. Select a trained anomaly detection run from the Anomaly Comparison page
2. Preview the extracted configurations (preprocessing, forecast, anomaly)
3. Compare against existing inference data (if any)
4. Save the configuration to the selected assertion's inference data store
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import streamlit as st

from ..common import (
    _SELECTED_ASSERTION,
    _SELECTED_ENDPOINT,
    EndpointCache,
    _get_datahub_url,
    _shorten_urn,
    get_model_hyperparameters,
    init_explorer_state,
)
from .anomaly_comparison import AnomalyDetectionRun, _get_anomaly_runs
from .model_training import TrainingRun, _get_training_runs

logger = logging.getLogger(__name__)

# =============================================================================
# Session State Keys
# =============================================================================

_MODEL_OVERRIDE_SELECTED_RUN = "model_override_selected_run"
_MODEL_OVERRIDE_CONFIRM_SAVE = "model_override_confirm_save"

# =============================================================================
# Optional Imports (observe-models)
# =============================================================================

try:
    from datahub_observe.algorithms.anomaly_detection.config import AnomalyModelConfig
    from datahub_observe.algorithms.forecasting.config import ForecastModelConfig
    from datahub_observe.algorithms.training.anomaly_evals import (
        AnomalyTrainingEvals,
        AnomalyTrainingRun,
    )
    from datahub_observe.algorithms.training.forecast_evals import (
        ForecastTrainingEvals,
        ForecastTrainingRun,
    )

    HAS_OBSERVE_MODELS = True
except ImportError:
    HAS_OBSERVE_MODELS = False
    AnomalyModelConfig = None  # type: ignore[misc, assignment]
    ForecastModelConfig = None  # type: ignore[misc, assignment]
    AnomalyTrainingEvals = None  # type: ignore[misc, assignment]
    AnomalyTrainingRun = None  # type: ignore[misc, assignment]
    ForecastTrainingEvals = None  # type: ignore[misc, assignment]
    ForecastTrainingRun = None  # type: ignore[misc, assignment]

try:
    from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig
    from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
        AnomalyConfigSerializer,
        AnomalyEvalsSerializer,
        ForecastConfigSerializer,
        ForecastEvalsSerializer,
        PreprocessingConfigSerializer,
    )

    HAS_INFERENCE_UTILS = True
except ImportError:
    HAS_INFERENCE_UTILS = False
    ModelConfig = None  # type: ignore[misc, assignment]
    PreprocessingConfigSerializer = None  # type: ignore[misc, assignment]
    ForecastConfigSerializer = None  # type: ignore[misc, assignment]
    AnomalyConfigSerializer = None  # type: ignore[misc, assignment]
    ForecastEvalsSerializer = None  # type: ignore[misc, assignment]
    AnomalyEvalsSerializer = None  # type: ignore[misc, assignment]


# =============================================================================
# Helper Functions: Future Predictions
# =============================================================================

# Default prediction horizon key for session state
_PREDICTION_HORIZON_KEY = "model_override_prediction_horizon"


def _infer_frequency_hours(df: pd.DataFrame) -> float:
    """Infer the data frequency in hours from a DataFrame.

    Args:
        df: DataFrame with 'ds' datetime column

    Returns:
        Estimated frequency in hours (default 24 if cannot infer)
    """
    if df is None or len(df) < 2 or "ds" not in df.columns:
        return 24.0  # Default to daily

    try:
        ds = pd.to_datetime(df["ds"]).sort_values()
        diffs = ds.diff().dropna()
        if len(diffs) == 0:
            return 24.0
        median_diff = diffs.median()
        return max(median_diff.total_seconds() / 3600, 1.0)  # At least 1 hour
    except Exception:
        return 24.0


def _is_forecast_based_model(model: object) -> bool:
    """Check if an anomaly model is forecast-based (can generate future predictions).

    Forecast-based models have an underlying forecast model that can predict
    future y values. Standalone models (like DeepSVDD) work directly on input
    features and cannot generate predictions for future data with unknown y values.

    Args:
        model: The anomaly detection model

    Returns:
        True if the model can generate future predictions
    """
    if model is None:
        return False

    # Check for forecast_model attribute (forecast-based anomaly models)
    if hasattr(model, "forecast_model") and model.forecast_model is not None:
        return True

    # Check class name for known forecast-based models
    model_class_name = model.__class__.__name__.lower()
    forecast_based_patterns = [
        "forecast",
        "adaptive_band",
        "datahub",
        "iforest_forecast",
    ]
    for pattern in forecast_based_patterns:
        if pattern in model_class_name:
            return True

    return False


def _generate_future_predictions(
    anomaly_run: "AnomalyDetectionRun",
    horizon_periods: int = 7,
) -> Optional[pd.DataFrame]:
    """Generate future predictions from a trained anomaly model.

    Creates a DataFrame with future dates and calls the model's detect()
    method to generate predictions with detection bands.

    Note: Only forecast-based anomaly models can generate future predictions.
    Standalone models like DeepSVDD work on raw features and cannot predict
    future values without actual y data.

    Args:
        anomaly_run: The trained anomaly detection run
        horizon_periods: Number of periods to predict into the future

    Returns:
        DataFrame with columns: ds, yhat, yhat_lower, yhat_upper,
        detection_lower, detection_upper, is_anomaly (NaN), anomaly_score (NaN)
        Returns None if model doesn't support future predictions.
    """
    model = anomaly_run.model
    if model is None:
        return None

    # Check if model has detect method
    if not hasattr(model, "detect"):
        return None

    # Check if this is a forecast-based model that can generate future predictions
    # Standalone models (like DeepSVDD) cannot predict future values
    if not _is_forecast_based_model(model):
        logger.info(
            f"Model {model.__class__.__name__} is not forecast-based. "
            "Cannot generate future predictions."
        )
        return None

    # Get training data to determine last timestamp and frequency
    train_df = anomaly_run.train_df
    test_df = anomaly_run.test_df

    # Use test_df end as the starting point for future predictions
    reference_df = test_df if test_df is not None and len(test_df) > 0 else train_df
    if reference_df is None or len(reference_df) == 0:
        return None

    try:
        # Infer frequency from the data
        freq_hours = _infer_frequency_hours(reference_df)

        # Get the last timestamp
        last_ts = pd.to_datetime(reference_df["ds"]).max()

        # Generate future dates
        future_dates = pd.date_range(
            start=last_ts + pd.Timedelta(hours=freq_hours),
            periods=horizon_periods,
            freq=pd.Timedelta(hours=freq_hours),
        )

        # Create future DataFrame with NaN y values
        future_df = pd.DataFrame({"ds": future_dates, "y": float("nan")})

        # Call detect() to get predictions with detection bands
        # The model will compute yhat, detection_lower, detection_upper
        # is_anomaly and anomaly_score will be NaN since y is NaN
        predictions = model.detect(future_df)  # type: ignore[attr-defined]

        return predictions

    except Exception as e:
        logger.warning(f"Failed to generate future predictions: {e}")
        return None


def _prepare_predictions_for_save(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare predictions DataFrame for inference_utils compatibility.

    Renames columns and converts timestamps to match expected format:
    - ds -> timestamp_ms (datetime to milliseconds)
    - detection_lower -> detection_band_lower
    - detection_upper -> detection_band_upper

    Args:
        df: Predictions DataFrame from anomaly model

    Returns:
        DataFrame with renamed columns and timestamp_ms
    """
    if df is None or df.empty:
        return df

    result = df.copy()

    # Convert ds (datetime) to timestamp_ms (milliseconds since epoch)
    if "ds" in result.columns:
        result["timestamp_ms"] = pd.to_datetime(result["ds"]).astype("int64") // 10**6

    # Rename detection columns to match inference_utils expectations
    column_renames = {
        "detection_lower": "detection_band_lower",
        "detection_upper": "detection_band_upper",
    }
    result = result.rename(columns=column_renames)

    return result


# =============================================================================
# Helper Functions: Config Extraction
# =============================================================================


def _parse_model_name_version(
    registry_key: Optional[str], model_name: Optional[str]
) -> tuple[Optional[str], Optional[str]]:
    """Parse model name and version from registry key or model name.

    Args:
        registry_key: Full registry key like "prophet (0.1.0)"
        model_name: Alternative model name

    Returns:
        Tuple of (name, version)
    """
    if registry_key and "(" in registry_key and ")" in registry_key:
        # Parse "prophet (0.1.0)" -> ("prophet", "0.1.0")
        parts = registry_key.rsplit("(", 1)
        name = parts[0].strip()
        version = parts[1].rstrip(")").strip()
        return name, version
    elif model_name:
        return model_name, None
    return None, None


def _build_forecast_evals(
    training_run: TrainingRun,
) -> Optional["ForecastTrainingEvals"]:
    """Build ForecastTrainingEvals from TrainingRun metrics.

    Args:
        training_run: The training run with metrics and data

    Returns:
        ForecastTrainingEvals or None if observe-models not available
    """
    if not HAS_OBSERVE_MODELS or ForecastTrainingEvals is None:
        return None

    metrics = training_run.metrics or {}

    # Extract timestamp ranges from data
    train_start_millis = None
    train_end_millis = None
    test_start_millis = None
    test_end_millis = None

    if training_run.train_df is not None and len(training_run.train_df) > 0:
        if "ds" in training_run.train_df.columns:
            train_start_millis = int(
                pd.to_datetime(training_run.train_df["ds"].min()).timestamp() * 1000
            )
            train_end_millis = int(
                pd.to_datetime(training_run.train_df["ds"].max()).timestamp() * 1000
            )

    if training_run.test_df is not None and len(training_run.test_df) > 0:
        if "ds" in training_run.test_df.columns:
            test_start_millis = int(
                pd.to_datetime(training_run.test_df["ds"].min()).timestamp() * 1000
            )
            test_end_millis = int(
                pd.to_datetime(training_run.test_df["ds"].max()).timestamp() * 1000
            )

    run = ForecastTrainingRun(
        mae=metrics.get("MAE", 0.0) or 0.0,
        rmse=metrics.get("RMSE", 0.0) or 0.0,
        mape=metrics.get("MAPE", 0.0) or 0.0,
        train_samples=(
            len(training_run.train_df) if training_run.train_df is not None else None
        ),
        test_samples=(
            len(training_run.test_df) if training_run.test_df is not None else None
        ),
        train_start_millis=train_start_millis,
        train_end_millis=train_end_millis,
        test_start_millis=test_start_millis,
        test_end_millis=test_end_millis,
    )
    evals = ForecastTrainingEvals(runs=[run])
    evals.compute_aggregated()
    return evals


def _compute_anomaly_metrics_from_detection(
    detection_results: pd.DataFrame,
) -> dict[str, Any]:
    """Compute anomaly metrics from detection results DataFrame.

    Args:
        detection_results: DataFrame with is_anomaly column

    Returns:
        Dict with anomaly_rate and detection counts
    """
    if detection_results is None or detection_results.empty:
        return {}

    total_points = len(detection_results)
    anomaly_count = 0

    if "is_anomaly" in detection_results.columns:
        anomaly_count = detection_results["is_anomaly"].sum()

    anomaly_rate = (anomaly_count / total_points * 100) if total_points > 0 else 0.0

    return {
        "anomaly_count": int(anomaly_count),
        "total_points": total_points,
        "anomaly_rate": anomaly_rate,
    }


def _build_anomaly_evals(
    anomaly_run: AnomalyDetectionRun,
) -> Optional["AnomalyTrainingEvals"]:
    """Build AnomalyTrainingEvals from AnomalyDetectionRun.

    Note: AnomalyTrainingRun requires precision, recall, and f1_score which
    require ground truth labels to compute. Since we don't have ground truth
    in the Model Override context, we return None if we can't compute proper
    classification metrics. Only basic metadata (sample counts, timestamps,
    anomaly_rate) can be captured without ground truth.

    Args:
        anomaly_run: The anomaly detection run with results

    Returns:
        AnomalyTrainingEvals or None if:
        - observe-models not available
        - no ground truth metrics available (precision/recall/f1 required)
    """
    if not HAS_OBSERVE_MODELS or AnomalyTrainingEvals is None:
        return None

    # AnomalyTrainingRun requires precision, recall, and f1_score as required fields.
    # These metrics require ground truth labels which we don't have in this context.
    # We can only build evals if we have classification metrics from ground truth.
    #
    # For now, we return None since we don't have ground truth.
    # In the future, we could:
    # 1. Pass ground truth from the Anomaly Comparison page if available
    # 2. Use placeholder values (not recommended - misleading)
    # 3. Store only the metadata without the evals
    return None


def _load_preprocessing_config_from_cache(
    preprocessing_id: str, hostname: Optional[str]
) -> Optional[dict[str, Any]]:
    """Load preprocessing config from cache metadata.

    Args:
        preprocessing_id: The preprocessing ID/label
        hostname: The endpoint hostname

    Returns:
        Preprocessing config dict or None if not found
    """
    if not hostname or not preprocessing_id:
        return None

    try:
        cache = EndpointCache(hostname)
        metadata = cache.get_preprocessing_metadata(preprocessing_id)
        if metadata and "preprocessing_config" in metadata:
            return metadata["preprocessing_config"]
    except Exception:
        pass

    return None


def _extract_configs_from_anomaly_run(
    anomaly_run: AnomalyDetectionRun,
    training_runs: dict[str, TrainingRun],
    horizon_periods: int = 7,
) -> dict[str, Any]:
    """Extract all configs from an anomaly detection run.

    Args:
        anomaly_run: The selected anomaly detection run
        training_runs: Dict of available training runs
        horizon_periods: Number of periods to generate future predictions

    Returns:
        Dict with all extracted configs and metadata
    """
    # Try to generate future predictions (for forecast-based models)
    future_predictions = _generate_future_predictions(anomaly_run, horizon_periods)

    # Prepare predictions for save (rename columns, convert timestamps)
    predictions_df = None
    predictions_type = None  # Track what type of predictions we have

    if future_predictions is not None:
        predictions_df = _prepare_predictions_for_save(future_predictions)
        predictions_type = "future"
    elif (
        anomaly_run.detection_results is not None
        and not anomaly_run.detection_results.empty
    ):
        # For standalone models (like DeepSVDD), use detection_results
        # which contains detection bands from the test period
        predictions_df = _prepare_predictions_for_save(anomaly_run.detection_results)
        predictions_type = "historical"

    result: dict[str, Any] = {
        "anomaly_run": anomaly_run,
        "forecast_run": None,
        # Model identification
        "anomaly_model_name": None,
        "anomaly_model_version": None,
        "forecast_model_name": None,
        "forecast_model_version": None,
        # Config objects
        "anomaly_config": None,
        "forecast_config": None,
        "preprocessing_config_dict": None,
        # Evals
        "anomaly_evals": None,
        "forecast_evals": None,
        # Serialized JSONs
        "anomaly_config_json": None,
        "forecast_config_json": None,
        "preprocessing_config_json": None,
        "anomaly_evals_json": None,
        "forecast_evals_json": None,
        # Predictions (future or historical detection results)
        "predictions_df": predictions_df,
        "predictions_type": predictions_type,  # "future" or "historical"
    }

    # Get hostname for cache access
    hostname = st.session_state.get(_SELECTED_ENDPOINT)

    # Extract anomaly model info
    anomaly_name, anomaly_version = _parse_model_name_version(
        anomaly_run.anomaly_model_key, anomaly_run.anomaly_model_name
    )
    result["anomaly_model_name"] = anomaly_name
    result["anomaly_model_version"] = anomaly_version

    # Extract anomaly config from model
    anomaly_config_dict = get_model_hyperparameters(anomaly_run.model)
    # Skip if result only contains "note" (no actual hyperparameters)
    if (
        anomaly_config_dict
        and "note" not in anomaly_config_dict
        and HAS_OBSERVE_MODELS
        and AnomalyModelConfig is not None
    ):
        try:
            result["anomaly_config"] = AnomalyModelConfig(
                hyperparameters=anomaly_config_dict
            )
        except Exception:
            pass

    # Build anomaly evals
    result["anomaly_evals"] = _build_anomaly_evals(anomaly_run)

    # Determine the preprocessing_id to use
    preprocessing_id: Optional[str] = None

    # Get forecast run if referenced
    forecast_run = None
    if anomaly_run.forecast_run_id and anomaly_run.forecast_run_id in training_runs:
        forecast_run = training_runs[anomaly_run.forecast_run_id]
        result["forecast_run"] = forecast_run

        # Extract forecast model info
        forecast_name, forecast_version = _parse_model_name_version(
            forecast_run.registry_key, forecast_run.model_name
        )
        result["forecast_model_name"] = forecast_name
        result["forecast_model_version"] = forecast_version

        # Extract forecast config from model
        forecast_config_dict = get_model_hyperparameters(forecast_run.model)
        # Skip if result only contains "note" (no actual hyperparameters)
        if (
            forecast_config_dict
            and "note" not in forecast_config_dict
            and HAS_OBSERVE_MODELS
            and ForecastModelConfig is not None
        ):
            try:
                result["forecast_config"] = ForecastModelConfig(
                    hyperparameters=forecast_config_dict
                )
            except Exception:
                pass

        # Build forecast evals
        result["forecast_evals"] = _build_forecast_evals(forecast_run)

        # Get preprocessing_id from forecast run
        preprocessing_id = forecast_run.preprocessing_id

        # First try the training run's cached config dict
        if forecast_run.preprocessing_config_dict:
            result["preprocessing_config_dict"] = forecast_run.preprocessing_config_dict
    else:
        # Standalone model - use anomaly run's preprocessing_id
        preprocessing_id = anomaly_run.preprocessing_id

    # If we don't have the config yet, try loading from cache metadata
    if not result["preprocessing_config_dict"] and preprocessing_id:
        result["preprocessing_config_dict"] = _load_preprocessing_config_from_cache(
            preprocessing_id, hostname
        )

    # Serialize configs if inference utils available
    if HAS_INFERENCE_UTILS:
        # Serialize preprocessing
        if result["preprocessing_config_dict"]:
            try:
                result["preprocessing_config_json"] = json.dumps(
                    result["preprocessing_config_dict"], indent=2
                )
            except Exception:
                pass

        # Serialize forecast config
        if result["forecast_config"] and ForecastConfigSerializer is not None:
            try:
                result["forecast_config_json"] = ForecastConfigSerializer.serialize(
                    result["forecast_config"]
                )
            except Exception:
                pass

        # Serialize anomaly config
        if result["anomaly_config"] and AnomalyConfigSerializer is not None:
            try:
                result["anomaly_config_json"] = AnomalyConfigSerializer.serialize(
                    result["anomaly_config"]
                )
            except Exception:
                pass

        # Serialize forecast evals
        if result["forecast_evals"] and ForecastEvalsSerializer is not None:
            try:
                result["forecast_evals_json"] = ForecastEvalsSerializer.serialize(
                    result["forecast_evals"]
                )
            except Exception:
                pass

        # Serialize anomaly evals
        if result["anomaly_evals"] and AnomalyEvalsSerializer is not None:
            try:
                result["anomaly_evals_json"] = AnomalyEvalsSerializer.serialize(
                    result["anomaly_evals"]
                )
            except Exception:
                pass

    return result


# =============================================================================
# UI Rendering Functions
# =============================================================================


def _get_run_score(run: AnomalyDetectionRun) -> float:
    """Get the score for a run (for sorting).

    Args:
        run: The anomaly detection run

    Returns:
        Score value (higher is better), or -inf if no score
    """
    if hasattr(run.model, "best_score") and run.model.best_score is not None:
        return float(run.model.best_score)
    return float("-inf")


def _build_run_display_name(run: AnomalyDetectionRun) -> str:
    """Build a display name for a run including scores.

    Args:
        run: The anomaly detection run

    Returns:
        Display name with scores appended
    """
    base_name = run.display_name
    scores: list[str] = []

    # Get anomaly count
    if (
        run.detection_results is not None
        and "is_anomaly" in run.detection_results.columns
    ):
        anomaly_count = int(run.detection_results["is_anomaly"].sum())
        scores.append(f"{anomaly_count} anomalies")

    # Get grid search score if available
    if hasattr(run.model, "best_score") and run.model.best_score is not None:
        scores.append(f"score: {run.model.best_score:.3f}")

    if scores:
        return f"{base_name} [{', '.join(scores)}]"
    return base_name


def _render_run_selection(
    anomaly_runs: dict[str, AnomalyDetectionRun],
) -> Optional[AnomalyDetectionRun]:
    """Render the run selection dropdown.

    Args:
        anomaly_runs: Available anomaly detection runs

    Returns:
        Selected run or None
    """
    if not anomaly_runs:
        st.warning(
            "No anomaly detection runs available. "
            "Please train some models on the **Anomaly Comparison** page first."
        )
        return None

    # Sort runs by score (highest first)
    sorted_run_ids = sorted(
        anomaly_runs.keys(),
        key=lambda run_id: _get_run_score(anomaly_runs[run_id]),
        reverse=True,
    )

    # Build options with display names including scores (in sorted order)
    run_options = {
        run_id: _build_run_display_name(anomaly_runs[run_id])
        for run_id in sorted_run_ids
    }

    # Get previously selected run
    prev_selected = st.session_state.get(_MODEL_OVERRIDE_SELECTED_RUN)
    default_idx = 0
    if prev_selected and prev_selected in run_options:
        default_idx = sorted_run_ids.index(prev_selected)

    selected_id = st.selectbox(
        "Select Anomaly Detection Run",
        options=sorted_run_ids,
        format_func=lambda x: run_options[x],
        index=default_idx,
        help="Choose a trained anomaly model run to save to the assertion (sorted by score, highest first)",
    )

    if selected_id:
        st.session_state[_MODEL_OVERRIDE_SELECTED_RUN] = selected_id
        return anomaly_runs[selected_id]

    return None


def _render_target_assertion(assertion_urn: Optional[str]) -> bool:
    """Render the target assertion section.

    Args:
        assertion_urn: The selected assertion URN

    Returns:
        True if valid assertion selected
    """
    st.subheader("Target Assertion")

    if not assertion_urn:
        st.warning(
            "No assertion selected. Please select an assertion to save model configuration."
        )
        # Import here to avoid circular import
        from ..timeseries_explorer import metric_cube_browser_page

        if st.button("📈 Go to Metric Cube Events", use_container_width=True):
            st.switch_page(metric_cube_browser_page)
        return False

    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.text_input(
            "Assertion URN",
            value=assertion_urn,
            disabled=True,
            help="The assertion where model config will be saved",
        )
    with col2:
        # Get DataHub URL for link
        datahub_url = _get_datahub_url(assertion_urn)
        if datahub_url:
            st.markdown(f"[View in DataHub]({datahub_url})")
    with col3:
        # Button to change assertion
        from ..timeseries_explorer import metric_cube_browser_page

        if st.button("Change", help="Select a different assertion"):
            st.switch_page(metric_cube_browser_page)

    return True


def _render_config_preview(extracted: dict[str, Any]) -> None:
    """Render the configuration preview section.

    Args:
        extracted: Extracted configs from the selected run
    """
    st.subheader("Configuration Preview")

    # Model identification summary (Anomaly first, then Forecast - consistent with Anomaly Comparison)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Anomaly Model**")
        version_str = (
            f" ({extracted['anomaly_model_version']})"
            if extracted["anomaly_model_version"]
            else ""
        )
        st.info(f"{extracted['anomaly_model_name'] or 'Unknown'}{version_str}")

    with col2:
        st.markdown("**Forecast Model**")
        if extracted["forecast_model_name"]:
            version_str = (
                f" ({extracted['forecast_model_version']})"
                if extracted["forecast_model_version"]
                else ""
            )
            st.info(f"{extracted['forecast_model_name']}{version_str}")
        else:
            st.info("_Standalone (no forecast model)_")

    # Config tabs (Anomaly before Forecast - consistent with Anomaly Comparison)
    tab_names = [
        "Preprocessing",
        "Anomaly Model",
        "Forecast Model",
        "Anomaly Evals",
        "Forecast Evals",
    ]
    tabs = st.tabs(tab_names)

    with tabs[0]:  # Preprocessing
        if extracted["preprocessing_config_json"]:
            st.code(extracted["preprocessing_config_json"], language="json")
        elif extracted["preprocessing_config_dict"]:
            st.code(
                json.dumps(extracted["preprocessing_config_dict"], indent=2),
                language="json",
            )
        else:
            st.warning("No preprocessing configuration available")

    with tabs[1]:  # Anomaly Model
        if extracted["anomaly_config_json"]:
            st.code(extracted["anomaly_config_json"], language="json")
        elif extracted["anomaly_config"]:
            st.code(str(extracted["anomaly_config"]), language="python")
        else:
            st.warning("No anomaly model configuration available")

    with tabs[2]:  # Forecast Model
        if extracted["forecast_config_json"]:
            st.code(extracted["forecast_config_json"], language="json")
        elif extracted["forecast_config"]:
            st.code(str(extracted["forecast_config"]), language="python")
        else:
            st.info("No forecast model configuration (standalone anomaly model)")

    with tabs[3]:  # Anomaly Evals
        if extracted["anomaly_evals_json"]:
            st.code(extracted["anomaly_evals_json"], language="json")
        elif extracted["anomaly_evals"]:
            st.code(str(extracted["anomaly_evals"]), language="python")
        else:
            st.info(
                "Anomaly evaluation metrics require ground truth labels "
                "(precision, recall, F1). These are computed on the Anomaly Comparison "
                "page when ground truth anomaly events are available."
            )

    with tabs[4]:  # Forecast Evals
        if extracted["forecast_evals_json"]:
            st.code(extracted["forecast_evals_json"], language="json")
        elif extracted["forecast_evals"]:
            st.code(str(extracted["forecast_evals"]), language="python")
        else:
            st.info("No forecast evaluation metrics available")


def _render_predictions_preview(
    predictions_df: Optional[pd.DataFrame], predictions_type: Optional[str] = None
) -> None:
    """Render the predictions DataFrame preview.

    Args:
        predictions_df: The predictions/detection results DataFrame
        predictions_type: "future" for forecast-based models, "historical" for standalone
    """
    if predictions_type == "future":
        st.subheader("Future Predictions Preview")
    else:
        st.subheader("Detection Results Preview")

    if predictions_df is None or predictions_df.empty:
        st.warning("No predictions or detection results available.")
        return

    if predictions_type == "future":
        st.info(
            "📈 **Future Predictions**: These are predictions for future time periods. "
            "The `y` column is empty (unknown) since we haven't observed these values yet."
        )
    else:
        st.info(
            "📊 **Historical Detection Results**: These are detection results from the test period. "
            "For standalone models (like DeepSVDD), detection bands are computed on observed data."
        )

    # Summary metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        period_label = (
            "Prediction Periods" if predictions_type == "future" else "Data Points"
        )
        st.metric(period_label, len(predictions_df))

    with col2:
        # Handle both ds and timestamp_ms columns
        if "ds" in predictions_df.columns:
            start_date = pd.to_datetime(predictions_df["ds"].min())
            end_date = pd.to_datetime(predictions_df["ds"].max())
            date_range = (
                f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            )
            st.metric("Date Range", date_range)
        elif "timestamp_ms" in predictions_df.columns:
            start_date = pd.to_datetime(predictions_df["timestamp_ms"].min(), unit="ms")
            end_date = pd.to_datetime(predictions_df["timestamp_ms"].max(), unit="ms")
            date_range = (
                f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            )
            st.metric("Date Range", date_range)

    with col3:
        # Show if detection bands are present
        has_bands = (
            "detection_band_lower" in predictions_df.columns
            or "detection_lower" in predictions_df.columns
        )
        st.metric("Detection Bands", "✓ Present" if has_bands else "✗ Missing")

    # Column summary - include both old and new column names
    available_cols = list(predictions_df.columns)
    expected_cols = [
        "timestamp_ms",
        "ds",
        "y",
        "yhat",
        "yhat_lower",
        "yhat_upper",
        "detection_band_lower",  # New name for inference_utils
        "detection_band_upper",  # New name for inference_utils
        "detection_lower",  # Original observe-models name
        "detection_upper",  # Original observe-models name
        "is_anomaly",
        "anomaly_score",
    ]
    present_cols = [c for c in expected_cols if c in available_cols]

    st.caption(f"**Columns present:** {', '.join(present_cols)}")

    # Sample data
    with st.expander("Show sample data (first 10 rows)", expanded=False):
        # Prioritize columns for display
        display_cols = [c for c in predictions_df.columns if c in expected_cols]
        if display_cols:
            st.dataframe(
                predictions_df[display_cols].head(10), use_container_width=True
            )
        else:
            st.dataframe(predictions_df.head(10), use_container_width=True)


def _format_change_indicator(old_val: Any, new_val: Any) -> str:
    """Return a change indicator emoji based on value comparison.

    Args:
        old_val: Existing value
        new_val: New value

    Returns:
        Emoji indicating change status
    """
    if old_val == new_val:
        return "➖"  # No change
    elif old_val is None or old_val == "N/A":
        return "🆕"  # New value added
    elif new_val is None or new_val == "N/A":
        return "🗑️"  # Value removed
    else:
        return "🔄"  # Value changed


def _render_comparison(
    existing_data: Optional[dict[str, Any]], extracted: dict[str, Any]
) -> None:
    """Render comparison between existing and new configs.

    Args:
        existing_data: Existing inference data (if any)
        extracted: Newly extracted configs
    """
    st.subheader("Comparison with Existing Data")

    if not existing_data:
        st.info(
            "No existing inference data for this assertion. This will be a new save."
        )
        return

    st.warning(
        "⚠️ Existing data found. Saving will overwrite the current configuration."
    )

    # Extract existing values
    existing_config = existing_data.get("model_config", {}) or {}
    existing_forecast = existing_config.get("forecast_model_name", "N/A")
    existing_forecast_ver = existing_config.get("forecast_model_version", "N/A")
    existing_anomaly = existing_config.get("anomaly_model_name", "N/A")
    existing_anomaly_ver = existing_config.get("anomaly_model_version", "N/A")
    existing_generated = existing_data.get("generated_at")

    # Extract new values
    new_forecast = extracted.get("forecast_model_name") or "N/A"
    new_forecast_ver = extracted.get("forecast_model_version") or "N/A"
    new_anomaly = extracted.get("anomaly_model_name") or "N/A"
    new_anomaly_ver = extracted.get("anomaly_model_version") or "N/A"

    # Build comparison table data (Anomaly first, then Forecast - consistent with Anomaly Comparison)
    comparison_data = []

    comparison_data.append(
        {
            "Field": "Anomaly Model",
            "Existing": f"{existing_anomaly} ({existing_anomaly_ver})",
            "New": f"{new_anomaly} ({new_anomaly_ver})",
            "Status": _format_change_indicator(
                f"{existing_anomaly} ({existing_anomaly_ver})",
                f"{new_anomaly} ({new_anomaly_ver})",
            ),
        }
    )

    comparison_data.append(
        {
            "Field": "Forecast Model",
            "Existing": f"{existing_forecast} ({existing_forecast_ver})",
            "New": f"{new_forecast} ({new_forecast_ver})",
            "Status": _format_change_indicator(
                f"{existing_forecast} ({existing_forecast_ver})",
                f"{new_forecast} ({new_forecast_ver})",
            ),
        }
    )

    # Preprocessing comparison
    existing_preproc = "Yes" if existing_data.get("preprocessing_config_json") else "No"
    new_preproc = (
        "Yes"
        if extracted.get("preprocessing_config_json")
        or extracted.get("preprocessing_config_dict")
        else "No"
    )
    comparison_data.append(
        {
            "Field": "Preprocessing Config",
            "Existing": existing_preproc,
            "New": new_preproc,
            "Status": _format_change_indicator(existing_preproc, new_preproc),
        }
    )

    # Anomaly config comparison (Anomaly before Forecast)
    existing_an_config = "Yes" if existing_data.get("anomaly_config_json") else "No"
    new_an_config = "Yes" if extracted.get("anomaly_config_json") else "No"
    comparison_data.append(
        {
            "Field": "Anomaly Config",
            "Existing": existing_an_config,
            "New": new_an_config,
            "Status": _format_change_indicator(existing_an_config, new_an_config),
        }
    )

    # Forecast config comparison
    existing_fc_config = "Yes" if existing_data.get("forecast_config_json") else "No"
    new_fc_config = "Yes" if extracted.get("forecast_config_json") else "No"
    comparison_data.append(
        {
            "Field": "Forecast Config",
            "Existing": existing_fc_config,
            "New": new_fc_config,
            "Status": _format_change_indicator(existing_fc_config, new_fc_config),
        }
    )

    # Anomaly evals comparison (Anomaly before Forecast)
    existing_an_evals = "Yes" if existing_data.get("anomaly_evals_json") else "No"
    new_an_evals = "Yes" if extracted.get("anomaly_evals_json") else "No"
    comparison_data.append(
        {
            "Field": "Anomaly Evaluations",
            "Existing": existing_an_evals,
            "New": new_an_evals,
            "Status": _format_change_indicator(existing_an_evals, new_an_evals),
        }
    )

    # Forecast evals comparison
    existing_fc_evals = "Yes" if existing_data.get("forecast_evals_json") else "No"
    new_fc_evals = "Yes" if extracted.get("forecast_evals_json") else "No"
    comparison_data.append(
        {
            "Field": "Forecast Evaluations",
            "Existing": existing_fc_evals,
            "New": new_fc_evals,
            "Status": _format_change_indicator(existing_fc_evals, new_fc_evals),
        }
    )

    # Predictions comparison
    existing_preds = existing_data.get("predictions_df")
    existing_preds_count = len(existing_preds) if existing_preds is not None else 0
    new_preds = extracted.get("predictions_df")
    new_preds_count = len(new_preds) if new_preds is not None else 0
    comparison_data.append(
        {
            "Field": "Predictions",
            "Existing": f"{existing_preds_count} rows"
            if existing_preds_count
            else "No",
            "New": f"{new_preds_count} rows" if new_preds_count else "No",
            "Status": _format_change_indicator(existing_preds_count, new_preds_count),
        }
    )

    # Generated timestamp
    if existing_generated:
        gen_time = datetime.fromtimestamp(existing_generated / 1000)
        existing_gen_str = gen_time.strftime("%Y-%m-%d %H:%M")
    else:
        existing_gen_str = "N/A"
    comparison_data.append(
        {
            "Field": "Generated At",
            "Existing": existing_gen_str,
            "New": "Now",
            "Status": "🔄",  # Always changing
        }
    )

    # Display comparison table
    comparison_df = pd.DataFrame(comparison_data)
    st.dataframe(
        comparison_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Field": st.column_config.TextColumn("Field", width="medium"),
            "Existing": st.column_config.TextColumn(
                "Current (Existing)", width="medium"
            ),
            "New": st.column_config.TextColumn("New (To Be Saved)", width="medium"),
            "Status": st.column_config.TextColumn("Change", width="small"),
        },
    )

    # Legend
    st.caption("**Legend:** ➖ No change | 🔄 Changed | 🆕 Added | 🗑️ Removed")

    # Count changes
    changes = sum(1 for row in comparison_data if row["Status"] not in ["➖"])
    if changes > 0:
        st.markdown(f"**{changes} field(s) will be updated**")

    # Show detailed diff for configs if changed
    with st.expander("🔍 Detailed Configuration Comparison", expanded=False):
        _render_detailed_config_diff(existing_data, extracted)


def _render_detailed_config_diff(
    existing_data: dict[str, Any], extracted: dict[str, Any]
) -> None:
    """Render detailed side-by-side JSON comparison of configurations.

    Args:
        existing_data: Existing inference data
        extracted: Newly extracted configs
    """
    # Tabs ordered: Preprocessing, Anomaly, Forecast (consistent with Anomaly Comparison)
    tabs = st.tabs(
        [
            "Preprocessing",
            "Anomaly Config",
            "Forecast Config",
            "Anomaly Evals",
            "Forecast Evals",
        ]
    )

    with tabs[0]:
        _render_json_diff(
            "Preprocessing Configuration",
            existing_data.get("preprocessing_config_json"),
            extracted.get("preprocessing_config_json")
            or (
                json.dumps(extracted.get("preprocessing_config_dict"), indent=2)
                if extracted.get("preprocessing_config_dict")
                else None
            ),
        )

    with tabs[1]:
        _render_json_diff(
            "Anomaly Configuration",
            existing_data.get("anomaly_config_json"),
            extracted.get("anomaly_config_json"),
        )

    with tabs[2]:
        _render_json_diff(
            "Forecast Configuration",
            existing_data.get("forecast_config_json"),
            extracted.get("forecast_config_json"),
        )

    with tabs[3]:
        _render_json_diff(
            "Anomaly Evaluations",
            existing_data.get("anomaly_evals_json"),
            extracted.get("anomaly_evals_json"),
        )

    with tabs[4]:
        _render_json_diff(
            "Forecast Evaluations",
            existing_data.get("forecast_evals_json"),
            extracted.get("forecast_evals_json"),
        )


def _render_json_diff(
    title: str, existing_json: Optional[str], new_json: Optional[str]
) -> None:
    """Render side-by-side JSON comparison.

    Args:
        title: Section title
        existing_json: Existing JSON string
        new_json: New JSON string
    """
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Existing**")
        if existing_json:
            try:
                # Pretty print the JSON
                parsed = json.loads(existing_json)
                st.code(json.dumps(parsed, indent=2), language="json")
            except json.JSONDecodeError:
                st.code(existing_json, language="json")
        else:
            st.caption("_No existing configuration_")

    with col2:
        st.markdown("**New**")
        if new_json:
            try:
                parsed = json.loads(new_json)
                st.code(json.dumps(parsed, indent=2), language="json")
            except json.JSONDecodeError:
                st.code(new_json, language="json")
        else:
            st.caption("_No new configuration_")

    # Show change summary
    if existing_json and new_json:
        try:
            existing_parsed = json.loads(existing_json)
            new_parsed = json.loads(new_json)

            # Find differences in keys
            existing_keys = set(_flatten_dict(existing_parsed).keys())
            new_keys = set(_flatten_dict(new_parsed).keys())

            added_keys = new_keys - existing_keys
            removed_keys = existing_keys - new_keys
            common_keys = existing_keys & new_keys

            # Check for changed values
            existing_flat = _flatten_dict(existing_parsed)
            new_flat = _flatten_dict(new_parsed)
            changed_keys = [
                k for k in common_keys if existing_flat.get(k) != new_flat.get(k)
            ]

            if added_keys or removed_keys or changed_keys:
                changes_summary = []
                if added_keys:
                    changes_summary.append(f"🆕 Added: {len(added_keys)} field(s)")
                if removed_keys:
                    changes_summary.append(f"🗑️ Removed: {len(removed_keys)} field(s)")
                if changed_keys:
                    changes_summary.append(f"🔄 Changed: {len(changed_keys)} field(s)")
                st.caption(" | ".join(changes_summary))
            else:
                st.caption("➖ No changes detected")
        except (json.JSONDecodeError, TypeError):
            pass
    elif existing_json and not new_json:
        st.caption("🗑️ Configuration will be removed")
    elif not existing_json and new_json:
        st.caption("🆕 New configuration will be added")
    else:
        st.caption("➖ No configuration on either side")


def _flatten_dict(d: dict[str, Any], parent_key: str = "") -> dict[str, Any]:
    """Flatten a nested dictionary for comparison.

    Args:
        d: Dictionary to flatten
        parent_key: Parent key prefix

    Returns:
        Flattened dictionary with dot-separated keys
    """
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_dict(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _render_save_section(
    cache: EndpointCache,
    assertion_urn: str,
    extracted: dict[str, Any],
    existing_data: Optional[dict[str, Any]],
) -> None:
    """Render the save confirmation section.

    Args:
        cache: The endpoint cache instance
        assertion_urn: Target assertion URN
        extracted: Extracted configs to save
        existing_data: Existing data (for overwrite warning)
    """
    st.subheader("Save Configuration")

    # Summary of what will be saved
    st.markdown("**What will be saved:**")

    items = []
    if extracted["preprocessing_config_json"] or extracted["preprocessing_config_dict"]:
        items.append("✅ Preprocessing configuration")
    else:
        items.append("⚠️ Preprocessing configuration (missing)")

    if extracted["forecast_config_json"]:
        items.append("✅ Forecast model configuration")
    else:
        items.append("➖ Forecast model configuration (standalone anomaly)")

    if extracted["anomaly_config_json"]:
        items.append("✅ Anomaly model configuration")
    else:
        items.append("⚠️ Anomaly model configuration (missing)")

    if extracted["forecast_evals_json"]:
        items.append("✅ Forecast training evaluations")

    if extracted["anomaly_evals_json"]:
        items.append("✅ Anomaly training evaluations")
    else:
        items.append("➖ Anomaly training evaluations (requires ground truth)")

    predictions_df = extracted.get("predictions_df")
    predictions_type = extracted.get("predictions_type")
    if predictions_df is not None and len(predictions_df) > 0:
        if predictions_type == "future":
            items.append(f"✅ Future Predictions ({len(predictions_df)} periods)")
        else:
            items.append(f"✅ Detection Results ({len(predictions_df)} periods)")
    else:
        items.append("⚠️ Predictions/Detection Results (not available)")

    for item in items:
        st.markdown(f"- {item}")

    # Confirmation checkbox if overwriting
    confirmed = True
    if existing_data:
        confirmed = st.checkbox(
            "I understand this will overwrite existing inference data",
            key=_MODEL_OVERRIDE_CONFIRM_SAVE,
        )

    # Save button
    col1, col2 = st.columns([1, 3])
    with col1:
        save_clicked = st.button(
            "💾 Save Configuration",
            type="primary",
            disabled=not confirmed,
            use_container_width=True,
        )

    if save_clicked:
        _execute_save(cache, assertion_urn, extracted)


def _execute_save(
    cache: EndpointCache, assertion_urn: str, extracted: dict[str, Any]
) -> None:
    """Execute the save operation.

    Args:
        cache: The endpoint cache instance
        assertion_urn: Target assertion URN
        extracted: Extracted configs to save
    """
    with st.spinner("Saving configuration..."):
        try:
            # Build model config dict
            model_config_dict: dict[str, Any] = {}

            if extracted["forecast_model_name"]:
                model_config_dict["forecast_model_name"] = extracted[
                    "forecast_model_name"
                ]
            if extracted["forecast_model_version"]:
                model_config_dict["forecast_model_version"] = extracted[
                    "forecast_model_version"
                ]
            if extracted["anomaly_model_name"]:
                model_config_dict["anomaly_model_name"] = extracted[
                    "anomaly_model_name"
                ]
            if extracted["anomaly_model_version"]:
                model_config_dict["anomaly_model_version"] = extracted[
                    "anomaly_model_version"
                ]

            # Prepare preprocessing JSON
            preprocessing_json = extracted.get("preprocessing_config_json")
            if not preprocessing_json and extracted.get("preprocessing_config_dict"):
                preprocessing_json = json.dumps(extracted["preprocessing_config_dict"])

            generated_at = int(time.time() * 1000)

            # Save using cache
            success = cache.save_inference_data(
                entity_urn=assertion_urn,
                model_config_dict=model_config_dict if model_config_dict else None,
                preprocessing_config_json=preprocessing_json,
                forecast_config_json=extracted.get("forecast_config_json"),
                anomaly_config_json=extracted.get("anomaly_config_json"),
                forecast_evals_json=extracted.get("forecast_evals_json"),
                anomaly_evals_json=extracted.get("anomaly_evals_json"),
                predictions_df=extracted.get("predictions_df"),
                generated_at=generated_at,
            )

            if success:
                st.success(
                    f"✅ Configuration saved successfully to `{_shorten_urn(assertion_urn)}`"
                )
                # Clear confirmation checkbox
                st.session_state.pop(_MODEL_OVERRIDE_CONFIRM_SAVE, None)

                # Reload the saved data to populate cache
                reloaded = cache.load_inference_data(assertion_urn)
                if reloaded:
                    st.info("📥 Cache refreshed with saved configuration")
                    logger.info(
                        "Reloaded inference data for %s after save", assertion_urn
                    )
            else:
                st.error("Failed to save configuration. Check logs for details.")

        except Exception as e:
            st.error(f"Error saving configuration: {e}")


# =============================================================================
# Main Page Render Function
# =============================================================================


def render_model_override_page() -> None:
    """Render the Model Override page."""
    st.title("Model Override")
    st.markdown(
        """
        Save trained model configurations from the **Anomaly Comparison** page
        back to the selected assertion's inference data store.
        """
    )

    # Initialize state
    init_explorer_state()

    # Check dependencies
    if not HAS_OBSERVE_MODELS:
        st.error(
            "⚠️ The `observe-models` package is not installed. "
            "Model configuration extraction requires this package."
        )
        return

    if not HAS_INFERENCE_UTILS:
        st.warning(
            "⚠️ Inference utilities not available. "
            "Some serialization features may be limited."
        )

    # Get current endpoint and assertion
    hostname = st.session_state.get(_SELECTED_ENDPOINT)
    assertion_urn = st.session_state.get(_SELECTED_ASSERTION)

    if not hostname:
        st.warning(
            "No endpoint selected. Please select an endpoint on the **Monitor Browser** page."
        )
        return

    # Validate target assertion
    if not _render_target_assertion(assertion_urn):
        return

    st.divider()

    # Get available runs
    anomaly_runs = _get_anomaly_runs()
    training_runs = _get_training_runs()

    # Run selection
    st.subheader("Select Model Run")
    selected_run = _render_run_selection(anomaly_runs)

    if not selected_run:
        return

    st.divider()

    # Prediction horizon configuration
    st.subheader("Prediction Settings")

    # Infer default horizon based on data frequency
    # Default is always 7 days worth of predictions
    # Use test_df if available and non-empty, otherwise fall back to train_df
    df_for_freq = (
        selected_run.test_df
        if selected_run.test_df is not None and not selected_run.test_df.empty
        else selected_run.train_df
    )
    freq_hours = _infer_frequency_hours(df_for_freq)
    periods_per_day = max(1, int(24 / freq_hours))
    default_horizon = 7 * periods_per_day  # 7 days worth of periods

    if freq_hours <= 1:
        horizon_help = (
            f"Number of periods to predict (~{periods_per_day} per day for hourly data)"
        )
    elif freq_hours <= 24:
        horizon_help = "Number of days to predict into the future"
    else:
        horizon_help = "Number of periods to predict into the future"

    horizon_periods = st.number_input(
        "Prediction Horizon (periods)",
        min_value=1,
        max_value=365 * periods_per_day,
        value=st.session_state.get(_PREDICTION_HORIZON_KEY, default_horizon),
        help=horizon_help,
        key=_PREDICTION_HORIZON_KEY,
    )

    # Show what this means in days
    days_predicted = horizon_periods / periods_per_day
    st.caption(
        f"≈ {days_predicted:.1f} days ({horizon_periods} periods at {freq_hours:.1f}h frequency)"
    )

    st.divider()

    # Extract configs from selected run with future predictions
    extracted = _extract_configs_from_anomaly_run(
        selected_run, training_runs, horizon_periods=int(horizon_periods)
    )

    # Render config preview
    _render_config_preview(extracted)

    st.divider()

    # Render predictions preview
    _render_predictions_preview(
        extracted.get("predictions_df"), extracted.get("predictions_type")
    )

    st.divider()

    # Get cache for loading existing data and saving
    cache = EndpointCache(hostname)
    existing_data = cache.load_inference_data(assertion_urn) if assertion_urn else None

    # Render comparison
    _render_comparison(existing_data, extracted)

    st.divider()

    # Render save section
    if assertion_urn:
        _render_save_section(cache, assertion_urn, extracted, existing_data)


__all__ = [
    "render_model_override_page",
    "AnomalyDetectionRun",
    "_get_anomaly_runs",
]
