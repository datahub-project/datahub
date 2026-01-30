"""
Inference Utilities

This module provides utilities for converting observe-models configurations and
pandas DataFrames into the data model structures used for storing forecast and
anomaly detection results in DataHub.

The data model leverages existing assertion/monitor aspects:
- AssertionEvaluationContext: Container for model metadata and predictions
- AssertionInferenceDetails: Model configuration, hyperparameters, training info
- EmbeddedAssertion: Individual prediction points with bounds

Public API:
===========

Assertion conversion namespaces:
- ForecastAssertions.from_df() / .to_df()
- AnomalyAssertions.from_df() / .to_df()
- FreshnessAssertions.from_df() / .to_df()

Serialization namespaces (in observe_adapter/serialization.py):
- PreprocessingConfigSerializer.serialize() / .deserialize()
- ForecastConfigSerializer.serialize() / .deserialize()
- AnomalyConfigSerializer.serialize() / .deserialize()
- ForecastEvalsSerializer.serialize() / .deserialize()
- AnomalyEvalsSerializer.serialize() / .deserialize()

Field Mappings:
===============

AssertionInferenceDetails:
--------------------------
├── modelId                               ← "observe-models" (package name)
├── modelVersion                          ← observe-models package version
├── confidence                            ← Backward-compat only; mirrors anomalyScore (else forecastScore)
├── generatedAt                           ← When model was trained (millis)
└── parameters: map[string, string]
    ├── "forecastModelName"               ← Registry name (e.g., "prophet")
    ├── "forecastModelVersion"            ← Registry version (e.g., "0.1.0")
    ├── "forecastConfigJson"              ← ForecastModelConfig JSON (with _schemaVersion)
    ├── "anomalyModelName"                ← Registry name (e.g., "datahub_forecast_anomaly")
    ├── "anomalyModelVersion"             ← Registry version (e.g., "0.1.0")
    ├── "anomalyConfigJson"               ← AnomalyModelConfig JSON (with _schemaVersion)
    ├── "preprocessingConfigJson"         ← JSON with _schemaVersion embedded
    ├── "forecastEvalsJson"               ← ForecastTrainingEvals JSON (with _schemaVersion)
    │                                       Contains runs with timestamps and metrics
    └── "anomalyEvalsJson"                ← AnomalyTrainingEvals JSON (with _schemaVersion)
                                            Contains runs with timestamps and metrics

EmbeddedAssertion (Forecast):
-----------------------------
└── evaluationTimeWindow.startTimeMillis  ← timestamp_ms
└── assertion.volumeAssertion.rowCountTotal.parameters
    ├── minValue.value                    ← yhat_lower (also in context)
    └── maxValue.value                    ← yhat_upper (also in context)
└── context: map[string, string]
    ├── "y"                               ← Actual value
    ├── "yhat"                            ← Predicted value
    ├── "yhatLower"                       ← CI lower (same as minValue.value)
    └── "yhatUpper"                       ← CI upper (same as maxValue.value)

EmbeddedAssertion (Anomaly Detection):
--------------------------------------
└── evaluationTimeWindow.startTimeMillis  ← timestamp_ms
└── assertion.volumeAssertion.rowCountTotal.parameters
    ├── minValue.value                    ← detection_band_lower (also in context)
    └── maxValue.value                    ← detection_band_upper (also in context)
└── context: map[string, string]
    ├── "y"                               ← Actual value
    ├── "yhat"                            ← Predicted value
    ├── "yhatLower"                       ← CI lower (from forecast)
    ├── "yhatUpper"                       ← CI upper (from forecast)
    ├── "anomalyScore"                    ← Anomaly score
    ├── "isAnomaly"                       ← Is anomaly flag
    ├── "detectionBandLower"              ← Detection lower (same as minValue.value)
    └── "detectionBandUpper"              ← Detection upper (same as maxValue.value)
"""

import logging
import os
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

import pandas as pd
from pydantic import BaseModel

# Get observe-models package version
try:
    import datahub_observe

    OBSERVE_MODELS_VERSION: str = getattr(datahub_observe, "__version__", "unknown")
except ImportError:
    OBSERVE_MODELS_VERSION = "unknown"

from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInferenceDetailsClass,
    AssertionInfoClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    EmbeddedAssertionClass,
    FixedIntervalScheduleClass,
    FreshnessAssertionInfoClass,
    FreshnessAssertionScheduleClass,
    FreshnessAssertionScheduleTypeClass,
    FreshnessAssertionTypeClass,
    RowCountTotalClass,
    TimeWindowClass,
    TimeWindowSizeClass,
    VolumeAssertionInfoClass,
    VolumeAssertionTypeClass,
)

# Reuse existing utilities
from datahub_executor.common.monitor.inference.utils import create_inference_source

logger = logging.getLogger(__name__)

_INFERENCE_V2_DEFAULT_PREDICTION_HORIZON_DAYS = 7
_ENV_INFERENCE_V2_PREDICTION_HORIZON_DAYS = (
    "DATAHUB_EXECUTOR_INFERENCE_V2_PREDICTION_HORIZON_DAYS"
)
_ENV_INFERENCE_V2_EVAL_TRAIN_RATIO = "DATAHUB_EXECUTOR_INFERENCE_V2_EVAL_TRAIN_RATIO"
_ENV_FORCE_RETUNE_ANOMALY_ONLY = "DATAHUB_EXECUTOR_FORCE_RETUNE_ANOMALY_ONLY"
_ENV_MODEL_PAIRINGS = "DATAHUB_EXECUTOR_MODEL_PAIRINGS"
_ENV_INFERENCE_V2_N_JOBS = "DATAHUB_EXECUTOR_INFERENCE_V2_N_JOBS"


def get_inference_v2_prediction_horizon_days() -> int:
    """Return default inference_v2 prediction horizon in days (ops-configurable)."""
    raw = os.environ.get(_ENV_INFERENCE_V2_PREDICTION_HORIZON_DAYS)
    if raw:
        try:
            v = int(raw)
            if v >= 1:
                return v
        except (TypeError, ValueError):
            pass
    return _INFERENCE_V2_DEFAULT_PREDICTION_HORIZON_DAYS


def get_inference_v2_eval_train_ratio() -> float:
    """Return train ratio for strict holdout evaluation (ops-configurable)."""
    raw = os.environ.get(_ENV_INFERENCE_V2_EVAL_TRAIN_RATIO)
    if raw:
        try:
            v = float(raw)
            # Keep a conservative safety range; avoids empty splits on small datasets.
            if 0.1 <= v <= 0.95:
                return v
        except (TypeError, ValueError):
            pass
    return 0.7


def get_default_prediction_num_intervals(*, interval_hours: int) -> int:
    """Return the default number of future intervals to predict for inference_v2."""
    hours = int(interval_hours)
    if hours <= 0:
        hours = 1
    horizon_days = get_inference_v2_prediction_horizon_days()
    # e.g. 7d @ 1h -> 168 intervals; @ 24h -> 7 intervals.
    return max(1, int((horizon_days * 24) / hours))


def get_force_retune_anomaly_only() -> bool:
    """Return whether force_retune should only retune anomaly model."""
    return os.environ.get(_ENV_FORCE_RETUNE_ANOMALY_ONLY, "false").lower() == "true"


def get_inference_v2_n_jobs() -> Optional[int]:
    """Return number of parallel jobs for model training (ops-configurable).

    Returns:
        Integer number of jobs if set and valid (>= 1), None otherwise.
        When None, the caller should use a default (typically 80% of CPU cores).
    """
    raw = os.environ.get(_ENV_INFERENCE_V2_N_JOBS)
    if raw:
        try:
            v = int(raw)
            if v >= 1:
                return v
        except (TypeError, ValueError):
            pass
    return None


def get_model_pairings_env() -> Optional[str]:
    """Return raw env override for inference_v2 model pairings (if set)."""
    raw = os.environ.get(_ENV_MODEL_PAIRINGS)
    if raw is None:
        return None
    raw = raw.strip()
    return raw or None


def split_time_series_df(
    df: pd.DataFrame, *, train_ratio: float
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a time-series df into train/eval by time order (strict, no shuffling)."""
    if df is None or df.empty:
        raise ValueError("Cannot split empty dataframe")
    if "ds" not in df.columns:
        raise ValueError("DataFrame must contain 'ds' column")

    ratio = float(train_ratio)
    if not (0.0 < ratio < 1.0):
        raise ValueError(f"train_ratio must be in (0, 1), got {ratio}")

    df_sorted = df.sort_values("ds").reset_index(drop=True)
    split_idx = int(len(df_sorted) * ratio)
    train_df = df_sorted.iloc[:split_idx].copy()
    eval_df = df_sorted.iloc[split_idx:].copy()
    if len(train_df) == 0 or len(eval_df) == 0:
        raise ValueError(
            f"Train/eval split produced empty partition: train={len(train_df)}, eval={len(eval_df)}"
        )
    return train_df, eval_df


def prepare_predictions_df_for_persistence(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize prediction df to inference_utils expectations (timestamp_ms + detection bands)."""
    if df is None or df.empty:
        return df

    out = df.copy()

    if "timestamp_ms" not in out.columns and "ds" in out.columns:
        out["timestamp_ms"] = pd.to_datetime(out["ds"]).astype("int64") // 10**6

    # Standardize detection-band column names.
    out = out.rename(
        columns={
            "detection_lower": "detection_band_lower",
            "detection_upper": "detection_band_upper",
        }
    )

    return out


def timestamp_ms_to_ds(
    df: pd.DataFrame, *, timestamp_col: str = "timestamp_ms", ds_col: str = "ds"
) -> pd.DataFrame:
    """Normalize a timestamp-millis column into a pandas datetime `ds` column."""
    if df is None or df.empty:
        return df
    if timestamp_col not in df.columns:
        raise ValueError(f"DataFrame missing '{timestamp_col}' column")
    out = df.copy()
    out[ds_col] = pd.to_datetime(out[timestamp_col], unit="ms")
    return out


# =============================================================================
# Pydantic Models for Model Configuration
# =============================================================================


class ModelConfig(BaseModel):
    """
    Configuration for a trained model.

    This model captures all configuration metadata needed to persist
    a trained model's settings in AssertionInferenceDetails.

    Model tracking aligns with observe-models RegistryEntry fields:
    - name -> forecast_model_name / anomaly_model_name
    - version -> forecast_model_version / anomaly_model_version

    Attributes:
        forecast_model_name: Registry name for forecast model (e.g., "prophet")
        forecast_model_version: Registry version for forecast model (e.g., "0.1.0")
        forecast_config_json: JSON string of ForecastModelConfig
        forecast_evals_json: JSON string of ForecastTrainingEvals
        forecast_score: Normalized forecast model score (0-1)
        anomaly_model_name: Registry name for anomaly model
        anomaly_model_version: Registry version for anomaly model
        anomaly_config_json: JSON string of AnomalyModelConfig
        anomaly_evals_json: JSON string of AnomalyTrainingEvals
        anomaly_score: Normalized anomaly model score (0-1)
        preprocessing_config_json: JSON string of preprocessing config
        generated_at: Timestamp when the model was trained/generated
    """

    # Forecast model info (optional for anomaly-only)
    forecast_model_name: Optional[str] = None
    forecast_model_version: Optional[str] = None
    forecast_config_json: Optional[str] = None
    forecast_evals_json: Optional[str] = None
    forecast_score: Optional[float] = None  # Normalized score (0-1)

    # Anomaly model info (optional for forecast-only)
    anomaly_model_name: Optional[str] = None
    anomaly_model_version: Optional[str] = None
    anomaly_config_json: Optional[str] = None
    anomaly_evals_json: Optional[str] = None
    anomaly_score: Optional[float] = None  # Normalized score (0-1)

    # Shared fields
    preprocessing_config_json: str
    generated_at: Optional[int] = None


# =============================================================================
# Inference Details Builders
# =============================================================================


def build_inference_details(
    model_config: ModelConfig,
    generated_at_millis: Optional[int] = None,
) -> AssertionInferenceDetailsClass:
    """
    Build AssertionInferenceDetailsClass from model configuration.

    Args:
        model_config: The model configuration dataclass
        generated_at_millis: Timestamp when the model was trained.
            If None, uses model_config.generated_at.

    Returns:
        AssertionInferenceDetailsClass ready for persistence
    """
    parameters: Dict[str, str] = {
        "preprocessingConfigJson": model_config.preprocessing_config_json,
    }

    # Forecast model info (optional)
    if model_config.forecast_model_name:
        parameters["forecastModelName"] = model_config.forecast_model_name
    if model_config.forecast_model_version:
        parameters["forecastModelVersion"] = model_config.forecast_model_version
    if model_config.forecast_config_json:
        parameters["forecastConfigJson"] = model_config.forecast_config_json
    if model_config.forecast_evals_json:
        parameters["forecastEvalsJson"] = model_config.forecast_evals_json
    if model_config.forecast_score is not None:
        parameters["forecastScore"] = str(model_config.forecast_score)

    # Anomaly model info (optional)
    if model_config.anomaly_model_name:
        parameters["anomalyModelName"] = model_config.anomaly_model_name
    if model_config.anomaly_model_version:
        parameters["anomalyModelVersion"] = model_config.anomaly_model_version
    if model_config.anomaly_config_json:
        parameters["anomalyConfigJson"] = model_config.anomaly_config_json
    if model_config.anomaly_evals_json:
        parameters["anomalyEvalsJson"] = model_config.anomaly_evals_json
    if model_config.anomaly_score is not None:
        parameters["anomalyScore"] = str(model_config.anomaly_score)

    effective_generated_at = generated_at_millis or model_config.generated_at

    # Backward-compatibility: some downstream readers still rely on
    # AssertionInferenceDetails.confidence. We set it to the anomaly score
    # (preferred), falling back to forecast score when anomaly is absent.
    legacy_confidence: Optional[float] = None
    if model_config.anomaly_score is not None:
        legacy_confidence = model_config.anomaly_score
    elif model_config.forecast_score is not None:
        legacy_confidence = model_config.forecast_score

    return AssertionInferenceDetailsClass(
        modelId="observe-models",
        modelVersion=OBSERVE_MODELS_VERSION,
        confidence=legacy_confidence,
        generatedAt=effective_generated_at,
        parameters=parameters,
    )


def parse_inference_details(
    details: AssertionInferenceDetailsClass,
) -> Optional[ModelConfig]:
    """
    Parse AssertionInferenceDetailsClass back to ModelConfig.

    Args:
        details: The AssertionInferenceDetailsClass to parse

    Returns:
        ModelConfig if parsing succeeds, None otherwise
    """
    if not details.parameters:
        logger.warning("No parameters found in inference details")
        return None

    if details.modelId and details.modelId != "observe-models":
        logger.warning(
            f"Unexpected modelId '{details.modelId}', expected 'observe-models'"
        )

    params = details.parameters
    preprocessing_config_json = params.get("preprocessingConfigJson")

    if not preprocessing_config_json:
        logger.warning(
            "Missing required preprocessingConfigJson in inference details parameters"
        )
        return None

    # Parse scores
    forecast_score = None
    if params.get("forecastScore"):
        try:
            forecast_score = float(params["forecastScore"])
        except (ValueError, TypeError):
            pass

    anomaly_score = None
    if params.get("anomalyScore"):
        try:
            anomaly_score = float(params["anomalyScore"])
        except (ValueError, TypeError):
            pass
    elif details.confidence is not None:
        # Backward-compatibility: older stored details used only `confidence`.
        # Treat it as the anomaly score when anomalyScore isn't present.
        try:
            anomaly_score = float(details.confidence)
        except (ValueError, TypeError):
            pass

    return ModelConfig(
        forecast_model_name=params.get("forecastModelName"),
        forecast_model_version=params.get("forecastModelVersion"),
        forecast_config_json=params.get("forecastConfigJson"),
        forecast_evals_json=params.get("forecastEvalsJson"),
        forecast_score=forecast_score,
        anomaly_model_name=params.get("anomalyModelName"),
        anomaly_model_version=params.get("anomalyModelVersion"),
        anomaly_config_json=params.get("anomalyConfigJson"),
        anomaly_evals_json=params.get("anomalyEvalsJson"),
        anomaly_score=anomaly_score,
        preprocessing_config_json=preprocessing_config_json,
        generated_at=details.generatedAt,
    )


# =============================================================================
# Embedded Assertion Helpers (private)
# =============================================================================


def _build_std_parameters(
    min_value: float, max_value: float
) -> AssertionStdParametersClass:
    """Build standard parameters for min/max bounds."""
    return AssertionStdParametersClass(
        minValue=AssertionStdParameterClass(type="NUMBER", value=str(min_value)),
        maxValue=AssertionStdParameterClass(type="NUMBER", value=str(max_value)),
    )


def _build_volume_assertion_info(
    entity_urn: str,
    min_value: float,
    max_value: float,
) -> VolumeAssertionInfoClass:
    """Build a volume assertion info with bounds."""
    return VolumeAssertionInfoClass(
        type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
        entity=entity_urn,
        rowCountTotal=RowCountTotalClass(
            operator=AssertionStdOperatorClass.BETWEEN,
            parameters=_build_std_parameters(min_value, max_value),
        ),
    )


def _create_embedded_assertion(
    assertion_info: AssertionInfoClass,
    timestamp_ms: int,
    window_size_seconds: int,
    context: Optional[Dict[str, str]],
) -> EmbeddedAssertionClass:
    """Create an EmbeddedAssertionClass with common fields."""
    return EmbeddedAssertionClass(
        assertion=assertion_info,
        evaluationTimeWindow=TimeWindowClass(
            startTimeMillis=timestamp_ms,
            length=TimeWindowSizeClass(
                unit="SECOND",
                multiple=window_size_seconds,
            ),
        ),
        context=context if context else None,
    )


def _extract_bounds_from_assertion(
    assertion: EmbeddedAssertionClass,
) -> tuple[Optional[float], Optional[float]]:
    """Extract min/max bounds from assertion's volumeAssertion parameters."""
    min_val, max_val = None, None
    if (
        assertion.assertion
        and assertion.assertion.volumeAssertion
        and assertion.assertion.volumeAssertion.rowCountTotal
        and assertion.assertion.volumeAssertion.rowCountTotal.parameters
    ):
        params = assertion.assertion.volumeAssertion.rowCountTotal.parameters
        if params.minValue and params.minValue.value:
            try:
                min_val = float(params.minValue.value)
            except (ValueError, TypeError):
                pass
        if params.maxValue and params.maxValue.value:
            try:
                max_val = float(params.maxValue.value)
            except (ValueError, TypeError):
                pass
    return min_val, max_val


# =============================================================================
# Assertion Conversion Namespace Classes
# =============================================================================


class ForecastAssertions:
    """Namespace for forecast assertion conversions."""

    REQUIRED_COLUMNS = {"timestamp_ms", "yhat_lower", "yhat_upper"}

    @staticmethod
    def from_df(
        df: pd.DataFrame,
        entity_urn: str,
        window_size_seconds: int = 3600,
    ) -> List[EmbeddedAssertionClass]:
        """
        Convert a forecast DataFrame to embedded assertions.

        Expected columns: timestamp_ms, yhat_lower, yhat_upper, y (opt), yhat (opt)

        Args:
            df: DataFrame with forecast predictions
            entity_urn: URN of the entity being forecasted
            window_size_seconds: Evaluation window size

        Returns:
            List of EmbeddedAssertionClass

        Raises:
            ValueError: If required columns are missing
        """
        missing = ForecastAssertions.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        results: List[EmbeddedAssertionClass] = []

        for _, row in df.iterrows():
            context: Dict[str, str] = {}
            if "y" in row and pd.notna(row["y"]):
                context["y"] = str(row["y"])
            if "yhat" in row and pd.notna(row["yhat"]):
                context["yhat"] = str(row["yhat"])
            if pd.notna(row["yhat_lower"]):
                context["yhatLower"] = str(row["yhat_lower"])
            if pd.notna(row["yhat_upper"]):
                context["yhatUpper"] = str(row["yhat_upper"])

            assertion_info = AssertionInfoClass(
                type="VOLUME",
                volumeAssertion=_build_volume_assertion_info(
                    entity_urn, float(row["yhat_lower"]), float(row["yhat_upper"])
                ),
                source=create_inference_source(),
            )

            results.append(
                _create_embedded_assertion(
                    assertion_info,
                    int(row["timestamp_ms"]),
                    window_size_seconds,
                    context,
                )
            )

        return results

    @staticmethod
    def to_df(assertions: List[EmbeddedAssertionClass]) -> pd.DataFrame:
        """
        Convert embedded assertions to a forecast DataFrame.

        Returns DataFrame with: timestamp_ms, y, yhat, yhat_lower, yhat_upper
        """
        records = []

        for assertion in assertions:
            record: Dict[str, Any] = {}

            if assertion.evaluationTimeWindow:
                record["timestamp_ms"] = assertion.evaluationTimeWindow.startTimeMillis

            min_val, max_val = _extract_bounds_from_assertion(assertion)
            if min_val is not None:
                record["yhat_lower"] = min_val
            if max_val is not None:
                record["yhat_upper"] = max_val

            if assertion.context:
                for ctx_key, df_col in [("y", "y"), ("yhat", "yhat")]:
                    if ctx_key in assertion.context:
                        try:
                            record[df_col] = float(assertion.context[ctx_key])
                        except (ValueError, TypeError):
                            pass

            records.append(record)

        return pd.DataFrame(records)


class AnomalyAssertions:
    """Namespace for anomaly assertion conversions."""

    REQUIRED_COLUMNS = {"timestamp_ms", "detection_band_lower", "detection_band_upper"}

    @staticmethod
    def from_df(
        df: pd.DataFrame,
        entity_urn: str,
        window_size_seconds: int = 3600,
    ) -> List[EmbeddedAssertionClass]:
        """
        Convert an anomaly DataFrame to embedded assertions.

        Expected columns: timestamp_ms, detection_band_lower, detection_band_upper,
                         y (opt), yhat (opt), yhat_lower (opt), yhat_upper (opt),
                         anomaly_score (opt), is_anomaly (opt)

        Args:
            df: DataFrame with anomaly detection results
            entity_urn: URN of the entity being monitored
            window_size_seconds: Evaluation window size

        Returns:
            List of EmbeddedAssertionClass

        Raises:
            ValueError: If required columns are missing
        """
        missing = AnomalyAssertions.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        results: List[EmbeddedAssertionClass] = []

        for _, row in df.iterrows():
            context: Dict[str, str] = {}

            # Optional context fields
            if "y" in row and pd.notna(row["y"]):
                context["y"] = str(row["y"])
            if "yhat" in row and pd.notna(row["yhat"]):
                context["yhat"] = str(row["yhat"])
            if "yhat_lower" in row and pd.notna(row["yhat_lower"]):
                context["yhatLower"] = str(row["yhat_lower"])
            if "yhat_upper" in row and pd.notna(row["yhat_upper"]):
                context["yhatUpper"] = str(row["yhat_upper"])
            if "anomaly_score" in row and pd.notna(row["anomaly_score"]):
                context["anomalyScore"] = str(row["anomaly_score"])
            if "is_anomaly" in row and pd.notna(row["is_anomaly"]):
                context["isAnomaly"] = str(row["is_anomaly"]).lower()

            # Detection bands also in context
            if pd.notna(row["detection_band_lower"]):
                context["detectionBandLower"] = str(row["detection_band_lower"])
            if pd.notna(row["detection_band_upper"]):
                context["detectionBandUpper"] = str(row["detection_band_upper"])

            assertion_info = AssertionInfoClass(
                type="VOLUME",
                volumeAssertion=_build_volume_assertion_info(
                    entity_urn,
                    float(row["detection_band_lower"]),
                    float(row["detection_band_upper"]),
                ),
                source=create_inference_source(),
            )

            results.append(
                _create_embedded_assertion(
                    assertion_info,
                    int(row["timestamp_ms"]),
                    window_size_seconds,
                    context,
                )
            )

        return results

    @staticmethod
    def to_df(assertions: List[EmbeddedAssertionClass]) -> pd.DataFrame:
        """
        Convert embedded assertions to an anomaly DataFrame.

        Returns DataFrame with: timestamp_ms, y, yhat, yhat_lower, yhat_upper,
                               detection_band_lower, detection_band_upper,
                               anomaly_score, is_anomaly
        """
        records = []

        for assertion in assertions:
            record: Dict[str, Any] = {}

            if assertion.evaluationTimeWindow:
                record["timestamp_ms"] = assertion.evaluationTimeWindow.startTimeMillis

            min_val, max_val = _extract_bounds_from_assertion(assertion)
            if min_val is not None:
                record["detection_band_lower"] = min_val
            if max_val is not None:
                record["detection_band_upper"] = max_val

            if assertion.context:
                for ctx_key, df_col in [
                    ("y", "y"),
                    ("yhat", "yhat"),
                    ("yhatLower", "yhat_lower"),
                    ("yhatUpper", "yhat_upper"),
                    ("anomalyScore", "anomaly_score"),
                ]:
                    if ctx_key in assertion.context:
                        try:
                            record[df_col] = float(assertion.context[ctx_key])
                        except (ValueError, TypeError):
                            pass

                if "isAnomaly" in assertion.context:
                    record["is_anomaly"] = (
                        assertion.context["isAnomaly"].lower() == "true"
                    )

            records.append(record)

        return pd.DataFrame(records)


class FreshnessAssertions:
    """Namespace for freshness assertion conversions."""

    REQUIRED_COLUMNS = {"timestamp_ms"}

    @staticmethod
    def from_df(
        df: pd.DataFrame,
        entity_urn: str,
        window_size_seconds: int = 3600,
    ) -> List[EmbeddedAssertionClass]:
        """
        Convert a freshness DataFrame to embedded assertions.

        Expected columns: timestamp_ms, expected_next_event_millis (opt), is_fresh (opt)

        Args:
            df: DataFrame with freshness predictions
            entity_urn: URN of the entity being monitored
            window_size_seconds: Evaluation window size

        Returns:
            List of EmbeddedAssertionClass

        Raises:
            ValueError: If required columns are missing
        """
        missing = FreshnessAssertions.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        results: List[EmbeddedAssertionClass] = []

        for _, row in df.iterrows():
            context: Dict[str, str] = {}

            if "expected_next_event_millis" in row and pd.notna(
                row["expected_next_event_millis"]
            ):
                context["expectedNextEventMillis"] = str(
                    int(row["expected_next_event_millis"])
                )
            if "is_fresh" in row and pd.notna(row["is_fresh"]):
                context["isFresh"] = str(row["is_fresh"]).lower()

            freshness_assertion_info = FreshnessAssertionInfoClass(
                type=FreshnessAssertionTypeClass.DATASET_CHANGE,
                entity=entity_urn,
                schedule=FreshnessAssertionScheduleClass(
                    type=FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
                    fixedInterval=FixedIntervalScheduleClass(
                        unit="SECOND",
                        multiple=window_size_seconds,
                    ),
                ),
            )

            assertion_info = AssertionInfoClass(
                type="FRESHNESS",
                freshnessAssertion=freshness_assertion_info,
                source=create_inference_source(),
            )

            results.append(
                _create_embedded_assertion(
                    assertion_info,
                    int(row["timestamp_ms"]),
                    window_size_seconds,
                    context,
                )
            )

        return results

    @staticmethod
    def to_df(assertions: List[EmbeddedAssertionClass]) -> pd.DataFrame:
        """
        Convert embedded assertions to a freshness DataFrame.

        Returns DataFrame with: timestamp_ms, expected_next_event_millis, is_fresh
        """
        records = []

        for assertion in assertions:
            record: Dict[str, Any] = {}

            if assertion.evaluationTimeWindow:
                record["timestamp_ms"] = assertion.evaluationTimeWindow.startTimeMillis

            if assertion.context:
                if "expectedNextEventMillis" in assertion.context:
                    try:
                        record["expected_next_event_millis"] = int(
                            assertion.context["expectedNextEventMillis"]
                        )
                    except (ValueError, TypeError):
                        pass
                if "isFresh" in assertion.context:
                    record["is_fresh"] = assertion.context["isFresh"].lower() == "true"

            records.append(record)

        return pd.DataFrame(records)


# =============================================================================
# Context Builders
# =============================================================================


def build_evaluation_context(
    model_config: ModelConfig,
    embedded_assertions: List[EmbeddedAssertionClass],
    generated_at_millis: Optional[int] = None,
) -> AssertionEvaluationContextClass:
    """
    Build a complete AssertionEvaluationContextClass.

    Args:
        model_config: The model configuration
        embedded_assertions: List of prediction points as embedded assertions
        generated_at_millis: Timestamp when the model was trained.
            If None, uses model_config.generated_at.

    Returns:
        AssertionEvaluationContextClass ready for persistence
    """
    inference_details = build_inference_details(
        model_config=model_config,
        generated_at_millis=generated_at_millis,
    )

    return AssertionEvaluationContextClass(
        embeddedAssertions=embedded_assertions,
        inferenceDetails=inference_details,
    )
