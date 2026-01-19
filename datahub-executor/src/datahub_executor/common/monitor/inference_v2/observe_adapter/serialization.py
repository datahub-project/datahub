"""
Serialization Utilities for observe-models

This module provides serialization utilities for converting observe-models
configurations to/from JSON strings for persistence in DataHub.

These serializers handle the observe-models types that are stored in
AssertionInferenceDetails.parameters. See inference_utils.py for the
complete field mapping documentation.

Public API:
===========

Serialization namespaces:
- PreprocessingConfigSerializer.serialize() / .deserialize()
- ForecastConfigSerializer.serialize() / .deserialize()
- AnomalyConfigSerializer.serialize() / .deserialize()
- ForecastEvalsSerializer.serialize() / .deserialize()
- AnomalyEvalsSerializer.serialize() / .deserialize()

Builder functions:
- build_forecast_training_evals() - Build ForecastTrainingEvals from metrics
- build_anomaly_training_evals() - Build AnomalyTrainingEvals from metrics
- build_model_config() - Build ModelConfig from trained observe-models
"""

import json
import logging
import time
from typing import Any, Callable, Dict, Optional, Type, TypeVar, Union

import pandas as pd

# Top-level imports for observe-models types
# This module lives in observe_adapter/ which has observe-models as a dependency
from datahub_observe.algorithms.anomaly_detection.config import AnomalyModelConfig
from datahub_observe.algorithms.forecasting.config import ForecastModelConfig
from datahub_observe.algorithms.preprocessing.field_metric_preprocessor import (
    FieldMetricPreprocessorConfig,
)
from datahub_observe.algorithms.preprocessing.preprocessor import PreprocessingConfig
from datahub_observe.algorithms.preprocessing.serialization import (
    config_from_json,
    config_to_dict,
    pipeline_config_from_json,
)
from datahub_observe.algorithms.preprocessing.volume_preprocessor import (
    VolumePreprocessorConfig,
)
from datahub_observe.algorithms.training.anomaly_evals import (
    AnomalyTrainingEvals,
    AnomalyTrainingRun,
)
from datahub_observe.algorithms.training.forecast_evals import (
    ForecastTrainingEvals,
    ForecastTrainingRun,
)
from datahub_observe.registry import get_model_registry

from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Type alias for preprocessing config types
PreprocessingConfigTypes = Union[
    PreprocessingConfig, VolumePreprocessorConfig, FieldMetricPreprocessorConfig
]


# =============================================================================
# Generic Serialization Helpers (private)
# =============================================================================


def _serialize(obj: Optional[T], type_name: str) -> Optional[str]:
    """
    Generic serializer for objects with to_json() method.

    Args:
        obj: Object to serialize (must have to_json() method)
        type_name: Name for logging

    Returns:
        JSON string, or None on failure
    """
    if obj is None:
        return None
    try:
        return obj.to_json()  # type: ignore
    except Exception as e:
        logger.warning(f"Failed to serialize {type_name}: {e}")
        return None


def _deserialize(
    json_str: str,
    importer: Callable[[], Type[T]],
    type_name: str,
) -> Optional[Union[T, Dict[str, Any]]]:
    """
    Generic deserializer for classes with from_json() method.

    Args:
        json_str: JSON string to deserialize
        importer: Callable that imports and returns the class (deferred import)
        type_name: Name for logging

    Returns:
        Deserialized object, raw dict as fallback, or None on complete failure
    """
    try:
        cls = importer()
        return cls.from_json(json_str)  # type: ignore
    except ImportError:
        logger.warning(f"observe-models not available for {type_name}")
        return None
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON in {type_name}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Failed to deserialize {type_name}: {e}")
        try:
            return json.loads(json_str)
        except Exception:
            return None


# =============================================================================
# Serialization Namespace Classes
# =============================================================================


class PreprocessingConfigSerializer:
    """Namespace for preprocessing config serialization."""

    @staticmethod
    def serialize(config: PreprocessingConfigTypes) -> Optional[str]:
        """
        Serialize observe-models preprocessing config to JSON.

        Args:
            config: PreprocessingConfig, VolumePreprocessorConfig,
                   or FieldMetricPreprocessorConfig

        Returns:
            JSON string, or None on failure
        """
        if config is None:
            return None
        try:
            return json.dumps(config_to_dict(config))
        except Exception as e:
            logger.warning(f"Failed to serialize preprocessing config: {e}")
            return None

    @staticmethod
    def deserialize(
        json_str: str,
    ) -> Optional[Union[PreprocessingConfigTypes, Dict[str, Any]]]:
        """
        Deserialize preprocessing config from JSON.

        Args:
            json_str: JSON string representation

        Returns:
            Configuration object, raw dict as fallback, or None on failure
        """
        try:
            result = pipeline_config_from_json(json_str)
            if result is not None:
                return result
            return config_from_json(json_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in preprocessing config: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to deserialize preprocessing config: {e}")
            try:
                return json.loads(json_str)
            except Exception:
                return None


class ForecastConfigSerializer:
    """Namespace for ForecastModelConfig serialization."""

    @staticmethod
    def serialize(config: ForecastModelConfig) -> Optional[str]:
        """Serialize ForecastModelConfig to JSON."""
        return _serialize(config, "ForecastModelConfig")

    @staticmethod
    def deserialize(
        json_str: str,
    ) -> Optional[Union[ForecastModelConfig, Dict[str, Any]]]:
        """Deserialize ForecastModelConfig from JSON."""

        def _import() -> Type[ForecastModelConfig]:
            return ForecastModelConfig

        return _deserialize(json_str, _import, "ForecastModelConfig")


class AnomalyConfigSerializer:
    """Namespace for AnomalyModelConfig serialization."""

    @staticmethod
    def serialize(config: AnomalyModelConfig) -> Optional[str]:
        """Serialize AnomalyModelConfig to JSON."""
        return _serialize(config, "AnomalyModelConfig")

    @staticmethod
    def deserialize(
        json_str: str,
    ) -> Optional[Union[AnomalyModelConfig, Dict[str, Any]]]:
        """Deserialize AnomalyModelConfig from JSON."""

        def _import() -> Type[AnomalyModelConfig]:
            return AnomalyModelConfig

        return _deserialize(json_str, _import, "AnomalyModelConfig")


class ForecastEvalsSerializer:
    """Namespace for ForecastTrainingEvals serialization."""

    @staticmethod
    def serialize(evals: ForecastTrainingEvals) -> Optional[str]:
        """Serialize ForecastTrainingEvals to JSON."""
        return _serialize(evals, "ForecastTrainingEvals")

    @staticmethod
    def deserialize(
        json_str: str,
    ) -> Optional[Union[ForecastTrainingEvals, Dict[str, Any]]]:
        """Deserialize ForecastTrainingEvals from JSON."""

        def _import() -> Type[ForecastTrainingEvals]:
            return ForecastTrainingEvals

        return _deserialize(json_str, _import, "ForecastTrainingEvals")


class AnomalyEvalsSerializer:
    """Namespace for AnomalyTrainingEvals serialization."""

    @staticmethod
    def serialize(evals: AnomalyTrainingEvals) -> Optional[str]:
        """Serialize AnomalyTrainingEvals to JSON."""
        return _serialize(evals, "AnomalyTrainingEvals")

    @staticmethod
    def deserialize(
        json_str: str,
    ) -> Optional[Union[AnomalyTrainingEvals, Dict[str, Any]]]:
        """Deserialize AnomalyTrainingEvals from JSON."""

        def _import() -> Type[AnomalyTrainingEvals]:
            return AnomalyTrainingEvals

        return _deserialize(json_str, _import, "AnomalyTrainingEvals")


# =============================================================================
# Training Evals Builders
# =============================================================================


def build_forecast_training_evals(
    metrics: Dict[str, float],
    train_df: Optional[pd.DataFrame] = None,
    eval_df: Optional[pd.DataFrame] = None,
) -> Optional[ForecastTrainingEvals]:
    """
    Build ForecastTrainingEvals from evaluation metrics and DataFrames.

    This creates a properly structured ForecastTrainingEvals object with
    training run metadata (sample counts, timestamp ranges) that matches
    the format used by model_override.py for consistency.

    Args:
        metrics: Dict of evaluation metrics (mae, rmse, mape, coverage).
        train_df: Training DataFrame with 'ds' column for timestamp ranges.
        eval_df: Evaluation/test DataFrame with 'ds' column for timestamp ranges.

    Returns:
        ForecastTrainingEvals object, or None if observe-models not available.
    """
    # Extract timestamp ranges from DataFrames
    train_start_millis: Optional[int] = None
    train_end_millis: Optional[int] = None
    train_samples: Optional[int] = None

    if train_df is not None and len(train_df) > 0:
        train_samples = len(train_df)
        if "ds" in train_df.columns:
            try:
                train_start_millis = int(
                    pd.to_datetime(train_df["ds"].min()).timestamp() * 1000
                )
                train_end_millis = int(
                    pd.to_datetime(train_df["ds"].max()).timestamp() * 1000
                )
            except Exception as e:
                logger.warning(f"Failed to extract train timestamps: {e}")

    test_start_millis: Optional[int] = None
    test_end_millis: Optional[int] = None
    test_samples: Optional[int] = None

    if eval_df is not None and len(eval_df) > 0:
        test_samples = len(eval_df)
        if "ds" in eval_df.columns:
            try:
                test_start_millis = int(
                    pd.to_datetime(eval_df["ds"].min()).timestamp() * 1000
                )
                test_end_millis = int(
                    pd.to_datetime(eval_df["ds"].max()).timestamp() * 1000
                )
            except Exception as e:
                logger.warning(f"Failed to extract eval timestamps: {e}")

    # Build the training run with metrics and metadata
    # Note: metrics keys are lowercase (mae, rmse, mape) from evaluate()
    run = ForecastTrainingRun(
        mae=metrics.get("mae", 0.0) or 0.0,
        rmse=metrics.get("rmse", 0.0) or 0.0,
        mape=metrics.get("mape", 0.0) or 0.0,
        train_samples=train_samples,
        test_samples=test_samples,
        train_start_millis=train_start_millis,
        train_end_millis=train_end_millis,
        test_start_millis=test_start_millis,
        test_end_millis=test_end_millis,
    )

    evals = ForecastTrainingEvals(runs=[run])
    evals.compute_aggregated()
    return evals


def build_anomaly_training_evals(
    metrics: Dict[str, float],
    train_df: Optional[pd.DataFrame] = None,
    eval_df: Optional[pd.DataFrame] = None,
) -> Optional[AnomalyTrainingEvals]:
    """
    Build AnomalyTrainingEvals from evaluation metrics and DataFrames.

    This creates a properly structured AnomalyTrainingEvals object with
    training run metadata (sample counts, timestamp ranges).

    NOTE: This function only builds evals when ground truth metrics are present.
    Without ground_truth during evaluation, anomaly_model.evaluate() returns
    {'anomaly_rate', 'mean_score', ...} which cannot populate AnomalyTrainingEvals.
    With ground_truth, it returns {'precision', 'recall', 'f1_score', 'accuracy'}.

    Args:
        metrics: Dict of evaluation metrics. Must contain 'precision', 'recall',
            and 'f1_score' keys (from evaluate() with ground_truth).
        train_df: Training DataFrame with 'ds' column for timestamp ranges.
        eval_df: Evaluation/test DataFrame with 'ds' column for timestamp ranges.

    Returns:
        AnomalyTrainingEvals object, or None if:
        - observe-models not available
        - Required ground truth metrics (precision, recall, f1_score) not present
    """
    # Check if we have ground truth metrics (precision, recall, f1_score)
    # Without these, we can't build AnomalyTrainingEvals
    required_keys = {"precision", "recall", "f1_score"}
    if not required_keys.issubset(metrics.keys()):
        # These metrics are only available when ground_truth was provided
        # to anomaly_model.evaluate(). Without ground_truth, we get
        # {'anomaly_rate', 'mean_score', ...} instead.
        return None

    # Extract timestamp ranges from DataFrames
    train_start_millis: Optional[int] = None
    train_end_millis: Optional[int] = None
    train_samples: Optional[int] = None

    if train_df is not None and len(train_df) > 0:
        train_samples = len(train_df)
        if "ds" in train_df.columns:
            try:
                train_start_millis = int(
                    pd.to_datetime(train_df["ds"].min()).timestamp() * 1000
                )
                train_end_millis = int(
                    pd.to_datetime(train_df["ds"].max()).timestamp() * 1000
                )
            except Exception as e:
                logger.warning(f"Failed to extract train timestamps: {e}")

    test_start_millis: Optional[int] = None
    test_end_millis: Optional[int] = None
    test_samples: Optional[int] = None

    if eval_df is not None and len(eval_df) > 0:
        test_samples = len(eval_df)
        if "ds" in eval_df.columns:
            try:
                test_start_millis = int(
                    pd.to_datetime(eval_df["ds"].min()).timestamp() * 1000
                )
                test_end_millis = int(
                    pd.to_datetime(eval_df["ds"].max()).timestamp() * 1000
                )
            except Exception as e:
                logger.warning(f"Failed to extract eval timestamps: {e}")

    # Build the training run with metrics and metadata
    run = AnomalyTrainingRun(
        precision=metrics.get("precision", 0.0) or 0.0,
        recall=metrics.get("recall", 0.0) or 0.0,
        f1_score=metrics.get("f1_score", 0.0) or 0.0,
        train_samples=train_samples,
        test_samples=test_samples,
        train_start_millis=train_start_millis,
        train_end_millis=train_end_millis,
        test_start_millis=test_start_millis,
        test_end_millis=test_end_millis,
    )

    evals = AnomalyTrainingEvals(runs=[run])
    evals.compute_aggregated()
    return evals


# =============================================================================
# Model Config Builder
# =============================================================================


def build_model_config(
    forecast_model: Optional[Any] = None,
    forecast_config: Optional[ForecastModelConfig] = None,
    anomaly_model: Optional[Any] = None,
    anomaly_config: Optional[AnomalyModelConfig] = None,
    preprocessing_config: Optional[Any] = None,
    has_detection_bands: bool = True,
    quality_score: float = 1.0,
    forecast_evals: Optional[Dict[str, float]] = None,
    anomaly_evals: Optional[Dict[str, float]] = None,
    forecast_registry_key: str = "prophet",
    anomaly_registry_key: str = "datahub_forecast_anomaly",
    train_df: Optional[pd.DataFrame] = None,
    eval_df: Optional[pd.DataFrame] = None,
) -> ModelConfig:
    """Build ModelConfig for persistence from model objects and configs.

    This function serializes model configurations into a ModelConfig suitable
    for storage in AssertionInferenceDetails. It uses the observe-models
    registry to get model names and versions.

    Args:
        forecast_model: Trained forecast model instance (optional).
        forecast_config: ForecastModelConfig (preferred) or None to extract from model.
        anomaly_model: Trained anomaly model instance (optional).
        anomaly_config: AnomalyModelConfig (preferred) or None to extract from model.
        preprocessing_config: Preprocessing config (PreprocessingConfig or similar).
        has_detection_bands: Whether detection bands were generated.
        quality_score: Overall quality score from evaluation (0.0-1.0).
        forecast_evals: Forecast evaluation metrics dict (mae, rmse, mape, coverage).
        anomaly_evals: Anomaly evaluation metrics dict. Serialized to
            AnomalyTrainingEvals only if ground_truth metrics (precision, recall,
            f1_score) are present. Without ground_truth, evaluate() returns
            {anomaly_rate, mean_score, ...} which cannot be serialized.
        forecast_registry_key: Key to look up forecast model in registry.
        anomaly_registry_key: Key to look up anomaly model in registry.
        train_df: Training DataFrame with 'ds' column for timestamp ranges.
            Used to build proper ForecastTrainingEvals with training metadata.
        eval_df: Evaluation DataFrame with 'ds' column for timestamp ranges.
            Used to build proper ForecastTrainingEvals with evaluation metadata.

    Returns:
        ModelConfig with serialized configurations ready for persistence.
    """
    registry = get_model_registry()

    # Get forecast model entry for version info
    forecast_entry = registry.get(forecast_registry_key)
    forecast_model_name = forecast_entry.name
    forecast_model_version = forecast_entry.version

    # Serialize forecast config - prefer provided config (preserves all fields)
    forecast_config_json = None
    if forecast_model is not None:
        if forecast_config is not None:
            forecast_config_json = ForecastConfigSerializer.serialize(forecast_config)
        else:
            # Fallback: reconstruct from model attributes
            hyperparams = getattr(forecast_model, "darts_model_config", {}) or {}
            fallback_config = ForecastModelConfig(hyperparameters=hyperparams)
            forecast_config_json = ForecastConfigSerializer.serialize(fallback_config)

    # Extract preprocessing config
    preprocessing_json = "{}"
    preproc_config = preprocessing_config
    if preproc_config is None and forecast_model is not None:
        preproc_config = getattr(forecast_model, "preprocessing_config", None)
    if preproc_config is not None:
        if hasattr(preproc_config, "to_preprocessing_config"):
            preproc_config = preproc_config.to_preprocessing_config()
        preprocessing_json = (
            PreprocessingConfigSerializer.serialize(preproc_config) or "{}"
        )

    # Serialize anomaly config - prefer provided config (preserves all fields)
    anomaly_model_name = None
    anomaly_model_version = None
    anomaly_config_json = None

    if has_detection_bands and anomaly_model is not None:
        anomaly_entry = registry.get(anomaly_registry_key)
        anomaly_model_name = anomaly_entry.name
        anomaly_model_version = anomaly_entry.version

        if anomaly_config is not None:
            anomaly_config_json = AnomalyConfigSerializer.serialize(anomaly_config)
        else:
            # Fallback: reconstruct from model attributes
            hyperparams = {}
            if hasattr(anomaly_model, "get_hyperparameters"):
                hyperparams = anomaly_model.get_hyperparameters() or {}
            anomaly_fallback_config = AnomalyModelConfig(hyperparameters=hyperparams)
            anomaly_config_json = AnomalyConfigSerializer.serialize(
                anomaly_fallback_config
            )

    # Serialize evaluation metrics
    # Build proper ForecastTrainingEvals with training metadata for consistency
    # with model_override.py format
    forecast_evals_json = None
    if forecast_evals:
        forecast_training_evals = build_forecast_training_evals(
            metrics=forecast_evals,
            train_df=train_df,
            eval_df=eval_df,
        )
        if forecast_training_evals is not None:
            forecast_evals_json = ForecastEvalsSerializer.serialize(
                forecast_training_evals
            )

    # Serialize anomaly evaluation metrics if ground truth metrics are available
    # AnomalyTrainingEvals requires precision, recall, f1_score which are only
    # available when ground_truth was provided to anomaly_model.evaluate().
    # Without ground_truth, evaluate() returns {anomaly_rate, mean_score, ...}
    # which cannot be serialized to AnomalyTrainingEvals.
    anomaly_evals_json = None
    if anomaly_evals:
        anomaly_training_evals = build_anomaly_training_evals(
            metrics=anomaly_evals,
            train_df=train_df,
            eval_df=eval_df,
        )
        if anomaly_training_evals is not None:
            anomaly_evals_json = AnomalyEvalsSerializer.serialize(
                anomaly_training_evals
            )

    return ModelConfig(
        forecast_model_name=forecast_model_name,
        forecast_model_version=forecast_model_version,
        forecast_config_json=forecast_config_json,
        forecast_evals_json=forecast_evals_json,
        anomaly_model_name=anomaly_model_name,
        anomaly_model_version=anomaly_model_version,
        anomaly_config_json=anomaly_config_json,
        anomaly_evals_json=anomaly_evals_json,
        preprocessing_config_json=preprocessing_json,
        confidence=quality_score,
        generated_at=int(time.time() * 1000),
    )
