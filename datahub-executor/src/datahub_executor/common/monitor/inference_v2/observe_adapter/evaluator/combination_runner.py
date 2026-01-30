"""
Model combination evaluation runner.

Provides utilities for evaluating multiple forecast + anomaly model pairings
and selecting the best one based on combined scores.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
    ModelPairing,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.metrics import (
    evaluate_anomaly_model,
    evaluate_forecast_model,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.tuning_policy import (
    TuningDecision,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.types import (
    AnomalyEvaluationResult,
    CombinationEvaluationResult,
    ForecastEvaluationResult,
)
from datahub_executor.common.monitor.inference_v2.types import (
    ProgressHook,
    normalize_progress_hooks,
)

if TYPE_CHECKING:
    from datahub_observe.algorithms.anomaly_detection.anomaly_base import (
        BaseAnomalyModel,
    )
    from datahub_observe.algorithms.anomaly_detection.forecast_anomaly_base import (
        BaseForecastAnomalyModel,
    )
    from datahub_observe.algorithms.forecasting.forecast_base import (
        DartsBaseForecastModel,
    )

logger = logging.getLogger(__name__)


def _invoke_progress_hooks(
    progress_hooks: Sequence[ProgressHook],
    message: str,
    progress: Optional[float],
) -> None:
    """Invoke all progress hooks with (message, progress, None, None)."""
    for hook in progress_hooks:
        try:
            hook(message, progress, None, None)
        except Exception as e:
            logger.warning("Progress hook raised: %s", e)


def _merge_ground_truth_for_filtering(
    df: pd.DataFrame,
    ground_truth: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Merge ground truth into training DataFrame to add 'type' column for filtering.

    Args:
        df: Training DataFrame with 'ds' and 'y' columns.
        ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.

    Returns:
        DataFrame with 'type' column added. Rows matching ground truth anomalies
        are marked as type='ANOMALY', others are marked as type='NORMAL'.
    """
    if ground_truth is None or ground_truth.empty:
        return df

    # Merge on 'ds' column, marking anomalies
    df_with_type = df.copy()
    df_with_type["type"] = "NORMAL"

    if "is_anomaly_gt" in ground_truth.columns:
        # Use .eq(True) to handle boolean Series correctly, including NaN values
        gt_anomalies = ground_truth[ground_truth["is_anomaly_gt"].eq(True)]
        if not gt_anomalies.empty:
            # Convert 'ds' to datetime for matching
            df_with_type["ds"] = pd.to_datetime(df_with_type["ds"])
            gt_anomalies = gt_anomalies.copy()
            gt_anomalies["ds"] = pd.to_datetime(gt_anomalies["ds"])

            # Mark matching rows as ANOMALY
            mask = df_with_type["ds"].isin(gt_anomalies["ds"])
            df_with_type.loc[mask, "type"] = "ANOMALY"

    return df_with_type


def evaluate_model_pairing(
    train_df: pd.DataFrame,
    eval_df: pd.DataFrame,
    forecast_model: Optional["DartsBaseForecastModel"],
    anomaly_model: Optional[Union["BaseForecastAnomalyModel", "BaseAnomalyModel"]],
    pairing: ModelPairing,
    ground_truth: Optional[pd.DataFrame] = None,
) -> CombinationEvaluationResult:
    """
    Evaluate a single model pairing.

    Supports both forecast-based anomaly models and direct anomaly models
    (where forecast_model is None).

    Args:
        train_df: Training DataFrame with 'ds' and 'y' columns.
        forecast_model: Trained forecast model instance, or None for direct anomaly models.
        anomaly_model: Trained anomaly model instance.
        pairing: ModelPairing with registry names and versions.
        ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
        cv_split_ratio: Ratio for train/eval split.

    Returns:
        CombinationEvaluationResult with scores and metrics.
    """
    errors: List[str] = []

    # Evaluate forecast model (only if pairing has one)
    forecast_result: ForecastEvaluationResult
    if pairing.requires_forecast and forecast_model is not None:
        forecast_result = evaluate_forecast_model(
            train_df=train_df,
            forecast_model=forecast_model,
            eval_df=eval_df,
        )
        if not forecast_result.success:
            errors.append(f"Forecast: {forecast_result.error}")
    else:
        # No forecast model - create a placeholder result
        forecast_result = ForecastEvaluationResult(
            score=1.0,  # Not applicable
            raw_metrics={},
            success=True,
            error=None,
        )

    # Evaluate anomaly model
    anomaly_result = evaluate_anomaly_model(
        train_df=train_df,
        anomaly_model=anomaly_model,
        ground_truth=ground_truth,
        eval_df=eval_df,
    )
    if not anomaly_result.success:
        errors.append(f"Anomaly: {anomaly_result.error}")

    # Compute combined score
    # For direct anomaly models, forecast_score is None so only anomaly score is used
    forecast_score_for_combined = (
        forecast_result.score if pairing.requires_forecast else None
    )
    combined_score = CombinationEvaluationResult.compute_combined_score(
        forecast_score=forecast_score_for_combined,
        anomaly_score=anomaly_result.score,
    )

    # Determine success based on whether forecast is required
    if pairing.requires_forecast:
        success = forecast_result.success and anomaly_result.success
    else:
        success = anomaly_result.success

    return CombinationEvaluationResult(
        combination_name=pairing.name,
        anomaly_model_key=pairing.anomaly_model_key,
        forecast_result=forecast_result,
        anomaly_result=anomaly_result,
        combined_score=combined_score,
        forecast_model_key=pairing.forecast_model_key,
        success=success,
        errors=errors,
    )


def run_pairing_evaluation(
    train_df: pd.DataFrame,
    eval_df: pd.DataFrame,
    pairings: List[ModelPairing],
    ground_truth: Optional[pd.DataFrame] = None,
    tuning_decision: Optional[TuningDecision] = None,
    preprocessing_config: Optional[object] = None,
    progress_hooks: Optional[Union[ProgressHook, Sequence[ProgressHook]]] = None,
    parallelization_kwargs: Optional[Dict[str, Any]] = None,
) -> List[CombinationEvaluationResult]:
    """
    Evaluate multiple model pairings.

    This function:
    1. Creates and trains forecast models for each unique forecast model
    2. Creates and trains anomaly models using the trained forecast models
    3. Evaluates each pairing
    4. Returns results sorted by combined score (highest first)

    Args:
        train_df: Training DataFrame with 'ds' and 'y' columns.
        pairings: List of ModelPairing objects to evaluate.
        ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
        tuning_decision: Optional tuning decision (affects hyperparameter search).
        preprocessing_config: Optional preprocessing config for models.
        progress_hooks: Optional progress callback(s). Each is called with
            (message, progress, step_name=None, step_result=None).
        parallelization_kwargs: Optional parallelization kwargs (use_parallelization, n_jobs).

    Returns:
        List of CombinationEvaluationResult sorted by combined_score (highest first).

    Raises:
        TrainingErrorException: If all pairings fail to train/evaluate.
    """
    from datahub_observe.registry import get_model_registry

    if not pairings:
        raise TrainingErrorException(
            message="No model pairings provided for evaluation",
            error_type=MonitorErrorTypeClass.INVALID_PARAMETERS,
            properties={"step": "pairing_evaluation"},
        )

    hooks = normalize_progress_hooks(progress_hooks)
    registry = get_model_registry()
    results: List[CombinationEvaluationResult] = []

    # Merge ground truth into training DataFrame to add 'type' column for filtering
    # This allows AnomalyDataFilterConfig to exclude anomalies from training
    train_df_with_type = _merge_ground_truth_for_filtering(train_df, ground_truth)

    # Cache trained forecast models to avoid retraining (keyed by model identifier)
    trained_forecast_models: Dict[str, "DartsBaseForecastModel"] = {}

    total = len(pairings)
    for idx, pairing in enumerate(pairings, start=1):
        logger.info(f"Evaluating pairing: {pairing}")
        _invoke_progress_hooks(
            hooks,
            f"Evaluating pairing {idx}/{total}: {pairing.name}",
            float(idx - 1) / float(total) if total > 0 else None,
        )

        try:
            # Get or train forecast model (only if pairing requires one)
            forecast_model: Optional["DartsBaseForecastModel"] = None
            if pairing.requires_forecast and pairing.forecast_model_key:
                if pairing.forecast_model_key in trained_forecast_models:
                    forecast_model = trained_forecast_models[pairing.forecast_model_key]
                else:
                    # Create and train forecast model
                    _invoke_progress_hooks(
                        hooks,
                        f"Training forecast model for pairing {pairing.name}: {pairing.forecast_model_key}",
                        float(idx - 1) / float(total) if total > 0 else None,
                    )
                    forecast_model = _create_and_train_forecast_model(
                        registry=registry,
                        forecast_model_key=pairing.forecast_model_key,
                        train_df=train_df_with_type,  # Use DataFrame with 'type' column
                        preprocessing_config=preprocessing_config,
                        forecast_config=None,  # Use defaults
                        tune_hyperparameters=(
                            tuning_decision.should_retune_forecast
                            if tuning_decision
                            else True
                        ),
                        parallelization_kwargs=parallelization_kwargs,
                    )
                    trained_forecast_models[pairing.forecast_model_key] = forecast_model

            # Create and train anomaly model
            _invoke_progress_hooks(
                hooks,
                f"Training anomaly model for pairing {pairing.name}: {pairing.anomaly_model_key}",
                float(idx - 1) / float(total) if total > 0 else None,
            )
            anomaly_model = _create_and_train_anomaly_model(
                registry=registry,
                anomaly_model_key=pairing.anomaly_model_key,
                forecast_model=forecast_model,  # None for direct anomaly models
                train_df=train_df_with_type,  # Use DataFrame with 'type' column
                ground_truth=ground_truth,
                preprocessing_config=preprocessing_config,
                anomaly_config=None,  # Use defaults
                tune_hyperparameters=(
                    tuning_decision.should_retune_anomaly if tuning_decision else True
                ),
                parallelization_kwargs=parallelization_kwargs,
            )

            # Evaluate pairing
            _invoke_progress_hooks(
                hooks,
                f"Scoring pairing {idx}/{total}: {pairing.name}",
                float(idx - 1) / float(total) if total > 0 else None,
            )
            result = evaluate_model_pairing(
                train_df=train_df,
                eval_df=eval_df,
                forecast_model=forecast_model,
                anomaly_model=anomaly_model,
                pairing=pairing,
                ground_truth=ground_truth,
            )
            results.append(result)
            _invoke_progress_hooks(
                hooks,
                f"Finished pairing {idx}/{total}: {pairing.name}",
                float(idx) / float(total) if total > 0 else None,
            )

        except TrainingErrorException:
            raise
        except Exception as e:
            logger.warning(f"Failed to evaluate pairing {pairing.name}: {e}")
            # Create failed result
            results.append(
                CombinationEvaluationResult(
                    combination_name=pairing.name,
                    forecast_model_key=pairing.forecast_model_key,
                    anomaly_model_key=pairing.anomaly_model_key,
                    forecast_result=ForecastEvaluationResult.failed(str(e)),
                    anomaly_result=AnomalyEvaluationResult.failed(str(e)),
                    combined_score=0.0,
                    success=False,
                    errors=[str(e)],
                )
            )
            _invoke_progress_hooks(
                hooks,
                f"Failed pairing {idx}/{total}: {pairing.name}",
                float(idx) / float(total) if total > 0 else None,
            )

    # Check if any pairings succeeded
    successful_results = [r for r in results if r.success]
    if not successful_results:
        # Collect error details from all failed pairings
        error_details = []
        for result in results:
            pairing_errors = []
            if result.errors:
                pairing_errors.extend(result.errors)
            if result.forecast_result.error:
                pairing_errors.append(f"Forecast: {result.forecast_result.error}")
            if result.anomaly_result.error:
                pairing_errors.append(f"Anomaly: {result.anomaly_result.error}")

            if pairing_errors:
                error_details.append(
                    f"{result.combination_name}: {'; '.join(pairing_errors)}"
                )
            else:
                error_details.append(f"{result.combination_name}: Unknown error")

        error_summary = "; ".join(error_details)
        # Format errors as a single string for properties dict (which expects str values)
        errors_str = " | ".join(error_details)
        raise TrainingErrorException(
            message=f"All {len(pairings)} model pairings failed to evaluate. Errors: {error_summary}",
            error_type=MonitorErrorTypeClass.MODEL_EVALUATION_FAILED,
            properties={
                "step": "pairing_evaluation",
                "pairings_tried": str(len(pairings)),
                "errors": errors_str,
            },
        )

    # Sort by combined score (highest first)
    results.sort(key=lambda r: r.combined_score, reverse=True)

    return results


def select_best_combination(
    results: List[CombinationEvaluationResult],
) -> Tuple[CombinationEvaluationResult, int]:
    """
    Select the best model combination from evaluation results.

    Args:
        results: List of CombinationEvaluationResult (should be sorted by score).

    Returns:
        Tuple of (best_result, index_in_list).

    Raises:
        ValueError: If no successful results are available.
    """
    successful_results = [r for r in results if r.success]
    if not successful_results:
        raise ValueError("No successful combination results to select from")

    # Results should already be sorted, but ensure we get the best one
    best_result = max(successful_results, key=lambda r: r.combined_score)
    index = results.index(best_result)

    return best_result, index


def _create_and_train_forecast_model(
    registry: object,
    forecast_model_key: str,
    train_df: pd.DataFrame,
    preprocessing_config: Optional[object],
    forecast_config: Optional[object],
    tune_hyperparameters: bool,
    parallelization_kwargs: Optional[Dict[str, Any]] = None,
) -> "DartsBaseForecastModel":
    """
    Create and train a forecast model.

    Args:
        registry: Model registry instance.
        forecast_model_key: Registry key for forecast model.
        train_df: Training DataFrame.
        preprocessing_config: Preprocessing configuration.
        forecast_config: Forecast model configuration.
        tune_hyperparameters: Whether to tune hyperparameters.
        parallelization_kwargs: Optional parallelization kwargs (use_parallelization, n_jobs).

    Returns:
        Trained forecast model.
    """
    # Lazily import to avoid heavy dependencies in slim builds.
    from datahub_observe.algorithms.forecasting.config import ForecastModelConfig

    if forecast_config is None:
        # Minimal default: allow the model to use its own defaults.
        forecast_config = ForecastModelConfig(hyperparameters={})

    # Get forecast model class from registry
    kwargs = parallelization_kwargs or {}
    forecast_model = registry.create_forecast_model(  # type: ignore
        forecast_model_key,
        preprocessing_config,
        forecast_config,
        **kwargs,
    )

    # Train
    forecast_model.train(train_df)

    return forecast_model


def _create_and_train_anomaly_model(
    registry: object,
    anomaly_model_key: str,
    forecast_model: Optional["DartsBaseForecastModel"],
    train_df: pd.DataFrame,
    ground_truth: Optional[pd.DataFrame],
    preprocessing_config: Optional[object],
    anomaly_config: Optional[object],
    tune_hyperparameters: bool,
    parallelization_kwargs: Optional[Dict[str, Any]] = None,
) -> Union["BaseForecastAnomalyModel", "BaseAnomalyModel"]:
    """
    Create and train an anomaly detection model.

    Supports both forecast-based anomaly models (require forecast_model) and
    direct anomaly models (forecast_model is None).

    Args:
        registry: Model registry instance.
        anomaly_model_key: Registry key for anomaly model.
        forecast_model: Trained forecast model (for forecast-based anomaly detection),
            or None for direct anomaly models like deepsvdd.
        train_df: Training DataFrame.
        ground_truth: Optional ground truth DataFrame.
        preprocessing_config: Preprocessing configuration.
        anomaly_config: Anomaly model configuration.
        tune_hyperparameters: Whether to tune hyperparameters.
        parallelization_kwargs: Optional parallelization kwargs (use_parallelization, n_jobs).

    Returns:
        Trained anomaly model.
    """
    from datahub_observe.algorithms.anomaly_detection.config import AnomalyModelConfig
    from datahub_observe.algorithms.forecasting.config import ForecastModelConfig

    if anomaly_config is None:
        anomaly_config = AnomalyModelConfig(
            hyperparameters={},
            forecast_model_config=ForecastModelConfig(hyperparameters={}),
        )

    # Decide whether the anomaly model requires a forecast model (metadata on registry entry).
    entry = registry.get(anomaly_model_key)  # type: ignore[attr-defined]
    requires_forecast = bool(entry.metadata.get("requires_forecast_model", False))

    if requires_forecast:
        if forecast_model is None:
            raise TrainingErrorException(
                message=f"Anomaly model '{anomaly_model_key}' requires a forecast model",
                error_type=MonitorErrorTypeClass.INVALID_PARAMETERS,
                properties={"step": "pairing_evaluation"},
            )

        # Avoid constructing a nested forecast model (and retraining it). Instantiate directly
        # with the already-trained forecast_model.
        # Use the exact same pattern as ModelFactory.create_anomaly_model()
        model_cls = entry.cls
        parallelization_kwargs = parallelization_kwargs or {}

        # Create anomaly model with the pre-trained forecast model.
        # Prefer using observe-models' from_config() so sensitivity and other config
        # fields are consistently applied (matches ModelFactory pattern exactly).
        if hasattr(model_cls, "from_config"):
            anomaly_model = model_cls.from_config(  # type: ignore[attr-defined]
                preprocessing_config=forecast_model.preprocessing_config,
                model_config=anomaly_config,
                forecast_model=forecast_model,
                **parallelization_kwargs,
            )
        else:
            anomaly_model = model_cls(
                forecast_model=forecast_model,
                **parallelization_kwargs,
            )
    else:
        # Direct anomaly model (no forecast required)
        parallelization_kwargs = parallelization_kwargs or {}

        anomaly_model = registry.create_anomaly_model(  # type: ignore
            anomaly_model_key,
            preprocessing_config,
            anomaly_config,
            **parallelization_kwargs,
        )

    # Merge ground truth into training DataFrame to add 'type' column for filtering
    train_df_with_type = _merge_ground_truth_for_filtering(train_df, ground_truth)

    # Train with ground truth if available
    anomaly_model.train(train_df_with_type, ground_truth=ground_truth)

    return anomaly_model
