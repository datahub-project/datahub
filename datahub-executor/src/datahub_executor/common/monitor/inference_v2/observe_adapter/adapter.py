"""
Adapter for observe-models package integration.

This module provides a simplified adapter that leverages datahub_observe's built-in
pipeline. The anomaly model handles preprocessing and forecasting internally via
composition with the forecast model.

Design: Single train() call handles the full pipeline internally.
"""

import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException
from datahub_executor.common.monitor.inference_v2.inference_utils import (
    ModelConfig,
    get_force_retune_anomaly_only,
    get_inference_v2_eval_train_ratio,
    prepare_predictions_df_for_persistence,
    split_time_series_df,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
    InputDataContext,
    ObserveDefaultsBuilder,
    get_defaults_for_context,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator import (
    TuningDecision,
    TuningReason,
    compute_evaluation_metrics,
    make_tuning_decision,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combination_runner import (
    run_pairing_evaluation,
    select_best_combination,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
    ModelPairing,
    get_pairings_from_env_or_default,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.scoring import (
    DEFAULT_ANOMALY_SCORE_THRESHOLD,
    DEFAULT_DROP_THRESHOLD,
    DEFAULT_FORECAST_SCORE_THRESHOLD,
    DEFAULT_MIN_SAMPLES_FOR_CV,
    compute_anomaly_score,
    compute_forecast_score,
    get_primary_anomaly_score,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
    InitializedModels,
    ModelFactory,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
    build_model_config,
)
from datahub_executor.common.monitor.inference_v2.types import (
    ProgressHook,
    StepResult,
    TrainingResult,
    TrainingResultBuilder,
    normalize_progress_hooks,
)

logger = logging.getLogger(__name__)


def _notify_progress(
    progress_hooks: Sequence[ProgressHook],
    message: str,
    progress: Optional[float],
    step_name: Optional[str] = None,
    step_result: Optional[StepResult] = None,
) -> None:
    for hook in progress_hooks:
        try:
            hook(message, progress, step_name, step_result)
        except Exception as e:
            logger.warning("Progress hook raised: %s", e)


class ObserveAdapter:
    """
    Simplified adapter for observe-models package.

    Uses the built-in pipeline in BaseForecastAnomalyModel:
    - Forecast model handles preprocessing internally
    - Anomaly model composes forecast model
    - Single train() call handles the full pipeline

    This class is stateless - all data flows through method parameters and return values.
    """

    def run_training_pipeline(
        self,
        df: pd.DataFrame,
        input_data_context: InputDataContext,
        num_intervals: int,
        interval_hours: int = 1,
        force_retune: bool = False,
        force_retune_forecast: bool = False,
        force_retune_anomaly: bool = False,
        sensitivity_level: int = 5,
        ground_truth: Optional[pd.DataFrame] = None,
        existing_model_config: Optional[ModelConfig] = None,
        model_combinations: Optional[List[ModelPairing]] = None,
        score_drop_threshold: float = DEFAULT_DROP_THRESHOLD,
        eval_train_ratio: Optional[float] = None,
        progress_hooks: Optional[Union[ProgressHook, Sequence[ProgressHook]]] = None,
    ) -> TrainingResult:
        """
        Run the training pipeline using datahub_observe's built-in orchestration.

        Steps:
        1. Create forecast and anomaly models with configs
        2. Train anomaly model (handles full pipeline internally)
        3. Check training quality and retry with retuning if quality is bad
        4. Generate future timestamps
        5. Get predictions with detection bands via detect()
        6. Fallback to basic forecast if detect() fails or not confident
        7. Build model config for persistence

        Args:
            df: DataFrame with columns 'ds' (datetime), 'y' (float).
            input_data_context: InputDataContext with metadata about the input
                data's shape (category, whether cumulative). Used to determine
                appropriate preprocessing and model configuration.
            num_intervals: Number of future intervals to predict.
            interval_hours: Size of each interval in hours.
            force_retune: If True, ignore existing hyperparameters and force fresh
                tuning. Behavior is configurable via DATAHUB_EXECUTOR_FORCE_RETUNE_ANOMALY_ONLY:
                - If not set or "false" (default): sets both force_retune_forecast and
                  force_retune_anomaly to True
                - If "true": only sets force_retune_anomaly to True
                Deprecated: use force_retune_forecast and force_retune_anomaly instead.
                If force_retune is True, it overrides the individual flags.
            force_retune_forecast: If True, force fresh tuning for forecast model only.
            force_retune_anomaly: If True, force fresh tuning for anomaly model only.
            sensitivity_level: Sensitivity level (1-10) for model behavior.
                Higher (7-10): more aggressive (tighter bands / more anomalies).
                Lower (1-3): more conservative (wider bands / fewer anomalies).
                Medium (4-6): balanced. In observe-models this maps to
                contamination/coverage-like parameters for anomaly models.
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns
                for anomaly feedback. is_anomaly_gt should be True for confirmed
                anomalies (status != REJECTED).
            existing_model_config: Previously trained model config for warm start.
            model_combinations: Optional list of ModelPairing objects to evaluate.
                If None or empty, uses env/default pairings. Evaluates all combinations
                and selects the best one.
            score_drop_threshold: Relative drop threshold (0.0-1.0) for triggering
                retuning when score drops from previous. Default: 0.25 (25% drop).

        Returns:
            TrainingResult containing prediction_df (with detection bands),
            model_config, and step_results tracking success/failure of each step.
        """
        if df is None or df.empty:
            raise TrainingErrorException(
                message="No training data available for observe pipeline",
                error_type=MonitorErrorTypeClass.INPUT_DATA_INSUFFICIENT,
                properties={"detail": "empty_training_dataframe"},
            )

        required_columns = {"ds", "y"}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise TrainingErrorException(
                message=(
                    "Training data is missing required columns: "
                    f"{', '.join(sorted(missing_columns))}"
                ),
                error_type=MonitorErrorTypeClass.INPUT_DATA_INVALID,
                properties={
                    "missing_columns": ",".join(sorted(missing_columns)),
                },
            )

        # Handle backward compatibility: if force_retune is True, override individual flags
        # Configurable via DATAHUB_EXECUTOR_FORCE_RETUNE_ANOMALY_ONLY:
        # - If not set or "false": force_retune=True sets both forecast and anomaly retune
        # - If "true": force_retune=True only sets anomaly retune
        if force_retune:
            force_retune_anomaly_only = get_force_retune_anomaly_only()
            if force_retune_anomaly_only:
                force_retune_anomaly = True
            else:
                force_retune_forecast = True
                force_retune_anomaly = True

        # Model pairings to evaluate: explicit list, or env/default pairings.
        if not model_combinations:
            model_combinations = get_pairings_from_env_or_default()

        return self._run_combination_evaluation(
            df=df,
            input_data_context=input_data_context,
            num_intervals=num_intervals,
            interval_hours=interval_hours,
            force_retune_forecast=force_retune_forecast,
            force_retune_anomaly=force_retune_anomaly,
            ground_truth=ground_truth,
            existing_model_config=existing_model_config,
            model_combinations=model_combinations,
            score_drop_threshold=score_drop_threshold,
            eval_train_ratio=eval_train_ratio,
            progress_hooks=progress_hooks,
        )

    def _run_combination_evaluation(
        self,
        df: pd.DataFrame,
        input_data_context: InputDataContext,
        num_intervals: int,
        interval_hours: int,
        force_retune_forecast: bool,
        force_retune_anomaly: bool,
        ground_truth: Optional[pd.DataFrame],
        existing_model_config: Optional[ModelConfig],
        model_combinations: List[ModelPairing],
        score_drop_threshold: float,
        eval_train_ratio: Optional[float],
        progress_hooks: Optional[Union[ProgressHook, Sequence[ProgressHook]]],
    ) -> TrainingResult:
        """
        Run training pipeline with model combination evaluation.

        This method:
        1. Evaluates all provided model combinations
        2. Selects the best combination based on combined scores
        3. Trains the best combination to get final predictions

        Args:
            df: DataFrame with columns 'ds' (datetime), 'y' (float).
            input_data_context: InputDataContext with metadata about the input
                data's shape.
            num_intervals: Number of future intervals to predict.
            interval_hours: Size of each interval in hours.
            force_retune_forecast: Force fresh tuning for forecast model.
            force_retune_anomaly: Force fresh tuning for anomaly model.
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
            existing_model_config: Previously trained model config for warm start.
            model_combinations: List of ModelPairing objects to evaluate.
            score_drop_threshold: Relative drop threshold for retuning decisions.

        Returns:
            TrainingResult from the best model combination.
        """
        builder = TrainingResultBuilder()
        all_hooks: tuple[ProgressHook, ...] = (builder,) + normalize_progress_hooks(
            progress_hooks
        )

        # Build defaults from input data context
        defaults = get_defaults_for_context(input_data_context)

        # Stored scores from last persisted run (existing_model_config is loaded
        # from evaluation_spec.context.inference_details in production).
        previous_forecast_score = None
        previous_anomaly_score = None
        if existing_model_config:
            previous_forecast_score = existing_model_config.forecast_score
            previous_anomaly_score = existing_model_config.anomaly_score

        # Make tuning decision
        tuning_decision = make_tuning_decision(
            previous_forecast_score=previous_forecast_score,
            previous_anomaly_score=previous_anomaly_score,
            ground_truth=ground_truth,
            force_retune_forecast=force_retune_forecast,
            force_retune_anomaly=force_retune_anomaly,
            has_previous_config=(existing_model_config is not None),
            forecast_score_threshold=DEFAULT_FORECAST_SCORE_THRESHOLD,
            anomaly_score_threshold=DEFAULT_ANOMALY_SCORE_THRESHOLD,
            drop_threshold=score_drop_threshold,
        )

        # Get preprocessing config from defaults, passing ground_truth to enable anomaly filtering
        preprocessing_config = defaults.preprocessing_config(ground_truth=ground_truth)

        # Get parallelization kwargs for consistent defaults across all models
        parallelization_kwargs = defaults.parallelization_kwargs()

        effective_train_ratio = (
            float(eval_train_ratio)
            if eval_train_ratio is not None
            else get_inference_v2_eval_train_ratio()
        )
        try:
            eval_train_df, eval_df = split_time_series_df(
                df, train_ratio=effective_train_ratio
            )
        except ValueError as e:
            raise TrainingErrorException(
                message=f"Insufficient data for strict evaluation split: {e}",
                error_type=MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT,
                properties={
                    "step": "evaluation_split",
                    "train_ratio": str(effective_train_ratio),
                    "row_count": str(len(df)),
                },
            ) from e

        # Evaluate all combinations
        logger.info(f"Evaluating {len(model_combinations)} model combinations")
        _notify_progress(
            all_hooks,
            f"Evaluating {len(model_combinations)} model pairings",
            0.0,
            "combination_evaluation",
            StepResult(success=True),
        )
        combination_results_dicts: Optional[List[Dict[str, Any]]] = None
        try:
            combination_results = run_pairing_evaluation(
                train_df=eval_train_df,
                eval_df=eval_df,
                pairings=model_combinations,
                ground_truth=ground_truth,
                tuning_decision=tuning_decision,
                preprocessing_config=preprocessing_config,
                progress_hooks=all_hooks,
                parallelization_kwargs=parallelization_kwargs,
            )

            # Select best combination
            best_result, best_index = select_best_combination(combination_results)
            best_pairing = model_combinations[best_index]

            logger.info(
                f"Selected best combination: {best_pairing.name} "
                f"(score: {best_result.combined_score:.3f})"
            )

            _notify_progress(
                all_hooks,
                f"Selected winner: {best_pairing.name} (score {best_result.combined_score:.3f})",
                1.0,
                "combination_selection",
                StepResult(success=True),
            )

            # Serialize combination_results for persistence (for UI alternatives tab)
            combination_results_dicts = [r.to_dict() for r in combination_results]

        except Exception as e:
            logger.error(f"Model combination evaluation failed: {e}")
            _notify_progress(
                all_hooks,
                str(e),
                None,
                "combination_evaluation",
                StepResult(success=False, error=str(e)),
            )
            raise TrainingErrorException(
                message=f"Model combination evaluation failed: {e}",
                error_type=MonitorErrorTypeClass.MODEL_EVALUATION_FAILED,
            ) from e

        # Create a ModelConfig with the best pairing's registry keys
        # This will cause ModelFactory to use those keys
        if existing_model_config:
            # Copy existing config but override registry keys
            # ModelConfig is a Pydantic BaseModel, so use model_copy() instead of dataclasses.replace()
            best_model_config = existing_model_config.model_copy(
                update={
                    "forecast_model_name": best_pairing.forecast_model_key
                    if best_pairing.forecast_model_key
                    else existing_model_config.forecast_model_name,
                    "anomaly_model_name": best_pairing.anomaly_model_key,
                }
            )
        else:
            # Create minimal config just to set registry keys
            best_model_config = ModelConfig(
                preprocessing_config_json="{}",
                forecast_model_name=best_pairing.forecast_model_key,
                anomaly_model_name=best_pairing.anomaly_model_key,
            )

        # Now train the selected combination using the extracted single-combination logic
        _notify_progress(all_hooks, f"Training winner: {best_pairing.name}", None)

        # Reuse defaults from top of method (single get_defaults_for_context call)
        # Pass the already-computed tuning_decision to avoid recomputation
        training_result = self._train_single_combination(
            df=df,
            input_data_context=input_data_context,
            num_intervals=num_intervals,
            interval_hours=interval_hours,
            defaults=defaults,
            tuning_decision=tuning_decision,  # Pass explicitly to skip recomputation
            existing_model_config=best_model_config,
            ground_truth=ground_truth,
            score_drop_threshold=score_drop_threshold,
            eval_train_ratio=None,  # Use default
            progress_hooks=all_hooks,
            result_builder=builder,
        )

        # Attach combination_results to the training result for UI display
        if combination_results_dicts is not None:
            training_result.combination_results = combination_results_dicts

        return training_result

    def _compute_tuning_decision_if_missing(
        self,
        existing_model_config: Optional[ModelConfig],
        ground_truth: Optional[pd.DataFrame],
        score_drop_threshold: float,
    ) -> TuningDecision:
        """Compute tuning decision from stored scores when not already provided."""
        previous_forecast_score = None
        previous_anomaly_score = None
        if existing_model_config:
            previous_forecast_score = existing_model_config.forecast_score
            previous_anomaly_score = existing_model_config.anomaly_score
        return make_tuning_decision(
            previous_forecast_score=previous_forecast_score,
            previous_anomaly_score=previous_anomaly_score,
            ground_truth=ground_truth,
            force_retune_forecast=False,
            force_retune_anomaly=False,
            has_previous_config=(existing_model_config is not None),
            forecast_score_threshold=DEFAULT_FORECAST_SCORE_THRESHOLD,
            anomaly_score_threshold=DEFAULT_ANOMALY_SCORE_THRESHOLD,
            drop_threshold=score_drop_threshold,
        )

    def _split_df_for_evaluation(
        self,
        df: pd.DataFrame,
        eval_train_ratio: Optional[float],
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Split dataframe into train/eval for strict holdout evaluation."""
        effective_train_ratio = (
            float(eval_train_ratio)
            if eval_train_ratio is not None
            else get_inference_v2_eval_train_ratio()
        )
        try:
            return split_time_series_df(df, train_ratio=effective_train_ratio)
        except ValueError as e:
            raise TrainingErrorException(
                message=f"Insufficient data for strict evaluation split: {e}",
                error_type=MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT,
                properties={
                    "step": "evaluation_split",
                    "train_ratio": str(effective_train_ratio),
                    "row_count": str(len(df)),
                },
            ) from e

    def _retrain_on_full_history(
        self,
        models: InitializedModels,
        df: pd.DataFrame,
        defaults: "ObserveDefaultsBuilder",
        ground_truth: Optional[pd.DataFrame],
        eval_train_df: pd.DataFrame,
        eval_df: pd.DataFrame,
        training_forecast_score: Optional[float],
        training_anomaly_score: float,
        forecast_evals: dict[str, float],
        anomaly_evals: dict[str, float],
    ) -> InitializedModels:
        """Retrain models on full history after holdout-based tuning and scoring."""
        assert models.forecast_registry_key is not None
        assert models.anomaly_registry_key is not None
        holdout_model_config = build_model_config(
            forecast_model=models.forecast_model,
            forecast_config=models.forecast_config,
            anomaly_model=models.anomaly_model,
            anomaly_config=models.anomaly_config,
            preprocessing_config=models.preprocessing_config,
            has_detection_bands=True,
            forecast_evals=forecast_evals,
            anomaly_evals=anomaly_evals,
            forecast_registry_key=models.forecast_registry_key,
            anomaly_registry_key=models.anomaly_registry_key,
            train_df=eval_train_df,
            eval_df=eval_df,
            forecast_score=training_forecast_score,
            anomaly_score=training_anomaly_score,
        )
        full_factory = ModelFactory(
            defaults,
            holdout_model_config,
            force_retune=False,
            ground_truth=ground_truth,
        )
        full_models = full_factory.create_models()
        assert full_models.anomaly_model is not None
        if hasattr(full_models.anomaly_model, "param_grid"):
            try:
                full_models.anomaly_model.param_grid = {}
            except Exception:
                pass
        df_with_type = self._merge_ground_truth_for_filtering(df, ground_truth)
        full_models.anomaly_model.train(df_with_type, ground_truth=ground_truth)
        return full_models

    def _run_predictions(
        self,
        models: InitializedModels,
        df: pd.DataFrame,
        num_intervals: int,
        interval_hours: int,
    ) -> Tuple[Optional[pd.DataFrame], StepResult]:
        """Generate predictions with detection bands; returns (prediction_df, step_result)."""
        future_df = self._generate_future_timestamps(df, num_intervals, interval_hours)
        try:
            assert models.anomaly_model is not None
            raw_prediction = models.anomaly_model.detect(future_df)
            prediction_df = self._format_prediction_output(raw_prediction)
            return (prediction_df, StepResult(success=True))
        except Exception as e:
            logger.warning(f"Prediction with detection bands failed: {e}")
            return (None, StepResult(success=False, error=str(e)))

    def _train_single_combination(
        self,
        df: pd.DataFrame,
        input_data_context: InputDataContext,
        num_intervals: int,
        interval_hours: int,
        defaults: "ObserveDefaultsBuilder",
        tuning_decision: Optional[TuningDecision],
        existing_model_config: Optional[ModelConfig],
        ground_truth: Optional[pd.DataFrame],
        score_drop_threshold: float,
        eval_train_ratio: Optional[float],
        progress_hooks: Sequence[ProgressHook],
        result_builder: TrainingResultBuilder,
    ) -> TrainingResult:
        """
        Train a single model combination (extracted from run_training_pipeline).

        This method handles the full training pipeline for a single model combination:
        1. Split data into train/eval
        2. Train and evaluate models
        3. Retry with retuning if quality is poor
        4. Retrain on full history
        5. Generate predictions
        6. Build model config for persistence

        Args:
            df: DataFrame with columns 'ds' (datetime), 'y' (float).
            input_data_context: InputDataContext with metadata about the input
                data's shape.
            num_intervals: Number of future intervals to predict.
            interval_hours: Size of each interval in hours.
            defaults: ObserveDefaultsBuilder providing context-aware default configs.
            tuning_decision: TuningDecision indicating whether to retune each model.
                If None, will be computed from existing_model_config and other factors.
            existing_model_config: Previously trained model config for warm start.
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
            score_drop_threshold: Relative drop threshold for retuning decisions.
            eval_train_ratio: Optional train ratio for evaluation split.
            progress_hooks: Sequence of progress callbacks (first is result_builder).
            result_builder: Builder that accumulates step_results and builds TrainingResult.

        Returns:
            TrainingResult containing prediction_df, model_config, and step_results.
        """

        if tuning_decision is None:
            tuning_decision = self._compute_tuning_decision_if_missing(
                existing_model_config, ground_truth, score_drop_threshold
            )

        logger.info(
            f"Tuning decision - forecast: {tuning_decision.should_retune_forecast} "
            f"({tuning_decision.forecast_reason.value}), "
            f"anomaly: {tuning_decision.should_retune_anomaly} "
            f"({tuning_decision.anomaly_reason.value})"
        )

        eval_train_df, eval_df = self._split_df_for_evaluation(df, eval_train_ratio)

        # Steps 1-3: Create models, train, and evaluate quality (on train split, eval on holdout)
        (
            models,
            training_anomaly_score,
            training_forecast_score,
            forecast_evals,
            anomaly_evals,
            has_tuned,
            eval_forecast_df,
            _,
            eval_detection_results,
        ) = self._train_and_evaluate(
            df=eval_train_df,
            defaults=defaults,
            existing_model_config=existing_model_config,
            tuning_decision=tuning_decision,
            ground_truth=ground_truth,
            eval_df=eval_df,
            progress_hooks=progress_hooks,
            result_builder=result_builder,
        )

        # Quality-based retuning: if quality is bad and we haven't already retuned,
        # retry with fresh tuning to see if we can get better results.
        retry_result = self._retry_training_if_needed(
            training_anomaly_score=training_anomaly_score,
            has_tuned=has_tuned,
            eval_train_df=eval_train_df,
            eval_df=eval_df,
            defaults=defaults,
            existing_model_config=existing_model_config,
            result_builder=result_builder,
            ground_truth=ground_truth,
            progress_hooks=progress_hooks,
        )

        # Update results if retry occurred
        if retry_result is not None:
            (
                models,
                training_anomaly_score,
                training_forecast_score,
                forecast_evals,
                anomaly_evals,
                _,
                eval_forecast_df,
                _,
                eval_detection_results,
            ) = retry_result

        # Quality below threshold: persist scores for next run, do not persist forecast.
        if training_anomaly_score < DEFAULT_ANOMALY_SCORE_THRESHOLD:
            logger.warning(
                "Training anomaly score %.3f below threshold %.3f; persisting scores only (no forecast)",
                training_anomaly_score,
                DEFAULT_ANOMALY_SCORE_THRESHOLD,
            )
            assert models.forecast_registry_key is not None
            assert models.anomaly_registry_key is not None
            model_config = build_model_config(
                forecast_model=models.forecast_model,
                forecast_config=models.forecast_config,
                anomaly_model=models.anomaly_model,
                anomaly_config=models.anomaly_config,
                preprocessing_config=models.preprocessing_config,
                forecast_evals=forecast_evals,
                anomaly_evals=anomaly_evals,
                has_detection_bands=False,
                forecast_registry_key=models.forecast_registry_key,
                anomaly_registry_key=models.anomaly_registry_key,
                train_df=eval_train_df,
                eval_df=eval_df,
                forecast_score=training_forecast_score,
                anomaly_score=training_anomaly_score,
            )
            result_builder.set_final(
                prediction_df=None,
                model_config=model_config,
                scores_only_persist=True,
                evaluation_df=eval_df,
                evaluation_forecast_df=eval_forecast_df,
                evaluation_detection_results=eval_detection_results,
                forecast_evals=forecast_evals,
                anomaly_evals=anomaly_evals,
            )
            return result_builder.build()

        # Step 4: Retrain on full history for final predictions.
        try:
            models = self._retrain_on_full_history(
                models=models,
                df=df,
                defaults=defaults,
                ground_truth=ground_truth,
                eval_train_df=eval_train_df,
                eval_df=eval_df,
                training_forecast_score=training_forecast_score,
                training_anomaly_score=training_anomaly_score,
                forecast_evals=forecast_evals,
                anomaly_evals=anomaly_evals,
            )
        except TrainingErrorException:
            raise
        except Exception as e:
            logger.error(f"Full-history retrain failed: {e}")
            raise TrainingErrorException(
                message=f"Full-history retrain failed: {e}",
                error_type=MonitorErrorTypeClass.MODEL_TRAINING_FAILED,
            ) from e

        # Step 5: Get predictions with detection bands.
        prediction_df, prediction_step_result = self._run_predictions(
            models, df, num_intervals, interval_hours
        )
        _notify_progress(
            progress_hooks,
            "Prediction complete",
            None,
            "prediction",
            prediction_step_result,
        )

        # Step 6: Build model config for persistence
        # create_models() always sets these fields
        assert models.forecast_registry_key is not None
        assert models.anomaly_registry_key is not None
        model_config = build_model_config(
            forecast_model=models.forecast_model,
            forecast_config=models.forecast_config,
            anomaly_model=models.anomaly_model,
            anomaly_config=models.anomaly_config,
            preprocessing_config=models.preprocessing_config,
            forecast_evals=forecast_evals,
            anomaly_evals=anomaly_evals,
            has_detection_bands=(prediction_df is not None),
            forecast_registry_key=models.forecast_registry_key,
            anomaly_registry_key=models.anomaly_registry_key,
            train_df=eval_train_df,
            eval_df=eval_df,
            forecast_score=training_forecast_score,
            anomaly_score=training_anomaly_score,
        )

        result_builder.set_final(
            prediction_df=prediction_df,
            model_config=model_config,
            evaluation_df=eval_df,
            evaluation_forecast_df=eval_forecast_df,
            evaluation_detection_results=eval_detection_results,
            forecast_evals=forecast_evals,
            anomaly_evals=anomaly_evals,
        )
        return result_builder.build()

    def _retry_training_if_needed(
        self,
        training_anomaly_score: float,
        has_tuned: bool,
        eval_train_df: pd.DataFrame,
        eval_df: pd.DataFrame,
        defaults: "ObserveDefaultsBuilder",
        existing_model_config: Optional[ModelConfig],
        result_builder: TrainingResultBuilder,
        ground_truth: Optional[pd.DataFrame],
        progress_hooks: Sequence[ProgressHook],
    ) -> Optional[
        tuple[
            InitializedModels,
            float,
            Optional[float],
            dict[str, float],
            dict[str, float],
            bool,
            Optional[pd.DataFrame],
            Optional[pd.DataFrame],
            Optional[pd.DataFrame],
        ]
    ]:
        """
        Retry training with forced retuning if quality is poor.

        This method checks if the training anomaly score is below threshold and
        we haven't already retuned. If so, it retries training with forced retuning
        for both models.

        Args:
            training_anomaly_score: Current training anomaly score.
            has_tuned: Whether tuning occurred during initial training.
            eval_train_df: Training DataFrame for evaluation split.
            eval_df: Evaluation DataFrame (holdout).
            defaults: ObserveDefaultsBuilder providing context-aware default configs.
            existing_model_config: Previously trained model config for warm start.
            result_builder: Builder to record step results.
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
            progress_hooks: Sequence of progress callbacks.

        Returns:
            Updated training results if retry occurred, None otherwise.
            The tuple contains: (models, training_anomaly_score, training_forecast_score,
            forecast_evals, anomaly_evals, has_tuned, eval_forecast_df, eval_df,
            eval_detection_results).
        """
        # Exit condition: only retry if score is below threshold AND we haven't already retuned
        if training_anomaly_score >= DEFAULT_ANOMALY_SCORE_THRESHOLD or has_tuned:
            return None

        logger.info(
            f"Training anomaly score {training_anomaly_score:.3f} below threshold "
            f"{DEFAULT_ANOMALY_SCORE_THRESHOLD:.3f}, forcing retune and retraining"
        )
        _notify_progress(
            progress_hooks,
            "Quality retune",
            None,
            "quality_retune",
            StepResult(success=True),
        )

        # Force retune both models
        retry_decision = TuningDecision(
            should_retune_forecast=True,
            should_retune_anomaly=True,
            forecast_reason=TuningReason.SCORE_BELOW_THRESHOLD,
            anomaly_reason=TuningReason.SCORE_BELOW_THRESHOLD,
            persist_even_if_not_improved=True,  # Prevent infinite loops
        )

        try:
            retry_results = self._train_and_evaluate(
                df=eval_train_df,
                defaults=defaults,
                existing_model_config=existing_model_config,
                tuning_decision=retry_decision,
                ground_truth=ground_truth,
                eval_df=eval_df,
                progress_hooks=progress_hooks,
                result_builder=result_builder,
            )
            logger.info(f"Post-retune training anomaly score: {retry_results[1]:.3f}")
            return retry_results
        except Exception as e:
            logger.warning(f"Retune and retraining failed: {e}")
            _notify_progress(
                progress_hooks,
                str(e),
                None,
                "quality_retune",
                StepResult(success=False, error=str(e)),
            )
            # Re-raise to maintain error handling behavior
            raise

    def _train_and_evaluate(
        self,
        df: pd.DataFrame,
        ground_truth: Optional[pd.DataFrame],
        defaults: "ObserveDefaultsBuilder",
        existing_model_config: Optional[ModelConfig],
        tuning_decision: Optional[TuningDecision] = None,
        eval_df: Optional[pd.DataFrame] = None,
        progress_hooks: Sequence[ProgressHook] = (),
        result_builder: Optional[TrainingResultBuilder] = None,
    ) -> tuple[
        InitializedModels,
        float,
        Optional[float],
        dict[str, float],
        dict[str, float],
        bool,
        Optional[pd.DataFrame],
        Optional[pd.DataFrame],
        Optional[pd.DataFrame],
    ]:
        """Create models, train, and evaluate quality in one cycle.

        This method encapsulates the full training cycle:
        1. Create and configure models
        2. Train the anomaly model
        3. Evaluate quality

        Args:
            df: Training DataFrame with 'ds' and 'y' columns.
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns
                for anomaly feedback. Used to improve anomaly detection tuning.
            defaults: ObserveDefaultsBuilder providing context-aware default configs.
            existing_model_config: Previously trained model config for warm start.
            tuning_decision: TuningDecision indicating whether to retune each model.
                If None, defaults to retuning both models.

        Returns:
            Tuple of (models, training_anomaly_score, training_forecast_score,
            forecast_evals, anomaly_evals, has_tuned, eval_forecast_df, eval_df,
            eval_detection_results). models is an InitializedModels containing
            forecast_model, anomaly_model, and their configs. has_tuned is True
            if tuning occurred during training. eval_forecast_df and eval_df are
            the holdout forecast and raw eval DataFrames; eval_detection_results
            is the formatted detection output on the eval window (or None).

        Raises:
            ModelCreationError: If model creation fails.
            ModelTrainingError: If model training fails.
        """
        # Default tuning decision if not provided
        if tuning_decision is None:
            tuning_decision = TuningDecision(
                should_retune_forecast=True,
                should_retune_anomaly=True,
                forecast_reason=TuningReason.NO_PREVIOUS_CONFIG,
                anomaly_reason=TuningReason.NO_PREVIOUS_CONFIG,
            )

        # Determine if we should force retune for forecast model
        force_retune_forecast = tuning_decision.should_retune_forecast
        force_retune_anomaly = tuning_decision.should_retune_anomaly

        # Merge ground truth into training DataFrame to add 'type' column for filtering
        # This allows AnomalyDataFilterConfig to exclude anomalies from training
        train_df_with_type = self._merge_ground_truth_for_filtering(df, ground_truth)

        # Step 1: Create and configure models
        try:
            _notify_progress(progress_hooks, "Creating models", None)
            factory = ModelFactory(
                defaults,
                existing_model_config,
                force_retune=force_retune_forecast,  # Use forecast decision for factory
                ground_truth=ground_truth,  # Pass ground_truth to enable anomaly filtering
            )
            models = factory.create_models()
            _notify_progress(
                progress_hooks,
                "Models created",
                None,
                "model_creation",
                StepResult(success=True),
            )
        except Exception as e:
            logger.error(f"Model creation failed: {e}")
            raise TrainingErrorException(
                message=f"Model creation failed: {e}",
                error_type=MonitorErrorTypeClass.MODEL_CREATION_FAILED,
            ) from e

        # Determine if tuning will occur: happens when hyperparameters are empty
        # (either due to force_retune or no existing hyperparameters)
        forecast_hyperparams_empty = not (
            models.forecast_config and models.forecast_config.hyperparameters
        )
        forecast_tuning_required = force_retune_forecast or forecast_hyperparams_empty

        # For anomaly model, check if param_grid is set (enables grid search)
        anomaly_model = models.anomaly_model
        anomaly_tuning_required: bool = force_retune_anomaly or bool(
            hasattr(anomaly_model, "param_grid")
            and anomaly_model.param_grid  # type: ignore
            and len(anomaly_model.param_grid) > 0  # type: ignore
        )

        # Step 2: Train the anomaly model (handles full pipeline internally)
        # Capture hyperparameters before training so we can persist them even on failure
        attempted_hyperparams = self._capture_attempted_hyperparameters(models)

        try:
            assert models.anomaly_model is not None  # create_models() always sets this
            fm = getattr(models, "forecast_model", None)
            forecast_label = (
                getattr(fm, "__class__", type("x", (), {})).__name__
                if fm is not None
                else "none"
            )
            anomaly_label = getattr(
                models.anomaly_model, "__class__", type("x", (), {})
            ).__name__
            _notify_progress(
                progress_hooks,
                f"Training pipeline (forecast={forecast_label}, anomaly={anomaly_label})",
                None,
            )
            # Use DataFrame with 'type' column merged from ground truth
            # This allows preprocessing to filter out anomalies during training
            models.anomaly_model.train(train_df_with_type, ground_truth=ground_truth)
            _notify_progress(
                progress_hooks,
                "Training complete",
                None,
                "training",
                StepResult(success=True),
            )
        except Exception as e:
            logger.error(f"Training failed: {e}")
            _notify_progress(
                progress_hooks,
                str(e),
                None,
                "training",
                StepResult(success=False, error=str(e)),
            )

            # Persist failed hyperparameters for future reference
            if attempted_hyperparams and result_builder is not None:
                try:
                    from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
                        build_failed_model_config,
                    )

                    failed_config = build_failed_model_config(
                        attempted_hyperparams=attempted_hyperparams,
                        error=str(e),
                        existing_config=existing_model_config,
                        forecast_registry_key=models.forecast_registry_key,
                        anomaly_registry_key=models.anomaly_registry_key,
                    )
                    # Store failed config in step_results for potential future use
                    # The config can be checked by make_tuning_decision() in future runs
                    _notify_progress(
                        progress_hooks,
                        str(e),
                        None,
                        "failed_config",
                        StepResult(
                            success=False,
                            error=str(e),
                            metadata={"failed_config": failed_config},
                        ),
                    )
                except Exception as persist_error:
                    logger.warning(
                        f"Failed to persist failed hyperparameters: {persist_error}"
                    )

            raise TrainingErrorException(
                message=f"Training failed: {e}",
                error_type=MonitorErrorTypeClass.MODEL_TRAINING_FAILED,
            ) from e

        # Step 3: Evaluate training quality
        try:
            _notify_progress(progress_hooks, "Evaluating quality", None)
            (
                training_anomaly_score,
                training_forecast_score,
                forecast_evals,
                anomaly_evals,
                eval_df,
            ) = self._evaluate_quality(
                df,
                models,
                ground_truth,
                progress_hooks,
                result_builder,
                eval_df=eval_df,
            )
        except TrainingErrorException:
            raise
        except Exception as e:
            logger.warning(f"Model evaluation failed: {e}")
            _notify_progress(
                progress_hooks,
                str(e),
                None,
                "evaluation",
                StepResult(success=False, error=str(e)),
            )
            raise TrainingErrorException(
                message=f"Model evaluation failed: {e}",
                error_type=MonitorErrorTypeClass.MODEL_EVALUATION_FAILED,
            ) from e

        # Best-effort: materialize an explicit forecast dataframe over the evaluation window
        # (for UI comparison / debugging). This is out-of-sample because df is the train split.
        eval_forecast_df: Optional[pd.DataFrame] = None
        if (
            eval_df is not None
            and models.forecast_model is not None
            and getattr(models.forecast_model, "is_trained", False)
        ):
            try:
                eval_forecast_df = models.forecast_model.predict(eval_df[["ds"]])
            except Exception:
                eval_forecast_df = None

        # Best-effort: generate detection results for the evaluation period
        # (for UI visualization matching training runs). This allows the UI to show
        # detected anomalies, detection bands, and forecast on the test split.
        eval_detection_results: Optional[pd.DataFrame] = None
        if (
            eval_df is not None
            and models.anomaly_model is not None
            and getattr(models.anomaly_model, "is_trained", False)
        ):
            try:
                raw_eval_detection = models.anomaly_model.detect(eval_df)
                eval_detection_results = self._format_prediction_output(
                    raw_eval_detection
                )
            except Exception:
                eval_detection_results = None

        # After training completes, record whether tuning occurred
        has_tuned = forecast_tuning_required or anomaly_tuning_required
        return (
            models,
            training_anomaly_score,
            training_forecast_score,
            forecast_evals,
            anomaly_evals,
            has_tuned,
            eval_forecast_df,
            eval_df,
            eval_detection_results,
        )

    def _evaluate_quality(
        self,
        df: pd.DataFrame,
        models: InitializedModels,
        ground_truth: Optional[pd.DataFrame],
        progress_hooks: Sequence[ProgressHook],
        result_builder: Optional[TrainingResultBuilder],
        eval_df: Optional[pd.DataFrame] = None,
    ) -> tuple[
        float,
        Optional[float],
        dict[str, float],
        dict[str, float],
        Optional[pd.DataFrame],
    ]:
        """Evaluate model quality using grid search metrics and validation split.

        Args:
            df: Training DataFrame.
            models: InitializedModels containing forecast and anomaly models.
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
                If provided, anomaly evaluation returns precision/recall/f1/accuracy.
            progress_hooks: Sequence of progress callbacks (first is result_builder).
            result_builder: Builder to record step results (unused; builder is in progress_hooks).

        Returns:
            Tuple of (training_anomaly_score, training_forecast_score, forecast_evals, anomaly_evals, eval_df).
        """
        try:
            # FAIL FAST: strict evaluation requires an explicit holdout window.
            if eval_df is None or len(eval_df) == 0:
                raise TrainingErrorException(
                    message="Missing evaluation holdout window (eval_df is empty)",
                    error_type=MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT,
                    properties={"step": "evaluation_split"},
                )

            # Also enforce minimum sample requirements (no fallback).
            anomaly_model = models.anomaly_model
            min_samples = getattr(
                anomaly_model, "min_samples_for_cv", DEFAULT_MIN_SAMPLES_FOR_CV
            )
            total_samples = len(df) + len(eval_df)
            if total_samples < min_samples:
                raise TrainingErrorException(
                    message=(
                        "Insufficient data for quality evaluation "
                        f"({total_samples} < {min_samples} samples). "
                        "Cannot validate model quality."
                    ),
                    error_type=MonitorErrorTypeClass.TRAINING_DATA_INSUFFICIENT,
                    properties={
                        "sample_count": str(total_samples),
                        "min_samples": str(min_samples),
                    },
                )

            # Compute evaluation metrics using validation split
            forecast_evals, anomaly_evals, eval_df = compute_evaluation_metrics(
                train_df=df,
                forecast_model=models.forecast_model,
                anomaly_model=models.anomaly_model,
                ground_truth=ground_truth,
                eval_df=eval_df,
            )

            # Prefer primary anomaly score from model (scores dict or best_model_score);
            # prefer GROUND_TRUTH then CLEAN_TEST when observe-models exposes score types.
            training_anomaly_score: float
            primary = get_primary_anomaly_score(anomaly_model)
            if primary is not None:
                training_anomaly_score = primary
            elif anomaly_evals:
                training_anomaly_score = compute_anomaly_score(anomaly_evals)
            else:
                training_anomaly_score = 0.6

            training_forecast_score: Optional[float] = None
            if forecast_evals:
                y_range = None
                forecast_model = models.forecast_model
                if (
                    forecast_model is not None
                    and getattr(forecast_model, "y_range", None) is not None
                ):
                    y_range = forecast_model.y_range
                elif eval_df is not None and "y" in eval_df.columns:
                    y_range = float(eval_df["y"].max() - eval_df["y"].min())
                try:
                    training_forecast_score = compute_forecast_score(
                        forecast_evals, y_range=y_range
                    )
                except Exception as e:
                    logger.warning(f"Failed to compute forecast score: {e}")

            _notify_progress(
                progress_hooks,
                "Evaluation complete",
                None,
                "evaluation",
                StepResult(success=True),
            )
            logger.info(
                "Training scores - anomaly: %.3f, forecast: %s",
                training_anomaly_score,
                "n/a"
                if training_forecast_score is None
                else f"{training_forecast_score:.3f}",
            )
            return (
                training_anomaly_score,
                training_forecast_score,
                forecast_evals,
                anomaly_evals,
                eval_df,
            )
        except TrainingErrorException:
            # Re-raise TrainingErrorException (e.g., insufficient data) to maintain error handling
            raise
        except Exception as e:
            logger.warning(f"Model evaluation failed: {e}")
            _notify_progress(
                progress_hooks,
                str(e),
                None,
                "evaluation",
                StepResult(success=False, error=str(e)),
            )
            # Re-raise as TrainingErrorException to maintain error handling behavior
            raise TrainingErrorException(
                message=f"Model evaluation failed: {e}",
                error_type=MonitorErrorTypeClass.MODEL_EVALUATION_FAILED,
            ) from e

    @staticmethod
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

    def _capture_attempted_hyperparameters(
        self, models: InitializedModels
    ) -> Optional[Dict[str, Any]]:
        """
        Capture hyperparameters that will be attempted during training.

        This allows us to persist hyperparameters even if training fails,
        so future runs can avoid retuning with the same failed hyperparameters
        if nothing fundamental about the data has changed.

        Args:
            models: InitializedModels containing forecast and anomaly models.

        Returns:
            Dictionary with 'forecast' and 'anomaly' keys containing hyperparameter
            dictionaries, or None if capture fails.
        """
        try:
            attempted = {}

            # Capture forecast model hyperparameters
            if models.forecast_config:
                attempted["forecast"] = dict(
                    models.forecast_config.hyperparameters or {}
                )
            elif models.forecast_model:
                # Try to extract from model if config not available
                if hasattr(models.forecast_model, "get_hyperparameters"):
                    attempted["forecast"] = dict(
                        models.forecast_model.get_hyperparameters() or {}
                    )
                elif hasattr(models.forecast_model, "darts_model_config"):
                    attempted["forecast"] = dict(
                        models.forecast_model.darts_model_config or {}
                    )

            # Capture anomaly model hyperparameters
            if models.anomaly_config:
                attempted["anomaly"] = dict(models.anomaly_config.hyperparameters or {})
            elif models.anomaly_model:
                # Try to extract from model if config not available
                if hasattr(models.anomaly_model, "get_hyperparameters"):
                    attempted["anomaly"] = dict(
                        models.anomaly_model.get_hyperparameters() or {}
                    )

            return attempted if attempted else None
        except Exception as e:
            logger.warning(f"Failed to capture attempted hyperparameters: {e}")
            return None

    def _generate_future_timestamps(
        self,
        df: pd.DataFrame,
        num_intervals: int,
        interval_hours: int,
    ) -> pd.DataFrame:
        """Generate future timestamps for prediction."""
        last_ts = df["ds"].max()
        if pd.isna(last_ts):
            last_ts = pd.Timestamp.now()

        future_timestamps = pd.date_range(
            start=last_ts,
            periods=num_intervals + 1,
            freq=f"{interval_hours}h",
        )[1:]  # Skip the first one (same as last_ts)

        # Anomaly detect() expects 'ds' and 'y'; use NaN for future (no actuals)
        return pd.DataFrame({"ds": future_timestamps, "y": float("nan")})

    def _format_prediction_output(self, results: pd.DataFrame) -> pd.DataFrame:
        """
        Format detect() output for predictions with detection bands.

        Required columns: timestamp_ms, detection_band_lower, detection_band_upper
        Optional columns: y, yhat, yhat_lower, yhat_upper, anomaly_score, is_anomaly
        """
        if results is None or results.empty:
            return results

        normalized = prepare_predictions_df_for_persistence(results)

        # Enforce required columns for persistence.
        if "timestamp_ms" not in normalized.columns:
            raise ValueError("Prediction results missing 'timestamp_ms' column")
        if "detection_band_lower" not in normalized.columns:
            raise ValueError("Prediction results missing 'detection_band_lower' column")
        if "detection_band_upper" not in normalized.columns:
            raise ValueError("Prediction results missing 'detection_band_upper' column")

        return normalized
