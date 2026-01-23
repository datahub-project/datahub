"""
Adapter for observe-models package integration.

This module provides a simplified adapter that leverages datahub_observe's built-in
pipeline. The anomaly model handles preprocessing and forecasting internally via
composition with the forecast model.

Design: Single train() call handles the full pipeline internally.
"""

import logging
from typing import Optional

import pandas as pd
from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException
from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig
from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
    InputDataContext,
    ObserveDefaultsBuilder,
    get_defaults_for_context,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator import (
    compute_evaluation_metrics,
    extract_quality_score,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
    InitializedModels,
    ModelFactory,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
    build_model_config,
)
from datahub_executor.common.monitor.inference_v2.types import (
    StepResult,
    TrainingResult,
)

logger = logging.getLogger(__name__)

# Stubbed quality threshold - can be refined later
DEFAULT_QUALITY_THRESHOLD = 0.5


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
        context: InputDataContext,
        num_intervals: int,
        interval_hours: int = 1,
        force_retune: bool = False,
        sensitivity_level: int = 5,
        ground_truth: Optional[pd.DataFrame] = None,
        existing_model_config: Optional[ModelConfig] = None,
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
            context: InputDataContext with metadata about the input data's shape
                (category, whether cumulative). Used to determine appropriate
                preprocessing and model configuration.
            num_intervals: Number of future intervals to predict.
            interval_hours: Size of each interval in hours.
            force_retune: If True, ignore existing hyperparameters and force fresh
                tuning. Set when hyperparameters are stale (exceeded retune threshold).
            sensitivity_level: Sensitivity level for anomaly detection (1-10).
                Higher values detect more anomalies. TODO: implement.
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns
                for anomaly feedback. is_anomaly_gt should be True for confirmed
                anomalies (status != REJECTED).
            existing_model_config: Previously trained model config for warm start.

        Returns:
            TrainingResult containing prediction_df (with detection bands),
            model_config, and step_results tracking success/failure of each step.
        """
        step_results: dict[str, StepResult] = {}
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

        # ------------------------------------------------------------------------ #
        # ------- TODO: migrate to supplier pattern / fallbacks workflow --------- #

        # Build defaults from context (describes the input df's characteristics)
        defaults = get_defaults_for_context(context)

        # Steps 1-3: Create models, train, and evaluate quality
        models, quality_score, forecast_evals, anomaly_evals, has_tuned, eval_df = (
            self._train_and_evaluate(
                df=df,
                defaults=defaults,
                existing_model_config=existing_model_config,
                step_results=step_results,
                force_retune=force_retune,
                ground_truth=ground_truth,
            )
        )

        # Quality-based retuning: if quality is bad and we haven't already retuned,
        # retry with fresh tuning to see if we can get better results.
        if quality_score < DEFAULT_QUALITY_THRESHOLD and not has_tuned:
            logger.info(
                f"Quality score {quality_score:.3f} below threshold "
                f"{DEFAULT_QUALITY_THRESHOLD}, forcing retune and retraining"
            )
            models, quality_score, forecast_evals, anomaly_evals, _, eval_df = (
                self._train_and_evaluate(
                    df=df,
                    defaults=defaults,
                    existing_model_config=existing_model_config,
                    step_results=step_results,
                    force_retune=True,
                    ground_truth=ground_truth,
                )
            )
            logger.info(f"Post-retune quality score: {quality_score:.3f}")

        if quality_score < DEFAULT_QUALITY_THRESHOLD:
            logger.warning(
                "Training quality score %.3f below threshold %.3f",
                quality_score,
                DEFAULT_QUALITY_THRESHOLD,
            )
            raise TrainingErrorException(
                message=(
                    "Training quality score "
                    f"{quality_score:.3f} below threshold {DEFAULT_QUALITY_THRESHOLD:.3f}"
                ),
                error_type=MonitorErrorTypeClass.PREDICTION_NOT_CONFIDENT,
                properties={
                    "quality_score": str(quality_score),
                    "quality_threshold": str(DEFAULT_QUALITY_THRESHOLD),
                },
            )

        # Step 4: Get predictions with detection bands using detect()
        # NOTE: detect() gives us predictions WITH detection bands
        # This is used to generate future prediction intervals for the assertion
        future_df = self._generate_future_timestamps(df, num_intervals, interval_hours)
        prediction_df: Optional[pd.DataFrame] = None
        try:
            assert models.anomaly_model is not None  # create_models() always sets this
            raw_prediction = models.anomaly_model.detect(future_df)
            prediction_df = self._format_prediction_output(raw_prediction)
            step_results["prediction"] = StepResult(success=True)
        except Exception as e:
            logger.warning(f"Prediction with detection bands failed: {e}")
            step_results["prediction"] = StepResult(success=False, error=str(e))

        # Step 5: Build model config for persistence
        # create_models() always sets these fields
        assert models.forecast_registry_key is not None
        assert models.anomaly_registry_key is not None
        model_config = build_model_config(
            forecast_model=models.forecast_model,
            forecast_config=models.forecast_config,
            anomaly_model=models.anomaly_model,
            anomaly_config=models.anomaly_config,
            preprocessing_config=models.preprocessing_config,
            quality_score=quality_score,
            forecast_evals=forecast_evals,
            anomaly_evals=anomaly_evals,
            has_detection_bands=(prediction_df is not None),
            forecast_registry_key=models.forecast_registry_key,
            anomaly_registry_key=models.anomaly_registry_key,
            train_df=df,
            eval_df=eval_df,
        )

        return TrainingResult(
            prediction_df=prediction_df,
            model_config=model_config,
            step_results=step_results,
        )

    def _train_and_evaluate(
        self,
        df: pd.DataFrame,
        ground_truth: Optional[pd.DataFrame],
        defaults: "ObserveDefaultsBuilder",
        existing_model_config: Optional[ModelConfig],
        step_results: dict[str, StepResult],
        force_retune: bool = False,
    ) -> tuple[
        InitializedModels,
        float,
        dict[str, float],
        dict[str, float],
        bool,
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
            step_results: Dict to record step results.
            force_retune: If True, ignore existing hyperparameters and use defaults
                to trigger fresh tuning.

        Returns:
            Tuple of (models, quality_score, forecast_evals, anomaly_evals, has_tuned, eval_df).
            models is an InitializedModels containing forecast_model, anomaly_model,
            and their configs.
            has_tuned is True if tuning occurred during training (either because
            force_retune was True or because no hyperparameters existed).
            eval_df is the holdout DataFrame used for evaluation.

        Raises:
            ModelCreationError: If model creation fails.
            ModelTrainingError: If model training fails.
        """
        # Step 1: Create and configure models
        try:
            factory = ModelFactory(
                defaults, existing_model_config, force_retune=force_retune
            )
            models = factory.create_models()
            step_results["model_creation"] = StepResult(success=True)
        except Exception as e:
            logger.error(f"Model creation failed: {e}")
            raise TrainingErrorException(
                message=f"Model creation failed: {e}",
                error_type=MonitorErrorTypeClass.MODEL_CREATION_FAILED,
            ) from e

        # Determine if tuning will occur: happens when hyperparameters are empty
        # (either due to force_retune or no existing hyperparameters)
        # TODO: also handle anomaly model hyperparameters.
        hyperparams_empty = not (
            models.forecast_config and models.forecast_config.hyperparameters
        )
        tuning_required = force_retune or hyperparams_empty

        # Step 2: Train the anomaly model (handles full pipeline internally)
        try:
            assert models.anomaly_model is not None  # create_models() always sets this
            models.anomaly_model.train(df, ground_truth=ground_truth)
            step_results["training"] = StepResult(success=True)
        except Exception as e:
            logger.error(f"Training failed: {e}")
            step_results["training"] = StepResult(success=False, error=str(e))
            raise TrainingErrorException(
                message=f"Training failed: {e}",
                error_type=MonitorErrorTypeClass.MODEL_TRAINING_FAILED,
            ) from e

        # Step 3: Evaluate training quality
        try:
            quality_score, forecast_evals, anomaly_evals, eval_df = (
                self._evaluate_quality(df, models, ground_truth, step_results)
            )
        except TrainingErrorException:
            raise
        except Exception as e:
            logger.warning(f"Model evaluation failed: {e}")
            step_results["evaluation"] = StepResult(success=False, error=str(e))
            raise TrainingErrorException(
                message=f"Model evaluation failed: {e}",
                error_type=MonitorErrorTypeClass.MODEL_EVALUATION_FAILED,
            ) from e
        # After training completes, tuning_required becomes has_tuned
        has_tuned = tuning_required
        return models, quality_score, forecast_evals, anomaly_evals, has_tuned, eval_df

    def _evaluate_quality(
        self,
        df: pd.DataFrame,
        models: InitializedModels,
        ground_truth: Optional[pd.DataFrame],
        step_results: dict[str, StepResult],
    ) -> tuple[float, dict[str, float], dict[str, float], Optional[pd.DataFrame]]:
        """Evaluate model quality using grid search metrics and validation split.

        Args:
            df: Training DataFrame.
            models: InitializedModels containing forecast and anomaly models.
            ground_truth: Optional DataFrame with 'ds' and 'is_anomaly_gt' columns.
                If provided, anomaly evaluation returns precision/recall/f1/accuracy.
            step_results: Dict to record evaluation step result.

        Returns:
            Tuple of (quality_score, forecast_evals, anomaly_evals, eval_df).
        """
        # Extract quality score from grid search best_score
        # This is the TRUE validation metric from hyperparameter tuning
        quality_score = extract_quality_score(df, models.anomaly_model)

        # Compute evaluation metrics using validation split
        # Note: These are "optimistic" for forecast since model saw full data
        # See evaluator.py docstring for discrepancies with streamlit_explorer
        forecast_evals, anomaly_evals, eval_df = compute_evaluation_metrics(
            train_df=df,
            forecast_model=models.forecast_model,
            anomaly_model=models.anomaly_model,
            ground_truth=ground_truth,
        )

        step_results["evaluation"] = StepResult(success=True)
        logger.info(f"Training quality score: {quality_score:.3f}")
        return quality_score, forecast_evals, anomaly_evals, eval_df

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

        return pd.DataFrame({"ds": future_timestamps})

    def _format_prediction_output(self, results: pd.DataFrame) -> pd.DataFrame:
        """
        Format detect() output for predictions with detection bands.

        Required columns: timestamp_ms, detection_band_lower, detection_band_upper
        Optional columns: y, yhat, yhat_lower, yhat_upper, anomaly_score, is_anomaly
        """
        result = pd.DataFrame()

        if "ds" in results.columns:
            result["timestamp_ms"] = results["ds"].astype("int64") // 10**6
        else:
            raise TrainingErrorException(
                message="Prediction results missing 'ds' column",
                error_type=MonitorErrorTypeClass.PREDICTION_FORMAT_ERROR,
                properties={"detail": "missing_prediction_timestamp"},
            )

        result["detection_band_lower"] = results.get("detection_band_lower", 0)
        result["detection_band_upper"] = results.get("detection_band_upper", 0)

        result["yhat_lower"] = results.get("yhat_lower")
        result["yhat_upper"] = results.get("yhat_upper")

        for col in ["y", "yhat"]:
            if col in results.columns:
                result[col] = results[col]

        # NOTE: we can ignore these columns for now, they are not used in the prediction.
        if "anomaly_score" in results.columns:
            result["anomaly_score"] = results["anomaly_score"]

        if "is_anomaly" in results.columns:
            result["is_anomaly"] = results["is_anomaly"]

        return result
