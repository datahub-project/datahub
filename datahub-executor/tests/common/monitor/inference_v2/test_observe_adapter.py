"""Tests for ObserveAdapter and ModelFactory configuration."""

import pytest

pytest.importorskip("datahub_observe")

from unittest.mock import MagicMock, patch

from datahub_executor.common.monitor.inference_v2.inference_utils import ModelConfig
from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
    InputDataContext,
    get_defaults_for_context,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator import (
    extract_quality_score,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory import (
    ModelFactory,
)


def _make_context(
    assertion_category: str = "volume",
    is_dataframe_cumulative: bool = False,
    allow_negative: bool | None = None,
) -> InputDataContext:
    """Helper to create InputDataContext for tests."""
    return InputDataContext(
        assertion_category=assertion_category,
        is_dataframe_cumulative=is_dataframe_cumulative,
        allow_negative=allow_negative,
    )


def _make_factory(
    context: InputDataContext | None = None,
    existing_model_config: ModelConfig | None = None,
) -> ModelFactory:
    """Helper to create ModelFactory for tests."""
    if context is None:
        context = _make_context()
    defaults = get_defaults_for_context(context)
    return ModelFactory(defaults, existing_model_config)


class TestModelFactoryWarmStart:
    """Tests for warm start configuration extraction methods in ModelFactory."""

    def test_get_existing_forecast_config_from_config_object(self) -> None:
        """Extract config when deserialize returns ForecastModelConfig object."""
        from datahub_observe.algorithms.forecasting.config import ForecastModelConfig

        model_config = ModelConfig(
            forecast_config_json='{"hyperparameters": {"interval_width": 0.95}}',
            preprocessing_config_json="{}",
        )

        mock_config = ForecastModelConfig(hyperparameters={"interval_width": 0.95})

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.ForecastConfigSerializer.deserialize",
            return_value=mock_config,
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._get_existing_forecast_config()

        assert result is not None
        assert result.hyperparameters == {"interval_width": 0.95}

    def test_get_existing_forecast_config_from_dict_fallback(self) -> None:
        """Extract config when deserialize returns dict (fallback path)."""
        model_config = ModelConfig(
            forecast_config_json='{"hyperparameters": {"changepoint_prior_scale": 0.05}}',
            preprocessing_config_json="{}",
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.ForecastConfigSerializer.deserialize",
            return_value={"hyperparameters": {"changepoint_prior_scale": 0.05}},
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._get_existing_forecast_config()

        assert result is not None
        assert result.hyperparameters == {"changepoint_prior_scale": 0.05}

    def test_get_existing_forecast_config_returns_none_when_no_config(self) -> None:
        """Returns None when no existing config."""
        factory = _make_factory(existing_model_config=None)
        result = factory._get_existing_forecast_config()
        assert result is None

    def test_get_existing_anomaly_config_from_config_object(self) -> None:
        """Extract anomaly config when deserialize returns AnomalyModelConfig."""
        from datahub_observe.algorithms.anomaly_detection.config import (
            AnomalyModelConfig,
        )

        model_config = ModelConfig(
            anomaly_config_json='{"hyperparameters": {"deviation_threshold": 1.8}}',
            preprocessing_config_json="{}",
        )

        mock_config = AnomalyModelConfig(hyperparameters={"deviation_threshold": 1.8})

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.AnomalyConfigSerializer.deserialize",
            return_value=mock_config,
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._get_existing_anomaly_config()

        assert result is not None
        assert result.hyperparameters == {"deviation_threshold": 1.8}

    def test_get_existing_preprocessing_config_returns_deserialized(self) -> None:
        """Returns deserialized preprocessing config when available."""
        model_config = ModelConfig(
            preprocessing_config_json='{"type": "volume", "convert_cumulative": true}',
        )

        mock_preproc = MagicMock()
        mock_preproc.type = "volume"

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.PreprocessingConfigSerializer.deserialize",
            return_value=mock_preproc,
        ):
            factory = _make_factory(existing_model_config=model_config)
            result = factory._get_existing_preprocessing_config()

        assert result == mock_preproc

    def test_get_existing_preprocessing_config_returns_none_when_no_config(
        self,
    ) -> None:
        """Returns None when no existing model config."""
        factory = _make_factory(existing_model_config=None)
        result = factory._get_existing_preprocessing_config()
        assert result is None


class TestModelFactoryPreprocessingConfig:
    """Tests for preprocessing config building in ModelFactory."""

    def test_build_preprocessing_config_returns_volume_config_when_cumulative(
        self,
    ) -> None:
        """Returns VolumePreprocessorConfig when is_dataframe_cumulative=True for volume assertions."""
        from datahub_observe.algorithms.preprocessing.volume_preprocessor import (
            VolumePreprocessorConfig,
        )

        context = _make_context(
            assertion_category="volume",
            is_dataframe_cumulative=True,
        )
        factory = _make_factory(context=context)
        result = factory._build_preprocessing_config()

        assert isinstance(result, VolumePreprocessorConfig)
        assert result.convert_cumulative is True
        assert result.allow_negative is False

    def test_build_preprocessing_config_returns_assertion_config_when_not_cumulative(
        self,
    ) -> None:
        """Returns AssertionPreprocessingConfig when is_dataframe_cumulative=False."""
        from datahub_observe.assertions.config import AssertionPreprocessingConfig

        context = _make_context(
            assertion_category="volume",
            is_dataframe_cumulative=False,
        )
        factory = _make_factory(context=context)
        result = factory._build_preprocessing_config()

        assert isinstance(result, AssertionPreprocessingConfig)

    def test_build_preprocessing_config_uses_existing_config_if_available(
        self,
    ) -> None:
        """Uses existing preprocessing config for warm start."""
        mock_preproc = MagicMock()
        model_config = ModelConfig(
            preprocessing_config_json='{"type": "volume"}',
        )

        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.model_factory.PreprocessingConfigSerializer.deserialize",
            return_value=mock_preproc,
        ):
            context = _make_context(
                assertion_category="volume",
                is_dataframe_cumulative=True,  # Should be ignored since existing config exists
            )
            factory = _make_factory(context=context, existing_model_config=model_config)
            result = factory._build_preprocessing_config()

        assert result == mock_preproc


class TestExtractQualityScore:
    """Tests for extract_quality_score function.

    The extract_quality_score function uses the anomaly model's best_score
    from grid search, which represents true held-out validation performance.

    NOTE: Discrepancies with streamlit_explorer/model_explorer approach:
    - Streamlit computes metrics manually from detection results
    - This function uses the package's built-in grid search metrics
    See evaluator.py docstring for full details.
    """

    def test_extract_quality_score_with_f1_score(self) -> None:
        """Returns F1 score when best_score >= 0 (ground_truth was provided)."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = 0.85  # F1 score from grid search
        mock_model.min_samples_for_cv = 10

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, mock_model)

        assert result == 0.85

    def test_extract_quality_score_with_negative_score(self) -> None:
        """Normalizes negative score (without ground_truth) to 0.5-1.0 range."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = -10  # Negative anomaly count
        mock_model.min_samples_for_cv = 10

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, mock_model)

        # Normalized: 1.0 + (-10) / 40.0 = 0.75
        assert result == 0.75

    def test_extract_quality_score_with_very_negative_score(self) -> None:
        """Clamps very negative scores to 0.5 minimum."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = -100  # Very many anomalies detected
        mock_model.min_samples_for_cv = 10

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, mock_model)

        # Normalized: max(0.5, 1.0 + (-100) / 40.0) = max(0.5, -1.5) = 0.5
        assert result == 0.5

    def test_extract_quality_score_with_none_best_score(self) -> None:
        """Returns 1.0 when best_score is None (cached/default params)."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = None  # Using cached or default params
        mock_model.min_samples_for_cv = 10

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, mock_model)

        assert result == 1.0

    def test_extract_quality_score_with_none_model(self) -> None:
        """Returns 1.0 when no anomaly model available."""
        import pandas as pd

        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=20), "y": range(20)}
        )
        result = extract_quality_score(train_df, None)

        assert result == 1.0

    def test_extract_quality_score_fails_with_insufficient_data(self) -> None:
        """Raises RuntimeError when data is below min_samples_for_cv."""
        import pandas as pd

        mock_model = MagicMock()
        mock_model.best_score = 0.85
        mock_model.min_samples_for_cv = 10

        # Only 5 samples, below threshold of 10
        train_df = pd.DataFrame(
            {"ds": pd.date_range("2024-01-01", periods=5), "y": range(5)}
        )

        with pytest.raises(RuntimeError, match="Insufficient data"):
            extract_quality_score(train_df, mock_model)
