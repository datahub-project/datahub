"""Tests for inference_utils module."""

import json

import pandas as pd
import pytest
from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInferenceDetailsClass,
    EmbeddedAssertionClass,
)
from datahub_observe.registry.types import ModelType

# Reuse shared URN utilities
from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    decode_metric_cube_urn,
    decode_monitor_urn,
    encode_monitor_urn,
    make_monitor_metric_cube_urn,
)
from datahub_executor.common.monitor.inference_v2.inference_utils import (
    OBSERVE_MODELS_VERSION,
    AnomalyAssertions,
    ForecastAssertions,
    FreshnessAssertions,
    ModelConfig,
    # Inference details
    build_evaluation_context,
    build_inference_details,
    parse_inference_details,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter.serialization import (
    AnomalyConfigSerializer,
    ForecastConfigSerializer,
    PreprocessingConfigSerializer,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_model_config() -> ModelConfig:
    """Create a sample ModelConfig for testing."""
    return ModelConfig(
        forecast_model_name="prophet",
        forecast_model_version="0.1.0",
        forecast_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"changepoint_prior_scale": 0.05}}',
        forecast_evals_json='{"_schemaVersion": "1.0.0", "runs": [{"mae": 0.05, "rmse": 0.08, "mape": 3.5, "train_start_millis": 1704067200000, "train_end_millis": 1706745600000, "test_start_millis": 1706745600001, "test_end_millis": 1707350400000}]}',
        preprocessing_config_json='{"type": "volume", "convert_cumulative": true, "_schemaVersion": "1.0.0"}',
        generated_at=1707350400000,
    )


@pytest.fixture
def sample_anomaly_model_config() -> ModelConfig:
    """Create a sample ModelConfig for anomaly detection testing."""
    return ModelConfig(
        forecast_model_name="prophet",
        forecast_model_version="0.1.0",
        forecast_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"changepoint_prior_scale": 0.05}}',
        forecast_evals_json='{"_schemaVersion": "1.0.0", "runs": [{"mae": 0.05, "rmse": 0.08, "mape": 3.5, "train_start_millis": 1704067200000, "train_end_millis": 1706745600000, "test_start_millis": 1706745600001, "test_end_millis": 1707350400000}]}',
        anomaly_model_name="datahub_forecast_anomaly",
        anomaly_model_version="0.1.0",
        anomaly_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"deviation_threshold": 1.8}}',
        anomaly_evals_json='{"_schemaVersion": "1.0.0", "runs": [{"precision": 0.85, "recall": 0.78, "f1_score": 0.81, "train_start_millis": 1704067200000, "train_end_millis": 1706745600000, "test_start_millis": 1706745600001, "test_end_millis": 1707350400000}]}',
        anomaly_score=0.95,
        preprocessing_config_json='{"type": "volume", "convert_cumulative": true, "_schemaVersion": "1.0.0"}',
        generated_at=1707350400000,
    )


@pytest.fixture
def sample_forecast_df() -> pd.DataFrame:
    """Create a sample forecast DataFrame for testing."""
    return pd.DataFrame(
        {
            "timestamp_ms": [1704067200000, 1704070800000, 1704074400000],
            "y": [100.0, 105.0, 110.0],
            "yhat": [102.0, 103.0, 108.0],
            "yhat_lower": [95.0, 96.0, 101.0],
            "yhat_upper": [109.0, 110.0, 115.0],
        }
    )


@pytest.fixture
def sample_anomaly_df() -> pd.DataFrame:
    """Create a sample anomaly detection DataFrame for testing."""
    return pd.DataFrame(
        {
            "timestamp_ms": [1704067200000, 1704070800000],
            "y": [100.0, 150.0],
            "yhat": [102.0, 105.0],
            "yhat_lower": [95.0, 98.0],
            "yhat_upper": [109.0, 112.0],
            "detection_band_lower": [80.0, 85.0],
            "detection_band_upper": [120.0, 125.0],
            "anomaly_score": [0.5, 2.5],
            "is_anomaly": [False, True],
        }
    )


# =============================================================================
# ModelType Enum Tests
# =============================================================================


class TestModelType:
    """Tests for ModelType enum (from observe-models registry)."""

    def test_model_type_values(self) -> None:
        """Test that ModelType enum has expected values matching observe-models."""
        assert ModelType.FORECAST.value == "forecast"
        assert ModelType.ANOMALY.value == "anomaly"
        assert ModelType.FRESHNESS.value == "freshness"

    def test_model_type_value_access(self) -> None:
        """Test that ModelType values can be accessed for serialization."""
        assert ModelType.FORECAST.value == "forecast"
        assert ModelType.ANOMALY.name == "ANOMALY"


# =============================================================================
# URN Utility Tests
# =============================================================================


class TestUrnUtilities:
    """Tests for URN encoding/decoding utilities."""

    def test_encode_decode_monitor_urn_simple(self) -> None:
        """Test encoding and decoding a simple monitor URN."""
        urn = "urn:li:monitor:123"
        encoded = encode_monitor_urn(urn)

        assert isinstance(encoded, str)
        assert encoded != urn

        decoded = decode_monitor_urn(encoded)
        assert decoded == urn

    def test_encode_decode_monitor_urn_complex(self) -> None:
        """Test encoding and decoding a complex monitor URN."""
        urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD),field_stats)"
        encoded = encode_monitor_urn(urn)
        decoded = decode_monitor_urn(encoded)
        assert decoded == urn

    def test_make_metric_cube_urn(self) -> None:
        """Test creating a MetricCube URN from a monitor URN."""
        monitor_urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.test_table,PROD),test_monitor)"
        metric_cube_urn = make_monitor_metric_cube_urn(monitor_urn)

        assert metric_cube_urn.startswith("urn:li:dataHubMetricCube:")

    def test_decode_metric_cube_urn(self) -> None:
        """Test extracting monitor URN from a MetricCube URN."""
        monitor_urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.test_table,PROD),test_monitor)"
        metric_cube_urn = make_monitor_metric_cube_urn(monitor_urn)

        extracted = decode_metric_cube_urn(metric_cube_urn)
        assert extracted == monitor_urn

    def test_decode_metric_cube_urn_invalid(self) -> None:
        """Test extracting from an invalid URN returns None."""
        result = decode_metric_cube_urn("invalid-urn")
        assert result is None


class TestPydanticModelFeatures:
    """Tests for Pydantic model features on ModelConfig."""

    def test_model_config_model_dump(self, sample_model_config: ModelConfig) -> None:
        """Test that ModelConfig can be serialized with model_dump."""
        data = sample_model_config.model_dump()

        assert data["forecast_model_name"] == "prophet"
        assert data["forecast_model_version"] == "0.1.0"
        assert data.get("confidence") is None

    def test_model_config_model_dump_json(
        self, sample_model_config: ModelConfig
    ) -> None:
        """Test that ModelConfig can be serialized to JSON."""
        json_str = sample_model_config.model_dump_json()
        data = json.loads(json_str)

        assert data["forecast_model_name"] == "prophet"

    def test_model_config_model_validate(self) -> None:
        """Test that ModelConfig can be validated from dict."""
        data = {
            "forecast_model_name": "arima",
            "forecast_model_version": "2.0.0",
            "forecast_config_json": '{"_schemaVersion": "1.0.0", "hyperparameters": {}}',
            "preprocessing_config_json": '{"_schemaVersion": "1.0.0"}',
        }
        config = ModelConfig.model_validate(data)

        assert config.forecast_model_name == "arima"
        assert config.forecast_model_version == "2.0.0"

    def test_model_config_with_anomaly_model(
        self, sample_anomaly_model_config: ModelConfig
    ) -> None:
        """Test ModelConfig with both forecast and anomaly model info."""
        data = sample_anomaly_model_config.model_dump()

        assert data["forecast_model_name"] == "prophet"
        assert data["forecast_model_version"] == "0.1.0"
        assert data["anomaly_model_name"] == "datahub_forecast_anomaly"
        assert data["anomaly_model_version"] == "0.1.0"


# =============================================================================
# Inference Details Tests
# =============================================================================


class TestBuildInferenceDetails:
    """Tests for build_inference_details function."""

    def test_build_inference_details_basic(
        self, sample_model_config: ModelConfig
    ) -> None:
        """Test building inference details with basic config."""
        result = build_inference_details(
            model_config=sample_model_config,
            generated_at_millis=1707350400000,
        )

        assert isinstance(result, AssertionInferenceDetailsClass)
        assert result.modelId == "observe-models"
        assert result.modelVersion == OBSERVE_MODELS_VERSION
        # Backward-compatibility: confidence mirrors anomalyScore when present,
        # otherwise forecastScore. This sample config has neither.
        assert result.confidence is None
        assert result.generatedAt == 1707350400000

    def test_build_inference_details_uses_model_config_generated_at(
        self, sample_model_config: ModelConfig
    ) -> None:
        """Test that generated_at falls back to model_config.generated_at."""
        result = build_inference_details(model_config=sample_model_config)
        assert result.generatedAt == sample_model_config.generated_at

    def test_build_inference_details_parameters(
        self, sample_model_config: ModelConfig
    ) -> None:
        """Test that parameters map is correctly populated."""
        result = build_inference_details(sample_model_config)

        assert result.parameters is not None
        assert result.parameters["forecastModelName"] == "prophet"
        assert result.parameters["forecastModelVersion"] == "0.1.0"
        assert "preprocessingConfigJson" in result.parameters
        assert "forecastConfigJson" in result.parameters
        assert "forecastEvalsJson" in result.parameters
        assert "train_start_millis" in result.parameters["forecastEvalsJson"]
        assert "configSchemaVersion" not in result.parameters

    def test_build_inference_details_with_anomaly_model(
        self, sample_anomaly_model_config: ModelConfig
    ) -> None:
        """Test building inference details with both forecast and anomaly model info."""
        result = build_inference_details(sample_anomaly_model_config)

        assert result.parameters is not None
        assert result.parameters["forecastModelName"] == "prophet"
        assert result.parameters["forecastModelVersion"] == "0.1.0"
        assert result.parameters["anomalyModelName"] == "datahub_forecast_anomaly"
        assert result.parameters["anomalyModelVersion"] == "0.1.0"

    def test_build_inference_details_forecast_only(self) -> None:
        """Test building inference details for forecast-only model."""
        config = ModelConfig(
            forecast_model_name="prophet",
            forecast_model_version="0.1.0",
            forecast_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {}}',
            preprocessing_config_json='{"_schemaVersion": "1.0.0"}',
        )

        result = build_inference_details(config)

        assert result.parameters is not None
        assert result.parameters["forecastModelName"] == "prophet"
        assert result.parameters["forecastModelVersion"] == "0.1.0"
        assert "forecastConfigJson" in result.parameters
        assert "anomalyModelName" not in result.parameters
        assert "anomalyModelVersion" not in result.parameters
        assert "anomalyConfigJson" not in result.parameters


class TestParseInferenceDetails:
    """Tests for parse_inference_details function."""

    def test_parse_inference_details_round_trip(
        self, sample_model_config: ModelConfig
    ) -> None:
        """Test that building and parsing is reversible."""
        details = build_inference_details(sample_model_config)
        parsed = parse_inference_details(details)

        assert parsed is not None
        assert parsed.forecast_model_name == sample_model_config.forecast_model_name
        assert (
            parsed.forecast_model_version == sample_model_config.forecast_model_version
        )
        assert (
            parsed.preprocessing_config_json
            == sample_model_config.preprocessing_config_json
        )
        assert parsed.forecast_evals_json == sample_model_config.forecast_evals_json
        assert parsed.generated_at == sample_model_config.generated_at

    def test_parse_inference_details_with_anomaly_model(
        self, sample_anomaly_model_config: ModelConfig
    ) -> None:
        """Test parsing with both forecast and anomaly model info."""
        details = build_inference_details(sample_anomaly_model_config)
        parsed = parse_inference_details(details)

        assert parsed is not None
        assert (
            parsed.forecast_model_name
            == sample_anomaly_model_config.forecast_model_name
        )
        assert (
            parsed.anomaly_model_name == sample_anomaly_model_config.anomaly_model_name
        )
        assert (
            parsed.anomaly_model_version
            == sample_anomaly_model_config.anomaly_model_version
        )

    def test_parse_inference_details_missing_parameters(self) -> None:
        """Test parsing with missing parameters returns None."""
        details = AssertionInferenceDetailsClass(
            modelId="observe-models",
            parameters=None,
        )

        result = parse_inference_details(details)
        assert result is None

    def test_parse_inference_details_incomplete_parameters(self) -> None:
        """Test parsing with missing preprocessingConfigJson returns None."""
        details = AssertionInferenceDetailsClass(
            modelId="observe-models",
            parameters={"forecastConfigJson": "{}"},
        )

        result = parse_inference_details(details)
        assert result is None

    def test_parse_inference_details_warns_on_wrong_model_id(
        self, sample_model_config: ModelConfig, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that parsing logs a warning when modelId is not 'observe-models'."""
        details = build_inference_details(sample_model_config)
        details.modelId = "wrong-model-id"

        parsed = parse_inference_details(details)

        assert parsed is not None
        assert "Unexpected modelId" in caplog.text


# =============================================================================
# Serialization Tests
# =============================================================================


class TestPreprocessingConfigSerializer:
    """Tests for PreprocessingConfigSerializer namespace."""

    def test_serialize_none(self) -> None:
        """Test serializing None returns None."""
        result = PreprocessingConfigSerializer.serialize(None)  # type: ignore[arg-type]
        assert result is None

    def test_deserialize_invalid_json(self) -> None:
        """Test deserializing invalid JSON returns None."""
        result = PreprocessingConfigSerializer.deserialize("not valid json")
        assert result is None

    def test_deserialize_empty(self) -> None:
        """Test deserializing empty dict returns a valid config or dict."""
        result = PreprocessingConfigSerializer.deserialize("{}")
        # Empty dict can be deserialized into a default PreprocessingConfig
        assert result is not None


class TestForecastConfigSerializer:
    """Tests for ForecastConfigSerializer namespace."""

    def test_serialize_none(self) -> None:
        """Test serializing None returns None."""
        result = ForecastConfigSerializer.serialize(None)  # type: ignore[arg-type]
        assert result is None

    def test_deserialize_invalid_json(self) -> None:
        """Test deserializing invalid JSON returns None."""
        result = ForecastConfigSerializer.deserialize("not valid json")
        assert result is None

    def test_serialize_round_trip(self) -> None:
        """Test ForecastModelConfig serialization round-trip."""
        try:
            from datahub_observe.algorithms.forecasting.config import (
                ForecastModelConfig,
            )

            config = ForecastModelConfig(hyperparameters={"interval_width": 0.95})
            json_str = ForecastConfigSerializer.serialize(config)

            assert json_str is not None
            assert "interval_width" in json_str
            assert "_schemaVersion" in json_str

            result = ForecastConfigSerializer.deserialize(json_str)
            assert result is not None

            if hasattr(result, "hyperparameters"):
                assert result.hyperparameters["interval_width"] == 0.95
            else:
                assert isinstance(result, dict)
                assert result.get("hyperparameters", {}).get("interval_width") == 0.95
        except ImportError:
            pytest.skip("observe-models not available")


class TestAnomalyConfigSerializer:
    """Tests for AnomalyConfigSerializer namespace."""

    def test_serialize_none(self) -> None:
        """Test serializing None returns None."""
        result = AnomalyConfigSerializer.serialize(None)  # type: ignore[arg-type]
        assert result is None

    def test_deserialize_invalid_json(self) -> None:
        """Test deserializing invalid JSON returns None."""
        result = AnomalyConfigSerializer.deserialize("not valid json")
        assert result is None

    def test_serialize_round_trip(self) -> None:
        """Test AnomalyModelConfig serialization round-trip."""
        try:
            from datahub_observe.algorithms.anomaly_detection.config import (
                AnomalyModelConfig,
            )

            config = AnomalyModelConfig(hyperparameters={"deviation_threshold": 1.8})
            json_str = AnomalyConfigSerializer.serialize(config)

            assert json_str is not None
            assert "deviation_threshold" in json_str
            assert "_schemaVersion" in json_str

            result = AnomalyConfigSerializer.deserialize(json_str)
            assert result is not None

            if hasattr(result, "hyperparameters"):
                assert result.hyperparameters["deviation_threshold"] == 1.8
            else:
                assert isinstance(result, dict)
                assert (
                    result.get("hyperparameters", {}).get("deviation_threshold") == 1.8
                )
        except ImportError:
            pytest.skip("observe-models not available")


class TestInferenceDetailsWithConfigs:
    """Tests for building inference details with config namespaces."""

    def test_build_inference_details_with_both_configs(self) -> None:
        """Test building inference details with both forecast and anomaly configs."""
        config = ModelConfig(
            forecast_model_name="prophet",
            forecast_model_version="0.1.0",
            forecast_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"interval_width": 0.95}}',
            anomaly_model_name="datahub_forecast_anomaly",
            anomaly_model_version="0.1.0",
            anomaly_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"deviation_threshold": 1.8}}',
            preprocessing_config_json='{"_schemaVersion": "1.0.0"}',
        )

        result = build_inference_details(config)

        assert result.parameters is not None
        assert "forecastConfigJson" in result.parameters
        assert "anomalyConfigJson" in result.parameters
        assert "interval_width" in result.parameters["forecastConfigJson"]
        assert "deviation_threshold" in result.parameters["anomalyConfigJson"]

    def test_parse_inference_details_with_both_configs(self) -> None:
        """Test parsing inference details preserves both configs."""
        config = ModelConfig(
            forecast_model_name="prophet",
            forecast_model_version="0.1.0",
            forecast_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"interval_width": 0.95}}',
            anomaly_model_name="datahub_forecast_anomaly",
            anomaly_model_version="0.1.0",
            anomaly_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"deviation_threshold": 1.8}}',
            preprocessing_config_json='{"_schemaVersion": "1.0.0"}',
        )

        details = build_inference_details(config)
        parsed = parse_inference_details(details)

        assert parsed is not None
        assert parsed.forecast_config_json == config.forecast_config_json
        assert parsed.anomaly_config_json == config.anomaly_config_json


# =============================================================================
# ForecastAssertions Tests
# =============================================================================


class TestForecastAssertions:
    """Tests for ForecastAssertions namespace."""

    def test_from_df_basic(self, sample_forecast_df: pd.DataFrame) -> None:
        """Test converting forecast DataFrame to embedded assertions."""
        entity_urn = "urn:li:dataset:(urn:li:dataPlatform:test,test_table,PROD)"

        result = ForecastAssertions.from_df(sample_forecast_df, entity_urn)

        assert len(result) == 3
        assert all(isinstance(ea, EmbeddedAssertionClass) for ea in result)

    def test_from_df_missing_required_columns_raises_error(self) -> None:
        """Test that missing required columns raises ValueError."""
        df = pd.DataFrame(
            {
                "timestamp_ms": [1704067200000],
                "y": [100.0],
                "yhat": [102.0],
            }
        )
        entity_urn = "urn:li:dataset:test"

        with pytest.raises(ValueError) as exc_info:
            ForecastAssertions.from_df(df, entity_urn)

        assert "Missing required columns" in str(exc_info.value)

    def test_from_df_timestamps(self, sample_forecast_df: pd.DataFrame) -> None:
        """Test that timestamps are correctly set."""
        entity_urn = "urn:li:dataset:test"

        result = ForecastAssertions.from_df(sample_forecast_df, entity_urn)

        assert result[0].evaluationTimeWindow is not None
        assert result[0].evaluationTimeWindow.startTimeMillis == 1704067200000
        assert result[1].evaluationTimeWindow is not None
        assert result[1].evaluationTimeWindow.startTimeMillis == 1704070800000

    def test_from_df_ci_bounds(self, sample_forecast_df: pd.DataFrame) -> None:
        """Test that CI bounds are in minValue/maxValue."""
        entity_urn = "urn:li:dataset:test"

        result = ForecastAssertions.from_df(sample_forecast_df, entity_urn)

        assertion = result[0].assertion
        assert assertion is not None
        assert assertion.volumeAssertion is not None
        assert assertion.volumeAssertion.rowCountTotal is not None

        params = assertion.volumeAssertion.rowCountTotal.parameters
        assert params is not None
        assert params.minValue is not None
        assert params.minValue.value == "95.0"
        assert params.maxValue is not None
        assert params.maxValue.value == "109.0"

    def test_from_df_context_values(self, sample_forecast_df: pd.DataFrame) -> None:
        """Test that y, yhat, and CI bounds are in context map."""
        entity_urn = "urn:li:dataset:test"

        result = ForecastAssertions.from_df(sample_forecast_df, entity_urn)

        context = result[0].context
        assert context is not None
        assert context["y"] == "100.0"
        assert context["yhat"] == "102.0"
        assert context["yhatLower"] == "95.0"
        assert context["yhatUpper"] == "109.0"

    def test_round_trip(self, sample_forecast_df: pd.DataFrame) -> None:
        """Test that conversion to assertions and back preserves data."""
        entity_urn = "urn:li:dataset:test"

        assertions = ForecastAssertions.from_df(sample_forecast_df, entity_urn)
        result_df = ForecastAssertions.to_df(assertions)

        assert len(result_df) == len(sample_forecast_df)
        assert "timestamp_ms" in result_df.columns
        assert "yhat_lower" in result_df.columns
        assert "yhat_upper" in result_df.columns
        assert "y" in result_df.columns
        assert "yhat" in result_df.columns

        assert result_df.iloc[0]["timestamp_ms"] == 1704067200000
        assert result_df.iloc[0]["y"] == 100.0
        assert result_df.iloc[0]["yhat"] == 102.0


# =============================================================================
# AnomalyAssertions Tests
# =============================================================================


class TestAnomalyAssertions:
    """Tests for AnomalyAssertions namespace."""

    def test_from_df_basic(self, sample_anomaly_df: pd.DataFrame) -> None:
        """Test converting anomaly DataFrame to embedded assertions."""
        entity_urn = "urn:li:dataset:test"

        result = AnomalyAssertions.from_df(sample_anomaly_df, entity_urn)

        assert len(result) == 2
        assert all(isinstance(ea, EmbeddedAssertionClass) for ea in result)

    def test_from_df_missing_required_columns_raises_error(self) -> None:
        """Test that missing required columns raises ValueError."""
        df = pd.DataFrame(
            {
                "timestamp_ms": [1704067200000],
                "y": [100.0],
                "anomaly_score": [0.5],
                "is_anomaly": [False],
            }
        )
        entity_urn = "urn:li:dataset:test"

        with pytest.raises(ValueError) as exc_info:
            AnomalyAssertions.from_df(df, entity_urn)

        assert "Missing required columns" in str(exc_info.value)

    def test_from_df_detection_bands(self, sample_anomaly_df: pd.DataFrame) -> None:
        """Test that detection bands are in minValue/maxValue."""
        entity_urn = "urn:li:dataset:test"

        result = AnomalyAssertions.from_df(sample_anomaly_df, entity_urn)

        assertion = result[0].assertion
        assert assertion is not None
        assert assertion.volumeAssertion is not None
        params = assertion.volumeAssertion.rowCountTotal.parameters  # type: ignore
        assert params is not None
        assert params.minValue is not None
        assert params.minValue.value == "80.0"
        assert params.maxValue is not None
        assert params.maxValue.value == "120.0"

    def test_from_df_context_values(self, sample_anomaly_df: pd.DataFrame) -> None:
        """Test that anomaly-specific values are in context map."""
        entity_urn = "urn:li:dataset:test"

        result = AnomalyAssertions.from_df(sample_anomaly_df, entity_urn)

        context = result[1].context
        assert context is not None
        assert context["y"] == "150.0"
        assert context["yhat"] == "105.0"
        assert context["yhatLower"] == "98.0"
        assert context["yhatUpper"] == "112.0"
        assert context["anomalyScore"] == "2.5"
        assert context["isAnomaly"] == "true"
        assert context["detectionBandLower"] == "85.0"
        assert context["detectionBandUpper"] == "125.0"

    def test_round_trip(self, sample_anomaly_df: pd.DataFrame) -> None:
        """Test that conversion to assertions and back preserves data."""
        entity_urn = "urn:li:dataset:test"

        assertions = AnomalyAssertions.from_df(sample_anomaly_df, entity_urn)
        result_df = AnomalyAssertions.to_df(assertions)

        assert len(result_df) == len(sample_anomaly_df)
        assert "detection_band_lower" in result_df.columns
        assert "detection_band_upper" in result_df.columns
        assert "anomaly_score" in result_df.columns
        assert "is_anomaly" in result_df.columns

        assert result_df.iloc[1]["y"] == 150.0
        assert result_df.iloc[1]["anomaly_score"] == 2.5
        assert result_df.iloc[1]["is_anomaly"] == True  # noqa: E712


# =============================================================================
# FreshnessAssertions Tests
# =============================================================================


class TestFreshnessAssertions:
    """Tests for FreshnessAssertions namespace."""

    def test_from_df_basic(self) -> None:
        """Test converting freshness DataFrame to embedded assertions."""
        df = pd.DataFrame(
            {
                "timestamp_ms": [1704067200000, 1704070800000],
                "expected_next_event_millis": [1704080000000, 1704090000000],
                "is_fresh": [True, False],
            }
        )
        entity_urn = "urn:li:dataset:test"

        result = FreshnessAssertions.from_df(df, entity_urn)

        assert len(result) == 2

        context = result[0].context
        assert context is not None
        assert context["expectedNextEventMillis"] == "1704080000000"
        assert context["isFresh"] == "true"

        context = result[1].context
        assert context is not None
        assert context["isFresh"] == "false"

    def test_from_df_uses_freshness_assertion_type(self) -> None:
        """Test that freshness uses FreshnessAssertionInfoClass."""
        df = pd.DataFrame(
            {
                "timestamp_ms": [1704067200000],
                "expected_next_event_millis": [1704080000000],
                "is_fresh": [True],
            }
        )
        entity_urn = "urn:li:dataset:test"

        result = FreshnessAssertions.from_df(df, entity_urn)

        assertion = result[0].assertion
        assert assertion is not None
        assert assertion.type == "FRESHNESS"
        assert assertion.freshnessAssertion is not None
        assert assertion.volumeAssertion is None

    def test_from_df_missing_timestamp_raises_error(self) -> None:
        """Test that missing timestamp_ms column raises ValueError."""
        df = pd.DataFrame(
            {
                "expected_next_event_millis": [1704080000000],
                "is_fresh": [True],
            }
        )
        entity_urn = "urn:li:dataset:test"

        with pytest.raises(ValueError) as exc_info:
            FreshnessAssertions.from_df(df, entity_urn)

        assert "timestamp_ms" in str(exc_info.value)

    def test_from_df_optional_columns_work(self) -> None:
        """Test that freshness works with only required columns."""
        df = pd.DataFrame(
            {
                "timestamp_ms": [1704067200000, 1704070800000],
            }
        )
        entity_urn = "urn:li:dataset:test"

        result = FreshnessAssertions.from_df(df, entity_urn)

        assert len(result) == 2
        assert result[0].context is None or result[0].context == {}

    def test_round_trip(self) -> None:
        """Test that conversion to assertions and back preserves data."""
        df = pd.DataFrame(
            {
                "timestamp_ms": [1704067200000, 1704070800000],
                "expected_next_event_millis": [1704080000000, 1704090000000],
                "is_fresh": [True, False],
            }
        )
        entity_urn = "urn:li:dataset:test"

        assertions = FreshnessAssertions.from_df(df, entity_urn)
        result_df = FreshnessAssertions.to_df(assertions)

        assert len(result_df) == 2
        assert "timestamp_ms" in result_df.columns
        assert "expected_next_event_millis" in result_df.columns
        assert "is_fresh" in result_df.columns

        assert result_df.iloc[0]["expected_next_event_millis"] == 1704080000000
        assert result_df.iloc[0]["is_fresh"] == True  # noqa: E712
        assert result_df.iloc[1]["is_fresh"] == False  # noqa: E712


# =============================================================================
# Context Builder Tests
# =============================================================================


class TestBuildEvaluationContext:
    """Tests for build_evaluation_context function."""

    def test_build_evaluation_context(
        self, sample_model_config: ModelConfig, sample_forecast_df: pd.DataFrame
    ) -> None:
        """Test building a complete evaluation context."""
        entity_urn = "urn:li:dataset:test"
        assertions = ForecastAssertions.from_df(sample_forecast_df, entity_urn)

        context = build_evaluation_context(
            model_config=sample_model_config,
            embedded_assertions=assertions,
            generated_at_millis=1707350400000,
        )

        assert isinstance(context, AssertionEvaluationContextClass)
        assert context.inferenceDetails is not None
        assert context.inferenceDetails.modelId == "observe-models"
        assert context.inferenceDetails.modelVersion == OBSERVE_MODELS_VERSION
        assert context.embeddedAssertions is not None
        assert len(context.embeddedAssertions) == 3

    def test_build_evaluation_context_uses_model_config_generated_at(
        self,
        sample_model_config: ModelConfig,
        sample_forecast_df: pd.DataFrame,
    ) -> None:
        """Test that generated_at falls back to model_config.generated_at."""
        entity_urn = "urn:li:dataset:test"
        assertions = ForecastAssertions.from_df(sample_forecast_df, entity_urn)

        context = build_evaluation_context(
            model_config=sample_model_config,
            embedded_assertions=assertions,
        )

        assert context.inferenceDetails is not None
        assert context.inferenceDetails.generatedAt == sample_model_config.generated_at
