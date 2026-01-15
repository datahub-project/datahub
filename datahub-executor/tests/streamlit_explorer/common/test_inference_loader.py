"""Tests for the inference_loader module."""

from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd

from scripts.streamlit_explorer.common.inference_loader import (
    InferenceData,
    _execute_graphql,
    _graphql_to_inference_details,
    _parse_embedded_assertion,
    _parse_embedded_assertions_to_df,
    _parse_parameters_map,
    fetch_assertion_evaluation_context,
    fetch_inference_data,
    fetch_monitor_evaluation_context,
    get_training_metrics_summary,
    parse_evaluation_context,
)


class TestInferenceData:
    """Tests for InferenceData container."""

    def test_empty_inference_data(self):
        """Test default empty InferenceData."""
        data = InferenceData()

        assert data.model_config is None
        assert data.preprocessing_config is None
        assert data.forecast_config is None
        assert data.anomaly_config is None
        assert data.forecast_evals is None
        assert data.anomaly_evals is None
        assert data.predictions_df is None
        assert data.entity_urn is None
        assert data.generated_at is None

    def test_has_inference_data_false_when_empty(self):
        """Test has_inference_data returns False when empty."""
        data = InferenceData()
        assert data.has_inference_data is False

    def test_has_inference_data_true_with_model_config(self):
        """Test has_inference_data returns True when model_config exists."""
        mock_config = MagicMock()
        data = InferenceData(model_config=mock_config)
        assert data.has_inference_data is True

    def test_has_predictions_false_when_empty(self):
        """Test has_predictions returns False when no predictions."""
        data = InferenceData()
        assert data.has_predictions is False

    def test_has_predictions_false_with_empty_df(self):
        """Test has_predictions returns False with empty DataFrame."""
        data = InferenceData(predictions_df=pd.DataFrame())
        assert data.has_predictions is False

    def test_has_predictions_true_with_data(self):
        """Test has_predictions returns True with valid DataFrame."""
        df = pd.DataFrame({"timestamp_ms": [1000, 2000], "yhat": [1.0, 2.0]})
        data = InferenceData(predictions_df=df)
        assert data.has_predictions is True

    def test_forecast_model_info_none_without_config(self):
        """Test forecast_model_info returns None without config."""
        data = InferenceData()
        assert data.forecast_model_info is None

    def test_forecast_model_info_with_model(self):
        """Test forecast_model_info extracts model info."""
        mock_config = MagicMock()
        mock_config.forecast_model_name = "prophet"
        mock_config.forecast_model_version = "1.0.0"

        data = InferenceData(model_config=mock_config)
        info = data.forecast_model_info

        assert info is not None
        assert info["name"] == "prophet"
        assert info["version"] == "1.0.0"

    def test_anomaly_model_info_none_without_config(self):
        """Test anomaly_model_info returns None without config."""
        data = InferenceData()
        assert data.anomaly_model_info is None

    def test_anomaly_model_info_with_model(self):
        """Test anomaly_model_info extracts model info."""
        mock_config = MagicMock()
        mock_config.anomaly_model_name = "isolation_forest"
        mock_config.anomaly_model_version = "2.0.0"

        data = InferenceData(model_config=mock_config)
        info = data.anomaly_model_info

        assert info is not None
        assert info["name"] == "isolation_forest"
        assert info["version"] == "2.0.0"

    def test_to_dict(self):
        """Test to_dict serialization."""
        df = pd.DataFrame({"timestamp_ms": [1000, 2000]})
        data = InferenceData(
            entity_urn="urn:li:assertion:test",
            generated_at=1234567890,
            predictions_df=df,
        )

        result = data.to_dict()

        assert result["entity_urn"] == "urn:li:assertion:test"
        assert result["generated_at"] == 1234567890
        assert result["has_predictions"] is True
        assert result["prediction_count"] == 2


class TestParseParametersMap:
    """Tests for _parse_parameters_map function."""

    def test_empty_list(self):
        """Test empty parameters list."""
        result = _parse_parameters_map([])
        assert result == {}

    def test_single_parameter(self):
        """Test single parameter."""
        params = [{"key": "foo", "value": "bar"}]
        result = _parse_parameters_map(params)
        assert result == {"foo": "bar"}

    def test_multiple_parameters(self):
        """Test multiple parameters."""
        params = [
            {"key": "a", "value": "1"},
            {"key": "b", "value": "2"},
            {"key": "c", "value": "3"},
        ]
        result = _parse_parameters_map(params)
        assert result == {"a": "1", "b": "2", "c": "3"}

    def test_missing_key(self):
        """Test parameter with missing key is skipped."""
        params = [{"value": "bar"}, {"key": "foo", "value": "baz"}]
        result = _parse_parameters_map(params)
        assert result == {"foo": "baz"}

    def test_null_value_included(self):
        """Test parameter with None value is skipped."""
        params: list[dict[str, str]] = [
            {"key": "foo", "value": None},  # type: ignore[dict-item]
            {"key": "bar", "value": "baz"},
        ]
        result = _parse_parameters_map(params)
        assert result == {"bar": "baz"}

    def test_empty_string_value_included(self):
        """Test parameter with empty string value is included."""
        params = [{"key": "foo", "value": ""}]
        result = _parse_parameters_map(params)
        assert result == {"foo": ""}


class TestGraphqlToInferenceDetails:
    """Tests for _graphql_to_inference_details function."""

    def test_none_input(self):
        """Test None input returns None."""
        result = _graphql_to_inference_details(None)  # type: ignore[arg-type]
        assert result is None

    def test_empty_dict(self):
        """Test empty dict returns None (treated as no data)."""
        result = _graphql_to_inference_details({})
        # Empty dict is falsy, so function returns None
        assert result is None

    def test_full_graphql_data(self):
        """Test full GraphQL data is converted correctly."""
        graphql_data = {
            "modelId": "test-model",
            "modelVersion": "1.0",
            "confidence": 0.95,
            "generatedAt": 1234567890,
            "parameters": [
                {"key": "preprocessing_config_json", "value": '{"test": true}'},
                {"key": "forecast_config_json", "value": '{"model": "prophet"}'},
            ],
        }

        result = _graphql_to_inference_details(graphql_data)

        assert result is not None
        assert result.modelId == "test-model"
        assert result.modelVersion == "1.0"
        assert result.confidence == 0.95
        assert result.generatedAt == 1234567890
        assert result.parameters is not None
        assert result.parameters.get("preprocessing_config_json") == '{"test": true}'
        assert result.parameters.get("forecast_config_json") == '{"model": "prophet"}'


class TestParseEmbeddedAssertionsToDf:
    """Tests for _parse_embedded_assertions_to_df function."""

    def test_empty_list(self):
        """Test empty list returns empty DataFrame."""
        result = _parse_embedded_assertions_to_df([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_forecast_assertions(self):
        """Test parsing forecast-style assertions."""
        assertions = [
            {
                "evaluationTimeWindow": {"startTimeMillis": 1000},
                "context": [
                    {"key": "y", "value": "100.0"},
                    {"key": "yhat", "value": "105.0"},
                    {"key": "yhatLower", "value": "95.0"},
                    {"key": "yhatUpper", "value": "115.0"},
                ],
            },
            {
                "evaluationTimeWindow": {"startTimeMillis": 2000},
                "context": [
                    {"key": "y", "value": "110.0"},
                    {"key": "yhat", "value": "108.0"},
                    {"key": "yhatLower", "value": "98.0"},
                    {"key": "yhatUpper", "value": "118.0"},
                ],
            },
        ]

        result = _parse_embedded_assertions_to_df(assertions)

        assert len(result) == 2
        assert "timestamp_ms" in result.columns
        assert "y" in result.columns
        assert "yhat" in result.columns
        assert "yhat_lower" in result.columns  # Renamed from yhatLower
        assert "yhat_upper" in result.columns  # Renamed from yhatUpper

        assert result["timestamp_ms"].iloc[0] == 1000
        assert result["y"].iloc[0] == 100.0
        assert result["yhat"].iloc[0] == 105.0

    def test_anomaly_assertions(self):
        """Test parsing anomaly-style assertions."""
        assertions = [
            {
                "evaluationTimeWindow": {"startTimeMillis": 1000},
                "context": [
                    {"key": "anomalyScore", "value": "0.95"},
                    {"key": "isAnomaly", "value": "true"},
                    {"key": "detectionBandLower", "value": "90.0"},
                    {"key": "detectionBandUpper", "value": "110.0"},
                ],
            },
        ]

        result = _parse_embedded_assertions_to_df(assertions)

        assert len(result) == 1
        assert "anomaly_score" in result.columns  # Renamed from anomalyScore
        assert "is_anomaly" in result.columns  # Renamed from isAnomaly
        assert "detection_band_lower" in result.columns
        assert "detection_band_upper" in result.columns

        assert result["anomaly_score"].iloc[0] == 0.95
        assert result["is_anomaly"].iloc[0] == True  # noqa: E712 - Use == for numpy bool

    def test_assertion_without_timestamp_skipped(self):
        """Test assertions without timestamp are skipped."""
        assertions: list[dict[str, Any]] = [
            {
                "evaluationTimeWindow": {},  # No timestamp
                "context": [{"key": "y", "value": "100"}],
            },
            {
                "evaluationTimeWindow": {"startTimeMillis": 1000},
                "context": [{"key": "y", "value": "200"}],
            },
        ]

        result = _parse_embedded_assertions_to_df(assertions)

        assert len(result) == 1
        assert result["y"].iloc[0] == 200.0


class TestParseEvaluationContext:
    """Tests for parse_evaluation_context function."""

    def test_none_input(self):
        """Test None input returns empty InferenceData."""
        result = parse_evaluation_context(None)  # type: ignore[arg-type]

        assert isinstance(result, InferenceData)
        assert result.has_inference_data is False

    def test_empty_dict(self):
        """Test empty dict returns empty InferenceData."""
        result = parse_evaluation_context({})

        assert isinstance(result, InferenceData)
        assert result.has_inference_data is False

    def test_entity_urn_passed_through(self):
        """Test entity_urn is preserved in result."""
        result = parse_evaluation_context({}, entity_urn="urn:li:assertion:test")

        assert result.entity_urn == "urn:li:assertion:test"

    def test_with_embedded_assertions(self):
        """Test parsing with embedded assertions."""
        graphql_data = {
            "embeddedAssertions": [
                {
                    "evaluationTimeWindow": {"startTimeMillis": 1000},
                    "context": [
                        {"key": "y", "value": "100"},
                        {"key": "yhat", "value": "105"},
                    ],
                },
            ],
        }

        result = parse_evaluation_context(graphql_data)

        assert result.has_predictions is True
        assert result.predictions_df is not None
        assert len(result.predictions_df) == 1


class TestGetTrainingMetricsSummary:
    """Tests for get_training_metrics_summary function."""

    def test_empty_inference_data(self):
        """Test summary for empty inference data."""
        data = InferenceData()
        summary = get_training_metrics_summary(data)

        assert summary["has_forecast"] is False
        assert summary["has_anomaly"] is False
        assert summary["forecast_metrics"] == {}
        assert summary["anomaly_metrics"] == {}

    def test_with_forecast_evals_object(self):
        """Test summary with forecast evals as object."""
        mock_evals = MagicMock()
        mock_evals.aggregated = MagicMock()
        mock_evals.aggregated.mae = 0.1
        mock_evals.aggregated.rmse = 0.2
        mock_evals.aggregated.mape = 0.05
        mock_evals.aggregated.coverage = 0.95

        data = InferenceData(forecast_evals=mock_evals)
        summary = get_training_metrics_summary(data)

        assert summary["has_forecast"] is True
        assert summary["forecast_metrics"]["mae"] == 0.1
        assert summary["forecast_metrics"]["rmse"] == 0.2
        assert summary["forecast_metrics"]["mape"] == 0.05
        assert summary["forecast_metrics"]["coverage"] == 0.95

    def test_with_forecast_evals_dict(self):
        """Test summary with forecast evals as dict."""
        evals_dict = {
            "aggregated": {
                "mae": 0.15,
                "rmse": 0.25,
                "mape": 0.08,
                "coverage": 0.92,
            }
        }

        data = InferenceData(forecast_evals=evals_dict)
        summary = get_training_metrics_summary(data)

        assert summary["has_forecast"] is True
        assert summary["forecast_metrics"]["mae"] == 0.15
        assert summary["forecast_metrics"]["rmse"] == 0.25

    def test_with_anomaly_evals_object(self):
        """Test summary with anomaly evals as object."""
        mock_evals = MagicMock()
        mock_evals.aggregated = MagicMock()
        mock_evals.aggregated.precision = 0.9
        mock_evals.aggregated.recall = 0.85
        mock_evals.aggregated.f1_score = 0.875
        mock_evals.aggregated.accuracy = 0.92

        data = InferenceData(anomaly_evals=mock_evals)
        summary = get_training_metrics_summary(data)

        assert summary["has_anomaly"] is True
        assert summary["anomaly_metrics"]["precision"] == 0.9
        assert summary["anomaly_metrics"]["recall"] == 0.85
        assert summary["anomaly_metrics"]["f1_score"] == 0.875

    def test_with_anomaly_evals_dict(self):
        """Test summary with anomaly evals as dict."""
        evals_dict = {
            "aggregated": {
                "precision": 0.88,
                "recall": 0.82,
                "f1_score": 0.85,
                "accuracy": 0.9,
            }
        }

        data = InferenceData(anomaly_evals=evals_dict)
        summary = get_training_metrics_summary(data)

        assert summary["has_anomaly"] is True
        assert summary["anomaly_metrics"]["precision"] == 0.88
        assert summary["anomaly_metrics"]["recall"] == 0.82


class TestParseEmbeddedAssertion:
    """Tests for _parse_embedded_assertion function."""

    def test_none_input(self):
        """Test None input returns None."""
        result = _parse_embedded_assertion(None)  # type: ignore[arg-type]
        assert result is None

    def test_empty_dict(self):
        """Test empty dict returns None (treated as no data)."""
        result = _parse_embedded_assertion({})
        # Empty dict is falsy, so function returns None
        assert result is None

    def test_with_timestamp_and_context_list(self):
        """Test parsing with timestamp and context as list."""
        assertion_data = {
            "evaluationTimeWindow": {"startTimeMillis": 1234567890},
            "context": [
                {"key": "y", "value": "100"},
                {"key": "yhat", "value": "105"},
            ],
        }

        result = _parse_embedded_assertion(assertion_data)

        assert result is not None
        assert result["timestamp_ms"] == 1234567890
        assert result["context"]["y"] == "100"
        assert result["context"]["yhat"] == "105"

    def test_with_context_as_dict(self):
        """Test parsing when context is already a dict."""
        assertion_data = {
            "evaluationTimeWindow": {"startTimeMillis": 1000},
            "context": {"key1": "value1", "key2": "value2"},
        }

        result = _parse_embedded_assertion(assertion_data)

        assert result is not None
        assert result["context"]["key1"] == "value1"

    def test_with_invalid_context(self):
        """Test parsing with invalid context type."""
        assertion_data = {
            "evaluationTimeWindow": {"startTimeMillis": 1000},
            "context": "invalid",  # Should become empty dict
        }

        result = _parse_embedded_assertion(assertion_data)

        assert result is not None
        assert result["context"] == {}


class TestExecuteGraphql:
    """Tests for _execute_graphql function."""

    @patch("scripts.streamlit_explorer.common.inference_loader.requests.post")
    def test_successful_request(self, mock_post):
        """Test successful GraphQL request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {"assertion": {"urn": "test"}}}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        result = _execute_graphql(
            "http://test/graphql",
            {"Authorization": "Bearer token"},
            "query { test }",
            {"var": "value"},
        )

        assert result == {"assertion": {"urn": "test"}}
        mock_post.assert_called_once()

    @patch("scripts.streamlit_explorer.common.inference_loader.requests.post")
    def test_graphql_errors_returns_none(self, mock_post):
        """Test GraphQL errors in response returns None."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "errors": [{"message": "Some error"}],
            "data": None,
        }
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        result = _execute_graphql(
            "http://test/graphql",
            {},
            "query { test }",
            {},
        )

        assert result is None

    @patch("scripts.streamlit_explorer.common.inference_loader.requests.post")
    def test_request_exception_returns_none(self, mock_post):
        """Test request exception returns None."""
        import requests

        mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")

        result = _execute_graphql(
            "http://test/graphql",
            {},
            "query { test }",
            {},
        )

        assert result is None


class TestFetchAssertionEvaluationContext:
    """Tests for fetch_assertion_evaluation_context function."""

    @patch("scripts.streamlit_explorer.common.inference_loader._execute_graphql")
    def test_returns_evaluation_context(self, mock_execute):
        """Test successful fetch returns evaluation context."""
        mock_execute.return_value = {
            "assertion": {
                "evaluationContext": {"inferenceDetails": {"modelId": "test"}}
            }
        }

        result = fetch_assertion_evaluation_context(
            "http://test/graphql",
            {},
            "urn:li:assertion:test",
        )

        assert result == {"inferenceDetails": {"modelId": "test"}}

    @patch("scripts.streamlit_explorer.common.inference_loader._execute_graphql")
    def test_returns_none_when_no_data(self, mock_execute):
        """Test returns None when no data."""
        mock_execute.return_value = None

        result = fetch_assertion_evaluation_context(
            "http://test/graphql",
            {},
            "urn:li:assertion:test",
        )

        assert result is None

    @patch("scripts.streamlit_explorer.common.inference_loader._execute_graphql")
    def test_returns_none_when_no_assertion(self, mock_execute):
        """Test returns None when assertion not in response."""
        mock_execute.return_value = {"assertion": None}

        result = fetch_assertion_evaluation_context(
            "http://test/graphql",
            {},
            "urn:li:assertion:test",
        )

        assert result is None


class TestFetchMonitorEvaluationContext:
    """Tests for fetch_monitor_evaluation_context function."""

    @patch("scripts.streamlit_explorer.common.inference_loader._execute_graphql")
    def test_returns_first_assertion_context(self, mock_execute):
        """Test returns first assertion's evaluation context."""
        mock_execute.return_value = {
            "monitor": {
                "info": {
                    "assertionMonitor": {
                        "assertions": [
                            {
                                "assertion": {
                                    "evaluationContext": {
                                        "inferenceDetails": {"modelId": "test"}
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }

        result = fetch_monitor_evaluation_context(
            "http://test/graphql",
            {},
            "urn:li:monitor:test",
        )

        assert result == {"inferenceDetails": {"modelId": "test"}}

    @patch("scripts.streamlit_explorer.common.inference_loader._execute_graphql")
    def test_returns_none_when_no_assertions(self, mock_execute):
        """Test returns None when no assertions."""
        mock_execute.return_value = {
            "monitor": {"info": {"assertionMonitor": {"assertions": []}}}
        }

        result = fetch_monitor_evaluation_context(
            "http://test/graphql",
            {},
            "urn:li:monitor:test",
        )

        assert result is None


class TestFetchInferenceData:
    """Tests for fetch_inference_data high-level function."""

    @patch(
        "scripts.streamlit_explorer.common.inference_loader.fetch_assertion_evaluation_context"
    )
    def test_fetches_with_assertion_urn(self, mock_fetch):
        """Test fetches using assertion URN."""
        mock_fetch.return_value = {
            "embeddedAssertions": [
                {
                    "evaluationTimeWindow": {"startTimeMillis": 1000},
                    "context": [{"key": "y", "value": "100"}],
                }
            ]
        }

        result = fetch_inference_data(
            "http://test/graphql",
            {},
            assertion_urn="urn:li:assertion:test",
        )

        assert result.entity_urn == "urn:li:assertion:test"
        mock_fetch.assert_called_once()

    @patch(
        "scripts.streamlit_explorer.common.inference_loader.fetch_monitor_evaluation_context"
    )
    def test_fetches_with_monitor_urn(self, mock_fetch):
        """Test fetches using monitor URN."""
        mock_fetch.return_value = None

        result = fetch_inference_data(
            "http://test/graphql",
            {},
            monitor_urn="urn:li:monitor:test",
        )

        assert result.entity_urn == "urn:li:monitor:test"
        mock_fetch.assert_called_once()

    def test_returns_empty_without_urn(self):
        """Test returns empty InferenceData without URN."""
        result = fetch_inference_data(
            "http://test/graphql",
            {},
        )

        assert result.entity_urn is None
        assert result.has_inference_data is False


class TestInferenceDataRoundTrip:
    """Integration tests for inference data processing."""

    def test_full_evaluation_context_parsing(self):
        """Test full GraphQL-like data parsing."""
        graphql_data = {
            "inferenceDetails": {
                "modelId": "forecast-model",
                "modelVersion": "2.0",
                "confidence": 0.88,
                "generatedAt": 1700000000000,
                "parameters": [
                    {
                        "key": "preprocessing_config_json",
                        "value": '{"normalize": true}',
                    },
                    {"key": "forecast_model_name", "value": "prophet"},
                    {"key": "forecast_model_version", "value": "1.0"},
                ],
            },
            "embeddedAssertions": [
                {
                    "evaluationTimeWindow": {"startTimeMillis": 1000},
                    "context": [
                        {"key": "y", "value": "100"},
                        {"key": "yhat", "value": "102"},
                        {"key": "yhatLower", "value": "95"},
                        {"key": "yhatUpper", "value": "109"},
                    ],
                },
                {
                    "evaluationTimeWindow": {"startTimeMillis": 2000},
                    "context": [
                        {"key": "y", "value": "105"},
                        {"key": "yhat", "value": "103"},
                        {"key": "yhatLower", "value": "96"},
                        {"key": "yhatUpper", "value": "110"},
                    ],
                },
            ],
        }

        result = parse_evaluation_context(
            graphql_data, entity_urn="urn:li:assertion:test123"
        )

        assert result.entity_urn == "urn:li:assertion:test123"
        assert result.has_predictions is True
        assert result.predictions_df is not None
        assert len(result.predictions_df) == 2

        # Check prediction values
        df = result.predictions_df
        assert df is not None
        assert df["timestamp_ms"].tolist() == [1000, 2000]
        assert df["y"].tolist() == [100.0, 105.0]
        assert df["yhat"].tolist() == [102.0, 103.0]
