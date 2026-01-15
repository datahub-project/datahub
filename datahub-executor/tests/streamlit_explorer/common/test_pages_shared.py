"""Tests for the pages/_shared.py module."""

from unittest.mock import MagicMock

from scripts.streamlit_explorer.common import _shorten_urn, get_model_hyperparameters


class TestShortenUrn:
    """Tests for the _shorten_urn helper function.

    The function simply truncates from the end with ellipsis if URN exceeds max_length.
    """

    def test_shorten_monitor_urn(self):
        """Test shortening a monitor URN truncates from end."""
        urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset2-v2,PROD),__system__volume)"
        result = _shorten_urn(urn)
        # Truncates from end, so starts with beginning of URN
        assert result.startswith("urn:li:monitor:")
        assert len(result) <= 60
        assert result.endswith("...")

    def test_shorten_monitor_urn_snowflake(self):
        """Test shortening a Snowflake monitor URN truncates from end."""
        urn = "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD),__system__volume)"
        result = _shorten_urn(urn)
        # Truncates from end, so starts with beginning of URN
        assert result.startswith("urn:li:monitor:")
        assert len(result) <= 60
        assert result.endswith("...")

    def test_shorten_assertion_urn(self):
        """Test shortening an assertion URN truncates from end."""
        urn = "urn:li:assertion:dXJuOmxpOmRhdGFzZXQ6KHVybjpsaTpkYXRhUGxhdGZvcm06a2Fma2EsU2FtcGxlQ3lwcmVzc0thZmthRGF0YXNldCxQUk9EKQ==-__system__volume"
        result = _shorten_urn(urn)
        assert result.startswith("urn:li:assertion:")
        assert len(result) <= 60
        assert result.endswith("...")

    def test_shorten_empty_urn(self):
        """Test shortening an empty URN."""
        assert _shorten_urn("") == ""
        assert _shorten_urn(None) == ""  # type: ignore[arg-type]

    def test_shorten_short_urn(self):
        """Test that short URNs are not truncated."""
        urn = "urn:li:assertion:abc123"
        result = _shorten_urn(urn)
        assert result == urn  # No truncation needed
        assert "..." not in result

    def test_shorten_unknown_urn_format(self):
        """Test fallback for unknown URN formats."""
        urn = "urn:li:unknown:some-long-identifier-that-needs-truncation-to-fit-in-display"
        result = _shorten_urn(urn, max_length=40)
        assert len(result) <= 40
        assert result.endswith("...")


class TestGetModelHyperparameters:
    """Tests for the get_model_hyperparameters function.

    This function extracts hyperparameters from trained models (observe-models)
    using a priority order: get_hyperparameters() > best_params > get_config_dict().
    """

    def test_none_model_returns_note(self):
        """Test that None model returns a dict with 'note' key."""
        result = get_model_hyperparameters(None)
        assert isinstance(result, dict)
        assert "note" in result
        assert result["note"] == "Model not available"

    def test_model_with_get_hyperparameters(self):
        """Test extraction using get_hyperparameters() method (preferred)."""
        model = MagicMock()
        model.get_hyperparameters.return_value = {
            "coverage_target": 0.95,
            "use_forecast_uncertainty": True,
            "min_band_factor": 1.0,
        }

        result = get_model_hyperparameters(model)

        assert result == {
            "coverage_target": 0.95,
            "use_forecast_uncertainty": True,
            "min_band_factor": 1.0,
        }
        model.get_hyperparameters.assert_called_once()

    def test_model_with_get_hyperparameters_returning_none_falls_back(self):
        """Test fallback when get_hyperparameters() returns None."""
        model = MagicMock()
        model.get_hyperparameters.return_value = None
        model.best_params = {"n_estimators": 100, "contamination": 0.01}

        result = get_model_hyperparameters(model)

        assert result == {"n_estimators": 100, "contamination": 0.01}

    def test_model_with_get_hyperparameters_empty_dict_falls_back(self):
        """Test fallback when get_hyperparameters() returns empty dict."""
        model = MagicMock()
        model.get_hyperparameters.return_value = {}
        model.best_params = {"learning_rate": 0.01}

        result = get_model_hyperparameters(model)

        assert result == {"learning_rate": 0.01}

    def test_model_with_get_hyperparameters_raising_exception_falls_back(self):
        """Test fallback when get_hyperparameters() raises an exception."""
        model = MagicMock()
        model.get_hyperparameters.side_effect = RuntimeError("Model not trained")
        model.best_params = {"depth": 5}

        result = get_model_hyperparameters(model)

        assert result == {"depth": 5}

    def test_model_with_best_params_from_grid_search(self):
        """Test extraction using best_params (grid search results)."""
        model = MagicMock(spec=["best_params"])  # No get_hyperparameters
        model.best_params = {
            "deviation_threshold": 2.0,
            "n_neighbors": 10,
        }

        result = get_model_hyperparameters(model)

        assert result == {"deviation_threshold": 2.0, "n_neighbors": 10}

    def test_model_with_empty_best_params_falls_back(self):
        """Test fallback when best_params is empty or falsy."""
        model = MagicMock(spec=["best_params", "get_config_dict"])
        model.best_params = {}
        model.get_config_dict.return_value = {"seasonality_mode": "multiplicative"}

        result = get_model_hyperparameters(model)

        assert result == {"seasonality_mode": "multiplicative"}
        model.get_config_dict.assert_called_once_with(include_training_params=False)

    def test_model_with_none_best_params_falls_back(self):
        """Test fallback when best_params is None."""
        model = MagicMock(spec=["best_params", "get_config_dict"])
        model.best_params = None
        model.get_config_dict.return_value = {"hidden_size": 64}

        result = get_model_hyperparameters(model)

        assert result == {"hidden_size": 64}

    def test_model_with_get_config_dict_legacy(self):
        """Test extraction using get_config_dict() (legacy fallback)."""
        model = MagicMock(
            spec=["get_config_dict"]
        )  # No get_hyperparameters or best_params
        model.get_config_dict.return_value = {
            "model_version": "0.1.0",
            "frequency": "D",
        }

        result = get_model_hyperparameters(model)

        assert result == {"model_version": "0.1.0", "frequency": "D"}
        model.get_config_dict.assert_called_once_with(include_training_params=False)

    def test_model_with_get_config_dict_returning_none(self):
        """Test that empty result is returned when get_config_dict returns None."""
        model = MagicMock(spec=["get_config_dict"])
        model.get_config_dict.return_value = None

        result = get_model_hyperparameters(model)

        assert "note" in result
        assert result["note"] == "No hyperparameters available"

    def test_model_with_get_config_dict_raising_exception(self):
        """Test handling when get_config_dict raises an exception."""
        model = MagicMock(spec=["get_config_dict"])
        model.get_config_dict.side_effect = ValueError("Config unavailable")

        result = get_model_hyperparameters(model)

        assert "note" in result
        assert result["note"] == "No hyperparameters available"

    def test_model_with_no_hyperparameter_methods(self):
        """Test model with no hyperparameter extraction methods."""
        model = MagicMock(spec=[])  # No relevant methods

        result = get_model_hyperparameters(model)

        assert isinstance(result, dict)
        assert "note" in result
        assert result["note"] == "No hyperparameters available"

    def test_returns_copy_of_hyperparameters(self):
        """Test that returned dict is a copy, not the original."""
        original = {"param1": "value1", "param2": 42}
        model = MagicMock()
        model.get_hyperparameters.return_value = original

        result = get_model_hyperparameters(model)

        # Modify result and verify original unchanged
        result["param1"] = "modified"
        assert original["param1"] == "value1"

    def test_priority_order_get_hyperparameters_over_best_params(self):
        """Test that get_hyperparameters() takes priority over best_params."""
        model = MagicMock()
        model.get_hyperparameters.return_value = {"from": "get_hyperparameters"}
        model.best_params = {"from": "best_params"}
        model.get_config_dict.return_value = {"from": "get_config_dict"}

        result = get_model_hyperparameters(model)

        assert result["from"] == "get_hyperparameters"

    def test_priority_order_best_params_over_get_config_dict(self):
        """Test that best_params takes priority over get_config_dict()."""
        model = MagicMock(spec=["best_params", "get_config_dict"])
        model.best_params = {"from": "best_params"}
        model.get_config_dict.return_value = {"from": "get_config_dict"}

        result = get_model_hyperparameters(model)

        assert result["from"] == "best_params"
        model.get_config_dict.assert_not_called()

    def test_handles_complex_hyperparameter_values(self):
        """Test handling of complex hyperparameter values (nested dicts, lists)."""
        model = MagicMock()
        model.get_hyperparameters.return_value = {
            "int_param": 100,
            "float_param": 0.01,
            "bool_param": True,
            "str_param": "auto",
            "list_param": [1, 2, 3],
            "dict_param": {"nested": "value"},
            "none_param": None,
        }

        result = get_model_hyperparameters(model)

        assert result["int_param"] == 100
        assert result["float_param"] == 0.01
        assert result["bool_param"] is True
        assert result["str_param"] == "auto"
        assert result["list_param"] == [1, 2, 3]
        assert result["dict_param"] == {"nested": "value"}
        assert result["none_param"] is None
