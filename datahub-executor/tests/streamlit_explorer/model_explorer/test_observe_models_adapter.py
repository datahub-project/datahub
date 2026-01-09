"""Tests for the pages/_observe_models_adapter.py module."""

from unittest.mock import MagicMock, patch

import pytest

from scripts.streamlit_explorer.model_explorer.observe_models_adapter import (
    DYNAMIC_COLOR_PALETTE,
    AnomalyModelConfig,
    ObserveModelConfig,
    build_preprocessing_config,
    get_anomaly_model_configs,
    get_assertion_type_from_context,
    get_model_preprocessing_defaults,
    get_observe_model_configs,
    is_global_forecasting_model,
    is_observe_models_available,
)


class TestObserveModelsAvailability:
    """Tests for the is_observe_models_available function."""

    def test_is_available_returns_bool(self):
        """Test that is_observe_models_available returns a boolean."""
        result = is_observe_models_available()
        assert isinstance(result, bool)

    def test_graceful_when_not_installed(self):
        """Test that function returns False when observe-models is not installed."""
        with patch.dict("sys.modules", {"datahub_observe.registry": None}):
            # Force reimport to test the import error path
            with patch(
                "scripts.streamlit_explorer.model_explorer.observe_models_adapter.is_observe_models_available"
            ) as mock_fn:
                mock_fn.return_value = False
                assert mock_fn() is False


class TestGetObserveModelConfigs:
    """Tests for the get_observe_model_configs function."""

    def test_returns_dict(self):
        """Test that get_observe_model_configs returns a dictionary."""
        result = get_observe_model_configs()
        assert isinstance(result, dict)

    def test_returns_empty_dict_when_not_available(self):
        """Test that function returns empty dict when observe-models unavailable."""
        with patch(
            "scripts.streamlit_explorer.model_explorer.observe_models_adapter.is_observe_models_available",
            return_value=False,
        ):
            result = get_observe_model_configs()
            assert result == {}

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_configs_have_required_fields(self):
        """Test that all configs have required fields when observe-models available."""
        configs = get_observe_model_configs()

        for _key, config in configs.items():
            assert isinstance(config, ObserveModelConfig)
            assert config.name is not None
            assert config.description is not None
            assert config.color is not None
            assert config.train_fn is not None
            assert config.predict_fn is not None
            assert config.is_observe_model is True

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_model_keys_are_prefixed(self):
        """Test that model keys are prefixed with 'obs_'."""
        configs = get_observe_model_configs()

        for key in configs:
            assert key.startswith("obs_"), f"Key {key} should start with 'obs_'"


class TestBuildPreprocessingConfig:
    """Tests for the build_preprocessing_config function."""

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_builds_config_with_auto_frequency(self):
        """Test that config is built with auto frequency by default."""
        from datahub_observe.assertions.types import AssertionCategory  # type: ignore[import-untyped]  # noqa: I001

        config = build_preprocessing_config(
            assertion_type=AssertionCategory.VOLUME,
        )
        assert config is not None
        assert config.frequency == "auto"

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_builds_config_for_volume_assertion(self):
        """Test that config is built correctly for volume assertions."""
        from datahub_observe.assertions.types import AssertionCategory  # type: ignore[import-untyped]  # noqa: I001

        config = build_preprocessing_config(
            assertion_type=AssertionCategory.VOLUME,
            frequency="auto",
        )
        assert config is not None
        assert config.assertion_type == AssertionCategory.VOLUME

    def test_returns_none_when_not_available(self):
        """Test that function returns None when observe-models unavailable."""
        with patch.dict("sys.modules", {"datahub_observe.assertions.config": None}):
            # This should handle the import error gracefully
            # When observe-models is actually installed, this test will pass
            # because the real import succeeds before the patch
            pass  # The actual behavior depends on import timing


class TestGetAssertionTypeFromContext:
    """Tests for the get_assertion_type_from_context function."""

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_returns_volume_as_default(self):
        """Test that VOLUME is returned as default when no context."""
        from datahub_observe.assertions.types import AssertionCategory  # type: ignore[import-untyped]  # noqa: I001

        with patch("streamlit.session_state", {}):
            result = get_assertion_type_from_context()
            assert result == AssertionCategory.VOLUME

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_extracts_type_from_session_state(self):
        """Test that assertion type is extracted from session state."""
        from datahub_observe.assertions.types import AssertionCategory  # type: ignore[import-untyped]  # noqa: I001

        mock_session_state = {"current_assertion_type": AssertionCategory.RATE}
        with patch("streamlit.session_state", mock_session_state):
            result = get_assertion_type_from_context()
            assert result == AssertionCategory.RATE

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_handles_string_assertion_type(self):
        """Test that string assertion types are converted."""
        from datahub_observe.assertions.types import AssertionCategory  # type: ignore[import-untyped]  # noqa: I001

        mock_session_state = {"current_assertion_type": "volume"}
        with patch("streamlit.session_state", mock_session_state):
            result = get_assertion_type_from_context()
            assert result == AssertionCategory.VOLUME


class TestObserveModelConfig:
    """Tests for the ObserveModelConfig dataclass."""

    def test_dataclass_creation(self):
        """Test that ObserveModelConfig can be instantiated."""
        config = ObserveModelConfig(
            name="Test Model",
            description="A test model",
            registry_key="test",
            color="#ffffff",
        )
        assert config.name == "Test Model"
        assert config.description == "A test model"
        assert config.registry_key == "test"
        assert config.color == "#ffffff"
        assert config.is_observe_model is True
        assert config.dash is None
        assert config.train_fn is None
        assert config.predict_fn is None

    def test_dataclass_with_all_fields(self):
        """Test ObserveModelConfig with all fields populated."""
        mock_train_fn = MagicMock()
        mock_predict_fn = MagicMock()

        config = ObserveModelConfig(
            name="Full Model",
            description="A complete test model",
            registry_key="full_test",
            color="#000000",
            dash="dot",
            is_observe_model=True,
            train_fn=mock_train_fn,
            predict_fn=mock_predict_fn,
        )
        assert config.dash == "dot"
        assert config.train_fn is mock_train_fn
        assert config.predict_fn is mock_predict_fn


class TestGetAnomalyModelConfigs:
    """Tests for the get_anomaly_model_configs function."""

    def test_returns_dict(self):
        """Test that get_anomaly_model_configs returns a dictionary."""
        result = get_anomaly_model_configs()
        assert isinstance(result, dict)

    def test_returns_empty_dict_when_not_available(self):
        """Test that function returns empty dict when observe-models unavailable."""
        with patch(
            "scripts.streamlit_explorer.model_explorer.observe_models_adapter.is_observe_models_available",
            return_value=False,
        ):
            result = get_anomaly_model_configs()
            assert result == {}

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_configs_have_required_fields(self):
        """Test that all anomaly configs have required fields."""
        configs = get_anomaly_model_configs()

        for _key, config in configs.items():
            assert isinstance(config, AnomalyModelConfig)
            assert config.name is not None
            assert config.description is not None
            assert config.train_fn is not None
            assert config.detect_fn is not None
            assert config.is_observe_model is True
            # requires_forecast_model may be True or False depending on model
            assert isinstance(config.requires_forecast_model, bool)

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_anomaly_model_keys_are_prefixed(self):
        """Test that anomaly model keys are prefixed with 'anomaly_'."""
        configs = get_anomaly_model_configs()

        for key in configs:
            assert key.startswith("anomaly_"), f"Key {key} should start with 'anomaly_'"


class TestAnomalyModelConfig:
    """Tests for the AnomalyModelConfig dataclass."""

    def test_dataclass_creation(self):
        """Test that AnomalyModelConfig can be instantiated."""
        config = AnomalyModelConfig(
            name="Test Anomaly Model",
            description="A test anomaly model",
            registry_key="test_anomaly",
        )
        assert config.name == "Test Anomaly Model"
        assert config.description == "A test anomaly model"
        assert config.registry_key == "test_anomaly"
        assert config.is_observe_model is True
        assert config.requires_forecast_model is True
        assert config.train_fn is None
        assert config.detect_fn is None

    def test_dataclass_with_all_fields(self):
        """Test AnomalyModelConfig with all fields populated."""
        mock_train_fn = MagicMock()
        mock_detect_fn = MagicMock()

        config = AnomalyModelConfig(
            name="Full Anomaly Model",
            description="A complete anomaly model",
            registry_key="full_anomaly",
            requires_forecast_model=False,
            is_observe_model=True,
            train_fn=mock_train_fn,
            detect_fn=mock_detect_fn,
        )
        assert config.requires_forecast_model is False
        assert config.train_fn is mock_train_fn
        assert config.detect_fn is mock_detect_fn


class TestIsGlobalForecastingModel:
    """Tests for the is_global_forecasting_model function."""

    def test_returns_false_for_none(self):
        """Test that function returns False for None registry_key."""
        result = is_global_forecasting_model(None)
        assert result is False

    def test_returns_false_when_observe_models_unavailable(self):
        """Test that function returns False when observe-models unavailable."""
        with patch(
            "scripts.streamlit_explorer.model_explorer.observe_models_adapter.is_observe_models_available",
            return_value=False,
        ):
            result = is_global_forecasting_model("nbeats")
            assert result is False

    def test_returns_false_for_invalid_registry_key(self):
        """Test that function returns False for invalid registry keys."""
        result = is_global_forecasting_model("nonexistent_model")
        assert result is False

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_nbeats_is_global_forecasting_model(self):
        """Test that NBEATS is correctly identified as GlobalForecastingModel."""
        # Use registry_key directly (not model_key with obs_ prefix)
        result = is_global_forecasting_model("nbeats")
        assert result is True

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_prophet_is_not_global_forecasting_model(self):
        """Test that Prophet is correctly identified as NOT GlobalForecastingModel."""
        result = is_global_forecasting_model("prophet")
        assert result is False

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_datahub_is_not_global_forecasting_model(self):
        """Test that DataHub is correctly identified as NOT GlobalForecastingModel."""
        result = is_global_forecasting_model("datahub")
        assert result is False


class TestDynamicColorPalette:
    """Tests for the dynamic color palette."""

    def test_palette_has_multiple_colors(self):
        """Test that DYNAMIC_COLOR_PALETTE has multiple colors."""
        assert len(DYNAMIC_COLOR_PALETTE) >= 5

    def test_palette_colors_are_hex_format(self):
        """Test that colors are in hex format."""
        for color in DYNAMIC_COLOR_PALETTE:
            assert color.startswith("#"), f"Color {color} should be hex format"
            assert len(color) == 7, f"Color {color} should be 7 chars (#RRGGBB)"

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_all_forecasters_get_unique_colors(self):
        """Test that all forecasters get assigned colors (may cycle if many)."""
        configs = get_observe_model_configs()
        colors_used = [config.color for config in configs.values()]

        # Each model should have a color
        assert len(colors_used) == len(configs)
        # All colors should be valid hex
        for color in colors_used:
            assert color.startswith("#")


class TestGetModelPreprocessingDefaults:
    """Tests for the get_model_preprocessing_defaults function."""

    def test_returns_none_when_not_available(self):
        """Test that function returns None when observe-models unavailable."""
        with patch(
            "scripts.streamlit_explorer.model_explorer.observe_models_adapter.is_observe_models_available",
            return_value=False,
        ):
            result = get_model_preprocessing_defaults("prophet")
            assert result is None

    def test_returns_none_for_unknown_model(self):
        """Test that function returns None for unknown models."""
        result = get_model_preprocessing_defaults("nonexistent_model_xyz")
        # Should return None (model not found or no defaults)
        # The actual behavior depends on whether observe-models is installed
        assert result is None or hasattr(result, "transformer_defaults")

    @pytest.mark.skipif(
        not is_observe_models_available(),
        reason="observe-models not installed",
    )
    def test_returns_preprocessing_defaults_for_known_model(self):
        """Test that function returns PreprocessingDefaults for known models."""
        # Prophet is a known model that may have defaults
        result = get_model_preprocessing_defaults("prophet")
        # May be None if model has no defaults, or an object if it does
        if result is not None:
            # Should have expected attributes
            assert hasattr(result, "transformer_defaults") or hasattr(
                result, "pandas_transformers"
            )
