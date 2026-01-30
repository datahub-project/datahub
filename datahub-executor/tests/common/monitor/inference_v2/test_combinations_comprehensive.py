"""Comprehensive tests for combinations module."""

from unittest.mock import patch

import pytest
from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException
from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
    DEFAULT_MODEL_PAIRINGS,
    ModelPairing,
    get_all_pairings,
    get_default_pairings,
    get_pairing_by_name,
    get_pairings_from_env_or_default,
    parse_pairing_identifier,
)


class TestModelPairing:
    """Tests for ModelPairing dataclass."""

    def test_model_pairing_with_versions(self) -> None:
        """ModelPairing generates correct keys with versions."""
        pairing = ModelPairing(
            anomaly_model="test_anomaly",
            anomaly_version="0.1.0",
            forecast_model="test_forecast",
            forecast_version="0.2.0",
        )

        assert pairing.anomaly_model_key == "test_anomaly@0.1.0"
        assert pairing.forecast_model_key == "test_forecast@0.2.0"
        assert pairing.name == "test_forecast_test_anomaly"

    def test_model_pairing_without_versions(self) -> None:
        """ModelPairing generates correct keys without versions."""
        pairing = ModelPairing(
            anomaly_model="test_anomaly",
            forecast_model="test_forecast",
        )

        assert pairing.anomaly_model_key == "test_anomaly"
        assert pairing.forecast_model_key == "test_forecast"
        assert pairing.name == "test_forecast_test_anomaly"

    def test_model_pairing_direct_anomaly(self) -> None:
        """ModelPairing handles direct anomaly models without forecast."""
        pairing = ModelPairing(anomaly_model="deepsvdd")

        assert pairing.forecast_model_key is None
        assert pairing.anomaly_model_key == "deepsvdd"
        assert pairing.name == "deepsvdd"
        assert pairing.requires_forecast is False

    def test_model_pairing_requires_forecast(self) -> None:
        """ModelPairing correctly identifies when forecast is required."""
        pairing_with_forecast = ModelPairing(
            anomaly_model="test_anomaly", forecast_model="test_forecast"
        )
        pairing_without_forecast = ModelPairing(anomaly_model="deepsvdd")

        assert pairing_with_forecast.requires_forecast is True
        assert pairing_without_forecast.requires_forecast is False

    def test_model_pairing_str_representation(self) -> None:
        """ModelPairing generates correct string representation."""
        pairing = ModelPairing(
            anomaly_model="test_anomaly",
            anomaly_version="0.1.0",
            forecast_model="test_forecast",
            forecast_version="0.2.0",
        )

        str_repr = str(pairing)
        assert "test_forecast@0.2.0" in str_repr
        assert "test_anomaly@0.1.0" in str_repr
        assert "+" in str_repr

    def test_model_pairing_str_representation_no_forecast(self) -> None:
        """ModelPairing generates correct string representation without forecast."""
        pairing = ModelPairing(anomaly_model="deepsvdd", anomaly_version="0.1.0")

        str_repr = str(pairing)
        assert "deepsvdd@0.1.0" in str_repr
        assert "+" not in str_repr


class TestGetDefaultPairings:
    """Tests for get_default_pairings function."""

    def test_get_default_pairings_returns_list(self) -> None:
        """get_default_pairings returns a list of ModelPairing objects."""
        pairings = get_default_pairings()

        assert isinstance(pairings, list)
        assert len(pairings) > 0
        assert all(isinstance(p, ModelPairing) for p in pairings)

    def test_get_default_pairings_returns_copy(self) -> None:
        """get_default_pairings returns a copy, not the original."""
        pairings1 = get_default_pairings()
        pairings2 = get_default_pairings()

        assert pairings1 is not pairings2
        assert pairings1 == pairings2


class TestGetAllPairings:
    """Tests for get_all_pairings function."""

    def test_get_all_pairings_returns_list(self) -> None:
        """get_all_pairings returns a list of ModelPairing objects."""
        pairings = get_all_pairings()

        assert isinstance(pairings, list)
        assert len(pairings) > 0
        assert all(isinstance(p, ModelPairing) for p in pairings)


class TestGetPairingByName:
    """Tests for get_pairing_by_name function."""

    def test_get_pairing_by_name_found(self) -> None:
        """get_pairing_by_name returns pairing when found."""
        # Use a known pairing from defaults
        known_pairing = DEFAULT_MODEL_PAIRINGS[0]
        pairing = get_pairing_by_name(known_pairing.name)

        assert pairing is not None
        assert pairing.name == known_pairing.name

    def test_get_pairing_by_name_not_found(self) -> None:
        """get_pairing_by_name returns None when not found."""
        pairing = get_pairing_by_name("nonexistent_pairing")

        assert pairing is None


class TestParsePairingIdentifier:
    """Tests for parse_pairing_identifier function."""

    def test_parse_pairing_identifier_simple(self) -> None:
        """parse_pairing_identifier parses simple forecast_anomaly format."""
        pairing = parse_pairing_identifier("prophet_adaptive_band")

        assert pairing.forecast_model == "prophet"
        assert pairing.anomaly_model == "adaptive_band"
        assert pairing.forecast_version is None
        assert pairing.anomaly_version is None

    def test_parse_pairing_identifier_with_versions(self) -> None:
        """parse_pairing_identifier parses forecast@v_anomaly@v format."""
        pairing = parse_pairing_identifier("prophet@0.1.0_adaptive_band@0.2.0")

        assert pairing.forecast_model == "prophet"
        assert pairing.forecast_version == "0.1.0"
        assert pairing.anomaly_model == "adaptive_band"
        assert pairing.anomaly_version == "0.2.0"

    def test_parse_pairing_identifier_forecast_version_only(self) -> None:
        """parse_pairing_identifier handles forecast version only."""
        pairing = parse_pairing_identifier("prophet@0.1.0_adaptive_band")

        assert pairing.forecast_model == "prophet"
        assert pairing.forecast_version == "0.1.0"
        assert pairing.anomaly_model == "adaptive_band"
        assert pairing.anomaly_version is None

    def test_parse_pairing_identifier_anomaly_version_only(self) -> None:
        """parse_pairing_identifier handles anomaly version only."""
        pairing = parse_pairing_identifier("prophet_adaptive_band@0.2.0")

        assert pairing.forecast_model == "prophet"
        assert pairing.forecast_version is None
        assert pairing.anomaly_model == "adaptive_band"
        assert pairing.anomaly_version == "0.2.0"

    def test_parse_pairing_identifier_empty_string(self) -> None:
        """parse_pairing_identifier raises on empty string."""
        with pytest.raises(ValueError, match="Empty"):
            parse_pairing_identifier("")

    def test_parse_pairing_identifier_no_underscore(self) -> None:
        """parse_pairing_identifier raises when no underscore separator."""
        with pytest.raises(ValueError, match="Invalid"):
            parse_pairing_identifier("prophet")

    def test_parse_pairing_identifier_malformed_version(self) -> None:
        """parse_pairing_identifier handles malformed version (empty after @)."""
        # When version is malformed (e.g., empty after @), forecast_model keeps the @
        pairing = parse_pairing_identifier("prophet@_adaptive_band")

        assert pairing.forecast_model == "prophet@"
        assert pairing.anomaly_model == "adaptive_band"


class TestGetPairingsFromEnvOrDefault:
    """Tests for get_pairings_from_env_or_default function."""

    def test_get_pairings_from_env_or_default_no_env_var(self) -> None:
        """get_pairings_from_env_or_default returns defaults when env var not set."""
        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations.get_model_pairings_env",
            return_value=None,
        ):
            pairings = get_pairings_from_env_or_default()

            assert len(pairings) > 0
            assert all(isinstance(p, ModelPairing) for p in pairings)

    def test_get_pairings_from_env_or_default_empty_env_var(self) -> None:
        """get_pairings_from_env_or_default returns defaults when env var is empty."""
        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations.get_model_pairings_env",
            return_value="",
        ):
            pairings = get_pairings_from_env_or_default()

            assert len(pairings) > 0

    def test_get_pairings_from_env_or_default_with_env_var(self) -> None:
        """get_pairings_from_env_or_default parses env var correctly."""
        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations.get_model_pairings_env",
            return_value="prophet_adaptive_band,datahub_datahub_forecast_anomaly",
        ):
            pairings = get_pairings_from_env_or_default()

            assert len(pairings) == 2
            assert all(isinstance(p, ModelPairing) for p in pairings)

    def test_get_pairings_from_env_or_default_with_known_name(self) -> None:
        """get_pairings_from_env_or_default resolves known pairing names."""
        known_pairing = DEFAULT_MODEL_PAIRINGS[0]
        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations.get_model_pairings_env",
            return_value=known_pairing.name,
        ):
            pairings = get_pairings_from_env_or_default()

            assert len(pairings) == 1
            assert pairings[0].name == known_pairing.name

    def test_get_pairings_from_env_or_default_invalid_pairing(self) -> None:
        """get_pairings_from_env_or_default raises on invalid pairing (no underscore)."""
        # Must contain no "_" so parse_pairing_identifier raises (expects 'forecast_anomaly')
        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations.get_model_pairings_env",
                return_value="invalidpairingnounderscore",
            ),
            pytest.raises(TrainingErrorException) as excinfo,
        ):
            get_pairings_from_env_or_default()

        assert excinfo.value.error_type == MonitorErrorTypeClass.INVALID_PARAMETERS
        assert "invalid_pairings" in excinfo.value.properties

    def test_get_pairings_from_env_or_default_mixed_valid_invalid(self) -> None:
        """get_pairings_from_env_or_default raises when some pairings are invalid."""
        # Second token has no "_" so parse_pairing_identifier raises for it
        with (
            patch(
                "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations.get_model_pairings_env",
                return_value="prophet_adaptive_band,invalidnounderscore",
            ),
            pytest.raises(TrainingErrorException) as excinfo,
        ):
            get_pairings_from_env_or_default()

        assert excinfo.value.error_type == MonitorErrorTypeClass.INVALID_PARAMETERS

    def test_get_pairings_from_env_or_default_with_whitespace(self) -> None:
        """get_pairings_from_env_or_default handles whitespace in env var."""
        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations.get_model_pairings_env",
            return_value=" prophet_adaptive_band , datahub_datahub_forecast_anomaly ",
        ):
            pairings = get_pairings_from_env_or_default()

            assert len(pairings) == 2
            assert all(isinstance(p, ModelPairing) for p in pairings)

    def test_get_pairings_from_env_or_default_with_versions(self) -> None:
        """get_pairings_from_env_or_default parses pairings with versions."""
        with patch(
            "datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations.get_model_pairings_env",
            return_value="prophet@0.1.0_adaptive_band@0.2.0",
        ):
            pairings = get_pairings_from_env_or_default()

            assert len(pairings) == 1
            assert pairings[0].forecast_version == "0.1.0"
            assert pairings[0].anomaly_version == "0.2.0"
