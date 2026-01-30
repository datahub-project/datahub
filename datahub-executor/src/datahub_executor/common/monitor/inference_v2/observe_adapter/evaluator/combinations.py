"""
Model Pairing Configuration.

Defines simple forecast + anomaly model pairings for evaluation.
Uses registry names and versions from observe-models.

This configuration belongs in datahub-executor (business logic),
not observe-models (ML library).
"""

from dataclasses import dataclass
from typing import List, Optional

from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import TrainingErrorException
from datahub_executor.common.monitor.inference_v2.inference_utils import (
    get_model_pairings_env,
)


@dataclass(frozen=True)
class ModelPairing:
    """
    Forecast + anomaly model pairing using registry names.

    Names must match what's registered in observe-models ModelRegistry.
    Versions are independent - forecast and anomaly can have different versions.

    For anomaly models that don't require a forecast model (e.g., "deepsvdd"),
    set forecast_model and forecast_version to None.

    Attributes:
        anomaly_model: Registry name for anomaly model (required)
            (e.g., "datahub_forecast_anomaly", "adaptive_band", "iforest", "deepsvdd")
        anomaly_version: Optional version of the anomaly model (e.g., "0.1.0").
            If None, the latest stable version (per observe-models registry) is used.
        forecast_model: Registry name for forecast model (optional for direct anomaly models)
            (e.g., "datahub", "prophet", "nbeats", "chronos2", "autogluon")
        forecast_version: Optional version of the forecast model.
            If None, the latest stable version (per observe-models registry) is used.

    Examples:
        >>> # Forecast-based anomaly detection
        >>> pairing = ModelPairing(
        ...     anomaly_model="datahub_forecast_anomaly",
        ...     anomaly_version="0.1.0",
        ...     forecast_model="datahub",
        ...     forecast_version="0.1.0",
        ... )
        >>>
        >>> # Direct anomaly detection (no forecast model)
        >>> pairing = ModelPairing(
        ...     anomaly_model="deepsvdd",
        ...     anomaly_version="0.1.0",
        ... )
    """

    anomaly_model: str
    anomaly_version: Optional[str] = None
    forecast_model: Optional[str] = None
    forecast_version: Optional[str] = None

    @property
    def name(self) -> str:
        """Generate a unique name for this pairing."""
        if self.forecast_model:
            return f"{self.forecast_model}_{self.anomaly_model}"
        return self.anomaly_model

    @property
    def forecast_model_key(self) -> Optional[str]:
        """Return the forecast model identifier (supports model@version)."""
        if self.forecast_model is None:
            return None
        if self.forecast_version:
            return f"{self.forecast_model}@{self.forecast_version}"
        return self.forecast_model

    @property
    def anomaly_model_key(self) -> str:
        """Return the anomaly model identifier (supports model@version)."""
        if self.anomaly_version:
            return f"{self.anomaly_model}@{self.anomaly_version}"
        return self.anomaly_model

    @property
    def requires_forecast(self) -> bool:
        """Check if this pairing requires a forecast model."""
        return self.forecast_model is not None

    def __str__(self) -> str:
        if self.forecast_model:
            forecast = (
                f"{self.forecast_model}@{self.forecast_version}"
                if self.forecast_version
                else self.forecast_model
            )
            anomaly = (
                f"{self.anomaly_model}@{self.anomaly_version}"
                if self.anomaly_version
                else self.anomaly_model
            )
            return f"{forecast} + {anomaly}"
        return (
            f"{self.anomaly_model}@{self.anomaly_version}"
            if self.anomaly_version
            else self.anomaly_model
        )


# =============================================================================
# Default Model Pairings
# =============================================================================

# Default pairings - evaluate both, select best
DEFAULT_MODEL_PAIRINGS: List[ModelPairing] = [
    # Prophet + Adaptive Band
    ModelPairing(
        anomaly_model="adaptive_band",
        forecast_model="prophet",
    ),
    # DataHub forecast + DataHub anomaly detection
    ModelPairing(
        anomaly_model="datahub_forecast_anomaly",
        forecast_model="datahub",
    ),
]


def get_default_pairings() -> List[ModelPairing]:
    """Get the default model pairings for evaluation."""
    return list(DEFAULT_MODEL_PAIRINGS)


def get_all_pairings() -> List[ModelPairing]:
    """Get all supported pairings (currently identical to defaults)."""
    return list(DEFAULT_MODEL_PAIRINGS)


def get_pairing_by_name(name: str) -> Optional[ModelPairing]:
    """
    Get a model pairing by its generated name.

    Args:
        name: The pairing name (e.g., "datahub_datahub_forecast_anomaly")

    Returns:
        ModelPairing if found, None otherwise.
    """
    for pairing in get_all_pairings():
        if pairing.name == name:
            return pairing
    return None


def _parse_model_identifier(raw: str) -> tuple[str, Optional[str]]:
    """
    Parse a model identifier of the form 'model' or 'model@version'.
    """
    token = raw.strip()
    if "@" not in token:
        return token, None
    model, version = token.split("@", 1)
    model = model.strip()
    version = version.strip()
    if not model or not version:
        return token, None
    return model, version


def parse_pairing_identifier(raw: str) -> ModelPairing:
    """
    Parse a pairing identifier into a ModelPairing.

    Supported:
    - 'forecast_anomaly' (e.g. 'prophet_adaptive_band')
    - 'forecast@v_anomaly@v' (e.g. 'prophet@0.1.0_adaptive_band@0.2.0')
    """
    token = raw.strip()
    if not token:
        raise ValueError("Empty pairing identifier")
    if "_" not in token:
        raise ValueError(
            f"Invalid pairing identifier '{token}'. Expected 'forecast_anomaly'."
        )
    forecast_raw, anomaly_raw = token.split("_", 1)
    forecast_model, forecast_version = _parse_model_identifier(forecast_raw)
    anomaly_model, anomaly_version = _parse_model_identifier(anomaly_raw)
    return ModelPairing(
        anomaly_model=anomaly_model,
        anomaly_version=anomaly_version,
        forecast_model=forecast_model,
        forecast_version=forecast_version,
    )


def get_pairings_from_env_or_default() -> List[ModelPairing]:
    """
    Return the pairings to evaluate for a run.

    Env var:
      DATAHUB_EXECUTOR_MODEL_PAIRINGS: comma-separated list of pairing identifiers.
      If unset/empty: return DEFAULT_MODEL_PAIRINGS.
    """
    raw = get_model_pairings_env()
    if raw is None:
        return get_default_pairings()

    requested = [t.strip() for t in raw.split(",") if t.strip()]
    if not requested:
        return get_default_pairings()

    parsed: List[ModelPairing] = []
    invalid: List[str] = []
    for token in requested:
        # First allow referencing by known name (no versions) for convenience.
        known = get_pairing_by_name(token)
        if known is not None:
            parsed.append(known)
            continue
        try:
            parsed.append(parse_pairing_identifier(token))
        except Exception:
            invalid.append(token)

    if invalid:
        raise TrainingErrorException(
            message="Invalid model pairing selection",
            error_type=MonitorErrorTypeClass.INVALID_PARAMETERS,
            properties={
                "step": "pairing_selection",
                "requested_pairings": ",".join(requested),
                "invalid_pairings": ",".join(invalid),
            },
        )

    return parsed
