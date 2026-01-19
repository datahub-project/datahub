"""
Configuration for the V2 inference pipeline using observe-models.
"""

import logging

from datahub_executor.config import DATAHUB_USE_OBSERVE_MODELS

logger = logging.getLogger(__name__)

# Check if datahub_observe package is available
try:
    import datahub_observe  # noqa: F401

    OBSERVE_MODELS_AVAILABLE = True
except ImportError:
    OBSERVE_MODELS_AVAILABLE = False
    logger.debug(
        "datahub_observe package not available. "
        "V2 inference pipeline with observe-models will be disabled."
    )

# Requires both the environment variable AND the package to be available.
USE_OBSERVE_MODELS = DATAHUB_USE_OBSERVE_MODELS and OBSERVE_MODELS_AVAILABLE

if DATAHUB_USE_OBSERVE_MODELS and not OBSERVE_MODELS_AVAILABLE:
    logger.warning(
        "DATAHUB_USE_OBSERVE_MODELS=true but datahub_observe package is not installed. "
        "Falling back to V1 inference pipeline."
    )


def should_use_observe_models() -> bool:
    """
    Check if we should use observe-models V2 pipeline.

    Returns:
        True if observe-models should be used, False otherwise.
    """
    return USE_OBSERVE_MODELS
