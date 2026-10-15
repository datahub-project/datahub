"""
Logging utilities for the secret masking framework.

This module provides loggers that are safe to use within masking code,
preventing re-entrancy deadlocks by writing directly to the original stderr.
"""

import logging
import sys

# Capture original stderr BEFORE any masking initialization
# This ensures masking-safe loggers write to unwrapped stderr
_original_stderr = sys.stderr

MASKING_LOGGER_NAMESPACE = "datahub.masking"


def is_masking_internal_logger(name: str) -> bool:
    """Exact namespace or dotted descendants only: a plain prefix match would
    also claim unrelated loggers (e.g. datahub.maskingness) and leave their
    handlers unmasked."""
    return name == MASKING_LOGGER_NAMESPACE or name.startswith(
        MASKING_LOGGER_NAMESPACE + "."
    )


def get_masking_safe_logger(name: str) -> logging.Logger:
    """
    Get a logger that bypasses masking to prevent re-entrancy deadlock.

    Writes directly to original stderr, preventing deadlock when masking code logs.
    """
    logger = logging.getLogger(name)

    # Only configure if not already configured (avoid duplicate handlers)
    if not logger.handlers:
        handler = logging.StreamHandler(_original_stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
        )
        logger.addHandler(handler)

        # CRITICAL: Don't propagate to root logger
        # This prevents the log from going through any parent handlers
        # that might have masking filters installed
        logger.propagate = False

    return logger


def reset_masking_safe_loggers() -> None:
    """Reset all masking-safe loggers to allow normal logging."""
    for name in list(logging.Logger.manager.loggerDict.keys()):
        if is_masking_internal_logger(name):
            logger = logging.getLogger(name)
            # Remove all handlers
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            # Reset propagate flag to allow normal logging
            logger.propagate = True
