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
    # Get all loggers under the masking namespace
    masking_namespace = "datahub.masking"

    for name in list(logging.Logger.manager.loggerDict.keys()):
        if name.startswith(masking_namespace):
            logger = logging.getLogger(name)
            # Remove all handlers
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            # Reset propagate flag to allow normal logging
            logger.propagate = True
