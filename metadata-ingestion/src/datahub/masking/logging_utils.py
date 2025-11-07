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

    This logger writes directly to the original stderr captured before
    masking initialization, ensuring it never goes through the masking
    filter which could cause deadlock.

    The deadlock scenario this prevents:
    1. Thread holds _pattern_lock in masking code
    2. Calls logger.warning() from within masking code
    3. Logger writes to wrapped sys.stderr
    4. Wrapper calls _mask_text() to mask the log message
    5. _mask_text() tries to acquire _pattern_lock again
    6. DEADLOCK - thread waiting for itself

    Args:
        name: Logger name (typically __name__ from the calling module)

    Returns:
        Configured logger that's safe to use within masking code

    Example:
        >>> from datahub.masking.logging_utils import get_masking_safe_logger
        >>> logger = get_masking_safe_logger(__name__)
        >>> logger.warning("This message bypasses masking")
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
    """
    Reset all masking-safe loggers (primarily for testing).

    This removes handlers from all loggers that were configured by
    get_masking_safe_logger(), allowing tests to start with clean state.
    """
    # Get all loggers under the masking namespace
    masking_namespace = "datahub.ingestion.masking"

    for name in list(logging.Logger.manager.loggerDict.keys()):
        if name.startswith(masking_namespace):
            logger = logging.getLogger(name)
            # Remove all handlers
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            # Reset propagate flag
            logger.propagate = True
