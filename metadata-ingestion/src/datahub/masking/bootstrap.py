"""
Bootstrap module for secret masking initialization.

This module provides a simple initialization function that sets up the
logging filter and exception hook for secret masking. It does NOT load
or discover secrets - that is the responsibility of the components that
read secrets (config loaders, Pydantic models, etc.).
"""

import logging
import sys
import threading
from typing import Optional

from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.masking_filter import (
    SecretMaskingFilter,
    install_masking_filter,
    uninstall_masking_filter,
)
from datahub.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)

# Bootstrap state tracking
_bootstrap_completed = False
_bootstrap_error: Optional[Exception] = None
_original_excepthook = None  # Track original exception hook for restoration
_bootstrap_lock = threading.Lock()  # Thread safety for concurrent initialization


def is_bootstrapped() -> bool:
    """
    Check if secret masking bootstrap has completed.

    Returns:
        True if bootstrap completed successfully
    """
    return _bootstrap_completed


def get_bootstrap_error() -> Optional[Exception]:
    """
    Get bootstrap error if bootstrap failed.

    Returns:
        Exception if bootstrap failed, None otherwise
    """
    return _bootstrap_error


def _install_exception_hook(registry: SecretRegistry) -> None:
    """
    Install custom exception hook to mask secrets in unhandled exceptions.

    This ensures that if an exception with secrets reaches sys.excepthook,
    it gets masked before being printed to stderr.

    Args:
        registry: SecretRegistry instance for masking
    """
    global _original_excepthook

    # Store original exception hook for later restoration
    if _original_excepthook is None:
        _original_excepthook = sys.excepthook

    original_excepthook = _original_excepthook

    masking_filter = SecretMaskingFilter(registry)

    def masking_excepthook(exc_type, exc_value, exc_traceback):
        """
        Custom exception hook that masks secrets in exception messages.
        """
        try:
            # Mask the exception args
            if exc_value and hasattr(exc_value, "args") and exc_value.args:
                # Mask each arg
                masked_args = tuple(
                    masking_filter._mask_text(arg) if isinstance(arg, str) else arg
                    for arg in exc_value.args
                )

                # Create new exception with masked args
                exc_value = type(exc_value)(*masked_args)
        except Exception as e:
            # If masking fails, log error but continue with original exception
            logger.error(f"Failed to mask exception: {e}")

        # Call original exception hook with (potentially) masked exception
        original_excepthook(exc_type, exc_value, exc_traceback)

    # Install the custom hook
    sys.excepthook = masking_excepthook
    logger.debug("Installed custom exception hook for secret masking")


def initialize_secret_masking(
    max_message_size: int = 5000,
    force: bool = False,
    debug_mode: bool = False,
) -> None:
    """
    Initialize secret masking infrastructure.

    This function sets up the logging filter and exception hook for secret masking.
    It does NOT load or discover secrets - that happens automatically when:
    - Environment variables are expanded in config files (${VAR} syntax)
    - Pydantic models with SecretStr fields are loaded

    Masking can be disabled by setting DATAHUB_DISABLE_SECRET_MASKING=true
    environment variable OR by passing debug_mode=True.

    Thread-safe: Uses lock to prevent concurrent initialization.

    Args:
        max_message_size: Maximum log message size before truncation
        force: Force re-initialization even if already initialized
        debug_mode: Skip initialization and log reason (for debugging/testing).
                   When True, secrets are NOT masked. Use with caution!

    Example:
        >>> # Normal production use
        >>> initialize_secret_masking()

        >>> # Debug mode - skip masking
        >>> initialize_secret_masking(debug_mode=True)
        [WARNING] Secret masking DISABLED via debug_mode parameter...
    """
    global _bootstrap_completed, _bootstrap_error

    # Check if masking is disabled for debugging (env var OR parameter)
    from datahub.masking.secret_registry import is_masking_enabled

    if debug_mode or not is_masking_enabled():
        reason = (
            "debug_mode parameter"
            if debug_mode
            else "DATAHUB_DISABLE_SECRET_MASKING environment variable"
        )
        logger.warning(
            f"Secret masking is DISABLED via {reason}. "
            "Sensitive information will be exposed in logs. Only use this for debugging!"
        )
        _bootstrap_completed = True  # Mark as completed to avoid repeated warnings
        return

    # Thread-safe initialization: acquire lock for entire operation
    with _bootstrap_lock:
        # Prevent double initialization
        if _bootstrap_completed and not force:
            logger.debug("Secret masking already initialized")
            return

        try:
            logger.info("Initializing secret masking infrastructure")

            # Get registry
            registry = SecretRegistry.get_instance()

            # Install logging filter + stdout wrapper
            install_masking_filter(
                secret_registry=registry,
                max_message_size=max_message_size,
                install_stdout_wrapper=True,
            )

            # Install custom exception hook to mask unhandled exceptions
            _install_exception_hook(registry)

            # Configure warnings to use logging
            logging.captureWarnings(True)

            # Disable HTTP debug output (prevent deadlock)
            try:
                import http.client

                http.client.HTTPConnection.debuglevel = 0
            except Exception:
                pass

            # Set HTTP-related loggers to INFO (not DEBUG)
            for logger_name in [
                "urllib3",
                "urllib3.connectionpool",
                "urllib3.util.retry",
                "requests",
            ]:
                try:
                    logging.getLogger(logger_name).setLevel(logging.INFO)
                except Exception:
                    pass

            _bootstrap_completed = True
            _bootstrap_error = None
            logger.info(
                "Secret masking infrastructure initialized successfully. "
                "Secrets will be registered automatically as they are loaded."
            )

        except Exception as e:
            _bootstrap_error = e
            logger.error(f"Failed to initialize secret masking: {e}", exc_info=True)
            # Don't raise - graceful degradation


def shutdown_secret_masking() -> None:
    """
    Shutdown secret masking system.

    Used primarily for testing or cleanup.
    """
    global _bootstrap_completed, _bootstrap_error, _original_excepthook

    try:
        uninstall_masking_filter()

        # Restore original exception hook
        if _original_excepthook is not None:
            sys.excepthook = _original_excepthook
            _original_excepthook = None

        # Clear registry
        registry = SecretRegistry.get_instance()
        registry.clear()

        _bootstrap_completed = False
        _bootstrap_error = None

        logger.info("Secret masking shutdown completed")
    except Exception as e:
        logger.error(f"Error during secret masking shutdown: {e}")
