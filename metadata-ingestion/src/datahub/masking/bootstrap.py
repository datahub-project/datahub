"""
Bootstrap module for secret masking initialization.

Architecture:
    This module sets up the masking infrastructure (logging filter + exception hook).
    Secret discovery is separated and happens automatically at point-of-read:
    - Config loaders register secrets during ${VAR} expansion
    - Pydantic models register SecretStr fields during validation

    This separation means:
    - Infrastructure setup is context-independent
    - Components own their secret registration
    - Errors surface at point-of-read (not during bootstrap)
"""

import logging
import sys
import threading
import traceback
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
    """Check if secret masking bootstrap has completed."""
    return _bootstrap_completed


def get_bootstrap_error() -> Optional[Exception]:
    """Get bootstrap error if bootstrap failed."""
    return _bootstrap_error


def _install_exception_hook(registry: SecretRegistry) -> None:
    """Install custom exception hook to mask secrets."""
    global _original_excepthook

    # Store original exception hook for later restoration
    if _original_excepthook is None:
        _original_excepthook = sys.excepthook

    original_excepthook = _original_excepthook

    masking_filter = SecretMaskingFilter(registry)

    def masking_excepthook(exc_type, exc_value, exc_traceback):
        """Custom exception hook that masks secrets in exception messages."""
        try:
            # Format the exception to a string
            tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            tb_text = "".join(tb_lines)

            # Mask secrets in the formatted traceback
            masked_tb_text = masking_filter.mask_text(tb_text)

            # Write masked traceback to stderr
            sys.stderr.write(masked_tb_text)
        except Exception as e:
            # If masking fails, fall back to original exception hook
            logger.error(f"Failed to mask exception: {e}")
            original_excepthook(exc_type, exc_value, exc_traceback)

    # Install the custom hook
    sys.excepthook = masking_excepthook
    logger.debug("Installed custom exception hook for secret masking")


def initialize_secret_masking(
    max_message_size: int = 5000,
    force: bool = False,
) -> None:
    """
    Initialize secret masking infrastructure (logging filter + exception hook).

    Secrets register automatically at point-of-read.
    """
    global _bootstrap_completed, _bootstrap_error

    # Check if masking is disabled via environment variable
    from datahub.masking.secret_registry import is_masking_enabled

    if not is_masking_enabled():
        logger.warning(
            "Secret masking is DISABLED via DATAHUB_DISABLE_SECRET_MASKING environment variable. "
            "Sensitive information will be exposed in logs. Only use this for debugging!"
        )
        _bootstrap_completed = True  # Mark as completed to avoid repeated warnings
        return

    registry = SecretRegistry.get_instance()

    # The filter + exception hook are installed once for the process lifetime;
    # they are harmless when no secrets are registered. Only secrets are scoped
    # per execution (ensure_execution below). `force` is accepted for backward
    # compatibility but no longer gates anything — installation is idempotent.
    #
    # The whole install-check + scope registration is done under _bootstrap_lock
    # (the same lock shutdown holds across its teardown), so a concurrent
    # shutdown can't decide it is the last execution and uninstall the filter
    # between our completion-check and our scope registration — which would let
    # this execution run unmasked.
    with _bootstrap_lock:
        if not _bootstrap_completed:
            try:
                logger.info("Initializing secret masking infrastructure")

                # Install logging filter + stdout wrapper (idempotent)
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
                return  # Don't raise - graceful degradation

        # Open a secret scope for this execution (idempotent within one context).
        # Secrets registered after this belong to this execution and are dropped
        # by the matching shutdown_secret_masking(), without affecting others.
        registry.ensure_execution()


def shutdown_secret_masking() -> None:
    """End the current execution's masking scope.

    Drops only this execution's secrets. If other executions are still running,
    masking stays installed (their secrets must keep being masked). Only when the
    last active execution ends do we fully tear down the filter/exception hook.
    """
    global _bootstrap_completed, _bootstrap_error, _original_excepthook

    try:
        registry = SecretRegistry.get_instance()
        # Hold _bootstrap_lock across the "am I the last execution?" decision and
        # the teardown, so it is atomic w.r.t. a concurrent initialize that is
        # checking _bootstrap_completed and registering a new scope. Otherwise an
        # execution starting in this window would skip re-install and then have
        # the filter uninstalled out from under it (running unmasked).
        with _bootstrap_lock:
            # Remove this execution's secrets; bail out if others are still active.
            if registry.end_execution():
                return

            # Last execution finished — fully tear down.
            uninstall_masking_filter()

            # Restore original exception hook
            if _original_excepthook is not None:
                sys.excepthook = _original_excepthook
                _original_excepthook = None

            # Reset masking-safe loggers to restore normal logging
            from datahub.masking.logging_utils import reset_masking_safe_loggers

            reset_masking_safe_loggers()

            _bootstrap_completed = False
            _bootstrap_error = None

        logger.info("Secret masking shutdown completed")
    except Exception as e:
        logger.error(f"Error during secret masking shutdown: {e}")
