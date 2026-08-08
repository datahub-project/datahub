"""Bootstrap for the secret-masking infrastructure."""

import logging
import sys
import threading
import traceback
from types import TracebackType
from typing import Optional

from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.masking_filter import SecretMaskingFilter, install_masking_filter
from datahub.masking.secret_registry import SecretRegistry, is_masking_enabled

logger = get_masking_safe_logger(__name__)

_bootstrap_completed = False
_bootstrap_error: Optional[Exception] = None
_original_excepthook = None
_disabled_warned = False
_bootstrap_lock = threading.Lock()

MASKING_ERROR_NOTE = "[masking failed; traceback suppressed for security]"


def is_bootstrapped() -> bool:
    """True once the filter + excepthook + stream wrappers are installed."""
    return _bootstrap_completed


def get_bootstrap_error() -> Optional[Exception]:
    """Return the last install error, or None if install succeeded."""
    return _bootstrap_error


def reset_bootstrap_state() -> None:
    # Test-only: clear the bootstrap latch and warn-once flags. Also call
    # SecretRegistry.reset_instance(); a stranded latch keeps the filter bound
    # to a dead singleton.
    global _bootstrap_completed, _bootstrap_error, _disabled_warned
    with _bootstrap_lock:
        _bootstrap_completed = False
        _bootstrap_error = None
        _disabled_warned = False


def _install_exception_hook(masking_filter: SecretMaskingFilter) -> None:
    # Process-lifetime excepthook; on masking failure print only the class
    # name plus a note, never the raw traceback (fail-closed).
    global _original_excepthook
    if _original_excepthook is None:
        _original_excepthook = sys.excepthook

    def masking_excepthook(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_traceback: Optional[TracebackType],
    ) -> None:
        try:
            tb_text = "".join(
                traceback.format_exception(exc_type, exc_value, exc_traceback)
            )
            masked = masking_filter.mask_text(tb_text)
            sys.stderr.write(masked)
        except Exception:
            sys.stderr.write(f"{exc_type.__name__}: {MASKING_ERROR_NOTE}\n")

    sys.excepthook = masking_excepthook
    logger.debug("Installed masking exception hook")


def initialize_secret_masking(
    max_message_size: int = 5000,
    force: bool = False,
) -> Optional[str]:
    # Install masking and open a per-execution secret scope. Returns the
    # execution token, or None if masking is disabled. Raises RuntimeError
    # if masking is enabled but install failed (fail-loud). ``force`` is
    # accepted and ignored — kept for backward compatibility.
    global _bootstrap_completed, _bootstrap_error, _disabled_warned

    if not is_masking_enabled():
        if not _disabled_warned:
            logger.warning(
                "Secret masking is DISABLED via "
                "DATAHUB_DISABLE_SECRET_MASKING. Sensitive information will "
                "be exposed in logs. Only use this for debugging."
            )
            _disabled_warned = True
        return None

    registry = SecretRegistry.get_instance()
    with _bootstrap_lock:
        if not _bootstrap_completed:
            try:
                logger.info("Initializing secret masking infrastructure")
                installed = install_masking_filter(
                    secret_registry=registry,
                    max_message_size=max_message_size,
                    install_stdout_wrapper=True,
                )
                _install_exception_hook(installed)
                logging.captureWarnings(True)
                import http.client

                http.client.HTTPConnection.debuglevel = 0
                for logger_name in [
                    "urllib3",
                    "urllib3.connectionpool",
                    "urllib3.util.retry",
                    "requests",
                ]:
                    logging.getLogger(logger_name).setLevel(logging.INFO)
                _bootstrap_completed = True
                _bootstrap_error = None
                logger.info(
                    "Secret masking infrastructure initialized; secrets "
                    "will be registered automatically as they are loaded."
                )
            except Exception as e:
                _bootstrap_error = e
                logger.critical(
                    "Failed to initialize secret masking: %s. Secrets may "
                    "reach logs unmasked.",
                    e,
                    exc_info=True,
                )
                raise RuntimeError(f"Secret masking installation failed: {e}") from e
        else:
            # Re-scan handlers and rebind to the current registry: handlers
            # added while the wrap was inert carry no filter.
            install_masking_filter(secret_registry=registry)
        return registry.begin_execution()


def shutdown_secret_masking(execution_id: Optional[str] = None) -> None:
    # End this execution's masking scope. No logging teardown, no excepthook
    # restore, no latch clearing — the filter installs once and is never
    # uninstalled in production.
    SecretRegistry.get_instance().end_execution(execution_id)
