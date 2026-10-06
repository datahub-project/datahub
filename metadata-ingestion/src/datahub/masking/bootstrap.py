"""
Bootstrap module for secret masking initialization.

Architecture:
    This module sets up the masking infrastructure (logging filter + exception hook).
    Secret discovery is separated and happens automatically at point-of-read:
    - Config loaders register secrets during ${VAR} expansion
    - Pydantic models register SecretStr fields during validation

    Masking state is process-lifetime: installation is idempotent (each call
    also covers logging handlers created since the previous one) and nothing
    is ever torn down, so no caller can disable masking for a concurrent one.
"""

import logging
import sys
import threading
import traceback
from types import TracebackType
from typing import Any, Callable, Optional, Type

from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.masking_filter import (
    SecretMaskingFilter,
    install_masking_filter,
)
from datahub.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)

_bootstrap_completed = False
_bootstrap_error: Optional[Exception] = None
_bootstrap_lock = threading.Lock()


def is_bootstrapped() -> bool:
    return _bootstrap_completed


def get_bootstrap_error() -> Optional[Exception]:
    return _bootstrap_error


class _MaskingExceptHook:
    """sys.excepthook replacement that masks the formatted traceback."""

    def __init__(
        self,
        masking_filter: SecretMaskingFilter,
        original_excepthook: Callable[..., Any],
    ) -> None:
        self._masking_filter = masking_filter
        self.original_excepthook = original_excepthook

    def __call__(
        self,
        exc_type: Type[BaseException],
        exc_value: BaseException,
        exc_traceback: Optional[TracebackType],
    ) -> None:
        try:
            tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            sys.stderr.write(self._masking_filter.mask_text("".join(tb_lines)))
        except Exception as e:
            logger.error(f"Failed to mask exception: {e}")
            self.original_excepthook(exc_type, exc_value, exc_traceback)


def _install_exception_hook(masking_filter: SecretMaskingFilter) -> None:
    if isinstance(sys.excepthook, _MaskingExceptHook):
        return
    sys.excepthook = _MaskingExceptHook(masking_filter, sys.excepthook)
    logger.debug("Installed custom exception hook for secret masking")


def _quiet_http_debug_logging() -> None:
    try:
        import http.client

        http.client.HTTPConnection.debuglevel = 0
    except Exception:
        pass

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


def initialize_secret_masking(max_message_size: int = 5000) -> None:
    """Install masking on logging handlers, stdout/stderr, and the exception
    hook. Safe and cheap to call at every execution start: repeated calls
    re-scan for handlers created since the previous call.
    """
    global _bootstrap_completed, _bootstrap_error

    from datahub.masking.secret_registry import is_masking_enabled

    if not is_masking_enabled():
        if not _bootstrap_completed:
            logger.warning(
                "Secret masking is DISABLED via DATAHUB_DISABLE_SECRET_MASKING environment variable. "
                "Sensitive information will be exposed in logs. Only use this for debugging!"
            )
        _bootstrap_completed = True
        return

    with _bootstrap_lock:
        try:
            masking_filter = install_masking_filter(
                secret_registry=SecretRegistry.get_instance(),
                max_message_size=max_message_size,
                install_stdout_wrapper=True,
            )
            _install_exception_hook(masking_filter)
            logging.captureWarnings(True)
            _quiet_http_debug_logging()

            if not _bootstrap_completed:
                logger.info("Secret masking infrastructure initialized")
            _bootstrap_completed = True
            _bootstrap_error = None

        except Exception as e:
            _bootstrap_error = e
            logger.error(f"Failed to initialize secret masking: {e}", exc_info=True)


def shutdown_secret_masking() -> None:
    """Masking is process-lifetime; nothing is torn down.

    Retained so callers written against the old per-run lifecycle keep
    working: tearing masking down here used to clear the shared registry and
    strip the filters, unmasking every concurrent execution.
    """
    logger.debug("shutdown_secret_masking() is a no-op; masking stays installed")
