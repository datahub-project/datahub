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
    _uninstall_masking_filter,
    install_masking_filter,
)
from datahub.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)

# Bootstrap state tracking
_bootstrap_completed = False
_bootstrap_error: Optional[Exception] = None
_original_excepthook = None  # Track original exception hook for restoration
# ``RLock`` so reset_instance can re-enter it if reached from inside a locked
# region (see SecretRegistry.reset_instance). shutdown_secret_masking takes
# this first, then SecretRegistry._lock; reset_instance calls
# reset_bootstrap_state (which acquires this) after releasing SecretRegistry._lock.
_bootstrap_lock = threading.RLock()


def is_bootstrapped() -> bool:
    """Check if secret masking bootstrap has completed."""
    return _bootstrap_completed


def reset_bootstrap_state() -> None:
    """Clear the bootstrap-completed latch.

    Called by ``SecretRegistry.reset_instance()`` (which tears down the
    installed filter) so the flag can't strand True while the filter is gone.
    Without this, the next ``initialize_secret_masking()`` short-circuits on
    the flag and runs with masking off. Safe to call when not bootstrapped.
    """
    global _bootstrap_completed, _bootstrap_error
    with _bootstrap_lock:
        _bootstrap_completed = False
        _bootstrap_error = None


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
) -> Optional[str]:
    """
    Initialize secret masking infrastructure (logging filter + exception hook)
    and open a new per-execution secret scope.

    Secrets register automatically at point-of-read. Each call opens a
    *distinct* scope and returns its token; the caller owns nesting and
    passes the token to ``shutdown_secret_masking(token)`` when done. Two
    calls on the same context return different tokens and own different
    groups, so ending one execution never drops another's secrets or
    uninstalls the filter while another execution is still live.

    Returns:
        The execution token (opaque string) for this scope, or ``None`` if
        masking is disabled by configuration. A return of ``None`` from a
        *failed* install is raised as ``RuntimeError`` instead, so a caller
        can distinguish "off by configuration" from "installation crashed,
        secrets now reaching logs" — the exact failure shape that made the
        Python 3.13 ``_acquireLock`` removal silent.

    Raises:
        RuntimeError: if masking is enabled but installation failed (the
        filter is not installed and secrets would reach logs unmasked).
        This is fail-loud for a security control: a silent ``None`` on
        failure left no signal.

    .. deprecated::
        ``force`` is deprecated and ignored. The filter is installed once for the
        process lifetime and every call opens a per-execution secret scope, so
        there is nothing to force. The argument is kept only for backward
        compatibility and will be removed in a future release.
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
        return None

    registry = SecretRegistry.get_instance()

    # The filter + exception hook are installed once for the process lifetime;
    # they are harmless when no secrets are registered. Only secrets are scoped
    # per execution (begin_execution below). `force` is accepted for backward
    # compatibility but no longer gates anything — installation is idempotent.
    #
    # The install decision uses ``_bootstrap_completed`` as the latch (not
    # ``masking_filter._installed_filter``) so that test mocks patching
    # ``install_masking_filter`` still latch correctly. The flag MUST be cleared
    # wherever the filter is uninstalled, or it strands True while the filter
    # is gone and the next initialize short-circuits with masking off.
    # ``reset_bootstrap_state()`` (called by ``SecretRegistry.reset_instance()``,
    # which tears down the filter) clears it.
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
                install_masking_filter(
                    secret_registry=registry,
                    max_message_size=max_message_size,
                    install_stdout_wrapper=True,
                )
                _install_exception_hook(registry)
                logging.captureWarnings(True)
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
                _bootstrap_completed = True
                _bootstrap_error = None
                logger.info(
                    "Secret masking infrastructure initialized successfully. "
                    "Secrets will be registered automatically as they are loaded."
                )
            except Exception as e:
                _bootstrap_error = e
                # D7: fail-loud for a security control. A silent None here left
                # no signal that masking was off — the exact shape that made
                # the Python 3.13 _acquireLock removal silent. Log at critical
                # (the masking-safe logger writes to sys.__stderr__, which
                # survives celery's stderr proxy) and raise so the caller can
                # decide whether to proceed unmasked.
                logger.critical(
                    f"Failed to initialize secret masking: {e}. "
                    f"Secrets may reach logs unmasked.",
                    exc_info=True,
                )
                raise RuntimeError(f"Secret masking installation failed: {e}") from e

        # Always open a *distinct* scope per call. The caller owns the
        # token; a second call on the same context does NOT alias onto an
        # earlier scope, so ending one execution never drops another's
        # secrets.
        exec_id = registry.begin_execution()
        return exec_id


def _teardown_hook() -> None:
    """Test seam: called at a defined point inside shutdown_secret_masking
    (after the last-execution decision, before teardown). Tests monkeypatch
    this to gate concurrent threads deterministically. No-op in production."""
    return None


def shutdown_secret_masking(execution_id: Optional[str] = None) -> None:
    """End the current execution's masking scope.

    Drops only this execution's secrets. If other executions are still running,
    masking stays installed (their secrets must keep being masked). Only when the
    last active execution ends do we fully tear down the filter/exception hook.

    Args:
        execution_id: Token returned by ``initialize_secret_masking``. Pass it
            when the teardown caller runs in a different thread/context than the
            initializer (e.g. a dispatcher). Without it, the ambient context's
            scope is ended — a silent no-op if that context has none.
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
            if registry.end_execution(execution_id):
                return

            # Last execution finished — fully tear down.
            # Test seam: tests gate concurrent threads on this point. Keep the
            # call immediately after the last-execution decision and before any
            # teardown mutation so the seam's contract is "the race window".
            _teardown_hook()

            # Internal caller: bypass the public guard (we already verified
            # via end_execution that no scopes are active).
            _uninstall_masking_filter()

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
