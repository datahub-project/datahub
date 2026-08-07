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
# True once we've warned about masking being disabled by env var. Separate
# from ``_bootstrap_completed`` (which means "the filter is installed") so a
# disabled call does not strand the install latch True — see
# ``initialize_secret_masking``.
_disabled_warned = False
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
    Also clears ``_disabled_warned`` so a transition from disabled to enabled
    re-warns if the env var is later flipped back off.
    """
    global _bootstrap_completed, _bootstrap_error, _disabled_warned
    with _bootstrap_lock:
        _bootstrap_completed = False
        _bootstrap_error = None
        _disabled_warned = False


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


def _restore_exception_hook() -> None:
    """Restore the original ``sys.excepthook`` saved at install time.

    Called from both ``shutdown_secret_masking`` (the refcounted lifecycle) and
    ``SecretRegistry.reset_instance`` (the test teardown path). Without this
    being called from ``reset_instance`` too, a ``shutdown_secret_masking()``
    that skipped teardown (peer execution still active on another thread)
    left ``reset_instance`` to drop the filter but NOT restore the excepthook —
    leaving a hook bound to a dead registry for the rest of the session.
    Idempotent: a no-op if no hook was ever installed.
    """
    global _original_excepthook
    if _original_excepthook is not None:
        sys.excepthook = _original_excepthook
        _original_excepthook = None


def _force_teardown_secret_masking() -> None:
    """Force-tear down the masking infrastructure regardless of active scopes.

    Test-only (underscore-prefixed): the refcounted ``shutdown_secret_masking``
    refuses to uninstall the filter while execution scopes are active (fail-
    safe — masking stays on). Tests need a path that tears down unconditionally
    so a leaked scope from a buggy test does not strand the filter into the
    next test. Production code should never call this — tearing down masking
    while a live execution is still logging is a deliberate fail-open.

    Uninstalls the filter, restores the excepthook, clears the bootstrap latch,
    and resets masking-safe loggers. Does NOT touch the registry's execution
    scopes — the caller (``SecretRegistry.reset_instance``) owns those.
    """
    global _bootstrap_completed, _bootstrap_error
    with _bootstrap_lock:
        try:
            _uninstall_masking_filter()
        except Exception as e:
            logger.debug("_force_teardown: uninstall failed: %r", e)
        try:
            _restore_exception_hook()
        except Exception as e:
            logger.debug("_force_teardown: restore excepthook failed: %r", e)
        try:
            from datahub.masking.logging_utils import reset_masking_safe_loggers

            reset_masking_safe_loggers()
        except Exception as e:
            logger.debug("_force_teardown: reset safe loggers failed: %r", e)
        _bootstrap_completed = False
        _bootstrap_error = None


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
        # Warn once per disabled episode. Do NOT latch ``_bootstrap_completed``
        # here: that flag means "the filter is installed", and a disabled call
        # installs nothing. Latching True on the disabled branch stranded the
        # flag so a later call with masking enabled short-circuited the
        # install (the ``if not _bootstrap_completed`` check below saw True)
        # but still ran ``begin_execution()`` and returned a non-None token —
        # the caller got the "masking is on" signal with no filter and no
        # excepthook. ``_disabled_warned`` is a separate warn-once flag that
        # does not affect the install decision.
        global _disabled_warned
        if not _disabled_warned:
            logger.warning(
                "Secret masking is DISABLED via DATAHUB_DISABLE_SECRET_MASKING "
                "environment variable. Sensitive information will be exposed in "
                "logs. Only use this for debugging!"
            )
            _disabled_warned = True
        return None

    registry = SecretRegistry.get_instance()

    # The filter + exception hook are installed once for the process lifetime;
    # they are harmless when no secrets are registered. Only secrets are scoped
    # per execution (begin_execution below). `force` is accepted for backward
    # compatibility but no longer gates anything — installation is idempotent.
    #
    # ``_bootstrap_completed`` is a pure "don't redo the expensive install
    # work" optimization, set only after a real install succeeds and cleared
    # on teardown. It is NOT the source of truth for "is the filter
    # installed?" — that is ``masking_filter._installed_filter``. A test mock
    # patching ``install_masking_filter`` returns a Mock (non-None), so it
    # latches correctly here without bootstrap reaching into
    # ``masking_filter._installed_filter``. The flag MUST be cleared wherever
    # the filter is uninstalled, or it strands True while the filter is gone
    # and the next initialize short-circuits with masking off.
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

    # Teardown swallows errors (failed teardown leaves masking ON, which is
    # fail-safe; raising from cleanup destroys the original exception) but
    # clears the bootstrap latch in a ``finally`` around the teardown steps so
    # a raise from ``_uninstall_masking_filter`` or
    # ``reset_masking_safe_loggers`` does not strand ``_bootstrap_completed``
    # True over a partially-torn-down filter — the next initialize would then
    # short-circuit with masking off. ``_uninstall_masking_filter`` is itself
    # step-wise tolerant, so a partial teardown is the only failure mode that
    # reaches this finally. The early-return path (other executions still
    # active) does NOT clear the latch — masking stays installed there.
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

            try:
                # Internal caller: bypass the public guard (we already
                # verified via end_execution that no scopes are active).
                _uninstall_masking_filter()

                # Restore original exception hook
                _restore_exception_hook()

                # Reset masking-safe loggers to restore normal logging
                from datahub.masking.logging_utils import reset_masking_safe_loggers

                reset_masking_safe_loggers()
            finally:
                # Clear the latch whether the teardown steps succeeded or not.
                # A stranded ``True`` over a half-torn state would make the
                # next initialize short-circuit the install and run with
                # masking off. ``_uninstall`` is step-wise tolerant, so even
                # a partial teardown leaves the globals consistent enough
                # for a re-install to rebuild from.
                _bootstrap_completed = False
                _bootstrap_error = None

        logger.info("Secret masking shutdown completed")
    except Exception as e:
        logger.error(f"Error during secret masking shutdown: {e}", exc_info=True)
