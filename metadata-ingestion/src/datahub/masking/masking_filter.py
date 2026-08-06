"""
Logging filter for masking secrets in log messages and streams.

This module provides a Python logging.Filter that automatically masks
registered secrets in all log output. Secrets are replaced with
***REDACTED:VARIABLE_NAME*** for debugging while preventing leaks.

Key Features:
- Automatic masking of messages, arguments, and exceptions
- Deferred pattern rebuild (only during masking, not registration)
- Circuit breaker for graceful degradation
- Message truncation (5KB default) for performance
- Stream wrappers for stdout/stderr coverage

Performance:
- Pattern rebuilt only when needed during masking operations
- Lock-free masking with COW snapshots
- Truncation before masking avoids regex on huge strings
- Performance warnings at 100/500 secrets
"""

import io
import logging
import re
import sys
import threading
from typing import Any, Dict, Optional, TextIO, Tuple

from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)

# The single installed filter instance for this process (identity-based
# install/uninstall tracking). See install_masking_filter. Forward-annotated
# because SecretMaskingFilter is defined below.
_installed_filter: Optional["SecretMaskingFilter"] = None

# Constants
REDACTED_MASKING_NAMESPACE = "datahub.masking."
REDACTED_FORMAT = "***REDACTED:{name}***"
MASKING_ERROR_MESSAGE = "[MASKING_ERROR - OUTPUT_SUPPRESSED_FOR_SECURITY]"

# Module-private sentinel stamped onto records after masking so the
# idempotency guard can recognize them with `is` rather than truthiness.
# A caller-supplied ``extra={'_datahub_masked': True}`` would otherwise forge
# the guard and disable masking for that record. A caller can't forge this
# object.
_MASKED = object()
CIRCUIT_OPEN_MESSAGE = "[REDACTED: Masking Circuit Open]"

# LogRecord standard attributes (per logging.LogRecord.__init__ + attributes set
# by the framework during formatting). Extras added via ``extra={...}`` are
# everything NOT in this set, and we mask those recursively. Iterating the whole
# __dict__ and masking pathname/funcName/module would waste regex on the hot path
# and could corrupt those fields.
_STANDARD_RECORD_ATTRS = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "process",
        "processName",
        "taskName",
        "message",
        "asctime",
    }
)


class SecretMaskingFilter(logging.Filter):
    """Logging filter that masks secrets in log records."""

    def __init__(
        self,
        secret_registry: Optional[SecretRegistry] = None,
        max_message_size: int = 5000,
    ):
        """Initialize the masking filter."""
        super().__init__()

        self._registry = secret_registry or SecretRegistry.get_instance()
        self._max_message_size = max_message_size

        # Thread safety: RLock for pattern access
        self._pattern_lock = threading.RLock()

        # Pattern state (immutable references - copy-on-write)
        self._pattern: Optional[re.Pattern] = None
        self._replacements: Dict[str, str] = {}
        self._last_version: int = 0

        # Circuit breaker to prevent cascading failures
        self._failure_count = 0
        self._max_failures = 10
        self._circuit_open = False

    def _check_and_rebuild_pattern(self) -> None:
        """Check if pattern needs rebuilding and rebuild if necessary."""
        MAX_REBUILD_ATTEMPTS = 10  # Prevent infinite loops

        # Track last successfully built pattern for emergency fallback
        last_built_pattern: Optional[re.Pattern] = None
        last_built_replacements: Dict[str, str] = {}
        last_built_version: int = 0

        for attempt in range(MAX_REBUILD_ATTEMPTS):
            # Quick check WITHOUT lock (fast path)
            current_version = self._registry.get_version()

            with self._pattern_lock:
                if current_version == self._last_version:
                    return  # Pattern is up to date

            # Build pattern OUTSIDE lock (expensive operations)
            secrets = self._registry.get_all_secrets()

            if not secrets:
                with self._pattern_lock:
                    self._pattern = None
                    self._replacements = {}
                    self._last_version = current_version
                return

            # Sort by length (longest first) - NOT under lock
            sorted_secrets = sorted(
                secrets.items(), key=lambda x: len(x[0]), reverse=True
            )

            # Build pattern - NOT under lock
            # CRITICAL: re.escape() ensures secrets with regex metacharacters
            # (e.g., ".*", "a+b", "test|prod") are matched literally, not as regex
            escaped_values = [re.escape(value) for value, _ in sorted_secrets]
            pattern_str = "|".join(escaped_values)

            # Compile regex - NOT under lock (this is the expensive part!)
            try:
                new_pattern = re.compile(pattern_str)
                new_replacements = {value: name for value, name in sorted_secrets}

                # Save this for emergency fallback
                last_built_pattern = new_pattern
                last_built_replacements = new_replacements
                last_built_version = current_version
            except Exception as e:
                logger.error(f"Failed to compile masking pattern: {e}")
                return  # Keep using old pattern

            # Warn about performance impact with large secret counts
            secret_count = len(secrets)
            if secret_count > 500:
                logger.warning(
                    f"Very large secret count ({secret_count})! "
                    f"This may impact masking performance. "
                    f"Consider using Aho-Corasick algorithm for better performance."
                )
            elif secret_count >= 100:
                logger.warning(
                    f"Large number of secrets registered ({secret_count}). "
                    f"This may impact masking performance."
                )

            # Atomic swap under lock (fast!)
            with self._pattern_lock:
                # Check version again - secrets might have changed while building
                registry_version = self._registry.get_version()

                if registry_version == current_version:
                    # Version is stable, safe to swap in the new pattern
                    self._pattern = new_pattern
                    self._replacements = new_replacements
                    self._last_version = current_version

                    if attempt > 0:
                        logger.debug(
                            f"Rebuilt masking pattern with {secret_count} secrets "
                            f"(version {current_version}) after {attempt + 1} attempts"
                        )
                    else:
                        logger.debug(
                            f"Rebuilt masking pattern with {secret_count} secrets "
                            f"(version {current_version})"
                        )
                    return  # Success!

                # Version changed during build, loop will retry
                logger.debug(
                    f"Pattern version changed during build "
                    f"(expected {current_version}, got {registry_version}). "
                    f"Retrying... (attempt {attempt + 1}/{MAX_REBUILD_ATTEMPTS})"
                )
                # Continue to next iteration of the loop

        # If we get here, we failed after MAX_REBUILD_ATTEMPTS
        # Emergency fallback: Use the last pattern we built if we have no pattern at all
        # Better to have a slightly stale pattern than no masking at all
        with self._pattern_lock:
            if self._pattern is None and last_built_pattern is not None:
                self._pattern = last_built_pattern
                self._replacements = last_built_replacements
                self._last_version = last_built_version
                logger.warning(
                    f"Emergency fallback: Using potentially stale pattern (version {last_built_version}) "
                    f"because no pattern was previously available and registry is changing too rapidly."
                )
            else:
                logger.error(
                    f"CRITICAL: Failed to rebuild masking pattern after {MAX_REBUILD_ATTEMPTS} attempts. "
                    f"Secrets are being modified too rapidly. "
                    f"Continuing with potentially stale pattern (version {self._last_version}). "
                    f"Some newly added secrets may not be masked until rate of changes decreases."
                )
        # Keep using the old pattern rather than crashing - graceful degradation

    def mask_text(self, text: str) -> str:
        """Mask secrets in text string.

        Public API for masking arbitrary text content. Thread-safe and includes
        automatic pattern rebuilding, circuit breaker protection, and error handling.

        Args:
            text: Text content to mask

        Returns:
            Text with secrets replaced by ***REDACTED:VARIABLE_NAME***
        """
        if not isinstance(text, str) or not text:
            return text

        # Get pattern snapshot (no lock during masking!)
        with self._pattern_lock:
            self._check_and_rebuild_pattern()
            pattern = self._pattern
            replacements = self._replacements  # No .copy() needed!

        # Pattern might be None if no secrets registered
        if pattern is None:
            return text

        # Circuit breaker - if too many failures, stop trying
        if self._circuit_open:
            return CIRCUIT_OPEN_MESSAGE

        # Mask secrets (outside lock - safe because immutable references)
        try:
            # Use callback to include variable name in masked output
            def replace_with_variable_name(match):
                """Replace matched secret with variable name."""
                secret_value = match.group(0)
                # Look up variable name (O(1) dict access)
                variable_name = replacements.get(secret_value, "UNKNOWN")
                # Return formatted mask
                return REDACTED_FORMAT.format(name=variable_name)

            masked = pattern.sub(replace_with_variable_name, text)

            # Success - reset failure count
            if self._failure_count > 0:
                self._failure_count = 0

            return masked

        except KeyError as e:
            self._failure_count += 1
            logger.error(
                f"CRITICAL: Secret masking failed due to replacement error "
                f"(failure {self._failure_count}/{self._max_failures}). "
                f"Message redacted for safety. Error: {e}"
            )
            if self._failure_count >= self._max_failures:
                self._circuit_open = True
                logger.critical(
                    "CRITICAL: Masking circuit breaker OPEN. All messages will be redacted."
                )
            return "[REDACTED: Masking Replacement Error]"

        except re.error as e:
            self._failure_count += 1
            logger.error(
                f"CRITICAL: Secret masking failed due to regex error "
                f"(failure {self._failure_count}/{self._max_failures}). "
                f"Message redacted for safety. Error: {e}"
            )
            if self._failure_count >= self._max_failures:
                self._circuit_open = True
                logger.critical(
                    "CRITICAL: Masking circuit breaker OPEN. All messages will be redacted."
                )
            return "[REDACTED: Masking Regex Error]"

        except MemoryError:
            self._failure_count += 1
            logger.error(
                f"CRITICAL: Secret masking failed due to memory error "
                f"(failure {self._failure_count}/{self._max_failures}). "
                f"Message redacted for safety."
            )
            if self._failure_count >= self._max_failures:
                self._circuit_open = True
                logger.critical(
                    "CRITICAL: Masking circuit breaker OPEN. All messages will be redacted."
                )
            return "[REDACTED: Masking Memory Error]"

        except Exception as e:
            self._failure_count += 1
            logger.error(
                f"CRITICAL: Secret masking failed with unexpected error "
                f"(failure {self._failure_count}/{self._max_failures}). "
                f"Message redacted for safety. Error type: {type(e).__name__}"
            )
            if self._failure_count >= self._max_failures:
                self._circuit_open = True
                logger.critical(
                    "CRITICAL: Masking circuit breaker OPEN. All messages will be redacted."
                )
            return "[REDACTED: Masking Error]"

    def rebind_registry(self, registry: "SecretRegistry") -> None:
        """Rebind the registry this filter reads, and force a pattern
        rebuild on the next mask.

        Used by ``install_masking_filter``'s refresh path when the caller
        hands in a different registry on a repeat install. Encapsulating
        the rebind here keeps the invariant (rebind implies version reset)
        in one place where it can't drift from the install path.

        Uses ``_last_version = -1`` (not 0): a fresh registry's version is 0,
        so ``0 == 0`` would make ``_check_and_rebuild_pattern``'s fast path
        skip the rebuild and the old pattern would persist (over-masking the
        old registry's values). ``-1`` never equals a real version, so the
        next mask always rebuilds. The pattern and replacements are cleared
        too so a stale pattern can't be served before the rebuild runs.

        The whole body runs under ``_pattern_lock`` (an ``RLock``, so nesting
        is safe). Every other write to ``_registry`` / ``_last_version`` /
        ``_pattern`` / ``_replacements`` happens under that lock, and
        ``mask_text`` snapshots them together under it — an unlocked
        rebind could land between the snapshot of ``_pattern`` and
        ``_replacements`` and emit cleartext (or a ``***REDACTED:UNKNOWN***``
        mismatch from a cleared ``_replacements`` paired with an old
        ``_pattern``). Clearing under the lock also makes the four writes
        atomic relative to each other.
        """
        with self._pattern_lock:
            self._registry = registry
            self._last_version = -1
            self._pattern = None
            self._replacements = {}

    def _mask_args(self, args: Any) -> Any:
        """Mask secrets in log arguments."""
        if not args:
            return args

        try:
            if isinstance(args, dict):
                return {
                    k: self.mask_text(v) if isinstance(v, str) else v
                    for k, v in args.items()
                }
            elif isinstance(args, tuple):
                return tuple(
                    self.mask_text(arg) if isinstance(arg, str) else arg for arg in args
                )
            else:
                return args
        except Exception as e:
            # Fail-secure: never return unmasked args on error
            logger.error(f"CRITICAL: Secret masking failed in args: {e}", exc_info=True)
            return (MASKING_ERROR_MESSAGE,)

    def _mask_exception(self, exc_info: Optional[Tuple]) -> Optional[str]:
        """Materialize traceback text from ``exc_info`` so filter() can mask it.

        Returns the unmasked traceback text (or a sentinel on error). filter()
        assigns the result to ``record.exc_text`` and masks it alongside any
        pre-existing ``exc_text``. Formatters reuse ``exc_text`` when set, so
        the masked text is what gets emitted.

        ``exc_info`` itself is left untouched: handler-based error reporters
        (Sentry, Datadog, anything reading ``record.exc_info``) keep the real
        exception with its chain and traceback.

        Stated trade-off: the previous implementation rebuilt the exception via
        ``type(exc_value)(*masked_args)``, which attempted to mask exception
        args for downstream error-reporter consumers — badly (it raised for
        any exception whose ``__init__`` signature didn't match its ``args``,
        common in DB drivers and source connectors, and the fallback returned
        a bare ``RuntimeError`` that destroyed the traceback, message, class,
        and ``__cause__`` chain). This PR trades masked-args-for-error-reporters
        for correctness of the *printed* output. That is the right trade
        because the printed output is what ships to log files and is the
        primary leak surface; error reporters are a secondary surface and
        their consumers can be separately addressed (e.g. by masking at the
        reporter integration). It is a stated decision, not a silent
        consequence.

        A middle option (mutating ``exc_value.args`` in place) was considered
        and rejected: it mutates a live exception the caller may re-raise or
        inspect after logging.

        Benefit that comes free: ``Formatter.formatException`` follows
        ``__cause__`` / ``__context__``, so the masked text covers the whole
        chain, including chained exception messages that the old rebuild
        dropped.

        Residual gap: a formatter with a custom ``formatException`` that
        re-derives from ``exc_info`` (instead of reusing ``exc_text``) bypasses
        the masked text. Same accepted residual as the structured-formatter
        case for extras.
        """
        if not exc_info:
            return None

        try:
            return logging.Formatter().formatException(exc_info)
        except Exception as e:
            # Fail-secure: return a sentinel string rather than fabricating a
            # RuntimeError, so the original exc_info (and error reporters)
            # survive. filter() will mask this (no-op since it has no secrets).
            # Note: do NOT log with exc_info=True here — that would call
            # formatException again and re-raise under the same failure.
            logger.error(
                f"CRITICAL: Secret masking failed materializing exception: {e}"
            )
            return MASKING_ERROR_MESSAGE

    def _truncate_message(self, message: str) -> str:
        """Truncate large messages before masking."""
        if not isinstance(message, str):
            return message

        if len(message) <= self._max_message_size:
            return message

        # Truncate with informative suffix
        truncated_bytes = len(message) - self._max_message_size
        return (
            f"{message[: self._max_message_size]}\n"
            f"... [{truncated_bytes} bytes truncated for performance]"
        )

    # Depth cap and cycle guard for _mask_value_recursive. extra= can carry
    # self-referential or very large structures onto the logging hot path; an
    # unbounded recursion would hang the logger. The cap is generous (real
    # config is rarely >10 deep); beyond it we return the value unchanged
    # rather than raise, so logging still proceeds (residual gap documented).
    _MAX_EXTRA_DEPTH = 10

    def _mask_value_recursive(
        self, value: Any, _depth: int = 0, _seen: Optional[set] = None
    ) -> Any:
        """Recursively mask secrets in a value's string leaves.

        Covers dict/list/tuple/set containers and string leaves. Non-string,
        non-container values pass through unchanged. Arbitrary objects are NOT
        stringified-and-masked (that would change behavior and risk calling
        ``__str__`` side effects); if a formatter later serializes such an
        object via ``%s``, the serialized form is masked at the formatter-output
        stage only if that output flows through the stdout/stderr wrapper —
        otherwise it is a residual gap, documented in install_masking_filter.

        Containers are *copied*, not mutated in place: ``record.__dict__["cfg"]``
        is the caller's live dict, so masking leaves in place would silently
        turn a config dict into ***REDACTED:X*** after the first log line — a
        nasty bug to chase. We build a masked copy and assign that to the record.

        A depth cap (``_MAX_EXTRA_DEPTH``) and an identity-based cycle guard
        (``_seen``) bound the recursion: extra= can carry self-referential or
        very large structures onto the hot path. Beyond the cap or on a cycle
        we return a placeholder string (``"<not masked: ...>"``) rather
        than the raw value — returning the raw subtree would emit any secret
        it contains in cleartext, and everything else in this module fails
        closed. The placeholder also keeps this docstring honest: "return
        the value unchanged" reads as "we mask less here" when it actually
        means "we emit the secret."
        """
        if _depth >= self._MAX_EXTRA_DEPTH:
            return "<not masked: depth limit>"
        if isinstance(value, str):
            return self.mask_text(value)
        if isinstance(value, dict):
            if _seen is None:
                _seen = set()
            if id(value) in _seen:
                return "<not masked: cycle>"
            _seen.add(id(value))
            try:
                return {
                    k: self._mask_value_recursive(v, _depth + 1, _seen)
                    for k, v in value.items()
                }
            finally:
                _seen.discard(id(value))
        if isinstance(value, (list, tuple, set, frozenset)):
            if _seen is None:
                _seen = set()
            if id(value) in _seen:
                return "<not masked: cycle>"
            _seen.add(id(value))
            try:
                masked = [
                    self._mask_value_recursive(v, _depth + 1, _seen) for v in value
                ]
            finally:
                _seen.discard(id(value))
            if isinstance(value, tuple):
                return tuple(masked)
            if isinstance(value, set):
                return set(masked)
            if isinstance(value, frozenset):
                return frozenset(masked)
            return masked
        return value

    def _mask_extras(self, record: logging.LogRecord) -> None:
        """Mask secrets in non-standard LogRecord attributes (``extra={...}``).

        Formatters pull fields from ``record.__dict__``; without this, a
        ``Formatter("%(dsn)s")`` with ``extra={"dsn": "db://u:sekret@h"}`` would
        emit the secret unmasked. Recurses into dict/list/tuple/set so nested
        containers (``extra={"cfg": {"password": "sekret"}}``) are covered.

        Standard LogRecord attributes are skipped: masking ``pathname`` /
        ``funcName`` / ``module`` would waste regex on the hot path and risks
        corrupting framework-set fields. Third-party libraries (structlog,
        gunicorn, ddtrace) inject their own attributes; this loop must not
        raise on unexpected types — it runs inside filter()'s try/except, and
        _mask_value_recursive returns non-container values unchanged.
        """
        for key, value in list(record.__dict__.items()):
            if key in _STANDARD_RECORD_ATTRS:
                continue
            if isinstance(value, (str, dict, list, tuple, set, frozenset)):
                record.__dict__[key] = self._mask_value_recursive(value)

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter and mask a log record.

        Idempotent per record: a sentinel (``record._datahub_masked``) is set
        after masking so the same record flowing through multiple handlers
        (each carrying this filter) is not re-truncated / re-masked. Without
        this, N handlers produce N× regex cost and N× truncation — the second
        truncation re-truncates the already-truncated text, eats the previous
        suffix, and reports a wrong byte count.
        """
        # Re-entrancy / idempotency guard. Records from the masking framework's
        # own loggers bypass masking entirely (their handlers are skipped at
        # install time, but they may propagate to root handlers after
        # reset_masking_safe_loggers flips propagate=True on teardown — see
        # bootstrap.shutdown_secret_masking). The startswith check is on the
        # record name, not the logger we attached to, so it catches propagation.
        if record.name.startswith(REDACTED_MASKING_NAMESPACE):
            return True

        if getattr(record, "_datahub_masked", None) is _MASKED:
            return True

        # Check if masking is disabled for debugging
        from datahub.masking.secret_registry import is_masking_enabled

        if not is_masking_enabled():
            return True  # Skip all masking and truncation for debugging

        try:
            # 1. Truncate large messages BEFORE masking (performance optimization)
            #    This is intentional: truncating first avoids regex on huge strings
            #    Security: Truncation removes end of message, so secrets at end
            #    are removed entirely (not just masked), which is acceptable
            if isinstance(record.msg, str):
                record.msg = self._truncate_message(record.msg)

            # 2. Mask the log message (after truncation for performance)
            if isinstance(record.msg, str):
                record.msg = self.mask_text(record.msg)

            # 3. Mask arguments (for formatting)
            if record.args:
                record.args = self._mask_args(record.args)

            # 4. Mask pre-formatted message if it exists
            if hasattr(record, "message") and record.message:
                record.message = self.mask_text(record.message)

            # 5. Materialize traceback text from exc_info so it can be masked.
            #    exc_info itself is left untouched (error reporters read it).
            #    See _mask_exception for why we do not rebuild the exception.
            if record.exc_info and not record.exc_text:
                record.exc_text = self._mask_exception(record.exc_info)

            # 6. Mask formatted exception text (pre-existing or just materialized)
            if record.exc_text:
                record.exc_text = self.mask_text(record.exc_text)

            # 7. Mask stack_info if present (Python 3.2+)
            if hasattr(record, "stack_info") and record.stack_info:
                record.stack_info = self.mask_text(record.stack_info)

            # 8. Mask non-standard attributes (extra={...}). Must run after msg/args
            #    masking and inside the try so unexpected types from third-party
            #    libraries do not break logging.
            self._mask_extras(record)

            # Mark as masked so subsequent handlers skip the work.
            record._datahub_masked = _MASKED

        except Exception as e:
            # NEVER let masking break logging
            try:
                sys.stderr.write(f"WARNING: Secret masking filter failed: {e}\n")
                sys.stderr.flush()
            except Exception:
                pass  # Even error reporting failed, continue silently

        return True  # Always let record through


class StreamMaskingWrapper:
    """Lightweight wrapper for stdout/stderr that masks secrets."""

    def __init__(self, original_stream: TextIO, masking_filter: SecretMaskingFilter):
        """Initialize stream wrapper."""
        self._original = original_stream
        self._filter = masking_filter

    def write(self, text: str) -> int:
        """Write text to stream with secrets masked."""
        # Type validation - text streams require strings
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")

        try:
            # Mask text (filter handles locking internally)
            masked = self._filter.mask_text(text)

            # Write WITHOUT holding any locks (prevents deadlock)
            self._original.write(masked)

            # Return length of MASKED text (contract compliance)
            return len(masked)

        except TypeError:
            # Re-raise type errors
            raise

        except Exception:
            # Graceful degradation for masking failures
            try:
                self._original.write(text)
                return len(text)
            except Exception:
                return 0

    def flush(self):
        """Flush the underlying stream."""
        try:
            if hasattr(self._original, "flush"):
                self._original.flush()
        except Exception:
            pass

    def __getattr__(self, name):
        """Delegate all other attributes to original stream."""
        return getattr(self._original, name)


def _iter_all_loggers() -> list[logging.Logger]:
    """Root logger plus every initialized named logger (skipping PlaceHolders).

    Returns a list (snapshot) so callers iterate a stable copy while other
    threads add/remove loggers. We deliberately do NOT take logging._acquireLock
    here: it is held during logging I/O on some paths and would risk deadlock,
    and list(loggerDict.keys()) + .get() is safe enough against concurrent
    mutation (a removed logger yields None from .get and is skipped).
    """
    loggers: list[logging.Logger] = [logging.getLogger()]
    # .get() per snapshotted key: a logger may be removed between snapshot and access.
    for name in list(logging.root.manager.loggerDict.keys()):
        obj = logging.root.manager.loggerDict.get(name)
        if isinstance(obj, logging.Logger):
            loggers.append(obj)
    return loggers


def _add_filter_to_existing_handlers(masking_filter: SecretMaskingFilter) -> None:
    """Attach the masking filter to all existing handlers.

    Masking lives on handlers, not the logger: Python skips logger-level filters
    for records propagated from child loggers, so a root-logger filter would miss
    almost everything. A handler filter sees every record reaching that output and
    masks it in place, without touching the handler's stream. (Repointing streams
    instead loops forever under celery -- see install_masking_filter.)

    Skip datahub.masking.* loggers: they log to the original stderr by design and
    carry no secrets, so filtering them only risks re-entrancy. The trailing dot
    avoids matching a hypothetical "datahub.maskingfoo" logger.
    """
    added = 0
    for log in _iter_all_loggers():
        if log.name.startswith(REDACTED_MASKING_NAMESPACE):
            continue
        # Copy: handlers may be added/removed by other threads during iteration.
        for handler in list(log.handlers):
            # Identity check: don't skip attaching because some OTHER
            # SecretMaskingFilter is present — that would let us miss handlers
            # we should cover, and on teardown we'd strip a filter we never added.
            # We only skip our own instance (re-install / refresh path).
            if masking_filter in handler.filters:
                continue
            handler.addFilter(masking_filter)
            added += 1
    if added:
        logger.debug(f"Installed SecretMaskingFilter on {added} handler(s)")


def _remove_filter_from_existing_handlers(masking_filter: SecretMaskingFilter) -> None:
    """Remove *our* masking filter instance from every handler it's on.

    Identity-based (not isinstance) so we never strip a different
    SecretMaskingFilter that someone else installed. Uses removeFilter (the
    logging API) rather than reassigning handler.filters.
    """
    for log in _iter_all_loggers():
        for handler in list(log.handlers):
            while masking_filter in handler.filters:
                handler.removeFilter(masking_filter)


def _is_real_stream(stream: object) -> bool:
    """True if ``stream`` is a real OS-backed stream safe to wrap.

    Fail-closed: if ``fileno()`` raises (pytest capture, celery LoggingProxy,
    structlog, IPython, journald), we treat it as a non-real stream and skip
    wrapping. Skipping costs nothing under a proxy: raw print()/stderr writes
    are converted to log records by the proxy and flow through handlers that
    carry the masking filter. Wrapping a proxy would re-enter logging and
    recurse.
    """
    try:
        stream.fileno()  # type: ignore[attr-defined]
        return True
    except (io.UnsupportedOperation, OSError, ValueError, AttributeError):
        return False


def install_masking_filter(
    secret_registry: Optional[SecretRegistry] = None,
    max_message_size: int = 5000,
    install_stdout_wrapper: bool = True,
) -> SecretMaskingFilter:
    """Enable secret masking: install the filter on existing handlers (+ root
    logger) and, optionally, wrap sys.stdout/stderr for raw writes.

    Masking happens at the handler level (see _add_filter_to_existing_handlers).
    Coverage is a snapshot of the handlers present now, so call this AFTER logging
    is configured; handlers added later are covered only by a re-install or, for
    stdout/stderr, by the stream wrapper.

    Fail-open limitations (residual gaps, documented so nobody over-engineers a
    addHandler monkeypatch later):
    - A handler added to a child logger after install can emit unmasked (a
      logger's own handlers run before ancestors'). Not a concern in the
      executor, where handlers exist before masking is installed per task.
    - A FileHandler added after install in a non-celery process: its writes
      don't go through the stdout/stderr wrapper, so they're unmasked. Under
      celery, late handlers' writes go through the proxy back into logging and
      are masked there; outside celery, late StreamHandler()s inherit the
      wrapped stderr. The residual gap is FileHandler-after-install in a
      non-celery process.
    - Arbitrary objects in extras are not stringified-and-masked at filter
      time (would risk __str__ side effects); a formatter that serializes them
      via %s may emit them unmasked unless the output flows through the
      stdout/stderr wrapper.

    Note: the "already installed → refresh" path re-scans handlers, re-adds
    the root-logger filter if something removed it, and rebinds the
    registry if the caller passed a different one (with a warning).
    ``max_message_size`` on a repeat call is still ignored (the filter
    instance is reused); call ``SecretRegistry.reset_instance()`` first
    for a full re-install with new args.
    """
    global _installed_filter

    root_logger = logging.getLogger()

    # Identity-based "already installed?" check. The previous isinstance check
    # could skip attaching because someone else's SecretMaskingFilter was
    # present, then strip that filter on teardown. Track our own instance.
    if _installed_filter is not None:
        # Already installed: re-scan to cover handlers added since (fail-open).
        _add_filter_to_existing_handlers(_installed_filter)

        # Re-add the root-logger filter if something removed it (partial
        # teardown state). Without this, a partially-torn-down state stays
        # partial: the handler filters are re-scanned but the root-logger
        # sentinel is gone, so the "already installed?" check on a later call
        # would re-install from scratch and attach a second filter.
        if _installed_filter not in root_logger.filters:
            root_logger.addFilter(_installed_filter)

        # Rebind the registry if the caller passed a different one. Without
        # this, install(r1) → reset_instance() → install(r2) leaves the
        # filter masking with r1 (now stale), so r2's secrets leak. The
        # filter is process-lifetime, but the registry it reads is not —
        # rebind so the same filter instance reads the new registry, and
        # reset _last_version to force a pattern rebuild on the next mask.
        if (
            secret_registry is not None
            and secret_registry is not _installed_filter._registry
        ):
            logger.warning(
                "Rebinding SecretMaskingFilter registry on repeat install; "
                "the previous registry is no longer active. "
                "Use SecretRegistry.reset_instance() between installs to "
                "fully tear down first."
            )
            _installed_filter.rebind_registry(secret_registry)

        # Honour install_stdout_wrapper on refresh. The wrapper block sits after
        # this path's early return, so a repeat install with
        # install_stdout_wrapper=True after an earlier install with False would
        # silently skip wrapping. The helper is idempotent and guarded by the
        # fileno() real-stream check, so calling it here is safe. We do NOT
        # unwrap when False is passed — teardown owns unwrapping.
        if install_stdout_wrapper:
            _wrap_std_streams(_installed_filter)

        logger.debug("SecretMaskingFilter already installed; refreshed handlers")
        return _installed_filter

    masking_filter = SecretMaskingFilter(
        secret_registry=secret_registry, max_message_size=max_message_size
    )

    # The root-logger filter is just the "already installed?" sentinel (and masks
    # records logged directly on root). The real masking is the handler filters
    # below, since logger-level filters don't see propagated child-logger records.
    root_logger.addFilter(masking_filter)
    _add_filter_to_existing_handlers(masking_filter)
    _installed_filter = masking_filter
    logger.info("Installed SecretMaskingFilter on root logger and existing handlers")

    # Wrap stdout/stderr only to mask raw writes (print(), C-extension output).
    # We do NOT repoint handler streams here: under celery, sys.stderr re-enters
    # logging, so a handler pointed at it would recurse infinitely and drop output.
    # Skip wrapping when the stream is not a real OS-backed stream (proxy / pytest
    # capture / structlog / etc.) — wrapping a proxy re-enters logging. Under a
    # proxy, raw writes become log records and flow through handlers that carry
    # the masking filter, so coverage doesn't suffer.
    if install_stdout_wrapper:
        _wrap_std_streams(masking_filter)

    return masking_filter


def _wrap_std_streams(masking_filter: "SecretMaskingFilter") -> None:
    """Wrap sys.stdout/stderr with StreamMaskingWrapper if they are real
    OS-backed streams and not already wrapped. Idempotent and guarded by the
    ``fileno()`` real-stream check, so it is safe to call on the refresh path
    (a repeat install with ``install_stdout_wrapper=True`` after an earlier
    install with ``False``). Does NOT unwrap when ``install_stdout_wrapper``
    is False — teardown owns unwrapping."""
    if not isinstance(sys.stdout, StreamMaskingWrapper) and _is_real_stream(sys.stdout):
        sys.stdout = StreamMaskingWrapper(sys.stdout, masking_filter)
        logger.debug("Wrapped sys.stdout with StreamMaskingWrapper")
    else:
        logger.debug(
            "Skipped wrapping sys.stdout (not a real stream / already wrapped)"
        )

    if not isinstance(sys.stderr, StreamMaskingWrapper) and _is_real_stream(sys.stderr):
        sys.stderr = StreamMaskingWrapper(sys.stderr, masking_filter)
        logger.debug("Wrapped sys.stderr with StreamMaskingWrapper")
    else:
        logger.debug(
            "Skipped wrapping sys.stderr (not a real stream / already wrapped)"
        )


def uninstall_masking_filter() -> None:
    """Remove secret masking filter from root logger and all handlers."""
    global _installed_filter

    root_logger = logging.getLogger()

    # Remove our instance (identity-based) from the root logger and handlers.
    if _installed_filter is not None:
        while _installed_filter in root_logger.filters:
            root_logger.removeFilter(_installed_filter)
        _remove_filter_from_existing_handlers(_installed_filter)
        _installed_filter = None

    # Unwrap stdout/stderr (only if we wrapped them)
    if isinstance(sys.stdout, StreamMaskingWrapper):
        sys.stdout = sys.stdout._original

    if isinstance(sys.stderr, StreamMaskingWrapper):
        sys.stderr = sys.stderr._original

    logger.info("Uninstalled SecretMaskingFilter")
