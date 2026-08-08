"""Logging filter that masks registered secrets in log records and streams."""

import io
import logging
import re
import sys
import threading
import uuid
from typing import Any, Dict, List, Optional, TextIO, Tuple

from datahub.masking.constants import REDACTED_FORMAT
from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)

REDACTED_MASKING_NAMESPACE = "datahub.masking."
MASKING_ERROR_MESSAGE = "[MASKING_ERROR - OUTPUT_SUPPRESSED_FOR_SECURITY]"

# Derive the marker prefix/suffix from REDACTED_FORMAT so the generic
# fallback regex tracks the constant, not a hardcoded copy.
_MARKER_PREFIX = REDACTED_FORMAT.split("{", 1)[0]
_MARKER_SUFFIX = REDACTED_FORMAT.rsplit("}", 1)[1]
_GENERIC_MARKER_RE = (
    re.escape(_MARKER_PREFIX) + r"[^*]{0,256}?" + re.escape(_MARKER_SUFFIX)
)

# Idempotency token stamped onto records after masking. A random string
# (not object()) so structured formatters that serialize record.__dict__ emit
# a valid line; compared by value so the guard survives cross-thread
# QueueHandler/QueueListener within a process. Randomness prevents forging
# extra={'_datahub_masked': ...} to disable masking for a record.
_MASKED = f"_datahub_masked_{uuid.uuid4().hex}"

# LogRecord standard attributes. Extras (extra={...}) are everything not in
# this set; we mask those recursively. _datahub_masked is included so the
# extras loop skips the idempotency token.
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
        "_datahub_masked",
    }
)

# The single installed filter instance. The addHandler wrap reads this; when
# None (disarmed) the wrap is inert.
_installed_filter: Optional["SecretMaskingFilter"] = None

# Whether the addHandler wrap has been installed. Installed once at first
# install_masking_filter call and never removed.
_addhandler_wrap_installed = False
_original_add_handler = logging.Logger.addHandler


def _is_masking_namespace_name(name: str) -> bool:
    """True for loggers in the masking framework's own namespace."""
    return name == "datahub.masking" or name.startswith(REDACTED_MASKING_NAMESPACE)


def _patched_add_handler(self: logging.Logger, hdlr: logging.Handler) -> None:
    # Call the original first so the handler is attached even if masking
    # is not armed; then attach the shared filter if it is.
    _original_add_handler(self, hdlr)
    f = _installed_filter
    if f is not None and f not in hdlr.filters:
        hdlr.addFilter(f)


def _install_addhandler_wrap() -> None:
    """Install the Logger.addHandler wrap once. Idempotent."""
    global _addhandler_wrap_installed
    if _addhandler_wrap_installed:
        return
    logging.Logger.addHandler = _patched_add_handler  # type: ignore[assignment]
    _addhandler_wrap_installed = True


class SecretMaskingFilter(logging.Filter):
    """Logging filter that masks secrets in log records."""

    def __init__(
        self,
        secret_registry: Optional[SecretRegistry] = None,
        max_message_size: int = 5000,
    ) -> None:
        super().__init__()
        self._registry = secret_registry or SecretRegistry.get_instance()
        self._max_message_size = max_message_size
        self._pattern_lock = threading.Lock()
        self._pattern: Optional[re.Pattern] = None
        self._replacements: Dict[str, str] = {}
        self._last_version: int = 0
        # max(longest expanded value, longest exact marker). The marker term
        # stays even after markers are deduped per name; dropping it would
        # narrow the pre-cut and let a cut land inside a marker.
        self._longest_pattern_length: int = 0
        self._rebuild_warned = False
        self._build_failure_warned = False

    # --- Pattern cache ----------------------------------------------------

    def _build_pattern(
        self, expanded: Dict[str, str]
    ) -> Tuple[re.Pattern, Dict[str, str], int]:
        """Compile the masking pattern from expanded keys -> names.

        Ordering is load-bearing: marker alternatives first, then values
        longest-first, then the generic marker fallback. With first-match
        alternation, a short secret that is a prefix of a longer one would
        win at the same position if listed first, leaking the longer secret's
        tail. Markers first makes mask_text idempotent on already-masked text
        (the marker alternative wins and passes through untouched). The
        generic fallback protects dropped-execution markers whose names are
        gone from the registry.
        """
        # Per-name markers (deduplicated): expanded keys share a name, so
        # per-key markers would multiply the alternation ~4x for nothing.
        names = list(dict.fromkeys(expanded.values()))
        marker_alts = [
            f"(?P<m{i}>{re.escape(REDACTED_FORMAT.format(name=n))})"
            for i, n in enumerate(names)
        ]
        # Value alternatives longest-first; re.escape ensures metacharacters
        # are matched literally.
        sorted_items = sorted(expanded.items(), key=lambda x: len(x[0]), reverse=True)
        value_alts = [
            f"(?P<v{i}>{re.escape(v)})" for i, (v, _) in enumerate(sorted_items)
        ]
        generic = [rf"(?P<g>{_GENERIC_MARKER_RE})"]
        pattern_str = "|".join(marker_alts + value_alts + generic)
        new_pattern = re.compile(pattern_str)
        new_replacements = {v: n for v, n in sorted_items}
        longest_value = len(sorted_items[0][0]) if sorted_items else 0
        longest_marker = max(
            (len(REDACTED_FORMAT.format(name=n)) for n in names),
            default=0,
        )
        new_longest = max(longest_value, longest_marker)
        return new_pattern, new_replacements, new_longest

    def _replace_match(self, match: re.Match, replacements: Dict[str, str]) -> str:
        # Dispatch on which alternative won (lastgroup), not the matched
        # text: a marker-shaped value is the same string as its marker, so
        # text-based dispatch would oscillate.
        last = match.lastgroup
        if last is not None and not last.startswith("v"):
            return match.group(0)
        return REDACTED_FORMAT.format(name=replacements.get(match.group(0), "UNKNOWN"))

    def _get_pattern(
        self,
    ) -> Tuple[Optional[re.Pattern], Dict[str, str], int, bool]:
        """Return (pattern, replacements, longest, ok).

        ``ok`` is False only when secrets exist but no pattern was ever built
        and the latest rebuild attempt failed — the caller must return
        MASKING_ERROR_MESSAGE in that case. When the registry is empty,
        ``pattern`` is None and ``ok`` is True (return text unchanged).
        """
        version = self._registry.get_version()
        with self._pattern_lock:
            if version == self._last_version:
                return (
                    self._pattern,
                    self._replacements,
                    self._longest_pattern_length,
                    True,
                )
        # Stale: snapshot and rebuild outside the lock.
        snap_version, expanded = self._registry.snapshot()
        if not expanded:
            with self._pattern_lock:
                self._pattern = None
                self._replacements = {}
                self._longest_pattern_length = 0
                self._last_version = snap_version
                self._rebuild_warned = False
                self._build_failure_warned = False
            return None, {}, 0, True
        try:
            new_pattern, new_replacements, new_longest = self._build_pattern(expanded)
        except Exception as e:
            # Keep previous pattern if we have one; log once.
            with self._pattern_lock:
                if self._pattern is not None:
                    if not self._rebuild_warned:
                        logger.warning(
                            "Masking pattern rebuild failed; keeping previous "
                            "pattern: %r",
                            e,
                        )
                        self._rebuild_warned = True
                    return (
                        self._pattern,
                        self._replacements,
                        self._longest_pattern_length,
                        True,
                    )
                # No previous pattern while secrets exist: fail closed.
                if not self._build_failure_warned:
                    logger.critical(
                        "Masking pattern rebuild failed with no previous "
                        "pattern while secrets exist; returning "
                        "MASKING_ERROR_MESSAGE: %r",
                        e,
                    )
                    self._build_failure_warned = True
                return None, {}, 0, False
        with self._pattern_lock:
            self._pattern = new_pattern
            self._replacements = new_replacements
            self._longest_pattern_length = new_longest
            self._last_version = snap_version
            self._rebuild_warned = False
            self._build_failure_warned = False
        return new_pattern, new_replacements, new_longest, True

    # --- Masking ----------------------------------------------------------

    def mask_text(self, text: str) -> str:
        """Mask secrets in ``text``. Idempotent on already-masked text.

        Returns the text unchanged when the registry is empty; returns
        MASKING_ERROR_MESSAGE when secrets exist but no pattern was ever
        built and the rebuild failed. Public API — never truncates.
        """
        if not isinstance(text, str) or not text:
            return text
        pattern, replacements, longest, ok = self._get_pattern()
        if not ok:
            return MASKING_ERROR_MESSAGE
        if pattern is None:
            return text
        try:
            return pattern.sub(lambda m: self._replace_match(m, replacements), text)
        except Exception:
            return MASKING_ERROR_MESSAGE

    _MAX_EXTRA_DEPTH = 10

    def _mask_value_recursive(
        self, value: Any, _depth: int = 0, _seen: Optional[set] = None
    ) -> Any:
        """Recursively mask string leaves in a container value.

        Containers are copied, not mutated in place. A depth cap and cycle
        guard bound the recursion; beyond them a placeholder is returned
        (never the raw subtree). Containers rebuild with their own type; if
        reconstruction raises, the container becomes MASKING_ERROR_MESSAGE.
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
            try:
                if hasattr(value, "_fields"):
                    return type(value)(*masked)
                return type(value)(masked)
            except Exception:
                return MASKING_ERROR_MESSAGE
        return value

    def _mask_args(self, args: Any) -> Any:
        if not args:
            return args
        return self._mask_value_recursive(args)

    def _materialize_exc_text(self, exc_info: Tuple) -> Optional[str]:
        # exc_info is left untouched so handler-based error reporters keep
        # the real exception; we only materialize text for masking.
        if not exc_info:
            return None
        return logging.Formatter().formatException(exc_info)

    def _strip_severed_tail(self, kept: str, keys: Any, longest: int) -> str:
        # Runs on MASKED text: every complete occurrence is already a marker,
        # so a tail matching a key prefix is a severed fragment. Remove the
        # longest such suffix (len >= 3, < key) that is a proper prefix of any
        # expanded key.
        for L in range(min(longest, len(kept)), 2, -1):
            for key in keys:
                if len(key) > L and kept.endswith(key[:L]):
                    return kept[:-L]
        return kept

    def _mask_bounded(
        self, text: str, budget: int, original_len: Optional[int] = None
    ) -> str:
        # Pre-cut to budget + longest under one snapshot, mask, then strip any
        # severed fragment from the masked tail, then final-cut to budget.
        # Stripping after masking is load-bearing: a secret prefix with a
        # self-overlapping border can match a coincidental overlap longer than
        # the real fragment before masking, severing a complete occurrence.
        pattern, replacements, longest, ok = self._get_pattern()
        if not ok:
            return MASKING_ERROR_MESSAGE
        pre_cut = False
        if len(text) > budget + longest:
            kept = text[: budget + longest]
            pre_cut = True
        else:
            kept = text
        if pattern is None:
            masked = kept
        else:
            try:
                masked = pattern.sub(
                    lambda m: self._replace_match(m, replacements), kept
                )
            except Exception:
                return MASKING_ERROR_MESSAGE
        if pre_cut and pattern is not None:
            masked = self._strip_severed_tail(masked, replacements.keys(), longest)
        if pre_cut or len(masked) > budget:
            base = original_len if original_len is not None else len(text)
            truncated_bytes = max(0, base - budget)
            masked = (
                f"{masked[:budget]}\n"
                f"... [{truncated_bytes} bytes truncated for performance]"
            )
        return masked

    def _mask_record_msg(self, record: logging.LogRecord) -> None:
        if isinstance(record.msg, str):
            if record.args:
                # Format string: mask but skip truncation — truncating a
                # format string can sever a %s placeholder.
                record.msg = self.mask_text(record.msg)
            else:
                original_len = len(record.msg)
                record.msg = self._mask_bounded(
                    record.msg, self._max_message_size, original_len
                )
        elif isinstance(record.msg, (dict, list, tuple, set, frozenset)):
            record.msg = self._mask_value_recursive(record.msg)
        elif record.msg is not None:
            plain = str(record.msg)
            masked = self.mask_text(plain)
            if masked != plain:
                record.msg = masked

    def _mask_extras(self, record: logging.LogRecord) -> None:
        """Mask non-standard record attributes (extra={...}) per field.

        One field that raises becomes MASKING_ERROR_MESSAGE while the rest
        are still masked. The caller's containers are not mutated in place —
        masked copies are assigned.
        """
        for key, value in list(record.__dict__.items()):
            if key in _STANDARD_RECORD_ATTRS:
                continue
            if isinstance(value, (str, dict, list, tuple, set, frozenset)):
                try:
                    record.__dict__[key] = self._mask_value_recursive(value)
                except Exception:
                    record.__dict__[key] = MASKING_ERROR_MESSAGE

    def filter(self, record: logging.LogRecord) -> bool:
        """Mask secrets in a log record. Always returns True.

        Fail-closed per field: each step runs in its own try; a field that
        raises is substituted with MASKING_ERROR_MESSAGE and the next step
        still runs. The idempotency sentinel is set in finally so it guards
        the failure path. An outer boundary catches anything that escapes the
        per-step tries and substitutes MASKING_ERROR_MESSAGE for the whole
        record.
        """
        if _is_masking_namespace_name(record.name):
            return True
        if getattr(record, "_datahub_masked", None) == _MASKED:
            return True
        try:
            try:
                self._mask_record_msg(record)
            except Exception:
                record.msg = MASKING_ERROR_MESSAGE
            try:
                if record.args:
                    record.args = self._mask_args(record.args)
            except Exception:
                record.args = (MASKING_ERROR_MESSAGE,)
            try:
                if hasattr(record, "message") and record.message:
                    record.message = self.mask_text(record.message)
            except Exception:
                record.message = MASKING_ERROR_MESSAGE
            try:
                if record.exc_info and not record.exc_text:
                    record.exc_text = self._materialize_exc_text(record.exc_info)
            except Exception:
                record.exc_text = MASKING_ERROR_MESSAGE
            try:
                if record.exc_text:
                    record.exc_text = self._mask_bounded(
                        record.exc_text,
                        2 * self._max_message_size,
                        len(record.exc_text),
                    )
            except Exception:
                record.exc_text = MASKING_ERROR_MESSAGE
            try:
                if hasattr(record, "stack_info") and record.stack_info:
                    record.stack_info = self.mask_text(record.stack_info)
            except Exception:
                record.stack_info = MASKING_ERROR_MESSAGE
            try:
                self._mask_extras(record)
            except Exception:
                # _mask_extras already masks per-field; reaching here means
                # the whole loop raised. Substitute a marker key to fail
                # closed without leaving extras unmasked.
                record.__dict__["_masking_error"] = MASKING_ERROR_MESSAGE
        except Exception:
            # Outer boundary: never let masking break logging. Clearing
            # exc_info here is deliberate — on the failure path nothing
            # guarantees it was masked.
            record.msg = MASKING_ERROR_MESSAGE
            record.args = ()
            record.exc_info = None
            record.exc_text = None
        finally:
            record._datahub_masked = _MASKED
        return True


class StreamMaskingWrapper:
    """Wraps stdout/stderr to mask secrets in raw writes."""

    def __init__(
        self, original_stream: TextIO, masking_filter: "SecretMaskingFilter"
    ) -> None:
        self._original = original_stream
        self._filter = masking_filter

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")
        try:
            masked = self._filter.mask_text(text)
            self._original.write(masked)
            return len(masked)
        except Exception:
            try:
                self._original.write(MASKING_ERROR_MESSAGE + "\n")
                return len(MASKING_ERROR_MESSAGE) + 1
            except Exception:
                return 0

    def flush(self) -> None:
        try:
            if hasattr(self._original, "flush"):
                self._original.flush()
        except (ValueError, OSError):
            pass

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._original, name)


def _is_real_stream(stream: object) -> bool:
    # Refuse to wrap proxies (e.g. Celery's LoggingProxy) — wrapping one
    # re-enters logging and silently drops writes; under a proxy, raw
    # writes already flow through masked handlers.
    try:
        stream.fileno()  # type: ignore[attr-defined]
        return True
    except (io.UnsupportedOperation, OSError, ValueError, AttributeError):
        return False


def _all_logger_handler_pairs() -> List[Tuple[logging.Logger, logging.Handler]]:
    """Snapshot (logger, handler) pairs for the root logger and every
    named logger. PlaceHolder entries in loggerDict are skipped."""
    root = logging.getLogger()
    pairs: List[Tuple[logging.Logger, logging.Handler]] = [
        (root, h) for h in list(root.handlers)
    ]
    for _name, obj in list(logging.root.manager.loggerDict.items()):
        if isinstance(obj, logging.Logger):
            for h in list(obj.handlers):
                pairs.append((obj, h))
    return pairs


def _attach_filter_to_handlers(masking_filter: "SecretMaskingFilter") -> None:
    """Attach the shared filter to every handler on every logger, skipping
    handlers in the masking namespace and handlers that already carry this
    filter instance (identity check, not isinstance)."""
    added = 0
    for log, handler in _all_logger_handler_pairs():
        if _is_masking_namespace_name(log.name):
            continue
        if masking_filter in handler.filters:
            continue
        handler.addFilter(masking_filter)
        added += 1
    if added:
        logger.debug("Attached SecretMaskingFilter to %d handler(s)", added)


def _wrap_std_streams(masking_filter: "SecretMaskingFilter") -> None:
    """Wrap sys.stdout/stderr with StreamMaskingWrapper if they are real
    OS-backed streams and not already wrapped. Idempotent."""
    if not isinstance(sys.stdout, StreamMaskingWrapper) and _is_real_stream(sys.stdout):
        sys.stdout = StreamMaskingWrapper(sys.stdout, masking_filter)
        logger.debug("Wrapped sys.stdout with StreamMaskingWrapper")
    if not isinstance(sys.stderr, StreamMaskingWrapper) and _is_real_stream(sys.stderr):
        sys.stderr = StreamMaskingWrapper(sys.stderr, masking_filter)
        logger.debug("Wrapped sys.stderr with StreamMaskingWrapper")


def install_masking_filter(
    secret_registry: Optional[SecretRegistry] = None,
    max_message_size: int = 5000,
    install_stdout_wrapper: bool = True,
) -> SecretMaskingFilter:
    # Install the shared filter on all logger handlers, install the
    # addHandler wrap so later handlers are covered, and optionally wrap
    # stdout/stderr. Re-invocation re-scans handlers (closes the interval
    # where the wrap was inert); max_message_size is ignored on re-invocation
    # (first install wins); install_stdout_wrapper=True on refresh does wrap.
    global _installed_filter
    if _installed_filter is not None:
        # Already installed: re-scan handlers, rebind registry if a different
        # one was passed, honour install_stdout_wrapper on refresh.
        _attach_filter_to_handlers(_installed_filter)
        if (
            secret_registry is not None
            and secret_registry is not _installed_filter._registry
        ):
            logger.debug("Rebinding SecretMaskingFilter registry on repeat install")
            _installed_filter._registry = secret_registry  # type: ignore[attr-defined]
            with _installed_filter._pattern_lock:  # type: ignore[attr-defined]
                _installed_filter._last_version = -1  # type: ignore[attr-defined]
                _installed_filter._pattern = None  # type: ignore[attr-defined]
                _installed_filter._replacements = {}  # type: ignore[attr-defined]
        if install_stdout_wrapper:
            _wrap_std_streams(_installed_filter)
        return _installed_filter

    masking_filter = SecretMaskingFilter(
        secret_registry=secret_registry, max_message_size=max_message_size
    )
    _installed_filter = masking_filter
    _install_addhandler_wrap()
    _attach_filter_to_handlers(masking_filter)
    logger.info("Installed SecretMaskingFilter on logger handlers")
    if install_stdout_wrapper:
        _wrap_std_streams(masking_filter)
    return masking_filter


def uninstall_masking_filter() -> None:
    # Test-only teardown; production never uninstalls. Raises off the main
    # thread or while live execution scopes exist.
    global _installed_filter
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError(
            "uninstall_masking_filter() must be called on the main thread"
        )
    registry = SecretRegistry.get_instance()
    if registry.has_active_executions():
        raise RuntimeError(
            "uninstall_masking_filter() called while execution scopes are "
            "still active; ending one execution can never unmask another"
        )
    if _installed_filter is None:
        return
    f = _installed_filter
    _installed_filter = None
    for _log, handler in _all_logger_handler_pairs():
        while f in handler.filters:
            handler.removeFilter(f)
    if isinstance(sys.stdout, StreamMaskingWrapper):
        sys.stdout = sys.stdout._original
    if isinstance(sys.stderr, StreamMaskingWrapper):
        sys.stderr = sys.stderr._original
    logger.info("Uninstalled SecretMaskingFilter")
