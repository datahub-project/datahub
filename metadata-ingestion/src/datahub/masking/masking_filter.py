"""
Logging filter for masking secrets in log messages and streams.

This module provides a Python logging.Filter that automatically masks
registered secrets in all log output. Secrets are replaced with
***REDACTED:VARIABLE_NAME*** for debugging while preventing leaks.

Guarantees:
- Masking a secret-free or already-masked text is the identity:
  mask_text(mask_text(x)) == mask_text(x)
- Fail-closed: when masking cannot be performed while secrets are
  registered, output is replaced with a fixed marker, never leaked
"""

import logging
import re
import sys
import threading
from typing import Any, Dict, List, Optional, TextIO, Tuple

from datahub.masking.constants import (
    CAPACITY_EXCEEDED_MESSAGE,
    CIRCUIT_OPEN_MESSAGE,
    MASKING_ERROR_MESSAGE,
    REDACTED_FORMAT,
    REDACTED_PREFIX,
    REDACTED_SUFFIX,
)
from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)

_SENTINEL_MESSAGES = (
    MASKING_ERROR_MESSAGE,
    CIRCUIT_OPEN_MESSAGE,
    CAPACITY_EXCEEDED_MESSAGE,
)


def _compiles(pattern_str: str) -> bool:
    try:
        re.compile(pattern_str)
        return True
    except Exception:
        return False


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
        """Rebuild the pattern if the registry changed. Called under _pattern_lock.

        The version is read before the secrets snapshot, so a registration
        landing mid-build leaves _last_version behind and the next call
        rebuilds again - the swap never needs retrying.
        """
        current_version = self._registry.get_version()
        if current_version == self._last_version:
            return

        secrets = self._registry.get_all_secrets()

        if not secrets:
            self._pattern = None
            self._replacements = {}
            self._last_version = current_version
            return

        sorted_secrets = sorted(secrets.items(), key=lambda x: len(x[0]), reverse=True)

        # CRITICAL: re.escape() ensures secrets with regex metacharacters
        # (e.g., ".*", "a+b", "test|prod") are matched literally, not as regex.
        # The marker alternative comes first so that already-masked spans are
        # consumed whole and never re-matched - this is what makes masking
        # idempotent even when a secret value collides with marker text. Only
        # markers bearing a name this filter could have produced are consumed;
        # a wildcard would let marker-shaped delimiters arriving in untrusted
        # text smuggle a secret through unmasked.
        names = sorted(
            {name for _, name in sorted_secrets} | {"UNKNOWN"},
            key=len,
            reverse=True,
        )
        marker_regex = (
            re.escape(REDACTED_PREFIX)
            + "(?:"
            + "|".join(re.escape(name) for name in names)
            + ")"
            + re.escape(REDACTED_SUFFIX)
        )
        escaped_values = [re.escape(value) for value, _ in sorted_secrets]
        pattern_str = "|".join(
            [
                marker_regex,
                *(re.escape(message) for message in _SENTINEL_MESSAGES),
                *escaped_values,
            ]
        )

        try:
            new_pattern = re.compile(pattern_str)
        except Exception as e:
            self._pattern = None
            self._replacements = {}
            self._open_circuit_for_compile_failure(type(e).__name__, sorted_secrets)
            return

        secret_count = len(secrets)
        if secret_count >= 100:
            logger.warning(
                f"Large number of secrets registered ({secret_count}). "
                f"This may impact masking performance."
            )

        self._pattern = new_pattern
        self._replacements = {value: name for value, name in sorted_secrets}
        self._last_version = current_version
        logger.debug(
            f"Rebuilt masking pattern with {secret_count} secrets "
            f"(version {current_version})"
        )

    def _open_circuit_for_compile_failure(
        self, exception_type: str, sorted_secrets: List[Tuple[str, str]]
    ) -> None:
        if self._circuit_open:
            return
        self._circuit_open = True
        offending = sorted(
            {name for value, name in sorted_secrets if not _compiles(re.escape(value))}
        )
        if offending:
            detail = f"offending secret(s): {', '.join(offending)}"
        else:
            total_length = sum(len(value) for value, _ in sorted_secrets)
            detail = (
                f"no single secret at fault; combined pattern too large "
                f"({len(sorted_secrets)} renderings, {total_length} characters)"
            )
        logger.error(
            f"Masking pattern failed to compile ({exception_type}); {detail}. "
            f"All output will be suppressed; restart the process to recover."
        )

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

        try:
            if self._registry.is_capacity_exceeded():
                return CAPACITY_EXCEEDED_MESSAGE

            if self._circuit_open:
                return CIRCUIT_OPEN_MESSAGE

            with self._pattern_lock:
                self._check_and_rebuild_pattern()
                pattern = self._pattern
                replacements = self._replacements

            if pattern is None:
                if self._registry.get_count() == 0:
                    return text
                return self._masking_failed("no pattern despite registered secrets")

            def replace_match(match: "re.Match[str]") -> str:
                matched = match.group(0)
                if matched.startswith(REDACTED_PREFIX) or matched in _SENTINEL_MESSAGES:
                    return matched
                return REDACTED_FORMAT.format(name=replacements.get(matched, "UNKNOWN"))

            masked = pattern.sub(replace_match, text)

            if self._failure_count > 0:
                self._failure_count = 0

            return masked

        except Exception as e:
            return self._masking_failed(type(e).__name__)

    def _masking_failed(self, cause: str) -> str:
        self._failure_count += 1
        logger.error(
            f"CRITICAL: Secret masking failed "
            f"(failure {self._failure_count}/{self._max_failures}); "
            f"output withheld. Cause: {cause}"
        )
        if self._failure_count >= self._max_failures and not self._circuit_open:
            self._circuit_open = True
            logger.critical(
                "CRITICAL: Masking circuit breaker OPEN. All messages will be redacted."
            )
        return MASKING_ERROR_MESSAGE

    def mask_structure(self, obj: Any) -> Any:
        """Mask every string in a nested structure of dicts/lists/tuples.

        For content that gets serialized afterwards (e.g. JSON reports):
        masking must happen before serialization, because escaping changes
        the rendering of a secret so that it no longer matches any
        registered value.
        """
        if isinstance(obj, str):
            return self.mask_text(obj)
        if isinstance(obj, dict):
            masked_items: Dict[Any, Any] = {}
            for key, value in obj.items():
                masked_key = self.mask_structure(key)
                if masked_key in masked_items:
                    suffix = 2
                    while f"{masked_key} (duplicate {suffix})" in masked_items:
                        suffix += 1
                    masked_key = f"{masked_key} (duplicate {suffix})"
                masked_items[masked_key] = self.mask_structure(value)
            return masked_items
        if isinstance(obj, (list, tuple)):
            return [self.mask_structure(item) for item in obj]
        return obj

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

    def _mask_exception(self, exc_info: Optional[Tuple]) -> Optional[Tuple]:
        """Mask secrets in exception information."""
        if not exc_info:
            return exc_info

        try:
            exc_type, exc_value, exc_traceback = exc_info

            # Mask exception message/args
            if exc_value and hasattr(exc_value, "args") and exc_value.args:
                masked_args = tuple(
                    self.mask_text(arg) if isinstance(arg, str) else arg
                    for arg in exc_value.args
                )
                # Create new exception instance with masked args
                exc_value = type(exc_value)(*masked_args)

            return (exc_type, exc_value, exc_traceback)

        except Exception as e:
            # Fail-secure: never return unmasked exception on error
            logger.error(
                f"CRITICAL: Secret masking failed in exception: {e}", exc_info=True
            )
            # Return a sanitized exception
            return (
                RuntimeError,
                RuntimeError(MASKING_ERROR_MESSAGE),
                None,
            )

    def _truncate_message(self, message: str) -> str:
        if not isinstance(message, str):
            return message

        if len(message) <= self._max_message_size:
            return message

        truncated_bytes = len(message) - self._max_message_size
        return (
            f"{message[: self._max_message_size]}\n"
            f"... [{truncated_bytes} bytes truncated for performance]"
        )

    def filter(self, record: logging.LogRecord) -> bool:
        """Mask every text field of a log record; the record always passes through.

        Masking runs before truncation: truncation can cut through a secret,
        leaving a prefix that no longer matches any registered value.
        """
        from datahub.masking.secret_registry import is_masking_enabled

        if not is_masking_enabled():
            return True

        try:
            if isinstance(record.msg, str):
                record.msg = self._truncate_message(self.mask_text(record.msg))

            if record.args:
                record.args = self._mask_args(record.args)

            if hasattr(record, "message") and record.message:
                record.message = self.mask_text(record.message)

            if record.exc_info:
                record.exc_info = self._mask_exception(record.exc_info)

            if record.exc_text:
                record.exc_text = self.mask_text(record.exc_text)

            if hasattr(record, "stack_info") and record.stack_info:
                record.stack_info = self.mask_text(record.stack_info)

        except Exception as e:
            record.msg = MASKING_ERROR_MESSAGE
            record.args = None
            record.exc_info = None
            record.exc_text = None
            record.stack_info = None
            if hasattr(record, "message"):
                record.message = MASKING_ERROR_MESSAGE
            try:
                sys.stderr.write(f"WARNING: Secret masking filter failed: {e}\n")
                sys.stderr.flush()
            except Exception:
                pass

        return True


class StreamMaskingWrapper:
    """Lightweight wrapper for stdout/stderr that masks secrets."""

    def __init__(self, original_stream: TextIO, masking_filter: SecretMaskingFilter):
        """Initialize stream wrapper."""
        self._original = original_stream
        self._filter = masking_filter

    def write(self, text: str) -> int:
        """Write text to stream with secrets masked."""
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")

        try:
            self._original.write(self._filter.mask_text(text))
        except TypeError:
            raise
        except Exception:
            try:
                self._original.write(MASKING_ERROR_MESSAGE + "\n")
            except Exception:
                return 0
        # "Input fully consumed": reporting fewer characters than were passed
        # would make a partial-write retry loop re-send raw unmasked text.
        return len(text)

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


def _update_existing_handlers() -> None:
    """Update all existing logging handlers to use wrapped streams."""
    updated_count = 0

    # Get all loggers (including root and all named loggers)
    all_loggers = [logging.getLogger()] + [
        logging.getLogger(name) for name in logging.root.manager.loggerDict
    ]

    for log in all_loggers:
        if not isinstance(log, logging.Logger):
            # Skip PlaceHolder objects in logger dict
            continue

        for handler in log.handlers:
            if isinstance(handler, logging.StreamHandler):
                # Check if handler is using an unwrapped stream
                if hasattr(handler, "stream"):
                    stream = handler.stream

                    # If handler's stream is the original unwrapped stdout/stderr,
                    # update it to use our wrapped version
                    if not isinstance(stream, StreamMaskingWrapper):
                        # Check if this is stdout or stderr by comparing the underlying file
                        try:
                            if hasattr(stream, "name"):
                                if stream.name == "<stderr>":
                                    handler.setStream(sys.stderr)
                                    updated_count += 1
                                elif stream.name == "<stdout>":
                                    handler.setStream(sys.stdout)
                                    updated_count += 1
                        except Exception:
                            # If we can't determine the stream, skip it
                            pass

    if updated_count > 0:
        logger.debug(f"Updated {updated_count} logging handlers to use wrapped streams")


_installed_filter: Optional[SecretMaskingFilter] = None
_install_lock = threading.Lock()


def _filterable_handlers() -> "list[logging.Handler]":
    handlers = []
    loggers = [logging.getLogger()] + [
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
        if not name.startswith("datahub.masking")
    ]
    for candidate in loggers:
        if isinstance(candidate, logging.Logger):
            handlers.extend(candidate.handlers)
    return handlers


def install_masking_filter(
    secret_registry: Optional[SecretRegistry] = None,
    max_message_size: int = 5000,
    install_stdout_wrapper: bool = True,
) -> SecretMaskingFilter:
    """Attach the masking filter to every logging handler and wrap stdout/stderr.

    Filters attach to handlers, not loggers: a filter on a logger only applies
    to records emitted directly on it, while propagated records from child
    loggers only pass through ancestor HANDLERS. Safe to call repeatedly -
    each call covers handlers created since the previous one.
    """
    global _installed_filter
    with _install_lock:
        if _installed_filter is None:
            _installed_filter = SecretMaskingFilter(
                secret_registry=secret_registry, max_message_size=max_message_size
            )
            logger.info("Created process-wide SecretMaskingFilter")
        masking_filter = _installed_filter

    attached_count = 0
    for handler in _filterable_handlers():
        if not any(isinstance(f, SecretMaskingFilter) for f in handler.filters):
            handler.addFilter(masking_filter)
            attached_count += 1
    if attached_count:
        logger.debug(f"Attached masking filter to {attached_count} handler(s)")

    if install_stdout_wrapper:
        if not isinstance(sys.stdout, StreamMaskingWrapper):
            sys.stdout = StreamMaskingWrapper(sys.stdout, masking_filter)
        if not isinstance(sys.stderr, StreamMaskingWrapper):
            sys.stderr = StreamMaskingWrapper(sys.stderr, masking_filter)
        _update_existing_handlers()

    return masking_filter


def uninstall_masking_filter() -> None:
    """Detach masking everywhere. Production never tears masking down; this
    exists so tests can isolate the process-global state they exercise."""
    global _installed_filter

    root_logger = logging.getLogger()
    root_logger.filters = [
        f for f in root_logger.filters if not isinstance(f, SecretMaskingFilter)
    ]
    for handler in _filterable_handlers():
        for existing in list(handler.filters):
            if isinstance(existing, SecretMaskingFilter):
                handler.removeFilter(existing)

    if isinstance(sys.stdout, StreamMaskingWrapper):
        sys.stdout = sys.stdout._original

    if isinstance(sys.stderr, StreamMaskingWrapper):
        sys.stderr = sys.stderr._original

    with _install_lock:
        _installed_filter = None

    logger.info("Uninstalled SecretMaskingFilter")
