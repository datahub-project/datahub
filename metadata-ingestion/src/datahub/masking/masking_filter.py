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
from typing import Any, Dict, Optional, TextIO, Tuple

from datahub.masking.constants import (
    CIRCUIT_OPEN_MESSAGE,
    MASKING_ERROR_MESSAGE,
    REDACTED_FORMAT,
    REDACTED_PREFIX,
    SENTINEL_MESSAGES,
)
from datahub.masking.logging_utils import (
    get_masking_safe_logger,
    is_masking_internal_logger,
)
from datahub.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)


class SecretMaskingFilter(logging.Filter):
    """Logging filter that masks secrets in log records."""

    def __init__(
        self,
        secret_registry: Optional[SecretRegistry] = None,
        max_message_size: int = 5000,
    ):
        """Initialize the masking filter.

        Instances are interchangeable views over the registry, which owns the
        compiled pattern and the fail-closed state; only the runtime circuit
        breaker below is per-instance.
        """
        super().__init__()

        self._registry = secret_registry or SecretRegistry.get_instance()
        self._max_message_size = max_message_size

        self._failure_count = 0
        self._max_failures = 10
        self._circuit_open = False

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
            if self._circuit_open:
                return CIRCUIT_OPEN_MESSAGE

            pattern, replacements = self._registry.get_pattern_and_replacements()

            suppression = self._registry.suppression_message()
            if suppression is not None:
                return suppression

            if pattern is None:
                if self._registry.get_count() == 0:
                    return text
                return self._masking_failed("no pattern despite registered secrets")

            def replace_match(match: "re.Match[str]") -> str:
                matched = match.group(0)
                if matched.startswith(REDACTED_PREFIX) or matched in SENTINEL_MESSAGES:
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


def _covered_loggers() -> "list[logging.Logger]":
    """Root plus all named loggers except masking's own diagnostic channel,
    which must stay unmasked so the causes of a suppression remain visible.

    Iterates a snapshot: loggerDict is mutated by any thread creating a
    logger, and materializing a PlaceHolder mutates it too, so the live dict
    must never be iterated. PlaceHolders carry no handlers - skip them
    instead of materializing them.
    """
    loggers = [logging.getLogger()]
    for name, candidate in list(logging.root.manager.loggerDict.items()):
        if is_masking_internal_logger(name):
            continue
        if isinstance(candidate, logging.Logger):
            loggers.append(candidate)
    return loggers


def _update_existing_handlers() -> None:
    """Point existing stream handlers at the wrapped stdout/stderr.

    Handlers created before masking was initialized hold references to the
    original unwrapped streams.
    """
    updated_count = 0
    for covered in _covered_loggers():
        for handler in covered.handlers:
            if not isinstance(handler, logging.StreamHandler):
                continue
            stream = getattr(handler, "stream", None)
            if stream is None or isinstance(stream, StreamMaskingWrapper):
                continue
            try:
                if getattr(stream, "name", None) == "<stderr>":
                    handler.setStream(sys.stderr)
                    updated_count += 1
                elif getattr(stream, "name", None) == "<stdout>":
                    handler.setStream(sys.stdout)
                    updated_count += 1
            except Exception:
                pass

    if updated_count > 0:
        logger.debug(f"Updated {updated_count} logging handlers to use wrapped streams")


def _filterable_handlers() -> "list[logging.Handler]":
    return [handler for covered in _covered_loggers() for handler in covered.handlers]


def install_masking_filter(
    secret_registry: Optional[SecretRegistry] = None,
    max_message_size: int = 5000,
    install_stdout_wrapper: bool = True,
) -> SecretMaskingFilter:
    """Attach a masking filter to every logging handler and wrap stdout/stderr.

    Filters attach to handlers, not loggers: a filter on a logger only applies
    to records emitted directly on it, while propagated records from child
    loggers only pass through ancestor HANDLERS. Safe to call repeatedly -
    each call covers handlers created since the previous one, and filter
    instances are interchangeable views over the registry.
    """
    masking_filter = SecretMaskingFilter(
        secret_registry=secret_registry, max_message_size=max_message_size
    )

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

    logger.info("Uninstalled SecretMaskingFilter")
