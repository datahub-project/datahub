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

import logging
import re
import sys
import threading
from typing import Any, Dict, Optional, TextIO, Tuple

from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)

# Constants
REDACTED_FORMAT = "***REDACTED:{name}***"
MASKING_ERROR_MESSAGE = "[MASKING_ERROR - OUTPUT_SUPPRESSED_FOR_SECURITY]"
CIRCUIT_OPEN_MESSAGE = "[REDACTED: Masking Circuit Open]"


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

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter and mask a log record."""
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

            # 5. Mask exception information
            if record.exc_info:
                record.exc_info = self._mask_exception(record.exc_info)

            # 6. Mask formatted exception text if it exists
            if record.exc_text:
                record.exc_text = self.mask_text(record.exc_text)

            # 7. Mask stack_info if present (Python 3.2+)
            if hasattr(record, "stack_info") and record.stack_info:
                record.stack_info = self.mask_text(record.stack_info)

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


def install_masking_filter(
    secret_registry: Optional[SecretRegistry] = None,
    max_message_size: int = 5000,
    install_stdout_wrapper: bool = True,
) -> SecretMaskingFilter:
    """Install secret masking filter on root logger and optionally wrap stdout/stderr."""
    # Create filter
    masking_filter = SecretMaskingFilter(
        secret_registry=secret_registry, max_message_size=max_message_size
    )

    # Install on root logger (affects all loggers)
    root_logger = logging.getLogger()

    # Check if already installed (avoid duplicates)
    existing_filters = [
        f for f in root_logger.filters if isinstance(f, SecretMaskingFilter)
    ]

    if existing_filters:
        logger.debug("SecretMaskingFilter already installed on root logger")
        return existing_filters[0]

    root_logger.addFilter(masking_filter)
    logger.info("Installed SecretMaskingFilter on root logger")

    # Optionally install stdout/stderr wrapper as backup
    if install_stdout_wrapper:
        if not isinstance(sys.stdout, StreamMaskingWrapper):
            sys.stdout = StreamMaskingWrapper(sys.stdout, masking_filter)
            logger.debug("Wrapped sys.stdout with StreamMaskingWrapper")

        if not isinstance(sys.stderr, StreamMaskingWrapper):
            sys.stderr = StreamMaskingWrapper(sys.stderr, masking_filter)
            logger.debug("Wrapped sys.stderr with StreamMaskingWrapper")

        # Update all existing logging handlers to use wrapped streams
        # Handlers created before masking was initialized will have cached
        # references to the original unwrapped stderr/stdout
        _update_existing_handlers()

    return masking_filter


def uninstall_masking_filter() -> None:
    """Remove secret masking filter from root logger."""
    root_logger = logging.getLogger()

    # Remove filters
    root_logger.filters = [
        f for f in root_logger.filters if not isinstance(f, SecretMaskingFilter)
    ]

    # Unwrap stdout/stderr
    if isinstance(sys.stdout, StreamMaskingWrapper):
        sys.stdout = sys.stdout._original

    if isinstance(sys.stderr, StreamMaskingWrapper):
        sys.stderr = sys.stderr._original

    logger.info("Uninstalled SecretMaskingFilter")
