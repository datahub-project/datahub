"""
Logging filter for secret masking.

This module provides secret masking at the logging layer.
"""

import logging
import re
import sys
import threading
from typing import Any, Dict, Optional, TextIO, Tuple

from datahub.ingestion.masking.logging_utils import get_masking_safe_logger
from datahub.ingestion.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)


class SecretMaskingFilter(logging.Filter):
    """
    Logging filter that masks secrets in log records.

    This filter:
    1. Masks secrets in log messages
    2. Masks secrets in log arguments
    3. Masks secrets in exception information
    4. Truncates large messages automatically
    5. Uses copy-on-write pattern for thread safety (no .copy() needed)

    Thread-safety: Uses RLock for pattern access but does NOT hold lock during
    masking operations or I/O. Pattern and replacements are replaced atomically
    using copy-on-write pattern.
    """

    def __init__(
        self,
        secret_registry: Optional[SecretRegistry] = None,
        max_message_size: int = 5000,
    ):
        """
        Initialize the masking filter.

        Args:
            secret_registry: SecretRegistry instance (uses singleton if None)
            max_message_size: Maximum log message size before truncation (bytes)
        """
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
        """
        Check if pattern needs rebuilding and rebuild if necessary.

        Uses loop instead of recursion to prevent stack overflow
        when secrets are being rapidly modified by concurrent threads.
        """
        MAX_REBUILD_ATTEMPTS = 10  # Prevent infinite loops

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
            escaped_values = [re.escape(value) for value, _ in sorted_secrets]
            pattern_str = "|".join(escaped_values)

            # Compile regex - NOT under lock (this is the expensive part!)
            try:
                new_pattern = re.compile(pattern_str)
                new_replacements = {value: name for value, name in sorted_secrets}
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
        logger.error(
            f"CRITICAL: Failed to rebuild masking pattern after {MAX_REBUILD_ATTEMPTS} attempts. "
            f"Secrets are being modified too rapidly. "
            f"Continuing with potentially stale pattern (version {self._last_version}). "
            f"Some newly added secrets may not be masked until rate of changes decreases."
        )
        # Keep using the old pattern rather than crashing - graceful degradation

    def _mask_text(self, text: str) -> str:
        """
        Mask secrets in text string, displaying variable names for debugging.

        Args:
            text: Text to mask

        Returns:
            Masked text with secrets replaced by ***REDACTED:VARIABLE_NAME***
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
            return "[REDACTED: Masking Circuit Open]"

        # Mask secrets (outside lock - safe because immutable references)
        try:
            # Use callback to include variable name in masked output
            def replace_with_variable_name(match):
                """Replace matched secret with variable name."""
                secret_value = match.group(0)
                # Look up variable name (O(1) dict access)
                variable_name = replacements.get(secret_value, "UNKNOWN")
                # Return formatted mask
                return f"***REDACTED:{variable_name}***"

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
        """
        Mask secrets in log arguments.

        Handles both tuple args (for % formatting) and dict args (for {} formatting).

        Args:
            args: Log record arguments

        Returns:
            Masked arguments
        """
        if not args:
            return args

        try:
            if isinstance(args, dict):
                return {
                    k: self._mask_text(v) if isinstance(v, str) else v
                    for k, v in args.items()
                }
            elif isinstance(args, tuple):
                return tuple(
                    self._mask_text(arg) if isinstance(arg, str) else arg
                    for arg in args
                )
            else:
                return args
        except Exception as e:
            # Fail-secure: never return unmasked args on error
            logger.error(f"CRITICAL: Secret masking failed in args: {e}", exc_info=True)
            return ("[MASKING_ERROR - OUTPUT_SUPPRESSED_FOR_SECURITY]",)

    def _mask_exception(self, exc_info: Optional[Tuple]) -> Optional[Tuple]:
        """
        Mask secrets in exception information.

        Args:
            exc_info: Exception info tuple (type, value, traceback)

        Returns:
            Exception info with masked strings
        """
        if not exc_info:
            return exc_info

        try:
            exc_type, exc_value, exc_traceback = exc_info

            # Mask exception message/args
            if exc_value and hasattr(exc_value, "args") and exc_value.args:
                masked_args = tuple(
                    self._mask_text(arg) if isinstance(arg, str) else arg
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
                RuntimeError("[MASKING_ERROR - OUTPUT_SUPPRESSED_FOR_SECURITY]"),
                None,
            )

    def _truncate_message(self, message: str) -> str:
        """
        Truncate large messages before masking.

        This prevents performance issues when masking very large strings
        (e.g., 80KB curl commands).

        Args:
            message: Log message

        Returns:
            Truncated message if too large
        """
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
        """
        Filter and mask a log record.

        This is called by the logging framework for every log record.
        We mask secrets and return True to allow the record through.

        Args:
            record: LogRecord to filter

        Returns:
            True (always allow record through)
        """
        try:
            # 1. Truncate large messages BEFORE masking (performance)
            if isinstance(record.msg, str):
                record.msg = self._truncate_message(record.msg)

            # 2. Mask the log message
            if isinstance(record.msg, str):
                record.msg = self._mask_text(record.msg)

            # 3. Mask arguments (for formatting)
            if record.args:
                record.args = self._mask_args(record.args)

            # 4. Mask pre-formatted message if it exists
            if hasattr(record, "message") and record.message:
                record.message = self._mask_text(record.message)

            # 5. Mask exception information
            if record.exc_info:
                record.exc_info = self._mask_exception(record.exc_info)

            # 6. Mask formatted exception text if it exists
            if record.exc_text:
                record.exc_text = self._mask_text(record.exc_text)

            # 7. Mask stack_info if present (Python 3.2+)
            if hasattr(record, "stack_info") and record.stack_info:
                record.stack_info = self._mask_text(record.stack_info)

        except Exception as e:
            # NEVER let masking break logging
            try:
                sys.stderr.write(f"WARNING: Secret masking filter failed: {e}\n")
                sys.stderr.flush()
            except Exception:
                pass  # Even error reporting failed, continue silently

        return True  # Always let record through


class StreamMaskingWrapper:
    """
    Lightweight wrapper for stdout/stderr that masks secrets.

    This is a BACKUP to the logging filter. It catches:
    - print() statements
    - Direct sys.stdout.write() calls
    - Third-party library output
    - C extension debug output

    Does NOT hold lock during write() operation to prevent deadlock.
    Uses the same masking logic as SecretMaskingFilter.
    """

    def __init__(self, original_stream: TextIO, masking_filter: SecretMaskingFilter):
        """
        Initialize stream wrapper.

        Args:
            original_stream: Original stdout/stderr stream
            masking_filter: SecretMaskingFilter instance (for masking logic)
        """
        self._original = original_stream
        self._filter = masking_filter

    def write(self, text: str) -> int:
        """
        Write text to stream with secrets masked.
        No lock held during write to prevent deadlock.

        Args:
            text: Text to write (must be str, not bytes)

        Returns:
            Number of characters written (of masked text)

        Raises:
            TypeError: If text is not a string
        """
        # Type validation - text streams require strings
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")

        try:
            # Mask text (filter handles locking internally)
            masked = self._filter._mask_text(text)

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
    """
    Update all existing logging handlers to use wrapped sys.stdout/sys.stderr.

    This function walks through all loggers and updates StreamHandlers that
    are using the original stdout/stderr to use the new wrapped versions.
    """
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
    """
    Install secret masking filter on root logger and optionally wrap stdout/stderr.

    This is the main entry point for enabling secret masking.

    Args:
        secret_registry: SecretRegistry instance (uses singleton if None)
        max_message_size: Maximum message size before truncation
        install_stdout_wrapper: Whether to also install stdout/stderr wrapper

    Returns:
        The installed SecretMaskingFilter instance
    """
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
    """
    Remove secret masking filter from root logger.

    Used primarily for testing.
    """
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
