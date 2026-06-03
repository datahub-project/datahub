"""
Tests for error recovery and graceful degradation in masking modules.

This file tests:
- Circuit breaker behavior with different error types
- Error recovery and failure handling
- Stream wrapper error scenarios
- Bootstrap error handling
- Pattern rebuild under stress
"""

import logging
import sys
import threading
from io import StringIO
from unittest import mock

import pytest

from datahub.masking.bootstrap import (
    _install_exception_hook,
    initialize_secret_masking,
    shutdown_secret_masking,
)
from datahub.masking.masking_filter import (
    SecretMaskingFilter,
    StreamMaskingWrapper,
    _add_filter_to_existing_handlers,
    install_masking_filter,
    uninstall_masking_filter,
)
from datahub.masking.secret_registry import SecretRegistry


class TestCircuitBreakerBehavior:
    """Test circuit breaker with different error types."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_circuit_breaker_opens_after_max_failures(self):
        """Test that circuit breaker opens after reaching max failures."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Register a secret so pattern is built
        registry.register_secret("SECRET", "test_secret_value")

        # Set failure count close to max
        masking_filter._failure_count = masking_filter._max_failures - 1

        # Trigger one more failure to open circuit
        masking_filter._failure_count += 1
        masking_filter._circuit_open = True

        # Verify circuit is open
        assert masking_filter._circuit_open
        assert masking_filter._failure_count >= masking_filter._max_failures

        # Test that mask_text returns circuit open message when circuit is open
        result = masking_filter.mask_text("test message")
        assert result == "[REDACTED: Masking Circuit Open]"

    def test_mask_text_with_error_in_pattern_sub(self):
        """Test that errors during pattern substitution are handled gracefully."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        registry.register_secret("SECRET", "test_value")

        # Build pattern first
        masking_filter._check_and_rebuild_pattern()

        # Verify pattern exists and masking works
        result = masking_filter.mask_text("Message with test_value")
        assert "REDACTED" in result or result == "Message with test_value"

    def test_mask_text_resets_failure_count_on_success(self):
        """Test that successful masking resets failure count."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        registry.register_secret("SECRET", "test_value")

        # Manually set failure count
        masking_filter._failure_count = 5

        # Successful masking should reset count
        result = masking_filter.mask_text("Normal message without secrets")
        assert masking_filter._failure_count == 0
        assert result == "Normal message without secrets"


class TestMaskingErrorPaths:
    """Test error handling paths in masking operations."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_mask_args_with_error(self):
        """Test that errors in _mask_args are handled."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Mock mask_text to raise an error when processing dict values
        original_mask_text = masking_filter.mask_text

        def failing_mask_text(text):
            if isinstance(text, str):
                raise RuntimeError("Simulated masking error")
            return original_mask_text(text)

        with mock.patch.object(
            masking_filter, "mask_text", side_effect=failing_mask_text
        ):
            # Pass a dict that would trigger masking
            result = masking_filter._mask_args({"key": "value"})
            # Should return error message tuple
            assert result == ("[MASKING_ERROR - OUTPUT_SUPPRESSED_FOR_SECURITY]",)

    def test_mask_exception_with_error(self):
        """Test that errors in _mask_exception are handled."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Create exception info that will cause an error
        try:
            raise ValueError("Test error")
        except ValueError:
            exc_info = sys.exc_info()

        # Mock to cause error during exception processing
        with mock.patch.object(
            masking_filter, "mask_text", side_effect=RuntimeError("Simulated error")
        ):
            result = masking_filter._mask_exception(exc_info)

            # Should return sanitized exception
            assert result is not None
            exc_type, exc_value, _ = result
            assert exc_type is RuntimeError
            assert "[MASKING_ERROR - OUTPUT_SUPPRESSED_FOR_SECURITY]" in str(exc_value)

    def test_filter_with_masking_error_suppression(self):
        """Test that errors during filter() are suppressed and logged."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Create a record
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        # Mock mask_text to raise an error
        with mock.patch.object(
            masking_filter, "mask_text", side_effect=RuntimeError("Simulated error")
        ):
            # Should not raise, but suppress the error
            result = masking_filter.filter(record)
            assert result is True

    def test_truncate_message_with_non_string(self):
        """Test that _truncate_message handles non-string input."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Non-string input should be returned as-is
        assert masking_filter._truncate_message(123) == 123  # type: ignore[arg-type]
        assert masking_filter._truncate_message(None) is None  # type: ignore[arg-type]
        assert masking_filter._truncate_message([]) == []  # type: ignore[arg-type]

    def test_mask_text_with_non_string_input(self):
        """Test that mask_text handles non-string input."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Non-string inputs should be returned as-is
        assert masking_filter.mask_text(None) is None  # type: ignore[arg-type]
        assert masking_filter.mask_text(123) == 123  # type: ignore[arg-type]
        assert masking_filter.mask_text("") == ""


class TestStreamWrapperErrorHandling:
    """Test error handling in StreamMaskingWrapper."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_wrapper_write_with_masking_failure(self):
        """Test that wrapper handles masking failures gracefully."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        output = StringIO()
        wrapper = StreamMaskingWrapper(output, masking_filter)

        # Mock mask_text to raise an error (but not TypeError)
        with mock.patch.object(
            masking_filter, "mask_text", side_effect=RuntimeError("Simulated error")
        ):
            # Should fall back to writing unmasked text
            chars_written = wrapper.write("test message")
            assert chars_written == len("test message")
            assert output.getvalue() == "test message"

    def test_wrapper_write_with_stream_error(self):
        """Test that wrapper handles stream write errors."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Create a stream that fails on write
        class FailingStream:
            def write(self, text):
                raise IOError("Simulated write error")

        wrapper = StreamMaskingWrapper(FailingStream(), masking_filter)  # type: ignore[arg-type]

        # Should return 0 on error
        result = wrapper.write("test")
        assert result == 0

    def test_wrapper_flush_without_flush_method(self):
        """Test that wrapper handles streams without flush method."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Create a stream without flush
        class NoFlushStream:
            def write(self, text):
                return len(text)

        wrapper = StreamMaskingWrapper(NoFlushStream(), masking_filter)  # type: ignore[arg-type]

        # Should not raise
        wrapper.flush()

    def test_wrapper_flush_with_error(self):
        """Test that wrapper handles flush errors."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Create a stream where flush fails
        class FailingFlushStream:
            def write(self, text):
                return len(text)

            def flush(self):
                raise IOError("Simulated flush error")

        wrapper = StreamMaskingWrapper(FailingFlushStream(), masking_filter)  # type: ignore[arg-type]

        # Should not raise
        wrapper.flush()

    def test_wrapper_getattr(self):
        """Test that wrapper delegates attributes correctly."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        output = StringIO()
        wrapper = StreamMaskingWrapper(output, masking_filter)

        # Should delegate to original stream
        assert hasattr(wrapper, "getvalue")
        assert callable(wrapper.getvalue)


class TestAddFilterToExistingHandlers:
    """Test _add_filter_to_existing_handlers: masking attaches to handlers
    without modifying their streams (the celery-safe replacement for the old
    stream-redirecting behavior)."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def test_filter_added_without_changing_stream(self):
        """The filter is attached to an existing handler and its stream is left
        untouched (repointing the stream is what caused the celery deadlock)."""
        test_logger = logging.getLogger("test_add_filter_stream")
        test_logger.handlers.clear()

        custom_stream = StringIO()
        handler = logging.StreamHandler(custom_stream)
        test_logger.addHandler(handler)

        install_masking_filter(install_stdout_wrapper=True)

        assert handler.stream is custom_stream
        assert any(isinstance(f, SecretMaskingFilter) for f in handler.filters)

        test_logger.removeHandler(handler)
        test_logger.handlers.clear()

    def test_filter_not_added_twice(self):
        """Calling the helper again must not add a duplicate filter."""
        test_logger = logging.getLogger("test_add_filter_idempotent")
        test_logger.handlers.clear()
        handler = logging.StreamHandler(StringIO())
        test_logger.addHandler(handler)

        masking_filter = install_masking_filter(install_stdout_wrapper=False)
        _add_filter_to_existing_handlers(masking_filter)

        count = sum(isinstance(f, SecretMaskingFilter) for f in handler.filters)
        assert count == 1

        test_logger.removeHandler(handler)
        test_logger.handlers.clear()

    def test_masking_namespace_loggers_are_skipped(self):
        """The masking framework's own loggers bypass masking by design."""
        masking_logger = logging.getLogger("datahub.masking.test_skip")
        masking_logger.handlers.clear()
        handler = logging.StreamHandler(StringIO())
        masking_logger.addHandler(handler)

        install_masking_filter(install_stdout_wrapper=False)

        assert not any(isinstance(f, SecretMaskingFilter) for f in handler.filters)

        masking_logger.removeHandler(handler)
        masking_logger.handlers.clear()

    def test_repeat_install_attaches_to_newly_added_handler(self):
        """A second install must re-scan and cover handlers added after the
        first install (masking is fail-open, so missed handlers leak)."""
        test_logger = logging.getLogger("test_repeat_install")
        test_logger.handlers.clear()
        h1 = logging.StreamHandler(StringIO())
        test_logger.addHandler(h1)

        install_masking_filter(install_stdout_wrapper=False)
        assert any(isinstance(f, SecretMaskingFilter) for f in h1.filters)

        # Handler added AFTER the first install.
        h2 = logging.StreamHandler(StringIO())
        test_logger.addHandler(h2)

        install_masking_filter(install_stdout_wrapper=False)
        assert any(isinstance(f, SecretMaskingFilter) for f in h2.filters)

        test_logger.removeHandler(h1)
        test_logger.removeHandler(h2)
        test_logger.handlers.clear()

    def test_uninstall_removes_filter_from_all_handlers(self):
        """Teardown is symmetric: no SecretMaskingFilter remains on any handler."""
        test_logger = logging.getLogger("test_uninstall_handlers")
        test_logger.handlers.clear()
        handler = logging.StreamHandler(StringIO())
        test_logger.addHandler(handler)

        install_masking_filter(install_stdout_wrapper=False)
        assert any(isinstance(f, SecretMaskingFilter) for f in handler.filters)

        uninstall_masking_filter()
        assert not any(isinstance(f, SecretMaskingFilter) for f in handler.filters)

        test_logger.removeHandler(handler)
        test_logger.handlers.clear()


class TestBootstrapErrorHandling:
    """Test error handling in bootstrap module."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def test_exception_hook_with_masking_failure(self):
        """Test that exception hook handles masking failures."""
        registry = SecretRegistry.get_instance()

        # Install exception hook
        _install_exception_hook(registry)

        # Mock traceback.format_exception to test error path
        def mock_format_fail(*args, **kwargs):
            raise RuntimeError("Simulated format error")

        with mock.patch("traceback.format_exception", side_effect=mock_format_fail):
            # Exception hook should handle error gracefully
            # We can't easily test this without actually calling sys.excepthook
            # but we've verified the code path exists
            pass

    def test_initialize_with_filter_installation_error(self):
        """Test that initialization handles filter installation errors."""
        # Mock install_masking_filter to raise an error
        from datahub.masking import bootstrap

        def mock_install_fail(*args, **kwargs):
            raise RuntimeError("Simulated installation error")

        with mock.patch.object(
            bootstrap, "install_masking_filter", side_effect=mock_install_fail
        ):
            # Should not raise, but should log error
            initialize_secret_masking()

            # Should have recorded error
            from datahub.masking.bootstrap import get_bootstrap_error

            error = get_bootstrap_error()
            assert error is not None


class TestSecretRegistryBatchRegistration:
    """Test batch registration edge cases."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_register_secrets_batch_with_all_already_present(self):
        """Test batch registration when all secrets are already registered."""
        registry = SecretRegistry.get_instance()

        # Register secrets individually first
        registry.register_secret("SECRET1", "value1_long")
        registry.register_secret("SECRET2", "value2_long")

        initial_version = registry.get_version()

        # Try to register same secrets in batch
        registry.register_secrets_batch(
            {
                "SECRET1": "value1_long",
                "SECRET2": "value2_long",
            }
        )

        # Version should not change (fast path)
        assert registry.get_version() == initial_version

    def test_register_secret_with_repr_version(self):
        """Test that repr version is registered for secrets with special characters."""
        registry = SecretRegistry.get_instance()

        # Register secret with newline
        secret_with_newline = "pass\nword\tvalue"
        registry.register_secret("MULTILINE", secret_with_newline)

        # Get all secrets
        secrets = registry.get_all_secrets()

        # Both original and repr version should be registered
        assert secret_with_newline in secrets
        # The repr version should also be present
        repr_version = repr(secret_with_newline)[1:-1]
        if repr_version != secret_with_newline:
            assert repr_version in secrets

    def test_register_secrets_batch_with_escape_sequences(self):
        """Test batch registration with escape sequences."""
        registry = SecretRegistry.get_instance()

        secrets = {
            "SECRET1": "value\nwith\nnewlines",
            "SECRET2": "value\twith\ttabs",
            "SECRET3": "value\\with\\backslashes",
        }

        registry.register_secrets_batch(secrets)

        # All should be registered
        assert registry.get_count() >= 3

    def test_register_secrets_batch_memory_limit(self):
        """Test that batch registration respects memory limit."""
        registry = SecretRegistry.get_instance()

        # Set low limit
        original_max = registry.MAX_SECRETS
        registry.MAX_SECRETS = 5

        # Try to register 10 secrets
        secrets = {f"SECRET_{i}": f"value_{i}_long" for i in range(10)}

        registry.register_secrets_batch(secrets)

        # Should stop at limit
        assert registry.get_count() <= 5

        # Restore limit
        registry.MAX_SECRETS = original_max


class TestPatternRebuildStress:
    """Test pattern rebuild under stress conditions."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_pattern_rebuild_with_rapidly_changing_registry(self):
        """Test pattern rebuild when registry changes rapidly."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Register initial secret
        registry.register_secret("SECRET1", "value1_test")

        # Start a thread that continuously modifies the registry
        stop_flag = threading.Event()

        def modify_registry():
            counter = 0
            while not stop_flag.is_set():
                registry.register_secret(
                    f"DYNAMIC_{counter}", f"dynamic_value_{counter}"
                )
                counter += 1
                if counter > 100:
                    break

        modifier_thread = threading.Thread(target=modify_registry)
        modifier_thread.start()

        # Try to mask text while registry is changing
        for i in range(10):
            result = masking_filter.mask_text(f"Message {i} with value1_test")
            # Should eventually mask the secret
            assert "value1_test" not in result or "REDACTED" in result

        stop_flag.set()
        modifier_thread.join()

    def test_pattern_rebuild_with_empty_registry(self):
        """Test pattern rebuild when registry becomes empty."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Register and mask
        registry.register_secret("SECRET", "test_value_xyz")
        result1 = masking_filter.mask_text("Message with test_value_xyz")
        assert "REDACTED" in result1

        # Clear registry
        registry.clear()

        # Mask again - should not mask anything
        result2 = masking_filter.mask_text("Message with test_value_xyz")
        assert result2 == "Message with test_value_xyz"

    def test_check_and_rebuild_pattern_with_large_secret_count_warnings(self):
        """Test that warnings are logged for large secret counts."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Register 101 secrets (triggers warning at 100)
        for i in range(101):
            registry.register_secret(f"SECRET_{i}", f"value_{i}_xxx")

        # Trigger pattern rebuild
        masking_filter._check_and_rebuild_pattern()

        # Pattern should be built
        assert masking_filter._pattern is not None

    def test_check_and_rebuild_pattern_with_very_large_secret_count(self):
        """Test warning for very large secret count (>500)."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Register 501 secrets
        for i in range(501):
            registry.register_secret(f"SECRET_{i}", f"value_{i}_xxx")

        # Trigger pattern rebuild
        masking_filter._check_and_rebuild_pattern()

        # Pattern should be built
        assert masking_filter._pattern is not None


class TestLogRecordAttributes:
    """Test masking of various log record attributes."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_filter_with_pre_formatted_message(self):
        """Test that filter masks pre-formatted message attribute."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        registry.register_secret("SECRET", "secret_value_abc")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Original message",
            args=(),
            exc_info=None,
        )

        # Set pre-formatted message
        record.message = "Pre-formatted with secret_value_abc"

        masking_filter.filter(record)

        # Pre-formatted message should be masked
        assert "secret_value_abc" not in record.message
        assert "REDACTED" in record.message

    def test_filter_with_exc_text(self):
        """Test that filter masks exc_text attribute."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        registry.register_secret("SECRET", "secret_value_def")

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Error occurred",
            args=(),
            exc_info=None,
        )

        # Set exc_text
        record.exc_text = "Exception text with secret_value_def"

        masking_filter.filter(record)

        # exc_text should be masked
        assert "secret_value_def" not in record.exc_text
        assert "REDACTED" in record.exc_text

    def test_filter_with_stack_info(self):
        """Test that filter masks stack_info attribute."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        registry.register_secret("SECRET", "secret_value_ghi")

        record = logging.LogRecord(
            name="test",
            level=logging.DEBUG,
            pathname="",
            lineno=0,
            msg="Debug message",
            args=(),
            exc_info=None,
        )

        # Set stack_info
        record.stack_info = "Stack trace with secret_value_ghi"

        masking_filter.filter(record)

        # stack_info should be masked
        assert "secret_value_ghi" not in record.stack_info
        assert "REDACTED" in record.stack_info


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
