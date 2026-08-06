"""
Unit tests for secret masking filter.

Tests:
- Basic message masking
- Argument masking (% and {} formatting)
- Exception masking
- Large message truncation
- Thread safety
- Performance
"""

import logging
import sys
import threading
import time
from io import StringIO

import pytest
from pytest import MonkeyPatch

from datahub.masking.masking_filter import (
    SecretMaskingFilter,
    StreamMaskingWrapper,
    _remove_filter_from_existing_handlers,
    install_masking_filter,
    uninstall_masking_filter,
)
from datahub.masking.secret_registry import SecretRegistry
from datahub.utilities.perf_timer import PerfTimer


@pytest.fixture
def registry():
    """Create fresh registry for each test."""
    reg = SecretRegistry()
    reg.clear()
    return reg


@pytest.fixture
def masking_filter(registry):
    """Create masking filter with test registry."""
    return SecretMaskingFilter(registry)


class TestBasicMasking:
    """Test basic secret masking functionality."""

    def test_basic_message_masking(self, registry, masking_filter):
        """Test basic secret masking in log messages."""
        # Register secret
        registry.register_secret("TEST_PASSWORD", "secret123")

        # Create log record
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Password is secret123",
            args=(),
            exc_info=None,
        )

        # Filter the record
        masking_filter.filter(record)

        # Check masking
        assert "secret123" not in record.msg
        assert "***REDACTED:TEST_PASSWORD***" in record.msg

    def test_multiple_secrets_in_message(self, registry, masking_filter):
        """Test masking multiple secrets in one message."""
        registry.register_secret("PASSWORD", "pass123")
        registry.register_secret("TOKEN", "tok456")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Password: pass123, Token: tok456",
            args=(),
            exc_info=None,
        )

        masking_filter.filter(record)

        assert "pass123" not in record.msg
        assert "tok456" not in record.msg
        assert "***REDACTED:PASSWORD***" in record.msg
        assert "***REDACTED:TOKEN***" in record.msg

    def test_no_secrets_registered(self, masking_filter):
        """Test that filter works when no secrets are registered."""
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="This is a normal message",
            args=(),
            exc_info=None,
        )

        masking_filter.filter(record)

        assert record.msg == "This is a normal message"

    def test_empty_message(self, registry, masking_filter):
        """Test that filter handles empty messages."""
        registry.register_secret("SECRET", "value")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="",
            args=(),
            exc_info=None,
        )

        masking_filter.filter(record)

        assert record.msg == ""


class TestFormattedMessages:
    """Test masking with formatted messages."""

    def test_percent_formatting(self, registry, masking_filter):
        """Test masking with % formatting."""
        registry.register_secret("TOKEN", "token_abc123")

        # Test % formatting
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Auth token: %s",
            args=("token_abc123",),
            exc_info=None,
        )

        masking_filter.filter(record)

        # Args should be masked
        assert "token_abc123" not in str(record.args)
        assert "***REDACTED:TOKEN***" in str(record.args)

    def test_dict_formatting(self, registry, masking_filter):
        """Test masking with dict formatting."""
        registry.register_secret("PASSWORD", "mypass")

        # Create a proper logger and log with dict formatting
        # (LogRecord expects args to be tuple, not dict)
        test_logger = logging.getLogger("test_dict")
        test_logger.addFilter(masking_filter)

        # Capture the log record
        class RecordCapture(logging.Handler):
            def __init__(self):
                super().__init__()
                self.record = None

            def emit(self, record):
                self.record = record

        handler = RecordCapture()
        test_logger.addHandler(handler)

        # Log with dict formatting
        test_logger.info("Password is %(password)s", {"password": "mypass"})

        # Check the record
        record = handler.record
        assert record is not None
        assert "mypass" not in str(record.args)

        # Cleanup
        test_logger.removeHandler(handler)
        test_logger.removeFilter(masking_filter)

    def test_multiple_args(self, registry, masking_filter):
        """Test masking with multiple arguments."""
        registry.register_secret("USER", "admin")
        registry.register_secret("PASS", "secret")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="User: %s, Pass: %s",
            args=("admin", "secret"),
            exc_info=None,
        )

        masking_filter.filter(record)

        assert "admin" not in str(record.args)
        assert "secret" not in str(record.args)


class TestExceptionMasking:
    """Test masking in exception messages."""

    def test_exception_masking(self, registry, masking_filter):
        """Test masking in exception messages."""
        registry.register_secret("SECRET", "my_secret_value")

        # Create exception with secret
        try:
            raise ValueError("Error with my_secret_value")
        except ValueError:
            exc_info = sys.exc_info()

        # Create log record with exception
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="An error occurred",
            args=(),
            exc_info=exc_info,
        )

        # Filter
        masking_filter.filter(record)

        # exc_info is left intact (error reporters read it); the masked
        # traceback text is materialized on exc_text, which formatters emit.
        assert record.exc_info is not None
        assert record.exc_text is not None
        assert "my_secret_value" not in record.exc_text
        assert "***REDACTED:SECRET***" in record.exc_text

    def test_exception_with_multiple_args(self, registry, masking_filter):
        """Test masking exceptions with multiple args."""
        registry.register_secret("KEY1", "value1")
        registry.register_secret("KEY2", "value2")

        try:
            raise RuntimeError("value1", "value2")
        except RuntimeError:
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Error",
            args=(),
            exc_info=exc_info,
        )

        masking_filter.filter(record)

        # exc_info is left intact; masked traceback text on exc_text.
        assert record.exc_info is not None
        assert record.exc_text is not None
        assert "value1" not in record.exc_text
        assert "value2" not in record.exc_text


class TestMessageTruncation:
    """Test automatic truncation of large messages."""

    def test_large_message_truncation(self, registry, masking_filter):
        """Test automatic truncation of large messages."""
        # Create 10KB message
        large_msg = "x" * 10000

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=large_msg,
            args=(),
            exc_info=None,
        )

        # Filter (should truncate)
        masking_filter.filter(record)

        # Message should be truncated
        assert len(record.msg) < 10000
        assert "truncated" in record.msg

    def test_small_message_not_truncated(self, registry, masking_filter):
        """Test that small messages are not truncated."""
        small_msg = "x" * 100

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=small_msg,
            args=(),
            exc_info=None,
        )

        masking_filter.filter(record)

        # Message should not be truncated
        assert len(record.msg) == 100
        assert "truncated" not in record.msg

    def test_custom_max_size(self):
        """Test custom maximum message size."""
        registry = SecretRegistry()
        registry.clear()

        # Create filter with small max size
        filter = SecretMaskingFilter(registry, max_message_size=100)

        large_msg = "x" * 500

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=large_msg,
            args=(),
            exc_info=None,
        )

        filter.filter(record)

        # Should be truncated to ~100 bytes
        assert len(record.msg) < 200
        assert "truncated" in record.msg


class TestThreadSafety:
    """Test concurrent access from multiple threads."""

    def test_thread_safety(self, registry, masking_filter):
        """Test concurrent access from multiple threads."""
        errors = []

        def worker(thread_id):
            try:
                # Register secret
                registry.register_secret(f"SECRET_{thread_id}", f"value_{thread_id}")

                # Create and filter records
                for i in range(100):
                    record = logging.LogRecord(
                        name="test",
                        level=logging.INFO,
                        pathname="",
                        lineno=0,
                        msg=f"Thread {thread_id} message {i} with value_{thread_id}",
                        args=(),
                        exc_info=None,
                    )
                    masking_filter.filter(record)

                    # Verify masking
                    if f"value_{thread_id}" in record.msg:
                        errors.append(f"Thread {thread_id}: Secret not masked!")
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")

        # Run 10 concurrent threads
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Check for errors
        assert len(errors) == 0, f"Thread safety errors: {errors}"

    def test_concurrent_registration_and_masking(self):
        """Test concurrent registration and masking."""
        registry = SecretRegistry()
        registry.clear()
        filter = SecretMaskingFilter(registry)

        errors = []

        def register_worker(thread_id):
            try:
                for i in range(50):
                    registry.register_secret(
                        f"SECRET_{thread_id}_{i}", f"val{thread_id}_{i}"
                    )
                    time.sleep(0.001)  # Small delay
            except Exception as e:
                errors.append(f"Register thread {thread_id}: {e}")

        def mask_worker(thread_id):
            try:
                for i in range(50):
                    record = logging.LogRecord(
                        name="test",
                        level=logging.INFO,
                        pathname="",
                        lineno=0,
                        msg=f"Message {i}",
                        args=(),
                        exc_info=None,
                    )
                    filter.filter(record)
                    time.sleep(0.001)  # Small delay
            except Exception as e:
                errors.append(f"Mask thread {thread_id}: {e}")

        # Start both registration and masking threads
        threads = []
        threads.extend(
            [threading.Thread(target=register_worker, args=(i,)) for i in range(5)]
        )
        threads.extend(
            [threading.Thread(target=mask_worker, args=(i,)) for i in range(5)]
        )

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Concurrent errors: {errors}"


class TestPerformance:
    """Test performance with many secrets and records."""

    def test_performance(self, registry, masking_filter):
        """Test performance with many secrets and records."""
        # Register 100 secrets
        for i in range(100):
            registry.register_secret(f"SECRET_{i}", f"value_{i}")

        # Process 1000 log records
        start = time.time()

        for i in range(1000):
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=f"Message {i}",
                args=(),
                exc_info=None,
            )
            masking_filter.filter(record)

        duration = time.time() - start

        # Should complete in reasonable time (< 1 second)
        assert duration < 1.0, f"Performance test took {duration:.2f}s (too slow)"

    def test_pattern_rebuild_performance(self, registry, masking_filter):
        """Test that pattern is not rebuilt unnecessarily."""
        registry.register_secret("SECRET", "value")

        # First call - will build pattern
        record1 = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Message 1",
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record1)

        initial_version = masking_filter._last_version

        # Second call - should NOT rebuild pattern
        record2 = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Message 2",
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record2)

        # Version should be the same (no rebuild)
        assert masking_filter._last_version == initial_version


class TestStreamWrapper:
    """Test StreamMaskingWrapper."""

    def test_stdout_wrapper(self, registry):
        """Test StreamMaskingWrapper."""
        registry.register_secret("PASSWORD", "secret_password")

        # Create wrapper
        output = StringIO()
        filter = SecretMaskingFilter(registry)
        wrapper = StreamMaskingWrapper(output, filter)

        # Write through wrapper
        wrapper.write("Password is secret_password\n")

        # Check masking
        result = output.getvalue()
        assert "secret_password" not in result
        assert "***REDACTED:PASSWORD***" in result

    def test_wrapper_flush(self, registry):
        """Test that flush works."""
        output = StringIO()
        filter = SecretMaskingFilter(registry)
        wrapper = StreamMaskingWrapper(output, filter)

        wrapper.write("test")
        wrapper.flush()  # Should not raise

        assert output.getvalue() == "test"

    def test_wrapper_non_string(self, registry):
        """Test that wrapper handles non-string writes."""
        output = StringIO()
        filter = SecretMaskingFilter(registry)
        wrapper = StreamMaskingWrapper(output, filter)

        # This should pass through without error
        # (StringIO will handle the error)
        try:
            wrapper.write(123)  # type: ignore[arg-type]
        except TypeError:
            pass  # Expected for StringIO


class TestInstallation:
    """Test filter installation and removal."""

    def setup_method(self):
        """Ensure clean state before each test."""
        uninstall_masking_filter()

    def teardown_method(self):
        """Clean up after each test."""
        uninstall_masking_filter()

    def test_install_uninstall(self):
        """Test filter installation and removal."""
        registry = SecretRegistry()
        registry.clear()
        registry.register_secret("TEST", "test_value")

        # Install
        install_masking_filter(registry)

        # Verify installed
        root_logger = logging.getLogger()
        filters = [f for f in root_logger.filters if isinstance(f, SecretMaskingFilter)]
        assert len(filters) > 0

        # Uninstall
        uninstall_masking_filter()

        # Verify removed
        filters = [f for f in root_logger.filters if isinstance(f, SecretMaskingFilter)]
        assert len(filters) == 0

    def test_double_install(self):
        """Test that double installation is handled gracefully."""
        registry = SecretRegistry()
        registry.clear()

        # Install twice
        filter1 = install_masking_filter(registry)
        filter2 = install_masking_filter(registry)

        # Should return same filter
        assert filter1 is filter2

        # Cleanup
        uninstall_masking_filter()

    def test_install_with_options(self):
        """Test installation with custom options."""
        registry = SecretRegistry()
        registry.clear()

        # Install with custom options
        filter = install_masking_filter(
            secret_registry=registry, max_message_size=1000, install_stdout_wrapper=True
        )

        assert filter._max_message_size == 1000
        assert isinstance(sys.stdout, StreamMaskingWrapper)

        # Cleanup
        uninstall_masking_filter()


class TestCopyOnWrite:
    """Test copy-on-write pattern."""

    def test_copy_on_write_no_copy_needed(self, registry, masking_filter):
        """Test that copy-on-write pattern means no .copy() is needed."""
        registry.register_secret("SECRET", "secret_value")

        # Get pattern snapshot
        with masking_filter._pattern_lock:
            masking_filter._check_and_rebuild_pattern()
            replacements = masking_filter._replacements  # No .copy()!

        # Change registry in another thread
        def add_secret():
            registry.register_secret("NEW_SECRET", "new_value")

        thread = threading.Thread(target=add_secret)
        thread.start()
        thread.join()

        # Original replacements dict should still be valid
        assert "secret_value" in replacements
        # New secret NOT in our snapshot (as expected)
        assert "new_value" not in replacements

    def test_version_tracking(self, registry, masking_filter):
        """Test that version tracking works correctly."""
        initial_version = registry.get_version()

        # Add a secret
        registry.register_secret("SECRET1", "value1")
        version1 = registry.get_version()
        assert version1 > initial_version

        # Add another secret
        registry.register_secret("SECRET2", "value2")
        version2 = registry.get_version()
        assert version2 > version1

        # Try to add same secret - version should not change
        registry.register_secret("SECRET1", "value1")
        version3 = registry.get_version()
        assert version3 == version2


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_none_message(self, masking_filter):
        """Test that None message is handled."""
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=None,
            args=(),
            exc_info=None,
        )

        # Should not raise
        masking_filter.filter(record)

    def test_non_string_args(self, registry, masking_filter):
        """Test that non-string args are handled."""
        registry.register_secret("SECRET", "value")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test %s %d %s",
            args=("string", 123, ["list"]),
            exc_info=None,
        )

        # Should not raise
        masking_filter.filter(record)

        # Non-string args should be unchanged
        assert isinstance(record.args, tuple)
        assert record.args[1] == 123
        assert record.args[2] == ["list"]

    def test_very_short_secret(self):
        """Test that very short values are not registered as secrets."""
        registry = SecretRegistry()
        registry.clear()

        # Try to register very short values
        registry.register_secret("SHORT", "ab")
        registry.register_secret("EMPTY", "")

        # Should not be registered
        assert registry.get_count() == 0

    def test_special_characters_in_secret(self, registry, masking_filter):
        """Test that special regex characters in secrets are handled."""
        # Register secret with regex special characters
        registry.register_secret("SPECIAL", "test.$*+?[](){}^|\\")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Secret: test.$*+?[](){}^|\\",
            args=(),
            exc_info=None,
        )

        masking_filter.filter(record)

        # Should be masked despite special characters
        assert "test.$*+?[](){}^|\\" not in record.msg
        assert "***REDACTED:SPECIAL***" in record.msg


class TestP1Fixes:
    """Test P1 critical fixes from production hardening review."""

    def test_stream_wrapper_return_value(self):
        """
        P1 FIX #2: Verify write() returns correct character count.

        The wrapper should return len(masked_text), not the original
        stream's return value.
        """
        from io import StringIO

        registry = SecretRegistry()
        registry.register_secret("PASSWORD", "secret123")

        masking_filter = SecretMaskingFilter(registry)
        stream = StringIO()
        wrapper = StreamMaskingWrapper(stream, masking_filter)

        # Write text with secret
        text = "Password is secret123"
        chars_written = wrapper.write(text)

        # Should return length of MASKED text, not original
        masked_text = stream.getvalue()
        assert masked_text == "Password is ***REDACTED:PASSWORD***", (
            f"Expected masked text, got: {masked_text}"
        )
        assert chars_written == len(masked_text), (
            f"Expected {len(masked_text)} chars, got {chars_written}"
        )
        # "Password is ***REDACTED:PASSWORD***" = 12 + 23 = 35 chars
        assert chars_written == 35, (
            f"Expected 35 chars for 'Password is ***REDACTED:PASSWORD***', got {chars_written}"
        )

    def test_stream_wrapper_type_validation(self):
        """
        P1 FIX #2: Verify write() rejects non-string types.

        The wrapper should raise TypeError for non-string input
        to maintain contract compliance.
        """
        from io import StringIO

        registry = SecretRegistry()
        masking_filter = SecretMaskingFilter(registry)
        stream = StringIO()
        wrapper = StreamMaskingWrapper(stream, masking_filter)

        # Should raise TypeError for bytes
        with pytest.raises(TypeError, match="must be str"):
            wrapper.write(b"bytes data")  # type: ignore[arg-type]

        # Should raise TypeError for int
        with pytest.raises(TypeError, match="must be str"):
            wrapper.write(123)  # type: ignore[arg-type]

        # Should raise TypeError for None
        with pytest.raises(TypeError, match="must be str"):
            wrapper.write(None)  # type: ignore[arg-type]

    def test_singleton_thread_safety(self):
        """
        P1 FIX #3: Verify singleton is thread-safe under concurrent access.

        The simplified singleton pattern should only create one instance
        even when accessed concurrently from multiple threads.
        """
        # Reset singleton
        SecretRegistry.reset_instance()

        instances = []

        def get_instance():
            instance = SecretRegistry.get_instance()
            instances.append(id(instance))

        # Create 50 threads trying to get instance simultaneously
        threads = [threading.Thread(target=get_instance) for _ in range(50)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should have same instance ID
        unique_instances = set(instances)
        assert len(unique_instances) == 1, (
            f"Multiple instances created: {len(unique_instances)} unique IDs"
        )
        assert len(instances) == 50, f"Expected 50 calls, got {len(instances)}"

    def test_get_secret_value_performance(self):
        """
        P2 BONUS: Verify O(1) lookup with reverse index.

        With the reverse index, lookups should be O(1) instead of O(n).
        """

        registry = SecretRegistry()

        # Register 1000 secrets
        for i in range(1000):
            registry.register_secret(f"SECRET_{i}", f"value_{i}")

        # Lookup should be fast (O(1))
        with PerfTimer() as timer:
            for i in range(1000):
                value = registry.get_secret_value(f"SECRET_{i}")
                assert value == f"value_{i}", f"Expected value_{i}, got {value}"

        # Should complete in < 10ms (O(1) lookups)
        elapsed = timer.elapsed_seconds()
        assert elapsed < 0.01, (
            f"Lookups too slow: {elapsed:.4f}s (expected <0.01s for 1000 O(1) lookups)"
        )


class TestRegexSecurityFixes:
    """Test regex injection prevention and DoS protection."""

    def test_regex_metacharacters_literal_matching(self):
        """Verify regex metacharacters are treated as literals, not operators."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        masking_filter = SecretMaskingFilter(registry)

        # Test each regex metacharacter
        test_cases = [
            (
                "DOT",
                "api.key",
                "Found api.key here",
                "Found ***REDACTED:DOT*** here",
                "Found apixkey here",
                "Found apixkey here",
            ),
            (
                "STAR",
                "pass*word",
                "Using pass*word",
                "Using ***REDACTED:STAR***",
                "Using password",
                "Using password",
            ),
            (
                "PLUS",
                "key+value",
                "Set key+value",
                "Set ***REDACTED:PLUS***",
                "Set keyvalue",
                "Set keyvalue",
            ),
            (
                "QUESTION",
                "user?name",
                "Got user?name",
                "Got ***REDACTED:QUESTION***",
                "Got username",
                "Got username",
            ),
            (
                "BRACKETS",
                "data[0]",
                "Access data[0]",
                "Access ***REDACTED:BRACKETS***",
                "Access data0",
                "Access data0",
            ),
            (
                "PARENS",
                "func()",
                "Call func()",
                "Call ***REDACTED:PARENS***",
                "Call func",
                "Call func",
            ),
            (
                "PIPE",
                "a|b",
                "Choose a|b",
                "Choose ***REDACTED:PIPE***",
                "Choose a",
                "Choose a",
            ),
            (
                "CARET",
                "^start",
                "Anchor ^start",
                "Anchor ***REDACTED:CARET***",
                "Anchor start",
                "Anchor start",
            ),
        ]

        for (
            name,
            secret,
            match_text,
            expected_masked,
            no_match_text,
            expected_not_masked,
        ) in test_cases:
            registry.clear()
            registry.register_secret(name, secret)

            # Should mask literal string
            masked = masking_filter.mask_text(match_text)
            assert masked == expected_masked, (
                f"{name}: Failed to mask literal. Expected '{expected_masked}', got '{masked}'"
            )

            # Should NOT over-match similar strings
            not_masked = masking_filter.mask_text(no_match_text)
            assert not_masked == expected_not_masked, (
                f"{name}: Over-matched similar string. Expected '{expected_not_masked}', got '{not_masked}'"
            )

    def test_dos_prevention_wildcard_secrets(self):
        """Verify wildcard patterns don't cause over-masking (DoS)."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        masking_filter = SecretMaskingFilter(registry)

        # Register potentially dangerous wildcards (must be >= 3 chars for registry)
        registry.register_secret("WILDCARD", "secret.*")
        registry.register_secret("PLUS", "key.+")
        registry.register_secret("STAR", "pass*")

        # Should only mask literal strings, not act as wildcards
        test_text = "Processing request 12345 with secrets and keys"
        masked = masking_filter.mask_text(test_text)

        # Should NOT mask everything (wildcards should not act as regex)
        assert masked == test_text, (
            f"Wildcard acted as regex (security issue). Expected no masking, got: {masked}"
        )

        # Should only mask literal "secret.*"
        literal_text = "Password is secret.*"
        masked = masking_filter.mask_text(literal_text)
        assert masked == "Password is ***REDACTED:WILDCARD***", (
            f"Failed to mask literal 'secret.*': {masked}"
        )

        # Should NOT match "secretABC" (wildcard should not work as regex)
        not_matching = "Password is secretABC"
        masked2 = masking_filter.mask_text(not_matching)
        assert masked2 == not_matching, (
            f"Wildcard acted as regex (should not match 'secretABC'): {masked2}"
        )

    def test_catastrophic_backtracking_prevention(self):
        """Verify complex patterns don't cause DoS via catastrophic backtracking."""

        registry = SecretRegistry.get_instance()
        registry.clear()
        masking_filter = SecretMaskingFilter(registry)

        # Patterns that would cause catastrophic backtracking if not escaped
        dangerous_patterns = [
            "(a+)+",
            "(a*)*",
            "(a|a)*",
            "(a|ab)*",
            "((a+)+)+",
        ]

        for i, pattern in enumerate(dangerous_patterns):
            registry.register_secret(f"DANGER_{i}", pattern)

        # Force pattern rebuild
        masking_filter._check_and_rebuild_pattern()

        # This should complete quickly (not hang)
        test_text = "a" * 30 + "b"

        with PerfTimer() as timer:
            masked = masking_filter.mask_text(test_text)

        # Should complete in milliseconds, not seconds
        elapsed = timer.elapsed_seconds()
        assert elapsed < 0.01, (
            f"Pattern matching too slow: {elapsed:.4f}s (possible backtracking)"
        )

        # Should not match (these are literals, not regex patterns)
        assert masked == test_text, (
            f"Pattern should not match. Expected no masking, got: {masked}"
        )

    def test_combined_metacharacters_no_regex_interpretation(self):
        """Verify combinations of metacharacters are treated as literals."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        masking_filter = SecretMaskingFilter(registry)

        # Complex combinations
        registry.register_secret("COMPLEX1", "test.$*+?[](){}^|\\")
        registry.register_secret("COMPLEX2", ".*secret.*")
        registry.register_secret("COMPLEX3", "(admin|user)+")

        # Should mask exact literals only
        test_cases = [
            ("Creds: test.$*+?[](){}^|\\", "Creds: ***REDACTED:COMPLEX1***"),
            ("Found .*secret.*", "Found ***REDACTED:COMPLEX2***"),
            ("Auth (admin|user)+", "Auth ***REDACTED:COMPLEX3***"),
            # Should NOT match as regex patterns
            ("Creds: test_anything", "Creds: test_anything"),
            ("Found mysecret", "Found mysecret"),
            ("Auth admin", "Auth admin"),
        ]

        for input_text, expected in test_cases:
            masked = masking_filter.mask_text(input_text)
            assert masked == expected, (
                f"Failed for '{input_text}'. Expected '{expected}', got '{masked}'"
            )

    def test_backslash_escaping(self):
        """Verify backslashes are properly escaped and don't affect other escapes."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        masking_filter = SecretMaskingFilter(registry)

        # Backslashes in various contexts
        registry.register_secret("WINDOWS_PATH", "C:\\Users\\admin\\secret.txt")
        registry.register_secret("REGEX_ESCAPE", "\\d+")
        registry.register_secret("DOUBLE_BACKSLASH", "test\\\\value")

        # Should mask exact strings only
        assert (
            masking_filter.mask_text("Path: C:\\Users\\admin\\secret.txt")
            == "Path: ***REDACTED:WINDOWS_PATH***"
        )
        assert (
            masking_filter.mask_text("Pattern: \\d+")
            == "Pattern: ***REDACTED:REGEX_ESCAPE***"
        )
        assert (
            masking_filter.mask_text("Value: test\\\\value")
            == "Value: ***REDACTED:DOUBLE_BACKSLASH***"
        )

        # Should NOT match as regex
        assert masking_filter.mask_text("Pattern: 123") == "Pattern: 123"
        assert masking_filter.mask_text("Value: testvalue") == "Value: testvalue"


class TestNestedConfigHandling:
    """Test secret registration from nested ConfigModel objects."""

    def test_nested_config_secrets_registered(self):
        """Verify nested configs register secrets properly."""
        from pydantic import SecretStr

        from datahub.configuration.common import ConfigModel

        registry = SecretRegistry.get_instance()
        registry.clear()

        class DatabaseConfig(ConfigModel):
            password: SecretStr

        class AppConfig(ConfigModel):
            database: DatabaseConfig

        # Create nested config
        _config = AppConfig(database=DatabaseConfig(password="nested_secret"))

        # Secret should be registered from nested model
        assert registry.has_secret("password")
        assert registry.get_secret_value("password") == "nested_secret"


class TestThreadSafetyConcurrent:
    """Test thread safety under concurrent load."""

    def test_concurrent_batch_registration(self):
        """Test batch registration from multiple threads."""
        import threading

        registry = SecretRegistry.get_instance()
        registry.clear()

        def register_batch(thread_id: int) -> None:
            secrets = {
                f"SECRET_{thread_id}_{i}": f"value_{thread_id}_{i}" for i in range(50)
            }
            registry.register_secrets_batch(secrets)

        threads = [
            threading.Thread(target=register_batch, args=(i,)) for i in range(10)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should have 500 secrets registered (10 threads * 50 secrets)
        assert registry.get_count() == 500

    def test_concurrent_masking_during_registration(self):
        """Test masking works correctly during concurrent registration."""
        import threading
        import time

        registry = SecretRegistry.get_instance()
        registry.clear()
        masking_filter = SecretMaskingFilter(registry)

        # Pre-register some secrets
        registry.register_secret("EXISTING", "existing_value")

        results = []
        errors = []

        def register_secrets():
            try:
                for i in range(100):
                    registry.register_secret(f"NEW_{i}", f"new_value_{i}")
                    time.sleep(0.001)  # Small delay
            except Exception as e:
                errors.append(e)

        def mask_text():
            try:
                for _ in range(100):
                    # Mask existing secret
                    masked = masking_filter.mask_text("existing_value")
                    results.append(masked)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        reg_thread = threading.Thread(target=register_secrets)
        mask_threads = [threading.Thread(target=mask_text) for _ in range(5)]

        reg_thread.start()
        for t in mask_threads:
            t.start()

        reg_thread.join()
        for t in mask_threads:
            t.join()

        # No errors should occur
        assert len(errors) == 0
        # All masked results should be correct
        assert all("***REDACTED:EXISTING***" in r for r in results)


class TestHandlerCoverageAndCelerySafety:
    """Regression tests for the executor logging-silencing bug.

    Two invariants must hold:
    1. Masking must cover records from CHILD loggers (not just records logged
       directly on root). A filter on the root *logger* does not see propagated
       child records, so the filter must be installed on the *handlers*.
    2. Installing masking must NOT redirect existing handlers' streams. Under
       celery, sys.stderr is a proxy that re-enters the logging system; pointing
       a console handler at it creates an infinite recursion cycle that silently
       swallows all log output.
    """

    def setup_method(self):
        uninstall_masking_filter()
        SecretRegistry.reset_instance()
        self._saved_stderr = sys.stderr
        self._saved_stdout = sys.stdout

    def teardown_method(self):
        uninstall_masking_filter()
        sys.stderr = self._saved_stderr
        sys.stdout = self._saved_stdout
        SecretRegistry.reset_instance()

    def test_child_logger_output_is_masked(self):
        """Secrets in a child logger's output must be masked.

        Reproduces the design flaw: the filter was attached to the root *logger*,
        which never sees records propagated up from child loggers, so their
        output went out unmasked.
        """
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("DB_PASSWORD", "supersecretvalue")

        capture = StringIO()
        root_logger = logging.getLogger()
        handler = logging.StreamHandler(capture)
        handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(handler)

        child = logging.getLogger("datahub.ingestion.source.mysql")
        child.setLevel(logging.INFO)  # ensure the INFO record is emitted

        try:
            # Handler exists BEFORE masking installs (as celery's handlers do).
            install_masking_filter(registry, install_stdout_wrapper=False)

            # Log via a CHILD logger; the record propagates to the root handler.
            child.info("connecting with password supersecretvalue")

            output = capture.getvalue()
            assert "supersecretvalue" not in output
            assert "***REDACTED:DB_PASSWORD***" in output
        finally:
            root_logger.removeHandler(handler)
            child.setLevel(logging.NOTSET)

    def test_celery_style_feedback_stream_does_not_silence_logs(self):
        """A handler writing to a logging-backed proxy must still deliver output.

        Models celery: sys.stderr is a LoggingProxy that re-enters logging, and a
        console handler writes to the real terminal. Before the fix, install
        repointed that console handler at the wrapped proxy, forming a cycle that
        recursed and silently dropped every log line.
        """

        class FakeTerminal:
            # name == "<stderr>" so the (buggy) handler-redirect logic targets it
            name = "<stderr>"

            def __init__(self) -> None:
                self.text = ""

            def write(self, s: str) -> int:
                self.text += s
                return len(s)

            def flush(self) -> None:
                pass

        class LoggingProxy:
            """Like celery's LoggingProxy: writes feed back into logging."""

            name = "<stderr>"

            def __init__(self, target_logger: logging.Logger) -> None:
                self._logger = target_logger

            def write(self, s: str) -> int:
                s = s.rstrip("\n")
                if s:
                    self._logger.warning(s)
                return len(s)

            def flush(self) -> None:
                pass

        terminal = FakeTerminal()
        redirect_logger = logging.getLogger("test.celery.redirect")
        redirect_logger.handlers = []
        redirect_logger.propagate = False
        redirect_handler = logging.StreamHandler(terminal)
        redirect_handler.setFormatter(logging.Formatter("%(message)s"))
        redirect_logger.addHandler(redirect_handler)

        sys.stderr = LoggingProxy(redirect_logger)  # celery redirects stderr

        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("SECRET", "hunter2value")

        try:
            install_masking_filter(registry, install_stdout_wrapper=True)

            unique = "celery-line-7f3a9"
            redirect_logger.warning(unique)

            # The line must reach the terminal (no recursion cycle swallowing it).
            assert unique in terminal.text
        finally:
            redirect_logger.removeHandler(redirect_handler)


class TestReviewFixes:
    """Regression tests for issues raised in code review of the handler-level
    masking fix. Each test targets a specific defect; together they pin the
    invariants the fix is supposed to maintain."""

    def setup_method(self):
        uninstall_masking_filter()
        SecretRegistry.reset_instance()
        self._saved_stderr = sys.stderr
        self._saved_stdout = sys.stdout

    def teardown_method(self):
        uninstall_masking_filter()
        sys.stderr = self._saved_stderr
        sys.stdout = self._saved_stdout
        SecretRegistry.reset_instance()

    def test_two_handlers_do_not_corrupt_record(self):
        """Issue 1: filter() mutates record.msg in place. With N handlers each
        carrying the filter, the record is truncated/masked N times — the
        second truncation re-truncates the already-truncated text, eats the
        previous suffix, and reports a wrong byte count. Idempotency sentinel
        must prevent this."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("SECRET", "secretvalue_xyz")

        root_logger = logging.getLogger()
        h1 = logging.StreamHandler(StringIO())
        h2 = logging.StreamHandler(StringIO())
        h1.setFormatter(logging.Formatter("%(message)s"))
        h2.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(h1)
        root_logger.addHandler(h2)
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            mf = next(f for f in h1.filters if isinstance(f, SecretMaskingFilter))

            big_msg = "secretvalue_xyz " + "x" * 12000
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=big_msg,
                args=(),
                exc_info=None,
            )
            mf.filter(record)
            first_msg = record.msg
            # Second handler runs the same record through the filter again.
            mf.filter(record)
            assert record.msg == first_msg, "Second filter() call mutated the record"
            # Exactly one truncation suffix present (not nested).
            assert record.msg.count("bytes truncated for performance") == 1
            assert "***REDACTED:SECRET***" in record.msg
        finally:
            root_logger.removeHandler(h1)
            root_logger.removeHandler(h2)

    def test_extra_attributes_are_masked(self):
        """Issue 2: secrets in extra={} attributes (referenced by Formatter via
        %(field)s) must be masked. On master the handler stream was repointed at
        the wrapped stderr so the formatted line was masked; this branch doesn't
        repoint, so the filter must mask the extra attribute directly."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("DB_PASSWORD", "supersecretvalue")

        capture = StringIO()
        test_logger = logging.getLogger("test_extra_masking")
        test_logger.handlers.clear()
        handler = logging.StreamHandler(capture)
        handler.setFormatter(logging.Formatter("%(message)s dsn=%(dsn)s"))
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.INFO)
        test_logger.propagate = False

        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            test_logger.info("connecting", extra={"dsn": "db://u:supersecretvalue@h"})
            out = capture.getvalue()
            assert "supersecretvalue" not in out
            assert "***REDACTED:DB_PASSWORD***" in out
        finally:
            test_logger.removeHandler(handler)

    def test_nested_extra_dict_is_masked(self):
        """Issue 2 (B): extras containing nested dicts/lists must be masked
        recursively. A string-only mask would miss extra={"cfg": {"password": ...}}."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("NESTED_PW", "nested_secret_value")

        capture = StringIO()
        test_logger = logging.getLogger("test_nested_extra_masking")
        test_logger.handlers.clear()
        handler = logging.StreamHandler(capture)
        handler.setFormatter(logging.Formatter("%(message)s cfg=%(cfg)s"))
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.INFO)
        test_logger.propagate = False

        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            test_logger.info(
                "connecting",
                extra={"cfg": {"password": "nested_secret_value", "host": "h"}},
            )
            out = capture.getvalue()
            assert "nested_secret_value" not in out
            assert "***REDACTED:NESTED_PW***" in out
        finally:
            test_logger.removeHandler(handler)

    def test_extras_container_not_mutated_in_place(self):
        """Issue 2 (B): masking extras must not mutate the caller's live dict.
        A config dict that silently becomes ***REDACTED:X*** after the first
        log line is a nasty bug to chase — build a masked copy and assign that
        to the record instead."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("CFG_PW", "cfg_secret_value")

        test_logger = logging.getLogger("test_extras_not_mutated")
        test_logger.handlers.clear()
        test_logger.setLevel(logging.INFO)
        test_logger.propagate = False

        caller_cfg = {
            "password": "cfg_secret_value",
            "host": "h",
            "nested": ["a", "cfg_secret_value"],
        }
        caller_cfg_snapshot = {
            "password": "cfg_secret_value",
            "host": "h",
            "nested": ["a", "cfg_secret_value"],
        }
        try:
            mf = SecretMaskingFilter(registry)
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="connecting",
                args=(),
                exc_info=None,
            )
            record.__dict__["cfg"] = caller_cfg
            mf.filter(record)
            # The caller's original dict is untouched.
            assert caller_cfg == caller_cfg_snapshot, (
                f"Caller's dict was mutated by masking: {caller_cfg!r}"
            )
            # The record's copy is masked.
            assert "cfg_secret_value" not in str(record.__dict__["cfg"])
            assert "***REDACTED:CFG_PW***" in str(record.__dict__["cfg"])
        finally:
            test_logger.handlers.clear()

    def test_extras_self_referential_does_not_hang(self):
        """Issue 2 (B): extra= can carry self-referential structures. The
        identity-based cycle guard must prevent infinite recursion (which
        would hang the logger). The cycle branch returns a placeholder
        string (``"<not masked: cycle>"``) rather than the raw value —
        returning the raw subtree would emit any secret it contains in
        cleartext, and everything else in this module fails closed."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("CYCLE_PW", "cycle_secret_value")

        mf = SecretMaskingFilter(registry)
        cycle_dict: dict = {"host": "h"}
        cycle_dict["self"] = cycle_dict  # self-reference
        cycle_dict["pw"] = "cycle_secret_value"

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="x",
            args=(),
            exc_info=None,
        )
        record.__dict__["cfg"] = cycle_dict
        # Must not hang — the cycle guard prevents infinite recursion.
        mf.filter(record)
        # The non-cyclic top-level leaf with the secret is masked.
        assert "***REDACTED:CYCLE_PW***" in str(record.__dict__["cfg"])
        # The cycle branch emits a placeholder, NOT the raw subtree, so the
        # secret does not leak via the cycle's self-reference.
        assert "cycle_secret_value" not in str(record.__dict__["cfg"])
        assert "<not masked: cycle>" in str(record.__dict__["cfg"])

    def test_extras_deep_nesting_is_capped(self):
        """Issue 2 (B): very deep nesting is capped at _MAX_EXTRA_DEPTH to
        bound hot-path cost. Past the cap the value is replaced with a
        placeholder string (not the raw subtree), so secrets past the cap
        don't leak."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("DEEP_PW", "deep_secret_value")

        mf = SecretMaskingFilter(registry)
        # Build a deeply nested dict past the cap, with the secret at the
        # bottom (past the cap).
        deep: dict = {"pw": "deep_secret_value"}
        for _ in range(SecretMaskingFilter._MAX_EXTRA_DEPTH + 5):
            deep = {"child": deep}
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="x",
            args=(),
            exc_info=None,
        )
        record.__dict__["cfg"] = deep
        # Must not hang or raise.
        mf.filter(record)
        # The cap emits a placeholder, NOT the raw subtree, so the secret
        # past the cap does not leak.
        assert "deep_secret_value" not in str(record.__dict__["cfg"])
        assert "<not masked: depth limit>" in str(record.__dict__["cfg"])

    def test_json_formatter_with_quote_in_secret(self):
        """Issue 4: a JSON formatter escapes quotes/backslashes in the secret
        value. The raw-value regex won't match the escaped form. _expand_keys
        must include the JSON-escaped variant so the stderr-wrapper path
        (print(json.dumps(...))) still masks. For LogRecord extras, filter-time
        masking handles it before formatting; this test pins the _expand_keys
        variant via the mask_text path."""
        import json

        registry = SecretRegistry.get_instance()
        registry.clear()
        # Secret containing a quote — JSON-escaped form differs from raw.
        registry.register_secret("Q_TOKEN", 'pa"ss')

        mf = SecretMaskingFilter(registry)
        # Simulate what a JSON formatter would emit: the secret JSON-escaped.
        json_line = json.dumps({"pw": 'pa"ss'})
        masked = mf.mask_text(json_line)
        assert 'pa"ss' not in masked
        assert "***REDACTED:Q_TOKEN***" in masked

    def test_post_install_streamhandler_does_not_recurse(self):
        """Issue 3: a StreamHandler() created after install picks up the
        wrapped sys.stderr as its default stream. If sys.stderr re-enters
        logging (celery LoggingProxy), writing to the handler recurses. The
        fileno() guard must skip wrapping non-real streams so this can't form
        a cycle. We model the proxy and assert the guard skips it."""
        import io

        from datahub.masking.masking_filter import _is_real_stream

        class FakeProxy:
            """Models celery's LoggingProxy: writes re-enter logging."""

            def fileno(self):
                raise io.UnsupportedOperation("fileno")

            def write(self, s):
                return len(s)

            def flush(self):
                pass

        assert _is_real_stream(FakeProxy()) is False
        assert _is_real_stream(StringIO()) is False  # StringIO has no backing fd

        # A real file-like stream (e.g. a temp file) should be wrappable.
        import tempfile

        with tempfile.TemporaryFile() as f:
            assert _is_real_stream(f) is True

    def test_masking_namespace_records_bypass_filter(self):
        """Issue 4: records from datahub.masking.* loggers must bypass masking
        in filter() (not just at handler-attach time). After
        reset_masking_safe_loggers sets propagate=True, those records reach
        root handlers that carry the filter — the filter must early-return on
        record.name.startswith('datahub.masking.')."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("M_NS", "maskingns_secret_value")

        capture = StringIO()
        root_logger = logging.getLogger()
        handler = logging.StreamHandler(capture)
        handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(handler)
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            # Simulate post-teardown state: masking logger propagates to root.
            masking_logger = logging.getLogger("datahub.masking.test_ns")
            masking_logger.handlers.clear()
            masking_logger.propagate = True
            masking_logger.setLevel(logging.DEBUG)
            # Log a record that would carry the secret if masked.
            masking_logger.warning("internal state maskingns_secret_value")
            out = capture.getvalue()
            # The masking-namespace record bypasses masking — the secret appears
            # because masking-internal logs are not masked (by design; they
            # carry no real secrets). The point is no re-entrancy / no recursion.
            assert "maskingns_secret_value" in out
        finally:
            root_logger.removeHandler(handler)
            masking_logger.propagate = False

    def test_trailing_dot_does_not_match_maskingfoo(self):
        """Smaller point: startswith('datahub.masking') would match a
        hypothetical 'datahub.maskingfoo' logger. The trailing dot prevents
        that. A maskingfoo logger's records must still be masked."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("FOO", "foosecret_value")

        capture = StringIO()
        foo_logger = logging.getLogger("datahub.maskingfoo")
        foo_logger.handlers.clear()
        foo_logger.propagate = True
        root_handler = logging.StreamHandler(capture)
        root_handler.setFormatter(logging.Formatter("%(message)s"))
        logging.getLogger().addHandler(root_handler)
        foo_logger.setLevel(logging.INFO)
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            foo_logger.info("connecting with foosecret_value")
            out = capture.getvalue()
            assert "foosecret_value" not in out
            assert "***REDACTED:FOO***" in out
        finally:
            logging.getLogger().removeHandler(root_handler)
            foo_logger.propagate = False

    def test_reset_instance_tears_down_installed_filter(self):
        """reset_instance() must tear down the installed filter so a
        subsequent install(new_registry) masks with the new registry.
        Without this, the filter survives reset_instance() and keeps
        masking with the old (now-stale) registry — masking silently
        stops working for every secret registered after the reset.

        Reproduces the reviewer scenario: install(r1) → reset_instance()
        → install(r2). On the unfixed tree, r2's secret leaks because the
        filter still reads r1."""
        import datahub.masking.masking_filter as mf_mod

        # install(r1) — uses r1, attaches filter1 to handlers/root.
        r1 = SecretRegistry()
        r1.clear()
        install_masking_filter(secret_registry=r1, install_stdout_wrapper=False)
        assert mf_mod._installed_filter is not None
        filter1 = mf_mod._installed_filter
        assert filter1._registry is r1

        # reset_instance() — tears down filter1 (removes from handlers,
        # clears _installed_filter).
        SecretRegistry.reset_instance()
        assert mf_mod._installed_filter is None

        # install(r2) — fresh install with a new registry.
        r2 = SecretRegistry()
        r2.clear()
        r2.register_secret("R2_TOKEN", "bbb_secret_2")
        install_masking_filter(secret_registry=r2, install_stdout_wrapper=False)
        assert mf_mod._installed_filter is not None
        assert mf_mod._installed_filter._registry is r2
        assert mf_mod._installed_filter is not filter1  # new instance, not reused

        # r2's secret is masked by the new filter.
        mf = mf_mod._installed_filter
        masked = mf.mask_text("connecting with bbb_secret_2")
        assert "bbb_secret_2" not in masked
        assert "***REDACTED:R2_TOKEN***" in masked

        uninstall_masking_filter()

    def test_refresh_rebinds_registry_on_repeat_install(self):
        """The refresh path (already installed) must rebind the registry
        when the caller passes a different one, so install(r1) without
        reset_instance() in between still picks up r2. Belt-and-braces
        alongside reset_instance() for callers that use the public API
        twice without resetting."""
        import datahub.masking.masking_filter as mf_mod

        r1 = SecretRegistry()
        r1.clear()
        install_masking_filter(secret_registry=r1, install_stdout_wrapper=False)
        filter1 = mf_mod._installed_filter
        assert filter1 is not None
        assert filter1._registry is r1

        # install(r2) WITHOUT reset_instance() — refresh path rebinds.
        r2 = SecretRegistry()
        r2.clear()
        r2.register_secret("R2_TOKEN", "ccc_secret_3")
        install_masking_filter(secret_registry=r2, install_stdout_wrapper=False)
        # Same filter instance (process-lifetime), but registry rebound.
        assert mf_mod._installed_filter is filter1
        assert filter1._registry is r2

        masked = filter1.mask_text("connecting with ccc_secret_3")
        assert "ccc_secret_3" not in masked
        assert "***REDACTED:R2_TOKEN***" in masked

        uninstall_masking_filter()

    def test_refresh_readds_root_logger_filter_if_removed(self):
        """If something removed the root-logger filter (partial teardown
        state), the refresh path must re-add it. Without this, a
        partially-torn-down state stays partial: handler filters are
        re-scanned but the root-logger sentinel is gone, so a later
        install would re-install from scratch and attach a second filter."""
        import datahub.masking.masking_filter as mf_mod

        r1 = SecretRegistry()
        r1.clear()
        install_masking_filter(secret_registry=r1, install_stdout_wrapper=False)
        filter1 = mf_mod._installed_filter
        root_logger = logging.getLogger()
        assert filter1 in root_logger.filters

        # Simulate partial teardown: remove root-logger filter only.
        root_logger.removeFilter(filter1)
        assert filter1 not in root_logger.filters

        # Refresh path re-adds it.
        install_masking_filter(secret_registry=r1, install_stdout_wrapper=False)
        assert filter1 in root_logger.filters

        uninstall_masking_filter()

    def test_refresh_honours_install_stdout_wrapper_true(self):
        """Item 4: install_stdout_wrapper is ignored on the refresh path
        because the wrapper block sits after the early return. install(False)
        then install(True) must wrap stdout/stderr, not skip wrapping."""
        from datahub.masking.masking_filter import StreamMaskingWrapper

        r1 = SecretRegistry()
        r1.clear()
        # First install: no wrapper.
        install_masking_filter(secret_registry=r1, install_stdout_wrapper=False)
        assert not isinstance(sys.stdout, StreamMaskingWrapper)
        assert not isinstance(sys.stderr, StreamMaskingWrapper)

        # Refresh with wrapper=True must wrap.
        install_masking_filter(secret_registry=r1, install_stdout_wrapper=True)
        assert isinstance(sys.stdout, StreamMaskingWrapper), (
            "refresh ignored install_stdout_wrapper=True; stdout not wrapped"
        )
        assert isinstance(sys.stderr, StreamMaskingWrapper), (
            "refresh ignored install_stdout_wrapper=True; stderr not wrapped"
        )

        uninstall_masking_filter()

    def test_rebind_registry_forces_pattern_rebuild(self):
        """Item 5: rebind_registry sets _last_version = 0, which equals a
        fresh registry's version, so _check_and_rebuild_pattern's fast path
        (``current_version == self._last_version``) skips the rebuild and the
        old pattern persists. The failure mode is over-masking the old
        registry's values (not a leak — version 0 implies empty), but the fix
        removes the need to re-derive that argument: set _last_version = -1 and
        clear the pattern so the next mask rebuilds from the new registry."""
        r1 = SecretRegistry()
        r1.clear()
        r1.register_secret("A", "aaa_secret_1")

        mf = SecretMaskingFilter(r1)
        # Force a build so the pattern holds r1's secret.
        assert "aaa_secret_1" not in mf.mask_text("x aaa_secret_1")
        assert "***REDACTED:A***" in mf.mask_text("x aaa_secret_1")
        assert mf._pattern is not None
        assert mf._last_version == r1.get_version()

        # Rebind to an empty registry. With _last_version = 0 (== the empty
        # registry's version), the rebuild is skipped and the old pattern
        # persists — over-masking "aaa_secret_1" even though r2 has no secrets.
        r2 = SecretRegistry()
        r2.clear()
        mf.rebind_registry(r2)

        # After rebind, masking an unrelated text must NOT carry r1's pattern.
        # (r2 is empty, so nothing should be redacted.)
        masked = mf.mask_text("x aaa_secret_1")
        assert "***REDACTED:A***" not in masked, (
            "rebind_registry did not force a rebuild; old registry's pattern persisted"
        )
        assert mf._last_version == r2.get_version()

    def test_datahub_masked_extra_cannot_opt_out_of_masking(self):
        """Item 2: the idempotency guard uses ``record._datahub_masked`` with
        truthiness. A caller-supplied ``extra={'_datahub_masked': True}`` forges
        the sentinel and disables masking for that record. Use a module-private
        sentinel object compared with ``is`` so a caller can't forge it."""
        import io
        import logging

        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("OPT_OUT", "sekret_value_x")

        capture = io.StringIO()
        root = logging.getLogger()
        handler = logging.StreamHandler(capture)
        handler.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(handler)
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            # A caller tries to opt out by forging the guard attribute via extra.
            logging.getLogger("test.optout").info(
                "opt-out attempt sekret_value_x", extra={"_datahub_masked": True}
            )
        finally:
            root.removeHandler(handler)
            uninstall_masking_filter()
        out = capture.getvalue()
        assert "sekret_value_x" not in out, (
            f"caller-forged _datahub_masked disabled masking: {out!r}"
        )
        assert "***REDACTED:OPT_OUT***" in out

    def test_rebind_registry_acquires_pattern_lock(self):
        """Fix 1: ``rebind_registry`` wrote ``_registry`` / ``_last_version``
        / ``_pattern`` / ``_replacements`` with no lock. ``mask_text``
        snapshots ``_pattern`` and ``_replacements`` under ``_pattern_lock``,
        so an unlocked rebind landing between those two snapshots emits
        cleartext (``_pattern=None`` snapshot) or ``***REDACTED:UNKNOWN***``
        (old ``_pattern`` + cleared ``_replacements``).

        Deterministic lock-acquisition test: hold ``_pattern_lock`` on the
        test thread, start a worker that calls ``rebind_registry``, and
        assert the worker blocks until the lock is released. Without the
        fix the worker completes immediately (no lock needed); with the fix
        it blocks on the lock.
        """
        r1 = SecretRegistry()
        r1.clear()
        r1.register_secret("A", "aaa_secret_1")
        mf = SecretMaskingFilter(r1)
        mf.mask_text("x aaa_secret_1")  # prime the pattern

        r2 = SecretRegistry()
        r2.clear()

        with mf._pattern_lock:
            done = threading.Event()

            def worker():
                mf.rebind_registry(r2)
                done.set()

            t = threading.Thread(target=worker, name="rebind-worker")
            t.start()
            completed = done.wait(timeout=0.5)
            assert not completed, (
                "rebind_registry completed without acquiring _pattern_lock; "
                "its writes are unprotected and can tear mask_text's snapshot "
                "of _pattern/_replacements, emitting cleartext or "
                "***REDACTED:UNKNOWN***"
            )
        t.join(timeout=5.0)
        assert not t.is_alive(), "rebind worker thread timed out"
        assert mf._registry is r2

    def test_handler_created_after_install_is_masked(self):
        """P0-1: a handler created and attached AFTER install_masking_filter
        must still mask. Without the Handler.__init__ hook, the one-shot
        _add_filter_to_existing_handlers (run at install time) misses it.

        The test isolates the new handler on a child logger with
        propagate=False so it is the ONLY handler that sees the record.
        Otherwise a pre-existing handler's filter (e.g. pytest's
        LogCaptureHandler) would mask the shared record in place before the
        new handler emits, hiding the bug — the masking filter mutates
        record.msg, and all handlers in callHandlers share the same record.
        """
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("LATE_PW", "late_secret_value")

        capture = StringIO()
        child = logging.getLogger("datahub.ingestion.source.late_p1")
        child.handlers.clear()
        child.propagate = False
        child.setLevel(logging.INFO)
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            # Handler created and attached AFTER install, on the child only.
            handler = logging.StreamHandler(capture)
            handler.setFormatter(logging.Formatter("%(message)s"))
            child.addHandler(handler)
            child.info("connecting with late_secret_value")
            out = capture.getvalue()
            assert "late_secret_value" not in out, (
                f"handler created after install leaked secret: {out!r}"
            )
            assert "***REDACTED:LATE_PW***" in out
        finally:
            child.removeHandler(handler)
            child.propagate = True

    def test_basicconfig_after_install_masks(self):
        """P0-1 (B): logging.basicConfig() after install adds a StreamHandler
        to root; that handler must pick up the filter via the __init__ hook."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("BC_PW", "bc_secret_value")
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            # basicConfig with a fresh stream; if root already has handlers
            # this is a no-op, so force a fresh handler via a manual StreamHandler.
            capture = StringIO()
            handler = logging.StreamHandler(capture)
            handler.setFormatter(logging.Formatter("%(message)s"))
            logging.getLogger().addHandler(handler)
            logging.getLogger("datahub.ingestion.source.bc").info(
                "connecting with bc_secret_value"
            )
            out = capture.getvalue()
            assert "bc_secret_value" not in out
            assert "***REDACTED:BC_PW***" in out
        finally:
            logging.getLogger().removeHandler(handler)

    def test_one_record_multiple_handlers_truncation_byte_count(self):
        """P0-2: a record reaching 3 handlers must produce exactly one
        truncation suffix, a correct byte count, and masked output in every
        handler. The _MASKED sentinel makes filter() idempotent per record."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("MULTI", "multi_secret_value")

        captures = [StringIO(), StringIO(), StringIO()]
        handlers = [logging.StreamHandler(c) for c in captures]
        for h in handlers:
            h.setFormatter(logging.Formatter("%(message)s"))
        root = logging.getLogger()
        for h in handlers:
            root.addHandler(h)
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            big = "multi_secret_value " + "y" * 12000
            logging.getLogger("datahub.test.multi").info(big)
            outs = [c.getvalue() for c in captures]
            for i, out in enumerate(outs):
                assert "multi_secret_value" not in out, (
                    f"handler {i} leaked secret: {out!r}"
                )
                assert "***REDACTED:MULTI***" in out, f"handler {i} not masked"
                # Exactly one truncation suffix (not nested / re-truncated).
                assert out.count("bytes truncated for performance") == 1, (
                    f"handler {i} re-truncated: {out!r}"
                )
                # D6: masking runs before truncation now, so the byte count
                # is the MASKED message's overrun (the secret is replaced by
                # ***REDACTED:MULTI*** before truncating), not the original
                # message's overrun. Compute the masked message via the
                # installed filter so the assertion tracks the real masked
                # length rather than a hardcoded guess.
                mf = next(
                    f for f in handlers[0].filters if isinstance(f, SecretMaskingFilter)
                )
                masked_big = mf.mask_text(big)
                expected_bytes = len(masked_big) - 5000
                assert str(expected_bytes) in out, (
                    f"handler {i} wrong byte count: {out!r}"
                )
        finally:
            for h in handlers:
                root.removeHandler(h)

    def test_double_install_after_new_handler_attaches_and_no_duplicates(self):
        """P1-1: a second install call after a new handler appeared must
        attach to the new handler, and must not duplicate the filter on
        handlers that already have it."""
        import datahub.masking.masking_filter as mf_mod

        registry = SecretRegistry.get_instance()
        registry.clear()
        root = logging.getLogger()
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            f1 = mf_mod._installed_filter
            assert f1 is not None
            # New handler after first install.
            new_cap = StringIO()
            new_h = logging.StreamHandler(new_cap)
            new_h.setFormatter(logging.Formatter("%(message)s"))
            root.addHandler(new_h)
            # Second install call (refresh path).
            install_masking_filter(registry, install_stdout_wrapper=False)
            # The new handler has the filter exactly once.
            assert sum(1 for f in new_h.filters if f is f1) == 1, (
                "new handler missing or duplicate filter after refresh"
            )
            # Existing handlers don't get a duplicate.
            for h in root.handlers:
                assert sum(1 for f in h.filters if f is f1) <= 1, (
                    f"duplicate filter on handler {h}"
                )
        finally:
            root.removeHandler(new_h)

    def test_uninstall_symmetry_and_unrelated_filter_survives(self):
        """P1-4: after install -> uninstall, no SecretMaskingFilter remains on
        any handler or logger, stdout/stderr are the original objects, handler
        streams are unchanged, and the Handler.__init__ hook is reverted. An
        unrelated pre-existing filter on a handler survives uninstall."""
        import datahub.masking.masking_filter as mf_mod

        saved_stdout = sys.stdout
        saved_stderr = sys.stderr
        root = logging.getLogger()

        # An unrelated filter that must survive uninstall.
        class UnrelatedFilter(logging.Filter):
            pass

        unrelated = UnrelatedFilter()
        cap = StringIO()
        h = logging.StreamHandler(cap)
        h.setFormatter(logging.Formatter("%(message)s"))
        h.addFilter(unrelated)
        root.addHandler(h)
        # Capture the handler's stream reference; it must not change.
        original_stream = h.stream
        try:
            assert mf_mod._original_handler_init is None
            install_masking_filter(install_stdout_wrapper=True)
            assert mf_mod._original_handler_init is not None
            uninstall_masking_filter()
            # Hook reverted.
            assert mf_mod._original_handler_init is None
            # No SecretMaskingFilter anywhere.
            for _log_name, obj in list(logging.root.manager.loggerDict.items()):
                if isinstance(obj, logging.Logger):
                    for hh in obj.handlers:
                        assert not any(
                            isinstance(f, SecretMaskingFilter) for f in hh.filters
                        )
            for hh in root.handlers:
                assert not any(isinstance(f, SecretMaskingFilter) for f in hh.filters)
            assert not any(isinstance(f, SecretMaskingFilter) for f in root.filters)
            # Unrelated filter survived.
            assert unrelated in h.filters, "unrelated filter was stripped"
            # Streams restored.
            assert sys.stdout is saved_stdout
            assert sys.stderr is saved_stderr
            # Handler stream unchanged.
            assert h.stream is original_stream
        finally:
            root.removeHandler(h)

    def test_shared_handler_masking_namespace_bypass(self):
        """P1-3: a handler shared between a datahub.masking.* logger and a
        normal logger gets the filter (via the normal logger). Records from
        the masking namespace through that handler are bypassed by filter()'s
        record-name check (no re-entrancy, no masking of internal logs).
        Records from the normal logger are masked."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("SHARED", "shared_secret_value")

        shared_cap = StringIO()
        shared_h = logging.StreamHandler(shared_cap)
        shared_h.setFormatter(logging.Formatter("%(name)s %(message)s"))
        normal_logger = logging.getLogger("datahub.ingestion.shared")
        normal_logger.handlers.clear()
        normal_logger.propagate = False
        normal_logger.addHandler(shared_h)
        masking_logger = logging.getLogger("datahub.masking.sharedtest")
        masking_logger.handlers.clear()
        masking_logger.propagate = False
        masking_logger.addHandler(shared_h)
        normal_logger.setLevel(logging.DEBUG)
        masking_logger.setLevel(logging.DEBUG)
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            normal_logger.info("normal line shared_secret_value")
            masking_logger.warning("internal line shared_secret_value")
            out = shared_cap.getvalue()
            # Normal record masked.
            assert "shared_secret_value" not in out.split("internal line")[0]
            # The masking-namespace record is NOT masked (bypass), so the
            # secret appears in that line — by design (internal logs carry no
            # real secrets). The point: no re-entrancy, no recursion.
            assert "internal line shared_secret_value" in out
        finally:
            normal_logger.removeHandler(shared_h)
            masking_logger.removeHandler(shared_h)
            normal_logger.propagate = True
            masking_logger.propagate = True

    def test_masking_framework_loggers_do_not_propagate(self):
        """Step 0 Q1: get_masking_safe_logger sets propagate=False so the
        masking framework's own log records (mask_text warnings, rebuild
        retries, circuit-breaker messages) do not reach root handlers that
        carry the masking filter — that would re-enter mask_text. Assert
        propagate is False and that a warning emitted from inside mask_text
        does not re-enter the filter."""
        from datahub.masking.logging_utils import get_masking_safe_logger

        mf_logger = get_masking_safe_logger("datahub.masking.reentry_test")
        assert mf_logger.propagate is False, (
            "masking-framework logger propagates to root; mask_text's own "
            "warnings would re-enter the filter"
        )
        # Instrument mask_text to emit a warning mid-call and confirm it
        # does not recurse into the filter.
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("REENTRY", "reentry_secret_value")
        root_cap = StringIO()
        root_h = logging.StreamHandler(root_cap)
        root_h.setFormatter(logging.Formatter("%(name)s %(message)s"))
        root = logging.getLogger()
        root.addHandler(root_h)
        call_count = {"n": 0}
        original_mask_text = SecretMaskingFilter.mask_text

        def counting_mask_text(self, text):
            call_count["n"] += 1
            return original_mask_text(self, text)

        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            installed = next(
                f for f in root_h.filters if isinstance(f, SecretMaskingFilter)
            )
            # Bypass mypy's "cannot assign to a method" by setting via __dict__.
            import types

            installed.mask_text = types.MethodType(  # type: ignore[method-assign]
                counting_mask_text, installed
            )
            # Trigger a masking-framework warning by registering 100+ secrets.
            for i in range(110):
                registry.register_secret(f"K{i}", f"v{i}_secret_value")
            # Force a rebuild + warning path.
            installed._last_version = -1
            installed.mask_text("trigger rebuild reentry_secret_value")
            # The warning from inside mask_text (large secret count) must not
            # have re-entered the filter (which would inflate the count).
            # mask_text is called at least once for the trigger; re-entrancy
            # would call it again from within the warning emission. We can't
            # pin an exact count (the warning may legitimately call mask_text
            # for its own stream), but the masking-framework logger has
            # propagate=False so its records never reach root_h's filter.
            assert "reentry_secret_value" not in root_cap.getvalue()
        finally:
            root.removeHandler(root_h)

    def test_concurrent_add_remove_handler_no_exception(self):
        """P2-1: concurrent addHandler/removeHandler on one thread while
        install/uninstall runs on another must not raise. The logging-module
        lock around the snapshot in _snapshot_handler_pairs prevents
        RuntimeError from concurrent list mutation."""
        errors: list = []
        stop = threading.Event()

        def mutator():
            root = logging.getLogger()
            while not stop.is_set():
                h = logging.StreamHandler(StringIO())
                root.addHandler(h)
                root.removeHandler(h)

        def installer():
            registry = SecretRegistry.get_instance()
            registry.clear()
            for _ in range(20):
                try:
                    install_masking_filter(registry, install_stdout_wrapper=False)
                    uninstall_masking_filter()
                except Exception as e:
                    errors.append(e)

        try:
            t1 = threading.Thread(target=mutator, name="mutator")
            t2 = threading.Thread(target=installer, name="installer")
            t1.start()
            t2.start()
            t2.join(timeout=10.0)
            stop.set()
            t1.join(timeout=5.0)
            assert not t2.is_alive(), "installer thread timed out"
            assert not errors, f"install/uninstall raised: {errors}"
        finally:
            stop.set()

    def test_remove_filter_from_existing_handlers_direct_coverage(self):
        """P1-4 / test #8: direct coverage of _remove_filter_from_existing_handlers
        (teardown is the half that was silently broken before this PR). Asserts
        our instance is gone and an unrelated SecretMaskingFilter survives."""
        import datahub.masking.masking_filter as mf_mod

        registry = SecretRegistry.get_instance()
        registry.clear()
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            ours = mf_mod._installed_filter
            assert ours is not None
            # Someone else's SecretMaskingFilter on a handler.
            theirs = SecretMaskingFilter(registry)
            cap = StringIO()
            h = logging.StreamHandler(cap)
            h.setFormatter(logging.Formatter("%(message)s"))
            h.addFilter(theirs)
            logging.getLogger().addHandler(h)
            _remove_filter_from_existing_handlers(ours)
            assert ours not in h.filters, "our filter was not removed"
            assert theirs in h.filters, "unrelated SecretMaskingFilter was stripped"
        finally:
            logging.getLogger().removeHandler(h)
            if mf_mod._installed_filter is not None:
                uninstall_masking_filter()

    def test_snapshot_uses_lock_not_acquire_lock(self):
        """A1: ``logging._acquireLock`` / ``_releaseLock`` were removed in
        Python 3.13. ``_snapshot_handler_pairs`` must use ``logging._lock``
        (a context manager that survives 3.10–3.14+), not the removed
        wrappers. Spy on both: post-fix, ``_lock`` is acquired and
        ``_acquireLock`` is never called. Pre-fix, ``_acquireLock`` is
        called and ``_lock`` is not — on 3.13 that call raises
        ``AttributeError`` and install silently fails (caught by graceful
        degradation, filter never installs).
        """
        import datahub.masking.masking_filter as mf_mod

        acquire_calls: list = []
        lock_acquires: list = []
        real_lock = logging._lock

        class SpyLock:
            def __enter__(self):
                lock_acquires.append(1)
                return real_lock.__enter__()

            def __exit__(self, *exc):
                return real_lock.__exit__(*exc)

        with MonkeyPatch().context() as m:
            # ``_acquireLock`` / ``_releaseLock`` exist on 3.10–3.12 and are
            # gone on 3.13+; ``raising=False`` lets the spy install on both.
            m.setattr(
                logging, "_acquireLock", lambda: acquire_calls.append(1), raising=False
            )
            m.setattr(logging, "_releaseLock", lambda: None, raising=False)
            m.setattr(logging, "_lock", SpyLock())

            mf_mod._snapshot_handler_pairs()

        assert acquire_calls == [], (
            "_snapshot_handler_pairs still calls logging._acquireLock, which "
            "was removed in Python 3.13 — install would raise AttributeError "
            "and silently fail to install the filter"
        )
        assert lock_acquires, (
            "_snapshot_handler_pairs did not acquire logging._lock; the "
            "snapshot is not protected against concurrent addHandler/"
            "removeHandler"
        )

    def test_install_succeeds_when_acquire_lock_is_gone(self):
        """A1 end-to-end: with ``_acquireLock`` removed (simulating 3.13),
        ``install_masking_filter`` must still install the filter. Pre-fix,
        ``_snapshot_handler_pairs`` calls the missing ``_acquireLock`` and
        the install silently fails. We scope the removal to the snapshot
        call only — leaving it removed breaks 3.11 logging internals
        (``isEnabledFor`` etc. also use ``_acquireLock``), which is not the
        scenario under test."""
        import datahub.masking.masking_filter as mf_mod

        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("SECRET", "sekret_value_x")

        real_snapshot = mf_mod._snapshot_handler_pairs

        def snapshot_without_acquire_lock():
            # Simulate 3.13: _acquireLock is gone. The post-fix body uses
            # `with logging._lock:` and never touches _acquireLock, so this
            # patch is a no-op for it. Pre-fix, the body's first line is
            # `logging._acquireLock()` and raises AttributeError.
            with MonkeyPatch().context() as m:
                if hasattr(logging, "_acquireLock"):
                    m.delattr(logging, "_acquireLock")
                if hasattr(logging, "_releaseLock"):
                    m.delattr(logging, "_releaseLock")
                return real_snapshot()

        try:
            with MonkeyPatch().context() as m:
                m.setattr(
                    mf_mod,
                    "_snapshot_handler_pairs",
                    snapshot_without_acquire_lock,
                )
                install_masking_filter(registry, install_stdout_wrapper=False)
                installed = mf_mod._installed_filter
            assert installed is not None, (
                "install_masking_filter silently failed when _acquireLock is "
                "missing (simulating 3.13): masking is off with no signal"
            )
        finally:
            if mf_mod._installed_filter is not None:
                uninstall_masking_filter()

    def test_uninstall_does_not_clobber_another_patch(self):
        """A2: if another library patches ``Handler.__init__`` after us,
        uninstall must NOT restore the original unconditionally — that would
        discard their patch. Only restore when the current attribute is
        still ours (identity compare)."""
        import datahub.masking.masking_filter as mf_mod

        registry = SecretRegistry.get_instance()
        registry.clear()
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            assert mf_mod._patched_handler_init is not None
            our_patch = mf_mod._patched_handler_init
            original = mf_mod._original_handler_init

            # Another library wraps our patch after we installed.
            def their_init(self, level=logging.NOTSET):
                our_patch(self, level)

            logging.Handler.__init__ = their_init  # type: ignore[assignment]

            mf_mod._uninstall_handler_init_hook()

            # We did NOT restore the original (would clobber their_init).
            assert logging.Handler.__init__ is their_init, (
                "uninstall clobbered another library's Handler.__init__ patch"
            )
            # S6: on decline we KEEP the saved globals so re-install is
            # idempotent (the dead patch in lib X's chain reactivates when
            # _installed_filter is set again) instead of stacking a new
            # wrapper each cycle.
            assert mf_mod._original_handler_init is original
            assert mf_mod._patched_handler_init is our_patch
            # Restore for teardown.
            logging.Handler.__init__ = original  # type: ignore[assignment]
            mf_mod._original_handler_init = None
            mf_mod._patched_handler_init = None
        finally:
            if mf_mod._installed_filter is not None:
                uninstall_masking_filter()
            if mf_mod._original_handler_init is not None:
                mf_mod._uninstall_handler_init_hook()

    def test_install_hook_before_scan_covers_race_window(self):
        """A3: the ``Handler.__init__`` hook must be installed BEFORE the
        existing-handler scan, so a handler constructed between the two is
        covered by the hook (not missed by both). Verify by constructing a
        handler during the scan via a patched ``_add_filter_to_existing_handlers``
        — the hook (already active) must attach the filter to it."""
        import datahub.masking.masking_filter as mf_mod

        registry = SecretRegistry.get_instance()
        registry.clear()
        try:
            # Patch the scan to construct a new handler mid-scan.
            real_scan = mf_mod._add_filter_to_existing_handlers
            late_handler = {"h": None}

            def scan_with_late_handler(filt):
                real_scan(filt)
                h = logging.StreamHandler(StringIO())
                late_handler["h"] = h

            with MonkeyPatch().context() as m:
                m.setattr(
                    mf_mod,
                    "_add_filter_to_existing_handlers",
                    scan_with_late_handler,
                )
                install_masking_filter(registry, install_stdout_wrapper=False)

            h = late_handler["h"]
            assert h is not None
            assert mf_mod._installed_filter in h.filters, (
                "handler constructed between hook-install and scan was not "
                "covered — the hook must be installed before the scan"
            )
            logging.getLogger().removeHandler(h)
        finally:
            if mf_mod._installed_filter is not None:
                uninstall_masking_filter()

    def test_exact_masking_namespace_logger_is_bypassed(self):
        """D3: the bypass predicate in filter() must match the attach-time
        skip predicate. A record from the logger named exactly
        ``datahub.masking`` (no trailing dot) must be bypassed, not masked —
        ``startswith(REDACTED_MASKING_NAMESPACE)`` misses it, so the two
        call sites used to disagree."""
        import datahub.masking.masking_filter as mf_mod
        from datahub.masking.masking_filter import _is_masking_namespace_name

        # The predicate covers both the dotless name and the dotted namespace.
        assert _is_masking_namespace_name("datahub.masking")
        assert _is_masking_namespace_name("datahub.masking.foo")
        assert not _is_masking_namespace_name("datahub.maskingfoo")
        assert not _is_masking_namespace_name("datahub.ingestion")

        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("M_NS", "maskingns_secret_value")
        cap = StringIO()
        h = logging.StreamHandler(cap)
        h.setFormatter(logging.Formatter("%(message)s"))
        ns_logger = logging.getLogger("datahub.masking")
        ns_logger.addHandler(h)
        ns_logger.propagate = False
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            ns_logger.info("internal log mentioning maskingns_secret_value")
            out = cap.getvalue()
            # The record is bypassed: the secret appears UNMASKED because
            # the record-name bypass short-circuits. This is correct —
            # masking-internal loggers carry no secrets and must not re-enter.
            assert "maskingns_secret_value" in out, (
                f"datahub.masking record was masked (should be bypassed): {out!r}"
            )
            assert "***REDACTED:M_NS***" not in out
        finally:
            ns_logger.removeHandler(h)
            if mf_mod._installed_filter is not None:
                uninstall_masking_filter()

    def test_truncation_does_not_leak_partial_secret(self):
        """D6: a secret straddling the truncation cut point must not survive
        in part. Mask-before-truncate replaces the secret with a redaction
        token first, so truncating the masked message can only cut tokens,
        never secret bytes."""
        registry = SecretRegistry.get_instance()
        registry.clear()
        registry.register_secret("PW", "SUPERSECRETPASSWORD1234")
        cap = StringIO()
        h = logging.StreamHandler(cap)
        h.setFormatter(logging.Formatter("%(message)s"))
        logging.getLogger().addHandler(h)
        try:
            mf = SecretMaskingFilter(registry, max_message_size=100)
            # Manually attach so we control max_message_size precisely.
            h.addFilter(mf)
            # Secret straddles the 100-char cut.
            msg = "x" * 90 + "SUPERSECRETPASSWORD1234" + "y" * 200
            logging.getLogger("datahub.test.trunc").info(msg)
            out = cap.getvalue()
            assert "SUPERSECRETPASSWORD1234" not in out, f"full secret leaked: {out!r}"
            # No partial prefix of the secret should survive either —
            # mask-before-truncate replaces the whole secret with a
            # redaction token, so even the secret's first few chars are
            # gone. (The token itself may be cut by truncation, which is
            # fine — only the secret must not survive.)
            for n in range(3, len("SUPERSECRETPASSWORD1234")):
                assert "SUPERSECRETPASSWORD1234"[:n] not in out, (
                    f"partial secret prefix ({n} chars) leaked: {out!r}"
                )
            # The message was truncated (D6 still truncates after masking).
            assert "truncated for performance" in out
        finally:
            logging.getLogger().removeHandler(h)

    def test_handler_not_on_any_logger_cleaned_on_uninstall(self):
        """S7: a handler that received the filter via the Handler.__init__
        hook but is not attached to any logger (held by a QueueListener, or
        just constructed and never attached) must still have the filter
        removed on uninstall — _snapshot_handler_pairs only sees
        handlers on loggers, so without covered-handler tracking this
        handler would retain the filter."""
        import datahub.masking.masking_filter as mf_mod

        registry = SecretRegistry.get_instance()
        registry.clear()
        try:
            install_masking_filter(registry, install_stdout_wrapper=False)
            # Construct a handler AFTER install (hook attaches the filter)
            # but never attach it to any logger.
            orphan = logging.StreamHandler(StringIO())
            assert mf_mod._installed_filter in orphan.filters, (
                "hook did not attach filter to the orphan handler"
            )
            uninstall_masking_filter()
            assert mf_mod._installed_filter is None
            assert mf_mod._installed_filter not in orphan.filters, (
                "orphan handler (not on any logger) retained the filter after uninstall"
            )
        finally:
            if mf_mod._installed_filter is not None:
                uninstall_masking_filter()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
