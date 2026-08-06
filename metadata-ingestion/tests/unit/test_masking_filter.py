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

from datahub.masking.masking_filter import (
    SecretMaskingFilter,
    StreamMaskingWrapper,
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
