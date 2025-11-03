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

from datahub.ingestion.masking.masking_filter import (
    SecretMaskingFilter,
    StreamMaskingWrapper,
    install_masking_filter,
    uninstall_masking_filter,
)
from datahub.ingestion.masking.secret_registry import SecretRegistry


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
        assert "{TEST_PASSWORD}" in record.msg

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
        assert "{PASSWORD}" in record.msg
        assert "{TOKEN}" in record.msg

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
        assert "{TOKEN}" in str(record.args)

    def test_dict_formatting(self, registry, masking_filter):
        """Test masking with dict formatting."""
        registry.register_secret("PASSWORD", "mypass")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Password is %(password)s",
            args={"password": "mypass"},
            exc_info=None,
        )

        masking_filter.filter(record)

        # Args dict should be masked
        assert "mypass" not in str(record.args)
        assert isinstance(record.args, dict)
        assert "{PASSWORD}" in str(record.args.get("password", ""))

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

        # Exception should be masked
        assert record.exc_info is not None
        _, exc_value, _ = record.exc_info
        assert "my_secret_value" not in str(exc_value)
        assert "{SECRET}" in str(exc_value)

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

        assert record.exc_info is not None
        _, exc_value, _ = record.exc_info
        exc_str = str(exc_value)
        assert "value1" not in exc_str
        assert "value2" not in exc_str


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
        assert "{PASSWORD}" in result

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
        with masking_filter._lock:
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
        assert "{SPECIAL}" in record.msg


class TestP1Fixes:
    """Test P1 critical fixes from production hardening review."""

    def test_pattern_rebuild_no_lock_contention(self):
        """
        P1 FIX #1: Verify pattern rebuild doesn't block concurrent logging.

        This test verifies that expensive regex compilation happens outside
        the lock, preventing blocking with 1000+ secrets.
        """
        import logging
        import time

        registry = SecretRegistry()
        masking_filter = SecretMaskingFilter(registry)

        # Register 1000 secrets
        for i in range(1000):
            registry.register_secret(f"SECRET_{i}", f"value_{i}_xxx")

        # Measure pattern rebuild time
        start = time.perf_counter()
        masking_filter._check_and_rebuild_pattern()
        rebuild_time = time.perf_counter() - start

        # Should complete in reasonable time
        assert rebuild_time < 0.1, (
            f"Rebuild too slow: {rebuild_time:.4f}s (expected <0.1s)"
        )

        # Test concurrent logging doesn't block
        log_times = []
        test_logger = logging.getLogger("test_concurrent")
        test_logger.addFilter(masking_filter)

        def log_message():
            start = time.perf_counter()
            test_logger.info("Test message with value_500_xxx")
            log_times.append(time.perf_counter() - start)

        # Trigger rebuild in background
        def trigger_rebuild():
            for i in range(1000, 1100):
                registry.register_secret(f"NEW_{i}", f"newval_{i}")

        rebuild_thread = threading.Thread(target=trigger_rebuild)
        log_threads = [threading.Thread(target=log_message) for _ in range(20)]

        rebuild_thread.start()
        for t in log_threads:
            t.start()

        rebuild_thread.join()
        for t in log_threads:
            t.join()

        # No log operation should be blocked for long
        max_log_time = max(log_times)
        # Allow up to 50ms for system variability (much better than seconds of blocking)
        assert max_log_time < 0.05, (
            f"Logging blocked: {max_log_time:.4f}s (expected <0.05s)"
        )

        # Cleanup
        test_logger.removeFilter(masking_filter)

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
        assert masked_text == "Password is {PASSWORD}", (
            f"Expected masked text, got: {masked_text}"
        )
        assert chars_written == len(masked_text), (
            f"Expected {len(masked_text)} chars, got {chars_written}"
        )
        # "Password is {PASSWORD}" = 12 + 10 = 22 chars
        assert chars_written == 22, (
            f"Expected 22 chars for 'Password is {{PASSWORD}}', got {chars_written}"
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
        import time

        registry = SecretRegistry()

        # Register 1000 secrets
        for i in range(1000):
            registry.register_secret(f"SECRET_{i}", f"value_{i}")

        # Lookup should be fast (O(1))
        start = time.perf_counter()
        for i in range(1000):
            value = registry.get_secret_value(f"SECRET_{i}")
            assert value == f"value_{i}", f"Expected value_{i}, got {value}"
        elapsed = time.perf_counter() - start

        # Should complete in < 10ms (O(1) lookups)
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
            masked = masking_filter._mask_text(match_text)
            assert masked == expected_masked, (
                f"{name}: Failed to mask literal. Expected '{expected_masked}', got '{masked}'"
            )

            # Should NOT over-match similar strings
            not_masked = masking_filter._mask_text(no_match_text)
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
        masked = masking_filter._mask_text(test_text)

        # Should NOT mask everything (wildcards should not act as regex)
        assert masked == test_text, (
            f"Wildcard acted as regex (security issue). Expected no masking, got: {masked}"
        )

        # Should only mask literal "secret.*"
        literal_text = "Password is secret.*"
        masked = masking_filter._mask_text(literal_text)
        assert masked == "Password is ***REDACTED:WILDCARD***", (
            f"Failed to mask literal 'secret.*': {masked}"
        )

        # Should NOT match "secretABC" (wildcard should not work as regex)
        not_matching = "Password is secretABC"
        masked2 = masking_filter._mask_text(not_matching)
        assert masked2 == not_matching, (
            f"Wildcard acted as regex (should not match 'secretABC'): {masked2}"
        )

    def test_catastrophic_backtracking_prevention(self):
        """Verify complex patterns don't cause DoS via catastrophic backtracking."""
        import time

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

        start = time.perf_counter()
        masked = masking_filter._mask_text(test_text)
        elapsed = time.perf_counter() - start

        # Should complete in milliseconds, not seconds
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
            masked = masking_filter._mask_text(input_text)
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
            masking_filter._mask_text("Path: C:\\Users\\admin\\secret.txt")
            == "Path: ***REDACTED:WINDOWS_PATH***"
        )
        assert (
            masking_filter._mask_text("Pattern: \\d+")
            == "Pattern: ***REDACTED:REGEX_ESCAPE***"
        )
        assert (
            masking_filter._mask_text("Value: test\\\\value")
            == "Value: ***REDACTED:DOUBLE_BACKSLASH***"
        )

        # Should NOT match as regex
        assert masking_filter._mask_text("Pattern: 123") == "Pattern: 123"
        assert masking_filter._mask_text("Value: testvalue") == "Value: testvalue"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
