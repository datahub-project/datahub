"""
Tests for secret masking bootstrap functionality.

Note: These tests mock filter installation because bootstrap only sets up
infrastructure (logging filter + exception hook). Secret discovery happens
automatically at point-of-read (config expansion, Pydantic validation).
"""

import logging
import sys
import threading
from io import StringIO
from unittest.mock import patch

from datahub.masking.bootstrap import (
    initialize_secret_masking,
    shutdown_secret_masking,
)
from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.masking_filter import SecretMaskingFilter, StreamMaskingWrapper
from datahub.masking.secret_registry import SecretRegistry


class TestBootstrapErrorHandling:
    """Test bootstrap initialization error handling."""

    def test_bootstrap_error_cleared_on_successful_retry(self):
        """
        Verify that _bootstrap_error is cleared when retry succeeds.

        Regression test for bug where _bootstrap_error remained set
        after successful initialization following a previous failure.

        Bug scenario:
        1. First initialization fails → _bootstrap_error is set
        2. Second initialization succeeds → _bootstrap_error should be None
        3. Without fix: _bootstrap_error still contains old error (misleading)
        """
        from datahub.masking.bootstrap import (
            get_bootstrap_error,
            initialize_secret_masking,
            is_bootstrapped,
            shutdown_secret_masking,
        )

        try:
            # Start clean
            shutdown_secret_masking()

            # First attempt: simulate failure during filter installation
            with patch(
                "datahub.masking.bootstrap.install_masking_filter"
            ) as mock_install:
                test_exception = Exception("Simulated installation failure")
                mock_install.side_effect = test_exception

                # Initialize (should fail gracefully)
                initialize_secret_masking()

                # Verify initialization failed
                assert not is_bootstrapped(), "Should not be bootstrapped after failure"

                # Verify error was recorded
                error = get_bootstrap_error()
                assert error is not None, "Error should be recorded after failure"
                assert "Simulated installation failure" in str(error), (
                    "Should record the actual error"
                )

            # Second attempt: simulate success
            with patch("datahub.masking.bootstrap.install_masking_filter"):
                # This time it succeeds (no side_effect)
                initialize_secret_masking()

                # Verify initialization succeeded
                assert is_bootstrapped(), (
                    "Should be bootstrapped after successful retry"
                )

                # CRITICAL: Verify error was cleared (this is the bug fix)
                error = get_bootstrap_error()
                assert error is None, (
                    "Error should be None after successful initialization. "
                    "If this assertion fails, _bootstrap_error is not being cleared on success. "
                    "Fix: Add '_bootstrap_error = None' after '_bootstrap_completed = True'"
                )

        finally:
            # Always cleanup
            shutdown_secret_masking()

    def test_bootstrap_error_set_on_failure(self):
        """Verify that _bootstrap_error is set when initialization fails."""
        from datahub.masking.bootstrap import (
            get_bootstrap_error,
            initialize_secret_masking,
            is_bootstrapped,
            shutdown_secret_masking,
        )

        try:
            shutdown_secret_masking()

            # Simulate failure during filter installation
            with patch(
                "datahub.masking.bootstrap.install_masking_filter"
            ) as mock_install:
                test_error = ValueError("Test failure during filter installation")
                mock_install.side_effect = test_error

                # Initialize (should fail gracefully, not raise)
                initialize_secret_masking()

                # Verify state
                assert not is_bootstrapped(), "Should not be bootstrapped after failure"

                error = get_bootstrap_error()
                assert error is not None, "Error should be recorded"
                assert isinstance(error, ValueError), (
                    "Should be the ValueError we raised"
                )
                assert "Test failure during filter installation" in str(error)

        finally:
            shutdown_secret_masking()

    def test_concurrent_initialization_is_idempotent(self):
        probe_handler = logging.NullHandler()
        logging.getLogger().addHandler(probe_handler)
        try:
            threads = [
                threading.Thread(target=initialize_secret_masking) for _ in range(10)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5.0)
                assert not t.is_alive()

            masking_filters = [
                f for f in probe_handler.filters if isinstance(f, SecretMaskingFilter)
            ]
            assert len(masking_filters) == 1
            assert isinstance(sys.stdout, StreamMaskingWrapper)
            assert not isinstance(sys.stdout._original, StreamMaskingWrapper)
        finally:
            logging.getLogger().removeHandler(probe_handler)


class TestExceptionHookOptimization:
    """Test exception hook filter reuse optimization."""

    def test_exception_hook_reuses_installed_filter(self):
        SecretRegistry.get_instance().register_secret("TEST_SECRET", "secret123")

        created_filters = []
        original_init = SecretMaskingFilter.__init__

        def track_init(self, *args, **kwargs):
            created_filters.append(self)
            return original_init(self, *args, **kwargs)

        with patch.object(SecretMaskingFilter, "__init__", track_init):
            initialize_secret_masking()
            assert len(created_filters) == 1

            current_hook = sys.excepthook
            for i in range(5):
                try:
                    raise ValueError(f"Test exception {i} with secret123")
                except ValueError as e:
                    current_hook(ValueError, e, e.__traceback__)

            assert len(created_filters) == 1

    def test_exception_hook_masks_secrets_registered_after_installation(self):
        registry = SecretRegistry.get_instance()
        registry.register_secret("SECRET1", "value123")
        initialize_secret_masking()
        registry.register_secret("SECRET2", "value456")

        current_hook = sys.excepthook
        captured = StringIO()
        original_stderr = sys.stderr
        sys.stderr = captured
        try:
            try:
                raise ValueError("Contains value123 and value456")
            except ValueError as e:
                current_hook(ValueError, e, e.__traceback__)
        finally:
            sys.stderr = original_stderr

        output = captured.getvalue()
        assert "value123" not in output
        assert "value456" not in output
        assert "***REDACTED:SECRET1***" in output
        assert "***REDACTED:SECRET2***" in output


class TestProcessLifetimeMasking:
    def test_shutdown_does_not_unmask_concurrent_use(self):
        sink = StringIO()
        probe_handler = logging.StreamHandler(sink)
        logging.getLogger().addHandler(probe_handler)
        try:
            initialize_secret_masking()
            SecretRegistry.get_instance().register_secret("DB_PASS", "hunter2secret")

            shutdown_secret_masking()

            logging.getLogger("myapp.connector").error("pw=hunter2secret")
            assert "hunter2secret" not in sink.getvalue()
            assert "***REDACTED:DB_PASS***" in sink.getvalue()
            assert SecretRegistry.get_instance().has_secret("DB_PASS")
        finally:
            logging.getLogger().removeHandler(probe_handler)

    def test_propagated_child_logger_records_are_masked(self):
        sink = StringIO()
        probe_handler = logging.StreamHandler(sink)
        logging.getLogger().addHandler(probe_handler)
        try:
            initialize_secret_masking()
            SecretRegistry.get_instance().register_secret("TOKEN", "tok-abc-123")

            logging.getLogger("myapp.database.connection").error(
                "connecting with tok-abc-123"
            )
            assert "tok-abc-123" not in sink.getvalue()
            assert "***REDACTED:TOKEN***" in sink.getvalue()
        finally:
            logging.getLogger().removeHandler(probe_handler)

    def test_reinitialize_attaches_to_handlers_created_later(self):
        initialize_secret_masking()

        late_logger = logging.getLogger("late.created.logger")
        late_handler = logging.NullHandler()
        late_logger.addHandler(late_handler)
        try:
            assert not any(
                isinstance(f, SecretMaskingFilter) for f in late_handler.filters
            )
            initialize_secret_masking()
            assert any(isinstance(f, SecretMaskingFilter) for f in late_handler.filters)
        finally:
            late_logger.removeHandler(late_handler)

    def test_masking_safe_logger_handlers_are_excluded(self):
        safe_logger = get_masking_safe_logger("datahub.masking.probe")
        initialize_secret_masking()
        for handler in safe_logger.handlers:
            assert not any(isinstance(f, SecretMaskingFilter) for f in handler.filters)
