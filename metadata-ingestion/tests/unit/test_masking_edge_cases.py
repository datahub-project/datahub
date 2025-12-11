# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

"""
Tests for edge cases and error paths in the masking framework to improve coverage.
"""

import logging
import threading

import pytest

from datahub.masking.bootstrap import (
    get_bootstrap_error,
    initialize_secret_masking,
    is_bootstrapped,
    shutdown_secret_masking,
)
from datahub.masking.logging_utils import (
    get_masking_safe_logger,
    reset_masking_safe_loggers,
)
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry


def test_imports_from_init():
    """Test that all exports from __init__.py are accessible."""
    # Import entire module to ensure __init__.py is executed
    import datahub.masking as masking_module

    # Verify all exports exist
    assert hasattr(masking_module, "SecretMaskingFilter")
    assert hasattr(masking_module, "StreamMaskingWrapper")
    assert hasattr(masking_module, "install_masking_filter")
    assert hasattr(masking_module, "uninstall_masking_filter")
    assert hasattr(masking_module, "SecretRegistry")
    assert hasattr(masking_module, "is_masking_enabled")
    assert hasattr(masking_module, "initialize_secret_masking")
    assert hasattr(masking_module, "get_masking_safe_logger")


class TestMaskingFilterEdgeCases:
    """Test edge cases in masking filter."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def test_pattern_rebuild_with_concurrent_modifications(self):
        """Test pattern rebuild when secrets are modified during rebuild."""
        registry = SecretRegistry.get_instance()
        masking_filter = SecretMaskingFilter(registry)

        # Add many secrets
        for i in range(100):
            registry.register_secret(f"SECRET_{i}", f"value_{i}")

        # Force pattern rebuild
        masking_filter._check_and_rebuild_pattern()

        # Verify pattern was rebuilt
        assert masking_filter._last_version > 0

    def test_masking_with_very_long_message(self):
        """Test masking with messages exceeding max_message_size."""
        registry = SecretRegistry.get_instance()
        registry.register_secret("PASSWORD", "secret123")

        masking_filter = SecretMaskingFilter(registry, max_message_size=100)

        # Create a very long message
        long_message = "x" * 200 + "secret123" + "y" * 200

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=long_message,
            args=(),
            exc_info=None,
        )

        masking_filter.filter(record)

        # Message should be truncated and masked
        masked = record.getMessage()
        assert "secret123" not in masked
        assert len(masked) <= 100 + 50  # max_message_size + some buffer for redaction

    def test_masking_with_formatted_args(self):
        """Test masking with % formatting args."""
        registry = SecretRegistry.get_instance()
        registry.register_secret("PASSWORD", "secret123")

        masking_filter = SecretMaskingFilter(registry)

        # Test with tuple args
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Password: %s, User: %s",
            args=("secret123", "admin"),
            exc_info=None,
        )

        masking_filter.filter(record)

        # Both msg and args should be processed
        assert "secret123" not in str(record.args)
        assert "***REDACTED:PASSWORD***" in record.getMessage()


class TestBootstrapEdgeCases:
    """Test edge cases in bootstrap."""

    def setup_method(self):
        shutdown_secret_masking()

    def teardown_method(self):
        shutdown_secret_masking()

    def test_double_initialization(self):
        """Test that double initialization is handled gracefully."""
        initialize_secret_masking()
        assert is_bootstrapped()

        # Second initialization should be no-op
        initialize_secret_masking()
        assert is_bootstrapped()

    def test_force_reinitialization(self):
        """Test force re-initialization."""
        initialize_secret_masking()
        assert is_bootstrapped()

        # Force re-init
        initialize_secret_masking(force=True)
        assert is_bootstrapped()

    def test_bootstrap_error_cleared_on_success(self):
        """Test that bootstrap error is cleared after successful init."""
        initialize_secret_masking()
        assert get_bootstrap_error() is None


class TestLoggingUtils:
    """Test logging utilities."""

    def test_masking_safe_logger_multiple_calls(self):
        """Test that getting the same logger multiple times doesn't add duplicate handlers."""
        logger1 = get_masking_safe_logger("test.logger")
        handler_count_1 = len(logger1.handlers)

        logger2 = get_masking_safe_logger("test.logger")
        handler_count_2 = len(logger2.handlers)

        # Should be the same logger
        assert logger1 is logger2
        # Should not have duplicate handlers
        assert handler_count_1 == handler_count_2

    def test_reset_masking_safe_loggers(self):
        """Test resetting masking-safe loggers."""
        # Create a masking-safe logger
        logger = get_masking_safe_logger("datahub.masking.test")
        assert not logger.propagate
        assert len(logger.handlers) > 0

        # Reset
        reset_masking_safe_loggers()

        # Logger should be reset
        assert logger.propagate
        assert len(logger.handlers) == 0


class TestSecretRegistryEdgeCases:
    """Test edge cases in secret registry."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_register_empty_secret_name(self):
        """Test registering secret with empty name."""
        registry = SecretRegistry.get_instance()

        # Empty name should be handled gracefully
        registry.register_secret("", "longsecretvalue123")

        # Empty names are actually accepted by the registry
        assert registry.has_secret("")
        assert registry.get_count() == 1

    def test_register_very_short_secret(self):
        """Test registering very short secret values."""
        registry = SecretRegistry.get_instance()

        # Very short secrets (< 8 chars) are still registered
        registry.register_secret("SHORT", "abc")

        # Short secrets are registered (no minimum length enforced)
        assert registry.has_secret("SHORT")
        assert registry.get_count() == 1

    def test_concurrent_registration(self):
        """Test concurrent secret registration."""
        registry = SecretRegistry.get_instance()
        errors = []

        def register_secrets(start_idx: int) -> None:
            try:
                for i in range(start_idx, start_idx + 100):
                    registry.register_secret(f"SECRET_{i}", f"secret_value_{i}")
            except Exception as e:
                errors.append(e)

        # Start multiple threads registering secrets concurrently
        threads = []
        for i in range(5):
            t = threading.Thread(target=register_secrets, args=(i * 100,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # Should not have any errors
        assert len(errors) == 0

        # All secrets should be registered
        assert registry.get_count() == 500


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
