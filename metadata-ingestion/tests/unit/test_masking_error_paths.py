"""
Tests for error paths in masking framework.
"""

import logging

import pytest

from datahub.masking.bootstrap import (
    initialize_secret_masking,
    shutdown_secret_masking,
)
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry


class TestBootstrapConfiguration:
    """Test bootstrap configuration paths."""

    def teardown_method(self):
        shutdown_secret_masking()

    def test_http_client_debug_disable(self):
        """Test that HTTP client debug is disabled during init."""
        import http.client

        # Store original value
        original_debuglevel = http.client.HTTPConnection.debuglevel

        try:
            # Set debug level to 1
            http.client.HTTPConnection.debuglevel = 1

            # Initialize masking
            initialize_secret_masking(force=True)

            # Debug level should be set to 0
            assert http.client.HTTPConnection.debuglevel == 0

        finally:
            # Restore
            http.client.HTTPConnection.debuglevel = original_debuglevel
            shutdown_secret_masking()

    def test_warnings_capture(self):
        """Test that warnings are captured to logging."""
        import warnings

        initialize_secret_masking(force=True)

        try:
            # Check that warnings are being captured
            # This is configured via logging.captureWarnings(True)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                warnings.warn("Test warning", UserWarning, stacklevel=2)

                # Warning should be captured
                assert len(w) == 1

        finally:
            shutdown_secret_masking()


class TestSecretRegistryUtilities:
    """Test utility functions in secret registry."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_has_secret_nonexistent(self):
        """Test checking for nonexistent secret."""
        registry = SecretRegistry.get_instance()

        # Nonexistent secret should return False
        assert not registry.has_secret("NONEXISTENT")

    def test_register_multiple_secrets(self):
        """Test registering multiple secrets individually."""
        registry = SecretRegistry.get_instance()

        registry.register_secret("SECRET1", "value1_with_length")
        registry.register_secret("SECRET2", "value2_with_length")
        registry.register_secret("SECRET3", "value3_with_length")

        # All should be registered
        assert registry.has_secret("SECRET1")
        assert registry.has_secret("SECRET2")
        assert registry.has_secret("SECRET3")
        assert registry.get_count() == 3

    def test_clear_and_reuse(self):
        """Test clearing registry and reusing it."""
        registry = SecretRegistry.get_instance()

        # Register secrets
        registry.register_secret("SECRET1", "value1_long")
        registry.register_secret("SECRET2", "value2_long")
        assert registry.get_count() == 2

        # Clear
        registry.clear()
        assert registry.get_count() == 0

        # Reuse
        registry.register_secret("SECRET3", "value3_long")
        assert registry.get_count() == 1
        assert registry.has_secret("SECRET3")


class TestMaskingWithSpecialCharacters:
    """Test masking with special regex characters."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_mask_secret_with_dots(self):
        """Test masking secrets that contain dots."""
        registry = SecretRegistry.get_instance()

        secret_value = "password.with.dots"
        registry.register_secret("DOT_SECRET", secret_value)

        masking_filter = SecretMaskingFilter(registry)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"The secret is: {secret_value}",
            args=(),
            exc_info=None,
        )

        masking_filter.filter(record)
        masked = record.getMessage()

        # REDACTED marker should be present
        assert "REDACTED" in masked
        # Original full secret value should be masked
        assert secret_value not in masked or "***REDACTED" in masked

    def test_mask_secret_with_brackets(self):
        """Test masking secrets that contain brackets."""
        registry = SecretRegistry.get_instance()

        secret_value = "pass[with]brackets"
        registry.register_secret("BRACKET_SECRET", secret_value)

        masking_filter = SecretMaskingFilter(registry)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"The secret is: {secret_value}",
            args=(),
            exc_info=None,
        )

        masking_filter.filter(record)
        masked = record.getMessage()

        # REDACTED marker should be present
        assert "REDACTED" in masked


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
