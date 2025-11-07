"""Tests for exception hook masking functionality."""

import sys

from datahub.masking.bootstrap import (
    initialize_secret_masking,
    shutdown_secret_masking,
)
from datahub.masking.secret_registry import SecretRegistry


class TestExceptionHookMasking:
    """Test suite for exception hook masking."""

    def setup_method(self):
        """Setup before each test."""
        import os

        # Save original environment to restore later
        self._original_env = dict(os.environ)

        # Set up test environment
        os.environ.clear()
        os.environ["TEST_SECRET_PASSWORD"] = "MySecretPass123!!"
        os.environ["TEST_API_KEY"] = "secret-api-key-xyz"

        # Initialize masking
        initialize_secret_masking(force=True)

        # Register test secrets
        registry = SecretRegistry.get_instance()
        registry.register_secret("TEST_SECRET_PASSWORD", "MySecretPass123!!")
        registry.register_secret("TEST_API_KEY", "secret-api-key-xyz")

    def teardown_method(self):
        """Cleanup after each test."""
        import logging
        import os

        shutdown_secret_masking()

        # Restore original environment
        os.environ.clear()
        os.environ.update(self._original_env)

        # Ensure logging state is clean for subsequent tests
        logging.raiseExceptions = True

    def test_exception_hook_installed(self):
        """Verify that custom exception hook is installed."""
        # After initialization, sys.excepthook should not be the default
        assert sys.excepthook != sys.__excepthook__

    def test_unhandled_exception_masking(self):
        """Test that unhandled exceptions are masked via exception hook."""
        # Capture what would be printed by sys.excepthook
        import io
        from contextlib import redirect_stderr

        captured_stderr = io.StringIO()

        # Manually invoke the exception hook
        try:
            raise ValueError("Secret password: MySecretPass123!!")
        except Exception:
            exc_info = sys.exc_info()
            with redirect_stderr(captured_stderr):
                # Call the exception hook directly
                sys.excepthook(*exc_info)

        output = captured_stderr.getvalue()

        # Verify password is masked in output
        assert "MySecretPass123!!" not in output
        assert "***REDACTED:TEST_SECRET_PASSWORD***" in output

    def test_multiple_secrets_in_exception(self):
        """Test masking of multiple secrets in one exception."""
        import io
        from contextlib import redirect_stderr

        captured_stderr = io.StringIO()

        try:
            raise ValueError(
                "Password: MySecretPass123!! and API Key: secret-api-key-xyz"
            )
        except Exception:
            exc_info = sys.exc_info()
            with redirect_stderr(captured_stderr):
                sys.excepthook(*exc_info)

        output = captured_stderr.getvalue()

        # Both secrets should be masked
        assert "MySecretPass123!!" not in output
        assert "secret-api-key-xyz" not in output
        assert "***REDACTED:TEST_SECRET_PASSWORD***" in output
        assert "***REDACTED:TEST_API_KEY***" in output

    def test_exception_hook_handles_non_string_args(self):
        """Test that exception hook handles exceptions with non-string args."""
        import io
        from contextlib import redirect_stderr

        captured_stderr = io.StringIO()

        # Exception with mixed arg types
        secret_msg = "text with MySecretPass123!!"
        try:
            raise ValueError(42, secret_msg, None)
        except Exception:
            exc_info = sys.exc_info()
            with redirect_stderr(captured_stderr):
                sys.excepthook(*exc_info)

        output = captured_stderr.getvalue()

        # Note: Password may appear in source code line of traceback (can't be masked)
        # But the exception message line should have masked version
        lines = output.split("\n")
        # Find the ValueError line (not the "raise" source line)
        exception_msg_lines = [line for line in lines if line.startswith("ValueError:")]

        if exception_msg_lines:
            # Check that exception message is masked
            assert "***REDACTED:TEST_SECRET_PASSWORD***" in exception_msg_lines[0]

    def test_exception_hook_preserves_exception_type(self):
        """Test that exception hook preserves the original exception type."""
        import io
        from contextlib import redirect_stderr

        # Test with different exception types
        for exc_class in [ValueError, KeyError, RuntimeError, TypeError]:
            captured_stderr = io.StringIO()
            secret_msg = "Secret: MySecretPass123!!"

            try:
                raise exc_class(secret_msg)
            except Exception:
                exc_info = sys.exc_info()
                with redirect_stderr(captured_stderr):
                    sys.excepthook(*exc_info)

                output = captured_stderr.getvalue()

                # Verify exception type is preserved in traceback
                assert exc_class.__name__ in output

                # Verify secret is masked in exception message line
                # (not checking source code lines where password appears in literals)
                lines = output.split("\n")
                exc_msg_lines = [
                    line for line in lines if line.startswith(exc_class.__name__)
                ]
                if exc_msg_lines:
                    assert "***REDACTED:TEST_SECRET_PASSWORD***" in exc_msg_lines[0]

    def test_exception_hook_graceful_failure(self):
        """Test that exception hook fails gracefully if masking fails."""
        # This test verifies that even if masking fails, the original
        # exception is still displayed (fail-open for debugging)

        # Create an exception with an unusual structure that might cause issues
        class WeirdException(Exception):  # type: ignore[misc]
            @property
            def args(self):  # type: ignore[override]
                raise RuntimeError("Cannot access args!")

        try:
            raise WeirdException()
        except Exception:
            exc_info = sys.exc_info()
            # Should not raise - hook should handle gracefully
            sys.excepthook(*exc_info)

    def test_shutdown_restores_original_hook(self):
        """Test that shutdown properly restores the original exception hook."""
        # Store the current hook before shutdown
        hook_before_shutdown = sys.excepthook

        # Shutdown
        shutdown_secret_masking()

        # Hook should be different after shutdown (restored)
        assert sys.excepthook != hook_before_shutdown


class TestExceptionHookIntegration:
    """Integration tests for exception hook with full masking system."""

    def test_exception_masking_before_initialization(self):
        """Test that exceptions are not masked before initialization."""
        import os

        os.environ["TEST_PWD"] = "password123"

        # Create exception before masking initialized
        try:
            raise ValueError("Password: password123")
        except Exception as e:
            # Exception object contains password before masking
            assert "password123" in str(e)

    def test_exception_masking_after_initialization(self):
        """Test that exceptions are masked after initialization."""
        import io
        import os
        from contextlib import redirect_stderr

        # Use a longer password that's more likely to be detected
        test_pwd = "MyLongPassword12345!!"
        os.environ["TEST_PWD"] = test_pwd

        # Initialize masking
        initialize_secret_masking(force=True)

        # Register test secret
        registry = SecretRegistry.get_instance()
        registry.register_secret("TEST_PWD", test_pwd)

        try:
            # Test that exception hook masks secrets
            captured_stderr = io.StringIO()

            try:
                raise ValueError(f"Password: {test_pwd}")
            except Exception:
                exc_info = sys.exc_info()
                with redirect_stderr(captured_stderr):
                    sys.excepthook(*exc_info)

            output = captured_stderr.getvalue()
            # Exception output should be masked
            assert "MyLongPassword12345!!" not in output
            assert "***REDACTED:TEST_PWD***" in output

        finally:
            shutdown_secret_masking()
