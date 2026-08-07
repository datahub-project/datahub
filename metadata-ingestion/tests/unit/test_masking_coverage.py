"""
Additional tests to increase coverage for masking modules.
"""

import logging
import os
import sys
from io import StringIO

import pytest

from datahub.masking.bootstrap import (
    initialize_secret_masking,
    shutdown_secret_masking,
)
from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry, is_masking_enabled


def test_masking_module_imports():
    """Test that all public exports from __init__.py are importable."""
    # Import from masking module to cover __init__.py
    import datahub.masking

    # Test that all exports are available
    assert hasattr(datahub.masking, "SecretMaskingFilter")
    assert hasattr(datahub.masking, "StreamMaskingWrapper")
    assert hasattr(datahub.masking, "install_masking_filter")
    assert hasattr(datahub.masking, "uninstall_masking_filter")
    assert hasattr(datahub.masking, "SecretRegistry")
    assert hasattr(datahub.masking, "is_masking_enabled")
    assert hasattr(datahub.masking, "initialize_secret_masking")
    assert hasattr(datahub.masking, "get_masking_safe_logger")

    # Verify imports work
    from datahub.masking import (
        SecretMaskingFilter,
        SecretRegistry,
        StreamMaskingWrapper,
    )

    assert SecretMaskingFilter is not None
    assert SecretRegistry is not None
    assert StreamMaskingWrapper is not None


class TestSecretRegistryEdgeCases:
    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_register_secret_empty_value(self):
        registry = SecretRegistry.get_instance()
        registry.register_secret("EMPTY", "")
        assert registry.get_count() == 0

    def test_register_secret_non_string(self):
        registry = SecretRegistry.get_instance()
        registry.register_secret("NUMBER", 123)  # type: ignore
        assert registry.get_count() == 0

    def test_register_secret_short_value(self):
        registry = SecretRegistry.get_instance()
        registry.register_secret("SHORT", "ab")
        assert registry.get_count() == 0

    def test_register_secret_with_escape_sequences(self):
        registry = SecretRegistry.get_instance()
        secret_with_newline = "pass\nword"
        registry.register_secret("MULTILINE", secret_with_newline)

        # Both original and repr version should be registered
        assert registry.has_secret("MULTILINE")
        assert registry.get_count() >= 1

    def test_register_secrets_batch_empty(self):
        registry = SecretRegistry.get_instance()
        registry.register_secrets_batch({})
        assert registry.get_count() == 0

    def test_register_secrets_batch_all_invalid(self):
        registry = SecretRegistry.get_instance()
        registry.register_secrets_batch(
            {
                "EMPTY": "",
                "SHORT": "ab",
                "NONE": None,  # type: ignore
            }
        )
        assert registry.get_count() == 0

    def test_memory_limit_single(self):
        registry = SecretRegistry.get_instance()
        original_max = registry.MAX_SECRETS
        registry.MAX_SECRETS = 5

        # Register 10 secrets, only 5 should be stored
        for i in range(10):
            registry.register_secret(f"SECRET_{i}", f"value_{i}")

        assert registry.get_count() <= 5
        registry.MAX_SECRETS = original_max

    def test_get_secret_value_not_found(self):
        registry = SecretRegistry.get_instance()
        assert registry.get_secret_value("NONEXISTENT") is None

    def test_has_secret_not_found(self):
        registry = SecretRegistry.get_instance()
        assert not registry.has_secret("NONEXISTENT")


class TestMaskingEnabled:
    def test_is_masking_enabled(self):
        # Default is enabled
        assert is_masking_enabled()

        # Disable via env var
        os.environ["DATAHUB_DISABLE_SECRET_MASKING"] = "true"
        assert not is_masking_enabled()

        os.environ["DATAHUB_DISABLE_SECRET_MASKING"] = "1"
        assert not is_masking_enabled()

        os.environ["DATAHUB_DISABLE_SECRET_MASKING"] = "false"
        assert is_masking_enabled()

        del os.environ["DATAHUB_DISABLE_SECRET_MASKING"]


class TestLoggingUtils:
    def test_get_masking_safe_logger(self):
        logger = get_masking_safe_logger(__name__)
        assert isinstance(logger, logging.Logger)
        assert logger.name == __name__

    def test_logger_can_log(self):
        logger = get_masking_safe_logger("test_logger")

        # Capture log output
        handler = logging.StreamHandler(StringIO())
        logger.addHandler(handler)

        logger.info("Test message")
        logger.debug("Debug message")
        logger.warning("Warning message")

        logger.removeHandler(handler)


class TestMaskingFilterDebugMode:
    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        # Ensure env var is cleaned up
        if "DATAHUB_DISABLE_SECRET_MASKING" in os.environ:
            del os.environ["DATAHUB_DISABLE_SECRET_MASKING"]

    def test_filter_masking_disabled_globally(self):
        os.environ["DATAHUB_DISABLE_SECRET_MASKING"] = "true"

        registry = SecretRegistry.get_instance()
        registry.register_secret("DISABLED_SECRET", "disabled_value")

        masking_filter = SecretMaskingFilter(registry)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Secret: disabled_value",
            args=(),
            exc_info=None,
        )

        result = masking_filter.filter(record)
        assert result
        # When masking is disabled, secrets should NOT be masked
        assert "disabled_value" in record.msg


class TestBootstrapEdgeCases:
    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def test_initialize_twice(self):
        initialize_secret_masking()
        initialize_secret_masking()  # Should be no-op

        # Only one filter should be installed
        root_logger = logging.getLogger()
        filters = [f for f in root_logger.filters if isinstance(f, SecretMaskingFilter)]
        assert len(filters) == 1

    def test_initialize_with_force(self):
        initialize_secret_masking()
        initialize_secret_masking(force=True)

        # Should still work
        root_logger = logging.getLogger()
        filters = [f for f in root_logger.filters if isinstance(f, SecretMaskingFilter)]
        assert len(filters) >= 1

    def test_shutdown_when_not_initialized(self):
        shutdown_secret_masking()  # Should be safe

    def test_shutdown_clears_filters(self):
        initialize_secret_masking()
        shutdown_secret_masking()

        root_logger = logging.getLogger()
        filters = [f for f in root_logger.filters if isinstance(f, SecretMaskingFilter)]
        assert len(filters) == 0

    def test_is_bootstrapped(self):
        from datahub.masking.bootstrap import is_bootstrapped

        shutdown_secret_masking()
        assert not is_bootstrapped()

        initialize_secret_masking()
        assert is_bootstrapped()

        shutdown_secret_masking()
        assert not is_bootstrapped()

    def test_get_bootstrap_error(self):
        from datahub.masking.bootstrap import get_bootstrap_error

        shutdown_secret_masking()
        assert get_bootstrap_error() is None

    def test_initialize_with_disabled_masking(self):
        """Test initialization when masking is disabled.

        A disabled initialize must NOT latch ``_bootstrap_completed``: that
        flag means "the filter is installed", and a disabled call installs
        nothing. Latching True on the disabled branch stranded the flag so
        a later call with masking enabled short-circuited the install but
        still returned a token — the caller got the "masking is on" signal
        with no filter. ``is_bootstrapped()`` is False after a disabled
        initialize, and a subsequent enabled initialize installs the
        filter for real.
        """
        from datahub.masking.bootstrap import is_bootstrapped

        os.environ["DATAHUB_DISABLE_SECRET_MASKING"] = "true"
        try:
            shutdown_secret_masking()
            token = initialize_secret_masking()
            # Disabled: no token, no filter installed.
            assert token is None
            assert not is_bootstrapped()

            # Flip masking back on and initialize again — the filter must
            # actually install now (the disabled call did not strand the
            # latch).
            del os.environ["DATAHUB_DISABLE_SECRET_MASKING"]
            from datahub.masking.secret_registry import is_masking_enabled

            assert is_masking_enabled()
            token2 = initialize_secret_masking()
            assert token2 is not None
            assert is_bootstrapped()
        finally:
            if "DATAHUB_DISABLE_SECRET_MASKING" in os.environ:
                del os.environ["DATAHUB_DISABLE_SECRET_MASKING"]
            shutdown_secret_masking()

    def test_shutdown_clears_latch_in_finally_when_teardown_raises(self):
        """If a teardown step raises, the bootstrap latch is still cleared
        (in ``finally`` around the teardown steps). Previously the latch
        was cleared as the last statement inside the try, so a raise
        stranded it ``True`` over a partially-torn-down filter — the next
        initialize then short-circuited with masking off."""
        import datahub.masking.bootstrap as bootstrap_mod
        from datahub.masking.bootstrap import is_bootstrapped

        # Install for real so teardown has something to tear down.
        token = initialize_secret_masking()
        assert token is not None
        assert is_bootstrapped()

        # Force the uninstall primitive to raise. ``_uninstall_masking_filter``
        # is step-wise tolerant and swallows its own errors, so patch the
        # name bootstrap imported to raise before the step-wise body runs.
        original = bootstrap_mod._uninstall_masking_filter

        def raising_uninstall():
            raise RuntimeError("forced teardown failure for test")

        bootstrap_mod._uninstall_masking_filter = raising_uninstall  # type: ignore[assignment]
        try:
            # shutdown swallows the error (teardown is fail-safe) but the
            # finally must still clear the latch.
            shutdown_secret_masking(token)
            assert not is_bootstrapped(), (
                "latch stranded True after teardown raised — next initialize "
                "would short-circuit with masking off"
            )
        finally:
            bootstrap_mod._uninstall_masking_filter = original  # type: ignore[assignment]
            # Clean up any state the failed teardown left behind.
            shutdown_secret_masking()

    def test_force_teardown_restores_excepthook_with_peer_scope_active(self):
        """Finding 10: ``shutdown_secret_masking()`` with no token skips
        teardown when a peer execution scope is active (e.g. opened on
        another thread). Previously ``reset_instance()`` then dropped the
        filter but never restored ``sys.excepthook``, leaving a hook bound
        to a dead registry. The force-teardown path restores the excepthook
        unconditionally, and the fixture asserts no leaked scopes."""
        import datahub.masking.bootstrap as bootstrap_mod

        # Install and capture the original excepthook.
        original_hook = sys.excepthook
        token = initialize_secret_masking()
        assert token is not None
        # After install, sys.excepthook is the masking hook.
        assert sys.excepthook is not original_hook

        # Simulate a peer scope opened on "another thread" by adding a group
        # directly to the registry without setting this thread's contextvar.
        registry = SecretRegistry.get_instance()
        peer_exec_id = "peer-scope-for-test"
        with registry._registry_lock:
            registry._groups.setdefault(peer_exec_id, {})
            registry._rebuild_locked()

        # shutdown_secret_masking() with no token: this thread has no ambient
        # scope, the peer group is active, so it returns True (skips teardown).
        result = shutdown_secret_masking()
        assert result is None  # shutdown returns None
        # The filter is still installed (teardown skipped).
        import datahub.masking.masking_filter as mf_mod

        assert mf_mod._installed_filter is not None
        # sys.excepthook is still the masking hook (not restored).
        assert sys.excepthook is not original_hook

        # Now the force-teardown path (what the fixture uses) tears down
        # unconditionally and restores the excepthook.
        bootstrap_mod._force_teardown_secret_masking()
        assert sys.excepthook is original_hook or sys.excepthook == original_hook
        assert mf_mod._installed_filter is None

        # Clean up the peer scope so the fixture's has_active_executions
        # assertion passes.
        with registry._registry_lock:
            registry._groups.pop(peer_exec_id, None)
            registry._rebuild_locked()
        SecretRegistry.reset_instance()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
