"""Tests for the bootstrap lifecycle — invariants 11 and scope halves of 3."""

import logging
import os
import sys
from unittest.mock import patch

import pytest

from datahub.masking.bootstrap import (
    get_bootstrap_error,
    initialize_secret_masking,
    is_bootstrapped,
    reset_bootstrap_state,
    shutdown_secret_masking,
)
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry


class TestDisabledMasking:
    """Invariant 11: with DATAHUB_DISABLE_SECRET_MASKING=true, nothing is
    installed and exactly one warning is emitted."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def teardown_method(self):
        if "DATAHUB_DISABLE_SECRET_MASKING" in os.environ:
            del os.environ["DATAHUB_DISABLE_SECRET_MASKING"]
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def test_disabled_returns_none_and_warns_once(self, caplog):
        os.environ["DATAHUB_DISABLE_SECRET_MASKING"] = "true"
        # Capture records from the masking-safe bootstrap logger directly,
        # since propagate=False keeps caplog from seeing them.
        bootstrap_logger = logging.getLogger("datahub.masking.bootstrap")
        records = []

        class _Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        cap = _Capture()
        bootstrap_logger.addHandler(cap)
        try:
            token = initialize_secret_masking()
            assert token is None
            assert not is_bootstrapped()
            # First call warns once.
            first_warnings = [r for r in records if "DISABLED" in r.getMessage()]
            assert len(first_warnings) == 1
            # Second call does not warn again.
            records.clear()
            initialize_secret_masking()
            second_warnings = [r for r in records if "DISABLED" in r.getMessage()]
            assert len(second_warnings) == 0
        finally:
            bootstrap_logger.removeHandler(cap)

    def test_disabled_then_enabled_installs_for_real(self):
        os.environ["DATAHUB_DISABLE_SECRET_MASKING"] = "true"
        initialize_secret_masking()
        assert not is_bootstrapped()
        del os.environ["DATAHUB_DISABLE_SECRET_MASKING"]
        token = initialize_secret_masking()
        assert token is not None
        assert is_bootstrapped()
        # A registered secret is masked.
        SecretRegistry.get_instance().register_secret("PW", "after_secret_value")
        mf = SecretMaskingFilter(SecretRegistry.get_instance())
        out = mf.mask_text("leak after_secret_value here")
        assert "after_secret_value" not in out
        assert "***REDACTED:PW***" in out


class TestInitializeInstallsOnce:
    """Invariant 11 (positive half): initialize installs the filter once;
    subsequent calls re-scan handlers and open a new scope each."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def test_initialize_returns_distinct_tokens(self):
        t1 = initialize_secret_masking()
        t2 = initialize_secret_masking()
        assert t1 is not None and t2 is not None
        assert t1 != t2

    def test_initialize_installs_filter_on_handlers(self):
        initialize_secret_masking()
        root = logging.getLogger()
        assert root.handlers, "root logger should have at least one handler"
        assert all(
            any(isinstance(f, SecretMaskingFilter) for f in h.filters)
            for h in root.handlers
        )

    def test_force_accepted_and_ignored(self):
        initialize_secret_masking()
        initialize_secret_masking(force=True)
        # Exactly one SecretMaskingFilter per handler (identity check on add).
        root = logging.getLogger()
        for h in root.handlers:
            assert sum(1 for f in h.filters if isinstance(f, SecretMaskingFilter)) == 1

    def test_initialize_failure_raises_and_records_error(self):
        with patch(
            "datahub.masking.bootstrap.install_masking_filter",
            side_effect=RuntimeError("install boom"),
        ):
            with pytest.raises(
                RuntimeError, match="Secret masking installation failed"
            ):
                initialize_secret_masking()
            assert not is_bootstrapped()
            err = get_bootstrap_error()
            assert err is not None
            assert "install boom" in str(err)


class TestShutdownDropsScope:
    """Invariant 3 (scope halves via bootstrap): ending one execution
    leaves the other's secrets masked and the filter installed."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def test_shutdown_one_leaves_other_masked(self):
        tok_a = initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("A", "aaa_secret_value")
        tok_b = initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("B", "bbb_secret_value")

        shutdown_secret_masking(tok_a)
        # Filter still installed; B's secret still registered.
        assert is_bootstrapped()
        assert SecretRegistry.get_instance().has_secret("B")
        assert not SecretRegistry.get_instance().has_secret("A")

        # A new mask still masks B.
        mf = SecretMaskingFilter(SecretRegistry.get_instance())
        out = mf.mask_text("leak bbb_secret_value here")
        assert "bbb_secret_value" not in out
        assert "***REDACTED:B***" in out

        shutdown_secret_masking(tok_b)
        assert SecretRegistry.get_instance().get_count() == 0

    def test_shutdown_no_scope_is_safe_noop(self):
        # No initialize called; shutdown is a safe no-op.
        shutdown_secret_masking()
        shutdown_secret_masking("nonexistent-token")

    def test_shutdown_does_not_uninstall_filter(self):
        tok = initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("X", "value_xyz_123456")
        shutdown_secret_masking(tok)
        # Filter stays installed on handlers; bootstrap latch stays set.
        assert is_bootstrapped()
        root = logging.getLogger()
        assert all(
            any(isinstance(f, SecretMaskingFilter) for f in h.filters)
            for h in root.handlers
        )

    def test_cross_thread_shutdown_with_token(self):
        import threading

        tok = initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("A", "aaa_secret_value")
        assert SecretRegistry.get_instance().get_count() > 0

        def shutdown_from_other_thread():
            shutdown_secret_masking(tok)

        t = threading.Thread(target=shutdown_from_other_thread)
        t.start()
        t.join(5)
        assert not t.is_alive()
        assert SecretRegistry.get_instance().get_count() == 0


class TestExcepthook:
    """The excepthook is installed once and masks the traceback; on masking
    failure it prints only the class name plus a note, never the raw
    traceback."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def test_excepthook_masks_traceback(self, capsys):
        initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("PW", "traceback_secret_value")
        hook = sys.excepthook
        try:
            raise ValueError("embed traceback_secret_value in message")
        except ValueError as e:
            hook(ValueError, e, e.__traceback__)
        captured = capsys.readouterr()
        assert "traceback_secret_value" not in captured.err
        assert "***REDACTED:PW***" in captured.err

    def test_excepthook_failure_does_not_leak(self, capsys):
        initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("PW", "leak_in_traceback_value")
        # Break mask_text so the hook's masking path raises.
        with patch.object(
            SecretMaskingFilter,
            "mask_text",
            side_effect=RuntimeError("mask boom"),
        ):
            hook = sys.excepthook
            try:
                raise ValueError("embed leak_in_traceback_value in message")
            except ValueError as e:
                hook(ValueError, e, e.__traceback__)
        captured = capsys.readouterr()
        # The raw traceback (with the secret) must NOT be written; only the
        # class name plus a masking-error note.
        assert "leak_in_traceback_value" not in captured.err
        assert "ValueError" in captured.err


class TestReinitHealsStrandedLatch:
    """Fix 3: re-init after reset_instance (without reset_bootstrap_state)
    rebinds the filter to the new registry; the re-scan covers handlers added
    while the wrap was inert."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        reset_bootstrap_state()

    def test_reinit_after_reset_instance_rebinds_and_masks(self):
        from io import StringIO

        initialize_secret_masking()
        reg = SecretRegistry.get_instance()
        reg.register_secret("PW", "first_secret_value")
        cap = StringIO()
        h = logging.StreamHandler(cap)
        h.setFormatter(logging.Formatter("%(message)s"))
        root = logging.getLogger()
        root.addHandler(h)
        try:
            log = logging.getLogger("test.reinit.rebind")
            log.setLevel(logging.INFO)
            log.info("leak first_secret_value here")
            assert "first_secret_value" not in cap.getvalue()
            assert "***REDACTED:PW***" in cap.getvalue()
        finally:
            root.removeHandler(h)

        # reset_instance WITHOUT reset_bootstrap_state strands the latch.
        SecretRegistry.reset_instance()
        assert is_bootstrapped()
        initialize_secret_masking()
        reg2 = SecretRegistry.get_instance()
        reg2.register_secret("PW", "second_secret_value")
        cap2 = StringIO()
        h2 = logging.StreamHandler(cap2)
        h2.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(h2)
        try:
            log = logging.getLogger("test.reinit.rebind2")
            log.setLevel(logging.INFO)
            log.info("leak second_secret_value here")
            out = cap2.getvalue()
            assert "second_secret_value" not in out, (
                f"re-init did not rebind filter; cleartext leaked: {out!r}"
            )
            assert "***REDACTED:PW***" in out
        finally:
            root.removeHandler(h2)

    def test_reinit_re_covers_handler_added_while_inert(self):
        from io import StringIO

        initialize_secret_masking()
        log = logging.getLogger("test.reinit.rescan")
        log.handlers.clear()
        log.propagate = True
        log.setLevel(logging.INFO)
        # Append a handler bypassing addHandler so the wrap misses it.
        plain = logging.StreamHandler(StringIO())
        plain.setFormatter(logging.Formatter("%(message)s"))
        log.handlers.append(plain)
        assert not any(isinstance(f, SecretMaskingFilter) for f in plain.filters)
        # Re-initialize; the scan attaches the filter to plain.
        initialize_secret_masking()
        assert any(isinstance(f, SecretMaskingFilter) for f in plain.filters)
        log.handlers.clear()
