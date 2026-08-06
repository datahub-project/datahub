"""Test SecretRegistry singleton and is_masking_enabled function."""

import contextlib
import logging
from typing import Iterator

import pytest

from datahub.masking.secret_registry import SecretRegistry, is_masking_enabled


@contextlib.contextmanager
def _capture_records(logger_name: str) -> Iterator[list]:
    """Attach a handler to ``logger_name`` for the duration of the block and
    remove the *same* handler object on exit.

    The masking loggers have ``propagate=False``, so pytest's ``caplog``
    (attached to root) doesn't see their records — tests must attach a
    handler directly. The original code did ``_logger.removeHandler(_Capture())``,
    which removes a *new* instance and leaves the original attached, accumulating
    records across tests and duplicating them into later assertions. Binding the
    handler to a variable and removing that same object fixes the leak.
    """
    logger = logging.getLogger(logger_name)
    records: list = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Capture()
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)


class TestSecretRegistrySingleton:
    """Test SecretRegistry singleton behavior."""

    def test_get_instance_returns_singleton(self):
        """get_instance should return the same instance."""
        instance1 = SecretRegistry.get_instance()
        instance2 = SecretRegistry.get_instance()

        assert instance1 is instance2

    def test_reset_instance_clears_singleton(self):
        """reset_instance should clear the singleton."""
        instance1 = SecretRegistry.get_instance()

        SecretRegistry.reset_instance()

        instance2 = SecretRegistry.get_instance()

        # Should be a new instance
        assert instance1 is not instance2


class TestIsMaskingEnabled:
    """Test is_masking_enabled function."""

    def test_masking_enabled_by_default(self):
        """Masking should be enabled when env var is not set."""
        with pytest.MonkeyPatch.context() as m:
            m.delenv("DATAHUB_DISABLE_SECRET_MASKING", raising=False)
            assert is_masking_enabled() is True

    def test_masking_disabled_with_true(self):
        """Masking should be disabled when env var is 'true'."""
        with pytest.MonkeyPatch.context() as m:
            m.setenv("DATAHUB_DISABLE_SECRET_MASKING", "true")
            assert is_masking_enabled() is False

    def test_masking_disabled_with_1(self):
        """Masking should be disabled when env var is '1'."""
        with pytest.MonkeyPatch.context() as m:
            m.setenv("DATAHUB_DISABLE_SECRET_MASKING", "1")
            assert is_masking_enabled() is False

    def test_masking_enabled_with_false(self):
        """Masking should be enabled when env var is 'false'."""
        with pytest.MonkeyPatch.context() as m:
            m.setenv("DATAHUB_DISABLE_SECRET_MASKING", "false")
            assert is_masking_enabled() is True


class TestSecretRegistryVersionTracking:
    """Test SecretRegistry version tracking."""

    def test_get_version_increments_on_register(self):
        """Version should increment when secrets are registered."""
        registry = SecretRegistry()
        registry.clear()

        initial_version = registry.get_version()

        registry.register_secret("KEY1", "value1")

        assert registry.get_version() > initial_version

    def test_get_version_unchanged_after_clear(self):
        """Version tracking after clear."""
        registry = SecretRegistry()
        registry.clear()

        version_after_clear = registry.get_version()

        registry.register_secret("KEY1", "value1")
        new_version = registry.get_version()

        assert new_version > version_after_clear


class TestSecretRegistryGetAllSecrets:
    """Test get_all_secrets method."""

    def test_get_all_secrets_returns_copy(self):
        """get_all_secrets should return a copy of secrets dict."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", "value1")

        secrets1 = registry.get_all_secrets()
        secrets2 = registry.get_all_secrets()

        # Should be different dict objects
        assert secrets1 is not secrets2
        # But with same content
        assert secrets1 == secrets2


class TestSecretRegistryInvalidInputs:
    """Test SecretRegistry with invalid inputs."""

    def test_register_empty_string_value_ignored(self):
        """Empty string values should be ignored."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", "")

        secrets = registry.get_all_secrets()
        assert len(secrets) == 0

    def test_register_short_string_value_ignored(self):
        """Strings shorter than 3 characters should be ignored."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", "ab")
        registry.register_secret("KEY2", "x")

        secrets = registry.get_all_secrets()
        assert len(secrets) == 0

    def test_register_non_string_value_ignored(self):
        """Non-string values should be ignored."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", 123)  # type: ignore

        secrets = registry.get_all_secrets()
        assert len(secrets) == 0

    def test_register_none_value_ignored(self):
        """None values should be ignored."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", None)  # type: ignore

        secrets = registry.get_all_secrets()
        assert len(secrets) == 0

    def test_duplicate_secret_value_uses_first_name(self):
        """Registering same value twice should use first variable name."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", "same_value")
        registry.register_secret("KEY2", "same_value")

        secrets = registry.get_all_secrets()

        # Should have only one entry
        assert len(secrets) == 1
        # Should use the first name
        assert secrets["same_value"] == "KEY1"

    def test_register_duplicate_with_different_case_treats_as_different(self):
        """Secret values are case-sensitive."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", "secret")
        registry.register_secret("KEY2", "SECRET")

        secrets = registry.get_all_secrets()

        # Should have two entries
        assert len(secrets) == 2
        assert "secret" in secrets
        assert "SECRET" in secrets

    def test_register_secret_with_special_chars_registers_url_encoded(self):
        """Secrets with special characters should also register SQLAlchemy-style URL-encoded version."""
        registry = SecretRegistry()
        registry.clear()

        # Password with special characters that SQLAlchemy encodes (only : @ /)
        registry.register_secret("password", "P#!ss@word")

        secrets = registry.get_all_secrets()

        # Should have both raw and SQLAlchemy-style encoded versions
        # SQLAlchemy only encodes : @ / (not # or !)
        assert "P#!ss@word" in secrets
        assert "P#!ss%40word" in secrets  # Only @ encoded to %40
        assert secrets["P#!ss@word"] == "password"
        assert secrets["P#!ss%40word"] == "password"


class TestSecretRegistryMaxSecrets:
    """Test max secrets limit."""

    def test_register_stops_at_max_secrets(self):
        """Registry should stop accepting secrets after MAX_SECRETS."""
        registry = SecretRegistry()
        registry.clear()

        # Register MAX_SECRETS
        for i in range(SecretRegistry.MAX_SECRETS):
            registry.register_secret(f"KEY_{i}", f"value_{i}")

        secrets_at_max = registry.get_all_secrets()
        count_at_max = len(secrets_at_max)

        # Try to register one more
        registry.register_secret("EXTRA_KEY", "extra_value")

        secrets_after = registry.get_all_secrets()

        # Should not have increased
        assert len(secrets_after) == count_at_max


class TestRegisterSecretsBatch:
    """Test batch registration of secrets."""

    def test_register_secrets_batch_with_dict(self):
        """Should register multiple secrets at once."""
        registry = SecretRegistry()
        registry.clear()

        secrets = {"KEY1": "value1", "KEY2": "value2", "KEY3": "value3"}

        registry.register_secrets_batch(secrets)

        all_secrets = registry.get_all_secrets()

        assert "value1" in all_secrets
        assert "value2" in all_secrets
        assert "value3" in all_secrets

    def test_register_secrets_batch_with_empty_dict(self):
        """Should handle empty dict gracefully."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secrets_batch({})

        # Should not raise

    def test_register_secrets_batch_filters_invalid_values(self):
        """Should filter out invalid values in batch registration."""
        registry = SecretRegistry()
        registry.clear()

        secrets = {
            "VALID": "valid_value",
            "EMPTY": "",
            "SHORT": "ab",  # Too short (< 3 chars)
            "NONE": None,  # type: ignore
            "INT": 123,  # type: ignore
        }

        registry.register_secrets_batch(secrets)  # type: ignore[arg-type]

        all_secrets = registry.get_all_secrets()

        # Only valid should be registered
        assert "valid_value" in all_secrets
        assert len(all_secrets) == 1

    def test_register_secrets_batch_with_special_chars_registers_url_encoded(self):
        """Batch registration should also register SQLAlchemy-style URL-encoded versions."""
        registry = SecretRegistry()
        registry.clear()

        secrets = {
            "password1": "P#!ss@word",  # Has @ which SQLAlchemy encodes
            "password2": "simplepass",  # No special chars
        }

        registry.register_secrets_batch(secrets)

        all_secrets = registry.get_all_secrets()

        # First password should have SQLAlchemy-style encoded version
        # SQLAlchemy only encodes : @ / (not # or !)
        assert "P#!ss@word" in all_secrets
        assert "P#!ss%40word" in all_secrets  # Only @ encoded

        # Second password should not have duplicate
        assert "simplepass" in all_secrets


class TestClearRegistry:
    """Test clearing the registry."""

    def test_clear_removes_all_secrets(self):
        """clear() should remove all secrets."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", "value1")
        registry.register_secret("KEY2", "value2")

        assert len(registry.get_all_secrets()) == 2

        registry.clear()

        assert len(registry.get_all_secrets()) == 0

    def test_clear_increments_version(self):
        """clear() should increment version."""
        registry = SecretRegistry()
        registry.clear()

        version_before = registry.get_version()

        registry.register_secret("KEY", "value")
        registry.clear()

        version_after = registry.get_version()

        assert version_after > version_before


class TestAdmitCapacityAndDuplicates:
    """Regression tests for the _admit_locked tri-state refactor.

    The previous bool helper conflated "duplicate" with "at capacity":
    both returned False, and both call sites treated False as capacity.
    That made duplicate registrations emit spurious "at capacity" warnings
    (with zero secrets registered) and permanently suppress the real one
    via _capacity_warned. It also let batches of multi-key-expanding values
    overshoot MAX_SECRETS, because the capacity check counted secrets
    instead of expanded keys (the unit mismatch).
    """

    def test_duplicate_registration_produces_no_capacity_warning(self):
        """A duplicate registration must not fire the "at capacity" warning."""
        registry = SecretRegistry()
        registry.clear()
        registry.register_secret("KEY1", "duplicate_value")
        with _capture_records("datahub.masking.secret_registry") as records:
            # Re-register the same value — duplicate, not capacity.
            registry.register_secret("KEY1_DUP", "duplicate_value")
        assert not any("at capacity" in r.getMessage() for r in records), (
            "duplicate registration fired a spurious capacity warning"
        )
        assert registry.get_count() > 0  # the first registration is still there

    def test_capacity_warning_fires_once_not_per_call(self):
        """The capacity warning fires once per rise to capacity, not per call."""
        registry = SecretRegistry()
        registry.clear()
        with (
            _capture_records("datahub.masking.secret_registry") as records,
            pytest.MonkeyPatch.context() as m,
        ):
            m.setattr(SecretRegistry, "MAX_SECRETS", 10)
            for i in range(20):
                registry.register_secret(f"K{i}", f"pa:ss@wo/rd{i}")
        capacity_warnings = [r for r in records if "at capacity" in r.getMessage()]
        assert len(capacity_warnings) == 1, (
            f"expected exactly one capacity warning, got {len(capacity_warnings)}"
        )

    def test_batch_does_not_overshoot_max_secrets(self):
        """A batch of multi-key-expanding values must not push the expanded
        key count past MAX_SECRETS. This is the regression test for the
        unit mismatch: the old batch check counted secrets, not expanded
        keys, so it let the union overshoot. Fails on the old code."""
        registry = SecretRegistry()
        registry.clear()
        # Each value contains chars that trigger repr + sqlalchemy + json
        # variants, so each secret expands to ~4 keys.
        expanding_value = "pa:ss@wo/rd"
        batch = {f"K{i}": expanding_value + str(i) for i in range(50)}
        with pytest.MonkeyPatch.context() as m:
            m.setattr(SecretRegistry, "MAX_SECRETS", 20)
            registry.register_secrets_batch(batch)
            assert registry.get_count() <= SecretRegistry.MAX_SECRETS, (
                f"batch overshot MAX_SECRETS: get_count()={registry.get_count()} "
                f"> MAX_SECRETS={SecretRegistry.MAX_SECRETS}"
            )

    def test_batch_duplicate_does_not_count_as_rejected(self):
        """Re-registering an existing batch must not report every entry as
        rejected (the old 'skipped 20 of 20' on a healthy run)."""
        registry = SecretRegistry()
        registry.clear()
        batch = {f"K{i}": f"batch_value_{i}" for i in range(20)}
        registry.register_secrets_batch(batch)
        with _capture_records("datahub.masking.secret_registry") as records:
            # Re-register the same batch — all duplicates, no capacity.
            registry.register_secrets_batch(batch)
        assert not any("at capacity" in r.getMessage() for r in records), (
            "duplicate batch fired a spurious capacity warning"
        )

    def test_capacity_warning_can_fire_again_after_room_freed(self):
        """After executions end and free room, _capacity_warned resets so
        the next capacity rise warns again (not permanently suppressed)."""
        registry = SecretRegistry()
        registry.clear()
        exec_id = registry.begin_execution()
        try:
            with pytest.MonkeyPatch.context() as m:
                m.setattr(SecretRegistry, "MAX_SECRETS", 10)
                # Rise to capacity — warns once.
                for i in range(30):
                    registry.register_secret(f"K{i}", f"secret_value_{i}")
                # End the execution — frees room, _rebuild_locked resets
                # _capacity_warned to False.
        finally:
            registry.end_execution(exec_id)
        assert registry.get_count() == 0  # all dropped
        # Now rise again — should warn again (not suppressed by stale flag).
        with pytest.MonkeyPatch.context() as m:
            m.setattr(SecretRegistry, "MAX_SECRETS", 10)
            with _capture_records("datahub.masking.secret_registry") as handler_records:
                for i in range(30):
                    registry.register_secret(f"K2_{i}", f"other_value_{i}")
        capacity_warnings = [
            r for r in handler_records if "at capacity" in r.getMessage()
        ]
        assert len(capacity_warnings) == 1, (
            f"expected the capacity warning to fire again after room was freed, "
            f"got {len(capacity_warnings)}"
        )


class TestEnsureExecutionStaleToken:
    """Regression: ensure_execution returns the ambient contextvar value
    without checking it still names a live group. A token-based end_execution
    from another thread drops the group but leaves the originating context's
    contextvar pointing at the dead id, so a later ensure_execution recreates
    a scope under a dead id and a later token holder targets the wrong scope.
    """

    def test_ensure_execution_revalidates_ambient_after_cross_thread_end(self):
        import threading

        registry = SecretRegistry()
        registry.clear()
        tok = registry.ensure_execution()
        registry.register_secret("A", "aaa_secret_1")
        assert tok in registry._groups

        # End from another thread using the token. This drops the group but
        # does NOT clear this context's contextvar (exec_id != ambient).
        t = threading.Thread(target=lambda: registry.end_execution(tok))
        t.start()
        t.join(timeout=5.0)
        assert not t.is_alive(), "worker thread timed out"
        # The group is gone, but the ambient contextvar still holds tok.
        assert tok not in registry._groups

        # ensure_execution must revalidate: tok names no live group, so it
        # must open a fresh scope rather than return the stale tok.
        new_tok = registry.ensure_execution()
        assert new_tok != tok, (
            "ensure_execution returned a stale ambient id that names no live group"
        )
        assert new_tok in registry._groups


class TestCaptureRecordsNoLeak:
    """Regression for Item 6: the inline ``_logger.removeHandler(_Capture())``
    pattern removed a *new* instance, leaving the original handler attached.
    Across the four capacity/duplicate tests, leaked handlers accumulated
    and duplicated records into later assertions. The ``_capture_records``
    contextmanager binds the handler to a variable and removes the same object.
    """

    def test_capture_records_removes_handler_on_exit(self):
        logger = logging.getLogger("datahub.masking.secret_registry")
        # Precondition: clean slate (a prior leaked handler would violate this).
        logger.handlers.clear()
        with _capture_records("datahub.masking.secret_registry"):
            assert len(logger.handlers) == 1, "handler not attached"
        assert logger.handlers == [], (
            "handler leaked after context exit — removeHandler removed a new "
            "instance instead of the one that was added"
        )

    def test_capture_records_does_not_duplicate_across_invocations(self):
        """A leaked handler from a first invocation would duplicate records
        into a second invocation's capture. Each record must appear once."""
        logger = logging.getLogger("datahub.masking.secret_registry")
        logger.handlers.clear()
        registry = SecretRegistry()
        registry.clear()

        # First invocation: register a secret (no capacity warning expected).
        with _capture_records("datahub.masking.secret_registry"):
            registry.register_secret("FIRST", "first_secret_value")

        # Second invocation: trigger a capacity warning.
        with (
            _capture_records("datahub.masking.secret_registry") as records2,
            pytest.MonkeyPatch.context() as m,
        ):
            m.setattr(SecretRegistry, "MAX_SECRETS", 5)
            for i in range(20):
                registry.register_secret(f"K{i}", f"pa:ss@wo/rd{i}")

        capacity_warnings = [r for r in records2 if "at capacity" in r.getMessage()]
        # If the first invocation's handler leaked, each warning would appear
        # twice (once via the leaked handler, once via the new one). Assert each
        # record object is captured exactly once.
        ids = [id(r) for r in records2]
        assert len(ids) == len(set(ids)), (
            "records duplicated across invocations by a leaked handler"
        )
        assert len(capacity_warnings) >= 1


class TestBatchRejectionCountInWarning:
    """Item 8 (optional): when a batch is partly rejected at capacity, the
    WARNING names one variable while the admitted/duplicates/rejected counts
    go to DEBUG (off in production). An operator sees 'Skipping registration
    of S_25' with no indication that 174 others were dropped. The rejected
    count must appear in the warning so an operator can see the blast radius.
    """

    def test_batch_rejection_count_appears_in_warning(self):
        registry = SecretRegistry()
        registry.clear()
        # 200 secrets, each expanding to ~4 keys; cap at 20 keys -> most rejected.
        batch = {f"S_{i}": f"pa:ss@wo/rd{i}" for i in range(200)}
        with pytest.MonkeyPatch.context() as m:
            m.setattr(SecretRegistry, "MAX_SECRETS", 20)
            with _capture_records("datahub.masking.secret_registry") as records:
                registry.register_secrets_batch(batch)
        warnings = [r for r in records if "at capacity" in r.getMessage()]
        assert len(warnings) == 1, f"expected one capacity warning, got {len(warnings)}"
        msg = warnings[0].getMessage()
        # The warning must surface the rejected count and the batch total, not
        # just one variable name. The current per-secret warning names only
        # S_25 with no indication of the 174 others dropped.
        assert "Skipped" in msg or "skipped" in msg.lower(), (
            f"warning didn't use 'Skipped' wording: {msg!r}"
        )
        # The batch total (200) must appear so an operator sees the blast radius.
        assert "200" in msg, f"warning didn't surface the batch total: {msg!r}"
