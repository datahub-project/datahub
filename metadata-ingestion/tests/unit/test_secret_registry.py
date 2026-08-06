"""Test SecretRegistry singleton and is_masking_enabled function."""

import pytest

from datahub.masking.secret_registry import SecretRegistry, is_masking_enabled


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
        import logging as _logging

        registry = SecretRegistry()
        registry.clear()
        registry.register_secret("KEY1", "duplicate_value")
        records: list = []

        class _Capture(_logging.Handler):
            def emit(self, record):
                records.append(record)

        _logger = _logging.getLogger("datahub.masking.secret_registry")
        _logger.addHandler(_Capture())
        try:
            # Re-register the same value — duplicate, not capacity.
            registry.register_secret("KEY1_DUP", "duplicate_value")
        finally:
            _logger.removeHandler(_Capture())
        assert not any("at capacity" in r.getMessage() for r in records), (
            "duplicate registration fired a spurious capacity warning"
        )
        assert registry.get_count() > 0  # the first registration is still there

    def test_capacity_warning_fires_once_not_per_call(self):
        """The capacity warning fires once per rise to capacity, not per call."""
        import logging as _logging

        registry = SecretRegistry()
        registry.clear()
        records: list = []

        class _Capture(_logging.Handler):
            def emit(self, record):
                records.append(record)

        _logger = _logging.getLogger("datahub.masking.secret_registry")
        _logger.addHandler(_Capture())
        try:
            with pytest.MonkeyPatch.context() as m:
                m.setattr(SecretRegistry, "MAX_SECRETS", 10)
                for i in range(20):
                    registry.register_secret(f"K{i}", f"pa:ss@wo/rd{i}")
        finally:
            _logger.removeHandler(_Capture())
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
        import logging as _logging

        registry = SecretRegistry()
        registry.clear()
        batch = {f"K{i}": f"batch_value_{i}" for i in range(20)}
        registry.register_secrets_batch(batch)
        records: list = []

        class _Capture(_logging.Handler):
            def emit(self, record):
                records.append(record)

        _logger = _logging.getLogger("datahub.masking.secret_registry")
        _logger.addHandler(_Capture())
        try:
            # Re-register the same batch — all duplicates, no capacity.
            registry.register_secrets_batch(batch)
        finally:
            _logger.removeHandler(_Capture())
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
        import logging as _logging

        with pytest.MonkeyPatch.context() as m:
            m.setattr(SecretRegistry, "MAX_SECRETS", 10)
            handler_records: list = []

            class _Capture(_logging.Handler):
                def emit(self, record):
                    handler_records.append(record)

            _logger = _logging.getLogger("datahub.masking.secret_registry")
            _logger.addHandler(_Capture())
            try:
                for i in range(30):
                    registry.register_secret(f"K2_{i}", f"other_value_{i}")
            finally:
                _logger.removeHandler(_Capture())
        capacity_warnings = [
            r for r in handler_records if "at capacity" in r.getMessage()
        ]
        assert len(capacity_warnings) == 1, (
            f"expected the capacity warning to fire again after room was freed, "
            f"got {len(capacity_warnings)}"
        )
