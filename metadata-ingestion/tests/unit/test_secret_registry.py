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
