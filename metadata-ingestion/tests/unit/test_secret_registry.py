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

        # Should have 3 entries: original + 2 quoted versions
        assert len(secrets) == 3
        # Should use the first name for all versions
        assert secrets["same_value"] == "KEY1"
        assert secrets['"same_value"'] == "KEY1"
        assert secrets["'same_value'"] == "KEY1"

    def test_register_duplicate_with_different_case_treats_as_different(self):
        """Secret values are case-sensitive."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", "secret")
        registry.register_secret("KEY2", "SECRET")

        secrets = registry.get_all_secrets()

        # Should have 6 entries: 2 originals + 4 quoted versions
        assert len(secrets) == 6
        assert "secret" in secrets
        assert "SECRET" in secrets
        assert '"secret"' in secrets
        assert "'secret'" in secrets
        assert '"SECRET"' in secrets
        assert "'SECRET'" in secrets


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

        # Only valid should be registered with its quoted versions
        assert "valid_value" in all_secrets
        assert '"valid_value"' in all_secrets
        assert "'valid_value'" in all_secrets
        assert len(all_secrets) == 3


class TestSecretRegistryQuotedVersions:
    """Test registration of quoted versions of secrets."""

    def test_register_secret_creates_quoted_versions(self):
        """Registering a secret should also register quoted versions."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("PASSWORD", "my_secret")

        secrets = registry.get_all_secrets()

        # Should have the original value
        assert "my_secret" in secrets

        # Should have double-quoted version
        assert '"my_secret"' in secrets

        # Should have single-quoted version
        assert "'my_secret'" in secrets

        # All should map to the same variable name
        assert secrets["my_secret"] == "PASSWORD"
        assert secrets['"my_secret"'] == "PASSWORD"
        assert secrets["'my_secret'"] == "PASSWORD"

    def test_register_secret_with_special_chars_creates_quoted_versions(self):
        """Secrets with special characters should register quoted versions."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("API_KEY", "key_with_special!@#")

        secrets = registry.get_all_secrets()

        assert "key_with_special!@#" in secrets
        assert '"key_with_special!@#"' in secrets
        assert "'key_with_special!@#'" in secrets

    def test_register_secrets_batch_creates_quoted_versions(self):
        """Batch registration should create quoted versions for all secrets."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secrets_batch(
            {"KEY1": "secret1", "KEY2": "secret2", "KEY3": "secret3"}
        )

        secrets = registry.get_all_secrets()

        # Verify all originals and their quoted versions exist
        for value in ["secret1", "secret2", "secret3"]:
            assert value in secrets
            assert f'"{value}"' in secrets
            assert f"'{value}'" in secrets

    def test_quoted_versions_help_mask_error_messages(self):
        """Quoted versions should help mask secrets in error messages.

        Registers a secret that might appear in error messages like:
        - "Role '8080' not found"
        - 'Port "8080" is already in use'
        """
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("DB_PORT", "8080")

        secrets = registry.get_all_secrets()

        # Verify quoted versions exist that could mask secrets in error messages
        assert "'8080'" in secrets
        assert '"8080"' in secrets

    def test_quoted_versions_not_created_for_short_values(self):
        """Quoted versions should not be created for values too short to register."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("SHORT", "ab")

        secrets = registry.get_all_secrets()

        # No secrets should be registered for values < 3 chars
        assert len(secrets) == 0
        assert "ab" not in secrets
        assert '"ab"' not in secrets
        assert "'ab'" not in secrets

    def test_quoted_versions_for_numeric_string_values(self):
        """Numeric string values should also get quoted versions."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("PORT", "8080")

        secrets = registry.get_all_secrets()

        assert "8080" in secrets
        assert '"8080"' in secrets
        assert "'8080'" in secrets

    def test_quoted_versions_already_exist_not_duplicated(self):
        """If a quoted version is already registered, it should not be duplicated."""
        registry = SecretRegistry()
        registry.clear()

        # Register a secret and its manually quoted version
        registry.register_secret("KEY1", "secret")
        registry.register_secret("KEY2", '"secret"')

        secrets = registry.get_all_secrets()

        # The quoted version should exist but map to the first registration (KEY1)
        # since it's registered automatically with the original
        assert '"secret"' in secrets
        assert secrets['"secret"'] == "KEY1"

    def test_mixed_quotes_in_secret_value(self):
        """Secrets containing quotes should still get additional quoted versions."""
        registry = SecretRegistry()
        registry.clear()

        # Secret value that already contains quotes
        registry.register_secret("API_KEY", 'my"secret')

        secrets = registry.get_all_secrets()

        # Original should be registered
        assert 'my"secret' in secrets

        # Quoted versions should also be registered
        assert '"my"secret"' in secrets
        assert "'my\"secret'" in secrets


class TestClearRegistry:
    """Test clearing the registry."""

    def test_clear_removes_all_secrets(self):
        """clear() should remove all secrets."""
        registry = SecretRegistry()
        registry.clear()

        registry.register_secret("KEY1", "value1")
        registry.register_secret("KEY2", "value2")

        # Should have 6 entries: 2 originals + 4 quoted versions
        assert len(registry.get_all_secrets()) == 6

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
