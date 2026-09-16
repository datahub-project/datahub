"""Test SecretRegistry singleton and is_masking_enabled function."""

from unittest.mock import patch

import pytest

from datahub.masking import secret_registry as registry_module
from datahub.masking.secret_registry import (
    LARGE_SECRET_RENDERING_COUNT,
    MAX_SECRET_VERSIONS,
    SecretRegistry,
    is_masking_enabled,
)


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


class TestCapacityFailClosed:
    def test_overflow_marks_registry_capacity_exceeded(self, monkeypatch):
        monkeypatch.setattr(SecretRegistry, "MAX_SECRETS", 3)
        registry = SecretRegistry()
        registry.register_secrets_batch(
            {f"KEY_{i}": f"secret-value-number-{i}" for i in range(5)}
        )
        assert registry.is_capacity_exceeded()
        assert len(registry.get_all_secrets()) == 3

    def test_clear_resets_capacity_exceeded(self, monkeypatch):
        monkeypatch.setattr(SecretRegistry, "MAX_SECRETS", 1)
        registry = SecretRegistry()
        registry.register_secrets_batch(
            {"KEY_A": "secret-value-alpha", "KEY_B": "secret-value-beta"}
        )
        assert registry.is_capacity_exceeded()
        registry.clear()
        assert not registry.is_capacity_exceeded()


class TestVersionCap:
    def test_only_last_max_versions_stay_maskable(self):
        registry = SecretRegistry()
        for generation in range(MAX_SECRET_VERSIONS + 1):
            registry.register_secret("API_KEY", f"api-key-value-gen{generation}")
        secrets = registry.get_all_secrets()
        assert "api-key-value-gen0" not in secrets
        for generation in range(1, MAX_SECRET_VERSIONS + 1):
            assert secrets[f"api-key-value-gen{generation}"] == "API_KEY"
        assert registry.get_registered_secrets() == {
            "API_KEY": f"api-key-value-gen{MAX_SECRET_VERSIONS}"
        }

    def test_eviction_spares_value_retained_under_another_name(self):
        registry = SecretRegistry()
        registry.register_secret("NAME_A", "shared-secret-material")
        registry.register_secret("NAME_B", "shared-secret-material")
        for generation in range(MAX_SECRET_VERSIONS):
            registry.register_secret("NAME_A", f"name-a-value-gen{generation}")
        assert "shared-secret-material" in registry.get_all_secrets()

    def test_eviction_without_additions_still_bumps_version(self):
        registry = SecretRegistry()
        registry.register_secret("NAME_B", "duplicate-value-material")
        for generation in range(MAX_SECRET_VERSIONS):
            registry.register_secret("NAME_A", f"name-a-value-gen{generation}")
        version_before = registry.get_version()
        registry.register_secret("NAME_A", "duplicate-value-material")
        assert registry.get_version() > version_before
        assert "name-a-value-gen0" not in registry.get_all_secrets()

    def test_reregistered_historical_value_becomes_current(self):
        registry = SecretRegistry()
        registry.register_secret("TOKEN", "token-value-first")
        registry.register_secret("TOKEN", "token-value-second")
        registry.register_secret("TOKEN", "token-value-first")
        assert registry.get_registered_secrets() == {"TOKEN": "token-value-first"}
        assert "token-value-second" in registry.get_all_secrets()


class TestLargeSecretWarning:
    def test_large_secret_warns_but_is_still_masked(self):
        registry = SecretRegistry()
        big_value = "\n".join(
            f"line-number-{i:04d}-material" for i in range(LARGE_SECRET_RENDERING_COUNT)
        )
        with patch.object(registry_module.logger, "warning") as warning:
            registry.register_secret("BIG_KEY", big_value)
        assert any("unusually large" in str(call) for call in warning.call_args_list)
        assert big_value in registry.get_all_secrets()


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


class TestUnprotectableValues:
    def test_trivial_literals_are_not_registered(self):
        registry = SecretRegistry()
        for value in ("True", "false", "YES", "no", "None", "null"):
            registry.register_secret("FLAG", value)
        assert registry.get_count() == 0
        assert not registry.has_secret("FLAG")

    def test_trivial_literal_rejection_is_logged(self):
        registry = SecretRegistry()
        with patch.object(registry_module.logger, "warning") as mock_warning:
            registry.register_secret("MY_FLAG", "true")
        logged = " ".join(str(c) for c in mock_warning.call_args_list)
        assert "MY_FLAG" in logged
        assert "NOT be masked" in logged

    def test_marker_shaped_value_is_not_registered(self):
        registry = SecretRegistry()
        registry.register_secret("EVIL", "***REDACTED:OTHER*** trailer")
        assert registry.get_count() == 0


class TestMultiLineFragments:
    def test_each_substantial_line_is_registered(self):
        key = (
            "multiline-secret-header-line\n"
            "bXVsdGlsaW5lLXNlY3JldC1ib2R5LWxpbmU\n"
            "multiline-secret-footer-line"
        )
        registry = SecretRegistry()
        registry.register_secret("GCP_KEY", key)
        secrets = registry.get_all_secrets()
        for line in key.splitlines():
            assert secrets[line] == "GCP_KEY"

    def test_cr_separated_secret_registers_fragments(self):
        registry = SecretRegistry()
        registry.register_secret(
            "LEGACY_KEY", "first-cr-line-material\rsecond-cr-line-material"
        )
        secrets = registry.get_all_secrets()
        assert secrets["first-cr-line-material"] == "LEGACY_KEY"
        assert secrets["second-cr-line-material"] == "LEGACY_KEY"

    def test_trailing_newline_registers_stripped_fragment(self):
        registry = SecretRegistry()
        registry.register_secret("TOKEN", "trailing-newline-token\n")
        assert "trailing-newline-token" in registry.get_all_secrets()

    def test_structural_short_lines_are_skipped(self):
        value = '{\n  "k": "longsecretbody123"\n}'
        registry = SecretRegistry()
        registry.register_secret("SA_JSON", value)
        secrets = registry.get_all_secrets()
        assert "{" not in secrets
        assert "}" not in secrets
        assert '"k": "longsecretbody123"' in secrets

    def test_fragments_get_variant_renderings(self):
        value = "first line filler text\ncol1\tabcdef123456"
        registry = SecretRegistry()
        registry.register_secret("CONF", value)
        secrets = registry.get_all_secrets()
        assert "col1\\tabcdef123456" in secrets


class TestRotationAndAccessors:
    def test_rotated_value_keeps_both_values_maskable(self):
        registry = SecretRegistry()
        registry.register_secret("TOKEN", "old-token-value")
        registry.register_secret("TOKEN", "new-token-value")
        secrets = registry.get_all_secrets()
        assert secrets["old-token-value"] == "TOKEN"
        assert secrets["new-token-value"] == "TOKEN"
        assert registry.get_secret_value("TOKEN") == "new-token-value"

    def test_get_registered_secrets_returns_name_to_value_copy(self):
        registry = SecretRegistry()
        registry.register_secret("A", "value-of-a")
        snapshot = registry.get_registered_secrets()
        assert snapshot == {"A": "value-of-a"}
        snapshot["B"] = "tamper"
        assert registry.get_registered_secrets() == {"A": "value-of-a"}

    def test_duplicate_value_under_new_name_updates_name_map_without_version_bump(
        self,
    ):
        registry = SecretRegistry()
        registry.register_secret("FIRST", "shared-value-123")
        version = registry.get_version()
        registry.register_secret("SECOND", "shared-value-123")
        assert registry.get_version() == version
        assert registry.get_all_secrets()["shared-value-123"] == "FIRST"
        assert registry.get_secret_value("SECOND") == "shared-value-123"


class TestSingleAndBatchEquivalence:
    def test_single_and_batch_registration_produce_identical_state(self):
        values = {
            "TOKEN": "tok-abc-123",
            "KEY": "line-one-long-enough\nline-two-long-enough",
            "URL_PASS": "p@ss:w/ord",
        }
        one_by_one = SecretRegistry()
        for name, value in values.items():
            one_by_one.register_secret(name, value)
        batched = SecretRegistry()
        batched.register_secrets_batch(values)
        assert one_by_one.get_all_secrets() == batched.get_all_secrets()
        assert one_by_one.get_registered_secrets() == batched.get_registered_secrets()


class TestMaskingDisabledGate:
    def test_registration_is_skipped_when_masking_disabled(self, monkeypatch):
        monkeypatch.setenv("DATAHUB_DISABLE_SECRET_MASKING", "true")
        registry = SecretRegistry()
        registry.register_secret("PW", "hunter2secret")
        registry.register_secrets_batch({"TOKEN": "tok-abc-123"})
        assert registry.get_count() == 0


class TestRepeatRegistration:
    def test_repeat_registration_is_a_noop(self):
        registry = SecretRegistry()
        registry.register_secrets_batch({"PW": "hunter2secret", "TOKEN": "tok-abc-123"})
        version = registry.get_version()
        secrets_before = registry.get_all_secrets()

        registry.register_secrets_batch({"PW": "hunter2secret", "TOKEN": "tok-abc-123"})

        assert registry.get_version() == version
        assert registry.get_all_secrets() == secrets_before

    def test_same_value_under_new_name_is_not_skipped(self):
        registry = SecretRegistry()
        registry.register_secret("FIRST", "shared-value-123")

        registry.register_secret("SECOND", "shared-value-123")

        assert registry.get_secret_value("SECOND") == "shared-value-123"
