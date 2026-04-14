"""
Tests for TransparentSecretStr type and model_dump_redacted().

TransparentSecretStr serializes as plain str in model_dump()/model_dump_json()
while preserving SecretStr masking in repr/str and secret registry registration.
"""

import json
from typing import Optional

import pytest
from pydantic import Field, SecretStr

from datahub.configuration.common import ConfigModel, TransparentSecretStr
from datahub.masking.secret_registry import SecretRegistry


class SimpleConfig(ConfigModel):
    password: TransparentSecretStr = Field(description="A password")
    host: str = Field(default="localhost", description="Host")


class NestedAuthConfig(ConfigModel):
    client_secret: TransparentSecretStr = Field(description="Client secret")
    client_id: str = Field(description="Client ID")


class ParentConfig(ConfigModel):
    auth: NestedAuthConfig = Field(description="Auth config")
    name: str = Field(description="Name")


class OptionalSecretConfig(ConfigModel):
    token: Optional[TransparentSecretStr] = Field(
        default=None, description="Optional token"
    )
    host: str = Field(default="localhost", description="Host")


class ExcludedFieldConfig(ConfigModel):
    password: TransparentSecretStr = Field(description="Password", exclude=True)
    host: str = Field(default="localhost", description="Host")


class MixedSecretConfig(ConfigModel):
    """Config with both TransparentSecretStr and regular SecretStr."""

    transparent_pw: TransparentSecretStr = Field(description="Transparent password")
    regular_secret: SecretStr = Field(description="Regular secret")
    host: str = Field(default="localhost", description="Host")


class ListNestedConfig(ConfigModel):
    auths: list[NestedAuthConfig] = Field(description="List of auths")
    name: str = Field(description="Name")


class DictNestedConfig(ConfigModel):
    auths: dict[str, NestedAuthConfig] = Field(description="Dict of auths")
    name: str = Field(description="Name")


class TestSerializationFidelity:
    """model_dump(), model_dump_json(), json.dumps(model_dump()) all return plain str."""

    def test_model_dump_returns_plain_str(self):
        config = SimpleConfig(password="s3cret")
        d = config.model_dump()
        assert d["password"] == "s3cret"
        assert isinstance(d["password"], str)

    def test_model_dump_json_returns_unmasked(self):
        config = SimpleConfig(password="s3cret")
        j = config.model_dump_json()
        parsed = json.loads(j)
        assert parsed["password"] == "s3cret"

    def test_model_dump_mode_json(self):
        config = SimpleConfig(password="s3cret")
        d = config.model_dump(mode="json")
        assert d["password"] == "s3cret"
        assert isinstance(d["password"], str)

    def test_json_dumps_model_dump_no_typeerror(self):
        config = SimpleConfig(password="s3cret")
        result = json.dumps(config.model_dump())
        parsed = json.loads(result)
        assert parsed["password"] == "s3cret"

    def test_nested_model_dump_returns_plain_str(self):
        config = ParentConfig.model_validate(
            {
                "auth": {"client_secret": "nested-secret", "client_id": "cid"},
                "name": "test",
            }
        )
        d = config.model_dump()
        assert d["auth"]["client_secret"] == "nested-secret"
        assert isinstance(d["auth"]["client_secret"], str)

    def test_nested_json_dumps_roundtrip(self):
        config = ParentConfig.model_validate(
            {
                "auth": {"client_secret": "nested-secret", "client_id": "cid"},
                "name": "test",
            }
        )
        result = json.dumps(config.model_dump())
        parsed = json.loads(result)
        assert parsed["auth"]["client_secret"] == "nested-secret"


class TestMaskingPreservation:
    """str()/repr() on the field still return '**********'."""

    def test_str_masked(self):
        config = SimpleConfig(password="s3cret")
        assert "s3cret" not in str(config.password)
        assert "**********" in str(config.password)

    def test_repr_masked(self):
        config = SimpleConfig(password="s3cret")
        assert "s3cret" not in repr(config.password)

    def test_model_repr_masked(self):
        config = SimpleConfig(password="s3cret")
        r = repr(config)
        assert "s3cret" not in r


class TestTypeIdentity:
    """isinstance(field, SecretStr) is True, .get_secret_value() works."""

    def test_isinstance_secret_str(self):
        config = SimpleConfig(password="s3cret")
        assert isinstance(config.password, SecretStr)

    def test_get_secret_value(self):
        config = SimpleConfig(password="s3cret")
        assert config.password.get_secret_value() == "s3cret"

    def test_accepts_secret_str_input(self):
        config = SimpleConfig(password=SecretStr("s3cret"))
        assert config.password.get_secret_value() == "s3cret"
        d = config.model_dump()
        assert d["password"] == "s3cret"


class TestSecretRegistration:
    """_collect_secrets() and SecretRegistry detect TransparentSecretStr fields."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_direct_field_registered(self):
        _config = SimpleConfig(password="reg-secret")
        registry = SecretRegistry.get_instance()
        assert registry.has_secret("password")
        assert registry.get_secret_value("password") == "reg-secret"

    def test_nested_field_registered(self):
        _config = ParentConfig.model_validate(
            {
                "auth": {"client_secret": "nested-reg", "client_id": "cid"},
                "name": "test",
            }
        )
        registry = SecretRegistry.get_instance()
        assert registry.has_secret("auth.client_secret")
        assert registry.get_secret_value("auth.client_secret") == "nested-reg"

    def test_collect_secrets_finds_transparent(self):
        config = SimpleConfig(password="collected")
        secrets: dict[str, str] = {}
        config._collect_secrets(secrets, prefix="")
        assert "password" in secrets
        assert secrets["password"] == "collected"

    def test_empty_secret_not_registered(self):
        _config = SimpleConfig(password="")
        registry = SecretRegistry.get_instance()
        assert registry.get_count() == 0


class TestEdgeCases:
    """Optional[TransparentSecretStr] with None, empty, exclude, defaults."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_optional_none(self):
        config = OptionalSecretConfig(token=None)
        d = config.model_dump()
        assert d["token"] is None

    def test_optional_with_value(self):
        config = OptionalSecretConfig(token="tok-123")
        d = config.model_dump()
        assert d["token"] == "tok-123"
        assert isinstance(config.token, SecretStr)

    def test_excluded_field_not_in_dump(self):
        config = ExcludedFieldConfig(password="excluded-pw")
        d = config.model_dump()
        assert "password" not in d

    def test_excluded_field_still_accessible(self):
        config = ExcludedFieldConfig(password="excluded-pw")
        assert config.password.get_secret_value() == "excluded-pw"

    def test_empty_string_serializes(self):
        config = SimpleConfig(password="")
        d = config.model_dump()
        assert d["password"] == ""


class TestTransportFidelity:
    """YAML → parse → model_dump() → json.dumps() → parse round-trip."""

    def test_dict_roundtrip(self):
        raw = {"password": "round-trip-pw", "host": "db.example.com"}
        config = SimpleConfig.model_validate(raw)
        dumped = config.model_dump()
        serialized = json.dumps(dumped)
        restored = json.loads(serialized)
        assert restored["password"] == "round-trip-pw"
        assert restored["host"] == "db.example.com"

    def test_nested_dict_roundtrip(self):
        raw = {
            "auth": {"client_secret": "rt-secret", "client_id": "cid"},
            "name": "test",
        }
        config = ParentConfig.model_validate(raw)
        dumped = config.model_dump()
        serialized = json.dumps(dumped)
        restored = json.loads(serialized)
        assert restored["auth"]["client_secret"] == "rt-secret"


class TestModelDumpRedacted:
    """model_dump_redacted() masks all SecretStr/TransparentSecretStr fields."""

    def test_direct_field_masked(self):
        config = SimpleConfig(password="visible")
        d = config.model_dump_redacted()
        assert d["password"] == "********"
        assert d["host"] == "localhost"

    def test_nested_field_masked(self):
        config = ParentConfig.model_validate(
            {
                "auth": {"client_secret": "visible", "client_id": "cid"},
                "name": "test",
            }
        )
        d = config.model_dump_redacted()
        assert d["auth"]["client_secret"] == "********"
        assert d["auth"]["client_id"] == "cid"
        assert d["name"] == "test"

    def test_mixed_secrets_masked(self):
        config = MixedSecretConfig(transparent_pw="tp", regular_secret="rs", host="h")
        d = config.model_dump_redacted()
        assert d["transparent_pw"] == "********"
        assert d["regular_secret"] == "********"
        assert d["host"] == "h"

    def test_list_nested_masked(self):
        config = ListNestedConfig.model_validate(
            {
                "auths": [
                    {"client_secret": "s1", "client_id": "c1"},
                    {"client_secret": "s2", "client_id": "c2"},
                ],
                "name": "test",
            }
        )
        d = config.model_dump_redacted()
        assert d["auths"][0]["client_secret"] == "********"
        assert d["auths"][1]["client_secret"] == "********"
        assert d["auths"][0]["client_id"] == "c1"

    def test_dict_nested_masked(self):
        config = DictNestedConfig.model_validate(
            {
                "auths": {
                    "prod": {"client_secret": "ps", "client_id": "pc"},
                    "dev": {"client_secret": "ds", "client_id": "dc"},
                },
                "name": "test",
            }
        )
        d = config.model_dump_redacted()
        assert d["auths"]["prod"]["client_secret"] == "********"
        assert d["auths"]["dev"]["client_secret"] == "********"
        assert d["auths"]["prod"]["client_id"] == "pc"

    def test_optional_none_in_redacted(self):
        config = OptionalSecretConfig(token=None)
        d = config.model_dump_redacted()
        assert d["token"] is None

    def test_optional_present_in_redacted(self):
        config = OptionalSecretConfig(token="secret-tok")
        d = config.model_dump_redacted()
        assert d["token"] == "********"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
