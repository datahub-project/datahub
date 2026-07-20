"""Tests for plugin configuration models."""

import os
from pathlib import Path

import pytest
import yaml

from datahub.plugin.plugin_config import (
    DiscoveredPlugin,
    PluginCapabilityEntry,
    PluginCapabilityType,
    PluginCompatibility,
    PluginManifest,
    PluginSystemConfig,
    RegistryConfig,
    SupportStatusType,
)


class TestPluginManifest:
    def test_minimal_manifest(self) -> None:
        manifest = PluginManifest(
            id="test-source",
            name="Test Source",
            type=PluginCapabilityType.SOURCE,
            entry_point="test_source.source:TestSource",
        )
        assert manifest.id == "test-source"
        assert manifest.api_version == "datahub/v1"
        assert manifest.type == PluginCapabilityType.SOURCE
        assert manifest.compatibility.datahub_min is None

    def test_full_manifest(self) -> None:
        manifest = PluginManifest(
            id="salesforce-source",
            name="Salesforce Source",
            type=PluginCapabilityType.SOURCE,
            entry_point="sf.source:SalesforceSource",
            config_class="sf.config:SalesforceConfig",
            description="Ingest from Salesforce",
            author="Acme Corp",
            compatibility=PluginCompatibility(
                datahub_min="0.12.0",
                python_min="3.9",
            ),
        )
        assert manifest.config_class == "sf.config:SalesforceConfig"
        assert manifest.compatibility.datahub_min == "0.12.0"

    def test_manifest_rejects_empty_id(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            PluginManifest(
                id="",
                name="Test",
                type=PluginCapabilityType.SOURCE,
                entry_point="mod:Cls",
            )

    def test_manifest_rejects_whitespace_id(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            PluginManifest(
                id="   ",
                name="Test",
                type=PluginCapabilityType.SOURCE,
                entry_point="mod:Cls",
            )

    def test_manifest_rejects_invalid_id_characters(self) -> None:
        with pytest.raises(ValueError, match="lowercase"):
            PluginManifest(
                id="My Plugin!!!",
                name="Test",
                type=PluginCapabilityType.SOURCE,
                entry_point="mod:Cls",
            )

    def test_manifest_rejects_uppercase_id(self) -> None:
        with pytest.raises(ValueError, match="lowercase"):
            PluginManifest(
                id="MySource",
                name="Test",
                type=PluginCapabilityType.SOURCE,
                entry_point="mod:Cls",
            )

    def test_manifest_accepts_underscores_in_id(self) -> None:
        manifest = PluginManifest(
            id="my_source",
            name="Test",
            type=PluginCapabilityType.SOURCE,
            entry_point="mod:Cls",
        )
        assert manifest.id == "my_source"

    def test_manifest_rejects_invalid_entry_point(self) -> None:
        with pytest.raises(ValueError, match="dotted import path"):
            PluginManifest(
                id="test-source",
                name="Test",
                type=PluginCapabilityType.SOURCE,
                entry_point="no_separator",
            )

    def test_manifest_rejects_empty_colon_entry_point(self) -> None:
        with pytest.raises(ValueError, match="Invalid colon-syntax"):
            PluginManifest(
                id="test-source",
                name="Test",
                type=PluginCapabilityType.SOURCE,
                entry_point=":Cls",
            )

    def test_manifest_rejects_empty_dot_entry_point(self) -> None:
        with pytest.raises(ValueError, match="Invalid dotted"):
            PluginManifest(
                id="test-source",
                name="Test",
                type=PluginCapabilityType.SOURCE,
                entry_point="module.",
            )

    def test_manifest_from_yaml(self) -> None:
        yaml_str = """
api_version: datahub/v1
id: my-source
name: My Source
type: source
entry_point: my_source.source:MySource
description: A test source
"""
        data = yaml.safe_load(yaml_str)
        manifest = PluginManifest.model_validate(data)
        assert manifest.id == "my-source"
        assert manifest.type == PluginCapabilityType.SOURCE

    def test_manifest_with_capabilities_and_support_status(self) -> None:
        manifest = PluginManifest(
            id="test-source",
            name="Test Source",
            type=PluginCapabilityType.SOURCE,
            entry_point="test_source.source:TestSource",
            support_status=SupportStatusType.COMMUNITY,
            capabilities=[
                PluginCapabilityEntry(
                    capability="SCHEMA_METADATA",
                    description="Extract schema",
                    supported=True,
                ),
                PluginCapabilityEntry(
                    capability="LINEAGE_COARSE",
                    description="Table lineage",
                    supported=False,
                ),
            ],
        )
        assert manifest.support_status == SupportStatusType.COMMUNITY
        assert len(manifest.capabilities) == 2
        assert manifest.capabilities[0].capability == "SCHEMA_METADATA"
        assert manifest.capabilities[1].supported is False

    def test_manifest_rejects_duplicate_capabilities(self) -> None:
        with pytest.raises(ValueError, match="Duplicate capability name"):
            PluginManifest(
                id="test-source",
                name="Test Source",
                type=PluginCapabilityType.SOURCE,
                entry_point="test_source.source:TestSource",
                capabilities=[
                    PluginCapabilityEntry(capability="SCHEMA_METADATA"),
                    PluginCapabilityEntry(capability="SCHEMA_METADATA"),
                ],
            )

    def test_manifest_backward_compat_without_new_fields(self) -> None:
        """Manifests without support_status/capabilities still parse."""
        yaml_str = """
api_version: datahub/v1
id: legacy-source
name: Legacy Source
type: source
entry_point: legacy.source:LegacySource
"""
        data = yaml.safe_load(yaml_str)
        manifest = PluginManifest.model_validate(data)
        assert manifest.support_status is None
        assert manifest.capabilities == []

    def test_manifest_yaml_round_trip_with_capabilities(self) -> None:
        yaml_str = """
api_version: datahub/v1
id: cap-source
name: Cap Source
type: source
entry_point: cap.source:CapSource
support_status: INCUBATING
capabilities:
  - capability: SCHEMA_METADATA
    description: Extract schema metadata
    supported: true
  - capability: LINEAGE_COARSE
    description: Table-level lineage
    supported: false
"""
        data = yaml.safe_load(yaml_str)
        manifest = PluginManifest.model_validate(data)
        assert manifest.support_status == SupportStatusType.INCUBATING
        assert len(manifest.capabilities) == 2
        assert manifest.capabilities[0].capability == "SCHEMA_METADATA"
        assert manifest.capabilities[1].supported is False

        # Round-trip back to dict
        dumped = manifest.model_dump()
        reloaded = PluginManifest.model_validate(dumped)
        assert reloaded.support_status == manifest.support_status
        assert len(reloaded.capabilities) == len(manifest.capabilities)


class TestDiscoveredPlugin:
    def test_discovered_plugin(self) -> None:
        manifest = PluginManifest(
            id="test-source",
            name="Test Source",
            type=PluginCapabilityType.SOURCE,
            entry_point="test_source.source:TestSource",
        )
        plugin = DiscoveredPlugin(
            manifest=manifest,
            package_name="datahub-test-source",
            version="1.0.0",
        )
        assert plugin.manifest.id == "test-source"
        assert plugin.package_name == "datahub-test-source"
        assert plugin.version == "1.0.0"

    def test_discovered_plugin_rejects_empty_package_name(self) -> None:
        manifest = PluginManifest(
            id="test-source",
            name="Test Source",
            type=PluginCapabilityType.SOURCE,
            entry_point="test_source.source:TestSource",
        )
        with pytest.raises(ValueError, match="package_name must not be empty"):
            DiscoveredPlugin(
                manifest=manifest,
                package_name="",
                version="1.0.0",
            )


class TestPluginSystemConfig:
    def test_default_config(self) -> None:
        config = PluginSystemConfig()
        assert config.registries == []

    def test_config_with_registries(self) -> None:
        config = PluginSystemConfig(
            registries=[
                RegistryConfig(
                    name="community",
                    url="https://example.com/index.json",
                ),
                RegistryConfig(
                    name="internal",
                    url="https://internal.com/index.json",
                    auth_type="bearer",
                    token_env="INTERNAL_TOKEN",
                ),
            ]
        )
        assert len(config.registries) == 2
        assert config.registries[1].auth_type == "bearer"


class TestPluginSystemConfigLoad:
    def test_load_returns_defaults_for_missing_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "datahub.plugin.plugin_config.PLUGIN_CONFIG_PATH",
            str(tmp_path / "nonexistent.yaml"),
        )
        config = PluginSystemConfig.load()
        assert config.registries == []

    def test_load_returns_defaults_for_corrupt_yaml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        corrupt = tmp_path / "config.yaml"
        corrupt.write_text(": : : not valid yaml [[[")
        monkeypatch.setattr(
            "datahub.plugin.plugin_config.PLUGIN_CONFIG_PATH", str(corrupt)
        )
        config = PluginSystemConfig.load()
        assert config.registries == []

    def test_load_returns_defaults_for_invalid_schema(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Valid YAML but invalid PluginSystemConfig (extra fields with forbid)."""
        bad_schema = tmp_path / "config.yaml"
        bad_schema.write_text("registries: not-a-list\n")
        monkeypatch.setattr(
            "datahub.plugin.plugin_config.PLUGIN_CONFIG_PATH", str(bad_schema)
        )
        config = PluginSystemConfig.load()
        assert config.registries == []

    def test_load_parses_valid_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        valid = tmp_path / "config.yaml"
        valid.write_text(
            "registries:\n  - name: test\n    url: https://example.com/index.json\n"
        )
        monkeypatch.setattr(
            "datahub.plugin.plugin_config.PLUGIN_CONFIG_PATH", str(valid)
        )
        config = PluginSystemConfig.load()
        assert len(config.registries) == 1
        assert config.registries[0].name == "test"


class TestPluginSystemConfigSave:
    def test_save_then_load_round_trip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """save() followed by load() returns an equivalent config."""
        config_path = str(tmp_path / "plugins" / "config.yaml")
        monkeypatch.setattr(
            "datahub.plugin.plugin_config.PLUGIN_CONFIG_PATH", config_path
        )
        monkeypatch.setattr(
            "datahub.plugin.plugin_config.PLUGINS_DIR", str(tmp_path / "plugins")
        )

        original = PluginSystemConfig(
            registries=[
                RegistryConfig(
                    name="enterprise",
                    url="https://internal.example.com/index.json",
                    auth_type="bearer",
                    token_env="INTERNAL_TOKEN",
                ),
                RegistryConfig(
                    name="community",
                    url="https://community.example.com/index.json",
                ),
            ]
        )
        original.save()
        loaded = PluginSystemConfig.load()

        assert len(loaded.registries) == 2
        assert loaded.registries[0].name == "enterprise"
        assert loaded.registries[0].auth_type == "bearer"
        assert loaded.registries[0].token_env == "INTERNAL_TOKEN"
        assert loaded.registries[1].name == "community"

    def test_save_creates_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """save() creates the plugins directory if it does not exist."""
        plugins_dir = str(tmp_path / "new_dir" / "plugins")
        config_path = str(tmp_path / "new_dir" / "plugins" / "config.yaml")
        monkeypatch.setattr(
            "datahub.plugin.plugin_config.PLUGIN_CONFIG_PATH", config_path
        )
        monkeypatch.setattr("datahub.plugin.plugin_config.PLUGINS_DIR", plugins_dir)

        config = PluginSystemConfig(
            registries=[
                RegistryConfig(name="test", url="https://example.com/index.json"),
            ]
        )
        config.save()

        assert os.path.isfile(config_path)
        loaded = PluginSystemConfig.load()
        assert len(loaded.registries) == 1

    def test_save_no_leftover_tmp_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Atomic save via os.replace should not leave a .tmp file behind."""
        config_path = str(tmp_path / "config.yaml")
        monkeypatch.setattr(
            "datahub.plugin.plugin_config.PLUGIN_CONFIG_PATH", config_path
        )
        monkeypatch.setattr("datahub.plugin.plugin_config.PLUGINS_DIR", str(tmp_path))

        config = PluginSystemConfig()
        config.save()

        assert os.path.isfile(config_path)
        assert not os.path.isfile(config_path + ".tmp")


class TestRegistryConfigValidation:
    def test_registry_config_rejects_empty_name(self) -> None:
        with pytest.raises(ValueError, match="Registry name must not be empty"):
            RegistryConfig(
                name="",
                url="https://example.com/index.json",
            )

    def test_registry_config_rejects_auth_without_token(self) -> None:
        with pytest.raises(ValueError, match="token_env"):
            RegistryConfig(
                name="internal",
                url="https://example.com/index.json",
                auth_type="bearer",
            )

    def test_registry_config_rejects_empty_url(self) -> None:
        with pytest.raises(ValueError, match="Registry url must not be empty"):
            RegistryConfig(name="test", url="")

    def test_registry_config_rejects_non_http_url(self) -> None:
        with pytest.raises(ValueError, match="must start with https:// or http://"):
            RegistryConfig(name="test", url="ftp://example.com/index.json")

    def test_registry_config_accepts_http_url(self) -> None:
        config = RegistryConfig(name="test", url="http://localhost:8080/index.json")
        assert config.url == "http://localhost:8080/index.json"

    def test_registry_config_accepts_https_url(self) -> None:
        config = RegistryConfig(name="test", url="https://example.com/index.json")
        assert config.url == "https://example.com/index.json"
