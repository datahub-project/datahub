"""Tests for the plugin CLI commands."""

import os
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from datahub.cli.plugin_cli import (
    info,
    init_plugin,
    install,
    list_plugins,
    registry_add,
    registry_list,
    registry_refresh,
    registry_remove,
    search,
    sync_manifest,
    uninstall,
    validate,
)
from datahub.plugin.plugin_config import (
    DiscoveredPlugin,
    PluginCapabilityEntry,
    PluginCapabilityType,
    PluginManifest,
    SupportStatusType,
)
from datahub.plugin.plugin_manager import InstallTarget
from datahub.plugin.registry_client import PluginIndexEntry


def _passthrough_target(spec: str, version: Optional[str] = None) -> InstallTarget:
    """An InstallTarget that resolves to the spec unchanged (no registry match)."""
    return InstallTarget(spec=spec, version=version, expected_sha256=None, entry=None)


def _make_plugin(
    plugin_id: str = "test-source",
    version: str = "1.0.0",
    plugin_type: PluginCapabilityType = PluginCapabilityType.SOURCE,
) -> DiscoveredPlugin:
    return DiscoveredPlugin(
        manifest=PluginManifest(
            id=plugin_id,
            name=f"Test {plugin_id}",
            type=plugin_type,
            entry_point="test_pkg.source:TestSource",
            description="A test plugin",
            author="Test Author",
        ),
        package_name=f"datahub-{plugin_id}",
        version=version,
    )


class TestInstallCommand:
    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_install_success(self, mock_manager_cls: MagicMock) -> None:
        manager = mock_manager_cls.return_value
        manager.resolve_install_target.return_value = _passthrough_target(
            "github:acme/test-source"
        )
        manager.install.return_value = [_make_plugin()]

        runner = CliRunner()
        result = runner.invoke(install, ["github:acme/test-source"])

        assert result.exit_code == 0
        assert "test-source@1.0.0" in result.output
        manager.install.assert_called_once_with(
            "github:acme/test-source", version=None, expected_sha256=None
        )

    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_install_multi_plugin_lists_all(self, mock_manager_cls: MagicMock) -> None:
        """A wheel that ships several connectors reports each installed plugin."""
        manager = mock_manager_cls.return_value
        manager.resolve_install_target.return_value = _passthrough_target(
            "github:acme/bundle"
        )
        manager.install.return_value = [
            _make_plugin(plugin_id="source-a"),
            _make_plugin(plugin_id="source-b"),
        ]

        runner = CliRunner()
        result = runner.invoke(install, ["github:acme/bundle"])

        assert result.exit_code == 0
        assert "Installed 2 plugins" in result.output
        assert "source-a@1.0.0" in result.output
        assert "source-b@1.0.0" in result.output

    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_install_with_version(self, mock_manager_cls: MagicMock) -> None:
        manager = mock_manager_cls.return_value
        manager.resolve_install_target.return_value = _passthrough_target(
            "github:acme/test-source", version="v2.0.0"
        )
        manager.install.return_value = [_make_plugin(version="2.0.0")]

        runner = CliRunner()
        result = runner.invoke(
            install, ["github:acme/test-source", "--version", "v2.0.0"]
        )

        assert result.exit_code == 0
        manager.install.assert_called_once_with(
            "github:acme/test-source", version="v2.0.0", expected_sha256=None
        )

    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_install_by_marketplace_id(self, mock_manager_cls: MagicMock) -> None:
        """A bare id resolved from a registry installs its github repo + checksum."""
        manager = mock_manager_cls.return_value
        entry = PluginIndexEntry(
            id="my-source",
            repo="acme/my-source",
            version="1.0.0",
            type=PluginCapabilityType.SOURCE,
            sha256="abc123",
            registry_name="community",
        )
        manager.resolve_install_target.return_value = InstallTarget(
            spec="github:acme/my-source",
            version="1.0.0",
            expected_sha256="abc123",
            entry=entry,
        )
        manager.install.return_value = [_make_plugin(plugin_id="my-source")]

        runner = CliRunner()
        result = runner.invoke(install, ["my-source"])

        assert result.exit_code == 0
        assert "from registry 'community'" in result.output
        assert "checksum verified" in result.output
        manager.install.assert_called_once_with(
            "github:acme/my-source", version="1.0.0", expected_sha256="abc123"
        )

    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_install_failure(self, mock_manager_cls: MagicMock) -> None:
        manager = mock_manager_cls.return_value
        manager.resolve_install_target.return_value = _passthrough_target(
            "github:acme/bad-plugin"
        )
        manager.install.side_effect = RuntimeError("Download failed")

        runner = CliRunner()
        result = runner.invoke(install, ["github:acme/bad-plugin"])

        assert result.exit_code != 0
        assert "Download failed" in result.output


class TestUninstallCommand:
    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_uninstall_success(self, mock_manager_cls: MagicMock) -> None:
        manager = mock_manager_cls.return_value

        runner = CliRunner()
        result = runner.invoke(uninstall, ["test-source"])

        assert result.exit_code == 0
        assert "Uninstalled test-source" in result.output
        manager.uninstall.assert_called_once_with("test-source")

    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_uninstall_not_installed(self, mock_manager_cls: MagicMock) -> None:
        manager = mock_manager_cls.return_value
        manager.uninstall.side_effect = KeyError("not-installed")

        runner = CliRunner()
        result = runner.invoke(uninstall, ["not-installed"])

        assert result.exit_code != 0
        assert "not installed" in result.output


class TestListCommand:
    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_list_with_plugins(self, mock_manager_cls: MagicMock) -> None:
        manager = mock_manager_cls.return_value
        manager.list_installed.return_value = {
            "test-source": _make_plugin(),
        }

        runner = CliRunner()
        result = runner.invoke(list_plugins)

        assert result.exit_code == 0
        assert "test-source" in result.output
        assert "1.0.0" in result.output

    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_list_empty(self, mock_manager_cls: MagicMock) -> None:
        manager = mock_manager_cls.return_value
        manager.list_installed.return_value = {}

        runner = CliRunner()
        result = runner.invoke(list_plugins)

        assert result.exit_code == 0
        assert "No plugins installed" in result.output


class TestInfoCommand:
    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_info_success(self, mock_manager_cls: MagicMock) -> None:
        manager = mock_manager_cls.return_value
        manager.get_installed.return_value = _make_plugin()

        runner = CliRunner()
        result = runner.invoke(info, ["test-source"])

        assert result.exit_code == 0
        assert "Test test-source" in result.output
        assert "1.0.0" in result.output
        assert "test_pkg.source:TestSource" in result.output

    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_info_not_installed(self, mock_manager_cls: MagicMock) -> None:
        manager = mock_manager_cls.return_value
        manager.get_installed.return_value = None

        runner = CliRunner()
        result = runner.invoke(info, ["missing-plugin"])

        assert result.exit_code != 0
        assert "not installed" in result.output


class TestSearchCommand:
    @patch("datahub.plugin.registry_client.RegistryClient")
    def test_search_with_results(self, mock_client_cls: MagicMock) -> None:
        client = mock_client_cls.return_value
        client.search.return_value = [
            PluginIndexEntry(
                id="salesforce-source",
                repo="acme/salesforce-source",
                version="1.0",
                type=PluginCapabilityType.SOURCE,
                description="Salesforce connector",
                author="Acme Corp",
            )
        ]

        runner = CliRunner()
        result = runner.invoke(search, ["salesforce"])

        assert result.exit_code == 0
        assert "salesforce-source" in result.output
        assert "Salesforce connector" in result.output

    @patch("datahub.plugin.registry_client.RegistryClient")
    def test_search_no_results(self, mock_client_cls: MagicMock) -> None:
        client = mock_client_cls.return_value
        client.search.return_value = []

        runner = CliRunner()
        result = runner.invoke(search, ["nonexistent"])

        assert result.exit_code == 0
        assert "No plugins found" in result.output

    @patch("datahub.plugin.registry_client.RegistryClient")
    def test_search_list_all(self, mock_client_cls: MagicMock) -> None:
        """Omitting query calls search with empty string."""
        client = mock_client_cls.return_value
        client.search.return_value = []

        runner = CliRunner()
        result = runner.invoke(search, [])

        assert result.exit_code == 0
        client.search.assert_called_once_with("", type_filter=None)


class TestRegistryCommands:
    @patch("datahub.plugin.plugin_config.PluginSystemConfig")
    def test_registry_list_empty(self, mock_config_cls: MagicMock) -> None:
        mock_config_cls.load.return_value = MagicMock(registries=[])

        runner = CliRunner()
        result = runner.invoke(registry_list)

        assert result.exit_code == 0
        assert "No custom registries" in result.output

    @patch("datahub.plugin.plugin_config.PluginSystemConfig")
    def test_registry_list_with_entries(self, mock_config_cls: MagicMock) -> None:
        reg = MagicMock(
            name="enterprise", url="https://example.com/index.json", enabled=True
        )
        reg.name = "enterprise"
        mock_config_cls.load.return_value = MagicMock(registries=[reg])

        runner = CliRunner()
        result = runner.invoke(registry_list)

        assert result.exit_code == 0
        assert "enterprise" in result.output

    @patch("datahub.plugin.plugin_config.PluginSystemConfig")
    @patch("datahub.plugin.plugin_config.RegistryConfig")
    def test_registry_add(
        self, mock_reg_config: MagicMock, mock_config_cls: MagicMock
    ) -> None:
        config = MagicMock(registries=[])
        mock_config_cls.load.return_value = config

        runner = CliRunner()
        result = runner.invoke(
            registry_add, ["myregistry", "https://example.com/index.json"]
        )

        assert result.exit_code == 0
        assert "Added registry" in result.output
        config.save.assert_called_once()

    @patch("datahub.plugin.plugin_config.PluginSystemConfig")
    def test_registry_add_duplicate(self, mock_config_cls: MagicMock) -> None:
        existing = MagicMock()
        existing.name = "myregistry"
        mock_config_cls.load.return_value = MagicMock(registries=[existing])

        runner = CliRunner()
        result = runner.invoke(
            registry_add, ["myregistry", "https://example.com/index.json"]
        )

        assert result.exit_code != 0
        assert "already exists" in result.output

    @patch("datahub.plugin.plugin_config.PluginSystemConfig")
    def test_registry_remove_success(self, mock_config_cls: MagicMock) -> None:
        existing = MagicMock()
        existing.name = "myregistry"
        config = MagicMock(registries=[existing])
        mock_config_cls.load.return_value = config

        runner = CliRunner()
        result = runner.invoke(registry_remove, ["myregistry"])

        assert result.exit_code == 0
        assert "Removed registry" in result.output
        config.save.assert_called_once()

    @patch("datahub.plugin.plugin_config.PluginSystemConfig")
    def test_registry_remove_not_found(self, mock_config_cls: MagicMock) -> None:
        config = MagicMock(registries=[])
        mock_config_cls.load.return_value = config

        runner = CliRunner()
        result = runner.invoke(registry_remove, ["missing"])

        assert result.exit_code != 0
        assert "not found" in result.output

    @patch("datahub.plugin.registry_client.RegistryClient")
    def test_registry_refresh(self, mock_client_cls: MagicMock) -> None:
        client = mock_client_cls.return_value

        runner = CliRunner()
        result = runner.invoke(registry_refresh)

        assert result.exit_code == 0
        assert "caches cleared" in result.output
        client.refresh.assert_called_once()


class TestInitCommand:
    def test_init_creates_namespaced_project(self, tmp_path: os.PathLike) -> None:
        runner = CliRunner()
        result = runner.invoke(
            init_plugin,
            ["acme/salesforce", "--type", "source", "--output-dir", str(tmp_path)],
        )

        assert result.exit_code == 0, result.output
        assert "Created plugin project" in result.output
        assert (
            Path(tmp_path) / "acme" / "src" / "acme" / "salesforce" / "source.py"
        ).exists()

    def test_init_adds_connector_to_existing_project(
        self, tmp_path: os.PathLike
    ) -> None:
        runner = CliRunner()
        runner.invoke(init_plugin, ["acme/salesforce", "--output-dir", str(tmp_path)])
        # Adding from the parent dir via namespace/connector appends to the project.
        result = runner.invoke(
            init_plugin, ["acme/workday", "--output-dir", str(tmp_path)]
        )

        assert result.exit_code == 0, result.output
        assert "Added connector 'workday'" in result.output
        assert (
            Path(tmp_path) / "acme" / "src" / "acme" / "workday" / "source.py"
        ).exists()

    def test_init_invalid_name(self, tmp_path: os.PathLike) -> None:
        runner = CliRunner()
        result = runner.invoke(init_plugin, ["INVALID", "--output-dir", str(tmp_path)])

        assert result.exit_code != 0
        assert "Invalid" in result.output


class TestValidateCommand:
    def test_validate_missing_manifest(self, tmp_path: os.PathLike) -> None:
        runner = CliRunner()
        result = runner.invoke(validate, [str(tmp_path)])

        assert result.exit_code != 0
        assert "Missing datahub-plugin.yaml" in result.output

    def test_validate_valid_manifest(self, tmp_path: os.PathLike) -> None:
        import yaml

        manifest_data = {
            "api_version": "datahub/v1",
            "id": "test-source",
            "name": "Test Source",
            "type": "source",
            "entry_point": "test_pkg.source:TestSource",
        }
        manifest_path = os.path.join(str(tmp_path), "datahub-plugin.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(manifest_data, f)

        runner = CliRunner()
        result = runner.invoke(validate, [str(tmp_path)])

        assert result.exit_code == 0
        assert "Validation passed" in result.output

    def test_validate_invalid_manifest(self, tmp_path: os.PathLike) -> None:
        manifest_path = os.path.join(str(tmp_path), "datahub-plugin.yaml")
        with open(manifest_path, "w") as f:
            f.write("not: valid: manifest: {{broken")

        runner = CliRunner()
        result = runner.invoke(validate, [str(tmp_path)])

        assert result.exit_code != 0

    def test_validate_cross_checks_support_status_mismatch(
        self, tmp_path: os.PathLike
    ) -> None:
        """Validate warns when manifest support_status differs from decorator."""
        import yaml

        manifest_data = {
            "api_version": "datahub/v1",
            "id": "test-source",
            "name": "Test Source",
            "type": "source",
            "entry_point": "test_pkg.source:TestSource",
            "support_status": "CERTIFIED",
        }
        manifest_path = os.path.join(str(tmp_path), "datahub-plugin.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(manifest_data, f)

        # Mock the entry point class with a different support status
        mock_cls = MagicMock()
        mock_cls.__name__ = "TestSource"
        mock_status = MagicMock()
        mock_status.name = "COMMUNITY"
        mock_cls.get_support_status = MagicMock(return_value=mock_status)
        mock_cls.get_capabilities = MagicMock(return_value=[])
        mock_cls.get_platform_name = MagicMock(return_value="Test Source")

        with patch(
            "datahub.cli.plugin_cli._try_import_entry_point", return_value=mock_cls
        ):
            runner = CliRunner()
            result = runner.invoke(validate, [str(tmp_path)])

        assert result.exit_code == 0
        assert "datahub plugin sync" in result.output

    def test_validate_cross_checks_capabilities_mismatch(
        self, tmp_path: os.PathLike
    ) -> None:
        """Validate warns when manifest capabilities differ from decorators."""
        import yaml

        manifest_data = {
            "api_version": "datahub/v1",
            "id": "test-source",
            "name": "Test Source",
            "type": "source",
            "entry_point": "test_pkg.source:TestSource",
            "capabilities": [
                {
                    "capability": "SCHEMA_METADATA",
                    "description": "Schema",
                    "supported": True,
                },
            ],
        }
        manifest_path = os.path.join(str(tmp_path), "datahub-plugin.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(manifest_data, f)

        # Mock the entry point class with different capabilities
        mock_cls = MagicMock()
        mock_cls.__name__ = "TestSource"
        mock_cap = MagicMock()
        mock_cap.capability = MagicMock()
        mock_cap.capability.name = "LINEAGE_COARSE"
        mock_cap.description = "Lineage"
        mock_cap.supported = True
        mock_cls.get_capabilities = MagicMock(return_value=[mock_cap])
        mock_cls.get_platform_name = MagicMock(return_value="Test Source")

        with patch(
            "datahub.cli.plugin_cli._try_import_entry_point", return_value=mock_cls
        ):
            runner = CliRunner()
            result = runner.invoke(validate, [str(tmp_path)])

        assert result.exit_code == 0
        assert "datahub plugin sync" in result.output


class TestInfoWithCapabilities:
    @patch("datahub.cli.plugin_cli.PluginManager")
    def test_info_displays_support_status(self, mock_manager_cls: MagicMock) -> None:
        plugin_info = DiscoveredPlugin(
            manifest=PluginManifest(
                id="cap-source",
                name="Cap Source",
                type=PluginCapabilityType.SOURCE,
                entry_point="cap.source:CapSource",
                support_status=SupportStatusType.INCUBATING,
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
            ),
            package_name="datahub-cap-source",
            version="1.0.0",
        )
        mock_manager_cls.return_value.get_installed.return_value = plugin_info

        runner = CliRunner()
        result = runner.invoke(info, ["cap-source"])

        assert result.exit_code == 0
        assert "INCUBATING" in result.output
        assert "Capabilities: 2" in result.output
        assert "+ SCHEMA_METADATA" in result.output
        assert "- LINEAGE_COARSE" in result.output


class TestSyncCommand:
    def test_sync_updates_manifest(self, tmp_path: os.PathLike) -> None:
        import yaml

        manifest_data = {
            "api_version": "datahub/v1",
            "id": "test-source",
            "name": "Test Source",
            "type": "source",
            "entry_point": "test_pkg.source:TestSource",
            "description": "My custom description",
            "author": "Test Author",
            "icon_url": "https://example.com/icon.png",
        }
        manifest_path = os.path.join(str(tmp_path), "datahub-plugin.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(manifest_data, f)

        # Mock the entry point class
        mock_cls = MagicMock()
        mock_cls.__name__ = "TestSource"
        mock_status = MagicMock()
        mock_status.name = "COMMUNITY"
        mock_cls.get_support_status = MagicMock(return_value=mock_status)

        mock_cap = MagicMock()
        mock_cap.capability = MagicMock()
        mock_cap.capability.name = "SCHEMA_METADATA"
        mock_cap.description = "Extract schema"
        mock_cap.supported = True
        mock_cls.get_capabilities = MagicMock(return_value=[mock_cap])
        mock_cls.get_platform_name = MagicMock(return_value="Synced Name")

        with patch(
            "datahub.cli.plugin_cli._try_import_entry_point", return_value=mock_cls
        ):
            runner = CliRunner()
            result = runner.invoke(sync_manifest, [str(tmp_path)])

        assert result.exit_code == 0
        assert "Synced" in result.output

        # Verify the manifest was updated
        with open(manifest_path) as f:
            updated = yaml.safe_load(f)
        assert updated["support_status"] == "COMMUNITY"
        assert updated["name"] == "Synced Name"
        assert len(updated["capabilities"]) == 1
        assert updated["capabilities"][0]["capability"] == "SCHEMA_METADATA"

        # Verify manual fields were preserved
        assert updated["description"] == "My custom description"
        assert updated["author"] == "Test Author"
        assert updated["icon_url"] == "https://example.com/icon.png"

    def test_sync_no_manifest(self, tmp_path: os.PathLike) -> None:
        runner = CliRunner()
        result = runner.invoke(sync_manifest, [str(tmp_path)])

        assert result.exit_code != 0
        assert "No datahub-plugin.yaml found" in result.output

    def test_sync_import_fails(self, tmp_path: os.PathLike) -> None:
        import yaml

        manifest_data = {
            "api_version": "datahub/v1",
            "id": "test-source",
            "name": "Test Source",
            "type": "source",
            "entry_point": "nonexistent.mod:Cls",
        }
        manifest_path = os.path.join(str(tmp_path), "datahub-plugin.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(manifest_data, f)

        with patch("datahub.cli.plugin_cli._try_import_entry_point", return_value=None):
            runner = CliRunner()
            result = runner.invoke(sync_manifest, [str(tmp_path)])

        assert result.exit_code != 0
        assert "Could not import" in result.output

    def test_sync_no_decorators(self, tmp_path: os.PathLike) -> None:
        import yaml

        manifest_data = {
            "api_version": "datahub/v1",
            "id": "test-source",
            "name": "Test Source",
            "type": "source",
            "entry_point": "test_pkg.source:Bare",
        }
        manifest_path = os.path.join(str(tmp_path), "datahub-plugin.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(manifest_data, f)

        # Mock a class with no decorator methods
        mock_cls = MagicMock(spec=[])

        with patch(
            "datahub.cli.plugin_cli._try_import_entry_point", return_value=mock_cls
        ):
            runner = CliRunner()
            result = runner.invoke(sync_manifest, [str(tmp_path)])

        assert result.exit_code == 0
        assert "Nothing to sync" in result.output
