"""Tests for the plugin manager."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from datahub.plugin.plugin_manager import (
    PluginManager,
    _extract_package_name,
    _find_manifest_path_in_dist,
    _pip_cmd,
)
from tests.unit.plugin.conftest import make_discovered_plugin


class TestExtractPackageName:
    def test_extracts_from_pip_output(self) -> None:
        output = (
            "Collecting my-plugin==1.0.0\n"
            "  Downloading my_plugin-1.0.0-py3-none-any.whl\n"
            "Successfully installed my-plugin-1.0.0\n"
        )
        assert _extract_package_name(output) == "my-plugin"

    def test_returns_none_for_empty_output(self) -> None:
        assert _extract_package_name("") is None

    def test_returns_none_for_no_success_line(self) -> None:
        assert _extract_package_name("Collecting foo\nDownloading...") is None

    def test_handles_multiple_packages(self) -> None:
        output = "Successfully installed my-plugin-1.0.0 dep-a-2.0 dep-b-3.1.0\n"
        assert _extract_package_name(output) == "my-plugin"

    def test_extracts_from_uv_output(self) -> None:
        output = (
            "Resolved 3 packages in 50ms\n"
            "Installed 1 package in 10ms\n"
            " + my-plugin==1.0.0\n"
        )
        assert _extract_package_name(output) == "my-plugin"

    def test_extracts_from_uv_output_with_multiple_packages(self) -> None:
        output = " + my-plugin==1.0.0\n + dep-a==2.0\n"
        # Returns the first package found
        assert _extract_package_name(output) == "my-plugin"


class TestPipCmd:
    @patch("datahub.plugin.plugin_manager._has_uv", return_value=True)
    def test_returns_uv_cmd_when_available(self, _mock_has_uv) -> None:
        cmd = _pip_cmd()
        assert cmd[1:] == ["-m", "uv", "pip"]

    @patch("datahub.plugin.plugin_manager._has_uv", return_value=False)
    def test_returns_pip_cmd_as_fallback(self, _mock_has_uv) -> None:
        cmd = _pip_cmd()
        assert cmd[1:] == ["-m", "pip"]


class TestDiscoverPluginsErrorBoundary:
    """Test that discover_plugins isolates failures per-distribution."""

    @patch("datahub.plugin.plugin_manager.distributions")
    @patch("datahub.plugin.plugin_manager._load_plugin_from_dist")
    def test_corrupted_manifest_does_not_block_others(
        self, mock_load, mock_dists
    ) -> None:
        """A distribution with a corrupted manifest should be skipped,
        not block discovery of valid plugins from other distributions."""
        from datahub.plugin.plugin_manager import discover_plugins

        good_dist = MagicMock()
        good_dist.name = "datahub-good-plugin"
        bad_dist = MagicMock()
        bad_dist.name = "datahub-bad-plugin"

        mock_dists.return_value = [bad_dist, good_dist]

        good_plugin = make_discovered_plugin(
            plugin_id="good-source", package_name="datahub-good-plugin"
        )

        def load_side_effect(dist):
            if dist is bad_dist:
                raise ValueError("corrupt manifest")
            return good_plugin

        mock_load.side_effect = load_side_effect
        result = discover_plugins()

        assert "good-source" in result
        assert len(result) == 1

    @patch("datahub.plugin.plugin_manager.distributions")
    def test_exception_in_one_dist_skips_it(self, mock_dists) -> None:
        """A distribution that raises during loading should be skipped."""
        from datahub.plugin.plugin_manager import discover_plugins

        bad_dist = MagicMock()
        bad_dist.name = "exploding-dist"
        bad_dist.files = None
        bad_dist.read_text.side_effect = OSError("corrupt dist")

        mock_dists.return_value = [bad_dist]
        result = discover_plugins()
        assert result == {}


class TestDiscoverPlugins:
    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_list_installed_delegates_to_discover(self, mock_discover) -> None:
        plugins = {"test-source": make_discovered_plugin()}
        mock_discover.return_value = plugins

        manager = PluginManager()
        result = manager.list_installed()
        assert result == plugins
        mock_discover.assert_called_once()

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_get_installed_found(self, mock_discover) -> None:
        plugins = {"test-source": make_discovered_plugin()}
        mock_discover.return_value = plugins

        manager = PluginManager()
        result = manager.get_installed("test-source")
        assert result is not None
        assert result.manifest.id == "test-source"

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_get_installed_not_found(self, mock_discover) -> None:
        mock_discover.return_value = {}

        manager = PluginManager()
        assert manager.get_installed("nonexistent") is None

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_is_installed(self, mock_discover) -> None:
        plugins = {"test-source": make_discovered_plugin()}
        mock_discover.return_value = plugins

        manager = PluginManager()
        assert manager.is_installed("test-source")
        assert not manager.is_installed("other-source")

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_uninstall_missing_plugin(self, mock_discover) -> None:
        mock_discover.return_value = {}

        manager = PluginManager()
        with pytest.raises(KeyError, match="not installed"):
            manager.uninstall("nonexistent")


class TestResolveSpec:
    def test_resolve_spec_local_wheel(self, tmp_path: Path) -> None:
        wheel_path = tmp_path / "my_plugin-1.2.3-py3-none-any.whl"
        wheel_path.write_text("fake wheel content")

        manager = PluginManager()
        pip_target = manager._resolve_spec(str(wheel_path), None)
        assert pip_target == str(wheel_path)

    def test_resolve_spec_pip(self) -> None:
        manager = PluginManager()
        pip_target = manager._resolve_spec("my-datahub-plugin==2.0", None)
        assert pip_target == "my-datahub-plugin==2.0"

    def test_resolve_spec_pip_with_version(self) -> None:
        manager = PluginManager()
        pip_target = manager._resolve_spec("my-plugin", "3.0.0")
        assert pip_target == "my-plugin==3.0.0"

    def test_resolve_spec_pip_version_not_duplicated(self) -> None:
        manager = PluginManager()
        pip_target = manager._resolve_spec("my-plugin==2.0", "3.0.0")
        # If spec already has ==, don't append version again
        assert pip_target == "my-plugin==2.0"

    @patch("datahub.plugin.plugin_manager.download_wheel", return_value="/tmp/a.whl")
    @patch("datahub.plugin.plugin_manager.resolve_github_spec")
    def test_resolve_spec_github_wheel(self, mock_resolve, mock_download) -> None:
        from datahub.plugin.github_resolver import ResolvedWheel

        fake = ResolvedWheel(
            download_url="https://github.com/acme/src/releases/download/v1.0/a.whl",
            version="1.0",
        )
        mock_resolve.return_value = fake

        manager = PluginManager()
        pip_target = manager._resolve_spec("github:acme/src", None)

        mock_resolve.assert_called_once_with("github:acme/src")
        mock_download.assert_called_once_with(fake)
        assert pip_target == "/tmp/a.whl"

    @patch("datahub.plugin.plugin_manager.download_wheel")
    @patch("datahub.plugin.plugin_manager.resolve_github_spec")
    def test_resolve_spec_github_git_source(self, mock_resolve, mock_download) -> None:
        from datahub.plugin.github_resolver import ResolvedGitSource

        fake = ResolvedGitSource(
            download_url="git+https://github.com/acme/src.git@v2.0",
            version="2.0",
        )
        mock_resolve.return_value = fake

        manager = PluginManager()
        pip_target = manager._resolve_spec("github:acme/src@v2.0", None)

        mock_resolve.assert_called_once_with("github:acme/src@v2.0")
        mock_download.assert_not_called()
        assert pip_target == "git+https://github.com/acme/src.git@v2.0"

    @patch("datahub.plugin.plugin_manager.download_wheel", return_value="/tmp/a.whl")
    @patch("datahub.plugin.plugin_manager.resolve_github_spec")
    def test_resolve_spec_github_version_override(
        self, mock_resolve, mock_download
    ) -> None:
        from datahub.plugin.github_resolver import ResolvedWheel

        fake = ResolvedWheel(
            download_url="https://example.com/a.whl",
            version="3.0",
        )
        mock_resolve.return_value = fake

        manager = PluginManager()
        manager._resolve_spec("github:acme/src@v1.0", "3.0")

        # version override rewrites the spec
        mock_resolve.assert_called_once_with("github:acme/src@3.0")


class TestInstall:
    @patch("datahub.plugin.plugin_manager._find_plugin_in_package")
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_success(self, _mock_pip_cmd, mock_run, mock_find) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "install", "my-plugin"],
            returncode=0,
            stdout="Successfully installed my-plugin-1.0.0\n",
            stderr="",
        )
        mock_find.return_value = make_discovered_plugin()

        manager = PluginManager()
        result = manager.install("my-plugin")
        assert result.manifest.id == "test-source"

    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_timeout(self, _mock_pip_cmd, mock_run) -> None:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=["pip"], timeout=300)

        manager = PluginManager()
        with pytest.raises(RuntimeError, match="timed out"):
            manager.install("slow-plugin")

    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_pip_failure(self, _mock_pip_cmd, mock_run) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "install", "bad-plugin"],
            returncode=1,
            stdout="",
            stderr="ERROR: No matching distribution\n",
        )

        manager = PluginManager()
        with pytest.raises(RuntimeError, match="pip command failed"):
            manager.install("bad-plugin")

    @patch("datahub.plugin.plugin_manager._find_plugin_in_package")
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_missing_manifest(self, _mock_pip_cmd, mock_run, mock_find) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "install", "no-manifest-plugin"],
            returncode=0,
            stdout="Successfully installed no-manifest-plugin-1.0.0\n",
            stderr="",
        )
        mock_find.return_value = None

        manager = PluginManager()
        with pytest.raises(ValueError, match="datahub-plugin.yaml"):
            manager.install("no-manifest-plugin")


class TestUninstall:
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager.discover_plugins")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_uninstall_success(self, _mock_pip_cmd, mock_discover, mock_run) -> None:
        mock_discover.return_value = {"test-source": make_discovered_plugin()}
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "uninstall", "datahub-test-source"],
            returncode=0,
            stdout="",
            stderr="",
        )

        manager = PluginManager()
        manager.uninstall("test-source")
        mock_run.assert_called_once()

    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager.discover_plugins")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_uninstall_pip_failure(
        self, _mock_pip_cmd, mock_discover, mock_run
    ) -> None:
        mock_discover.return_value = {"test-source": make_discovered_plugin()}
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "uninstall", "datahub-test-source"],
            returncode=1,
            stdout="",
            stderr="ERROR: cannot uninstall\n",
        )

        manager = PluginManager()
        with pytest.raises(RuntimeError, match="pip command failed"):
            manager.uninstall("test-source")

    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager.discover_plugins")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_uninstall_timeout(self, _mock_pip_cmd, mock_discover, mock_run) -> None:
        mock_discover.return_value = {"test-source": make_discovered_plugin()}
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=["pip"], timeout=120)

        manager = PluginManager()
        with pytest.raises(RuntimeError, match="timed out"):
            manager.uninstall("test-source")


class TestFindManifestPathInDist:
    def test_finds_manifest_via_dist_files(self, tmp_path: Path) -> None:
        manifest_path = tmp_path / "datahub-plugin.yaml"
        manifest_path.write_text("id: test")

        mock_file = MagicMock()
        mock_file.name = "datahub-plugin.yaml"
        mock_file.locate.return_value = str(manifest_path)

        dist = MagicMock()
        dist.files = [mock_file]
        dist.read_text.return_value = None

        result = _find_manifest_path_in_dist(dist)
        assert result == str(manifest_path)

    def test_finds_manifest_via_find_spec(self, tmp_path: Path) -> None:
        # Create a fake package dir with a manifest
        pkg_dir = tmp_path / "my_plugin"
        pkg_dir.mkdir()
        manifest = pkg_dir / "datahub-plugin.yaml"
        manifest.write_text("id: test")
        init = pkg_dir / "__init__.py"
        init.write_text("")

        dist = MagicMock()
        dist.files = None
        dist.read_text.return_value = "my_plugin\n"

        mock_spec = MagicMock()
        mock_spec.origin = str(init)

        with patch("importlib.util.find_spec", return_value=mock_spec):
            result = _find_manifest_path_in_dist(dist)

        assert result == str(manifest)

    def test_returns_none_when_both_strategies_fail(self) -> None:
        dist = MagicMock()
        dist.files = []
        dist.read_text.return_value = None

        assert _find_manifest_path_in_dist(dist) is None
