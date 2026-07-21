"""Tests for the plugin manager."""

import subprocess
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.plugin.plugin_config import PluginCapabilityType
from datahub.plugin.plugin_manager import (
    PluginManager,
    _find_manifest_path_in_dist,
    _load_plugins_from_dist,
    _package_name_from_target,
    _pip_cmd,
)
from datahub.plugin.registry_client import PluginIndexEntry
from tests.unit.plugin.conftest import make_discovered_plugin


def _index_entry(
    version: str = "1.0.0", sha256: Optional[str] = "deadbeef"
) -> PluginIndexEntry:
    return PluginIndexEntry(
        id="my-source",
        repo="acme/my-source",
        version=version,
        type=PluginCapabilityType.SOURCE,
        sha256=sha256,
        registry_name="community",
    )


class TestPackageNameFromTarget:
    def test_wheel_filename(self) -> None:
        assert (
            _package_name_from_target("/tmp/x/my_plugin-1.0.0-py3-none-any.whl")
            == "my_plugin"
        )

    def test_pip_spec_with_version(self) -> None:
        assert _package_name_from_target("my-plugin==1.0.0") == "my-plugin"

    def test_git_url_returns_none(self) -> None:
        assert (
            _package_name_from_target("git+https://github.com/acme/src.git@v1") is None
        )


class TestPipCmd:
    @patch("datahub.plugin.plugin_manager._has_uv", return_value=True)
    def test_returns_uv_cmd_when_available(self, _mock_has_uv: MagicMock) -> None:
        cmd = _pip_cmd()
        assert cmd[1:] == ["-m", "uv", "pip"]

    @patch("datahub.plugin.plugin_manager._has_uv", return_value=False)
    def test_returns_pip_cmd_as_fallback(self, _mock_has_uv: MagicMock) -> None:
        cmd = _pip_cmd()
        assert cmd[1:] == ["-m", "pip"]


class TestLoadPluginsFromDist:
    def _dist(self, name: str = "datahub-bundle", version: str = "1.2.3") -> MagicMock:
        dist = MagicMock()
        dist.name = name
        dist.version = version
        return dist

    def test_single_plugin_manifest(self, tmp_path: Path) -> None:
        manifest = tmp_path / "datahub-plugin.yaml"
        manifest.write_text(
            "api_version: datahub/v1\n"
            "id: solo-source\n"
            "name: Solo\n"
            "type: source\n"
            "entry_point: pkg.source:Solo\n"
        )
        with patch(
            "datahub.plugin.plugin_manager._find_manifest_path_in_dist",
            return_value=str(manifest),
        ):
            plugins = _load_plugins_from_dist(self._dist())

        assert [p.manifest.id for p in plugins] == ["solo-source"]
        assert plugins[0].package_name == "datahub-bundle"
        assert plugins[0].version == "1.2.3"

    def test_multi_plugin_manifest_yields_all(self, tmp_path: Path) -> None:
        manifest = tmp_path / "datahub-plugin.yaml"
        manifest.write_text(
            "api_version: datahub/v1\n"
            "author: tester\n"
            "plugins:\n"
            "  - id: source-a\n"
            "    name: A\n"
            "    type: source\n"
            "    entry_point: pkg.a:A\n"
            "  - id: source-b\n"
            "    name: B\n"
            "    type: source\n"
            "    entry_point: pkg.b:B\n"
        )
        with patch(
            "datahub.plugin.plugin_manager._find_manifest_path_in_dist",
            return_value=str(manifest),
        ):
            plugins = _load_plugins_from_dist(self._dist())

        # One wheel, two connectors — both discovered, sharing package identity.
        assert [p.manifest.id for p in plugins] == ["source-a", "source-b"]
        assert {p.package_name for p in plugins} == {"datahub-bundle"}
        assert {p.version for p in plugins} == {"1.2.3"}
        assert all(p.manifest.author == "tester" for p in plugins)

    def test_no_manifest_returns_empty(self) -> None:
        with patch(
            "datahub.plugin.plugin_manager._find_manifest_path_in_dist",
            return_value=None,
        ):
            assert _load_plugins_from_dist(self._dist()) == []


class TestDiscoverPluginsErrorBoundary:
    """Test that discover_plugins isolates failures per-distribution."""

    @patch("datahub.plugin.plugin_manager.distributions")
    @patch("datahub.plugin.plugin_manager._load_plugins_from_dist")
    def test_corrupted_manifest_does_not_block_others(
        self, mock_load: MagicMock, mock_dists: MagicMock
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
            return [good_plugin]

        mock_load.side_effect = load_side_effect
        result = discover_plugins()

        assert "good-source" in result
        assert len(result) == 1

    @patch("datahub.plugin.plugin_manager.distributions")
    def test_exception_in_one_dist_skips_it(self, mock_dists: MagicMock) -> None:
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
    def test_list_installed_delegates_to_discover(
        self, mock_discover: MagicMock
    ) -> None:
        plugins = {"test-source": make_discovered_plugin()}
        mock_discover.return_value = plugins

        manager = PluginManager()
        result = manager.list_installed()
        assert result == plugins
        mock_discover.assert_called_once()

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_get_installed_found(self, mock_discover: MagicMock) -> None:
        plugins = {"test-source": make_discovered_plugin()}
        mock_discover.return_value = plugins

        manager = PluginManager()
        result = manager.get_installed("test-source")
        assert result is not None
        assert result.manifest.id == "test-source"

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_get_installed_not_found(self, mock_discover: MagicMock) -> None:
        mock_discover.return_value = {}

        manager = PluginManager()
        assert manager.get_installed("nonexistent") is None

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_is_installed(self, mock_discover: MagicMock) -> None:
        plugins = {"test-source": make_discovered_plugin()}
        mock_discover.return_value = plugins

        manager = PluginManager()
        assert manager.is_installed("test-source")
        assert not manager.is_installed("other-source")

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_uninstall_missing_plugin(self, mock_discover: MagicMock) -> None:
        mock_discover.return_value = {}

        manager = PluginManager()
        with pytest.raises(KeyError, match="not installed"):
            manager.uninstall("nonexistent")


class TestInstall:
    @patch("datahub.plugin.plugin_manager.discover_plugins")
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_success_new_plugin_appears(
        self, _mock_pip_cmd: MagicMock, mock_run: MagicMock, mock_discover: MagicMock
    ) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "install", "my-plugin"],
            returncode=0,
            stdout="",
            stderr="",
        )
        # Nothing before, one plugin after -> identified by the discovery diff.
        mock_discover.side_effect = [{}, {"test-source": make_discovered_plugin()}]

        manager = PluginManager()
        result = manager.install("my-plugin")
        assert [p.manifest.id for p in result] == ["test-source"]

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_multi_plugin_wheel_returns_all(
        self,
        _mock_pip_cmd: MagicMock,
        mock_run: MagicMock,
        mock_discover: MagicMock,
    ) -> None:
        # One wheel adds two connectors -> both are returned, not just the first.
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "install", "bundle"], returncode=0, stdout="", stderr=""
        )
        mock_discover.side_effect = [
            {},
            {
                "source-a": make_discovered_plugin(
                    plugin_id="source-a", package_name="datahub-bundle"
                ),
                "source-b": make_discovered_plugin(
                    plugin_id="source-b", package_name="datahub-bundle"
                ),
            },
        ]

        manager = PluginManager()
        result = manager.install("bundle-pkg")
        assert sorted(p.manifest.id for p in result) == ["source-a", "source-b"]

    @patch("datahub.plugin.plugin_manager._find_plugins_in_package")
    @patch("datahub.plugin.plugin_manager.discover_plugins", return_value={})
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_reinstall_no_new_id_uses_package_name(
        self,
        _mock_pip_cmd: MagicMock,
        mock_run: MagicMock,
        _mock_discover: MagicMock,
        mock_find: MagicMock,
    ) -> None:
        # Reinstall: discovery is unchanged (no new id), so the plugin is found
        # via the package name derived from the resolved target.
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "install", "my-plugin"], returncode=0, stdout="", stderr=""
        )
        mock_find.return_value = [make_discovered_plugin()]

        manager = PluginManager()
        result = manager.install("my-plugin==1.0")
        assert [p.manifest.id for p in result] == ["test-source"]
        mock_find.assert_called_once_with("my-plugin")

    @patch("datahub.plugin.plugin_manager.discover_plugins", return_value={})
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_timeout(
        self,
        _mock_pip_cmd: MagicMock,
        mock_run: MagicMock,
        _mock_discover: MagicMock,
    ) -> None:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=["pip"], timeout=300)

        manager = PluginManager()
        with pytest.raises(RuntimeError, match="timed out"):
            manager.install("slow-plugin")

    @patch("datahub.plugin.plugin_manager.discover_plugins", return_value={})
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_pip_failure(
        self,
        _mock_pip_cmd: MagicMock,
        mock_run: MagicMock,
        _mock_discover: MagicMock,
    ) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "install", "bad-plugin"],
            returncode=1,
            stdout="",
            stderr="ERROR: No matching distribution\n",
        )

        manager = PluginManager()
        with pytest.raises(RuntimeError, match="pip command failed"):
            manager.install("bad-plugin")

    @patch("datahub.plugin.plugin_manager._find_plugins_in_package", return_value=[])
    @patch("datahub.plugin.plugin_manager.discover_plugins", return_value={})
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_missing_manifest(
        self,
        _mock_pip_cmd: MagicMock,
        mock_run: MagicMock,
        _mock_discover: MagicMock,
        _mock_find: MagicMock,
    ) -> None:
        # No new plugin discovered and no manifest for the package -> error.
        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip", "install", "no-manifest-plugin"],
            returncode=0,
            stdout="",
            stderr="",
        )

        manager = PluginManager()
        with pytest.raises(ValueError, match="datahub-plugin.yaml"):
            manager.install("no-manifest-plugin")

    @patch("datahub.plugin.github_resolver.download_wheel")
    @patch("datahub.plugin.github_resolver.resolve_github_spec")
    @patch("datahub.plugin.plugin_manager.discover_plugins")
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_install_threads_sha256_to_download(
        self,
        _mock_pip_cmd: MagicMock,
        mock_run: MagicMock,
        mock_discover: MagicMock,
        mock_resolve: MagicMock,
        mock_download: MagicMock,
    ) -> None:
        from datahub.plugin.github_resolver import ResolvedWheel

        mock_run.return_value = subprocess.CompletedProcess(
            args=["pip"], returncode=0, stdout="", stderr=""
        )
        mock_discover.side_effect = [{}, {"test-source": make_discovered_plugin()}]
        mock_resolve.return_value = ResolvedWheel(
            download_url="https://example.com/a.whl", version="1.0"
        )
        mock_download.return_value = "/tmp/a.whl"

        PluginManager().install("github:acme/src", expected_sha256="abc123")

        # The expected checksum reaches download_wheel for verification.
        assert mock_download.call_args.kwargs["expected_sha256"] == "abc123"


class TestUninstall:
    @patch("subprocess.run")
    @patch("datahub.plugin.plugin_manager.discover_plugins")
    @patch("datahub.plugin.plugin_manager._pip_cmd", return_value=["pip"])
    def test_uninstall_success(
        self, _mock_pip_cmd: MagicMock, mock_discover: MagicMock, mock_run: MagicMock
    ) -> None:
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
        self, _mock_pip_cmd: MagicMock, mock_discover: MagicMock, mock_run: MagicMock
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
    def test_uninstall_timeout(
        self, _mock_pip_cmd: MagicMock, mock_discover: MagicMock, mock_run: MagicMock
    ) -> None:
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


class TestResolveInstallTarget:
    @patch("datahub.plugin.plugin_manager.RegistryClient")
    def test_bare_id_found_uses_repo_and_checksum(self, mock_rc: MagicMock) -> None:
        mock_rc.return_value.resolve.return_value = _index_entry()

        target = PluginManager().resolve_install_target("my-source")

        assert target.spec == "github:acme/my-source"
        assert target.version == "1.0.0"
        assert target.expected_sha256 == "deadbeef"
        assert target.entry is not None

    @patch("datahub.plugin.plugin_manager.RegistryClient")
    def test_version_override_drops_index_checksum(self, mock_rc: MagicMock) -> None:
        # The index sha256 is for 1.0.0; a request for a different version must
        # not carry that checksum onto a different wheel.
        mock_rc.return_value.resolve.return_value = _index_entry(version="1.0.0")

        target = PluginManager().resolve_install_target("my-source", version="2.0.0")

        assert target.spec == "github:acme/my-source"
        assert target.version == "2.0.0"
        assert target.expected_sha256 is None

    @patch("datahub.plugin.plugin_manager.RegistryClient")
    def test_bare_id_not_in_registry_passes_through(self, mock_rc: MagicMock) -> None:
        mock_rc.return_value.resolve.return_value = None

        target = PluginManager().resolve_install_target("random-pkg")

        assert target.spec == "random-pkg"
        assert target.entry is None
        assert target.expected_sha256 is None

    @patch("datahub.plugin.plugin_manager.RegistryClient")
    def test_non_bare_spec_skips_registry_lookup(self, mock_rc: MagicMock) -> None:
        # A github:/pip/wheel spec never triggers a registry (network) lookup.
        target = PluginManager().resolve_install_target("github:acme/src")

        assert target.spec == "github:acme/src"
        assert target.entry is None
        mock_rc.assert_not_called()
