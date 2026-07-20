"""Tests for the plugin scaffolding system."""

from pathlib import Path

import pytest
import yaml

from datahub.plugin.scaffold import scaffold_plugin


class TestScaffoldPlugin:
    def test_scaffold_source(self, tmp_path: Path) -> None:
        project = scaffold_plugin(
            name="my-test-source",
            plugin_type="source",
            output_dir=str(tmp_path),
            description="A test source plugin",
        )

        assert project.exists()
        assert (project / "pyproject.toml").exists()
        assert (project / "README.md").exists()
        assert (project / "src" / "my_test_source" / "__init__.py").exists()
        assert (project / "src" / "my_test_source" / "source.py").exists()
        assert (project / "src" / "my_test_source" / "config.py").exists()
        assert (project / "tests" / "test_source.py").exists()
        assert (project / ".github" / "workflows" / "release.yml").exists()

        # Validate manifest (lives inside the package for pip discovery)
        manifest_path = project / "src" / "my_test_source" / "datahub-plugin.yaml"
        assert manifest_path.exists()
        with open(manifest_path) as f:
            manifest = yaml.safe_load(f)
        assert manifest["id"] == "my-test-source"
        assert manifest["type"] == "source"
        assert "my_test_source.source:MyTestSource" in manifest["entry_point"]

    def test_scaffold_sink(self, tmp_path: Path) -> None:
        project = scaffold_plugin(
            name="my-sink",
            plugin_type="sink",
            output_dir=str(tmp_path),
        )

        assert (project / "src" / "my_sink" / "sink.py").exists()
        assert (project / "tests" / "test_sink.py").exists()

        with open(project / "src" / "my_sink" / "datahub-plugin.yaml") as f:
            manifest = yaml.safe_load(f)
        assert manifest["type"] == "sink"

    def test_scaffold_transformer(self, tmp_path: Path) -> None:
        project = scaffold_plugin(
            name="my-transformer",
            plugin_type="transformer",
            output_dir=str(tmp_path),
        )

        assert (project / "src" / "my_transformer" / "transformer.py").exists()

    def test_scaffold_includes_package_data_manifest(self, tmp_path: Path) -> None:
        """The manifest should also be in the src package dir so pip can find it."""
        project = scaffold_plugin(
            name="test-plugin",
            plugin_type="source",
            output_dir=str(tmp_path),
        )
        pkg_manifest = project / "src" / "test_plugin" / "datahub-plugin.yaml"
        assert pkg_manifest.exists()

    def test_pyproject_has_entry_point(self, tmp_path: Path) -> None:
        project = scaffold_plugin(
            name="ep-test",
            plugin_type="source",
            output_dir=str(tmp_path),
        )
        content = (project / "pyproject.toml").read_text()
        assert "datahub.ingestion.source.plugins" in content
        assert "ep-test" in content

    def test_rejects_invalid_name(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Invalid plugin name"):
            scaffold_plugin(
                name="My Plugin",
                plugin_type="source",
                output_dir=str(tmp_path),
            )

    def test_rejects_name_starting_with_digit(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Invalid plugin name"):
            scaffold_plugin(
                name="1bad-name",
                plugin_type="source",
                output_dir=str(tmp_path),
            )

    def test_rejects_uppercase_name(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Invalid plugin name"):
            scaffold_plugin(
                name="MyPlugin",
                plugin_type="source",
                output_dir=str(tmp_path),
            )

    def test_scaffold_includes_support_status_and_capabilities(
        self, tmp_path: Path
    ) -> None:
        project = scaffold_plugin(
            name="cap-test",
            plugin_type="source",
            output_dir=str(tmp_path),
        )
        manifest_path = project / "src" / "cap_test" / "datahub-plugin.yaml"
        with open(manifest_path) as f:
            manifest = yaml.safe_load(f)
        assert manifest["support_status"] == "COMMUNITY"
        assert isinstance(manifest["capabilities"], list)
        assert len(manifest["capabilities"]) >= 1
        assert manifest["capabilities"][0]["capability"] == "SCHEMA_METADATA"
