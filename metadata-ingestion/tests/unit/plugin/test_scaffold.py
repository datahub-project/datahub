"""Tests for the plugin scaffolding system."""

from pathlib import Path

import pytest
import yaml

from datahub.plugin.plugin_config import PluginManifestFile
from datahub.plugin.scaffold import (
    ScaffoldResult,
    add_connector,
    create_project,
    parse_connector_ref,
    scaffold_plugin,
)


class TestParseConnectorRef:
    def test_namespace_and_connector(self) -> None:
        assert parse_connector_ref("acme/salesforce") == ("acme", "salesforce")

    def test_bare_connector(self) -> None:
        assert parse_connector_ref("salesforce") == (None, "salesforce")

    def test_rejects_uppercase(self) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            parse_connector_ref("Acme/Salesforce")

    def test_rejects_leading_digit(self) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            parse_connector_ref("1bad")


class TestCreateProject:
    def test_namespaced_layout(self, tmp_path: Path) -> None:
        result = create_project(
            namespace="acme",
            connector="salesforce",
            plugin_type="source",
            output_dir=str(tmp_path),
            description="Salesforce connector",
        )
        project = result.project_dir
        assert project == tmp_path / "acme"

        # Namespace package holds the shared manifest; connector is a subpackage.
        assert (project / "src" / "acme" / "__init__.py").exists()
        assert (project / "src" / "acme" / "datahub-plugin.yaml").exists()
        assert (project / "src" / "acme" / "salesforce" / "__init__.py").exists()
        assert (project / "src" / "acme" / "salesforce" / "source.py").exists()
        assert (project / "src" / "acme" / "salesforce" / "config.py").exists()
        assert (project / "tests" / "test_salesforce.py").exists()
        assert (project / ".github" / "workflows" / "release.yml").exists()

        assert result.entry_point == "acme.salesforce.source:Salesforce"
        assert result.created_project is True

    def test_manifest_is_valid_multi_form(self, tmp_path: Path) -> None:
        result = create_project(
            namespace="acme",
            connector="salesforce",
            plugin_type="source",
            output_dir=str(tmp_path),
        )
        data = yaml.safe_load(result.manifest_path.read_text())
        # The generated manifest must parse via the real consumer.
        manifest_file = PluginManifestFile.model_validate(data)
        assert [m.id for m in manifest_file.plugins] == ["salesforce"]
        assert (
            manifest_file.plugins[0].entry_point == "acme.salesforce.source:Salesforce"
        )
        assert manifest_file.plugins[0].capabilities[0].capability == "SCHEMA_METADATA"

    def test_pyproject_entry_point(self, tmp_path: Path) -> None:
        create_project(
            namespace="acme",
            connector="salesforce",
            plugin_type="source",
            output_dir=str(tmp_path),
        )
        pyproject = (tmp_path / "acme" / "pyproject.toml").read_text()
        assert '[project.entry-points."datahub.ingestion.source.plugins"]' in pyproject
        assert 'salesforce = "acme.salesforce.source:Salesforce"' in pyproject

    def test_sink_type(self, tmp_path: Path) -> None:
        result = create_project(
            namespace="acme",
            connector="my-sink",
            plugin_type="sink",
            output_dir=str(tmp_path),
        )
        assert (result.project_dir / "src" / "acme" / "my_sink" / "sink.py").exists()
        assert result.entry_point == "acme.my_sink.sink:MySink"

    def test_rejects_invalid_connector(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            create_project(
                namespace="acme",
                connector="Bad Name",
                plugin_type="source",
                output_dir=str(tmp_path),
            )


class TestAddConnector:
    def _project(self, tmp_path: Path) -> ScaffoldResult:
        return create_project(
            namespace="acme",
            connector="salesforce",
            plugin_type="source",
            output_dir=str(tmp_path),
        )

    def test_adds_second_connector(self, tmp_path: Path) -> None:
        first = self._project(tmp_path)
        result = add_connector(
            project_dir=str(first.project_dir),
            manifest_path=str(first.manifest_path),
            connector="workday",
            plugin_type="source",
        )

        assert result.created_project is False
        assert result.pyproject_updated is True
        assert (first.project_dir / "src" / "acme" / "workday" / "source.py").exists()
        assert (first.project_dir / "tests" / "test_workday.py").exists()

        # Both connectors are now in the one shared manifest, and it still parses.
        data = yaml.safe_load(first.manifest_path.read_text())
        manifest_file = PluginManifestFile.model_validate(data)
        assert sorted(m.id for m in manifest_file.plugins) == ["salesforce", "workday"]

        pyproject = (first.project_dir / "pyproject.toml").read_text()
        assert 'workday = "acme.workday.source:Workday"' in pyproject

    def test_duplicate_connector_rejected(self, tmp_path: Path) -> None:
        first = self._project(tmp_path)
        with pytest.raises(ValueError, match="already exists"):
            add_connector(
                project_dir=str(first.project_dir),
                manifest_path=str(first.manifest_path),
                connector="salesforce",
                plugin_type="source",
            )

    def test_flat_manifest_converted_to_multi(self, tmp_path: Path) -> None:
        first = self._project(tmp_path)
        # Rewrite the manifest into the legacy flat (single-plugin) form.
        first.manifest_path.write_text(
            "api_version: datahub/v1\n"
            "author: me\n"
            "id: salesforce\n"
            "name: Salesforce\n"
            "type: source\n"
            "entry_point: acme.salesforce.source:Salesforce\n"
        )

        add_connector(
            project_dir=str(first.project_dir),
            manifest_path=str(first.manifest_path),
            connector="workday",
            plugin_type="source",
        )

        data = yaml.safe_load(first.manifest_path.read_text())
        assert "plugins" in data
        # Package-level key hoisted to the top; per-plugin fields kept on entry.
        assert data["author"] == "me"
        manifest_file = PluginManifestFile.model_validate(data)
        assert sorted(m.id for m in manifest_file.plugins) == ["salesforce", "workday"]

    def test_adding_new_type_creates_entry_point_table(self, tmp_path: Path) -> None:
        first = self._project(tmp_path)  # source-only pyproject
        add_connector(
            project_dir=str(first.project_dir),
            manifest_path=str(first.manifest_path),
            connector="my-sink",
            plugin_type="sink",
        )
        pyproject = (first.project_dir / "pyproject.toml").read_text()
        assert '[project.entry-points."datahub.ingestion.sink.plugins"]' in pyproject
        assert 'my-sink = "acme.my_sink.sink:MySink"' in pyproject

    def test_missing_pyproject_reports_flag(self, tmp_path: Path) -> None:
        first = self._project(tmp_path)
        (first.project_dir / "pyproject.toml").unlink()
        result = add_connector(
            project_dir=str(first.project_dir),
            manifest_path=str(first.manifest_path),
            connector="workday",
            plugin_type="source",
        )
        # The connector is still written; only the pyproject wiring is skipped.
        assert result.pyproject_updated is False
        assert (first.project_dir / "src" / "acme" / "workday" / "source.py").exists()


class TestGeneratedConnectorsLoad:
    """Generated connectors must actually import and instantiate — file existence
    is not enough (a wrong base-class generic still writes a file but fails to
    load)."""

    def test_all_three_types_import_and_instantiate(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import importlib

        from datahub.ingestion.api.common import PipelineContext

        first = create_project(
            namespace="loadns",
            connector="mysrc",
            plugin_type="source",
            output_dir=str(tmp_path),
        )
        add_connector(
            project_dir=str(first.project_dir),
            manifest_path=str(first.manifest_path),
            connector="mysink",
            plugin_type="sink",
        )
        add_connector(
            project_dir=str(first.project_dir),
            manifest_path=str(first.manifest_path),
            connector="myxform",
            plugin_type="transformer",
        )

        monkeypatch.syspath_prepend(str(first.project_dir / "src"))
        ctx = PipelineContext(run_id="scaffold-load-test")

        src_cls = importlib.import_module("loadns.mysrc.source").Mysrc
        sink_cls = importlib.import_module("loadns.mysink.sink").Mysink
        xform_cls = importlib.import_module("loadns.myxform.transformer").Myxform

        # create() is what the pipeline calls — exercise the real construction path.
        assert src_cls.create({}, ctx) is not None
        assert sink_cls.create({}, ctx) is not None
        assert xform_cls.create({}, ctx) is not None


class TestScaffoldPluginBackCompat:
    def test_bare_name_namespace_equals_connector(self, tmp_path: Path) -> None:
        project = scaffold_plugin(
            name="my-test-source",
            plugin_type="source",
            output_dir=str(tmp_path),
        )
        assert project == tmp_path / "my-test-source"
        # namespace == connector -> nested package/connector dirs.
        source = project / "src" / "my_test_source" / "my_test_source" / "source.py"
        assert source.exists()
        data = yaml.safe_load(
            (project / "src" / "my_test_source" / "datahub-plugin.yaml").read_text()
        )
        manifest_file = PluginManifestFile.model_validate(data)
        assert manifest_file.plugins[0].id == "my-test-source"
