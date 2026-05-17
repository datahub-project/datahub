"""
Unit tests for selective_ci_checks.py.

Tests the classify() and build_import_graph() functions against
synthetic connector structures to verify correct behavior for all
tiers: direct connectors, shared bases, non-connector changes,
and safe defaults.
"""

import sys
from pathlib import Path

import pytest
import yaml

# Add scripts dir to path so we can import the module
# tests/unit/ -> tests/ -> metadata-ingestion/ -> metadata-ingestion/scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))
from selective_ci_checks import (
    CIDecisions,
    EntryPointInfo,
    _find_source_dir,
    _narrow_ep_tests,
    _register_plugin_variants,
    _source_path_to_module,
    build_import_graph,
    build_source_to_test_dirs,
    classify,
    load_connector_registry,
    validate,
)


@pytest.fixture
def repo_root(tmp_path):
    """Create a minimal but realistic connector structure for testing."""
    mi = tmp_path / "metadata-ingestion"
    src = mi / "src" / "datahub" / "ingestion" / "source"
    tests = mi / "tests" / "integration"

    # Direct connector -- auto-discovered by convention (no connector.yaml)
    (src / "powerbi").mkdir(parents=True)
    (src / "powerbi" / "powerbi.py").write_text(
        "from datahub.ingestion.api.source import Source\n"
        "class PowerBISource(Source): pass\n"
    )
    (src / "powerbi" / "__init__.py").write_text("")
    (tests / "powerbi").mkdir(parents=True)

    # Shared source directory — no connector.yaml needed; discovered automatically
    (src / "sql").mkdir(parents=True)
    (src / "sql" / "sql_common.py").write_text("class SQLAlchemySource: pass\n")
    (src / "sql" / "clickhouse.py").write_text(
        "from datahub.ingestion.source.sql.sql_common import SQLAlchemySource\n"
        "class ClickhouseSource(SQLAlchemySource): pass\n"
    )
    (src / "sql" / "__init__.py").write_text("")
    (tests / "clickhouse").mkdir(parents=True)

    # Consumer that imports from sql base -- auto-discovered by convention
    (src / "mysql").mkdir(parents=True)
    (src / "mysql" / "mysql.py").write_text(
        "from datahub.ingestion.source.sql.sql_common import SQLAlchemySource\n"
        "class MySQLSource(SQLAlchemySource): pass\n"
    )
    (src / "mysql" / "__init__.py").write_text("")
    (tests / "mysql").mkdir(parents=True)

    # Another consumer -- auto-discovered
    (src / "postgres").mkdir(parents=True)
    (src / "postgres" / "postgres.py").write_text(
        "from datahub.ingestion.source.sql.sql_common import SQLAlchemySource\n"
        "class PostgresSource(SQLAlchemySource): pass\n"
    )
    (src / "postgres" / "__init__.py").write_text("")
    (tests / "postgres").mkdir(parents=True)

    # Connector with no shared deps -- auto-discovered
    (src / "kafka").mkdir(parents=True)
    (src / "kafka" / "kafka.py").write_text(
        "from datahub.ingestion.api.source import Source\n"
        "class KafkaSource(Source): pass\n"
    )
    (src / "kafka" / "__init__.py").write_text("")
    (tests / "kafka").mkdir(parents=True)

    # Create tests/ and scripts/ dirs for SAFE_PREFIXES
    (mi / "scripts").mkdir(parents=True, exist_ok=True)

    # setup.py with entry points so EP-based narrowing works
    (mi / "setup.py").write_text(
        "entry_points = {\n"
        '    "datahub.ingestion.source.plugins": [\n'
        '        "clickhouse = datahub.ingestion.source.sql.clickhouse:ClickhouseSource",\n'
        "    ]\n"
        "}\n"
    )

    return tmp_path


class TestNonConnectorChanges:
    """Any file NOT under source/{connector}/ triggers full suite."""

    def test_api_change(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/api/source.py"], repo_root
        )
        assert d.run_all_integration is True

    def test_setup_py(self, repo_root):
        d = classify(["metadata-ingestion/setup.py"], repo_root)
        assert d.run_all_integration is True

    def test_metadata_models(self, repo_root):
        d = classify(["metadata-models/src/main/resources/entity.graphql"], repo_root)
        assert d.run_all_integration is True

    def test_emitter_change(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/emitter/mce_builder.py"], repo_root
        )
        assert d.run_all_integration is True

    def test_workflow_change(self, repo_root):
        d = classify([".github/workflows/metadata-ingestion.yml"], repo_root)
        # Not under metadata-ingestion/ or metadata-models/ -- ignored
        assert d.run_all_integration is False
        assert d.test_matrix == []


class TestSharedBase:
    """Shared base changes trigger all consumers via import analysis."""

    def test_sql_base_change_triggers_consumers(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/sql/sql_common.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        paths = {e["test_path"] for e in d.test_matrix}
        # mysql and postgres detected via import analysis
        assert "tests/integration/mysql/" in paths
        assert "tests/integration/postgres/" in paths
        # clickhouse lives inside sql/ and maps via entry points
        assert "tests/integration/clickhouse/" in paths
        # powerbi does NOT import from sql
        assert "tests/integration/powerbi/" not in paths


class TestDirectConnector:
    """Direct connector change triggers only that connector's tests."""

    def test_powerbi_only(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "powerbi"
        assert d.test_matrix[0]["test_path"] == "tests/integration/powerbi/"

    def test_kafka_only(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "kafka"


class TestSafeDefaults:
    """Unknown or unmatched files trigger full suite."""

    def test_unknown_metadata_ingestion_file(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/brand_new.py"], repo_root
        )
        assert d.run_all_integration is True

    def test_unknown_source_dir(self, repo_root):
        (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
            / "unknown_connector"
        ).mkdir()
        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/unknown_connector/foo.py"
            ],
            repo_root,
        )
        assert d.run_all_integration is True

    def test_non_metadata_file_ignored(self, repo_root):
        d = classify(["docs/README.md"], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []

    def test_empty_diff(self, repo_root):
        d = classify([], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []


class TestIntegrationTestChanges:
    """Integration test / golden file changes trigger that connector's tests."""

    def test_test_file_triggers_connector(self, repo_root):
        d = classify(
            ["metadata-ingestion/tests/integration/powerbi/test_powerbi.py"], repo_root
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "powerbi"

    def test_golden_file_triggers_connector(self, repo_root):
        d = classify(
            ["metadata-ingestion/tests/integration/kafka/golden_mces.json"], repo_root
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "kafka"


class TestSafePrefixes:
    """Unit tests, scripts, docs don't trigger integration tests."""

    def test_unit_test_change(self, repo_root):
        d = classify(["metadata-ingestion/tests/unit/test_something.py"], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []

    def test_script_change(self, repo_root):
        d = classify(["metadata-ingestion/scripts/selective_ci_checks.py"], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []

    def test_docs_change(self, repo_root):
        d = classify(["metadata-ingestion/docs/dev_guides/selective_ci.md"], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []


class TestMixedChanges:
    """Multiple changed files are handled correctly."""

    def test_connector_plus_non_connector(self, repo_root):
        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py",
                "metadata-ingestion/setup.py",
            ],
            repo_root,
        )
        assert d.run_all_integration is True

    def test_two_connectors(self, repo_root):
        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py",
                "metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py",
            ],
            repo_root,
        )
        assert d.run_all_integration is False
        connectors = {e["connector"] for e in d.test_matrix}
        assert connectors == {"powerbi", "kafka"}

    def test_connector_plus_test(self, repo_root):
        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py",
                "metadata-ingestion/tests/integration/powerbi/test_powerbi.py",
            ],
            repo_root,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "powerbi"


class TestExtraSourcePaths:
    """Connectors with extra_source_paths trigger correctly."""

    def test_extra_source_path_triggers_connector(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        tests = repo_root / "metadata-ingestion" / "tests" / "integration"

        # Add extra_source_paths to powerbi
        (src / "powerbi" / "connector.yaml").write_text(
            yaml.dump(
                {
                    "extra_source_paths": [
                        "src/datahub/ingestion/source/powerbi_report_server/"
                    ],
                }
            )
        )
        # Create the extra source dir
        (src / "powerbi_report_server").mkdir(parents=True, exist_ok=True)
        (src / "powerbi_report_server" / "__init__.py").write_text("")
        (tests / "powerbi_report_server").mkdir(parents=True, exist_ok=True)

        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/powerbi_report_server/server.py"
            ],
            repo_root,
        )
        assert d.run_all_integration is False
        connectors = {e["connector"] for e in d.test_matrix}
        assert "powerbi" in connectors
        assert "powerbi_report_server" in connectors


class TestExtraTestPaths:
    """Connectors with extra_test_paths include all test dirs."""

    def test_extra_test_paths_included(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        tests = repo_root / "metadata-ingestion" / "tests" / "integration"

        # Add extra_test_paths to powerbi
        (src / "powerbi" / "connector.yaml").write_text(
            yaml.dump(
                {
                    "extra_test_paths": ["tests/integration/powerbi_extras/"],
                }
            )
        )
        (tests / "powerbi_extras").mkdir(parents=True, exist_ok=True)

        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        paths = {e["test_path"] for e in d.test_matrix}
        assert "tests/integration/powerbi/" in paths
        assert "tests/integration/powerbi_extras/" in paths


class TestSafetyNet:
    """Changed source dirs that produce no tests fall back to full suite."""

    def test_shared_base_with_empty_test_paths(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )

        # Create a source dir with no test path and no consumers
        (src / "orphan_base").mkdir(parents=True)
        (src / "orphan_base" / "base.py").write_text("class OrphanBase: pass\n")

        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/orphan_base/base.py"],
            repo_root,
        )
        # Safety net: changed source dirs with no test output -> full suite
        assert d.run_all_integration is True


class TestImportGraph:
    """Verify import graph is built correctly."""

    def test_mysql_depends_on_sql(self, repo_root):
        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        mysql_deps = graph.get("src/datahub/ingestion/source/mysql", set())
        assert "src/datahub/ingestion/source/sql" in mysql_deps

    def test_powerbi_does_not_depend_on_sql(self, repo_root):
        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        powerbi_deps = graph.get("src/datahub/ingestion/source/powerbi", set())
        assert "src/datahub/ingestion/source/sql" not in powerbi_deps

    def test_no_self_imports(self, repo_root):
        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        for src_dir, deps in graph.items():
            assert src_dir not in deps, f"{src_dir} has self-import"

    def test_bare_import_detected(self, repo_root):
        """Test that 'import datahub.ingestion.source.sql' (bare import) is detected."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        tests = repo_root / "metadata-ingestion" / "tests" / "integration"

        (src / "bare_importer").mkdir(parents=True)
        (src / "bare_importer" / "source.py").write_text(
            "import datahub.ingestion.source.sql.sql_common\n"
        )
        (src / "bare_importer" / "__init__.py").write_text("")
        (tests / "bare_importer").mkdir(parents=True)

        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        bare_deps = graph.get("src/datahub/ingestion/source/bare_importer", set())
        assert "src/datahub/ingestion/source/sql" in bare_deps

    def test_syntax_error_skipped(self, repo_root):
        """A .py file with syntax errors doesn't crash the import graph build."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "broken.py").write_text("def broken(\n")

        registry = load_connector_registry(repo_root)
        graph = build_import_graph(repo_root, registry)
        # Should complete without error
        assert "src/datahub/ingestion/source/powerbi" in graph


class TestConventionDiscovery:
    """Verify convention-based connector discovery."""

    def test_convention_discovers_matching_dirs(self, repo_root):
        registry = load_connector_registry(repo_root)
        names = {c.source_dir.split("/")[-1] for c in registry}
        assert "powerbi" in names
        assert "kafka" in names
        assert "mysql" in names
        assert "postgres" in names

    def test_shared_dir_auto_discovered(self, repo_root):
        """Shared source directories are auto-registered without connector.yaml."""
        registry = load_connector_registry(repo_root)
        sql_entries = [c for c in registry if c.source_dir.endswith("/sql")]
        assert len(sql_entries) == 1
        # No connector.yaml — is_shared_base is not set
        assert not hasattr(sql_entries[0], "is_shared_base")

    def test_underscore_dirs_excluded(self, repo_root):
        """__pycache__ and similar dirs are not treated as connectors."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "__pycache__").mkdir(parents=True, exist_ok=True)
        registry = load_connector_registry(repo_root)
        names = {c.source_dir.split("/")[-1] for c in registry}
        assert "__pycache__" not in names


class TestMalformedYAML:
    """Malformed connector.yaml files are handled gracefully."""

    def test_invalid_yaml_syntax_exits(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "connector.yaml").write_text("is_shared_base: [unterminated")
        with pytest.raises(SystemExit):
            load_connector_registry(repo_root)

    def test_yaml_non_dict_exits(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "connector.yaml").write_text("- just\n- a\n- list\n")
        with pytest.raises(SystemExit):
            load_connector_registry(repo_root)

    def test_empty_yaml_uses_convention(self, repo_root):
        """An empty connector.yaml falls back to convention discovery."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "connector.yaml").write_text("")

        registry = load_connector_registry(repo_root)
        powerbi = [c for c in registry if c.source_dir.endswith("/powerbi")]
        assert len(powerbi) == 1
        # Convention should still discover the test path
        assert powerbi[0].test_path == "tests/integration/powerbi/"


class TestValidate:
    """Validate function catches misconfigurations."""

    def test_valid_config_no_errors(self, repo_root):
        errors = validate(repo_root)
        assert errors == []

    def test_nonexistent_test_path(self, repo_root):
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        (src / "powerbi" / "connector.yaml").write_text(
            yaml.dump(
                {
                    "test_path": "tests/integration/does_not_exist/",
                }
            )
        )
        errors = validate(repo_root)
        assert any("does not exist" in e for e in errors)


class TestCIDecisionsInvariant:
    """CIDecisions enforces mutual exclusivity."""

    def test_run_all_with_matrix_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            CIDecisions(
                run_all_integration=True,
                test_matrix=[{"connector": "x", "test_path": "y"}],
            )

    def test_run_all_empty_matrix_ok(self):
        d = CIDecisions(run_all_integration=True)
        assert d.test_matrix == []

    def test_selective_with_matrix_ok(self):
        d = CIDecisions(test_matrix=[{"connector": "x", "test_path": "y"}])
        assert d.run_all_integration is False


class TestSQLNarrowing:
    """Verify per-file narrowing inside shared source directories."""

    def test_thin_wrapper_runs_only_its_tests(self, repo_root):
        """Changing clickhouse.py only triggers clickhouse tests, not all SQL."""
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/sql/clickhouse.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        paths = {e["test_path"] for e in d.test_matrix}
        assert "tests/integration/clickhouse/" in paths
        assert "tests/integration/mysql/" not in paths
        assert "tests/integration/postgres/" not in paths

    def test_thin_wrapper_does_not_bleed_to_powerbi(self, repo_root):
        """Changing clickhouse.py does not trigger powerbi tests."""
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/sql/clickhouse.py"],
            repo_root,
        )
        paths = {e["test_path"] for e in d.test_matrix}
        assert "tests/integration/powerbi/" not in paths

    def test_shared_utility_triggers_all_sql(self, repo_root):
        """Changing sql_common.py triggers all SQL connector tests."""
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/sql/sql_common.py"],
            repo_root,
        )
        assert d.run_all_integration is False
        paths = {e["test_path"] for e in d.test_matrix}
        assert "tests/integration/mysql/" in paths
        assert "tests/integration/postgres/" in paths
        assert "tests/integration/clickhouse/" in paths
        assert "tests/integration/powerbi/" not in paths


class TestNarrowEpTests:
    """Direct unit tests for _narrow_ep_tests."""

    EP_MODULES = {
        "datahub.ingestion.source.sql.clickhouse": EntryPointInfo(
            source_dir="src/datahub/ingestion/source/sql",
            test_path="tests/integration/clickhouse/",
        ),
        "datahub.ingestion.source.sql.mysql": EntryPointInfo(
            source_dir="src/datahub/ingestion/source/sql",
            test_path="tests/integration/mysql/",
        ),
    }

    def test_specific_file_returns_its_test(self):
        result = _narrow_ep_tests(
            "src/datahub/ingestion/source/sql",
            ["metadata-ingestion/src/datahub/ingestion/source/sql/clickhouse.py"],
            self.EP_MODULES,
        )
        assert result == {"tests/integration/clickhouse/"}

    def test_shared_utility_returns_none(self):
        result = _narrow_ep_tests(
            "src/datahub/ingestion/source/sql",
            ["metadata-ingestion/src/datahub/ingestion/source/sql/sql_common.py"],
            self.EP_MODULES,
        )
        assert result is None

    def test_no_files_in_dir_returns_none(self):
        result = _narrow_ep_tests(
            "src/datahub/ingestion/source/sql",
            ["metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py"],
            self.EP_MODULES,
        )
        assert result is None

    def test_multiple_files_accumulates_tests(self):
        result = _narrow_ep_tests(
            "src/datahub/ingestion/source/sql",
            [
                "metadata-ingestion/src/datahub/ingestion/source/sql/clickhouse.py",
                "metadata-ingestion/src/datahub/ingestion/source/sql/mysql.py",
            ],
            self.EP_MODULES,
        )
        assert result == {
            "tests/integration/clickhouse/",
            "tests/integration/mysql/",
        }

    def test_one_shared_file_blocks_narrowing(self):
        """Even one shared utility file prevents narrowing."""
        result = _narrow_ep_tests(
            "src/datahub/ingestion/source/sql",
            [
                "metadata-ingestion/src/datahub/ingestion/source/sql/clickhouse.py",
                "metadata-ingestion/src/datahub/ingestion/source/sql/sql_common.py",
            ],
            self.EP_MODULES,
        )
        assert result is None


class TestBuildSourceToTestDirs:
    """Verify entry-point-based test dir discovery."""

    def test_clickhouse_maps_to_sql_source(self, repo_root):
        registry = load_connector_registry(repo_root)
        ep_mapping = build_source_to_test_dirs(repo_root, registry)
        sql_tests = ep_mapping.source_to_tests.get(
            "src/datahub/ingestion/source/sql", set()
        )
        assert "tests/integration/clickhouse/" in sql_tests

    def test_ep_modules_populated(self, repo_root):
        registry = load_connector_registry(repo_root)
        ep_mapping = build_source_to_test_dirs(repo_root, registry)
        info = ep_mapping.ep_modules["datahub.ingestion.source.sql.clickhouse"]
        assert info.test_path == "tests/integration/clickhouse/"
        assert info.source_dir == "src/datahub/ingestion/source/sql"

    def test_missing_setup_py_returns_empty(self, repo_root):
        (repo_root / "metadata-ingestion" / "setup.py").unlink()
        registry = load_connector_registry(repo_root)
        ep_mapping = build_source_to_test_dirs(repo_root, registry)
        assert ep_mapping.source_to_tests == {}
        assert ep_mapping.ep_modules == {}


class TestSourcePathToModule:
    """Direct unit tests for _source_path_to_module."""

    def test_directory_path(self):
        assert (
            _source_path_to_module("src/datahub/ingestion/source/sql")
            == "datahub.ingestion.source.sql"
        )

    def test_file_path(self):
        assert (
            _source_path_to_module("src/datahub/ingestion/source/sql/clickhouse.py")
            == "datahub.ingestion.source.sql.clickhouse"
        )

    def test_single_file_connector(self):
        assert (
            _source_path_to_module("src/datahub/ingestion/source/feast.py")
            == "datahub.ingestion.source.feast"
        )

    def test_nested_directory(self):
        assert (
            _source_path_to_module("src/datahub/ingestion/source/sql/mssql")
            == "datahub.ingestion.source.sql.mssql"
        )


class TestFindSourceDir:
    """Direct unit tests for _find_source_dir."""

    def test_exact_match(self, repo_root):
        registry = load_connector_registry(repo_root)
        sorted_reg = sorted(registry, key=lambda c: len(c.source_dir), reverse=True)
        result = _find_source_dir("datahub.ingestion.source.sql", sorted_reg)
        assert result == "src/datahub/ingestion/source/sql"

    def test_submodule_match(self, repo_root):
        registry = load_connector_registry(repo_root)
        sorted_reg = sorted(registry, key=lambda c: len(c.source_dir), reverse=True)
        result = _find_source_dir("datahub.ingestion.source.sql.clickhouse", sorted_reg)
        assert result == "src/datahub/ingestion/source/sql"

    def test_no_match(self, repo_root):
        registry = load_connector_registry(repo_root)
        sorted_reg = sorted(registry, key=lambda c: len(c.source_dir), reverse=True)
        result = _find_source_dir("datahub.ingestion.source.nonexistent", sorted_reg)
        assert result is None


class TestRegisterPluginVariants:
    """Direct unit tests for _register_plugin_variants."""

    def test_plain_name(self):
        mapping: dict[str, str] = {}
        _register_plugin_variants(mapping, "clickhouse", "mod.clickhouse")
        assert mapping["clickhouse"] == "mod.clickhouse"

    def test_hyphen_underscore_swap(self):
        mapping: dict[str, str] = {}
        _register_plugin_variants(mapping, "kafka-connect", "mod.kafka_connect")
        assert "kafka-connect" in mapping
        assert "kafka_connect" in mapping

    def test_datahub_prefix_stripped(self):
        mapping: dict[str, str] = {}
        _register_plugin_variants(
            mapping, "datahub-business-glossary", "mod.business_glossary"
        )
        assert "datahub-business-glossary" in mapping
        assert "business-glossary" in mapping
        assert "business_glossary" in mapping
        assert "datahub_business_glossary" in mapping

    def test_no_datahub_prefix(self):
        mapping: dict[str, str] = {}
        _register_plugin_variants(mapping, "mysql", "mod.mysql")
        # Should NOT have "datahub-mysql" variants
        assert "datahub-mysql" not in mapping
        assert set(mapping.keys()) == {"mysql"}


class TestSingleFileConnector:
    """Test single-file connector discovery and classification."""

    @pytest.fixture
    def single_file_repo(self, repo_root):
        """Extend the base fixture with a single-file connector."""
        src = (
            repo_root
            / "metadata-ingestion"
            / "src"
            / "datahub"
            / "ingestion"
            / "source"
        )
        tests = repo_root / "metadata-ingestion" / "tests" / "integration"
        (src / "feast.py").write_text(
            "from datahub.ingestion.api.source import Source\n"
            "class FeastSource(Source): pass\n"
        )
        (tests / "feast").mkdir(parents=True)
        return repo_root

    def test_single_file_in_registry(self, single_file_repo):
        registry = load_connector_registry(single_file_repo)
        feast_entries = [c for c in registry if c.source_dir.endswith("feast.py")]
        assert len(feast_entries) == 1
        assert feast_entries[0].is_single_file is True
        assert feast_entries[0].test_path == "tests/integration/feast/"

    def test_single_file_change_triggers_tests(self, single_file_repo):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/feast.py"],
            single_file_repo,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "feast"

    def test_similar_name_does_not_match(self, single_file_repo):
        """feast_enterprise.py should NOT match the feast.py connector."""
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/feast_enterprise.py"],
            single_file_repo,
        )
        # Unknown file -> safety net -> run all
        assert d.run_all_integration is True


class TestRootLevelIntegrationTestFile:
    """Files directly in tests/integration/ trigger full suite."""

    def test_conftest_triggers_run_all(self, repo_root):
        d = classify(["metadata-ingestion/tests/integration/conftest.py"], repo_root)
        assert d.run_all_integration is True

    def test_subdirectory_file_does_not_trigger_run_all(self, repo_root):
        d = classify(
            ["metadata-ingestion/tests/integration/powerbi/test_powerbi.py"], repo_root
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1


class TestSafePrefixConftest:
    """tests/conftest.py matches SAFE_PREFIXES, distinct from tests/integration/conftest.py."""

    def test_root_conftest_is_safe(self, repo_root):
        d = classify(["metadata-ingestion/tests/conftest.py"], repo_root)
        assert d.run_all_integration is False
        assert d.test_matrix == []


class TestSourceFileExtensions:
    """Any file in a connector source dir triggers tests, except .md."""

    def test_yaml_change_triggers_connector(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/powerbi/config.yaml"],
            repo_root,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "powerbi"

    def test_json_change_triggers_connector(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/powerbi/schema.json"],
            repo_root,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "powerbi"

    def test_unknown_extension_triggers_connector(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/powerbi/data.parquet"],
            repo_root,
        )
        assert d.run_all_integration is False
        assert len(d.test_matrix) == 1
        assert d.test_matrix[0]["connector"] == "powerbi"

    def test_markdown_file_ignored(self, repo_root):
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/powerbi/README.md"],
            repo_root,
        )
        # .md is in IGNORED_SOURCE_EXTENSIONS — documentation only
        assert d.run_all_integration is False
        assert d.test_matrix == []


class TestNarrowEpParentPackage:
    """Parent-package matching in _narrow_ep_tests."""

    def test_init_file_matches_child_ep(self):
        """Changing sql/__init__.py matches the clickhouse EP via parent-package rule."""
        ep_modules = {
            "datahub.ingestion.source.sql.clickhouse": EntryPointInfo(
                source_dir="src/datahub/ingestion/source/sql",
                test_path="tests/integration/clickhouse/",
            ),
        }
        result = _narrow_ep_tests(
            "src/datahub/ingestion/source/sql",
            ["metadata-ingestion/src/datahub/ingestion/source/sql/__init__.py"],
            ep_modules,
        )
        # __init__.py module is datahub.ingestion.source.sql.__init__
        # This does NOT match clickhouse EP via parent-package rule because
        # "datahub.ingestion.source.sql.clickhouse" does not start with
        # "datahub.ingestion.source.sql.__init__."
        # So narrowing fails → returns None → caller runs all SQL tests
        assert result is None


class TestKnownConnectorWithoutTests:
    """Connector with a setup.py entry point but no integration test directory.

    This is the 'glue scenario': glue.py lives in source/aws/ alongside shared
    utilities (aws_common.py, s3_util.py). Without this fix, changing glue.py
    would cascade through the import graph to every connector that imports from
    source/aws/ (athena, redshift, s3, snowflake, etc.) because the narrowing
    logic couldn't distinguish glue (a known connector) from a shared utility.
    """

    @pytest.fixture
    def aws_like_repo(self, tmp_path):
        """Create a repo structure mimicking source/aws/ with glue + shared utils."""
        mi = tmp_path / "metadata-ingestion"
        src = mi / "src" / "datahub" / "ingestion" / "source"
        tests = mi / "tests" / "integration"

        # Shared source dir (like source/aws/) with multiple connectors + utilities
        (src / "aws").mkdir(parents=True)
        (src / "aws" / "__init__.py").write_text("")
        (src / "aws" / "aws_common.py").write_text("class AwsSourceConfig: pass\n")
        (src / "aws" / "s3_util.py").write_text("def make_s3_urn(): pass\n")
        (src / "aws" / "glue.py").write_text(
            "from datahub.ingestion.source.aws.aws_common import AwsSourceConfig\n"
            "class GlueSource: pass\n"
        )
        (src / "aws" / "sagemaker.py").write_text(
            "from datahub.ingestion.source.aws.aws_common import AwsSourceConfig\n"
            "class SagemakerSource: pass\n"
        )
        # s3 has integration tests, glue and sagemaker do not
        (tests / "s3").mkdir(parents=True)

        # Consumer that imports from aws (like source/athena/)
        (src / "athena").mkdir(parents=True)
        (src / "athena" / "__init__.py").write_text("")
        (src / "athena" / "athena.py").write_text(
            "from datahub.ingestion.source.aws.s3_util import make_s3_urn\n"
            "class AthenaSource: pass\n"
        )
        (tests / "athena").mkdir(parents=True)

        # Independent connector
        (src / "kafka").mkdir(parents=True)
        (src / "kafka" / "__init__.py").write_text("")
        (src / "kafka" / "kafka.py").write_text("class KafkaSource: pass\n")
        (tests / "kafka").mkdir(parents=True)

        (mi / "scripts").mkdir(parents=True, exist_ok=True)
        (mi / "setup.py").write_text(
            "entry_points = {\n"
            '    "datahub.ingestion.source.plugins": [\n'
            '        "s3 = datahub.ingestion.source.aws.s3_util:S3Source",\n'
            '        "glue = datahub.ingestion.source.aws.glue:GlueSource",\n'
            '        "sagemaker = datahub.ingestion.source.aws.sagemaker:SagemakerSource",\n'
            '        "athena = datahub.ingestion.source.athena.athena:AthenaSource",\n'
            '        "kafka = datahub.ingestion.source.kafka.kafka:KafkaSource",\n'
            "    ]\n"
            "}\n"
        )

        return tmp_path

    def test_known_ep_without_tests_does_not_cascade(self, aws_like_repo):
        """Changing glue.py should NOT trigger athena/s3/kafka tests."""
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/aws/glue.py"],
            aws_like_repo,
        )
        assert d.run_all_integration is False
        assert d.test_matrix == []

    def test_shared_utility_still_cascades(self, aws_like_repo):
        """Changing aws_common.py SHOULD trigger all dependents (athena, s3)."""
        d = classify(
            ["metadata-ingestion/src/datahub/ingestion/source/aws/aws_common.py"],
            aws_like_repo,
        )
        assert d.run_all_integration is False
        paths = {e["test_path"] for e in d.test_matrix}
        assert "tests/integration/athena/" in paths
        assert "tests/integration/s3/" in paths
        assert "tests/integration/kafka/" not in paths

    def test_mixed_known_ep_and_shared_utility_cascades(self, aws_like_repo):
        """Changing both glue.py and aws_common.py should cascade (aws_common is shared)."""
        d = classify(
            [
                "metadata-ingestion/src/datahub/ingestion/source/aws/glue.py",
                "metadata-ingestion/src/datahub/ingestion/source/aws/aws_common.py",
            ],
            aws_like_repo,
        )
        assert d.run_all_integration is False
        paths = {e["test_path"] for e in d.test_matrix}
        assert "tests/integration/athena/" in paths
        assert "tests/integration/s3/" in paths

    def test_narrow_ep_tests_with_known_ep_no_tests(self):
        """Direct test: _narrow_ep_tests returns empty set for known EP without tests."""
        aws_dir = "src/datahub/ingestion/source/aws"
        ep_modules = {
            "datahub.ingestion.source.aws.s3_util": EntryPointInfo(
                source_dir=aws_dir, test_path="tests/integration/s3/"
            ),
            "datahub.ingestion.source.aws.glue": EntryPointInfo(source_dir=aws_dir),
            "datahub.ingestion.source.aws.sagemaker": EntryPointInfo(
                source_dir=aws_dir
            ),
        }
        result = _narrow_ep_tests(
            aws_dir,
            ["metadata-ingestion/src/datahub/ingestion/source/aws/glue.py"],
            ep_modules,
        )
        # glue is a known EP (test_path=None) — narrowing succeeds with empty set
        assert result == set()

    def test_narrow_ep_tests_shared_utility_returns_none(self):
        """Direct test: _narrow_ep_tests returns None for shared utility."""
        aws_dir = "src/datahub/ingestion/source/aws"
        ep_modules = {
            "datahub.ingestion.source.aws.s3_util": EntryPointInfo(
                source_dir=aws_dir, test_path="tests/integration/s3/"
            ),
            "datahub.ingestion.source.aws.glue": EntryPointInfo(source_dir=aws_dir),
        }
        result = _narrow_ep_tests(
            aws_dir,
            ["metadata-ingestion/src/datahub/ingestion/source/aws/aws_common.py"],
            ep_modules,
        )
        # aws_common is NOT a key in ep_modules — narrowing fails
        assert result is None
