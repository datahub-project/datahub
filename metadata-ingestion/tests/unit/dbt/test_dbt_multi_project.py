import json
import pathlib
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource


def _make_source(**config_overrides: Any) -> DBTCoreSource:
    config: Dict[str, Any] = {
        "manifest_path": "unused/manifest.json",
        "target_platform": "postgres",
        "enable_meta_mapping": False,
    }
    config.update(config_overrides)
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-multi-project")
    ctx.graph = None
    return DBTCoreSource(DBTCoreConfig(**config), ctx)


def test_expand_glob_path_returns_sorted_local_matches(tmp_path: pathlib.Path) -> None:
    # Created out of order on purpose: expansion must not depend on creation order.
    for name in ["c.json", "a.json", "b.json"]:
        (tmp_path / name).write_text("{}")

    source = _make_source()
    expanded = source._expand_glob_path(f"{tmp_path}/*.json")

    assert expanded == [
        f"{tmp_path}/a.json",
        f"{tmp_path}/b.json",
        f"{tmp_path}/c.json",
    ]


def test_expand_glob_path_passes_through_non_glob_path() -> None:
    source = _make_source()
    assert source._expand_glob_path("s3://bucket/project/manifest.json") == [
        "s3://bucket/project/manifest.json"
    ]


def test_expand_run_results_paths_preserves_config_order(
    tmp_path: pathlib.Path,
) -> None:
    # Two literal entries, declared newest-first. run_results files are appended
    # per node, so the caller's declared order must survive expansion.
    for name in ["run_results_a.json", "run_results_z.json"]:
        (tmp_path / name).write_text("{}")

    source = _make_source(
        run_results_paths=[
            f"{tmp_path}/run_results_z.json",
            f"{tmp_path}/run_results_a.json",
        ]
    )

    assert source._expand_run_results_paths() == [
        f"{tmp_path}/run_results_z.json",
        f"{tmp_path}/run_results_a.json",
    ]


def test_globbed_manifest_rejects_explicit_catalog_path() -> None:
    with pytest.raises(ValueError, match="catalog_path"):
        DBTCoreConfig(
            manifest_path="s3://bucket/*/manifest.json",
            catalog_path="s3://bucket/project_a/catalog.json",
            target_platform="postgres",
            aws_connection={"aws_region": "us-east-1"},
        )


def test_globbed_manifest_rejects_explicit_sources_path() -> None:
    with pytest.raises(ValueError, match="sources_path"):
        DBTCoreConfig(
            manifest_path="s3://bucket/*/manifest.json",
            sources_path="s3://bucket/project_a/sources.json",
            target_platform="postgres",
            aws_connection={"aws_region": "us-east-1"},
        )


def test_globbed_manifest_alone_is_accepted() -> None:
    config = DBTCoreConfig(
        manifest_path="s3://bucket/*/manifest.json",
        target_platform="postgres",
        aws_connection={"aws_region": "us-east-1"},
    )
    assert config.catalog_path is None


def test_non_glob_manifest_still_accepts_explicit_catalog_path() -> None:
    config = DBTCoreConfig(
        manifest_path="/tmp/project_a/manifest.json",
        catalog_path="/tmp/project_a/catalog.json",
        target_platform="postgres",
    )
    assert config.catalog_path == "/tmp/project_a/catalog.json"


def _write_project(
    root: pathlib.Path,
    project: str,
    models: List[Dict[str, str]],
    exposures: Optional[Dict[str, Dict[str, Any]]] = None,
    catalog_generated_at: Optional[str] = None,
    package_name: Optional[str] = None,
) -> None:
    """Write a minimal dbt target/ directory for one project.

    package_name overrides the dbt package name embedded in each model's
    unique_id (defaults to `project`), so a test can put two distinct project
    directories on a dbt package name that collides across them.
    """
    project_dir = root / project
    project_dir.mkdir(parents=True, exist_ok=True)
    pkg = package_name or project
    nodes: Dict[str, Any] = {}
    for model in models:
        unique_id = f"model.{pkg}.{model['name']}"
        nodes[unique_id] = {
            "unique_id": unique_id,
            "name": model["name"],
            "database": model["database"],
            "schema": model["schema"],
            "resource_type": "model",
            "package_name": pkg,
            "config": {"materialized": "table"},
            "description": "",
            "columns": {},
            "meta": {},
            "tags": [],
            "depends_on": {"nodes": []},
            "compiled": True,
            "compiled_code": "select 1 as col_a",
            "raw_code": "select 1 as col_a",
            "language": "sql",
            "original_file_path": f"models/{model['name']}.sql",
            "alias": model["name"],
            "checksum": {"name": "none", "checksum": ""},
        }
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json",
            "dbt_version": "1.8.0",
            "adapter_type": "postgres",
            "project_name": project,
            "generated_at": "2026-01-01T00:00:00.000000Z",
            "invocation_id": f"invocation-{project}",
        },
        "nodes": nodes,
        "sources": {},
        "exposures": exposures or {},
        "metrics": {},
        "macros": {},
        "child_map": {},
        "parent_map": {},
        "disabled": {},
        "semantic_models": {},
    }
    (project_dir / "manifest.json").write_text(json.dumps(manifest))

    if catalog_generated_at is not None:
        catalog = {
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
                "dbt_version": "1.8.0",
                "generated_at": catalog_generated_at,
            },
            "nodes": {},
            "sources": {},
        }
        (project_dir / "catalog.json").write_text(json.dumps(catalog))


def test_glob_fans_out_over_multiple_projects(tmp_path: pathlib.Path) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "events", "database": "db", "schema": "sch_b"}]
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source.load_nodes()

    assert {node.dbt_name for node in nodes} == {
        "model.project_a.orders",
        "model.project_b.events",
    }
    assert source.report.manifests_loaded == 2
    assert source.report.manifests_failed == 0


def test_glob_stamps_per_project_provenance(tmp_path: pathlib.Path) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")

    nodes = source.load_nodes()

    assert nodes[0].artifact_props["manifest_version"] == "1.8.0"
    assert nodes[0].artifact_props["manifest_adapter"] == "postgres"


def test_glob_stamps_semantic_model_provenance(tmp_path: pathlib.Path) -> None:
    """Task 3 review item: semantic-model nodes must carry provenance too, not just
    regular model nodes. Uses a hand-written manifest rather than _write_project,
    since folding a semantic_model entry into that helper's signature would ripple
    into Tasks 5/6, which consume _write_project as-is.
    """
    project_dir = tmp_path / "project_a"
    project_dir.mkdir(parents=True)
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json",
            "dbt_version": "1.8.0",
            "adapter_type": "postgres",
            "project_name": "project_a",
            "generated_at": "2026-01-01T00:00:00.000000Z",
            "invocation_id": "invocation-project_a",
        },
        "nodes": {},
        "sources": {},
        "exposures": {},
        "metrics": {},
        "macros": {},
        "child_map": {},
        "parent_map": {},
        "disabled": {},
        "semantic_models": {
            "semantic_model.project_a.order_metrics": {
                "name": "order_metrics",
                "description": "",
                "node_relation": {"database": "db", "schema": "sch_a"},
                "depends_on": {"nodes": []},
                "entities": [],
                "dimensions": [],
                "measures": [{"name": "count", "agg": "count", "description": ""}],
                "tags": [],
                "meta": {},
            }
        },
    }
    (project_dir / "manifest.json").write_text(json.dumps(manifest))

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source.load_nodes()

    semantic_model_nodes = [
        node for node in nodes if node.node_type == "semantic_model"
    ]
    assert len(semantic_model_nodes) == 1
    assert semantic_model_nodes[0].artifact_props["manifest_version"] == "1.8.0"
    assert semantic_model_nodes[0].artifact_props["manifest_adapter"] == "postgres"


def test_glob_missing_sibling_artifacts_warns_and_continues(
    tmp_path: pathlib.Path,
) -> None:
    """A project that never ran `dbt docs generate` has no catalog.json/sources.json.
    That must not fail the whole multi-project run, and must actually warn - a prior
    version of this test never checked report.warnings, so it would have kept passing
    even if the warning silently stopped firing."""
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source.load_nodes()

    assert {node.dbt_name for node in nodes} == {"model.project_a.orders"}
    assert source.report.manifests_loaded == 1
    assert source.report.manifests_failed == 0

    warnings_by_title = {w.title: w for w in source.report.warnings}
    assert "No catalog file found for project" in warnings_by_title
    assert "No sources file found for project" in warnings_by_title
    manifest_path = f"{tmp_path}/project_a/manifest.json"
    assert manifest_path in list(
        warnings_by_title["No catalog file found for project"].context
    )
    assert manifest_path in list(
        warnings_by_title["No sources file found for project"].context
    )


def test_glob_accumulates_exposures_across_projects(tmp_path: pathlib.Path) -> None:
    """loadManifestAndCatalog is called once per project under fan-out, and
    self._exposures is read exactly once at emit time (load_exposures), so
    overwriting it per project - instead of accumulating - would silently drop
    every project's exposures but the alphabetically-last one."""
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch_a"}],
        exposures={"exposure.project_a.dashboard_a": {"name": "dashboard_a"}},
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "events", "database": "db", "schema": "sch_b"}],
        exposures={"exposure.project_b.dashboard_b": {"name": "dashboard_b"}},
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    source.load_nodes()

    assert {e.name for e in source.load_exposures()} == {
        "dashboard_a",
        "dashboard_b",
    }


def test_glob_attributes_catalog_generated_at_per_project(
    tmp_path: pathlib.Path,
) -> None:
    """catalog_generated_at used to live on the report - one slot, so under fan-out
    the last-loaded project's timestamp silently applied to every node's dataset
    profile. It must now be attributed per-node to that node's own project."""
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch_a"}],
        catalog_generated_at="2020-01-01T00:00:00.000000Z",
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "events", "database": "db", "schema": "sch_b"}],
        catalog_generated_at="2021-06-01T00:00:00.000000Z",
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes_by_name = {node.dbt_name: node for node in source.load_nodes()}

    orders_generated_at = nodes_by_name["model.project_a.orders"].catalog_generated_at
    events_generated_at = nodes_by_name["model.project_b.events"].catalog_generated_at
    assert orders_generated_at is not None and orders_generated_at.year == 2020
    assert events_generated_at is not None and events_generated_at.year == 2021


def test_corrupt_manifest_is_a_failure_and_other_projects_still_load(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "events", "database": "db", "schema": "sch_b"}]
    )
    broken = tmp_path / "project_c"
    broken.mkdir()
    broken_manifest_path = str(broken / "manifest.json")
    (broken / "manifest.json").write_text("{ this is not valid json")

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source.load_nodes()

    assert {node.dbt_name for node in nodes} == {
        "model.project_a.orders",
        "model.project_b.events",
    }
    assert source.report.manifests_loaded == 2
    assert source.report.manifests_failed == 1
    # Must be a failure, not a warning: the stale-entity-removal handler keys on
    # report.failures to skip soft-deletion. A warning here would soft-delete
    # every dataset belonging to project_c.
    assert source.report.failures
    # An operator with 200 globbed projects needs to know which one broke. The
    # framework appends the exception detail onto the same context entry, so
    # check the path is present rather than requiring an exact-match element.
    assert any(
        broken_manifest_path in entry for entry in source.report.failures[0].context
    )


def test_non_glob_corrupt_manifest_raises_instead_of_reporting_failure(
    tmp_path: pathlib.Path,
) -> None:
    """Single-project (non-glob) mode must fail loudly, not silently swallow into
    an empty successful run - the glob-mode tolerance above must not leak into the
    historical non-glob behaviour."""
    project_dir = tmp_path / "project_a"
    project_dir.mkdir()
    manifest_path = project_dir / "manifest.json"
    manifest_path.write_text("{ this is not valid json")

    source = _make_source(manifest_path=str(manifest_path))

    with pytest.raises(json.JSONDecodeError):
        source.load_nodes()

    assert source.report.manifests_failed == 0
    assert not source.report.failures


def test_ambiguous_sibling_read_failure_does_not_assert_absence(
    tmp_path: pathlib.Path,
) -> None:
    """catalog.json/sources.json reads that fail ambiguously - as object storage
    does for a missing key, a permission error, or throttling, see
    read_file_as_bytes - must not be reported with the same "no file found"
    wording used for a definite local FileNotFoundError, and must surface the
    underlying error for diagnosis. Mocks read_file_as_bytes directly (same
    pattern as test_load_file_as_json_handles_utf8_bom in test_dbt_source.py),
    so no real S3/GCS client is needed."""
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )

    def fake_read(uri: str, *args: Any, **kwargs: Any) -> bytes:
        if uri.endswith("manifest.json"):
            return pathlib.Path(uri).read_bytes()
        # Mimics read_file_as_bytes wrapping a get_object failure - this could
        # just as easily be a missing key, throttling, or a network error.
        raise ValueError(f"Failed to read {uri} from object store: 403 Forbidden")

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    with mock.patch(
        "datahub.ingestion.source.dbt.dbt_core.read_file_as_bytes",
        side_effect=fake_read,
    ):
        nodes = source.load_nodes()

    assert {node.dbt_name for node in nodes} == {"model.project_a.orders"}

    warnings_by_title = {w.title: w for w in source.report.warnings}
    assert "Could not read catalog file for project" in warnings_by_title
    assert "Could not read sources file for project" in warnings_by_title
    # The definite-absence wording must not fire for an ambiguous read failure.
    assert "No catalog file found for project" not in warnings_by_title
    assert "No sources file found for project" not in warnings_by_title

    catalog_warning = warnings_by_title["Could not read catalog file for project"]
    sources_warning = warnings_by_title["Could not read sources file for project"]
    assert any("403 Forbidden" in entry for entry in catalog_warning.context)
    assert any("403 Forbidden" in entry for entry in sources_warning.context)


def test_cross_project_collision_fails_and_drops_all_contenders(
    tmp_path: pathlib.Path,
) -> None:
    # Both projects materialise db.shared.orders -> identical URN.
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "shared"}],
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders", "database": "db", "schema": "shared"}],
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source._check_duplicate_models(source.load_nodes())

    assert nodes == []
    assert source.report.duplicate_models_detected == 1
    assert source.report.failures


def test_cross_project_collision_drop_mode_keeps_first_by_dbt_name(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "shared"}],
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders", "database": "db", "schema": "shared"}],
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        fail_on_duplicate_models=False,
    )
    nodes = source._check_duplicate_models(source.load_nodes())

    assert [node.dbt_name for node in nodes] == ["model.project_a.orders"]
    assert source.report.duplicate_models_detected == 1


def test_distinct_schemas_do_not_collide(tmp_path: pathlib.Path) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "orders", "database": "db", "schema": "sch_b"}]
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source._check_duplicate_models(source.load_nodes())

    assert len(nodes) == 2
    assert source.report.duplicate_models_detected == 0


def test_duplicate_unique_id_across_projects_fails_and_drops_all_contenders(
    tmp_path: pathlib.Path,
) -> None:
    # Two projects scaffolded from the same template, package name never renamed:
    # both models resolve to model.shared_pkg.orders, even though they target
    # different tables (so this is not also a target-table collision).
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch_a"}],
        package_name="shared_pkg",
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders", "database": "db", "schema": "sch_b"}],
        package_name="shared_pkg",
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    all_nodes = source.load_nodes()
    nodes, exposures = source._check_duplicate_unique_ids(
        all_nodes, source.load_exposures()
    )

    assert nodes == []
    assert source.report.duplicate_unique_ids_detected == 1
    assert source.report.failures


def test_duplicate_unique_id_drop_mode_keeps_first_loaded(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch_a"}],
        package_name="shared_pkg",
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders", "database": "db", "schema": "sch_b"}],
        package_name="shared_pkg",
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        fail_on_duplicate_models=False,
    )
    all_nodes = source.load_nodes()
    nodes, exposures = source._check_duplicate_unique_ids(
        all_nodes, source.load_exposures()
    )

    # Manifests are loaded in sorted path order, so project_a is "first loaded".
    assert len(nodes) == 1
    assert nodes[0].schema == "sch_a"
    assert source.report.duplicate_unique_ids_detected == 1


def test_duplicate_exposure_unique_id_across_projects_fails(
    tmp_path: pathlib.Path,
) -> None:
    # Same collision, but on an exposure: both projects declare an exposure
    # under the same unique_id (shared package name), so DBTExposure.get_urn
    # would collide the same way DBTNode.get_urn does for models.
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders_a", "database": "db", "schema": "sch_a"}],
        exposures={"exposure.shared_pkg.dashboard": {"name": "dashboard_a"}},
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders_b", "database": "db", "schema": "sch_b"}],
        exposures={"exposure.shared_pkg.dashboard": {"name": "dashboard_b"}},
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    all_nodes = source.load_nodes()
    nodes, exposures = source._check_duplicate_unique_ids(
        all_nodes, source.load_exposures()
    )

    assert exposures == []
    assert len(nodes) == 2  # the models themselves don't collide
    assert source.report.duplicate_unique_ids_detected == 1
    assert source.report.failures


def test_distinct_package_names_do_not_collide_on_unique_id(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "orders", "database": "db", "schema": "sch_b"}]
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    all_nodes = source.load_nodes()
    nodes, exposures = source._check_duplicate_unique_ids(
        all_nodes, source.load_exposures()
    )

    assert len(nodes) == 2
    assert source.report.duplicate_unique_ids_detected == 0
