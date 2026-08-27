import json
import pathlib
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    UpstreamLineageClass,
)


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
    depends_on: Optional[Dict[str, List[str]]] = None,
) -> None:
    """Write a minimal dbt target/ directory for one project.

    depends_on maps a model name to the unique_ids it refs, for tests that need a
    real downstream edge. package_name overrides the dbt package name embedded in each model's
    unique_id (defaults to `project`), so a test can put two distinct project
    directories on a dbt package name that collides across them. Each entry in
    `models` may set "resource_type" (defaults to "model") to write a seed or
    snapshot node instead.
    """
    project_dir = root / project
    project_dir.mkdir(parents=True, exist_ok=True)
    pkg = package_name or project
    nodes: Dict[str, Any] = {}
    for model in models:
        resource_type = model.get("resource_type", "model")
        unique_id = f"{resource_type}.{pkg}.{model['name']}"
        nodes[unique_id] = {
            "unique_id": unique_id,
            "name": model["name"],
            "database": model["database"],
            "schema": model["schema"],
            "resource_type": resource_type,
            "package_name": pkg,
            "config": {"materialized": "table"},
            "description": "",
            "columns": {},
            "meta": {},
            "tags": [],
            "depends_on": {"nodes": (depends_on or {}).get(model["name"], [])},
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


@pytest.mark.parametrize("glob_mode", [True, False])
def test_manifest_path_is_a_node_field_not_a_custom_property(
    tmp_path: pathlib.Path, glob_mode: bool
) -> None:
    """manifest_path is internal provenance, used to name the originating project in
    a collision report. It must never reach customProperties: it would publish
    bucket names and prefix layout to every catalog user, and a prefix carrying a
    run id or timestamp would churn a new datasetProperties version every run."""
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    manifest_path = f"{tmp_path}/project_a/manifest.json"
    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json" if glob_mode else manifest_path
    )

    nodes = source.load_nodes()

    assert nodes[0].manifest_path == manifest_path
    assert "manifest_path" not in nodes[0].artifact_props


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


def test_object_store_not_found_code_reported_as_definite_absence(
    tmp_path: pathlib.Path,
) -> None:
    """An object-store read that reports a missing key is definite absence.

    read_file_as_bytes wraps every get_object failure in one generic ValueError,
    but preserves the underlying ClientError as __cause__, so the error code still
    distinguishes a missing key from a genuinely ambiguous failure. Without that
    split, an estate where half the projects never run `dbt docs generate` emits a
    warning per project that reads like an infrastructure fault.
    """
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )

    def fake_read(uri: str, *args: Any, **kwargs: Any) -> bytes:
        if uri.endswith("manifest.json"):
            return pathlib.Path(uri).read_bytes()
        cause = Exception("NoSuchKey")
        cause.response = {"Error": {"Code": "NoSuchKey"}}  # type: ignore[attr-defined]
        raise ValueError(f"Failed to read {uri} from object store") from cause

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    with mock.patch(
        "datahub.ingestion.source.dbt.dbt_core.read_file_as_bytes",
        side_effect=fake_read,
    ):
        source.load_nodes()

    titles = {w.title for w in source.report.warnings}
    assert "No catalog file found for project" in titles
    assert "No sources file found for project" in titles
    assert "Could not read catalog file for project" not in titles
    assert "Could not read sources file for project" not in titles


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
    # Counts colliding entities, not colliding keys.
    assert source.report.duplicate_models_detected == 2
    failures_by_title = {f.title: f for f in source.report.failures}
    assert "Duplicate model names across dbt projects" in failures_by_title


def test_cross_project_seed_collision_fails(tmp_path: pathlib.Path) -> None:
    # Same trap, but on a seed rather than a model: exists_in_target_platform is
    # true for seeds too, so two projects seeding the same relation collide on
    # the same dataset URN exactly like two models would.
    _write_project(
        tmp_path,
        "project_a",
        [
            {
                "name": "lookup",
                "database": "db",
                "schema": "shared",
                "resource_type": "seed",
            }
        ],
    )
    _write_project(
        tmp_path,
        "project_b",
        [
            {
                "name": "lookup",
                "database": "db",
                "schema": "shared",
                "resource_type": "seed",
            }
        ],
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source._check_duplicate_models(source.load_nodes())

    assert nodes == []
    assert source.report.duplicate_models_detected == 2
    failures_by_title = {f.title: f for f in source.report.failures}
    assert "Duplicate model names across dbt projects" in failures_by_title


def test_cross_project_collision_drop_mode_keeps_lowest_dbt_name(
    tmp_path: pathlib.Path,
) -> None:
    # The winning contender is deliberately NOT the first one loaded: project_a
    # sorts first by path, but its package name puts its dbt_name last. Only the
    # documented rule - lowest-sorting dbt_name - picks project_z's node, so this
    # fixture actually pins the tie-break instead of passing under any rule.
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "shared"}],
        package_name="zzz_pkg",
    )
    _write_project(
        tmp_path,
        "project_z",
        [{"name": "orders", "database": "db", "schema": "shared"}],
        package_name="aaa_pkg",
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        fail_on_duplicate_models=False,
    )
    nodes = source._check_duplicate_models(source.load_nodes())

    assert [node.dbt_name for node in nodes] == ["model.aaa_pkg.orders"]
    assert source.report.duplicate_models_detected == 2
    warnings_by_title = {w.title: w for w in source.report.warnings}
    warning = warnings_by_title["Duplicate model names across dbt projects"]
    assert any("keeping model.aaa_pkg.orders" in entry for entry in warning.context)


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
    assert source.report.duplicate_node_unique_ids_detected == 2
    failures_by_title = {f.title: f for f in source.report.failures}
    failure = failures_by_title["Duplicate dbt unique_id across projects"]
    # An operator with many colliding projects needs the actual directories, not
    # just a count - both manifests must be named so they can go fix the right ones.
    manifest_a = f"{tmp_path}/project_a/manifest.json"
    manifest_b = f"{tmp_path}/project_b/manifest.json"
    assert any(manifest_a in entry and manifest_b in entry for entry in failure.context)


def test_duplicate_unique_id_drop_mode_keeps_first_loaded(
    tmp_path: pathlib.Path,
) -> None:
    # The surviving node's own attributes deliberately sort last: project_a wins
    # only because its manifest path sorts first, so this fixture pins the
    # documented rule rather than passing under any ordering.
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch_z"}],
        package_name="shared_pkg",
    )
    _write_project(
        tmp_path,
        "project_z",
        [{"name": "orders", "database": "db", "schema": "sch_a"}],
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
    assert nodes[0].schema == "sch_z"
    assert source.report.duplicate_node_unique_ids_detected == 2
    warnings_by_title = {w.title: w for w in source.report.warnings}
    warning = warnings_by_title["Duplicate dbt unique_id across projects"]
    manifest_a = f"{tmp_path}/project_a/manifest.json"
    assert any(f"keeping {manifest_a}" in entry for entry in warning.context)


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
    assert source.report.duplicate_exposure_unique_ids_detected == 2
    failures_by_title = {f.title: f for f in source.report.failures}
    failure = failures_by_title["Duplicate dbt unique_id across projects"]
    manifest_a = f"{tmp_path}/project_a/manifest.json"
    manifest_b = f"{tmp_path}/project_b/manifest.json"
    assert any(manifest_a in entry and manifest_b in entry for entry in failure.context)


def test_duplicate_exposure_unique_id_drop_mode_keeps_first_loaded(
    tmp_path: pathlib.Path,
) -> None:
    # As above, the surviving exposure's name sorts last, so only manifest-path
    # order selects it.
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders_a", "database": "db", "schema": "sch_a"}],
        exposures={"exposure.shared_pkg.dashboard": {"name": "dashboard_zeta"}},
    )
    _write_project(
        tmp_path,
        "project_z",
        [{"name": "orders_b", "database": "db", "schema": "sch_b"}],
        exposures={"exposure.shared_pkg.dashboard": {"name": "dashboard_alpha"}},
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        fail_on_duplicate_models=False,
    )
    all_nodes = source.load_nodes()
    nodes, exposures = source._check_duplicate_unique_ids(
        all_nodes, source.load_exposures()
    )

    # Manifests are loaded in sorted path order, so project_a's exposure is
    # "first loaded" and survives.
    assert [e.name for e in exposures] == ["dashboard_zeta"]
    assert source.report.duplicate_exposure_unique_ids_detected == 2
    warnings_by_title = {w.title: w for w in source.report.warnings}
    warning = warnings_by_title["Duplicate dbt unique_id across projects"]
    manifest_a = f"{tmp_path}/project_a/manifest.json"
    assert any(f"keeping {manifest_a}" in entry for entry in warning.context)


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
    assert source.report.duplicate_node_unique_ids_detected == 0


def test_case_only_collision_detected_when_urns_are_lowercased(
    tmp_path: pathlib.Path,
) -> None:
    """Two projects whose relation names differ only in case still share one URN.

    convert_urns_to_lowercase folds the case when the URN is built, so grouping on
    the raw database.schema.name would miss this collision and let the two projects
    silently clobber each other's aspects - which is exactly what the check exists
    to prevent. Snowflake dbt projects commonly write uppercase relation names,
    which is why the option exists at all.
    """
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "ORDERS", "database": "DB", "schema": "SHARED"}],
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders", "database": "db", "schema": "shared"}],
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        convert_urns_to_lowercase=True,
    )
    all_nodes = source.load_nodes()
    # get_workunits_internal sets this flag on every node before the collision
    # check runs; mirror that here since this test calls the check directly.
    for node in all_nodes:
        node.convert_urns_to_lowercase = True

    assert source._check_duplicate_models(all_nodes) == []
    assert source.report.duplicate_models_detected == 2


def test_case_only_collision_not_reported_without_lowercasing(
    tmp_path: pathlib.Path,
) -> None:
    """Without convert_urns_to_lowercase the two relations really are distinct URNs,
    so the same fixture must not fail the run."""
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "ORDERS", "database": "DB", "schema": "SHARED"}],
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders", "database": "db", "schema": "shared"}],
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")

    assert len(source._check_duplicate_models(source.load_nodes())) == 2
    assert source.report.duplicate_models_detected == 0


def test_collision_emits_nothing_for_colliding_urn_end_to_end(
    tmp_path: pathlib.Path,
) -> None:
    """Drive a collision through the real pipeline, not just the check in isolation.

    Asserting the check's return value proves nothing about emission: the contenders
    used to survive in all_nodes_map, so downstream references still produced an
    upstreamLineage edge to a URN that was never itself emitted - materializing a
    key-only stub dataset for the very relation the projects were fighting over.
    """
    _write_project(
        tmp_path,
        "project_a",
        [
            {"name": "orders", "database": "db", "schema": "shared"},
            {"name": "orders_summary", "database": "db", "schema": "sch_a"},
        ],
        depends_on={"orders_summary": ["model.project_a.orders"]},
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders", "database": "db", "schema": "shared"}],
    )
    _write_project(
        tmp_path,
        "project_c",
        [{"name": "events", "database": "db", "schema": "sch_c"}],
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        # The default PATCH semantics require a graph, which this offline test has no
        # need for otherwise.
        write_semantics="OVERRIDE",
    )
    workunits = list(source.get_workunits())

    metadata_workunits = [wu for wu in workunits if isinstance(wu, MetadataWorkUnit)]
    described_urns = {
        wu.get_urn()
        for wu in metadata_workunits
        if wu.get_aspect_of_type(DatasetPropertiesClass) is not None
    }
    referenced_urns = {wu.get_urn() for wu in metadata_workunits} | {
        upstream.dataset
        for wu in metadata_workunits
        for lineage in [wu.get_aspect_of_type(UpstreamLineageClass)]
        if lineage is not None
        for upstream in lineage.upstreams
    }

    non_colliding_urn = make_dataset_urn_with_platform_instance(
        platform="dbt", name="db.sch_c.events", platform_instance=None, env="PROD"
    )
    assert non_colliding_urn in described_urns

    # The colliding relation must not be described, and must not be referenced at
    # all - a surviving lineage edge to it would materialize a key-only stub
    # dataset for the very relation the two projects were fighting over.
    assert not any("db.shared.orders" in urn for urn in described_urns)
    assert not any("db.shared.orders" in urn for urn in referenced_urns)
    assert source.report.failures
