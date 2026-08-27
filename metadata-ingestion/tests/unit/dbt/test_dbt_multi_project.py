import json
import pathlib
from typing import Any, Dict, List, Optional
from unittest import mock

import dateutil.parser
import pytest

import datahub.ingestion.source.dbt.dbt_core as dbt_core_module
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    UpstreamLineageClass,
)
from datahub.utilities.time import datetime_to_ts_millis


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


def test_presigned_http_url_is_not_a_glob() -> None:
    """A '?' starts an HTTP(S) URL's query string, where presigned URLs carry their
    signature - it is not a glob metacharacter there. Treating it as one expanded the
    URL to nothing and turned a working recipe into a green run with zero assets."""
    url = "https://bucket.s3.amazonaws.com/manifest.json?X-Amz-Signature=abc"
    source = _make_source()

    assert source._expand_glob_path(url) == [url]
    assert source.report.warnings == []


def test_http_url_with_a_globbed_path_still_warns() -> None:
    """Only the URL's query string is exempt. A real pattern in the path component is
    still an unsupported glob, and must keep saying so rather than 404 later."""
    source = _make_source()

    assert source._expand_glob_path("https://host/*/manifest.json") == []
    assert [w.title for w in source.report.warnings] == [
        "Glob patterns not supported for HTTP(S) URIs"
    ]


def test_literal_local_path_containing_glob_characters_is_read_literally(
    tmp_path: pathlib.Path,
) -> None:
    """A directory may simply be named with glob metacharacters. `dbt[prod]` char-classes
    to a class that matches nothing, so expanding it silently found no manifest."""
    project_dir = tmp_path / "dbt[prod]"
    project_dir.mkdir()
    manifest_path = str(project_dir / "manifest.json")
    (project_dir / "manifest.json").write_text("{}")

    source = _make_source()

    assert source._expand_glob_path(manifest_path) == [manifest_path]
    assert source.report.warnings == []


def test_literal_path_with_glob_characters_accepts_explicit_catalog_path(
    tmp_path: pathlib.Path,
) -> None:
    """The config validator rejects catalog_path only for a real glob. A literal path
    that happens to contain those characters names exactly one manifest, so its
    catalog_path can be paired with it."""
    project_dir = tmp_path / "dbt[prod]"
    project_dir.mkdir()
    (project_dir / "manifest.json").write_text("{}")

    config = DBTCoreConfig(
        manifest_path=str(project_dir / "manifest.json"),
        catalog_path=str(project_dir / "catalog.json"),
        target_platform="postgres",
    )

    assert config.catalog_path == str(project_dir / "catalog.json")


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


def test_test_connection_expands_globbed_manifest_path(
    tmp_path: pathlib.Path,
) -> None:
    """Test Connection is an advertised capability, and must not fail a recipe that
    would ingest fine. Handing the raw glob to load_file_as_json treats the pattern
    as a literal path or object key, so a working multi-project recipe reported as
    unreachable."""
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "events", "database": "db", "schema": "sch_b"}]
    )

    report = DBTCoreSource.test_connection(
        {
            "manifest_path": f"{tmp_path}/*/manifest.json",
            "target_platform": "postgres",
        }
    )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable, report.basic_connectivity.failure_reason


def test_test_connection_reports_glob_matching_nothing(tmp_path: pathlib.Path) -> None:
    """A glob that matches nothing is a real misconfiguration and must be reported
    as one, naming the pattern - not silently pass because no read was attempted."""
    report = DBTCoreSource.test_connection(
        {
            "manifest_path": f"{tmp_path}/*/manifest.json",
            "target_platform": "postgres",
        }
    )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    failure_reason = report.basic_connectivity.failure_reason or ""
    assert "matched no files" in failure_reason
    assert f"{tmp_path}/*/manifest.json" in failure_reason


def test_test_connection_reports_object_store_failure_detail() -> None:
    """An object-store error must not be reported as "matched no files".

    Glob expansion returns an empty list both when the store refuses the request -
    bad credentials, a missing bucket, a throttled listing - and when it succeeds
    over a prefix holding no manifests. Collapsing the first into the second sends
    an operator looking for a wrong prefix instead of at their credentials."""
    with mock.patch(
        "datahub.ingestion.source.dbt.dbt_core.expand_object_store_glob",
        side_effect=ValueError("InvalidAccessKeyId: key is not valid"),
    ):
        report = DBTCoreSource.test_connection(
            {
                "manifest_path": "s3://bucket/*/manifest.json",
                "target_platform": "postgres",
                "aws_connection": {"aws_region": "us-east-1"},
            }
        )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    failure_reason = report.basic_connectivity.failure_reason or ""
    assert "InvalidAccessKeyId" in failure_reason
    # A genuine zero-match reads differently - see
    # test_test_connection_reports_glob_matching_nothing.
    assert "matched no files" not in failure_reason


def test_test_connection_non_glob_path_unchanged(tmp_path: pathlib.Path) -> None:
    """The historical single-manifest behaviour must be untouched: a good path is
    capable, a missing one is not."""
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )

    ok = DBTCoreSource.test_connection(
        {
            "manifest_path": f"{tmp_path}/project_a/manifest.json",
            "target_platform": "postgres",
        }
    )
    assert ok.basic_connectivity is not None
    assert ok.basic_connectivity.capable, ok.basic_connectivity.failure_reason

    missing = DBTCoreSource.test_connection(
        {
            "manifest_path": f"{tmp_path}/project_a/nope.json",
            "target_platform": "postgres",
        }
    )
    assert missing.basic_connectivity is not None
    assert not missing.basic_connectivity.capable


def _write_project(
    root: pathlib.Path,
    project: str,
    models: List[Dict[str, str]],
    exposures: Optional[Dict[str, Dict[str, Any]]] = None,
    catalog_generated_at: Optional[str] = None,
    package_name: Optional[str] = None,
    depends_on: Optional[Dict[str, List[str]]] = None,
    semantic_models: Optional[Dict[str, Dict[str, Any]]] = None,
    generated_at: str = "2026-01-01T00:00:00.000000Z",
) -> None:
    """Write a minimal dbt target/ directory for one project.

    depends_on maps a model name to the unique_ids it refs, for tests that need a
    real downstream edge. package_name overrides the dbt package name embedded in each model's
    unique_id (defaults to `project`), so a test can put two distinct project
    directories on a dbt package name that collides across them. Each entry in
    `models` may set "resource_type" (defaults to "model") to write a seed or
    snapshot node instead. semantic_models is written verbatim into the manifest's
    semantic_models section, and generated_at overrides the manifest's own
    generated_at (which drives Query entity timestamps).
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
            "generated_at": generated_at,
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
        "semantic_models": semantic_models or {},
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


def test_manifest_glob_matching_nothing_is_a_failure(tmp_path: pathlib.Path) -> None:
    """The manifest is the one mandatory dbt artifact, so a glob that matches none of
    them must fail rather than warn.

    The same misconfiguration already hard-fails test_connection. Warning instead
    produced a green run with zero assets, leaving mass soft-deletion to be caught
    only by the stale-entity handler's generic events-produced fail-safe, whose error
    never names the glob as the cause.
    """
    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")

    nodes = source.load_nodes()

    assert nodes == []
    failures_by_title = {f.title: f for f in source.report.failures}
    assert "manifest_path glob matched no files" in failures_by_title
    assert any(
        str(tmp_path) in entry
        for entry in failures_by_title["manifest_path glob matched no files"].context
    )


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
    """Semantic-model nodes must carry per-project artifact provenance too, not just
    regular model nodes - they are built on a separate code path from the manifest's
    semantic_models section."""
    _write_project(
        tmp_path,
        "project_a",
        [],
        semantic_models={
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
    )

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


def test_failed_project_contributes_no_exposures(tmp_path: pathlib.Path) -> None:
    """A project skipped by the per-project failure handler must contribute nothing.

    Exposures were appended to self._exposures partway through
    loadManifestAndCatalog, before semantic-model extraction ran. A project that
    failed after that point was skipped for its nodes but its exposures were
    already on self and still emitted, breaking the isolation guarantee.
    """
    for project in ["project_a", "project_b", "project_c"]:
        _write_project(
            tmp_path,
            project,
            [{"name": f"orders_{project}", "database": "db", "schema": project}],
            exposures={
                f"exposure.{project}.dashboard": {"name": f"dashboard_{project}"}
            },
            semantic_models={
                f"semantic_model.{project}.metrics": _semantic_model(
                    f"semantic_model.{project}.metrics", "metrics", "db", project
                )
            },
        )

    real_extract = dbt_core_module.extract_semantic_models

    def fail_for_project_b(
        *, manifest_semantic_models: Dict[str, Any], **kwargs: Any
    ) -> List[Any]:
        # Fails strictly after this project's exposures have been parsed, which is
        # the window the bug lived in.
        if "semantic_model.project_b.metrics" in manifest_semantic_models:
            raise RuntimeError("semantic model extraction blew up")
        return real_extract(manifest_semantic_models=manifest_semantic_models, **kwargs)

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    with mock.patch.object(
        dbt_core_module, "extract_semantic_models", side_effect=fail_for_project_b
    ):
        nodes = source.load_nodes()

    assert source.report.manifests_loaded == 2
    assert source.report.manifests_failed == 1
    assert not any(node.dbt_name.endswith("orders_project_b") for node in nodes)
    assert {e.name for e in source.load_exposures()} == {
        "dashboard_project_a",
        "dashboard_project_c",
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


def test_glob_query_timestamps_come_from_each_projects_own_manifest(
    tmp_path: pathlib.Path,
) -> None:
    """Query created/lastModified must come from the node's own manifest.

    report.manifest_info is deliberately left unset in glob mode (no single project
    may represent the whole run), so a query-timestamp path reading only that field
    fell back to now() on every glob run - churning every query aspect on every
    ingest, the same problem that moved manifest_path off customProperties.
    """
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch_a"}],
        generated_at="2020-01-01T00:00:00.000000Z",
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "events", "database": "db", "schema": "sch_b"}],
        generated_at="2021-06-01T00:00:00.000000Z",
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes_by_name = {node.dbt_name: node for node in source.load_nodes()}

    ts_a = source._get_query_timestamp(nodes_by_name["model.project_a.orders"])
    ts_b = source._get_query_timestamp(nodes_by_name["model.project_b.events"])

    assert ts_a == datetime_to_ts_millis(
        dateutil.parser.parse("2020-01-01T00:00:00.000000Z")
    )
    assert ts_b == datetime_to_ts_millis(
        dateutil.parser.parse("2021-06-01T00:00:00.000000Z")
    )
    assert ts_a != ts_b
    # The whole point: no now() fallback, so the values are stable across runs.
    assert source.report.query_timestamps_fallback_used is False


def test_non_glob_query_timestamp_still_uses_the_single_manifest(
    tmp_path: pathlib.Path,
) -> None:
    """Backward compatibility: single-project runs resolve the same timestamp they
    always did, and still never hit the now() fallback."""
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch_a"}],
        generated_at="2019-03-04T05:06:07.000000Z",
    )

    source = _make_source(manifest_path=f"{tmp_path}/project_a/manifest.json")
    nodes = source.load_nodes()

    assert source._get_query_timestamp(nodes[0]) == datetime_to_ts_millis(
        dateutil.parser.parse("2019-03-04T05:06:07.000000Z")
    )
    assert source.report.query_timestamps_fallback_used is False


def test_query_timestamp_falls_back_to_report_manifest_info(
    tmp_path: pathlib.Path,
) -> None:
    """A node with no per-node manifest timestamp (dbt Cloud builds nodes that way)
    must still resolve from the report-level manifest_info rather than now()."""
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch_a"}],
        generated_at="2018-07-08T09:10:11.000000Z",
    )

    source = _make_source(manifest_path=f"{tmp_path}/project_a/manifest.json")
    node = source.load_nodes()[0]
    node.manifest_generated_at = None

    assert source._get_query_timestamp(node) == datetime_to_ts_millis(
        dateutil.parser.parse("2018-07-08T09:10:11.000000Z")
    )
    assert source.report.query_timestamps_fallback_used is False


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


def test_local_os_error_on_sibling_catalog_only_warns(
    tmp_path: pathlib.Path,
) -> None:
    """An unreadable local sibling artifact must warn, exactly as the same condition
    on an object store does.

    A local read raises OSError subclasses that are not FileNotFoundError - here
    IsADirectoryError, in production usually PermissionError - while S3/GCS surface
    every failure as a ValueError from read_file_as_bytes. Catching only ValueError
    escalated the local case into a whole-project failure, which also suppresses
    stale-entity soft-deletion run-wide, so the same fault behaved differently
    depending only on where the artifacts live.
    """
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "events", "database": "db", "schema": "sch_b"}]
    )
    (tmp_path / "project_b" / "catalog.json").mkdir()

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source.load_nodes()

    assert {node.dbt_name for node in nodes} == {
        "model.project_a.orders",
        "model.project_b.events",
    }
    assert source.report.manifests_failed == 0
    assert source.report.failures == []

    manifest_b = f"{tmp_path}/project_b/manifest.json"
    ambiguous = [
        w
        for w in source.report.warnings
        if w.title == "Could not read catalog file for project"
        and any(manifest_b in entry for entry in w.context)
    ]
    assert len(ambiguous) == 1
    # Not described as absent: the file is there, it just cannot be read.
    assert not [
        w
        for w in source.report.warnings
        if w.title == "No catalog file found for project"
        and any(manifest_b in entry for entry in w.context)
    ]


def test_undecodable_sibling_catalog_is_corrupt_not_absent(
    tmp_path: pathlib.Path,
) -> None:
    """Invalid UTF-8 in a sibling catalog.json must not be downgraded to absence.

    UnicodeDecodeError is a ValueError subclass but not a JSONDecodeError, so a
    catalog.json that exists and is unreadable was reported as "no catalog file
    found" and the project ingested silently without any column metadata.
    """
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "events", "database": "db", "schema": "sch_b"}]
    )
    # Real bytes, not a mock: structurally valid JSON carrying one latin-1 byte, so
    # json.loads' encoding sniffing settles on UTF-8 and the decode - not the parse -
    # is what fails. (UTF-16 would not exercise this: json.detect_encoding spots it
    # and decodes it happily.)
    (tmp_path / "project_b" / "catalog.json").write_bytes(
        b'{"metadata": {"project_name": "caf\xe9"}, "nodes": {}, "sources": {}}'
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source.load_nodes()

    # project_a genuinely has no catalog.json, so that warning is expected for it -
    # what must not happen is project_b's undecodable file being described as absent
    # or as an ambiguous read failure.
    manifest_b = f"{tmp_path}/project_b/manifest.json"
    absence_warnings = [
        w
        for w in source.report.warnings
        if w.title
        in {
            "No catalog file found for project",
            "Could not read catalog file for project",
        }
        and any(manifest_b in entry for entry in w.context)
    ]
    assert absence_warnings == []

    # Per-project isolation still applies: the corrupt project is skipped as a
    # failure and the healthy one still ingests.
    assert {node.dbt_name for node in nodes} == {"model.project_a.orders"}
    assert source.report.manifests_failed == 1
    failures_by_title = {f.title: f for f in source.report.failures}
    assert "Failed to load dbt project" in failures_by_title


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


def _semantic_model(
    unique_id: str, name: str, database: str, schema: str, wraps: Optional[str] = None
) -> Dict[str, Any]:
    """One manifest semantic_models entry.

    A dbt semantic model's node_relation is the relation of the model it wraps, so
    database/schema come from that model - which is why a semantic model shares a
    get_db_fqn() with its own project's model when dbt's naming convention gives
    them the same name.
    """
    return {
        "name": name,
        "description": "",
        "node_relation": {"database": database, "schema": schema, "alias": name},
        "depends_on": {"nodes": [wraps] if wraps else []},
        "entities": [],
        "dimensions": [],
        "measures": [{"name": "count", "agg": "count", "description": ""}],
        "tags": [],
        "meta": {},
    }


def test_cross_project_semantic_model_collision_fails(tmp_path: pathlib.Path) -> None:
    """Two projects' semantic models resolving to one relation is a real collision.

    exists_in_target_platform is true for semantic models, so they receive the same
    get_db_fqn()-derived dataset URN as a model - two projects claiming it would
    silently overwrite each other's aspects exactly as two models would.
    """
    for project in ["project_a", "project_b"]:
        _write_project(
            tmp_path,
            project,
            [
                {
                    "name": f"orders_{project}",
                    "database": "db",
                    "schema": f"sch_{project}",
                }
            ],
            semantic_models={
                f"semantic_model.{project}.order_metrics": _semantic_model(
                    f"semantic_model.{project}.order_metrics",
                    "order_metrics",
                    "db",
                    "shared",
                )
            },
        )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = source._check_duplicate_models(source.load_nodes())

    assert [node.dbt_name for node in nodes] == [
        "model.project_a.orders_project_a",
        "model.project_b.orders_project_b",
    ]
    assert source.report.duplicate_models_detected == 2
    failures_by_title = {f.title: f for f in source.report.failures}
    assert "Duplicate model names across dbt projects" in failures_by_title


def test_semantic_model_aliasing_its_own_project_model_is_not_a_collision(
    tmp_path: pathlib.Path,
) -> None:
    """dbt's own naming convention must not be reported as a cross-project collision.

    dbt names a semantic model after the model it wraps (`- name: orders` on
    `model: ref('orders')`), and its node_relation is that model's relation - so the
    two share one database.schema.name inside a single project by design. Reporting
    that would hard-fail correct single-project recipes and, because report.failure
    trips the stale-entity-removal guard, suppress soft-deletion estate-wide.
    """
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch"}],
        semantic_models={
            "semantic_model.project_a.orders": _semantic_model(
                "semantic_model.project_a.orders",
                "orders",
                "db",
                "sch",
                wraps="model.project_a.orders",
            )
        },
    )

    source = _make_source(manifest_path=f"{tmp_path}/project_a/manifest.json")
    all_nodes = source.load_nodes()
    # Precondition: the two really do share a URN, so this fixture exercises the
    # exemption rather than passing because the collision never arose.
    assert len({node.get_db_fqn() for node in all_nodes}) == 1

    assert source._check_duplicate_models(all_nodes) == all_nodes
    assert source.report.duplicate_models_detected == 0
    assert not source.report.failures


def _write_colliding_projects_with_aliases(tmp_path: pathlib.Path) -> None:
    """Two projects materializing db.shared.orders, each with its own semantic alias.

    Package names are chosen so the tie-break winner (lowest-sorting dbt_name) is
    project_b's node, i.e. NOT the first manifest loaded.
    """
    for project, package in [("project_a", "zzz_pkg"), ("project_b", "aaa_pkg")]:
        _write_project(
            tmp_path,
            project,
            [{"name": "orders", "database": "db", "schema": "shared"}],
            package_name=package,
            semantic_models={
                f"semantic_model.{package}.orders": _semantic_model(
                    f"semantic_model.{package}.orders",
                    "orders",
                    "db",
                    "shared",
                    wraps=f"model.{package}.orders",
                )
            },
        )


def test_contested_urn_also_drops_exempted_semantic_aliases(
    tmp_path: pathlib.Path,
) -> None:
    """A same-project alias is exempt from *deciding* a collision, not from its outcome.

    Both projects' aliases resolve to the contested URN too, so leaving them in
    place would emit to the very URN the models were dropped over - performing the
    overwrite this check exists to prevent, while the report claimed nothing was
    emitted."""
    _write_colliding_projects_with_aliases(tmp_path)

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    all_nodes = source.load_nodes()
    # Precondition: all four nodes really do share one URN.
    assert len({node.get_db_fqn() for node in all_nodes}) == 1
    assert len(all_nodes) == 4

    assert source._check_duplicate_models(all_nodes) == []
    assert source.report.duplicate_models_detected == 4
    failures_by_title = {f.title: f for f in source.report.failures}
    assert "Duplicate model names across dbt projects" in failures_by_title


def test_contested_urn_drop_mode_keeps_only_the_winning_manifest(
    tmp_path: pathlib.Path,
) -> None:
    """Keep-first keeps one project, not one node.

    The winner's own alias is legitimate aliasing of the node that won the URN, so
    it stays; the losing project's model and alias both go, since either would
    overwrite the survivor on the shared URN."""
    _write_colliding_projects_with_aliases(tmp_path)

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        fail_on_duplicate_models=False,
    )
    kept = source._check_duplicate_models(source.load_nodes())

    assert sorted(node.dbt_name for node in kept) == [
        "model.aaa_pkg.orders",
        "semantic_model.aaa_pkg.orders",
    ]
    # Every survivor comes from the winner's manifest.
    assert {node.manifest_path for node in kept} == {
        f"{tmp_path}/project_b/manifest.json"
    }
    assert source.report.duplicate_models_detected == 4


def _write_two_semantic_models(
    tmp_path: pathlib.Path, models: List[Dict[str, str]]
) -> None:
    """One project whose two packages both declare a semantic model named `orders`.

    Semantic-model names are unique within a package, so two of them can only land
    on one URN when they come from different packages of the same manifest - an
    installed package and the root project, say. Both resolve to db.shared.orders.
    """
    _write_project(
        tmp_path,
        "project_a",
        models,
        semantic_models={
            f"semantic_model.{package}.orders": _semantic_model(
                f"semantic_model.{package}.orders", "orders", "db", "shared"
            )
            for package in ["pkg_a", "pkg_b"]
        },
    )


def test_two_semantic_models_without_a_wrapped_model_collide(
    tmp_path: pathlib.Path,
) -> None:
    """Two semantic models must not exempt each other.

    The aliasing exemption exists because a semantic model wraps a *model*. With no
    non-semantic node on the URN, neither of these is aliasing anything - they would
    simply overwrite each other's aspects, which is a collision."""
    _write_two_semantic_models(
        tmp_path, [{"name": "other", "database": "db", "schema": "shared"}]
    )

    source = _make_source(manifest_path=f"{tmp_path}/project_a/manifest.json")
    nodes = source._check_duplicate_models(source.load_nodes())

    assert [node.dbt_name for node in nodes] == ["model.project_a.other"]
    assert source.report.duplicate_models_detected == 2
    failures_by_title = {f.title: f for f in source.report.failures}
    assert "Duplicate model names across dbt projects" in failures_by_title


def test_two_semantic_models_wrapping_one_model_are_benign(
    tmp_path: pathlib.Path,
) -> None:
    """Both are aliases of the same non-semantic node, so the URN is not contested.

    Requiring the same-manifest sibling to be non-semantic must not turn a model
    plus two semantic models over it into a reported collision."""
    _write_two_semantic_models(
        tmp_path, [{"name": "orders", "database": "db", "schema": "shared"}]
    )

    source = _make_source(manifest_path=f"{tmp_path}/project_a/manifest.json")
    all_nodes = source.load_nodes()
    # Precondition: all three share one URN, so the exemption is what makes this pass.
    assert len({node.get_db_fqn() for node in all_nodes}) == 1
    assert len(all_nodes) == 3

    assert source._check_duplicate_models(all_nodes) == all_nodes
    assert source.report.duplicate_models_detected == 0
    assert not source.report.failures


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


def test_drop_mode_rewires_exposure_depends_on_to_the_survivor(
    tmp_path: pathlib.Path,
) -> None:
    """An exposure depending on a dropped contender must follow the survivor.

    keep-first mode rewired node.upstream_nodes but not DBTExposure.depends_on,
    which holds the same dbt_name keys and is resolved the same way to build
    exposure lineage - so the exposure silently lost the edge instead of pointing
    at the retained node, whose URN is identical.
    """
    # project_a's model loses the tie-break (its package name sorts last), and
    # project_a is also where the exposure lives - so the exposure's dependency is
    # exactly the dropped contender.
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "shared"}],
        package_name="zzz_pkg",
        exposures={
            "exposure.zzz_pkg.dashboard": {
                "name": "dashboard_a",
                "depends_on": {"nodes": ["model.zzz_pkg.orders"]},
            }
        },
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
    all_nodes = source.load_nodes()
    exposure = next(
        e
        for e in source.load_exposures()
        if e.unique_id == "exposure.zzz_pkg.dashboard"
    )
    # Precondition: the exposure points at the contender that is about to be dropped.
    assert exposure.depends_on == ["model.zzz_pkg.orders"]

    kept = source._check_duplicate_models(all_nodes)

    assert [node.dbt_name for node in kept] == ["model.aaa_pkg.orders"]
    assert exposure.depends_on == ["model.aaa_pkg.orders"]


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


def test_duplicate_unique_id_drop_mode_keeps_the_survivors_run_results(
    tmp_path: pathlib.Path,
) -> None:
    """The retained contender must keep the run results attributed to the collision.

    load_run_results runs inside load_nodes, before this pass, and resolves results
    through its own local node map - which collapses colliding unique_ids last-wins.
    So results land on whichever contender loaded last, and keep-first mode retains
    the one that loaded first: without merging, the survivor silently loses its test
    results and model performance metadata.
    """
    # project_a sorts first by path, so it is the "first loaded" survivor, while
    # load_run_results' own last-wins map attaches the result to project_z's node.
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
    run_results_path = tmp_path / "run_results.json"
    run_results_path.write_text(
        json.dumps(
            {
                "metadata": {
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
                    "dbt_version": "1.8.0",
                    "generated_at": "2026-01-02T00:00:00.000000Z",
                    "invocation_id": "invocation-run",
                },
                "results": [
                    {
                        "status": "success",
                        "unique_id": "model.shared_pkg.orders",
                        "timing": [
                            {
                                "name": "execute",
                                "started_at": "2026-01-02T00:00:00.000000Z",
                                "completed_at": "2026-01-02T00:00:05.000000Z",
                            }
                        ],
                    }
                ],
            }
        )
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        run_results_paths=[str(run_results_path)],
        fail_on_duplicate_models=False,
    )
    all_nodes = source.load_nodes()

    # Precondition: the result really did land on the contender about to be dropped.
    by_schema = {node.schema: node for node in all_nodes}
    assert len(by_schema["sch_a"].model_performances) == 1
    assert by_schema["sch_z"].model_performances == []

    nodes, _ = source._check_duplicate_unique_ids(all_nodes, source.load_exposures())

    assert len(nodes) == 1
    assert nodes[0].schema == "sch_z"  # project_a survives (first loaded)
    assert len(nodes[0].model_performances) == 1
    assert nodes[0].model_performances[0].run_id == "invocation-run"


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


def test_collision_between_excluded_nodes_does_not_fail_the_run(
    tmp_path: pathlib.Path,
) -> None:
    """The class-1 model-URN collision stays restricted to _is_allowed_node: its harm
    is aspect clobber on the contenders' own shared URN, so excluding both of them
    from emission genuinely removes the harm.

    The two projects use distinct package names so they don't also collide on
    unique_id - that class of collision is a different pass with a different rule,
    since its harm is not confined to the contenders (see
    test_node_pass_detects_collision_even_when_one_contender_is_excluded).
    """
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "shared"}],
        package_name="excluded_pkg_a",
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders", "database": "db", "schema": "shared"}],
        package_name="excluded_pkg_b",
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        node_name_pattern={
            "deny": ["model.excluded_pkg_a..*", "model.excluded_pkg_b..*"]
        },
    )
    all_nodes = source.load_nodes()
    nodes, _ = source._check_duplicate_unique_ids(all_nodes, source.load_exposures())

    assert source._check_duplicate_models(nodes) == nodes
    assert not source.report.failures
    assert source.report.duplicate_models_detected == 0
    assert source.report.duplicate_node_unique_ids_detected == 0


def test_node_pass_detects_collision_even_when_one_contender_is_excluded(
    tmp_path: pathlib.Path,
) -> None:
    """The class-2 unique_id collision pass must NOT restrict to _is_allowed_node,
    unlike its class-1 and exposure siblings.

    node_name_pattern can't discriminate these two contenders - they share a
    dbt_name by construction, so it would deny (or allow) both identically.
    materialized_node_pattern can, because it keys on database/schema/name, which
    differ across the two projects. Excluding project_a here leaves project_b
    emitted; all_nodes_map is built from the unfiltered node list, so without this
    pass project_b's dbt_name would resolve to whichever of the two loaded last,
    silently corrupting lineage for a node nobody excluded.
    """
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db_a", "schema": "sch"}],
        package_name="shared_pkg",
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "orders", "database": "db_b", "schema": "sch"}],
        package_name="shared_pkg",
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        materialized_node_pattern={"database_pattern": {"deny": ["db_a"]}},
    )
    # Loaded once: load_nodes re-reads every manifest, extends self._exposures, and
    # increments report.manifests_loaded, so a second call would double all three.
    all_nodes = source.load_nodes()
    assert not source._is_allowed_node(
        next(node for node in all_nodes if node.database == "db_a")
    )

    nodes, _ = source._check_duplicate_unique_ids(all_nodes, source.load_exposures())

    assert nodes == []
    assert source.report.duplicate_node_unique_ids_detected == 2
    failures_by_title = {f.title: f for f in source.report.failures}
    assert "Duplicate dbt unique_id across projects" in failures_by_title


def test_exposure_collision_ignored_when_exposures_are_disabled(
    tmp_path: pathlib.Path,
) -> None:
    """Same guarantee for exposures, which are gated by their own config switch."""
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

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        entities_enabled={"exposures": "NO"},
    )
    all_nodes = source.load_nodes()
    _, exposures = source._check_duplicate_unique_ids(
        all_nodes, source.load_exposures()
    )

    assert len(exposures) == 2
    assert not source.report.failures
    assert source.report.duplicate_exposure_unique_ids_detected is None
