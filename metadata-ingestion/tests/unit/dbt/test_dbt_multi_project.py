import json
import pathlib
import threading
from typing import Any, Dict, List, NoReturn, Optional, Set
from unittest import mock

import dateutil.parser
import pytest

import datahub.ingestion.source.dbt.dbt_artifacts as dbt_artifacts_module
import datahub.ingestion.source.dbt.dbt_core as dbt_core_module
from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.object_store_files import ObjectNotFoundError
from datahub.ingestion.source.dbt.dbt_common import (
    DBTMetricsParse,
    DBTNode,
    DBTProject,
    _query_id_prefix,
)
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.ingestion.source.dbt.dbt_tests import DBTTest
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


def _load_projects(source: DBTCoreSource) -> List[DBTProject]:
    return list(source.load_projects())


def _load_nodes(source: DBTCoreSource) -> List[DBTNode]:
    return [node for project in source.load_projects() for node in project.nodes]


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


def test_presigned_http_url_is_not_a_glob() -> None:
    """A '?' in an HTTP(S) URL starts the query string (where presigned URLs carry
    their signature), so the URL is passed through unexpanded."""
    url = "https://bucket.s3.amazonaws.com/manifest.json?X-Amz-Signature=abc"
    source = _make_source()

    assert source._expand_glob_path(url) == [url]
    assert source.report.warnings == []


def test_http_url_with_a_globbed_path_still_warns() -> None:
    """Only the query string is exempt: a pattern in the URL's path is an
    unsupported glob and warns."""
    source = _make_source()

    assert source._expand_glob_path("https://host/*/manifest.json") == []
    (warning,) = source.report.warnings
    assert "https://host/*/manifest.json" in " ".join(warning.context)


def test_http_glob_warning_keeps_the_query_string_out_of_the_report() -> None:
    """A presigned URL carries its signature in the query string; the warning names
    the URL without it so the report never records a credential."""
    source = _make_source()

    source._expand_glob_path("https://host/*/manifest.json?X-Amz-Signature=secret")

    (warning,) = source.report.warnings
    assert "https://host/*/manifest.json" in " ".join(warning.context)
    assert "secret" not in " ".join(warning.context)


def test_http_glob_matching_nothing_keeps_the_query_string_out_of_the_failure() -> None:
    source = _make_source(
        manifest_path="https://host/*/manifest.json?X-Amz-Signature=secret"
    )

    assert _load_projects(source) == []
    assert source.report.failures
    for entry in [*source.report.failures, *source.report.warnings]:
        assert "secret" not in " ".join(entry.context)


def test_refused_cloud_listing_is_reported_once(tmp_path: pathlib.Path) -> None:
    """A listing the store refuses (bad credentials, missing bucket, throttling) is
    reported by the expander; the loader must not add a second "matched no files"
    failure that points the operator at the wrong cause."""
    source = _make_source(
        manifest_path="s3://bucket/*/manifest.json",
        aws_connection={"aws_region": "us-east-1"},
    )

    with mock.patch.object(
        dbt_artifacts_module,
        "expand_object_store_glob",
        side_effect=ValueError("InvalidAccessKeyId: key is not valid"),
    ):
        projects = _load_projects(source)

    assert projects == []
    (failure,) = source.report.failures
    assert "InvalidAccessKeyId" in " ".join(failure.context)


def test_escaped_glob_character_matches_a_literal_directory(
    tmp_path: pathlib.Path,
) -> None:
    project_dir = tmp_path / "dbt[prod]"
    project_dir.mkdir()
    (project_dir / "manifest.json").write_text("{}")

    source = _make_source()

    assert source._expand_glob_path(f"{tmp_path}/dbt[[]prod]/manifest.json") == [
        str(project_dir / "manifest.json")
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


@pytest.mark.parametrize(
    "manifest_path, sibling, accepted",
    [
        (
            "s3://bucket/*/manifest.json",
            {"catalog_path": "s3://bucket/project_a/catalog.json"},
            False,
        ),
        (
            "s3://bucket/*/manifest.json",
            {"sources_path": "s3://bucket/project_a/sources.json"},
            False,
        ),
        ("s3://bucket/*/manifest.json", {"platform_instance": "shared"}, False),
        (
            "s3://bucket/*/manifest.json",
            {"semantic_model_project_name": "pinned"},
            False,
        ),
        (
            "s3://bucket/*/manifest.json",
            {"git_info": {"repo": "github.com/org/repo"}},
            False,
        ),
        ("s3://bucket/*/manifest.json", {}, True),
        ("s3://bucket/project_a/manifest.json", {"platform_instance": "shared"}, True),
        (
            "s3://bucket/project_a/manifest.json",
            {"semantic_model_project_name": "pinned"},
            True,
        ),
        (
            "/data/project_a/manifest.json",
            {"catalog_path": "/data/project_a/catalog.json"},
            True,
        ),
    ],
)
def test_explicit_sibling_paths_are_rejected_only_alongside_a_glob(
    manifest_path: str, sibling: Dict[str, Any], accepted: bool
) -> None:
    """A globbed manifest_path reads catalog.json/sources.json from each match's own
    directory, so an explicit sibling path alongside it is a config error, while a
    literal manifest_path still pairs with one."""

    def build() -> DBTCoreConfig:
        return DBTCoreConfig(
            manifest_path=manifest_path,
            target_platform="postgres",
            aws_connection=(
                {"aws_region": "us-east-1"}
                if manifest_path.startswith("s3://")
                else None
            ),
            **sibling,
        )

    if accepted:
        assert build().catalog_path == sibling.get("catalog_path")
    else:
        with pytest.raises(ValueError, match=next(iter(sibling))):
            build()


def test_test_connection_expands_globbed_manifest_path(
    tmp_path: pathlib.Path,
) -> None:
    """Test Connection expands a globbed manifest_path instead of reading the
    pattern as a literal path, so a recipe that ingests fine also connects."""
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
        "datahub.ingestion.source.dbt.dbt_artifacts.expand_object_store_glob",
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
    project_name: Optional[str] = None,
) -> None:
    """Write a minimal dbt target/ directory for one project.

    depends_on maps a model name to the unique_ids it refs, for tests that need a
    real downstream edge. package_name overrides the dbt package name embedded in each model's
    unique_id (defaults to `project`), so a test can put two distinct project
    directories on a dbt package name that collides across them. Each entry in
    `models` may set "resource_type" (defaults to "model") to write a seed or
    snapshot node instead. semantic_models is written verbatim into the manifest's
    semantic_models section, and generated_at overrides the manifest's own
    generated_at (which drives Query entity timestamps). project_name overrides
    metadata.project_name (defaults to `project`), and a model may set
    "package_name" to place it in an installed package.
    """
    project_dir = root / project
    project_dir.mkdir(parents=True, exist_ok=True)
    pkg = package_name or project
    nodes: Dict[str, Any] = {}
    for model in models:
        resource_type = model.get("resource_type", "model")
        pkg_for_model = model.get("package_name", pkg)
        unique_id = f"{resource_type}.{pkg_for_model}.{model['name']}"
        nodes[unique_id] = {
            "unique_id": unique_id,
            "name": model["name"],
            "database": model["database"],
            "schema": model["schema"],
            "resource_type": resource_type,
            "package_name": pkg_for_model,
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
            "project_name": project_name or project,
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


def _write_run_results(path: pathlib.Path, unique_id: str, invocation_id: str) -> None:
    """Write a minimal run_results.json with one successful execution of unique_id."""
    path.write_text(
        json.dumps(
            {
                "metadata": {
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json",
                    "dbt_version": "1.8.0",
                    "generated_at": "2026-01-02T00:00:00.000000Z",
                    "invocation_id": invocation_id,
                },
                "results": [
                    {
                        "status": "success",
                        "unique_id": unique_id,
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


def test_glob_fans_out_over_multiple_projects(tmp_path: pathlib.Path) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "events", "database": "db", "schema": "sch_b"}]
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = _load_nodes(source)

    assert {node.dbt_name for node in nodes} == {
        "model.project_a.orders",
        "model.project_b.events",
    }
    assert source.report.manifests_loaded == 2
    assert source.report.manifests_failed == 0


def test_manifest_glob_matching_nothing_is_a_failure(tmp_path: pathlib.Path) -> None:
    """The manifest is the one mandatory dbt artifact, so a glob matching none is a
    failure naming the pattern, which also keeps stale-entity removal from running."""
    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")

    nodes = _load_nodes(source)

    assert nodes == []
    assert any(
        str(tmp_path) in entry for f in source.report.failures for entry in f.context
    )


@pytest.mark.parametrize("glob_mode", [True, False])
def test_manifest_path_is_a_project_field_not_a_custom_property(
    tmp_path: pathlib.Path, glob_mode: bool
) -> None:
    """manifest_path is internal provenance, names the other manifest in the
    duplicate-project_name failure. It must never reach customProperties: it would publish
    bucket names and prefix layout to every catalog user, and a prefix carrying a
    run id or timestamp would churn a new datasetProperties version every run."""
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    manifest_path = f"{tmp_path}/project_a/manifest.json"
    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json" if glob_mode else manifest_path,
        write_semantics="OVERRIDE",
    )

    project = _load_projects(source)[0]

    assert project.manifest_path == manifest_path
    assert "manifest_path" not in project.artifact_props

    properties = [
        wu.get_aspect_of_type(DatasetPropertiesClass)
        for wu in source.get_workunits()
        if isinstance(wu, MetadataWorkUnit)
        and wu.get_aspect_of_type(DatasetPropertiesClass) is not None
    ]
    assert properties
    assert all(
        p is not None and "manifest_path" not in p.customProperties for p in properties
    )


def test_glob_records_per_project_provenance(
    tmp_path: pathlib.Path,
) -> None:
    """Each project records its own artifact provenance. Semantic models are built on
    a separate code path from the manifest's semantic_models section, so they are
    checked alongside a regular model."""
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "sch_a"}],
        semantic_models={
            "semantic_model.project_a.order_metrics": _semantic_model(
                "semantic_model.project_a.order_metrics", "order_metrics", "db", "sch_a"
            )
        },
    )
    projects = _load_projects(_make_source(manifest_path=f"{tmp_path}/*/manifest.json"))

    assert {node.node_type for p in projects for node in p.nodes} == {
        "model",
        "semantic_model",
    }
    for project in projects:
        assert project.artifact_props["manifest_version"] == "1.8.0"
        assert project.artifact_props["manifest_adapter"] == "postgres"


def test_glob_missing_sibling_artifacts_warns_and_continues(
    tmp_path: pathlib.Path,
) -> None:
    """A project with no catalog.json/sources.json still loads, with one warning per
    missing file naming the project's manifest."""
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    nodes = _load_nodes(source)

    assert {node.dbt_name for node in nodes} == {"model.project_a.orders"}
    assert source.report.manifests_loaded == 1
    assert source.report.manifests_failed == 0

    manifest_path = f"{tmp_path}/project_a/manifest.json"
    assert (
        len([w for w in source.report.warnings if manifest_path in list(w.context)])
        == 2
    )


def test_glob_accumulates_exposures_across_projects(tmp_path: pathlib.Path) -> None:
    """Each project emits its own exposures, so the run-level counter must
    accumulate across projects and each exposure urn must carry its own
    project's platform instance."""
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

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json", write_semantics="OVERRIDE"
    )
    workunits = list(source.get_workunits())

    assert source.report.num_exposures_emitted == 2
    dashboard_urns = {
        wu.get_urn()
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit)
        and wu.get_urn().startswith("urn:li:dashboard:")
    }
    # The instance prefix, not just the unique_id, which names the project anyway.
    assert any("(dbt,project_a.exposure." in urn for urn in dashboard_urns)
    assert any("(dbt,project_b.exposure." in urn for urn in dashboard_urns)


def test_failed_project_contributes_no_exposures(tmp_path: pathlib.Path) -> None:
    """A project that fails late in its load (after its exposures were parsed)
    contributes no exposures, only its neighbours do."""
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
        # Fails strictly after this project's exposures have been parsed.
        if "semantic_model.project_b.metrics" in manifest_semantic_models:
            raise RuntimeError("semantic model extraction blew up")
        return real_extract(manifest_semantic_models=manifest_semantic_models, **kwargs)

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    with mock.patch.object(
        dbt_core_module, "extract_semantic_models", side_effect=fail_for_project_b
    ):
        projects = _load_projects(source)

    nodes = [node for project in projects for node in project.nodes]
    assert source.report.manifests_loaded == 2
    assert source.report.manifests_failed == 1
    assert not any(node.dbt_name.endswith("orders_project_b") for node in nodes)
    assert {e.name for project in projects for e in project.exposures} == {
        "dashboard_project_a",
        "dashboard_project_c",
    }


def test_glob_attributes_catalog_generated_at_per_project(
    tmp_path: pathlib.Path,
) -> None:
    """Each project carries its own catalog's generated_at."""
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
    projects_by_name = {p.project_name: p for p in _load_projects(source)}

    orders_generated_at = projects_by_name["project_a"].catalog_generated_at
    events_generated_at = projects_by_name["project_b"].catalog_generated_at
    assert orders_generated_at is not None and orders_generated_at.year == 2020
    assert events_generated_at is not None and events_generated_at.year == 2021


def test_glob_query_timestamps_come_from_each_projects_own_manifest(
    tmp_path: pathlib.Path,
) -> None:
    """Query created/lastModified come from the node's own manifest, not now(), so
    Query aspects stay stable across runs in glob mode (where report.manifest_info
    is unset)."""
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
    timestamps = {}
    for project in _load_projects(source):
        source._current_project = project
        timestamps[project.project_name] = source._get_query_timestamp()
    source._current_project = None
    ts_a = timestamps["project_a"]
    ts_b = timestamps["project_b"]

    assert ts_a == datetime_to_ts_millis(
        dateutil.parser.parse("2020-01-01T00:00:00.000000Z")
    )
    assert ts_b == datetime_to_ts_millis(
        dateutil.parser.parse("2021-06-01T00:00:00.000000Z")
    )
    assert ts_a != ts_b
    # The whole point: no now() fallback, so the values are stable across runs.
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
    _load_nodes(source)
    source._current_project = _project(manifest_generated_at=None)

    assert source._get_query_timestamp() == datetime_to_ts_millis(
        dateutil.parser.parse("2018-07-08T09:10:11.000000Z")
    )
    assert source.report.query_timestamps_fallback_used is False


def test_unparseable_manifest_timestamps_share_one_fallback(
    tmp_path: pathlib.Path,
) -> None:
    """Nodes whose manifest generated_at cannot be parsed all get one now() for the
    run and the report flags the fallback, so Query aspects stay mutually consistent
    within a run instead of each node churning its own timestamp."""
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "a", "database": "db", "schema": "sch_a"}],
        generated_at="not-a-timestamp",
    )
    _write_project(
        tmp_path,
        "project_b",
        [{"name": "b", "database": "db", "schema": "sch_b"}],
        generated_at="also-not-a-timestamp",
    )
    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    timestamps: Set[int] = set()
    for project in _load_projects(source):
        source._current_project = project
        timestamps.update(source._get_query_timestamp() for _ in project.nodes)
    source._current_project = None

    assert len(timestamps) == 1
    assert source.report.query_timestamps_fallback_used is True


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
    nodes = _load_nodes(source)

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


def test_corrupt_run_results_costs_only_its_own_results(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "orders", "database": "db", "schema": "b"}]
    )
    _write_run_results(
        tmp_path / "project_a" / "run_results.json", "model.project_a.orders", "inv-a"
    )
    corrupt = tmp_path / "project_b" / "run_results.json"
    corrupt.write_text("{not json")

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        run_results_paths=[f"{tmp_path}/*/run_results.json"],
    )
    projects = _load_projects(source)

    assert [p.project_name for p in projects] == ["project_a", "project_b"]
    assert source.report.manifests_failed == 0
    assert any(str(corrupt) in w.context[0] for w in source.report.warnings)


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
        _load_nodes(source)

    assert source.report.manifests_failed == 0
    assert not source.report.failures


def test_non_glob_missing_explicit_catalog_path_still_raises(
    tmp_path: pathlib.Path,
) -> None:
    """Only glob-derived sibling guesses are optional. An explicitly configured
    catalog_path that cannot be read is a misconfiguration, and single-project mode
    must keep failing on it: degraded to a warning, a typo in catalog_path would
    silently produce a run with no schema metadata."""
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch_a"}]
    )
    source = _make_source(
        manifest_path=f"{tmp_path}/project_a/manifest.json",
        catalog_path=f"{tmp_path}/project_a/nope.json",
    )
    with pytest.raises(FileNotFoundError):
        _load_nodes(source)


def test_memory_error_propagates_instead_of_being_skipped(
    tmp_path: pathlib.Path,
) -> None:
    """A MemoryError is not contained by skipping the project: it stops the run at
    the first project instead of being recorded as one failed project."""
    for project in ("project_a", "project_b", "project_c"):
        _write_project(
            tmp_path, project, [{"name": project, "database": "db", "schema": "sch"}]
        )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    attempted: List[str] = []

    def _raise_memory_error(
        manifest_path: str, *args: object, **kwargs: object
    ) -> NoReturn:
        attempted.append(manifest_path)
        raise MemoryError("catalog too large to parse")

    with mock.patch.object(source, "_load_project", _raise_memory_error):
        with pytest.raises(MemoryError):
            _load_nodes(source)

    # Stopped at the first project instead of fetching the other two.
    assert len(attempted) == 1
    assert source.report.manifests_failed == 0
    assert not source.report.failures


def test_object_store_glob_fans_out_over_uri_matches(tmp_path: pathlib.Path) -> None:
    """Drive the advertised s3:// shape through expansion, sibling URIs and prefetch.

    Every other fan-out test uses local paths. Here glob expansion returns two
    object-store manifests and read_file_as_bytes serves bytes by URI, pinning that
    sibling artifacts are derived as URIs beside each manifest, that the prefetch
    pool rather than the main thread performs every read (ArtifactReader.load_json
    silently falls back to a direct read for a URI that was not prefetched, so a
    node-set comparison alone cannot tell), and that a missing key is definite
    absence, which warns and keeps the project.
    """
    projects = ["project_a", "project_b"]
    for name in projects:
        _write_project(
            tmp_path,
            name,
            [{"name": f"m_{name}", "database": "db", "schema": name}],
            catalog_generated_at="2026-01-01T00:00:00.000000Z",
        )
    objects = {
        f"s3://bucket/{name}/{artifact}": (tmp_path / name / artifact).read_bytes()
        for name in projects
        for artifact in ["manifest.json", "catalog.json"]
    }
    del objects["s3://bucket/project_b/catalog.json"]  # never ran `dbt docs generate`
    requested: List[str] = []
    reader_threads: Set[str] = set()

    def fake_read(uri: str, *args: Any, **kwargs: Any) -> bytes:
        requested.append(uri)
        reader_threads.add(threading.current_thread().name)
        if uri not in objects:
            raise ObjectNotFoundError(uri)
        return objects[uri]

    source = _make_source(
        manifest_path="s3://bucket/*/manifest.json",
        aws_connection={"aws_region": "us-east-1"},
    )
    with (
        mock.patch.object(
            dbt_artifacts_module,
            "expand_object_store_glob",
            return_value=[
                "s3://bucket/project_b/manifest.json",
                "s3://bucket/project_a/manifest.json",
            ],
        ),
        mock.patch.object(
            dbt_artifacts_module, "read_file_as_bytes", side_effect=fake_read
        ),
    ):
        nodes = _load_nodes(source)

    assert {node.dbt_name for node in nodes} == {
        "model.project_a.m_project_a",
        "model.project_b.m_project_b",
    }
    assert list(source.report.manifest_paths_expanded) == [
        "s3://bucket/project_a/manifest.json",
        "s3://bucket/project_b/manifest.json",
    ]
    assert "s3://bucket/project_a/catalog.json" in requested
    assert "s3://bucket/project_b/sources.json" in requested
    assert "MainThread" not in reader_threads
    assert source.report.failures == []
    # The missing key is reported as absence for that project, which still loads.
    assert any(
        "s3://bucket/project_b/manifest.json" in " ".join(w.context)
        for w in source.report.warnings
    )


@pytest.mark.parametrize("artifact", ["catalog.json", "sources.json"])
def test_ambiguous_sibling_read_failure_skips_the_project(
    tmp_path: pathlib.Path, artifact: str
) -> None:
    """A read failure that does not prove absence (permissions, throttling) fails
    the project: as "no catalog" it would overwrite schemas with manifest-only
    columns, and the failure keeps stale-entity removal from deleting anything."""
    for name in ["project_a", "project_b"]:
        _write_project(
            tmp_path, name, [{"name": f"m_{name}", "database": "db", "schema": name}]
        )
    unreadable = f"{tmp_path}/project_b/{artifact}"

    def fake_read(uri: str, *args: Any, **kwargs: Any) -> bytes:
        if uri == unreadable:
            raise ValueError(f"Failed to read {uri} from object store: 403 Forbidden")
        return pathlib.Path(uri).read_bytes()

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    with mock.patch.object(
        dbt_artifacts_module, "read_file_as_bytes", side_effect=fake_read
    ):
        projects = _load_projects(source)

    assert [p.project_name for p in projects] == ["project_a"]
    assert list(source.report.manifest_paths_failed) == [
        f"{tmp_path}/project_b/manifest.json"
    ]


def test_missing_catalog_with_only_include_if_in_catalog_skips_the_project(
    tmp_path: pathlib.Path,
) -> None:
    """Every node would be filtered out, and with only a warning the project's
    entities would be soft-deleted."""
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "m_a", "database": "db", "schema": "a"}],
        catalog_generated_at="2026-01-01T00:00:00.000000Z",
    )
    _write_project(
        tmp_path, "project_b", [{"name": "m_b", "database": "db", "schema": "b"}]
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json", only_include_if_in_catalog=True
    )
    projects = _load_projects(source)

    assert [p.project_name for p in projects] == ["project_a"]
    assert source.report.manifests_failed == 1


def test_undecodable_sibling_catalog_is_corrupt_not_absent(
    tmp_path: pathlib.Path,
) -> None:
    """A sibling catalog.json with invalid UTF-8 fails its project rather than being
    reported as absent."""
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
    nodes = _load_nodes(source)

    # project_a genuinely has no catalog.json and warns; project_b must not.
    manifest_b = f"{tmp_path}/project_b/manifest.json"
    assert not [
        w
        for w in source.report.warnings
        if any(manifest_b in entry for entry in w.context)
    ]

    assert {node.dbt_name for node in nodes} == {"model.project_a.orders"}
    assert list(source.report.manifest_paths_failed) == [manifest_b]


def _semantic_model(
    unique_id: str, name: str, database: str, schema: str, wraps: Optional[str] = None
) -> Dict[str, Any]:
    """One manifest semantic_models entry.

    A dbt semantic model's node_relation is the relation of the model it wraps, so
    database/schema come from that model - so this helper writes a semantic model wrapping
    the named model. It gets its own dataset urn (get_db_fqn() returns its
    unique_id).
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


def test_artifact_read_concurrency_matches_sequential_results_off_the_main_thread(
    tmp_path: pathlib.Path,
) -> None:
    """Prefetch must not change what is loaded, and it must actually do the reading.

    ArtifactReader.load_json falls back to a direct read for any URI that was not
    prefetched, so a prefetch that silently served nothing would still produce
    identical nodes. The thread check closes that hole: with prefetch active no
    artifact - manifest, sibling or run_results - may be read on the main thread.
    """
    for i in range(5):
        _write_project(
            tmp_path,
            f"project_{i}",
            [{"name": f"model_{i}", "database": "db", "schema": f"sch_{i}"}],
            catalog_generated_at="2026-01-01T00:00:00.000000Z",
        )
    for i in range(2):
        _write_run_results(
            tmp_path / f"project_{i}" / "run_results.json",
            f"model.project_{i}.model_{i}",
            f"invocation-{i}",
        )
    config: Dict[str, Any] = {
        "manifest_path": f"{tmp_path}/*/manifest.json",
        "run_results_paths": [f"{tmp_path}/*/run_results.json"],
    }
    sequential = _load_nodes(_make_source(**config, artifact_read_concurrency=1))

    parallel_source = _make_source(**config, artifact_read_concurrency=4)
    real_read = dbt_artifacts_module.read_file_as_bytes
    reader_threads: Set[str] = set()

    def recording_read(uri: str, *args: Any, **kwargs: Any) -> bytes:
        reader_threads.add(threading.current_thread().name)
        return real_read(uri, *args, **kwargs)

    with mock.patch.object(
        dbt_artifacts_module, "read_file_as_bytes", side_effect=recording_read
    ):
        parallel = _load_nodes(parallel_source)

    # Same nodes in the same order: prefetch must not reorder project processing.
    assert [node.dbt_name for node in parallel] == [
        node.dbt_name for node in sequential
    ]
    assert parallel_source.report.manifests_loaded == 5
    assert parallel_source.report.manifests_failed == 0
    assert "MainThread" not in reader_threads
    assert {
        node.dbt_name: [p.run_id for p in node.model_performances]
        for node in parallel
        if node.model_performances
    } == {
        "model.project_0.model_0": ["invocation-0"],
        "model.project_1.model_1": ["invocation-1"],
    }


def test_artifact_read_concurrency_replays_fetch_errors_per_project(
    tmp_path: pathlib.Path,
) -> None:
    """A fetch error captured in a worker must fail only its own project.

    manifest.json as a directory makes the read raise (IsADirectoryError) inside
    the prefetch worker; the error must surface on the main thread through the
    same per-project isolation path as a sequential read failure.
    """
    for name in ["project_a", "project_c"]:
        _write_project(
            tmp_path, name, [{"name": f"m_{name}", "database": "db", "schema": name}]
        )
    (tmp_path / "project_b" / "manifest.json").mkdir(parents=True)

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json", artifact_read_concurrency=4
    )
    nodes = _load_nodes(source)

    assert {node.dbt_name for node in nodes} == {
        "model.project_a.m_project_a",
        "model.project_c.m_project_c",
    }
    assert source.report.manifests_loaded == 2
    assert source.report.manifests_failed == 1


def _node(dbt_name: str, **overrides: Any) -> DBTNode:
    defaults: Dict[str, Any] = dict(
        database=None,
        schema=None,
        name=dbt_name.split(".")[-1],
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name=dbt_name,
        dbt_file_path=None,
        dbt_package_name=dbt_name.split(".")[1],
        node_type=dbt_name.split(".")[0],
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
    )
    defaults.update(overrides)
    return DBTNode(**defaults)


def _project(**overrides: Any) -> DBTProject:
    defaults: Dict[str, Any] = dict(
        nodes=[],
        exposures=[],
        metrics=DBTMetricsParse(),
        platform_instance=None,
        project_name=None,
        manifest_path=None,
        artifact_props={},
        catalog_generated_at=None,
        manifest_generated_at=None,
    )
    defaults.update(overrides)
    return DBTProject(**defaults)


def _described_urns(source: DBTCoreSource) -> Set[str]:
    return {
        wu.get_urn()
        for wu in source.get_workunits()
        if isinstance(wu, MetadataWorkUnit)
        and wu.get_aspect_of_type(DatasetPropertiesClass) is not None
    }


def test_emit_loop_scopes_the_instance_to_each_project() -> None:
    source = _make_source(write_semantics="OVERRIDE")
    projects = [
        _project(
            platform_instance="project_a",
            project_name="project_a",
            nodes=[_node("model.project_a.orders", database="db", schema="sch_a")],
        ),
        _project(
            platform_instance="project_b",
            project_name="project_b",
            nodes=[_node("model.project_b.orders", database="db", schema="sch_b")],
        ),
    ]
    source.load_projects = lambda: iter(projects)  # type: ignore[method-assign]

    assert _described_urns(source) == {
        make_dataset_urn_with_platform_instance(
            "dbt", "db.sch_a.orders", "project_a", "PROD"
        ),
        make_dataset_urn_with_platform_instance(
            "dbt", "db.sch_b.orders", "project_b", "PROD"
        ),
    }


def test_an_empty_project_emits_nothing_and_fails_nothing() -> None:
    source = _make_source(write_semantics="OVERRIDE")
    source.load_projects = lambda: iter(  # type: ignore[method-assign]
        [_project(platform_instance="empty", project_name="empty")]
    )

    assert list(source.get_workunits()) == []
    assert source.report.failures == []
    assert source.report.num_exposures_emitted == 0


def test_assertion_urns_carry_the_current_projects_instance() -> None:
    source = _make_source()
    test_node = _node(
        "test.pkg.unique_dim_id",
        upstream_nodes=["model.pkg.dim"],
    )
    test_node.test_info = DBTTest(
        qualified_test_name="not_null", column_name="id", kw_args={}
    )
    model_node = _node("model.pkg.dim", database="db", schema="sch")

    def assertion_urns(instance: str) -> Set[str]:
        source._current_project = _project(platform_instance=instance)
        try:
            return {
                str(mcp.entityUrn)
                for mcp in source.create_test_entity_mcps(
                    [test_node], {"model.pkg.dim": model_node}
                )
            }
        finally:
            source._current_project = None

    a = assertion_urns("project_a")
    b = assertion_urns("project_b")
    assert a and b
    assert a.isdisjoint(b)


def test_each_project_gets_its_project_name_as_platform_instance(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "sch"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "orders", "database": "db", "schema": "sch"}]
    )
    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json", write_semantics="OVERRIDE"
    )

    workunits = [
        wu for wu in source.get_workunits() if isinstance(wu, MetadataWorkUnit)
    ]
    described = {
        wu.get_urn()
        for wu in workunits
        if wu.get_aspect_of_type(DatasetPropertiesClass) is not None
    }

    # Same relation in both projects: distinct dbt urns, one shared warehouse urn
    # (which carries lineage, not properties, so it is checked among all urns).
    assert described >= {
        make_dataset_urn_with_platform_instance(
            "dbt", "db.sch.orders", "project_a", "PROD"
        ),
        make_dataset_urn_with_platform_instance(
            "dbt", "db.sch.orders", "project_b", "PROD"
        ),
    }
    assert make_dataset_urn("postgres", "db.sch.orders", "PROD") in {
        wu.get_urn() for wu in workunits
    }
    assert source.report.failures == []


def test_shared_package_models_are_distinct_per_project(
    tmp_path: pathlib.Path,
) -> None:
    """Two projects installing one dbt package carry identical unique_ids.
    Each must emit its own copy, with its own intra-project lineage."""
    for project, schema in (("project_a", "sch_a"), ("project_b", "sch_b")):
        _write_project(
            tmp_path,
            project,
            [
                {
                    "name": "dim",
                    "database": "db",
                    "schema": f"{schema}_elem",
                    "package_name": "elementary",
                },
                {"name": "report", "database": "db", "schema": schema},
            ],
            depends_on={"report": ["model.elementary.dim"]},
        )
    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json", write_semantics="OVERRIDE"
    )
    workunits = [
        wu for wu in source.get_workunits() if isinstance(wu, MetadataWorkUnit)
    ]
    described = {
        wu.get_urn()
        for wu in workunits
        if wu.get_aspect_of_type(DatasetPropertiesClass) is not None
    }
    lineage = {
        wu.get_urn(): wu.get_aspect_of_type(UpstreamLineageClass)
        for wu in workunits
        if wu.get_aspect_of_type(UpstreamLineageClass) is not None
    }

    for project, schema in (("project_a", "sch_a"), ("project_b", "sch_b")):
        dim = make_dataset_urn_with_platform_instance(
            "dbt", f"db.{schema}_elem.dim", project, "PROD"
        )
        report = make_dataset_urn_with_platform_instance(
            "dbt", f"db.{schema}.report", project, "PROD"
        )
        assert dim in described and report in described
        report_lineage = lineage[report]
        assert report_lineage is not None
        assert {u.dataset for u in report_lineage.upstreams} == {
            make_dataset_urn("postgres", f"db.{schema}_elem.dim", "PROD")
        }
    assert source.report.failures == []


def test_run_results_match_the_project_in_their_directory(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "orders", "database": "db", "schema": "b"}]
    )
    _write_run_results(
        tmp_path / "project_a" / "run_results.json", "model.project_a.orders", "inv-a"
    )
    _write_run_results(
        tmp_path / "project_b" / "run_results.json", "model.project_b.orders", "inv-b"
    )
    (tmp_path / "stray").mkdir()
    _write_run_results(
        tmp_path / "stray" / "run_results.json", "model.project_a.orders", "inv-stray"
    )

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json",
        run_results_paths=[f"{tmp_path}/*/run_results.json"],
    )
    projects = {p.project_name: p for p in _load_projects(source)}

    runs = {
        name: [perf.run_id for perf in project.nodes[0].model_performances]
        for name, project in projects.items()
    }
    assert runs == {"project_a": ["inv-a"], "project_b": ["inv-b"]}
    stray = [w for w in source.report.warnings if any("stray" in c for c in w.context)]
    assert len(stray) == 1


@pytest.mark.parametrize("absolute_run_results", [False, True])
def test_run_results_pair_with_manifests_despite_dot_slash_prefix(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, absolute_run_results: bool
) -> None:
    """`./dbt/a`, `dbt/a` and `/abs/dbt/a` are the same directory and must pair."""
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "a"}]
    )
    _write_run_results(
        tmp_path / "project_a" / "run_results.json", "model.project_a.orders", "inv-a"
    )
    monkeypatch.chdir(tmp_path)

    source = _make_source(
        manifest_path="./*/manifest.json",
        run_results_paths=[
            f"{tmp_path}/*/run_results.json"
            if absolute_run_results
            else "*/run_results.json"
        ],
    )
    projects = _load_projects(source)

    assert [perf.run_id for perf in projects[0].nodes[0].model_performances] == [
        "inv-a"
    ]
    assert not [
        w for w in source.report.warnings if any("run_results" in c for c in w.context)
    ]


def test_a_manifest_without_project_name_is_a_failure_for_that_project(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "a"}]
    )
    _write_project(
        tmp_path, "project_b", [{"name": "orders", "database": "db", "schema": "b"}]
    )
    manifest = tmp_path / "project_b" / "manifest.json"
    data = json.loads(manifest.read_text())
    del data["metadata"]["project_name"]
    manifest.write_text(json.dumps(data))

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    projects = _load_projects(source)

    assert [p.project_name for p in projects] == ["project_a"]
    assert list(source.report.manifest_paths_failed) == [str(manifest)]


def test_two_manifests_with_one_project_name_fail_the_second(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path,
        "dev",
        [{"name": "orders", "database": "db", "schema": "a"}],
        project_name="analytics",
    )
    _write_project(
        tmp_path,
        "prod",
        [{"name": "orders", "database": "db", "schema": "a"}],
        project_name="analytics",
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    projects = _load_projects(source)

    assert [p.manifest_path for p in projects] == [f"{tmp_path}/dev/manifest.json"]
    assert source.report.manifests_failed == 1
    context = " ".join(source.report.failures[0].context)
    assert "analytics" in context
    # Names the build that was kept, not only the one that was skipped.
    assert f"{tmp_path}/dev/manifest.json" in context
    # Rejected before any other artifact was read, so nothing else was reported.
    assert not [w for w in source.report.warnings if "prod" in " ".join(w.context)]


def test_single_manifest_keeps_the_configured_platform_instance(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path, "project_a", [{"name": "orders", "database": "db", "schema": "a"}]
    )
    source = _make_source(
        manifest_path=f"{tmp_path}/project_a/manifest.json",
        platform_instance="legacy_instance",
        write_semantics="OVERRIDE",
    )

    assert make_dataset_urn_with_platform_instance(
        "dbt", "db.a.orders", "legacy_instance", "PROD"
    ) in _described_urns(source)


def test_a_failed_project_does_not_leak_into_its_neighbours(
    tmp_path: pathlib.Path,
) -> None:
    _write_project(
        tmp_path, "a_first", [{"name": "orders", "database": "db", "schema": "a"}]
    )
    _write_project(
        tmp_path, "b_broken", [{"name": "orders", "database": "db", "schema": "b"}]
    )
    _write_project(
        tmp_path, "c_last", [{"name": "orders", "database": "db", "schema": "c"}]
    )
    (tmp_path / "b_broken" / "manifest.json").write_text("{not json")

    source = _make_source(
        manifest_path=f"{tmp_path}/*/manifest.json", write_semantics="OVERRIDE"
    )

    assert _described_urns(source) >= {
        make_dataset_urn_with_platform_instance(
            "dbt", "db.a.orders", "a_first", "PROD"
        ),
        make_dataset_urn_with_platform_instance("dbt", "db.c.orders", "c_last", "PROD"),
    }
    assert source.report.manifests_failed == 1


@pytest.mark.parametrize(
    "scheme, connection",
    [
        ("s3", {"aws_connection": {"aws_region": "us-east-1"}}),
        (
            "gs",
            {
                "gcs_connection": {
                    "credential": {"hmac_access_id": "id", "hmac_access_secret": "s"}
                }
            },
        ),
    ],
)
def test_object_store_run_results_pair_with_their_project(
    tmp_path: pathlib.Path, scheme: str, connection: Dict[str, Any]
) -> None:
    for name in ["project_a", "project_b"]:
        _write_project(
            tmp_path, name, [{"name": "orders", "database": "db", "schema": name}]
        )
        _write_run_results(
            tmp_path / name / "run_results.json",
            f"model.{name}.orders",
            f"inv-{name}",
        )
    root = f"{scheme}://bucket"

    def fake_expand(pattern: str, *args: Any, **kwargs: Any) -> List[str]:
        filename = pattern.rsplit("/", 1)[1]
        return [f"{root}/{name}/{filename}" for name in ["project_a", "project_b"]]

    def fake_read(uri: str, *args: Any, **kwargs: Any) -> bytes:
        local = tmp_path / uri[len(root) + 1 :]
        if not local.exists():
            raise ObjectNotFoundError(uri)
        return local.read_bytes()

    source = _make_source(
        manifest_path=f"{root}/*/manifest.json",
        run_results_paths=[f"{root}/*/run_results.json"],
        **connection,
    )
    with (
        mock.patch.object(
            dbt_artifacts_module, "expand_object_store_glob", side_effect=fake_expand
        ),
        mock.patch.object(
            dbt_artifacts_module, "read_file_as_bytes", side_effect=fake_read
        ),
    ):
        projects = _load_projects(source)

    assert {
        p.project_name: [perf.run_id for perf in p.nodes[0].model_performances]
        for p in projects
    } == {"project_a": ["inv-project_a"], "project_b": ["inv-project_b"]}


def test_project_outside_the_emit_loop_raises_in_glob_mode() -> None:
    source = _make_source(
        manifest_path="s3://bucket/*/manifest.json",
        aws_connection={"aws_region": "us-east-1"},
    )

    with pytest.raises(RuntimeError):
        _ = source._dbt_platform_instance


def test_unparseable_catalog_generated_at_warns(tmp_path: pathlib.Path) -> None:
    _write_project(
        tmp_path,
        "project_a",
        [{"name": "orders", "database": "db", "schema": "a"}],
        catalog_generated_at="not a timestamp",
    )

    source = _make_source(manifest_path=f"{tmp_path}/*/manifest.json")
    (project,) = _load_projects(source)

    assert project.catalog_generated_at is None
    assert any("not a timestamp" in " ".join(w.context) for w in source.report.warnings)


def test_query_id_prefix_keeps_non_ascii_project_names_distinct() -> None:
    assert _query_id_prefix("analytics") == "analytics"
    assert _query_id_prefix("datos_año") != _query_id_prefix("datos_aõo")
