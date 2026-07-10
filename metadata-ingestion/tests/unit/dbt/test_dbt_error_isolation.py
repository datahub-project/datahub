from datetime import datetime
from typing import Any, Dict

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt import dbt_common
from datahub.ingestion.source.dbt.dbt_common import (
    DBT_PLATFORM,
    DBTColumn,
    DBTExposure,
    DBTNode,
    DBTSourceReport,
)
from datahub.ingestion.source.dbt.dbt_core import (
    DBTCoreConfig,
    DBTCoreSource,
    extract_dbt_entities,
)
from datahub.ingestion.source.dbt.dbt_tests import (
    DBTFreshnessCriteria,
    DBTFreshnessInfo,
    DBTTest,
)


def _make_source(target_platform: str = "postgres") -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    ctx.graph = None
    return DBTCoreSource(
        DBTCoreConfig(
            manifest_path="temp/",
            catalog_path="temp/",
            target_platform=target_platform,
            enable_meta_mapping=False,
        ),
        ctx,
    )


def _make_sql_model_node(
    dbt_name: str,
    file_path: str,
    compiled_code: str = "select 1 as col1",
) -> DBTNode:
    return DBTNode(
        database="mydb",
        schema="myschema",
        name=dbt_name.rsplit(".", 1)[-1],
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=compiled_code,
        dbt_adapter="postgres",
        dbt_name=dbt_name,
        dbt_file_path=file_path,
        dbt_package_name="mypackage",
        node_type="model",
        max_loaded_at=None,
        materialization="table",
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        compiled_code=compiled_code,
    )


def _make_test_node(dbt_name: str, file_path: str, upstream_dbt_name: str) -> DBTNode:
    return DBTNode(
        database="mydb",
        schema="myschema",
        name=dbt_name.rsplit(".", 1)[-1],
        alias=None,
        comment="",
        description="",
        language=None,
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name=dbt_name,
        dbt_file_path=file_path,
        dbt_package_name="mypackage",
        node_type="test",
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        upstream_nodes=[upstream_dbt_name],
        test_info=DBTTest(
            qualified_test_name="not_null", column_name="col_a", kw_args={}
        ),
    )


def _make_source_node_with_freshness(dbt_name: str, file_path: str) -> DBTNode:
    return DBTNode(
        database="mydb",
        schema="myschema",
        name=dbt_name.rsplit(".", 1)[-1],
        alias=None,
        comment="",
        description="",
        language=None,
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name=dbt_name,
        dbt_file_path=file_path,
        dbt_package_name="mypackage",
        node_type="source",
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        freshness_info=DBTFreshnessInfo(
            invocation_id="run-1",
            status="pass",
            max_loaded_at=datetime(2024, 1, 1),
            snapshotted_at=datetime(2024, 1, 2),
            max_loaded_at_time_ago_in_s=3600.0,
            warn_after=DBTFreshnessCriteria(count=1, period="day"),
            error_after=None,
        ),
    )


def _make_exposure(unique_id: str, file_path: str) -> DBTExposure:
    return DBTExposure(
        name=unique_id.rsplit(".", 1)[-1],
        unique_id=unique_id,
        type="dashboard",
        dbt_file_path=file_path,
    )


def _valid_manifest_entity(name: str) -> Dict[str, Any]:
    return {
        "name": name,
        "database": "test_db",
        "schema": "test_schema",
        "resource_type": "model",
        "original_file_path": f"models/{name}.sql",
        "config": {"materialized": "table"},
        "description": "",
        "meta": {},
        "tags": [],
    }


def test_infer_schemas_and_update_cll_isolates_node_failure(monkeypatch):
    """One node's CLL processing crashes; the other node is still processed and
    the failure is recorded with the failing node's name and file path."""
    source = _make_source()
    good = _make_sql_model_node("model.pkg.good_model", "models/good_model.sql")
    bad = _make_sql_model_node("model.pkg.bad_model", "models/bad_model.sql")
    all_nodes_map = {good.dbt_name: good, bad.dbt_name: bad}

    original_parse_cll = source._parse_cll

    def fake_parse_cll(node, cte_mapping, schema_resolver):
        if node.dbt_name == bad.dbt_name:
            raise RuntimeError("simulated sqlglot crash")
        return original_parse_cll(node, cte_mapping, schema_resolver)

    monkeypatch.setattr(source, "_parse_cll", fake_parse_cll)

    # Must not raise, even though one node's CLL processing crashes.
    source._infer_schemas_and_update_cll(all_nodes_map)

    assert source.report.node_cll_failures == 1
    # The good node was still processed (its CLL debug info got populated).
    assert good.cll_debug_info is not None

    [failure_entry] = source.report.node_cll_failures_list
    assert bad.dbt_name in failure_entry
    assert "models/bad_model.sql" in failure_entry


def test_infer_schemas_and_update_cll_all_nodes_failing_does_not_abort(monkeypatch):
    """Even if every node fails, the method completes without raising (no abort)."""
    source = _make_source()
    node_a = _make_sql_model_node("model.pkg.a", "models/a.sql")
    node_b = _make_sql_model_node("model.pkg.b", "models/b.sql")
    all_nodes_map = {node_a.dbt_name: node_a, node_b.dbt_name: node_b}

    def always_raise(node, cte_mapping, schema_resolver):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(source, "_parse_cll", always_raise)

    source._infer_schemas_and_update_cll(all_nodes_map)

    assert source.report.node_cll_failures == 2


def test_infer_schemas_and_update_cll_failure_preserves_existing_columns(monkeypatch):
    """A CLL-only failure degrades (keeps the node's already-known table-level
    columns) rather than dropping the node or clearing its schema."""
    source = _make_source()
    bad = _make_sql_model_node("model.pkg.bad_model", "models/bad_model.sql")
    bad.columns = [
        DBTColumn(name="col_a", comment="", description="", index=0, data_type="text")
    ]
    all_nodes_map = {bad.dbt_name: bad}

    def always_raise(node, cte_mapping, schema_resolver):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(source, "_parse_cll", always_raise)

    source._infer_schemas_and_update_cll(all_nodes_map)

    assert source.report.node_cll_failures == 1
    # Degraded, not dropped: the node keeps its pre-existing table-level columns.
    assert [c.name for c in bad.columns] == ["col_a"]


def test_infer_schemas_and_update_cll_downstream_node_still_processes_after_upstream_failure(
    monkeypatch,
):
    """Failure isolation must not corrupt topological-order processing: a node
    downstream of a node whose CLL processing failed is still processed."""
    source = _make_source()
    upstream = _make_sql_model_node("model.pkg.upstream", "models/upstream.sql")
    downstream = _make_sql_model_node(
        "model.pkg.downstream",
        "models/downstream.sql",
        compiled_code="select col1 from upstream",
    )
    downstream.upstream_nodes = [upstream.dbt_name]
    all_nodes_map = {upstream.dbt_name: upstream, downstream.dbt_name: downstream}

    original_parse_cll = source._parse_cll

    def fake_parse_cll(node, cte_mapping, schema_resolver):
        if node.dbt_name == upstream.dbt_name:
            raise RuntimeError("simulated crash")
        return original_parse_cll(node, cte_mapping, schema_resolver)

    monkeypatch.setattr(source, "_parse_cll", fake_parse_cll)

    source._infer_schemas_and_update_cll(all_nodes_map)

    assert source.report.node_cll_failures == 1
    [failure_entry] = source.report.node_cll_failures_list
    assert upstream.dbt_name in failure_entry

    # The downstream node was still processed despite its upstream's failure.
    assert downstream.cll_debug_info is not None


def test_extract_dbt_entities_isolates_malformed_node():
    """A malformed manifest node (missing required keys) is dropped, while
    other nodes are extracted normally and the failure is counted."""
    report = DBTSourceReport()
    manifest_entities = {
        "model.test.good_model": _valid_manifest_entity("good_model"),
        "model.test.bad_model": {
            "name": "bad_model",
            "resource_type": "model",
            "original_file_path": "models/bad_model.sql",
            "config": {"materialized": "table"},
            # Missing "database"/"schema" triggers a KeyError during extraction.
        },
    }

    nodes = extract_dbt_entities(
        all_manifest_entities=manifest_entities,
        all_catalog_entities=None,
        sources_results=[],
        manifest_adapter="postgres",
        use_identifiers=False,
        tag_prefix="dbt:",
        only_include_if_in_catalog=False,
        include_database_name=True,
        report=report,
    )

    assert [node.dbt_name for node in nodes] == ["model.test.good_model"]
    assert report.node_extraction_failures == 1

    [failure_entry] = report.node_extraction_failures_list
    assert "model.test.bad_model" in failure_entry
    assert "models/bad_model.sql" in failure_entry


def test_create_dbt_platform_mces_isolates_node_emission_failure(monkeypatch):
    """One node's emission crashes; the other node's workunits are still
    produced and the failure is recorded with name + file path."""
    source = _make_source()
    good = _make_sql_model_node("model.pkg.good_model", "models/good_model.sql")
    bad = _make_sql_model_node("model.pkg.bad_model", "models/bad_model.sql")
    all_nodes_map = {good.dbt_name: good, bad.dbt_name: bad}

    original_generate_base_dbt_aspects = source._generate_base_dbt_aspects

    def fake_generate_base_dbt_aspects(node, *args, **kwargs):
        if node.dbt_name == bad.dbt_name:
            raise RuntimeError("simulated emission crash")
        return original_generate_base_dbt_aspects(node, *args, **kwargs)

    monkeypatch.setattr(
        source, "_generate_base_dbt_aspects", fake_generate_base_dbt_aspects
    )

    workunits = list(
        source.create_dbt_platform_mces(
            dbt_nodes=[good, bad],
            additional_custom_props_filtered={},
            all_nodes_map=all_nodes_map,
        )
    )

    good_urn = good.get_urn(
        DBT_PLATFORM, source.config.env, source.config.platform_instance
    )
    bad_urn = bad.get_urn(
        DBT_PLATFORM, source.config.env, source.config.platform_instance
    )
    emitted_urns = {wu.get_urn() for wu in workunits}

    assert good_urn in emitted_urns
    assert bad_urn not in emitted_urns
    assert source.report.node_emission_failures == 1

    [failure_entry] = source.report.node_emission_failures_list
    assert bad.dbt_name in failure_entry
    assert "models/bad_model.sql" in failure_entry


def test_create_dbt_platform_mces_partial_emission_before_failure(monkeypatch):
    """Per the locked design, isolation does NOT buffer-and-rollback: if a
    node's failure occurs after some of its workunits were already yielded,
    those workunits stay in the output rather than being discarded."""
    source = _make_source()
    bad = _make_sql_model_node("model.pkg.bad_model", "models/bad_model.sql")

    def fake_get_patched_mce(mce):
        # Fires after the DataPlatformInstance aspect (and any subtype/standalone
        # aspects) have already been yielded for this node, but before its main
        # MCE snapshot would be.
        raise RuntimeError("simulated crash after partial emission")

    monkeypatch.setattr(source, "get_patched_mce", fake_get_patched_mce)

    workunits = list(
        source.create_dbt_platform_mces(
            dbt_nodes=[bad],
            additional_custom_props_filtered={},
            all_nodes_map={bad.dbt_name: bad},
        )
    )

    bad_urn = bad.get_urn(
        DBT_PLATFORM, source.config.env, source.config.platform_instance
    )
    assert workunits, "partial workunits from before the failure should survive"
    assert all(wu.get_urn() == bad_urn for wu in workunits)
    assert source.report.node_emission_failures == 1


def test_create_target_platform_mces_isolates_node_emission_failure(monkeypatch):
    """One node's target-platform emission crashes; the other node's
    workunits are still produced and the failure is recorded."""
    source = _make_source()
    good = _make_sql_model_node("model.pkg.good_model", "models/good_model.sql")
    bad = _make_sql_model_node("model.pkg.bad_model", "models/bad_model.sql")

    original_create_query_entity_mcps = source._create_query_entity_mcps

    def fake_create_query_entity_mcps(node, node_datahub_urn):
        if node.dbt_name == bad.dbt_name:
            raise RuntimeError("simulated emission crash")
        return original_create_query_entity_mcps(node, node_datahub_urn)

    monkeypatch.setattr(
        source, "_create_query_entity_mcps", fake_create_query_entity_mcps
    )

    workunits = list(source.create_target_platform_mces(dbt_nodes=[good, bad]))

    good_urn = good.get_urn(
        source.config.target_platform,
        source.config.env,
        source.config.target_platform_instance,
    )
    bad_urn = bad.get_urn(
        source.config.target_platform,
        source.config.env,
        source.config.target_platform_instance,
    )
    emitted_urns = {wu.get_urn() for wu in workunits}

    assert good_urn in emitted_urns
    assert bad_urn not in emitted_urns
    assert source.report.node_emission_failures == 1

    [failure_entry] = source.report.node_emission_failures_list
    assert bad.dbt_name in failure_entry
    assert "models/bad_model.sql" in failure_entry


def test_create_test_entity_mcps_isolates_node_emission_failure(monkeypatch):
    """One test node's emission crashes; the other test node's assertion
    MCPs are still produced and the failure is recorded."""
    source = _make_source()
    upstream = _make_sql_model_node("model.pkg.upstream", "models/upstream.sql")
    good = _make_test_node(
        "test.pkg.good_test", "tests/good_test.sql", upstream.dbt_name
    )
    bad = _make_test_node("test.pkg.bad_test", "tests/bad_test.sql", upstream.dbt_name)
    all_nodes_map = {
        upstream.dbt_name: upstream,
        good.dbt_name: good,
        bad.dbt_name: bad,
    }

    original_get_upstreams_for_test = dbt_common.get_upstreams_for_test

    def fake_get_upstreams_for_test(
        test_node, all_nodes_map, platform_instance, environment
    ):
        if test_node.dbt_name == bad.dbt_name:
            raise RuntimeError("simulated emission crash")
        return original_get_upstreams_for_test(
            test_node=test_node,
            all_nodes_map=all_nodes_map,
            platform_instance=platform_instance,
            environment=environment,
        )

    monkeypatch.setattr(
        dbt_common, "get_upstreams_for_test", fake_get_upstreams_for_test
    )

    mcps = list(
        source.create_test_entity_mcps(
            test_nodes=[good, bad],
            extra_custom_props={},
            all_nodes_map=all_nodes_map,
        )
    )

    assert mcps, "good test's assertion MCPs should still be produced"
    assert source.report.node_emission_failures == 1

    [failure_entry] = source.report.node_emission_failures_list
    assert bad.dbt_name in failure_entry
    assert "tests/bad_test.sql" in failure_entry


def test_create_freshness_assertion_mcps_isolates_node_emission_failure(monkeypatch):
    """One source node's freshness-assertion emission crashes; the other
    source node's MCPs are still produced and the failure is recorded."""
    source = _make_source()
    good = _make_source_node_with_freshness(
        "source.pkg.good_source", "models/good_source.yml"
    )
    bad = _make_source_node_with_freshness(
        "source.pkg.bad_source", "models/bad_source.yml"
    )

    original_make_assertion_from_freshness = dbt_common.make_assertion_from_freshness

    def fake_make_assertion_from_freshness(
        extra_custom_props, node, assertion_urn, upstream_urn
    ):
        if node.dbt_name == bad.dbt_name:
            raise RuntimeError("simulated crash")
        return original_make_assertion_from_freshness(
            extra_custom_props, node, assertion_urn, upstream_urn
        )

    monkeypatch.setattr(
        dbt_common,
        "make_assertion_from_freshness",
        fake_make_assertion_from_freshness,
    )

    mcps = list(
        source.create_freshness_assertion_mcps([good, bad], extra_custom_props={})
    )

    assert mcps, "good source's freshness assertion MCPs should still be produced"
    assert source.report.node_emission_failures == 1

    [failure_entry] = source.report.node_emission_failures_list
    assert bad.dbt_name in failure_entry
    assert "models/bad_source.yml" in failure_entry


def test_create_exposure_mcps_isolates_node_emission_failure(monkeypatch):
    """One exposure's emission crashes; the other exposure's MCPs are still
    produced and the failure is recorded with its unique_id and file path."""
    source = _make_source()
    good = _make_exposure("exposure.pkg.good_exposure", "models/good_exposure.yml")
    bad = _make_exposure("exposure.pkg.bad_exposure", "models/bad_exposure.yml")

    original_get_urn = DBTExposure.get_urn

    def fake_get_urn(self, platform_instance=None):
        if self.unique_id == bad.unique_id:
            raise RuntimeError("simulated crash")
        return original_get_urn(self, platform_instance=platform_instance)

    monkeypatch.setattr(DBTExposure, "get_urn", fake_get_urn)

    mcps = list(source.create_exposure_mcps([good, bad], all_nodes_map={}))

    assert mcps, "good exposure's MCPs should still be produced"
    assert source.report.node_emission_failures == 1

    [failure_entry] = source.report.node_emission_failures_list
    assert bad.unique_id in failure_entry
    assert "models/bad_exposure.yml" in failure_entry
