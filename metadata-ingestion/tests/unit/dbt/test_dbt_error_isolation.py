from typing import Any, Dict
from unittest import mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import (
    DBT_PLATFORM,
    DBTNode,
    DBTSourceReport,
)
from datahub.ingestion.source.dbt.dbt_core import (
    DBTCoreConfig,
    DBTCoreSource,
    extract_dbt_entities,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult


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


def test_parse_cll_catches_sqlglot_lineage_crash(monkeypatch):
    """A native sqlglot_lineage crash is caught and degrades to an error result
    instead of propagating and aborting the run."""
    source = _make_source()
    node = _make_sql_model_node("model.pkg.my_model", "models/my_model.sql")
    schema_resolver = SchemaResolver(platform="postgres")

    monkeypatch.setattr(
        "datahub.ingestion.source.dbt.dbt_common.sqlglot_lineage",
        mock.Mock(side_effect=RuntimeError("native crash")),
    )

    result = source._parse_cll(node, cte_mapping={}, schema_resolver=schema_resolver)

    assert isinstance(result, SqlParsingResult)
    assert result.debug_info.table_error is not None
    assert source.report.sql_parser_table_errors == 1


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
