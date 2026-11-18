import copy
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
from unittest import mock

from datahub.emitter import mce_builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dbt.dbt_common import (
    DBT_PLATFORM,
    DBTColumn,
    DBTModelPerformance,
    DBTNode,
)
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)

SERVER_OWNER = "urn:li:corpuser:server_owner"
SERVER_DBT_OWNER = "urn:li:corpuser:server_dbt_owner"


def _make_graph() -> mock.MagicMock:
    graph = mock.MagicMock()
    graph.get_ownership.return_value = OwnershipClass(
        owners=[
            OwnerClass(owner=SERVER_OWNER, type=OwnershipTypeClass.DATAOWNER),
            OwnerClass(
                owner=SERVER_DBT_OWNER,
                type=OwnershipTypeClass.DATAOWNER,
                source=OwnershipSourceClass(
                    type=OwnershipSourceTypeClass.SOURCE_CONTROL
                ),
            ),
        ]
    )
    graph.get_tags.return_value = mce_builder.make_global_tag_aspect_with_tag_list(
        ["server_tag", "dbt:server_dbt_tag"]
    )
    graph.get_glossary_terms.return_value = (
        mce_builder.make_glossary_terms_aspect_from_urn_list(
            ["urn:li:glossaryTerm:server_term"]
        )
    )
    return graph


def _make_source(graph: Any, **overrides: Any) -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    ctx.graph = graph
    config: Dict[str, Any] = {
        "manifest_path": "temp/",
        "catalog_path": "temp/",
        "target_platform": "postgres",
        "tag_prefix": "dbt:",
        "enable_meta_mapping": True,
        "dbt_is_primary_sibling": False,
        "meta_mapping": {
            "business_owner": {
                "match": ".*",
                "operation": "add_owner",
                "config": {"owner_type": "user"},
            },
            "term": {
                "match": ".*",
                "operation": "add_term",
                "config": {"term": "{{ $match }}"},
            },
            "domain": {
                "match": ".*",
                "operation": "add_domain",
                "config": {"domain": "{{ $match }}"},
            },
            "frequency": {
                "match": ".*",
                "operation": "add_structured_property",
                "config": {
                    "structured_property_urn": "urn:li:structuredProperty:frequency"
                },
            },
            "docs": {
                "match": ".*",
                "operation": "add_doc_link",
                "config": {"link": "{{ $match }}", "description": "docs"},
            },
        },
    }
    config.update(overrides)
    return DBTCoreSource(DBTCoreConfig(**config), ctx)


def _make_node(meta: Dict[str, Any], tags: List[str]) -> DBTNode:
    return DBTNode(
        database="my_db",
        schema="my_schema",
        name="my_model",
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code="select 1 as id",
        dbt_adapter="postgres",
        dbt_name="model.my_package.my_model",
        dbt_file_path="models/my_model.sql",
        dbt_package_name="my_package",
        node_type="model",
        max_loaded_at=None,
        materialization="table",
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        compiled_code="select 1 as id",
        meta=meta,
        tags=tags,
    )


FULL_META = {
    "business_owner": "alice",
    "term": "my_term",
    "domain": "my_domain",
    "frequency": "daily",
    "docs": "https://example.com/docs",
}


def _run(source: DBTCoreSource, node: DBTNode) -> List[Tuple[str, Any]]:
    # Deep-copy while iterating, so a later in-place edit of a yielded aspect is caught.
    out = []
    for wu in source.create_dbt_platform_mcps(
        dbt_nodes=[node], all_nodes_map={node.dbt_name: node}
    ):
        assert isinstance(wu, MetadataWorkUnit)
        out.append((wu.get_urn(), copy.deepcopy(wu)))
    # create_dbt_platform_mcps swallows per-node errors; a failed node must not pass.
    assert source.report.node_emission_failures == 0
    return out


def _urn(source: DBTCoreSource, node: DBTNode) -> str:
    return node.get_urn(DBT_PLATFORM, source.config.env, None)


def _aspect(out: List[Tuple[str, Any]], aspect_type: type) -> Any:
    found = [wu.get_aspect_of_type(aspect_type) for _, wu in out]
    found = [a for a in found if a is not None]
    assert len(found) == 1, f"expected one {aspect_type.__name__}, got {len(found)}"
    return found[0]


def _aspect_names(out: List[Tuple[str, Any]]) -> List[Tuple[str, str]]:
    return [(urn, wu.metadata.aspectName) for urn, wu in out]


def test_patch_merges_server_owners_tags_terms() -> None:
    graph = _make_graph()
    source = _make_source(graph)
    node = _make_node(FULL_META, tags=["node_tag"])
    urn = _urn(source, node)

    out = _run(source, node)

    owners = {
        (o.owner, o.source.type if o.source else None)
        for o in _aspect(out, OwnershipClass).owners
    }
    # The server's SOURCE_CONTROL owner is replaced by dbt's; others are kept.
    assert owners == {
        ("urn:li:corpuser:alice", OwnershipSourceTypeClass.SOURCE_CONTROL),
        (SERVER_OWNER, None),
    }

    tags = [t.tag for t in _aspect(out, GlobalTagsClass).tags]
    # Server tags with the dbt prefix are replaced; others are kept.
    assert tags == ["urn:li:tag:node_tag", "urn:li:tag:server_tag"]

    terms = [t.urn for t in _aspect(out, GlossaryTermsClass).terms]
    assert terms == ["urn:li:glossaryTerm:my_term", "urn:li:glossaryTerm:server_term"]

    graph.get_ownership.assert_called_once_with(urn)
    graph.get_tags.assert_called_once_with(urn)
    graph.get_glossary_terms.assert_called_once_with(urn)


def test_patch_adds_no_aspect_when_node_has_none() -> None:
    graph = _make_graph()
    source = _make_source(graph, enable_owner_extraction=False)
    node = _make_node({}, tags=[])

    names = {name for _, name in _aspect_names(_run(source, node))}

    assert {"datasetProperties", "schemaMetadata"} <= names
    assert names.isdisjoint({"ownership", "globalTags", "glossaryTerms"})
    graph.get_ownership.assert_not_called()
    graph.get_tags.assert_not_called()
    graph.get_glossary_terms.assert_not_called()


def test_patch_output_order() -> None:
    source = _make_source(
        _make_graph(),
        column_meta_mapping={
            "frequency": {
                "match": ".*",
                "operation": "add_structured_property",
                "config": {
                    "structured_property_urn": "urn:li:structuredProperty:frequency"
                },
            },
        },
    )
    node = _make_node(FULL_META, tags=["node_tag"])
    node.columns = [
        DBTColumn(
            name="id",
            comment="",
            description="",
            index=0,
            data_type="integer",
            meta={"frequency": "daily"},
        )
    ]
    node.model_performances = [
        DBTModelPerformance(
            run_id="run-1",
            status="success",
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
        )
    ]
    node.row_count = 10
    urn = _urn(source, node)

    field_urn = mce_builder.make_schema_field_urn(urn, "id")
    run_urn = "urn:li:dataProcessInstance:d4847e8e702e21636438f63676c76940"

    assert _aspect_names(_run(source, node)) == [
        (urn, "subTypes"),
        (urn, "dataPlatformInstance"),
        (urn, "domains"),
        (urn, "structuredProperties"),
        (urn, "siblings"),
        (urn, "datasetProperties"),
        (urn, "status"),
        (urn, "ownership"),
        (urn, "globalTags"),
        (urn, "glossaryTerms"),
        (urn, "institutionalMemory"),
        (urn, "schemaMetadata"),
        (urn, "viewProperties"),
        (field_urn, "structuredProperties"),
        (run_urn, "dataProcessInstanceProperties"),
        (run_urn, "dataProcessInstanceRelationships"),
        (run_urn, "dataProcessInstanceOutput"),
        (run_urn, "dataProcessInstanceRunEvent"),
        (run_urn, "dataProcessInstanceRunEvent"),
        (urn, "datasetProfile"),
    ]


def test_patch_without_graph_keeps_dbt_values() -> None:
    source = _make_source(None)
    node = _make_node(FULL_META, tags=["node_tag", "node_tag"])

    out = _run(source, node)

    assert [o.owner for o in _aspect(out, OwnershipClass).owners] == [
        "urn:li:corpuser:alice"
    ]
    assert [t.tag for t in _aspect(out, GlobalTagsClass).tags] == [
        "urn:li:tag:node_tag"
    ]
    assert [t.urn for t in _aspect(out, GlossaryTermsClass).terms] == [
        "urn:li:glossaryTerm:my_term"
    ]
