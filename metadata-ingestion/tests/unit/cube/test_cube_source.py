from typing import Dict, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.cube.config import CubeSourceConfig
from datahub.ingestion.source.cube.cube import CubeSource
from datahub.ingestion.source.cube.cube_lineage import CubeLineageBuilder
from datahub.ingestion.source.cube.models import (
    CubeColumnReference,
    CubeEntity,
    CubeMember,
)
from datahub.metadata.schema_classes import (
    DomainsClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    OwnershipClass,
    SiblingsClass,
)


def _lineage_builder(source: CubeSource) -> CubeLineageBuilder:
    return CubeLineageBuilder(
        config=source.config,
        ctx=source.ctx,
        warehouse_platform=source.config.warehouse_platform,
        warehouse_database=source.config.warehouse_database,
    )


def _source(**overrides: object) -> CubeSource:
    base: Dict[str, object] = {
        "api_url": "https://demo.cubecloud.dev/cubejs-api",
        "api_token": "t",
    }
    base.update(overrides)
    config = CubeSourceConfig.model_validate(base)
    return CubeSource(config, PipelineContext(run_id="test"))


def _aspects(wus: List[MetadataWorkUnit]) -> Dict[str, object]:
    out: Dict[str, object] = {}
    for wu in wus:
        aspect = wu.metadata.aspect  # type: ignore[union-attr]
        if aspect is not None:
            out[type(aspect).__name__] = aspect
    return out


def test_deployment_url_derived_from_api_url() -> None:
    assert _source()._deployment_url() == "https://demo.cubecloud.dev"
    assert (
        _source(deployment_url="https://ui.example.com/")._deployment_url()
        == "https://ui.example.com"
    )


def test_meta_mapping_emits_owner_tag_and_term() -> None:
    source = _source(
        meta_mapping={
            "owner": {
                "match": ".*",
                "operation": "add_owner",
                "config": {"owner_type": "user"},
            },
            "certified": {
                "match": True,
                "operation": "add_tag",
                "config": {"tag": "certified"},
            },
            "glossary": {
                "match": ".*",
                "operation": "add_term",
                "config": {"term": "Revenue"},
            },
        }
    )
    entity = CubeEntity(
        name="orders",
        meta={
            "owner": "jdoe@example.com",
            "certified": True,
            "glossary": "Revenue",
        },
    )

    aspects = _aspects(list(source._emit_entity_meta(entity, "urn:li:dataset:test")))
    assert isinstance(aspects.get("OwnershipClass"), OwnershipClass)
    assert isinstance(aspects.get("GlobalTagsClass"), GlobalTagsClass)
    assert isinstance(aspects.get("GlossaryTermsClass"), GlossaryTermsClass)


def test_datahub_meta_block_emits_domain() -> None:
    source = _source()
    entity = CubeEntity(
        name="orders",
        meta={"datahub": {"domain": "analytics"}},
    )

    aspects = _aspects(list(source._emit_entity_meta(entity, "urn:li:dataset:test")))
    domains = aspects.get("DomainsClass")
    assert isinstance(domains, DomainsClass)
    assert domains.domains == ["urn:li:domain:analytics"]


def test_pattern_domain_assignment() -> None:
    source = _source(
        domain={"urn:li:domain:analytics": {"allow": ["orders.*"]}},
    )
    entity = CubeEntity(name="orders")

    aspects = _aspects(list(source._emit_entity_meta(entity, "urn:li:dataset:test")))
    domains = aspects.get("DomainsClass")
    assert isinstance(domains, DomainsClass)
    assert domains.domains == ["urn:li:domain:analytics"]


def test_no_meta_emits_nothing() -> None:
    source = _source()
    entity = CubeEntity(name="orders")
    assert list(source._emit_entity_meta(entity, "urn:li:dataset:test")) == []


def test_temporal_dimension_is_tagged() -> None:
    source = _source()
    field = source._build_schema_field(
        CubeMember(name="created_at", is_measure=False, is_temporal=True)
    )
    assert field.globalTags is not None
    tags = {t.tag for t in field.globalTags.tags}
    assert "urn:li:tag:Dimension" in tags
    assert "urn:li:tag:Temporal" in tags


def test_column_meta_mapping_tags_and_terms_field() -> None:
    source = _source(
        column_meta_mapping={
            "pii": {
                "match": True,
                "operation": "add_tag",
                "config": {"tag": "pii"},
            },
            "term": {
                "match": ".*",
                "operation": "add_term",
                "config": {"term": "CustomerData"},
            },
        }
    )
    field = source._build_schema_field(
        CubeMember(
            name="email",
            is_measure=False,
            meta={"pii": True, "term": "CustomerData"},
        )
    )
    assert field.globalTags is not None
    assert any(t.tag == "urn:li:tag:pii" for t in field.globalTags.tags)
    assert field.glossaryTerms is not None
    assert any("CustomerData" in t.urn for t in field.glossaryTerms.terms)


def _one_to_one_entity() -> CubeEntity:
    return CubeEntity(
        name="orders",
        table_references=[CubeColumnReference(schema_name="public", table="orders")],
    )


def test_siblings_emitted_for_one_to_one_cube() -> None:
    source = _source(warehouse_platform="postgres", warehouse_database="analytics")
    wus = list(
        source._emit_siblings(
            _one_to_one_entity(),
            "urn:li:dataset:(urn:li:dataPlatform:cube,orders,PROD)",
            _lineage_builder(source),
        )
    )

    siblings = [
        aspect
        for wu in wus
        if isinstance((aspect := getattr(wu.metadata, "aspect", None)), SiblingsClass)
    ]
    assert len(siblings) == 1
    assert any("postgres" in s for s in siblings[0].siblings)
    assert siblings[0].primary is False
    # A reverse patch on the warehouse table is also emitted.
    assert len(wus) == 2


def test_no_siblings_for_view() -> None:
    source = _source(warehouse_platform="postgres", warehouse_database="analytics")
    entity = CubeEntity(name="orders_view", is_view=True, cube_references=["orders"])
    assert (
        list(
            source._emit_siblings(
                entity,
                "urn:li:dataset:(urn:li:dataPlatform:cube,orders_view,PROD)",
                _lineage_builder(source),
            )
        )
        == []
    )


def test_view_definition_uses_sql_for_cube() -> None:
    source = _source()
    entity = CubeEntity(name="orders", sql="SELECT * FROM public.orders")
    view = source._view_definition(entity)
    assert view is not None
    assert view.viewLanguage == "SQL"
    assert view.viewLogic == "SELECT * FROM public.orders"
    assert view.materialized is False


def test_view_definition_reconstructs_includes_for_view() -> None:
    source = _source()
    entity = CubeEntity(
        name="orders_view",
        is_view=True,
        measures=[
            CubeMember(
                name="count", is_measure=True, member_references=["orders.count"]
            )
        ],
        dimensions=[
            CubeMember(
                name="status", is_measure=False, member_references=["orders.status"]
            )
        ],
    )
    view = source._view_definition(entity)
    assert view is not None
    assert view.viewLanguage == "YAML"
    assert view.viewLogic == "includes:\n  - orders.count\n  - orders.status"


def test_view_definition_none_for_cube_without_sql() -> None:
    assert _source()._view_definition(CubeEntity(name="orders")) is None


def test_hidden_cube_filtered_by_default() -> None:
    source = _source()
    hidden = CubeEntity(name="secret", is_hidden=True)
    visible = CubeEntity(name="orders")
    assert source._should_emit(hidden) is False
    assert source._should_emit(visible) is True

    assert _source(include_hidden=True)._should_emit(hidden) is True


def test_hidden_members_excluded_from_schema() -> None:
    source = _source()
    entity = CubeEntity(
        name="orders",
        measures=[CubeMember(name="count", is_measure=True, is_hidden=True)],
        dimensions=[CubeMember(name="status", is_measure=False)],
    )
    fields = source._build_schema_fields(entity)
    assert [f.fieldPath for f in fields] == ["status"]

    fields_all = _source(include_hidden=True)._build_schema_fields(entity)
    assert {f.fieldPath for f in fields_all} == {"count", "status"}


def test_structural_metadata_as_custom_properties() -> None:
    from datahub.ingestion.source.cube.models import (
        CubeFolder,
        CubeHierarchy,
        CubeJoin,
    )

    source = _source()
    entity = CubeEntity(
        name="orders",
        file_name="cubes/orders.yml",
        joins=[CubeJoin(name="users", relationship="belongsTo")],
        hierarchies=[CubeHierarchy(name="geo", levels=["country", "city"])],
        folders=[CubeFolder(name="Core", members=["count", "status"])],
        pre_aggregation_names=["main_rollup"],
    )
    props = source._custom_properties(entity)
    assert props["file_name"] == "cubes/orders.yml"
    assert props["join.users"] == "belongsTo"
    assert props["hierarchy.geo"] == "country, city"
    assert props["folder.Core"] == "count, status"
    assert props["pre_aggregations"] == "main_rollup"


def test_measure_json_props() -> None:
    source = _source()
    field = source._build_schema_field(
        CubeMember(
            name="count",
            is_measure=True,
            format="percent",
            drill_members=["id", "status"],
            cumulative=True,
        )
    )
    assert field.jsonProps is not None
    assert '"format": "percent"' in field.jsonProps
    assert "drillMembers" in field.jsonProps

    plain = source._build_schema_field(CubeMember(name="status", is_measure=False))
    assert plain.jsonProps is None


def test_siblings_disabled() -> None:
    source = _source(
        warehouse_platform="postgres",
        warehouse_database="analytics",
        emit_siblings=False,
    )
    assert (
        list(
            source._emit_siblings(
                _one_to_one_entity(),
                "urn:li:dataset:(urn:li:dataPlatform:cube,orders,PROD)",
                _lineage_builder(source),
            )
        )
        == []
    )
