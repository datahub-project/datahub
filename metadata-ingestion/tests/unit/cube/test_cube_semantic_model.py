from typing import Dict, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.cube.config import CubeSourceConfig, CubeSourceReport
from datahub.ingestion.source.cube.cube_semantic_model import (
    CubeSemanticModelMapper,
    parse_cube_join_sql,
)
from datahub.ingestion.source.cube.models import (
    CubeEntity,
    CubeJoin,
    CubeMember,
)
from datahub.metadata.schema_classes import (
    ERModelRelationshipCardinalityClass,
    MetricInfoClass,
    NumberTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    StringTypeClass,
)


def _config(**overrides: object) -> CubeSourceConfig:
    base: Dict[str, object] = {
        "api_url": "https://demo.cubecloud.dev/cubejs-api",
        "api_token": "t",
        "emit_semantic_model_entities": True,
    }
    base.update(overrides)
    return CubeSourceConfig.model_validate(base)


def _mapper(**overrides: object) -> CubeSemanticModelMapper:
    config = _config(**overrides)
    return CubeSemanticModelMapper(
        config=config,
        path="demo",
        cube_dataset_urn_fn=lambda name: (
            f"urn:li:dataset:(urn:li:dataPlatform:cube,{name},PROD)"
        ),
        container_urn="urn:li:container:cube_demo",
        report=CubeSourceReport(),
    )


def _orders_cube() -> CubeEntity:
    return CubeEntity(
        name="orders",
        description="Customer orders",
        joins=[
            CubeJoin(
                name="customers",
                relationship="many_to_one",
                sql="{CUBE}.customer_id = {customers}.id",
            )
        ],
        measures=[
            CubeMember(name="count", is_measure=True, agg_type="count"),
            CubeMember(name="total_amount", is_measure=True, agg_type="sum"),
        ],
        dimensions=[
            CubeMember(name="id", is_measure=False, is_primary_key=True),
            CubeMember(name="status", is_measure=False),
            CubeMember(name="created_at", is_measure=False, is_temporal=True),
            CubeMember(name="customer_id", is_measure=False),
        ],
    )


def _customers_cube() -> CubeEntity:
    return CubeEntity(
        name="customers",
        description="Registered customers",
        dimensions=[
            CubeMember(name="id", is_measure=False, is_primary_key=True),
            CubeMember(name="name", is_measure=False),
            CubeMember(name="city", is_measure=False),
        ],
    )


def _orders_view() -> CubeEntity:
    return CubeEntity(
        name="orders_view",
        is_view=True,
        description="Query-ready orders",
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            ),
            CubeMember(
                name="total_amount",
                is_measure=True,
                agg_type="sum",
                member_references=["orders.total_amount"],
            ),
        ],
        dimensions=[
            CubeMember(
                name="status",
                is_measure=False,
                member_references=["orders.status"],
            ),
            CubeMember(
                name="created_at",
                is_measure=False,
                is_temporal=True,
                member_references=["orders.created_at"],
            ),
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            ),
            CubeMember(
                name="city",
                is_measure=False,
                member_references=["customers.city"],
            ),
        ],
    )


def _aspects_by_urn(wus: List[MetadataWorkUnit]) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    for wu in wus:
        urn = wu.get_urn()
        aspect = wu.metadata.aspect  # type: ignore[union-attr]
        if aspect is None:
            continue
        out.setdefault(urn, {})[type(aspect).__name__] = aspect
    return out


def test_parse_cube_join_sql() -> None:
    assert parse_cube_join_sql(None) == []
    assert parse_cube_join_sql("{CUBE}.customer_id = {customers}.id") == [
        ("customer_id", "customers", "id")
    ]
    assert parse_cube_join_sql("{customers}.id = {CUBE}.customer_id") == [
        ("customer_id", "customers", "id")
    ]
    assert parse_cube_join_sql("{CUBE}.a = {other}.b AND {CUBE}.c = {other}.d") == [
        ("a", "other", "b"),
        ("c", "other", "d"),
    ]
    assert parse_cube_join_sql("{other}.b = {CUBE}.a AND {CUBE}.c = {other}.d") == [
        ("a", "other", "b"),
        ("c", "other", "d"),
    ]


def test_view_emits_semantic_model_metrics_and_logical_datasets() -> None:
    mapper = _mapper()
    cubes = {"orders": _orders_cube(), "customers": _customers_cube()}
    aspects = _aspects_by_urn(list(mapper.emit(_orders_view(), cubes)))

    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    assert model_urn in aspects
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.name == "orders_view"
    assert info.datasets in (None, [])
    assert info.relationships
    rel = info.relationships[0]
    assert rel.from_ == "orders"
    assert rel.to == "customers"
    assert rel.fromColumns == ["customer_id"]
    assert rel.toColumns == ["id"]
    assert rel.cardinality == ERModelRelationshipCardinalityClass.N_ONE

    view_dataset = "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view,PROD)"
    assert view_dataset not in aspects

    orders_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.orders,PROD)"
    )
    customers_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.customers,PROD)"
    )
    assert orders_logical in aspects
    assert customers_logical in aspects
    assert aspects[orders_logical]["SubTypesClass"].typeNames == [  # type: ignore[attr-defined]
        DatasetSubTypes.SEMANTIC_MODEL_DATASET
    ]
    props = aspects[orders_logical]["SemanticModelPropertiesClass"]
    assert isinstance(props, SemanticModelPropertiesClass)
    assert props.alias == "orders"
    assert props.semanticModel == model_urn

    count_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.orders,count)"
    )
    amount_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.orders,total_amount)"
    )
    assert count_metric in aspects
    assert amount_metric in aspects
    metric_info = aspects[count_metric]["MetricInfoClass"]
    assert isinstance(metric_info, MetricInfoClass)
    assert metric_info.semanticModel == model_urn

    assert mapper.view_chart_inputs["orders_view"] == [
        orders_logical,
        customers_logical,
    ]
    assert mapper.report.semantic_models_emitted == 1
    assert mapper.report.metrics_emitted == 2
    assert mapper.report.semantic_model_datasets_emitted == 2


def test_cloud_measure_without_agg_type_gets_numeric_field_and_expression() -> None:
    # Regression: Cloud view measures often report the aggregation only via
    # `type` (e.g. "count") and leave `aggType` unset. The semantic-model
    # path used to pass that raw Cube type straight through as the field's
    # native type, which resolves to NullType downstream, and left the
    # metric with no expression since it only consulted `agg_type`.
    cube = CubeEntity(
        name="orders",
        measures=[CubeMember(name="count", is_measure=True, data_type="count")],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                data_type="count",
                member_references=["orders.count"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(list(mapper.emit(view, {"orders": cube})))

    orders_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.orders,PROD)"
    )
    schema_meta = aspects[orders_logical]["SchemaMetadataClass"]
    fields = schema_meta.fields  # type: ignore[attr-defined]
    count_field = next(f for f in fields if f.fieldPath == "count")
    assert isinstance(count_field.type.type, NumberTypeClass)

    count_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.orders,count)"
    )
    metric_info = aspects[count_metric]["MetricInfoClass"]
    assert isinstance(metric_info, MetricInfoClass)
    assert metric_info.expression is not None
    assert metric_info.expression.dialects[0].expression == "count(orders.count)"


def test_calculated_measure_with_primitive_type_keeps_its_own_type() -> None:
    # Regression: forcing every measure to "number" was too broad -- a
    # calculated measure can carry a real primitive type (e.g. a string
    # measure built from a CASE expression), which must pass through as-is
    # rather than being coerced to numeric, matching the classic dataset
    # path's CUBE_TYPE_TO_SCHEMA_FIELD_TYPE lookup.
    cube = CubeEntity(
        name="orders",
        measures=[CubeMember(name="status_label", is_measure=True, data_type="string")],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders"],
        measures=[
            CubeMember(
                name="status_label",
                is_measure=True,
                data_type="string",
                member_references=["orders.status_label"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(list(mapper.emit(view, {"orders": cube})))

    orders_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.orders,PROD)"
    )
    schema_meta = aspects[orders_logical]["SchemaMetadataClass"]
    fields = schema_meta.fields  # type: ignore[attr-defined]
    field = next(f for f in fields if f.fieldPath == "status_label")
    assert isinstance(field.type.type, StringTypeClass)

    metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.orders,status_label)"
    )
    metric_info = aspects[metric]["MetricInfoClass"]
    assert isinstance(metric_info, MetricInfoClass)
    assert metric_info.expression is None


def test_geo_dimension_maps_to_string_not_nulltype() -> None:
    # Regression: geo has no dedicated DataHub SQL-type primitive, so passing
    # it through as-is resolves to NullType downstream. The classic dataset
    # path already maps geo to StringType (CUBE_TYPE_TO_SCHEMA_FIELD_TYPE);
    # the semantic-model path must match, for both dimensions (geo's primary
    # use in Cube) and calculated geo measures.
    cube = CubeEntity(
        name="stores",
        measures=[
            CubeMember(name="centroid", is_measure=True, data_type="geo"),
        ],
        dimensions=[
            CubeMember(name="location", is_measure=False, data_type="geo"),
        ],
    )
    view = CubeEntity(
        name="stores_view",
        is_view=True,
        cube_references=["stores"],
        measures=[
            CubeMember(
                name="centroid",
                is_measure=True,
                data_type="geo",
                member_references=["stores.centroid"],
            )
        ],
        dimensions=[
            CubeMember(
                name="location",
                is_measure=False,
                data_type="geo",
                member_references=["stores.location"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(list(mapper.emit(view, {"stores": cube})))

    stores_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.stores_view.stores,PROD)"
    )
    schema_meta = aspects[stores_logical]["SchemaMetadataClass"]
    fields = schema_meta.fields  # type: ignore[attr-defined]
    location_field = next(f for f in fields if f.fieldPath == "location")
    centroid_field = next(f for f in fields if f.fieldPath == "centroid")
    assert isinstance(location_field.type.type, StringTypeClass)
    assert isinstance(centroid_field.type.type, StringTypeClass)


def _orders_cube_with_hidden_join_key() -> CubeEntity:
    cube = _orders_cube()
    cube.dimensions = [
        d
        if d.name != "customer_id"
        else CubeMember(name="customer_id", is_measure=False, is_hidden=True)
        for d in cube.dimensions
    ]
    return cube


def test_hidden_join_column_excluded_from_schema_when_include_hidden_false() -> None:
    # Regression: customer_id is a hidden dimension on `orders`, used only as
    # the join key to `customers`. _ensure_join_column used to pull it from
    # cube.members (unfiltered), leaking it into the schema regardless of
    # include_hidden. It must only appear via visible_members().
    mapper = _mapper(include_hidden=False)
    cubes = {
        "orders": _orders_cube_with_hidden_join_key(),
        "customers": _customers_cube(),
    }
    aspects = _aspects_by_urn(list(mapper.emit(_orders_view(), cubes)))

    orders_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.orders,PROD)"
    )
    schema_meta = aspects[orders_logical]["SchemaMetadataClass"]
    field_paths = [f.fieldPath for f in schema_meta.fields]  # type: ignore[attr-defined]
    assert "customer_id" not in field_paths

    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert not info.relationships


def test_hidden_join_column_included_when_include_hidden_true() -> None:
    mapper = _mapper(include_hidden=True)
    cubes = {
        "orders": _orders_cube_with_hidden_join_key(),
        "customers": _customers_cube(),
    }
    aspects = _aspects_by_urn(list(mapper.emit(_orders_view(), cubes)))

    orders_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.orders,PROD)"
    )
    schema_meta = aspects[orders_logical]["SchemaMetadataClass"]
    field_paths = [f.fieldPath for f in schema_meta.fields]  # type: ignore[attr-defined]
    assert "customer_id" in field_paths

    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.relationships
    rel = info.relationships[0]
    assert rel.fromColumns == ["customer_id"]


def test_core_view_without_cube_references_uses_alias_member() -> None:
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(list(mapper.emit(view, {"orders": _orders_cube()})))
    assert any(urn.startswith("urn:li:semanticModel:") for urn in aspects)
    assert any(urn.startswith("urn:li:metric:") for urn in aspects)
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view,PROD)" not in aspects
    )


def test_empty_view_does_not_emit_semantic_model() -> None:
    view = CubeEntity(name="empty_view", is_view=True)
    mapper = _mapper()
    wus = list(mapper.emit(view, {}))
    assert wus == []
    assert mapper.view_chart_inputs["empty_view"] == []
    assert mapper.report.semantic_models_emitted == 0


def test_same_named_measures_from_different_cubes_get_distinct_metric_urns() -> None:
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            ),
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                title="Customer count",
                member_references=["customers.count"],
            ),
        ],
    )
    cubes = {
        "orders": _orders_cube(),
        "customers": CubeEntity(
            name="customers",
            measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
            dimensions=[CubeMember(name="id", is_measure=False, is_primary_key=True)],
        ),
    }
    mapper = _mapper()
    aspects = _aspects_by_urn(list(mapper.emit(view, cubes)))
    orders_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.orders,count)"
    )
    customers_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.customers,count)"
    )
    assert orders_metric in aspects
    assert customers_metric in aspects
    customer_info = aspects[customers_metric]["MetricInfoClass"]
    assert isinstance(customer_info, MetricInfoClass)
    assert customer_info.name == "Customer count"


def test_reversed_join_sql_emits_relationship() -> None:
    cube = CubeEntity(
        name="orders",
        joins=[
            CubeJoin(
                name="customers",
                relationship="many_to_one",
                sql="{customers}.id = {CUBE}.customer_id",
            )
        ],
        measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
        dimensions=[CubeMember(name="customer_id", is_measure=False)],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(mapper.emit(view, {"orders": cube, "customers": _customers_cube()}))
    )
    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.relationships
    rel = info.relationships[0]
    assert rel.from_ == "orders"
    assert rel.to == "customers"
    assert rel.fromColumns == ["customer_id"]
    assert rel.toColumns == ["id"]


def test_join_matches_member_by_sql_column_not_just_name() -> None:
    # Regression: join SQL ("{CUBE}.user_id") names the underlying SQL column,
    # which commonly differs from the member's JS-identifier name in
    # camelCase schemas (e.g. a dimension named "userId" with `sql: user_id`).
    # _ensure_join_column used to match only by member.name, so this join
    # would silently resolve to no columns despite the member existing.
    cube = CubeEntity(
        name="orders",
        joins=[
            CubeJoin(
                name="customers",
                relationship="many_to_one",
                sql="{CUBE}.user_id = {customers}.id",
            )
        ],
        measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
        dimensions=[CubeMember(name="userId", is_measure=False, sql_column="user_id")],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(mapper.emit(view, {"orders": cube, "customers": _customers_cube()}))
    )
    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.relationships
    rel = info.relationships[0]
    # fromColumns must reference the schema field's actual field_path
    # (member.name), not the raw join SQL text -- the emitted logical
    # dataset's schema has a field named "userId", not "user_id".
    assert rel.fromColumns == ["userId"]
    assert rel.toColumns == ["id"]

    orders_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.orders,PROD)"
    )
    schema_meta = aspects[orders_logical]["SchemaMetadataClass"]
    field_paths = [f.fieldPath for f in schema_meta.fields]  # type: ignore[attr-defined]
    assert "userId" in field_paths


def test_unparsed_join_sql_is_warned_and_skipped() -> None:
    cube = CubeEntity(
        name="orders",
        joins=[
            CubeJoin(
                name="customers",
                relationship="many_to_one",
                sql="{CUBE}.customer_id > {customers}.id",
            )
        ],
        measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(mapper.emit(view, {"orders": cube, "customers": _customers_cube()}))
    )
    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert not info.relationships
    assert mapper.report.warnings


def test_view_referencing_missing_cube_skips_lineage_with_warning() -> None:
    # Regression: a view's cube_references naming a cube that isn't in
    # cubes_by_name (stale/renamed aliasMember, malformed API response) used to
    # still build a logical dataset with a fabricated upstream lineage edge to
    # that nonexistent cube, with zero warning.
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "missing_cube"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="region",
                is_measure=False,
                member_references=["missing_cube.region"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(list(mapper.emit(view, {"orders": _orders_cube()})))
    missing_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.missing_cube,PROD)"
    )
    assert missing_logical in aspects
    upstream = aspects[missing_logical].get("UpstreamLineageClass")
    assert upstream is None
    assert any(
        "Cube reference not found" in (w.title or "") for w in mapper.report.warnings
    )


def test_relationship_skipped_when_referenced_cube_not_found() -> None:
    # Regression: _relationships silently dropped a cube's joins when the cube
    # itself was missing from cubes_by_name, with no warning -- same root
    # cause as the logical-dataset case above, different code path. This
    # fixture deliberately gives "missing_cube" NO members (unlike the
    # logical-dataset test above): that way field_paths_by_cube["missing_cube"]
    # is empty, so _logical_datasets' `continue` on empty fields skips it
    # before ever reaching its own "cube is None" check -- isolating this test
    # to _relationships' independent iteration over referenced_cube_names().
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "missing_cube"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
    )
    mapper = _mapper()
    list(mapper.emit(view, {"orders": _orders_cube()}))
    assert any(
        "Cube reference not found" in (w.title or "") for w in mapper.report.warnings
    )


def test_duplicate_measure_name_within_cube_is_deduped_with_warning() -> None:
    # Regression: two measures with the same name from the SAME cube produced
    # two Metric objects with an identical URN (id=member.name); last write
    # won silently. The cross-cube alias suffix does not disambiguate this.
    # Also regression: an earlier version of this fix deduped only the Metric
    # objects (first-write-wins) while a separate, undeduped dict built the
    # schema field (last-write-wins) -- so the metric and its own logical
    # dataset's schema field reported two different descriptions for "the
    # same" field. Both must now agree (first-write-wins for both).
    cube = CubeEntity(
        name="orders",
        measures=[
            CubeMember(name="count", is_measure=True, agg_type="count"),
        ],
        dimensions=[CubeMember(name="id", is_measure=False, is_primary_key=True)],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                title="First count",
                member_references=["orders.count"],
            ),
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                title="Second count",
                member_references=["orders.count"],
            ),
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(list(mapper.emit(view, {"orders": cube})))
    count_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.orders,count)"
    )
    assert count_metric in aspects
    info = aspects[count_metric]["MetricInfoClass"]
    assert isinstance(info, MetricInfoClass)
    assert info.name == "First count"
    assert mapper.report.metrics_emitted == 1

    orders_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.orders,PROD)"
    )
    schema_meta = aspects[orders_logical]["SchemaMetadataClass"]
    count_fields = [f for f in schema_meta.fields if f.fieldPath == "count"]  # type: ignore[attr-defined]
    assert len(count_fields) == 1
    assert count_fields[0].description == "First count"

    assert any(
        "Duplicate Cube member name" in (w.title or "") for w in mapper.report.warnings
    )


def test_ambiguous_member_warns_and_falls_back_to_view_bucket() -> None:
    # Regression: a multi-cube view member with no resolvable source cube
    # (missing aliasMember) was silently bucketed under the view's own name
    # with no warning that it couldn't be attributed to a specific cube. Also
    # verifies the fallback actually happens (a mutation that instead dropped
    # the member entirely would still pass a warning-only assertion).
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(name="unattributed_field", is_measure=False),
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(
            mapper.emit(
                view, {"orders": _orders_cube(), "customers": _customers_cube()}
            )
        )
    )
    assert any(
        "Could not attribute Cube view member to a cube" in (w.title or "")
        for w in mapper.report.warnings
    )
    # No spurious "Cube reference not found" for the view's own fallback
    # bucket -- that's a distinct, already-warned-about situation.
    assert not any(
        "Cube reference not found" in (w.title or "") for w in mapper.report.warnings
    )
    fallback_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.orders_view,PROD)"
    )
    assert fallback_logical in aspects
    schema_meta = aspects[fallback_logical]["SchemaMetadataClass"]
    assert [f.fieldPath for f in schema_meta.fields] == [  # type: ignore[attr-defined]
        "unattributed_field"
    ]
    props = aspects[fallback_logical]["SemanticModelPropertiesClass"]
    assert props.alias == "orders_view"  # type: ignore[attr-defined]


def test_unrecognized_join_relationship_warns() -> None:
    cube = CubeEntity(
        name="orders",
        joins=[
            CubeJoin(
                name="customers",
                relationship="some_future_relationship_type",
                sql="{CUBE}.customer_id = {customers}.id",
            )
        ],
        measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
        dimensions=[CubeMember(name="customer_id", is_measure=False)],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(mapper.emit(view, {"orders": cube, "customers": _customers_cube()}))
    )
    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.relationships
    assert info.relationships[0].cardinality is None
    assert any(
        "Unrecognized Cube join relationship" in (w.title or "")
        for w in mapper.report.warnings
    )


def test_join_with_unresolvable_column_warns_and_skips() -> None:
    # Regression: SQL that parses fine but names a column that doesn't exist
    # on either cube (e.g. a typo) silently dropped the whole relationship,
    # unlike the sibling "could not parse" case which already warned.
    cube = CubeEntity(
        name="orders",
        joins=[
            CubeJoin(
                name="customers",
                relationship="many_to_one",
                sql="{CUBE}.typo_col = {customers}.id",
            )
        ],
        measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(mapper.emit(view, {"orders": cube, "customers": _customers_cube()}))
    )
    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert not info.relationships
    assert any(
        "Could not resolve Cube join columns" in (w.title or "")
        for w in mapper.report.warnings
    )


def test_emit_increments_measures_and_dimensions_scanned() -> None:
    # Regression: _emit_entity increments report.measures_scanned/
    # dimensions_scanned; the semantic-model mapper's emit() did not, silently
    # zeroing these stats for views whenever the flag was enabled.
    mapper = _mapper()
    cubes = {"orders": _orders_cube(), "customers": _customers_cube()}
    list(mapper.emit(_orders_view(), cubes))
    assert mapper.report.measures_scanned == 2
    assert mapper.report.dimensions_scanned == 4


def test_join_without_sql_is_skipped() -> None:
    cube = CubeEntity(
        name="orders",
        joins=[CubeJoin(name="customers", relationship="many_to_one")],
        measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(mapper.emit(view, {"orders": cube, "customers": _customers_cube()}))
    )
    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert not info.relationships
