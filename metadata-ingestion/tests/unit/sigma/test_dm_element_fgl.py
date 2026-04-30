import datetime as dt
from typing import Dict, List, Set

from datahub.emitter import mce_builder as builder
from datahub.ingestion.source.sigma.config import SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import (
    SigmaDataModel,
    SigmaDataModelColumn,
    SigmaDataModelElement,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource


def _source() -> SigmaSource:
    source = SigmaSource.__new__(SigmaSource)
    source.reporter = SigmaSourceReport()
    return source


def _urn(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:sigma,{name},PROD)"


def _column(column_id: str, name: str, formula: str | None) -> SigmaDataModelColumn:
    return SigmaDataModelColumn(columnId=column_id, name=name, formula=formula)


def _element(
    element_id: str,
    name: str,
    columns: List[SigmaDataModelColumn],
    source_ids: List[str] | None = None,
) -> SigmaDataModelElement:
    # The model_validator on SigmaDataModelElement discards non-dict columns
    # (mimicking the /elements API which returns bare strings). Pass dicts so
    # the validator keeps them and pydantic coerces them back to model objects.
    return SigmaDataModelElement(
        elementId=element_id,
        name=name,
        columns=[c.model_dump() for c in columns],
        source_ids=source_ids or [],
    )


def _upstream_element(
    element_id: str,
    name: str,
    col_names: List[str],
) -> SigmaDataModelElement:
    """Minimal upstream element with no-formula columns for canonical col lookup."""
    return _element(
        element_id,
        name,
        [_column(f"{element_id}-{c}", c, None) for c in col_names],
    )


def _data_model(elements: List[SigmaDataModelElement]) -> SigmaDataModel:
    now = dt.datetime.now(dt.timezone.utc)
    return SigmaDataModel(
        dataModelId="dm-1",
        name="DM",
        createdAt=now,
        updatedAt=now,
        elements=elements,
    )


def _build(
    source: SigmaSource,
    element: SigmaDataModelElement,
    *,
    element_dataset_urn: str | None = None,
    element_name_to_dataset_urns: Dict[str, List[str]] | None = None,
    entity_level_upstream_urns: Set[str] | None = None,
    upstream_elements: List[SigmaDataModelElement] | None = None,
) -> list:
    all_elements = [element] + (upstream_elements or [])
    return source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=element_dataset_urn or _urn(element.elementId),
        element_name_to_dataset_urns=element_name_to_dataset_urns or {},
        entity_level_upstream_urns=entity_level_upstream_urns or set(),
        data_model=_data_model(all_elements),
    )


def test_trivial_passthrough_resolves() -> None:
    source = _source()
    upstream_urn = _urn("a")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "[A/x]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns={"a": [upstream_urn]},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["x"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(upstream_urn, "x")]
    assert lineages[0].downstreams == [
        builder.make_schema_field_urn(downstream_urn, "x")
    ]
    assert source.reporter.data_model_element_fgl_intra_resolved == 1
    assert source.reporter.data_model_element_fgl_emitted == 1
    assert source.reporter.data_model_element_columns_with_formula == 1


def test_multi_ref_formula_emits_one_lineage_per_ref() -> None:
    source = _source()
    upstream_urn = _urn("a")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "Sum([A/p], [A/q])")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns={"a": [upstream_urn]},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["p", "q"])],
    )

    assert [lineage.upstreams for lineage in lineages] == [
        [builder.make_schema_field_urn(upstream_urn, "p")],
        [builder.make_schema_field_urn(upstream_urn, "q")],
    ]
    assert [lineage.downstreams for lineage in lineages] == [
        [builder.make_schema_field_urn(downstream_urn, "x")],
        [builder.make_schema_field_urn(downstream_urn, "x")],
    ]


def test_bare_sibling_ref_is_skipped() -> None:
    source = _source()
    element = _element("b", "B", [_column("b-y", "y", "[B_other_col]")])

    assert _build(source, element) == []
    assert source.reporter.data_model_element_fgl_sibling_skipped == 1


def test_parameter_ref_is_skipped() -> None:
    source = _source()
    element = _element("b", "B", [_column("b-z", "z", "[P_Date_Range]")])

    assert _build(source, element) == []
    assert source.reporter.data_model_element_fgl_param_skipped == 1


def test_cross_dm_ref_is_counted_unresolved() -> None:
    source = _source()
    element = _element("b", "B", [_column("b-x", "x", "[OtherSource/y]")])

    assert _build(source, element) == []
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 1


def test_orphan_upstream_is_dropped() -> None:
    source = _source()
    upstream_urn = _urn("a")
    element = _element("b", "B", [_column("b-x", "x", "[A/x]")])

    assert (
        _build(source, element, element_name_to_dataset_urns={"a": [upstream_urn]})
        == []
    )
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1


def test_element_name_collision_is_filtered_by_entity_level_upstreams() -> None:
    source = _source()
    # winner_urn and loser_urn are listed in element order so urn_to_cols can
    # correctly pair each URN with its element's schema.
    winner_urn = _urn("rdm-1")
    loser_urn = _urn("rdm-2")
    downstream_urn = _urn("b")
    element = _element(
        "b",
        "B",
        [_column("b-x", "x", "[random data model/c]")],
        source_ids=["rdm-1"],
    )

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        # URN order matches upstream_elements order (rdm-1 first, rdm-2 second).
        element_name_to_dataset_urns={"random data model": [winner_urn, loser_urn]},
        entity_level_upstream_urns={winner_urn},
        upstream_elements=[
            _upstream_element("rdm-1", "random data model", ["c"]),
            _upstream_element("rdm-2", "random data model", ["c"]),
        ],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(winner_urn, "c")]


def test_duplicate_element_names_different_schemas_validates_correct_element() -> None:
    # Two elements share the name "orders" but have different schemas.
    # The surviving upstream (orders-a) has "amount"; the non-surviving (orders-b)
    # has "revenue".  urn_to_cols must pair each URN with its own element's schema
    # so the wrong column set isn't used for validation.
    source = _source()
    orders_a_urn = _urn("orders-a")
    orders_b_urn = _urn("orders-b")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "[orders/amount]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns={"orders": [orders_a_urn, orders_b_urn]},
        entity_level_upstream_urns={orders_a_urn},
        upstream_elements=[
            _upstream_element("orders-a", "orders", ["amount"]),
            _upstream_element("orders-b", "orders", ["revenue"]),
        ],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(orders_a_urn, "amount")
    ]
    assert source.reporter.data_model_element_fgl_intra_resolved == 1


def test_duplicate_element_names_surviving_element_lacks_column() -> None:
    # The surviving upstream element does NOT have the referenced column.
    # FGL must be dropped to avoid a dangling schemaField URN, even though
    # the non-surviving element does have that column.
    source = _source()
    orders_a_urn = _urn("orders-a")
    orders_b_urn = _urn("orders-b")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "[orders/revenue]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns={"orders": [orders_a_urn, orders_b_urn]},
        entity_level_upstream_urns={orders_a_urn},
        upstream_elements=[
            _upstream_element("orders-a", "orders", ["amount"]),
            _upstream_element("orders-b", "orders", ["revenue"]),
        ],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_unknown_upstream_column == 1


def test_dedup_loser_formula_is_dropped() -> None:
    source = _source()
    upstream_urn = _urn("a")
    downstream_urn = _urn("b")
    element = _element(
        "b",
        "B",
        [
            _column("col-1", "x", "[A/winner]"),
            _column("col-2", "x", "[A/loser]"),
        ],
    )

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns={"a": [upstream_urn]},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["winner"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(upstream_urn, "winner")
    ]
    assert source.reporter.data_model_element_fgl_dropped_dedup_loser == 1


def test_output_order_is_stable_for_shuffled_columns() -> None:
    upstream_a = _urn("a")
    upstream_c = _urn("c")
    downstream_urn = _urn("b")
    columns = [
        _column("b-y", "y", "[C/c]"),
        _column("b-x", "x", "Sum([A/q], [A/p])"),
    ]
    name_map: Dict[str, List[str]] = {"a": [upstream_a], "c": [upstream_c]}
    upstream_urns: Set[str] = {upstream_a, upstream_c}
    upstream_els = [
        _upstream_element("a", "A", ["p", "q"]),
        _upstream_element("c", "C", ["c"]),
    ]

    first = _build(
        _source(),
        _element("b", "B", columns),
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns=name_map,
        entity_level_upstream_urns=upstream_urns,
        upstream_elements=upstream_els,
    )
    second = _build(
        _source(),
        _element("b", "B", list(reversed(columns))),
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns=name_map,
        entity_level_upstream_urns=upstream_urns,
        upstream_elements=upstream_els,
    )

    assert first == second
    assert [(lineage.downstreams[0], lineage.upstreams[0]) for lineage in first] == [
        (
            builder.make_schema_field_urn(downstream_urn, "x"),
            builder.make_schema_field_urn(upstream_a, "p"),
        ),
        (
            builder.make_schema_field_urn(downstream_urn, "x"),
            builder.make_schema_field_urn(upstream_a, "q"),
        ),
        (
            builder.make_schema_field_urn(downstream_urn, "y"),
            builder.make_schema_field_urn(upstream_c, "c"),
        ),
    ]


def test_quoted_bracket_literal_does_not_emit_fgl() -> None:
    source = _source()
    element = _element("b", "B", [_column("b-x", "x", 'If([status]="[FAILED]", 1, 0)')])

    assert _build(source, element) == []
    assert source.reporter.data_model_element_fgl_sibling_skipped == 1


def test_case_insensitive_element_name_lookup() -> None:
    # Formula uses "Orders" (mixed case); element is named "orders" (lowercase).
    # element_name_to_dataset_urns is keyed lowercase, and ref.source is lowercased
    # before lookup — so the ref must resolve despite the mismatch.
    source = _source()
    upstream_urn = _urn("orders")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "[Orders/revenue]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns={"orders": [upstream_urn]},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("orders-el", "orders", ["revenue"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(upstream_urn, "revenue")
    ]


def test_duplicate_refs_in_formula_are_deduplicated() -> None:
    # If([A/x] = 0, [A/x], [A/x] / 2) has three textual occurrences of [A/x].
    # Only one FGL entry should be emitted, not three.
    source = _source()
    upstream_urn = _urn("a")
    downstream_urn = _urn("b")
    element = _element(
        "b", "B", [_column("b-x", "x", "If([A/x] = 0, [A/x], [A/x] / 2)")]
    )

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns={"a": [upstream_urn]},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["x"])],
    )

    assert len(lineages) == 1
    assert source.reporter.data_model_element_fgl_intra_resolved == 1
    assert source.reporter.data_model_element_fgl_emitted == 1


def test_unknown_upstream_column_is_dropped() -> None:
    # Formula references [A/nonexistent] but upstream element A only has column "x".
    # FGL must be dropped to avoid a dangling schemaField URN.
    source = _source()
    upstream_urn = _urn("a")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "[A/nonexistent]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_dataset_urns={"a": [upstream_urn]},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["x"])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_unknown_upstream_column == 1
