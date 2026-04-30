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
    element_name_to_eids: Dict[str, List[str]] | None = None,
    elementId_to_dataset_urn: Dict[str, str] | None = None,
    entity_level_upstream_urns: Set[str] | None = None,
    upstream_elements: List[SigmaDataModelElement] | None = None,
) -> list:
    all_elements = [element] + (upstream_elements or [])
    return source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=element_dataset_urn or _urn(element.elementId),
        element_name_to_eids=element_name_to_eids or {},
        elementId_to_dataset_urn=elementId_to_dataset_urn or {},
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
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": upstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["x"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(upstream_urn, "x")]
    assert lineages[0].downstreams == [
        builder.make_schema_field_urn(downstream_urn, "x")
    ]
    assert source.reporter.data_model_element_fgl_emitted == 1


def test_multi_ref_formula_emits_one_lineage_per_ref() -> None:
    source = _source()
    upstream_urn = _urn("a")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "Sum([A/p], [A/q])")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": upstream_urn},
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
    assert source.reporter.data_model_element_fgl_emitted == 0


def test_parameter_ref_is_skipped() -> None:
    source = _source()
    element = _element("b", "B", [_column("b-z", "z", "[P_Date_Range]")])

    assert _build(source, element) == []
    assert source.reporter.data_model_element_fgl_emitted == 0


def test_cross_dm_ref_is_counted_unresolved() -> None:
    source = _source()
    element = _element("b", "B", [_column("b-x", "x", "[OtherSource/y]")])

    assert _build(source, element) == []
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 1


def test_orphan_upstream_genuinely_dropped_when_lineage_api_gap_exists() -> None:
    # Element IS in this DM (found in element_name_to_eids) but /lineage does
    # not report it as an upstream (entity_level_upstream_urns is empty).
    # This is the rare case where /lineage genuinely omits an intra-DM edge.
    source = _source()
    upstream_urn = _urn("a")
    element = _element("b", "B", [_column("b-x", "x", "[A/x]")])

    assert (
        _build(
            source,
            element,
            element_name_to_eids={"a": ["a"]},
            elementId_to_dataset_urn={"a": upstream_urn},
            # entity_level_upstream_urns empty → /lineage API gap
        )
        == []
    )
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1


def test_element_name_collision_is_filtered_by_entity_level_upstreams() -> None:
    source = _source()
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
        element_name_to_eids={"random data model": ["rdm-1", "rdm-2"]},
        elementId_to_dataset_urn={"rdm-1": winner_urn, "rdm-2": loser_urn},
        entity_level_upstream_urns={winner_urn},
        upstream_elements=[
            _upstream_element("rdm-1", "random data model", ["c"]),
            _upstream_element("rdm-2", "random data model", ["c"]),
        ],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(winner_urn, "c")]


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
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": upstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["winner"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(upstream_urn, "winner")
    ]
    assert source.reporter.data_model_element_fgl_emitted == 1


def test_output_order_is_stable_for_shuffled_columns() -> None:
    upstream_a = _urn("a")
    upstream_c = _urn("c")
    downstream_urn = _urn("b")
    columns = [
        _column("b-y", "y", "[C/c]"),
        _column("b-x", "x", "Sum([A/q], [A/p])"),
    ]
    name_eids: Dict[str, List[str]] = {"a": ["a"], "c": ["c"]}
    eid_to_urn: Dict[str, str] = {"a": upstream_a, "c": upstream_c}
    upstream_urns: Set[str] = {upstream_a, upstream_c}
    upstream_els = [
        _upstream_element("a", "A", ["p", "q"]),
        _upstream_element("c", "C", ["c"]),
    ]

    first = _build(
        _source(),
        _element("b", "B", columns),
        element_dataset_urn=downstream_urn,
        element_name_to_eids=name_eids,
        elementId_to_dataset_urn=eid_to_urn,
        entity_level_upstream_urns=upstream_urns,
        upstream_elements=upstream_els,
    )
    second = _build(
        _source(),
        _element("b", "B", list(reversed(columns))),
        element_dataset_urn=downstream_urn,
        element_name_to_eids=name_eids,
        elementId_to_dataset_urn=eid_to_urn,
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
    assert source.reporter.data_model_element_fgl_emitted == 0


def test_case_insensitive_element_name_lookup() -> None:
    source = _source()
    upstream_urn = _urn("orders")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "[Orders/revenue]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"orders": ["orders-el"]},
        elementId_to_dataset_urn={"orders-el": upstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("orders-el", "orders", ["revenue"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(upstream_urn, "revenue")
    ]


def test_duplicate_refs_in_formula_are_deduplicated() -> None:
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
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": upstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["x"])],
    )

    assert len(lineages) == 1
    assert source.reporter.data_model_element_fgl_emitted == 1


def test_unknown_upstream_column_is_dropped() -> None:
    source = _source()
    upstream_urn = _urn("a")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "[A/nonexistent]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": upstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["x"])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_unknown_upstream_column == 1


def test_duplicate_element_names_different_schemas_validates_correct_element() -> None:
    source = _source()
    orders_a_urn = _urn("orders-a")
    orders_b_urn = _urn("orders-b")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "[orders/amount]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"orders": ["orders-a", "orders-b"]},
        elementId_to_dataset_urn={"orders-a": orders_a_urn, "orders-b": orders_b_urn},
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
    assert source.reporter.data_model_element_fgl_emitted == 1


def test_duplicate_element_names_surviving_element_lacks_column() -> None:
    source = _source()
    orders_a_urn = _urn("orders-a")
    orders_b_urn = _urn("orders-b")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-x", "x", "[orders/revenue]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"orders": ["orders-a", "orders-b"]},
        elementId_to_dataset_urn={"orders-a": orders_a_urn, "orders-b": orders_b_urn},
        entity_level_upstream_urns={orders_a_urn},
        upstream_elements=[
            _upstream_element("orders-a", "orders", ["amount"]),
            _upstream_element("orders-b", "orders", ["revenue"]),
        ],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_unknown_upstream_column == 1


def test_self_reference_is_warehouse_passthrough_deferred() -> None:
    """Element named X with formula [X/col] is a warehouse-passthrough, not intra-DM.

    The element's name matches the underlying warehouse table name (a common Sigma
    authoring pattern).  The resolver must detect the self-reference and increment
    fgl_warehouse_passthrough_deferred rather than emitting self-referential FGL.
    """
    source = _source()
    self_urn = _urn("data.csv")
    warehouse_urn = _urn("snowflake-inode")
    element = _element(
        "elem-data-csv",
        "data.csv",
        [_column("c1", "city", "[data.csv/city]")],
    )

    lineages = _build(
        source,
        element,
        element_dataset_urn=self_urn,
        element_name_to_eids={"data.csv": ["elem-data-csv"]},
        elementId_to_dataset_urn={"elem-data-csv": self_urn},
        # /lineage reports the warehouse inode as upstream, not the element itself
        entity_level_upstream_urns={warehouse_urn},
        upstream_elements=[_upstream_element("elem-data-csv", "data.csv", ["city"])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 1
    assert source.reporter.data_model_element_fgl_emitted == 0
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 0
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0


def test_name_collision_picks_first_sorted_urn() -> None:
    """Two siblings share a name and both pass the /lineage filter.

    The resolver picks sorted(surviving_urns)[0], matching T2 PR1's collision
    precedent and Sigma's server-side coalescing.  fgl_collision_pick_first
    is incremented once per ref that triggers this path.
    """
    source = _source()
    # URN for "elem-aaa" sorts before URN for "elem-zzz" lexicographically
    urn_aaa = _urn("aaa")
    urn_zzz = _urn("zzz")
    downstream_urn = _urn("b")
    element = _element(
        "b",
        "B",
        [_column("b-x", "x", "[shared name/team1]")],
    )

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"shared name": ["elem-aaa", "elem-zzz"]},
        elementId_to_dataset_urn={"elem-aaa": urn_aaa, "elem-zzz": urn_zzz},
        entity_level_upstream_urns={urn_aaa, urn_zzz},
        upstream_elements=[
            _upstream_element("elem-aaa", "shared name", ["team1"]),
            _upstream_element("elem-zzz", "shared name", ["team1"]),
        ],
    )

    assert len(lineages) == 1
    # sorted([urn_aaa, urn_zzz])[0] == urn_aaa since "aaa" < "zzz"
    assert lineages[0].upstreams == [builder.make_schema_field_urn(urn_aaa, "team1")]
    assert source.reporter.data_model_element_fgl_collision_pick_first == 1
    assert source.reporter.data_model_element_fgl_emitted == 1
