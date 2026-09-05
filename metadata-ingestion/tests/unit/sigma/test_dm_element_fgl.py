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
    source.dm_element_urn_by_name = {}
    source.dm_element_urn_to_cols = {}
    source._upstream_schema_unavailable_warned = set()
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
        warehouse_url_id_map={},
        discovered_upstreams=set(),
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
    # The ref is parsed but never reaches a resolver, so the column falls
    # through to the no-resolvable-ref path (non-inode columnId).
    assert source.reporter.data_model_element_fgl_no_ref_unresolved == 1
    assert source.reporter.data_model_element_fgl_no_ref_warehouse_unresolved == 0


def test_parameter_ref_is_skipped() -> None:
    source = _source()
    element = _element("b", "B", [_column("b-z", "z", "[P_Date_Range]")])

    assert _build(source, element) == []
    assert source.reporter.data_model_element_fgl_emitted == 0
    assert source.reporter.data_model_element_fgl_no_ref_unresolved == 1
    assert source.reporter.data_model_element_fgl_no_ref_warehouse_unresolved == 0


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
    # The upstream HAS a schema; the column name simply is not in it. Must not
    # also land in the fetch-failure bucket.
    assert source.reporter.data_model_element_fgl_upstream_schema_unavailable == 0


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


def test_cross_dm_ref_resolves_via_source_scoped_index() -> None:
    """Bracket ref to an element absent from the current DM resolves when the
    element's source_ids point to the DM that owns the named element."""
    source = _source()
    dm_url_id = "other-dm"
    other_urn = _urn("other-dm-element")
    downstream_urn = _urn("elem-downstream")
    element = _element(
        "elem-downstream",
        "Downstream",
        [_column("c1", "city", "[other_dm_element/city]")],
        source_ids=[f"{dm_url_id}/some-suffix"],
    )
    source.dm_element_urn_by_name = {dm_url_id: {"other_dm_element": [other_urn]}}
    source.dm_element_urn_to_cols = {other_urn: {"city": "city", "date": "date"}}

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"downstream": ["elem-downstream"]},
        elementId_to_dataset_urn={"elem-downstream": downstream_urn},
        entity_level_upstream_urns={other_urn},
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(other_urn, "city")]
    assert lineages[0].downstreams == [
        builder.make_schema_field_urn(downstream_urn, "city")
    ]
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 1
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
    # Cross-DM FGL does not tick the intra-DM emit counter.
    assert source.reporter.data_model_element_fgl_emitted == 0


def test_cross_dm_ref_not_in_source_dm_increments_deferred() -> None:
    """Bracket ref to a name absent from the source DMs in element.source_ids
    increments cross_dm_deferred — even if that name exists in an unrelated DM."""
    source = _source()
    downstream_urn = _urn("elem-downstream")
    element = _element(
        "elem-downstream",
        "Downstream",
        [_column("c1", "city", "[unknown_thing/city]")],
        source_ids=["some-dm/suffix"],
    )
    # "unknown_thing" not in "some-dm"; exists only in "unrelated-dm" which
    # is not in source_ids — must not be linked.
    source.dm_element_urn_by_name = {
        "some-dm": {"some_dm_element": [_urn("some-dm-element")]},
        "unrelated-dm": {"unknown_thing": [_urn("unrelated-element")]},
    }
    source.dm_element_urn_to_cols = {_urn("some-dm-element"): {"col": "col"}}

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"downstream": ["elem-downstream"]},
        elementId_to_dataset_urn={"elem-downstream": downstream_urn},
        entity_level_upstream_urns={_urn("some-other-element")},
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 1
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 0


def test_cross_dm_collision_picks_first_sorted_urn() -> None:
    """Two source DMs share an element name; resolver picks sorted[0]."""
    source = _source()
    urn_aaa = _urn("aaa-dm-element")
    urn_zzz = _urn("zzz-dm-element")
    downstream_urn = _urn("elem-downstream")
    element = _element(
        "elem-downstream",
        "Downstream",
        [_column("c1", "col", "[shared_name/col]")],
        source_ids=["dm-zzz/s1", "dm-aaa/s2"],
    )
    source.dm_element_urn_by_name = {
        "dm-aaa": {"shared_name": [urn_aaa]},
        "dm-zzz": {"shared_name": [urn_zzz]},
    }
    source.dm_element_urn_to_cols = {urn_aaa: {"col": "col"}, urn_zzz: {"col": "col"}}

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"downstream": ["elem-downstream"]},
        elementId_to_dataset_urn={"elem-downstream": downstream_urn},
        entity_level_upstream_urns={urn_aaa, urn_zzz},
    )

    assert len(lineages) == 1
    # sorted([urn_aaa, urn_zzz])[0] == urn_aaa since "aaa" < "zzz"
    assert lineages[0].upstreams == [builder.make_schema_field_urn(urn_aaa, "col")]
    assert source.reporter.data_model_element_fgl_cross_dm_collision_pick_first == 1
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 1


def test_cross_dm_collision_entity_level_breaks_tie() -> None:
    """When exactly one collision candidate is a confirmed entity-level upstream,
    it wins without incrementing the collision counter."""
    source = _source()
    urn_correct = _urn("correct-dm-element")
    urn_other = _urn("other-dm-element")
    downstream_urn = _urn("elem-downstream")
    element = _element(
        "elem-downstream",
        "Downstream",
        [_column("c1", "col", "[shared_name/col]")],
        source_ids=["dm-correct/s1", "dm-other/s2"],
    )
    source.dm_element_urn_by_name = {
        "dm-correct": {"shared_name": [urn_correct]},
        "dm-other": {"shared_name": [urn_other]},
    }
    source.dm_element_urn_to_cols = {
        urn_correct: {"col": "col"},
        urn_other: {"col": "col"},
    }

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"downstream": ["elem-downstream"]},
        elementId_to_dataset_urn={"elem-downstream": downstream_urn},
        entity_level_upstream_urns={urn_correct},
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(urn_correct, "col")]
    assert source.reporter.data_model_element_fgl_cross_dm_collision_pick_first == 0
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 1


def test_cross_dm_singleton_not_in_entity_level_upstreams_still_emits() -> None:
    """A singleton cross-DM candidate not in entity_level_upstream_urns still
    emits FGL — Sigma's /lineage API does not always surface cross-DM formula
    dependencies at the entity level."""
    source = _source()
    dm_url_id = "other-dm"
    upstream_urn = _urn("other-dm-element")
    downstream_urn = _urn("elem-downstream")
    element = _element(
        "elem-downstream",
        "Downstream",
        [_column("c1", "city", "[other_dm_element/city]")],
        source_ids=[f"{dm_url_id}/suffix"],
    )
    source.dm_element_urn_by_name = {dm_url_id: {"other_dm_element": [upstream_urn]}}
    source.dm_element_urn_to_cols = {upstream_urn: {"city": "city"}}

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"downstream": ["elem-downstream"]},
        elementId_to_dataset_urn={"elem-downstream": downstream_urn},
        entity_level_upstream_urns={_urn("some-warehouse-table")},
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(upstream_urn, "city")
    ]
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 1
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0


def test_cross_dm_unknown_upstream_column_is_dropped() -> None:
    """Formula ref column absent from the resolved upstream element's schema
    increments cross_dm_dropped_unknown_upstream_column and emits no FGL."""
    source = _source()
    dm_url_id = "other-dm"
    upstream_urn = _urn("other-dm-element")
    downstream_urn = _urn("elem-downstream")
    element = _element(
        "elem-downstream",
        "Downstream",
        [_column("c1", "missing_col", "[other_dm_element/missing_col]")],
        source_ids=[f"{dm_url_id}/suffix"],
    )
    source.dm_element_urn_by_name = {dm_url_id: {"other_dm_element": [upstream_urn]}}
    # Upstream schema has "city" and "date" but NOT "missing_col".
    source.dm_element_urn_to_cols = {upstream_urn: {"city": "city", "date": "date"}}

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"downstream": ["elem-downstream"]},
        elementId_to_dataset_urn={"elem-downstream": downstream_urn},
        entity_level_upstream_urns={upstream_urn},
    )

    assert lineages == []
    assert (
        source.reporter.data_model_element_fgl_cross_dm_dropped_unknown_upstream_column
        == 1
    )
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 0
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
    # Producer schema is non-empty, so the fetch-failure bucket must stay clear.
    assert (
        source.reporter.data_model_element_fgl_cross_dm_upstream_schema_unavailable == 0
    )


def test_self_named_cross_dm_element_resolves_fgl() -> None:
    """Element named 'Custom SQL' in DM A with formula [Custom SQL/col] and
    source_ids pointing to DM B resolves FGL against DM B's 'Custom SQL' element.

    Without the fix the self-name-only branch goes straight to warehouse passthrough
    and emits 0 FGLs. With the fix, cross-DM is tried first and succeeds because
    DM B has a matching element name and column. Mirrors dev-tenant element YqPcfY1MZm.
    """
    source = _source()
    dm_b_url_id = "dm-b"
    producer_urn = _urn("producer-custom-sql")
    consumer_urn = _urn("consumer-custom-sql")

    consumer = _element(
        "consumer-eid",
        "Custom SQL",
        [_column("c1", "Visit Id", "[Custom SQL/Visit Id]")],
        source_ids=[f"{dm_b_url_id}/s1qt_Ccng5"],
    )

    source.dm_element_urn_by_name = {dm_b_url_id: {"custom sql": [producer_urn]}}
    source.dm_element_urn_to_cols = {producer_urn: {"visit id": "Visit Id"}}

    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        element_name_to_eids={"custom sql": ["consumer-eid"]},
        elementId_to_dataset_urn={"consumer-eid": consumer_urn},
        entity_level_upstream_urns={producer_urn},
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(producer_urn, "Visit Id")
    ]
    assert lineages[0].downstreams == [
        builder.make_schema_field_urn(consumer_urn, "Visit Id")
    ]
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 1
    assert source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0


def test_self_named_warehouse_element_unaffected_without_cross_dm_sources() -> None:
    """Regression: self-named element with no cross-DM source_ids still defers
    to warehouse passthrough. The cross-DM probe is guarded by source_ids so
    warehouse-only elements are unaffected and cross_dm_deferred is not inflated.
    """
    source = _source()
    self_urn = _urn("customers")
    element = _element(
        "elem-customers",
        "CUSTOMERS",
        [_column("c1", "id", "[CUSTOMERS/id]")],
        # source_ids=[] — no cross-DM refs, warehouse-only element
    )

    lineages = _build(
        source,
        element,
        element_dataset_urn=self_urn,
        element_name_to_eids={"customers": ["elem-customers"]},
        elementId_to_dataset_urn={"elem-customers": self_urn},
        entity_level_upstream_urns=set(),
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 1
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 0


def test_orphan_branch_rescued_by_cross_dm_on_name_collision() -> None:
    """When a sibling shares the consumer's formula-ref name but isn't in /lineage
    upstreams, the orphan branch tries cross-DM before dropping.

    Mirrors dev-tenant: DM 'Test Data Model' has TWO elements named 'Custom SQL'
    (sibling XpQ7V2hYt6 and consumer YqPcfY1MZm). YqPcfY1MZm's formula
    [Custom SQL/<col>] finds XpQ7V2hYt6 as intra-DM candidate (after self-strip),
    but XpQ7V2hYt6 is NOT in entity_level_upstream_urns. Old code: orphan drop.
    New code: cross-DM rescue succeeds via source_ids → DM B's 'Custom SQL'.
    """
    source = _source()
    dm_b_url_id = "dm-b"
    producer_urn = _urn("producer-custom-sql")
    sibling_urn = _urn("sibling-custom-sql")
    consumer_urn = _urn("consumer-custom-sql")

    consumer = _element(
        "consumer-eid",
        "Custom SQL",
        [_column("c1", "Visit Id", "[Custom SQL/Visit Id]")],
        source_ids=[f"{dm_b_url_id}/s1qt_Ccng5"],
    )
    sibling = _upstream_element("sibling-eid", "Custom SQL", ["Visit Id"])

    source.dm_element_urn_by_name = {dm_b_url_id: {"custom sql": [producer_urn]}}
    source.dm_element_urn_to_cols = {producer_urn: {"visit id": "Visit Id"}}

    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        # Both sibling and consumer share the name "Custom SQL" in this DM.
        element_name_to_eids={"custom sql": ["sibling-eid", "consumer-eid"]},
        elementId_to_dataset_urn={
            "sibling-eid": sibling_urn,
            "consumer-eid": consumer_urn,
        },
        # Entity-level upstream is cross-DM producer, NOT sibling.
        entity_level_upstream_urns={producer_urn},
        upstream_elements=[sibling],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(producer_urn, "Visit Id")
    ]
    assert lineages[0].downstreams == [
        builder.make_schema_field_urn(consumer_urn, "Visit Id")
    ]
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 1
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 0
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0


def test_orphan_branch_not_rescued_without_cross_dm_sources() -> None:
    """Regression: name collision with no cross-DM source_ids still hits orphan drop.
    The rescue guard (element.source_ids) prevents false-positive cross_dm_deferred
    for genuine orphans.
    """
    source = _source()
    sibling_urn = _urn("sibling")
    consumer_urn = _urn("consumer")

    consumer = _element(
        "consumer-eid",
        "Shared",
        [_column("c1", "x", "[Shared/x]")],
        # source_ids=[] — no cross-DM refs
    )
    sibling = _upstream_element("sibling-eid", "Shared", ["x"])

    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        element_name_to_eids={"shared": ["sibling-eid", "consumer-eid"]},
        elementId_to_dataset_urn={
            "sibling-eid": sibling_urn,
            "consumer-eid": consumer_urn,
        },
        # sibling not in entity_level_upstream_urns → genuine orphan
        entity_level_upstream_urns=set(),
        upstream_elements=[sibling],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 0


def test_intra_dm_only_source_ids_not_treated_as_cross_dm() -> None:
    """Regression: source_ids containing only bare intra-DM element IDs (no '/')
    must not trigger a cross-DM probe and must not inflate cross_dm_deferred.

    Covers Case A (orphan-drop branch): a sibling shares the name but isn't a
    lineage upstream, so surviving_urns is empty and the orphan-drop path fires.
    Case B (self-named strip branch) is covered in
    test_self_named_intra_dm_source_ids_not_treated_as_cross_dm below.
    """
    source = _source()
    sibling_urn = _urn("sibling")
    consumer_urn = _urn("consumer")

    consumer = _element(
        "consumer-eid",
        "Shared",
        [_column("c1", "x", "[Shared/x]")],
        # Intra-DM source IDs only — no "/" separator, not cross-DM shaped.
        source_ids=["some-intra-dm-eid"],
    )
    sibling = _upstream_element("sibling-eid", "Shared", ["x"])

    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        element_name_to_eids={"shared": ["sibling-eid", "consumer-eid"]},
        elementId_to_dataset_urn={
            "sibling-eid": sibling_urn,
            "consumer-eid": consumer_urn,
        },
        entity_level_upstream_urns=set(),
        upstream_elements=[sibling],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 0


def test_self_named_intra_dm_source_ids_not_treated_as_cross_dm() -> None:
    """Case B: element is the sole intra-DM candidate for its own name (self-named
    strip branch). After stripping itself, candidate_eids_after_self_strip is empty
    and _try_emit_self_named_cross_dm_fgl is called. When source_ids contains only
    bare intra-DM IDs, the guard must short-circuit without a cross-DM probe.
    Falls through to warehouse passthrough (deferred here — no warehouse FGL).
    """
    source = _source()
    consumer_urn = _urn("consumer")

    consumer = _element(
        "consumer-eid",
        "Orders",
        [_column("c1", "x", "[Orders/x]")],
        source_ids=["some-intra-dm-eid"],  # bare ID, no "/" — not cross-DM shaped
    )

    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        # Only the element itself under "orders"; after self-strip the list is empty.
        element_name_to_eids={"orders": ["consumer-eid"]},
        elementId_to_dataset_urn={"consumer-eid": consumer_urn},
        entity_level_upstream_urns=set(),
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 0
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 0
    assert source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 1


def test_inode_source_ids_excluded_from_cross_dm_guard() -> None:
    """inode-<urlId>/<suffix> shaped source_ids must not pass the cross-DM guard
    even though they contain '/'. Only <dm-url-id>/<suffix> entries (without the
    'inode-' prefix) qualify as cross-DM sources.
    """
    source = _source()
    consumer_urn = _urn("consumer")
    sibling_urn = _urn("sibling")

    consumer = _element(
        "consumer-eid",
        "Shared",
        [_column("c1", "x", "[Shared/x]")],
        # inode-shaped entry has '/' but is NOT a cross-DM source ID.
        source_ids=["inode-abc123/some-suffix"],
    )
    sibling = _upstream_element("sibling-eid", "Shared", ["x"])

    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        element_name_to_eids={"shared": ["sibling-eid", "consumer-eid"]},
        elementId_to_dataset_urn={
            "sibling-eid": sibling_urn,
            "consumer-eid": consumer_urn,
        },
        entity_level_upstream_urns=set(),
        upstream_elements=[sibling],
    )

    # Sibling is not a lineage upstream and inode source_ids are not cross-DM;
    # orphan-drop fires without touching cross-DM counters.
    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 0


def test_empty_upstream_schema_is_counted_separately() -> None:
    """An upstream element with no columns is an API failure, not a name mismatch.

    One failed /columns fetch empties every element in a data model, so folding
    this into dropped_unknown_upstream_column hides the real cause.
    """
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
        upstream_elements=[_upstream_element("a", "A", [])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_upstream_schema_unavailable == 1
    assert source.reporter.data_model_element_fgl_dropped_unknown_upstream_column == 0


def test_cross_dm_empty_upstream_schema_is_counted_separately() -> None:
    """Cross-DM producer present in the bridge map but with an empty schema."""
    source = _source()
    dm_url_id = "other-dm"
    upstream_urn = _urn("other-dm-element")
    downstream_urn = _urn("elem-downstream")
    element = _element(
        "elem-downstream",
        "Downstream",
        [_column("c1", "city", "[other_dm_element/city]")],
        source_ids=[f"{dm_url_id}/suffix"],
    )
    source.dm_element_urn_by_name = {dm_url_id: {"other_dm_element": [upstream_urn]}}
    source.dm_element_urn_to_cols = {upstream_urn: {}}

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"downstream": ["elem-downstream"]},
        elementId_to_dataset_urn={"elem-downstream": downstream_urn},
        entity_level_upstream_urns={upstream_urn},
    )

    assert lineages == []
    assert (
        source.reporter.data_model_element_fgl_cross_dm_upstream_schema_unavailable == 1
    )
    # Intra-DM counter must not absorb a cross-DM producer.
    assert source.reporter.data_model_element_fgl_upstream_schema_unavailable == 0
    assert (
        source.reporter.data_model_element_fgl_cross_dm_dropped_unknown_upstream_column
        == 0
    )
    # A producer missing from the bridge map entirely stays on `deferred`.
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0


def test_cross_dm_absent_producer_stays_deferred() -> None:
    """Producer not in dm_element_urn_to_cols at all keeps the deferred counter."""
    source = _source()
    dm_url_id = "other-dm"
    upstream_urn = _urn("other-dm-element")
    downstream_urn = _urn("elem-downstream")
    element = _element(
        "elem-downstream",
        "Downstream",
        [_column("c1", "city", "[other_dm_element/city]")],
        source_ids=[f"{dm_url_id}/suffix"],
    )
    source.dm_element_urn_by_name = {dm_url_id: {"other_dm_element": [upstream_urn]}}
    source.dm_element_urn_to_cols = {}

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"downstream": ["elem-downstream"]},
        elementId_to_dataset_urn={"elem-downstream": downstream_urn},
        entity_level_upstream_urns={upstream_urn},
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 1
    assert (
        source.reporter.data_model_element_fgl_cross_dm_upstream_schema_unavailable == 0
    )


def test_formula_less_column_does_not_guess_intra_dm_upstream() -> None:
    """A formula-less column must never be name-matched against siblings.

    There is no bracket ref to resolve, so matching on column name alone would
    fabricate an edge to every same-named sibling -- both sides of a join.
    """
    source = _source()
    upstream_urn = _urn("a")
    downstream_urn = _urn("b")
    element = _element("b", "B", [_column("b-account-id", "Account Id", "")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"a": ["a"], "b": ["b"]},
        elementId_to_dataset_urn={"a": upstream_urn, "b": downstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["Account Id"])],
    )

    assert lineages == []
    # Non-inode columnId: nothing to resolve against, expected volume.
    assert source.reporter.data_model_element_fgl_no_ref_unresolved == 1
    assert source.reporter.data_model_element_fgl_no_ref_warehouse_unresolved == 0
    assert source.reporter.data_model_element_fgl_emitted == 0


def test_empty_upstream_schema_warns_once_per_upstream() -> None:
    """One empty upstream must not emit a warning per referencing column.

    A partial /columns abort leaves many refs pointing at the same empty
    element; the dedupe set is the only thing keeping that out of the report.
    """
    source = _source()
    upstream_urn = _urn("a")
    downstream_urn = _urn("b")
    element = _element(
        "b",
        "B",
        [
            _column("b-x", "x", "[A/x]"),
            _column("b-y", "y", "[A/y]"),
        ],
    )

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": upstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", [])],
    )

    assert lineages == []
    # Both columns counted, one warning.
    assert source.reporter.data_model_element_fgl_upstream_schema_unavailable == 2
    assert len(source.reporter.warnings) == 1


# ---------------------------------------------------------------------------
# Join-chain refs: [JoinElement/SourceElement/Column]
#
# Sigma encodes a column reached through a join this way, so the element that
# owns the column is the second-to-last segment. The legacy first-slash split
# picks the join element instead, which is a real sibling -- it resolves, then
# fails the column lookup, and the edge is silently dropped.
# ---------------------------------------------------------------------------


def test_join_chain_resolves_to_owning_element() -> None:
    """The reported shape: [WRK/WRK CA_DIM_DOCUMENTS/Account Id].

    An element named "WRK" also exists, so the first segment resolves to the
    wrong sibling. The edge must land on the owning element instead.
    """
    source = _source()
    join_urn = _urn("join")
    owner_urn = _urn("owner")
    downstream_urn = _urn("consumer")
    element = _element(
        "consumer",
        "Consumer",
        [
            _column(
                "c-account-id", "Account Id", "[WRK/WRK CA_DIM_DOCUMENTS/Account Id]"
            )
        ],
    )

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"wrk": ["join"], "wrk ca_dim_documents": ["owner"]},
        elementId_to_dataset_urn={"join": join_urn, "owner": owner_urn},
        entity_level_upstream_urns={join_urn, owner_urn},
        upstream_elements=[
            # The join element deliberately has a column that is NOT the target,
            # mirroring production: the first segment matches but the qualified
            # column name cannot exist there.
            _upstream_element("join", "WRK", ["Some Other Column"]),
            _upstream_element("owner", "WRK CA_DIM_DOCUMENTS", ["Account Id"]),
        ],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(owner_urn, "Account Id")
    ]
    assert source.reporter.data_model_element_fgl_join_chain_resolved == 1
    assert source.reporter.data_model_element_fgl_dropped_unknown_upstream_column == 0
    assert source.reporter.data_model_element_fgl_join_chain_unresolved == 0


def test_nested_join_chain_resolves_to_deepest_element() -> None:
    source = _source()
    owner_urn = _urn("e3")
    downstream_urn = _urn("consumer")
    element = _element("consumer", "Consumer", [_column("c-x", "x", "[E1/E2/E3/col]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"e3": ["e3"]},
        elementId_to_dataset_urn={"e3": owner_urn},
        entity_level_upstream_urns={owner_urn},
        upstream_elements=[_upstream_element("e3", "E3", ["col"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(owner_urn, "col")]
    assert source.reporter.data_model_element_fgl_join_chain_resolved == 1


def test_join_chain_on_self_named_consumer_still_resolves() -> None:
    """Consumer named E1 with formula [E1/E2/col].

    The legacy path treats the first segment as a self-reference and diverts to
    warehouse-passthrough, never looking at E2. Self-strip must be applied per
    candidate so the owning element is still reached.
    """
    source = _source()
    owner_urn = _urn("e2")
    downstream_urn = _urn("e1")
    element = _element("e1", "E1", [_column("e1-x", "x", "[E1/E2/col]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"e1": ["e1"], "e2": ["e2"]},
        elementId_to_dataset_urn={"e1": downstream_urn, "e2": owner_urn},
        entity_level_upstream_urns={owner_urn},
        upstream_elements=[_upstream_element("e2", "E2", ["col"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(owner_urn, "col")]
    assert source.reporter.data_model_element_fgl_join_chain_resolved == 1
    assert source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0


def test_join_chain_owning_element_in_another_dm_resolves_cross_dm() -> None:
    """E1 is a local sibling but E2 lives in a source data model.

    Committing to the first segment would keep the ref on the intra-DM path and
    drop it; each candidate must be tried intra-DM then cross-DM.
    """
    source = _source()
    dm_url_id = "other-dm"
    e1_urn = _urn("e1")
    e2_urn = _urn("other-dm-e2")
    downstream_urn = _urn("consumer")
    element = _element(
        "consumer",
        "Consumer",
        [_column("c-x", "x", "[E1/E2/col]")],
        source_ids=[f"{dm_url_id}/suffix"],
    )
    source.dm_element_urn_by_name = {dm_url_id: {"e2": [e2_urn]}}
    source.dm_element_urn_to_cols = {e2_urn: {"col": "col"}}

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"e1": ["e1"]},
        elementId_to_dataset_urn={"e1": e1_urn},
        entity_level_upstream_urns={e1_urn},
        upstream_elements=[_upstream_element("e1", "E1", ["unrelated"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(e2_urn, "col")]
    assert source.reporter.data_model_element_fgl_join_chain_resolved == 1
    assert source.reporter.data_model_element_fgl_dropped_unknown_upstream_column == 0


def test_join_chain_prefers_owning_element_over_qualified_column() -> None:
    """Collision: E2 has `col` AND E1 has a column literally named `E2/col`.

    Join-chain reading wins -- 1608 real join chains in the observed tenant
    versus no confirmed slash-containing column name.
    """
    source = _source()
    e1_urn = _urn("e1")
    e2_urn = _urn("e2")
    downstream_urn = _urn("consumer")
    element = _element("consumer", "Consumer", [_column("c-x", "x", "[E1/E2/col]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"e1": ["e1"], "e2": ["e2"]},
        elementId_to_dataset_urn={"e1": e1_urn, "e2": e2_urn},
        entity_level_upstream_urns={e1_urn, e2_urn},
        upstream_elements=[
            _upstream_element("e1", "E1", ["E2/col"]),
            _upstream_element("e2", "E2", ["col"]),
        ],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(e2_urn, "col")]


def test_slash_containing_element_name_still_resolves() -> None:
    """No candidate matches the join-chain reading, so the prefix wins."""
    source = _source()
    owner_urn = _urn("weird")
    downstream_urn = _urn("consumer")
    element = _element("consumer", "Consumer", [_column("c-x", "x", "[a/b/c]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"a/b": ["weird"]},
        elementId_to_dataset_urn={"weird": owner_urn},
        entity_level_upstream_urns={owner_urn},
        upstream_elements=[_upstream_element("weird", "a/b", ["c"])],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(owner_urn, "c")]


def test_join_chain_with_no_valid_candidate_is_sub_counted() -> None:
    """Nothing validates: falls back to the legacy path, counted once there."""
    source = _source()
    join_urn = _urn("join")
    downstream_urn = _urn("consumer")
    element = _element("consumer", "Consumer", [_column("c-x", "x", "[WRK/E2/col]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"wrk": ["join"]},
        elementId_to_dataset_urn={"join": join_urn},
        entity_level_upstream_urns={join_urn},
        upstream_elements=[_upstream_element("join", "WRK", ["unrelated"])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_join_chain_unresolved == 1
    assert source.reporter.data_model_element_fgl_join_chain_resolved == 0
    # Residual bucket is owned by the legacy path and counted exactly once.
    assert source.reporter.data_model_element_fgl_dropped_unknown_upstream_column == 1


def test_single_slash_ref_does_not_touch_join_chain_counters() -> None:
    source = _source()
    upstream_urn = _urn("a")
    element = _element("b", "B", [_column("b-x", "x", "[A/x]")])

    lineages = _build(
        source,
        element,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": upstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["x"])],
    )

    assert len(lineages) == 1
    assert source.reporter.data_model_element_fgl_join_chain_resolved == 0
    assert source.reporter.data_model_element_fgl_join_chain_unresolved == 0


def test_join_chain_resolves_when_owner_absent_from_direct_lineage() -> None:
    """The owning element is a TRANSITIVE upstream, so /lineage never lists it.

    Regression test for the fix that mattered: requiring membership in
    entity_level_upstream_urns made every intra-DM join-chain candidate fail,
    because a join chain reaches its source through the join element and Sigma
    reports only the direct one. The owner must resolve anyway, and must be
    reported back for promotion to an entity-level upstream.
    """
    source = _source()
    join_urn = _urn("join")
    owner_urn = _urn("owner")
    downstream_urn = _urn("consumer")
    discovered: set = set()
    element = _element(
        "consumer", "Consumer", [_column("c-x", "x", "[JOIN/OWNER/col]")]
    )

    lineages = source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"join": ["join"], "owner": ["owner"]},
        elementId_to_dataset_urn={"join": join_urn, "owner": owner_urn},
        # Only the join element is a direct upstream -- the owner is not.
        entity_level_upstream_urns={join_urn},
        data_model=_data_model(
            [
                element,
                _upstream_element("join", "JOIN", ["unrelated"]),
                _upstream_element("owner", "OWNER", ["col"]),
            ]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=discovered,
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(owner_urn, "col")]
    assert source.reporter.data_model_element_fgl_join_chain_resolved == 1
    # Reported back so the caller can declare it in ``upstreams``.
    assert discovered == {owner_urn}
    assert source.reporter.data_model_element_fgl_join_chain_upstream_added == 1
