import datetime as dt
from typing import Dict, List, Set
from unittest.mock import MagicMock

import pytest

from datahub.emitter import mce_builder as builder
from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
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
    # /spec is off unless a test turns it on.
    source.config = SigmaSourceConfig(
        client_id="x", client_secret="y", extract_data_model_spec_lineage=False
    )
    source._dm_spec_index_cache = {}
    source._dm_column_lookup_cache = None
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
    discovered_upstreams: Set[str] | None = None,
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
        discovered_upstreams=set()
        if discovered_upstreams is None
        else discovered_upstreams,
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


def test_orphan_ref_dropped_when_the_siblings_schema_is_unknown() -> None:
    # Element IS in this DM (found in element_name_to_eids) but /lineage does
    # not report it as an upstream, and its columns are unknown, so nothing
    # shows it owns the column and orphan recovery does not apply.
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
    # Sibling does NOT own "x", so orphan recovery does not fire and the
    # cross-DM guard is still what this exercises.
    sibling = _upstream_element("sibling-eid", "Shared", ["other"])

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
    # Sibling does NOT own "x", so orphan recovery does not fire and the
    # cross-DM guard is still what this exercises.
    sibling = _upstream_element("sibling-eid", "Shared", ["other"])

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
    # Sibling does NOT own "x", so orphan recovery does not fire and the
    # cross-DM guard is still what this exercises.
    sibling = _upstream_element("sibling-eid", "Shared", ["other"])

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


# ---------------------------------------------------------------------------
# Orphan recovery: a sibling Sigma's /lineage did not list
# ---------------------------------------------------------------------------


def test_orphan_ref_recovered_when_sibling_owns_the_column() -> None:
    """/lineage omitting an intra-DM sibling is a reporting gap, not evidence.

    Where the named sibling owns the referenced column, the ref is trustworthy:
    the edge is emitted and the sibling is reported as a discovered upstream.
    """
    source = _source()
    sibling_urn = _urn("sibling-eid")
    discovered: Set[str] = set()
    element = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])

    lineages = _build(
        source,
        element,
        element_name_to_eids={"shared": ["sibling-eid"]},
        elementId_to_dataset_urn={"sibling-eid": sibling_urn},
        # Sigma did not list the sibling as an upstream.
        entity_level_upstream_urns=set(),
        upstream_elements=[_upstream_element("sibling-eid", "Shared", ["x"])],
        discovered_upstreams=discovered,
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(sibling_urn, "x")]
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 0
    assert source.reporter.data_model_element_fgl_orphan_recovered == 1
    assert discovered == {sibling_urn}


def test_orphan_ref_still_dropped_when_sibling_lacks_the_column() -> None:
    """Recovery is earned by the schema, not assumed from the name match."""
    source = _source()
    discovered: Set[str] = set()
    element = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])

    lineages = _build(
        source,
        element,
        element_name_to_eids={"shared": ["sibling-eid"]},
        elementId_to_dataset_urn={"sibling-eid": _urn("sibling-eid")},
        entity_level_upstream_urns=set(),
        upstream_elements=[_upstream_element("sibling-eid", "Shared", ["other"])],
        discovered_upstreams=discovered,
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1
    assert discovered == set()


def test_a_recovered_sibling_is_an_entity_level_upstream() -> None:
    """A column edge to a Dataset missing from ``upstreams`` is not rendered."""
    source = _source()
    listed_urn = _urn("listed-eid")
    sibling_urn = _urn("sibling-eid")
    element = _element(
        "consumer",
        "Consumer",
        [_column("c1", "x", "[Shared/x]")],
        source_ids=["listed-eid"],
    )
    data_model = _data_model(
        [
            element,
            _upstream_element("listed-eid", "Listed", ["y"]),
            _upstream_element("sibling-eid", "Shared", ["x"]),
        ]
    )

    lineage = source._gen_data_model_element_upstream_lineage(
        element,
        data_model,
        _urn("consumer"),
        elementId_to_dataset_urn={
            "listed-eid": listed_urn,
            "sibling-eid": sibling_urn,
        },
        element_name_to_eids={"listed": ["listed-eid"], "shared": ["sibling-eid"]},
        warehouse_url_id_map={},
    )

    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == sorted([listed_urn, sibling_urn])
    assert lineage.fineGrainedLineages is not None
    assert lineage.fineGrainedLineages[0].upstreams == [
        builder.make_schema_field_urn(sibling_urn, "x")
    ]


# ---------------------------------------------------------------------------
# An upstream element whose column list came back empty
# ---------------------------------------------------------------------------


def test_empty_upstream_schema_is_counted_separately() -> None:
    """An upstream element with no columns is a fetch problem, not a name
    mismatch, so it is not folded into dropped_unknown_upstream_column."""
    source = _source()
    upstream_urn = _urn("a")
    element = _element("b", "B", [_column("b-x", "x", "[A/x]")])

    lineages = _build(
        source,
        element,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": upstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", [])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_upstream_schema_unavailable == 1
    assert source.reporter.data_model_element_fgl_dropped_unknown_upstream_column == 0


def test_empty_upstream_schema_warns_once_per_upstream() -> None:
    """A partial /columns abort leaves many refs pointing at the same empty
    element; one warning per upstream, not per referencing column."""
    source = _source()
    upstream_urn = _urn("a")
    element = _element(
        "b",
        "B",
        [_column("b-x", "x", "[A/x]"), _column("b-y", "y", "[A/y]")],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": upstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", [])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_upstream_schema_unavailable == 2
    # The report groups warnings by title, so count the contexts too.
    assert len(source.reporter.warnings) == 1
    assert len(source.reporter.warnings[0].context) == 1
    # Names the Data Model, so it can be matched to a pagination-abort warning.
    assert "data_model=dm-1" in str(source.reporter.warnings[0].context)


def _collision_build(
    source: SigmaSource, producer_cols: Dict[str, str] | None
) -> tuple:
    """Two elements named "Custom SQL": the consumer reads another model's
    "Custom SQL", and an unlisted sibling of the same name has the column."""
    producer_urn = _urn("producer-custom-sql")
    sibling_urn = _urn("sibling-custom-sql")
    consumer_urn = _urn("consumer-custom-sql")
    consumer = _element(
        "consumer-eid",
        "Custom SQL",
        [_column("c1", "Visit Id", "[Custom SQL/Visit Id]")],
        source_ids=["dm-b/s1"],
    )
    if producer_cols is not None:
        source.dm_element_urn_by_name = {"dm-b": {"custom sql": [producer_urn]}}
        source.dm_element_urn_to_cols = {producer_urn: producer_cols}
    discovered: Set[str] = set()
    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        element_name_to_eids={"custom sql": ["sibling-eid", "consumer-eid"]},
        elementId_to_dataset_urn={
            "sibling-eid": sibling_urn,
            "consumer-eid": consumer_urn,
        },
        entity_level_upstream_urns={producer_urn},
        upstream_elements=[
            _upstream_element("sibling-eid", "Custom SQL", ["Visit Id"])
        ],
        discovered_upstreams=discovered,
    )
    return lineages, discovered


@pytest.mark.parametrize(
    "producer_cols",
    [{}, None],
    ids=["producer-columns-empty", "producer-model-not-ingested"],
)
def test_no_recovery_when_the_element_reads_another_model(
    producer_cols: Dict[str, str] | None,
) -> None:
    """A same-named sibling is a name collision when /lineage points at another
    Data Model, even after the cross-DM rescue fails."""
    source = _source()
    lineages, discovered = _collision_build(source, producer_cols)
    assert lineages == []
    assert discovered == set()
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


def test_no_recovery_when_two_unlisted_siblings_own_the_column() -> None:
    """With no /lineage signal, nothing breaks the tie."""
    source = _source()
    discovered: Set[str] = set()
    element = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])

    lineages = _build(
        source,
        element,
        element_name_to_eids={"shared": ["sib-1", "sib-2"]},
        elementId_to_dataset_urn={"sib-1": _urn("sib-1"), "sib-2": _urn("sib-2")},
        entity_level_upstream_urns=set(),
        upstream_elements=[
            _upstream_element("sib-1", "Shared", ["x"]),
            _upstream_element("sib-2", "Shared", ["x"]),
        ],
        discovered_upstreams=discovered,
    )

    assert lineages == []
    assert discovered == set()
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1


# ---------------------------------------------------------------------------
# Union branches from /spec
# ---------------------------------------------------------------------------


def _union_spec(sources: List[dict], source_columns: List[str]) -> dict:
    return {
        "schemaVersion": 1,
        "dataModelId": "dm-1",
        "pages": [
            {
                "id": "p1",
                "elements": [
                    {
                        "id": "u",
                        "kind": "table",
                        "source": {
                            "kind": "union",
                            "sources": sources,
                            "matches": [
                                {
                                    "outputColumnName": "Out",
                                    "sourceColumns": source_columns,
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _with_spec(source: SigmaSource, spec: dict | None) -> MagicMock:
    source.config = SigmaSourceConfig(client_id="x", client_secret="y")
    source.sigma_api = MagicMock()
    source.sigma_api.get_data_model_spec.return_value = spec
    return source.sigma_api


_BRANCH_A = {"kind": "table", "elementId": "a"}
_BRANCH_B = {"kind": "table", "elementId": "b"}


def _build_union(source: SigmaSource, discovered: Set[str] | None = None) -> list:
    # The output column's formula names branch A only, as Sigma writes it.
    union = _element("u", "U", [_column("u-out", "Out", "[A/a]")])
    return _build(
        source,
        union,
        element_name_to_eids={"a": ["a"], "b": ["b"]},
        elementId_to_dataset_urn={"a": _urn("a"), "b": _urn("b")},
        entity_level_upstream_urns={_urn("a")},
        upstream_elements=[
            _upstream_element("a", "A", ["a"]),
            _upstream_element("b", "B", ["b"]),
        ],
        discovered_upstreams=discovered,
    )


def test_a_union_output_column_gets_every_branch() -> None:
    """The formula names one branch; /spec names the others."""
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))
    discovered: Set[str] = set()

    lineages = _build_union(source, discovered)

    assert sorted(lin.upstreams[0] for lin in lineages) == [
        builder.make_schema_field_urn(_urn("a"), "a"),
        builder.make_schema_field_urn(_urn("b"), "b"),
    ]
    assert all(lin.confidenceScore == 1.0 for lin in lineages)
    # Branch A was already an edge from the formula; B is the gain.
    assert source.reporter.data_model_element_fgl_union_resolved == 1
    assert discovered == {_urn("b")}


def test_a_branch_column_is_matched_by_id_or_by_name() -> None:
    source = _source()
    # Branch B's column id, and branch A's name in another case.
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[A]", "[b-b]"]))

    lineages = _build_union(source)

    assert len(lineages) == 2
    assert source.reporter.data_model_element_fgl_union_resolved == 1


@pytest.mark.parametrize(
    "branch",
    [
        {"kind": "warehouse-table", "connectionId": "c", "path": ["D", "S", "T"]},
        {"kind": "data-model", "elementId": "b", "dataModelId": "dm-2"},
    ],
    ids=["warehouse-table", "another-data-model"],
)
def test_a_branch_outside_this_model_is_not_mapped(branch: dict) -> None:
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, branch], ["[a]", "[b]"]))

    lineages = _build_union(source)

    assert [lin.upstreams[0] for lin in lineages] == [
        builder.make_schema_field_urn(_urn("a"), "a")
    ]
    assert source.reporter.data_model_element_fgl_union_resolved == 0
    assert source.reporter.data_model_element_fgl_union_branch_unmapped == 1


def test_no_spec_call_when_the_flag_is_off() -> None:
    source = _source()
    api = _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))
    source.config = SigmaSourceConfig(
        client_id="x", client_secret="y", extract_data_model_spec_lineage=False
    )

    assert len(_build_union(source)) == 1
    api.get_data_model_spec.assert_not_called()


def test_a_failed_fetch_is_not_drift() -> None:
    """The API client counts and reports the failure; nothing was read."""
    source = _source()
    _with_spec(source, None)

    assert len(_build_union(source)) == 1
    assert source.reporter.data_model_spec_drift_detected == 0


def test_an_unsupported_schema_is_reported_and_adds_no_union_edges() -> None:
    """The parser's contract: distrust a read whose schemaVersion differs."""
    source = _source()
    spec = _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"])
    spec["schemaVersion"] = 2
    _with_spec(source, spec)

    assert len(_build_union(source)) == 1
    assert source.reporter.data_model_element_fgl_union_resolved == 0
    assert source.reporter.data_model_spec_drift_detected == 1
    assert "data_model=dm-1" in str(source.reporter.warnings[0].context)


def test_the_spec_is_fetched_once_per_data_model() -> None:
    source = _source()
    api = _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))

    _build_union(source)
    _build_union(source)

    api.get_data_model_spec.assert_called_once_with("dm-1")


def test_a_misaligned_union_adds_no_union_edges() -> None:
    """More column slots than branches: pairing a slot with a branch is a
    guess, so the parser marks the union unreadable and nothing is emitted."""
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]", "[c]"]))

    assert len(_build_union(source)) == 1
    assert source.reporter.data_model_element_fgl_union_resolved == 0
    assert source.reporter.data_model_spec_drift_detected == 1


def _build_union_with_branch_b(source: SigmaSource, b_columns: List[tuple]) -> list:
    union = _element("u", "U", [_column("u-out", "Out", "[A/a]")])
    branch_b = _element(
        "b", "B", [_column(col_id, name, None) for col_id, name in b_columns]
    )
    return _build(
        source,
        union,
        element_name_to_eids={"a": ["a"], "b": ["b"]},
        elementId_to_dataset_urn={"a": _urn("a"), "b": _urn("b")},
        entity_level_upstream_urns={_urn("a")},
        upstream_elements=[_upstream_element("a", "A", ["a"]), branch_b],
    )


@pytest.mark.parametrize("name_first", [True, False], ids=["name-first", "id-first"])
def test_a_column_id_is_not_mistaken_for_another_columns_name(name_first: bool) -> None:
    """Branch B has "Name" and another column whose author-chosen id is
    "Name" (Sigma's as-code examples use ids like this): the name wins."""
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[Name]"]))
    columns = [("x1", "Name"), ("Name", "Full Name")]

    lineages = _build_union_with_branch_b(
        source, columns if name_first else columns[::-1]
    )

    assert builder.make_schema_field_urn(_urn("b"), "Name") in [
        lin.upstreams[0] for lin in lineages
    ]


def test_an_ambiguous_case_insensitive_match_claims_nothing() -> None:
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[key]"]))

    lineages = _build_union_with_branch_b(source, [("k1", "Key"), ("k2", "KEY")])

    assert len(lineages) == 1
    assert source.reporter.data_model_element_fgl_union_unresolved == 1


def test_an_output_that_is_not_a_column_of_the_union_is_counted() -> None:
    """Seen on a real model: /spec keeps an output the element does not have."""
    source = _source()
    spec = _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"])
    spec["pages"][0]["elements"][0]["source"]["matches"][0]["outputColumnName"] = "Gone"
    _with_spec(source, spec)

    assert len(_build_union(source)) == 1
    assert source.reporter.data_model_element_fgl_union_unresolved == 2


def test_an_absent_output_counts_each_branch_by_kind() -> None:
    source = _source()
    warehouse = {
        "kind": "warehouse-table",
        "connectionId": "c",
        "path": ["D", "S", "T"],
    }
    spec = _union_spec([_BRANCH_A, warehouse], ["[a]", "[b]"])
    spec["pages"][0]["elements"][0]["source"]["matches"][0]["outputColumnName"] = "Gone"
    _with_spec(source, spec)

    _build_union(source)

    assert source.reporter.data_model_element_fgl_union_unresolved == 1
    assert source.reporter.data_model_element_fgl_union_branch_unmapped == 1
