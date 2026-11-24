import datetime as dt
from typing import Any, Dict, List, Set, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.emitter import mce_builder as builder
from datahub.ingestion.source.sigma import sigma as sigma_module
from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import (
    CustomSqlEntry,
    SigmaDataModel,
    SigmaDataModelColumn,
    SigmaDataModelElement,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource, _WarehouseTableRef


def _source() -> SigmaSource:
    source = SigmaSource.__new__(SigmaSource)
    source.reporter = SigmaSourceReport()
    source.dm_element_urn_by_name = {}
    source.dm_element_urn_to_cols = {}
    source._upstream_schema_unavailable_warned = set()
    source._dm_recovery_contexts = {}
    source.sigma_dataset_name_by_url_id = {}
    source.sigma_dataset_urn_by_url_id = {}
    source._warehouse_ref_by_url_id = {}
    source.sigma_api = MagicMock()
    source.sigma_api.get_file_metadata.return_value = None
    # /spec is off unless a test turns it on.
    source.config = SigmaSourceConfig(
        client_id="x", client_secret="y", extract_data_model_spec_lineage=False
    )
    source._dm_spec_index_cache = {}
    source._dm_column_lookup_cache = None
    source._dm_spec_drift_logged = False
    source._join_partner_cache = None
    return source


# A warehouse source whose /files lookup misses (_source() returns None).
_WH_INODE = "inode-whUrlId000000000000001"


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
        columns_complete=True,
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
    union_upstreams: Set[str] | None = None,
    join_key_upstreams: Set[str] | None = None,
    warehouse_url_id_map: Dict[str, _WarehouseTableRef] | None = None,
    custom_sql_by_name: Dict[str, CustomSqlEntry] | None = None,
    columns_complete: bool = True,
) -> list:
    all_elements = [element] + (upstream_elements or [])
    data_model = _data_model(all_elements)
    data_model.custom_sql_by_name = custom_sql_by_name or {}
    data_model.columns_complete = columns_complete
    return source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=element_dataset_urn or _urn(element.elementId),
        element_name_to_eids=element_name_to_eids or {},
        elementId_to_dataset_urn=elementId_to_dataset_urn or {},
        entity_level_upstream_urns=entity_level_upstream_urns or set(),
        data_model=data_model,
        warehouse_url_id_map=warehouse_url_id_map or {},
        discovered_upstreams=set()
        if discovered_upstreams is None
        else discovered_upstreams,
        union_upstreams=union_upstreams,
        join_key_upstreams=join_key_upstreams,
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
        [_column(f"{_WH_INODE}/CITY", "city", "[data.csv/city]")],
        source_ids=[_WH_INODE],
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


def test_cross_dm_ref_resolves_when_column_id_names_a_foreign_inode() -> None:
    """An inode- columnId that names none of the element's own sources must
    not short-circuit the cross-DM lookup."""
    source = _source()
    dm_url_id = "other-dm"
    other_urn = _urn("other-dm-element")
    downstream_urn = _urn("elem-downstream")
    element = _element(
        "elem-downstream",
        "Downstream",
        [_column("inode-notOwnSource/CITY", "city", "[other_dm_element/city]")],
        source_ids=[f"{dm_url_id}/some-suffix"],
    )
    source.dm_element_urn_by_name = {dm_url_id: {"other_dm_element": [other_urn]}}
    source.dm_element_urn_to_cols = {other_urn: {"city": "city"}}

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
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 1
    assert source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0


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
        [_column(f"{_WH_INODE}/ID", "id", "[CUSTOMERS/id]")],
        source_ids=[_WH_INODE],  # no cross-DM refs, warehouse-only element
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
        [_column(f"{_WH_INODE}/X", "x", "[Orders/x]")],
        # bare ID, no "/" — not cross-DM shaped
        source_ids=["some-intra-dm-eid", _WH_INODE],
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


def test_no_recovery_for_a_warehouse_column_named_like_a_sibling() -> None:
    """A warehouse column's formula names its table, not an element: a sibling
    named like the table, here built on the same table, is a collision."""
    source = _source()
    discovered: Set[str] = set()
    element = _element(
        "raw",
        "Orders Raw",
        [_column("inode-u1/AMOUNT", "Amount", "[ORDERS/Amount]")],
        source_ids=["inode-u1"],
    )
    sibling = _element(
        "curated",
        "Orders",
        [_column("inode-u1/AMOUNT", "Amount", "[ORDERS/Amount]")],
        ["inode-u1"],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"orders": ["curated"]},
        elementId_to_dataset_urn={"curated": _urn("curated")},
        entity_level_upstream_urns=set(),
        upstream_elements=[sibling],
        discovered_upstreams=discovered,
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0
    assert discovered == set()


@pytest.mark.parametrize("via_middle", [False, True], ids=["direct", "transitive"])
def test_no_recovery_for_a_sibling_that_reads_the_element(via_middle: bool) -> None:
    source = _source()
    discovered: Set[str] = set()
    element = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])
    middle = _element("middle", "Middle", [_column("m-y", "y", None)], ["consumer"])
    sibling = _element(
        "sibling-eid",
        "Shared",
        [_column("s-x", "x", None)],
        ["middle" if via_middle else "consumer"],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"shared": ["sibling-eid"]},
        elementId_to_dataset_urn={"sibling-eid": _urn("sibling-eid")},
        entity_level_upstream_urns=set(),
        upstream_elements=[middle, sibling],
        discovered_upstreams=discovered,
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1
    assert discovered == set()


def test_no_recovery_for_a_ref_naming_the_element_itself() -> None:
    """Two elements named after the same source, each with a calculated column
    on it: the ref names the element's own name, so it is not a sibling (and
    recovering both would be a cycle)."""
    source = _source()
    lineages = {}
    for eid, other in [("a", "b"), ("b", "a")]:
        element = _element(
            eid, "ORDERS", [_column(f"{eid}-c", "Amount", "[ORDERS/Amount] * 2")]
        )
        lineages[eid] = _build(
            source,
            element,
            element_name_to_eids={"orders": ["a", "b"]},
            elementId_to_dataset_urn={"a": _urn("a"), "b": _urn("b")},
            entity_level_upstream_urns=set(),
            upstream_elements=[_upstream_element(other, "ORDERS", ["Amount"])],
        )

    assert lineages == {"a": [], "b": []}
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


def test_no_recovery_for_a_calculated_column_on_a_table_this_element_reads() -> None:
    """The passthrough column shows ORDERS is a warehouse table here, so a
    sibling named ORDERS is a collision for the calculated column too."""
    source = _source()
    element = _element(
        "raw",
        "Orders Raw",
        [
            _column("inode-t1/AMOUNT", "Amount", "[ORDERS/Amount]"),
            _column("c-double", "Double", "[ORDERS/Amount] * 2"),
        ],
        source_ids=["inode-t1"],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"orders": ["orders-eid"]},
        elementId_to_dataset_urn={"orders-eid": _urn("orders-eid")},
        upstream_elements=[_upstream_element("orders-eid", "ORDERS", ["Amount"])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


def test_no_recovery_when_the_passthrough_column_is_shadowed() -> None:
    """A calculated field shadowing the passthrough wins the dedup, but the
    passthrough still shows ORDERS is a table this element reads."""
    source = _source()
    element = _element(
        "raw",
        "Orders Raw",
        [
            _column("inode-t1/AMOUNT", "Amount", "[ORDERS/Amount]"),
            _column("c-amt", "Amount", "[ORDERS/Amount] + 0"),
            _column("c-double", "Double", "[ORDERS/Amount] * 2"),
        ],
        source_ids=["inode-t1"],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"orders": ["orders-eid"]},
        elementId_to_dataset_urn={"orders-eid": _urn("orders-eid")},
        upstream_elements=[_upstream_element("orders-eid", "ORDERS", ["Amount"])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


def test_no_recovery_from_a_sibling_whose_formula_names_this_element() -> None:
    """Neither is listed, so /lineage misses the link; the read graph has it
    through the formula names. Otherwise each would
    recover the other into a cycle."""
    source = _source()
    alpha = _element("a", "Alpha", [_column("a-x", "x", "[Beta/y]")])
    beta = _element(
        "b", "Beta", [_column("b-y", "y", None), _column("b-z", "z", "[Alpha/x]")]
    )

    lineages = _build(
        source,
        alpha,
        element_name_to_eids={"alpha": ["a"], "beta": ["b"]},
        elementId_to_dataset_urn={"b": _urn("b")},
        upstream_elements=[beta],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


def test_no_recovery_for_a_table_the_element_reads_without_a_passthrough() -> None:
    """The table name comes from the element's inode source too."""
    source = _source()
    element = _element(
        "raw",
        "Orders Raw",
        [_column("c-double", "Double", "[ORDERS/Amount] * 2")],
        source_ids=["inode-u1"],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"orders": ["orders-eid"]},
        elementId_to_dataset_urn={"orders-eid": _urn("orders-eid")},
        upstream_elements=[_upstream_element("orders-eid", "ORDERS", ["Amount"])],
        warehouse_url_id_map={
            "u1": _WarehouseTableRef(
                connection_id="conn", db="DB", schema="S", table="ORDERS"
            )
        },
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


def test_no_recovery_when_a_table_the_element_reads_has_no_known_name() -> None:
    """With /files failed and no passthrough on the inode, ORDERS may be that
    table, so the sibling named ORDERS is not taken."""
    source = _source()
    element = _element(
        "raw",
        "Orders Raw",
        [_column("c-double", "Double", "[ORDERS/Amount] * 2")],
        source_ids=["inode-u1"],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"orders": ["orders-eid"]},
        elementId_to_dataset_urn={"orders-eid": _urn("orders-eid")},
        upstream_elements=[_upstream_element("orders-eid", "ORDERS", ["Amount"])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


_OTHER_TABLE = _WarehouseTableRef(
    connection_id="conn", db="DB", schema="S", table="OTHER"
)


def _calc_on(
    source_ids: List[str], extra: List[SigmaDataModelColumn]
) -> SigmaDataModelElement:
    return _element(
        "raw",
        "Orders Raw",
        [*extra, _column("c-double", "Double", "[ORDERS/Amount] * 2")],
        source_ids=source_ids,
    )


def _recover_from_orders(
    element: SigmaDataModelElement, warehouse_url_id_map: Dict[str, _WarehouseTableRef]
) -> list:
    return _build(
        _source(),
        element,
        element_name_to_eids={"orders": ["orders-eid"]},
        elementId_to_dataset_urn={"orders-eid": _urn("orders-eid")},
        upstream_elements=[_upstream_element("orders-eid", "ORDERS", ["Amount"])],
        warehouse_url_id_map=warehouse_url_id_map,
    )


def test_no_recovery_for_a_renamed_custom_sql_element() -> None:
    """Its columns still read [Custom SQL/COL], so a sibling left with the
    default name "Custom SQL" is a collision, not its source."""
    source = _source()
    element = _element(
        "orders-sql",
        "Orders SQL",
        [_column("c1", "ORDER_ID", "[Custom SQL/ORDER_ID]")],
        source_ids=["Custom SQL", "regions-eid"],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"custom sql": ["other-sql"]},
        elementId_to_dataset_urn={"other-sql": _urn("other-sql")},
        upstream_elements=[_upstream_element("other-sql", "Custom SQL", ["ORDER_ID"])],
        custom_sql_by_name={"Custom SQL": CustomSqlEntry(name="Custom SQL")},
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


def _reads_dataset(
    column_formula: str,
    sibling_name: str,
    warehouse_url_id_map: Dict[str, _WarehouseTableRef] | None = None,
) -> list:
    """An element on Sigma Dataset SD1 ("My Dataset"); its columnIds are
    opaque, as in the integration fixture, so only the name names it."""
    source = _source()
    source.sigma_dataset_name_by_url_id = {"SD1": "My Dataset"}
    element = _element(
        "on-sd",
        "On SD",
        [_column("c1", "x", column_formula)],
        source_ids=["inode-SD1"],
    )
    return _build(
        source,
        element,
        element_name_to_eids={sibling_name.lower(): ["sib"]},
        elementId_to_dataset_urn={"sib": _urn("sib")},
        upstream_elements=[_upstream_element("sib", sibling_name, ["x"])],
        warehouse_url_id_map=warehouse_url_id_map,
    )


def test_an_element_on_a_sigma_dataset_recovers_a_sibling() -> None:
    assert len(_reads_dataset("[Shared/x]", "Shared")) == 1


def test_no_recovery_for_a_sibling_named_like_the_sigma_dataset() -> None:
    assert _reads_dataset("[My Dataset/x]", "My Dataset") == []


def test_an_inode_also_catalogued_as_a_table_keeps_its_dataset_name() -> None:
    table = _WarehouseTableRef(connection_id="c", db="D", schema="S", table="RAW")
    assert _reads_dataset("[My Dataset/x] * 2", "My Dataset", {"SD1": table}) == []


def test_a_listed_same_named_element_is_the_one_a_formula_reads() -> None:
    """S lists E2 and reads [Orders/amt]; E, also named Orders, is not what
    S reads, so E may recover its ref to S."""
    source = _source()
    e = _element("e", "Orders", [_column("e-x", "x", "[S/x]")])
    s_el = _element("s", "S", [_column("s-x", "x", "[Orders/amt]")], source_ids=["e2"])
    e2 = _upstream_element("e2", "Orders", ["amt"])

    lineages = _build(
        source,
        e,
        element_name_to_eids={"orders": ["e", "e2"], "s": ["s"]},
        elementId_to_dataset_urn={"s": _urn("s"), "e2": _urn("e2")},
        upstream_elements=[s_el, e2],
    )

    assert len(lineages) == 1


def test_one_unnamed_inode_among_named_ones_still_declines() -> None:
    element = _calc_on(["inode-u1", "inode-u2"], [])
    assert _recover_from_orders(element, {"u1": _OTHER_TABLE}) == []


def test_a_passthrough_column_with_no_ref_does_not_name_its_inode() -> None:
    element = _calc_on(["inode-u1"], [_column("inode-u1/K", "K", "1")])
    assert _recover_from_orders(element, {}) == []


def test_an_inode_named_by_files_alone_lets_recovery_proceed() -> None:
    """u1 is OTHER, so ORDERS is a sibling, not a table this element reads."""
    element = _calc_on(["inode-u1"], [])
    assert len(_recover_from_orders(element, {"u1": _OTHER_TABLE})) == 1


def test_an_unnamed_inode_elsewhere_in_the_model_does_not_break_the_graph() -> None:
    source = _source()
    consumer = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])
    owner = _element(
        "sibling-eid",
        "Shared",
        [_column("s-x", "x", "[T/x]")],
        source_ids=["inode-u9"],
    )

    lineages = _build(
        source,
        consumer,
        element_name_to_eids={"shared": ["sibling-eid"]},
        elementId_to_dataset_urn={"sibling-eid": _urn("sibling-eid")},
        upstream_elements=[owner],
    )

    assert len(lineages) == 1


def test_an_inode_column_id_the_element_does_not_read_names_no_table() -> None:
    """A columnId can carry another element's inode; only the element's own
    inode source_ids make its formula source a table."""
    source = _source()
    element = _element(
        "child",
        "Child",
        [_column("inode-t1/AMOUNT", "Amount", "[Orders/Amount]")],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"orders": ["orders-eid"]},
        elementId_to_dataset_urn={"orders-eid": _urn("orders-eid")},
        upstream_elements=[_upstream_element("orders-eid", "Orders", ["Amount"])],
    )

    assert len(lineages) == 1
    assert source.reporter.data_model_element_fgl_orphan_recovered == 1


def test_no_recovery_that_closes_a_longer_cycle_by_name() -> None:
    """A names B, B names C, C names A, none listed: recovering A's ref to B
    would close A -> B -> C -> A."""
    source = _source()
    a = _element("a", "Alpha", [_column("a-x", "x", "[Beta/y]")])
    b = _element("b", "Beta", [_column("b-y", "y", "[Gamma/z]")])
    c = _element("c", "Gamma", [_column("c-z", "z", "[Alpha/x]")])

    lineages = _build(
        source,
        a,
        element_name_to_eids={"alpha": ["a"], "beta": ["b"], "gamma": ["c"]},
        elementId_to_dataset_urn={"b": _urn("b"), "c": _urn("c")},
        upstream_elements=[b, c],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


def test_a_sibling_reading_a_table_named_like_the_element_still_recovers() -> None:
    """[CUSTOMERS/id] in the owner names its warehouse table, not the
    consumer called Customers, so it is no cycle."""
    source = _source()
    consumer = _element("cust", "Customers", [_column("c1", "x", "[Shared/x]")])
    owner = _element(
        "shared",
        "Shared",
        [_column("inode-t9/X", "x", "[CUSTOMERS/x]")],
        source_ids=["inode-t9"],
    )

    lineages = _build(
        source,
        consumer,
        element_name_to_eids={"customers": ["cust"], "shared": ["shared"]},
        elementId_to_dataset_urn={"shared": _urn("shared")},
        upstream_elements=[owner],
    )

    assert len(lineages) == 1
    assert source.reporter.data_model_element_fgl_orphan_recovered == 1


def test_the_recovery_context_is_built_once_per_data_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """It parses every formula in the model, so not once per recovered ref."""
    builds: List[str] = []
    real = sigma_module._dm_recovery_context

    def counting(*args: Any) -> Any:
        builds.append(args[0].dataModelId)
        return real(*args)

    monkeypatch.setattr(sigma_module, "_dm_recovery_context", counting)
    source = _source()
    sibling = _upstream_element("sibling-eid", "Shared", ["x", "y"])
    consumers = [
        _element(
            eid,
            eid,
            [
                _column(f"{eid}-x", "x", "[Shared/x]"),
                _column(f"{eid}-y", "y", "[Shared/y]"),
            ],
        )
        for eid in ["c1", "c2"]
    ]
    for consumer in consumers:
        # The same Data Model each time: every element, the consumer first.
        others = [c for c in consumers if c is not consumer]
        _build(
            source,
            consumer,
            element_name_to_eids={"shared": ["sibling-eid"]},
            elementId_to_dataset_urn={"sibling-eid": _urn("sibling-eid")},
            upstream_elements=[sibling, *others],
        )

    assert source.reporter.data_model_element_fgl_orphan_recovered == 4
    assert builds == ["dm-1"]


def test_no_recovery_when_a_same_named_sibling_has_no_columns() -> None:
    """Its columns are unknown, not absent, so it may own the column too."""
    source = _source()
    element = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])

    lineages = _build(
        source,
        element,
        element_name_to_eids={"shared": ["s1", "s2"]},
        elementId_to_dataset_urn={"s1": _urn("s1"), "s2": _urn("s2")},
        upstream_elements=[
            _upstream_element("s1", "Shared", ["x"]),
            _upstream_element("s2", "Shared", []),
        ],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


def test_no_recovery_when_the_column_listing_is_partial() -> None:
    """A partial /columns listing can leave a same-named sibling missing the
    column it owns, so the sibling that has it is not known to be the owner."""
    source = _source()
    discovered: Set[str] = set()
    element = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])

    lineages = _build(
        source,
        element,
        element_name_to_eids={"shared": ["s1", "s2"]},
        elementId_to_dataset_urn={"s1": _urn("s1"), "s2": _urn("s2")},
        upstream_elements=[
            _upstream_element("s1", "Shared", ["y"]),
            _upstream_element("s2", "Shared", ["x"]),
        ],
        discovered_upstreams=discovered,
        columns_complete=False,
    )

    assert lineages == []
    assert discovered == set()
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1


def test_a_repeated_recovered_ref_counts_one_edge() -> None:
    source = _source()
    element = _element(
        "consumer",
        "Consumer",
        [_column("c1", "x", "If([Shared/x] > 0, [Shared/x], 0)")],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"shared": ["sibling-eid"]},
        elementId_to_dataset_urn={"sibling-eid": _urn("sibling-eid")},
        entity_level_upstream_urns=set(),
        upstream_elements=[_upstream_element("sibling-eid", "Shared", ["x"])],
    )

    assert len(lineages) == 1
    assert lineages[0].confidenceScore < 1.0
    assert source.reporter.data_model_element_fgl_orphan_recovered == 1


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
    assert source.reporter.data_model_element_intra_upstreams == 1
    assert source.reporter.data_model_element_intra_upstreams_recovered == 1


def test_no_recovery_when_no_upstream_of_the_element_resolves() -> None:
    """Sigma lists no upstream, so no UpstreamLineage is built at all."""
    source = _source()
    element = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])

    lineage = source._gen_data_model_element_upstream_lineage(
        element,
        _data_model([element, _upstream_element("sibling-eid", "Shared", ["x"])]),
        _urn("consumer"),
        elementId_to_dataset_urn={"sibling-eid": _urn("sibling-eid")},
        element_name_to_eids={"shared": ["sibling-eid"]},
        warehouse_url_id_map={},
    )

    assert lineage is None
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


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


def test_no_recovery_for_an_element_reading_another_model_by_another_name() -> None:
    """The cross-DM guard holds even when the ref does not name the element."""
    source = _source()
    element = _element(
        "consumer",
        "Consumer",
        [_column("c1", "x", "[Shared/x]")],
        source_ids=["dm-b/s1"],
    )

    lineages = _build(
        source,
        element,
        element_name_to_eids={"shared": ["sibling-eid"]},
        elementId_to_dataset_urn={"sibling-eid": _urn("sibling-eid")},
        upstream_elements=[_upstream_element("sibling-eid", "Shared", ["x"])],
    )

    assert lineages == []
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


# How a union element's column reads the union's output "Out", whatever the
# element is called.
_READS_OUT = "[Union of 2 Sources/Out]"


def _build_union(
    source: SigmaSource,
    union_upstreams: Set[str] | None = None,
    union_columns: List[SigmaDataModelColumn] | None = None,
) -> list:
    union = _element("u", "U", union_columns or [_column("u-out", "Out", _READS_OUT)])
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
        union_upstreams=union_upstreams,
    )


def test_a_union_output_column_gets_every_branch() -> None:
    """The column's formula names the output, not a branch; /spec names them."""
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))
    union_upstreams: Set[str] = set()

    lineages = _build_union(source, union_upstreams)

    assert sorted(lin.upstreams[0] for lin in lineages) == [
        builder.make_schema_field_urn(_urn("a"), "a"),
        builder.make_schema_field_urn(_urn("b"), "b"),
    ]
    assert all(lin.confidenceScore == 1.0 for lin in lineages)
    assert source.reporter.data_model_element_fgl_union_resolved == 2
    assert union_upstreams == {_urn("a"), _urn("b")}


def test_a_renamed_union_column_still_gets_every_branch() -> None:
    """Renaming the element's column keeps the output's name in /spec; the
    column's formula still reads the output by that name."""
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))

    lineages = _build_union(
        source, union_columns=[_column("u-out", "Renamed", _READS_OUT)]
    )

    assert {lin.downstreams[0] for lin in lineages} == {
        builder.make_schema_field_urn(_urn("u"), "Renamed")
    }
    assert len(lineages) == 2


def test_a_column_reusing_a_renamed_outputs_name_gets_no_branch() -> None:
    """After a rename, an unrelated column may take the output's old name: it
    does not read the output, and a bare [Out] names that column, so neither
    gets a union edge."""
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))

    lineages = _build_union(
        source,
        union_columns=[
            _column("u-raw", "Raw Out", "[Out]"),
            _column("u-out", "Out", '"constant"'),
        ],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_union_unresolved == 2


def test_a_bare_ref_to_the_output_gets_no_union_edges() -> None:
    """A bare ref names the element's own column; Sigma does not resolve it
    to the union's output."""
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))

    assert (
        _build_union(source, union_columns=[_column("u-r", "Renamed", "[Out]")]) == []
    )


def test_a_union_output_ref_is_not_deferred_as_cross_dm() -> None:
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))

    _build_union(source)

    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
    assert source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 0


def test_a_column_reading_a_branch_directly_gets_no_union_edges() -> None:
    """[A/a] in a union element reads branch A's column, not the output."""
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))

    lineages = _build_union(source, union_columns=[_column("u-out", "Out", "[A/a]")])

    assert [lin.upstreams[0] for lin in lineages] == [
        builder.make_schema_field_urn(_urn("a"), "a")
    ]
    assert source.reporter.data_model_element_fgl_union_resolved == 0


def test_a_branch_column_is_matched_by_id_or_by_name() -> None:
    source = _source()
    # Branch B's column id, and branch A's name in another case.
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[A]", "[b-b]"]))

    lineages = _build_union(source)

    assert len(lineages) == 2
    assert source.reporter.data_model_element_fgl_union_resolved == 2


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
    assert source.reporter.data_model_element_fgl_union_resolved == 1
    assert source.reporter.data_model_element_fgl_union_branch_unmapped == 1


def test_no_spec_call_when_the_flag_is_off() -> None:
    source = _source()
    api = _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))
    source.config = SigmaSourceConfig(
        client_id="x", client_secret="y", extract_data_model_spec_lineage=False
    )

    assert _build_union(source) == []
    api.get_data_model_spec.assert_not_called()


def test_a_failed_fetch_is_not_drift() -> None:
    """The API client counts and reports the failure; nothing was read."""
    source = _source()
    _with_spec(source, None)

    assert _build_union(source) == []
    assert source.reporter.data_model_spec_drift_detected == 0


def test_an_unsupported_schema_is_reported_and_adds_no_union_edges() -> None:
    """The parser's contract: distrust a read whose schemaVersion differs."""
    source = _source()
    spec = _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"])
    spec["schemaVersion"] = 2
    _with_spec(source, spec)

    assert _build_union(source) == []
    assert source.reporter.data_model_element_fgl_union_resolved == 0
    # The output ref is counted, not deferred as a cross-DM ref.
    assert source.reporter.data_model_element_fgl_union_unresolved == 1
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
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

    assert _build_union(source) == []
    assert source.reporter.data_model_element_fgl_union_resolved == 0
    assert source.reporter.data_model_element_fgl_union_unresolved == 1
    assert source.reporter.data_model_element_fgl_cross_dm_deferred == 0
    assert source.reporter.data_model_spec_drift_detected == 1


def _build_union_with_branch_b(
    source: SigmaSource, b_columns: List[Tuple[str, str]]
) -> list:
    union = _element("u", "U", [_column("u-out", "Out", _READS_OUT)])
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

    assert _build_union(source) == []
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


def test_a_branch_lineage_omits_is_an_upstream_but_not_orphan_recovery() -> None:
    source = _source()
    _with_spec(source, _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"]))
    union = _element("u", "U", [_column("u-out", "Out", _READS_OUT)], ["a"])
    data_model = _data_model(
        [union, _upstream_element("a", "A", ["a"]), _upstream_element("b", "B", ["b"])]
    )

    lineage = source._gen_data_model_element_upstream_lineage(
        union,
        data_model,
        _urn("u"),
        elementId_to_dataset_urn={"a": _urn("a"), "b": _urn("b")},
        element_name_to_eids={"a": ["a"], "b": ["b"]},
        warehouse_url_id_map={},
    )

    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == [_urn("a"), _urn("b")]
    assert source.reporter.data_model_element_intra_upstreams_recovered == 0
    assert source.reporter.data_model_element_union_upstreams_added == 1


def test_drift_is_counted_per_model_but_logged_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = _source()
    spec = _union_spec([_BRANCH_A, _BRANCH_B], ["[a]", "[b]"])
    spec["schemaVersion"] = 2
    _with_spec(source, spec)
    now = dt.datetime.now(dt.timezone.utc)

    with caplog.at_level("WARNING"):
        for dm_id in ("dm-1", "dm-2"):
            source._get_dm_spec_index(
                SigmaDataModel(
                    dataModelId=dm_id, name="DM", createdAt=now, updatedAt=now
                )
            )

    assert source.reporter.data_model_spec_drift_detected == 2
    assert caplog.text.count("Sigma Data Model spec not fully read") == 1


# ---------------------------------------------------------------------------
# Join keys from /spec
# ---------------------------------------------------------------------------

_SIDE_A = {"kind": "table", "elementId": "a"}
_SIDE_B = {"kind": "table", "elementId": "b"}
_SIDE_C = {"kind": "table", "elementId": "c"}
_SIDE_X = {"kind": "table", "elementId": "x"}


def _join(
    left: dict,
    right: dict,
    *,
    join_type: str = "inner",
    left_formula: str = "[Key]",
    right_formula: str = "[Key]",
) -> dict:
    return {
        "joinType": join_type,
        "left": left,
        "right": right,
        "columns": [{"left": left_formula, "right": right_formula}],
    }


def _spec_of_joins(joins_by_element: Dict[str, List[dict]]) -> dict:
    """A /spec whose join elements each join their entries in order."""
    return {
        "schemaVersion": 1,
        "dataModelId": "dm-1",
        "pages": [
            {
                "id": "p1",
                "elements": [
                    {
                        "id": element_id,
                        "kind": "table",
                        "source": {
                            "kind": "join",
                            "primarySource": joins[0]["left"],
                            "joins": joins,
                        },
                    }
                    for element_id, joins in joins_by_element.items()
                ],
            }
        ],
    }


def _join_spec(
    *, join_type: str = "inner", right: dict = _SIDE_B, right_formula: str = "[Key]"
) -> dict:
    return _spec_of_joins(
        {"j": [_join(_SIDE_A, right, join_type=join_type, right_formula=right_formula)]}
    )


def _build_join(
    source: SigmaSource,
    *,
    element_id: str = "j",
    source_ids: List[str] | None = None,
    listed: Set[str] | None = None,
    discovered: Set[str] | None = None,
    join_key_upstreams: Set[str] | None = None,
    key: str = "Key",
) -> list:
    """Element ``element_id`` reads A's key column; J joins A to B on it."""
    side_a = _upstream_element("a", "A", [key])
    side_b = _upstream_element("b", "B", [key])
    join = _element(
        "j", "J", [_column("j-key", key, f"[A/{key}]")], source_ids=["a", "b"]
    )
    elements = [side_a, side_b, join]
    if element_id == "j":
        element = join
    else:
        element = _element(
            element_id,
            element_id.upper(),
            [_column(f"{element_id}-key", key, f"[A/{key}]")],
            source_ids=source_ids or [],
        )
        elements.append(element)
    return _build(
        source,
        element,
        element_name_to_eids={"a": ["a"], "b": ["b"], "j": ["j"]},
        elementId_to_dataset_urn={e.elementId: _urn(e.elementId) for e in elements},
        entity_level_upstream_urns=(
            {_urn("a"), _urn("b")} if listed is None else listed
        ),
        upstream_elements=[e for e in elements if e is not element],
        discovered_upstreams=discovered,
        join_key_upstreams=join_key_upstreams,
    )


def _key_edges(lineages: list) -> Dict[str, float]:
    return {lin.upstreams[0]: lin.confidenceScore for lin in lineages}


def _build_over(
    source: SigmaSource,
    element: SigmaDataModelElement,
    others: List[SigmaDataModelElement],
    listed: List[str],
) -> list:
    """Build ``element`` in a model with ``others``; ``listed`` ids are its
    /lineage upstreams. Each element id is its lower-cased name."""
    return _build(
        source,
        element,
        element_name_to_eids={e.elementId: [e.elementId] for e in others},
        elementId_to_dataset_urn={
            e.elementId: _urn(e.elementId) for e in [element, *others]
        },
        entity_level_upstream_urns={_urn(e) for e in listed},
        upstream_elements=others,
    )


@pytest.mark.parametrize(("join_type", "score"), [("inner", 0.7), ("left-outer", 0.6)])
def test_a_join_output_column_gets_the_other_key(join_type: str, score: float) -> None:
    """The formula names A's key; the ON clause says B's key is equal to it."""
    source = _source()
    _with_spec(source, _join_spec(join_type=join_type))

    lineages = _build_join(source)

    assert _key_edges(lineages) == {
        builder.make_schema_field_urn(_urn("a"), "Key"): 1.0,
        builder.make_schema_field_urn(_urn("b"), "Key"): score,
    }
    assert {lin.upstreams[0]: lin.transformOperation for lin in lineages} == {
        builder.make_schema_field_urn(_urn("a"), "Key"): None,
        builder.make_schema_field_urn(_urn("b"), "Key"): "JOIN_KEY",
    }
    assert source.reporter.data_model_element_fgl_join_key_resolved == 1


@pytest.mark.parametrize(
    ("left_formula", "right_formula", "bare"),
    [
        ("[Key]", "[Key]", True),
        ("Left([Key], 3)", "[Key]", False),
        ("[Key]", "Coalesce([Key], 0)", False),
    ],
    ids=["bare", "left-transformed", "right-transformed"],
)
def test_only_a_bare_key_on_both_sides_gets_the_other_key(
    left_formula: str, right_formula: str, bare: bool
) -> None:
    """Left([Zip], 3) = [Zip3] matches values, but Zip is not Zip3."""
    source = _source()
    _with_spec(
        source,
        _spec_of_joins(
            {
                "j": [
                    _join(
                        _SIDE_A,
                        _SIDE_B,
                        left_formula=left_formula,
                        right_formula=right_formula,
                    )
                ]
            }
        ),
    )

    edges = _key_edges(_build_join(source))

    assert (builder.make_schema_field_urn(_urn("b"), "Key") in edges) is bare
    assert source.reporter.data_model_join_key_partner_transformed == (0 if bare else 1)


@pytest.mark.parametrize("key", ["Order Id (Orders)", "Revenue, USD"])
def test_a_key_whose_name_the_field_urn_encodes_gets_the_other_key(key: str) -> None:
    """A schemaField urn encodes ( ) and , in the column name; Sigma names a
    join's duplicate key column like "Customer Id (Customers)"."""
    source = _source()
    _with_spec(
        source,
        _spec_of_joins(
            {
                "j": [
                    _join(
                        _SIDE_A,
                        _SIDE_B,
                        left_formula=f"[{key}]",
                        right_formula=f"[{key}]",
                    )
                ]
            }
        ),
    )

    edges = _key_edges(_build_join(source, key=key))

    assert edges == {
        builder.make_schema_field_urn(_urn("a"), key): 1.0,
        builder.make_schema_field_urn(_urn("b"), key): 0.7,
    }


@pytest.mark.parametrize("a_first", [True, False])
def test_the_best_score_wins_whatever_the_formula_order(a_first: bool) -> None:
    """A left-joins X, X inner-joins C: reached from A, X's key is outer; from
    C, inner. The inner reading wins in either argument order."""
    source = _source()
    _with_spec(
        source,
        _spec_of_joins(
            {
                "j": [
                    _join(_SIDE_A, _SIDE_X, join_type="left-outer"),
                    _join(_SIDE_X, _SIDE_C),
                ]
            }
        ),
    )
    sides = [_upstream_element(e, e.upper(), ["Key"]) for e in ("a", "x", "c")]
    refs = ["[A/Key]", "[C/Key]"] if a_first else ["[C/Key]", "[A/Key]"]
    join = _element(
        "j",
        "J",
        [_column("j-key", "Key", f"Coalesce({refs[0]}, {refs[1]})")],
        source_ids=["a", "x", "c"],
    )

    edges = _key_edges(_build_over(source, join, sides, ["a", "x", "c"]))

    assert edges[builder.make_schema_field_urn(_urn("x"), "Key")] == 0.7


@pytest.mark.parametrize(("first_join", "score"), [("inner", 0.7), ("left-outer", 0.6)])
def test_chained_join_keys_are_followed(first_join: str, score: float) -> None:
    """A = B then B = C: C's key reaches A's through B, and an outer hop
    anywhere on the path scores as outer."""
    source = _source()
    _with_spec(
        source,
        _spec_of_joins(
            {
                "j": [
                    _join(_SIDE_A, _SIDE_B, join_type=first_join),
                    _join(_SIDE_B, _SIDE_C),
                ]
            }
        ),
    )
    sides = [_upstream_element(e, e.upper(), ["Key"]) for e in ("a", "b", "c")]
    join = _element(
        "j", "J", [_column("j-key", "Key", "[A/Key]")], source_ids=["a", "b", "c"]
    )

    edges = _key_edges(_build_over(source, join, sides, ["a", "b", "c"]))

    assert edges == {
        builder.make_schema_field_urn(_urn("a"), "Key"): 1.0,
        builder.make_schema_field_urn(_urn("b"), "Key"): score,
        builder.make_schema_field_urn(_urn("c"), "Key"): score,
    }
    assert source.reporter.data_model_element_fgl_join_key_resolved == 2


_ORDERS = {"kind": "table", "elementId": "orders"}
_DATES = {"kind": "table", "elementId": "dates"}


def _orders_and_dates() -> List[SigmaDataModelElement]:
    return [
        _upstream_element("orders", "Orders", ["Order Date", "Ship Date"]),
        _upstream_element("dates", "Dates", ["Date"]),
    ]


def _two_joins_on_dates() -> dict:
    """J1 joins Dates on Order Date; J2 joins J1 to Dates again on Ship Date."""
    return _spec_of_joins(
        {
            "j1": [
                _join(
                    _ORDERS, _DATES, left_formula="[Order Date]", right_formula="[Date]"
                )
            ],
            "j2": [
                _join(
                    {"kind": "table", "elementId": "j1"},
                    _DATES,
                    left_formula="[Ship Date]",
                    right_formula="[Date]",
                )
            ],
        }
    )


def _j1() -> SigmaDataModelElement:
    return _element(
        "j1",
        "J1",
        [
            _column("j1-od", "Order Date", "[Orders/Order Date]"),
            _column("j1-sd", "Ship Date", "[Orders/Ship Date]"),
        ],
        source_ids=["orders", "dates"],
    )


def test_keys_do_not_chain_across_join_elements_sharing_a_side() -> None:
    """The two Dates rows differ, so J2's Ship Date is not Order Date."""
    source = _source()
    _with_spec(source, _two_joins_on_dates())
    j2 = _element(
        "j2",
        "J2",
        [_column("j2-sd", "Ship Date", "[J1/Ship Date]")],
        source_ids=["j1", "dates"],
    )

    edges = _key_edges(
        _build_over(source, j2, [*_orders_and_dates(), _j1()], ["j1", "dates"])
    )

    assert edges == {
        builder.make_schema_field_urn(_urn("j1"), "Ship Date"): 1.0,
        builder.make_schema_field_urn(_urn("dates"), "Date"): 0.7,
    }


def test_an_earlier_joins_rule_does_not_apply_to_this_joins_rows() -> None:
    """J2's Ship Day reads Dates directly; J1's Order Date = Date holds for
    J1's Dates row, not J2's."""
    source = _source()
    _with_spec(source, _two_joins_on_dates())
    j2 = _element(
        "j2",
        "J2",
        [_column("j2-day", "Ship Day", "[Dates/Date]")],
        source_ids=["j1", "dates"],
    )

    edges = _key_edges(
        _build_over(source, j2, [*_orders_and_dates(), _j1()], ["j1", "dates"])
    )

    assert edges == {
        builder.make_schema_field_urn(_urn("dates"), "Date"): 1.0,
        builder.make_schema_field_urn(_urn("j1"), "Ship Date"): 0.7,
    }


def test_a_self_join_after_a_dropped_first_key_adds_no_keys() -> None:
    """The first join's key is composite, so no predicate survives to name
    the primary; Orders is still joined to itself."""
    orders_ref = {"kind": "table", "elementId": "orders"}
    source = _source()
    spec = _spec_of_joins(
        {
            "j": [
                _join(
                    orders_ref,
                    {"kind": "table", "elementId": "customers"},
                    left_formula='Concat([First], " ", [Last])',
                    right_formula="[Full Name]",
                ),
                _join(
                    orders_ref,
                    orders_ref,
                    left_formula="[Parent Id]",
                    right_formula="[Order Id]",
                ),
                _join(
                    orders_ref,
                    {"kind": "table", "elementId": "returns"},
                    left_formula="[Parent Id]",
                    right_formula="[Order Ref]",
                ),
            ]
        }
    )
    _with_spec(source, spec)
    sides = [
        _upstream_element("orders", "Orders", ["Order Id", "Parent Id"]),
        _upstream_element("customers", "Customers", ["Full Name"]),
        _upstream_element("returns", "Returns", ["Order Ref"]),
    ]
    join = _element(
        "j",
        "J",
        [_column("j-id", "Order Id", "[Orders/Order Id]")],
        source_ids=["orders", "customers", "returns"],
    )

    edges = _key_edges(
        _build_over(source, join, sides, ["orders", "customers", "returns"])
    )

    assert list(edges) == [builder.make_schema_field_urn(_urn("orders"), "Order Id")]
    assert source.reporter.data_model_join_key_repeated_side_skipped == 1


def test_a_join_element_joining_one_side_twice_adds_no_keys() -> None:
    """/spec join entries have no id, so the two Dates keys cannot be told
    apart."""
    source = _source()
    _with_spec(
        source,
        _spec_of_joins(
            {
                "j": [
                    _join(
                        _ORDERS,
                        _DATES,
                        left_formula="[Order Date]",
                        right_formula="[Date]",
                    ),
                    _join(
                        _ORDERS,
                        _DATES,
                        left_formula="[Ship Date]",
                        right_formula="[Date]",
                    ),
                ]
            }
        ),
    )
    join = _element(
        "j",
        "J",
        [_column("j-sd", "Ship Date", "[Orders/Ship Date]")],
        source_ids=["orders", "dates"],
    )

    edges = _key_edges(
        _build_over(source, join, _orders_and_dates(), ["orders", "dates"])
    )

    assert list(edges) == [builder.make_schema_field_urn(_urn("orders"), "Ship Date")]
    assert source.reporter.data_model_join_key_repeated_side_skipped == 1


def test_a_compound_key_in_one_entry_is_not_a_repeated_side() -> None:
    source = _source()
    spec = _join_spec()
    spec["pages"][0]["elements"][0]["source"]["joins"][0]["columns"].append(
        {"left": "[Other]", "right": "[Other]"}
    )
    _with_spec(source, spec)

    edges = _key_edges(_build_join(source))

    assert builder.make_schema_field_urn(_urn("b"), "Key") in edges
    assert source.reporter.data_model_join_key_repeated_side_skipped == 0


def test_a_descendant_of_the_join_gets_no_key() -> None:
    """Only the join element's own columns read its ON clause."""
    source = _source()
    _with_spec(source, _join_spec())

    edges = _key_edges(_build_join(source, element_id="d", source_ids=["j"]))

    assert list(edges) == [builder.make_schema_field_urn(_urn("a"), "Key")]
    assert source.reporter.data_model_element_fgl_join_key_resolved == 0


def test_an_element_not_reading_through_the_join_gets_no_key() -> None:
    """Reading A's key column alone does not apply J's equality."""
    source = _source()
    _with_spec(source, _join_spec())

    edges = _key_edges(_build_join(source, element_id="e", source_ids=["a"]))

    assert list(edges) == [builder.make_schema_field_urn(_urn("a"), "Key")]
    assert source.reporter.data_model_element_fgl_join_key_resolved == 0


@pytest.mark.parametrize(
    ("right", "right_formula", "unmapped", "unresolved"),
    [
        (
            {"kind": "warehouse-table", "connectionId": "c", "path": ["D", "S", "T"]},
            "[Key]",
            1,
            0,
        ),
        (
            {"kind": "data-model", "elementId": "b", "dataModelId": "dm-2"},
            "[Key]",
            1,
            0,
        ),
        (_SIDE_B, "[Missing]", 0, 1),
    ],
    ids=["warehouse-table", "another-data-model", "column-absent"],
)
def test_a_predicate_that_cannot_be_mapped_is_counted(
    right: dict, right_formula: str, unmapped: int, unresolved: int
) -> None:
    source = _source()
    _with_spec(source, _join_spec(right=right, right_formula=right_formula))

    edges = _key_edges(_build_join(source))

    assert list(edges) == [builder.make_schema_field_urn(_urn("a"), "Key")]
    assert source.reporter.data_model_join_key_partner_unmapped == unmapped
    assert source.reporter.data_model_join_key_partner_unresolved == unresolved


def test_partners_are_built_once_per_data_model() -> None:
    """Counted per model, not once per element that asks."""
    source = _source()
    _with_spec(source, _join_spec(right_formula="[Missing]"))

    _build_join(source)
    _build_join(source, element_id="d", source_ids=["j"])

    assert source.reporter.data_model_join_key_partner_unresolved == 1


def test_an_unsupported_schema_adds_no_join_keys() -> None:
    source = _source()
    spec = _join_spec()
    spec["schemaVersion"] = 2
    _with_spec(source, spec)

    assert len(_build_join(source)) == 1
    assert source.reporter.data_model_element_fgl_join_key_resolved == 0


def test_an_unlisted_key_partner_becomes_an_upstream() -> None:
    """B is not among J's listed upstreams here, so an edge to B's key needs B
    declared as an upstream too, apart from the siblings orphan recovery
    adds."""
    source = _source()
    _with_spec(source, _join_spec())
    discovered: Set[str] = set()
    join_key_upstreams: Set[str] = set()

    _build_join(
        source,
        listed={_urn("a")},
        discovered=discovered,
        join_key_upstreams=join_key_upstreams,
    )

    assert join_key_upstreams == {_urn("b")}
    assert _urn("b") not in discovered
