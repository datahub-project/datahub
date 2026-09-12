import datetime as dt
from typing import Dict, List, Set
from unittest.mock import MagicMock

from datahub.emitter import mce_builder as builder
from datahub.ingestion.source.sigma.config import (
    SigmaSourceConfig,
    SigmaSourceReport,
)
from datahub.ingestion.source.sigma.connection_registry import (
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.data_classes import (
    SigmaDataModel,
    SigmaDataModelColumn,
    SigmaDataModelElement,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource, _WarehouseTableRef
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineageClass,
)


def _source() -> SigmaSource:
    source = SigmaSource.__new__(SigmaSource)
    # __new__ skips __init__, so attributes a real instance always
    # has must be set here or diagnostics reading them raise.
    # Mirrors SigmaSource.__init__, which __new__ skips. Calling the real
    # initialiser keeps this from drifting again.
    source._init_diagnostic_state()
    source.reporter = SigmaSourceReport()
    source.dm_element_urn_by_name = {}
    source.dm_element_urn_to_cols = {}
    source._upstream_schema_unavailable_warned = set()
    # No /spec: these tests cover formula-derived lineage, and a Data Model with
    # no readable join predicates must leave that lineage exactly as it was.
    source.config = SigmaSourceConfig.model_validate(
        {"client_id": "t", "client_secret": "t"}
    )
    source._dm_spec_index_cache = {}
    source._join_partner_cache = None
    source._dm_column_lookup_cache = None
    source._dm_ancestors_cache = {}
    source.dm_keys_by_element_id = {}
    source.dm_element_urn_by_key_and_eid = {}
    source.sigma_api = MagicMock()
    source.sigma_api.get_data_model_spec.return_value = None
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
        # Deliberately NOT "Shared": a ref naming the element's OWN name
        # resolves to that element's own source, so a same-named sibling is
        # never the referent. This test is about the cross-DM guard on a
        # genuine orphan, which needs the ref to name a DIFFERENT element.
        "Consumer",
        [_column("c1", "x", "[Shared/x]")],
        # source_ids=[] — no cross-DM refs
    )
    # Sibling does NOT own "x": schema-based orphan recovery must not fire,
    # so the cross-DM guard under test is still the behaviour exercised.
    sibling = _upstream_element("sibling-eid", "Shared", ["other"])

    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        element_name_to_eids={"shared": ["sibling-eid"]},
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
        # Deliberately NOT "Shared": a ref naming the element's OWN name
        # resolves to that element's own source, so a same-named sibling is
        # never the referent. This test is about the cross-DM guard on a
        # genuine orphan, which needs the ref to name a DIFFERENT element.
        "Consumer",
        [_column("c1", "x", "[Shared/x]")],
        # Intra-DM source IDs only — no "/" separator, not cross-DM shaped.
        source_ids=["some-intra-dm-eid"],
    )
    # Sibling does NOT own "x": schema-based orphan recovery must not fire,
    # so the cross-DM guard under test is still the behaviour exercised.
    sibling = _upstream_element("sibling-eid", "Shared", ["other"])

    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        element_name_to_eids={"shared": ["sibling-eid"]},
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
        # Deliberately NOT "Shared": a ref naming the element's OWN name
        # resolves to that element's own source, so a same-named sibling is
        # never the referent. This test is about the cross-DM guard on a
        # genuine orphan, which needs the ref to name a DIFFERENT element.
        "Consumer",
        [_column("c1", "x", "[Shared/x]")],
        # inode-shaped entry has '/' but is NOT a cross-DM source ID.
        source_ids=["inode-abc123/some-suffix"],
    )
    # Sibling does NOT own "x": schema-based orphan recovery must not fire,
    # so the cross-DM guard under test is still the behaviour exercised.
    sibling = _upstream_element("sibling-eid", "Shared", ["other"])

    lineages = _build(
        source,
        consumer,
        element_dataset_urn=consumer_urn,
        element_name_to_eids={"shared": ["sibling-eid"]},
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
    element = _element("b", "B", [_column("b-account-id", "Col Id", "")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"a": ["a"], "b": ["b"]},
        elementId_to_dataset_urn={"a": upstream_urn, "b": downstream_urn},
        entity_level_upstream_urns={upstream_urn},
        upstream_elements=[_upstream_element("a", "A", ["Col Id"])],
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
    """The reported shape: [GRP_A/GRP_A DIM_B/Col Id].

    An element named "GRP_A" also exists, so the first segment resolves to the
    wrong sibling. The edge must land on the owning element instead.
    """
    source = _source()
    join_urn = _urn("join")
    owner_urn = _urn("owner")
    downstream_urn = _urn("consumer")
    element = _element(
        "consumer",
        "Consumer",
        [_column("c-col-id", "Col Id", "[GRP_A/GRP_A DIM_B/Col Id]")],
    )

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"grp_a": ["join"], "grp_a dim_b": ["owner"]},
        elementId_to_dataset_urn={"join": join_urn, "owner": owner_urn},
        entity_level_upstream_urns={join_urn, owner_urn},
        upstream_elements=[
            # The join element deliberately has a column that is NOT the target,
            # mirroring production: the first segment matches but the qualified
            # column name cannot exist there.
            _upstream_element("join", "GRP_A", ["Some Other Column"]),
            _upstream_element("owner", "GRP_A DIM_B", ["Col Id"]),
        ],
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(owner_urn, "Col Id")]
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
    element = _element("consumer", "Consumer", [_column("c-x", "x", "[GRP_A/E2/col]")])

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"grp_a": ["join"]},
        elementId_to_dataset_urn={"join": join_urn},
        entity_level_upstream_urns={join_urn},
        upstream_elements=[_upstream_element("join", "GRP_A", ["unrelated"])],
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


def test_inode_column_id_still_tries_cross_dm_when_warehouse_fails() -> None:
    """A failed warehouse lookup must not abort cross-DM resolution.

    On a cross-DM-sourced element the warehouse inode belongs to the PRODUCER,
    so failing to resolve it locally says nothing about whether the formula's
    ref resolves. Short-circuiting there lost every such edge: the observed
    element carried columnId='inode-<urlId>/<COL>' with formula
    '[Producer/Column]' and emitted nothing at all.
    """
    source = _source()
    dm_url_id = "producer-dm"
    producer_urn = _urn("producer-el")
    downstream_urn = _urn("consumer")
    element = _element(
        "consumer",
        "Consumer",
        # inode-shaped columnId, but no warehouse map is supplied so the
        # warehouse path must fail.
        [_column("inode-abc/COL_ID", "Col Id", "[Producer/Col Id]")],
        source_ids=[f"{dm_url_id}/suffix"],
    )
    source.dm_element_urn_by_name = {dm_url_id: {"producer": [producer_urn]}}
    source.dm_element_urn_to_cols = {producer_urn: {"col id": "Col Id"}}

    lineages = _build(
        source,
        element,
        element_dataset_urn=downstream_urn,
        entity_level_upstream_urns={producer_urn},
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [
        builder.make_schema_field_urn(producer_urn, "Col Id")
    ]
    assert source.reporter.data_model_element_fgl_cross_dm_resolved == 1
    # The warehouse attempt still failed and is still reported.
    assert source.reporter.data_model_element_fgl_warehouse_passthrough_deferred == 1


def test_orphan_ref_recovered_when_sibling_owns_the_column() -> None:
    """/lineage omitting an intra-DM sibling is a reporting gap, not evidence.

    Where the named sibling demonstrably owns the referenced column, the ref is
    trustworthy and the edge is emitted, with the sibling promoted to an
    entity-level upstream. Measured at 162 of 167 orphan drops on a real tenant.
    """
    source = _source()
    sibling_urn = _urn("sibling-eid")
    downstream_urn = _urn("consumer")
    discovered: set = set()
    element = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])

    lineages = source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=downstream_urn,
        element_name_to_eids={"shared": ["sibling-eid"]},
        elementId_to_dataset_urn={"sibling-eid": sibling_urn},
        # Sigma did not list the sibling as an upstream.
        entity_level_upstream_urns=set(),
        data_model=_data_model(
            [element, _upstream_element("sibling-eid", "Shared", ["x"])]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=discovered,
    )

    assert len(lineages) == 1
    assert lineages[0].upstreams == [builder.make_schema_field_urn(sibling_urn, "x")]
    assert source.reporter.data_model_element_fgl_orphan_recovered == 1
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 0
    assert discovered == {sibling_urn}


def test_orphan_ref_still_dropped_when_sibling_lacks_the_column() -> None:
    """Recovery is earned by the schema, not assumed from the name match."""
    source = _source()
    sibling_urn = _urn("sibling-eid")
    element = _element("consumer", "Consumer", [_column("c1", "x", "[Shared/x]")])

    lineages = _build(
        source,
        element,
        element_name_to_eids={"shared": ["sibling-eid"]},
        elementId_to_dataset_urn={"sibling-eid": sibling_urn},
        entity_level_upstream_urns=set(),
        upstream_elements=[_upstream_element("sibling-eid", "Shared", ["other"])],
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_dropped_orphan_upstream == 1
    assert source.reporter.data_model_element_fgl_orphan_recovered == 0


# ---------------------------------------------------------------------------
# Join-key lineage from /spec
# ---------------------------------------------------------------------------

_LEFT_COL_ID = "a-col-k"
_RIGHT_COL_ID = "c-col-k"


def _join_spec_source(source: SigmaSource, join_type: str = "left-outer") -> None:
    """Make /spec report one join predicate: A.col_k == C.col_k.

    Shape mirrors a live tenant: the predicate lives under
    ``source.joins[].columns[]``, and each side's element is named by the
    sibling ``joins[].left`` / ``.right`` descriptor.
    """
    spec_mock = MagicMock()
    source.sigma_api = spec_mock
    spec_mock.get_data_model_spec.return_value = {
        "kind": "data-model",
        "pages": [
            {
                "elements": [
                    {
                        "id": "a",
                        "columns": [{"id": _LEFT_COL_ID, "formula": ""}],
                        "source": {"kind": "warehouse-table"},
                    },
                    {
                        "id": "c",
                        "columns": [{"id": _RIGHT_COL_ID, "formula": ""}],
                        "source": {"kind": "warehouse-table"},
                    },
                    {
                        "id": "j",
                        "columns": [],
                        "source": {
                            "kind": "join",
                            "primarySource": {"elementId": "a", "kind": "table"},
                            "joins": [
                                {
                                    "joinType": join_type,
                                    "left": {"elementId": "a", "kind": "table"},
                                    "right": {"elementId": "c", "kind": "table"},
                                    # Sides are Sigma FORMULAS, not identifiers:
                                    # a live tenant spells them "[Col]" and
                                    # "Coalesce([Col], -2)".
                                    "columns": [
                                        {
                                            "left": "[col_k]",
                                            "right": "Coalesce([col_k], -2)",
                                        }
                                    ],
                                }
                            ],
                        },
                    },
                ]
            }
        ],
    }


def test_join_key_edge_is_scored_below_a_formula_edge() -> None:
    """A predicate is an equality, not a copy -- consumers must be able to tell."""
    source = _source()
    _join_spec_source(source)
    a_urn, c_urn, b_urn = _urn("a"), _urn("c"), _urn("b")
    # source_ids=["j"]: B reads through the join, which is what makes the
    # predicate evidence about B at all.
    element = _element(
        "b", "B", [_column("b-key", "col_k", "[A/col_k]")], source_ids=["j"]
    )

    lineages = source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=b_urn,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": a_urn, "c": c_urn},
        entity_level_upstream_urns={a_urn},
        data_model=_data_model(
            [
                element,
                _element("a", "A", [_column(_LEFT_COL_ID, "col_k", None)]),
                _element("c", "C", [_column(_RIGHT_COL_ID, "col_k", None)]),
                _element("j", "J", [], source_ids=["a", "c"]),
            ]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=set(),
    )

    by_upstream = {
        (lineage.upstreams or [])[0]: lineage.confidenceScore for lineage in lineages
    }
    assert by_upstream[builder.make_schema_field_urn(a_urn, "col_k")] == 1.0
    # The fixture's joinType is "left-outer" -- one of the four values Sigma's
    # write API actually accepts -- so the equality holds only on matched rows
    # and the edge lands in the outer-join tier rather than at 0.7. It read
    # "left" before, which Sigma rejects outright, and passed only because the
    # code's join-type set had guessed the same invented value.
    assert by_upstream[builder.make_schema_field_urn(c_urn, "col_k")] == 0.6


def test_inner_join_key_edge_scores_above_an_outer_one() -> None:
    """An inner join asserts the equality for every row it produces."""
    source = _source()
    _join_spec_source(source, join_type="inner")
    a_urn, c_urn, b_urn = _urn("a"), _urn("c"), _urn("b")
    element = _element(
        "b", "B", [_column("b-key", "col_k", "[A/col_k]")], source_ids=["j"]
    )

    lineages = source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=b_urn,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": a_urn, "c": c_urn},
        entity_level_upstream_urns={a_urn},
        data_model=_data_model(
            [
                element,
                _element("a", "A", [_column(_LEFT_COL_ID, "col_k", None)]),
                _element("c", "C", [_column(_RIGHT_COL_ID, "col_k", None)]),
                _element("j", "J", [], source_ids=["a", "c"]),
            ]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=set(),
    )

    by_upstream = {
        (lineage.upstreams or [])[0]: lineage.confidenceScore for lineage in lineages
    }
    assert by_upstream[builder.make_schema_field_urn(c_urn, "col_k")] == 0.7


def _foreign_spec_source(source: SigmaSource) -> None:
    """/spec where the RIGHT side belongs to a different Data Model.

    Sigma lets a model join in an element it does not own. The side still
    carries an elementId, so the parser accepts it, but the element is absent
    from this model's own element list.
    """
    spec_mock = MagicMock()
    source.sigma_api = spec_mock
    spec_mock.get_data_model_spec.return_value = {
        "kind": "data-model",
        "pages": [
            {
                "elements": [
                    {
                        "id": "a",
                        "columns": [{"id": _LEFT_COL_ID, "formula": ""}],
                        "source": {"kind": "warehouse-table"},
                    },
                    {
                        "id": "j",
                        "columns": [],
                        "source": {
                            "kind": "join",
                            "primarySource": {"elementId": "a", "kind": "table"},
                            "joins": [
                                {
                                    "joinType": "inner",
                                    "left": {"elementId": "a", "kind": "table"},
                                    "right": {
                                        "dataModelId": "other-dm",
                                        "elementId": "shared",
                                        "kind": "table",
                                    },
                                    "columns": [
                                        {"left": "[col_k]", "right": "[col_k]"}
                                    ],
                                }
                            ],
                        },
                    },
                ]
            }
        ],
    }


_FOREIGN_URN = _urn("other-dm.shared")


def _register_foreign_element(source: SigmaSource, *, keys: List[str]) -> None:
    for key in keys:
        source.dm_element_urn_by_key_and_eid[(key, "shared")] = _FOREIGN_URN
        source.dm_keys_by_element_id.setdefault("shared", set()).add(key)
    source.dm_element_urn_to_cols[_FOREIGN_URN] = {"col_k": "Col K"}


def _build_with_foreign(
    source: SigmaSource, element: SigmaDataModelElement
) -> List[FineGrainedLineageClass]:
    a_urn = _urn("a")
    return source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=_urn("b"),
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": a_urn},
        entity_level_upstream_urns={a_urn},
        data_model=_data_model(
            [
                element,
                _element("a", "A", [_column(_LEFT_COL_ID, "col_k", None)]),
                _element("j", "J", [], source_ids=["a"]),
            ]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=set(),
    )


def test_join_side_owned_by_another_data_model_resolves() -> None:
    """The shape behind 9 of 10 unresolved predicates on one tenant.

    A shared mapping element is joined into many models. The side names it by
    elementId plus dataModelId, and it is absent from the joining model's own
    element list, so a lookup restricted to that model finds nothing.
    """
    source = _source()
    _foreign_spec_source(source)
    _register_foreign_element(source, keys=["other-dm"])
    element = _element(
        "b", "B", [_column("b-key", "col_k", "[A/col_k]")], source_ids=["j"]
    )

    lineages = _build_with_foreign(source, element)

    upstreams = [(lineage.upstreams or [])[0] for lineage in lineages]
    assert builder.make_schema_field_urn(_FOREIGN_URN, "Col K") in upstreams
    assert source.reporter.data_model_join_key_foreign_resolved == 1
    assert source.reporter.data_model_element_fgl_join_key_resolved == 1


def test_ambiguous_foreign_element_id_is_refused() -> None:
    """Element ids repeat across models, so an unpinned id must not be guessed."""
    source = _source()
    _foreign_spec_source(source)
    # Two models define 'shared', and the side's dataModelId names neither.
    source.dm_element_urn_by_key_and_eid[("dm-x", "shared")] = _urn("dm-x.shared")
    source.dm_element_urn_by_key_and_eid[("dm-y", "shared")] = _urn("dm-y.shared")
    source.dm_keys_by_element_id["shared"] = {"dm-x", "dm-y"}
    element = _element(
        "b", "B", [_column("b-key", "col_k", "[A/col_k]")], source_ids=["j"]
    )

    lineages = _build_with_foreign(source, element)

    assert [(lineage.upstreams or [])[0] for lineage in lineages] == [
        builder.make_schema_field_urn(_urn("a"), "col_k")
    ]
    assert source.reporter.data_model_join_key_foreign_resolved == 0
    assert source.reporter.data_model_join_key_foreign_dm_unknown == 1


def test_element_off_the_join_path_gets_no_join_key_edge() -> None:
    """A predicate constrains only the elements that read through its join.

    D references the same key column as the join's left side but never touches
    the join. Expanding it would assert an equality D's data path never applies
    -- lineage invented from a predicate about somebody else.
    """
    source = _source()
    _join_spec_source(source)
    a_urn, c_urn, d_urn = _urn("a"), _urn("c"), _urn("d")
    # source_ids names A directly, NOT the join element 'j'.
    element = _element(
        "d", "D", [_column("d-key", "col_k", "[A/col_k]")], source_ids=["a"]
    )

    lineages = source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=d_urn,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": a_urn, "c": c_urn},
        entity_level_upstream_urns={a_urn},
        data_model=_data_model(
            [
                element,
                _element("a", "A", [_column(_LEFT_COL_ID, "col_k", None)]),
                _element("c", "C", [_column(_RIGHT_COL_ID, "col_k", None)]),
                _element("j", "J", [], source_ids=["a", "c"]),
            ]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=set(),
    )

    # The formula edge stands; the join partner does not come along with it.
    assert [(lineage.upstreams or [])[0] for lineage in lineages] == [
        builder.make_schema_field_urn(a_urn, "col_k")
    ]
    assert source.reporter.data_model_element_fgl_join_key_resolved == 0
    assert source.reporter.data_model_join_key_out_of_join_path == 1


def test_join_partner_outside_the_run_is_counted_not_emitted() -> None:
    """The partner element was filtered out, so no dangling edge is invented."""
    source = _source()
    _join_spec_source(source)
    a_urn, b_urn = _urn("a"), _urn("b")
    element = _element("b", "B", [_column("b-key", "col_k", "[A/col_k]")])

    lineages = source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=b_urn,
        element_name_to_eids={"a": ["a"]},
        # 'c' deliberately absent.
        elementId_to_dataset_urn={"a": a_urn},
        entity_level_upstream_urns={a_urn},
        data_model=_data_model(
            [element, _element("a", "A", [_column(_LEFT_COL_ID, "col_k", None)])]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=set(),
    )

    assert [(lineage.upstreams or [])[0] for lineage in lineages] == [
        builder.make_schema_field_urn(a_urn, "col_k")
    ]
    assert source.reporter.data_model_join_key_partner_unresolved == 1
    assert source.reporter.data_model_element_fgl_join_key_resolved == 0


def test_column_with_no_edge_gets_no_join_key_edge() -> None:
    """Only columns an edge already reaches are expanded -- never invented."""
    source = _source()
    _join_spec_source(source)
    a_urn, c_urn = _urn("a"), _urn("c")
    # Formula names nothing resolvable, so no edge exists to expand.
    element = _element("b", "B", [_column("b-key", "col_k", "[P_Param]")])

    lineages = source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=_urn("b"),
        element_name_to_eids={},
        elementId_to_dataset_urn={"a": a_urn, "c": c_urn},
        entity_level_upstream_urns=set(),
        data_model=_data_model(
            [
                element,
                _element("a", "A", [_column(_LEFT_COL_ID, "col_k", None)]),
                _element("c", "C", [_column(_RIGHT_COL_ID, "col_k", None)]),
            ]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=set(),
    )

    assert lineages == []
    assert source.reporter.data_model_element_fgl_join_key_resolved == 0


def test_predicates_that_match_no_existing_edge_are_reported() -> None:
    """The last silent way join-key lineage can produce nothing.

    Predicates resolve into partner columns, but no edge this element already
    has lands on a column any predicate names. Without this counter the report
    shows "0 join-key edges" for a reason indistinguishable from "no predicates
    were read at all" -- the same silence that cost two full runs.
    """
    source = _source()
    _join_spec_source(source)
    a_urn, c_urn, b_urn = _urn("a"), _urn("c"), _urn("b")
    # The element's only edge is on a column no predicate mentions.
    element = _element("b", "B", [_column("b-other", "other", "[A/other]")])

    lineages = source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=b_urn,
        element_name_to_eids={"a": ["a"]},
        elementId_to_dataset_urn={"a": a_urn, "c": c_urn},
        entity_level_upstream_urns={a_urn},
        data_model=_data_model(
            [
                element,
                _element(
                    "a",
                    "A",
                    [
                        _column(_LEFT_COL_ID, "col_k", None),
                        _column("a-other", "other", None),
                    ],
                ),
                _element("c", "C", [_column(_RIGHT_COL_ID, "col_k", None)]),
            ]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=set(),
    )

    assert [(lineage.upstreams or [])[0] for lineage in lineages] == [
        builder.make_schema_field_urn(a_urn, "other")
    ]
    assert source.reporter.data_model_element_fgl_join_key_resolved == 0
    assert source.reporter.data_model_join_key_no_matching_edge == 1


# ---------------------------------------------------------------------------
# Union lineage from /spec
# ---------------------------------------------------------------------------


def _union_spec_source(source: SigmaSource, source_columns: List[str]) -> None:
    """Make /spec report a union of A and C feeding element U's column 'k'."""
    spec_mock = MagicMock()
    source.sigma_api = spec_mock
    spec_mock.get_data_model_spec.return_value = {
        "kind": "data-model",
        "pages": [
            {
                "elements": [
                    {
                        "id": "a",
                        "columns": [{"id": "a-k", "formula": ""}],
                        "source": {"kind": "warehouse-table"},
                    },
                    {
                        "id": "c",
                        "columns": [{"id": "c-k", "formula": ""}],
                        "source": {"kind": "warehouse-table"},
                    },
                    {
                        "id": "u",
                        "columns": [{"id": "u-k", "formula": ""}],
                        "source": {
                            "kind": "union",
                            "sources": [
                                {"elementId": "a", "kind": "table"},
                                {"elementId": "c", "kind": "table"},
                            ],
                            "matches": [
                                {
                                    "outputColumnName": "k",
                                    "sourceColumns": source_columns,
                                }
                            ],
                        },
                    },
                ]
            }
        ],
    }


def _build_union(source: SigmaSource, source_columns: List[str]) -> list:
    # Sigma sends each branch column as a FORMULA, not a bare name.
    _union_spec_source(source, [f"[{c}]" if c else c for c in source_columns])
    a_urn, c_urn, u_urn = _urn("a"), _urn("c"), _urn("u")
    # The output column's formula names ONE branch, which is the whole problem:
    # without /spec, branch C is invisible no matter how many branches stack.
    element = _element("u", "U", [_column("u-k", "k", "[A/k]")], source_ids=["a", "c"])
    return source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=u_urn,
        element_name_to_eids={"a": ["a"], "c": ["c"]},
        elementId_to_dataset_urn={"a": a_urn, "c": c_urn, "u": u_urn},
        entity_level_upstream_urns={a_urn, c_urn},
        data_model=_data_model(
            [
                element,
                _upstream_element("a", "A", ["k"]),
                _upstream_element("c", "C", ["k"]),
            ]
        ),
        warehouse_url_id_map={},
        discovered_upstreams=set(),
    )


def test_union_output_column_gets_an_edge_from_every_branch() -> None:
    source = _source()
    lineages = _build_union(source, ["a-k", "c-k"])

    downstream = builder.make_schema_field_urn(_urn("u"), "k")
    upstreams = {
        fgl.upstreams[0] for fgl in lineages if fgl.downstreams == [downstream]
    }
    # Without the union path only the formula's own branch (A) would appear.
    assert builder.make_schema_field_urn(_urn("c"), "k") in upstreams
    assert builder.make_schema_field_urn(_urn("a"), "k") in upstreams
    assert source.reporter.data_model_element_fgl_union_resolved >= 1


def test_union_branch_column_matches_by_display_name_too() -> None:
    """/spec does not say whether it named a column by id or by name."""
    source = _source()
    lineages = _build_union(source, ["k", "k"])

    downstream = builder.make_schema_field_urn(_urn("u"), "k")
    upstreams = {
        fgl.upstreams[0] for fgl in lineages if fgl.downstreams == [downstream]
    }
    assert builder.make_schema_field_urn(_urn("c"), "k") in upstreams


def test_union_branch_naming_an_absent_column_is_counted_not_invented() -> None:
    source = _source()
    lineages = _build_union(source, ["a-k", "no-such-column"])

    downstream = builder.make_schema_field_urn(_urn("u"), "k")
    upstreams = {
        fgl.upstreams[0] for fgl in lineages if fgl.downstreams == [downstream]
    }
    assert builder.make_schema_field_urn(_urn("c"), "k") not in upstreams
    assert source.reporter.data_model_union_branch_column_absent == 1


# ---------------------------------------------------------------------------
# A join whose BOTH sides live in other Data Models
# ---------------------------------------------------------------------------

_LEFT_FOREIGN_URN = _urn("left-dm.dim_a")
_RIGHT_FOREIGN_URN = _urn("right-dm.fact_b")


def _both_sides_foreign_spec(source: SigmaSource) -> None:
    """A join element whose left AND right sides name other models' elements.

    The join itself belongs to this model; only the two inputs are foreign.
    Sigma sends ``dataModelId`` on each side, which is the only thing that can
    pin an element id -- ids repeat across models.
    """
    spec_mock = MagicMock()
    source.sigma_api = spec_mock
    spec_mock.get_data_model_spec.return_value = {
        "kind": "data-model",
        "pages": [
            {
                "elements": [
                    {
                        "id": "j",
                        "columns": [],
                        "source": {
                            "kind": "join",
                            "joins": [
                                {
                                    "joinType": "inner",
                                    "left": {
                                        "dataModelId": "left-dm",
                                        "elementId": "dim_a",
                                        "kind": "table",
                                    },
                                    "right": {
                                        "dataModelId": "right-dm",
                                        "elementId": "fact_b",
                                        "kind": "table",
                                    },
                                    "columns": [
                                        {"left": "[col_k]", "right": "[col_k]"}
                                    ],
                                }
                            ],
                        },
                    },
                ]
            }
        ],
    }
    source.dm_element_urn_by_key_and_eid[("left-dm", "dim_a")] = _LEFT_FOREIGN_URN
    source.dm_element_urn_by_key_and_eid[("right-dm", "fact_b")] = _RIGHT_FOREIGN_URN
    source.dm_keys_by_element_id["dim_a"] = {"left-dm"}
    source.dm_keys_by_element_id["fact_b"] = {"right-dm"}
    source.dm_element_urn_to_cols[_LEFT_FOREIGN_URN] = {"col_k": "Col K"}
    source.dm_element_urn_to_cols[_RIGHT_FOREIGN_URN] = {"col_k": "Col K"}


def test_join_with_both_sides_in_other_models_still_expands() -> None:
    """The reported symptom: a join element linked to only one of its inputs.

    The join's own output column carries a formula naming the LEFT input, so
    /columns produces that edge and nothing else. Both inputs live in other
    Data Models, and the expansion used to require the edge's upstream to be an
    element of THIS model -- which threw the predicate away precisely when both
    sides were foreign, leaving the right-hand input with no column lineage.
    """
    source = _source()
    _both_sides_foreign_spec(source)
    source.dm_element_urn_by_name = {"left-dm": {"dim_a": [_LEFT_FOREIGN_URN]}}
    source.dm_element_urn_to_cols[_LEFT_FOREIGN_URN] = {"col_k": "Col K"}
    element = _element(
        "j",
        "J",
        [_column("j-key", "Col K", "[dim_a/col_k]")],
        source_ids=["left-dm/suffix"],
    )

    lineages = source._build_dm_element_fine_grained_lineages(
        element=element,
        element_dataset_urn=_urn("j"),
        element_name_to_eids={},
        # The formula's own edge resolves cross-DM, so its upstream is a
        # foreign URN -- not a member of elementId_to_dataset_urn.
        elementId_to_dataset_urn={"j": _urn("j")},
        entity_level_upstream_urns={_LEFT_FOREIGN_URN},
        data_model=_data_model([element]),
        warehouse_url_id_map={},
        discovered_upstreams=set(),
    )

    upstreams = {(lineage.upstreams or [""])[0] for lineage in lineages}
    assert builder.make_schema_field_urn(_RIGHT_FOREIGN_URN, "Col K") in upstreams
    assert source.reporter.data_model_element_fgl_join_key_resolved == 1


def test_blank_source_id_is_not_reported_as_an_unrecognised_shape() -> None:
    """ "Unknown shape" invites a hunt for a missing parser branch.

    A blank entry in ``source_ids`` has no shape to recognise. On one tenant
    (2026-09) all 19 "unknown shapes" were this, which would have sent someone
    looking for a Sigma descriptor that was never there.
    """
    source = _source()
    element = _element("b", "B", [_column("b-x", "x", None)], source_ids=[""])

    source._gen_data_model_element_upstream_lineage(
        element,
        _data_model([element]),
        _urn("b"),
        elementId_to_dataset_urn={"b": _urn("b")},
        element_name_to_eids={},
        warehouse_url_id_map={},
    )

    assert source.reporter.data_model_element_upstreams_empty_source_id == 1
    assert source.reporter.data_model_element_upstreams_unknown_shape == 0


class TestDataModelSpecOptOut:
    """``extract_data_model_spec_lineage=False`` must drop BOTH /spec lineages.

    The flag was called ``extract_join_key_lineage`` and documented only in
    terms of join predicates, while union branch edges go through the same
    ``_get_dm_spec_index``. Turning it off therefore silently dropped 1.0-score
    union edges that have nothing to do with the "equality, not a value copy"
    reasoning the flag was named for. This test is what makes the rename
    load-bearing rather than cosmetic.
    """

    def _source(self, *, enabled: bool) -> SigmaSource:
        source = _source()
        source.config = SigmaSourceConfig.model_validate(
            {
                "client_id": "t",
                "client_secret": "t",
                "extract_data_model_spec_lineage": enabled,
            }
        )
        return source

    def test_disabling_it_drops_join_key_edges(self) -> None:
        source = self._source(enabled=False)
        _join_spec_source(source)
        element = _element(
            "b", "B", [_column("b-key", "col_k", "[A/col_k]")], source_ids=["j"]
        )
        source._build_dm_element_fine_grained_lineages(
            element=element,
            element_dataset_urn=_urn("b"),
            element_name_to_eids={"a": ["a"]},
            elementId_to_dataset_urn={"a": _urn("a"), "c": _urn("c")},
            entity_level_upstream_urns={_urn("a")},
            data_model=_data_model(
                [
                    element,
                    _element("a", "A", [_column(_LEFT_COL_ID, "col_k", None)]),
                    _element("c", "C", [_column(_RIGHT_COL_ID, "col_k", None)]),
                    _element("j", "J", [], source_ids=["a", "c"]),
                ]
            ),
            warehouse_url_id_map={},
            discovered_upstreams=set(),
        )
        assert source.reporter.data_model_element_fgl_join_key_resolved == 0

    def test_disabling_it_also_drops_union_branch_edges(self) -> None:
        """The half the old flag name did not describe."""
        source = self._source(enabled=False)
        lineages = _build_union(source, ["a-k", "c-k"])

        upstreams = {(fgl.upstreams or [""])[0] for fgl in lineages}
        assert builder.make_schema_field_urn(_urn("c"), "k") not in upstreams
        assert source.reporter.data_model_element_fgl_union_resolved == 0

    def test_enabled_is_the_default(self) -> None:
        source = _source()
        assert source.config.extract_data_model_spec_lineage is True


def test_same_named_passthrough_siblings_do_not_link_to_each_other() -> None:
    """Sigma names a warehouse-sourced element after its table.

    So every element reading one table carries the same name, and a ref like
    "[THE_TABLE/Brand]" matches its own element's name AND every sibling's.
    Stripping only the element's own id left the siblings as candidates, and the
    resolver picked one -- fabricating a sibling edge instead of pointing at the
    warehouse table /lineage reports. Observed on a live fixture as a MUTUAL
    A<->B cycle across four independent passthroughs.
    """
    source = _source()
    a_urn, b_urn = _urn("a"), _urn("b")
    # Both named after the warehouse table they read, as Sigma names them.
    a = _element("a", "THE_TABLE", [_column("a-brand", "Brand", "[THE_TABLE/Brand]")])
    b = _element("b", "THE_TABLE", [_column("b-brand", "Brand", "[THE_TABLE/Brand]")])

    lineages = _build(
        source,
        a,
        element_dataset_urn=a_urn,
        element_name_to_eids={"the_table": ["a", "b"]},
        elementId_to_dataset_urn={"a": a_urn, "b": b_urn},
        entity_level_upstream_urns={b_urn},
        upstream_elements=[b],
    )

    assert lineages == [], (
        "a same-named sibling is never the referent of a self-named ref; the "
        f"real upstream is the warehouse table. got {lineages}"
    )
    assert source.reporter.data_model_element_fgl_self_named_siblings_skipped == 1


def test_a_ref_naming_a_differently_named_sibling_still_resolves() -> None:
    """The fix must stay narrow: normal sibling resolution is untouched."""
    source = _source()
    up_urn, down_urn = _urn("up"), _urn("down")
    down = _element("down", "Downstream", [_column("d1", "x", "[Upstream/x]")])

    lineages = _build(
        source,
        down,
        element_dataset_urn=down_urn,
        element_name_to_eids={"upstream": ["up"]},
        elementId_to_dataset_urn={"up": up_urn, "down": down_urn},
        entity_level_upstream_urns={up_urn},
        upstream_elements=[_upstream_element("up", "Upstream", ["x"])],
    )

    assert [lineage.upstreams[0] for lineage in lineages] == [
        builder.make_schema_field_urn(up_urn, "x")
    ]
    assert source.reporter.data_model_element_fgl_self_named_siblings_skipped == 0


def test_a_ref_naming_a_declared_warehouse_table_beats_a_same_named_sibling() -> None:
    """The general form of the same-named-sibling fix.

    The first version keyed on the element's OWN name, which missed this: an
    element called something else entirely, whose ref names a warehouse table
    it declares, while SIBLINGS carry that table's name. Orphan recovery picked
    a sibling and fabricated the edge one step further out. Caught by extending
    the dev fixture with a deliberately NAMED passthrough.
    """
    source = _source()
    # Empty registry: the ref now reaches the warehouse path, which resolves to
    # nothing without a connection. That is fine for this assertion -- what
    # matters is that NO sibling edge is emitted.
    source.connection_registry = SigmaConnectionRegistry(by_id={})
    named_urn, sib_urn = _urn("named"), _urn("sib")
    named = _element(
        "named",
        # Deliberately NOT the table's name.
        "NAMED_ELEMENT",
        [_column("inode-u1/BRAND", "Brand", "[THE_TABLE/Brand]")],
        source_ids=["inode-u1"],
    )
    sibling = _element("sib", "THE_TABLE", [_column("sib-brand", "Brand", None)])

    lineages = source._build_dm_element_fine_grained_lineages(
        element=named,
        element_dataset_urn=named_urn,
        element_name_to_eids={"the_table": ["sib"]},
        elementId_to_dataset_urn={"named": named_urn, "sib": sib_urn},
        entity_level_upstream_urns={sib_urn},
        data_model=_data_model([named, sibling]),
        warehouse_url_id_map={
            "u1": _WarehouseTableRef(
                connection_id="c1", db="DB", schema="SC", table="THE_TABLE"
            )
        },
        discovered_upstreams=set(),
    )

    assert lineages == [], (
        "the ref names a warehouse table this element declares, so the table is "
        f"the referent -- not a sibling that shares its name. got {lineages}"
    )
    assert source.reporter.data_model_element_fgl_self_named_siblings_skipped == 1


class TestTheDataModelSideChecksItself:
    """fgl_emitted is the headline lineage number and had no identity test.

    It counts FGL OBJECTS while the path counters count resolutions, so those
    two can never be reconciled directly. The identity that IS checkable is per
    COLUMN: every Data Model column either produced lineage or recorded a
    reason it did not. Without it, an unexplained shift in fgl_emitted means
    arithmetic across a 100MB log -- which is exactly what the chart-side check
    was built to stop, and the next run is the one where DM numbers move.
    """

    def _source(self) -> SigmaSource:
        src = _source()
        src.reporter = SigmaSourceReport()
        return src

    def test_a_column_that_produced_lineage_is_filed_as_such(self) -> None:
        src = self._source()
        src._note_dm_column_outcome(
            element=_element("e1", "E", []),
            column=_column("id-c", "c", None),
            produced=True,
            reasons_before=src._dm_column_reason_tally(),
        )
        r = src.reporter
        assert (r.dm_columns_total, r.dm_columns_with_lineage) == (1, 1)
        assert r.dm_columns_without_lineage_unattributed == 0

    def test_a_silent_column_with_a_reason_is_attributed(self) -> None:
        src = self._source()
        before = src._dm_column_reason_tally()
        src.reporter.data_model_element_fgl_no_ref_unresolved += 1
        src._note_dm_column_outcome(
            element=_element("e1", "E", []),
            column=_column("id-c", "c", None),
            produced=False,
            reasons_before=before,
        )
        r = src.reporter
        assert r.dm_columns_without_lineage == 1
        assert r.dm_columns_without_lineage_by_reason == {"no_ref_unresolved": 1}
        assert r.dm_columns_without_lineage_unattributed == 0

    def test_a_silent_column_with_NO_reason_is_flagged_and_sampled(self) -> None:
        """The whole point: a drop path added without a counter shows up here
        instead of being invisible until someone does arithmetic."""
        src = self._source()
        src._note_dm_column_outcome(
            element=_element("e1", "E", []),
            column=_column("id-c", "orphan", None),
            produced=False,
            reasons_before=src._dm_column_reason_tally(),
        )
        r = src.reporter
        assert r.dm_columns_without_lineage_unattributed == 1
        assert "column='orphan'" in list(r.dm_unattributed_column_samples)[0]
