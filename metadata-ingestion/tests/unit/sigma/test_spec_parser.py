from typing import Any, Dict, List, Tuple

import pytest

from datahub.ingestion.source.sigma.spec_parser import (
    SUPPORTED_SCHEMA_VERSION,
    DataModelSpecIndex,
    UnionOutputColumn,
    parse_data_model_spec,
)

# A predicate side is a Sigma formula, not an identifier.
_LEFT_EXPR = "[Col A]"
_RIGHT_EXPR = "[Col B]"

# Sigma stores an element side's kind as "table".
_ELEMENT_SIDE_L = {"elementId": "el-left", "groupingId": "g1", "kind": "table"}
_ELEMENT_SIDE_R = {"elementId": "el-right", "groupingId": "g2", "kind": "table"}
_WAREHOUSE_SIDE = {
    "connectionId": "conn-1",
    "kind": "warehouse-table",
    "path": ["DB", "SCHEMA", "TABLE"],
}


def _plain_table(element_id: str) -> Dict[str, Any]:
    """A warehouse-table element: the commonest element, and never unreadable."""
    return {"id": element_id, "kind": "table", "source": dict(_WAREHOUSE_SIDE)}


def _spec(source: Dict[str, Any], element_id: str = "el-x") -> Dict[str, Any]:
    elements = [
        _plain_table("el-left"),
        _plain_table("el-right"),
        {"id": element_id, "source": source},
    ]
    return {"kind": "data-model", "pages": [{"id": "p1", "elements": elements}]}


def _one_join(
    columns: List[Dict[str, Any]],
    *,
    left: Dict[str, Any] = _ELEMENT_SIDE_L,
    right: Dict[str, Any] = _ELEMENT_SIDE_R,
    join_type: str = "inner",
) -> Dict[str, Any]:
    return {
        "joinType": join_type,
        "left": left,
        "right": right,
        "columns": columns,
    }


def _join_source(*joins: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "kind": "join",
        "primarySource": {"elementId": "el-left", "kind": "table"},
        "joins": list(joins),
    }


def _parse_join(*joins: Dict[str, Any]) -> DataModelSpecIndex:
    return parse_data_model_spec(_spec(_join_source(*joins)))


def _branches(column: UnionOutputColumn) -> List[Tuple[Any, str]]:
    return [(b.element_id, b.column) for b in column.branches]


# --- The shapes Sigma publishes, structure verbatim, ids generic -------------


def test_the_published_join_example_yields_its_one_pair() -> None:
    source = {
        "kind": "join",
        "joins": [
            {
                "left": {"elementId": "el-flights", "kind": "table"},
                "right": {"elementId": "el-airports", "kind": "table"},
                "columns": [{"left": "[Origin]", "right": "[Code]"}],
                "joinType": "inner",
            }
        ],
        "primarySource": {"elementId": "el-flights", "kind": "table"},
    }
    spec = {
        "pages": [
            {
                "elements": [
                    _plain_table("el-flights"),
                    _plain_table("el-airports"),
                    {"id": "el-join", "kind": "table", "source": source},
                ]
            }
        ]
    }
    index = parse_data_model_spec(spec)

    assert len(index.pairs) == 1
    pair = index.pairs[0]
    assert (pair.left.element_id, pair.left.column) == ("el-flights", "Origin")
    assert (pair.right.element_id, pair.right.column) == ("el-airports", "Code")
    assert not pair.is_outer
    assert index.unreadable_join_element_ids == []
    assert index.unreadable_union_element_ids == []
    assert index.unrecognised_element_count == 0


def _stored_table(
    element_id: str, name: str, columns: List[Tuple[str, str]]
) -> Dict[str, Any]:
    return {
        "id": element_id,
        "kind": "table",
        "name": name,
        "source": dict(_WAREHOUSE_SIDE),
        "columns": [
            {"id": cid, "formula": "[TABLE/Key]", "name": cname}
            for cid, cname in columns
        ],
        "order": [cid for cid, _ in columns],
    }


def test_a_chained_join_attributes_each_key_to_its_own_entry() -> None:
    """A spec Sigma stored for C chained onto B and D onto A, structure
    verbatim, table and column names generic. Each entry's `left` names the
    element that owns its left column: Sigma's write API rejects a spec where
    it does not ("Column reference not found")."""
    el = lambda i: {"elementId": i, "kind": "table"}  # noqa: E731
    join = {
        "kind": "join",
        "joins": [
            {
                "left": el("pA"),
                "right": el("pB"),
                "columns": [{"left": "[A Key]", "right": "[B Key]"}],
                "joinType": "inner",
            },
            {
                "left": el("pB"),
                "right": el("pC"),
                "columns": [
                    {
                        "left": "[B Link Key]",
                        "right": "[C Key]",
                        "op": "is-not-distinct-from",
                    }
                ],
                "joinType": "left-outer",
            },
            {
                "left": el("pA"),
                "right": el("pD"),
                "columns": [{"left": "[A Key]", "right": "[D Key]"}],
                "joinType": "inner",
            },
        ],
        "primarySource": el("pA"),
    }
    spec = {
        "schemaVersion": 1,
        "pages": [
            {
                "elements": [
                    _stored_table("pA", "A", [("a_key", "A Key")]),
                    _stored_table(
                        "pB", "B", [("b_key", "B Key"), ("b_ckey", "B Link Key")]
                    ),
                    _stored_table("pC", "C", [("c_key", "C Key")]),
                    _stored_table("pD", "D", [("d_key", "D Key")]),
                    {"id": "pJoin", "kind": "table", "name": "J", "source": join},
                ]
            }
        ],
    }
    index = parse_data_model_spec(spec)

    assert [
        (
            p.left.element_id,
            p.left.column,
            p.right.element_id,
            p.right.column,
            p.is_outer,
        )
        for p in index.pairs
    ] == [
        ("pA", "A Key", "pB", "B Key", False),
        ("pB", "B Link Key", "pC", "C Key", True),
        ("pA", "A Key", "pD", "D Key", False),
    ]
    assert index.unreadable_join_element_ids == []
    assert index.unrecognised_element_count == 0
    assert index.is_supported_schema


def test_the_published_union_example_keeps_its_warehouse_branch() -> None:
    """The first branch is a warehouse table, not an element."""
    source = {
        "kind": "union",
        "sources": [
            {
                "connectionId": "conn-1",
                "kind": "warehouse-table",
                "path": ["DB", "SCHEMA", "TABLE"],
            },
            {"elementId": "el-b", "kind": "table"},
        ],
        "matches": [
            {"outputColumnName": "State", "sourceColumns": ["[State]", "[State]"]},
            {"outputColumnName": "Year", "sourceColumns": ["[Year]", "[Year]"]},
        ],
    }
    spec = {
        "pages": [
            {
                "elements": [
                    _plain_table("el-b"),
                    {"id": "el-union", "kind": "table", "source": source},
                ]
            }
        ]
    }
    index = parse_data_model_spec(spec)

    assert [c.output_column for c in index.unions] == ["State", "Year"]
    assert _branches(index.unions[0]) == [(None, "State"), ("el-b", "State")]
    warehouse = index.unions[0].branches[0]
    assert (warehouse.connection_id, warehouse.path) == (
        "conn-1",
        ("DB", "SCHEMA", "TABLE"),
    )
    assert [b.source_index for b in index.unions[0].branches] == [0, 1]
    assert index.unreadable_union_element_ids == []
    assert index.unreadable_join_element_ids == []
    assert index.unrecognised_element_count == 0


# --- Join predicates ----------------------------------------------------------


def test_a_side_wrapped_in_a_function_is_still_a_key_equality() -> None:
    index = _parse_join(
        _one_join([{"left": "[Col A]", "right": "Coalesce([Col B], -2)"}])
    )
    assert (index.pairs[0].left.column, index.pairs[0].right.column) == (
        "Col A",
        "Col B",
    )
    assert index.pairs[0].right.expression == "Coalesce([Col B], -2)"


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ("If(IsNull([Key]), -1, [Key])", "Key"),
        # Key participation, not value equality.
        ("[A] + 1", "A"),
        # With no control of that name, a `P_` ref is a column like any other...
        ("[P_KEY]", "P_KEY"),
        # ...so beside another column it is two columns, not a key.
        ("[A] + [P_offset]", None),
        ("[A] = [B]", None),
        ("42", None),
        ("[A] = [Other/B]", None),
        # Compared exactly: Sigma's case rule is unverified, and two claims less.
        ("If(IsNull([Key]), -1, [KEY])", None),
        ("Coalesce([A], [Rel/B])", None),
        # A lone multi-segment ref names a relationship or element, not a column.
        ("[Rel/B]", None),
        ("[A] + [A/B]", None),
    ],
)
def test_a_side_is_a_key_only_if_it_names_one_column(expr: str, expected: Any) -> None:
    index = _parse_join(_one_join([{"left": expr, "right": "[B]"}]))
    assert [p.left.column for p in index.pairs] == ([expected] if expected else [])
    # A well-formed predicate that is not a key is not a parse failure.
    assert index.unreadable_join_element_ids == []


@pytest.mark.parametrize(
    ("join_type", "expected_outer"),
    [
        ("inner", False),
        ("left-outer", True),
        ("right-outer", True),
        ("full-outer", True),
        ("lookup", True),
        (" Left-Outer ", True),
    ],
)
def test_outer_joins_are_flagged(join_type: str, expected_outer: bool) -> None:
    index = _parse_join(
        _one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}], join_type=join_type)
    )
    assert index.pairs[0].join_type == join_type.strip().lower()
    assert index.pairs[0].is_outer is expected_outer


@pytest.mark.parametrize("join_type", [None, "", 3], ids=["absent", "empty", "int"])
def test_a_missing_join_type_is_reported_not_read_as_inner(join_type: Any) -> None:
    """A stored spec always carries joinType, so its absence is a renamed key,
    and reading it as inner would put an outer join in the top tier."""
    join = _one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])
    if join_type is None:
        del join["joinType"]
    else:
        join["joinType"] = join_type
    index = _parse_join(join)
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-x"]


def test_an_unknown_join_type_is_reported_not_scored() -> None:
    """An anti join's equality holds on no output rows, so a type this parser
    does not know must not land in any confidence tier."""
    index = _parse_join(
        _one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}], join_type="anti")
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-x"]


@pytest.mark.parametrize(
    ("op", "expect_pair", "expect_readable"),
    [
        ("=", True, True),
        # Absent is equality: Sigma's published join example omits op.
        ("<absent>", True, True),
        (None, True, True),
        (" = ", True, True),
        ("!=", False, True),
        ("<", False, True),
        ("WITHIN", False, True),
        # Null-safe equality, as the spec stores the UI's `<=>`.
        ("is-not-distinct-from", True, True),
        ("is-distinct-from", False, True),
        # The UI symbol itself is not a stored spelling.
        ("<=>", False, False),
        # Unknown, e.g. a renamed "=": reported, not silently declined.
        ("eq", False, False),
        ("", False, False),
        (0, False, False),
        (False, False, False),
        (1, False, False),
    ],
)
def test_only_equality_claims_a_key_edge(
    op: Any, expect_pair: bool, expect_readable: bool
) -> None:
    predicate: Dict[str, Any] = {"left": _LEFT_EXPR, "right": _RIGHT_EXPR}
    if op != "<absent>":
        predicate["op"] = op
    index = _parse_join(_one_join([predicate]))
    assert bool(index.pairs) is expect_pair
    assert (index.unreadable_join_element_ids == []) is expect_readable


@pytest.mark.parametrize(
    "side",
    [
        {"connectionId": "c1", "kind": "warehouse-table"},
        {"path": ["D", "S", "T"], "kind": "warehouse-table"},
        {"connectionId": "c1", "path": "D.S.T"},
        {"connectionId": "c1", "path": ["D", 1, "T"]},
        {"connectionId": "c1", "path": ["D", "", "T"]},
        {"connectionId": 42, "path": ["D", "S", "T"]},
        {"connectionId": "c1", "path": []},
    ],
    ids=[
        "connection-only",
        "path-only",
        "string-path",
        "non-string-segment",
        "empty-segment",
        "non-string-connection",
        "empty-path",
    ],
)
def test_an_incomplete_warehouse_side_is_drift(side: Dict[str, Any]) -> None:
    """The consumer needs both connection and path to name the table."""
    join_index = _parse_join(
        _one_join([{"left": "[X]", "right": _RIGHT_EXPR}], left=side)
    )
    assert join_index.unreadable_join_element_ids == ["el-x"]
    union_index = _parse_union(_union([side, _el("el-b")], ["[X]", "[Y]"]))
    assert _branches(union_index.unions[0]) == [("el-b", "Y")]
    assert union_index.unreadable_union_element_ids == ["el-union"]


@pytest.mark.parametrize(
    ("side", "recorded"),
    [("[T/Rel/K]", True), ("[Other/K]", True), ("[A] = [B]", False), ("42", False)],
)
def test_a_multi_segment_join_side_is_recorded(side: str, recorded: bool) -> None:
    """Valid Sigma, and possibly a key this parser cannot map -- unlike a
    literal or composite side, which is simply not a key."""
    index = _parse_join(_one_join([{"left": side, "right": "[B]"}]))
    assert index.pairs == []
    assert index.unreadable_join_element_ids == []
    assert index.multi_segment_ref_element_ids == (["el-x"] if recorded else [])


@pytest.mark.parametrize("first", [True, False], ids=["first-join", "last-join"])
def test_a_multi_segment_side_is_recorded_beside_a_clean_join(first: bool) -> None:
    through = _one_join([{"left": "[T/Rel/K]", "right": "[B]"}])
    clean = _one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])
    index = _parse_join(*([through, clean] if first else [clean, through]))
    assert len(index.pairs) == 1
    assert index.multi_segment_ref_element_ids == ["el-x"]


def test_a_warehouse_side_is_a_pair_the_consumer_can_map() -> None:
    """The consumer decides whether to emit a key edge to a warehouse table,
    so the pair must reach it, with the table's identity."""
    index = _parse_join(
        _one_join([{"left": "[SOME_COL]", "right": _RIGHT_EXPR}], left=_WAREHOUSE_SIDE)
    )
    pair = index.pairs[0]
    assert (pair.left.element_id, pair.left.connection_id, pair.left.path) == (
        None,
        "conn-1",
        ("DB", "SCHEMA", "TABLE"),
    )
    assert (pair.right.element_id, pair.right.column) == ("el-right", "Col B")
    assert index.unreadable_join_element_ids == []


def test_two_warehouse_tables_on_one_column_are_not_a_self_join() -> None:
    other = {**_WAREHOUSE_SIDE, "path": ["DB", "SCHEMA", "OTHER"]}
    index = _parse_join(
        _one_join([{"left": "[K]", "right": "[K]"}], left=_WAREHOUSE_SIDE, right=other)
    )
    assert len(index.pairs) == 1


def test_the_models_own_data_model_id_names_a_local_element() -> None:
    """Otherwise a self-join would look cross-model and be emitted."""
    own = {**_ELEMENT_SIDE_L, "dataModelId": "dm-self"}
    spec = _spec(_join_source(_one_join([{"left": "[K]", "right": "[K]"}], right=own)))
    spec["dataModelId"] = "dm-self"
    index = parse_data_model_spec(spec)
    assert index.pairs == []
    assert index.unreadable_join_element_ids == []


@pytest.mark.parametrize("side", ["", "   "], ids=["empty", "blank"])
def test_an_empty_join_side_is_drift_not_a_literal(side: str) -> None:
    """A side is a required formula."""
    index = _parse_join(_one_join([{"left": side, "right": "[B]"}]))
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-x"]


def test_a_cross_join_is_read_but_yields_no_pair() -> None:
    """Sigma builds a cross join with a `True = True` key: no column, no drift."""
    spec = _spec(_join_source(_one_join([{"left": "True", "right": "True"}])))
    spec["schemaVersion"] = 1
    index = parse_data_model_spec(spec)
    assert index.pairs == []
    assert not index.drift_detected


def test_multiple_joins_and_multi_column_predicates() -> None:
    index = _parse_join(
        _one_join(
            [
                {"left": _LEFT_EXPR, "right": _RIGHT_EXPR},
                {"left": "[A]", "right": "[B]"},
            ]
        ),
        _one_join(
            [{"left": "[C]", "right": "[D]"}],
            left=_ELEMENT_SIDE_R,
            right=_ELEMENT_SIDE_L,
        ),
    )
    assert [(p.left.column, p.right.column) for p in index.pairs] == [
        ("Col A", "Col B"),
        ("A", "B"),
        ("C", "D"),
    ]


def test_a_self_join_on_one_column_is_not_an_edge() -> None:
    same = _one_join(
        [{"left": "[K]", "right": "[K]"}, {"left": "[K]", "right": "[J]"}],
        left=_ELEMENT_SIDE_L,
        right=_ELEMENT_SIDE_L,
    )
    index = _parse_join(same)
    assert [(p.left.column, p.right.column) for p in index.pairs] == [("K", "J")]
    assert index.unreadable_join_element_ids == []


def test_the_same_element_id_in_another_model_is_not_a_self_join() -> None:
    """Element ids are not unique across Data Models."""
    foreign_same_id = {**_ELEMENT_SIDE_L, "dataModelId": "dm-2"}
    index = _parse_join(
        _one_join([{"left": "[K]", "right": "[K]"}], right=foreign_same_id)
    )
    assert len(index.pairs) == 1


def test_a_cross_model_side_keeps_its_data_model_id() -> None:
    foreign = {"dataModelId": "dm-2", "elementId": "el-far", "kind": "table"}
    index = _parse_join(
        _one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}], right=foreign)
    )
    assert index.pairs[0].right.data_model_id == "dm-2"
    assert index.pairs[0].left.data_model_id is None


def test_an_empty_joins_list_is_not_unreadable() -> None:
    index = parse_data_model_spec(_spec({"kind": "join", "joins": []}))
    assert index.pairs == []
    assert index.unreadable_join_element_ids == []


@pytest.mark.parametrize(
    "element_id", ["", "  ", 5, True], ids=["empty", "blank", "int", "bool"]
)
def test_an_element_with_no_string_id_is_counted(element_id: Any) -> None:
    """It cannot be named in a report, but it must not vanish either."""
    source = _join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]))
    index = parse_data_model_spec(_spec(source, element_id=element_id))
    assert index.pairs == []
    assert index.unrecognised_element_count == 1


def test_a_renamed_id_key_is_counted_on_every_element() -> None:
    spec = _spec(_join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])))
    for element in spec["pages"][0]["elements"]:
        element["ident"] = element.pop("id")
    index = parse_data_model_spec(spec)
    assert index.pairs == []
    assert index.unrecognised_element_count == 3


@pytest.mark.parametrize(
    "kind", ["", "  ", None, 3], ids=["empty", "blank", "null", "int"]
)
def test_a_source_with_no_usable_kind_is_counted(kind: Any) -> None:
    source = _join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]))
    source["kind"] = kind
    index = parse_data_model_spec(_spec(source))
    assert index.pairs == []
    assert index.unrecognised_element_count == 1


@pytest.mark.parametrize("kind", ["join-v2", "pivot"])
def test_an_unknown_source_kind_is_counted_not_assumed_single_source(
    kind: str,
) -> None:
    """A renamed "join" would otherwise stop every join with nothing reported.
    Named by kind, so the report says which elements and which kind."""
    source = _join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]))
    source["kind"] = kind
    index = parse_data_model_spec(_spec(source))
    assert index.pairs == []
    assert index.unrecognised_kind_element_ids == {kind: ["el-x"]}
    assert index.drift_detected


@pytest.mark.parametrize(
    "kind", ["warehouse-table", "table", "sql", "csv-table", "data-model"]
)
def test_a_published_single_source_kind_is_quiet(kind: str) -> None:
    index = parse_data_model_spec(_spec({"kind": kind}))
    assert index.unrecognised_element_count == 0


def test_the_published_transpose_is_unmapped_not_drift() -> None:
    """Valid Sigma, but its upstream columns appear only in `columnsToMerge`,
    which /columns formulas do not reach. Structure verbatim, ids generic."""
    transpose = {
        "kind": "transpose",
        "source": dict(_WAREHOUSE_SIDE),
        "direction": "column-to-row",
        "columnsToMerge": ["Start", "End"],
        "columnLabelForMergedColumns": "Event type",
        "columnLabelForValues": "Event time",
    }
    index = parse_data_model_spec(_spec(transpose, element_id="el-t"))
    assert index.unmapped_element_ids == {"transpose": ["el-t"]}
    assert index.unrecognised_element_count == 0


@pytest.mark.parametrize("kind", ["control", " Control "])
def test_a_control_is_skipped_case_insensitively(kind: str) -> None:
    spec = _spec({"kind": "warehouse-table", **_WAREHOUSE_SIDE})
    spec["pages"][0]["elements"].append(
        {"id": "ctrl-1", "kind": kind, "source": {"kind": "source"}}
    )
    assert parse_data_model_spec(spec).unrecognised_element_count == 0


def test_a_control_is_not_a_data_element() -> None:
    """A control's source binds its value to a column; it carries no lineage.
    Structure verbatim from Sigma's published list-values control."""
    spec = _spec({"kind": "warehouse-table", **_WAREHOUSE_SIDE})
    spec["pages"][0]["elements"].append(
        {
            "id": "ctrl-1",
            "kind": "control",
            "source": {
                "kind": "source",
                "source": {"kind": "table", "elementId": "el-left"},
                "columnId": "inode-1/COL_A",
            },
        }
    )
    index = parse_data_model_spec(spec)
    assert index.unrecognised_element_count == 0
    assert (index.element_count, index.sourced_element_count) == (4, 3)


def test_a_source_that_is_not_an_object_is_counted() -> None:
    index = parse_data_model_spec(_spec("warehouse-table"))  # type: ignore[arg-type]
    assert index.unrecognised_element_count == 1


def test_a_renamed_source_key_leaves_no_sourced_elements() -> None:
    """Otherwise it would look exactly like a model of plain tables."""
    legit = _spec(_join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])))
    renamed = _spec(
        _join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]))
    )
    for element in renamed["pages"][0]["elements"]:
        element["src"] = element.pop("source")
    good, bad = parse_data_model_spec(legit), parse_data_model_spec(renamed)
    assert (good.element_count, good.sourced_element_count) == (3, 3)
    assert (bad.element_count, bad.sourced_element_count) == (3, 0)


@pytest.mark.parametrize(
    ("version", "expected"),
    [(1, 1), (2, 2), ("1", None), (True, None), ("<absent>", None)],
)
def test_the_schema_version_is_read(version: Any, expected: Any) -> None:
    spec = _spec(_join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])))
    if version != "<absent>":
        spec["schemaVersion"] = version
    index = parse_data_model_spec(spec)
    assert index.schema_version == expected
    assert index.is_supported_schema is (expected == SUPPORTED_SCHEMA_VERSION)


@pytest.mark.parametrize(
    "data_model_id",
    [None, 123, {"id": "dm-2"}, "", "  "],
    # No published Sigma document contains a null; absent keys are omitted.
    ids=["null", "int", "dict", "empty", "blank"],
)
def test_an_unusable_data_model_id_is_drift_not_a_local_element(
    data_model_id: Any,
) -> None:
    """Element ids repeat across models, so falling back to this one would
    attach the key to whatever local element shares the id."""
    side = {**_ELEMENT_SIDE_R, "dataModelId": data_model_id}
    join_index = _parse_join(
        _one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}], right=side)
    )
    assert join_index.pairs == []
    assert join_index.unreadable_join_element_ids == ["el-x"]
    union_index = _parse_union(_union([side, _el("el-b")], ["[X]", "[Y]"]))
    assert union_index.unreadable_union_element_ids == ["el-union"]


def test_a_renamed_kind_key_is_counted() -> None:
    source = _join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]))
    source["type"] = source.pop("kind")
    index = parse_data_model_spec(_spec(source))
    assert index.pairs == []
    assert index.unrecognised_element_count == 1


def test_an_element_with_no_source_is_not_counted() -> None:
    """A control or text element has no lineage to read."""
    spec = _spec(_join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])))
    spec["pages"][0]["elements"].append({"id": "ctrl-1", "kind": "control"})
    assert parse_data_model_spec(spec).unrecognised_element_count == 0


def test_the_kind_is_read_case_insensitively() -> None:
    source = _join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]))
    source["kind"] = " Join "
    assert len(parse_data_model_spec(_spec(source)).pairs) == 1


# --- Drift: reported per join, even when part of the element was read ---------


@pytest.mark.parametrize(
    "source",
    [
        {"kind": "join", "on": [{"lhs": "x", "rhs": "y"}]},
        # The join's side descriptors renamed.
        {
            "kind": "join",
            "joins": [
                {
                    "lhs": _ELEMENT_SIDE_L,
                    "rhs": _ELEMENT_SIDE_R,
                    "columns": [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}],
                }
            ],
        },
        # A side that lost its elementId is a changed shape, not a warehouse.
        _join_source(
            _one_join(
                [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}],
                right={"element": "el-right", "kind": "table"},
            )
        ),
        # The predicate list renamed.
        _join_source(
            {
                "joinType": "inner",
                "left": _ELEMENT_SIDE_L,
                "right": _ELEMENT_SIDE_R,
                "on": [],
            }
        ),
        # A join with no predicates: nothing states which columns match.
        _join_source(_one_join([])),
        # A `joins` entry that is not an object.
        _join_source("junk"),  # type: ignore[arg-type]
    ],
    ids=[
        "no-joins",
        "renamed-sides",
        "lost-element-id",
        "renamed-columns",
        "no-predicates",
        "non-dict-join",
    ],
)
def test_an_unrecognised_join_shape_is_unreadable(source: Dict[str, Any]) -> None:
    index = parse_data_model_spec(_spec(source))
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-x"]


_GOOD_JOIN = _one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])
_RENAMED_JOIN = {
    "joinType": "inner",
    "lhs": _ELEMENT_SIDE_L,
    "rhs": _ELEMENT_SIDE_R,
    "columns": [{"left": "[C]", "right": "[D]"}],
}
_GOOD_PREDICATE = {"left": _LEFT_EXPR, "right": _RIGHT_EXPR}
_MALFORMED_PREDICATE = {"lhs": "[C]", "rhs": "[D]"}


@pytest.mark.parametrize("good_first", [True, False], ids=["good-first", "bad-first"])
def test_a_broken_join_is_flagged_beside_a_good_one(good_first: bool) -> None:
    joins = [_GOOD_JOIN, _RENAMED_JOIN]
    index = _parse_join(*(joins if good_first else joins[::-1]))
    # The good join is still read, and the element is still reported.
    assert len(index.pairs) == 1
    assert index.unreadable_join_element_ids == ["el-x"]


@pytest.mark.parametrize("good_first", [True, False], ids=["good-first", "bad-first"])
def test_a_malformed_predicate_is_flagged_beside_a_good_one(good_first: bool) -> None:
    entries = [_GOOD_PREDICATE, _MALFORMED_PREDICATE]
    index = _parse_join(_one_join(entries if good_first else entries[::-1]))
    assert len(index.pairs) == 1
    assert index.unreadable_join_element_ids == ["el-x"]


@pytest.mark.parametrize(
    "spec",
    [
        None,
        {},
        {"pages": None},
        {"pages": 5},
        {"pages": ["junk"]},
        {"pages": [{"elements": 7}]},
        {"pages": [{"elements": ["junk"]}]},
    ],
)
def test_missing_or_malformed_spec_is_inert(spec: Any) -> None:
    index = parse_data_model_spec(spec)
    assert index.pairs == []
    assert index.unions == []
    assert index.element_count == 0


def test_every_element_is_counted() -> None:
    """A renamed `pages` or `source` key shows up as no elements, or none with a
    source, instead of as a clean empty index."""
    spec = _spec(_join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])))
    spec["pages"][0]["elements"].append({"id": "ctrl-1", "kind": "control"})
    assert parse_data_model_spec(spec).element_count == 4


# --- Unions -------------------------------------------------------------------


def _union(sources: List[Dict[str, Any]], source_columns: List[Any]) -> Dict[str, Any]:
    return {
        "kind": "union",
        "sources": sources,
        "matches": [{"outputColumnName": "OUT", "sourceColumns": source_columns}],
    }


def _parse_union(source: Dict[str, Any]) -> DataModelSpecIndex:
    return parse_data_model_spec(_spec(source, element_id="el-union"))


def _el(element_id: str) -> Dict[str, Any]:
    return {"elementId": element_id, "kind": "table"}


def test_union_branches_are_formulas_paired_by_position() -> None:
    index = _parse_union(_union([_el("el-a"), _el("el-b")], ["[c-a]", "[c-b]"]))
    assert len(index.unions) == 1
    assert index.unions[0].union_element_id == "el-union"
    assert index.unions[0].output_column == "OUT"
    assert _branches(index.unions[0]) == [("el-a", "c-a"), ("el-b", "c-b")]
    assert index.unreadable_union_element_ids == []


def test_a_cross_model_union_branch_keeps_its_data_model_id() -> None:
    foreign = {"dataModelId": "dm-2", "elementId": "el-far", "kind": "table"}
    index = _parse_union(_union([_el("el-a"), foreign], ["[c-a]", "[c-far]"]))
    assert [b.data_model_id for b in index.unions[0].branches] == [None, "dm-2"]


def test_a_branch_naming_several_columns_contributes_each() -> None:
    """A branch is a data flow: every column it names feeds the output."""
    index = _parse_union(
        _union([_el("el-a"), _el("el-b")], ['Concat([First], " ", [Last])', "[C]"])
    )
    branches = index.unions[0].branches
    assert [(b.element_id, b.column, b.source_index) for b in branches] == [
        ("el-a", "First", 0),
        ("el-a", "Last", 0),
        ("el-b", "C", 1),
    ]
    assert index.unreadable_union_element_ids == []


def test_a_cross_element_ref_in_a_branch_is_recorded_too() -> None:
    """`[Element/Col]` is the common two-part form, not a relationship."""
    index = _parse_union(_union([_el("el-a"), _el("el-b")], ["[Left Elem/A]", "[B]"]))
    assert _branches(index.unions[0]) == [("el-b", "B")]
    assert index.multi_segment_ref_element_ids == ["el-union"]


def test_a_relationship_ref_in_a_branch_is_counted_not_drift() -> None:
    """Valid Sigma, but mapping it needs the relationship's target."""
    index = _parse_union(_union([_el("el-a"), _el("el-b")], ["[A] + [Rel/Col]", "[C]"]))
    assert _branches(index.unions[0]) == [("el-a", "A"), ("el-b", "C")]
    assert index.multi_segment_ref_element_ids == ["el-union"]
    assert index.unreadable_union_element_ids == []


def test_a_constant_branch_is_not_drift() -> None:
    """A branch can contribute a literal; it names no column to map."""
    index = _parse_union(_union([_el("el-a"), _el("el-b")], ['"n/a"', "[C]"]))
    assert _branches(index.unions[0]) == [("el-b", "C")]
    assert index.unreadable_union_element_ids == []


def _with_control(spec: Dict[str, Any], control_id: str) -> Dict[str, Any]:
    spec["pages"][0]["elements"].append(
        {"id": f"ctrl-{control_id}", "kind": "control", "controlId": control_id}
    )
    return spec


@pytest.mark.parametrize("declared", [True, False], ids=["control", "no-control"])
def test_a_parameter_is_a_declared_control_in_a_union(declared: bool) -> None:
    """Sigma names a parameter by its control's `controlId`, not by a prefix."""
    spec = _spec(_union([_el("el-a"), _el("el-b")], ["[Region]", "[r]"]), "el-union")
    index = parse_data_model_spec(_with_control(spec, "Region") if declared else spec)
    expected = [("el-b", "r")] if declared else [("el-a", "Region"), ("el-b", "r")]
    assert _branches(index.unions[0]) == expected
    assert index.unreadable_union_element_ids == []


@pytest.mark.parametrize("declared", [True, False], ids=["control", "no-control"])
def test_a_parameter_is_a_declared_control_on_a_join_side(declared: bool) -> None:
    """The same rule as a union: a declared control is a constant beside a key."""
    spec = _spec(_join_source(_one_join([{"left": "[A] + [Offset]", "right": "[B]"}])))
    index = parse_data_model_spec(_with_control(spec, "Offset") if declared else spec)
    assert [p.left.column for p in index.pairs] == (["A"] if declared else [])


def test_two_warehouse_branches_stay_distinct() -> None:
    first = {"connectionId": "c-1", "kind": "warehouse-table", "path": ["D", "S", "T1"]}
    second = {
        "connectionId": "c-1",
        "kind": "warehouse-table",
        "path": ["D", "S", "T2"],
    }
    index = _parse_union(_union([first, second], ["[Amount]", "[Amount]"]))
    branches = index.unions[0].branches
    assert len(set(branches)) == 2
    assert [b.path[-1] for b in branches] == ["T1", "T2"]


def test_a_branch_keeps_its_position_past_an_empty_slot() -> None:
    index = _parse_union(
        _union([_el("el-a"), _el("el-b"), _el("el-c")], ["[x]", "", "[z]"])
    )
    assert [b.source_index for b in index.unions[0].branches] == [0, 2]


@pytest.mark.parametrize("empty", ["", None])
def test_a_branch_contributing_nothing_is_skipped(empty: Any) -> None:
    index = _parse_union(_union([_el("el-a"), _el("el-b")], ["[c-a]", empty]))
    assert _branches(index.unions[0]) == [("el-a", "c-a")]
    assert index.unreadable_union_element_ids == []


def test_an_unknown_branch_is_flagged_and_the_good_branch_still_read() -> None:
    unknown = {"element": "el-a", "kind": "table"}
    index = _parse_union(_union([unknown, _el("el-b")], ["[c-a]", "[c-b]"]))
    assert _branches(index.unions[0]) == [("el-b", "c-b")]
    assert index.unreadable_union_element_ids == ["el-union"]


def test_a_union_with_sources_but_no_output_columns_is_reported() -> None:
    """Like a join with no predicates, it says nothing about what it produces."""
    source = {"kind": "union", "sources": [_el("el-a")], "matches": []}
    assert _parse_union(source).unreadable_union_element_ids == ["el-union"]
    empty = {"kind": "union", "sources": [], "matches": []}
    assert _parse_union(empty).unreadable_union_element_ids == []


def test_a_column_past_the_sources_is_reported_not_reassigned() -> None:
    """Pairing the extra column with another branch would assert false lineage."""
    index = _parse_union(_union([_el("el-a")], ["[c-a]", "[c-b]"]))
    assert _branches(index.unions[0]) == [("el-a", "c-a")]
    assert index.unreadable_union_element_ids == ["el-union"]


@pytest.mark.parametrize(
    "source",
    [
        {"kind": "union", "branches": [], "columnMap": {}},
        # `matches` renamed.
        {"kind": "union", "sources": [_el("el-a")], "columnMatches": []},
        # A branch descriptor this parser does not recognise.
        _union([{"element": "el-a", "kind": "table"}], ["[c-a]"]),
        # A slot that is not a formula.
        _union([_el("el-a")], [{"column": "c-a"}]),
        _union([_el("el-a")], [0]),
        _union([_el("el-a")], [False]),
        _union([_el("el-a")], [[]]),
        {"kind": "union", "sources": [_el("el-a")], "matches": ["junk"]},
        {
            "kind": "union",
            "sources": [_el("el-a")],
            "matches": [{"outputColumnName": "", "sourceColumns": ["[c-a]"]}],
        },
        {
            "kind": "union",
            "sources": [_el("el-a")],
            "matches": [{"outputColumnName": 7, "sourceColumns": ["[c-a]"]}],
        },
        {
            "kind": "union",
            "sources": [_el("el-a")],
            "matches": [{"outputColumnName": "OUT", "sourceColumns": "[c-a]"}],
        },
    ],
    ids=[
        "no-sources",
        "renamed-matches",
        "unknown-branch",
        "non-string-slot",
        "zero-slot",
        "false-slot",
        "list-slot",
        "non-dict-match",
        "empty-output-name",
        "non-string-output-name",
        "non-list-source-columns",
    ],
)
def test_an_unrecognised_union_shape_is_reported(source: Dict[str, Any]) -> None:
    index = _parse_union(source)
    assert index.unions == []
    assert index.unreadable_union_element_ids == ["el-union"]
    assert index.unreadable_join_element_ids == []


def test_a_declared_relationship_is_not_lineage() -> None:
    """A relationship is a declared join; only the join's predicate is read."""
    spec = _spec(_join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])))
    spec["pages"][0]["elements"][0]["relationships"] = [
        {
            "id": "rel-1",
            "targetElementId": "el-right",
            "keys": [{"sourceColumnId": "c-a", "targetColumnId": "c-b"}],
            "name": "Relationship",
        }
    ]
    index = parse_data_model_spec(spec)

    assert len(index.pairs) == 1


def test_an_empty_model_is_not_drift() -> None:
    """A real, freshly created Data Model: one page, no elements."""
    spec = {
        "schemaVersion": 1,
        "pages": [{"id": "p1", "name": "Page 1", "elements": []}],
    }
    index = parse_data_model_spec(spec)
    assert (index.element_count, index.structure_readable) == (0, True)
    assert not index.drift_detected


def test_a_text_element_without_a_source_is_not_drift() -> None:
    spec = _spec(_join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])))
    spec["schemaVersion"] = 1
    spec["pages"][0]["elements"].append({"id": "txt-1", "kind": "text"})
    assert not parse_data_model_spec(spec).drift_detected


def test_a_clean_read_is_not_drift() -> None:
    spec = _spec(_join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])))
    spec["schemaVersion"] = 1
    assert not parse_data_model_spec(spec).drift_detected


@pytest.mark.parametrize(
    "break_it",
    [
        lambda s: s["pages"][0]["elements"][2]["source"].update(kind="join-v2"),
        lambda s: s["pages"][0]["elements"][2]["source"].update(joins="x"),
        lambda s: s["pages"][0]["elements"][2].update(id=""),
        lambda s: s.update(schemaVersion=2),
        lambda s: s.update(sheets=s.pop("pages")),
        lambda s: s["pages"][0].update(items=s["pages"][0].pop("elements")),
        lambda s: [e.update(src=e.pop("source")) for e in s["pages"][0]["elements"]],
    ],
    ids=[
        "unknown-kind",
        "unreadable-join",
        "no-id",
        "schema",
        "renamed-pages",
        "renamed-elements",
        "renamed-source",
    ],
)
def test_every_drift_signal_sets_drift_detected(break_it: Any) -> None:
    spec = _spec(_join_source(_one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}])))
    spec["schemaVersion"] = 1
    break_it(spec)
    assert parse_data_model_spec(spec).drift_detected


def test_valid_but_unmapped_sigma_is_not_drift() -> None:
    """A transpose and a relationship ref are recorded, not treated as drift."""
    spec = _spec(
        _union([_el("el-a"), _el("el-b")], ["[A] + [Rel/Col]", "[C]"]), "el-union"
    )
    spec["schemaVersion"] = 1
    spec["pages"][0]["elements"].append(
        {"id": "el-t", "source": {"kind": "transpose", "source": dict(_WAREHOUSE_SIDE)}}
    )
    index = parse_data_model_spec(spec)
    assert index.multi_segment_ref_element_ids == ["el-union"]
    assert index.unmapped_element_ids == {"transpose": ["el-t"]}
    assert not index.drift_detected
