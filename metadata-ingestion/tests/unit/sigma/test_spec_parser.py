from typing import Any, Dict, List, Tuple

import pytest

from datahub.ingestion.source.sigma.spec_parser import (
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
        # A parameter beside a column is a constant...
        ("[A] + [P_offset]", "A"),
        # ...but a lone P_ ref would be joining on a constant, so it is a column.
        ("[P_KEY]", "P_KEY"),
        ("[A] = [B]", None),
        ("42", None),
        ("[A] = [Other/B]", None),
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


def test_a_warehouse_side_is_read_but_yields_no_pair() -> None:
    index = _parse_join(
        _one_join([{"left": "[SOME_COL]", "right": _RIGHT_EXPR}], left=_WAREHOUSE_SIDE)
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == []


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


@pytest.mark.parametrize("element_id", ["", 5, True], ids=["empty", "int", "bool"])
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
