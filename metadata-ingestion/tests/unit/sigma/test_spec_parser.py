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


def _spec(source: Dict[str, Any], element_id: str = "el-x") -> Dict[str, Any]:
    return {
        "kind": "data-model",
        "pages": [{"id": "p1", "elements": [{"id": element_id, "source": source}]}],
    }


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
    index = parse_data_model_spec(_spec(source, element_id="el-join"))

    assert len(index.pairs) == 1
    pair = index.pairs[0]
    assert (pair.left.element_id, pair.left.column) == ("el-flights", "Origin")
    assert (pair.right.element_id, pair.right.column) == ("el-airports", "Code")
    assert not pair.is_outer
    assert index.unreadable_join_element_ids == []


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
    index = parse_data_model_spec(_spec(source, element_id="el-union"))

    assert [c.output_column for c in index.unions] == ["State", "Year"]
    assert _branches(index.unions[0]) == [(None, "State"), ("el-b", "State")]
    assert index.unreadable_union_element_ids == []


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
        # Parameters count as constants.
        ("[A] + [P_offset]", "A"),
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
        # Absent means inner.
        ("", False),
    ],
)
def test_outer_joins_are_flagged(join_type: str, expected_outer: bool) -> None:
    index = _parse_join(
        _one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}], join_type=join_type)
    )
    assert index.pairs[0].join_type == (join_type or "inner")
    assert index.pairs[0].is_outer is expected_outer


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
        ("", True, True),
        ("!=", False, True),
        ("<", False, True),
        ("within", False, True),
        # Unknown, e.g. a renamed "=": reported, not silently declined.
        ("eq", False, False),
    ],
)
def test_only_equality_claims_a_key_edge(
    op: str, expect_pair: bool, expect_readable: bool
) -> None:
    predicate = {"left": _LEFT_EXPR, "right": _RIGHT_EXPR}
    if op:
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
    ],
    ids=["no-joins", "renamed-sides", "lost-element-id", "renamed-columns"],
)
def test_an_unrecognised_join_shape_is_unreadable(source: Dict[str, Any]) -> None:
    index = parse_data_model_spec(_spec(source))
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-x"]


def test_a_good_join_does_not_hide_a_broken_sibling() -> None:
    renamed = {
        "joinType": "inner",
        "lhs": _ELEMENT_SIDE_L,
        "rhs": _ELEMENT_SIDE_R,
        "columns": [{"left": "[C]", "right": "[D]"}],
    }
    index = _parse_join(
        _one_join([{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]), renamed
    )
    # The good join is still read, and the element is still reported.
    assert len(index.pairs) == 1
    assert index.unreadable_join_element_ids == ["el-x"]


def test_a_good_predicate_does_not_hide_a_malformed_sibling() -> None:
    index = _parse_join(
        _one_join(
            [
                {"left": _LEFT_EXPR, "right": _RIGHT_EXPR},
                {"lhs": "[C]", "rhs": "[D]"},
            ]
        )
    )
    assert len(index.pairs) == 1
    assert index.unreadable_join_element_ids == ["el-x"]


@pytest.mark.parametrize(
    "spec", [None, {}, {"pages": None}, {"pages": [{"elements": ["junk"]}]}]
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


@pytest.mark.parametrize("empty", ["", None])
def test_a_branch_contributing_nothing_is_skipped(empty: Any) -> None:
    index = _parse_union(_union([_el("el-a"), _el("el-b")], ["[c-a]", empty]))
    assert _branches(index.unions[0]) == [("el-a", "c-a")]
    assert index.unreadable_union_element_ids == []


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
    ],
    ids=["no-sources", "renamed-matches", "unknown-branch", "non-string-slot"],
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
