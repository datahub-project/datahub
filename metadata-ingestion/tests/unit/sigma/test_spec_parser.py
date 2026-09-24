from typing import Any, Dict, List

import pytest

from datahub.ingestion.source.sigma.spec_parser import parse_data_model_spec

# A predicate side is a Sigma formula, not an identifier.
_LEFT_EXPR = "[Col A]"
_RIGHT_EXPR = "[Col B]"

# Sigma stores an element side's kind as "table".
_ELEMENT_SIDE_L = {"elementId": "el-left", "groupingId": "g1", "kind": "table"}
_ELEMENT_SIDE_R = {"elementId": "el-right", "groupingId": "g2", "kind": "table"}
_WAREHOUSE_SIDE = {
    "connectionId": "conn-1",
    "kind": "warehouse-table",
    "path": ["Connection Root", "DB", "SCHEMA"],
}


def _element(
    element_id: str, column_ids: List[str], source: Dict[str, Any]
) -> Dict[str, Any]:
    return {
        "id": element_id,
        "kind": "table",
        "order": list(column_ids),
        "columns": [{"id": c, "formula": "[X]"} for c in column_ids],
        "source": source,
    }


def _spec(source: Dict[str, Any], element_id: str = "el-join") -> Dict[str, Any]:
    return {
        "kind": "data-model",
        "pages": [
            {
                "id": "p1",
                "elements": [
                    _element("el-left", ["c-a"], {"kind": "warehouse-table"}),
                    _element("el-right", ["c-b"], {"kind": "warehouse-table"}),
                    _element(element_id, [], source),
                ],
            }
        ],
    }


def _join(
    left: Dict[str, Any], right: Dict[str, Any], columns: List[Dict[str, Any]]
) -> Dict[str, Any]:
    return {
        "kind": "join",
        "primarySource": {"elementId": "el-left", "kind": "table"},
        "joins": [
            {
                "joinType": "left-outer",
                "left": left,
                "right": right,
                "columns": columns,
            }
        ],
    }


def _union(sources: List[str], source_columns: List[str]) -> Dict[str, Any]:
    return {
        "kind": "union",
        "sources": [{"elementId": s, "kind": "table"} for s in sources],
        "matches": [{"outputColumnName": "OUT", "sourceColumns": source_columns}],
    }


def test_predicate_is_read_from_joins() -> None:
    index = parse_data_model_spec(
        _spec(
            _join(
                _ELEMENT_SIDE_L,
                _ELEMENT_SIDE_R,
                [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}],
            )
        )
    )
    assert len(index.pairs) == 1
    predicate = index.pairs[0]
    assert predicate.join_element_id == "el-join"
    assert (predicate.left.element_id, predicate.left.column) == ("el-left", "Col A")
    assert (predicate.right.element_id, predicate.right.column) == (
        "el-right",
        "Col B",
    )
    assert index.unreadable_join_element_ids == []


def test_a_side_wrapped_in_a_function_is_still_a_key_equality() -> None:
    index = parse_data_model_spec(
        _spec(
            _join(
                _ELEMENT_SIDE_L,
                _ELEMENT_SIDE_R,
                [{"left": "[Col A]", "right": "Coalesce([Col B], -2)"}],
            )
        )
    )
    assert (index.pairs[0].left.column, index.pairs[0].right.column) == (
        "Col A",
        "Col B",
    )
    assert index.pairs[0].right.expression == "Coalesce([Col B], -2)"


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
        # Unknown types do not claim the outer tier.
        ("something-new", False),
    ],
)
def test_outer_joins_are_flagged(join_type: str, expected_outer: bool) -> None:
    source = _join(
        _ELEMENT_SIDE_L, _ELEMENT_SIDE_R, [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]
    )
    source["joins"][0]["joinType"] = join_type
    index = parse_data_model_spec(_spec(source))
    assert index.pairs[0].join_type == (join_type or "inner")
    assert index.pairs[0].is_outer is expected_outer


@pytest.mark.parametrize("expr", ["[A] = [B]", "42"])
def test_composite_or_literal_side_is_refused(expr: str) -> None:
    index = parse_data_model_spec(
        _spec(_join(_ELEMENT_SIDE_L, _ELEMENT_SIDE_R, [{"left": expr, "right": "[B]"}]))
    )
    assert index.pairs == []


@pytest.mark.parametrize(
    ("op", "expect_pair"),
    [("=", True), ("", True), ("!=", False), ("<", False), ("within", False)],
)
def test_only_equality_claims_a_key_edge(op: str, expect_pair: bool) -> None:
    predicate = {"left": _LEFT_EXPR, "right": _RIGHT_EXPR}
    if op:
        predicate["op"] = op
    index = parse_data_model_spec(
        _spec(_join(_ELEMENT_SIDE_L, _ELEMENT_SIDE_R, [predicate]))
    )
    assert bool(index.pairs) is expect_pair
    # A declined operator is read correctly, not a parse failure.
    assert index.unreadable_join_element_ids == []


def test_a_warehouse_side_is_read_but_yields_no_pair() -> None:
    """The commonest real shape: one side is a table the spec does not describe."""
    index = parse_data_model_spec(
        _spec(
            _join(
                _WAREHOUSE_SIDE,
                _ELEMENT_SIDE_R,
                [{"left": "[SOME_COL]", "right": _RIGHT_EXPR}],
            )
        )
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == []


def test_multiple_joins_and_multi_column_predicates() -> None:
    source = _join(
        _ELEMENT_SIDE_L,
        _ELEMENT_SIDE_R,
        [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}, {"left": "[A]", "right": "[B]"}],
    )
    source["joins"].append(
        {
            "joinType": "inner",
            "left": _ELEMENT_SIDE_R,
            "right": _ELEMENT_SIDE_L,
            "columns": [{"left": "[C]", "right": "[D]"}],
        }
    )
    index = parse_data_model_spec(_spec(source))
    assert [(p.left.column, p.right.column) for p in index.pairs] == [
        ("Col A", "Col B"),
        ("A", "B"),
        ("C", "D"),
    ]


def test_a_cross_model_side_keeps_its_data_model_id() -> None:
    foreign = {"dataModelId": "dm-2", "elementId": "el-far", "kind": "table"}
    index = parse_data_model_spec(
        _spec(
            _join(
                _ELEMENT_SIDE_L, foreign, [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]
            )
        )
    )
    assert index.pairs[0].right.data_model_id == "dm-2"
    assert index.pairs[0].left.data_model_id is None


def test_empty_joins_list_is_not_unreadable() -> None:
    index = parse_data_model_spec(
        _spec(
            {
                "kind": "join",
                "joins": [],
                "primarySource": {"elementId": "el-left", "kind": "table"},
            }
        )
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == []


def test_unrecognised_join_shape_is_reported_as_unreadable() -> None:
    index = parse_data_model_spec(
        _spec({"kind": "join", "on": [{"lhs": "x", "rhs": "y"}]})
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-join"]


def test_renamed_side_descriptors_are_unreadable_not_warehouse_sides() -> None:
    """A missing elementId must not be read as a warehouse table, or a renamed
    descriptor would hide behind the commonest real shape."""
    index = parse_data_model_spec(
        _spec(
            {
                "kind": "join",
                "joins": [
                    {
                        "lhs": _ELEMENT_SIDE_L,
                        "rhs": _ELEMENT_SIDE_R,
                        "columns": [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}],
                    }
                ],
            }
        )
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-join"]


def test_a_side_that_lost_its_element_id_is_unreadable_not_a_warehouse() -> None:
    """A dict side with neither an elementId nor a warehouse key is a changed
    shape, not a warehouse table."""
    renamed = {"element": "el-right", "kind": "table"}
    index = parse_data_model_spec(
        _spec(
            _join(
                _ELEMENT_SIDE_L, renamed, [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}]
            )
        )
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-join"]


@pytest.mark.parametrize(
    "spec", [None, {}, {"pages": None}, {"pages": [{"elements": ["junk"]}]}]
)
def test_missing_or_malformed_spec_is_inert(spec: Any) -> None:
    index = parse_data_model_spec(spec)
    assert index.pairs == []
    assert index.unions == []


def test_union_pairs_each_branch_positionally_by_formula() -> None:
    """Branch columns are formulas too, so the brackets are stripped."""
    index = parse_data_model_spec(
        _spec(_union(["el-a", "el-b"], ["[c-a]", "[c-b]"]), element_id="el-union")
    )
    assert len(index.unions) == 1
    union = index.unions[0]
    assert union.union_element_id == "el-union"
    assert union.output_column == "OUT"
    assert union.branches == (("el-a", "c-a"), ("el-b", "c-b"))
    assert index.union_branch_index_out_of_range == 0


def test_a_union_column_past_the_sources_is_counted_not_reassigned() -> None:
    """Pairing the extra column with another branch would assert false lineage."""
    index = parse_data_model_spec(
        _spec(_union(["el-a"], ["[c-a]", "[c-b]"]), element_id="el-union")
    )
    assert index.unions[0].branches == (("el-a", "c-a"),)
    assert index.union_branch_index_out_of_range == 1


def test_a_union_branch_contributing_nothing_is_skipped() -> None:
    index = parse_data_model_spec(
        _spec(_union(["el-a", "el-b"], ["[c-a]", ""]), element_id="el-union")
    )
    assert index.unions[0].branches == (("el-a", "c-a"),)
    assert index.union_branch_index_out_of_range == 0


def test_an_unreadable_union_is_not_filed_as_a_broken_join() -> None:
    index = parse_data_model_spec(
        _spec({"kind": "union", "branches": [], "columnMap": {}}, element_id="el-union")
    )
    assert index.unions == []
    assert index.unreadable_join_element_ids == []


def test_a_declared_relationship_is_not_lineage() -> None:
    """A relationship is a declared, unused join; only the join is read."""
    spec = _spec(
        _join(
            _ELEMENT_SIDE_L,
            _ELEMENT_SIDE_R,
            [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}],
        )
    )
    spec["pages"][0]["elements"][0]["relationships"] = [
        {
            "id": "rel1",
            "targetElementId": "el-right",
            "keys": [{"sourceColumnId": "c-a", "targetColumnId": "c-b"}],
        }
    ]
    index = parse_data_model_spec(spec)

    assert len(index.pairs) == 1
    assert index.pairs[0].join_element_id == "el-join"
