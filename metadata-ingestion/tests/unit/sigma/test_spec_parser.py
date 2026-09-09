"""Unit tests for the Data Model /spec join-key parser.

The fixtures below mirror the shape a live tenant's /spec actually returns,
recovered from the key skeletons this parser logs when it cannot read a
descriptor::

    source = {"kind": "join",
              "primarySource": {...},
              "joins": [{"joinType": ..., "left": {...}, "right": {...},
                         "columns": [{"left": ..., "right": ..., "op": ...}]}]}

An earlier version looked for the predicate at ``source.columns[]`` and matched
sides by scanning for column ids belonging to the same document. It read zero
predicates across a full customer run, for two reasons pinned here: the
predicate lives under ``joins``, and a join side is frequently a warehouse
table whose columns the document never describes.
"""

from typing import Any, Dict, List

from datahub.ingestion.source.sigma.spec_parser import parse_data_model_spec

# A predicate side is a Sigma formula, not an identifier.
_LEFT_EXPR = "[Col A]"
_RIGHT_EXPR = "[Col B]"

_ELEMENT_SIDE_L = {"elementId": "el-left", "groupingId": "g1", "kind": "element"}
_ELEMENT_SIDE_R = {"elementId": "el-right", "groupingId": "g2", "kind": "element"}
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


def _spec(join_source: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "kind": "data-model",
        "pages": [
            {
                "id": "p1",
                "elements": [
                    _element("el-left", ["c-a"], {"kind": "warehouse-table"}),
                    _element("el-right", ["c-b"], {"kind": "warehouse-table"}),
                    _element("el-join", [], join_source),
                ],
            }
        ],
    }


def _join(
    left: Dict[str, Any], right: Dict[str, Any], columns: List[Dict[str, Any]]
) -> Dict[str, Any]:
    return {
        "kind": "join",
        "primarySource": {"kind": "warehouse-table", "connectionId": "c"},
        "joins": [
            {"joinType": "left", "left": left, "right": right, "columns": columns}
        ],
    }


def test_predicate_is_read_from_joins_not_from_source_columns() -> None:
    index = parse_data_model_spec(
        _spec(
            _join(
                _ELEMENT_SIDE_L,
                _ELEMENT_SIDE_R,
                [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR, "op": "equals"}],
            )
        ),
        data_model_id="dm-1",
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


def test_side_expression_yields_the_column_it_references() -> None:
    """Sides are formulas: a wrapped column is still a key equality.

    A live tenant spells them "[Col A]" and "Coalesce([Col A], -2)".
    Treating the raw string as an identifier read 61 predicates and resolved 0.
    """
    index = parse_data_model_spec(
        _spec(
            _join(
                _ELEMENT_SIDE_L,
                _ELEMENT_SIDE_R,
                [{"left": "[Col A]", "right": "Coalesce([Col B], -2)"}],
            )
        ),
        data_model_id="dm-1",
    )
    assert (index.pairs[0].left.column, index.pairs[0].right.column) == (
        "Col A",
        "Col B",
    )
    # The raw text is kept so a log line can show what was parsed.
    assert index.pairs[0].right.expression == "Coalesce([Col B], -2)"


def test_join_type_is_captured_and_outer_joins_are_flagged() -> None:
    """An outer join's ON equality holds only on the rows it matched.

    The consumer scores those edges lower, so the parser has to carry the type
    rather than discard it.
    """
    for join_type, expected_outer in (
        ("inner", False),
        ("left", True),
        ("full-outer", True),
        ("", False),
    ):
        source = _join(
            _ELEMENT_SIDE_L,
            _ELEMENT_SIDE_R,
            [{"left": _LEFT_EXPR, "right": _RIGHT_EXPR}],
        )
        source["joins"][0]["joinType"] = join_type
        index = parse_data_model_spec(_spec(source), data_model_id="dm-1")
        assert index.pairs[0].join_type == join_type
        assert index.pairs[0].is_outer is expected_outer


def test_composite_or_literal_side_is_refused() -> None:
    """Two columns or none is not a simple key equality."""
    for expr in ("[A] = [B]", "42"):
        index = parse_data_model_spec(
            _spec(
                _join(
                    _ELEMENT_SIDE_L, _ELEMENT_SIDE_R, [{"left": expr, "right": "[B]"}]
                )
            ),
            data_model_id="dm-1",
        )
        assert index.pairs == []


def test_warehouse_side_is_counted_not_treated_as_a_parse_failure() -> None:
    """The commonest real shape: one side is a table, with no columns in the doc."""
    index = parse_data_model_spec(
        _spec(
            _join(
                _WAREHOUSE_SIDE,
                _ELEMENT_SIDE_R,
                [{"left": "[SOME_COL]", "right": _RIGHT_EXPR}],
            )
        ),
        data_model_id="dm-1",
    )
    assert index.pairs == []
    assert index.warehouse_side_predicates == 1
    # Crucially NOT reported as unreadable -- that counter must stay meaningful
    # as "the shape assumption is wrong".
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
    index = parse_data_model_spec(_spec(source), data_model_id="dm-1")
    assert [(p.left.column, p.right.column) for p in index.pairs] == [
        ("Col A", "Col B"),
        ("A", "B"),
        ("C", "D"),
    ]


def test_empty_joins_list_is_not_unreadable() -> None:
    index = parse_data_model_spec(
        _spec({"kind": "join", "joins": [], "primarySource": {"kind": "table"}}),
        data_model_id="dm-1",
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == []


def test_unrecognised_join_shape_is_reported_as_unreadable() -> None:
    index = parse_data_model_spec(
        _spec({"kind": "join", "on": [{"lhs": "x", "rhs": "y"}]}),
        data_model_id="dm-1",
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-join"]


def test_source_kinds_are_counted() -> None:
    index = parse_data_model_spec(
        _spec(_join(_ELEMENT_SIDE_L, _ELEMENT_SIDE_R, [])),
        data_model_id="dm-1",
    )
    assert index.source_kind_counts == {"warehouse-table": 2, "join": 1}


def test_missing_or_malformed_spec_is_inert() -> None:
    for spec in (None, {}, {"pages": None}, {"pages": [{"elements": ["junk"]}]}):
        index = parse_data_model_spec(spec, data_model_id="dm-1")
        assert index.pairs == []


def test_renamed_side_descriptors_are_unreadable_not_warehouse_side() -> None:
    """The signal must not classify a shape change as an expected outcome.

    If Sigma renames ``joins[].left``/``.right`` while keeping
    ``columns[].left``/``.right``, both sides lose their element id. Treating
    "no element id" as proof of a warehouse table would file the mismatch under
    the commonest real shape and leave unreadable_join_element_ids at zero --
    silencing the one counter that exists to catch exactly that change.
    """
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
        ),
        data_model_id="dm-1",
    )
    assert index.pairs == []
    assert index.unreadable_join_element_ids == ["el-join"]
    # Must NOT be mistaken for the genuine warehouse-side case.
    assert index.warehouse_side_predicates == 0


# The 'union' descriptor, recovered from a live tenant's key skeletons (2026-09):
#
#     source = {"kind": "union",
#               "sources": [{"elementId": ..., "groupingId": ..., "kind": ...}],
#               "matches": [{"outputColumnName": ...,
#                            "sourceColumns": [...]}]}
#
# ``sourceColumns`` is positional against ``sources``.
def _union_spec(source: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "kind": "data-model",
        "pages": [
            {
                "id": "p1",
                "elements": [
                    _element("el-a", ["c-a"], {"kind": "warehouse-table"}),
                    _element("el-b", ["c-b"], {"kind": "warehouse-table"}),
                    _element("el-union", [], source),
                ],
            }
        ],
    }


def test_union_branch_columns_are_formulas_not_bare_names() -> None:
    """The shape that made this read 1,077 output columns and emit 0 edges.

    ``sourceColumns`` entries are Sigma formulas -- a live tenant sent ``[Id]``
    and ``[ID]`` -- exactly like join predicate sides. Passing them through
    verbatim carried the brackets into every column lookup, so nothing matched.
    """
    index = parse_data_model_spec(
        _union_spec(
            {
                "kind": "union",
                "sources": [
                    {"elementId": "el-a", "kind": "table"},
                    {"elementId": "el-b", "kind": "table"},
                ],
                "matches": [
                    {"outputColumnName": "OUT", "sourceColumns": ["[c-a]", "[c-b]"]}
                ],
            }
        ),
        data_model_id="dm-1",
    )
    assert index.unions[0].branches == (("el-a", "c-a"), ("el-b", "c-b"))


def test_union_output_column_pairs_each_branch_positionally() -> None:
    index = parse_data_model_spec(
        _union_spec(
            {
                "kind": "union",
                "sources": [
                    {"elementId": "el-a", "groupingId": "g1", "kind": "table"},
                    {"elementId": "el-b", "groupingId": "g2", "kind": "table"},
                ],
                "matches": [
                    {"outputColumnName": "OUT", "sourceColumns": ["[c-a]", "[c-b]"]}
                ],
            }
        ),
        data_model_id="dm-1",
    )
    assert len(index.unions) == 1
    union = index.unions[0]
    assert union.union_element_id == "el-union"
    assert union.output_column == "OUT"
    assert union.branches == (("el-a", "c-a"), ("el-b", "c-b"))
    assert index.union_branch_index_out_of_range == 0


def test_union_branch_beyond_the_sources_list_is_counted_not_reassigned() -> None:
    """A misalignment must surface as a number, never as a wrong pairing.

    Positional alignment is an inference: the skeleton log could only show that
    ``sourceColumns`` and ``sources`` have equal length. If that ever stops
    holding, pairing the extra column with some other branch would assert
    lineage that does not exist, so the entry is dropped and counted.
    """
    index = parse_data_model_spec(
        _union_spec(
            {
                "kind": "union",
                "sources": [{"elementId": "el-a", "kind": "table"}],
                "matches": [
                    {"outputColumnName": "OUT", "sourceColumns": ["[c-a]", "[c-b]"]}
                ],
            }
        ),
        data_model_id="dm-1",
    )
    assert index.unions[0].branches == (("el-a", "c-a"),)
    assert index.union_branch_index_out_of_range == 1


def test_union_branch_contributing_no_column_is_skipped_silently() -> None:
    """An empty slot is a normal union, not a defect."""
    index = parse_data_model_spec(
        _union_spec(
            {
                "kind": "union",
                "sources": [
                    {"elementId": "el-a", "kind": "table"},
                    {"elementId": "el-b", "kind": "table"},
                ],
                "matches": [
                    {"outputColumnName": "OUT", "sourceColumns": ["[c-a]", ""]}
                ],
            }
        ),
        data_model_id="dm-1",
    )
    assert index.unions[0].branches == (("el-a", "c-a"),)
    assert index.union_branch_index_out_of_range == 0


def test_unreadable_union_yields_no_pairs_and_no_join_failure() -> None:
    """A union this parser cannot read must not be filed as a broken join."""
    index = parse_data_model_spec(
        _union_spec({"kind": "union", "branches": [], "columnMap": {}}),
        data_model_id="dm-1",
    )
    assert index.unions == []
    assert index.unreadable_join_element_ids == []
