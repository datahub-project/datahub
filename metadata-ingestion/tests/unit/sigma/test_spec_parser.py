"""Unit tests for the Data Model /spec join-key parser.

The join descriptor's exact key names are documented but were not present on
any Data Model reachable from the integration credentials, so the parser
anchors on column ids it can prove belong to the same document rather than on
key names. These tests pin that behaviour: a renamed key still resolves, and a
descriptor that yields anything other than two known column ids per predicate
resolves to nothing rather than to a guess.

The element/column skeleton below matches a real /spec response's shape:
    {"kind": "data-model", "pages": [{"elements": [
        {"id", "kind", "order", "columns": [{"id", "formula"}], "source": {...}}]}]}
"""

from typing import Any, Dict, List

from datahub.ingestion.source.sigma.spec_parser import (
    SpecColumnRef,
    parse_data_model_spec,
)

_LEFT_COL = "col-left-0000000000000000000000000"
_RIGHT_COL = "col-right-000000000000000000000000"
_OTHER_COL = "col-other-000000000000000000000000"


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


def _spec(elements: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"kind": "data-model", "pages": [{"id": "p1", "elements": elements}]}


def _join_spec(join_source: Dict[str, Any]) -> Dict[str, Any]:
    return _spec(
        [
            _element("el-left", [_LEFT_COL], {"kind": "warehouse-table"}),
            _element("el-right", [_RIGHT_COL, _OTHER_COL], {"kind": "warehouse-table"}),
            _element("el-join", [], join_source),
        ]
    )


def test_documented_shape_yields_a_symmetric_pair() -> None:
    index = parse_data_model_spec(
        _join_spec(
            {
                "kind": "join",
                "columns": [{"left": _LEFT_COL, "right": _RIGHT_COL}],
            }
        ),
        data_model_id="dm-1",
    )
    left = SpecColumnRef("el-left", _LEFT_COL)
    right = SpecColumnRef("el-right", _RIGHT_COL)
    # A predicate is an equality, so it must be readable from either side.
    assert index.partners_of("el-left", _LEFT_COL) == {right}
    assert index.partners_of("el-right", _RIGHT_COL) == {left}
    assert index.unreadable_join_element_ids == []


def test_renamed_predicate_keys_still_resolve() -> None:
    """Key names are unverified against a live tenant; column ids are not."""
    index = parse_data_model_spec(
        _join_spec(
            {
                "kind": "join",
                "on": [
                    {"lhs": {"columnId": _LEFT_COL}, "rhs": {"columnId": _RIGHT_COL}}
                ],
            }
        ),
        data_model_id="dm-1",
    )
    assert index.partners_of("el-left", _LEFT_COL) == {
        SpecColumnRef("el-right", _RIGHT_COL)
    }


def test_predicate_with_three_columns_is_refused() -> None:
    """Only a two-column equality is a key pair; anything else is not guessed."""
    index = parse_data_model_spec(
        _join_spec(
            {
                "kind": "join",
                "columns": [{"a": _LEFT_COL, "b": _RIGHT_COL, "c": _OTHER_COL}],
            }
        ),
        data_model_id="dm-1",
    )
    assert index.partners == {}
    assert index.unreadable_join_element_ids == ["el-join"]


def test_join_naming_no_known_column_is_refused() -> None:
    index = parse_data_model_spec(
        _join_spec(
            {"kind": "join", "columns": [{"left": "not-a-column-of-this-spec"}]}
        ),
        data_model_id="dm-1",
    )
    assert index.partners == {}
    assert index.unreadable_join_element_ids == ["el-join"]


def test_non_join_sources_produce_no_pairs_but_are_counted() -> None:
    index = parse_data_model_spec(
        _spec(
            [
                _element("el-a", [_LEFT_COL], {"kind": "warehouse-table"}),
                _element("el-b", [_RIGHT_COL], {"kind": "table"}),
            ]
        ),
        data_model_id="dm-1",
    )
    assert index.partners == {}
    assert index.source_kind_counts == {"warehouse-table": 1, "table": 1}


def test_missing_or_malformed_spec_is_inert() -> None:
    for spec in (None, {}, {"pages": None}, {"pages": [{"elements": ["junk"]}]}):
        index = parse_data_model_spec(spec, data_model_id="dm-1")
        assert index.partners == {}
        assert index.element_id_by_column_id == {}
