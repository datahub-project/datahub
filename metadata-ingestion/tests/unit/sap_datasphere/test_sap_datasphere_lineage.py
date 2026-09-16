from typing import List

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.sap_datasphere.constants import (
    MALFORMED_COL_SENTINEL,
    REMOTE_CONNECTION_KEY,
    REMOTE_ENTITY_DELIMITER,
    REMOTE_ENTITY_KEY,
)
from datahub.ingestion.source.sap_datasphere.lineage import (
    CsnLineageExtractor,
    parse_remote_table_source,
)
from datahub.ingestion.source.sap_datasphere.models import (
    ColumnLineageContext,
    ColumnLineagePair,
    UpstreamColRef,
    UpstreamRef,
)


def test_column_lineage_pair_sentinel_with_no_upstream_refs_is_valid():
    """A sentinel downstream_col with empty upstream_refs satisfies the invariant."""
    pair = ColumnLineagePair(downstream_col=MALFORMED_COL_SENTINEL)
    assert pair.downstream_col == MALFORMED_COL_SENTINEL
    assert pair.upstream_refs == []


def test_lineage_from_simple_select_extracts_one_upstream():
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["SAP.TIME.M_TIME_DIMENSION"], "as": "T"},
                "columns": [{"ref": ["DATE_SQL"]}, {"ref": ["YEAR_VAL"]}],
            }
        },
    }
    extractor = CsnLineageExtractor()
    upstreams = extractor.extract_upstream_refs(csn_def)
    assert upstreams == [UpstreamRef(name="SAP.TIME.M_TIME_DIMENSION", qualified=True)]


def test_lineage_returns_empty_when_no_query():
    csn_def = {"kind": "entity", "elements": {"COL1": {"type": "cds.String"}}}
    extractor = CsnLineageExtractor()
    assert extractor.extract_upstream_refs(csn_def) == []


def test_lineage_returns_empty_when_no_from_ref():
    csn_def = {"kind": "entity", "query": {"SELECT": {"columns": [{"val": 1}]}}}
    extractor = CsnLineageExtractor()
    assert extractor.extract_upstream_refs(csn_def) == []


def test_lineage_handles_join_with_two_refs():
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {
                    "join": "inner",
                    "args": [
                        {"ref": ["SAP.TIME.M_TIME_DIMENSION"], "as": "T"},
                        {"ref": ["SAP.TIME.M_TIME_DIMENSION_TMONTH"], "as": "M"},
                    ],
                    "on": [{"ref": ["T", "MONTH"]}, "=", {"ref": ["M", "MONTH"]}],
                },
                "columns": [{"ref": ["T", "DATE_SQL"]}, {"ref": ["M", "MONTH_NAME"]}],
            }
        },
    }
    extractor = CsnLineageExtractor()
    upstreams = extractor.extract_upstream_refs(csn_def)
    assert set(upstreams) == {
        UpstreamRef(name="SAP.TIME.M_TIME_DIMENSION", qualified=True),
        UpstreamRef(name="SAP.TIME.M_TIME_DIMENSION_TMONTH", qualified=True),
    }


def test_cross_space_from_ref_is_qualified_not_space_prefixed():
    """Regression: a dotted FROM ref (``SPACE.OBJECT``) is already space-qualified; double-prefixing a cross-space ref into a phantom URN dropped the real lineage edge."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {
                    "join": "inner",
                    "args": [
                        {"ref": ["SAP_BW.V_VCOPSER03_N045"], "as": "B"},
                        {"ref": ["LOCAL_DIM"], "as": "L"},
                    ],
                    "on": [{"ref": ["B", "K"]}, "=", {"ref": ["L", "K"]}],
                },
                "columns": [{"ref": ["B", "C"]}, {"ref": ["L", "D"]}],
            }
        },
    }
    refs = CsnLineageExtractor().extract_upstream_refs(csn_def)
    assert set(refs) == {
        UpstreamRef(name="SAP_BW.V_VCOPSER03_N045", qualified=True),
        UpstreamRef(name="LOCAL_DIM", qualified=False),
    }


def test_lineage_reads_remote_source_annotation():
    csn_def = {
        "kind": "entity",
        "@remote.source": "SNOWFLAKE_PROD",
        "elements": {"ID": {"type": "cds.String"}},
    }
    extractor = CsnLineageExtractor()
    assert extractor.remote_source(csn_def) == "SNOWFLAKE_PROD"


def test_lineage_returns_none_for_remote_source_when_absent():
    csn_def = {"kind": "entity", "elements": {}}
    extractor = CsnLineageExtractor()
    assert extractor.remote_source(csn_def) is None


def test_walk_from_handles_inline_subquery():
    """An inline subquery should be unwrapped one level so the inner table reference becomes the upstream."""
    extractor = CsnLineageExtractor()
    csn_def = {
        "kind": "entity",
        "query": {"SELECT": {"from": {"SELECT": {"from": {"ref": ["INNER_TABLE"]}}}}},
    }
    refs = extractor.extract_upstream_refs(csn_def)
    assert refs == [UpstreamRef(name="INNER_TABLE", qualified=False)]


def test_walk_from_handles_doubly_nested_subquery():
    """Arbitrary nesting depth — a three-level inline subquery still resolves."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"SELECT": {"from": {"SELECT": {"from": {"ref": ["DEEP"]}}}}}
            }
        },
    }
    refs = CsnLineageExtractor().extract_upstream_refs(csn_def)
    assert refs == [UpstreamRef(name="DEEP", qualified=False)]


def test_lineage_skips_non_string_ref_element():
    """A dict ref[0] (parametrized reference) must be skipped rather than yielding a non-string upstream."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": [{"id": "PARAM", "args": []}]},
                "columns": [{"ref": ["X"]}],
            }
        },
    }
    extractor = CsnLineageExtractor()
    upstreams = extractor.extract_upstream_refs(csn_def)
    assert upstreams == [], (
        f"Expected non-string ref[0] to be skipped; got: {upstreams}"
    )


def test_column_lineage_pair_carries_downstream_and_refs():
    pair = ColumnLineagePair(
        downstream_col="total",
        upstream_refs=[UpstreamColRef(qname="BASE_TABLE", col="AMOUNT")],
        transform_op="AGGREGATE",
    )
    assert pair.downstream_col == "total"
    assert pair.upstream_refs == [UpstreamColRef(qname="BASE_TABLE", col="AMOUNT")]
    assert pair.transform_op == "AGGREGATE"


def test_alias_map_single_source_uses_empty_string_key():
    """A single-source FROM with no `as` alias maps the empty-string key so a one-segment ref resolves unambiguously."""
    extractor = CsnLineageExtractor()
    alias_map = extractor._build_alias_map({"ref": ["BASE_TABLE"]})
    assert alias_map == {"": "BASE_TABLE", "BASE_TABLE": "BASE_TABLE"}


def test_alias_map_with_explicit_alias():
    """With explicit `as`, both the alias AND `""` map to the source, since real SAP CSN uses `FROM T AS T` with unqualified column refs."""
    extractor = CsnLineageExtractor()
    alias_map = extractor._build_alias_map({"ref": ["BASE_TABLE"], "as": "b"})
    assert alias_map == {"b": "BASE_TABLE", "": "BASE_TABLE"}


def test_alias_map_single_source_with_self_alias_still_resolves_unqualified():
    """Real live-tenant case: `FROM SAP.TIME.M_TIME_DIMENSION AS M_TIME_DIMENSION` with unqualified column refs must still resolve."""
    extractor = CsnLineageExtractor()
    alias_map = extractor._build_alias_map(
        {"ref": ["SAP.TIME.M_TIME_DIMENSION"], "as": "M_TIME_DIMENSION"}
    )
    assert alias_map[""] == "SAP.TIME.M_TIME_DIMENSION"
    assert alias_map["M_TIME_DIMENSION"] == "SAP.TIME.M_TIME_DIMENSION"


def test_alias_map_join_with_two_aliased_sources():
    extractor = CsnLineageExtractor()
    from_node = {
        "join": "inner",
        "args": [
            {"ref": ["USERS"], "as": "u"},
            {"ref": ["ORDERS"], "as": "o"},
        ],
    }
    alias_map = extractor._build_alias_map(from_node)
    assert alias_map == {"u": "USERS", "o": "ORDERS"}


def test_alias_map_returns_empty_for_non_dict():
    extractor = CsnLineageExtractor()
    assert extractor._build_alias_map(None) == {}
    assert extractor._build_alias_map([]) == {}


def test_walk_expr_collects_unqualified_ref_via_single_source():
    extractor = CsnLineageExtractor()
    alias_map = {"": "BASE", "BASE": "BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["AMOUNT"]}, alias_map, out, unresolved)
    assert out == [UpstreamColRef(qname="BASE", col="AMOUNT")]


def test_walk_expr_collects_qualified_ref_via_alias():
    extractor = CsnLineageExtractor()
    alias_map = {"u": "USERS"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["u", "ID"]}, alias_map, out, unresolved)
    assert out == [UpstreamColRef(qname="USERS", col="ID")]


def test_walk_expr_unqualified_ref_in_multi_source_skipped():
    """When the FROM is a JOIN (no empty-string key), unqualified refs cannot be safely attributed — collect nothing rather than guess."""
    extractor = CsnLineageExtractor()
    alias_map = {"u": "USERS", "o": "ORDERS"}  # no "" key
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["UNKNOWN_COL"]}, alias_map, out, unresolved)
    assert out == []


def test_walk_expr_collects_func_args_refs():
    extractor = CsnLineageExtractor()
    alias_map = {"": "BASE", "BASE": "BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr(
        {"func": "SUM", "args": [{"ref": ["AMOUNT"]}]},
        alias_map,
        out,
        unresolved,
    )
    assert out == [UpstreamColRef(qname="BASE", col="AMOUNT")]


def test_walk_expr_collects_xpr_multi_ref():
    """`xpr` is the CSN form for arbitrary expressions like `a + b`."""
    extractor = CsnLineageExtractor()
    alias_map = {"": "BASE", "BASE": "BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr(
        {"xpr": [{"ref": ["A"]}, "+", {"ref": ["B"]}]},
        alias_map,
        out,
        unresolved,
    )
    assert out == [
        UpstreamColRef(qname="BASE", col="A"),
        UpstreamColRef(qname="BASE", col="B"),
    ]


def test_walk_expr_ignores_literals():
    extractor = CsnLineageExtractor()
    alias_map = {"": "BASE", "BASE": "BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"val": 42}, alias_map, out, unresolved)
    assert out == []


def test_walk_expr_handles_nested_case():
    """A CASE/cast expression nests further `xpr` calls."""
    extractor = CsnLineageExtractor()
    alias_map = {"": "BASE", "BASE": "BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr(
        {
            "xpr": [
                "case",
                {"xpr": [{"ref": ["X"]}, ">", {"val": 0}]},
                "then",
                {"ref": ["Y"]},
            ]
        },
        alias_map,
        out,
        unresolved,
    )
    assert out == [
        UpstreamColRef(qname="BASE", col="X"),
        UpstreamColRef(qname="BASE", col="Y"),
    ]


def test_walk_expr_scalar_subquery_resolves_against_inner_from():
    """A scalar subquery column must resolve against the SUBQUERY's own FROM, not the outer alias map."""
    extractor = CsnLineageExtractor()
    outer_alias_map = {"": "OUTER_BASE", "OUTER_BASE": "OUTER_BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    scalar_subquery = {
        "SELECT": {
            "from": {"ref": ["SALES"]},
            "columns": [
                {"func": "MAX", "args": [{"ref": ["amount"]}]},
            ],
        }
    }
    extractor._walk_expr(scalar_subquery, outer_alias_map, out, unresolved)
    # Ref resolves to SALES.amount (inner FROM), not OUTER_BASE.amount.
    assert out == [UpstreamColRef(qname="SALES", col="amount")]
    assert unresolved == []


def test_walk_expr_scalar_subquery_with_join_in_inner_from():
    """Subquery whose inner FROM is a JOIN — refs use qualified aliases against the subquery's own alias map."""
    extractor = CsnLineageExtractor()
    outer_alias_map = {"": "OUTER_BASE", "OUTER_BASE": "OUTER_BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    subq = {
        "SELECT": {
            "from": {
                "join": "inner",
                "args": [
                    {"ref": ["USERS"], "as": "u"},
                    {"ref": ["ORDERS"], "as": "o"},
                ],
            },
            "columns": [
                {"ref": ["u", "id"]},
                {"ref": ["o", "amount"]},
            ],
        }
    }
    extractor._walk_expr(subq, outer_alias_map, out, unresolved)
    assert out == [
        UpstreamColRef(qname="USERS", col="id"),
        UpstreamColRef(qname="ORDERS", col="amount"),
    ]


def test_walk_expr_subquery_without_from_records_unresolved_for_correlated_refs():
    """Subqueries without a FROM clause (e.g. correlated subqueries) can't be resolved in v1; their refs land in `unresolved` because the inner alias map is empty."""
    extractor = CsnLineageExtractor()
    outer_alias_map = {"": "OUTER_BASE", "OUTER_BASE": "OUTER_BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    correlated = {
        "SELECT": {
            "columns": [
                {"func": "COUNT", "args": [{"ref": ["x"]}]},
            ],
        }
    }
    extractor._walk_expr(correlated, outer_alias_map, out, unresolved)
    assert out == []
    assert len(unresolved) == 1


def test_walk_expr_subquery_with_no_columns_emits_unresolved():
    """A scalar subquery without an explicit `columns` array (e.g. `SELECT *`) cannot be unrolled — record the limitation in `unresolved`."""
    extractor = CsnLineageExtractor()
    outer_alias_map = {"": "OUTER_BASE", "OUTER_BASE": "OUTER_BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    subq = {"SELECT": {"from": {"ref": ["SOME_TABLE"]}}}
    extractor._walk_expr(subq, outer_alias_map, out, unresolved)
    assert out == []
    assert len(unresolved) == 1
    assert "subquery" in unresolved[0].lower()


def test_extract_column_lineage_aliased_scalar_subquery():
    """End-to-end: a view whose single column is itself a scalar subquery `(SELECT MAX(amount) FROM SALES) AS peak`."""
    extractor = CsnLineageExtractor()
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["DUMMY"]},
                "columns": [
                    {
                        "SELECT": {
                            "from": {"ref": ["SALES"]},
                            "columns": [
                                {"func": "MAX", "args": [{"ref": ["amount"]}]},
                            ],
                        },
                        "as": "peak",
                    },
                ],
            }
        },
    }
    pairs = extractor.extract_column_lineage(csn_def)
    assert len(pairs) == 1
    pair = pairs[0]
    assert pair.downstream_col == "peak"
    assert pair.upstream_refs == [UpstreamColRef(qname="SALES", col="amount")]


def test_resolve_output_name_uses_as_alias_when_present():
    extractor = CsnLineageExtractor()
    name = extractor._resolve_output_name({"ref": ["X", "Y"], "as": "renamed"})
    assert name == "renamed"


def test_resolve_output_name_falls_back_to_last_ref_segment():
    extractor = CsnLineageExtractor()
    assert extractor._resolve_output_name({"ref": ["BASE", "AMOUNT"]}) == "AMOUNT"
    assert extractor._resolve_output_name({"ref": ["COL"]}) == "COL"


def test_resolve_output_name_returns_none_for_unnamed_expressions():
    """A `func` with no `as` cannot produce a downstream column name."""
    extractor = CsnLineageExtractor()
    assert extractor._resolve_output_name({"func": "SUM", "args": []}) is None


def test_infer_transform_identifies_aggregate_funcs():
    extractor = CsnLineageExtractor()
    assert extractor._infer_transform({"func": "SUM", "args": []}) == "AGGREGATE"
    assert extractor._infer_transform({"func": "count", "args": []}) == "AGGREGATE"


def test_infer_transform_identifies_generic_func_as_transformation():
    extractor = CsnLineageExtractor()
    assert extractor._infer_transform({"func": "UPPER", "args": []}) == "TRANSFORMATION"


def test_infer_transform_xpr_is_expression():
    extractor = CsnLineageExtractor()
    assert extractor._infer_transform({"xpr": []}) == "EXPRESSION"


def test_infer_transform_aliased_ref_is_rename():
    extractor = CsnLineageExtractor()
    assert extractor._infer_transform({"ref": ["X"], "as": "y"}) == "RENAME"


def test_infer_transform_direct_ref_is_identity():
    extractor = CsnLineageExtractor()
    assert extractor._infer_transform({"ref": ["X"]}) == "IDENTITY"


def test_extract_column_lineage_simple_select_all_columns_passthrough():
    """SELECT ID, NAME, AMOUNT FROM BASE_TABLE → three IDENTITY pairs."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE_TABLE"]},
                "columns": [
                    {"ref": ["ID"]},
                    {"ref": ["NAME"]},
                    {"ref": ["AMOUNT"]},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert pairs == [
        ColumnLineagePair(
            downstream_col="ID",
            upstream_refs=[UpstreamColRef(qname="BASE_TABLE", col="ID")],
            transform_op="IDENTITY",
        ),
        ColumnLineagePair(
            downstream_col="NAME",
            upstream_refs=[UpstreamColRef(qname="BASE_TABLE", col="NAME")],
            transform_op="IDENTITY",
        ),
        ColumnLineagePair(
            downstream_col="AMOUNT",
            upstream_refs=[UpstreamColRef(qname="BASE_TABLE", col="AMOUNT")],
            transform_op="IDENTITY",
        ),
    ]


def test_extract_column_lineage_aggregate_with_alias():
    """SELECT SUM(AMOUNT) AS total FROM BASE_TABLE → one AGGREGATE pair."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE_TABLE"]},
                "columns": [
                    {"func": "SUM", "args": [{"ref": ["AMOUNT"]}], "as": "total"},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert pairs == [
        ColumnLineagePair(
            downstream_col="total",
            upstream_refs=[UpstreamColRef(qname="BASE_TABLE", col="AMOUNT")],
            transform_op="AGGREGATE",
        ),
    ]


def test_extract_column_lineage_join_with_qualified_refs():
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {
                    "join": "inner",
                    "args": [
                        {"ref": ["USERS"], "as": "u"},
                        {"ref": ["ORDERS"], "as": "o"},
                    ],
                },
                "columns": [
                    {"ref": ["u", "ID"]},
                    {"ref": ["o", "AMOUNT"]},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert pairs == [
        ColumnLineagePair(
            downstream_col="ID",
            upstream_refs=[UpstreamColRef(qname="USERS", col="ID")],
            transform_op="IDENTITY",
        ),
        ColumnLineagePair(
            downstream_col="AMOUNT",
            upstream_refs=[UpstreamColRef(qname="ORDERS", col="AMOUNT")],
            transform_op="IDENTITY",
        ),
    ]


def test_extract_column_lineage_xpr_multi_upstream():
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {"xpr": [{"ref": ["A"]}, "+", {"ref": ["B"]}], "as": "total"},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert pairs == [
        ColumnLineagePair(
            downstream_col="total",
            upstream_refs=[
                UpstreamColRef(qname="BASE", col="A"),
                UpstreamColRef(qname="BASE", col="B"),
            ],
            transform_op="EXPRESSION",
        ),
    ]


def test_extract_column_lineage_literal_column_skipped():
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {"val": 5, "as": "constant"},
                    {"ref": ["REAL_COL"]},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert pairs == [
        ColumnLineagePair(
            downstream_col="REAL_COL",
            upstream_refs=[UpstreamColRef(qname="BASE", col="REAL_COL")],
            transform_op="IDENTITY",
        ),
    ]


def test_extract_column_lineage_returns_empty_for_legitimate_non_select_csn():
    """Legitimate non-SELECT entity shapes (base table, no query, no columns) return [] silently — they are not malformed."""
    extractor = CsnLineageExtractor()
    assert extractor.extract_column_lineage({}) == []
    assert extractor.extract_column_lineage({"kind": "entity"}) == []
    assert extractor.extract_column_lineage({"query": {}}) == []
    assert extractor.extract_column_lineage({"query": {"SELECT": {}}}) == []
    assert (
        extractor.extract_column_lineage(
            {"query": {"SELECT": {"from": {"ref": ["X"]}}}}
        )
        == []
    )


def test_extract_column_lineage_surfaces_malformed_csn_structurally():
    """Structurally-broken CSN (wrong types at expected fields) is surfaced as a synthetic <malformed> pair so the caller can populate the report counter."""
    extractor = CsnLineageExtractor()
    pairs = extractor.extract_column_lineage({"query": "not-a-dict"})
    assert len(pairs) == 1
    assert pairs[0].downstream_col == "<malformed>"
    assert pairs[0].unresolved_refs

    pairs = extractor.extract_column_lineage({"query": {"SELECT": "not-a-dict"}})
    assert len(pairs) == 1
    assert pairs[0].downstream_col == "<malformed>"

    pairs = extractor.extract_column_lineage(
        {"query": {"SELECT": {"from": {"ref": ["X"]}, "columns": "not-a-list"}}}
    )
    assert len(pairs) == 1
    assert pairs[0].downstream_col == "<malformed>"


def test_walk_expr_unqualified_ref_in_multi_source_records_to_unresolved():
    extractor = CsnLineageExtractor()
    alias_map = {"u": "USERS", "o": "ORDERS"}  # multi-source, no "" key
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["MYSTERY_COL"]}, alias_map, out, unresolved)
    assert out == []
    assert len(unresolved) == 1
    assert "MYSTERY_COL" in unresolved[0]


def test_walk_expr_unknown_alias_records_to_unresolved():
    extractor = CsnLineageExtractor()
    alias_map = {"u": "USERS"}  # 'x' not in map
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["x", "ID"]}, alias_map, out, unresolved)
    assert out == []
    assert len(unresolved) == 1
    assert "x" in unresolved[0]


def test_walk_expr_three_segment_ref_records_to_unresolved():
    extractor = CsnLineageExtractor()
    alias_map = {"": "BASE", "BASE": "BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["DB", "SCHEMA", "TABLE"]}, alias_map, out, unresolved)
    assert out == []
    assert len(unresolved) == 1


def test_extract_column_lineage_unnamed_expression_records_to_unresolved():
    """SUM(x) without AS alias is surfaced as <unnamed> rather than silently dropped."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [{"func": "SUM", "args": [{"ref": ["AMOUNT"]}]}],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert len(pairs) == 1
    assert pairs[0].downstream_col == "<unnamed>"
    assert pairs[0].unresolved_refs


def test_extract_column_lineage_records_walker_unresolved_per_pair():
    """A column mixing resolvable and unresolvable refs records both on the same pair."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {
                    "join": "inner",
                    "args": [
                        {"ref": ["USERS"], "as": "u"},
                        {"ref": ["ORDERS"], "as": "o"},
                    ],
                },
                "columns": [
                    {
                        "xpr": [{"ref": ["u", "ID"]}, "+", {"ref": ["x", "MYSTERY"]}],
                        "as": "combo",
                    }
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert len(pairs) == 1
    assert pairs[0].downstream_col == "combo"
    assert pairs[0].upstream_refs == [UpstreamColRef(qname="USERS", col="ID")]
    assert len(pairs[0].unresolved_refs) == 1
    assert "x" in pairs[0].unresolved_refs[0]


def test_extract_column_lineage_deduplicates_repeated_upstreams():
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {"xpr": [{"ref": ["A"]}, "+", {"ref": ["A"]}], "as": "doubled"},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert pairs == [
        ColumnLineagePair(
            downstream_col="doubled",
            upstream_refs=[UpstreamColRef(qname="BASE", col="A")],
            transform_op="EXPRESSION",
        ),
    ]


def test_extract_column_lineage_skips_columns_with_no_resolvable_refs():
    """SELECT * yields a `*` col name; the source layer filters it out, so the extractor just emits whatever CSN gives."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {"ref": ["BASE", "*"]},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert pairs == [
        ColumnLineagePair(
            downstream_col="*",
            upstream_refs=[UpstreamColRef(qname="BASE", col="*")],
            transform_op="IDENTITY",
        ),
    ]


def test_upstream_col_ref_exposes_named_fields():
    """UpstreamColRef is addressed by name (qname/col), not by tuple position."""
    ref = UpstreamColRef(qname="BASE", col="AMOUNT")
    assert ref.qname == "BASE"
    assert ref.col == "AMOUNT"
    assert ref == UpstreamColRef(qname="BASE", col="AMOUNT")


def test_walk_expr_emits_upstream_col_ref_instances():
    extractor = CsnLineageExtractor()
    alias_map = {"u": "USERS"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["u", "ID"]}, alias_map, out, unresolved)
    assert out == [UpstreamColRef(qname="USERS", col="ID")]
    assert out[0].qname == "USERS"
    assert out[0].col == "ID"


def test_column_lineage_context_bundles_pairs_with_downstream_urn():
    """ColumnLineageContext bundles the state needed by ``_build_upstream_lineage`` so the pieces can't be passed inconsistently."""
    pair = ColumnLineagePair(
        downstream_col="total",
        upstream_refs=[UpstreamColRef(qname="BASE", col="AMOUNT")],
        transform_op="AGGREGATE",
    )
    ctx = ColumnLineageContext(
        pairs=[pair],
        downstream_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:hana,S1.MID,PROD)",
    )
    assert ctx.pairs == [pair]
    assert ctx.downstream_dataset_urn.startswith("urn:li:dataset:")


def test_safe_select_returns_select_dict_for_well_formed_csn():
    extractor = CsnLineageExtractor()
    csn_def = {"query": {"SELECT": {"from": {"ref": ["X"]}, "columns": []}}}
    envelope = extractor._safe_select(csn_def)
    assert envelope.select == {"from": {"ref": ["X"]}, "columns": []}
    assert envelope.malformed is None


def test_safe_select_returns_malformed_pair_for_non_dict_query():
    extractor = CsnLineageExtractor()
    envelope = extractor._safe_select({"query": "not-a-dict"})
    assert envelope.select is None
    assert envelope.malformed is not None
    assert isinstance(envelope.malformed, ColumnLineagePair)
    assert envelope.malformed.downstream_col == MALFORMED_COL_SENTINEL


def test_safe_select_returns_malformed_pair_for_non_dict_select():
    extractor = CsnLineageExtractor()
    envelope = extractor._safe_select({"query": {"SELECT": "not-a-dict"}})
    assert envelope.select is None
    assert envelope.malformed is not None
    assert isinstance(envelope.malformed, ColumnLineagePair)
    assert envelope.malformed.downstream_col == MALFORMED_COL_SENTINEL


def test_safe_select_returns_empty_for_legitimate_base_table():
    extractor = CsnLineageExtractor()
    envelope = extractor._safe_select({"kind": "entity"})
    assert envelope.select is None
    assert envelope.malformed is None


def test_safe_select_returns_empty_for_select_without_select_key():
    """``query`` present but no ``SELECT`` is legitimate (e.g. INSERT/UPDATE CSN forms) — return [] rather than emit malformed."""
    extractor = CsnLineageExtractor()
    envelope = extractor._safe_select({"query": {"INSERT": {}}})
    assert envelope.select is None
    assert envelope.malformed is None


def test_safe_select_returns_empty_for_non_dict_csn():
    extractor = CsnLineageExtractor()
    envelope = extractor._safe_select("not-a-dict")  # type: ignore[arg-type]
    assert envelope.select is None
    assert envelope.malformed is None


def test_malformed_pair_carries_reason_in_unresolved_refs():
    extractor = CsnLineageExtractor()
    pair = extractor._malformed_pair("test reason")
    assert pair.downstream_col == MALFORMED_COL_SENTINEL
    assert pair.upstream_refs == []
    assert pair.unresolved_refs == ["test reason"]


def test_column_lineage_pair_sentinel_with_upstream_refs_raises():
    # Must be a ValueError (not AssertionError): the invariant prevents a malformed
    # schemaField URN and must hold under ``python -O``, which strips asserts.
    with pytest.raises(ValidationError, match="Sentinel ColumnLineagePair"):
        ColumnLineagePair(
            downstream_col=MALFORMED_COL_SENTINEL,
            upstream_refs=[UpstreamColRef(qname="X", col="y")],
        )


def test_walk_expr_diagnostic_escapes_newlines_in_repr():
    """Walker diagnostics must not contain raw newlines, which would break per-line scanning of report.column_lineage_unresolved."""
    extractor = CsnLineageExtractor()
    alias_map = {"u": "USERS"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    weird_ref = {"ref": ["x", "y\nz", "w"]}
    extractor._walk_expr(weird_ref, alias_map, out, unresolved)
    assert len(unresolved) == 1
    assert "\n" not in unresolved[0], (
        f"Newline leaked into report diagnostic: {unresolved[0]!r}"
    )
    assert "\\n" in unresolved[0], (
        f"Expected escaped \\n in diagnostic: {unresolved[0]!r}"
    )


def test_extract_column_lineage_dedups_repeated_unresolved_refs():
    """An xpr referencing the same unknown alias N times should produce ONE unresolved_refs entry, not N."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {
                    "join": "inner",
                    "args": [
                        {"ref": ["KNOWN"], "as": "k"},
                        {"ref": ["OTHER"], "as": "o"},
                    ],
                },
                "columns": [
                    {
                        "xpr": [
                            {"ref": ["x", "A"]},  # unknown alias 'x'
                            "+",
                            {"ref": ["x", "A"]},  # same unknown alias + col
                            "+",
                            {"ref": ["x", "B"]},  # same alias, different col
                        ],
                        "as": "computed",
                    },
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert len(pairs) == 1
    pair = pairs[0]
    # x.A is deduped but x.B stays (different col), so two unresolved entries.
    assert len(pair.unresolved_refs) == 2, (
        f"Expected 2 deduped unresolved entries (x.A + x.B), got "
        f"{len(pair.unresolved_refs)}: {pair.unresolved_refs}"
    )


def test_projection_pseudo_alias_resolves_to_sibling_upstream():
    """A calculated column referencing a sibling output via the ``$projection`` pseudo-alias must resolve to that sibling's real upstream, not an unknown-alias failure."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE_SRC"], "as": "BASE_SRC"},
                "columns": [
                    {"ref": ["BASE_SRC", "base_measure"], "as": "base_measure"},
                    {"ref": ["$projection", "base_measure"], "as": "calc_measure"},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    by_name = {p.downstream_col: p for p in pairs}
    assert by_name["calc_measure"].upstream_refs == [
        UpstreamColRef(qname="BASE_SRC", col="base_measure"),
    ]
    assert by_name["calc_measure"].unresolved_refs == []


def test_projection_pseudo_alias_inside_expression_resolves():
    """``$projection`` refs nested inside an ``xpr`` resolve transitively to the underlying upstream."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE_SRC"]},
                "columns": [
                    {"ref": ["BASE_SRC", "qty"], "as": "qty"},
                    {
                        "xpr": [{"ref": ["$projection", "qty"]}, "*", {"val": 100}],
                        "as": "qty_scaled",
                    },
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    by_name = {p.downstream_col: p for p in pairs}
    assert by_name["qty_scaled"].upstream_refs == [
        UpstreamColRef(qname="BASE_SRC", col="qty")
    ]
    assert by_name["qty_scaled"].transform_op == "EXPRESSION"


def test_projection_pseudo_alias_chained_resolves_transitively():
    """A chain ``c -> $projection.b -> $projection.a -> BASE.x`` resolves all the way down to the real upstream column."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE_SRC"]},
                "columns": [
                    {"ref": ["BASE_SRC", "x"], "as": "a"},
                    {"ref": ["$projection", "a"], "as": "b"},
                    {"ref": ["$projection", "b"], "as": "c"},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    by_name = {p.downstream_col: p for p in pairs}
    assert by_name["c"].upstream_refs == [UpstreamColRef(qname="BASE_SRC", col="x")]
    assert by_name["c"].unresolved_refs == []


def test_projection_pseudo_alias_cycle_terminates_and_records_unresolved():
    """A reference cycle must terminate instead of recursing forever, and the unresolved break is surfaced."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE_SRC"]},
                "columns": [
                    {"ref": ["$projection", "b"], "as": "a"},
                    {"ref": ["$projection", "a"], "as": "b"},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    by_name = {p.downstream_col: p for p in pairs}
    assert any("cycle" in u for u in by_name["a"].unresolved_refs)


def test_projection_pseudo_alias_to_unknown_output_col_is_unresolved():
    """A ``$projection`` ref to a non-existent output column is recorded as unresolved, not silently dropped or resolved to a phantom upstream."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE_SRC"]},
                "columns": [
                    {"ref": ["$projection", "does_not_exist"], "as": "calc"},
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    assert len(pairs) == 1
    assert pairs[0].upstream_refs == []
    assert any("unknown output col" in u for u in pairs[0].unresolved_refs)


def test_walk_expr_projection_without_context_records_unresolved():
    """Without a projection_map (e.g. inside a subquery scope), a ``$projection`` ref lands in ``unresolved`` rather than being attributed to a FROM alias."""
    extractor = CsnLineageExtractor()
    alias_map = {"": "BASE", "BASE": "BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["$projection", "x"]}, alias_map, out, unresolved)
    assert out == []
    assert len(unresolved) == 1
    assert "projection" in unresolved[0].lower()


def test_association_projection_columns_are_skipped():
    """Regression: columns projecting a CDS association element are navigations, not scalar columns, and must not produce phantom column-lineage pairs."""
    csn_def = {
        "kind": "entity",
        "elements": {
            "_DAY_OF_WEEK": {
                "type": "cds.Association",
                "target": "SAP.TIME.M_TIME_DIMENSION_TDAY",
                "on": [{"ref": ["DAY_OF_WEEK"]}, "=", {"ref": ["_DAY_OF_WEEK", "ID"]}],
            },
            "DAY_OF_WEEK": {"type": "cds.String"},
            "MONTH": {"type": "cds.String"},
        },
        "query": {
            "SELECT": {
                "from": {"ref": ["SAP.TIME.M_TIME_DIMENSION"]},
                "columns": [
                    {"ref": ["DAY_OF_WEEK"]},
                    {"ref": ["MONTH"]},
                    {
                        "ref": ["_DAY_OF_WEEK"]
                    },  # association projection — must be skipped
                ],
            }
        },
    }
    extractor = CsnLineageExtractor()
    pairs = extractor.extract_column_lineage(csn_def)
    downstream_cols = {p.downstream_col for p in pairs}
    assert "DAY_OF_WEEK" in downstream_cols
    assert "MONTH" in downstream_cols
    # The association projection must not add a phantom "_DAY_OF_WEEK" upstream.
    assert "_DAY_OF_WEEK" not in downstream_cols
    all_upstream_cols = {ref.col for p in pairs for ref in p.upstream_refs}
    assert "_DAY_OF_WEEK" not in all_upstream_cols


def test_association_projection_skip_is_silent_not_unresolved():
    """Skipping an association projection must be silent — not recorded as an unresolved ref, since it's a legitimate skip."""
    csn_def = {
        "kind": "entity",
        "elements": {
            "_M": {"type": "cds.Association", "target": "OTHER", "on": []},
        },
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [{"ref": ["_M"]}],
            }
        },
    }
    extractor = CsnLineageExtractor()
    pairs = extractor.extract_column_lineage(csn_def)
    assert pairs == []


def test_used_association_target_becomes_table_upstream():
    """An association referenced by the query turns its target into a table-level upstream; a dotted target is flagged qualified so the source layer emits it as-is."""
    csn_def = {
        "kind": "entity",
        "elements": {
            "_DAY": {
                "type": "cds.Association",
                "target": "SAP.TIME.M_TIME_DIMENSION_TDAY",
                "on": [{"ref": ["DAY"]}, "=", {"ref": ["_DAY", "ID"]}],
            },
            "DAY": {"type": "cds.String"},
        },
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [{"ref": ["DAY"]}, {"ref": ["_DAY"]}],
            }
        },
    }
    targets = CsnLineageExtractor().extract_association_targets(csn_def)
    assert [(t.name, t.qualified) for t in targets] == [
        ("SAP.TIME.M_TIME_DIMENSION_TDAY", True)
    ]


def test_declared_but_unused_association_is_not_a_lineage_edge():
    """An association declared but never referenced by the query must not produce an upstream edge (used-only semantics)."""
    csn_def = {
        "kind": "entity",
        "elements": {
            "_UNUSED": {"type": "cds.Association", "target": "OTHER_ENTITY"},
            "COL": {"type": "cds.String"},
        },
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [{"ref": ["COL"]}],
            }
        },
    }
    assert CsnLineageExtractor().extract_association_targets(csn_def) == []


def test_association_qualified_column_ref_resolves_to_target():
    """A field projected through an association is attributed to the association's target entity, not dropped as an unknown alias."""
    csn_def = {
        "kind": "entity",
        "elements": {
            "_CUST": {"type": "cds.Association", "target": "CUSTOMERS"},
            "AMOUNT": {"type": "cds.Decimal"},
        },
        "query": {
            "SELECT": {
                "from": {"ref": ["SALES"], "as": "SALES"},
                "columns": [
                    {"ref": ["AMOUNT"]},
                    {"ref": ["_CUST", "NAME"], "as": "CUSTOMER_NAME"},
                ],
            }
        },
    }
    extractor = CsnLineageExtractor()
    pairs = extractor.extract_column_lineage(csn_def)
    by_col = {p.downstream_col: p for p in pairs}
    assert by_col["CUSTOMER_NAME"].upstream_refs == [
        UpstreamColRef(qname="CUSTOMERS", col="NAME", qualified=False)
    ]
    # And the association target is a table-level upstream.
    targets = extractor.extract_association_targets(csn_def)
    assert [(t.name, t.qualified) for t in targets] == [("CUSTOMERS", False)]


def test_used_composition_target_and_projected_column_resolve_to_target():
    """A cds.Composition behaves like a cds.Association for lineage: a used composition names a table-level upstream and a projected column resolves to its target."""
    csn_def = {
        "kind": "entity",
        "elements": {
            "_ITEMS": {"type": "cds.Composition", "target": "ORDER_ITEMS"},
            "TOTAL": {"type": "cds.Decimal"},
        },
        "query": {
            "SELECT": {
                "from": {"ref": ["ORDERS"], "as": "ORDERS"},
                "columns": [
                    {"ref": ["TOTAL"]},
                    {"ref": ["_ITEMS", "SKU"], "as": "ITEM_SKU"},
                ],
            }
        },
    }
    extractor = CsnLineageExtractor()
    by_col = {p.downstream_col: p for p in extractor.extract_column_lineage(csn_def)}
    assert by_col["ITEM_SKU"].upstream_refs == [
        UpstreamColRef(qname="ORDER_ITEMS", col="SKU", qualified=False)
    ]
    targets = extractor.extract_association_targets(csn_def)
    assert [(t.name, t.qualified) for t in targets] == [("ORDER_ITEMS", False)]


def test_union_collects_upstreams_from_all_branches():
    csn_def = {
        "kind": "entity",
        "query": {
            "SET": {
                "op": "union",
                "all": True,
                "args": [
                    {"SELECT": {"from": {"ref": ["SALES_EU"]}, "columns": []}},
                    {"SELECT": {"from": {"ref": ["SALES_US"]}, "columns": []}},
                ],
            }
        },
    }
    refs = CsnLineageExtractor().extract_upstream_refs(csn_def)
    assert set(refs) == {
        UpstreamRef(name="SALES_EU", qualified=False),
        UpstreamRef(name="SALES_US", qualified=False),
    }


def test_union_merges_column_lineage_by_output_name():
    """A UNION output column draws from the aligned column in every branch, so its upstream refs are the union of both branches."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SET": {
                "op": "union",
                "args": [
                    {
                        "SELECT": {
                            "from": {"ref": ["SALES_EU"], "as": "SALES_EU"},
                            "columns": [{"ref": ["AMOUNT"], "as": "TOTAL"}],
                        }
                    },
                    {
                        "SELECT": {
                            "from": {"ref": ["SALES_US"], "as": "SALES_US"},
                            "columns": [{"ref": ["AMOUNT"], "as": "TOTAL"}],
                        }
                    },
                ],
            }
        },
    }
    pairs = CsnLineageExtractor().extract_column_lineage(csn_def)
    by_col = {p.downstream_col: p for p in pairs}
    assert set(by_col) == {"TOTAL"}
    assert set(by_col["TOTAL"].upstream_refs) == {
        UpstreamColRef(qname="SALES_EU", col="AMOUNT"),
        UpstreamColRef(qname="SALES_US", col="AMOUNT"),
    }


def test_parse_remote_table_source_extracts_connection_and_path():
    entity = {
        REMOTE_CONNECTION_KEY: "MY_REMOTE_CONN",
        REMOTE_ENTITY_KEY: REMOTE_ENTITY_DELIMITER.join(
            ["MY_DB", "MY_SCHEMA", "MY_TABLE"]
        ),
    }
    source = parse_remote_table_source(entity)
    assert source is not None
    assert source.connection == "MY_REMOTE_CONN"
    assert source.path_parts == ["MY_DB", "MY_SCHEMA", "MY_TABLE"]
    assert source.qualified_name == "MY_DB.MY_SCHEMA.MY_TABLE"


def test_parse_remote_table_source_drops_null_database_segment():
    """SAP encodes an absent database segment as the literal '<NULL>'; the qualified name should drop it and any empties."""
    entity = {
        REMOTE_CONNECTION_KEY: "MY_REMOTE_CONN",
        REMOTE_ENTITY_KEY: REMOTE_ENTITY_DELIMITER.join(
            ["<NULL>", "MY_SCHEMA", "MY_TABLE"]
        ),
    }
    source = parse_remote_table_source(entity)
    assert source is not None
    assert source.qualified_name == "MY_SCHEMA.MY_TABLE"


def test_parse_remote_table_source_returns_none_when_not_federated():
    assert parse_remote_table_source({"elements": {}}) is None
    assert parse_remote_table_source({REMOTE_CONNECTION_KEY: "C"}) is None
    assert parse_remote_table_source({REMOTE_ENTITY_KEY: "a\x7fb"}) is None


def test_parse_remote_table_source_strips_sql_quoted_identifier():
    """Some adapters (e.g. HANA DP Agent) encode the origin as a SQL-quoted dotted identifier; the quotes must be stripped so the upstream URN matches the native HANA connector's unquoted form and stitches."""
    entity = {
        REMOTE_CONNECTION_KEY: "HANA_DP_AGENT",
        REMOTE_ENTITY_KEY: '"SAPTCH"."/BIC/AEADOSCM282"',
    }
    source = parse_remote_table_source(entity)
    assert source is not None
    assert source.qualified_name == "SAPTCH./BIC/AEADOSCM282"
