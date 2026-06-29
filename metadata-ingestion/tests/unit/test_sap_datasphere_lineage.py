from typing import List

import pytest

from datahub.ingestion.source.sap_datasphere.lineage import (
    _MALFORMED_COL_SENTINEL,
    ColumnLineageContext,
    ColumnLineagePair,
    CsnLineageExtractor,
    UpstreamColRef,
)


def test_column_lineage_pair_sentinel_with_no_upstream_refs_is_valid():
    """A sentinel downstream_col with empty upstream_refs satisfies the invariant."""
    pair = ColumnLineagePair(downstream_col=_MALFORMED_COL_SENTINEL)
    assert pair.downstream_col == _MALFORMED_COL_SENTINEL
    assert pair.upstream_refs == ()


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
    assert upstreams == ["SAP.TIME.M_TIME_DIMENSION"]


def test_lineage_returns_empty_when_no_query():
    csn_def = {"kind": "entity", "elements": {"COL1": {"type": "cds.String"}}}
    extractor = CsnLineageExtractor()
    assert extractor.extract_upstream_refs(csn_def) == []


def test_lineage_returns_empty_when_no_from_ref():
    # A CSN definition without a 'from' clause (degenerate / synthetic entity)
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
        "SAP.TIME.M_TIME_DIMENSION",
        "SAP.TIME.M_TIME_DIMENSION_TMONTH",
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
    """L5: CSN ``query.SELECT.from.SELECT.from.ref[]`` — an inline subquery should
    be unwrapped one level so the inner table reference becomes the upstream."""
    extractor = CsnLineageExtractor()
    csn_def = {
        "kind": "entity",
        "query": {"SELECT": {"from": {"SELECT": {"from": {"ref": ["INNER_TABLE"]}}}}},
    }
    refs = extractor.extract_upstream_refs(csn_def)
    assert refs == ["INNER_TABLE"]


def test_walk_from_handles_doubly_nested_subquery():
    """L5: arbitrary nesting depth — three-level inline subquery still resolves."""
    csn_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"SELECT": {"from": {"SELECT": {"from": {"ref": ["DEEP"]}}}}}
            }
        },
    }
    refs = CsnLineageExtractor().extract_upstream_refs(csn_def)
    assert refs == ["DEEP"]


def test_lineage_skips_non_string_ref_element():
    """If CSN ref[0] is a dict (parametrized reference) instead of a string,
    the extractor should skip it rather than yielding a non-string upstream."""
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
    # Non-string ref should be skipped entirely
    assert upstreams == [], (
        f"Expected non-string ref[0] to be skipped; got: {upstreams}"
    )
    # Make sure nothing in the result is a dict (sanity check)
    assert all(isinstance(u, str) for u in upstreams)


def test_column_lineage_pair_is_frozen_dataclass():
    """ColumnLineagePair carries the downstream column name + its upstream refs."""
    pair = ColumnLineagePair(
        downstream_col="total",
        upstream_refs=(UpstreamColRef("BASE_TABLE", "AMOUNT"),),
        transform_op="AGGREGATE",
    )
    assert pair.downstream_col == "total"
    assert pair.upstream_refs == (UpstreamColRef("BASE_TABLE", "AMOUNT"),)
    assert pair.transform_op == "AGGREGATE"


def test_alias_map_single_source_uses_empty_string_key():
    """A single-source FROM with no `as` alias maps the empty-string key to the
    qualified name. This lets `{"ref": ["X"]}` (one segment) resolve unambiguously."""
    extractor = CsnLineageExtractor()
    alias_map = extractor._build_alias_map({"ref": ["BASE_TABLE"]})
    assert alias_map == {"": "BASE_TABLE", "BASE_TABLE": "BASE_TABLE"}


def test_alias_map_with_explicit_alias():
    """Single-source FROM with explicit `as` — both the alias AND `""` map to the
    source. Unqualified column refs MUST still resolve in this shape (real SAP
    CSN uses `FROM T AS T` with unqualified column refs — verified against the
    live tenant)."""
    extractor = CsnLineageExtractor()
    alias_map = extractor._build_alias_map({"ref": ["BASE_TABLE"], "as": "b"})
    assert alias_map == {"b": "BASE_TABLE", "": "BASE_TABLE"}


def test_alias_map_single_source_with_self_alias_still_resolves_unqualified():
    """Real-world case from live SAP tenant: `FROM SAP.TIME.M_TIME_DIMENSION
    AS M_TIME_DIMENSION` with unqualified column refs like `{ref: ["DATE_SQL"]}`."""
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
    assert out == [("BASE", "AMOUNT")]


def test_walk_expr_collects_qualified_ref_via_alias():
    extractor = CsnLineageExtractor()
    alias_map = {"u": "USERS"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["u", "ID"]}, alias_map, out, unresolved)
    assert out == [("USERS", "ID")]


def test_walk_expr_unqualified_ref_in_multi_source_skipped():
    """When the FROM is a JOIN (no empty-string key), unqualified refs cannot be
    safely attributed — collect nothing rather than guess."""
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
    assert out == [("BASE", "AMOUNT")]


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
    assert out == [("BASE", "A"), ("BASE", "B")]


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
    assert out == [("BASE", "X"), ("BASE", "Y")]


def test_walk_expr_scalar_subquery_resolves_against_inner_from():
    """A scalar subquery `(SELECT MAX(amount) FROM SALES)` appearing as a column
    expression should produce a ref to SALES.amount — resolved against the
    SUBQUERY's own FROM, NOT the outer alias map."""
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
    # Ref resolves to SALES.amount (inner FROM), NOT to OUTER_BASE.amount
    assert out == [("SALES", "amount")]
    assert unresolved == []


def test_walk_expr_scalar_subquery_with_join_in_inner_from():
    """Subquery whose inner FROM is a JOIN — refs there use qualified aliases
    against the subquery's own alias map."""
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
    assert out == [("USERS", "id"), ("ORDERS", "amount")]


def test_walk_expr_subquery_without_from_records_unresolved_for_correlated_refs():
    """Subqueries without a FROM clause (e.g., correlated subqueries that rely
    on the outer scope) can't be resolved by us in v1. Refs inside such
    subqueries land in `unresolved` because the inner alias map is empty."""
    extractor = CsnLineageExtractor()
    outer_alias_map = {"": "OUTER_BASE", "OUTER_BASE": "OUTER_BASE"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    correlated = {
        "SELECT": {
            "columns": [
                # This is a correlated ref to the outer scope — we cannot resolve it
                {"func": "COUNT", "args": [{"ref": ["x"]}]},
            ],
        }
    }
    extractor._walk_expr(correlated, outer_alias_map, out, unresolved)
    # The inner alias map is empty (no FROM), so the ref to `x` is unresolvable
    assert out == []
    assert len(unresolved) == 1


def test_walk_expr_subquery_with_no_columns_emits_unresolved():
    """A scalar subquery without an explicit `columns` array (e.g., `SELECT *`)
    cannot be unrolled — record the limitation in `unresolved`."""
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
    """End-to-end through `extract_column_lineage`: a view with one column that
    is itself a scalar subquery `(SELECT MAX(amount) FROM SALES) AS peak`."""
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
    assert pair.upstream_refs == (UpstreamColRef(qname="SALES", col="amount"),)


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
        ColumnLineagePair("ID", (UpstreamColRef("BASE_TABLE", "ID"),), "IDENTITY"),
        ColumnLineagePair("NAME", (UpstreamColRef("BASE_TABLE", "NAME"),), "IDENTITY"),
        ColumnLineagePair(
            "AMOUNT", (UpstreamColRef("BASE_TABLE", "AMOUNT"),), "IDENTITY"
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
            "total", (UpstreamColRef("BASE_TABLE", "AMOUNT"),), "AGGREGATE"
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
        ColumnLineagePair("ID", (UpstreamColRef("USERS", "ID"),), "IDENTITY"),
        ColumnLineagePair("AMOUNT", (UpstreamColRef("ORDERS", "AMOUNT"),), "IDENTITY"),
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
            "total",
            (UpstreamColRef("BASE", "A"), UpstreamColRef("BASE", "B")),
            "EXPRESSION",
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
            "REAL_COL", (UpstreamColRef("BASE", "REAL_COL"),), "IDENTITY"
        ),
    ]


def test_extract_column_lineage_returns_empty_for_legitimate_non_select_csn():
    """Legitimate non-SELECT entity shapes (base table, no query, no columns)
    return [] silently — they are not malformed."""
    extractor = CsnLineageExtractor()
    assert extractor.extract_column_lineage({}) == []
    # No query → base table
    assert extractor.extract_column_lineage({"kind": "entity"}) == []
    # No SELECT under query
    assert extractor.extract_column_lineage({"query": {}}) == []
    # No `from`
    assert extractor.extract_column_lineage({"query": {"SELECT": {}}}) == []
    # No `columns`
    assert (
        extractor.extract_column_lineage(
            {"query": {"SELECT": {"from": {"ref": ["X"]}}}}
        )
        == []
    )


def test_extract_column_lineage_surfaces_malformed_csn_structurally():
    """Structurally-broken CSN (wrong types at expected fields) is surfaced as a
    synthetic <malformed> pair so the caller can populate the report counter."""
    extractor = CsnLineageExtractor()
    # query is a string, not a dict
    pairs = extractor.extract_column_lineage({"query": "not-a-dict"})
    assert len(pairs) == 1
    assert pairs[0].downstream_col == "<malformed>"
    assert pairs[0].unresolved_refs

    # SELECT is a string
    pairs = extractor.extract_column_lineage({"query": {"SELECT": "not-a-dict"}})
    assert len(pairs) == 1
    assert pairs[0].downstream_col == "<malformed>"

    # columns is a string
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
    """A column that mixes resolvable refs with unresolvable ones records both
    on the same pair."""
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
    assert pairs[0].upstream_refs == (UpstreamColRef("USERS", "ID"),)
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
        ColumnLineagePair("doubled", (UpstreamColRef("BASE", "A"),), "EXPRESSION"),
    ]


def test_extract_column_lineage_skips_columns_with_no_resolvable_refs():
    """SELECT * (star expansion) produces a tuple with `*` as col name; the source
    layer is expected to filter it out. The extractor emits whatever CSN gives."""
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
        ColumnLineagePair("*", (UpstreamColRef("BASE", "*"),), "IDENTITY"),
    ]


def test_upstream_col_ref_is_a_named_tuple_that_unpacks_like_a_tuple():
    """UpstreamColRef must remain tuple-compatible so existing call sites
    that unpack ``for qname, col in pair.upstream_refs:`` continue to work."""
    ref = UpstreamColRef(qname="BASE", col="AMOUNT")
    qname, col = ref
    assert qname == "BASE"
    assert col == "AMOUNT"
    # Equality with a plain tuple still holds.
    assert ref == ("BASE", "AMOUNT")


def test_walk_expr_now_emits_upstream_col_ref_instances():
    """``_walk_expr`` records ``UpstreamColRef`` instances; values are
    tuple-equal to the prior 2-tuple shape for back-compat."""
    extractor = CsnLineageExtractor()
    alias_map = {"u": "USERS"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    extractor._walk_expr({"ref": ["u", "ID"]}, alias_map, out, unresolved)
    assert out == [("USERS", "ID")]
    assert isinstance(out[0], UpstreamColRef)
    assert out[0].qname == "USERS"
    assert out[0].col == "ID"


def test_column_lineage_context_bundles_pairs_with_downstream_urn():
    """The ColumnLineageContext value object bundles the two pieces of state
    needed by ``_build_upstream_lineage`` so they can't be passed inconsistently."""
    pair = ColumnLineagePair(
        downstream_col="total",
        upstream_refs=(UpstreamColRef("BASE", "AMOUNT"),),
        transform_op="AGGREGATE",
    )
    ctx = ColumnLineageContext(
        pairs=(pair,),
        downstream_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:hana,S1.MID,PROD)",
    )
    assert ctx.pairs == (pair,)
    assert ctx.downstream_dataset_urn.startswith("urn:li:dataset:")


def test_safe_select_returns_select_dict_for_well_formed_csn():
    extractor = CsnLineageExtractor()
    csn_def = {"query": {"SELECT": {"from": {"ref": ["X"]}, "columns": []}}}
    select, malformed = extractor._safe_select(csn_def)
    assert select == {"from": {"ref": ["X"]}, "columns": []}
    assert malformed is None


def test_safe_select_returns_malformed_pair_for_non_dict_query():
    extractor = CsnLineageExtractor()
    select, malformed = extractor._safe_select({"query": "not-a-dict"})
    assert select is None
    assert malformed is not None
    assert isinstance(malformed, ColumnLineagePair)
    assert malformed.downstream_col == _MALFORMED_COL_SENTINEL


def test_safe_select_returns_malformed_pair_for_non_dict_select():
    extractor = CsnLineageExtractor()
    select, malformed = extractor._safe_select({"query": {"SELECT": "not-a-dict"}})
    assert select is None
    assert malformed is not None
    assert isinstance(malformed, ColumnLineagePair)
    assert malformed.downstream_col == _MALFORMED_COL_SENTINEL


def test_safe_select_returns_none_none_for_legitimate_base_table():
    extractor = CsnLineageExtractor()
    select, malformed = extractor._safe_select({"kind": "entity"})  # no query
    assert select is None
    assert malformed is None


def test_safe_select_returns_none_none_for_select_without_select_key():
    """``query`` present but no ``SELECT`` is legitimate (e.g. INSERT/UPDATE
    CSN forms) — caller should return [] rather than emit malformed."""
    extractor = CsnLineageExtractor()
    select, malformed = extractor._safe_select({"query": {"INSERT": {}}})
    assert select is None
    assert malformed is None


def test_safe_select_returns_none_none_for_non_dict_csn():
    extractor = CsnLineageExtractor()
    select, malformed = extractor._safe_select("not-a-dict")  # type: ignore[arg-type]
    assert select is None
    assert malformed is None


def test_malformed_pair_carries_reason_in_unresolved_refs():
    extractor = CsnLineageExtractor()
    pair = extractor._malformed_pair("test reason")
    assert pair.downstream_col == _MALFORMED_COL_SENTINEL
    assert pair.upstream_refs == ()
    assert pair.unresolved_refs == ("test reason",)


def test_column_lineage_pair_sentinel_with_upstream_refs_raises():
    """Constructing a sentinel-valued pair WITH upstream_refs violates the invariant."""
    # Must be a ValueError (not AssertionError): the invariant prevents a
    # malformed schemaField URN and must hold under ``python -O``, which strips
    # ``assert`` statements.
    with pytest.raises(ValueError, match="Sentinel ColumnLineagePair"):
        ColumnLineagePair(
            downstream_col=_MALFORMED_COL_SENTINEL,
            upstream_refs=(UpstreamColRef("X", "y"),),
        )


def test_walk_expr_diagnostic_escapes_newlines_in_repr():
    """Diagnostic strings emitted by the walker must not contain raw newlines
    (would break per-line scanning of report.column_lineage_unresolved)."""
    extractor = CsnLineageExtractor()
    alias_map = {"u": "USERS"}
    out: List[UpstreamColRef] = []
    unresolved: List[str] = []
    # 3-segment ref with embedded newline in inner segment — exercises the
    # `<unresolvable ref shape ...>` branch (len(segs) != 1 and != 2).
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
    """An xpr referencing the same unknown alias N times should produce ONE
    unresolved_refs entry, not N."""
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
    # Both x.A and x.B references unresolvable; the x.A pair should be deduped
    # but x.B should still be present (different col).
    assert len(pair.unresolved_refs) == 2, (
        f"Expected 2 deduped unresolved entries (x.A + x.B), got "
        f"{len(pair.unresolved_refs)}: {pair.unresolved_refs}"
    )


def test_association_projection_columns_are_skipped():
    """SELECT columns that project a CDS association element (leading-underscore
    name declared as type cds.Association in the view's elements) must NOT
    produce column-lineage pairs — they are navigations to other entities,
    not scalar columns of the FROM table. Regression for phantom fine-grained
    edges that never render in the UI.
    """
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
    # the association projection must NOT appear, and must NOT add a phantom
    # upstream ref to a "_DAY_OF_WEEK" column
    assert "_DAY_OF_WEEK" not in downstream_cols
    all_upstream_cols = {ref.col for p in pairs for ref in p.upstream_refs}
    assert "_DAY_OF_WEEK" not in all_upstream_cols


def test_association_projection_skip_is_silent_not_unresolved():
    """Skipping an association projection should be silent — it must not be
    recorded as an unresolved ref (it's a legitimate, understood skip)."""
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
    # No pairs at all (the only column was an association projection)
    assert pairs == []
