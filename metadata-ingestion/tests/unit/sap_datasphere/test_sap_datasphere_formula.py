from datahub.ingestion.source.sap_datasphere.formula import (
    extract_calculated_column_formulas,
    make_description_with_formula,
    render_cqn_expression,
)


def test_render_simple_ref():
    assert render_cqn_expression({"ref": ["AMOUNT"]}) == "AMOUNT"
    assert render_cqn_expression({"ref": ["T", "AMOUNT"]}) == "T.AMOUNT"


def test_render_strips_projection_alias():
    assert render_cqn_expression({"ref": ["$projection", "qty"]}) == "qty"


def test_render_literals():
    assert render_cqn_expression({"val": 42}) == "42"
    assert render_cqn_expression({"val": "X"}) == "'X'"
    assert render_cqn_expression({"val": True}) == "TRUE"
    # A JSON null renders as SQL NULL, not the Python "None".
    assert render_cqn_expression({"val": None}) == "NULL"


def test_render_null_inside_case_expression():
    node = {
        "xpr": [
            "case",
            "when",
            {"ref": ["X"]},
            "then",
            {"val": None},
            "else",
            {"ref": ["Y"]},
            "end",
        ]
    }
    assert render_cqn_expression(node) == "case when X then NULL else Y end"


def test_render_escapes_embedded_apostrophe():
    assert render_cqn_expression({"val": "O'Reilly"}) == "'O''Reilly'"


def test_render_function_call():
    node = {"func": "SUM", "args": [{"ref": ["AMOUNT"]}]}
    assert render_cqn_expression(node) == "SUM(AMOUNT)"


def test_render_zero_arg_function():
    assert (
        render_cqn_expression({"func": "CURRENT_DATE", "args": []}) == "CURRENT_DATE()"
    )
    assert render_cqn_expression({"func": "CURRENT_DATE"}) == "CURRENT_DATE()"


def test_render_empty_func_name_is_none():
    # An empty ``func`` token must not render as ``()``.
    assert render_cqn_expression({"func": "", "args": [{"ref": ["A"]}]}) is None


def test_render_float_literal():
    assert render_cqn_expression({"val": 1.5}) == "1.5"


def test_render_malformed_val_is_none():
    # A nested dict/list under ``val`` is malformed — never fake a SQL literal.
    assert render_cqn_expression({"val": {"foo": 1}}) is None
    assert render_cqn_expression({"val": [1, 2]}) is None


def test_render_case_as_first_class_key():
    # ``case`` can arrive as a dict key carrying a token stream, not only as a
    # bare token inside an ``xpr``.
    node = {"case": ["when", {"ref": ["X"]}, "then", {"val": 1}, "else", {"val": 0}]}
    assert render_cqn_expression(node) == "when X then 1 else 0"


def test_render_cast_as_first_class_key():
    node = {"cast": [{"ref": ["AMOUNT"]}, "as", "DECIMAL"]}
    assert render_cqn_expression(node) == "AMOUNT as DECIMAL"


def test_render_nested_case_operand_parenthesized():
    # A first-class ``case`` operand inside an infix xpr is wrapped, like a nested xpr.
    node = {
        "xpr": [
            {"case": ["when", {"ref": ["X"]}, "then", {"val": 1}, "else", {"val": 0}]},
            "*",
            {"val": 10},
        ]
    }
    assert render_cqn_expression(node) == "(when X then 1 else 0) * 10"


def test_render_cast_type_only_dict_is_none():
    # A cast node carrying only type metadata (no renderable operand) is skipped.
    assert render_cqn_expression({"cast": {"type": "cds.Decimal"}}) is None


def test_render_rejects_expression_with_unrenderable_child():
    assert (
        render_cqn_expression({"func": "COALESCE", "args": [{"ref": ["A"]}, {}]})
        is None
    )
    assert render_cqn_expression({"xpr": [{"ref": ["A"]}, "+", {"mystery": 1}]}) is None


def test_render_infix_expression():
    node = {"xpr": [{"ref": ["A"]}, "+", {"ref": ["B"]}]}
    assert render_cqn_expression(node) == "A + B"


def test_render_nested_expression_parenthesized():
    node = {
        "xpr": [
            {"xpr": [{"ref": ["A"]}, "+", {"ref": ["B"]}]},
            "*",
            {"val": 100},
        ]
    }
    assert render_cqn_expression(node) == "(A + B) * 100"


def test_render_in_list():
    node = {"func": "IN", "args": [{"ref": ["C"]}, {"list": [{"val": 1}, {"val": 2}]}]}
    assert render_cqn_expression(node) == "C IN (1, 2)"


def test_render_in_parenthesizes_compound_left_operand():
    # A compound left operand must be wrapped, not flattened to ``A + B IN (1)``.
    node = {
        "func": "IN",
        "args": [
            {"xpr": [{"ref": ["A"]}, "+", {"ref": ["B"]}]},
            {"list": [{"val": 1}]},
        ],
    }
    assert render_cqn_expression(node) == "(A + B) IN (1)"


def test_render_in_parenthesizes_case_left_operand():
    node = {
        "func": "IN",
        "args": [
            {"case": ["when", {"ref": ["X"]}, "then", {"val": 1}, "else", {"val": 0}]},
            {"list": [{"val": 1}]},
        ],
    }
    assert render_cqn_expression(node) == "(when X then 1 else 0) IN (1)"


def test_render_unknown_shape_is_none():
    assert render_cqn_expression({"mystery": 1}) is None
    assert render_cqn_expression(None) is None


def test_extract_formula_from_query_column():
    csn_def = {
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {"ref": ["QTY"]},
                    {"xpr": [{"ref": ["PRICE"]}, "*", {"ref": ["QTY"]}], "as": "TOTAL"},
                ],
            }
        }
    }
    formulas = extract_calculated_column_formulas(csn_def)
    assert formulas == {"TOTAL": "PRICE * QTY"}


def test_extract_formula_from_top_level_case_column():
    # A query column whose only calc key is a top-level ``case`` (no ``xpr``
    # wrapper) must still be recognized as calculated, not treated as a rename.
    csn_def = {
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {
                        "case": [
                            "when",
                            {"ref": ["X"]},
                            "then",
                            {"val": 1},
                            "else",
                            {"val": 0},
                        ],
                        "as": "STATUS",
                    },
                ],
            }
        }
    }
    assert extract_calculated_column_formulas(csn_def) == {
        "STATUS": "when X then 1 else 0"
    }


def test_extract_ignores_plain_ref_with_cast_type_metadata():
    # ``cast`` beside a plain ``ref`` is type-cast metadata on an ordinary typed
    # projection (dict value), not a calculation (list value) — must not surface
    # a misleading ``formula: <col>`` line.
    csn_def = {
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {
                        "ref": ["AMOUNT"],
                        "cast": {"type": "cds.Decimal"},
                        "as": "AMOUNT",
                    },
                ],
            }
        }
    }
    assert extract_calculated_column_formulas(csn_def) == {}


def test_extract_union_branches_both_calculated_first_wins():
    # Both branches compute a formula for the same output name; branch 0's
    # formula must win over branch 1's, per the first-occurrence-wins contract.
    csn_def = {
        "query": {
            "SET": {
                "op": "union",
                "args": [
                    _branch(
                        [{"xpr": [{"ref": ["P"]}, "*", {"ref": ["Q"]}], "as": "TOTAL"}]
                    ),
                    _branch(
                        [{"xpr": [{"ref": ["X"]}, "+", {"ref": ["Y"]}], "as": "TOTAL"}]
                    ),
                ],
            }
        }
    }
    assert extract_calculated_column_formulas(csn_def) == {"TOTAL": "P * Q"}


def test_extract_formula_from_union_branches():
    csn_def = {
        "query": {
            "SET": {
                "op": "union",
                "args": [
                    {
                        "SELECT": {
                            "from": {"ref": ["A"]},
                            "columns": [
                                {
                                    "xpr": [{"ref": ["P"]}, "*", {"ref": ["Q"]}],
                                    "as": "TOTAL",
                                },
                            ],
                        }
                    },
                    {
                        "SELECT": {
                            "from": {"ref": ["B"]},
                            "columns": [{"ref": ["TOTAL"]}],
                        }
                    },
                ],
            }
        }
    }
    assert extract_calculated_column_formulas(csn_def) == {"TOTAL": "P * Q"}


def test_extract_union_branch_maps_to_first_branch_output_name():
    # The calculated column lives in the second branch under a different alias; it
    # must surface under the first branch's positional output name.
    csn_def = {
        "query": {
            "SET": {
                "op": "union",
                "args": [
                    {
                        "SELECT": {
                            "from": {"ref": ["A"]},
                            "columns": [{"ref": ["AMOUNT"], "as": "TOTAL"}],
                        }
                    },
                    {
                        "SELECT": {
                            "from": {"ref": ["B"]},
                            "columns": [
                                {
                                    "xpr": [{"ref": ["P"]}, "*", {"ref": ["Q"]}],
                                    "as": "BRANCH2_ALIAS",
                                },
                            ],
                        }
                    },
                ],
            }
        }
    }
    assert extract_calculated_column_formulas(csn_def) == {"TOTAL": "P * Q"}


def _branch(columns):
    return {"SELECT": {"from": {"ref": ["T"]}, "columns": columns}}


def test_extract_union_three_branches_with_extra_trailing_column():
    # Branch 0 defines two output names; a later branch has a third column with no
    # corresponding output name — the index guard must skip it, not raise.
    csn_def = {
        "query": {
            "SET": {
                "op": "union",
                "args": [
                    _branch([{"ref": ["A"]}, {"ref": ["B"]}]),
                    _branch(
                        [
                            {"xpr": [{"ref": ["A"]}, "+", {"val": 1}], "as": "X"},
                            {"ref": ["B"]},
                        ]
                    ),
                    _branch(
                        [
                            {"ref": ["A"]},
                            {"ref": ["B"]},
                            {"xpr": [{"ref": ["C"]}, "*", {"val": 2}], "as": "EXTRA"},
                        ]
                    ),
                ],
            }
        }
    }
    # Branch-1 calc takes branch-0's positional name; branch-2's extra column,
    # having no branch-0 name, falls back to its own alias.
    assert extract_calculated_column_formulas(csn_def) == {
        "A": "A + 1",
        "EXTRA": "C * 2",
    }


def test_extract_formula_from_element_value():
    csn_def = {
        "elements": {
            "GROSS": {
                "type": "cds.Decimal",
                "value": {"xpr": [{"ref": ["NET"]}, "+", {"ref": ["TAX"]}]},
            }
        }
    }
    assert extract_calculated_column_formulas(csn_def) == {"GROSS": "NET + TAX"}


def test_extract_skips_pure_null_placeholder_column():
    csn_def = {
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {"val": None, "as": "PLACEHOLDER"},
                    {"xpr": [{"ref": ["A"]}, "+", {"ref": ["B"]}], "as": "REAL"},
                ],
            }
        }
    }
    assert extract_calculated_column_formulas(csn_def) == {"REAL": "A + B"}


def test_extract_constant_value_column():
    # A column whose only calc key is ``val`` is still a calculated column.
    csn_def = {
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [{"val": 42, "as": "ANSWER"}],
            }
        }
    }
    assert extract_calculated_column_formulas(csn_def) == {"ANSWER": "42"}


def test_extract_duplicate_name_first_occurrence_wins():
    # The same output name defined by a query column and an element: the query
    # column is read first, so its formula is authoritative.
    csn_def = {
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {"xpr": [{"ref": ["A"]}, "+", {"ref": ["B"]}], "as": "TOTAL"},
                ],
            }
        },
        "elements": {
            "TOTAL": {"value": {"xpr": [{"ref": ["C"]}, "*", {"ref": ["D"]}]}},
        },
    }
    assert extract_calculated_column_formulas(csn_def) == {"TOTAL": "A + B"}


def test_extract_ignores_plain_and_unnamed_columns():
    csn_def = {
        "query": {
            "SELECT": {
                "from": {"ref": ["BASE"]},
                "columns": [
                    {"ref": ["A"], "as": "renamed"},
                    {"func": "SUM", "args": [{"ref": ["X"]}]},
                ],
            }
        }
    }
    assert extract_calculated_column_formulas(csn_def) == {}


def test_make_description_combines_label_and_formula():
    assert (
        make_description_with_formula("Total revenue", "PRICE * QTY")
        == "Total revenue\n\nformula: PRICE * QTY"
    )


def test_make_description_formula_only():
    assert make_description_with_formula(None, "PRICE * QTY") == "formula: PRICE * QTY"


def test_make_description_label_only():
    assert make_description_with_formula("Just a label", None) == "Just a label"


def test_make_description_empty():
    assert make_description_with_formula(None, None) is None
