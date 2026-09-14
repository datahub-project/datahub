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
    # A JSON null literal renders as SQL NULL, not the Python "None".
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
    # An apostrophe must be doubled so the literal stays valid SQL-like quoting.
    assert render_cqn_expression({"val": "O'Reilly"}) == "'O''Reilly'"


def test_render_function_call():
    node = {"func": "SUM", "args": [{"ref": ["AMOUNT"]}]}
    assert render_cqn_expression(node) == "SUM(AMOUNT)"


def test_render_rejects_expression_with_unrenderable_child():
    # A function or infix expression with an unsupported operand renders empty
    # rather than surfacing a truncated ``COALESCE(A, )`` / ``A +``.
    assert (
        render_cqn_expression({"func": "COALESCE", "args": [{"ref": ["A"]}, {}]}) == ""
    )
    assert render_cqn_expression({"xpr": [{"ref": ["A"]}, "+", {"mystery": 1}]}) == ""


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
    assert render_cqn_expression(node) == "IN(C, (1, 2))"


def test_render_unknown_shape_is_empty():
    assert render_cqn_expression({"mystery": 1}) == ""
    assert render_cqn_expression(None) == ""


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
    # Plain projections carry no formula; only the calculated column does.
    assert formulas == {"TOTAL": "PRICE * QTY"}


def test_extract_formula_from_union_branches():
    # UNION/INTERSECT/EXCEPT bodies live under query.SET.args, not query.SELECT.
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
    # The bare-NULL column is noise; only the real calculation is surfaced.
    assert extract_calculated_column_formulas(csn_def) == {"REAL": "A + B"}


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
    # A rename is not a calculation; an unnamed aggregate has no column to attach to.
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
