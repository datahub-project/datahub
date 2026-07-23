"""Unit tests for omni_lineage_parser helpers."""

import pytest

from datahub.ingestion.source.omni.omni_lineage_parser import (
    FieldRef,
    extract_field_refs,
    normalize_field_name,
    parse_field_list,
)


class TestExtractFieldRefs:
    def test_empty_string(self) -> None:
        assert extract_field_refs("") == set()

    def test_dollar_brace_syntax(self) -> None:
        assert extract_field_refs("${orders.amount}") == {
            FieldRef(view="orders", field="amount")
        }

    def test_multiple_dollar_brace_refs(self) -> None:
        assert extract_field_refs("${orders.amount} + ${orders.tax}") == {
            FieldRef(view="orders", field="amount"),
            FieldRef(view="orders", field="tax"),
        }

    def test_plain_dot_syntax(self) -> None:
        assert extract_field_refs("orders.amount") == {
            FieldRef(view="orders", field="amount")
        }

    def test_sql_expression_with_plain_refs(self) -> None:
        refs = extract_field_refs("SUM(orders.amount)")
        assert FieldRef(view="orders", field="amount") in refs

    def test_mixed_syntax(self) -> None:
        refs = extract_field_refs("${orders.amount} + items.quantity")
        assert FieldRef(view="orders", field="amount") in refs
        assert FieldRef(view="items", field="quantity") in refs

    def test_no_refs_in_plain_sql(self) -> None:
        assert extract_field_refs("SUM(amount)") == set()

    def test_underscored_names(self) -> None:
        assert extract_field_refs("${order_items.line_total}") == {
            FieldRef(view="order_items", field="line_total")
        }

    def test_duplicate_refs_deduplicated(self) -> None:
        refs = extract_field_refs("${orders.amount} + ${orders.amount}")
        assert refs == {FieldRef(view="orders", field="amount")}


class TestNormalizeFieldName:
    def test_plain_name(self) -> None:
        assert normalize_field_name("orders.amount") == "orders.amount"

    def test_dollar_brace_wrapper(self) -> None:
        assert normalize_field_name("${orders.amount}") == "orders.amount"

    def test_whitespace_stripped(self) -> None:
        assert normalize_field_name("  orders.amount  ") == "orders.amount"

    def test_dollar_brace_with_whitespace(self) -> None:
        assert normalize_field_name("  ${orders.amount}  ") == "orders.amount"

    def test_empty_string(self) -> None:
        assert normalize_field_name("") == ""


class TestParseFieldList:
    def test_simple_fields(self) -> None:
        assert parse_field_list(["orders.amount", "orders.tax"]) == {
            FieldRef(view="orders", field="amount"),
            FieldRef(view="orders", field="tax"),
        }

    def test_dollar_brace_fields(self) -> None:
        assert parse_field_list(["${orders.amount}"]) == {
            FieldRef(view="orders", field="amount")
        }

    def test_skips_unqualified_names(self) -> None:
        assert parse_field_list(["amount", "orders.amount"]) == {
            FieldRef(view="orders", field="amount")
        }

    def test_empty_list(self) -> None:
        assert parse_field_list([]) == set()

    @pytest.mark.parametrize(
        "fields,expected",
        [
            (
                ["customers.customer_id", "customers.lifetime_value"],
                {
                    FieldRef(view="customers", field="customer_id"),
                    FieldRef(view="customers", field="lifetime_value"),
                },
            ),
            (
                ["${orders.created_at}", "orders.total_revenue"],
                {
                    FieldRef(view="orders", field="created_at"),
                    FieldRef(view="orders", field="total_revenue"),
                },
            ),
        ],
    )
    def test_mixed_formats(self, fields: list, expected: set) -> None:
        assert parse_field_list(fields) == expected
