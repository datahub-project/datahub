"""Tests for config_constants module."""

from datahub.ingestion.source.kafka_connect.config_constants import (
    parse_comma_separated_list,
)


class TestParseCommaSeparatedList:
    """Test parse_comma_separated_list() edge cases."""

    def test_empty_string(self) -> None:
        """Empty string should return empty list."""
        assert parse_comma_separated_list("") == []

    def test_whitespace_only(self) -> None:
        """Whitespace-only string should return empty list."""
        assert parse_comma_separated_list("   ") == []
        assert parse_comma_separated_list("\t\n") == []

    def test_single_item(self) -> None:
        """Single item should return list with one element."""
        assert parse_comma_separated_list("item1") == ["item1"]

    def test_multiple_items(self) -> None:
        """Multiple items should be split correctly."""
        assert parse_comma_separated_list("item1,item2,item3") == [
            "item1",
            "item2",
            "item3",
        ]

    def test_leading_comma(self) -> None:
        """Leading comma should be ignored."""
        assert parse_comma_separated_list(",item1,item2") == ["item1", "item2"]

    def test_trailing_comma(self) -> None:
        """Trailing comma should be ignored."""
        assert parse_comma_separated_list("item1,item2,") == ["item1", "item2"]

    def test_leading_and_trailing_commas(self) -> None:
        """Leading and trailing commas should be ignored."""
        assert parse_comma_separated_list(",item1,item2,") == ["item1", "item2"]

    def test_consecutive_commas(self) -> None:
        """Multiple consecutive commas should be treated as empty items and filtered."""
        assert parse_comma_separated_list("item1,,item2,,,item3") == [
            "item1",
            "item2",
            "item3",
        ]

    def test_whitespace_around_items(self) -> None:
        """Whitespace around items should be stripped."""
        assert parse_comma_separated_list(" item1 , item2 , item3 ") == [
            "item1",
            "item2",
            "item3",
        ]

    def test_whitespace_only_items_filtered(self) -> None:
        """Items that are only whitespace should be filtered out."""
        assert parse_comma_separated_list("item1,  ,item2") == ["item1", "item2"]
        assert parse_comma_separated_list("item1, \t ,item2") == ["item1", "item2"]

    def test_mixed_whitespace_and_empty(self) -> None:
        """Mixed whitespace and empty items should all be filtered."""
        assert parse_comma_separated_list("item1, , ,  ,item2") == ["item1", "item2"]

    def test_complex_real_world_example(self) -> None:
        """Real-world example with various edge cases."""
        input_str = " table1 , , table2,  table3  ,, table4,  "
        expected = ["table1", "table2", "table3", "table4"]
        assert parse_comma_separated_list(input_str) == expected

    def test_items_with_special_characters(self) -> None:
        """Items with special characters should be preserved."""
        assert parse_comma_separated_list("schema.table1,schema.table2") == [
            "schema.table1",
            "schema.table2",
        ]
        assert parse_comma_separated_list("db-name,table_name") == [
            "db-name",
            "table_name",
        ]

    def test_items_with_numbers(self) -> None:
        """Items with numbers should be preserved."""
        assert parse_comma_separated_list("table1,table2,table3") == [
            "table1",
            "table2",
            "table3",
        ]

    def test_very_long_list(self) -> None:
        """Long lists should be handled correctly."""
        items = [f"item{i}" for i in range(100)]
        input_str = ",".join(items)
        assert parse_comma_separated_list(input_str) == items


# TestConnectorConfigKeys class removed - testing string constant assignments
# provides no value as typos would be immediately visible in code.
# These are simple string assignments, not business logic that needs protection.
