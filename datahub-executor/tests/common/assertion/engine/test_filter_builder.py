from datahub_executor.common.assertion.engine.evaluator.filter_builder import (
    FilterBuilder,
)
from datahub_executor.common.types import DatasetFilterType


class TestFilterBuilder:
    def test_empty_filter(self) -> None:
        builder = FilterBuilder(None)
        result = builder.get_sql()
        assert result == ""

    def test_remove_where_clause(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "  wHeRE   x = 'value'   "}
        )
        result = builder.get_sql()
        assert result == "x = 'value'"

    def test_remove_semicolon(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "  x = 'value'      ;;;;;   "}
        )
        result = builder.get_sql()
        assert result == "x = 'value'"

    def test_where_in_column_name(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "where_table_name = 'where'"}
        )
        result = builder.get_sql()
        assert result == "where_table_name = 'where'"

    def test_remove_leading_and(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "  AND   x = 'value'   "}
        )
        result = builder.get_sql()
        assert result == "x = 'value'"

    def test_and_in_column_name(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "and_table_name = 'and'"}
        )
        result = builder.get_sql()
        assert result == "and_table_name = 'and'"

    def test_remove_where_and_leading_and(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "WHERE AND x = 'value'"}
        )
        result = builder.get_sql()
        assert result == "x = 'value'"

    def test_remove_multiple_leading_ands(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "AND AND AND x = 'value'"}
        )
        result = builder.get_sql()
        assert result == "x = 'value'"

    def test_or_keyword_not_stripped(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "OR x = 'value'"}
        )
        result = builder.get_sql()
        assert result == "OR x = 'value'"
