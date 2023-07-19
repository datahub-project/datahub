from datahub_monitors.assertion.engine.evaluator.filter_builder import FilterBuilder
from datahub_monitors.types import DatasetFilterType


class TestFilterBuilder:
    def test_empty_filter(self) -> None:
        builder = FilterBuilder(None)
        result = builder.get_sql()
        assert result == ""

    def test_remove_where_clause(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "  wHeRE   x = 'value'"}
        )
        result = builder.get_sql()
        assert result == "x = 'value'"

    def test_remove_semicolon(self) -> None:
        builder = FilterBuilder(
            {"type": DatasetFilterType.SQL, "sql": "x = 'value';;;;;   "}
        )
        result = builder.get_sql()
        assert result == "x = 'value'"
