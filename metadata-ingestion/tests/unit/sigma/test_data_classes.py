from datahub.ingestion.source.sigma.data_classes import Element


class TestElementColumnFormulas:
    def test_plain_string_columns_keep_empty_formula_map(self) -> None:
        element = Element(
            elementId="element-1",
            name="Plain Columns",
            url="https://example.com/element-1",
            columns=["Account Id", "Status"],
        )

        assert element.columns == ["Account Id", "Status"]
        assert element.column_formulas == {}

    def test_mixed_columns_extract_formula_entries_and_keep_plain_names(self) -> None:
        raw_columns = [
            "Account Id",
            {"name": "Status", "formula": '[Events/status] = "OK"'},
            {"name": "Total", "formula": None},
        ]
        element = Element(
            elementId="element-1",
            name="Mixed Columns",
            url="https://example.com/element-1",
            columns=raw_columns,  # type: ignore[arg-type]
        )

        assert element.columns == ["Account Id", "Status", "Total"]
        assert element.column_formulas == {
            "Status": '[Events/status] = "OK"',
            "Total": None,
        }

    def test_dict_column_without_name_is_skipped(self) -> None:
        element = Element(
            elementId="element-1",
            name="Unnamed Formula Column",
            url="https://example.com/element-1",
            columns=[
                {"formula": "[Events/status]"},
                {"name": "", "formula": "[Events/empty]"},
                {"name": "Status", "formula": "[Events/status]"},
            ],  # type: ignore[arg-type]
        )

        assert element.columns == ["Status"]
        assert element.column_formulas == {"Status": "[Events/status]"}
