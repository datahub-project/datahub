import pytest

from datahub.ingestion.source.hex_v2.yaml_parser import HexYamlParser
from tests.unit.hex_v2.conftest import load_fixture


@pytest.fixture
def parser() -> HexYamlParser:
    return HexYamlParser()


@pytest.fixture
def yaml_content() -> str:
    return load_fixture("project_export.yaml")


class TestHexYamlParser:
    def test_parse_returns_project_id_and_title(
        self, parser: HexYamlParser, yaml_content: str
    ) -> None:
        result = parser.parse(yaml_content)

        assert result is not None
        assert result.project_id == "019db1be-8dd0-7001-9326-5588f745dfe8"
        assert result.title == "Assertions"

    def test_parse_extracts_shared_connection_ids(
        self, parser: HexYamlParser, yaml_content: str
    ) -> None:
        result = parser.parse(yaml_content)

        assert result is not None
        assert "conn-snowflake-us" in result.shared_connection_ids
        assert "conn-snowflake-eu" in result.shared_connection_ids

    def test_only_cells_with_connection_id_included(
        self, parser: HexYamlParser, yaml_content: str
    ) -> None:
        result = parser.parse(yaml_content)

        assert result is not None
        # Only cells with non-None dataConnectionId should be included
        cell_ids = [c.cell_id for c in result.sql_cells]
        assert "cell-001" in cell_ids  # has conn-snowflake-us
        assert "cell-003" in cell_ids  # has conn-snowflake-eu (nested in COLLAPSIBLE)
        # In-memory transform cells must be excluded
        assert "cell-002" not in cell_ids
        assert "cell-004" not in cell_ids

    def test_collapsible_cells_are_walked_recursively(
        self, parser: HexYamlParser, yaml_content: str
    ) -> None:
        result = parser.parse(yaml_content)

        assert result is not None
        # cell-003 is inside a COLLAPSIBLE — must be found
        cell_ids = [c.cell_id for c in result.sql_cells]
        assert "cell-003" in cell_ids

    def test_sql_source_and_connection_preserved(
        self, parser: HexYamlParser, yaml_content: str
    ) -> None:
        result = parser.parse(yaml_content)

        assert result is not None
        cell = next(c for c in result.sql_cells if c.cell_id == "cell-001")
        assert "dim_customer" in cell.sql_source
        assert cell.data_connection_id == "conn-snowflake-us"
        assert cell.cell_label == "Customer list"

    def test_explore_and_markdown_cells_ignored(
        self, parser: HexYamlParser, yaml_content: str
    ) -> None:
        result = parser.parse(yaml_content)

        assert result is not None
        # Only SQL cells should appear
        assert all(c.sql_source for c in result.sql_cells)

    def test_invalid_yaml_returns_none(self, parser: HexYamlParser) -> None:
        result = parser.parse("this: is: not: valid: yaml: ][")

        assert result is None

    def test_empty_yaml_returns_none(self, parser: HexYamlParser) -> None:
        result = parser.parse("")

        assert result is None

    def test_yaml_with_no_sql_cells_returns_empty_list(
        self, parser: HexYamlParser
    ) -> None:
        yaml_content = """
schemaVersion: 3
meta:
  projectId: proj-abc
  title: Empty Project
sharedAssets:
  dataConnections: []
cells:
  - cellType: MARKDOWN
    cellId: md-001
    cellLabel: Notes
    config:
      source: "# Hello"
"""
        result = parser.parse(yaml_content)

        assert result is not None
        assert result.project_id == "proj-abc"
        assert result.sql_cells == []
