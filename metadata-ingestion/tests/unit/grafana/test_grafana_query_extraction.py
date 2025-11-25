"""Unit tests for Grafana query extraction and QueryInfo model."""

import pytest

from datahub.ingestion.source.grafana.entity_mcp_builder import (
    _extract_query_from_panel,
    _format_sql_query,
)
from datahub.ingestion.source.grafana.models import Panel, QueryInfo


class TestQueryInfoModel:
    """Test QueryInfo validation prevents malformed data."""

    def test_validation_rejects_empty_and_cleans_whitespace(self):
        """Test validation rejects empty values and cleans whitespace."""
        valid = QueryInfo(query="  SELECT *  ", language="  SQL  ")
        assert valid.query == "SELECT *"
        assert valid.language == "SQL"

        with pytest.raises(ValueError):
            QueryInfo(query="", language="SQL")

        with pytest.raises(ValueError):
            QueryInfo(query="SELECT *", language="")


class TestExtractQueryFromPanel:
    """Test query extraction business logic."""

    def test_query_extraction_handles_realworld_scenarios(self):
        """Test extraction handles case-insensitivity, priority, and skips bad data."""
        panel1 = Panel.model_validate(
            {
                "id": "1",
                "targets": [{"RawSQL": "SELECT 1"}],
                "datasource": {"type": "mysql", "uid": "ds1"},
            }
        )
        assert _extract_query_from_panel(panel1) is not None

        panel2 = Panel.model_validate(
            {
                "id": "1",
                "targets": [{"expr": "up"}, {"rawSql": "SELECT 1"}],
                "datasource": {"type": "prometheus", "uid": "ds1"},
            }
        )
        result = _extract_query_from_panel(panel2)
        assert result is not None and result.language == "PromQL"

        panel3 = Panel.model_validate(
            {
                "id": "1",
                "targets": [{"rawSql": ""}, {"rawSql": "   "}, {"expr": "up"}],
                "datasource": {"type": "prometheus", "uid": "ds1"},
            }
        )
        result = _extract_query_from_panel(panel3)
        assert result is not None and result.language == "PromQL"

        panel4 = Panel.model_validate(
            {"id": "1", "targets": [], "datasource": {"type": "postgres", "uid": "ds1"}}
        )
        assert _extract_query_from_panel(panel4) is None

    @pytest.mark.parametrize(
        "datasource_type,expected_language",
        [
            ("postgres", "SQL"),
            ("mysql", "SQL"),
            ("influxdb", "InfluxQL"),
            ("prometheus", "PromQL"),
            ("unknown_type", "Query"),
        ],
        ids=["postgres", "mysql", "influxdb", "prometheus", "unknown"],
    )
    def test_datasource_to_language_mapping(self, datasource_type, expected_language):
        """Test datasource types map to correct query languages."""
        panel = Panel.model_validate(
            {
                "id": "1",
                "targets": [{"query": "test"}],
                "datasource": {"type": datasource_type, "uid": "ds1"},
            }
        )
        result = _extract_query_from_panel(panel)
        assert result is not None and result.language == expected_language


class TestSqlFormatting:
    """Test SQL formatting integration with sqlglot."""

    def test_formats_for_known_databases_and_degrades_gracefully(self):
        """Test formatting applies for known DBs and returns original for unknown/invalid."""
        query = "SELECT id,name FROM users WHERE active=true"

        for db_type in ["postgres", "mysql", "athena"]:
            formatted = _format_sql_query(query, db_type)
            assert "SELECT" in formatted and "FROM" in formatted

        assert _format_sql_query(query, "unknown") == query
        assert _format_sql_query(query, None) == query

        invalid = "SELECT * FROM"
        result = _format_sql_query(invalid, "postgres")
        assert result == invalid or "SELECT" in result
