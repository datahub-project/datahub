"""Unit tests for Grafana query extraction and QueryInfo model."""

import pytest

from datahub.ingestion.source.grafana.entity_mcp_builder import (
    _extract_query_from_panel,
    _format_sql_query,
)
from datahub.ingestion.source.grafana.lineage import _clean_grafana_template_variables
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


class TestGrafanaTemplateVariableCleaning:
    """Test Grafana template variable removal for SQL parsing."""

    @pytest.mark.parametrize(
        "input_query,expected_pattern",
        [
            # ${...} format with formatting → string literal
            (
                "WHERE time > ${__from:date:'YYYY/MM/DD'}",
                "WHERE time > 'grafana_var'",
            ),
            # [[...]] deprecated format → identifier (often table/column names)
            (
                "SELECT * FROM [[table_name]]",
                "SELECT * FROM grafana_identifier",
            ),
            # $__timeFilter macro → TRUE (boolean condition)
            (
                "WHERE $__timeFilter(timestamp_column)",
                "WHERE TRUE",
            ),
            # Simple $variable format → string literal
            (
                "SELECT * FROM $datasource.table",
                "SELECT * FROM 'grafana_var'.table",
            ),
            # Multiple variables in one query
            (
                "SELECT * FROM $table WHERE date > ${__from} AND status = '$status'",
                "SELECT * FROM 'grafana_var' WHERE date > 'grafana_var' AND status = ''grafana_var''",
            ),
            # Real-world complex query from user
            (
                "cast(date_trunc('day', from_unixtime(event_timestamp)) as varchar) as event_day WHERE from_unixtime(event_timestamp) > cast (replace(${__from:date:'YYYY/MM/DD'}, '/','-') as timestamp)",
                "cast(date_trunc('day', from_unixtime(event_timestamp)) as varchar) as event_day WHERE from_unixtime(event_timestamp) > cast (replace('grafana_var', '/','-') as timestamp)",
            ),
            # No variables - query unchanged
            (
                "SELECT * FROM users WHERE active = true",
                "SELECT * FROM users WHERE active = true",
            ),
            # Time macros replaced with TRUE
            (
                "WHERE $__timeFrom() < timestamp AND $__timeTo() > timestamp",
                "WHERE TRUE < timestamp AND TRUE > timestamp",
            ),
            # Macro without parentheses (common usage)
            (
                "WHERE event_timestamp $__timeFilter AND status = 'active'",
                "WHERE event_timestamp TRUE AND status = 'active'",
            ),
            # Variable inside quotes (gets double-quoted but valid SQL)
            (
                "WHERE lower(sensor_serial) = lower('$serial')",
                "WHERE lower(sensor_serial) = lower(''grafana_var'')",
            ),
            # Real-world user query with macro without parens
            (
                "select cast(event_timestamp as timestamp) from datalake_agg.devices where event_timestamp $__timeFilter and lower(sensor_serial) = lower('$serial') order by 1",
                "select cast(event_timestamp as timestamp) from datalake_agg.devices where event_timestamp TRUE and lower(sensor_serial) = lower(''grafana_var'') order by 1",
            ),
        ],
        ids=[
            "braced_with_format",
            "deprecated_brackets",
            "time_filter_macro",
            "simple_dollar",
            "multiple_variables",
            "realworld_complex",
            "no_variables",
            "time_macros",
            "macro_without_parens",
            "variable_in_quotes",
            "realworld_user_query",
        ],
    )
    def test_removes_all_grafana_variable_formats(self, input_query, expected_pattern):
        """Test all Grafana variable formats are replaced with context-appropriate values."""
        result = _clean_grafana_template_variables(input_query)
        assert result == expected_pattern


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

    def test_preserves_grafana_variables_for_display(self):
        """Test that SQL formatting preserves Grafana template variables for human readability."""
        # View definition should show original query with variables intact
        query_with_vars = "SELECT * FROM users WHERE created > ${__from:date}"

        formatted = _format_sql_query(query_with_vars, "postgres")

        # Should still contain the original Grafana variable
        assert "${__from:date}" in formatted
        assert "SELECT" in formatted
