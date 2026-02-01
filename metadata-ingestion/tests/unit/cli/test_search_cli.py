import json

import pytest
from click.testing import CliRunner

from datahub.cli.search_cli import (
    convert_simple_to_filter_dsl,
    describe_filter_func,
    format_facets_output,
    format_json_output,
    format_table_output,
    format_urns_output,
    list_available_filters,
    parse_complex_filters,
    parse_simple_filters,
    search,
    validate_and_merge_filters,
)


class TestFilterParsing:
    """Tests for filter parsing functions."""

    def test_parse_simple_filters_single(self):
        result = parse_simple_filters(["platform=snowflake"])
        assert result == {"platform": ["snowflake"]}

    def test_parse_simple_filters_multiple(self):
        result = parse_simple_filters(["platform=snowflake", "entity_type=dataset"])
        assert result == {"platform": ["snowflake"], "entity_type": ["dataset"]}

    def test_parse_simple_filters_comma_separated(self):
        result = parse_simple_filters(["platform=snowflake,bigquery"])
        assert result == {"platform": ["snowflake", "bigquery"]}

    def test_parse_simple_filters_multiple_same_key(self):
        result = parse_simple_filters(["platform=snowflake", "platform=bigquery"])
        assert result == {"platform": ["snowflake", "bigquery"]}

    def test_parse_simple_filters_mixed(self):
        result = parse_simple_filters(
            ["platform=snowflake,bigquery", "entity_type=dataset"]
        )
        assert result == {
            "platform": ["snowflake", "bigquery"],
            "entity_type": ["dataset"],
        }

    def test_parse_simple_filters_invalid_format(self):
        with pytest.raises(Exception) as exc_info:
            parse_simple_filters(["invalid_filter"])
        assert "Expected key=value" in str(exc_info.value)

    def test_convert_simple_to_filter_dsl_single(self):
        parsed = {"platform": ["snowflake"]}
        result = convert_simple_to_filter_dsl(parsed)
        assert result is not None

    def test_convert_simple_to_filter_dsl_multiple(self):
        parsed = {"platform": ["snowflake"], "entity_type": ["dataset"]}
        result = convert_simple_to_filter_dsl(parsed)
        assert result is not None

    def test_parse_complex_filters_valid(self):
        filters_json = '{"and": [{"platform": ["snowflake"]}, {"env": ["PROD"]}]}'
        result = parse_complex_filters(filters_json)
        assert result is not None

    def test_parse_complex_filters_invalid_json(self):
        with pytest.raises(Exception) as exc_info:
            parse_complex_filters("{invalid json")
        assert "Invalid JSON" in str(exc_info.value)

    def test_validate_and_merge_filters_simple_only(self):
        result = validate_and_merge_filters(["platform=snowflake"], None)
        assert result is not None

    def test_validate_and_merge_filters_complex_only(self):
        result = validate_and_merge_filters([], '{"platform": ["snowflake"]}')
        assert result is not None

    def test_validate_and_merge_filters_both_error(self):
        with pytest.raises(Exception) as exc_info:
            validate_and_merge_filters(["platform=snowflake"], '{"env": ["PROD"]}')
        assert "Cannot use both" in str(exc_info.value)

    def test_validate_and_merge_filters_neither(self):
        result = validate_and_merge_filters([], None)
        assert result is None


class TestOutputFormatting:
    """Tests for output formatting functions."""

    def test_format_json_output(self):
        results = {
            "searchResults": [],
            "total": 0,
            "start": 0,
        }
        output = format_json_output(results)
        assert '"searchResults"' in output
        assert '"total"' in output

    def test_format_json_output_with_results(self):
        results = {
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)",
                        "properties": {"name": "test_dataset"},
                    }
                }
            ],
            "total": 1,
            "start": 0,
        }
        output = format_json_output(results)
        parsed = json.loads(output)
        assert len(parsed["searchResults"]) == 1
        assert parsed["total"] == 1

    def test_format_table_output_empty(self):
        results = {"searchResults": [], "total": 0, "start": 0}
        output = format_table_output(results)
        assert "No results found" in output

    def test_format_table_output_with_results(self):
        results = {
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)",
                        "properties": {
                            "name": "test_dataset",
                            "description": "A test dataset",
                        },
                        "platform": {"name": "snowflake"},
                    }
                }
            ],
            "total": 1,
            "start": 0,
        }
        output = format_table_output(results)
        assert "test_dataset" in output
        assert "snowflake" in output
        assert "Showing 1-1 of 1 results" in output

    def test_format_urns_output_empty(self):
        results: dict = {"searchResults": []}
        output = format_urns_output(results)
        assert output == ""

    def test_format_urns_output_with_results(self):
        results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:dataset:1"}},
                {"entity": {"urn": "urn:li:dataset:2"}},
            ]
        }
        output = format_urns_output(results)
        assert "urn:li:dataset:1" in output
        assert "urn:li:dataset:2" in output
        assert output.count("\n") == 1

    def test_format_facets_output_json(self):
        results = {
            "facets": [
                {
                    "field": "platform",
                    "displayName": "Platform",
                    "aggregations": [
                        {"value": "snowflake", "count": 10},
                        {"value": "bigquery", "count": 5},
                    ],
                }
            ]
        }
        output = format_facets_output(results, "json")
        parsed = json.loads(output)
        assert len(parsed["facets"]) == 1
        assert parsed["facets"][0]["field"] == "platform"

    def test_format_facets_output_table(self):
        results = {
            "facets": [
                {
                    "field": "platform",
                    "displayName": "Platform",
                    "aggregations": [
                        {"value": "snowflake", "count": 10},
                        {"value": "bigquery", "count": 5},
                    ],
                }
            ]
        }
        output = format_facets_output(results, "table")
        assert "Platform" in output
        assert "snowflake" in output
        assert "10" in output

    def test_format_facets_output_empty(self):
        results: dict = {"facets": []}
        output = format_facets_output(results, "table")
        assert "No facets available" in output


class TestDiscoveryCommands:
    """Tests for filter discovery functions."""

    def test_list_available_filters(self, capsys):
        list_available_filters()
        captured = capsys.readouterr()
        assert "Available Filters" in captured.out
        assert "entity_type" in captured.out
        assert "platform" in captured.out
        assert "Usage Examples" in captured.out

    def test_describe_filter_platform(self, capsys):
        describe_filter_func("platform")
        captured = capsys.readouterr()
        assert "Filter: platform" in captured.out
        assert "snowflake" in captured.out

    def test_describe_filter_entity_type(self, capsys):
        describe_filter_func("entity_type")
        captured = capsys.readouterr()
        assert "Filter: entity_type" in captured.out
        assert "dataset" in captured.out

    def test_describe_filter_unknown(self, capsys):
        describe_filter_func("unknown_filter")
        captured = capsys.readouterr()
        assert "Unknown filter" in captured.out


class TestCliCommand:
    """Tests for the CLI command."""

    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(search, ["--help"])
        assert result.exit_code == 0
        assert "Search across DataHub entities" in result.output

    def test_query_help(self):
        runner = CliRunner()
        result = runner.invoke(search, ["query", "--help"])
        assert result.exit_code == 0
        assert "Execute search query" in result.output

    def test_diagnose_help(self):
        runner = CliRunner()
        result = runner.invoke(search, ["diagnose", "--help"])
        assert result.exit_code == 0
        assert "Diagnose search configuration" in result.output

    def test_list_filters(self):
        runner = CliRunner()
        result = runner.invoke(search, ["--list-filters"])
        assert result.exit_code == 0
        assert "Available Filters" in result.output

    def test_list_filters_explicit_query(self):
        runner = CliRunner()
        result = runner.invoke(search, ["query", "--list-filters"])
        assert result.exit_code == 0
        assert "Available Filters" in result.output

    def test_describe_filter(self):
        runner = CliRunner()
        result = runner.invoke(search, ["--describe-filter", "platform"])
        assert result.exit_code == 0
        assert "Filter: platform" in result.output

    def test_describe_filter_explicit_query(self):
        runner = CliRunner()
        result = runner.invoke(search, ["query", "--describe-filter", "platform"])
        assert result.exit_code == 0
        assert "Filter: platform" in result.output

    def test_invalid_limit(self):
        runner = CliRunner()
        result = runner.invoke(search, ["*", "--limit", "0"])
        assert result.exit_code != 0
        assert "must be at least 1" in result.output

    def test_invalid_offset(self):
        runner = CliRunner()
        result = runner.invoke(search, ["*", "--offset", "-1"])
        assert result.exit_code != 0
        assert "must be non-negative" in result.output

    def test_conflicting_filters(self):
        runner = CliRunner()
        result = runner.invoke(
            search,
            [
                "*",
                "--filter",
                "platform=snowflake",
                "--filters",
                '{"env": ["PROD"]}',
            ],
        )
        assert result.exit_code != 0
        assert "Cannot use both" in result.output

    def test_table_shortcut(self):
        runner = CliRunner()
        result = runner.invoke(search, ["query", "--help"])
        assert "--table" in result.output
        assert "Shortcut for --format table" in result.output

    def test_urns_only_shortcut(self):
        runner = CliRunner()
        result = runner.invoke(search, ["query", "--help"])
        assert "--urns-only" in result.output
        assert "Shortcut for --format urns" in result.output


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_parse_simple_filters_whitespace(self):
        result = parse_simple_filters(["platform = snowflake , bigquery "])
        assert result == {"platform": ["snowflake", "bigquery"]}

    def test_format_table_output_missing_fields(self):
        results = {
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:corpuser:test",
                    }
                }
            ],
            "total": 1,
            "start": 0,
        }
        output = format_table_output(results)
        assert "(unnamed)" in output

    def test_format_table_output_long_description(self):
        long_desc = "x" * 100
        results = {
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:dataset:test",
                        "properties": {
                            "name": "test",
                            "description": long_desc,
                        },
                    }
                }
            ],
            "total": 1,
            "start": 0,
        }
        output = format_table_output(results)
        assert "..." in output
        # Check that description was truncated
        assert "xxxx..." in output or "xxx..." in output
