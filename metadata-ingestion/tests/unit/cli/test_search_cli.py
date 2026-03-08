import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from datahub.cli.search_cli import (
    EXIT_CONNECTION,
    EXIT_GENERAL,
    EXIT_PERMISSION,
    EXIT_USAGE,
    SearchCliError,
    _build_search_query,
    _extract_entity_name,
    _load_projection,
    _print_keyword_diagnostics,
    _print_semantic_diagnostics,
    _validate_projection,
    convert_simple_to_filter_dsl,
    describe_filter_func,
    diagnose_keyword_search,
    diagnose_semantic_search,
    execute_search,
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
from datahub.sdk.search_filters import _And


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
        # Single filter should pass through as-is, not wrapped in AND
        assert not isinstance(result, _And)

    def test_convert_simple_to_filter_dsl_multiple(self):
        # Multiple filters must be AND-wrapped so both conditions apply
        parsed = {"platform": ["snowflake"], "env": ["PROD"]}
        result = convert_simple_to_filter_dsl(parsed)
        assert isinstance(result, _And)
        assert len(result.and_) == 2

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


class TestExtractEntityName:
    """Tests for _extract_entity_name covering all DataHub entity name patterns."""

    def test_properties_name(self):
        entity = {"properties": {"name": "my_dataset"}}
        assert _extract_entity_name(entity) == "my_dataset"

    def test_properties_display_name(self):
        entity = {"properties": {"displayName": "My Dashboard"}}
        assert _extract_entity_name(entity) == "My Dashboard"

    def test_properties_name_preferred_over_display_name(self):
        entity = {"properties": {"name": "raw_name", "displayName": "Display Name"}}
        assert _extract_entity_name(entity) == "raw_name"

    def test_info_title(self):
        # Document entities store name under info.title
        entity = {"info": {"title": "My Document"}}
        assert _extract_entity_name(entity) == "My Document"

    def test_top_level_name(self):
        # CorpGroup, Tag, GlossaryTerm store name at top level
        entity = {"name": "my_tag"}
        assert _extract_entity_name(entity) == "my_tag"

    def test_username(self):
        entity = {"username": "alice"}
        assert _extract_entity_name(entity) == "alice"

    def test_no_match_returns_none(self):
        entity = {"urn": "urn:li:dataset:test"}
        assert _extract_entity_name(entity) is None

    def test_ml_entity_with_properties_description_only(self):
        # MLModel has properties.description but NOT properties.name — must not show as "(unnamed)"
        entity = {"name": "my_model", "properties": {"description": "A model"}}
        assert _extract_entity_name(entity) == "my_model"

    def test_corpuser_falls_back_to_username_when_no_display_name(self):
        # CorpUser without displayName must fall back to username
        entity = {"username": "alice", "properties": {"email": "alice@example.com"}}
        assert _extract_entity_name(entity) == "alice"

    def test_corpgroup_falls_back_to_name_when_no_display_name(self):
        # CorpGroup without displayName must fall back to top-level name
        entity = {"name": "data-eng", "properties": {"email": "data-eng@example.com"}}
        assert _extract_entity_name(entity) == "data-eng"


class TestDryRun:
    """Tests for --dry-run flag."""

    def test_dry_run_basic_query(self):
        """Dry-run with a basic query returns expected JSON structure."""
        runner = CliRunner()
        result = runner.invoke(search, ["query", "users", "--dry-run"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["operation_name"] == "search"
        assert parsed["graphql_field"] == "searchAcrossEntities"
        assert parsed["variables"]["query"] == "users"
        assert parsed["variables"]["count"] == 10
        assert parsed["variables"]["start"] == 0

    def test_dry_run_with_filters(self):
        """Dry-run with filters includes compiled filters in variables."""
        runner = CliRunner()
        result = runner.invoke(
            search,
            ["query", "*", "--filter", "platform=snowflake", "--dry-run"],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["variables"]["orFilters"] is not None
        assert len(parsed["variables"]["orFilters"]) > 0

    def test_dry_run_semantic(self):
        """Dry-run with --semantic shows semantic operation name."""
        runner = CliRunner()
        result = runner.invoke(
            search, ["query", "financial reports", "--semantic", "--dry-run"]
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["operation_name"] == "semanticSearch"
        assert parsed["graphql_field"] == "semanticSearchAcrossEntities"

    def test_dry_run_with_sorting(self):
        """Dry-run with sorting includes sortInput in variables."""
        runner = CliRunner()
        result = runner.invoke(
            search,
            ["query", "*", "--sort-by", "name", "--sort-order", "asc", "--dry-run"],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        sort_input = parsed["variables"]["sortInput"]
        assert sort_input["sortCriteria"][0]["field"] == "name"
        assert sort_input["sortCriteria"][0]["sortOrder"] == "ASCENDING"

    def test_dry_run_with_limit_and_offset(self):
        """Dry-run respects --limit and --offset."""
        runner = CliRunner()
        result = runner.invoke(
            search,
            ["query", "*", "--limit", "25", "--offset", "50", "--dry-run"],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["variables"]["count"] == 25
        assert parsed["variables"]["start"] == 50

    def test_dry_run_does_not_call_graph(self):
        """Dry-run does not call get_default_graph (works offline)."""
        with patch("datahub.cli.search_cli.get_default_graph") as mock_get_graph:
            runner = CliRunner()
            result = runner.invoke(search, ["query", "test", "--dry-run"])
            assert result.exit_code == 0
            mock_get_graph.assert_not_called()

    def test_execute_search_dry_run_returns_info_dict(self):
        """execute_search with dry_run=True returns info dict without executing."""
        result = execute_search(
            query="test",
            filters=None,
            num_results=10,
            sort_by=None,
            sort_order="desc",
            offset=0,
            semantic=False,
            facets_only=False,
            view_urn=None,
            dry_run=True,
        )
        assert result["operation_name"] == "search"
        assert result["graphql_field"] == "searchAcrossEntities"
        assert result["variables"]["query"] == "test"

    def test_dry_run_with_view_urn(self):
        """Dry-run includes view URN in variables."""
        runner = CliRunner()
        result = runner.invoke(
            search,
            ["query", "*", "--view", "urn:li:dataHubView:my_view", "--dry-run"],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["variables"]["viewUrn"] == "urn:li:dataHubView:my_view"


class TestAgentContext:
    """Tests for --agent-context flag."""

    def test_agent_context_prints_content(self):
        """--agent-context prints the SEARCH_AGENT_CONTEXT.md file and exits."""
        runner = CliRunner()
        result = runner.invoke(search, ["--agent-context"])
        assert result.exit_code == 0
        assert "# DataHub Search CLI - Agent Context" in result.output

    def test_agent_context_contains_sections(self):
        """Output contains expected section headers."""
        runner = CliRunner()
        result = runner.invoke(search, ["query", "--agent-context"])
        assert result.exit_code == 0
        assert "## Output Discipline" in result.output
        assert "## Projection" in result.output
        assert "## Dry Run" in result.output
        assert "## Filters" in result.output
        assert "## Pagination" in result.output
        assert "## Common Recipes" in result.output

    def test_agent_context_does_not_call_graph(self):
        """--agent-context does not require a DataHub connection."""
        with patch("datahub.cli.search_cli.get_default_graph") as mock_get_graph:
            runner = CliRunner()
            result = runner.invoke(search, ["--agent-context"])
            assert result.exit_code == 0
            mock_get_graph.assert_not_called()


class TestProjection:
    """Tests for --projection flag and related functions."""

    def test_validate_projection_rejects_mutation(self):
        with pytest.raises(Exception, match="selection set"):
            _validate_projection('mutation { deleteEntity(urn: "x") }')

    def test_validate_projection_rejects_introspection(self):
        with pytest.raises(Exception, match="Introspection"):
            _validate_projection("__schema { types { name } }")

    def test_validate_projection_rejects_variables(self):
        with pytest.raises(Exception, match="Variable definitions"):
            _validate_projection("urn $foo")

    def test_validate_projection_rejects_directives(self):
        with pytest.raises(Exception, match="Directives"):
            _validate_projection("urn @skip(if: true)")

    def test_validate_projection_rejects_unbalanced_braces(self):
        with pytest.raises(Exception, match="Unbalanced braces"):
            _validate_projection("{ urn")

    def test_validate_projection_rejects_comments(self):
        with pytest.raises(Exception, match="Comments"):
            _validate_projection("urn # inject")

    def test_validate_projection_rejects_too_long(self):
        with pytest.raises(Exception, match="too long"):
            _validate_projection("x" * 6000)

    def test_validate_projection_accepts_valid(self):
        _validate_projection("{ urn type ... on Dataset { properties { name } } }")

    def test_build_query_default(self):
        """No projection returns the full .gql file content."""
        result = _build_search_query(semantic=False, projection=None)
        assert "SearchEntityInfo" in result
        assert "FacetEntityInfo" in result

    def test_build_query_with_projection(self):
        """Projection replaces SearchEntityInfo with user's selection set."""
        result = _build_search_query(semantic=False, projection="urn type")
        assert "SearchEntityInfo" not in result
        assert "urn type" in result
        assert "FacetEntityInfo" in result

    def test_build_query_includes_platform_fragment(self):
        """PlatformFields fragment is always present in projection queries."""
        result = _build_search_query(
            semantic=False, projection="urn type ...PlatformFields"
        )
        assert "fragment PlatformFields on DataPlatform" in result

    def test_build_query_semantic(self):
        """Semantic query uses correct operation/field names."""
        result = _build_search_query(semantic=True, projection="urn type")
        assert "query semanticSearch(" in result
        assert "semanticSearchAcrossEntities" in result
        assert "matchedFields" in result

    def test_build_query_strips_outer_braces(self):
        """Both '{ urn type }' and 'urn type' produce equivalent queries."""
        with_braces = _build_search_query(semantic=False, projection="{ urn type }")
        without_braces = _build_search_query(semantic=False, projection="urn type")
        assert with_braces == without_braces

    def test_projection_in_dry_run(self):
        """Dry-run output includes projection and built query."""
        runner = CliRunner()
        result = runner.invoke(
            search,
            ["query", "*", "--projection", "urn type", "--dry-run"],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["projection"] == "urn type"
        assert "query" in parsed
        assert "urn type" in parsed["query"]
        assert "SearchEntityInfo" not in parsed["query"]

    def test_load_projection_inline(self):
        """Inline string passes through unchanged."""
        assert _load_projection("urn type") == "urn type"

    def test_load_projection_from_file(self, tmp_path):
        """@path reads file contents."""
        f = tmp_path / "fields.gql"
        f.write_text("urn type\n... on Dataset { properties { name } }\n")
        result = _load_projection(f"@{f}")
        assert "urn type" in result
        assert "Dataset" in result

    def test_projection_cli_flag(self):
        """--projection reaches execute_search via CLI."""
        with patch("datahub.cli.search_cli.get_default_graph") as mock_graph:
            mock_client = mock_graph.return_value
            mock_client.execute_graphql.return_value = {
                "searchAcrossEntities": {
                    "start": 0,
                    "count": 0,
                    "total": 0,
                    "searchResults": [],
                    "facets": [],
                }
            }
            runner = CliRunner()
            result = runner.invoke(
                search,
                ["query", "*", "--projection", "urn type"],
            )
            assert result.exit_code == 0
            call_args = mock_client.execute_graphql.call_args
            query_str = call_args.kwargs.get("query") or call_args[1].get("query")
            assert "SearchEntityInfo" not in query_str
            assert "urn type" in query_str

    def test_projection_validation_runs_before_search(self):
        """Invalid projection is rejected before any network call."""
        with patch("datahub.cli.search_cli.get_default_graph") as mock_graph:
            runner = CliRunner()
            result = runner.invoke(
                search,
                ["query", "*", "--projection", "mutation { bad }"],
            )
            assert result.exit_code != 0
            assert "selection set" in result.output
            mock_graph.assert_not_called()


class TestDiagnoseKeywordSearch:
    """Tests for keyword search diagnostics."""

    def _make_graph(self, side_effect=None, return_value=None):
        from unittest.mock import MagicMock

        graph = MagicMock()
        if side_effect:
            graph.execute_graphql.side_effect = side_effect
        elif return_value is not None:
            graph.execute_graphql.return_value = return_value
        return graph

    def test_connected_and_working(self):
        graph = self._make_graph(
            side_effect=[
                # ping query
                {"appConfig": {"telemetryConfig": {"enableThirdPartyLogging": False}}},
                # wildcard search
                {"searchAcrossEntities": {"total": 42}},
            ]
        )
        result = diagnose_keyword_search(graph)
        assert result["connected"] is True
        assert result["search_works"] is True
        assert result["total_entities"] == 42

    def test_connected_zero_entities(self):
        graph = self._make_graph(
            side_effect=[
                {"appConfig": {"telemetryConfig": {"enableThirdPartyLogging": False}}},
                {"searchAcrossEntities": {"total": 0}},
            ]
        )
        result = diagnose_keyword_search(graph)
        assert result["connected"] is True
        assert result["search_works"] is True
        assert result["total_entities"] == 0

    def test_connection_failure(self):
        graph = self._make_graph(side_effect=Exception("Connection refused"))
        result = diagnose_keyword_search(graph)
        assert result["connected"] is False
        assert "Connection refused" in result["connection_error"]
        assert result["search_works"] is False

    def test_connected_but_search_fails(self):
        graph = self._make_graph(
            side_effect=[
                {"appConfig": {"telemetryConfig": {"enableThirdPartyLogging": False}}},
                Exception("Search index unavailable"),
            ]
        )
        result = diagnose_keyword_search(graph)
        assert result["connected"] is True
        assert result["search_works"] is False
        assert "Search index unavailable" in result["search_error"]


class TestDiagnoseSemanticSearch:
    """Tests for semantic search diagnostics."""

    def _make_graph(self, side_effects):
        from unittest.mock import MagicMock

        graph = MagicMock()
        graph.execute_graphql.side_effect = side_effects
        return graph

    def test_semantic_fully_enabled(self):
        graph = self._make_graph(
            [
                # introspection — semanticSearchAcrossEntities present
                {
                    "__type": {
                        "fields": [
                            {"name": "searchAcrossEntities"},
                            {"name": "semanticSearchAcrossEntities"},
                        ]
                    }
                },
                # config query
                {
                    "appConfig": {
                        "semanticSearchConfig": {
                            "enabled": True,
                            "enabledEntities": ["dataset", "document"],
                            "embeddingConfig": {
                                "provider": "openai",
                                "modelId": "text-embedding-3-large",
                                "modelEmbeddingKey": "text_embedding_3_large",
                                "awsProviderConfig": None,
                            },
                        }
                    }
                },
                # test semantic query succeeds
                {"semanticSearchAcrossEntities": {"total": 5}},
            ]
        )
        result = diagnose_semantic_search(graph)
        assert result["semantic_available"] is True
        assert result["semantic_enabled"] is True
        assert result["semantic_error"] is None
        assert result["config"]["enabled"] is True
        assert "dataset" in result["config"]["enabled_entities"]

    def test_semantic_not_in_schema(self):
        graph = self._make_graph(
            [
                # introspection — no semantic field
                {"__type": {"fields": [{"name": "searchAcrossEntities"}]}},
                # config query
                {"appConfig": {"semanticSearchConfig": {}}},
            ]
        )
        result = diagnose_semantic_search(graph)
        assert result["semantic_available"] is False
        assert result["semantic_enabled"] is False

    def test_semantic_available_but_query_fails(self):
        graph = self._make_graph(
            [
                # introspection — semantic field present
                {
                    "__type": {
                        "fields": [
                            {"name": "searchAcrossEntities"},
                            {"name": "semanticSearchAcrossEntities"},
                        ]
                    }
                },
                # config query
                {"appConfig": {"semanticSearchConfig": {"enabled": False}}},
                # test semantic query fails
                Exception("Semantic search is not enabled"),
            ]
        )
        result = diagnose_semantic_search(graph)
        assert result["semantic_available"] is True
        assert result["semantic_enabled"] is False
        assert "not enabled" in result["semantic_error"]


class TestDiagnosePrinting:
    """Tests for diagnostic text output."""

    def test_print_keyword_connected(self, capsys):
        _print_keyword_diagnostics(
            {
                "connected": True,
                "connection_error": None,
                "search_works": True,
                "total_entities": 100,
                "search_error": None,
            }
        )
        out = capsys.readouterr().out
        assert "Connected to DataHub" in out
        assert "100" in out

    def test_print_keyword_zero_entities_warns(self, capsys):
        _print_keyword_diagnostics(
            {
                "connected": True,
                "connection_error": None,
                "search_works": True,
                "total_entities": 0,
                "search_error": None,
            }
        )
        out = capsys.readouterr().out
        assert "0" in out
        assert "ingestion" in out

    def test_print_keyword_not_connected(self, capsys):
        _print_keyword_diagnostics(
            {
                "connected": False,
                "connection_error": "Connection refused",
                "search_works": False,
                "total_entities": None,
                "search_error": None,
            }
        )
        out = capsys.readouterr().out
        assert "Cannot connect" in out
        assert "Connection refused" in out

    def test_print_semantic_enabled(self, capsys):
        _print_semantic_diagnostics(
            {
                "semantic_available": True,
                "semantic_enabled": True,
                "semantic_error": None,
                "config": {
                    "enabled": True,
                    "enabled_entities": ["dataset"],
                    "embedding_config": {
                        "provider": "openai",
                        "modelId": "text-embedding-3-large",
                        "modelEmbeddingKey": "key",
                        "awsProviderConfig": None,
                    },
                },
            }
        )
        out = capsys.readouterr().out
        assert "ENABLED" in out
        assert "openai" in out
        assert "dataset" in out

    def test_print_semantic_not_available(self, capsys):
        _print_semantic_diagnostics(
            {
                "semantic_available": False,
                "semantic_enabled": False,
                "semantic_error": "Not in schema",
                "config": {},
            }
        )
        out = capsys.readouterr().out
        assert "NOT available" in out


class TestDiagnoseCommand:
    """Tests for the diagnose CLI subcommand."""

    def test_diagnose_text_output(self):
        with patch("datahub.cli.search_cli.get_default_graph") as mock_get_graph:
            mock_graph = mock_get_graph.return_value
            mock_graph.execute_graphql.side_effect = [
                # keyword: ping
                {"appConfig": {"telemetryConfig": {"enableThirdPartyLogging": False}}},
                # keyword: wildcard search
                {"searchAcrossEntities": {"total": 10}},
                # semantic: introspection
                {"__type": {"fields": [{"name": "searchAcrossEntities"}]}},
                # semantic: config
                {"appConfig": {"semanticSearchConfig": {}}},
            ]
            runner = CliRunner()
            result = runner.invoke(search, ["diagnose"])
            assert result.exit_code == 0
            assert "Search Diagnostics" in result.output
            assert "Connected to DataHub" in result.output
            assert "10" in result.output

    def test_diagnose_json_output(self):
        with patch("datahub.cli.search_cli.get_default_graph") as mock_get_graph:
            mock_graph = mock_get_graph.return_value
            mock_graph.execute_graphql.side_effect = [
                {"appConfig": {"telemetryConfig": {"enableThirdPartyLogging": False}}},
                {"searchAcrossEntities": {"total": 5}},
                {"__type": {"fields": [{"name": "searchAcrossEntities"}]}},
                {"appConfig": {"semanticSearchConfig": {}}},
            ]
            runner = CliRunner()
            result = runner.invoke(search, ["diagnose", "--format", "json"])
            assert result.exit_code == 0
            parsed = json.loads(result.output)
            assert parsed["keyword"]["connected"] is True
            assert parsed["keyword"]["total_entities"] == 5
            assert "semantic" in parsed


class TestSearchCliError:
    """Tests for structured error output and exit codes."""

    def test_show_json_when_not_tty(self):
        """Non-TTY stderr gets structured JSON error output."""
        err = SearchCliError(
            "Something failed",
            error_type="search_error",
            suggestion="try again",
        )
        with patch("datahub.cli.search_cli.sys") as mock_sys:
            mock_sys.stderr.isatty.return_value = False
            with patch("datahub.cli.search_cli.click.echo") as mock_echo:
                err.show()
                assert mock_echo.called
                call_args = mock_echo.call_args
                output = call_args[0][0]
                parsed = json.loads(output)
                assert parsed["error"] == "search_error"
                assert parsed["message"] == "Something failed"
                assert parsed["suggestion"] == "try again"
                assert call_args[1]["err"] is True

    def test_show_text_when_tty(self):
        """TTY stderr gets click's default text error output."""
        from datahub.cli.search_cli import click as search_click

        err = SearchCliError(
            "Something failed",
            error_type="search_error",
        )
        with patch("datahub.cli.search_cli.sys") as mock_sys:
            mock_sys.stderr.isatty.return_value = True
            with patch.object(
                search_click.ClickException, "show", return_value=None
            ) as mock_super_show:
                err.show()
                mock_super_show.assert_called_once()

    def test_exit_code_defaults_to_general(self):
        err = SearchCliError("fail")
        assert err.exit_code == EXIT_GENERAL

    def test_exit_code_usage(self):
        err = SearchCliError("bad input", exit_code=EXIT_USAGE)
        assert err.exit_code == EXIT_USAGE

    def test_exit_code_permission(self):
        err = SearchCliError("denied", exit_code=EXIT_PERMISSION)
        assert err.exit_code == EXIT_PERMISSION

    def test_exit_code_connection(self):
        err = SearchCliError("timeout", exit_code=EXIT_CONNECTION)
        assert err.exit_code == EXIT_CONNECTION

    def test_no_suggestion_omits_key(self):
        """When suggestion is None, the JSON output omits the key."""
        err = SearchCliError("fail", error_type="search_error")
        with patch("datahub.cli.search_cli.sys") as mock_sys:
            mock_sys.stderr.isatty.return_value = False
            with patch("datahub.cli.search_cli.click.echo") as mock_echo:
                err.show()
                output = mock_echo.call_args[0][0]
                parsed = json.loads(output)
                assert "suggestion" not in parsed

    def test_invalid_limit_exit_code(self):
        """--limit 0 returns exit code 2 (usage error)."""
        runner = CliRunner()
        result = runner.invoke(search, ["*", "--limit", "0"])
        assert result.exit_code == EXIT_USAGE

    def test_invalid_offset_exit_code(self):
        """--offset -1 returns exit code 2 (usage error)."""
        runner = CliRunner()
        result = runner.invoke(search, ["*", "--offset", "-1"])
        assert result.exit_code == EXIT_USAGE

    def test_semantic_search_error_type(self):
        """Semantic search failure produces correct error type."""
        with patch("datahub.cli.search_cli.get_default_graph") as mock_get_graph:
            mock_graph = mock_get_graph.return_value
            mock_graph.execute_graphql.side_effect = Exception(
                "Semantic search is not enabled"
            )
            runner = CliRunner()
            result = runner.invoke(search, ["query", "test", "--semantic"])
            assert result.exit_code != 0

    def test_connection_error_exit_code(self):
        """Connection errors produce exit code 5."""
        with patch("datahub.cli.search_cli.get_default_graph") as mock_get_graph:
            mock_get_graph.side_effect = Exception("Connection refused")
            runner = CliRunner()
            result = runner.invoke(search, ["query", "test"])
            assert result.exit_code != 0

    def test_agent_context_includes_error_handling_section(self):
        """SEARCH_AGENT_CONTEXT.md documents structured error handling."""
        runner = CliRunner()
        result = runner.invoke(search, ["--agent-context"])
        assert result.exit_code == 0
        assert "## Error Handling" in result.output
        assert "usage_error" in result.output
        assert "connection_error" in result.output
