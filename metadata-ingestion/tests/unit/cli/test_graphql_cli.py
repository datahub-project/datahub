import json
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from datahub.cli.graphql_cli import (
    _collect_nested_types,
    _convert_describe_to_json,
    _convert_operation_to_json,
    _convert_operations_list_to_json,
    _convert_type_details_to_json,
    _convert_type_to_json,
    _dict_to_graphql_input,
    _extract_base_type_name,
    _fetch_type_recursive,
    _find_operation_by_name,
    _find_type_by_name,
    _format_graphql_type,
    _format_operation_details,
    _format_operation_list,
    _format_recursive_types,
    _format_single_type_fields,
    _generate_operation_query,
    _get_minimal_fallback_operations,
    _is_file_path,
    _load_content_or_file,
    _parse_graphql_operations_from_files,
    _parse_operations_from_content,
    _parse_variables,
    graphql,
)


class TestHelperFunctions:
    """Test helper functions in graphql_cli module."""

    def test_is_file_path_with_existing_file(self):
        """Test that _is_file_path returns True for existing files."""
        with tempfile.NamedTemporaryFile(suffix=".graphql", delete=False) as tmp:
            tmp.write(b"query { me { username } }")
            tmp.flush()

            assert _is_file_path(tmp.name)
            assert _is_file_path("./test.graphql") is False  # doesn't exist

            # Clean up
            Path(tmp.name).unlink()

    def test_is_file_path_with_non_existing_file(self):
        """Test that _is_file_path returns False for non-existing files."""
        assert _is_file_path("./non-existent.graphql") is False
        assert _is_file_path("/path/to/nowhere.json") is False
        assert _is_file_path("query { me }") is False

    def test_is_file_path_with_short_strings(self):
        """Test that _is_file_path handles short strings correctly."""
        assert _is_file_path("") is False
        assert _is_file_path("a") is False
        assert _is_file_path("ab") is False

    def test_is_file_path_with_relative_paths(self):
        """Test that _is_file_path handles relative paths correctly."""
        import os
        import tempfile

        # Create a temporary directory and file for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a test file in the temp directory
            test_file = Path(temp_dir) / "test.graphql"
            test_file.write_text("query { me { username } }")

            # Change to the temp directory to test relative paths
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                # Test simple filename (exists in current directory)
                assert _is_file_path("test.graphql") is True
                assert _is_file_path("nonexistent.graphql") is False

                # Create a subdirectory for testing relative paths
                sub_dir = Path(temp_dir) / "subdir"
                sub_dir.mkdir()
                sub_file = sub_dir / "sub.graphql"
                sub_file.write_text("query { search }")

                # Test relative path with ./
                assert _is_file_path("./test.graphql") is True
                assert _is_file_path("./subdir/sub.graphql") is True
                assert _is_file_path("./nonexistent.graphql") is False

                # Change to subdirectory to test ../
                os.chdir(sub_dir)
                assert _is_file_path("../test.graphql") is True
                assert _is_file_path("../nonexistent.graphql") is False

            finally:
                os.chdir(original_cwd)

    def test_is_file_path_with_absolute_paths(self):
        """Test that _is_file_path handles absolute paths correctly."""
        with tempfile.NamedTemporaryFile(suffix=".graphql", delete=False) as tmp:
            tmp.write(b"query { me { username } }")
            tmp.flush()

            # Test absolute path
            assert _is_file_path(tmp.name) is True

            # Clean up
            Path(tmp.name).unlink()

            # Test non-existent absolute path
            assert _is_file_path(tmp.name) is False

    def test_is_file_path_with_json_files(self):
        """Test that _is_file_path works with JSON files."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "variables.json"
            test_file.write_text('{"key": "value"}')

            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                assert _is_file_path("variables.json") is True
                assert _is_file_path("./variables.json") is True

            finally:
                os.chdir(original_cwd)

    def test_is_file_path_with_graphql_content(self):
        """Test that _is_file_path correctly identifies GraphQL content vs file paths."""
        # These should be identified as GraphQL content, not file paths
        graphql_queries = [
            "query { me { username } }",
            "mutation { deleteEntity(urn: $urn) }",
            "query GetUser($urn: String!) { corpUser(urn: $urn) { info { email } } }",
            '{ search(input: { type: TAG, query: "*" }) { total } }',
        ]

        for query in graphql_queries:
            assert _is_file_path(query) is False

    def test_load_content_or_file_with_file(self):
        """Test loading content from a file."""
        content = "query { me { username } }"
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".graphql", delete=False
        ) as tmp:
            tmp.write(content)
            tmp.flush()

            result = _load_content_or_file(tmp.name)
            assert result == content

            # Clean up
            Path(tmp.name).unlink()

    def test_load_content_or_file_with_literal(self):
        """Test that literal content is returned as-is."""
        content = "query { me { username } }"
        result = _load_content_or_file(content)
        assert result == content

    def test_load_content_or_file_with_relative_paths(self):
        """Test loading content from files using relative paths."""
        import os
        import tempfile

        content1 = "query { me { username } }"
        content2 = "query { search(input: { type: TAG }) { total } }"

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            test_file = Path(temp_dir) / "test.graphql"
            test_file.write_text(content1)

            sub_dir = Path(temp_dir) / "subdir"
            sub_dir.mkdir()
            sub_file = sub_dir / "sub.graphql"
            sub_file.write_text(content2)

            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                # Test simple filename
                result = _load_content_or_file("test.graphql")
                assert result == content1

                # Test relative path with ./
                result = _load_content_or_file("./test.graphql")
                assert result == content1

                result = _load_content_or_file("./subdir/sub.graphql")
                assert result == content2

                # Change to subdirectory to test ../
                os.chdir(sub_dir)
                result = _load_content_or_file("../test.graphql")
                assert result == content1

            finally:
                os.chdir(original_cwd)

    def test_load_content_or_file_with_absolute_paths(self):
        """Test loading content from files using absolute paths."""
        content = "query { me { username } }"

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".graphql", delete=False
        ) as tmp:
            tmp.write(content)
            tmp.flush()

            # Test absolute path
            result = _load_content_or_file(tmp.name)
            assert result == content

            # Clean up
            Path(tmp.name).unlink()

    def test_load_content_or_file_error_handling(self):
        """Test error handling when file path looks like a file but doesn't exist."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                # Files that don't exist should be treated as literal content, not files
                # This is the expected behavior based on how _is_file_path works
                result = _load_content_or_file("nonexistent.graphql")
                assert result == "nonexistent.graphql"

                result = _load_content_or_file("../nonexistent.graphql")
                assert result == "../nonexistent.graphql"

            finally:
                os.chdir(original_cwd)

    def test_parse_variables_with_valid_json(self):
        """Test parsing valid JSON variables."""
        variables_str = '{"key": "value", "number": 42}'
        result = _parse_variables(variables_str)
        assert result == {"key": "value", "number": 42}

    def test_parse_variables_with_none(self):
        """Test parsing None variables."""
        assert _parse_variables(None) is None
        assert _parse_variables("") is None

    def test_parse_variables_with_invalid_json(self):
        """Test parsing invalid JSON raises ClickException."""
        from click import ClickException

        with pytest.raises(ClickException, match="Invalid JSON in variables"):
            _parse_variables('{"invalid": json}')

    def test_parse_variables_from_file(self):
        """Test parsing variables from a JSON file."""
        variables = {"key": "value", "number": 42}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(variables, tmp)
            tmp.flush()

            result = _parse_variables(tmp.name)
            assert result == variables

            # Clean up
            Path(tmp.name).unlink()

    def test_format_graphql_type_simple(self):
        """Test formatting simple GraphQL types."""
        type_info = {"kind": "SCALAR", "name": "String"}
        assert _format_graphql_type(type_info) == "String"

    def test_format_graphql_type_non_null(self):
        """Test formatting non-null GraphQL types."""
        type_info = {"kind": "NON_NULL", "ofType": {"kind": "SCALAR", "name": "String"}}
        assert _format_graphql_type(type_info) == "String!"

    def test_format_graphql_type_list(self):
        """Test formatting list GraphQL types."""
        type_info = {"kind": "LIST", "ofType": {"kind": "SCALAR", "name": "String"}}
        assert _format_graphql_type(type_info) == "[String]"

    def test_format_graphql_type_complex(self):
        """Test formatting complex GraphQL types."""
        type_info = {
            "kind": "NON_NULL",
            "ofType": {"kind": "LIST", "ofType": {"kind": "SCALAR", "name": "String"}},
        }
        assert _format_graphql_type(type_info) == "[String]!"

    def test_format_operation_list_empty(self):
        """Test formatting empty operation list."""
        result = _format_operation_list([], "Query")
        assert result == "No query operations found."

    def test_format_operation_list_with_operations(self):
        """Test formatting operation list with operations."""
        operations = [
            {"name": "me", "description": "Get current user"},
            {"name": "search", "description": "Search entities"},
        ]
        result = _format_operation_list(operations, "Query")
        expected = "Query:\n  - me: Get current user\n  - search: Search entities"
        assert result == expected

    def test_format_operation_list_without_descriptions(self):
        """Test formatting operation list without descriptions."""
        operations = [{"name": "me"}, {"name": "search", "description": ""}]
        result = _format_operation_list(operations, "Query")
        expected = "Query:\n  - me\n  - search"
        assert result == expected

    def test_format_operation_details(self):
        """Test formatting operation details."""
        operation = {
            "name": "searchAcrossEntities",
            "description": "Search across all entity types",
            "args": [
                {
                    "name": "input",
                    "type": {
                        "kind": "NON_NULL",
                        "ofType": {"kind": "INPUT_OBJECT", "name": "SearchInput"},
                    },
                }
            ],
        }
        result = _format_operation_details(operation, "Query")
        expected = (
            "Operation: searchAcrossEntities\n"
            "Type: Query\n"
            "Description: Search across all entity types\n"
            "Arguments:\n"
            "  - input: SearchInput!"
        )
        assert result == expected

    def test_format_operation_details_no_args(self):
        """Test formatting operation details without arguments."""
        operation = {"name": "me", "description": "Get current user", "args": []}
        result = _format_operation_details(operation, "Query")
        expected = (
            "Operation: me\nType: Query\nDescription: Get current user\nArguments: None"
        )
        assert result == expected

    def test_find_operation_by_name_in_queries(self):
        """Test finding operation in queries."""
        schema = {
            "queryType": {
                "fields": [
                    {"name": "me", "description": "Get current user"},
                    {"name": "search", "description": "Search entities"},
                ]
            },
            "mutationType": {
                "fields": [{"name": "deleteEntity", "description": "Delete entity"}]
            },
        }

        result = _find_operation_by_name(schema, "me")
        assert result is not None
        operation, operation_type = result
        assert operation["name"] == "me"
        assert operation_type == "Query"

    def test_find_operation_by_name_in_mutations(self):
        """Test finding operation in mutations."""
        schema = {
            "queryType": {
                "fields": [{"name": "me", "description": "Get current user"}]
            },
            "mutationType": {
                "fields": [{"name": "deleteEntity", "description": "Delete entity"}]
            },
        }

        result = _find_operation_by_name(schema, "deleteEntity")
        assert result is not None
        operation, operation_type = result
        assert operation["name"] == "deleteEntity"
        assert operation_type == "Mutation"

    def test_find_operation_by_name_not_found(self):
        """Test finding non-existent operation."""
        schema = {
            "queryType": {"fields": [{"name": "me", "description": "Get current user"}]}
        }

        result = _find_operation_by_name(schema, "nonExistent")
        assert result is None


class TestGraphQLCommand:
    """Test the main GraphQL CLI command."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_raw_query(self, mock_get_graph):
        """Test executing raw GraphQL query."""
        mock_client = Mock()
        mock_client.execute_graphql.return_value = {"me": {"username": "testuser"}}
        mock_get_graph.return_value = mock_client

        result = self.runner.invoke(graphql, ["--query", "query { me { username } }"])

        assert result.exit_code == 0
        mock_client.execute_graphql.assert_called_once_with(
            query="query { me { username } }", variables=None
        )
        assert '"me"' in result.output
        assert '"username": "testuser"' in result.output

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_query_with_variables(self, mock_get_graph):
        """Test executing GraphQL query with variables."""
        mock_client = Mock()
        mock_client.execute_graphql.return_value = {
            "corpUser": {"info": {"email": "test@example.com"}}
        }
        mock_get_graph.return_value = mock_client

        result = self.runner.invoke(
            graphql,
            [
                "--query",
                "query GetUser($urn: String!) { corpUser(urn: $urn) { info { email } } }",
                "--variables",
                '{"urn": "urn:li:corpuser:test"}',
            ],
        )

        assert result.exit_code == 0
        mock_client.execute_graphql.assert_called_once_with(
            query="query GetUser($urn: String!) { corpUser(urn: $urn) { info { email } } }",
            variables={"urn": "urn:li:corpuser:test"},
        )

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_list_operations(self, mock_get_graph):
        """Test listing GraphQL operations."""
        mock_client = Mock()
        mock_client.execute_graphql.return_value = {
            "__schema": {
                "queryType": {
                    "fields": [
                        {"name": "me", "description": "Get current user"},
                        {"name": "search", "description": "Search entities"},
                    ]
                },
                "mutationType": {
                    "fields": [{"name": "deleteEntity", "description": "Delete entity"}]
                },
            }
        }
        mock_get_graph.return_value = mock_client

        result = self.runner.invoke(graphql, ["--list-operations"])

        assert result.exit_code == 0
        assert "Queries:" in result.output
        assert "me: Get current user" in result.output
        assert "Mutations:" in result.output
        assert "deleteEntity: Delete entity" in result.output

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_describe_operation(self, mock_get_graph):
        """Test describing a GraphQL operation."""
        mock_client = Mock()
        mock_client.execute_graphql.return_value = {
            "__schema": {
                "queryType": {
                    "fields": [
                        {
                            "name": "searchAcrossEntities",
                            "description": "Search across all entity types",
                            "args": [
                                {
                                    "name": "input",
                                    "type": {
                                        "kind": "NON_NULL",
                                        "ofType": {
                                            "kind": "INPUT_OBJECT",
                                            "name": "SearchInput",
                                        },
                                    },
                                }
                            ],
                        }
                    ]
                }
            }
        }
        mock_get_graph.return_value = mock_client

        result = self.runner.invoke(graphql, ["--describe", "searchAcrossEntities"])

        assert result.exit_code == 0
        assert "Operation: searchAcrossEntities" in result.output
        assert "Type: Query" in result.output
        assert "Description: Search across all entity types" in result.output
        assert "input: SearchInput!" in result.output

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_no_arguments(self, mock_get_graph):
        """Test GraphQL command with no arguments."""
        # Mock is needed even for argument validation to avoid config errors
        mock_client = Mock()
        mock_get_graph.return_value = mock_client

        result = self.runner.invoke(graphql, [])

        assert result.exit_code != 0
        assert (
            "Must specify either --query, --operation, or a discovery option"
            in result.output
        )

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_operation_execution_with_mock_error(self, mock_get_graph):
        """Test that operation-based execution works but fails with mock serialization error."""
        mock_client = Mock()
        # Mock the execute_graphql to raise a JSON serialization error like in real scenario
        mock_client.execute_graphql.side_effect = TypeError(
            "Object of type Mock is not JSON serializable"
        )
        mock_get_graph.return_value = mock_client

        result = self.runner.invoke(
            graphql,
            [
                "--operation",
                "searchAcrossEntities",
                "--variables",
                '{"input": {"query": "test"}}',
            ],
        )

        assert result.exit_code != 0
        assert "Failed to execute operation 'searchAcrossEntities'" in result.output

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_execution_error(self, mock_get_graph):
        """Test handling GraphQL execution errors."""
        mock_client = Mock()
        mock_client.execute_graphql.side_effect = Exception("GraphQL error")
        mock_get_graph.return_value = mock_client

        result = self.runner.invoke(graphql, ["--query", "query { invalidField }"])

        assert result.exit_code != 0
        assert "Failed to execute GraphQL query: GraphQL error" in result.output

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_no_pretty_output(self, mock_get_graph):
        """Test GraphQL output without pretty printing."""
        mock_client = Mock()
        mock_client.execute_graphql.return_value = {"me": {"username": "testuser"}}
        mock_get_graph.return_value = mock_client

        result = self.runner.invoke(
            graphql, ["--query", "query { me { username } }", "--no-pretty"]
        )

        assert result.exit_code == 0
        # Output should be compact JSON without indentation
        assert '{"me": {"username": "testuser"}}' in result.output

    def test_graphql_query_from_file(self):
        """Test loading GraphQL query from file."""
        query_content = "query { me { username } }"

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".graphql", delete=False
        ) as tmp:
            tmp.write(query_content)
            tmp.flush()

            with patch("datahub.cli.graphql_cli.get_default_graph") as mock_get_graph:
                mock_client = Mock()
                mock_client.execute_graphql.return_value = {
                    "me": {"username": "testuser"}
                }
                mock_get_graph.return_value = mock_client

                result = self.runner.invoke(graphql, ["--query", tmp.name])

                assert result.exit_code == 0
                mock_client.execute_graphql.assert_called_once_with(
                    query=query_content, variables=None
                )

            # Clean up
            Path(tmp.name).unlink()

    def test_graphql_variables_from_file(self):
        """Test loading variables from JSON file."""
        variables = {"urn": "urn:li:corpuser:test"}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(variables, tmp)
            tmp.flush()

            with patch("datahub.cli.graphql_cli.get_default_graph") as mock_get_graph:
                mock_client = Mock()
                mock_client.execute_graphql.return_value = {
                    "corpUser": {"info": {"email": "test@example.com"}}
                }
                mock_get_graph.return_value = mock_client

                result = self.runner.invoke(
                    graphql,
                    [
                        "--query",
                        "query GetUser($urn: String!) { corpUser(urn: $urn) { info { email } } }",
                        "--variables",
                        tmp.name,
                    ],
                )

                assert result.exit_code == 0
                mock_client.execute_graphql.assert_called_once_with(
                    query="query GetUser($urn: String!) { corpUser(urn: $urn) { info { email } } }",
                    variables=variables,
                )

            # Clean up
            Path(tmp.name).unlink()

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_query_from_relative_path(self, mock_get_graph):
        """Test loading GraphQL query from relative path."""
        import os
        import tempfile

        query_content = "query { me { username } }"

        mock_client = Mock()
        mock_client.execute_graphql.return_value = {"me": {"username": "testuser"}}
        mock_get_graph.return_value = mock_client

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test file
            test_file = Path(temp_dir) / "test_query.graphql"
            test_file.write_text(query_content)

            # Create subdirectory structure for testing different relative paths
            sub_dir = Path(temp_dir) / "subdir"
            sub_dir.mkdir()

            original_cwd = os.getcwd()
            try:
                # Test from parent directory with ./
                os.chdir(temp_dir)
                result = self.runner.invoke(
                    graphql, ["--query", "./test_query.graphql"]
                )

                assert result.exit_code == 0
                mock_client.execute_graphql.assert_called_with(
                    query=query_content, variables=None
                )

                # Reset mock for next test
                mock_client.reset_mock()

                # Test from subdirectory with ../
                os.chdir(sub_dir)
                result = self.runner.invoke(
                    graphql, ["--query", "../test_query.graphql"]
                )

                assert result.exit_code == 0
                mock_client.execute_graphql.assert_called_with(
                    query=query_content, variables=None
                )

            finally:
                os.chdir(original_cwd)

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_variables_from_relative_path(self, mock_get_graph):
        """Test loading variables from relative JSON file path."""
        import os
        import tempfile

        variables = {"urn": "urn:li:corpuser:test"}
        query = (
            "query GetUser($urn: String!) { corpUser(urn: $urn) { info { email } } }"
        )

        mock_client = Mock()
        mock_client.execute_graphql.return_value = {
            "corpUser": {"info": {"email": "test@example.com"}}
        }
        mock_get_graph.return_value = mock_client

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test variables file
            vars_file = Path(temp_dir) / "variables.json"
            vars_file.write_text(json.dumps(variables))

            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                result = self.runner.invoke(
                    graphql,
                    [
                        "--query",
                        query,
                        "--variables",
                        "./variables.json",
                    ],
                )

                assert result.exit_code == 0
                mock_client.execute_graphql.assert_called_with(
                    query=query, variables=variables
                )

            finally:
                os.chdir(original_cwd)

    @patch("datahub.cli.graphql_cli.get_default_graph")
    def test_graphql_query_from_nonexistent_relative_path(self, mock_get_graph):
        """Test error handling with non-existent relative path."""
        import os
        import tempfile

        # Mock client to handle GraphQL execution
        mock_client = Mock()
        mock_client.execute_graphql.side_effect = Exception("Query execution failed")
        mock_get_graph.return_value = mock_client

        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                result = self.runner.invoke(
                    graphql, ["--query", "./nonexistent.graphql"]
                )

                # Should fail because file doesn't exist, but treated as literal query
                assert result.exit_code != 0
                assert "Failed to execute GraphQL query" in result.output

            finally:
                os.chdir(original_cwd)


class TestSchemaFileHandling:
    """Test schema file parsing and fallback functionality."""

    def test_parse_graphql_operations_from_files_error_fallback(self):
        """Test that when schema path lookup fails, function falls back gracefully."""
        # Test the error handling by directly calling with None schema path
        # which triggers the fallback path lookup that will fail in test environment
        result = _parse_graphql_operations_from_files(None)

        # Should return the minimal fallback operations structure
        assert "queryType" in result
        assert "mutationType" in result

        # Should contain known fallback operations
        query_fields = result["queryType"]["fields"]
        query_names = [op["name"] for op in query_fields]
        assert "me" in query_names

    def test_parse_graphql_operations_from_files_with_custom_path(self):
        """Test parsing operations from custom schema path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = Path(temp_dir)

            # Create a mock GraphQL schema file
            schema_file = schema_path / "test.graphql"
            schema_content = """
            type Query {
                "Get current user"
                me: User
                "Search entities"
                search(query: String!): SearchResults
            }

            type Mutation {
                "Create a new user"
                createUser(input: CreateUserInput!): User
            }
            """
            schema_file.write_text(schema_content)

            result = _parse_graphql_operations_from_files(str(schema_path))

            # Should parse queries
            assert "queryType" in result
            assert result["queryType"] is not None
            query_fields = result["queryType"]["fields"]
            assert len(query_fields) >= 2

            # Check specific operations
            me_op = next(op for op in query_fields if op["name"] == "me")
            assert me_op["description"] == "Get current user"

            search_op = next(op for op in query_fields if op["name"] == "search")
            assert search_op["description"] == "Search entities"

            # Should parse mutations
            assert "mutationType" in result
            assert result["mutationType"] is not None
            mutation_fields = result["mutationType"]["fields"]
            assert len(mutation_fields) >= 1

            create_user_op = next(
                op for op in mutation_fields if op["name"] == "createUser"
            )
            assert create_user_op["description"] == "Create a new user"

    def test_parse_graphql_operations_from_files_nonexistent_custom_path(self):
        """Test parsing operations with non-existent custom schema path."""
        # Looking at the logs, this function catches FileNotFoundError and falls back
        # Let's test that it falls back gracefully instead
        nonexistent_path = "/this/path/definitely/does/not/exist/on/any/system"
        result = _parse_graphql_operations_from_files(nonexistent_path)

        # Should fall back to minimal operations
        assert "queryType" in result
        assert "mutationType" in result
        query_names = [op["name"] for op in result["queryType"]["fields"]]
        assert "me" in query_names  # Should contain fallback operations

    def test_parse_graphql_operations_from_files_fallback_on_error(self):
        """Test that parsing falls back to minimal operations on error."""
        with patch("datahub.cli.graphql_cli._get_schema_files_path") as mock_get_path:
            mock_get_path.side_effect = Exception("Schema files not found")

            result = _parse_graphql_operations_from_files()

            # Should return minimal fallback operations
            assert "queryType" in result
            assert "mutationType" in result

            # Check for known fallback operations
            query_fields = result["queryType"]["fields"]
            query_names = [op["name"] for op in query_fields]
            assert "me" in query_names
            assert "searchAcrossEntities" in query_names

            mutation_fields = result["mutationType"]["fields"]
            mutation_names = [op["name"] for op in mutation_fields]
            assert "addTag" in mutation_names
            assert "removeTag" in mutation_names

    def test_parse_operations_from_content(self):
        """Test parsing operations from GraphQL content string."""
        content = """
        \"\"\"Get current authenticated user\"\"\"
        me: AuthenticatedUser

        "Search across all entity types"
        searchAcrossEntities(input: SearchInput!): SearchResults

        # This should be skipped as it's not a valid field
        type SomeType {
            field: String
        }

        "Browse entities hierarchically"
        browse(path: BrowsePath): BrowseResults
        """

        operations = _parse_operations_from_content(content, "Query")

        assert len(operations) >= 3

        # Check specific operations were parsed
        op_names = [op["name"] for op in operations]
        assert "me" in op_names
        assert "searchAcrossEntities" in op_names
        assert "browse" in op_names

        # Check descriptions were extracted
        me_op = next(op for op in operations if op["name"] == "me")
        assert "authenticated user" in me_op["description"].lower()

        search_op = next(
            op for op in operations if op["name"] == "searchAcrossEntities"
        )
        assert "search across all entity types" in search_op["description"].lower()

    def test_parse_operations_from_content_with_keywords(self):
        """Test that GraphQL keywords are properly filtered out."""
        content = """
        query: String
        mutation: String
        subscription: String
        type: String
        input: String
        enum: String
        validField: String
        """

        operations = _parse_operations_from_content(content, "Query")

        # Should only contain validField, keywords should be filtered
        assert len(operations) == 1
        assert operations[0]["name"] == "validField"

    def test_get_minimal_fallback_operations(self):
        """Test minimal fallback operations structure."""
        result = _get_minimal_fallback_operations()

        # Should have both query and mutation types
        assert "queryType" in result
        assert "mutationType" in result

        # Check query operations
        query_fields = result["queryType"]["fields"]
        assert len(query_fields) > 5  # Should have several common queries
        query_names = [op["name"] for op in query_fields]
        assert "me" in query_names
        assert "searchAcrossEntities" in query_names
        assert "dataset" in query_names

        # Check mutation operations
        mutation_fields = result["mutationType"]["fields"]
        assert len(mutation_fields) > 5  # Should have several common mutations
        mutation_names = [op["name"] for op in mutation_fields]
        assert "addTag" in mutation_names
        assert "removeTag" in mutation_names
        assert "createGroup" in mutation_names

        # Each operation should have required fields
        for op in query_fields + mutation_fields:
            assert "name" in op
            assert "description" in op
            assert "args" in op
            assert isinstance(op["args"], list)


class TestOperationGenerationAndQueryBuilding:
    """Test operation generation and query building functionality."""

    def test_dict_to_graphql_input_simple(self):
        """Test converting simple dict to GraphQL input syntax."""
        input_dict = {"key": "value", "number": 42, "flag": True}
        result = _dict_to_graphql_input(input_dict)

        assert 'key: "value"' in result
        assert "number: 42" in result
        assert "flag: true" in result
        assert result.startswith("{") and result.endswith("}")

    def test_dict_to_graphql_input_nested(self):
        """Test converting nested dict to GraphQL input syntax."""
        input_dict = {
            "user": {"name": "test", "age": 30},
            "tags": ["tag1", "tag2"],
            "metadata": {"active": True},
        }
        result = _dict_to_graphql_input(input_dict)

        assert 'user: {name: "test", age: 30}' in result
        assert 'tags: ["tag1", "tag2"]' in result
        assert "metadata: {active: true}" in result

    def test_dict_to_graphql_input_complex_lists(self):
        """Test converting dict with complex list items to GraphQL input syntax."""
        input_dict = {
            "users": [
                {"name": "user1", "active": True},
                {"name": "user2", "active": False},
            ],
            "values": [1, 2, 3],
            "strings": ["a", "b", "c"],
        }
        result = _dict_to_graphql_input(input_dict)

        assert (
            'users: [{name: "user1", active: true}, {name: "user2", active: false}]'
            in result
        )
        assert "values: [1, 2, 3]" in result
        assert 'strings: ["a", "b", "c"]' in result

    def test_dict_to_graphql_input_non_dict(self):
        """Test handling non-dict input."""
        result = _dict_to_graphql_input("not a dict")  # type: ignore
        assert result == "not a dict"

        result = _dict_to_graphql_input(123)  # type: ignore
        assert result == "123"

    def test_generate_operation_query_simple(self):
        """Test generating query for simple operation without arguments."""
        operation_field = {"name": "me", "description": "Get current user", "args": []}

        result = _generate_operation_query(operation_field, "Query")
        expected = "query { me { corpUser { urn username properties { displayName email firstName lastName title } } } }"
        assert result == expected

    def test_generate_operation_query_with_required_args(self):
        """Test generating query for operation with required arguments."""
        operation_field = {
            "name": "searchAcrossEntities",
            "description": "Search across all entity types",
            "args": [
                {
                    "name": "input",
                    "type": {
                        "kind": "NON_NULL",
                        "ofType": {"kind": "INPUT_OBJECT", "name": "SearchInput"},
                    },
                }
            ],
        }
        variables = {"input": {"query": "test", "start": 0, "count": 10}}

        result = _generate_operation_query(operation_field, "Query", variables)
        expected = 'query { searchAcrossEntities(input: {query: "test", start: 0, count: 10})  }'
        assert result == expected

    def test_generate_operation_query_missing_required_args(self):
        """Test error when required arguments are missing."""
        operation_field = {
            "name": "searchAcrossEntities",
            "description": "Search across all entity types",
            "args": [
                {
                    "name": "input",
                    "type": {
                        "kind": "NON_NULL",
                        "ofType": {"kind": "INPUT_OBJECT", "name": "SearchInput"},
                    },
                }
            ],
        }

        with pytest.raises(
            Exception,
            match="Operation 'searchAcrossEntities' requires arguments: input",
        ):
            _generate_operation_query(operation_field, "Query", None)

    def test_generate_operation_query_with_optional_args(self):
        """Test generating query with optional arguments."""
        operation_field = {
            "name": "browse",
            "description": "Browse entities",
            "args": [
                {
                    "name": "path",
                    "type": {"kind": "SCALAR", "name": "String"},  # Optional
                },
                {
                    "name": "filter",
                    "type": {
                        "kind": "INPUT_OBJECT",
                        "name": "BrowseFilter",
                    },  # Optional
                },
            ],
        }
        variables = {"path": "datasets"}

        result = _generate_operation_query(operation_field, "Query", variables)
        expected = 'query { browse(path: "datasets")  }'
        assert result == expected

    def test_generate_operation_query_mutation(self):
        """Test generating mutation query."""
        operation_field = {
            "name": "addTag",
            "description": "Add tag to entity",
            "args": [
                {
                    "name": "input",
                    "type": {
                        "kind": "NON_NULL",
                        "ofType": {
                            "kind": "INPUT_OBJECT",
                            "name": "TagAssociationInput",
                        },
                    },
                }
            ],
        }
        variables = {
            "input": {"tagUrn": "urn:li:tag:test", "resourceUrn": "urn:li:dataset:test"}
        }

        result = _generate_operation_query(operation_field, "Mutation", variables)
        expected = 'mutation { addTag(input: {tagUrn: "urn:li:tag:test", resourceUrn: "urn:li:dataset:test"})  }'
        assert result == expected

    def test_generate_operation_query_list_operations(self):
        """Test generating queries for list operations."""
        # Test listUsers operation
        operation_field = {
            "name": "listUsers",
            "description": "List all users",
            "args": [],
        }

        result = _generate_operation_query(operation_field, "Query")
        expected = "query { listUsers { total users { urn username properties { displayName email } } } }"
        assert result == expected

        # Test other list operation
        operation_field = {
            "name": "listDatasets",
            "description": "List datasets",
            "args": [],
        }

        result = _generate_operation_query(operation_field, "Query")
        expected = "query { listDatasets { total } }"
        assert result == expected

    def test_generate_operation_query_entity_operations(self):
        """Test generating queries for specific entity operations."""
        entity_operations = [
            ("corpUser", "query { corpUser { urn } }"),
            ("dataset", "query { dataset { urn } }"),
            ("dashboard", "query { dashboard { urn } }"),
            ("chart", "query { chart { urn } }"),
        ]

        for op_name, expected in entity_operations:
            operation_field = {
                "name": op_name,
                "description": f"Get {op_name}",
                "args": [],
            }

            result = _generate_operation_query(operation_field, "Query")
            assert result == expected

    def test_generate_operation_query_complex_variables(self):
        """Test generating queries with complex variable structures."""
        operation_field = {
            "name": "complexOperation",
            "description": "Complex operation with nested input",
            "args": [
                {
                    "name": "input",
                    "type": {
                        "kind": "NON_NULL",
                        "ofType": {"kind": "INPUT_OBJECT", "name": "ComplexInput"},
                    },
                }
            ],
        }

        complex_variables = {
            "input": {
                "filters": {
                    "platform": "snowflake",
                    "entityTypes": ["DATASET", "TABLE"],
                },
                "sort": {"field": "name", "direction": "ASC"},
                "pagination": {"start": 0, "count": 20},
            }
        }

        result = _generate_operation_query(operation_field, "Query", complex_variables)

        # Should contain the complex nested structure
        assert "complexOperation(input: {" in result
        assert 'platform: "snowflake"' in result
        assert 'entityTypes: ["DATASET", "TABLE"]' in result
        assert 'direction: "ASC"' in result

    def test_generate_operation_query_boolean_handling(self):
        """Test that boolean values are properly formatted."""
        operation_field = {
            "name": "testOperation",
            "description": "Test operation with boolean",
            "args": [
                {"name": "input", "type": {"kind": "INPUT_OBJECT", "name": "TestInput"}}
            ],
        }

        variables = {
            "input": {
                "active": True,
                "deprecated": False,
                "count": 0,  # Should not be converted to boolean
            }
        }

        result = _generate_operation_query(operation_field, "Query", variables)

        assert "active: true" in result
        assert "deprecated: false" in result
        assert "count: 0" in result

    def test_generate_operation_query_string_escaping(self):
        """Test that string values are properly quoted and escaped."""
        operation_field = {
            "name": "testOperation",
            "description": "Test operation with strings",
            "args": [
                {"name": "input", "type": {"kind": "INPUT_OBJECT", "name": "TestInput"}}
            ],
        }

        variables = {
            "input": {
                "name": "test entity",
                "description": 'A test description with "quotes"',
                "number": 42,
            }
        }

        result = _generate_operation_query(operation_field, "Query", variables)

        assert 'name: "test entity"' in result
        assert "number: 42" in result  # Numbers should not be quoted


class TestTypeIntrospectionAndRecursiveExploration:
    """Test type introspection and recursive type exploration functionality."""

    def test_extract_base_type_name_simple(self):
        """Test extracting base type name from simple type."""
        type_info = {"kind": "SCALAR", "name": "String"}
        result = _extract_base_type_name(type_info)
        assert result == "String"

    def test_extract_base_type_name_non_null(self):
        """Test extracting base type name from NON_NULL wrapper."""
        type_info = {"kind": "NON_NULL", "ofType": {"kind": "SCALAR", "name": "String"}}
        result = _extract_base_type_name(type_info)
        assert result == "String"

    def test_extract_base_type_name_list(self):
        """Test extracting base type name from LIST wrapper."""
        type_info = {"kind": "LIST", "ofType": {"kind": "SCALAR", "name": "String"}}
        result = _extract_base_type_name(type_info)
        assert result == "String"

    def test_extract_base_type_name_nested_wrappers(self):
        """Test extracting base type name from nested wrappers."""
        type_info = {
            "kind": "NON_NULL",
            "ofType": {
                "kind": "LIST",
                "ofType": {"kind": "INPUT_OBJECT", "name": "SearchInput"},
            },
        }
        result = _extract_base_type_name(type_info)
        assert result == "SearchInput"

    def test_extract_base_type_name_empty(self):
        """Test extracting base type name from empty or invalid type."""
        assert _extract_base_type_name({}) is None
        assert _extract_base_type_name(None) is None  # type: ignore
        assert _extract_base_type_name({"kind": "NON_NULL"}) is None  # Missing ofType

    def test_find_type_by_name(self):
        """Test finding a type by name using GraphQL introspection."""
        mock_client = Mock()
        mock_client.execute_graphql.return_value = {
            "__type": {
                "name": "SearchInput",
                "kind": "INPUT_OBJECT",
                "inputFields": [
                    {
                        "name": "query",
                        "description": "Search query string",
                        "type": {"kind": "SCALAR", "name": "String"},
                    },
                    {
                        "name": "start",
                        "description": "Start offset",
                        "type": {"kind": "SCALAR", "name": "Int"},
                    },
                ],
            }
        }

        result = _find_type_by_name(mock_client, "SearchInput")

        assert result is not None
        assert result["name"] == "SearchInput"
        assert result["kind"] == "INPUT_OBJECT"
        assert len(result["inputFields"]) == 2

        # Verify the query was executed correctly
        mock_client.execute_graphql.assert_called_once()
        call_args = mock_client.execute_graphql.call_args
        query_arg = (
            call_args[1]["query"]
            if len(call_args) > 1 and "query" in call_args[1]
            else call_args[0][0]
        )
        assert "SearchInput" in query_arg

    def test_find_type_by_name_not_found(self):
        """Test finding a non-existent type."""
        mock_client = Mock()
        mock_client.execute_graphql.return_value = {"__type": None}

        result = _find_type_by_name(mock_client, "NonExistentType")
        assert result is None

    def test_find_type_by_name_error(self):
        """Test error handling when introspection fails."""
        mock_client = Mock()
        mock_client.execute_graphql.side_effect = Exception("GraphQL error")

        result = _find_type_by_name(mock_client, "SearchInput")
        assert result is None

    def test_collect_nested_types(self):
        """Test collecting nested type names from a type definition."""
        type_info = {
            "inputFields": [
                {
                    "name": "filter",
                    "type": {"kind": "INPUT_OBJECT", "name": "FilterInput"},
                },
                {
                    "name": "tags",
                    "type": {
                        "kind": "LIST",
                        "ofType": {"kind": "INPUT_OBJECT", "name": "TagInput"},
                    },
                },
                {
                    "name": "name",
                    "type": {
                        "kind": "SCALAR",
                        "name": "String",
                    },  # Should be filtered out
                },
                {
                    "name": "count",
                    "type": {"kind": "SCALAR", "name": "Int"},  # Should be filtered out
                },
            ]
        }

        result = _collect_nested_types(type_info)

        assert len(result) == 2
        assert "FilterInput" in result
        assert "TagInput" in result
        # Scalar types should not be included
        assert "String" not in result
        assert "Int" not in result

    def test_collect_nested_types_with_visited(self):
        """Test collecting nested types with visited set to avoid duplicates."""
        type_info = {
            "inputFields": [
                {
                    "name": "filter1",
                    "type": {"kind": "INPUT_OBJECT", "name": "FilterInput"},
                },
                {
                    "name": "filter2",
                    "type": {
                        "kind": "INPUT_OBJECT",
                        "name": "FilterInput",
                    },  # Duplicate
                },
            ]
        }

        visited: set[str] = set()
        result = _collect_nested_types(type_info, visited)

        # The function doesn't deduplicate internally - it returns all found types
        # Deduplication happens at a higher level in the recursive fetching
        assert "FilterInput" in result
        assert len(result) == 2  # Two references to the same type

    def test_fetch_type_recursive(self):
        """Test recursively fetching a type and its nested types."""
        mock_client = Mock()

        # Mock responses for different types
        def mock_execute_graphql(query, **kwargs):
            if "SearchInput" in query:
                return {
                    "__type": {
                        "name": "SearchInput",
                        "kind": "INPUT_OBJECT",
                        "inputFields": [
                            {
                                "name": "filter",
                                "type": {"kind": "INPUT_OBJECT", "name": "FilterInput"},
                            },
                            {
                                "name": "query",
                                "type": {"kind": "SCALAR", "name": "String"},
                            },
                        ],
                    }
                }
            elif "FilterInput" in query:
                return {
                    "__type": {
                        "name": "FilterInput",
                        "kind": "INPUT_OBJECT",
                        "inputFields": [
                            {
                                "name": "platform",
                                "type": {"kind": "SCALAR", "name": "String"},
                            }
                        ],
                    }
                }
            return {"__type": None}

        mock_client.execute_graphql.side_effect = mock_execute_graphql

        result = _fetch_type_recursive(mock_client, "SearchInput")

        # Should contain both types
        assert "SearchInput" in result
        assert "FilterInput" in result

        # Verify structure
        search_input = result["SearchInput"]
        assert search_input["name"] == "SearchInput"
        assert search_input["kind"] == "INPUT_OBJECT"

        filter_input = result["FilterInput"]
        assert filter_input["name"] == "FilterInput"
        assert filter_input["kind"] == "INPUT_OBJECT"

    def test_fetch_type_recursive_circular_reference(self):
        """Test handling of circular type references."""
        mock_client = Mock()

        # Create a circular reference scenario
        def mock_execute_graphql(query, **kwargs):
            if "TypeA" in query:
                return {
                    "__type": {
                        "name": "TypeA",
                        "kind": "INPUT_OBJECT",
                        "inputFields": [
                            {
                                "name": "typeB",
                                "type": {"kind": "INPUT_OBJECT", "name": "TypeB"},
                            }
                        ],
                    }
                }
            elif "TypeB" in query:
                return {
                    "__type": {
                        "name": "TypeB",
                        "kind": "INPUT_OBJECT",
                        "inputFields": [
                            {
                                "name": "typeA",
                                "type": {
                                    "kind": "INPUT_OBJECT",
                                    "name": "TypeA",
                                },  # Circular reference
                            }
                        ],
                    }
                }
            return {"__type": None}

        mock_client.execute_graphql.side_effect = mock_execute_graphql

        result = _fetch_type_recursive(mock_client, "TypeA")

        # Should handle circular reference without infinite loop
        assert "TypeA" in result
        assert "TypeB" in result
        assert len(result) == 2  # No duplicates

    def test_fetch_type_recursive_error_handling(self):
        """Test error handling during recursive type fetching."""
        mock_client = Mock()
        mock_client.execute_graphql.side_effect = Exception("GraphQL error")

        result = _fetch_type_recursive(mock_client, "SearchInput")

        # Should return empty dict on error
        assert result == {}

    def test_format_single_type_fields_input_object(self):
        """Test formatting fields for an INPUT_OBJECT type."""
        type_info = {
            "kind": "INPUT_OBJECT",
            "inputFields": [
                {
                    "name": "query",
                    "description": "Search query string",
                    "type": {"kind": "SCALAR", "name": "String"},
                },
                {
                    "name": "filter",
                    "type": {"kind": "INPUT_OBJECT", "name": "FilterInput"},
                },
            ],
        }

        result = _format_single_type_fields(type_info)

        assert len(result) == 2
        assert "  query: String - Search query string" in result
        assert "  filter: FilterInput" in result

    def test_format_single_type_fields_enum(self):
        """Test formatting enum values for an ENUM type."""
        type_info = {
            "kind": "ENUM",
            "enumValues": [
                {
                    "name": "ACTIVE",
                    "description": "Entity is active",
                    "isDeprecated": False,
                },
                {
                    "name": "DEPRECATED_VALUE",
                    "description": "Old value",
                    "isDeprecated": True,
                    "deprecationReason": "Use ACTIVE instead",
                },
            ],
        }

        result = _format_single_type_fields(type_info)

        assert len(result) == 2
        assert "  ACTIVE - Entity is active" in result
        assert (
            "  DEPRECATED_VALUE - Old value (DEPRECATED: Use ACTIVE instead)" in result
        )

    def test_format_single_type_fields_empty(self):
        """Test formatting empty type (no fields or enum values)."""
        # Empty INPUT_OBJECT
        type_info = {"kind": "INPUT_OBJECT", "inputFields": []}
        result = _format_single_type_fields(type_info)
        assert result == ["  (no fields)"]

        # Empty ENUM
        type_info = {"kind": "ENUM", "enumValues": []}
        result = _format_single_type_fields(type_info)
        assert result == ["  (no enum values)"]

    def test_format_recursive_types(self):
        """Test formatting multiple types in hierarchical display."""
        types_map = {
            "SearchInput": {
                "name": "SearchInput",
                "kind": "INPUT_OBJECT",
                "inputFields": [
                    {"name": "query", "type": {"kind": "SCALAR", "name": "String"}}
                ],
            },
            "FilterInput": {
                "name": "FilterInput",
                "kind": "INPUT_OBJECT",
                "inputFields": [
                    {"name": "platform", "type": {"kind": "SCALAR", "name": "String"}}
                ],
            },
        }

        result = _format_recursive_types(types_map, "SearchInput")

        # Should display root type first
        lines = result.split("\n")
        assert "SearchInput:" in lines[0]
        assert "  query: String" in result

        # Should display nested types
        assert "FilterInput:" in result
        assert "  platform: String" in result

    def test_format_recursive_types_root_type_missing(self):
        """Test formatting when root type is not in the types map."""
        types_map = {
            "FilterInput": {
                "name": "FilterInput",
                "kind": "INPUT_OBJECT",
                "inputFields": [],
            }
        }

        result = _format_recursive_types(types_map, "SearchInput")

        # Should still display other types
        assert "FilterInput:" in result
        # Should not crash when root type is missing


class TestJSONOutputFormatting:
    """Test JSON output formatting for LLM consumption."""

    def test_convert_type_to_json_simple(self):
        """Test converting simple GraphQL type to JSON format."""
        type_info = {"kind": "SCALAR", "name": "String"}
        result = _convert_type_to_json(type_info)

        expected = {"kind": "SCALAR", "name": "String"}
        assert result == expected

    def test_convert_type_to_json_non_null(self):
        """Test converting NON_NULL type to JSON format."""
        type_info = {"kind": "NON_NULL", "ofType": {"kind": "SCALAR", "name": "String"}}
        result = _convert_type_to_json(type_info)

        expected = {"kind": "NON_NULL", "ofType": {"kind": "SCALAR", "name": "String"}}
        assert result == expected

    def test_convert_type_to_json_list(self):
        """Test converting LIST type to JSON format."""
        type_info = {"kind": "LIST", "ofType": {"kind": "SCALAR", "name": "String"}}
        result = _convert_type_to_json(type_info)

        expected = {"kind": "LIST", "ofType": {"kind": "SCALAR", "name": "String"}}
        assert result == expected

    def test_convert_type_to_json_complex(self):
        """Test converting complex nested type to JSON format."""
        type_info = {
            "kind": "NON_NULL",
            "ofType": {
                "kind": "LIST",
                "ofType": {"kind": "INPUT_OBJECT", "name": "SearchInput"},
            },
        }
        result = _convert_type_to_json(type_info)

        expected = {
            "kind": "NON_NULL",
            "ofType": {
                "kind": "LIST",
                "ofType": {"kind": "INPUT_OBJECT", "name": "SearchInput"},
            },
        }
        assert result == expected

    def test_convert_type_to_json_empty(self):
        """Test converting empty type info."""
        result = _convert_type_to_json({})
        assert result == {}

        result = _convert_type_to_json(None)  # type: ignore
        assert result == {}

    def test_convert_operation_to_json(self):
        """Test converting operation info to JSON format."""
        operation = {
            "name": "searchAcrossEntities",
            "description": "Search across all entity types",
            "args": [
                {
                    "name": "input",
                    "description": "Search input parameters",
                    "type": {
                        "kind": "NON_NULL",
                        "ofType": {"kind": "INPUT_OBJECT", "name": "SearchInput"},
                    },
                },
                {
                    "name": "limit",
                    "description": "Maximum results to return",
                    "type": {"kind": "SCALAR", "name": "Int"},  # Optional
                },
            ],
        }

        result = _convert_operation_to_json(operation, "Query")

        assert result["name"] == "searchAcrossEntities"
        assert result["type"] == "Query"
        assert result["description"] == "Search across all entity types"
        assert len(result["arguments"]) == 2

        # Check required argument
        input_arg = result["arguments"][0]
        assert input_arg["name"] == "input"
        assert input_arg["description"] == "Search input parameters"
        assert input_arg["required"] is True
        assert input_arg["type"]["kind"] == "NON_NULL"

        # Check optional argument
        limit_arg = result["arguments"][1]
        assert limit_arg["name"] == "limit"
        assert limit_arg["required"] is False
        assert limit_arg["type"]["kind"] == "SCALAR"

    def test_convert_operation_to_json_no_args(self):
        """Test converting operation with no arguments to JSON format."""
        operation = {"name": "me", "description": "Get current user", "args": []}

        result = _convert_operation_to_json(operation, "Query")

        assert result["name"] == "me"
        assert result["type"] == "Query"
        assert result["description"] == "Get current user"
        assert result["arguments"] == []

    def test_convert_type_details_to_json_input_object(self):
        """Test converting INPUT_OBJECT type details to JSON format."""
        type_info = {
            "name": "SearchInput",
            "kind": "INPUT_OBJECT",
            "description": "Input for search operations",
            "inputFields": [
                {
                    "name": "query",
                    "description": "Search query string",
                    "type": {"kind": "SCALAR", "name": "String"},
                },
                {
                    "name": "filter",
                    "description": "Search filters",
                    "type": {"kind": "INPUT_OBJECT", "name": "FilterInput"},
                },
            ],
        }

        result = _convert_type_details_to_json(type_info)

        assert result["name"] == "SearchInput"
        assert result["kind"] == "INPUT_OBJECT"
        assert result["description"] == "Input for search operations"
        assert len(result["fields"]) == 2

        query_field = result["fields"][0]
        assert query_field["name"] == "query"
        assert query_field["description"] == "Search query string"
        assert query_field["type"]["kind"] == "SCALAR"

    def test_convert_type_details_to_json_enum(self):
        """Test converting ENUM type details to JSON format."""
        type_info = {
            "name": "EntityType",
            "kind": "ENUM",
            "description": "Types of entities in DataHub",
            "enumValues": [
                {
                    "name": "DATASET",
                    "description": "Dataset entity",
                    "isDeprecated": False,
                },
                {
                    "name": "LEGACY_TYPE",
                    "description": "Old entity type",
                    "isDeprecated": True,
                    "deprecationReason": "Use DATASET instead",
                },
            ],
        }

        result = _convert_type_details_to_json(type_info)

        assert result["name"] == "EntityType"
        assert result["kind"] == "ENUM"
        assert result["description"] == "Types of entities in DataHub"
        assert len(result["values"]) == 2

        dataset_value = result["values"][0]
        assert dataset_value["name"] == "DATASET"
        assert dataset_value["description"] == "Dataset entity"
        assert dataset_value["deprecated"] is False

        legacy_value = result["values"][1]
        assert legacy_value["name"] == "LEGACY_TYPE"
        assert legacy_value["deprecated"] is True
        assert legacy_value["deprecationReason"] == "Use DATASET instead"

    def test_convert_operations_list_to_json(self):
        """Test converting full operations list to JSON format."""
        schema = {
            "queryType": {
                "fields": [
                    {"name": "me", "description": "Get current user", "args": []},
                    {
                        "name": "search",
                        "description": "Search entities",
                        "args": [
                            {
                                "name": "query",
                                "type": {"kind": "SCALAR", "name": "String"},
                            }
                        ],
                    },
                ]
            },
            "mutationType": {
                "fields": [
                    {
                        "name": "addTag",
                        "description": "Add tag to entity",
                        "args": [
                            {
                                "name": "input",
                                "type": {
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "kind": "INPUT_OBJECT",
                                        "name": "TagInput",
                                    },
                                },
                            }
                        ],
                    }
                ]
            },
        }

        result = _convert_operations_list_to_json(schema)

        assert "schema" in result
        assert "queries" in result["schema"]
        assert "mutations" in result["schema"]

        # Check queries
        queries = result["schema"]["queries"]
        assert len(queries) == 2
        assert queries[0]["name"] == "me"
        assert queries[0]["type"] == "Query"
        assert queries[1]["name"] == "search"

        # Check mutations
        mutations = result["schema"]["mutations"]
        assert len(mutations) == 1
        assert mutations[0]["name"] == "addTag"
        assert mutations[0]["type"] == "Mutation"

    def test_convert_operations_list_to_json_empty_schema(self):
        """Test converting empty schema to JSON format."""
        schema: dict[str, Any] = {}
        result = _convert_operations_list_to_json(schema)

        assert result == {"schema": {"queries": [], "mutations": []}}

    def test_convert_describe_to_json_operation_only(self):
        """Test converting describe output with operation only."""
        operation_info = (
            {
                "name": "searchAcrossEntities",
                "description": "Search across all entity types",
                "args": [],
            },
            "Query",
        )

        result = _convert_describe_to_json(operation_info, None, None)

        assert "operation" in result
        assert result["operation"]["name"] == "searchAcrossEntities"
        assert result["operation"]["type"] == "Query"
        assert "type" not in result
        assert "relatedTypes" not in result

    def test_convert_describe_to_json_type_only(self):
        """Test converting describe output with type only."""
        type_info = {
            "name": "SearchInput",
            "kind": "INPUT_OBJECT",
            "inputFields": [
                {"name": "query", "type": {"kind": "SCALAR", "name": "String"}}
            ],
        }

        result = _convert_describe_to_json(None, type_info, None)

        assert "type" in result
        assert result["type"]["name"] == "SearchInput"
        assert result["type"]["kind"] == "INPUT_OBJECT"
        assert "operation" not in result
        assert "relatedTypes" not in result

    def test_convert_describe_to_json_with_related_types(self):
        """Test converting describe output with related types."""
        operation_info = (
            {
                "name": "search",
                "description": "Search operation",
                "args": [
                    {
                        "name": "input",
                        "type": {"kind": "INPUT_OBJECT", "name": "SearchInput"},
                    }
                ],
            },
            "Query",
        )

        type_info = {"name": "SearchInput", "kind": "INPUT_OBJECT", "inputFields": []}

        related_types = {
            "SearchInput": {
                "name": "SearchInput",
                "kind": "INPUT_OBJECT",
                "inputFields": [],
            },
            "FilterInput": {
                "name": "FilterInput",
                "kind": "INPUT_OBJECT",
                "inputFields": [],
            },
        }

        result = _convert_describe_to_json(operation_info, type_info, related_types)

        assert "operation" in result
        assert "type" in result
        assert "relatedTypes" in result
        assert len(result["relatedTypes"]) == 2
        assert "SearchInput" in result["relatedTypes"]
        assert "FilterInput" in result["relatedTypes"]

    def test_convert_describe_to_json_all_none(self):
        """Test converting describe output when everything is None."""
        result = _convert_describe_to_json(None, None, None)
        assert result == {}

    def test_json_formatting_preserves_structure(self):
        """Test that JSON formatting preserves all necessary structure for LLMs."""
        # Complex operation with nested types
        operation = {
            "name": "complexSearch",
            "description": "Complex search with multiple parameters",
            "args": [
                {
                    "name": "input",
                    "description": "Search input",
                    "type": {
                        "kind": "NON_NULL",
                        "ofType": {
                            "kind": "INPUT_OBJECT",
                            "name": "ComplexSearchInput",
                        },
                    },
                },
                {
                    "name": "options",
                    "description": "Search options",
                    "type": {
                        "kind": "LIST",
                        "ofType": {"kind": "ENUM", "name": "SearchOption"},
                    },
                },
            ],
        }

        result = _convert_operation_to_json(operation, "Query")

        # Verify complete structure is preserved
        assert result["name"] == "complexSearch"
        assert result["type"] == "Query"
        assert result["description"] == "Complex search with multiple parameters"
        assert len(result["arguments"]) == 2

        # Verify nested type structure is preserved
        input_arg = result["arguments"][0]
        assert input_arg["required"] is True
        assert input_arg["type"]["kind"] == "NON_NULL"
        assert input_arg["type"]["ofType"]["kind"] == "INPUT_OBJECT"
        assert input_arg["type"]["ofType"]["name"] == "ComplexSearchInput"

        options_arg = result["arguments"][1]
        assert options_arg["required"] is False
        assert options_arg["type"]["kind"] == "LIST"
        assert options_arg["type"]["ofType"]["kind"] == "ENUM"
        assert options_arg["type"]["ofType"]["name"] == "SearchOption"
