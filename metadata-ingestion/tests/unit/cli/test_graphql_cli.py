import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from datahub.cli.graphql_cli import (
    _find_operation_by_name,
    _format_graphql_type,
    _format_operation_details,
    _format_operation_list,
    _is_file_path,
    _load_content_or_file,
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
