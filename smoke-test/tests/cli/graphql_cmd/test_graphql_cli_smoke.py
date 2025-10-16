"""
Smoke tests for DataHub GraphQL CLI functionality.

These tests validate the core GraphQL CLI features including:
- Schema discovery and introspection
- File-based query execution with relative paths
- JSON output formatting
- CLI integration with DataHub instances
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Optional

import pytest
import requests

from tests.utils import run_datahub_cmd, wait_for_healthcheck_util


class TestGraphQLCLIStandalone:
    """Fast standalone tests that don't require full DataHub functionality."""

    def setup_method(self):
        """Set up test environment variables."""
        self.original_env = os.environ.copy()
        # Ensure we have the required DataHub connection info
        os.environ.setdefault("DATAHUB_GMS_HOST", "http://localhost:8080")
        os.environ.setdefault("DATAHUB_GMS_TOKEN", "")

    def teardown_method(self):
        """Restore original environment."""
        os.environ.clear()
        os.environ.update(self.original_env)

    def _run_datahub_cli(
        self, args: list[str], input_data: Optional[str] = None
    ) -> tuple[int, str, str]:
        """
        Run datahub CLI command and return (exit_code, stdout, stderr).

        Args:
            args: CLI arguments (e.g., ['graphql', '--schema'])
            input_data: Optional stdin input

        Returns:
            Tuple of (exit_code, stdout, stderr)
        """
        result = run_datahub_cmd(args, input=input_data)
        return result.exit_code, result.stdout, result.stderr

    def test_graphql_help(self):
        """Test that GraphQL CLI help is accessible."""
        exit_code, stdout, stderr = self._run_datahub_cli(["graphql", "--help"])

        assert exit_code == 0, f"CLI help failed with stderr: {stderr}"
        assert "GraphQL" in stdout or "graphql" in stdout
        assert "--list-operations" in stdout or "--schema-path" in stdout
        assert "--query" in stdout

    def test_graphql_schema_discovery(self):
        """Test GraphQL schema discovery functionality."""
        # This should work even without authentication for schema discovery
        exit_code, stdout, stderr = self._run_datahub_cli(
            ["graphql", "--list-operations"]
        )

        # Command may exit with error (no DataHub connection) but should not crash
        assert exit_code in [0, 1], f"Unexpected exit code. stderr: {stderr}"
        assert "Traceback" not in stderr  # No Python crashes
        assert (
            "graphql" not in stderr.lower() or "command not found" not in stderr.lower()
        )

    def test_graphql_file_path_handling(self):
        """Test that GraphQL CLI properly handles file path arguments."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".graphql", delete=False
        ) as f:
            f.write("{ __typename }")
            temp_path = f.name

        try:
            # Test with absolute path
            exit_code, stdout, stderr = self._run_datahub_cli(
                ["graphql", "--query", temp_path, "--format", "json"]
            )

            # Should recognize as file path (may fail due to missing DataHub connection)
            assert exit_code in [0, 1], (
                f"Unexpected exit code with file path. stderr: {stderr}"
            )
            assert "No such file or directory" not in stderr
            assert "FileNotFoundError" not in stderr

        finally:
            os.unlink(temp_path)

    def test_graphql_relative_path_handling(self):
        """Test that GraphQL CLI handles relative paths correctly."""
        # Create a temporary GraphQL file in a subdirectory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            query_file = temp_path / "test.graphql"
            query_file.write_text("{ __typename }")

            # Change to parent directory and use relative path
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_path.parent)
                relative_path = os.path.relpath(str(query_file))

                exit_code, stdout, stderr = self._run_datahub_cli(
                    ["graphql", "--query", relative_path, "--format", "json"]
                )

                # Should recognize relative path (may fail due to missing DataHub connection)
                assert exit_code in [0, 1], (
                    f"Relative path handling failed. stderr: {stderr}"
                )
                assert "No such file or directory" not in stderr
                assert "FileNotFoundError" not in stderr

            finally:
                os.chdir(original_cwd)


class TestGraphQLCLIIntegration:
    """Integration tests requiring full DataHub functionality."""

    @pytest.fixture(autouse=True)
    def setup_datahub(self, auth_session):
        """Ensure DataHub is running and accessible."""
        self.auth_session = auth_session
        wait_for_healthcheck_util(requests)

    def _run_authenticated_graphql(self, args: list[str]) -> tuple[int, str, str]:
        """Run GraphQL CLI with proper authentication."""
        gms_url = self.auth_session.gms_url()
        gms_token = self.auth_session.gms_token()
        token_preview = f"{gms_token[:20]}..." if gms_token else "None"

        print(
            f"[DEBUG TEST] Running GraphQL command with: "
            f"GMS_URL={gms_url}, TOKEN={token_preview}"
        )

        result = run_datahub_cmd(
            args,
            env={
                "DATAHUB_GMS_URL": gms_url,
                "DATAHUB_GMS_TOKEN": gms_token,
            },
        )

        if result.exit_code != 0:
            print(f"[DEBUG TEST] Command failed with exit code {result.exit_code}")
            print(f"[DEBUG TEST] Stderr: {result.stderr[:500]}")

        return result.exit_code, result.stdout, result.stderr

    def test_graphql_schema_introspection(self):
        """Test GraphQL schema introspection with authentication."""
        exit_code, stdout, stderr = self._run_authenticated_graphql(
            ["graphql", "--list-operations", "--format", "json"]
        )

        assert exit_code == 0, f"Schema introspection failed: {stderr}"

        # Should produce some output showing operations
        if stdout.strip():
            # Either JSON format or human-readable format is acceptable
            if stdout.strip().startswith("{") or stdout.strip().startswith("["):
                try:
                    schema_data = json.loads(stdout)
                    assert isinstance(schema_data, (dict, list))
                except json.JSONDecodeError as e:
                    pytest.fail(f"Schema output is not valid JSON: {e}")
            else:
                # Human readable format
                assert len(stdout.strip()) > 0

    def test_graphql_simple_query_execution(self):
        """Test execution of a simple GraphQL query."""
        simple_query = "{ __typename }"

        exit_code, stdout, stderr = self._run_authenticated_graphql(
            ["graphql", "--query", simple_query, "--format", "json"]
        )

        assert exit_code == 0, f"Simple query execution failed: {stderr}"

        # Parse JSON output
        try:
            result = json.loads(stdout)
            assert isinstance(result, dict)
            # Should contain query response - either data field or direct response
            assert "data" in result or "__typename" in result or len(result) > 0
        except json.JSONDecodeError as e:
            pytest.fail(f"Query result is not valid JSON: {e}")

    def test_graphql_query_from_file(self):
        """Test GraphQL query execution from file with relative path."""
        query_content = """{
  __typename
}"""

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".graphql", delete=False
        ) as f:
            f.write(query_content)
            temp_path = f.name

        try:
            # Test with absolute path
            exit_code, stdout, stderr = self._run_authenticated_graphql(
                ["graphql", "--query", temp_path, "--format", "json"]
            )

            assert exit_code == 0, f"File query execution failed: {stderr}"

            # Parse JSON output
            try:
                result = json.loads(stdout)
                assert isinstance(result, dict)
                assert "data" in result or "__typename" in result or len(result) > 0
            except json.JSONDecodeError as e:
                pytest.fail(f"File query result is not valid JSON: {e}")

        finally:
            os.unlink(temp_path)

    def test_graphql_list_operations(self):
        """Test GraphQL CLI list operations functionality."""
        for operation_type in ["queries", "mutations"]:
            exit_code, stdout, stderr = self._run_authenticated_graphql(
                ["graphql", f"--list-{operation_type}", "--format", "json"]
            )

            assert exit_code == 0, f"List {operation_type} failed: {stderr}"

            # Should produce some output (might be empty list)
            if stdout.strip():
                try:
                    result = json.loads(stdout)
                    assert isinstance(result, (list, dict))
                except json.JSONDecodeError:
                    # Some operations might produce non-JSON output, which is also acceptable
                    assert len(stdout.strip()) > 0

    def test_graphql_json_output_format(self):
        """Test that JSON output format is properly structured."""
        exit_code, stdout, stderr = self._run_authenticated_graphql(
            ["graphql", "--query", "{ __typename }", "--format", "json"]
        )

        assert exit_code == 0, f"JSON output test failed: {stderr}"

        # Verify JSON structure
        try:
            result = json.loads(stdout)
            assert isinstance(result, dict)

            # Should follow GraphQL response format or be direct data
            if "data" in result:
                assert isinstance(result["data"], (dict, type(None)))
            elif "__typename" in result:
                # Direct response format
                assert isinstance(result, dict)
            if "errors" in result:
                assert isinstance(result["errors"], list)

        except json.JSONDecodeError as e:
            pytest.fail(f"JSON output is malformed: {e}")

    def test_graphql_error_handling(self):
        """Test GraphQL CLI error handling with invalid queries."""
        invalid_query = "{ invalidField { doesNotExist } }"

        exit_code, stdout, stderr = self._run_authenticated_graphql(
            ["graphql", "--query", invalid_query, "--format", "json"]
        )

        # Should handle errors gracefully
        assert exit_code in [0, 1], (
            f"Error handling failed with unexpected exit code. stderr: {stderr}"
        )

        if exit_code == 0:
            # If successful, should contain error information in GraphQL response
            try:
                result = json.loads(stdout)
                # GraphQL errors should be in the response
                assert "errors" in result or "data" in result
            except json.JSONDecodeError:
                pytest.fail("Invalid query should produce structured error response")
        else:
            # If exit code 1, should have meaningful error message
            assert len(stderr.strip()) > 0, "Should provide error message on failure"


class TestGraphQLCLIFileHandling:
    """Specific tests for file handling improvements."""

    def test_json_file_detection(self):
        """Test that CLI properly detects and handles JSON files."""
        json_content = {"query": "{ __typename }"}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(json_content, f)
            temp_path = f.name

        try:
            # Should recognize .json extension
            result = run_datahub_cmd(["graphql", "--query", temp_path])
            stderr = result.stderr

            # Should not fail due to file detection issues
            assert "No such file or directory" not in stderr
            assert "FileNotFoundError" not in stderr

        finally:
            os.unlink(temp_path)

    def test_relative_path_resolution(self):
        """Test basic relative path resolution scenarios."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Test simple relative path scenario
            query_file = temp_path / "query.graphql"
            query_file.write_text("{ __typename }")

            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                test_path = "./query.graphql"

                result = run_datahub_cmd(["graphql", "--query", test_path])
                stderr = result.stderr

                # File should be found and recognized
                assert "No such file or directory" not in stderr
                assert "FileNotFoundError" not in stderr

            finally:
                os.chdir(original_cwd)
