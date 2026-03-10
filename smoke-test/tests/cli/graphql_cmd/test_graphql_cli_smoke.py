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
from typing import Any, Optional

import pytest
import requests

from tests.utils import run_datahub_cmd, wait_for_healthcheck_util


class TestGraphQLCLIStandalone:
    """Fast standalone tests that validate GraphQL CLI behavior against a live DataHub."""

    def _run_datahub_cli(
        self, auth_session: Any, args: list[str], input_data: Optional[str] = None
    ) -> tuple[int, str, str]:
        result = run_datahub_cmd(
            args,
            input=input_data,
            env={
                "DATAHUB_GMS_URL": auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
            },
        )
        return result.exit_code, result.stdout, result.stderr

    def test_graphql_help(self, auth_session: Any):
        """Test that GraphQL CLI help is accessible."""
        exit_code, stdout, stderr = self._run_datahub_cli(
            auth_session, ["graphql", "--help"]
        )

        assert exit_code == 0, f"CLI help failed with stderr: {stderr}"
        assert "GraphQL" in stdout or "graphql" in stdout
        assert "--list-operations" in stdout or "--schema-path" in stdout
        assert "--query" in stdout

    def test_graphql_schema_discovery(self, auth_session: Any):
        """Test GraphQL schema discovery via live introspection."""
        exit_code, stdout, stderr = self._run_datahub_cli(
            auth_session, ["graphql", "--list-operations"]
        )

        assert exit_code == 0, f"Schema discovery failed. stderr: {stderr}"
        assert "Traceback" not in stderr

    def test_graphql_file_path_handling(self, auth_session: Any):
        """Test that GraphQL CLI properly handles absolute file path arguments."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".graphql", delete=False
        ) as f:
            f.write("{ __typename }")
            temp_path = f.name

        try:
            exit_code, stdout, stderr = self._run_datahub_cli(
                auth_session,
                ["graphql", "--query", temp_path, "--format", "json"],
            )

            assert exit_code == 0, (
                f"Unexpected exit code with file path. stderr: {stderr}"
            )
            assert "No such file or directory" not in stderr
            assert "FileNotFoundError" not in stderr

        finally:
            os.unlink(temp_path)

    def test_graphql_relative_path_handling(self, auth_session: Any):
        """Test that GraphQL CLI handles relative paths correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            query_file = temp_path / "test.graphql"
            query_file.write_text("{ __typename }")

            original_cwd = os.getcwd()
            try:
                os.chdir(temp_path.parent)
                relative_path = os.path.relpath(str(query_file))

                exit_code, stdout, stderr = self._run_datahub_cli(
                    auth_session,
                    ["graphql", "--query", relative_path, "--format", "json"],
                )

                assert exit_code == 0, (
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
        result = run_datahub_cmd(
            args,
            env={
                "DATAHUB_GMS_URL": self.auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": self.auth_session.gms_token(),
            },
        )
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


class TestAgentFriendlyFeatures:
    """
    Smoke tests for the agent-friendly additions to `datahub graphql`.

    Proves that --agent-context, --dry-run, structured exit codes, and
    non-TTY help injection work correctly against a live DataHub instance.
    """

    @pytest.fixture(autouse=True)
    def setup_datahub(self, auth_session):
        self.auth_session = auth_session

    def _run(self, args: list) -> tuple:
        """Run `datahub graphql ...` with auth. Returns (exit_code, stdout, stderr)."""
        result = run_datahub_cmd(
            ["graphql"] + args,
            env={
                "DATAHUB_GMS_URL": self.auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": self.auth_session.gms_token(),
            },
        )
        return result.exit_code, result.stdout, result.stderr

    def test_agent_context_flag_exits_zero(self):
        """--agent-context prints the context file and exits 0 (no GMS call needed)."""
        exit_code, stdout, _ = self._run(["--agent-context"])
        assert exit_code == 0
        assert "## Output Discipline" in stdout
        assert "## Dry Run" in stdout
        assert "## Error Handling" in stdout

    def test_non_tty_help_includes_agent_context(self):
        """--help in non-TTY (CliRunner/pipe) appends GRAPHQL_AGENT_CONTEXT.md."""
        exit_code, stdout, _ = self._run(["--help"])
        assert exit_code == 0
        # Core help content
        assert "--dry-run" in stdout
        assert "--agent-context" in stdout
        # Agent context injected at the end
        assert "## Output Discipline" in stdout

    def test_dry_run_with_raw_query_returns_json(self):
        """`--dry-run --query` returns JSON without hitting the GraphQL endpoint."""
        query = "{ me { corpUser { urn } } }"
        exit_code, stdout, stderr = self._run(["--query", query, "--dry-run"])
        assert exit_code == 0, f"dry-run failed: {stderr}"
        data = json.loads(stdout)
        assert data["dry_run"] is True
        assert data["query"] == query
        assert "variables" in data
        assert isinstance(data["variables"], dict)

    def test_dry_run_with_operation_resolves_via_introspection(self):
        """`--dry-run --operation me` introspects the live schema and shows the query."""
        exit_code, stdout, stderr = self._run(["--operation", "me", "--dry-run"])
        assert exit_code == 0, f"dry-run operation failed: {stderr}"
        data = json.loads(stdout)
        assert data["dry_run"] is True
        assert data["operation"] == "me"
        assert "me" in data["query"]
        assert "operation_type" in data

    def test_no_args_exits_with_usage_code(self):
        """`datahub graphql` with no flags exits 2 (usage error)."""
        exit_code, _, stderr = self._run([])
        assert exit_code == 2
        # In non-TTY context, error should be JSON
        error = json.loads(stderr)
        assert error["error"] == "usage_error"

    def test_list_operations_returns_real_schema(self):
        """`--list-operations --format json` returns real DataHub operations."""
        exit_code, stdout, stderr = self._run(["--list-operations", "--format", "json"])
        assert exit_code == 0, f"list-operations failed: {stderr}"
        data = json.loads(stdout)
        assert "schema" in data
        queries = data["schema"].get("queries", [])
        mutations = data["schema"].get("mutations", [])
        assert len(queries) > 0, "Expected at least one query in live schema"
        assert len(mutations) > 0, "Expected at least one mutation in live schema"
        # Spot-check that well-known operations are present
        query_names = {q["name"] for q in queries}
        assert "me" in query_names or "search" in query_names

    def test_describe_me_returns_operation_details(self):
        """`--describe me --format json` returns operation details with arguments."""
        exit_code, stdout, stderr = self._run(["--describe", "me", "--format", "json"])
        assert exit_code == 0, f"describe failed: {stderr}"
        data = json.loads(stdout)
        assert "operation" in data
        assert data["operation"]["name"] == "me"
        assert data["operation"]["type"] in ("Query", "Mutation")

    def test_execute_me_query_returns_current_user(self):
        """`--query '{ me { corpUser { urn } } }'` returns the authenticated user URN."""
        exit_code, stdout, stderr = self._run(
            ["--query", "{ me { corpUser { urn } } }", "--format", "json"]
        )
        assert exit_code == 0, f"me query failed: {stderr}"
        data = json.loads(stdout)
        # DataHub returns either {me: {...}} or {data: {me: {...}}}
        me_data = data.get("me") or (data.get("data") or {}).get("me")
        assert me_data is not None, f"Expected 'me' in response, got: {data}"
        urn = (me_data.get("corpUser") or {}).get("urn", "")
        assert urn.startswith("urn:li:corpuser:")
