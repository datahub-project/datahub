"""Tests for datahub.ai.snowflake.snowflake module."""

from pathlib import Path
from unittest.mock import Mock

from datahub_agent_context.snowflake.generators import (
    generate_configuration_sql,
    generate_cortex_agent_sql,
    generate_network_rules_sql,
    generate_stored_procedure_sql,
)
from datahub_agent_context.snowflake.snowflake import (
    build_connection_params,
    create_snowflake_agent,
    execute_sql_in_snowflake,
    execute_sql_scripts_in_snowflake,
    extract_domain_from_url,
    generate_all_sql_scripts,
    write_sql_files_to_disk,
)


class TestExtractDomainFromUrl:
    """Tests for extract_domain_from_url function."""

    def test_extract_domain_https(self) -> None:
        """Test extracting domain from HTTPS URL."""
        assert extract_domain_from_url("https://test.acryl.io") == "test.acryl.io"

    def test_extract_domain_http(self) -> None:
        """Test extracting domain from HTTP URL."""
        assert extract_domain_from_url("http://test.acryl.io") == "test.acryl.io"

    def test_extract_domain_with_path(self) -> None:
        """Test extracting domain from URL with path."""
        assert (
            extract_domain_from_url("https://test.acryl.io/some/path")
            == "test.acryl.io"
        )

    def test_extract_domain_with_port(self) -> None:
        """Test extracting domain from URL with port."""
        assert extract_domain_from_url("https://localhost:8080") == "localhost:8080"

    def test_extract_domain_no_protocol(self) -> None:
        """Test extracting domain from URL without protocol raises error."""
        import pytest

        with pytest.raises(ValueError, match="must start with http:// or https://"):
            extract_domain_from_url("example.com")

    def test_extract_domain_empty_string(self) -> None:
        """Test that empty string raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="must be a non-empty string"):
            extract_domain_from_url("")

    def test_extract_domain_whitespace_only(self) -> None:
        """Test that whitespace-only string raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="must start with http:// or https://"):
            extract_domain_from_url("   ")

    def test_extract_domain_none_value(self) -> None:
        """Test that None value raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="must be a non-empty string"):
            extract_domain_from_url(None)  # type: ignore

    def test_extract_domain_invalid_no_domain(self) -> None:
        """Test that URL with no domain raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="Could not extract domain"):
            extract_domain_from_url("https://")

    def test_extract_domain_invalid_no_dot(self) -> None:
        """Test that domain without dot (except localhost) raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="Invalid domain format"):
            extract_domain_from_url("https://invaliddomain")

    def test_extract_domain_localhost(self) -> None:
        """Test that localhost is accepted without dots."""
        assert extract_domain_from_url("http://localhost") == "localhost"
        assert extract_domain_from_url("http://localhost:8080") == "localhost:8080"

    def test_extract_domain_with_leading_trailing_whitespace(self) -> None:
        """Test that leading/trailing whitespace is handled."""
        assert extract_domain_from_url("  https://test.acryl.io  ") == "test.acryl.io"


class TestBuildConnectionParams:
    """Tests for build_connection_params function."""

    def test_build_connection_params_password_auth(self) -> None:
        """Test building connection params for password authentication."""
        params = build_connection_params(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_password="test_password",
            sf_authenticator="snowflake",
        )

        assert params == {
            "account": "test_account",
            "user": "test_user",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "password": "test_password",
        }

    def test_build_connection_params_externalbrowser_auth(self) -> None:
        """Test building connection params for SSO authentication."""
        params = build_connection_params(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_password=None,
            sf_authenticator="externalbrowser",
        )

        assert params == {
            "account": "test_account",
            "user": "test_user",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "authenticator": "externalbrowser",
        }

    def test_build_connection_params_oauth_auth(self) -> None:
        """Test building connection params for OAuth authentication."""
        params = build_connection_params(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_password=None,
            sf_authenticator="oauth",
        )

        assert params == {
            "account": "test_account",
            "user": "test_user",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "authenticator": "oauth",
        }

    def test_build_connection_params_omits_none_values(self) -> None:
        """Test that None values are omitted from connection params."""
        params = build_connection_params(
            sf_account=None,
            sf_user="test_user",
            sf_role=None,
            sf_warehouse="test_warehouse",
            sf_password="test_password",
            sf_authenticator="snowflake",
        )

        assert params == {
            "user": "test_user",
            "warehouse": "test_warehouse",
            "password": "test_password",
        }
        assert "account" not in params
        assert "role" not in params

    def test_build_connection_params_password_auth_without_password(self) -> None:
        """Test password auth without password (edge case)."""
        params = build_connection_params(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_password=None,
            sf_authenticator="snowflake",
        )

        assert "password" not in params


class TestGenerateAllSqlScripts:
    """Tests for generate_all_sql_scripts function."""

    def test_generate_all_sql_scripts_returns_five_scripts(self) -> None:
        """Test that all five SQL scripts are generated."""
        scripts = generate_all_sql_scripts(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
            datahub_url="https://test.acryl.io",
            datahub_token="test_token",
            agent_name="TEST_AGENT",
            agent_display_name="Test Agent",
            agent_color="blue",
            enable_mutations=True,
            execute_mode=False,
        )

        assert len(scripts) == 5
        script_names = [name for name, _ in scripts]
        assert script_names == [
            "00_configuration.sql",
            "01_network_rules.sql",
            "02_datahub_udfs.sql",
            "03_stored_procedure.sql",
            "04_cortex_agent.sql",
        ]

    def test_generate_all_sql_scripts_content_not_empty(self) -> None:
        """Test that generated scripts have content."""
        scripts = generate_all_sql_scripts(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
            datahub_url="https://test.acryl.io",
            datahub_token="test_token",
            agent_name="TEST_AGENT",
            agent_display_name="Test Agent",
            agent_color="blue",
            enable_mutations=True,
            execute_mode=False,
        )

        for script_name, script_content in scripts:
            assert len(script_content) > 0, f"{script_name} should have content"

    def test_generate_all_sql_scripts_execute_mode(self) -> None:
        """Test that execute mode affects configuration SQL."""
        scripts = generate_all_sql_scripts(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
            datahub_url="https://test.acryl.io",
            datahub_token="test_token",
            agent_name="TEST_AGENT",
            agent_display_name="Test Agent",
            agent_color="blue",
            enable_mutations=True,
            execute_mode=True,
        )

        config_sql = scripts[0][1]
        assert "test_token" in config_sql

    def test_generate_all_sql_scripts_with_mutations(self) -> None:
        """Test that mutations setting affects UDF generation."""
        scripts_with_mutations = generate_all_sql_scripts(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
            datahub_url="https://test.acryl.io",
            datahub_token="test_token",
            agent_name="TEST_AGENT",
            agent_display_name="Test Agent",
            agent_color="blue",
            enable_mutations=True,
            execute_mode=False,
        )

        scripts_without_mutations = generate_all_sql_scripts(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
            datahub_url="https://test.acryl.io",
            datahub_token="test_token",
            agent_name="TEST_AGENT",
            agent_display_name="Test Agent",
            agent_color="blue",
            enable_mutations=False,
            execute_mode=False,
        )

        udfs_with = scripts_with_mutations[2][1]
        udfs_without = scripts_without_mutations[2][1]

        assert "ADD_TAGS" in udfs_with
        assert "ADD_TAGS" not in udfs_without


class TestWriteSqlFilesToDisk:
    """Tests for write_sql_files_to_disk function."""

    def test_write_sql_files_to_disk(self, tmp_path: Path) -> None:
        """Test writing SQL files to disk."""
        scripts = [
            ("00_test.sql", "SELECT 1;"),
            ("01_test.sql", "SELECT 2;"),
        ]

        write_sql_files_to_disk(tmp_path, scripts, enable_mutations=True)

        assert (tmp_path / "00_test.sql").exists()
        assert (tmp_path / "01_test.sql").exists()
        assert (tmp_path / "00_test.sql").read_text() == "SELECT 1;"
        assert (tmp_path / "01_test.sql").read_text() == "SELECT 2;"


class TestExecuteSqlScriptsInSnowflake:
    """Tests for execute_sql_scripts_in_snowflake function."""

    def test_execute_sql_scripts_success(self) -> None:
        """Test successful execution of all scripts."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.description = None
        mock_conn.execute_string.return_value = [mock_cursor]

        scripts = [
            ("00_test.sql", "SELECT 1;"),
            ("01_test.sql", "SELECT 2;"),
        ]

        result = execute_sql_scripts_in_snowflake(mock_conn, scripts)

        assert result is True
        assert mock_conn.execute_string.call_count == 2

    def test_execute_sql_scripts_failure_stops_execution(self) -> None:
        """Test that script execution stops on first failure."""
        mock_conn = Mock()
        mock_conn.execute_string.side_effect = Exception("SQL execution failed")

        scripts = [
            ("00_test.sql", "SELECT 1;"),
            ("01_test.sql", "SELECT 2;"),
            ("02_test.sql", "SELECT 3;"),
        ]

        result = execute_sql_scripts_in_snowflake(mock_conn, scripts)

        assert result is False
        assert mock_conn.execute_string.call_count == 1


class TestExecuteSqlInSnowflake:
    """Tests for execute_sql_in_snowflake function."""

    def test_execute_sql_success(self) -> None:
        """Test successful SQL execution with results."""
        mock_conn = Mock()
        mock_cursor1 = Mock()
        mock_cursor1.description = ["col1"]
        mock_cursor1.fetchall.return_value = [{"col1": "value1"}]

        mock_cursor2 = Mock()
        mock_cursor2.description = None

        mock_conn.execute_string.return_value = [mock_cursor1, mock_cursor2]

        sql_content = "SELECT 1; SELECT 2;"
        result = execute_sql_in_snowflake(mock_conn, sql_content, "test.sql")

        assert result is True
        mock_conn.execute_string.assert_called_once_with(
            sql_content, remove_comments=True
        )
        mock_cursor1.fetchall.assert_called_once()
        mock_cursor1.close.assert_called_once()
        mock_cursor2.close.assert_called_once()

    def test_execute_sql_no_results(self) -> None:
        """Test SQL execution with no results."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.description = None
        mock_conn.execute_string.return_value = [mock_cursor]

        result = execute_sql_in_snowflake(mock_conn, "CREATE TABLE test();", "test.sql")

        assert result is True
        mock_cursor.close.assert_called_once()

    def test_execute_sql_fetch_error_continues(self) -> None:
        """Test that fetchall errors are handled gracefully."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.description = ["col1"]
        mock_cursor.fetchall.side_effect = Exception("No results available")
        mock_conn.execute_string.return_value = [mock_cursor]

        result = execute_sql_in_snowflake(mock_conn, "SELECT 1;", "test.sql")

        assert result is True

    def test_execute_sql_connection_error(self) -> None:
        """Test handling of connection errors."""
        mock_conn = Mock()
        mock_conn.execute_string.side_effect = Exception("Connection failed")

        result = execute_sql_in_snowflake(mock_conn, "SELECT 1;", "test.sql")

        assert result is False


class TestGenerateConfigurationSql:
    """Tests for generate_configuration_sql function."""

    def test_generate_configuration_basic(self) -> None:
        """Test basic configuration SQL generation."""
        result = generate_configuration_sql(
            sf_account="test_account",
            sf_user="test_user",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
            datahub_url="https://test.acryl.io",
            datahub_token="test_token",
            agent_name="TEST_AGENT",
            agent_display_name="Test Agent",
            agent_color="blue",
            execute=True,  # Include actual token in SQL
        )

        assert "SET SF_ACCOUNT = 'test_account';" in result
        assert "SET SF_USER = 'test_user';" in result
        assert "SET SF_ROLE = 'test_role';" in result
        assert "SET SF_WAREHOUSE = 'test_warehouse';" in result
        assert "SET SF_DATABASE = 'test_db';" in result
        assert "SET SF_SCHEMA = 'test_schema';" in result
        assert "SET DATAHUB_URL = 'https://test.acryl.io';" in result
        assert "SECRET_STRING = 'https://test.acryl.io'" in result
        assert "SECRET_STRING = 'test_token'" in result
        assert "SET AGENT_NAME = 'TEST_AGENT';" in result
        assert "SET AGENT_DISPLAY_NAME = 'Test Agent';" in result
        assert "SET AGENT_COLOR = 'blue';" in result
        assert "USE DATABASE IDENTIFIER($SF_DATABASE);" in result
        assert "USE SCHEMA IDENTIFIER($SF_SCHEMA);" in result
        assert "USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);" in result

    def test_generate_configuration_secrets(self) -> None:
        """Test that secrets are created in configuration SQL."""
        result = generate_configuration_sql(
            sf_account="test",
            sf_user="test",
            sf_role="test",
            sf_warehouse="test",
            sf_database="test",
            sf_schema="test",
            datahub_url="https://example.com",
            datahub_token="secret_token_123",
            agent_name="AGENT",
            agent_display_name="Agent",
            agent_color="blue",
            execute=True,  # Include actual token in SQL
        )

        assert "CREATE OR REPLACE SECRET datahub_url" in result
        assert "CREATE OR REPLACE SECRET datahub_token" in result
        assert "SECRET_STRING = 'https://example.com'" in result
        assert "SECRET_STRING = 'secret_token_123'" in result


class TestGenerateNetworkRulesSql:
    """Tests for generate_network_rules_sql function."""

    def test_generate_network_rules_basic(self) -> None:
        """Test network rules SQL generation."""
        result = generate_network_rules_sql("test.acryl.io")

        assert "CREATE OR REPLACE NETWORK RULE datahub_api_rule" in result
        assert "MODE = EGRESS" in result
        assert "TYPE = HOST_PORT" in result
        assert "VALUE_LIST = ('test.acryl.io')" in result
        assert "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION datahub_access" in result
        assert "ALLOWED_NETWORK_RULES = (datahub_api_rule)" in result
        assert "ALLOWED_AUTHENTICATION_SECRETS = (datahub_url, datahub_token)" in result
        assert "GRANT USAGE ON INTEGRATION datahub_access" in result

    def test_generate_network_rules_uses_variables(self) -> None:
        """Test that network rules SQL uses configuration variables."""
        result = generate_network_rules_sql("example.com")

        assert "USE DATABASE IDENTIFIER($SF_DATABASE);" in result
        assert "USE SCHEMA IDENTIFIER($SF_SCHEMA);" in result
        assert "USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);" in result
        assert (
            "GRANT USAGE ON INTEGRATION datahub_access TO ROLE IDENTIFIER($SF_ROLE);"
            in result
        )


class TestGenerateStoredProcedureSql:
    """Tests for generate_stored_procedure_sql function."""

    def test_generate_stored_procedure_basic(self) -> None:
        """Test stored procedure SQL generation."""
        result = generate_stored_procedure_sql()

        assert (
            "CREATE OR REPLACE PROCEDURE EXECUTE_DYNAMIC_SQL(SQL_TEXT STRING)" in result
        )
        assert "RETURNS VARIANT" in result
        assert "LANGUAGE JAVASCRIPT" in result
        assert "EXECUTE AS CALLER" in result

    def test_generate_stored_procedure_select_validation(self) -> None:
        """Test that stored procedure validates SELECT queries."""
        result = generate_stored_procedure_sql()

        assert "var queryUpper = SQL_TEXT.trim().toUpperCase();" in result
        assert "if (!queryUpper.startsWith('SELECT'))" in result
        assert '"Only SELECT queries are allowed' in result

    def test_generate_stored_procedure_uses_variables(self) -> None:
        """Test that stored procedure SQL uses configuration variables."""
        result = generate_stored_procedure_sql()

        assert "USE DATABASE IDENTIFIER($SF_DATABASE);" in result
        assert "USE SCHEMA IDENTIFIER($SF_SCHEMA);" in result
        assert "USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);" in result
        assert (
            "GRANT USAGE ON PROCEDURE EXECUTE_DYNAMIC_SQL(STRING) TO ROLE IDENTIFIER($SF_ROLE);"
            in result
        )


class TestGenerateCortexAgentSql:
    """Tests for generate_cortex_agent_sql function."""

    def test_generate_cortex_agent_basic(self) -> None:
        """Test Cortex Agent SQL generation."""
        result = generate_cortex_agent_sql(
            agent_name="TEST_AGENT",
            agent_display_name="Test Agent",
            agent_color="blue",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
        )

        assert "CREATE OR REPLACE AGENT TEST_AGENT" in result
        assert '"display_name": "Test Agent"' in result
        assert '"color": "blue"' in result

    def test_generate_cortex_agent_all_tools(self) -> None:
        """Test that all expected tools are included."""
        result = generate_cortex_agent_sql(
            agent_name="AGENT",
            agent_display_name="Agent",
            agent_color="blue",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
        )

        # Search & Discovery tools
        assert "search_datahub" in result
        assert "get_entities" in result
        assert "list_schema_fields" in result

        # Lineage tools
        assert "get_lineage" in result
        assert "get_lineage_paths_between" in result

        # Query analysis
        assert "get_dataset_queries" in result

        # Document search
        assert "search_documents" in result
        assert "grep_documents" in result

        # Mutation tools
        assert "add_tags" in result
        assert "remove_tags" in result
        assert "update_description" in result
        assert "set_domains" in result
        assert "remove_domains" in result
        assert "add_owners" in result
        assert "remove_owners" in result
        assert "add_glossary_terms" in result
        assert "remove_glossary_terms" in result
        assert "add_structured_properties" in result
        assert "remove_structured_properties" in result

        # User info
        assert "get_me" in result

        # SQL Executor
        assert "SqlExecutor" in result

    def test_generate_cortex_agent_uses_variables(self) -> None:
        """Test that Cortex Agent SQL uses configuration variables."""
        result = generate_cortex_agent_sql(
            agent_name="AGENT",
            agent_display_name="Agent",
            agent_color="blue",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
        )

        assert "USE DATABASE IDENTIFIER($SF_DATABASE);" in result
        assert "USE SCHEMA IDENTIFIER($SF_SCHEMA);" in result
        assert "USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);" in result
        assert "warehouse: test_warehouse" in result
        assert "identifier: test_db.test_schema.SEARCH_DATAHUB" in result
        assert "GRANT USAGE ON AGENT AGENT TO ROLE IDENTIFIER($SF_ROLE);" in result

    def test_generate_cortex_agent_mutation_tools_use_variables(self) -> None:
        """Test that mutation tool resources correctly use warehouse/db/schema variables."""
        result = generate_cortex_agent_sql(
            agent_name="AGENT",
            agent_display_name="Agent",
            agent_color="blue",
            sf_warehouse="test_warehouse",
            sf_database="test_db",
            sf_schema="test_schema",
            include_mutations=True,
        )

        # Verify mutation tools use the provided variables, not template placeholders
        assert "warehouse: test_warehouse" in result
        assert "identifier: test_db.test_schema.ADD_TAGS" in result
        assert "identifier: test_db.test_schema.REMOVE_TAGS" in result
        assert "identifier: test_db.test_schema.UPDATE_DESCRIPTION" in result

        # Ensure no unresolved template placeholders remain
        assert "{warehouse}" not in result
        assert "{database}" not in result
        assert "{schema}" not in result


class TestCreateSnowflakeAgent:
    """Tests for create_snowflake_agent CLI command."""

    def test_create_snowflake_agent_generates_files(self, tmp_path: Path) -> None:
        """Test that create_snowflake_agent generates all SQL files."""
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            create_snowflake_agent,
            [
                "--sf-account",
                "test_account",
                "--sf-user",
                "test_user",
                "--sf-role",
                "test_role",
                "--sf-warehouse",
                "test_warehouse",
                "--sf-database",
                "test_db",
                "--sf-schema",
                "test_schema",
                "--datahub-url",
                "https://test.acryl.io",
                "--datahub-token",
                "test_token",
                "--agent-name",
                "TEST_AGENT",
                "--agent-display-name",
                "Test Agent",
                "--agent-color",
                "blue",
                "--output-dir",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0
        assert (tmp_path / "00_configuration.sql").exists()
        assert (tmp_path / "01_network_rules.sql").exists()
        assert (tmp_path / "02_datahub_udfs.sql").exists()
        assert (tmp_path / "03_stored_procedure.sql").exists()
        assert (tmp_path / "04_cortex_agent.sql").exists()

    def test_create_snowflake_agent_without_mutations(self, tmp_path: Path) -> None:
        """Test creating agent with mutations disabled."""
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            create_snowflake_agent,
            [
                "--sf-account",
                "test_account",
                "--sf-user",
                "test_user",
                "--sf-role",
                "test_role",
                "--sf-warehouse",
                "test_warehouse",
                "--sf-database",
                "test_db",
                "--sf-schema",
                "test_schema",
                "--datahub-url",
                "https://test.acryl.io",
                "--datahub-token",
                "test_token",
                "--no-enable-mutations",
                "--output-dir",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0
        # Check that UDFs file has fewer functions
        udfs_content = (tmp_path / "02_datahub_udfs.sql").read_text()
        assert "10 Python UDFs" in udfs_content
        assert "ADD_TAGS" not in udfs_content

    def test_create_snowflake_agent_execute_requires_password(
        self, tmp_path: Path
    ) -> None:
        """Test that execute mode with password auth requires password."""
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            create_snowflake_agent,
            [
                "--sf-account",
                "test_account",
                "--sf-user",
                "test_user",
                "--sf-role",
                "test_role",
                "--sf-warehouse",
                "test_warehouse",
                "--sf-database",
                "test_db",
                "--sf-schema",
                "test_schema",
                "--datahub-url",
                "https://test.acryl.io",
                "--datahub-token",
                "test_token",
                "--output-dir",
                str(tmp_path),
                "--execute",
                # Missing --sf-password
            ],
        )

        assert result.exit_code == 0
        assert "Error" in result.output or "required" in result.output

    def test_create_snowflake_agent_domain_extraction(self, tmp_path: Path) -> None:
        """Test that DataHub domain is correctly extracted from URL."""
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            create_snowflake_agent,
            [
                "--sf-account",
                "test_account",
                "--sf-user",
                "test_user",
                "--sf-role",
                "test_role",
                "--sf-warehouse",
                "test_warehouse",
                "--sf-database",
                "test_db",
                "--sf-schema",
                "test_schema",
                "--datahub-url",
                "https://test.acryl.io/some/path",
                "--datahub-token",
                "test_token",
                "--output-dir",
                str(tmp_path),
            ],
        )

        assert result.exit_code == 0
        # Check that network rules file has the correct domain
        network_rules = (tmp_path / "01_network_rules.sql").read_text()
        assert "test.acryl.io" in network_rules

    def test_create_snowflake_agent_execute_success(self, tmp_path: Path) -> None:
        """Test successful execution with mocked Snowflake connection."""
        from unittest.mock import MagicMock, patch

        from click.testing import CliRunner

        # Mock snowflake connector
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_conn.execute_string.return_value = [mock_cursor]

        # Mock snowflake module
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_conn

        runner = CliRunner()
        with (
            patch.dict(
                "sys.modules",
                {
                    "snowflake": mock_snowflake,
                    "snowflake.connector": mock_snowflake.connector,
                },
            ),
            patch(
                "datahub_agent_context.snowflake.snowflake.auto_detect_snowflake_params",
                return_value=(
                    "test_account",
                    "test_user",
                    "test_role",
                    "test_warehouse",
                    "test_db",
                    "test_schema",
                ),
            ),
        ):
            result = runner.invoke(
                create_snowflake_agent,
                [
                    "--sf-account",
                    "test_account",
                    "--sf-user",
                    "test_user",
                    "--sf-role",
                    "test_role",
                    "--sf-warehouse",
                    "test_warehouse",
                    "--sf-database",
                    "test_db",
                    "--sf-schema",
                    "test_schema",
                    "--datahub-url",
                    "https://test.acryl.io",
                    "--datahub-token",
                    "test_token",
                    "--sf-password",
                    "test_password",
                    "--output-dir",
                    str(tmp_path),
                    "--execute",
                ],
            )

        assert result.exit_code == 0
        assert "Connected successfully" in result.output
        assert "All scripts executed successfully" in result.output
        # Verify connection was called with correct params
        mock_conn.execute_string.assert_called()
        mock_conn.close.assert_called_once()

    def test_create_snowflake_agent_execute_connection_failure(
        self, tmp_path: Path
    ) -> None:
        """Test handling of connection failure during execution."""
        from unittest.mock import MagicMock, patch

        from click.testing import CliRunner

        # Mock snowflake module that raises exception
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.side_effect = Exception("Connection failed")

        runner = CliRunner()
        with patch.dict(
            "sys.modules",
            {
                "snowflake": mock_snowflake,
                "snowflake.connector": mock_snowflake.connector,
            },
        ):
            result = runner.invoke(
                create_snowflake_agent,
                [
                    "--sf-account",
                    "test_account",
                    "--sf-user",
                    "test_user",
                    "--sf-role",
                    "test_role",
                    "--sf-warehouse",
                    "test_warehouse",
                    "--sf-database",
                    "test_db",
                    "--sf-schema",
                    "test_schema",
                    "--datahub-url",
                    "https://test.acryl.io",
                    "--datahub-token",
                    "test_token",
                    "--sf-password",
                    "test_password",
                    "--output-dir",
                    str(tmp_path),
                    "--execute",
                ],
            )

        assert result.exit_code == 0
        assert "Error" in result.output or "failed" in result.output.lower()

    def test_create_snowflake_agent_execute_script_failure(
        self, tmp_path: Path
    ) -> None:
        """Test handling of script execution failure."""
        from unittest.mock import MagicMock, patch

        from click.testing import CliRunner

        # Mock connection that fails on execute_string
        mock_conn = MagicMock()
        mock_conn.execute_string.side_effect = Exception("SQL execution failed")

        # Mock snowflake module
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_conn

        runner = CliRunner()
        with (
            patch.dict(
                "sys.modules",
                {
                    "snowflake": mock_snowflake,
                    "snowflake.connector": mock_snowflake.connector,
                },
            ),
            patch(
                "datahub_agent_context.snowflake.snowflake.auto_detect_snowflake_params",
                return_value=(
                    "test_account",
                    "test_user",
                    "test_role",
                    "test_warehouse",
                    "test_db",
                    "test_schema",
                ),
            ),
        ):
            result = runner.invoke(
                create_snowflake_agent,
                [
                    "--sf-account",
                    "test_account",
                    "--sf-user",
                    "test_user",
                    "--sf-role",
                    "test_role",
                    "--sf-warehouse",
                    "test_warehouse",
                    "--sf-database",
                    "test_db",
                    "--sf-schema",
                    "test_schema",
                    "--datahub-url",
                    "https://test.acryl.io",
                    "--datahub-token",
                    "test_token",
                    "--sf-password",
                    "test_password",
                    "--output-dir",
                    str(tmp_path),
                    "--execute",
                ],
            )

        assert result.exit_code == 0
        assert "failed" in result.output.lower()
        mock_conn.close.assert_called_once()

    def test_create_snowflake_agent_execute_all_scripts_in_order(
        self, tmp_path: Path
    ) -> None:
        """Test that all SQL scripts are executed in the correct order."""
        from unittest.mock import MagicMock, patch

        from click.testing import CliRunner

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_conn.execute_string.return_value = [mock_cursor]

        # Mock snowflake module
        mock_snowflake = MagicMock()
        mock_snowflake.connector.connect.return_value = mock_conn

        runner = CliRunner()
        with (
            patch.dict(
                "sys.modules",
                {
                    "snowflake": mock_snowflake,
                    "snowflake.connector": mock_snowflake.connector,
                },
            ),
            patch(
                "datahub_agent_context.snowflake.snowflake.auto_detect_snowflake_params",
                return_value=(
                    "test_account",
                    "test_user",
                    "test_role",
                    "test_warehouse",
                    "test_db",
                    "test_schema",
                ),
            ),
        ):
            result = runner.invoke(
                create_snowflake_agent,
                [
                    "--sf-account",
                    "test_account",
                    "--sf-user",
                    "test_user",
                    "--sf-role",
                    "test_role",
                    "--sf-warehouse",
                    "test_warehouse",
                    "--sf-database",
                    "test_db",
                    "--sf-schema",
                    "test_schema",
                    "--datahub-url",
                    "https://test.acryl.io",
                    "--datahub-token",
                    "test_token",
                    "--sf-password",
                    "test_password",
                    "--output-dir",
                    str(tmp_path),
                    "--execute",
                ],
            )

        assert result.exit_code == 0
        # Verify all 5 scripts were executed
        assert mock_conn.execute_string.call_count == 5
        # Verify scripts executed in order by checking output
        assert "00_configuration.sql" in result.output
        assert "01_network_rules.sql" in result.output
        assert "02_datahub_udfs.sql" in result.output
        assert "03_stored_procedure.sql" in result.output
        assert "04_cortex_agent.sql" in result.output
