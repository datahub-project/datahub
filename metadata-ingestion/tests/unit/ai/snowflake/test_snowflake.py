"""Tests for datahub.ai.snowflake.snowflake module."""

from pathlib import Path
from unittest.mock import Mock

from datahub.ai.snowflake.snowflake import (
    create_snowflake_agent,
    execute_sql_in_snowflake,
    generate_configuration_sql,
    generate_cortex_agent_sql,
    generate_network_rules_sql,
    generate_stored_procedure_sql,
)


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
            datahub_ips="('1.2.3.4')",
            agent_name="TEST_AGENT",
            agent_display_name="Test Agent",
            agent_color="blue",
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
            datahub_ips="('1.1.1.1')",
            agent_name="AGENT",
            agent_display_name="Agent",
            agent_color="blue",
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
        assert "9 Python UDFs" in udfs_content
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
