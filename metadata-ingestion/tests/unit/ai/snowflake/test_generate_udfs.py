"""Tests for datahub.ai.snowflake.generate_udfs module."""

from pathlib import Path

from datahub.ai.snowflake.generate_udfs import (
    generate_all_udfs,
    generate_datahub_udfs_sql,
)


class TestGenerateAllUdfs:
    """Tests for generate_all_udfs function."""

    def test_generate_all_udfs_with_mutations(self) -> None:
        """Test that all UDFs are generated when mutations are enabled."""
        udfs = generate_all_udfs(include_mutations=True)

        # Should have all 20 tools
        assert len(udfs) == 20

        # Read-only tools (9)
        assert "SEARCH_DATAHUB" in udfs
        assert "GET_ENTITIES" in udfs
        assert "LIST_SCHEMA_FIELDS" in udfs
        assert "GET_LINEAGE" in udfs
        assert "GET_LINEAGE_PATHS_BETWEEN" in udfs
        assert "GET_DATASET_QUERIES" in udfs
        assert "SEARCH_DOCUMENTS" in udfs
        assert "GREP_DOCUMENTS" in udfs
        assert "GET_ME" in udfs

        # Write tools (11)
        assert "ADD_TAGS" in udfs
        assert "REMOVE_TAGS" in udfs
        assert "UPDATE_DESCRIPTION" in udfs
        assert "SET_DOMAINS" in udfs
        assert "REMOVE_DOMAINS" in udfs
        assert "ADD_OWNERS" in udfs
        assert "REMOVE_OWNERS" in udfs
        assert "ADD_GLOSSARY_TERMS" in udfs
        assert "REMOVE_GLOSSARY_TERMS" in udfs
        assert "ADD_STRUCTURED_PROPERTIES" in udfs
        assert "REMOVE_STRUCTURED_PROPERTIES" in udfs

    def test_generate_all_udfs_without_mutations(self) -> None:
        """Test that only read-only UDFs are generated when mutations are disabled."""
        udfs = generate_all_udfs(include_mutations=False)

        # Should have only 9 read-only tools
        assert len(udfs) == 9

        # Read-only tools present
        assert "SEARCH_DATAHUB" in udfs
        assert "GET_ENTITIES" in udfs
        assert "LIST_SCHEMA_FIELDS" in udfs
        assert "GET_LINEAGE" in udfs
        assert "GET_LINEAGE_PATHS_BETWEEN" in udfs
        assert "GET_DATASET_QUERIES" in udfs
        assert "SEARCH_DOCUMENTS" in udfs
        assert "GREP_DOCUMENTS" in udfs
        assert "GET_ME" in udfs

        # Write tools absent
        assert "ADD_TAGS" not in udfs
        assert "REMOVE_TAGS" not in udfs
        assert "UPDATE_DESCRIPTION" not in udfs
        assert "SET_DOMAINS" not in udfs
        assert "REMOVE_DOMAINS" not in udfs
        assert "ADD_OWNERS" not in udfs
        assert "REMOVE_OWNERS" not in udfs
        assert "ADD_GLOSSARY_TERMS" not in udfs
        assert "REMOVE_GLOSSARY_TERMS" not in udfs
        assert "ADD_STRUCTURED_PROPERTIES" not in udfs
        assert "REMOVE_STRUCTURED_PROPERTIES" not in udfs

    def test_udf_content_is_valid_sql(self) -> None:
        """Test that each UDF contains valid SQL structure."""
        udfs = generate_all_udfs(include_mutations=True)

        for function_name, udf_sql in udfs.items():
            # Each UDF should have CREATE FUNCTION statement
            assert f"CREATE OR REPLACE FUNCTION {function_name}" in udf_sql
            assert "RETURNS VARIANT" in udf_sql
            assert "LANGUAGE PYTHON" in udf_sql
            assert "RUNTIME_VERSION = '3.10'" in udf_sql
            assert "PACKAGES = ('datahub-agent-context>=1.3.1.8')" in udf_sql
            assert (
                "SECRETS = ('datahub_url_secret' = datahub_url, 'datahub_token_secret' = datahub_token)"
                in udf_sql
            )
            assert "EXTERNAL_ACCESS_INTEGRATIONS = (datahub_access)" in udf_sql


class TestGenerateDatahubUdfsSql:
    """Tests for generate_datahub_udfs_sql function."""

    def test_generate_udfs_sql_with_mutations(self) -> None:
        """Test SQL script generation with mutations enabled."""
        sql = generate_datahub_udfs_sql(include_mutations=True)

        # Should contain header
        assert "Step 2: DataHub API UDFs for Cortex Agent" in sql
        assert "using datahub-agent-context" in sql

        # Should mention total count (20 UDFs)
        assert "20 Python UDFs" in sql or "20" in sql

        # Should contain USE statements
        assert "USE DATABASE IDENTIFIER($SF_DATABASE);" in sql
        assert "USE SCHEMA IDENTIFIER($SF_SCHEMA);" in sql
        assert "USE WAREHOUSE IDENTIFIER($SF_WAREHOUSE);" in sql

        # Should contain all UDF definitions
        assert "CREATE OR REPLACE FUNCTION SEARCH_DATAHUB" in sql
        assert "CREATE OR REPLACE FUNCTION ADD_TAGS" in sql
        assert "CREATE OR REPLACE FUNCTION UPDATE_DESCRIPTION" in sql

        # Should contain GRANT statements
        assert "GRANT USAGE ON FUNCTION" in sql

        # Should contain verification section
        assert "SHOW FUNCTIONS LIKE" in sql

    def test_generate_udfs_sql_without_mutations(self) -> None:
        """Test SQL script generation with mutations disabled."""
        sql = generate_datahub_udfs_sql(include_mutations=False)

        # Should mention 9 UDFs (read-only)
        assert "9 Python UDFs" in sql or "9" in sql

        # Should contain read-only UDFs
        assert "CREATE OR REPLACE FUNCTION SEARCH_DATAHUB" in sql
        assert "CREATE OR REPLACE FUNCTION GET_ENTITIES" in sql

        # Should NOT contain mutation UDFs
        assert "CREATE OR REPLACE FUNCTION ADD_TAGS" not in sql
        assert "CREATE OR REPLACE FUNCTION UPDATE_DESCRIPTION" not in sql
        assert "CREATE OR REPLACE FUNCTION SET_DOMAINS" not in sql

    def test_generate_udfs_sql_grant_statements(self) -> None:
        """Test that GRANT statements are generated for all UDFs."""
        sql = generate_datahub_udfs_sql(include_mutations=True)

        # Check for GRANT statements with correct signatures
        assert "GRANT USAGE ON FUNCTION SEARCH_DATAHUB(STRING, STRING)" in sql
        assert "GRANT USAGE ON FUNCTION GET_ENTITIES(STRING)" in sql
        assert "GRANT USAGE ON FUNCTION GET_ME()" in sql
        assert "GRANT USAGE ON FUNCTION ADD_TAGS(STRING, STRING, STRING)" in sql
        assert (
            "GRANT USAGE ON FUNCTION UPDATE_DESCRIPTION(STRING, STRING, STRING, STRING)"
            in sql
        )

    def test_generate_udfs_sql_show_statements(self) -> None:
        """Test that SHOW statements are generated for verification."""
        sql = generate_datahub_udfs_sql(include_mutations=True)

        # Should have SHOW statements for all UDFs
        assert "SHOW FUNCTIONS LIKE 'SEARCH_DATAHUB';" in sql
        assert "SHOW FUNCTIONS LIKE 'GET_ENTITIES';" in sql
        assert "SHOW FUNCTIONS LIKE 'GET_ME';" in sql

    def test_generate_udfs_sql_verification_query(self) -> None:
        """Test that verification query lists all functions."""
        sql = generate_datahub_udfs_sql(include_mutations=True)

        # Should have final SELECT statement with all functions
        assert "SELECT" in sql
        assert "'All 20 DataHub UDFs created successfully!'" in sql or "All 20" in sql
        assert "$SF_DATABASE || '.' || $SF_SCHEMA || '.SEARCH_DATAHUB'" in sql


class TestGenerateUdfsMain:
    """Tests for the main CLI command."""

    def test_main_stdout_output(self) -> None:
        """Test that SQL is printed to stdout when no output file specified."""
        from click.testing import CliRunner

        from datahub.ai.snowflake.generate_udfs import main

        runner = CliRunner()
        result = runner.invoke(main, [])

        assert result.exit_code == 0
        assert "CREATE OR REPLACE FUNCTION" in result.output

    def test_main_file_output(self, tmp_path: Path) -> None:
        """Test that SQL is written to file when output specified."""
        from click.testing import CliRunner

        from datahub.ai.snowflake.generate_udfs import main

        output_file = tmp_path / "test.sql"
        runner = CliRunner()
        result = runner.invoke(main, ["-o", str(output_file)])

        assert result.exit_code == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "CREATE OR REPLACE FUNCTION" in content

    def test_main_with_mutations_flag(self) -> None:
        """Test enabling mutations via CLI flag."""
        from click.testing import CliRunner

        from datahub.ai.snowflake.generate_udfs import main

        runner = CliRunner()
        result = runner.invoke(main, ["--enable-mutations"])

        assert result.exit_code == 0
        assert "ADD_TAGS" in result.output

    def test_main_without_mutations_flag(self) -> None:
        """Test disabling mutations via CLI flag."""
        from click.testing import CliRunner

        from datahub.ai.snowflake.generate_udfs import main

        runner = CliRunner()
        result = runner.invoke(main, ["--no-enable-mutations"])

        assert result.exit_code == 0
        assert "ADD_TAGS" not in result.output
        assert "SEARCH_DATAHUB" in result.output
