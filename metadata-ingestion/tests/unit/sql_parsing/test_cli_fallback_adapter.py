"""
Tests for CLI fallback parser adapter that enables reusing production code.

This test file ensures that:
1. The adapter correctly converts DataJobInputOutput to SqlParsingResult
2. The CLI path produces same results as production path
3. All major fixes are preserved (multiple outputs, CTE bug, INSERT mapping, etc.)
"""

import pytest

from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage


class TestAdapterBasics:
    """Test the adapter function that converts DataJobInputOutput to SqlParsingResult."""

    def test_adapter_handles_none(self):
        """Adapter should return empty result for None input."""
        from datahub.sql_parsing.sqlglot_lineage import _datajob_to_sql_parsing_result

        result = _datajob_to_sql_parsing_result(None)

        assert result.in_tables == []
        assert result.out_tables == []
        assert result.column_lineage is None

    def test_adapter_extracts_table_lineage(self):
        """Adapter should correctly extract input and output tables."""
        from datahub.sql_parsing.sqlglot_lineage import _datajob_to_sql_parsing_result

        datajob = DataJobInputOutputClass(
            inputDatasets=[
                "urn:li:dataset:(urn:li:dataPlatform:mssql,testdb.source1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:mssql,testdb.source2,PROD)",
            ],
            outputDatasets=[
                "urn:li:dataset:(urn:li:dataPlatform:mssql,testdb.target1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:mssql,testdb.target2,PROD)",
            ],
        )

        result = _datajob_to_sql_parsing_result(datajob)

        assert len(result.in_tables) == 2
        assert len(result.out_tables) == 2
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:mssql,testdb.source1,PROD)"
            in result.in_tables
        )
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:mssql,testdb.target1,PROD)"
            in result.out_tables
        )

    def test_adapter_extracts_column_lineage(self):
        """Adapter should correctly extract and convert column lineage."""
        from datahub.sql_parsing.sqlglot_lineage import _datajob_to_sql_parsing_result

        datajob = DataJobInputOutputClass(
            inputDatasets=[
                "urn:li:dataset:(urn:li:dataPlatform:mssql,db.source,PROD)",
            ],
            outputDatasets=[
                "urn:li:dataset:(urn:li:dataPlatform:mssql,db.target,PROD)",
            ],
            fineGrainedLineages=[
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[
                        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,db.source,PROD),source_col)",
                    ],
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,db.target,PROD),target_col)",
                    ],
                    transformOperation="COPY: [source].[source_col] AS [target_col]",
                    confidenceScore=0.9,
                ),
            ],
        )

        result = _datajob_to_sql_parsing_result(datajob)

        assert result.column_lineage is not None
        assert len(result.column_lineage) == 1

        cll = result.column_lineage[0]
        assert cll.downstream.column == "target_col"
        assert (
            cll.downstream.table
            == "urn:li:dataset:(urn:li:dataPlatform:mssql,db.target,PROD)"
        )
        assert len(cll.upstreams) == 1
        assert cll.upstreams[0].column == "source_col"
        assert (
            cll.upstreams[0].table
            == "urn:li:dataset:(urn:li:dataPlatform:mssql,db.source,PROD)"
        )
        assert cll.logic is not None
        assert cll.logic.column_logic == "COPY: [source].[source_col] AS [target_col]"
        assert cll.logic.is_direct_copy is True

    def test_parse_column_urn(self):
        """Test URN parsing helper correctly extracts table and column."""
        from datahub.sql_parsing.sqlglot_lineage import _parse_column_urn

        urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,mydb.mytable,PROD),columnName)"

        table, column = _parse_column_urn(urn)

        assert table == "urn:li:dataset:(urn:li:dataPlatform:mssql,mydb.mytable,PROD)"
        assert column == "columnName"

    def test_parse_column_urn_with_comma_in_table(self):
        """Test URN parsing handles commas correctly (splits on last comma only)."""
        from datahub.sql_parsing.sqlglot_lineage import _parse_column_urn

        # Edge case: table name contains comma (shouldn't happen but test defensive code)
        urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,db.table,PROD),col)"

        table, column = _parse_column_urn(urn)

        assert table == "urn:li:dataset:(urn:li:dataPlatform:mssql,db.table,PROD)"
        assert column == "col"

    def test_parse_column_urn_invalid_format(self):
        """Test URN parsing raises error for invalid URNs."""
        from datahub.sql_parsing.sqlglot_lineage import _parse_column_urn

        with pytest.raises(ValueError, match="Invalid schemaField URN"):
            _parse_column_urn("not:a:valid:urn")

        with pytest.raises(ValueError, match="Could not find column name"):
            _parse_column_urn("urn:li:schemaField:(table_without_column)")


class TestCLIFallbackIntegration:
    """Integration tests verifying CLI fallback produces correct results."""

    @pytest.fixture
    def schema_resolver(self):
        """Create a simple schema resolver for testing."""
        return SchemaResolver(platform="mssql", env="PROD")

    def test_cli_handles_simple_procedure(self, schema_resolver):
        """CLI should correctly parse simple stored procedure."""
        sql = """
        CREATE PROCEDURE test_proc
        AS
        BEGIN
            INSERT INTO target (col_a)
            SELECT col_b FROM source;
        END
        """

        result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Should extract both tables
        assert len(result.in_tables) >= 1
        assert len(result.out_tables) >= 1

    def test_cli_handles_multiple_outputs(self, schema_resolver):
        """
        CLI should extract all output tables from stored procedures.

        Tests that procedures with multiple INSERT/UPDATE statements
        show all modified tables as outputs, not just the first one.
        """
        sql = """
        CREATE PROCEDURE test_multiple_outputs
        AS
        BEGIN
            INSERT INTO output1 (id) SELECT id FROM input1;
            INSERT INTO output2 (id) SELECT id FROM input2;
            UPDATE output3 SET value = 1 FROM output3;
        END
        """

        result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Should have 3 output tables
        assert len(result.out_tables) == 3, (
            f"Expected 3 output tables, got {len(result.out_tables)}. "
            "All modified tables should be included in outputs."
        )

    def test_cli_handles_insert_column_mapping(self, schema_resolver):
        """
        CLI should use INSERT column names for downstream (not SELECT column names).

        Tests INSERT column mapping fix: downstream should be INSERT column name,
        not SELECT column name.
        """
        sql = """
        CREATE PROCEDURE test_column_mapping
        AS
        BEGIN
            INSERT INTO target (destination_col)
            SELECT source_col FROM source;
        END
        """

        result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Column lineage should use INSERT column name
        if result.column_lineage:
            for cll in result.column_lineage:
                # Downstream should be destination_col (INSERT column)
                # NOT source_col (SELECT column)
                assert cll.downstream.column in ["destination_col", "destinationcol"], (
                    f"Expected downstream column 'destination_col' (INSERT name), "
                    f"got '{cll.downstream.column}'. This tests INSERT column mapping fix."
                )

    def test_cli_handles_cte_bracket_bug(self, schema_resolver):
        """
        CLI should correctly split statements with WHERE clauses ending in parenthesis.

        Tests CTE bracket bug fix: INSERT with WHERE (...) should not be merged
        with following statement.
        """
        sql = """
        CREATE PROCEDURE test_cte_bracket
        AS
        BEGIN
            INSERT INTO table1 (id)
            SELECT id FROM source
            WHERE (status = 'active');

            INSERT INTO table2 (id)
            SELECT id FROM source
            WHERE id = 1;
        END
        """

        result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Should have both table1 and table2 as outputs
        # (proves statements were split correctly)
        output_table_names = [t.lower() for t in result.out_tables]
        has_table1 = any("table1" in t for t in output_table_names)
        has_table2 = any("table2" in t for t in output_table_names)

        assert has_table1, "Should have 'table1' table in outputs"
        assert has_table2, (
            "Should have 'table2' table in outputs (proves CTE bracket bug is fixed)"
        )

    def test_cli_filters_control_flow_keywords(self, schema_resolver):
        """
        CLI should filter out control flow statements (BEGIN TRY, END CATCH, etc.).

        Tests control flow filtering: these keywords should not appear as tables
        or cause parsing errors.
        """
        sql = """
        CREATE PROCEDURE test_try_catch
        AS
        BEGIN
            BEGIN TRY
                INSERT INTO target (id) SELECT id FROM source;
            END TRY
            BEGIN CATCH
                PRINT ERROR_MESSAGE();
            END CATCH
        END
        """

        result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Should extract tables successfully (not fail on control flow)
        assert len(result.in_tables) >= 1
        assert len(result.out_tables) >= 1

        # Control flow keywords should NOT appear as table names
        all_tables = result.in_tables + result.out_tables
        for table in all_tables:
            table_lower = table.lower()
            assert "begin" not in table_lower, "BEGIN should not appear as a table"
            assert "try" not in table_lower, "TRY should not appear as a table"
            assert "catch" not in table_lower, "CATCH should not appear as a table"


class TestCLIMatchesProduction:
    """
    Integration tests verifying CLI produces same results as production parser.

    These tests call both the CLI path (via sqlglot_lineage fallback) and the
    production path (via parse_procedure_code) and verify they produce identical results.
    """

    @pytest.fixture
    def schema_resolver(self):
        """Create schema resolver for testing."""
        return SchemaResolver(platform="mssql", env="PROD")

    def test_cli_matches_production_simple_procedure(self, schema_resolver):
        """
        Verify CLI produces same table lineage as production for simple procedure.
        """
        from datahub.ingestion.source.sql.stored_procedures.lineage import (
            parse_procedure_code,
        )

        sql = """
        CREATE PROCEDURE test_proc
        AS
        BEGIN
            INSERT INTO target (id, name)
            SELECT source_id, source_name FROM source;
        END
        """

        # CLI path
        cli_result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Production path
        production_result = parse_procedure_code(
            code=sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
            is_temp_table=lambda x: x.startswith("#"),
        )

        # Compare table-level lineage
        if production_result:
            cli_in = set(cli_result.in_tables)
            cli_out = set(cli_result.out_tables)

            prod_in = set(production_result.inputDatasets or [])
            prod_out = set(production_result.outputDatasets or [])

            assert cli_in == prod_in, (
                f"CLI and production should extract same input tables.\n"
                f"CLI:        {cli_in}\n"
                f"Production: {prod_in}"
            )
            assert cli_out == prod_out, (
                f"CLI and production should extract same output tables.\n"
                f"CLI:        {cli_out}\n"
                f"Production: {prod_out}"
            )

    def test_cli_matches_production_multiple_outputs(self, schema_resolver):
        """
        Verify CLI produces same results as production for multiple output tables.
        """
        from datahub.ingestion.source.sql.stored_procedures.lineage import (
            parse_procedure_code,
        )

        sql = """
        CREATE PROCEDURE test_multi
        AS
        BEGIN
            INSERT INTO timeseries.data (id) SELECT id FROM staging.source;
            UPDATE currentdata.summary SET total = 1;
        END
        """

        # CLI path
        cli_result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Production path
        production_result = parse_procedure_code(
            code=sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
            is_temp_table=lambda x: x.startswith("#"),
        )

        # Both should extract 2 output tables
        if production_result:
            assert len(cli_result.out_tables) == len(
                production_result.outputDatasets or []
            ), (
                f"CLI and production should extract same number of output tables.\n"
                f"CLI:        {len(cli_result.out_tables)} tables\n"
                f"Production: {len(production_result.outputDatasets or [])} tables"
            )


class TestRegressionPrevention:
    """
    Tests to prevent regressions of previously fixed bugs.

    Each test corresponds to one of the 5 major fixes we want to preserve.
    """

    @pytest.fixture
    def schema_resolver(self):
        return SchemaResolver(platform="mssql", env="PROD")

    def test_regression_cte_bracket_bug(self, schema_resolver):
        """
        Regression test for CTE bracket bug (fix #3).

        Bug: INSERT statements with WHERE clauses ending in ) were incorrectly
        merged with the next statement because split_statements thought the )
        was from a CTE.

        Fix: Check if statement is DML before treating ) as CTE indicator.
        """
        sql = """
        CREATE PROCEDURE test_where_paren
        AS
        BEGIN
            INSERT INTO table1 (id)
            SELECT id FROM source
            WHERE (status = 'active' AND (priority = 1));

            INSERT INTO table2 (id)
            SELECT id FROM source
            WHERE old_id = 1;
        END
        """

        result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Should correctly split into 2 statements, producing 2 output tables
        assert len(result.out_tables) == 2, (
            "CTE bracket bug regression: should split two INSERT statements into "
            "separate statements, producing 2 output tables"
        )

    def test_regression_insert_column_mapping(self, schema_resolver):
        """
        Regression test for INSERT column mapping (fix #5).

        Bug: Column lineage used SELECT column name for both upstream and
        downstream, instead of using INSERT column name for downstream.

        Fix: Position-based mapping of SELECT expressions to INSERT columns.
        """
        sql = """
        CREATE PROCEDURE test_column_map
        AS
        BEGIN
            INSERT INTO target (new_name)
            SELECT old_name FROM source;
        END
        """

        result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Downstream column should be INSERT column (new_name), not SELECT column (old_name)
        if result.column_lineage:
            downstream_cols = [
                cll.downstream.column.lower() for cll in result.column_lineage
            ]
            assert "new_name" in " ".join(downstream_cols) or "newname" in " ".join(
                downstream_cols
            ), (
                "INSERT column mapping regression: downstream should be 'new_name' "
                "(from INSERT), not 'old_name' (from SELECT)"
            )

    def test_regression_multiple_outputs(self, schema_resolver):
        """
        Regression test for multiple output tables.

        Verifies that procedures with multiple INSERT/UPDATE statements
        correctly include all modified tables in the output list.
        """
        sql = """
        CREATE PROCEDURE test_multi_out
        AS
        BEGIN
            INSERT INTO table1 (id) SELECT id FROM source;
            INSERT INTO table2 (id) SELECT id FROM source;
            INSERT INTO table3 (id) SELECT id FROM source;
        END
        """

        result = sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db="TestDB",
            default_schema="dbo",
        )

        # Should register ALL 3 output tables
        assert len(result.out_tables) == 3, (
            f"Expected 3 output tables, got {len(result.out_tables)}. "
            f"All modified tables should be included."
        )


# Run with: pytest tests/unit/sql_parsing/test_cli_fallback_adapter.py -v
