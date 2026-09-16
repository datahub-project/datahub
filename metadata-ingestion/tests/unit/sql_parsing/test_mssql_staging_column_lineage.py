"""
Test for MSSQL stored procedure column lineage with cross-database references.

This test reproduces the issue where column lineage from Staging database
to TimeSeries/CurrentData databases was missing.
"""

import pytest

from datahub.ingestion.source.sql.stored_procedures.lineage import parse_procedure_code
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver


def _make_schema_field(field_path: str) -> SchemaFieldClass:
    """Helper to create a schema field."""
    return SchemaFieldClass(
        fieldPath=field_path,
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="VARCHAR(100)",
    )


@pytest.fixture
def schema_resolver_with_all_dbs():
    """Create a schema resolver that knows about Staging, TimeSeries, and CurrentData tables."""
    schema_resolver = SchemaResolver(
        platform="mssql",
        platform_instance=None,
        env="PROD",
    )

    # Add Staging tables
    schema_resolver.add_schema_metadata(
        urn="urn:li:dataset:(urn:li:dataPlatform:mssql,staging.dbo.source_table,PROD)",
        schema_metadata=SchemaMetadataClass(
            schemaName="source_table",
            platform="urn:li:dataPlatform:mssql",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=[
                _make_schema_field("id"),
                _make_schema_field("date_field"),
                _make_schema_field("value_field"),
                _make_schema_field("action_flag"),
            ],
        ),
    )

    # Add TimeSeries tables
    schema_resolver.add_schema_metadata(
        urn="urn:li:dataset:(urn:li:dataPlatform:mssql,timeseries.dbo.fact_table,PROD)",
        schema_metadata=SchemaMetadataClass(
            schemaName="fact_table",
            platform="urn:li:dataPlatform:mssql",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=[
                _make_schema_field("id"),
                _make_schema_field("date_field"),
                _make_schema_field("metric_value"),
            ],
        ),
    )

    # Add CurrentData tables
    schema_resolver.add_schema_metadata(
        urn="urn:li:dataset:(urn:li:dataPlatform:mssql,currentdata.dbo.fact_table,PROD)",
        schema_metadata=SchemaMetadataClass(
            schemaName="fact_table",
            platform="urn:li:dataPlatform:mssql",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=[
                _make_schema_field("id"),
                _make_schema_field("date_field"),
                _make_schema_field("metric_value"),
            ],
        ),
    )

    return schema_resolver


def test_cross_database_column_lineage_with_staging(schema_resolver_with_all_dbs):
    """
    Test that column lineage from Staging to TimeSeries/CurrentData is preserved.

    This reproduces an issue where:
    - Table-level lineage: Staging → TimeSeries → CurrentData (works)
    - Column-level lineage: Missing Staging.id → TimeSeries/CurrentData
    """
    procedure_code = """
    CREATE PROCEDURE dbo.update_fact_table
    @IsSnapshot VARCHAR(5) = 'NO'
    AS
    BEGIN
        SET NOCOUNT ON;
        BEGIN TRY
            -- Insert from Staging to TimeSeries
            INSERT INTO TimeSeries.dbo.fact_table
                (id, date_field, metric_value)
            SELECT
                src.id,
                src.date_field,
                src.value_field
            FROM Staging.dbo.source_table src WITH (TABLOCK)
            LEFT JOIN TimeSeries.dbo.fact_table dst
              ON src.id = dst.id
              AND src.date_field = dst.date_field
            WHERE (dst.id IS NULL OR dst.date_field IS NULL)
              AND (src.action_flag IS NULL OR (src.action_flag <> 'DELETE'));

            -- Create temp table from TimeSeries
            DROP TABLE IF EXISTS #TEMP;
            WITH TIME_SERIES AS (
                SELECT
                    id,
                    date_field,
                    metric_value,
                    ordinal = ROW_NUMBER() OVER (
                        PARTITION BY id
                        ORDER BY date_field DESC
                    )
                FROM TimeSeries.dbo.fact_table
                WHERE id IN (
                    SELECT id
                    FROM Staging.dbo.source_table src WITH (TABLOCK)
                )
            )
            SELECT
                id,
                date_field,
                metric_value
            INTO #TEMP
            FROM TIME_SERIES
            WHERE ordinal = 1;

            -- Insert from #TEMP to CurrentData
            INSERT INTO CurrentData.dbo.fact_table
                (id, date_field, metric_value)
            SELECT
                src.id,
                src.date_field,
                src.metric_value
            FROM #TEMP src
            LEFT JOIN CurrentData.dbo.fact_table dst
              ON src.id = dst.id
            WHERE dst.id IS NULL;
        END TRY
        BEGIN CATCH
            DECLARE @ErrorMessage NVARCHAR(4000);
            SELECT @ErrorMessage = ERROR_MESSAGE();
            THROW;
        END CATCH;
    END;
    """

    result = parse_procedure_code(
        schema_resolver=schema_resolver_with_all_dbs,
        default_db="Staging",
        default_schema="dbo",
        code=procedure_code,
        is_temp_table=lambda table: table.startswith("#"),
    )

    assert result is not None
    assert result.inputDatasets is not None
    assert result.outputDatasets is not None
    assert result.fineGrainedLineages is not None

    # Check table-level lineage
    input_tables = {urn.lower() for urn in result.inputDatasets}
    output_tables = {urn.lower() for urn in result.outputDatasets}

    # Should have Staging as input
    assert any("staging.dbo.source_table" in urn for urn in input_tables), (
        f"Staging table not in inputs: {input_tables}"
    )

    # Should have TimeSeries and CurrentData as outputs
    assert any("timeseries.dbo.fact_table" in urn for urn in output_tables), (
        f"TimeSeries table not in outputs: {output_tables}"
    )
    assert any("currentdata.dbo.fact_table" in urn for urn in output_tables), (
        f"CurrentData table not in outputs: {output_tables}"
    )

    # Check column-level lineage
    # Extract all column lineage paths
    import re

    column_lineage_paths = []
    field_urn_pattern = re.compile(r"urn:li:schemaField:\((.*),(.*)\)")

    for cll in result.fineGrainedLineages:
        for upstream_field in cll.upstreams or []:
            for downstream_field in cll.downstreams or []:
                # Extract table and column from field URNs
                # Format: urn:li:schemaField:(TABLE_URN,COLUMN)
                upstream_match = field_urn_pattern.search(upstream_field)
                downstream_match = field_urn_pattern.search(downstream_field)

                if upstream_match and downstream_match:
                    upstream_table = upstream_match.group(1).lower()
                    upstream_col = upstream_match.group(2).lower()
                    downstream_table = downstream_match.group(1).lower()
                    downstream_col = downstream_match.group(2).lower()
                    column_lineage_paths.append(
                        {
                            "upstream_table": upstream_table,
                            "upstream_col": upstream_col,
                            "downstream_table": downstream_table,
                            "downstream_col": downstream_col,
                        }
                    )

    # CRITICAL: Should have Staging → TimeSeries column lineage
    staging_to_timeseries = [
        path
        for path in column_lineage_paths
        if "source_table" in path["upstream_table"]
        and "timeseries" in path["downstream_table"]
        and "id" in path["upstream_col"]
        and "id" in path["downstream_col"]
    ]
    assert len(staging_to_timeseries) > 0, (
        f"Missing Staging → TimeSeries column lineage. All paths: {column_lineage_paths}"
    )

    # Should have TimeSeries → #TEMP column lineage
    timeseries_to_temp = [
        path
        for path in column_lineage_paths
        if "timeseries" in path["upstream_table"]
        and "temp" in path["downstream_table"]
        and "id" in path["upstream_col"]
        and "id" in path["downstream_col"]
    ]
    assert len(timeseries_to_temp) > 0, (
        f"Missing TimeSeries → #TEMP column lineage. All paths: {column_lineage_paths}"
    )

    # Should have #TEMP → CurrentData column lineage
    temp_to_currentdata = [
        path
        for path in column_lineage_paths
        if "temp" in path["upstream_table"]
        and "currentdata" in path["downstream_table"]
        and "id" in path["upstream_col"]
        and "id" in path["downstream_col"]
    ]
    assert len(temp_to_currentdata) > 0, (
        f"Missing #TEMP → CurrentData column lineage. All paths: {column_lineage_paths}"
    )

    # Should have fully qualified table names (3 parts: database.schema.table)
    # Note: temp tables like #TEMP will have staging.dbo.temp (3 parts)
    for path in column_lineage_paths:
        upstream_table_parts = path["upstream_table"].count(".")
        downstream_table_parts = path["downstream_table"].count(".")
        assert upstream_table_parts >= 2, (
            f"Upstream table should have 3+ parts (database.schema.table), "
            f"got: {path['upstream_table']}"
        )
        assert downstream_table_parts >= 2, (
            f"Downstream table should have 3+ parts (database.schema.table), "
            f"got: {path['downstream_table']}"
        )
