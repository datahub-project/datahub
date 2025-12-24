"""Unit tests for temporary table filtering logic."""

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.source.sql.mysql import TEMP_TABLE_PATTERN
from datahub.ingestion.source.sql.oracle import ORACLE_TEMP_TABLE_PATTERN
from datahub.ingestion.source.sql.postgres import POSTGRES_TEMP_TABLE_PATTERN
from datahub.ingestion.source.sql.stored_procedures.base import (
    _filter_temp_tables_from_lineage,
)
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)


def test_temp_table_pattern_basic():
    """Test TEMP_TABLE_PATTERN regex with basic CREATE TEMPORARY TABLE."""
    sql = "CREATE TEMPORARY TABLE temp_results (id INT, value VARCHAR(100));"
    matches = TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "temp_results")


def test_temp_table_pattern_with_database():
    """Test TEMP_TABLE_PATTERN with database-qualified temp table."""
    sql = "CREATE TEMPORARY TABLE mydb.temp_staging (data TEXT);"
    matches = TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("mydb", "temp_staging")


def test_temp_table_pattern_if_not_exists():
    """Test TEMP_TABLE_PATTERN with IF NOT EXISTS clause."""
    sql = "CREATE TEMPORARY TABLE IF NOT EXISTS temp_cache (key VARCHAR(255));"
    matches = TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "temp_cache")


def test_temp_table_pattern_global_temporary():
    """Test TEMP_TABLE_PATTERN with GLOBAL TEMPORARY TABLE."""
    sql = "CREATE GLOBAL TEMPORARY TABLE session_data (session_id INT);"
    matches = TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "session_data")


def test_temp_table_pattern_multiple_tables():
    """Test TEMP_TABLE_PATTERN with multiple temp tables in one procedure."""
    sql = """
    CREATE TEMPORARY TABLE temp1 (id INT);
    INSERT INTO temp1 SELECT * FROM source;
    CREATE TEMPORARY TABLE temp2 (id INT);
    INSERT INTO temp2 SELECT * FROM temp1;
    """
    matches = TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 2
    table_names = [match[1] for match in matches]
    assert "temp1" in table_names
    assert "temp2" in table_names


def test_temp_table_pattern_case_insensitive():
    """Test TEMP_TABLE_PATTERN is case-insensitive."""
    sql_variations = [
        "CREATE TEMPORARY TABLE TempData (x INT);",
        "create temporary table tempdata (x INT);",
        "Create Temporary Table TEMPDATA (x INT);",
    ]

    for sql in sql_variations:
        matches = TEMP_TABLE_PATTERN.findall(sql)
        assert len(matches) == 1


def test_temp_table_pattern_with_backticks():
    """Test TEMP_TABLE_PATTERN with MySQL backtick identifiers."""
    sql = "CREATE TEMPORARY TABLE `temp_results` (id INT);"
    matches = TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "temp_results")


def test_temp_table_pattern_database_with_backticks():
    """Test TEMP_TABLE_PATTERN with backticked database and table names."""
    sql = "CREATE TEMPORARY TABLE `mydb`.`temp_staging` (data TEXT);"
    matches = TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("mydb", "temp_staging")


def test_temp_table_pattern_not_regular_table():
    """Test TEMP_TABLE_PATTERN doesn't match regular CREATE TABLE."""
    sql = "CREATE TABLE permanent_table (id INT);"
    matches = TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 0


def test_filter_temp_tables_from_lineage_basic():
    """Test filtering temp tables from input/output datasets."""
    lineage = DataJobInputOutputClass(
        inputDatasets=[
            make_dataset_urn("mysql", "db.real_table", "PROD"),
            make_dataset_urn("mysql", "db.temp_staging", "PROD"),
            make_dataset_urn("mysql", "db.another_real", "PROD"),
        ],
        outputDatasets=[
            make_dataset_urn("mysql", "db.output_table", "PROD"),
            make_dataset_urn("mysql", "db.tmp_output", "PROD"),
        ],
    )

    def is_temp(table_name: str) -> bool:
        return "temp" in table_name.lower() or "tmp" in table_name.lower()

    filtered = _filter_temp_tables_from_lineage(lineage, is_temp)

    assert len(filtered.inputDatasets) == 2
    assert "temp_staging" not in str(filtered.inputDatasets)
    assert "real_table" in str(filtered.inputDatasets)
    assert "another_real" in str(filtered.inputDatasets)

    assert len(filtered.outputDatasets) == 1
    assert "tmp_output" not in str(filtered.outputDatasets)
    assert "output_table" in str(filtered.outputDatasets)


def test_filter_temp_tables_empty_lineage():
    """Test filtering with empty lineage."""
    lineage = DataJobInputOutputClass(
        inputDatasets=[],
        outputDatasets=[],
    )

    def is_temp(table_name: str) -> bool:
        return "temp" in table_name.lower()

    filtered = _filter_temp_tables_from_lineage(lineage, is_temp)

    assert filtered.inputDatasets == []
    assert filtered.outputDatasets == []


def test_filter_temp_tables_all_temp():
    """Test filtering when all tables are temp (edge case)."""
    lineage = DataJobInputOutputClass(
        inputDatasets=[
            make_dataset_urn("mysql", "db.temp1", "PROD"),
            make_dataset_urn("mysql", "db.temp2", "PROD"),
        ],
        outputDatasets=[
            make_dataset_urn("mysql", "db.temp_output", "PROD"),
        ],
    )

    def is_temp(table_name: str) -> bool:
        return "temp" in table_name.lower()

    filtered = _filter_temp_tables_from_lineage(lineage, is_temp)

    assert filtered.inputDatasets == []
    assert filtered.outputDatasets == []


def test_filter_temp_tables_fine_grained_lineage():
    """Test filtering temp tables from fine-grained (column-level) lineage."""
    lineage = DataJobInputOutputClass(
        inputDatasets=[
            make_dataset_urn("mysql", "db.real_table", "PROD"),
            make_dataset_urn("mysql", "db.temp_table", "PROD"),
        ],
        outputDatasets=[
            make_dataset_urn("mysql", "db.output", "PROD"),
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,db.real_table,PROD),col1)",
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,db.temp_table,PROD),col2)",
                ],
                downstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,db.output,PROD),result)",
                ],
            )
        ],
    )

    def is_temp(table_name: str) -> bool:
        return "temp" in table_name.lower()

    filtered = _filter_temp_tables_from_lineage(lineage, is_temp)

    # Dataset level should be filtered
    assert len(filtered.inputDatasets) == 1
    assert "real_table" in str(filtered.inputDatasets)
    assert "temp_table" not in str(filtered.inputDatasets)

    # Fine-grained lineage should also be filtered
    assert filtered.fineGrainedLineages is not None
    assert len(filtered.fineGrainedLineages) == 1
    fgl = filtered.fineGrainedLineages[0]
    assert fgl.upstreams is not None
    assert len(fgl.upstreams) == 1
    assert "real_table" in fgl.upstreams[0]
    assert "temp_table" not in str(fgl.upstreams)


def test_filter_temp_tables_removes_empty_fine_grained():
    """Test that fine-grained lineage entries with no remaining fields are removed."""
    lineage = DataJobInputOutputClass(
        inputDatasets=[
            make_dataset_urn("mysql", "db.temp1", "PROD"),
        ],
        outputDatasets=[
            make_dataset_urn("mysql", "db.output", "PROD"),
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,db.temp1,PROD),col1)",
                ],
                downstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,db.output,PROD),result)",
                ],
            )
        ],
    )

    def is_temp(table_name: str) -> bool:
        return "temp" in table_name.lower()

    filtered = _filter_temp_tables_from_lineage(lineage, is_temp)

    # After filtering all temp upstreams, the fine-grained lineage should be removed
    assert filtered.fineGrainedLineages == []


def test_filter_temp_tables_preserves_legitimate_tables():
    """Test that tables with 'temp' in legitimate names aren't filtered incorrectly."""
    lineage = DataJobInputOutputClass(
        inputDatasets=[
            make_dataset_urn("mysql", "db.temperature_readings", "PROD"),
            make_dataset_urn("mysql", "db.template_config", "PROD"),
            make_dataset_urn("mysql", "db.attempt_log", "PROD"),
        ],
        outputDatasets=[],
    )

    # Only filter exact temp table names from a known set
    temp_tables = {"temp_staging", "tmp_results"}

    def is_temp(table_name: str) -> bool:
        table_only = table_name.split(".")[-1]
        return table_only in temp_tables

    filtered = _filter_temp_tables_from_lineage(lineage, is_temp)

    # All legitimate tables should remain
    assert len(filtered.inputDatasets) == 3
    assert "temperature_readings" in str(filtered.inputDatasets)
    assert "template_config" in str(filtered.inputDatasets)
    assert "attempt_log" in str(filtered.inputDatasets)


def test_filter_temp_tables_schema_qualified_names():
    """Test filtering with schema-qualified table names."""
    lineage = DataJobInputOutputClass(
        inputDatasets=[
            make_dataset_urn("postgres", "mydb.public.users", "PROD"),
            make_dataset_urn("postgres", "mydb.staging.temp_import", "PROD"),
        ],
        outputDatasets=[],
    )

    def is_temp(table_name: str) -> bool:
        # Extract just the table name (last component)
        table_only = table_name.split(".")[-1]
        return table_only.startswith("temp_")

    filtered = _filter_temp_tables_from_lineage(lineage, is_temp)

    assert len(filtered.inputDatasets) == 1
    assert "users" in str(filtered.inputDatasets)
    assert "temp_import" not in str(filtered.inputDatasets)


# PostgreSQL Pattern Tests
def test_postgres_temp_table_pattern_basic():
    """Test POSTGRES_TEMP_TABLE_PATTERN with basic CREATE TEMP TABLE."""
    sql = "CREATE TEMP TABLE temp_results (id INT, value VARCHAR(100));"
    matches = POSTGRES_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "temp_results")


def test_postgres_temp_table_pattern_temporary():
    """Test POSTGRES_TEMP_TABLE_PATTERN with CREATE TEMPORARY TABLE."""
    sql = "CREATE TEMPORARY TABLE temp_staging (data TEXT);"
    matches = POSTGRES_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "temp_staging")


def test_postgres_temp_table_pattern_with_schema():
    """Test POSTGRES_TEMP_TABLE_PATTERN with schema-qualified temp table."""
    sql = "CREATE TEMP TABLE public.temp_cache (key VARCHAR(255));"
    matches = POSTGRES_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("public", "temp_cache")


def test_postgres_temp_table_pattern_if_not_exists():
    """Test POSTGRES_TEMP_TABLE_PATTERN with IF NOT EXISTS clause."""
    sql = "CREATE TEMP TABLE IF NOT EXISTS session_data (session_id INT);"
    matches = POSTGRES_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "session_data")


def test_postgres_temp_table_pattern_with_quotes():
    """Test POSTGRES_TEMP_TABLE_PATTERN with double-quoted identifiers."""
    sql = 'CREATE TEMP TABLE "temp_results" (id INT);'
    matches = POSTGRES_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "temp_results")


def test_postgres_temp_table_pattern_not_regular_table():
    """Test POSTGRES_TEMP_TABLE_PATTERN doesn't match regular CREATE TABLE."""
    sql = "CREATE TABLE permanent_table (id INT);"
    matches = POSTGRES_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 0


# Oracle Pattern Tests
def test_oracle_temp_table_pattern_basic():
    """Test ORACLE_TEMP_TABLE_PATTERN with basic CREATE GLOBAL TEMPORARY TABLE."""
    sql = "CREATE GLOBAL TEMPORARY TABLE temp_results (id NUMBER, value VARCHAR2(100));"
    matches = ORACLE_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "temp_results")


def test_oracle_temp_table_pattern_with_schema():
    """Test ORACLE_TEMP_TABLE_PATTERN with schema-qualified temp table."""
    sql = "CREATE GLOBAL TEMPORARY TABLE myschema.temp_staging (data CLOB);"
    matches = ORACLE_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("myschema", "temp_staging")


def test_oracle_temp_table_pattern_with_quotes():
    """Test ORACLE_TEMP_TABLE_PATTERN with double-quoted identifiers."""
    sql = 'CREATE GLOBAL TEMPORARY TABLE "TEMP_RESULTS" (id NUMBER);'
    matches = ORACLE_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 1
    assert matches[0] == ("", "TEMP_RESULTS")


def test_oracle_temp_table_pattern_case_insensitive():
    """Test ORACLE_TEMP_TABLE_PATTERN is case-insensitive."""
    sql_variations = [
        "CREATE GLOBAL TEMPORARY TABLE TempData (x NUMBER);",
        "create global temporary table tempdata (x NUMBER);",
        "Create Global Temporary Table TEMPDATA (x NUMBER);",
    ]

    for sql in sql_variations:
        matches = ORACLE_TEMP_TABLE_PATTERN.findall(sql)
        assert len(matches) == 1


def test_oracle_temp_table_pattern_not_regular_table():
    """Test ORACLE_TEMP_TABLE_PATTERN doesn't match regular CREATE TABLE."""
    sql = "CREATE TABLE permanent_table (id NUMBER);"
    matches = ORACLE_TEMP_TABLE_PATTERN.findall(sql)

    assert len(matches) == 0


def test_oracle_temp_table_pattern_not_local_temp():
    """Test ORACLE_TEMP_TABLE_PATTERN doesn't match CREATE TEMPORARY TABLE (non-GLOBAL)."""
    sql = "CREATE TEMPORARY TABLE temp_table (id NUMBER);"
    matches = ORACLE_TEMP_TABLE_PATTERN.findall(sql)

    # Oracle requires GLOBAL keyword, so this shouldn't match
    assert len(matches) == 0
