"""
Unit tests for _TableName class and MSSQL temp table prefix restoration.

Tests the table name extraction and special handling for MSSQL temp tables.
The temp table prefix restoration is now in sqlglot_lineage.py and is
dialect-aware - only applying to MSSQL dialect.
"""

import sqlglot

from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.sqlglot_lineage import (
    _restore_mssql_temp_table_prefix,
    _table_name_from_sqlglot_table,
)
from datahub.sql_parsing.sqlglot_utils import get_dialect


class TestRestoreMssqlTempTablePrefix:
    """Tests for the _restore_mssql_temp_table_prefix helper function."""

    def _get_mssql_dialect(self) -> sqlglot.Dialect:
        return get_dialect("mssql")

    def _get_postgres_dialect(self) -> sqlglot.Dialect:
        return get_dialect("postgres")

    def test_mssql_no_flags_returns_unchanged(self):
        """MSSQL: Table name without temp flags should be returned unchanged."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="regular_table"),
        )
        result = _restore_mssql_temp_table_prefix(table, self._get_mssql_dialect())
        assert result == "regular_table"

    def test_mssql_local_temp_adds_hash_prefix(self):
        """MSSQL: Identifier with temporary=True should add # prefix."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="temptable", temporary=True),
        )
        result = _restore_mssql_temp_table_prefix(table, self._get_mssql_dialect())
        assert result == "#temptable"

    def test_mssql_global_temp_adds_double_hash_prefix(self):
        """MSSQL: Identifier with global=True should add ## prefix."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="globaltemp", **{"global": True}),
        )
        result = _restore_mssql_temp_table_prefix(table, self._get_mssql_dialect())
        assert result == "##globaltemp"

    def test_mssql_no_double_prefix_local(self):
        """MSSQL: Should not add # if already present."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="#already_prefixed", temporary=True),
        )
        result = _restore_mssql_temp_table_prefix(table, self._get_mssql_dialect())
        assert result == "#already_prefixed"

    def test_mssql_no_double_prefix_global(self):
        """MSSQL: Should not add ## if already present."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="##already_prefixed", **{"global": True}),
        )
        result = _restore_mssql_temp_table_prefix(table, self._get_mssql_dialect())
        assert result == "##already_prefixed"

    def test_mssql_global_takes_precedence_over_local(self):
        """MSSQL: If both global and temporary are set, global (##) takes precedence."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(
                this="temptable", temporary=True, **{"global": True}
            ),
        )
        result = _restore_mssql_temp_table_prefix(table, self._get_mssql_dialect())
        assert result == "##temptable"

    def test_non_mssql_dialect_returns_unchanged(self):
        """Non-MSSQL dialects should not add # prefix even with temporary flag."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="temptable", temporary=True),
        )
        result = _restore_mssql_temp_table_prefix(table, self._get_postgres_dialect())
        assert result == "temptable"

    def test_none_dialect_returns_unchanged(self):
        """None dialect should not add # prefix."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="temptable", temporary=True),
        )
        result = _restore_mssql_temp_table_prefix(table, None)
        assert result == "temptable"


class TestTableNameFromSqlglotTable:
    """Tests for _TableName.from_sqlglot_table() method (basic functionality)."""

    def test_basic_table_extraction(self):
        """Basic table name extraction should work."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="my_table"),
        )
        result = _TableName.from_sqlglot_table(table)
        assert result.table == "my_table"
        assert result.database is None
        assert result.db_schema is None

    def test_qualified_table_extraction(self):
        """Fully qualified table names should be extracted correctly."""
        table = sqlglot.exp.Table(
            catalog=sqlglot.exp.Identifier(this="my_db"),
            db=sqlglot.exp.Identifier(this="my_schema"),
            this=sqlglot.exp.Identifier(this="my_table"),
        )
        result = _TableName.from_sqlglot_table(table)
        assert result.table == "my_table"
        assert result.database == "my_db"
        assert result.db_schema == "my_schema"

    def test_default_db_and_schema(self):
        """Default db and schema should be applied when not present."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="my_table"),
        )
        result = _TableName.from_sqlglot_table(
            table, default_db="default_db", default_schema="default_schema"
        )
        assert result.table == "my_table"
        assert result.database == "default_db"
        assert result.db_schema == "default_schema"

    def test_explicit_overrides_default(self):
        """Explicit catalog/db should override defaults."""
        table = sqlglot.exp.Table(
            catalog=sqlglot.exp.Identifier(this="explicit_db"),
            db=sqlglot.exp.Identifier(this="explicit_schema"),
            this=sqlglot.exp.Identifier(this="my_table"),
        )
        result = _TableName.from_sqlglot_table(
            table, default_db="default_db", default_schema="default_schema"
        )
        assert result.database == "explicit_db"
        assert result.db_schema == "explicit_schema"


class TestTableNameFromSqlglotTableWithDialect:
    """Tests for _table_name_from_sqlglot_table() with dialect awareness."""

    def _get_mssql_dialect(self) -> sqlglot.Dialect:
        return get_dialect("mssql")

    def _get_postgres_dialect(self) -> sqlglot.Dialect:
        return get_dialect("postgres")

    def test_mssql_local_temp_gets_prefix(self):
        """MSSQL local temp table should get # prefix."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="temptable", temporary=True),
        )
        result = _table_name_from_sqlglot_table(table, self._get_mssql_dialect())
        assert result.table == "#temptable"

    def test_mssql_global_temp_gets_prefix(self):
        """MSSQL global temp table should get ## prefix."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="globaltemp", **{"global": True}),
        )
        result = _table_name_from_sqlglot_table(table, self._get_mssql_dialect())
        assert result.table == "##globaltemp"

    def test_postgres_temp_no_prefix(self):
        """PostgreSQL temp table should NOT get # prefix."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="temptable", temporary=True),
        )
        result = _table_name_from_sqlglot_table(table, self._get_postgres_dialect())
        assert result.table == "temptable"

    def test_none_dialect_no_prefix(self):
        """None dialect should NOT add # prefix."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="temptable", temporary=True),
        )
        result = _table_name_from_sqlglot_table(table, None)
        assert result.table == "temptable"


class TestMSSQLTempTableExtraction:
    """Tests for MSSQL local temporary table handling with real SQL parsing.

    MSSQL uses # prefix for local temp tables and ## for global temp tables.
    SQLGlot strips these prefixes but sets flags on the identifier:
    - Local temp (#): 'temporary' flag
    - Global temp (##): 'global' flag

    We need to restore the prefix for downstream temp table detection.
    """

    def _get_mssql_dialect(self) -> sqlglot.Dialect:
        return get_dialect("mssql")

    def test_mssql_local_temp_select_into(self):
        """MSSQL SELECT INTO #temptable should preserve # prefix."""
        sql = "SELECT key INTO #mytemptable FROM source_table"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [
            _table_name_from_sqlglot_table(t, self._get_mssql_dialect()).table
            for t in tables
        ]

        assert "#mytemptable" in table_names, (
            f"Expected #mytemptable, got {table_names}"
        )
        assert "source_table" in table_names

    def test_mssql_local_temp_select_from(self):
        """MSSQL SELECT FROM #temptable should preserve # prefix."""
        sql = "SELECT key FROM #mytemptable"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [
            _table_name_from_sqlglot_table(t, self._get_mssql_dialect()).table
            for t in tables
        ]

        assert "#mytemptable" in table_names

    def test_mssql_local_temp_create_table(self):
        """MSSQL CREATE TABLE #temp should preserve # prefix."""
        sql = "CREATE TABLE #mytemptable (id INT)"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [
            _table_name_from_sqlglot_table(t, self._get_mssql_dialect()).table
            for t in tables
        ]

        assert "#mytemptable" in table_names

    def test_mssql_global_temp_select_into(self):
        """MSSQL SELECT INTO ##globaltemp should preserve ## prefix."""
        sql = "SELECT key INTO ##globaltemp FROM source_table"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [
            _table_name_from_sqlglot_table(t, self._get_mssql_dialect()).table
            for t in tables
        ]

        assert "##globaltemp" in table_names, (
            f"Expected ##globaltemp, got {table_names}"
        )

    def test_mssql_global_temp_select_from(self):
        """MSSQL SELECT FROM ##globaltemp should preserve ## prefix."""
        sql = "SELECT key FROM ##globaltemp"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [
            _table_name_from_sqlglot_table(t, self._get_mssql_dialect()).table
            for t in tables
        ]

        assert "##globaltemp" in table_names

    def test_mssql_global_temp_create_table(self):
        """MSSQL CREATE TABLE ##global should preserve ## prefix."""
        sql = "CREATE TABLE ##globaltemp (id INT)"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [
            _table_name_from_sqlglot_table(t, self._get_mssql_dialect()).table
            for t in tables
        ]

        assert "##globaltemp" in table_names

    def test_mssql_full_lineage_scenario(self):
        """Test the full lineage scenario from the bug report.

        Query 1: SELECT INTO #mytemptable FROM myrawtable
        Query 2: SELECT INTO myprodtable FROM #mytemptable

        Expected: #mytemptable should be identified as temp in both queries.
        """
        dialect = self._get_mssql_dialect()

        # Query 1: Create temp table from raw table
        sql1 = "SELECT key INTO #mytemptable FROM [mydb].[myschema].myrawtable"
        parsed1 = sqlglot.parse(sql1, dialect="tsql")[0]
        assert parsed1 is not None

        tables1 = {
            _table_name_from_sqlglot_table(t, dialect).table
            for t in parsed1.find_all(sqlglot.exp.Table)
        }
        assert "#mytemptable" in tables1, "Query 1 should have #mytemptable"
        assert "myrawtable" in tables1, "Query 1 should have myrawtable"

        # Query 2: Create prod table from temp table
        sql2 = "SELECT key INTO [mydb].[myschema].myprodtable FROM #mytemptable"
        parsed2 = sqlglot.parse(sql2, dialect="tsql")[0]
        assert parsed2 is not None

        tables2 = {
            _table_name_from_sqlglot_table(t, dialect).table
            for t in parsed2.find_all(sqlglot.exp.Table)
        }
        assert "#mytemptable" in tables2, "Query 2 should have #mytemptable"
        assert "myprodtable" in tables2, "Query 2 should have myprodtable"

    def test_mssql_multipart_local_temp_table(self):
        """MSSQL multi-part temp table names like mydb.dbo.#staging should work."""
        sql = "SELECT * FROM mydb.dbo.#staging"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        assert len(tables) == 1

        table_name = _table_name_from_sqlglot_table(
            tables[0], self._get_mssql_dialect()
        )
        assert table_name.table == "#staging", (
            f"Expected #staging, got {table_name.table}"
        )
        assert table_name.database == "mydb"
        assert table_name.db_schema == "dbo"

    def test_mssql_multipart_global_temp_table(self):
        """MSSQL multi-part global temp table names like mydb.dbo.##staging should work."""
        sql = "SELECT * FROM mydb.dbo.##staging"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        assert len(tables) == 1

        table_name = _table_name_from_sqlglot_table(
            tables[0], self._get_mssql_dialect()
        )
        assert table_name.table == "##staging", (
            f"Expected ##staging, got {table_name.table}"
        )
        assert table_name.database == "mydb"
        assert table_name.db_schema == "dbo"

    def test_mssql_insert_into_multipart_temp(self):
        """MSSQL INSERT INTO mydb.dbo.#staging should preserve # prefix."""
        sql = "INSERT INTO mydb.dbo.#staging SELECT * FROM source_table"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = {
            _table_name_from_sqlglot_table(t, self._get_mssql_dialect()) for t in tables
        }

        # Find the temp table
        temp_tables = [t for t in table_names if t.table.startswith("#")]
        assert len(temp_tables) == 1
        assert temp_tables[0].table == "#staging"
        assert temp_tables[0].database == "mydb"
        assert temp_tables[0].db_schema == "dbo"

    def test_regular_table_not_affected(self):
        """Regular tables (without temporary flag) should not get # prefix."""
        sql = "SELECT * FROM regular_table"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        assert len(tables) == 1

        table_name = _table_name_from_sqlglot_table(
            tables[0], self._get_mssql_dialect()
        )
        assert table_name.table == "regular_table"
        assert not table_name.table.startswith("#")

    def test_is_temp_table_compatibility(self):
        """Verify is_temp_table() pattern works with restored prefix."""

        def is_temp_table_check(name: str) -> bool:
            parts = name.split(".")
            table_name = parts[-1]
            return table_name.startswith("#")

        dialect = self._get_mssql_dialect()

        # Local temp
        sql = "SELECT key INTO #mytemp FROM source"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None
        for t in parsed.find_all(sqlglot.exp.Table):
            table_name = _table_name_from_sqlglot_table(t, dialect)
            if table_name.table.startswith("#"):
                assert is_temp_table_check(table_name.table)

        # Global temp
        sql = "SELECT key INTO ##myglobal FROM source"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None
        for t in parsed.find_all(sqlglot.exp.Table):
            table_name = _table_name_from_sqlglot_table(t, dialect)
            if table_name.table.startswith("##"):
                assert is_temp_table_check(table_name.table)

    def test_mssql_temp_table_in_cte(self):
        """MSSQL temp table referenced in CTE should preserve # prefix."""
        sql = """
        WITH cte AS (
            SELECT * FROM #temp_source
        )
        SELECT * FROM cte JOIN #temp_target ON cte.id = #temp_target.id
        """
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        dialect = self._get_mssql_dialect()
        table_names = [
            _table_name_from_sqlglot_table(t, dialect).table
            for t in parsed.find_all(sqlglot.exp.Table)
        ]

        assert "#temp_source" in table_names
        assert "#temp_target" in table_names

    def test_mssql_temp_table_with_alias(self):
        """MSSQL temp table with alias should preserve # prefix."""
        sql = "SELECT t.* FROM #my_temp_table AS t"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        dialect = self._get_mssql_dialect()
        tables = list(parsed.find_all(sqlglot.exp.Table))
        assert len(tables) == 1

        table_name = _table_name_from_sqlglot_table(tables[0], dialect)
        assert table_name.table == "#my_temp_table"

    def test_mssql_temp_table_in_subquery(self):
        """MSSQL temp table in subquery should preserve # prefix."""
        sql = """
        SELECT * FROM regular_table
        WHERE id IN (SELECT id FROM #temp_filter)
        """
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        dialect = self._get_mssql_dialect()
        table_names = [
            _table_name_from_sqlglot_table(t, dialect).table
            for t in parsed.find_all(sqlglot.exp.Table)
        ]

        assert "regular_table" in table_names
        assert "#temp_filter" in table_names

    def test_mssql_temp_table_in_join(self):
        """MSSQL temp table in JOIN should preserve # prefix."""
        sql = """
        SELECT a.*, b.*
        FROM permanent_table a
        INNER JOIN #staging_data b ON a.id = b.id
        LEFT JOIN ##global_cache c ON b.cache_id = c.id
        """
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        dialect = self._get_mssql_dialect()
        table_names = [
            _table_name_from_sqlglot_table(t, dialect).table
            for t in parsed.find_all(sqlglot.exp.Table)
        ]

        assert "permanent_table" in table_names
        assert "#staging_data" in table_names
        assert "##global_cache" in table_names


class TestRedshiftTempTableExtraction:
    """Tests for Redshift temporary table handling.

    Redshift handles temp tables differently from MSSQL:
    - CREATE TABLE #name: sqlglot preserves # in the name (no temporary flag)
    - CREATE TEMP TABLE name: sqlglot sets TemporaryProperty but no # needed

    Since our prefix restoration only applies to MSSQL, Redshift should work as-is.
    """

    def _get_redshift_dialect(self) -> sqlglot.Dialect:
        return get_dialect("redshift")

    def test_redshift_hash_prefix_table(self):
        """Redshift CREATE TABLE #name preserves # prefix (no flag needed)."""
        sql = "CREATE TABLE #staging AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="redshift")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [
            _table_name_from_sqlglot_table(t, self._get_redshift_dialect()).table
            for t in tables
        ]

        assert "#staging" in table_names, (
            f"Redshift # prefix should be preserved: {table_names}"
        )

    def test_redshift_temp_keyword_table(self):
        """Redshift CREATE TEMP TABLE name works without # prefix."""
        sql = "CREATE TEMP TABLE staging AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="redshift")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [
            _table_name_from_sqlglot_table(t, self._get_redshift_dialect()).table
            for t in tables
        ]

        # TEMP keyword creates temp table without # prefix
        assert "staging" in table_names


class TestOtherDialectsTempTables:
    """Tests to ensure other dialects don't get # prefix added.

    Only MSSQL uses the temporary/global flags that we check.
    Other dialects use TEMPORARY keyword which doesn't result in these flags.
    """

    def test_postgresql_temp_table_no_hash(self):
        """PostgreSQL temp tables should NOT get # prefix."""
        sql = "CREATE TEMPORARY TABLE my_temp AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="postgres")[0]
        assert parsed is not None

        dialect = get_dialect("postgres")
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_table_name_from_sqlglot_table(t, dialect).table for t in tables]

        assert not any(t.startswith("#") for t in table_names), (
            f"PostgreSQL should not add # prefix: {table_names}"
        )

    def test_snowflake_temp_table_no_hash(self):
        """Snowflake temp tables should NOT get # prefix."""
        sql = "CREATE TEMPORARY TABLE my_temp AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="snowflake")[0]
        assert parsed is not None

        dialect = get_dialect("snowflake")
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_table_name_from_sqlglot_table(t, dialect).table for t in tables]

        assert not any(t.startswith("#") for t in table_names), (
            f"Snowflake should not add # prefix: {table_names}"
        )

    def test_bigquery_temp_table_no_hash(self):
        """BigQuery temp tables should NOT get # prefix."""
        sql = "CREATE TEMP TABLE my_temp AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
        assert parsed is not None

        dialect = get_dialect("bigquery")
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_table_name_from_sqlglot_table(t, dialect).table for t in tables]

        assert not any(t.startswith("#") for t in table_names), (
            f"BigQuery should not add # prefix: {table_names}"
        )

    def test_hive_temp_table_no_hash(self):
        """Hive temp tables should NOT get # prefix."""
        sql = "CREATE TEMPORARY TABLE my_temp AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="hive")[0]
        assert parsed is not None

        dialect = get_dialect("hive")
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_table_name_from_sqlglot_table(t, dialect).table for t in tables]

        assert not any(t.startswith("#") for t in table_names), (
            f"Hive should not add # prefix: {table_names}"
        )


class TestDotExpressionTableNames:
    """Tests for table names with more than 3 parts (Dot expressions).

    When a table has more than 3 parts (e.g., a.b.c.d.table), SQLGlot
    represents the table name as a Dot expression rather than a simple
    Identifier. We need to handle this case for temp table detection.
    """

    def _get_mssql_dialect(self) -> sqlglot.Dialect:
        return get_dialect("mssql")

    def test_multipart_dot_table_name(self):
        """Table with >3 parts should merge into table name."""
        # Construct a Dot expression: a.b.tablename
        dot_expr = sqlglot.exp.Dot(
            this=sqlglot.exp.Identifier(this="a"),
            expression=sqlglot.exp.Dot(
                this=sqlglot.exp.Identifier(this="b"),
                expression=sqlglot.exp.Identifier(this="tablename"),
            ),
        )
        table = sqlglot.exp.Table(this=dot_expr)

        result = _table_name_from_sqlglot_table(table, self._get_mssql_dialect())
        assert result.table == "a.b.tablename"

    def test_multipart_dot_local_temp_table(self):
        """Dot expression with temporary flag should get # prefix."""
        # Construct a Dot expression where the final identifier has temporary=True
        dot_expr = sqlglot.exp.Dot(
            this=sqlglot.exp.Identifier(this="a"),
            expression=sqlglot.exp.Dot(
                this=sqlglot.exp.Identifier(this="b"),
                expression=sqlglot.exp.Identifier(this="temptable", temporary=True),
            ),
        )
        table = sqlglot.exp.Table(this=dot_expr)

        result = _table_name_from_sqlglot_table(table, self._get_mssql_dialect())
        assert result.table == "a.b.#temptable", (
            f"Expected a.b.#temptable, got {result.table}"
        )

    def test_multipart_dot_global_temp_table(self):
        """Dot expression with global flag should get ## prefix."""
        # Construct a Dot expression where the final identifier has global=True
        dot_expr = sqlglot.exp.Dot(
            this=sqlglot.exp.Identifier(this="a"),
            expression=sqlglot.exp.Dot(
                this=sqlglot.exp.Identifier(this="b"),
                expression=sqlglot.exp.Identifier(
                    this="globaltemp", **{"global": True}
                ),
            ),
        )
        table = sqlglot.exp.Table(this=dot_expr)

        result = _table_name_from_sqlglot_table(table, self._get_mssql_dialect())
        assert result.table == "a.b.##globaltemp", (
            f"Expected a.b.##globaltemp, got {result.table}"
        )

    def test_multipart_dot_no_temp_flags(self):
        """Dot expression without temp flags should not get # prefix."""
        dot_expr = sqlglot.exp.Dot(
            this=sqlglot.exp.Identifier(this="part1"),
            expression=sqlglot.exp.Dot(
                this=sqlglot.exp.Identifier(this="part2"),
                expression=sqlglot.exp.Identifier(this="regular_table"),
            ),
        )
        table = sqlglot.exp.Table(this=dot_expr)

        result = _table_name_from_sqlglot_table(table, self._get_mssql_dialect())
        assert result.table == "part1.part2.regular_table"
        assert not result.table.endswith("#regular_table")

    def test_multipart_dot_non_mssql_no_prefix(self):
        """Dot expression with temporary flag but non-MSSQL dialect should not get prefix."""
        dot_expr = sqlglot.exp.Dot(
            this=sqlglot.exp.Identifier(this="a"),
            expression=sqlglot.exp.Dot(
                this=sqlglot.exp.Identifier(this="b"),
                expression=sqlglot.exp.Identifier(this="temptable", temporary=True),
            ),
        )
        table = sqlglot.exp.Table(this=dot_expr)

        postgres_dialect = get_dialect("postgres")
        result = _table_name_from_sqlglot_table(table, postgres_dialect)
        assert result.table == "a.b.temptable", (
            f"Expected a.b.temptable (no # prefix for Postgres), got {result.table}"
        )

    def test_mssql_4part_temp_table_real_sql(self):
        """Test 4-part temp table name with real SQL parsing.

        4-part names (server.database.schema.table) create Dot expressions
        in SQLGlot, which exercises the Dot branch in _table_name_from_sqlglot_table.
        """
        sql = "SELECT * FROM server.mydb.dbo.#staging"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        assert len(tables) == 1

        # Verify this creates a Dot expression (4-part name)
        assert isinstance(tables[0].this, sqlglot.exp.Dot)

        table_name = _table_name_from_sqlglot_table(
            tables[0], self._get_mssql_dialect()
        )
        # The # prefix should be restored in the merged table name
        assert "#staging" in table_name.table, (
            f"Expected #staging in table name, got {table_name.table}"
        )

    def test_mssql_4part_global_temp_table_real_sql(self):
        """Test 4-part global temp table name with real SQL parsing."""
        sql = "SELECT * FROM server.mydb.dbo.##globaltemp"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        assert len(tables) == 1

        # Verify this creates a Dot expression
        assert isinstance(tables[0].this, sqlglot.exp.Dot)

        table_name = _table_name_from_sqlglot_table(
            tables[0], self._get_mssql_dialect()
        )
        # The ## prefix should be restored
        assert "##globaltemp" in table_name.table, (
            f"Expected ##globaltemp in table name, got {table_name.table}"
        )


class TestTableNameEquality:
    """Tests for _TableName equality and hashing."""

    def test_same_table_names_are_equal(self):
        """Same table names should be equal."""
        t1 = _TableName(database="db", db_schema="schema", table="table")
        t2 = _TableName(database="db", db_schema="schema", table="table")
        assert t1 == t2

    def test_different_table_names_not_equal(self):
        """Different table names should not be equal."""
        t1 = _TableName(database="db", db_schema="schema", table="table1")
        t2 = _TableName(database="db", db_schema="schema", table="table2")
        assert t1 != t2

    def test_temp_table_different_from_regular(self):
        """Temp table (#table) should be different from regular table."""
        temp = _TableName(database="db", db_schema="schema", table="#mytable")
        regular = _TableName(database="db", db_schema="schema", table="mytable")
        assert temp != regular

    def test_table_name_hashable(self):
        """_TableName should be hashable for use in sets/dicts."""
        t1 = _TableName(database="db", db_schema="schema", table="table")
        t2 = _TableName(database="db", db_schema="schema", table="#temptable")

        # Should be able to use in set
        table_set = {t1, t2}
        assert len(table_set) == 2

        # Same table should hash to same value
        t3 = _TableName(database="db", db_schema="schema", table="table")
        assert hash(t1) == hash(t3)


class TestTableNameQualified:
    """Tests for _TableName.qualified() method."""

    def test_qualified_adds_defaults(self):
        """qualified() should add default db/schema if not present."""
        table = _TableName(table="my_table")
        qualified = table.qualified(
            dialect=get_dialect("mssql"),
            default_db="default_db",
            default_schema="default_schema",
        )
        assert qualified.database == "default_db"
        assert qualified.db_schema == "default_schema"
        assert qualified.table == "my_table"

    def test_qualified_preserves_temp_prefix(self):
        """qualified() should preserve # prefix on temp tables."""
        table = _TableName(table="#temptable")
        qualified = table.qualified(
            dialect=get_dialect("mssql"),
            default_db="mydb",
            default_schema="dbo",
        )
        assert qualified.table == "#temptable"
        assert qualified.database == "mydb"
        assert qualified.db_schema == "dbo"
