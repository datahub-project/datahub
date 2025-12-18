"""
Unit tests for _TableName class, especially temp table handling.

Tests the from_sqlglot_table method which extracts table names from
sqlglot parsed expressions, with special handling for MSSQL/Redshift
local temporary tables (prefixed with #).
"""

import sqlglot

from datahub.sql_parsing._models import _restore_temp_table_prefix, _TableName


class TestRestoreTempTablePrefix:
    """Tests for the _restore_temp_table_prefix helper function."""

    def test_no_flags_returns_unchanged(self):
        """Table name without temp flags should be returned unchanged."""
        identifier = sqlglot.exp.Identifier(this="regular_table")
        result = _restore_temp_table_prefix("regular_table", identifier)
        assert result == "regular_table"

    def test_local_temp_adds_hash_prefix(self):
        """Identifier with temporary=True should add # prefix."""
        identifier = sqlglot.exp.Identifier(this="temptable", temporary=True)
        result = _restore_temp_table_prefix("temptable", identifier)
        assert result == "#temptable"

    def test_global_temp_adds_double_hash_prefix(self):
        """Identifier with global=True should add ## prefix."""
        identifier = sqlglot.exp.Identifier(this="globaltemp", **{"global": True})
        result = _restore_temp_table_prefix("globaltemp", identifier)
        assert result == "##globaltemp"

    def test_no_double_prefix_local(self):
        """Should not add # if already present."""
        identifier = sqlglot.exp.Identifier(this="#already_prefixed", temporary=True)
        result = _restore_temp_table_prefix("#already_prefixed", identifier)
        assert result == "#already_prefixed"

    def test_no_double_prefix_global(self):
        """Should not add ## if already present."""
        identifier = sqlglot.exp.Identifier(
            this="##already_prefixed", **{"global": True}
        )
        result = _restore_temp_table_prefix("##already_prefixed", identifier)
        assert result == "##already_prefixed"

    def test_identifier_without_args_attr(self):
        """Non-identifier expression without args should return unchanged."""

        # Create a mock expression without args attribute
        class MockExprNoArgs:
            pass

        mock_expr = MockExprNoArgs()
        result = _restore_temp_table_prefix("table_name", mock_expr)  # type: ignore
        assert result == "table_name"

    def test_global_takes_precedence_over_local(self):
        """If both global and temporary are set, global (##) takes precedence."""
        identifier = sqlglot.exp.Identifier(
            this="temptable", temporary=True, **{"global": True}
        )
        result = _restore_temp_table_prefix("temptable", identifier)
        assert result == "##temptable"


class TestTableNameFromSqlglotTable:
    """Tests for _TableName.from_sqlglot_table() method."""

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


class TestMSSQLTempTableExtraction:
    """Tests for MSSQL local temporary table handling.

    MSSQL uses # prefix for local temp tables and ## for global temp tables.
    SQLGlot strips these prefixes but sets flags on the identifier:
    - Local temp (#): 'temporary' flag
    - Global temp (##): 'global_' flag

    We need to restore the prefix for downstream temp table detection.
    """

    def test_mssql_local_temp_select_into(self):
        """MSSQL SELECT INTO #temptable should preserve # prefix."""
        sql = "SELECT key INTO #mytemptable FROM source_table"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

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
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert "#mytemptable" in table_names

    def test_mssql_local_temp_create_table(self):
        """MSSQL CREATE TABLE #temp should preserve # prefix."""
        sql = "CREATE TABLE #mytemptable (id INT)"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert "#mytemptable" in table_names

    def test_mssql_global_temp_select_into(self):
        """MSSQL SELECT INTO ##globaltemp should preserve ## prefix."""
        sql = "SELECT key INTO ##globaltemp FROM source_table"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert "##globaltemp" in table_names, (
            f"Expected ##globaltemp, got {table_names}"
        )

    def test_mssql_global_temp_select_from(self):
        """MSSQL SELECT FROM ##globaltemp should preserve ## prefix."""
        sql = "SELECT key FROM ##globaltemp"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert "##globaltemp" in table_names

    def test_mssql_global_temp_create_table(self):
        """MSSQL CREATE TABLE ##global should preserve ## prefix."""
        sql = "CREATE TABLE ##globaltemp (id INT)"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert "##globaltemp" in table_names

    def test_mssql_full_lineage_scenario(self):
        """Test the full lineage scenario from the bug report.

        Query 1: SELECT INTO #mytemptable FROM myrawtable
        Query 2: SELECT INTO myprodtable FROM #mytemptable

        Expected: #mytemptable should be identified as temp in both queries.
        """
        # Query 1: Create temp table from raw table
        sql1 = "SELECT key INTO #mytemptable FROM [mydb].[myschema].myrawtable"
        parsed1 = sqlglot.parse(sql1, dialect="tsql")[0]
        assert parsed1 is not None

        tables1 = {
            _TableName.from_sqlglot_table(t).table
            for t in parsed1.find_all(sqlglot.exp.Table)
        }
        assert "#mytemptable" in tables1, "Query 1 should have #mytemptable"
        assert "myrawtable" in tables1, "Query 1 should have myrawtable"

        # Query 2: Create prod table from temp table
        sql2 = "SELECT key INTO [mydb].[myschema].myprodtable FROM #mytemptable"
        parsed2 = sqlglot.parse(sql2, dialect="tsql")[0]
        assert parsed2 is not None

        tables2 = {
            _TableName.from_sqlglot_table(t).table
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

        table_name = _TableName.from_sqlglot_table(tables[0])
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

        table_name = _TableName.from_sqlglot_table(tables[0])
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
        table_names = {_TableName.from_sqlglot_table(t) for t in tables}

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

        table_name = _TableName.from_sqlglot_table(tables[0])
        assert table_name.table == "regular_table"
        assert not table_name.table.startswith("#")

    def test_no_double_prefix_local_temp(self):
        """Table with existing # should not get another # added."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="#already_prefixed", temporary=True),
        )
        result = _TableName.from_sqlglot_table(table)
        assert result.table == "#already_prefixed"

    def test_no_double_prefix_global_temp(self):
        """Table with existing ## should not get another ## added."""
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="##already_prefixed", **{"global": True}),
        )
        result = _TableName.from_sqlglot_table(table)
        assert result.table == "##already_prefixed"

    def test_global_temp_prefix_added_when_missing(self):
        """Global temp table without ## prefix should get ## added.

        This tests the case where SQLGlot strips the ## prefix but sets
        the global_ flag. We need to restore the prefix.
        """
        # Create identifier WITHOUT ## prefix but WITH global_=True
        # This mimics what SQLGlot does when parsing ##globaltemp
        table = sqlglot.exp.Table(
            this=sqlglot.exp.Identifier(this="globaltemp", **{"global": True}),
        )
        result = _TableName.from_sqlglot_table(table)
        assert result.table == "##globaltemp", (
            f"Expected ##globaltemp, got {result.table}"
        )

    def test_is_temp_table_compatibility(self):
        """Verify is_temp_table() pattern works with restored prefix."""

        def is_temp_table_check(name: str) -> bool:
            parts = name.split(".")
            table_name = parts[-1]
            return table_name.startswith("#")

        # Local temp
        sql = "SELECT key INTO #mytemp FROM source"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None
        for t in parsed.find_all(sqlglot.exp.Table):
            table_name = _TableName.from_sqlglot_table(t)
            if table_name.table.startswith("#"):
                assert is_temp_table_check(table_name.table)

        # Global temp
        sql = "SELECT key INTO ##myglobal FROM source"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None
        for t in parsed.find_all(sqlglot.exp.Table):
            table_name = _TableName.from_sqlglot_table(t)
            if table_name.table.startswith("##"):
                assert is_temp_table_check(table_name.table)


class TestRedshiftTempTableExtraction:
    """Tests for Redshift temporary table handling.

    Redshift handles temp tables differently from MSSQL:
    - CREATE TABLE #name: sqlglot preserves # in the name (no temporary flag)
    - CREATE TEMP TABLE name: sqlglot sets TemporaryProperty but no # needed
    """

    def test_redshift_hash_prefix_table(self):
        """Redshift CREATE TABLE #name preserves # prefix (no flag needed)."""
        sql = "CREATE TABLE #staging AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="redshift")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert "#staging" in table_names, (
            f"Redshift # prefix should be preserved: {table_names}"
        )

    def test_redshift_temp_keyword_table(self):
        """Redshift CREATE TEMP TABLE name works without # prefix."""
        sql = "CREATE TEMP TABLE staging AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="redshift")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        # TEMP keyword creates temp table without # prefix
        assert "staging" in table_names


class TestOtherDialectsTempTables:
    """Tests to ensure other dialects don't get # prefix added incorrectly.

    Only MSSQL/Redshift use # for temp tables. Other dialects use TEMPORARY
    keyword which doesn't result in the 'temporary' flag on the identifier.
    """

    def test_postgresql_temp_table_no_hash(self):
        """PostgreSQL temp tables should NOT get # prefix."""
        sql = "CREATE TEMPORARY TABLE my_temp AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="postgres")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert not any(t.startswith("#") for t in table_names), (
            f"PostgreSQL should not add # prefix: {table_names}"
        )

    def test_snowflake_temp_table_no_hash(self):
        """Snowflake temp tables should NOT get # prefix."""
        sql = "CREATE TEMPORARY TABLE my_temp AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="snowflake")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert not any(t.startswith("#") for t in table_names), (
            f"Snowflake should not add # prefix: {table_names}"
        )

    def test_bigquery_temp_table_no_hash(self):
        """BigQuery temp tables should NOT get # prefix."""
        sql = "CREATE TEMP TABLE my_temp AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="bigquery")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert not any(t.startswith("#") for t in table_names), (
            f"BigQuery should not add # prefix: {table_names}"
        )

    def test_hive_temp_table_no_hash(self):
        """Hive temp tables should NOT get # prefix."""
        sql = "CREATE TEMPORARY TABLE my_temp AS SELECT * FROM source"
        parsed = sqlglot.parse(sql, dialect="hive")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_names = [_TableName.from_sqlglot_table(t).table for t in tables]

        assert not any(t.startswith("#") for t in table_names), (
            f"Hive should not add # prefix: {table_names}"
        )


class TestDotExpressionTableNames:
    """Tests for table names with more than 3 parts (Dot expressions).

    When a table has more than 3 parts (e.g., a.b.c.d.table), SQLGlot
    represents the table name as a Dot expression rather than a simple
    Identifier. We need to handle this case for temp table detection.
    """

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

        result = _TableName.from_sqlglot_table(table)
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

        result = _TableName.from_sqlglot_table(table)
        assert result.table == "a.b.#temptable", (
            f"Expected a.b.#temptable, got {result.table}"
        )

    def test_multipart_dot_global_temp_table(self):
        """Dot expression with global_ flag should get ## prefix."""
        # Construct a Dot expression where the final identifier has global_=True
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

        result = _TableName.from_sqlglot_table(table)
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

        result = _TableName.from_sqlglot_table(table)
        assert result.table == "part1.part2.regular_table"
        assert not result.table.endswith("#regular_table")

    def test_mssql_4part_temp_table_real_sql(self):
        """Test 4-part temp table name with real SQL parsing.

        4-part names (server.database.schema.table) create Dot expressions
        in SQLGlot, which exercises the Dot branch in from_sqlglot_table.
        """
        sql = "SELECT * FROM server.mydb.dbo.#staging"
        parsed = sqlglot.parse(sql, dialect="tsql")[0]
        assert parsed is not None

        tables = list(parsed.find_all(sqlglot.exp.Table))
        assert len(tables) == 1

        # Verify this creates a Dot expression (4-part name)
        assert isinstance(tables[0].this, sqlglot.exp.Dot)

        table_name = _TableName.from_sqlglot_table(tables[0])
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

        table_name = _TableName.from_sqlglot_table(tables[0])
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
            dialect=sqlglot.dialects.TSQL(),
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
            dialect=sqlglot.dialects.TSQL(),
            default_db="mydb",
            default_schema="dbo",
        )
        assert qualified.table == "#temptable"
        assert qualified.database == "mydb"
        assert qualified.db_schema == "dbo"
