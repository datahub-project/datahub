import pytest
import sqlglot

from datahub.ingestion.source.dremio.dremio_source import DremioSchemaResolver
from datahub.sql_parsing._models import _TableName


class TestDremioSchemaResolver:
    """Test the custom DremioSchemaResolver that handles arbitrary-depth table hierarchies."""

    @pytest.fixture
    def resolver(self):
        """Create a DremioSchemaResolver instance for testing."""
        return DremioSchemaResolver(
            platform="dremio",
            platform_instance=None,
            env="PROD",
        )

    @pytest.mark.parametrize(
        "database,db_schema,table,expected_urn",
        [
            (
                None,
                "space",
                "table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.space.table,PROD)",
            ),
            (
                "source",
                "schema",
                "table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.source.schema.table,PROD)",
            ),
            (
                "source",
                "folder1",
                "folder2.table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.source.folder1.folder2.table,PROD)",
            ),
            (
                "source",
                "folder1",
                "folder2.subfolder.table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.source.folder1.folder2.subfolder.table,PROD)",
            ),
            (
                "source",
                "folder1",
                "folder2.folder3.folder4.table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.source.folder1.folder2.folder3.folder4.table,PROD)",
            ),
            (
                "dremio",
                "source",
                "table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.source.table,PROD)",
            ),
            (
                "source",
                None,
                "table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.source.table,PROD)",
            ),
            (
                None,
                None,
                "table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.table,PROD)",
            ),
            (
                "my-source",
                "my_schema",
                "my.table.with.dots",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.my-source.my_schema.my.table.with.dots,PROD)",
            ),
        ],
    )
    def test_urn_generation(self, resolver, database, db_schema, table, expected_urn):
        """Test URN generation for various table name formats."""
        table_name = _TableName(
            database=database,
            db_schema=db_schema,
            table=table,
        )
        urn = resolver.get_urn_for_table(table_name)
        assert urn == expected_urn

    def test_table_already_with_dremio_prefix_mixed_case(self, resolver):
        """Test when SQL includes 'Dremio' with different casing."""
        table = _TableName(
            database="Dremio",
            db_schema="source",
            table="table",
        )
        urn = resolver.get_urn_for_table(table)
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:dremio,Dremio.source.table,PROD)"
        )

    @pytest.mark.parametrize(
        "database,db_schema,table,expected_urn",
        [
            (
                "MySource",
                "Sales",
                "Orders",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.mysource.sales.orders,PROD)",
            ),
            (
                "MySource",
                "Sales",
                "Customers.Archive",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.mysource.sales.customers.archive,PROD)",
            ),
        ],
    )
    def test_lowercase_option(self, resolver, database, db_schema, table, expected_urn):
        """Test that lowercase option works correctly."""
        table_name = _TableName(
            database=database,
            db_schema=db_schema,
            table=table,
        )
        urn = resolver.get_urn_for_table(table_name, lower=True)
        assert urn == expected_urn

    def test_with_platform_instance(self):
        """Test URN generation with platform instance."""
        resolver = DremioSchemaResolver(
            platform="dremio",
            platform_instance="prod-instance",
            env="PROD",
        )
        table = _TableName(
            database="source",
            db_schema="schema",
            table="table",
        )
        urn = resolver.get_urn_for_table(table)
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:dremio,prod-instance.dremio.source.schema.table,PROD)"
        )

    def test_mixed_case_platform_instance(self):
        """Test mixed case handling with platform instance."""
        resolver = DremioSchemaResolver(
            platform="dremio",
            platform_instance="Prod-Instance",
            env="PROD",
        )
        table = _TableName(
            database="MySource",
            db_schema="Sales",
            table="Orders",
        )

        urn_lower = resolver.get_urn_for_table(table, lower=True, mixed=False)
        assert (
            urn_lower
            == "urn:li:dataset:(urn:li:dataPlatform:dremio,prod-instance.dremio.mysource.sales.orders,PROD)"
        )

        urn_mixed = resolver.get_urn_for_table(table, lower=True, mixed=True)
        assert (
            urn_mixed
            == "urn:li:dataset:(urn:li:dataPlatform:dremio,Prod-Instance.dremio.mysource.sales.orders,PROD)"
        )

    def test_real_world_example_samples_dataset(self, resolver):
        """Test a real-world example from Dremio Samples."""
        table = _TableName(
            database="Samples",
            db_schema="samples.dremio.com",
            table="NYC-weather.csv",
        )
        urn = resolver.get_urn_for_table(table, lower=True)
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.samples.samples.dremio.com.nyc-weather.csv,PROD)"
        )

    def test_real_world_example_deep_hierarchy(self, resolver):
        """Test a real-world example with deep folder structure."""
        table = _TableName(
            database="MySource",
            db_schema="folder1",
            table="folder2.subfolder.mytable",
        )
        urn = resolver.get_urn_for_table(table, lower=True)
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.mysource.folder1.folder2.subfolder.mytable,PROD)"
        )


class TestDremioSchemaResolverWithSQLGlot:
    """Integration tests that verify the resolver works correctly with actual SQLGlot parsing."""

    @pytest.fixture
    def resolver(self):
        """Create a DremioSchemaResolver instance for testing."""
        return DremioSchemaResolver(
            platform="dremio",
            platform_instance=None,
            env="PROD",
        )

    def _parse_table_from_sql(self, sql: str) -> _TableName:
        """Helper to parse a table reference from SQL using SQLGlot."""
        parsed = sqlglot.parse_one(sql, dialect="dremio")
        for table in parsed.find_all(sqlglot.exp.Table):
            return _TableName.from_sqlglot_table(table)
        raise ValueError(f"No table found in SQL: {sql}")

    @pytest.mark.parametrize(
        "sql,expected_database,expected_schema,expected_table,expected_urn",
        [
            (
                "SELECT * FROM space.table",
                None,
                "space",
                "table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.space.table,PROD)",
            ),
            (
                "SELECT * FROM source.schema.table",
                "source",
                "schema",
                "table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.source.schema.table,PROD)",
            ),
            (
                "SELECT * FROM source.folder1.folder2.table",
                "source",
                "folder1",
                "folder2.table",
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.source.folder1.folder2.table,PROD)",
            ),
        ],
    )
    def test_sqlglot_table_parsing(
        self,
        resolver,
        sql,
        expected_database,
        expected_schema,
        expected_table,
        expected_urn,
    ):
        """Verify SQLGlot parses table names correctly and resolver generates correct URNs."""
        table = self._parse_table_from_sql(sql)

        assert table.database == expected_database
        assert table.db_schema == expected_schema
        assert table.table == expected_table

        urn = resolver.get_urn_for_table(table, lower=True)
        assert urn == expected_urn

    def test_sqlglot_quoted_identifiers(self, resolver):
        """Verify SQLGlot handles identifiers correctly (Drill doesn't support quoted identifiers)."""
        sql = "SELECT * FROM MySource.Sales.Orders"
        table = self._parse_table_from_sql(sql)

        assert table.database == "MySource"
        assert table.db_schema == "Sales"
        assert table.table == "Orders"

        urn = resolver.get_urn_for_table(table, lower=True)
        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.mysource.sales.orders,PROD)"
        )

    def test_sqlglot_with_default_db(self, resolver):
        """Verify how default_db affects table resolution."""
        sql = "SELECT * FROM space.table"
        table = self._parse_table_from_sql(sql)

        qualified_table = table.qualified(
            dialect=sqlglot.Dialect.get_or_raise("dremio"),
            default_db="dremio",
            default_schema=None,
        )

        assert qualified_table.database == "dremio"
        assert qualified_table.db_schema == "space"
        assert qualified_table.table == "table"

        urn = resolver.get_urn_for_table(qualified_table, lower=True)
        assert (
            urn == "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.space.table,PROD)"
        )

    def test_sqlglot_complex_query_with_joins(self, resolver):
        """Verify resolver works with complex queries."""
        sql = """
        SELECT o.order_id, c.customer_name
        FROM source.sales.orders o
        JOIN source.sales.customers c ON o.customer_id = c.customer_id
        """
        parsed = sqlglot.parse_one(sql, dialect="dremio")

        parsed_tables = []
        for table in parsed.find_all(sqlglot.exp.Table):
            parsed_tables.append(_TableName.from_sqlglot_table(table))

        assert len(parsed_tables) == 2

        for parsed_table in parsed_tables:
            assert parsed_table.database == "source"
            assert parsed_table.db_schema == "sales"
            assert parsed_table.table in ["orders", "customers"]

            urn = resolver.get_urn_for_table(parsed_table, lower=True)
            assert urn.startswith(
                "urn:li:dataset:(urn:li:dataPlatform:dremio,dremio.source.sales."
            )

    def test_multi_part_table_name_with_parts(self):
        """Test that multi-part Dremio table names use the parts field correctly."""
        resolver = DremioSchemaResolver(
            platform="dremio",
            platform_instance="test-instance",
            env="PROD",
        )

        # Test with a 5-part table name (typical in Dremio CE)
        sql = 'SELECT * FROM "Performance"."source"."schema"."folder"."table_name"'
        parsed = sqlglot.parse_one(sql)
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table = tables[0]

        table_name = _TableName.from_sqlglot_table(table)
        urn = resolver.get_urn_for_table(table_name)

        # Verify the full hierarchy is preserved
        expected_urn_part = (
            "test-instance.dremio.performance.source.schema.folder.table_name"
        )
        assert expected_urn_part in urn.lower(), (
            f"Expected '{expected_urn_part}' in '{urn}'"
        )

    def test_standard_3_part_table_names_still_work(self):
        """Test that standard 3-part table names continue to work correctly."""
        resolver = DremioSchemaResolver(
            platform="dremio",
            platform_instance="test-instance",
            env="PROD",
        )

        table_name_3part = _TableName(
            database="MySource", db_schema="sales", table="orders"
        )

        urn_3part = resolver.get_urn_for_table(table_name_3part)
        expected_3part = "test-instance.dremio.mysource.sales.orders"

        assert expected_3part in urn_3part.lower(), (
            f"Expected '{expected_3part}' in '{urn_3part}'"
        )

    def test_unquoted_multi_part_table_names(self, resolver):
        """Test that unquoted 5-part table names are handled correctly by SQLGlot."""
        sql = "SELECT * FROM MySpace.folder1.folder2.folder3.table"
        parsed = sqlglot.parse_one(sql, dialect="dremio")
        tables = list(parsed.find_all(sqlglot.exp.Table))
        table_name = _TableName.from_sqlglot_table(tables[0])

        # Verify SQLGlot correctly populates parts for unquoted identifiers
        assert table_name.parts is not None
        assert len(table_name.parts) == 5
        assert table_name.parts == ("MySpace", "folder1", "folder2", "folder3", "table")

        urn = resolver.get_urn_for_table(table_name, lower=True)
        assert "dremio.myspace.folder1.folder2.folder3.table" in urn.lower()

    def test_home_folder_with_at_symbol(self, resolver):
        """Test that home folders (starting with @) are handled correctly in URNs."""
        table = _TableName(
            database="@john.doe",
            db_schema="personal",
            table="analysis",
        )
        urn = resolver.get_urn_for_table(table, lower=True)
        assert "@john.doe" in urn.lower()
        assert "dremio.@john.doe.personal.analysis" in urn.lower()
