"""Unit tests for Google Sheets Pydantic models."""

from datahub.ingestion.source.google_sheets.models import DatabaseReference


class TestDatabaseReference:
    """Tests for DatabaseReference factory methods."""

    def test_create_bigquery(self):
        """Test BigQuery factory method."""
        ref = DatabaseReference.create_bigquery("my_project", "my_dataset", "my_table")

        assert ref.platform == "bigquery"
        assert ref.table_identifier == "my_project.my_dataset.my_table"

    def test_create_snowflake_with_table(self):
        """Test Snowflake factory method with table."""
        ref = DatabaseReference.create_snowflake("my_db", "my_schema", "my_table")

        assert ref.platform == "snowflake"
        assert ref.table_identifier == "my_db.my_schema.my_table"

    def test_create_snowflake_schema_only(self):
        """Test Snowflake factory method without table (schema-level reference)."""
        ref = DatabaseReference.create_snowflake("my_db", "my_schema", None)

        assert ref.platform == "snowflake"
        assert ref.table_identifier == "my_db.my_schema"

    def test_create_postgres(self):
        """Test PostgreSQL factory method."""
        ref = DatabaseReference.create_postgres("my_db", "public", "my_table")

        assert ref.platform == "postgres"
        assert ref.table_identifier == "my_db.public.my_table"

    def test_create_mysql(self):
        """Test MySQL factory method (no schema)."""
        ref = DatabaseReference.create_mysql("my_db", "my_table")

        assert ref.platform == "mysql"
        assert ref.table_identifier == "my_db.my_table"

    def test_create_redshift(self):
        """Test Redshift factory method."""
        ref = DatabaseReference.create_redshift("my_db", "public", "my_table")

        assert ref.platform == "redshift"
        assert ref.table_identifier == "my_db.public.my_table"

    def test_database_reference_consistency(self):
        """Test that factory methods produce consistent identifiers."""
        # BigQuery: 3 parts (project.dataset.table)
        bq_ref = DatabaseReference.create_bigquery("p", "d", "t")
        assert bq_ref.table_identifier.count(".") == 2

        # Postgres: 3 parts (database.schema.table)
        pg_ref = DatabaseReference.create_postgres("d", "s", "t")
        assert pg_ref.table_identifier.count(".") == 2

        # MySQL: 2 parts (database.table)
        mysql_ref = DatabaseReference.create_mysql("d", "t")
        assert mysql_ref.table_identifier.count(".") == 1
