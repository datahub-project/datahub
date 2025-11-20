from datahub.ingestion.source.dataform.dataform_models import DataformColumn
from datahub.ingestion.source.dataform.dataform_utils import (
    DataformToSchemaFieldConverter,
    extract_sql_tables_from_query,
    get_dataform_entity_name,
    get_dataform_platform_for_target,
    parse_dataform_file_path,
    validate_dataform_config,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


class TestDataformToSchemaFieldConverter:
    def test_string_type_conversion(self):
        """Test string type conversions."""
        columns = [
            DataformColumn(name="text_field", type="STRING", description="Text field"),
            DataformColumn(
                name="varchar_field", type="VARCHAR(255)", description="Varchar field"
            ),
            DataformColumn(
                name="char_field", type="CHAR(10)", description="Char field"
            ),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 3

        for field in schema_fields:
            assert isinstance(field.type.type, StringTypeClass)
            assert field.nullable is True
            assert field.recursive is False

    def test_numeric_type_conversion(self):
        """Test numeric type conversions."""
        columns = [
            DataformColumn(name="int_field", type="INT64", description="Integer field"),
            DataformColumn(
                name="float_field", type="FLOAT64", description="Float field"
            ),
            DataformColumn(
                name="decimal_field", type="DECIMAL(10,2)", description="Decimal field"
            ),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 3

        for field in schema_fields:
            assert isinstance(field.type.type, NumberTypeClass)

    def test_boolean_type_conversion(self):
        """Test boolean type conversion."""
        columns = [
            DataformColumn(name="bool_field", type="BOOL", description="Boolean field"),
            DataformColumn(
                name="boolean_field", type="BOOLEAN", description="Boolean field"
            ),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 2

        for field in schema_fields:
            assert isinstance(field.type.type, BooleanTypeClass)

    def test_date_time_type_conversion(self):
        """Test date/time type conversions."""
        columns = [
            DataformColumn(name="date_field", type="DATE", description="Date field"),
            DataformColumn(name="time_field", type="TIME", description="Time field"),
            DataformColumn(
                name="timestamp_field", type="TIMESTAMP", description="Timestamp field"
            ),
            DataformColumn(
                name="datetime_field", type="DATETIME", description="Datetime field"
            ),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 4

        # Date field
        assert isinstance(schema_fields[0].type.type, DateTypeClass)

        # Time fields
        for field in schema_fields[1:]:
            assert isinstance(field.type.type, TimeTypeClass)

    def test_binary_type_conversion(self):
        """Test binary type conversions."""
        columns = [
            DataformColumn(name="bytes_field", type="BYTES", description="Bytes field"),
            DataformColumn(
                name="binary_field", type="BINARY", description="Binary field"
            ),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 2

        for field in schema_fields:
            assert isinstance(field.type.type, BytesTypeClass)

    def test_array_type_conversion(self):
        """Test array type conversions."""
        columns = [
            DataformColumn(
                name="array_field", type="ARRAY<STRING>", description="Array field"
            ),
            DataformColumn(
                name="repeated_field",
                type="REPEATED STRING",
                description="Repeated field",
            ),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 2

        for field in schema_fields:
            assert isinstance(field.type.type, ArrayTypeClass)

    def test_struct_type_conversion(self):
        """Test struct/record type conversions."""
        columns = [
            DataformColumn(
                name="struct_field",
                type="STRUCT<name STRING, age INT64>",
                description="Struct field",
            ),
            DataformColumn(
                name="record_field",
                type="RECORD<id STRING, data JSON>",
                description="Record field",
            ),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 2

        for field in schema_fields:
            assert isinstance(field.type.type, RecordTypeClass)

    def test_unknown_type_defaults_to_string(self):
        """Test that unknown types default to string."""
        columns = [
            DataformColumn(
                name="unknown_field", type="UNKNOWN_TYPE", description="Unknown field"
            ),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 1
        assert isinstance(schema_fields[0].type.type, StringTypeClass)

    def test_null_type_handling(self):
        """Test null/empty type handling."""
        columns = [
            DataformColumn(name="null_field", type="", description="Null type field"),
            DataformColumn(name="none_field", type=None, description="None type field"),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 2

        for field in schema_fields:
            assert isinstance(field.type.type, NullTypeClass)

    def test_schema_field_properties(self):
        """Test schema field properties are set correctly."""
        columns = [
            DataformColumn(
                name="test_field", type="STRING", description="Test field description"
            ),
        ]

        schema_fields = list(DataformToSchemaFieldConverter.get_schema_fields(columns))

        assert len(schema_fields) == 1
        field = schema_fields[0]

        assert field.fieldPath == "test_field"
        assert field.description == "Test field description"
        assert field.nativeDataType == "STRING"
        assert field.nullable is True
        assert field.recursive is False


class TestUtilityFunctions:
    def test_get_dataform_entity_name_full(self):
        """Test entity name generation with all components."""
        name = get_dataform_entity_name("analytics", "customer_metrics", "my-project")
        assert name == "my-project.analytics.customer_metrics"

    def test_get_dataform_entity_name_no_database(self):
        """Test entity name generation without database."""
        name = get_dataform_entity_name("analytics", "customer_metrics")
        assert name == "analytics.customer_metrics"

    def test_get_dataform_entity_name_no_schema(self):
        """Test entity name generation without schema."""
        name = get_dataform_entity_name("", "customer_metrics", "my-project")
        assert name == "my-project.customer_metrics"

    def test_get_dataform_platform_for_target(self):
        """Test platform name mapping."""
        assert get_dataform_platform_for_target("bigquery") == "bigquery"
        assert get_dataform_platform_for_target("postgres") == "postgres"
        assert get_dataform_platform_for_target("postgresql") == "postgres"
        assert get_dataform_platform_for_target("redshift") == "redshift"
        assert get_dataform_platform_for_target("snowflake") == "snowflake"
        assert get_dataform_platform_for_target("mysql") == "mysql"
        assert get_dataform_platform_for_target("sqlserver") == "mssql"
        assert get_dataform_platform_for_target("mssql") == "mssql"
        assert get_dataform_platform_for_target("oracle") == "oracle"
        assert get_dataform_platform_for_target("databricks") == "databricks"
        assert get_dataform_platform_for_target("spark") == "spark"
        assert get_dataform_platform_for_target("hive") == "hive"

        # Unknown platform should return as-is
        assert get_dataform_platform_for_target("unknown") == "unknown"

    def test_extract_sql_tables_from_query(self):
        """Test SQL table extraction from queries."""
        # Basic SELECT query
        query1 = (
            "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id"
        )
        tables1 = extract_sql_tables_from_query(query1)
        assert "customers" in tables1
        assert "orders" in tables1

        # Query with schema-qualified tables
        query2 = "SELECT * FROM analytics.customers c JOIN raw.orders o ON c.id = o.customer_id"
        tables2 = extract_sql_tables_from_query(query2)
        assert "analytics.customers" in tables2
        assert "raw.orders" in tables2

        # INSERT INTO query
        query3 = "INSERT INTO target_table SELECT * FROM source_table"
        tables3 = extract_sql_tables_from_query(query3)
        assert "target_table" in tables3
        assert "source_table" in tables3

        # UPDATE query
        query4 = "UPDATE customer_metrics SET total_orders = 5"
        tables4 = extract_sql_tables_from_query(query4)
        assert "customer_metrics" in tables4

    def test_parse_dataform_file_path(self):
        """Test Dataform file path parsing."""
        # Standard definitions path
        path1 = "definitions/analytics/customer_metrics.sqlx"
        result1 = parse_dataform_file_path(path1)
        assert result1["filename"] == "customer_metrics.sqlx"
        assert result1["extension"] == ".sqlx"
        assert result1["relative_path"] == "analytics/customer_metrics.sqlx"

        # Tables directory
        path2 = "definitions/tables/dim_customers.sqlx"
        result2 = parse_dataform_file_path(path2)
        assert result2["entity_type"] == "tables"
        assert result2["relative_path"] == "tables/dim_customers.sqlx"

        # Assertions directory
        path3 = "definitions/assertions/customer_data_quality.sqlx"
        result3 = parse_dataform_file_path(path3)
        assert result3["entity_type"] == "assertions"

        # Operations directory
        path4 = "definitions/operations/refresh_cache.sqlx"
        result4 = parse_dataform_file_path(path4)
        assert result4["entity_type"] == "operations"

        # Path without definitions
        path5 = "src/models/customer.sql"
        result5 = parse_dataform_file_path(path5)
        assert result5["filename"] == "customer.sql"
        assert "entity_type" not in result5

    def test_validate_dataform_config_valid_cloud(self):
        """Test configuration validation for valid cloud config."""
        config = {
            "cloud_config": {
                "project_id": "test-project",
                "repository_id": "test-repo",
                "service_account_key_file": "/path/to/key.json",
            },
            "target_platform": "bigquery",
        }

        errors = validate_dataform_config(config)
        assert len(errors) == 0

    def test_validate_dataform_config_valid_core(self):
        """Test configuration validation for valid core config."""
        config = {
            "core_config": {"project_path": "/path/to/project"},
            "target_platform": "postgres",
        }

        errors = validate_dataform_config(config)
        assert len(errors) == 0

    def test_validate_dataform_config_missing_both(self):
        """Test configuration validation with missing configs."""
        config = {"target_platform": "bigquery"}

        errors = validate_dataform_config(config)
        assert len(errors) == 1
        assert "Either 'cloud_config' or 'core_config' must be provided" in errors[0]

    def test_validate_dataform_config_both_provided(self):
        """Test configuration validation with both configs."""
        config = {
            "cloud_config": {
                "project_id": "test-project",
                "repository_id": "test-repo",
            },
            "core_config": {"project_path": "/path/to/project"},
            "target_platform": "bigquery",
        }

        errors = validate_dataform_config(config)
        assert len(errors) == 1
        assert (
            "Only one of 'cloud_config' or 'core_config' can be provided" in errors[0]
        )

    def test_validate_dataform_config_missing_target_platform(self):
        """Test configuration validation with missing target platform."""
        config = {
            "cloud_config": {"project_id": "test-project", "repository_id": "test-repo"}
        }

        errors = validate_dataform_config(config)
        assert len(errors) == 1
        assert "target_platform is required" in errors[0]

    def test_validate_dataform_config_missing_cloud_required_fields(self):
        """Test configuration validation with missing cloud config fields."""
        config = {
            "cloud_config": {
                "project_id": "test-project"
                # Missing repository_id
            },
            "target_platform": "bigquery",
        }

        errors = validate_dataform_config(config)
        assert len(errors) == 1
        assert "cloud_config.repository_id is required" in errors[0]

    def test_validate_dataform_config_missing_core_required_fields(self):
        """Test configuration validation with missing core config fields."""
        config = {
            "core_config": {
                # Missing project_path
            },
            "target_platform": "postgres",
        }

        errors = validate_dataform_config(config)
        assert len(errors) == 1
        assert "core_config.project_path is required" in errors[0]

    def test_validate_dataform_config_missing_cloud_auth(self):
        """Test configuration validation with missing cloud authentication."""
        config = {
            "cloud_config": {
                "project_id": "test-project",
                "repository_id": "test-repo",
                # Missing authentication
            },
            "target_platform": "bigquery",
        }

        errors = validate_dataform_config(config)
        assert len(errors) == 1
        assert (
            "Either 'service_account_key_file' or 'credentials_json' must be provided"
            in errors[0]
        )
