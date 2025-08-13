import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.hive_metastore import (
    HiveMetastore,
    HiveMetastoreSource,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    MapTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    UnionTypeClass,
)


class TestHiveMetastoreSource(unittest.TestCase):
    def setUp(self):
        self.mock_config = HiveMetastore(
            host_port="localhost:3306",
            database="metastore",
            username="user",
            password="password",
        )
        self.mock_ctx = PipelineContext(run_id="test-run")

        # Create a patcher for the SQLAlchemyClient
        self.sql_client_patcher = patch(
            "datahub.ingestion.source.sql.hive_metastore.SQLAlchemyClient"
        )
        self.mock_sql_client_class = self.sql_client_patcher.start()
        self.mock_sql_client = MagicMock()
        self.mock_sql_client_class.return_value = self.mock_sql_client

        # Create a mock Inspector
        self.mock_inspector = MagicMock(spec=Inspector)

    def tearDown(self):
        self.sql_client_patcher.stop()

    def test_create(self):
        config_dict = {
            "host_port": "localhost:3306",
            "database": "metastore",
            "username": "user",
            "password": "password",
        }
        source = HiveMetastoreSource.create(config_dict, self.mock_ctx)
        self.assertIsInstance(source, HiveMetastoreSource)
        self.assertEqual(source.platform, "hive")

    @patch(
        "datahub.ingestion.source.sql.hive_metastore.get_schema_fields_for_hive_column"
    )
    def test_schema_field_v1_vs_v2_flag(self, mock_get_schema_fields_for_hive_column):
        """Test that use_schema_field_v2 flag affects schema field generation"""
        # Setup mock to return a v1-style field
        mock_v1_field = SchemaField(
            fieldPath="id",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
            nativeDataType="int",
            description="ID column",
            nullable=True,
        )
        mock_get_schema_fields_for_hive_column.return_value = [mock_v1_field]

        # Create a source with v1 schema fields
        config_v1 = HiveMetastore(
            host_port="localhost:3306",
            database="metastore",
            username="user",
            password="password",
            use_schema_field_v2=False,
        )
        source_v1 = HiveMetastoreSource(config_v1, self.mock_ctx)

        # Create a source with v2 schema fields
        config_v2 = HiveMetastore(
            host_port="localhost:3306",
            database="metastore",
            username="user",
            password="password",
            use_schema_field_v2=True,
        )
        source_v2 = HiveMetastoreSource(config_v2, self.mock_ctx)

        # Test with a simple column
        column = {"col_name": "id", "col_type": "int", "col_description": "ID column"}

        # For v1 test, use the mocked function
        fields_v1 = source_v1.get_schema_fields_for_column(
            "test_dataset", column, self.mock_inspector
        )

        # For v2 test, use the actual implementation
        mock_get_schema_fields_for_hive_column.reset_mock()  # Reset the mock for v2 test
        fields_v2 = source_v2.get_schema_fields_for_column(
            "test_dataset", column, self.mock_inspector
        )

        # V1 should have a different format than V2
        self.assertEqual(fields_v1[0].fieldPath, "id")
        self.assertEqual(fields_v2[0].fieldPath, "[version=2.0].[type=int].id")

    def test_schema_field_v2_primitives(self):
        """Test V2 schema field generation for primitive types"""
        config = HiveMetastore(
            host_port="localhost:3306",
            database="metastore",
            username="user",
            password="password",
            use_schema_field_v2=True,
        )
        source = HiveMetastoreSource(config, self.mock_ctx)

        # Test different primitive types
        test_cases = [
            ("string_col", "string", StringTypeClass),
            ("int_col", "int", NumberTypeClass),
            ("bigint_col", "bigint", NumberTypeClass),
            ("boolean_col", "boolean", BooleanTypeClass),
            ("double_col", "double", NumberTypeClass),
            ("varchar_col", "varchar(255)", StringTypeClass),
        ]

        for col_name, col_type, expected_type_class in test_cases:
            column = {
                "col_name": col_name,
                "col_type": col_type,
                "col_description": f"Test {col_name}",
            }
            fields = source.get_schema_fields_for_column(
                "test_dataset", column, self.mock_inspector
            )

            self.assertEqual(len(fields), 1)
            field = fields[0]
            self.assertEqual(
                field.fieldPath,
                f"[version=2.0].[type={source._get_v2_type_name(col_type)}].{col_name}",
            )
            self.assertEqual(field.nativeDataType, col_type)
            self.assertIsInstance(field.type.type, expected_type_class)
            self.assertEqual(field.description, f"Test {col_name}")

    def test_schema_field_v2_complex_types(self):
        """Test V2 schema field generation for complex types"""
        config = HiveMetastore(
            host_port="localhost:3306",
            database="metastore",
            username="user",
            password="password",
            use_schema_field_v2=True,
        )
        source = HiveMetastoreSource(config, self.mock_ctx)

        # Test struct type
        struct_column = {
            "col_name": "user_info",
            "col_type": "struct<name:string,age:int,address:string>",
            "col_description": "User information",
        }
        struct_fields = source.get_schema_fields_for_column(
            "test_dataset", struct_column, self.mock_inspector
        )

        # Should have the main struct field and 3 child fields
        self.assertEqual(len(struct_fields), 4)
        # Main struct field
        self.assertEqual(
            struct_fields[0].fieldPath, "[version=2.0].[type=struct].user_info"
        )
        self.assertIsInstance(struct_fields[0].type.type, RecordTypeClass)

        # Test array type
        array_column = {
            "col_name": "tags",
            "col_type": "array<string>",
            "col_description": "Tags list",
        }
        array_fields = source.get_schema_fields_for_column(
            "test_dataset", array_column, self.mock_inspector
        )

        self.assertEqual(len(array_fields), 1)
        self.assertEqual(
            array_fields[0].fieldPath, "[version=2.0].[type=array].[type=string].tags"
        )
        self.assertIsInstance(array_fields[0].type.type, ArrayTypeClass)

        # Test map type
        map_column = {
            "col_name": "properties",
            "col_type": "map<string,string>",
            "col_description": "Properties map",
        }
        map_fields = source.get_schema_fields_for_column(
            "test_dataset", map_column, self.mock_inspector
        )

        self.assertEqual(len(map_fields), 1)
        self.assertEqual(
            map_fields[0].fieldPath, "[version=2.0].[type=map].[type=string].properties"
        )
        self.assertIsInstance(map_fields[0].type.type, MapTypeClass)

        # Test union type
        union_column = {
            "col_name": "value",
            "col_type": "uniontype<int,string>",
            "col_description": "Union of int and string",
        }
        union_fields = source.get_schema_fields_for_column(
            "test_dataset", union_column, self.mock_inspector
        )

        # Should have the main union field and 2 member fields
        self.assertEqual(len(union_fields), 3)
        self.assertEqual(union_fields[0].fieldPath, "[version=2.0].[type=union].value")
        self.assertIsInstance(union_fields[0].type.type, UnionTypeClass)

    def test_schema_field_v2_nested_complex_types(self):
        """Test V2 schema field generation for nested complex types"""
        config = HiveMetastore(
            host_port="localhost:3306",
            database="metastore",
            username="user",
            password="password",
            use_schema_field_v2=True,
        )
        source = HiveMetastoreSource(config, self.mock_ctx)

        # Test nested struct in array
        nested_array_column = {
            "col_name": "users",
            "col_type": "array<struct<name:string,age:int>>",
            "col_description": "List of users",
        }
        nested_array_fields = source.get_schema_fields_for_column(
            "test_dataset", nested_array_column, self.mock_inspector
        )

        # Should have array field + struct field inside array + 2 primitive fields inside struct
        self.assertGreaterEqual(len(nested_array_fields), 3)
        self.assertEqual(
            nested_array_fields[0].fieldPath,
            "[version=2.0].[type=array].[type=struct].users",
        )

        # Test nested map with struct values
        nested_map_column = {
            "col_name": "user_map",
            "col_type": "map<string,struct<name:string,age:int>>",
            "col_description": "Map of users",
        }
        nested_map_fields = source.get_schema_fields_for_column(
            "test_dataset", nested_map_column, self.mock_inspector
        )

        # Should have map field + struct field inside map + 2 primitive fields inside struct
        self.assertGreaterEqual(len(nested_map_fields), 3)
        self.assertEqual(
            nested_map_fields[0].fieldPath,
            "[version=2.0].[type=map].[type=struct].user_map",
        )

    def test_parsing_helper_methods(self):
        """Test the helper methods for parsing complex types"""
        config = HiveMetastore(
            host_port="localhost:3306",
            database="metastore",
            username="user",
            password="password",
            use_schema_field_v2=True,
        )
        source = HiveMetastoreSource(config, self.mock_ctx)

        # Test struct field parsing
        struct_content = "name:string,age:int,address:struct<street:string,zip:int>"
        struct_fields = source._parse_struct_fields(struct_content)
        self.assertEqual(len(struct_fields), 3)
        self.assertEqual(struct_fields[0], ("name", "string"))
        self.assertEqual(struct_fields[1], ("age", "int"))
        self.assertEqual(struct_fields[2], ("address", "struct<street:string,zip:int>"))

        # Test map type parsing
        map_content = "string,struct<name:string,age:int>"
        key_type, value_type = source._parse_map_types(map_content)
        self.assertEqual(key_type, "string")
        self.assertEqual(value_type, "struct<name:string,age:int>")

        # Test union type parsing
        union_content = "int,string,struct<name:string,age:int>"
        union_types = source._parse_union_types(union_content)
        self.assertEqual(len(union_types), 3)
        self.assertEqual(union_types[0], "int")
        self.assertEqual(union_types[1], "string")
        self.assertEqual(union_types[2], "struct<name:string,age:int>")

    @patch(
        "datahub.ingestion.source.sql.hive_metastore.get_schema_fields_for_hive_column"
    )
    def test_fallback_to_v1_on_error(self, mock_get_schema_fields_for_hive_column):
        """Test that the source falls back to v1 schema fields if v2 parsing fails"""
        # Setup mock to return a v1-style field
        mock_v1_field = SchemaField(
            fieldPath="id",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
            nativeDataType="int",
            description="ID column",
            nullable=True,
        )
        mock_get_schema_fields_for_hive_column.return_value = [mock_v1_field]

        config = HiveMetastore(
            host_port="localhost:3306",
            database="metastore",
            username="user",
            password="password",
            use_schema_field_v2=True,
        )
        source = HiveMetastoreSource(config, self.mock_ctx)

        # Create a patched source that will fail on v2 parsing
        with patch.object(
            source, "_generate_schema_fields_v2", side_effect=Exception("Test error")
        ):
            column = {
                "col_name": "id",
                "col_type": "int",
                "col_description": "ID column",
            }
            fields = source.get_schema_fields_for_column(
                "test_dataset", column, self.mock_inspector
            )

            # Should fall back to v1 format
            self.assertEqual(len(fields), 1)
            self.assertEqual(fields[0].fieldPath, "id")
