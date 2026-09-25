"""Unit tests for Dataplex schema extraction utilities."""

from unittest.mock import Mock

from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_schema import (
    extract_field_value,
    extract_graph_schema_from_entry_aspects,
    extract_schema_from_entry_aspects,
    map_aspect_type_to_datahub,
    process_schema_field_item,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


class TestExtractFieldValue:
    """Test extract_field_value function."""

    def test_extract_from_dict_with_string_value(self):
        """Test extraction from dict with string_value attribute."""
        mock_value = Mock()
        mock_value.string_value = "test_value"

        field_data = {"name": mock_value}
        result = extract_field_value(field_data, "name")

        assert result == "test_value"

    def test_extract_from_dict_with_primitive(self):
        """Test extraction from dict with primitive value."""
        field_data = {"name": "test_value"}
        result = extract_field_value(field_data, "name")

        assert result == "test_value"

    def test_extract_from_dict_missing_key(self):
        """Test extraction from dict with missing key."""
        field_data = {"other": "value"}
        result = extract_field_value(field_data, "name", "default_value")

        assert result == "default_value"

    def test_extract_from_dict_none_value(self):
        """Test extraction from dict with None value."""
        field_data = {"name": None}
        result = extract_field_value(field_data, "name", "default_value")

        assert result == "default_value"

    def test_extract_from_object_with_attribute(self):
        """Test extraction from object with attribute."""
        field_data = Mock()
        field_data.name = "test_value"

        result = extract_field_value(field_data, "name")

        assert result == "test_value"

    def test_extract_from_object_missing_attribute(self):
        """Test extraction from object with missing attribute."""
        field_data = Mock(spec=[])
        result = extract_field_value(field_data, "name", "default_value")

        assert result == "default_value"


class TestProcessSchemaFieldItem:
    """Test process_schema_field_item function."""

    def test_process_struct_value(self):
        """Test processing protobuf Value with struct_value."""
        mock_field = Mock()
        mock_field.name = Mock()
        mock_field.name.string_value = "column1"

        mock_struct = Mock()
        mock_struct.fields = {"name": mock_field}

        field_value = Mock()
        field_value.struct_value = mock_struct

        result = process_schema_field_item(field_value, "entry_id")

        assert result is not None
        assert "name" in result

    def test_process_dict_like_object(self):
        """Test processing dict-like object."""
        field_value = {"name": "column1", "type": "STRING"}

        result = process_schema_field_item(field_value, "entry_id")

        assert result == field_value

    def test_process_none_value(self):
        """Test processing None value."""
        result = process_schema_field_item(None, "entry_id")
        assert result is None


class TestMapAspectTypeToDatahub:
    """Test map_aspect_type_to_datahub function."""

    def test_map_string_types(self):
        """Test mapping string types."""
        for type_str in ["STRING", "VARCHAR", "CHAR", "TEXT", "string"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, StringTypeClass)

    def test_map_integer_types(self):
        """Test mapping integer types."""
        for type_str in ["INTEGER", "INT", "INT64", "LONG", "integer"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, NumberTypeClass)

    def test_map_float_types(self):
        """Test mapping float types, including BigQuery's FLOAT64/BIGNUMERIC."""
        for type_str in [
            "FLOAT",
            "FLOAT64",
            "DOUBLE",
            "NUMERIC",
            "BIGNUMERIC",
            "DECIMAL",
        ]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, NumberTypeClass)

    def test_map_boolean_types(self):
        """Test mapping boolean types."""
        for type_str in ["BOOLEAN", "BOOL", "boolean"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, BooleanTypeClass)

    def test_map_time_types(self):
        """Test mapping time types."""
        for type_str in ["TIMESTAMP", "DATETIME", "DATE", "TIME"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, TimeTypeClass)

    def test_map_bytes_types(self):
        """Test mapping bytes types."""
        for type_str in ["BYTES", "BINARY"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, BytesTypeClass)

    def test_map_record_types(self):
        """Test mapping record types."""
        for type_str in ["RECORD", "STRUCT"]:
            result = map_aspect_type_to_datahub(type_str)
            assert isinstance(result.type, RecordTypeClass)

    def test_map_array_type(self):
        """Test mapping array type."""
        result = map_aspect_type_to_datahub("ARRAY")
        assert isinstance(result.type, ArrayTypeClass)

    def test_map_unknown_type(self):
        """Test mapping unknown type defaults to string."""
        result = map_aspect_type_to_datahub("UNKNOWN_TYPE")
        assert isinstance(result.type, StringTypeClass)


class TestExtractSchemaFromEntryAspects:
    """Test extract_schema_from_entry_aspects function."""

    def test_extract_no_aspects(self):
        """Test extraction when entry has no aspects."""
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {}

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_no_schema_aspect(self):
        """Test extraction when entry has aspects but no schema aspect."""
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "other-aspect": Mock(),
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_schema_aspect_no_data(self):
        """Test extraction when schema aspect has no data."""
        schema_aspect = Mock()
        schema_aspect.data = None

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_schema_aspect_no_fields(self):
        """Test extraction when schema aspect data has no column/field data."""
        schema_aspect = Mock()
        schema_aspect.data = {"other_key": "value"}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_schema_with_columns_list_value(self):
        """Test extraction with columns in list_value format."""
        # Mock field data
        field_name = Mock()
        field_name.string_value = "column1"

        field_type = Mock()
        field_type.string_value = "STRING"

        field_struct = Mock()
        field_struct.fields = {"name": field_name, "type": field_type}

        field_value = Mock()
        field_value.struct_value = field_struct

        list_value = Mock()
        list_value.values = [field_value]

        columns_data = Mock()
        columns_data.list_value = list_value

        schema_aspect = Mock()
        schema_aspect.data = {"columns": columns_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 1
        assert result.fields[0].fieldPath == "column1"
        assert result.fields[0].nativeDataType == "STRING"

    def test_extract_schema_with_fields_iterable(self):
        """Test extraction with fields as iterable."""
        fields_data = [
            {"name": "column1", "type": "STRING", "description": "Test column"},
            {"name": "column2", "type": "INTEGER"},
        ]

        schema_aspect = Mock()
        schema_aspect.data = {"fields": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 2
        assert result.fields[0].fieldPath == "column1"
        assert result.fields[0].description == "Test column"
        assert result.fields[1].fieldPath == "column2"

    def test_extract_schema_fallback_aspect_key(self):
        """Test extraction with fallback schema aspect key."""
        fields_data = [{"name": "column1", "type": "STRING"}]

        schema_aspect = Mock()
        schema_aspect.data = {"columns": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "custom/schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 1

    def test_extract_schema_with_datatype_fallback(self):
        """Test extraction using dataType field as fallback."""
        fields_data = [{"name": "column1", "dataType": "BOOLEAN"}]

        schema_aspect = Mock()
        schema_aspect.data = {"fields": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 1
        assert isinstance(result.fields[0].type.type, BooleanTypeClass)

    def test_extract_schema_with_column_name_fallback(self):
        """Test extraction using column field as name fallback."""
        fields_data = [{"column": "column1", "type": "STRING"}]

        schema_aspect = Mock()
        schema_aspect.data = {"fields": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is not None
        assert len(result.fields) == 1
        assert result.fields[0].fieldPath == "column1"

    def test_extract_schema_exception_handling(self):
        """Test exception handling during extraction."""
        schema_aspect = Mock()
        # Make data raise exception when accessed
        schema_aspect.data = Mock()
        type(schema_aspect.data).__iter__ = Mock(side_effect=Exception("Test error"))

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None

    def test_extract_schema_no_valid_fields(self):
        """Test extraction when no valid fields can be extracted."""
        # Fields without name should be skipped
        fields_data = [{"type": "STRING", "description": "No name"}]

        schema_aspect = Mock()
        schema_aspect.data = {"fields": fields_data}

        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {
            "dataplex-types.global.schema": schema_aspect,
        }

        result = extract_schema_from_entry_aspects(entry, "test_entry", "bigquery")

        assert result is None


def _make_node_value(name: str) -> Mock:
    """Build a mock protobuf Value for a graph node entry."""
    name_val = Mock()
    name_val.string_value = name

    struct = Mock()
    struct.fields = {"name": name_val}

    value = Mock()
    value.struct_value = struct
    return value


def _make_edge_value(name: str, source: str, destination: str) -> Mock:
    """Build a mock protobuf Value for a graph edge entry."""
    name_val = Mock()
    name_val.string_value = name

    src_name_val = Mock()
    src_name_val.string_value = source
    src_struct = Mock()
    src_struct.fields = {"name": src_name_val}
    src_val = Mock()
    src_val.struct_value = src_struct

    dst_name_val = Mock()
    dst_name_val.string_value = destination
    dst_struct = Mock()
    dst_struct.fields = {"name": dst_name_val}
    dst_val = Mock()
    dst_val.struct_value = dst_struct

    edge_struct = Mock()
    edge_struct.fields = {"name": name_val, "source": src_val, "destination": dst_val}

    value = Mock()
    value.struct_value = edge_struct
    return value


def _make_graph_aspect(nodes: list, edges: list) -> Mock:
    nodes_list = Mock()
    nodes_list.values = nodes
    nodes_val = Mock()
    nodes_val.list_value = nodes_list

    edges_list = Mock()
    edges_list.values = edges
    edges_val = Mock()
    edges_val.list_value = edges_list

    aspect = Mock()
    aspect.data = {"nodes": nodes_val, "edges": edges_val}
    return aspect


class TestExtractGraphSchemaFromEntryAspects:
    def test_no_aspects_returns_none(self) -> None:
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {}
        assert extract_graph_schema_from_entry_aspects(entry, "g", "spanner") is None

    def test_no_graph_schema_aspect_returns_none(self) -> None:
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {"some-other-aspect": Mock()}
        assert extract_graph_schema_from_entry_aspects(entry, "g", "spanner") is None

    def test_graph_schema_aspect_no_data_returns_none(self) -> None:
        aspect = Mock()
        aspect.data = None
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {"655216118709.global.graph-schema": aspect}
        assert extract_graph_schema_from_entry_aspects(entry, "g", "spanner") is None

    def test_nodes_and_edges_extracted(self) -> None:
        aspect = _make_graph_aspect(
            nodes=[_make_node_value("Users"), _make_node_value("Orders")],
            edges=[_make_edge_value("ShoppingCarts", "Users", "Products")],
        )
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {"655216118709.global.graph-schema": aspect}

        result = extract_graph_schema_from_entry_aspects(
            entry, "ECommerceGraph", "spanner"
        )

        assert result is not None
        assert result.fields == [
            SchemaFieldClass(
                fieldPath="[nodes].Users",
                type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                nativeDataType="NODE",
                nullable=True,
                recursive=False,
            ),
            SchemaFieldClass(
                fieldPath="[nodes].Orders",
                type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                nativeDataType="NODE",
                nullable=True,
                recursive=False,
            ),
            SchemaFieldClass(
                fieldPath="[edges].ShoppingCarts",
                type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                nativeDataType="EDGE",
                description="Users \u2192 Products",
                nullable=True,
                recursive=False,
            ),
        ]

    def test_schema_name_and_platform(self) -> None:
        aspect = _make_graph_aspect(
            nodes=[_make_node_value("A")],
            edges=[],
        )
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {"655216118709.global.graph-schema": aspect}

        result = extract_graph_schema_from_entry_aspects(entry, "MyGraph", "spanner")

        assert result is not None
        assert result.schemaName == "MyGraph"
        assert "spanner" in result.platform

    def test_nodes_and_edges_native_list_form(self) -> None:
        """Python-native list form produced by proto-plus auto-marshaling."""
        aspect = Mock()
        aspect.data = {
            "nodes": [
                {"name": "Users"},
                {"name": "Orders"},
            ],
            "edges": [
                {
                    "name": "ShoppingCarts",
                    "source": {"name": "Users"},
                    "destination": {"name": "Products"},
                }
            ],
        }
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {"655216118709.global.graph-schema": aspect}

        result = extract_graph_schema_from_entry_aspects(
            entry, "ECommerceGraph", "spanner"
        )

        assert result is not None
        assert result.fields == [
            SchemaFieldClass(
                fieldPath="[nodes].Users",
                type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                nativeDataType="NODE",
                nullable=True,
                recursive=False,
            ),
            SchemaFieldClass(
                fieldPath="[nodes].Orders",
                type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                nativeDataType="NODE",
                nullable=True,
                recursive=False,
            ),
            SchemaFieldClass(
                fieldPath="[edges].ShoppingCarts",
                type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                nativeDataType="EDGE",
                description="Users \u2192 Products",
                nullable=True,
                recursive=False,
            ),
        ]

    def test_empty_nodes_and_edges_returns_none(self) -> None:
        aspect = Mock()
        aspect.data = {"nodes": [], "edges": []}
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {"655216118709.global.graph-schema": aspect}
        assert extract_graph_schema_from_entry_aspects(entry, "g", "spanner") is None


class TestComplexColumns:
    """Structured columns expand to v2 fieldPaths; flat columns are unchanged."""

    @staticmethod
    def _entry(columns: list) -> dataplex_v1.Entry:
        aspect = Mock()
        aspect.data = {"fields": columns}
        entry = Mock(spec=dataplex_v1.Entry)
        entry.aspects = {"655216118709.global.schema": aspect}
        return entry

    def test_repeated_record_becomes_array_of_struct(self) -> None:
        entry = self._entry(
            [
                {
                    "name": "items",
                    "dataType": "RECORD",
                    "mode": "REPEATED",
                    "description": "line items",
                    "fields": [
                        {"name": "item_id", "dataType": "INT64", "mode": "REQUIRED"},
                        {"name": "label", "dataType": "STRING"},
                    ],
                }
            ]
        )

        schema = extract_schema_from_entry_aspects(entry, "my_table", "bigquery")

        assert schema is not None
        paths = [schema_field.fieldPath for schema_field in schema.fields]
        assert paths == [
            "[version=2.0].[type=struct].[type=array].[type=struct].items",
            "[version=2.0].[type=struct].[type=array].[type=struct].items.[type=long].item_id",
            "[version=2.0].[type=struct].[type=array].[type=struct].items.[type=string].label",
        ]
        assert isinstance(schema.fields[0].type.type, ArrayTypeClass)
        assert schema.fields[0].nativeDataType == "ARRAY<RECORD>"
        assert schema.fields[0].description == "line items"
        # REQUIRED children stay non-nullable; the rest default to nullable.
        assert schema.fields[1].nullable is False
        assert schema.fields[2].nullable is True

    def test_hive_style_native_type_is_parsed(self) -> None:
        entry = self._entry(
            [{"name": "tags", "dataType": "array<struct<key:string,value:string>>"}]
        )

        schema = extract_schema_from_entry_aspects(entry, "my_table", "hive")

        assert schema is not None
        paths = [schema_field.fieldPath for schema_field in schema.fields]
        assert paths[0].endswith(".tags")
        assert any(path.endswith(".tags.[type=string].key") for path in paths)
        assert any(path.endswith(".tags.[type=string].value") for path in paths)

    def test_flat_columns_keep_their_plain_path(self) -> None:
        entry = self._entry(
            [
                {"name": "col_a", "dataType": "STRING"},
                {"name": "col_b", "dataType": "INT64"},
            ]
        )

        schema = extract_schema_from_entry_aspects(entry, "my_table", "bigquery")

        assert schema is not None
        assert [schema_field.fieldPath for schema_field in schema.fields] == [
            "col_a",
            "col_b",
        ]
        assert all(schema_field.nullable for schema_field in schema.fields)

    def test_bare_record_without_children_stays_flat(self) -> None:
        """No structure to expand, and a plain fieldPath avoids churning the
        schemaField URNs that column docs and tags hang off."""
        entry = self._entry([{"name": "payload", "dataType": "RECORD"}])

        schema = extract_schema_from_entry_aspects(entry, "my_table", "bigquery")

        assert schema is not None
        assert [schema_field.fieldPath for schema_field in schema.fields] == ["payload"]
        assert isinstance(schema.fields[0].type.type, RecordTypeClass)

    def test_repeated_scalar_becomes_an_array(self) -> None:
        entry = self._entry(
            [{"name": "labels", "dataType": "STRING", "mode": "REPEATED"}]
        )

        schema = extract_schema_from_entry_aspects(entry, "my_table", "bigquery")

        assert schema is not None
        assert isinstance(schema.fields[0].type.type, ArrayTypeClass)
        assert schema.fields[0].nativeDataType == "ARRAY<STRING>"

    def test_unparseable_complex_type_falls_back_to_a_flat_row(self) -> None:
        entry = self._entry([{"name": "broken", "dataType": "struct<<<"}])

        schema = extract_schema_from_entry_aspects(entry, "my_table", "hive")

        assert schema is not None
        assert [schema_field.fieldPath for schema_field in schema.fields] == ["broken"]
