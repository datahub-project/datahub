from typing import List

import pytest

from datahub.ingestion.extractor.protobuf_util import (
    ProtobufSchema,
    protobuf_schema_to_mce_fields,
)
from datahub.metadata.schema_classes import ArrayTypeClass, SchemaFieldClass


def test_protobuf_schema_to_mce_fields_with_single_empty_message() -> None:
    schema: str = """
syntax = "proto3";

message Test1 {
}
"""
    fields: List[SchemaFieldClass] = protobuf_schema_to_mce_fields(
        ProtobufSchema("main_1.proto", schema)
    )

    assert 0 == len(fields)


def test_protobuf_schema_to_mce_fields_with_single_message_single_field_key_schema() -> (
    None
):
    schema: str = """
syntax = "proto3";

message Test2 {
    string field_1 = 1;
}
"""
    fields: List[SchemaFieldClass] = protobuf_schema_to_mce_fields(
        ProtobufSchema("main_2.proto", schema), is_key_schema=True
    )
    assert 1 == len(fields)
    assert (
        "[version=2.0].[key=True].[type=Test2].[type=string].field_1"
        == fields[0].fieldPath
    )
    assert "string" == fields[0].nativeDataType


def test_protobuf_schema_to_mce_fields_with_two_messages_enum() -> None:
    schema: str = """
syntax = "proto3";

message Test3 {
    string field_1 = 1;

    enum anEnum {
        first = 0;
        second = 1;
    }

    anEnum field_2 = 2;
}

message Test4 {
    int64 anInteger = 1;
}
"""
    fields: List[SchemaFieldClass] = protobuf_schema_to_mce_fields(
        ProtobufSchema("main_3.proto", schema)
    )

    assert 5 == len(fields)
    assert "[version=2.0].[type=Test3].Test3" == fields[0].fieldPath
    assert "[version=2.0].[type=Test3].Test3.[type=enum].field_2" == fields[1].fieldPath
    assert (
        "[version=2.0].[type=Test3].Test3.[type=string].field_1" == fields[2].fieldPath
    )
    assert "[version=2.0].[type=Test4].Test4" == fields[3].fieldPath
    assert (
        "[version=2.0].[type=Test4].Test4.[type=long].anInteger" == fields[4].fieldPath
    )


def test_protobuf_schema_to_mce_fields_nested():
    schema: str = """
syntax = "proto3";

message Test5 {
    Nested1 f1 = 1;
    message Nested1 {
        Nested2 f2 = 1;
        message Nested2 {
            Nested3 f3 = 1;
            message Nested3 {
                message Nested4 {}
                Nested4 f4 = 1;
            }
        }
    }
}
"""
    fields: List[SchemaFieldClass] = protobuf_schema_to_mce_fields(
        ProtobufSchema("main_4.proto", schema)
    )

    assert 4 == len(fields)
    assert "[version=2.0].[type=Test5].[type=Test5_Nested1].f1" == fields[0].fieldPath
    assert (
        "[version=2.0].[type=Test5].[type=Test5_Nested1].f1.[type=Test5_Nested1_Nested2].f2"
        == fields[1].fieldPath
    )
    assert (
        "[version=2.0].[type=Test5].[type=Test5_Nested1].f1.[type=Test5_Nested1_Nested2].f2.[type=Test5_Nested1_Nested2_Nested3].f3"
        == fields[2].fieldPath
    )
    assert (
        "[version=2.0].[type=Test5].[type=Test5_Nested1].f1.[type=Test5_Nested1_Nested2].f2.[type=Test5_Nested1_Nested2_Nested3].f3.[type=Test5_Nested1_Nested2_Nested3_Nested4].f4"
        == fields[3].fieldPath
    )
    assert "Test5.Nested1.Nested2.Nested3.Nested4" == fields[3].nativeDataType


def test_protobuf_schema_to_mce_fields_repeated() -> None:
    schema: str = """
syntax = "proto3";

message Test6 {
    repeated int64 aList = 1;
}
"""
    fields: List[SchemaFieldClass] = protobuf_schema_to_mce_fields(
        ProtobufSchema("main_5.proto", schema)
    )

    assert 1 == len(fields)
    assert (
        "[version=2.0].[type=Test6].[type=array].[type=long].aList"
        == fields[0].fieldPath
    )
    assert "int64" == fields[0].nativeDataType
    assert isinstance(fields[0].type.type, ArrayTypeClass)
    assert fields[0].type.type.nestedType is not None
    assert "int64" == fields[0].type.type.nestedType[0]


def test_protobuf_schema_to_mce_fields_nestd_repeated() -> None:
    schema: str = """
syntax = "proto3";

message Test7 {
    repeated Nested aList = 1;

    message Nested {
        string name = 1;
    }
}
"""
    fields: List[SchemaFieldClass] = protobuf_schema_to_mce_fields(
        ProtobufSchema("main_6.proto", schema)
    )

    assert 2 == len(fields)
    assert (
        "[version=2.0].[type=Test7].[type=array].[type=Test7_Nested].aList"
        == fields[0].fieldPath
    )
    assert (
        "[version=2.0].[type=Test7].[type=array].[type=Test7_Nested].aList.[type=string].name"
        == fields[1].fieldPath
    )
    assert "Test7.Nested" == fields[0].nativeDataType
    assert isinstance(fields[0].type.type, ArrayTypeClass)
    assert fields[0].type.type.nestedType is not None
    assert "Test7.Nested" == fields[0].type.type.nestedType[0]


# This is not how maps should be encoded but we need to find a good way of detecting
# maps in protobuf before we change it back
#
def test_protobuf_schema_to_mce_fields_map() -> None:
    schema: str = """
syntax = "proto3";

message Test8 {
    map<string, int64> map_1 = 1;
    map<string, Nested> map_2 = 2;

    message Nested {
        string aString = 1;
    }
}
"""
    fields: List[SchemaFieldClass] = protobuf_schema_to_mce_fields(
        ProtobufSchema("main_7.proto", schema)
    )

    assert 7 == len(fields)
    assert (
        "[version=2.0].[type=Test8].[type=array].[type=Test8_Map1Entry].map_1"
        == fields[0].fieldPath
    )
    assert (
        "[version=2.0].[type=Test8].[type=array].[type=Test8_Map1Entry].map_1.[type=long].value"
        == fields[1].fieldPath
    )
    assert (
        "[version=2.0].[type=Test8].[type=array].[type=Test8_Map1Entry].map_1.[type=string].key"
        == fields[2].fieldPath
    )
    assert (
        "[version=2.0].[type=Test8].[type=array].[type=Test8_Map2Entry].map_2"
        == fields[3].fieldPath
    )
    assert (
        "[version=2.0].[type=Test8].[type=array].[type=Test8_Map2Entry].map_2.[type=Test8_Nested].value"
        == fields[4].fieldPath
    )
    assert (
        "[version=2.0].[type=Test8].[type=array].[type=Test8_Map2Entry].map_2.[type=Test8_Nested].value.[type=string].aString"
        == fields[5].fieldPath
    )
    assert (
        "[version=2.0].[type=Test8].[type=array].[type=Test8_Map2Entry].map_2.[type=string].key"
        == fields[6].fieldPath
    )


def test_protobuf_schema_to_mce_fields_with_complex_schema() -> None:
    schema: str = """
syntax = "proto3";

message Test9 {
    string string_field_1  = 1;
    bool   boolean_field_1 = 2;
    int64  int64_field_1   = 3;

    EmptyNested emptyMsg = 4;

    // an empty message
    message EmptyNested {};

    oneof payload {
        string pl_1 = 5;
        int64  pl_2 = 6;
    }

    enum anEnum {
        idle = 0;
        spinning = 1;
    }
}

message Test10 {
    int64 an_int_64_field = 1;
}
"""
    fields: List[SchemaFieldClass] = protobuf_schema_to_mce_fields(
        ProtobufSchema("main_8.proto", schema)
    )

    assert 10 == len(fields)
    assert "[version=2.0].[type=Test10].Test10" == fields[0].fieldPath
    assert (
        "[version=2.0].[type=Test10].Test10.[type=long].an_int_64_field"
        == fields[1].fieldPath
    )
    assert "[version=2.0].[type=Test9].Test9" == fields[2].fieldPath
    assert (
        "[version=2.0].[type=Test9].Test9.[type=Test9_EmptyNested].emptyMsg"
        == fields[3].fieldPath
    )
    assert (
        "[version=2.0].[type=Test9].Test9.[type=bool].boolean_field_1"
        == fields[4].fieldPath
    )
    assert (
        "[version=2.0].[type=Test9].Test9.[type=long].int64_field_1"
        == fields[5].fieldPath
    )
    assert (
        "[version=2.0].[type=Test9].Test9.[type=string].string_field_1"
        == fields[6].fieldPath
    )
    assert (
        "[version=2.0].[type=Test9].Test9.[type=union].payload" == fields[7].fieldPath
    )
    assert (
        "[version=2.0].[type=Test9].Test9.[type=union].payload.[type=long].pl_2"
        == fields[8].fieldPath
    )
    assert (
        "[version=2.0].[type=Test9].Test9.[type=union].payload.[type=string].pl_1"
        == fields[9].fieldPath
    )


def test_protobuf_schema_with_recursive_type() -> None:
    schema: str = """
syntax = "proto3";

message Test11 {
    string a_property = 1;
    Test11 recursive = 2;
}
"""
    with pytest.raises(Exception) as e_info:
        protobuf_schema_to_mce_fields(ProtobufSchema("main_9.proto", schema))

    assert str(e_info.value) == "Cyclic schemas are not supported"
