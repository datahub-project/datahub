package io.datahubproject.schematron.converters.avro;

import static org.testng.Assert.*;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.schema.*;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import org.apache.avro.Schema;
import org.testng.annotations.*;

@Test(groups = "unit")
class AvroSchemaConverterTest {

  private AvroSchemaConverter avroSchemaConverter = AvroSchemaConverter.builder().build();
  private DataPlatformUrn dataPlatformUrn =
      DataPlatformUrn.createFromString("urn:li:dataPlatform:foo");

  AvroSchemaConverterTest() throws URISyntaxException {}

  @Test(groups = "basic")
  void testPrimitiveTypes() throws IOException {
    SchemaMetadata schema =
        avroSchemaConverter.toDataHubSchema(
            readAvroSchema("primitive_types.avsc"), false, false, dataPlatformUrn, null);

    schema.getFields().forEach(System.out::println);
    schema.getFields().stream().map(SchemaField::getFieldPath).forEach(System.out::println);

    assertEquals(schema.getFields().size(), 14);

    assertSchemaField(
        schema.getFields().get(0),
        "[version=2.0].[type=PrimitiveType].[type=int].intField",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(1),
        "[version=2.0].[type=PrimitiveType].[type=union].intFieldV2",
        "union",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new UnionType()
                        .setNestedTypes(new StringArray(Collections.singletonList("union"))))));
    assertSchemaField(
        schema.getFields().get(2),
        "[version=2.0].[type=PrimitiveType].[type=union].[type=int].intFieldV2",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(3),
        "[version=2.0].[type=PrimitiveType].[type=null].nullField",
        "null",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NullType())));
    assertSchemaField(
        schema.getFields().get(4),
        "[version=2.0].[type=PrimitiveType].[type=union].nullFieldV2",
        "union",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new UnionType()
                        .setNestedTypes(new StringArray(Collections.singletonList("union"))))));
    assertSchemaField(
        schema.getFields().get(5),
        "[version=2.0].[type=PrimitiveType].[type=long].longField",
        "long",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(6),
        "[version=2.0].[type=PrimitiveType].[type=float].floatField",
        "float",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(7),
        "[version=2.0].[type=PrimitiveType].[type=double].doubleField",
        "double",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(8),
        "[version=2.0].[type=PrimitiveType].[type=string].stringField",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(9),
        "[version=2.0].[type=PrimitiveType].[type=boolean].booleanField",
        "boolean",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType())));
    assertSchemaField(
        schema.getFields().get(10),
        "[version=2.0].[type=PrimitiveType].[type=int].nullableIntField",
        "int",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(11),
        "[version=2.0].[type=PrimitiveType].[type=long].nullableLongField",
        "long",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(12),
        "[version=2.0].[type=PrimitiveType].[type=string].nullableStringField",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(13),
        "[version=2.0].[type=PrimitiveType].[type=enum].status",
        "Enum",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new EnumType())));
  }

  @Test(groups = "basic")
  void testComplexMaps() throws IOException {
    SchemaMetadata schema =
        avroSchemaConverter.toDataHubSchema(
            readAvroSchema("complex_maps.avsc"), false, false, dataPlatformUrn, null);

    schema.getFields().forEach(System.out::println);
    schema.getFields().stream().map(SchemaField::getFieldPath).forEach(System.out::println);

    assertEquals(schema.getFields().size(), 19);

    assertSchemaField(
        schema.getFields().get(0),
        "[version=2.0].[type=MapType].[type=map].mapOfString",
        "map<string,string>",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("string"))));
    assertSchemaField(
        schema.getFields().get(1),
        "[version=2.0].[type=MapType].[type=map].mapOfComplexType",
        "map<string,ComplexType>",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("ComplexType"))));
    assertSchemaField(
        schema.getFields().get(2),
        "[version=2.0].[type=MapType].[type=map].mapOfComplexType.[type=ComplexType].value",
        "ComplexType",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(3),
        "[version=2.0].[type=MapType].[type=map].mapOfComplexType.[type=ComplexType].value.[type=string].field1",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(4),
        "[version=2.0].[type=MapType].[type=map].mapOfComplexType.[type=ComplexType].value.[type=int].field2",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(5),
        "[version=2.0].[type=MapType].[type=map].mapOfNullableString",
        "map<string,string>",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("string"))));
    assertSchemaField(
        schema.getFields().get(6),
        "[version=2.0].[type=MapType].[type=map].mapOfNullableString.[type=string].value",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(7),
        "[version=2.0].[type=MapType].[type=map].mapOfNullableComplexType",
        "map<string,ComplexTypeNullable>",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("ComplexTypeNullable"))));
    assertSchemaField(
        schema.getFields().get(8),
        "[version=2.0].[type=MapType].[type=map].mapOfNullableComplexType.[type=ComplexTypeNullable].value",
        "ComplexTypeNullable",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(9),
        "[version=2.0].[type=MapType].[type=map].mapOfNullableComplexType.[type=ComplexTypeNullable].value.[type=string].field1",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(10),
        "[version=2.0].[type=MapType].[type=map].mapOfNullableComplexType.[type=ComplexTypeNullable].value.[type=int].field2",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(11),
        "[version=2.0].[type=MapType].[type=map].mapOfArray",
        "map<string,array>",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("array"))));
    assertSchemaField(
        schema.getFields().get(12),
        "[version=2.0].[type=MapType].[type=map].mapOfArray.[type=array].value",
        "array(string)",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("string")))));
    assertSchemaField(
        schema.getFields().get(13),
        "[version=2.0].[type=MapType].[type=map].mapOfMap",
        "map<string,map>",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("map"))));
    assertSchemaField(
        schema.getFields().get(14),
        "[version=2.0].[type=MapType].[type=map].mapOfMap.[type=map].value",
        "map<string,int>",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("int"))));
    assertSchemaField(
        schema.getFields().get(15),
        "[version=2.0].[type=MapType].[type=map].mapOfUnion",
        "map<string,union>",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("union"))));
    assertSchemaField(
        schema.getFields().get(16),
        "[version=2.0].[type=MapType].[type=map].mapOfUnion.[type=union].value",
        "union",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new UnionType().setNestedTypes(new StringArray("union")))));
    assertSchemaField(
        schema.getFields().get(17),
        "[version=2.0].[type=MapType].[type=map].mapOfUnion.[type=union].[type=string].value",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(18),
        "[version=2.0].[type=MapType].[type=map].mapOfUnion.[type=union].[type=int].value",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
  }

  @Test(groups = "basic")
  void testComplexArrays() throws IOException {
    SchemaMetadata schema =
        avroSchemaConverter.toDataHubSchema(
            readAvroSchema("complex_arrays.avsc"), false, false, dataPlatformUrn, null);

    schema.getFields().forEach(System.out::println);
    schema.getFields().stream().map(SchemaField::getFieldPath).forEach(System.out::println);

    assertEquals(schema.getFields().size(), 20);

    assertSchemaField(
        schema.getFields().get(0),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfString",
        "array(string)",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("string")))));
    assertSchemaField(
        schema.getFields().get(1),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfMap",
        "array(map)",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("map")))));
    assertSchemaField(
        schema.getFields().get(2),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfMap.[type=map].items",
        "map<string,string>",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("string"))));
    assertSchemaField(
        schema.getFields().get(3),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfRecord",
        "array(ComplexType)",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("ComplexType")))));
    assertSchemaField(
        schema.getFields().get(4),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfRecord.[type=ComplexType].items",
        "ComplexType",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(5),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfRecord.[type=ComplexType].items.[type=string].field1",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(6),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfRecord.[type=ComplexType].items.[type=int].field2",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(7),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfArray",
        "array(array)",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("array")))));
    assertSchemaField(
        schema.getFields().get(8),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfArray.[type=array].items",
        "array(string)",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("string")))));
    assertSchemaField(
        schema.getFields().get(9),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfUnion",
        "array(union)",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("union")))));
    assertSchemaField(
        schema.getFields().get(10),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfUnion.[type=union].items",
        "union",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new UnionType().setNestedTypes(new StringArray("union")))));
    assertSchemaField(
        schema.getFields().get(11),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfUnion.[type=union].[type=string].items",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(12),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfUnion.[type=union].[type=int].items",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(13),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfUnion.[type=union].[type=boolean].items",
        "boolean",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType())));
    assertSchemaField(
        schema.getFields().get(14),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfNullableString",
        "array(string)",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("string")))));
    assertSchemaField(
        schema.getFields().get(15),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfNullableString.[type=string].items",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(16),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfNullableRecord",
        "array(ComplexTypeNullable)",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("ComplexTypeNullable")))));
    assertSchemaField(
        schema.getFields().get(17),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfNullableRecord.[type=ComplexTypeNullable].items",
        "ComplexTypeNullable",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(18),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfNullableRecord.[type=ComplexTypeNullable].items.[type=string].field1",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(19),
        "[version=2.0].[type=ArrayType].[type=array].arrayOfNullableRecord.[type=ComplexTypeNullable].items.[type=int].field2",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
  }

  @Test(groups = "basic")
  void testComplexStructs() throws IOException {
    SchemaMetadata schema =
        avroSchemaConverter.toDataHubSchema(
            readAvroSchema("complex_structs.avsc"), false, false, dataPlatformUrn, null);

    schema.getFields().forEach(System.out::println);
    schema.getFields().stream().map(SchemaField::getFieldPath).forEach(System.out::println);

    assertEquals(schema.getFields().size(), 13);

    assertSchemaField(
        schema.getFields().get(0),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField",
        "ComplexStruct",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(1),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=string].fieldString",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(2),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=int].fieldInt",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(3),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=boolean].fieldBoolean",
        "boolean",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType())));
    assertSchemaField(
        schema.getFields().get(4),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=map].fieldMap",
        "map<string,string>",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("string"))));
    assertSchemaField(
        schema.getFields().get(5),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=NestedRecord].fieldRecord",
        "NestedRecord",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(6),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=NestedRecord].fieldRecord.[type=string].nestedField1",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(7),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=NestedRecord].fieldRecord.[type=int].nestedField2",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(8),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=array].fieldArray",
        "array(string)",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new ArrayType().setNestedType(new StringArray("string")))));
    assertSchemaField(
        schema.getFields().get(9),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=union].fieldUnion",
        "union",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new UnionType().setNestedTypes(new StringArray("union")))));
    assertSchemaField(
        schema.getFields().get(10),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=union].[type=string].fieldUnion",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(11),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=union].[type=int].fieldUnion",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(12),
        "[version=2.0].[type=StructType].[type=ComplexStruct].structField.[type=map].fieldNullableMap",
        "map<string,string>",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("string"))));
  }

  @Test(groups = "basic")
  void testComplexUnions() throws IOException {
    SchemaMetadata schema =
        avroSchemaConverter.toDataHubSchema(
            readAvroSchema("complex_unions.avsc"), false, false, dataPlatformUrn, null);

    schema.getFields().forEach(System.out::println);
    schema.getFields().stream().map(SchemaField::getFieldPath).forEach(System.out::println);

    assertEquals(schema.getFields().size(), 14);

    assertSchemaField(
        schema.getFields().get(0),
        "[version=2.0].[type=UnionType].[type=union].fieldUnionNullablePrimitives",
        "union",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new UnionType().setNestedTypes(new StringArray("union")))));
    assertSchemaField(
        schema.getFields().get(1),
        "[version=2.0].[type=UnionType].[type=union].[type=string].fieldUnionNullablePrimitives",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(2),
        "[version=2.0].[type=UnionType].[type=union].[type=int].fieldUnionNullablePrimitives",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(3),
        "[version=2.0].[type=UnionType].[type=union].[type=boolean].fieldUnionNullablePrimitives",
        "boolean",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType())));
    assertSchemaField(
        schema.getFields().get(4),
        "[version=2.0].[type=UnionType].[type=union].fieldUnionComplexTypes",
        "union",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new UnionType().setNestedTypes(new StringArray("union")))));
    assertSchemaField(
        schema.getFields().get(5),
        "[version=2.0].[type=UnionType].[type=union].[type=NestedRecord].fieldUnionComplexTypes",
        "NestedRecord",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(6),
        "[version=2.0].[type=UnionType].[type=union].[type=NestedRecord].fieldUnionComplexTypes.[type=string].nestedField1",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(7),
        "[version=2.0].[type=UnionType].[type=union].[type=NestedRecord].fieldUnionComplexTypes.[type=int].nestedField2",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(8),
        "[version=2.0].[type=UnionType].[type=union].[type=map].fieldUnionComplexTypes",
        "map<string,string>",
        false,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("string"))));
    assertSchemaField(
        schema.getFields().get(9),
        "[version=2.0].[type=UnionType].[type=union].fieldUnionPrimitiveAndComplex",
        "union",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new UnionType().setNestedTypes(new StringArray("union")))));
    assertSchemaField(
        schema.getFields().get(10),
        "[version=2.0].[type=UnionType].[type=union].[type=string].fieldUnionPrimitiveAndComplex",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(11),
        "[version=2.0].[type=UnionType].[type=union].[type=ComplexTypeRecord].fieldUnionPrimitiveAndComplex",
        "ComplexTypeRecord",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(12),
        "[version=2.0].[type=UnionType].[type=union].[type=ComplexTypeRecord].fieldUnionPrimitiveAndComplex.[type=string].complexField1",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(13),
        "[version=2.0].[type=UnionType].[type=union].[type=ComplexTypeRecord].fieldUnionPrimitiveAndComplex.[type=int].complexField2",
        "int",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
  }

  @Test(groups = "basic")
  void testLogicalTypes() throws IOException {
    SchemaMetadata schema =
        avroSchemaConverter.toDataHubSchema(
            readAvroSchema("logical_types.avsc"), false, false, dataPlatformUrn, null);

    schema.getFields().forEach(System.out::println);
    schema.getFields().stream().map(SchemaField::getFieldPath).forEach(System.out::println);

    assertEquals(schema.getFields().size(), 9);

    assertSchemaField(
        schema.getFields().get(0),
        "[version=2.0].[type=LogicalTypes].[type=decimal].decimalField",
        "bytes(decimal)",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())),
        "{\"scale\":2,\"logicalType\":\"decimal\",\"precision\":9}");
    assertSchemaField(
        schema.getFields().get(1),
        "[version=2.0].[type=LogicalTypes].[type=decimal].decimalFieldWithoutScale",
        "bytes(decimal)",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())),
        "{\"logicalType\":\"decimal\",\"precision\":9}");
    assertSchemaField(
        schema.getFields().get(2),
        "[version=2.0].[type=LogicalTypes].[type=bytes].decimalFieldWithoutPrecisionAndScale",
        "bytes",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType())),
        "{\"logicalType\":\"decimal\"}");
    assertSchemaField(
        schema.getFields().get(3),
        "[version=2.0].[type=LogicalTypes].[type=timestamp-millis].timestampMillisField",
        "long(timestamp-millis)",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType())),
        "{\"logicalType\":\"timestamp-millis\"}");
    assertSchemaField(
        schema.getFields().get(4),
        "[version=2.0].[type=LogicalTypes].[type=timestamp-micros].timestampMicrosField",
        "long(timestamp-micros)",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType())),
        "{\"logicalType\":\"timestamp-micros\"}");
    assertSchemaField(
        schema.getFields().get(5),
        "[version=2.0].[type=LogicalTypes].[type=date].dateField",
        "int(date)",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new DateType())),
        "{\"logicalType\":\"date\"}");
    assertSchemaField(
        schema.getFields().get(6),
        "[version=2.0].[type=LogicalTypes].[type=time-millis].timeMillisField",
        "int(time-millis)",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType())),
        "{\"logicalType\":\"time-millis\"}");
    assertSchemaField(
        schema.getFields().get(7),
        "[version=2.0].[type=LogicalTypes].[type=time-micros].timeMicrosField",
        "long(time-micros)",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType())),
        "{\"logicalType\":\"time-micros\"}");
    assertSchemaField(
        schema.getFields().get(8),
        "[version=2.0].[type=LogicalTypes].[type=uuid].uuidField",
        "string(uuid)",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())),
        "{\"logicalType\":\"uuid\"}");
  }

  @Test(groups = "basic")
  void testUsersRecord() throws IOException {
    // this is a test case got during the Hudi integration
    SchemaMetadata schema =
        avroSchemaConverter.toDataHubSchema(
            readAvroSchema("users_record.avsc"), false, false, dataPlatformUrn, null);

    schema.getFields().forEach(System.out::println);
    schema.getFields().stream().map(SchemaField::getFieldPath).forEach(System.out::println);

    assertEquals(schema.getFields().size(), 21);

    assertSchemaField(
        schema.getFields().get(0),
        "[version=2.0].[type=users_record].[type=string]._hoodie_commit_time",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(1),
        "[version=2.0].[type=users_record].[type=string]._hoodie_commit_seqno",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(2),
        "[version=2.0].[type=users_record].[type=string]._hoodie_record_key",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(3),
        "[version=2.0].[type=users_record].[type=string]._hoodie_partition_path",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(4),
        "[version=2.0].[type=users_record].[type=string]._hoodie_file_name",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(5),
        "[version=2.0].[type=users_record].[type=string].user_id",
        "string",
        false,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(6),
        "[version=2.0].[type=users_record].[type=string].name",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(7),
        "[version=2.0].[type=users_record].[type=address].address",
        "address",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(8),
        "[version=2.0].[type=users_record].[type=address].address.[type=string].street",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(9),
        "[version=2.0].[type=users_record].[type=address].address.[type=string].city",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(10),
        "[version=2.0].[type=users_record].[type=address].address.[type=string].country",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(11),
        "[version=2.0].[type=users_record].[type=address].address.[type=string].postal_code",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(12),
        "[version=2.0].[type=users_record].[type=address].address.[type=timestamp-micros].created_at",
        "long(timestamp-micros)",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType())),
        "{\"logicalType\":\"timestamp-micros\"}");
    assertSchemaField(
        schema.getFields().get(13),
        "[version=2.0].[type=users_record].[type=contact].contact",
        "contact",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())));
    assertSchemaField(
        schema.getFields().get(14),
        "[version=2.0].[type=users_record].[type=contact].contact.[type=string].email",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(15),
        "[version=2.0].[type=users_record].[type=contact].contact.[type=string].phone",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    assertSchemaField(
        schema.getFields().get(16),
        "[version=2.0].[type=users_record].[type=timestamp-micros].created_at",
        "long(timestamp-micros)",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType())),
        "{\"logicalType\":\"timestamp-micros\"}");
    assertSchemaField(
        schema.getFields().get(17),
        "[version=2.0].[type=users_record].[type=timestamp-micros].updated_at",
        "long(timestamp-micros)",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType())),
        "{\"logicalType\":\"timestamp-micros\"}");
    assertSchemaField(
        schema.getFields().get(18),
        "[version=2.0].[type=users_record].[type=map].props",
        "map<string,int>",
        true,
        false,
        new SchemaFieldDataType()
            .setType(
                SchemaFieldDataType.Type.create(
                    new MapType().setKeyType("string").setValueType("int"))));
    assertSchemaField(
        schema.getFields().get(19),
        "[version=2.0].[type=users_record].[type=map].props.[type=int].value",
        "int",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    assertSchemaField(
        schema.getFields().get(20),
        "[version=2.0].[type=users_record].[type=string].country",
        "string",
        true,
        false,
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
  }

  private void assertSchemaField(
      SchemaField field,
      String expectedPath,
      String expectedNativeType,
      boolean expectedNullable,
      boolean expectedIsPartOfKey,
      SchemaFieldDataType expectedType) {
    assertSchemaField(
        field,
        expectedPath,
        expectedNativeType,
        expectedNullable,
        expectedIsPartOfKey,
        expectedType,
        null);
  }

  private void assertSchemaField(
      SchemaField field,
      String expectedPath,
      String expectedNativeType,
      boolean expectedNullable,
      boolean expectedIsPartOfKey,
      SchemaFieldDataType expectedType,
      String expectedJsonProps) {
    assertEquals(field.getFieldPath(), expectedPath);
    assertEquals(field.getNativeDataType(), expectedNativeType);
    assertEquals(field.isNullable(), expectedNullable);
    assertEquals(field.isIsPartOfKey(), expectedIsPartOfKey);
    assertEquals(field.getType(), expectedType);
    if (expectedJsonProps != null) {
      assertEquals(field.getJsonProps(), expectedJsonProps);
    }
  }

  private Schema readAvroSchema(String schemaFileName) throws IOException {
    String schemaPath = getClass().getClassLoader().getResource(schemaFileName).getPath();
    File schemaFile = new File(schemaPath);
    return new Schema.Parser().parse(schemaFile);
  }
}
