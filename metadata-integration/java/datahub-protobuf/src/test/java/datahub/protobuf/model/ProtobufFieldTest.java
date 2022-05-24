package datahub.protobuf.model;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.linkedin.data.template.StringArray;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.FixedType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.RecordType;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;


public class ProtobufFieldTest {
    private static final DescriptorProto EXPECTED_MESSAGE_PROTO = DescriptorProto.newBuilder()
            .setName("message1")
            .build();
    private static final FileDescriptorProto EXPECTED_FILE_PROTO = FileDescriptorProto.newBuilder()
            .addMessageType(EXPECTED_MESSAGE_PROTO)
            .setPackage("protobuf")
            .build();
    private static final ProtobufMessage EXPECTED_MESSAGE = ProtobufMessage.builder()
            .messageProto(EXPECTED_MESSAGE_PROTO)
            .fileProto(EXPECTED_FILE_PROTO)
            .build();


    @Test
    public void fieldTest() {
        FieldDescriptorProto expectedField =  FieldDescriptorProto.newBuilder()
                .setName("field1")
                .setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                .build();
        DescriptorProto expectedMessage1 = DescriptorProto.newBuilder()
                .setName("message1")
                .addField(expectedField)
                .build();
        FileDescriptorProto expectedFile = FileDescriptorProto.newBuilder()
                .addMessageType(expectedMessage1)
                .setPackage("protobuf")
                .build();
        ProtobufMessage expectedMessage = ProtobufMessage.builder()
                .messageProto(expectedMessage1)
                .fileProto(expectedFile)
                .build();

        ProtobufField test = ProtobufField.builder()
                .fieldProto(expectedField)
                .protobufMessage(expectedMessage)
                .build();

        assertEquals("field1", test.name());
        assertEquals("protobuf.message1.field1", test.fullName());
        assertEquals("[type=bytes]", test.fieldPathType());
        assertEquals("protobuf.message1", test.parentMessageName());
        assertEquals(expectedMessage1, test.messageProto());
        assertEquals(expectedFile, test.fileProto());
        assertNull(test.oneOfProto());
        assertEquals("bytes", test.nativeType());
        assertFalse(test.isMessage());
        assertEquals(1, test.sortWeight());
        assertEquals(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType())), test.schemaFieldDataType());
        assertEquals("ProtobufField[protobuf.message1.field1]", test.toString());
    }

    @Test
    public void fieldPathTypeTest() {
        Arrays.stream(FieldDescriptorProto.Type.values()).forEach(type -> {
            final  FieldDescriptorProto expectedField;
            if (type == FieldDescriptorProto.Type.TYPE_MESSAGE) {
                expectedField = FieldDescriptorProto.newBuilder()
                        .setName("field1")
                        .setNumber(1)
                        .setTypeName(EXPECTED_MESSAGE.fullName())
                        .setType(type)
                        .build();
            } else {
                expectedField = FieldDescriptorProto.newBuilder()
                        .setName("field1")
                        .setNumber(1)
                        .setType(type)
                        .build();
            }

            ProtobufField test = ProtobufField.builder()
                    .fieldProto(expectedField)
                    .protobufMessage(EXPECTED_MESSAGE)
                    .build();

            if (type.equals(FieldDescriptorProto.Type.TYPE_MESSAGE)) {
                assertEquals("[type=protobuf_message1]", test.fieldPathType());
            } else if (type.name().endsWith("64")) {
                assertEquals("[type=long]", test.fieldPathType());
            } else if (type.name().endsWith("32")) {
                assertEquals("[type=int]", test.fieldPathType());
            } else if (type.name().endsWith("BOOL")) {
                assertEquals("[type=boolean]", test.fieldPathType());
            } else {
                assertEquals(String.format("[type=%s]", type.name().split("_")[1].toLowerCase()), test.fieldPathType());
            }
        });
    }

    @Test
    public void fieldPathTypeArrayTest() {
        Arrays.stream(FieldDescriptorProto.Type.values()).forEach(type -> {
            final  FieldDescriptorProto expectedField;

            if (type == FieldDescriptorProto.Type.TYPE_MESSAGE) {
                expectedField = FieldDescriptorProto.newBuilder()
                        .setName("field1")
                        .setNumber(1)
                        .setTypeName(EXPECTED_MESSAGE.fullName())
                        .setType(type)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .build();
            } else {
                expectedField = FieldDescriptorProto.newBuilder()
                        .setName("field1")
                        .setNumber(1)
                        .setType(type)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .build();
            }

            ProtobufField test = ProtobufField.builder()
                    .fieldProto(expectedField)
                    .protobufMessage(EXPECTED_MESSAGE)
                    .build();

            if (type.equals(FieldDescriptorProto.Type.TYPE_MESSAGE)) {
                assertEquals("[type=array].[type=protobuf_message1]", test.fieldPathType());
            } else if (type.name().endsWith("64")) {
                assertEquals("[type=array].[type=long]", test.fieldPathType());
            } else if (type.name().endsWith("32")) {
                assertEquals("[type=array].[type=int]", test.fieldPathType());
            } else if (type.name().endsWith("BOOL")) {
                assertEquals("[type=array].[type=boolean]", test.fieldPathType());
            } else {
                assertEquals(String.format("[type=array].[type=%s]", type.name().split("_")[1].toLowerCase()), test.fieldPathType());
            }
        });
    }

    @Test
    public void schemaFieldTypeTest() {
        Arrays.stream(FieldDescriptorProto.Type.values()).forEach(type -> {
            final  FieldDescriptorProto expectedField;
            if (type == FieldDescriptorProto.Type.TYPE_MESSAGE) {
                expectedField = FieldDescriptorProto.newBuilder()
                        .setName("field1")
                        .setNumber(1)
                        .setTypeName(EXPECTED_MESSAGE.fullName())
                        .setType(type)
                        .build();
            } else {
                expectedField = FieldDescriptorProto.newBuilder()
                        .setName("field1")
                        .setNumber(1)
                        .setType(type)
                        .build();
            }

            ProtobufField test = ProtobufField.builder()
                    .fieldProto(expectedField)
                    .protobufMessage(EXPECTED_MESSAGE)
                    .build();

            if (Set.of("TYPE_MESSAGE", "TYPE_GROUP").contains(type.name())) {
                assertEquals(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())), test.schemaFieldDataType());
            } else if (type.name().contains("FIXED")) {
                assertEquals(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new FixedType())), test.schemaFieldDataType());
            } else if (type.name().endsWith("64") || type.name().endsWith("32") || Set.of("TYPE_DOUBLE", "TYPE_FLOAT").contains(type.name())) {
                assertEquals(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())), test.schemaFieldDataType());
            } else if (type.name().endsWith("BOOL")) {
                assertEquals(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType())), test.schemaFieldDataType());
            } else if (type.name().endsWith("STRING")) {
                assertEquals(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())), test.schemaFieldDataType());
            } else if (type.name().endsWith("ENUM")) {
                assertEquals(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new EnumType())), test.schemaFieldDataType());
            } else if (type.name().endsWith("BYTES")) {
                assertEquals(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType())), test.schemaFieldDataType());
            } else {
                fail(String.format("Add test case for %s", type));
            }
        });
    }

    @Test
    public void schemaFieldTypeArrayTest() {
        Arrays.stream(FieldDescriptorProto.Type.values()).forEach(type -> {
            final  FieldDescriptorProto expectedField;
            if (type == FieldDescriptorProto.Type.TYPE_MESSAGE) {
                expectedField = FieldDescriptorProto.newBuilder()
                        .setName("field1")
                        .setNumber(1)
                        .setTypeName(EXPECTED_MESSAGE.fullName())
                        .setType(type)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .build();
            } else {
                expectedField = FieldDescriptorProto.newBuilder()
                        .setName("field1")
                        .setNumber(1)
                        .setType(type)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .build();
            }

            ProtobufField test = ProtobufField.builder()
                    .fieldProto(expectedField)
                    .protobufMessage(EXPECTED_MESSAGE)
                    .build();

            assertEquals(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType()
                    .setNestedType(new StringArray()))), test.schemaFieldDataType());
        });
    }
}
