package datahub.protobuf.model;

import static org.testng.Assert.*;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.UnionType;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class ProtobufOneOfFieldTest {

  @Test
  public void oneOfTest() {
    OneofDescriptorProto expectedOneOf =
        OneofDescriptorProto.newBuilder().setName("oneof1").build();
    FieldDescriptorProto expectedField =
        FieldDescriptorProto.newBuilder().setName("field1").setOneofIndex(0).build();
    DescriptorProto expectedMessage =
        DescriptorProto.newBuilder()
            .setName("message1")
            .addOneofDecl(expectedOneOf)
            .addField(expectedField)
            .build();
    FileDescriptorProto expectedFile =
        FileDescriptorProto.newBuilder()
            .addMessageType(expectedMessage)
            .setPackage("protobuf")
            .build();

    ProtobufOneOfField test =
        ProtobufOneOfField.oneOfBuilder()
            .fieldProto(expectedField)
            .protobufMessage(
                ProtobufMessage.builder()
                    .fileProto(expectedFile)
                    .messageProto(expectedMessage)
                    .build())
            .build();

    assertEquals("oneof1", test.name());
    assertEquals("protobuf.message1.oneof1", test.fullName());
    assertEquals("[type=union]", test.fieldPathType());
    assertEquals("oneof", test.nativeType());
    assertEquals(expectedOneOf, test.oneOfProto());
    assertEquals(expectedMessage, test.messageProto());
    assertEquals(expectedFile, test.fileProto());
    assertFalse(test.isMessage());
    assertEquals(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new UnionType())),
        test.schemaFieldDataType());
    assertEquals("ProtobufOneOf[protobuf.message1.oneof1]", test.toString());
  }

  @Test
  public void oneOfEqualityTest() {
    OneofDescriptorProto oneof1Message1 =
        OneofDescriptorProto.newBuilder().setName("oneof1").build();
    OneofDescriptorProto oneof2Message1 =
        OneofDescriptorProto.newBuilder().setName("oneof2").build();
    OneofDescriptorProto oneof1Message2 =
        OneofDescriptorProto.newBuilder().setName("oneof1").build();
    OneofDescriptorProto oneof1Message1Dup =
        OneofDescriptorProto.newBuilder().setName("oneof1").build();

    FieldDescriptorProto expectedField1 =
        FieldDescriptorProto.newBuilder().setName("field1").setOneofIndex(0).build();
    FieldDescriptorProto expectedField2 =
        FieldDescriptorProto.newBuilder().setName("field2").setOneofIndex(1).build();
    FieldDescriptorProto expectedField1Dup =
        FieldDescriptorProto.newBuilder().setName("field3").setOneofIndex(3).build();
    DescriptorProto expectedMessage1 =
        DescriptorProto.newBuilder()
            .setName("message1")
            .addAllOneofDecl(List.of(oneof1Message1, oneof2Message1, oneof1Message1Dup))
            .addField(expectedField1)
            .addField(expectedField2)
            .addField(expectedField1Dup)
            .build();

    FieldDescriptorProto expectedField3 =
        FieldDescriptorProto.newBuilder().setName("field3").setOneofIndex(0).build();
    DescriptorProto expectedMessage2 =
        DescriptorProto.newBuilder()
            .setName("message2")
            .addAllOneofDecl(List.of(oneof1Message2))
            .addField(expectedField3)
            .build();

    FileDescriptorProto expectedFile =
        FileDescriptorProto.newBuilder()
            .addAllMessageType(List.of(expectedMessage1, expectedMessage2))
            .setPackage("protobuf")
            .build();

    ProtobufOneOfField test1 =
        ProtobufOneOfField.oneOfBuilder()
            .fieldProto(expectedField1)
            .protobufMessage(
                ProtobufMessage.builder()
                    .fileProto(expectedFile)
                    .messageProto(expectedMessage1)
                    .build())
            .build();
    ProtobufOneOfField test1Dup =
        ProtobufOneOfField.oneOfBuilder()
            .fieldProto(expectedField1)
            .protobufMessage(
                ProtobufMessage.builder()
                    .fileProto(expectedFile)
                    .messageProto(expectedMessage1)
                    .build())
            .build();
    ProtobufOneOfField test2 =
        ProtobufOneOfField.oneOfBuilder()
            .fieldProto(expectedField2)
            .protobufMessage(
                ProtobufMessage.builder()
                    .fileProto(expectedFile)
                    .messageProto(expectedMessage1)
                    .build())
            .build();
    ProtobufOneOfField test3 =
        ProtobufOneOfField.oneOfBuilder()
            .fieldProto(expectedField3)
            .protobufMessage(
                ProtobufMessage.builder()
                    .fileProto(expectedFile)
                    .messageProto(expectedMessage2)
                    .build())
            .build();

    assertEquals(test1, test1Dup);
    assertNotEquals(test1, test3);
    assertNotEquals(test1, test2);
    assertEquals(
        Set.of(test1, test2, test3),
        Stream.of(test1, test2, test3, test1Dup).collect(Collectors.toSet()));
  }

  @Test
  public void oneOfCommentWithSiblingMessageHavingMoreOneofsTest() {
    // Outer declares one oneof; its sibling Other declares two. Source locations for every
    // top-level message share the same file, so Outer's comment lookup sees Other's oneof
    // index 1 and must not resolve it against Outer.
    OneofDescriptorProto outerChoice = OneofDescriptorProto.newBuilder().setName("choice").build();
    FieldDescriptorProto outerField =
        FieldDescriptorProto.newBuilder().setName("a").setOneofIndex(0).build();
    DescriptorProto outer =
        DescriptorProto.newBuilder()
            .setName("Outer")
            .addOneofDecl(outerChoice)
            .addField(outerField)
            .build();

    OneofDescriptorProto otherFirst = OneofDescriptorProto.newBuilder().setName("first").build();
    OneofDescriptorProto otherSecond = OneofDescriptorProto.newBuilder().setName("second").build();
    FieldDescriptorProto otherField1 =
        FieldDescriptorProto.newBuilder().setName("c").setOneofIndex(0).build();
    FieldDescriptorProto otherField2 =
        FieldDescriptorProto.newBuilder().setName("d").setOneofIndex(1).build();
    DescriptorProto other =
        DescriptorProto.newBuilder()
            .setName("Other")
            .addAllOneofDecl(List.of(otherFirst, otherSecond))
            .addAllField(List.of(otherField1, otherField2))
            .build();

    FileDescriptorProto file =
        FileDescriptorProto.newBuilder()
            .setPackage("protobuf")
            .addAllMessageType(List.of(outer, other))
            .setSourceCodeInfo(
                SourceCodeInfo.newBuilder()
                    .addLocation(oneofLocation(0, 0, "Outer choice."))
                    .addLocation(oneofLocation(1, 0, "Other first."))
                    .addLocation(oneofLocation(1, 1, "Other second.")))
            .build();

    ProtobufOneOfField outerOneOf =
        ProtobufOneOfField.oneOfBuilder()
            .fieldProto(outerField)
            .protobufMessage(ProtobufMessage.builder().fileProto(file).messageProto(outer).build())
            .build();
    ProtobufOneOfField otherSecondOneOf =
        ProtobufOneOfField.oneOfBuilder()
            .fieldProto(otherField2)
            .protobufMessage(ProtobufMessage.builder().fileProto(file).messageProto(other).build())
            .build();

    assertEquals(outerOneOf.comment(), "Outer choice.");
    assertEquals(otherSecondOneOf.comment(), "Other second.");
  }

  private static SourceCodeInfo.Location oneofLocation(
      int messageIndex, int oneofIndex, String comment) {
    return SourceCodeInfo.Location.newBuilder()
        .addPath(FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER)
        .addPath(messageIndex)
        .addPath(DescriptorProto.ONEOF_DECL_FIELD_NUMBER)
        .addPath(oneofIndex)
        .setLeadingComments(comment)
        .build();
  }
}
