package datahub.protobuf.model;

import static org.testng.Assert.*;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.linkedin.schema.MapType;
import com.linkedin.schema.RecordType;
import com.linkedin.schema.SchemaFieldDataType;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class ProtobufMessageTest {

  @Test
  public void messageTest() {
    DescriptorProto expectedMessage = DescriptorProto.newBuilder().setName("message1").build();
    DescriptorProto expectedParentMessage1 =
        DescriptorProto.newBuilder()
            .setName("messageParent1")
            .addNestedType(expectedMessage)
            .build();

    FileDescriptorProto expectedFile =
        FileDescriptorProto.newBuilder()
            .addMessageType(expectedMessage)
            .setPackage("protobuf")
            .build();

    ProtobufMessage testParent =
        ProtobufMessage.builder()
            .messageProto(expectedParentMessage1)
            .fileProto(expectedFile)
            .build();
    ProtobufMessage test =
        ProtobufMessage.builder()
            .messageProto(expectedMessage)
            .parentMessageProto(expectedParentMessage1)
            .fileProto(expectedFile)
            .build();

    assertEquals("messageParent1", testParent.name());
    assertEquals("protobuf.messageParent1", testParent.fullName());
    assertEquals("protobuf.messageParent1", testParent.nativeType());
    assertEquals("[type=protobuf_messageParent1]", testParent.fieldPathType());
    assertEquals(expectedFile, testParent.fileProto());
    assertEquals(expectedParentMessage1, testParent.messageProto());
    assertEquals(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())),
        testParent.schemaFieldDataType());
    assertEquals("ProtobufMessage[protobuf.messageParent1]", testParent.toString());

    assertEquals("message1", test.name());
    assertEquals("protobuf.messageParent1.message1", test.fullName());
    assertEquals("protobuf.messageParent1.message1", test.nativeType());
    assertEquals("[type=protobuf_messageParent1_message1]", test.fieldPathType());
    assertEquals(expectedFile, test.fileProto());
    assertEquals(expectedMessage, test.messageProto());
    assertEquals(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())),
        test.schemaFieldDataType());
    assertEquals("ProtobufMessage[protobuf.messageParent1.message1]", test.toString());
  }

  @Test
  public void mapTest() {
    DescriptorProto expectedMap = DescriptorProto.newBuilder().setName("MapFieldEntry").build();
    DescriptorProto expectedParentMessage1 =
        DescriptorProto.newBuilder().setName("messageParent1").addNestedType(expectedMap).build();

    FileDescriptorProto expectedFile =
        FileDescriptorProto.newBuilder().addMessageType(expectedMap).setPackage("protobuf").build();

    ProtobufMessage testParent =
        ProtobufMessage.builder()
            .messageProto(expectedParentMessage1)
            .fileProto(expectedFile)
            .build();
    ProtobufMessage testMap =
        ProtobufMessage.builder()
            .messageProto(expectedMap)
            .parentMessageProto(expectedParentMessage1)
            .fileProto(expectedFile)
            .build();

    assertEquals("messageParent1", testParent.name());
    assertEquals("protobuf.messageParent1", testParent.fullName());
    assertEquals("protobuf.messageParent1", testParent.nativeType());
    assertEquals("[type=protobuf_messageParent1]", testParent.fieldPathType());
    assertEquals(expectedFile, testParent.fileProto());
    assertEquals(expectedParentMessage1, testParent.messageProto());
    assertEquals(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType())),
        testParent.schemaFieldDataType());
    assertEquals("ProtobufMessage[protobuf.messageParent1]", testParent.toString());

    assertEquals("MapFieldEntry", testMap.name());
    assertEquals("protobuf.messageParent1.MapFieldEntry", testMap.fullName());
    assertEquals("protobuf.messageParent1.MapFieldEntry", testMap.nativeType());
    assertEquals("[type=protobuf_messageParent1_MapFieldEntry]", testMap.fieldPathType());
    assertEquals(expectedFile, testMap.fileProto());
    assertEquals(expectedMap, testMap.messageProto());
    assertEquals(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new MapType())),
        testMap.schemaFieldDataType());
    assertEquals("ProtobufMessage[protobuf.messageParent1.MapFieldEntry]", testMap.toString());
  }

  @Test
  public void messageEqualityTest() {
    DescriptorProto expectedMessage1 = DescriptorProto.newBuilder().setName("message1").build();
    DescriptorProto expectedMessage2 = DescriptorProto.newBuilder().setName("message2").build();
    DescriptorProto expectedMessage1Dup = DescriptorProto.newBuilder().setName("message1").build();

    FileDescriptorProto expectedFile =
        FileDescriptorProto.newBuilder()
            .addAllMessageType(List.of(expectedMessage1, expectedMessage2, expectedMessage1Dup))
            .setPackage("protobuf")
            .build();

    ProtobufMessage test1 =
        ProtobufMessage.builder().messageProto(expectedMessage1).fileProto(expectedFile).build();
    ProtobufMessage test2 =
        ProtobufMessage.builder().messageProto(expectedMessage2).fileProto(expectedFile).build();
    ProtobufMessage test1Dup =
        ProtobufMessage.builder().messageProto(expectedMessage1Dup).fileProto(expectedFile).build();

    assertEquals(test1, test1Dup);
    assertNotEquals(test1, test2);
    assertEquals(
        Set.of(test1, test2), Stream.of(test1, test2, test1Dup).collect(Collectors.toSet()));
  }

  @Test
  public void majorVersionTest() {
    DescriptorProto expectedMessage1 = DescriptorProto.newBuilder().setName("message1").build();

    FileDescriptorProto expectedFile1 =
        FileDescriptorProto.newBuilder()
            .setName("zendesk/v1/platform/test.proto")
            .setPackage("protobuf")
            .build();
    ProtobufMessage test1 =
        ProtobufMessage.builder().messageProto(expectedMessage1).fileProto(expectedFile1).build();
    assertEquals(1, test1.majorVersion());

    FileDescriptorProto expectedFile2 =
        FileDescriptorProto.newBuilder()
            .setName("zendesk/v2/platform/test.proto")
            .setPackage("protobuf")
            .build();
    ProtobufMessage test2 =
        ProtobufMessage.builder().messageProto(expectedMessage1).fileProto(expectedFile2).build();
    assertEquals(2, test2.majorVersion());

    FileDescriptorProto expectedFile3 =
        FileDescriptorProto.newBuilder()
            .setName("zendesk/platform/test.proto")
            .setPackage("protobuf")
            .build();
    ProtobufMessage test3 =
        ProtobufMessage.builder().messageProto(expectedMessage1).fileProto(expectedFile3).build();
    assertEquals(1, test3.majorVersion());
  }
}
