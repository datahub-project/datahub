/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.protobuf.model;

import static org.testng.Assert.*;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.SchemaFieldDataType;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class ProtobufEnumTest {

  @Test
  public void enumTest() {
    EnumDescriptorProto expectedEnum = EnumDescriptorProto.newBuilder().setName("enum1").build();
    DescriptorProto expectedMessage = DescriptorProto.newBuilder().build();
    FileDescriptorProto expectedFile =
        FileDescriptorProto.newBuilder()
            .addMessageType(expectedMessage)
            .setPackage("protobuf")
            .addEnumType(expectedEnum)
            .build();

    ProtobufEnum test =
        ProtobufEnum.enumBuilder()
            .enumProto(expectedEnum)
            .messageProto(expectedMessage)
            .fileProto(expectedFile)
            .build();

    assertEquals("enum1", test.name());
    assertEquals("protobuf.enum1", test.fullName());
    assertEquals("[type=enum]", test.fieldPathType());
    assertEquals("enum", test.nativeType());
    assertEquals(expectedMessage, test.messageProto());
    assertEquals(expectedFile, test.fileProto());
    assertEquals(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new EnumType())),
        test.schemaFieldDataType());
    assertEquals("ProtobufEnum[protobuf.enum1]", test.toString());
    assertEquals("", test.comment());
  }

  @Test
  public void enumEqualityTest() {
    EnumDescriptorProto enum1 = EnumDescriptorProto.newBuilder().setName("enum1").build();
    EnumDescriptorProto enum2 = EnumDescriptorProto.newBuilder().setName("enum2").build();
    EnumDescriptorProto enum1Dup = EnumDescriptorProto.newBuilder().setName("enum1").build();

    DescriptorProto expectedMessage = DescriptorProto.newBuilder().build();
    FileDescriptorProto expectedFile =
        FileDescriptorProto.newBuilder()
            .addMessageType(expectedMessage)
            .setPackage("protobuf")
            .addAllEnumType(List.of(enum1, enum2, enum1Dup))
            .build();

    ProtobufEnum test1 =
        ProtobufEnum.enumBuilder()
            .enumProto(enum1)
            .messageProto(expectedMessage)
            .fileProto(expectedFile)
            .build();
    ProtobufEnum test2 =
        ProtobufEnum.enumBuilder()
            .enumProto(enum2)
            .messageProto(expectedMessage)
            .fileProto(expectedFile)
            .build();
    ProtobufEnum test1Dup =
        ProtobufEnum.enumBuilder()
            .enumProto(enum1Dup)
            .messageProto(expectedMessage)
            .fileProto(expectedFile)
            .build();

    assertEquals(test1, test1Dup);
    assertNotEquals(test1, test2);
    assertEquals(
        Set.of(test1, test2), Stream.of(test1, test2, test1Dup).collect(Collectors.toSet()));
  }
}
