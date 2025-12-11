/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.protobuf.model;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.SchemaFieldDataType;
import datahub.protobuf.ProtobufUtils;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;

@Getter
public class ProtobufEnum extends ProtobufMessage {

  private final EnumDescriptorProto enumProto;

  @Builder(builderMethodName = "enumBuilder")
  public ProtobufEnum(
      FileDescriptorProto fileProto, DescriptorProto messageProto, EnumDescriptorProto enumProto) {
    super(messageProto, null, fileProto);
    this.enumProto = enumProto;
  }

  @Override
  public String name() {
    return enumProto.getName();
  }

  @Override
  public String fieldPathType() {
    return "[type=enum]";
  }

  @Override
  public String nativeType() {
    return "enum";
  }

  @Override
  public SchemaFieldDataType schemaFieldDataType() throws IllegalStateException {
    return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new EnumType()));
  }

  @Override
  public String comment() {
    return messageLocations()
        .filter(
            loc ->
                loc.getPathCount() > 3
                    && loc.getPath(2) == DescriptorProto.ENUM_TYPE_FIELD_NUMBER
                    && enumProto == messageProto().getEnumType(loc.getPath(3)))
        .map(ProtobufUtils::collapseLocationComments)
        .collect(Collectors.joining("\n"))
        .trim();
  }

  @Override
  public String toString() {
    return String.format("ProtobufEnum[%s]", fullName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ProtobufEnum that = (ProtobufEnum) o;

    return getEnumProto().equals(that.getEnumProto());
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + getEnumProto().hashCode();
    return result;
  }
}
