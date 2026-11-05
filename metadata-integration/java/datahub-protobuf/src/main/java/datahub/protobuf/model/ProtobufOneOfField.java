package datahub.protobuf.model;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.UnionType;
import datahub.protobuf.ProtobufUtils;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;

@Getter
public class ProtobufOneOfField extends ProtobufField {
  public static final String NATIVE_TYPE = "oneof";
  public static final String FIELD_PATH_TYPE = "[type=union]";

  @Builder(builderMethodName = "oneOfBuilder")
  public ProtobufOneOfField(ProtobufMessage protobufMessage, FieldDescriptorProto fieldProto) {
    super(protobufMessage, fieldProto, null, null, null, null, null);
  }

  @Override
  public String name() {
    return oneOfProto().getName();
  }

  @Override
  public String fieldPathType() {
    return FIELD_PATH_TYPE;
  }

  @Override
  public String nativeType() {
    return NATIVE_TYPE;
  }

  @Override
  public boolean isMessage() {
    return false;
  }

  @Override
  public SchemaFieldDataType schemaFieldDataType() throws IllegalStateException {
    return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new UnionType()));
  }

  @Override
  public String comment() {
    return messageLocations()
        .filter(
            loc -> {
              if (loc.getPathCount() <= 3
                  || loc.getPath(2) != DescriptorProto.ONEOF_DECL_FIELD_NUMBER) {
                return false;
              }
              // messageLocations() spans every top-level message in the file, so resolve the
              // message this location belongs to before looking up the oneof by index.
              DescriptorProto messageType = fileProto().getMessageType(loc.getPath(1));
              int oneofIndex = loc.getPath(3);
              return oneofIndex < messageType.getOneofDeclCount()
                  && oneOfProto() == messageType.getOneofDecl(oneofIndex);
            })
        .map(ProtobufUtils::collapseLocationComments)
        .collect(Collectors.joining("\n"))
        .trim();
  }

  @Override
  public String toString() {
    return String.format("ProtobufOneOf[%s]", fullName());
  }
}
