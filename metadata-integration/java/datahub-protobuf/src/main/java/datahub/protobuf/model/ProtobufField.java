package datahub.protobuf.model;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
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
import datahub.protobuf.ProtobufUtils;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
@AllArgsConstructor
public class ProtobufField implements ProtobufElement {

  private final ProtobufMessage protobufMessage;
  private final FieldDescriptorProto fieldProto;
  private final String nativeType;
  private final String fieldPathType;
  private final Boolean isMessageType;
  private final SchemaFieldDataType schemaFieldDataType;
  private final Boolean isNestedType;

  public OneofDescriptorProto oneOfProto() {
    if (fieldProto.hasOneofIndex()) {
      return protobufMessage.messageProto().getOneofDecl(fieldProto.getOneofIndex());
    }
    return null;
  }

  @Override
  public FileDescriptorProto fileProto() {
    return protobufMessage.fileProto();
  }

  @Override
  public DescriptorProto messageProto() {
    return protobufMessage.messageProto();
  }

  public String parentMessageName() {
    return protobufMessage.fullName();
  }

  @Override
  public String name() {
    return fieldProto.getName();
  }

  @Override
  public String fullName() {
    return String.join(".", parentMessageName(), name());
  }

  public String getNativeType() {
    return nativeType();
  }

  public int getNumber() {
    return fieldProto.getNumber();
  }

  @Override
  public String nativeType() {
    return Optional.ofNullable(nativeType)
        .orElseGet(
            () -> {
              if (fieldProto.getTypeName().isEmpty()) {
                return fieldProto.getType().name().split("_")[1].toLowerCase();
              } else {
                return fieldProto.getTypeName().replaceFirst("^[.]", "");
              }
            });
  }

  @Override
  public String fieldPathType() {
    return Optional.ofNullable(fieldPathType)
        .orElseGet(
            () -> {
              final String pathType;

              switch (fieldProto.getType()) {
                case TYPE_DOUBLE:
                  pathType = "double";
                  break;
                case TYPE_FLOAT:
                  pathType = "float";
                  break;
                case TYPE_SFIXED64:
                case TYPE_FIXED64:
                case TYPE_UINT64:
                case TYPE_INT64:
                case TYPE_SINT64:
                  pathType = "long";
                  break;
                case TYPE_FIXED32:
                case TYPE_SFIXED32:
                case TYPE_INT32:
                case TYPE_UINT32:
                case TYPE_SINT32:
                  pathType = "int";
                  break;
                case TYPE_BYTES:
                  pathType = "bytes";
                  break;
                case TYPE_ENUM:
                  pathType = "enum";
                  break;
                case TYPE_BOOL:
                  pathType = "boolean";
                  break;
                case TYPE_STRING:
                  pathType = "string";
                  break;
                case TYPE_GROUP:
                case TYPE_MESSAGE:
                  pathType = nativeType().replace(".", "_");
                  break;
                default:
                  throw new IllegalStateException(
                      String.format(
                          "Unexpected FieldDescriptorProto => FieldPathType %s",
                          fieldProto.getType()));
              }

              StringArray fieldPath = new StringArray();

              if (schemaFieldDataType().getType().isArrayType()) {
                fieldPath.add("[type=array]");
              }

              fieldPath.add(String.format("[type=%s]", pathType));

              return String.join(".", fieldPath);
            });
  }

  public boolean isMessage() {
    return Optional.ofNullable(isMessageType)
        .orElseGet(() -> fieldProto.getType().equals(FieldDescriptorProto.Type.TYPE_MESSAGE));
  }

  public int sortWeight() {
    return messageProto().getFieldList().indexOf(fieldProto) + 1;
  }

  public SchemaFieldDataType schemaFieldDataType() throws IllegalStateException {
    return Optional.ofNullable(schemaFieldDataType)
        .orElseGet(
            () -> {
              final SchemaFieldDataType.Type fieldType;

              switch (fieldProto.getType()) {
                case TYPE_DOUBLE:
                case TYPE_FLOAT:
                case TYPE_INT64:
                case TYPE_UINT64:
                case TYPE_INT32:
                case TYPE_UINT32:
                case TYPE_SINT32:
                case TYPE_SINT64:
                  fieldType = SchemaFieldDataType.Type.create(new NumberType());
                  break;
                case TYPE_GROUP:
                case TYPE_MESSAGE:
                  fieldType = SchemaFieldDataType.Type.create(new RecordType());
                  break;
                case TYPE_BYTES:
                  fieldType = SchemaFieldDataType.Type.create(new BytesType());
                  break;
                case TYPE_ENUM:
                  fieldType = SchemaFieldDataType.Type.create(new EnumType());
                  break;
                case TYPE_BOOL:
                  fieldType = SchemaFieldDataType.Type.create(new BooleanType());
                  break;
                case TYPE_STRING:
                  fieldType = SchemaFieldDataType.Type.create(new StringType());
                  break;
                case TYPE_FIXED64:
                case TYPE_FIXED32:
                case TYPE_SFIXED32:
                case TYPE_SFIXED64:
                  fieldType = SchemaFieldDataType.Type.create(new FixedType());
                  break;
                default:
                  throw new IllegalStateException(
                      String.format(
                          "Unexpected FieldDescriptorProto => SchemaFieldDataType: %s",
                          fieldProto.getType()));
              }

              if (fieldProto.getLabel().equals(FieldDescriptorProto.Label.LABEL_REPEATED)) {
                return new SchemaFieldDataType()
                    .setType(
                        SchemaFieldDataType.Type.create(
                            new ArrayType().setNestedType(new StringArray())));
              }

              return new SchemaFieldDataType().setType(fieldType);
            });
  }

  @Override
  public Stream<SourceCodeInfo.Location> messageLocations() {
    List<SourceCodeInfo.Location> fileLocations = fileProto().getSourceCodeInfo().getLocationList();
    return fileLocations.stream()
        .filter(
            loc ->
                loc.getPathCount() > 1
                    && loc.getPath(0) == FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER);
  }

  @Override
  public String comment() {
    return messageLocations()
        .filter(location -> location.getPathCount() > 3)
        .filter(
            location ->
                !ProtobufUtils.collapseLocationComments(location).isEmpty()
                    && !isEnumType(location.getPathList()))
        .filter(
            location -> {
              List<Integer> pathList = location.getPathList();
              DescriptorProto messageType = fileProto().getMessageType(pathList.get(1));

              if (!isNestedType
                  && location.getPath(2) == DescriptorProto.FIELD_FIELD_NUMBER
                  && fieldProto == messageType.getField(location.getPath(3))) {
                return true;
              } else if (isNestedType
                  && location.getPath(2) == DescriptorProto.NESTED_TYPE_FIELD_NUMBER
                  && fieldProto == getNestedTypeFields(pathList, messageType)) {
                return true;
              }
              return false;
            })
        .map(ProtobufUtils::collapseLocationComments)
        .collect(Collectors.joining("\n"))
        .trim();
  }

  private FieldDescriptorProto getNestedTypeFields(
      List<Integer> pathList, DescriptorProto messageType) {
    int pathSize = pathList.size();
    List<Integer> nestedValues = new ArrayList<>(pathSize);

    for (int index = 0; index < pathSize; index++) {
      if (index > 1
          && index % 2 == 0
          && pathList.get(index) == DescriptorProto.NESTED_TYPE_FIELD_NUMBER) {
        nestedValues.add(pathList.get(index + 1));
      }
    }

    for (Integer value : nestedValues) {
      messageType = messageType.getNestedType(value);
    }

    int fieldIndex = pathList.get(pathList.size() - 1);
    if (isFieldPath(pathList)
        && pathSize % 2 == 0
        && fieldIndex < messageType.getFieldList().size()) {
      return messageType.getField(fieldIndex);
    }

    return null;
  }

  private boolean isFieldPath(List<Integer> pathList) {
    return pathList.get(pathList.size() - 2) == DescriptorProto.FIELD_FIELD_NUMBER;
  }

  private boolean isEnumType(List<Integer> pathList) {
    for (int index = 0; index < pathList.size(); index++) {
      if (index > 1
          && index % 2 == 0
          && pathList.get(index) == DescriptorProto.ENUM_TYPE_FIELD_NUMBER) {
        return true;
      }
    }
    return false;
  }

  @Override
  public <T> Stream<T> accept(ProtobufModelVisitor<T> visitor, VisitContext context) {
    return visitor.visitField(this, context);
  }

  @Override
  public String toString() {
    return String.format("ProtobufField[%s]", fullName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProtobufElement that = (ProtobufElement) o;

    return fullName().equals(that.fullName());
  }

  @Override
  public int hashCode() {
    return fullName().hashCode();
  }

  public boolean isEnum() {
    return getFieldProto().getType() == DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM;
  }

  public Optional<DescriptorProtos.EnumDescriptorProto> getEnumDescriptor() {
    if (!isEnum()) {
      return Optional.empty();
    }

    String enumTypeName = getFieldProto().getTypeName();
    String shortEnumTypeName = enumTypeName.substring(enumTypeName.lastIndexOf('.') + 1);

    return getProtobufMessage().fileProto().getEnumTypeList().stream()
        .filter(enumType -> enumType.getName().equals(shortEnumTypeName))
        .findFirst();
  }

  public List<DescriptorProtos.EnumValueDescriptorProto> getEnumValues() {
    return getEnumDescriptor()
        .map(DescriptorProtos.EnumDescriptorProto::getValueList)
        .orElse(Collections.emptyList());
  }

  public Map<String, String> getEnumValuesWithComments() {
    Optional<DescriptorProtos.EnumDescriptorProto> enumProtoOpt = getEnumDescriptor();
    if (enumProtoOpt.isEmpty()) {
      return Collections.emptyMap();
    }

    DescriptorProtos.EnumDescriptorProto enumProto = enumProtoOpt.get();
    Map<String, String> valueComments = new LinkedHashMap<>();
    List<DescriptorProtos.EnumValueDescriptorProto> values = enumProto.getValueList();
    List<DescriptorProtos.SourceCodeInfo.Location> locations =
        getProtobufMessage().fileProto().getSourceCodeInfo().getLocationList();

    int enumIndex = getProtobufMessage().fileProto().getEnumTypeList().indexOf(enumProto);

    for (int i = 0; i < values.size(); i++) {
      DescriptorProtos.EnumValueDescriptorProto value = values.get(i);
      int finalI = i;
      String comment =
          locations.stream()
              .filter(loc -> isEnumValueLocation(loc, enumIndex, finalI))
              .findFirst()
              .map(ProtobufUtils::collapseLocationComments)
              .orElse("");

      valueComments.put(value.getName(), comment);
    }

    return valueComments;
  }

  private boolean isEnumValueLocation(
      DescriptorProtos.SourceCodeInfo.Location location, int enumIndex, int valueIndex) {
    return location.getPathCount() > 3
        && location.getPath(0) == DescriptorProtos.FileDescriptorProto.ENUM_TYPE_FIELD_NUMBER
        && location.getPath(1) == enumIndex
        && location.getPath(2) == DescriptorProtos.EnumDescriptorProto.VALUE_FIELD_NUMBER
        && location.getPath(3) == valueIndex;
  }
}
