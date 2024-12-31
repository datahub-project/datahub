package datahub.protobuf.visitors;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.tag.TagProperties;
import com.linkedin.util.Pair;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProtobufExtensionUtil {

  private ProtobufExtensionUtil() {}

  public static DescriptorProtos.FieldDescriptorProto extendProto(
      DescriptorProtos.FieldDescriptorProto proto, ExtensionRegistry registry) {
    try {
      return DescriptorProtos.FieldDescriptorProto.parseFrom(proto.toByteArray(), registry);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public enum DataHubMetadataType {
    PROPERTY,
    TAG,
    TAG_LIST,
    TERM,
    OWNER,
    DOMAIN,
    DEPRECATION;

    public static final String PROTOBUF_TYPE = "DataHubMetadataType";
  }

  public static List<Pair<Descriptors.FieldDescriptor, Object>> filterByDataHubType(
      List<Pair<Descriptors.FieldDescriptor, Object>> options,
      ExtensionRegistry registry,
      DataHubMetadataType filterType) {
    return options.stream()
        .filter(
            entry -> {
              DescriptorProtos.FieldDescriptorProto extendedProtoOptions =
                  extendProto(entry.getKey().toProto(), registry);
              Optional<DataHubMetadataType> dataHubMetadataType =
                  extendedProtoOptions.getOptions().getAllFields().entrySet().stream()
                      .filter(
                          extEntry ->
                              extEntry.getKey().getJavaType()
                                  == Descriptors.FieldDescriptor.JavaType.ENUM)
                      .flatMap(
                          extEntry -> {
                            if (extEntry.getKey().isRepeated()) {
                              return ((Collection<Descriptors.EnumValueDescriptor>)
                                      extEntry.getValue())
                                  .stream();
                            } else {
                              return Stream.of(
                                  (Descriptors.EnumValueDescriptor) extEntry.getValue());
                            }
                          })
                      .filter(
                          enumDesc ->
                              enumDesc
                                  .getType()
                                  .getFullName()
                                  .endsWith("." + DataHubMetadataType.PROTOBUF_TYPE))
                      .map(enumDesc -> DataHubMetadataType.valueOf(enumDesc.getName()))
                      .filter(dhmt -> dhmt.equals(filterType))
                      .findFirst();

              return filterType.equals(dataHubMetadataType.orElse(DataHubMetadataType.PROPERTY));
            })
        .collect(Collectors.toList());
  }

  public static Stream<Map.Entry<String, String>> getProperties(
      Descriptors.FieldDescriptor field, DescriptorProtos.DescriptorProto value) {
    return value.getUnknownFields().asMap().entrySet().stream()
        .map(
            unknown -> {
              Descriptors.FieldDescriptor fieldDesc =
                  field.getMessageType().findFieldByNumber(unknown.getKey());
              String fieldValue =
                  unknown.getValue().getLengthDelimitedList().stream()
                      .map(ByteString::toStringUtf8)
                      .collect(Collectors.joining(""));
              return Map.entry(
                  String.join(".", field.getFullName(), fieldDesc.getName()), fieldValue);
            });
  }

  public static Stream<TagProperties> extractTagPropertiesFromOptions(
      List<Pair<Descriptors.FieldDescriptor, Object>> options, ExtensionRegistry registry) {
    Stream<TagProperties> tags =
        filterByDataHubType(options, registry, DataHubMetadataType.TAG).stream()
            .filter(e -> e.getKey().isExtension())
            .flatMap(
                extEntry -> {
                  if (extEntry.getKey().isRepeated()) {
                    return ((Collection<?>) extEntry.getValue())
                        .stream().map(v -> Pair.of(extEntry.getKey(), v));
                  } else {
                    return Stream.of(extEntry);
                  }
                })
            .map(
                entry -> {
                  switch (entry.getKey().getJavaType()) {
                    case STRING:
                      return new TagProperties()
                          .setName(
                              String.format(
                                  "%s.%s", entry.getKey().getName(), entry.getValue().toString()))
                          .setDescription(entry.getKey().getFullName());
                    case BOOLEAN:
                      if ((boolean) entry.getValue()) {
                        return new TagProperties()
                            .setName(entry.getKey().getName())
                            .setDescription(
                                String.format("%s is true.", entry.getKey().getFullName()));
                      }
                      return null;
                    case ENUM:
                      Descriptors.EnumValueDescriptor desc =
                          (Descriptors.EnumValueDescriptor) entry.getValue();
                      String name =
                          String.format("%s.%s", desc.getType().getName(), desc.getName());
                      String others =
                          entry.getKey().getEnumType().getValues().stream()
                              .map(Descriptors.EnumValueDescriptor::getName)
                              .collect(Collectors.joining(", "));
                      return new TagProperties()
                          .setName(name)
                          .setDescription(String.format("Enum %s of {%s}", name, others));
                    default:
                      return null;
                  }
                })
            .filter(Objects::nonNull);

    Stream<TagProperties> tagListTags =
        filterByDataHubType(options, registry, DataHubMetadataType.TAG_LIST).stream()
            .filter(e -> e.getKey().isExtension())
            .flatMap(
                entry -> {
                  switch (entry.getKey().getJavaType()) {
                    case STRING:
                      return Arrays.stream(entry.getValue().toString().split(","))
                          .map(
                              t ->
                                  new TagProperties()
                                      .setName(t.trim())
                                      .setDescription(entry.getKey().getFullName()));
                    default:
                      return Stream.empty();
                  }
                })
            .filter(Objects::nonNull);

    Stream<TagProperties> deprecationTag;
    if (options.stream()
        .anyMatch(
            opt ->
                opt.getKey().getFullName().endsWith(".deprecated")
                    && opt.getKey().getFullName().startsWith("google.protobuf.")
                    && opt.getKey().getJavaType() == Descriptors.FieldDescriptor.JavaType.BOOLEAN
                    && (Boolean) opt.getValue())) {
      deprecationTag = Stream.of(new TagProperties().setName("deprecated").setColorHex("#FF0000"));
    } else {
      deprecationTag = Stream.empty();
    }

    return Stream.of(tags, tagListTags, deprecationTag)
        .reduce(Stream::concat)
        .orElse(Stream.empty());
  }

  public static Stream<GlossaryTermAssociation> extractTermAssociationsFromOptions(
      List<Pair<Descriptors.FieldDescriptor, Object>> fieldOptions, ExtensionRegistry registry) {
    return filterByDataHubType(fieldOptions, registry, DataHubMetadataType.TERM).stream()
        .filter(e -> e.getKey().isExtension())
        .flatMap(
            extEntry -> {
              if (extEntry.getKey().isRepeated()) {
                return ((Collection<?>) extEntry.getValue())
                    .stream().map(v -> Pair.of(extEntry.getKey(), v));
              } else {
                return Stream.of(extEntry);
              }
            })
        .map(
            entry -> {
              switch (entry.getKey().getJavaType()) {
                case STRING:
                  return new GlossaryTermAssociation()
                      .setUrn(new GlossaryTermUrn(entry.getValue().toString()));
                case ENUM:
                  Descriptors.EnumValueDescriptor desc =
                      (Descriptors.EnumValueDescriptor) entry.getValue();
                  String name = String.format("%s.%s", desc.getType().getName(), desc.getName());
                  return new GlossaryTermAssociation().setUrn(new GlossaryTermUrn(name));
                default:
                  return null;
              }
            })
        .filter(Objects::nonNull);
  }
}
