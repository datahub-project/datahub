package datahub.protobuf.visitors;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.tag.TagProperties;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProtobufExtensionUtil {

    private ProtobufExtensionUtil() { }

    public static DescriptorProtos.FieldDescriptorProto extendProto(DescriptorProtos.FieldDescriptorProto proto, ExtensionRegistry registry) {
        try {
            return DescriptorProtos.FieldDescriptorProto.parseFrom(proto.toByteArray(), registry);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public enum DataHubMetadataType {
        PROPERTY, TAG, TAG_LIST, TERM, OWNER, DOMAIN;

        public static final String PROTOBUF_TYPE = "DataHubMetadataType";
    }

    public static Map<Descriptors.FieldDescriptor, Object> filterByDataHubType(Map<Descriptors.FieldDescriptor, Object> options,
                                                                               ExtensionRegistry registry, DataHubMetadataType filterType) {
        return options.entrySet().stream()
                .filter(entry -> {
                    DescriptorProtos.FieldDescriptorProto extendedProtoOptions = extendProto(entry.getKey().toProto(), registry);
                    Optional<DataHubMetadataType> dataHubMetadataType = extendedProtoOptions.getOptions().getAllFields().entrySet().stream()
                            .filter(extEntry -> extEntry.getKey().getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM)
                            .flatMap(extEntry -> {
                                if (extEntry.getKey().isRepeated()) {
                                    return ((Collection<Descriptors.EnumValueDescriptor>) extEntry.getValue()).stream();
                                } else {
                                    return Stream.of((Descriptors.EnumValueDescriptor) extEntry.getValue());
                                }
                            })
                            .filter(enumDesc -> enumDesc.getType().getFullName().endsWith("." + DataHubMetadataType.PROTOBUF_TYPE))
                            .map(enumDesc -> DataHubMetadataType.valueOf(enumDesc.getName()))
                            .filter(dhmt -> dhmt.equals(filterType))
                            .findFirst();

                    return filterType.equals(dataHubMetadataType.orElse(DataHubMetadataType.PROPERTY));
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Stream<Map.Entry<String, String>> getProperties(Descriptors.FieldDescriptor field, DescriptorProtos.DescriptorProto value)  {
        return value.getUnknownFields().asMap().entrySet().stream().map(unknown -> {
            Descriptors.FieldDescriptor fieldDesc = field.getMessageType().findFieldByNumber(unknown.getKey());
            String fieldValue = unknown.getValue().getLengthDelimitedList().stream().map(ByteString::toStringUtf8).collect(Collectors.joining(""));
            return Map.entry(String.join(".", field.getFullName(), fieldDesc.getName()), fieldValue);
        });
    }

    public static Stream<TagProperties> extractTagPropertiesFromOptions(Map<Descriptors.FieldDescriptor, Object> options, ExtensionRegistry registry) {
        Stream<TagProperties> tags = filterByDataHubType(options, registry, DataHubMetadataType.TAG).entrySet().stream()
                .filter(e -> e.getKey().isExtension())
                .flatMap(extEntry -> {
                    if (extEntry.getKey().isRepeated()) {
                        return ((Collection<?>) extEntry.getValue()).stream().map(v -> Map.entry(extEntry.getKey(), v));
                    } else {
                        return Stream.of(extEntry);
                    }
                })
                .map(entry -> {
                    switch (entry.getKey().getJavaType()) {
                        case STRING:
                            return  new TagProperties()
                                    .setName(String.format("%s.%s", entry.getKey().getName(), entry.getValue().toString()))
                                    .setDescription(entry.getKey().getFullName());
                        case BOOLEAN:
                            if ((boolean) entry.getValue()) {
                                return new TagProperties()
                                        .setName(entry.getKey().getName())
                                        .setDescription(String.format("%s is true.", entry.getKey().getFullName()));
                            }
                            return null;
                        case ENUM:
                            Descriptors.EnumValueDescriptor desc = (Descriptors.EnumValueDescriptor) entry.getValue();
                            String name = String.format("%s.%s", desc.getType().getName(), desc.getName());
                            String others = entry.getKey().getEnumType().getValues().stream()
                                    .map(Descriptors.EnumValueDescriptor::getName).collect(Collectors.joining(", "));
                            return new TagProperties()
                                    .setName(name)
                                    .setDescription(String.format("Enum %s of {%s}", name, others));
                        default:
                            return null;
                    }
                }).filter(Objects::nonNull);

        Stream<TagProperties> tagListTags = filterByDataHubType(options, registry, DataHubMetadataType.TAG_LIST).entrySet().stream()
                .filter(e -> e.getKey().isExtension())
                .flatMap(entry -> {
                    switch (entry.getKey().getJavaType()) {
                        case STRING:
                            return Arrays.stream(entry.getValue().toString().split(","))
                                    .map(t -> new TagProperties()
                                            .setName(t.trim())
                                            .setDescription(entry.getKey().getFullName()));
                        default:
                            return Stream.empty();
                    }
                }).filter(Objects::nonNull);

        return Stream.concat(tags, tagListTags);
    }

    public static Stream<GlossaryTermAssociation> extractTermAssociationsFromOptions(Map<Descriptors.FieldDescriptor, Object> options,
                                                                                     ExtensionRegistry registry) {
        return filterByDataHubType(options, registry, DataHubMetadataType.TERM).entrySet().stream()
                .filter(e -> e.getKey().isExtension())
                .flatMap(extEntry -> {
                    if (extEntry.getKey().isRepeated()) {
                        return ((Collection<?>) extEntry.getValue()).stream().map(v -> Map.entry(extEntry.getKey(), v));
                    } else {
                        return Stream.of(extEntry);
                    }
                })
                .map(entry -> {
                    switch (entry.getKey().getJavaType()) {
                        case STRING:
                            return new GlossaryTermAssociation()
                                    .setUrn(new GlossaryTermUrn(entry.getValue().toString()));
                        case ENUM:
                            Descriptors.EnumValueDescriptor desc = (Descriptors.EnumValueDescriptor) entry.getValue();
                            String name = String.format("%s.%s", desc.getType().getName(), desc.getName());
                            return new GlossaryTermAssociation()
                                    .setUrn(new GlossaryTermUrn(name));
                        default:
                            return null;
                    }
                }).filter(Objects::nonNull);
    }
}
