package datahub.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProtobufUtils {
    private ProtobufUtils() { }

    public static String collapseLocationComments(DescriptorProtos.SourceCodeInfo.Location location) {
       String orig = Stream.concat(location.getLeadingDetachedCommentsList().stream(),
                       Stream.of(location.getLeadingComments(), location.getTrailingComments()))
               .filter(Objects::nonNull)
               .flatMap(line -> Arrays.stream(line.split("\n")))
               .map(line -> line.replaceFirst("^[*/ ]+", ""))
               .collect(Collectors.joining("\n"))
               .trim();

        /*
         * Sometimes DataHub doesn't like these strings. Not sure if its DataHub
         * or protobuf issue: https://github.com/protocolbuffers/protobuf/issues/4691
         *
         * We essentially smash utf8 chars to ascii here
         */
        return new String(orig.getBytes(StandardCharsets.ISO_8859_1));
    }

    public static ExtensionRegistry buildRegistry(DescriptorProtos.FileDescriptorSet fileSet) {
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        Map<String, DescriptorProtos.FileDescriptorProto> descriptorProtoMap = fileSet.getFileList().stream()
                .collect(Collectors.toMap(DescriptorProtos.FileDescriptorProto::getName, Function.identity()));
        Map<String, Descriptors.FileDescriptor> descriptorCache = new HashMap<>();

        fileSet.getFileList().forEach(fdp -> {
            try {
                Descriptors.FileDescriptor file = descriptorFromProto(fdp, descriptorProtoMap, descriptorCache);
                Stream.concat(file.getExtensions().stream(), file.getMessageTypes().stream().flatMap(msg -> msg.getExtensions().stream()))
                        .forEach(ext -> addToRegistry(fdp, ext, registry));
            } catch (Descriptors.DescriptorValidationException e) {
                e.printStackTrace();
            }
        });
        return registry;
    }

    private static void addToRegistry(DescriptorProtos.FileDescriptorProto fileDescriptorProto,
                                      Descriptors.FieldDescriptor fieldDescriptor, ExtensionRegistry registry) {
        if (fieldDescriptor.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            registry.add(fieldDescriptor);
        } else {
            fileDescriptorProto.getMessageTypeList().stream()
                    .filter(typ -> typ.getName().equals(fieldDescriptor.getMessageType().getName()))
                    .findFirst().ifPresent(messageType -> registry.add(fieldDescriptor, messageType.getDefaultInstanceForType()));
            fieldDescriptor.getMessageType().getFields()
                    .stream().filter(Descriptors.FieldDescriptor::isExtension)
                    .forEach(f -> addToRegistry(fileDescriptorProto, f, registry));
        }
    }

    /**
     * Recursively constructs file descriptors for all dependencies of the supplied proto and returns
     * a {@link Descriptors.FileDescriptor} for the supplied proto itself. For maximal efficiency, reuse the
     * descriptorCache argument across calls.
     */
    private static Descriptors.FileDescriptor descriptorFromProto(
            DescriptorProtos.FileDescriptorProto descriptorProto,
            Map<String, DescriptorProtos.FileDescriptorProto> descriptorProtoIndex,
            Map<String, Descriptors.FileDescriptor> descriptorCache) throws Descriptors.DescriptorValidationException {
        // First, check the cache.
        String descriptorName = descriptorProto.getName();
        if (descriptorCache.containsKey(descriptorName)) {
            return descriptorCache.get(descriptorName);
        }

        // Then, fetch all the required dependencies recursively.
        ImmutableList.Builder<Descriptors.FileDescriptor> dependencies = ImmutableList.builder();
        for (String dependencyName : descriptorProto.getDependencyList()) {
            if (!descriptorProtoIndex.containsKey(dependencyName)) {
                throw new IllegalArgumentException("Could not find dependency: " + dependencyName);
            }
            DescriptorProtos.FileDescriptorProto dependencyProto = descriptorProtoIndex.get(dependencyName);
            dependencies.add(descriptorFromProto(dependencyProto, descriptorProtoIndex, descriptorCache));
        }

        // Finally, construct the actual descriptor.
        Descriptors.FileDescriptor[] empty = new Descriptors.FileDescriptor[0];
        return Descriptors.FileDescriptor.buildFrom(descriptorProto, dependencies.build().toArray(empty), false);
    }

}

