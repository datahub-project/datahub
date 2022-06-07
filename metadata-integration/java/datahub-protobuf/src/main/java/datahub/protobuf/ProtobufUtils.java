package datahub.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.linkedin.util.Pair;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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

    /*
     * Reflection used to prevent an exception deep inside the protobuf library due to a getter method
     * mutating the json name field and causing an equality check to fail between an instance that has and has not
     * had the getter called.
     *
     * https://github.com/protocolbuffers/protobuf/blob/main/java/core/src/main/java/com/google/protobuf/Descriptors.java#L1105
     *
     * java.lang.IllegalArgumentException: FieldDescriptors can only be compared to other FieldDescriptors for fields of the same message type.
     *      at com.google.protobuf.Descriptors$FieldDescriptor.compareTo(Descriptors.java:1344)
     *      at com.google.protobuf.Descriptors$FieldDescriptor.compareTo(Descriptors.java:1057)
     *      at java.base/java.util.TreeMap.put(TreeMap.java:566)
     *      at java.base/java.util.AbstractMap.putAll(AbstractMap.java:281)
     *      at java.base/java.util.TreeMap.putAll(TreeMap.java:325)
     *      at com.google.protobuf.GeneratedMessageV3$ExtendableMessage.getAllFields(GeneratedMessageV3.java:1240)
     *
     */
    private static final Method FIELD_OPT_EXT_FIELDS_METHOD;
    private static final Method FIELD_OPT_ALL_FIELD_METHOD;
    private static final Method MSG_OPT_EXT_FIELDS_METHOD;
    private static final Method MSG_OPT_ALL_FIELD_METHOD;
    static {
        try {
            FIELD_OPT_EXT_FIELDS_METHOD = DescriptorProtos.FieldOptions.class.getSuperclass()
                    .getDeclaredMethod("getExtensionFields");
            FIELD_OPT_EXT_FIELDS_METHOD.setAccessible(true);

            FIELD_OPT_ALL_FIELD_METHOD = DescriptorProtos.FieldOptions.class.getSuperclass().getSuperclass()
                    .getDeclaredMethod("getAllFieldsMutable", boolean.class);
            FIELD_OPT_ALL_FIELD_METHOD.setAccessible(true);

            MSG_OPT_EXT_FIELDS_METHOD = DescriptorProtos.MessageOptions.class.getSuperclass()
                    .getDeclaredMethod("getExtensionFields");
            MSG_OPT_EXT_FIELDS_METHOD.setAccessible(true);

            MSG_OPT_ALL_FIELD_METHOD = DescriptorProtos.MessageOptions.class.getSuperclass().getSuperclass()
                    .getDeclaredMethod("getAllFieldsMutable", boolean.class);
            MSG_OPT_ALL_FIELD_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Pair<Descriptors.FieldDescriptor, Object>> getFieldOptions(DescriptorProtos.FieldDescriptorProto fieldProto) {
        try {
            LinkedList<Pair<Descriptors.FieldDescriptor, Object>> options = new LinkedList<>();

            options.addAll(((Map<Descriptors.FieldDescriptor, Object>) FIELD_OPT_EXT_FIELDS_METHOD.invoke(fieldProto.getOptions()))
                    .entrySet()
                    .stream()
                    .map(e -> Pair.of(e.getKey(), e.getValue()))
                    .collect(Collectors.toList()));

            options.addAll(((Map<Descriptors.FieldDescriptor, Object>) FIELD_OPT_ALL_FIELD_METHOD.invoke(fieldProto.getOptions(), false))
                    .entrySet()
                    .stream()
                    .map(e -> Pair.of(e.getKey(), e.getValue()))
                    .collect(Collectors.toList()));

            return options;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Pair<Descriptors.FieldDescriptor, Object>> getMessageOptions(DescriptorProtos.DescriptorProto messageProto) {
        try {
            LinkedList<Pair<Descriptors.FieldDescriptor, Object>> options = new LinkedList<>();

            options.addAll(((Map<Descriptors.FieldDescriptor, Object>) MSG_OPT_EXT_FIELDS_METHOD.invoke(messageProto.getOptions()))
                    .entrySet()
                    .stream()
                    .map(e -> Pair.of(e.getKey(), e.getValue()))
                    .collect(Collectors.toList()));

            options.addAll(((Map<Descriptors.FieldDescriptor, Object>) MSG_OPT_ALL_FIELD_METHOD.invoke(messageProto.getOptions(),
                    false))
                    .entrySet()
                    .stream()
                    .map(e -> Pair.of(e.getKey(), e.getValue()))
                    .collect(Collectors.toList()));

            return options;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
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
        Descriptors.FileDescriptor descript = Descriptors.FileDescriptor.buildFrom(descriptorProto, dependencies.build().toArray(empty), false);
        descriptorCache.put(descript.getName(), descript);
        return descript;
    }

}

