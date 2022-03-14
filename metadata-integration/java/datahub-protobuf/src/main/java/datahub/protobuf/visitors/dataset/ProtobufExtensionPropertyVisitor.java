package datahub.protobuf.visitors.dataset;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.VisitContext;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ProtobufExtensionPropertyVisitor implements ProtobufModelVisitor<DatasetProperties> {

    @Override
    public Stream<DatasetProperties> visitGraph(VisitContext context) {
        Map<String, String> properties = ProtobufExtensionUtil.filterByDataHubType(context.root().messageProto()
                        .getOptions().getAllFields(), context.getGraph().getRegistry(), ProtobufExtensionUtil.DataHubMetadataType.PROPERTY)
                .entrySet().stream().flatMap(fd -> {
                    if (fd.getKey().getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                        return Stream.of(Map.entry(fd.getKey().getName(), fd.getValue().toString()));
                    } else {
                        Descriptors.FieldDescriptor field = fd.getKey();
                        DescriptorProtos.DescriptorProto value = (DescriptorProtos.DescriptorProto) fd.getValue();
                        return getProperties(field, value);
                    }
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return Stream.of(new DatasetProperties().setCustomProperties(new StringMap(properties)));
    }

    private static Stream<Map.Entry<String, String>> getProperties(Descriptors.FieldDescriptor field, DescriptorProtos.DescriptorProto value)  {
       return value.getUnknownFields().asMap().entrySet().stream().map(unknown -> {
            Descriptors.FieldDescriptor fieldDesc = field.getMessageType().findFieldByNumber(unknown.getKey());
            String fieldValue = unknown.getValue().getLengthDelimitedList().stream().map(ByteString::toStringUtf8).collect(Collectors.joining(""));
            return Map.entry(String.join(".", field.getFullName(), fieldDesc.getName()), fieldValue);
        });
    }
}
