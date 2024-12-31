package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.ProtobufUtils.getMessageOptions;
import static datahub.protobuf.visitors.ProtobufExtensionUtil.getProperties;

import com.google.gson.Gson;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PropertyVisitor implements ProtobufModelVisitor<DatasetProperties> {
  private static final Gson GSON = new Gson();

  @Override
  public Stream<DatasetProperties> visitGraph(VisitContext context) {
    Map<String, String> properties =
        ProtobufExtensionUtil.filterByDataHubType(
                getMessageOptions(context.root().messageProto()),
                context.getGraph().getRegistry(),
                ProtobufExtensionUtil.DataHubMetadataType.PROPERTY)
            .stream()
            .flatMap(
                fd -> {
                  if (fd.getKey().getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    if (fd.getKey().isRepeated()) {
                      return Stream.of(
                          Map.entry(
                              fd.getKey().getName(),
                              GSON.toJson(
                                  ((Collection<?>) fd.getValue())
                                      .stream()
                                          .map(Object::toString)
                                          .collect(Collectors.toList()))));
                    } else {
                      return Stream.of(Map.entry(fd.getKey().getName(), fd.getValue().toString()));
                    }
                  } else {
                    Descriptors.FieldDescriptor field = fd.getKey();
                    DescriptorProtos.DescriptorProto value =
                        (DescriptorProtos.DescriptorProto) fd.getValue();
                    return getProperties(field, value);
                  }
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return Stream.of(new DatasetProperties().setCustomProperties(new StringMap(properties)));
  }
}
