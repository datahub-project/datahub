package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.ProtobufUtils.getMessageOptions;

import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.util.Pair;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class PropertyVisitor implements ProtobufModelVisitor<DatasetProperties> {
  private static final Gson GSON = new Gson();

  @Override
  public Stream<DatasetProperties> visitGraph(VisitContext context) {
    Map<String, String> properties = new HashMap<>();

    List<Pair<Descriptors.FieldDescriptor, Object>> propertyOptions =
        ProtobufExtensionUtil.filterByDataHubType(
            getMessageOptions(context.root().messageProto()),
            context.getGraph().getRegistry(),
            ProtobufExtensionUtil.DataHubMetadataType.PROPERTY);

    for (Pair<Descriptors.FieldDescriptor, Object> fd : propertyOptions) {
      var fieldDescriptor = fd.getKey();
      if (fieldDescriptor.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        if (fieldDescriptor.isRepeated()) {
          List<String> stringValues = new ArrayList<>();
          for (Object item : (Collection<?>) fd.getValue()) {
            stringValues.add(item.toString());
          }
          properties.put(fieldDescriptor.getName(), GSON.toJson(stringValues));
        } else {
          properties.put(fieldDescriptor.getName(), fd.getValue().toString());
        }
      }
    }

    return Stream.of(new DatasetProperties().setCustomProperties(new StringMap(properties)));
  }
}
