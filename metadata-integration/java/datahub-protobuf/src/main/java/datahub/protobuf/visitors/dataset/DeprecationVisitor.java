package datahub.protobuf.visitors.dataset;

import static datahub.protobuf.ProtobufUtils.getMessageOptions;

import com.google.protobuf.Descriptors;
import com.linkedin.common.Deprecation;
import com.linkedin.util.Pair;
import datahub.protobuf.visitors.ProtobufExtensionUtil;
import datahub.protobuf.visitors.ProtobufModelVisitor;
import datahub.protobuf.visitors.VisitContext;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeprecationVisitor implements ProtobufModelVisitor<Deprecation> {

  @Override
  public Stream<Deprecation> visitGraph(VisitContext context) {
    if (context.root().messageProto().getOptions().getDeprecated()) {
      List<Pair<Descriptors.FieldDescriptor, Object>> deprecationOptions =
          ProtobufExtensionUtil.filterByDataHubType(
              getMessageOptions(context.root().messageProto()),
              context.getGraph().getRegistry(),
              ProtobufExtensionUtil.DataHubMetadataType.DEPRECATION);

      String decommissionNote =
          deprecationOptions.stream()
              .filter(
                  opt -> opt.getKey().getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING)
              .flatMap(
                  opt -> {
                    if (opt.getKey().isRepeated()) {
                      return ((Collection<String>) opt.getValue()).stream();
                    } else {
                      return Stream.of(opt.getValue());
                    }
                  })
              .map(Object::toString)
              .collect(Collectors.joining("\n"));

      Optional<Long> decommissionTime =
          deprecationOptions.stream()
              .filter(
                  opt -> opt.getKey().getJavaType() == Descriptors.FieldDescriptor.JavaType.LONG)
              .map(opt -> (Long) opt.getValue())
              .findFirst();

      return Stream.of(
          new Deprecation()
              .setDeprecated(true)
              .setNote(decommissionNote)
              .setDecommissionTime(decommissionTime.orElse(0L))
              .setActor(context.getAuditStamp().getActor()));
    } else {
      return Stream.empty();
    }
  }
}
