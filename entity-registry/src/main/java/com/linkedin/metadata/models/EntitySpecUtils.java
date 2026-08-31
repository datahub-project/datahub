package com.linkedin.metadata.models;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class EntitySpecUtils {
  private EntitySpecUtils() {}

  public static List<String> getEntityTimeseriesAspectNames(
      @Nonnull EntityRegistry entityRegistry, @Nonnull String entityName) {
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    final List<String> timeseriesAspectNames =
        entitySpec.getAspectSpecs().stream()
            .filter(x -> x.isTimeseries())
            .map(x -> x.getName())
            .collect(Collectors.toList());
    return timeseriesAspectNames;
  }

  public static Map<String, List<PathSpec>> getSearchableFieldsToPathSpecs(
      @Nonnull EntityRegistry entityRegistry, @Nonnull List<String> entityNames) {
    List<EntitySpec> entitySpecs =
        entityNames.stream()
            .map(name -> entityRegistry.getEntitySpec(name))
            .collect(Collectors.toList());
    return entitySpecs.stream()
        .flatMap(entitySpec -> entitySpec.getSearchableFieldsToPathSpecsMap().entrySet().stream())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (s1, s2) ->
                    Stream.concat(s1.stream(), s2.stream())
                        .distinct()
                        .collect(Collectors.toList())));
  }
}
