package com.linkedin.metadata.models;

import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class EntitySpecUtils {
  private EntitySpecUtils() {
  }

  public static List<String> getEntityTimeseriesAspectNames(@Nonnull EntityRegistry entityRegistry,
      @Nonnull String entityName) {
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    final List<String> timeseriesAspectNames = entitySpec.getAspectSpecs()
        .stream()
        .filter(x -> x.isTimeseries())
        .map(x -> x.getName())
        .collect(Collectors.toList());
    return timeseriesAspectNames;
  }
}
