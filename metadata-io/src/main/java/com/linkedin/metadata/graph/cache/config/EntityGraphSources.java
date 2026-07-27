package com.linkedin.metadata.graph.cache.config;

import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import java.util.List;
import javax.annotation.Nonnull;

public final class EntityGraphSources {

  private EntityGraphSources() {}

  @Nonnull
  public static List<GraphSnapshotSource> supportedBuildSources(
      @Nonnull EntityGraphDefinition definition) {
    return List.of(definition.getBuildSource());
  }

  public static boolean supports(
      @Nonnull EntityGraphDefinition definition, @Nonnull GraphSnapshotSource source) {
    return definition.getBuildSource() == source;
  }
}
