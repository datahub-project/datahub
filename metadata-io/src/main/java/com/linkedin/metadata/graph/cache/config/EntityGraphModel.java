package com.linkedin.metadata.graph.cache.config;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.List;
import java.util.OptionalInt;
import lombok.Builder;
import lombok.Value;

public final class EntityGraphModel {

  private EntityGraphModel() {}

  @Value
  @Builder
  public static class GraphEdgeTriplet {
    String sourceEntityType;
    String destinationEntityType;
    String relationshipType;
  }

  @Value
  @Builder
  public static class ResolvedGraphEdge {
    GraphEdgeTriplet triplet;
    String aspectName;
    String searchField;
    boolean searchable;
    RelationshipDirection graphDirection;
  }

  @Value
  @Builder
  public static class GraphBounds {
    int maxVertices;
    OptionalInt maxEdges;
  }

  @Value
  @Builder
  public static class EntityGraphScope {
    ScopeMode mode;
    int maxDepth;
  }

  @Value
  @Builder
  public static class GraphBindings {
    List<String> filterFields;
    List<String> policyFieldTypes;
  }

  @Value
  @Builder
  public static class LocalEvictionLimits {
    boolean enabled;
    int maxViews;
    long maxEstimatedBytes;
  }

  @Value
  @Builder
  public static class EntityGraphDefinition {
    String graphId;
    List<GraphEdgeTriplet> edges;
    List<ResolvedGraphEdge> resolvedEdges;
    EntityGraphScope scope;
    GraphBindings bindings;
    GraphBounds bounds;
    PopulationStrategy populationStrategy;
    RebuildExecution rebuildExecution;
    int populationIntervalSeconds;
    int scrollBatchSize;
    LocalEvictionLimits localEviction;
    boolean enabled;
    GraphSnapshotSource buildSource;
  }
}
