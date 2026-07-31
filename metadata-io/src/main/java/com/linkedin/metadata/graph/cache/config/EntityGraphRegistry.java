package com.linkedin.metadata.graph.cache.config;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.GraphDefinition;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.GraphPopulation;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.LocalEviction;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBindings;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBounds;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphEdgeTriplet;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.LocalEvictionLimits;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.ResolvedGraphEdge;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry.EdgeInfo;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

@Slf4j
public class EntityGraphRegistry {

  private final Map<String, EntityGraphDefinition> graphsById;
  private final Map<String, EntityGraphBinding> bindingsByFilterField;
  private final Map<String, EntityGraphBinding> bindingsByPolicyField;
  private final Map<String, Set<String>> candidateGraphIdsByEntityAndAspect;
  private final Map<String, Set<String>> relationshipTypesByGraphAndAspect;

  private EntityGraphRegistry(
      Map<String, EntityGraphDefinition> graphsById,
      Map<String, EntityGraphBinding> bindingsByFilterField,
      Map<String, EntityGraphBinding> bindingsByPolicyField,
      Map<String, Set<String>> candidateGraphIdsByEntityAndAspect,
      Map<String, Set<String>> relationshipTypesByGraphAndAspect) {
    this.graphsById = graphsById;
    this.bindingsByFilterField = bindingsByFilterField;
    this.bindingsByPolicyField = bindingsByPolicyField;
    this.candidateGraphIdsByEntityAndAspect = candidateGraphIdsByEntityAndAspect;
    this.relationshipTypesByGraphAndAspect = relationshipTypesByGraphAndAspect;
  }

  @Nonnull
  public static EntityGraphRegistry build(
      @Nonnull EntityGraphCacheProperties properties,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull LineageRegistry lineageRegistry) {
    EntityGraphEdgeResolver resolver = new EntityGraphEdgeResolver(entityRegistry);
    Map<String, EntityGraphDefinition> byId = new HashMap<>();
    Map<String, EntityGraphBinding> byFilter = new HashMap<>();
    Map<String, EntityGraphBinding> byPolicy = new HashMap<>();
    Map<String, Set<String>> candidateGraphIdsByEntityAndAspect = new HashMap<>();
    Map<String, Set<String>> relationshipTypesByGraphAndAspect = new HashMap<>();
    List<String> errors = new ArrayList<>();

    for (Map.Entry<String, GraphDefinition> entry : properties.getGraphs().entrySet()) {
      String graphId = entry.getKey();
      GraphDefinition graphConfig = entry.getValue();
      if (graphConfig == null || !graphConfig.isEnabled()) {
        continue;
      }

      List<GraphEdgeTriplet> triplets;
      if (graphConfig.getLineage() != null) {
        triplets = expandLineageEdges(graphConfig, lineageRegistry);
      } else {
        triplets = expandTripletEdges(graphConfig);
      }

      List<ResolvedGraphEdge> resolved = resolver.resolveAll(triplets);
      if (resolved.isEmpty()) {
        if (graphConfig.isEnabled()) {
          errors.add(
              "graph "
                  + graphId
                  + ": resolved zero edges from entity registry (check entity types, "
                  + "relationshipType, and lineage filters)");
        }
        continue;
      }

      PopulationStrategy strategy = resolveStrategy(graphConfig, properties);
      GraphPopulation population =
          graphConfig.getPopulation() != null ? graphConfig.getPopulation() : new GraphPopulation();
      GraphSnapshotSource buildSource = resolveBuildSource(graphId, graphConfig, resolved, errors);
      if (buildSource == null) {
        continue;
      }

      int maxVertices =
          graphConfig.getBounds() != null ? graphConfig.getBounds().getMaxVertices() : 10000;
      int maxEdges =
          graphConfig.getBounds() != null && graphConfig.getBounds().getMaxEdges() != null
              ? graphConfig.getBounds().getMaxEdges()
              : (int) (maxVertices * 1.5);

      EntityGraphDefinition definition =
          EntityGraphDefinition.builder()
              .graphId(graphId)
              .edges(triplets)
              .resolvedEdges(resolved)
              .scope(
                  EntityGraphScope.builder()
                      .mode(
                          graphConfig.getScope() != null
                              ? graphConfig.getScope().getMode()
                              : ScopeMode.FULL)
                      .maxDepth(resolveMaxDepth(graphConfig))
                      .build())
              .bindings(
                  GraphBindings.builder()
                      .filterFields(
                          graphConfig.getBindings() != null
                              ? graphConfig.getBindings().getFilterFields()
                              : List.of())
                      .policyFieldTypes(
                          graphConfig.getBindings() != null
                              ? graphConfig.getBindings().getPolicyFieldTypes()
                              : List.of())
                      .build())
              .bounds(
                  GraphBounds.builder()
                      .maxVertices(maxVertices)
                      .maxEdges(OptionalInt.of(maxEdges))
                      .build())
              .populationStrategy(strategy)
              .rebuildExecution(resolveRebuildExecution(graphConfig))
              .populationIntervalSeconds(resolvePopulationIntervalSeconds(population))
              .scrollBatchSize(
                  graphConfig.getScroll() != null ? graphConfig.getScroll().getBatchSize() : 500)
              .localEviction(resolveLocalEviction(properties, graphConfig))
              .buildSource(buildSource)
              .enabled(true)
              .build();

      byId.put(graphId, definition);
      indexBindings(definition, byFilter, byPolicy, errors);
      indexInvalidationCandidates(
          graphId, resolved, candidateGraphIdsByEntityAndAspect, relationshipTypesByGraphAndAspect);
    }

    if (properties.isEnabled()) {
      if (byId.isEmpty()) {
        errors.add("entity graph cache has no enabled graphs with resolvable edges");
      }
      validateKnownGraphs(byId, errors);
      if (!errors.isEmpty()) {
        throw new IllegalStateException(
            "Invalid entity graph cache configuration: " + String.join("; ", errors));
      }
    }

    return new EntityGraphRegistry(
        byId,
        byFilter,
        byPolicy,
        candidateGraphIdsByEntityAndAspect,
        relationshipTypesByGraphAndAspect);
  }

  @Nullable
  public EntityGraphBinding resolveKnownGraph(@Nonnull KnownEntityGraph known) {
    EntityGraphDefinition definition = graphsById.get(known.getConfigKey());
    if (definition == null || !definition.isEnabled()) {
      return null;
    }
    return EntityGraphBinding.builder()
        .graphId(known.getConfigKey())
        .source(definition.getBuildSource())
        .build();
  }

  private static void validateKnownGraphs(
      @Nonnull Map<String, EntityGraphDefinition> byId, @Nonnull List<String> errors) {
    for (KnownEntityGraph known : KnownEntityGraph.values()) {
      EntityGraphDefinition definition = byId.get(known.getConfigKey());
      if (definition == null) {
        errors.add(
            "required graph "
                + known.getConfigKey()
                + " ("
                + known.name()
                + ") is missing or disabled");
        continue;
      }
      if (definition.getBuildSource() != known.getExpectedBuildSource()) {
        errors.add(
            "graph "
                + known.getConfigKey()
                + " ("
                + known.name()
                + ") requires buildSource "
                + known.getExpectedBuildSource().name().toLowerCase(Locale.ROOT)
                + " but was "
                + definition.getBuildSource().name().toLowerCase(Locale.ROOT));
      }
      ScopeMode actualScope = definition.getScope().getMode();
      if (!matchesScopeRequirement(actualScope, known.getExpectedScope())) {
        errors.add(
            "graph "
                + known.getConfigKey()
                + " ("
                + known.name()
                + ") requires scope.mode "
                + known.getExpectedScope().name()
                + " but was "
                + actualScope.name());
      }
    }
  }

  private static boolean matchesScopeRequirement(
      @Nonnull ScopeMode actual, @Nonnull KnownEntityGraph.ScopeRequirement expected) {
    return switch (expected) {
      case FULL -> actual == ScopeMode.FULL;
      case PARTIAL -> actual == ScopeMode.PARTIAL;
    };
  }

  private static void indexInvalidationCandidates(
      @Nonnull String graphId,
      @Nonnull List<ResolvedGraphEdge> resolvedEdges,
      @Nonnull Map<String, Set<String>> candidateGraphIdsByEntityAndAspect,
      @Nonnull Map<String, Set<String>> relationshipTypesByGraphAndAspect) {
    for (ResolvedGraphEdge edge : resolvedEdges) {
      String aspectName = edge.getAspectName();
      if (!StringUtils.hasText(aspectName)) {
        continue;
      }
      String sourceType = edge.getTriplet().getSourceEntityType().toLowerCase(Locale.ROOT);
      String destinationType =
          edge.getTriplet().getDestinationEntityType().toLowerCase(Locale.ROOT);
      String relationshipType = edge.getTriplet().getRelationshipType();

      candidateGraphIdsByEntityAndAspect
          .computeIfAbsent(candidateKey(sourceType, aspectName), k -> new HashSet<>())
          .add(graphId);
      candidateGraphIdsByEntityAndAspect
          .computeIfAbsent(candidateKey(destinationType, aspectName), k -> new HashSet<>())
          .add(graphId);
      relationshipTypesByGraphAndAspect
          .computeIfAbsent(graphAspectKey(graphId, aspectName), k -> new HashSet<>())
          .add(relationshipType);
    }
  }

  @Nonnull
  private static String candidateKey(@Nonnull String entityType, @Nonnull String aspectName) {
    return entityType.toLowerCase(Locale.ROOT) + "|" + aspectName;
  }

  @Nonnull
  private static String graphAspectKey(@Nonnull String graphId, @Nonnull String aspectName) {
    return graphId + "|" + aspectName;
  }

  @Nullable
  private static GraphSnapshotSource resolveBuildSource(
      @Nonnull String graphId,
      @Nonnull GraphDefinition graphConfig,
      @Nonnull List<ResolvedGraphEdge> resolvedEdges,
      @Nonnull List<String> errors) {
    if (!StringUtils.hasText(graphConfig.getBuildSource())) {
      errors.add("graph " + graphId + ": buildSource is required (primary, graph, or search)");
      return null;
    }
    GraphSnapshotSource source;
    try {
      source =
          GraphSnapshotSource.valueOf(graphConfig.getBuildSource().trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      errors.add("graph " + graphId + ": buildSource must be primary, graph, or search");
      return null;
    }

    ScopeMode scopeMode =
        graphConfig.getScope() != null ? graphConfig.getScope().getMode() : ScopeMode.FULL;
    if (scopeMode == ScopeMode.FULL && source == GraphSnapshotSource.PRIMARY) {
      errors.add("graph " + graphId + ": FULL scope requires buildSource graph or search");
      return null;
    }
    if (source == GraphSnapshotSource.SEARCH
        && resolvedEdges.stream().noneMatch(ResolvedGraphEdge::isSearchable)) {
      errors.add(
          "graph "
              + graphId
              + ": buildSource search requires at least one searchable edge from entity registry");
      return null;
    }
    return source;
  }

  private static void indexBindings(
      EntityGraphDefinition definition,
      Map<String, EntityGraphBinding> byFilter,
      Map<String, EntityGraphBinding> byPolicy,
      List<String> errors) {
    EntityGraphBinding binding =
        EntityGraphBinding.builder()
            .graphId(definition.getGraphId())
            .source(definition.getBuildSource())
            .build();
    if (definition.getBindings().getFilterFields() != null) {
      for (String field : definition.getBindings().getFilterFields()) {
        String key = field.toLowerCase(Locale.ROOT);
        EntityGraphBinding existing = byFilter.putIfAbsent(key, binding);
        if (existing != null && !existing.getGraphId().equals(definition.getGraphId())) {
          errors.add(
              "filter field '"
                  + field
                  + "' is bound to graphs "
                  + existing.getGraphId()
                  + " and "
                  + definition.getGraphId());
        }
      }
    }
    if (definition.getBindings().getPolicyFieldTypes() != null) {
      for (String field : definition.getBindings().getPolicyFieldTypes()) {
        String key = field.toUpperCase(Locale.ROOT);
        EntityGraphBinding existing = byPolicy.putIfAbsent(key, binding);
        if (existing != null && !existing.getGraphId().equals(definition.getGraphId())) {
          errors.add(
              "policy field type '"
                  + field
                  + "' is bound to graphs "
                  + existing.getGraphId()
                  + " and "
                  + definition.getGraphId());
        }
      }
    }
  }

  private static PopulationStrategy resolveStrategy(
      GraphDefinition graph, EntityGraphCacheProperties properties) {
    if (graph.getPopulation() == null || graph.getPopulation().getStrategy() == null) {
      throw new IllegalStateException(
          "Graph population.strategy is required — no global default is supported");
    }
    return graph.getPopulation().getStrategy();
  }

  @Nonnull
  static List<GraphEdgeTriplet> expandTripletEdges(@Nonnull GraphDefinition graph) {
    if (!CollectionUtils.isEmpty(graph.getEdges())) {
      return graph.getEdges().stream()
          .map(
              e ->
                  GraphEdgeTriplet.builder()
                      .sourceEntityType(e.getSourceEntityType())
                      .destinationEntityType(e.getDestinationEntityType())
                      .relationshipType(e.getRelationshipType())
                      .build())
          .collect(Collectors.toList());
    }
    if (!CollectionUtils.isEmpty(graph.getEntityTypes())
        && StringUtils.hasText(graph.getRelationshipType())) {
      List<GraphEdgeTriplet> triplets = new ArrayList<>();
      for (String entityType : graph.getEntityTypes()) {
        triplets.add(
            GraphEdgeTriplet.builder()
                .sourceEntityType(entityType)
                .destinationEntityType(entityType)
                .relationshipType(graph.getRelationshipType())
                .build());
      }
      return triplets;
    }
    return List.of();
  }

  @Nonnull
  static List<GraphEdgeTriplet> expandLineageEdges(
      @Nonnull GraphDefinition graph, @Nonnull LineageRegistry lineageRegistry) {
    Set<Triple<String, String, String>> lineageEdges = new HashSet<>();
    lineageRegistry
        .getLineageSpecs()
        .forEach(
            (entityName, spec) -> {
              for (EdgeInfo edge : spec.getUpstreamEdges()) {
                addLineageTriplet(lineageEdges, entityName, edge);
              }
              for (EdgeInfo edge : spec.getDownstreamEdges()) {
                addLineageTriplet(lineageEdges, entityName, edge);
              }
            });

    Set<String> filterTypes = null;
    if (graph.getLineage() != null
        && graph.getLineage().getEntityTypes() != null
        && !graph.getLineage().getEntityTypes().isEmpty()) {
      filterTypes =
          graph.getLineage().getEntityTypes().stream()
              .map(t -> t.toLowerCase(Locale.ROOT))
              .collect(Collectors.toSet());
    }

    List<GraphEdgeTriplet> result = new ArrayList<>();
    for (Triple<String, String, String> edge : lineageEdges) {
      if (filterTypes != null
          && !filterTypes.contains(edge.getLeft().toLowerCase(Locale.ROOT))
          && !filterTypes.contains(edge.getMiddle().toLowerCase(Locale.ROOT))) {
        continue;
      }
      result.add(
          GraphEdgeTriplet.builder()
              .sourceEntityType(edge.getLeft())
              .destinationEntityType(edge.getMiddle())
              .relationshipType(edge.getRight())
              .build());
    }
    return result;
  }

  private static void addLineageTriplet(
      @Nonnull Set<Triple<String, String, String>> lineageEdges,
      @Nonnull String entityName,
      @Nonnull EdgeInfo edge) {
    if (edge.getDirection() == RelationshipDirection.OUTGOING) {
      lineageEdges.add(Triple.of(entityName, edge.getOpposingEntityType(), edge.getType()));
    } else {
      lineageEdges.add(Triple.of(edge.getOpposingEntityType(), entityName, edge.getType()));
    }
  }

  @Nonnull
  private static LocalEvictionLimits resolveLocalEviction(
      @Nonnull EntityGraphCacheProperties properties, @Nonnull GraphDefinition graphConfig) {
    LocalEviction global = properties.getEviction().getLocal();
    if (graphConfig.getEviction() == null || graphConfig.getEviction().getLocal() == null) {
      return toLocalEvictionLimits(global);
    }
    LocalEviction graphLocal = graphConfig.getEviction().getLocal();
    LocalEviction defaults = new LocalEviction();
    return LocalEvictionLimits.builder()
        .enabled(graphLocal.isEnabled())
        .maxViews(
            graphLocal.getMaxViews() != defaults.getMaxViews()
                ? graphLocal.getMaxViews()
                : global.getMaxViews())
        .maxEstimatedBytes(
            graphLocal.getMaxEstimatedBytes() != defaults.getMaxEstimatedBytes()
                ? graphLocal.getMaxEstimatedBytes()
                : global.getMaxEstimatedBytes())
        .build();
  }

  @Nonnull
  private static RebuildExecution resolveRebuildExecution(@Nonnull GraphDefinition graphConfig) {
    GraphPopulation population = graphConfig.getPopulation();
    if (population == null || population.getRebuildExecution() == null) {
      return RebuildExecution.SYNC;
    }
    return population.getRebuildExecution();
  }

  private static int resolveMaxDepth(@Nonnull GraphDefinition graphConfig) {
    if (graphConfig.getScope() == null) {
      return 0;
    }
    return Math.max(0, graphConfig.getScope().getMaxDepth());
  }

  private static int resolvePopulationIntervalSeconds(@Nonnull GraphPopulation population) {
    if (population.getIntervalSeconds() > 0) {
      return population.getIntervalSeconds();
    }
    return 300;
  }

  @Nonnull
  private static LocalEvictionLimits toLocalEvictionLimits(@Nonnull LocalEviction local) {
    return LocalEvictionLimits.builder()
        .enabled(local.isEnabled())
        .maxViews(local.getMaxViews())
        .maxEstimatedBytes(local.getMaxEstimatedBytes())
        .build();
  }

  @Nonnull
  public Map<String, EntityGraphDefinition> getGraphsById() {
    return graphsById;
  }

  public EntityGraphDefinition getDefinition(@Nonnull String graphId) {
    return graphsById.get(graphId);
  }

  public boolean hasFullScopeGraphs() {
    return graphsById.values().stream()
        .anyMatch(definition -> definition.getScope().getMode() == ScopeMode.FULL);
  }

  @Nullable
  public EntityGraphBinding getBindingForFilterField(@Nonnull String searchField) {
    return bindingsByFilterField.get(searchField.toLowerCase(Locale.ROOT));
  }

  @Nullable
  public EntityGraphBinding getBindingForPolicyField(@Nonnull String fieldType) {
    return bindingsByPolicyField.get(fieldType.toUpperCase(Locale.ROOT));
  }

  @Nonnull
  public List<EntityGraphDefinition> getScheduledFullGraphs() {
    return graphsById.values().stream()
        .filter(
            g ->
                g.getScope().getMode() == ScopeMode.FULL
                    && g.getPopulationStrategy() == PopulationStrategy.SCHEDULED)
        .collect(Collectors.toList());
  }

  /** Graphs where entityType matches source or destination on a resolved edge with aspectName. */
  @Nonnull
  public Set<String> getCandidateGraphIds(@Nonnull String entityType, @Nonnull String aspectName) {
    Set<String> candidates =
        candidateGraphIdsByEntityAndAspect.get(candidateKey(entityType, aspectName));
    return candidates == null ? Collections.emptySet() : Collections.unmodifiableSet(candidates);
  }

  /**
   * Graphs whose resolved edges include {@code entityType} as source or destination.
   *
   * <p>Canonical lookup for entity-wide (key-aspect) sync delete gating and for {@link
   * com.linkedin.metadata.graph.cache.service.invalidation.GraphCacheInvalidator} when applying
   * batch entries with {@code aspectName=null}. Includes edges even when the relationship aspect is
   * not indexed in {@link #getCandidateGraphIds}; per-aspect deletes still require that index.
   */
  @Nonnull
  public Set<String> getGraphIdsForEntityType(@Nonnull String entityType) {
    Set<String> graphIds = new LinkedHashSet<>();
    for (EntityGraphDefinition definition : graphsById.values()) {
      if (definition.getResolvedEdges().stream()
          .anyMatch(
              edge ->
                  entityType.equals(edge.getTriplet().getSourceEntityType())
                      || entityType.equals(edge.getTriplet().getDestinationEntityType()))) {
        graphIds.add(definition.getGraphId());
      }
    }
    return Collections.unmodifiableSet(graphIds);
  }

  /**
   * Relationship types on graph edges that use {@code aspectName}. Used to gate sync invalidation
   * (empty set → graph skipped); URN membership is checked separately via vertex lookup.
   */
  @Nonnull
  public Set<String> getRelationshipTypesForAspect(
      @Nonnull String graphId, @Nonnull String aspectName) {
    Set<String> relationshipTypes =
        relationshipTypesByGraphAndAspect.get(graphAspectKey(graphId, aspectName));
    return relationshipTypes == null
        ? Collections.emptySet()
        : Collections.unmodifiableSet(relationshipTypes);
  }
}
