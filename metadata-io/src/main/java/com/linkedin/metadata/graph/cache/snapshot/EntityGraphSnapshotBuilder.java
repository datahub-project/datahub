package com.linkedin.metadata.graph.cache.snapshot;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphBuildFailure;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.ResolvedGraphEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityGraphSnapshotBuilder {

  private static final HexFormat HEX = HexFormat.of();
  private static final SortCriterion URN_SORT =
      new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING);

  @Value
  @Builder
  public static class BuildResult {
    EntityGraphSnapshot snapshot;
    CacheStatus status;
    String failureReason;
  }

  @Nonnull
  public BuildResult build(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nullable Collection<String> seeds) {
    // seeds is unused for FULL builds; seed-scoped builds use buildPartial().

    if (definition.getScope().getMode() == ScopeMode.PARTIAL) {
      throw new IllegalStateException(
          "PARTIAL graphs must be built via buildPartial(); graphId=" + definition.getGraphId());
    }

    if (source == GraphSnapshotSource.SEARCH) {
      return buildFullFromSearch(opContext, definition);
    }
    if (source == GraphSnapshotSource.GRAPH) {
      return buildFullFromGraph(opContext, definition);
    }
    return buildFailure(opContext, definition, "primary_full_unsupported");
  }

  @Nonnull
  private BuildResult buildFullFromSearch(
      @Nonnull OperationContext opContext, @Nonnull EntityGraphDefinition definition) {
    EdgeAccumulator accumulator = new EdgeAccumulator(definition);

    boolean anySearchable =
        definition.getResolvedEdges().stream().anyMatch(ResolvedGraphEdge::isSearchable);
    if (!anySearchable) {
      return buildFailure(opContext, definition, "search_not_configured");
    }

    boolean searchOk = buildFromSearch(opContext, definition, null, accumulator);
    if (accumulator.isScrollIncomplete()) {
      return buildFailure(opContext, definition, "scroll_incomplete");
    }
    if (!searchOk || accumulator.getEdges().isEmpty()) {
      return buildFailure(opContext, definition, searchOk ? "empty_snapshot" : "search_failed");
    }

    if (accumulator.isBypassed()) {
      return buildFailure(opContext, definition, accumulator.getBypassReason());
    }

    return snapshotFromEdges(
        definition,
        accumulator.getEdges(),
        GraphSnapshotSource.SEARCH.name(),
        EntityGraphCacheKeys.fullCacheKey(definition.getGraphId(), GraphSnapshotSource.SEARCH),
        TraversalCoverage.fullComplete());
  }

  @Nonnull
  private BuildResult buildFullFromGraph(
      @Nonnull OperationContext opContext, @Nonnull EntityGraphDefinition definition) {
    EdgeAccumulator accumulator = new EdgeAccumulator(definition);
    boolean graphOk = buildFromGraph(opContext, definition, null, accumulator);
    if (accumulator.isScrollIncomplete()) {
      return buildFailure(opContext, definition, "scroll_incomplete");
    }
    if (!graphOk || accumulator.getEdges().isEmpty()) {
      return buildFailure(opContext, definition, graphOk ? "empty_snapshot" : "graph_failed");
    }

    if (accumulator.isBypassed()) {
      return buildFailure(opContext, definition, accumulator.getBypassReason());
    }

    return snapshotFromEdges(
        definition,
        accumulator.getEdges(),
        GraphSnapshotSource.GRAPH.name(),
        EntityGraphCacheKeys.fullCacheKey(definition.getGraphId(), GraphSnapshotSource.GRAPH),
        TraversalCoverage.fullComplete());
  }

  @Nonnull
  public BuildResult buildPartial(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull Collection<String> seeds,
      @Nonnull TraversalDirection direction,
      @Nullable List<DirectedEdge> existingEdges,
      @Nullable TraversalCoverage existingCoverage,
      @Nullable String existingCacheKey) {

    if (source == GraphSnapshotSource.SEARCH) {
      boolean anySearchable =
          definition.getResolvedEdges().stream().anyMatch(ResolvedGraphEdge::isSearchable);
      if (!anySearchable) {
        return buildFailure(opContext, definition, "search_not_configured");
      }
      if (direction != TraversalDirection.FORWARD) {
        return buildFailure(opContext, definition, "search_reverse_unsupported");
      }
    }

    if (source == GraphSnapshotSource.PRIMARY) {
      if (direction != TraversalDirection.FORWARD) {
        return buildFailure(opContext, definition, "primary_reverse_unsupported");
      }
    }

    Set<String> seedSet = seeds.stream().collect(Collectors.toCollection(LinkedHashSet::new));
    EdgeAccumulator accumulator = new EdgeAccumulator(definition);
    if (existingEdges != null) {
      for (DirectedEdge edge : existingEdges) {
        accumulator.addEdge(
            edge.getSourceUrn(),
            edge.getDestinationUrn(),
            edge.getRelationshipType(),
            definition.getBounds().getMaxVertices(),
            definition.getBounds().getMaxEdges().orElse(Integer.MAX_VALUE));
        if (accumulator.isBypassed()) {
          break;
        }
      }
    }

    int maxDepth = definition.getScope().getMaxDepth();
    RelationshipDirection scrollDirection =
        direction == TraversalDirection.FORWARD
            ? RelationshipDirection.OUTGOING
            : RelationshipDirection.INCOMING;

    Set<String> frontier = new LinkedHashSet<>(seedSet);
    Set<String> visited = new HashSet<>(seedSet);
    int exploredDepth = 0;
    String truncationReason = null;
    boolean scrollIncomplete = false;

    for (int depth = 0;
        depth < maxDepth && !frontier.isEmpty() && truncationReason == null;
        depth++) {
      Set<String> nextFrontier = new LinkedHashSet<>();

      for (String frontierUrn : frontier) {
        if (truncationReason != null) {
          break;
        }
        PartialScrollResult scrollResult =
            scrollPartialLayer(
                opContext, definition, source, Set.of(frontierUrn), scrollDirection, accumulator);
        if (scrollResult.isScrollIncomplete()) {
          scrollIncomplete = true;
        }
        if (accumulator.isBypassed()) {
          truncationReason = accumulator.getBypassReason();
          break;
        }

        for (String neighbor : scrollResult.getNeighbors()) {
          if (visited.add(neighbor)) {
            nextFrontier.add(neighbor);
          }
        }
      }

      exploredDepth = depth + 1;
      frontier = nextFrontier;
    }

    if (truncationReason == null && scrollIncomplete) {
      truncationReason = "scroll_incomplete";
    }
    if (truncationReason == null && accumulator.isBypassed()) {
      truncationReason = accumulator.getBypassReason();
    }

    boolean complete = truncationReason == null;
    DirectionCoverage directionCoverage =
        DirectionCoverage.builder()
            .direction(direction)
            .explored(true)
            .exploredDepth(exploredDepth)
            .configuredMaxDepth(maxDepth)
            .complete(complete)
            .truncationReason(truncationReason)
            .build();

    TraversalCoverage coverage =
        existingCoverage == null
            ? TraversalCoverage.builder().direction(directionCoverage).build()
            : existingCoverage.withDirection(directionCoverage);

    if (accumulator.isBypassed() && accumulator.getEdges().isEmpty()) {
      return buildFailure(opContext, definition, accumulator.getBypassReason());
    }

    List<DirectedEdge> edges = accumulator.getEdges();
    EntityGraphView buildView = new EntityGraphView(edges);
    edges = buildView.inducedComponentEdges(seedSet);
    if (edges.isEmpty()) {
      return buildFailure(opContext, definition, "empty_component");
    }

    String fingerprint = topologyFingerprint(edges);
    String cacheKey =
        existingCacheKey != null
            ? existingCacheKey
            : EntityGraphCacheKeys.componentCacheKey(definition.getGraphId(), source, fingerprint);

    return snapshotFromEdges(definition, edges, source.name(), cacheKey, coverage);
  }

  @Nonnull
  private PartialScrollResult scrollPartialLayer(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull Set<String> frontierUrns,
      @Nonnull RelationshipDirection scrollDirection,
      @Nonnull EdgeAccumulator accumulator) {

    if (source == GraphSnapshotSource.PRIMARY) {
      return scrollPartialLayerFromAspects(
          opContext, definition, frontierUrns, scrollDirection, accumulator);
    }
    if (source == GraphSnapshotSource.SEARCH) {
      return scrollPartialLayerFromSearch(
          opContext, definition, frontierUrns, scrollDirection, accumulator);
    }
    return scrollPartialLayerFromGraph(
        opContext, definition, frontierUrns, scrollDirection, accumulator);
  }

  @Nonnull
  private PartialScrollResult scrollPartialLayerFromAspects(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nonnull Set<String> frontierUrns,
      @Nonnull RelationshipDirection scrollDirection,
      @Nonnull EdgeAccumulator accumulator) {

    if (scrollDirection != RelationshipDirection.OUTGOING) {
      return new PartialScrollResult(Set.of(), false);
    }

    AspectRetriever aspectRetriever = opContext.getRetrieverContext().getAspectRetriever();
    EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();
    opContext
        .getMetricUtils()
        .ifPresent(m -> m.incrementMicrometer("entity.graph.build.primary_aspect", 1));

    Set<Urn> urns =
        frontierUrns.stream()
            .map(UrnUtils::getUrn)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    Set<String> aspectNames =
        definition.getResolvedEdges().stream()
            .map(ResolvedGraphEdge::getAspectName)
            .filter(name -> name != null && !name.isBlank())
            .collect(Collectors.toCollection(LinkedHashSet::new));

    Map<Urn, Map<String, Aspect>> aspectBatch;
    try {
      aspectBatch = aspectRetriever.getLatestAspectObjects(opContext, urns, aspectNames);
    } catch (Exception e) {
      log.warn("Aspect batch read failed for graph {}", definition.getGraphId(), e);
      return new PartialScrollResult(Set.of(), true);
    }

    Set<String> neighbors = new LinkedHashSet<>();
    for (String frontierUrn : frontierUrns) {
      if (accumulator.isBypassed()) {
        break;
      }
      Urn urn = UrnUtils.getUrn(frontierUrn);
      Map<String, Aspect> aspectsForUrn = aspectBatch.getOrDefault(urn, Map.of());
      for (ResolvedGraphEdge resolved : definition.getResolvedEdges()) {
        if (accumulator.isBypassed()) {
          break;
        }
        Aspect aspect = aspectsForUrn.get(resolved.getAspectName());
        if (aspect == null) {
          continue;
        }
        AspectSpec aspectSpec =
            entityRegistry
                .getEntitySpec(urn.getEntityType())
                .getAspectSpec(resolved.getAspectName());
        if (aspectSpec == null) {
          continue;
        }
        RecordTemplate record =
            RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(), aspect.data());
        for (RelationshipFieldSpec relationshipField :
            matchingRelationshipFields(resolved, aspectSpec)) {
          Map<RelationshipFieldSpec, List<Object>> extracted =
              FieldExtractor.extractFields(record, List.of(relationshipField));
          for (Object value : extracted.getOrDefault(relationshipField, List.of())) {
            String destUrn = relationshipUrnToString(value);
            if (destUrn == null || destUrn.isBlank()) {
              continue;
            }
            accumulator.addEdge(
                frontierUrn,
                destUrn,
                resolved.getTriplet().getRelationshipType(),
                definition.getBounds().getMaxVertices(),
                definition.getBounds().getMaxEdges().orElse(Integer.MAX_VALUE));
            neighbors.add(destUrn);
          }
        }
      }
    }

    return new PartialScrollResult(neighbors, false);
  }

  @Nonnull
  private PartialScrollResult scrollPartialLayerFromGraph(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nonnull Set<String> frontierUrns,
      @Nonnull RelationshipDirection scrollDirection,
      @Nonnull EdgeAccumulator accumulator) {

    GraphRetriever graphRetriever = opContext.getRetrieverContext().getGraphRetriever();
    opContext
        .getMetricUtils()
        .ifPresent(m -> m.incrementMicrometer("entity.graph.build.graph_scroll", 1));

    Set<String> neighbors = new LinkedHashSet<>();
    boolean scrollIncomplete = false;

    Filter urnFilter =
        QueryUtils.newDisjunctiveFilter(
            CriterionUtils.buildCriterion(
                "urn",
                com.linkedin.metadata.query.filter.Condition.EQUAL,
                new ArrayList<>(frontierUrns)));

    for (ResolvedGraphEdge resolved : definition.getResolvedEdges()) {
      if (accumulator.isBypassed()) {
        break;
      }

      String sourceType = resolved.getTriplet().getSourceEntityType();
      String destType = resolved.getTriplet().getDestinationEntityType();
      String relType = resolved.getTriplet().getRelationshipType();

      RelatedEntitiesScrollResult result = null;
      do {
        result =
            graphRetriever.scrollRelatedEntities(
                Set.of(sourceType),
                urnFilter,
                Set.of(destType),
                QueryUtils.EMPTY_FILTER,
                Set.of(relType),
                new RelationshipFilter().setDirection(scrollDirection),
                Edge.EDGE_SORT_CRITERION,
                result == null ? null : result.getScrollId(),
                GraphRetriever.DEFAULT_EDGE_FETCH_LIMIT,
                null,
                null);

        if (result == null || result.getEntities() == null) {
          break;
        }

        for (RelatedEntities related : result.getEntities()) {
          if (accumulator.isBypassed()) {
            break;
          }
          accumulator.addEdge(
              related.getSourceUrn(),
              related.getDestinationUrn(),
              relType,
              definition.getBounds().getMaxVertices(),
              definition.getBounds().getMaxEdges().orElse(Integer.MAX_VALUE));
          neighbors.add(related.asRelatedEntity().getUrn());
        }

        if (result.getScrollId() != null && accumulator.isBypassed()) {
          break;
        }
      } while (result.getScrollId() != null && !accumulator.isBypassed());

      if (result != null && result.getScrollId() != null) {
        scrollIncomplete = true;
      }
    }

    return new PartialScrollResult(neighbors, scrollIncomplete);
  }

  @Nonnull
  private PartialScrollResult scrollPartialLayerFromSearch(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nonnull Set<String> frontierUrns,
      @Nonnull RelationshipDirection scrollDirection,
      @Nonnull EdgeAccumulator accumulator) {

    if (scrollDirection != RelationshipDirection.OUTGOING) {
      return new PartialScrollResult(Set.of(), false);
    }

    opContext
        .getMetricUtils()
        .ifPresent(m -> m.incrementMicrometer("entity.graph.build.search_scroll", 1));

    Map<String, List<ResolvedGraphEdge>> edgesBySourceType = new HashMap<>();
    for (ResolvedGraphEdge edge : definition.getResolvedEdges()) {
      if (!edge.isSearchable()) {
        continue;
      }
      edgesBySourceType
          .computeIfAbsent(
              edge.getTriplet().getSourceEntityType().toLowerCase(Locale.ROOT),
              k -> new ArrayList<>())
          .add(edge);
    }

    if (edgesBySourceType.isEmpty()) {
      return new PartialScrollResult(Set.of(), true);
    }

    Set<String> sourceTypes =
        definition.getResolvedEdges().stream()
            .map(e -> e.getTriplet().getSourceEntityType().toLowerCase(Locale.ROOT))
            .collect(Collectors.toCollection(LinkedHashSet::new));

    Filter urnFilter =
        QueryUtils.newDisjunctiveFilter(
            CriterionUtils.buildCriterion(
                "urn",
                com.linkedin.metadata.query.filter.Condition.EQUAL,
                new ArrayList<>(frontierUrns)));

    SearchFlags flags =
        new SearchFlags()
            .setFulltext(false)
            .setSkipCache(true)
            .setSkipAggregates(true)
            .setSkipHighlighting(true)
            .setIncludeSoftDeleted(false)
            .setIncludeRestricted(true);

    SearchRetriever searchRetriever = opContext.getRetrieverContext().getSearchRetriever();
    Set<String> neighbors = new LinkedHashSet<>();
    boolean scrollIncomplete = false;

    try {
      String scrollId = null;
      do {
        ScrollResult scrollResult =
            searchRetriever.scroll(
                new ArrayList<>(sourceTypes),
                urnFilter,
                scrollId,
                definition.getScrollBatchSize(),
                List.of(URN_SORT),
                flags);

        if (scrollResult.getEntities() == null || scrollResult.getEntities().isEmpty()) {
          break;
        }

        for (SearchEntity entity : scrollResult.getEntities()) {
          if (accumulator.isBypassed()) {
            break;
          }
          String sourceUrn = entity.getEntity().toString();
          if (!frontierUrns.contains(sourceUrn)) {
            continue;
          }
          List<ResolvedGraphEdge> applicable =
              edgesBySourceType.getOrDefault(
                  UrnUtils.getUrn(sourceUrn).getEntityType().toLowerCase(Locale.ROOT), List.of());
          for (ResolvedGraphEdge resolved : applicable) {
            String destUrn = extractRelatedUrn(entity, resolved.getSearchField());
            if (destUrn != null && !destUrn.isBlank()) {
              accumulator.addEdge(
                  sourceUrn,
                  destUrn,
                  resolved.getTriplet().getRelationshipType(),
                  definition.getBounds().getMaxVertices(),
                  definition.getBounds().getMaxEdges().orElse(Integer.MAX_VALUE));
              neighbors.add(destUrn);
            }
          }
        }

        scrollId = scrollResult.getScrollId();
        if (scrollId != null && accumulator.isBypassed()) {
          break;
        }
      } while (scrollId != null && !accumulator.isBypassed());

      if (scrollId != null) {
        scrollIncomplete = true;
      }
    } catch (Exception e) {
      log.warn("Search partial scroll failed for graph {}", definition.getGraphId(), e);
      return new PartialScrollResult(Set.of(), true);
    }

    return new PartialScrollResult(neighbors, scrollIncomplete);
  }

  @Nonnull
  private BuildResult snapshotFromEdges(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull List<DirectedEdge> edges,
      @Nonnull String buildSource,
      @Nonnull String cacheKey,
      @Nullable TraversalCoverage coverage) {

    Set<String> vertices = new LinkedHashSet<>();
    for (DirectedEdge edge : edges) {
      vertices.add(edge.getSourceUrn());
      vertices.add(edge.getDestinationUrn());
    }

    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(definition.getGraphId())
            .cacheKey(cacheKey)
            .generation(0)
            .buildSource(buildSource)
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(vertices.size())
            .edgeCount(edges.size())
            .topologyFingerprint(topologyFingerprint(edges))
            .traversalCoverage(coverage)
            .build();

    return BuildResult.builder().snapshot(snapshot).status(CacheStatus.ACTIVE).build();
  }

  private boolean buildFromSearch(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nullable Collection<String> seeds,
      @Nonnull EdgeAccumulator accumulator) {

    Set<String> sourceTypes =
        definition.getResolvedEdges().stream()
            .map(e -> e.getTriplet().getSourceEntityType().toLowerCase(Locale.ROOT))
            .collect(Collectors.toCollection(LinkedHashSet::new));

    Map<String, List<ResolvedGraphEdge>> edgesBySourceType = new HashMap<>();
    for (ResolvedGraphEdge edge : definition.getResolvedEdges()) {
      if (!edge.isSearchable()) {
        continue;
      }
      edgesBySourceType
          .computeIfAbsent(
              edge.getTriplet().getSourceEntityType().toLowerCase(Locale.ROOT),
              k -> new ArrayList<>())
          .add(edge);
    }

    SearchFlags flags =
        new SearchFlags()
            .setFulltext(false)
            .setSkipCache(true)
            .setSkipAggregates(true)
            .setSkipHighlighting(true)
            .setIncludeSoftDeleted(false)
            .setIncludeRestricted(true);

    SearchRetriever searchRetriever = opContext.getRetrieverContext().getSearchRetriever();
    String scrollId = null;
    int batchSize = definition.getScrollBatchSize();

    try {
      do {
        ScrollResult scrollResult =
            searchRetriever.scroll(
                new ArrayList<>(sourceTypes), null, scrollId, batchSize, List.of(URN_SORT), flags);

        if (scrollResult.getEntities() == null || scrollResult.getEntities().isEmpty()) {
          break;
        }

        for (SearchEntity entity : scrollResult.getEntities()) {
          if (accumulator.isBypassed()) {
            return false;
          }
          String sourceUrn = entity.getEntity().toString();
          List<ResolvedGraphEdge> applicable =
              edgesBySourceType.getOrDefault(
                  UrnUtils.getUrn(sourceUrn).getEntityType().toLowerCase(Locale.ROOT), List.of());
          for (ResolvedGraphEdge resolved : applicable) {
            String destUrn = extractRelatedUrn(entity, resolved.getSearchField());
            if (destUrn != null && !destUrn.isBlank()) {
              accumulator.addEdge(
                  sourceUrn,
                  destUrn,
                  resolved.getTriplet().getRelationshipType(),
                  definition.getBounds().getMaxVertices(),
                  definition.getBounds().getMaxEdges().orElse(Integer.MAX_VALUE));
            }
          }
        }

        scrollId = scrollResult.getScrollId();
      } while (scrollId != null && !accumulator.isBypassed());

      if (scrollId != null) {
        accumulator.markScrollIncomplete();
      }

      return !accumulator.getEdges().isEmpty();
    } catch (Exception e) {
      log.warn("Search scroll build failed for graph {}", definition.getGraphId(), e);
      return false;
    }
  }

  private boolean buildFromGraph(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nullable Collection<String> seeds,
      @Nonnull EdgeAccumulator accumulator) {

    GraphRetriever graphRetriever = opContext.getRetrieverContext().getGraphRetriever();
    opContext
        .getMetricUtils()
        .ifPresent(m -> m.incrementMicrometer("entity.graph.build.graph_scroll", 1));

    for (ResolvedGraphEdge resolved : definition.getResolvedEdges()) {
      if (accumulator.isBypassed()) {
        return false;
      }

      String sourceType = resolved.getTriplet().getSourceEntityType();
      String destType = resolved.getTriplet().getDestinationEntityType();
      String relType = resolved.getTriplet().getRelationshipType();

      RelatedEntitiesScrollResult result = null;
      do {
        result =
            graphRetriever.scrollRelatedEntities(
                Set.of(sourceType),
                QueryUtils.EMPTY_FILTER,
                Set.of(destType),
                QueryUtils.EMPTY_FILTER,
                Set.of(relType),
                new RelationshipFilter().setDirection(resolved.getGraphDirection()),
                Edge.EDGE_SORT_CRITERION,
                result == null ? null : result.getScrollId(),
                GraphRetriever.DEFAULT_EDGE_FETCH_LIMIT,
                null,
                null);

        if (result == null || result.getEntities() == null) {
          if (result == null) {
            accumulator.markScrollIncomplete();
          }
          break;
        }

        result
            .getEntities()
            .forEach(
                related -> {
                  if (accumulator.isBypassed()) {
                    return;
                  }
                  accumulator.addEdge(
                      related.getSourceUrn(),
                      related.getDestinationUrn(),
                      relType,
                      definition.getBounds().getMaxVertices(),
                      definition.getBounds().getMaxEdges().orElse(Integer.MAX_VALUE));
                });
      } while (result != null && result.getScrollId() != null && !accumulator.isBypassed());

      if (result != null && result.getScrollId() != null) {
        accumulator.markScrollIncomplete();
      }
    }

    return !accumulator.getEdges().isEmpty();
  }

  @Nonnull
  private BuildResult buildFailure(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nullable String reason) {
    recordBuildFailure(opContext, definition, reason);
    return BuildResult.builder()
        .status(GraphBuildFailure.statusForFailedBuild(reason))
        .failureReason(reason)
        .build();
  }

  private static void recordBuildFailure(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphDefinition definition,
      @Nullable String reason) {
    String metric = GraphBuildFailure.failureMetric(reason);
    opContext
        .getMetricUtils()
        .ifPresent(
            m ->
                m.incrementMicrometer(
                    metric, 1, "graphId", definition.getGraphId(), "reason", reason));
  }

  @Nullable
  static String extractRelatedUrn(@Nonnull SearchEntity entity, @Nullable String searchField) {
    if (searchField == null || entity.getExtraFields() == null) {
      return null;
    }
    StringMap extra = entity.getExtraFields();
    String direct = extra.get(searchField);
    if (direct != null && !direct.isBlank()) {
      return stripQuotes(direct);
    }
    String leaf = leafFieldName(searchField);
    if (leaf != null) {
      String leafVal = extra.get(leaf);
      if (leafVal != null && !leafVal.isBlank()) {
        return stripQuotes(leafVal);
      }
      String keyword = extra.get(leaf + ".keyword");
      if (keyword != null && !keyword.isBlank()) {
        return stripQuotes(keyword);
      }
    }
    return null;
  }

  private static String leafFieldName(@Nonnull String searchField) {
    int idx = searchField.lastIndexOf('.');
    return idx >= 0 ? searchField.substring(idx + 1) : searchField;
  }

  private static String stripQuotes(@Nonnull String value) {
    String trimmed = value.trim();
    if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
      return trimmed.substring(1, trimmed.length() - 1);
    }
    return trimmed;
  }

  @Nonnull
  private static List<RelationshipFieldSpec> matchingRelationshipFields(
      @Nonnull ResolvedGraphEdge resolved, @Nonnull AspectSpec aspectSpec) {
    String relationshipType = resolved.getTriplet().getRelationshipType();
    String destinationType = resolved.getTriplet().getDestinationEntityType();
    return aspectSpec.getRelationshipFieldSpecs().stream()
        .filter(field -> field.getRelationshipName().equalsIgnoreCase(relationshipType))
        .filter(
            field ->
                field.getValidDestinationTypes().stream()
                    .anyMatch(dest -> dest.equalsIgnoreCase(destinationType)))
        .collect(Collectors.toList());
  }

  @Nullable
  private static String relationshipUrnToString(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Urn urn) {
      return urn.toString();
    }
    return value.toString();
  }

  @Nonnull
  static String topologyFingerprint(@Nonnull List<DirectedEdge> edges) {
    List<String> canonical =
        edges.stream()
            .map(DirectedEdge::canonicalLine)
            .sorted(String.CASE_INSENSITIVE_ORDER)
            .collect(Collectors.toList());
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (String line : canonical) {
        digest.update(line.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) '\n');
      }
      return HEX.formatHex(digest.digest(), 0, 16);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final class PartialScrollResult {
    private final Set<String> neighbors;
    private final boolean scrollIncomplete;

    PartialScrollResult(Set<String> neighbors, boolean scrollIncomplete) {
      this.neighbors = neighbors;
      this.scrollIncomplete = scrollIncomplete;
    }

    Set<String> getNeighbors() {
      return neighbors;
    }

    boolean isScrollIncomplete() {
      return scrollIncomplete;
    }
  }

  private static final class EdgeAccumulator {
    private final EntityGraphDefinition definition;
    private final Map<String, DirectedEdge> edgesByLine = new LinkedHashMap<>();
    private final Set<String> vertices = new LinkedHashSet<>();
    private boolean bypassed;
    private boolean scrollIncomplete;
    private String bypassReason;

    EdgeAccumulator(EntityGraphDefinition definition) {
      this.definition = definition;
    }

    void addEdge(String sourceUrn, String destUrn, String relType, int maxVertices, int maxEdges) {
      if (bypassed) {
        return;
      }
      vertices.add(sourceUrn);
      vertices.add(destUrn);
      if (vertices.size() > maxVertices) {
        bypass("vertex_limit");
        return;
      }
      DirectedEdge edge =
          DirectedEdge.builder()
              .sourceUrn(sourceUrn)
              .destinationUrn(destUrn)
              .relationshipType(relType)
              .build();
      edgesByLine.putIfAbsent(edge.canonicalLine(), edge);
      if (edgesByLine.size() > maxEdges) {
        bypass("edge_limit");
      }
    }

    private void bypass(String reason) {
      bypassed = true;
      bypassReason = reason;
      log.warn(
          "Graph {} exceeded bounds: reason={} vertices={} edges={}",
          definition.getGraphId(),
          reason,
          vertices.size(),
          edgesByLine.size());
    }

    boolean isBypassed() {
      return bypassed;
    }

    void markScrollIncomplete() {
      scrollIncomplete = true;
    }

    boolean isScrollIncomplete() {
      return scrollIncomplete;
    }

    String getBypassReason() {
      return bypassReason;
    }

    List<DirectedEdge> getEdges() {
      return List.copyOf(edgesByLine.values());
    }
  }
}
