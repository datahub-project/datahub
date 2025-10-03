package com.linkedin.metadata.graph.elastic.utils;

import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_LIFECYCLE_OWNER;
import static com.linkedin.metadata.graph.elastic.utils.GraphQueryConstants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.IntegerArray;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.elastic.ThreadSafePathStore;
import com.linkedin.metadata.models.registry.LineageRegistry.EdgeInfo;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.utils.DataPlatformInstanceUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;

/**
 * Utility class for graph query operations that were previously static methods in
 * BaseGraphQueryDAO. This class provides reusable functionality for graph traversal and path
 * manipulation.
 */
@Slf4j
public final class GraphQueryUtils {

  private GraphQueryUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Common utility method for building query filters. This can be shared across different
   * implementations.
   */
  static void addFilterToQueryBuilder(
      @Nonnull Filter filter,
      @Nullable String node,
      org.opensearch.index.query.BoolQueryBuilder rootQuery) {
    org.opensearch.index.query.BoolQueryBuilder orQuery =
        org.opensearch.index.query.QueryBuilders.boolQuery();
    for (com.linkedin.metadata.query.filter.ConjunctiveCriterion conjunction : filter.getOr()) {
      final org.opensearch.index.query.BoolQueryBuilder andQuery =
          org.opensearch.index.query.QueryBuilders.boolQuery();
      final List<com.linkedin.metadata.query.filter.Criterion> criterionArray =
          conjunction.getAnd();
      if (!criterionArray.stream()
          .allMatch(
              criterion ->
                  com.linkedin.metadata.query.filter.Condition.EQUAL.equals(
                      criterion.getCondition()))) {
        throw new RuntimeException(
            "Currently Elastic query filter only supports EQUAL condition " + criterionArray);
      }
      criterionArray.forEach(
          criterion ->
              andQuery.filter(
                  org.opensearch.index.query.QueryBuilders.termsQuery(
                      (node == null ? "" : node + ".") + criterion.getField(),
                      criterion.getValues())));
      orQuery.should(andQuery);
    }
    if (!orQuery.should().isEmpty()) {
      orQuery.minimumShouldMatch(1);
    }
    rootQuery.filter(orQuery);
  }

  public static BoolQueryBuilder buildQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphQueryConfiguration graphQueryConfiguration,
      @Nonnull final GraphFilters graphFilters) {
    return buildQuery(opContext, graphQueryConfiguration, graphFilters, null);
  }

  public static BoolQueryBuilder buildQuery(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphQueryConfiguration graphQueryConfiguration,
      @Nonnull final GraphFilters graphFilters,
      @Nullable final String lifecycleOwner) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    final RelationshipDirection relationshipDirection = graphFilters.getRelationshipDirection();

    // set source filter
    String sourceNode =
        relationshipDirection == RelationshipDirection.OUTGOING
            ? GraphQueryConstants.SOURCE
            : GraphQueryConstants.DESTINATION;
    if (graphFilters.isSourceTypesFilterEnabled()) {
      finalQuery.filter(
          QueryBuilders.termsQuery(sourceNode + ".entityType", graphFilters.getSourceTypes()));
    }
    Optional.ofNullable(graphFilters.getSourceEntityFilter())
        .ifPresent(
            sourceEntityFilter ->
                addFilterToQueryBuilder(sourceEntityFilter, sourceNode, finalQuery));

    // set destination filter
    String destinationNode =
        relationshipDirection == RelationshipDirection.OUTGOING
            ? GraphQueryConstants.DESTINATION
            : GraphQueryConstants.SOURCE;
    if (graphFilters.isDestinationTypesFilterEnabled()) {
      finalQuery.filter(
          QueryBuilders.termsQuery(
              destinationNode + ".entityType", graphFilters.getDestinationTypes()));
    }
    Optional.ofNullable(graphFilters.getDestinationEntityFilter())
        .ifPresent(
            destinationEntityFilter ->
                addFilterToQueryBuilder(destinationEntityFilter, destinationNode, finalQuery));

    // set relationship filter
    if (!graphFilters.getRelationshipTypes().isEmpty()) {
      BoolQueryBuilder relationshipQuery = QueryBuilders.boolQuery();
      graphFilters
          .getRelationshipTypes()
          .forEach(
              relationshipType ->
                  relationshipQuery.should(
                      QueryBuilders.termQuery(
                          GraphQueryConstants.RELATIONSHIP_TYPE, relationshipType)));
      relationshipQuery.minimumShouldMatch(1);

      finalQuery.filter(relationshipQuery);
    }

    // general filter
    Optional.ofNullable(graphFilters.getRelationshipFilter())
        .map(RelationshipFilter::getOr)
        .ifPresent(or -> addFilterToQueryBuilder(new Filter().setOr(or), null, finalQuery));

    if (lifecycleOwner != null) {
      finalQuery.filter(QueryBuilders.termQuery(EDGE_FIELD_LIFECYCLE_OWNER, lifecycleOwner));
    }
    if (!Optional.ofNullable(opContext.getSearchContext().getSearchFlags().isIncludeSoftDeleted())
        .orElse(false)) {
      applyExcludeSoftDelete(graphQueryConfiguration, finalQuery);
    }

    return finalQuery;
  }

  private static void applyExcludeSoftDelete(
      GraphQueryConfiguration graphQueryConfiguration, BoolQueryBuilder boolQueryBuilder) {
    if (graphQueryConfiguration.isGraphStatusEnabled()) {
      Arrays.stream(EdgeUrnType.values())
          .map(
              edgeUrnType ->
                  QueryBuilders.termsQuery(
                      GraphFilterUtils.getUrnStatusFieldName(edgeUrnType), "true"))
          .filter(statusQuery -> !boolQueryBuilder.mustNot().contains(statusQuery))
          .forEach(boolQueryBuilder::mustNot);
    }
  }

  /**
   * Check if a path contains a cycle by comparing the path size to the unique URNs set size. A
   * cycle exists if any URN appears more than once in the path.
   */
  public static boolean containsCycle(final UrnArray path) {
    Set<Urn> urnSet = path.stream().collect(Collectors.toUnmodifiableSet());
    return (path.size() != urnSet.size());
  }

  /** Check if a URN's platform matches any of the provided platforms. */
  public static boolean platformMatches(Urn urn, UrnArray platforms) {
    return platforms.stream()
        .anyMatch(
            platform ->
                DataPlatformInstanceUtils.getDataPlatform(urn)
                    .toString()
                    .equals(platform.toString()));
  }

  /** Clone a path by creating a new UrnArray with the same contents. */
  public static UrnArray clonePath(final UrnArray basePath) {
    try {
      return basePath.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(String.format("Failed to clone path %s", basePath), e);
    }
  }

  /** Get paths that go through a specific via entity up to a destination URN. */
  public static UrnArrayArray getViaPaths(
      ThreadSafePathStore existingPaths, Urn destinationUrn, Urn viaEntity) {
    Set<UrnArray> destinationPaths = existingPaths.getPaths(destinationUrn);
    UrnArrayArray viaPaths = new UrnArrayArray();
    for (UrnArray destPath : destinationPaths) {
      UrnArray viaPath = new UrnArray();
      for (Urn urn : destPath) {
        viaPath.add(urn);
        if (urn.equals(viaEntity)) {
          break;
        }
      }
      viaPaths.add(viaPath);
    }
    return viaPaths;
  }

  /** Extract the via entity from a document. */
  @Nullable
  public static Urn extractViaEntity(Map<String, Object> document) {
    String viaContent = (String) document.getOrDefault("via", null);
    if (viaContent != null) {
      try {
        return Urn.createFromString(viaContent);
      } catch (Exception e) {
        log.warn("Failed to parse urn from via entity {}", viaContent);
      }
    }
    return null;
  }

  /** Extract the created timestamp from a document. */
  @Nullable
  public static Long extractCreatedOn(Map<String, Object> document) {
    Number createdOnNumber = (Number) document.getOrDefault(CREATED_ON, null);
    return createdOnNumber != null ? createdOnNumber.longValue() : null;
  }

  /** Extract the created actor from a document. */
  @Nullable
  public static Urn extractCreatedActor(Map<String, Object> document) {
    String createdActorString = (String) document.getOrDefault(CREATED_ACTOR, null);
    return createdActorString == null ? null : UrnUtils.getUrn(createdActorString);
  }

  /** Extract the updated timestamp from a document. */
  @Nullable
  public static Long extractUpdatedOn(Map<String, Object> document) {
    Number updatedOnNumber = (Number) document.getOrDefault(UPDATED_ON, null);
    return updatedOnNumber != null ? updatedOnNumber.longValue() : null;
  }

  /** Extract the updated actor from a document. */
  @Nullable
  public static Urn extractUpdatedActor(Map<String, Object> document) {
    String updatedActorString = (String) document.getOrDefault(UPDATED_ACTOR, null);
    return updatedActorString == null ? null : UrnUtils.getUrn(updatedActorString);
  }

  /** Check if a document represents a manual relationship. */
  public static boolean isManual(Map<String, Object> document) {
    Map<String, Object> properties;
    if (document.containsKey(PROPERTIES) && document.get(PROPERTIES) instanceof Map) {
      properties = (Map<String, Object>) document.get(PROPERTIES);
    } else {
      properties = Collections.emptyMap();
    }
    return properties.containsKey(SOURCE) && properties.get(SOURCE).equals(UI);
  }

  /** Check if a relationship is connected to a specific input entity. */
  public static boolean isRelationshipConnectedToInput(
      LineageRelationship relationship, Urn inputUrn, ThreadSafePathStore existingPaths) {

    Set<UrnArray> paths = existingPaths.getPaths(relationship.getEntity());
    if (paths == null || paths.isEmpty()) {
      return false;
    }

    for (UrnArray path : paths) {
      if (path.contains(inputUrn)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Extracts lineage relationships from a search response for a set of entity URNs. This method
   * processes search hits and builds lineage relationships based on the search results.
   *
   * @param entityUrns The set of entity URNs to extract relationships for
   * @param searchResponse The search response containing the hits
   * @param lineageGraphFilters Filters for the lineage graph
   * @param visitedEntities Set of already visited entities
   * @param viaEntities Set of via entities
   * @param numHops Current number of hops in the traversal
   * @param remainingHops Remaining hops to traverse
   * @param existingPaths Existing paths in the graph
   * @param exploreMultiplePaths Whether to explore multiple paths
   * @return List of extracted lineage relationships
   */
  public static List<LineageRelationship> extractRelationships(
      @Nonnull Set<Urn> entityUrns,
      @Nonnull org.opensearch.action.search.SearchResponse searchResponse,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      ThreadSafePathStore existingPaths,
      boolean exploreMultiplePaths) {
    try {
      Map<Urn, LineageRelationship> lineageRelationshipMap = new HashMap<>();
      final SearchHit[] hits = searchResponse.getHits().getHits();
      log.debug("numHits: {}, numHops {}, remainingHops {}", hits.length, numHops, remainingHops);
      int index = -1;
      for (SearchHit hit : hits) {
        processSearchHit(
            hit,
            entityUrns,
            index,
            exploreMultiplePaths,
            visitedEntities,
            lineageGraphFilters,
            existingPaths,
            numHops,
            false,
            lineageRelationshipMap,
            viaEntities);
      }

      List<LineageRelationship> result = new ArrayList<>(lineageRelationshipMap.values());
      log.debug("Number of lineage relationships in list: {}", result.size());
      return result;
    } catch (Exception e) {
      // This exception handler merely exists to log the exception at an appropriate point and
      // rethrow
      log.error("Caught exception", e);
      throw e;
    }
  }

  /**
   * Processes a search hit to extract lineage relationship information. This method extracts URNs,
   * timestamps, and other metadata from the search hit document.
   *
   * @param hit The search hit to process
   * @param entityUrns Set of entity URNs to process
   * @param index Index of the current hit
   * @param exploreMultiplePaths Whether to explore multiple paths
   * @param visitedEntities Set of already visited entities
   * @param lineageGraphFilters Filters for the lineage graph
   * @param existingPaths Existing paths in the graph
   * @param numHops Current number of hops in the traversal
   * @param truncatedChildren Whether children are truncated
   * @param lineageRelationshipMap Map to store lineage relationships
   * @param viaEntities Set of via entities
   */
  public static void processSearchHit(
      SearchHit hit,
      Set<Urn> entityUrns,
      int index,
      boolean exploreMultiplePaths,
      Set<Urn> visitedEntities,
      LineageGraphFilters lineageGraphFilters,
      ThreadSafePathStore existingPaths,
      int numHops,
      boolean truncatedChildren,
      Map<Urn, LineageRelationship> lineageRelationshipMap,
      Set<Urn> viaEntities) {
    index++;
    // Extract fields
    final Map<String, Object> document = hit.getSourceAsMap();

    // Extract source and destination URNs using utility class
    final Urn sourceUrn =
        com.linkedin.metadata.search.utils.UrnExtractionUtils.extractUrnFromNestedFieldSafely(
            document, SOURCE, "source");
    final Urn destinationUrn =
        com.linkedin.metadata.search.utils.UrnExtractionUtils.extractUrnFromNestedFieldSafely(
            document, DESTINATION, "destination");

    // Skip if either URN is null
    if (sourceUrn == null || destinationUrn == null) {
      log.debug(
          "Skipping edge with null URNs: sourceUrn={}, destinationUrn={}",
          sourceUrn,
          destinationUrn);
      return;
    }

    final String type = document.get(RELATIONSHIP_TYPE).toString();
    if (sourceUrn.equals(destinationUrn)) {
      log.debug("Skipping a self-edge of type {} on {}", type, sourceUrn);
      return;
    }
    final Number createdOnNumber = (Number) document.getOrDefault(CREATED_ON, null);
    final Long createdOn = createdOnNumber != null ? createdOnNumber.longValue() : null;
    final Number updatedOnNumber = (Number) document.getOrDefault(UPDATED_ON, null);
    final Long updatedOn = updatedOnNumber != null ? updatedOnNumber.longValue() : null;
    final String createdActorString = (String) document.getOrDefault(CREATED_ACTOR, null);
    final Urn createdActor =
        createdActorString == null ? null : UrnUtils.getUrn(createdActorString);
    final String updatedActorString = (String) document.getOrDefault(UPDATED_ACTOR, null);
    final Urn updatedActor =
        updatedActorString == null ? null : UrnUtils.getUrn(updatedActorString);
    final Map<String, Object> properties;
    if (document.containsKey(PROPERTIES) && document.get(PROPERTIES) instanceof Map) {
      properties = (Map<String, Object>) document.get(PROPERTIES);
    } else {
      properties = Collections.emptyMap();
    }
    boolean isManual = properties.containsKey(SOURCE) && properties.get(SOURCE).equals(UI);
    Urn viaEntity = null;
    String viaContent = (String) document.getOrDefault("via", null);
    if (viaContent != null) {
      try {
        viaEntity = Urn.createFromString(viaContent);
      } catch (Exception e) {
        log.warn("Failed to parse urn from via entity {}", viaContent);
      }
    }

    // Process outgoing edge
    processOutgoingEdge(
        entityUrns,
        sourceUrn,
        index,
        exploreMultiplePaths,
        visitedEntities,
        destinationUrn,
        lineageGraphFilters,
        type,
        existingPaths,
        viaEntity,
        numHops,
        createdOn,
        createdActor,
        updatedOn,
        updatedActor,
        isManual,
        truncatedChildren,
        lineageRelationshipMap,
        viaEntities);

    // Process incoming edge
    processIncomingEdge(
        entityUrns,
        sourceUrn,
        exploreMultiplePaths,
        visitedEntities,
        destinationUrn,
        lineageGraphFilters,
        type,
        existingPaths,
        viaEntity,
        numHops,
        createdOn,
        createdActor,
        updatedOn,
        updatedActor,
        isManual,
        truncatedChildren,
        lineageRelationshipMap,
        viaEntities);
  }

  /**
   * Processes an outgoing edge to create lineage relationships. This method handles the creation of
   * relationships for outgoing edges in the lineage graph.
   *
   * @param entityUrns Set of entity URNs to process
   * @param sourceUrn Source URN of the edge
   * @param index Index of the current edge
   * @param exploreMultiplePaths Whether to explore multiple paths
   * @param visitedEntities Set of already visited entities
   * @param destinationUrn Destination URN of the edge
   * @param lineageGraphFilters Filters for the lineage graph
   * @param type Type of the relationship
   * @param existingPaths Existing paths in the graph
   * @param viaEntity Via entity if any
   * @param numHops Current number of hops in the traversal
   * @param createdOn Creation timestamp
   * @param createdActor Creation actor
   * @param updatedOn Update timestamp
   * @param updatedActor Update actor
   * @param isManual Whether the relationship is manual
   * @param truncatedChildren Whether children are truncated
   * @param lineageRelationshipMap Map to store lineage relationships
   * @param viaEntities Set of via entities
   */
  public static void processOutgoingEdge(
      Set<Urn> entityUrns,
      Urn sourceUrn,
      int index,
      boolean exploreMultiplePaths,
      Set<Urn> visitedEntities,
      Urn destinationUrn,
      LineageGraphFilters lineageGraphFilters,
      String type,
      ThreadSafePathStore existingPaths,
      Urn viaEntity,
      int numHops,
      Long createdOn,
      Urn createdActor,
      Long updatedOn,
      Urn updatedActor,
      boolean isManual,
      boolean truncatedChildren,
      Map<Urn, LineageRelationship> lineageRelationshipMap,
      Set<Urn> viaEntities) {
    if (entityUrns.contains(sourceUrn)) {
      log.debug("{}: entity urns contains source urn {}", index, sourceUrn);
      // Skip if already visited or if we're exploring multiple paths
      // Skip if edge is not a valid outgoing edge
      if ((exploreMultiplePaths || !visitedEntities.contains(destinationUrn))
          && lineageGraphFilters.containsEdgeInfo(
              sourceUrn.getEntityType(),
              new EdgeInfo(type, RelationshipDirection.OUTGOING, destinationUrn.getEntityType()))) {

        if (visitedEntities.contains(destinationUrn)) {
          log.debug("Found a second path to the same urn {}", destinationUrn);
        }
        // Append the edge to a set of unique graph paths.
        if (addEdgeToPaths(existingPaths, sourceUrn, viaEntity, destinationUrn)) {
          final LineageRelationship relationship =
              createLineageRelationship(
                  type,
                  destinationUrn,
                  numHops,
                  new UrnArrayArray(existingPaths.getPaths(destinationUrn)),
                  // Fetch the paths to the next level entity.
                  createdOn,
                  createdActor,
                  updatedOn,
                  updatedActor,
                  isManual,
                  truncatedChildren);
          log.debug("Adding relationship {} to urn {}", relationship, destinationUrn);
          lineageRelationshipMap.put(relationship.getEntity(), relationship);
          if ((viaEntity != null) && (!viaEntities.contains(viaEntity))) {
            UrnArrayArray viaPaths = getViaPaths(existingPaths, destinationUrn, viaEntity);
            LineageRelationship viaRelationship =
                createLineageRelationship(
                    type,
                    viaEntity,
                    numHops,
                    viaPaths,
                    createdOn,
                    createdActor,
                    updatedOn,
                    updatedActor,
                    isManual,
                    truncatedChildren);
            viaEntities.add(viaEntity);
            lineageRelationshipMap.put(viaRelationship.getEntity(), viaRelationship);
            log.debug("Adding via entity {} with paths {}", viaEntity, viaPaths);
          }
        }
        visitedEntities.add(destinationUrn);
      }
    }
  }

  /**
   * Processes an incoming edge to create lineage relationships. This method handles the creation of
   * relationships for incoming edges in the lineage graph.
   *
   * @param entityUrns Set of entity URNs to process
   * @param sourceUrn Source URN of the edge
   * @param exploreMultiplePaths Whether to explore multiple paths
   * @param visitedEntities Set of already visited entities
   * @param destinationUrn Destination URN of the edge
   * @param lineageGraphFilters Filters for the lineage graph
   * @param type Type of the relationship
   * @param existingPaths Existing paths in the graph
   * @param viaEntity Via entity if any
   * @param numHops Current number of hops in the traversal
   * @param createdOn Creation timestamp
   * @param createdActor Creation actor
   * @param updatedOn Update timestamp
   * @param updatedActor Update actor
   * @param isManual Whether the relationship is manual
   * @param truncatedChildren Whether children are truncated
   * @param lineageRelationshipMap Map to store lineage relationships
   * @param viaEntities Set of via entities
   */
  public static void processIncomingEdge(
      Set<Urn> entityUrns,
      Urn sourceUrn,
      boolean exploreMultiplePaths,
      Set<Urn> visitedEntities,
      Urn destinationUrn,
      LineageGraphFilters lineageGraphFilters,
      String type,
      ThreadSafePathStore existingPaths,
      Urn viaEntity,
      int numHops,
      Long createdOn,
      Urn createdActor,
      Long updatedOn,
      Urn updatedActor,
      boolean isManual,
      boolean truncatedChildren,
      Map<Urn, LineageRelationship> lineageRelationshipMap,
      Set<Urn> viaEntities) {
    if (entityUrns.contains(destinationUrn)) {
      // Skip if already visited or if we're exploring multiple paths
      // Skip if edge is not a valid outgoing edge
      log.debug("entity urns contains destination urn {}", destinationUrn);
      if ((exploreMultiplePaths || !visitedEntities.contains(sourceUrn))
          && lineageGraphFilters.containsEdgeInfo(
              destinationUrn.getEntityType(),
              new EdgeInfo(type, RelationshipDirection.INCOMING, sourceUrn.getEntityType()))) {
        if (visitedEntities.contains(sourceUrn)) {
          log.debug("Found a second path to the same urn {}", sourceUrn);
        }
        visitedEntities.add(sourceUrn);
        // Append the edge to a set of unique graph paths.
        if (addEdgeToPaths(existingPaths, destinationUrn, viaEntity, sourceUrn)) {
          log.debug("Adding incoming edge: {}, {}, {}", destinationUrn, viaEntity, sourceUrn);
          final LineageRelationship relationship =
              createLineageRelationship(
                  type,
                  sourceUrn,
                  numHops,
                  new UrnArrayArray(existingPaths.getPaths(sourceUrn)),
                  // Fetch the paths to the next level entity.
                  createdOn,
                  createdActor,
                  updatedOn,
                  updatedActor,
                  isManual,
                  truncatedChildren);
          log.debug("Adding relationship {} to urn {}", relationship, sourceUrn);
          lineageRelationshipMap.put(relationship.getEntity(), relationship);
          if ((viaEntity != null) && (!viaEntities.contains(viaEntity))) {
            UrnArrayArray viaPaths = getViaPaths(existingPaths, sourceUrn, viaEntity);
            viaEntities.add(viaEntity);
            LineageRelationship viaRelationship =
                createLineageRelationship(
                    type,
                    viaEntity,
                    numHops,
                    viaPaths,
                    createdOn,
                    createdActor,
                    updatedOn,
                    updatedActor,
                    isManual,
                    truncatedChildren);
            lineageRelationshipMap.put(viaRelationship.getEntity(), viaRelationship);
            log.debug("Adding via relationship {} to urn {}", viaRelationship, viaEntity);
          }
        }
      }
    }
  }

  /**
   * Creates a lineage relationship object with the specified parameters. This method constructs a
   * LineageRelationship object with all the necessary metadata.
   *
   * @param type Type of the relationship
   * @param entityUrn Entity URN for the relationship
   * @param numHops Number of hops in the traversal
   * @param paths Paths to reach the entity
   * @param createdOn Creation timestamp
   * @param createdActor Creation actor
   * @param updatedOn Update timestamp
   * @param updatedActor Update actor
   * @param isManual Whether the relationship is manual
   * @param truncatedChildren Whether children are truncated
   * @return The created LineageRelationship object
   */
  public static LineageRelationship createLineageRelationship(
      @Nonnull final String type,
      @Nonnull final Urn entityUrn,
      final int numHops,
      @Nonnull final UrnArrayArray paths,
      @Nullable final Long createdOn,
      @Nullable final Urn createdActor,
      @Nullable final Long updatedOn,
      @Nullable final Urn updatedActor,
      final boolean isManual,
      final boolean truncatedChildren) {
    final LineageRelationship relationship =
        new LineageRelationship()
            .setType(type)
            .setEntity(entityUrn)
            .setDegree(numHops)
            .setDegrees(new IntegerArray(ImmutableList.of(numHops)))
            .setPaths(paths);
    if (createdOn != null) {
      relationship.setCreatedOn(createdOn);
    }
    if (createdActor != null) {
      relationship.setCreatedActor(createdActor);
    }
    if (updatedOn != null) {
      relationship.setUpdatedOn(updatedOn);
    }
    if (updatedActor != null) {
      relationship.setUpdatedActor(updatedActor);
    }
    relationship.setIsManual(isManual);
    relationship.setTruncatedChildren(truncatedChildren);
    return relationship;
  }

  /**
   * Extracts lineage relationships from a stream of search responses. This method processes
   * multiple search responses to extract relationships while respecting entity limits per hop.
   *
   * @param entityUrns Set of entity URNs to process
   * @param searchResponses Stream of search responses
   * @param lineageGraphFilters Filters for the lineage graph
   * @param visitedEntities Set of already visited entities
   * @param viaEntities Set of via entities
   * @param numHops Current number of hops in the traversal
   * @param existingPaths Existing paths in the graph
   * @param exploreMultiplePaths Whether to explore multiple paths
   * @param entitiesPerHopLimit Limit on entities per hop
   * @return List of extracted lineage relationships
   */
  public static List<LineageRelationship> extractRelationshipsFromSearchResponses(
      @Nonnull Set<Urn> entityUrns,
      @Nonnull Stream<org.opensearch.action.search.SearchResponse> searchResponses,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      ThreadSafePathStore existingPaths,
      boolean exploreMultiplePaths,
      @Nullable Integer entitiesPerHopLimit) {

    try {
      Map<Urn, LineageRelationship> lineageRelationshipMap = new ConcurrentHashMap<>();

      // Track count of entities discovered per input entity
      Map<Urn, Set<Urn>> entitiesPerInputUrn = new HashMap<>();
      entityUrns.forEach(urn -> entitiesPerInputUrn.put(urn, new HashSet<>()));

      // Process all search responses first to gather all possible edges
      Map<Urn, Set<SearchHit>> hitsPerInputEntity = new HashMap<>();
      entityUrns.forEach(urn -> hitsPerInputEntity.put(urn, new HashSet<>()));

      // Organize hits by which input entity they relate to
      searchResponses.forEach(
          searchResponse -> {
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
              Map<String, Object> document = hit.getSourceAsMap();

              // Extract source and destination URNs
              String sourceUrnStr =
                  ((Map<String, Object>) document.get(SOURCE)).get("urn").toString();
              String destinationUrnStr =
                  ((Map<String, Object>) document.get(DESTINATION)).get("urn").toString();
              Urn sourceUrn = UrnUtils.getUrn(sourceUrnStr);
              Urn destinationUrn = UrnUtils.getUrn(destinationUrnStr);

              // Assign hit to input entity it relates to
              if (entityUrns.contains(sourceUrn)) {
                hitsPerInputEntity.get(sourceUrn).add(hit);
              }

              if (entityUrns.contains(destinationUrn)) {
                hitsPerInputEntity.get(destinationUrn).add(hit);
              }
            }
          });

      // Now process hits per input entity, respecting individual limits
      for (Urn inputUrn : entityUrns) {
        Set<SearchHit> hits = hitsPerInputEntity.get(inputUrn);
        Set<Urn> discoveredEntities = entitiesPerInputUrn.get(inputUrn);

        // Process hits for this input entity
        int index = -1;
        for (SearchHit hit : hits) {
          index++;
          Map<String, Object> document = hit.getSourceAsMap();

          // Extract source and destination URNs
          String sourceUrnStr = ((Map<String, Object>) document.get(SOURCE)).get("urn").toString();
          String destinationUrnStr =
              ((Map<String, Object>) document.get(DESTINATION)).get("urn").toString();
          Urn sourceUrn = UrnUtils.getUrn(sourceUrnStr);
          Urn destinationUrn = UrnUtils.getUrn(destinationUrnStr);

          // Determine which entity is the "new" entity in this relationship
          Urn newEntityUrn = null;
          boolean isOutgoing = false;

          if (sourceUrn.equals(inputUrn) && !entityUrns.contains(destinationUrn)) {
            // Outgoing edge to a new entity
            newEntityUrn = destinationUrn;
            isOutgoing = true;
          } else if (destinationUrn.equals(inputUrn) && !entityUrns.contains(sourceUrn)) {
            // Incoming edge from a new entity
            newEntityUrn = sourceUrn;
            isOutgoing = false;
          }

          // If we found a new entity and haven't reached the limit yet
          if (newEntityUrn != null
              && (entitiesPerHopLimit == null || discoveredEntities.size() < entitiesPerHopLimit)) {

            // Add to discovered entities for this input
            discoveredEntities.add(newEntityUrn);

            // Process the hit to extract the relationship
            boolean truncatedResults = false; // We'll handle truncation separately

            // Process the search hit based on direction
            if (isOutgoing) {
              processOutgoingEdge(
                  entityUrns,
                  sourceUrn,
                  index,
                  exploreMultiplePaths,
                  visitedEntities,
                  destinationUrn,
                  lineageGraphFilters,
                  document.get(RELATIONSHIP_TYPE).toString(),
                  existingPaths,
                  extractViaEntity(document),
                  numHops,
                  extractCreatedOn(document),
                  extractCreatedActor(document),
                  extractUpdatedOn(document),
                  extractUpdatedActor(document),
                  isManual(document),
                  truncatedResults,
                  lineageRelationshipMap,
                  viaEntities);
            } else {
              processIncomingEdge(
                  entityUrns,
                  sourceUrn,
                  exploreMultiplePaths,
                  visitedEntities,
                  destinationUrn,
                  lineageGraphFilters,
                  document.get(RELATIONSHIP_TYPE).toString(),
                  existingPaths,
                  extractViaEntity(document),
                  numHops,
                  extractCreatedOn(document),
                  extractCreatedActor(document),
                  extractUpdatedOn(document),
                  extractUpdatedActor(document),
                  isManual(document),
                  truncatedResults,
                  lineageRelationshipMap,
                  viaEntities);
            }
          }
        }

        // Mark relationships as truncated if we hit the limit
        if (entitiesPerHopLimit != null && discoveredEntities.size() >= entitiesPerHopLimit) {
          for (LineageRelationship relationship : lineageRelationshipMap.values()) {
            // Check if this relationship belongs to the current input entity
            if (isRelationshipConnectedToInput(relationship, inputUrn, existingPaths)) {
              relationship.setTruncatedChildren(true);
            }
          }
        }
      }

      List<LineageRelationship> result = new ArrayList<>(lineageRelationshipMap.values());
      log.debug("Number of lineage relationships in list: {}", result.size());
      return result;
    } catch (Exception e) {
      log.error("Caught exception while processing search responses", e);
      throw e;
    }
  }

  /**
   * Adds an individual relationship edge to a running set of unique paths to each node in the
   * graph.
   *
   * <p>Specifically, this method updates 'existingPaths', which is a map of an entity urn
   * representing a node in the lineage graph to the full paths that can be traversed to reach it
   * from a the origin node for which lineage was requested.
   *
   * <p>This method strictly assumes that edges are being added IN ORDER, level-by-level working
   * outwards from the originally requested source node. If edges are added to the path set in an
   * out of order manner, then the paths to a given node may be partial / incomplete.
   *
   * <p>Note that calling this method twice with the same edge is not safe. It will result in
   * duplicate paths being appended into the list of paths to the provided child urn.
   *
   * @param existingPaths a running set of unique, uni-directional paths to each node in the graph
   *     starting from the original root node for which lineage was requested.
   * @param parentUrn the "parent" node (or source node) in the edge to add. This is a logical
   *     source node in a uni-directional path from the source to the destination node. Note that
   *     this is NOT always the URN corresponding to the "source" field that is physically stored
   *     inside the Graph Store.
   * @param childUrn the "child" node (or dest node) in the edge to add. This is a logical dest node
   *     in a uni-directional path from the source to the destination node. Note that this is NOT
   *     always the URN corresponding to the "destination" field that is physically stored inside
   *     the Graph Store.
   */
  public static boolean addEdgeToPaths(
      @Nonnull final ThreadSafePathStore existingPaths,
      @Nonnull final Urn parentUrn,
      final Urn viaUrn,
      @Nonnull final Urn childUrn) {
    boolean edgeAdded = false;
    // Collect all full-paths to this child node. This is what will be returned.
    Set<UrnArray> pathsToParent = existingPaths.getPaths(parentUrn);
    if (!pathsToParent.isEmpty()) {
      // If there are existing paths to this parent node, then we attempt
      // to append the child to each of the existing paths (lengthen it).
      // We then store this as a separate, unique path associated with the child.
      for (UrnArray pathToParent : pathsToParent) {
        if (GraphQueryUtils.containsCycle(pathToParent)) {
          log.debug("Skipping extending path {} because it contains a cycle", pathToParent);
          continue;
        }
        UrnArray pathToChild = GraphQueryUtils.clonePath(pathToParent);
        if (viaUrn != null) {
          pathToChild.add(viaUrn);
        }
        pathToChild.add(childUrn);
        // Use the thread-safe addPath method which handles duplicates automatically
        existingPaths.addPath(childUrn, pathToChild);
        edgeAdded = true;
      }
    } else {
      // No existing paths to this parent urn. Let's create a new path to the child!
      UrnArray pathToChild = new UrnArray();
      if (viaUrn == null) {
        pathToChild.addAll(ImmutableList.of(parentUrn, childUrn));
      } else {
        pathToChild.addAll(ImmutableList.of(parentUrn, viaUrn, childUrn));
      }
      // Use the thread-safe addPath method which handles duplicates automatically
      existingPaths.addPath(childUrn, pathToChild);
      edgeAdded = true;
    }
    return edgeAdded;
  }
}
