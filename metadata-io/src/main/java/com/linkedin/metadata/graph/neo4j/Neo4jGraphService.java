package com.linkedin.metadata.graph.neo4j;

import com.datahub.util.Statement;
import com.datahub.util.exception.RetryLimitReached;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.types.Relationship;

@Slf4j
public class Neo4jGraphService implements GraphService {

  private static final int MAX_TRANSACTION_RETRY = 3;
  private final LineageRegistry lineageRegistry;
  private final Driver driver;
  private final SessionConfig sessionConfig;
  @Getter private final GraphServiceConfiguration graphServiceConfig;

  public Neo4jGraphService(
      @Nonnull LineageRegistry lineageRegistry,
      @Nonnull Driver driver,
      @Nonnull SessionConfig sessionConfig,
      @Nonnull GraphServiceConfiguration graphServiceConfig) {
    this.lineageRegistry = lineageRegistry;
    this.driver = driver;
    this.sessionConfig = sessionConfig;
    this.graphServiceConfig = graphServiceConfig;
  }

  @Override
  public LineageRegistry getLineageRegistry() {
    return lineageRegistry;
  }

  @Override
  public void addEdge(@Nonnull final Edge edge) {
    log.debug(
        String.format(
            "Adding Edge source: %s, destination: %s, type: %s",
            edge.getSource(), edge.getDestination(), edge.getRelationshipType()));

    final String sourceType = edge.getSource().getEntityType();
    final String destinationType = edge.getDestination().getEntityType();
    final String sourceUrn = edge.getSource().toString();
    final String destinationUrn = edge.getDestination().toString();

    // Introduce startUrn, endUrn for real source node and destination node without consider direct
    // or indirect pattern match
    String endUrn = destinationUrn;
    String startUrn = sourceUrn;
    // Extra relationship typename start with r_ for
    // direct-outgoing-downstream/indirect-incoming-upstream relationships
    String reverseRelationshipType = "r_" + edge.getRelationshipType();

    // Build parameterized query instead of string concatenation
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("MERGE (source:").append(sourceType).append(" {urn: $sourceUrn}) ");
    queryBuilder.append("MERGE (destination:").append(destinationType).append(" {urn: $destUrn}) ");
    queryBuilder
        .append("MERGE (source)-[:")
        .append(edge.getRelationshipType())
        .append("]->(destination) ");

    if (isSourceDestReversed(sourceType, edge.getRelationshipType())) {
      endUrn = sourceUrn;
      startUrn = destinationUrn;
      queryBuilder
          .append("MERGE (destination)-[r:")
          .append(reverseRelationshipType)
          .append("]->(source) ");
    } else {
      queryBuilder
          .append("MERGE (source)-[r:")
          .append(reverseRelationshipType)
          .append("]->(destination) ");
    }

    // Add SET clause for properties
    List<String> propertySetters = new ArrayList<>();

    if (edge.getCreatedOn() != null) {
      propertySetters.add("r.createdOn = $createdOn");
    }
    if (edge.getCreatedActor() != null) {
      propertySetters.add("r.createdActor = $createdActor");
    }
    if (edge.getUpdatedOn() != null) {
      propertySetters.add("r.updatedOn = $updatedOn");
    }
    if (edge.getUpdatedActor() != null) {
      propertySetters.add("r.updatedActor = $updatedActor");
    }

    // Add custom properties
    if (edge.getProperties() != null) {
      for (Map.Entry<String, Object> entry : edge.getProperties().entrySet()) {
        final Set<String> preservedKeySet =
            Set.of("createdOn", "createdActor", "updatedOn", "updatedActor", "startUrn", "endUrn");
        if (preservedKeySet.contains(entry.getKey())) {
          throw new UnsupportedOperationException(
              String.format(
                  "Tried setting properties on graph edge but property key is preserved. Key: %s",
                  entry.getKey()));
        }
        if (entry.getValue() instanceof String) {
          propertySetters.add("r." + entry.getKey() + " = $prop_" + entry.getKey());
        } else {
          throw new UnsupportedOperationException(
              String.format(
                  "Tried setting properties on graph edge but property value type is not supported. Key: %s, Value: %s ",
                  entry.getKey(), entry.getValue()));
        }
      }
    }

    // Add startUrn and endUrn properties
    propertySetters.add("r.startUrn = $startUrn");
    propertySetters.add("r.endUrn = $endUrn");

    // Add SET clause if there are properties to set
    if (!propertySetters.isEmpty()) {
      queryBuilder.append("SET ").append(String.join(", ", propertySetters));
    }

    // Build parameters map
    Map<String, Object> params = new HashMap<>();
    params.put("sourceUrn", sourceUrn);
    params.put("destUrn", destinationUrn);
    params.put("startUrn", startUrn);
    params.put("endUrn", endUrn);

    if (edge.getCreatedOn() != null) {
      params.put("createdOn", edge.getCreatedOn());
    }
    if (edge.getCreatedActor() != null) {
      params.put("createdActor", edge.getCreatedActor());
    }
    if (edge.getUpdatedOn() != null) {
      params.put("updatedOn", edge.getUpdatedOn());
    }
    if (edge.getUpdatedActor() != null) {
      params.put("updatedActor", edge.getUpdatedActor());
    }

    // Add custom properties to parameters
    if (edge.getProperties() != null) {
      for (Map.Entry<String, Object> entry : edge.getProperties().entrySet()) {
        if (entry.getValue() instanceof String) {
          params.put("prop_" + entry.getKey(), entry.getValue());
        }
      }
    }

    // Execute the query
    final Statement statement = buildStatement(queryBuilder.toString(), params);
    executeStatements(Collections.singletonList(statement));
  }

  @Override
  public void upsertEdge(final Edge edge) {
    addEdge(edge);
  }

  @Override
  public void removeEdge(final Edge edge) {
    log.debug(
        String.format(
            "Deleting Edge source: %s, destination: %s, type: %s",
            edge.getSource(), edge.getDestination(), edge.getRelationshipType()));

    final String sourceType = edge.getSource().getEntityType();
    final String destinationType = edge.getDestination().getEntityType();
    final String sourceUrn = edge.getSource().toString();
    final String destinationUrn = edge.getDestination().toString();

    String startUrn = sourceUrn;
    String startType = sourceType;
    String endUrn = destinationUrn;
    String endType = destinationType;
    String reverseRelationshipType = "r_" + edge.getRelationshipType();

    if (isSourceDestReversed(sourceType, edge.getRelationshipType())) {
      endUrn = sourceUrn;
      endType = sourceType;
      startUrn = destinationUrn;
      startType = destinationType;
    }

    final List<Statement> statements = new ArrayList<>();

    // DELETE relationship - using parameterized query for property values
    final String mergeRelationshipTemplate =
        "MATCH (source:%s {urn: $sourceUrn})-[r:%s]->(destination:%s {urn: $destUrn}) DELETE r";

    // Format the query template with node labels and relationship type
    final String statement =
        String.format(
            mergeRelationshipTemplate, sourceType, edge.getRelationshipType(), destinationType);

    // Parameters for the first statement - only for property values
    Map<String, Object> params = new HashMap<>();
    params.put("sourceUrn", sourceUrn);
    params.put("destUrn", destinationUrn);

    statements.add(buildStatement(statement, params));

    // Format the query template for reverse relationship
    final String statementR =
        String.format(mergeRelationshipTemplate, startType, reverseRelationshipType, endType);

    // Parameters for the reverse relationship statement - only for property values
    Map<String, Object> paramsR = new HashMap<>();
    paramsR.put("sourceUrn", startUrn);
    paramsR.put("destUrn", endUrn);

    statements.add(buildStatement(statementR, paramsR));

    executeStatements(statements);
  }

  @Nonnull
  @Override
  public EntityLineageResult getLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      int offset,
      @Nullable Integer count,
      int maxHops) {
    log.debug("Neo4j getLineage maxHops = {}", maxHops);
    count = ConfigUtils.applyLimit(graphServiceConfig, count);
    final var statementAndParams =
        generateLineageStatementAndParameters(
            entityUrn,
            lineageGraphFilters,
            maxHops,
            opContext.getSearchContext().getLineageFlags());

    final var statement = statementAndParams.getFirst();
    final var parameters = statementAndParams.getSecond();

    List<Record> neo4jResult =
        statement != null
            ? runQuery(buildStatement(statement, parameters)).list()
            : new ArrayList<>();

    LineageRelationshipArray relations = new LineageRelationshipArray();
    neo4jResult.stream()
        .skip(offset)
        .limit(count)
        .forEach(
            item -> {
              String urn = item.values().get(2).asNode().get("urn").asString();
              try {
                final var path = item.get(1).asPath();
                final List<Urn> nodeListAsPath =
                    StreamSupport.stream(path.nodes().spliterator(), false)
                        .map(node -> createFromString(node.get("urn").asString()))
                        .collect(Collectors.toList());

                final var firstRelationship =
                    Optional.ofNullable(Iterables.getFirst(path.relationships(), null));

                relations.add(
                    new LineageRelationship()
                        .setEntity(Urn.createFromString(urn))
                        // although firstRelationship should never be absent, provide "" as fallback
                        // value
                        .setType(firstRelationship.map(Relationship::type).orElse(""))
                        .setDegree(path.length())
                        .setPaths(new UrnArrayArray(new UrnArray(nodeListAsPath))));
              } catch (URISyntaxException ignored) {
                log.warn("Can't convert urn = {}, Error = {}", urn, ignored.getMessage());
              }
            });
    EntityLineageResult result =
        new EntityLineageResult()
            .setStart(offset)
            .setCount(relations.size())
            .setRelationships(relations)
            .setTotal(neo4jResult.size());

    log.debug("Neo4j getLineage results = {}", result);
    return result;
  }

  private String getPathFindingLabelFilter(Set<String> entityNames) {
    return entityNames.stream().map(x -> String.format("+%s", x)).collect(Collectors.joining("|"));
  }

  private String getPathFindingRelationshipFilter(
      @Nonnull Set<String> entityNames, @Nullable LineageDirection direction) {
    // relationshipFilter supports mixing different directions for various relation types,
    // so simply transform entries lineage registry into format of filter
    final var filterComponents = new HashSet<String>();
    for (final var entityName : entityNames) {
      if (direction != null) {
        for (final var edgeInfo : lineageRegistry.getLineageRelationships(entityName, direction)) {
          final var type = edgeInfo.getType();
          if (edgeInfo.getDirection() == RelationshipDirection.INCOMING) {
            filterComponents.add("<" + type);
          } else {
            filterComponents.add(type + ">");
          }
        }
      } else {
        // return disjunctive combination of edge types regardless of direction
        for (final var direction1 :
            List.of(LineageDirection.UPSTREAM, LineageDirection.DOWNSTREAM)) {
          for (final var edgeInfo :
              lineageRegistry.getLineageRelationships(entityName, direction1)) {
            filterComponents.add(edgeInfo.getType());
          }
        }
      }
    }
    return String.join("|", filterComponents);
  }

  private Pair<String, Map<String, Object>> generateLineageStatementAndParameters(
      @Nonnull Urn entityUrn,
      LineageGraphFilters lineageGraphFilters,
      int maxHops,
      @Nullable LineageFlags lineageFlags) {

    final var parameterMap =
        new HashMap<String, Object>(
            Map.of(
                "urn", entityUrn.toString(),
                "labelFilter",
                    getPathFindingLabelFilter(lineageGraphFilters.getAllowedEntityTypes()),
                "relationshipFilter",
                    getPathFindingRelationshipFilter(
                        lineageGraphFilters.getAllowedEntityTypes(),
                        lineageGraphFilters.getLineageDirection()),
                "maxHops", maxHops));

    final String entityType = entityUrn.getEntityType();

    if (lineageFlags == null
        || (lineageFlags.getStartTimeMillis() == null && lineageFlags.getEndTimeMillis() == null)) {
      // if no time filtering required, simply find all expansion paths to other nodes
      final var statement =
          String.format(
              "MATCH (a:%s {urn: $urn}) "
                  + "CALL apoc.path.spanningTree(a, { "
                  + "  relationshipFilter: $relationshipFilter, "
                  + "  labelFilter: $labelFilter, "
                  + "  minLevel: 1, "
                  + "  maxLevel: $maxHops "
                  + "}) "
                  + "YIELD path "
                  + "WITH a, path AS path "
                  + "RETURN a, path, last(nodes(path));",
              entityType);
      return Pair.of(statement, parameterMap);
    } else {
      // when needing time filtering, possibility on multiple paths between two
      // nodes must be considered, and we need to construct more complex query

      // use r_ edges until they are no longer useful
      final var relationFilter =
          getPathFindingRelationshipFilter(lineageGraphFilters.getAllowedEntityTypes(), null)
              .replaceAll("(\\w+)", "r_$1");
      final var relationshipPattern =
          String.format(
              (lineageGraphFilters.getLineageDirection() == LineageDirection.UPSTREAM
                  ? "<-[:%s*1..%d]-"
                  : "-[:%s*1..%d]->"),
              relationFilter,
              maxHops);

      // two steps:
      // 1. find list of nodes reachable within maxHops
      // 2. find the shortest paths from start node to every other node in these nodes
      //    (note: according to the docs of shortestPath, WHERE conditions are applied during path
      // exploration, not
      //     after path exploration is done)
      final var statement =
          String.format(
              "MATCH (a:%s {urn: $urn}) "
                  + "CALL apoc.path.subgraphNodes(a, { "
                  + "  relationshipFilter: $relationshipFilter, "
                  + "  labelFilter: $labelFilter, "
                  + "  minLevel: 1, "
                  + "  maxLevel: $maxHops "
                  + "}) "
                  + "YIELD node AS b "
                  + "WITH a, b "
                  + "MATCH path = shortestPath((a)"
                  + relationshipPattern
                  + "(b)) "
                  + "WHERE a <> b "
                  + "  AND ALL(rt IN relationships(path) WHERE "
                  + "    (rt.source IS NOT NULL AND rt.source = 'UI') OR "
                  + "    (rt.createdOn IS NULL AND rt.updatedOn IS NULL) OR "
                  + "    ($startTimeMillis <= rt.createdOn <= $endTimeMillis OR "
                  + "     $startTimeMillis <= rt.updatedOn <= $endTimeMillis) "
                  + "  ) "
                  + "RETURN a, path, b;",
              entityType);

      // provide dummy start/end time when not provided, so no need to
      // format clause differently if either of them is missing
      parameterMap.put(
          "startTimeMillis",
          lineageFlags.getStartTimeMillis() == null ? 0 : lineageFlags.getStartTimeMillis());
      parameterMap.put(
          "endTimeMillis",
          lineageFlags.getEndTimeMillis() == null
              ? System.currentTimeMillis()
              : lineageFlags.getEndTimeMillis());

      return Pair.of(statement, parameterMap);
    }
  }

  @Override
  @Nonnull
  public RelatedEntitiesResult findRelatedEntities(
      @Nonnull final OperationContext opContext,
      @Nonnull final GraphFilters graphFilters,
      final int offset,
      @Nullable Integer count) {

    count = ConfigUtils.applyLimit(graphServiceConfig, count);

    log.debug(
        "Finding related Neo4j nodes sourceType: {}, sourceEntityFilter: {}, destinationType: {}, destinationEntityFilter: {}, relationshipTypes: {}, relationshipFilter: {}, offset: {}, count: {}",
        graphFilters.getSourceTypes(),
        graphFilters.getSourceEntityFilter(),
        graphFilters.getDestinationTypes(),
        graphFilters.getDestinationEntityFilter(),
        graphFilters.getRelationshipTypes(),
        graphFilters.getRelationshipFilter(),
        offset,
        count);

    if (graphFilters.noResultsByType()) {
      return new RelatedEntitiesResult(offset, 0, 0, Collections.emptyList());
    }

    final String srcCriteria = filterToCriteria(graphFilters.getSourceEntityFilter()).trim();
    final String destCriteria = filterToCriteria(graphFilters.getDestinationEntityFilter()).trim();
    final String edgeCriteria = relationshipFilterToCriteria(graphFilters.getRelationshipFilter());

    final RelationshipDirection relationshipDirection = graphFilters.getRelationshipDirection();

    String matchTemplate = "MATCH (src %s)-[r%s %s]-(dest %s)%s";
    if (relationshipDirection == RelationshipDirection.INCOMING) {
      matchTemplate = "MATCH (src %s)<-[r%s %s]-(dest %s)%s";
    } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
      matchTemplate = "MATCH (src %s)-[r%s %s]->(dest %s)%s";
    }

    String srcNodeLabel = StringUtils.EMPTY;
    // Create a URN from the String. Only proceed if srcCriteria is not null or empty
    if (StringUtils.isNotEmpty(srcCriteria)) {
      final String urnValue =
          graphFilters
              .getSourceEntityFilter()
              .getOr()
              .get(0)
              .getAnd()
              .get(0)
              .getValues()
              .get(0)
              .toString();
      try {
        final Urn urn = Urn.createFromString(urnValue);
        srcNodeLabel = urn.getEntityType();
        matchTemplate = matchTemplate.replace("(src ", "(src:%s ");
      } catch (URISyntaxException e) {
        log.error("Failed to parse URN: {} ", urnValue, e);
      }
    }

    String relationshipTypeFilter = "";
    if (!graphFilters.getRelationshipTypes().isEmpty()) {
      relationshipTypeFilter = ":" + StringUtils.join(graphFilters.getRelationshipTypes(), "|");
    }

    String whereClause =
        computeEntityTypeWhereClause(
            graphFilters.getSourceTypes(), graphFilters.getDestinationTypes());

    // Build Statement strings
    String baseStatementString;

    if (StringUtils.isNotEmpty(srcNodeLabel)) {
      baseStatementString =
          String.format(
              matchTemplate,
              srcNodeLabel,
              srcCriteria,
              relationshipTypeFilter,
              edgeCriteria,
              destCriteria,
              whereClause);
    } else {
      baseStatementString =
          String.format(
              matchTemplate,
              srcCriteria,
              relationshipTypeFilter,
              edgeCriteria,
              destCriteria,
              whereClause);
    }
    log.info(baseStatementString);

    final String returnNodes =
        "RETURN dest, type(r)"; // Return both related entity and the relationship type.
    final String returnCount = "RETURN count(*)"; // For getting the total results.

    final String resultStatementString =
        String.format("%s %s SKIP $offset LIMIT $count", baseStatementString, returnNodes);
    final String countStatementString = String.format("%s %s", baseStatementString, returnCount);

    // Build Statements
    final Statement resultStatement =
        new Statement(resultStatementString, ImmutableMap.of("offset", offset, "count", count));
    final Statement countStatement = new Statement(countStatementString, Collections.emptyMap());

    // Execute Queries
    final List<RelatedEntity> relatedEntities =
        runQuery(resultStatement)
            .list(
                record ->
                    new RelatedEntity(
                        record.values().get(1).asString(), // Relationship Type
                        record
                            .values()
                            .get(0)
                            .asNode()
                            .get("urn")
                            .asString(), // Urn TODO: Validate this works against Neo4j.
                        null));
    final int totalCount = runQuery(countStatement).single().get(0).asInt();
    return new RelatedEntitiesResult(offset, relatedEntities.size(), totalCount, relatedEntities);
  }

  private String computeEntityTypeWhereClause(
      @Nonnull final Set<String> sourceTypes, @Nonnull final Set<String> destinationTypes) {
    String whereClause = " WHERE left(type(r), 2)<>'r_' ";

    Boolean hasSourceTypes = sourceTypes != null && !sourceTypes.isEmpty();
    Boolean hasDestTypes = destinationTypes != null && !destinationTypes.isEmpty();
    if (hasSourceTypes && hasDestTypes) {
      whereClause =
          String.format(
              " WHERE left(type(r), 2)<>'r_' AND %s AND %s",
              sourceTypes.stream().map(type -> "src:" + type).collect(Collectors.joining(" OR ")),
              destinationTypes.stream()
                  .map(type -> "dest:" + type)
                  .collect(Collectors.joining(" OR ")));
    } else if (hasSourceTypes) {
      whereClause =
          String.format(
              " WHERE left(type(r), 2)<>'r_' AND %s",
              sourceTypes.stream().map(type -> "src:" + type).collect(Collectors.joining(" OR ")));
    } else if (hasDestTypes) {
      whereClause =
          String.format(
              " WHERE left(type(r), 2)<>'r_' AND %s",
              destinationTypes.stream()
                  .map(type -> "dest:" + type)
                  .collect(Collectors.joining(" OR ")));
    }
    return whereClause;
  }

  public void removeNode(@Nonnull final OperationContext opContext, @Nonnull final Urn urn) {

    log.debug("Removing Neo4j node with urn: {}", urn);
    final String srcNodeLabel = urn.getEntityType();

    // also delete any relationship going to or from it
    final String matchTemplate = "MATCH (node:%s {urn: $urn}) DETACH DELETE node";
    final String statement = String.format(matchTemplate, srcNodeLabel);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    runQuery(buildStatement(statement, params)).consume();
  }

  /**
   * Remove relationships and reverse relationships by check incoming/outgoing relationships. for
   * example: a-[consumes]->b, a<-[r_consumes]-b a-[produces]->b, a-[r_produces]->b should not
   * remove a<-[r_downstreamOf]-b when relationshipDirection equal incoming. should remove
   * a-[consumes]->b, a<-[r_consumes]-b, a-[produces]->b, a-[r_produces]->b when
   * relationshipDirection equal outgoing.
   *
   * @param urn Entity relationship type
   * @param relationshipTypes Entity relationship type
   * @param relationshipFilter Query relationship filter
   */
  public void removeEdgesFromNode(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter) {

    log.debug(
        "Removing Neo4j edge types from node with urn: {}, types: {}, filter: {}",
        urn,
        relationshipTypes,
        relationshipFilter);

    // also delete any relationship going to or from it
    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();
    final String srcNodeLabel = urn.getEntityType();

    String matchTemplate = "MATCH (src:%s {urn: $urn})-[r%s]-(dest) RETURN type(r), dest, 2";
    if (relationshipDirection == RelationshipDirection.INCOMING) {
      matchTemplate = "MATCH (src:%s {urn: $urn})<-[r%s]-(dest) RETURN type(r), dest, 0";
    } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
      matchTemplate = "MATCH (src:%s {urn: $urn})-[r%s]->(dest) RETURN type(r), dest, 1";
    }

    String relationshipTypeFilter = "";
    if (!relationshipTypes.isEmpty()) {
      relationshipTypeFilter = ":" + StringUtils.join(relationshipTypes, "|");
    }
    final String statement = String.format(matchTemplate, srcNodeLabel, relationshipTypeFilter);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());
    List<Record> neo4jResult =
        statement != null ? runQuery(buildStatement(statement, params)).list() : new ArrayList<>();
    if (!neo4jResult.isEmpty()) {
      String removeMode = neo4jResult.get(0).values().get(2).toString();
      if (removeMode.equals("2")) {
        final String matchDeleteTemplate = "MATCH (src:%s {urn: $urn})-[r%s]-(dest) DELETE r";
        relationshipTypeFilter = "";
        if (!relationshipTypes.isEmpty()) {
          relationshipTypeFilter =
              ":"
                  + StringUtils.join(relationshipTypes, "|")
                  + "|r_"
                  + StringUtils.join(relationshipTypes, "|r_");
        }
        final String statementNoDirection =
            String.format(matchDeleteTemplate, srcNodeLabel, relationshipTypeFilter);
        runQuery(buildStatement(statementNoDirection, params)).consume();
      } else {
        for (Record typeDest : neo4jResult) {
          String relationshipType = typeDest.values().get(0).asString();
          String destUrnString = typeDest.values().get(1).asNode().get("urn").asString();
          Urn destUrn = createFromString(destUrnString);
          if (removeMode.equals("0")) {
            removeEdge(new Edge(destUrn, urn, relationshipType, null, null, null, null, null));
          } else {
            removeEdge(new Edge(urn, destUrn, relationshipType, null, null, null, null, null));
          }
        }
      }
    }
  }

  public void removeNodesMatchingLabel(@Nonnull String labelPattern) {
    log.debug("Removing Neo4j nodes matching label {}", labelPattern);
    final String matchTemplate =
        "MATCH (n) WHERE any(l IN labels(n) WHERE l=~'%s') DETACH DELETE n";
    final String statement = String.format(matchTemplate, labelPattern);

    final Map<String, Object> params = new HashMap<>();

    runQuery(buildStatement(statement, params)).consume();
  }

  @Override
  public void clear() {
    removeNodesMatchingLabel(".*");
  }

  @VisibleForTesting
  public void wipe() {
    runQuery(new Statement("MATCH (n) DETACH DELETE n", Map.of())).consume();
  }

  // visible for testing
  @Nonnull
  Statement buildStatement(@Nonnull String queryTemplate, @Nonnull Map<String, Object> params) {
    for (Map.Entry<String, Object> entry : params.entrySet()) {
      String k = entry.getKey();
      Object v = entry.getValue();
      params.put(k, toPropertyValue(v));
    }
    return new Statement(queryTemplate, params);
  }

  @Nonnull
  private Object toPropertyValue(@Nonnull Object obj) {
    if (obj instanceof Urn) {
      return obj.toString();
    }
    return obj;
  }

  @AllArgsConstructor
  @Data
  private static final class ExecutionResult {
    private long tookMs;
    private int retries;
  }

  /**
   * Executes a list of statements with parameters in one transaction.
   *
   * @param statements List of statements with parameters to be executed in order
   */
  private ExecutionResult executeStatements(@Nonnull List<Statement> statements) {
    final StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    int retry = 0;
    try (final Session session = driver.session(sessionConfig)) {
      for (retry = 0; retry <= MAX_TRANSACTION_RETRY; retry++) {
        try {
          session.executeWrite(
              tx -> {
                for (Statement statement : statements) {
                  tx.run(statement.getCommandText(), statement.getParams());
                }
                return null;
              });
          break;
        } catch (Neo4jException e) {
          log.warn("Failed to execute Neo4j write transaction. Retry count: {}", retry, e);
          if (retry == MAX_TRANSACTION_RETRY) {
            throw new RetryLimitReached(
                "Failed to execute Neo4j write transaction after "
                    + MAX_TRANSACTION_RETRY
                    + " retries",
                e);
          }
        }
      }
    }

    stopWatch.stop();
    return new ExecutionResult(stopWatch.getTime(), retry);
  }

  /**
   * Runs a query statement with parameters and return StatementResult.
   *
   * @param statement a statement with parameters to be executed
   * @return list of elements in the query result
   */
  @Nonnull
  @WithSpan
  private Result runQuery(@Nonnull Statement statement) {
    log.debug("Running Neo4j query {}", statement);
    return driver.session(sessionConfig).run(statement.getCommandText(), statement.getParams());
  }

  // Returns "key:value" String, if value is not primitive, then use toString() and double quote it
  @Nonnull
  private static String toCriterionString(@Nonnull String key, @Nonnull Object value) {
    if (ClassUtils.isPrimitiveOrWrapper(value.getClass())) {
      return key + ":" + value;
    }

    return key + ":\"" + value + "\"";
  }

  /**
   * Converts {@link RelationshipFilter} to neo4j query criteria, filter criterion condition
   * requires to be EQUAL.
   *
   * @param filter Query relationship filter
   * @return Neo4j criteria string
   */
  @Nonnull
  private static String relationshipFilterToCriteria(@Nonnull RelationshipFilter filter) {
    return disjunctionToCriteria(filter.getOr());
  }

  /**
   * Converts {@link Filter} to neo4j query criteria, filter criterion condition requires to be
   * EQUAL.
   *
   * @param filter Query Filter
   * @return Neo4j criteria string
   */
  @Nonnull
  private static String filterToCriteria(@Nonnull Filter filter) {
    return disjunctionToCriteria(filter.getOr());
  }

  private static String disjunctionToCriteria(final ConjunctiveCriterionArray disjunction) {
    if (disjunction.size() > 1) {
      // TODO: Support disjunctions (ORs).
      throw new UnsupportedOperationException(
          "Neo4j query filter only supports 1 set of conjunction criteria");
    }
    final CriterionArray criterionArray =
        disjunction.size() > 0 ? disjunction.get(0).getAnd() : new CriterionArray();
    return criterionToString(criterionArray);
  }

  /**
   * Converts {@link CriterionArray} to neo4j query string.
   *
   * @param criterionArray CriterionArray in a Filter
   * @return Neo4j criteria string
   */
  @Nonnull
  private static String criterionToString(@Nonnull CriterionArray criterionArray) {
    if (!criterionArray.stream()
        .allMatch(criterion -> Condition.EQUAL.equals(criterion.getCondition()))) {
      throw new RuntimeException(
          "Neo4j query filter only support EQUAL condition " + criterionArray);
    }

    final StringJoiner joiner = new StringJoiner(",", "{", "}");

    criterionArray.forEach(
        criterion ->
            joiner.add(toCriterionString(criterion.getField(), criterion.getValues().get(0))));

    return joiner.length() <= 2 ? "" : joiner.toString();
  }

  /** Gets Node based on Urn, if not exist, creates placeholder node. */
  @Nonnull
  private Statement getOrInsertNode(@Nonnull Urn urn) {
    final String nodeType = urn.getEntityType();

    final String mergeTemplate = "MERGE (node:%s {urn: $urn}) RETURN node";
    final String statement = String.format(mergeTemplate, nodeType);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    return buildStatement(statement, params);
  }

  @Override
  public boolean supportsMultiHop() {
    return true;
  }

  /**
   * Reverse incoming/outgoing direction check by compare sourceType and relationshipType to
   * LineageSpec. for example: sourceType: dataset, relationshipType: downstreamOf. downstreamOf
   * relationship type and outgoing relationship direction for dataset from LineageSpec, is inside
   * upstreamEdges. source(dataset) -[downstreamOf]-> dest means upstreamEdge for source(dataset)
   * dest -[r_downstreamOf]-> source(dataset), need reverse source and dest * sourceType: datajob,
   * relationshipType: produces. produces relationship type and outgoing relationship direction for
   * datajob from LineageSpec, is inside downstreamEdges. source(datajob) -[produces]-> dest means
   * downstreamEdge for source(datajob) source(dataset) -[r_produces]-> dest, do not need to reverse
   * source and dest
   *
   * @param sourceType Entity type
   * @param relationshipType Entity relationship type
   */
  private boolean isSourceDestReversed(
      @Nonnull String sourceType, @Nonnull String relationshipType) {
    // Get real direction by check INCOMING/OUTGOING direction and RelationshipType
    LineageRegistry.LineageSpec sourceLineageSpec = getLineageRegistry().getLineageSpec(sourceType);
    if (sourceLineageSpec != null) {
      List<LineageRegistry.EdgeInfo> upstreamCheck =
          sourceLineageSpec.getUpstreamEdges().stream()
              .filter(
                  t ->
                      t.getDirection() == RelationshipDirection.OUTGOING
                          && t.getType().equals(relationshipType))
              .collect(Collectors.toList());
      if (!upstreamCheck.isEmpty() || sourceType.equals("schemaField")) {
        return true;
      }
    }
    return false;
  }

  protected static @Nullable Urn createFromString(@Nonnull String rawUrn) {
    try {
      return Urn.createFromString(rawUrn);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  @Nonnull
  @Override
  public RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {

    count = ConfigUtils.applyLimit(graphServiceConfig, count);

    if (graphFilters.noResultsByType()) {
      return new RelatedEntitiesScrollResult(0, 0, null, Collections.emptyList());
    }

    final String srcCriteria = filterToCriteria(graphFilters.getSourceEntityFilter()).trim();
    final String destCriteria = filterToCriteria(graphFilters.getDestinationEntityFilter()).trim();
    final String edgeCriteria = relationshipFilterToCriteria(graphFilters.getRelationshipFilter());

    final RelationshipDirection relationshipDirection = graphFilters.getRelationshipDirection();

    String matchTemplate = "MATCH (src %s)-[r%s %s]-(dest %s)%s";
    if (relationshipDirection == RelationshipDirection.INCOMING) {
      matchTemplate = "MATCH (src %s)<-[r%s %s]-(dest %s)%s";
    } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
      matchTemplate = "MATCH (src %s)-[r%s %s]->(dest %s)%s";
    }

    String srcNodeLabel = StringUtils.EMPTY;
    // Create a URN from the String. Only proceed if srcCriteria is not null or empty
    if (StringUtils.isNotEmpty(srcCriteria)) {
      final String urnValue =
          graphFilters
              .getSourceEntityFilter()
              .getOr()
              .get(0)
              .getAnd()
              .get(0)
              .getValues()
              .get(0)
              .toString();
      try {
        final Urn urn = Urn.createFromString(urnValue);
        srcNodeLabel = urn.getEntityType();
        matchTemplate = matchTemplate.replace("(src ", "(src:%s ");
      } catch (URISyntaxException e) {
        log.error("Failed to parse URN: {} ", urnValue, e);
      }
    }

    String relationshipTypeFilter = "";
    if (!graphFilters.getRelationshipTypes().isEmpty()) {
      relationshipTypeFilter = ":" + StringUtils.join(graphFilters.getRelationshipTypes(), "|");
    }

    String whereClause =
        computeEntityTypeWhereClause(
            graphFilters.getSourceTypes(), graphFilters.getDestinationTypes());

    // Build Statement strings
    String baseStatementString;

    if (StringUtils.isNotEmpty(srcNodeLabel)) {
      baseStatementString =
          String.format(
              matchTemplate,
              srcNodeLabel,
              srcCriteria,
              relationshipTypeFilter,
              edgeCriteria,
              destCriteria,
              whereClause);
    } else {
      baseStatementString =
          String.format(
              matchTemplate,
              srcCriteria,
              relationshipTypeFilter,
              edgeCriteria,
              destCriteria,
              whereClause);
    }
    log.info(baseStatementString);

    final String returnNodes =
        "RETURN dest, src, type(r)"; // Return both related entity and the relationship type.
    final String returnCount = "RETURN count(*)"; // For getting the total results.

    final String resultStatementString =
        String.format("%s %s SKIP $offset LIMIT $count", baseStatementString, returnNodes);
    final String countStatementString = String.format("%s %s", baseStatementString, returnCount);

    int offset = 0;
    if (Objects.nonNull(scrollId)) {
      offset = Integer.valueOf(SearchAfterWrapper.fromScrollId(scrollId).getPitId().toString());
    }

    // Build Statements
    final Statement resultStatement =
        new Statement(resultStatementString, ImmutableMap.of("offset", offset, "count", count));
    final Statement countStatement = new Statement(countStatementString, Collections.emptyMap());

    // Execute Queries
    final List<RelatedEntities> relatedEntities =
        runQuery(resultStatement)
            .list(
                record ->
                    new RelatedEntities(
                        record.values().get(2).asString(), // Relationship Type
                        record.values().get(0).asNode().get("urn").asString(),
                        record.values().get(1).asNode().get("urn").asString(),
                        relationshipDirection,
                        null));
    final int totalCount = runQuery(countStatement).single().get(0).asInt();
    log.info("Total Related Entities: {}", totalCount);
    // return new RelatedEntitiesResult(0, relatedEntities.size(), totalCount, relatedEntities);
    String nextScrollId = null;
    if (relatedEntities.size() == count) {
      String pitId = Integer.toString(offset + count);
      nextScrollId = new SearchAfterWrapper(null, pitId, 0L).toScrollId();
    }
    return RelatedEntitiesScrollResult.builder()
        .entities(relatedEntities)
        .pageSize(relatedEntities.size())
        .numResults(totalCount)
        .scrollId(nextScrollId)
        .build();
  }
}
