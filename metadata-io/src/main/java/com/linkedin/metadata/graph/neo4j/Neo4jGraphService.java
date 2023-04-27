package com.linkedin.metadata.graph.neo4j;

import com.codahale.metrics.Timer;
import com.datahub.util.Statement;
import com.datahub.util.exception.RetryLimitReached;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.Edge;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
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
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.types.Node;


@Slf4j
public class Neo4jGraphService implements GraphService {

  private static final int MAX_TRANSACTION_RETRY = 3;
  private final LineageRegistry _lineageRegistry;
  private final Driver _driver;
  private SessionConfig _sessionConfig;

  private static final String SOURCE = "source";
  private static final String UI = "UI";

  public Neo4jGraphService(@Nonnull LineageRegistry lineageRegistry, @Nonnull Driver driver) {
    this(lineageRegistry, driver, SessionConfig.defaultConfig());
  }

  public Neo4jGraphService(@Nonnull LineageRegistry lineageRegistry, @Nonnull Driver driver, @Nonnull SessionConfig sessionConfig) {
    this._lineageRegistry = lineageRegistry;
    this._driver = driver;
    this._sessionConfig = sessionConfig;
  }

  @Override
  public LineageRegistry getLineageRegistry() {
    return _lineageRegistry;
  }

  @Override
  public void addEdge(@Nonnull final Edge edge) {

    log.debug(String.format("Adding Edge source: %s, destination: %s, type: %s",
        edge.getSource(),
        edge.getDestination(),
        edge.getRelationshipType()));

    final String sourceType = edge.getSource().getEntityType();
    final String destinationType = edge.getDestination().getEntityType();
    final String sourceUrn = edge.getSource().toString();
    final String destinationUrn = edge.getDestination().toString();

    // Introduce startUrn, endUrn for real source node and destination node without consider direct or indirect pattern match
    String endUrn = destinationUrn;
    String startUrn = sourceUrn;
    String endType = destinationType;
    String startType = sourceType;
    // Extra relationship typename start with r_ for direct-outgoing-downstream/indirect-incoming-upstream relationships
    String reverseRelationshipType = "r_" + edge.getRelationshipType();

    if (isSourceDestReversed(sourceType, edge.getRelationshipType())) {
      endUrn = sourceUrn;
      endType = sourceType;
      startUrn = destinationUrn;
      startType = destinationType;
    }

    final List<Statement> statements = new ArrayList<>();

    // Add/Update source & destination node first
    statements.add(getOrInsertNode(edge.getSource()));
    statements.add(getOrInsertNode(edge.getDestination()));

    // Add/Update relationship
    final String mergeRelationshipTemplate =
        "MATCH (source:%s {urn: '%s'}),(destination:%s {urn: '%s'}) MERGE (source)-[r:%s]->(destination) ";
    String statement = String.format(mergeRelationshipTemplate, sourceType, sourceUrn, destinationType, destinationUrn,
        edge.getRelationshipType());

    String statementR = String.format(mergeRelationshipTemplate, startType, startUrn, endType, endUrn, reverseRelationshipType);

    // Add/Update relationship properties
    String setCreatedOnTemplate;
    String setcreatedActorTemplate;
    String setupdatedOnTemplate;
    String setupdatedActorTemplate;
    String setPropertyTemplate;
    final StringJoiner propertiesTemplateJoiner = new StringJoiner(", ");
    if (edge.getCreatedOn() != null) {
      setCreatedOnTemplate = String.format("r.createdOn = %s", edge.getCreatedOn());
      propertiesTemplateJoiner.add(setCreatedOnTemplate);
    }
    if (edge.getCreatedActor() != null) {
      setcreatedActorTemplate = String.format("r.createdActor = '%s'", edge.getCreatedActor());
      propertiesTemplateJoiner.add(setcreatedActorTemplate);
    }
    if (edge.getUpdatedOn() != null) {
      setupdatedOnTemplate = String.format("r.updatedOn = %s", edge.getUpdatedOn());
      propertiesTemplateJoiner.add(setupdatedOnTemplate);
    }
    if (edge.getUpdatedActor() != null) {
      setupdatedActorTemplate = String.format("r.updatedActor = '%s'", edge.getUpdatedActor());
      propertiesTemplateJoiner.add(setupdatedActorTemplate);
    }
    if (edge.getProperties() != null) {
      for (Map.Entry<String, Object> entry : edge.getProperties().entrySet()) {
        // Make sure extra keys in properties are not preserved
        final Set<String> preservedKeySet =
            Set.of("createdOn", "createdActor", "updatedOn", "updatedActor", "startUrn", "endUrn");
        if (preservedKeySet.contains(entry.getKey())) {
          throw new UnsupportedOperationException(
              String.format("Tried setting properties on graph edge but property key is preserved. Key: %s",
                  entry.getKey()));
        }
        if (entry.getValue() instanceof String) {
          setPropertyTemplate = String.format("r.%s = '%s'", entry.getKey(), entry.getValue());
          propertiesTemplateJoiner.add(setPropertyTemplate);
        } else {
          throw new UnsupportedOperationException(String.format(
              "Tried setting properties on graph edge but property value type is not supported. Key: %s, Value: %s ",
              entry.getKey(), entry.getValue()));
        }
      }
    }
    final String setStartEndUrnTemplate = String.format("r.startUrn = '%s', r.endUrn = '%s'", startUrn, endUrn);
    propertiesTemplateJoiner.add(setStartEndUrnTemplate);
    if (!StringUtils.isEmpty(propertiesTemplateJoiner.toString())) {
      statementR = String.format("%s SET %s", statementR, propertiesTemplateJoiner);
    }

    statements.add(buildStatement(statement, new HashMap<>()));
    statements.add(buildStatement(statementR, new HashMap<>()));
    executeStatements(statements);
  }

  @Override
  public void upsertEdge(final Edge edge) {
    addEdge(edge);
  }

  @Override
  public void removeEdge(final Edge edge) {
    log.debug(
        String.format("Deleting Edge source: %s, destination: %s, type: %s", edge.getSource(), edge.getDestination(),
            edge.getRelationshipType()));

    final String sourceType = edge.getSource().getEntityType();
    final String destinationType = edge.getDestination().getEntityType();
    final String sourceUrn = edge.getSource().toString();
    final String destinationUrn = edge.getDestination().toString();

    String endUrn = destinationUrn;
    String startUrn = sourceUrn;
    String endType = destinationType;
    String startType = sourceType;
    String reverseRelationshipType = "r_" + edge.getRelationshipType();

    if (isSourceDestReversed(sourceType, edge.getRelationshipType())) {
      endUrn = sourceUrn;
      endType = sourceType;
      startUrn = destinationUrn;
      startType = destinationType;
    }

    final List<Statement> statements = new ArrayList<>();

    // DELETE relationship
    final String mergeRelationshipTemplate = "MATCH (source:%s {urn: '%s'})-[r:%s]->(destination:%s {urn: '%s'}) DELETE r";
    final String statement =
        String.format(mergeRelationshipTemplate, sourceType, sourceUrn, edge.getRelationshipType(), destinationType,
            destinationUrn);
    final String statementR = String.format(mergeRelationshipTemplate, startType, startUrn, reverseRelationshipType, endType, endUrn);

    statements.add(buildStatement(statement, new HashMap<>()));
    statements.add(buildStatement(statementR, new HashMap<>()));
    executeStatements(statements);
  }

  @Nonnull
  @WithSpan
  @Override
  public EntityLineageResult getLineage(@Nonnull Urn entityUrn, @Nonnull LineageDirection direction,
      GraphFilters graphFilters, int offset, int count, int maxHops) {
    return getLineage(entityUrn, direction, graphFilters, offset, count, maxHops, null, null);
  }

  @Nonnull
  @Override
  public EntityLineageResult getLineage(@Nonnull Urn entityUrn, @Nonnull LineageDirection direction,
      GraphFilters graphFilters, int offset, int count, int maxHops, @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    log.debug(String.format("Neo4j getLineage maxHops = %d", maxHops));

    final String statement =
        generateLineageStatement(entityUrn, direction, graphFilters, maxHops, startTimeMillis, endTimeMillis);

    List<Record> neo4jResult =
        statement != null ? runQuery(buildStatement(statement, new HashMap<>())).list() : new ArrayList<>();

    // It is possible to have more than 1 path from node A to node B in the graph and previous query returns all the paths.
    // We convert the List into Map with only the shortest paths. "item.get(i).size()" is the path size between two nodes in relation.
    // The key for mapping is the destination node as the source node is always the same, and it is defined by parameter.
    neo4jResult = neo4jResult.stream()
        .collect(Collectors.toMap(item -> item.values().get(2).asNode().get("urn").asString(), Function.identity(),
            (item1, item2) -> item1.get(1).size() < item2.get(1).size() ? item1 : item2))
        .values()
        .stream()
        .collect(Collectors.toList());

    LineageRelationshipArray relations = new LineageRelationshipArray();
    neo4jResult.stream().skip(offset).limit(count).forEach(item -> {
      String urn = item.values().get(2).asNode().get("urn").asString();
      String relationType = ((InternalRelationship) item.get(1).asList().get(0)).type().split("r_")[1];
      int numHops = item.get(1).size();
      try {
        // Generate path from r in neo4jResult
        List<Urn> pathFromRelationships =
            item.values().get(1).asList(Collections.singletonList(new ArrayList<Node>())).stream().map(t -> createFromString(
                  // Get real upstream node/downstream node by direction
                  ((InternalRelationship) t).get(direction == LineageDirection.UPSTREAM ? "startUrn" : "endUrn")
                      .asString())).collect(Collectors.toList());
        if (direction == LineageDirection.UPSTREAM) {
          // For ui to show path correctly, reverse path for UPSTREAM direction
          Collections.reverse(pathFromRelationships);
          // Add missing original node to the end since we generate path from relationships
          pathFromRelationships.add(Urn.createFromString(item.values().get(0).asNode().get("urn").asString()));
        } else {
          // Add missing original node to the beginning since we generate path from relationships
          pathFromRelationships.add(0, Urn.createFromString(item.values().get(0).asNode().get("urn").asString()));
        }

        relations.add(new LineageRelationship().setEntity(Urn.createFromString(urn))
            .setType(relationType)
            .setDegree(numHops)
            .setPaths(new UrnArrayArray(new UrnArray(pathFromRelationships))));
      } catch (URISyntaxException ignored) {
        log.warn(String.format("Can't convert urn = %s, Error = %s", urn, ignored.getMessage()));
      }
    });

    EntityLineageResult result = new EntityLineageResult().setStart(offset)
            .setCount(relations.size())
            .setRelationships(relations)
            .setTotal(neo4jResult.size());

    log.debug(String.format("Neo4j getLineage results = %s", result));
    return result;
  }

  private String generateLineageStatement(@Nonnull Urn entityUrn, @Nonnull LineageDirection direction,
      GraphFilters graphFilters, int maxHops, @Nullable Long startTimeMillis, @Nullable Long endTimeMillis) {
    String statement;
    final String allowedEntityTypes = String.join(" OR b:", graphFilters.getAllowedEntityTypes());

    final String multiHopMatchTemplateIndirect = "MATCH p = shortestPath((a {urn: '%s'})<-[r*1..%d]-(b)) ";
    final String multiHopMatchTemplateDirect = "MATCH p = shortestPath((a {urn: '%s'})-[r*1..%d]->(b)) ";
    // directionFilterTemplate should apply to all condition.
    final String multiHopMatchTemplate =
        direction == LineageDirection.UPSTREAM ? multiHopMatchTemplateIndirect : multiHopMatchTemplateDirect;
    final String fullQueryTemplate = generateFullQueryTemplate(multiHopMatchTemplate, startTimeMillis, endTimeMillis);

    if (startTimeMillis != null && endTimeMillis != null) {
      statement =
          String.format(fullQueryTemplate, startTimeMillis, endTimeMillis, entityUrn, maxHops, allowedEntityTypes,
              entityUrn);
    } else if (startTimeMillis != null) {
      statement = String.format(fullQueryTemplate, startTimeMillis, entityUrn, maxHops, allowedEntityTypes, entityUrn);
    } else if (endTimeMillis != null) {
      statement = String.format(fullQueryTemplate, endTimeMillis, entityUrn, maxHops, allowedEntityTypes, entityUrn);
    } else {
      statement = String.format(fullQueryTemplate, entityUrn, maxHops, allowedEntityTypes, entityUrn);
    }

    return statement;
  }

  @Nonnull
  public RelatedEntitiesResult findRelatedEntities(@Nullable final List<String> sourceTypes,
      @Nonnull final Filter sourceEntityFilter, @Nullable final List<String> destinationTypes,
      @Nonnull final Filter destinationEntityFilter, @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter, final int offset, final int count) {

    log.debug(String.format("Finding related Neo4j nodes sourceType: %s, sourceEntityFilter: %s, destinationType: %s, ",
        sourceTypes, sourceEntityFilter, destinationTypes) + String.format(
        "destinationEntityFilter: %s, relationshipTypes: %s, relationshipFilter: %s, ", destinationEntityFilter,
        relationshipTypes, relationshipFilter) + String.format("offset: %s, count: %s", offset, count));

    if (sourceTypes != null && sourceTypes.isEmpty() || destinationTypes != null && destinationTypes.isEmpty()) {
      return new RelatedEntitiesResult(offset, 0, 0, Collections.emptyList());
    }

    final String srcCriteria = filterToCriteria(sourceEntityFilter).trim();
    final String destCriteria = filterToCriteria(destinationEntityFilter).trim();
    final String edgeCriteria = relationshipFilterToCriteria(relationshipFilter);

    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();

    String matchTemplate = "MATCH (src %s)-[r%s %s]-(dest %s)%s";
    if (relationshipDirection == RelationshipDirection.INCOMING) {
      matchTemplate = "MATCH (src %s)<-[r%s %s]-(dest %s)%s";
    } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
      matchTemplate = "MATCH (src %s)-[r%s %s]->(dest %s)%s";
    }

    final String returnNodes = String.format("RETURN dest, type(r)"); // Return both related entity and the relationship type.
    final String returnCount = "RETURN count(*)"; // For getting the total results.

    String relationshipTypeFilter = "";
    if (!relationshipTypes.isEmpty()) {
      relationshipTypeFilter = ":" + StringUtils.join(relationshipTypes, "|");
    }

    String whereClause = computeEntityTypeWhereClause(sourceTypes, destinationTypes);

  // Build Statement strings
    String baseStatementString =
        String.format(matchTemplate, srcCriteria, relationshipTypeFilter, edgeCriteria, destCriteria, whereClause);

    log.info(baseStatementString);

    final String resultStatementString = String.format("%s %s SKIP $offset LIMIT $count", baseStatementString, returnNodes);
    final String countStatementString = String.format("%s %s", baseStatementString, returnCount);

    // Build Statements
    final Statement resultStatement = new Statement(resultStatementString, ImmutableMap.of("offset", offset, "count", count));
    final Statement countStatement =  new Statement(countStatementString, Collections.emptyMap());

    // Execute Queries
    final List<RelatedEntity> relatedEntities = runQuery(resultStatement).list(record ->
        new RelatedEntity(
            record.values().get(1).asString(), // Relationship Type
            record.values().get(0).asNode().get("urn").asString())); // Urn TODO: Validate this works against Neo4j.
    final int totalCount = runQuery(countStatement).single().get(0).asInt();
    return new RelatedEntitiesResult(offset, relatedEntities.size(), totalCount, relatedEntities);
  }

  private String computeEntityTypeWhereClause(@Nonnull final List<String> sourceTypes,
      @Nonnull final List<String> destinationTypes) {
    String whereClause = " WHERE left(type(r), 2)<>'r_' ";

    Boolean hasSourceTypes = sourceTypes != null && !sourceTypes.isEmpty();
    Boolean hasDestTypes = destinationTypes != null && !destinationTypes.isEmpty();
    if (hasSourceTypes && hasDestTypes) {
      whereClause = String.format(" WHERE left(type(r), 2)<>'r_' AND %s AND %s",
          sourceTypes.stream().map(type -> "src:" + type).collect(Collectors.joining(" OR ")),
          destinationTypes.stream().map(type -> "dest:" + type).collect(Collectors.joining(" OR ")));
    } else if (hasSourceTypes) {
      whereClause = String.format(" WHERE left(type(r), 2)<>'r_' AND %s",
          sourceTypes.stream().map(type -> "src:" + type).collect(Collectors.joining(" OR ")));
    } else if (hasDestTypes) {
      whereClause = String.format(" WHERE left(type(r), 2)<>'r_' AND %s",
          destinationTypes.stream().map(type -> "dest:" + type).collect(Collectors.joining(" OR ")));
    }
    return whereClause;
  }

  public void removeNode(@Nonnull final Urn urn) {

    log.debug(String.format("Removing Neo4j node with urn: %s", urn));

    // also delete any relationship going to or from it
    final String matchTemplate = "MATCH (node {urn: $urn}) DETACH DELETE node";
    final String statement = String.format(matchTemplate);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    runQuery(buildStatement(statement, params)).consume();
  }

  /**
   * Remove relationships and reverse relationships by check incoming/outgoing relationships.
   * for example:
   * a-[consumes]->b, a<-[r_consumes]-b
   * a-[produces]->b, a-[r_produces]->b
   * should not remove a<-[r_downstreamOf]-b when relationshipDirection equal incoming.
   * should remove a-[consumes]->b, a<-[r_consumes]-b, a-[produces]->b, a-[r_produces]->b
   * when relationshipDirection equal outgoing.
   *
   * @param urn Entity relationship type
   * @param relationshipTypes Entity relationship type
   * @param relationshipFilter Query relationship filter
   *
   */
  public void removeEdgesFromNode(
      @Nonnull final Urn urn,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter) {

    log.debug(String.format("Removing Neo4j edge types from node with urn: %s, types: %s, filter: %s",
        urn,
        relationshipTypes,
        relationshipFilter));

    // also delete any relationship going to or from it
    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();

    String matchTemplate = "MATCH (src {urn: $urn})-[r%s]-(dest) RETURN type(r), dest, 2";
    if (relationshipDirection == RelationshipDirection.INCOMING) {
      matchTemplate = "MATCH (src {urn: $urn})<-[r%s]-(dest) RETURN type(r), dest, 0";
    } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
      matchTemplate = "MATCH (src {urn: $urn})-[r%s]->(dest) RETURN type(r), dest, 1";
    }

    String relationshipTypeFilter = "";
    if (!relationshipTypes.isEmpty()) {
      relationshipTypeFilter = ":" + StringUtils.join(relationshipTypes, "|");
    }
    final String statement = String.format(matchTemplate, relationshipTypeFilter);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());
    List<Record> neo4jResult =
        statement != null ? runQuery(buildStatement(statement, params)).list() : new ArrayList<>();
    if (!neo4jResult.isEmpty()) {
      String removeMode = neo4jResult.get(0).values().get(2).toString();
      if (removeMode.equals("2")) {
        final String matchDeleteTemplate = "MATCH (src {urn: $urn})-[r%s]-(dest) DELETE r";
        relationshipTypeFilter = "";
        if (!relationshipTypes.isEmpty()) {
          relationshipTypeFilter =
              ":" + StringUtils.join(relationshipTypes, "|") + "|r_" + StringUtils.join(relationshipTypes, "|r_");
        }
        final String statementNoDirection = String.format(matchDeleteTemplate, relationshipTypeFilter);
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
    log.debug(String.format("Removing Neo4j nodes matching label %s", labelPattern));
    final String matchTemplate =
        "MATCH (n) WHERE any(l IN labels(n) WHERE l=~'%s') DETACH DELETE n";
    final String statement = String.format(matchTemplate, labelPattern);

    final Map<String, Object> params = new HashMap<>();

    runQuery(buildStatement(statement, params)).consume();
  }

  @Override
  public void configure() {
    // Do nothing
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
  private synchronized ExecutionResult executeStatements(@Nonnull List<Statement> statements) {
    int retry = 0;
    final StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    Exception lastException;
    try (final Session session = _driver.session(_sessionConfig)) {
      do {
        try {
          session.writeTransaction(tx -> {
            for (Statement statement : statements) {
              tx.run(statement.getCommandText(), statement.getParams());
            }
            return 0;
          });
          lastException = null;
          break;
        } catch (Neo4jException e) {
          lastException = e;
        }
      } while (++retry <= MAX_TRANSACTION_RETRY);
    }

    if (lastException != null) {
      throw new RetryLimitReached(
          "Failed to execute Neo4j write transaction after " + MAX_TRANSACTION_RETRY + " retries", lastException);
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
  private Result runQuery(@Nonnull Statement statement) {
    log.debug(String.format("Running Neo4j query %s", statement.toString()));
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "runQuery").time()) {
      return _driver.session(_sessionConfig).run(statement.getCommandText(), statement.getParams());
    }
  }

  @Nonnull
  private static String toCriterionWhereString(@Nonnull String key, @Nonnull Object value) {
    if (ClassUtils.isPrimitiveOrWrapper(value.getClass())) {
      return key + " = " + value;
    }

    return key + " = \"" + value.toString() + "\"";
  }

  // Returns "key:value" String, if value is not primitive, then use toString() and double quote it
  @Nonnull
  private static String toCriterionString(@Nonnull String key, @Nonnull Object value) {
    if (ClassUtils.isPrimitiveOrWrapper(value.getClass())) {
      return key + ":" + value;
    }

    return key + ":\"" + value.toString() + "\"";
  }

  /**
   * Converts {@link RelationshipFilter} to neo4j query criteria, filter criterion condition requires to be EQUAL.
   *
   * @param filter Query relationship filter
   * @return Neo4j criteria string
   */
  @Nonnull
  private static String relationshipFilterToCriteria(@Nonnull RelationshipFilter filter) {
    return disjunctionToCriteria(filter.getOr());
  }

  /**
   * Converts {@link Filter} to neo4j query criteria, filter criterion condition requires to be EQUAL.
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
      throw new UnsupportedOperationException("Neo4j query filter only supports 1 set of conjunction criteria");
    }
    final CriterionArray criterionArray = disjunction.size() > 0 ? disjunction.get(0).getAnd() : new CriterionArray();
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
    if (!criterionArray.stream().allMatch(criterion -> Condition.EQUAL.equals(criterion.getCondition()))) {
      throw new RuntimeException("Neo4j query filter only support EQUAL condition " + criterionArray);
    }

    final StringJoiner joiner = new StringJoiner(",", "{", "}");

    criterionArray.forEach(criterion -> joiner.add(toCriterionString(criterion.getField(), criterion.getValue())));

    return joiner.length() <= 2 ? "" : joiner.toString();
  }

  /**
   * Gets Node based on Urn, if not exist, creates placeholder node.
   */
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
   * Reverse incoming/outgoing direction check by compare sourceType and relationshipType to LineageSpec.
   * for example:
   * sourceType: dataset, relationshipType: downstreamOf.
   * downstreamOf relationship type and outgoing relationship direction for dataset from LineageSpec,
   * is inside upstreamEdges.
   * source(dataset) -[downstreamOf]-> dest means upstreamEdge for source(dataset)
   * dest -[r_downstreamOf]-> source(dataset), need reverse source and dest
   * *
   * sourceType: datajob, relationshipType: produces.
   * produces relationship type and outgoing relationship direction for datajob from LineageSpec,
   * is inside downstreamEdges.
   * source(datajob) -[produces]-> dest means downstreamEdge for source(datajob)
   * source(dataset) -[r_produces]-> dest, do not need to reverse source and dest
   *
   * @param sourceType Entity type
   * @param relationshipType Entity relationship type
   *
   */
  private boolean isSourceDestReversed(@Nonnull String sourceType, @Nonnull String relationshipType) {
    // Get real direction by check INCOMING/OUTGOING direction and RelationshipType
    LineageRegistry.LineageSpec sourceLineageSpec = getLineageRegistry().getLineageSpec(sourceType);
    if (sourceLineageSpec != null) {
      List<LineageRegistry.EdgeInfo> upstreamCheck = sourceLineageSpec.getUpstreamEdges()
          .stream()
          .filter(t -> t.getDirection() == RelationshipDirection.OUTGOING && t.getType().equals(relationshipType))
          .collect(Collectors.toList());
      if (!upstreamCheck.isEmpty() || sourceType.equals("schemaField")) {
        return true;
      }
    }
    return false;
  }

  protected static @Nullable
  Urn createFromString(@Nonnull String rawUrn) {
    try {
      return Urn.createFromString(rawUrn);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  private String generateFullQueryTemplate(@Nonnull String multiHopMatchTemplate, @Nullable Long startTimeMillis, @Nullable Long endTimeMillis) {
    final String sourceUiCheck = String.format("(EXISTS(rt.%s) AND rt.%s = '%s') ", SOURCE, SOURCE, UI);
    final String whereTemplate = "WHERE (b:%s) AND b.urn <> '%s' ";
    final String returnTemplate = "RETURN a,r,b";
    String withTimeTemplate = "";
    String timeFilterConditionTemplate = "AND ALL(rt IN relationships(p) WHERE left(type(rt), 2)='r_')";

    if (startTimeMillis != null && endTimeMillis != null) {
      withTimeTemplate = "WITH %d as startTimeMillis, %d as endTimeMillis ";
      timeFilterConditionTemplate =
          "AND ALL(rt IN relationships(p) WHERE " + sourceUiCheck + "OR "
              + "(NOT EXISTS(rt.createdOn) AND NOT EXISTS(rt.updatedOn)) OR "
              + "((rt.createdOn >= startTimeMillis AND rt.createdOn <= endTimeMillis) OR "
              + "(rt.updatedOn >= startTimeMillis AND rt.updatedOn <= endTimeMillis))) "
              + "AND ALL(rt IN relationships(p) WHERE left(type(rt), 2)='r_')";
    } else if (startTimeMillis != null) {
      withTimeTemplate = "WITH %d as startTimeMillis ";
      timeFilterConditionTemplate =
          "AND ALL(rt IN relationships(p) WHERE " + sourceUiCheck + "OR "
              + "(NOT EXISTS(rt.createdOn) AND NOT EXISTS(rt.updatedOn)) OR "
              + "(rt.createdOn >= startTimeMillis OR rt.updatedOn >= startTimeMillis)) "
              + "AND ALL(rt IN relationships(p) WHERE left(type(rt), 2)='r_')";
    } else if (endTimeMillis != null) {
      withTimeTemplate = "WITH %d as endTimeMillis ";
      timeFilterConditionTemplate =
          "AND ALL(rt IN relationships(p) WHERE " + sourceUiCheck + "OR "
              + "(NOT EXISTS(rt.createdOn) AND NOT EXISTS(rt.updatedOn)) OR "
              + "(rt.createdOn <= endTimeMillis OR rt.updatedOn <= endTimeMillis)) "
              + "AND ALL(rt IN relationships(p) WHERE left(type(rt), 2)='r_')";
    }
    final StringJoiner fullQueryTemplateJoiner = new StringJoiner(" ");
    fullQueryTemplateJoiner.add(withTimeTemplate);
    fullQueryTemplateJoiner.add(multiHopMatchTemplate);
    fullQueryTemplateJoiner.add(whereTemplate);
    fullQueryTemplateJoiner.add(timeFilterConditionTemplate);
    fullQueryTemplateJoiner.add(returnTemplate);

    return fullQueryTemplateJoiner.toString();
  }
}
