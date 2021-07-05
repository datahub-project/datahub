package com.linkedin.metadata.graph;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;

@Slf4j
public class Neo4jGraphService implements GraphService {

  private static final int MAX_TRANSACTION_RETRY = 3;
  private final Driver _driver;
  private SessionConfig _sessionConfig;

  public Neo4jGraphService(@Nonnull Driver driver) {
    this(driver, SessionConfig.defaultConfig());
  }

  public Neo4jGraphService(@Nonnull Driver driver, @Nonnull SessionConfig sessionConfig) {
    this._driver = driver;
    this._sessionConfig = sessionConfig;
  }

  public void addEdge(@Nonnull final Edge edge) {

    log.debug(String.format("Adding Edge source: %s, destination: %s, type: %s",
        edge.getSource(),
        edge.getDestination(),
        edge.getRelationshipType()));

    final String sourceType = edge.getSource().getEntityType();
    final String destinationType = edge.getDestination().getEntityType();

    final List<Statement> statements = new ArrayList<>();

    // Add/Update source & destination node first
    statements.add(getOrInsertNode(edge.getSource()));
    statements.add(getOrInsertNode(edge.getDestination()));

    // Add/Update relationship
    final String mergeRelationshipTemplate =
        "MATCH (source:%s {urn: $sourceUrn}),(destination:%s {urn: $destinationUrn}) MERGE (source)-[r:%s]->(destination) SET r = $properties";
    final String statement =
        String.format(mergeRelationshipTemplate, sourceType, destinationType, edge.getRelationshipType());

    final Map<String, Object> paramsMerge = new HashMap<>();
    paramsMerge.put("sourceUrn", edge.getSource().toString());
    paramsMerge.put("destinationUrn", edge.getDestination().toString());
    paramsMerge.put("properties", new HashMap<>());

    statements.add(buildStatement(statement, paramsMerge));

    executeStatements(statements);
  }

  @Nonnull
  public List<String> findRelatedUrns(
      @Nullable final String sourceType,
      @Nonnull final Filter sourceEntityFilter,
      @Nullable final String destinationType,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter,
      final int offset,
      final int count) {

    log.debug(
        String.format("Finding related Neo4j nodes sourceType: %s, sourceEntityFilter: %s, destinationType: %s, ",
            sourceType, sourceEntityFilter, destinationType)
        + String.format(
        "destinationEntityFilter: %s, relationshipTypes: %s, relationshipFilter: %s, ",
            destinationEntityFilter, relationshipTypes, relationshipFilter)
        + String.format(
            "offset: %s, count: %s",
            offset, count)
    );

    final String srcCriteria = filterToCriteria(sourceEntityFilter);
    final String destCriteria = filterToCriteria(destinationEntityFilter);
    final String edgeCriteria = criterionToString(relationshipFilter.getCriteria());

    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();

    String matchTemplate = "MATCH (src%s %s)-[r%s %s]-(dest%s %s) RETURN dest";
    if (relationshipDirection == RelationshipDirection.INCOMING) {
      matchTemplate = "MATCH (src%s %s)<-[r%s %s]-(dest%s %s) RETURN dest";
    } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
      matchTemplate = "MATCH (src%s %s)-[r%s %s]->(dest%s %s) RETURN dest";
    }

    String relationshipTypeFilter = "";
    if (relationshipTypes.size() > 0) {
      relationshipTypeFilter = ":" + StringUtils.join(relationshipTypes, "|");
    }

    String statementString =
        String.format(matchTemplate, sourceType, srcCriteria, relationshipTypeFilter, edgeCriteria,
            destinationType, destCriteria);

    statementString += " SKIP $offset LIMIT $count";

    final Statement statement = new Statement(statementString, ImmutableMap.of("offset", offset, "count", count));

    return runQuery(statement).list(record -> record.values().get(0).asNode().get("urn").asString());
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

    String matchTemplate = "MATCH (src {urn: $urn})-[r%s]-(dest) DELETE r";
    if (relationshipDirection == RelationshipDirection.INCOMING) {
      matchTemplate = "MATCH (src {urn: $urn})<-[r%s]-(dest) DELETE r";
    } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
      matchTemplate = "MATCH (src {urn: $urn})-[r%s]->(dest) DELETE r";
    }

    String relationshipTypeFilter = "";
    if (relationshipTypes.size() > 0) {
      relationshipTypeFilter = ":" + StringUtils.join(relationshipTypes, "|");
    }
    final String statement = String.format(matchTemplate, relationshipTypeFilter);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    runQuery(buildStatement(statement, params)).consume();
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
    return _driver.session(_sessionConfig).run(statement.getCommandText(), statement.getParams());
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
   * Converts {@link Filter} to neo4j query criteria, filter criterion condition requires to be EQUAL.
   *
   * @param filter Query Filter
   * @return Neo4j criteria string
   */
  @Nonnull
  private static String filterToCriteria(@Nonnull Filter filter) {
    return criterionToString(filter.getCriteria());
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
}
