package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.validator.EntityValidator;
import com.linkedin.metadata.validator.RelationshipValidator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static com.linkedin.metadata.dao.Neo4jUtil.*;


/**
 * An Neo4j implementation of {@link BaseGraphWriterDAO}.
 */
public class Neo4jGraphWriterDAO extends BaseGraphWriterDAO {

  private final Driver _driver;

  public Neo4jGraphWriterDAO(@Nonnull Driver driver) {
    this._driver = driver;
  }

  @Override
  public <ENTITY extends RecordTemplate> void addEntities(@Nonnull List<ENTITY> entities) throws Exception {

    entities.forEach(entity -> EntityValidator.validateEntitySchema(entity.getClass()));
    executeStatements(entities.stream().map(this::addNode).collect(Collectors.toList()));
  }

  @Override
  public <URN extends Urn> void removeEntities(@Nonnull List<URN> urns) throws Exception {
    executeStatements(urns.stream().map(this::removeNode).collect(Collectors.toList()));
  }

  @Override
  public <RELATIONSHIP extends RecordTemplate> void addRelationships(@Nonnull List<RELATIONSHIP> relationships,
      @Nonnull RemovalOption removalOption) throws Exception {

    relationships.forEach(relationship -> RelationshipValidator.validateRelationshipSchema(relationship.getClass()));
    executeStatements(addEdges(relationships, removalOption));
  }

  @Override
  public <RELATIONSHIP extends RecordTemplate> void removeRelationships(@Nonnull List<RELATIONSHIP> relationships)
      throws Exception {

    relationships.forEach(relationship -> RelationshipValidator.validateRelationshipSchema(relationship.getClass()));
    executeStatements(relationships.stream().map(this::removeEdge).collect(Collectors.toList()));
  }

  /**
   * Executes a list of statements with parameters in one transaction.
   *
   * @param statements List of statements with parameters to be executed in order
   */
  private void executeStatements(@Nonnull List<Statement> statements) throws Exception {
    try (final Session session = _driver.session()) {
      session.writeTransaction(tx -> {
        statements.forEach(statement -> tx.run(statement.getCommandText(), statement.getParams()));
        return 0;
      });
    }
  }

  /**
   * Run a query statement with parameters and return StatementResult
   *
   * @param statement a statement with parameters to be executed
   */
  @Nonnull
  private StatementResult runQuery(@Nonnull Statement statement) throws Exception {
    try (final Session session = _driver.session()) {
      return session.run(statement.getCommandText(), statement.getParams());
    }
  }

  // used in testing
  @Nonnull
  Optional<Map<String, Object>> getNode(@Nonnull Urn urn) throws Exception {
    List<Map<String, Object>> nodes = getAllNodes(urn);
    if (nodes.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(nodes.get(0));
  }

  // used in testing
  @Nonnull
  List<Map<String, Object>> getAllNodes(@Nonnull Urn urn) throws Exception {
    final String statement = "MATCH (node {urn: $urn}) RETURN node";

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    final StatementResult result = runQuery(buildStatement(statement, params));
    return result.list().stream().map(record -> record.values().get(0).asMap()).collect(Collectors.toList());
  }

  // used in testing
  @Nonnull
  <RELATIONSHIP extends RecordTemplate> List<Map<String, Object>> getEdges(@Nonnull RELATIONSHIP relationship)
      throws Exception {
    final Urn sourceUrn = getUrn(relationship, SOURCE_FIELD);
    final Urn destinationUrn = getUrn(relationship, DESTINATION_FIELD);
    final String type = getType(relationship);

    final String getTemplate = "MATCH (source {urn: $sourceUrn})-[r:%s]->(destination {urn: $destinationUrn}) RETURN r";
    final String statement = String.format(getTemplate, type);

    final Map<String, Object> params = new HashMap<>();
    params.put("sourceUrn", sourceUrn.toString());
    params.put("destinationUrn", destinationUrn.toString());

    final StatementResult result = runQuery(buildStatement(statement, params));
    return result.list().stream().map(record -> record.values().get(0).asMap()).collect(Collectors.toList());
  }

  // used in testing
  @Nonnull
  <RELATIONSHIP extends RecordTemplate> List<Map<String, Object>> getEdgesFromSource(@Nonnull Urn sourceUrn,
      @Nonnull Class<RELATIONSHIP> relationshipClass) throws Exception {
    final String type = getType(relationshipClass);

    final String template = "MATCH (source {urn: $sourceUrn})-[r:%s]->() RETURN r";
    final String statement = String.format(template, type);

    final Map<String, Object> params = new HashMap<>();
    params.put("sourceUrn", sourceUrn.toString());

    final StatementResult result = runQuery(buildStatement(statement, params));
    return result.list().stream().map(record -> record.values().get(0).asMap()).collect(Collectors.toList());
  }

  @Nonnull
  private <ENTITY extends RecordTemplate> Statement addNode(@Nonnull ENTITY entity) {
    final Urn urn = getUrn(entity, URN_FIELD);

    final String mergeTemplate =
        "MERGE (node {urn: $urn}) ON CREATE SET node = $properties ON MATCH SET node = $properties SET node:%s RETURN node";
    final String statement = String.format(mergeTemplate, getType(entity));

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());
    params.put("properties", entityToNode(entity));

    return buildStatement(statement, params);
  }

  @Nonnull
  private <URN extends Urn> Statement removeNode(@Nonnull URN urn) {
    // also delete any relationship going to or from it
    final String statement = "MATCH (node {urn: $urn}) DETACH DELETE node";

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    return buildStatement(statement, params);
  }

  /**
   * Gets Node based on Urn, if not exist, creates placeholder node
   */
  @Nonnull
  private Statement getOrInsertNode(@Nonnull Urn urn) {
    final String statement = "MERGE (node {urn: $urn}) RETURN node";

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    return buildStatement(statement, params);
  }

  @Nonnull
  private <RELATIONSHIP extends RecordTemplate> List<Statement> addEdges(@Nonnull List<RELATIONSHIP> relationships,
      @Nonnull RemovalOption removalOption) {
    // if no relationships, return
    if (relationships.isEmpty()) {
      return Collections.emptyList();
    }

    final List<Statement> statements = new ArrayList<>();

    // remove existing edges according to RemovalOption
    final Urn source0Urn = getUrn(relationships.get(0), SOURCE_FIELD);
    final Urn destination0Urn = getUrn(relationships.get(0), DESTINATION_FIELD);
    final String relationType = getType(relationships.get(0));

    final Map<String, Object> params = new HashMap<>();

    if (removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) {
      checkSameUrn(relationships, SOURCE_FIELD, source0Urn);

      final String removeTemplate = "MATCH (source {urn: $urn})-[relation:%s]->() DELETE relation";
      final String statement = String.format(removeTemplate, relationType);

      params.put("urn", source0Urn.toString());

      statements.add(buildStatement(statement, params));
    } else if (removalOption == RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION) {
      checkSameUrn(relationships, DESTINATION_FIELD, destination0Urn);

      final String removeTemplate = "MATCH ()-[relation:%s]->(destination {urn: $urn}) DELETE relation";
      final String statement = String.format(removeTemplate, relationType);

      params.put("urn", destination0Urn.toString());

      statements.add(buildStatement(statement, params));
    } else if (removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION) {
      checkSameUrn(relationships, SOURCE_FIELD, source0Urn);
      checkSameUrn(relationships, DESTINATION_FIELD, destination0Urn);

      final String removeTemplate =
          "MATCH (source {urn: $sourceUrn})-[relation:%s]->(destination {urn: $destinationUrn}) DELETE relation";
      final String statement = String.format(removeTemplate, relationType);

      params.put("sourceUrn", source0Urn.toString());
      params.put("destinationUrn", destination0Urn.toString());

      statements.add(buildStatement(statement, params));
    }

    // create new edges, TODO: check if exact same (all attributes) edge exists
    final String mergeTemplate =
        "MERGE (source {urn: $sourceUrn}) MERGE (destination {urn: $destinationUrn}) CREATE (source)-[r:%s]->(destination) SET r = $properties";

    relationships.forEach(relationship -> {
      final Urn srcUrn = getUrn(relationship, SOURCE_FIELD);
      final Urn destUrn = getUrn(relationship, DESTINATION_FIELD);

      final String statement = String.format(mergeTemplate, getType(relationship));

      final Map<String, Object> paramsMerge = new HashMap<>();
      paramsMerge.put("sourceUrn", srcUrn.toString());
      paramsMerge.put("destinationUrn", destUrn.toString());
      paramsMerge.put("properties", relationshipToEdge(relationship));

      statements.add(buildStatement(statement, paramsMerge));
    });

    return statements;
  }

  private <T extends RecordTemplate> void checkSameUrn(@Nonnull List<T> records, @Nonnull String field,
      @Nonnull Urn compare) {
    if (!records.stream().allMatch(relation -> compare.equals(getUrn(relation, field)))) {
      throw new IllegalArgumentException("Records have different " + field + " Urn");
    }
  }

  @Nonnull
  private <RELATIONSHIP extends RecordTemplate> Statement removeEdge(@Nonnull RELATIONSHIP relationship) {

    final Urn sourceUrn = getUrn(relationship, SOURCE_FIELD);
    final Urn destinationUrn = getUrn(relationship, DESTINATION_FIELD);

    final String remove =
        "MATCH (source {urn: $sourceUrn})-[relation:%s %s]->(destination {urn: $destinationUrn}) DELETE relation";
    final String criteria = relationshipToCriteria(relationship);
    final String statement = String.format(remove, getType(relationship), criteria);

    final Map<String, Object> params = new HashMap<>();
    params.put("sourceUrn", sourceUrn.toString());
    params.put("destinationUrn", destinationUrn.toString());

    return buildStatement(statement, params);
  }

  @Nonnull
  private Statement buildStatement(@Nonnull String queryTemplate, @Nonnull Map<String, Object> params) {
    params.forEach((k, v) -> params.put(k, toPropertyValue(v)));
    return new Statement(queryTemplate, params);
  }

  @Nonnull
  private Object toPropertyValue(@Nonnull Object obj) {
    if (obj instanceof Urn) {
      return obj.toString();
    }
    return obj;
  }
}
