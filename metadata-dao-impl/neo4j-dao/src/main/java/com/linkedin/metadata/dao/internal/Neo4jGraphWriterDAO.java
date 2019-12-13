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
    final String matchTemplate = "MATCH (node%s {urn: $urn}) RETURN node";

    final String sourceType = getNodeType(urn);
    final String statement = String.format(matchTemplate, sourceType);

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
    final String relationshipType = getType(relationship);

    final String sourceType = getNodeType(sourceUrn);
    final String destinationType = getNodeType(destinationUrn);

    final String matchTemplate =
        "MATCH (source%s {urn: $sourceUrn})-[r:%s]->(destination%s {urn: $destinationUrn}) RETURN r";
    final String statement = String.format(matchTemplate, sourceType, relationshipType, destinationType);

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
    final String relationshipType = getType(relationshipClass);
    final String sourceType = getNodeType(sourceUrn);

    final String matchTemplate = "MATCH (source%s {urn: $sourceUrn})-[r:%s]->() RETURN r";
    final String statement = String.format(matchTemplate, sourceType, relationshipType);

    final Map<String, Object> params = new HashMap<>();
    params.put("sourceUrn", sourceUrn.toString());

    final StatementResult result = runQuery(buildStatement(statement, params));
    return result.list().stream().map(record -> record.values().get(0).asMap()).collect(Collectors.toList());
  }

  @Nonnull
  private <ENTITY extends RecordTemplate> Statement addNode(@Nonnull ENTITY entity) {
    final Urn urn = getUrn(entity, URN_FIELD);
    final String nodeType = getNodeType(urn);

    final String mergeTemplate = "MERGE (node%s {urn: $urn}) ON CREATE SET node%s SET node = $properties RETURN node";
    final String statement = String.format(mergeTemplate, nodeType, nodeType);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());
    params.put("properties", entityToNode(entity));

    return buildStatement(statement, params);
  }

  @Nonnull
  private <URN extends Urn> Statement removeNode(@Nonnull URN urn) {
    // also delete any relationship going to or from it
    final String nodeType = getNodeType(urn);

    final String matchTemplate = "MATCH (node%s {urn: $urn}) DETACH DELETE node";
    final String statement = String.format(matchTemplate, nodeType);

    final Map<String, Object> params = new HashMap<>();
    params.put("urn", urn.toString());

    return buildStatement(statement, params);
  }

  /**
   * Gets Node based on Urn, if not exist, creates placeholder node
   */
  @Nonnull
  private Statement getOrInsertNode(@Nonnull Urn urn) {
    final String nodeType = getNodeType(urn);

    final String mergeTemplate = "MERGE (node%s {urn: $urn}) RETURN node";
    final String statement = String.format(mergeTemplate, nodeType);

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

    final String sourceType = getNodeType(source0Urn);
    final String destinationType = getNodeType(destination0Urn);

    final Map<String, Object> params = new HashMap<>();

    if (removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE) {
      checkSameUrn(relationships, SOURCE_FIELD, source0Urn);

      final String removeTemplate = "MATCH (source%s {urn: $urn})-[relation:%s]->() DELETE relation";
      final String statement = String.format(removeTemplate, sourceType, relationType);

      params.put("urn", source0Urn.toString());

      statements.add(buildStatement(statement, params));
    } else if (removalOption == RemovalOption.REMOVE_ALL_EDGES_TO_DESTINATION) {
      checkSameUrn(relationships, DESTINATION_FIELD, destination0Urn);

      final String removeTemplate = "MATCH ()-[relation:%s]->(destination%s {urn: $urn}) DELETE relation";
      final String statement = String.format(removeTemplate, relationType, destinationType);

      params.put("urn", destination0Urn.toString());

      statements.add(buildStatement(statement, params));
    } else if (removalOption == RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION) {
      checkSameUrn(relationships, SOURCE_FIELD, source0Urn);
      checkSameUrn(relationships, DESTINATION_FIELD, destination0Urn);

      final String removeTemplate =
          "MATCH (source%s {urn: $sourceUrn})-[relation:%s]->(destination%s {urn: $destinationUrn}) DELETE relation";
      final String statement = String.format(removeTemplate, sourceType, relationType, destinationType);

      params.put("sourceUrn", source0Urn.toString());
      params.put("destinationUrn", destination0Urn.toString());

      statements.add(buildStatement(statement, params));
    }

    relationships.forEach(relationship -> {
      final Urn srcUrn = getUrn(relationship, SOURCE_FIELD);
      final Urn destUrn = getUrn(relationship, DESTINATION_FIELD);
      final String sourceNodeType = getNodeType(srcUrn);
      final String destinationNodeType = getNodeType(destUrn);

      // Add/Update source & destination node first
      statements.add(getOrInsertNode(srcUrn));
      statements.add(getOrInsertNode(destUrn));

      // Add/Update relationship
      final String mergeRelationshipTemplate =
          "MATCH (source%s {urn: $sourceUrn}),(destination%s {urn: $destinationUrn}) MERGE (source)-[r:%s]->(destination) SET r = $properties";
      final String statement =
          String.format(mergeRelationshipTemplate, sourceNodeType, destinationNodeType, getType(relationship));

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

    final String sourceType = getNodeType(sourceUrn);
    final String destinationType = getNodeType(destinationUrn);

    final String removeMatchTemplate =
        "MATCH (source%s {urn: $sourceUrn})-[relation:%s %s]->(destination%s {urn: $destinationUrn}) DELETE relation";
    final String criteria = relationshipToCriteria(relationship);
    final String statement =
        String.format(removeMatchTemplate, sourceType, getType(relationship), criteria, destinationType);

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
