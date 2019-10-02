package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.validator.EntityValidator;
import com.linkedin.metadata.validator.RelationshipValidator;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;

import static com.linkedin.metadata.dao.Neo4jUtil.*;


/**
 * An Neo4j implementation of {@link BaseQueryDAO}.
 */
public class Neo4jQueryDAO extends BaseQueryDAO {

  private final Driver _driver;

  public Neo4jQueryDAO(@Nonnull Driver driver) {
    this._driver = driver;
  }

  @Nonnull
  @Override
  public <ENTITY extends RecordTemplate> List<ENTITY> findEntities(@Nonnull Class<ENTITY> entityClass,
      @Nonnull Filter filter, int offset, int count) {
    EntityValidator.validateEntitySchema(entityClass);

    return findEntities(entityClass, findNodes(entityClass, filter, offset, count));
  }

  @Nonnull
  @Override
  public <ENTITY extends RecordTemplate> List<ENTITY> findEntities(@Nonnull Class<ENTITY> entityClass,
      @Nonnull Statement queryStatement) {
    EntityValidator.validateEntitySchema(entityClass);

    return runQuery(queryStatement).list(record -> nodeRecordToEntity(entityClass, record));
  }

  @Nonnull
  @Override
  public List<RecordTemplate> findMixedTypesEntities(@Nonnull Statement queryStatement) {
    return runQuery(queryStatement).list(this::nodeRecordToEntity);
  }

  @Nonnull
  @Override
  public <SRC_ENTITY extends RecordTemplate, DEST_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<DEST_ENTITY> findEntities(
      @Nonnull Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nonnull Class<DEST_ENTITY> destinationEntityClass, @Nonnull Filter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull Filter relationshipFilter, int offset, int count) {

    EntityValidator.validateEntitySchema(sourceEntityClass);
    EntityValidator.validateEntitySchema(destinationEntityClass);
    RelationshipValidator.validateRelationshipSchema(relationshipType);

    final String srcCriteria = filterToCriteria(sourceEntityFilter);
    final String edgeCriteria = filterToCriteria(relationshipFilter);
    final String destCriteria = filterToCriteria(destinationEntityFilter);

    final String matchTemplate =
        "MATCH (src:%s %s)-[r:%s %s]-(dest:%s %s) RETURN dest";
    final String statementString =
        String.format(matchTemplate, getType(sourceEntityClass), srcCriteria, getType(relationshipType), edgeCriteria,
            getType(destinationEntityClass), destCriteria);

    Statement statement = buildStatement(statementString, "src.urn", offset, count); // TODO: make orderBy an input param

    return runQuery(statement).list(record -> nodeRecordToEntity(destinationEntityClass, record));
  }

  @Nonnull
  @Override
  public <SRC_ENTITY extends RecordTemplate, DEST_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationships(
      @Nullable Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nullable Class<DEST_ENTITY> destinationEntityClass, @Nonnull Filter destinationEnityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull Filter relationshipFilter, int offset, int count) {
    if (sourceEntityClass != null) {
      EntityValidator.validateEntitySchema(sourceEntityClass);
    }
    if (destinationEntityClass != null) {
      EntityValidator.validateEntitySchema(destinationEntityClass);
    }
    RelationshipValidator.validateRelationshipSchema(relationshipType);

    return runQuery(findEdges(sourceEntityClass, sourceEntityFilter, destinationEntityClass, destinationEnityFilter,
        relationshipType, relationshipFilter, offset, count)).list(
        record -> edgeRecordToRelationship(relationshipType, record));
  }

  @Nonnull
  @Override
  public <RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationships(
      @Nonnull Class<RELATIONSHIP> relationshipClass, @Nonnull Statement queryStatement) {
    RelationshipValidator.validateRelationshipSchema(relationshipClass);

    return runQuery(queryStatement).list(record -> edgeRecordToRelationship(relationshipClass, record));
  }

  @Nonnull
  @Override
  public List<RecordTemplate> findMixedTypesRelationships(@Nonnull Statement queryStatement) {
    return runQuery(queryStatement).list(this::edgeRecordToRelationship);
  }

  /**
   * Runs a query statement with parameters and return StatementResult
   *
   * @param statement a statement with parameters to be executed
   */
  @Nonnull
  private StatementResult runQuery(@Nonnull Statement statement) {
    try (final Session session = _driver.session()) {
      return session.run(statement.getCommandText(), statement.getParams());
    }
  }

  @Nonnull
  private <ENTITY extends RecordTemplate> Statement findNodes(@Nonnull Class<ENTITY> entityClass,
      @Nonnull Filter filter, int offset, int count) {
    final String nodeType = getType(entityClass);

    final String findTemplate = "MATCH (node:%s %s) RETURN node";
    final String criteria = filterToCriteria(filter);
    final String statement = String.format(findTemplate, nodeType, criteria);

    return buildStatement(statement, "node.urn", offset, count);
  }

  @Nonnull
  private <SRC_ENTITY extends RecordTemplate, DEST_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate> Statement findEdges(
      @Nullable Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEnityFilter,
      @Nullable Class<DEST_ENTITY> destinationEnityClass, @Nonnull Filter destinationFilter,
      @Nonnull Class<RELATIONSHIP> relationshipClass, @Nonnull Filter relationshipFilter, int offset, int count) {

    final String srcType = sourceEntityClass == null ? "" : ":" + getType(sourceEntityClass);
    final String srcCriteria = filterToCriteria(sourceEnityFilter);
    final String destType = destinationEnityClass == null ? "" : ":" + getType(destinationEnityClass);
    final String destCriteria = filterToCriteria(destinationFilter);
    final String edgeType = getType(relationshipClass);
    final String edgeCriteria = filterToCriteria(relationshipFilter);

    final String findTemplate = "MATCH (src%s %s)-[r:%s %s]->(dest%s %s) RETURN src, r, dest";
    final String statement =
        String.format(findTemplate, srcType, srcCriteria, edgeType, edgeCriteria, destType, destCriteria);

    return buildStatement(statement, "src.urn", offset, count);
  }

  @Nonnull
  private Statement buildStatement(@Nonnull String statement, @Nonnull String orderBy, int offset, int count) {
    if (offset <= 0 && count < 0) {
      return new Statement(statement, Collections.emptyMap());
    }

    String orderStatement = statement + " ORDER BY " + orderBy;

    final Map<String, Object> params = new HashMap<>();
    if (offset > 0) {
      orderStatement += " SKIP $offset";
      params.put("offset", offset);
    }
    if (count >= 0) {
      orderStatement += " LIMIT $count";
      params.put("count", count);
    }

    return new Statement(orderStatement, params);
  }

  @Nonnull
  private <ENTITY extends RecordTemplate> ENTITY nodeRecordToEntity(@Nonnull Class<ENTITY> entityClass,
      @Nonnull Record nodeRecord) {
    return nodeToEntity(entityClass, nodeRecord.values().get(0).asNode());
  }

  @Nonnull
  private RecordTemplate nodeRecordToEntity(@Nonnull Record nodeRecord) {
    return nodeToEntity(nodeRecord.values().get(0).asNode());
  }

  @Nonnull
  private <RELATIONSHIP extends RecordTemplate> RELATIONSHIP edgeRecordToRelationship(
      @Nonnull Class<RELATIONSHIP> relationshipClass, @Nonnull Record edgeRecord) {
    final List<Value> values = edgeRecord.values();
    return edgeToRelationship(relationshipClass, values.get(0).asNode(), values.get(2).asNode(),
        values.get(1).asRelationship());
  }

  @Nonnull
  private RecordTemplate edgeRecordToRelationship(@Nonnull Record edgeRecord) {
    final List<Value> values = edgeRecord.values();
    return edgeToRelationship(values.get(0).asNode(), values.get(2).asNode(), values.get(1).asRelationship());
  }
}
