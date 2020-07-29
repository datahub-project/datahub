package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import com.linkedin.metadata.validator.EntityValidator;
import com.linkedin.metadata.validator.RelationshipValidator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.javatuples.Triplet;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Path;

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

    return runQuery(queryStatement, record -> nodeRecordToEntity(entityClass, record));
  }

  @Nonnull
  @Override
  public List<RecordTemplate> findMixedTypesEntities(@Nonnull Statement queryStatement) {
    return runQuery(queryStatement, this::nodeRecordToEntity);
  }

  @Nonnull
  @Override
  public <SRC_ENTITY extends RecordTemplate, DEST_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RecordTemplate> findEntities(
      @Nullable Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nullable Class<DEST_ENTITY> destinationEntityClass, @Nonnull Filter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull RelationshipFilter relationshipFilter, int minHops,
      int maxHops, int offset, int count) {

    if (sourceEntityClass != null) {
      EntityValidator.validateEntitySchema(sourceEntityClass);
    }
    if (destinationEntityClass != null) {
      EntityValidator.validateEntitySchema(destinationEntityClass);
    }
    RelationshipValidator.validateRelationshipSchema(relationshipType);

    final String srcType = getTypeOrEmptyString(sourceEntityClass);
    final String srcCriteria = filterToCriteria(sourceEntityFilter);
    final String destType = getTypeOrEmptyString(destinationEntityClass);
    final String destCriteria = filterToCriteria(destinationEntityFilter);
    final String edgeType = getType(relationshipType);
    final String edgeCriteria = criterionToString(relationshipFilter.getCriteria());

    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();

    String matchTemplate = "MATCH (src%s %s)-[r:%s*%d..%d %s]-(dest%s %s) RETURN dest";
    if (relationshipDirection == RelationshipDirection.INCOMING) {
      matchTemplate = "MATCH (src%s %s)<-[r:%s*%d..%d %s]-(dest%s %s) RETURN dest";
    } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
      matchTemplate = "MATCH (src%s %s)-[r:%s*%d..%d %s]->(dest%s %s) RETURN dest";
    }

    final String statementString =
        String.format(matchTemplate, srcType, srcCriteria, edgeType, minHops, maxHops, edgeCriteria, destType,
            destCriteria);

    final Statement statement = buildStatement(statementString, offset, count);

    return runQuery(statement, this::nodeRecordToEntity);
  }

  @Nonnull
  @Override
  public <SRC_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate, INTER_ENTITY extends RecordTemplate> List<RecordTemplate> findEntities(
      @Nullable Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nonnull List<Triplet<Class<RELATIONSHIP>, RelationshipFilter, Class<INTER_ENTITY>>> traversePaths, int offset,
      int count) {
    if (sourceEntityClass != null) {
      EntityValidator.validateEntitySchema(sourceEntityClass);
    }

    final String srcType = getTypeOrEmptyString(sourceEntityClass);
    final String srcCriteria = filterToCriteria(sourceEntityFilter);

    StringBuilder matchTemplate = new StringBuilder().append("MATCH (src%s %s)");
    int pathCounter = 0;

    // for each triplet, construct substatement via relationship type + relationship filer + inter entity
    for (Triplet<Class<RELATIONSHIP>, RelationshipFilter, Class<INTER_ENTITY>> path : traversePaths) {

      pathCounter++; // Cannot use the same relationship variable 'r' or 'dest' for multiple patterns

      final String edgeType = getTypeOrEmptyString(path.getValue0());
      final String edgeCriteria = path.getValue1() == null ? "" : criterionToString(path.getValue1().getCriteria());
      final RelationshipDirection relationshipDirection =
          path.getValue1() == null ? RelationshipDirection.UNDIRECTED : path.getValue1().getDirection();
      final String destType = getTypeOrEmptyString(path.getValue2());

      String subTemplate = "-[r%d%s %s]-(dest%d%s)";
      if (relationshipDirection == RelationshipDirection.INCOMING) {
        subTemplate = "<-[r%d%s %s]-(dest%d%s)";
      } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
        subTemplate = "-[r%d%s %s]->(dest%d%s)";
      }
      String subStatementString =
          String.format(subTemplate, pathCounter, edgeType, edgeCriteria, pathCounter, destType);

      matchTemplate.append(subStatementString);
    }

    // last INTER_ENTITY will be the Destination Entity
    String lastEntity = String.format("dest%d", pathCounter);
    matchTemplate.append("RETURN ").append(lastEntity);

    final String statementString = String.format(matchTemplate.toString(), srcType, srcCriteria);
    final Statement statement = buildStatement(statementString, offset, count);

    return runQuery(statement, this::nodeRecordToEntity);
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
        relationshipType, relationshipFilter, offset, count), record -> edgeRecordToRelationship(relationshipType, record));
  }

  @Nonnull
  @Override
  public <RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationships(
      @Nonnull Class<RELATIONSHIP> relationshipClass, @Nonnull Statement queryStatement) {
    RelationshipValidator.validateRelationshipSchema(relationshipClass);

    return runQuery(queryStatement, record -> edgeRecordToRelationship(relationshipClass, record));
  }

  @Nonnull
  @Override
  public List<RecordTemplate> findMixedTypesRelationships(@Nonnull Statement queryStatement) {
    return runQuery(queryStatement, this::edgeRecordToRelationship);
  }

  @Nonnull
  public <SRC_ENTITY extends RecordTemplate, DEST_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate>
  List<List<RecordTemplate>> getTraversedPaths(
      @Nullable Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nullable Class<DEST_ENTITY> destinationEntityClass, @Nonnull Filter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull RelationshipFilter relationshipFilter,
      int minHops, int maxHops, int offset, int count) {

    if (sourceEntityClass != null) {
      EntityValidator.validateEntitySchema(sourceEntityClass);
    }
    if (destinationEntityClass != null) {
      EntityValidator.validateEntitySchema(destinationEntityClass);
    }
    RelationshipValidator.validateRelationshipSchema(relationshipType);

    final String srcType = getTypeOrEmptyString(sourceEntityClass);
    final String srcCriteria = filterToCriteria(sourceEntityFilter);
    final String destType = getTypeOrEmptyString(destinationEntityClass);
    final String destCriteria = filterToCriteria(destinationEntityFilter);
    final String edgeType = getType(relationshipType);
    final String edgeCriteria = criterionToString(relationshipFilter.getCriteria());

    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();

    String matchTemplate = "MATCH p=(src%s %s)-[r:%s*%d..%d %s]-(dest%s %s) RETURN p";
    if (relationshipDirection == RelationshipDirection.INCOMING) {
      matchTemplate = "MATCH p=(src%s %s)<-[r:%s*%d..%d %s]-(dest%s %s) RETURN p";
    } else if (relationshipDirection == RelationshipDirection.OUTGOING) {
      matchTemplate = "MATCH p=(src%s %s)-[r:%s*%d..%d %s]->(dest%s %s) RETURN p";
    }

    final String statementString =
        String.format(matchTemplate, srcType, srcCriteria, edgeType, minHops, maxHops, edgeCriteria, destType,
            destCriteria);

    final Statement statement = buildStatement(statementString, "length(p), dest.urn", offset, count);

    return runQuery(statement, this::pathRecordToPathList);
  }

  /**
   * Runs a query statement with parameters and return StatementResult
   *
   * @param statement a statement with parameters to be executed
   * @param mapperFunction lambda to transform query result
   * @return List<T> list of elements in the query result
   */
  @Nonnull
  private <T> List<T> runQuery(@Nonnull Statement statement, @Nonnull Function<Record, T> mapperFunction) {
    try (final Session session = _driver.session()) {
      return session.run(statement.getCommandText(), statement.getParams()).list(mapperFunction);
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
  private Statement buildStatement(@Nonnull String statement, int offset, int count) {
    return buildStatement(statement, null, offset, count);
  }

  @Nonnull
  private Statement buildStatement(@Nonnull String statement, @Nullable String orderBy, int offset, int count) {
    String orderStatement = statement;
    if (orderBy != null) {
      orderStatement += " ORDER BY " + orderBy;
    }

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
  private List<RecordTemplate> pathRecordToPathList(@Nonnull Record pathRecord) {
    final Path path = pathRecord.values().get(0).asPath();
    final List<RecordTemplate> pathList = new ArrayList<>();

    StreamSupport.stream(path.spliterator(), false)
        .map(Neo4jUtil::pathSegmentToRecordList)
        .forEach(segment -> {
          if (pathList.isEmpty()) {
            pathList.add(segment.get(0));
          }
          pathList.add(segment.get(1));
          pathList.add(segment.get(2));
        });

    return pathList;
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
