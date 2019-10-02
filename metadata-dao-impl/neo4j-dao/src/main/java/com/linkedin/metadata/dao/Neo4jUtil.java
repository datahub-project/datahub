package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Filter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.ClassUtils;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;


public class Neo4jUtil {

  public static final String URN_FIELD = "urn";
  public static final String SOURCE_FIELD = "source";
  public static final String DESTINATION_FIELD = "destination";

  private Neo4jUtil() {
    // Util class
  }

  /**
   * Converts ENTITY to node (field:value map)
   *
   * @param entity ENTITY defined in models
   * @return unmodifiable field value map
   */
  @Nonnull
  public static <ENTITY extends RecordTemplate> Map<String, Object> entityToNode(@Nonnull ENTITY entity) {
    final Map<String, Object> fields = new HashMap<>();

    // put all field values
    entity.data().forEach((k, v) -> fields.put(k, toValueObject(v)));

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Converts RELATIONSHIP to edge (field:value map), excluding source and destination
   *
   * @param relationship RELATIONSHIP defined in models
   * @return unmodifiable field value map
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> Map<String, Object> relationshipToEdge(
      @Nonnull RELATIONSHIP relationship) {
    final Map<String, Object> fields = new HashMap<>();

    // put all field values except source and destination
    relationship.data().forEach((k, v) -> {
      if (!SOURCE_FIELD.equals(k) && !DESTINATION_FIELD.equals(k)) {
        fields.put(k, toValueObject(v));
      }
    });

    return Collections.unmodifiableMap(fields);
  }

  /**
   * Converts RELATIONSHIP to cypher matching criteria, excluding source and destination, e.g. {key: "value"}
   *
   * @param relationship RELATIONSHIP defined in models
   * @return Criteria String, or "" if no additional fields in relationship
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> String relationshipToCriteria(
      @Nonnull RELATIONSHIP relationship) {
    final StringJoiner joiner = new StringJoiner(",", "{", "}");

    // put all field values except source and destination
    relationship.data().forEach((k, v) -> {
      if (!SOURCE_FIELD.equals(k) && !DESTINATION_FIELD.equals(k)) {
        joiner.add(toCriterionString(k, v));
      }
    });

    return joiner.length() <= 2 ? "" : joiner.toString();
  }

  // Returns self if primitive type, otherwise, return toString()
  @Nonnull
  private static Object toValueObject(@Nonnull Object obj) {
    if (ClassUtils.isPrimitiveOrWrapper(obj.getClass())) {
      return obj;
    }

    return obj.toString();
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
   * Converts {@link Filter} to neo4j query criteria, filter criterion condition requires to be EQUAL
   *
   * @param filter Query Filter
   * @return Neo4j criteria string
   */
  @Nonnull
  public static String filterToCriteria(@Nonnull Filter filter) {
    if (!filter.getCriteria().stream().allMatch(criterion -> Condition.EQUAL.equals(criterion.getCondition()))) {
      throw new RuntimeException("Neo4j query filter only support EQUAL condition " + filter);
    }

    final StringJoiner joiner = new StringJoiner(",", "{", "}");

    filter.getCriteria()
        .forEach(criterion -> joiner.add(toCriterionString(criterion.getField(), criterion.getValue())));

    return joiner.length() <= 2 ? "" : joiner.toString();
  }

  /**
   * Converts node (field:value map) to ENTITY
   *
   * @param entityClass Class of Entity
   * @param node Neo4j Node of entityClass type
   * @return ENTITY
   */
  @Nonnull
  public static <ENTITY extends RecordTemplate> ENTITY nodeToEntity(@Nonnull Class<ENTITY> entityClass,
      @Nonnull Node node) {
    return RecordUtils.toRecordTemplate(entityClass, new DataMap(node.asMap()));
  }

  /**
   * Converts node (field:value map) to ENTITY RecordTemplate
   *
   * @param node Neo4j Node of entityClass type
   * @return RecordTemplate
   */
  @Nonnull
  public static RecordTemplate nodeToEntity(@Nonnull Node node) {

    final String className = node.labels().iterator().next();
    return RecordUtils.toRecordTemplate(className, new DataMap(node.asMap()));
  }

  /**
   * Converts edge (source-relationship->destination) to RELATIONSHIP
   *
   * @param relationshipClass Class of RELATIONSHIP
   * @param source Neo4j source Node
   * @param destination Neo4j destination Node
   * @param relationship Neo4j relationship
   * @return ENTITY
   */
  @Nonnull
  public static <RELATIONSHIP extends RecordTemplate> RELATIONSHIP edgeToRelationship(
      @Nonnull Class<RELATIONSHIP> relationshipClass, @Nonnull Node source, @Nonnull Node destination,
      @Nonnull Relationship relationship) {

    final DataMap dataMap = relationshipDataMap(source, destination, relationship);
    return RecordUtils.toRecordTemplate(relationshipClass, dataMap);
  }

  /**
   * Converts edge (source-relationship->destination) to RELATIONSHIP RecordTemplate
   *
   * @param source Neo4j source Node
   * @param destination Neo4j destination Node
   * @param relationship Neo4j relationship
   * @return ENTITY RecordTemplate
   */
  @Nonnull
  public static RecordTemplate edgeToRelationship(@Nonnull Node source, @Nonnull Node destination,
      @Nonnull Relationship relationship) {

    final String className = relationship.type();
    final DataMap dataMap = relationshipDataMap(source, destination, relationship);
    return RecordUtils.toRecordTemplate(className, dataMap);
  }

  @Nonnull
  private static DataMap relationshipDataMap(@Nonnull Node source, @Nonnull Node destination,
      @Nonnull Relationship relationship) {

    final DataMap dataMap = new DataMap(relationship.asMap());
    dataMap.put(SOURCE_FIELD, source.get(URN_FIELD).asString());
    dataMap.put(DESTINATION_FIELD, destination.get(URN_FIELD).asString());
    return dataMap;
  }

  /**
   * Extracts Urn field from a record
   *
   * @param record extends RecordTemplate
   * @param fieldName urn field name in record
   * @return Urn
   */
  @Nonnull
  public static <T extends RecordTemplate> Urn getUrn(@Nonnull T record, @Nonnull String fieldName) {
    return getUrn(record.data().getString(fieldName));
  }

  @Nonnull
  private static Urn getUrn(@Nonnull String urn) {
    try {
      return Urn.createFromString(urn);
    } catch (Exception ex) {
      throw new ModelConversionException("Unable to deserialize URN " + urn, ex);
    }
  }

  // Gets the Node/Edge type from an Entity/Relationship, using the backtick-quoted FQCN
  @Nonnull
  public static String getType(@Nonnull RecordTemplate record) {
    return getType(record.getClass());
  }

  // Gets the Node/Edge type from an Entity/Relationship class, using the backtick-quoted FQCN
  @Nonnull
  public static String getType(@Nonnull Class<? extends RecordTemplate> recordClass) {
    return new StringBuilder("`").append(recordClass.getCanonicalName()).append("`").toString();
  }
}
