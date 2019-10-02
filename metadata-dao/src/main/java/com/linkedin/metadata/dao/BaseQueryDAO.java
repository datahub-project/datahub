package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.utils.Statement;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A base class for all Query DAOs.
 *
 * Query DAO is a standardized interface to query the centralized graph DB.
 * See http://go/gma for more details.
 */
public abstract class BaseQueryDAO {

  /**
   * Finds a list of entities of a specific type based on the given filter on the entity
   *
   * @param entityClass the entity class to query
   * @param filter the filter to apply when querying
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @param <ENTITY> returned entity type. Must be a type defined in com.linkedin.metadata.entity.
   * @return a list of entities that match the conditions specified in {@code filter}
   */
  @Nonnull
  public abstract <ENTITY extends RecordTemplate> List<ENTITY> findEntities(@Nonnull Class<ENTITY> entityClass,
      @Nonnull Filter filter, int offset, int count);

  /**
   * Finds a list of entities of a specific type using a raw graph query statement.
   *
   * @param entityClass the entity class to query
   * @param queryStatement a {@link Statement} with query text and parameters
   * @param <ENTITY> returned entity type. Must be a type defined in com.linkedin.metadata.entity.
   * @return a list of entities from the outcome of the query statement
   */
  @Nonnull
  public abstract <ENTITY extends RecordTemplate> List<ENTITY> findEntities(@Nonnull Class<ENTITY> entityClass,
      @Nonnull Statement queryStatement);

  /**
   * Finds a list of entities containing a mixture of different types using a graph query.
   *
   * @param queryStatement a {@link Statement} with query text and parameters
   * @return a list of entities from the outcome of the query statement
   */
  @Nonnull
  public abstract List<RecordTemplate> findMixedTypesEntities(@Nonnull Statement queryStatement);

  /**
   * Finds a list of entities of a specific type based on the given relationship filter and source/destination entity filter.
   *
   * @param sourceEntityClass the source entity class to query
   * @param sourceEntityFilter the filter to apply to the source entity when querying
   * @param destinationEntityClass the destination entity class
   * @param destinationEntityFilter the filter to apply to the destination entity when querying
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   *
   * @param <SRC_ENTITY> source ENTITY type. Must be a type defined in com.linkedin.metadata.entity.
   * @param <DEST_ENTITY> destination ENTITY type. Must be a type defined in com.linkedin.metadata.entity.
   * @param <RELATIONSHIP> returned relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @return a list of entities that match the conditions specified in {@code filter}
   */
  @Nonnull
  public abstract <SRC_ENTITY extends RecordTemplate, DEST_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate>
  List<DEST_ENTITY> findEntities(
      @Nonnull Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nonnull Class<DEST_ENTITY> destinationEntityClass, @Nonnull Filter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull Filter relationshipFilter, int offset, int count);

  /**
   * Finds a list of relationships of a specific type based on the given relationship filter and source entity filter.
   *
   * @param sourceEntityClass the source entity class to query
   * @param sourceEntityFilter the filter to apply to the source entity when querying
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @param <ENTITY> source ENTITY type. Must be a type defined in com.linkedin.metadata.entity.
   * @param <RELATIONSHIP> returned relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @return a list of relationships that match the conditions specified in {@code filter}
   */
  @Nonnull
  public <ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationshipsFromSource(
      @Nullable Class<ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull Filter relationshipFilter, int offset, int count) {
    return findRelationships(sourceEntityClass, sourceEntityFilter, null,
        new Filter().setCriteria(new CriterionArray()), relationshipType, relationshipFilter, offset, count);
  }

  /**
   * Finds a list of relationships of a specific type based on the given relationship filter and destination entity filter.
   *
   * @param destinationEntityClass the destination entity class
   * @param destinationEntityFilter the filter to apply to the destination entity when querying
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @param <ENTITY> destination ENTITY type. Must be a type defined in com.linkedin.metadata.entity.
   * @param <RELATIONSHIP> returned relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @return a list of relationships that match the conditions specified in {@code filter}
   */
  @Nonnull
  public <ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationshipsFromDestination(
      @Nullable Class<ENTITY> destinationEntityClass, @Nonnull Filter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull Filter relationshipFilter, int offset, int count) {
    return findRelationships(null, new Filter().setCriteria(new CriterionArray()), destinationEntityClass,
        destinationEntityFilter, relationshipType, relationshipFilter, offset, count);
  }

  /**
   * Finds a list of relationships of a specific type based on the given relationship filter and source/destination entity filter.
   *
   * @param sourceEntityClass the source entity class to query
   * @param sourceEntityFilter the filter to apply to the source entity when querying
   * @param destinationEntityClass the destination entity class
   * @param destinationEntityFilter the filter to apply to the destination entity when querying
   * @param relationshipType the type of relationship to query
   * @param relationshipFilter the filter to apply to relationship when querying
   * @param offset the offset query should start at. Ignored if set to a negative value.
   * @param count the maximum number of entities to return. Ignored if set to a non-positive value.
   * @param <SRC_ENTITY> source ENTITY type. Must be a type defined in com.linkedin.metadata.entity.
   * @param <DEST_ENTITY> destination ENTITY type. Must be a type defined in com.linkedin.metadata.entity.
   * @param <RELATIONSHIP> returned relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @return a list of relationships that match the conditions specified in {@code filter}
   */
  @Nonnull
  public abstract <SRC_ENTITY extends RecordTemplate, DEST_ENTITY extends RecordTemplate, RELATIONSHIP extends RecordTemplate>
  List<RELATIONSHIP> findRelationships(
      @Nullable Class<SRC_ENTITY> sourceEntityClass, @Nonnull Filter sourceEntityFilter,
      @Nullable Class<DEST_ENTITY> destinationEntityClass, @Nonnull Filter destinationEntityFilter,
      @Nonnull Class<RELATIONSHIP> relationshipType, @Nonnull Filter relationshipFilter, int offset, int count);

  /**
   * Finds a list of relationships of a specific type using a graph query.
   *
   * @param relationshipClass the relationship class to query
   * @param queryStatement a {@link Statement} with query text and parameters
   * @param <RELATIONSHIP> returned relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @return a list of relationships from the outcome of the query statement
   */
  @Nonnull
  public abstract <RELATIONSHIP extends RecordTemplate> List<RELATIONSHIP> findRelationships(
      @Nonnull Class<RELATIONSHIP> relationshipClass, @Nonnull Statement queryStatement);

  /**
   * Finds a list of relationships containing a mixture of different types using a graph query.
   *
   * @param queryStatement a {@link Statement} with query text and parameters
   * @return a list of relationships from the outcome of the query statement
   */
  @Nonnull
  public abstract List<RecordTemplate> findMixedTypesRelationships(@Nonnull Statement queryStatement);
}
