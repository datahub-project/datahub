package com.linkedin.metadata.dao.internal;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * A base class for all Graph Writer DAOs.
 *
 * Graph Writer DAO is a standardized interface to update a centralized graph DB.
 */
public abstract class BaseGraphWriterDAO {

  public enum RemovalOption {
    REMOVE_NONE,
    REMOVE_ALL_EDGES_FROM_SOURCE,
    REMOVE_ALL_EDGES_TO_DESTINATION,
    REMOVE_ALL_EDGES_FROM_SOURCE_TO_DESTINATION
  }

  /**
   * Adds or Updates an entity in the graph.
   *
   * @param entity the entity to be persisted
   * @param <ENTITY> entity type. Must be a type defined in com.linkedin.metadata.entity.
   * @throws Exception
   */
  public <ENTITY extends RecordTemplate> void addEntity(@Nonnull ENTITY entity) throws Exception {
    addEntities(Collections.singletonList(entity));
  }

  /**
   * Adds or Updates a batch of entities in the graph.
   *
   * @param entities the list of entities to be persisted
   * @param <ENTITY> entity type. Must be a type defined in com.linkedin.metadata.entity.
   * @throws Exception
   */
  public abstract <ENTITY extends RecordTemplate> void addEntities(@Nonnull List<ENTITY> entities) throws Exception;

  /**
   * Deletes an entity in the graph.
   *
   * @param urn the URN of the entity to be removed
   * @param <URN> URN type of the entity
   * @throws Exception
   */
  public <URN extends Urn> void removeEntity(@Nonnull URN urn) throws Exception {
    removeEntities(Collections.singletonList(urn));
  }

  /**
   * Deletes a batch of entities in the graph.
   *
   * @param urns the URNs of the entities to be removed
   * @param <URN> URN type of the entity
   * @throws Exception
   */
  public abstract <URN extends Urn> void removeEntities(@Nonnull List<URN> urns) throws Exception;

  /**
   * Adds a relationship in the graph.
   *
   * @param relationship the relationship to be persisted
   * @param <RELATIONSHIP> relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @throws Exception
   */
  public <RELATIONSHIP extends RecordTemplate> void addRelationship(@Nonnull RELATIONSHIP relationship)
      throws Exception {
    addRelationship(relationship, RemovalOption.REMOVE_NONE);
  }

  /**
   * Adds a relationship in the graph, with removal operations before adding
   *
   * @param relationship the relationship to be persisted
   * @param removalOption whether to remove existing relationship of the same type
   * @param <RELATIONSHIP> relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @throws Exception
   */
  public <RELATIONSHIP extends RecordTemplate> void addRelationship(@Nonnull RELATIONSHIP relationship,
      @Nonnull RemovalOption removalOption) throws Exception {
    addRelationships(Collections.singletonList(relationship), removalOption);
  }

  /**
   * Adds a batch of relationships in the graph.
   *
   * @param relationships the list of relationships to be persisted
   * @param <RELATIONSHIP> relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @throws Exception
   */
  public <RELATIONSHIP extends RecordTemplate> void addRelationships(@Nonnull List<RELATIONSHIP> relationships)
      throws Exception {
    addRelationships(relationships, RemovalOption.REMOVE_NONE);
  }

  /**
   * Adds a batch of relationships in the graph, with removal operations before adding
   *
   * @param relationships the list of relationships to be persisted
   * @param removalOption whether to remove existing relationship of the same type
   * @param <RELATIONSHIP> relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @throws Exception
   */
  public abstract <RELATIONSHIP extends RecordTemplate> void addRelationships(@Nonnull List<RELATIONSHIP> relationships,
      @Nonnull RemovalOption removalOption) throws Exception;

  /**
   * Deletes an relationship in the graph.
   *
   * @param relationship the relationship to be removed
   * @param <RELATIONSHIP> relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @throws Exception
   */
  public <RELATIONSHIP extends RecordTemplate> void removeRelationship(@Nonnull RELATIONSHIP relationship)
      throws Exception {
    removeRelationships(Collections.singletonList(relationship));
  }

  /**
   * Deletes a batch of relationships in the graph.
   *
   * @param relationships the list of relationships to be removed
   * @param <RELATIONSHIP> relationship type. Must be a type defined in com.linkedin.metadata.relationship.
   * @throws Exception
   */
  public abstract <RELATIONSHIP extends RecordTemplate> void removeRelationships(
      @Nonnull List<RELATIONSHIP> relationships) throws Exception;
}
