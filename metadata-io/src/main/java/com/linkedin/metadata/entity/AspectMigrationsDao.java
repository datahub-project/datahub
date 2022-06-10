package com.linkedin.metadata.entity;

import javax.annotation.Nonnull;

/**
 * This interface is a split-off from {@link AspectDao} to segregate the methods that are only called by data migration
 * tasks. This separation is not technically necessary, but it felt dangerous to leave entire-table queries mixed
 * with the rest.
 */
public interface AspectMigrationsDao {

  /**
   * Return a paged list of _all_ URNs in the database.
   * @param start Start offset of a page.
   * @param pageSize Number of records in a page.
   * @return An iterable of {@code String} URNs.
   */
  @Nonnull
  Iterable<String> listAllUrns(final int start, final int pageSize);

  /**
   * Return a paged list of _all_ Primary Keys in the database
   * @param start Start offset of a page
   * @param pageSize Number of records in a page
   * @return An iterable of {@code EntityAspectIdentifier} primary keys
   */
  @Nonnull
  Iterable<EntityAspectIdentifier> listAllPrimaryKeys(final int start, final int pageSize);

  /**
   * Return the count of entities (unique URNs) in the database.
   * @return Count of entities.
   */
  long countEntities();

  /**
   * Return the count of aspects (all URNs) in the database.
   * @return Count of aspects.
   */
  long countAspects();

  /**
   * Check if any record of given {@param aspectName} exists in the database.
   * @param aspectName Name of an entity aspect to search for.
   * @return {@code true} if at least one record of given {@param aspectName} is found. {@code false} otherwise.
   */
  boolean checkIfAspectExists(@Nonnull final String aspectName);
}
