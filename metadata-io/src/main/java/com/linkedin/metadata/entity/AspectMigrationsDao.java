package com.linkedin.metadata.entity;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * This interface is a split-off from {@link AspectDao} to segregate the methods that are only
 * called by data migration tasks. This separation is not technically necessary, but it felt
 * dangerous to leave entire-table queries mixed with the rest.
 */
public interface AspectMigrationsDao {

  /**
   * Return a paged list of _all_ URNs in the database.
   *
   * @param operationContext
   * @param start Start offset of a page.
   * @param pageSize Number of records in a page.
   * @return An iterable of {@code String} URNs.
   */
  @Nonnull
  Iterable<String> listAllUrns(
      OperationContext operationContext, final int start, final int pageSize);

  /**
   * Return the count of entities (unique URNs) in the database.
   *
   * @return Count of entities.
   */
  long countEntities(OperationContext operationContext);

  /**
   * Check if any record of given {@param aspectName} exists in the database.
   *
   * @param operationContext
   * @param aspectName Name of an entity aspect to search for.
   * @return {@code true} if at least one record of given {@param aspectName} is found. {@code
   *     false} otherwise.
   */
  boolean checkIfAspectExists(OperationContext operationContext, @Nonnull final String aspectName);
}
