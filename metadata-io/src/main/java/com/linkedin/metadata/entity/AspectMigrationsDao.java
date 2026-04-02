package com.linkedin.metadata.entity;

import java.util.Set;
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
   * @param start Start offset of a page.
   * @param pageSize Number of records in a page.
   * @return An iterable of {@code String} URNs.
   */
  @Nonnull
  Iterable<String> listAllUrns(final int start, final int pageSize);

  /**
   * Return the count of entities (unique URNs) in the database.
   *
   * @return Count of entities.
   */
  long countEntities();

  /**
   * Check if any record of given {@param aspectName} exists in the database.
   *
   * @param aspectName Name of an entity aspect to search for.
   * @return {@code true} if at least one record of given {@param aspectName} is found. {@code
   *     false} otherwise.
   */
  boolean checkIfAspectExists(@Nonnull final String aspectName);

  /**
   * Returns {@code true} if any latest-version (v0) row for the given aspect exists whose stored
   * {@code schemaVersion} matches one of {@code sourceVersions}. An absent or {@code null} {@code
   * schemaVersion} in the DB row is treated as {@code DEFAULT_SCHEMA_VERSION} (1).
   *
   * <p>Used by migration upgrade steps to skip a full table scan when all data is already at the
   * current schema version.
   *
   * @param aspectName name of the aspect to check
   * @param sourceVersions set of schema versions that indicate a row needs migration
   * @return {@code true} if at least one row needs migration, {@code false} otherwise
   */
  boolean hasAspectsNeedingMigration(@Nonnull String aspectName, @Nonnull Set<Long> sourceVersions);
}
