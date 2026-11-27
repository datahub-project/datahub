package com.linkedin.metadata.entity;

import java.util.Arrays;
import java.util.Locale;
import javax.annotation.Nonnull;

/**
 * What a hard delete that runs in this process does when more graph edges point at the entity than
 * {@code hardDelete.graph-reference-cleanup-limit} lets one request remove.
 */
public enum GraphReferenceOverLimit {
  /**
   * Delete the entity without removing the graph references to it, record a warning and a metric,
   * and still run the cleanup of the references that are not graph edges. The key delete drops the
   * graph edges, so the entities that held them keep the URN in their aspects.
   */
  SKIP,
  /** Remove nothing and fail the delete with a {@link GraphReferenceLimitExceededException}. */
  REFUSE;

  /**
   * Reads the configured value, ignoring case and surrounding whitespace. Spring binds enums
   * case-sensitively, so the setting is read as a string and parsed here.
   *
   * @throws IllegalArgumentException when the value names neither mode
   */
  @Nonnull
  public static GraphReferenceOverLimit parse(@Nonnull final String value) {
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "hardDelete.graph-reference-over-limit must be one of "
              + Arrays.toString(values())
              + ", was '"
              + value
              + "'",
          e);
    }
  }
}
