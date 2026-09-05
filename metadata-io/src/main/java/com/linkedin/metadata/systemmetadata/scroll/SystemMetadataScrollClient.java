package com.linkedin.metadata.systemmetadata.scroll;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Backend-agnostic interface for paged enumeration of the system metadata index.
 *
 * <p>Allows {@link com.linkedin.metadata.aspect.consistency.ConsistencyService} to walk the (urn,
 * aspect) catalog without knowing whether the underlying store is Elasticsearch or PostgreSQL.
 * Implementations are responsible for translating {@link SystemMetadataScrollRequest} into the
 * native query language and encoding/decoding their own continuation tokens.
 *
 * <p>Concrete implementations:
 *
 * <ul>
 *   <li>{@link ESSystemMetadataScrollClient} — wraps {@link
 *       com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO}
 *   <li>{@link PostgresSystemMetadataScrollClient} — reads the pgSystemMetadata table with keyset
 *       pagination on {@code (urn, aspect)}
 * </ul>
 */
public interface SystemMetadataScrollClient {

  /**
   * Read the next page of unique URNs matching the request.
   *
   * @param opContext operation context
   * @param request filter + pagination configuration
   * @return URNs in this page plus a continuation token (null when exhausted)
   */
  @Nonnull
  SystemMetadataScrollResult scrollUrns(
      @Nonnull OperationContext opContext, @Nonnull SystemMetadataScrollRequest request);

  /**
   * Count matching system-metadata documents (same filters as {@link #scrollUrns}, ignoring
   * pagination). Empty when the store cannot answer (soft-fail).
   */
  @Nonnull
  Optional<Long> countUrns(
      @Nonnull OperationContext opContext, @Nonnull SystemMetadataScrollRequest request);
}
