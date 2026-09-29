package com.linkedin.metadata.aspect.consistency.check;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.consistency.SystemMetadataFilter;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Request parameters for batch consistency checks.
 *
 * <p>Encapsulates the configuration for running consistency checks on a batch of entities. Use the
 * builder to construct requests with sensible defaults.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Check all entities of a type
 * CheckBatchRequest request = CheckBatchRequest.builder()
 *     .entityType("assertion")
 *     .batchSize(100)
 *     .build();
 *
 * // Check specific entities (URN filter)
 * CheckBatchRequest request = CheckBatchRequest.builder()
 *     .urns(Set.of(urn1, urn2))
 *     .build();
 * }</pre>
 */
@Value
@Builder
public class CheckBatchRequest {

  /**
   * Entity type to check (e.g., "assertion", "monitor").
   *
   * <p>If not specified, will be derived from urns or checkIds. If both entityType and urns are
   * provided, all URNs must match the specified entityType.
   */
  @Nullable String entityType;

  /**
   * Optional URN filter.
   *
   * <p>When provided, limits the check to these specific URNs. All URNs must be of the same entity
   * type. If entityType is not specified, it will be derived from the URNs.
   */
  @Nullable Set<Urn> urns;

  /**
   * Specific check IDs to run.
   *
   * <p>If provided, all checks must be for the same entity type. If null/empty, runs all default
   * checks for the specified entityType.
   */
  @Nullable List<String> checkIds;

  /** Maximum number of entities to fetch per batch. Defaults to 100. */
  @Builder.Default int batchSize = 100;

  /**
   * Scroll ID for pagination continuation.
   *
   * <p>Pass null for the first batch. Use the scrollId from the previous CheckResult to continue
   * pagination.
   */
  @Nullable String scrollId;

  /**
   * Optional filter configuration for timestamps and aspect filtering.
   *
   * <p>Use to filter entities by modification time (gePitEpochMs, lePitEpochMs) or to include/
   * exclude soft-deleted entities.
   */
  @Nullable SystemMetadataFilter filter;

  /**
   * Whether to include soft-deleted entities in the check.
   *
   * <p>Derived from filter.isIncludeSoftDeleted() if filter is set, otherwise defaults to false.
   */
  public boolean isIncludeSoftDeleted() {
    return filter != null && filter.isIncludeSoftDeleted();
  }
}
