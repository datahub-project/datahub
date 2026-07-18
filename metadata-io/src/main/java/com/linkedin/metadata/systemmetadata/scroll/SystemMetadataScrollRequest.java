package com.linkedin.metadata.systemmetadata.scroll;

import com.linkedin.common.urn.Urn;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Backend-agnostic request for scrolling the system metadata index.
 *
 * <p>Used by {@link com.linkedin.metadata.aspect.consistency.ConsistencyService} to enumerate (urn,
 * aspect) entries without depending on Elasticsearch primitives. Implementations of {@link
 * SystemMetadataScrollClient} translate this request into the underlying storage's native query
 * (Elasticsearch BoolQuery, SQL WHERE clause, etc.).
 *
 * <p>Filter semantics:
 *
 * <ul>
 *   <li>{@code entityType} restricts to URNs starting with {@code urn:li:<entityType>:}.
 *   <li>{@code urns} (when non-empty) further restricts to that exact URN set.
 *   <li>{@code aspects} (when non-empty) restricts to entries whose aspect name is in the set
 *       (OR-semantics).
 *   <li>{@code gePitEpochMs}/{@code lePitEpochMs} apply to {@code aspectModifiedTime}, with
 *       fallback to {@code aspectCreatedTime} when modified is absent.
 *   <li>{@code includeSoftDeleted=false} (default) excludes entries where {@code removed=true}.
 * </ul>
 */
@Value
@Builder
public class SystemMetadataScrollRequest {

  /** Entity type to scope by URN prefix (e.g., {@code "assertion"}). Required. */
  @Nonnull String entityType;

  /** Optional URN whitelist; when non-empty, results are limited to these URNs. */
  @Nullable Set<Urn> urns;

  /** Optional aspect-name filter (OR semantics across listed aspects). */
  @Nullable List<String> aspects;

  /** Inclusive lower bound on aspect modified/created time (epoch ms). */
  @Nullable Long gePitEpochMs;

  /** Inclusive upper bound on aspect modified/created time (epoch ms). */
  @Nullable Long lePitEpochMs;

  /** When false (default), excludes soft-deleted entries (removed=true). */
  boolean includeSoftDeleted;

  /**
   * Pagination cursor returned by a prior {@link SystemMetadataScrollResult}. Null on first call.
   */
  @Nullable String scrollId;

  /** Maximum number of (urn, aspect) entries to read per call. */
  @Builder.Default int batchSize = 100;
}
