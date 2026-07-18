package com.linkedin.metadata.systemmetadata.scroll;

import com.linkedin.common.urn.Urn;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Backend-agnostic batch result from {@link SystemMetadataScrollClient}.
 *
 * <p>Contains the unique URNs found in the current page plus an opaque continuation token. The
 * token format is implementation-specific and only meaningful when passed back into the same
 * implementation that produced it.
 */
@Value
@Builder
public class SystemMetadataScrollResult {

  /** Unique URNs found in this page (deduplicated across (urn, aspect) rows). */
  @Nonnull Set<Urn> urns;

  /**
   * Continuation token to pass into the next scroll call, or {@code null} when the scan is
   * exhausted.
   */
  @Nullable String nextScrollId;

  /** Empty result with no URNs and no continuation. */
  public static SystemMetadataScrollResult empty() {
    return SystemMetadataScrollResult.builder().urns(Set.of()).nextScrollId(null).build();
  }

  /** Convenience: true when there are more pages to read. */
  public boolean hasMore() {
    return nextScrollId != null;
  }
}
