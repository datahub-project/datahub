package com.linkedin.metadata.aspect.consistency.check;

import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

/**
 * Result of a consistency check batch operation.
 *
 * <p>Contains the issues found and scroll ID for pagination. Use {@link #hasMore()} to check if
 * there are more entities to process.
 */
@Data
@Builder
public class CheckResult {

  /** Number of entities scanned in this batch */
  private final int entitiesScanned;

  /** Number of issues found in this batch */
  private final int issuesFound;

  /** Issues found in this batch */
  @Nonnull private final List<ConsistencyIssue> issues;

  /** Scroll ID for pagination continuation. Null when no more results. */
  @Nullable private final String scrollId;

  /**
   * Check if there are more entities to process.
   *
   * @return true if scrollId exists (more data available)
   */
  public boolean hasMore() {
    return scrollId != null;
  }

  /** Create an empty result with no issues */
  public static CheckResult empty() {
    return CheckResult.builder().entitiesScanned(0).issuesFound(0).issues(List.of()).build();
  }
}
