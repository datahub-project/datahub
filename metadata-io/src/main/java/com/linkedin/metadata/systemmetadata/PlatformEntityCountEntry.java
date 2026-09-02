package com.linkedin.metadata.systemmetadata;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** One (entityType, platform) inventory row from the entity search index. */
@Value
@Builder
public class PlatformEntityCountEntry {
  @Nonnull String entityType;

  /** Short platform id (e.g. {@code snowflake}) or {@link PlatformEntityCounts#NO_PLATFORM}. */
  @Nonnull String platform;

  long activeCount;
  long softDeletedCount;

  public long totalCount() {
    return activeCount + softDeletedCount;
  }
}
