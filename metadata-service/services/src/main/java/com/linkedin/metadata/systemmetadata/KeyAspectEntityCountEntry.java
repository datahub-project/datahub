package com.linkedin.metadata.systemmetadata;

import java.io.Serializable;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class KeyAspectEntityCountEntry implements Serializable {
  private static final long serialVersionUID = 1L;

  @Nonnull String entityType;
  @Nonnull String keyAspect;
  long activeCount;
  long softDeletedCount;

  @Nonnull
  public static KeyAspectEntityCountEntry of(
      @Nonnull String entityType, @Nonnull String keyAspect, @Nonnull KeyAspectCount count) {
    return KeyAspectEntityCountEntry.builder()
        .entityType(entityType)
        .keyAspect(keyAspect)
        .activeCount(count.getActiveCount())
        .softDeletedCount(count.getSoftDeletedCount())
        .build();
  }

  public long totalCount() {
    return activeCount + softDeletedCount;
  }
}
