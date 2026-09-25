package com.linkedin.metadata.systemmetadata;

import java.io.Serializable;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class KeyAspectCount implements Serializable {
  private static final long serialVersionUID = 1L;

  long activeCount;
  long softDeletedCount;

  @Nonnull
  public static KeyAspectCount empty() {
    return new KeyAspectCount(0L, 0L);
  }

  public long totalCount() {
    return activeCount + softDeletedCount;
  }
}
