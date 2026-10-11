package com.linkedin.metadata.systemmetadata;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor
public class KeyAspectEntityCountResult implements Serializable {
  private static final long serialVersionUID = 1L;

  @Nonnull List<KeyAspectEntityCountEntry> counts;
  @Nonnull List<String> requestedTypes;
  @Nonnull Instant computedAt;
  boolean cacheHit;

  public long activeTotal() {
    return counts.stream().mapToLong(KeyAspectEntityCountEntry::getActiveCount).sum();
  }

  public long softDeletedTotal() {
    return counts.stream().mapToLong(KeyAspectEntityCountEntry::getSoftDeletedCount).sum();
  }

  public long totalCount() {
    return counts.stream().mapToLong(KeyAspectEntityCountEntry::totalCount).sum();
  }
}
