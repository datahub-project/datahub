package com.linkedin.metadata.systemmetadata;

import java.time.Instant;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PlatformEntityCountResult {
  @Nonnull List<PlatformEntityCountEntry> counts;
  @Nonnull List<String> requestedTypes;
  @Nonnull Instant computedAt;
}
