package com.linkedin.metadata.messaging;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class LagMetricsSnapshot {
  long maxLag;
  long medianLag;
  long totalLag;
  long avgLag;
}
