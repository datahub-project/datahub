package com.linkedin.metadata.config.ratelimit;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CapacityLimitConfig {
  @Builder.Default private boolean enabled = true;
  @Builder.Default private int initialLimit = 200;
  @Builder.Default private int minLimit = 20;
  @Builder.Default private int maxLimit = 5000;
}
