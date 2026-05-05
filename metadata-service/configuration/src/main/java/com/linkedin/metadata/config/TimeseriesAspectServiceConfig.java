package com.linkedin.metadata.config;

import com.linkedin.metadata.config.shared.LimitConfig;
import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class TimeseriesAspectServiceConfig {
  @Builder.Default private ExecutorServiceConfig query = ExecutorServiceConfig.builder().build();
  private LimitConfig limit;
  @Builder.Default private CacheConfig cache = CacheConfig.builder().build();

  @Data
  @Builder(toBuilder = true)
  @AllArgsConstructor
  @NoArgsConstructor
  public static class CacheConfig {
    @Builder.Default private boolean enabled = false;
    @Builder.Default private int ttlHours = 48;
    @Builder.Default private int ttlJitterMinutes = 60;
    @Builder.Default private int maxSize = 100000;
    @Builder.Default private Set<String> cachedAspects = new HashSet<>();
  }
}
