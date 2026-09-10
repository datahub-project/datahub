package com.linkedin.metadata.config.cache;

import lombok.Data;

@Data
public class KeyAspectEntityCountCacheConfiguration {
  boolean enabled;
  long ttlSeconds;
  int maxEntityTypes;
  KeyAspectEntityCountSingleFlightConfiguration singleFlight;
}
