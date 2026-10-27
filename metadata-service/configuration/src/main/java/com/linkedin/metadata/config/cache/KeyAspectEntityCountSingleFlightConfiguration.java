package com.linkedin.metadata.config.cache;

import lombok.Data;

@Data
public class KeyAspectEntityCountSingleFlightConfiguration {
  long staleBuildingMillis;
  long waiterMaxMillis;
  long pollIntervalMillis;
}
