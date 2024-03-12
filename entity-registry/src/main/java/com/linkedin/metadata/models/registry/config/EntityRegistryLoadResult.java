package com.linkedin.metadata.models.registry.config;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
public class EntityRegistryLoadResult {
  private LoadStatus loadResult;
  private String registryLocation;
  private String failureReason;
  @Setter private int failureCount;
}
