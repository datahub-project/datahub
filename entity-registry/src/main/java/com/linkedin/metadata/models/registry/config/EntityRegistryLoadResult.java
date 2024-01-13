package com.linkedin.metadata.models.registry.config;

import java.util.Set;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
public class EntityRegistryLoadResult {
  private LoadStatus loadResult;
  private String registryLocation;
  private String failureReason;
  @Setter private int failureCount;
  private PluginLoadResult plugins;

  @Builder
  @Data
  public static class PluginLoadResult {
    private int validatorCount;
    private int mutationHookCount;
    private int mcpSideEffectCount;
    private int mclSideEffectCount;

    @Builder.Default private Set<String> validatorClasses = Set.of();
    @Builder.Default private Set<String> mutationHookClasses = Set.of();
    @Builder.Default private Set<String> mcpSideEffectClasses = Set.of();
    @Builder.Default private Set<String> mclSideEffectClasses = Set.of();
  }
}
