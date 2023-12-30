package com.linkedin.metadata.aspect.plugins.config;

import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PluginConfiguration {
  private List<AspectPluginConfig> aspectPayloadValidators = List.of();
  private List<AspectPluginConfig> mutationHooks = List.of();
  private List<AspectPluginConfig> mclSideEffects = List.of();
  private List<AspectPluginConfig> mcpSideEffects = List.of();

  public static PluginConfiguration EMPTY = new PluginConfiguration();
}
