package com.linkedin.metadata.aspect.plugins.config;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PluginConfiguration {
  private List<AspectPluginConfig> aspectPayloadValidators = Collections.emptyList();
  private List<AspectPluginConfig> mutationHooks = Collections.emptyList();
  private List<AspectPluginConfig> mclSideEffects = Collections.emptyList();
  private List<AspectPluginConfig> mcpSideEffects = Collections.emptyList();

  public static PluginConfiguration EMPTY = new PluginConfiguration();

  public static PluginConfiguration merge(PluginConfiguration a, PluginConfiguration b) {
    return new PluginConfiguration(
        Stream.concat(
                a.getAspectPayloadValidators().stream(), b.getAspectPayloadValidators().stream())
            .collect(Collectors.toList()),
        Stream.concat(a.getMutationHooks().stream(), b.getMutationHooks().stream())
            .collect(Collectors.toList()),
        Stream.concat(a.getMclSideEffects().stream(), b.getMclSideEffects().stream())
            .collect(Collectors.toList()),
        Stream.concat(a.getMcpSideEffects().stream(), b.getMcpSideEffects().stream())
            .collect(Collectors.toList()));
  }
}
