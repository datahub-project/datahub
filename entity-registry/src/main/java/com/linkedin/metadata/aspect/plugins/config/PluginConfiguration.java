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
  private static final List<String> VALIDATOR_PACKAGES =
      List.of(
          "com.linkedin.metadata.aspect.plugins.validation",
          "com.linkedin.metadata.aspect.validation");
  private static final List<String> HOOK_PACKAGES =
      List.of("com.linkedin.metadata.aspect.plugins.hooks", "com.linkedin.metadata.aspect.hooks");

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

  public Stream<AspectPluginConfig> streamAll() {
    return Stream.concat(
        Stream.concat(
            Stream.concat(aspectPayloadValidators.stream(), mutationHooks.stream()),
            mclSideEffects.stream()),
        mcpSideEffects.stream());
  }

  public List<String> validatorPackages() {
    return aspectPayloadValidators.stream()
        .flatMap(
            cfg ->
                cfg.getPackageScan() != null
                    ? cfg.getPackageScan().stream()
                    : VALIDATOR_PACKAGES.stream())
        .distinct()
        .collect(Collectors.toList());
  }

  public List<String> mcpSideEffectPackages() {
    return mcpSideEffects.stream()
        .flatMap(
            cfg ->
                cfg.getPackageScan() != null
                    ? cfg.getPackageScan().stream()
                    : HOOK_PACKAGES.stream())
        .distinct()
        .collect(Collectors.toList());
  }

  public List<String> mclSideEffectPackages() {
    return mclSideEffects.stream()
        .flatMap(
            cfg ->
                cfg.getPackageScan() != null
                    ? cfg.getPackageScan().stream()
                    : HOOK_PACKAGES.stream())
        .distinct()
        .collect(Collectors.toList());
  }

  public List<String> mutationPackages() {
    return mutationHooks.stream()
        .flatMap(
            cfg ->
                cfg.getPackageScan() != null
                    ? cfg.getPackageScan().stream()
                    : HOOK_PACKAGES.stream())
        .distinct()
        .collect(Collectors.toList());
  }
}
