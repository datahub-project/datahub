package com.linkedin.metadata.aspect.plugins;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.aspect.plugins.hooks.MCLSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.models.registry.config.EntityRegistryLoadResult;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.MethodInfo;
import io.github.classgraph.ScanResult;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PluginFactory {

  private static final String[] VALIDATOR_PACKAGES = {
    "com.linkedin.metadata.aspect.plugins.validation", "com.linkedin.metadata.aspect.validation"
  };
  private static final String[] HOOK_PACKAGES = {
    "com.linkedin.metadata.aspect.plugins.hooks", "com.linkedin.metadata.aspect.hooks"
  };

  public static PluginFactory withCustomClasspath(
      @Nullable PluginConfiguration pluginConfiguration, @Nonnull List<ClassLoader> classLoaders) {
    return new PluginFactory(pluginConfiguration, classLoaders);
  }

  public static PluginFactory withConfig(@Nullable PluginConfiguration pluginConfiguration) {
    return PluginFactory.withCustomClasspath(pluginConfiguration, Collections.emptyList());
  }

  public static PluginFactory empty() {
    return PluginFactory.withConfig(PluginConfiguration.EMPTY);
  }

  public static PluginFactory merge(PluginFactory a, PluginFactory b) {
    return PluginFactory.withCustomClasspath(
        PluginConfiguration.merge(a.getPluginConfiguration(), b.getPluginConfiguration()),
        Stream.concat(a.getClassLoaders().stream(), b.getClassLoaders().stream())
            .collect(Collectors.toList()));
  }

  @Getter private final PluginConfiguration pluginConfiguration;
  @Nonnull @Getter private final List<ClassLoader> classLoaders;
  @Getter private final List<AspectPayloadValidator> aspectPayloadValidators;
  @Getter private final List<MutationHook> mutationHooks;
  @Getter private final List<MCLSideEffect> mclSideEffects;
  @Getter private final List<MCPSideEffect> mcpSideEffects;

  private final ClassGraph classGraph;

  public PluginFactory(
      @Nullable PluginConfiguration pluginConfiguration, @Nonnull List<ClassLoader> classLoaders) {
    this.classGraph =
        new ClassGraph()
            .enableRemoteJarScanning()
            .enableExternalClasses()
            .enableClassInfo()
            .enableMethodInfo();

    this.classLoaders = classLoaders;

    if (!this.classLoaders.isEmpty()) {
      classLoaders.forEach(this.classGraph::addClassLoader);
    }

    this.pluginConfiguration =
        pluginConfiguration == null ? PluginConfiguration.EMPTY : pluginConfiguration;
    this.aspectPayloadValidators = buildAspectPayloadValidators(this.pluginConfiguration);
    this.mutationHooks = buildMutationHooks(this.pluginConfiguration);
    this.mclSideEffects = buildMCLSideEffects(this.pluginConfiguration);
    this.mcpSideEffects = buildMCPSideEffects(this.pluginConfiguration);
  }

  /**
   * Returns applicable {@link AspectPayloadValidator} implementations given the change type and
   * entity/aspect information.
   *
   * @param changeType The type of change to be validated
   * @param entityName The entity name
   * @param aspectName The aspect name
   * @return List of validator implementations
   */
  @Nonnull
  public List<AspectPayloadValidator> getAspectPayloadValidators(
      @Nonnull ChangeType changeType, @Nonnull String entityName, @Nonnull String aspectName) {
    return aspectPayloadValidators.stream()
        .filter(plugin -> plugin.shouldApply(changeType, entityName, aspectName))
        .collect(Collectors.toList());
  }

  /**
   * Return mutation hooks for {@link com.linkedin.data.template.RecordTemplate}
   *
   * @param changeType The type of change
   * @param entityName The entity name
   * @param aspectName The aspect name
   * @return Mutation hooks
   */
  @Nonnull
  public List<MutationHook> getMutationHooks(
      @Nonnull ChangeType changeType, @Nonnull String entityName, @Nonnull String aspectName) {
    return mutationHooks.stream()
        .filter(plugin -> plugin.shouldApply(changeType, entityName, aspectName))
        .collect(Collectors.toList());
  }

  /**
   * Returns the side effects to apply to {@link com.linkedin.mxe.MetadataChangeProposal}. Side
   * effects can generate one or more additional MCPs during write operations.
   *
   * @param changeType The type of change
   * @param entityName The entity name
   * @param aspectName The aspect name
   * @return MCP side effects
   */
  @Nonnull
  public List<MCPSideEffect> getMCPSideEffects(
      @Nonnull ChangeType changeType, @Nonnull String entityName, @Nonnull String aspectName) {
    return mcpSideEffects.stream()
        .filter(plugin -> plugin.shouldApply(changeType, entityName, aspectName))
        .collect(Collectors.toList());
  }

  /**
   * Returns the side effects to apply to {@link com.linkedin.mxe.MetadataChangeLog}. Side effects
   * can generate one or more additional MCLs during write operations.
   *
   * @param changeType The type of change
   * @param entityName The entity name
   * @param aspectName The aspect name
   * @return MCL side effects
   */
  @Nonnull
  public List<MCLSideEffect> getMCLSideEffects(
      @Nonnull ChangeType changeType, @Nonnull String entityName, @Nonnull String aspectName) {
    return mclSideEffects.stream()
        .filter(plugin -> plugin.shouldApply(changeType, entityName, aspectName))
        .collect(Collectors.toList());
  }

  @Nonnull
  public EntityRegistryLoadResult.PluginLoadResult getPluginLoadResult() {
    return EntityRegistryLoadResult.PluginLoadResult.builder()
        .validatorCount(aspectPayloadValidators.size())
        .mutationHookCount(mutationHooks.size())
        .mcpSideEffectCount(mcpSideEffects.size())
        .mclSideEffectCount(mclSideEffects.size())
        .validatorClasses(
            aspectPayloadValidators.stream()
                .map(cls -> cls.getClass().getName())
                .collect(Collectors.toSet()))
        .mutationHookClasses(
            mutationHooks.stream().map(cls -> cls.getClass().getName()).collect(Collectors.toSet()))
        .mcpSideEffectClasses(
            mcpSideEffects.stream()
                .map(cls -> cls.getClass().getName())
                .collect(Collectors.toSet()))
        .mclSideEffectClasses(
            mclSideEffects.stream()
                .map(cls -> cls.getClass().getName())
                .collect(Collectors.toSet()))
        .build();
  }

  private List<AspectPayloadValidator> buildAspectPayloadValidators(
      @Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? Collections.emptyList()
        : applyDisable(
            build(
                AspectPayloadValidator.class,
                pluginConfiguration.getAspectPayloadValidators(),
                VALIDATOR_PACKAGES));
  }

  private List<MutationHook> buildMutationHooks(@Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? Collections.emptyList()
        : applyDisable(
            build(MutationHook.class, pluginConfiguration.getMutationHooks(), HOOK_PACKAGES));
  }

  private List<MCLSideEffect> buildMCLSideEffects(
      @Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? Collections.emptyList()
        : applyDisable(
            build(MCLSideEffect.class, pluginConfiguration.getMclSideEffects(), HOOK_PACKAGES));
  }

  private List<MCPSideEffect> buildMCPSideEffects(
      @Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? Collections.emptyList()
        : applyDisable(
            build(MCPSideEffect.class, pluginConfiguration.getMcpSideEffects(), HOOK_PACKAGES));
  }

  private <T> List<T> build(
      Class<?> baseClazz, List<AspectPluginConfig> configs, String... packageNames) {
    try (ScanResult scanResult = classGraph.acceptPackages(packageNames).scan()) {

      Map<String, ClassInfo> classMap =
          scanResult.getSubclasses(baseClazz).stream()
              .collect(Collectors.toMap(ClassInfo::getName, Function.identity()));

      return configs.stream()
          .flatMap(
              config -> {
                try {
                  ClassInfo classInfo = classMap.get(config.getClassName());
                  if (classInfo == null) {
                    throw new IllegalStateException(
                        String.format(
                            "The following class cannot be loaded: %s", config.getClassName()));
                  }
                  MethodInfo constructorMethod = classInfo.getConstructorInfo().get(0);
                  return Stream.of(
                      (T) constructorMethod.loadClassAndGetConstructor().newInstance(config));
                } catch (Exception e) {
                  log.error(
                      "Error constructing entity registry plugin class: {}",
                      config.getClassName(),
                      e);
                  return Stream.empty();
                }
              })
          .collect(Collectors.toList());

    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Failed to load entity registry plugins: %s.", baseClazz.getName()), e);
    }
  }

  @Nonnull
  private static <T extends PluginSpec> List<T> applyDisable(@Nonnull List<T> plugins) {
    return IntStream.range(0, plugins.size())
        .mapToObj(
            idx -> {
              List<T> subsequentPlugins = plugins.subList(idx + 1, plugins.size());
              T thisPlugin = plugins.get(idx);
              AspectPluginConfig thisPluginConfig = thisPlugin.getConfig();

              if (subsequentPlugins.stream()
                  .anyMatch(
                      otherPlugin -> thisPluginConfig.isDisabledBy(otherPlugin.getConfig()))) {
                return null;
              }

              return thisPlugin;
            })
        .filter(Objects::nonNull)
        .filter(p -> p.getConfig().isEnabled())
        .collect(Collectors.toList());
  }
}
