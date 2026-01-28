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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
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

  public static PluginFactory withCustomClasspath(
      @Nullable PluginConfiguration pluginConfiguration, @Nonnull List<ClassLoader> classLoaders) {
    return new PluginFactory(pluginConfiguration, classLoaders).loadPlugins();
  }

  public static PluginFactory withConfig(@Nullable PluginConfiguration pluginConfiguration) {
    return PluginFactory.withCustomClasspath(pluginConfiguration, Collections.emptyList());
  }

  public static PluginFactory empty() {
    return PluginFactory.withConfig(PluginConfiguration.EMPTY);
  }

  /**
   * Merges two PluginFactory instances, combining their plugins and configurations.
   *
   * <p><strong>Merge Strategy:</strong>
   *
   * <ul>
   *   <li><strong>Empty handling:</strong> If either factory is empty, returns the non-empty one
   *   <li><strong>Plugin inclusion:</strong> Includes all plugins from factory A first, then all
   *       plugins from factory B
   *   <li><strong>Duplicate resolution:</strong> Factory A plugins are filtered out only if
   *       disabled by factory B configurations
   *   <li><strong>Configuration merging:</strong> Combines PluginConfiguration objects from both
   *       factories
   * </ul>
   *
   * <p><strong>Plugin Inclusion Logic:</strong>
   *
   * <p>The merge process follows this sequence:
   *
   * <ol>
   *   <li>Include all plugins from factory A, except those that are disabled by factory B's
   *       configurations
   *   <li>Add all plugins from factory B
   * </ol>
   *
   * <p><strong>Disabling Logic:</strong>
   *
   * <p>A plugin from factory A is filtered out (disabled) if any configuration in factory B would
   * disable it. A plugin A is considered "disabled by" a configuration B when:
   *
   * <ol>
   *   <li>Plugin A is currently enabled
   *   <li>Plugin A and configuration B have identical settings except for the enabled flag
   *   <li>Configuration B has enabled=false
   * </ol>
   *
   * <p>This allows factory B to selectively "turn off" specific plugins from factory A by providing
   * matching disabled configurations, while still adding its own plugins.
   *
   * <p>The {@code isDisabledBy()} method in {@code AspectPluginConfig} compares all configuration
   * fields except {@code enabled} (className, packageScan, supportedOperations,
   * supportedEntityAspectNames, spring) and returns {@code true} when the current plugin is
   * enabled, the other configuration is disabled, and all other fields match.
   *
   * <p><strong>Examples:</strong>
   *
   * <pre>{@code
   * // Basic merge - no conflicts
   * PluginFactory factoryA = createFactory(enabledValidatorA);
   * PluginFactory factoryB = createFactory(enabledValidatorB);
   * PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);
   * // Result: [validatorA, validatorB]
   *
   * // Selective disabling - B disables specific plugin from A
   * PluginFactory factoryA = createFactory(enabledValidator, anotherEnabledValidator);
   * PluginFactory factoryB = createFactory(disabledValidator, newEnabledValidator); // first config matches A's validator but disabled
   * PluginFactory merged = PluginFactory.merge(factoryA, factoryB, null);
   * // Result: [anotherEnabledValidator, disabledValidator, newEnabledValidator] (A's enabledValidator filtered out)
   * }</pre>
   *
   * <p><strong>Unit Test Coverage:</strong>
   *
   * <p>The merge behavior is comprehensively tested by {@code PluginFactoryMergeTest} which
   * validates: empty factory handling, non-overlapping plugin merging, duplicate resolution and
   * disabling logic, plugin ordering preservation, and configuration merging across all plugin
   * types.
   *
   * @param a The first plugin factory (plugins included first in merge result)
   * @param b The second plugin factory (plugins included second, can disable plugins from A)
   * @param pluginFactoryProvider Optional factory provider for creating new instances when neither
   *     factory has loaded plugins yet
   * @return A new PluginFactory containing the merged plugins and configurations
   * @see AspectPluginConfig#isDisabledBy(AspectPluginConfig)
   * @see PluginConfiguration#merge(PluginConfiguration, PluginConfiguration)
   */
  public static PluginFactory merge(
      PluginFactory a,
      PluginFactory b,
      @Nullable
          BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider) {

    if (b.isEmpty()) {
      return a;
    }
    if (a.isEmpty()) {
      return b;
    }

    PluginConfiguration mergedPluginConfig =
        PluginConfiguration.merge(a.pluginConfiguration, b.pluginConfiguration);
    List<ClassLoader> mergedClassLoaders =
        Stream.concat(a.getClassLoaders().stream(), b.getClassLoaders().stream())
            .collect(Collectors.toList());

    if (!a.hasLoadedPlugins() && !b.hasLoadedPlugins()) {
      if (pluginFactoryProvider != null) {
        return pluginFactoryProvider.apply(mergedPluginConfig, mergedClassLoaders);
      } else {
        if (mergedPluginConfig
            .streamAll()
            .anyMatch(config -> config.getSpring() != null && config.getSpring().isEnabled())) {
          throw new IllegalStateException(
              "Unexpected Spring configuration found without a provided Spring Plugin Factory");
        }
        return PluginFactory.withCustomClasspath(mergedPluginConfig, mergedClassLoaders);
      }
    }

    PluginFactory loadedA = a.hasLoadedPlugins() ? a : a.loadPlugins();
    PluginFactory loadedB = b.hasLoadedPlugins() ? b : b.loadPlugins();

    return new PluginFactory(
        mergedPluginConfig,
        mergedClassLoaders,
        Stream.concat(
                loadedA.aspectPayloadValidators.stream()
                    .filter(
                        aPlugin ->
                            loadedB.pluginConfiguration.getAspectPayloadValidators().stream()
                                .noneMatch(bConfig -> aPlugin.getConfig().isDisabledBy(bConfig))),
                loadedB.aspectPayloadValidators.stream())
            .collect(Collectors.toList()),
        Stream.concat(
                loadedA.mutationHooks.stream()
                    .filter(
                        aPlugin ->
                            loadedB.pluginConfiguration.getMutationHooks().stream()
                                .noneMatch(bConfig -> aPlugin.getConfig().isDisabledBy(bConfig))),
                loadedB.mutationHooks.stream())
            .collect(Collectors.toList()),
        Stream.concat(
                loadedA.mclSideEffects.stream()
                    .filter(
                        aPlugin ->
                            loadedB.pluginConfiguration.getMclSideEffects().stream()
                                .noneMatch(bConfig -> aPlugin.getConfig().isDisabledBy(bConfig))),
                loadedB.mclSideEffects.stream())
            .collect(Collectors.toList()),
        Stream.concat(
                loadedA.mcpSideEffects.stream()
                    .filter(
                        aPlugin ->
                            loadedB.pluginConfiguration.getMcpSideEffects().stream()
                                .noneMatch(bConfig -> aPlugin.getConfig().isDisabledBy(bConfig))),
                loadedB.mcpSideEffects.stream())
            .collect(Collectors.toList()));
  }

  @Getter private final PluginConfiguration pluginConfiguration;
  @Nonnull @Getter private final List<ClassLoader> classLoaders;
  @Getter private List<AspectPayloadValidator> aspectPayloadValidators;
  @Getter private List<MutationHook> mutationHooks;
  @Getter private List<MCLSideEffect> mclSideEffects;
  @Getter private List<MCPSideEffect> mcpSideEffects;

  private static final Map<Long, List<PluginSpec>> pluginCache = new ConcurrentHashMap<>();

  public PluginFactory(
      @Nullable PluginConfiguration pluginConfiguration, @Nonnull List<ClassLoader> classLoaders) {
    this.classLoaders = classLoaders;
    this.pluginConfiguration =
        pluginConfiguration == null ? PluginConfiguration.EMPTY : pluginConfiguration;
  }

  public PluginFactory(
      @Nullable PluginConfiguration pluginConfiguration,
      @Nonnull List<ClassLoader> classLoaders,
      @Nonnull List<AspectPayloadValidator> aspectPayloadValidators,
      @Nonnull List<MutationHook> mutationHooks,
      @Nonnull List<MCLSideEffect> mclSideEffects,
      @Nonnull List<MCPSideEffect> mcpSideEffects) {
    this.classLoaders = classLoaders;
    this.pluginConfiguration =
        pluginConfiguration == null ? PluginConfiguration.EMPTY : pluginConfiguration;
    this.aspectPayloadValidators = applyDisable(aspectPayloadValidators);
    this.mutationHooks = applyDisable(mutationHooks);
    this.mclSideEffects = applyDisable(mclSideEffects);
    this.mcpSideEffects = applyDisable(mcpSideEffects);
  }

  public PluginFactory loadPlugins() {
    if (this.aspectPayloadValidators != null
        || this.mutationHooks != null
        || this.mclSideEffects != null
        || this.mcpSideEffects != null) {
      log.error("Plugins are already loaded. Re-building plugins will be skipped.");
    } else {
      this.aspectPayloadValidators = buildAspectPayloadValidators(this.pluginConfiguration);
      this.mutationHooks = buildMutationHooks(this.pluginConfiguration);
      this.mclSideEffects = buildMCLSideEffects(this.pluginConfiguration);
      this.mcpSideEffects = buildMCPSideEffects(this.pluginConfiguration);
      logSummary(
          Stream.of(
                  this.aspectPayloadValidators,
                  this.mutationHooks,
                  this.mclSideEffects,
                  this.mcpSideEffects)
              .flatMap(List::stream)
              .collect(Collectors.toList()));
    }
    return this;
  }

  public boolean isEmpty() {
    return this.pluginConfiguration.isEmpty()
        && Optional.ofNullable(this.aspectPayloadValidators).map(List::isEmpty).orElse(true)
        && Optional.ofNullable(this.mutationHooks).map(List::isEmpty).orElse(true)
        && Optional.ofNullable(this.mclSideEffects).map(List::isEmpty).orElse(true)
        && Optional.ofNullable(this.mcpSideEffects).map(List::isEmpty).orElse(true);
  }

  public boolean hasLoadedPlugins() {
    return Stream.of(
            this.aspectPayloadValidators,
            this.mutationHooks,
            this.mcpSideEffects,
            this.mcpSideEffects)
        .anyMatch(Objects::nonNull);
  }

  private void logSummary(List<PluginSpec> pluginSpecs) {
    if (!pluginSpecs.isEmpty()) {
      log.info(
          "Enabled {} plugins. {}",
          pluginSpecs.size(),
          pluginSpecs.stream()
              .map(
                  v ->
                      String.join(
                          ", ",
                          Collections.singletonList(
                              String.format("%s", v.getConfig().getClassName()))))
              .sorted()
              .collect(Collectors.toList()));
    }
  }

  /**
   * Memory intensive operation because of the size of the jars. Limit packages, classes scanned,
   * cache results
   *
   * @param configs plugin configurations
   * @return auto-closeable scan result
   */
  protected static <T extends PluginSpec> List<T> initPlugins(
      @Nonnull List<ClassLoader> classLoaders,
      @Nonnull Class<?> baseClazz,
      @Nonnull List<String> packageNames,
      @Nonnull List<AspectPluginConfig> configs) {

    List<String> classNames =
        configs.stream().map(AspectPluginConfig::getClassName).collect(Collectors.toList());

    if (classNames.isEmpty()) {
      return Collections.emptyList();
    } else {
      long key =
          IntStream.concat(
                  classLoaders.stream().mapToInt(Object::hashCode),
                  IntStream.concat(
                      IntStream.of(baseClazz.getName().hashCode()),
                      configs.stream().mapToInt(AspectPluginConfig::hashCode)))
              .sum();

      return (List<T>)
          pluginCache.computeIfAbsent(
              key,
              k -> {
                try {
                  ClassGraph classGraph =
                      new ClassGraph()
                          .acceptPackages(packageNames.stream().distinct().toArray(String[]::new))
                          .acceptClasses(classNames.stream().distinct().toArray(String[]::new))
                          .enableRemoteJarScanning()
                          .enableExternalClasses()
                          .enableClassInfo()
                          .enableMethodInfo();
                  if (!classLoaders.isEmpty()) {
                    classLoaders.forEach(classGraph::addClassLoader);
                  }

                  try (ScanResult scanResult = classGraph.scan()) {
                    Map<String, ClassInfo> classMap =
                        scanResult.getSubclasses(baseClazz).stream()
                            .collect(Collectors.toMap(ClassInfo::getName, Function.identity()));

                    return configs.stream()
                        .map(
                            config -> {
                              try {
                                ClassInfo classInfo = classMap.get(config.getClassName());
                                if (classInfo == null) {
                                  throw new IllegalStateException(
                                      String.format(
                                          "The following class cannot be loaded: %s",
                                          config.getClassName()));
                                }
                                MethodInfo constructorMethod =
                                    classInfo.getConstructorInfo().get(0);
                                return ((T)
                                        constructorMethod
                                            .loadClassAndGetConstructor()
                                            .newInstance())
                                    .setConfig(config);
                              } catch (Exception e) {
                                log.error(
                                    "Error constructing entity registry plugin class: {}",
                                    config.getClassName(),
                                    e);
                                return (T) null;
                              }
                            })
                        .filter(Objects::nonNull)
                        .filter(PluginSpec::enabled)
                        .collect(Collectors.toList());
                  }
                } catch (Exception e) {
                  throw new IllegalArgumentException(
                      String.format(
                          "Failed to load entity registry plugins: %s.", baseClazz.getName()),
                      e);
                }
              });
    }
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
                pluginConfiguration.validatorPackages(),
                pluginConfiguration.getAspectPayloadValidators()));
  }

  private List<MutationHook> buildMutationHooks(@Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? Collections.emptyList()
        : applyDisable(
            build(
                MutationHook.class,
                pluginConfiguration.mutationPackages(),
                pluginConfiguration.getMutationHooks()));
  }

  private List<MCLSideEffect> buildMCLSideEffects(
      @Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? Collections.emptyList()
        : applyDisable(
            build(
                MCLSideEffect.class,
                pluginConfiguration.mclSideEffectPackages(),
                pluginConfiguration.getMclSideEffects()));
  }

  private List<MCPSideEffect> buildMCPSideEffects(
      @Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? Collections.emptyList()
        : applyDisable(
            build(
                MCPSideEffect.class,
                pluginConfiguration.mcpSideEffectPackages(),
                pluginConfiguration.getMcpSideEffects()));
  }

  /**
   * Load plugins given the base class (i.e. a validator) and the name of the implementing class
   * found in the configuration objects.
   *
   * <p>For performance reasons, scan the packages found in packageNames
   *
   * <p>Designed to avoid any Spring dependency, see alternative implementation for Spring
   *
   * @param baseClazz base class for the plugin
   * @param configs configuration with implementing class information
   * @param packageNames package names to scan
   * @return list of plugin instances
   * @param <T> the plugin class
   */
  protected <T extends PluginSpec> List<T> build(
      Class<?> baseClazz, List<String> packageNames, List<AspectPluginConfig> configs) {
    List<AspectPluginConfig> nonSpringConfigs =
        configs.stream()
            .filter(
                config ->
                    config.getSpring() == null
                        || Boolean.FALSE.equals(config.getSpring().isEnabled()))
            .collect(Collectors.toList());

    return initPlugins(classLoaders, baseClazz, packageNames, nonSpringConfigs);
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
