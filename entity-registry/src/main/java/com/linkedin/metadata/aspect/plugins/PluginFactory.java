package com.linkedin.metadata.aspect.plugins;

import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.aspect.plugins.hooks.MCLSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.MethodInfo;
import io.github.classgraph.ScanResult;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PluginFactory {
  public static PluginFactory getInstance() {
    return INSTANCE;
  }

  public static PluginFactory withCustomClasspath(@Nullable Path pluginLocation) {
    return new PluginFactory(pluginLocation);
  }

  private static final PluginFactory INSTANCE = new PluginFactory(null);

  @Setter @Getter private PluginConfiguration defaultPluginConfiguration;

  private final ClassGraph classGraph;

  public PluginFactory(@Nullable Path pluginLocation) {
    this.classGraph =
        new ClassGraph()
            .enableRemoteJarScanning()
            .enableExternalClasses()
            .enableClassInfo()
            .enableMethodInfo();

    if (pluginLocation != null) {
      this.classGraph.overrideClasspath(pluginLocation);
    }
  }

  public List<AspectPayloadValidator> buildAspectPayloadValidators() {
    return buildAspectPayloadValidators(defaultPluginConfiguration);
  }

  public List<AspectPayloadValidator> buildAspectPayloadValidators(
      @Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? List.of()
        : build(
            AspectPayloadValidator.class,
            pluginConfiguration.getAspectPayloadValidators(),
            "com.linkedin.metadata.aspect.plugins.validation");
  }

  public List<MutationHook> buildMutationHooks() {
    return buildMutationHooks(defaultPluginConfiguration);
  }

  public List<MutationHook> buildMutationHooks(@Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? List.of()
        : build(
            MutationHook.class,
            pluginConfiguration.getMutationHooks(),
            "com.linkedin.metadata.aspect.plugins.hooks");
  }

  public List<MCLSideEffect<?>> buildMCLSideEffects() {
    return buildMCLSideEffects(defaultPluginConfiguration);
  }

  public List<MCLSideEffect<?>> buildMCLSideEffects(
      @Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? List.of()
        : build(
            MCLSideEffect.class,
            pluginConfiguration.getMclSideEffects(),
            "com.linkedin.metadata.aspect.plugins.hooks");
  }

  public List<MCPSideEffect<?, ?>> buildMCPSideEffects() {
    return buildMCPSideEffects(defaultPluginConfiguration);
  }

  public List<MCPSideEffect<?, ?>> buildMCPSideEffects(
      @Nullable PluginConfiguration pluginConfiguration) {
    return pluginConfiguration == null
        ? List.of()
        : build(
            MCPSideEffect.class,
            pluginConfiguration.getMcpSideEffects(),
            "com.linkedin.metadata.aspect.plugins.hooks");
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
}
