package com.linkedin.gms.factory.entityregistry;

import com.datahub.plugins.metadata.aspect.SpringPluginFactory;
import com.linkedin.gms.factory.plugins.SpringStandardPluginConfiguration;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import com.linkedin.metadata.aspect.plugins.hooks.MCLSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MCPObserver;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Slf4j
@Configuration
@Import({ConfigEntityRegistryFactory.class, PluginEntityRegistryFactory.class})
public class EntityRegistryFactory {

  @Autowired
  @Qualifier("configEntityRegistry")
  private ConfigEntityRegistry configEntityRegistry;

  @Autowired
  @Qualifier("pluginEntityRegistry")
  private PluginEntityRegistryLoader pluginEntityRegistryLoader;

  @Autowired private ApplicationContext applicationContext;

  @SneakyThrows
  @Bean("entityRegistry")
  @Primary
  @Nonnull
  protected EntityRegistry getInstance(
      SpringStandardPluginConfiguration springStandardPluginConfiguration)
      throws EntityRegistryException {
    BiFunction<PluginConfiguration, List<ClassLoader>, PluginFactory> pluginFactoryProvider =
        (config, loaders) -> new SpringPluginFactory(applicationContext, config, loaders);
    MergedEntityRegistry baseEntityRegistry =
        new MergedEntityRegistry(new SnapshotEntityRegistry(pluginFactoryProvider))
            .apply(configEntityRegistry);
    pluginEntityRegistryLoader.withBaseRegistry(baseEntityRegistry).start(true);
    return baseEntityRegistry;
  }

  /**
   * Runs after all singleton beans are created. Picks up any plugin beans that were unavailable
   * during entity registry construction due to circular dependencies (e.g. plugins that
   * transitively depend on EntityRegistry via systemOperationContext).
   */
  @Bean
  SmartInitializingSingleton entityRegistryPluginRefresh(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry) {
    return () -> {
      PluginFactory pluginFactory = entityRegistry.getPluginFactory();

      List<AspectPayloadValidator> newValidators =
          findMissing(
              BeanFactoryUtils.beansOfTypeIncludingAncestors(
                      applicationContext, AspectPayloadValidator.class)
                  .values(),
              pluginFactory.getAspectPayloadValidators());
      List<MutationHook> newHooks =
          findMissing(
              BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, MutationHook.class)
                  .values(),
              pluginFactory.getMutationHooks());
      List<MCLSideEffect> newMclEffects =
          findMissing(
              BeanFactoryUtils.beansOfTypeIncludingAncestors(
                      applicationContext, MCLSideEffect.class)
                  .values(),
              pluginFactory.getMclSideEffects());
      List<MCPSideEffect> newMcpEffects =
          findMissing(
              BeanFactoryUtils.beansOfTypeIncludingAncestors(
                      applicationContext, MCPSideEffect.class)
                  .values(),
              pluginFactory.getMcpSideEffects());
      List<MCPObserver> newObservers =
          findMissing(
              BeanFactoryUtils.beansOfTypeIncludingAncestors(applicationContext, MCPObserver.class)
                  .values(),
              pluginFactory.getMcpObservers());

      int total =
          newValidators.size()
              + newHooks.size()
              + newMclEffects.size()
              + newMcpEffects.size()
              + newObservers.size();

      if (total == 0) {
        log.info("Entity registry plugin refresh: all Spring plugin beans already registered");
        return;
      }

      pluginFactory.appendPlugins(
          newValidators, newHooks, newMclEffects, newMcpEffects, newObservers);

      log.info(
          "Entity registry plugin refresh: appended {} plugin(s) — "
              + "validators={}, mutationHooks={}, mclSideEffects={}, mcpSideEffects={}, mcpObservers={}",
          total,
          newValidators.stream()
              .map(p -> p.getClass().getSimpleName())
              .collect(Collectors.toList()),
          newHooks.stream().map(p -> p.getClass().getSimpleName()).collect(Collectors.toList()),
          newMclEffects.stream()
              .map(p -> p.getClass().getSimpleName())
              .collect(Collectors.toList()),
          newMcpEffects.stream()
              .map(p -> p.getClass().getSimpleName())
              .collect(Collectors.toList()),
          newObservers.stream()
              .map(p -> p.getClass().getSimpleName())
              .collect(Collectors.toList()));
    };
  }

  private static <T> List<T> findMissing(Collection<T> springBeans, List<T> registered) {
    Set<T> registeredSet = Set.copyOf(registered);
    return springBeans.stream()
        .filter(bean -> !registeredSet.contains(bean))
        .collect(Collectors.toList());
  }
}
