package com.datahub.plugins.metadata.aspect;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPObserver;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Verifies that plugin beans created after {@link SpringPluginFactory} construction (due to
 * circular dependencies during entity registry initialization) can be reconciled via {@link
 * PluginFactory#appendPlugins}.
 *
 * <p>Simulates the production scenario where (an {@link MCPObserver}) transitively depends on
 * {@code EntityRegistry} and is therefore unavailable when {@link SpringPluginFactory#build}
 * queries {@code BeanFactoryUtils.beansOfTypeIncludingAncestors}.
 */
public class SpringPluginLateRegistrationTest {

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  /**
   * Simulates the circular dependency scenario:
   *
   * <ol>
   *   <li>Build a Spring context with an MCPObserver that is NOT registered as a Spring-scanned
   *       plugin config (simulating the bean being invisible during entity registry construction)
   *   <li>Construct the entity registry via SpringPluginFactory — the observer is missing
   *   <li>Run the same reconciliation logic as the SmartInitializingSingleton in
   *       EntityRegistryFactory
   *   <li>Verify the observer is now in the registry's plugin factory
   * </ol>
   */
  @Test
  public void testLateRegisteredObserverPickedUpByAppendPlugins() {
    AnnotationConfigApplicationContext springContext = new AnnotationConfigApplicationContext();
    springContext.register(LateObserverConfiguration.class);
    springContext.refresh();

    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            SpringPluginLateRegistrationTest.class
                .getClassLoader()
                .getResourceAsStream(SpringPluginFactoryTest.REGISTRY_FILE_1));

    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(configEntityRegistry);
    PluginFactory pluginFactory = mergedRegistry.getPluginFactory();

    List<MCPObserver> initialObservers = pluginFactory.getMcpObservers();
    Set<String> initialClasses =
        initialObservers.stream()
            .map(o -> o.getClass().getSimpleName())
            .collect(Collectors.toSet());
    assertTrue(
        !initialClasses.contains("LateObserver"),
        "LateObserver should NOT be in the initial plugin factory (simulates circular dep)");

    Collection<MCPObserver> springObservers =
        springContext.getBeansOfType(MCPObserver.class).values();
    List<MCPObserver> missing =
        springObservers.stream()
            .filter(bean -> !initialObservers.contains(bean))
            .collect(Collectors.toList());

    assertEquals(missing.size(), 1, "Should find exactly one missing observer from Spring context");

    pluginFactory.appendPlugins(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        missing);

    List<MCPObserver> finalObservers = pluginFactory.getMcpObservers();
    Set<String> finalClasses =
        finalObservers.stream().map(o -> o.getClass().getSimpleName()).collect(Collectors.toSet());
    assertTrue(
        finalClasses.contains("LateObserver"),
        "LateObserver should be present after appendPlugins reconciliation");

    springContext.close();
  }

  @Test
  public void testAppendPluginsIsNoopWhenAllRegistered() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            SpringPluginLateRegistrationTest.class
                .getClassLoader()
                .getResourceAsStream(SpringPluginFactoryTest.REGISTRY_FILE_1));

    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(configEntityRegistry);
    PluginFactory pluginFactory = mergedRegistry.getPluginFactory();

    List<MCPObserver> existingObservers = pluginFactory.getMcpObservers();
    int beforeSize = existingObservers.size();

    pluginFactory.appendPlugins(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        List.copyOf(existingObservers));

    assertEquals(pluginFactory.getMcpObservers().size(), beforeSize);
  }

  @Configuration
  static class LateObserverConfiguration {
    @Bean
    public MCPObserver lateObserver() {
      LateObserver observer = new LateObserver();
      observer.setConfig(
          AspectPluginConfig.builder()
              .className(LateObserver.class.getName())
              .enabled(true)
              .supportedOperations(List.of("UPSERT", "CREATE"))
              .supportedEntityAspectNames(
                  List.of(
                      AspectPluginConfig.EntityAspectName.builder()
                          .entityName("dataHubExecutionRequest")
                          .aspectName("dataHubExecutionRequestResult")
                          .build()))
              .build());
      return observer;
    }
  }

  @Getter
  @Setter
  @Accessors(chain = true)
  static class LateObserver extends MCPObserver {
    private AspectPluginConfig config;

    @Override
    public PluginSpec setConfig(@Nonnull AspectPluginConfig config) {
      this.config = config;
      return this;
    }

    @Override
    protected void observeMCPs(
        Collection<? extends BatchItem> items, @Nonnull RetrieverContext retrieverContext) {}
  }
}
