package com.linkedin.gms.factory.context;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.common.ObjectMapperFactory;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheScheduler;
import com.linkedin.metadata.graph.cache.service.EntityGraphCacheService;
import com.linkedin.metadata.graph.cache.store.EntityGraphMemoryPressureMonitor;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.concurrent.ExecutorService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Loads {@link EntityGraphCacheFactory} with the production {@code application.yaml} and the
 * bundled {@code entity-graph-cache.yaml}, and checks what {@code
 * datahub.gms.entityGraphCache.enabled} and the {@code configJson} overlay ({@code
 * ENTITY_GRAPH_CACHE_CONFIG_JSON}) do to the wiring.
 */
public class EntityGraphCacheFactoryTest {

  private static final String DOMAIN = KnownEntityGraph.DOMAIN.getConfigKey();

  private TestHazelcastInstanceFactory hazelcastFactory;
  private HazelcastInstance hazelcast;

  @BeforeClass
  public void startHazelcast() {
    hazelcastFactory = new TestHazelcastInstanceFactory();
    hazelcast = hazelcastFactory.newHazelcastInstance();
  }

  @AfterClass(alwaysRun = true)
  public void stopHazelcast() {
    if (hazelcastFactory != null) {
      hazelcastFactory.shutdownAll();
    }
  }

  private ApplicationContextRunner withoutHazelcast() {
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(EntityGraphCacheFactory.class, ObjectMapperFactory.class)
        .withBean("entityRegistry", EntityRegistry.class, () -> entityRegistry)
        .withBean(
            "systemOperationContext",
            OperationContext.class,
            TestOperationContexts::systemContextNoValidate);
  }

  private ApplicationContextRunner withHazelcast() {
    return withoutHazelcast()
        .withBean("hazelcastInstance", HazelcastInstance.class, () -> hazelcast);
  }

  @Test
  public void testEnabledByDefaultWiresTheHazelcastBackedService() {
    withHazelcast()
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              EntityGraphCache cache = context.getBean("entityGraphCache", EntityGraphCache.class);
              assertThat(cache)
                  .isNotSameAs(EntityGraphCache.NO_OP)
                  .isSameAs(context.getBean("entityGraphCacheService"));
              assertThat(context).hasSingleBean(EntityGraphCacheScheduler.class);
              assertThat(context).hasSingleBean(EntityGraphMemoryPressureMonitor.class);

              // The bundled entity-graph-cache.yaml is loaded: the domain graph is rebuilt on a
              // schedule with its shipped bounds.
              EntityGraphRegistry registry = context.getBean(EntityGraphRegistry.class);
              EntityGraphDefinition domain = registry.getDefinition(DOMAIN);
              assertThat(domain).isNotNull();
              assertThat(domain.isEnabled()).isTrue();
              assertThat(domain.getBounds().getMaxVertices()).isEqualTo(500);
              assertThat(registry.getScheduledFullGraphs())
                  .extracting(EntityGraphDefinition::getGraphId)
                  .contains(DOMAIN);
            });
  }

  @Test
  public void testJsonOverlayMergesIntoTheBundledGraph() {
    withHazelcast()
        .withPropertyValues(
            "datahub.gms.entityGraphCache.configJson="
                + "{\"graphs\":{\"domain\":{\"bounds\":{\"maxVertices\":20000}}}}")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              EntityGraphDefinition domain =
                  context.getBean(EntityGraphRegistry.class).getDefinition(DOMAIN);
              assertThat(domain.getBounds().getMaxVertices()).isEqualTo(20000);
              // Fields the overlay does not mention keep their bundled values.
              assertThat(domain.isEnabled()).isTrue();
              assertThat(domain.getBindings().getFilterFields()).contains("domains.keyword");
            });
  }

  @Test
  public void testJsonOverlayCanDisableABundledGraph() {
    withHazelcast()
        .withPropertyValues(
            "datahub.gms.entityGraphCache.configJson={\"graphs\":{\"domain\":{\"enabled\":false}}}")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean(EntityGraphRegistry.class).getScheduledFullGraphs())
                  .extracting(EntityGraphDefinition::getGraphId)
                  .doesNotContain(DOMAIN);
            });
  }

  @Test
  public void testDisabledRegistersOnlyTheNoOpCache() {
    // No Hazelcast bean: MAE/MCE/upgrade contexts run the factory with the cache off and no
    // cluster.
    withoutHazelcast()
        .withPropertyValues("datahub.gms.entityGraphCache.enabled=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean("entityGraphCache", EntityGraphCache.class))
                  .isSameAs(EntityGraphCache.NO_OP);
              assertThat(context).doesNotHaveBean(EntityGraphCacheService.class);
              assertThat(context).doesNotHaveBean(EntityGraphRegistry.class);
              assertThat(context).doesNotHaveBean(EntityGraphCacheScheduler.class);
              assertThat(context).doesNotHaveBean(EntityGraphMemoryPressureMonitor.class);
              assertThat(context).doesNotHaveBean("entityGraphRebuildExecutor");
              assertThat(context.getBeansOfType(ExecutorService.class)).isEmpty();
            });
  }

  @Test
  public void testEnabledWithoutHazelcastFailsStartup() {
    withoutHazelcast()
        .run(
            context ->
                assertThat(context)
                    .hasFailed()
                    .getFailure()
                    .rootCause()
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("requires Hazelcast"));
  }
}
