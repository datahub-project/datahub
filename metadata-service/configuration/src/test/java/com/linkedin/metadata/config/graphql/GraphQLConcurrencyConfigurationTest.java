package com.linkedin.metadata.config.graphql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import org.testng.annotations.Test;

public class GraphQLConcurrencyConfigurationTest {

  @Test
  public void testConfiguredSizesAreUsedWhenPositive() {
    GraphQLConcurrencyConfiguration config = config(40, 800, 0);
    assertEquals(config.resolveCorePoolSize(), 40);
    assertEquals(config.resolveMaxPoolSize(), 800);
    assertTrue(config.useSynchronousQueue());
    assertTrue(config.createWorkQueue() instanceof SynchronousQueue);
  }

  @Test
  public void testPositiveQueueSizeUsesBoundedQueue() {
    GraphQLConcurrencyConfiguration config = config(40, 800, 256);
    assertFalse(config.useSynchronousQueue());
    assertTrue(config.createWorkQueue() instanceof ArrayBlockingQueue);
  }

  @Test
  public void testScaleWithProcessorsForcesLegacySizingAndQueue() {
    GraphQLConcurrencyConfiguration config = config(40, 800, 256);
    config.setScaleWithProcessors(true);

    int processors = Runtime.getRuntime().availableProcessors();
    assertEquals(config.resolveCorePoolSize(), processors * 5);
    assertEquals(config.resolveMaxPoolSize(), processors * 100);
    assertTrue(config.useSynchronousQueue());
    assertTrue(config.createWorkQueue() instanceof SynchronousQueue);
  }

  @Test
  public void testNegativeCorePoolSizeSentinelUsesProcessors() {
    GraphQLConcurrencyConfiguration config = config(-1, 800, 0);
    assertEquals(config.resolveCorePoolSize(), Runtime.getRuntime().availableProcessors() * 5);
    assertEquals(config.resolveMaxPoolSize(), 800);
  }

  @Test
  public void testNonPositiveMaxPoolSizeSentinelUsesProcessors() {
    GraphQLConcurrencyConfiguration config = config(40, 0, 0);
    assertEquals(config.resolveMaxPoolSize(), Runtime.getRuntime().availableProcessors() * 100);
  }

  @Test
  public void testNonPositiveQueueSizeUsesSynchronousQueue() {
    GraphQLConcurrencyConfiguration config = config(40, 800, 0);
    assertTrue(config.useSynchronousQueue());
    assertTrue(config.createWorkQueue() instanceof SynchronousQueue);
  }

  private static GraphQLConcurrencyConfiguration config(int core, int max, int queue) {
    GraphQLConcurrencyConfiguration configuration = new GraphQLConcurrencyConfiguration();
    configuration.setScaleWithProcessors(false);
    configuration.setCorePoolSize(core);
    configuration.setMaxPoolSize(max);
    configuration.setQueueSize(queue);
    configuration.setKeepAlive(60);
    return configuration;
  }
}
