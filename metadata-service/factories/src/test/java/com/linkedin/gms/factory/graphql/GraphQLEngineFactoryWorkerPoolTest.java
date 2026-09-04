package com.linkedin.gms.factory.graphql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.graphql.GraphQLConcurrencyConfiguration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.testng.annotations.Test;

public class GraphQLEngineFactoryWorkerPoolTest {

  @Test
  public void testBoundedQueueEnablesCoreTimeoutWhenKeepAlivePositive() {
    GraphQLConcurrencyConfiguration config = config(40, 800, 256, 60);
    ThreadPoolExecutor pool = GraphQLEngineFactory.createGraphQLThreadPool(config);
    try {
      assertTrue(pool.getQueue() instanceof ArrayBlockingQueue);
      assertTrue(pool.allowsCoreThreadTimeOut());
      assertEquals(pool.getCorePoolSize(), 40);
      assertEquals(pool.getMaximumPoolSize(), 800);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testBoundedQueueWithZeroKeepAliveDoesNotThrow() {
    GraphQLConcurrencyConfiguration config = config(40, 800, 256, 0);
    ThreadPoolExecutor pool = GraphQLEngineFactory.createGraphQLThreadPool(config);
    try {
      assertTrue(pool.getQueue() instanceof ArrayBlockingQueue);
      assertFalse(pool.allowsCoreThreadTimeOut());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testScaleWithProcessorsUsesProcessorSizedPoolAndSynchronousQueue() {
    GraphQLConcurrencyConfiguration config = config(40, 800, 256, 60);
    config.setScaleWithProcessors(true);
    int processors = Runtime.getRuntime().availableProcessors();
    ThreadPoolExecutor pool = GraphQLEngineFactory.createGraphQLThreadPool(config);
    try {
      assertEquals(pool.getCorePoolSize(), processors * 5);
      assertEquals(pool.getMaximumPoolSize(), processors * 100);
      assertTrue(pool.getQueue() instanceof SynchronousQueue);
      assertFalse(pool.allowsCoreThreadTimeOut());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testSentinelCoreAndMaxPoolSizesUseProcessors() {
    GraphQLConcurrencyConfiguration config = config(-1, 0, 0, 60);
    int processors = Runtime.getRuntime().availableProcessors();
    ThreadPoolExecutor pool = GraphQLEngineFactory.createGraphQLThreadPool(config);
    try {
      assertEquals(pool.getCorePoolSize(), processors * 5);
      assertEquals(pool.getMaximumPoolSize(), processors * 100);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testMaxPoolSizeBelowCoreFailsFast() {
    GraphQLConcurrencyConfiguration config = config(40, 10, 0, 60);
    assertThrows(
        IllegalArgumentException.class, () -> GraphQLEngineFactory.createGraphQLThreadPool(config));
  }

  private static GraphQLConcurrencyConfiguration config(
      int core, int max, int queue, int keepAlive) {
    GraphQLConcurrencyConfiguration configuration = new GraphQLConcurrencyConfiguration();
    configuration.setScaleWithProcessors(false);
    configuration.setCorePoolSize(core);
    configuration.setMaxPoolSize(max);
    configuration.setQueueSize(queue);
    configuration.setKeepAlive(keepAlive);
    configuration.setStackSize(256000);
    return configuration;
  }
}
