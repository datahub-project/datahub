package com.linkedin.datahub.graphql.util;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

/**
 * Concurrency coverage for request-scoped {@link AspectLoadContext} unions. Multiple entity loaders
 * may merge selections into one {@link QueryContext} concurrently; the union must remain complete
 * and {@link AspectUtils#getOptimizedAspects} must never return null.
 */
public class AspectUtilsConcurrencyTest {

  private static final Set<String> DEFAULT_ASPECTS =
      ImmutableSet.of("datasetKey", "datasetProperties", "ownership", "globalTags", "status");

  private static final class AccumulatingContext implements QueryContext {
    private final ConcurrentHashMap<String, AspectLoadContext> aspectLoadContexts =
        new ConcurrentHashMap<>();
    private final AspectMappingRegistry registry;

    AccumulatingContext(AspectMappingRegistry registry) {
      this.registry = registry;
    }

    @Override
    public boolean isAuthenticated() {
      return true;
    }

    @Override
    public com.datahub.authentication.Authentication getAuthentication() {
      return null;
    }

    @Override
    public com.datahub.plugins.auth.authorization.Authorizer getAuthorizer() {
      return null;
    }

    @Override
    public io.datahubproject.metadata.context.OperationContext getOperationContext() {
      return null;
    }

    @Override
    public com.linkedin.metadata.config.DataHubAppConfiguration getDataHubAppConfig() {
      return null;
    }

    @Override
    public int getMaxParentDepth() {
      return 50;
    }

    @Override
    public AspectMappingRegistry getAspectMappingRegistry() {
      return registry;
    }

    @Override
    public void setAspectMappingRegistry(AspectMappingRegistry aspectMappingRegistry) {}

    @Override
    public void mergeAspectLoadContext(String entityTypeName, AspectLoadContext loadContext) {
      aspectLoadContexts.merge(entityTypeName, loadContext, AspectLoadContext::union);
    }

    @Override
    public AspectLoadContext getAspectLoadContext(String entityTypeName) {
      return aspectLoadContexts.get(entityTypeName);
    }
  }

  @Test
  public void testSafetyInvariantsUnderConcurrentMerge() throws Exception {
    AccumulatingContext context = new AccumulatingContext(null);

    int threads = 16;
    int iterations = 200;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      pool.submit(
          () -> {
            try {
              start.await();
              for (int i = 0; i < iterations; i++) {
                AspectLoadContext contribution =
                    AspectLoadContext.of(
                        ImmutableSet.of(threadId % 2 == 0 ? "ownership" : "dataPlatformInstance"));
                context.mergeAspectLoadContext("Dataset", contribution);
                Set<String> result =
                    AspectUtils.getOptimizedAspects(
                        context, "Dataset", DEFAULT_ASPECTS, "datasetKey");
                if (result == null) {
                  violations.add("null result");
                } else if (!result.contains("datasetKey")) {
                  violations.add("missing key aspect: " + result);
                }
              }
            } catch (Throwable e) {
              violations.add("exception: " + e);
            }
          });
    }
    start.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "threads did not finish");
    assertTrue(violations.isEmpty(), "Safety violations: " + violations);

    Set<String> finalAspects =
        AspectUtils.getOptimizedAspects(context, "Dataset", DEFAULT_ASPECTS, "datasetKey");
    assertTrue(finalAspects.contains("ownership"), finalAspects.toString());
    assertTrue(finalAspects.contains("dataPlatformInstance"), finalAspects.toString());
  }

  @Test
  public void testFallbackWhenFetchAllUnderConcurrency() throws Exception {
    AccumulatingContext context = new AccumulatingContext(null);

    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();

    for (int t = 0; t < threads; t++) {
      pool.submit(
          () -> {
            try {
              start.await();
              for (int i = 0; i < 200; i++) {
                context.mergeAspectLoadContext("Dataset", AspectLoadContext.fetchAll());
                Set<String> result =
                    AspectUtils.getOptimizedAspects(
                        context, "Dataset", DEFAULT_ASPECTS, "datasetKey");
                if (!result.equals(DEFAULT_ASPECTS)) {
                  violations.add("expected full fallback, got: " + result);
                }
              }
            } catch (Throwable e) {
              violations.add("exception: " + e);
            }
          });
    }
    start.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "threads did not finish");
    assertTrue(violations.isEmpty(), "Fallback violations: " + violations);
  }
}
