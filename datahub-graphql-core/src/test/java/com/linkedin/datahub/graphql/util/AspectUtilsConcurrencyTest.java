package com.linkedin.datahub.graphql.util;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Concurrency characterization for aspect optimization.
 *
 * <p>{@link QueryContext} carries a single mutable {@code DataFetchingEnvironment}. Multiple entity
 * loaders can run within one request, so we verify the safety invariants that must hold regardless
 * of interleaving:
 *
 * <ul>
 *   <li>{@link AspectUtils#getOptimizedAspects} never returns {@code null}.
 *   <li>The always-include key aspect is always present when optimization succeeds.
 *   <li>When the registry cannot resolve a selection, it falls back to the full default set.
 * </ul>
 */
public class AspectUtilsConcurrencyTest {

  private static final Set<String> DEFAULT_ASPECTS =
      ImmutableSet.of("datasetKey", "datasetProperties", "ownership", "globalTags", "status");

  /** Minimal context that emulates SpringQueryContext's mutable DFE/registry storage. */
  private static final class MutableContext implements QueryContext {
    private final AtomicReference<DataFetchingEnvironment> dfe = new AtomicReference<>();
    private final AspectMappingRegistry registry;

    MutableContext(AspectMappingRegistry registry) {
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
    public DataFetchingEnvironment getDataFetchingEnvironment() {
      return dfe.get();
    }

    @Override
    public void setDataFetchingEnvironment(DataFetchingEnvironment environment) {
      dfe.set(environment);
    }

    @Override
    public AspectMappingRegistry getAspectMappingRegistry() {
      return registry;
    }

    @Override
    public void setAspectMappingRegistry(AspectMappingRegistry aspectMappingRegistry) {}
  }

  private DataFetchingEnvironment dfeReturning(Set<String> resolvedAspects, boolean fallback) {
    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet selectionSet = Mockito.mock(DataFetchingFieldSelectionSet.class);
    Mockito.when(env.getSelectionSet()).thenReturn(selectionSet);
    Mockito.when(selectionSet.getFields()).thenReturn(List.of());
    return env;
  }

  @Test
  public void testSafetyInvariantsUnderConcurrentDfeMutation() throws Exception {
    AspectMappingRegistry registry = Mockito.mock(AspectMappingRegistry.class);
    // Registry always resolves to a minimal set for this test's selection.
    Mockito.when(registry.getRequiredAspects(Mockito.eq("Dataset"), Mockito.anyList()))
        .thenReturn(ImmutableSet.of("datasetProperties"));

    MutableContext context = new MutableContext(registry);

    int threads = 16;
    int iterations = 200;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();

    for (int t = 0; t < threads; t++) {
      pool.submit(
          () -> {
            try {
              start.await();
              for (int i = 0; i < iterations; i++) {
                // Each iteration overwrites the shared DFE, then resolves — emulating many
                // concurrent entity loaders sharing one request context.
                context.setDataFetchingEnvironment(dfeReturning(Set.of(), false));
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
  }

  @Test
  public void testFallbackWhenRegistryReturnsNullUnderConcurrency() throws Exception {
    AspectMappingRegistry registry = Mockito.mock(AspectMappingRegistry.class);
    // Simulate an unmapped field: registry returns null -> must fall back to full set.
    Mockito.when(registry.getRequiredAspects(Mockito.eq("Dataset"), Mockito.anyList()))
        .thenReturn(null);

    MutableContext context = new MutableContext(registry);

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
                context.setDataFetchingEnvironment(dfeReturning(Set.of(), true));
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
