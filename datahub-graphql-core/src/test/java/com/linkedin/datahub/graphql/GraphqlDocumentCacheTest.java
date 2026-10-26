package com.linkedin.datahub.graphql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import graphql.GraphqlErrorBuilder;
import graphql.execution.preparsed.PreparsedDocumentEntry;
import graphql.language.Document;
import graphql.parser.Parser;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class GraphqlDocumentCacheTest {

  private static final Parser PARSER = new Parser();

  private static PreparsedDocumentEntry parsed(String query) {
    return new PreparsedDocumentEntry(PARSER.parseDocument(query));
  }

  @Test
  public void testMissThenHitSkipsRecompute() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    String query = "{ hello }";
    AtomicInteger computeCount = new AtomicInteger();

    cache.getOrCompute(
        query,
        () -> {
          computeCount.incrementAndGet();
          return parsed(query);
        });
    cache.getOrCompute(
        query,
        () -> {
          computeCount.incrementAndGet();
          return parsed(query);
        });

    assertEquals(computeCount.get(), 1);
  }

  @Test
  public void testReadOnlyLookupServesCachedDocument() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    String query = "{ hello }";
    PreparsedDocumentEntry entry = cache.getOrCompute(query, () -> parsed(query));

    assertSame(cache.getCachedDocument(query), entry.getDocument());
  }

  @Test
  public void testReadOnlyLookupMissReturnsNullWithoutComputing() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    assertNull(cache.getCachedDocument("{ neverCached }"));
  }

  @Test
  public void testDistinctQueriesDoNotCollide() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    cache.getOrCompute("{ hello }", () -> parsed("{ hello }"));
    cache.getOrCompute("{ world }", () -> parsed("{ world }"));

    Document helloDoc = cache.getCachedDocument("{ hello }");
    Document worldDoc = cache.getCachedDocument("{ world }");
    assertTrue(helloDoc != null && worldDoc != null && helloDoc != worldDoc);
    assertNull(cache.getCachedDocument("{ missing }"));
  }

  @Test
  public void testEvictsUnderWeightPressure() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(2048);
    for (int i = 0; i < 50; i++) {
      String query = "{ field" + UUID.randomUUID().toString().replace("-", "") + " }";
      cache.getOrCompute(query, () -> parsed(query));
    }
    cache.cleanUpForTesting();

    assertTrue(cache.stats().evictionCount() > 0);
  }

  @Test
  public void testExpireAfterAccessIsOneHour() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    assertEquals(cache.getExpireAfterAccess(), Duration.ofHours(1));
  }

  @Test
  public void testEnabledByDefault() {
    assertTrue(new GraphqlDocumentCache(1024 * 1024).isEnabled());
  }

  @Test
  public void testDisabledCacheNeitherWritesNorReads() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    cache.setEnabled(false);
    String query = "{ hello }";
    AtomicInteger computeCount = new AtomicInteger();

    cache.getOrCompute(
        query,
        () -> {
          computeCount.incrementAndGet();
          return parsed(query);
        });
    cache.getOrCompute(
        query,
        () -> {
          computeCount.incrementAndGet();
          return parsed(query);
        });

    assertEquals(computeCount.get(), 2);
    assertNull(cache.getCachedDocument(query));
  }

  @Test
  public void testMaximumWeightIsAdjustableInPlace() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    assertEquals(cache.getMaximumWeightBytes(), 1024 * 1024);

    cache.setMaximumWeightBytes(2048);

    assertEquals(cache.getMaximumWeightBytes(), 2048);
  }

  @Test
  public void testEntryWithValidationErrorsIsCachedAndDocumentIsReadable() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    String query = "{ hello }";
    Document document = PARSER.parseDocument(query);
    PreparsedDocumentEntry withValidationErrors =
        new PreparsedDocumentEntry(
            document, List.of(GraphqlErrorBuilder.newError().message("bad field").build()));
    AtomicInteger computeCount = new AtomicInteger();

    cache.getOrCompute(
        query,
        () -> {
          computeCount.incrementAndGet();
          return withValidationErrors;
        });
    PreparsedDocumentEntry second =
        cache.getOrCompute(
            query,
            () -> {
              computeCount.incrementAndGet();
              return withValidationErrors;
            });

    assertEquals(computeCount.get(), 1);
    assertTrue(second.hasErrors());
    assertSame(cache.getCachedDocument(query), document);
  }

  @Test
  public void testSyntaxErrorEntryIsNotServedAsCachedDocument() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    String query = "{ invalid syntax";
    PreparsedDocumentEntry syntaxError =
        new PreparsedDocumentEntry(GraphqlErrorBuilder.newError().message("syntax error").build());

    cache.getOrCompute(query, () -> syntaxError);

    assertNull(cache.getCachedDocument(query));
  }

  @Test
  public void testConcurrentGetOrComputeInvokesSupplierOnce() throws InterruptedException {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    String query = "{ hello }";
    AtomicInteger computeCount = new AtomicInteger();
    int threadCount = 16;
    CountDownLatch ready = new CountDownLatch(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    try {
      for (int i = 0; i < threadCount; i++) {
        executor.submit(
            () -> {
              ready.countDown();
              try {
                start.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              cache.getOrCompute(
                  query,
                  () -> {
                    computeCount.incrementAndGet();
                    return parsed(query);
                  });
            });
      }
      ready.await();
      start.countDown();
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }

    assertEquals(computeCount.get(), 1);
  }

  @Test
  public void testRegisterMetricsBindsMeters() {
    GraphqlDocumentCache cache = new GraphqlDocumentCache(1024 * 1024);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    cache.registerMetrics(meterRegistry, "graphqlDocumentCacheTest-" + UUID.randomUUID());

    assertTrue(meterRegistry.getMeters().stream().anyMatch(m -> !m.getId().getTags().isEmpty()));
  }
}
