package com.linkedin.metadata.search.elasticsearch.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.base.Ticker;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class StructuredPropertyMappingCacheTest {

  private final OperationContext operationContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void testFieldNamesIncludeUrnNameSiblings() {
    Map<String, Object> mapping =
        Map.of(
            "properties",
            Map.of(
                "structuredProperties",
                Map.of(
                    "properties",
                    Map.of(
                        "my_property", Map.of("type", "keyword"),
                        "my_property__urn_name", Map.of("type", "keyword")))));

    assertEquals(
        StructuredPropertyMappingCache.fieldNames(mapping),
        Set.of("structuredProperties.my_property", "structuredProperties.my_property__urn_name"));
  }

  @Test
  public void testFieldNamesFlattenVersionedLeaves() {
    Map<String, Object> stringLeaf =
        Map.of("type", "keyword", "fields", Map.of("keyword", Map.of("type", "keyword")));
    Map<String, Object> mapping =
        Map.of(
            "properties",
            Map.of(
                "structuredProperties",
                Map.of(
                    "properties",
                    Map.of(
                        "kept",
                        Map.of("type", "keyword"),
                        "_versioned",
                        Map.of(
                            "properties",
                            Map.of(
                                "hello",
                                Map.of(
                                    "properties",
                                    Map.of(
                                        "00000000000001",
                                        Map.of("properties", Map.of("string", stringLeaf)))),
                                "gone",
                                Map.of(
                                    "properties",
                                    Map.of(
                                        "00000000000001",
                                        Map.of(
                                            "properties",
                                            Map.of(
                                                "string",
                                                Map.of("type", "keyword"),
                                                "string__urn_name",
                                                Map.of("type", "keyword")))))))))));

    assertEquals(
        StructuredPropertyMappingCache.fieldNames(mapping),
        Set.of(
            "structuredProperties.kept",
            "structuredProperties._versioned.hello.00000000000001.string",
            "structuredProperties._versioned.gone.00000000000001.string",
            "structuredProperties._versioned.gone.00000000000001.string__urn_name"));
  }

  @Test
  public void testFieldNamesWalkObjectTypedVersionedNodes() {
    Map<String, Object> stringLeaf =
        Map.of("type", "keyword", "fields", Map.of("keyword", Map.of("type", "keyword")));
    Map<String, Object> mapping =
        Map.of(
            "properties",
            Map.of(
                "structuredProperties",
                objectMapping(
                    Map.of(
                        "kept",
                        Map.of("type", "keyword"),
                        "_versioned",
                        objectMapping(
                            Map.of(
                                "hello",
                                objectMapping(
                                    Map.of(
                                        "00000000000001",
                                        objectMapping(Map.of("string", stringLeaf))))))))));

    assertEquals(
        StructuredPropertyMappingCache.fieldNames(mapping),
        Set.of(
            "structuredProperties.kept",
            "structuredProperties._versioned.hello.00000000000001.string"));
  }

  @Test
  public void testFieldNamesAcceptATypeWrappedMapping() {
    Map<String, Object> mapping =
        Map.of(
            "_doc",
            Map.of(
                "properties",
                Map.of(
                    "structuredProperties",
                    Map.of("properties", Map.of("only_here", Map.of("type", "keyword"))))));

    assertEquals(
        StructuredPropertyMappingCache.fieldNames(mapping),
        Set.of("structuredProperties.only_here"));
  }

  @Test
  public void testSecondReadInsideTheTtlDoesNotReload() throws IOException {
    ManualTicker ticker = new ManualTicker();
    AtomicInteger reads = new AtomicInteger();
    StructuredPropertyMappingCache cache =
        new StructuredPropertyMappingCache(
            true,
            300,
            ticker,
            (opContext, indexName) -> {
              reads.incrementAndGet();
              return Map.of(
                  "properties",
                  Map.of(
                      "structuredProperties",
                      Map.of("properties", Map.of("kept", Map.of("type", "keyword")))));
            });

    Optional<Set<String>> first = cache.fields(operationContext, "datasetindex_v2");
    Optional<Set<String>> second = cache.fields(operationContext, "datasetindex_v2");

    assertEquals(first, Optional.of(Set.of("structuredProperties.kept")));
    assertEquals(second, first);
    assertEquals(reads.get(), 1);

    ticker.advanceSeconds(301);
    cache.fields(operationContext, "datasetindex_v2");
    assertEquals(reads.get(), 2);
  }

  @Test
  public void testFailedReadIsNotCached() {
    AtomicInteger reads = new AtomicInteger();
    StructuredPropertyMappingCache cache =
        new StructuredPropertyMappingCache(
            true,
            300,
            (opContext, indexName) -> {
              reads.incrementAndGet();
              throw new IOException("mapping unavailable");
            });

    assertFalse(cache.fields(operationContext, "datasetindex_v2").isPresent());
    assertFalse(cache.fields(operationContext, "datasetindex_v2").isPresent());
    assertEquals(reads.get(), 2);
  }

  @Test
  public void testDisabledCacheDoesNotRead() {
    AtomicInteger reads = new AtomicInteger();
    StructuredPropertyMappingCache cache =
        new StructuredPropertyMappingCache(
            false,
            300,
            (opContext, indexName) -> {
              reads.incrementAndGet();
              throw new IOException("should not be called");
            });

    assertTrue(cache.fields(operationContext, "datasetindex_v2").orElseThrow().isEmpty());
    assertEquals(reads.get(), 0);
  }

  private static Map<String, Object> objectMapping(Map<String, Object> properties) {
    return Map.of("type", "object", "properties", properties);
  }

  private static final class ManualTicker extends Ticker {
    private long nanos;

    void advanceSeconds(long seconds) {
      nanos += TimeUnit.SECONDS.toNanos(seconds);
    }

    @Override
    public long read() {
      return nanos;
    }
  }
}
