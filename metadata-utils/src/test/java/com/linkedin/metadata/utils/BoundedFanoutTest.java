package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.GenericScrollIterator;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BoundedFanoutTest {

  private SimpleMeterRegistry registry;
  private MetricUtils metricUtils;

  @BeforeMethod
  public void setup() {
    registry = new SimpleMeterRegistry();
    metricUtils = MetricUtils.builder().registry(registry).build();
  }

  @Test
  public void testForEachBatchProcessesAllItemsInBatches() {
    String hook = "BatchAllHook";
    List<Integer> items = IntStream.range(0, 5).boxed().collect(Collectors.toList());
    List<Integer> seen = new ArrayList<>();

    int processed = BoundedFanout.forEachBatch(items, 2, 10, metricUtils, hook, seen::addAll);

    assertEquals(processed, 5);
    assertEquals(seen, items);
    assertEquals(fanoutSizeTotal(hook), 5.0);
    assertNull(capHitCounter(hook));
  }

  @Test
  public void testForEachBatchCapTruncatesAndWarns() {
    String hook = "BatchCapHook";
    List<Integer> items = IntStream.range(0, 5).boxed().collect(Collectors.toList());
    List<Integer> seen = new ArrayList<>();

    int processed = BoundedFanout.forEachBatch(items, 2, 3, metricUtils, hook, seen::addAll);

    assertEquals(processed, 3);
    assertEquals(seen.size(), 3);
    assertEquals(capHitCount(hook), 1.0);
  }

  @Test
  public void testForEachBatchEmptyIsNoOp() {
    String hook = "BatchEmptyHook";
    List<Integer> seen = new ArrayList<>();

    int processed =
        BoundedFanout.forEachBatch(List.<Integer>of(), 2, 10, metricUtils, hook, seen::addAll);

    assertEquals(processed, 0);
    assertTrue(seen.isEmpty());
    assertNull(
        registry
            .find(MetricUtils.DATAHUB_HOOK_FANOUT_SIZE)
            .tag(MetricUtils.HOOK_TAG, hook)
            .summary());
  }

  @Test
  public void testForEachPageProcessesAllPages() {
    String hook = "PageAllHook";
    List<ScrollResult> pages = new ArrayList<>();

    int processed = BoundedFanout.forEachPage(iterator(3, 2), 100, metricUtils, hook, pages::add);

    assertEquals(processed, 6);
    assertEquals(pages.size(), 3);
    assertEquals(fanoutSizeTotal(hook), 6.0);
    assertNull(capHitCounter(hook));
  }

  @Test
  public void testForEachPageCapStopsScrolling() {
    String hook = "PageCapHook";
    List<ScrollResult> pages = new ArrayList<>();

    int processed = BoundedFanout.forEachPage(iterator(5, 2), 3, metricUtils, hook, pages::add);

    // Two pages of 2 cross the cap of 3; scrolling stops with pages remaining.
    assertEquals(processed, 4);
    assertEquals(pages.size(), 2);
    assertEquals(capHitCount(hook), 1.0);
  }

  /** Iterator over {@code numPages} pages of {@code pageSize} placeholder entities each. */
  private static GenericScrollIterator iterator(int numPages, int pageSize) {
    SearchRetriever retriever =
        (entities, filters, scrollId, count, sortCriteria, searchFlags) -> {
          int page = scrollId == null ? 0 : Integer.parseInt(scrollId);
          SearchEntityArray arr = new SearchEntityArray();
          for (int i = 0; i < pageSize; i++) {
            arr.add(new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:corpuser:test")));
          }
          ScrollResult result = new ScrollResult();
          result.setEntities(arr);
          result.setNumEntities(pageSize);
          result.setPageSize(pageSize);
          if (page + 1 < numPages) {
            result.setScrollId(String.valueOf(page + 1));
          }
          return result;
        };
    return GenericScrollIterator.builder()
        .searchRetriever(retriever)
        .filter(new Filter())
        .entities(List.of("dataset"))
        .count(pageSize)
        .build();
  }

  private double fanoutSizeTotal(String hook) {
    return registry
        .find(MetricUtils.DATAHUB_HOOK_FANOUT_SIZE)
        .tag(MetricUtils.HOOK_TAG, hook)
        .summary()
        .totalAmount();
  }

  private Counter capHitCounter(String hook) {
    return registry
        .find(MetricUtils.DATAHUB_HOOK_FANOUT_CAP_HIT)
        .tag(MetricUtils.HOOK_TAG, hook)
        .counter();
  }

  private double capHitCount(String hook) {
    return capHitCounter(hook).count();
  }
}
