package com.linkedin.metadata.utils;

import com.google.common.collect.Lists;
import com.linkedin.metadata.entity.GenericScrollIterator;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Bounded iteration helpers for hook fan-out (RFC-0b). Each entry point enforces a hard cap,
 * records the realized fan-out width on {@link MetricUtils#DATAHUB_HOOK_FANOUT_SIZE}, and — when
 * the cap truncates the work — logs a WARN and increments {@link
 * MetricUtils#DATAHUB_HOOK_FANOUT_CAP_HIT} so operators see silent truncation.
 *
 * <p>Ships unadopted: call sites migrate to these in later phases. Metrics are emitted only when a
 * {@link MetricUtils} is supplied.
 */
@Slf4j
public final class BoundedFanout {

  /** Default ceiling on entities processed per fan-out when a caller does not specify one. */
  public static final int DEFAULT_HARD_CAP = 10_000;

  private BoundedFanout() {}

  /**
   * Scroll pages until the iterator is exhausted or {@code hardCap} entities have been processed,
   * invoking {@code pageConsumer} for each page. Returns the number of entities processed.
   */
  public static int forEachPage(
      GenericScrollIterator iterator,
      int hardCap,
      @Nullable MetricUtils metricUtils,
      String hookName,
      Consumer<ScrollResult> pageConsumer) {
    if (hardCap <= 0) {
      throw new IllegalArgumentException("hardCap must be positive, got " + hardCap);
    }

    int processed = 0;
    boolean capHit = false;
    while (iterator.hasNext()) {
      ScrollResult page = iterator.next();
      pageConsumer.accept(page);
      processed += page.getEntities() != null ? page.getEntities().size() : 0;
      if (processed >= hardCap) {
        capHit = iterator.hasNext();
        break;
      }
    }

    recordFanout(metricUtils, hookName, processed, capHit, hardCap);
    return processed;
  }

  /**
   * Partition {@code items} into batches of at most {@code batchSize} and invoke {@code
   * batchConsumer} for each, processing no more than {@code hardCap} items. Returns the number of
   * items processed.
   */
  public static <T> int forEachBatch(
      List<T> items,
      int batchSize,
      int hardCap,
      @Nullable MetricUtils metricUtils,
      String hookName,
      Consumer<List<T>> batchConsumer) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive, got " + batchSize);
    }
    if (hardCap <= 0) {
      throw new IllegalArgumentException("hardCap must be positive, got " + hardCap);
    }
    if (items == null || items.isEmpty()) {
      return 0;
    }

    boolean capHit = items.size() > hardCap;
    List<T> bounded = capHit ? items.subList(0, hardCap) : items;
    for (List<T> batch : Lists.partition(bounded, batchSize)) {
      batchConsumer.accept(batch);
    }

    recordFanout(metricUtils, hookName, bounded.size(), capHit, hardCap);
    return bounded.size();
  }

  private static void recordFanout(
      @Nullable MetricUtils metricUtils,
      String hookName,
      int processed,
      boolean capHit,
      int hardCap) {
    if (metricUtils != null) {
      metricUtils.recordHookFanout(processed, hookName);
    }
    if (capHit) {
      log.warn(
          "Hook {} hit fan-out hard cap of {} (processed {} and truncated); downstream work is incomplete",
          hookName,
          hardCap,
          processed);
      if (metricUtils != null) {
        metricUtils.incrementMicrometer(
            MetricUtils.DATAHUB_HOOK_FANOUT_CAP_HIT, 1, MetricUtils.HOOK_TAG, hookName);
      }
    }
  }
}
