package com.linkedin.metadata.systemmetadata.metrics;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/** Exports key-aspect entity counts to Micrometer gauges. */
public class MicrometerEntityCountMetricsSink implements EntityCountMetricsSink {

  public static final String ENTITY_COUNT_METRIC = "datahub.entity.count";
  public static final String REFRESH_DURATION_METRIC = "datahub.entity.count.refresh.duration";
  public static final String REFRESH_ERRORS_METRIC = "datahub.entity.count.refresh.errors";
  public static final String REFRESH_LAST_SUCCESS_METRIC =
      "datahub.entity.count.refresh.last_success_epoch_seconds";

  public static final String TAG_ENTITY_TYPE = "entity_type";
  public static final String TAG_REMOVAL_STATUS = "removal_status";
  public static final String REMOVAL_STATUS_ACTIVE = "active";
  public static final String REMOVAL_STATUS_SOFT_DELETED = "soft_deleted";

  private final MeterRegistry registry;
  private final ConcurrentHashMap<EntityCountGaugeKey, AtomicLong> entityCountGauges =
      new ConcurrentHashMap<>();
  private final Timer refreshDuration;
  private final Counter refreshErrors;
  private final AtomicLong lastSuccessEpochSeconds = new AtomicLong(0);

  public MicrometerEntityCountMetricsSink(@Nonnull MeterRegistry registry) {
    this.registry = registry;
    this.refreshDuration = registry.timer(REFRESH_DURATION_METRIC);
    this.refreshErrors = registry.counter(REFRESH_ERRORS_METRIC);
    registry.gauge(REFRESH_LAST_SUCCESS_METRIC, lastSuccessEpochSeconds, AtomicLong::get);
  }

  @Override
  public void publish(@Nonnull KeyAspectEntityCountResult result) {
    Set<EntityCountGaugeKey> updatedKeys = new HashSet<>();
    for (KeyAspectEntityCountEntry entry : result.getCounts()) {
      updatedKeys.add(
          setGauge(entry.getEntityType(), REMOVAL_STATUS_ACTIVE, entry.getActiveCount()));
      updatedKeys.add(
          setGauge(
              entry.getEntityType(), REMOVAL_STATUS_SOFT_DELETED, entry.getSoftDeletedCount()));
    }
    for (Map.Entry<EntityCountGaugeKey, AtomicLong> gauge : entityCountGauges.entrySet()) {
      if (!updatedKeys.contains(gauge.getKey())) {
        gauge.getValue().set(0);
      }
    }
  }

  @Nonnull
  public Timer refreshDuration() {
    return refreshDuration;
  }

  public void recordRefreshError() {
    refreshErrors.increment();
  }

  public void recordRefreshSuccess() {
    lastSuccessEpochSeconds.set(System.currentTimeMillis() / 1000);
  }

  @Nonnull
  private EntityCountGaugeKey setGauge(
      @Nonnull String entityType, @Nonnull String removalStatus, long count) {
    EntityCountGaugeKey key = new EntityCountGaugeKey(entityType, removalStatus);
    AtomicLong holder =
        entityCountGauges.computeIfAbsent(
            key,
            ignored -> {
              AtomicLong gaugeValue = new AtomicLong(0);
              registry.gauge(
                  ENTITY_COUNT_METRIC,
                  Tags.of(TAG_ENTITY_TYPE, entityType, TAG_REMOVAL_STATUS, removalStatus),
                  gaugeValue,
                  AtomicLong::get);
              return gaugeValue;
            });
    holder.set(count);
    return key;
  }

  private record EntityCountGaugeKey(String entityType, String removalStatus) {}
}
