package com.linkedin.metadata.usage.flush;

import com.linkedin.metadata.usage.registry.metrics.UsageMetricIncrementResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Exports flushed usage aggregation rows to Micrometer (Prometheus-compatible). */
public class MicrometerUsageFlushSink implements UsageFlushSink {

  public static final String ACTIVE_IDENTITIES_METRIC = "datahub.usage.active_identities";

  private final UsageMetricRegistry metricRegistry;
  private final MeterRegistry registry;
  private final ConcurrentHashMap<ActiveIdentityGaugeKey, AtomicInteger> activeIdentityGauges =
      new ConcurrentHashMap<>();

  public MicrometerUsageFlushSink(
      @Nonnull UsageMetricRegistry metricRegistry, @Nonnull MeterRegistry registry) {
    this.metricRegistry = metricRegistry;
    this.registry = registry;
  }

  @Override
  public void publish(@Nonnull UsageFlushBatch batch) {
    for (AdditiveUsageRow row : batch.additiveRows()) {
      UsageMetricRegistry.MetricDefinition metricDefinition =
          metricRegistry.apiUsageMetrics().get(row.metricName());
      if (metricDefinition == null) {
        continue;
      }
      UsageMetricIncrementResolver.micrometerCounterName(metricDefinition)
          .ifPresent(
              micrometerName ->
                  registry
                      .counter(micrometerName, buildTags(row.dimensions(), row.actorClass()))
                      .increment(row.valueSum()));
    }

    for (DistinctUsageSnapshot snapshot : batch.distinctSnapshots()) {
      publishActiveIdentityGauge(
          snapshot.metricName(), snapshot.actorClass(), snapshot.distinctCount());
    }
  }

  private void publishActiveIdentityGauge(
      @Nonnull String identityMetric, @Nonnull String actorClass, int distinctCount) {
    ActiveIdentityGaugeKey key = new ActiveIdentityGaugeKey(identityMetric, actorClass);
    AtomicInteger holder =
        activeIdentityGauges.computeIfAbsent(
            key,
            ignored -> {
              AtomicInteger gaugeValue = new AtomicInteger(0);
              registry.gauge(
                  ACTIVE_IDENTITIES_METRIC,
                  Tags.of("identity_metric", identityMetric, "actor_class", actorClass),
                  gaugeValue,
                  AtomicInteger::get);
              return gaugeValue;
            });
    holder.set(distinctCount);
  }

  @Nonnull
  private Tags buildTags(
      @Nonnull Map<String, String> dimensions, @Nullable UsageActorClass actorClass) {
    Tags tags = Tags.empty();
    tags = withTag(tags, "usage_operation", dimensions.get("usage_operation"));
    tags = withTag(tags, "agent_class", dimensions.get("agent_class"));
    tags = withTag(tags, "request_api", dimensions.get("request_api"));
    tags = withTag(tags, "auth_channel", dimensions.get("auth_channel"));
    if (actorClass != null) {
      tags = withTag(tags, "actor_class", actorClass.dimensionValue());
    } else {
      tags = withTag(tags, "actor_class", dimensions.get("actor_class"));
    }
    return tags;
  }

  @Nonnull
  private static Tags withTag(@Nonnull Tags tags, @Nonnull String key, @Nullable String value) {
    if (value != null && !value.isEmpty()) {
      return tags.and(key, value);
    }
    return tags;
  }

  private record ActiveIdentityGaugeKey(String identityMetric, String actorClass) {}
}
