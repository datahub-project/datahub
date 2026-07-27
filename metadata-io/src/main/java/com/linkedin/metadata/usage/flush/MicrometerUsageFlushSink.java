package com.linkedin.metadata.usage.flush;

import com.linkedin.metadata.usage.UsageDimensions;
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
                  Tags.of(
                      "identity_metric", identityMetric, UsageDimensions.ACTOR_CLASS, actorClass),
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
    tags =
        withTag(
            tags, UsageDimensions.USAGE_OPERATION, dimensions.get(UsageDimensions.USAGE_OPERATION));
    tags = withTag(tags, UsageDimensions.AGENT_CLASS, dimensions.get(UsageDimensions.AGENT_CLASS));
    tags = withTag(tags, UsageDimensions.AGENT_NAME, dimensions.get(UsageDimensions.AGENT_NAME));
    tags = withTag(tags, UsageDimensions.REQUEST_API, dimensions.get(UsageDimensions.REQUEST_API));
    tags =
        withTag(tags, UsageDimensions.AUTH_CHANNEL, dimensions.get(UsageDimensions.AUTH_CHANNEL));
    if (actorClass != null) {
      tags = withTag(tags, UsageDimensions.ACTOR_CLASS, actorClass.dimensionValue());
    } else {
      tags =
          withTag(tags, UsageDimensions.ACTOR_CLASS, dimensions.get(UsageDimensions.ACTOR_CLASS));
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
