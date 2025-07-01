package com.linkedin.metadata.utils.metrics;

import static com.linkedin.metadata.utils.metrics.MetricUtils.DROPWIZARD_METRIC;
import static com.linkedin.metadata.utils.metrics.MetricUtils.DROPWIZARD_NAME;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/** Created to forward opentelemetry spans to dropwizard for backwards compatibility */
public class MetricSpanExporter implements SpanExporter {
  private static final AttributeKey<String> DROPWIZARD_ATTR_KEY =
      AttributeKey.stringKey(DROPWIZARD_METRIC);
  private static final AttributeKey<String> DROPWIZARD_NAME_ATTR_KEY =
      AttributeKey.stringKey(DROPWIZARD_NAME);

  @Override
  public CompletableResultCode export(Collection<SpanData> spans) {
    spans.stream().filter(this::shouldRecordMetric).forEach(this::recordSpanMetric);

    return CompletableResultCode.ofSuccess();
  }

  private boolean shouldRecordMetric(SpanData span) {
    // Check for the recordMetric attribute
    return Boolean.parseBoolean(span.getAttributes().get(DROPWIZARD_ATTR_KEY))
        || span.getAttributes().get(DROPWIZARD_NAME_ATTR_KEY) != null;
  }

  private void recordSpanMetric(SpanData span) {
    // Calculate duration in nanoseconds
    long durationNanos = span.getEndEpochNanos() - span.getStartEpochNanos();
    String dropWizardName = span.getAttributes().get(DROPWIZARD_NAME_ATTR_KEY);
    String dropWizardMetricName =
        dropWizardName == null
            ? MetricRegistry.name(span.getName())
            : MetricRegistry.name(dropWizardName);

    // Update timer with the span duration
    Timer timer = MetricUtils.get().timer(dropWizardMetricName);
    timer.update(durationNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public CompletableResultCode flush() {
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode shutdown() {
    return CompletableResultCode.ofSuccess();
  }
}
