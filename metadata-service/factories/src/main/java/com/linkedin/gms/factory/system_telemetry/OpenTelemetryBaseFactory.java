package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.system_telemetry.usage.DataHubUsageSpanExporter;
import com.linkedin.metadata.utils.metrics.MetricSpanExporter;
import io.datahubproject.metadata.context.TraceContext;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.kafka.clients.producer.Producer;

/** Common System OpenTelemetry */
public abstract class OpenTelemetryBaseFactory {
  private static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");

  protected abstract String getApplicationComponent();

  protected TraceContext traceContext(
      ConfigurationProvider configurationProvider, Producer<String, String> dueProducer) {
    SpanProcessor usageSpanExporter = getUsageSpanExporter(configurationProvider, dueProducer);
    OpenTelemetry openTelemetry = openTelemetry(usageSpanExporter);
    return TraceContext.builder()
        .tracer(tracer(openTelemetry))
        .usageSpanExporter(usageSpanExporter)
        .build();
  }

  @Nullable
  private SpanProcessor getUsageSpanExporter(
      ConfigurationProvider configurationProvider, Producer<String, String> dueProducer) {
    if (dueProducer != null
        && configurationProvider.getPlatformAnalytics().isEnabled()
        && configurationProvider.getPlatformAnalytics().getUsageExport().isEnabled()) {
      return BatchSpanProcessor.builder(
              new DataHubUsageSpanExporter(
                  dueProducer,
                  configurationProvider.getKafka().getTopics().getDataHubUsage(),
                  configurationProvider.getPlatformAnalytics().getUsageExport()))
          .build();
    }
    return null;
  }

  private Tracer tracer(OpenTelemetry openTelemetry) {
    return openTelemetry.getTracer(getApplicationComponent());
  }

  private OpenTelemetry openTelemetry(SpanProcessor usageSpanExporter) {
    return AutoConfiguredOpenTelemetrySdk.builder()
        .addPropertiesCustomizer(
            (configProperties) -> {
              Map<String, String> props = new HashMap<>();
              // override exporters to "none" if not specified
              Map.of(
                      "OTEL_METRICS_EXPORTER", "otel.metrics.exporter",
                      "OTEL_TRACES_EXPORTER", "otel.traces.exporter",
                      "OTEL_LOGS_EXPORTER", "otel.logs.exporter")
                  .forEach(
                      (envVar, propKey) -> {
                        String value = System.getenv(envVar);
                        if (value == null || value.trim().isEmpty()) {
                          props.put(propKey, "none");
                        }
                      });

              return props;
            })
        .addTracerProviderCustomizer(
            (sdkTracerProviderBuilder, configProperties) -> {
              sdkTracerProviderBuilder
                  .addSpanProcessor(
                      TraceContext.LOG_SPAN_EXPORTER != null
                          ? SimpleSpanProcessor.create(TraceContext.LOG_SPAN_EXPORTER)
                          : SimpleSpanProcessor.create(
                              new MetricSpanExporter())) // Fallback for exporter
                  .addSpanProcessor(BatchSpanProcessor.builder(new MetricSpanExporter()).build())
                  .setIdGenerator(
                      TraceContext.TRACE_ID_GENERATOR != null
                          ? TraceContext.TRACE_ID_GENERATOR
                          : io.opentelemetry.sdk.trace.IdGenerator
                              .random()) // Fallback for ID generator
                  .setResource(
                      Resource.getDefault()
                          .merge(
                              Resource.create(
                                  Attributes.of(
                                      SERVICE_NAME,
                                      getApplicationComponent() != null
                                          ? getApplicationComponent()
                                          : "default-service"))));
              if (usageSpanExporter != null) {
                sdkTracerProviderBuilder.addSpanProcessor(usageSpanExporter);
              }
              return sdkTracerProviderBuilder;
            })
        .addPropagatorCustomizer(
            (existingPropagator, configProperties) -> {
              // If OTEL_PROPAGATORS is not set or doesn't include tracecontext,
              // return W3C propagator, otherwise keep existing
              String propagators = configProperties.getString("OTEL_PROPAGATORS");
              return (propagators == null || !propagators.contains("tracecontext"))
                  ? W3CTraceContextPropagator.getInstance()
                  : existingPropagator;
            })
        .addMetricExporterCustomizer(
            (metricExporter, configProperties) -> {
              String metricsExporter = configProperties.getString("OTEL_METRICS_EXPORTER");
              return (metricsExporter == null || metricsExporter.trim().isEmpty())
                  ? (metricExporter != null
                      ? metricExporter
                      : (MetricExporter) new MetricSpanExporter())
                  : metricExporter;
            })
        .build()
        .getOpenTelemetrySdk();
  }
}
