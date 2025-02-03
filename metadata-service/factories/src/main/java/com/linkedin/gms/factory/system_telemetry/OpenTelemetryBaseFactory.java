package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.metadata.utils.metrics.MetricSpanExporter;
import io.datahubproject.metadata.context.TraceContext;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.HashMap;
import java.util.Map;

/** Common System OpenTelemetry */
public abstract class OpenTelemetryBaseFactory {
  private static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");

  protected abstract String getApplicationComponent();

  protected TraceContext traceContext() {
    return TraceContext.builder().tracer(tracer(openTelemetry())).build();
  }

  private Tracer tracer(OpenTelemetry openTelemetry) {
    return openTelemetry.getTracer(getApplicationComponent());
  }

  private OpenTelemetry openTelemetry() {
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
            (sdkTracerProviderBuilder, configProperties) ->
                sdkTracerProviderBuilder
                    .addSpanProcessor(SimpleSpanProcessor.create(TraceContext.LOG_SPAN_EXPORTER))
                    .addSpanProcessor(BatchSpanProcessor.builder(new MetricSpanExporter()).build())
                    .setIdGenerator(TraceContext.TRACE_ID_GENERATOR)
                    .setResource(
                        Resource.getDefault()
                            .merge(
                                Resource.create(
                                    Attributes.of(SERVICE_NAME, getApplicationComponent())))))
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
                  ? null // Return null to disable the exporter
                  : metricExporter;
            })
        .build()
        .getOpenTelemetrySdk();
  }
}
