package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.system_telemetry.usage.DataHubUsageSpanExporter;
import com.linkedin.metadata.event.UsageEventPublisher;
import com.linkedin.metadata.utils.metrics.MetricSpanExporter;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Common System OpenTelemetry */
@Slf4j
public abstract class OpenTelemetryBaseFactory {
  private static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");

  protected abstract String getApplicationComponent();

  protected SystemTelemetryContext traceContext(
      MetricUtils metricUtils,
      ConfigurationProvider configurationProvider,
      UsageEventPublisher usageEventPublisher) {

    SpanProcessor usageSpanExporter =
        getUsageSpanExporter(configurationProvider, usageEventPublisher);
    OpenTelemetry openTelemetry = openTelemetry(metricUtils, usageSpanExporter);
    return SystemTelemetryContext.builder()
        .metricUtils(metricUtils)
        .tracer(tracer(openTelemetry))
        .usageSpanExporter(usageSpanExporter)
        .build();
  }

  @Nullable
  private SpanProcessor getUsageSpanExporter(
      ConfigurationProvider configurationProvider, UsageEventPublisher usageEventPublisher) {
    if (usageEventPublisher != null
        && configurationProvider.getPlatformAnalytics().isEnabled()
        && configurationProvider.getPlatformAnalytics().getUsageExport().isEnabled()) {
      // Bounded async BSP: drops spans when the queue is full — never blocks the producer thread.
      return BatchSpanProcessor.builder(
              new DataHubUsageSpanExporter(
                  usageEventPublisher,
                  configurationProvider.getKafka().getTopics().getDataHubUsage(),
                  configurationProvider.getPlatformAnalytics().getUsageExport()))
          .setMaxQueueSize(2048)
          .setMaxExportBatchSize(512)
          .setScheduleDelay(Duration.ofSeconds(5))
          .setExporterTimeout(Duration.ofSeconds(10))
          .build();
    }
    return null;
  }

  private Tracer tracer(OpenTelemetry openTelemetry) {
    return openTelemetry.getTracer(getApplicationComponent());
  }

  private OpenTelemetry openTelemetry(MetricUtils metricUtils, SpanProcessor usageSpanExporter) {
    OpenTelemetrySdk sdk =
        AutoConfiguredOpenTelemetrySdk.builder()
            // Do NOT let autoconfigure register a JVM shutdown hook during build().
            // It registers unconditionally via Runtime.addShutdownHook(), which throws
            // IllegalStateException("Shutdown in progress") if the JVM is already tearing
            // down (e.g. an earlier bean/startup failure has triggered shutdown). That
            // exception is wrapped into a ConfigurationException -> BeanCreationException on
            // the traceContext bean, masking the real upstream failure. We register our own
            // guarded hook below instead so SDK shutdown still flushes spans on a healthy
            // exit, but a teardown-time race never masks the genuine error.
            .disableShutdownHook()
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
                  // Network/heavy exporters use async BatchSpanProcessor so span-end never blocks
                  // the request thread. BSP drops spans when the queue is full (bounded at 2048)
                  // rather than blocking the producer — this is the intended behavior.
                  // Full sampling is retained: no sampler is configured, so every span is recorded.
                  //
                  // EXCEPTION — ConditionalLogSpanExporter (LOG_SPAN_EXPORTER) stays on a
                  // SimpleSpanProcessor (synchronous, same thread) ON PURPOSE. It decides whether
                  // to
                  // emit a trace log by reading a ThreadLocal (logTracingEnabled) set on the
                  // request
                  // thread via the X-Enable-Trace-Log header/cookie. A BatchSpanProcessor exports
                  // on a
                  // background thread where that ThreadLocal is never populated, so batching would
                  // make
                  // isLogTracingEnabled() always return false and silently disable the feature. The
                  // synchronous cost is negligible: when the header is absent, export() is a single
                  // ThreadLocal read + early return, so it does not block normal user operations.
                  sdkTracerProviderBuilder
                      .addSpanProcessor(
                          // MetricSpanExporter: forwards OTel spans to Dropwizard timers.
                          // Pure in-memory update — no ordering requirement, async-safe.
                          BatchSpanProcessor.builder(new MetricSpanExporter(metricUtils))
                              .setMaxQueueSize(2048)
                              .setMaxExportBatchSize(512)
                              .setScheduleDelay(Duration.ofSeconds(5))
                              .setExporterTimeout(Duration.ofSeconds(10))
                              .build())
                      .addSpanProcessor(
                          // ConditionalLogSpanExporter: MUST stay synchronous — see note above.
                          // No-op on the request thread unless X-Enable-Trace-Log is active.
                          SimpleSpanProcessor.create(SystemTelemetryContext.LOG_SPAN_EXPORTER))
                      .setIdGenerator(
                          SystemTelemetryContext.TRACE_ID_GENERATOR != null
                              ? SystemTelemetryContext.TRACE_ID_GENERATOR
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
                          : (MetricExporter) new MetricSpanExporter(metricUtils))
                      : metricExporter;
                })
            .build()
            .getOpenTelemetrySdk();

    registerShutdownHook(sdk);
    return sdk;
  }

  /**
   * Registers a JVM shutdown hook that closes the OpenTelemetry SDK (flushing any pending spans) on
   * a normal exit. Unlike the autoconfigure SDK's built-in hook, registration is guarded: if the
   * JVM is already shutting down, {@link Runtime#addShutdownHook} throws {@code
   * IllegalStateException("Shutdown in progress")}. We swallow that single case so a teardown-time
   * race never propagates out of bean creation and mask the real upstream failure that triggered
   * the shutdown.
   */
  private static void registerShutdownHook(OpenTelemetrySdk sdk) {
    Thread hook = new Thread(sdk::close, "otel-sdk-shutdown");
    try {
      Runtime.getRuntime().addShutdownHook(hook);
    } catch (IllegalStateException e) {
      // JVM shutdown already in progress; nothing to flush against and the SDK will be GC'd.
      // Do not rethrow: this would otherwise surface as a misleading BeanCreationException.
      log.warn(
          "JVM shutdown already in progress; skipping OpenTelemetry SDK shutdown-hook registration.",
          e);
    }
  }
}