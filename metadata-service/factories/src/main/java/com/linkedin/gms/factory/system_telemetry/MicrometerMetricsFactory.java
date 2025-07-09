package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.trace.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

@Slf4j
@Configuration
public class MicrometerMetricsFactory {

  /** Preserve legacy flat names by ignoring tags */
  @Deprecated
  private static final HierarchicalNameMapper LEGACY_HIERARCHICAL_MAPPER =
      new HierarchicalNameMapper() {
        @NotNull
        @Override
        public String toHierarchicalName(Meter.Id id, @NotNull NamingConvention namingConvention) {
          return id.getName();
        }
      };

  /**
   * Provides the default JMX configuration for exporting metrics to JMX. This bean is only created
   * when: 1. No other JmxConfig bean exists (@ConditionalOnMissingBean) 2. JMX metrics export is
   * explicitly enabled in application properties
   *
   * <p>The JMX registry exposes metrics as MBeans that can be viewed through tools like JConsole or
   * JMX.
   *
   * @return Default JmxConfig that defines how metrics are exported to JMX
   */
  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnProperty(
      prefix = "management.metrics.export.jmx",
      name = "enabled",
      havingValue = "true")
  public JmxConfig customJmxConfig() {
    return JmxConfig.DEFAULT;
  }

  /**
   * Creates a JMX meter registry that exports metrics as JMX MBeans. This registry uses a custom
   * hierarchical name mapper that preserves legacy flat naming by ignoring tags, ensuring backward
   * compatibility with existing JMX metric consumers.
   *
   * <p>Only activated when JMX export is enabled via management.metrics.export.jmx.enabled=true
   *
   * @param config JMX configuration defining export behavior
   * @param clock Clock instance for timing measurements
   * @return JmxMeterRegistry that exposes metrics as JMX MBeans with legacy flat names
   */
  @Bean
  @ConditionalOnProperty(
      prefix = "management.metrics.export.jmx",
      name = "enabled",
      havingValue = "true")
  public JmxMeterRegistry jmxMeterRegistry(JmxConfig config, Clock clock) {
    return new JmxMeterRegistry(config, clock, LEGACY_HIERARCHICAL_MAPPER);
  }

  /**
   * Customizes the JMX meter registry to only expose legacy Dropwizard metrics. This filter ensures
   * that only metrics tagged with MetricUtils.DROPWIZARD_METRIC are exported to JMX, preventing new
   * Micrometer-native metrics from appearing in JMX.
   *
   * <p>This separation allows: - Legacy systems to continue consuming metrics via JMX without
   * changes - New metrics to be exposed through modern systems like Prometheus
   *
   * @return MeterRegistryCustomizer that filters metrics based on DROPWIZARD_METRIC tag
   */
  @Bean
  @ConditionalOnProperty(
      prefix = "management.metrics.export.jmx",
      name = "enabled",
      havingValue = "true")
  public MeterRegistryCustomizer<JmxMeterRegistry> jmxMetricsCustomization() {
    return registry ->
        registry
            .config()
            .meterFilter(
                new MeterFilter() {
                  @Override
                  public MeterFilterReply accept(Meter.Id id) {
                    return id.getTag(MetricUtils.DROPWIZARD_METRIC) != null
                        ? MeterFilterReply.ACCEPT
                        : MeterFilterReply.DENY;
                  }
                });
  }

  /**
   * Customizes the Prometheus meter registry to exclude legacy Dropwizard metrics. This is the
   * inverse of the JMX filter - it only accepts metrics WITHOUT the DROPWIZARD_METRIC tag,
   * providing a clean slate for modern metric collection.
   *
   * <p>This separation strategy allows: - Prometheus to collect only new, properly-tagged
   * Micrometer metrics - Gradual migration from legacy metrics to modern metrics - Clean metric
   * namespaces in Prometheus without legacy naming conventions
   *
   * @return MeterRegistryCustomizer that excludes legacy Dropwizard metrics from Prometheus
   */
  @Bean
  @ConditionalOnProperty(
      prefix = "management.metrics.export.prometheus",
      name = "enabled",
      havingValue = "true")
  public MeterRegistryCustomizer<PrometheusMeterRegistry> prometheusMetricsCustomization() {
    return registry ->
        registry
            .config()
            .meterFilter(
                new MeterFilter() {
                  @Override
                  public MeterFilterReply accept(Meter.Id id) {
                    return id.getTag(MetricUtils.DROPWIZARD_METRIC) == null
                        ? MeterFilterReply.ACCEPT
                        : MeterFilterReply.DENY;
                  }
                });
  }

  /**
   * Creates an OtelTracer that bridges the existing OpenTelemetry Tracer with Micrometer's tracing
   * system. This allows Micrometer's Observation API to create OpenTelemetry spans using your
   * custom-configured tracer.
   *
   * @return OtelTracer that wraps your existing tracer for use with Micrometer Observation API
   */
  @Bean
  @ConditionalOnProperty(prefix = "management.tracing", name = "enabled", havingValue = "true")
  public OtelTracer otelTracer(SystemTelemetryContext telemetryContext) {
    // Use your existing tracer
    Tracer existingTracer = telemetryContext.getTracer();
    return new OtelTracer(existingTracer, new OtelCurrentTraceContext(), null);
  }

  /**
   * Creates the ObservationRegistry which is the central component of Micrometer's Observation API.
   * This registry allows you to instrument code once and get both metrics AND traces from the same
   * instrumentation.
   *
   * <p>When you use @Observed annotations or manually create observations, this registry ensures
   * that: 1. Metrics are recorded in your MeterRegistry (timers, counters, etc.) 2. Traces/spans
   * are created via your OpenTelemetry tracer
   *
   * @param meterRegistry The Micrometer registry where metrics will be recorded
   * @param otelTracer The bridged OpenTelemetry tracer for creating spans
   * @return ObservationRegistry configured with both metrics and tracing handlers
   */
  @Bean
  @ConditionalOnBean({OtelTracer.class, MeterRegistry.class})
  public ObservationRegistry observationRegistry(
      MeterRegistry meterRegistry, OtelTracer otelTracer) {
    ObservationRegistry registry = ObservationRegistry.create();

    // Add Micrometer metrics handler
    registry
        .observationConfig()
        .observationHandler(new DefaultMeterObservationHandler(meterRegistry));

    // Add OpenTelemetry tracing handler
    registry
        .observationConfig()
        .observationHandler(new DefaultTracingObservationHandler(otelTracer));

    log.info("Setup OpenTelemetry / Micrometer bridge.");

    return registry;
  }

  /**
   * Configures which HTTP requests should be observed. Excludes health checks and actuator
   * endpoints from creating spans/metrics.
   */
  @Bean
  public ObservationPredicate noActuatorObservations() {
    return (name, context) -> {
      if (context instanceof ServerRequestObservationContext serverContext) {
        String path = serverContext.getCarrier().getRequestURI();
        return !path.startsWith("/actuator") && !path.equals("/health") && !path.equals("/config");
      }
      return true;
    };
  }
}
