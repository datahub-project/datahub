package com.linkedin.gms.factory.system_telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.trace.Tracer;
import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.http.server.observation.ServerRequestObservationContext;
import org.springframework.mock.web.MockHttpServletRequest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest
public class MicrometerMetricsFactoryTest {

  private ApplicationContextRunner contextRunner;
  private MicrometerMetricsFactory factory;

  @Mock private SystemTelemetryContext telemetryContext;

  @Mock private Tracer tracer;

  @Mock private Clock clock;

  private AutoCloseable mockitoCloseable;

  @BeforeMethod
  public void setUp() {
    mockitoCloseable = MockitoAnnotations.openMocks(this);
    factory = new MicrometerMetricsFactory();

    contextRunner =
        new ApplicationContextRunner()
            .withUserConfiguration(MicrometerMetricsFactory.class)
            .withBean(Clock.class, () -> clock)
            .withBean(SystemTelemetryContext.class, () -> telemetryContext);

    when(telemetryContext.getTracer()).thenReturn(tracer);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mockitoCloseable != null) {
      mockitoCloseable.close();
    }
  }

  @Test
  public void testJmxConfigBeanCreation() {
    contextRunner
        .withPropertyValues("management.metrics.export.jmx.enabled=true")
        .run(
            context -> {
              assertThat(context).hasSingleBean(JmxConfig.class);
              JmxConfig jmxConfig = context.getBean(JmxConfig.class);
              assertThat(jmxConfig).isEqualTo(JmxConfig.DEFAULT);
            });
  }

  @Test
  public void testJmxConfigBeanNotCreatedWhenDisabled() {
    contextRunner
        .withPropertyValues("management.metrics.export.jmx.enabled=false")
        .run(
            context -> {
              assertThat(context).doesNotHaveBean(JmxConfig.class);
            });
  }

  @Test
  public void testJmxMeterRegistryCreation() {
    contextRunner
        .withPropertyValues("management.metrics.export.jmx.enabled=true")
        .run(
            context -> {
              assertThat(context).hasSingleBean(JmxMeterRegistry.class);
              JmxMeterRegistry registry = context.getBean(JmxMeterRegistry.class);
              assertThat(registry).isNotNull();
            });
  }

  @Test
  public void testJmxMeterRegistryNotCreatedWhenDisabled() {
    contextRunner
        .withPropertyValues("management.metrics.export.jmx.enabled=false")
        .run(
            context -> {
              assertThat(context).doesNotHaveBean(JmxMeterRegistry.class);
            });
  }

  @Test
  public void testJmxMetricsCustomizationFiltersDropwizardMetrics() {
    // Create the customizer directly
    MeterRegistryCustomizer<JmxMeterRegistry> customizer = factory.jmxMetricsCustomization();

    // Create a test registry
    JmxMeterRegistry registry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);

    // Apply the customization
    customizer.customize(registry);

    // Create test meters with different tags
    registry.counter("test.dropwizard.metric", MetricUtils.DROPWIZARD_METRIC, "true");
    registry.counter("test.regular.metric");

    // Verify that only the dropwizard metric is registered
    assertThat(registry.getMeters())
        .hasSize(1)
        .extracting(meter -> meter.getId().getName())
        .containsOnly("test.dropwizard.metric");
  }

  @Test
  public void testPrometheusMetricsCustomizationExcludesDropwizardMetrics() {
    // Create the customizer directly
    MeterRegistryCustomizer<PrometheusMeterRegistry> customizer =
        factory.prometheusMetricsCustomization();

    // Create a test registry
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    // Apply the customization
    customizer.customize(registry);

    // Create test meters with different tags
    registry.counter("test.dropwizard.metric", MetricUtils.DROPWIZARD_METRIC, "true");
    registry.counter("test.regular.metric");

    // Verify that only the regular metric is registered (dropwizard metric is excluded)
    assertThat(registry.getMeters())
        .hasSize(1)
        .extracting(meter -> meter.getId().getName())
        .containsOnly("test.regular.metric");
  }

  @Test
  public void testOtelTracerCreation() {
    contextRunner
        .withPropertyValues("management.tracing.enabled=true")
        .run(
            context -> {
              assertThat(context).hasSingleBean(OtelTracer.class);
              OtelTracer otelTracer = context.getBean(OtelTracer.class);
              assertThat(otelTracer).isNotNull();
            });
  }

  @Test
  public void testOtelTracerNotCreatedWhenDisabled() {
    contextRunner
        .withPropertyValues("management.tracing.enabled=false")
        .run(
            context -> {
              assertThat(context).doesNotHaveBean(OtelTracer.class);
            });
  }

  @Test
  public void testObservationRegistryNotCreatedWithoutDependencies() {
    contextRunner
        .withPropertyValues("management.tracing.enabled=false")
        .run(
            context -> {
              assertThat(context).doesNotHaveBean(ObservationRegistry.class);
            });
  }

  @Test
  public void testNoActuatorObservationsPredicateFiltersActuatorPaths() {
    ObservationPredicate predicate = factory.noActuatorObservations();

    // Test actuator paths that should be excluded
    assertThat(testPredicateWithPath(predicate, "/actuator/health")).isFalse();
    assertThat(testPredicateWithPath(predicate, "/actuator/metrics")).isFalse();
    assertThat(testPredicateWithPath(predicate, "/health")).isFalse();
    assertThat(testPredicateWithPath(predicate, "/config")).isFalse();

    // Test regular paths that should be included
    assertThat(testPredicateWithPath(predicate, "/api/users")).isTrue();
    assertThat(testPredicateWithPath(predicate, "/")).isTrue();
    assertThat(testPredicateWithPath(predicate, "/api/actuator"))
        .isTrue(); // Not starting with /actuator
  }

  @Test
  public void testFullIntegrationWithAllComponentsEnabled() {
    contextRunner
        .withPropertyValues(
            "management.metrics.export.jmx.enabled=true",
            "management.metrics.export.prometheus.enabled=true",
            "management.tracing.enabled=true")
        .run(
            context -> {
              // Verify all beans are created
              assertThat(context).hasSingleBean(JmxConfig.class);
              assertThat(context).hasSingleBean(JmxMeterRegistry.class);
              assertThat(context).hasSingleBean(OtelTracer.class);
              assertThat(context).hasSingleBean(ObservationRegistry.class);
              assertThat(context).hasSingleBean(ObservationPredicate.class);

              // Verify customizers are applied
              assertThat(context).hasBean("jmxMetricsCustomization");
              assertThat(context).hasBean("prometheusMetricsCustomization");
            });
  }

  @Test
  public void testMinimalConfigurationWithEverythingDisabled() {
    contextRunner
        .withPropertyValues(
            "management.metrics.export.jmx.enabled=false",
            "management.metrics.export.prometheus.enabled=false",
            "management.tracing.enabled=false")
        .run(
            context -> {
              // Only the ObservationPredicate should be created
              assertThat(context).hasSingleBean(ObservationPredicate.class);

              // Everything else should be absent
              assertThat(context).doesNotHaveBean(JmxConfig.class);
              assertThat(context).doesNotHaveBean(JmxMeterRegistry.class);
              assertThat(context).doesNotHaveBean(OtelTracer.class);
              assertThat(context).doesNotHaveBean(ObservationRegistry.class);
            });
  }

  @Test
  public void testJmxMetricsWithLegacyHierarchicalNameMapper() throws Exception {
    // Create a JMX registry using the factory method
    JmxMeterRegistry registry = factory.jmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);

    // Create a metric with tags
    registry.counter("test.metric.name", "tag1", "value1", "tag2", "value2").increment();

    // Get the MBean server
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    // The legacy mapper creates a flat name with type=meters
    ObjectName objectName = new ObjectName("metrics:name=test.metric.name,type=meters");

    // Verify the MBean exists with the flat name
    assertThat(mBeanServer.isRegistered(objectName)).isTrue();

    // Verify the count value
    Object count = mBeanServer.getAttribute(objectName, "Count");
    assertThat(count).isEqualTo(1L);

    // Verify that the name is flat (no tags in the object name)
    String objectNameString = objectName.toString();
    assertThat(objectNameString).doesNotContain("tag1");
    assertThat(objectNameString).doesNotContain("value1");
    assertThat(objectNameString).doesNotContain("tag2");
    assertThat(objectNameString).doesNotContain("value2");

    // Clean up
    registry.close();
  }

  @Test
  public void testJmxMetricsMultipleMetricsWithSameNameDifferentTagsThrowsException() {
    // This test verifies that the legacy mapper causes metrics with the same name
    // but different tags to conflict because they map to the same flat name
    JmxMeterRegistry registry = factory.jmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);

    try {
      // Create first counter
      registry.counter("duplicate.metric", "env", "prod").increment();

      // Attempting to create a second counter with same name but different tags should fail
      try {
        registry.counter("duplicate.metric", "env", "dev");
        fail("Expected IllegalArgumentException to be thrown");
      } catch (IllegalArgumentException e) {
        assertThat(e.getMessage()).contains("A metric named duplicate.metric already exists");
      }
    } finally {
      // Clean up
      registry.close();
    }
  }

  @Test
  public void testJmxMetricsLegacyMapperReturnsSameCounterForSameTags() throws Exception {
    // This test verifies that requesting a metric with the same name and tags
    // returns the same counter instance
    JmxMeterRegistry registry = factory.jmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);

    // Create counter with tags
    Counter counter1 = registry.counter("shared.metric", "env", "prod", "region", "us-east");
    counter1.increment();

    // Request the same metric with the same name and tags - should return the same instance
    Counter counter2 = registry.counter("shared.metric", "env", "prod", "region", "us-east");

    // These should be the same counter instance
    assertThat(counter1).isSameAs(counter2);

    // Increment the "second" counter
    counter2.increment();

    // Both should show count of 2 since they're the same counter
    assertThat(counter1.count()).isEqualTo(2.0);
    assertThat(counter2.count()).isEqualTo(2.0);

    // Verify in JMX
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = new ObjectName("metrics:name=shared.metric,type=meters");
    Object count = mBeanServer.getAttribute(objectName, "Count");
    assertThat(count).isEqualTo(2L);

    // Clean up
    registry.close();
  }

  @Test
  public void testJmxMetricsLegacyMapperIgnoresTagsInJmxName() throws Exception {
    // This test verifies that the legacy mapper creates flat JMX names without tags
    JmxMeterRegistry registry = factory.jmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);

    // Create a counter with multiple tags
    Counter counter =
        registry.counter("tagged.metric", "env", "prod", "region", "us-east", "service", "api");
    counter.increment(5);

    // Get the MBean server
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    // The JMX name should be flat without any tag information
    ObjectName objectName = new ObjectName("metrics:name=tagged.metric,type=meters");
    assertThat(mBeanServer.isRegistered(objectName)).isTrue();

    // Verify the count
    Object count = mBeanServer.getAttribute(objectName, "Count");
    assertThat(count).isEqualTo(5L);

    // Verify that no MBeans exist with tag information in their names
    Set<ObjectName> allMetrics =
        mBeanServer.queryNames(new ObjectName("metrics:name=tagged.metric,*"), null);
    assertThat(allMetrics).hasSize(1);

    // The single MBean should not contain any tag keys in its name
    ObjectName foundName = allMetrics.iterator().next();
    String nameString = foundName.toString();
    assertThat(nameString).doesNotContain("env");
    assertThat(nameString).doesNotContain("region");
    assertThat(nameString).doesNotContain("service");
    assertThat(nameString).doesNotContain("prod");
    assertThat(nameString).doesNotContain("us-east");
    assertThat(nameString).doesNotContain("api");

    // Clean up
    registry.close();
  }

  // Helper method to test predicate with a specific path
  private boolean testPredicateWithPath(ObservationPredicate predicate, String path) {
    ServerRequestObservationContext context = mock(ServerRequestObservationContext.class);
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setRequestURI(path);
    when(context.getCarrier()).thenReturn(request);

    return predicate.test("test.observation", context);
  }
}
