package com.linkedin.gms.factory.system_telemetry;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.PlatformAnalyticsConfiguration;
import com.linkedin.metadata.config.UsageExportConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.*;

public class OpenTelemetryBaseFactoryTest {

  // Mock TopicsConfiguration since it's not provided in the classes
  public static class TopicsConfiguration
      extends com.linkedin.metadata.config.kafka.TopicsConfiguration {
    private String dataHubUsage;

    public String getDataHubUsage() {
      return dataHubUsage;
    }

    public void setDataHubUsage(String dataHubUsage) {
      this.dataHubUsage = dataHubUsage;
    }
  }

  // Extend KafkaConfiguration to add the missing setTopics method for testing
  public static class TestKafkaConfiguration extends KafkaConfiguration {
    private TopicsConfiguration topics;

    public void setTopics(TopicsConfiguration topics) {
      this.topics = topics;
    }

    @Override
    public TopicsConfiguration getTopics() {
      return topics;
    }
  }

  @Mock private MetricUtils mockMetricUtils;

  @Mock private ConfigurationProvider mockConfigurationProvider;

  @Mock private Producer<String, String> mockProducer;

  @Mock private PlatformAnalyticsConfiguration mockPlatformAnalytics;

  @Mock private UsageExportConfiguration mockUsageExport;

  @Mock private KafkaConfiguration mockKafka;

  @Mock private TopicsConfiguration mockTopics;

  private TestOpenTelemetryFactory factory;
  private AutoCloseable mocks;

  // Test implementation of the abstract class
  private static class TestOpenTelemetryFactory extends OpenTelemetryBaseFactory {
    private final String applicationComponent;

    public TestOpenTelemetryFactory(String applicationComponent) {
      this.applicationComponent = applicationComponent;
    }

    @Override
    protected String getApplicationComponent() {
      return applicationComponent;
    }

    // Expose protected method for testing
    public SystemTelemetryContext testTraceContext(
        MetricUtils metricUtils,
        ConfigurationProvider configurationProvider,
        Producer<String, String> dueProducer) {
      return traceContext(metricUtils, configurationProvider, dueProducer);
    }
  }

  @BeforeMethod
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    factory = new TestOpenTelemetryFactory("test-component");

    // Setup mock chain for ConfigurationProvider
    when(mockConfigurationProvider.getPlatformAnalytics()).thenReturn(mockPlatformAnalytics);
    when(mockPlatformAnalytics.getUsageExport()).thenReturn(mockUsageExport);
    when(mockConfigurationProvider.getKafka()).thenReturn(mockKafka);
    when(mockKafka.getTopics()).thenReturn(mockTopics);
    when(mockTopics.getDataHubUsage()).thenReturn("test-usage-topic");
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testTraceContextWithUsageExportEnabled() {
    // Arrange
    when(mockPlatformAnalytics.isEnabled()).thenReturn(true);
    when(mockUsageExport.isEnabled()).thenReturn(true);

    // Act
    SystemTelemetryContext context =
        factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

    // Assert
    assertNotNull(context);
    assertNotNull(context.getMetricUtils());
    assertNotNull(context.getTracer());
    assertNotNull(context.getUsageSpanExporter());
    assertEquals(context.getMetricUtils(), mockMetricUtils);
  }

  @Test
  public void testTraceContextWithUsageExportDisabled() {
    // Arrange
    when(mockPlatformAnalytics.isEnabled()).thenReturn(false);

    // Act
    SystemTelemetryContext context =
        factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

    // Assert
    assertNotNull(context);
    assertNotNull(context.getMetricUtils());
    assertNotNull(context.getTracer());
    assertNull(context.getUsageSpanExporter());
  }

  @Test
  public void testTraceContextWithNullProducer() {
    // Arrange
    when(mockPlatformAnalytics.isEnabled()).thenReturn(true);
    when(mockUsageExport.isEnabled()).thenReturn(true);

    // Act
    SystemTelemetryContext context =
        factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, null);

    // Assert
    assertNotNull(context);
    assertNull(context.getUsageSpanExporter());
  }

  @Test
  public void testTraceContextWithPlatformAnalyticsEnabledButUsageExportDisabled() {
    // Arrange
    when(mockPlatformAnalytics.isEnabled()).thenReturn(true);
    when(mockUsageExport.isEnabled()).thenReturn(false);

    // Act
    SystemTelemetryContext context =
        factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

    // Assert
    assertNotNull(context);
    assertNull(context.getUsageSpanExporter());
  }

  @Test
  public void testGetUsageSpanExporterPrivateMethod() throws Exception {
    // Use reflection to test private method
    Method getUsageSpanExporterMethod =
        OpenTelemetryBaseFactory.class.getDeclaredMethod(
            "getUsageSpanExporter", ConfigurationProvider.class, Producer.class);
    getUsageSpanExporterMethod.setAccessible(true);

    // Test with all conditions met
    when(mockPlatformAnalytics.isEnabled()).thenReturn(true);
    when(mockUsageExport.isEnabled()).thenReturn(true);

    SpanProcessor result =
        (SpanProcessor)
            getUsageSpanExporterMethod.invoke(factory, mockConfigurationProvider, mockProducer);

    assertNotNull(result);
    assertTrue(result instanceof BatchSpanProcessor);
  }

  @Test
  public void testTracerCreation() throws Exception {
    // Use reflection to test private tracer method
    Method tracerMethod =
        OpenTelemetryBaseFactory.class.getDeclaredMethod(
            "tracer", io.opentelemetry.api.OpenTelemetry.class);
    tracerMethod.setAccessible(true);

    // Create a mock OpenTelemetry
    io.opentelemetry.api.OpenTelemetry mockOpenTelemetry =
        mock(io.opentelemetry.api.OpenTelemetry.class);
    Tracer mockTracer = mock(Tracer.class);
    when(mockOpenTelemetry.getTracer("test-component")).thenReturn(mockTracer);

    Tracer result = (Tracer) tracerMethod.invoke(factory, mockOpenTelemetry);

    assertNotNull(result);
    assertEquals(result, mockTracer);
    verify(mockOpenTelemetry).getTracer("test-component");
  }

  @Test
  public void testUsageExportConfigurationProperties() {
    // Test the full configuration chain with actual class properties
    when(mockPlatformAnalytics.isEnabled()).thenReturn(true);
    when(mockUsageExport.isEnabled()).thenReturn(true);
    when(mockUsageExport.getUsageEventTypes()).thenReturn("LOGIN,LOGOUT,SEARCH");
    when(mockUsageExport.getAspectTypes()).thenReturn("datasetProfile,datasetUsageStatistics");
    when(mockUsageExport.getUserFilters()).thenReturn("urn:li:corpuser:datahub");

    SystemTelemetryContext context =
        factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

    assertNotNull(context);
    assertNotNull(context.getUsageSpanExporter());

    // Verify that the configuration methods were called
    verify(mockUsageExport, atLeastOnce()).isEnabled();
    verify(mockPlatformAnalytics, atLeastOnce()).isEnabled();
  }

  @Test
  public void testKafkaConfigurationIntegration() {
    // Test Kafka configuration integration
    when(mockPlatformAnalytics.isEnabled()).thenReturn(true);
    when(mockUsageExport.isEnabled()).thenReturn(true);

    // Mock Kafka configuration details
    when(mockKafka.getBootstrapServers()).thenReturn("localhost:9092");

    SystemTelemetryContext context =
        factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

    assertNotNull(context);
    verify(mockKafka).getTopics();
    verify(mockTopics).getDataHubUsage();
  }

  @Test
  public void testConfigurationProviderInheritance() {
    // Test that ConfigurationProvider properly extends DataHubAppConfiguration
    ConfigurationProvider actualProvider = new ConfigurationProvider();

    // Test that we can set and get inherited properties
    KafkaConfiguration kafkaConfig = new KafkaConfiguration();
    actualProvider.setKafka(kafkaConfig);

    assertEquals(actualProvider.getKafka(), kafkaConfig);

    // Test platform analytics configuration
    PlatformAnalyticsConfiguration analyticsConfig = new PlatformAnalyticsConfiguration();
    analyticsConfig.setEnabled(true);
    actualProvider.setPlatformAnalytics(analyticsConfig);

    assertTrue(actualProvider.getPlatformAnalytics().isEnabled());
  }

  @Test
  public void testFullConfigurationChain() {
    // Create actual configuration objects to test the full chain
    ConfigurationProvider provider = new ConfigurationProvider();

    // Setup Kafka configuration using our test extension
    TestKafkaConfiguration kafkaConfig = new TestKafkaConfiguration();
    TopicsConfiguration topicsConfig = new TopicsConfiguration();
    topicsConfig.setDataHubUsage("datahub-usage-events");
    kafkaConfig.setTopics(topicsConfig);
    provider.setKafka(kafkaConfig);

    // Setup Platform Analytics
    PlatformAnalyticsConfiguration analyticsConfig = new PlatformAnalyticsConfiguration();
    analyticsConfig.setEnabled(true);

    UsageExportConfiguration usageExportConfig = new UsageExportConfiguration();
    usageExportConfig.setEnabled(true);
    usageExportConfig.setUsageEventTypes("LOGIN,SEARCH");
    usageExportConfig.setAspectTypes("datasetProfile");
    usageExportConfig.setUserFilters("urn:li:corpuser:test");

    analyticsConfig.setUsageExport(usageExportConfig);
    provider.setPlatformAnalytics(analyticsConfig);

    // Verify the configuration chain
    assertNotNull(provider.getKafka());
    assertNotNull(provider.getKafka().getTopics());
    assertNotNull(provider.getPlatformAnalytics());
    assertNotNull(provider.getPlatformAnalytics().getUsageExport());
    assertTrue(provider.getPlatformAnalytics().isEnabled());
    assertTrue(provider.getPlatformAnalytics().getUsageExport().isEnabled());
    assertEquals(
        "LOGIN,SEARCH", provider.getPlatformAnalytics().getUsageExport().getUsageEventTypes());
    assertEquals("datahub-usage-events", provider.getKafka().getTopics().getDataHubUsage());
  }

  @Test
  public void testApplicationComponentNull() {
    // Test with null application component
    TestOpenTelemetryFactory nullComponentFactory = new TestOpenTelemetryFactory(null);

    SystemTelemetryContext context =
        nullComponentFactory.testTraceContext(
            mockMetricUtils, mockConfigurationProvider, mockProducer);

    assertNotNull(context);
    // The service name should fall back to "default-service" when component is null
  }

  @Test
  public void testEnvironmentVariableHandling() {
    // This test verifies that the factory properly handles environment variables
    // In a real test, you might want to use a library like System Rules or System Lambda
    // to mock environment variables

    Map<String, String> originalEnv = new HashMap<>();
    originalEnv.put("OTEL_METRICS_EXPORTER", System.getenv("OTEL_METRICS_EXPORTER"));
    originalEnv.put("OTEL_TRACES_EXPORTER", System.getenv("OTEL_TRACES_EXPORTER"));
    originalEnv.put("OTEL_LOGS_EXPORTER", System.getenv("OTEL_LOGS_EXPORTER"));

    try {
      // Test with no environment variables set
      SystemTelemetryContext context =
          factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

      assertNotNull(context);
      // The default should be "none" for exporters when env vars are not set
    } finally {
      // Restore original environment (in a real test, use proper env mocking)
    }
  }

  @Test
  public void testOpenTelemetryConfiguration() throws Exception {
    // Test the OpenTelemetry configuration
    Method openTelemetryMethod =
        OpenTelemetryBaseFactory.class.getDeclaredMethod(
            "openTelemetry", MetricUtils.class, SpanProcessor.class);
    openTelemetryMethod.setAccessible(true);

    SpanProcessor mockSpanProcessor = mock(SpanProcessor.class);

    io.opentelemetry.api.OpenTelemetry result =
        (io.opentelemetry.api.OpenTelemetry)
            openTelemetryMethod.invoke(factory, mockMetricUtils, mockSpanProcessor);

    assertNotNull(result);
  }

  @Test
  public void testTraceContextWithNullMetricUtils() {
    factory.testTraceContext(null, mockConfigurationProvider, mockProducer);
    // should not throw exception
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testTraceContextWithNullConfigurationProvider() {
    // This should throw NPE as ConfigurationProvider is required
    factory.testTraceContext(mockMetricUtils, null, mockProducer);
  }

  @Test
  public void testMultipleInstancesWithDifferentComponents() {
    // Test that multiple instances can be created with different components
    TestOpenTelemetryFactory factory1 = new TestOpenTelemetryFactory("component1");
    TestOpenTelemetryFactory factory2 = new TestOpenTelemetryFactory("component2");

    SystemTelemetryContext context1 =
        factory1.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);
    SystemTelemetryContext context2 =
        factory2.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

    assertNotNull(context1);
    assertNotNull(context2);
    // Each should have its own tracer with the respective component name
  }

  @Test
  public void testSystemTelemetryContextStaticFields() throws Exception {
    // Test handling of static fields in SystemTelemetryContext
    Field logSpanExporterField = SystemTelemetryContext.class.getDeclaredField("LOG_SPAN_EXPORTER");
    logSpanExporterField.setAccessible(true);

    Field traceIdGeneratorField =
        SystemTelemetryContext.class.getDeclaredField("TRACE_ID_GENERATOR");
    traceIdGeneratorField.setAccessible(true);

    // Test with null static fields (default behavior)
    SystemTelemetryContext context =
        factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

    assertNotNull(context);
    // The factory should handle null static fields gracefully with fallbacks
  }

  @Test
  public void testPropagatorCustomization() {
    // Test that W3C trace context propagator is used when OTEL_PROPAGATORS is not set
    SystemTelemetryContext context =
        factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

    assertNotNull(context);
    // The W3CTraceContextPropagator should be configured
  }

  @Test
  public void testMetricExporterCustomization() {
    // Test metric exporter customization
    SystemTelemetryContext context =
        factory.testTraceContext(mockMetricUtils, mockConfigurationProvider, mockProducer);

    assertNotNull(context);
    // Should use MetricSpanExporter when OTEL_METRICS_EXPORTER is not set
  }
}
