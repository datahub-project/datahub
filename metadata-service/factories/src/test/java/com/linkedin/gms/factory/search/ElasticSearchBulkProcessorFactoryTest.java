package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.telemetry.RequestAttributionConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.BulkTelemetryConfig;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import org.mockito.Answers;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@TestPropertySource(locations = "classpath:/application.yaml")
@SpringBootTest(classes = {ElasticSearchBulkProcessorFactory.class})
// Bulk settings are read from bound configuration rather than individual @Value placeholders, so
// that a cluster can overlay them.
@EnableConfigurationProperties(ConfigurationProvider.class)
public class ElasticSearchBulkProcessorFactoryTest extends AbstractTestNGSpringContextTests {
  @Autowired ESBulkProcessor test;

  @MockitoBean public MetricUtils metricUtils;

  @MockitoBean(name = "searchClientShim", answers = Answers.RETURNS_MOCKS)
  SearchClientShim<?> searchClientShim;

  @Test
  void testInjection() {
    assertNotNull(test);
    assertEquals(WriteRequest.RefreshPolicy.NONE, test.getWriteRequestRefreshPolicy());
  }

  @Test
  void byQueryRequestOptionsUsesConfiguredSocketTimeoutMs() {
    RequestOptions opts = ElasticSearchBulkProcessorFactory.buildByQueryRequestOptions(180);
    assertNotNull(opts.getRequestConfig());
    assertEquals(180_000, opts.getRequestConfig().getSocketTimeout());
  }

  @Test
  void attributionIsNullWithoutTelemetryBlock() {
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    assertNull(ElasticSearchBulkProcessorFactory.attribution(provider));
    TelemetryConfiguration telemetry = new TelemetryConfiguration();
    when(provider.getTelemetry()).thenReturn(telemetry);
    assertSame(
        ElasticSearchBulkProcessorFactory.attribution(provider), telemetry.getRequestAttribution());
  }

  @Test
  void buildPassesAttributionFlagsToTheShim() {
    BulkProcessorConfiguration config = new BulkProcessorConfiguration();
    config.setRefreshPolicy("NONE");
    Tracer tracer = OpenTelemetry.noop().getTracer("test");

    SearchClientShim<?> off = mock(SearchClientShim.class);
    ElasticSearchBulkProcessorFactory.build(off, config, 1, null);
    verify(off).configureBulkTelemetry(BulkTelemetryConfig.DISABLED);

    RequestAttributionConfiguration attribution = new RequestAttributionConfiguration();
    attribution.setServiceName("gms");
    SearchClientShim<?> disabled = mock(SearchClientShim.class);
    ElasticSearchBulkProcessorFactory.build(disabled, config, 1, null, attribution, tracer);
    verify(disabled).configureBulkTelemetry(BulkTelemetryConfig.of(tracer, false, false, "gms"));

    attribution.setEnabled(true);
    SearchClientShim<?> spansOnly = mock(SearchClientShim.class);
    ElasticSearchBulkProcessorFactory.build(spansOnly, config, 1, null, attribution, tracer);
    verify(spansOnly).configureBulkTelemetry(BulkTelemetryConfig.of(tracer, true, false, "gms"));

    attribution.setOpensearchOpaqueId(true);
    SearchClientShim<?> both = mock(SearchClientShim.class);
    ElasticSearchBulkProcessorFactory.build(both, config, 1, null, attribution, tracer);
    verify(both).configureBulkTelemetry(BulkTelemetryConfig.of(tracer, true, true, "gms"));

    assertSame(
        ElasticSearchBulkProcessorFactory.bulkTelemetry(null, tracer, null),
        BulkTelemetryConfig.DISABLED);
  }

  @Test
  void bulkTelemetryPublishesToTheMetricsRegistry() {
    RequestAttributionConfiguration attribution = new RequestAttributionConfiguration();
    attribution.setEnabled(true);
    Tracer tracer = OpenTelemetry.noop().getTracer("test");
    MeterRegistry registry = new SimpleMeterRegistry();
    MetricUtils metricUtils = mock(MetricUtils.class);
    when(metricUtils.getRegistry()).thenReturn(registry);

    assertSame(
        ElasticSearchBulkProcessorFactory.bulkTelemetry(attribution, tracer, metricUtils)
            .getMeterRegistry(),
        registry);
    assertNull(
        ElasticSearchBulkProcessorFactory.bulkTelemetry(attribution, tracer, null)
            .getMeterRegistry());
  }
}
