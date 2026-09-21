package com.linkedin.gms;

import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.SearchClientShims;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.gms.factory.search.SemanticSearchServiceFactory;
import com.linkedin.gms.factory.search.semantic.EmbeddingProviderFactory;
import com.linkedin.gms.factory.search.semantic.SemanticEntitySearchServiceFactory;
import com.linkedin.gms.factory.telemetry.DailyReport;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.MOCK,
    properties = {
      "telemetry.enabledServer=true",
      "spring.main.allow-bean-definition-overriding=true",
      "authentication.tokenService.signingKey=test-signing-key-for-tests",
      "authentication.tokenService.salt=test-salt-for-tests",
    })
@ContextConfiguration(classes = {CommonApplicationConfig.class, SpringTest.TestBeans.class})
public class SpringTest extends AbstractTestNGSpringContextTests {

  // Mock Beans take precedence, we add these to avoid needing to configure data sources etc. while
  // still testing prod config
  @MockitoBean private Database database;

  @MockitoBean private BootstrapManager bootstrapManager;

  @MockitoBean private Clock clock;

  @MockitoBean private MetricUtils metricUtils;

  // Mock semantic search factories to avoid needing full configuration
  @MockitoBean private EmbeddingProviderFactory embeddingProviderFactory;

  @MockitoBean private SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockitoBean private SemanticSearchServiceFactory semanticSearchServiceFactory;

  @Test
  public void testTelemetry() {
    DailyReport dailyReport = this.applicationContext.getBean(DailyReport.class);
    assertNotNull(dailyReport);
  }

  @TestConfiguration
  public static class TestBeans {

    @Bean
    @Primary
    @SuppressWarnings("unchecked")
    public SearchClientShim<?> searchClientShim() {
      SearchClientShim<?> shim = Mockito.mock(SearchClientShim.class);
      Mockito.when(shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
      return shim;
    }

    @Bean
    public OperationContext systemOperationContext(SearchClientShim<?> searchClientShim) {
      return TestOperationContexts.withFixedSearchClient(
          TestOperationContexts.systemContextNoSearchAuthorization(), searchClientShim);
    }

    @Primary
    @Bean
    public EntityRegistry entityRegistry(OperationContext systemOperationContext) {
      return systemOperationContext.getEntityRegistry();
    }

    @Primary
    @Bean
    public MeterRegistry meterRegistry() {
      return new SimpleMeterRegistry();
    }

    @Bean(name = "searchClientShims")
    @Primary
    public SearchClientShims searchClientShims(SearchClientShim<?> searchClientShim) {
      return new SearchClientShims(
          Map.of(ElasticSearchConfiguration.PRIMARY_CLUSTER, searchClientShim));
    }

    @Bean(name = "searchClusterRegistry")
    @Primary
    public SearchClusterRegistry searchClusterRegistry(
        ConfigurationProvider configurationProvider, SearchClientShim<?> searchClientShim) {
      return SearchClusterRegistry.singleCluster(
          configurationProvider.getElasticSearch(),
          searchClientShim,
          Mockito.mock(ESBulkProcessor.class),
          Mockito.mock(ESIndexBuilder.class));
    }
  }
}
