package com.linkedin.gms;

import com.linkedin.gms.factory.search.SearchClientShims;
import com.linkedin.gms.factory.search.SemanticSearchServiceFactory;
import com.linkedin.gms.factory.search.semantic.EmbeddingProviderFactory;
import com.linkedin.gms.factory.search.semantic.SemanticEntitySearchServiceFactory;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

/**
 * Boots the GMS Spring context from the production {@code application.yaml}. Not production wiring:
 * the database, bootstrap manager, {@code Clock}, {@code MetricUtils} and the semantic search
 * factories are mocks; the search client is a mock OpenSearch shim (with a matching shims bean; the
 * production {@code SearchClusterRegistryFactory} builds the cluster registry over it); and {@code
 * systemOperationContext}, {@code entityRegistry} and {@code meterRegistry} are test instances.
 * Subclasses add one targeted override each; Spring caches a context per distinct override.
 *
 * <p>Subclasses must declare their own {@code @Test} methods: Gradle only detects a test class that
 * carries {@code @Test} itself.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.MOCK,
    properties = {
      "telemetry.enabledServer=true",
      "spring.main.allow-bean-definition-overriding=true",
      "authentication.tokenService.signingKey=test-signing-key-for-tests",
      "authentication.tokenService.salt=test-salt-for-tests",
    })
@ContextConfiguration(
    classes = {CommonApplicationConfig.class, AbstractGmsSpringContextTest.TestBeans.class})
public abstract class AbstractGmsSpringContextTest extends AbstractTestNGSpringContextTests {

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

  // GMSApplication is a @SpringBootApplication, so production gets the Kafka listener
  // infrastructure from Boot auto-configuration. @ContextConfiguration bypasses auto-configuration,
  // so enable it explicitly for beans that inject KafkaListenerEndpointRegistry.
  @EnableKafka
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
  }
}
