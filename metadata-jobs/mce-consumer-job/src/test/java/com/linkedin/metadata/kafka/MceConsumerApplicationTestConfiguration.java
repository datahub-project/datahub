package com.linkedin.metadata.kafka;

import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.context.SystemOperationContextFactory;
import com.linkedin.gms.factory.search.SearchClientShims;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.gms.factory.search.SemanticSearchServiceFactory;
import com.linkedin.gms.factory.search.semantic.EmbeddingProviderFactory;
import com.linkedin.gms.factory.search.semantic.SemanticEntitySearchServiceFactory;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.Map;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@TestConfiguration
@Import(
    value = {
      SystemAuthenticationFactory.class,
      SystemOperationContextFactory.class,
    })
public class MceConsumerApplicationTestConfiguration {

  @MockitoBean public KafkaHealthChecker kafkaHealthChecker;

  // Use @Bean @Primary to prevent EntityServiceFactory from creating a real EntityServiceImpl
  // (which would try to connect to MySQL through its dao dependency chain)
  @Bean
  @Primary
  @SuppressWarnings("unchecked")
  public EntityService<?> entityService() {
    return Mockito.mock(EntityService.class);
  }

  @MockitoBean protected TimeseriesAspectService timeseriesAspectService;

  @MockitoBean protected SiblingGraphService siblingGraphService;

  // Mock semantic search factories to avoid needing full configuration
  @MockitoBean public EmbeddingProviderFactory embeddingProviderFactory;

  @MockitoBean public SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockitoBean public SemanticSearchServiceFactory semanticSearchServiceFactory;

  @MockitoBean public MetricUtils metricUtils;

  // Use @Bean @Primary to prevent SearchClientShimFactory from connecting to localhost:9200
  // See: https://github.com/spring-projects/spring-framework/issues/33934
  @Bean
  @Primary
  @SuppressWarnings("unchecked")
  public SearchClientShim<?> searchClientShim() {
    SearchClientShim<?> mock = Mockito.mock(SearchClientShim.class);
    Mockito.when(mock.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    return mock;
  }

  /**
   * Keep SearchClusterRegistryFactory from building a live {@code searchClientShims} bean against
   * localhost:9200.
   */
  @Bean(name = "searchClientShims")
  @Primary
  public SearchClientShims searchClientShims(SearchClientShim<?> searchClientShim) {
    return new SearchClientShims(
        Map.of(ElasticSearchConfiguration.PRIMARY_CLUSTER, searchClientShim));
  }

  @Bean(name = "searchClusterRegistry")
  @Primary
  public SearchClusterRegistry searchClusterRegistry(SearchClientShim<?> searchClientShim) {
    SearchClusterRegistry registry = Mockito.mock(SearchClusterRegistry.class);
    ESBulkProcessor bulkProcessor = Mockito.mock(ESBulkProcessor.class);
    ESIndexBuilder indexBuilder = Mockito.mock(ESIndexBuilder.class);
    ElasticSearchConfiguration config = Mockito.mock(ElasticSearchConfiguration.class);
    BulkProcessorConfiguration bulkConfig = Mockito.mock(BulkProcessorConfiguration.class);
    Mockito.when(bulkConfig.getNumRetries()).thenReturn(1);
    Mockito.when(config.getBulkProcessor()).thenReturn(bulkConfig);
    Mockito.doReturn(searchClientShim).when(registry).clientFor(Mockito.any(SearchComponent.class));
    Mockito.when(registry.bulkProcessorFor(Mockito.any(SearchComponent.class)))
        .thenReturn(bulkProcessor);
    Mockito.when(registry.indexBuilderFor(Mockito.any(SearchComponent.class)))
        .thenReturn(indexBuilder);
    Mockito.when(registry.configFor(Mockito.any(SearchComponent.class))).thenReturn(config);
    return registry;
  }

  // Use @Bean @Primary to prevent EbeanDatabaseFactory from trying to connect to MySQL
  @Bean
  @Primary
  public Database ebeanServer() {
    return Mockito.mock(Database.class);
  }

  // Use real EntityRegistry to prevent EntityRegistryFactory from calling methods on a plain mock
  // (MergedEntityRegistry.apply(configEntityRegistry) throws AssertionError on unstubbed mocks)
  @Bean
  @Primary
  public EntityRegistry entityRegistry() {
    return TestOperationContexts.constructNewEntityRegistry();
  }

  // Use @Bean (no @Primary) to prevent ConfigEntityRegistryFactory from loading
  // entity-registry.yml.
  // Do NOT add @Primary — ConfigEntityRegistry is a subtype of EntityRegistry and would
  // compete with the entityRegistry bean above, causing NoUniqueBeanDefinitionException.
  // See: https://github.com/spring-projects/spring-framework/issues/33934
  @Bean
  public ConfigEntityRegistry configEntityRegistry() {
    return Mockito.mock(ConfigEntityRegistry.class);
  }

  @Bean
  @Primary
  public EntityClientConfig entityClientConfig() {
    return EntityClientConfig.builder()
        .backoffPolicy(new ExponentialBackoff(1))
        .retryCount(1)
        .batchGetV2Size(1)
        .batchGetV2Concurrency(2)
        .build();
  }

  // Use @Bean @Primary to prevent SystemEntityClientFactory from attempting to initialize
  // a real client during context startup. The test uses RestTemplate directly.
  @Bean
  @Primary
  public SystemEntityClient systemEntityClient() {
    return Mockito.mock(SystemEntityClient.class);
  }
}
