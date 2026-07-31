package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.search.SemanticSearchServiceFactory;
import com.linkedin.gms.factory.search.semantic.EmbeddingProviderFactory;
import com.linkedin.gms.factory.search.semantic.SemanticEntitySearchServiceFactory;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@TestConfiguration
@Import(value = {SystemAuthenticationFactory.class})
public class MaeConsumerApplicationTestConfiguration {

  @MockitoBean private KafkaHealthChecker kafkaHealthChecker;

  @MockitoBean private EntityServiceImpl _entityServiceImpl;

  @MockitoBean private Database ebeanServer;

  // Use real EntityRegistry to prevent EntityRegistryFactory from calling methods on a plain mock
  // (MergedEntityRegistry.apply(configEntityRegistry) throws NPE on unstubbed mocks)
  @Bean
  @Primary
  public EntityRegistry entityRegistry() {
    return TestOperationContexts.defaultEntityRegistry();
  }

  @MockitoBean private RestrictedService restrictedService;

  @MockitoBean private SecretService secretService;

  @MockitoBean private GraphService _graphService;

  @MockitoBean private ElasticSearchSystemMetadataService _elasticSearchSystemMetadataService;

  // Use @Bean to prevent ConfigEntityRegistryFactory from loading entity-registry.yml.
  // Do NOT add @Primary — ConfigEntityRegistry is a subtype of EntityRegistry and would
  // compete with the @MockitoBean entityRegistry above, causing NoUniqueBeanDefinitionException.
  // See: https://github.com/spring-projects/spring-framework/issues/33934
  @Bean
  public ConfigEntityRegistry configEntityRegistry() {
    return Mockito.mock(ConfigEntityRegistry.class);
  }

  @MockitoBean public ElasticSearchService elasticSearchService;

  @MockitoBean public EntitySearchService entitySearchService;

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

  // Mock semantic search factories to avoid needing full configuration
  @MockitoBean public EmbeddingProviderFactory embeddingProviderFactory;

  @MockitoBean public SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockitoBean public SemanticSearchServiceFactory semanticSearchServiceFactory;
}
