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
import io.ebean.Database;
import org.mockito.Answers;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@TestConfiguration
@Import(value = {SystemAuthenticationFactory.class})
public class MaeConsumerApplicationTestConfiguration {

  @MockitoBean private KafkaHealthChecker kafkaHealthChecker;

  @MockitoBean private EntityServiceImpl _entityServiceImpl;

  @MockitoBean private Database ebeanServer;

  @MockitoBean private EntityRegistry entityRegistry;

  @MockitoBean private RestrictedService restrictedService;

  @MockitoBean private SecretService secretService;

  @MockitoBean private GraphService _graphService;

  @MockitoBean private ElasticSearchSystemMetadataService _elasticSearchSystemMetadataService;

  @MockitoBean private ConfigEntityRegistry _configEntityRegistry;

  @MockitoBean public ElasticSearchService elasticSearchService;

  @MockitoBean public EntitySearchService entitySearchService;

  @MockitoBean public MetricUtils metricUtils;

  @MockitoBean(answers = Answers.RETURNS_MOCKS)
  public SearchClientShim<?> searchClientShim;

  // Mock semantic search factories to avoid needing full configuration
  @MockitoBean public EmbeddingProviderFactory embeddingProviderFactory;

  @MockitoBean public SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockitoBean public SemanticSearchServiceFactory semanticSearchServiceFactory;
}
