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
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import io.ebean.Database;
import org.mockito.Answers;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

@TestConfiguration
@Import(value = {SystemAuthenticationFactory.class})
public class MaeConsumerApplicationTestConfiguration {

  @MockBean private KafkaHealthChecker kafkaHealthChecker;

  @MockBean private EntityServiceImpl _entityServiceImpl;

  @MockBean private Database ebeanServer;

  @MockBean private EntityRegistry entityRegistry;

  @MockBean private RestrictedService restrictedService;

  @MockBean private SecretService secretService;

  @MockBean private GraphService _graphService;

  @MockBean private ElasticSearchSystemMetadataService _elasticSearchSystemMetadataService;

  @MockBean private ConfigEntityRegistry _configEntityRegistry;

  @MockBean public ElasticSearchService elasticSearchService;

  @MockBean public MetricUtils metricUtils;

  @MockBean(answer = Answers.RETURNS_MOCKS)
  public SearchClientShim<?> searchClientShim;

  // Mock semantic search factories to avoid needing full configuration
  @MockBean public EmbeddingProviderFactory embeddingProviderFactory;

  @MockBean public SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockBean public SemanticSearchServiceFactory semanticSearchServiceFactory;
}
