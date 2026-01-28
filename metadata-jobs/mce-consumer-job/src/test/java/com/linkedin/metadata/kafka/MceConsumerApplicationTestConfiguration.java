package com.linkedin.metadata.kafka;

import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.entity.client.SystemRestliEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.context.SystemOperationContextFactory;
import com.linkedin.gms.factory.search.SemanticSearchServiceFactory;
import com.linkedin.gms.factory.search.semantic.EmbeddingProviderFactory;
import com.linkedin.gms.factory.search.semantic.SemanticEntitySearchServiceFactory;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import com.linkedin.restli.client.Client;
import io.ebean.Database;
import java.net.URI;
import org.mockito.Answers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@TestConfiguration
@Import(
    value = {
      SystemAuthenticationFactory.class,
      SystemOperationContextFactory.class,
    })
public class MceConsumerApplicationTestConfiguration {

  @Autowired private TestRestTemplate restTemplate;

  @MockBean public KafkaHealthChecker kafkaHealthChecker;

  @MockBean public EntityService<?> _entityService;

  @MockBean(answer = Answers.RETURNS_MOCKS)
  public SearchClientShim<?> searchClientShim;

  @Bean
  @Primary
  public SystemEntityClient systemEntityClient(
      @Qualifier("configurationProvider") final ConfigurationProvider configurationProvider,
      final EntityClientConfig entityClientConfig,
      final MetricUtils metricUtils) {
    String selfUri = restTemplate.getRootUri();
    final Client restClient = DefaultRestliClientFactory.getRestLiClient(URI.create(selfUri), null);
    return new SystemRestliEntityClient(
        restClient,
        entityClientConfig,
        configurationProvider.getCache().getClient().getEntityClient(),
        metricUtils);
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

  @MockBean public Database ebeanServer;

  @MockBean protected TimeseriesAspectService timeseriesAspectService;

  @MockBean protected EntityRegistry entityRegistry;

  @MockBean protected ConfigEntityRegistry configEntityRegistry;

  @MockBean protected SiblingGraphService siblingGraphService;

  // Mock semantic search factories to avoid needing full configuration
  @MockBean public EmbeddingProviderFactory embeddingProviderFactory;

  @MockBean public SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockBean public SemanticSearchServiceFactory semanticSearchServiceFactory;
}
