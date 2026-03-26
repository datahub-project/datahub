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
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
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

  @LocalServerPort private int port;

  @MockitoBean public KafkaHealthChecker kafkaHealthChecker;

  @MockitoBean public EntityService<?> _entityService;

  @MockitoBean(answers = Answers.RETURNS_MOCKS)
  public SearchClientShim<?> searchClientShim;

  @Bean
  @Primary
  public SystemEntityClient systemEntityClient(
      @Qualifier("configurationProvider") final ConfigurationProvider configurationProvider,
      final EntityClientConfig entityClientConfig,
      final MetricUtils metricUtils) {
    String selfUri = "http://localhost:" + port;
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

  @MockitoBean public Database ebeanServer;

  @MockitoBean protected TimeseriesAspectService timeseriesAspectService;

  @MockitoBean protected EntityRegistry entityRegistry;

  // Use @Bean @Primary to prevent ConfigEntityRegistryFactory from loading entity-registry.yml
  // See: https://github.com/spring-projects/spring-framework/issues/33934
  @Bean
  @Primary
  public ConfigEntityRegistry configEntityRegistry() {
    return Mockito.mock(ConfigEntityRegistry.class);
  }

  @MockitoBean protected SiblingGraphService siblingGraphService;

  // Mock semantic search factories to avoid needing full configuration
  @MockitoBean public EmbeddingProviderFactory embeddingProviderFactory;

  @MockitoBean public SemanticEntitySearchServiceFactory semanticEntitySearchServiceFactory;

  @MockitoBean public SemanticSearchServiceFactory semanticSearchServiceFactory;
}
