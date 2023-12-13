package com.linkedin.gms.factory.entity;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.DataHubKafkaProducerFactory;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.client.SystemJavaEntityClient;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnExpression("'${entityClient.preferredImpl:java}'.equals('java')")
@Import({DataHubKafkaProducerFactory.class})
public class JavaEntityClientFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("deleteEntityService")
  private DeleteEntityService _deleteEntityService;

  @Autowired
  @Qualifier("searchService")
  private SearchService _searchService;

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService _entitySearchService;

  @Autowired
  @Qualifier("cachingEntitySearchService")
  private CachingEntitySearchService _cachingEntitySearchService;

  @Autowired
  @Qualifier("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Autowired
  @Qualifier("relationshipSearchService")
  private LineageSearchService _lineageSearchService;

  @Autowired
  @Qualifier("kafkaEventProducer")
  private EventProducer _eventProducer;

  @Bean("javaEntityClient")
  public JavaEntityClient getJavaEntityClient(
      @Qualifier("restliEntityClient") final RestliEntityClient restliEntityClient) {
    return new JavaEntityClient(
        _entityService,
        _deleteEntityService,
        _entitySearchService,
        _cachingEntitySearchService,
        _searchService,
        _lineageSearchService,
        _timeseriesAspectService,
        _eventProducer,
        restliEntityClient);
  }

  @Bean("systemJavaEntityClient")
  public SystemJavaEntityClient systemJavaEntityClient(
      @Qualifier("configurationProvider") final ConfigurationProvider configurationProvider,
      @Qualifier("systemAuthentication") final Authentication systemAuthentication,
      @Qualifier("systemRestliEntityClient") final RestliEntityClient restliEntityClient) {
    SystemJavaEntityClient systemJavaEntityClient =
        new SystemJavaEntityClient(
            _entityService,
            _deleteEntityService,
            _entitySearchService,
            _cachingEntitySearchService,
            _searchService,
            _lineageSearchService,
            _timeseriesAspectService,
            _eventProducer,
            restliEntityClient,
            systemAuthentication,
            configurationProvider.getCache().getClient().getEntityClient());

    _entityService.setSystemEntityClient(systemJavaEntityClient);

    return systemJavaEntityClient;
  }
}
