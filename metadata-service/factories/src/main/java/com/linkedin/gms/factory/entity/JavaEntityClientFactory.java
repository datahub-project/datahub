package com.linkedin.gms.factory.entity;

import com.linkedin.entity.client.JavaEntityClient;
import com.linkedin.gms.factory.kafka.DataHubKafkaProducerFactory;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
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
  public JavaEntityClient getJavaEntityClient() {
    return new JavaEntityClient(
        _entityService,
        _deleteEntityService,
        _entitySearchService,
        _cachingEntitySearchService,
        _searchService,
        _lineageSearchService,
        _timeseriesAspectService,
        _eventProducer);
  }
}
