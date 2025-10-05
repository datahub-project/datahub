package com.linkedin.datahub.upgrade.loadindices.config;

import com.linkedin.datahub.upgrade.loadindices.LoadIndices;
import com.linkedin.datahub.upgrade.loadindices.LoadIndicesIndexManager;
import com.linkedin.datahub.upgrade.loadindices.NoOpKafkaEventProducer;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Slf4j
@Configuration
@Import(SystemAuthenticationFactory.class)
public class LoadIndicesConfig {

  /**
   * Provides a no-op KafkaEventProducer for LoadIndices upgrade operations. This prevents
   * connection attempts to Kafka during index loading.
   */
  @Bean(name = "kafkaEventProducer")
  @ConditionalOnMissingBean(name = "kafkaEventProducer")
  @Nonnull
  public KafkaEventProducer noOpKafkaEventProducer() {
    log.info("Creating NoOpKafkaEventProducer for LoadIndices upgrade operations");
    return new NoOpKafkaEventProducer();
  }

  @Bean(name = "loadIndicesIndexManager")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public LoadIndicesIndexManager createIndexManager(
      @Qualifier("systemOperationContext") final OperationContext systemOperationContext,
      @Qualifier("searchClientShim") SearchClientShim<?> searchClient,
      @Qualifier("elasticSearchIndexBuilder")
          final com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder indexBuilder)
      throws Exception {
    return new LoadIndicesIndexManager(
        searchClient, systemOperationContext.getSearchContext().getIndexConvention(), indexBuilder);
  }

  @Bean(name = "loadIndices")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public LoadIndices createInstance(
      final Database ebeanServer,
      final EntityService<?> entityService,
      final UpdateIndicesService updateIndicesService,
      @Qualifier("loadIndicesIndexManager") final LoadIndicesIndexManager indexManager,
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final AspectDao aspectDao) {
    return new LoadIndices(
        ebeanServer,
        entityService,
        updateIndicesService,
        indexManager,
        systemMetadataService,
        timeseriesAspectService,
        entitySearchService,
        graphService,
        aspectDao);
  }

  @Bean(name = "loadIndicesCassandra")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  public LoadIndices createNotImplInstance() {
    throw new IllegalStateException("loadIndices is not supported for cassandra!");
  }
}
