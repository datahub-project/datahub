package com.linkedin.datahub.upgrade.loadindices.config;

import com.linkedin.datahub.upgrade.loadindices.LoadIndices;
import com.linkedin.datahub.upgrade.loadindices.LoadIndicesIndexManager;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.SystemMetadataServiceImplementation;
import com.linkedin.metadata.entity.AspectDao;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Slf4j
@Configuration
@Import(SystemAuthenticationFactory.class)
public class LoadIndicesConfig {

  @Bean(name = "loadIndicesIndexManager")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public LoadIndicesIndexManager createIndexManager(
      @Qualifier("systemOperationContext") final OperationContext systemOperationContext,
      @Qualifier("searchClientShim") SearchClientShim<?> searchClient,
      @Qualifier("elasticSearchIndexBuilder")
          final com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder indexBuilder,
      final ConfigurationProvider configurationProvider)
      throws Exception {
    boolean includeSystemMetadataEsIndex =
        configurationProvider.getSystemMetadataService().getImplementation()
            != SystemMetadataServiceImplementation.postgres;
    return new LoadIndicesIndexManager(
        searchClient,
        systemOperationContext.getSearchContext().getIndexConvention(),
        indexBuilder,
        includeSystemMetadataEsIndex);
  }

  @Bean(name = "loadIndices")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public LoadIndices createInstance(
      final Database ebeanServer,
      final UpdateIndicesService updateIndicesService,
      @Qualifier("loadIndicesIndexManager") final LoadIndicesIndexManager indexManager,
      final SystemMetadataService systemMetadataService,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final GraphService graphService,
      final AspectDao aspectDao) {
    return new LoadIndices(
        ebeanServer,
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
