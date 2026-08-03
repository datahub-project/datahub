package com.linkedin.metadata.kafka.config;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import com.linkedin.metadata.kafka.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.kafka.usage.DataHubUsageEventIndexer;
import com.linkedin.metadata.kafka.usage.ElasticsearchDataHubUsageEventIndexer;
import com.linkedin.metadata.kafka.usage.PostgresDataHubUsageEventIndexer;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class UsageEventsIndexingConfiguration {

  /**
   * Persists Kafka usage-events to Postgres or Elasticsearch based on analytics configuration.
   *
   * <p>{@code elasticsearchConnector} is optional via {@link ObjectProvider} so this configuration
   * still wires when {@code elasticsearch.enabled=false} (Postgres-only profiles). The check below
   * surfaces a clear error if the operator selects elasticsearch mode without the connector.
   */
  @Bean(name = "dataHubUsageEventIndexer")
  @Nonnull
  public DataHubUsageEventIndexer dataHubUsageEventIndexer(
      ConfigurationProvider configurationProvider,
      ObjectProvider<PostgresUsageEventsStore> postgresUsageEventsStore,
      ObjectProvider<ElasticsearchConnector> elasticsearchConnectorProvider,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    if (configurationProvider.getPlatformAnalytics().getUsageEvents().usePostgresql()) {
      PostgresUsageEventsStore store = postgresUsageEventsStore.getIfAvailable();
      if (store == null) {
        throw new IllegalStateException(
            "platformAnalytics.usageEvents.implementation is postgres but PostgresUsageEventsStore is not "
                + "available (ensure Postgres usage-events beans are wired and pgTimeseriesEbeanServer "
                + "exists — same postgres.pgTimeseries.pool as SqlSetup pgTimeseries / usage events).");
      }
      return new PostgresDataHubUsageEventIndexer(store, systemOperationContext);
    }

    log.debug("Usage events indexer: elasticsearch mode");
    ElasticsearchConnector elasticsearchConnector = elasticsearchConnectorProvider.getIfAvailable();
    if (elasticsearchConnector == null) {
      throw new IllegalStateException(
          "platformAnalytics.usageEvents.implementation is elasticsearch but ElasticsearchConnector "
              + "is not available (set elasticsearch.enabled=true or switch implementation to postgres).");
    }
    return new ElasticsearchDataHubUsageEventIndexer(elasticsearchConnector, indexConvention);
  }
}
