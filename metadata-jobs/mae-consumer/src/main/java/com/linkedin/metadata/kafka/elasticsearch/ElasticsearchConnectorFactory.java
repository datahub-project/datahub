package com.linkedin.metadata.kafka.elasticsearch;

import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.SearchComponent;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ElasticsearchConnectorFactory {

  @Bean(name = "elasticsearchConnector")
  @Nonnull
  public ElasticsearchConnector createInstance(SearchClusterRegistry searchClusterRegistry) {
    return new ElasticsearchConnector(
        searchClusterRegistry.bulkProcessorFor(SearchComponent.USAGE),
        searchClusterRegistry.configFor(SearchComponent.USAGE).getBulkProcessor().getNumRetries());
  }
}
