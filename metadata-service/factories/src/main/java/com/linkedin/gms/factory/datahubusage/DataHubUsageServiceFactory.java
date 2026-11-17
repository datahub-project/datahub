package com.linkedin.gms.factory.datahubusage;

import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.datahubusage.DataHubUsageService;
import com.linkedin.metadata.datahubusage.DataHubUsageServiceImpl;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class DataHubUsageServiceFactory {

  @Bean
  public DataHubUsageService dataHubUsageService(
      IndexConvention indexConvention, SearchClusterRegistry searchClusterRegistry) {
    return new DataHubUsageServiceImpl(
        searchClusterRegistry.clientFor(SearchComponent.USAGE), indexConvention);
  }
}
