package com.linkedin.gms.factory.dataHubUsage;

import com.linkedin.metadata.dataHubUsage.DataHubUsageServiceImpl;
import com.linkedin.metadata.datahubusage.DataHubUsageService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class DataHubUsageServiceFactory {

  @Bean
  public DataHubUsageService dataHubUsageService(
      RestHighLevelClient elasticClient, IndexConvention indexConvention) {
    return new DataHubUsageServiceImpl(elasticClient, indexConvention);
  }
}
