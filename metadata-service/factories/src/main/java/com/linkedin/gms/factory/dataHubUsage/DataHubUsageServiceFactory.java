package com.linkedin.gms.factory.dataHubUsage;

import com.linkedin.metadata.dataHubUsage.DataHubUsageServiceImpl;
import com.linkedin.metadata.datahubusage.DataHubUsageService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class DataHubUsageServiceFactory {

  @Bean
  public DataHubUsageService dataHubUsageService(
      SearchClientShim<?> elasticClient, IndexConvention indexConvention, MetricUtils metricUtils) {
    return new DataHubUsageServiceImpl(elasticClient, indexConvention, metricUtils);
  }
}
