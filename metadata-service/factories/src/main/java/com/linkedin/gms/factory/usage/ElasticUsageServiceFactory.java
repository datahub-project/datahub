package com.linkedin.gms.factory.usage;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.metadata.usage.elasticsearch.ElasticUsageService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.Nonnull;


@Configuration
@Import({RestHighLevelClientFactory.class, IndexConventionFactory.class})
public class ElasticUsageServiceFactory {
  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Value("${ES_BULK_REQUESTS_LIMIT:1}")
  private Integer bulkRequestsLimit;

  @Value("${ES_BULK_FLUSH_PERIOD:1}")
  private Integer bulkFlushPeriod;

  @Value("${ES_BULK_NUM_RETRIES:3}")
  private Integer numRetries;

  @Value("${ES_BULK_RETRY_INTERVAL:1}")
  private Long retryInterval;

  @Bean(name = "elasticUsageService")
  @Nonnull
  protected ElasticUsageService getInstance() {
    return new ElasticUsageService(searchClient, indexConvention,
            bulkRequestsLimit, bulkFlushPeriod, numRetries, retryInterval);
  }
}
