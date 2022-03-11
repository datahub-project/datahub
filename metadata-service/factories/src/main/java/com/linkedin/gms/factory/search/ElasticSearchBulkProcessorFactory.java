package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import javax.annotation.Nonnull;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@Import({RestHighLevelClientFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ElasticSearchBulkProcessorFactory {
  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Value("${elasticsearch.bulkProcessor.requestsLimit}")
  private Integer bulkRequestsLimit;

  @Value("${elasticsearch.bulkProcessor.flushPeriod}")
  private Integer bulkFlushPeriod;

  @Value("${elasticsearch.bulkProcessor.numRetries}")
  private Integer numRetries;

  @Value("${elasticsearch.bulkProcessor.retryInterval}")
  private Long retryInterval;

  @Bean(name = "elasticSearchBulkProcessor")
  @Nonnull
  protected BulkProcessor getInstance() {
    return BulkProcessor.builder((request, bulkListener) -> {
      searchClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
    }, BulkListener.getInstance())
        .setBulkActions(bulkRequestsLimit)
        .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
        .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(retryInterval), numRetries))
        .build();
  }
}
