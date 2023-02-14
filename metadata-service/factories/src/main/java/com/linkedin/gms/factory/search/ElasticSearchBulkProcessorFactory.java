package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Slf4j
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

  @Value("#{new Boolean('${elasticsearch.bulkProcessor.async}')}")
  private boolean async;

  @Value("${elasticsearch.bulkProcessor.refreshPolicy}")
  private String refreshPolicy;

  @Bean(name = "elasticSearchBulkProcessor")
  @Nonnull
  protected ESBulkProcessor getInstance() {
    return ESBulkProcessor.builder(searchClient)
            .async(async)
            .bulkFlushPeriod(bulkFlushPeriod)
            .bulkRequestsLimit(bulkRequestsLimit)
            .retryInterval(retryInterval)
            .numRetries(numRetries)
            .writeRequestRefreshPolicy(WriteRequest.RefreshPolicy.valueOf(refreshPolicy))
            .build();
  }
}
