package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({RestHighLevelClientFactory.class, IndexConventionFactory.class})
public class ElasticSearchGraphServiceFactory {
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

  @Bean(name = "elasticSearchGraphService")
  @Nonnull
  protected ElasticSearchGraphService getInstance() {
    return new ElasticSearchGraphService(
        searchClient,
        indexConvention,
        new ESGraphWriteDAO(
            searchClient,
            indexConvention,
            bulkRequestsLimit,
            bulkFlushPeriod,
            numRetries,
            retryInterval),
        new ESGraphQueryDAO(searchClient, indexConvention));
  }
}
