package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({RestHighLevelClientFactory.class, IndexConventionFactory.class})
public class ElasticSearchSystemMetadataServiceFactory {
  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Value("${elasticsearch.bulkProcessor.requestsLimit}")
  private Integer bulkRequestsLimit;

  @Value("${elasticsearch.bulkProcessor.flushPeriod}")
  private Integer bulkFlushPeriod;

  @Value("${elasticsearch.bulkProcessor.numRetries}")
  private Integer numRetries;

  @Value("${elasticsearch.bulkProcessor.retryInterval}")
  private Long retryInterval;

  @Bean(name = "elasticSearchSystemMetadataService")
  @Nonnull
  protected ElasticSearchSystemMetadataService getInstance() {
    return new ElasticSearchSystemMetadataService(
        searchClient,
        indexConvention,
        new ESSystemMetadataDAO(
            searchClient,
            indexConvention,
            bulkRequestsLimit,
            bulkFlushPeriod,
            numRetries,
            retryInterval)
    );
  }
}
