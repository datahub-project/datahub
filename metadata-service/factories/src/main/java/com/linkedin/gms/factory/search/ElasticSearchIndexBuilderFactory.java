package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
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
@Import({RestHighLevelClientFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ElasticSearchIndexBuilderFactory {
  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Value("${elasticsearch.index.numShards}")
  private Integer numShards;

  @Value("${elasticsearch.index.numReplicas}")
  private Integer numReplicas;

  @Value("${elasticsearch.index.numRetries}")
  private Integer numRetries;

  @Bean(name = "elasticSearchIndexBuilder")
  @Nonnull
  protected ESIndexBuilder getInstance() {
    return new ESIndexBuilder(searchClient, numShards, numReplicas, numRetries);
  }
}