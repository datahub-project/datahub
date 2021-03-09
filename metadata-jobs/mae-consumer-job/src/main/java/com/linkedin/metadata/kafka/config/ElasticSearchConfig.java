package com.linkedin.metadata.kafka.config;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.gms.factory.common.ElasticsearchSSLContextFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.metadata.builders.search.BaseIndexBuilder;
import com.linkedin.metadata.builders.search.SnapshotProcessor;
import com.linkedin.metadata.utils.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.utils.elasticsearch.ElasticsearchConnectorFactory;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Slf4j
@Configuration
@Import({RestHighLevelClientFactory.class, IndexBuildersConfig.class, ElasticsearchSSLContextFactory.class})
public class ElasticSearchConfig {

  @Value("${ELASTICSEARCH_HOST:localhost}")
  private String elasticSearchHost;
  @Value("${ELASTICSEARCH_PORT:9200}")
  private int elasticSearchPort;

  @Bean
  public ElasticsearchConnector elasticSearchConnector() {
    final ElasticsearchConnector elasticSearchConnector =
        ElasticsearchConnectorFactory.createInstance(elasticSearchHost, elasticSearchPort);
    log.info("ElasticSearchConnector built successfully");
    return elasticSearchConnector;
  }

  @Bean
  public SnapshotProcessor snapshotProcessor(@Nonnull Set<BaseIndexBuilder<? extends RecordTemplate>> indexBuilders) {
    return new SnapshotProcessor(indexBuilders);
  }
}
