package com.linked.metadata.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.linkedin.common.factory.RestHighLevelClientFactory;
import com.linkedin.metadata.builders.search.RegisteredIndexBuilders;
import com.linkedin.metadata.builders.search.SnapshotProcessor;
import com.linkedin.metadata.utils.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.utils.elasticsearch.ElasticsearchConnectorFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnableAutoConfiguration
@Import({RestHighLevelClientFactory.class})
public class ElasticSearchConfig {

    @Value("${ELASTICSEARCH_HOST:localhost}")
    private String elasticSearchHost;
    @Value("${ELASTICSEARCH_PORT:9200}")
    private int elasticSearchPort;

    @Bean
    public ElasticsearchConnector elasticSearchConnector() {
        ElasticsearchConnector elasticSearchConnector = ElasticsearchConnectorFactory.createInstance(
                elasticSearchHost,
                elasticSearchPort
        );
        log.info("ElasticSearchConnector built successfully");
        return elasticSearchConnector;
    }

    @Bean
    public SnapshotProcessor snapshotProcessor() {
        return new SnapshotProcessor(RegisteredIndexBuilders.REGISTERED_INDEX_BUILDERS);
    }
}
