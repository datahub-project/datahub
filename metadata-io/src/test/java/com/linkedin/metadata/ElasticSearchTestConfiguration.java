package com.linkedin.metadata;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import javax.annotation.Nonnull;

import java.util.Map;


@TestConfiguration
public class ElasticSearchTestConfiguration {
    private static final int HTTP_PORT = 9200;
    private static final int REFRESH_INTERVAL_SECONDS = 1;

    public static void syncAfterWrite() throws InterruptedException {
        Thread.sleep(REFRESH_INTERVAL_SECONDS * 1000);
    }

    @Scope("singleton")
    @Bean(name = "testElasticsearchContainer")
    @Nonnull
    public ElasticsearchContainer elasticsearchContainer() {
        ElasticTestUtils.ES_CONTAINER.start();
        return ElasticTestUtils.ES_CONTAINER;
    }

    @Primary
    @Scope("singleton")
    @Bean(name = "elasticSearchRestHighLevelClient")
    @Nonnull
    public RestHighLevelClient getElasticsearchClient(@Qualifier("testElasticsearchContainer") ElasticsearchContainer esContainer) {
        // A helper method to create an ElasticseachContainer defaulting to the current image and version, with the ability
        // within firewalled environments to override with an environment variable to point to the offline repository.
        // A helper method to construct a standard rest client for Elastic search.
        final RestClientBuilder builder =
                RestClient.builder(new HttpHost(
                        "localhost",
                        esContainer.getMappedPort(HTTP_PORT), "http")
                ).setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(1).build()));

        builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
                setConnectionRequestTimeout(30000));

        return new RestHighLevelClient(builder);
    }

    /*
      Cannot use the factory class without circular dependencies
     */
    @Primary
    @Bean(name = "elasticSearchBulkProcessor")
    @Nonnull
    public ESBulkProcessor getBulkProcessor(@Qualifier("elasticSearchRestHighLevelClient") RestHighLevelClient searchClient) {
        return ESBulkProcessor.builder(searchClient)
                .async(true)
                /*
                 * Force a refresh as part of this request. This refresh policy does not scale for high indexing or search throughput but is useful
                 * to present a consistent view to for indices with very low traffic. And it is wonderful for tests!
                 */
                .writeRequestRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .bulkRequestsLimit(1000)
                .bulkFlushPeriod(REFRESH_INTERVAL_SECONDS - 1)
                .retryInterval(1L)
                .numRetries(1)
                .build();
    }

    @Primary
    @Bean(name = "elasticSearchIndexBuilder")
    @Nonnull
    public ESIndexBuilder getIndexBuilder(@Qualifier("elasticSearchRestHighLevelClient") RestHighLevelClient searchClient) {
        return new ESIndexBuilder(searchClient, 1, 1, 3, 1, Map.of(), false);
    }
}
