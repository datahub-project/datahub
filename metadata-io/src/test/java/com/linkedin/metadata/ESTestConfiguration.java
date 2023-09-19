package com.linkedin.metadata;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.version.GitVersion;
import java.util.Optional;
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
public class ESTestConfiguration {
    private static final int HTTP_PORT = 9200;
    public static final int REFRESH_INTERVAL_SECONDS = 5;

    public static void syncAfterWrite(ESBulkProcessor bulkProcessor) throws InterruptedException {
        bulkProcessor.flush();
        Thread.sleep(1000);
    }

    @Bean
    public SearchConfiguration searchConfiguration() {
        SearchConfiguration searchConfiguration = new SearchConfiguration();
        searchConfiguration.setMaxTermBucketSize(20);

        ExactMatchConfiguration exactMatchConfiguration = new ExactMatchConfiguration();
        exactMatchConfiguration.setExclusive(false);
        exactMatchConfiguration.setExactFactor(10.0f);
        exactMatchConfiguration.setWithPrefix(true);
        exactMatchConfiguration.setPrefixFactor(6.0f);
        exactMatchConfiguration.setCaseSensitivityFactor(0.7f);
        exactMatchConfiguration.setEnableStructured(true);

        WordGramConfiguration wordGramConfiguration = new WordGramConfiguration();
        wordGramConfiguration.setTwoGramFactor(1.2f);
        wordGramConfiguration.setThreeGramFactor(1.5f);
        wordGramConfiguration.setFourGramFactor(1.8f);

        PartialConfiguration partialConfiguration = new PartialConfiguration();
        partialConfiguration.setFactor(0.4f);
        partialConfiguration.setUrnFactor(0.5f);

        searchConfiguration.setExactMatch(exactMatchConfiguration);
        searchConfiguration.setWordGram(wordGramConfiguration);
        searchConfiguration.setPartial(partialConfiguration);
        return searchConfiguration;
    }

    @Bean
    public CustomSearchConfiguration customSearchConfiguration() throws Exception {
        CustomConfiguration customConfiguration = new CustomConfiguration();
        customConfiguration.setEnabled(true);
        customConfiguration.setFile("search_config_builder_test.yml");
        return customConfiguration.resolve(new YAMLMapper());
    }

    @Scope("singleton")
    @Bean(name = "testElasticsearchContainer")
    @Nonnull
    public ElasticsearchContainer elasticsearchContainer() {
        ESTestUtils.ES_CONTAINER.start();
        return ESTestUtils.ES_CONTAINER;
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
                .bulkRequestsLimit(10000)
                .bulkFlushPeriod(REFRESH_INTERVAL_SECONDS - 1)
                .retryInterval(1L)
                .numRetries(1)
                .build();
    }

    @Primary
    @Bean(name = "elasticSearchIndexBuilder")
    @Nonnull
    protected ESIndexBuilder getIndexBuilder(@Qualifier("elasticSearchRestHighLevelClient") RestHighLevelClient searchClient) {
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        return new ESIndexBuilder(searchClient, 1, 1, 3, 1, Map.of(),
                false, false,
                new ElasticSearchConfiguration(), gitVersion);
    }

    @Bean(name = "entityRegistry")
    public EntityRegistry entityRegistry() throws EntityRegistryException {
        return new ConfigEntityRegistry(
                ESTestConfiguration.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    }

    @Bean(name = "longTailEntityRegistry")
    public EntityRegistry longTailEntityRegistry() throws EntityRegistryException {
        return new ConfigEntityRegistry(
                ESTestConfiguration.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    }
}
