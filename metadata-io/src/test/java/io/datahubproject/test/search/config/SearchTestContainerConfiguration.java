package io.datahubproject.test.search.config;

import static io.datahubproject.test.search.BulkProcessorTestUtils.replaceBulkProcessorListener;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.version.GitVersion;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.testcontainers.containers.GenericContainer;

/**
 * This configuration is for `test containers` it builds these objects tied to the test container
 * instantiated for tests. Could be ES or OpenSearch, etc.
 *
 * <p>Does your test required a running instance? If no, {@link
 * io.datahubproject.test.search.config.SearchCommonTestConfiguration} instead.
 */
@TestConfiguration
public class SearchTestContainerConfiguration {
  // This port is overridden by the specific test container instance
  private static final int HTTP_PORT = 9200;
  public static final int REFRESH_INTERVAL_SECONDS = 5;

  @Primary
  @Bean(name = "searchRestHighLevelClient")
  @Nonnull
  public RestHighLevelClient getElasticsearchClient(
      @Qualifier("testSearchContainer") GenericContainer<?> searchContainer) {
    // A helper method to create a search test container defaulting to the current image and
    // version, with the ability
    // within firewalled environments to override with an environment variable to point to the
    // offline repository.
    // A helper method to construct a standard rest client for search.
    final RestClientBuilder builder =
        RestClient.builder(
                new HttpHost("localhost", searchContainer.getMappedPort(HTTP_PORT), "http"))
            .setHttpClientConfigCallback(
                httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultIOReactorConfig(
                        IOReactorConfig.custom().setIoThreadCount(1).build()));

    builder.setRequestConfigCallback(
        requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(30000));

    return new RestHighLevelClient(builder);
  }

  /*
   Cannot use the factory class without circular dependencies
  */
  @Primary
  @Bean(name = "searchBulkProcessor")
  @Nonnull
  public ESBulkProcessor getBulkProcessor(
      @Qualifier("searchRestHighLevelClient") RestHighLevelClient searchClient) {
    ESBulkProcessor esBulkProcessor =
        ESBulkProcessor.builder(searchClient)
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
    replaceBulkProcessorListener(esBulkProcessor);
    return esBulkProcessor;
  }

  @Primary
  @Bean(name = "searchIndexBuilder")
  @Nonnull
  protected ESIndexBuilder getIndexBuilder(
      @Qualifier("searchRestHighLevelClient") RestHighLevelClient searchClient) {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    return new ESIndexBuilder(
        searchClient,
        1,
        1,
        3,
        1,
        Map.of(),
        false,
        false,
        false,
        new ElasticSearchConfiguration(),
        gitVersion);
  }
}
