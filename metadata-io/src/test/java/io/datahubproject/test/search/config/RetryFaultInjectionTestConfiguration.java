package io.datahubproject.test.search.config;

import static io.datahubproject.test.search.BulkProcessorTestUtils.replaceBulkProcessorListener;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.FaultInjectingSearchClientShim;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.opensearch.action.support.WriteRequest;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.testcontainers.containers.GenericContainer;

/**
 * Test configuration that wraps the real SearchClientShim with {@link
 * FaultInjectingSearchClientShim} so retry logic (count retry, createIndex retry) can be exercised
 * against real ES/OpenSearch containers. Tests set the fault spec per test via {@link
 * io.datahubproject.test.search.FaultSpec.Holder#set(FaultSpec)} in @BeforeMethod and clear it
 * in @AfterMethod. The bulk processor is built with the real shim so replaceBulkProcessorListener
 * works.
 */
@TestConfiguration
public class RetryFaultInjectionTestConfiguration extends SearchTestContainerConfiguration {

  @Override
  @Nonnull
  public SearchClientShim<?> getElasticsearchClient(
      @Qualifier("testSearchContainer") GenericContainer<?> searchContainer) throws IOException {
    SearchClientShim<?> realShim = super.getElasticsearchClient(searchContainer);
    return new FaultInjectingSearchClientShim(realShim);
  }

  @Override
  @Primary
  @Bean(name = "searchBulkProcessor")
  @Nonnull
  public ESBulkProcessor getBulkProcessor(
      @Qualifier("searchClientShim") SearchClientShim<?> searchClient) {
    SearchClientShim<?> shimForBulk =
        searchClient instanceof FaultInjectingSearchClientShim
            ? ((FaultInjectingSearchClientShim) searchClient).getDelegate()
            : searchClient;
    ESBulkProcessor esBulkProcessor =
        ESBulkProcessor.builder(shimForBulk, null)
            .async(true)
            .writeRequestRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .bulkRequestsLimit(10000)
            .bulkFlushPeriod(REFRESH_INTERVAL_SECONDS - 1)
            .retryInterval(1L)
            .numRetries(1)
            .build();
    replaceBulkProcessorListener(esBulkProcessor);
    return esBulkProcessor;
  }
}
