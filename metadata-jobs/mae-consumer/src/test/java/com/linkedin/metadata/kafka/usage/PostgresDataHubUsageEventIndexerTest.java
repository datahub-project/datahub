package com.linkedin.metadata.kafka.usage;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.kafka.transformer.DataHubUsageEventTransformer;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.testng.annotations.Test;

public class PostgresDataHubUsageEventIndexerTest {

  @Test
  public void indexBatch_skipsMalformedTimestampsWithoutFailingBatch() throws Exception {
    PostgresAnalyticsStore store = mock(PostgresAnalyticsStore.class);
    PgAnalyticsStoreRegistry.StoreHandle handle = mock(PgAnalyticsStoreRegistry.StoreHandle.class);
    when(handle.getStore()).thenReturn(store);
    PgAnalyticsStoreRegistry registry = mock(PgAnalyticsStoreRegistry.class);
    when(registry.resolve(AnalyticsMetricFamilies.DATAHUB_USAGE)).thenReturn(handle);

    String badJson =
        "{\"type\":\"SearchEvent\",\"timestamp\":\"not-a-timestamp\",\"actorUrn\":\"urn:li:corpuser:x\"}";
    DataHubUsageEventIndexer.IndexableUsageEvent badEvent =
        new DataHubUsageEventIndexer.IndexableUsageEvent(
            new DataHubUsageEventTransformer.TransformedDocument("doc-1", badJson), "doc-1");

    PostgresDataHubUsageEventIndexer indexer = new PostgresDataHubUsageEventIndexer(registry);
    indexer.indexBatch(
        TestOperationContexts.systemContextNoSearchAuthorization(), List.of(badEvent));

    verify(store, never()).insertEvents(anyList());
  }
}
