package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.IngestionSourceExecutionRequests;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class LatestIngestionSourceExecutionsBatchLoaderTest {

  @Test
  public void testBatchLoadMapsResultsAndMissingKeysAsTotalZero() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    String sourceWithRun = TEST_INGESTION_SOURCE_URN.toString();
    String sourceMissing = "urn:li:dataHubIngestionSource:missing";

    Map<String, SearchResult> filterResults = new HashMap<>();
    filterResults.put(
        sourceWithRun,
        new SearchResult()
            .setFrom(0)
            .setPageSize(1)
            .setNumEntities(3)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(TEST_EXECUTION_REQUEST_URN)))));

    when(mockClient.filterLatestByValues(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq("ingestionSource"),
            eq(List.of(sourceWithRun, sourceMissing)),
            any(),
            eq(1)))
        .thenReturn(filterResults);

    LatestIngestionSourceExecutionsBatchLoader loader =
        new LatestIngestionSourceExecutionsBatchLoader(mockClient);

    List<IngestionSourceExecutionRequests> results =
        loader.batchLoad(List.of(sourceWithRun, sourceMissing), mockContext);

    assertEquals(results.size(), 2);

    IngestionSourceExecutionRequests withRun = results.get(0);
    assertEquals((int) withRun.getStart(), 0);
    assertEquals((int) withRun.getCount(), 1);
    assertEquals((int) withRun.getTotal(), 3);
    assertEquals(withRun.getExecutionRequests().size(), 1);
    assertEquals(
        withRun.getExecutionRequests().get(0).getUrn(), TEST_EXECUTION_REQUEST_URN.toString());
    assertEquals(withRun.getExecutionRequests().get(0).getType(), EntityType.EXECUTION_REQUEST);

    IngestionSourceExecutionRequests missing = results.get(1);
    assertEquals((int) missing.getStart(), 0);
    assertEquals((int) missing.getCount(), 1);
    assertEquals((int) missing.getTotal(), 0);
    assertTrue(missing.getExecutionRequests().isEmpty());

    Mockito.verify(mockClient, Mockito.times(1))
        .filterLatestByValues(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq("ingestionSource"),
            eq(List.of(sourceWithRun, sourceMissing)),
            any(),
            eq(1));
  }
}
