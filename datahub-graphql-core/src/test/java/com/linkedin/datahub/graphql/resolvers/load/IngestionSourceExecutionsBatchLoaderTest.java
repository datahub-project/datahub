package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import org.testng.annotations.Test;

public class IngestionSourceExecutionsBatchLoaderTest {

  @Test
  public void testBatchLoadMapsResultsIncludingEmptyGroups() throws Exception {
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
    filterResults.put(
        sourceMissing,
        new SearchResult()
            .setFrom(0)
            .setPageSize(1)
            .setNumEntities(0)
            .setEntities(new SearchEntityArray()));

    when(mockClient.searchLatestPerGroup(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq("ingestionSource"),
            eq(List.of(sourceWithRun, sourceMissing)),
            any()))
        .thenReturn(filterResults);

    IngestionSourceExecutionsBatchLoader loader =
        new IngestionSourceExecutionsBatchLoader(mockClient);

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
    assertEquals((int) missing.getTotal(), 0);
    assertTrue(missing.getExecutionRequests().isEmpty());

    verify(mockClient, times(1))
        .searchLatestPerGroup(
            any(),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            eq("ingestionSource"),
            eq(List.of(sourceWithRun, sourceMissing)),
            any());
    verify(mockClient, never())
        .filter(any(), anyString(), any(), anyList(), anyInt(), nullable(Integer.class));
  }

  @Test
  public void testSingleKeyUsesFilterNotAggregation() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    when(mockClient.filter(any(), anyString(), any(), anyList(), eq(0), eq(1)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(
                            new SearchEntity().setEntity(TEST_EXECUTION_REQUEST_URN)))));

    IngestionSourceExecutionsBatchLoader loader =
        new IngestionSourceExecutionsBatchLoader(mockClient);

    List<IngestionSourceExecutionRequests> results =
        loader.batchLoad(List.of(TEST_INGESTION_SOURCE_URN.toString()), mockContext);

    assertEquals(results.size(), 1);
    assertEquals((int) results.get(0).getTotal(), 1);
    verify(mockClient, times(1)).filter(any(), anyString(), any(), anyList(), eq(0), eq(1));
    verify(mockClient, never())
        .searchLatestPerGroup(any(), anyString(), anyString(), anyCollection(), any());
  }
}
