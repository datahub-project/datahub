package com.linkedin.datahub.graphql.resolvers.ingest.source;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListIngestionSourceResolverTest {

  private static final ListIngestionSourcesInput TEST_INPUT =
      new ListIngestionSourcesInput(0, 20, null, null, null);

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq(""),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_INGESTION_SOURCE_URN)))));

    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    var result = resolver.get(mockEnv).get();

    // Data Assertions
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getIngestionSources().size(), 1);

    assertEquals(
        result.getIngestionSources().get(0).getUrn(), TEST_INGESTION_SOURCE_URN.toString());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    Mockito.verify(mockClient, Mockito.times(0))
        .search(
            any(),
            Mockito.any(),
            Mockito.eq(""),
            Mockito.anyMap(),
            Mockito.anyInt(),
            Mockito.anyInt());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  void testBuildSortCriteriaForNameField() {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    com.linkedin.datahub.graphql.generated.SortCriterion input =
        new com.linkedin.datahub.graphql.generated.SortCriterion();
    input.setField("name");
    input.setSortOrder(com.linkedin.datahub.graphql.generated.SortOrder.ASCENDING);

    List<SortCriterion> result = resolver.buildSortCriteria(input);

    assertEquals(2, result.size());
    assertEquals("type", result.get(0).getField());
    assertEquals(SortOrder.ASCENDING, result.get(0).getOrder());
    assertEquals("name", result.get(1).getField());
    assertEquals(SortOrder.ASCENDING, result.get(1).getOrder());
  }

  @Test
  void testBuildSortCriteriaForNonNameField() {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    com.linkedin.datahub.graphql.generated.SortCriterion input =
        new com.linkedin.datahub.graphql.generated.SortCriterion();
    input.setField("createdAt");
    input.setSortOrder(com.linkedin.datahub.graphql.generated.SortOrder.DESCENDING);

    List<SortCriterion> result = resolver.buildSortCriteria(input);

    assertEquals(1, result.size());
    assertEquals("createdAt", result.get(0).getField());
    assertEquals(SortOrder.DESCENDING, result.get(0).getOrder());
  }
}
