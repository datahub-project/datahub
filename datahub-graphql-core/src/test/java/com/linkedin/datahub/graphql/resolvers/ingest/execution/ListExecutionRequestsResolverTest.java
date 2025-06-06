package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.buildFilter;
import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListExecutionRequestsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListExecutionRequestsResolverTest {

  private static final String TEST_EXECUTION_REQUEST_URN = "urn:li:executionRequest:test-execution";

  private static final ListExecutionRequestsInput TEST_INPUT =
      new ListExecutionRequestsInput(0, 20, null, null, null, null);

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock search response
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("*"),
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
                        ImmutableSet.of(
                            new SearchEntity()
                                .setEntity(Urn.createFromString(TEST_EXECUTION_REQUEST_URN))))));

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    var result = resolver.get(mockEnv).get();
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getExecutionRequests().size(), 1);

    var executionRequest = result.getExecutionRequests().get(0);
    assertEquals(executionRequest.getUrn(), TEST_EXECUTION_REQUEST_URN);
    assertEquals(executionRequest.getType(), EntityType.EXECUTION_REQUEST);
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetWithCustomQuery() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    ListExecutionRequestsInput customInput =
        new ListExecutionRequestsInput(0, 20, "custom-query", null, null, null);

    // Verify custom query is passed to search
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("custom-query"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray()));

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(customInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);
  }

  @Test
  public void testGetWithSystemSourcesOnly() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    ListExecutionRequestsInput inputWithSystemSourcesOnly =
        new ListExecutionRequestsInput(0, 20, "*", List.of(), null, true);

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.eq(
                    buildFilter(
                        Stream.of(
                                new FacetFilterInput(
                                    "sourceType",
                                    null,
                                    ImmutableList.of("SYSTEM"),
                                    false,
                                    FilterOperator.EQUAL))
                            .toList(),
                        Collections.emptyList())),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(
                            new SearchEntity()
                                .setEntity(
                                    Urn.createFromString("urn:li:dataHubIngestionSource:id-1"))))));
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.eq(
                    buildFilter(
                        Stream.of(
                                new FacetFilterInput(
                                    "ingestionSource",
                                    null,
                                    ImmutableList.of("urn:li:dataHubIngestionSource:id-1"),
                                    false,
                                    FilterOperator.EQUAL))
                            .toList(),
                        Collections.emptyList())),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray()));

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSystemSourcesOnly);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);
  }

  @Test
  public void testGetWithoutSystemSources() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    ListExecutionRequestsInput inputWithSystemSourcesOnly =
        new ListExecutionRequestsInput(0, 20, "*", List.of(), null, false);

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.eq(
                    buildFilter(
                        Stream.of(
                                new FacetFilterInput(
                                    "sourceType",
                                    null,
                                    ImmutableList.of("SYSTEM"),
                                    false,
                                    FilterOperator.EQUAL))
                            .toList(),
                        Collections.emptyList())),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(
                            new SearchEntity()
                                .setEntity(
                                    Urn.createFromString("urn:li:dataHubIngestionSource:id-1"))))));
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.eq(
                    buildFilter(
                        Stream.of(
                                new FacetFilterInput(
                                    "ingestionSource",
                                    null,
                                    ImmutableList.of("urn:li:dataHubIngestionSource:id-1"),
                                    true,
                                    FilterOperator.EQUAL))
                            .toList(),
                        Collections.emptyList())),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray()));

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSystemSourcesOnly);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);
  }
}
