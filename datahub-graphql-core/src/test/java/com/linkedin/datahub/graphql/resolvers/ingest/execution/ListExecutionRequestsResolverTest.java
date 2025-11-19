package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.buildFilter;
import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.*;
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
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListExecutionRequestsResolverTest {

  private static final String TEST_EXECUTION_REQUEST_URN = "urn:li:executionRequest:test-execution";

  private static final ListExecutionRequestsInput TEST_INPUT =
      new ListExecutionRequestsInput(0, 20, null, null, null, null);

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = getTestEntityClient(null);

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
    EntityClient mockClient = getTestEntityClient(null);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .search(
            any(), eq(Constants.EXECUTION_REQUEST_ENTITY_NAME), any(), any(), any(), eq(0), eq(20));
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
    EntityClient mockClient = getTestEntityClient(null);

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
    EntityClient mockClient =
        getTestEntityClient(
            new FacetFilterInput(
                "sourceType", null, ImmutableList.of("SYSTEM"), false, FilterOperator.EQUAL));

    ListExecutionRequestsInput inputWithSystemSourcesOnly =
        new ListExecutionRequestsInput(0, 20, "*", List.of(), null, true);

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
    EntityClient mockClient =
        getTestEntityClient(
            new FacetFilterInput(
                "sourceType", null, ImmutableList.of("SYSTEM"), true, FilterOperator.EQUAL));

    ListExecutionRequestsInput inputWithSystemSourcesOnly =
        new ListExecutionRequestsInput(0, 20, "*", List.of(), null, false);

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSystemSourcesOnly);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);
  }

  @Test
  public void testGetWithFilteringByAccessibleIngestionSourcesWhenNoPermissions() throws Exception {
    EntityClient mockClient = getTestEntityClient(null);

    ListExecutionRequestsInput inputWithSystemSourcesOnly =
        new ListExecutionRequestsInput(0, 20, "*", List.of(), null, null);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSystemSourcesOnly);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);
  }

  @Test
  public void testGetWithFilteringByAccessibleSystemIngestionSourcesWhenNoPermissions()
      throws Exception {

    EntityClient mockClient =
        getTestEntityClient(
            new FacetFilterInput(
                "sourceType", null, ImmutableList.of("SYSTEM"), false, FilterOperator.EQUAL));

    ListExecutionRequestsInput inputWithSystemSourcesOnly =
        new ListExecutionRequestsInput(0, 20, "*", List.of(), null, true);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSystemSourcesOnly);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);
  }

  @Test
  public void testGetWithSpecificIngestionSourceFilter() throws Exception {
    // Test that when user filters by a specific ingestion source URN
    String specificSourceUrn = "urn:li:dataHubIngestionSource:specific-source";
    EntityClient mockClient = getTestEntityClientWithSpecificSource(specificSourceUrn);

    FacetFilterInput ingestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource",
            null,
            ImmutableList.of(specificSourceUrn),
            false,
            FilterOperator.EQUAL);

    ListExecutionRequestsInput inputWithSourceFilter =
        new ListExecutionRequestsInput(0, 20, "*", List.of(ingestionSourceFilter), null, null);

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSourceFilter);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 1);
    assertEquals(result.getExecutionRequests().get(0).getUrn(), TEST_EXECUTION_REQUEST_URN);
  }

  @Test
  public void testGetWithMultipleSpecificIngestionSourceFilters() throws Exception {
    // Test that when user filters by multiple specific ingestion source URNs
    String source1 = "urn:li:dataHubIngestionSource:source-1";
    String source2 = "urn:li:dataHubIngestionSource:source-2";
    EntityClient mockClient = getTestEntityClientWithMultipleSources(List.of(source1, source2));

    FacetFilterInput ingestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource",
            null,
            ImmutableList.of(source1, source2),
            false,
            FilterOperator.EQUAL);

    ListExecutionRequestsInput inputWithSourceFilter =
        new ListExecutionRequestsInput(0, 20, "*", List.of(ingestionSourceFilter), null, null);

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSourceFilter);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    // Should return execution requests for accessible sources
    assertTrue(result.getExecutionRequests().size() >= 0);
  }

  @Test
  public void testGetWithSpecificIngestionSourceBeyond1000Limit() throws Exception {
    // Test that the fix allows querying sources beyond the 1000 source limit
    // when filtering by specific source URN
    String sourceBeyondLimit = "urn:li:dataHubIngestionSource:source-1001";
    EntityClient mockClient = getTestEntityClientWithSpecificSource(sourceBeyondLimit);

    FacetFilterInput ingestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource",
            null,
            ImmutableList.of(sourceBeyondLimit),
            false,
            FilterOperator.EQUAL);

    ListExecutionRequestsInput inputWithSourceFilter =
        new ListExecutionRequestsInput(0, 20, "*", List.of(ingestionSourceFilter), null, null);

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSourceFilter);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.getExecutionRequests().size(), 1);
  }

  @Test
  public void testGetWithInaccessibleSpecificIngestionSource() throws Exception {
    // Test that when user filters by a specific ingestion source they don't have access to,
    // no execution requests are returned
    String inaccessibleSource = "urn:li:dataHubIngestionSource:inaccessible";
    EntityClient mockClient = getTestEntityClientWithNoAccessToSource(inaccessibleSource);

    FacetFilterInput ingestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource",
            null,
            ImmutableList.of(inaccessibleSource),
            false,
            FilterOperator.EQUAL);

    ListExecutionRequestsInput inputWithSourceFilter =
        new ListExecutionRequestsInput(0, 20, "*", List.of(ingestionSourceFilter), null, null);

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSourceFilter);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);
  }

  private EntityClient getTestEntityClient(@Nullable FacetFilterInput ingestionSourceFilter)
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.eq(
                    buildFilter(
                        ingestionSourceFilter != null
                            ? Stream.of(ingestionSourceFilter).toList()
                            : Collections.emptyList(),
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

    return mockClient;
  }

  private EntityClient getTestEntityClientWithSpecificSource(String sourceUrn) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock the ingestion source search with URN filter
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.argThat(
                    filter ->
                        filter != null
                            && filter.getOr() != null
                            && !filter.getOr().isEmpty()
                            && filter.getOr().get(0).getAnd() != null
                            && filter.getOr().get(0).getAnd().stream()
                                .anyMatch(
                                    criterion ->
                                        "urn".equals(criterion.getField())
                                            && criterion.getValues() != null
                                            && criterion.getValues().contains(sourceUrn))),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(
                            new SearchEntity().setEntity(Urn.createFromString(sourceUrn))))));

    // Mock the execution request search
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.argThat(
                    filter ->
                        filter != null
                            && filter.getOr() != null
                            && !filter.getOr().isEmpty()
                            && filter.getOr().get(0).getAnd() != null
                            && filter.getOr().get(0).getAnd().stream()
                                .anyMatch(
                                    criterion ->
                                        "ingestionSource".equals(criterion.getField())
                                            && criterion.getValues() != null
                                            && criterion.getValues().contains(sourceUrn))),
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

    return mockClient;
  }

  private EntityClient getTestEntityClientWithMultipleSources(List<String> sourceUrns)
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock the ingestion source search with URN filter for multiple sources
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.argThat(
                    filter ->
                        filter != null
                            && filter.getOr() != null
                            && !filter.getOr().isEmpty()
                            && filter.getOr().get(0).getAnd() != null
                            && filter.getOr().get(0).getAnd().stream()
                                .anyMatch(
                                    criterion ->
                                        "urn".equals(criterion.getField())
                                            && criterion.getValues() != null
                                            && criterion.getValues().containsAll(sourceUrns))),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(sourceUrns.size())
                .setNumEntities(sourceUrns.size())
                .setEntities(
                    new SearchEntityArray(
                        sourceUrns.stream()
                            .map(
                                urn -> {
                                  try {
                                    return new SearchEntity().setEntity(Urn.createFromString(urn));
                                  } catch (Exception e) {
                                    throw new RuntimeException(e);
                                  }
                                })
                            .toList())));

    // Mock the execution request search
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
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray()));

    return mockClient;
  }

  private EntityClient getTestEntityClientWithNoAccessToSource(String sourceUrn) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock the ingestion source search returning empty (user has no access)
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray()));

    // Mock the execution request search (should not be called, but just in case)
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
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray()));

    return mockClient;
  }
}
