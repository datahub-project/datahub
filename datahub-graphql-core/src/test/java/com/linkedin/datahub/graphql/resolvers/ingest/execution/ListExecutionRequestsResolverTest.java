package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.buildFilter;
import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListExecutionRequestsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.GraphQLContext;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
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
                "sourceType", ImmutableList.of("SYSTEM"), false, FilterOperator.EQUAL));

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
                "sourceType", ImmutableList.of("SYSTEM"), true, FilterOperator.EQUAL));

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
                "sourceType", ImmutableList.of("SYSTEM"), false, FilterOperator.EQUAL));

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
  public void testMultiPageIngestionSourceFetching() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // First page of ingestion sources - return full page of 1000 to trigger pagination
    SearchEntityArray firstPageEntities = new SearchEntityArray();
    for (int i = 1; i <= 1000; i++) {
      firstPageEntities.add(
          new SearchEntity()
              .setEntity(Urn.createFromString("urn:li:dataHubIngestionSource:id-" + i)));
    }

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
                .setPageSize(1000)
                .setNumEntities(1500)
                .setEntities(firstPageEntities));

    // Second page of ingestion sources - return partial page to stop pagination
    SearchEntityArray secondPageEntities = new SearchEntityArray();
    for (int i = 1001; i <= 1500; i++) {
      secondPageEntities.add(
          new SearchEntity()
              .setEntity(Urn.createFromString("urn:li:dataHubIngestionSource:id-" + i)));
    }

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(1000),
                Mockito.eq(1000)))
        .thenReturn(
            new SearchResult()
                .setFrom(1000)
                .setPageSize(500)
                .setNumEntities(1500)
                .setEntities(secondPageEntities));

    // Mock execution request search with all ingestion sources in filter
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

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);

    // Verify both pages were fetched
    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(0),
            Mockito.eq(1000));

    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(1000),
            Mockito.eq(1000));

    // Verify sources from both pages end up in the execution request filter
    assertEquals(captureExecutionRequestIngestionSourceFilterValues(mockClient).size(), 1500);
  }

  @Test
  public void testIngestionSourcePaginationStopsAtElasticsearchMaxResultWindow() throws Exception {
    // 12,500 accessible sources: more than Elasticsearch's default max_result_window (10,000)
    List<String> accessibleSources = new ArrayList<>();
    for (int i = 1; i <= 12_500; i++) {
      accessibleSources.add("urn:li:dataHubIngestionSource:id-" + i);
    }
    EntityClient mockClient = getFakeEntityClient(accessibleSources, Map.of());

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);

    // Pages at offsets 0, 1000, ..., 9000 are fetched; none at or beyond the 10,000 window
    for (int start = 0; start < 10_000; start += 1000) {
      Mockito.verify(mockClient, Mockito.times(1))
          .search(
              any(),
              Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
              Mockito.eq("*"),
              Mockito.any(),
              Mockito.any(),
              Mockito.eq(start),
              Mockito.eq(1000));
    }
    Mockito.verify(mockClient, Mockito.never())
        .search(
            any(),
            Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            intThat(start -> start >= 10_000),
            anyInt());

    // The execution request filter is truncated to the first 10,000 sources
    assertEquals(captureExecutionRequestIngestionSourceFilterValues(mockClient).size(), 10_000);
  }

  @Test
  public void testGetWithSpecificIngestionSourceFilter() throws Exception {
    // Test that when user filters by a specific ingestion source URN, only that source is looked
    // up (restricted to accessible sources) instead of collecting all accessible sources
    String specificSourceUrn = "urn:li:dataHubIngestionSource:specific-source";
    EntityClient mockClient =
        getFakeEntityClient(
            List.of("urn:li:dataHubIngestionSource:other-source", specificSourceUrn),
            Map.of(TEST_EXECUTION_REQUEST_URN, specificSourceUrn));

    FacetFilterInput ingestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource", ImmutableList.of(specificSourceUrn), false, FilterOperator.EQUAL);

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

    // The ingestion source lookup is restricted to the requested URN
    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.eq(
                buildFilter(
                    List.of(
                        new FacetFilterInput(
                            "urn", List.of(specificSourceUrn), false, FilterOperator.EQUAL)),
                    Collections.emptyList())),
            Mockito.any(),
            Mockito.eq(0),
            Mockito.eq(1000));
    assertEquals(
        captureExecutionRequestIngestionSourceFilterValues(mockClient), List.of(specificSourceUrn));
  }

  @Test
  public void testGetWithMultipleSpecificIngestionSourceFilters() throws Exception {
    // Test that when user filters by multiple specific ingestion source URNs, runs of all the
    // accessible ones are returned
    String source1 = "urn:li:dataHubIngestionSource:source-1";
    String source2 = "urn:li:dataHubIngestionSource:source-2";
    EntityClient mockClient =
        getFakeEntityClient(
            List.of(source1, source2, "urn:li:dataHubIngestionSource:source-3"),
            Map.of(
                "urn:li:executionRequest:run-1", source1,
                "urn:li:executionRequest:run-2", source2,
                "urn:li:executionRequest:run-3", "urn:li:dataHubIngestionSource:source-3"));

    FacetFilterInput ingestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource", ImmutableList.of(source1, source2), false, FilterOperator.EQUAL);

    ListExecutionRequestsInput inputWithSourceFilter =
        new ListExecutionRequestsInput(0, 20, "*", List.of(ingestionSourceFilter), null, null);

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSourceFilter);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(
        result.getExecutionRequests().stream().map(r -> r.getUrn()).sorted().toList(),
        List.of("urn:li:executionRequest:run-1", "urn:li:executionRequest:run-2"));
  }

  @Test
  public void testGetWithSpecificIngestionSourceBeyond1000Limit() throws Exception {
    // Test that a selected source which is not among the first 1000 sources is still found
    List<String> accessibleSources = new ArrayList<>();
    for (int i = 1; i <= 1001; i++) {
      accessibleSources.add("urn:li:dataHubIngestionSource:source-" + i);
    }
    String sourceBeyondLimit = "urn:li:dataHubIngestionSource:source-1001";
    EntityClient mockClient =
        getFakeEntityClient(
            accessibleSources, Map.of(TEST_EXECUTION_REQUEST_URN, sourceBeyondLimit));

    FacetFilterInput ingestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource", ImmutableList.of(sourceBeyondLimit), false, FilterOperator.EQUAL);

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
    assertEquals(result.getExecutionRequests().get(0).getUrn(), TEST_EXECUTION_REQUEST_URN);
  }

  @Test
  public void testGetWithInaccessibleSpecificIngestionSource() throws Exception {
    // Test that when user filters by a specific ingestion source they don't have access to,
    // no execution requests of it are returned
    String accessibleSource = "urn:li:dataHubIngestionSource:accessible";
    String inaccessibleSource = "urn:li:dataHubIngestionSource:inaccessible";
    EntityClient mockClient =
        getFakeEntityClient(
            List.of(accessibleSource),
            Map.of(
                "urn:li:executionRequest:accessible-run", accessibleSource,
                "urn:li:executionRequest:inaccessible-run", inaccessibleSource));

    FacetFilterInput ingestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource", ImmutableList.of(inaccessibleSource), false, FilterOperator.EQUAL);

    ListExecutionRequestsInput inputWithSourceFilter =
        new ListExecutionRequestsInput(0, 20, "*", List.of(ingestionSourceFilter), null, null);

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSourceFilter);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(result.getExecutionRequests().size(), 0);
    assertFalse(
        captureExecutionRequestIngestionSourceFilterValues(mockClient)
            .contains(inaccessibleSource));
  }

  @Test
  public void testGetWithSpecificIngestionSourceFilterKeepsSystemSourcesFilter() throws Exception {
    String specificSourceUrn = "urn:li:dataHubIngestionSource:specific-source";
    EntityClient mockClient =
        getFakeEntityClient(
            List.of(specificSourceUrn), Map.of(TEST_EXECUTION_REQUEST_URN, specificSourceUrn));

    FacetFilterInput ingestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource", ImmutableList.of(specificSourceUrn), false, FilterOperator.EQUAL);

    ListExecutionRequestsInput inputWithSourceFilter =
        new ListExecutionRequestsInput(0, 20, "*", List.of(ingestionSourceFilter), null, false);

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(inputWithSourceFilter);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Both the system / non-system restriction and the requested URN restrict the lookup
    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.eq(
                buildFilter(
                    List.of(
                        new FacetFilterInput(
                            "sourceType", ImmutableList.of("SYSTEM"), true, FilterOperator.EQUAL),
                        new FacetFilterInput(
                            "urn", List.of(specificSourceUrn), false, FilterOperator.EQUAL)),
                    Collections.emptyList())),
            Mockito.any(),
            Mockito.eq(0),
            Mockito.eq(1000));
  }

  @Test
  public void testGetWithNegatedIngestionSourceFilterCollectsAllAccessibleSources()
      throws Exception {
    String excludedSource = "urn:li:dataHubIngestionSource:excluded";
    String otherSource = "urn:li:dataHubIngestionSource:other";
    EntityClient mockClient =
        getFakeEntityClient(
            List.of(excludedSource, otherSource),
            Map.of(
                "urn:li:executionRequest:excluded-run", excludedSource,
                "urn:li:executionRequest:other-run", otherSource));

    FacetFilterInput negatedIngestionSourceFilter =
        new FacetFilterInput(
            "ingestionSource", ImmutableList.of(excludedSource), true, FilterOperator.EQUAL);

    ListExecutionRequestsInput input =
        new ListExecutionRequestsInput(
            0, 20, "*", List.of(negatedIngestionSourceFilter), null, null);

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();
    assertEquals(
        result.getExecutionRequests().stream().map(r -> r.getUrn()).toList(),
        List.of("urn:li:executionRequest:other-run"));
  }

  @Test
  public void testIngestionSourceUrnsMemoizedAcrossInvocationsInSameRequest() throws Exception {
    EntityClient mockClient = getTestEntityClient(null);
    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    // A shared GraphQLContext simulates a single GraphQL request that invokes the resolver
    // several times.
    GraphQLContext sharedContext = GraphQLContext.newContext().build();
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getGraphQlContext()).thenReturn(sharedContext);

    resolver.get(mockEnv).get();
    resolver.get(mockEnv).get();

    // Ingestion sources are enumerated only once for the whole request, despite two invocations...
    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(0),
            Mockito.eq(1000));
    // ...while each invocation still issues its own execution-request search.
    Mockito.verify(mockClient, Mockito.times(2))
        .search(
            any(),
            Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(0),
            Mockito.eq(20));
  }

  @Test
  public void testIngestionSourceUrnsNotMemoizedWithoutGraphQLContext() throws Exception {
    EntityClient mockClient = getTestEntityClient(null);
    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    // No request-scoped store available (the fallback path, exercised by older callers and tests
    // that don't stub getGraphQlContext()). Each invocation must re-scan independently.
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getGraphQlContext()).thenReturn(null);

    resolver.get(mockEnv).get();
    resolver.get(mockEnv).get();

    // Without a shared context to memoize on, both invocations enumerate ingestion sources.
    Mockito.verify(mockClient, Mockito.times(2))
        .search(
            any(),
            Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(0),
            Mockito.eq(1000));
  }

  @Test
  public void testFailedIngestionSourceScanIsEvictedAndRetriedInSameRequest() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    // The first ingestion-source scan fails; a second invocation in the same request must
    // re-attempt
    // it (the failed entry is evicted from the request cache) rather than inherit the memoized
    // failure.
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq("*"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenThrow(new RemoteInvocationException("boom"));

    GraphQLContext sharedContext = GraphQLContext.newContext().build();
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getGraphQlContext()).thenReturn(sharedContext);

    // Both invocations surface the underlying failure rather than completing successfully.
    assertThrows(() -> resolver.get(mockEnv).join());
    assertThrows(() -> resolver.get(mockEnv).join());

    // The failed scan was evicted after the first invocation, so the second invocation re-attempts
    // it instead of reusing a memoized failure.
    Mockito.verify(mockClient, Mockito.times(2))
        .search(
            any(),
            Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            Mockito.eq("*"),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(0),
            Mockito.eq(1000));
  }

  @Test
  public void testIngestionSourceUrnsMemoizedSeparatelyPerRequestedSources() throws Exception {
    // Invocations in the same request that select different ingestion sources must not share the
    // memoized source list, or one would be filtered by the other's sources.
    String source1 = "urn:li:dataHubIngestionSource:source-1";
    String source2 = "urn:li:dataHubIngestionSource:source-2";
    EntityClient mockClient =
        getFakeEntityClient(
            List.of(source1, source2),
            Map.of(
                "urn:li:executionRequest:run-1", source1,
                "urn:li:executionRequest:run-2", source2));
    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    GraphQLContext sharedContext = GraphQLContext.newContext().build();
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getGraphQlContext()).thenReturn(sharedContext);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(
            new ListExecutionRequestsInput(
                0,
                20,
                "*",
                List.of(
                    new FacetFilterInput(
                        "ingestionSource", List.of(source1), false, FilterOperator.EQUAL)),
                null,
                null));
    var result1 = resolver.get(mockEnv).get();

    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(
            new ListExecutionRequestsInput(
                0,
                20,
                "*",
                List.of(
                    new FacetFilterInput(
                        "ingestionSource", List.of(source2), false, FilterOperator.EQUAL)),
                null,
                null));
    var result2 = resolver.get(mockEnv).get();

    assertEquals(
        result1.getExecutionRequests().stream().map(r -> r.getUrn()).toList(),
        List.of("urn:li:executionRequest:run-1"));
    assertEquals(
        result2.getExecutionRequests().stream().map(r -> r.getUrn()).toList(),
        List.of("urn:li:executionRequest:run-2"));
  }

  /** Returns the values of the accessible-source filter of the (single) execution search. */
  private static List<String> captureExecutionRequestIngestionSourceFilterValues(
      EntityClient mockClient) throws Exception {
    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            any(),
            filterCaptor.capture(),
            any(),
            anyInt(),
            anyInt());
    List<Criterion> ingestionSourceCriteria =
        filterCaptor.getValue().getOr().get(0).getAnd().stream()
            .filter(c -> "ingestionSource".equals(c.getField()))
            .toList();
    // The accessible-source filter is the last one added
    return ingestionSourceCriteria.get(ingestionSourceCriteria.size() - 1).getValues();
  }

  /**
   * An entity client that behaves like a small search index: ingestion source searches page over
   * {@code accessibleSourceUrns} (honouring a {@code urn} filter and rejecting pages beyond
   * Elasticsearch's default max_result_window), and execution request searches return the runs
   * whose source satisfies every {@code ingestionSource} criterion of the filter.
   */
  private static EntityClient getFakeEntityClient(
      List<String> accessibleSourceUrns, Map<String, String> runToSourceUrn) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                any(),
                any(),
                any(),
                anyInt(),
                anyInt()))
        .thenAnswer(
            invocation -> {
              Filter filter = invocation.getArgument(3);
              int start = invocation.getArgument(5);
              int count = invocation.getArgument(6);
              if (start + count > 10_000) {
                throw new IllegalArgumentException("Result window is too large");
              }
              List<String> matching =
                  accessibleSourceUrns.stream()
                      .filter(urn -> matchesCriteria(filter, "urn", urn))
                      .toList();
              List<String> page =
                  matching.subList(
                      Math.min(start, matching.size()), Math.min(start + count, matching.size()));
              return new SearchResult()
                  .setFrom(start)
                  .setPageSize(page.size())
                  .setNumEntities(matching.size())
                  .setEntities(toSearchEntities(page));
            });

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                any(),
                any(),
                any(),
                anyInt(),
                anyInt()))
        .thenAnswer(
            invocation -> {
              Filter filter = invocation.getArgument(3);
              List<String> runs =
                  runToSourceUrn.entrySet().stream()
                      .filter(e -> matchesCriteria(filter, "ingestionSource", e.getValue()))
                      .map(Map.Entry::getKey)
                      .sorted()
                      .toList();
              return new SearchResult()
                  .setFrom(0)
                  .setPageSize(runs.size())
                  .setNumEntities(runs.size())
                  .setEntities(toSearchEntities(runs));
            });

    return mockClient;
  }

  private static boolean matchesCriteria(@Nullable Filter filter, String field, String value) {
    if (filter == null) {
      return true;
    }
    return filter.getOr().get(0).getAnd().stream()
        .filter(c -> field.equals(c.getField()))
        .allMatch(c -> c.getValues().contains(value) != c.isNegated());
  }

  private static SearchEntityArray toSearchEntities(List<String> urns) {
    return new SearchEntityArray(
        urns.stream().map(urn -> new SearchEntity().setEntity(UrnUtils.getUrn(urn))).toList());
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
}
