package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.buildFilter;
import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

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
import java.util.Set;
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
                        Set.of(
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
                "sourceType", null, List.of("SYSTEM"), false, FilterOperator.EQUAL));

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
                "sourceType", null, List.of("SYSTEM"), true, FilterOperator.EQUAL));

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
  public void testGetWithFilteringByAccessibleSystemIngestionSourcesWhenNoPermissions()
      throws Exception {

    EntityClient mockClient =
        getTestEntityClient(
            new FacetFilterInput(
                "sourceType", null, List.of("SYSTEM"), false, FilterOperator.EQUAL));

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
  }

  @Test
  public void testFallbackWithoutIngestionSourceFilterWhenFilterTooLarge() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Urn sourceUrn = Urn.createFromString("urn:li:dataHubIngestionSource:id-1");
    Urn executionRequestUrn = Urn.createFromString(TEST_EXECUTION_REQUEST_URN);

    // Mock ingestion source search
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
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(Set.of(new SearchEntity().setEntity(sourceUrn)))));

    // First attempt with ingestion source filter fails (e.g., filter too large)
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("*"),
                argThat(
                    filter ->
                        filter != null
                            && filter.getOr() != null
                            && !filter.getOr().isEmpty()
                            && !filter.getOr().get(0).getAnd().isEmpty()
                            && filter
                                .getOr()
                                .get(0)
                                .getAnd()
                                .get(0)
                                .getField()
                                .equals("ingestionSource")),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenThrow(new RuntimeException("Request too large"));

    // Second attempt without ingestion source filter succeeds
    SearchEntityArray executionRequestEntities = new SearchEntityArray();
    executionRequestEntities.add(new SearchEntity().setEntity(executionRequestUrn));

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq("*"),
                argThat(
                    filter ->
                        filter == null
                            || filter.getOr() == null
                            || filter.getOr().isEmpty()
                            || filter.getOr().get(0).getAnd().isEmpty()
                            || !filter
                                .getOr()
                                .get(0)
                                .getAnd()
                                .get(0)
                                .getField()
                                .equals("ingestionSource")),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(executionRequestEntities));

    ListExecutionRequestsResolver resolver = new ListExecutionRequestsResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();

    // Verify fallback without ingestion source filter worked
    assertEquals(result.getExecutionRequests().size(), 1);
    assertEquals(result.getExecutionRequests().get(0).getUrn(), TEST_EXECUTION_REQUEST_URN);

    // Verify both search attempts were made
    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            Mockito.eq("*"),
            argThat(
                filter ->
                    filter != null
                        && filter.getOr() != null
                        && !filter.getOr().isEmpty()
                        && !filter.getOr().get(0).getAnd().isEmpty()
                        && filter
                            .getOr()
                            .get(0)
                            .getAnd()
                            .get(0)
                            .getField()
                            .equals("ingestionSource")),
            Mockito.any(),
            Mockito.eq(0),
            Mockito.eq(20));

    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            Mockito.eq("*"),
            argThat(
                filter ->
                    filter == null
                        || filter.getOr() == null
                        || filter.getOr().isEmpty()
                        || filter.getOr().get(0).getAnd().isEmpty()
                        || !filter
                            .getOr()
                            .get(0)
                            .getAnd()
                            .get(0)
                            .getField()
                            .equals("ingestionSource")),
            Mockito.any(),
            Mockito.eq(0),
            Mockito.eq(20));
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
                            ? List.of(ingestionSourceFilter)
                            : Collections.emptyList(),
                        Collections.emptyList())),
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
                        Set.of(
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
                                    List.of("urn:li:dataHubIngestionSource:id-1"),
                                    false,
                                    FilterOperator.EQUAL))
                            .toList(),
                        List.of())),
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
