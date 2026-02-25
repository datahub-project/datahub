package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.ingestion.ExecutionRequestType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class GetLatestSuccessfulExecutionRequestResolverTest {

  private static final String TEST_EXECUTION_REQUEST_URN = "urn:li:executionRequest:test-execution";
  private static final String TEST_INGESTION_SOURCE_URN =
      "urn:li:dataHubIngestionSource:test-source";

  private static final ListExecutionRequestsInput TEST_INPUT =
      new ListExecutionRequestsInput(0, 20, null, null, null, null);
  private static final Logger log =
      LoggerFactory.getLogger(GetLatestSuccessfulExecutionRequestResolverTest.class);

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockEntityClient =
        getMockedEntityClient(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(
                            new SearchEntity()
                                .setEntity(Urn.createFromString(TEST_EXECUTION_REQUEST_URN))))));
    ExecutionRequestType mockExecutionRequestType = Mockito.mock(ExecutionRequestType.class);
    Mockito.when(
            mockExecutionRequestType.batchLoad(
                Mockito.eq(List.of(TEST_EXECUTION_REQUEST_URN)), any()))
        .thenReturn(
            List.of(
                DataFetcherResult.<ExecutionRequest>newResult()
                    .data(getTestExecutionRequest())
                    .build()));
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(getTestIngestionSource());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    GetLatestSuccessfulExecutionRequestResolver resolver =
        new GetLatestSuccessfulExecutionRequestResolver(mockEntityClient, mockExecutionRequestType);
    var result = resolver.get(mockEnv).get();

    assertEquals(result.getUrn(), TEST_EXECUTION_REQUEST_URN);
  }

  @Test
  public void testGetSuccessWithEmptyResponse() throws Exception {
    EntityClient mockEntityClient =
        getMockedEntityClient(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray(ImmutableSet.of())));
    ExecutionRequestType mockExecutionRequestType = Mockito.mock(ExecutionRequestType.class);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(getTestIngestionSource());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    GetLatestSuccessfulExecutionRequestResolver resolver =
        new GetLatestSuccessfulExecutionRequestResolver(mockEntityClient, mockExecutionRequestType);
    var result = resolver.get(mockEnv).get();

    assertNull(result);
  }

  private ExecutionRequest getTestExecutionRequest() {
    return new ExecutionRequest(
        TEST_EXECUTION_REQUEST_URN,
        EntityType.EXECUTION_REQUEST,
        "testId",
        new ExecutionRequestInput("task", null, null, 0L, null, null, "default"),
        null,
        null,
        null);
  }

  private IngestionSource getTestIngestionSource() {
    IngestionSource ingestionSource = new IngestionSource();
    ingestionSource.setUrn(TEST_INGESTION_SOURCE_URN);
    return ingestionSource;
  }

  private EntityClient getMockedEntityClient(SearchResult filterSearchResult) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.eq(
                    Objects.requireNonNull(
                        ResolverUtils.buildFilter(
                            List.of(
                                new FacetFilterInput(
                                    "ingestionSource",
                                    null,
                                    List.of(TEST_INGESTION_SOURCE_URN),
                                    false,
                                    FilterOperator.EQUAL),
                                new FacetFilterInput(
                                    "executionResultStatus",
                                    null,
                                    List.of("SUCCESS"),
                                    false,
                                    FilterOperator.EQUAL)),
                            Collections.emptyList()))),
                Mockito.eq(
                    Collections.singletonList(
                        new com.linkedin.metadata.query.filter.SortCriterion()
                            .setField("requestTimeMs")
                            .setOrder(SortOrder.DESCENDING))),
                Mockito.eq(0),
                Mockito.eq(1)))
        .thenReturn(filterSearchResult);

    return mockClient;
  }
}
