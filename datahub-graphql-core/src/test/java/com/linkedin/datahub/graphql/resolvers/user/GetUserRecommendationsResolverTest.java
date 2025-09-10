package com.linkedin.datahub.graphql.resolvers.user;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetUserRecommendationsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GetUserRecommendationsResolverTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:test.user";
  private static final String TEST_USER_URN_2 = "urn:li:corpuser:test.user2";

  @Test
  public void testGetUserRecommendationsUnauthorized() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GetUserRecommendationsResolver resolver = new GetUserRecommendationsResolver(mockClient);

    // Create mock context without manage users permission
    QueryContext mockContext = getMockDenyContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Should throw authorization exception
    assertThrows(() -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetUserRecommendationsCallsCorrectMethods() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GetUserRecommendationsResolver resolver = new GetUserRecommendationsResolver(mockClient);

    // Mock search result with empty results
    SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(6);
    mockSearchResult.setNumEntities(0);
    mockSearchResult.setEntities(new SearchEntityArray());

    Mockito.when(mockClient.search(any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(mockSearchResult);
    Mockito.when(mockClient.batchGetV2(any(), any(), any(), any())).thenReturn(ImmutableMap.of());

    // Create mock context with permissions
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getArgument("input"))
        .thenReturn(
            ImmutableMap.of(
                "limit", 6,
                "sortBy", "USAGE_TOTAL_PAST_30_DAYS",
                "platformFilter", "urn:li:dataPlatform:snowflake"));

    // Execute resolver
    CompletableFuture<GetUserRecommendationsResult> result = resolver.get(mockEnv);
    GetUserRecommendationsResult finalResult = result.get();

    // Verify basic functionality
    assertNotNull(finalResult);
    assertEquals((int) finalResult.getStart(), 0);
    assertEquals((int) finalResult.getCount(), 6);
    assertEquals((int) finalResult.getTotal(), 0);
    assertNotNull(finalResult.getUsers());

    // Verify search was called with correct entity type
    Mockito.verify(mockClient)
        .search(any(), eq(CORP_USER_ENTITY_NAME), any(), any(), any(), eq(0), eq(6));
  }
}
