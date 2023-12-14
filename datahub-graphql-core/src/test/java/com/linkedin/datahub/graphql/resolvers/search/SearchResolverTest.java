package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.generated.SearchInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SearchResolverTest {
  @Test
  public void testDefaultSearchFlags() throws Exception {
    EntityClient mockClient = initMockSearchEntityClient();
    final SearchResolver resolver = new SearchResolver(mockClient);

    final SearchInput testInput = new SearchInput(EntityType.DATASET, "", 0, 10, null, null, null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    verifyMockSearchEntityClient(
        mockClient,
        Constants.DATASET_ENTITY_NAME, // Verify that merged entity types were used.
        "",
        null,
        null,
        0,
        10,
        new com.linkedin.metadata.query.SearchFlags()
            .setFulltext(true)
            .setSkipAggregates(false)
            .setSkipHighlighting(true) // empty/wildcard
            .setMaxAggValues(20)
            .setSkipCache(false));
  }

  @Test
  public void testOverrideSearchFlags() throws Exception {
    EntityClient mockClient = initMockSearchEntityClient();
    final SearchResolver resolver = new SearchResolver(mockClient);

    final SearchFlags inputSearchFlags = new SearchFlags();
    inputSearchFlags.setFulltext(false);
    inputSearchFlags.setSkipAggregates(true);
    inputSearchFlags.setSkipHighlighting(true);
    inputSearchFlags.setMaxAggValues(10);
    inputSearchFlags.setSkipCache(true);

    final SearchInput testInput =
        new SearchInput(EntityType.DATASET, "", 1, 11, null, null, inputSearchFlags);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    verifyMockSearchEntityClient(
        mockClient,
        Constants.DATASET_ENTITY_NAME, // Verify that merged entity types were used.
        "",
        null,
        null,
        1,
        11,
        new com.linkedin.metadata.query.SearchFlags()
            .setFulltext(false)
            .setSkipAggregates(true)
            .setSkipHighlighting(true)
            .setMaxAggValues(10)
            .setSkipCache(true));
  }

  @Test
  public void testNonWildCardSearchFlags() throws Exception {
    EntityClient mockClient = initMockSearchEntityClient();
    final SearchResolver resolver = new SearchResolver(mockClient);

    final SearchInput testInput =
        new SearchInput(EntityType.DATASET, "not a wildcard", 0, 10, null, null, null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    verifyMockSearchEntityClient(
        mockClient,
        Constants.DATASET_ENTITY_NAME, // Verify that merged entity types were used.
        "not a wildcard",
        null, // Verify that view filter was used.
        null,
        0,
        10,
        new com.linkedin.metadata.query.SearchFlags()
            .setFulltext(true)
            .setSkipAggregates(false)
            .setSkipHighlighting(false) // empty/wildcard
            .setMaxAggValues(20)
            .setSkipCache(false));
  }

  private EntityClient initMockSearchEntityClient() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(
            client.search(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.any(Authentication.class),
                Mockito.any()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));
    return client;
  }

  private void verifyMockSearchEntityClient(
      EntityClient mockClient,
      String entityName,
      String query,
      Filter filter,
      SortCriterion sortCriterion,
      int start,
      int limit,
      com.linkedin.metadata.query.SearchFlags searchFlags)
      throws Exception {
    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            Mockito.eq(entityName),
            Mockito.eq(query),
            Mockito.eq(filter),
            Mockito.eq(sortCriterion),
            Mockito.eq(start),
            Mockito.eq(limit),
            Mockito.any(Authentication.class),
            Mockito.eq(searchFlags));
  }

  private SearchResolverTest() {}
}
