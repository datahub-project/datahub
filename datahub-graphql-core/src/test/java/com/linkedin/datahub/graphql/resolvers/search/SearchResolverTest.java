package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;

import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.generated.SearchInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.QueryEmbeddingException;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFailClosedException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SearchResolverTest {

  private com.linkedin.metadata.query.SearchFlags setConvertSchemaFieldsToDatasets(
      com.linkedin.metadata.query.SearchFlags flags, boolean value) {
    if (value) {
      return flags.setGroupingSpec(
          new com.linkedin.metadata.query.GroupingSpec()
              .setGroupingCriteria(
                  new GroupingCriterionArray(
                      new com.linkedin.metadata.query.GroupingCriterion()
                          .setBaseEntityType(SCHEMA_FIELD_ENTITY_NAME)
                          .setGroupingEntityType(DATASET_ENTITY_NAME))));
    } else {
      return flags.setGroupingSpec(null, SetMode.REMOVE_IF_NULL);
    }
  }

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
        Collections.emptyList(),
        0,
        10,
        setConvertSchemaFieldsToDatasets(
            new com.linkedin.metadata.query.SearchFlags()
                .setFulltext(true)
                .setSkipAggregates(false)
                .setSkipHighlighting(true) // empty/wildcard
                .setSkipCache(false)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            true));
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
        Collections.emptyList(),
        1,
        11,
        setConvertSchemaFieldsToDatasets(
            new com.linkedin.metadata.query.SearchFlags()
                .setFulltext(false)
                .setSkipAggregates(true)
                .setSkipHighlighting(true)
                .setMaxAggValues(10)
                .setSkipCache(true),
            false));
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
        Collections.emptyList(),
        0,
        10,
        setConvertSchemaFieldsToDatasets(
            new com.linkedin.metadata.query.SearchFlags()
                .setFulltext(true)
                .setSkipAggregates(false)
                .setSkipHighlighting(false) // empty/wildcard
                .setSkipCache(false)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            true));
  }

  @Test
  public void testANamedTemplateThatCannotEmbedTheQueryFailsTypedOrFallsBack() throws Exception {
    final RuntimeException timedOut =
        new SearchTemplateFailClosedException(
            "Search template urn:li:searchTemplate:semantic failed to retrieve by vector",
            new QueryEmbeddingException(QueryEmbeddingException.TIMEOUT, null));
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    // The search without the flag, then the search with it and its keyword fallback
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt()))
        .thenThrow(timedOut)
        .thenThrow(timedOut)
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));
    final SearchResolver resolver = new SearchResolver(mockClient);
    final SearchFlags flags = new SearchFlags();
    flags.setSearchTemplate("urn:li:searchTemplate:semantic");
    final SearchInput input =
        new SearchInput(EntityType.DOCUMENT, "quarterly revenue", 0, 10, null, null, flags);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    CompletionException failure =
        Assert.expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Assert.assertTrue(failure.getCause() instanceof DataHubGraphQLException, failure.toString());
    Assert.assertEquals(
        ((DataHubGraphQLException) failure.getCause()).errorCode(),
        DataHubGraphQLErrorCode.DEADLINE_EXCEEDED);

    // Opted in, the same search without the named template serves it, marked with the reason
    flags.setKeywordFallbackOnEmbeddingFailure(true);

    SearchResults result = resolver.get(mockEnv).get();

    Assert.assertEquals(result.getKeywordFallback(), QueryEmbeddingException.TIMEOUT);
  }

  @Test
  public void testWithNoTemplateNamedThereIsNoKeywordFallback() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt()))
        .thenThrow(
            new SearchTemplateFailClosedException(
                "Search template urn:li:searchTemplate:semantic failed to retrieve by vector",
                new QueryEmbeddingException(QueryEmbeddingException.TIMEOUT, null)));
    final SearchResolver resolver = new SearchResolver(mockClient);
    final SearchFlags flags = new SearchFlags();
    flags.setKeywordFallbackOnEmbeddingFailure(true);
    final SearchInput input =
        new SearchInput(EntityType.DOCUMENT, "quarterly revenue", 0, 10, null, null, flags);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    CompletionException failure =
        Assert.expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Assert.assertEquals(
        ((DataHubGraphQLException) failure.getCause()).errorCode(),
        DataHubGraphQLErrorCode.DEADLINE_EXCEEDED);
    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt());
  }

  private EntityClient initMockSearchEntityClient() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(
            client.search(
                any(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt()))
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
      List<SortCriterion> sortCriteria,
      int start,
      int limit,
      com.linkedin.metadata.query.SearchFlags searchFlags)
      throws Exception {
    Mockito.verify(mockClient, Mockito.times(1))
        .search(
            any(),
            Mockito.eq(entityName),
            Mockito.eq(query),
            Mockito.eq(filter),
            Mockito.eq(sortCriteria),
            Mockito.eq(start),
            Mockito.eq(limit));
  }

  private SearchResolverTest() {}
}
