package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;

import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.generated.SearchInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
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
                .setMaxAggValues(20)
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
                .setMaxAggValues(20)
                .setSkipCache(false)
                .setIncludeSoftDeleted(false)
                .setIncludeRestricted(false),
            true));
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
