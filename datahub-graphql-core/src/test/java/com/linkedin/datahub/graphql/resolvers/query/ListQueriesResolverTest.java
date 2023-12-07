package com.linkedin.datahub.graphql.resolvers.query;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListQueriesInput;
import com.linkedin.datahub.graphql.generated.QuerySource;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ListQueriesResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TEST_QUERY_URN = Urn.createFromTuple("query", "test-id");

  private static final ListQueriesInput TEST_INPUT_FULL_FILTERS =
      new ListQueriesInput(0, 20, null, QuerySource.MANUAL, TEST_DATASET_URN.toString());
  private static final ListQueriesInput TEST_INPUT_SOURCE_FILTER =
      new ListQueriesInput(0, 30, null, QuerySource.MANUAL, null);
  private static final ListQueriesInput TEST_INPUT_ENTITY_FILTER =
      new ListQueriesInput(0, 40, null, null, TEST_DATASET_URN.toString());

  @DataProvider(name = "inputs")
  public static Object[][] inputs() {
    return new Object[][] {
      {TEST_INPUT_FULL_FILTERS}, {TEST_INPUT_SOURCE_FILTER}, {TEST_INPUT_ENTITY_FILTER}
    };
  }

  @Test(dataProvider = "inputs")
  public void testGetSuccess(final ListQueriesInput input) throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.search(
                Mockito.eq(Constants.QUERY_ENTITY_NAME),
                Mockito.eq(
                    input.getQuery() == null
                        ? ListQueriesResolver.DEFAULT_QUERY
                        : input.getQuery()),
                Mockito.eq(buildFilter(input.getSource(), input.getDatasetUrn())),
                Mockito.eq(
                    new SortCriterion()
                        .setField(ListQueriesResolver.CREATED_AT_FIELD)
                        .setOrder(SortOrder.DESCENDING)),
                Mockito.eq(input.getStart()),
                Mockito.eq(input.getCount()),
                Mockito.any(Authentication.class),
                Mockito.eq(new SearchFlags().setFulltext(true).setSkipHighlighting(true))))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_QUERY_URN)))));

    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertEquals((int) resolver.get(mockEnv).get().getStart(), 0);
    assertEquals((int) resolver.get(mockEnv).get().getCount(), 1);
    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getQueries().size(), 1);
    assertEquals(
        resolver.get(mockEnv).get().getQueries().get(0).getUrn(), TEST_QUERY_URN.toString());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .search(
            Mockito.any(),
            Mockito.eq("*"),
            Mockito.anyMap(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.any(Authentication.class),
            Mockito.eq(new SearchFlags().setFulltext(true).setSkipHighlighting(true)));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .search(
            Mockito.any(),
            Mockito.eq(""),
            Mockito.anyMap(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.any(Authentication.class),
            Mockito.eq(new SearchFlags().setFulltext(true).setSkipHighlighting(true)));
    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private Filter buildFilter(@Nullable QuerySource source, @Nullable String entityUrn) {
    final AndFilterInput criteria = new AndFilterInput();
    List<FacetFilterInput> andConditions = new ArrayList<>();

    if (source != null) {
      andConditions.add(
          new FacetFilterInput(
              ListQueriesResolver.QUERY_SOURCE_FIELD,
              null,
              ImmutableList.of(source.toString()),
              false,
              FilterOperator.EQUAL));
    }
    if (entityUrn != null) {
      andConditions.add(
          new FacetFilterInput(
              ListQueriesResolver.QUERY_ENTITIES_FIELD,
              null,
              ImmutableList.of(entityUrn),
              false,
              FilterOperator.EQUAL));
    }
    criteria.setAnd(andConditions);
    return ResolverUtils.buildFilter(Collections.emptyList(), ImmutableList.of(criteria));
  }
}
