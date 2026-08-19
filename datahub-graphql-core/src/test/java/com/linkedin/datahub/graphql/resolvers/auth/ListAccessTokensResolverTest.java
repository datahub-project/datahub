package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListAccessTokenInput;
import com.linkedin.datahub.graphql.generated.ListAccessTokenResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListAccessTokensResolverTest {

  @Test
  public void testGetSuccess() throws Exception {
    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final QueryContext mockAllowContext = TestUtils.getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockAllowContext);

    final ListAccessTokenInput input = new ListAccessTokenInput();
    input.setStart(0);
    input.setCount(100);
    FacetFilterInput filter = new FacetFilterInput();
    filter.setField("actor");
    filter.setValues(ImmutableList.of("urn:li:corpuser:test"));
    final ImmutableList<FacetFilterInput> filters = ImmutableList.of(filter);
    final List<FacetFilterInput> expectedFilters = new ArrayList<>(filters);
    expectedFilters.add(
        new FacetFilterInput("tokenType", ImmutableList.of("SESSION"), true, FilterOperator.EQUAL));

    input.setFilters(filters);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.ACCESS_TOKEN_ENTITY_NAME),
                Mockito.eq(""),
                Mockito.eq(buildFilter(expectedFilters, Collections.emptyList())),
                Mockito.any(List.class),
                Mockito.eq(input.getStart()),
                Mockito.eq(input.getCount())))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setNumEntities(0)
                .setPageSize(0)
                .setEntities(new SearchEntityArray()));

    final ListAccessTokensResolver resolver = new ListAccessTokensResolver(mockClient);
    final ListAccessTokenResult listAccessTokenResult = resolver.get(mockEnv).get();
  }
}
