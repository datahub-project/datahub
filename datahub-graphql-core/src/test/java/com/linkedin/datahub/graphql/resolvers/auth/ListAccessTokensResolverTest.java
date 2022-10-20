package com.linkedin.datahub.graphql.resolvers.auth;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.ListAccessTokenInput;
import com.linkedin.datahub.graphql.generated.ListAccessTokenResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import junit.framework.TestCase;
import org.mockito.Mockito;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


public class ListAccessTokensResolverTest extends TestCase {

//  @Test
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

    input.setFilters(filters);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.filter(
        Mockito.eq(Constants.ACCESS_TOKEN_ENTITY_NAME),
            Mockito.eq(buildFilter(filters, Collections.emptyList())),
            Mockito.notNull(),
            Mockito.eq(input.getStart()),
            Mockito.eq(input.getCount()),
            Mockito.eq(getAuthentication(mockEnv))))
        .thenReturn(null);

    final ListAccessTokensResolver resolver = new ListAccessTokensResolver(mockClient);
    final ListAccessTokenResult listAccessTokenResult = resolver.get(mockEnv).get();
  }
}
