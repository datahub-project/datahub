package com.linkedin.datahub.graphql.resolvers.view;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubViewType;
import com.linkedin.datahub.graphql.generated.ListGlobalViewsInput;
import com.linkedin.datahub.graphql.generated.ListViewsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class ListGlobalViewsResolverTest {

  private static final Urn TEST_URN = Urn.createFromTuple("dataHubView", "test-id");
  private static final Urn TEST_USER = UrnUtils.getUrn("urn:li:corpuser:test");

  private static final ListGlobalViewsInput TEST_INPUT = new ListGlobalViewsInput(
      0, 20, ""
  );

  @Test
  public void testGetSuccessInput() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(mockClient.search(
        Mockito.eq(Constants.DATAHUB_VIEW_ENTITY_NAME),
        Mockito.eq(""),
        Mockito.eq(
            new Filter()
                .setOr(new ConjunctiveCriterionArray(ImmutableList.of(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(ImmutableList.of(
                            new Criterion()
                                .setField("type.keyword")
                                .setValue(DataHubViewType.GLOBAL.toString())
                                .setValues(new StringArray(
                                    ImmutableList.of(DataHubViewType.GLOBAL.toString())))
                                .setCondition(Condition.EQUAL)
                                .setNegated(false)
                        )))
                )))
        ),
        Mockito.any(),
        Mockito.eq(0),
        Mockito.eq(20),
        Mockito.any(Authentication.class)
    )).thenReturn(
        new SearchResult()
            .setFrom(0)
            .setPageSize(1)
            .setNumEntities(1)
            .setEntities(new SearchEntityArray(ImmutableSet.of(new SearchEntity().setEntity(TEST_URN))))
    );

    ListGlobalViewsResolver resolver = new ListGlobalViewsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ListViewsResult result = resolver.get(mockEnv).get();

    // Data Assertions
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getViews().size(), 1);
    assertEquals(result.getViews().get(0).getUrn(), TEST_URN.toString());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListMyViewsResolver resolver = new ListMyViewsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).search(
        Mockito.any(),
        Mockito.eq(""),
        Mockito.anyMap(),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).search(
        Mockito.any(),
        Mockito.eq(""),
        Mockito.anyMap(),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class));
    ListMyViewsResolver resolver = new ListMyViewsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}