package com.linkedin.datahub.graphql.resolvers.view;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubViewType;
import com.linkedin.datahub.graphql.generated.ListMyViewsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
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

public class ListMyViewsResolverTest {

  private static final Urn TEST_URN = Urn.createFromTuple("dataHubView", "test-id");
  private static final Urn TEST_USER = UrnUtils.getUrn("urn:li:corpuser:test");

  private static final ListMyViewsInput TEST_INPUT_1 =
      new ListMyViewsInput(0, 20, "", DataHubViewType.GLOBAL);

  private static final ListMyViewsInput TEST_INPUT_2 = new ListMyViewsInput(0, 20, "", null);

  @Test
  public void testGetSuccessInput1() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.search(
                Mockito.eq(Constants.DATAHUB_VIEW_ENTITY_NAME),
                Mockito.eq(""),
                Mockito.eq(
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                ImmutableList.of(
                                    new ConjunctiveCriterion()
                                        .setAnd(
                                            new CriterionArray(
                                                ImmutableList.of(
                                                    new Criterion()
                                                        .setField("createdBy.keyword")
                                                        .setValue(TEST_USER.toString())
                                                        .setValues(
                                                            new StringArray(
                                                                ImmutableList.of(
                                                                    TEST_USER.toString())))
                                                        .setCondition(Condition.EQUAL)
                                                        .setNegated(false),
                                                    new Criterion()
                                                        .setField("type.keyword")
                                                        .setValue(DataHubViewType.GLOBAL.toString())
                                                        .setValues(
                                                            new StringArray(
                                                                ImmutableList.of(
                                                                    DataHubViewType.GLOBAL
                                                                        .toString())))
                                                        .setCondition(Condition.EQUAL)
                                                        .setNegated(false)))))))),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20),
                Mockito.any(Authentication.class),
                Mockito.eq(new SearchFlags().setFulltext(true))))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_URN)))));

    ListMyViewsResolver resolver = new ListMyViewsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_1);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    assertEquals(resolver.get(mockEnv).get().getStart(), 0);
    assertEquals(resolver.get(mockEnv).get().getCount(), 1);
    assertEquals(resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getViews().size(), 1);
    assertEquals(resolver.get(mockEnv).get().getViews().get(0).getUrn(), TEST_URN.toString());
  }

  @Test
  public void testGetSuccessInput2() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.search(
                Mockito.eq(Constants.DATAHUB_VIEW_ENTITY_NAME),
                Mockito.eq(""),
                Mockito.eq(
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                ImmutableList.of(
                                    new ConjunctiveCriterion()
                                        .setAnd(
                                            new CriterionArray(
                                                ImmutableList.of(
                                                    new Criterion()
                                                        .setField("createdBy.keyword")
                                                        .setValue(TEST_USER.toString())
                                                        .setValues(
                                                            new StringArray(
                                                                ImmutableList.of(
                                                                    TEST_USER.toString())))
                                                        .setCondition(Condition.EQUAL)
                                                        .setNegated(false)))))))),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20),
                Mockito.any(Authentication.class),
                Mockito.eq(new SearchFlags().setFulltext(true))))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_URN)))));

    ListMyViewsResolver resolver = new ListMyViewsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_2);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    assertEquals(resolver.get(mockEnv).get().getStart(), 0);
    assertEquals(resolver.get(mockEnv).get().getCount(), 1);
    assertEquals(resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getViews().size(), 1);
    assertEquals(resolver.get(mockEnv).get().getViews().get(0).getUrn(), TEST_URN.toString());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListMyViewsResolver resolver = new ListMyViewsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_1);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .search(
            Mockito.any(),
            Mockito.eq(""),
            Mockito.anyMap(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.any(Authentication.class),
            Mockito.eq(new SearchFlags().setFulltext(true)));
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
            Mockito.eq(new SearchFlags().setFulltext(true)));
    ListMyViewsResolver resolver = new ListMyViewsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_1);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
