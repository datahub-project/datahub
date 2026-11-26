package com.linkedin.datahub.graphql.resolvers.view;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubViewType;
import com.linkedin.datahub.graphql.generated.ListGlobalViewsInput;
import com.linkedin.datahub.graphql.generated.ListViewsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFieldModel;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListGlobalViewsResolverTest {

  private static final Urn TEST_URN = Urn.createFromTuple("dataHubView", "test-id");
  private static final Urn TEST_USER = UrnUtils.getUrn("urn:li:corpuser:test");

  private static final String V2_CREATED_AT_FIELD = "createdAt";

  /** V3 stores the created time as the first write, resolved through the V3 field mapping. */
  private static final String V3_CREATED_AT_FIELD =
      V3SearchFieldModel.forSearchableEntities(TestOperationContexts.defaultEntityRegistry())
          .resolver()
          .exactPath(MappingConstants.SYSTEM_CREATED_TIME_FIELD, Intent.SORT);

  private static final EntityIndexConfiguration V3_INDEX =
      EntityIndexConfiguration.builder()
          .v3(EntityIndexVersionConfiguration.builder().enabled(true).build())
          .build();

  private static final ListGlobalViewsInput TEST_INPUT = new ListGlobalViewsInput(0, 20, "");

  @Test
  public void testGetSuccessInput() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.search(
                any(),
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
                                                    buildCriterion(
                                                        "type",
                                                        Condition.EQUAL,
                                                        DataHubViewType.GLOBAL.toString())))))))),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_URN)))));

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
    assertSortField(mockClient, V2_CREATED_AT_FIELD);
  }

  @Test
  public void testGetSuccessSortsBySystemCreatedTimeWhenSearchV3() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.DATAHUB_VIEW_ENTITY_NAME),
                Mockito.eq(""),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_URN)))));

    ListGlobalViewsResolver resolver = new ListGlobalViewsResolver(mockClient, V3_INDEX);

    QueryContext mockContext = getMockAllowContext(TEST_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertEquals(resolver.get(mockEnv).get().getViews().size(), 1);
    assertSortField(mockClient, V3_CREATED_AT_FIELD);
  }

  private static void assertSortField(EntityClient mockClient, String field) throws Exception {
    org.mockito.ArgumentCaptor<java.util.List<SortCriterion>> sort =
        org.mockito.ArgumentCaptor.forClass(java.util.List.class);
    Mockito.verify(mockClient, Mockito.atLeastOnce())
        .search(
            any(),
            Mockito.eq(Constants.DATAHUB_VIEW_ENTITY_NAME),
            Mockito.eq(""),
            Mockito.any(),
            sort.capture(),
            Mockito.eq(0),
            Mockito.eq(20));
    assertEquals(sort.getValue().size(), 1);
    assertEquals(sort.getValue().get(0).getField(), field);
    assertEquals(sort.getValue().get(0).getOrder(), SortOrder.DESCENDING);
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
    Mockito.verify(mockClient, Mockito.times(0))
        .search(
            any(),
            Mockito.any(),
            Mockito.eq(""),
            Mockito.anyMap(),
            Mockito.anyInt(),
            Mockito.anyInt());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .search(
            any(),
            Mockito.any(),
            Mockito.eq(""),
            Mockito.anyMap(),
            Mockito.anyInt(),
            Mockito.anyInt());
    ListMyViewsResolver resolver = new ListMyViewsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
