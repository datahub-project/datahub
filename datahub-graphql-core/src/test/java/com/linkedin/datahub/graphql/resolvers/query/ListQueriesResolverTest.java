package com.linkedin.datahub.graphql.resolvers.query;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListQueriesInput;
import com.linkedin.datahub.graphql.generated.ListQueriesResult;
import com.linkedin.datahub.graphql.generated.QuerySource;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
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
      new ListQueriesInput(
          0, 20, null, QuerySource.MANUAL, TEST_DATASET_URN.toString(), null, null);
  private static final ListQueriesInput TEST_INPUT_SOURCE_FILTER =
      new ListQueriesInput(0, 30, null, QuerySource.MANUAL, null, null, null);
  private static final ListQueriesInput TEST_INPUT_ENTITY_FILTER =
      new ListQueriesInput(0, 40, null, null, TEST_DATASET_URN.toString(), null, null);

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
                any(),
                Mockito.eq(Constants.QUERY_ENTITY_NAME),
                Mockito.eq(
                    input.getQuery() == null
                        ? ListQueriesResolver.DEFAULT_QUERY
                        : input.getQuery()),
                Mockito.eq(buildFilter(input.getSource(), input.getDatasetUrn())),
                Mockito.eq(
                    Collections.singletonList(
                        new SortCriterion()
                            .setField(ListQueriesResolver.CREATED_AT_FIELD)
                            .setOrder(SortOrder.DESCENDING))),
                Mockito.eq(input.getStart()),
                Mockito.eq(input.getCount())))
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
  public void testGetFiltersQueriesWhenViewAuthEnabledAndSubjectNotViewable() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    QuerySubjects querySubjects = new QuerySubjects();
    querySubjects.setSubjects(
        new QuerySubjectArray(new QuerySubject().setEntity(TEST_DATASET_URN)));
    Mockito.when(
            aspectRetriever.getLatestAspectObjects(
                any(),
                eq(ImmutableSet.of(TEST_QUERY_URN)),
                eq(ImmutableSet.of(Constants.QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_QUERY_URN,
                ImmutableMap.of(
                    Constants.QUERY_SUBJECTS_ASPECT_NAME, new Aspect(querySubjects.data()))));

    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(Constants.QUERY_ENTITY_NAME),
                any(),
                any(),
                any(),
                anyInt(),
                any()))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_QUERY_URN)))));

    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);
    QueryContext mockContext = createViewAuthContext(mockAuthorizer, aspectRetriever);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ListQueriesResult result = resolver.get(mockEnv).get();
    assertEquals((int) result.getCount(), 0);
    assertEquals(result.getQueries().size(), 0);
    assertEquals((int) result.getTotal(), 1);
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
            any(),
            Mockito.any(),
            Mockito.eq("*"),
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
              ImmutableList.of(source.toString()),
              false,
              FilterOperator.EQUAL));
    }
    if (entityUrn != null) {
      andConditions.add(
          new FacetFilterInput(
              ListQueriesResolver.QUERY_ENTITIES_FIELD,
              ImmutableList.of(entityUrn),
              false,
              FilterOperator.EQUAL));
    }
    criteria.setAnd(andConditions);
    return ResolverUtils.buildFilter(Collections.emptyList(), ImmutableList.of(criteria));
  }

  private QueryContext createViewAuthContext(
      Authorizer authorizer, AspectRetriever aspectRetriever) {
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "test"), "");

    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(
                TestOperationContexts.emptyActiveUsersAspectRetriever(
                    aspectRetriever::getEntityRegistry))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .build();

    OperationContext systemContext =
        TestOperationContexts.systemContext(
            () ->
                OperationContextConfig.builder()
                    .viewAuthorizationConfiguration(
                        ViewAuthorizationConfiguration.builder().enabled(true).build())
                    .build(),
            null,
            null,
            null,
            () -> retrieverContext,
            null,
            null,
            null);

    OperationContext userContext =
        systemContext.asSession(RequestContext.TEST, authorizer, userAuth);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(userContext);
    Mockito.when(mockContext.getAuthorizer()).thenReturn(authorizer);
    Mockito.when(mockContext.getAuthentication()).thenReturn(userAuth);
    return mockContext;
  }
}
