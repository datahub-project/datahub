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
import com.linkedin.metadata.authorization.PoliciesConfig;
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
import java.util.Set;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ListQueriesResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TEST_DATASET_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)");
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

    // Execute resolver. Query reads are privilege-filtered by default for non-system actors, so
    // the context must resolve the query's subjects and grant VIEW_ENTITY_QUERIES.
    QueryContext mockContext =
        createContext(
            queryViewPrivilegeAuthorizer(true), mockQuerySubjectsAspectRetriever(), false);
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
    // The reported total excludes entities redacted from this page.
    assertEquals((int) result.getTotal(), 0);
  }

  /**
   * Query-read authorization is enabled by default (independent of the legacy view-authorization
   * flag): an actor lacking {@code VIEW_ENTITY_QUERIES} on the query's subject dataset must not see
   * the query in listQueries results.
   */
  @Test
  public void testGetFiltersQueriesWhenActorLacksViewEntityQueriesAndViewAuthDisabled()
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AspectRetriever aspectRetriever = mockQuerySubjectsAspectRetriever();
    Authorizer mockAuthorizer = queryViewPrivilegeAuthorizer(false);

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
    QueryContext mockContext = createContext(mockAuthorizer, aspectRetriever, false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ListQueriesResult result = resolver.get(mockEnv).get();
    assertEquals(
        result.getQueries().size(),
        0,
        "Query with an unauthorized subject dataset leaked to an actor lacking"
            + " VIEW_ENTITY_QUERIES");
  }

  /** Mirror allow-case: an actor granted VIEW_ENTITY_QUERIES still sees the query. */
  @Test
  public void testGetReturnsQueriesWhenActorHasViewEntityQueries() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AspectRetriever aspectRetriever = mockQuerySubjectsAspectRetriever();
    Authorizer mockAuthorizer = queryViewPrivilegeAuthorizer(true);

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
    QueryContext mockContext = createContext(mockAuthorizer, aspectRetriever, false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ListQueriesResult result = resolver.get(mockEnv).get();
    assertEquals(result.getQueries().size(), 1);
    assertEquals(result.getQueries().get(0).getUrn(), TEST_QUERY_URN.toString());
  }

  /**
   * Escape valve: with query-read authorization explicitly disabled (and legacy view-auth off), no
   * filtering — and no subject lookups — happen at all.
   */
  @Test
  public void testGetReturnsQueriesWhenQueryAuthorizationDisabled() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());
    Authorizer mockAuthorizer = queryViewPrivilegeAuthorizer(false);

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
    QueryContext mockContext =
        createContext(
            mockAuthorizer,
            aspectRetriever,
            ViewAuthorizationConfiguration.builder()
                .enabled(false)
                .queryEntities(
                    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
                        .enabled(false)
                        .build())
                .build());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ListQueriesResult result = resolver.get(mockEnv).get();
    assertEquals(result.getQueries().size(), 1);
    Mockito.verify(aspectRetriever, Mockito.never()).getLatestAspectObjects(any(), any(), any());
  }

  /**
   * COMPAT mode requires all subjects on the dataset view page's Queries tab ({@code datasetUrn}
   * set) but any-subject everywhere else ({@code datasetUrn} unset) — verified with a two-subject
   * query where the actor holds VIEW_ENTITY_QUERIES on only one of the two subjects.
   */
  @Test
  public void testCompatModeRequiresAllSubjectsOnlyWhenDatasetScoped() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    QuerySubjects twoSubjects = new QuerySubjects();
    twoSubjects.setSubjects(
        new QuerySubjectArray(
            new QuerySubject().setEntity(TEST_DATASET_URN),
            new QuerySubject().setEntity(TEST_DATASET_URN_2)));
    Mockito.when(
            aspectRetriever.getLatestAspectObjects(
                any(),
                eq(ImmutableSet.of(TEST_QUERY_URN)),
                eq(ImmutableSet.of(Constants.QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_QUERY_URN,
                ImmutableMap.of(
                    Constants.QUERY_SUBJECTS_ASPECT_NAME, new Aspect(twoSubjects.data()))));

    // Grants VIEW_ENTITY_QUERIES on TEST_DATASET_URN only, not TEST_DATASET_URN_2.
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              boolean allowed =
                  request.getResourceSpec().isPresent()
                      && TEST_DATASET_URN
                          .toString()
                          .equals(request.getResourceSpec().get().getEntity());
              return new AuthorizationResult(
                  request,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  "");
            });

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

    ViewAuthorizationConfiguration compatConfig =
        ViewAuthorizationConfiguration.builder()
            .enabled(false)
            .queryEntities(
                ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
                    .enabled(true)
                    .requireAllSubjects(
                        ViewAuthorizationConfiguration.RequireAllSubjectsMode.COMPAT)
                    .build())
            .build();

    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);

    // Dataset-scoped (the dataset view page's Queries tab): COMPAT requires all subjects, so the
    // query must be excluded since TEST_DATASET_URN_2 is not viewable.
    QueryContext datasetScopedContext =
        createContext(mockAuthorizer, aspectRetriever, compatConfig);
    DataFetchingEnvironment datasetScopedEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(datasetScopedEnv.getArgument(Mockito.eq("input")))
        .thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(datasetScopedEnv.getContext()).thenReturn(datasetScopedContext);
    assertEquals(
        resolver.get(datasetScopedEnv).get().getQueries().size(),
        0,
        "COMPAT must require all subjects on the dataset view page");

    // Unscoped (everywhere else): COMPAT is any-subject, so the query is included.
    QueryContext unscopedContext = createContext(mockAuthorizer, aspectRetriever, compatConfig);
    DataFetchingEnvironment unscopedEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(unscopedEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_SOURCE_FILTER);
    Mockito.when(unscopedEnv.getContext()).thenReturn(unscopedContext);
    assertEquals(
        resolver.get(unscopedEnv).get().getQueries().size(),
        1,
        "COMPAT must accept any single subject when not scoped to a dataset");
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

  private AspectRetriever mockQuerySubjectsAspectRetriever() {
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
    return aspectRetriever;
  }

  /**
   * Authorizer modeling an actor with general read access (VIEW_ENTITY_PAGE, GET_ENTITY, ...) whose
   * grant of the query-view privilege group (VIEW_ENTITY_QUERIES / EDIT_ENTITY_QUERIES /
   * EDIT_ENTITY) is controlled by {@code hasQueryViewPrivilege}.
   */
  private Authorizer queryViewPrivilegeAuthorizer(boolean hasQueryViewPrivilege) {
    Set<String> queryViewPrivileges =
        ImmutableSet.of(
            PoliciesConfig.VIEW_ENTITY_QUERIES_PRIVILEGE.getType(),
            PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType(),
            PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType(),
            PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE.getType());
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              boolean allowed =
                  hasQueryViewPrivilege || !queryViewPrivileges.contains(request.getPrivilege());
              return new AuthorizationResult(
                  request,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  "");
            });
    return mockAuthorizer;
  }

  private QueryContext createViewAuthContext(
      Authorizer authorizer, AspectRetriever aspectRetriever) {
    return createContext(authorizer, aspectRetriever, true);
  }

  private QueryContext createContext(
      Authorizer authorizer, AspectRetriever aspectRetriever, boolean viewAuthorizationEnabled) {
    return createContext(
        authorizer,
        aspectRetriever,
        ViewAuthorizationConfiguration.builder().enabled(viewAuthorizationEnabled).build());
  }

  private QueryContext createContext(
      Authorizer authorizer,
      AspectRetriever aspectRetriever,
      ViewAuthorizationConfiguration viewAuthorizationConfiguration) {
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
                    .viewAuthorizationConfiguration(viewAuthorizationConfiguration)
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
