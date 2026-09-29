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
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
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
import com.linkedin.metadata.search.ScrollResult;
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
import java.util.stream.Collectors;
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
  private static final Urn TEST_QUERY_URN_DENIED = Urn.createFromTuple("query", "denied-id");
  private static final Urn TEST_QUERY_URN_ALLOWED = Urn.createFromTuple("query", "allowed-id");

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
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.scrollAcrossEntities(
                any(),
                Mockito.eq(ImmutableList.of(Constants.QUERY_ENTITY_NAME)),
                Mockito.eq(
                    input.getQuery() == null
                        ? ListQueriesResolver.DEFAULT_QUERY
                        : input.getQuery()),
                Mockito.eq(buildFilter(input.getSource(), input.getDatasetUrn())),
                Mockito.isNull(),
                Mockito.eq(ListQueriesResolver.AUTHORIZATION_SCROLL_KEEP_ALIVE),
                Mockito.eq(
                    List.of(
                        new SortCriterion()
                            .setField(ListQueriesResolver.CREATED_AT_FIELD)
                            .setOrder(SortOrder.DESCENDING),
                        new SortCriterion()
                            .setField(ListQueriesResolver.URN_SORT_FIELD)
                            .setOrder(SortOrder.ASCENDING))),
                Mockito.eq(ListQueriesResolver.AUTHORIZATION_SCROLL_BATCH_SIZE)))
        .thenReturn(
            new ScrollResult()
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_QUERY_URN)))));

    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);

    // Query reads are privilege-filtered by default for non-system actors, so the context must
    // resolve the query's subjects and grant VIEW_ENTITY_QUERIES.
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

    QuerySubjects querySubjects = singleSubject(TEST_DATASET_URN);
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

    Authorizer mockAuthorizer = denyAllAuthorizer();

    mockSingleBatchScroll(mockClient, TEST_QUERY_URN);

    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);
    QueryContext mockContext = createViewAuthContext(mockAuthorizer, aspectRetriever);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ListQueriesResult result = resolver.get(mockEnv).get();
    assertEquals((int) result.getCount(), 0);
    assertEquals(result.getQueries().size(), 0);
    // The reported total reflects the exact authorized total across the full scan, not a
    // page-relative subtraction.
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

    mockSingleBatchScroll(mockClient, TEST_QUERY_URN);

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

    mockSingleBatchScroll(mockClient, TEST_QUERY_URN);

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
    Mockito.verify(mockClient, Mockito.never())
        .scrollAcrossEntities(any(), any(), any(), any(), any(), any(), any(), anyInt());
  }

  /**
   * An actor holding VIEW_ALL_QUERIES uses the same fast path as query-authorization-disabled: it
   * proves every match is already visible to them regardless of subjects, so there's nothing to
   * scroll or authorize per-query.
   */
  @Test
  public void testGetReturnsQueriesWhenActorHasViewAllQueriesPrivilege() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());
    Authorizer mockAuthorizer = viewAllQueriesAuthorizer();

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
    // Query-view-authorization active by default (VAE off, dedicated flag defaults on) — only the
    // VIEW_ALL_QUERIES bypass should route this to the fast path.
    QueryContext mockContext = createContext(mockAuthorizer, aspectRetriever, false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ListQueriesResult result = resolver.get(mockEnv).get();
    assertEquals(result.getQueries().size(), 1);
    Mockito.verify(aspectRetriever, Mockito.never()).getLatestAspectObjects(any(), any(), any());
    Mockito.verify(mockClient, Mockito.never())
        .scrollAcrossEntities(any(), any(), any(), any(), any(), any(), any(), anyInt());
  }

  /**
   * COMPAT mode tracks VIEW_AUTHORIZATION_ENABLED's runtime state uniformly: a two-subject query
   * where the actor holds VIEW_ENTITY_QUERIES on only one subject is included when the flag is off
   * (any-subject, matching the old default) and excluded once it's on (require-all).
   */
  @Test
  public void testCompatModeTracksViewAuthorizationEnabled() throws Exception {
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
    Authorizer mockAuthorizer = datasetScopedAuthorizer(TEST_DATASET_URN);

    mockSingleBatchScroll(mockClient, TEST_QUERY_URN);

    ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig compat =
        ViewAuthorizationConfiguration.QueryEntityAuthorizationConfig.builder()
            .enabled(true)
            .requireAllSubjects(ViewAuthorizationConfiguration.RequireAllSubjectsMode.COMPAT)
            .build();
    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);

    // VIEW_AUTHORIZATION_ENABLED off: any-subject, so the query is included.
    ViewAuthorizationConfiguration viewAuthOffConfig =
        ViewAuthorizationConfiguration.builder().enabled(false).queryEntities(compat).build();
    QueryContext viewAuthOffContext =
        createContext(mockAuthorizer, aspectRetriever, viewAuthOffConfig);
    DataFetchingEnvironment viewAuthOffEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(viewAuthOffEnv.getArgument(Mockito.eq("input")))
        .thenReturn(TEST_INPUT_SOURCE_FILTER);
    Mockito.when(viewAuthOffEnv.getContext()).thenReturn(viewAuthOffContext);
    assertEquals(
        resolver.get(viewAuthOffEnv).get().getQueries().size(),
        1,
        "COMPAT must be any-subject when VIEW_AUTHORIZATION_ENABLED is off");

    // VIEW_AUTHORIZATION_ENABLED on: require-all, so the query is excluded.
    ViewAuthorizationConfiguration viewAuthOnConfig =
        ViewAuthorizationConfiguration.builder().enabled(true).queryEntities(compat).build();
    QueryContext viewAuthOnContext =
        createContext(mockAuthorizer, aspectRetriever, viewAuthOnConfig);
    DataFetchingEnvironment viewAuthOnEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(viewAuthOnEnv.getArgument(Mockito.eq("input")))
        .thenReturn(TEST_INPUT_SOURCE_FILTER);
    Mockito.when(viewAuthOnEnv.getContext()).thenReturn(viewAuthOnContext);
    assertEquals(
        resolver.get(viewAuthOnEnv).get().getQueries().size(),
        0,
        "COMPAT must require all subjects once VIEW_AUTHORIZATION_ENABLED is on");
  }

  /**
   * The raw search order is [denied, allowed]. Pagination and total must reflect only the
   * authorized stream: page 0 (count=1) returns the allowed query with total=1; page 1 (start=1,
   * count=1) is empty with the SAME total=1 — not a total that shifts across pages, and not the
   * denied query leaking through because it happened to occupy the requested raw offset.
   */
  @Test
  public void testGetPaginatesAuthorizedStreamNotRawPage() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    Mockito.when(
            aspectRetriever.getLatestAspectObjects(
                any(),
                eq(ImmutableSet.of(TEST_QUERY_URN_DENIED, TEST_QUERY_URN_ALLOWED)),
                eq(ImmutableSet.of(Constants.QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_QUERY_URN_DENIED,
                ImmutableMap.of(
                    Constants.QUERY_SUBJECTS_ASPECT_NAME,
                    new Aspect(singleSubject(TEST_DATASET_URN_2).data())),
                TEST_QUERY_URN_ALLOWED,
                ImmutableMap.of(
                    Constants.QUERY_SUBJECTS_ASPECT_NAME,
                    new Aspect(singleSubject(TEST_DATASET_URN).data()))));

    Authorizer mockAuthorizer = datasetScopedAuthorizer(TEST_DATASET_URN);
    mockSingleBatchScroll(mockClient, TEST_QUERY_URN_DENIED, TEST_QUERY_URN_ALLOWED);

    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);
    QueryContext mockContext = createContext(mockAuthorizer, aspectRetriever, false);

    ListQueriesInput page0Input = new ListQueriesInput(0, 1, null, null, null, null, null);
    DataFetchingEnvironment page0Env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(page0Env.getArgument(Mockito.eq("input"))).thenReturn(page0Input);
    Mockito.when(page0Env.getContext()).thenReturn(mockContext);
    ListQueriesResult page0 = resolver.get(page0Env).get();
    assertEquals(page0.getQueries().size(), 1);
    assertEquals(page0.getQueries().get(0).getUrn(), TEST_QUERY_URN_ALLOWED.toString());
    assertEquals((int) page0.getTotal(), 1);

    ListQueriesInput page1Input = new ListQueriesInput(1, 1, null, null, null, null, null);
    DataFetchingEnvironment page1Env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(page1Env.getArgument(Mockito.eq("input"))).thenReturn(page1Input);
    Mockito.when(page1Env.getContext()).thenReturn(mockContext);
    ListQueriesResult page1 = resolver.get(page1Env).get();
    assertEquals(page1.getQueries().size(), 0);
    assertEquals((int) page1.getTotal(), 1, "total must not shift across pages");
  }

  /** An actor denied on every candidate sees total=0 on every page, not a shifting count. */
  @Test
  public void testGetReturnsZeroTotalWhenAllQueriesDenied() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    Mockito.when(
            aspectRetriever.getLatestAspectObjects(
                any(),
                eq(ImmutableSet.of(TEST_QUERY_URN_DENIED, TEST_QUERY_URN_ALLOWED)),
                eq(ImmutableSet.of(Constants.QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_QUERY_URN_DENIED,
                ImmutableMap.of(
                    Constants.QUERY_SUBJECTS_ASPECT_NAME,
                    new Aspect(singleSubject(TEST_DATASET_URN_2).data())),
                TEST_QUERY_URN_ALLOWED,
                ImmutableMap.of(
                    Constants.QUERY_SUBJECTS_ASPECT_NAME,
                    new Aspect(singleSubject(TEST_DATASET_URN).data()))));

    Authorizer mockAuthorizer = denyAllAuthorizer();
    mockSingleBatchScroll(mockClient, TEST_QUERY_URN_DENIED, TEST_QUERY_URN_ALLOWED);

    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);
    QueryContext mockContext = createContext(mockAuthorizer, aspectRetriever, false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    ListQueriesResult result = resolver.get(mockEnv).get();
    assertEquals(result.getQueries().size(), 0);
    assertEquals((int) result.getTotal(), 0);
  }

  /**
   * A request whose authorized scan would exceed {@link
   * ListQueriesResolver#MAX_QUERY_OVERFETCH_CANDIDATES} is rejected outright rather than returning
   * a partial page or an inexact total.
   */
  @Test
  public void testGetThrowsWhenCandidateScanExceedsCap() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    List<SearchEntity> fullBatch = new ArrayList<>();
    for (int i = 0; i < ListQueriesResolver.AUTHORIZATION_SCROLL_BATCH_SIZE; i++) {
      fullBatch.add(new SearchEntity().setEntity(Urn.createFromTuple("query", "cap-test-" + i)));
    }
    // Never-ending scroll: always a full batch, always a scrollId to continue with.
    Mockito.when(
            mockClient.scrollAcrossEntities(
                any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .thenReturn(
            new ScrollResult().setEntities(new SearchEntityArray(fullBatch)).setScrollId("more"));

    // Deny everything, including VIEW_ALL_QUERIES (which would otherwise route this to the fast
    // path and skip the scroll loop entirely). With no subjects aspect found, every candidate is
    // fail-closed regardless of the authorizer — the cap must trip on candidate count alone before
    // authorization outcome matters.
    Authorizer mockAuthorizer = denyAllAuthorizer();
    Mockito.when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(ImmutableMap.of());

    ListQueriesResolver resolver = new ListQueriesResolver(mockClient);
    QueryContext mockContext = createContext(mockAuthorizer, aspectRetriever, false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_FULL_FILTERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try {
      resolver.get(mockEnv).join();
      fail("expected a CompletionException from the candidate-cap abort");
    } catch (CompletionException thrown) {
      Throwable rootCause = thrown.getCause().getCause();
      assertTrue(
          rootCause instanceof DataHubGraphQLException,
          "expected the candidate-cap abort to surface as a DataHubGraphQLException, got: "
              + rootCause);
      assertEquals(
          ((DataHubGraphQLException) rootCause).errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    }
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

  /**
   * {@code getMockAllowContext} resolves a real {@link OperationContext} with query-read
   * authorization enabled by default (and not system auth), so this exercises the same scrolling,
   * authorized path as production traffic — not the {@code EntityClient.search} fast path.
   */
  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .scrollAcrossEntities(any(), any(), any(), any(), any(), any(), any(), anyInt());
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

  private static QuerySubjects singleSubject(Urn datasetUrn) {
    QuerySubjects subjects = new QuerySubjects();
    subjects.setSubjects(new QuerySubjectArray(new QuerySubject().setEntity(datasetUrn)));
    return subjects;
  }

  /** Stubs a single, non-continuing scroll batch (no scrollId) returning exactly these urns. */
  private void mockSingleBatchScroll(EntityClient mockClient, Urn... urns) throws Exception {
    Mockito.when(
            mockClient.scrollAcrossEntities(
                any(),
                Mockito.eq(ImmutableList.of(Constants.QUERY_ENTITY_NAME)),
                any(),
                any(),
                Mockito.isNull(),
                Mockito.eq(ListQueriesResolver.AUTHORIZATION_SCROLL_KEEP_ALIVE),
                any(),
                Mockito.eq(ListQueriesResolver.AUTHORIZATION_SCROLL_BATCH_SIZE)))
        .thenReturn(
            new ScrollResult()
                .setEntities(
                    new SearchEntityArray(
                        java.util.Arrays.stream(urns)
                            .map(urn -> new SearchEntity().setEntity(urn))
                            .collect(Collectors.toList()))));
  }

  private AspectRetriever mockQuerySubjectsAspectRetriever() {
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    QuerySubjects querySubjects = singleSubject(TEST_DATASET_URN);
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
    // Deliberately excludes VIEW_ALL_QUERIES_PRIVILEGE: that platform-level bypass now has its own
    // dedicated fast path and test (testGetReturnsQueriesWhenActorHasViewAllQueriesPrivilege) — a
    // "true" here must exercise the per-subject-dataset scroll path, not the bypass.
    Set<String> queryViewPrivileges =
        ImmutableSet.of(
            PoliciesConfig.VIEW_ENTITY_QUERIES_PRIVILEGE.getType(),
            PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType(),
            PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType());
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              boolean allowed;
              if (PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE
                  .getType()
                  .equals(request.getPrivilege())) {
                allowed = false;
              } else {
                allowed =
                    hasQueryViewPrivilege || !queryViewPrivileges.contains(request.getPrivilege());
              }
              return new AuthorizationResult(
                  request,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  "");
            });
    return mockAuthorizer;
  }

  /** Authorizer granting the query-view privilege group only on {@code grantedDatasetUrn}. */
  private Authorizer datasetScopedAuthorizer(Urn grantedDatasetUrn) {
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              boolean allowed =
                  request.getResourceSpec().isPresent()
                      && grantedDatasetUrn
                          .toString()
                          .equals(request.getResourceSpec().get().getEntity());
              return new AuthorizationResult(
                  request,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  "");
            });
    return mockAuthorizer;
  }

  private Authorizer denyAllAuthorizer() {
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));
    return mockAuthorizer;
  }

  /** Authorizer granting only the platform-level VIEW_ALL_QUERIES privilege (no resource spec). */
  private Authorizer viewAllQueriesAuthorizer() {
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              boolean allowed =
                  PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE
                      .getType()
                      .equals(request.getPrivilege());
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
