package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AggregateAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AggregateAcrossEntitiesResolverTest {

  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public static void testApplyViewNullBaseFilter() throws Exception {
    Filter viewFilter = createFilter("field", "test");
    DataHubViewInfo info = getViewInfo(viewFilter);

    ViewService mockService = initMockViewService(TEST_VIEW_URN, info);

    List<String> facets = ImmutableList.of("platform", "domains");

    EntityClient mockClient =
        initMockEntityClient(
            ImmutableList.of(Constants.DATASET_ENTITY_NAME),
            "",
            viewFilter,
            0,
            0,
            facets,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final AggregateAcrossEntitiesResolver resolver =
        new AggregateAcrossEntitiesResolver(mockClient, mockService);

    final AggregateAcrossEntitiesInput testInput =
        new AggregateAcrossEntitiesInput(
            ImmutableList.of(EntityType.DATASET), "", facets, null, TEST_VIEW_URN.toString(), null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    verifyMockEntityClient(
        mockClient,
        ImmutableList.of(
            Constants.DATASET_ENTITY_NAME), // Verify that merged entity types were used.
        "",
        viewFilter, // Verify that view filter was used.
        0,
        0,
        facets // Verify called with facets we provide
        );

    verifyMockViewService(mockService, TEST_VIEW_URN);
  }

  @Test
  public static void testApplyViewBaseFilter() throws Exception {
    Filter viewFilter = createFilter("field", "test");
    DataHubViewInfo info = getViewInfo(viewFilter);

    ViewService mockService = initMockViewService(TEST_VIEW_URN, info);

    Filter baseFilter = createFilter("baseField.keyword", "baseTest");

    EntityClient mockClient =
        initMockEntityClient(
            ImmutableList.of(Constants.DATASET_ENTITY_NAME),
            "",
            SearchUtils.combineFilters(baseFilter, viewFilter),
            0,
            0,
            null,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final AggregateAcrossEntitiesResolver resolver =
        new AggregateAcrossEntitiesResolver(mockClient, mockService);

    final AggregateAcrossEntitiesInput testInput =
        new AggregateAcrossEntitiesInput(
            ImmutableList.of(EntityType.DATASET),
            "",
            null,
            ImmutableList.of(
                new AndFilterInput(
                    ImmutableList.of(
                        new FacetFilterInput(
                            "baseField",
                            "baseTest",
                            ImmutableList.of("baseTest"),
                            false,
                            FilterOperator.EQUAL)))),
            TEST_VIEW_URN.toString(),
            null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    verifyMockEntityClient(
        mockClient,
        ImmutableList.of(
            Constants.DATASET_ENTITY_NAME), // Verify that merged entity types were used.
        "",
        SearchUtils.combineFilters(baseFilter, viewFilter), // Verify that merged filters were used.
        0,
        0,
        null);

    verifyMockViewService(mockService, TEST_VIEW_URN);
  }

  @Test
  public static void testApplyViewNullBaseEntityTypes() throws Exception {
    Filter viewFilter = createFilter("field", "test");
    DataHubViewInfo info = getViewInfo(viewFilter);
    List<String> facets = ImmutableList.of("platform");

    ViewService mockService = initMockViewService(TEST_VIEW_URN, info);

    EntityClient mockClient =
        initMockEntityClient(
            ImmutableList.of(Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME),
            "",
            viewFilter,
            0,
            0,
            facets,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final AggregateAcrossEntitiesResolver resolver =
        new AggregateAcrossEntitiesResolver(mockClient, mockService);

    final AggregateAcrossEntitiesInput testInput =
        new AggregateAcrossEntitiesInput(null, "", facets, null, TEST_VIEW_URN.toString(), null);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    verifyMockEntityClient(
        mockClient,
        ImmutableList.of(
            Constants.DATASET_ENTITY_NAME,
            Constants.DASHBOARD_ENTITY_NAME), // Verify that view entity types were honored.
        "",
        viewFilter, // Verify that merged filters were used.
        0,
        0,
        facets // Verify facets passed in were used
        );

    verifyMockViewService(mockService, TEST_VIEW_URN);
  }

  @Test
  public static void testApplyViewEmptyBaseEntityTypes() throws Exception {
    Filter viewFilter = createFilter("field", "test");
    DataHubViewInfo info = getViewInfo(viewFilter);
    List<String> facets = ImmutableList.of();

    ViewService mockService = initMockViewService(TEST_VIEW_URN, info);

    EntityClient mockClient =
        initMockEntityClient(
            ImmutableList.of(Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME),
            "",
            viewFilter,
            0,
            0,
            null,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final AggregateAcrossEntitiesResolver resolver =
        new AggregateAcrossEntitiesResolver(mockClient, mockService);

    final AggregateAcrossEntitiesInput testInput =
        new AggregateAcrossEntitiesInput(
            Collections.emptyList(), "", facets, null, TEST_VIEW_URN.toString(), null);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    verifyMockEntityClient(
        mockClient,
        ImmutableList.of(
            Constants.DATASET_ENTITY_NAME,
            Constants.DASHBOARD_ENTITY_NAME), // Verify that view entity types were honored.
        "",
        viewFilter, // Verify that merged filters were used.
        0,
        0,
        null // Verify that an empty list for facets in input sends null
        );

    verifyMockViewService(mockService, TEST_VIEW_URN);
  }

  @Test
  public static void testApplyViewViewDoesNotExist() throws Exception {
    // When a view does not exist, the endpoint should WARN and not apply the view.

    ViewService mockService = initMockViewService(TEST_VIEW_URN, null);

    List<String> searchEntityTypes =
        SEARCHABLE_ENTITY_TYPES.stream()
            .map(EntityTypeMapper::getName)
            .collect(Collectors.toList());

    EntityClient mockClient =
        initMockEntityClient(
            searchEntityTypes,
            "",
            null,
            0,
            0,
            null,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final AggregateAcrossEntitiesResolver resolver =
        new AggregateAcrossEntitiesResolver(mockClient, mockService);
    final AggregateAcrossEntitiesInput testInput =
        new AggregateAcrossEntitiesInput(
            Collections.emptyList(), "", null, null, TEST_VIEW_URN.toString(), null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    verifyMockEntityClient(mockClient, searchEntityTypes, "", null, 0, 0, null);
  }

  @Test
  public static void testErrorFetchingResults() throws Exception {
    ViewService mockService = initMockViewService(TEST_VIEW_URN, null);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.any(Authentication.class)))
        .thenThrow(new RemoteInvocationException());

    final AggregateAcrossEntitiesResolver resolver =
        new AggregateAcrossEntitiesResolver(mockClient, mockService);
    final AggregateAcrossEntitiesInput testInput =
        new AggregateAcrossEntitiesInput(
            Collections.emptyList(), "", null, null, TEST_VIEW_URN.toString(), null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static Filter createFilter(String field, String value) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            ImmutableList.of(
                                new Criterion()
                                    .setField(field)
                                    .setValue(value)
                                    .setCondition(Condition.EQUAL)
                                    .setNegated(false)
                                    .setValues(new StringArray(ImmutableList.of(value))))))));
  }

  private static DataHubViewInfo getViewInfo(Filter viewFilter) {
    DataHubViewInfo info = new DataHubViewInfo();
    info.setName("test");
    info.setType(DataHubViewType.GLOBAL);
    info.setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    info.setLastModified(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    info.setDefinition(
        new DataHubViewDefinition()
            .setEntityTypes(
                new StringArray(
                    ImmutableList.of(
                        Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME)))
            .setFilter(viewFilter));
    return info;
  }

  private static ViewService initMockViewService(Urn viewUrn, DataHubViewInfo viewInfo) {
    ViewService service = Mockito.mock(ViewService.class);
    Mockito.when(service.getViewInfo(Mockito.eq(viewUrn), Mockito.any(Authentication.class)))
        .thenReturn(viewInfo);
    return service;
  }

  private static EntityClient initMockEntityClient(
      List<String> entityTypes,
      String query,
      Filter filter,
      int start,
      int limit,
      List<String> facets,
      SearchResult result)
      throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(
            client.searchAcrossEntities(
                Mockito.eq(entityTypes),
                Mockito.eq(query),
                Mockito.eq(filter),
                Mockito.eq(start),
                Mockito.eq(limit),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.any(Authentication.class),
                Mockito.eq(facets)))
        .thenReturn(result);
    return client;
  }

  private static void verifyMockEntityClient(
      EntityClient mockClient,
      List<String> entityTypes,
      String query,
      Filter filter,
      int start,
      int limit,
      List<String> facets)
      throws Exception {
    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(
            Mockito.eq(entityTypes),
            Mockito.eq(query),
            Mockito.eq(filter),
            Mockito.eq(start),
            Mockito.eq(limit),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(Authentication.class),
            Mockito.eq(facets));
  }

  private static void verifyMockViewService(ViewService mockService, Urn viewUrn) {
    Mockito.verify(mockService, Mockito.times(1))
        .getViewInfo(Mockito.eq(viewUrn), Mockito.any(Authentication.class));
  }

  private AggregateAcrossEntitiesResolverTest() {}
}
