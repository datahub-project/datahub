package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
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

public class SearchAcrossEntitiesResolverTest {

  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public static void testApplyViewNullBaseFilter() throws Exception {

    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    buildCriterion("field", Condition.EQUAL, "test"))))));

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

    ViewService mockService = initMockViewService(TEST_VIEW_URN, info);

    EntityClient mockClient =
        initMockEntityClient(
            ImmutableList.of(Constants.DATASET_ENTITY_NAME),
            "",
            viewFilter,
            0,
            10,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, mockService);

    final SearchAcrossEntitiesInput testInput =
        new SearchAcrossEntitiesInput(
            ImmutableList.of(EntityType.DATASET),
            "",
            0,
            10,
            null,
            null,
            TEST_VIEW_URN.toString(),
            null,
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
        viewFilter, // Verify that view filter was used.
        0,
        10);

    verifyMockViewService(mockService, TEST_VIEW_URN);
  }

  @Test
  public static void testApplyViewBaseFilter() throws Exception {

    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    buildCriterion("field", Condition.EQUAL, "test"))))));

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

    ViewService mockService = initMockViewService(TEST_VIEW_URN, info);

    Filter baseFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    buildCriterion("baseField", Condition.EQUAL, "baseTest"))))));

    EntityClient mockClient =
        initMockEntityClient(
            ImmutableList.of(Constants.DATASET_ENTITY_NAME),
            "",
            SearchUtils.combineFilters(baseFilter, viewFilter),
            0,
            10,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, mockService);

    final SearchAcrossEntitiesInput testInput =
        new SearchAcrossEntitiesInput(
            ImmutableList.of(EntityType.DATASET),
            "",
            0,
            10,
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
            null,
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
        10);

    verifyMockViewService(mockService, TEST_VIEW_URN);
  }

  @Test
  public static void testApplyViewNullBaseEntityTypes() throws Exception {
    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    buildCriterion("field", Condition.EQUAL, "test"))))));

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

    ViewService mockService = initMockViewService(TEST_VIEW_URN, info);

    EntityClient mockClient =
        initMockEntityClient(
            ImmutableList.of(Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME),
            "",
            viewFilter,
            0,
            10,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, mockService);

    final SearchAcrossEntitiesInput testInput =
        new SearchAcrossEntitiesInput(
            null, "", 0, 10, null, null, TEST_VIEW_URN.toString(), null, null);
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
        10);

    verifyMockViewService(mockService, TEST_VIEW_URN);
  }

  @Test
  public static void testApplyViewEmptyBaseEntityTypes() throws Exception {
    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    buildCriterion("field", Condition.EQUAL, "test"))))));

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

    ViewService mockService = initMockViewService(TEST_VIEW_URN, info);

    EntityClient mockClient =
        initMockEntityClient(
            ImmutableList.of(Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME),
            "",
            viewFilter,
            0,
            10,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, mockService);

    final SearchAcrossEntitiesInput testInput =
        new SearchAcrossEntitiesInput(
            Collections.emptyList(), // Empty Entity Types
            "",
            0,
            10,
            null,
            null,
            TEST_VIEW_URN.toString(),
            null,
            null);
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
        10);

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
            10,
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    final SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, mockService);
    final SearchAcrossEntitiesInput testInput =
        new SearchAcrossEntitiesInput(
            Collections.emptyList(), // Empty Entity Types
            "",
            0,
            10,
            null,
            null,
            TEST_VIEW_URN.toString(),
            null,
            null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    verifyMockEntityClient(mockClient, searchEntityTypes, "", null, 0, 10);
  }

  @Test
  public static void testApplyViewErrorFetchingView() throws Exception {
    // When a view cannot be successfully resolved, the endpoint show THROW.

    ViewService mockService = initMockViewService(TEST_VIEW_URN, null);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.eq(Collections.emptyList()),
                Mockito.eq(Collections.emptyList())))
        .thenThrow(new RemoteInvocationException());

    final SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, mockService);
    final SearchAcrossEntitiesInput testInput =
        new SearchAcrossEntitiesInput(
            Collections.emptyList(), // Empty Entity Types
            "",
            0,
            10,
            null,
            null,
            TEST_VIEW_URN.toString(),
            null,
            null);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static ViewService initMockViewService(Urn viewUrn, DataHubViewInfo viewInfo) {
    ViewService service = Mockito.mock(ViewService.class);
    Mockito.when(service.getViewInfo(any(), Mockito.eq(viewUrn))).thenReturn(viewInfo);
    return service;
  }

  private static EntityClient initMockEntityClient(
      List<String> entityTypes,
      String query,
      Filter filter,
      int start,
      int limit,
      SearchResult result)
      throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(
            client.searchAcrossEntities(
                any(),
                Mockito.argThat(
                    argument ->
                        argument != null
                            && argument.containsAll(entityTypes)
                            && entityTypes.containsAll(argument)),
                Mockito.eq(query),
                Mockito.eq(filter),
                Mockito.eq(start),
                Mockito.eq(limit),
                Mockito.eq(Collections.emptyList()),
                Mockito.eq(null)))
        .thenReturn(result);
    return client;
  }

  private static void verifyMockEntityClient(
      EntityClient mockClient,
      List<String> entityTypes,
      String query,
      Filter filter,
      int start,
      int limit)
      throws Exception {
    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(
            any(),
            Mockito.argThat(
                argument ->
                    argument != null
                        && argument.containsAll(entityTypes)
                        && entityTypes.containsAll(argument)),
            Mockito.eq(query),
            Mockito.eq(filter),
            Mockito.eq(start),
            Mockito.eq(limit),
            Mockito.eq(Collections.emptyList()),
            Mockito.eq(null));
  }

  private static void verifyMockViewService(ViewService mockService, Urn viewUrn) {
    Mockito.verify(mockService, Mockito.times(1)).getViewInfo(any(), Mockito.eq(viewUrn));
  }

  private SearchAcrossEntitiesResolverTest() {}
}
