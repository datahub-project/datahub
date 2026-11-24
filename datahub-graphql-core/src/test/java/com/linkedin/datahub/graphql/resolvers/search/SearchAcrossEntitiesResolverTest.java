package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.config.search.EntityTypeListConfig.DEFAULT_SEARCH_ENTITY_TYPES;
import static com.linkedin.metadata.config.search.EntityTypeListConfig.parseCsv;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SearchSortInput;
import com.linkedin.datahub.graphql.generated.SortCriterion;
import com.linkedin.datahub.graphql.generated.SortOrder;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.QueryEmbeddingException;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFailClosedException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateNoAnchorException;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
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
            FilterUtils.combineFilters(baseFilter, viewFilter),
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
        FilterUtils.combineFilters(baseFilter, viewFilter), // Verify that merged filters were used.
        0,
        10);

    verifyMockViewService(mockService, TEST_VIEW_URN);
  }

  @Test
  public static void testFilterRemovesDuplicates() throws Exception {
    // Has no duplicate values
    Filter expectedFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    buildCriterion("baseField", Condition.EQUAL, "baseTest"))))));

    ViewService mockService = Mockito.mock(ViewService.class);
    EntityClient mockClient =
        initMockEntityClient(
            ImmutableList.of(Constants.DATASET_ENTITY_NAME),
            "",
            expectedFilter,
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
                            ImmutableList.of("baseTest", "baseTest"),
                            false,
                            FilterOperator.EQUAL)))),
            null,
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
        expectedFilter,
        0,
        10);
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
  public static void testSearchWithSearchFlagsAndSortInput() throws Exception {
    ViewService mockService = Mockito.mock(ViewService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SearchResult emptyResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(0)
            .setFrom(0)
            .setPageSize(0)
            .setMetadata(new SearchResultMetadata());
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), eq("query"), any(), eq(0), eq(10), any(), any()))
        .thenReturn(emptyResult);

    SearchAcrossEntitiesInput input =
        new SearchAcrossEntitiesInput(
            ImmutableList.of(EntityType.DATASET),
            "query",
            0,
            10,
            null,
            null,
            null,
            new SearchFlags(),
            null);
    SearchSortInput sortInput = new SearchSortInput();
    SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField("name");
    sortCriterion.setSortOrder(SortOrder.ASCENDING);
    sortInput.setSortCriteria(ImmutableList.of(sortCriterion));
    input.setSortInput(sortInput);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, mockService);
    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(any(), any(), eq("query"), any(), eq(0), eq(10), any(), any());
  }

  @Test
  public static void testSearchWithStructuredPropertyFacets() throws Exception {
    ViewService mockService = Mockito.mock(ViewService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SearchResult emptyResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(0)
            .setFrom(0)
            .setPageSize(0)
            .setMetadata(new SearchResultMetadata());
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
        .thenReturn(emptyResult);

    SearchFlags flags = new SearchFlags();
    flags.setIncludeStructuredPropertyFacets(true);
    SearchAcrossEntitiesInput input =
        new SearchAcrossEntitiesInput(
            ImmutableList.of(EntityType.DATASET), "q", 0, 10, null, null, null, flags, null);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, mockService);
    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.atLeast(1))
        .searchAcrossEntities(any(), any(), any(), any(), anyInt(), anyInt(), any(), any());
  }

  @Test
  public static void testApplyViewViewDoesNotExist() throws Exception {
    // When a view does not exist, the endpoint should WARN and not apply the view.
    // Note: Since DOCUMENT is in the default search entity types, document default filters
    // (state=PUBLISHED, showInGlobalContext=true) are automatically applied.

    ViewService mockService = initMockViewService(TEST_VIEW_URN, null);

    List<String> searchEntityTypes = parseCsv(DEFAULT_SEARCH_ENTITY_TYPES);

    // Create the expected document filter that gets applied when DOCUMENT is in entity types
    Filter expectedDocumentFilter = buildDocumentDefaultFilter();

    EntityClient mockClient =
        initMockEntityClient(
            searchEntityTypes,
            "",
            expectedDocumentFilter,
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

    verifyMockEntityClient(mockClient, searchEntityTypes, "", expectedDocumentFilter, 0, 10);
  }

  /**
   * Builds the default document filter applied when DOCUMENT is in entity types. Since
   * getMockAllowContext allows all privileges (canManageDocuments=true), the legacy clause omits
   * the state != UNPUBLISHED filter.
   */
  private static Filter buildDocumentDefaultFilter() {
    Criterion lifecycleCriterion = new Criterion();
    lifecycleCriterion.setField("lifecycleStage");
    lifecycleCriterion.setCondition(Condition.EQUAL);
    lifecycleCriterion.setValues(
        new com.linkedin.data.template.StringArray(
            Collections.singletonList("urn:li:lifecycleStageType:PUBLISHED")));

    ConjunctiveCriterion publishedClause =
        new ConjunctiveCriterion()
            .setAnd(
                new CriterionArray(
                    ImmutableList.of(lifecycleCriterion, buildShowInGlobalContextCriterion())));

    Criterion isNullCriterion = new Criterion();
    isNullCriterion.setField("lifecycleStage");
    isNullCriterion.setCondition(Condition.IS_NULL);

    ConjunctiveCriterion legacyClause =
        new ConjunctiveCriterion()
            .setAnd(
                new CriterionArray(
                    ImmutableList.of(isNullCriterion, buildShowInGlobalContextCriterion())));

    return new Filter()
        .setOr(new ConjunctiveCriterionArray(ImmutableList.of(publishedClause, legacyClause)));
  }

  private static Criterion buildShowInGlobalContextCriterion() {
    Criterion criterion = new Criterion();
    criterion.setField("showInGlobalContext");
    criterion.setCondition(Condition.EQUAL);
    criterion.setValues(
        new com.linkedin.data.template.StringArray(Collections.singletonList("false")));
    criterion.setNegated(true);
    return criterion;
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

  @Test
  public static void testRecordsWhetherTheCallerSetAFilterBeforeTheDefaults() throws Exception {
    // A vector store that cannot filter reads this to tell the caller's filter from the
    // published-document default the resolver adds
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));
    DataHubViewInfo view = new DataHubViewInfo();
    view.setName("test");
    view.setType(DataHubViewType.GLOBAL);
    view.setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    view.setLastModified(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    view.setDefinition(
        new DataHubViewDefinition()
            .setEntityTypes(new StringArray(ImmutableList.of(Constants.DOCUMENT_ENTITY_NAME)))
            .setFilter(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            new ConjunctiveCriterion()
                                .setAnd(
                                    new CriterionArray(
                                        ImmutableList.of(
                                            buildCriterion("field", Condition.EQUAL, "test"))))))));
    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, initMockViewService(TEST_VIEW_URN, view));
    List<AndFilterInput> facet =
        ImmutableList.of(
            new AndFilterInput(
                ImmutableList.of(
                    new FacetFilterInput(
                        "platform",
                        ImmutableList.of("urn:li:dataPlatform:hive"),
                        false,
                        FilterOperator.EQUAL))));

    for (SearchAcrossEntitiesInput input :
        ImmutableList.of(
            new SearchAcrossEntitiesInput(
                ImmutableList.of(EntityType.DOCUMENT), "q", 0, 10, null, null, null, null, null),
            new SearchAcrossEntitiesInput(
                ImmutableList.of(EntityType.DOCUMENT), "q", 0, 10, null, facet, null, null, null),
            new SearchAcrossEntitiesInput(
                ImmutableList.of(EntityType.DOCUMENT),
                "q",
                0,
                10,
                null,
                null,
                TEST_VIEW_URN.toString(),
                null,
                null))) {
      DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
      QueryContext mockContext = getMockAllowContext();
      Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
      Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
      resolver.get(mockEnv).get();
    }

    // No filter, then a facet the caller set, then a view the caller chose
    ArgumentCaptor<OperationContext> contexts = ArgumentCaptor.forClass(OperationContext.class);
    Mockito.verify(mockClient, Mockito.times(3))
        .searchAcrossEntities(
            contexts.capture(), any(), any(), any(), anyInt(), anyInt(), any(), any());
    Assert.assertEquals(
        contexts.getAllValues().stream()
            .map(context -> context.getSearchContext().getCallerFiltered())
            .collect(Collectors.toList()),
        List.of(false, true, true));
  }

  @Test
  public static void testHoldsASimilarityAnchorToTheDocumentDefaults() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));
    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, Mockito.mock(ViewService.class));
    SearchFlags hiddenStages = new SearchFlags();
    hiddenStages.setIncludeHiddenLifecycleStages(true);

    for (SearchAcrossEntitiesInput input :
        ImmutableList.of(
            new SearchAcrossEntitiesInput(
                ImmutableList.of(EntityType.DOCUMENT), "q", 0, 10, null, null, null, null, null),
            new SearchAcrossEntitiesInput(
                ImmutableList.of(EntityType.DATASET), "q", 0, 10, null, null, null, null, null),
            new SearchAcrossEntitiesInput(
                ImmutableList.of(EntityType.DOCUMENT),
                "q",
                0,
                10,
                null,
                null,
                null,
                hiddenStages,
                null))) {
      DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
      QueryContext mockContext = getMockAllowContext();
      Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
      Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
      resolver.get(mockEnv).get();
    }

    // A search of datasets adds no defaults to its filter, but it can still name a document as its
    // anchor, so the anchor is held to them. Only includeHiddenLifecycleStages lifts them.
    ArgumentCaptor<OperationContext> contexts = ArgumentCaptor.forClass(OperationContext.class);
    Mockito.verify(mockClient, Mockito.times(3))
        .searchAcrossEntities(
            contexts.capture(), any(), any(), any(), anyInt(), anyInt(), any(), any());
    Assert.assertEquals(
        contexts.getAllValues().stream()
            .map(context -> context.getSearchContext().getAnchorFilter())
            .collect(Collectors.toList()),
        Arrays.asList(buildDocumentDefaultFilter(), buildDocumentDefaultFilter(), null));
  }

  @Test
  public static void testAViewFilterOnPlacementLiftsThePlacementClauseInItsBranch()
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));
    // The view matches documents hidden from global context, or documents in a domain
    DataHubViewInfo view = new DataHubViewInfo();
    view.setName("test");
    view.setType(DataHubViewType.GLOBAL);
    view.setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    view.setLastModified(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    view.setDefinition(
        new DataHubViewDefinition()
            .setEntityTypes(new StringArray(ImmutableList.of(Constants.DOCUMENT_ENTITY_NAME)))
            .setFilter(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            new ConjunctiveCriterion()
                                .setAnd(
                                    new CriterionArray(
                                        buildCriterion(
                                            "showInGlobalContext", Condition.EQUAL, "false"))),
                            new ConjunctiveCriterion()
                                .setAnd(
                                    new CriterionArray(
                                        buildCriterion(
                                            "domains",
                                            Condition.EQUAL,
                                            "urn:li:domain:sales")))))));
    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, initMockViewService(TEST_VIEW_URN, view));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(
            new SearchAcrossEntitiesInput(
                ImmutableList.of(EntityType.DOCUMENT),
                "q",
                0,
                10,
                null,
                null,
                TEST_VIEW_URN.toString(),
                null,
                null));
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    ArgumentCaptor<Filter> filter = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(mockClient)
        .searchAcrossEntities(
            any(), any(), any(), filter.capture(), anyInt(), anyInt(), any(), any());
    // Each view branch joined with the published clause and the legacy clause. Only the two
    // that carry the view's placement criterion lose the placement default.
    List<ConjunctiveCriterion> branches = filter.getValue().getOr();
    Assert.assertEquals(branches.size(), 4);
    int lifted = 0;
    for (ConjunctiveCriterion branch : branches) {
      boolean viewNamesPlacement = hasPlacementCriterion(branch, false);
      Assert.assertEquals(hasPlacementCriterion(branch, true), !viewNamesPlacement, branch + "");
      lifted += viewNamesPlacement ? 1 : 0;
    }
    Assert.assertEquals(lifted, 2);
  }

  private static boolean hasPlacementCriterion(ConjunctiveCriterion branch, boolean negated) {
    return branch.getAnd().stream()
        .anyMatch(
            criterion ->
                "showInGlobalContext".equals(criterion.getField())
                    && criterion.isNegated() == negated);
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
                Mockito.eq(Collections.emptyList())))
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
            Mockito.eq(Collections.emptyList()));
  }

  private static void verifyMockViewService(ViewService mockService, Urn viewUrn) {
    Mockito.verify(mockService, Mockito.times(1)).getViewInfo(any(), Mockito.eq(viewUrn));
  }

  @Test
  public static void testStructuredPropertyFacetsScopedToSearchedEntityTypes() throws Exception {
    // Given: three enabled structured properties — one scoped to datasets, one scoped to glossary
    // terms, and one with no entityTypes in its search document.
    SearchEntity datasetOnlySp =
        new SearchEntity()
            .setEntity(UrnUtils.getUrn("urn:li:structuredProperty:datasetOnly"))
            .setExtraFields(
                new StringMap(Map.of("entityTypes", "[\"urn:li:entityType:datahub.dataset\"]")));
    SearchEntity termScopedSp =
        new SearchEntity()
            .setEntity(UrnUtils.getUrn("urn:li:structuredProperty:termScoped"))
            .setExtraFields(
                new StringMap(
                    Map.of("entityTypes", "[\"urn:li:entityType:datahub.glossaryTerm\"]")));
    SearchEntity noTypesSp =
        new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:structuredProperty:noTypes"));
    SearchResult spFetchResult =
        new SearchResult()
            .setEntities(new SearchEntityArray(datasetOnlySp, termScopedSp, noTypesSp))
            .setFrom(0)
            .setPageSize(100)
            .setNumEntities(3)
            .setMetadata(new SearchResultMetadata());

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    // Structured property fetch (7-arg overload used by getStructuredPropertyFacets)
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                anyList(),
                Mockito.eq("*"),
                any(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.eq(Collections.emptyList())))
        .thenReturn(spFetchResult);
    // Main search call
    SearchResult emptyResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(0)
            .setMetadata(new SearchResultMetadata());
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                anyList(),
                Mockito.anyString(),
                any(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                anyList(),
                anyList()))
        .thenReturn(emptyResult);

    ViewService mockService = Mockito.mock(ViewService.class);
    final SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, mockService);

    // When: searching only glossary terms with structured property facets enabled
    final SearchAcrossEntitiesInput testInput = new SearchAcrossEntitiesInput();
    testInput.setTypes(ImmutableList.of(EntityType.GLOSSARY_TERM));
    testInput.setQuery("");
    testInput.setStart(0);
    testInput.setCount(10);
    SearchFlags searchFlags = new SearchFlags();
    searchFlags.setIncludeStructuredPropertyFacets(true);
    testInput.setSearchFlags(searchFlags);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).join();

    // Then: the dataset-scoped property is excluded; the matching and the entityTypes-less
    // (fail-open) properties are kept.
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<String>> facetsCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(
            any(),
            anyList(),
            Mockito.anyString(),
            any(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            anyList(),
            facetsCaptor.capture());
    Assert.assertEquals(
        facetsCaptor.getValue(),
        ImmutableList.of("structuredProperties.termScoped", "structuredProperties.noTypes"));
  }

  private SearchAcrossEntitiesResolverTest() {}

  @Test
  public static void testANamedTemplateThatCannotEmbedTheQueryFailsWithItsReason()
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, Mockito.mock(ViewService.class));
    SearchAcrossEntitiesInput input = namedTemplateSearch("urn:li:searchTemplate:semantic");

    // As on semantic search: a timeout is an exceeded deadline, and a throttle is a transient
    // unavailability, each naming the reason
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
        .thenThrow(embeddingFailure(QueryEmbeddingException.TIMEOUT))
        .thenThrow(embeddingFailure(QueryEmbeddingException.REJECTED));
    DataHubGraphQLException timedOut = typedError(resolver, input);
    Assert.assertEquals(timedOut.errorCode(), DataHubGraphQLErrorCode.DEADLINE_EXCEEDED);
    Assert.assertTrue(timedOut.getMessage().contains(QueryEmbeddingException.TIMEOUT));
    DataHubGraphQLException throttled = typedError(resolver, input);
    Assert.assertEquals(throttled.errorCode(), DataHubGraphQLErrorCode.SERVICE_UNAVAILABLE);
    Assert.assertTrue(throttled.getMessage().contains(QueryEmbeddingException.REJECTED));
    // Without the flag nothing else runs
    Mockito.verify(mockClient, Mockito.times(2))
        .searchAcrossEntities(any(), any(), any(), any(), anyInt(), anyInt(), any(), any());
  }

  @Test
  public static void testTheKeywordFallbackRunsTheSameSearchWithoutTheNamedTemplate()
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
        .thenThrow(embeddingFailure(QueryEmbeddingException.REJECTED))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(
                        new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:document:revenue"))))
                .setNumEntities(1)
                .setFrom(0)
                .setPageSize(10)
                .setMetadata(new SearchResultMetadata()));
    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, Mockito.mock(ViewService.class));
    SearchAcrossEntitiesInput input = namedTemplateSearch("urn:li:searchTemplate:semantic");
    input.getSearchFlags().setKeywordFallbackOnEmbeddingFailure(true);

    SearchResults result = resolver.get(environment(input)).get();

    Assert.assertEquals(result.getKeywordFallback(), QueryEmbeddingException.REJECTED);
    Assert.assertEquals(result.getTotal(), 1);
    ArgumentCaptor<OperationContext> contexts = ArgumentCaptor.forClass(OperationContext.class);
    ArgumentCaptor<String> queries = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Filter> filters = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(mockClient, Mockito.times(2))
        .searchAcrossEntities(
            contexts.capture(),
            any(),
            queries.capture(),
            filters.capture(),
            eq(0),
            eq(10),
            any(),
            any());
    Assert.assertTrue(
        contexts.getAllValues().get(0).getSearchContext().getSearchFlags().hasSearchTemplate());
    Assert.assertFalse(
        contexts.getAllValues().get(1).getSearchContext().getSearchFlags().hasSearchTemplate());
    Assert.assertEquals(queries.getAllValues().get(1), queries.getAllValues().get(0));
    Assert.assertEquals(filters.getAllValues().get(1), filters.getAllValues().get(0));
  }

  @Test
  public static void testWithNoTemplateNamedTheEmbeddingErrorStandsAtOnce() throws Exception {
    // A slot or route selected the vector rung, so dropping a template would run it again
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
        .thenThrow(embeddingFailure(QueryEmbeddingException.REJECTED));
    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, Mockito.mock(ViewService.class));
    SearchFlags flags = new SearchFlags();
    flags.setKeywordFallbackOnEmbeddingFailure(true);
    SearchAcrossEntitiesInput input =
        new SearchAcrossEntitiesInput(
            ImmutableList.of(EntityType.DOCUMENT),
            "quarterly revenue",
            0,
            10,
            null,
            null,
            null,
            flags,
            null);

    Assert.assertEquals(
        typedError(resolver, input).errorCode(), DataHubGraphQLErrorCode.SERVICE_UNAVAILABLE);
    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(any(), any(), any(), any(), anyInt(), anyInt(), any(), any());
  }

  @Test
  public static void testAFallbackThatRunsAVectorRungAgainKeepsTheEmbeddingError()
      throws Exception {
    // The named template is dropped, and a slot or route selects a vector rung for the rest
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
        .thenThrow(embeddingFailure(QueryEmbeddingException.REJECTED))
        .thenThrow(embeddingFailure(QueryEmbeddingException.TIMEOUT));
    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, Mockito.mock(ViewService.class));
    SearchAcrossEntitiesInput input = namedTemplateSearch("urn:li:searchTemplate:semantic");
    input.getSearchFlags().setKeywordFallbackOnEmbeddingFailure(true);

    DataHubGraphQLException error = typedError(resolver, input);

    Assert.assertEquals(error.errorCode(), DataHubGraphQLErrorCode.SERVICE_UNAVAILABLE);
    Assert.assertTrue(error.getMessage().contains(QueryEmbeddingException.REJECTED));
    Mockito.verify(mockClient, Mockito.times(2))
        .searchAcrossEntities(any(), any(), any(), any(), anyInt(), anyInt(), any(), any());
  }

  @Test
  public static void testSimilarityNeverFallsBackAndAnUnusableAnchorIsABadRequest()
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
        .thenThrow(
            new SearchTemplateNoAnchorException(
                "Search template urn:li:searchTemplate:similarity finds similar entities, and"
                    + " urn:li:document:revenue has no stored vectors for document"))
        .thenThrow(
            new SearchTemplateFailClosedException(
                "Search template urn:li:searchTemplate:similarity could not read the anchor"));
    SearchAcrossEntitiesResolver resolver =
        new SearchAcrossEntitiesResolver(mockClient, Mockito.mock(ViewService.class));
    SearchAcrossEntitiesInput input = namedTemplateSearch("urn:li:searchTemplate:similarity");
    input.getSearchFlags().setKeywordFallbackOnEmbeddingFailure(true);

    // An anchor the request cannot use is the caller's error, as on semantic search
    Assert.assertEquals(
        typedError(resolver, input).errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    // Similarity embeds nothing, so the flag does not apply and another failure is not retyped
    CompletionException failure =
        Assert.expectThrows(
            CompletionException.class, () -> resolver.get(environment(input)).join());
    Assert.assertFalse(failure.getCause() instanceof DataHubGraphQLException, failure.toString());
    Mockito.verify(mockClient, Mockito.times(2))
        .searchAcrossEntities(any(), any(), any(), any(), anyInt(), anyInt(), any(), any());
  }

  /** A keyword search that names {@code template} in the searchTemplate flag. */
  private static SearchAcrossEntitiesInput namedTemplateSearch(String template) {
    SearchFlags flags = new SearchFlags();
    flags.setSearchTemplate(template);
    return new SearchAcrossEntitiesInput(
        ImmutableList.of(EntityType.DOCUMENT),
        "quarterly revenue",
        0,
        10,
        null,
        null,
        null,
        flags,
        null);
  }

  /** How a vector rung that could not embed the query fails the search below the resolver. */
  private static RuntimeException embeddingFailure(String reason) {
    return new SearchTemplateFailClosedException(
        "Search template urn:li:searchTemplate:semantic failed to retrieve by vector",
        new QueryEmbeddingException(reason, null));
  }

  private static DataFetchingEnvironment environment(SearchAcrossEntitiesInput input) {
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    return mockEnv;
  }

  private static DataHubGraphQLException typedError(
      SearchAcrossEntitiesResolver resolver, SearchAcrossEntitiesInput input) {
    CompletionException failure =
        Assert.expectThrows(
            CompletionException.class, () -> resolver.get(environment(input)).join());
    Assert.assertTrue(failure.getCause() instanceof DataHubGraphQLException, failure.toString());
    return (DataHubGraphQLException) failure.getCause();
  }
}
