package com.linkedin.datahub.graphql.resolvers.versioning;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SearchSortInput;
import com.linkedin.datahub.graphql.generated.SortCriterion;
import com.linkedin.datahub.graphql.generated.SortOrder;
import com.linkedin.datahub.graphql.generated.VersionSet;
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
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class VersionsSearchResolverTest {

  private static final String VERSION_SET_URN = "urn:li:versionSet:(my_version_set,dataset)";
  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  private static final SearchAcrossEntitiesInput BASIC_INPUT =
      new SearchAcrossEntitiesInput(
          List.of(EntityType.DATASET), "", 0, 10, null, null, null, null, null);

  private static final SearchAcrossEntitiesInput COMPLEX_INPUT =
      new SearchAcrossEntitiesInput(
          List.of(EntityType.CHART, EntityType.DATASET),
          "query",
          2,
          5,
          null,
          List.of(
              AndFilterInput.builder()
                  .setAnd(
                      List.of(
                          FacetFilterInput.builder()
                              .setField("field1")
                              .setValues(List.of("1", "2"))
                              .build(),
                          FacetFilterInput.builder()
                              .setField("field2")
                              .setValues(List.of("a"))
                              .build()))
                  .build(),
              AndFilterInput.builder()
                  .setAnd(
                      List.of(
                          FacetFilterInput.builder()
                              .setField("field3")
                              .setValues(List.of("3", "4"))
                              .build(),
                          FacetFilterInput.builder()
                              .setField("field4")
                              .setValues(List.of("b"))
                              .build()))
                  .build()),
          TEST_VIEW_URN.toString(),
          SearchFlags.builder().setSkipCache(true).build(),
          SearchSortInput.builder()
              .setSortCriteria(
                  List.of(
                      SortCriterion.builder()
                          .setField("sortField1")
                          .setSortOrder(SortOrder.DESCENDING)
                          .build(),
                      SortCriterion.builder()
                          .setField("sortField2")
                          .setSortOrder(SortOrder.ASCENDING)
                          .build()))
              .build());

  @Test
  public void testGetSuccessBasic() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient();
    ViewService mockViewService = Mockito.mock(ViewService.class);
    VersionsSearchResolver resolver = new VersionsSearchResolver(mockEntityClient, mockViewService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(BASIC_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    VersionSet versionSet = new VersionSet();
    versionSet.setUrn(VERSION_SET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(versionSet);

    SearchResults result = resolver.get(mockEnv).get();

    // Validate the result
    assertEquals(result.getSearchResults().size(), 0);

    // Validate that we called the search service correctly
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .searchAcrossEntities(
            Mockito.argThat(
                context ->
                    !context.getSearchContext().getSearchFlags().isFilterNonLatestVersions()),
            Mockito.eq(List.of(Constants.DATASET_ENTITY_NAME)),
            Mockito.eq("*"),
            Mockito.eq(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            new ConjunctiveCriterion()
                                .setAnd(
                                    new CriterionArray(
                                        CriterionUtils.buildCriterion(
                                            "versionSet", Condition.EQUAL, VERSION_SET_URN)))))),
            Mockito.eq(0),
            Mockito.eq(10),
            Mockito.eq(
                List.of(
                    new com.linkedin.metadata.query.filter.SortCriterion()
                        .setField(VERSION_SORT_ID_FIELD_NAME)
                        .setOrder(com.linkedin.metadata.query.filter.SortOrder.DESCENDING))),
            any());
  }

  @Test
  public void testGetSuccessComplex() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient();

    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                List.of(buildCriterion("viewField", Condition.EQUAL, "test"))))));
    DataHubViewInfo viewInfo =
        new DataHubViewInfo()
            .setName("test")
            .setType(DataHubViewType.GLOBAL)
            .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
            .setLastModified(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
            .setDefinition(
                new DataHubViewDefinition()
                    .setEntityTypes(
                        new StringArray(
                            List.of(
                                Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME)))
                    .setFilter(viewFilter));
    ViewService mockViewService = Mockito.mock(ViewService.class);
    Mockito.when(mockViewService.getViewInfo(any(), Mockito.eq(TEST_VIEW_URN)))
        .thenReturn(viewInfo);

    VersionsSearchResolver resolver = new VersionsSearchResolver(mockEntityClient, mockViewService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(COMPLEX_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    VersionSet versionSet = new VersionSet();
    versionSet.setUrn(VERSION_SET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(versionSet);

    SearchResults result = resolver.get(mockEnv).get();

    // Validate the result
    assertEquals(result.getSearchResults().size(), 0);

    // Validate that we called the search service correctly
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .searchAcrossEntities(
            Mockito.argThat(
                context ->
                    !context.getSearchContext().getSearchFlags().isFilterNonLatestVersions()
                        && context.getSearchContext().getSearchFlags().isSkipCache()),
            Mockito.eq(List.of(Constants.DATASET_ENTITY_NAME)),
            Mockito.eq("query"),
            Mockito.eq(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            new ConjunctiveCriterion()
                                .setAnd(
                                    new CriterionArray(
                                        CriterionUtils.buildCriterion(
                                            "field1", Condition.EQUAL, "1", "2"),
                                        CriterionUtils.buildCriterion(
                                            "field2", Condition.EQUAL, "a"),
                                        CriterionUtils.buildCriterion(
                                            "versionSet", Condition.EQUAL, VERSION_SET_URN),
                                        CriterionUtils.buildCriterion(
                                            "viewField", Condition.EQUAL, "test"))),
                            new ConjunctiveCriterion()
                                .setAnd(
                                    new CriterionArray(
                                        CriterionUtils.buildCriterion(
                                            "field3", Condition.EQUAL, "3", "4"),
                                        CriterionUtils.buildCriterion(
                                            "field4", Condition.EQUAL, "b"),
                                        CriterionUtils.buildCriterion(
                                            "versionSet", Condition.EQUAL, VERSION_SET_URN),
                                        CriterionUtils.buildCriterion(
                                            "viewField", Condition.EQUAL, "test")))))),
            Mockito.eq(2),
            Mockito.eq(5),
            Mockito.eq(
                List.of(
                    new com.linkedin.metadata.query.filter.SortCriterion()
                        .setField("sortField1")
                        .setOrder(com.linkedin.metadata.query.filter.SortOrder.DESCENDING),
                    new com.linkedin.metadata.query.filter.SortCriterion()
                        .setField("sortField2")
                        .setOrder(com.linkedin.metadata.query.filter.SortOrder.ASCENDING),
                    new com.linkedin.metadata.query.filter.SortCriterion()
                        .setField(VERSION_SORT_ID_FIELD_NAME)
                        .setOrder(com.linkedin.metadata.query.filter.SortOrder.DESCENDING))),
            any());
  }

  @Test
  public void testThrowsError() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient();
    ViewService mockViewService = Mockito.mock(ViewService.class);

    Mockito.when(
            mockEntityClient.searchAcrossEntities(
                any(), any(), any(), any(), Mockito.anyInt(), Mockito.anyInt(), any(), any()))
        .thenThrow(new RemoteInvocationException());

    VersionsSearchResolver resolver = new VersionsSearchResolver(mockEntityClient, mockViewService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(BASIC_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    VersionSet versionSet = new VersionSet();
    versionSet.setUrn(VERSION_SET_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(versionSet);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private EntityClient initMockEntityClient() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Mockito.when(
            client.searchAcrossEntities(
                any(),
                any(),
                Mockito.anyString(),
                any(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                any(),
                Mockito.eq(null)))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));

    return client;
  }
}
