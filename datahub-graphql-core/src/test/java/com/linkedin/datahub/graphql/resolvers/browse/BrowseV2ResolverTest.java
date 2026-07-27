package com.linkedin.datahub.graphql.resolvers.browse;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.BrowseResultsV2;
import com.linkedin.datahub.graphql.generated.BrowseV2Input;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.chart.BrowseV2Resolver;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.browse.BrowseResultGroupV2;
import com.linkedin.metadata.browse.BrowseResultGroupV2Array;
import com.linkedin.metadata.browse.BrowseResultMetadata;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BrowseV2ResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");

  @Test
  public static void testBrowseV2Success() throws Exception {
    FormService mockFormService = Mockito.mock(FormService.class);
    ViewService mockService = Mockito.mock(ViewService.class);
    EntityClient mockClient =
        initMockEntityClient(
            "dataset",
            "␟test␟path",
            "*",
            null,
            0,
            10,
            new BrowseResultV2()
                .setNumGroups(2)
                .setGroups(
                    new BrowseResultGroupV2Array(
                        new BrowseResultGroupV2()
                            .setCount(5)
                            .setName("first group")
                            .setHasSubGroups(true),
                        new BrowseResultGroupV2()
                            .setCount(4)
                            .setName("second group")
                            .setHasSubGroups(false)))
                .setMetadata(
                    new BrowseResultMetadata().setPath("␟test␟path").setTotalNumEntities(100))
                .setFrom(0)
                .setPageSize(10));

    final BrowseV2Resolver resolver =
        new BrowseV2Resolver(mockClient, mockService, mockFormService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BrowseV2Input input = new BrowseV2Input();
    input.setPath(ImmutableList.of("test", "path"));
    input.setType(EntityType.DATASET);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    BrowseResultsV2 result = resolver.get(mockEnv).get();

    compareResultToExpectedData(result, getExpectedResult());
  }

  @Test
  public static void testBrowseV2SuccessWithQueryAndFilter() throws Exception {
    FormService mockFormService = Mockito.mock(FormService.class);
    ViewService mockService = Mockito.mock(ViewService.class);

    List<AndFilterInput> orFilters = new ArrayList<>();
    AndFilterInput andFilterInput = new AndFilterInput();
    FacetFilterInput facetFilterInput = new FacetFilterInput();
    facetFilterInput.setField("owners");
    facetFilterInput.setValues(ImmutableList.of("urn:li:corpuser:test"));
    andFilterInput.setAnd(ImmutableList.of(facetFilterInput));
    orFilters.add(andFilterInput);
    Filter filter = ResolverUtils.buildFilter(null, orFilters);

    EntityClient mockClient =
        initMockEntityClient(
            "dataset",
            "␟test␟path",
            "test",
            filter,
            0,
            10,
            new BrowseResultV2()
                .setNumGroups(2)
                .setGroups(
                    new BrowseResultGroupV2Array(
                        new BrowseResultGroupV2()
                            .setCount(5)
                            .setName("first group")
                            .setHasSubGroups(true),
                        new BrowseResultGroupV2()
                            .setCount(4)
                            .setName("second group")
                            .setHasSubGroups(false)))
                .setMetadata(
                    new BrowseResultMetadata().setPath("␟test␟path").setTotalNumEntities(100))
                .setFrom(0)
                .setPageSize(10));

    final BrowseV2Resolver resolver =
        new BrowseV2Resolver(mockClient, mockService, mockFormService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BrowseV2Input input = new BrowseV2Input();
    input.setPath(ImmutableList.of("test", "path"));
    input.setType(EntityType.DATASET);
    input.setQuery("test");
    input.setOrFilters(orFilters);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    BrowseResultsV2 result = resolver.get(mockEnv).get();

    compareResultToExpectedData(result, getExpectedResult());
  }

  @Test
  public static void testBrowseV2SuccessWithView() throws Exception {
    DataHubViewInfo viewInfo = createViewInfo(new StringArray());
    FormService mockFormService = Mockito.mock(FormService.class);
    ViewService viewService = initMockViewService(TEST_VIEW_URN, viewInfo);

    EntityClient mockClient =
        initMockEntityClient(
            "dataset",
            "␟test␟path",
            "*",
            viewInfo.getDefinition().getFilter(),
            0,
            10,
            new BrowseResultV2()
                .setNumGroups(2)
                .setGroups(
                    new BrowseResultGroupV2Array(
                        new BrowseResultGroupV2()
                            .setCount(5)
                            .setName("first group")
                            .setHasSubGroups(true),
                        new BrowseResultGroupV2()
                            .setCount(4)
                            .setName("second group")
                            .setHasSubGroups(false)))
                .setMetadata(
                    new BrowseResultMetadata().setPath("␟test␟path").setTotalNumEntities(100))
                .setFrom(0)
                .setPageSize(10));

    final BrowseV2Resolver resolver =
        new BrowseV2Resolver(mockClient, viewService, mockFormService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BrowseV2Input input = new BrowseV2Input();
    input.setPath(ImmutableList.of("test", "path"));
    input.setType(EntityType.DATASET);
    input.setViewUrn(TEST_VIEW_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    BrowseResultsV2 result = resolver.get(mockEnv).get();

    compareResultToExpectedData(result, getExpectedResult());
  }

  private static void compareResultToExpectedData(
      BrowseResultsV2 result, BrowseResultsV2 expected) {
    Assert.assertEquals(result.getCount(), expected.getCount());
    Assert.assertEquals(result.getStart(), expected.getStart());
    Assert.assertEquals(result.getTotal(), expected.getTotal());
    Assert.assertEquals(result.getGroups().size(), expected.getGroups().size());
    result
        .getGroups()
        .forEach(
            group -> {
              Assert.assertTrue(
                  expected.getGroups().stream()
                          .filter(g -> g.getName().equals(group.getName()))
                          .count()
                      > 0);
            });
    Assert.assertEquals(result.getMetadata().getPath(), expected.getMetadata().getPath());
    Assert.assertEquals(
        result.getMetadata().getTotalNumEntities(), expected.getMetadata().getTotalNumEntities());
  }

  private static BrowseResultsV2 getExpectedResult() {
    BrowseResultsV2 results = new BrowseResultsV2();
    results.setTotal(2);
    results.setStart(0);
    results.setCount(10);

    List<com.linkedin.datahub.graphql.generated.BrowseResultGroupV2> groups = new ArrayList<>();
    com.linkedin.datahub.graphql.generated.BrowseResultGroupV2 browseGroup1 =
        new com.linkedin.datahub.graphql.generated.BrowseResultGroupV2();
    browseGroup1.setName("first group");
    browseGroup1.setCount(5L);
    browseGroup1.setHasSubGroups(true);
    groups.add(browseGroup1);
    com.linkedin.datahub.graphql.generated.BrowseResultGroupV2 browseGroup2 =
        new com.linkedin.datahub.graphql.generated.BrowseResultGroupV2();
    browseGroup2.setName("second group");
    browseGroup2.setCount(4L);
    browseGroup2.setHasSubGroups(false);
    groups.add(browseGroup2);
    results.setGroups(groups);

    com.linkedin.datahub.graphql.generated.BrowseResultMetadata resultMetadata =
        new com.linkedin.datahub.graphql.generated.BrowseResultMetadata();
    resultMetadata.setPath(ImmutableList.of("test", "path"));
    resultMetadata.setTotalNumEntities(100L);
    results.setMetadata(resultMetadata);

    return results;
  }

  private static EntityClient initMockEntityClient(
      String entityName,
      String path,
      String query,
      Filter filter,
      int start,
      int limit,
      BrowseResultV2 result)
      throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(
            client.browseV2(
                Mockito.any(),
                Mockito.eq(ImmutableList.of(entityName)),
                Mockito.eq(path),
                Mockito.eq(filter),
                Mockito.eq(query),
                Mockito.eq(start),
                Mockito.eq(limit)))
        .thenReturn(result);
    return client;
  }

  private static ViewService initMockViewService(Urn viewUrn, DataHubViewInfo viewInfo) {
    ViewService service = Mockito.mock(ViewService.class);
    Mockito.when(service.getViewInfo(any(), Mockito.eq(viewUrn))).thenReturn(viewInfo);
    return service;
  }

  private static DataHubViewInfo createViewInfo(StringArray entityNames) {
    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    buildCriterion("test", Condition.EQUAL, "test"))))));

    DataHubViewInfo info = new DataHubViewInfo();
    info.setName("test");
    info.setType(DataHubViewType.GLOBAL);
    info.setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    info.setLastModified(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    info.setDefinition(
        new DataHubViewDefinition().setEntityTypes(entityNames).setFilter(viewFilter));
    return info;
  }

  @Test
  public static void testDocumentTypeAppliesDefaultFilters() throws Exception {
    // When browsing DOCUMENT entities, default document filters (showInGlobalContext) should be
    // applied so bridge documents don't inflate browse counts.
    FormService mockFormService = Mockito.mock(FormService.class);
    ViewService mockService = Mockito.mock(ViewService.class);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.browseV2(
                any(),
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.any(Filter.class),
                Mockito.anyString(),
                Mockito.anyInt(),
                Mockito.anyInt()))
        .thenReturn(
            new BrowseResultV2()
                .setNumGroups(0)
                .setGroups(new BrowseResultGroupV2Array())
                .setMetadata(new BrowseResultMetadata().setPath("").setTotalNumEntities(0))
                .setFrom(0)
                .setPageSize(10));

    final BrowseV2Resolver resolver =
        new BrowseV2Resolver(mockClient, mockService, mockFormService);

    BrowseV2Input input = new BrowseV2Input();
    input.setPath(new ArrayList<>());
    input.setType(EntityType.DOCUMENT);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(mockClient)
        .browseV2(
            any(),
            eq(Collections.singletonList(Constants.DOCUMENT_ENTITY_NAME)),
            eq(""),
            filterCaptor.capture(),
            eq("*"),
            eq(0),
            eq(10));
    Filter appliedFilter = filterCaptor.getValue();
    Assert.assertNotNull(appliedFilter);
    Assert.assertTrue(
        appliedFilter.getOr().stream()
            .flatMap(clause -> clause.getAnd().stream())
            .anyMatch(c -> "showInGlobalContext".equals(c.getField())));
  }

  @Test
  public void testGetEntityNamesDefaultsIncludeDocument() {
    // With no explicit types, browseV2 falls back to BROWSE_ENTITY_TYPES. Documents
    // must be in that default so the platform-browse sidebar can navigate
    // document-only platforms (e.g. Confluence); otherwise their browse tree is empty.
    List<String> defaultEntityNames = BrowseV2Resolver.getEntityNames(new BrowseV2Input());
    Assert.assertTrue(
        defaultEntityNames.contains(EntityTypeMapper.getName(EntityType.DOCUMENT)),
        "Default browse entity types should include DOCUMENT");
  }

  private BrowseV2ResolverTest() {}
}
