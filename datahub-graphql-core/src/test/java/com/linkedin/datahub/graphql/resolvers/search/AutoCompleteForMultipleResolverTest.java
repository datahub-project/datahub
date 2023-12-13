package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.dashboard.DashboardType;
import com.linkedin.datahub.graphql.types.dataflow.DataFlowType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.AutoCompleteEntityArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AutoCompleteForMultipleResolverTest {

  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  private AutoCompleteForMultipleResolverTest() {}

  public static void testAutoCompleteResolverSuccess(
      EntityClient mockClient,
      ViewService viewService,
      String entityName,
      EntityType entityType,
      SearchableEntityType<?, ?> entity,
      Urn viewUrn,
      Filter filter)
      throws Exception {
    final AutoCompleteForMultipleResolver resolver =
        new AutoCompleteForMultipleResolver(ImmutableList.of(entity), viewService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    AutoCompleteMultipleInput input = new AutoCompleteMultipleInput();
    input.setQuery("test");
    input.setTypes(ImmutableList.of(entityType));
    input.setLimit(10);
    if (viewUrn != null) {
      input.setViewUrn(viewUrn.toString());
    }
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();
    verifyMockEntityClient(mockClient, entityName, "test", filter, 10);
  }

  // test our main entity types
  @Test
  public static void testAutoCompleteResolverSuccessForDifferentEntities() throws Exception {
    ViewService viewService = initMockViewService(null, null);
    // Daatasets
    EntityClient mockClient =
        initMockEntityClient(
            Constants.DATASET_ENTITY_NAME,
            "test",
            null,
            10,
            new AutoCompleteResult()
                .setQuery("test")
                .setEntities(new AutoCompleteEntityArray())
                .setSuggestions(new StringArray()));
    testAutoCompleteResolverSuccess(
        mockClient,
        viewService,
        Constants.DATASET_ENTITY_NAME,
        EntityType.DATASET,
        new DatasetType(mockClient),
        null,
        null);

    // Dashboards
    mockClient =
        initMockEntityClient(
            Constants.DASHBOARD_ENTITY_NAME,
            "test",
            null,
            10,
            new AutoCompleteResult()
                .setQuery("test")
                .setEntities(new AutoCompleteEntityArray())
                .setSuggestions(new StringArray()));
    testAutoCompleteResolverSuccess(
        mockClient,
        viewService,
        Constants.DASHBOARD_ENTITY_NAME,
        EntityType.DASHBOARD,
        new DashboardType(mockClient),
        null,
        null);

    // DataFlows
    mockClient =
        initMockEntityClient(
            Constants.DATA_FLOW_ENTITY_NAME,
            "test",
            null,
            10,
            new AutoCompleteResult()
                .setQuery("test")
                .setEntities(new AutoCompleteEntityArray())
                .setSuggestions(new StringArray()));
    testAutoCompleteResolverSuccess(
        mockClient,
        viewService,
        Constants.DATA_FLOW_ENTITY_NAME,
        EntityType.DATA_FLOW,
        new DataFlowType(mockClient),
        null,
        null);
  }

  // test filters with a given view
  @Test
  public static void testAutoCompleteResolverWithViewFilter() throws Exception {
    DataHubViewInfo viewInfo = createViewInfo(new StringArray());
    ViewService viewService = initMockViewService(TEST_VIEW_URN, viewInfo);
    EntityClient mockClient =
        initMockEntityClient(
            Constants.DATASET_ENTITY_NAME,
            "test",
            null,
            10,
            new AutoCompleteResult()
                .setQuery("test")
                .setEntities(new AutoCompleteEntityArray())
                .setSuggestions(new StringArray()));
    testAutoCompleteResolverSuccess(
        mockClient,
        viewService,
        Constants.DATASET_ENTITY_NAME,
        EntityType.DATASET,
        new DatasetType(mockClient),
        TEST_VIEW_URN,
        viewInfo.getDefinition().getFilter());
  }

  // test entity type filters with a given view
  @Test
  public static void testAutoCompleteResolverWithViewEntityFilter() throws Exception {
    // have view be a filter to only get dashboards
    StringArray entityNames = new StringArray();
    entityNames.add(Constants.DASHBOARD_ENTITY_NAME);
    DataHubViewInfo viewInfo = createViewInfo(entityNames);
    ViewService viewService = initMockViewService(TEST_VIEW_URN, viewInfo);
    EntityClient mockClient =
        initMockEntityClient(
            Constants.DASHBOARD_ENTITY_NAME,
            "test",
            null,
            10,
            new AutoCompleteResult()
                .setQuery("test")
                .setEntities(new AutoCompleteEntityArray())
                .setSuggestions(new StringArray()));

    // ensure we do hit the entity client for dashboards since dashboards are in our view
    testAutoCompleteResolverSuccess(
        mockClient,
        viewService,
        Constants.DASHBOARD_ENTITY_NAME,
        EntityType.DASHBOARD,
        new DashboardType(mockClient),
        TEST_VIEW_URN,
        viewInfo.getDefinition().getFilter());

    // if the view has only dashboards, we should not make an auto-complete request on other entity
    // types
    Mockito.verify(mockClient, Mockito.times(0))
        .autoComplete(
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq("test"),
            Mockito.eq(viewInfo.getDefinition().getFilter()),
            Mockito.eq(10),
            Mockito.any(Authentication.class));
  }

  @Test
  public static void testAutoCompleteResolverFailNoQuery() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ViewService viewService = initMockViewService(null, null);
    final AutoCompleteForMultipleResolver resolver =
        new AutoCompleteForMultipleResolver(
            ImmutableList.of(new DatasetType(mockClient)), viewService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    AutoCompleteMultipleInput input = new AutoCompleteMultipleInput();
    // don't set query on input
    input.setTypes(ImmutableList.of(EntityType.DATASET));
    input.setLimit(10);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(ValidationException.class, () -> resolver.get(mockEnv).join());
  }

  private static EntityClient initMockEntityClient(
      String entityName, String query, Filter filters, int limit, AutoCompleteResult result)
      throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(
            client.autoComplete(
                Mockito.eq(entityName),
                Mockito.eq(query),
                Mockito.eq(filters),
                Mockito.eq(limit),
                Mockito.any(Authentication.class)))
        .thenReturn(result);
    return client;
  }

  private static ViewService initMockViewService(Urn viewUrn, DataHubViewInfo viewInfo) {
    ViewService service = Mockito.mock(ViewService.class);
    Mockito.when(service.getViewInfo(Mockito.eq(viewUrn), Mockito.any(Authentication.class)))
        .thenReturn(viewInfo);
    return service;
  }

  private static void verifyMockEntityClient(
      EntityClient mockClient, String entityName, String query, Filter filters, int limit)
      throws Exception {
    Mockito.verify(mockClient, Mockito.times(1))
        .autoComplete(
            Mockito.eq(entityName),
            Mockito.eq(query),
            Mockito.eq(filters),
            Mockito.eq(limit),
            Mockito.any(Authentication.class));
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
                                    new Criterion()
                                        .setField("field")
                                        .setValue("test")
                                        .setValues(new StringArray(ImmutableList.of("test"))))))));

    DataHubViewInfo info = new DataHubViewInfo();
    info.setName("test");
    info.setType(DataHubViewType.GLOBAL);
    info.setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    info.setLastModified(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
    info.setDefinition(
        new DataHubViewDefinition().setEntityTypes(entityNames).setFilter(viewFilter));
    return info;
  }
}
