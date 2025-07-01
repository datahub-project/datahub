package com.linkedin.datahub.graphql.resolvers.view;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.DataHubViewDefinitionInput;
import com.linkedin.datahub.graphql.generated.DataHubViewFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.LogicalOperator;
import com.linkedin.datahub.graphql.generated.UpdateViewInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateViewResolverTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:dataHubView:test-id");
  private static final Urn TEST_AUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:auth");
  private static final Urn TEST_UNAUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:no-auth");

  private static final UpdateViewInput TEST_INPUT =
      new UpdateViewInput(
          "test-name",
          "test-description",
          new DataHubViewDefinitionInput(
              ImmutableList.of(EntityType.DATASET, EntityType.DASHBOARD),
              new DataHubViewFilterInput(
                  LogicalOperator.AND,
                  ImmutableList.of(
                      new FacetFilterInput(
                          "test1",
                          null,
                          ImmutableList.of("value1", "value2"),
                          false,
                          FilterOperator.EQUAL),
                      new FacetFilterInput(
                          "test2",
                          null,
                          ImmutableList.of("value1", "value2"),
                          true,
                          FilterOperator.IN)))));

  @Test
  public void testGetSuccessGlobalViewIsCreator() throws Exception {
    ViewService mockService = initViewService(DataHubViewType.GLOBAL);
    UpdateViewResolver resolver = new UpdateViewResolver(mockService);

    // Execute resolver - user is allowed since he owns the thing.
    QueryContext mockContext = getMockDenyContext(TEST_AUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubView view = resolver.get(mockEnv).get();
    assertEquals(view.getName(), TEST_INPUT.getName());
    assertEquals(view.getDescription(), TEST_INPUT.getDescription());
    assertEquals(view.getViewType(), com.linkedin.datahub.graphql.generated.DataHubViewType.GLOBAL);
    assertEquals(view.getType(), EntityType.DATAHUB_VIEW);

    Mockito.verify(mockService, Mockito.times(1))
        .updateView(
            any(),
            Mockito.eq(TEST_URN),
            Mockito.eq(TEST_INPUT.getName()),
            Mockito.eq(TEST_INPUT.getDescription()),
            Mockito.eq(
                new DataHubViewDefinition()
                    .setEntityTypes(
                        new StringArray(
                            ImmutableList.of(
                                Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME)))
                    .setFilter(
                        new Filter()
                            .setOr(
                                new ConjunctiveCriterionArray(
                                    ImmutableList.of(
                                        new ConjunctiveCriterion()
                                            .setAnd(
                                                new CriterionArray(
                                                    ImmutableList.of(
                                                        buildCriterion(
                                                            "test1",
                                                            Condition.EQUAL,
                                                            "value1",
                                                            "value2"),
                                                        buildCriterion(
                                                            "test2",
                                                            Condition.IN,
                                                            true,
                                                            "value1",
                                                            "value2"))))))))),
            Mockito.anyLong());
  }

  @Test
  public void testGetSuccessGlobalViewManageGlobalViews() throws Exception {
    ViewService mockService = initViewService(DataHubViewType.GLOBAL);
    UpdateViewResolver resolver = new UpdateViewResolver(mockService);

    // Execute resolver - user is allowed since he owns the thing.
    QueryContext mockContext = getMockAllowContext(TEST_UNAUTHORIZED_USER.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DataHubView view = resolver.get(mockEnv).get();
    assertEquals(view.getName(), TEST_INPUT.getName());
    assertEquals(view.getDescription(), TEST_INPUT.getDescription());
    assertEquals(view.getViewType(), com.linkedin.datahub.graphql.generated.DataHubViewType.GLOBAL);
    assertEquals(view.getType(), EntityType.DATAHUB_VIEW);

    Mockito.verify(mockService, Mockito.times(1))
        .updateView(
            any(),
            Mockito.eq(TEST_URN),
            Mockito.eq(TEST_INPUT.getName()),
            Mockito.eq(TEST_INPUT.getDescription()),
            Mockito.eq(
                new DataHubViewDefinition()
                    .setEntityTypes(
                        new StringArray(
                            ImmutableList.of(
                                Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME)))
                    .setFilter(
                        new Filter()
                            .setOr(
                                new ConjunctiveCriterionArray(
                                    ImmutableList.of(
                                        new ConjunctiveCriterion()
                                            .setAnd(
                                                new CriterionArray(
                                                    ImmutableList.of(
                                                        buildCriterion(
                                                            "test1",
                                                            Condition.EQUAL,
                                                            "value1",
                                                            "value2"),
                                                        buildCriterion(
                                                            "test2",
                                                            Condition.IN,
                                                            true,
                                                            "value1",
                                                            "value2"))))))))),
            Mockito.anyLong());
  }

  @Test
  public void testGetViewServiceException() throws Exception {
    // Update resolver
    ViewService mockService = Mockito.mock(ViewService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .updateView(any(), any(Urn.class), any(), any(), any(), Mockito.anyLong());

    UpdateViewResolver resolver = new UpdateViewResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Update resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ViewService mockService = initViewService(DataHubViewType.GLOBAL);
    UpdateViewResolver resolver = new UpdateViewResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext(TEST_UNAUTHORIZED_USER.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  private static ViewService initViewService(DataHubViewType viewType) {
    ViewService mockService = Mockito.mock(ViewService.class);

    DataHubViewInfo testInfo =
        new DataHubViewInfo()
            .setType(viewType)
            .setName(TEST_INPUT.getName())
            .setDescription(TEST_INPUT.getDescription())
            .setCreated(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L))
            .setLastModified(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L))
            .setDefinition(
                new DataHubViewDefinition()
                    .setEntityTypes(new StringArray())
                    .setFilter(new Filter()));

    EntityResponse testEntityResponse =
        new EntityResponse()
            .setUrn(TEST_URN)
            .setEntityName(Constants.DATAHUB_VIEW_ENTITY_NAME)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.DATAHUB_VIEW_INFO_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setName(Constants.DATAHUB_VIEW_INFO_ASPECT_NAME)
                            .setType(AspectType.VERSIONED)
                            .setValue(new Aspect(testInfo.data())))));

    Mockito.when(mockService.getViewInfo(any(), Mockito.eq(TEST_URN))).thenReturn(testInfo);

    Mockito.when(mockService.getViewEntityResponse(any(), Mockito.eq(TEST_URN)))
        .thenReturn(testEntityResponse);

    return mockService;
  }
}
