package com.linkedin.datahub.graphql.resolvers.view;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubViewDefinitionInput;
import com.linkedin.datahub.graphql.generated.DataHubViewFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.LogicalOperator;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ViewUtilsTest {

  private static final Urn TEST_AUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:auth");
  private static final Urn TEST_UNAUTHORIZED_USER = UrnUtils.getUrn("urn:li:corpuser:no-auth");

  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");

  @Test
  public static void testCanCreatePersonalViewAllowed() {
    boolean res =
        ViewUtils.canCreateView(DataHubViewType.PERSONAL, Mockito.mock(QueryContext.class));
    Assert.assertTrue(res);
  }

  @Test
  public static void testCanCreateGlobalViewAllowed() {
    QueryContext context = getMockAllowContext(TEST_AUTHORIZED_USER.toString());
    boolean res = ViewUtils.canCreateView(DataHubViewType.GLOBAL, context);
    Assert.assertTrue(res);
  }

  @Test
  public static void testCanCreateGlobalViewDenied() {
    QueryContext context = getMockDenyContext(TEST_AUTHORIZED_USER.toString());
    boolean res = ViewUtils.canCreateView(DataHubViewType.GLOBAL, context);
    Assert.assertFalse(res);
  }

  @Test
  public void testCanUpdateViewSuccessGlobalViewIsCreator() {
    ViewService mockService = initViewService(DataHubViewType.GLOBAL);
    QueryContext mockContext = getMockDenyContext(TEST_AUTHORIZED_USER.toString());

    assertTrue(ViewUtils.canUpdateView(mockService, TEST_VIEW_URN, mockContext));

    Mockito.verify(mockService, Mockito.times(1))
        .getViewInfo(Mockito.eq(TEST_VIEW_URN), Mockito.any(Authentication.class));
  }

  @Test
  public void testCanUpdateViewSuccessGlobalViewCanManageGlobalViews() {
    ViewService mockService = initViewService(DataHubViewType.GLOBAL);
    QueryContext mockContext = getMockDenyContext(TEST_AUTHORIZED_USER.toString());

    assertTrue(ViewUtils.canUpdateView(mockService, TEST_VIEW_URN, mockContext));

    Mockito.verify(mockService, Mockito.times(1))
        .getViewInfo(Mockito.eq(TEST_VIEW_URN), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetFailureGlobalViewIsNotCreatorOrManager() {
    ViewService mockService = initViewService(DataHubViewType.GLOBAL);
    QueryContext mockContext = getMockDenyContext(TEST_UNAUTHORIZED_USER.toString());

    assertFalse(ViewUtils.canUpdateView(mockService, TEST_VIEW_URN, mockContext));

    Mockito.verify(mockService, Mockito.times(1))
        .getViewInfo(Mockito.eq(TEST_VIEW_URN), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetSuccessPersonalViewIsCreator() {
    ViewService mockService = initViewService(DataHubViewType.PERSONAL);
    QueryContext mockContext = getMockDenyContext(TEST_AUTHORIZED_USER.toString());

    assertTrue(ViewUtils.canUpdateView(mockService, TEST_VIEW_URN, mockContext));

    Mockito.verify(mockService, Mockito.times(1))
        .getViewInfo(Mockito.eq(TEST_VIEW_URN), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetFailurePersonalViewIsNotCreator() {
    ViewService mockService = initViewService(DataHubViewType.PERSONAL);
    QueryContext mockContext = getMockDenyContext(TEST_UNAUTHORIZED_USER.toString());

    assertFalse(ViewUtils.canUpdateView(mockService, TEST_VIEW_URN, mockContext));

    Mockito.verify(mockService, Mockito.times(1))
        .getViewInfo(Mockito.eq(TEST_VIEW_URN), Mockito.any(Authentication.class));
  }

  @Test
  public void testMapDefinition() throws Exception {

    DataHubViewDefinitionInput input =
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
                        FilterOperator.IN),
                    new FacetFilterInput(
                        "test2",
                        null,
                        ImmutableList.of("value3", "value4"),
                        true,
                        FilterOperator.CONTAIN))));

    DataHubViewDefinition expectedResult =
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
                                                new Criterion()
                                                    .setNegated(false)
                                                    .setValues(
                                                        new StringArray(
                                                            ImmutableList.of("value1", "value2")))
                                                    .setValue("value1") // Disgraceful
                                                    .setField(
                                                        "test1.keyword") // Consider whether we
                                                    // should NOT go through
                                                    // the keyword mapping.
                                                    .setCondition(Condition.IN),
                                                new Criterion()
                                                    .setNegated(true)
                                                    .setValues(
                                                        new StringArray(
                                                            ImmutableList.of("value3", "value4")))
                                                    .setValue("value3") // Disgraceful
                                                    .setField(
                                                        "test2.keyword") // Consider whether we
                                                    // should NOT go through
                                                    // the keyword mapping.
                                                    .setCondition(Condition.CONTAIN))))))));

    assertEquals(ViewUtils.mapDefinition(input), expectedResult);
  }

  private static ViewService initViewService(DataHubViewType viewType) {
    ViewService mockService = Mockito.mock(ViewService.class);

    DataHubViewInfo testInfo =
        new DataHubViewInfo()
            .setType(viewType)
            .setName("test-name")
            .setDescription("test-description")
            .setCreated(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L))
            .setLastModified(new AuditStamp().setActor(TEST_AUTHORIZED_USER).setTime(0L))
            .setDefinition(
                new DataHubViewDefinition()
                    .setEntityTypes(new StringArray())
                    .setFilter(new Filter()));

    Mockito.when(
            mockService.getViewInfo(Mockito.eq(TEST_VIEW_URN), Mockito.any(Authentication.class)))
        .thenReturn(testInfo);

    return mockService;
  }
}
