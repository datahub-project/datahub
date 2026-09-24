package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.CHART_USAGE_STATISTICS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROFILE_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.TIMESERIES;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TimeseriesAuthUtilTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,tsauth,PROD)");
  private static final Urn DASHBOARD_URN = UrnUtils.getUrn("urn:li:dashboard:(looker,tsauth)");

  private OperationContext opContext;
  private MockedStatic<AuthUtil> authUtilMock;
  private MockedStatic<EntityAuthorizationUtils> entityAuthMock;

  @BeforeMethod
  public void setUp() {
    opContext = mock(OperationContext.class);
    when(opContext.isSystemAuth()).thenReturn(false);
    authUtilMock = Mockito.mockStatic(AuthUtil.class);
    entityAuthMock = Mockito.mockStatic(EntityAuthorizationUtils.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMock.close();
    entityAuthMock.close();
  }

  @Test
  public void testSystemAuthAllows() {
    when(opContext.isSystemAuth()).thenReturn(true);
    assertTrue(TimeseriesAuthUtil.canReadEntity(opContext, DATASET_URN));
    assertTrue(TimeseriesAuthUtil.canReadApi(opContext, DATASET_URN));
    assertTrue(
        TimeseriesAuthUtil.canViewAspect(
            opContext, DATASET_URN, DATASET_ENTITY_NAME, DATASET_PROFILE_ASPECT_NAME));
  }

  @Test
  public void testMismatchEntityOrBlankAspectDenies() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN))
        .thenReturn(true);
    assertFalse(
        TimeseriesAuthUtil.canViewAspect(
            opContext, DATASET_URN, "chart", DATASET_PROFILE_ASPECT_NAME));
    assertFalse(TimeseriesAuthUtil.canViewAspect(opContext, DATASET_URN, DATASET_ENTITY_NAME, " "));
  }

  @Test
  public void testEditEntityGrantsCanReadEntity() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN))
        .thenReturn(true);
    assertTrue(TimeseriesAuthUtil.canReadEntity(opContext, DATASET_URN));
  }

  @Test
  public void testNoEntityReadDeniesViewAspectEvenWithProfilePrivilege() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN))
        .thenReturn(false);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(opContext),
                    eq(PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE),
                    any(EntitySpec.class)))
        .thenReturn(true);
    assertFalse(
        TimeseriesAuthUtil.canViewAspect(
            opContext, DATASET_URN, DATASET_ENTITY_NAME, DATASET_PROFILE_ASPECT_NAME));
  }

  @Test
  public void testEditWithoutProfilePrivilegeDeniesDatasetProfile() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN))
        .thenReturn(true);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(opContext),
                    eq(PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE),
                    any(EntitySpec.class)))
        .thenReturn(false);
    assertFalse(
        TimeseriesAuthUtil.canViewAspect(
            opContext, DATASET_URN, DATASET_ENTITY_NAME, DATASET_PROFILE_ASPECT_NAME));
  }

  @Test
  public void testUnmappedAspectRequiresEntityReadOnly() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN))
        .thenReturn(true);
    assertTrue(
        TimeseriesAuthUtil.canViewAspect(
            opContext, DATASET_URN, DATASET_ENTITY_NAME, "assertionRunEvent"));
    assertFalse(
        TimeseriesAuthUtil.isSensitiveMappedAspect(DATASET_ENTITY_NAME, "assertionRunEvent"));
    assertFalse(
        TimeseriesAuthUtil.isSensitiveMappedAspect("chart", CHART_USAGE_STATISTICS_ASPECT_NAME));
  }

  @Test
  public void testDashboardUsageIsMapped() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DASHBOARD_URN))
        .thenReturn(true);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(opContext),
                    eq(PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE),
                    any(EntitySpec.class)))
        .thenReturn(true);
    assertTrue(
        TimeseriesAuthUtil.isSensitiveMappedAspect(
            DASHBOARD_ENTITY_NAME, DASHBOARD_USAGE_STATISTICS_ASPECT_NAME));
    assertTrue(
        TimeseriesAuthUtil.canViewAspect(
            opContext,
            DASHBOARD_URN,
            DASHBOARD_ENTITY_NAME,
            DASHBOARD_USAGE_STATISTICS_ASPECT_NAME));
  }

  @Test
  public void testCanReadApiFollowsRestFlag() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN))
        .thenReturn(true);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedUrns(
                    eq(opContext), eq(TIMESERIES), eq(READ), eq(List.of(DATASET_URN))))
        .thenReturn(false);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(opContext),
                    eq(PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE),
                    any(EntitySpec.class)))
        .thenReturn(true);
    assertFalse(TimeseriesAuthUtil.canReadApi(opContext, DATASET_URN));
    assertFalse(
        TimeseriesAuthUtil.canReadAspect(
            opContext, DATASET_URN, DATASET_ENTITY_NAME, DATASET_PROFILE_ASPECT_NAME));
    assertTrue(
        TimeseriesAuthUtil.canViewAspect(
            opContext, DATASET_URN, DATASET_ENTITY_NAME, DATASET_PROFILE_ASPECT_NAME));
  }

  @Test
  public void testExtractUrnsFromFilter() {
    Criterion criterion = new Criterion();
    criterion.setField("urn");
    criterion.setCondition(Condition.EQUAL);
    criterion.setValues(new com.linkedin.data.template.StringArray(DATASET_URN.toString()));
    ConjunctiveCriterion andGroup = new ConjunctiveCriterion();
    andGroup.setAnd(new CriterionArray(criterion));
    Filter filter = new Filter();
    filter.setOr(new ConjunctiveCriterionArray(andGroup));
    assertEquals(TimeseriesAuthUtil.extractUrnsFromFilter(filter), List.of(DATASET_URN));
    assertTrue(TimeseriesAuthUtil.extractUrnsFromFilter(null).isEmpty());
  }

  @Test
  public void testCanReadAggregatedStatsSensitiveWithoutUrnDenied() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityType(
                    eq(opContext), eq(TIMESERIES), eq(READ), eq(DATASET_ENTITY_NAME)))
        .thenReturn(true);
    assertFalse(
        TimeseriesAuthUtil.canReadAggregatedStats(
            opContext, DATASET_ENTITY_NAME, DATASET_PROFILE_ASPECT_NAME, null));
  }

  @Test
  public void testCanReadAggregatedStatsUnmappedUsesTypeLevel() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityType(
                    eq(opContext), eq(TIMESERIES), eq(READ), eq(DATASET_ENTITY_NAME)))
        .thenReturn(true);
    assertTrue(
        TimeseriesAuthUtil.canReadAggregatedStats(
            opContext, DATASET_ENTITY_NAME, "assertionRunEvent", null));
  }

  @Test
  public void testCanReadAggregatedStatsUrnScoped() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN))
        .thenReturn(true);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedUrns(
                    eq(opContext), eq(TIMESERIES), eq(READ), eq(List.of(DATASET_URN))))
        .thenReturn(true);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(opContext),
                    eq(PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE),
                    any(EntitySpec.class)))
        .thenReturn(true);
    Criterion criterion = new Criterion();
    criterion.setField("urn");
    criterion.setCondition(Condition.EQUAL);
    criterion.setValues(new com.linkedin.data.template.StringArray(DATASET_URN.toString()));
    ConjunctiveCriterion andGroup = new ConjunctiveCriterion();
    andGroup.setAnd(new CriterionArray(criterion));
    Filter filter = new Filter();
    filter.setOr(new ConjunctiveCriterionArray(andGroup));
    assertTrue(
        TimeseriesAuthUtil.canReadAggregatedStats(
            opContext, DATASET_ENTITY_NAME, DATASET_PROFILE_ASPECT_NAME, filter));
  }

  @Test
  public void testCanReadAggregatedStatsMixedOrDenied() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN))
        .thenReturn(true);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedUrns(
                    eq(opContext), eq(TIMESERIES), eq(READ), eq(List.of(DATASET_URN))))
        .thenReturn(true);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(opContext),
                    eq(PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE),
                    any(EntitySpec.class)))
        .thenReturn(true);

    Criterion urnCriterion = new Criterion();
    urnCriterion.setField("urn");
    urnCriterion.setCondition(Condition.EQUAL);
    urnCriterion.setValues(new com.linkedin.data.template.StringArray(DATASET_URN.toString()));
    ConjunctiveCriterion urnGroup = new ConjunctiveCriterion();
    urnGroup.setAnd(new CriterionArray(urnCriterion));

    Criterion otherCriterion = new Criterion();
    otherCriterion.setField("eventGranularity");
    otherCriterion.setCondition(Condition.EQUAL);
    otherCriterion.setValues(new com.linkedin.data.template.StringArray("DAY"));
    ConjunctiveCriterion otherGroup = new ConjunctiveCriterion();
    otherGroup.setAnd(new CriterionArray(otherCriterion));

    Filter mixed = new Filter();
    mixed.setOr(new ConjunctiveCriterionArray(urnGroup, otherGroup));

    assertFalse(TimeseriesAuthUtil.isFilterFullyUrnScoped(mixed));
    assertEquals(TimeseriesAuthUtil.extractUrnsFromFilter(mixed), List.of(DATASET_URN));
    assertFalse(
        TimeseriesAuthUtil.canReadAggregatedStats(
            opContext, DATASET_ENTITY_NAME, DATASET_PROFILE_ASPECT_NAME, mixed));
  }

  @Test
  public void testOmitUnauthorizedTimeseriesAspects() {
    entityAuthMock
        .when(() -> EntityAuthorizationUtils.canViewEntity(opContext, DATASET_URN))
        .thenReturn(true);
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(opContext),
                    eq(PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE),
                    any(EntitySpec.class)))
        .thenReturn(false);
    Map<String, String> aspects = Map.of(DATASET_PROFILE_ASPECT_NAME, "profile", "status", "ok");
    Map<String, String> omitted =
        TimeseriesAuthUtil.omitUnauthorizedTimeseriesAspects(
            opContext, DATASET_URN, aspects, name -> DATASET_PROFILE_ASPECT_NAME.equals(name));
    assertEquals(omitted, Map.of("status", "ok"));
  }
}
