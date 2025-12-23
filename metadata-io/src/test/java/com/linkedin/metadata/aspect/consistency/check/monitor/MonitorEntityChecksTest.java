package com.linkedin.metadata.aspect.consistency.check.monitor;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.AggregateCheckTestHelper;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Aggregate tests for monitor entity consistency checks.
 *
 * <p>These tests run ALL monitor checks together to validate aggregate behavior. When one check
 * identifies an issue, other checks should correctly skip or return empty for that entity.
 *
 * <p>Tests are organized by entity state/scenario rather than by individual check.
 */
public class MonitorEntityChecksTest {

  @Mock private EntityService<?> mockEntityService;

  private EntityRegistry testEntityRegistry;
  private OperationContext testOpContext;
  private ConsistencyCheckRegistry registry;
  private AggregateCheckTestHelper helper;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Use real entity registry for full validation support
    testEntityRegistry = TestOperationContexts.defaultEntityRegistry();
    testOpContext = TestOperationContexts.systemContextNoSearchAuthorization(testEntityRegistry);

    // Create all monitor checks
    List<ConsistencyCheck> checks =
        List.of(
            new MonitorEntityTypeInvalidCheck(testEntityRegistry),
            new MonitorEntityNotFoundCheck(testEntityRegistry),
            new MonitorEntitySoftDeletedCheck(testEntityRegistry),
            new MonitorAssertionsEmptyCheck(testEntityRegistry),
            new MonitorAssertionsMultipleCheck(testEntityRegistry),
            new MonitorAssertionRefTypeInvalidCheck(testEntityRegistry),
            new MonitorAssertionNotFoundCheck(testEntityRegistry),
            new MonitorAssertionSoftDeletedCheck(testEntityRegistry),
            new MonitorEntityUrnMismatchCheck(testEntityRegistry));

    registry = new ConsistencyCheckRegistry(checks);
    helper = new AggregateCheckTestHelper(registry);

    // Default mock behavior: entities and assertions exist and are not soft-deleted
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(false)))
        .thenReturn(true);
  }

  // ============================================================================
  // Aggregate Scenario Tests
  // ============================================================================

  /**
   * Happy path test: Valid monitor should have no issues from any check.
   *
   * <p>This validates the happy path for all monitor checks:
   *
   * <ul>
   *   <li>MonitorEntityTypeInvalidCheck - entity type is valid (dataset)
   *   <li>MonitorEntityNotFoundCheck - entity exists
   *   <li>MonitorEntitySoftDeletedCheck - entity is not soft-deleted
   *   <li>MonitorAssertionsEmptyCheck - has exactly one assertion
   *   <li>MonitorAssertionsMultipleCheck - has exactly one assertion
   *   <li>MonitorAssertionRefTypeInvalidCheck - assertion ref is entity type "assertion"
   *   <li>MonitorAssertionNotFoundCheck - assertion exists
   *   <li>MonitorAssertionSoftDeletedCheck - assertion is not soft-deleted
   *   <li>MonitorEntityUrnMismatchCheck - assertion entityUrn matches monitor entityUrn
   * </ul>
   */
  @Test
  public void testValidMonitor_NoIssues() {
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,valid,PROD)");
    Urn monitorUrn = UrnUtils.getUrn("urn:li:monitor:(" + datasetUrn.toString() + ",valid123)");
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:valid123");

    when(mockEntityService.exists(eq(testOpContext), eq(datasetUrn), eq(true))).thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(datasetUrn), eq(false))).thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(assertionUrn), eq(true))).thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(assertionUrn), eq(false))).thenReturn(true);

    EntityResponse response = createMonitorResponseWithAssertion(monitorUrn, assertionUrn);

    // Cache matching assertion info (to avoid mismatch check firing)
    CheckContext context = buildContext();
    AssertionInfo assertionInfo = createAssertionInfo(datasetUrn);
    context.cacheAspect(assertionUrn, ASSERTION_INFO_ASPECT_NAME, assertionInfo);

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, Map.of(monitorUrn, response));

    helper.assertTotalIssueCount(issues, 0);
  }

  // Note: testMonitorInvalidEntityType_SoftDeleteIssue is omitted because it depends on
  // entity registry configuration. The valid entity types are derived from PDL annotations
  // at runtime. Integration tests with a properly configured entity registry should cover
  // this scenario. The MonitorEntityTypeInvalidCheck has individual unit tests that mock
  // the valid entity types.

  @Test
  public void testMonitorEntityNotFound_HardDeleteIssue() {
    // Scenario: Monitor references an entity that does not exist
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,nonexistent,PROD)");
    Urn monitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + datasetUrn.toString() + ",entityNotFound123)");
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:entityNotFound123");

    // Entity does not exist
    when(mockEntityService.exists(eq(testOpContext), eq(datasetUrn), eq(true))).thenReturn(false);

    EntityResponse response = createMonitorResponseWithAssertion(monitorUrn, assertionUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, Map.of(monitorUrn, response));

    // monitor-entity-not-found should fire with HARD_DELETE (entity is truly gone)
    helper.assertIssueFromCheck(
        issues, "monitor-entity-not-found", monitorUrn, ConsistencyFixType.HARD_DELETE);

    // Other checks should skip since entity doesn't exist
    helper.assertNoIssueFromCheck(issues, "monitor-entity-type-invalid");
  }

  @Test
  public void testMonitorEntitySoftDeleted_SoftDeleteIssue() {
    // Scenario: Monitor references a soft-deleted entity
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,deleted,PROD)");
    Urn monitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + datasetUrn.toString() + ",entityDeleted123)");
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:entityDeleted123");

    // Entity exists but is soft-deleted
    when(mockEntityService.exists(eq(testOpContext), eq(datasetUrn), eq(true))).thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(datasetUrn), eq(false))).thenReturn(false);

    EntityResponse response = createMonitorResponseWithAssertion(monitorUrn, assertionUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, Map.of(monitorUrn, response));

    // monitor-entity-soft-deleted should fire
    helper.assertIssueFromCheck(
        issues, "monitor-entity-soft-deleted", monitorUrn, ConsistencyFixType.SOFT_DELETE);
  }

  @Test
  public void testMonitorAssertionsEmpty_SoftDeleteIssue() {
    // Scenario: Monitor has empty assertions array
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Urn monitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + datasetUrn.toString() + ",emptyAssertions123)");

    EntityResponse response = createMonitorResponseWithEmptyAssertions(monitorUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, Map.of(monitorUrn, response));

    // monitor-assertions-empty should fire
    helper.assertIssueFromCheck(
        issues, "monitor-assertions-empty", monitorUrn, ConsistencyFixType.SOFT_DELETE);
  }

  @Test
  public void testMonitorAssertionsMultiple_UpsertIssue() {
    // Scenario: Monitor has multiple assertions (should only have one)
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Urn monitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + datasetUrn.toString() + ",multipleAssertions123)");
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:primary");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:extra1");
    Urn assertionUrn3 = UrnUtils.getUrn("urn:li:assertion:extra2");

    EntityResponse response =
        createMonitorResponseWithMultipleAssertions(
            monitorUrn, List.of(assertionUrn1, assertionUrn2, assertionUrn3));
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, Map.of(monitorUrn, response));

    // monitor-assertions-multiple should fire with UPSERT (to trim to one)
    helper.assertIssueFromCheck(
        issues, "monitor-assertions-multiple", monitorUrn, ConsistencyFixType.UPSERT);

    ConsistencyIssue issue = helper.getIssue(issues, "monitor-assertions-multiple", monitorUrn);
    assertNotNull(issue);
    assertTrue(issue.getDescription().contains("Monitor has 3 assertions"));
  }

  @Test
  public void testMonitorAssertionNotFound_HardDeleteIssue() {
    // Scenario: Monitor references an assertion that does not exist
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Urn monitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + datasetUrn.toString() + ",assertionNotFound123)");
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:nonexistent");

    // Assertion does not exist
    when(mockEntityService.exists(eq(testOpContext), eq(assertionUrn), eq(true))).thenReturn(false);

    EntityResponse response = createMonitorResponseWithAssertion(monitorUrn, assertionUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, Map.of(monitorUrn, response));

    // monitor-assertion-not-found should fire with HARD_DELETE (assertion is truly gone)
    helper.assertIssueFromCheck(
        issues, "monitor-assertion-not-found", monitorUrn, ConsistencyFixType.HARD_DELETE);
  }

  @Test
  public void testMonitorAssertionSoftDeleted_SoftDeleteIssue() {
    // Scenario: Monitor references a soft-deleted assertion
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Urn monitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + datasetUrn.toString() + ",assertionDeleted123)");
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:softDeleted");

    // Assertion exists but is soft-deleted
    when(mockEntityService.exists(eq(testOpContext), eq(assertionUrn), eq(true))).thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(assertionUrn), eq(false)))
        .thenReturn(false);

    EntityResponse response = createMonitorResponseWithAssertion(monitorUrn, assertionUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, Map.of(monitorUrn, response));

    // monitor-assertion-soft-deleted should fire
    helper.assertIssueFromCheck(
        issues, "monitor-assertion-soft-deleted", monitorUrn, ConsistencyFixType.SOFT_DELETE);
  }

  @Test
  public void testMonitorEntityUrnMismatch_SoftDeleteIssue() {
    // Scenario: Monitor is for dataset1 but assertion references dataset2
    Urn monitorDatasetUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,dataset1,PROD)");
    Urn assertionDatasetUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,dataset2,PROD)");
    Urn monitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + monitorDatasetUrn.toString() + ",mismatch123)");
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:mismatch123");

    // Assertion exists
    when(mockEntityService.exists(eq(testOpContext), eq(assertionUrn), eq(true))).thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(assertionUrn), eq(false))).thenReturn(true);

    EntityResponse response = createMonitorResponseWithAssertion(monitorUrn, assertionUrn);

    // Cache assertion info with DIFFERENT entity URN
    CheckContext context = buildContext();
    AssertionInfo assertionInfo = createAssertionInfo(assertionDatasetUrn);
    context.cacheAspect(assertionUrn, ASSERTION_INFO_ASPECT_NAME, assertionInfo);

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, Map.of(monitorUrn, response));

    // monitor-entity-urn-mismatch should fire
    helper.assertIssueFromCheck(
        issues, "monitor-entity-urn-mismatch", monitorUrn, ConsistencyFixType.SOFT_DELETE);

    ConsistencyIssue issue = helper.getIssue(issues, "monitor-entity-urn-mismatch", monitorUrn);
    assertNotNull(issue);
    assertTrue(issue.getDescription().contains("doesn't match"));
  }

  @Test
  public void testMonitorAssertionRefTypeInvalid_SoftDeleteIssue() {
    // Scenario: Monitor references a non-assertion entity type in its assertion array
    Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Urn monitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + datasetUrn.toString() + ",invalidRef123)");
    // Intentionally wrong - this should be an assertion URN, not a dataset URN
    Urn invalidAssertionRef =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,wrongType,PROD)");

    EntityResponse response = createMonitorResponseWithAssertion(monitorUrn, invalidAssertionRef);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, Map.of(monitorUrn, response));

    // monitor-assertion-ref-type-invalid should fire
    helper.assertIssueFromCheck(
        issues, "monitor-assertion-ref-type-invalid", monitorUrn, ConsistencyFixType.SOFT_DELETE);

    ConsistencyIssue issue =
        helper.getIssue(issues, "monitor-assertion-ref-type-invalid", monitorUrn);
    assertNotNull(issue);
    assertTrue(issue.getDescription().contains("non-assertion entity type"));
    assertTrue(issue.getDescription().contains("dataset"));
  }

  @Test
  public void testMultipleMonitors_MixedResults() {
    // Scenario: Multiple monitors with different issues
    Urn validDatasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,valid,PROD)");
    Urn missingDatasetUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,missing,PROD)");

    Urn validMonitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + validDatasetUrn.toString() + ",valid)");
    Urn missingEntityMonitorUrn =
        UrnUtils.getUrn("urn:li:monitor:(" + missingDatasetUrn.toString() + ",missing)");

    Urn validAssertionUrn = UrnUtils.getUrn("urn:li:assertion:valid");
    Urn missingAssertionUrn = UrnUtils.getUrn("urn:li:assertion:missing");

    // Valid dataset exists, missing dataset doesn't
    when(mockEntityService.exists(eq(testOpContext), eq(validDatasetUrn), eq(true)))
        .thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(validDatasetUrn), eq(false)))
        .thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(missingDatasetUrn), eq(true)))
        .thenReturn(false);
    when(mockEntityService.exists(eq(testOpContext), eq(validAssertionUrn), eq(true)))
        .thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(validAssertionUrn), eq(false)))
        .thenReturn(true);

    Map<Urn, EntityResponse> responses = new HashMap<>();
    responses.put(
        validMonitorUrn, createMonitorResponseWithAssertion(validMonitorUrn, validAssertionUrn));
    responses.put(
        missingEntityMonitorUrn,
        createMonitorResponseWithAssertion(missingEntityMonitorUrn, missingAssertionUrn));

    // Cache assertion info for valid monitor
    CheckContext context = buildContext();
    AssertionInfo assertionInfo = createAssertionInfo(validDatasetUrn);
    context.cacheAspect(validAssertionUrn, ASSERTION_INFO_ASPECT_NAME, assertionInfo);

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(MONITOR_ENTITY_NAME, context, responses);

    // Valid monitor: no issues
    // Missing entity monitor: HARD_DELETE from monitor-entity-not-found
    helper.assertIssueFromCheck(
        issues,
        "monitor-entity-not-found",
        missingEntityMonitorUrn,
        ConsistencyFixType.HARD_DELETE);

    // Count issues for valid monitor - should be 0
    List<ConsistencyIssue> validMonitorIssues =
        issues.stream()
            .filter(i -> validMonitorUrn.equals(i.getEntityUrn()))
            .collect(Collectors.toList());
    assertTrue(
        validMonitorIssues.isEmpty(), "Valid monitor should have no issues: " + validMonitorIssues);
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private CheckContext buildContext() {
    return CheckContext.builder()
        .operationContext(testOpContext)
        .entityService(mockEntityService)
        .build();
  }

  private AssertionInfo createAssertionInfo(Urn entityUrn) {
    AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.FRESHNESS);
    info.setEntityUrn(entityUrn);
    FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
    freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    freshnessInfo.setEntity(entityUrn);
    info.setFreshnessAssertion(freshnessInfo);
    return info;
  }

  private EntityResponse createBasicMonitorResponse(Urn monitorUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(monitorUrn);
    response.setEntityName(MONITOR_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    MonitorStatus monitorStatus = new MonitorStatus();
    monitorStatus.setMode(MonitorMode.ACTIVE);
    monitorInfo.setStatus(monitorStatus);
    infoAspect.setValue(new Aspect(monitorInfo.data()));
    aspects.put(MONITOR_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createMonitorResponseWithEmptyAssertions(Urn monitorUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(monitorUrn);
    response.setEntityName(MONITOR_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    MonitorStatus monitorStatus = new MonitorStatus();
    monitorStatus.setMode(MonitorMode.ACTIVE);
    monitorInfo.setStatus(monitorStatus);
    AssertionMonitor assertionMonitor = new AssertionMonitor();
    assertionMonitor.setAssertions(new AssertionEvaluationSpecArray()); // Empty
    monitorInfo.setAssertionMonitor(assertionMonitor);
    infoAspect.setValue(new Aspect(monitorInfo.data()));
    aspects.put(MONITOR_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createMonitorResponseWithAssertion(Urn monitorUrn, Urn assertionUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(monitorUrn);
    response.setEntityName(MONITOR_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    MonitorStatus monitorStatus = new MonitorStatus();
    monitorStatus.setMode(MonitorMode.ACTIVE);
    monitorInfo.setStatus(monitorStatus);
    AssertionMonitor assertionMonitor = new AssertionMonitor();
    AssertionEvaluationSpec spec = new AssertionEvaluationSpec();
    spec.setAssertion(assertionUrn);
    CronSchedule schedule = new CronSchedule();
    schedule.setCron("0 0 0 * * ?");
    schedule.setTimezone("UTC");
    spec.setSchedule(schedule);
    assertionMonitor.setAssertions(new AssertionEvaluationSpecArray(List.of(spec)));
    monitorInfo.setAssertionMonitor(assertionMonitor);
    infoAspect.setValue(new Aspect(monitorInfo.data()));
    aspects.put(MONITOR_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createMonitorResponseWithMultipleAssertions(
      Urn monitorUrn, List<Urn> assertionUrns) {
    EntityResponse response = new EntityResponse();
    response.setUrn(monitorUrn);
    response.setEntityName(MONITOR_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    MonitorStatus monitorStatus = new MonitorStatus();
    monitorStatus.setMode(MonitorMode.ACTIVE);
    monitorInfo.setStatus(monitorStatus);
    AssertionMonitor assertionMonitor = new AssertionMonitor();

    CronSchedule schedule = new CronSchedule();
    schedule.setCron("0 0 0 * * ?");
    schedule.setTimezone("UTC");

    List<AssertionEvaluationSpec> specs =
        assertionUrns.stream()
            .map(
                urn -> {
                  AssertionEvaluationSpec spec = new AssertionEvaluationSpec();
                  spec.setAssertion(urn);
                  spec.setSchedule(schedule);
                  return spec;
                })
            .collect(Collectors.toList());

    assertionMonitor.setAssertions(new AssertionEvaluationSpecArray(specs));
    monitorInfo.setAssertionMonitor(assertionMonitor);
    infoAspect.setValue(new Aspect(monitorInfo.data()));
    // Add SystemMetadata for conditional write headers
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setVersion("1");
    infoAspect.setSystemMetadata(systemMetadata);
    aspects.put(MONITOR_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }
}
