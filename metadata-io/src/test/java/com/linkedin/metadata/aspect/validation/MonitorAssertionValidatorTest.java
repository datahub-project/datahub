package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MonitorAssertionValidatorTest {
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test-assertion");
  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn(
          "urn:li:monitor:(urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Atest%2Ctest%2CPROD),test-monitor)");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(MonitorAssertionValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "UPDATE"))
          .supportedEntityAspectNames(
              List.of(
                  new AspectPluginConfig.EntityAspectName(
                      ASSERTION_ENTITY_NAME, ASSERTION_INFO_ASPECT_NAME),
                  new AspectPluginConfig.EntityAspectName(
                      MONITOR_ENTITY_NAME, MONITOR_INFO_ASPECT_NAME)))
          .build();

  @Mock private RetrieverContext mockRetrieverContext;

  @Mock private AspectRetriever mockAspectRetriever;

  private EntityRegistry entityRegistry;

  private MonitorAssertionValidator validator;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = new TestEntityRegistry();
    validator = new MonitorAssertionValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  // ==================== AssertionInfo Tests ====================

  @Test
  public void testValidAssertionInfo() {
    AssertionInfo assertionInfo = createValidFreshnessAssertionInfo();

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_ASSERTION_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(TEST_ASSERTION_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_ASSERTION_URN.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for valid AssertionInfo");
  }

  @Test
  public void testAssertionInfoMissingEntityUrnButDerivable() {
    // When entityUrn is missing but the sub-property has an entity, validation should pass
    // because AssertionInfoMutator can derive it
    AssertionInfo assertionInfo = createValidFreshnessAssertionInfo();
    assertionInfo.removeEntityUrn();

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_ASSERTION_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(TEST_ASSERTION_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_ASSERTION_URN.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass when entityUrn is missing but derivable from sub-property");
  }

  @Test
  public void testAssertionInfoNoEntityResolvable() {
    // When neither entityUrn nor entity in sub-property is present, validation should fail
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    // Note: not setting entityUrn AND not setting freshnessAssertion - this should fail

    List<AspectValidationException> exceptions =
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_ASSERTION_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(TEST_ASSERTION_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_ASSERTION_URN.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .collect(Collectors.toList());

    // Should have 2 errors: one for no resolvable entity, one for type mismatch
    assertEquals(exceptions.size(), 2, "Expected validation to fail for no resolvable entity");
    assertTrue(
        exceptions.stream().anyMatch(e -> e.getMessage().contains("resolvable entity")),
        "Expected error message to mention resolvable entity");
  }

  @Test
  public void testAssertionInfoTypeMismatch() {
    // Create assertion with FRESHNESS type but no freshnessAssertion sub-property
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    assertionInfo.setEntityUrn(TEST_DATASET_URN);
    // Note: not setting freshnessAssertion - this should fail

    List<AspectValidationException> exceptions =
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_ASSERTION_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(TEST_ASSERTION_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_ASSERTION_URN.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .collect(Collectors.toList());

    assertEquals(exceptions.size(), 1, "Expected validation to fail for type mismatch");
    assertTrue(
        exceptions.get(0).getMessage().contains("freshnessAssertion"),
        "Expected error message to mention freshnessAssertion");
  }

  @Test
  public void testAssertionInfoVolumeTypeMismatch() {
    // Create assertion with VOLUME type but no volumeAssertion sub-property
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.VOLUME);
    assertionInfo.setEntityUrn(TEST_DATASET_URN);
    // Note: not setting volumeAssertion - this should fail

    List<AspectValidationException> exceptions =
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_ASSERTION_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(TEST_ASSERTION_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_ASSERTION_URN.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .collect(Collectors.toList());

    assertEquals(exceptions.size(), 1, "Expected validation to fail for VOLUME type mismatch");
    assertTrue(
        exceptions.get(0).getMessage().contains("volumeAssertion"),
        "Expected error message to mention volumeAssertion");
  }

  @Test
  public void testAssertionInfoValidWithVolumeType() {
    AssertionInfo assertionInfo = createValidVolumeAssertionInfo();

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_ASSERTION_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(TEST_ASSERTION_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_ASSERTION_URN.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(assertionInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for valid VOLUME AssertionInfo");
  }

  // ==================== MonitorInfo Tests ====================

  @Test
  public void testValidMonitorInfo() {
    MonitorInfo monitorInfo = createValidMonitorInfo();

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_MONITOR_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_MONITOR_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_MONITOR_URN.getEntityType())
                                .getAspectSpec(MONITOR_INFO_ASPECT_NAME))
                        .recordTemplate(monitorInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for valid MonitorInfo");
  }

  @Test
  public void testMonitorInfoMissingAssertionMonitor() {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    monitorInfo.setStatus(createActiveStatus());
    // Note: not setting assertionMonitor - this should fail

    List<AspectValidationException> exceptions =
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_MONITOR_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_MONITOR_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_MONITOR_URN.getEntityType())
                                .getAspectSpec(MONITOR_INFO_ASPECT_NAME))
                        .recordTemplate(monitorInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .collect(Collectors.toList());

    assertEquals(exceptions.size(), 1, "Expected validation to fail for missing assertionMonitor");
    assertTrue(
        exceptions.get(0).getMessage().contains("assertionMonitor"),
        "Expected error message to mention assertionMonitor");
  }

  @Test
  public void testMonitorInfoEmptyAssertions() {
    // Empty assertions array is now allowed for intermediate states
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    monitorInfo.setStatus(createActiveStatus());

    AssertionMonitor assertionMonitor = new AssertionMonitor();
    assertionMonitor.setAssertions(new AssertionEvaluationSpecArray());
    monitorInfo.setAssertionMonitor(assertionMonitor);

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_MONITOR_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_MONITOR_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_MONITOR_URN.getEntityType())
                                .getAspectSpec(MONITOR_INFO_ASPECT_NAME))
                        .recordTemplate(monitorInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for empty assertions (allowed for intermediate states)");
  }

  @Test
  public void testMonitorInfoMultipleAssertions() {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    monitorInfo.setStatus(createActiveStatus());

    AssertionMonitor assertionMonitor = new AssertionMonitor();
    assertionMonitor.setAssertions(
        new AssertionEvaluationSpecArray(
            createAssertionEvaluationSpec(TEST_ASSERTION_URN),
            createAssertionEvaluationSpec(UrnUtils.getUrn("urn:li:assertion:test-assertion-2"))));
    monitorInfo.setAssertionMonitor(assertionMonitor);

    List<AspectValidationException> exceptions =
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_MONITOR_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_MONITOR_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_MONITOR_URN.getEntityType())
                                .getAspectSpec(MONITOR_INFO_ASPECT_NAME))
                        .recordTemplate(monitorInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .collect(Collectors.toList());

    assertEquals(exceptions.size(), 1, "Expected validation to fail for multiple assertions");
    assertTrue(
        exceptions.get(0).getMessage().contains("at most one assertion"),
        "Expected error message to mention 1:1 relationship");
  }

  @Test
  public void testMonitorInfoNonAssertionTypeNoValidation() {
    // FRESHNESS type monitors should not require assertionMonitor
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.FRESHNESS);
    monitorInfo.setStatus(createActiveStatus());
    // Note: not setting assertionMonitor - this should NOT fail for FRESHNESS type

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_MONITOR_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_MONITOR_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_MONITOR_URN.getEntityType())
                                .getAspectSpec(MONITOR_INFO_ASPECT_NAME))
                        .recordTemplate(monitorInfo)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass for FRESHNESS type monitor without assertionMonitor");
  }

  // ==================== Null Aspect Tests ====================

  @Test
  public void testNullAssertionInfo() {
    // When getAspect returns null, validation should pass (no-op)
    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_ASSERTION_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(TEST_ASSERTION_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_ASSERTION_URN.getEntityType())
                                .getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
                        .recordTemplate(null)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass when AssertionInfo is null");
  }

  @Test
  public void testNullMonitorInfo() {
    // When getAspect returns null, validation should pass (no-op)
    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_MONITOR_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_MONITOR_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_MONITOR_URN.getEntityType())
                                .getAspectSpec(MONITOR_INFO_ASPECT_NAME))
                        .recordTemplate(null)
                        .build()),
                mockRetrieverContext,
                null)
            .count(),
        0,
        "Expected validation to pass when MonitorInfo is null");
  }

  // ==================== Helper Methods ====================

  private AssertionInfo createValidFreshnessAssertionInfo() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    assertionInfo.setEntityUrn(TEST_DATASET_URN);

    FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
    freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    freshnessInfo.setEntity(TEST_DATASET_URN);

    FreshnessAssertionSchedule schedule = new FreshnessAssertionSchedule();
    schedule.setType(FreshnessAssertionScheduleType.CRON);
    FreshnessCronSchedule cronSchedule = new FreshnessCronSchedule();
    cronSchedule.setCron("0 0 * * *");
    cronSchedule.setTimezone("UTC");
    schedule.setCron(cronSchedule);
    freshnessInfo.setSchedule(schedule);

    assertionInfo.setFreshnessAssertion(freshnessInfo);
    return assertionInfo;
  }

  private AssertionInfo createValidVolumeAssertionInfo() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.VOLUME);
    assertionInfo.setEntityUrn(TEST_DATASET_URN);

    VolumeAssertionInfo volumeInfo = new VolumeAssertionInfo();
    volumeInfo.setType(VolumeAssertionType.ROW_COUNT_TOTAL);
    volumeInfo.setEntity(TEST_DATASET_URN);

    assertionInfo.setVolumeAssertion(volumeInfo);
    return assertionInfo;
  }

  private MonitorInfo createValidMonitorInfo() {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    monitorInfo.setStatus(createActiveStatus());

    AssertionMonitor assertionMonitor = new AssertionMonitor();
    assertionMonitor.setAssertions(
        new AssertionEvaluationSpecArray(createAssertionEvaluationSpec(TEST_ASSERTION_URN)));
    monitorInfo.setAssertionMonitor(assertionMonitor);

    return monitorInfo;
  }

  private MonitorStatus createActiveStatus() {
    MonitorStatus status = new MonitorStatus();
    status.setMode(MonitorMode.ACTIVE);
    return status;
  }

  private AssertionEvaluationSpec createAssertionEvaluationSpec(Urn assertionUrn) {
    AssertionEvaluationSpec spec = new AssertionEvaluationSpec();
    spec.setAssertion(assertionUrn);

    CronSchedule schedule = new CronSchedule();
    schedule.setCron("0 0 * * *");
    schedule.setTimezone("UTC");
    spec.setSchedule(schedule);

    return spec;
  }
}
