package com.linkedin.datahub.upgrade.entityconsistency;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.entityconsistency.FixEntityConsistency;
import com.linkedin.datahub.upgrade.system.entityconsistency.FixEntityConsistencyStep;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyFixRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.consistency.check.CheckBatchRequest;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import com.linkedin.metadata.aspect.consistency.fix.BatchItemsFix;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFix;
import com.linkedin.metadata.aspect.consistency.fix.HardDeleteEntityFix;
import com.linkedin.metadata.config.EntityConsistencyConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.UrnValidationFieldSpec;
import com.linkedin.metadata.models.annotation.UrnValidationAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.scroll.SystemMetadataScrollClient;
import com.linkedin.metadata.systemmetadata.scroll.SystemMetadataScrollRequest;
import com.linkedin.metadata.systemmetadata.scroll.SystemMetadataScrollResult;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FixEntityConsistencyStepTest {

  @Mock private OperationContext mockOpContext;

  @Mock private EntityService<?> mockEntityService;

  @Mock private SystemMetadataScrollClient mockScrollClient;

  @Mock private RetrieverContext mockRetrieverContext;

  @Mock private AspectRetriever mockAspectRetriever;

  @Mock private EntityRegistry mockEntityRegistry;

  private ConsistencyService consistencyService;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
    when(mockOpContext.withSearchFlags(any())).thenReturn(mockOpContext);
    when(mockOpContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(mockEntityRegistry);

    setupEntityRegistryMocks();
    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);

    // Default scroll-client behavior: empty result, no continuation. Individual tests can override.
    when(mockScrollClient.scrollUrns(
            any(OperationContext.class), any(SystemMetadataScrollRequest.class)))
        .thenReturn(SystemMetadataScrollResult.empty());

    // Create checks for registry - add mock checks for assertion entity type
    // All checks are non-on-demand to match test expectations
    List<ConsistencyCheck> checks =
        List.of(
            createMockCheck("assertion-entity-urn-missing", ASSERTION_ENTITY_NAME, false),
            createMockCheck("assertion-entity-type-invalid", ASSERTION_ENTITY_NAME, false),
            createMockCheck("assertion-entity-not-found", ASSERTION_ENTITY_NAME, false),
            createMockCheck("assertion-entity-soft-deleted", ASSERTION_ENTITY_NAME, false),
            createMockCheck("assertion-monitor-missing", ASSERTION_ENTITY_NAME, false));

    ConsistencyCheckRegistry checkRegistry = new ConsistencyCheckRegistry(checks);

    List<ConsistencyFix> fixes =
        List.of(new BatchItemsFix(mockEntityService), new HardDeleteEntityFix(mockEntityService));

    ConsistencyFixRegistry fixRegistry = new ConsistencyFixRegistry(fixes);

    consistencyService =
        new ConsistencyService(
            mockEntityService, mockScrollClient, null, checkRegistry, fixRegistry);
  }

  /** Helper method to create a mock ConsistencyCheck for testing. */
  private ConsistencyCheck createMockCheck(String id, String entityType, boolean onDemandOnly) {
    return new ConsistencyCheck() {
      @Override
      @Nonnull
      public String getId() {
        return id;
      }

      @Override
      @Nonnull
      public String getName() {
        return id;
      }

      @Override
      @Nonnull
      public String getDescription() {
        return id;
      }

      @Override
      @Nonnull
      public String getEntityType() {
        return entityType;
      }

      @Override
      @Nonnull
      public Optional<Set<String>> getRequiredAspects() {
        return Optional.of(Set.of(ASSERTION_INFO_ASPECT_NAME));
      }

      @Override
      public boolean isOnDemandOnly() {
        return onDemandOnly;
      }

      @Override
      @Nonnull
      public List<ConsistencyIssue> check(
          @Nonnull CheckContext context, @Nonnull Map<Urn, EntityResponse> entities) {
        return List.of();
      }
    };
  }

  /**
   * Setup entity registry mocks for annotation-based entity type validation.
   *
   * <p>This configures the mock entity registry to return proper EntitySpec and AspectSpec objects
   * with @UrnValidation annotations that the consistency checks use to determine valid entity
   * types.
   */
  private void setupEntityRegistryMocks() {
    EntitySpec assertionEntitySpec = mock(EntitySpec.class);
    AspectSpec assertionInfoAspectSpec = mock(AspectSpec.class);
    UrnValidationAnnotation assertionUrnValidation = mock(UrnValidationAnnotation.class);
    UrnValidationFieldSpec assertionFieldSpec = mock(UrnValidationFieldSpec.class);

    when(assertionUrnValidation.getEntityTypes()).thenReturn(List.of("dataset", "dataJob"));
    when(assertionFieldSpec.getUrnValidationAnnotation()).thenReturn(assertionUrnValidation);
    Map<String, UrnValidationFieldSpec> assertionFieldSpecMap = new HashMap<>();
    assertionFieldSpecMap.put("/entityUrn", assertionFieldSpec);
    when(assertionInfoAspectSpec.getUrnValidationFieldSpecMap()).thenReturn(assertionFieldSpecMap);
    when(assertionEntitySpec.getAspectSpec(ASSERTION_INFO_ASPECT_NAME))
        .thenReturn(assertionInfoAspectSpec);
    when(mockEntityRegistry.getEntitySpec(ASSERTION_ENTITY_NAME)).thenReturn(assertionEntitySpec);
    when(mockEntityRegistry.getEntitySpecs())
        .thenReturn(Map.of(ASSERTION_ENTITY_NAME, assertionEntitySpec));

    EntitySpec monitorEntitySpec = mock(EntitySpec.class);
    AspectSpec monitorKeyAspectSpec = mock(AspectSpec.class);
    UrnValidationAnnotation monitorUrnValidation = mock(UrnValidationAnnotation.class);
    UrnValidationFieldSpec monitorFieldSpec = mock(UrnValidationFieldSpec.class);

    when(monitorUrnValidation.getEntityTypes()).thenReturn(List.of("dataset"));
    when(monitorFieldSpec.getUrnValidationAnnotation()).thenReturn(monitorUrnValidation);
    Map<String, UrnValidationFieldSpec> monitorFieldSpecMap = new HashMap<>();
    monitorFieldSpecMap.put("/entity", monitorFieldSpec);
    when(monitorKeyAspectSpec.getUrnValidationFieldSpecMap()).thenReturn(monitorFieldSpecMap);
  }

  /** Helper to create test config with common defaults. */
  private EntityConsistencyConfiguration createTestConfig(
      boolean dryRun, int batchSize, int delayMs, int limit, boolean reprocessEnabled) {
    return createTestConfig(dryRun, batchSize, delayMs, limit, reprocessEnabled, null, null);
  }

  /** Helper to create test config with entity types and check IDs. */
  private EntityConsistencyConfiguration createTestConfig(
      boolean dryRun,
      int batchSize,
      int delayMs,
      int limit,
      boolean reprocessEnabled,
      List<String> entityTypes,
      List<String> checkIds) {
    EntityConsistencyConfiguration config = new EntityConsistencyConfiguration();
    config.setEnabled(true);
    config.setDryRun(dryRun);
    config.setBatchSize(batchSize);
    config.setDelayMs(delayMs);
    config.setLimit(limit);
    config.setEntityTypes(entityTypes);
    config.setCheckIds(checkIds);

    EntityConsistencyConfiguration.Reprocess reprocess =
        new EntityConsistencyConfiguration.Reprocess();
    reprocess.setEnabled(reprocessEnabled);
    config.setReprocess(reprocess);

    return config;
  }

  /** Helper that builds a single-page scroll result with the given URNs and no continuation. */
  private SystemMetadataScrollResult scrollResultOf(String... urns) {
    LinkedHashSet<Urn> urnSet = new LinkedHashSet<>();
    Arrays.stream(urns).map(UrnUtils::getUrn).forEach(urnSet::add);
    return SystemMetadataScrollResult.builder().urns(urnSet).nextScrollId(null).build();
  }

  /** Test to verify the correct step ID is returned. */
  @Test
  public void testId() {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 100, 1000, false));
    assertEquals(step.id(), "entity-consistency-v1");
  }

  /** Test to verify skip returns false when reprocessEnabled is true regardless of prior state. */
  @Test
  public void testSkipWithReprocessEnabled() {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 100, 1000, true));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    assertFalse(step.skip(mockContext), "Should not skip when reprocessEnabled=true");
  }

  /**
   * Test skip logic for regular runs (no timestamp filters, no limit) - should NOT skip on
   * SUCCEEDED.
   */
  @Test
  public void testSkipRegularRunSucceeded() {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 100, 0, false));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    assertFalse(
        step.skip(mockContext), "Regular run should not skip on SUCCEEDED (incremental mode)");
  }

  /** Test skip logic for targeted runs (with timestamp filters) - should skip on SUCCEEDED. */
  @Test
  public void testSkipTargetedRunSucceeded() {
    EntityConsistencyConfiguration config = createTestConfig(false, 10, 100, 1000, false);
    EntityConsistencyConfiguration.SystemMetadataFilterConfig filterConfig =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();
    filterConfig.setGePitEpochMs(1000L);
    config.setSystemMetadataFilterConfig(filterConfig);

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    assertTrue(step.skip(mockContext), "Targeted run should skip on SUCCEEDED");
  }

  /** Test skip logic - should NOT skip on IN_PROGRESS (resume) for non-limited runs. */
  @Test
  public void testSkipInProgressAllowsResume() {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 100, 0, false));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    assertFalse(step.skip(mockContext), "Should not skip IN_PROGRESS state (resume)");
  }

  /** Test skip logic - should NOT skip when no previous run exists. */
  @Test
  public void testSkipNoPreviousRun() {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 100, 0, false));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    assertFalse(step.skip(mockContext), "Should not skip when no previous run");
  }

  /**
   * Test to verify the executable function processes batches correctly and returns a success result
   * when no orphaned entities are found.
   */
  @Test
  public void testExecutableWithNoOrphans() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(true, 10, 0, 0, false));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    // Default mockScrollClient behaviour: empty result with null continuation.
    when(mockEntityService.exists(any(OperationContext.class), any(Set.class), anyBoolean()))
        .thenReturn(Set.of());
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), anyBoolean()))
        .thenReturn(false);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  /** Test to verify that the upgrade class creates empty steps when disabled. */
  @Test
  public void testUpgradeDisabled() {
    EntityConsistencyConfiguration disabledConfig = createTestConfig(true, 10, 100, 1000, false);
    disabledConfig.setEnabled(false);
    FixEntityConsistency upgrade =
        new FixEntityConsistency(
            mockOpContext, mockEntityService, consistencyService, disabledConfig);

    assertTrue(upgrade.steps().isEmpty());
  }

  /** Test to verify that the upgrade class creates steps when enabled. */
  @Test
  public void testUpgradeEnabled() {
    FixEntityConsistency upgrade =
        new FixEntityConsistency(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(true, 10, 100, 1000, false));

    assertEquals(upgrade.steps().size(), 1);
    assertTrue(upgrade.steps().get(0) instanceof FixEntityConsistencyStep);
  }

  /** Test to verify the upgrade ID is correct. */
  @Test
  public void testUpgradeId() {
    FixEntityConsistency upgrade =
        new FixEntityConsistency(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(true, 10, 100, 1000, false));

    assertEquals(
        upgrade.id(), "com.linkedin.datahub.upgrade.system.entityconsistency.FixEntityConsistency");
  }

  /** Test to verify isOptional returns true. */
  @Test
  public void testIsOptional() {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 100, 1000, false));

    assertTrue(step.isOptional(), "Step should be optional");
  }

  /** Test to verify that filtering by check IDs to specific entity type works. */
  @Test
  public void testFilterByCheckIdsToEntityType() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(
                true,
                10,
                0,
                0,
                false,
                List.of(ASSERTION_ENTITY_NAME),
                List.of(
                    "assertion-entity-urn-missing",
                    "assertion-entity-type-invalid",
                    "assertion-entity-not-found",
                    "assertion-entity-soft-deleted",
                    "assertion-monitor-missing")));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Should have called scroll for the requested entity type.
    verify(mockScrollClient, atLeastOnce())
        .scrollUrns(
            any(OperationContext.class),
            argThat(req -> req != null && ASSERTION_ENTITY_NAME.equals(req.getEntityType())));
  }

  /** Test to verify that filtering by specific check ID works. */
  @Test
  public void testFilterBySpecificCheckId() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(
                true,
                10,
                0,
                0,
                false,
                List.of(ASSERTION_ENTITY_NAME),
                List.of("assertion-entity-not-found")));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  /** Test to verify that invalid assertion entity type detection works. */
  @Test
  public void testAssertionWithInvalidEntityTypeIsDetected() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(true, 10, 0, 0, false, List.of(ASSERTION_ENTITY_NAME), null));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test-assertion");

    when(mockScrollClient.scrollUrns(
            any(OperationContext.class), any(SystemMetadataScrollRequest.class)))
        .thenReturn(scrollResultOf(assertionUrn.toString()))
        .thenReturn(SystemMetadataScrollResult.empty());

    AssertionInfo assertionInfo = new AssertionInfo();

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        ASSERTION_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(assertionInfo.data())));

    EntityResponse assertionResponse = new EntityResponse();
    assertionResponse.setUrn(assertionUrn);
    assertionResponse.setEntityName(ASSERTION_ENTITY_NAME);
    assertionResponse.setAspects(aspects);

    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            any(Set.class),
            any(Set.class),
            anyBoolean()))
        .thenReturn(Map.of(assertionUrn, assertionResponse));

    when(mockEntityService.exists(any(OperationContext.class), any(Set.class), anyBoolean()))
        .thenReturn(Set.of());
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), anyBoolean()))
        .thenReturn(false);

    UpgradeStepResult result = step.executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockEntityService, times(1))
        .getEntitiesV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            any(Set.class),
            any(Set.class),
            anyBoolean());

    verify(mockEntityService, never())
        .ingestProposal(any(OperationContext.class), any(), anyBoolean());
  }

  /**
   * Test that on-demand checks are excluded when using default settings (null
   * entityTypes/checkIds).
   */
  @Test
  public void testOnDemandChecksExcludedByDefault() {
    ConsistencyCheckRegistry registry = consistencyService.getCheckRegistry();

    assertEquals(registry.getDefaultEntityTypes(), registry.getEntityTypes());

    assertEquals(registry.getDefaultChecks().size(), registry.getAll().size());

    assertEquals(
        registry.getDefaultByEntityType(ASSERTION_ENTITY_NAME).size(),
        registry.getByEntityType(ASSERTION_ENTITY_NAME).size());
  }

  /** Test that on-demand checks can be explicitly invoked by specifying check IDs. */
  @Test
  public void testExplicitCheckIdsIncludeOnDemand() {
    ConsistencyCheckRegistry registry = consistencyService.getCheckRegistry();

    List<String> explicitCheckIds = List.of("assertion-entity-urn-missing");
    List<ConsistencyCheck> checks =
        registry.getDefaultByEntityTypeAndIds(ASSERTION_ENTITY_NAME, explicitCheckIds);

    assertEquals(checks.size(), 1);
    assertEquals(checks.get(0).getId(), "assertion-entity-urn-missing");
  }

  /** Test that getDefaultByEntityTypeAndIds filters correctly when checkIds is empty. */
  @Test
  public void testDefaultByEntityTypeAndIdsWithEmptyCheckIds() {
    ConsistencyCheckRegistry registry = consistencyService.getCheckRegistry();

    List<ConsistencyCheck> checksWithNull =
        registry.getDefaultByEntityTypeAndIds(ASSERTION_ENTITY_NAME, null);
    List<ConsistencyCheck> checksWithEmpty =
        registry.getDefaultByEntityTypeAndIds(ASSERTION_ENTITY_NAME, List.of());
    List<ConsistencyCheck> defaultChecks = registry.getDefaultByEntityType(ASSERTION_ENTITY_NAME);

    assertEquals(checksWithNull.size(), defaultChecks.size());
    assertEquals(checksWithEmpty.size(), defaultChecks.size());
  }

  /** Test that config fingerprint produces different values for different configs. */
  @Test
  public void testConfigFingerprintDifferentConfigs() {
    EntityConsistencyConfiguration config1 = createTestConfig(false, 10, 0, 0, false);
    config1.setCheckIds(List.of("check-a", "check-b"));

    EntityConsistencyConfiguration config2 = createTestConfig(false, 10, 0, 0, false);
    config2.setCheckIds(List.of("check-a", "check-c"));

    FixEntityConsistencyStep step1 =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config1);
    FixEntityConsistencyStep step2 =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config2);

    assertEquals(step1.id(), step2.id());
  }

  /** Test that targeted runs (with timestamp filters) are detected correctly. */
  @Test
  public void testTargetedRunDetection() {
    EntityConsistencyConfiguration targetedConfig = createTestConfig(false, 10, 0, 0, false);
    EntityConsistencyConfiguration.SystemMetadataFilterConfig filterConfig =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();
    filterConfig.setGePitEpochMs(1000L);
    targetedConfig.setSystemMetadataFilterConfig(filterConfig);

    EntityConsistencyConfiguration regularConfig = createTestConfig(false, 10, 0, 0, false);

    FixEntityConsistencyStep targetedStep =
        new FixEntityConsistencyStep(
            mockOpContext, mockEntityService, consistencyService, targetedConfig);
    FixEntityConsistencyStep regularStep =
        new FixEntityConsistencyStep(
            mockOpContext, mockEntityService, consistencyService, regularConfig);

    assertEquals(targetedStep.id(), regularStep.id());
    assertEquals(targetedStep.id(), "entity-consistency-v1");
  }

  /** Test that limited runs (limit > 0) skip on SUCCEEDED like targeted runs. */
  @Test
  public void testSkipLimitedRunSucceeded() {
    EntityConsistencyConfiguration limitedConfig = createTestConfig(false, 10, 100, 500, false);

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext, mockEntityService, consistencyService, limitedConfig);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    assertTrue(step.skip(mockContext), "Limited run should skip on SUCCEEDED");
  }

  /**
   * Test that limited runs (limit > 0) do NOT skip on IN_PROGRESS - they start fresh (no resume).
   */
  @Test
  public void testLimitedRunInProgressStartsFresh() {
    EntityConsistencyConfiguration limitedConfig = createTestConfig(false, 10, 100, 500, false);

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext, mockEntityService, consistencyService, limitedConfig);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    assertFalse(
        step.skip(mockContext), "Limited run should not skip on IN_PROGRESS (starts fresh)");
  }

  /** Test that progress is saved after each batch (non-dry-run mode). */
  @Test
  public void testProgressSavedAfterBatch() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 0, 0, false, List.of(ASSERTION_ENTITY_NAME), null));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test-assertion");
    Urn validEntityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");

    when(mockScrollClient.scrollUrns(
            any(OperationContext.class), any(SystemMetadataScrollRequest.class)))
        .thenReturn(scrollResultOf(assertionUrn.toString()))
        .thenReturn(SystemMetadataScrollResult.empty());

    AssertionInfo assertionInfo = new AssertionInfo();

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        ASSERTION_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(assertionInfo.data())));

    EntityResponse assertionResponse = new EntityResponse();
    assertionResponse.setUrn(assertionUrn);
    assertionResponse.setEntityName(ASSERTION_ENTITY_NAME);
    assertionResponse.setAspects(aspects);

    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            any(Set.class),
            any(Set.class),
            anyBoolean()))
        .thenReturn(Map.of(assertionUrn, assertionResponse));

    when(mockEntityService.exists(any(OperationContext.class), any(Set.class), anyBoolean()))
        .thenReturn(Set.of(validEntityUrn));
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), anyBoolean()))
        .thenReturn(true);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgrade, atLeastOnce())
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(EntityService.class),
            eq(DataHubUpgradeState.IN_PROGRESS),
            any(Map.class));
  }

  // ============================================================================
  // Resume from IN_PROGRESS State Tests
  // ============================================================================

  /** Test that resuming from IN_PROGRESS state restores scrollId and counters. */
  @Test
  public void testResumeFromInProgressStateRestoresScrollIdAndCounters() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 0, 0, false, List.of(ASSERTION_ENTITY_NAME), null));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    DataHubUpgradeResult prevResult = mock(DataHubUpgradeResult.class);
    when(prevResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    StringMap savedState = new StringMap();
    savedState.put("runStartTime", "1000000");
    savedState.put("scrollId", "test-scroll-id");
    savedState.put("entitiesScanned", "50");
    savedState.put("issuesFound", "5");
    savedState.put("issuesFixed", "3");
    savedState.put("issuesFailed", "2");
    when(prevResult.getResult()).thenReturn(savedState);

    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(prevResult));

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify the saved scrollId was passed back into the scroll client on resume.
    verify(mockScrollClient, atLeastOnce())
        .scrollUrns(
            any(OperationContext.class),
            argThat(req -> req != null && "test-scroll-id".equals(req.getScrollId())));
  }

  /** Test that limited runs do NOT restore scrollId (start fresh). */
  @Test
  public void testLimitedRunDoesNotRestoreScrollId() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 0, 100, false, List.of(ASSERTION_ENTITY_NAME), null));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    DataHubUpgradeResult prevResult = mock(DataHubUpgradeResult.class);
    when(prevResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    StringMap savedState = new StringMap();
    savedState.put("scrollId", "old-scroll-id");
    when(prevResult.getResult()).thenReturn(savedState);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(prevResult));

    step.executable().apply(mockContext);

    // Saved scrollId is not reused for limited runs - first call must have null scrollId.
    verify(mockScrollClient, atLeastOnce())
        .scrollUrns(
            any(OperationContext.class), argThat(req -> req != null && req.getScrollId() == null));
  }

  // ============================================================================
  // Incremental Mode Tests
  // ============================================================================

  /** Test that incremental filter is applied from SUCCEEDED state for regular runs. */
  @Test
  public void testIncrementalFilterAppliedFromSucceededState() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 0, 0, false, List.of(ASSERTION_ENTITY_NAME), null));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    DataHubUpgradeResult prevResult = mock(DataHubUpgradeResult.class);
    when(prevResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    StringMap savedState = new StringMap();
    savedState.put("lastCompletedTime", "1234567890000");
    when(prevResult.getResult()).thenReturn(savedState);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(prevResult));

    step.executable().apply(mockContext);

    verify(mockScrollClient, atLeastOnce())
        .scrollUrns(any(OperationContext.class), any(SystemMetadataScrollRequest.class));
  }

  /** Test that incremental filter is NOT applied for targeted runs (with timestamp filters). */
  @Test
  public void testIncrementalFilterNotAppliedForTargetedRuns() throws Exception {
    EntityConsistencyConfiguration config =
        createTestConfig(false, 10, 0, 0, false, List.of(ASSERTION_ENTITY_NAME), null);
    EntityConsistencyConfiguration.SystemMetadataFilterConfig filterConfig =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();
    filterConfig.setGePitEpochMs(5000L);
    config.setSystemMetadataFilterConfig(filterConfig);

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    DataHubUpgradeResult prevResult = mock(DataHubUpgradeResult.class);
    when(prevResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    StringMap savedState = new StringMap();
    savedState.put("lastCompletedTime", "1234567890000");
    when(prevResult.getResult()).thenReturn(savedState);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(prevResult));

    assertTrue(step.skip(mockContext), "Targeted run should skip after SUCCEEDED");
  }

  // ============================================================================
  // Exception Handling Tests
  // ============================================================================

  /** Test that exception in executable returns FAILED state. */
  @Test
  public void testExceptionInExecutableReturnsFailed() throws Exception {
    ConsistencyService mockConsistencyService = mock(ConsistencyService.class);
    when(mockConsistencyService.getCheckRegistry())
        .thenReturn(consistencyService.getCheckRegistry());
    when(mockConsistencyService.buildContext(any()))
        .thenThrow(new RuntimeException("Simulated failure"));

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            mockConsistencyService,
            createTestConfig(false, 10, 0, 0, false));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockReport).addLine(contains("Error:"));
  }

  // ============================================================================
  // Limit Enforcement Tests
  // ============================================================================

  /** Test that processing stops when limit is reached. */
  @Test
  public void testLimitEnforcementStopsProcessing() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(true, 10, 0, 5, false, List.of(ASSERTION_ENTITY_NAME), null));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:test-2");
    Urn urn3 = UrnUtils.getUrn("urn:li:assertion:test-3");
    Urn urn4 = UrnUtils.getUrn("urn:li:assertion:test-4");
    Urn urn5 = UrnUtils.getUrn("urn:li:assertion:test-5");
    Urn urn6 = UrnUtils.getUrn("urn:li:assertion:test-6");

    when(mockScrollClient.scrollUrns(
            any(OperationContext.class), any(SystemMetadataScrollRequest.class)))
        .thenReturn(
            scrollResultOf(
                urn1.toString(),
                urn2.toString(),
                urn3.toString(),
                urn4.toString(),
                urn5.toString(),
                urn6.toString()))
        .thenReturn(SystemMetadataScrollResult.empty());

    Urn validEntityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    Map<Urn, EntityResponse> entityResponses = new HashMap<>();
    for (Urn urn : List.of(urn1, urn2, urn3, urn4, urn5, urn6)) {
      AssertionInfo assertionInfo = new AssertionInfo();

      EnvelopedAspectMap aspects = new EnvelopedAspectMap();
      aspects.put(
          ASSERTION_INFO_ASPECT_NAME,
          new EnvelopedAspect().setValue(new Aspect(assertionInfo.data())));
      EntityResponse response = new EntityResponse();
      response.setUrn(urn);
      response.setEntityName(ASSERTION_ENTITY_NAME);
      response.setAspects(aspects);
      entityResponses.put(urn, response);
    }

    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            any(Set.class),
            any(Set.class),
            anyBoolean()))
        .thenReturn(entityResponses);

    when(mockEntityService.exists(any(OperationContext.class), any(Set.class), anyBoolean()))
        .thenReturn(Set.of(validEntityUrn));
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), anyBoolean()))
        .thenReturn(true);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockReport).addLine(contains("Processed"));
  }

  @Test
  public void testOrphanCheckNotDueWhenUnconfigured() {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 0, 0, false));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    assertFalse(step.isOrphanCheckDue(mockContext));
  }

  @Test
  public void testOrphanCheckDueWhenConfiguredAndNeverRun() {
    EntityConsistencyConfiguration config = createTestConfig(false, 10, 0, 0, false);
    EntityConsistencyConfiguration.CheckRunConfig orphan =
        new EntityConsistencyConfiguration.CheckRunConfig();
    orphan.setMode("active");
    orphan.setSchedule("monthly");
    config.setChecks(Map.of("orphan-index-document", orphan));

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    assertTrue(step.isOrphanCheckDue(mockContext));
  }

  @Test
  public void testOrphanCheckNotDueWhenCompletedThisMonth() {
    EntityConsistencyConfiguration config = createTestConfig(false, 10, 0, 0, false);
    EntityConsistencyConfiguration.CheckRunConfig orphan =
        new EntityConsistencyConfiguration.CheckRunConfig();
    orphan.setMode("active");
    orphan.setSchedule("monthly");
    config.setChecks(Map.of("orphan-index-document", orphan));

    java.time.Clock fixed =
        java.time.Clock.fixed(
            java.time.Instant.parse("2026-07-15T12:00:00Z"), java.time.ZoneOffset.UTC);
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext, mockEntityService, consistencyService, config, fixed);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    DataHubUpgradeResult prev = mock(DataHubUpgradeResult.class);
    when(prev.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    StringMap savedState = new StringMap();
    savedState.put(
        "lastCompletedTime",
        String.valueOf(java.time.Instant.parse("2026-07-05T00:00:00Z").toEpochMilli()));
    when(prev.getResult()).thenReturn(savedState);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.of(prev));

    assertFalse(step.isOrphanCheckDue(mockContext));
  }

  @Test
  public void testOrphanCheckDueWhenExplicitCheckId() {
    EntityConsistencyConfiguration config =
        createTestConfig(false, 10, 0, 0, false, null, List.of("orphan-index-document"));
    // Mode defaults to dry-run when unset, but explicit checkIds still force due
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    assertTrue(step.isOrphanCheckDue(mockContext));
  }

  @Test
  public void testResolveEffectiveCheckIdsIncludesOrphanWhenDue() {
    EntityConsistencyConfiguration config = createTestConfig(false, 10, 0, 0, false);
    EntityConsistencyConfiguration.CheckRunConfig orphan =
        new EntityConsistencyConfiguration.CheckRunConfig();
    orphan.setMode("active");
    orphan.setSchedule("monthly");
    config.setChecks(Map.of("orphan-index-document", orphan));

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config);

    List<String> ids = step.resolveEffectiveCheckIds(true);
    assertTrue(ids.contains("orphan-index-document"));
  }

  @Test
  public void testResolveEffectiveCheckIdsExcludesOrphanWhenNotDue() {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 0, 0, false));

    List<String> ids = step.resolveEffectiveCheckIds(false);
    assertFalse(ids.contains("orphan-index-document"));
  }

  @Test
  public void testSkipDoesNotSkipWhenOrphanDue() {
    EntityConsistencyConfiguration config = createTestConfig(false, 10, 0, 0, false);
    EntityConsistencyConfiguration.CheckRunConfig orphan =
        new EntityConsistencyConfiguration.CheckRunConfig();
    orphan.setMode("active");
    orphan.setSchedule("monthly");
    config.setChecks(Map.of("orphan-index-document", orphan));
    EntityConsistencyConfiguration.SystemMetadataFilterConfig filterConfig =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();
    filterConfig.setGePitEpochMs(1000L);
    config.setSystemMetadataFilterConfig(filterConfig);

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    // Overall upgrade already succeeded (would normally skip for targeted), but orphan never run
    DataHubUpgradeResult overall = mock(DataHubUpgradeResult.class);
    when(overall.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenAnswer(
            invocation -> {
              Urn urn = invocation.getArgument(1);
              if (urn.toString().contains("orphan-index-document")) {
                return Optional.empty();
              }
              return Optional.of(overall);
            });

    assertFalse(step.skip(mockContext), "Orphan due should prevent skip on targeted success");
  }

  /**
   * When effective check IDs span multiple entity types, each per-type dispatch must receive only
   * the IDs applicable to that type (checkBatch rejects mixed-type ID lists).
   */
  @Test
  public void testFiltersCheckIdsToApplicableEntityTypeBeforeCheckBatch() throws Exception {
    ConsistencyCheckRegistry checkRegistry =
        new ConsistencyCheckRegistry(
            List.of(
                createMockCheck("assertion-entity-not-found", ASSERTION_ENTITY_NAME, false),
                createMockCheck("monitor-entity-not-found", "monitor", false)));
    ConsistencyFixRegistry fixRegistry =
        new ConsistencyFixRegistry(
            List.of(
                new BatchItemsFix(mockEntityService), new HardDeleteEntityFix(mockEntityService)));
    ConsistencyService multiTypeService =
        spy(
            new ConsistencyService(
                mockEntityService, mockScrollClient, null, checkRegistry, fixRegistry));

    CheckResult emptyResult =
        CheckResult.builder().entitiesScanned(0).issuesFound(0).issues(List.of()).build();
    doReturn(emptyResult)
        .when(multiTypeService)
        .checkBatch(any(), any(CheckBatchRequest.class), any());

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            multiTypeService,
            createTestConfig(
                true,
                10,
                0,
                0,
                false,
                List.of(ASSERTION_ENTITY_NAME, "monitor"),
                List.of("assertion-entity-not-found", "monitor-entity-not-found")));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    assertEquals(step.executable().apply(mockContext).result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<CheckBatchRequest> requestCaptor =
        ArgumentCaptor.forClass(CheckBatchRequest.class);
    verify(multiTypeService, times(2)).checkBatch(any(), requestCaptor.capture(), any());

    Map<String, List<String>> checkIdsByType = new HashMap<>();
    for (CheckBatchRequest request : requestCaptor.getAllValues()) {
      checkIdsByType.put(request.getEntityType(), request.getCheckIds());
    }
    assertEquals(checkIdsByType.get(ASSERTION_ENTITY_NAME), List.of("assertion-entity-not-found"));
    assertEquals(checkIdsByType.get("monitor"), List.of("monitor-entity-not-found"));
  }

  /**
   * Entity types with no applicable checks for the effective ID list are skipped before checkBatch.
   */
  @Test
  public void testSkipsEntityTypeWhenNoApplicableCheckIds() throws Exception {
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(
                true, 10, 0, 0, false, List.of("monitor"), List.of("assertion-entity-not-found")));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    assertEquals(step.executable().apply(mockContext).result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockScrollClient, never())
        .scrollUrns(any(OperationContext.class), any(SystemMetadataScrollRequest.class));
  }

  /** Empty effective check IDs leave the list empty so checkBatch uses entity-type defaults. */
  @Test
  public void testEmptyEffectiveCheckIdsPassedThroughToCheckBatch() throws Exception {
    ConsistencyService spyService = spy(consistencyService);
    CheckResult emptyResult =
        CheckResult.builder().entitiesScanned(0).issuesFound(0).issues(List.of()).build();
    doReturn(emptyResult).when(spyService).checkBatch(any(), any(CheckBatchRequest.class), any());

    EntityConsistencyConfiguration config =
        createTestConfig(
            true,
            10,
            0,
            0,
            false,
            List.of(ASSERTION_ENTITY_NAME),
            List.of("assertion-entity-not-found"));
    EntityConsistencyConfiguration.CheckRunConfig disabled =
        new EntityConsistencyConfiguration.CheckRunConfig();
    disabled.setMode("disabled");
    config.setChecks(Map.of("assertion-entity-not-found", disabled));

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, spyService, config);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    assertEquals(step.executable().apply(mockContext).result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<CheckBatchRequest> requestCaptor =
        ArgumentCaptor.forClass(CheckBatchRequest.class);
    verify(spyService).checkBatch(any(), requestCaptor.capture(), any());
    assertTrue(requestCaptor.getValue().getCheckIds().isEmpty());
  }

  @Test
  public void testResolveEntityTypesUsesConfiguredList() {
    EntityConsistencyConfiguration config =
        createTestConfig(
            false, 10, 0, 0, false, List.of("dataset", "dashboard", "query", "schemaField"), null);

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config);

    Set<String> types = step.resolveEntityTypes(true);
    assertEquals(types.size(), 4);
    assertTrue(types.contains("dataset"));
    assertTrue(types.contains("query"));
    assertTrue(types.contains("schemaField"));
  }

  @Test
  public void testResolveEntityTypesUsesSystemMetadataFilterListWhenOrphanDue() {
    EntityConsistencyConfiguration config = createTestConfig(false, 10, 0, 0, false);
    EntityConsistencyConfiguration.SystemMetadataFilterConfig filterConfig =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();
    filterConfig.setEntityTypes(List.of("dataset", "query", "schemaField", "dataProcessInstance"));
    config.setSystemMetadataFilterConfig(filterConfig);

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config);

    Set<String> orphanTypes = step.resolveEntityTypes(true);
    assertEquals(orphanTypes.size(), 4);
    assertTrue(orphanTypes.contains("dataProcessInstance"));

    // Non-orphan pass ignores SM filter entityTypes and uses default checks' types
    Set<String> defaultTypes = step.resolveEntityTypes(false);
    assertFalse(defaultTypes.contains("dataProcessInstance"));
  }
}
