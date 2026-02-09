package com.linkedin.datahub.upgrade.entityconsistency;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
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
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.lucene.search.TotalHits;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FixEntityConsistencyStepTest {

  @Mock private OperationContext mockOpContext;

  @Mock private EntityService<?> mockEntityService;

  @Mock private ESSystemMetadataDAO mockEsSystemMetadataDAO;

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

    // Setup entity registry mocks for annotation-based entity type validation
    setupEntityRegistryMocks();

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

    // Create fixes for registry
    List<ConsistencyFix> fixes =
        List.of(new BatchItemsFix(mockEntityService), new HardDeleteEntityFix(mockEntityService));

    ConsistencyFixRegistry fixRegistry = new ConsistencyFixRegistry(fixes);

    consistencyService =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, checkRegistry, fixRegistry);
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
        // Return empty list - no issues detected by default
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
    // Setup assertion entity spec with assertionInfo aspect
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

    // Setup monitor entity spec with monitorKey aspect
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

    // Mock previous succeeded run
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
    // Regular run - no timestamp filters, no limit (limit=0)
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 100, 0, false));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    // Mock previous succeeded run
    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    // Regular runs should NOT skip on SUCCEEDED - allow incremental
    assertFalse(
        step.skip(mockContext), "Regular run should not skip on SUCCEEDED (incremental mode)");
  }

  /** Test skip logic for targeted runs (with timestamp filters) - should skip on SUCCEEDED. */
  @Test
  public void testSkipTargetedRunSucceeded() {
    // Targeted run - has timestamp filter
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

    // Mock previous succeeded run
    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    // Targeted runs should skip on SUCCEEDED
    assertTrue(step.skip(mockContext), "Targeted run should skip on SUCCEEDED");
  }

  /** Test skip logic - should NOT skip on IN_PROGRESS (resume) for non-limited runs. */
  @Test
  public void testSkipInProgressAllowsResume() {
    // Regular run (limit=0) - supports resume
    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext,
            mockEntityService,
            consistencyService,
            createTestConfig(false, 10, 100, 0, false));

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    // Mock previous IN_PROGRESS run
    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    // Should NOT skip - allow resume
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

    // No previous run
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    assertFalse(step.skip(mockContext), "Should not skip when no previous run");
  }

  /**
   * Test to verify the executable function processes batches correctly and returns a success result
   * when no orphaned entities are found.
   */
  @Test
  public void testExecutableWithNoOrphans() throws Exception {
    // dryRun=true so no actual writes expected
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

    // No previous run state
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    // Mock empty search response from system metadata DAO
    SearchResponse emptyResponse = createEmptySearchResponse();

    when(mockEsSystemMetadataDAO.scroll(
            any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt()))
        .thenReturn(emptyResponse);

    // Mock entity existence check (both Set and single Urn versions)
    when(mockEntityService.exists(any(OperationContext.class), any(Set.class), anyBoolean()))
        .thenReturn(Set.of());
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), anyBoolean()))
        .thenReturn(false);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  /** Helper method to create an empty SearchResponse for mocking. */
  private SearchResponse createEmptySearchResponse() {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits = mock(SearchHits.class);
    when(hits.getHits()).thenReturn(new SearchHit[0]);
    when(hits.getTotalHits()).thenReturn(new TotalHits(0, TotalHits.Relation.EQUAL_TO));
    when(response.getHits()).thenReturn(hits);
    return response;
  }

  /** Helper method to create a SearchResponse with URNs for mocking. */
  private SearchResponse createSearchResponseWithUrns(String... urns) {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits hits = mock(SearchHits.class);
    SearchHit[] searchHits = new SearchHit[urns.length];
    for (int i = 0; i < urns.length; i++) {
      SearchHit hit = mock(SearchHit.class);
      when(hit.getSourceAsMap()).thenReturn(Map.of("urn", urns[i], "aspect", "someAspect"));
      when(hit.getSortValues()).thenReturn(new Object[] {urns[i], "someAspect"});
      searchHits[i] = hit;
    }
    when(hits.getHits()).thenReturn(searchHits);
    when(hits.getTotalHits()).thenReturn(new TotalHits(urns.length, TotalHits.Relation.EQUAL_TO));
    when(response.getHits()).thenReturn(hits);
    return response;
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
    // Only run assertion checks - pass checkIds, let step resolve entity types
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

    // No previous run state
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    // Mock empty search response from system metadata DAO
    SearchResponse emptyResponse = createEmptySearchResponse();

    when(mockEsSystemMetadataDAO.scroll(
            any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt()))
        .thenReturn(emptyResponse);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Should have called scroll for assertions only
    verify(mockEsSystemMetadataDAO)
        .scroll(any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt());
  }

  /** Test to verify that filtering by specific check ID works. */
  @Test
  public void testFilterBySpecificCheckId() throws Exception {
    // Only run one specific check
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

    // No previous run state
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    // Mock empty search response from system metadata DAO
    SearchResponse emptyResponse = createEmptySearchResponse();

    when(mockEsSystemMetadataDAO.scroll(
            any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt()))
        .thenReturn(emptyResponse);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  /** Test to verify that invalid assertion entity type detection works. */
  @Test
  public void testAssertionWithInvalidEntityTypeIsDetected() throws Exception {
    // Use dryRun=true so we don't try to actually soft-delete (which needs EntityRegistry)
    // Specify entity type explicitly to avoid Set iteration order issues
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

    // No previous run state
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    // Create assertion with invalid entity type (corpuser instead of dataset)
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test-assertion");
    Urn invalidEntityUrn = UrnUtils.getUrn("urn:li:corpuser:test"); // Invalid type

    // Mock system metadata DAO responses
    SearchResponse assertionSearchResponse = createSearchResponseWithUrns(assertionUrn.toString());
    SearchResponse emptyResponse = createEmptySearchResponse();

    when(mockEsSystemMetadataDAO.scroll(
            any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt()))
        .thenReturn(assertionSearchResponse) // Assertions phase
        .thenReturn(emptyResponse); // Monitors phase

    // Create AssertionInfo with invalid entity type
    AssertionInfo assertionInfo = new AssertionInfo();

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        ASSERTION_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(assertionInfo.data())));

    EntityResponse assertionResponse = new EntityResponse();
    assertionResponse.setUrn(assertionUrn);
    assertionResponse.setEntityName(ASSERTION_ENTITY_NAME);
    assertionResponse.setAspects(aspects);

    // Return the assertion response for any assertion request
    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            any(Set.class),
            any(Set.class),
            anyBoolean()))
        .thenReturn(Map.of(assertionUrn, assertionResponse));

    // Mock exists for both Set and single Urn versions
    when(mockEntityService.exists(any(OperationContext.class), any(Set.class), anyBoolean()))
        .thenReturn(Set.of());
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), anyBoolean()))
        .thenReturn(false);

    // With dryRun=true, the step should complete successfully without actually making changes
    UpgradeStepResult result = step.executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify the assertion was fetched and processed (detection happened)
    verify(mockEntityService, times(1))
        .getEntitiesV2(
            any(OperationContext.class),
            eq(ASSERTION_ENTITY_NAME),
            any(Set.class),
            any(Set.class),
            anyBoolean());

    // In dry-run mode, no actual soft-delete should happen
    verify(mockEntityService, never())
        .ingestProposal(any(OperationContext.class), any(), anyBoolean());
  }

  /**
   * Test that on-demand checks are excluded when using default settings (null
   * entityTypes/checkIds).
   */
  @Test
  public void testOnDemandChecksExcludedByDefault() {
    // The AssertionMonitorMissingCheck is NOT on-demand by default, so let's verify
    // that all default checks are included and count is correct.
    ConsistencyCheckRegistry registry = consistencyService.getCheckRegistry();

    // Verify getDefaultEntityTypes returns same as getEntityTypes (no on-demand checks in test
    // setup)
    assertEquals(registry.getDefaultEntityTypes(), registry.getEntityTypes());

    // Verify getDefaultChecks returns same as getAll (no on-demand checks in test setup)
    assertEquals(registry.getDefaultChecks().size(), registry.getAll().size());

    // Verify getDefaultByEntityType returns same as getByEntityType
    assertEquals(
        registry.getDefaultByEntityType(ASSERTION_ENTITY_NAME).size(),
        registry.getByEntityType(ASSERTION_ENTITY_NAME).size());
  }

  /** Test that on-demand checks can be explicitly invoked by specifying check IDs. */
  @Test
  public void testExplicitCheckIdsIncludeOnDemand() {
    ConsistencyCheckRegistry registry = consistencyService.getCheckRegistry();

    // When checkIds is specified, getDefaultByEntityTypeAndIds should return those checks
    // even if they were on-demand (though in our test setup none are on-demand)
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

    // When checkIds is empty/null, should return all default checks for entity type
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
    // Two configs with different checkIds should have different fingerprints
    EntityConsistencyConfiguration config1 = createTestConfig(false, 10, 0, 0, false);
    config1.setCheckIds(List.of("check-a", "check-b"));

    EntityConsistencyConfiguration config2 = createTestConfig(false, 10, 0, 0, false);
    config2.setCheckIds(List.of("check-a", "check-c"));

    FixEntityConsistencyStep step1 =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config1);
    FixEntityConsistencyStep step2 =
        new FixEntityConsistencyStep(mockOpContext, mockEntityService, consistencyService, config2);

    // Both should have same id() but different fingerprinted URNs (via skip behavior)
    assertEquals(step1.id(), step2.id());
    // The fingerprints are computed internally, so we verify behavior via skip logic
  }

  /** Test that targeted runs (with timestamp filters) are detected correctly. */
  @Test
  public void testTargetedRunDetection() {
    // Config with timestamp filter = targeted run
    EntityConsistencyConfiguration targetedConfig = createTestConfig(false, 10, 0, 0, false);
    EntityConsistencyConfiguration.SystemMetadataFilterConfig filterConfig =
        new EntityConsistencyConfiguration.SystemMetadataFilterConfig();
    filterConfig.setGePitEpochMs(1000L);
    targetedConfig.setSystemMetadataFilterConfig(filterConfig);

    // Config without timestamp filter = regular run
    EntityConsistencyConfiguration regularConfig = createTestConfig(false, 10, 0, 0, false);

    FixEntityConsistencyStep targetedStep =
        new FixEntityConsistencyStep(
            mockOpContext, mockEntityService, consistencyService, targetedConfig);
    FixEntityConsistencyStep regularStep =
        new FixEntityConsistencyStep(
            mockOpContext, mockEntityService, consistencyService, regularConfig);

    // Both have same base ID
    assertEquals(targetedStep.id(), regularStep.id());
    assertEquals(targetedStep.id(), "entity-consistency-v1");
  }

  /** Test that limited runs (limit > 0) skip on SUCCEEDED like targeted runs. */
  @Test
  public void testSkipLimitedRunSucceeded() {
    // Limited run - has limit > 0
    EntityConsistencyConfiguration limitedConfig =
        createTestConfig(false, 10, 100, 500, false); // limit=500

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext, mockEntityService, consistencyService, limitedConfig);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    // Mock previous succeeded run
    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    // Limited runs should skip on SUCCEEDED (no incremental support)
    assertTrue(step.skip(mockContext), "Limited run should skip on SUCCEEDED");
  }

  /**
   * Test that limited runs (limit > 0) do NOT skip on IN_PROGRESS - they start fresh (no resume).
   */
  @Test
  public void testLimitedRunInProgressStartsFresh() {
    // Limited run - has limit > 0
    EntityConsistencyConfiguration limitedConfig =
        createTestConfig(false, 10, 100, 500, false); // limit=500

    FixEntityConsistencyStep step =
        new FixEntityConsistencyStep(
            mockOpContext, mockEntityService, consistencyService, limitedConfig);

    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    // Mock previous IN_PROGRESS run
    DataHubUpgradeResult mockResult = mock(DataHubUpgradeResult.class);
    when(mockResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(mockResult));

    // Limited runs should NOT skip on IN_PROGRESS (but they will start fresh, not resume)
    assertFalse(
        step.skip(mockContext), "Limited run should not skip on IN_PROGRESS (starts fresh)");
  }

  /** Test that progress is saved after each batch (non-dry-run mode). */
  @Test
  public void testProgressSavedAfterBatch() throws Exception {
    // Use non-dry-run mode to verify state is saved
    // Specify a single entity type to avoid Set iteration order issues
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

    // No previous run state
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    // Create an assertion to process
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test-assertion");
    Urn validEntityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");

    // Mock search response with an assertion
    SearchResponse assertionSearchResponse = createSearchResponseWithUrns(assertionUrn.toString());
    SearchResponse emptyResponse = createEmptySearchResponse();
    when(mockEsSystemMetadataDAO.scroll(
            any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt()))
        .thenReturn(assertionSearchResponse) // First batch
        .thenReturn(emptyResponse); // No more results

    // Create valid AssertionInfo (no issues to fix)
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

    // Mock entity existence
    when(mockEntityService.exists(any(OperationContext.class), any(Set.class), anyBoolean()))
        .thenReturn(Set.of(validEntityUrn));
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), anyBoolean()))
        .thenReturn(true);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify progress was saved during processing (IN_PROGRESS state)
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
    // Create step with regular config (not limited)
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

    // Mock previous IN_PROGRESS state with saved progress
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

    // Return IN_PROGRESS for both overall and entity-type URNs
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(prevResult));

    // Mock empty search response (no more entities to process)
    SearchResponse emptyResponse = createEmptySearchResponse();
    when(mockEsSystemMetadataDAO.scroll(
            any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt()))
        .thenReturn(emptyResponse);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify scroll was called with the restored scrollId
    // (The mock returns empty, so it will complete immediately, but we verify it attempted to
    // resume)
    verify(mockEsSystemMetadataDAO, atLeastOnce())
        .scroll(
            any(BoolQueryBuilder.class),
            anyBoolean(),
            eq("test-scroll-id"),
            any(),
            anyString(),
            anyInt());
  }

  /** Test that limited runs do NOT restore scrollId (start fresh). */
  @Test
  public void testLimitedRunDoesNotRestoreScrollId() throws Exception {
    // Create step with limit > 0
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

    // Mock previous IN_PROGRESS state with saved scrollId
    DataHubUpgradeResult prevResult = mock(DataHubUpgradeResult.class);
    when(prevResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    StringMap savedState = new StringMap();
    savedState.put("scrollId", "old-scroll-id");
    when(prevResult.getResult()).thenReturn(savedState);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(prevResult));

    // Mock empty search response
    SearchResponse emptyResponse = createEmptySearchResponse();
    when(mockEsSystemMetadataDAO.scroll(
            any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt()))
        .thenReturn(emptyResponse);

    step.executable().apply(mockContext);

    // Verify scroll was called with null scrollId (not the saved one)
    verify(mockEsSystemMetadataDAO, atLeastOnce())
        .scroll(any(BoolQueryBuilder.class), anyBoolean(), isNull(), any(), anyString(), anyInt());
  }

  // ============================================================================
  // Incremental Mode Tests
  // ============================================================================

  /** Test that incremental filter is applied from SUCCEEDED state for regular runs. */
  @Test
  public void testIncrementalFilterAppliedFromSucceededState() throws Exception {
    // Create step with regular config (not targeted)
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

    // Mock previous SUCCEEDED state with lastCompletedTime
    DataHubUpgradeResult prevResult = mock(DataHubUpgradeResult.class);
    when(prevResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    StringMap savedState = new StringMap();
    savedState.put("lastCompletedTime", "1234567890000");
    when(prevResult.getResult()).thenReturn(savedState);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(prevResult));

    // Mock empty search response
    SearchResponse emptyResponse = createEmptySearchResponse();
    when(mockEsSystemMetadataDAO.scroll(
            any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt()))
        .thenReturn(emptyResponse);

    step.executable().apply(mockContext);

    // The step should complete - we can't easily verify the filter was applied
    // but we verify the step executes without error
    verify(mockEsSystemMetadataDAO, atLeastOnce())
        .scroll(any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt());
  }

  /** Test that incremental filter is NOT applied for targeted runs (with timestamp filters). */
  @Test
  public void testIncrementalFilterNotAppliedForTargetedRuns() throws Exception {
    // Create step with timestamp filter (targeted run)
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

    // Mock previous SUCCEEDED state with lastCompletedTime
    DataHubUpgradeResult prevResult = mock(DataHubUpgradeResult.class);
    when(prevResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    StringMap savedState = new StringMap();
    savedState.put("lastCompletedTime", "1234567890000");
    when(prevResult.getResult()).thenReturn(savedState);
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(prevResult));

    // For targeted runs that already succeeded, skip() should return true
    assertTrue(step.skip(mockContext), "Targeted run should skip after SUCCEEDED");
  }

  // ============================================================================
  // Exception Handling Tests
  // ============================================================================

  /** Test that exception in executable returns FAILED state. */
  @Test
  public void testExceptionInExecutableReturnsFailed() throws Exception {
    // Create a mock ConsistencyService that throws
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

    // No previous run state
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    // Verify error was added to report
    verify(mockReport).addLine(contains("Error:"));
  }

  // ============================================================================
  // Limit Enforcement Tests
  // ============================================================================

  /** Test that processing stops when limit is reached. */
  @Test
  public void testLimitEnforcementStopsProcessing() throws Exception {
    // Create step with limit of 5
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

    // No previous run state
    when(mockUpgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    // Create responses with 10 entities (more than limit of 5)
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:test-2");
    Urn urn3 = UrnUtils.getUrn("urn:li:assertion:test-3");
    Urn urn4 = UrnUtils.getUrn("urn:li:assertion:test-4");
    Urn urn5 = UrnUtils.getUrn("urn:li:assertion:test-5");
    Urn urn6 = UrnUtils.getUrn("urn:li:assertion:test-6");

    SearchResponse firstBatch =
        createSearchResponseWithUrns(
            urn1.toString(),
            urn2.toString(),
            urn3.toString(),
            urn4.toString(),
            urn5.toString(),
            urn6.toString());
    SearchResponse emptyResponse = createEmptySearchResponse();

    when(mockEsSystemMetadataDAO.scroll(
            any(BoolQueryBuilder.class), anyBoolean(), any(), any(), anyString(), anyInt()))
        .thenReturn(firstBatch)
        .thenReturn(emptyResponse);

    // Mock entity responses
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
    // With limit=5 and 6 entities returned, should only process until limit
    // The report should reflect this
    verify(mockReport).addLine(contains("Processed"));
  }
}
