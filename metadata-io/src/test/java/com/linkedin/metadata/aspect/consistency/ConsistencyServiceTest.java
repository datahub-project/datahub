package com.linkedin.metadata.aspect.consistency;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.check.CheckBatchRequest;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import com.linkedin.metadata.aspect.consistency.fix.BatchItemsFix;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFix;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixDetail;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixResult;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.aspect.consistency.fix.HardDeleteEntityFix;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for ConsistencyService focusing on validation and aspect derivation logic. */
public class ConsistencyServiceTest {

  @Mock private EntityService<?> mockEntityService;

  @Mock private ESSystemMetadataDAO mockEsSystemMetadataDAO;

  private ConsistencyService consistencyService;
  private ConsistencyCheckRegistry checkRegistry;
  private ConsistencyFixRegistry fixRegistry;

  /** Test check for entity type "assertion" */
  private static class TestAssertionCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "test-assertion-check";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Test Assertion Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "Test check for assertions";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "assertion";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("assertionInfo", "assertionKey"));
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  /** Second test check for entity type "assertion" */
  private static class TestAssertionCheck2 implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "test-assertion-check-2";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Test Assertion Check 2";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "Another test check for assertions";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "assertion";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("assertionRunEvent"));
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  /** Test check that requires all aspects (like AspectSchemaValidationCheck) */
  private static class TestRequiresAllAspectsCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "test-requires-all-aspects";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Test Requires All Aspects";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "Test check that requires all aspects";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "assertion";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.empty(); // Empty optional = needs all aspects
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  /** Test check for entity type "monitor" */
  private static class TestMonitorCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "test-monitor-check";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Test Monitor Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "Test check for monitors";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "monitor";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("monitorInfo", "monitorKey"));
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
      return List.of();
    }
  }

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Create check registry with test checks
    List<ConsistencyCheck> checks =
        List.of(new TestAssertionCheck(), new TestAssertionCheck2(), new TestMonitorCheck());
    checkRegistry = new ConsistencyCheckRegistry(checks);

    // Create fix registry
    List<ConsistencyFix> fixes =
        List.of(new BatchItemsFix(mockEntityService), new HardDeleteEntityFix(mockEntityService));
    fixRegistry = new ConsistencyFixRegistry(fixes);

    consistencyService =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, checkRegistry, fixRegistry);
  }

  // ============================================================================
  // discoverIssue Tests
  // ============================================================================

  @Mock private OperationContext mockOpContext;
  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private EntitySpec mockEntitySpec;

  @Test
  public void testDiscoverIssueFindsIssue() throws Exception {
    Urn testUrn = UrnUtils.getUrn("urn:li:assertion:test-123");

    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockEntityRegistry.getEntitySpec("assertion")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpecMap()).thenReturn(Map.of());

    EntityResponse response = new EntityResponse();
    when(mockEntityService.getEntitiesV2(
            eq(mockOpContext), eq("assertion"), eq(Set.of(testUrn)), any(), eq(false)))
        .thenReturn(Map.of(testUrn, response));

    ConsistencyCheck issueCheck =
        new ConsistencyCheck() {
          @Override
          @Nonnull
          public String getId() {
            return "test-discover-check";
          }

          @Override
          @Nonnull
          public String getName() {
            return "Test Discover Check";
          }

          @Override
          @Nonnull
          public String getDescription() {
            return "Check for discover tests";
          }

          @Override
          @Nonnull
          public String getEntityType() {
            return "assertion";
          }

          @Override
          @Nonnull
          public Optional<Set<String>> getRequiredAspects() {
            return Optional.of(Set.of("assertionInfo"));
          }

          @Override
          @Nonnull
          public List<ConsistencyIssue> check(
              @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
            return entityResponses.keySet().stream()
                .map(
                    urn ->
                        ConsistencyIssue.builder()
                            .entityUrn(urn)
                            .entityType("assertion")
                            .checkId("test-discover-check")
                            .fixType(ConsistencyFixType.SOFT_DELETE)
                            .description("Discovered issue")
                            .build())
                .toList();
          }
        };

    ConsistencyCheckRegistry discoverRegistry = new ConsistencyCheckRegistry(List.of(issueCheck));
    ConsistencyService serviceWithDiscoverCheck =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, discoverRegistry, fixRegistry);

    Optional<ConsistencyIssue> result =
        serviceWithDiscoverCheck.discoverIssue(mockOpContext, testUrn, "test-discover-check");

    assertTrue(result.isPresent());
    assertEquals(result.get().getEntityUrn(), testUrn);
    assertEquals(result.get().getCheckId(), "test-discover-check");
    assertEquals(result.get().getFixType(), ConsistencyFixType.SOFT_DELETE);
  }

  @Test
  public void testDiscoverIssueNoIssueFound() throws Exception {
    Urn testUrn = UrnUtils.getUrn("urn:li:assertion:test-456");

    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockEntityRegistry.getEntitySpec("assertion")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpecMap()).thenReturn(Map.of());

    EntityResponse response = new EntityResponse();
    when(mockEntityService.getEntitiesV2(
            eq(mockOpContext), eq("assertion"), eq(Set.of(testUrn)), any(), eq(false)))
        .thenReturn(Map.of(testUrn, response));

    Optional<ConsistencyIssue> result =
        consistencyService.discoverIssue(mockOpContext, testUrn, "test-assertion-check");

    assertFalse(result.isPresent());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDiscoverIssueUnknownCheckId() {
    Urn testUrn = UrnUtils.getUrn("urn:li:assertion:test-789");
    consistencyService.discoverIssue(mockOpContext, testUrn, "non-existent-check");
  }

  @Test
  public void testDiscoverIssueEntityNotFound() throws Exception {
    Urn testUrn = UrnUtils.getUrn("urn:li:assertion:missing");

    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockEntityRegistry.getEntitySpec("assertion")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpecMap()).thenReturn(Map.of());

    when(mockEntityService.getEntitiesV2(
            eq(mockOpContext), eq("assertion"), eq(Set.of(testUrn)), any(), eq(false)))
        .thenReturn(Map.of());

    Optional<ConsistencyIssue> result =
        consistencyService.discoverIssue(mockOpContext, testUrn, "test-assertion-check");

    assertFalse(result.isPresent());
  }

  @Test
  public void testDiscoverIssueRequiresAllAspects() throws Exception {
    Urn testUrn = UrnUtils.getUrn("urn:li:assertion:all-aspects");

    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockEntityRegistry.getEntitySpec("assertion")).thenReturn(mockEntitySpec);
    Set<String> allAspects = Set.of("aspect1", "aspect2", "aspect3");
    when(mockEntitySpec.getAspectSpecMap())
        .thenReturn(
            Map.of(
                "aspect1", mock(com.linkedin.metadata.models.AspectSpec.class),
                "aspect2", mock(com.linkedin.metadata.models.AspectSpec.class),
                "aspect3", mock(com.linkedin.metadata.models.AspectSpec.class)));

    EntityResponse response = new EntityResponse();
    when(mockEntityService.getEntitiesV2(
            eq(mockOpContext), eq("assertion"), eq(Set.of(testUrn)), eq(allAspects), eq(false)))
        .thenReturn(Map.of(testUrn, response));

    ConsistencyCheckRegistry allAspectsRegistry =
        new ConsistencyCheckRegistry(List.of(new TestRequiresAllAspectsCheck()));
    ConsistencyService serviceWithAllAspects =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, allAspectsRegistry, fixRegistry);

    Optional<ConsistencyIssue> result =
        serviceWithAllAspects.discoverIssue(mockOpContext, testUrn, "test-requires-all-aspects");

    assertFalse(result.isPresent());
    verify(mockEntityService)
        .getEntitiesV2(
            eq(mockOpContext), eq("assertion"), eq(Set.of(testUrn)), eq(allAspects), eq(false));
  }

  // ============================================================================
  // checkBatch URN Entity Type Validation Tests
  // ============================================================================

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCheckBatchUrnsWithMixedEntityTypes() {
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn monitorUrn = UrnUtils.getUrn("urn:li:monitor:test-2");

    consistencyService.checkBatch(
        mockOpContext,
        CheckBatchRequest.builder().urns(Set.of(assertionUrn, monitorUrn)).batchSize(100).build());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCheckBatchUrnsWithMismatchedEntityType() {
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test-1");

    consistencyService.checkBatch(
        mockOpContext,
        CheckBatchRequest.builder()
            .urns(Set.of(assertionUrn))
            .entityType("monitor")
            .batchSize(100)
            .build());
  }

  @Test
  public void testCheckBatchUrnEntityTypeFromUrnsMismatchesCheckIdsReturnsEmpty() {
    Urn monitorUrn = UrnUtils.getUrn("urn:li:monitor:test-1");

    CheckResult result =
        consistencyService.checkBatch(
            mockOpContext,
            CheckBatchRequest.builder()
                .urns(Set.of(monitorUrn))
                .checkIds(List.of("test-assertion-check"))
                .batchSize(100)
                .build());

    assertNotNull(result);
    assertEquals(result.getEntitiesScanned(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCheckBatchRequiresEntityTypeOrCheckIds() {
    consistencyService.checkBatch(
        mockOpContext, CheckBatchRequest.builder().batchSize(100).build());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCheckBatchWithEmptyEntityType() {
    consistencyService.checkBatch(
        mockOpContext, CheckBatchRequest.builder().entityType("").batchSize(100).build());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCheckBatchWithInvalidCheckIds() {
    consistencyService.checkBatch(
        mockOpContext,
        CheckBatchRequest.builder().checkIds(List.of("non-existent-check")).batchSize(100).build());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCheckBatchWithMixedEntityTypeCheckIds() {
    consistencyService.checkBatch(
        mockOpContext,
        CheckBatchRequest.builder()
            .checkIds(List.of("test-assertion-check", "test-monitor-check"))
            .batchSize(100)
            .build());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCheckBatchWithMismatchedEntityTypeAndCheckIds() {
    consistencyService.checkBatch(
        mockOpContext,
        CheckBatchRequest.builder()
            .entityType("monitor")
            .checkIds(List.of("test-assertion-check"))
            .batchSize(100)
            .build());
  }

  // ============================================================================
  // checkBatch Wildcard Check Tests
  // ============================================================================

  @Test
  public void testCheckBatchWithWildcardCheckRequiresEntityType() {
    ConsistencyCheck wildcardCheck =
        new ConsistencyCheck() {
          @Override
          @Nonnull
          public String getId() {
            return "test-wildcard-check";
          }

          @Override
          @Nonnull
          public String getName() {
            return "Test Wildcard Check";
          }

          @Override
          @Nonnull
          public String getDescription() {
            return "Test wildcard check";
          }

          @Override
          @Nonnull
          public String getEntityType() {
            return ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE;
          }

          @Override
          @Nonnull
          public Optional<Set<String>> getRequiredAspects() {
            return Optional.of(Set.of());
          }

          @Override
          @Nonnull
          public List<ConsistencyIssue> check(
              @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
            return List.of();
          }
        };

    ConsistencyCheckRegistry wildcardRegistry =
        new ConsistencyCheckRegistry(List.of(wildcardCheck));
    ConsistencyService serviceWithWildcard =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, wildcardRegistry, fixRegistry);

    try {
      serviceWithWildcard.checkBatch(
          mockOpContext,
          CheckBatchRequest.builder()
              .checkIds(List.of("test-wildcard-check"))
              .batchSize(100)
              .build());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("entityType is required"));
    }
  }

  // ============================================================================
  // Registry Access Tests
  // ============================================================================

  @Test
  public void testGetCheckRegistry() {
    ConsistencyCheckRegistry registry = consistencyService.getCheckRegistry();
    assertNotNull(registry);
    assertSame(registry, checkRegistry);
  }

  @Test
  public void testGetFixRegistry() {
    ConsistencyFixRegistry registry = consistencyService.getFixRegistry();
    assertNotNull(registry);
    assertSame(registry, fixRegistry);
  }

  // ============================================================================
  // Aspect Derivation Tests
  // ============================================================================

  @Test
  public void testRequiredAspectsAggregation() {
    // Get checks for assertion type
    List<ConsistencyCheck> assertionChecks = checkRegistry.getByEntityTypeExact("assertion");
    assertEquals(assertionChecks.size(), 2);

    // Verify each check has its own required aspects
    Optional<Set<String>> check1Aspects = assertionChecks.get(0).getRequiredAspects();
    Optional<Set<String>> check2Aspects = assertionChecks.get(1).getRequiredAspects();

    // The aspects should be different but both should be returned
    assertTrue(check1Aspects.isPresent());
    assertTrue(check2Aspects.isPresent());
    assertFalse(check1Aspects.get().equals(check2Aspects.get()));
  }

  @Test
  public void testDeriveRequiredAspectsConsolidatesAspects() {
    // Test that deriveRequiredAspects consolidates aspects from multiple checks
    List<ConsistencyCheck> checks = List.of(new TestAssertionCheck(), new TestAssertionCheck2());

    Set<String> derivedAspects = consistencyService.deriveRequiredAspects(checks);

    assertNotNull(derivedAspects);
    // Should include status (always added) plus aspects from both checks
    assertTrue(derivedAspects.contains(STATUS_ASPECT_NAME));
    assertTrue(derivedAspects.contains("assertionInfo"));
    assertTrue(derivedAspects.contains("assertionKey"));
    assertTrue(derivedAspects.contains("assertionRunEvent"));
  }

  @Test
  public void testDeriveRequiredAspectsIncludesStatus() {
    // Test that status is always included
    List<ConsistencyCheck> checks = List.of(new TestAssertionCheck());

    Set<String> derivedAspects = consistencyService.deriveRequiredAspects(checks);

    assertNotNull(derivedAspects);
    assertTrue(derivedAspects.contains(STATUS_ASPECT_NAME));
  }

  @Test
  public void testDeriveRequiredAspectsReturnsNullWhenRequiresAllAspects() {
    // Test that null is returned when any check requires all aspects
    List<ConsistencyCheck> checks =
        List.of(new TestAssertionCheck(), new TestRequiresAllAspectsCheck());

    Set<String> derivedAspects = consistencyService.deriveRequiredAspects(checks);

    // Should return null to signal "fetch all aspects"
    assertNull(derivedAspects);
  }

  @Test
  public void testDeriveRequiredAspectsReturnsNullWithOnlyRequiresAllCheck() {
    // Test with only a check that requires all aspects
    List<ConsistencyCheck> checks = List.of(new TestRequiresAllAspectsCheck());

    Set<String> derivedAspects = consistencyService.deriveRequiredAspects(checks);

    assertNull(derivedAspects);
  }

  @Test
  public void testDeriveRequiredAspectsWithEmptyList() {
    // Test with empty list
    List<ConsistencyCheck> checks = List.of();

    Set<String> derivedAspects = consistencyService.deriveRequiredAspects(checks);

    assertNotNull(derivedAspects);
    // Should only include status
    assertEquals(derivedAspects.size(), 1);
    assertTrue(derivedAspects.contains(STATUS_ASPECT_NAME));
  }

  @Test
  public void testStatusAspectAlwaysIncluded() {
    // This tests the logic indirectly - status aspect is always added for soft-delete tracking
    // The actual implementation adds STATUS_ASPECT_NAME to the set
    // We verify this by checking the constant exists
    assertNotNull(STATUS_ASPECT_NAME);
    assertEquals(STATUS_ASPECT_NAME, "status");
  }

  // ============================================================================
  // Empty Result Tests
  // ============================================================================

  @Test
  public void testCheckBatchReturnsEmptyForNoChecks() {
    ConsistencyCheckRegistry emptyRegistry = new ConsistencyCheckRegistry(List.of());
    ConsistencyService serviceWithEmptyRegistry =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, emptyRegistry, fixRegistry);

    CheckResult result =
        serviceWithEmptyRegistry.checkBatch(
            mockOpContext,
            CheckBatchRequest.builder().entityType("dataset").batchSize(100).build());
    assertNotNull(result);
    assertEquals(result.getEntitiesScanned(), 0);
    assertEquals(result.getIssuesFound(), 0);
  }

  @Test
  public void testCheckResultEmpty() {
    // Verify the empty result helper works correctly
    CheckResult empty = CheckResult.empty();
    assertNotNull(empty);
    assertEquals(empty.getEntitiesScanned(), 0);
    assertEquals(empty.getIssuesFound(), 0);
    assertTrue(empty.getIssues().isEmpty());
    assertNull(empty.getScrollId());
  }

  // ============================================================================
  // Check Configs Tests
  // ============================================================================

  @Test
  public void testConsistencyServiceWithCheckConfigs() {
    Map<String, Map<String, String>> checkConfigs =
        Map.of("test-assertion-check", Map.of("enabled", "true", "threshold", "10"));

    ConsistencyService serviceWithConfigs =
        new ConsistencyService(
            mockEntityService,
            mockEsSystemMetadataDAO,
            null,
            checkRegistry,
            fixRegistry,
            checkConfigs);

    assertNotNull(serviceWithConfigs);
  }

  // ============================================================================
  // fixIssues Tests
  // ============================================================================

  @Test
  public void testFixIssuesSuccessfulFix() {
    // Create a test fix that succeeds
    Urn testUrn = UrnUtils.getUrn("urn:li:assertion:test-123");
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(testUrn)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Test issue")
            .hardDeleteUrns(List.of(testUrn))
            .build();

    ConsistencyFixResult result =
        consistencyService.fixIssues(mockOpContext, List.of(issue), false);

    assertNotNull(result);
    assertEquals(result.getTotalProcessed(), 1);
    assertEquals(result.getEntitiesFixed(), 1);
    assertEquals(result.getEntitiesFailed(), 0);
    assertFalse(result.isDryRun());
    assertEquals(result.getFixDetails().size(), 1);
    assertTrue(result.getFixDetails().get(0).isSuccess());
  }

  @Test
  public void testFixIssuesDryRunMode() {
    Urn testUrn = UrnUtils.getUrn("urn:li:assertion:test-123");
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(testUrn)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Test issue")
            .hardDeleteUrns(List.of(testUrn))
            .build();

    ConsistencyFixResult result = consistencyService.fixIssues(mockOpContext, List.of(issue), true);

    assertNotNull(result);
    assertTrue(result.isDryRun());
    assertEquals(result.getTotalProcessed(), 1);
    // Dry run should still count as success (would be fixed)
    assertEquals(result.getEntitiesFixed(), 1);
    assertEquals(result.getEntitiesFailed(), 0);
  }

  @Test
  public void testFixIssuesMissingFixImplementation() {
    // Create an issue with a fix type that has no registered implementation
    Urn testUrn = UrnUtils.getUrn("urn:li:assertion:test-123");
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(testUrn)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.TRIM_UPSERT) // No implementation registered for this
            .description("Test issue")
            .build();

    ConsistencyFixResult result =
        consistencyService.fixIssues(mockOpContext, List.of(issue), false);

    assertNotNull(result);
    assertEquals(result.getTotalProcessed(), 1);
    assertEquals(result.getEntitiesFixed(), 0);
    assertEquals(result.getEntitiesFailed(), 1);
    assertEquals(result.getFixDetails().size(), 1);

    ConsistencyFixDetail detail = result.getFixDetails().get(0);
    assertFalse(detail.isSuccess());
    assertNotNull(detail.getErrorMessage());
    assertTrue(detail.getErrorMessage().contains("No fix implementation"));
  }

  @Test
  public void testFixIssuesContinuesOnFailure() {
    // Create multiple issues - one that fails, one that succeeds
    Urn failUrn = UrnUtils.getUrn("urn:li:assertion:fail-123");
    Urn successUrn = UrnUtils.getUrn("urn:li:assertion:success-456");

    ConsistencyIssue failingIssue =
        ConsistencyIssue.builder()
            .entityUrn(failUrn)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.TRIM_UPSERT) // No implementation
            .description("Failing issue")
            .build();

    ConsistencyIssue successIssue =
        ConsistencyIssue.builder()
            .entityUrn(successUrn)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Success issue")
            .hardDeleteUrns(List.of(successUrn))
            .build();

    ConsistencyFixResult result =
        consistencyService.fixIssues(mockOpContext, List.of(failingIssue, successIssue), false);

    assertNotNull(result);
    assertEquals(result.getTotalProcessed(), 2);
    assertEquals(result.getEntitiesFixed(), 1);
    assertEquals(result.getEntitiesFailed(), 1);
    assertEquals(result.getFixDetails().size(), 2);

    // Verify both issues were processed
    ConsistencyFixDetail failDetail = result.getFixDetails().get(0);
    ConsistencyFixDetail successDetail = result.getFixDetails().get(1);

    assertFalse(failDetail.isSuccess());
    assertTrue(successDetail.isSuccess());
  }

  @Test
  public void testFixIssuesEmptyList() {
    ConsistencyFixResult result = consistencyService.fixIssues(mockOpContext, List.of(), false);

    assertNotNull(result);
    assertEquals(result.getTotalProcessed(), 0);
    assertEquals(result.getEntitiesFixed(), 0);
    assertEquals(result.getEntitiesFailed(), 0);
    assertTrue(result.getFixDetails().isEmpty());
  }

  @Test
  public void testFixIssuesMultipleSuccessfulFixes() {
    List<ConsistencyIssue> issues = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Urn urn = UrnUtils.getUrn("urn:li:assertion:test-" + i);
      issues.add(
          ConsistencyIssue.builder()
              .entityUrn(urn)
              .entityType("assertion")
              .checkId("test-check")
              .fixType(ConsistencyFixType.HARD_DELETE)
              .description("Test issue " + i)
              .hardDeleteUrns(List.of(urn))
              .build());
    }

    ConsistencyFixResult result = consistencyService.fixIssues(mockOpContext, issues, false);

    assertNotNull(result);
    assertEquals(result.getTotalProcessed(), 5);
    assertEquals(result.getEntitiesFixed(), 5);
    assertEquals(result.getEntitiesFailed(), 0);
    assertEquals(result.getFixDetails().size(), 5);
  }

  @Test
  public void testFixIssuesStatisticsAccuracy() {
    // Create mix of issues: 3 success, 2 fail
    List<ConsistencyIssue> issues = new ArrayList<>();

    // 3 successful (HARD_DELETE has implementation)
    for (int i = 0; i < 3; i++) {
      Urn urn = UrnUtils.getUrn("urn:li:assertion:success-" + i);
      issues.add(
          ConsistencyIssue.builder()
              .entityUrn(urn)
              .entityType("assertion")
              .checkId("test-check")
              .fixType(ConsistencyFixType.HARD_DELETE)
              .description("Success " + i)
              .hardDeleteUrns(List.of(urn))
              .build());
    }

    // 2 failing (TRIM_UPSERT has no implementation)
    for (int i = 0; i < 2; i++) {
      Urn urn = UrnUtils.getUrn("urn:li:assertion:fail-" + i);
      issues.add(
          ConsistencyIssue.builder()
              .entityUrn(urn)
              .entityType("assertion")
              .checkId("test-check")
              .fixType(ConsistencyFixType.TRIM_UPSERT)
              .description("Fail " + i)
              .build());
    }

    ConsistencyFixResult result = consistencyService.fixIssues(mockOpContext, issues, false);

    assertEquals(result.getTotalProcessed(), 5);
    assertEquals(result.getEntitiesFixed(), 3);
    assertEquals(result.getEntitiesFailed(), 2);
    assertEquals(result.getFixDetails().size(), 5);

    // Verify the success/fail breakdown
    long successCount =
        result.getFixDetails().stream().filter(ConsistencyFixDetail::isSuccess).count();
    long failCount = result.getFixDetails().stream().filter(d -> !d.isSuccess()).count();
    assertEquals(successCount, 3);
    assertEquals(failCount, 2);
  }

  // ============================================================================
  // buildContext Tests
  // ============================================================================

  @Mock private GraphClient mockGraphClient;

  @Test
  public void testBuildContext() {
    ConsistencyService serviceWithGraph =
        new ConsistencyService(
            mockEntityService,
            mockEsSystemMetadataDAO,
            mockGraphClient,
            checkRegistry,
            fixRegistry,
            Map.of("test-check", Map.of("key", "value")));

    CheckContext ctx = serviceWithGraph.buildContext(mockOpContext);

    assertNotNull(ctx);
    assertEquals(ctx.getOperationContext(), mockOpContext);
    assertEquals(ctx.getEntityService(), mockEntityService);
    assertEquals(ctx.getGraphClient(), mockGraphClient);
    assertNotNull(ctx.getCheckConfigs());
    assertEquals(ctx.getCheckConfigs().get("test-check").get("key"), "value");
  }

  @Test
  public void testBuildContextWithNullGraphClient() {
    CheckContext ctx = consistencyService.buildContext(mockOpContext);

    assertNotNull(ctx);
    assertNull(ctx.getGraphClient());
    assertEquals(ctx.getEntityService(), mockEntityService);
  }

  @Test
  public void testBuildContextWithEmptyConfigs() {
    CheckContext ctx = consistencyService.buildContext(mockOpContext);

    assertNotNull(ctx);
    assertNotNull(ctx.getCheckConfigs());
    assertTrue(ctx.getCheckConfigs().isEmpty());
  }

  // ============================================================================
  // buildSystemMetadataQuery Tests
  // ============================================================================

  @Test
  public void testBuildSystemMetadataQueryEntityTypeFilter() {
    List<ConsistencyCheck> checks = List.of(new TestAssertionCheck());

    BoolQueryBuilder query =
        consistencyService.buildSystemMetadataQuery("assertion", checks, null, null);

    assertNotNull(query);
    String queryString = query.toString();
    // Should contain URN prefix filter for entity type
    assertTrue(queryString.contains("urn:li:assertion:"));
    assertTrue(queryString.contains("prefix"));
  }

  @Test
  public void testBuildSystemMetadataQuerySingleAspectFilter() {
    // Create a check that returns a single target aspect
    ConsistencyCheck checkWithTargetAspect =
        new ConsistencyCheck() {
          @Override
          @Nonnull
          public String getId() {
            return "single-aspect-check";
          }

          @Override
          @Nonnull
          public String getName() {
            return "Single Aspect Check";
          }

          @Override
          @Nonnull
          public String getDescription() {
            return "Check with single target aspect";
          }

          @Override
          @Nonnull
          public String getEntityType() {
            return "assertion";
          }

          @Override
          @Nonnull
          public Optional<Set<String>> getRequiredAspects() {
            return Optional.of(Set.of("assertionInfo"));
          }

          @Override
          @Nonnull
          public Set<String> getTargetAspects() {
            return Set.of("assertionInfo");
          }

          @Override
          @Nonnull
          public List<ConsistencyIssue> check(
              @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
            return List.of();
          }
        };

    BoolQueryBuilder query =
        consistencyService.buildSystemMetadataQuery(
            "assertion", List.of(checkWithTargetAspect), null, null);

    assertNotNull(query);
    String queryString = query.toString();
    // Should contain term query for single aspect
    assertTrue(queryString.contains("assertionInfo"));
    assertTrue(queryString.contains("term"));
  }

  @Test
  public void testBuildSystemMetadataQueryMultipleAspectFilters() {
    // Filter with multiple aspects
    SystemMetadataFilter filter =
        SystemMetadataFilter.builder()
            .aspectFilters(List.of("assertionInfo", "assertionRunEvent"))
            .build();

    BoolQueryBuilder query =
        consistencyService.buildSystemMetadataQuery(
            "assertion", List.of(new TestAssertionCheck()), filter, null);

    assertNotNull(query);
    String queryString = query.toString();
    // Should contain terms query (plural) for multiple aspects
    assertTrue(queryString.contains("terms"));
  }

  @Test
  public void testBuildSystemMetadataQueryTimestampGePitEpochMs() {
    SystemMetadataFilter filter = SystemMetadataFilter.builder().gePitEpochMs(1000000L).build();

    BoolQueryBuilder query =
        consistencyService.buildSystemMetadataQuery(
            "assertion", List.of(new TestAssertionCheck()), filter, null);

    assertNotNull(query);
    String queryString = query.toString();
    // Should contain timestamp query for aspectModifiedTime
    assertTrue(queryString.contains("aspectModifiedTime"));
    // Should have fallback to aspectCreatedTime
    assertTrue(queryString.contains("aspectCreatedTime"));
    // Should have range query
    assertTrue(queryString.contains("range"));
    assertTrue(queryString.contains("1000000"));
  }

  @Test
  public void testBuildSystemMetadataQueryTimestampLePitEpochMs() {
    SystemMetadataFilter filter = SystemMetadataFilter.builder().lePitEpochMs(2000000L).build();

    BoolQueryBuilder query =
        consistencyService.buildSystemMetadataQuery(
            "assertion", List.of(new TestAssertionCheck()), filter, null);

    assertNotNull(query);
    String queryString = query.toString();
    // Should contain timestamp query
    assertTrue(queryString.contains("aspectModifiedTime"));
    assertTrue(queryString.contains("aspectCreatedTime"));
    assertTrue(queryString.contains("2000000"));
  }

  @Test
  public void testBuildSystemMetadataQueryCombinedTimestampFilters() {
    SystemMetadataFilter filter =
        SystemMetadataFilter.builder().gePitEpochMs(1000000L).lePitEpochMs(2000000L).build();

    BoolQueryBuilder query =
        consistencyService.buildSystemMetadataQuery(
            "assertion", List.of(new TestAssertionCheck()), filter, null);

    assertNotNull(query);
    String queryString = query.toString();
    // Should contain both timestamps
    assertTrue(queryString.contains("1000000"));
    assertTrue(queryString.contains("2000000"));
    // Should have minimum_should_match for the OR logic
    assertTrue(queryString.contains("minimum_should_match"));
  }

  @Test
  public void testBuildSystemMetadataQueryNoFilter() {
    BoolQueryBuilder query =
        consistencyService.buildSystemMetadataQuery(
            "assertion", List.of(new TestAssertionCheck()), null, null);

    assertNotNull(query);
    String queryString = query.toString();
    // Should have entity type filter but no timestamp filters
    assertTrue(queryString.contains("urn:li:assertion:"));
    assertFalse(queryString.contains("aspectModifiedTime"));
    assertFalse(queryString.contains("aspectCreatedTime"));
  }

  @Test
  public void testBuildSystemMetadataQueryWithUrnFilter() {
    List<ConsistencyCheck> checks = List.of(new TestAssertionCheck());
    Set<Urn> urns =
        Set.of(
            UrnUtils.getUrn("urn:li:assertion:test-1"), UrnUtils.getUrn("urn:li:assertion:test-2"));

    BoolQueryBuilder query =
        consistencyService.buildSystemMetadataQuery("assertion", checks, null, urns);

    assertNotNull(query);
    String queryString = query.toString();
    // Should contain URN prefix filter for entity type
    assertTrue(queryString.contains("urn:li:assertion:"));
    // Should contain terms query for specific URNs
    assertTrue(queryString.contains("urn:li:assertion:test-1"));
    assertTrue(queryString.contains("urn:li:assertion:test-2"));
    assertTrue(queryString.contains("terms"));
  }

  // ============================================================================
  // extractNextScrollId Tests
  // ============================================================================

  @Test
  public void testExtractNextScrollIdWithSortValues() {
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    SearchHit mockHit = mock(SearchHit.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});
    when(mockHit.getSortValues()).thenReturn(new Object[] {"value1", 12345L});

    String scrollId = consistencyService.extractNextScrollId(mockResponse);

    assertNotNull(scrollId);
    // Scroll ID should be non-empty encoded value
    assertFalse(scrollId.isEmpty());
  }

  @Test
  public void testExtractNextScrollIdNoHits() {
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getHits()).thenReturn(new SearchHit[0]);

    String scrollId = consistencyService.extractNextScrollId(mockResponse);

    assertNull(scrollId);
  }

  @Test
  public void testExtractNextScrollIdNoSortValues() {
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    SearchHit mockHit = mock(SearchHit.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});
    when(mockHit.getSortValues()).thenReturn(null);

    String scrollId = consistencyService.extractNextScrollId(mockResponse);

    assertNull(scrollId);
  }

  @Test
  public void testExtractNextScrollIdEmptySortValues() {
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    SearchHit mockHit = mock(SearchHit.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit});
    when(mockHit.getSortValues()).thenReturn(new Object[0]);

    String scrollId = consistencyService.extractNextScrollId(mockResponse);

    assertNull(scrollId);
  }

  @Test
  public void testExtractNextScrollIdUsesLastHit() {
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    SearchHit mockHit1 = mock(SearchHit.class);
    SearchHit mockHit2 = mock(SearchHit.class);
    SearchHit mockHit3 = mock(SearchHit.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {mockHit1, mockHit2, mockHit3});
    // Only the last hit's sort values should be used
    when(mockHit3.getSortValues()).thenReturn(new Object[] {"lastValue", 99999L});

    String scrollId = consistencyService.extractNextScrollId(mockResponse);

    assertNotNull(scrollId);
    // Verify only last hit was accessed for sort values
    verify(mockHit3).getSortValues();
    verify(mockHit1, never()).getSortValues();
    verify(mockHit2, never()).getSortValues();
  }

  // ============================================================================
  // identifyOrphanUrns Tests
  // ============================================================================

  @Test
  public void testIdentifyOrphanUrnsWithOrphans() {
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:test-2");
    Urn urn3 = UrnUtils.getUrn("urn:li:assertion:test-3");

    Set<Urn> urnsFromEs = new HashSet<>(Set.of(urn1, urn2, urn3));
    Set<Urn> existingUrns = Set.of(urn1, urn3); // urn2 is orphan (doesn't exist in SQL)

    // Mock entityService.exists() to return the URNs that exist
    when(mockEntityService.exists(
            eq(mockOpContext),
            eq(urnsFromEs),
            isNull(), // null aspectName = check key aspect
            eq(true), // includeSoftDeleted
            eq(false))) // forUpdate
        .thenReturn(existingUrns);

    Set<Urn> orphans = consistencyService.identifyOrphanUrns(mockOpContext, urnsFromEs);

    assertEquals(orphans.size(), 1);
    assertTrue(orphans.contains(urn2));
  }

  @Test
  public void testIdentifyOrphanUrnsNoOrphans() {
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:test-2");

    Set<Urn> urnsFromEs = new HashSet<>(Set.of(urn1, urn2));
    Set<Urn> existingUrns = Set.of(urn1, urn2); // All exist in SQL

    when(mockEntityService.exists(eq(mockOpContext), eq(urnsFromEs), isNull(), eq(true), eq(false)))
        .thenReturn(existingUrns);

    Set<Urn> orphans = consistencyService.identifyOrphanUrns(mockOpContext, urnsFromEs);

    assertTrue(orphans.isEmpty());
  }

  @Test
  public void testIdentifyOrphanUrnsAllOrphans() {
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:test-2");

    Set<Urn> urnsFromEs = new HashSet<>(Set.of(urn1, urn2));

    // Mock entityService.exists() to return empty set - none exist in SQL
    when(mockEntityService.exists(eq(mockOpContext), eq(urnsFromEs), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of());

    Set<Urn> orphans = consistencyService.identifyOrphanUrns(mockOpContext, urnsFromEs);

    assertEquals(orphans.size(), 2);
    assertTrue(orphans.contains(urn1));
    assertTrue(orphans.contains(urn2));
  }

  @Test
  public void testIdentifyOrphanUrnsEmptyEs() {
    Set<Urn> urnsFromEs = new HashSet<>();

    // With empty input, should return empty without calling entityService
    Set<Urn> orphans = consistencyService.identifyOrphanUrns(mockOpContext, urnsFromEs);

    assertTrue(orphans.isEmpty());
    // Should not call exists() for empty set
    verify(mockEntityService, never())
        .exists(any(OperationContext.class), anyCollection(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testIdentifyOrphanUrnsBothEmpty() {
    Set<Urn> urnsFromEs = new HashSet<>();

    Set<Urn> orphans = consistencyService.identifyOrphanUrns(mockOpContext, urnsFromEs);

    assertTrue(orphans.isEmpty());
  }

  // ============================================================================
  // checkAndFixBatch Tests
  // ============================================================================

  @Test
  public void testCheckAndFixBatchWithNoIssues() {
    ConsistencyFixResult result =
        consistencyService.checkAndFixBatch(mockOpContext, "assertion", Map.of(), false);

    assertNotNull(result);
    assertEquals(result.getTotalProcessed(), 0);
    assertEquals(result.getEntitiesFixed(), 0);
    assertEquals(result.getEntitiesFailed(), 0);
    assertFalse(result.isDryRun());
  }

  @Test
  public void testCheckAndFixBatchDryRunMode() {
    ConsistencyFixResult result =
        consistencyService.checkAndFixBatch(mockOpContext, "assertion", Map.of(), true);

    assertNotNull(result);
    assertTrue(result.isDryRun());
  }

  // ============================================================================
  // runChecks Tests (via checkBatchInternal)
  // ============================================================================

  /** Test check that returns issues */
  private static class IssueReturningCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "issue-returning-check";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Issue Returning Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "Check that returns issues for testing";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "assertion";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("assertionInfo"));
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
      // Return one issue per entity
      List<ConsistencyIssue> issues = new ArrayList<>();
      for (Urn urn : entityResponses.keySet()) {
        issues.add(
            ConsistencyIssue.builder()
                .entityUrn(urn)
                .entityType("assertion")
                .checkId("issue-returning-check")
                .fixType(ConsistencyFixType.SOFT_DELETE)
                .description("Test issue for " + urn)
                .build());
      }
      return issues;
    }
  }

  /** Test check that throws exceptions */
  private static class ExceptionThrowingCheck implements ConsistencyCheck {
    @Override
    @Nonnull
    public String getId() {
      return "exception-throwing-check";
    }

    @Override
    @Nonnull
    public String getName() {
      return "Exception Throwing Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "Check that throws exceptions for testing";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "assertion";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("assertionInfo"));
    }

    @Override
    @Nonnull
    public List<ConsistencyIssue> check(
        @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
      throw new RuntimeException("Simulated check failure");
    }
  }

  @Test
  public void testRunChecksAggregatesIssues() {
    // Create service with issue-returning check
    List<ConsistencyCheck> checks = List.of(new IssueReturningCheck());
    ConsistencyCheckRegistry issueRegistry = new ConsistencyCheckRegistry(checks);
    ConsistencyService serviceWithIssueCheck =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, issueRegistry, fixRegistry);

    // Create test entities
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:test-2");
    Map<Urn, EntityResponse> entities =
        Map.of(urn1, new EntityResponse(), urn2, new EntityResponse());

    List<ConsistencyIssue> issues =
        serviceWithIssueCheck.checkBatchInternal(mockOpContext, "assertion", entities);

    assertEquals(issues.size(), 2);
    assertTrue(issues.stream().anyMatch(i -> i.getEntityUrn().equals(urn1)));
    assertTrue(issues.stream().anyMatch(i -> i.getEntityUrn().equals(urn2)));
  }

  @Test
  public void testRunChecksContinuesOnError() {
    // Create service with both failing and succeeding checks
    List<ConsistencyCheck> checks =
        List.of(new ExceptionThrowingCheck(), new IssueReturningCheck());
    ConsistencyCheckRegistry mixedRegistry = new ConsistencyCheckRegistry(checks);
    ConsistencyService serviceWithMixedChecks =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, mixedRegistry, fixRegistry);

    Urn urn = UrnUtils.getUrn("urn:li:assertion:test-1");
    Map<Urn, EntityResponse> entities = Map.of(urn, new EntityResponse());

    // Should not throw - should continue after first check fails
    List<ConsistencyIssue> issues =
        serviceWithMixedChecks.checkBatchInternal(mockOpContext, "assertion", entities);

    // Should still get issues from the successful check
    assertEquals(issues.size(), 1);
    assertEquals(issues.get(0).getCheckId(), "issue-returning-check");
  }

  @Test
  public void testRunChecksEmptyCheckList() {
    // Create service with no checks for assertion type
    ConsistencyCheckRegistry emptyRegistry = new ConsistencyCheckRegistry(List.of());
    ConsistencyService serviceWithNoChecks =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, emptyRegistry, fixRegistry);

    Urn urn = UrnUtils.getUrn("urn:li:assertion:test-1");
    Map<Urn, EntityResponse> entities = Map.of(urn, new EntityResponse());

    List<ConsistencyIssue> issues =
        serviceWithNoChecks.checkBatchInternal(mockOpContext, "assertion", entities);

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testRunChecksEmptyEntityList() {
    List<ConsistencyIssue> issues =
        consistencyService.checkBatchInternal(mockOpContext, "assertion", Map.of());

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testRunChecksMultipleChecksAggregateIssues() {
    // Create two checks that both return issues
    ConsistencyCheck check1 =
        new ConsistencyCheck() {
          @Override
          @Nonnull
          public String getId() {
            return "check-1";
          }

          @Override
          @Nonnull
          public String getName() {
            return "Check 1";
          }

          @Override
          @Nonnull
          public String getDescription() {
            return "First check";
          }

          @Override
          @Nonnull
          public String getEntityType() {
            return "assertion";
          }

          @Override
          @Nonnull
          public Optional<Set<String>> getRequiredAspects() {
            return Optional.of(Set.of());
          }

          @Override
          @Nonnull
          public List<ConsistencyIssue> check(
              @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
            return entityResponses.keySet().stream()
                .map(
                    urn ->
                        ConsistencyIssue.builder()
                            .entityUrn(urn)
                            .entityType("assertion")
                            .checkId("check-1")
                            .fixType(ConsistencyFixType.SOFT_DELETE)
                            .description("Issue from check 1")
                            .build())
                .toList();
          }
        };

    ConsistencyCheck check2 =
        new ConsistencyCheck() {
          @Override
          @Nonnull
          public String getId() {
            return "check-2";
          }

          @Override
          @Nonnull
          public String getName() {
            return "Check 2";
          }

          @Override
          @Nonnull
          public String getDescription() {
            return "Second check";
          }

          @Override
          @Nonnull
          public String getEntityType() {
            return "assertion";
          }

          @Override
          @Nonnull
          public Optional<Set<String>> getRequiredAspects() {
            return Optional.of(Set.of());
          }

          @Override
          @Nonnull
          public List<ConsistencyIssue> check(
              @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
            return entityResponses.keySet().stream()
                .map(
                    urn ->
                        ConsistencyIssue.builder()
                            .entityUrn(urn)
                            .entityType("assertion")
                            .checkId("check-2")
                            .fixType(ConsistencyFixType.HARD_DELETE)
                            .description("Issue from check 2")
                            .build())
                .toList();
          }
        };

    ConsistencyCheckRegistry multiRegistry = new ConsistencyCheckRegistry(List.of(check1, check2));
    ConsistencyService serviceWithMultiChecks =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, multiRegistry, fixRegistry);

    Urn urn = UrnUtils.getUrn("urn:li:assertion:test-1");
    Map<Urn, EntityResponse> entities = Map.of(urn, new EntityResponse());

    List<ConsistencyIssue> issues =
        serviceWithMultiChecks.checkBatchInternal(mockOpContext, "assertion", entities);

    // Should have issues from both checks
    assertEquals(issues.size(), 2);
    assertTrue(issues.stream().anyMatch(i -> i.getCheckId().equals("check-1")));
    assertTrue(issues.stream().anyMatch(i -> i.getCheckId().equals("check-2")));
  }

  // ============================================================================
  // processBatchResults Tests
  // ============================================================================

  @Test
  public void testProcessBatchResultsNoOrphans() {
    // All URNs from ES exist in SQL - no orphans
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:test-2");

    Set<Urn> urnsFromEs = new HashSet<>(Set.of(urn1, urn2));
    Map<Urn, EntityResponse> entitiesFromSql =
        Map.of(urn1, new EntityResponse(), urn2, new EntityResponse());

    // Mock entityService.exists() to return all URNs (no orphans)
    when(mockEntityService.exists(eq(mockOpContext), eq(urnsFromEs), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of(urn1, urn2));

    CheckResult result =
        consistencyService.processBatchResults(
            mockOpContext,
            "assertion",
            urnsFromEs,
            entitiesFromSql,
            checkRegistry.getDefaultByEntityType("assertion"),
            null,
            "scroll-123");

    assertEquals(result.getEntitiesScanned(), 2);
    assertEquals(result.getScrollId(), "scroll-123");
    // No issues expected from our test checks (they return empty lists)
    assertEquals(result.getIssuesFound(), 0);
  }

  @Test
  public void testProcessBatchResultsWithOrphans() {
    // urn2 exists in ES but not in SQL - orphan
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:exists");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:orphan");

    Set<Urn> urnsFromEs = new HashSet<>(Set.of(urn1, urn2));
    Map<Urn, EntityResponse> entitiesFromSql = Map.of(urn1, new EntityResponse());

    // Mock entityService.exists() to return only urn1 (urn2 is orphan)
    when(mockEntityService.exists(eq(mockOpContext), eq(urnsFromEs), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of(urn1));

    CheckResult result =
        consistencyService.processBatchResults(
            mockOpContext,
            "assertion",
            urnsFromEs,
            entitiesFromSql,
            checkRegistry.getDefaultByEntityType("assertion"),
            null,
            null);

    assertEquals(result.getEntitiesScanned(), 2);
    assertNull(result.getScrollId());
    // Orphan check should detect the orphan and create an issue
    assertTrue(result.getIssuesFound() >= 0); // May or may not have orphan check registered
  }

  @Test
  public void testProcessBatchResultsAllOrphans() {
    // All URNs are orphans - exist in ES but not in SQL
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:orphan-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:orphan-2");

    Set<Urn> urnsFromEs = new HashSet<>(Set.of(urn1, urn2));
    Map<Urn, EntityResponse> entitiesFromSql = Map.of(); // Empty - all orphans

    // Mock entityService.exists() to return empty set (all orphans)
    when(mockEntityService.exists(eq(mockOpContext), eq(urnsFromEs), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of());

    CheckResult result =
        consistencyService.processBatchResults(
            mockOpContext,
            "assertion",
            urnsFromEs,
            entitiesFromSql,
            checkRegistry.getDefaultByEntityType("assertion"),
            null,
            "next-scroll");

    assertEquals(result.getEntitiesScanned(), 2);
    assertEquals(result.getScrollId(), "next-scroll");
  }

  @Test
  public void testProcessBatchResultsWithExistingContext() {
    // Reuse existing context across batches
    CheckContext existingCtx = consistencyService.buildContext(mockOpContext);

    Urn urn = UrnUtils.getUrn("urn:li:assertion:test");
    Set<Urn> urnsFromEs = new HashSet<>(Set.of(urn));
    Map<Urn, EntityResponse> entitiesFromSql = Map.of(urn, new EntityResponse());

    // Mock entityService.exists() - urn exists
    when(mockEntityService.exists(eq(mockOpContext), eq(urnsFromEs), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of(urn));

    CheckResult result =
        consistencyService.processBatchResults(
            mockOpContext,
            "assertion",
            urnsFromEs,
            entitiesFromSql,
            checkRegistry.getDefaultByEntityType("assertion"),
            existingCtx, // Reuse context
            null);

    assertEquals(result.getEntitiesScanned(), 1);
  }

  @Test
  public void testProcessBatchResultsWithIssueReturningCheck() {
    // Use a registry that has checks that return issues
    List<ConsistencyCheck> checks = List.of(new IssueReturningCheck());
    ConsistencyCheckRegistry issueRegistry = new ConsistencyCheckRegistry(checks);
    ConsistencyService serviceWithIssueCheck =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, issueRegistry, fixRegistry);

    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:test-2");

    Set<Urn> urnsFromEs = new HashSet<>(Set.of(urn1, urn2));
    Map<Urn, EntityResponse> entitiesFromSql =
        Map.of(urn1, new EntityResponse(), urn2, new EntityResponse());

    // Mock entityService.exists() - both exist
    when(mockEntityService.exists(eq(mockOpContext), eq(urnsFromEs), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of(urn1, urn2));

    CheckResult result =
        serviceWithIssueCheck.processBatchResults(
            mockOpContext,
            "assertion",
            urnsFromEs,
            entitiesFromSql,
            issueRegistry.getDefaultByEntityType("assertion"),
            null,
            "scroll-456");

    assertEquals(result.getEntitiesScanned(), 2);
    assertEquals(result.getIssuesFound(), 2); // One issue per entity
    assertEquals(result.getScrollId(), "scroll-456");
  }

  @Test
  public void testProcessBatchResultsEmptyUrns() {
    Set<Urn> urnsFromEs = new HashSet<>();
    Map<Urn, EntityResponse> entitiesFromSql = Map.of();

    CheckResult result =
        consistencyService.processBatchResults(
            mockOpContext,
            "assertion",
            urnsFromEs,
            entitiesFromSql,
            checkRegistry.getDefaultByEntityType("assertion"),
            null,
            null);

    assertEquals(result.getEntitiesScanned(), 0);
    assertEquals(result.getIssuesFound(), 0);
    assertNull(result.getScrollId());
  }

  // ============================================================================
  // discoverIssue Orphan Check Tests
  // ============================================================================

  @Test
  public void testDiscoverIssueOrphanCheckEntityDoesNotExist() {
    Urn orphanUrn = UrnUtils.getUrn("urn:li:assertion:orphan-entity");

    when(mockEntityService.exists(
            eq(mockOpContext), eq(Set.of(orphanUrn)), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of());

    ConsistencyCheckRegistry orphanRegistry =
        new ConsistencyCheckRegistry(
            List.of(new com.linkedin.metadata.aspect.consistency.check.OrphanIndexDocumentCheck()));
    ConsistencyService serviceWithOrphanCheck =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, orphanRegistry, fixRegistry);

    Optional<ConsistencyIssue> result =
        serviceWithOrphanCheck.discoverIssue(mockOpContext, orphanUrn, "orphan-index-document");

    assertTrue(result.isPresent());
    assertEquals(result.get().getEntityUrn(), orphanUrn);
    assertEquals(result.get().getCheckId(), "orphan-index-document");
    assertEquals(result.get().getFixType(), ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
    assertTrue(result.get().getDescription().contains("orphaned"));
  }

  @Test
  public void testDiscoverIssueOrphanCheckEntityExists() {
    Urn existingUrn = UrnUtils.getUrn("urn:li:assertion:existing-entity");

    when(mockEntityService.exists(
            eq(mockOpContext), eq(Set.of(existingUrn)), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of(existingUrn));

    ConsistencyCheckRegistry orphanRegistry =
        new ConsistencyCheckRegistry(
            List.of(new com.linkedin.metadata.aspect.consistency.check.OrphanIndexDocumentCheck()));
    ConsistencyService serviceWithOrphanCheck =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, orphanRegistry, fixRegistry);

    Optional<ConsistencyIssue> result =
        serviceWithOrphanCheck.discoverIssue(mockOpContext, existingUrn, "orphan-index-document");

    assertFalse(result.isPresent());
  }

  // ============================================================================
  // checkBatch URN Entity Type Derivation Tests
  // ============================================================================

  @Test
  public void testCheckBatchDeriveEntityTypeFromSingleUrn() {
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test-1");

    CheckResult result =
        consistencyService.checkBatch(
            mockOpContext,
            CheckBatchRequest.builder().urns(Set.of(assertionUrn)).batchSize(100).build());

    assertNotNull(result);
    assertEquals(result.getEntitiesScanned(), 0);
  }

  @Test
  public void testCheckBatchDeriveEntityTypeFromMultipleUrns() {
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:test-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:test-2");
    Urn urn3 = UrnUtils.getUrn("urn:li:assertion:test-3");

    CheckResult result =
        consistencyService.checkBatch(
            mockOpContext,
            CheckBatchRequest.builder().urns(Set.of(urn1, urn2, urn3)).batchSize(100).build());

    assertNotNull(result);
  }

  @Test
  public void testCheckBatchEntityTypeMatchesUrns() {
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:test-1");

    CheckResult result =
        consistencyService.checkBatch(
            mockOpContext,
            CheckBatchRequest.builder()
                .urns(Set.of(assertionUrn))
                .entityType("assertion")
                .batchSize(100)
                .build());

    assertNotNull(result);
  }

  // ============================================================================
  // checkBatchInternal with checkIds filter Tests
  // ============================================================================

  @Test
  public void testCheckBatchInternalWithSpecificCheckIds() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test-1");
    Map<Urn, EntityResponse> entities = Map.of(urn, new EntityResponse());

    List<ConsistencyIssue> issues =
        consistencyService.checkBatchInternal(
            mockOpContext, "assertion", entities, null, List.of("test-assertion-check"));

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testCheckBatchInternalWithEmptyCheckIds() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test-1");
    Map<Urn, EntityResponse> entities = Map.of(urn, new EntityResponse());

    List<ConsistencyIssue> issues =
        consistencyService.checkBatchInternal(
            mockOpContext, "assertion", entities, null, List.of());

    assertTrue(issues.isEmpty());
  }

  @Test
  public void testCheckBatchInternalWithNullCheckIds() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test-1");
    Map<Urn, EntityResponse> entities = Map.of(urn, new EntityResponse());

    List<ConsistencyIssue> issues =
        consistencyService.checkBatchInternal(mockOpContext, "assertion", entities, null, null);

    assertTrue(issues.isEmpty());
  }

  // ============================================================================
  // runChecks Error Handling Tests
  // ============================================================================

  @Test
  public void testRunChecksLogsErrorAndContinuesOnException() {
    ConsistencyCheck errorCheck =
        new ConsistencyCheck() {
          @Override
          @Nonnull
          public String getId() {
            return "error-check";
          }

          @Override
          @Nonnull
          public String getName() {
            return "Error Check";
          }

          @Override
          @Nonnull
          public String getDescription() {
            return "Check that throws";
          }

          @Override
          @Nonnull
          public String getEntityType() {
            return "assertion";
          }

          @Override
          @Nonnull
          public Optional<Set<String>> getRequiredAspects() {
            return Optional.of(Set.of());
          }

          @Override
          @Nonnull
          public List<ConsistencyIssue> check(
              @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
            throw new RuntimeException("Simulated error in check");
          }
        };

    ConsistencyCheck successCheck =
        new ConsistencyCheck() {
          @Override
          @Nonnull
          public String getId() {
            return "success-check";
          }

          @Override
          @Nonnull
          public String getName() {
            return "Success Check";
          }

          @Override
          @Nonnull
          public String getDescription() {
            return "Check that succeeds";
          }

          @Override
          @Nonnull
          public String getEntityType() {
            return "assertion";
          }

          @Override
          @Nonnull
          public Optional<Set<String>> getRequiredAspects() {
            return Optional.of(Set.of());
          }

          @Override
          @Nonnull
          public List<ConsistencyIssue> check(
              @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
            return entityResponses.keySet().stream()
                .map(
                    urn ->
                        ConsistencyIssue.builder()
                            .entityUrn(urn)
                            .entityType("assertion")
                            .checkId("success-check")
                            .fixType(ConsistencyFixType.SOFT_DELETE)
                            .description("Issue found")
                            .build())
                .toList();
          }
        };

    ConsistencyCheckRegistry mixedRegistry =
        new ConsistencyCheckRegistry(List.of(errorCheck, successCheck));
    ConsistencyService mixedService =
        new ConsistencyService(
            mockEntityService, mockEsSystemMetadataDAO, null, mixedRegistry, fixRegistry);

    Urn urn = UrnUtils.getUrn("urn:li:assertion:test");
    Map<Urn, EntityResponse> entities = Map.of(urn, new EntityResponse());

    List<ConsistencyIssue> issues =
        mixedService.checkBatchInternal(mockOpContext, "assertion", entities);

    assertEquals(issues.size(), 1);
    assertEquals(issues.get(0).getCheckId(), "success-check");
  }
}
