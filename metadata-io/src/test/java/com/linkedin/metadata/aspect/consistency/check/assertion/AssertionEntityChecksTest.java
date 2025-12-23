package com.linkedin.metadata.aspect.consistency.check.assertion;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
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
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Aggregate tests for assertion entity consistency checks.
 *
 * <p>These tests run ALL assertion checks together to validate aggregate behavior. When one check
 * identifies an issue, other checks should correctly skip or return empty for that entity.
 *
 * <p>Tests are organized by entity state/scenario rather than by individual check.
 */
public class AssertionEntityChecksTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private GraphClient mockGraphClient;

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

    // Create all assertion checks
    List<ConsistencyCheck> checks =
        List.of(
            new AssertionEntityUrnMissingCheck(testEntityRegistry),
            new AssertionEntityTypeInvalidCheck(testEntityRegistry),
            new AssertionEntityNotFoundCheck(testEntityRegistry),
            new AssertionEntitySoftDeletedCheck(testEntityRegistry),
            new AssertionMonitorMissingCheck(testEntityRegistry),
            new AssertionTypeMismatchCheck(testEntityRegistry));

    registry = new ConsistencyCheckRegistry(checks);
    helper = new AggregateCheckTestHelper(registry);

    // Default mock behavior: entity exists and is NOT soft-deleted
    // exists(ctx, urn, true) = true (exists including soft-deleted)
    // exists(ctx, urn, false) = true (exists when excluding soft-deleted, meaning NOT soft-deleted)
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(true)))
        .thenReturn(true);
    when(mockEntityService.exists(any(OperationContext.class), any(Urn.class), eq(false)))
        .thenReturn(true);

    // Default mock behavior: no monitor exists (empty relationships)
    EntityRelationships emptyRelationships = new EntityRelationships();
    emptyRelationships.setRelationships(new EntityRelationshipArray());
    when(mockGraphClient.getRelatedEntities(any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(emptyRelationships);
  }

  // ============================================================================
  // Aggregate Scenario Tests
  // ============================================================================

  /**
   * Happy path test: Valid assertion should have no issues from any check.
   *
   * <p>This validates the happy path for all assertion checks:
   *
   * <ul>
   *   <li>AssertionEntityUrnMissingCheck - entityUrn is present
   *   <li>AssertionEntityTypeInvalidCheck - entityUrn is valid type (dataset)
   *   <li>AssertionEntityNotFoundCheck - entity exists
   *   <li>AssertionEntitySoftDeletedCheck - entity is not soft-deleted
   *   <li>AssertionMonitorMissingCheck - external assertion doesn't need monitor
   *   <li>AssertionTypeMismatchCheck - type has matching sub-property populated
   * </ul>
   */
  @Test
  public void testValidAssertion_NoIssues() {
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:valid123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockEntityService.exists(eq(testOpContext), eq(entityUrn), eq(true))).thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(entityUrn), eq(false))).thenReturn(true);

    EntityResponse response = createExternalAssertionResponse(assertionUrn, entityUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    helper.assertTotalIssueCount(issues, 0);
  }

  @Test
  public void testAssertionMissingEntityUrn_Derivable_UpsertIssue() {
    // Scenario: Assertion missing entityUrn but can derive from freshnessAssertion.entity
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:missingUrn123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    EntityResponse response =
        createAssertionWithDerivableEntityUrn(assertionUrn, entityUrn, AssertionType.FRESHNESS);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    // Only assertion-entity-urn-missing should fire with UPSERT
    helper.assertTotalIssueCount(issues, 1);
    helper.assertIssueFromCheck(
        issues, "assertion-entity-urn-missing", assertionUrn, ConsistencyFixType.UPSERT);

    // Verify other checks did not fire
    helper.assertNoIssueFromCheck(issues, "assertion-entity-type-invalid");
    helper.assertNoIssueFromCheck(issues, "assertion-entity-not-found");
    helper.assertNoIssueFromCheck(issues, "assertion-entity-soft-deleted");
  }

  @Test
  public void testAssertionMissingEntityUrn_NotDerivable_SoftDeleteIssue() {
    // Scenario: Assertion missing entityUrn and cannot derive -> soft delete
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:cannotDerive123");

    EntityResponse response = createAssertionWithoutDerivableEntityUrn(assertionUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    // Only assertion-entity-urn-missing should fire with SOFT_DELETE
    helper.assertTotalIssueCount(issues, 1);
    helper.assertIssueFromCheck(
        issues, "assertion-entity-urn-missing", assertionUrn, ConsistencyFixType.SOFT_DELETE);
  }

  @Test
  public void testAssertionInvalidEntityType_SoftDeleteIssue() {
    // Scenario: Assertion references an invalid entity type
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:invalidType123");
    // corpUser is not a valid entity type for assertions
    Urn invalidEntityUrn = UrnUtils.getUrn("urn:li:corpuser:testUser");

    EntityResponse response =
        createAssertionResponse(assertionUrn, invalidEntityUrn, AssertionType.FRESHNESS);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    // assertion-entity-type-invalid should fire
    helper.assertIssueFromCheck(
        issues, "assertion-entity-type-invalid", assertionUrn, ConsistencyFixType.SOFT_DELETE);

    // Other checks may skip due to invalid entity type
    helper.assertNoIssueFromCheck(issues, "assertion-entity-urn-missing");
  }

  @Test
  public void testAssertionEntityNotFound_HardDeleteIssue() {
    // Scenario: Assertion references an entity that does not exist
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:entityNotFound123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,nonexistent,PROD)");

    // Entity does not exist
    when(mockEntityService.exists(eq(testOpContext), eq(entityUrn), eq(true))).thenReturn(false);

    EntityResponse response =
        createAssertionResponse(assertionUrn, entityUrn, AssertionType.FRESHNESS);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    // assertion-entity-not-found should fire with HARD_DELETE
    helper.assertIssueFromCheck(
        issues, "assertion-entity-not-found", assertionUrn, ConsistencyFixType.HARD_DELETE);

    // Other checks should skip since entity doesn't exist
    helper.assertNoIssueFromCheck(issues, "assertion-entity-urn-missing");
    helper.assertNoIssueFromCheck(issues, "assertion-entity-type-invalid");
  }

  @Test
  public void testAssertionEntitySoftDeleted_SoftDeleteIssue() {
    // Scenario: Assertion references a soft-deleted entity
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:entitySoftDeleted123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,deleted,PROD)");

    // Entity exists but is soft-deleted
    // exists(ctx, urn, true) = true (exists including soft-deleted)
    // exists(ctx, urn, false) = false (doesn't exist when excluding soft-deleted, meaning it IS
    // soft-deleted)
    when(mockEntityService.exists(eq(testOpContext), eq(entityUrn), eq(true))).thenReturn(true);
    when(mockEntityService.exists(eq(testOpContext), eq(entityUrn), eq(false))).thenReturn(false);

    CheckContext context = buildContext();

    EntityResponse response =
        createAssertionResponse(assertionUrn, entityUrn, AssertionType.FRESHNESS);

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    // assertion-entity-soft-deleted should fire
    helper.assertIssueFromCheck(
        issues, "assertion-entity-soft-deleted", assertionUrn, ConsistencyFixType.SOFT_DELETE);
  }

  @Test
  public void testNativeAssertionMissingMonitor_WithConfig_CreateIssue() {
    // Scenario: Native assertion without a monitor, with cron config provided
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:nativeNoMonitor123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockEntityService.exists(eq(testOpContext), eq(entityUrn), eq(true))).thenReturn(true);

    // No monitor exists (already default mock behavior)

    EntityResponse response = createNativeAssertionResponse(assertionUrn, entityUrn);

    // Create context with cron configuration for assertion-monitor-missing check
    Map<String, String> checkConfig = new HashMap<>();
    checkConfig.put(AssertionMonitorMissingCheck.CONFIG_CRON_SCHEDULE, "0 0 0 * * ?");
    checkConfig.put(AssertionMonitorMissingCheck.CONFIG_CRON_TIMEZONE, "UTC");

    CheckContext context =
        CheckContext.builder()
            .operationContext(testOpContext)
            .entityService(mockEntityService)
            .graphClient(mockGraphClient)
            .checkConfigs(Map.of("assertion-monitor-missing", checkConfig))
            .build();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    // assertion-monitor-missing should fire with CREATE
    helper.assertIssueFromCheck(
        issues, "assertion-monitor-missing", assertionUrn, ConsistencyFixType.CREATE);

    // Other checks should not fire for this valid assertion
    helper.assertNoIssueFromCheck(issues, "assertion-entity-urn-missing");
    helper.assertNoIssueFromCheck(issues, "assertion-entity-type-invalid");
    helper.assertNoIssueFromCheck(issues, "assertion-entity-not-found");
  }

  @Test
  public void testNativeAssertionWithMonitor_NoIssues() {
    // Scenario: Native assertion that already has a monitor
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:hasMonitor123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Urn monitorUrn =
        UrnUtils.getUrn(
            "urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD),hasMonitor123)");

    when(mockEntityService.exists(eq(testOpContext), eq(entityUrn), eq(true))).thenReturn(true);

    // Monitor exists
    EntityRelationships relationships = new EntityRelationships();
    EntityRelationshipArray relationshipArray = new EntityRelationshipArray();
    EntityRelationship relationship = new EntityRelationship();
    relationship.setEntity(monitorUrn);
    relationship.setType(EVALUATES_RELATIONSHIP_NAME);
    relationshipArray.add(relationship);
    relationships.setRelationships(relationshipArray);

    when(mockGraphClient.getRelatedEntities(
            eq(assertionUrn.toString()), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(relationships);

    EntityResponse response = createNativeAssertionResponse(assertionUrn, entityUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    // No issues - assertion is valid and has a monitor
    helper.assertTotalIssueCount(issues, 0);
  }

  @Test
  public void testExternalAssertion_NoMonitorNeeded() {
    // Scenario: External assertion doesn't need a monitor
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:external123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockEntityService.exists(eq(testOpContext), eq(entityUrn), eq(true))).thenReturn(true);

    EntityResponse response = createExternalAssertionResponse(assertionUrn, entityUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    // No issues for external assertion
    helper.assertTotalIssueCount(issues, 0);
  }

  @Test
  public void testMultipleAssertions_MixedResults() {
    // Scenario: Multiple assertions with different issues
    Urn validAssertionUrn = UrnUtils.getUrn("urn:li:assertion:valid");
    Urn missingUrnAssertionUrn = UrnUtils.getUrn("urn:li:assertion:missingUrn");
    Urn notFoundAssertionUrn = UrnUtils.getUrn("urn:li:assertion:notFound");

    Urn validEntityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,valid,PROD)");
    Urn derivableEntityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,derivable,PROD)");
    Urn nonExistentEntityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,nonexistent,PROD)");

    // Valid entity exists
    when(mockEntityService.exists(eq(testOpContext), eq(validEntityUrn), eq(true)))
        .thenReturn(true);
    // Non-existent entity does not exist
    when(mockEntityService.exists(eq(testOpContext), eq(nonExistentEntityUrn), eq(true)))
        .thenReturn(false);

    Map<Urn, EntityResponse> responses = new HashMap<>();
    responses.put(
        validAssertionUrn, createExternalAssertionResponse(validAssertionUrn, validEntityUrn));
    responses.put(
        missingUrnAssertionUrn,
        createAssertionWithDerivableEntityUrn(
            missingUrnAssertionUrn, derivableEntityUrn, AssertionType.FRESHNESS));
    responses.put(
        notFoundAssertionUrn,
        createAssertionResponse(
            notFoundAssertionUrn, nonExistentEntityUrn, AssertionType.FRESHNESS));

    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(ASSERTION_ENTITY_NAME, context, responses);

    // Valid assertion: no issues
    // Missing URN assertion: UPSERT from assertion-entity-urn-missing
    // Not found assertion: HARD_DELETE from assertion-entity-not-found
    helper.assertTotalIssueCount(issues, 2);
    helper.assertIssueFromCheck(
        issues, "assertion-entity-urn-missing", missingUrnAssertionUrn, ConsistencyFixType.UPSERT);
    helper.assertIssueFromCheck(
        issues, "assertion-entity-not-found", notFoundAssertionUrn, ConsistencyFixType.HARD_DELETE);
  }

  @Test
  public void testAssertionTypeMismatch_SoftDeleteIssue() {
    // Scenario: Assertion has type FRESHNESS but freshnessAssertion property is empty
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:typeMismatch123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockEntityService.exists(eq(testOpContext), eq(entityUrn), eq(true))).thenReturn(true);

    // Create assertion with FRESHNESS type but empty freshnessAssertion
    EntityResponse response =
        createAssertionWithTypeMismatch(assertionUrn, entityUrn, AssertionType.FRESHNESS);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues =
        helper.runChecksForEntityType(
            ASSERTION_ENTITY_NAME, context, Map.of(assertionUrn, response));

    // assertion-type-mismatch should fire with SOFT_DELETE
    helper.assertIssueFromCheck(
        issues, "assertion-type-mismatch", assertionUrn, ConsistencyFixType.SOFT_DELETE);

    ConsistencyIssue issue = helper.getIssue(issues, "assertion-type-mismatch", assertionUrn);
    assertNotNull(issue);
    assertTrue(issue.getDescription().contains("FRESHNESS"));
    assertTrue(issue.getDescription().contains("freshnessAssertion"));
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private CheckContext buildContext() {
    return CheckContext.builder()
        .operationContext(testOpContext)
        .entityService(mockEntityService)
        .graphClient(mockGraphClient)
        .build();
  }

  private EntityResponse createAssertionResponse(
      Urn assertionUrn, Urn entityUrn, AssertionType type) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(type);
    assertionInfo.setEntityUrn(entityUrn);
    if (type == AssertionType.FRESHNESS) {
      FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
      freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
      freshnessInfo.setEntity(entityUrn);
      assertionInfo.setFreshnessAssertion(freshnessInfo);
    }
    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createExternalAssertionResponse(Urn assertionUrn, Urn entityUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATASET);
    assertionInfo.setEntityUrn(entityUrn);
    DatasetAssertionInfo datasetInfo = new DatasetAssertionInfo();
    datasetInfo.setDataset(entityUrn);
    datasetInfo.setScope(DatasetAssertionScope.DATASET_ROWS);
    datasetInfo.setOperator(AssertionStdOperator.NOT_NULL);
    assertionInfo.setDatasetAssertion(datasetInfo);
    AssertionSource source = new AssertionSource();
    source.setType(AssertionSourceType.EXTERNAL);
    assertionInfo.setSource(source);
    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createNativeAssertionResponse(Urn assertionUrn, Urn entityUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    assertionInfo.setEntityUrn(entityUrn);
    FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
    freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    freshnessInfo.setEntity(entityUrn);
    assertionInfo.setFreshnessAssertion(freshnessInfo);
    AssertionSource source = new AssertionSource();
    source.setType(AssertionSourceType.NATIVE);
    assertionInfo.setSource(source);
    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createAssertionWithDerivableEntityUrn(
      Urn assertionUrn, Urn derivableEntityUrn, AssertionType type) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(type);
    // NO entityUrn set - this is what we're testing

    if (type == AssertionType.FRESHNESS) {
      FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
      freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
      freshnessInfo.setEntity(derivableEntityUrn);
      assertionInfo.setFreshnessAssertion(freshnessInfo);
    } else if (type == AssertionType.DATASET) {
      DatasetAssertionInfo datasetInfo = new DatasetAssertionInfo();
      datasetInfo.setDataset(derivableEntityUrn);
      datasetInfo.setScope(DatasetAssertionScope.DATASET_ROWS);
      datasetInfo.setOperator(AssertionStdOperator.NOT_NULL);
      assertionInfo.setDatasetAssertion(datasetInfo);
    }

    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createAssertionWithoutDerivableEntityUrn(Urn assertionUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    // NO entityUrn set
    FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
    freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    // Don't set entity - makes it non-derivable
    assertionInfo.setFreshnessAssertion(freshnessInfo);

    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }

  /**
   * Create an assertion with a type set but the corresponding sub-property is empty. This tests
   * AssertionTypeMismatchCheck.
   */
  private EntityResponse createAssertionWithTypeMismatch(
      Urn assertionUrn, Urn entityUrn, AssertionType type) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(type);
    assertionInfo.setEntityUrn(entityUrn);
    // Intentionally NOT setting the type-specific sub-property (e.g., freshnessAssertion)
    // This creates a type/property mismatch
    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);
    return response;
  }
}
