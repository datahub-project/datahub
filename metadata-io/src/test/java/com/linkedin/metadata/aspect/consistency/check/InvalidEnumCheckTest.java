package com.linkedin.metadata.aspect.consistency.check;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for the generic {@link InvalidEnumCheck} class.
 *
 * <p>These tests verify that the base InvalidEnumCheck class:
 *
 * <ul>
 *   <li>Detects invalid enum values in aspects
 *   <li>Returns DELETE_ASPECT fix type (not HARD_DELETE)
 *   <li>Populates batchItems with DeleteItemImpl for the specific aspect
 * </ul>
 */
public class InvalidEnumCheckTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private GraphClient mockGraphClient;

  private EntityRegistry testEntityRegistry;
  private OperationContext testOpContext;
  private InvalidEnumCheck check;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    testEntityRegistry = TestOperationContexts.defaultEntityRegistry();
    testOpContext = TestOperationContexts.systemContextNoSearchAuthorization(testEntityRegistry);

    check = new InvalidEnumCheck();
  }

  private CheckContext buildContext() {
    return CheckContext.builder()
        .operationContext(testOpContext)
        .entityService(mockEntityService)
        .graphClient(mockGraphClient)
        .build();
  }

  /**
   * Test that the generic InvalidEnumCheck uses DELETE_ASPECT with batchItems instead of
   * HARD_DELETE. This validates that only the specific aspect with the invalid enum is deleted, not
   * the entire entity.
   */
  @Test
  public void testInvalidEnumCheck_DeleteAspectWithBatchItems() {
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:genericEnumTest");
    EntityResponse response = createAssertionWithRawType(assertionUrn, "INVALID_VALUE");
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues = check.check(context, Map.of(assertionUrn, response));

    // Generic check should fire with DELETE_ASPECT (not HARD_DELETE)
    assertEquals(issues.size(), 1);
    ConsistencyIssue issue = issues.get(0);
    assertEquals(issue.getFixType(), ConsistencyFixType.DELETE_ASPECT);
    assertEquals(issue.getEntityUrn(), assertionUrn);
    assertTrue(
        issue.getDescription().contains("is not an enum symbol"),
        "Description should contain enum error message");

    // Verify batchItems is populated with a DeleteItemImpl for DELETE_ASPECT fix type
    assertNotNull(issue.getBatchItems(), "batchItems should be populated for DELETE_ASPECT");
    assertEquals(issue.getBatchItems().size(), 1);
    assertTrue(
        issue.getBatchItems().get(0) instanceof DeleteItemImpl,
        "batchItems should contain DeleteItemImpl");

    DeleteItemImpl deleteItem = (DeleteItemImpl) issue.getBatchItems().get(0);
    assertEquals(deleteItem.getUrn(), assertionUrn);
    assertEquals(deleteItem.getAspectName(), ASSERTION_INFO_ASPECT_NAME);

    // Verify hardDeleteUrns is NOT populated (DELETE_ASPECT uses batchItems, not hardDeleteUrns)
    assertNull(
        issue.getHardDeleteUrns(),
        "hardDeleteUrns should be null for DELETE_ASPECT (uses batchItems)");
  }

  /** Test that a valid enum value produces no issues. */
  @Test
  public void testValidEnumValue_NoIssue() {
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:validEnumTest");
    EntityResponse response = createAssertionWithRawType(assertionUrn, "FRESHNESS");
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues = check.check(context, Map.of(assertionUrn, response));

    assertTrue(issues.isEmpty(), "Valid enum value should not produce any issues");
  }

  /** Test that a missing enum field does not trigger the check (different from invalid). */
  @Test
  public void testMissingEnumField_NoIssue() {
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:missingEnumTest");
    EntityResponse response = createAssertionWithMissingType(assertionUrn);
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues = check.check(context, Map.of(assertionUrn, response));

    assertTrue(issues.isEmpty(), "Missing enum field should not produce any issues");
  }

  /** Test that softdeleted entities are skipped by default. */
  @Test
  public void testSoftDeletedEntity_Skipped() {
    Urn assertionUrn = UrnUtils.getUrn("urn:li:assertion:softDeletedEnumTest");
    EntityResponse response = createSoftDeletedAssertionWithRawType(assertionUrn, "INVALID_VALUE");
    CheckContext context = buildContext();

    List<ConsistencyIssue> issues = check.check(context, Map.of(assertionUrn, response));

    assertTrue(issues.isEmpty(), "Soft-deleted entities should be skipped");
  }

  /** Test that the check returns wildcard entity type. */
  @Test
  public void testEntityType_IsWildcard() {
    assertEquals(
        check.getEntityType(),
        "*",
        "InvalidEnumCheck should return wildcard entity type to handle all entities");
  }

  /** Test that the check is on-demand only by default. */
  @Test
  public void testIsOnDemandOnly_True() {
    assertTrue(check.isOnDemandOnly(), "InvalidEnumCheck should be on-demand only by default");
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Create an assertion with a raw type string value in the DataMap. This bypasses enum validation
   * during construction and allows testing invalid enum values.
   */
  private EntityResponse createAssertionWithRawType(Urn assertionUrn, String typeValue) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    // Create assertionInfo with raw type value directly in DataMap
    DataMap assertionInfoData = new DataMap();
    assertionInfoData.put("type", typeValue);
    EnvelopedAspect assertionInfoAspect = new EnvelopedAspect();
    assertionInfoAspect.setValue(new Aspect(assertionInfoData));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, assertionInfoAspect);

    // Add status aspect (not soft-deleted)
    Status status = new Status().setRemoved(false);
    EnvelopedAspect statusAspect = new EnvelopedAspect();
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    response.setAspects(aspects);
    return response;
  }

  /**
   * Create a soft-deleted assertion with a raw type string value in the DataMap. Used to test that
   * soft-deleted entities are skipped.
   */
  private EntityResponse createSoftDeletedAssertionWithRawType(Urn assertionUrn, String typeValue) {
    EntityResponse response = createAssertionWithRawType(assertionUrn, typeValue);

    // Override status to be soft-deleted
    Status status = new Status().setRemoved(true);
    EnvelopedAspect statusAspect = new EnvelopedAspect();
    statusAspect.setValue(new Aspect(status.data()));
    response.getAspects().put(STATUS_ASPECT_NAME, statusAspect);

    return response;
  }

  /**
   * Create an assertion with missing type field in the DataMap. This tests that a missing type
   * field does not trigger the invalid enum check.
   */
  private EntityResponse createAssertionWithMissingType(Urn assertionUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    // Create assertionInfo without type field
    DataMap assertionInfoData = new DataMap();
    // No type field set
    EnvelopedAspect assertionInfoAspect = new EnvelopedAspect();
    assertionInfoAspect.setValue(new Aspect(assertionInfoData));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, assertionInfoAspect);

    // Add status aspect (not soft-deleted)
    Status status = new Status().setRemoved(false);
    EnvelopedAspect statusAspect = new EnvelopedAspect();
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    response.setAspects(aspects);
    return response;
  }
}
