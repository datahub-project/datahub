package com.linkedin.metadata.aspect.consistency.check.assertion;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.UrnValidationFieldSpec;
import com.linkedin.metadata.models.annotation.UrnValidationAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for AbstractAssertionCheck base class functionality. */
public class AbstractAssertionCheckTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private OperationContext mockOpContext;
  @Mock private EntityRegistry mockEntityRegistry;

  private CheckContext checkContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    checkContext =
        CheckContext.builder()
            .operationContext(mockOpContext)
            .entityService(mockEntityService)
            .build();

    // Setup entity registry mocks for AbstractAssertionCheck constructor
    setupEntityRegistryMocks();
  }

  private void setupEntityRegistryMocks() {
    EntitySpec entitySpec = mock(EntitySpec.class);
    AspectSpec aspectSpec = mock(AspectSpec.class);

    when(mockEntityRegistry.getEntitySpec(ASSERTION_ENTITY_NAME)).thenReturn(entitySpec);
    when(entitySpec.getAspectSpec(ASSERTION_INFO_ASPECT_NAME)).thenReturn(aspectSpec);

    // Setup URN validation field specs
    UrnValidationFieldSpec fieldSpec = mock(UrnValidationFieldSpec.class);
    UrnValidationAnnotation annotation = mock(UrnValidationAnnotation.class);
    when(fieldSpec.getUrnValidationAnnotation()).thenReturn(annotation);
    when(annotation.getEntityTypes()).thenReturn(List.of("dataset", "dataJob", "chart"));

    Map<String, UrnValidationFieldSpec> fieldSpecMap = new HashMap<>();
    fieldSpecMap.put("entityUrn", fieldSpec);
    when(aspectSpec.getUrnValidationFieldSpecMap()).thenReturn(fieldSpecMap);
  }

  // ============================================================================
  // Test Check - Simple implementation for testing
  // ============================================================================

  /** Test check that returns issues based on assertion info. */
  private class TestAssertionCheck extends AbstractAssertionCheck {

    private final boolean returnIssue;

    TestAssertionCheck(boolean returnIssue) {
      super(mockEntityRegistry);
      this.returnIssue = returnIssue;
    }

    @Override
    @Nonnull
    public String getName() {
      return "Test Assertion Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "A test assertion check";
    }

    @Override
    @Nonnull
    protected List<ConsistencyIssue> checkAssertion(
        @Nonnull CheckContext ctx,
        @Nonnull Urn assertionUrn,
        @Nonnull EntityResponse response,
        @Nonnull AssertionInfo assertionInfo) {
      if (returnIssue) {
        return List.of(
            createIssueBuilder(assertionUrn, ConsistencyFixType.SOFT_DELETE)
                .description("Test issue for assertion")
                .build());
      }
      return Collections.emptyList();
    }
  }

  // ============================================================================
  // Entity Type Tests
  // ============================================================================

  @Test
  public void testEntityTypeIsAssertion() {
    TestAssertionCheck check = new TestAssertionCheck(false);
    assertEquals(check.getEntityType(), ASSERTION_ENTITY_NAME);
  }

  // ============================================================================
  // Required Aspects Tests
  // ============================================================================

  @Test
  public void testRequiredAspectsIncludesAssertionInfoAndStatus() {
    TestAssertionCheck check = new TestAssertionCheck(false);
    assertTrue(check.getRequiredAspects().isPresent());
    Set<String> aspects = check.getRequiredAspects().get();

    assertTrue(aspects.contains(ASSERTION_INFO_ASPECT_NAME));
    assertTrue(aspects.contains(STATUS_ASPECT_NAME));
    assertEquals(aspects.size(), 2);
  }

  // ============================================================================
  // AssertionInfo Extraction Tests
  // ============================================================================

  @Test
  public void testReturnsEmptyWhenNoAssertionInfo() {
    TestAssertionCheck check = new TestAssertionCheck(true);
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test123");

    // Create response without AssertionInfo
    EntityResponse response = createEntityResponseWithoutAssertionInfo(urn);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(urn, response));

    // Should return empty since no assertion info present
    assertTrue(issues.isEmpty());
  }

  @Test
  public void testExtractsAssertionInfoAndCallsCheck() {
    TestAssertionCheck check = new TestAssertionCheck(true);
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    EntityResponse response = createEntityResponseWithAssertionInfo(urn, entityUrn);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(urn, response));

    assertEquals(issues.size(), 1);
    assertEquals(issues.get(0).getDescription(), "Test issue for assertion");
  }

  // ============================================================================
  // Valid Entity Type Tests
  // ============================================================================

  @Test
  public void testValidAssertionEntityTypes() {
    TestAssertionCheck check = new TestAssertionCheck(false);

    // These should be valid based on the mock setup
    assertTrue(check.isValidAssertionEntityType("dataset"));
    assertTrue(check.isValidAssertionEntityType("dataJob"));
    assertTrue(check.isValidAssertionEntityType("chart"));

    // These should not be valid
    assertFalse(check.isValidAssertionEntityType("schemaField")); // Excluded
    assertFalse(check.isValidAssertionEntityType("unknownEntity"));
  }

  // ============================================================================
  // Caching Tests
  // ============================================================================

  @Test
  public void testAssertionInfoCachedInContext() {
    TestAssertionCheck check = new TestAssertionCheck(false);
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test123");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    EntityResponse response = createEntityResponseWithAssertionInfo(urn, entityUrn);

    check.check(checkContext, Map.of(urn, response));

    // Verify assertion info was cached
    AssertionInfo cached = checkContext.getCachedAspect(urn, ASSERTION_INFO_ASPECT_NAME);
    assertNotNull(cached);
    assertEquals(cached.getType(), AssertionType.FRESHNESS);
  }

  // ============================================================================
  // ID Derivation Tests
  // ============================================================================

  @Test
  public void testIdDerivedFromClassName() {
    TestAssertionCheck check = new TestAssertionCheck(false);
    // TestAssertionCheck -> test-assertion (after removing "Check" suffix)
    assertEquals(check.getId(), "test-assertion");
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private EntityResponse createEntityResponseWithoutAssertionInfo(Urn urn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);
    response.setAspects(aspects);

    return response;
  }

  private EntityResponse createEntityResponseWithAssertionInfo(Urn assertionUrn, Urn entityUrn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(assertionUrn);
    response.setEntityName(ASSERTION_ENTITY_NAME);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    // Add status aspect
    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    // Add assertion info aspect with freshness assertion
    EnvelopedAspect infoAspect = new EnvelopedAspect();
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
    freshnessInfo.setEntity(entityUrn);
    assertionInfo.setFreshnessAssertion(freshnessInfo);
    infoAspect.setValue(new Aspect(assertionInfo.data()));
    aspects.put(ASSERTION_INFO_ASPECT_NAME, infoAspect);

    response.setAspects(aspects);

    return response;
  }
}
