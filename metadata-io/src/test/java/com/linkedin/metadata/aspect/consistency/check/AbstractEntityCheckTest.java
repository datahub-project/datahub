package com.linkedin.metadata.aspect.consistency.check;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.UNVERSIONED_ASPECT_VERSION;
import static org.testng.Assert.*;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for AbstractEntityCheck base class functionality. */
public class AbstractEntityCheckTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private OperationContext mockOpContext;

  private CheckContext checkContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    checkContext =
        CheckContext.builder()
            .operationContext(mockOpContext)
            .entityService(mockEntityService)
            .build();
  }

  // ============================================================================
  // Test Check - Simple implementation for testing base class
  // ============================================================================

  /** Test check that returns a fixed issue for each entity. */
  private static class TestCheck extends AbstractEntityCheck {
    private final List<ConsistencyIssue> issuesToReturn;

    TestCheck() {
      this.issuesToReturn = Collections.emptyList();
    }

    TestCheck(List<ConsistencyIssue> issuesToReturn) {
      this.issuesToReturn = issuesToReturn;
    }

    @Override
    @Nonnull
    public String getName() {
      return "Test Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "A test check implementation";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "dataset";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("datasetProperties"));
    }

    @Override
    @Nonnull
    protected List<ConsistencyIssue> checkEntity(
        @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response) {
      return issuesToReturn;
    }
  }

  /** Test check with exception throwing behavior. */
  private static class ThrowingCheck extends AbstractEntityCheck {
    @Override
    @Nonnull
    public String getName() {
      return "Throwing Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "A check that throws exceptions";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "dataset";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("datasetProperties"));
    }

    @Override
    @Nonnull
    protected List<ConsistencyIssue> checkEntity(
        @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response) {
      throw new RuntimeException("Test exception");
    }
  }

  /** Test check that doesn't skip soft-deleted entities. */
  private static class NoSkipSoftDeletedCheck extends AbstractEntityCheck {
    @Override
    @Nonnull
    public String getName() {
      return "No Skip Soft Deleted Check";
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "A check that processes soft-deleted entities";
    }

    @Override
    @Nonnull
    public String getEntityType() {
      return "dataset";
    }

    @Override
    @Nonnull
    public Optional<Set<String>> getRequiredAspects() {
      return Optional.of(Set.of("datasetProperties"));
    }

    @Override
    protected boolean skipSoftDeleted() {
      return false;
    }

    @Override
    @Nonnull
    protected List<ConsistencyIssue> checkEntity(
        @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response) {
      return List.of(
          createIssueBuilder(urn, ConsistencyFixType.SOFT_DELETE)
              .description("Found soft-deleted entity")
              .build());
    }
  }

  // ============================================================================
  // ID Derivation Tests
  // ============================================================================

  @Test
  public void testIdDerivedFromClassName() {
    TestCheck check = new TestCheck();
    assertEquals(check.getId(), "test");
  }

  @Test
  public void testIdDerivedFromClassNameWithMultipleWords() {
    // Create inline class with complex name
    AbstractEntityCheck check =
        new AbstractEntityCheck() {
          @Override
          @Nonnull
          public String getName() {
            return "Test";
          }

          @Override
          @Nonnull
          public String getDescription() {
            return "Test";
          }

          @Override
          @Nonnull
          public String getEntityType() {
            return "dataset";
          }

          @Override
          @Nonnull
          public Optional<Set<String>> getRequiredAspects() {
            return Optional.of(Set.of());
          }

          @Override
          @Nonnull
          protected List<ConsistencyIssue> checkEntity(
              @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response) {
            return List.of();
          }
        };

    // Anonymous class ID will be based on $1, $2, etc. - just verify it doesn't throw
    assertNotNull(check.getId());
  }

  @Test
  public void testIdCached() {
    TestCheck check = new TestCheck();
    String id1 = check.getId();
    String id2 = check.getId();
    assertSame(id1, id2); // Should return same cached instance
  }

  // ============================================================================
  // Soft Delete Handling Tests
  // ============================================================================

  @Test
  public void testSkipsSoftDeletedEntitiesByDefault() {
    TestCheck check =
        new TestCheck(
            List.of(
                ConsistencyIssue.builder()
                    .entityUrn(
                        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)"))
                    .entityType("dataset")
                    .checkId("test")
                    .fixType(ConsistencyFixType.SOFT_DELETE)
                    .description("test")
                    .build()));

    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    EntityResponse softDeletedResponse = createSoftDeletedEntityResponse(urn);

    Map<Urn, EntityResponse> entityResponses = Map.of(urn, softDeletedResponse);

    List<ConsistencyIssue> issues = check.check(checkContext, entityResponses);

    // Should skip soft-deleted entities - no issues returned
    assertTrue(issues.isEmpty());
  }

  @Test
  public void testProcessesSoftDeletedWhenConfigured() {
    NoSkipSoftDeletedCheck check = new NoSkipSoftDeletedCheck();

    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    EntityResponse softDeletedResponse = createSoftDeletedEntityResponse(urn);

    Map<Urn, EntityResponse> entityResponses = Map.of(urn, softDeletedResponse);

    List<ConsistencyIssue> issues = check.check(checkContext, entityResponses);

    // Should process soft-deleted entity and return issue
    assertEquals(issues.size(), 1);
    assertEquals(issues.get(0).getDescription(), "Found soft-deleted entity");
  }

  @Test
  public void testProcessesNonSoftDeletedEntities() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    ConsistencyIssue expectedIssue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .build();

    TestCheck check = new TestCheck(List.of(expectedIssue));
    EntityResponse normalResponse = createNormalEntityResponse(urn);

    Map<Urn, EntityResponse> entityResponses = Map.of(urn, normalResponse);

    List<ConsistencyIssue> issues = check.check(checkContext, entityResponses);

    assertEquals(issues.size(), 1);
    assertEquals(issues.get(0).getDescription(), "Test issue");
  }

  // ============================================================================
  // Exception Handling Tests
  // ============================================================================

  @Test
  public void testExceptionHandlingContinuesProcessing() {
    ThrowingCheck check = new ThrowingCheck();

    Urn urn1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test1,PROD)");
    Urn urn2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test2,PROD)");

    Map<Urn, EntityResponse> entityResponses = new HashMap<>();
    entityResponses.put(urn1, createNormalEntityResponse(urn1));
    entityResponses.put(urn2, createNormalEntityResponse(urn2));

    // Should not throw - exceptions are caught and logged
    List<ConsistencyIssue> issues = check.check(checkContext, entityResponses);

    // No issues returned when exceptions occur
    assertTrue(issues.isEmpty());
  }

  // ============================================================================
  // Empty Batch Tests
  // ============================================================================

  @Test
  public void testEmptyBatchReturnsEmptyList() {
    TestCheck check = new TestCheck();
    List<ConsistencyIssue> issues = check.check(checkContext, Map.of());
    assertTrue(issues.isEmpty());
  }

  // ============================================================================
  // Issue Builder Tests
  // ============================================================================

  @Test
  public void testCreateIssueBuilderPopulatesCommonFields() {
    TestCheck check = new TestCheck();
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    ConsistencyIssue issue =
        check
            .createIssueBuilder(urn, ConsistencyFixType.HARD_DELETE)
            .description("Test description")
            .build();

    assertEquals(issue.getEntityUrn(), urn);
    assertEquals(issue.getEntityType(), "dataset");
    assertEquals(issue.getCheckId(), "test");
    assertEquals(issue.getFixType(), ConsistencyFixType.HARD_DELETE);
    assertEquals(issue.getDescription(), "Test description");
  }

  // ============================================================================
  // Aspect Extraction Tests
  // ============================================================================

  @Test
  public void testGetAspectReturnsAspectWhenPresent() {
    TestCheck check = new TestCheck();
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    EntityResponse response = createNormalEntityResponse(urn);

    EnvelopedAspect aspect = check.getAspect(response, STATUS_ASPECT_NAME);
    assertNotNull(aspect);
  }

  @Test
  public void testGetAspectReturnsNullWhenNotPresent() {
    TestCheck check = new TestCheck();
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    EntityResponse response = createNormalEntityResponse(urn);

    EnvelopedAspect aspect = check.getAspect(response, "nonExistentAspect");
    assertNull(aspect);
  }

  // ============================================================================
  // Conditional Write Header Tests
  // ============================================================================

  @Test
  public void testBuildConditionalWriteHeadersWithVersionedAspect() {
    TestCheck check = new TestCheck();
    Status status = new Status().setRemoved(false);

    // Create original aspect with system metadata containing a version
    EnvelopedAspect originalAspect = new EnvelopedAspect();
    originalAspect.setValue(new Aspect(status.data()));
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setVersion("42");
    originalAspect.setSystemMetadata(systemMetadata);

    Map<String, String> headers = check.buildConditionalWriteHeaders(originalAspect);

    // Verify the headers contain the version
    assertNotNull(headers);
    assertEquals(headers.get(HTTP_HEADER_IF_VERSION_MATCH), "42");
  }

  @Test
  public void testBuildConditionalWriteHeadersWithNullAspect() {
    TestCheck check = new TestCheck();

    Map<String, String> headers = check.buildConditionalWriteHeaders(null);

    // Verify the headers use unversioned marker
    assertNotNull(headers);
    assertEquals(headers.get(HTTP_HEADER_IF_VERSION_MATCH), UNVERSIONED_ASPECT_VERSION);
  }

  @Test
  public void testBuildConditionalWriteHeadersWithNoSystemMetadata() {
    TestCheck check = new TestCheck();
    Status status = new Status().setRemoved(false);

    // Create original aspect without system metadata
    EnvelopedAspect originalAspect = new EnvelopedAspect();
    originalAspect.setValue(new Aspect(status.data()));
    // No system metadata set

    Map<String, String> headers = check.buildConditionalWriteHeaders(originalAspect);

    // Verify the headers use unversioned marker
    assertNotNull(headers);
    assertEquals(headers.get(HTTP_HEADER_IF_VERSION_MATCH), UNVERSIONED_ASPECT_VERSION);
  }

  @Test
  public void testBuildConditionalWriteHeadersWithNullVersion() {
    TestCheck check = new TestCheck();
    Status status = new Status().setRemoved(false);

    // Create original aspect with system metadata but null version
    EnvelopedAspect originalAspect = new EnvelopedAspect();
    originalAspect.setValue(new Aspect(status.data()));
    SystemMetadata systemMetadata = new SystemMetadata();
    // Version is not set, remains null
    originalAspect.setSystemMetadata(systemMetadata);

    Map<String, String> headers = check.buildConditionalWriteHeaders(originalAspect);

    // Verify the headers use unversioned marker
    assertNotNull(headers);
    assertEquals(headers.get(HTTP_HEADER_IF_VERSION_MATCH), UNVERSIONED_ASPECT_VERSION);
  }

  @Test
  public void testBuildConditionalWriteHeadersWithEmptyVersion() {
    TestCheck check = new TestCheck();
    Status status = new Status().setRemoved(false);

    // Create original aspect with system metadata and empty version
    EnvelopedAspect originalAspect = new EnvelopedAspect();
    originalAspect.setValue(new Aspect(status.data()));
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setVersion(""); // Empty string version
    originalAspect.setSystemMetadata(systemMetadata);

    Map<String, String> headers = check.buildConditionalWriteHeaders(originalAspect);

    // Empty string is a valid version - returns it as-is
    assertNotNull(headers);
    assertEquals(headers.get(HTTP_HEADER_IF_VERSION_MATCH), "");
  }

  @Test
  public void testBuildConditionalWriteHeadersWithDifferentVersions() {
    TestCheck check = new TestCheck();
    Status status = new Status().setRemoved(false);

    // Test various version values
    String[] versions = {"1", "100", "abc123", "-1", "0"};
    for (String version : versions) {
      EnvelopedAspect originalAspect = new EnvelopedAspect();
      originalAspect.setValue(new Aspect(status.data()));
      SystemMetadata systemMetadata = new SystemMetadata();
      systemMetadata.setVersion(version);
      originalAspect.setSystemMetadata(systemMetadata);

      Map<String, String> headers = check.buildConditionalWriteHeaders(originalAspect);

      assertNotNull(headers);
      assertEquals(
          headers.get(HTTP_HEADER_IF_VERSION_MATCH), version, "Failed for version: " + version);
    }
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private EntityResponse createSoftDeletedEntityResponse(Urn urn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(urn.getEntityType());

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(true);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);
    response.setAspects(aspects);

    return response;
  }

  private EntityResponse createNormalEntityResponse(Urn urn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(urn.getEntityType());

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);
    response.setAspects(aspects);

    return response;
  }
}
