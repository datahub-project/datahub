package com.linkedin.metadata.aspect.consistency;

import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixDetail;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixResult;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

/** Tests for Issue and CheckResult builder patterns and helper methods. */
public class ConsistencyIssueAndCheckResultTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:assertion:test-assertion-123");
  private static final Urn RELATED_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

  // ============================================================================
  // Issue Tests
  // ============================================================================

  @Test
  public void testIssueBuilderMinimal() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.SOFT_DELETE)
            .description("Test issue")
            .build();

    assertEquals(issue.getEntityUrn(), TEST_URN);
    assertEquals(issue.getEntityType(), "assertion");
    assertEquals(issue.getCheckId(), "test-check");
    assertEquals(issue.getFixType(), ConsistencyFixType.SOFT_DELETE);
    assertEquals(issue.getDescription(), "Test issue");

    // Optional fields should be null
    assertNull(issue.getRelatedUrns());
    assertNull(issue.getDetails());
    assertNull(issue.getBatchItems());
    assertNull(issue.getHardDeleteUrns());
    assertNull(issue.getMetadata());
  }

  @Test
  public void testIssueBuilderWithAllFields() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Full issue")
            .relatedUrns(List.of(RELATED_URN))
            .details("Additional details")
            .batchItems(List.of())
            .hardDeleteUrns(List.of(TEST_URN))
            .metadata(Map.of("key", "value"))
            .build();

    assertEquals(issue.getRelatedUrns().size(), 1);
    assertEquals(issue.getRelatedUrns().get(0), RELATED_URN);
    assertEquals(issue.getDetails(), "Additional details");
    assertNotNull(issue.getBatchItems());
    assertEquals(issue.getHardDeleteUrns().size(), 1);
    assertEquals(issue.getMetadata().get("key"), "value");
  }

  @Test
  public void testIssueForEntityStaticMethod() {
    ConsistencyIssue issue =
        ConsistencyIssue.forEntity(TEST_URN, "assertion", "my-check", ConsistencyFixType.UPSERT)
            .description("Created via forEntity")
            .build();

    assertEquals(issue.getEntityUrn(), TEST_URN);
    assertEquals(issue.getEntityType(), "assertion");
    assertEquals(issue.getCheckId(), "my-check");
    assertEquals(issue.getFixType(), ConsistencyFixType.UPSERT);
    assertEquals(issue.getDescription(), "Created via forEntity");
  }

  @Test
  public void testIssueForEntityWithAdditionalFields() {
    ConsistencyIssue issue =
        ConsistencyIssue.forEntity(
                TEST_URN, "monitor", "monitor-check", ConsistencyFixType.SOFT_DELETE)
            .description("Monitor issue")
            .relatedUrns(List.of(RELATED_URN))
            .details("Some details")
            .build();

    assertEquals(issue.getEntityType(), "monitor");
    assertEquals(issue.getFixType(), ConsistencyFixType.SOFT_DELETE);
    assertNotNull(issue.getRelatedUrns());
    assertEquals(issue.getDetails(), "Some details");
  }

  // ============================================================================
  // CheckResult Tests
  // ============================================================================

  @Test
  public void testCheckResultEmpty() {
    CheckResult result = CheckResult.empty();

    assertEquals(result.getEntitiesScanned(), 0);
    assertEquals(result.getIssuesFound(), 0);
    assertNotNull(result.getIssues());
    assertTrue(result.getIssues().isEmpty());
    assertNull(result.getScrollId());
  }

  @Test
  public void testCheckResultBuilder() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.SOFT_DELETE)
            .description("Test issue")
            .build();

    CheckResult result =
        CheckResult.builder()
            .entitiesScanned(100)
            .issuesFound(1)
            .issues(List.of(issue))
            .scrollId("scroll123")
            .build();

    assertEquals(result.getEntitiesScanned(), 100);
    assertEquals(result.getIssuesFound(), 1);
    assertEquals(result.getIssues().size(), 1);
    assertEquals(result.getIssues().get(0), issue);
    assertEquals(result.getScrollId(), "scroll123");
  }

  @Test
  public void testCheckResultWithNoIssues() {
    CheckResult result =
        CheckResult.builder().entitiesScanned(50).issuesFound(0).issues(List.of()).build();

    assertEquals(result.getEntitiesScanned(), 50);
    assertEquals(result.getIssuesFound(), 0);
    assertTrue(result.getIssues().isEmpty());
    assertNull(result.getScrollId());
  }

  @Test
  public void testCheckResultWithMultipleIssues() {
    ConsistencyIssue issue1 =
        ConsistencyIssue.forEntity(TEST_URN, "assertion", "check1", ConsistencyFixType.SOFT_DELETE)
            .description("Issue 1")
            .build();

    ConsistencyIssue issue2 =
        ConsistencyIssue.forEntity(RELATED_URN, "dataset", "check2", ConsistencyFixType.UPSERT)
            .description("Issue 2")
            .build();

    CheckResult result =
        CheckResult.builder()
            .entitiesScanned(200)
            .issuesFound(2)
            .issues(List.of(issue1, issue2))
            .scrollId("next-page")
            .build();

    assertEquals(result.getEntitiesScanned(), 200);
    assertEquals(result.getIssuesFound(), 2);
    assertEquals(result.getIssues().size(), 2);
    assertEquals(result.getScrollId(), "next-page");
  }

  // ============================================================================
  // FixDetail Tests
  // ============================================================================

  @Test
  public void testFixDetailSuccess() {
    ConsistencyFixDetail detail =
        ConsistencyFixDetail.builder()
            .urn(TEST_URN)
            .action(ConsistencyFixType.SOFT_DELETE)
            .success(true)
            .details("Entity soft-deleted")
            .build();

    assertEquals(detail.getUrn(), TEST_URN);
    assertEquals(detail.getAction(), ConsistencyFixType.SOFT_DELETE);
    assertTrue(detail.isSuccess());
    assertEquals(detail.getDetails(), "Entity soft-deleted");
    assertNull(detail.getErrorMessage());
  }

  @Test
  public void testFixDetailFailure() {
    ConsistencyFixDetail detail =
        ConsistencyFixDetail.builder()
            .urn(TEST_URN)
            .action(ConsistencyFixType.HARD_DELETE)
            .success(false)
            .errorMessage("Delete operation failed")
            .build();

    assertFalse(detail.isSuccess());
    assertEquals(detail.getErrorMessage(), "Delete operation failed");
    assertNull(detail.getDetails());
  }

  @Test
  public void testFixDetailPartialSuccess() {
    ConsistencyFixDetail detail =
        ConsistencyFixDetail.builder()
            .urn(TEST_URN)
            .action(ConsistencyFixType.UPSERT)
            .success(false)
            .details("Partial success: some aspects updated")
            .errorMessage("Some aspects failed")
            .build();

    assertFalse(detail.isSuccess());
    assertNotNull(detail.getDetails());
    assertNotNull(detail.getErrorMessage());
  }

  // ============================================================================
  // FixResult Tests
  // ============================================================================

  @Test
  public void testFixResultSuccess() {
    ConsistencyFixDetail detail =
        ConsistencyFixDetail.builder()
            .urn(TEST_URN)
            .action(ConsistencyFixType.SOFT_DELETE)
            .success(true)
            .build();

    ConsistencyFixResult result =
        ConsistencyFixResult.builder()
            .dryRun(false)
            .totalProcessed(1)
            .entitiesFixed(1)
            .entitiesFailed(0)
            .fixDetails(List.of(detail))
            .build();

    assertFalse(result.isDryRun());
    assertEquals(result.getTotalProcessed(), 1);
    assertEquals(result.getEntitiesFixed(), 1);
    assertEquals(result.getEntitiesFailed(), 0);
    assertEquals(result.getFixDetails().size(), 1);
  }

  @Test
  public void testFixResultDryRun() {
    ConsistencyFixResult result =
        ConsistencyFixResult.builder()
            .dryRun(true)
            .totalProcessed(5)
            .entitiesFixed(5)
            .entitiesFailed(0)
            .fixDetails(List.of())
            .build();

    assertTrue(result.isDryRun());
    assertEquals(result.getTotalProcessed(), 5);
  }

  @Test
  public void testFixResultWithFailures() {
    ConsistencyFixDetail successDetail =
        ConsistencyFixDetail.builder()
            .urn(TEST_URN)
            .action(ConsistencyFixType.SOFT_DELETE)
            .success(true)
            .build();

    ConsistencyFixDetail failureDetail =
        ConsistencyFixDetail.builder()
            .urn(RELATED_URN)
            .action(ConsistencyFixType.SOFT_DELETE)
            .success(false)
            .errorMessage("Failed")
            .build();

    ConsistencyFixResult result =
        ConsistencyFixResult.builder()
            .dryRun(false)
            .totalProcessed(2)
            .entitiesFixed(1)
            .entitiesFailed(1)
            .fixDetails(List.of(successDetail, failureDetail))
            .build();

    assertEquals(result.getTotalProcessed(), 2);
    assertEquals(result.getEntitiesFixed(), 1);
    assertEquals(result.getEntitiesFailed(), 1);
    assertEquals(result.getFixDetails().size(), 2);
  }
}
