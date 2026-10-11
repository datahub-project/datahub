package com.linkedin.metadata.aspect.consistency.check;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for OrphanIndexDocumentCheck. */
public class OrphanIndexDocumentCheckTest {

  @Mock private OperationContext mockOpContext;
  @Mock private EntityService<?> mockEntityService;
  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;

  private OrphanIndexDocumentCheck check;
  private CheckContext checkContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockOpContext.getAspectRetriever()).thenReturn(mockAspectRetriever);

    check = new OrphanIndexDocumentCheck();
    checkContext =
        CheckContext.builder()
            .operationContext(mockOpContext)
            .entityService(mockEntityService)
            .build();
  }

  // ============================================================================
  // Basic Check Properties
  // ============================================================================

  @Test
  public void testCheckId() {
    assertEquals(check.getId(), "orphan-index-document");
  }

  @Test
  public void testCheckName() {
    assertEquals(check.getName(), "Orphan Index Document");
  }

  @Test
  public void testCheckDescription() {
    assertTrue(check.getDescription().contains("ES indices"));
    assertTrue(check.getDescription().contains("SQL"));
  }

  @Test
  public void testEntityType() {
    assertEquals(check.getEntityType(), ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE);
  }

  @Test
  public void testRequiredAspects() {
    // Orphan check doesn't need any specific aspects
    assertTrue(check.getRequiredAspects().isPresent());
    assertTrue(check.getRequiredAspects().get().isEmpty());
  }

  @Test
  public void testIsOnDemandOnly() {
    assertTrue(check.isOnDemandOnly());
  }

  // ============================================================================
  // Check Method Tests
  // ============================================================================

  @Test
  public void testCheckReturnsEmptyList() {
    // The normal check() method should return empty list
    // Orphan detection happens via checkOrphans()
    List<ConsistencyIssue> issues = check.check(checkContext, Map.of());
    assertTrue(issues.isEmpty());
  }

  // ============================================================================
  // checkOrphans Tests
  // ============================================================================

  @Test
  public void testCheckOrphansWithNoOrphans() {
    // No orphan URNs in context
    List<ConsistencyIssue> issues = check.checkOrphans(checkContext, "assertion");
    assertTrue(issues.isEmpty());
  }

  @Test
  public void testCheckOrphansWithSingleOrphan() {
    Urn orphanUrn = UrnUtils.getUrn("urn:li:assertion:orphan-1");
    checkContext.addOrphanUrns("assertion", Set.of(orphanUrn));

    List<ConsistencyIssue> issues = check.checkOrphans(checkContext, "assertion");

    assertEquals(issues.size(), 1);
    ConsistencyIssue issue = issues.get(0);
    assertEquals(issue.getEntityUrn(), orphanUrn);
    assertEquals(issue.getEntityType(), "assertion");
    assertEquals(issue.getCheckId(), "orphan-index-document");
    assertEquals(issue.getFixType(), ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
    assertTrue(issue.getDescription().contains("orphaned"));
    assertEquals(issue.getHardDeleteUrns().size(), 1);
    assertEquals(issue.getHardDeleteUrns().get(0), orphanUrn);
  }

  @Test
  public void testCheckOrphansWithMultipleOrphans() {
    Urn orphan1 = UrnUtils.getUrn("urn:li:monitor:orphan-1");
    Urn orphan2 = UrnUtils.getUrn("urn:li:monitor:orphan-2");
    Urn orphan3 = UrnUtils.getUrn("urn:li:monitor:orphan-3");

    checkContext.addOrphanUrns("monitor", Set.of(orphan1, orphan2, orphan3));

    List<ConsistencyIssue> issues = check.checkOrphans(checkContext, "monitor");

    assertEquals(issues.size(), 3);
    for (ConsistencyIssue issue : issues) {
      assertEquals(issue.getEntityType(), "monitor");
      assertEquals(issue.getCheckId(), "orphan-index-document");
      assertEquals(issue.getFixType(), ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
    }
  }

  @Test
  public void testCheckOrphansForDifferentEntityType() {
    // Add orphans for assertion type
    Urn assertionOrphan = UrnUtils.getUrn("urn:li:assertion:orphan-1");
    checkContext.addOrphanUrns("assertion", Set.of(assertionOrphan));

    // Check for monitor type - should be empty
    List<ConsistencyIssue> issues = check.checkOrphans(checkContext, "monitor");
    assertTrue(issues.isEmpty());

    // Check for assertion type - should find the orphan
    issues = check.checkOrphans(checkContext, "assertion");
    assertEquals(issues.size(), 1);
  }

  @Test
  public void testCheckOrphansMultipleEntityTypes() {
    // Add orphans for multiple entity types
    Urn assertionOrphan = UrnUtils.getUrn("urn:li:assertion:orphan-1");
    Urn monitorOrphan = UrnUtils.getUrn("urn:li:monitor:orphan-1");

    checkContext.addOrphanUrns("assertion", Set.of(assertionOrphan));
    checkContext.addOrphanUrns("monitor", Set.of(monitorOrphan));

    // Check each type separately
    List<ConsistencyIssue> assertionIssues = check.checkOrphans(checkContext, "assertion");
    assertEquals(assertionIssues.size(), 1);
    assertEquals(assertionIssues.get(0).getEntityType(), "assertion");

    List<ConsistencyIssue> monitorIssues = check.checkOrphans(checkContext, "monitor");
    assertEquals(monitorIssues.size(), 1);
    assertEquals(monitorIssues.get(0).getEntityType(), "monitor");
  }

  @Test
  public void testIssueDescriptionContainsUrn() {
    Urn orphanUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    checkContext.addOrphanUrns("dataset", Set.of(orphanUrn));

    List<ConsistencyIssue> issues = check.checkOrphans(checkContext, "dataset");

    assertEquals(issues.size(), 1);
    assertTrue(issues.get(0).getDescription().contains(orphanUrn.toString()));
  }
}
