package com.linkedin.metadata.aspect.consistency.fix;

import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestoreIndicesFixTest {

  private static final Urn URN_1 = UrnUtils.getUrn("urn:li:document:doc-1");
  private static final Urn URN_2 = UrnUtils.getUrn("urn:li:document:doc-2");
  private static final Urn URN_3 = UrnUtils.getUrn("urn:li:document:doc-3");
  private static final Urn ORPHAN_URN = UrnUtils.getUrn("urn:li:document:orphan");

  private EntityService<?> entityService;
  private OperationContext opContext;
  private RestoreIndicesFix fix;

  @BeforeMethod
  public void setup() {
    entityService = mock(EntityService.class);
    opContext = mock(OperationContext.class);
    fix = new RestoreIndicesFix(entityService);
  }

  @Test
  public void testDryRunPerformsZeroStorageOrKafkaOperations() {
    ConsistencyIssue scoped = issue(URN_1, Map.of("aspects", SEMANTIC_CONTENT_ASPECT_NAME));
    ConsistencyIssue full = issue(URN_2, null);

    List<ConsistencyFixDetail> details = fix.applyBatch(opContext, List.of(scoped, full), true);

    assertEquals(details.size(), 2);
    assertTrue(details.get(0).isSuccess());
    assertTrue(details.get(0).getDetails().contains("aspects=semanticContent"));
    assertTrue(details.get(1).isSuccess());
    assertTrue(details.get(1).getDetails().contains("all aspects"));
    verifyNoInteractions(entityService);
  }

  @Test
  public void testApplyBatchGroupsByAspectScopeGuardsOrphansAndPreservesOrder() throws Exception {
    when(entityService.exists(
            eq(opContext),
            eq(Set.of(URN_1, ORPHAN_URN, URN_2, URN_3)),
            isNull(),
            eq(true),
            eq(false)))
        .thenReturn(Set.of(URN_1, URN_2, URN_3));

    RestoreIndicesResult migratedTwo = new RestoreIndicesResult();
    migratedTwo.rowsMigrated = 2;
    RestoreIndicesResult migratedFive = new RestoreIndicesResult();
    migratedFive.rowsMigrated = 5;

    when(entityService.restoreIndices(
            eq(opContext),
            eq(Set.of(URN_1, URN_3)),
            eq(Set.of(SEMANTIC_CONTENT_ASPECT_NAME)),
            eq(2),
            eq(false)))
        .thenReturn(List.of(migratedTwo));
    when(entityService.restoreIndices(eq(opContext), eq(Set.of(URN_2)), isNull(), eq(1), eq(false)))
        .thenReturn(List.of(migratedFive));

    List<ConsistencyIssue> issues =
        List.of(
            issue(URN_1, Map.of("aspects", SEMANTIC_CONTENT_ASPECT_NAME)),
            issue(ORPHAN_URN, Map.of("aspects", SEMANTIC_CONTENT_ASPECT_NAME)),
            issue(URN_2, null),
            issue(URN_3, Map.of("aspects", SEMANTIC_CONTENT_ASPECT_NAME)));

    List<ConsistencyFixDetail> details = fix.applyBatch(opContext, issues, false);

    assertEquals(details.size(), 4);
    assertEquals(details.get(0).getUrn(), URN_1);
    assertTrue(details.get(0).isSuccess());
    assertEquals(details.get(1).getUrn(), ORPHAN_URN);
    assertFalse(details.get(1).isSuccess());
    assertTrue(details.get(1).getErrorMessage().contains("key aspect"));
    assertEquals(details.get(2).getUrn(), URN_2);
    assertTrue(details.get(2).isSuccess());
    assertEquals(details.get(3).getUrn(), URN_3);
    assertTrue(details.get(3).isSuccess());
  }

  @Test
  public void testDuplicateUrnsPromoteAllAspectsOverSubset() throws Exception {
    when(entityService.exists(eq(opContext), eq(Set.of(URN_1)), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of(URN_1));
    when(entityService.restoreIndices(eq(opContext), eq(Set.of(URN_1)), isNull(), eq(1), eq(false)))
        .thenReturn(List.of(new RestoreIndicesResult()));

    List<ConsistencyFixDetail> details =
        fix.applyBatch(
            opContext,
            List.of(
                issue(URN_1, Map.of("aspects", SEMANTIC_CONTENT_ASPECT_NAME)), issue(URN_1, null)),
            false);

    assertEquals(details.size(), 2);
    assertTrue(details.get(0).isSuccess());
    assertTrue(details.get(1).isSuccess());
    verify(entityService, times(1))
        .restoreIndices(eq(opContext), eq(Set.of(URN_1)), isNull(), eq(1), eq(false));
  }

  @Test
  public void testMultiUrnBatchFailureIsolatesPerUrnWhileSingleUrnDoesNotRetry() throws Exception {
    when(entityService.exists(eq(opContext), any(), isNull(), eq(true), eq(false)))
        .thenReturn(Set.of(URN_1, URN_2, URN_3));

    when(entityService.restoreIndices(
            eq(opContext),
            eq(Set.of(URN_1, URN_2)),
            eq(Set.of(SEMANTIC_CONTENT_ASPECT_NAME)),
            eq(2),
            eq(false)))
        .thenThrow(new RuntimeException("batch failed"));
    when(entityService.restoreIndices(
            eq(opContext),
            eq(Set.of(URN_1)),
            eq(Set.of(SEMANTIC_CONTENT_ASPECT_NAME)),
            eq(1),
            eq(false)))
        .thenReturn(List.of(new RestoreIndicesResult()));
    when(entityService.restoreIndices(
            eq(opContext),
            eq(Set.of(URN_2)),
            eq(Set.of(SEMANTIC_CONTENT_ASPECT_NAME)),
            eq(1),
            eq(false)))
        .thenThrow(new RuntimeException("corrupt row on urn 2"));

    when(entityService.restoreIndices(eq(opContext), eq(Set.of(URN_3)), isNull(), eq(1), eq(false)))
        .thenThrow(new RuntimeException("single urn failed"));

    List<ConsistencyFixDetail> details =
        fix.applyBatch(
            opContext,
            List.of(
                issue(URN_1, Map.of("aspects", SEMANTIC_CONTENT_ASPECT_NAME)),
                issue(URN_2, Map.of("aspects", SEMANTIC_CONTENT_ASPECT_NAME)),
                issue(URN_3, null)),
            false);

    assertEquals(details.size(), 3);
    assertTrue(details.get(0).isSuccess());
    assertFalse(details.get(1).isSuccess());
    assertFalse(details.get(2).isSuccess());
    // Single-URN group URN_3 must only be attempted once, never retried.
    verify(entityService, times(1))
        .restoreIndices(eq(opContext), eq(Set.of(URN_3)), isNull(), anyInt(), anyBoolean());
  }

  private static ConsistencyIssue issue(Urn urn, Map<String, String> metadata) {
    return ConsistencyIssue.forEntity(
            urn, "document", "semantic-content-index-mismatch", ConsistencyFixType.RESTORE_INDICES)
        .description("Mismatch")
        .metadata(metadata)
        .build();
  }
}
