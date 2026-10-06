package com.linkedin.metadata.entity;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.List;
import org.testng.annotations.Test;

public class BatchWriteResultTest {

  private static final Urn URN_A = UrnUtils.getUrn("urn:li:corpuser:a");
  private static final Urn URN_B = UrnUtils.getUrn("urn:li:corpuser:b");
  private static final String ASPECT = "status";

  @Test
  public void partitionsByOutcome() {
    BatchWriteResult result =
        new BatchWriteResult(
            List.of(
                AspectWriteResult.committed(URN_A, ASPECT, 0L),
                AspectWriteResult.conflict(URN_B, ASPECT),
                AspectWriteResult.failed(URN_A, "keyAspect", new IllegalStateException("nope")),
                AspectWriteResult.noop(URN_B, "keyAspect")));

    assertEquals(result.committedResults().size(), 1);
    assertEquals(result.committedResults().get(0).getUrn(), URN_A);
    assertEquals(result.failureResults().size(), 1);
    assertEquals(result.conflictedUrns(), java.util.Set.of(URN_B));
    assertTrue(result.hasConflicts());
  }

  @Test
  public void conflictedUrnsDedupedPerUrn() {
    BatchWriteResult result =
        new BatchWriteResult(
            List.of(
                AspectWriteResult.conflict(URN_A, "status"),
                AspectWriteResult.conflict(URN_A, "ownership")));

    assertEquals(result.conflictedUrns(), java.util.Set.of(URN_A));
  }

  @Test
  public void emptyBatchHasNoConflicts() {
    BatchWriteResult result = new BatchWriteResult(List.of());

    assertFalse(result.hasConflicts());
    assertTrue(result.conflictedUrns().isEmpty());
    assertTrue(result.committedResults().isEmpty());
    assertTrue(result.failureResults().isEmpty());
  }
}
