package com.linkedin.metadata.graph.cache.snapshot;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import org.testng.annotations.Test;

public class TraversalCoverageTest {

  @Test
  public void canSatisfyRequiresExploredAndComplete() {
    TraversalCoverage incomplete =
        TraversalCoverage.builder()
            .direction(
                DirectionCoverage.builder()
                    .direction(TraversalDirection.FORWARD)
                    .explored(true)
                    .exploredDepth(1)
                    .configuredMaxDepth(5)
                    .complete(false)
                    .truncationReason("max_depth")
                    .build())
            .build();

    assertFalse(incomplete.canSatisfy(TraversalDirection.FORWARD));
    assertFalse(incomplete.canSatisfy(TraversalDirection.REVERSE));
  }

  @Test
  public void canSatisfyWhenDirectionComplete() {
    TraversalCoverage coverage =
        TraversalCoverage.builder()
            .direction(
                DirectionCoverage.builder()
                    .direction(TraversalDirection.REVERSE)
                    .explored(true)
                    .exploredDepth(2)
                    .configuredMaxDepth(5)
                    .complete(true)
                    .build())
            .build();

    assertTrue(coverage.canSatisfy(TraversalDirection.REVERSE));
    assertFalse(coverage.canSatisfy(TraversalDirection.FORWARD));
  }

  @Test
  public void withDirectionMergesPriorDirections() {
    TraversalCoverage forwardOnly =
        TraversalCoverage.builder()
            .direction(
                DirectionCoverage.builder()
                    .direction(TraversalDirection.FORWARD)
                    .explored(true)
                    .exploredDepth(1)
                    .configuredMaxDepth(5)
                    .complete(true)
                    .build())
            .build();

    TraversalCoverage merged =
        forwardOnly.withDirection(
            DirectionCoverage.builder()
                .direction(TraversalDirection.REVERSE)
                .explored(true)
                .exploredDepth(1)
                .configuredMaxDepth(5)
                .complete(true)
                .build());

    assertTrue(merged.canSatisfy(TraversalDirection.FORWARD));
    assertTrue(merged.canSatisfy(TraversalDirection.REVERSE));
  }

  @Test
  public void isStrictImprovementOverDetectsNewCompleteDirection() {
    TraversalCoverage existing =
        TraversalCoverage.builder()
            .direction(
                DirectionCoverage.builder()
                    .direction(TraversalDirection.FORWARD)
                    .explored(true)
                    .exploredDepth(1)
                    .configuredMaxDepth(5)
                    .complete(true)
                    .build())
            .build();

    TraversalCoverage improved =
        existing.withDirection(
            DirectionCoverage.builder()
                .direction(TraversalDirection.REVERSE)
                .explored(true)
                .exploredDepth(1)
                .configuredMaxDepth(5)
                .complete(true)
                .build());

    assertTrue(improved.isStrictImprovementOver(existing));
    assertFalse(existing.isStrictImprovementOver(improved));
  }
}
