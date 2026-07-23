package com.linkedin.datahub.upgrade.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.List;
import org.testng.annotations.Test;

public class UpgradeSummaryFormatterTest {

  @Test
  public void testFormatFailedWithCauseAndNotExecuted() {
    List<UpgradeSummaryFormatter.StepLine> steps =
        List.of(
            new UpgradeSummaryFormatter.StepLine(
                1, 5, "ScaleDownEvaluationStep", "SUCCEEDED", 0, null),
            new UpgradeSummaryFormatter.StepLine(
                2,
                5,
                "BuildIndicesStep",
                "FAILED",
                0,
                "Reindex dataproductindex_v2 failed. Doc count 500 != 498."),
            new UpgradeSummaryFormatter.StepLine(3, 5, "StepThree", "NOT_EXECUTED", 0, null),
            new UpgradeSummaryFormatter.StepLine(4, 5, "StepFour", "NOT_EXECUTED", 0, null),
            new UpgradeSummaryFormatter.StepLine(5, 5, "StepFive", "NOT_EXECUTED", 0, null));

    String summary =
        UpgradeSummaryFormatter.format("SystemUpdate", DataHubUpgradeState.FAILED, steps, 12345L);

    String expected =
        String.join(
            "\n",
            "UPGRADE SUMMARY: SystemUpdate FAILED",
            "  Step 1/5: ScaleDownEvaluationStep - SUCCEEDED",
            "  Step 2/5: BuildIndicesStep - FAILED (0 retries)",
            "    Cause: Reindex dataproductindex_v2 failed. Doc count 500 != 498.",
            "  Steps 3-5: NOT EXECUTED",
            "  duration_ms=12345");
    assertEquals(summary, expected);
  }

  @Test
  public void testFormatSucceeded() {
    List<UpgradeSummaryFormatter.StepLine> steps =
        List.of(
            new UpgradeSummaryFormatter.StepLine(1, 2, "StepA", "SUCCEEDED", 0, null),
            new UpgradeSummaryFormatter.StepLine(2, 2, "StepB", "SUCCEEDED", 1, null));

    String summary =
        UpgradeSummaryFormatter.format("MyUpgrade", DataHubUpgradeState.SUCCEEDED, steps, 100L);

    assertTrue(summary.startsWith("UPGRADE SUMMARY: MyUpgrade SUCCEEDED"));
    assertTrue(summary.contains("Step 1/2: StepA - SUCCEEDED"));
    assertTrue(summary.contains("Step 2/2: StepB - SUCCEEDED"));
    assertTrue(summary.contains("duration_ms=100"));
  }

  @Test
  public void testFindCauseForStepPrefersRootCauseLine() {
    List<String> lines =
        List.of(
            "Caught exception during attempt 0 of Step with id BuildIndicesStep: wrapper",
            "BuildIndicesStep failed. Root cause: [DocCountMismatchException] Doc count 500 != 498.");

    String cause = UpgradeSummaryFormatter.findCauseForStep("BuildIndicesStep", lines);
    assertEquals(cause, "[DocCountMismatchException] Doc count 500 != 498.");
  }

  @Test
  public void testFindCauseForStepMissing() {
    assertNull(UpgradeSummaryFormatter.findCauseForStep("MissingStep", List.of("unrelated")));
  }

  @Test
  public void testFormatSucceededWithWarningsShowsWarningLabel() {
    List<UpgradeSummaryFormatter.StepLine> steps =
        List.of(
            new UpgradeSummaryFormatter.StepLine(
                1,
                1,
                "BuildIndicesStep",
                UpgradeSummaryFormatter.SUCCEEDED_WITH_WARNINGS,
                0,
                "index foo reindexed with document failures: documentFailures=2"));

    String summary =
        UpgradeSummaryFormatter.format("SystemUpdate", DataHubUpgradeState.SUCCEEDED, steps, 50L);

    assertTrue(
        summary.contains(
            "Step 1/1: BuildIndicesStep - " + UpgradeSummaryFormatter.SUCCEEDED_WITH_WARNINGS));
    assertTrue(
        summary.contains(
            "    Warning: index foo reindexed with document failures: documentFailures=2"));
    // Overall upgrade result stays SUCCEEDED (soft-pass).
    assertTrue(summary.startsWith("UPGRADE SUMMARY: SystemUpdate SUCCEEDED"));
  }

  @Test
  public void testFindSoftPassNoteForStep() {
    List<String> lines =
        List.of("BuildIndicesStep: index foo reindexed with document failures: documentFailures=1");

    String note = UpgradeSummaryFormatter.findSoftPassNoteForStep("BuildIndicesStep", lines);
    assertEquals(note, "index foo reindexed with document failures: documentFailures=1");
    // Full cause scanner ignores soft-pass lines (succeeded path uses soft helper only).
    assertNull(UpgradeSummaryFormatter.findCauseForStep("BuildIndicesStep", lines));
  }

  @Test
  public void testFindCauseForStepBreaksOnRootCause() {
    List<String> lines =
        List.of(
            "BuildIndicesStep failed. Root cause: first",
            "BuildIndicesStep failed. Root cause: second-should-not-win");

    assertEquals(UpgradeSummaryFormatter.findCauseForStep("BuildIndicesStep", lines), "first");
  }
}
