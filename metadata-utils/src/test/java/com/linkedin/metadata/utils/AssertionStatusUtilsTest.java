package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.assertion.AssertionStatus;
import org.testng.annotations.Test;

public class AssertionStatusUtilsTest {

  @Test
  public void testMapsRunResultsAndMonitorErrors() {
    assertEquals(
        AssertionStatusUtils.resolveStatus(false, AssertionResultType.SUCCESS),
        AssertionStatus.PASSING);
    assertEquals(
        AssertionStatusUtils.resolveStatus(false, AssertionResultType.FAILURE),
        AssertionStatus.FAILING);
    assertEquals(
        AssertionStatusUtils.resolveStatus(true, AssertionResultType.SUCCESS),
        AssertionStatus.ERROR);
    assertEquals(
        AssertionStatusUtils.resolveStatus(false, AssertionResultType.ERROR),
        AssertionStatus.ERROR);
    assertEquals(
        AssertionStatusUtils.resolveStatus(false, AssertionResultType.INIT), AssertionStatus.INIT);
    assertNull(AssertionStatusUtils.resolveStatus(false, (AssertionResultType) null));
  }

  @Test
  public void testUsesMostRecentSummaryTimestamp() {
    AssertionRunSummary summary =
        new AssertionRunSummary().setLastPassedAtMillis(100L).setLastFailedAtMillis(200L);

    assertEquals(AssertionStatusUtils.resolveStatus(false, summary), AssertionStatus.FAILING);
  }

  @Test
  public void testUsesEverySummaryTimestamp() {
    AssertionRunSummary summary =
        new AssertionRunSummary()
            .setLastFailedAtMillis(100L)
            .setLastErroredAtMillis(200L)
            .setLastPassedAtMillis(300L)
            .setLastInitializedAtMillis(400L);

    assertEquals(AssertionStatusUtils.resolveStatus(false, summary), AssertionStatus.INIT);
    assertEquals(AssertionStatusUtils.resolveStatus(true, summary), AssertionStatus.ERROR);
    assertNull(AssertionStatusUtils.resolveStatus(false, (AssertionRunSummary) null));
  }
}
