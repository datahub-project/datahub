package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;

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
  }

  @Test
  public void testUsesMostRecentSummaryTimestamp() {
    AssertionRunSummary summary =
        new AssertionRunSummary().setLastPassedAtMillis(100L).setLastFailedAtMillis(200L);

    assertEquals(AssertionStatusUtils.resolveStatus(false, summary), AssertionStatus.FAILING);
  }
}
