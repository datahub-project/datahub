package com.linkedin.metadata.service.async.delete;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import org.testng.annotations.Test;

public class DeleteEntityReportTest {
  private static final String URN = "urn:li:tag:my_tag";

  @Test
  public void everyOutcomeRoundTrips() {
    for (ConditionalDeleteOutcome outcome : ConditionalDeleteOutcome.values()) {
      final DeleteEntityReport report = new DeleteEntityReport(URN, outcome, 12L, 3L, 2);

      assertEquals(DeleteEntityReport.fromJson(report.toJson()), report);
    }
  }

  @Test
  public void alreadyDeletedReportsNothingDone() {
    assertEquals(
        DeleteEntityReport.alreadyDeleted(UrnUtils.getUrn(URN)),
        new DeleteEntityReport(URN, ConditionalDeleteOutcome.ALREADY_DELETED, 0L, 0L, 0));
  }

  @Test
  public void badReportsAreRejected() {
    expectThrows(IllegalArgumentException.class, () -> DeleteEntityReport.fromJson("not json"));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            DeleteEntityReport.fromJson(
                "{\"urn\":\""
                    + URN
                    + "\",\"outcome\":\"CHANGED\",\"rowsDeleted\":0,"
                    + "\"timeseriesRowsDeleted\":0,\"referencesRemoved\":0}"));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            DeleteEntityReport.fromJson(
                "{\"outcome\":\"DELETED\",\"rowsDeleted\":0,\"timeseriesRowsDeleted\":0,"
                    + "\"referencesRemoved\":0}"));
  }
}
