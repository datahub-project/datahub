package com.linkedin.metadata.ingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

public class IngestionSourceRunEventsTest {

  private static final Urn RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run");
  private static final Urn OTHER_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:other");

  @Test
  public void testEventHoldsFactsAboutTheRun() {
    DataHubIngestionSourceRunEvent pending =
        IngestionSourceRunEvents.buildEvent(RUN, input(100L), null, "mysql");
    assertEquals(pending.getStatus(), IngestionSourceRunEvents.PENDING_STATUS);
    assertEquals(pending.getTimestampMillis(), 100L);
    assertEquals(pending.getMessageId(), RUN + ":PENDING");
    assertEquals(pending.getSourceType(), "mysql");
    assertEquals(pending.getExecutorId(), "default");

    // Every writer derives the same timestamp, so the same run and status is one document.
    DataHubIngestionSourceRunEvent finished =
        IngestionSourceRunEvents.buildEvent(
            RUN,
            input(100L),
            new ExecutionRequestResult()
                .setStatus("SUCCESS")
                .setStartTimeMs(110L)
                .setDurationMs(5L),
            null);
    assertEquals(finished.getTimestampMillis(), 115L);
    assertFalse(finished.hasSourceType());
  }

  @Test
  public void testSummaryClassifiesStatuses() {
    assertEquals(
        summary(RUN, 100L, "SUCCESS").getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(100L));
    for (String failed : new String[] {"FAILURE", "TIMEOUT", "ABORTED", "ROLLBACK_FAILED"}) {
      DataHubIngestionSourceRunSummary summary = summary(RUN, 100L, failed);
      assertEquals(summary.getLastFailedRunRequestedAtMillis(), Long.valueOf(100L), failed);
      assertFalse(summary.hasLastSuccessfulRunRequestedAtMillis(), failed);
    }
    DataHubIngestionSourceRunSummary cancelled = summary(RUN, 100L, "CANCELLED");
    assertFalse(cancelled.hasLastFailedRunRequestedAtMillis());
    assertFalse(cancelled.hasLastSuccessfulRunRequestedAtMillis());
  }

  @Test
  public void testFirstSummaryIsKeptAsProposed() {
    DataHubIngestionSourceRunSummary proposed = summary(RUN, 100L, "SUCCESS");

    assertFalse(IngestionSourceRunEvents.merge(null, proposed));
    assertEquals(proposed, summary(RUN, 100L, "SUCCESS"));
  }

  @Test
  public void testOlderRunKeepsTheLatestRunButRecordsItsOutcome() {
    DataHubIngestionSourceRunSummary stored = withTimes(summary(RUN, 200L, "RUNNING"), 150L, null);
    DataHubIngestionSourceRunSummary proposed = summary(OTHER_RUN, 100L, "FAILURE");

    assertTrue(IngestionSourceRunEvents.merge(stored, proposed));

    assertEquals(proposed.getLastExecutionRequest(), RUN);
    assertEquals(proposed.getLastRunRequestedAtMillis(), 200L);
    assertEquals(proposed.getLastRunStatus(), "RUNNING");
    assertEquals(proposed.getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(150L));
    assertEquals(proposed.getLastFailedRunRequestedAtMillis(), Long.valueOf(100L));
  }

  @Test
  public void testNewStatusOfTheLatestRunReplacesIt() {
    DataHubIngestionSourceRunSummary stored = withTimes(summary(RUN, 200L, "RUNNING"), 150L, 120L);
    DataHubIngestionSourceRunSummary proposed = summary(RUN, 200L, "SUCCESS");

    IngestionSourceRunEvents.merge(stored, proposed);

    assertEquals(proposed.getLastRunStatus(), "SUCCESS");
    assertEquals(proposed.getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(200L));
    assertEquals(proposed.getLastFailedRunRequestedAtMillis(), Long.valueOf(120L));
  }

  @Test
  public void testOutcomeTimesNeverMoveBackwards() {
    DataHubIngestionSourceRunSummary stored = withTimes(summary(RUN, 200L, "SUCCESS"), 200L, 180L);
    DataHubIngestionSourceRunSummary proposed =
        withTimes(summary(OTHER_RUN, 100L, "SUCCESS"), 100L, 90L);

    IngestionSourceRunEvents.merge(stored, proposed);

    assertEquals(proposed, stored);
  }

  @Test
  public void testRollbackOfTheLastSuccessTakesTheWritersReplacement() {
    DataHubIngestionSourceRunSummary stored = withTimes(summary(RUN, 200L, "SUCCESS"), 200L, null);

    DataHubIngestionSourceRunSummary replaced =
        withTimes(summary(RUN, 200L, "ROLLED_BACK"), 150L, null);
    IngestionSourceRunEvents.merge(stored, replaced);
    assertEquals(replaced.getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(150L));

    DataHubIngestionSourceRunSummary none = summary(RUN, 200L, "ROLLED_BACK");
    IngestionSourceRunEvents.merge(stored, none);
    assertFalse(none.hasLastSuccessfulRunRequestedAtMillis());
  }

  @Test
  public void testRollbackOfAnOlderRunKeepsTheLastSuccess() {
    DataHubIngestionSourceRunSummary stored = withTimes(summary(RUN, 200L, "SUCCESS"), 200L, null);
    DataHubIngestionSourceRunSummary proposed = summary(OTHER_RUN, 100L, "ROLLED_BACK");

    IngestionSourceRunEvents.merge(stored, proposed);

    assertEquals(proposed, stored);
  }

  private static DataHubIngestionSourceRunSummary summary(
      Urn run, long requestedAt, String status) {
    return IngestionSourceRunEvents.buildSummary(
        run, input(requestedAt), new ExecutionRequestResult().setStatus(status));
  }

  private static DataHubIngestionSourceRunSummary withTimes(
      DataHubIngestionSourceRunSummary summary, @Nullable Long success, @Nullable Long failure) {
    summary.removeLastSuccessfulRunRequestedAtMillis();
    summary.removeLastFailedRunRequestedAtMillis();
    if (success != null) {
      summary.setLastSuccessfulRunRequestedAtMillis(success);
    }
    if (failure != null) {
      summary.setLastFailedRunRequestedAtMillis(failure);
    }
    return summary;
  }

  private static ExecutionRequestInput input(long requestedAt) {
    return new ExecutionRequestInput()
        .setTask("RUN_INGEST")
        .setExecutorId("default")
        .setRequestedAt(requestedAt)
        .setArgs(new StringMap());
  }
}
