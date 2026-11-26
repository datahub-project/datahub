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
import org.testng.annotations.Test;

public class IngestionSourceRunEventsTest {

  private static final Urn RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run");
  private static final Urn OTHER_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:other");
  private static final Urn SUCCESS_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:success");

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
    DataHubIngestionSourceRunSummary success = summary(RUN, 100L, "SUCCESS");
    assertEquals(success.getLastSuccessfulExecutionRequest(), RUN);
    assertEquals(success.getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(100L));
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
  public void testRedeliveredSummaryIsUnchanged() {
    DataHubIngestionSourceRunSummary stored =
        withFailure(withSuccess(summary(RUN, 200L, "SUCCESS"), RUN, 200L), 150L);
    DataHubIngestionSourceRunSummary redelivered = copy(stored);

    assertFalse(IngestionSourceRunEvents.merge(stored, redelivered));
    assertEquals(redelivered, stored);
  }

  @Test
  public void testOlderRunKeepsTheLatestRunButRecordsItsOutcome() {
    DataHubIngestionSourceRunSummary stored =
        withSuccess(summary(RUN, 200L, "RUNNING"), SUCCESS_RUN, 150L);
    DataHubIngestionSourceRunSummary proposed = summary(OTHER_RUN, 100L, "FAILURE");

    assertTrue(IngestionSourceRunEvents.merge(stored, proposed));

    assertEquals(proposed.getLastExecutionRequest(), RUN);
    assertEquals(proposed.getLastRunRequestedAtMillis(), 200L);
    assertEquals(proposed.getLastRunStatus(), "RUNNING");
    assertEquals(proposed.getLastSuccessfulExecutionRequest(), SUCCESS_RUN);
    assertEquals(proposed.getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(150L));
    assertEquals(proposed.getLastFailedRunRequestedAtMillis(), Long.valueOf(100L));
  }

  @Test
  public void testNewStatusOfTheLatestRunReplacesIt() {
    DataHubIngestionSourceRunSummary stored =
        withFailure(withSuccess(summary(RUN, 200L, "RUNNING"), SUCCESS_RUN, 150L), 120L);
    DataHubIngestionSourceRunSummary proposed = summary(RUN, 200L, "SUCCESS");

    IngestionSourceRunEvents.merge(stored, proposed);

    assertEquals(proposed.getLastRunStatus(), "SUCCESS");
    assertEquals(proposed.getLastSuccessfulExecutionRequest(), RUN);
    assertEquals(proposed.getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(200L));
    assertEquals(proposed.getLastFailedRunRequestedAtMillis(), Long.valueOf(120L));
  }

  @Test
  public void testRequestsAtTheSameTimeAreOrderedByUrn() {
    // OTHER_RUN's urn sorts before RUN's, so RUN is the latest whichever is stored.
    DataHubIngestionSourceRunSummary proposedLater = summary(RUN, 100L, "SUCCESS");
    IngestionSourceRunEvents.merge(summary(OTHER_RUN, 100L, "SUCCESS"), proposedLater);
    assertEquals(proposedLater.getLastExecutionRequest(), RUN);
    assertEquals(proposedLater.getLastSuccessfulExecutionRequest(), RUN);

    DataHubIngestionSourceRunSummary proposedEarlier = summary(OTHER_RUN, 100L, "SUCCESS");
    IngestionSourceRunEvents.merge(summary(RUN, 100L, "SUCCESS"), proposedEarlier);
    assertEquals(proposedEarlier.getLastExecutionRequest(), RUN);
    assertEquals(proposedEarlier.getLastSuccessfulExecutionRequest(), RUN);
  }

  @Test
  public void testOutcomeTimesNeverMoveBackwards() {
    DataHubIngestionSourceRunSummary stored =
        withFailure(withSuccess(summary(RUN, 200L, "SUCCESS"), RUN, 200L), 180L);
    DataHubIngestionSourceRunSummary proposed =
        withFailure(withSuccess(summary(OTHER_RUN, 100L, "SUCCESS"), OTHER_RUN, 100L), 90L);

    IngestionSourceRunEvents.merge(stored, proposed);

    assertEquals(proposed, stored);
  }

  @Test
  public void testRollbackOfTheLastSuccessTakesTheWritersReplacement() {
    DataHubIngestionSourceRunSummary stored = summary(RUN, 200L, "SUCCESS");

    DataHubIngestionSourceRunSummary replaced =
        withSuccess(summary(RUN, 200L, "ROLLED_BACK"), OTHER_RUN, 150L);
    IngestionSourceRunEvents.merge(stored, replaced);
    assertEquals(replaced.getLastSuccessfulExecutionRequest(), OTHER_RUN);
    assertEquals(replaced.getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(150L));

    DataHubIngestionSourceRunSummary none = summary(RUN, 200L, "ROLLED_BACK");
    IngestionSourceRunEvents.merge(stored, none);
    assertFalse(none.hasLastSuccessfulExecutionRequest());
    assertFalse(none.hasLastSuccessfulRunRequestedAtMillis());
  }

  @Test
  public void testRollbackOfAnotherRunWithTheSameRequestTimeKeepsTheLastSuccess() {
    // The last success is identified by its execution request, not its request time.
    DataHubIngestionSourceRunSummary stored = summary(RUN, 200L, "SUCCESS");
    DataHubIngestionSourceRunSummary proposed = summary(OTHER_RUN, 200L, "ROLLED_BACK");

    IngestionSourceRunEvents.merge(stored, proposed);

    assertEquals(proposed, stored);
  }

  private static DataHubIngestionSourceRunSummary summary(
      Urn run, long requestedAt, String status) {
    return IngestionSourceRunEvents.buildSummary(
        run, input(requestedAt), new ExecutionRequestResult().setStatus(status));
  }

  private static DataHubIngestionSourceRunSummary withSuccess(
      DataHubIngestionSourceRunSummary summary, Urn run, long requestedAt) {
    IngestionSourceRunEvents.setLastSuccess(summary, run, requestedAt);
    return summary;
  }

  private static DataHubIngestionSourceRunSummary withFailure(
      DataHubIngestionSourceRunSummary summary, long requestedAt) {
    return summary.setLastFailedRunRequestedAtMillis(requestedAt);
  }

  private static DataHubIngestionSourceRunSummary copy(DataHubIngestionSourceRunSummary summary) {
    try {
      return summary.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }

  private static ExecutionRequestInput input(long requestedAt) {
    return new ExecutionRequestInput()
        .setTask("RUN_INGEST")
        .setExecutorId("default")
        .setRequestedAt(requestedAt)
        .setArgs(new StringMap());
  }
}
