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
import org.testng.annotations.Test;

public class IngestionSourceRunEventsTest {

  private static final Urn RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run");
  private static final ExecutionRequestInput INPUT =
      new ExecutionRequestInput()
          .setTask("RUN_INGEST")
          .setExecutorId("default")
          .setRequestedAt(100L)
          .setArgs(new StringMap());

  @Test
  public void testRunWithoutResultIsPending() {
    DataHubIngestionSourceRunEvent event =
        IngestionSourceRunEvents.buildEvent(RUN, INPUT, null, 5L, null);

    assertEquals(event.getStatus(), IngestionSourceRunEvents.PENDING_STATUS);
    assertEquals(event.getRequestedAtMillis(), 100L);
    assertEquals(event.getTimestampMillis(), 5L);
    assertEquals(event.getMessageId(), RUN + ":PENDING");
    assertFalse(event.hasSuccessfulRunRequestedAtMillis());
    assertFalse(event.hasFailedRunRequestedAtMillis());
  }

  @Test
  public void testOnlyTerminalOutcomesSetTheirTimestamp() {
    DataHubIngestionSourceRunEvent success =
        IngestionSourceRunEvents.buildEvent(
            RUN, INPUT, new ExecutionRequestResult().setStatus("SUCCESS"), 5L, null);
    DataHubIngestionSourceRunEvent failure =
        IngestionSourceRunEvents.buildEvent(
            RUN, INPUT, new ExecutionRequestResult().setStatus("FAILURE"), 5L, null);

    assertEquals(success.getSuccessfulRunRequestedAtMillis(), Long.valueOf(100L));
    assertFalse(success.hasFailedRunRequestedAtMillis());
    assertEquals(failure.getFailedRunRequestedAtMillis(), Long.valueOf(100L));
    assertFalse(failure.hasSuccessfulRunRequestedAtMillis());
  }

  @Test
  public void testLastOutcomesCarryForwardFromThePreviousEvent() {
    // The index keeps only the newest event per source in a batch, so each event must hold the
    // last success and failure times itself.
    DataHubIngestionSourceRunEvent previous =
        new DataHubIngestionSourceRunEvent()
            .setTimestampMillis(1L)
            .setExecutionRequest(RUN)
            .setRequestedAtMillis(80L)
            .setStatus("SUCCESS")
            .setSuccessfulRunRequestedAtMillis(80L)
            .setFailedRunRequestedAtMillis(60L);

    DataHubIngestionSourceRunEvent failure =
        IngestionSourceRunEvents.buildEvent(
            RUN, INPUT, new ExecutionRequestResult().setStatus("FAILURE"), 5L, previous);
    DataHubIngestionSourceRunEvent pending =
        IngestionSourceRunEvents.buildEvent(RUN, INPUT, null, 5L, previous);

    assertEquals(failure.getSuccessfulRunRequestedAtMillis(), Long.valueOf(80L));
    assertEquals(failure.getFailedRunRequestedAtMillis(), Long.valueOf(100L));
    assertEquals(pending.getSuccessfulRunRequestedAtMillis(), Long.valueOf(80L));
    assertEquals(pending.getFailedRunRequestedAtMillis(), Long.valueOf(60L));
  }

  @Test
  public void testPendingPreviousEventCarriesNothing() {
    DataHubIngestionSourceRunEvent pending =
        IngestionSourceRunEvents.buildEvent(RUN, INPUT, null, 1L, null);

    DataHubIngestionSourceRunEvent success =
        IngestionSourceRunEvents.buildEvent(
            RUN, INPUT, new ExecutionRequestResult().setStatus("SUCCESS"), 5L, pending);

    assertEquals(success.getSuccessfulRunRequestedAtMillis(), Long.valueOf(100L));
    assertFalse(success.hasFailedRunRequestedAtMillis());
  }

  @Test
  public void testOnlyALaterRecordedRunSupersedes() {
    DataHubIngestionSourceRunEvent later =
        new DataHubIngestionSourceRunEvent()
            .setTimestampMillis(1L)
            .setExecutionRequest(RUN)
            .setRequestedAtMillis(200L)
            .setStatus("PENDING");

    assertTrue(IngestionSourceRunEvents.isSupersededBy(INPUT, later));
    assertFalse(IngestionSourceRunEvents.isSupersededBy(INPUT, later.setRequestedAtMillis(100L)));
    assertFalse(IngestionSourceRunEvents.isSupersededBy(INPUT, null));
  }

  @Test
  public void testStatusTimeUsesCompletionWhenKnown() {
    ExecutionRequestResult finished =
        new ExecutionRequestResult().setStatus("SUCCESS").setStartTimeMs(200L).setDurationMs(50L);

    assertEquals(IngestionSourceRunEvents.statusTimeMillis(INPUT, finished), 250L);
    assertEquals(
        IngestionSourceRunEvents.statusTimeMillis(
            INPUT, new ExecutionRequestResult().setStatus("RUNNING")),
        100L);
  }
}
