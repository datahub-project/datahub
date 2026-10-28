package com.linkedin.metadata.ingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

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
        IngestionSourceRunEvents.buildEvent(RUN, INPUT, null, 5L);

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
            RUN, INPUT, new ExecutionRequestResult().setStatus("SUCCESS"), 5L);
    DataHubIngestionSourceRunEvent failure =
        IngestionSourceRunEvents.buildEvent(
            RUN, INPUT, new ExecutionRequestResult().setStatus("FAILURE"), 5L);

    assertEquals(success.getSuccessfulRunRequestedAtMillis(), Long.valueOf(100L));
    assertFalse(success.hasFailedRunRequestedAtMillis());
    assertEquals(failure.getFailedRunRequestedAtMillis(), Long.valueOf(100L));
    assertFalse(failure.hasSuccessfulRunRequestedAtMillis());
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
