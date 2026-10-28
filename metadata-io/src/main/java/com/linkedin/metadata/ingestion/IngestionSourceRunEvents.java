package com.linkedin.metadata.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Builds {@code dataHubIngestionSourceRunEvent} aspects from an execution request. */
public final class IngestionSourceRunEvents {

  /** Status of a run the executor has not reported a result for yet, as shown in the UI. */
  public static final String PENDING_STATUS = "PENDING";

  public static final String SUCCESS_STATUS = "SUCCESS";
  public static final String FAILURE_STATUS = "FAILURE";

  private IngestionSourceRunEvents() {}

  @Nullable
  public static Urn ingestionSourceUrn(@Nullable ExecutionRequestInput input) {
    return input != null && input.hasSource() && input.getSource().hasIngestionSource()
        ? input.getSource().getIngestionSource()
        : null;
  }

  @Nonnull
  public static String status(@Nullable ExecutionRequestResult result) {
    return result != null ? result.getStatus() : PENDING_STATUS;
  }

  /**
   * The time a run reached its current status: start plus duration once the executor reports both,
   * otherwise the request time. Used where the status change itself was not observed.
   */
  public static long statusTimeMillis(
      @Nonnull ExecutionRequestInput input, @Nullable ExecutionRequestResult result) {
    return result != null && result.hasStartTimeMs() && result.hasDurationMs()
        ? result.getStartTimeMs() + result.getDurationMs()
        : input.getRequestedAt();
  }

  @Nonnull
  public static DataHubIngestionSourceRunEvent buildEvent(
      @Nonnull Urn executionRequestUrn,
      @Nonnull ExecutionRequestInput input,
      @Nullable ExecutionRequestResult result,
      long timestampMillis) {
    String status = status(result);
    DataHubIngestionSourceRunEvent event =
        new DataHubIngestionSourceRunEvent()
            .setTimestampMillis(timestampMillis)
            // One document per run and status, so a replayed event overwrites itself.
            .setMessageId(executionRequestUrn + ":" + status)
            .setExecutionRequest(executionRequestUrn)
            .setRequestedAtMillis(input.getRequestedAt())
            .setStatus(status);
    if (SUCCESS_STATUS.equals(status)) {
      event.setSuccessfulRunRequestedAtMillis(input.getRequestedAt());
    } else if (FAILURE_STATUS.equals(status)) {
      event.setFailedRunRequestedAtMillis(input.getRequestedAt());
    }
    return event;
  }
}
