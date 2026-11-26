package com.linkedin.metadata.ingestion;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_ABORTED;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_FAILURE;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_ROLLED_BACK;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_ROLLING_BACK;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_SUCCESS;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_STATUS_TIMEOUT;

import com.linkedin.common.urn.Urn;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Builds {@code dataHubIngestionSourceRunEvent} and {@code dataHubIngestionSourceRunSummary}
 * aspects from an execution request, and decides which statuses count as a success or a failure.
 */
public final class IngestionSourceRunEvents {

  /** Status of a run the executor has not reported a result for yet, as shown in the UI. */
  public static final String PENDING_STATUS = "PENDING";

  public static final Set<String> SUCCESS_STATUSES = Set.of(EXECUTION_REQUEST_STATUS_SUCCESS);

  /** Terminal errors. CANCELLED is left out because a user chose to stop the run. */
  public static final Set<String> FAILURE_STATUSES =
      Set.of(
          EXECUTION_REQUEST_STATUS_FAILURE,
          EXECUTION_REQUEST_STATUS_TIMEOUT,
          EXECUTION_REQUEST_STATUS_ABORTED,
          EXECUTION_REQUEST_STATUS_ROLLBACK_FAILED);

  /** Statuses a rollback writes onto an earlier, already finished run. */
  public static final Set<String> ROLLBACK_STATUSES =
      Set.of(EXECUTION_REQUEST_STATUS_ROLLING_BACK, EXECUTION_REQUEST_STATUS_ROLLED_BACK);

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
   * The time a run reached its current status, derived from the request alone so that every writer
   * gives the same event the same timestamp, and so the same timeseries document.
   */
  public static long statusTimeMillis(
      @Nonnull ExecutionRequestInput input, @Nullable ExecutionRequestResult result) {
    if (result != null && result.hasStartTimeMs()) {
      return result.hasDurationMs()
          ? result.getStartTimeMs() + result.getDurationMs()
          : result.getStartTimeMs();
    }
    return input.getRequestedAt();
  }

  @Nonnull
  public static DataHubIngestionSourceRunEvent buildEvent(
      @Nonnull Urn executionRequestUrn,
      @Nonnull ExecutionRequestInput input,
      @Nullable ExecutionRequestResult result,
      @Nullable String sourceType) {
    String status = status(result);
    DataHubIngestionSourceRunEvent event =
        new DataHubIngestionSourceRunEvent()
            .setTimestampMillis(statusTimeMillis(input, result))
            // One document per run and status, so a replayed event overwrites itself.
            .setMessageId(executionRequestUrn + ":" + status)
            .setExecutionRequest(executionRequestUrn)
            .setRequestedAtMillis(input.getRequestedAt())
            .setStatus(status);
    if (sourceType != null) {
      event.setSourceType(sourceType);
    }
    if (input.hasExecutorId()) {
      event.setExecutorId(input.getExecutorId());
    }
    return event;
  }

  /**
   * The summary this run implies on its own. It is merged with the stored summary on write (see
   * {@link #merge}), so it only needs to describe this run.
   */
  @Nonnull
  public static DataHubIngestionSourceRunSummary buildSummary(
      @Nonnull Urn executionRequestUrn,
      @Nonnull ExecutionRequestInput input,
      @Nullable ExecutionRequestResult result) {
    String status = status(result);
    long requestedAt = input.getRequestedAt();
    DataHubIngestionSourceRunSummary summary =
        new DataHubIngestionSourceRunSummary()
            .setLastExecutionRequest(executionRequestUrn)
            .setLastRunRequestedAtMillis(requestedAt)
            .setLastRunStatus(status);
    if (SUCCESS_STATUSES.contains(status)) {
      summary.setLastSuccessfulRunRequestedAtMillis(requestedAt);
    } else if (FAILURE_STATUSES.contains(status)) {
      summary.setLastFailedRunRequestedAtMillis(requestedAt);
    }
    return summary;
  }

  /**
   * Merges a proposed summary into the stored one. Runs of one source can be processed out of
   * order, so:
   *
   * <ul>
   *   <li>the latest run moves to the proposed run when it is the same request (a new status) or a
   *       newer one;
   *   <li>the last success and failure times only move forward;
   *   <li>except that when the proposed run is a rollback of the stored last success, its last
   *       success is the replacement the writer looked up, or absent when there is none.
   * </ul>
   *
   * @return true when {@code proposed} was changed
   */
  public static boolean merge(
      @Nullable DataHubIngestionSourceRunSummary stored,
      @Nonnull DataHubIngestionSourceRunSummary proposed) {
    if (stored == null) {
      return false;
    }
    DataHubIngestionSourceRunSummary before = copy(proposed);
    if (!isSameOrNewerRun(stored, proposed)) {
      proposed
          .setLastExecutionRequest(stored.getLastExecutionRequest())
          .setLastRunRequestedAtMillis(stored.getLastRunRequestedAtMillis())
          .setLastRunStatus(stored.getLastRunStatus());
    }
    boolean rollsBackStoredSuccess =
        ROLLBACK_STATUSES.contains(before.getLastRunStatus())
            && Objects.equals(
                stored.getLastSuccessfulRunRequestedAtMillis(),
                before.getLastRunRequestedAtMillis());
    if (!rollsBackStoredSuccess) {
      setOrRemove(
          proposed,
          true,
          later(
              stored.getLastSuccessfulRunRequestedAtMillis(),
              proposed.getLastSuccessfulRunRequestedAtMillis()));
    }
    setOrRemove(
        proposed,
        false,
        later(
            stored.getLastFailedRunRequestedAtMillis(),
            proposed.getLastFailedRunRequestedAtMillis()));
    return !before.equals(proposed);
  }

  private static boolean isSameOrNewerRun(
      @Nonnull DataHubIngestionSourceRunSummary stored,
      @Nonnull DataHubIngestionSourceRunSummary proposed) {
    if (stored.getLastExecutionRequest().equals(proposed.getLastExecutionRequest())) {
      return true;
    }
    int byTime =
        Long.compare(proposed.getLastRunRequestedAtMillis(), stored.getLastRunRequestedAtMillis());
    // Ties break on the urn so every writer agrees on the latest run.
    return byTime != 0
        ? byTime > 0
        : proposed
                .getLastExecutionRequest()
                .toString()
                .compareTo(stored.getLastExecutionRequest().toString())
            > 0;
  }

  private static void setOrRemove(
      @Nonnull DataHubIngestionSourceRunSummary summary, boolean success, @Nullable Long value) {
    if (success) {
      if (value != null) {
        summary.setLastSuccessfulRunRequestedAtMillis(value);
      } else {
        summary.removeLastSuccessfulRunRequestedAtMillis();
      }
    } else if (value != null) {
      summary.setLastFailedRunRequestedAtMillis(value);
    } else {
      summary.removeLastFailedRunRequestedAtMillis();
    }
  }

  @Nullable
  private static Long later(@Nullable Long a, @Nullable Long b) {
    if (a == null || b == null) {
      return a != null ? a : b;
    }
    return Long.valueOf(Math.max(a, b));
  }

  @Nonnull
  private static DataHubIngestionSourceRunSummary copy(
      @Nonnull DataHubIngestionSourceRunSummary summary) {
    try {
      return summary.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException("Failed to copy ingestion source run summary", e);
    }
  }
}
