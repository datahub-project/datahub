package com.linkedin.metadata.ingestion;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class IngestionSourceRunSummaryUtils {

  private IngestionSourceRunSummaryUtils() {}

  @Nonnull
  public static DataHubIngestionSourceRunSummary buildSummary(
      @Nonnull Urn executionRequestUrn,
      @Nonnull ExecutionRequestInput input,
      @Nullable ExecutionRequestResult result) {
    return new DataHubIngestionSourceRunSummary()
        .setLastExecutionRequest(executionRequestUrn)
        .setLastRunRequestedAtMillis(input.getRequestedAt())
        .setLastRunStatus(result != null ? result.getStatus() : null, SetMode.IGNORE_NULL);
  }

  /**
   * Equal timestamps keep the existing summary so two runs requested in the same millisecond cannot
   * flip it back and forth.
   */
  public static boolean shouldReplace(
      @Nullable DataHubIngestionSourceRunSummary existing,
      @Nonnull DataHubIngestionSourceRunSummary candidate) {
    if (existing == null) {
      return true;
    }
    return Objects.equals(existing.getLastExecutionRequest(), candidate.getLastExecutionRequest())
        ? !existing.equals(candidate)
        : candidate.getLastRunRequestedAtMillis() > existing.getLastRunRequestedAtMillis();
  }
}
