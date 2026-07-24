package com.linkedin.datahub.upgrade.impl;

import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Formats a human-grepable multi-line upgrade completion summary for on-call triage. */
final class UpgradeSummaryFormatter {

  private UpgradeSummaryFormatter() {}

  /** One step line in the upgrade summary. */
  static final class StepLine {
    final int index;
    final int total;
    final String name;
    final String status;
    final int retryCount;
    @Nullable final String cause;

    StepLine(
        int index,
        int total,
        @Nonnull String name,
        @Nonnull String status,
        int retryCount,
        @Nullable String cause) {
      this.index = index;
      this.total = total;
      this.name = name;
      this.status = status;
      this.retryCount = retryCount;
      this.cause = cause;
    }
  }

  /**
   * Builds the multi-line UPGRADE SUMMARY text.
   *
   * <p>Consecutive {@code NOT_EXECUTED} steps are collapsed to {@code Steps X-Y: NOT EXECUTED}.
   */
  static String format(
      @Nonnull String upgradeId,
      @Nonnull DataHubUpgradeState overall,
      @Nonnull List<StepLine> steps,
      long durationMs) {
    StringBuilder sb = new StringBuilder();
    sb.append("UPGRADE SUMMARY: ").append(upgradeId).append(' ').append(overall.name());

    int i = 0;
    while (i < steps.size()) {
      StepLine step = steps.get(i);
      if ("NOT_EXECUTED".equals(step.status)) {
        int start = step.index;
        int end = step.index;
        int j = i + 1;
        while (j < steps.size() && "NOT_EXECUTED".equals(steps.get(j).status)) {
          end = steps.get(j).index;
          j++;
        }
        sb.append('\n');
        if (start == end) {
          sb.append("  Step ")
              .append(start)
              .append('/')
              .append(step.total)
              .append(": NOT EXECUTED");
        } else {
          sb.append("  Steps ").append(start).append('-').append(end).append(": NOT EXECUTED");
        }
        i = j;
        continue;
      }

      sb.append('\n');
      sb.append("  Step ")
          .append(step.index)
          .append('/')
          .append(step.total)
          .append(": ")
          .append(step.name)
          .append(" - ")
          .append(step.status);
      if ("FAILED".equals(step.status)) {
        sb.append(" (").append(step.retryCount).append(" retries)");
      }
      if (step.cause != null && !step.cause.isBlank()) {
        String label = "FAILED".equals(step.status) ? "Cause" : "Note";
        sb.append('\n').append("    ").append(label).append(": ").append(step.cause);
      }
      i++;
    }

    sb.append('\n').append("  duration_ms=").append(durationMs);
    return sb.toString();
  }

  /**
   * Best-effort extraction of a failure cause for a step from upgrade report lines.
   *
   * <p>Looks for step-authored root-cause lines and manager-caught exception lines.
   */
  @Nullable
  static String findCauseForStep(@Nonnull String stepId, @Nonnull List<String> reportLines) {
    String best = null;
    String rootCausePrefix = stepId + " failed. Root cause:";
    String failurePrefix = stepId + " failed:";
    String caughtMarker = "Step with id " + stepId + ":";

    for (String line : reportLines) {
      if (line == null) {
        continue;
      }
      String trimmed = line.trim();
      if (trimmed.startsWith(rootCausePrefix)) {
        // Highest priority — stop scanning.
        best = extractAfter(trimmed, rootCausePrefix);
        break;
      } else if (trimmed.startsWith(failurePrefix)) {
        best = extractAfter(trimmed, failurePrefix);
      } else if (line.contains("Caught exception") && line.contains(caughtMarker)) {
        best = extractAfter(line, caughtMarker);
      }
    }
    return best != null && !best.isBlank() ? best.trim() : null;
  }

  static boolean isFailureOrAbort(@Nonnull DataHubUpgradeState state) {
    return Objects.equals(state, DataHubUpgradeState.FAILED)
        || Objects.equals(state, DataHubUpgradeState.ABORTED);
  }

  private static String extractAfter(String line, String marker) {
    int idx = line.indexOf(marker);
    if (idx < 0) {
      return line;
    }
    return line.substring(idx + marker.length()).trim();
  }
}
