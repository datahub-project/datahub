package com.linkedin.metadata.utils.elasticsearch;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.client.tasks.GetTaskResponse;

/**
 * High-level {@link GetTaskResponse} plus document failures parsed from the raw task API JSON that
 * the HLRC response type does not retain.
 */
public record TaskResultWithFailures(
    @Nonnull GetTaskResponse taskResponse,
    @Nonnull List<TaskFailureDetail> failures,
    int totalFailureCount,
    @Nullable String rawFallback) {

  public TaskResultWithFailures {
    failures = failures == null ? List.of() : failures;
    if (totalFailureCount < failures.size()) {
      totalFailureCount = failures.size();
    }
    Objects.requireNonNull(taskResponse, "taskResponse");
  }

  /** Convenience when total equals the (possibly uncapped) list size and no raw fallback. */
  public TaskResultWithFailures(
      @Nonnull GetTaskResponse taskResponse, @Nullable List<TaskFailureDetail> failures) {
    this(taskResponse, failures, failures == null ? 0 : failures.size(), null);
  }

  /** Convenience when total is known and there is no raw fallback. */
  public TaskResultWithFailures(
      @Nonnull GetTaskResponse taskResponse,
      @Nullable List<TaskFailureDetail> failures,
      int totalFailureCount) {
    this(taskResponse, failures, totalFailureCount, null);
  }

  /** From {@link TaskFailureParser#parse(String)} so capped details / raw fallback are preserved. */
  public TaskResultWithFailures(
      @Nonnull GetTaskResponse taskResponse, @Nullable TaskFailureParseResult parseResult) {
    this(
        taskResponse,
        parseResult == null ? List.of() : parseResult.details(),
        parseResult == null ? 0 : parseResult.totalCount(),
        parseResult == null ? null : parseResult.rawFallback());
  }

  @Nonnull
  public TaskFailureParseResult failureParse() {
    return new TaskFailureParseResult(failures, totalFailureCount, rawFallback);
  }
}
