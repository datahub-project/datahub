package io.datahubproject.openlineage.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Value;

/**
 * Body of a {@code /lineage/batch} response.
 *
 * <p>This package is the {@code modelPackage} the OpenAPI generator is configured with, and the
 * generated {@code LineageApi} imports this class by that name. It is written by hand because the
 * generator is configured to emit apis only, so a {@code $ref} in the spec resolves to a type that
 * has to exist here already.
 *
 * <p>Field names are the OpenLineage spec's own. They are snake_case, so a producer written against
 * the spec can read the outcome without a DataHub-shaped mapping.
 *
 * <p>Every failure this endpoint reports is a conversion failure, which is deterministic — the same
 * bytes fail the same way — so {@code retriable} is always false and {@code retriable} in the
 * summary is always zero. A failure that a resend could fix is a server fault, and those surface as
 * a 500 for the whole request rather than as a per-event entry.
 */
@Value
public class LineageBatchResult {
  public static final String STATUS_SUCCESS = "success";
  public static final String STATUS_PARTIAL_SUCCESS = "partial_success";

  String status;
  Summary summary;

  @JsonProperty("failed_events")
  List<FailedEvent> failedEvents;

  public static LineageBatchResult of(int received, List<FailedEvent> failedEvents) {
    int failed = failedEvents.size();
    int retriable = (int) failedEvents.stream().filter(FailedEvent::isRetriable).count();
    return new LineageBatchResult(
        failed == 0 ? STATUS_SUCCESS : STATUS_PARTIAL_SUCCESS,
        new Summary(received, received - failed, failed, retriable, failed - retriable),
        failedEvents);
  }

  @Value
  public static class Summary {
    int received;
    int successful;
    int failed;
    int retriable;

    @JsonProperty("non_retriable")
    int nonRetriable;
  }

  @Value
  public static class FailedEvent {
    /** Position in the request array, so the producer can identify the event it sent. */
    int index;

    String reason;
    boolean retriable;
  }
}
