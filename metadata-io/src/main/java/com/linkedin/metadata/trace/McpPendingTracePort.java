package com.linkedin.metadata.trace;

import com.linkedin.common.urn.Urn;
import io.datahubproject.openapi.v1.models.TraceStorageStatus;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Transport-neutral MCP log scan for async trace (Kafka {@link MCPTraceReader} or pgQueue). */
public interface McpPendingTracePort {

  @Nonnull
  Map<Urn, Map<String, TraceStorageStatus>> tracePendingStatuses(
      @Nonnull Map<Urn, List<String>> urnAspectPairs,
      @Nonnull String traceId,
      @Nullable Long traceTimestampMillis);

  @Nonnull
  Map<Urn, Map<String, TraceStorageStatus>> tracePendingStatuses(
      @Nonnull Map<Urn, List<String>> urnAspectPairs,
      @Nonnull String traceId,
      @Nullable Long traceTimestampMillis,
      boolean skipCache);
}
