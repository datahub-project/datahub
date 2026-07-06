package com.linkedin.metadata.usage;

import com.linkedin.data.template.StringMap;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * MCP header used to skip queue-path usage recording when GMS HTTP already billed the proposal.
 *
 * <p>Stored on {@link MetadataChangeProposal#getHeaders()} so dedup survives Kafka, pgQueue, and
 * direct MCP producers without transport-specific record headers.
 */
public final class UsageDedupHeaders {

  public static final String USAGE_PRE_RECORDED = "X-DataHub-Usage-PreRecorded";

  private static final String PRE_RECORDED_VALUE = "1";

  private UsageDedupHeaders() {}

  public static boolean isPreRecorded(@Nullable MetadataChangeProposal mcp) {
    return isPreRecorded(mcp != null && mcp.hasHeaders() ? mcp.getHeaders() : null);
  }

  public static boolean isPreRecorded(@Nullable Map<String, String> headers) {
    if (headers == null || headers.isEmpty()) {
      return false;
    }
    return headers.entrySet().stream()
        .filter(entry -> USAGE_PRE_RECORDED.equalsIgnoreCase(entry.getKey()))
        .map(Map.Entry::getValue)
        .anyMatch(PRE_RECORDED_VALUE::equals);
  }

  public static void stampPreRecorded(@Nonnull MetadataChangeProposal mcp) {
    if (mcp.hasHeaders() && mcp.getHeaders() != null) {
      mcp.getHeaders().put(USAGE_PRE_RECORDED, PRE_RECORDED_VALUE);
      return;
    }
    mcp.setHeaders(new StringMap(Map.of(USAGE_PRE_RECORDED, PRE_RECORDED_VALUE)));
  }

  /** Mutable copy helper for tests. */
  @Nonnull
  static Map<String, String> withPreRecorded(@Nullable Map<String, String> headers) {
    Map<String, String> merged = headers == null ? new HashMap<>() : new HashMap<>(headers);
    merged.put(USAGE_PRE_RECORDED, PRE_RECORDED_VALUE);
    return merged;
  }
}
