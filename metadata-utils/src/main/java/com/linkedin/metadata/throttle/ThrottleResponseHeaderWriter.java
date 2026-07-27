package com.linkedin.metadata.throttle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ThrottleResponseHeaderWriter {

  private ThrottleResponseHeaderWriter() {}

  @Nonnull
  public static Map<String, String> createDenialHeaders(
      @Nullable String ruleId,
      @Nullable ThrottleMechanismType mechanismType,
      @Nullable ThrottleResponseSource source,
      long retryAfterMs) {
    Map<String, String> headers = new LinkedHashMap<>();
    applyDenial(headers, ruleId, mechanismType, source, retryAfterMs);
    return headers;
  }

  public static void applyDenial(
      @Nonnull Map<String, String> headers,
      @Nullable String ruleId,
      @Nullable ThrottleMechanismType mechanismType,
      @Nullable ThrottleResponseSource source,
      long retryAfterMs) {
    if (source != null) {
      headers.put(ThrottleResponseHeaders.SOURCE, source.headerValue());
    }
    if (ruleId != null && !ruleId.isBlank()) {
      headers.put(ThrottleResponseHeaders.RULE, ruleId);
    }
    if (mechanismType != null) {
      headers.put(ThrottleResponseHeaders.TYPE, mechanismType.headerValue());
    }
    if (retryAfterMs >= 0) {
      headers.put(
          ThrottleResponseHeaders.RETRY_AFTER,
          String.valueOf(TimeUnit.MILLISECONDS.toSeconds(retryAfterMs)));
    }
  }
}
