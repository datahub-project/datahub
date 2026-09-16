package com.linkedin.metadata.graph.elastic;

import org.opensearch.common.unit.TimeValue;

/** Timeout / keepAlive derivations for graph queries, kept pure so they can be unit-tested. */
public final class GraphQueryTimeouts {

  /** Extra slack beyond (timeout + drain) so a slice never loses its PIT right at the deadline. */
  static final int KEEP_ALIVE_MARGIN_SECONDS = 10;

  private GraphQueryTimeouts() {}

  /**
   * The PIT keepAlive must outlive the whole query budget plus the post-timeout slice drain, or
   * in-flight slices lose their search context near the deadline (search_context_missing). Derive
   * it from timeoutSeconds so the two can't drift out of sync: keep the configured value when it is
   * already long enough, otherwise raise it to timeout + drain + margin.
   *
   * @param configuredKeepAlive the configured keepAlive (e.g. "55s"), parsed as a {@link TimeValue}
   * @param timeoutSeconds the graph query wall-clock budget
   * @param drainSeconds the post-timeout slice drain budget (0 if unset)
   * @return a TimeValue-parseable keepAlive string that is at least timeout + drain + margin
   */
  public static String computeEffectiveKeepAlive(
      String configuredKeepAlive, long timeoutSeconds, int drainSeconds) {
    long minSeconds = timeoutSeconds + Math.max(0, drainSeconds) + KEEP_ALIVE_MARGIN_SECONDS;
    long configuredSeconds =
        TimeValue.parseTimeValue(configuredKeepAlive, "keepAlive").getMillis() / 1000L;
    return configuredSeconds >= minSeconds ? configuredKeepAlive : minSeconds + "s";
  }
}
