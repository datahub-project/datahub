package io.datahubproject.metadata.context;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Closed vocabulary of agent classifications used for usage rollup bucketing.
 *
 * <p>The UAA parser emits free-form strings; rollup keys must be drawn from this fixed enum so that
 * downstream metric filters can be configured against stable values. Any parser output that does
 * not map to a known value falls through to {@link #UNKNOWN}. {@link #SYSTEM} is reserved for
 * ingestion paths with no request context (e.g., internal system operations, consumer replays).
 */
public enum AgentClass {
  BROWSER(true),
  MOBILE_APP(true),
  CLI(false),
  INGESTION(false),
  SDK(false),
  ROBOT(false),
  CRAWLER(false),
  LIBRARY(false),
  EMAIL_CLIENT(false),
  HACKER(false),
  SYSTEM(false),
  UNKNOWN(false);

  private final boolean human;

  AgentClass(boolean human) {
    this.human = human;
  }

  public boolean isHuman() {
    return human;
  }

  /**
   * Render this value as a metric label. Compact, lowercase, no separators — preserves the legacy
   * raw-UAA-string label format (e.g. {@code "Mobile App"} → {@code "mobileapp"}, {@code "Email
   * Client"} → {@code "emailclient"}) so existing dashboards and alerts continue to match.
   */
  @Nonnull
  public String toMetricLabel() {
    return name().toLowerCase().replace("_", "");
  }

  /**
   * Map a raw UAA agent-class string (e.g., {@code "Browser"}, {@code "Mobile App"}, {@code
   * "INGESTION"}) to a canonical {@link AgentClass}. Returns {@link #UNKNOWN} for null, empty, or
   * unrecognized inputs. Case-insensitive; whitespace is collapsed to underscore.
   */
  @Nonnull
  public static AgentClass fromRawUserAgentClass(@Nullable String raw) {
    if (raw == null || raw.isEmpty()) {
      return UNKNOWN;
    }
    String normalized = raw.trim().toUpperCase().replaceAll("\\s+", "_");
    try {
      return AgentClass.valueOf(normalized);
    } catch (IllegalArgumentException e) {
      return UNKNOWN;
    }
  }
}
