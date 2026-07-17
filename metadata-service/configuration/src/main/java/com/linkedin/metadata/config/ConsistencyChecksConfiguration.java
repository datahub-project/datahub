package com.linkedin.metadata.config;

import java.util.Map;
import lombok.Data;

/**
 * Configuration for consistency checks.
 *
 * <p>This is a top-level configuration that is shared by both:
 *
 * <ul>
 *   <li>The system update job (datahub-upgrade)
 *   <li>The OpenAPI consistency endpoints
 * </ul>
 *
 * <p>Loaded from application.yaml under the "consistencyChecks" section.
 *
 * <p>Example configuration:
 *
 * <pre>
 * consistencyChecks:
 *   checks:
 *     assertion-monitor-missing:
 *       defaultCronSchedule: "0 0 0 * * ?"
 *       defaultCronTimezone: "UTC"
 * </pre>
 */
@Data
public class ConsistencyChecksConfiguration {

  /**
   * Default grace period in seconds for API endpoints. Entities modified within this window are
   * excluded from checks to avoid false positives from eventual consistency. Default: 5 minutes
   * (300 seconds).
   */
  private long gracePeriodSeconds;

  /**
   * Per-check configuration, keyed by check ID. Values are strings; callers should coerce types.
   */
  private Map<String, Map<String, String>> checks;

  /**
   * Get configuration for a specific check.
   *
   * @param checkId the check ID
   * @return configuration map for the check, or empty map if not configured
   */
  public Map<String, String> getCheckConfig(String checkId) {
    if (checks == null) {
      return Map.of();
    }
    return checks.getOrDefault(checkId, Map.of());
  }

  /**
   * Get a string configuration value for a check.
   *
   * @param checkId the check ID
   * @param key the configuration key
   * @param defaultValue default value if not configured
   * @return the configuration value or default
   */
  public String getCheckString(String checkId, String key, String defaultValue) {
    Map<String, String> checkConfig = getCheckConfig(checkId);
    String value = checkConfig.get(key);
    return value != null ? value : defaultValue;
  }

  /**
   * Get a boolean configuration value for a check.
   *
   * @param checkId the check ID
   * @param key the configuration key
   * @param defaultValue default value if not configured
   * @return the configuration value or default
   */
  public boolean getCheckBoolean(String checkId, String key, boolean defaultValue) {
    Map<String, String> checkConfig = getCheckConfig(checkId);
    String value = checkConfig.get(key);
    return value != null ? Boolean.parseBoolean(value) : defaultValue;
  }

  /**
   * Get an integer configuration value for a check.
   *
   * @param checkId the check ID
   * @param key the configuration key
   * @param defaultValue default value if not configured
   * @return the configuration value or default
   */
  public int getCheckInt(String checkId, String key, int defaultValue) {
    Map<String, String> checkConfig = getCheckConfig(checkId);
    String value = checkConfig.get(key);
    if (value != null) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }
}
