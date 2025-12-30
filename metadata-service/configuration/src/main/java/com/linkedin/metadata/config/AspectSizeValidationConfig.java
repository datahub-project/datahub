package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for aspect size validation that applies to ALL aspect writes (REST API, GraphQL,
 * MCP, etc.). Will be nested under datahub.validation.aspectSize in application.yaml.
 *
 * <p>All defaults are specified in application.yaml via environment variables. No defaults in Java
 * code to avoid confusion when values become null unexpectedly during config refresh.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AspectSizeValidationConfig {
  /**
   * Validates existing aspect in DB before patch application (measures: raw JSON string character
   * count from database). Use to catch pre-existing oversized aspects.
   */
  private AspectCheckpointConfig prePatch;

  /**
   * Validates aspect after patch application, in DAO before DB write (measures: serialized JSON
   * character count, same unit as prePatch). Use to catch bloat from patch application. Validation
   * happens on JSON already created for DB write - zero additional serialization cost.
   */
  private AspectCheckpointConfig postPatch;

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class AspectCheckpointConfig {
    private boolean enabled;
    private Long warnSizeBytes; // Optional: log warning without blocking write
    private long maxSizeBytes;
    private OversizedAspectRemediation oversizedRemediation;
  }
}
