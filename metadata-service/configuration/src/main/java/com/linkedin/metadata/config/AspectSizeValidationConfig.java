package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for aspect size validation that applies to ALL aspect writes (REST API, GraphQL,
 * MCP, etc.). Will be nested under datahub.validation.aspectSize in application.yaml.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AspectSizeValidationConfig {
  /**
   * Validates existing aspect in DB before patch application (measures: raw JSON string character
   * count from database). Use to catch pre-existing oversized aspects. REPLACE_WITH_PATCH
   * remediation deletes the oversized aspect and continues with the write as an insert.
   */
  private AspectCheckpointConfig prePatch =
      new AspectCheckpointConfig(false, 15728640L, OversizedAspectRemediation.REPLACE_WITH_PATCH);

  /**
   * Validates aspect after patch application, in DAO before DB write (measures: serialized JSON
   * character count, same unit as prePatch). Use to catch bloat from patch application. Validation
   * happens on JSON already created for DB write - zero additional serialization cost.
   */
  private AspectCheckpointConfig postPatch =
      new AspectCheckpointConfig(false, 15728640L, OversizedAspectRemediation.DELETE);

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class AspectCheckpointConfig {
    private boolean enabled;
    private long maxSizeBytes;
    private OversizedAspectRemediation oversizedRemediation;
  }
}
