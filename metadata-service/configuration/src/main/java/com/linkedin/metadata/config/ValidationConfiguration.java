package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Validation configuration for DataHub operations.
 *
 * <p>All defaults are specified in application.yaml via environment variables. No defaults in Java
 * code to avoid confusion when values become null unexpectedly during config refresh.
 */
@Data
public class ValidationConfiguration {
  /** Aspect size validation configuration (applies to ALL aspect writes: REST/GraphQL/MCP) */
  private AspectSizeValidationConfig aspectSize;
}
