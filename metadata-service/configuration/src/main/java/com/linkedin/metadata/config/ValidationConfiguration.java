package com.linkedin.metadata.config;

import lombok.Data;

/** Validation configuration for DataHub operations. */
@Data
public class ValidationConfiguration {
  /** Aspect size validation configuration (applies to ALL aspect writes: REST/GraphQL/MCP) */
  private AspectSizeValidationConfig aspectSize;
}
