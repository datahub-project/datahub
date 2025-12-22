package com.linkedin.metadata.config;

/**
 * Defines how to handle aspects that exceed the configured size threshold during MCP processing.
 */
public enum OversizedAspectRemediation {
  /**
   * Hard delete the oversized aspect from the database and reject the MCP with an error. This
   * prevents oversized aspects from accumulating in the database.
   */
  DELETE,

  /**
   * Leave the oversized aspect in the database, log a warning, and reject the MCP with an error.
   * This is safer for initial rollout but may allow oversized aspects to accumulate.
   */
  IGNORE

  // Future options:
  // DEAD_LETTER_QUEUE - Route to a special topic for manual review
  // TRUNCATE - Attempt to truncate the aspect to fit within limits
}
