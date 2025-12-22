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
  IGNORE,

  /**
   * (Pre-patch only) Replace the oversized existing aspect with the new patch as if it were an
   * insert (no merge). Deletes the old oversized aspect and proceeds with the write operation. This
   * allows the operation to succeed while cleaning up the oversized data.
   */
  REPLACE_WITH_PATCH

  // Future options:
  // DEAD_LETTER_QUEUE - Route to a special topic for manual review
  // TRUNCATE - Attempt to truncate the aspect to fit within limits
}
