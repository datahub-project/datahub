package com.linkedin.metadata.usage.flush;

/** Why a flush window was closed. */
public enum FlushTrigger {
  SCHEDULED,
  MAX_WINDOW,
  CARDINALITY,
  SHUTDOWN
}
