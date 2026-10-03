package com.linkedin.metadata.graph.cache;

/** How far the caller walked before asking the cache to publish. */
public enum FullWalkExtent {
  /** A limited or partial result. Must not replace a directional closure. */
  LIMITED,
  /** The caller finished an unlimited walk in one direction and it was not depth-capped. */
  FULL_UNLIMITED
}
