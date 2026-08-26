package com.linkedin.metadata.graph.cache;

/** Whether a graph read may use cached snapshots or must attempt a live build. */
public enum ReadMode {
  CACHED,
  EPHEMERAL
}
