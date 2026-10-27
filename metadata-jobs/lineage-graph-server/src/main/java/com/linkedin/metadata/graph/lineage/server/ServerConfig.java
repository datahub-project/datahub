package com.linkedin.metadata.graph.lineage.server;

import java.nio.file.Path;

/**
 * Settings of the Lineage Graph Service and its snapshot builder, from environment variables with
 * bench-friendly defaults.
 *
 * @param snapshotRoot where the builder publishes snapshots and the service reads the latest
 * @param reloadPollMillis how often the service checks for a newer snapshot
 * @param walkConcurrency walks (with their search and encoding) that run at once; others queue
 * @param buildEveryMillis the builder's period
 */
public record ServerConfig(
    int port,
    String openSearchUrl,
    String graphIndex,
    int scanSlices,
    int scanPageSize,
    Path snapshotRoot,
    long reloadPollMillis,
    int walkConcurrency,
    long buildEveryMillis) {

  static ServerConfig fromEnv() {
    return new ServerConfig(
        Integer.parseInt(env("LINEAGE_GRAPH_SERVER_PORT", "8090")),
        env("ELASTICSEARCH_URL", "http://localhost:9200"),
        env("LINEAGE_GRAPH_INDEX", "graph_service_v1"),
        Integer.parseInt(env("LINEAGE_GRAPH_SCAN_SLICES", "6")),
        Integer.parseInt(env("LINEAGE_GRAPH_SCAN_PAGE_SIZE", "10000")),
        Path.of(env("LINEAGE_GRAPH_SNAPSHOT_ROOT", "/tmp/lineage-graph/snapshots")),
        Long.parseLong(env("LINEAGE_GRAPH_RELOAD_POLL_MS", "10000")),
        Integer.parseInt(env("LINEAGE_GRAPH_WALK_CONCURRENCY", "2")),
        Long.parseLong(env("LINEAGE_GRAPH_BUILD_EVERY_MS", "600000")));
  }

  private static String env(String name, String fallback) {
    String v = System.getenv(name);
    return v == null || v.isBlank() ? fallback : v;
  }
}
