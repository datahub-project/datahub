package com.linkedin.metadata.graph.lineage.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.inmemory.LightningNodeFacts;
import com.linkedin.metadata.graph.lineage.core.BaseGraph;
import com.linkedin.metadata.graph.lineage.core.GraphBuilder;
import com.linkedin.metadata.graph.lineage.core.SnapshotIO;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.foreign.Arena;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds Lineage Graph Service snapshots: scans the lineage edges in the OpenSearch graph index as
 * of one point in time (a scroll's consistent view), builds the graph, and publishes it under the
 * snapshot root for replicas to load. Runs as its own job, so replicas are never sized for a
 * build's peak memory.
 *
 * <p>Every URN-derived fact (entity type, platform, environment, schema field parent) is computed
 * here with the same code GMS uses ({@link LightningNodeFacts}) and stored, so the service never
 * parses URNs.
 *
 * <p>Usage: {@code lineage-snapshot-builder [--once]}. Without {@code --once} it builds every
 * {@code LINEAGE_GRAPH_BUILD_EVERY_MS}. Each snapshot directory gets a {@code build.json} with the
 * step timings and peak heap.
 */
@Slf4j
public final class LineageSnapshotBuilder {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private final ServerConfig config;
  private final Set<String> lineageTypes;

  LineageSnapshotBuilder(ServerConfig config, Set<String> lineageTypes) {
    this.config = config;
    this.lineageTypes = lineageTypes;
  }

  public static void main(String[] args) throws Exception {
    ServerConfig config = ServerConfig.fromEnv();
    boolean once = List.of(args).contains("--once");
    long t = System.nanoTime();
    EntityRegistry registry = TestOperationContexts.defaultEntityRegistry();
    Set<String> types = lineageTypes(registry, new LineageRegistry(registry));
    log.info("Lineage relationship types ({} ms): {}", ms(t), types);
    LineageSnapshotBuilder builder = new LineageSnapshotBuilder(config, types);
    while (true) {
      long start = System.currentTimeMillis();
      try {
        builder.buildOnce();
      } catch (Exception e) {
        if (once) {
          throw e;
        }
        log.error("Snapshot build failed; the previous snapshot stays the latest", e);
      }
      if (once) {
        return;
      }
      long sleep = config.buildEveryMillis() - (System.currentTimeMillis() - start);
      if (sleep > 0) {
        Thread.sleep(sleep);
      }
    }
  }

  /** Scans, builds and publishes one snapshot; returns its directory. */
  Path buildOnce() throws Exception {
    resetPeakHeap();
    Map<String, Object> report = new LinkedHashMap<>();
    long t0 = System.nanoTime();
    GraphBuilder builder = new GraphBuilder(LightningNodeFacts.INSTANCE);
    OpenSearchGraphScanner.Stats scan =
        new OpenSearchGraphScanner(config).scan(lineageTypes, builder);
    report.put("scanMs", scan.ms());
    report.put("scannedDocs", scan.docs());
    report.put("builderHeapBytesBeforeBuild", builder.heapBytes());

    try (Arena arena = Arena.ofConfined()) {
      long t = System.nanoTime();
      BaseGraph graph = builder.build(arena);
      report.put("buildMs", ms(t));
      report.put("buildStepsMs", builder.timingsMs());
      report.put("duplicates", builder.duplicates());
      report.put("unreadableNodes", builder.unreadableNodes());
      report.put("nodes", graph.nodeCount());
      report.put("edges", graph.edgeCount());

      long builtAt = System.currentTimeMillis();
      Map<String, String> stats = new LinkedHashMap<>();
      stats.put("scannedDocs", Long.toString(scan.docs()));
      stats.put("duplicates", Integer.toString(builder.duplicates()));
      stats.put("unreadableNodes", Integer.toString(builder.unreadableNodes()));
      t = System.nanoTime();
      Path dir =
          SnapshotIO.publish(
              graph,
              new SnapshotIO.Meta(builtAt, scan.pointInTimeMillis(), stats),
              config.snapshotRoot());
      report.put("writeMs", ms(t));
      report.put("totalMs", ms(t0));
      report.put("snapshotBytes", SnapshotIO.sizeOnDisk(dir));
      report.put("graphBytes", graph.memory());
      report.put("peakHeapBytes", peakHeap());
      report.put("snapshot", dir.toString());
      Files.writeString(dir.resolve("build.json"), MAPPER.writeValueAsString(report));
      log.info("Published snapshot {}: {}", dir, MAPPER.writeValueAsString(report));
      return dir;
    }
  }

  /** Every relationship type a lineage walk can follow, from the entity registry. */
  static Set<String> lineageTypes(EntityRegistry registry, LineageRegistry lineageRegistry) {
    Set<String> types = new TreeSet<>();
    for (EntitySpec spec : registry.getEntitySpecs().values()) {
      for (LineageDirection d : LineageDirection.values()) {
        if (d == LineageDirection.$UNKNOWN) {
          continue;
        }
        lineageRegistry
            .getLineageRelationships(spec.getName(), d)
            .forEach(e -> types.add(e.getType()));
      }
    }
    return types;
  }

  private static void resetPeakHeap() {
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP) {
        pool.resetPeakUsage();
      }
    }
  }

  /** Sum of the heap pools' peaks since the last reset: an upper bound on the build's peak heap. */
  private static long peakHeap() {
    long sum = 0;
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP && pool.getPeakUsage() != null) {
        sum += pool.getPeakUsage().getUsed();
      }
    }
    return sum;
  }

  private static long ms(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }
}
