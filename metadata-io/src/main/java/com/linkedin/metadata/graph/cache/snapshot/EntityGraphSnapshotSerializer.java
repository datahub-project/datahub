package com.linkedin.metadata.graph.cache.snapshot;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EntityGraphSnapshotSerializer implements StreamSerializer<EntityGraphSnapshot> {

  /**
   * Payload version written when no direction is a trusted full walk. Pods that have not picked up
   * version 2 can still read these snapshots.
   */
  public static final int SERIALIZER_VERSION = 1;

  /** Payload version written only while some direction has trusted seeds. */
  public static final int TRUSTED_FULL_WALK_VERSION = 2;

  @Override
  public int getTypeId() {
    return 9002;
  }

  @Override
  public void write(@Nonnull ObjectDataOutput out, @Nonnull EntityGraphSnapshot snapshot)
      throws IOException {
    int version = versionFor(snapshot);
    out.writeInt(version);
    out.writeString(snapshot.getGraphId());
    out.writeString(snapshot.getCacheKey());
    out.writeLong(snapshot.getGeneration());
    out.writeString(snapshot.getBuildSource());
    out.writeLong(snapshot.getBuiltAtMillis());
    out.writeInt(snapshot.getVertexCount());
    out.writeInt(snapshot.getEdgeCount());
    out.writeString(snapshot.getTopologyFingerprint());
    writeCoverage(out, snapshot.getTraversalCoverage(), version);
    String cacheStatus = snapshot.getCacheStatus();
    out.writeBoolean(cacheStatus != null);
    if (cacheStatus != null) {
      out.writeString(cacheStatus);
    }
    List<DirectedEdge> edges = snapshot.getEdges();
    int edgeSize = edges == null ? 0 : edges.size();
    out.writeInt(edgeSize);
    if (edges != null) {
      for (DirectedEdge edge : edges) {
        out.writeString(edge.getSourceUrn());
        out.writeString(edge.getDestinationUrn());
        out.writeString(edge.getRelationshipType());
      }
    }
  }

  @Override
  @Nonnull
  public EntityGraphSnapshot read(@Nonnull ObjectDataInput in) throws IOException {
    int version = in.readInt();
    if (version != SERIALIZER_VERSION && version != TRUSTED_FULL_WALK_VERSION) {
      throw new IOException(
          "Unsupported EntityGraphSnapshot serializer version: "
              + version
              + " (expected "
              + SERIALIZER_VERSION
              + " or "
              + TRUSTED_FULL_WALK_VERSION
              + ")");
    }
    String graphId = in.readString();
    String cacheKey = in.readString();
    long generation = in.readLong();
    String buildSource = in.readString();
    long builtAtMillis = in.readLong();
    int vertexCount = in.readInt();
    int edgeCount = in.readInt();
    String fingerprint = in.readString();
    TraversalCoverage coverage = readCoverage(in, version);
    String cacheStatus = in.readBoolean() ? in.readString() : null;
    int edgeSize = in.readInt();
    List<DirectedEdge> edges = new ArrayList<>(edgeSize);
    for (int i = 0; i < edgeSize; i++) {
      edges.add(
          DirectedEdge.builder()
              .sourceUrn(in.readString())
              .destinationUrn(in.readString())
              .relationshipType(in.readString())
              .build());
    }
    return EntityGraphSnapshot.builder()
        .graphId(graphId)
        .cacheKey(cacheKey)
        .generation(generation)
        .buildSource(buildSource)
        .builtAtMillis(builtAtMillis)
        .vertexCount(vertexCount)
        .edgeCount(edgeCount)
        .topologyFingerprint(fingerprint)
        .traversalCoverage(coverage)
        .cacheStatus(cacheStatus)
        .edges(edges)
        .build();
  }

  static int versionFor(@Nonnull EntityGraphSnapshot snapshot) {
    TraversalCoverage coverage = snapshot.getTraversalCoverage();
    return coverage != null && coverage.hasTrustedFullWalk()
        ? TRUSTED_FULL_WALK_VERSION
        : SERIALIZER_VERSION;
  }

  private static void writeCoverage(
      @Nonnull ObjectDataOutput out, @Nullable TraversalCoverage coverage, int version)
      throws IOException {
    if (coverage == null) {
      out.writeInt(-1);
      return;
    }
    List<DirectionCoverage> directions = coverage.getDirections();
    out.writeInt(directions.size());
    for (DirectionCoverage direction : directions) {
      out.writeString(direction.getDirection().name());
      out.writeBoolean(direction.isExplored());
      out.writeInt(direction.getExploredDepth());
      out.writeInt(direction.getConfiguredMaxDepth());
      out.writeBoolean(direction.isComplete());
      String reason = direction.getTruncationReason();
      out.writeBoolean(reason != null);
      if (reason != null) {
        out.writeString(reason);
      }
      if (version >= TRUSTED_FULL_WALK_VERSION) {
        List<String> trustedSeeds = direction.getTrustedSeeds();
        out.writeInt(trustedSeeds.size());
        for (String seed : trustedSeeds) {
          out.writeString(seed);
        }
        List<String> trustedEdgeLines = direction.getTrustedEdgeLines();
        out.writeInt(trustedEdgeLines.size());
        for (String edgeLine : trustedEdgeLines) {
          out.writeString(edgeLine);
        }
      }
    }
  }

  @Nullable
  private static TraversalCoverage readCoverage(@Nonnull ObjectDataInput in, int version)
      throws IOException {
    int size = in.readInt();
    if (size < 0) {
      return null;
    }
    TraversalCoverage.TraversalCoverageBuilder builder = TraversalCoverage.builder();
    for (int i = 0; i < size; i++) {
      TraversalDirection direction = TraversalDirection.valueOf(in.readString());
      boolean explored = in.readBoolean();
      int exploredDepth = in.readInt();
      int configuredMaxDepth = in.readInt();
      boolean complete = in.readBoolean();
      String truncationReason = in.readBoolean() ? in.readString() : null;
      List<String> trustedSeeds = List.of();
      List<String> trustedEdgeLines = List.of();
      if (version >= TRUSTED_FULL_WALK_VERSION) {
        int seedCount = in.readInt();
        if (seedCount > 0) {
          List<String> seeds = new ArrayList<>(seedCount);
          for (int seedIndex = 0; seedIndex < seedCount; seedIndex++) {
            seeds.add(in.readString());
          }
          trustedSeeds = List.copyOf(seeds);
        }
        int edgeCount = in.readInt();
        if (edgeCount > 0) {
          List<String> lines = new ArrayList<>(edgeCount);
          for (int edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
            lines.add(in.readString());
          }
          trustedEdgeLines = List.copyOf(lines);
        }
      }
      builder.direction(
          DirectionCoverage.builder()
              .direction(direction)
              .explored(explored)
              .exploredDepth(exploredDepth)
              .configuredMaxDepth(configuredMaxDepth)
              .complete(complete)
              .truncationReason(truncationReason)
              .trustedSeeds(trustedSeeds)
              .trustedEdgeLines(trustedEdgeLines)
              .build());
    }
    return builder.build();
  }
}
