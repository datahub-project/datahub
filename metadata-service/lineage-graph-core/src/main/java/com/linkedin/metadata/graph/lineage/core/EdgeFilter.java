package com.linkedin.metadata.graph.lineage.core;

import java.util.List;

/**
 * The lineage edges a walk may follow, compiled against a graph's type tables.
 *
 * <p>This is the in-memory form of {@code LineageGraphFilters.containsEdgeInfo(entityType,
 * EdgeInfo(relType, direction, opposingType))}: one bitset of allowed opposing entity types per
 * (entity type, relationship type, direction), so a check is one array read and one bit test.
 */
public final class EdgeFilter {

  public enum Direction {
    OUTGOING,
    INCOMING
  }

  /** One allowed (entity type, relationship type, direction, opposing entity type) rule. */
  public record Rule(
      String entityType, String relationshipType, Direction direction, String opposingType) {}

  private final int types;
  private final int rels;
  private final int words;
  private final long[] bits;
  private final boolean any;

  private EdgeFilter(int types, int rels, long[] bits, boolean any) {
    this.types = types;
    this.rels = rels;
    this.words = (types + 63) >>> 6;
    this.bits = bits;
    this.any = any;
  }

  /** Compiles rules; names unknown to the graph are ignored since no edge can match them. */
  public static EdgeFilter compile(GraphSnapshot graph, List<Rule> rules) {
    int types = graph.entityTypes().size();
    int rels = graph.relTypes().size();
    int words = (types + 63) >>> 6;
    long[] bits = new long[Math.max(1, types * rels * 2 * words)];
    boolean any = false;
    for (Rule r : rules) {
      int t = graph.entityTypes().find(r.entityType());
      int rel = graph.relTypes().find(r.relationshipType());
      int o = graph.entityTypes().find(r.opposingType());
      if (t < 0 || rel < 0 || o < 0) {
        continue;
      }
      int base = ((t * rels + rel) * 2 + r.direction().ordinal()) * words;
      bits[base + (o >>> 6)] |= 1L << (o & 63);
      any = true;
    }
    return new EdgeFilter(types, rels, bits, any);
  }

  /** True if at least one rule matched the graph's types; otherwise no edge can pass. */
  public boolean matchesAnything() {
    return any;
  }

  boolean allows(int entityType, int relType, int direction, int opposingType) {
    if (entityType >= types || relType >= rels || opposingType >= types) {
      return false;
    }
    int base = ((entityType * rels + relType) * 2 + direction) * words;
    return (bits[base + (opposingType >>> 6)] & (1L << (opposingType & 63))) != 0;
  }
}
