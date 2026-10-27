package com.linkedin.metadata.graph.lineage.core;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable lineage graph held off-heap, in compressed sparse rows in both directions.
 *
 * <p>Node {@code u}'s outgoing edges are positions {@code fwdOff[u] .. fwdOff[u+1]} of the forward
 * arrays and its incoming edges are positions {@code revOff[u] .. revOff[u+1]} of the reverse
 * arrays. Each edge is stored once in each direction with everything a walk reads: the neighbour,
 * one attribute byte ({@code relType:5}, 3 bits spare) and the via node. Rows are in the
 * visualization's priority order (see {@link GraphBuilder}), so a per-hop limit takes the first N.
 *
 * <p>Per node: {@code nodeMeta} ({@code entityType:8 | platform+1:16 | environment+1:8}) and the
 * schema field parent. Both are computed at build time by {@link NodeFacts}, so a walk or a search
 * never parses a URN.
 *
 * <p>Edge timestamps, actors, the manual flag and the lifecycle owner are not kept; they only
 * appear in responses for a page and are looked up for that page.
 */
public final class BaseGraph {

  public static final int NO_ID = -1;

  static final int REL_MASK = 0x1F;

  static final int MAX_REL_TYPES = 32;
  static final int MAX_ENTITY_TYPES = 256;
  static final int MAX_PLATFORMS = (1 << 16) - 1;
  // 0 is "none" and 255 marks a node whose facts could not be read.
  static final int MAX_ENVIRONMENTS = 254;
  static final int ENV_UNREADABLE = 0xFF;

  final UrnDictionary urns;
  final TypeTable entityTypes;
  final TypeTable relTypes;
  final TypeTable platforms;
  final TypeTable environments;
  final int nodes;
  final int edges;

  final MemorySegment nodeMeta;
  final MemorySegment parent;
  final MemorySegment fwdOff;
  final MemorySegment fwdNbr;
  final MemorySegment fwdAttr;
  final MemorySegment fwdVia;
  final MemorySegment revOff;
  final MemorySegment revNbr;
  final MemorySegment revAttr;
  final MemorySegment revVia;

  /** Segment names, in snapshot file order. */
  static final String[] SEGMENTS = {
    "nodeMeta",
    "parent",
    "fwdOff",
    "fwdNbr",
    "fwdAttr",
    "fwdVia",
    "revOff",
    "revNbr",
    "revAttr",
    "revVia"
  };

  BaseGraph(
      UrnDictionary urns,
      TypeTable entityTypes,
      TypeTable relTypes,
      TypeTable platforms,
      TypeTable environments,
      int nodes,
      int edges,
      MemorySegment[] seg) {
    this.urns = urns;
    this.entityTypes = entityTypes;
    this.relTypes = relTypes;
    this.platforms = platforms;
    this.environments = environments;
    this.nodes = nodes;
    this.edges = edges;
    this.nodeMeta = seg[0];
    this.parent = seg[1];
    this.fwdOff = seg[2];
    this.fwdNbr = seg[3];
    this.fwdAttr = seg[4];
    this.fwdVia = seg[5];
    this.revOff = seg[6];
    this.revNbr = seg[7];
    this.revAttr = seg[8];
    this.revVia = seg[9];
  }

  /** Allocates zeroed segments for a graph of the given size. */
  static BaseGraph allocate(
      Arena arena,
      UrnDictionary urns,
      TypeTable entityTypes,
      TypeTable relTypes,
      TypeTable platforms,
      TypeTable environments,
      int nodes,
      int edges) {
    long e = Math.max(1, edges);
    long n = Math.max(1, nodes);
    long[] bytes = {
      n * 4, n * 4, (nodes + 1L) * 4, e * 4, e, e * 4, (nodes + 1L) * 4, e * 4, e, e * 4
    };
    MemorySegment[] seg = new MemorySegment[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      seg[i] = arena.allocate(bytes[i], 8);
    }
    return new BaseGraph(urns, entityTypes, relTypes, platforms, environments, nodes, edges, seg);
  }

  MemorySegment[] segments() {
    return new MemorySegment[] {
      nodeMeta, parent, fwdOff, fwdNbr, fwdAttr, fwdVia, revOff, revNbr, revAttr, revVia
    };
  }

  /**
   * Reads every page of a memory-mapped graph in now, so later walks don't fault them in. Returns
   * the bytes touched; a graph built in memory has nothing to load.
   */
  public long warm() {
    long bytes = 0;
    MemorySegment[] segs = segments();
    MemorySegment[] all = java.util.Arrays.copyOf(segs, segs.length + 2);
    all[segs.length] = urns.data();
    all[segs.length + 1] = urns.blockOffsets();
    for (MemorySegment seg : all) {
      if (seg.isMapped()) {
        seg.load();
        bytes += seg.byteSize();
      }
    }
    return bytes;
  }

  public int nodeCount() {
    return nodes;
  }

  public int edgeCount() {
    return edges;
  }

  public UrnDictionary urns() {
    return urns;
  }

  public TypeTable entityTypes() {
    return entityTypes;
  }

  public TypeTable relTypes() {
    return relTypes;
  }

  public TypeTable platforms() {
    return platforms;
  }

  public TypeTable environments() {
    return environments;
  }

  /** Packed node facts; read them with the static accessors below. */
  public int meta(int node) {
    return nodeMeta.getAtIndex(JAVA_INT, node);
  }

  public int entityType(int node) {
    return meta(node) & 0xFF;
  }

  /** Platform id, or -1 when the node has none. */
  public int platform(int node) {
    return platformOf(meta(node));
  }

  /** Environment id, or -1 when the node has none. */
  public int environment(int node) {
    return environmentOf(meta(node));
  }

  /** Whether the builder could not read this node's facts; searches must not answer for it. */
  public boolean unreadable(int node) {
    return unreadableOf(meta(node));
  }

  /** The schema field parent's node id, or {@link #NO_ID}. */
  public int parent(int node) {
    return parent.getAtIndex(JAVA_INT, node);
  }

  public static int platformOf(int meta) {
    return ((meta >>> 8) & 0xFFFF) - 1;
  }

  public static int environmentOf(int meta) {
    int env = meta >>> 24;
    return env == ENV_UNREADABLE ? -1 : env - 1;
  }

  public static boolean unreadableOf(int meta) {
    return (meta >>> 24) == ENV_UNREADABLE;
  }

  static int packMeta(int entityType, int platform, int environment, boolean unreadable) {
    int env = unreadable ? ENV_UNREADABLE : environment + 1;
    return entityType | ((platform + 1) << 8) | (env << 24);
  }

  int fwdStart(int node) {
    return fwdOff.getAtIndex(JAVA_INT, node);
  }

  int fwdEnd(int node) {
    return fwdOff.getAtIndex(JAVA_INT, node + 1);
  }

  int fwdNbr(int i) {
    return fwdNbr.getAtIndex(JAVA_INT, i);
  }

  int fwdAttr(int i) {
    return fwdAttr.get(JAVA_BYTE, i) & 0xFF;
  }

  int fwdVia(int i) {
    return fwdVia.getAtIndex(JAVA_INT, i);
  }

  int revStart(int node) {
    return revOff.getAtIndex(JAVA_INT, node);
  }

  int revEnd(int node) {
    return revOff.getAtIndex(JAVA_INT, node + 1);
  }

  int revNbr(int i) {
    return revNbr.getAtIndex(JAVA_INT, i);
  }

  int revAttr(int i) {
    return revAttr.get(JAVA_BYTE, i) & 0xFF;
  }

  int revVia(int i) {
    return revVia.getAtIndex(JAVA_INT, i);
  }

  /** Bytes held per structure, for {@code /stats} and the bench. */
  public Map<String, Long> memory() {
    Map<String, Long> m = new LinkedHashMap<>();
    m.put("urnDictionary", urns.bytes());
    m.put("nodeMeta", nodeMeta.byteSize());
    m.put("parent", parent.byteSize());
    m.put("offsets", fwdOff.byteSize() + revOff.byteSize());
    m.put("neighbours", fwdNbr.byteSize() + revNbr.byteSize());
    m.put("attributes", fwdAttr.byteSize() + revAttr.byteSize());
    m.put("via", fwdVia.byteSize() + revVia.byteSize());
    m.put("total", m.values().stream().mapToLong(Long::longValue).sum());
    return m;
  }
}
