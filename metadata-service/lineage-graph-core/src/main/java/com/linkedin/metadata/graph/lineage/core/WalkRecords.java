package com.linkedin.metadata.graph.lineage.core;

/**
 * Layout of a walk record: the 16 ints (one 64-byte cache line) a walk keeps for each node that has
 * a relationship or a stored path, packed into one {@code int[]}.
 *
 * <p>One array of records instead of one array per field matters: G1 and ZGC start large arrays on
 * region boundaries, so a dozen parallel per-field arrays read at the same index all map to the
 * same cache sets and evict each other. Packed, a node's bookkeeping is a single cache line.
 */
final class WalkRecords {

  static final int STRIDE = 16;

  static final int NODE = 0;
  static final int FLAGS = 1;

  /** Record of the parent the stored path extends, or -1 if the parent had no record. */
  static final int S_PARENT_REC = 2;

  static final int S_PARENT_NODE = 3;
  static final int S_VIA = 4;

  /** relType:8 | kind1:2 | kind2:2 | (unused):4 | hopRelType:8 | hopKind:2. */
  static final int PACKED = 5;

  static final int DEGREE = 7;
  static final int DEGREE2 = 8;
  static final int ARG1 = 9;
  static final int ARG2 = 10;

  /** The hop in which this record last had a relationship put; 0 if never. */
  static final int HOP_STAMP = 11;

  static final int HOP_ARG = 13;

  // FLAGS bits
  static final int F_HAS_STORED = 1;
  static final int F_PARENT_HAD_STORED = 2;
  static final int F_CYCLIC = 4;
  static final int F_EXPLORED = 8;
  static final int F_IN_RESULT = 16;

  // Path kinds
  static final int KIND_NONE = 0;

  /** The path is the record's own stored path; arg is the record itself. */
  static final int KIND_STORED = 1;

  /** The path is the stored path of record {@code arg}, cut after this node. */
  static final int KIND_VIA_OF = 2;

  private WalkRecords() {}

  static int relType(int packed) {
    return packed & 0xFF;
  }

  static int kind1(int packed) {
    return (packed >>> 8) & 3;
  }

  static int kind2(int packed) {
    return (packed >>> 10) & 3;
  }

  static int hopRelType(int packed) {
    return (packed >>> 16) & 0xFF;
  }

  static int hopKind(int packed) {
    return (packed >>> 24) & 3;
  }

  static int withRelationship(int packed, int relType, int kind1) {
    return (packed & ~0x3FF) | relType | (kind1 << 8);
  }

  static int withKind2(int packed, int kind2) {
    return (packed & ~(3 << 10)) | (kind2 << 10);
  }

  static int withHop(int packed, int relType, int kind) {
    return (packed & 0x0000FFFF) | (relType << 16) | (kind << 24);
  }
}
