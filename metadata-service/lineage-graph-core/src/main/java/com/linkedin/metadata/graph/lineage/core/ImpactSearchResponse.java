package com.linkedin.metadata.graph.lineage.core;

import java.util.List;

/**
 * A lineage search answered next to the graph ({@code DHLS}): either one page of the matching
 * relationships with the total and facet counts, or, when the walk found too few relationships for
 * the graph-only search to apply, the full walk result.
 *
 * @param full the full walk, only in {@link Mode#FULL}
 * @param walked relationships the walk found
 * @param prefiltered relationships left after the entity type and degree filters
 * @param total relationships matching every filter (0 in {@link Mode#FULL})
 * @param facets platform, entity type and origin, in that order (empty in {@link Mode#FULL})
 * @param searchMicros time the server spent filtering and paging after the walk
 */
public record ImpactSearchResponse(
    Mode mode,
    ImpactResponse full,
    WalkResult.Status status,
    boolean partial,
    int hops,
    long edgesScanned,
    long walkMicros,
    long searchMicros,
    long watermark,
    long lagMillis,
    int walked,
    int prefiltered,
    int total,
    List<Facet> facets,
    List<Row> rows) {

  public enum Mode {
    LIGHTNING,
    FULL
  }

  /**
   * Values in the order they were first counted, with their counts.
   *
   * @param countedAfterLastNew whether a value already seen was counted again after the last new
   *     value appeared
   */
  public record Facet(List<String> values, long[] counts, boolean countedAfterLastNew) {}

  /** One relationship of the page; paths are lists of URNs, root first. */
  public record Row(String urn, int degree, List<List<String>> paths) {}
}
