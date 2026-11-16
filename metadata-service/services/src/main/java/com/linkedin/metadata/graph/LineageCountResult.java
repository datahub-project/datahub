package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import java.util.List;
import lombok.Value;

/**
 * A one-hop lineage count: how many distinct entities neighbour a set of source entities, plus a
 * sample of them so a caller can apply the same post-processing a paged walk would (ghost and
 * sibling filtering) without fetching edges.
 */
@Value
public class LineageCountResult {
  /** Distinct neighbours, excluding the source entities themselves. */
  int total;

  /** Up to the requested sample size of those neighbours, in no particular order. */
  List<Urn> sample;

  public static LineageCountResult empty() {
    return new LineageCountResult(0, List.of());
  }
}
