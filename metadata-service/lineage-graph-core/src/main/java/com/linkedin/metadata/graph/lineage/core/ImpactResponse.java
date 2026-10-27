package com.linkedin.metadata.graph.lineage.core;

import java.util.List;

/**
 * A decoded walk result, as the GMS client sees it: plain strings and numbers, no graph ids.
 *
 * @param walkMicros time the server spent walking
 * @param watermark change sequence of the snapshot walked
 * @param lagMillis how far the snapshot trailed the change stream when the walk started
 */
public record ImpactResponse(
    WalkResult.Status status,
    boolean partial,
    int hops,
    long edgesScanned,
    long walkMicros,
    long watermark,
    long lagMillis,
    List<Relationship> relationships) {

  /** One lineage relationship; optional values are null when absent. */
  public record Relationship(
      String urn,
      String type,
      int degree,
      int[] degrees,
      boolean explored,
      boolean manual,
      Long createdOn,
      String createdActor,
      Long updatedOn,
      String updatedActor,
      List<List<String>> paths) {}
}
