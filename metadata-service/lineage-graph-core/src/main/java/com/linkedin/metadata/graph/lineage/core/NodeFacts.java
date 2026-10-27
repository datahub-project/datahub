package com.linkedin.metadata.graph.lineage.core;

/**
 * Works out, once per node at build time, what searches read from a URN: its entity type, platform
 * and environment facets, and the parent a schema field is grouped under. The snapshot stores the
 * results, so the service never parses URNs.
 *
 * <p>The builder passes an implementation backed by GMS's own {@code LightningUrnFields}, so stored
 * facts equal what GMS computes. {@link #URN_PARTS} is a dependency-free default for tests and CSV
 * inputs.
 */
@FunctionalInterface
public interface NodeFacts {

  /**
   * @param entityType the URN's entity type
   * @param platform platform facet value, or null
   * @param environment environment (origin) facet value, or null
   * @param parent the URN of the schema field's parent, or null when the node is not a schema field
   * @param unreadable the URN could not be read the way GMS reads it; searches touching the node
   *     are left to GMS
   */
  record Facts(
      String entityType, String platform, String environment, String parent, boolean unreadable) {}

  Facts of(String urn);

  /** Entity type and the first embedded data platform URN, read off the URN's bytes. */
  NodeFacts URN_PARTS =
      urn -> {
        byte[] b = urn.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String platform = UrnParts.platform(b, 0, b.length);
        return new Facts(
            UrnParts.entityType(b, 0, b.length),
            platform == null ? null : "urn:li:dataPlatform:" + platform,
            null,
            null,
            false);
      };
}
