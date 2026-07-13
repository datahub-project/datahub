package com.linkedin.metadata.graph.cache;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/** Outcome of a typed one-hop (or shallow) membership neighbor listing. */
public sealed interface MembershipNeighborResult {

  /** A directed membership edge from the perspective of the query seed. */
  record Neighbor(@Nonnull String neighborUrn, @Nonnull String relationshipType) {}

  record Hit(@Nonnull List<Neighbor> neighbors, int total) implements MembershipNeighborResult {}

  record Miss(@Nonnull ReadMissReason reason) implements MembershipNeighborResult {}

  @Nonnull
  static MembershipNeighborResult miss(@Nonnull ReadMissReason reason) {
    return new Miss(reason);
  }

  @Nonnull
  static MembershipNeighborResult fromNeighbors(@Nonnull List<Neighbor> neighbors, int total) {
    return new Hit(List.copyOf(neighbors), total);
  }

  default boolean isHit() {
    return this instanceof Hit;
  }

  default boolean isMiss() {
    return this instanceof Miss;
  }

  @Nonnull
  default List<Neighbor> neighborsOrEmpty() {
    if (this instanceof Hit hit) {
      return hit.neighbors();
    }
    return Collections.emptyList();
  }
}
