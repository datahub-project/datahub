package com.linkedin.metadata.graph.cache;

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;

/** Outcome of a graph expand request. */
public sealed interface GraphReadResult {

  record Hit(@Nonnull Set<String> vertices) implements GraphReadResult {}

  /** Valid expand with no related vertices beyond seeds (e.g. leaf domain with no descendants). */
  record EmptyHit(@Nonnull Set<String> vertices) implements GraphReadResult {}

  record Miss(@Nonnull ReadMissReason reason) implements GraphReadResult {}

  @Nonnull
  static GraphReadResult miss(@Nonnull ReadMissReason reason) {
    return new Miss(reason);
  }

  @Nonnull
  static GraphReadResult fromVertices(@Nonnull Set<String> vertices) {
    if (vertices.isEmpty()) {
      return new EmptyHit(Collections.emptySet());
    }
    return new Hit(vertices);
  }

  /**
   * Returns vertices when the read succeeded; empty set for {@link EmptyHit}; empty for {@link
   * Miss}.
   */
  @Nonnull
  default Set<String> verticesOrEmpty() {
    return switch (this) {
      case Hit hit -> hit.vertices();
      case EmptyHit emptyHit -> emptyHit.vertices();
      case Miss miss -> Collections.emptySet();
    };
  }

  default boolean isHit() {
    return this instanceof Hit || this instanceof EmptyHit;
  }

  default boolean isMiss() {
    return this instanceof Miss;
  }
}
