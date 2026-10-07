package com.linkedin.metadata.graph.cache;

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;

/** Outcome of a graph expand request. */
public sealed interface GraphReadResult {

  /**
   * @param scopeTruncated true when a definition-depth partial read stopped at {@code
   *     scope.maxDepth} with nodes still queued. The vertices are the prefix. Callers that need the
   *     full closure use {@link #missIfScopeTruncated()}.
   */
  record Hit(@Nonnull Set<String> vertices, boolean scopeTruncated) implements GraphReadResult {
    public Hit(@Nonnull Set<String> vertices) {
      this(vertices, false);
    }
  }

  /** Valid expand with no related vertices beyond seeds (e.g. leaf domain with no descendants). */
  record EmptyHit(@Nonnull Set<String> vertices, boolean scopeTruncated)
      implements GraphReadResult {
    public EmptyHit(@Nonnull Set<String> vertices) {
      this(vertices, false);
    }
  }

  record Miss(@Nonnull ReadMissReason reason) implements GraphReadResult {}

  @Nonnull
  static GraphReadResult miss(@Nonnull ReadMissReason reason) {
    return new Miss(reason);
  }

  @Nonnull
  static GraphReadResult fromVertices(@Nonnull Set<String> vertices) {
    return fromVertices(vertices, false);
  }

  /**
   * @param scopeTruncated see {@link Hit#scopeTruncated()}
   */
  @Nonnull
  static GraphReadResult fromVertices(@Nonnull Set<String> vertices, boolean scopeTruncated) {
    if (vertices.isEmpty()) {
      return new EmptyHit(Collections.emptySet(), scopeTruncated);
    }
    return new Hit(vertices, scopeTruncated);
  }

  /**
   * Returns vertices when the read succeeded; empty set for {@link EmptyHit}; empty for {@link
   * Miss}.
   */
  @Nonnull
  default Set<String> verticesOrEmpty() {
    if (this instanceof Hit hit) {
      return hit.vertices();
    } else if (this instanceof EmptyHit emptyHit) {
      return emptyHit.vertices();
    }
    return Collections.emptySet();
  }

  default boolean isHit() {
    return this instanceof Hit || this instanceof EmptyHit;
  }

  default boolean isMiss() {
    return this instanceof Miss;
  }

  /** True when this hit is only the prefix inside a partial graph's configured depth. */
  default boolean isScopeTruncated() {
    if (this instanceof Hit hit) {
      return hit.scopeTruncated();
    }
    if (this instanceof EmptyHit emptyHit) {
      return emptyHit.scopeTruncated();
    }
    return false;
  }

  /**
   * Turns a scope-truncated prefix into {@link ReadMissReason#TRUNCATED}. A hit that walked the
   * requested closure is unchanged. Per-call limit truncation is already a miss.
   */
  @Nonnull
  default GraphReadResult missIfScopeTruncated() {
    if (isScopeTruncated()) {
      return miss(ReadMissReason.TRUNCATED);
    }
    return this;
  }
}
