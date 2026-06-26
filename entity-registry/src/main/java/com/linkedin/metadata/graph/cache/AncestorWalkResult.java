package com.linkedin.metadata.graph.cache;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/** Outcome of an ordered forward ancestor walk. */
public sealed interface AncestorWalkResult {

  record Hit(@Nonnull List<String> ancestors) implements AncestorWalkResult {}

  record Miss(@Nonnull ReadMissReason reason) implements AncestorWalkResult {}

  @Nonnull
  static AncestorWalkResult miss(@Nonnull ReadMissReason reason) {
    return new Miss(reason);
  }

  @Nonnull
  static AncestorWalkResult fromAncestors(@Nonnull List<String> ancestors) {
    return new Hit(ancestors);
  }

  @Nonnull
  default List<String> ancestorsOrEmpty() {
    return switch (this) {
      case Hit hit -> hit.ancestors();
      case Miss miss -> Collections.emptyList();
    };
  }

  default boolean isHit() {
    return this instanceof Hit;
  }

  default boolean isMiss() {
    return this instanceof Miss;
  }
}
