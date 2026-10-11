package com.linkedin.metadata.graph.cache.snapshot;

import com.linkedin.metadata.graph.cache.TraversalDirection;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class TraversalCoverage implements Serializable {
  private static final long serialVersionUID = 1L;

  @Singular @Nonnull List<DirectionCoverage> directions;

  public boolean canSatisfy(@Nonnull TraversalDirection direction) {
    DirectionCoverage coverage = directionFor(direction);
    return coverage != null && coverage.isExplored() && coverage.isComplete();
  }

  /** True when a caller-signaled full walk was written back for {@code direction}. */
  public boolean isTrustedFullWalk(@Nonnull TraversalDirection direction) {
    DirectionCoverage coverage = directionFor(direction);
    return coverage != null && coverage.isTrustedFullWalk();
  }

  public boolean hasTrustedFullWalk() {
    return directions.stream().anyMatch(DirectionCoverage::isTrustedFullWalk);
  }

  /**
   * True when this coverage strictly improves completeness over {@code other} for any direction.
   */
  public boolean isStrictImprovementOver(@Nullable TraversalCoverage other) {
    if (other == null || other.getDirections().isEmpty()) {
      return directions.stream().anyMatch(DirectionCoverage::isComplete);
    }
    for (DirectionCoverage candidate : directions) {
      DirectionCoverage existing = other.directionFor(candidate.getDirection());
      if (candidate.isComplete() && (existing == null || !existing.isComplete())) {
        return true;
      }
      if (candidate.isComplete()
          && existing != null
          && existing.isComplete()
          && candidate.getExploredDepth() > existing.getExploredDepth()) {
        return true;
      }
    }
    return false;
  }

  @Nonnull
  public TraversalCoverage withDirection(@Nonnull DirectionCoverage updated) {
    Map<TraversalDirection, DirectionCoverage> merged = new EnumMap<>(TraversalDirection.class);
    for (DirectionCoverage direction : directions) {
      merged.put(direction.getDirection(), direction);
    }
    merged.put(updated.getDirection(), updated);
    return TraversalCoverage.builder().directions(new ArrayList<>(merged.values())).build();
  }

  @Nonnull
  public static TraversalCoverage fullComplete() {
    return TraversalCoverage.builder()
        .direction(
            DirectionCoverage.builder()
                .direction(TraversalDirection.FORWARD)
                .explored(true)
                .exploredDepth(Integer.MAX_VALUE)
                .configuredMaxDepth(Integer.MAX_VALUE)
                .complete(true)
                .build())
        .direction(
            DirectionCoverage.builder()
                .direction(TraversalDirection.REVERSE)
                .explored(true)
                .exploredDepth(Integer.MAX_VALUE)
                .configuredMaxDepth(Integer.MAX_VALUE)
                .complete(true)
                .build())
        .build();
  }

  @Nonnull
  public static TraversalCoverage incomplete() {
    return TraversalCoverage.builder()
        .direction(
            DirectionCoverage.builder()
                .direction(TraversalDirection.FORWARD)
                .explored(false)
                .exploredDepth(0)
                .configuredMaxDepth(0)
                .complete(false)
                .build())
        .direction(
            DirectionCoverage.builder()
                .direction(TraversalDirection.REVERSE)
                .explored(false)
                .exploredDepth(0)
                .configuredMaxDepth(0)
                .complete(false)
                .build())
        .build();
  }

  @Nullable
  public DirectionCoverage getDirection(@Nonnull TraversalDirection direction) {
    return directionFor(direction);
  }

  @Nullable
  private DirectionCoverage directionFor(@Nonnull TraversalDirection direction) {
    for (DirectionCoverage coverage : directions) {
      if (coverage.getDirection() == direction) {
        return coverage;
      }
    }
    return null;
  }

  @Value
  @Builder
  public static class DirectionCoverage implements Serializable {
    private static final long serialVersionUID = 1L;

    @Nonnull TraversalDirection direction;
    boolean explored;
    int exploredDepth;
    int configuredMaxDepth;
    boolean complete;
    @Nullable String truncationReason;

    /**
     * Seeds whose closure in {@link #direction} was actually walked. A non-empty list is the trust
     * stamp. Full-path reads trust only roots inside that closure, not every vertex that shares the
     * component.
     */
    @Builder.Default @Nonnull List<String> trustedSeeds = List.of();

    /**
     * Canonical lines of the edges that walk produced. Full-path reads follow this set, so leftover
     * component edges stay available to ordinary expands and are not part of the trusted closure.
     */
    @Builder.Default @Nonnull List<String> trustedEdgeLines = List.of();

    /** True when a full walk was written back for this direction. */
    public boolean isTrustedFullWalk() {
      return !trustedSeeds.isEmpty();
    }
  }
}
