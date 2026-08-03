package com.linkedin.metadata.entity.coordinator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Immutable, fully-computed plan for a single coordinated command.
 *
 * @param commandId identifier of the command that produced this plan
 * @param conflictKeys coordination tokens this plan contends on (see {@link ConflictKey}); sorted
 *     for deterministic acquisition order
 * @param observedVersions the version observed (via non-locking read) for each aspect the plan
 *     depends on, used for optimistic-concurrency validation at apply time. The value is the {@code
 *     SystemMetadata.version} string of the observed aspect (from {@code
 *     SystemAspect.getSystemMetadataVersion()}). Policy: an aspect that was observed absent, or
 *     whose system metadata carried no version, is represented by the empty string {@code ""} —
 *     never a {@code null} value — so callers can rely on the map having no null values.
 * @param mutations the proposed changes keyed by {@link AspectKey}, in {@link AspectKey} order
 */
public record MutationPlan(
    @Nonnull String commandId,
    @Nonnull SortedSet<ConflictKey> conflictKeys,
    @Nonnull Map<AspectKey, String> observedVersions,
    @Nonnull SortedMap<AspectKey, PlannedMutation> mutations) {

  /** Sentinel value for "no observed version" (aspect absent, or no version in system metadata). */
  public static final String NO_OBSERVED_VERSION = "";

  public MutationPlan {
    Objects.requireNonNull(commandId, "commandId");

    SortedSet<ConflictKey> copiedConflictKeys = new TreeSet<>();
    if (conflictKeys != null) {
      copiedConflictKeys.addAll(conflictKeys);
    }
    conflictKeys = Collections.unmodifiableSortedSet(copiedConflictKeys);

    Map<AspectKey, String> copiedObservedVersions = new LinkedHashMap<>();
    if (observedVersions != null) {
      copiedObservedVersions.putAll(observedVersions);
    }
    observedVersions = Collections.unmodifiableMap(copiedObservedVersions);

    SortedMap<AspectKey, PlannedMutation> copiedMutations = new TreeMap<>();
    if (mutations != null) {
      copiedMutations.putAll(mutations);
    }
    mutations = Collections.unmodifiableSortedMap(copiedMutations);
  }

  /** The mutation keys in {@link AspectKey} order. */
  @Nonnull
  public SortedSet<AspectKey> sortedKeyset() {
    return Collections.unmodifiableSortedSet(new TreeSet<>(mutations.keySet()));
  }

  /**
   * Returns a new plan whose conflict-key set is the union of this plan's and {@code other}'s. This
   * plan is left unchanged.
   */
  @Nonnull
  public MutationPlan mergeConflictKeys(@Nonnull MutationPlan other) {
    SortedSet<ConflictKey> merged = new TreeSet<>(conflictKeys);
    merged.addAll(other.conflictKeys);
    return new MutationPlan(commandId, merged, observedVersions, mutations);
  }
}
