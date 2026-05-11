package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DEFAULT_SCHEMA_VERSION;

import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Orchestrates all registered {@link AspectMigrationMutator} instances as a single {@link
 * MutationHook} that runs before every other hook (priority {@link #MIGRATION_PRIORITY}).
 *
 * <p>On construction it builds a per-aspect chain of mutators sorted by {@link
 * AspectMigrationMutator#getSourceVersion()}. At runtime it finds the correct mutator for each
 * item's stored schema version and applies the migration in-place on both read and write paths.
 *
 * <p>The chain starts enabled. Call {@link #disable()} once the background migration job has swept
 * all existing data; after that the chain becomes a zero-overhead pass-through. If no mutators are
 * registered the chain disables itself immediately at construction.
 */
@Slf4j
public class AspectMigrationMutatorChain extends MutationHook {

  @Getter
  @Setter
  @Accessors(chain = true)
  @Nonnull
  private AspectPluginConfig config;

  /**
   * {@code aspectName → list of mutators sorted ascending by sourceVersion}. Populated in the
   * constructor; immutable after that.
   */
  private final Map<String, List<AspectMigrationMutator>> chainByAspect;

  private final AtomicBoolean enabled = new AtomicBoolean(true);

  /**
   * Create a chain from the supplied mutators.
   *
   * <p>Gaps between registered mutators (e.g. mutators for v2→v3 and v4→v5 but nothing for v1→v2 or
   * v3→v4) are bridged at runtime by advancing the version marker without transforming the payload.
   * Each gap is logged once at INFO level here, at construction time, since the set of registered
   * mutators is fixed for the lifetime of the chain.
   */
  public AspectMigrationMutatorChain(@Nonnull List<AspectMigrationMutator> mutators) {
    Map<String, List<AspectMigrationMutator>> byAspect = new HashMap<>();
    for (AspectMigrationMutator m : mutators) {
      byAspect.computeIfAbsent(m.getAspectName(), k -> new ArrayList<>()).add(m);
    }
    // Sort each list by sourceVersion ascending so chain traversal works correctly.
    byAspect.forEach(
        (aspect, list) ->
            list.sort(Comparator.comparingLong(AspectMigrationMutator::getSourceVersion)));

    this.chainByAspect = Collections.unmodifiableMap(byAspect);

    if (byAspect.isEmpty()) {
      enabled.set(false);
      log.info("AspectMigrationMutatorChain: no mutators registered, chain disabled.");
    } else {
      log.info(
          "AspectMigrationMutatorChain initialised with {} aspect(s): {}",
          byAspect.size(),
          byAspect.keySet());
      // Log any version gaps once at construction. Gaps are bridged at runtime by advancing the
      // version marker without a transform, so data is never stranded on an unreachable version.
      byAspect.forEach(
          (aspect, list) -> {
            long expected = DEFAULT_SCHEMA_VERSION;
            for (AspectMigrationMutator m : list) {
              if (m.getSourceVersion() > expected) {
                log.info(
                    "AspectMigrationMutatorChain: aspect '{}' has no mutator for v{} → v{} — gap will be bridged (version advanced, no transform applied).",
                    aspect,
                    expected,
                    m.getSourceVersion());
              }
              expected = m.getTargetVersion();
            }
          });
    }
  }

  @Override
  public int getPriority() {
    return MIGRATION_PRIORITY;
  }

  // ── Read path ──────────────────────────────────────────────────────────────

  @Override
  protected Stream<Pair<ReadItem, Boolean>> readMutation(
      @Nonnull Collection<ReadItem> items, @Nonnull RetrieverContext retrieverContext) {
    if (!enabled.get()) {
      return items.stream().map(i -> Pair.of(i, false));
    }
    // Collect eagerly so mutations happen regardless of whether the caller consumes the stream.
    List<Pair<ReadItem, Boolean>> results =
        items.stream()
            .map(
                item -> {
                  List<AspectMigrationMutator> chain =
                      chainByAspect.getOrDefault(item.getAspectName(), Collections.emptyList());
                  boolean mutated = false;
                  ReadItem current = item;
                  for (AspectMigrationMutator mutator : chain) {
                    if (bridgeGap(current.getSystemMetadata(), mutator.getSourceVersion())) {
                      mutated = true;
                    }
                    // Call readMutation directly — bypasses shouldApply() config filtering since
                    // the chain owns the routing logic.
                    List<Pair<ReadItem, Boolean>> result =
                        mutator
                            .readMutation(Collections.singletonList(current), retrieverContext)
                            .collect(Collectors.toList());
                    if (!result.isEmpty()) {
                      current = result.get(0).getFirst();
                      if (Boolean.TRUE.equals(result.get(0).getSecond())) {
                        mutated = true;
                        // Continue to next hop in case of multi-hop chain.
                      }
                    }
                  }
                  // Bridge trailing gap: advance to the aspect's current schema version if the
                  // last registered mutator's target is still below it (no transforms needed
                  // for those intermediate versions). Also fires when the per-aspect chain is
                  // empty (no mutators registered for this aspect).
                  if (current.getAspectSpec() != null
                      && bridgeGap(
                          current.getSystemMetadata(),
                          current.getAspectSpec().getSchemaVersion())) {
                    mutated = true;
                  }
                  return Pair.of(current, mutated);
                })
            .collect(Collectors.toList());
    return results.stream();
  }

  // ── Write path ─────────────────────────────────────────────────────────────

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    if (!enabled.get()) {
      return changeMCPS.stream().map(i -> Pair.of(i, false));
    }
    // Collect eagerly so mutations happen regardless of whether the caller consumes the stream.
    List<Pair<ChangeMCP, Boolean>> results =
        changeMCPS.stream()
            .map(
                item -> {
                  List<AspectMigrationMutator> chain =
                      chainByAspect.getOrDefault(item.getAspectName(), Collections.emptyList());
                  boolean mutated = false;
                  ChangeMCP current = item;
                  for (AspectMigrationMutator mutator : chain) {
                    if (bridgeGap(current.getSystemMetadata(), mutator.getSourceVersion())) {
                      mutated = true;
                    }
                    // Call writeMutation directly — bypasses shouldApply() config filtering.
                    List<Pair<ChangeMCP, Boolean>> result =
                        mutator
                            .writeMutation(Collections.singletonList(current), retrieverContext)
                            .collect(Collectors.toList());
                    if (!result.isEmpty()) {
                      current = result.get(0).getFirst();
                      if (Boolean.TRUE.equals(result.get(0).getSecond())) {
                        mutated = true;
                      }
                    }
                  }
                  // Bridge trailing gap: advance to the aspect's current schema version if the
                  // last registered mutator's target is still below it (no transforms needed
                  // for those intermediate versions). This prevents MigrateAspectsStep from
                  // re-processing rows that are fully migrated but below the current schema
                  // version.
                  // Also fires when the per-aspect chain is empty (no mutators registered for this
                  // aspect).
                  if (current.getAspectSpec() != null
                      && bridgeGap(
                          current.getSystemMetadata(),
                          current.getAspectSpec().getSchemaVersion())) {
                    mutated = true;
                  }
                  return Pair.of(current, mutated);
                })
            .collect(Collectors.toList());
    return results.stream();
  }

  /** Returns {@code true} when the chain is active. */
  public boolean isEnabled() {
    return enabled.get();
  }

  /**
   * Disables the chain. Called by the background migration job once all existing data has been
   * swept and migrated; after this the chain becomes a zero-overhead pass-through.
   */
  public void disable() {
    if (enabled.compareAndSet(true, false)) {
      log.info("AspectMigrationMutatorChain disabled — all migrations complete.");
    }
  }

  /**
   * Advances the stored schema version to {@code targetVersion} when it is behind, bridging any gap
   * where no mutator was registered. The mutator's own {@code isSourceVersion} check then fires
   * correctly on the now-matching version.
   *
   * @return {@code true} if the version was advanced (i.e. a gap was bridged), {@code false} if the
   *     stored version was already at or above {@code targetVersion}
   */
  private static boolean bridgeGap(@Nullable SystemMetadata sm, long targetVersion) {
    if (sm == null) {
      return false;
    }
    long stored = sm.hasSchemaVersion() ? sm.getSchemaVersion() : DEFAULT_SCHEMA_VERSION;
    if (stored < targetVersion) {
      sm.setSchemaVersion(targetVersion);
      return true;
    }
    return false;
  }

  /**
   * Returns the chain map for inspection / validation. Keys are aspect names, values are mutators
   * sorted by sourceVersion ascending.
   */
  @Nonnull
  public Map<String, List<AspectMigrationMutator>> getChainByAspect() {
    return chainByAspect;
  }
}
