package com.linkedin.metadata.entity.coordinator;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * An {@link AspectRetriever} that overlays a set of {@link PlannedMutation}s on top of a delegate
 * (typically a non-locking database read). Reads reflect the proposed state:
 *
 * <ul>
 *   <li>a {@link PlannedDelete} for a key hides any database aspect for that key (returned absent);
 *   <li>a {@link PlannedUpsert} for a key returns the proposed aspect instead of the database one;
 *   <li>otherwise the read falls through to the delegate.
 * </ul>
 *
 * <p>The overlay is immutable; {@link #put(PlannedMutation)} returns a new overlay with the added
 * mutation, leaving this instance unchanged. Overlay keys are matched at the latest version, so
 * mutations are looked up via {@link AspectKey#latest(String, String)}.
 */
public final class MutationOverlay implements AspectRetriever {

  @Nonnull private final AspectRetriever delegate;
  @Nonnull private final Map<AspectKey, PlannedMutation> mutations;

  public MutationOverlay(
      @Nonnull AspectRetriever delegate, @Nonnull Map<AspectKey, PlannedMutation> mutations) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.mutations =
        Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(mutations, "mutations")));
  }

  /** An overlay with no proposed mutations — reads pass straight through to the delegate. */
  @Nonnull
  public static MutationOverlay of(@Nonnull AspectRetriever delegate) {
    return new MutationOverlay(delegate, Collections.emptyMap());
  }

  /**
   * Returns a new overlay with {@code mutation} added (replacing any mutation for the same key).
   */
  @Nonnull
  public MutationOverlay put(@Nonnull PlannedMutation mutation) {
    Objects.requireNonNull(mutation, "mutation");
    Map<AspectKey, PlannedMutation> next = new HashMap<>(mutations);
    next.put(mutation.key(), mutation);
    return new MutationOverlay(delegate, next);
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
      @Nonnull OperationFingerprint context, Set<Urn> urns, Set<String> aspectNames) {
    Map<Urn, Map<String, Aspect>> base =
        delegate.getLatestAspectObjects(context, urns, aspectNames);

    Map<Urn, Map<String, Aspect>> result = new HashMap<>();
    for (Urn urn : urns) {
      Map<String, Aspect> aspectMap = new HashMap<>(base.getOrDefault(urn, Collections.emptyMap()));
      for (String aspectName : aspectNames) {
        PlannedMutation mutation = mutations.get(AspectKey.latest(urn.toString(), aspectName));
        if (mutation instanceof PlannedDelete) {
          aspectMap.remove(aspectName);
        } else if (mutation instanceof PlannedUpsert upsert) {
          aspectMap.put(aspectName, upsert.proposedAspect());
        }
      }
      if (!aspectMap.isEmpty()) {
        result.put(urn, aspectMap);
      }
    }
    return result;
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
      @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames) {
    Map<Urn, Map<String, SystemAspect>> base =
        delegate.getLatestSystemAspects(context, urnAspectNames);

    Map<Urn, Map<String, SystemAspect>> result = new HashMap<>();
    for (Map.Entry<Urn, Set<String>> entry : urnAspectNames.entrySet()) {
      Urn urn = entry.getKey();
      Map<String, SystemAspect> aspectMap =
          new HashMap<>(base.getOrDefault(urn, Collections.emptyMap()));
      for (String aspectName : entry.getValue()) {
        PlannedMutation mutation = mutations.get(AspectKey.latest(urn.toString(), aspectName));
        if (mutation instanceof PlannedDelete) {
          aspectMap.remove(aspectName);
        } else if (mutation instanceof PlannedUpsert upsert) {
          aspectMap.put(aspectName, upsert.proposedSystemAspect());
        }
      }
      if (!aspectMap.isEmpty()) {
        result.put(urn, aspectMap);
      }
    }
    return result;
  }

  @Nonnull
  @Override
  public Map<Urn, Boolean> entityExists(@Nonnull OperationFingerprint context, Set<Urn> urns) {
    Map<Urn, Boolean> base = delegate.entityExists(context, urns);

    Map<Urn, Boolean> result = new HashMap<>(base);
    for (Urn urn : urns) {
      boolean exists = result.getOrDefault(urn, false);
      // A proposed upsert can make an entity exist that the database does not yet know about.
      // Aspect-level deletes are not treated as removing the entity, so they never flip existence
      // to false here.
      if (!exists && hasUpsertForUrn(urn)) {
        exists = true;
      }
      result.put(urn, exists);
    }
    return result;
  }

  @Nonnull
  @Override
  public EntityRegistry getEntityRegistry() {
    return delegate.getEntityRegistry();
  }

  private boolean hasUpsertForUrn(@Nonnull Urn urn) {
    String urnString = urn.toString();
    return mutations.entrySet().stream()
        .anyMatch(e -> e.getValue() instanceof PlannedUpsert && e.getKey().urn().equals(urnString));
  }
}
