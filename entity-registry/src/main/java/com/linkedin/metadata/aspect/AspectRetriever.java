package com.linkedin.metadata.aspect;

import com.datahub.context.OperationFingerprint;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.models.registry.EmptyEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Retrieves aspects keyed by URN. Every lookup takes an {@link OperationFingerprint} as its first
 * argument — the narrow entity-registry-side view over the running {@code OperationContext} — so
 * multi-tenant / multi-partition deployments route reads correctly.
 *
 * <p>The {@code OperationFingerprint} parameter must be non-null. The full {@code OperationContext}
 * class implements {@link OperationFingerprint}, so callers that hold a real {@code
 * OperationContext} pass it through transparently.
 */
public interface AspectRetriever {

  /** No-op singleton for bootstrap / test contexts where no real retriever is available. */
  AspectRetriever EMPTY = new EmptyAspectRetriever();

  class EmptyAspectRetriever implements AspectRetriever {
    @Nonnull
    @Override
    public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
        @Nonnull OperationFingerprint context, Set<Urn> urns, Set<String> aspectNames) {
      return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
        @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames) {
      return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public Map<Urn, Boolean> entityExists(@Nonnull OperationFingerprint context, Set<Urn> urns) {
      return Collections.emptyMap();
    }

    @Nonnull
    @Override
    public EntityRegistry getEntityRegistry() {
      return EmptyEntityRegistry.EMPTY;
    }
  }

  @Nullable
  default Aspect getLatestAspectObject(
      @Nonnull OperationFingerprint context,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName) {
    return getLatestAspectObjects(context, ImmutableSet.of(urn), ImmutableSet.of(aspectName))
        .getOrDefault(urn, Collections.emptyMap())
        .get(aspectName);
  }

  /**
   * Returns for each URN, the map of aspectName to Aspect.
   *
   * @param context required view of the current operation context
   * @param urns urns to fetch
   * @param aspectNames aspect names
   */
  @Nonnull
  Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
      @Nonnull OperationFingerprint context, Set<Urn> urns, Set<String> aspectNames);

  @Nullable
  default SystemAspect getLatestSystemAspect(
      @Nonnull OperationFingerprint context,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName) {
    return getLatestSystemAspects(context, ImmutableMap.of(urn, ImmutableSet.of(aspectName)))
        .getOrDefault(urn, Collections.emptyMap())
        .get(aspectName);
  }

  /** {@link OperationFingerprint}-aware batch lookup for system aspects. */
  @Nonnull
  Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
      @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames);

  @Nonnull
  Map<Urn, Boolean> entityExists(@Nonnull OperationFingerprint context, Set<Urn> urns);

  @Nonnull
  EntityRegistry getEntityRegistry();
}
