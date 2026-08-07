package com.linkedin.datahub.graphql;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Per-load aspect selection contributed by a GraphQL field resolver. Multiple loads of the same
 * entity type within a request are {@link #union(AspectLoadContext) unioned} before {@code
 * batchLoad} so aliased sibling fields with disjoint selections still fetch a complete aspect set.
 *
 * <p>Aspect sets are intentionally not part of DataLoader cache keys: URN-keyed batching is
 * preserved, and the request-scoped union covers all invocation contexts in the batch.
 */
public final class AspectLoadContext {

  private static final AspectLoadContext FETCH_ALL = new AspectLoadContext(true, Set.of());

  private final boolean fetchAllAspects;
  private final Set<String> requiredAspects;

  private AspectLoadContext(boolean fetchAllAspects, @Nonnull Set<String> requiredAspects) {
    this.fetchAllAspects = fetchAllAspects;
    this.requiredAspects = requiredAspects;
  }

  /** Signal that optimization is unavailable and the full default aspect set must be fetched. */
  @Nonnull
  public static AspectLoadContext fetchAll() {
    return FETCH_ALL;
  }

  @Nonnull
  public static AspectLoadContext of(@Nonnull Set<String> aspects) {
    return new AspectLoadContext(
        false, Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(aspects))));
  }

  /**
   * Builds a load context from {@link AspectMappingRegistry#getRequiredAspects}. A null registry
   * result means unmapped fields were encountered and callers must fall back to all aspects.
   */
  @Nonnull
  public static AspectLoadContext fromRequiredAspects(@Nullable Set<String> requiredAspects) {
    if (requiredAspects == null) {
      return FETCH_ALL;
    }
    return of(requiredAspects);
  }

  public boolean isFetchAll() {
    return fetchAllAspects;
  }

  @Nonnull
  public Set<String> getRequiredAspects() {
    return requiredAspects;
  }

  /** Returns a context covering every aspect required by either operand. */
  @Nonnull
  public AspectLoadContext union(@Nullable AspectLoadContext other) {
    if (other == null) {
      return this;
    }
    if (this.fetchAllAspects || other.fetchAllAspects) {
      return FETCH_ALL;
    }
    Set<String> merged = new HashSet<>(this.requiredAspects);
    merged.addAll(other.requiredAspects);
    return of(merged);
  }

  /**
   * Resolves the concrete aspect set to fetch: full defaults on fallback, otherwise the unioned
   * required aspects plus any always-included aspects (e.g. key aspects).
   */
  @Nonnull
  public Set<String> resolve(
      @Nonnull Set<String> defaultAspects, @Nullable String... alwaysIncludeAspects) {
    if (fetchAllAspects) {
      return defaultAspects;
    }
    Set<String> result = new HashSet<>(requiredAspects);
    if (alwaysIncludeAspects != null && alwaysIncludeAspects.length > 0) {
      Collections.addAll(result, alwaysIncludeAspects);
    }
    return result;
  }

  @Override
  public String toString() {
    if (fetchAllAspects) {
      return "AspectLoadContext{FETCH_ALL}";
    }
    return "AspectLoadContext{" + requiredAspects + "}";
  }
}
