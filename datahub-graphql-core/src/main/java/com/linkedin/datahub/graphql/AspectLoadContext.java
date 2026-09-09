package com.linkedin.datahub.graphql;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Per-load aspect selection contributed by a GraphQL field resolver.
 *
 * <p><b>Invariant:</b> two merge layers cooperate:
 *
 * <ul>
 *   <li><b>Resolver-side merge</b> ({@link QueryContext#mergeAspectLoadContext}) runs at enqueue
 *       time so aliased sibling fields that share a DataLoader key still contribute to the
 *       request-scoped union even when the DataLoader cache collapses duplicate keys before
 *       dispatch (identical cache keys share one pending future and suppress later key contexts).
 *   <li><b>Dispatch-side merge</b> ({@link
 *       com.linkedin.datahub.graphql.util.AspectUtils#unionKeyContexts}) unions key contexts that
 *       survive into the batch, covering load paths that only pass key context.
 * </ul>
 *
 * <p>DataLoader cache keys for entity loaders include a stable signature of this context (via
 * {@link #cacheKeySignature()} in {@code GmsGraphQLEngine.createDataLoader}) so a later dispatch of
 * the same URN with a disjoint selection does not reuse an under-hydrated cached {@code
 * DataFetcherResult}. Loads without an {@link AspectLoadContext} key context keep the original
 * key-only cache behavior.
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

  /**
   * Stable signature for DataLoader cache keys. Equal contexts (including {@link #fetchAll()})
   * produce equal signatures so identical selections still cache-hit across loads.
   */
  @Nonnull
  public Object cacheKeySignature() {
    if (fetchAllAspects) {
      return "FETCH_ALL";
    }
    // requiredAspects is an unmodifiable HashSet; Set equality is order-independent.
    return requiredAspects;
  }

  /** Returns a context covering every aspect required by either operand. */
  @Nonnull
  public AspectLoadContext union(@Nullable AspectLoadContext other) {
    if (other == null || other == this) {
      return this;
    }
    if (this.fetchAllAspects || other.fetchAllAspects) {
      return FETCH_ALL;
    }
    if (this.requiredAspects.containsAll(other.requiredAspects)) {
      return this;
    }
    if (other.requiredAspects.containsAll(this.requiredAspects)) {
      return other;
    }
    Set<String> merged = new HashSet<>(this.requiredAspects);
    merged.addAll(other.requiredAspects);
    // Skip of()'s defensive copy; merged is a fresh HashSet we exclusively own.
    return new AspectLoadContext(false, Collections.unmodifiableSet(merged));
  }

  /**
   * Resolves the concrete aspect set to fetch: full defaults on fallback, otherwise the unioned
   * required aspects plus any always-included aspects (e.g. key aspects).
   *
   * <p>The returned set may be immutable (for example when this context is {@link #fetchAll()} and
   * {@code defaultAspects} is returned as-is). Callers must not mutate the result.
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AspectLoadContext)) {
      return false;
    }
    AspectLoadContext that = (AspectLoadContext) o;
    return fetchAllAspects == that.fetchAllAspects && requiredAspects.equals(that.requiredAspects);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fetchAllAspects, requiredAspects);
  }

  @Override
  public String toString() {
    if (fetchAllAspects) {
      return "AspectLoadContext{FETCH_ALL}";
    }
    return "AspectLoadContext{" + requiredAspects + "}";
  }
}
