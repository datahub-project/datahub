package com.datahub.context;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Immutable, type-keyed container of {@link Enrichment} values carried on {@code
 * OperationFingerprint} / {@code OperationContext}.
 *
 * <p>One entry per concrete {@link Enrichment} implementation class. Retrieval is typed via {@link
 * #get(Class)} — no string keys, no casts at call sites. See {@link Enrichment} for the extension
 * pattern rationale.
 *
 * <p>Instances are constructed via {@link #of(Enrichment...)} or {@link #builder()} and are safe to
 * share across threads.
 */
public final class EnrichmentBundle {

  public static final EnrichmentBundle EMPTY = new EnrichmentBundle(Collections.emptyMap());

  private final Map<Class<? extends Enrichment>, Enrichment> byType;

  private EnrichmentBundle(@Nonnull final Map<Class<? extends Enrichment>, Enrichment> byType) {
    // Preserve insertion order (helps deterministic serialization / debug logs); copy for safety.
    this.byType = Collections.unmodifiableMap(new LinkedHashMap<>(byType));
  }

  /** Convenience factory for the common case of a small, statically-known set of enrichments. */
  @Nonnull
  public static EnrichmentBundle of(@Nonnull final Enrichment... enrichments) {
    if (enrichments.length == 0) {
      return EMPTY;
    }
    final Map<Class<? extends Enrichment>, Enrichment> map = new LinkedHashMap<>();
    for (Enrichment enrichment : enrichments) {
      map.put(enrichment.getClass(), enrichment);
    }
    return new EnrichmentBundle(map);
  }

  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Return the enrichment stored under {@code type} (i.e. the enrichment whose concrete class is
   * exactly {@code type}), or empty if no such enrichment is present. No subclass lookup.
   */
  @Nonnull
  public <T extends Enrichment> Optional<T> get(@Nonnull final Class<T> type) {
    return Optional.ofNullable(type.cast(byType.get(type)));
  }

  public boolean isEmpty() {
    return byType.isEmpty();
  }

  /**
   * Return a new {@link EnrichmentBundle} containing everything in this container plus {@code
   * additional}. If an enrichment of the same concrete class is already present, {@code additional}
   * replaces it (last-writer-wins). This instance is unchanged.
   */
  @Nonnull
  public EnrichmentBundle plus(@Nonnull final Enrichment additional) {
    return toBuilder().add(additional).build();
  }

  /**
   * Return a new {@link EnrichmentBundle} merging {@code other} on top of this one — entries in
   * {@code other} replace same-class entries here. This instance is unchanged.
   */
  @Nonnull
  public EnrichmentBundle plus(@Nonnull final EnrichmentBundle other) {
    if (other.isEmpty()) {
      return this;
    }
    if (this.isEmpty()) {
      return other;
    }
    final Builder b = toBuilder();
    for (Enrichment enrichment : other.byType.values()) {
      b.add(enrichment);
    }
    return b.build();
  }

  /**
   * Derive a mutable {@link Builder} pre-populated with this container's entries. Useful for
   * incremental composition. This instance is unchanged.
   */
  @Nonnull
  public Builder toBuilder() {
    final Builder b = new Builder();
    b.byType.putAll(byType);
    return b;
  }

  public static final class Builder {
    private final Map<Class<? extends Enrichment>, Enrichment> byType = new LinkedHashMap<>();

    private Builder() {}

    /**
     * Add {@code enrichment}, replacing any prior value stored under the same concrete class.
     * Returns {@code this} for chaining.
     */
    @Nonnull
    public Builder add(@Nonnull final Enrichment enrichment) {
      byType.put(enrichment.getClass(), enrichment);
      return this;
    }

    @Nonnull
    public EnrichmentBundle build() {
      return byType.isEmpty() ? EMPTY : new EnrichmentBundle(byType);
    }
  }
}
