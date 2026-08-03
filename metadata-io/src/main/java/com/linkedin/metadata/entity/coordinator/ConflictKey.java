package com.linkedin.metadata.entity.coordinator;

import java.util.Comparator;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Immutable coordination token used to serialize commands that would otherwise race on the same
 * logical resource. A {@code domain} namespaces the kind of conflict (e.g. {@code
 * "SCHEMA_FIELD_LINKAGE"}) and {@code id} identifies the specific contended resource within that
 * domain (e.g. the parent dataset URN). Two commands that produce an intersecting set of conflict
 * keys must not apply concurrently.
 *
 * <p>Natural ordering is {@code domain -> id}, giving a deterministic acquisition order.
 */
public record ConflictKey(@Nonnull String domain, @Nonnull String id)
    implements Comparable<ConflictKey> {

  private static final Comparator<ConflictKey> COMPARATOR =
      Comparator.comparing(ConflictKey::domain).thenComparing(ConflictKey::id);

  public ConflictKey {
    Objects.requireNonNull(domain, "domain");
    Objects.requireNonNull(id, "id");
  }

  @Nonnull
  public static ConflictKey of(@Nonnull String domain, @Nonnull String id) {
    return new ConflictKey(domain, id);
  }

  @Override
  public int compareTo(@Nonnull ConflictKey other) {
    return COMPARATOR.compare(this, other);
  }
}
