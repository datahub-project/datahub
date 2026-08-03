package com.linkedin.metadata.entity.coordinator;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Maps a planned mutation (by its {@link AspectKey}) to the {@link ConflictKey} that names its
 * serialization domain. Operations that resolve to the same {@link ConflictKey} must be serialized;
 * operations with disjoint keys may run concurrently.
 *
 * <p>v1 rules:
 *
 * <ul>
 *   <li>A {@code schemaField} URN coordinates on its <b>parent dataset</b>: {@code
 *       ConflictKey.of("SCHEMA_FIELD_LINKAGE", parentDatasetUrn)}. This collapses the fan-out of
 *       per-field linkage writes onto the single dataset that owns them.
 *   <li>Any other URN coordinates on <b>itself</b>: {@code ConflictKey.of(entityType, urn)}.
 * </ul>
 *
 * <p>Resolution is pure and deterministic: no I/O, no locks, no DB access. The same {@link
 * AspectKey} always yields the same {@link ConflictKey}. A {@code schemaField} URN that cannot be
 * parsed to a parent dataset falls back to the generic {@code (entityType, urn)} rule rather than
 * throwing.
 */
public final class ConflictKeyResolver {

  /** Conflict domain for schemaField linkage, coordinated on the parent dataset URN. */
  static final String SCHEMA_FIELD_LINKAGE_DOMAIN = "SCHEMA_FIELD_LINKAGE";

  /** Resolve the conflict key for a single mutation key. */
  @Nonnull
  public ConflictKey resolve(@Nonnull AspectKey key) {
    Urn urn = UrnUtils.getUrn(key.urn());

    Optional<Pair<Urn, String>> parent = SchemaFieldUtils.parseSchemaFieldUrn(urn);
    if (parent.isPresent()) {
      return ConflictKey.of(SCHEMA_FIELD_LINKAGE_DOMAIN, parent.get().getFirst().toString());
    }

    return ConflictKey.of(urn.getEntityType(), urn.toString());
  }

  /** Resolve the conflict keys for a whole plan's keyset, in {@link ConflictKey} order. */
  @Nonnull
  public SortedSet<ConflictKey> resolveAll(@Nonnull Collection<AspectKey> keys) {
    SortedSet<ConflictKey> resolved = new TreeSet<>();
    for (AspectKey key : keys) {
      resolved.add(resolve(key));
    }
    return resolved;
  }
}
