package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Records which {@link EntityDocumentIdHasher} scheme a V3 entity index was built under, as {@code
 * _meta.docIdScheme} on the index mapping, and tells whether that still matches the running hasher.
 *
 * <p>An index whose documents carry ids from another scheme cannot be repaired in place or by
 * {@code _reindex}: both keep the old ids, so a live write under the new scheme lands beside the
 * stale copy of the same URN as a duplicate. Such an index has to be rebuilt by a fill from primary
 * storage into a fresh index. Provenance is written once at creation and never changes afterwards,
 * so the check is exact: a missing or different stamp means the documents predate the current
 * scheme. Indices built before stamping existed have no stamp and are rebuilt once.
 *
 * <p>Stamp only when creating an index. Applying a stamped mapping to an existing index would
 * relabel it as current without touching its documents.
 */
public class V3DocumentIdScheme {

  public static final String META = "_meta";
  public static final String DOC_ID_SCHEME = "docIdScheme";

  private final EntityDocumentIdHasher hasher;

  public V3DocumentIdScheme(@Nonnull final EntityDocumentIdHasher hasher) {
    this.hasher = hasher;
  }

  @Nonnull
  public String current() {
    return hasher.scheme();
  }

  /** The scheme recorded on a live index, read from its mapping source. */
  @Nonnull
  public static Optional<String> stampedOn(@Nullable final Map<String, Object> currentMappings) {
    if (currentMappings == null || !(currentMappings.get(META) instanceof Map<?, ?> meta)) {
      return Optional.empty();
    }
    return Optional.ofNullable(meta.get(DOC_ID_SCHEME)).map(Object::toString);
  }

  /** True unless the mapping records exactly the running hasher's scheme. */
  public boolean requiresDocumentRebuild(@Nullable final Map<String, Object> currentMappings) {
    return stampedOn(currentMappings).filter(current()::equals).isEmpty();
  }

  /** A copy of {@code mappings} that records the running scheme, for creating a new index. */
  @Nonnull
  public Map<String, Object> stampedMappings(@Nonnull final Map<String, Object> mappings) {
    final Map<String, Object> meta = new HashMap<>();
    if (mappings.get(META) instanceof Map<?, ?> existing) {
      existing.forEach((key, value) -> meta.put(String.valueOf(key), value));
    }
    meta.put(DOC_ID_SCHEME, current());
    final Map<String, Object> stamped = new HashMap<>(mappings);
    stamped.put(META, meta);
    return stamped;
  }
}
