package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import java.net.URISyntaxException;
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
 *
 * <p>Whether the index a rebuild leaves behind can still take writes under the running scheme is a
 * separate question, answered for an unstamped index by {@link #countIdsFromAnotherScheme}: the
 * rebuild is unavoidable without reading every id, but rollback writes into the old index address
 * its documents when their ids are the running hasher's, which a sample can judge.
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

  /**
   * How many of the sampled documents, given as id to urn, carry an id the running hasher does not
   * derive from their urn. Zero means a write under the running scheme addresses the documents the
   * sample saw, so an index that records no scheme may keep receiving rollback writes after its
   * rebuild. A sample cannot see a minority written under another scheme: an index that lived
   * through a scheme change and was mostly rewritten since reads as current, and rollback writes
   * then land beside those documents' old copies, which is the state the index was already in
   * before the rebuild. A document whose urn does not parse counts as from another scheme, since no
   * write under the running one can address it.
   */
  public long countIdsFromAnotherScheme(
      @Nonnull final OperationFingerprint operation, @Nonnull final Map<String, String> urnsById) {
    return urnsById.entrySet().stream()
        .filter(entry -> !idDerivedFrom(operation, entry.getValue()).equals(entry.getKey()))
        .count();
  }

  @Nonnull
  private String idDerivedFrom(
      @Nonnull final OperationFingerprint operation, @Nonnull final String urn) {
    try {
      return hasher.documentId(operation, Urn.createFromString(urn));
    } catch (URISyntaxException e) {
      return "";
    }
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
