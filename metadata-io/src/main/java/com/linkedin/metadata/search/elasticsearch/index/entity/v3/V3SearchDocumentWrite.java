package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Search-document write request for a V3 entity index.
 *
 * <p>{@code indexName} is the index the document is written to: the search group's alias for live
 * writes, or a physical backing index when keeping an old index current for rollback after an alias
 * swap or filling a next index before the swap. The writer resolves nothing; the caller decides the
 * target.
 *
 * <p>{@code upsertSource} is the document a {@link Kind#MERGE_EXISTING} write creates when the
 * index holds none. It is set only when the write must not lose its non-semantic fields to a miss,
 * and is dropped when the write is redirected to a fill.
 */
public record V3SearchDocumentWrite(
    @Nonnull String indexName,
    @Nonnull String docId,
    @Nonnull Urn urn,
    @Nonnull String source,
    @Nonnull Kind kind,
    @Nullable String upsertSource) {

  public V3SearchDocumentWrite(
      @Nonnull String indexName,
      @Nonnull String docId,
      @Nonnull Urn urn,
      @Nonnull String source,
      @Nonnull Kind kind) {
    this(indexName, docId, urn, source, kind, null);
  }

  /** The same document and kind, addressed to another index. */
  @Nonnull
  public V3SearchDocumentWrite redirectedTo(@Nonnull final String targetIndexName) {
    return new V3SearchDocumentWrite(targetIndexName, docId, urn, source, kind, upsertSource);
  }

  /**
   * The same write, addressed to an index a fill is building. The fill writes only documents that
   * are absent, so a merge there must not create a partial one that would block it, and a delete
   * becomes the hard-delete placeholder so the fill cannot bring the entity back.
   */
  @Nonnull
  public V3SearchDocumentWrite redirectedToFill(@Nonnull final String fillIndexName) {
    final Kind fillKind =
        switch (kind) {
          case MERGE -> Kind.MERGE_EXISTING;
          case DELETE -> Kind.TOMBSTONE;
          default -> kind;
        };
    // The fill creates documents itself, so a merge into it must not also seed one.
    return new V3SearchDocumentWrite(fillIndexName, docId, urn, source, fillKind);
  }

  /**
   * How the document is applied. Only the fill into a named target index is conditional on what the
   * index holds. It reads primary storage outside the per-URN order that live and catch-up events
   * share, so it must never overwrite a document another writer has already put there.
   */
  public enum Kind {
    /** Replaces the whole document. Catch-up, and LoadIndices maintenance of the alias. */
    REPLACE,
    /** Writes the whole document only when the index holds none. The fill into a target index. */
    CREATE,
    /**
     * Replaces the whole document with the hard-delete placeholder. Written only while a fill can
     * still bring the entity back, and removed once catch-up for that fill completes.
     */
    TOMBSTONE,
    /**
     * Removes the document. {@code source} carries the hard-delete placeholder, which a copy of the
     * write into an index a fill is building writes instead.
     */
    DELETE,
    /** Merges a partial document, creating it when absent. Live writes. */
    MERGE,
    /**
     * Merges a partial document into an existing one. When it is absent, the write does nothing, or
     * creates {@code upsertSource} if the write carries one.
     */
    MERGE_EXISTING
  }
}
