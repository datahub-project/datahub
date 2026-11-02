package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/**
 * Produces Elasticsearch/OpenSearch {@code _id} values for Search V3 entity documents.
 *
 * <p>OSS ships a URN-only SHA-256 implementation. An extension module may register a replacement
 * bean that also reads {@link OperationFingerprint#getEnrichment(Class)} without this interface
 * declaring which enrichment types exist.
 */
public interface EntityDocumentIdHasher {

  /**
   * Stable document id for {@code urn} under {@code operation}.
   *
   * @param operation fingerprint of the in-flight operation ({@link OperationFingerprint#EMPTY} on
   *     bootstrap / tests)
   * @param urn entity urn; never used as the raw {@code _id}
   * @return ES-safe document id; never {@code null}
   */
  @Nonnull
  String documentId(@Nonnull OperationFingerprint operation, @Nonnull Urn urn);

  /**
   * Stable name of the id derivation this hasher implements. It is recorded on every V3 index built
   * under it (see {@code V3DocumentIdScheme}) so that a later change of derivation is detected and
   * the index rebuilt rather than filled with duplicate documents. Change it whenever {@link
   * #documentId} would start producing different ids for the same input. The class-name default
   * only covers implementations that never declared one; declare an explicit name so a class rename
   * does not read as a scheme change.
   */
  @Nonnull
  default String scheme() {
    return getClass().getName();
  }
}
